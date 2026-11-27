use std::sync::Arc;
use tokio::sync::Mutex;
use arrow_schema::SchemaRef;
use rtdb_columnar::{DeclaredBatch, DeclaredSchema, MemoryReservation, OwnedBatch};
use rtdb_node::{ControlExecutionOptions, ControlFrontend, ControlResult, NativeSession, PreparedSql, ResultSignaturePolicy, control_schema};
use rtdb_plan::ControlStatement;
use rtdb_query::{BoundParameters, QueryCursor};
use rtdb_sql::{AdmittedControl, parse_admitted_control_in};
use rtdb_tx::{Documents, SqlArguments, TxOp};
use rtdb_types::{Anomaly, LogicalType, Result, TableRef, Value};
use tokio_util::sync::CancellationToken;
use rtdb_types::StructFields;
use crate::{arrow::BoundArguments, region::EmbeddedRegion};

pub(crate) struct SessionHandle {
    pub region: Arc<EmbeddedRegion>,
    pub state: Mutex<NativeSession>,
    pub cancellation: CancellationToken,
    _reservation: Arc<MemoryReservation>,
}
impl SessionHandle {
    pub fn new(region: Arc<EmbeddedRegion>, session: NativeSession) -> Result<Arc<Self>> {
        let reservation = region.memory().reserve(std::mem::size_of::<Self>() + 4096)?;
        let cancellation = session.state().cancellation().clone();
        Ok(Arc::new(Self { region, state: Mutex::new(session), cancellation, _reservation: reservation }))
    }
    pub fn close(&self) { self.cancellation.cancel(); self.state.blocking_lock().state_mut().close(); }
    pub fn ensure_open(&self) -> Result<()> { if self.cancellation.is_cancelled() { Err(super::closed()) } else { Ok(()) } }
}
impl Drop for SessionHandle { fn drop(&mut self) { self.cancellation.cancel(); self.state.get_mut().state_mut().close(); } }

const SOURCE_ALLOCATION_OVERHEAD: usize = 256;

enum Source {
    Sql { text: String, prepared: Option<PreparedSql>, _text_reservation: Arc<MemoryReservation> },
    Set(AdmittedControl),
    Show { statement: AdmittedControl, prepared: bool },
    Bulk { table: TableRef, _name_reservation: Arc<MemoryReservation> },
}
struct StatementState { source: Source, bound: Option<BoundArguments> }
pub(crate) struct StatementHandle {
    pub session: Arc<SessionHandle>,
    creation_database: String,
    state: Mutex<Option<StatementState>>,
    _reservation: Arc<MemoryReservation>,
}
impl StatementHandle {
    pub fn new(session: Arc<SessionHandle>, source: &str, bulk: bool) -> Result<Arc<Self>> {
        session.ensure_open()?;
        let connection = session.state.blocking_lock();
        let reservation = session.region.memory().reserve(super::add(std::mem::size_of::<Self>() + 1024, connection.state().database().len())?)?;
        let creation_database = connection.state().database().to_owned();
        drop(connection);
        let initial = Self::source_state(&session, source, bulk)?;
        let statement = Self { session, creation_database, state: Mutex::new(Some(initial)), _reservation: reservation };
        Ok(Arc::new(statement))
    }
    pub fn replace_source(&self, text: &str, bulk: bool) -> Result<()> {
        self.session.ensure_open()?;
        let mut state = self.state.blocking_lock();
        if state.is_none() { return Err(super::closed()); }
        *state = Some(Self::source_state(&self.session, text, bulk)?);
        Ok(())
    }
    fn source_state(session: &Arc<SessionHandle>, text: &str, bulk: bool) -> Result<StatementState> {
        let memory = session.region.memory();
        let source = if bulk {
            let mut parts = text.rsplit('.');
            let table = parts.next().unwrap_or("");
            let schema = parts.next().unwrap_or("public");
            if table.is_empty() || schema.is_empty() { return Err(super::invalid("Bulk ingest requires a table name")); }
            let bytes = super::add(super::add(schema.len(), table.len())?, SOURCE_ALLOCATION_OVERHEAD)?;
            let reservation = memory.reserve(bytes)?;
            Source::Bulk { table: TableRef { schema: schema.into(), table: table.into() }, _name_reservation: reservation }
        } else if let Some(control) = parse_admitted_control_in(text, &Default::default(), memory)? {
            let statement = control.statement();
            if matches!(statement, ControlStatement::Begin(_) | ControlStatement::Commit(_) | ControlStatement::Rollback) {
                return Err(Anomaly::incorrect("xtdb.adbc/tx-control-as-sql", "BEGIN, START TRANSACTION, COMMIT and ROLLBACK aren't available as SQL here: use the transaction API (ADBC's autocommit, commit and rollback; Flight SQL's BeginTransaction and EndTransaction; Xtdb.Connection's begin, commitSync, commitAsync and rollbackTx), and SET TRANSACTION for a transaction's characteristics"));
            }
            if !matches!(statement, ControlStatement::Show { .. } | ControlStatement::ShowTransactionIsolationLevel | ControlStatement::SetTransaction(_) | ControlStatement::SetSessionCharacteristics(_) | ControlStatement::SetTimeZone(_) | ControlStatement::SetAwaitToken(_) | ControlStatement::SetParameter { .. } | ControlStatement::SetRole) {
                return Err(Anomaly::incorrect("xtdb/not-a-query", "This statement is not supported through ADBC"));
            }
            if statement.is_show() {
                Source::Show { statement: control, prepared: false }
            } else { Source::Set(control) }
        } else {
            let reservation = memory.reserve(super::add(text.len(), SOURCE_ALLOCATION_OVERHEAD)?)?;
            Source::Sql { text: text.into(), prepared: None, _text_reservation: reservation }
        };
        Ok(StatementState { source, bound: None })
    }
    pub fn close(&self) { self.state.blocking_lock().take(); }
    pub fn bind(&self, values: BoundArguments) -> Result<()> {
        self.session.ensure_open()?;
        self.state.blocking_lock().as_mut().ok_or_else(super::closed)?.bound = Some(values); Ok(())
    }
    pub fn prepare(self: &Arc<Self>, token: &CancellationToken) -> Result<()> {
        if token.is_cancelled() { return Err(super::cancelled()); }
        let statement = Arc::clone(self);
        let token = token.clone();
        self.session.region.run(move || async move {
            let mut locked = statement.state.lock().await;
            let state = locked.as_mut().ok_or_else(super::closed)?;
            let mut session = statement.session.state.lock().await;
            match &mut state.source {
                Source::Sql { text, prepared, .. } => {
                    if text.is_empty() { return Err(Anomaly::incorrect("xtdb.adbc/no-sql", "SQL query not set")); }
                    let context = session.prepare_context_for(&statement.creation_database, &token).await?;
                    *prepared = Some(PreparedSql::prepare(text, &context, Default::default(), ResultSignaturePolicy::Dynamic)?);
                }
                Source::Show { prepared, .. } => { session.state().revalidate().await?; *prepared = true; }
                Source::Set(_) => { session.state().revalidate().await?; }
                Source::Bulk { .. } => return Err(Anomaly::incorrect("xtdb.adbc/bulk-ingest-no-prepare", "Bulk ingest does not support prepare")),
            }
            Ok(())
        })
    }
    pub fn schema(self: &Arc<Self>, parameters: bool, token: &CancellationToken) -> Result<crate::arrow::OwnedSchema> {
        self.session.ensure_open()?;
        let statement = Arc::clone(self);
        let token = token.clone();
        self.session.region.run(move || async move {
            let locked = statement.state.lock().await;
            let state = locked.as_ref().ok_or_else(super::closed)?;
            let mut session = statement.session.state.lock().await;
            match &state.source {
                Source::Sql { prepared, .. } => {
                    let prepared = prepared.as_ref().ok_or_else(not_prepared)?;
                    if parameters { return crate::arrow::OwnedSchema::parameters(prepared.parameter_count(), statement.session.region.memory()); }
                    if prepared.is_dml() { return crate::arrow::OwnedSchema::empty(statement.session.region.memory()); }
                    let context = session.prepare_context_for(&statement.creation_database, &token).await?;
                    let result = {
                        context.check_cancelled()?;
                        let count = prepared.parameter_count();
                        let _parameter_types = context.memory.reserve(super::mul(count, std::mem::size_of::<LogicalType>())?)?;
                        let types = vec![LogicalType::Null; count];
                        prepared.result_schema(&context, &types)?
                    };
                    crate::arrow::OwnedSchema::logical(result.schema(), statement.session.region.memory())
                }
                Source::Show { statement: control, prepared } => {
                    if !*prepared { return Err(not_prepared()); }
                    if parameters { crate::arrow::OwnedSchema::empty(statement.session.region.memory()) } else {
                        crate::arrow::OwnedSchema::logical(&control_schema(control.statement())?.unwrap_or_default(), statement.session.region.memory())
                    }
                }
                Source::Set(_) => crate::arrow::OwnedSchema::empty(statement.session.region.memory()),
                Source::Bulk { .. } => Err(Anomaly::incorrect("xtdb.adbc/bulk-ingest-no-query", "Bulk ingest does not have a query schema")),
            }
        })
    }
    pub fn query(self: &Arc<Self>, token: &CancellationToken) -> Result<Arc<CursorHandle>> {
        let _token_storage = self.session.region.memory().reserve(4096)?;
        let result_token = token.child_token();
        let statement = Arc::clone(self);
        let token = result_token.clone();
        let cursor = self.session.region.run(move || async move {
            let mut locked = statement.state.lock().await;
            let state = locked.as_mut().ok_or_else(super::closed)?;
            let mut session = statement.session.state.lock().await;
            let args = state.bound.as_ref().map(|bound| &bound.arguments);
            match &mut state.source {
                Source::Sql { text, prepared, .. } => {
                    if text.is_empty() { return Err(Anomaly::incorrect("xtdb.adbc/no-sql", "SQL query not set")); }
                    if prepared.is_none() {
                        if args.is_some() { return Err(Anomaly::incorrect("xtdb.adbc/bind-without-prepare", "Call prepare before executing with bound arguments")); }
                        let context = session.prepare_context_for(&statement.creation_database, &token).await?;
                        *prepared = Some(PreparedSql::prepare(text, &context, Default::default(), ResultSignaturePolicy::Dynamic)?);
                    }
                    let context = session.query_context_for(&statement.creation_database, &token).await?;
                    let prepared = prepared.as_ref().ok_or_else(not_prepared)?;
                    check_argument_row_count(args)?;
                    if let Some(args) = args
                        && let Some(page) = args.first_arrow_page()
                        && let Some(cursor) = prepared.try_open_query_arrow(Arc::clone(&context), page, args.types()).await? {
                        return Ok(cursor);
                    }
                    let values = argument_row(args, statement.session.region.memory())?;
                    prepared.open_query_columns_typed(context, values.0, args.map(SqlArguments::types).unwrap_or_default()).await
                }
                Source::Show { statement: control, prepared } => {
                    if !*prepared && args.is_some() { return Err(Anomaly::incorrect("xtdb.adbc/bind-without-prepare", "Call prepare before executing with bound arguments")); }
                    *prepared = true;
                    match session.execute_control_arrow_with(control.statement(), None, &token, ControlExecutionOptions { frontend: ControlFrontend::Adbc, implicit_transaction: false }).await? {
                        ControlResult::Rows(cursor) => Ok(cursor),
                        _ => Err(Anomaly::incorrect("xtdb/not-a-query", "Statement does not produce rows")),
                    }
                }
                Source::Set(_) => Err(Anomaly::incorrect("xtdb/not-a-query", "Statement does not produce rows")),
                Source::Bulk { .. } => Err(Anomaly::incorrect("xtdb.adbc/bulk-ingest-no-query", "Bulk ingest does not support queries")),
            }
        })?;
        CursorHandle::new(Arc::clone(&self.session.region), cursor, result_token)
    }
    pub fn update(self: &Arc<Self>, token: &CancellationToken) -> Result<i64> {
        let statement = Arc::clone(self);
        let token = token.clone();
        self.session.region.run(move || async move {
            let mut locked = statement.state.lock().await;
            let state = locked.as_mut().ok_or_else(super::closed)?;
            let mut session = statement.session.state.lock().await;
            session.state().revalidate().await?;
            match &mut state.source {
                Source::Sql { text, prepared, .. } => {
                    if text.is_empty() { return Err(Anomaly::incorrect("xtdb.adbc/no-sql", "SQL query not set")); }
                    if prepared.is_none() {
                        let context = session.prepare_context_for(&statement.creation_database, &token).await?;
                        *prepared = Some(PreparedSql::prepare(text, &context, Default::default(), ResultSignaturePolicy::Dynamic)?);
                    }
                    let operation = prepared.as_ref().ok_or_else(not_prepared)?.dml_operation(state.bound.as_ref().map(|bound| bound.arguments.clone()))?;
                    token.run_until_cancelled(session.state_mut().execute_operation(operation)).await.ok_or_else(super::cancelled)??;
                    Ok(-1)
                }
                Source::Set(control) => {
                    let args = state.bound.as_ref().map(|bound| &bound.arguments);
                    check_argument_row_count(args)?;
                    let memory = statement.session.region.memory();
                    let options = ControlExecutionOptions { frontend: ControlFrontend::Adbc, implicit_transaction: false };
                    let result = if let Some(args) = args && let Some(page) = args.first_arrow_page() {
                        let parameters = BoundParameters::from_arrow(page.clone(), 0, args.execution_types().collect(), memory)?;
                        session.execute_control_arrow_with(control.statement(), Some(&parameters), &token, options).await?
                    } else {
                        let (values, _reservation) = argument_row(args, memory)?;
                        session.execute_control_with(control.statement(), &values, &token, options).await?
                    };
                    match result {
                        ControlResult::Command { .. } => Ok(-1), _ => Err(Anomaly::incorrect("xtdb.adbc/not-an-update", "Statement is not an update")),
                    }
                }
                Source::Show { .. } => Err(Anomaly::incorrect("xtdb.adbc/not-an-update", "Statement is not an update")),
                Source::Bulk { table, .. } => {
                    let Some(bound) = state.bound.take() else { return Ok(0); };
                    let count = i64::try_from(bound.arguments.row_count()).map_err(|_| super::invalid("Bulk row count exceeds i64"))?;
                    match bound.arguments.ingest_named_documents(&bound.names, statement.session.region.memory()) {
                        Ok(Some(documents)) => {
                            let operation = TxOp::PutArrowDocs { table: table.clone(), documents };
                            token.run_until_cancelled(session.state_mut().execute_operation(operation)).await.ok_or_else(super::cancelled)??;
                            return Ok(count);
                        }
                        Ok(None) => {}
                        Err(error) => return Err(error),
                    }
                    let node = super::add(super::mul(std::mem::size_of::<(String, Value)>(), 16)?, 256)?;
                    let bytes = bound.arguments.rows().try_fold(4096, |bytes, row| {
                        bound.names.iter().zip(row?.iter()).try_fold(bytes, |bytes, (name, value)| {
                            super::add(bytes, super::add(super::add(value.retained_bytes()?, name.len())?, node)?)
                        })
                    })?;
                    let _scratch = statement.session.region.memory().reserve(super::mul(bytes, 2)?)?;
                    let rows = bound.arguments.rows().map(|row| {
                        bound.names.iter().cloned().zip(row?.iter().cloned()).map(|(name, mut value)| {
                            rtdb_columnar::admit_execution_value(&mut value)?;
                            Ok((name, value))
                        }).collect::<Result<StructFields<_>>>()
                    }).collect::<Result<_>>()?;
                    let operation = TxOp::PutDocs { table: table.clone(), documents: Documents::new(rows, statement.session.region.memory())?, valid_from: None, valid_to: None };
                    token.run_until_cancelled(session.state_mut().execute_operation(operation)).await.ok_or_else(super::cancelled)??;
                    Ok(count)
                }
            }
        })
    }
}
fn argument_row(arguments: Option<&SqlArguments>, memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<(Vec<Value>, Arc<MemoryReservation>)> {
    check_argument_row_count(arguments)?;
    let Some(arguments) = arguments else { return Ok((Vec::new(), memory.reserve(0)?)); };
    let row = arguments.rows().next().ok_or_else(|| super::invalid("Argument row is missing"))??;
    let bound = row.iter().try_fold(1024, |bytes, value| super::add(bytes, value.retained_bytes()?))?;
    let reservation = memory.reserve(bound)?;
    Ok((row.to_vec(), reservation))
}
fn check_argument_row_count(arguments: Option<&SqlArguments>) -> Result<()> {
    if arguments.is_some_and(|arguments| arguments.row_count() != 1) {
        return Err(Anomaly::incorrect("xtdb/incorrect-arg-count", "A query or SET requires one argument row"));
    }
    Ok(())
}
fn not_prepared() -> Anomaly { Anomaly::incorrect("xtdb.adbc/not-prepared", "Call prepare first") }

pub(crate) struct CursorHandle {
    pub region: Arc<EmbeddedRegion>,
    pub token: CancellationToken,
    schema: DeclaredSchema,
    state: Mutex<Option<CursorInput>>,
}
enum CursorInput { Query(QueryCursor), Batch(OwnedBatch) }
impl CursorHandle {
    pub fn new(region: Arc<EmbeddedRegion>, cursor: QueryCursor, token: CancellationToken) -> Result<Arc<Self>> {
        let mut bound = 4096;
        for field in cursor.schema().fields() { bound = super::add(bound, super::add(super::mul(field.data_type.retained_bytes()?, 8)?, super::mul(field.name.len(), 4)?)?)?; }
        let reservation = region.memory().reserve(bound)?;
        let schema = DeclaredSchema::with_reservation(crate::arrow::logical_schema(cursor.schema())?, reservation);
        Ok(Arc::new(Self { region, token, schema, state: Mutex::new(Some(CursorInput::Query(cursor))) }))
    }
    pub fn from_batch(region: Arc<EmbeddedRegion>, batch: OwnedBatch, token: CancellationToken) -> Result<Arc<Self>> {
        let schema = batch.batch().schema_ref().as_ref();
        let reservation = region.memory().reserve(crate::arrow::schema_bound(schema)?)?;
        let schema = DeclaredSchema::with_reservation(schema.clone(), reservation);
        Ok(Arc::new(Self { region, token: token.child_token(), schema, state: Mutex::new(Some(CursorInput::Batch(batch))) }))
    }
    pub fn schema(&self) -> &SchemaRef { self.schema.schema() }
    pub fn next(self: &Arc<Self>) -> Result<Option<DeclaredBatch>> {
        let cursor_handle = Arc::clone(self);
        self.region.run(move || async move {
            let mut state = cursor_handle.state.lock().await;
            if cursor_handle.token.is_cancelled() { state.take(); return Err(super::cancelled()); }
            if matches!(state.as_ref(), Some(CursorInput::Batch(_))) {
                let Some(CursorInput::Batch(batch)) = state.take() else { unreachable!() };
                return cursor_handle.schema.conform(batch, cursor_handle.region.memory()).map(Some);
            }
            let Some(CursorInput::Query(cursor)) = state.as_mut() else { return Ok(None); };
            let result = cursor.next_batch().await.and_then(|batch| {
                batch.map(|batch| cursor_handle.schema.conform(batch, cursor_handle.region.memory()).inspect_err(|_| cursor.record_consumer_error())).transpose()
            });
            if !matches!(&result, Ok(Some(_))) { state.take(); }
            result
        })
    }
    pub fn consumer_error(&self) { if let Some(CursorInput::Query(cursor)) = self.state.blocking_lock().as_ref() { cursor.record_consumer_error(); } }
    pub fn close(&self) { self.token.cancel(); if let Some(CursorInput::Query(mut cursor)) = self.state.blocking_lock().take() { cursor.cancel(); } }
}
impl Drop for CursorHandle {
    fn drop(&mut self) {
        self.token.cancel();
        if let Some(CursorInput::Query(mut cursor)) = self.state.get_mut().take() { cursor.cancel(); }
    }
}
