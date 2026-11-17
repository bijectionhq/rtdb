use std::collections::BTreeMap;
use std::sync::Arc;
use rtdb_columnar::{MemoryReservation, OwnedBatch, estimate_read_value_bytes, read_value};
use rtdb_node::{CommitResult, ControlExecutionOptions, ControlFrontend, ControlResult, NativeSession, Node, PreparedSql, TransactionStatus, control_schema};
use rtdb_pg_codec::{Format, SessionEnvironment};
use rtdb_plan::{ControlStatement, DiscardTarget};
use rtdb_query::QueryCursor;
use rtdb_types::{Anomaly, PgType, Result, Value};
use tokio::io::AsyncWrite;
use tokio_util::sync::CancellationToken;
use crate::{ServerConfig, connection::{Startup, apply_startup, display_parameter}, error::{cancelled, protocol}, server::CancellationSlot, wire::{Column, Frame, Input, Writer}};

enum PreparedKind { Empty, Query(PreparedSql), Dml(PreparedSql), Control(ControlStatement), Keywords }
struct Prepared { source: String, kind: PreparedKind, oids: Vec<i32>, columns: Option<Vec<Column>>, _reservation: Arc<MemoryReservation> }
struct RegisteredStatement { prepared: Arc<Prepared>, _reservation: Arc<MemoryReservation> }
struct QueryPortal { cursor: QueryCursor, batch: Option<OwnedBatch>, row: usize, sent: u64 }
enum PortalState { Query(QueryPortal), Ready }
struct Portal { prepared: Arc<Prepared>, arguments: Vec<Value>, _charges: Vec<Arc<MemoryReservation>>, columns: Option<Vec<Column>>, state: PortalState, cancellation: CancellationToken }

pub(crate) struct ProtocolSession {
    session: NativeSession,
    node: Node,
    config: Arc<ServerConfig>,
    startup: Startup,
    cancellations: Arc<CancellationSlot>,
    extended: bool,
    skip_until_sync: bool,
    implicit: bool,
    transaction_generation: u64,
    statements: BTreeMap<String, RegisteredStatement>,
    portals: BTreeMap<String, Portal>,
    copy: Option<crate::copy::CopyInput>,
}
impl ProtocolSession {
    pub fn new(session: NativeSession, node: Node, config: Arc<ServerConfig>, startup: Startup, cancellations: Arc<CancellationSlot>) -> Self { Self { session, node, config, startup, cancellations, extended: false, skip_until_sync: false, implicit: false, transaction_generation: 0, statements: BTreeMap::new(), portals: BTreeMap::new(), copy: None } }
    pub fn extended(&self) -> bool { self.extended }
    pub fn close(&mut self) { self.copy = None; self.portals.clear(); self.statements.clear(); self.session.state_mut().close(); }
    fn environment(&self) -> Result<SessionEnvironment> { SessionEnvironment::new(self.session.state().parameter_entries(), self.session.state().effective_timezone(), self.node.memory()) }
    fn token(&self) -> CancellationToken { self.session.state().cancellation().child_token() }
    fn register_statement(&mut self, name: String, prepared: Arc<Prepared>) -> Result<()> { let reservation = self.node.memory().reserve(name.capacity().checked_add(512).ok_or_else(|| protocol("Statement name is too large"))?)?; self.statements.insert(name, RegisteredStatement { prepared, _reservation: reservation }); Ok(()) }
    fn ready(&self) -> u8 { match self.session.state().status() { TransactionStatus::Idle | TransactionStatus::Closed => b'I', TransactionStatus::Failed => b'E', _ => b'T' } }
    fn options(&self) -> rtdb_sql::PrepareOptions { rtdb_sql::PrepareOptions { search_path: self.session.state().parameter("search_path").unwrap_or("public").split(',').map(|name| name.trim().trim_matches('"').to_owned()).collect(), ..Default::default() } }
    fn begin_implicit(&mut self, read_only: Option<bool>) -> Result<()> { if self.session.state().status() != TransactionStatus::Idle { return Ok(()); } let mut settings = rtdb_node::TransactionSettings::default(); settings.access = read_only.map(|read_only| if read_only { rtdb_node::AccessSettings::ReadOnly(Default::default()) } else { rtdb_node::AccessSettings::ReadWrite(Default::default()) }); self.session.state_mut().begin_with(settings)?; self.implicit = true; Ok(()) }
    fn end_transaction(&mut self) { self.portals.clear(); self.implicit = false; self.transaction_generation = self.transaction_generation.wrapping_add(1); }
    async fn commit(&mut self) -> Result<()> { let _sample = rtdb_plan::ExecutionSample::new(self.node.execution_observer(), rtdb_plan::ExecutionTimer::PgwireTransaction); let result = self.session.state_mut().commit_tx(None).await; self.end_transaction(); if let CommitResult::Executed(tx) = result? { if let Some(error) = tx.error() { return Err(error.clone()); } } Ok(()) }
    fn rollback(&mut self) -> Result<()> { self.session.state_mut().rollback_tx()?; self.end_transaction(); Ok(()) }

    pub async fn handle<W: AsyncWrite + Unpin>(&mut self, frame: Frame, writer: &mut Writer<W>) -> Result<()> {
        if self.skip_until_sync && frame.tag != b'S' { return Ok(()); }
        let result = self.message(&frame, writer).await;
        if let Err(error) = result {
            if error.code == "xtdb.pgwire/connection-error" { return Err(error); }
            if self.extended { self.skip_until_sync = true; }
            self.session.state_mut().fail_transaction(error.clone());
            writer.error(&error, self.environment()?.fallback(), false).await?;
        }
        Ok(())
    }

    async fn message<W: AsyncWrite + Unpin>(&mut self, frame: &Frame, writer: &mut Writer<W>) -> Result<()> {
        let mut input = Input::new(&frame.body);
        match frame.tag {
            b'Q' => { let sql = input.string()?; input.end()?; self.simple(sql, writer).await?; }
            b'P' => {
                self.extended = true;
                let name = input.string()?.to_owned(); let sql = input.string()?;
                let count = usize::from(input.short()?); let oids = (0..count).map(|_| input.integer()).collect::<Result<Vec<_>>>()?; input.end()?;
                let prepared = self.prepare(sql, oids).await.inspect_err(|_| { if self.session.state().is_read_write() { self.record_transaction_error(); } })?;
                Self::planning_warnings(&prepared, writer).await?;
                self.register_statement(name, prepared)?; writer.empty(b'1').await?;
            }
            b'B' => {
                self.extended = true;
                let name = input.string()?.to_owned(); let statement = input.string()?;
                let prepared = Arc::clone(&self.statements.get(statement).ok_or_else(|| protocol("no prepared statement"))?.prepared);
                let count = usize::from(input.short()?); let formats = (0..count).map(|_| Format::from_code(input.short()? as i16)).collect::<Result<Vec<_>>>()?;
                let count = usize::from(input.short()?);
                if matches!(prepared.kind, PreparedKind::Dml(_)) && count != prepared.oids.len() { self.record_transaction_error(); return Err(Anomaly::incorrect("xtdb.pgwire/missing-arg-types", "Missing types for args - client must specify types for all non-null params in DML statements")); }
                let mut arguments = Vec::with_capacity(count); let mut charges = Vec::with_capacity(count + 1);
                for index in 0..count {
                    let length = input.integer()?; if length == -1 { arguments.push(Value::Null); continue; }
                    let bytes = input.take(usize::try_from(length).map_err(|_| protocol("Negative argument length"))?)?;
                    let oid = *prepared.oids.get(index).ok_or_else(|| protocol("Argument count differs from parameter OIDs"))?;
                    if oid == 0 && matches!(prepared.kind, PreparedKind::Dml(_)) { self.record_transaction_error(); return Err(Anomaly::incorrect("xtdb.pgwire/missing-arg-types", "Missing types for args - client must specify types for all non-null params in DML statements")); }
                    let ty = PgType::from_oid(oid).ok_or_else(|| Anomaly::unsupported("xtdb.pgwire/unsupported-param-type", format!("Unsupported param type OID {oid}")))?;
                    let format = item_format(&formats, count, index)?;
                    let decoded = rtdb_pg_codec::decode(ty, format, bytes, self.node.memory()).map_err(|error| invalid_argument(error, index, format))?;
                    let (value, charge) = decoded.into_parts(); arguments.push(value); charges.push(charge);
                }
                if matches!(prepared.kind, PreparedKind::Query(_) | PreparedKind::Control(ControlStatement::Execute { .. })) && count != prepared.oids.len() { return Err(protocol("Argument count differs from prepared statement")); }
                let count = usize::from(input.short()?); let result_formats = (0..count).map(|_| Format::from_code(input.short()? as i16)).collect::<Result<Vec<_>>>()?; input.end()?;
                if name.is_empty() { self.portals.remove(&name); } else if self.portals.contains_key(&name) { return Err(protocol("Named portals must be explicit closed before they can be redefined")); }
                charges.push(Arc::clone(&frame.charge));
                let portal = self.bind(prepared, arguments, charges, &result_formats).await?; self.portals.insert(name, portal); writer.empty(b'2').await?;
            }
            b'D' => {
                let kind = input.byte()?; let name = input.string()?; input.end()?;
                match kind {
                    b'S' => { if let Some(statement) = self.statements.get(name) { let statement = &statement.prepared; if !matches!(statement.kind, PreparedKind::Empty | PreparedKind::Keywords) { writer.parameters(&statement.oids).await?; } Self::description(statement.columns.as_deref(), self.environment()?.fallback(), writer).await?; } else { writer.empty(b'n').await?; } },
                    b'P' => Self::description(self.portals.get(name).and_then(|portal| portal.columns.as_deref()), self.environment()?.fallback(), writer).await?,
                    _ => return Err(protocol("Invalid describe target")),
                }
            }
            b'E' => {
                let name = input.string()?.to_owned(); let limit = input.integer()?; input.end()?; if limit < 0 { return Err(protocol("Execute row limit cannot be negative")); }
                let generation = self.transaction_generation;
                let mut portal = self.portals.remove(&name).ok_or_else(|| protocol("no such portal"))?;
                let result = self.execute(&mut portal, limit as usize, writer).await;
                if generation == self.transaction_generation { self.portals.insert(name, portal); }
                result?;
            }
            b'C' => { let kind = input.byte()?; let name = input.string()?; input.end()?; match kind { b'P' => { self.portals.remove(name); }, b'S' => { if let Some(statement) = self.statements.remove(name) { self.portals.retain(|_, portal| !Arc::ptr_eq(&portal.prepared, &statement.prepared)); } }, _ => return Err(protocol("Invalid close target")) } writer.empty(b'3').await?; }
            b'S' => { input.end()?; if self.implicit { let result = if self.session.state().status() == TransactionStatus::Failed { self.rollback() } else { self.commit().await }; if let Err(error) = result { writer.error(&error, self.environment()?.fallback(), false).await?; } } self.skip_until_sync = false; self.extended = false; writer.ready(self.ready()).await?; }
            b'H' => { input.end()?; writer.flush().await?; }
            b'd' => { self.copy.as_mut().ok_or_else(|| protocol("COPY IN not in progress, cannot write data"))?.append(input.remaining()).await?; }
            b'c' => { input.end()?; let copy = self.copy.take().ok_or_else(|| protocol("COPY IN not in progress"))?; let token = self.token(); let result = copy.finish(&mut self.session, &self.node, &token).await; if self.session.state().status() == TransactionStatus::Idle { self.end_transaction(); } match result { Ok(rows) => writer.string(b'C', &format!("COPY {rows}")).await?, Err(error) => writer.error(&error, self.environment()?.fallback(), false).await? } writer.ready(self.ready()).await?; }
            b'f' => { self.copy = None; writer.ready(self.ready()).await?; }
            b'p' => (),
            _ => return Err(Anomaly::unsupported("xtdb.pgwire/unknown-client-msg", format!("unknown client message: {}", frame.tag))),
        }
        Ok(())
    }

    async fn prepare(&mut self, source: &str, mut oids: Vec<i32>) -> Result<Arc<Prepared>> {
        let _scratch = self.node.memory().reserve(source.len().checked_mul(128).and_then(|n| n.checked_add(8192)).ok_or_else(|| protocol("SQL is too large"))?)?;
        let spans = rtdb_sql::split_statements(source)?;
        if spans.len() > 1 { return Err(protocol("A prepared statement must contain exactly one statement")); }
        let source = replace_query(spans.first().map_or("", |span| &source[span.start..span.end]));
        let options = self.options(); let token = self.token();
        let (kind, columns, count) = if source.is_empty() { (PreparedKind::Empty, None, 0) }
        else if source.to_ascii_lowercase().starts_with("select string_agg(word, ',') from pg_catalog.pg_get_keywords()") { (PreparedKind::Keywords, Some(vec![Column { name: "col1".into(), ty: PgType::VarChar, format: Format::Text }]), 0) }
        else if let Some(control) = rtdb_sql::parse_control(source, &options)? {
            let count = match &control { ControlStatement::Execute { .. } => control.parameter_count(), ControlStatement::Show { .. } => 0, _ => oids.len() }; let columns = control_schema(&control)?.map(|schema| schema.fields().iter().map(|field| Ok(Column { name: field.name.clone(), ty: PgType::from_logical(&field.data_type)?, format: Format::Text })).collect::<Result<_>>()).transpose()?;
            (PreparedKind::Control(control), columns, count)
        } else {
            let context = self.session.prepare_context(&token).await?;
            let prepared = PreparedSql::prepare_postgres(source, &context, options.search_path, &oids)?;
            let count = prepared.parameter_count();
            if prepared.is_dml() { (PreparedKind::Dml(prepared), None, oids.len()) }
            else { let columns = prepared.prepared_schema().zip(prepared.postgres_types()).map(|(schema, types)| schema.fields().iter().zip(types).map(|(field, ty)| Column { name: field.name.clone(), ty: *ty, format: Format::Text }).collect()); (PreparedKind::Query(prepared), columns, count) }
        };
        oids.resize(count, 0);
        let column_bytes = columns.as_deref().unwrap_or_default().iter().try_fold(0usize, |bytes, column| bytes.checked_add(column.name.capacity()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(|| protocol("Column descriptors are too large")))?;
        let reservation = self.node.memory().reserve(source.len().checked_mul(128).and_then(|n| n.checked_add(count.checked_mul(256)?)).and_then(|n| n.checked_add(oids.capacity().checked_mul(std::mem::size_of::<i32>())?)).and_then(|n| n.checked_add(column_bytes)).and_then(|n| n.checked_add(8192)).ok_or_else(|| protocol("Prepared statement is too large"))?)?;
        Ok(Arc::new(Prepared { source: source.into(), kind, oids, columns, _reservation: reservation }))
    }

    async fn planning_warnings<W: AsyncWrite + Unpin>(prepared: &Prepared, writer: &mut Writer<W>) -> Result<()> {
        if let PreparedKind::Query(statement) | PreparedKind::Dml(statement) = &prepared.kind {
            for notice in statement.notices() { writer.planning_warning(&notice.message).await?; }
        }
        Ok(())
    }

    async fn bind(&mut self, prepared: Arc<Prepared>, arguments: Vec<Value>, mut charges: Vec<Arc<MemoryReservation>>, formats: &[Format]) -> Result<Portal> {
        let token = self.token();
        let bound = prepared.columns.as_deref().unwrap_or_default().iter().try_fold(8192usize, |bytes, column| bytes.checked_add(column.name.len()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(|| protocol("Portal descriptors are too large")))?;
        charges.push(self.node.memory().reserve(bound)?);
        let columns = prepared.columns.as_ref().map(|columns| columns.iter().enumerate().map(|(index, column)| Ok(Column { name: column.name.clone(), ty: column.ty, format: item_format(formats, columns.len(), index)? })).collect::<Result<Vec<_>>>()).transpose()?;
        let mut portal = Portal { prepared: Arc::clone(&prepared), arguments, _charges: charges, columns, state: PortalState::Ready, cancellation: token.clone() };
        let control_options = self.control_options();
        let cursor = match &prepared.kind {
            PreparedKind::Query(query) => { let checked = !self.session.state().is_read_write(); let context = self.session.query_context_with(&token, checked).await?; Some(query.open_query(context, std::mem::take(&mut portal.arguments)).await?) },
            PreparedKind::Control(statement @ ControlStatement::Show { .. }) => match self.session.execute_control_with(statement, &portal.arguments, &token, control_options).await? { ControlResult::Rows(cursor) => Some(cursor), _ => return Err(protocol("SHOW did not produce rows")) },
            PreparedKind::Control(statement @ ControlStatement::Execute { .. }) => {
                let ControlResult::Execute { name, arguments, reservation, allocation: _allocation } = self.session.execute_control_with(statement, &portal.arguments, &token, control_options).await? else { return Err(protocol("EXECUTE did not produce arguments")); };
                let inner = Arc::clone(&self.statements.get(&name).ok_or_else(|| protocol("no prepared statement"))?.prepared);
                if !matches!(inner.kind, PreparedKind::Query(_) | PreparedKind::Dml(_)) { return Err(Anomaly::unsupported("xtdb.pgwire/unsupported-execute", "EXECUTE only supports a prepared query or DML statement")); }
                if matches!(inner.kind, PreparedKind::Query(_)) && arguments.len() != inner.oids.len() { return Err(protocol("EXECUTE argument count differs from prepared statement")); }
                return Box::pin(self.bind(inner, arguments, vec![reservation], formats)).await;
            }
            _ => None,
        };
        if let Some(cursor) = cursor {
            let actual = cursor.schema();
            if let Some(columns) = &portal.columns { if columns.len() != actual.len() || columns.iter().zip(actual.fields()).any(|(column, field)| !column.ty.accepts_result(&field.data_type).unwrap_or(false)) { return Err(Anomaly::new(rtdb_types::AnomalyCategory::Conflict, "xtdb/prepared-query-out-of-date", "cached plan must not change result type")); } }
            portal.state = PortalState::Query(QueryPortal { cursor, batch: None, row: 0, sent: 0 });
        }
        Ok(portal)
    }
    fn control_options(&self) -> ControlExecutionOptions { ControlExecutionOptions { frontend: ControlFrontend::PostgreSql, implicit_transaction: self.implicit } }
    fn record_transaction_error(&self) { if let Some(observer) = self.node.execution_observer() { observer.record_error(rtdb_plan::ExecutionError::Transaction); } }
    async fn description<W: AsyncWrite + Unpin>(columns: Option<&[Column]>, fallback: PgType, writer: &mut Writer<W>) -> Result<()> { match columns { Some(columns) => writer.columns(columns, fallback).await, None => writer.empty(b'n').await } }

    async fn execute<W: AsyncWrite + Unpin>(&mut self, portal: &mut Portal, limit: usize, writer: &mut Writer<W>) -> Result<()> {
        let prepared = Arc::clone(&portal.prepared);
        let failed = self.session.state().status() == TransactionStatus::Failed;
        if failed && !matches!(prepared.kind, PreparedKind::Control(ControlStatement::Rollback)) { return Err(protocol("current transaction is aborted, commands ignored until ROLLBACK is received")); }
        match &prepared.kind {
            PreparedKind::Empty => writer.empty(b'I').await,
            PreparedKind::Keywords => { let env = self.environment()?; let value = rtdb_pg_codec::encode(PgType::VarChar, Format::Text, &Value::Utf8("xtdb".into()), &env, self.node.memory())?; writer.row(&[value]).await?; writer.string(b'C', "SELECT 1").await },
            PreparedKind::Query(_) | PreparedKind::Control(ControlStatement::Show { .. }) => {
                if matches!(prepared.kind, PreparedKind::Query(_)) {
                    if self.session.state().is_read_write() && !is_type_probe(&prepared.source) { return Err(Anomaly::incorrect("xtdb/queries-in-read-write-tx", "Queries are unsupported in a DML transaction").with_context("query", prepared.source.clone().into())); }
                    self.begin_implicit(Some(true))?;
                }
                let token = portal.cancellation.clone(); let _active = self.cancellations.enter(&token);
                token.run_until_cancelled(self.stream(portal, limit, writer)).await.ok_or_else(cancelled)?
            }
            PreparedKind::Dml(statement) => {
                if self.config.read_only { return Err(Anomaly::incorrect("xtdb/dml-in-read-only-server", "DML is not allowed on the READ ONLY server")); }
                if self.session.state().is_read_only() { return Err(Anomaly::incorrect("xtdb/dml-in-read-only-tx", "DML is not allowed in a READ ONLY transaction").with_context("query", prepared.source.clone().into())); }
                self.begin_implicit(Some(false))?;
                let bound = portal.arguments.iter().try_fold(1024usize, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(|| protocol("DML arguments are too large")))?;
                let _arguments = self.node.memory().reserve(bound)?;
                let args = if portal.arguments.is_empty() { None } else { Some(rtdb_tx::SqlArguments::from_rows(vec![portal.arguments.clone()], self.node.memory())?) };
                self.session.state_mut().execute_operation(statement.dml_operation(args)?).await?;
                writer.string(b'C', &dml_tag(&prepared.source)).await
            }
            PreparedKind::Control(statement) => {
                let _sample = matches!(statement, ControlStatement::Commit(_)).then(|| rtdb_plan::ExecutionSample::new(self.node.execution_observer(), rtdb_plan::ExecutionTimer::PgwireTransaction));
                let options = self.control_options();
                let result = self.session.execute_control_with(statement, &portal.arguments, &portal.cancellation, options).await;
                if matches!(statement, ControlStatement::Commit(_) | ControlStatement::Rollback) && self.session.state().status() == TransactionStatus::Idle { self.end_transaction(); }
                match result? {
                    ControlResult::Command { tag, parameter_status, allocation: _allocation, .. } => {
                        if matches!(statement, ControlStatement::Begin(_)) { self.implicit = false; }
                        if let Some((name, value)) = parameter_status { writer.parameter(display_parameter(&name), if name == "standard_conforming_strings" { "on" } else { &value }).await?; }
                        if matches!(statement, ControlStatement::Discard(DiscardTarget::All)) { self.end_transaction(); self.statements.clear(); apply_startup(&mut self.session, &self.startup.parameters, writer).await?; }
                        if matches!(statement, ControlStatement::SetRole) { return Ok(()); }
                        writer.string(b'C', &tag).await
                    }
                    ControlResult::Prepare { name, source, allocation: _allocation } => { let prepared = self.prepare(&source, Vec::new()).await?; Self::planning_warnings(&prepared, writer).await?; self.register_statement(name, prepared)?; writer.string(b'C', "PREPARE").await },
                    ControlResult::CopyIn { table, format, allocation: _allocation } => { self.copy = Some(crate::copy::CopyInput::new(table, format, self.node.memory(), self.config.maximum_copy_bytes)?); let format = self.copy.as_ref().ok_or_else(|| protocol("COPY state missing"))?.wire_format(); writer.send(b'G', &[format.code() as u8, 0, 1, 0, format.code() as u8]).await?; writer.flush().await },
                    _ => Err(protocol("Unexpected control execution result")),
                }
            }
        }
    }

    async fn stream<W: AsyncWrite + Unpin>(&mut self, portal: &mut Portal, limit: usize, writer: &mut Writer<W>) -> Result<()> {
        let columns = portal.columns.as_deref().ok_or_else(|| protocol("Query portal has no column descriptors"))?;
        let PortalState::Query(query) = &mut portal.state else { return Err(protocol("Query portal has no cursor")); };
        let environment = self.environment()?; let mut emitted = 0;
        while limit == 0 || emitted < limit {
            if query.batch.as_ref().is_none_or(|batch| query.row == batch.batch().num_rows()) {
                loop { query.batch = query.cursor.next_batch().await?; query.row = 0; if query.batch.as_ref().is_none_or(|batch| batch.batch().num_rows() != 0) { break; } }
            }
            let Some(batch) = &query.batch else { break; };
            let schema = batch.batch().schema();
            let row = query.row;
            let memory = Arc::clone(self.node.memory());
            let consumption: Result<()> = async {
                let bound = (0..columns.len()).try_fold(columns.len() * 128 + 4096, |bound, column| bound.checked_add(estimate_read_value_bytes(schema.field(column), batch.batch().column(column).as_ref(), row)?).ok_or_else(|| protocol("Result row allocation overflow")))?;
                let _scratch = memory.reserve(bound)?;
                let mut values = Vec::with_capacity(columns.len());
                for (index, column) in columns.iter().enumerate() { let value = read_value(schema.field(index), batch.batch().column(index).as_ref(), row)?; values.push(rtdb_pg_codec::encode(column.ty, column.format, &value, &environment, &memory)?); }
                writer.row(&values).await
            }.await;
            if let Err(error) = consumption { query.cursor.record_consumer_error(); return Err(error); }
            query.row += 1; query.sent = query.sent.checked_add(1).ok_or_else(|| protocol("Result row count overflow"))?; emitted += 1;
        }
        if limit != 0 && emitted == limit { writer.empty(b's').await?; writer.flush().await }
        else { writer.string(b'C', &format!("{} {}", head(&portal.prepared.source), query.sent)).await }
    }

    async fn simple<W: AsyncWrite + Unpin>(&mut self, source: &str, writer: &mut Writer<W>) -> Result<()> {
        self.extended = false; self.portals.remove("");
        let result = self.simple_statements(source, writer).await;
        if let Err(error) = result { writer.error(&error, self.environment()?.fallback(), false).await?; }
        if self.copy.is_none() { writer.ready(self.ready()).await?; }
        Ok(())
    }
    async fn simple_statements<W: AsyncWrite + Unpin>(&mut self, source: &str, writer: &mut Writer<W>) -> Result<()> {
        let spans = rtdb_sql::split_statements(source)?;
        if spans.is_empty() { writer.empty(b'I').await?; return Ok(()); }
        for span in spans {
            let sql = &source[span.start..span.end]; let control = rtdb_sql::parse_control(sql, &self.options())?;
            if !matches!(control, Some(ControlStatement::Begin(_) | ControlStatement::Discard(_) | ControlStatement::SetTransaction(_) | ControlStatement::Show { .. } | ControlStatement::CopyIn { .. })) { self.begin_implicit(None)?; }
            let result: Result<()> = async {
                let prepared = self.prepare(sql, Vec::new()).await?;
                Self::planning_warnings(&prepared, writer).await?;
                if !prepared.oids.is_empty() { return Err(protocol("Parameters not allowed in simple queries")); }
                let mut portal = self.bind(prepared, Vec::new(), Vec::new(), &[]).await?;
                if portal.columns.is_some() { Self::description(portal.columns.as_deref(), self.environment()?.fallback(), writer).await?; }
                self.execute(&mut portal, 0, writer).await
            }.await;
            if let Err(error) = result { if self.implicit { self.rollback()?; } else { self.session.state_mut().fail_transaction(error.clone()); } writer.error(&error, self.environment()?.fallback(), false).await?; }
        }
        if self.implicit { if self.session.state().status() == TransactionStatus::Failed { self.rollback()?; } else { self.commit().await?; } }
        Ok(())
    }
}

fn invalid_argument(error: Anomaly, index: usize, format: Format) -> Anomaly {
    Anomaly::new_nullable(rtdb_types::AnomalyCategory::Incorrect, "xtdb.pgwire/invalid-arg-representation", error.message)
        .with_context("arg-idx", index.into()).with_context("arg-format", if format == Format::Binary { "binary" } else { "text" }.into())
}
fn item_format(formats: &[Format], count: usize, index: usize) -> Result<Format> { match formats.len() { 0 => Ok(Format::Text), 1 => Ok(formats[0]), length if length == count => Ok(formats[index]), _ => Err(protocol("Invalid format code count")) } }
fn head(source: &str) -> String { source.split_whitespace().next().unwrap_or_default().to_ascii_uppercase() }
fn dml_tag(source: &str) -> String { match head(source).as_str() { "INSERT" => "INSERT 0 0".into(), "UPDATE" => "UPDATE 0".into(), "DELETE" => "DELETE 0".into(), "PATCH" => "PATCH 0".into(), "ERASE" => "ERASE 0".into(), "CREATE" => "CREATE TABLE".into(), value => value.into() } }
fn is_type_probe(source: &str) -> bool { let mut prior_space = false; source.chars().filter(|character| { let repeated = *character == ' ' && prior_space; prior_space = *character == ' '; !repeated }).collect::<String>() == "SELECT pg_type.oid, typname FROM pg_catalog.pg_type LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r from pg_namespace as ns join ( select s.r, (current_schemas(false))[s.r] as nspname from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r using ( nspname ) ) as sp ON sp.nspoid = typnamespace WHERE typname = $1 ORDER BY sp.r, pg_type.oid DESC LIMIT 1" }
fn replace_query(source: &str) -> &str { if source == "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass ORDER BY nspname" { "SELECT n.oid,n.nspname,n.nspowner,n.nspacl,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass ORDER BY nspname" } else { source } }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argument_reclassification_preserves_nullable_messages_and_parameter_location() {
        for message in [None, Some(String::new()), Some("invalid input".into())] {
            for format in [Format::Text, Format::Binary] {
                let error = invalid_argument(Anomaly::new_nullable(rtdb_types::AnomalyCategory::Fault, "test/decode", message.clone()), 2, format);
                assert_eq!(error.message, message);
                assert_eq!(error.category, rtdb_types::AnomalyCategory::Incorrect);
                assert_eq!(error.code, "xtdb.pgwire/invalid-arg-representation");
                assert_eq!(error.context["arg-idx"], 2);
                assert_eq!(error.context["arg-format"], if format == Format::Binary { "binary" } else { "text" });
            }
        }
    }
}
