use std::sync::Arc;
use std::time::Duration;

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_expr::{EvalContext, evaluate};
use rtdb_format::basis::decode_tx_basis;
use rtdb_plan::{Binder, ControlAccessOptions, ControlCommitMode, ControlStatement, ControlValue, CopyFormat, DiscardTarget, Expr, Identifier, Schema, TransactionOptions};
use rtdb_query::{QueryContext, QueryCursor};
use rtdb_tx::UserMetadata;
use rtdb_types::{Anomaly, AnomalyCategory, LogicalType, Result, ScalarType, TimeUnit, Timestamp, Value};
use tokio_util::sync::CancellationToken;

use crate::{AccessSettings, CommitMode, CommitResult, LastSubmittedTx, NativeSession, OptionUpdate, ReadOnlySettings, ReadWriteSettings, TransactionSettings, TransactionStatus};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ControlFrontend { #[default] Native, PostgreSql, Adbc, FlightSql }

#[derive(Clone, Copy, Debug, Default)]
pub struct ControlExecutionOptions {
    pub frontend: ControlFrontend,
    pub implicit_transaction: bool,
}

pub enum ControlResult {
    Command { tag: String, transaction: Option<CommitResult>, parameter_status: Option<(String, String)> },
    Rows(QueryCursor),
    Prepare { name: String, source: String },
    Execute { name: String, arguments: Vec<Value>, reservation: Arc<MemoryReservation> },
    CopyIn { table: Vec<Identifier>, format: Option<CopyFormat> },
}

pub fn control_schema(statement: &ControlStatement) -> Result<Option<Schema>> {
    let ControlStatement::Show { name } = statement else { return Ok(None); };
    let text = LogicalType::Scalar(ScalarType::Utf8);
    let integer = LogicalType::Scalar(ScalarType::I64).maybe();
    let instant = LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) });
    let fields = match name.as_str() {
        "clock_time" => vec![(name.as_str(), instant)],
        "snapshot_token" | "timezone" | "transaction_isolation" => vec![(name.as_str(), text)],
        "standard_conforming_strings" => vec![(name.as_str(), LogicalType::Scalar(ScalarType::Bool).maybe())],
        "latest_submitted_tx" => vec![("tx_id", integer), ("system_time", instant.maybe()), ("committed", LogicalType::Scalar(ScalarType::Bool).maybe()), ("error", LogicalType::Scalar(ScalarType::Transit).maybe()), ("await_token", text.maybe())],
        "latest_completed_txs" => vec![("db_name", text), ("part", LogicalType::Scalar(ScalarType::I32)), ("tx_id", integer), ("system_time", instant.maybe())],
        "latest_submitted_msg_ids" | "latest_processed_msg_ids" => vec![("db_name", text), ("part", LogicalType::Scalar(ScalarType::I32)), ("msg_id", integer)],
        _ => vec![(name.as_str(), text.maybe())],
    };
    let mut binder = Binder::default();
    Schema::new(fields.into_iter().map(|(name, ty)| binder.field(name, None, ty)).collect::<Result<Vec<_>>>()?).map(Some)
}

impl NativeSession {
    /// `cancellation` must descend from `state().cancellation()`; dropping preparation never pins a bare BEGIN.
    pub async fn prepare_context(&mut self, cancellation: &CancellationToken) -> Result<Arc<QueryContext>> {
        cancellation.run_until_cancelled(async {
            let basis = self.state.prepare_basis().await?;
            Ok(self.service.node.query_context(self.state.database(), &basis, self.service.identity.as_ref(), cancellation).await?.with_observer(crate::SessionService::execution_observer(self.service.as_ref())).with_tracer(self.service.node.tracer(), None))
        }).await.ok_or_else(cancelled)?
    }

    /// Prepares against a statement's captured default database without changing the session's current database.
    /// `cancellation` must descend from `state().cancellation()`.
    pub async fn prepare_context_for(&mut self, database: &str, cancellation: &CancellationToken) -> Result<Arc<QueryContext>> {
        cancellation.run_until_cancelled(async {
            let basis = self.state.prepare_basis().await?;
            Ok(self.service.node.query_context(database, &basis, self.service.identity.as_ref(), cancellation).await?.with_observer(crate::SessionService::execution_observer(self.service.as_ref())).with_tracer(self.service.node.tracer(), None))
        }).await.ok_or_else(cancelled)?
    }

    /// Opens a checked read against a statement's captured default database and the session's read basis.
    /// `cancellation` must descend from `state().cancellation()`.
    pub async fn query_context_for(&mut self, database: &str, cancellation: &CancellationToken) -> Result<Arc<QueryContext>> {
        cancellation.run_until_cancelled(async {
            let basis = self.state.read_basis().await?;
            Ok(self.service.node.query_context(database, &basis, self.service.identity.as_ref(), cancellation).await?.with_observer(crate::SessionService::execution_observer(self.service.as_ref())).with_tracer(self.service.node.tracer(), None))
        }).await.ok_or_else(cancelled)?
    }

    /// Revalidates and awaits this session before opening the supplied captured basis.
    /// `cancellation` must descend from `state().cancellation()`; this does not resolve the current transaction's access mode.
    pub async fn query_context_at(&mut self, basis: &crate::QueryBasis, cancellation: &CancellationToken) -> Result<Arc<QueryContext>> {
        cancellation.run_until_cancelled(async {
            self.state.prepare_basis().await?;
            Ok(self.service.node.query_context(self.state.database(), basis, self.service.identity.as_ref(), cancellation).await?.with_observer(crate::SessionService::execution_observer(self.service.as_ref())).with_tracer(self.service.node.tracer(), None))
        }).await.ok_or_else(cancelled)?
    }

    /// `cancellation` must descend from `state().cancellation()`.
    /// Unchecked reads are for EXECUTE arguments and the frontend's exact pgjdbc type probe.
    pub async fn query_context_with(&mut self, cancellation: &CancellationToken, checked: bool) -> Result<Arc<QueryContext>> {
        cancellation.run_until_cancelled(async {
            let basis = if checked { self.state.read_basis().await? } else { self.state.unchecked_read_basis().await? };
            Ok(self.service.node.query_context(self.state.database(), &basis, self.service.identity.as_ref(), cancellation).await?.with_observer(crate::SessionService::execution_observer(self.service.as_ref())).with_tracer(self.service.node.tracer(), None))
        }).await.ok_or_else(cancelled)?
    }

    /// `cancellation` must descend from `state().cancellation()`; the frontend owns returned PREPARE, EXECUTE, and COPY protocol state.
    pub async fn execute_control(&mut self, statement: &ControlStatement, arguments: &[Value], cancellation: &CancellationToken) -> Result<ControlResult> {
        self.execute_control_with(statement, arguments, cancellation, ControlExecutionOptions::default()).await
    }

    /// The cancellation token must descend from this session; protocol options distinguish explicit from implicit transactions.
    pub async fn execute_control_with(&mut self, statement: &ControlStatement, arguments: &[Value], cancellation: &CancellationToken, options: ControlExecutionOptions) -> Result<ControlResult> {
        cancellation.run_until_cancelled(self.control(statement, arguments, cancellation, options)).await.ok_or_else(cancelled)?
    }

    async fn control(&mut self, statement: &ControlStatement, arguments: &[Value], cancellation: &CancellationToken, options: ControlExecutionOptions) -> Result<ControlResult> {
        self.state.revalidate().await?;
        if matches!(options.frontend, ControlFrontend::Adbc | ControlFrontend::FlightSql) && matches!(statement, ControlStatement::Begin(_) | ControlStatement::Commit(_) | ControlStatement::Rollback) {
            return Err(Anomaly::incorrect("xtdb.adbc/tx-control-as-sql", "Use the transaction API to begin, commit, or roll back transactions"));
        }
        let failed_inspection = matches!(options.frontend, ControlFrontend::Adbc | ControlFrontend::FlightSql)
            && matches!(statement, ControlStatement::Show { .. }) && !statement.requires_read_transaction();
        if self.state.status() == TransactionStatus::Failed && !matches!(statement, ControlStatement::Rollback) && !failed_inspection {
            return Err(Anomaly::incorrect("xtdb/tx-failed", "Current transaction is aborted; roll back before issuing more statements"));
        }
        if arguments.len() < statement.parameter_count() { return Err(Anomaly::incorrect("xtdb/missing-parameter", "Control statement has fewer parameters than its expressions")); }
        match statement {
            ControlStatement::Begin(options) => {
                self.state.begin()?;
                let settings = transaction_settings(options, arguments, self.state.effective_timezone(), self.state.session_timezone(), self.service.node.now(), self.service.node.memory());
                if let Err(error) = settings.and_then(|settings| self.state.set_transaction(settings)) { self.state.rollback_tx()?; return Err(error); }
                Ok(command("BEGIN"))
            }
            ControlStatement::SetTransaction(options) => {
                let settings = transaction_settings(options, arguments, self.state.effective_timezone(), self.state.session_timezone(), self.service.node.now(), self.service.node.memory())?;
                self.state.set_transaction(settings)?; Ok(command("SET TRANSACTION"))
            }
            ControlStatement::Commit(mode) => {
                let result = self.state.commit_tx(mode.map(|mode| match mode { ControlCommitMode::Sync => CommitMode::Sync, ControlCommitMode::Async => CommitMode::Async })).await?;
                if let CommitResult::Executed(tx) = &result { if let Some(error) = tx.error() { return Err(error.clone()); } }
                Ok(ControlResult::Command { tag: "COMMIT".into(), transaction: Some(result), parameter_status: None })
            }
            ControlStatement::Rollback => { self.state.rollback_tx()?; Ok(command("ROLLBACK")) }
            ControlStatement::SetSessionCharacteristics(access) => { self.state.set_session_characteristics(*access)?; Ok(command("SET SESSION CHARACTERISTICS")) }
            ControlStatement::SetTimeZone(expression) => {
                let value = self.control_value(expression, arguments)?;
                self.state.set_timezone(string(value, "TIME ZONE")?.ok_or_else(|| invalid("TIME ZONE requires a string"))?)?;
                Ok(ControlResult::Command { tag: "SET TIME ZONE".into(), transaction: None, parameter_status: Some(("TimeZone".into(), self.state.effective_timezone().into())) })
            }
            ControlStatement::SetAwaitToken(expression) => {
                self.state.set_await_token(string(self.control_value(expression, arguments)?, "AWAIT_TOKEN")?)?; Ok(command("SET AWAIT_TOKEN"))
            }
            ControlStatement::SetParameter { name, value } => {
                let value = self.control_value(value, arguments)?;
                let value = if matches!(value, Value::Null | Value::Undefined) { None } else {
                    let Value::Utf8(value) = rtdb_expr::cast(&value, &LogicalType::Scalar(ScalarType::Utf8), self.state.effective_timezone())? else { return Err(invalid("Session parameter could not be formatted")); };
                    Some(value)
                };
                self.state.set_parameter(name.clone(), value.clone())?;
                Ok(ControlResult::Command { tag: "SET".into(), transaction: None, parameter_status: value.map(|value| (name.clone(), value)) })
            }
            ControlStatement::SetRole => Ok(command("SET ROLE")),
            ControlStatement::Discard(target) => {
                if *target == DiscardTarget::All { self.state.reset()?; }
                Ok(command(match target { DiscardTarget::All => "DISCARD ALL", DiscardTarget::Plans => "DISCARD PLANS", DiscardTarget::Temporary => "DISCARD TEMP", DiscardTarget::Sequences => "DISCARD SEQUENCES" }))
            }
            ControlStatement::Show { name } => self.show(name, statement.requires_read_transaction(), cancellation).await.map(ControlResult::Rows),
            ControlStatement::Prepare { name, source } => Ok(ControlResult::Prepare { name: name.clone(), source: source.clone() }),
            ControlStatement::Execute { name, arguments: expressions } => {
                let context = self.query_context_with(cancellation, false).await?;
                let bound = arguments.iter().try_fold(8192usize, |bytes, value| size(bytes, value.retained_bytes()?))?;
                let reservation = context.memory.reserve(size(bound, expressions.len().checked_mul(8192).ok_or_else(|| invalid("EXECUTE allocation size overflow"))?)?)?;
                let eval = EvalContext { parameters: arguments, current_time: context.current_time, timezone: &context.timezone, outer_rows: &[] };
                let arguments = expressions.iter().map(|expression| evaluate(expression, &Schema::default(), &[], &eval)).collect::<Result<Vec<_>>>()?;
                let actual = arguments.iter().try_fold(0usize, |bytes, value| size(bytes, value.retained_bytes()?))?;
                if actual > reservation.bytes() { return Err(invalid("EXECUTE result exceeds its allocation reservation")); }
                Ok(ControlResult::Execute { name: name.clone(), arguments, reservation })
            }
            ControlStatement::CopyIn { table, format } => Ok(ControlResult::CopyIn { table: table.clone(), format: *format }),
            ControlStatement::AttachDatabase { name, configuration_yaml } => {
                self.check_database_control("attach", options)?;
                let configuration = configuration_yaml.as_deref().map(rtdb_format::config::decode_database_yaml).transpose()?.unwrap_or_else(rtdb_format::config::default_database_config);
                let result = self.service.node.attach_database(name.clone(), configuration, self.service.identity.as_ref(), cancellation).await?;
                if let Some(error) = result.error() { return Err(error.clone()); }
                Ok(ControlResult::Command { tag: "ATTACH DATABASE".into(), transaction: Some(CommitResult::Executed(result)), parameter_status: None })
            }
            ControlStatement::DetachDatabase { name } => {
                self.check_database_control("detach", options)?;
                let result = self.service.node.detach_database(name.clone(), self.service.identity.as_ref(), cancellation).await?;
                if let Some(error) = result.error() { return Err(error.clone()); }
                Ok(ControlResult::Command { tag: "DETACH DATABASE".into(), transaction: Some(CommitResult::Executed(result)), parameter_status: None })
            }
        }
    }

    fn control_value(&self, expression: &Expr, parameters: &[Value]) -> Result<Value> {
        evaluate(expression, &Schema::default(), &[], &EvalContext { parameters, current_time: self.service.node.now(), timezone: self.state.effective_timezone(), outer_rows: &[] })
    }

    fn check_database_control(&self, action: &str, options: ControlExecutionOptions) -> Result<()> {
        if self.state.database() != "xtdb" { return Err(Anomaly::incorrect(format!("xtdb.pgwire/{action}-db-on-secondary"), format!("Can only {action} databases when connected to the primary 'xtdb' database."))); }
        if self.state.status() != TransactionStatus::Idle && !options.implicit_transaction { return Err(Anomaly::incorrect(format!("xtdb.pgwire/{action}-db-in-tx"), format!("Cannot {action} a database in a transaction."))); }
        Ok(())
    }

    async fn show(&mut self, name: &str, checked: bool, cancellation: &CancellationToken) -> Result<QueryCursor> {
        let basis = if checked { self.state.read_basis().await? } else { self.state.inspection_basis()? };
        let context = self.service.node.query_context(self.state.database(), &basis, self.service.identity.as_ref(), cancellation).await?
            .with_observer(crate::SessionService::execution_observer(self.service.as_ref())).with_tracer(self.service.node.tracer(), None);
        let mut bound = 8192usize;
        if let Some(value) = self.state.parameter(name) { bound = size(bound, value.len().checked_mul(4).ok_or_else(|| invalid("SHOW allocation size overflow"))?)?; }
        if let Some(token) = self.state.effective_await_token() { bound = size(bound, token.len().checked_mul(4).ok_or_else(|| invalid("SHOW token size overflow"))?)?; }
        let snapshot_token = if name == "snapshot_token" { Some(basis.snapshot_token()) } else { None };
        if let Some(token) = &snapshot_token { bound = size(bound, token.len().checked_mul(4).ok_or_else(|| invalid("SHOW token size overflow"))?)?; }
        let error = match (name, self.state.last_submitted()) { ("latest_submitted_tx", Some(LastSubmittedTx::Executed(tx))) => tx.encoded_error()?, _ => None };
        if let Some(error) = &error { bound = size(bound, error.as_ref().len().checked_mul(4).ok_or_else(|| invalid("SHOW error size overflow"))?)?; }
        let databases = if ["latest_completed_txs", "latest_submitted_msg_ids", "latest_processed_msg_ids"].contains(&name) {
            self.service.node.await_basis(self.state.effective_await_token().map(decode_tx_basis).transpose()?, Duration::from_secs(60), cancellation).await?;
            self.service.node.catalog().open_databases()
        } else { Default::default() };
        for name in databases.keys() { bound = size(bound, size(8192, name.len().checked_mul(4).ok_or_else(|| invalid("SHOW database name size overflow"))?)?)?; }
        let reservation = context.memory.reserve(bound)?;
        let text = LogicalType::Scalar(ScalarType::Utf8);
        let integer = LogicalType::Scalar(ScalarType::I64);
        let instant = LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) });
        let (fields, rows) = match name {
            "snapshot_token" => (vec![(name, text.clone())], vec![vec![Value::Utf8(snapshot_token.unwrap_or_default())]]),
            "clock_time" => (vec![(name, instant.clone())], vec![vec![timestamp(basis.current_time)]]),
            "timezone" => (vec![(name, text.clone())], vec![vec![Value::Utf8(self.state.effective_timezone().into())]]),
            "transaction_isolation" => (vec![(name, text.clone())], vec![vec![Value::Utf8("read committed".into())]]),
            "await_token" => (vec![(name, text.clone().maybe())], vec![vec![self.state.effective_await_token().map_or(Value::Null, |token| Value::Utf8(token.into()))]]),
            "standard_conforming_strings" => (vec![(name, LogicalType::Scalar(ScalarType::Bool).maybe())], vec![vec![Value::Bool(self.state.parameter(name).is_some_and(|value| ["on", "true", "yes", "1"].contains(&value.to_ascii_lowercase().as_str())))]]),
            "latest_submitted_tx" => {
                let fields = vec![("tx_id", integer.clone().maybe()), ("system_time", instant.clone().maybe()), ("committed", LogicalType::Scalar(ScalarType::Bool).maybe()), ("error", LogicalType::Scalar(ScalarType::Transit).maybe()), ("await_token", text.clone().maybe())];
                let rows = self.state.last_submitted().map(|tx| {
                    let (system_time, committed) = match tx { LastSubmittedTx::Submitted(_) => (Value::Null, Value::Null), LastSubmittedTx::Executed(tx) => (timestamp(tx.tx_key().system_time), Value::Bool(tx.committed())) };
                    vec![Value::I64(tx.tx_id().0), system_time, committed, error.as_ref().map_or(Value::Null, |error| Value::Transit(error.as_ref().to_vec())), self.state.effective_await_token().map_or(Value::Null, |token| Value::Utf8(token.into()))]
                }).into_iter().collect();
                (fields, rows)
            }
            "latest_completed_txs" => {
                let fields = vec![("db_name", text.clone()), ("part", LogicalType::Scalar(ScalarType::I32)), ("tx_id", integer.clone().maybe()), ("system_time", instant.clone().maybe())];
                let rows = databases.iter().map(|(name, database)| {
                    let key = database.progress().current().tx_key;
                    vec![Value::Utf8(name.clone()), Value::I32(0), key.map_or(Value::Null, |key| Value::I64(key.tx_id.0)), key.map_or(Value::Null, |key| timestamp(key.system_time))]
                }).collect();
                (fields, rows)
            }
            "latest_submitted_msg_ids" | "latest_processed_msg_ids" => {
                let fields = vec![("db_name", text.clone()), ("part", LogicalType::Scalar(ScalarType::I32)), ("msg_id", integer.clone().maybe())];
                let rows = databases.iter().map(|(database_name, database)| {
                    let position = if name == "latest_submitted_msg_ids" { database.latest_submitted_position()? } else { database.progress().current().source_position };
                    Ok(vec![Value::Utf8(database_name.clone()), Value::I32(0), Value::I64(position.0)])
                }).collect::<Result<Vec<_>>>()?;
                (fields, rows)
            }
            _ => (vec![(name, text.maybe())], vec![vec![self.state.parameter(name).map_or(Value::Null, |value| Value::Utf8(value.into()))]]),
        };
        let mut binder = Binder::default();
        let schema = Schema::new(fields.into_iter().map(|(name, ty)| binder.field(name, None, ty)).collect::<Result<Vec<_>>>()?)?;
        QueryCursor::from_rows(schema, rows, context, reservation)
    }
}

pub fn transaction_settings(options: &TransactionOptions, parameters: &[Value], timezone: &str, session_timezone: &str, current_time: Timestamp, memory: &Arc<MemoryBudget>) -> Result<TransactionSettings> {
    let eval = |expression: &Expr, timezone: &str| evaluate(expression, &Schema::default(), &[], &EvalContext { parameters, current_time, timezone, outer_rows: &[] });
    let timezone_update = update(options.timezone.as_ref(), |expression| string(eval(expression, timezone)?, "TIMEZONE"))?;
    let timezone = match &timezone_update { OptionUpdate::Set(timezone) => rtdb_expr::normalize_timezone(timezone)?, OptionUpdate::Clear => session_timezone.to_owned(), OptionUpdate::Unchanged => timezone.to_owned() };
    let token = |expression: &Expr| string(eval(expression, &timezone)?, "Transaction token");
    let instant = |expression: &Expr| {
        let value = eval(expression, &timezone)?;
        if matches!(value, Value::Null | Value::Undefined) { Ok(None) } else { rtdb_expr::timestamp_micros(&value, &timezone).map(Some) }
    };
    let access = match &options.access {
        None => None,
        Some(ControlAccessOptions::ReadOnly(options)) => Some(AccessSettings::ReadOnly(ReadOnlySettings {
            snapshot_token: update(options.snapshot_token.as_ref(), token)?, snapshot_time: update(options.snapshot_time.as_ref(), instant)?,
            clock_time: update(options.clock_time.as_ref(), instant)?, await_token: update(options.await_token.as_ref(), token)?,
        })),
        Some(ControlAccessOptions::ReadWrite(options)) => Some(AccessSettings::ReadWrite(ReadWriteSettings {
            system_time: update(options.system_time.as_ref(), instant)?,
            user_metadata: update(options.metadata.as_ref(), |expression| match eval(expression, &timezone)? { Value::Null | Value::Undefined => Ok(None), value => UserMetadata::new(value, memory).map(Some) })?,
            asynchronous: update(options.asynchronous.as_ref(), |expression| match eval(expression, &timezone)? { Value::Null | Value::Undefined => Ok(None), Value::Bool(value) => Ok(Some(value)), _ => Err(invalid("ASYNC requires a boolean")) })?,
        })),
    };
    Ok(TransactionSettings { access, timezone: timezone_update })
}

fn update<T>(option: Option<&ControlValue>, evaluate: impl FnOnce(&Expr) -> Result<Option<T>>) -> Result<OptionUpdate<T>> {
    Ok(match option { None => OptionUpdate::Unchanged, Some(ControlValue::Default) => OptionUpdate::Clear,
        Some(ControlValue::Expression(expression)) => match evaluate(expression)? { Some(value) => OptionUpdate::Set(value), None => OptionUpdate::Clear },
    })
}
fn string(value: Value, option: &str) -> Result<Option<String>> {
    match value { Value::Null | Value::Undefined => Ok(None), Value::Utf8(value) => Ok(Some(value)), _ => Err(invalid(format!("{option} requires a string"))) }
}
fn command(tag: &str) -> ControlResult { ControlResult::Command { tag: tag.into(), transaction: None, parameter_status: None } }
fn timestamp(value: Timestamp) -> Value { Value::Timestamp { value: value.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) } }
fn size(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(|| invalid("Control allocation size overflow")) }
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("xtdb/invalid-transaction-option", message) }
fn cancelled() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb/query-cancelled", "Control execution was cancelled") }
