use std::sync::Arc;
use std::time::Duration;

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_expr::{EvalContext, EvaluationScope, evaluate_async};
use rtdb_plan::{Binder, ControlAccessOptions, ControlCommitMode, ControlStatement, ControlValue, CopyFormat, DiscardTarget, Expr, Identifier, Schema, TransactionOptions};
use rtdb_query::{QueryContext, QueryCursor};
use rtdb_tx::UserMetadata;
use rtdb_types::{Anomaly, AnomalyCategory, LogicalType, Result, ScalarType, TimeUnit, Timestamp, Value};
use tokio_util::sync::CancellationToken;

use crate::{AccessSettings, CommitMode, CommitResult, LastSubmittedTx, NativeSession, OptionUpdate, ReadOnlySettings, ReadWriteSettings, TransactionSettings, TransactionStatus};
use crate::node::HostedSourceReceipt;

enum SourceInspection {
    Owner(String),
    Barrier(HostedSourceReceipt),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ControlFrontend { #[default] Native, PostgreSql, Adbc, FlightSql }

#[derive(Clone, Copy, Debug, Default)]
pub struct ControlExecutionOptions {
    pub frontend: ControlFrontend,
    pub implicit_transaction: bool,
}

/// Owned control results retain their evaluation scope through transfer or protocol output.
/// When extracting a payload, retain `allocation` until that payload has another allocation owner.
pub enum ControlResult {
    Command { tag: String, transaction: Option<CommitResult>, parameter_status: Option<(String, String)>, allocation: Arc<EvaluationScope> },
    Rows(QueryCursor),
    Prepare { name: String, source: String, allocation: Arc<EvaluationScope> },
    Execute { name: String, arguments: Vec<Value>, reservation: Arc<MemoryReservation>, allocation: Arc<EvaluationScope> },
    CopyIn { table: Vec<Identifier>, format: Option<CopyFormat>, allocation: Arc<EvaluationScope> },
}

pub fn control_schema(statement: &ControlStatement) -> Result<Option<Schema>> {
    let ControlStatement::Show { name } = statement else { return Ok(None); };
    let text = LogicalType::Scalar(ScalarType::Utf8);
    let integer = LogicalType::Scalar(ScalarType::I64).maybe();
    let instant = LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) });
    let fields = match name.as_str() {
        "clock_time" => vec![(name.as_str(), instant)],
        "snapshot_token" | "timezone" | "transaction_isolation" | "rtdb_ordered_system_time" | "rtdb_bounded_retry" | "rtdb_source_barrier"
        | "rtdb_hosted_source_owner" => vec![(name.as_str(), text)],
        "rtdb_hosted_source_barrier" => vec![(name.as_str(), text), ("tx_id", integer), ("system_time", instant.maybe())],
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
        self.prepare_context_with_timezone(None, cancellation).await
    }

    /// Prepares on the current catalogue with the timezone captured by a gateway before a move.
    pub async fn prepare_context_with_timezone(&mut self, timezone: Option<&str>, cancellation: &CancellationToken) -> Result<Arc<QueryContext>> {
        cancellation.run_until_cancelled(async {
            let mut basis = self.state.prepare_basis().await?;
            if let Some(timezone) = timezone { basis.timezone = rtdb_expr::normalize_timezone(timezone)?; }
            Ok(self.service.query_context(self.state.database(), &basis, cancellation).await?.with_observer(crate::SessionService::execution_observer(self.service.as_ref())).with_tracer(self.service.node.tracer(), None))
        }).await.ok_or_else(cancelled)?
    }

    /// Prepares against a statement's captured default database without changing the session's current database.
    /// `cancellation` must descend from `state().cancellation()`.
    pub async fn prepare_context_for(&mut self, database: &str, cancellation: &CancellationToken) -> Result<Arc<QueryContext>> {
        cancellation.run_until_cancelled(async {
            let basis = self.state.prepare_basis().await?;
            Ok(self.service.query_context(database, &basis, cancellation).await?.with_observer(crate::SessionService::execution_observer(self.service.as_ref())).with_tracer(self.service.node.tracer(), None))
        }).await.ok_or_else(cancelled)?
    }

    /// Opens a checked read against a statement's captured default database and the session's read basis.
    /// `cancellation` must descend from `state().cancellation()`.
    pub async fn query_context_for(&mut self, database: &str, cancellation: &CancellationToken) -> Result<Arc<QueryContext>> {
        cancellation.run_until_cancelled(async {
            let basis = self.state.read_basis().await?;
            Ok(self.service.query_context(database, &basis, cancellation).await?.with_observer(crate::SessionService::execution_observer(self.service.as_ref())).with_tracer(self.service.node.tracer(), None))
        }).await.ok_or_else(cancelled)?
    }

    /// Revalidates and awaits this session before opening the supplied captured basis.
    /// `cancellation` must descend from `state().cancellation()`; this does not resolve the current transaction's access mode.
    pub async fn query_context_at(&mut self, basis: &crate::QueryBasis, cancellation: &CancellationToken) -> Result<Arc<QueryContext>> {
        cancellation.run_until_cancelled(async {
            self.state.prepare_basis().await?;
            Ok(self.service.query_context(self.state.database(), basis, cancellation).await?.with_observer(crate::SessionService::execution_observer(self.service.as_ref())).with_tracer(self.service.node.tracer(), None))
        }).await.ok_or_else(cancelled)?
    }

    /// `cancellation` must descend from `state().cancellation()`.
    /// Unchecked reads are for EXECUTE arguments and the frontend's exact pgjdbc type probe.
    pub async fn query_context_with(&mut self, cancellation: &CancellationToken, checked: bool) -> Result<Arc<QueryContext>> {
        cancellation.run_until_cancelled(async {
            let basis = if checked { self.state.read_basis().await? } else { self.state.unchecked_read_basis().await? };
            Ok(self.service.query_context(self.state.database(), &basis, cancellation).await?.with_observer(crate::SessionService::execution_observer(self.service.as_ref())).with_tracer(self.service.node.tracer(), None))
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

    /// Revalidates the session and admits the arguments for a frontend-owned SQL PREPARE.
    /// A failed transaction or an argument reservation failure rejects preparation.
    /// The cancellation token must descend from this session.
    /// Keep the returned scope through preparation of the immediate child.
    pub async fn admit_frontend_prepare(&mut self, arguments: &[Value], cancellation: &CancellationToken) -> Result<Arc<EvaluationScope>> {
        cancellation.run_until_cancelled(async {
            self.state.revalidate().await?;
            self.check_control_transaction(false)?;
            self.admit_control_arguments(arguments)
        }).await.ok_or_else(cancelled)?
    }

    fn check_control_transaction(&self, allows_failed: bool) -> Result<()> {
        if self.state.status() == TransactionStatus::Failed && !allows_failed {
            return Err(Anomaly::incorrect("xtdb/tx-failed", "current transaction is aborted, commands ignored until ROLLBACK is received"));
        }
        Ok(())
    }

    fn admit_control_arguments(&self, arguments: &[Value]) -> Result<Arc<EvaluationScope>> {
        let allocation = Arc::new(EvaluationScope::new(self.service.node.memory())?);
        allocation.reserve(arguments.iter().try_fold(256usize, |bytes, value| size(bytes, value.retained_bytes()?))?)?;
        Ok(allocation)
    }

    async fn control(&mut self, statement: &ControlStatement, arguments: &[Value], cancellation: &CancellationToken, options: ControlExecutionOptions) -> Result<ControlResult> {
        self.state.revalidate().await?;
        if matches!(options.frontend, ControlFrontend::Adbc | ControlFrontend::FlightSql) && matches!(statement, ControlStatement::Begin(_) | ControlStatement::Commit(_) | ControlStatement::Rollback) {
            return Err(Anomaly::incorrect("xtdb.adbc/tx-control-as-sql", "Use the transaction API to begin, commit, or roll back transactions"));
        }
        let failed_inspection = matches!(options.frontend, ControlFrontend::Adbc | ControlFrontend::FlightSql)
            && matches!(statement, ControlStatement::Show { .. }) && !statement.requires_read_transaction();
        self.check_control_transaction(matches!(statement, ControlStatement::Rollback) || failed_inspection)?;
        if arguments.len() < statement.parameter_count() { return Err(Anomaly::incorrect("xtdb/missing-parameter", "Control statement has fewer parameters than its expressions")); }
        let allocation = self.admit_control_arguments(arguments)?;
        let mut admitted_arguments = arguments.to_vec();
        admitted_arguments.iter_mut().for_each(rtdb_columnar::admit_execution_value);
        let arguments = admitted_arguments.as_slice();
        let needs_catalog = match statement {
            ControlStatement::Begin(options) | ControlStatement::SetTransaction(options) => options.expressions().iter().any(|expr| control_requires_catalog(expr, arguments)),
            ControlStatement::SetTimeZone(expr) | ControlStatement::SetAwaitToken(expr) | ControlStatement::SetParameter { value: expr, .. } => control_requires_catalog(expr, arguments),
            _ => false,
        };
        let catalog_context = if needs_catalog {
            let basis = self.state.inspection_basis()?;
            Some(self.service.query_context(self.state.database(), &basis, cancellation).await?)
        } else { None };
        let catalog = catalog_context.as_ref().map(|context| &context.catalog as &dyn rtdb_expr::PgCatalogLookup);
        match statement {
            ControlStatement::Begin(options) => {
                self.state.check_begin()?;
                let settings = transaction_settings(options, arguments, self.state.effective_timezone(), self.state.session_timezone(), self.service.node.query_instant(), self.service.node.memory(), &allocation, catalog).await?;
                self.state.begin_with(settings)?;
                Ok(command("BEGIN", allocation))
            }
            ControlStatement::SetTransaction(options) => {
                let settings = transaction_settings(options, arguments, self.state.effective_timezone(), self.state.session_timezone(), self.service.node.query_instant(), self.service.node.memory(), &allocation, catalog).await?;
                self.state.set_transaction(settings)?; Ok(command("SET TRANSACTION", allocation))
            }
            ControlStatement::Commit(mode) => {
                let result = self.state.commit_tx(mode.map(|mode| match mode { ControlCommitMode::Sync => CommitMode::Sync, ControlCommitMode::Async => CommitMode::Async })).await?;
                if let CommitResult::Executed(tx) = &result { if let Some(error) = tx.error() { return Err(error.clone()); } }
                Ok(ControlResult::Command { tag: "COMMIT".into(), transaction: Some(result), parameter_status: None, allocation })
            }
            ControlStatement::Rollback => { self.state.rollback_tx()?; Ok(command("ROLLBACK", allocation)) }
            ControlStatement::SetSessionCharacteristics(access) => { self.state.set_session_characteristics(*access)?; Ok(command("SET SESSION CHARACTERISTICS", allocation)) }
            ControlStatement::SetTimeZone(expression) => {
                let value = self.control_value(expression, arguments, &allocation, catalog).await?;
                self.state.set_timezone(string(value, "TIME ZONE")?.ok_or_else(|| invalid("TIME ZONE requires a string"))?)?;
                allocation.reserve(self.state.effective_timezone().len())?;
                Ok(ControlResult::Command { tag: "SET TIME ZONE".into(), transaction: None, parameter_status: Some(("TimeZone".into(), self.state.effective_timezone().into())), allocation })
            }
            ControlStatement::SetAwaitToken(expression) => {
                let value = self.control_value(expression, arguments, &allocation, catalog).await?;
                self.state.set_await_token(string(value, "AWAIT_TOKEN")?)?; Ok(command("SET AWAIT_TOKEN", allocation))
            }
            ControlStatement::SetParameter { name, value } => {
                let value = match value {
                    Expr::DurationLiteral(value) => {
                        allocation.reserve(rtdb_types::DurationLiteral::MAX_TEXT_LEN)?;
                        Value::Utf8(value.to_iso_string())
                    }
                    value => self.control_value(value, arguments, &allocation, catalog).await?,
                };
                allocation.reserve(rtdb_expr::text_allocation_bound(&value)?)?;
                allocation.reserve(name.len().checked_mul(4).ok_or_else(|| invalid("SET name allocation overflow"))?)?;
                let value = if matches!(value, Value::Null | Value::Undefined) { None } else {
                    let eval = EvalContext { parameters: rtdb_expr::Parameters::Values(arguments), parameter_types: None, current_time: self.service.node.query_instant(), timezone: self.state.effective_timezone(), outer_rows: &[], snapshot_token: None, allocation: Some(&allocation), catalog };
                    let Value::Utf8(value) = rtdb_expr::scalar_call("str", &[value], &eval)? else { return Err(invalid("Session parameter could not be formatted")); };
                    Some(value)
                };
                self.state.set_parameter(name.clone(), value.clone())?;
                Ok(ControlResult::Command { tag: "SET".into(), transaction: None, parameter_status: value.map(|value| (name.clone(), value)), allocation })
            }
            ControlStatement::SetRole => Ok(command("SET ROLE", allocation)),
            ControlStatement::Discard(target) => {
                if *target == DiscardTarget::All { self.state.reset()?; }
                Ok(command(match target { DiscardTarget::All => "DISCARD ALL", DiscardTarget::Plans => "DISCARD PLANS", DiscardTarget::Temporary => "DISCARD TEMP", DiscardTarget::Sequences => "DISCARD SEQUENCES" }, allocation))
            }
            ControlStatement::Show { name } => {
                let source = if name == "rtdb_hosted_source_owner" {
                    let history = self.service.confirm_hosted_source_owner(self.state.database(), cancellation).await?;
                    Some(SourceInspection::Owner(history))
                } else if matches!(name.as_str(), "rtdb_source_barrier" | "rtdb_hosted_source_barrier") {
                    if self.state.status() != TransactionStatus::Idle && !options.implicit_transaction {
                        return Err(Anomaly::incorrect("rtdb/source-barrier-in-transaction", "Complete the transaction before awaiting a source barrier"));
                    }
                    self.service.await_durable_source(
                        self.state.database(), name == "rtdb_hosted_source_barrier", cancellation,
                    ).await?.map(SourceInspection::Barrier)
                } else {
                    None
                };
                self.show(name, statement.requires_read_transaction(), source, cancellation).await.map(ControlResult::Rows)
            },
            ControlStatement::Prepare { name, source } => {
                allocation.reserve(size(name.len(), source.len())?)?;
                Ok(ControlResult::Prepare { name: name.clone(), source: source.clone(), allocation })
            }
            ControlStatement::Execute { name, arguments: expressions } => {
                let context = self.query_context_with(cancellation, false).await?;
                let bound = arguments.iter().try_fold(8192usize, |bytes, value| size(bytes, value.retained_bytes()?))?;
                let _scratch = context.memory.reserve(size(bound, expressions.len().checked_mul(8192).ok_or_else(|| invalid("EXECUTE allocation size overflow"))?)?)?;
                let eval = EvalContext { parameters: rtdb_expr::Parameters::Values(arguments), parameter_types: None, current_time: context.current_time, timezone: &context.timezone, outer_rows: &[], snapshot_token: context.snapshot_token(), allocation: Some(&allocation), catalog: Some(&context.catalog) };
                let mut arguments = Vec::with_capacity(expressions.len());
                for expression in expressions { arguments.push(evaluate_async(expression, &Schema::default(), &[], &eval).await?); }
                let actual = arguments.iter().try_fold(0usize, |bytes, value| size(bytes, value.retained_bytes()?))?;
                let reservation = context.memory.reserve(size(actual, size(name.len(), 256)?)?)?;
                allocation.reserve(name.len())?;
                Ok(ControlResult::Execute { name: name.clone(), arguments, reservation, allocation })
            }
            ControlStatement::CopyOut { .. } => Err(Anomaly::unsupported(
                "xtdb/copy-out-frontend", "COPY TO STDOUT uses the PgWire protocol",
            )),
            ControlStatement::CopyIn { table, format } => {
                allocation.reserve(table.iter().try_fold(256usize, |bytes, identifier| size(bytes, size(identifier.text.len(), 128)?))?)?;
                Ok(ControlResult::CopyIn { table: table.clone(), format: *format, allocation })
            }
            ControlStatement::AttachDatabase { name, configuration_yaml, source } => {
                self.check_database_control("attach", name, options)?;
                let configuration = configuration_yaml.as_deref().map(rtdb_format::config::decode_database_yaml).transpose()
                    .map_err(|error| Anomaly::incorrect("xtdb/invalid-database-config", format!("Invalid database config in `ATTACH DATABASE`: {}", error.message.as_deref().unwrap_or("null")))
                        .with_context("sql", source.clone().into()))?
                    .unwrap_or_else(rtdb_format::config::default_database_config);
                let result = self.service.node.attach_database(name.clone(), configuration, self.service.identity.as_ref(), cancellation).await?;
                if let Some(error) = result.error() { return Err(error.clone()); }
                Ok(ControlResult::Command { tag: "ATTACH DATABASE".into(), transaction: Some(CommitResult::Executed(result)), parameter_status: None, allocation })
            }
            ControlStatement::DetachDatabase { name } => {
                self.check_database_control("detach", name, options)?;
                let result = self.service.node.detach_database(name.clone(), self.service.identity.as_ref(), cancellation).await?;
                if let Some(error) = result.error() { return Err(error.clone()); }
                Ok(ControlResult::Command { tag: "DETACH DATABASE".into(), transaction: Some(CommitResult::Executed(result)), parameter_status: None, allocation })
            }
        }
    }

    async fn control_value(&self, expression: &Expr, parameters: &[Value], allocation: &EvaluationScope, catalog: Option<&dyn rtdb_expr::PgCatalogLookup>) -> Result<Value> {
        evaluate_async(expression, &Schema::default(), &[], &EvalContext { parameters: rtdb_expr::Parameters::Values(parameters), parameter_types: None, current_time: self.service.node.query_instant(), timezone: self.state.effective_timezone(), outer_rows: &[], snapshot_token: None, allocation: Some(allocation), catalog }).await
    }

    fn check_database_control(&self, action: &str, name: &str, options: ControlExecutionOptions) -> Result<()> {
        if self.state.database() != "xtdb" { return Err(Anomaly::incorrect(format!("xtdb.pgwire/{action}-db-on-secondary"), format!("Can only {action} databases when connected to the primary 'xtdb' database.")).with_context("db", self.state.database().into())); }
        if self.state.status() != TransactionStatus::Idle && !options.implicit_transaction { return Err(Anomaly::incorrect(format!("xtdb.pgwire/{action}-db-in-tx"), format!("Cannot {action} a database in a transaction.")).with_context("db-name", name.into())); }
        Ok(())
    }

    async fn show(&mut self, name: &str, checked: bool, source: Option<SourceInspection>, cancellation: &CancellationToken) -> Result<QueryCursor> {
        let basis = if checked { self.state.read_basis().await? } else { self.state.inspection_basis()? };
        let context = self.service.query_context(self.state.database(), &basis, cancellation).await?
            .with_observer(crate::SessionService::execution_observer(self.service.as_ref())).with_tracer(self.service.node.tracer(), None);
        let mut bound = 8192usize;
        if let Some(value) = self.state.parameter(name) { bound = size(bound, value.len().checked_mul(4).ok_or_else(|| invalid("SHOW allocation size overflow"))?)?; }
        if let Some(token) = if name == "latest_submitted_tx" { self.state.last_submitted_await_token() } else { self.state.effective_await_token() } { bound = size(bound, token.len().checked_mul(4).ok_or_else(|| invalid("SHOW token size overflow"))?)?; }
        let snapshot_token = if name == "snapshot_token" { Some(basis.snapshot_token()) } else { None };
        if let Some(token) = &snapshot_token { bound = size(bound, token.len().checked_mul(4).ok_or_else(|| invalid("SHOW token size overflow"))?)?; }
        let error = match (name, self.state.last_submitted()) { ("latest_submitted_tx", Some(LastSubmittedTx::Executed(tx))) => tx.encoded_error()?, _ => None };
        if let Some(error) = &error { bound = size(bound, error.as_ref().len().checked_mul(4).ok_or_else(|| invalid("SHOW error size overflow"))?)?; }
        let databases = if ["latest_completed_txs", "latest_submitted_msg_ids", "latest_processed_msg_ids"].contains(&name) {
            crate::SessionService::await_client_token(self.service.as_ref(), self.state.effective_await_token(), Duration::from_secs(60), cancellation).await?;
            self.service.visible_databases()?
        } else { Default::default() };
        for name in databases.keys() { bound = size(bound, size(8192, name.len().checked_mul(4).ok_or_else(|| invalid("SHOW database name size overflow"))?)?)?; }
        let reservation = context.memory.reserve(bound)?;
        let text = LogicalType::Scalar(ScalarType::Utf8);
        let integer = LogicalType::Scalar(ScalarType::I64);
        let instant = LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) });
        let (fields, rows) = match name {
            "snapshot_token" => (vec![(name, text.clone())], vec![vec![Value::Utf8(snapshot_token.unwrap_or_default())]]),
            "clock_time" => {
                let zone = Some(self.state.effective_timezone().to_owned());
                (vec![(name, LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: zone.clone() }))],
                 vec![vec![Value::Timestamp { value: basis.current_time.as_micros()?.0, unit: TimeUnit::Microsecond, timezone: zone }]])
            }
            "timezone" => (vec![(name, text.clone())], vec![vec![Value::Utf8(self.state.effective_timezone().into())]]),
            "rtdb_ordered_system_time" | "rtdb_bounded_retry" | "rtdb_source_barrier" => (vec![(name, text.clone())], vec![vec![Value::Utf8("1".into())]]),
            "rtdb_hosted_source_owner" => {
                let Some(SourceInspection::Owner(history)) = source else {
                    return Err(Anomaly::fault("rtdb.node/hosted-source-result", "Hosted source confirmation has no history"));
                };
                (vec![(name, text.clone())], vec![vec![Value::Utf8(history)]])
            },
            "rtdb_hosted_source_barrier" => {
                let Some(SourceInspection::Barrier(receipt)) = source else {
                    return Err(Anomaly::fault("rtdb.node/hosted-source-result", "Hosted source barrier has no receipt"));
                };
                let fields = vec![(name, text.clone()), ("tx_id", integer.clone().maybe()), ("system_time", instant.clone().maybe())];
                let (offset, time) = receipt.applied.map_or((Value::Null, Value::Null), |key| {
                    (Value::I64(key.tx_id.0), timestamp(key.system_time))
                });
                (fields, vec![vec![Value::Utf8(receipt.history), offset, time]])
            },
            "transaction_isolation" => (vec![(name, text.clone())], vec![vec![Value::Utf8("read committed".into())]]),
            "await_token" => (vec![(name, text.clone().maybe())], vec![vec![self.state.effective_await_token().map_or(Value::Null, |token| Value::Utf8(token.into()))]]),
            "standard_conforming_strings" => (vec![(name, LogicalType::Scalar(ScalarType::Bool).maybe())], vec![vec![Value::Bool(self.state.parameter(name).is_some_and(|value| ["on", "true", "yes", "1"].contains(&value.to_ascii_lowercase().as_str())))]]),
            "latest_submitted_tx" => {
                let fields = vec![("tx_id", integer.clone().maybe()), ("system_time", instant.clone().maybe()), ("committed", LogicalType::Scalar(ScalarType::Bool).maybe()), ("error", LogicalType::Scalar(ScalarType::Transit).maybe()), ("await_token", text.clone().maybe())];
                let rows = self.state.last_submitted().map(|tx| {
                    let (system_time, committed) = match tx { LastSubmittedTx::Submitted(_) => (Value::Null, Value::Null), LastSubmittedTx::Executed(tx) => (timestamp(tx.tx_key().system_time), Value::Bool(tx.committed())) };
                    vec![Value::I64(tx.tx_id().0), system_time, committed, error.as_ref().map_or(Value::Null, |error| Value::Transit(error.as_ref().to_vec())), self.state.last_submitted_await_token().map_or(Value::Null, |token| Value::Utf8(token.into()))]
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

/// Retain `allocation` until returned settings are installed into charged session state or dropped.
/// `catalog` must be the captured basis when an expression resolves relation names.
/// Evaluation stays on the caller's future; dropping it cancels SLEEP without installing partial settings.
pub async fn transaction_settings(options: &TransactionOptions, parameters: &[Value], timezone: &str, session_timezone: &str, current_time: impl Into<rtdb_types::Instant>, memory: &Arc<MemoryBudget>, allocation: &EvaluationScope, catalog: Option<&dyn rtdb_expr::PgCatalogLookup>) -> Result<TransactionSettings> {
    let initial = EvalContext { parameters: rtdb_expr::Parameters::Values(parameters), parameter_types: None, current_time: current_time.into(), timezone, outer_rows: &[], snapshot_token: None, allocation: Some(allocation), catalog };
    let timezone_update = update(options.timezone.as_ref(), &initial, |value| string(value, "TIMEZONE")).await?;
    let timezone = match &timezone_update { OptionUpdate::Set(timezone) => rtdb_expr::normalize_timezone(timezone)?, OptionUpdate::Clear => session_timezone.to_owned(), OptionUpdate::Unchanged => timezone.to_owned() };
    let eval = EvalContext { timezone: &timezone, ..initial };
    let token = |value| string(value, "Transaction token");
    let instant = |value| {
        if matches!(value, Value::Null | Value::Undefined) { Ok(None) } else { rtdb_expr::timestamp_micros(&value, &timezone).map(Some) }
    };
    let access = match &options.access {
        None => None,
        Some(ControlAccessOptions::ReadOnly(options)) => Some(AccessSettings::ReadOnly(ReadOnlySettings {
            snapshot_token: update(options.snapshot_token.as_ref(), &eval, token).await?, snapshot_time: update(options.snapshot_time.as_ref(), &eval, instant).await?,
            clock_time: clock_update(options.clock_time.as_ref(), &eval).await?, await_token: update(options.await_token.as_ref(), &eval, token).await?,
        })),
        Some(ControlAccessOptions::ReadWrite(options)) => Some(AccessSettings::ReadWrite(ReadWriteSettings {
            system_time: update(options.system_time.as_ref(), &eval, instant).await?,
            user_metadata: update(options.metadata.as_ref(), &eval, |value| match value { Value::Null | Value::Undefined => Ok(None), value => UserMetadata::new(value, memory).map(Some) }).await?,
            asynchronous: update(options.asynchronous.as_ref(), &eval, |value| match value { Value::Null | Value::Undefined => Ok(None), Value::Bool(value) => Ok(Some(value)), _ => Err(invalid("ASYNC requires a boolean")) }).await?,
        })),
    };
    Ok(TransactionSettings { access, timezone: timezone_update })
}

async fn update<T>(option: Option<&ControlValue>, context: &EvalContext<'_>, convert: impl FnOnce(Value) -> Result<Option<T>>) -> Result<OptionUpdate<T>> {
    Ok(match option { None => OptionUpdate::Unchanged, Some(ControlValue::Default) => OptionUpdate::Clear,
        Some(ControlValue::Expression(expression)) => match convert(evaluate_async(expression, &Schema::default(), &[], context).await?)? { Some(value) => OptionUpdate::Set(value), None => OptionUpdate::Clear },
        Some(ControlValue::TimestampLiteral(_)) => return Err(invalid("An unadmitted timestamp literal requires CLOCK_TIME resolution")),
    })
}
async fn clock_update(option: Option<&ControlValue>, context: &EvalContext<'_>) -> Result<OptionUpdate<rtdb_types::Instant>> {
    if let Some(ControlValue::TimestampLiteral(text)) = option {
        return rtdb_types::parse_sql_timestamp_instant(text,context.timezone).map(OptionUpdate::Set);
    }
    update(option,context,|value| if matches!(value,Value::Null | Value::Undefined) { Ok(None) }
        else { rtdb_expr::query_clock_instant(&value,context.timezone).map(Some) }).await
}
fn string(value: Value, option: &str) -> Result<Option<String>> {
    match value { Value::Null | Value::Undefined => Ok(None), Value::Utf8(value) => Ok(Some(value)), _ => Err(invalid(format!("{option} requires a string"))) }
}
fn control_requires_catalog(expression: &Expr, arguments: &[Value]) -> bool {
    fn regclass(value: &Value) -> bool { match value {
        Value::RegClass(_) => true,
        Value::List(values) | Value::Set(values) | Value::FixedSizeList(values) => values.iter().any(regclass),
        Value::Struct(fields) => fields.values().any(regclass),
        Value::Map { entries, .. } => entries.iter().any(|(key, value)| regclass(key) || regclass(value)),
        _ => false,
    } }
    if rtdb_expr::requires_pg_catalog(expression) { return true; }
    let mut required = false;
    expression.visit(&mut |expr| {
        if let Expr::Parameter { index, .. } = expr { required |= arguments.get(*index).is_some_and(regclass); }
    });
    required
}
fn command(tag: &str, allocation: Arc<EvaluationScope>) -> ControlResult { ControlResult::Command { tag: tag.into(), transaction: None, parameter_status: None, allocation } }
fn timestamp(value: Timestamp) -> Value { Value::Timestamp { value: value.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) } }
fn size(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(|| invalid("Control allocation size overflow")) }
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("xtdb/invalid-transaction-option", message) }
fn cancelled() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb/query-cancelled", "Control execution was cancelled") }
