use std::sync::Arc;

use rtdb_columnar::{MemoryReservation, OwnedBatch, Selection};
use rtdb_plan::{CatalogLookups, ColumnId, Field, RecordingCatalog, Schema, SqlNotice};
use rtdb_query::{PreparedQuery, QueryContext, QueryCursor, RuntimeSchema};
use rtdb_sql::{DeferredStatementSyntax, ParsedStatement, PrepareOptions, StatementKind, StatementSyntaxBatch};
use rtdb_tx::{SqlArguments, TxOp};
use rtdb_types::{Anomaly, AnomalyCategory, LogicalType, MemoryBudget, PgType, Result, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResultSignaturePolicy { Dynamic, Exact, PostgreSql }

struct PreparedData {
    source: String,
    options: PrepareOptions,
    parameters: Vec<LogicalType>,
    notices: Vec<SqlNotice>,
    ordered_index_command_tag: Option<&'static str>,
    snapshot_selector: rtdb_plan::QuerySettings,
    result: PreparedResult,
    policy: ResultSignaturePolicy,
    _reservation: Arc<MemoryReservation>,
    plan: std::sync::Mutex<Option<CachedPlan>>,
}
enum PreparedResult { Query { schema: Schema, pg_types: Vec<PgType>, parameter_projection: bool }, Dml }

/// An immutable SQL preparation; each query execution binds against its supplied snapshot.
/// Session access checks and read-basis capture precede this API and remain the caller's obligation.
#[derive(Clone)]
pub struct PreparedSql(Arc<PreparedData>);

fn deep<T>(source: &str, prepare: impl FnOnce() -> T) -> T {
    const DEEP_SOURCE_BYTES: usize = 4096;
    if source.len() >= DEEP_SOURCE_BYTES {
        stacker::grow(64 << 20, prepare)
    } else {
        stacker::maybe_grow(512 << 10, 8 << 20, prepare)
    }
}

#[cfg(test)]
mod stack_tests {
    #[test]
    fn short_sql_preparation_checks_the_callers_remaining_stack() {
        stacker::grow(128 << 10, || {
            super::deep("SELECT 1", || assert!(stacker::remaining_stack().unwrap() > 512 << 10));
        });
    }
}

impl PreparedSql {
    pub fn prepare(source: &str, context: &Arc<QueryContext>, mut options: PrepareOptions, policy: ResultSignaturePolicy) -> Result<Self> {
        let source_reservation = admit_source(source, context, &mut options)?;
        let syntax = admit_sql_syntax(source, &context.memory)?;
        let parsed = match one_statement(source, &context.memory) {
            Ok(parsed) => parsed,
            Err(error) => {
                drop((syntax, source_reservation));
                return Err(statement_syntax_error(source, error, &context.memory));
            }
        };
        deep(source, || Self::prepare_parsed(parsed, context, options, policy, false, false, source_reservation))
    }

    pub fn prepare_postgres(source: &str, context: &Arc<QueryContext>, search_path: Vec<String>, parameter_oids: &[i32]) -> Result<Self> {
        let syntax = rtdb_sql::parse_syntax_in(source, &context.memory)?;
        Self::prepare_postgres_syntax(syntax, context, search_path, parameter_oids)
    }

    /// Requires syntax parsed with the same budget as the context.
    /// Returns the first source-order resolution error before checking that the input contains exactly one statement.
    /// Query parameter OIDs are positional. Missing OIDs or OIDs without a logical type leave query typing dynamic.
    pub fn prepare_postgres_syntax(
        syntax: StatementSyntaxBatch<'_>, context: &Arc<QueryContext>, search_path: Vec<String>, parameter_oids: &[i32],
    ) -> Result<Self> {
        let parsed = exactly_one_statement(syntax.resolve_in(&context.memory)?)?;
        Self::prepare_postgres_parsed(parsed, context, search_path, parameter_oids)
    }

    /// Resolves and prepares the selected statement in the reached query context.
    pub fn prepare_postgres_statement(syntax: DeferredStatementSyntax<'_>, context: &Arc<QueryContext>, search_path: Vec<String>, parameter_oids: &[i32]) -> Result<Self> {
        let parsed = syntax.resolve_in(&context.memory)?;
        Self::prepare_postgres_parsed(parsed, context, search_path, parameter_oids)
    }

    fn prepare_postgres_parsed(parsed: ParsedStatement<'_>, context: &Arc<QueryContext>, search_path: Vec<String>, parameter_oids: &[i32]) -> Result<Self> {
        let source = parsed.source();
        deep(source, || Self::prepare_postgres_parsed_at(parsed, context, search_path, parameter_oids))
    }

    fn prepare_postgres_parsed_at(parsed: ParsedStatement<'_>, context: &Arc<QueryContext>, search_path: Vec<String>, parameter_oids: &[i32]) -> Result<Self> {
        let source = parsed.source();
        if parsed.kind() == StatementKind::Dml {
            let mut options = PrepareOptions { search_path, ..Default::default() };
            let source_reservation = admit_source(source, context, &mut options)?;
            return Self::prepare_parsed(parsed, context, options, ResultSignaturePolicy::PostgreSql, false, parameter_oids.is_empty(), source_reservation);
        }
        let count = parsed.parameter_count();
        let _parameters = context.memory.reserve(count.checked_mul(1024).and_then(|n| n.checked_add(1024)).ok_or_else(size_error)?)?;
        let mut options = PrepareOptions { search_path, ..Default::default() };
        let mut dynamic = false;
        for index in 0..count {
            match PgType::from_oid(parameter_oids.get(index).copied().unwrap_or(0)).and_then(PgType::logical_type) {
                Some(ty) => { options.parameter_types.insert(index, ty.maybe()); }
                None => { dynamic = true; }
            }
        }
        let source_reservation = admit_source(source, context, &mut options)?;
        Self::prepare_parsed(parsed, context, options, ResultSignaturePolicy::PostgreSql, dynamic, parameter_oids.is_empty(), source_reservation)
    }

    fn prepare_parsed(
        parsed: ParsedStatement<'_>, context: &Arc<QueryContext>, options: PrepareOptions, policy: ResultSignaturePolicy,
        dynamic_pg: bool, dml_notices: bool, source_reservation: Arc<MemoryReservation>,
    ) -> Result<Self> {
        let source = parsed.source();
        let count = parsed.parameter_count();
        let ordered_index_command_tag = parsed.ordered_index_command_tag();
        let snapshot_selector = parsed.snapshot_selector()?;
        if options.parameter_types.keys().any(|index| *index >= count) { return Err(parameter_error("Declared parameter lies outside the statement")); }
        let parameter_bytes = options.parameter_types.values().try_fold(count.checked_mul(1024).and_then(|n| n.checked_add(1024)).ok_or_else(size_error)?, |bytes, ty| bytes.checked_add(ty.retained_bytes()?).ok_or_else(size_error))?;
        let _parameters = context.memory.reserve(parameter_bytes)?;
        let mut parameters = (0..count).map(|index| options.parameter_types.get(&index).cloned().unwrap_or(LogicalType::Null)).collect::<Vec<_>>();
        let mut notices = Vec::new();
        let result = match parsed.kind() {
            StatementKind::Dml => {
                if dml_notices { notices = parsed.bind(&context.catalog, &options).map(|prepared| prepared.notices).unwrap_or_default(); }
                PreparedResult::Dml
            }
            StatementKind::Query => {
                let prepared = parsed.bind(&context.catalog, &options)?;
                for (index, ty) in &prepared.parameter_types {
                    if !options.parameter_types.contains_key(index) { parameters[*index] = ty.clone(); }
                }
                notices = prepared.notices.clone();
                let query = PreparedQuery::from_prepared(prepared, &context.memory)?;
                let schema = if policy == ResultSignaturePolicy::PostgreSql && !dynamic_pg {
                    query.result_schema(context)?.schema().clone()
                } else {
                    query.schema().clone()
                };
                let pg_types = schema.fields().iter().map(|field| if dynamic_pg || policy != ResultSignaturePolicy::PostgreSql { Ok(PgType::Default) } else { PgType::from_logical(&field.data_type) }).collect::<Result<_>>()?;
                PreparedResult::Query { schema, pg_types, parameter_projection: query.is_parameter_projection() }
            }
            StatementKind::Session => return Err(session_statement(source, &options, &context.memory)),
        };
        let source = source.to_owned();
        let bytes = retained_bytes(&source, &options, &parameters, &notices, &result)?;
        let bytes = bytes.checked_add(snapshot_selector.retained_bytes()?).ok_or_else(size_error)?;
        let reservation = context.memory.reserve(bytes)?;
        drop(source_reservation);
        Ok(Self(Arc::new(PreparedData { source, options, parameters, notices, ordered_index_command_tag, snapshot_selector, result, policy, _reservation: reservation, plan: Default::default() })))
    }

    pub fn source(&self) -> &str { &self.0.source }
    pub fn parameter_count(&self) -> usize { self.0.parameters.len() }
    pub fn parameter_types(&self) -> &[LogicalType] { &self.0.parameters }
    /// Planning warnings in source traversal order; repeated occurrences are retained.
    /// Execution-time rebinding does not append warnings to this immutable preparation.
    pub fn notices(&self) -> &[SqlNotice] { &self.0.notices }
    pub fn ordered_index_command_tag(&self) -> Option<&'static str> { self.0.ordered_index_command_tag }
    pub fn is_dml(&self) -> bool { matches!(&self.0.result, PreparedResult::Dml) }
    pub fn prepared_schema(&self) -> Option<&Schema> { match &self.0.result { PreparedResult::Query { schema, .. } => Some(schema), PreparedResult::Dml => None } }
    pub fn postgres_types(&self) -> Option<&[PgType]> { match &self.0.result { PreparedResult::Query { pg_types, .. } => Some(pg_types), PreparedResult::Dml => None } }

    /// ADBC parameters are positional, have empty field names, and publish unknown nullable types.
    pub fn parameter_schema(&self) -> Result<Schema> {
        Schema::new((0..self.parameter_count()).map(|index| Ok(Field { id: ColumnId(u32::try_from(index).map_err(|_| size_error())?), name: String::new(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Null })).collect::<Result<_>>()?)
    }

    /// Returns the current Arrow signature from supplied physical parameter fields without opening vectors.
    pub fn result_schema(&self, context: &Arc<QueryContext>, argument_types: &[LogicalType]) -> Result<Arc<RuntimeSchema>> {
        deep(self.source(), || self.bind(context, argument_types, false))?.result_schema(context)
    }

    pub async fn open_query(&self, context: Arc<QueryContext>, arguments: Vec<Value>) -> Result<QueryCursor> {
        let bound = arguments.iter().try_fold(1024usize, |bytes, value| bytes.checked_add(value.retained_bytes()?.checked_mul(8).ok_or_else(size_error)?).ok_or_else(size_error))?;
        let _types = context.memory.reserve(bound)?;
        let types = arguments.iter().map(Value::logical_type).collect::<Vec<_>>();
        self.open_query_typed(context, arguments, &types).await
    }

    /// Opens one argument row using its vector field types, preserving typed nulls and empty containers.
    /// Unsigned storage types and values enter execution at the corresponding signed width.
    pub async fn open_query_typed(&self, context: Arc<QueryContext>, arguments: Vec<Value>, types: &[LogicalType]) -> Result<QueryCursor> {
        self.open_query_batches(context, arguments, types, false).await
    }

    /// Opens Arrow batches directly for a client that consumes Arrow.
    pub async fn open_query_columns_typed(&self, context: Arc<QueryContext>, arguments: Vec<Value>, types: &[LogicalType]) -> Result<QueryCursor> {
        self.open_query_batches(context, arguments, types, true).await
    }

    pub async fn try_open_query_arrow(&self, context: Arc<QueryContext>, arguments: &OwnedBatch, types: &[LogicalType]) -> Result<Option<QueryCursor>> {
        if arguments.batch().num_rows() == 0 || !self.0.options.parameter_types.is_empty() { return Ok(None); }
        if arguments.batch().num_columns() != self.parameter_count() {
            return Err(parameter_error("Argument count differs from the prepared statement"));
        }
        if let Some(cursor) = self.try_open_parameter_projection(Arc::clone(&context), arguments, types).await? {
            return Ok(Some(cursor));
        }
        let arguments = if arguments.batch().num_rows() == 1 {
            arguments.clone()
        } else {
            match arguments.select(&Selection::Indices(vec![0])) {
                Ok(row) => row,
                Err(error) if error.code == "rtdb/memory-budget-exceeded" => return Ok(None),
                Err(error) => return Err(error),
            }
        };
        let selector = &self.0.snapshot_selector;
        let context = if let Some(rtdb_plan::SettingValue::Parameter { index }) = &selector.snapshot_token {
            let batch = arguments.batch();
            let width = index.checked_add(1).ok_or_else(size_error)?;
            let scratch = width.checked_mul(std::mem::size_of::<Value>()).ok_or_else(size_error)?;
            let cell = rtdb_columnar::estimate_read_cell_bytes(batch.schema_ref().field(*index), batch.column(*index).as_ref(), 0)?;
            let _charge = context.memory.reserve(scratch.checked_add(cell).ok_or_else(size_error)?)?;
            let mut values = vec![Value::Null; width];
            values[*index] = rtdb_columnar::read_execution_value(batch.schema_ref().field(*index), batch.column(*index).as_ref(), 0)?;
            context.reopen_sql_snapshot(selector, rtdb_expr::Parameters::Values(&values)).await?
        } else {
            context.reopen_sql_snapshot(selector, rtdb_expr::Parameters::Values(&[])).await?
        };
        let query = deep(self.source(), || self.bind(&context, types, true))?;
        if self.0.policy == ResultSignaturePolicy::Exact { self.check_signature(query.schema())?; }
        let cursor = query.execute_arrow_columns(context, arguments, 0, types, Some(self.source())).await?;
        if self.0.policy == ResultSignaturePolicy::PostgreSql { self.check_signature(cursor.runtime_schema())?; }
        Ok(Some(cursor))
    }

    /// Uses the first Arrow parameter row directly when the query only projects parameters.
    pub async fn try_open_parameter_projection(&self, context: Arc<QueryContext>, arguments: &OwnedBatch, types: &[LogicalType]) -> Result<Option<QueryCursor>> {
        if !matches!(&self.0.result, PreparedResult::Query { parameter_projection: true, .. })
            || self.0.policy != ResultSignaturePolicy::Dynamic || !self.0.options.parameter_types.is_empty()
            || arguments.batch().num_rows() == 0 || types.len() != self.parameter_count() {
            return Ok(None);
        }
        let projection = async {
            let query = deep(self.source(), || self.bind(&context, types, true))?;
            query.execute_parameter_projection(context, arguments, types, Some(self.source())).await
        }.await;
        match projection {
            Err(error) if error.code == "rtdb/memory-budget-exceeded" => Ok(None),
            result => result,
        }
    }

    async fn open_query_batches(&self, context: Arc<QueryContext>, mut arguments: Vec<Value>, types: &[LogicalType], columnar: bool) -> Result<QueryCursor> {
        if arguments.len() != self.parameter_count() { return Err(parameter_error("Argument count differs from the prepared statement")); }
        arguments.iter_mut().try_for_each(rtdb_columnar::admit_execution_value)?;
        for (index, ty) in &self.0.options.parameter_types { arguments[*index] = rtdb_expr::cast(&arguments[*index], &rtdb_columnar::execution_type(ty), &context.timezone)?; }
        let context = context.reopen_sql_snapshot(&self.0.snapshot_selector, rtdb_expr::Parameters::Values(&arguments)).await?;
        let query = deep(self.source(), || self.bind(&context, types, true))?;
        if self.0.policy == ResultSignaturePolicy::Exact { self.check_signature(query.schema())?; }
        let cursor = if columnar {
            query.execute_columns_with_text(context, arguments, Some(self.source())).await?
        } else {
            query.execute_column_rows(context, arguments, Some(self.source())).await?
        };
        if self.0.policy == ResultSignaturePolicy::PostgreSql { self.check_signature(cursor.runtime_schema())?; }
        Ok(cursor)
    }

    /// Preserves absent arguments and an explicitly empty argument batch as distinct source operations.
    /// SQL argument-width validation belongs to submission and transaction resolution.
    /// Table lookup occurs during transaction resolution.
    pub fn dml_operation(&self, arguments: Option<SqlArguments>) -> Result<TxOp> {
        if !self.is_dml() { return Err(kind_error()); }
        Ok(TxOp::Sql { sql: self.0.source.clone(), args: arguments })
    }

    fn bind(&self, context: &Arc<QueryContext>, argument_types: &[LogicalType], execution: bool) -> Result<PreparedQuery> {
        context.check_cancelled()?;
        if self.is_dml() { return Err(kind_error()); }
        if argument_types.len() != self.parameter_count() { return Err(parameter_error("Parameter field count differs from the prepared statement")); }
        let mut options = self.0.options.clone();
        options.default_timezone = context.timezone.clone();
        if execution { for ty in options.parameter_types.values_mut() { *ty = rtdb_columnar::execution_type(ty); } }
        for (index, ty) in argument_types.iter().enumerate() { options.parameter_types.entry(index).or_insert_with(|| if execution { rtdb_columnar::execution_type(ty) } else { ty.clone() }); }
        let cached = self.0.plan.lock().ok().and_then(|plan| plan.as_ref().filter(|plan| plan.options == options).map(|plan| (plan.lookups.clone(), plan.query.clone())));
        if let Some((lookups, query)) = cached && lookups.replay(&context.catalog)? { return query.recharged(&context.memory); }
        let recording = RecordingCatalog::new(&context.catalog);
        let _syntax = admit_sql_syntax(&self.0.source, &context.memory)?;
        let query = PreparedQuery::from_prepared(rtdb_sql::prepare_in(&self.0.source, &recording, &options, &context.memory)?, &context.memory)?;
        // A plan's allocations stay charged to the budget it was planned in, so the kept copy is planned in the statement's own.
        if let Some(lookups) = recording.finish() {
            let budget = self.0._reservation.budget();
            let kept = rtdb_sql::prepare_in(&self.0.source, &context.catalog, &options, budget).and_then(|prepared| PreparedQuery::from_prepared(prepared, budget));
            if let (Ok(kept), Ok(mut plan)) = (kept, self.0.plan.lock()) { *plan = Some(CachedPlan { options, lookups: Arc::new(lookups), query: kept }); }
        }
        Ok(query)
    }

    fn check_signature(&self, actual: &Schema) -> Result<()> {
        let PreparedResult::Query { schema, pg_types, .. } = &self.0.result else { return Err(kind_error()); };
        let compatible = match self.0.policy {
            ResultSignaturePolicy::Dynamic => true,
            ResultSignaturePolicy::Exact => schema.fields().iter().map(|field| (&field.name, &field.data_type)).eq(actual.fields().iter().map(|field| (&field.name, &field.data_type))),
            ResultSignaturePolicy::PostgreSql => {
                schema.len() == actual.len() && pg_types.iter().zip(actual.fields()).map(|(prepared, field)| prepared.accepts_result(&field.data_type)).collect::<Result<Vec<_>>>()?.into_iter().all(|compatible| compatible)
            }
        };
        if compatible { Ok(()) } else { Err(Anomaly::new(AnomalyCategory::Conflict, "xtdb/prepared-query-out-of-date", "cached plan must not change result type")) }
    }
}

/// The last plan a statement bound, and what its planning read from the catalog: a snapshot that answers those
/// lookups the same plans the statement the same way, so it reuses the plan.
struct CachedPlan { options: PrepareOptions, lookups: Arc<CatalogLookups>, query: PreparedQuery }

fn admit_source(source: &str, context: &QueryContext, options: &mut PrepareOptions) -> Result<Arc<MemoryReservation>> {
    context.check_cancelled()?;
    options.default_timezone = context.timezone.clone();
    options.generate_series_end_exclusive = context.options.generate_series_end_exclusive;
    context.memory.reserve(source.len().checked_add(256).ok_or_else(size_error)?)
}

pub(crate) async fn prepare_query_at_snapshot(
    source: &str, context: Arc<QueryContext>, options: &PrepareOptions, arguments: &[Value],
) -> Result<(PreparedQuery, Arc<QueryContext>)> {
    let _syntax = admit_sql_syntax(source, &context.memory)?;
    let memory = Arc::clone(&context.memory);
    let parsed = one_statement(source, &memory)?;
    let context = context.reopen_sql_snapshot(&parsed.snapshot_selector()?, rtdb_expr::Parameters::Values(arguments)).await?;
    let prepared = parsed.bind(&context.catalog, options)?;
    Ok((PreparedQuery::from_prepared(prepared, &memory)?, context))
}

pub(crate) fn prepare_query(source: &str, context: &QueryContext, options: &PrepareOptions) -> Result<PreparedQuery> {
    let _syntax = admit_sql_syntax(source, &context.memory)?;
    let prepared = rtdb_sql::prepare_in(source, &context.catalog, options, &context.memory)?;
    PreparedQuery::from_prepared(prepared, &context.memory)
}

fn admit_sql_syntax(source: &str, memory: &Arc<MemoryBudget>) -> Result<Arc<MemoryReservation>> {
    memory.reserve(rtdb_sql::statement_syntax_estimate(source.len()).ok_or_else(size_error)?)
}

fn one_statement<'input>(source: &'input str, memory: &'input Arc<MemoryBudget>) -> Result<ParsedStatement<'input>> {
    exactly_one_statement(rtdb_sql::parse_in(source, memory)?)
}

/// Statements outside the reference grammar fail there as parse errors rather than as unsupported features.
fn session_statement(source: &str, options: &PrepareOptions, memory: &Arc<MemoryBudget>) -> Anomaly {
    match rtdb_sql::parse_admitted_control_in(source, options, memory) {
        Ok(Some(_)) => Anomaly::unsupported("rtdb.node/prepared-session-statement", "Session-control statements execute through the session API"),
        Ok(None) => Anomaly::incorrect("xtdb/sql-error", "Unsupported SQL statement"),
        Err(error) => error,
    }
}

fn statement_syntax_error(source: &str, error: Anomaly, memory: &Arc<MemoryBudget>) -> Anomaly {
    match rtdb_sql::statement_syntax_diagnostic(source, &error, memory) {
        Ok(Some(diagnostic)) => diagnostic.anomaly().clone(),
        Ok(None) => error,
        Err(admission) => admission,
    }
}

fn exactly_one_statement(mut parsed: Vec<ParsedStatement<'_>>) -> Result<ParsedStatement<'_>> {
    if parsed.len() != 1 { return Err(Anomaly::incorrect("xtdb/sql-error", "A prepared statement must contain exactly one SQL statement")); }
    Ok(parsed.remove(0))
}
fn parameter_error(message: &str) -> Anomaly { Anomaly::incorrect("xtdb/parameter-count-mismatch", message) }
fn kind_error() -> Anomaly { Anomaly::incorrect("rtdb.node/statement-kind", "Prepared statement has the wrong execution kind") }
fn size_error() -> Anomaly { Anomaly::incorrect("rtdb.node/preparation-size", "Prepared statement retained size exceeds usize") }
fn retained_bytes(source: &String, options: &PrepareOptions, parameters: &Vec<LogicalType>, notices: &Vec<SqlNotice>, result: &PreparedResult) -> Result<usize> {
    let mut bytes = 1024usize.checked_add(source.capacity()).and_then(|bytes| bytes.checked_add(options.default_timezone.capacity())).ok_or_else(size_error)?;
    for path in &options.search_path { bytes = bytes.checked_add(path.capacity()).and_then(|n| n.checked_add(64)).ok_or_else(size_error)?; }
    for ty in options.parameter_types.values().chain(parameters) { bytes = bytes.checked_add(ty.retained_bytes()?).and_then(|n| n.checked_add(256)).ok_or_else(size_error)?; }
    bytes = bytes.checked_add(options.search_path.capacity().checked_mul(std::mem::size_of::<String>()).ok_or_else(size_error)?).and_then(|n| n.checked_add(parameters.capacity().checked_mul(std::mem::size_of::<LogicalType>())?)).ok_or_else(size_error)?;
    bytes = bytes.checked_add(notices.capacity().checked_mul(std::mem::size_of::<SqlNotice>()).ok_or_else(size_error)?).ok_or_else(size_error)?;
    for notice in notices { bytes = bytes.checked_add(notice.message.capacity()).ok_or_else(size_error)?; }
    if let PreparedResult::Query { schema, pg_types, .. } = result {
        bytes = bytes.checked_add(pg_types.capacity().checked_mul(std::mem::size_of::<PgType>()).ok_or_else(size_error)?).ok_or_else(size_error)?;
        for field in schema.fields() { bytes = bytes.checked_add(field.data_type.retained_bytes()?).and_then(|n| n.checked_add(field.name.capacity())).and_then(|n| n.checked_add(256)).ok_or_else(size_error)?; }
    }
    Ok(bytes)
}
