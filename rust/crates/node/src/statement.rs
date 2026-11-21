use std::{collections::{BTreeMap, VecDeque}, sync::Arc};

use parking_lot::Mutex;
use rtdb_columnar::MemoryReservation;
use rtdb_plan::{Catalog, CatalogLookups, ColumnId, Field, PreparedStatement, RecordingCatalog, Schema, SqlNotice, Statement};
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
    result: PreparedResult,
    policy: ResultSignaturePolicy,
    _reservation: Arc<MemoryReservation>,
    plan: std::sync::Mutex<Option<CachedPlan>>,
}
enum PreparedResult { Query { schema: Schema, pg_types: Vec<PgType> }, Dml }

/// An immutable SQL preparation; each query execution binds against its supplied snapshot.
/// Session access checks and read-basis capture precede this API and remain the caller's obligation.
#[derive(Clone)]
pub struct PreparedSql(Arc<PreparedData>, Option<Arc<QueryPlanCache>>);

/// Binding and planning recurse as deep as the statement's expressions nest, which only long source can reach,
/// so a long statement is prepared on a stack of its own rather than overflowing the caller's.
fn deep<T>(source: &str, prepare: impl FnOnce() -> T) -> T {
    const DEEP_SOURCE_BYTES: usize = 4096;
    if source.len() < DEEP_SOURCE_BYTES { prepare() } else { stacker::grow(64 << 20, prepare) }
}

impl PreparedSql {
    pub fn prepare(source: &str, context: &Arc<QueryContext>, options: PrepareOptions, policy: ResultSignaturePolicy) -> Result<Self> {
        Self::prepare_with_cache(source, context, options, policy, None)
    }

    pub(crate) fn prepare_with_cache(source: &str, context: &Arc<QueryContext>, mut options: PrepareOptions, policy: ResultSignaturePolicy, cache: Option<&Arc<QueryPlanCache>>) -> Result<Self> {
        let source_reservation = admit_source(source, context, &mut options)?;
        let syntax = admit_sql_syntax(source, &context.memory)?;
        let parsed = match one_statement(source, &context.memory) {
            Ok(parsed) => parsed,
            Err(error) => {
                drop((syntax, source_reservation));
                return Err(statement_syntax_error(source, error, &context.memory));
            }
        };
        deep(source, || Self::prepare_parsed(parsed, context, options, policy, false, false, source_reservation, cache))
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
        Self::prepare_postgres_syntax_with_cache(syntax, context, search_path, parameter_oids, None)
    }

    pub(crate) fn prepare_postgres_syntax_with_cache(
        syntax: StatementSyntaxBatch<'_>, context: &Arc<QueryContext>, search_path: Vec<String>, parameter_oids: &[i32], cache: Option<&Arc<QueryPlanCache>>,
    ) -> Result<Self> {
        let parsed = exactly_one_statement(syntax.resolve_in(&context.memory)?)?;
        Self::prepare_postgres_parsed(parsed, context, search_path, parameter_oids, cache)
    }

    /// Resolves and prepares the selected statement in the reached query context.
    pub fn prepare_postgres_statement(syntax: DeferredStatementSyntax<'_>, context: &Arc<QueryContext>, search_path: Vec<String>, parameter_oids: &[i32]) -> Result<Self> {
        Self::prepare_postgres_statement_with_cache(syntax, context, search_path, parameter_oids, None)
    }

    pub(crate) fn prepare_postgres_statement_with_cache(
        syntax: DeferredStatementSyntax<'_>, context: &Arc<QueryContext>, search_path: Vec<String>, parameter_oids: &[i32], cache: Option<&Arc<QueryPlanCache>>,
    ) -> Result<Self> {
        let parsed = syntax.resolve_in(&context.memory)?;
        Self::prepare_postgres_parsed(parsed, context, search_path, parameter_oids, cache)
    }

    fn prepare_postgres_parsed(parsed: ParsedStatement<'_>, context: &Arc<QueryContext>, search_path: Vec<String>, parameter_oids: &[i32], cache: Option<&Arc<QueryPlanCache>>) -> Result<Self> {
        let source = parsed.source();
        deep(source, || Self::prepare_postgres_parsed_at(parsed, context, search_path, parameter_oids, cache))
    }

    fn prepare_postgres_parsed_at(parsed: ParsedStatement<'_>, context: &Arc<QueryContext>, search_path: Vec<String>, parameter_oids: &[i32], cache: Option<&Arc<QueryPlanCache>>) -> Result<Self> {
        let source = parsed.source();
        if parsed.kind() == StatementKind::Dml {
            let mut options = PrepareOptions { search_path, ..Default::default() };
            let source_reservation = admit_source(source, context, &mut options)?;
            return Self::prepare_parsed(parsed, context, options, ResultSignaturePolicy::PostgreSql, false, parameter_oids.is_empty(), source_reservation, cache);
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
        Self::prepare_parsed(parsed, context, options, ResultSignaturePolicy::PostgreSql, dynamic, parameter_oids.is_empty(), source_reservation, cache)
    }

    fn prepare_parsed(
        parsed: ParsedStatement<'_>, context: &Arc<QueryContext>, options: PrepareOptions, policy: ResultSignaturePolicy,
        dynamic_pg: bool, dml_notices: bool, source_reservation: Arc<MemoryReservation>, cache: Option<&Arc<QueryPlanCache>>,
    ) -> Result<Self> {
        let source = parsed.source();
        let count = parsed.parameter_count();
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
                let bind = |catalog: &dyn Catalog| parsed.bind(catalog, &options);
                let bound = match cache {
                    Some(cache) => cache.bind(source, context, &options, bind)?,
                    None => Arc::new(BoundQuery::new(bind(&context.catalog)?, &context.memory)?),
                };
                for (index, ty) in &bound.parameter_types {
                    if !options.parameter_types.contains_key(index) { parameters[*index] = ty.clone(); }
                }
                notices.clone_from(&bound.notices);
                let query = &bound.query;
                let schema = if policy == ResultSignaturePolicy::PostgreSql && !dynamic_pg {
                    query.result_schema(context)?.schema().clone()
                } else {
                    query.schema().clone()
                };
                let pg_types = schema.fields().iter().map(|field| if dynamic_pg || policy != ResultSignaturePolicy::PostgreSql { Ok(PgType::Default) } else { PgType::from_logical(&field.data_type) }).collect::<Result<_>>()?;
                PreparedResult::Query { schema, pg_types }
            }
            StatementKind::Session => return Err(session_statement(source, &options, &context.memory)),
        };
        let source = source.to_owned();
        let bytes = retained_bytes(&source, &options, &parameters, &notices, &result)?;
        let reservation = context.memory.reserve(bytes)?;
        drop(source_reservation);
        Ok(Self(Arc::new(PreparedData { source, options, parameters, notices, result, policy, _reservation: reservation, plan: Default::default() }), cache.cloned()))
    }

    pub fn source(&self) -> &str { &self.0.source }
    pub fn parameter_count(&self) -> usize { self.0.parameters.len() }
    pub fn parameter_types(&self) -> &[LogicalType] { &self.0.parameters }
    /// Planning warnings in source traversal order; repeated occurrences are retained.
    /// Execution-time rebinding does not append warnings to this immutable preparation.
    pub fn notices(&self) -> &[SqlNotice] { &self.0.notices }
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
    pub async fn open_query_typed(&self, context: Arc<QueryContext>, mut arguments: Vec<Value>, types: &[LogicalType]) -> Result<QueryCursor> {
        if arguments.len() != self.parameter_count() { return Err(parameter_error("Argument count differs from the prepared statement")); }
        arguments.iter_mut().for_each(rtdb_columnar::admit_execution_value);
        for (index, ty) in &self.0.options.parameter_types { arguments[*index] = rtdb_expr::cast(&arguments[*index], &rtdb_columnar::execution_type(ty), &context.timezone)?; }
        let query = deep(self.source(), || self.bind(&context, types, true))?;
        if self.0.policy == ResultSignaturePolicy::Exact { self.check_signature(query.schema())?; }
        let cursor = query.execute_with_text(context, arguments, Some(self.source())).await?;
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
        if let Some(cache) = &self.1 { return cache.prepare(&self.0.source, context, &options); }
        let cached = self.0.plan.lock().ok().and_then(|plan| plan.as_ref().filter(|plan| plan.options == options).map(|plan| (plan.lookups.clone(), plan.query.clone())));
        if let Some((lookups, query)) = cached && lookups.replay(&context.catalog)? { return query.recharged(&context.memory); }
        let recording = RecordingCatalog::new(&context.catalog);
        let _syntax = admit_sql_syntax(&self.0.source, &context.memory)?;
        let query = PreparedQuery::from_prepared(rtdb_sql::prepare_in(&self.0.source, &recording, &options, &context.memory)?, &context.memory)?;
        if let Some(lookups) = recording.finish() {
            let budget = self.0._reservation.budget();
            let kept = rtdb_sql::prepare_in(&self.0.source, &context.catalog, &options, budget).and_then(|prepared| PreparedQuery::from_prepared(prepared, budget));
            if let (Ok(kept), Ok(mut plan)) = (kept, self.0.plan.lock()) { *plan = Some(CachedPlan { options, lookups: Arc::new(lookups), query: kept }); }
        }
        Ok(query)
    }

    fn check_signature(&self, actual: &Schema) -> Result<()> {
        let PreparedResult::Query { schema, pg_types } = &self.0.result else { return Err(kind_error()); };
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

pub(crate) struct QueryPlanCache {
    memory: Arc<MemoryBudget>,
    entries: Mutex<VecDeque<Arc<SharedQueryPlan>>>,
}

struct SharedQueryPlan {
    source: String,
    options: PrepareOptions,
    lookups: CatalogLookups,
    bound: Arc<BoundQuery>,
}

struct BoundQuery {
    query: PreparedQuery,
    parameter_types: BTreeMap<usize, LogicalType>,
    notices: Vec<SqlNotice>,
    _cache_reservation: Option<Arc<MemoryReservation>>,
}

impl BoundQuery {
    fn new(mut prepared: PreparedStatement, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let parameter_types = prepared.parameter_types.clone();
        let notices = std::mem::take(&mut prepared.notices);
        let query = PreparedQuery::from_prepared(prepared, memory)?;
        Ok(Self { query, parameter_types, notices, _cache_reservation: None })
    }

    fn charge_cache_metadata(mut self, memory: &Arc<MemoryBudget>, key_bytes: usize) -> Result<Self> {
        let mut bytes = key_bytes;
        for ty in self.parameter_types.values() {
            bytes = bytes.checked_add(ty.retained_bytes()?).and_then(|n| n.checked_add(256)).ok_or_else(size_error)?;
        }
        for notice in &self.notices {
            bytes = bytes.checked_add(notice.message.len()).and_then(|n| n.checked_add(64)).ok_or_else(size_error)?;
        }
        self._cache_reservation = Some(memory.reserve(bytes)?);
        Ok(self)
    }
}

impl QueryPlanCache {
    const ENTRIES: usize = 128;
    const ENTRY_BYTES: usize = 512 << 10;

    pub(crate) fn new(memory: &Arc<MemoryBudget>) -> Self {
        Self { memory: memory.scope_with_limit((memory.limit_bytes() / 64).min(16 << 20)), entries: Mutex::new(VecDeque::new()) }
    }

    pub(crate) fn prepare(&self, source: &str, context: &QueryContext, options: &PrepareOptions) -> Result<PreparedQuery> {
        let mut syntax = None;
        let bound = self.bind(source, context, options, |catalog| {
            syntax = Some(admit_sql_syntax(source, &context.memory)?);
            rtdb_sql::prepare_in(source, catalog, options, &context.memory)
        })?;
        drop(syntax);
        Ok(bound.query.clone())
    }

    fn bind(&self, source: &str, context: &QueryContext, options: &PrepareOptions, prepare: impl FnOnce(&dyn Catalog) -> Result<PreparedStatement>) -> Result<Arc<BoundQuery>> {
        context.check_cancelled()?;
        let cached = {
            let mut entries = self.entries.lock();
            let index = entries.iter().position(|entry| entry.source == source && entry.options == *options);
            index.and_then(|index| entries.remove(index)).map(|entry| { entries.push_back(Arc::clone(&entry)); entry })
        };
        if let Some(cached) = cached && cached.lookups.replay(&context.catalog)? { return Ok(Arc::clone(&cached.bound)); }
        let recording = RecordingCatalog::new(&context.catalog);
        let prepared = prepare(&recording)?;
        let cacheable = matches!(&prepared.statement, Statement::Query(_));
        let bound = BoundQuery::new(prepared, &context.memory)?;
        if cacheable && let Some(lookups) = recording.finish() {
            self.remember(source, options, &context.catalog, lookups);
        }
        Ok(Arc::new(bound))
    }

    fn remember(&self, source: &str, options: &PrepareOptions, catalog: &dyn Catalog, lookups: CatalogLookups) {
        let Ok(bytes) = cache_entry_bytes(source, options, &lookups) else { return; };
        let budget = self.memory.scope_with_limit(Self::ENTRY_BYTES);
        let cache_plan = || -> Result<BoundQuery> {
            let _syntax = admit_sql_syntax(source, &budget)?;
            // Raw expression forms retain the budget used when parsing them.
            let prepared = rtdb_sql::prepare_in(source, catalog, options, &budget)?;
            BoundQuery::new(prepared, &budget)?.charge_cache_metadata(&budget, bytes)
        };
        let Ok(bound) = cache_plan() else { return; };
        let mut entries = self.entries.lock();
        let index = entries.iter().position(|entry| entry.source == source && entry.options == *options);
        if let Some(index) = index { entries.remove(index); }
        if entries.len() == Self::ENTRIES { entries.pop_front(); }
        entries.push_back(Arc::new(SharedQueryPlan { source: source.to_owned(), options: options.clone(), lookups, bound: Arc::new(bound) }));
    }
}

fn cache_entry_bytes(source: &str, options: &PrepareOptions, lookups: &CatalogLookups) -> Result<usize> {
    let mut bytes = lookups.retained_bytes()?.checked_add(1024).and_then(|n| n.checked_add(source.len())).and_then(|n| n.checked_add(options.default_timezone.len())).ok_or_else(size_error)?;
    for path in &options.search_path { bytes = bytes.checked_add(path.len()).and_then(|n| n.checked_add(64)).ok_or_else(size_error)?; }
    for ty in options.parameter_types.values() { bytes = bytes.checked_add(ty.retained_bytes()?).and_then(|n| n.checked_add(256)).ok_or_else(size_error)?; }
    Ok(bytes)
}

fn admit_source(source: &str, context: &QueryContext, options: &mut PrepareOptions) -> Result<Arc<MemoryReservation>> {
    context.check_cancelled()?;
    options.default_timezone = context.timezone.clone();
    options.generate_series_end_exclusive = context.options.generate_series_end_exclusive;
    context.memory.reserve(source.len().checked_add(256).ok_or_else(size_error)?)
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
    if let PreparedResult::Query { schema, pg_types } = result {
        bytes = bytes.checked_add(pg_types.capacity().checked_mul(std::mem::size_of::<PgType>()).ok_or_else(size_error)?).ok_or_else(size_error)?;
        for field in schema.fields() { bytes = bytes.checked_add(field.data_type.retained_bytes()?).and_then(|n| n.checked_add(field.name.capacity())).and_then(|n| n.checked_add(256)).ok_or_else(size_error)?; }
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::QueryPlanCache;
    use rtdb_index::{PartitionState, TransactionBuffer};
    use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryContext};
    use rtdb_sql::PrepareOptions;
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    use rtdb_types::{Interval, MemoryBudget, StructFields, TableRef, Timestamp, TransactionKey, TxId, Value};
    use std::{collections::BTreeMap, sync::Arc};
    use tokio_util::sync::CancellationToken;

    fn context(partition: &PartitionState, memory: &Arc<MemoryBudget>) -> Arc<QueryContext> {
        let database = DatabaseSnapshot { partitions: vec![partition.value().snapshot()], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        QueryContext::new(catalog, memory.scope(), Timestamp(20), "UTC", Default::default(), &CancellationToken::new()).unwrap()
    }

    fn write(partition: &PartitionState, memory: &Arc<MemoryBudget>, tx: i64, value: Value) {
        let mut buffer = TransactionBuffer::new(partition.value().snapshot(), TransactionKey { tx_id: TxId(tx), system_time: Timestamp(tx) }, Arc::clone(memory)).unwrap();
        buffer.put(TableRef::new("public", "docs"), StructFields::from([("_id".into(), Value::I64(tx)), ("v".into(), value)]), Interval::from_micros(0, i64::MAX).unwrap()).unwrap();
        partition.apply(&buffer.stage().unwrap()).unwrap();
    }

    #[test]
    fn shared_query_plans_revalidate_the_catalog_and_charge_the_plan_once_on_hits() {
        let memory = MemoryBudget::new(1 << 28);
        let cache = QueryPlanCache::new(&memory);
        let partition = PartitionState::empty(Default::default()).unwrap();
        let options = PrepareOptions::default();
        let sql = "SELECT v FROM docs";
        write(&partition, &memory, 1, Value::I64(1));
        let first = cache.prepare(sql, &context(&partition, &memory), &options).unwrap();
        let next_context = context(&partition, &memory);
        let next = cache.prepare(sql, &next_context, &options).unwrap();
        let again = cache.prepare(sql, &next_context, &options).unwrap();
        assert!(Arc::ptr_eq(next.relation(), again.relation()));
        assert_eq!(next_context.memory.used_bytes(), 0);
        write(&partition, &memory, 2, Value::Utf8("text".into()));
        let changed = cache.prepare(sql, &context(&partition, &memory), &options).unwrap();
        assert!(!Arc::ptr_eq(first.relation(), changed.relation()));
        assert_ne!(first.schema(), changed.schema());
        let still_changed = cache.prepare(sql, &context(&partition, &memory), &options).unwrap();
        let reused = cache.prepare(sql, &context(&partition, &memory), &options).unwrap();
        assert_eq!(changed.schema(), reused.schema());
        assert!(Arc::ptr_eq(still_changed.relation(), reused.relation()));
    }

    #[test]
    fn cached_regex_metadata_releases_the_original_query_budget() {
        let memory = MemoryBudget::new(1 << 28);
        let partition = PartitionState::empty(Default::default()).unwrap();
        let cache = QueryPlanCache::new(&memory);
        let context = context(&partition, &memory);
        let options = PrepareOptions::default();
        let sql = "SELECT regexp_replace('abc', 'a', 'x')";
        let query = cache.prepare(sql, &context, &options).unwrap();
        assert!(context.memory.used_bytes() > 0);
        drop(query);
        assert_eq!(context.memory.used_bytes(), 0);
        let first_hit = cache.prepare(sql, &context, &options).unwrap();
        let next_hit = cache.prepare(sql, &context, &options).unwrap();
        assert!(Arc::ptr_eq(first_hit.relation(), next_hit.relation()));
        assert_eq!(context.memory.used_bytes(), 0);
    }
}
