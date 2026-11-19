use rtdb_types::StructFields;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rtdb_columnar::MemoryBudget;
use rtdb_index::Snapshot;
use rtdb_plan::{Catalog, CatalogRuntime, QualifiedTable, TableBinding};
use rtdb_storage::cache::BufferPool;
use rtdb_types::{Anomaly, AnomalyCategory, LogicalType, Result, TableRef, Value};
use tokio_util::sync::CancellationToken;

use crate::{invalid, spill::SpillRegion};

#[derive(Clone)]
pub struct DatabaseSnapshot { pub partitions: Vec<Snapshot>, pub storage: Arc<BufferPool> }

#[derive(Clone)]
pub struct CatalogSnapshot {
    pub(crate) default_database: String,
    pub(crate) databases: BTreeMap<String, DatabaseSnapshot>,
    transaction_scoped: bool,
    pub(crate) runtime: Option<Arc<dyn CatalogRuntime>>,
}

impl CatalogSnapshot {
    pub fn new(default_database: impl Into<String>, databases: BTreeMap<String, DatabaseSnapshot>, transaction_scoped: bool) -> Result<Self> {
        let default_database = default_database.into();
        if !databases.contains_key(&default_database) { return Err(invalid("missing-default-database", "Default database has no pinned snapshot")); }
        if transaction_scoped && databases.len() != 1 { return Err(invalid("transaction-catalog-scope", "An index-time query must contain exactly its transaction's database")); }
        if transaction_scoped && databases.get(&default_database).is_none_or(|database| database.partitions.len() != 1) {
            return Err(invalid("transaction-partition-scope", "An index-time query must contain exactly its resolving partition"));
        }
        Ok(Self { default_database, databases, transaction_scoped, runtime: None })
    }
    pub fn with_runtime(mut self, runtime: Arc<dyn CatalogRuntime>) -> Self { self.runtime = Some(runtime); self }
    pub fn database_names(&self) -> impl Iterator<Item = &str> { self.databases.keys().map(String::as_str) }
    /// Reserves schema enumeration and joins before creating the returned table bindings.
    /// Clones of the result share its retained allocation charge.
    pub fn tables_reserved(&self, database: &str, memory: &Arc<MemoryBudget>) -> Result<rtdb_plan::CatalogRows<TableBinding>> {
        let overflow = || invalid("catalog-size", "Table metadata allocation exceeds addressable memory");
        let mut bytes = crate::system_schema::allocation_bound(database)?;
        for partition in &self.database(database)?.partitions { bytes = bytes.checked_add(partition.schema_allocation_bound(database)?).ok_or_else(overflow)?; }
        let reservation = memory.reserve(bytes)?;
        Ok(rtdb_plan::CatalogRows::new(self.tables(database)?, reservation))
    }
    pub fn database(&self, name: &str) -> Result<&DatabaseSnapshot> {
        self.databases.get(name).ok_or_else(|| Anomaly::incorrect("xtdb/unknown-db", format!("Unknown database: {name}")))
    }
    pub(crate) fn runtime_scan_type(&self, table: &TableBinding, field: &rtdb_plan::Field) -> LogicalType {
        if crate::system_schema::definition(&table.name.table).is_some() && !table.columns.contains_key(&field.name) { LogicalType::Null }
        else { rtdb_columnar::execution_type(&field.data_type) }
    }
    fn binding(&self, database: &str, table: &TableRef) -> Result<Option<TableBinding>> {
        let snapshots = &self.database(database)?.partitions;
        if let Some(definition) = crate::system_schema::definition(table) { return Ok(Some(definition.binding(database))); }
        if !snapshots.iter().any(|snapshot| snapshot.contains_table(table)) { return Ok(None); }
        let mut columns: Option<StructFields<LogicalType>> = None;
        let mut oid = None;
        for snapshot in snapshots {
            if !snapshot.contains_table(table) { continue; }
            if let Some(entry) = snapshot.table_entry(table) {
                let next = entry.oid as i32;
                if oid.is_some_and(|previous| previous != next) { return Err(Anomaly::fault("rtdb.query/partition-table-oid", "Partition catalogs disagree on table OID")); }
                oid = Some(next);
            }
            let next = snapshot.column_types(table);
            columns = Some(match columns { Some(previous) => rtdb_index::merge_column_types(&previous, &next), None => next });
        }
        Ok(Some(TableBinding { name: QualifiedTable { database: database.to_owned(), table: table.clone() }, oid, columns: columns.unwrap_or_default() }))
    }
}

impl Catalog for CatalogSnapshot {
    fn default_database(&self) -> &str { &self.default_database }
    fn databases(&self) -> BTreeSet<String> { self.databases.keys().cloned().collect() }
    fn table(&self, name: &QualifiedTable) -> Result<Option<TableBinding>> { self.binding(&name.database, &name.table) }
    fn tables(&self, database: &str) -> Result<Vec<TableBinding>> {
        let names: BTreeSet<_> = self.database(database)?.partitions.iter().flat_map(Snapshot::tables).collect();
        let names: BTreeSet<_> = names.into_iter().chain(crate::system_schema::DEFINITIONS.iter().map(|definition| TableRef::new(definition.schema, definition.name))).collect();
        names.iter().map(|table| self.binding(database, table)?.ok_or_else(|| invalid("missing-table", "Enumerated table has no binding"))).collect()
    }
    fn transaction_scoped(&self) -> bool { self.transaction_scoped }
}

impl rtdb_expr::PgCatalogLookup for CatalogSnapshot {
    fn relations(&self, allocation: &rtdb_expr::EvaluationScope, visit: &mut dyn FnMut(i32, &TableRef) -> Result<()>) -> Result<()> {
        for (name, database) in &self.databases {
            let mut bytes = crate::system_schema::allocation_bound(name)?;
            for partition in &database.partitions { bytes = bytes.checked_add(partition.schema_allocation_bound(name)?).ok_or_else(|| invalid("catalog-size", "Relation lookup exceeds addressable memory"))?; }
            allocation.reserve(bytes)?;
            for table in self.tables(name)? { visit(rtdb_types::pg_table_oid(table.oid, &table.name.table), &table.name.table)?; }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct QueryOptions {
    pub batch_rows: usize,
    pub list_batch_rows: usize,
    pub sort_memory_bytes: usize,
    /// Build-row threshold for join Bloom/IID filters; exceeding it disables the filters.
    pub join_build_rows: i64,
    pub spill_limit_bytes: u64,
    pub bypass_temporal_pruning: bool,
    /// SQL GENERATE_SERIES excludes its endpoint when the process compatibility setting is enabled.
    pub generate_series_end_exclusive: bool,
    /// Stable object-index algorithm for sort runs and complete percentile groups.
    pub object_sort: rtdb_types::ObjectSort,
}
impl Default for QueryOptions {
    fn default() -> Self { Self { batch_rows: 1024, list_batch_rows: 1024, sort_memory_bytes: 8 * 1024 * 1024, join_build_rows: 100_000, spill_limit_bytes: 1024 * 1024 * 1024, bypass_temporal_pruning: false, generate_series_end_exclusive: false, object_sort: rtdb_types::ObjectSort::default() } }
}

pub struct QueryContext {
    pub catalog: CatalogSnapshot,
    pub memory: Arc<MemoryBudget>,
    pub current_time: rtdb_types::Instant,
    pub timezone: String,
    pub options: QueryOptions,
    pub cancellation: CancellationToken,
    pub(crate) owner_cancellation: CancellationToken,
    pub(crate) spill: Arc<SpillRegion>,
    pub(crate) execution_observer: Option<Arc<dyn rtdb_plan::ExecutionObserver>>,
    pub(crate) tracer: Option<Arc<dyn rtdb_plan::Tracer>>,
    pub(crate) trace_parent: Option<rtdb_plan::SpanContext>,
    pub(crate) read_basis: Option<Arc<crate::settings::ReadBasis>>,
}

impl QueryContext {
    pub fn new(catalog: CatalogSnapshot, memory: Arc<MemoryBudget>, current_time: impl Into<rtdb_types::Instant>, timezone: impl Into<String>, options: QueryOptions, parent_cancellation: &CancellationToken) -> Result<Arc<Self>> {
        Self::with_owner(catalog, memory, current_time, timezone, options, parent_cancellation, parent_cancellation)
    }
    /// Both cancellation sources end active pulls; cancelling a cursor leaves either parent unchanged.
    /// The query holds a child of each, so its per-row checks never contend on a token every query shares.
    pub fn with_owner(catalog: CatalogSnapshot, memory: Arc<MemoryBudget>, current_time: impl Into<rtdb_types::Instant>, timezone: impl Into<String>, options: QueryOptions, parent_cancellation: &CancellationToken, owner_cancellation: &CancellationToken) -> Result<Arc<Self>> {
        if options.batch_rows == 0 || options.list_batch_rows == 0 || options.sort_memory_bytes == 0 {
            return Err(invalid("invalid-options", "Batch rows and sort bytes must be positive"));
        }
        let spill = Arc::new(SpillRegion::new(options.spill_limit_bytes)?);
        Ok(Arc::new(Self { catalog, memory, current_time: current_time.into(), timezone: timezone.into(), options, cancellation: parent_cancellation.child_token(), owner_cancellation: owner_cancellation.child_token(), spill, execution_observer: None, tracer: None, trace_parent: None, read_basis: None }))
    }
    /// Applies only to cursors opened from this context; existing cursors retain their original observer.
    pub fn with_observer(self: Arc<Self>, observer: Option<Arc<dyn rtdb_plan::ExecutionObserver>>) -> Arc<Self> {
        Arc::new(Self { catalog: self.catalog.clone(), memory: Arc::clone(&self.memory), current_time: self.current_time,
            timezone: self.timezone.clone(), options: self.options.clone(), cancellation: self.cancellation.clone(), owner_cancellation: self.owner_cancellation.clone(), spill: Arc::clone(&self.spill), execution_observer: observer, tracer: self.tracer.clone(), trace_parent: self.trace_parent, read_basis: self.read_basis.clone() })
    }
    /// Applies explicit span parentage to subsequently opened executions without thread-local context.
    pub fn with_tracer(self: Arc<Self>, tracer: Option<Arc<dyn rtdb_plan::Tracer>>, parent: Option<rtdb_plan::SpanContext>) -> Arc<Self> {
        Arc::new(Self { catalog: self.catalog.clone(), memory: Arc::clone(&self.memory), current_time: self.current_time,
            timezone: self.timezone.clone(), options: self.options.clone(), cancellation: self.cancellation.clone(), owner_cancellation: self.owner_cancellation.clone(), spill: Arc::clone(&self.spill), execution_observer: self.execution_observer.clone(), tracer, trace_parent: parent, read_basis: self.read_basis.clone() })
    }
    pub fn check_cancelled(&self) -> Result<()> {
        if self.cancellation.is_cancelled() || self.owner_cancellation.is_cancelled() { Err(cancelled()) } else { Ok(()) }
    }
    pub fn spill_bytes(&self) -> u64 { self.spill.retained_bytes() }
    pub fn spill_peak_bytes(&self) -> u64 { self.spill.peak_bytes() }
    pub fn snapshot_token(&self) -> Option<&str> { self.read_basis.as_ref().and_then(|basis| basis.token.as_deref()) }
    pub(crate) fn execution_child(&self) -> Arc<Self> {
        self.execution_child_with_memory(self.memory.scope())
    }
    pub(crate) fn execution_child_with_memory(&self, memory: Arc<MemoryBudget>) -> Arc<Self> {
        Arc::new(Self { catalog: self.catalog.clone(), memory, current_time: self.current_time,
            timezone: self.timezone.clone(), options: self.options.clone(), cancellation: self.cancellation.child_token(), owner_cancellation: self.owner_cancellation.clone(), spill: Arc::clone(&self.spill), execution_observer: self.execution_observer.clone(), tracer: self.tracer.clone(), trace_parent: self.trace_parent, read_basis: self.read_basis.clone() })
    }
}

pub(crate) fn cancelled() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb/query-cancelled", "Query execution was cancelled") }

#[derive(Clone)]
pub(crate) struct Execution {
    pub context: Arc<QueryContext>,
    pub parameters: Arc<crate::parameters::BoundParameters>,
    pub outer: Arc<OuterRows>,
    pub bindings: Option<Arc<crate::materialized::BindingScope>>,
    pub analysis: Option<Arc<crate::analysis::AnalysisNode>>,
}

pub(crate) struct OuterRows { rows: Vec<(rtdb_plan::Schema, Arc<crate::cursor::OwnedRow>)>, _reservation: Arc<rtdb_columnar::MemoryReservation> }
impl OuterRows {
    pub fn schemas(&self) -> impl Iterator<Item = &rtdb_plan::Schema> { self.rows.iter().map(|(schema, _)| schema) }
    pub fn empty(memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let reservation = memory.reserve(256)?;
        Ok(Arc::new(Self { rows: Vec::new(), _reservation: reservation }))
    }
    pub fn prepend(&self, schema: &rtdb_plan::Schema, row: Arc<crate::cursor::OwnedRow>, memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let bound = self.rows.iter().try_fold(crate::runtime_schema::schema_bound(schema)?, |bytes, (schema, _)| {
            bytes.checked_add(crate::runtime_schema::schema_bound(schema)?).ok_or_else(|| crate::invalid("outer-size", "Outer schema allocation exceeds addressable memory"))
        })?;
        let reservation = memory.reserve(bound)?;
        let mut rows = Vec::with_capacity(self.rows.len() + 1); rows.push((schema.clone(), row)); rows.extend(self.rows.iter().cloned());
        Ok(Arc::new(Self { rows, _reservation: reservation }))
    }
}

impl Execution {
    pub async fn bind_arguments(&self, arguments: &[(usize, rtdb_plan::Expr)], schema: &rtdb_plan::Schema, row: Arc<crate::cursor::OwnedRow>) -> Result<Self> {
        let scope = self.scope()?;
        let parameter_count = arguments.iter().try_fold(self.parameters.values().len(), |count, (index, _)| index.checked_add(1).map(|next| count.max(next)).ok_or_else(|| crate::invalid("parameter-count", "Apply parameter index overflow")))?;
        let retained_bytes = self.parameters.bytes()?;
        scope.reserve(parameter_count.checked_mul(std::mem::size_of::<Value>() + std::mem::size_of::<rtdb_types::LogicalType>()).and_then(|bytes| bytes.checked_mul(2)).and_then(|bytes| bytes.checked_add(retained_bytes)).ok_or_else(|| crate::invalid("parameter-count", "Apply parameters exceed addressable memory"))?)?;
        let mut parameters = Vec::with_capacity(parameter_count);
        parameters.extend_from_slice(self.parameters.values());
        parameters.resize(parameter_count, Value::Null);
        let mut types = Vec::with_capacity(parameter_count);
        types.extend_from_slice(self.parameters.types());
        types.resize(parameter_count, rtdb_types::LogicalType::Null);
        for (index, expression) in arguments {
            types[*index] = self.expression_type(&scope, expression, schema)?;
            parameters[*index] = self.evaluate(&scope, expression, schema, &row.values).await?;
        }
        let outer = self.outer.prepend(schema, row, &self.context.memory)?;
        Ok(Self { context: Arc::clone(&self.context), parameters: Arc::new(crate::parameters::BoundParameters::new(parameters, types, &self.context.memory)?), outer, bindings: self.bindings.clone(), analysis: self.analysis.clone() })
    }
    pub fn child(&self, index: usize) -> Self {
        let mut execution = self.clone(); execution.analysis = self.analysis.as_ref().and_then(|node| node.child(index)); execution
    }
    pub fn scope(&self) -> Result<rtdb_expr::EvaluationScope> { rtdb_expr::EvaluationScope::new(&self.context.memory) }
    pub fn expression_type(&self, scope: &rtdb_expr::EvaluationScope, expression: &rtdb_plan::Expr, schema: &rtdb_plan::Schema) -> Result<rtdb_types::LogicalType> {
        scope.reserve(self.outer.rows.len().checked_mul(std::mem::size_of::<rtdb_expr::OuterRow<'_>>()).ok_or_else(|| Anomaly::incorrect("rtdb.query/expression-size", "Outer row bindings exceed addressable memory"))?)?;
        let outer_rows: Vec<_> = self.outer.rows.iter().map(|(schema,row)|rtdb_expr::OuterRow { schema,values: &row.values }).collect();
        let context = rtdb_expr::EvalContext { parameters: self.parameters.values(),parameter_types: Some(self.parameters.types()),current_time: self.context.current_time,timezone: &self.context.timezone,outer_rows: &outer_rows,snapshot_token: self.context.snapshot_token(),allocation: Some(scope),catalog: Some(&self.context.catalog) };
        rtdb_expr::expression_type(expression,schema,&context)
    }
    pub async fn evaluate(&self, scope: &rtdb_expr::EvaluationScope, expression: &rtdb_plan::Expr, schema: &rtdb_plan::Schema, row: &[Value]) -> Result<Value> {
        let invocation = self.prepare_expression(expression,schema,scope)?;
        self.evaluate_invocation(scope,&invocation,row).await
    }
    pub fn prepare_expression<'a>(&self, expression: &'a rtdb_plan::Expr, schema: &'a rtdb_plan::Schema, scope: &rtdb_expr::EvaluationScope) -> Result<rtdb_expr::ExpressionInvocation<'a>> {
        self.context.check_cancelled()?;
        scope.reserve(self.outer.rows.len().checked_mul(std::mem::size_of::<rtdb_expr::OuterRow<'_>>()).ok_or_else(|| Anomaly::incorrect("rtdb.query/expression-size", "Outer row bindings exceed addressable memory"))?)?;
        let outer_rows: Vec<_> = self.outer.rows.iter().map(|(schema,row)|rtdb_expr::OuterRow { schema,values: &row.values }).collect();
        let context = rtdb_expr::EvalContext { parameters: self.parameters.values(),parameter_types: Some(self.parameters.types()),current_time: self.context.current_time,timezone: &self.context.timezone,outer_rows: &outer_rows,snapshot_token: self.context.snapshot_token(),allocation: Some(scope),catalog: Some(&self.context.catalog) };
        rtdb_expr::ExpressionInvocation::prepare(expression,schema,&context)
    }
    pub async fn evaluate_invocation(&self, scope: &rtdb_expr::EvaluationScope, invocation: &rtdb_expr::ExpressionInvocation<'_>, row: &[Value]) -> Result<Value> {
        self.context.check_cancelled()?;
        scope.reserve(4096)?;
        scope.reserve(self.outer.rows.len().checked_mul(std::mem::size_of::<rtdb_expr::OuterRow<'_>>()).ok_or_else(|| Anomaly::incorrect("rtdb.query/expression-size", "Outer row bindings exceed addressable memory"))?)?;
        let outer_rows: Vec<_> = self.outer.rows.iter().map(|(schema, row)| rtdb_expr::OuterRow { schema, values: &row.values }).collect();
        let context = rtdb_expr::EvalContext { parameters: self.parameters.values(), parameter_types: Some(self.parameters.types()), current_time: self.context.current_time, timezone: &self.context.timezone, outer_rows: &outer_rows, snapshot_token: self.context.snapshot_token(), allocation: Some(scope), catalog: Some(&self.context.catalog) };
        self.context.owner_cancellation.run_until_cancelled(self.context.cancellation.run_until_cancelled(
            invocation.evaluate_projection_async(row,&context))).await.flatten().ok_or_else(cancelled)?
    }
    pub async fn open_list(&self, expression: rtdb_plan::Expr, page_rows: Option<std::num::NonZeroUsize>) -> Result<crate::list::ListCursor> {
        let execution = self.clone();
        let context = Arc::clone(&self.context);
        let stream = async_stream::try_stream! {
            let scope = execution.scope()?;
            let schema = rtdb_plan::Schema::default();
            scope.reserve(execution.outer.rows.len().checked_mul(std::mem::size_of::<rtdb_expr::OuterRow<'_>>()).ok_or_else(|| crate::invalid("outer-size", "Outer row bindings exceed addressable memory"))?)?;
            let outer_rows: Vec<_> = execution.outer.rows.iter().map(|(schema, row)| rtdb_expr::OuterRow { schema, values: &row.values }).collect();
            let eval = rtdb_expr::EvalContext { parameters: execution.parameters.values(), parameter_types: Some(execution.parameters.types()), current_time: execution.context.current_time,
                timezone: &execution.context.timezone, outer_rows: &outer_rows, snapshot_token: execution.context.snapshot_token(), allocation: Some(&scope), catalog: Some(&execution.context.catalog) };
            let invocation = rtdb_expr::ExpressionInvocation::prepare(&expression, &schema, &eval)?;
            let page_rows = page_rows.map_or(execution.context.options.list_batch_rows,std::num::NonZeroUsize::get);
            let list = invocation.open_list(&[], &eval, page_rows).await?;
            match list.into_indexed() {
                Ok(batches) => yield crate::list::ListEvent::Opened(Some(batches)),
                Err(mut list) => {
                    yield crate::list::ListEvent::Opened(None);
                    while let Some(batch) = list.next_batch().await? {
                        yield crate::list::ListEvent::Batch(batch);
                    }
                }
            };
        };
        crate::list::ListCursor::open(Box::pin(stream), context).await
    }
    pub async fn evaluate_list_invocation(&self, scope: &rtdb_expr::EvaluationScope, invocation: &rtdb_expr::ExpressionInvocation<'_>, row: &[Value]) -> Result<rtdb_expr::EvaluatedList> {
        self.context.check_cancelled()?;
        scope.reserve(4096)?;
        scope.reserve(self.outer.rows.len().checked_mul(std::mem::size_of::<rtdb_expr::OuterRow<'_>>()).ok_or_else(|| Anomaly::incorrect("rtdb.query/expression-size", "Outer row bindings exceed addressable memory"))?)?;
        let outer_rows: Vec<_> = self.outer.rows.iter().map(|(schema, row)| rtdb_expr::OuterRow { schema, values: &row.values }).collect();
        let context = rtdb_expr::EvalContext { parameters: self.parameters.values(), parameter_types: Some(self.parameters.types()), current_time: self.context.current_time, timezone: &self.context.timezone, outer_rows: &outer_rows, snapshot_token: self.context.snapshot_token(), allocation: Some(scope), catalog: Some(&self.context.catalog) };
        self.context.owner_cancellation.run_until_cancelled(self.context.cancellation.run_until_cancelled(
            invocation.evaluate_list_async(row,&context))).await.flatten().ok_or_else(cancelled)?
    }
}
