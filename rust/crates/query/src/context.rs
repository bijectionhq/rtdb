use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rtdb_columnar::MemoryBudget;
use rtdb_index::Snapshot;
use rtdb_plan::{Catalog, CatalogRuntime, QualifiedTable, TableBinding};
use rtdb_storage::cache::BufferPool;
use rtdb_types::{Anomaly, AnomalyCategory, LogicalType, Result, TableRef, Timestamp, Value};
use tokio_util::sync::CancellationToken;

use crate::{invalid, spill::SpillRegion};

#[derive(Clone)]
pub struct DatabaseSnapshot { pub partitions: Vec<Snapshot>, pub storage: Arc<BufferPool> }

#[derive(Clone)]
pub struct CatalogSnapshot {
    default_database: String,
    databases: BTreeMap<String, DatabaseSnapshot>,
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
    fn binding(&self, database: &str, table: &TableRef) -> Result<Option<TableBinding>> {
        let snapshots = &self.database(database)?.partitions;
        if let Some(definition) = crate::system_schema::definition(table) { return Ok(Some(definition.binding(database))); }
        if !snapshots.iter().any(|snapshot| snapshot.tables().contains(table)) { return Ok(None); }
        let mut columns: Option<BTreeMap<String, LogicalType>> = None;
        let mut oid = None;
        for snapshot in snapshots {
            if !snapshot.tables().contains(table) { continue; }
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

#[derive(Clone, Debug)]
pub struct QueryOptions {
    pub batch_rows: usize,
    pub sort_memory_bytes: usize,
    pub spill_limit_bytes: u64,
    pub merge_fan_in: usize,
    pub bypass_temporal_pruning: bool,
}
impl Default for QueryOptions {
    fn default() -> Self { Self { batch_rows: 1024, sort_memory_bytes: 8 * 1024 * 1024, spill_limit_bytes: 1024 * 1024 * 1024, merge_fan_in: 16, bypass_temporal_pruning: false } }
}

pub struct QueryContext {
    pub catalog: CatalogSnapshot,
    pub memory: Arc<MemoryBudget>,
    pub current_time: Timestamp,
    pub timezone: String,
    pub options: QueryOptions,
    pub cancellation: CancellationToken,
    pub(crate) owner_cancellation: CancellationToken,
    pub(crate) spill: Arc<SpillRegion>,
    pub(crate) execution_observer: Option<Arc<dyn rtdb_plan::ExecutionObserver>>,
    pub(crate) tracer: Option<Arc<dyn rtdb_plan::Tracer>>,
    pub(crate) trace_parent: Option<rtdb_plan::SpanContext>,
}

impl QueryContext {
    pub fn new(catalog: CatalogSnapshot, memory: Arc<MemoryBudget>, current_time: Timestamp, timezone: impl Into<String>, options: QueryOptions, parent_cancellation: &CancellationToken) -> Result<Arc<Self>> {
        Self::with_owner(catalog, memory, current_time, timezone, options, parent_cancellation, parent_cancellation)
    }
    /// Both cancellation sources end active pulls; cancelling a cursor leaves either parent unchanged.
    pub fn with_owner(catalog: CatalogSnapshot, memory: Arc<MemoryBudget>, current_time: Timestamp, timezone: impl Into<String>, options: QueryOptions, parent_cancellation: &CancellationToken, owner_cancellation: &CancellationToken) -> Result<Arc<Self>> {
        if options.batch_rows == 0 || options.sort_memory_bytes == 0 || options.merge_fan_in < 2 {
            return Err(invalid("invalid-options", "Batch rows and sort bytes must be positive; merge fan-in must be at least two"));
        }
        let spill = Arc::new(SpillRegion::new(options.spill_limit_bytes)?);
        Ok(Arc::new(Self { catalog, memory, current_time, timezone: timezone.into(), options, cancellation: parent_cancellation.child_token(), owner_cancellation: owner_cancellation.clone(), spill, execution_observer: None, tracer: None, trace_parent: None }))
    }
    /// Applies only to cursors opened from this context; existing cursors retain their original observer.
    pub fn with_observer(self: Arc<Self>, observer: Option<Arc<dyn rtdb_plan::ExecutionObserver>>) -> Arc<Self> {
        Arc::new(Self { catalog: self.catalog.clone(), memory: Arc::clone(&self.memory), current_time: self.current_time,
            timezone: self.timezone.clone(), options: self.options.clone(), cancellation: self.cancellation.clone(), owner_cancellation: self.owner_cancellation.clone(), spill: Arc::clone(&self.spill), execution_observer: observer, tracer: self.tracer.clone(), trace_parent: self.trace_parent })
    }
    /// Applies explicit span parentage to subsequently opened executions without thread-local context.
    pub fn with_tracer(self: Arc<Self>, tracer: Option<Arc<dyn rtdb_plan::Tracer>>, parent: Option<rtdb_plan::SpanContext>) -> Arc<Self> {
        Arc::new(Self { catalog: self.catalog.clone(), memory: Arc::clone(&self.memory), current_time: self.current_time,
            timezone: self.timezone.clone(), options: self.options.clone(), cancellation: self.cancellation.clone(), owner_cancellation: self.owner_cancellation.clone(), spill: Arc::clone(&self.spill), execution_observer: self.execution_observer.clone(), tracer, trace_parent: parent })
    }
    pub fn check_cancelled(&self) -> Result<()> {
        if self.cancellation.is_cancelled() || self.owner_cancellation.is_cancelled() { Err(cancelled()) } else { Ok(()) }
    }
    pub fn spill_bytes(&self) -> u64 { self.spill.retained_bytes() }
    pub fn spill_peak_bytes(&self) -> u64 { self.spill.peak_bytes() }
    pub(crate) fn execution_child(&self) -> Arc<Self> {
        Arc::new(Self { catalog: self.catalog.clone(), memory: self.memory.scope(), current_time: self.current_time,
            timezone: self.timezone.clone(), options: self.options.clone(), cancellation: self.cancellation.child_token(), owner_cancellation: self.owner_cancellation.clone(), spill: Arc::clone(&self.spill), execution_observer: self.execution_observer.clone(), tracer: self.tracer.clone(), trace_parent: self.trace_parent })
    }
}

pub(crate) fn cancelled() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb/query-cancelled", "Query execution was cancelled") }

#[derive(Clone)]
pub(crate) struct Execution {
    pub context: Arc<QueryContext>,
    pub parameters: Arc<crate::cursor::OwnedRow>,
    pub outer: Vec<(rtdb_plan::Schema, Arc<crate::cursor::OwnedRow>)>,
}

impl Execution {
    pub fn evaluate(&self, expression: &rtdb_plan::Expr, schema: &rtdb_plan::Schema, row: &[Value]) -> Result<Value> {
        self.context.check_cancelled()?;
        let _decimal_validation = self.context.memory.reserve(4096)?;
        let outer_rows: Vec<_> = self.outer.iter().map(|(schema, row)| rtdb_expr::OuterRow { schema, values: &row.values }).collect();
        let context = rtdb_expr::EvalContext { parameters: &self.parameters.values, current_time: self.context.current_time, timezone: &self.context.timezone, outer_rows: &outer_rows };
        rtdb_expr::evaluate_projection(expression, schema, row, &context)
    }
}
