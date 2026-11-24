use rtdb_types::StructFields;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rtdb_columnar::MemoryBudget;
use rtdb_index::Snapshot;
use rtdb_plan::{Catalog, CatalogRuntime, QualifiedTable, TableBinding};
use rtdb_storage::{cache::{BufferPool, HeldCatalogRead}, sealing::{PlaintextDelivery, PlaintextView}};
use rtdb_types::{Anomaly, AnomalyCategory, LogicalType, Result, TableRef, Timestamp, Value};
use tokio_util::sync::CancellationToken;

use crate::{invalid, spill::SpillRegion};

#[derive(Clone)]
pub struct DatabaseSnapshot { pub partitions: Vec<Snapshot>, pub storage: Arc<BufferPool> }

/// Binds the physical catalog in this snapshot to the store that will serve its reads.
pub async fn retain_catalog_snapshot(snapshot: Snapshot, storage: &BufferPool) -> Result<Snapshot> {
    let permit = hold_snapshot(&snapshot, storage).await?;
    Ok(snapshot.with_catalog_read(permit))
}

async fn hold_snapshot(snapshot: &Snapshot, storage: &BufferPool) -> Result<Arc<HeldCatalogRead>> {
    let cut = snapshot.catalog_cut();
    let paths = cut.and_then(|_| snapshot.catalog_file_paths());
    if let Some(hold) = snapshot.catalog_read_permit::<HeldCatalogRead>()
        && storage.holds_catalog_with_paths(&hold, cut, paths.as_deref())
    {
        return Ok(hold);
    }
    if let Some(cut) = cut {
        let anchor = match snapshot.table_catalog().block() {
            Some(block) => block.boundary_replica_position().zip(block.publication_sha256())
                .map(|(boundary, root)| rtdb_types::CatalogCheckpoint { boundary, root }),
            None => Some(rtdb_types::CatalogCheckpoint {
                boundary: rtdb_types::ReplicaPosition::from_epoch_offset(cut.replica_epoch, -1)?,
                root: [0; 32],
            }),
        };
        if let Some(anchor) = anchor {
            return storage.hold_anchored_catalog(cut, anchor, paths).await;
        }
    }
    storage.hold_catalog_with_paths(cut, paths).await
}

#[derive(Clone, Debug)]
pub struct OrderedIndexStorage {
    pub table: TableRef,
    pub name: String,
    pub version: i64,
    pub through_block: i64,
    pub bytes: i64,
}

#[async_trait::async_trait]
pub trait OrderedIndexInventory: Send + Sync {
    /// Resolve the published index files for the database in this query's captured catalog.
    async fn sample(&self, database: &str, context: &QueryContext) -> Result<rtdb_plan::CatalogRows<OrderedIndexStorage>>;
}

#[derive(Clone)]
pub struct CatalogSnapshot {
    pub(crate) default_database: String,
    pub(crate) databases: Arc<BTreeMap<String, DatabaseSnapshot>>,
    transaction_scoped: bool,
    owned_relation: Option<(QualifiedTable, Option<rtdb_types::EntityId>)>,
    pub(crate) owned_position_mask: Option<(Timestamp, u32)>,
    pub(crate) owned_transaction_cut: Option<rtdb_types::TxId>,
    owned_read_guard: Option<Arc<dyn Fn() -> Result<()> + Send + Sync>>,
    pub(crate) runtime: Option<Arc<dyn CatalogRuntime>>,
    pub(crate) ordered_indexes: Option<Arc<dyn OrderedIndexInventory>>,
    pub(crate) fragmenter: Option<Arc<dyn crate::fragment::ScanFragmenter>>,
    pub(crate) fragment_residence: Option<crate::FragmentResidence>,
    read_lease: Option<Arc<rtdb_storage::ReadLease>>,
    read_view: Arc<tokio::sync::OnceCell<Arc<rtdb_storage::HeldView>>>,
    snapshot_suffix: Option<Arc<dyn Send + Sync>>,
    resumed_storage_owner: Option<Arc<BufferPool>>,
}

impl CatalogSnapshot {
    pub fn new(default_database: impl Into<String>, databases: BTreeMap<String, DatabaseSnapshot>, transaction_scoped: bool) -> Result<Self> {
        let default_database = default_database.into();
        if !databases.contains_key(&default_database) { return Err(invalid("missing-default-database", "Default database has no pinned snapshot")); }
        if transaction_scoped && databases.len() != 1 { return Err(invalid("transaction-catalog-scope", "An index-time query must contain exactly its transaction's database")); }
        if transaction_scoped && databases.get(&default_database).is_none_or(|database| database.partitions.len() != 1) {
            return Err(invalid("transaction-partition-scope", "An index-time query must contain exactly its resolving partition"));
        }
        Ok(Self { default_database, databases: Arc::new(databases), transaction_scoped, owned_relation: None, owned_position_mask: None, owned_transaction_cut: None, owned_read_guard: None, runtime: None, ordered_indexes: None, fragmenter: None, fragment_residence: None, read_lease: None, read_view: Arc::default(), snapshot_suffix: None, resumed_storage_owner: None })
    }
    pub(crate) fn uses_owned_event_cut(&self) -> bool {
        self.owned_transaction_cut.is_some() || self.owned_position_mask.is_some()
    }

    pub fn with_runtime(mut self, runtime: Arc<dyn CatalogRuntime>) -> Self { self.runtime = Some(runtime); self }
    pub fn with_ordered_indexes(mut self, inventory: Arc<dyn OrderedIndexInventory>) -> Self {
        self.ordered_indexes = Some(inventory);
        self
    }
    /// Splits this catalog's scans into fragments wherever `fragmenter` asks.
    pub fn with_fragmenter(mut self, fragmenter: Option<Arc<dyn crate::fragment::ScanFragmenter>>, residence: Option<crate::FragmentResidence>) -> Self {
        self.fragmenter = fragmenter;
        self.fragment_residence = residence;
        self
    }
    /// Provider requests for this catalog's files begin only while the lease admits them.
    pub fn with_read_lease(mut self, lease: Arc<rtdb_storage::ReadLease>) -> Self { self.read_lease = Some(lease); self }
    pub fn read_lease(&self) -> Option<Arc<rtdb_storage::ReadLease>> { self.read_lease.clone() }
    pub(crate) fn reset_sql_snapshot_view(
        &mut self, suffix: Option<Arc<dyn Send + Sync>>, owner: Option<Arc<BufferPool>>,
    ) {
        self.read_view = Arc::default();
        self.snapshot_suffix = suffix;
        self.resumed_storage_owner = owner;
        self.fragmenter = None;
        self.fragment_residence = None;
    }
    /// Checks the placement that authorized the captured or reconstructed catalog.
    pub fn belongs_to_storage(&self, database: &str, current: &Arc<BufferPool>) -> Result<bool> {
        let captured = self.database(database)?;
        let owner = self.resumed_storage_owner.as_ref().filter(|_| database == self.default_database)
            .unwrap_or(&captured.storage);
        Ok(Arc::ptr_eq(owner, current))
    }

    pub(crate) async fn held_view(&self) -> Result<Arc<rtdb_storage::HeldView>> {
        self.check_view_lease()?;
        self.read_view.get_or_try_init(|| async {
            let mut registrations = Vec::new();
            for database in self.databases.values() {
                for snapshot in &database.partitions {
                    registrations.push(hold_snapshot(snapshot, &database.storage).await?);
                }
            }
            let owners = Arc::new((Arc::clone(&self.databases), self.snapshot_suffix.clone()));
            Ok(Arc::new(rtdb_storage::HeldView::new(owners, registrations)))
        }).await.map(Arc::clone)
    }
    pub(crate) fn check_view_lease(&self) -> Result<()> {
        if let Some(lease) = &self.read_lease { lease.check()?; }
        Ok(())
    }
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
    pub(crate) fn owned_identity(&self) -> Option<&rtdb_types::EntityId> {
        self.owned_relation.as_ref().and_then(|(_, id)| id.as_ref())
    }
    pub(crate) fn permits_table(&self, name: &QualifiedTable) -> bool {
        match &self.owned_relation {
            Some((admitted, _)) => admitted == name,
            None => !TableRef::owned_schema(&name.table.schema),
        }
    }
    fn binding(&self, database: &str, table: &TableRef) -> Result<Option<TableBinding>> {
        let name = QualifiedTable { database: database.to_owned(), table: table.clone() };
        if !self.permits_table(&name) { return Ok(None); }
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
        Ok(Some(TableBinding { name, oid, columns: columns.unwrap_or_default() }))
    }
}

impl Catalog for CatalogSnapshot {
    fn default_database(&self) -> &str { &self.default_database }
    fn databases(&self) -> BTreeSet<String> { self.databases.keys().cloned().collect() }
    fn table(&self, name: &QualifiedTable) -> Result<Option<TableBinding>> { self.binding(&name.database, &name.table) }
    fn tables(&self, database: &str) -> Result<Vec<TableBinding>> {
        if let Some((admitted, _)) = &self.owned_relation {
            return if admitted.database == database {
                Ok(self.table(admitted)?.into_iter().collect())
            } else {
                Ok(Vec::new())
            };
        }
        let names: BTreeSet<_> = self.database(database)?.partitions.iter().flat_map(Snapshot::tables).filter(|table| !TableRef::owned_schema(&table.schema)).collect();
        let names: BTreeSet<_> = names.into_iter().chain(crate::system_schema::DEFINITIONS.iter().map(|definition| TableRef::new(definition.schema, definition.name))).collect();
        names.iter().map(|table| self.binding(database, table)?.ok_or_else(|| invalid("missing-table", "Enumerated table has no binding"))).collect()
    }
    fn transaction_scoped(&self) -> bool { self.transaction_scoped }
}

impl rtdb_expr::PgCatalogLookup for CatalogSnapshot {
    fn relations(&self, allocation: &rtdb_expr::EvaluationScope, visit: &mut dyn FnMut(i32, &TableRef) -> Result<()>) -> Result<()> {
        for (name, database) in self.databases.iter() {
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
    pub materialized_spill_rows: usize,
    pub bypass_temporal_pruning: bool,
    /// SQL GENERATE_SERIES excludes its endpoint when the process compatibility setting is enabled.
    pub generate_series_end_exclusive: bool,
    /// Stable object-index algorithm for sort runs and complete percentile groups.
    pub object_sort: rtdb_types::ObjectSort,
    /// The bytes a query admits against the node's budget at a time; zero admits each reservation as it is made.
    pub memory_quantum: usize,
}
impl QueryOptions {
    /// The budget a query reserves from: `node` itself, or a child admitting against it `memory_quantum` bytes at a time.
    pub fn query_memory(&self, node: &Arc<MemoryBudget>) -> Arc<MemoryBudget> {
        if self.memory_quantum == 0 { Arc::clone(node) } else { node.quantized(self.memory_quantum) }
    }
}
impl Default for QueryOptions {
    fn default() -> Self { Self { batch_rows: 1024, list_batch_rows: 1024, sort_memory_bytes: 64 * 1024 * 1024, join_build_rows: 100_000, spill_limit_bytes: 1024 * 1024 * 1024, materialized_spill_rows: 100_000, bypass_temporal_pruning: false, generate_series_end_exclusive: false, object_sort: rtdb_types::ObjectSort::default(), memory_quantum: 0 } }
}

#[derive(Clone)]
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
    pub(crate) application_name: Option<(Arc<str>, Arc<rtdb_columnar::MemoryReservation>)>,
    pub(crate) operator_root: Option<Arc<Self>>,
    pub(crate) read_basis: Option<Arc<crate::settings::ReadBasis>>,
    pub(crate) snapshot_admission: Option<Arc<dyn crate::settings::SnapshotAdmission>>,
    pub(crate) snapshot_tokens: Option<Arc<crate::settings::SnapshotTokenIssuer>>,
    pub(crate) accelerators: Arc<[Arc<dyn crate::ScanAccelerator>]>,
    pub(crate) query_cpu: Option<Arc<crate::QueryCpu>>,
    plaintext: Option<Arc<PlaintextView>>,
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
        Ok(Arc::new(Self { catalog, memory, current_time: current_time.into(), timezone: timezone.into(), options, cancellation: parent_cancellation.child_token(), owner_cancellation: owner_cancellation.child_token(), spill, execution_observer: None, tracer: None, trace_parent: None, application_name: None, operator_root: None, read_basis: None, snapshot_admission: None, snapshot_tokens: None, accelerators: Arc::from(Vec::new()), query_cpu: None, plaintext: None }))
    }
    /// Applies only to cursors opened from this context; existing cursors retain their original observer.
    pub fn with_observer(mut self: Arc<Self>, observer: Option<Arc<dyn rtdb_plan::ExecutionObserver>>) -> Arc<Self> {
        Arc::make_mut(&mut self).execution_observer = observer;
        self
    }
    /// Scans opened from this context consult these before opening; see `ScanAccelerator`.
    pub fn with_accelerators(mut self: Arc<Self>, accelerators: Arc<[Arc<dyn crate::ScanAccelerator>]>) -> Arc<Self> {
        Arc::make_mut(&mut self).accelerators = accelerators;
        self
    }
    /// Applies explicit span parentage to subsequently opened executions without thread-local context.
    pub fn with_tracer(mut self: Arc<Self>, tracer: Option<Arc<dyn rtdb_plan::Tracer>>, parent: Option<rtdb_plan::SpanContext>) -> Arc<Self> {
        let context = Arc::make_mut(&mut self);
        context.tracer = tracer;
        context.trace_parent = parent;
        self
    }

    pub fn with_application_name(mut self: Arc<Self>, name: Option<&str>) -> Arc<Self> {
        if self.tracer.as_ref().is_some_and(|tracer| tracer.query_enabled()) {
            let application_name = name.and_then(|name| {
                let reservation = self.memory.reserve(name.len().checked_add(64)?).ok()?;
                Some((Arc::from(name), reservation))
            });
            Arc::make_mut(&mut self).application_name = application_name;
        }
        self
    }

    pub fn with_query_cpu(mut self: Arc<Self>, counter: Option<Arc<crate::QueryCpu>>) -> Arc<Self> {
        if let Some(counter) = counter { Arc::make_mut(&mut self).query_cpu = Some(counter); }
        self
    }

    pub(crate) fn for_owned_relation(mut self: Arc<Self>, table: QualifiedTable, id: Option<rtdb_types::EntityId>) -> Arc<Self> {
        let context = Arc::make_mut(&mut self);
        context.catalog.owned_relation = Some((table, id));
        context.catalog.runtime = None;
        context.catalog.fragmenter = None;
        context.accelerators = Default::default();
        self
    }

    pub fn with_owned_read_guard(mut self: Arc<Self>, guard: Arc<dyn Fn() -> Result<()> + Send + Sync>) -> Arc<Self> {
        let slot = &mut Arc::make_mut(&mut self).catalog.owned_read_guard;
        *slot = Some(if let Some(base) = slot.take() {
            Arc::new(move || { base()?; guard() })
        } else {
            guard
        });
        self
    }

    /// Holds the selected catalogs from before the first read through answer production.
    pub async fn reading<T>(self: &Arc<Self>, work: impl std::future::Future<Output = Result<T>>) -> Result<T> {
        self.check_cancelled()?;
        let view = self.owner_cancellation.run_until_cancelled(
            self.cancellation.run_until_cancelled(self.catalog.held_view()),
        ).await.flatten().ok_or_else(cancelled)??;
        let lease = self.catalog.read_lease();
        let work = rtdb_storage::IoRegion::retaining(self.clone(), rtdb_storage::IoRegion::leased(lease.clone(), work));
        let plaintext = self.plaintext.clone().unwrap_or_else(|| PlaintextView::new(Arc::clone(&self.memory)));
        let work = plaintext.reading(async {
            for partition in self.catalog.databases.values().flat_map(|database| &database.partitions) {
                if let Some(read) = partition.certified_read() {
                    PlaintextView::track_certified_read(read)?;
                }
            }
            let work = self.owner_cancellation.run_until_cancelled(self.cancellation.run_until_cancelled(work));
            let answer = if let Some(lease) = lease {
                let deadline = lease.local_cutoff()?;
                tokio::time::timeout_at(deadline.into(), work).await.map_err(|_| {
                    Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.storage/read-lease-expired", "The read lease expired while computing the answer")
                })?
            } else {
                work.await
            }.flatten().ok_or_else(cancelled)??;
            self.grant_delivery(&plaintext).await?;
            Ok(answer)
        });
        rtdb_storage::IoRegion::reading(view, work).await
    }

    /// Attach the exact catalog used by a read-only action to its answer transport.
    pub async fn retain_delivery_cut(&self, cutoff: Arc<rtdb_storage::ReadLease>) -> Result<()> {
        self.check_cancelled()?;
        let catalog = self.catalog.read_lease().ok_or_else(|| Anomaly::new(
            AnomalyCategory::Unavailable, "rtdb.query/no-delivery-lease", "The answer has no protected catalog lease",
        ))?;
        let lease = catalog.earlier(cutoff);
        lease.check()?;
        let view = self.catalog.held_view().await?;
        for partition in self.catalog.databases.values().flat_map(|database| &database.partitions) {
            if let Some(read) = partition.certified_read() {
                PlaintextView::track_certified_read(read)?;
            }
        }
        PlaintextView::track_protected_cut(view, lease)
    }

    pub(crate) fn with_plaintext(mut self: Arc<Self>) -> Arc<Self> {
        if self.plaintext.is_none() {
            let view = PlaintextView::new(Arc::clone(&self.memory));
            Arc::make_mut(&mut self).plaintext = Some(view);
        }
        self
    }

    pub(crate) async fn delivery_grant(self: &Arc<Self>) -> Result<PlaintextDelivery> {
        let plaintext = self.plaintext.clone().unwrap_or_else(|| PlaintextView::new(Arc::clone(&self.memory)));
        let view = self.catalog.held_view().await?;
        let lease = self.catalog.read_lease();
        let work = rtdb_storage::IoRegion::leased(lease.clone(), self.grant_delivery(&plaintext));
        let grant = rtdb_storage::IoRegion::reading(view.clone(), work).await?;
        Ok(grant.holding(view, lease))
    }

    async fn grant_delivery(&self, plaintext: &PlaintextView) -> Result<PlaintextDelivery> {
        self.check_cancelled()?;
        let grant = self.owner_cancellation.run_until_cancelled(
            self.cancellation.run_until_cancelled(plaintext.grant()),
        ).await.flatten().ok_or_else(cancelled)??;
        self.catalog.check_view_lease()?;
        self.check_cancelled()?;
        grant.check()?;
        Ok(grant)
    }

    /// An idle transport must release buffered results and their ordered catalog hold by this cutoff.
    pub fn retention_deadline(&self) -> Option<std::time::Instant> {
        self.catalog.read_lease().and_then(|lease| lease.provider_cutoff())
    }

    pub fn check_cancelled(&self) -> Result<()> {
        if self.cancellation.is_cancelled() || self.owner_cancellation.is_cancelled() { return Err(cancelled()); }
        if let Some(guard) = &self.catalog.owned_read_guard { guard()?; }
        Ok(())
    }
    /// Checks for cancellation at the first of every 1024 indices: reading a token takes its lock, which per row adds up.
    pub fn check_cancelled_at(&self, index: usize) -> Result<()> {
        if index % 1024 == 0 { self.check_cancelled() } else { Ok(()) }
    }
    pub fn spill_bytes(&self) -> u64 { self.spill.retained_bytes() }
    pub fn spill_peak_bytes(&self) -> u64 { self.spill.peak_bytes() }
    pub fn snapshot_token(&self) -> Option<&str> { self.read_basis.as_ref().and_then(|basis| basis.token.as_deref()) }
    pub(crate) fn execution_child(&self) -> Arc<Self> {
        self.execution_child_with_memory(self.memory.scope())
    }
    pub(crate) fn execution_child_with_memory(&self, memory: Arc<MemoryBudget>) -> Arc<Self> {
        Arc::new(Self { memory, cancellation: self.cancellation.child_token(), ..self.clone() })
    }

    pub(crate) fn operator_scope(self: &Arc<Self>) -> Arc<Self> {
        debug_assert!(self.operator_root.is_none());
        Arc::new(Self { memory: self.memory.scope(), operator_root: Some(Arc::clone(self)), ..(**self).clone() })
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

#[derive(Clone)]
enum OuterInput {
    Values(Arc<crate::cursor::OwnedRow>),
    Arrow { batch: Arc<crate::columnar::ColumnBatch>, row: usize },
}

impl OuterInput {
    fn expression(&self) -> rtdb_expr::ExpressionRow<'_> {
        match self {
            Self::Values(row) => rtdb_expr::ExpressionRow::Values(&row.values),
            Self::Arrow { .. } => rtdb_expr::ExpressionRow::Borrowed(self),
        }
    }
}

impl rtdb_expr::RowInput for OuterInput {
    fn value(&self, index: usize, scope: Option<&rtdb_expr::EvaluationScope>) -> Result<Option<Value>> {
        let Self::Arrow { batch, row } = self else { return Ok(None); };
        batch.columns.get(index).map(|column| column.expression_value(*row, false, scope)).transpose()
    }

    fn arrow_cell(&self, index: usize) -> Option<rtdb_columnar::ArrowCell<'_>> {
        let Self::Arrow { batch, row } = self else { return None; };
        batch.columns.get(index)?.expression_cell(*row)
    }
}

pub(crate) struct OuterRows { rows: Vec<(rtdb_plan::Schema, OuterInput)>, _reservation: Arc<rtdb_columnar::MemoryReservation> }
impl OuterRows {
    fn expression_rows(&self) -> Vec<rtdb_expr::OuterRow<'_>> {
        self.rows.iter().map(|(schema, row)| rtdb_expr::OuterRow {
            schema,
            values: match row {
                OuterInput::Values(row) => &row.values,
                OuterInput::Arrow { .. } => &[],
            },
        }).collect()
    }

    pub fn schemas(&self) -> impl Iterator<Item = &rtdb_plan::Schema> { self.rows.iter().map(|(schema, _)| schema) }
    pub fn empty(memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let reservation = memory.reserve(256)?;
        Ok(Arc::new(Self { rows: Vec::new(), _reservation: reservation }))
    }
    fn prepend(&self, schema: &rtdb_plan::Schema, row: OuterInput, memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let bound = self.rows.iter().try_fold(crate::runtime_schema::schema_bound(schema)?, |bytes, (schema, _)| {
            bytes.checked_add(crate::runtime_schema::schema_bound(schema)?).ok_or_else(|| crate::invalid("outer-size", "Outer schema allocation exceeds addressable memory"))
        })?;
        let reservation = memory.reserve(bound)?;
        let mut rows = Vec::with_capacity(self.rows.len() + 1); rows.push((schema.clone(), row)); rows.extend(self.rows.iter().cloned());
        Ok(Arc::new(Self { rows, _reservation: reservation }))
    }
}

struct ExpressionInputs<'a> { parameters: &'a crate::parameters::BoundParameters, outer: &'a OuterRows }

impl rtdb_expr::ParameterLookup for ExpressionInputs<'_> {
    fn width(&self) -> usize { rtdb_expr::ParameterLookup::width(self.parameters) }
    fn value(&self, position: usize) -> Result<Option<&Value>> { self.parameters.get(position) }
    fn known_value(&self, position: usize) -> Option<&Value> { rtdb_expr::ParameterLookup::known_value(self.parameters, position) }
    fn arrow_cell(&self, position: usize) -> Option<rtdb_columnar::ArrowCell<'_>> { rtdb_expr::ParameterLookup::arrow_cell(self.parameters, position) }

    fn correlated_value(&self, depth: usize, position: usize, scope: Option<&rtdb_expr::EvaluationScope>) -> Result<Option<Value>> {
        self.outer.rows.get(depth).map_or(Ok(None), |(_, row)| rtdb_expr::RowInput::value(row, position, scope))
    }
}

impl Execution {
    pub async fn bind_arguments(&self, arguments: &[(usize, rtdb_plan::Expr)], schema: &rtdb_plan::Schema, row: Arc<crate::cursor::OwnedRow>) -> Result<Self> {
        self.bind_outer(arguments, schema, OuterInput::Values(row)).await
    }

    pub async fn bind_arrow_arguments(&self, arguments: &[(usize, rtdb_plan::Expr)], schema: &rtdb_plan::Schema,
        batch: Arc<crate::columnar::ColumnBatch>, row: usize) -> Result<Self> {
        self.bind_outer(arguments, schema, OuterInput::Arrow { batch, row }).await
    }

    async fn bind_outer(&self, arguments: &[(usize, rtdb_plan::Expr)], schema: &rtdb_plan::Schema, row: OuterInput) -> Result<Self> {
        let scope = self.scope()?;
        let parameter_count = arguments.iter().try_fold(self.parameters.types().len(), |count, (index, _)| index.checked_add(1).map(|next| count.max(next)).ok_or_else(|| crate::invalid("parameter-count", "Apply parameter index overflow")))?;
        let slots = parameter_count.checked_mul(std::mem::size_of::<rtdb_types::LogicalType>())
            .and_then(|bytes| bytes.checked_add(arguments.len().checked_mul(std::mem::size_of::<(usize, Value)>())?))
            .ok_or_else(|| crate::invalid("parameter-count", "Apply parameters exceed addressable memory"))?;
        let existing = self.parameters.types().iter().try_fold(slots, |bytes, ty| {
            bytes.checked_add(ty.retained_bytes()?).ok_or_else(|| crate::invalid("parameter-count", "Apply parameters exceed addressable memory"))
        })?;
        scope.reserve(existing)?;
        let mut parameters = Vec::with_capacity(arguments.len());
        let mut types = Vec::with_capacity(parameter_count);
        types.extend_from_slice(self.parameters.types());
        types.resize(parameter_count, rtdb_types::LogicalType::Null);
        for (index, expression) in arguments {
            types[*index] = self.expression_type(&scope, expression, schema)?;
            let invocation = self.prepare_expression(expression, schema, &scope)?;
            parameters.push((*index, self.evaluate_invocation_row(&scope, &invocation, row.expression()).await?));
        }
        let outer = self.outer.prepend(schema, row, &self.context.memory)?;
        Ok(Self { context: Arc::clone(&self.context), parameters: Arc::new(self.parameters.overlay(parameters, types, &self.context.memory)?), outer, bindings: self.bindings.clone(), analysis: self.analysis.clone() })
    }
    pub fn in_operator(mut self) -> Self {
        if self.context.operator_root.is_none() && (self.analysis.is_some() || self.context.trace_parent.is_some()) {
            self.context = self.context.operator_scope();
        }
        self
    }

    pub fn child(&self, index: usize) -> Self {
        let mut execution = self.clone();
        if let Some(root) = &self.context.operator_root { execution.context = Arc::clone(root); }
        execution.analysis = self.analysis.as_ref().and_then(|node| node.child(index));
        execution
    }
    pub fn scope(&self) -> Result<rtdb_expr::EvaluationScope> { rtdb_expr::EvaluationScope::new(&self.context.memory) }
    pub fn expression_type(&self, scope: &rtdb_expr::EvaluationScope, expression: &rtdb_plan::Expr, schema: &rtdb_plan::Schema) -> Result<rtdb_types::LogicalType> {
        scope.reserve(self.outer.rows.len().checked_mul(std::mem::size_of::<rtdb_expr::OuterRow<'_>>()).ok_or_else(|| Anomaly::incorrect("rtdb.query/expression-size", "Outer row bindings exceed addressable memory"))?)?;
        let outer_rows = self.outer.expression_rows();
        let inputs = ExpressionInputs { parameters: &self.parameters, outer: &self.outer };
        let context = rtdb_expr::EvalContext { parameters: rtdb_expr::Parameters::Lookup(&inputs),parameter_types: Some(self.parameters.types()),current_time: self.context.current_time,timezone: &self.context.timezone,outer_rows: &outer_rows,snapshot_token: self.context.snapshot_token(),allocation: Some(scope),catalog: Some(&self.context.catalog) };
        rtdb_expr::expression_type(expression,schema,&context)
    }
    pub async fn evaluate(&self, scope: &rtdb_expr::EvaluationScope, expression: &rtdb_plan::Expr, schema: &rtdb_plan::Schema, row: &[Value]) -> Result<Value> {
        let invocation = self.prepare_expression(expression,schema,scope)?;
        self.evaluate_invocation(scope,&invocation,row).await
    }
    pub fn prepare_expression<'a>(&self, expression: &'a rtdb_plan::Expr, schema: &'a rtdb_plan::Schema, scope: &rtdb_expr::EvaluationScope) -> Result<rtdb_expr::ExpressionInvocation<'a>> {
        self.context.check_cancelled()?;
        scope.reserve(self.outer.rows.len().checked_mul(std::mem::size_of::<rtdb_expr::OuterRow<'_>>()).ok_or_else(|| Anomaly::incorrect("rtdb.query/expression-size", "Outer row bindings exceed addressable memory"))?)?;
        let outer_rows = self.outer.expression_rows();
        let inputs = ExpressionInputs { parameters: &self.parameters, outer: &self.outer };
        let context = rtdb_expr::EvalContext { parameters: rtdb_expr::Parameters::Lookup(&inputs),parameter_types: Some(self.parameters.types()),current_time: self.context.current_time,timezone: &self.context.timezone,outer_rows: &outer_rows,snapshot_token: self.context.snapshot_token(),allocation: Some(scope),catalog: Some(&self.context.catalog) };
        rtdb_expr::ExpressionInvocation::prepare(expression,schema,&context)
    }
    pub async fn evaluate_invocation(&self, scope: &rtdb_expr::EvaluationScope, invocation: &rtdb_expr::ExpressionInvocation<'_>, row: &[Value]) -> Result<Value> {
        self.evaluate_invocation_row(scope, invocation, rtdb_expr::ExpressionRow::Values(row)).await
    }
    pub async fn evaluate_invocation_row(
        &self, scope: &rtdb_expr::EvaluationScope, invocation: &rtdb_expr::ExpressionInvocation<'_>,
        row: rtdb_expr::ExpressionRow<'_>,
    ) -> Result<Value> {
        self.context.check_cancelled()?;
        scope.reserve(4096)?;
        scope.reserve(self.outer.rows.len().checked_mul(std::mem::size_of::<rtdb_expr::OuterRow<'_>>()).ok_or_else(|| Anomaly::incorrect("rtdb.query/expression-size", "Outer row bindings exceed addressable memory"))?)?;
        let outer_rows = self.outer.expression_rows();
        let inputs = ExpressionInputs { parameters: &self.parameters, outer: &self.outer };
        let context = rtdb_expr::EvalContext { parameters: rtdb_expr::Parameters::Lookup(&inputs), parameter_types: Some(self.parameters.types()), current_time: self.context.current_time, timezone: &self.context.timezone, outer_rows: &outer_rows, snapshot_token: self.context.snapshot_token(), allocation: Some(scope), catalog: Some(&self.context.catalog) };
        self.context.owner_cancellation.run_until_cancelled(self.context.cancellation.run_until_cancelled(
            invocation.evaluate_projection_row_async(row,&context))).await.flatten().ok_or_else(cancelled)?
    }
    pub async fn open_list(&self, expression: rtdb_plan::Expr, page_rows: Option<std::num::NonZeroUsize>) -> Result<crate::list::ListCursor> {
        let execution = self.clone();
        let context = Arc::clone(&self.context);
        let stream = async_stream::try_stream! {
            let scope = execution.scope()?;
            let schema = rtdb_plan::Schema::default();
            scope.reserve(execution.outer.rows.len().checked_mul(std::mem::size_of::<rtdb_expr::OuterRow<'_>>()).ok_or_else(|| crate::invalid("outer-size", "Outer row bindings exceed addressable memory"))?)?;
            let outer_rows = execution.outer.expression_rows();
            let inputs = ExpressionInputs { parameters: &execution.parameters, outer: &execution.outer };
            let eval = rtdb_expr::EvalContext { parameters: rtdb_expr::Parameters::Lookup(&inputs), parameter_types: Some(execution.parameters.types()), current_time: execution.context.current_time,
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
    pub async fn evaluate_list_invocation(&self, scope: &rtdb_expr::EvaluationScope, invocation: &rtdb_expr::ExpressionInvocation<'_>, row: rtdb_expr::ExpressionRow<'_>) -> Result<rtdb_expr::EvaluatedList> {
        self.context.check_cancelled()?;
        scope.reserve(4096)?;
        scope.reserve(self.outer.rows.len().checked_mul(std::mem::size_of::<rtdb_expr::OuterRow<'_>>()).ok_or_else(|| Anomaly::incorrect("rtdb.query/expression-size", "Outer row bindings exceed addressable memory"))?)?;
        let outer_rows = self.outer.expression_rows();
        let inputs = ExpressionInputs { parameters: &self.parameters, outer: &self.outer };
        let context = rtdb_expr::EvalContext { parameters: rtdb_expr::Parameters::Lookup(&inputs), parameter_types: Some(self.parameters.types()), current_time: self.context.current_time, timezone: &self.context.timezone, outer_rows: &outer_rows, snapshot_token: self.context.snapshot_token(), allocation: Some(scope), catalog: Some(&self.context.catalog) };
        self.context.owner_cancellation.run_until_cancelled(self.context.cancellation.run_until_cancelled(
            invocation.evaluate_list_row_async(row,&context))).await.flatten().ok_or_else(cancelled)?
    }
}
