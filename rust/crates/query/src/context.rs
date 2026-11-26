use rtdb_types::StructFields;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, OnceLock};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_index::Snapshot;
use rtdb_plan::{Catalog, CatalogRuntime, QualifiedTable, TableBinding};
use rtdb_storage::{cache::{BufferPool, CatalogAdmission}, sealing::{PlaintextDelivery, PlaintextView}};
use rtdb_types::{Anomaly, AnomalyCategory, LogicalType, Result, TableRef, TransactionKey, Value};
#[cfg(test)]
use rtdb_types::Timestamp;
use tokio_util::sync::CancellationToken;

use crate::{
    invalid,
    parameters::{ArrowBinding, OverlayValue, ParameterTypes},
    runtime_schema::RuntimeSchema,
    spill::SpillRegion,
};

#[derive(Clone)]
pub struct DatabaseSnapshot { pub partitions: Vec<Snapshot>, pub storage: Arc<BufferPool> }

/// The custody the storage owner grants for reading this snapshot's captured catalog, reusing custody already attached
/// to the snapshot. Only a hosted journal grants any; every other store keeps no reader state and returns `None`.
async fn admit_snapshot(snapshot: &Snapshot, storage: &BufferPool) -> Result<Option<Arc<dyn Send + Sync>>> {
    let cut = snapshot.catalog_cut();
    if let Some(admission) = snapshot.catalog_read_permit::<CatalogAdmission>()
        && storage.admitted(&admission, cut)
    {
        return Ok(Some(admission as Arc<dyn Send + Sync>));
    }
    let checkpoint = match cut { Some(_) => snapshot.catalog_checkpoint()?, None => None };
    Ok(storage.admit_catalog(cut, checkpoint).await?.map(|admission| admission as Arc<dyn Send + Sync>))
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
    databases: Arc<BTreeMap<String, DatabaseSnapshot>>,
    transaction_scoped: bool,
    archive_history: bool,
    /// Derived from `databases` and `owned_relation`; reset by every change to either.
    relation_names: Arc<Mutex<Option<Arc<RelationNames>>>>,
    owned_relation: Option<(QualifiedTable, OwnedSelection)>,
    /// The masked commit, named by its position (T7), and its first masked commit position.
    pub(crate) owned_position_mask: Option<(TransactionKey, u32)>,
    pub(crate) owned_transaction_cut: Option<rtdb_types::TxId>,
    owned_read_guard: Option<Arc<dyn Fn() -> Result<()> + Send + Sync>>,
    pub(crate) runtime: Option<Arc<dyn CatalogRuntime>>,
    pub(crate) ordered_indexes: Option<Arc<dyn OrderedIndexInventory>>,
    pub(crate) fragmenter: Option<Arc<dyn crate::fragment::ScanFragmenter>>,
    pub(crate) fragment_residence: Option<crate::FragmentResidence>,
    read_lease: Option<Arc<rtdb_storage::ReadLease>>,
    read_view: Arc<tokio::sync::OnceCell<Arc<rtdb_storage::HeldView>>>,
    snapshot_suffix: Option<Arc<dyn Send + Sync>>,
    delivery_owner: Option<(Arc<dyn Send + Sync>, std::time::Instant)>,
    resumed_storage_owner: Option<Arc<BufferPool>>,
}

#[derive(Clone)]
pub(crate) enum OwnedSelection {
    Relation,
    PositionedProjection,
    Point(rtdb_types::EntityId),
    Artefacts(Arc<OwnedArtefactKeys>),
    Candidates(crate::CandidateIids),
}

pub(crate) struct OwnedArtefactKeys {
    pub(crate) by_iid: BTreeMap<rtdb_types::Iid, Vec<arrow_buffer::Buffer>>,
    pub(crate) _reservation: Arc<rtdb_columnar::MemoryReservation>,
}

impl CatalogSnapshot {
    pub fn new(default_database: impl Into<String>, databases: BTreeMap<String, DatabaseSnapshot>, transaction_scoped: bool) -> Result<Self> {
        let default_database = default_database.into();
        if !databases.contains_key(&default_database) { return Err(invalid("missing-default-database", "Default database has no pinned snapshot")); }
        if transaction_scoped && databases.len() != 1 { return Err(invalid("transaction-catalog-scope", "An index-time query must contain exactly its transaction's database")); }
        if transaction_scoped && databases.get(&default_database).is_none_or(|database| database.partitions.len() != 1) {
            return Err(invalid("transaction-partition-scope", "An index-time query must contain exactly its resolving partition"));
        }
        Ok(Self { default_database, databases: Arc::new(databases), transaction_scoped, archive_history: false, relation_names: Default::default(), owned_relation: None, owned_position_mask: None, owned_transaction_cut: None, owned_read_guard: None, runtime: None, ordered_indexes: None, fragmenter: None, fragment_residence: None, read_lease: None, read_view: Arc::default(), snapshot_suffix: None, delivery_owner: None, resumed_storage_owner: None })
    }
    /// Enable explicit positional control history on a dedicated archive reader.
    pub fn with_archive_history(mut self) -> Self {
        self.archive_history = true;
        self
    }

    pub(crate) fn admits_archive_history(&self, name: &QualifiedTable) -> bool {
        self.archive_history && self.owned_relation.is_none()
            && name.database == self.default_database
            && name.table.schema == "public"
            && name.table.table.starts_with("bj_")
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
    pub fn read_lease(&self) -> Option<Arc<rtdb_storage::ReadLease>> {
        let cutoff = self.databases.values().flat_map(|database| &database.partitions)
            .filter_map(Snapshot::read_custody_deadline)
            .chain(self.delivery_owner.as_ref().map(|(_, cutoff)| *cutoff)).min();
        match (self.read_lease.clone(), cutoff) {
            (lease, None) => lease,
            (Some(lease), Some(cutoff)) => Some(lease.earlier(Arc::new(rtdb_storage::ReadLease::until(cutoff)))),
            (None, Some(cutoff)) => Some(Arc::new(rtdb_storage::ReadLease::until(cutoff))),
        }
    }
    pub(crate) fn database_snapshots(&self) -> &Arc<BTreeMap<String, DatabaseSnapshot>> { &self.databases }
    /// Clones keep sharing the relation-name memo of the snapshots they were cloned with; this one leaves it.
    pub(crate) fn databases_mut(&mut self) -> &mut BTreeMap<String, DatabaseSnapshot> {
        self.relation_names = Default::default();
        Arc::make_mut(&mut self.databases)
    }
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
        for database in self.databases.values().filter(|database| database.storage.object_store().local_erasure_custody().is_some()) {
            for snapshot in &database.partitions {
                if snapshot.catalog_cut().is_some() || snapshot.read_custody_deadline().is_none() {
                    database.storage.object_store().require_erasure_catalog(snapshot.catalog_cut()).await?;
                }
            }
        }
        self.read_view.get_or_try_init(|| async {
            let mut admissions = Vec::new();
            for database in self.databases.values() {
                for snapshot in &database.partitions {
                    if snapshot.catalog_cut().is_some() || snapshot.read_custody_deadline().is_none() {
                        admissions.extend(admit_snapshot(snapshot, &database.storage).await?);
                    }
                }
            }
            let owners = Arc::new((Arc::clone(&self.databases), self.snapshot_suffix.clone(), self.delivery_owner.clone()));
            Ok(Arc::new(rtdb_storage::HeldView::new(owners, admissions)))
        }).await.map(Arc::clone)
    }
    pub(crate) fn check_view_lease(&self) -> Result<()> {
        if let Some(lease) = self.read_lease() { lease.check()?; }
        Ok(())
    }
    pub fn database_names(&self) -> impl Iterator<Item = &str> { self.databases.keys().map(String::as_str) }

    /// Lists visible table identities in schema and table order under a name-only reservation.
    pub fn table_names_reserved(&self, database: &str, memory: &Arc<MemoryBudget>) -> Result<rtdb_plan::CatalogRows<TableRef>> {
        let overflow = || invalid("catalog-size", "Table identities exceed addressable memory");
        let bytes = self.table_name_sources(database)?.try_fold(4096usize, |bytes, (schema, table)| {
            bytes.checked_add(schema.len()).and_then(|n| n.checked_add(table.len()))
                .and_then(|n| n.checked_add(512)).ok_or_else(overflow)
        })?;
        let reservation = memory.reserve(bytes)?;
        let names = self.table_name_sources(database)?.collect::<BTreeSet<_>>();
        let rows = names.into_iter().map(|(schema, table)| TableRef::new(schema, table)).collect();
        Ok(rtdb_plan::CatalogRows::new(rows, reservation))
    }

    fn table_name_sources<'a>(&'a self, database: &'a str) -> Result<impl Iterator<Item = (&'a str, &'a str)>> {
        let snapshot = self.database(database)?;
        let ordinary = self.owned_relation.is_none();
        let stored = snapshot.partitions.iter().filter(move |_| ordinary).flat_map(Snapshot::table_names)
            .filter(|name| !TableRef::owned_schema(&name.schema))
            .map(|name| (name.schema.as_str(), name.table.as_str()));
        let system = crate::system_schema::DEFINITIONS.iter().filter(move |_| ordinary)
            .map(|definition| -> (&'a str, &'a str) { (definition.schema, definition.name) });
        let owned = self.owned_relation.iter()
            .filter(move |(admitted, _)| {
                admitted.database == database && (crate::system_schema::definition(&admitted.table).is_some()
                    || snapshot.partitions.iter().any(|part| part.contains_table(&admitted.table)))
            })
            .map(|(admitted, _)| (admitted.table.schema.as_str(), admitted.table.table.as_str()));
        Ok(stored.chain(system).chain(owned))
    }

    /// Funds the logical field tree and join scratch for just this table.
    pub fn table_binding_reserved(&self, name: &QualifiedTable, memory: &Arc<MemoryBudget>) -> Result<Option<(TableBinding, Arc<MemoryReservation>)>> {
        if !self.permits_table(name) { return Ok(None); }
        self.binding_reserved(&name.database, &name.table, memory)
    }

    pub(crate) fn binding_reserved(&self, database: &str, table: &TableRef, memory: &Arc<MemoryBudget>) -> Result<Option<(TableBinding, Arc<MemoryReservation>)>> {
        let snapshot = self.database(database)?;
        let overflow = || invalid("catalog-size", "Table metadata allocation exceeds addressable memory");
        let base = [database.len(), table.schema.len(), table.table.len()]
            .into_iter().try_fold(4096usize, |bytes, len| bytes.checked_add(len).ok_or_else(overflow))?;
        let bytes = if let Some(definition) = crate::system_schema::definition(table) {
            base.checked_add(definition.allocation_bound(database)?).ok_or_else(overflow)?
        } else {
            snapshot.partitions.iter().filter(|part| part.contains_table(table))
                .try_fold(base, |bytes, part| {
                    bytes.checked_add(part.table_schema_allocation_bound(database, table)?).ok_or_else(overflow)
                })?
        };
        let reservation = memory.reserve(bytes)?;
        Ok(self.binding(database, table)?.map(|binding| (binding, reservation)))
    }

    pub(crate) fn catalog_table_oid(&self, database: &str, table: &TableRef) -> Result<Option<i32>> {
        let mut oid = None;
        if crate::system_schema::definition(table).is_none() {
            for snapshot in &self.database(database)?.partitions {
                if snapshot.contains_table(table) { include_table_oid(&mut oid, snapshot, table)?; }
            }
        }
        Ok(oid)
    }

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
        match self.owned_relation.as_ref() {
            Some((_, OwnedSelection::Point(id))) => Some(id),
            _ => None,
        }
    }
    pub(crate) fn admits_owned_event_positions(&self, name: &QualifiedTable) -> bool {
        matches!(&self.owned_relation,
            Some((table, OwnedSelection::Point(_) | OwnedSelection::PositionedProjection)) if table == name)
    }
    pub(crate) fn owned_artefact_keys(&self) -> Option<&OwnedArtefactKeys> {
        match self.owned_relation.as_ref() {
            Some((_, OwnedSelection::Artefacts(keys))) => Some(keys),
            _ => None,
        }
    }
    pub(crate) fn owned_candidates(&self, name: &QualifiedTable) -> Option<&crate::CandidateIids> {
        match self.owned_relation.as_ref() {
            Some((table, OwnedSelection::Candidates(iids))) if table == name => Some(iids),
            _ => None,
        }
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
            include_table_oid(&mut oid, snapshot, table)?;
            let next = snapshot.column_types(table);
            columns = Some(match columns { Some(previous) => rtdb_index::merge_column_types(&previous, &next), None => next });
        }
        let mut columns = columns.unwrap_or_default();
        if self.admits_archive_history(&name) {
            use rtdb_types::{ARCHIVE_TX_FROM, ARCHIVE_TX_TO, ScalarType};
            columns.insert(ARCHIVE_TX_FROM.into(), LogicalType::Scalar(ScalarType::I64));
            columns.insert(ARCHIVE_TX_TO.into(), LogicalType::Scalar(ScalarType::I64).maybe());
        }
        Ok(Some(TableBinding { name, oid, columns }))
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
        let names = self.table_name_sources(database)?.collect::<BTreeSet<_>>();
        names.into_iter().map(|(schema, table)| {
            self.binding(database, &TableRef::new(schema, table))?.ok_or_else(|| invalid("missing-table", "Enumerated table has no binding"))
        }).collect()
    }
    fn transaction_scoped(&self) -> bool { self.transaction_scoped }
}

fn include_table_oid(oid: &mut Option<i32>, snapshot: &Snapshot, table: &TableRef) -> Result<()> {
    if let Some(entry) = snapshot.table_entry(table) {
        let next = entry.oid as i32;
        if oid.is_some_and(|previous| previous != next) {
            return Err(Anomaly::fault("rtdb.query/partition-table-oid", "Partition catalogs disagree on table OID"));
        }
        *oid = Some(next);
    }
    Ok(())
}

struct RelationNames {
    by_oid: BTreeMap<i32, TableRef>,
    by_name: BTreeMap<String, BTreeMap<String, i32>>,
    _reservation: Arc<MemoryReservation>,
}

impl CatalogSnapshot {
    fn relation_names(&self, memory: &Arc<MemoryBudget>) -> Result<Arc<RelationNames>> {
        let mut cached = self.relation_names.lock().map_err(|_| invalid("catalog-state", "Relation name lookup was poisoned"))?;
        if let Some(names) = &*cached { return Ok(Arc::clone(names)); }

        let overflow = || invalid("catalog-size", "Relation lookup exceeds addressable memory");
        let bound = |bytes: usize, schema: &str, table: &str| {
            schema.len().checked_add(table.len()).and_then(|len| len.checked_mul(4))
                .and_then(|len| len.checked_add(512)).and_then(|len| bytes.checked_add(len)).ok_or_else(overflow)
        };
        let bytes = if let Some((admitted, _)) = &self.owned_relation {
            bound(4096, &admitted.table.schema, &admitted.table.table)?
        } else {
            self.databases.values().try_fold(4096usize, |bytes, database| {
                let bytes = crate::system_schema::DEFINITIONS.iter()
                    .try_fold(bytes, |bytes, definition| bound(bytes, definition.schema, definition.name))?;
                database.partitions.iter().flat_map(Snapshot::table_names)
                    .filter(|name| !TableRef::owned_schema(&name.schema))
                    .try_fold(bytes, |bytes, name| bound(bytes, &name.schema, &name.table))
            })?
        };
        let reservation = memory.reserve(bytes)?;
        let mut names = RelationNames { by_oid: BTreeMap::new(), by_name: BTreeMap::new(), _reservation: reservation };
        for (database, snapshot) in self.databases.iter() {
            let tables = if let Some((admitted, _)) = &self.owned_relation {
                if &admitted.database != database || !snapshot.partitions.iter().any(|part| part.contains_table(&admitted.table)) { continue; }
                BTreeSet::from([admitted.table.clone()])
            } else {
                snapshot.partitions.iter().flat_map(Snapshot::table_names).filter(|name| !TableRef::owned_schema(&name.schema)).cloned()
                    .chain(crate::system_schema::DEFINITIONS.iter().map(|def| TableRef::new(def.schema, def.name))).collect()
            };
            for table in tables {
                let mut oid = None;
                if crate::system_schema::definition(&table).is_none() {
                    for partition in &snapshot.partitions {
                        if partition.contains_table(&table) { include_table_oid(&mut oid, partition, &table)?; }
                    }
                }
                let oid = rtdb_types::pg_table_oid(oid, &table);
                names.by_name.entry(table.schema.clone()).or_default().insert(table.table.clone(), oid);
                names.by_oid.insert(oid, table);
            }
        }
        let names = Arc::new(names);
        *cached = Some(Arc::clone(&names));
        Ok(names)
    }
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

    fn relation_name(&self, oid: i32, allocation: &rtdb_expr::EvaluationScope) -> Result<Option<String>> {
        self.relation_names(allocation.memory_budget())?.by_oid.get(&oid)
            .map(|table| rtdb_expr::format_relation_name(table, allocation)).transpose()
    }

    fn relation_oid(&self, schema: Option<&str>, table: &str, allocation: &rtdb_expr::EvaluationScope) -> Result<Option<i32>> {
        let names = self.relation_names(allocation.memory_budget())?;
        let find = |schema| names.by_name.get(schema).and_then(|tables| tables.get(table)).copied();
        Ok(match schema { Some(schema) => find(schema), None => find("pg_catalog").or_else(|| find("public")) })
    }
}

#[derive(Clone, Debug)]
pub struct QueryOptions {
    pub batch_rows: usize,
    pub list_batch_rows: usize,
    /// Bytes a sort may hold, ordering scratch included, before it spills its block as a sorted run.
    pub sort_memory_bytes: usize,
    /// The largest limit a sort answers by keeping only its candidates; a larger one sorts blocks and spills them.
    pub top_k_rows: usize,
    /// Bytes a hash aggregation or DISTINCT may grow by before it sends rows of new keys to disk.
    pub hash_memory_bytes: usize,
    /// Fan-out of one level of hash partitioning; at least two, so that each level separates keys.
    pub spill_partitions: usize,
    /// Hash partitioning levels after which an operator holds what remains in memory: keys sharing a 32-bit hash never
    /// separate, so this bounds the levels hash collisions alone would add.
    pub max_spill_depth: u32,
    /// Build-row threshold for join Bloom/IID filters; exceeding it disables the filters.
    pub join_build_rows: i64,
    /// Disk bytes one query's spill files may hold at once; zero disables spilling.
    pub spill_limit_bytes: u64,
    pub materialized_spill_rows: usize,
    /// Bytes a materialized binding may buffer before it writes its pages to disk instead.
    pub materialized_memory_bytes: usize,
    pub bypass_temporal_pruning: bool,
    /// SQL GENERATE_SERIES excludes its endpoint when the process compatibility setting is enabled.
    pub generate_series_end_exclusive: bool,
    /// Stable object-index algorithm for sort runs and complete percentile groups.
    pub object_sort: rtdb_types::ObjectSort,
}
impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            batch_rows: 1024, list_batch_rows: 1024, sort_memory_bytes: 64 << 20, top_k_rows: 12_800, hash_memory_bytes: 64 << 20,
            spill_partitions: 16, max_spill_depth: 6, join_build_rows: 100_000, spill_limit_bytes: 1 << 30, materialized_spill_rows: 100_000,
            materialized_memory_bytes: 64 << 20, bypass_temporal_pruning: false, generate_series_end_exclusive: false,
            object_sort: rtdb_types::ObjectSort::default(),
        }
    }
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
    delivery_guard: Option<Arc<dyn QueryDeliveryGuard>>,
    pub(crate) spatial_refinement: bool,
    shared_plans: Option<Arc<crate::SharedPlans>>,
}

#[async_trait::async_trait]
pub trait QueryDeliveryGuard: Send + Sync {
    async fn check(&self, cancellation: &CancellationToken) -> Result<()>;
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
        if options.spill_partitions < 2 { return Err(invalid("invalid-options", "Spill partitions must be at least two")); }
        // Under local erasure custody no readable copy of an erased row may outlive the query, so its spill files are sealed.
        let spill = Arc::new(if catalog.databases.values().any(|database| database.storage.object_store().local_erasure_custody().is_some()) {
            SpillRegion::sealed(options.spill_limit_bytes)?
        } else {
            SpillRegion::new(options.spill_limit_bytes)?
        });
        Ok(Arc::new(Self { catalog, memory, current_time: current_time.into(), timezone: timezone.into(), options, cancellation: parent_cancellation.child_token(), owner_cancellation: owner_cancellation.child_token(), spill, execution_observer: None, tracer: None, trace_parent: None, application_name: None, operator_root: None, read_basis: None, snapshot_admission: None, snapshot_tokens: None, accelerators: Arc::from(Vec::new()), query_cpu: None, plaintext: None, delivery_guard: None, spatial_refinement: false, shared_plans: None }))
    }

    pub fn with_delivery_owner(mut self: Arc<Self>, owner: Arc<dyn Send + Sync>, deadline: std::time::Instant) -> Arc<Self> {
        let catalog = &mut Arc::make_mut(&mut self).catalog;
        catalog.delivery_owner = Some((owner, deadline));
        catalog.read_view = Arc::default();
        self
    }

    pub fn with_delivery_guard(mut self: Arc<Self>, guard: Arc<dyn QueryDeliveryGuard>) -> Arc<Self> {
        Arc::make_mut(&mut self).delivery_guard = Some(guard);
        self
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

    pub fn with_shared_plans(mut self: Arc<Self>, plans: Arc<crate::SharedPlans>) -> Arc<Self> {
        Arc::make_mut(&mut self).shared_plans = Some(plans);
        self
    }
    pub fn shared_plans(&self) -> Option<&Arc<crate::SharedPlans>> { self.shared_plans.as_ref() }

    pub fn with_query_cpu(mut self: Arc<Self>, counter: Option<Arc<crate::QueryCpu>>) -> Arc<Self> {
        if let Some(counter) = counter { Arc::make_mut(&mut self).query_cpu = Some(counter); }
        self
    }

    pub(crate) fn for_owned_relation(self: Arc<Self>, table: QualifiedTable, id: Option<rtdb_types::EntityId>) -> Arc<Self> {
        self.for_owned_selection(table, id.map_or(OwnedSelection::Relation, OwnedSelection::Point))
    }

    /// Restricts an ordinary ranked verifier to one visible table and its funded candidate IIDs.
    /// Requires a catalog without any prior relation selection.
    pub fn for_ranked_candidates(self: Arc<Self>, table: QualifiedTable, iids: crate::CandidateIids) -> Result<Arc<Self>> {
        if self.catalog.owned_relation.is_some() || !self.catalog.permits_table(&table) {
            return Err(invalid("scan-iid-scope", "Ranked candidates require an ordinary table in the held catalog"));
        }
        Ok(self.for_owned_selection(table, OwnedSelection::Candidates(iids)))
    }

    pub(crate) fn for_owned_selection(mut self: Arc<Self>, table: QualifiedTable, selection: OwnedSelection) -> Arc<Self> {
        let context = Arc::make_mut(&mut self);
        context.catalog.owned_relation = Some((table, selection));
        context.catalog.relation_names = Default::default();
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
        // Each wrapper below moves `work`; boxing it first keeps those moves pointer-sized.
        let work = Box::pin(work);
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

    pub fn with_plaintext(mut self: Arc<Self>) -> Arc<Self> {
        if self.plaintext.is_none() {
            let view = PlaintextView::new(Arc::clone(&self.memory));
            Arc::make_mut(&mut self).plaintext = Some(view);
        }
        self
    }

    pub async fn delivery_grant(self: &Arc<Self>) -> Result<PlaintextDelivery> {
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
        if let Some(guard) = &self.delivery_guard {
            self.owner_cancellation.run_until_cancelled(guard.check(&self.cancellation))
                .await.ok_or_else(cancelled)??;
        }
        self.catalog.check_view_lease()?;
        self.check_cancelled()?;
        grant.check()?;
        Ok(grant)
    }

    /// An idle transport must release buffered results and their ordered catalog hold by this cutoff.
    pub fn retention_deadline(&self) -> Option<std::time::Instant> {
        self.catalog.read_lease().and_then(|lease| lease.provider_cutoff())
    }

    pub async fn retain_wire_scope(&self, until: std::time::Instant) -> Result<()> {
        if self.catalog.read_lease().is_some_and(|lease| {
            lease.local_cutoff().map_or(true, |cutoff| cutoff < until)
        }) {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb/unretained",
                "The catalog lease cannot cover the exact read scope"));
        }
        self.catalog.held_view().await?;
        self.check_cancelled()
    }

    pub fn for_wire_scope_page(&self, cancellation: &CancellationToken) -> Arc<Self> {
        Arc::new(Self {
            cancellation: cancellation.child_token(),
            owner_cancellation: self.cancellation.child_token(),
            ..self.clone()
        })
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

struct OuterInput { batch: Arc<crate::columnar::ColumnBatch>, row: usize }

impl rtdb_expr::RowInput for OuterInput {
    fn value(&self, index: usize, scope: Option<&rtdb_expr::EvaluationScope>) -> Result<Option<Value>> {
        self.batch.columns.get(index).map(|column| column.expression_value(self.row, false, scope)).transpose()
    }

    fn arrow_cell(&self, index: usize) -> Option<rtdb_columnar::ArrowCell<'_>> {
        self.batch.columns.get(index)?.expression_cell(self.row)
    }
}

struct OuterFrame {
    schema: Arc<RuntimeSchema>,
    input: OuterInput,
    parent: Arc<OuterRows>,
}

pub(crate) struct OuterRows {
    frame: Option<OuterFrame>,
    len: usize,
    _reservation: Arc<MemoryReservation>,
}

impl OuterRows {
    fn frames(&self) -> impl Iterator<Item = &OuterFrame> {
        std::iter::successors(self.frame.as_ref(), |&frame| frame.parent.frame.as_ref())
    }

    fn expression_rows(&self) -> Vec<rtdb_expr::OuterRow<'_>> {
        let mut rows = Vec::with_capacity(self.len);
        rows.extend(self.schemas().map(|schema| rtdb_expr::OuterRow { schema, values: &[] }));
        rows
    }

    pub fn schemas(&self) -> impl Iterator<Item = &rtdb_plan::Schema> {
        self.frames().map(|frame| frame.schema.schema())
    }

    pub fn empty(memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let reservation = memory.reserve(256)?;
        Ok(Arc::new(Self { frame: None, len: 0, _reservation: reservation }))
    }

    fn prepend(self: &Arc<Self>, schema: &Arc<RuntimeSchema>, input: OuterInput, memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let len = self.len.checked_add(1).ok_or_else(|| invalid("outer-size", "Outer row bindings exceed addressable memory"))?;
        let reservation = memory.reserve(256)?;
        let frame = OuterFrame { schema: Arc::clone(schema), input, parent: Arc::clone(self) };
        Ok(Arc::new(Self { frame: Some(frame), len, _reservation: reservation }))
    }
}

struct ExpressionInputs<'a> { parameters: &'a crate::parameters::BoundParameters, outer: &'a OuterRows }

impl rtdb_expr::ParameterLookup for ExpressionInputs<'_> {
    fn width(&self) -> usize { rtdb_expr::ParameterLookup::width(self.parameters) }
    fn value(&self, position: usize) -> Result<Option<&Value>> { self.parameters.get(position) }
    fn known_value(&self, position: usize) -> Option<&Value> { rtdb_expr::ParameterLookup::known_value(self.parameters, position) }
    fn arrow_cell(&self, position: usize) -> Option<rtdb_columnar::ArrowCell<'_>> { rtdb_expr::ParameterLookup::arrow_cell(self.parameters, position) }
    fn arrow_backing(&self, position: usize) -> Option<&Arc<MemoryReservation>> {
        rtdb_expr::ParameterLookup::arrow_backing(self.parameters, position)
    }

    fn correlated_value(&self, depth: usize, position: usize, scope: Option<&rtdb_expr::EvaluationScope>) -> Result<Option<Value>> {
        self.outer.frames().nth(depth).map_or(Ok(None), |frame| rtdb_expr::RowInput::value(&frame.input, position, scope))
    }
}

pub(crate) struct ApplyArguments {
    schema: Arc<RuntimeSchema>,
    arguments: Vec<(usize, rtdb_plan::Expr)>,
    types: OnceLock<Arc<ParameterTypes>>,
}

impl ApplyArguments {
    pub(crate) fn new(schema: Arc<RuntimeSchema>, arguments: Vec<(usize, rtdb_plan::Expr)>) -> Self {
        Self { schema, arguments, types: OnceLock::new() }
    }

    pub(crate) async fn bind(&self, execution: &Execution, batch: Arc<crate::columnar::ColumnBatch>, row: usize) -> Result<Execution> {
        let outer_input = OuterInput { batch, row };
        let schema = self.schema.schema();
        let memory = &execution.context.memory;
        let scope = execution.scope()?;
        let size_error = || invalid("parameter-count", "Apply parameters exceed addressable memory");
        let parent_types = execution.parameters.types();
        let width = if self.types.get().is_none() {
            Some(self.arguments.iter().try_fold(parent_types.len(), |count, (index, _)| {
                index.checked_add(1).map(|next| count.max(next))
                    .ok_or_else(|| invalid("parameter-count", "Apply parameter index overflow"))
            })?)
        } else {
            None
        };
        let slots = self.arguments.len().checked_mul(std::mem::size_of::<(usize, OverlayValue)>()).ok_or_else(size_error)?;
        let draft = if let Some(width) = width {
            let inline = width.checked_mul(std::mem::size_of::<LogicalType>())
                .and_then(|bytes| bytes.checked_add(slots)).ok_or_else(size_error)?;
            let bound = parent_types.iter().try_fold(inline, |bytes, ty| {
                bytes.checked_add(ty.retained_bytes()?).ok_or_else(size_error)
            })?;
            let types_scope = scope.scratch();
            types_scope.reserve(bound - slots)?;
            Some((width, types_scope))
        } else {
            None
        };
        scope.reserve(slots)?;
        let mut draft = draft.map(|(width, types_scope)| {
            let mut types = Vec::with_capacity(width);
            types.extend_from_slice(parent_types);
            types.resize(width, LogicalType::Null);
            (types, types_scope)
        });
        let mut parameters = Vec::with_capacity(self.arguments.len());
        for (index, expression) in &self.arguments {
            // A later type error must wait until the first reached row has evaluated its earlier arguments.
            if let Some((types, types_scope)) = &mut draft {
                types[*index] = execution.expression_type(types_scope, expression, schema)?;
            }
            if let rtdb_plan::Expr::Column { id, .. } = expression
                && let Some(position) = schema.position(*id)
                && let Some(binding) = ArrowBinding::new(Arc::clone(&outer_input.batch), position, row, Arc::clone(memory))
            {
                parameters.push((*index, OverlayValue::Arrow(binding)));
                continue;
            }
            let invocation = execution.prepare_expression(expression, schema, &scope)?;
            let value = execution.evaluate_invocation_row(&scope, &invocation, rtdb_expr::ExpressionRow::Borrowed(&outer_input)).await?;
            parameters.push((*index, OverlayValue::Scalar(value)));
        }
        let outer = execution.outer.prepend(&self.schema, outer_input, memory)?;
        let prepared = draft.map(|(types, scope)| ParameterTypes::from_scope(types, scope)).transpose()?;
        let types = self.types.get().or(prepared.as_ref()).expect("Apply parameter layout was prepared");
        let parameters = Arc::new(execution.parameters.overlay_sources(parameters, Arc::clone(types), memory)?);
        if let Some(types) = prepared {
            self.types.get_or_init(|| types);
        }
        Ok(Execution {
            context: Arc::clone(&execution.context),
            parameters,
            outer,
            bindings: execution.bindings.clone(),
            analysis: execution.analysis.clone(),
        })
    }
}

impl Execution {
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
        scope.reserve(self.outer.len.checked_mul(std::mem::size_of::<rtdb_expr::OuterRow<'_>>()).ok_or_else(|| Anomaly::incorrect("rtdb.query/expression-size", "Outer row bindings exceed addressable memory"))?)?;
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
        scope.reserve(self.outer.len.checked_mul(std::mem::size_of::<rtdb_expr::OuterRow<'_>>()).ok_or_else(|| Anomaly::incorrect("rtdb.query/expression-size", "Outer row bindings exceed addressable memory"))?)?;
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
        scope.reserve(self.outer.len.checked_mul(std::mem::size_of::<rtdb_expr::OuterRow<'_>>()).ok_or_else(|| Anomaly::incorrect("rtdb.query/expression-size", "Outer row bindings exceed addressable memory"))?)?;
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
            scope.reserve(execution.outer.len.checked_mul(std::mem::size_of::<rtdb_expr::OuterRow<'_>>()).ok_or_else(|| crate::invalid("outer-size", "Outer row bindings exceed addressable memory"))?)?;
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
        scope.reserve(self.outer.len.checked_mul(std::mem::size_of::<rtdb_expr::OuterRow<'_>>()).ok_or_else(|| Anomaly::incorrect("rtdb.query/expression-size", "Outer row bindings exceed addressable memory"))?)?;
        let outer_rows = self.outer.expression_rows();
        let inputs = ExpressionInputs { parameters: &self.parameters, outer: &self.outer };
        let context = rtdb_expr::EvalContext { parameters: rtdb_expr::Parameters::Lookup(&inputs), parameter_types: Some(self.parameters.types()), current_time: self.context.current_time, timezone: &self.context.timezone, outer_rows: &outer_rows, snapshot_token: self.context.snapshot_token(), allocation: Some(scope), catalog: Some(&self.context.catalog) };
        self.context.owner_cancellation.run_until_cancelled(self.context.cancellation.run_until_cancelled(
            invocation.evaluate_list_row_async(row,&context))).await.flatten().ok_or_else(cancelled)?
    }
}

#[cfg(test)]
mod apply_argument_tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use rtdb_expr::ParameterLookup;
    use rtdb_plan::{BinaryOp, ColumnId, Expr, Field, Schema};
    use rtdb_types::ScalarType;

    fn execution() -> Execution {
        let database = DatabaseSnapshot {
            partitions: vec![],
            storage: Arc::new(BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
        };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let memory = MemoryBudget::new(1 << 24);
        let context = QueryContext::new(catalog, Arc::clone(&memory), Timestamp(0), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap();
        let parameters = crate::parameters::BoundParameters::infer(vec![Value::I64(7), Value::I64(8)], &memory).unwrap();
        Execution {
            context,
            parameters: Arc::new(parameters),
            outer: OuterRows::empty(&memory).unwrap(),
            bindings: None,
            analysis: None,
        }
    }

    fn input(id: u32, values: Vec<i64>, memory: &Arc<MemoryBudget>) -> (Arc<RuntimeSchema>, Arc<crate::columnar::ColumnBatch>) {
        let name = format!("c{id}");
        let field = Field {
            id: ColumnId(id), name: name.clone(), qualifier: None, qualified_only: false, unnamed: false,
            data_type: LogicalType::Scalar(ScalarType::I64),
        };
        let schema = Schema::new(vec![field]).unwrap();
        let record = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![ArrowField::new(name, DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(values))],
        ).unwrap();
        let batch = rtdb_columnar::OwnedBatch::new(record, memory).unwrap();
        (
            RuntimeSchema::fixed(&schema, memory).unwrap(),
            Arc::new(crate::columnar::ColumnBatch::from_owned(batch).unwrap()),
        )
    }

    #[tokio::test]
    async fn apply_rows_share_types_across_pages_and_keep_sparse_overlays_lazy() {
        let execution = execution();
        let memory = &execution.context.memory;
        let baseline = memory.used_bytes();
        let (schema, first_page) = input(0, vec![11], memory);
        let (_, second_page) = input(0, vec![20, 22], memory);
        let first_page_owner = Arc::downgrade(&first_page);
        let column = schema.schema().fields()[0].expression();
        let i64_type = LogicalType::Scalar(ScalarType::I64);
        let arguments = ApplyArguments::new(schema, vec![
            (0, Expr::Literal(Value::I64(9))),
            (3, Expr::Literal(Value::Bool(false))),
            (3, column),
            (4, Expr::Parameter { index: 0, data_type: i64_type.clone() }),
        ]);

        let first = arguments.bind(&execution, first_page, 0).await.unwrap();
        let second = arguments.bind(&execution, second_page, 1).await.unwrap();
        assert!(std::ptr::eq(first.parameters.types(), second.parameters.types()));
        assert_eq!(second.parameters.types(), [i64_type.clone(), i64_type.clone(), LogicalType::Null, i64_type.clone(), i64_type]);
        assert_eq!(second.parameters.get(0).unwrap(), Some(&Value::I64(9)));
        assert_eq!(second.parameters.get(1).unwrap(), Some(&Value::I64(8)));
        assert_eq!(second.parameters.known_value(2), Some(&Value::Null));
        assert_eq!(second.parameters.get(4).unwrap(), Some(&Value::I64(7)));
        assert_eq!(second.parameters.get(5).unwrap(), None);
        assert_eq!(first.parameters.arrow_cell(3).unwrap().row, 0);
        assert_eq!(second.parameters.arrow_cell(3).unwrap().row, 1);
        assert!(first.parameters.known_value(3).is_none());
        assert!(second.parameters.known_value(3).is_none());
        assert_eq!(first.parameters.get(3).unwrap(), Some(&Value::I64(11)));
        drop(first);
        assert!(first_page_owner.upgrade().is_none());
        assert_eq!(second.parameters.get(3).unwrap(), Some(&Value::I64(22)));
        drop((arguments, second));
        assert_eq!(memory.used_bytes(), baseline);
    }

    #[tokio::test]
    async fn apply_reports_earlier_row_error_before_later_argument_type_error() {
        let execution = execution();
        let (schema, page) = input(0, vec![0, 2], &execution.context.memory);
        let column = schema.schema().fields()[0].expression();
        let missing = Expr::column(ColumnId(99), LogicalType::Scalar(ScalarType::I64));
        let divide = Expr::binary(BinaryOp::Divide, Expr::Literal(Value::I64(8)), column);
        let arguments = ApplyArguments::new(schema, vec![(0, divide), (1, missing)]);

        let error = arguments.bind(&execution, Arc::clone(&page), 0).await.err().unwrap();
        assert_eq!(error.code, "xtdb.expression/division-by-zero");
        assert!(arguments.types.get().is_none());
        let error = arguments.bind(&execution, page, 1).await.err().unwrap();
        assert_eq!(error.code, "rtdb.expr/missing-column");
        assert!(arguments.types.get().is_none());
    }

    #[tokio::test]
    async fn nested_apply_retains_outer_schema_and_source_in_depth_order() {
        let execution = execution();
        let memory = &execution.context.memory;
        let (parent_schema, parent_page) = input(10, vec![1, 2], memory);
        let (child_schema, child_page) = input(20, vec![3], memory);
        let parent_page_owner = Arc::downgrade(&parent_page);
        let parent_schema_owner = Arc::downgrade(&parent_schema);
        let parent_arguments = ApplyArguments::new(parent_schema, vec![]);
        let child_arguments = ApplyArguments::new(child_schema, vec![]);
        let parent = parent_arguments.bind(&execution, parent_page, 1).await.unwrap();
        let child = child_arguments.bind(&parent, child_page, 0).await.unwrap();

        drop((parent_arguments, parent));
        let schema = parent_schema_owner.upgrade().unwrap();
        assert!(std::ptr::eq(child.outer.schemas().nth(1).unwrap(), schema.schema()));
        assert_eq!(child.outer.schemas().next().unwrap().fields()[0].id, ColumnId(20));
        assert!(parent_page_owner.upgrade().is_some());
        let scope = child.scope().unwrap();
        let inputs = ExpressionInputs { parameters: &child.parameters, outer: &child.outer };
        assert_eq!(inputs.correlated_value(0, 0, Some(&scope)).unwrap(), Some(Value::I64(3)));
        assert_eq!(inputs.correlated_value(1, 0, Some(&scope)).unwrap(), Some(Value::I64(2)));
        assert_eq!(inputs.correlated_value(2, 0, Some(&scope)).unwrap(), None);
        drop((child, schema));
        assert!(parent_page_owner.upgrade().is_none());
        assert!(parent_schema_owner.upgrade().is_none());
    }
}

#[test]
fn catalog_relation_identities_are_built_once_and_charged_to_the_catalog() {
    use rtdb_expr::PgCatalogLookup;

    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), DatabaseSnapshot {
        partitions: vec![],
        storage: Arc::new(BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
    })]), false).unwrap();
    let memory = MemoryBudget::new(1 << 20);
    let scope = rtdb_expr::EvaluationScope::new(&memory).unwrap();
    let oid = catalog.relation_oid(None, "pg_class", &scope).unwrap().unwrap();
    let used = memory.used_bytes();
    for _ in 0..100 {
        assert_eq!(catalog.relation_oid(Some("pg_catalog"), "pg_class", &scope).unwrap(), Some(oid));
    }
    assert_eq!(memory.used_bytes(), used);
    assert_eq!(catalog.relation_name(oid, &scope).unwrap().as_deref(), Some("pg_class"));
}

#[test]
fn ranked_candidates_keep_the_held_guard_and_cannot_replace_a_narrower_scope() -> Result<()> {
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), DatabaseSnapshot {
        partitions: vec![],
        storage: Arc::new(BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
    })]), false)?;
    let memory = MemoryBudget::new(1 << 20);
    let context = QueryContext::new(catalog, Arc::clone(&memory), Timestamp(100), "UTC",
        QueryOptions::default(), &CancellationToken::new())?;
    let context = context.with_owned_read_guard(Arc::new(|| Err(invalid("test-read-guard", "Held read closed"))));
    let table = QualifiedTable { database: "xtdb".into(), table: TableRef::new("public", "ranked") };
    let other = QualifiedTable { database: "xtdb".into(), table: TableRef::new("public", "other") };
    let owned = QualifiedTable { database: "xtdb".into(), table: rtdb_types::OwnedRelation::new("ranked")?.table("alice") };
    let iid = rtdb_types::Iid([1; 16]);
    let candidates = crate::CandidateIids::from_sorted(&[iid], &memory)?;

    let ranked = Arc::clone(&context).for_ranked_candidates(table.clone(), candidates.clone())?;
    assert!(ranked.catalog.owned_candidates(&table).unwrap().contains(iid));
    assert!(!ranked.catalog.permits_table(&other));
    assert!(ranked.check_cancelled().is_err());
    assert!(Arc::clone(&context).for_ranked_candidates(owned, candidates.clone()).is_err());
    let point = context.for_owned_relation(table.clone(), Some(rtdb_types::EntityId::Uuid([2; 16])));
    assert!(point.for_ranked_candidates(table, candidates).is_err());
    Ok(())
}
