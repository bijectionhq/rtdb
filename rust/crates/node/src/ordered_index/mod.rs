mod accelerate;
mod checkpoint;
mod correction;
mod domain;
mod ipc;
mod key;
mod maintenance;
mod postings;
mod projection;
mod references;
mod replay_coverage;
mod seek;
mod tree;
mod usage;

use std::{collections::BTreeMap, future::Future, mem::size_of, ops::Deref, sync::Arc};

use arrow_array::RecordBatch;
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation, read_owned_entity_id};
use rtdb_index::Snapshot;
use rtdb_plan::{Catalog, ColumnId, Field, QualifiedTable, Relation, Scan, TemporalFilter};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::{ObjectKey, cache::BufferPool};
use rtdb_types::{Anomaly, Instant, LogicalType, Result, ScalarType, TableRef, Value};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

pub(crate) use accelerate::{OrderedIndexAccelerator, RowBounds};
pub(crate) use maintenance::OrderedIndexMaintenance;
pub(crate) use usage::{Inventory, owned_file_bytes};

use rtdb_format::keys::ORDERED_INDEX_ROOT as INDEX_ROOT;
const LEAF_LAYOUT_VERSION: u8 = 11;
const PAGE_LAYOUT_VERSION: u8 = 13;
const ARROW_ONLY_DIRECTORY_VERSION: u8 = 14;
const CATALOG: (&str, &str) = ("xt", "ordered_indexes");

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Definition {
    name: String,
    table: TableRef,
    columns: Vec<String>,
    version: i64,
}

impl Definition {
    fn directory(&self, storage: &BufferPool) -> Result<ObjectKey> {
        let layout = if storage.object_store().ordered_index_arrow_only() {
            ARROW_ONLY_DIRECTORY_VERSION
        } else {
            PAGE_LAYOUT_VERSION
        };
        let mut digest = Sha256::new();
        serde_json::to_writer(
            &mut digest,
            &(
                &self.table.schema,
                &self.table.table,
                &self.name,
                self.version,
                layout,
            ),
        )
        .map_err(format_error)?;
        let digest = digest.finalize();
        Ok(ObjectKey::new(format!(
            "{INDEX_ROOT}/{:016x}-{digest:x}",
            self.version
        ))?)
    }

    fn payload_bytes_for(&self, table: &TableRef) -> Result<usize> {
        definition_payload_bytes(
            &self.name, &table.schema, &table.table,
            self.columns.iter().map(|column| Ok(column.as_str())),
        )
    }
}

struct DefinitionSet {
    rows: Vec<Definition>,
    reservation: Arc<MemoryReservation>,
}

impl DefinitionSet {
    fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        Ok(Self { rows: Vec::new(), reservation: memory.reserve(256)? })
    }

    fn iter(&self) -> std::slice::Iter<'_, Definition> {
        self.rows.iter()
    }

    fn push_row(&mut self, batch: &RecordBatch, row: usize) -> Result<()> {
        let cell = |column: usize| Some(ArrowCell {
            field: batch.schema_ref().fields().get(column)?.as_ref(),
            array: batch.columns().get(column)?.as_ref(),
            row,
        });
        let text = |column| cell(column).ok_or_else(definition_text_error).and_then(definition_text);
        let columns = || cell(3).and_then(ArrowCell::list)
            .ok_or_else(|| format_error("Expected index columns"));
        let version = cell(4).and_then(ArrowCell::int64)
            .ok_or_else(|| format_error("Expected an index version"))?;
        let (name, schema, table) = (text(0)?, text(1)?, text(2)?);
        let bytes = definition_payload_bytes(name, schema, table, columns()?.map(definition_text))?;
        self.reserve_row(bytes)?;
        let columns = columns()?;
        let mut names = Vec::with_capacity(columns.len());
        for cell in columns {
            names.push(definition_text(cell)?.to_owned());
        }
        self.rows.push(Definition {
            name: name.to_owned(),
            table: TableRef::new(schema, table),
            columns: names,
            version,
        });
        Ok(())
    }

    fn push_overlay(&mut self, source: usize, table: &TableRef) -> Result<()> {
        self.reserve_row(self.rows[source].payload_bytes_for(table)?)?;
        let definition = &self.rows[source];
        self.rows.push(Definition {
            name: definition.name.clone(),
            table: table.clone(),
            columns: definition.columns.clone(),
            version: definition.version,
        });
        Ok(())
    }

    fn reserve_row(&mut self, bytes: usize) -> Result<()> {
        let capacity = self.rows.capacity();
        let lease = Arc::get_mut(&mut self.reservation).expect("The catalog builder owns its reservation");
        let retained = lease.bytes().checked_add(bytes).ok_or_else(definition_allocation_overflow)?;
        if self.rows.len() == capacity {
            let next = capacity.checked_mul(2).ok_or_else(definition_allocation_overflow)?.max(1);
            let allocation = next.checked_mul(size_of::<Definition>()).ok_or_else(definition_allocation_overflow)?;
            lease.grow_to(retained.checked_add(allocation).ok_or_else(definition_allocation_overflow)?)?;
            self.rows.try_reserve_exact(next - capacity).map_err(format_error)?;
            lease.shrink_to(lease.bytes() - capacity * size_of::<Definition>())?;
        } else {
            lease.grow_to(retained)?;
        }
        Ok(())
    }
}

fn definition_payload_bytes<'a>(
    name: &str, schema: &str, table: &str,
    mut columns: impl ExactSizeIterator<Item = Result<&'a str>>,
) -> Result<usize> {
    let bytes = columns.len().checked_mul(size_of::<String>())
        .and_then(|bytes| bytes.checked_add(name.len()))
        .and_then(|bytes| bytes.checked_add(schema.len()))
        .and_then(|bytes| bytes.checked_add(table.len()))
        .ok_or_else(definition_allocation_overflow)?;
    columns.try_fold(bytes, |bytes, column| {
        bytes.checked_add(column?.len()).ok_or_else(definition_allocation_overflow)
    })
}

fn definition_text(cell: ArrowCell<'_>) -> Result<&str> {
    cell.utf8().ok_or_else(definition_text_error)
}

fn definition_text_error() -> Anomaly {
    format_error("Expected an index name or column")
}

fn definition_allocation_overflow() -> Anomaly {
    Anomaly::incorrect("rtdb/allocation-overflow", "Ordered index definitions exceed addressable memory")
}

type DefinitionEntry = Arc<tokio::sync::OnceCell<Arc<DefinitionSet>>>;

#[derive(Default)]
struct DefinitionCache {
    tables: parking_lot::Mutex<BTreeMap<(String, Instant, usize), DefinitionEntry>>,
}

impl DefinitionCache {
    async fn get_or_load(
        &self, database: &str, now: Instant, budget: &Arc<MemoryBudget>,
        cancellation: &CancellationToken, load: impl Future<Output = Result<DefinitionSet>>,
    ) -> Result<Option<Arc<DefinitionSet>>> {
        let entry = {
            let mut tables = self.tables.lock();
            let key = (database.to_owned(), now, Arc::as_ptr(budget) as usize);
            if !tables.contains_key(&key) && tables.len() >= 4 {
                tables.pop_first();
            }
            Arc::clone(tables.entry(key).or_default())
        };
        let loaded = entry.get_or_try_init(|| async { load.await.map(Arc::new) });
        match cancellation.run_until_cancelled(loaded).await.ok_or_else(maintenance::cancelled)? {
            Ok(definitions) => Ok(Some(Arc::clone(definitions))),
            Err(error) if error.code == "rtdb/memory-budget-exceeded" => Ok(None),
            Err(error) => Err(error),
        }
    }
}

async fn definitions(
    database: &str, snapshot: &Snapshot, storage: &Arc<BufferPool>, memory: &Arc<MemoryBudget>,
    now: Instant, cancellation: &CancellationToken,
) -> Result<Option<Arc<DefinitionSet>>> {
    if !snapshot.contains_table(&TableRef::new(CATALOG.0, CATALOG.1)) {
        return Ok(None);
    }
    if cancellation.is_cancelled() { return Err(maintenance::cancelled()); }
    if let Some(budget) = memory.snapshot_budget() {
        let cache = snapshot.memo::<DefinitionCache>();
        let load = load_definitions(database, snapshot, storage, memory, budget, now, cancellation);
        if let Some(definitions) = cache.get_or_load(database, now, budget, cancellation, load).await? {
            return Ok(Some(definitions));
        }
    }
    let load = load_definitions(database, snapshot, storage, memory, memory, now, cancellation);
    let definitions = cancellation.run_until_cancelled(load).await.ok_or_else(maintenance::cancelled)??;
    Ok(Some(Arc::new(definitions)))
}

async fn load_definitions(
    database: &str,
    snapshot: &Snapshot,
    storage: &Arc<BufferPool>,
    memory: &Arc<MemoryBudget>,
    definition_budget: &Arc<MemoryBudget>,
    now: Instant,
    cancellation: &CancellationToken,
) -> Result<DefinitionSet> {
    let mut definitions = DefinitionSet::new(definition_budget)?;
    let name = QualifiedTable {
        database: database.to_owned(),
        table: TableRef::new(CATALOG.0, CATALOG.1),
    };
    let catalog = CatalogSnapshot::new(
        database,
        BTreeMap::from([(
            database.to_string(),
            DatabaseSnapshot {
                partitions: vec![snapshot.clone()],
                storage: Arc::clone(storage),
            },
        )]),
        false,
    )?;
    let context = QueryContext::new(
        catalog,
        Arc::clone(memory),
        now,
        "UTC",
        QueryOptions::default(),
        cancellation,
    )?;
    let table = context.catalog.table(&name)?
        .ok_or_else(|| format_error("The ordered index catalog is absent at the captured basis"))?;
    let columns = ["_id", "schema_name", "table_name", "columns", "version"]
        .into_iter().enumerate().map(|(id, name)| Field {
            id: ColumnId(id as u32),
            name: name.to_owned(),
            qualifier: None,
            qualified_only: false,
            unnamed: false,
            data_type: table.scan_column_type(name),
        }).collect();
    let scan = Relation::scan(Scan {
        table, columns, valid_time: TemporalFilter::Default, system_time: TemporalFilter::Default,
        clamp_valid_time: false, iids: None, predicates: Vec::new(),
        top_k: None, ordered_top_k: None, matches: Vec::new(),
    })?;
    let cursor = PreparedQuery::new_in(Arc::new(scan), memory)?
        .execute_columns(context, Vec::new()).await?;
    read_definitions(cursor, &mut definitions).await?;
    Ok(definitions)
}

async fn owned_definitions(
    database: &str, snapshot: &Snapshot, storage: &Arc<BufferPool>, memory: &Arc<MemoryBudget>,
    now: Instant, cancellation: &CancellationToken,
) -> Result<Option<DefinitionSet>> {
    if !snapshot.table_names().any(TableRef::is_owned_path_catalog) {
        return Ok(None);
    }
    let tables = snapshot.tables();
    let catalog = CatalogSnapshot::new(database, BTreeMap::from([(
        database.to_owned(), DatabaseSnapshot { partitions: vec![snapshot.clone()], storage: Arc::clone(storage) },
    )]), false)?;
    let context = QueryContext::new(catalog, Arc::clone(memory), now, "UTC", QueryOptions::default(), cancellation)?;
    let mut world_tables = BTreeMap::<_, Vec<_>>::new();
    for table in &tables {
        if let Some(root) = table.owned_world_root_table() {
            world_tables.entry(root).or_default().push(table);
        }
    }
    let mut definitions = DefinitionSet::new(memory)?;
    for table in tables.iter().filter(|table| table.is_owned_path_catalog()) {
        if let Some(cursor) = rtdb_query::open_owned_path_catalog(Arc::clone(&context), table, None).await? {
            let start = definitions.rows.len();
            read_definitions(cursor, &mut definitions).await?;
            for source in start..definitions.rows.len() {
                let target = &definitions.rows[source].table;
                if !table.owns_path_target(target) {
                    return Err(format_error("Owned path definition refers to another owner or an internal catalog"));
                }
                if let Some(overlays) = world_tables.get(target) {
                    for overlay in overlays {
                        definitions.push_overlay(source, overlay)?;
                    }
                }
            }
        }
    }
    definitions.rows.sort_unstable();
    Ok(Some(definitions))
}

async fn read_definitions(mut cursor: rtdb_query::QueryCursor, definitions: &mut DefinitionSet) -> Result<()> {
    let start = definitions.rows.len();
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        for row in 0..batch.num_rows() {
            definitions.push_row(batch, row)?;
        }
    }
    definitions.rows[start..].sort_unstable();
    Ok(())
}

pub struct OwnedRangePage {
    pub schema: Arc<arrow_schema::Schema>,
    pub batch: Option<rtdb_columnar::OwnedBatch>,
}

/// A non-null full-key selector at a held Arrow or native document boundary.
#[derive(Clone, Copy)]
pub enum OwnedKeyBound<'a> {
    Arrow {
        field: &'a arrow_schema::FieldRef,
        array: &'a arrow_array::ArrayRef,
        row: usize,
    },
    Values(&'a rtdb_types::StructFields<Value>),
}

/// Selector ownership across the World archive mailbox. Arrow columns keep their input allocation permits.
pub enum RetainedKeyBound {
    Arrow {
        field: arrow_schema::FieldRef,
        array: arrow_array::ArrayRef,
        row: usize,
    },
    Values(rtdb_types::StructFields<Value>),
}

impl RetainedKeyBound {
    pub fn borrowed(&self) -> OwnedKeyBound<'_> {
        match self {
            Self::Arrow { field, array, row } => OwnedKeyBound::Arrow { field, array, row: *row },
            Self::Values(fields) => OwnedKeyBound::Values(fields),
        }
    }
}

impl<'a> OwnedKeyBound<'a> {
    fn arrow_cell(self) -> Option<ArrowCell<'a>> {
        match self {
            Self::Arrow { field, array, row } => Some(ArrowCell { field, array: array.as_ref(), row }),
            Self::Values(_) => None,
        }
    }

    fn arrow_field(self, name: &str) -> Option<ArrowCell<'a>> {
        self.arrow_cell()?.structure()?.find(|cell| cell.field.name() == name)
    }

    pub fn retained(self) -> RetainedKeyBound {
        match self {
            Self::Arrow { field, array, row } => RetainedKeyBound::Arrow {
                field: Arc::clone(field), array: Arc::clone(array), row,
            },
            Self::Values(fields) => RetainedKeyBound::Values(fields.clone()),
        }
    }

    fn identity_type(self) -> Option<Result<LogicalType>> {
        match self {
            Self::Values(fields) => fields.get("_id").map(|id| id.owned_entity_id().map(|_| id.logical_type())),
            Self::Arrow { .. } => self.arrow_field("_id").map(|cell| {
                read_owned_entity_id(cell.field, cell.array, cell.row)?;
                cell.logical_type()
            }),
        }
    }

    pub(crate) fn fixed_key(self, column: &str) -> Result<rtdb_types::FixedRangeKey> {
        use rtdb_types::FixedRangeKey;
        match self {
            Self::Values(fields) => FixedRangeKey::from_bound(fields, column),
            Self::Arrow { .. } => {
                let invalid = accelerate::invalid_bound;
                if self.arrow_cell().and_then(|cell| cell.structure()).ok_or_else(invalid)?.len() != 2 {
                    return Err(invalid());
                }
                let key = self.arrow_field(column).and_then(ArrowCell::binary).ok_or_else(invalid)?;
                let id = self.arrow_field("_id").ok_or_else(invalid)?;
                if let Some(id) = id.binary() {
                    FixedRangeKey::from_binary_parts(key, id)
                } else {
                    FixedRangeKey::from_parts(key, id.utf8().ok_or_else(invalid)?)
                }
            }
        }
    }
}

/// Full-key bounds for a covered page. Selectors name every path column.
pub struct OwnedRangeRequest<'a> {
    pub lower: Option<OwnedKeyBound<'a>>,
    pub upper: Option<OwnedKeyBound<'a>>,
    pub after: Option<OwnedKeyBound<'a>>,
    pub descending: bool,
    pub limit: usize,
}

pub struct OwnedCountRequest<'a> {
    pub lower: Option<OwnedKeyBound<'a>>,
    pub upper: Option<OwnedKeyBound<'a>>,
    pub valid_at: rtdb_types::Timestamp,
}

pub struct OwnedDecimalSum {
    pub coefficient: [u8; 32],
    pub scale: i8,
}

pub struct OwnedRangeCount {
    pub count: u64,
    pub sum: Option<i128>,
    pub decimal_sum: Option<OwnedDecimalSum>,
    pub present: rtdb_types::Timestamp,
    pub valid_at: rtdb_types::Timestamp,
    pub expiry: Option<rtdb_types::Timestamp>,
}

pub async fn range_owned_cut(
    context: Arc<QueryContext>, owner: &str, relation: &rtdb_types::OwnedRelation,
    path: &rtdb_types::OwnedRelation, request: OwnedRangeRequest<'_>, valid: rtdb_types::Timestamp,
) -> Result<OwnedRangePage> {
    owned_range(context, owner, relation, path, RowBounds::Compound(request), Some(valid)).await
}

struct OwnedProjection {
    fields: Vec<Field>,
    empty: bool,
}

pub(crate) fn uncovered_owned_range() -> Anomaly {
    Anomaly::new(
        rtdb_types::AnomalyCategory::Unavailable,
        "rtdb.owned/path-uncovered",
        "The held basis has no fully covered generation of this owned path",
    )
}

pub(crate) async fn owned_range(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &rtdb_types::OwnedRelation,
    path: &rtdb_types::OwnedRelation,
    bounds: RowBounds<'_>,
    valid_at: Option<rtdb_types::Timestamp>,
) -> Result<OwnedRangePage> {
    let definition = owned_definition(&context, owner, relation, path).await?;
    let database = context
        .catalog
        .database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else {
        return Err(uncovered_owned_range());
    };
    let projection = owned_projection(
        &context,
        snapshot,
        &definition,
        owner,
        relation,
        bounds.identity_bounds(),
    )
    .await?;
    let work = accelerate::owned_page(
        &context, snapshot, &database.storage, &definition, projection, bounds, valid_at,
    );
    context.reading(work).await
}

pub async fn owned_count(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &rtdb_types::OwnedRelation,
    path: &rtdb_types::OwnedRelation,
    request: OwnedCountRequest<'_>,
) -> Result<OwnedRangeCount> {
    owned_count_at(context, owner, relation, path, request, None).await
}

/// Read the certified ordered measure at an already admitted native cut.
pub async fn measure_owned_cut(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &rtdb_types::OwnedRelation,
    path: &rtdb_types::OwnedRelation,
    request: OwnedCountRequest<'_>,
) -> Result<OwnedRangeCount> {
    owned_total_at(context, owner, relation, path, request, None, true).await
}

pub(crate) async fn owned_count_at(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &rtdb_types::OwnedRelation,
    path: &rtdb_types::OwnedRelation,
    request: OwnedCountRequest<'_>,
    lifetime: Option<i64>,
) -> Result<OwnedRangeCount> {
    owned_total_at(context, owner, relation, path, request, lifetime, false).await
}

pub(crate) async fn owned_total_at(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &rtdb_types::OwnedRelation,
    path: &rtdb_types::OwnedRelation,
    request: OwnedCountRequest<'_>,
    lifetime: Option<i64>,
    measured: bool,
) -> Result<OwnedRangeCount> {
    let definition = owned_definition(&context, owner, relation, path).await?;
    let domain = if measured {
        let [.., measure, identity] = definition.columns.as_slice() else {
            return Err(uncovered_owned_range());
        };
        let (schema, _charge) =
            rtdb_query::read_owned_relation_schema(Arc::clone(&context), owner, relation)
                .await?.ok_or_else(uncovered_owned_range)?;
        if identity != "_id" { return Err(uncovered_owned_range()); }
        Some(match schema.path_column_logical_type(measure) {
            Some(LogicalType::Scalar(ScalarType::I64)) => tree::MeasureDomain::Int64,
            Some(LogicalType::Scalar(ScalarType::Decimal { precision, scale, bit_width: 128 })) =>
                tree::MeasureDomain::Decimal128(
                    precision.try_into().map_err(|_| uncovered_owned_range())?,
                    scale.try_into().map_err(|_| uncovered_owned_range())?,
                ),
            _ => return Err(uncovered_owned_range()),
        })
    } else {
        None
    };
    let database = context
        .catalog
        .database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else {
        return Err(uncovered_owned_range());
    };
    let projection = owned_projection(&context, snapshot, &definition, owner, relation,
        [request.lower, request.upper, None]).await?;
    let work = accelerate::owned_count(
        &context, snapshot, &database.storage, &definition, projection, request, lifetime, domain,
    );
    context.reading(work).await
}

async fn owned_projection(
    context: &Arc<QueryContext>,
    snapshot: &Snapshot,
    definition: &Definition,
    owner: &str,
    relation: &rtdb_types::OwnedRelation,
    bounds: [Option<OwnedKeyBound<'_>>; 3],
) -> Result<OwnedProjection> {
    let empty = snapshot.owned_routing_inventory(owner, relation)?.data_exhausted();
    let mut types = snapshot.column_types_for(&definition.table, definition.columns.clone());
    if empty || relation.world().is_some() {
        let root = relation.root_relation();
        let (declaration, _charge) =
            rtdb_query::read_owned_relation_schema(Arc::clone(context), owner, root.as_ref().unwrap_or(relation))
                .await?
                .ok_or_else(uncovered_owned_range)?;
        let identity = match bounds.into_iter().flatten().find_map(OwnedKeyBound::identity_type) {
            Some(identity) => identity?,
            None => types.get("_id").cloned()
                .unwrap_or(LogicalType::Scalar(ScalarType::Utf8)),
        };
        for name in &definition.columns {
            let ty = if name == "_id" {
                // Identity has no declaration until the first fact. Its bound
                // supplies a concrete type for an empty compound page.
                identity.clone()
            } else {
                declaration.path_column_logical_type(name).ok_or_else(uncovered_owned_range)?
            };
            types.insert(name.clone(), ty);
        }
    }
    let fields = definition
        .columns
        .iter()
        .enumerate()
        .map(|(id, name)| Field {
            id: ColumnId(id as u32),
            name: name.clone(),
            qualifier: None,
            qualified_only: false,
            unnamed: false,
            data_type: types.get(name.as_str()).cloned().unwrap_or(LogicalType::Null),
        })
        .collect();
    Ok(OwnedProjection { fields, empty })
}

/// Resolve the ordered key declaration within an already admitted archived cut.
pub async fn path_columns_at_cut(
    context: &Arc<QueryContext>, owner: &str,
    relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
) -> Result<OwnedPathColumns> {
    Ok(owned_definition(context, owner, relation, path).await?.into_columns())
}

pub async fn stable_owned_path_columns(
    before: &Arc<QueryContext>,
    after: &Arc<QueryContext>,
    owner: &str,
    relation: &rtdb_types::OwnedRelation,
    path: &rtdb_types::OwnedRelation,
) -> Result<OwnedPathColumns> {
    let definition = owned_definition(before, owner, relation, path).await?;
    if definition.definition != owned_definition(after, owner, relation, path).await?.definition {
        return Err(uncovered_owned_range());
    }
    Ok(definition.into_columns())
}

#[derive(Debug)]
pub struct OwnedPathColumns {
    columns: Vec<String>,
    _reservation: Arc<MemoryReservation>,
}

impl Deref for OwnedPathColumns {
    type Target = [String];

    fn deref(&self) -> &Self::Target {
        &self.columns
    }
}

struct HeldDefinition {
    definition: Definition,
    reservation: Arc<MemoryReservation>,
}

impl HeldDefinition {
    fn into_columns(self) -> OwnedPathColumns {
        let Self { definition: Definition { columns, .. }, reservation } = self;
        OwnedPathColumns { columns, _reservation: reservation }
    }
}

impl Deref for HeldDefinition {
    type Target = Definition;

    fn deref(&self) -> &Self::Target {
        &self.definition
    }
}

async fn owned_definition(
    context: &Arc<QueryContext>,
    owner: &str,
    relation: &rtdb_types::OwnedRelation,
    path: &rtdb_types::OwnedRelation,
) -> Result<HeldDefinition> {
    let catalog = rtdb_types::OwnedRelation::path_catalog(owner);
    let root = relation.root_relation();
    let target = root.as_ref().unwrap_or(relation).table(owner);
    let cursor =
        rtdb_query::open_owned_path_catalog(Arc::clone(context), &catalog, Some(path.name()))
            .await?
            .ok_or_else(uncovered_owned_range)?;
    let mut definitions = DefinitionSet::new(&context.memory)?;
    read_definitions(cursor, &mut definitions).await?;
    let [definition] = definitions.rows.as_mut_slice() else { return Err(uncovered_owned_range()); };
    if definition.name != path.name() || definition.table != target {
        return Err(uncovered_owned_range());
    }
    if let Some(root) = root {
        let table_bytes = (target.table.len() - root.name().len())
            .checked_add(relation.name().len())
            .and_then(|bytes| bytes.checked_add(target.schema.len()))
            .ok_or_else(definition_allocation_overflow)?;
        let lease = Arc::get_mut(&mut definitions.reservation).expect("The held definition owns its reservation");
        let retained = lease.bytes().checked_add(table_bytes).ok_or_else(definition_allocation_overflow)?;
        lease.grow_to(retained)?;
        definition.table = relation.table(owner);
    }
    let definition = definitions.rows.pop().ok_or_else(uncovered_owned_range)?;
    let DefinitionSet { rows, reservation } = definitions;
    drop(rows);
    Ok(HeldDefinition { definition, reservation })
}

fn format_error(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::fault("rtdb.ordered-index/format", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::Value;

    fn definition_batch() -> Result<RecordBatch> {
        let cells = [
            ("_id", [Value::Utf8("by_key".into()), Value::Utf8("bad_columns".into()), Value::I64(0)]),
            ("schema_name", std::array::from_fn(|_| Value::Utf8("public".into()))),
            ("table_name", std::array::from_fn(|_| Value::Utf8("events".into()))),
            ("columns", [
                Value::list(vec![Value::Utf8("key".into()), Value::Utf8("_id".into())]),
                Value::list(vec![Value::Utf8("other".into()), Value::Null]),
                Value::list(vec![Value::Utf8("_id".into())]),
            ]),
            ("version", [Value::I64(12), Value::I64(13), Value::I32(14)]),
        ];
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        for (name, values) in cells {
            let ty = LogicalType::join_all(&values.each_ref().map(Value::logical_type));
            fields.push(rtdb_columnar::to_field(name, &ty)?);
            columns.push(rtdb_columnar::build_array(&ty, &values)?);
        }
        RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns).map_err(format_error)
    }

    #[test]
    fn ordered_definition_admits_text_and_i64_from_selected_union_legs() -> Result<()> {
        let batch = definition_batch()?;
        let memory = MemoryBudget::new(4096);
        let mut definitions = DefinitionSet::new(&memory)?;
        definitions.push_row(&batch, 0)?;
        assert_eq!(
            definitions.rows.as_slice(),
            &[Definition {
                name: "by_key".into(),
                table: TableRef::new("public", "events"),
                columns: vec!["key".into(), "_id".into()],
                version: 12,
            }],
        );
        assert!(definitions.push_row(&batch, 1).is_err());
        assert!(definitions.push_row(&batch, 2).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn ordered_definition_caller_retains_reservation_after_memo_eviction() -> Result<()> {
        let batch = definition_batch()?;
        let memory = MemoryBudget::new(4096);
        let cache = DefinitionCache::default();
        let cancellation = CancellationToken::new();
        let load = || async {
            let mut definitions = DefinitionSet::new(&memory)?;
            definitions.push_row(&batch, 0)?;
            Ok(definitions)
        };
        let caller = cache.get_or_load("database", rtdb_types::Timestamp(0).into(), &memory, &cancellation, load())
            .await?.unwrap();
        let charged = memory.used_bytes();
        for micros in 1..=4 {
            let now = rtdb_types::Timestamp(micros).into();
            assert!(cache.get_or_load("database", now, &memory, &cancellation, load()).await?.is_some());
        }
        assert_eq!(memory.used_bytes(), 5 * charged);
        assert!(caller.rows[0].columns.iter().map(String::as_str).eq(["key", "_id"]));
        drop(caller);
        assert_eq!(memory.used_bytes(), 4 * charged);
        drop(cache);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn exhausted_ordered_definition_memo_does_not_publish_unfunded_rows() -> Result<()> {
        let batch = definition_batch()?;
        let memory = MemoryBudget::new(4096);
        let occupied = memory.reserve(memory.limit_bytes() - 256)?;
        let cache = DefinitionCache::default();
        let cancellation = CancellationToken::new();
        let now = rtdb_types::Timestamp(0).into();
        let load = || async {
            let mut definitions = DefinitionSet::new(&memory)?;
            definitions.push_row(&batch, 0)?;
            Ok(definitions)
        };
        assert!(cache.get_or_load("database", now, &memory, &cancellation, load()).await?.is_none());
        assert_eq!(memory.used_bytes(), occupied.bytes());
        drop(occupied);
        let caller = cache.get_or_load("database", now, &memory, &cancellation, load()).await?.unwrap();
        assert_eq!(caller.rows[0].name, "by_key");
        assert_eq!(memory.used_bytes(), caller.reservation.bytes());
        Ok(())
    }

    #[test]
    fn definition_directory_matches_the_legacy_json_preimage() -> Result<()> {
        let definition = Definition {
            name: "path / \\ \" é".into(),
            table: TableRef::new("quoted\n", "表"),
            columns: vec!["key".into(), "_id".into()],
            version: 12345,
        };
        let preimage = serde_json::to_vec(&(
            &definition.table.schema,
            &definition.table.table,
            &definition.name,
            definition.version,
            PAGE_LAYOUT_VERSION,
        ))
        .map_err(format_error)?;
        let legacy = ObjectKey::new(format!(
            "{INDEX_ROOT}/{:016x}-{:x}",
            definition.version,
            Sha256::digest(&preimage),
        ))?;
        let storage = BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20);
        assert_eq!(definition.directory(&storage)?, legacy);
        Ok(())
    }
}
