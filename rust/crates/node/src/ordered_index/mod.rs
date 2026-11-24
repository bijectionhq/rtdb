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

use std::{collections::BTreeMap, sync::Arc};

use arrow_array::RecordBatch;
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation, read_entity_id, read_value};
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
const PAGE_LAYOUT_VERSION: u8 = 11;
const CATALOG: (&str, &str) = ("xt", "ordered_indexes");

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Definition {
    name: String,
    table: TableRef,
    columns: Vec<String>,
    version: i64,
}

impl Definition {
    fn directory(&self) -> Result<ObjectKey> {
        let mut digest = Sha256::new();
        serde_json::to_writer(
            &mut digest,
            &(
                &self.table.schema,
                &self.table.table,
                &self.name,
                self.version,
                PAGE_LAYOUT_VERSION,
            ),
        )
        .map_err(format_error)?;
        let digest = digest.finalize();
        Ok(ObjectKey::new(format!(
            "{INDEX_ROOT}/{:016x}-{digest:x}",
            self.version
        ))?)
    }

    fn from_row(batch: &RecordBatch, row: usize) -> Result<Self> {
        let cell = |name| ArrowCell::named(batch, name, row);
        let text = |name| {
            cell(name)
                .and_then(ArrowCell::utf8)
                .ok_or_else(|| format_error("Expected an index name or column"))
        };
        let columns = cell("columns")
            .and_then(ArrowCell::list)
            .ok_or_else(|| format_error("Expected index columns"))?;
        let version = cell("version")
            .and_then(ArrowCell::int64)
            .ok_or_else(|| format_error("Expected an index version"))?;
        Ok(Self {
            name: text("_id")?.to_owned(),
            table: TableRef::new(text("schema_name")?, text("table_name")?),
            columns: columns
                .map(|cell| {
                    cell.utf8()
                        .map(str::to_owned)
                        .ok_or_else(|| format_error("Expected an index name or column"))
                })
                .collect::<Result<_>>()?,
            version,
        })
    }
}

#[derive(Default)]
struct DefinitionCache {
    tables: parking_lot::Mutex<BTreeMap<(String, Instant, usize), Arc<tokio::sync::OnceCell<CachedDefinitions>>>>,
}

type CachedDefinitions = (Arc<[Definition]>, Option<Arc<MemoryReservation>>);

async fn definitions(
    database: &str, snapshot: &Snapshot, storage: &Arc<BufferPool>, memory: &Arc<MemoryBudget>,
    now: Instant, cancellation: &CancellationToken,
) -> Result<Arc<[Definition]>> {
    let load = || load_definitions(database, snapshot, storage, memory, now, cancellation);
    let Some(budget) = memory.snapshot_budget() else { return load().await.map(Into::into); };
    if !snapshot.contains_table(&TableRef::new(CATALOG.0, CATALOG.1)) {
        return Ok(Arc::from([]));
    }
    if cancellation.is_cancelled() { return Err(maintenance::cancelled()); }
    let cache = snapshot.memo::<DefinitionCache>();
    let entry = {
        let mut tables = cache.tables.lock();
        let key = (database.to_owned(), now, Arc::as_ptr(budget) as usize);
        if !tables.contains_key(&key) && tables.len() >= 4 {
            tables.pop_first();
        }
        Arc::clone(tables.entry(key).or_default())
    };
    let loaded = entry.get_or_try_init(|| async {
        let definitions = load().await?;
        let bytes = definitions.iter().fold(256usize, |bytes, definition| {
            bytes.saturating_add(256 + definition.name.len() + definition.table.schema.len() + definition.table.table.len())
                .saturating_add(definition.columns.iter().map(|column| 32 + column.len()).sum::<usize>())
        });
        Ok::<_, Anomaly>((definitions.into(), budget.reserve(bytes).ok()))
    });
    let (definitions, charge) = cancellation.run_until_cancelled(loaded).await.ok_or_else(maintenance::cancelled)??;
    if charge.is_none() {
        cache.tables.lock().retain(|_, candidate| !Arc::ptr_eq(candidate, &entry));
    }
    Ok(Arc::clone(definitions))
}

async fn load_definitions(
    database: &str,
    snapshot: &Snapshot,
    storage: &Arc<BufferPool>,
    memory: &Arc<MemoryBudget>,
    now: Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<Definition>> {
    let name = QualifiedTable {
        database: database.to_owned(),
        table: TableRef::new(CATALOG.0, CATALOG.1),
    };
    if !snapshot.contains_table(&name.table) {
        return Ok(Vec::new());
    }
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
    read_definitions(cursor).await
}

async fn owned_definitions(
    database: &str, snapshot: &Snapshot, storage: &Arc<BufferPool>, memory: &Arc<MemoryBudget>,
    now: Instant, cancellation: &CancellationToken,
) -> Result<Vec<Definition>> {
    let tables = snapshot.tables();
    let owned_catalogs: Vec<_> = tables.iter().filter(|table| table.is_owned_path_catalog()).collect();
    if owned_catalogs.is_empty() { return Ok(Vec::new()); }
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
    let mut definitions = Vec::new();
    for table in owned_catalogs {
        if let Some(cursor) = rtdb_query::open_owned_path_catalog(Arc::clone(&context), table, None).await? {
            for definition in read_definitions(cursor).await? {
                if !table.owns_path_target(&definition.table) {
                    return Err(format_error("Owned path definition refers to another owner or an internal catalog"));
                }
                if let Some(overlays) = world_tables.get(&definition.table) {
                    definitions.extend(overlays.iter().map(|table| Definition {
                        table: (**table).clone(),
                        ..definition.clone()
                    }));
                }
                definitions.push(definition);
            }
        }
    }
    definitions.sort();
    Ok(definitions)
}

async fn read_definitions(mut cursor: rtdb_query::QueryCursor) -> Result<Vec<Definition>> {
    let mut definitions = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        for row in 0..batch.num_rows() {
            if let Ok(definition) = Definition::from_row(batch, row) {
                definitions.push(definition);
                continue;
            }
            let schema = batch.schema_ref();
            let cell = |column| read_value(schema.field(column), batch.column(column).as_ref(), row);
            let text = |value| match value {
                Value::Utf8(value) => Ok(value),
                _ => Err(format_error("Expected an index name or column")),
            };
            let Value::List(columns) = cell(3)? else {
                return Err(format_error("Expected index columns"));
            };
            let Value::I64(version) = cell(4)? else {
                return Err(format_error("Expected an index version"));
            };
            definitions.push(Definition {
                name: text(cell(0)?)?,
                table: TableRef::new(text(cell(1)?)?, text(cell(2)?)?),
                columns: columns.into_values().into_iter().map(text).collect::<Result<_>>()?,
                version,
            });
        }
    }
    definitions.sort();
    Ok(definitions)
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
            Self::Values(fields) => fields.get("_id").map(|id| id.entity_id().map(|_| id.logical_type())),
            Self::Arrow { .. } => self.arrow_field("_id").map(|cell| {
                read_entity_id(cell.field, cell.array, cell.row)?;
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
                let id = self.arrow_field("_id").and_then(ArrowCell::utf8).ok_or_else(invalid)?;
                FixedRangeKey::from_parts(key, id)
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
) -> Result<Vec<String>> {
    Ok(owned_definition(context, owner, relation, path).await?.columns)
}

pub async fn stable_owned_path_columns(
    before: &Arc<QueryContext>,
    after: &Arc<QueryContext>,
    owner: &str,
    relation: &rtdb_types::OwnedRelation,
    path: &rtdb_types::OwnedRelation,
) -> Result<Vec<String>> {
    let definition = owned_definition(before, owner, relation, path).await?;
    if definition != owned_definition(after, owner, relation, path).await? {
        return Err(uncovered_owned_range());
    }
    Ok(definition.columns)
}

async fn owned_definition(
    context: &Arc<QueryContext>,
    owner: &str,
    relation: &rtdb_types::OwnedRelation,
    path: &rtdb_types::OwnedRelation,
) -> Result<Definition> {
    let catalog = rtdb_types::OwnedRelation::path_catalog(owner);
    let root = relation.root_relation();
    let target = root.as_ref().unwrap_or(relation).table(owner);
    let cursor =
        rtdb_query::open_owned_path_catalog(Arc::clone(context), &catalog, Some(path.name()))
            .await?
            .ok_or_else(uncovered_owned_range)?;
    let mut definitions = read_definitions(cursor).await?.into_iter();
    let mut definition = definitions
        .next()
        .filter(|definition| {
            definition.name == path.name()
                && definition.table == target
                && definitions.next().is_none()
        })
        .ok_or_else(uncovered_owned_range)?;
    definition.table = relation.table(owner);
    Ok(definition)
}

fn format_error(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::fault("rtdb.ordered-index/format", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_definition_admits_text_and_i64_from_selected_union_legs() -> Result<()> {
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
        let batch = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns)
            .map_err(format_error)?;
        assert_eq!(
            Definition::from_row(&batch, 0)?,
            Definition {
                name: "by_key".into(),
                table: TableRef::new("public", "events"),
                columns: vec!["key".into(), "_id".into()],
                version: 12,
            },
        );
        assert!(Definition::from_row(&batch, 1).is_err());
        assert!(Definition::from_row(&batch, 2).is_err());
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
        assert_eq!(definition.directory()?, legacy);
        Ok(())
    }
}
