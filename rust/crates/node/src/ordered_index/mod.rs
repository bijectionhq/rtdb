mod accelerate;
mod checkpoint;
mod correction;
mod domain;
mod key;
mod maintenance;
mod postings;
mod projection;
mod replay_coverage;
mod seek;
mod tree;
mod usage;

use std::{collections::BTreeMap, sync::Arc};

use rtdb_columnar::{MemoryBudget, read_value};
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
const PAGE_LAYOUT_VERSION: u8 = 9;
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
        let digest = Sha256::digest(
            serde_json::to_vec(&(
                &self.table.schema,
                &self.table.table,
                &self.name,
                self.version,
                PAGE_LAYOUT_VERSION,
            ))
            .map_err(format_error)?,
        );
        Ok(ObjectKey::new(format!(
            "{INDEX_ROOT}/{:016x}-{digest:x}",
            self.version
        ))?)
    }
}

async fn definitions(
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
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            let cell =
                |column| read_value(schema.field(column), batch.column(column).as_ref(), row);
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
                columns: columns.into_iter().map(text).collect::<Result<_>>()?,
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

/// Full-key bounds for a covered page. Bound documents name every path column.
pub struct OwnedRangeRequest<'a> {
    pub lower: Option<&'a rtdb_types::StructFields<Value>>,
    pub upper: Option<&'a rtdb_types::StructFields<Value>>,
    pub after: Option<&'a rtdb_types::StructFields<Value>>,
    pub descending: bool,
    pub limit: usize,
}

pub struct OwnedCountRequest<'a> {
    pub lower: Option<&'a rtdb_types::StructFields<Value>>,
    pub upper: Option<&'a rtdb_types::StructFields<Value>>,
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
        bounds.identity_bound(),
    )
    .await?;
    let work = accelerate::owned_page(
        &context, snapshot, &database.storage, &definition, projection, bounds, valid_at,
    );
    rtdb_storage::IoRegion::leased(context.catalog.read_lease(), work).await
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
    let identity = [request.lower, request.upper]
        .into_iter()
        .flatten()
        .find_map(|key| key.get("_id"));
    let projection = owned_projection(&context, snapshot, &definition, owner, relation, identity).await?;
    let work = accelerate::owned_count(
        &context, snapshot, &database.storage, &definition, projection, request, lifetime, domain,
    );
    rtdb_storage::IoRegion::leased(context.catalog.read_lease(), work).await
}

async fn owned_projection(
    context: &Arc<QueryContext>,
    snapshot: &Snapshot,
    definition: &Definition,
    owner: &str,
    relation: &rtdb_types::OwnedRelation,
    identity: Option<&Value>,
) -> Result<OwnedProjection> {
    let empty = snapshot.relation_event_frontier(&definition.table)? == 0;
    let mut types = snapshot.column_types_for(&definition.table, definition.columns.clone());
    if empty || relation.world().is_some() {
        let root = relation.root_relation();
        let (declaration, _charge) =
            rtdb_query::read_owned_relation_schema(Arc::clone(context), owner, root.as_ref().unwrap_or(relation))
                .await?
                .ok_or_else(uncovered_owned_range)?;
        let identity = match identity {
            Some(identity) => {
                identity.entity_id()?;
                identity.logical_type()
            }
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
