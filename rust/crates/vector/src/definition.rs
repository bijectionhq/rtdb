use std::collections::BTreeMap;
use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, read_execution_value};
use rtdb_index::Snapshot;
use rtdb_maintenance::ArtefactDefinition;
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::cache::BufferPool;
use rtdb_types::{Anomaly, Instant, Result, TableRef, Value};
use tokio_util::sync::CancellationToken;

/// The artefact kind of approximate vector generations (vector.allium: VectorGeneration).
pub const KIND: &str = "vector";
/// The catalog relation holding vector index definitions (vector.allium: VectorIndexDefinition).
pub const DEFINITIONS: (&str, &str) = ("xt", "vector_indexes");
/// The generation file layout; it participates in every artefact address.
pub(crate) const FILE_VERSION: u32 = 1;

/// How an approximate index selects candidates.
/// IVF is the only method: each generation is a pure function of its trie, so it is byte-deterministic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Method { Ivf }

impl Method {
    pub fn name(self) -> &'static str { match self { Self::Ivf => "ivf" } }

    pub fn parse(name: &str) -> Result<Self> {
        match name.to_ascii_lowercase().as_str() {
            "ivf" => Ok(Self::Ivf),
            _ => Err(Anomaly::incorrect("rtdb.vector/method", format!("Unknown approximate method {name}; the supported method is ivf"))),
        }
    }
}

/// The declared approximate route: its method and the recall it targets.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Approximate { pub method: Method, pub recall: f64 }

impl Approximate {
    pub fn new(method: Method, recall: f64) -> Result<Self> {
        if !(recall.is_finite() && recall > 0.0 && recall <= 1.0) {
            return Err(Anomaly::incorrect("rtdb.vector/recall", "An approximate recall target lies in (0, 1]"));
        }
        Ok(Self { method, recall })
    }
}

/// One generation of a vector index definition, as `xt.vector_indexes` holds it.
/// The generation is the transaction that declared it, so a redeclaration after a drop has a fresh one.
#[derive(Clone, Debug, PartialEq)]
pub struct VectorDefinition {
    pub name: String,
    pub table: TableRef,
    pub column: String,
    pub partition: Vec<String>,
    pub dimensions: usize,
    pub approximate: Approximate,
    pub generation: i64,
}

impl VectorDefinition {
    /// The artefact ID: the index name, its method and the file layout.
    pub fn artefact_id(&self) -> String {
        format!("{}~{}f{}", self.name, self.approximate.method.name(), FILE_VERSION)
    }

    /// Generations cover current tries; history and temporal ranges take the exact route.
    pub fn artefact(&self) -> ArtefactDefinition {
        ArtefactDefinition {
            table: self.table.clone(), kind: KIND.into(), id: self.artefact_id(), version: self.generation,
            historical: false, group_bytes: None, packed: false,
        }
    }

    /// The columns a generation build projects from each put.
    pub(crate) fn put_columns(&self) -> Vec<String> {
        let mut columns = self.partition.clone();
        columns.push(self.column.clone());
        columns
    }
}

/// The definitions in force in the database's snapshots, as of `now` in valid time.
pub async fn definitions_in(
    database: &str, partitions: Vec<Snapshot>, storage: Arc<BufferPool>, memory: Arc<MemoryBudget>, now: impl Into<Instant>,
    cancellation: &CancellationToken,
) -> Result<Vec<VectorDefinition>> {
    let table = TableRef::new(DEFINITIONS.0, DEFINITIONS.1);
    if !partitions.iter().any(|snapshot| snapshot.contains_table(&table)) { return Ok(Vec::new()); }
    let catalog = CatalogSnapshot::new(database, BTreeMap::from([(database.to_string(), DatabaseSnapshot { partitions, storage })]), false)?;
    let context = QueryContext::new(catalog, Arc::clone(&memory), now, "UTC", QueryOptions::default(), cancellation)?;
    let sql = "SELECT _id, schema_name, table_name, column_name, partition_columns, dimensions, method, recall, version FROM xt.vector_indexes";
    let prepared = rtdb_sql::prepare_in(sql, &context.catalog, &rtdb_sql::PrepareOptions::default(), &memory)?;
    let mut cursor = PreparedQuery::from_prepared(prepared, &memory)?.execute(context, Vec::new()).await?;
    let mut definitions = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        let schema = batch.schema_ref();
        for row in 0..batch.num_rows() {
            let value = |column: usize| read_execution_value(schema.field(column), batch.column(column).as_ref(), row);
            let text = |column: usize| match value(column)? { Value::Utf8(text) => Ok(text), _ => Err(malformed(schema.field(column).name())) };
            let integer = |column: usize| match value(column)? {
                Value::I64(value) => Ok(value), Value::I32(value) => Ok(i64::from(value)), _ => Err(malformed(schema.field(column).name())),
            };
            let partition = match value(4)? {
                Value::Null | Value::Undefined => Vec::new(),
                list => {
                    let view = list.list_view().ok_or_else(|| malformed("partition_columns"))?;
                    (0..view.len()).map(|index| view.utf8(index).map(str::to_owned).ok_or_else(|| malformed("partition_columns")))
                        .collect::<Result<Vec<_>>>()?
                }
            };
            let recall = match value(7)? { Value::F64(value) => value, Value::F32(value) => f64::from(value), _ => return Err(malformed("recall")) };
            let dimensions = usize::try_from(integer(5)?).ok().filter(|dimensions| *dimensions > 0).ok_or_else(|| malformed("dimensions"))?;
            definitions.push(VectorDefinition {
                name: text(0)?, table: TableRef::new(text(1)?, text(2)?), column: text(3)?, partition, dimensions,
                approximate: Approximate::new(Method::parse(&text(6)?)?, recall)?, generation: integer(8)?,
            });
        }
    }
    definitions.sort_by(|left, right| (&left.table, &left.name, left.generation).cmp(&(&right.table, &right.name, right.generation)));
    Ok(definitions)
}

/// Every owner's approximate vector paths in the snapshot, for the artefact lane.
pub(crate) async fn owned_definitions_in(
    database: &str, snapshot: Snapshot, storage: Arc<BufferPool>, memory: Arc<MemoryBudget>, now: impl Into<Instant>,
    cancellation: &CancellationToken,
) -> Result<Vec<VectorDefinition>> {
    let catalogs: Vec<_> = snapshot.tables().into_iter().filter(TableRef::is_owned_vector_catalog).collect();
    if catalogs.is_empty() { return Ok(Vec::new()); }
    let catalog = CatalogSnapshot::new(database, BTreeMap::from([(database.to_owned(), DatabaseSnapshot { partitions: vec![snapshot], storage })]), false)?;
    let context = QueryContext::new(catalog, Arc::clone(&memory), now, "UTC", QueryOptions::default(), cancellation)?;
    let mut definitions = Vec::new();
    for catalog in catalogs {
        definitions.extend(owned_catalog_definitions(Arc::clone(&context), &catalog, None).await?);
    }
    Ok(definitions)
}

/// The approximate vector path over `column` of an owned relation, from its owner's catalog.
pub async fn owned_definition(context: Arc<QueryContext>, table: &TableRef, column: &str) -> Result<Option<VectorDefinition>> {
    let Some(catalog) = table.owned_vector_catalog() else { return Ok(None) };
    Ok(owned_catalog_definitions(context, &catalog, None).await?.into_iter()
        .find(|definition| &definition.table == table && definition.column == column))
}

async fn owned_catalog_definitions(context: Arc<QueryContext>, catalog: &TableRef, path: Option<&str>) -> Result<Vec<VectorDefinition>> {
    let Some(mut cursor) = rtdb_query::open_owned_path_catalog(context, catalog, path).await? else { return Ok(Vec::new()) };
    let mut definitions = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        for row in 0..batch.num_rows() {
            let cell = |name| rtdb_columnar::ArrowCell::named(batch, name, row);
            let text = |name| cell(name).and_then(rtdb_columnar::ArrowCell::utf8).map(str::to_owned).ok_or_else(|| malformed(name));
            let mut columns = cell("columns").and_then(rtdb_columnar::ArrowCell::list).ok_or_else(|| malformed("columns"))?;
            let column = columns.next().and_then(|cell| cell.utf8()).map(str::to_owned).ok_or_else(|| malformed("columns"))?;
            if columns.next().is_some() { return Err(malformed("columns")); }
            let table = TableRef::new(text("schema_name")?, text("table_name")?);
            if !catalog.owns_path_target(&table) { return Err(malformed("table_name")); }
            let recall = match cell("recall").map(|cell| read_execution_value(cell.field, cell.array, cell.row)).transpose()? {
                Some(Value::F64(recall)) => recall,
                _ => return Err(malformed("recall")),
            };
            let dimensions = cell("dimensions").and_then(rtdb_columnar::ArrowCell::int64)
                .and_then(|dimensions| usize::try_from(dimensions).ok()).filter(|dimensions| *dimensions > 0).ok_or_else(|| malformed("dimensions"))?;
            definitions.push(VectorDefinition {
                name: text("_id")?, table, column, partition: Vec::new(), dimensions,
                approximate: Approximate::new(Method::Ivf, recall)?,
                generation: cell("version").and_then(rtdb_columnar::ArrowCell::int64).ok_or_else(|| malformed("version"))?,
            });
        }
    }
    Ok(definitions)
}

fn malformed(column: &str) -> Anomaly {
    Anomaly::fault("rtdb.vector/malformed-definition", format!("Malformed vector index definition in {column}"))
}
