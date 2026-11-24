use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Float32Array, Float64Array, RecordBatch};
use rtdb_columnar::{ArrowCell, MemoryBudget, read_execution_value};
use rtdb_index::Snapshot;
use rtdb_maintenance::ArtefactDefinition;
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::cache::BufferPool;
use rtdb_types::{Anomaly, Instant, ListKind, LogicalType, Result, ScalarType, TableRef, Value};
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
        for row in 0..batch.num_rows() {
            definitions.push(sql_definition(batch, row)?);
        }
    }
    definitions.sort_by(|left, right| (&left.table, &left.name, left.generation).cmp(&(&right.table, &right.name, right.generation)));
    Ok(definitions)
}

fn sql_definition(batch: &RecordBatch, row: usize) -> Result<VectorDefinition> {
    let cell = |column| ArrowCell {
        field: batch.schema_ref().field(column),
        array: batch.column(column).as_ref(),
        row,
    };
    let text = |column| catalog_text(cell(column));
    let partition = catalog_partition(cell(4))?;
    let recall = catalog_recall(cell(7), true)?;
    let dimensions = usize::try_from(catalog_integer(cell(5))?).ok().filter(|dimensions| *dimensions > 0)
        .ok_or_else(|| malformed("dimensions"))?;
    Ok(VectorDefinition {
        name: text(0)?.into_owned(),
        table: TableRef::new(text(1)?, text(2)?),
        column: text(3)?.into_owned(),
        partition,
        dimensions,
        approximate: Approximate::new(Method::parse(&text(6)?)?, recall)?,
        generation: catalog_integer(cell(8))?,
    })
}

fn nonnull(logical: &LogicalType) -> &LogicalType {
    match logical { LogicalType::Maybe(inner) => inner, logical => logical }
}

fn catalog_scalar(cell: ArrowCell<'_>) -> Option<(ArrowCell<'_>, ScalarType)> {
    let (cell, logical) = cell.logical_position(None).ok()?;
    let LogicalType::Scalar(scalar) = nonnull(&logical) else { return None; };
    (!cell.array.is_null(cell.row) && cell.has_scalar_storage(scalar)).then(|| (cell, scalar.clone()))
}

fn decode_catalog_cell(cell: ArrowCell<'_>) -> Result<Value> {
    read_execution_value(cell.field, cell.array, cell.row)
}

fn catalog_text(cell: ArrowCell<'_>) -> Result<Cow<'_, str>> {
    if let Some((selected, ScalarType::Utf8)) = catalog_scalar(cell)
        && let Some(text) = selected.utf8()
    {
        return Ok(Cow::Borrowed(text));
    }
    match decode_catalog_cell(cell)? {
        Value::Utf8(text) => Ok(Cow::Owned(text)),
        _ => Err(malformed(cell.field.name())),
    }
}

fn catalog_integer(cell: ArrowCell<'_>) -> Result<i64> {
    if let Some((selected, ScalarType::I32 | ScalarType::I64 | ScalarType::U32 | ScalarType::U64)) = catalog_scalar(cell)
        && let Some(value) = selected.signed_integer()
    {
        return Ok(value);
    }
    match decode_catalog_cell(cell)? {
        Value::I32(value) => Ok(i64::from(value)),
        Value::I64(value) => Ok(value),
        _ => Err(malformed(cell.field.name())),
    }
}

fn catalog_recall(cell: ArrowCell<'_>, allow_f32: bool) -> Result<f64> {
    if let Some((selected, scalar)) = catalog_scalar(cell) {
        let value = match scalar {
            ScalarType::F64 => selected.array.as_any().downcast_ref::<Float64Array>()
                .map(|array| array.value(selected.row)),
            ScalarType::F32 if allow_f32 => selected.array.as_any().downcast_ref::<Float32Array>()
                .map(|array| f64::from(array.value(selected.row))),
            _ => None,
        };
        if let Some(value) = value { return Ok(value); }
    }
    match decode_catalog_cell(cell)? {
        Value::F64(value) => Ok(value),
        Value::F32(value) if allow_f32 => Ok(f64::from(value)),
        _ => Err(malformed("recall")),
    }
}

fn borrowed_partition(cell: ArrowCell<'_>) -> Option<Vec<String>> {
    let (cell, logical) = cell.logical_position(None).ok()?;
    if logical == LogicalType::Null || cell.array.is_null(cell.row) { return Some(Vec::new()); }
    let LogicalType::List { kind, element } = nonnull(&logical) else { return None; };
    if cell.sequence_kind().as_ref() != Some(kind) { return None; }
    match kind {
        ListKind::List | ListKind::Set => (),
        ListKind::FixedSizeList(width) => {
            i32::try_from(cell.row.checked_mul(usize::try_from(*width).ok()?)?).ok()?;
        }
        _ => return None,
    }
    cell.sequence()?.map(|child| {
        let (child, logical) = child.logical_position(Some(element)).ok()?;
        match nonnull(&logical) {
            LogicalType::Scalar(ScalarType::Utf8) => child.utf8().map(str::to_owned),
            _ => None,
        }
    }).collect()
}

fn catalog_partition(cell: ArrowCell<'_>) -> Result<Vec<String>> {
    if let Some(columns) = borrowed_partition(cell) { return Ok(columns); }
    // A decoding error in a later element takes precedence over an earlier non-text element.
    match decode_catalog_cell(cell)? {
        Value::Null | Value::Undefined => Ok(Vec::new()),
        list => {
            let view = list.list_view().ok_or_else(|| malformed("partition_columns"))?;
            (0..view.len())
                .map(|index| view.utf8(index).map(str::to_owned).ok_or_else(|| malformed("partition_columns")))
                .collect()
        }
    }
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
            let cell = |name| ArrowCell::named(batch, name, row);
            let text = |name| cell(name).and_then(ArrowCell::utf8).map(str::to_owned).ok_or_else(|| malformed(name));
            let mut columns = cell("columns").and_then(ArrowCell::list).ok_or_else(|| malformed("columns"))?;
            let column = columns.next().and_then(|cell| cell.utf8()).map(str::to_owned).ok_or_else(|| malformed("columns"))?;
            if columns.next().is_some() { return Err(malformed("columns")); }
            let table = TableRef::new(text("schema_name")?, text("table_name")?);
            if !catalog.owns_path_target(&table) { return Err(malformed("table_name")); }
            let recall = catalog_recall(cell("recall").ok_or_else(|| malformed("recall"))?, false)?;
            let dimensions = cell("dimensions").and_then(ArrowCell::int64)
                .and_then(|dimensions| usize::try_from(dimensions).ok()).filter(|dimensions| *dimensions > 0).ok_or_else(|| malformed("dimensions"))?;
            definitions.push(VectorDefinition {
                name: text("_id")?, table, column, partition: Vec::new(), dimensions,
                approximate: Approximate::new(Method::Ivf, recall)?,
                generation: cell("version").and_then(ArrowCell::int64).ok_or_else(|| malformed("version"))?,
            });
        }
    }
    Ok(definitions)
}

fn malformed(column: &str) -> Anomaly {
    Anomaly::fault("rtdb.vector/malformed-definition", format!("Malformed vector index definition in {column}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Array, ArrayRef, ListArray, NullArray, StringArray, UInt32Array, UInt64Array,
        builder::{ListBuilder, StringBuilder},
    };
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::{DataType, Field};

    #[test]
    fn catalog_rows_keep_partition_and_number_semantics() {
        let mut partition = ListBuilder::new(StringBuilder::new());
        partition.values().append_value("tenant");
        partition.append(true);
        partition.append(false);
        partition.append(false);
        partition.append(false);
        let text = |value: &str| Arc::new(StringArray::from(vec![value; 4])) as ArrayRef;
        let batch = RecordBatch::try_from_iter([
            ("_id", text("idx")),
            ("schema_name", text("public")),
            ("table_name", text("docs")),
            ("column_name", text("embedding")),
            ("partition_columns", Arc::new(partition.finish())),
            ("dimensions", Arc::new(UInt32Array::from(vec![3, 4, u32::MAX, u32::MAX]))),
            ("method", text("IVF")),
            ("recall", Arc::new(Float32Array::from(vec![Some(0.9), Some(1.0), None, Some(1.0)]))),
            ("version", Arc::new(UInt64Array::from(vec![u64::MAX, 2, 3, 4]))),
        ]).unwrap();
        let first = sql_definition(&batch, 0).unwrap();
        assert_eq!(first.partition, ["tenant"]);
        assert_eq!(first.dimensions, 3);
        assert_eq!(first.generation, -1);
        assert_eq!(first.approximate, Approximate { method: Method::Ivf, recall: f64::from(0.9_f32) });
        assert!(sql_definition(&batch, 1).unwrap().partition.is_empty());
        assert_eq!(sql_definition(&batch, 2), Err(malformed("recall")));
        assert_eq!(sql_definition(&batch, 3), Err(malformed("dimensions")));

        let absent = Field::new("partition_columns", DataType::Null, true);
        let array = NullArray::new(1);
        assert!(catalog_partition(ArrowCell { field: &absent, array: &array, row: 0 }).unwrap().is_empty());
        assert_eq!(catalog_recall(ArrowCell::named(&batch, "recall", 0).unwrap(), false), Err(malformed("recall")));
    }

    #[test]
    fn partition_decode_error_precedes_non_text_validation() {
        let item = Arc::new(Field::new("item", DataType::Utf8, true).with_metadata(
            [(rtdb_columnar::EXTENSION_NAME_KEY.to_owned(), "uri".to_owned())].into(),
        ));
        let list = ListArray::new(item, OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(StringArray::from(vec![None, Some("not a uri")])), None);
        let field = Field::new("partition_columns", list.data_type().clone(), false);
        let error = catalog_partition(ArrowCell { field: &field, array: &list, row: 0 }).unwrap_err();
        assert_eq!(error.code, "xtdb/invalid-uri");
    }
}
