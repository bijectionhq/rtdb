use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Float32Array, Float64Array, RecordBatch};
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation, estimate_read_value_bytes, read_execution_value};
use rtdb_index::Snapshot;
use rtdb_maintenance::ArtefactDefinition;
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryCursor, QueryOptions};
use rtdb_storage::cache::BufferPool;
use rtdb_types::{Anomaly, Instant, ListKind, LogicalType, Result, ScalarType, TableRef, Value};
use rtdb_plan::Catalog;
use tokio_util::sync::CancellationToken;

use crate::shortlist::admit_growth;

/// The artefact kind of approximate vector generations (vector.allium: VectorGeneration).
pub const KIND: &str = "vector";
/// The catalog relation holding vector index definitions (vector.allium: VectorIndexDefinition).
pub const DEFINITIONS: (&str, &str) = ("xt", "vector_indexes");
const NATIVE_DEFINITIONS: &str = "SELECT index_name || '~native' AS name, schema_name, table_name, column_name,
    partition_columns, dimensions, method, recall, version FROM xt.native_vector_builds";
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

/// Catalog metadata with its retained allocation grant.
pub struct CatalogDefinition {
    definition: VectorDefinition,
    _allocation: Arc<MemoryReservation>,
}

impl std::ops::Deref for CatalogDefinition {
    type Target = VectorDefinition;

    fn deref(&self) -> &VectorDefinition { &self.definition }
}

pub struct Definitions {
    rows: Vec<CatalogDefinition>,
    allocation: Arc<MemoryReservation>,
}

impl Definitions {
    fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        Ok(Self { rows: Vec::new(), allocation: memory.reserve(0)? })
    }

    fn push(&mut self, row: CatalogDefinition) -> Result<()> {
        admit_growth(&mut self.rows, 0, Arc::get_mut(&mut self.allocation).expect("vector catalog owns its allocation"))?;
        self.rows.push(row);
        Ok(())
    }

    pub fn rows(&self) -> &[CatalogDefinition] { &self.rows }

    fn sort(&mut self, memory: &Arc<MemoryBudget>) -> Result<()> {
        let _sort = memory.reserve(self.rows.len().saturating_mul(size_of::<CatalogDefinition>()))?;
        self.rows.sort_by(|left, right| (&left.table, &left.name, left.generation).cmp(&(&right.table, &right.name, right.generation)));
        Ok(())
    }

    pub(crate) fn take(&mut self, matches: impl Fn(&VectorDefinition) -> bool) -> Option<CatalogDefinition> {
        let index = self.rows.iter().position(|definition| matches(definition))?;
        Some(self.rows.swap_remove(index))
    }
}

/// The definitions in force in the database's snapshots, as of `now` in valid time.
pub async fn definitions_in(
    database: &str, partitions: Vec<Snapshot>, storage: Arc<BufferPool>, memory: Arc<MemoryBudget>, now: impl Into<Instant>,
    cancellation: &CancellationToken,
) -> Result<Definitions> {
    let mut definitions = Definitions::new(&memory)?;
    visit_sql_definitions(database, partitions, storage, &memory, now, cancellation,
        |row, allocation| definitions.push(row.own(allocation)?)).await?;
    definitions.sort(&memory)?;
    Ok(definitions)
}

pub(crate) async fn native_definitions_in(
    database: &str, snapshot: Snapshot, storage: Arc<BufferPool>, memory: Arc<MemoryBudget>, now: impl Into<Instant>,
    cancellation: &CancellationToken,
) -> Result<Definitions> {
    let mut definitions = Definitions::new(&memory)?;
    let catalog = CatalogSnapshot::new(database, BTreeMap::from([(
        database.to_owned(), DatabaseSnapshot { partitions: vec![snapshot], storage },
    )]), false)?;
    let context = QueryContext::new(catalog, Arc::clone(&memory), now, "UTC", QueryOptions::default(), cancellation)?;
    if let Some(cursor) = open_native_definitions(context, None).await? {
        visit_sql_rows(cursor, &memory, |row, allocation| definitions.push(row.own(allocation)?)).await?;
        definitions.sort(&memory)?;
    }
    Ok(definitions)
}

pub(crate) async fn committed_native_definition(
    context: Arc<QueryContext>, table: &TableRef, name: &str,
) -> Result<Option<CatalogDefinition>> {
    let memory = Arc::clone(&context.memory);
    let _id = memory.reserve(table.schema.len() + table.table.len() + name.len() + 64)?;
    let id = rtdb_types::rtdb_v1::ranked_build_id(table, name);
    let Some(cursor) = open_native_definitions(context, Some(id)).await? else { return Ok(None); };
    let mut found = None;
    let mut count = 0;
    visit_sql_rows(cursor, &memory, |row, allocation| {
        count += 1;
        if count == 1 && row.schema.as_ref() == table.schema.as_str() && row.table.as_ref() == table.table.as_str()
            && row.name.strip_suffix("~native") == Some(name)
        {
            found = Some(row.own(allocation)?);
        }
        Ok(())
    }).await?;
    Ok(found.filter(|_| count == 1))
}

async fn open_native_definitions(context: Arc<QueryContext>, id: Option<String>) -> Result<Option<QueryCursor>> {
    let table = TableRef::new("xt", rtdb_types::rtdb_v1::VECTOR_BUILD_CATALOG);
    if !context.catalog.database(context.catalog.default_database())?.partitions.iter()
        .any(|snapshot| snapshot.contains_table(&table))
    {
        return Ok(None);
    }
    let (sql, arguments) = match id {
        Some(id) => (Cow::Owned(format!("{NATIVE_DEFINITIONS} WHERE _id = $1")), vec![Value::Utf8(id)]),
        None => (Cow::Borrowed(NATIVE_DEFINITIONS), Vec::new()),
    };
    let memory = Arc::clone(&context.memory);
    let prepared = rtdb_sql::prepare_in(&sql, &context.catalog, &rtdb_sql::PrepareOptions::default(), &memory)?;
    PreparedQuery::from_prepared(prepared, &memory)?.execute(context, arguments).await.map(Some)
}

pub(crate) async fn sql_definition_for(
    context: &rtdb_query::ScanRequest<'_>, table: &TableRef, column: &str,
) -> Result<Option<CatalogDefinition>> {
    let mut found: Option<CatalogDefinition> = None;
    visit_sql_definitions(context.database, context.partitions.to_vec(), Arc::clone(context.storage), context.memory,
        context.current_time, context.cancellation, |row, allocation| {
            if row.schema.as_ref() == table.schema.as_str() && row.table.as_ref() == table.table.as_str()
                && row.column.as_ref() == column && found.as_ref().is_none_or(|previous| {
                (row.name.as_ref(), row.generation) < (previous.name.as_str(), previous.generation)
            }) {
                found = Some(row.own(allocation)?);
            }
            Ok(())
        }).await?;
    Ok(found)
}

async fn visit_sql_definitions(
    database: &str, partitions: Vec<Snapshot>, storage: Arc<BufferPool>, memory: &Arc<MemoryBudget>, now: impl Into<Instant>,
    cancellation: &CancellationToken, visit: impl FnMut(SqlRow<'_>, Arc<MemoryReservation>) -> Result<()>,
) -> Result<()> {
    let table = TableRef::new(DEFINITIONS.0, DEFINITIONS.1);
    if !partitions.iter().any(|snapshot| snapshot.contains_table(&table)) { return Ok(()); }
    let catalog = CatalogSnapshot::new(database, BTreeMap::from([(database.to_string(), DatabaseSnapshot { partitions, storage })]), false)?;
    let context = QueryContext::new(catalog, Arc::clone(memory), now, "UTC", QueryOptions::default(), cancellation)?;
    let sql = "SELECT _id, schema_name, table_name, column_name, partition_columns, dimensions, method, recall, version FROM xt.vector_indexes";
    let prepared = rtdb_sql::prepare_in(sql, &context.catalog, &rtdb_sql::PrepareOptions::default(), memory)?;
    let cursor = PreparedQuery::from_prepared(prepared, memory)?.execute(context, Vec::new()).await?;
    visit_sql_rows(cursor, memory, visit).await
}

async fn visit_sql_rows(
    mut cursor: QueryCursor, memory: &Arc<MemoryBudget>, mut visit: impl FnMut(SqlRow<'_>, Arc<MemoryReservation>) -> Result<()>,
) -> Result<()> {
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        for row in 0..batch.num_rows() {
            let mut allocation = memory.reserve(256)?;
            let charge = Arc::get_mut(&mut allocation).expect("vector catalog row owns its allocation");
            let definition = sql_row(batch, row, |cell| {
                let bytes = estimate_read_value_bytes(cell.field, cell.array, cell.row)?;
                charge.grow_to(charge.bytes().saturating_add(bytes.saturating_mul(2)))
            })?;
            visit(definition, allocation)?;
        }
    }
    Ok(())
}

fn definition_bytes(definition: &VectorDefinition) -> usize {
    [
        128,
        definition.name.capacity(),
        definition.table.schema.capacity(),
        definition.table.table.capacity(),
        definition.column.capacity(),
        definition.partition.capacity().saturating_mul(size_of::<String>()),
    ].into_iter().chain(definition.partition.iter().map(String::capacity)).fold(0, usize::saturating_add)
}

struct SqlRow<'a> {
    name: Cow<'a, str>,
    schema: Cow<'a, str>,
    table: Cow<'a, str>,
    column: Cow<'a, str>,
    partition: Vec<Cow<'a, str>>,
    dimensions: usize,
    approximate: Approximate,
    generation: i64,
}

impl SqlRow<'_> {
    fn into_definition(self) -> VectorDefinition {
        VectorDefinition {
            name: self.name.into_owned(), table: TableRef::new(self.schema, self.table), column: self.column.into_owned(),
            partition: self.partition.into_iter().map(Cow::into_owned).collect(),
            dimensions: self.dimensions, approximate: self.approximate, generation: self.generation,
        }
    }

    fn own(self, mut allocation: Arc<MemoryReservation>) -> Result<CatalogDefinition> {
        let definition = self.into_definition();
        Arc::get_mut(&mut allocation).expect("vector definition owns its allocation").shrink_to(definition_bytes(&definition))?;
        Ok(CatalogDefinition { definition, _allocation: allocation })
    }
}

fn sql_row<'a>(
    batch: &'a RecordBatch, row: usize, mut admit: impl FnMut(ArrowCell<'a>) -> Result<()>,
) -> Result<SqlRow<'a>> {
    let mut cell = |column| {
        let cell = ArrowCell {
            field: batch.schema_ref().field(column),
            array: batch.column(column).as_ref(),
            row,
        };
        admit(cell)?;
        Ok::<_, Anomaly>(cell)
    };
    let partition = catalog_partition(cell(4)?)?;
    let recall = catalog_recall(cell(7)?, true, None)?;
    let dimensions = usize::try_from(catalog_integer(cell(5)?)?).ok().filter(|dimensions| *dimensions > 0)
        .ok_or_else(|| malformed("dimensions"))?;
    Ok(SqlRow {
        name: catalog_text(cell(0)?)?,
        schema: catalog_text(cell(1)?)?,
        table: catalog_text(cell(2)?)?,
        column: catalog_text(cell(3)?)?,
        partition,
        dimensions,
        approximate: Approximate::new(Method::parse(&catalog_text(cell(6)?)?)?, recall)?,
        generation: catalog_integer(cell(8)?)?,
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

fn catalog_recall(cell: ArrowCell<'_>, allow_f32: bool, memory: Option<&Arc<MemoryBudget>>) -> Result<f64> {
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
    let _decode = memory.map(|memory| {
        memory.reserve(estimate_read_value_bytes(cell.field, cell.array, cell.row)?)
    }).transpose()?;
    match decode_catalog_cell(cell)? {
        Value::F64(value) => Ok(value),
        Value::F32(value) if allow_f32 => Ok(f64::from(value)),
        _ => Err(malformed("recall")),
    }
}

fn borrowed_partition(cell: ArrowCell<'_>) -> Option<Vec<Cow<'_, str>>> {
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
            LogicalType::Scalar(ScalarType::Utf8) => child.utf8().map(Cow::Borrowed),
            _ => None,
        }
    }).collect()
}

fn catalog_partition(cell: ArrowCell<'_>) -> Result<Vec<Cow<'_, str>>> {
    if let Some(columns) = borrowed_partition(cell) { return Ok(columns); }
    // A decoding error in a later element takes precedence over an earlier non-text element.
    match decode_catalog_cell(cell)? {
        Value::Null | Value::Undefined => Ok(Vec::new()),
        list => {
            let view = list.list_view().ok_or_else(|| malformed("partition_columns"))?;
            (0..view.len())
                .map(|index| view.utf8(index).map(|text| Cow::Owned(text.to_owned())).ok_or_else(|| malformed("partition_columns")))
                .collect()
        }
    }
}

/// Every owner's approximate vector paths in the snapshot, for the artefact lane.
pub(crate) async fn owned_definitions_in(
    database: &str, snapshot: Snapshot, storage: Arc<BufferPool>, memory: Arc<MemoryBudget>, now: impl Into<Instant>,
    cancellation: &CancellationToken,
) -> Result<Definitions> {
    let mut catalog_allocation = memory.reserve(0)?;
    let mut catalogs = Vec::<TableRef>::new();
    for table in snapshot.table_names().filter(|table| table.is_owned_vector_catalog()) {
        let bytes = table.schema.len().saturating_add(table.table.len());
        admit_growth(&mut catalogs, bytes, Arc::get_mut(&mut catalog_allocation).expect("catalog inventory owns its allocation"))?;
        catalogs.push(table.clone());
    }
    let mut definitions = Definitions::new(&memory)?;
    if catalogs.is_empty() { return Ok(definitions); }
    catalogs.sort_unstable();
    catalogs.dedup();
    let catalog = CatalogSnapshot::new(database, BTreeMap::from([(database.to_owned(), DatabaseSnapshot { partitions: vec![snapshot], storage })]), false)?;
    let context = QueryContext::new(catalog, Arc::clone(&memory), now, "UTC", QueryOptions::default(), cancellation)?;
    for catalog in &catalogs {
        visit_owned_catalog(Arc::clone(&context), catalog, |row| definitions.push(row.own(&memory)?)).await?;
    }
    Ok(definitions)
}

/// The approximate vector path over `column` of an owned relation, from its owner's catalog.
pub async fn owned_definition(context: Arc<QueryContext>, table: &TableRef, column: &str) -> Result<Option<CatalogDefinition>> {
    let Some(catalog) = table.owned_vector_catalog() else { return Ok(None) };
    let memory = Arc::clone(&context.memory);
    let mut found = None;
    visit_owned_catalog(context, &catalog, |row| {
        if found.is_none() && row.schema == table.schema.as_str() && row.table == table.table.as_str() && row.column == column {
            found = Some(row.own(&memory)?);
        }
        Ok(())
    }).await?;
    Ok(found)
}

struct OwnedRow<'a> {
    name: &'a str,
    schema: &'a str,
    table: &'a str,
    column: &'a str,
    dimensions: usize,
    approximate: Approximate,
    generation: i64,
}

impl OwnedRow<'_> {
    fn own(self, memory: &Arc<MemoryBudget>) -> Result<CatalogDefinition> {
        let bytes = [self.name, self.schema, self.table, self.column].into_iter()
            .fold(128usize, |bytes, field| bytes.saturating_add(field.len()));
        let allocation = memory.reserve(bytes)?;
        let definition = VectorDefinition {
            name: self.name.to_owned(), table: TableRef::new(self.schema, self.table), column: self.column.to_owned(),
            partition: Vec::new(), dimensions: self.dimensions, approximate: self.approximate, generation: self.generation,
        };
        Ok(CatalogDefinition { definition, _allocation: allocation })
    }
}

async fn visit_owned_catalog(
    context: Arc<QueryContext>, catalog: &TableRef, mut visit: impl FnMut(OwnedRow<'_>) -> Result<()>,
) -> Result<()> {
    let memory = Arc::clone(&context.memory);
    let Some(mut cursor) = rtdb_query::open_owned_path_catalog(context, catalog, None).await? else { return Ok(()) };
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        for row in 0..batch.num_rows() {
            let cell = |name| ArrowCell::named(batch, name, row);
            let text = |name| cell(name).and_then(ArrowCell::utf8).ok_or_else(|| malformed(name));
            let mut columns = cell("columns").and_then(ArrowCell::list).ok_or_else(|| malformed("columns"))?;
            let column = columns.next().and_then(|cell| cell.utf8()).ok_or_else(|| malformed("columns"))?;
            if columns.next().is_some() { return Err(malformed("columns")); }
            let schema = text("schema_name")?;
            let table = text("table_name")?;
            if !catalog.owns_path_target_parts(schema, table) { return Err(malformed("table_name")); }
            let recall = cell("recall").ok_or_else(|| malformed("recall"))?;
            let recall = catalog_recall(recall, false, Some(&memory))?;
            let dimensions = cell("dimensions").and_then(ArrowCell::int64)
                .and_then(|dimensions| usize::try_from(dimensions).ok()).filter(|dimensions| *dimensions > 0).ok_or_else(|| malformed("dimensions"))?;
            visit(OwnedRow {
                name: text("_id")?, schema, table, column, dimensions,
                approximate: Approximate::new(Method::Ivf, recall)?,
                generation: cell("version").and_then(ArrowCell::int64).ok_or_else(|| malformed("version"))?,
            })?;
        }
    }
    Ok(())
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

    fn sql_definition(batch: &RecordBatch, row: usize) -> Result<VectorDefinition> {
        sql_row(batch, row, |_| Ok(())).map(SqlRow::into_definition)
    }

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
        let row = sql_row(&batch, 0, |_| Ok(())).unwrap();
        assert!(matches!(&row.name, Cow::Borrowed("idx")));
        assert!(matches!(row.partition.as_slice(), [Cow::Borrowed("tenant")]));
        let first = row.into_definition();
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
        assert_eq!(catalog_recall(ArrowCell::named(&batch, "recall", 0).unwrap(), false, None), Err(malformed("recall")));
    }

    #[test]
    fn path_target_authority_validates_borrowed_namespace_and_ordinary_name() {
        let relation = rtdb_types::OwnedRelation::new("documents").unwrap();
        let target = relation.table("owner");
        let foreign = relation.table("other");
        let catalog = target.owned_vector_catalog().unwrap();
        assert!(catalog.owns_path_target_parts(&target.schema, &target.table));
        assert!(!catalog.owns_path_target_parts(&foreign.schema, &foreign.table));
        assert!(!catalog.owns_path_target_parts(&target.schema, &catalog.table));
        assert!(!catalog.owns_path_target_parts("public", &target.table));
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
