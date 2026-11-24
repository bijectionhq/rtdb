use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{BooleanArray, Int64Array};
use rtdb_columnar::{MemoryBudget, TextCell, from_field, selected_cell, text_cell, utf8_list};
use rtdb_index::Snapshot;
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::cache::BufferPool;
use rtdb_types::{Anomaly, Instant, LogicalType, Result, TableRef};
use tokio_util::sync::CancellationToken;

/// One version of a search index definition, as `xt.search_indexes` holds it (fts.allium: SearchIndexDefinition).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SearchDefinition {
    pub name: String, pub table: TableRef, pub columns: Vec<String>, pub analyzer: rtdb_text::Analyzer, pub version: i64, pub history: bool,
}

impl SearchDefinition {
    /// The artefact ID for the default paged writer and the definition's analyzer at its current version.
    pub fn artefact_id(&self) -> String { self.artefact_id_for(crate::file::FORMAT_VERSION) }

    pub(crate) fn artefact_id_for(&self, format: i32) -> String {
        format!("{}~{}.a{}p2f{}", self.name, self.analyzer, rtdb_text::ANALYZER_VERSION, format)
    }

    /// Whether the definition's files answer `query` on `column`: they hold the column's tokens under the query's analyzer.
    pub fn serves(&self, table: &TableRef, column: &str, analyzer: rtdb_text::Analyzer) -> bool {
        &self.table == table && self.analyzer == analyzer && self.columns.iter().any(|indexed| indexed == column)
    }
}

pub const DEFINITIONS: (&str, &str) = ("xt", "search_indexes");

/// A declared optional text column may have no physical text at this cut.
pub(crate) fn has_owned_search_text(snapshot: &Snapshot, table: &TableRef, column: &str) -> Result<bool> {
    let types = snapshot.column_types_for(table, [column.to_owned()]);
    let kind = types.get(column).unwrap_or(&LogicalType::Nothing);
    kind.owned_search_text_presence().ok_or_else(|| Anomaly::incorrect(
        "rtdb.owned/search-column", "Owned search path no longer describes only clear text",
    ))
}

/// The definitions in force in the database's snapshots, as of `now` in valid time.
pub async fn definitions_in(database: &str, partitions: Vec<Snapshot>, storage: Arc<BufferPool>, memory: Arc<MemoryBudget>, now: Instant, cancellation: &CancellationToken) -> Result<Vec<SearchDefinition>> {
    let table = TableRef::new(DEFINITIONS.0, DEFINITIONS.1);
    if !partitions.iter().any(|snapshot| snapshot.contains_table(&table)) { return Ok(Vec::new()); }
    let catalog = CatalogSnapshot::new(database, BTreeMap::from([(database.to_string(), DatabaseSnapshot { partitions, storage })]), false)?;
    let context = QueryContext::new(catalog, Arc::clone(&memory), now, "UTC", QueryOptions::default(), cancellation)?;
    let sql = "SELECT _id, schema_name, table_name, columns, version, history, analyzer FROM xt.search_indexes";
    read_public_definitions(context, sql, Vec::new()).await
}

pub(crate) async fn native_build_definitions(
    database: &str, snapshot: Snapshot, storage: &Arc<BufferPool>, memory: &Arc<MemoryBudget>,
    now: Instant, cancellation: &CancellationToken,
) -> Result<Vec<SearchDefinition>> {
    let table = TableRef::new("xt", rtdb_types::rtdb_v1::SEARCH_BUILD_CATALOG);
    if !snapshot.contains_table(&table) { return Ok(Vec::new()); }
    let catalog = CatalogSnapshot::new(database, BTreeMap::from([(
        database.to_owned(), DatabaseSnapshot { partitions: vec![snapshot], storage: Arc::clone(storage) },
    )]), false)?;
    let context = QueryContext::new(catalog, Arc::clone(memory), now, "UTC", QueryOptions::default(), cancellation)?;
    read_public_definitions(context, NATIVE_BUILD_DEFINITIONS, Vec::new()).await
}

const NATIVE_BUILD_DEFINITIONS: &str = "SELECT index_name || '~native' AS name, schema_name, table_name,
    columns, version, history, analyzer FROM xt.native_search_builds";

pub(crate) async fn committed_native_definition(
    context: Arc<QueryContext>, table: &TableRef, name: &str,
) -> Result<Option<SearchDefinition>> {
    let catalog = TableRef::new("xt", rtdb_types::rtdb_v1::SEARCH_BUILD_CATALOG);
    if !context.catalog.database(context.catalog.default_database())?.partitions.iter()
        .any(|snapshot| snapshot.contains_table(&catalog))
    {
        return Ok(None);
    }
    let id = rtdb_types::rtdb_v1::ranked_build_id(table, name);
    let sql = format!("{NATIVE_BUILD_DEFINITIONS} WHERE _id = $1");
    let mut definitions = read_public_definitions(context, &sql, vec![rtdb_types::Value::Utf8(id)]).await?;
    Ok(definitions.pop().filter(|definition| definitions.is_empty()
        && &definition.table == table && definition.name == format!("{name}~native")))
}

async fn read_public_definitions(context: Arc<QueryContext>, sql: &str, arguments: Vec<rtdb_types::Value>) -> Result<Vec<SearchDefinition>> {
    let memory = Arc::clone(&context.memory);
    let prepared = rtdb_sql::prepare_in(sql, &context.catalog, &rtdb_sql::PrepareOptions::default(), &memory)?;
    let cursor = PreparedQuery::from_prepared(prepared, &memory)?.execute(context, arguments).await?;
    read_definitions(cursor, None).await
}

/// Definitions used by FTS maintenance for one database partition's protected catalogs.
pub(crate) async fn owned_definitions_in(
    database: &str, snapshot: Snapshot, storage: &Arc<BufferPool>, memory: &Arc<MemoryBudget>,
    now: Instant, cancellation: &CancellationToken,
) -> Result<Vec<SearchDefinition>> {
    let tables: Vec<_> = snapshot.tables().into_iter().filter(TableRef::is_owned_search_catalog).collect();
    if tables.is_empty() { return Ok(Vec::new()); }
    let catalog = CatalogSnapshot::new(database, BTreeMap::from([(
        database.to_owned(), DatabaseSnapshot { partitions: vec![snapshot], storage: Arc::clone(storage) },
    )]), false)?;
    let context = QueryContext::new(catalog, Arc::clone(memory), now, "UTC", QueryOptions::default(), cancellation)?;
    let mut definitions = Vec::new();
    for table in tables {
        if let Some(cursor) = rtdb_query::open_owned_path_catalog(Arc::clone(&context), &table, None).await? {
            definitions.extend(read_definitions(cursor, Some(&table)).await?);
        }
    }
    definitions.sort();
    Ok(definitions)
}

pub(crate) async fn owned_definition(
    context: Arc<QueryContext>, owner: &str, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
) -> Result<Option<SearchDefinition>> {
    let catalog = rtdb_types::OwnedRelation::search_catalog(owner);
    let Some(cursor) = rtdb_query::open_owned_path_catalog(context, &catalog, Some(path.name())).await? else {
        return Ok(None);
    };
    let mut definitions = read_definitions(cursor, Some(&catalog)).await?.into_iter();
    let Some(definition) = definitions.next() else { return Ok(None); };
    if definition.name != path.name() || definitions.next().is_some() {
        return Err(Anomaly::fault("rtdb.search/owned-definition-identity", "Owned search definition did not resolve its full identity"));
    }
    Ok((definition.table == relation.table(owner)).then_some(definition))
}

async fn read_definitions(
    mut cursor: rtdb_query::QueryCursor, owned_catalog: Option<&TableRef>,
) -> Result<Vec<SearchDefinition>> {
    let mut definitions = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        let schema = batch.schema_ref();
        for row in 0..batch.num_rows() {
            let cell = |column: usize| (schema.field(column), batch.column(column).as_ref());
            let text = |column| {
                let (field, array) = cell(column);
                let logical = from_field(field)?;
                match text_cell(&logical, field, array, row)? {
                    TextCell::Utf8(text) => Ok(text),
                    _ => Err(malformed(field.name())),
                }
            };
            let (field, array) = cell(3);
            let columns = utf8_list(field, array, row)?.ok_or_else(|| malformed("columns"))?
                .map(|text| text.map(str::to_owned)).collect::<Result<Vec<_>>>()?;
            let (field, array) = cell(4);
            let (_, version, position) = selected_cell(field, array, row)?.ok_or_else(|| malformed("version"))?;
            let version = version.as_any().downcast_ref::<Int64Array>().ok_or_else(|| malformed("version"))?.value(position);
            let table = TableRef::new(text(1)?, text(2)?);
            let history = if let Some(catalog) = owned_catalog {
                if !catalog.owns_path_target(&table) || columns.len() != 1 {
                    return Err(Anomaly::fault("rtdb.search/malformed-owned-definition", "Owned search definition does not describe one column of a relation in its namespace"));
                }
                true
            } else {
                let (field, array) = cell(5);
                match selected_cell(field, array, row)? {
                    None => true,
                    Some((_, history, position)) => history.as_any().downcast_ref::<BooleanArray>()
                        .ok_or_else(|| malformed("history"))?.value(position),
                }
            };
            let analyzer = optional_analyzer(cell(if owned_catalog.is_some() { 5 } else { 6 }), row)?;
            definitions.push(SearchDefinition { name: text(0)?.to_owned(), table, columns, analyzer, version, history });
        }
    }
    definitions.sort();
    Ok(definitions)
}

/// A definition without an analyzer uses the simple one.
fn optional_analyzer((field, array): (&arrow_schema::Field, &dyn arrow_array::Array), row: usize) -> Result<rtdb_text::Analyzer> {
    match text_cell(&from_field(field)?, field, array, row)? {
        TextCell::Utf8(name) => rtdb_text::Analyzer::parse(name).ok_or_else(|| malformed("analyzer")),
        TextCell::Missing => Ok(rtdb_text::Analyzer::Simple),
        TextCell::Other => Err(malformed("analyzer")),
    }
}

fn malformed(column: &str) -> Anomaly {
    Anomaly::fault("rtdb.search/malformed-definition", format!("Malformed search index definition in {column}"))
}
