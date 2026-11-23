use std::collections::BTreeMap;
use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_index::Snapshot;
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::cache::BufferPool;
use rtdb_types::{Anomaly, Instant, Result, TableRef, Value};
use tokio_util::sync::CancellationToken;

/// One version of a search index definition, as `xt.search_indexes` holds it (fts.allium: SearchIndexDefinition).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SearchDefinition { pub name: String, pub table: TableRef, pub columns: Vec<String>, pub version: i64, pub history: bool }

impl SearchDefinition {
    /// The artefact definition id names the analyzer and file format versions, so a change to either builds new search files rather than reading old ones.
    pub fn artefact_id(&self) -> String { format!("{}~a{}f{}", self.name, rtdb_text::ANALYZER_VERSION, crate::file::FORMAT_VERSION) }
}

pub const DEFINITIONS: (&str, &str) = ("xt", "search_indexes");

/// The definitions in force in the database's snapshots, as of `now` in valid time.
pub async fn definitions_in(database: &str, partitions: Vec<Snapshot>, storage: Arc<BufferPool>, memory: Arc<MemoryBudget>, now: Instant, cancellation: &CancellationToken) -> Result<Vec<SearchDefinition>> {
    let table = TableRef::new(DEFINITIONS.0, DEFINITIONS.1);
    if !partitions.iter().any(|snapshot| snapshot.contains_table(&table)) { return Ok(Vec::new()); }
    let catalog = CatalogSnapshot::new(database, BTreeMap::from([(database.to_string(), DatabaseSnapshot { partitions, storage })]), false)?;
    let context = QueryContext::new(catalog, Arc::clone(&memory), now, "UTC", QueryOptions::default(), cancellation)?;
    let sql = "SELECT _id, schema_name, table_name, columns, version, history FROM xt.search_indexes";
    let prepared = rtdb_sql::prepare_in(sql, &context.catalog, &rtdb_sql::PrepareOptions::default(), &memory)?;
    let mut cursor = PreparedQuery::from_prepared(prepared, &memory)?.execute(context, Vec::new()).await?;
    let mut definitions = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            let cell = |column: usize| read_value(schema.field(column), batch.column(column).as_ref(), row);
            let text = |value: Value| match value { Value::Utf8(text) => Ok(text), other => Err(malformed(&other)) };
            let columns = match cell(3)? { Value::List(values) => values.into_values().into_iter().map(text).collect::<Result<Vec<_>>>()?, other => return Err(malformed(&other)) };
            let version = match cell(4)? { Value::I64(version) => version, other => return Err(malformed(&other)) };
            // A definition written before the option existed covers history.
            let history = match cell(5)? { Value::Bool(history) => history, Value::Null => true, other => return Err(malformed(&other)) };
            definitions.push(SearchDefinition { name: text(cell(0)?)?, table: TableRef::new(text(cell(1)?)?, text(cell(2)?)?), columns, version, history });
        }
    }
    definitions.sort();
    Ok(definitions)
}

fn malformed(value: &Value) -> Anomaly { Anomaly::fault("rtdb.search/malformed-definition", format!("Malformed search index definition value: {value:?}")) }
