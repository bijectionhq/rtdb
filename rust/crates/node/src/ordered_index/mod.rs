mod accelerate;
mod checkpoint;
mod domain;
mod key;
mod maintenance;
mod projection;
mod seek;
mod tree;

use std::{collections::BTreeMap, sync::Arc};

use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_index::Snapshot;
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::{ObjectKey, cache::BufferPool};
use rtdb_types::{Anomaly, Instant, Result, TableRef, Value};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

pub(crate) use accelerate::OrderedIndexAccelerator;
pub(crate) use maintenance::OrderedIndexMaintenance;

use rtdb_format::keys::ORDERED_INDEX_ROOT as INDEX_ROOT;
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
    if !snapshot.contains_table(&TableRef::new(CATALOG.0, CATALOG.1)) {
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
    let sql = "SELECT _id, schema_name, table_name, columns, version FROM xt.ordered_indexes";
    let prepared = rtdb_sql::prepare_in(
        sql,
        &context.catalog,
        &rtdb_sql::PrepareOptions::default(),
        memory,
    )?;
    let mut cursor = PreparedQuery::from_prepared(prepared, memory)?
        .execute(context, Vec::new())
        .await?;
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
                columns: columns.into_values().into_iter().map(text).collect::<Result<_>>()?,
                version,
            });
        }
    }
    definitions.sort();
    Ok(definitions)
}

fn format_error(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::fault("rtdb.ordered-index/format", error.to_string())
}
