use std::{collections::BTreeMap, sync::Arc};

use rtdb_columnar::{ArrowCell, MemoryBudget};
use rtdb_index::Snapshot;
use rtdb_maintenance::ArtefactDefinition;
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryCursor, QueryOptions};
use rtdb_storage::cache::BufferPool;
use rtdb_types::{Anomaly, OwnedRelation, Result, TableRef, Timestamp};
use tokio_util::sync::CancellationToken;

pub(crate) const KIND: &str = "spatial";
const FILE_VERSION: u32 = 2;

pub(crate) struct Definition {
    pub(crate) name: String,
    pub(crate) table: TableRef,
    pub(crate) geometry: String,
    pub(crate) partition: Vec<String>,
    pub(crate) generation: i64,
}

impl Definition {
    pub(crate) fn artefact_id(&self) -> String {
        format!("{}~f{}", self.name, FILE_VERSION)
    }

    pub(crate) fn artefact(&self) -> ArtefactDefinition {
        ArtefactDefinition {
            table: self.table.clone(),
            kind: KIND.into(),
            id: self.artefact_id(),
            version: self.generation,
            historical: true,
            group_bytes: None,
            packed: false,
        }
    }
}

pub(crate) async fn definitions(
    database: &str,
    snapshot: Snapshot,
    storage: Arc<BufferPool>,
    memory: Arc<MemoryBudget>,
    now: Timestamp,
    cancellation: &CancellationToken,
) -> Result<Vec<Definition>> {
    let tables: Vec<_> = snapshot
        .tables()
        .into_iter()
        .filter(TableRef::is_owned_spatial_catalog)
        .collect();
    if tables.is_empty() {
        return Ok(Vec::new());
    }
    let catalog = CatalogSnapshot::new(
        database,
        BTreeMap::from([(
            database.to_owned(),
            DatabaseSnapshot {
                partitions: vec![snapshot],
                storage,
            },
        )]),
        false,
    )?;
    let context = QueryContext::new(
        catalog,
        memory,
        now,
        "UTC",
        QueryOptions::default(),
        cancellation,
    )?;
    let mut output = Vec::new();
    for table in tables {
        if let Some(cursor) =
            rtdb_query::open_owned_path_catalog(Arc::clone(&context), &table, None).await?
        {
            output.extend(collect(cursor, &table).await?);
        }
    }
    Ok(output)
}

pub(crate) async fn held_definition(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    path: &OwnedRelation,
) -> Result<Definition> {
    let catalog = OwnedRelation::spatial_catalog(owner);
    let absent = || {
        Anomaly::incorrect(
            "rtdb.owned/spatial-path",
            "Owned relation has no such spatial path at the held cut",
        )
    };
    let cursor = rtdb_query::open_owned_path_catalog(context, &catalog, Some(path.name()))
        .await?
        .ok_or_else(absent)?;
    let mut definitions = collect(cursor, &catalog).await?;
    let definition = definitions.pop().ok_or_else(absent)?;
    if !definitions.is_empty()
        || definition.name != path.name()
        || definition.table != relation.table(owner)
    {
        return Err(absent());
    }
    Ok(definition)
}

async fn collect(mut cursor: QueryCursor, catalog: &TableRef) -> Result<Vec<Definition>> {
    let malformed = || {
        Anomaly::fault(
            "rtdb.spatial/path-catalog",
            "The spatial path catalog contains an invalid declaration",
        )
    };
    let mut output = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        for row in 0..batch.num_rows() {
            let cell = |name| ArrowCell::named(batch, name, row);
            let text = |name| cell(name).and_then(ArrowCell::utf8).ok_or_else(malformed);
            let name = text("_id")?;
            let schema = text("schema_name")?;
            let table = text("table_name")?;
            let columns = cell("columns")
                .and_then(ArrowCell::list)
                .ok_or_else(malformed)?;
            let generation = cell("version")
                .and_then(ArrowCell::int64)
                .ok_or_else(malformed)?;
            let table = TableRef::new(schema, table);
            if !(1..=16).contains(&columns.len()) || !catalog.owns_path_target(&table) {
                return Err(malformed());
            }
            let mut columns = columns.map(|column| {
                column
                    .utf8()
                    .filter(|name| !name.starts_with('_'))
                    .map(str::to_owned)
                    .ok_or_else(malformed)
            });
            let geometry = columns.next().ok_or_else(malformed)??;
            let partition = columns.collect::<Result<_>>()?;
            output.push(Definition {
                name: name.to_owned(),
                table,
                geometry,
                partition,
                generation,
            });
        }
    }
    Ok(output)
}
