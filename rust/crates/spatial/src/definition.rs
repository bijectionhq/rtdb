use std::{collections::BTreeMap, sync::Arc};

use arrow_schema::DataType;
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation};
use rtdb_format::keys::spatial_posting_id;
use rtdb_index::Snapshot;
use rtdb_maintenance::ArtefactDefinition;
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryCursor, QueryOptions};
use rtdb_storage::cache::BufferPool;
use rtdb_types::{Anomaly, OwnedRelation, Result, SpatialGeometryEncoding, TableRef, Timestamp};
use tokio_util::sync::CancellationToken;

pub(crate) use rtdb_format::keys::SPATIAL_ARTEFACT_KIND as KIND;

pub(crate) struct Definition {
    pub(crate) name: String,
    pub(crate) table: TableRef,
    pub(crate) geometry: String,
    pub(crate) encoding: SpatialGeometryEncoding,
    pub(crate) partition: Vec<String>,
    pub(crate) generation: i64,
    pub(crate) reservation: Arc<MemoryReservation>,
}

impl Definition {
    pub(crate) fn artefact_id(&self) -> String {
        spatial_posting_id(&self.name)
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
    let native = TableRef::new("xt", rtdb_types::rtdb_v1::SPATIAL_BUILD_CATALOG);
    let has_native = snapshot.contains_table(&native);
    let tables: Vec<_> = snapshot
        .tables()
        .into_iter()
        .filter(TableRef::is_owned_spatial_catalog)
        .collect();
    if tables.is_empty() && !has_native {
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
            visit(cursor, Some(&table), &context.memory, |definition| {
                output.reserve_exact(1);
                output.push(definition);
            }).await?;
        }
    }
    if has_native && let Some(cursor) = rtdb_query::open_native_spatial_builds(Arc::clone(&context), None).await? {
        visit(cursor, None, &context.memory, |definition| {
            output.reserve_exact(1);
            output.push(definition);
        }).await?;
    }
    Ok(output)
}

pub(crate) async fn committed_native_definition(
    context: Arc<QueryContext>, table: &TableRef, name: &str,
) -> Result<Option<Definition>> {
    let _id = context.memory.reserve(table.schema.len() + table.table.len() + name.len() + 64)?;
    let id = rtdb_types::rtdb_v1::ranked_build_id(table, name);
    let Some(cursor) = rtdb_query::open_native_spatial_builds(Arc::clone(&context), Some(&id)).await? else { return Ok(None); };
    let (definition, duplicate) = select(cursor, None, &context.memory).await?;
    Ok(definition.filter(|definition| !duplicate && &definition.table == table
        && definition.name.strip_suffix("~native") == Some(name)))
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
    let cursor = rtdb_query::open_owned_path_catalog(Arc::clone(&context), &catalog, Some(path.name()))
        .await?
        .ok_or_else(absent)?;
    let (definition, duplicate) = select(cursor, Some(&catalog), &context.memory).await?;
    let definition = definition.ok_or_else(absent)?;
    if duplicate
        || definition.name != path.name()
        || definition.table != relation.table(owner)
    {
        return Err(absent());
    }
    Ok(definition)
}

pub fn path_geometry_encoding(cell: Option<ArrowCell<'_>>) -> Result<SpatialGeometryEncoding> {
    let Some(cell) = cell else { return Ok(SpatialGeometryEncoding::TaggedWkb); };
    let value = cell.physical()?;
    if value.field.data_type() == &DataType::Null || value.array.is_null(value.row) {
        return Ok(SpatialGeometryEncoding::TaggedWkb);
    }
    SpatialGeometryEncoding::parse(value.utf8().ok_or_else(malformed)?)
}

fn malformed() -> Anomaly {
    Anomaly::fault("rtdb.spatial/path-catalog", "The spatial path catalog contains an invalid declaration")
}

async fn select(
    cursor: QueryCursor, owned_catalog: Option<&TableRef>, memory: &Arc<MemoryBudget>,
) -> Result<(Option<Definition>, bool)> {
    let mut definition = None;
    let mut duplicate = false;
    visit(cursor, owned_catalog, memory, |candidate| {
        if definition.is_some() {
            duplicate = true;
        } else {
            definition = Some(candidate);
        }
    }).await?;
    Ok((definition, duplicate))
}

async fn visit(
    mut cursor: QueryCursor, owned_catalog: Option<&TableRef>, memory: &Arc<MemoryBudget>, mut accept: impl FnMut(Definition),
) -> Result<()> {
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        for row in 0..batch.num_rows() {
            let cell = |name| ArrowCell::named(batch, name, row);
            let text = |name| cell(name).and_then(ArrowCell::utf8).ok_or_else(malformed);
            let name = match owned_catalog {
                Some(_) => text("_id")?,
                None => text("index_name")?,
            };
            let schema = text("schema_name")?;
            let table = text("table_name")?;
            let columns = cell("columns")
                .and_then(ArrowCell::list)
                .ok_or_else(malformed)?;
            let generation = cell("version")
                .and_then(ArrowCell::int64)
                .ok_or_else(malformed)?;
            let (encoding, suffix) = match owned_catalog {
                Some(_) => (path_geometry_encoding(cell("geometry_encoding"))?, ""),
                None => (SpatialGeometryEncoding::parse(text("geometry_encoding")?)?, "~native"),
            };
            if !(1..=16).contains(&columns.len()) {
                return Err(malformed());
            }
            let base = 2 * size_of::<Definition>() + 256 + 2 * (schema.len() + table.len() + name.len() + suffix.len());
            let mut reservation = memory.reserve(base)?;
            let table = TableRef::new(schema, table);
            if owned_catalog.is_some_and(|catalog| !catalog.owns_path_target(&table))
                || (owned_catalog.is_none() && text("_id")? != rtdb_types::rtdb_v1::ranked_build_id(&table, name))
            {
                return Err(malformed());
            }
            let count = columns.len();
            let mut names = [""; 16];
            for (target, column) in names.iter_mut().zip(columns) {
                *target = column.utf8().filter(|name| owned_catalog.is_none() || !name.starts_with('_')).ok_or_else(malformed)?;
            }
            let partition = &names[1..count];
            let bytes = base.checked_add((count - 1) * size_of::<String>())
                .and_then(|bytes| names[..count].iter().try_fold(bytes, |sum, name| sum.checked_add(name.len())))
                .ok_or_else(crate::postings::work_limit)?;
            Arc::get_mut(&mut reservation).expect("catalog row owns its reservation").grow_to(bytes)?;
            accept(Definition {
                name: [name, suffix].concat(),
                table,
                geometry: names[0].to_owned(),
                encoding,
                partition: partition.iter().map(|name| (*name).to_owned()).collect(),
                generation,
                reservation,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn catalog_reads_both_legacy_and_explicit_geometry_encodings() {
    use arrow_array::{NullArray, StringArray};
    use arrow_schema::Field;

    let missing = Field::new("geometry_encoding", DataType::Null, true);
    let absent = NullArray::new(1);
    let text = Field::new("geometry_encoding", DataType::Utf8, true);
    let present = StringArray::from(vec![None, Some("bijection-geometry-v1")]);
    for cell in [
        None,
        Some(ArrowCell { field: &missing, array: &absent, row: 0 }),
        Some(ArrowCell { field: &text, array: &present, row: 0 }),
    ] {
        assert_eq!(path_geometry_encoding(cell).unwrap(), SpatialGeometryEncoding::TaggedWkb);
    }
    let explicit = ArrowCell { field: &text, array: &present, row: 1 };
    assert_eq!(path_geometry_encoding(Some(explicit)).unwrap(), SpatialGeometryEncoding::BijectionGeometry);
}
