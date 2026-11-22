use std::sync::Arc;

use rtdb_plan::{Catalog, ColumnId, Expr, Field, QualifiedTable, Relation, Scan, TemporalFilter};
use rtdb_types::{Anomaly, EntityId, Interval, OwnedRelation, Result, TableRef, TimeUnit, Timestamp, Value};

use crate::{PreparedQuery, QueryContext, QueryCursor};

/// Opens a single admitted IID read without granting SQL access to the owned namespace.
pub async fn open_owned_point(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    id: &EntityId,
    valid: Option<Interval>,
    columns: Option<&[&str]>,
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() {
        return Err(Anomaly::incorrect("rtdb.owned/no-owner", "Owned relations require an authenticated principal"));
    }
    open_private_point(context, relation.table(owner), id, valid, columns).await
}

pub async fn open_owned_relation_schema(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() {
        return Err(Anomaly::incorrect("rtdb.owned/no-owner", "Owned relations require an authenticated principal"));
    }
    open_private_point(
        context, OwnedRelation::relation_catalog(owner), &EntityId::String(relation.name().into()),
        None, Some(&["schema"]),
    ).await
}

async fn open_private_point(
    context: Arc<QueryContext>, table: TableRef, id: &EntityId,
    valid: Option<Interval>, columns: Option<&[&str]>,
) -> Result<Option<QueryCursor>> {
    let name = QualifiedTable {
        database: context.catalog.default_database().to_owned(),
        table,
    };
    let context = context.for_owned_relation(name.clone(), Some(id.clone()));
    let Some(table) = context.catalog.table(&name)? else { return Ok(None); };
    let mut fields: Vec<&str> = columns.map_or_else(|| table.columns.keys().map(String::as_str).collect(), |names| names.to_vec());
    for required in ["_id", "_system_from"] {
        if !fields.contains(&required) { fields.push(required); }
    }
    let columns = fields.into_iter().enumerate().map(|(index, name)| Field {
        id: ColumnId(index as u32), name: name.into(), qualifier: None, qualified_only: false,
        unnamed: false, data_type: table.scan_column_type(name),
    }).collect();
    let timestamp = |time: Timestamp| Expr::Literal(Value::Timestamp {
        value: time.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()),
    });
    let valid_time = valid.map_or(TemporalFilter::Default, |valid| TemporalFilter::FromTo {
        from: timestamp(valid.lower()), to: timestamp(valid.upper()),
    });
    let scan = Arc::new(Relation::scan(Scan {
        table, columns, valid_time, system_time: TemporalFilter::Default, clamp_valid_time: false,
        iids: Some([id.iid()].into()), predicates: Vec::new(), matches: Vec::new(), top_k: None, ordered_top_k: None,
    })?);
    let query = PreparedQuery::new_in(scan, &context.memory)?;
    query.execute(context, Vec::new()).await.map(Some)
}

/// Interprets the winning revision only after checking the complete stored identity.
pub fn owned_row_revision(
    batch: &arrow_array::RecordBatch,
    row: usize,
    expected_id: &rtdb_types::EntityId,
    memory: &Arc<rtdb_columnar::MemoryBudget>,
) -> Result<Timestamp> {
    let schema = batch.schema_ref();
    let (id_column, id_field) = schema.column_with_name("_id")
        .ok_or_else(|| Anomaly::fault("rtdb.owned/row-identity", "Owned read omitted its identity column"))?;
    let id = batch.column(id_column).as_ref();
    let bound = rtdb_columnar::estimate_read_value_bytes(id_field, id, row)?;
    let _scratch = memory.reserve(bound.checked_add(4096)
        .ok_or_else(|| Anomaly::fault("rtdb.owned/row-identity", "Owned identity allocation overflow"))?)?;
    if &rtdb_columnar::read_value(id_field, id, row)?.entity_id()? != expected_id {
        return Err(Anomaly::new(rtdb_types::AnomalyCategory::Conflict, "rtdb.owned/iid-collision", "Owned row locator resolves to a different complete identity"));
    }
    let (revision_column, revision_field) = schema.column_with_name("_system_from")
        .ok_or_else(|| Anomaly::fault("rtdb.owned/row-revision", "Owned read omitted its revision column"))?;
    match rtdb_columnar::read_value(revision_field, batch.column(revision_column).as_ref(), row)? {
        Value::Timestamp { value, unit: TimeUnit::Microsecond, .. } => Ok(Timestamp(value)),
        _ => Err(Anomaly::fault("rtdb.owned/row-revision", "Owned row revision is not a microsecond timestamp")),
    }
}

/// Reads the fixed protected path catalog shape for engine maintenance and resolution.
pub async fn open_owned_path_catalog(
    context: Arc<QueryContext>,
    catalog: &TableRef,
    path: Option<&str>,
) -> Result<Option<QueryCursor>> {
    if !catalog.is_owned_path_catalog() && !catalog.is_owned_search_catalog() {
        return Err(Anomaly::incorrect("rtdb.owned/path-catalog", "Expected an owned path catalog"));
    }
    open_private_catalog(
        context, catalog.clone(), &["_id", "schema_name", "table_name", "columns", "version"],
        path.map(|name| EntityId::String(name.into()).iid()),
    ).await
}

async fn open_private_catalog(
    context: Arc<QueryContext>, catalog: TableRef, fields: &[&str], iid: Option<rtdb_types::Iid>,
) -> Result<Option<QueryCursor>> {
    let name = QualifiedTable { database: context.catalog.default_database().to_owned(), table: catalog };
    let context = context.for_owned_relation(name.clone(), None);
    let Some(table) = context.catalog.table(&name)? else { return Ok(None); };
    let columns = fields.iter().copied().enumerate().map(|(index, name)| Field {
            id: ColumnId(index as u32), name: name.into(), qualifier: None, qualified_only: false,
            unnamed: false, data_type: table.scan_column_type(name),
        }).collect();
    let scan = Arc::new(Relation::scan(Scan {
        table, columns, valid_time: TemporalFilter::Default, system_time: TemporalFilter::Default,
        clamp_valid_time: false, iids: iid.map(|iid| [iid].into()),
        predicates: Vec::new(), matches: Vec::new(), top_k: None, ordered_top_k: None,
    })?);
    let query = PreparedQuery::new_in(scan, &context.memory)?;
    query.execute(context, Vec::new()).await.map(Some)
}

pub async fn read_owned_world(
    context: Arc<QueryContext>, owner: &str, world: rtdb_types::Iid,
) -> Result<Option<(rtdb_types::OwnedWorld, Arc<rtdb_columnar::MemoryReservation>)>> {
    use rtdb_types::{OwnedWorld, StructFields};

    let memory = Arc::clone(&context.memory);
    let id = EntityId::Uuid(world.0);
    let names = ["_id", "capture", "manifest", "basis_id", "basis_time", "until", "schemas"];
    let Some(mut cursor) = open_owned_point(
        context, owner, &OwnedRelation::world_catalog(), &id, None, Some(&names),
    ).await? else { return Ok(None); };
    let mut record = None;
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        for row in 0..batch.num_rows() {
            if record.is_some() { return Err(OwnedWorld::unavailable()); }
            let mut fields = StructFields::new();
            let mut reservations = Vec::new();
            for name in names {
                let (index, field) = batch.schema_ref().column_with_name(name).ok_or_else(OwnedWorld::unavailable)?;
                let values = batch.column(index).as_ref();
                let size = rtdb_columnar::estimate_read_value_bytes(field, values, row)?;
                reservations.push(memory.reserve(size)?);
                fields.insert(name.into(), rtdb_columnar::read_value(field, values, row)?);
            }
            let schema_bytes = match fields.get("schemas") {
                Some(Value::Binary(bytes)) if bytes.len() <= rtdb_types::WorldSchemas::MAX_BYTES => bytes.len(),
                _ => return Err(OwnedWorld::unavailable()),
            };
            let retained = memory.reserve(schema_bytes * 32 + 4096)?;
            let decoded = OwnedWorld::from_record(&fields)?;
            if decoded.id != world { return Err(OwnedWorld::unavailable()); }
            record = Some((decoded, retained));
        }
    }
    Ok(record)
}

pub async fn read_owned_parent_schemas(
    context: Arc<QueryContext>, owner: &str,
) -> Result<(rtdb_types::WorldSchemas, Arc<rtdb_columnar::MemoryReservation>)> {
    use std::collections::BTreeMap;
    use rtdb_types::{OwnedSchema, OwnedWorld, WorldSchemas};

    if owner.is_empty() { return Err(OwnedWorld::unavailable()); }
    let memory = Arc::clone(&context.memory);
    let mut charge = memory.reserve(4096)?;
    let mut bytes = 0usize;
    let mut fields = BTreeMap::new();
    if let Some(mut rows) = open_private_catalog(
        context, OwnedRelation::relation_catalog(owner), &["_id", "schema"], None,
    ).await? {
        while let Some(batch) = rows.next_batch().await? {
            let batch = batch.batch();
            let (id_index, id_field) = batch.schema_ref().column_with_name("_id").ok_or_else(OwnedWorld::unavailable)?;
            let (schema_index, schema_field) = batch.schema_ref().column_with_name("schema").ok_or_else(OwnedWorld::unavailable)?;
            for row in 0..batch.num_rows() {
                let id_col = batch.column(id_index).as_ref();
                let schema_col = batch.column(schema_index).as_ref();
                let size = rtdb_columnar::estimate_read_value_bytes(id_field, id_col, row)?
                    .checked_add(rtdb_columnar::estimate_read_value_bytes(schema_field, schema_col, row)?)
                    .ok_or_else(OwnedWorld::unavailable)?;
                bytes = bytes.checked_add(size).filter(|n| *n <= WorldSchemas::MAX_BYTES)
                    .ok_or_else(OwnedWorld::unavailable)?;
                Arc::get_mut(&mut charge).expect("world schema admission owns the reservation")
                    .grow_to(bytes * 32 + 4096)?;
                let Value::Utf8(relation) = rtdb_columnar::read_value(id_field, id_col, row)? else { return Err(OwnedWorld::unavailable()); };
                let Value::Binary(schema) = rtdb_columnar::read_value(schema_field, schema_col, row)? else { return Err(OwnedWorld::unavailable()); };
                if fields.insert(relation, OwnedSchema::from_json(&schema)?).is_some() { return Err(OwnedWorld::unavailable()); }
            }
        }
    }
    Ok((WorldSchemas::new(fields)?, charge))
}
