use std::sync::Arc;

use rtdb_plan::{Catalog, ColumnId, Expr, Field, QualifiedTable, Relation, Scan, SortExpr, TemporalFilter};
use rtdb_types::{Anomaly, AnomalyCategory, EntityId, Iid, Interval, OwnedRelation, Result, TableRef, TimeUnit, Timestamp, Value};

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
    if relation.is_world_catalog() {
        if !matches!(id, EntityId::Uuid(_)) { return Err(rtdb_types::OwnedWorld::unavailable()); }
        return open_owned_table_point(context, relation.table(owner), id.iid(), Some(id), valid, columns, false).await;
    }
    match owned_address(Arc::clone(&context), owner, relation, id).await? {
        OwnedAddress::Assigned(locator) => open_owned_table_point(context, relation.table(owner), locator, Some(id), valid, columns, false).await,
        OwnedAddress::Vacant(_) => Ok(None),
    }
}

/// Opens a point at a caller-certified system cut within this retained catalog.
pub async fn open_owned_point_at_system(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId,
    valid: Interval, system: Timestamp,
) -> Result<Option<QueryCursor>> {
    if relation.world().is_some() || relation.is_world_catalog() {
        return Err(address_unavailable());
    }
    let context = owned_context_at_system(context, system)?;
    open_owned_point(context, owner, relation, id, Some(valid), None).await
}

fn owned_context_at_system(context: Arc<QueryContext>, system: Timestamp) -> Result<Arc<QueryContext>> {
    let held = context.current_time.as_micros()?;
    let database = context.catalog.database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else { return Err(address_unavailable()); };
    if snapshot.system_time_cap() != Some(held) || system > held {
        return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/history-cut", "Owned history requires an exact retained system cut"));
    }
    if system == held {
        Ok(context)
    } else {
        context.with_owned_system_cut(system, &context.cancellation)
    }
}

/// Returns the held system point's winning rows across a clipped business-valid window.
pub async fn open_owned_valid_during(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId, valid: Interval,
) -> Result<Option<QueryCursor>> {
    let system = context.current_time.as_micros()?;
    open_owned_valid_during_at(context, owner, relation, id, valid, system).await
}

/// Reads a certified earlier system point without replacing the retained physical catalog.
pub async fn open_owned_valid_during_at(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId,
    valid: Interval, system: Timestamp,
) -> Result<Option<QueryCursor>> {
    if relation.world().is_some() || relation.is_world_catalog() || valid.is_empty() {
        return Err(Anomaly::incorrect("rtdb.owned/history-window", "Owned history requires a nonempty valid window on a root relation"));
    }
    let context = owned_context_at_system(context, system)?;
    let OwnedAddress::Assigned(locator) = owned_address(Arc::clone(&context), owner, relation, id).await? else {
        return Ok(None);
    };
    open_owned_table_point(context, relation.table(owner), locator, Some(id), Some(valid), None, true).await
}

/// Reads one full identity in a relation's private uniqueness table.
pub async fn open_owned_unique_point(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId, columns: &[&str],
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() {
        return Err(Anomaly::incorrect("rtdb.owned/no-owner", "Owned relations require an authenticated principal"));
    }
    open_owned_table_point(context, relation.unique_table(owner), id.iid(), Some(id), None, Some(columns), false).await
}

/// Reads one complete key from a protected capacity counter table.
pub async fn open_owned_capacity_point(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId, columns: &[&str],
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() {
        return Err(Anomaly::incorrect("rtdb.owned/no-owner", "Owned relations require an authenticated principal"));
    }
    open_owned_table_point(context, relation.capacity_table(owner), id.iid(), Some(id), None, Some(columns)).await
}

pub async fn open_owned_relation_schema(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() {
        return Err(Anomaly::incorrect("rtdb.owned/no-owner", "Owned relations require an authenticated principal"));
    }
    let id = EntityId::String(relation.name().into());
    open_owned_table_point(
        context, OwnedRelation::relation_catalog(owner), id.iid(), Some(&id), None, Some(&["schema"]), false,
    ).await
}

pub(crate) async fn open_owned_table_point(
    context: Arc<QueryContext>, table: TableRef, locator: Iid, id: Option<&EntityId>,
    valid: Option<Interval>, columns: Option<&[&str]>, history: bool,
) -> Result<Option<QueryCursor>> {
    let name = QualifiedTable {
        database: context.catalog.default_database().to_owned(),
        table,
    };
    let context = context.for_owned_relation(name.clone(), id.cloned());
    let Some(table) = context.catalog.table(&name)? else { return Ok(None); };
    let mut fields: Vec<&str> = columns.map_or_else(|| table.columns.keys().map(String::as_str).collect(), |names| names.to_vec());
    for required in ["_id", "_system_from"] {
        if !fields.contains(&required) { fields.push(required); }
    }
    if history {
        for required in ["_valid_from", "_valid_to"] {
            if !fields.contains(&required) { fields.push(required); }
        }
    }
    let columns: Vec<_> = fields.into_iter().enumerate().map(|(index, name)| Field {
        id: ColumnId(index as u32), name: name.into(), qualifier: None, qualified_only: false,
        unnamed: false, data_type: table.scan_column_type(name),
    }).collect();
    let order = columns.iter().find(|field| field.name == "_valid_from").map(Field::expression);
    let timestamp = |time: Timestamp| Expr::Literal(Value::Timestamp {
        value: time.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()),
    });
    let valid_time = valid.map_or(TemporalFilter::Default, |valid| TemporalFilter::FromTo {
        from: timestamp(valid.lower()), to: timestamp(valid.upper()),
    });
    let scan = Arc::new(Relation::scan(Scan {
        table, columns, valid_time, system_time: TemporalFilter::Default, clamp_valid_time: history,
        iids: Some([locator].into()), predicates: Vec::new(), matches: Vec::new(), top_k: None, ordered_top_k: None,
    })?);
    let plan = if history {
        let expr = order.ok_or_else(|| Anomaly::fault("rtdb.owned/history-schema", "Owned history omitted its valid start"))?;
        Arc::new(scan.sort(vec![SortExpr { expr, descending: false, nulls_first: false }])?)
    } else {
        scan
    };
    let query = PreparedQuery::new_in(plan, &context.memory)?;
    query.execute(context, Vec::new()).await.map(Some)
}

/// The found permanent assignment, or a slot the ordered resolver can reserve.
pub enum OwnedAddress {
    Assigned(Iid),
    Vacant(Iid),
}

/// Rejects old owned storage if its complete address registry was never established.
pub fn require_owned_address_coverage(context: &QueryContext, owner: &str, relation: &OwnedRelation) -> Result<bool> {
    if owner.is_empty() || relation.is_world_catalog() { return Err(address_unavailable()); }
    let database = context.catalog.database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else { return Err(address_unavailable()); };
    if snapshot.contains_table(&relation.address_table(owner)) { return Ok(true); }
    let table = relation.table(owner);
    if snapshot.iter_segments(&table).any(|segment| segment.row_count() != 0)
        || !snapshot.trie_entries(&table).is_empty()
        || snapshot.table_catalog().metadata(&table).is_some_and(|metadata| metadata.row_count != 0)
    {
        return Err(address_unavailable());
    }
    Ok(false)
}

/// Probes only the held snapshot's protected registry, using the complete typed key.
pub async fn owned_address(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId,
) -> Result<OwnedAddress> {
    if !require_owned_address_coverage(&context, owner, relation)? {
        return Ok(OwnedAddress::Vacant(id.iid()));
    }
    let registry = relation.address_table(owner);
    let table = relation.table(owner);
    for locator in id.owned_locator_candidates() {
        context.check_cancelled()?;
        let slot = EntityId::Uuid(locator.0);
        let columns = ["address"];
        let cursor = open_owned_table_point(Arc::clone(&context), registry.clone(), locator, Some(&slot), None, Some(&columns), false).await?;
        let Some(occupant) = address_row(cursor, &slot, &context).await? else {
            // A vacant registry slot may never cover an unregistered data row.
            let cursor = open_owned_table_point(Arc::clone(&context), table, locator, None, Some(Interval::ALL), Some(&["_id"]), false).await?;
            if let Some(mut cursor) = cursor {
                while let Some(batch) = cursor.next_batch().await? {
                    if batch.batch().num_rows() != 0 { return Err(address_unavailable()); }
                }
            }
            return Ok(OwnedAddress::Vacant(locator));
        };
        if &occupant == id {
            return Ok(OwnedAddress::Assigned(locator));
        }
        if !occupant.owned_locator_candidates().any(|candidate| candidate == locator) {
            return Err(address_unavailable());
        }
    }
    Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/address-exhausted", "No owned address locator is available"))
}

async fn address_row(
    cursor: Option<QueryCursor>, slot: &EntityId, context: &QueryContext,
) -> Result<Option<EntityId>> {
    let Some(mut cursor) = cursor else { return Ok(None); };
    let mut occupant = None;
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        if batch.num_rows() == 0 { continue; }
        if occupant.is_some() || batch.num_rows() != 1 { return Err(address_unavailable()); }
        owned_row_revision(batch, 0, slot, &context.memory)?;
        let (position, field) = batch.schema_ref().column_with_name("address").ok_or_else(address_unavailable)?;
        let column = batch.column(position).as_ref();
        let bytes = rtdb_columnar::estimate_read_value_bytes(field, column, 0)?;
        let _scratch = context.memory.reserve(bytes.saturating_add(4096))?;
        occupant = Some(rtdb_columnar::read_value(field, column, 0)?.entity_id().map_err(|_| address_unavailable())?);
    }
    Ok(occupant)
}

fn address_unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/address-uncovered", "The owned relation has no complete address registry for this cut")
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
    let names = ["_id", "capture", "manifest", "basis_id", "basis_time", "until", "schemas", "dropped"];
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

pub struct OwnedChangeEntry {
    pub system_time: Timestamp,
    pub through_events: i64,
    pub value: Value,
    pub retained: Arc<rtdb_columnar::MemoryReservation>,
}

pub async fn read_owned_fixed_range_rule(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
) -> Result<Option<rtdb_types::FixedRangeRule>> {
    let id = EntityId::String("rule".into());
    let memory = Arc::clone(&context.memory);
    let Some(mut cursor) = open_owned_table_point(
        context, relation.fixed_range_table(owner), id.iid(), Some(&id), None, Some(&["fixed_rule"]), false,
    ).await? else { return Ok(None); };
    let mut rule = None;
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        for row in 0..batch.num_rows() {
            if rule.is_some() { return Err(rtdb_types::change_feed_uncovered()); }
            let (column, field) = batch.schema_ref().column_with_name("fixed_rule")
                .ok_or_else(rtdb_types::change_feed_uncovered)?;
            let values = batch.column(column).as_ref();
            let bytes = rtdb_columnar::estimate_read_value_bytes(field, values, row)?;
            let _scratch = memory.reserve(bytes.checked_mul(2).and_then(|n| n.checked_add(1024))
                .ok_or_else(rtdb_types::change_feed_uncovered)?)?;
            rule = Some(rtdb_types::FixedRangeRule::from_value(&rtdb_columnar::read_value(field, values, row)?)?);
        }
    }
    Ok(rule)
}

pub async fn read_owned_change_entry(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, revision: i64,
) -> Result<OwnedChangeEntry> {
    read_owned_journal_cell(context, owner, relation, revision, "entry").await
}

pub async fn read_owned_fixed_range_changes(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, revision: i64,
) -> Result<OwnedChangeEntry> {
    read_owned_journal_cell(context, owner, relation, revision, "fixed_keys").await
}

async fn read_owned_journal_cell(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, revision: i64, cell: &str,
) -> Result<OwnedChangeEntry> {
    use rtdb_types::change_feed_uncovered;

    let id = EntityId::Integer(revision);
    let memory = Arc::clone(&context.memory);
    let journal = relation.change_journal(owner)?;
    let Some(mut cursor) = open_owned_table_point(context, journal, id.iid(), Some(&id), None, Some(&[cell, "through_events"]), false).await? else {
        return Err(change_feed_uncovered());
    };
    let mut entry = None;
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        for row in 0..batch.num_rows() {
            if entry.is_some() { return Err(change_feed_uncovered()); }
            let system_time = owned_row_revision(batch, row, &id, &memory)?;
            let (frontier_column, frontier_field) = batch.schema_ref().column_with_name("through_events")
                .ok_or_else(change_feed_uncovered)?;
            let Value::I64(through) = rtdb_columnar::read_value(frontier_field, batch.column(frontier_column).as_ref(), row)?
                else { return Err(change_feed_uncovered()); };
            if through < 0 { return Err(change_feed_uncovered()); }
            let (index, field) = batch.schema_ref().column_with_name(cell).ok_or_else(change_feed_uncovered)?;
            let values = batch.column(index).as_ref();
            let size = rtdb_columnar::estimate_read_value_bytes(field, values, row)?;
            let retained = memory.reserve(size.checked_mul(2).and_then(|bytes| bytes.checked_add(4096))
                .ok_or_else(change_feed_uncovered)?)?;
            entry = Some(OwnedChangeEntry {
                system_time, through_events: through, value: rtdb_columnar::read_value(field, values, row)?, retained,
            });
        }
    }
    entry.ok_or_else(change_feed_uncovered)
}
