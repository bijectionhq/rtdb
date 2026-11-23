use std::sync::Arc;

use rtdb_plan::{BinaryOp, Catalog, ColumnId, Expr, Field, QualifiedTable, Relation, Scan, SortExpr, TemporalFilter};
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
    if relation.is_world_catalog() && !matches!(id, EntityId::Uuid(_)) {
        return Err(rtdb_types::OwnedWorld::unavailable());
    }
    open_owned_table_point(context, relation.table(owner), id.iid(), Some(id), valid, columns, false).await
}

/// Read the winning revision or exact absence for an owned point.
pub async fn read_owned_point_revision(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    id: &EntityId,
    valid: Interval,
) -> Result<Option<Timestamp>> {
    let columns = ["_id", "_system_from"];
    let Some(mut cursor) = open_owned_point(
        Arc::clone(&context), owner, relation, id, Some(valid), Some(&columns),
    ).await? else {
        return Ok(None);
    };
    let mut latest = None;
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() {
            let revision = owned_row_revision(batch.batch(), row, id, &context.memory)?;
            latest = Some(latest.map_or(revision, |previous: Timestamp| previous.max(revision)));
        }
    }
    Ok(latest)
}

/// Project the original validity endpoints along with a live lifetime point.
pub async fn open_owned_lifetime_point(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId, valid: Interval,
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() || !relation.is_root_data() || valid.is_empty() { return Err(address_unavailable()); }
    open_owned_table_point_with_edges(context, relation.table(owner), id.iid(), Some(id), Some(valid), None, false, true).await
}

/// Revalidates an immutable artefact's nominated row with its complete key.
/// The caller supplies the locator from a covered artefact, and still has to
/// compare the returned row's full identity and source revision before use.
#[doc(hidden)]
pub async fn open_owned_artefact_candidate(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, key: &EntityId, locator: Iid,
    valid: Interval, columns: &[&str],
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() || relation.world().is_some() || relation.is_world_catalog() || valid.is_empty() {
        return Err(address_unavailable());
    }
    open_owned_table_point(context, relation.table(owner), locator, Some(key), Some(valid), Some(columns), false).await
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

/// Reads a certified earlier system point without replacing the retained physical catalog.
pub async fn open_owned_valid_during_at(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId,
    valid: Interval, system: Timestamp,
) -> Result<Option<QueryCursor>> {
    if relation.world().is_some() || relation.is_world_catalog() || valid.is_empty() {
        return Err(Anomaly::incorrect("rtdb.owned/history-window", "Owned history requires a nonempty valid window on a root relation"));
    }
    let context = owned_context_at_system(context, system)?;
    if owner.is_empty() { return Err(address_unavailable()); }
    open_owned_table_point(context, relation.table(owner), id.iid(), Some(id), Some(valid), None, true).await
}

/// Opens the winning valid boundaries, including local world tombstones.
pub async fn open_owned_valid_boundaries(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId,
    valid: Interval, system: Timestamp,
) -> Result<Option<QueryCursor>> {
    if relation.is_world_catalog() || valid.is_empty() {
        return Err(Anomaly::incorrect("rtdb.owned/history-window", "Invalid owned valid window"));
    }
    let context = owned_context_at_system(context, system)?;
    if owner.is_empty() { return Err(address_unavailable()); }
    open_owned_table_point(context, relation.table(owner), id.iid(), Some(id), Some(valid), Some(&["_id"]), true).await
}

/// Streams the projected members of an owned Root at the declaring transaction's cut.
pub async fn open_owned_constraint_projection(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
    valid: Timestamp, columns: &[&str],
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() || !relation.is_root_data() || columns.is_empty() {
        return Err(address_unavailable());
    }
    let name = QualifiedTable {
        database: context.catalog.default_database().to_owned(),
        table: relation.table(owner),
    };
    let context = context.for_owned_relation(name.clone(), None);
    let Some(table) = context.catalog.table(&name)? else { return Ok(None); };
    let columns = columns.iter().enumerate().map(|(index, name)| Field {
        id: ColumnId(index as u32), name: (*name).into(), qualifier: None, qualified_only: false,
        unnamed: false, data_type: table.scan_column_type(name),
    }).collect();
    let scan = Arc::new(Relation::scan(Scan {
        table, columns,
        valid_time: TemporalFilter::At(Expr::Literal(Value::Timestamp {
            value: valid.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()),
        })),
        system_time: TemporalFilter::Default, clamp_valid_time: false,
        iids: None, predicates: Vec::new(), matches: Vec::new(), top_k: None, ordered_top_k: None,
    })?);
    PreparedQuery::new_in(scan, &context.memory)?.execute(context, Vec::new()).await.map(Some)
}

/// Opens a bounded page of visible identities in complete UTF-8 key order.
pub async fn open_owned_identity_page(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, system: Timestamp,
    valid: Timestamp, after: Option<&str>, fetch: i64,
) -> Result<Option<QueryCursor>> {
    if relation.world().is_some() || relation.is_world_catalog() {
        return Err(address_unavailable());
    }
    open_owned_identity_candidates(context, owner, relation, system, valid, after, None, fetch).await
}

/// Read complete keys only within the installed protected carrier prefix.
pub async fn open_owned_identity_prefix(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
    at: Timestamp, after: Option<&str>, prefix: &str, fetch: i64,
) -> Result<Option<QueryCursor>> {
    if relation.world().is_some() || relation.is_world_catalog() || prefix.is_empty()
        || !prefix.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || after.is_some_and(|id| !id.starts_with(prefix))
    {
        return Err(address_unavailable());
    }
    open_owned_identity_candidates(context, owner, relation, at, at, after, Some(prefix), fetch).await
}

async fn open_owned_identity_candidates(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, system: Timestamp,
    valid: Timestamp, after: Option<&str>, prefix: Option<&str>, fetch: i64,
) -> Result<Option<QueryCursor>> {
    let context = owned_context_at_system(context, system)?;
    if owner.is_empty() { return Err(address_unavailable()); }
    let name = QualifiedTable {
        database: context.catalog.default_database().to_owned(),
        table: relation.table(owner),
    };
    let context = context.for_owned_relation(name.clone(), None);
    let Some(table) = context.catalog.table(&name)? else { return Ok(None); };
    let id = Field {
        id: ColumnId(0), name: "_id".into(), qualifier: None, qualified_only: false,
        unnamed: false, data_type: table.scan_column_type("_id"),
    };
    let order = id.expression();
    let mut predicates: Vec<_> = after.map(|after| Expr::binary(
        BinaryOp::Gt, order.clone(), Expr::Literal(Value::Utf8(after.into())),
    )).into_iter().collect();
    if let Some(prefix) = prefix {
        let mut upper = prefix.to_owned();
        let last = upper.pop().ok_or_else(address_unavailable)?;
        upper.push(char::from(last as u8 + 1));
        predicates.extend([
            Expr::binary(BinaryOp::GtEq, order.clone(), Expr::Literal(Value::Utf8(prefix.into()))),
            Expr::binary(BinaryOp::Lt, order.clone(), Expr::Literal(Value::Utf8(upper))),
        ]);
    }
    let scan = Arc::new(Relation::scan(Scan {
        table,
        columns: vec![id],
        valid_time: TemporalFilter::At(Expr::Literal(Value::Timestamp {
            value: valid.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()),
        })),
        system_time: TemporalFilter::Default,
        clamp_valid_time: false,
        iids: None, predicates, matches: Vec::new(), top_k: None, ordered_top_k: None,
    })?);
    let sorted = Arc::new(scan.sort(vec![SortExpr {
        expr: order, descending: false, nulls_first: false,
    }])?);
    let page = Arc::new(sorted.limit(None, Some(Expr::Literal(Value::I64(fetch))))?);
    PreparedQuery::new_in(page, &context.memory)?.execute(context, Vec::new()).await.map(Some)
}

/// A bounded page of ordered UTF-8 keys with retained memory ownership.
pub struct OwnedIdentityCandidates {
    pub keys: Vec<String>,
    _charges: Vec<Arc<rtdb_columnar::MemoryReservation>>,
}

impl OwnedIdentityCandidates {
    pub fn into_parts(self) -> (Vec<String>, Vec<Arc<rtdb_columnar::MemoryReservation>>) {
        (self.keys, self._charges)
    }
}

/// Reads root identities or local shadow candidates in one exact native cut.
pub async fn owned_identity_candidates(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, system: Timestamp,
    valid: Timestamp, after: Option<&str>, fetch: usize,
) -> Result<OwnedIdentityCandidates> {
    let unavailable = || Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/world-identities",
        "The held world identity candidates exceed their exact coverage or byte bound");
    if !(1..=129).contains(&fetch) || relation.is_world_catalog() { return Err(unavailable()); }
    let context = owned_context_at_system(context, system)?;
    let mut charges = vec![context.memory.reserve(fetch * 128)?];
    let mut keys: Vec<String> = Vec::with_capacity(fetch);
    let mut remaining: usize = 8 << 20;
    if let Some(mut cursor) = open_owned_identity_candidates(
        Arc::clone(&context), owner, relation, system, valid, after, None, fetch as i64,
    ).await? {
        while let Some(batch) = cursor.next_batch().await? {
            let batch = batch.batch();
            let (column, field) = batch.schema_ref().column_with_name("_id").ok_or_else(unavailable)?;
            let array = batch.column(column).as_ref();
            for row in 0..batch.num_rows() {
                let size = rtdb_columnar::estimate_read_value_bytes(field, array, row)?;
                remaining = remaining.checked_sub(size).ok_or_else(unavailable)?;
                charges.push(context.memory.reserve(size)?);
                let Value::Utf8(key) = rtdb_columnar::read_execution_value(field, array, row)? else {
                    return Err(unavailable());
                };
                if keys.len() == fetch || keys.last().map(String::as_str).or(after).is_some_and(|prior| prior >= key.as_str()) {
                    return Err(unavailable());
                }
                let id = EntityId::String(key.clone());
                let mut point = open_owned_point(
                    Arc::clone(&context), owner, relation, &id,
                    Some(Interval::at(valid)?), Some(&["_id"]),
                ).await?.ok_or_else(unavailable)?;
                let mut found = false;
                while let Some(hit) = point.next_batch().await? {
                    if hit.batch().num_rows() == 0 { continue; }
                    if found || hit.batch().num_rows() != 1 { return Err(unavailable()); }
                    owned_row_revision(hit.batch(), 0, &id, &context.memory)?;
                    found = true;
                }
                if !found { return Err(unavailable()); }
                keys.push(key);
            }
        }
    }
    context.check_cancelled()?;
    Ok(OwnedIdentityCandidates { keys, _charges: charges })
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
    open_owned_table_point(context, relation.capacity_table(owner), id.iid(), Some(id), None, Some(columns), false).await
}

/// Reads the exact immutable lifetime policy and its current creation floor.
pub async fn read_owned_lifetime_floor(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
) -> Result<rtdb_types::OwnedLifetimeFloor> {
    if owner.is_empty() || !relation.is_root_data() { return Err(rtdb_types::OwnedWorld::unavailable()); }
    read_owned_lifetime_floor_table(context, relation.lifetime_table(owner)).await
}

pub async fn read_owned_lifetime_floor_table(
    context: Arc<QueryContext>, table: TableRef,
) -> Result<rtdb_types::OwnedLifetimeFloor> {
    let malformed = || Anomaly::fault("rtdb.owned/lifetime-floor", "The declared lifetime has no usable expiry floor");
    let relation = OwnedRelation::new(table.owned_lifetime_relation().ok_or_else(malformed)?)?;
    let id = EntityId::String(relation.name().into());
    let memory = Arc::clone(&context.memory);
    let columns = ["_id", "duration", "expired_created_through", "swept_present_through"];
    let Some(mut cursor) = open_owned_table_point(
        context, table, id.iid(), Some(&id), None, Some(&columns), false,
    ).await? else { return Err(malformed()); };
    let mut floor = None;
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        if batch.num_rows() == 0 { continue; }
        if floor.is_some() || batch.num_rows() != 1 { return Err(malformed()); }
        let mut document = rtdb_types::StructFields::new();
        let mut charges = Vec::with_capacity(columns.len());
        for name in columns {
            let (index, field) = batch.schema_ref().column_with_name(name).ok_or_else(malformed)?;
            let array = batch.column(index).as_ref();
            let bytes = rtdb_columnar::estimate_read_value_bytes(field, array, 0)?;
            charges.push(memory.reserve(bytes.saturating_add(4096))?);
            document.insert(name.into(), rtdb_columnar::read_value(field, array, 0)?);
        }
        floor = Some(rtdb_types::OwnedLifetimeFloor::decode(&document, &relation)?);
    }
    floor.ok_or_else(malformed)
}

/// Reads the exact declared schema at the caller's admitted cut.
pub async fn read_owned_relation_schema(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
) -> Result<Option<(rtdb_types::OwnedSchema, Arc<rtdb_columnar::MemoryReservation>)>> {
    if owner.is_empty() {
        return Err(Anomaly::incorrect("rtdb.owned/no-owner", "Owned relations require an authenticated principal"));
    }
    let id = EntityId::String(relation.name().into());
    let memory = Arc::clone(&context.memory);
    let Some(mut cursor) = open_owned_table_point(
        context, OwnedRelation::relation_catalog(owner), id.iid(), Some(&id), None,
        Some(&["schema"]), false,
    ).await? else { return Ok(None); };
    let malformed = || Anomaly::fault("rtdb.owned/schema-catalog", "Invalid stored owned declaration");
    let mut declaration = None;
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        if batch.num_rows() == 0 { continue; }
        let (index, field) = batch.schema_ref().column_with_name("schema").ok_or_else(malformed)?;
        let values = batch.column(index).as_ref();
        for row in 0..batch.num_rows() {
            if declaration.is_some() { return Err(malformed()); }
            let bytes = rtdb_columnar::estimate_read_value_bytes(field, values, row)?;
            let decoded = memory.reserve(bytes.checked_mul(8).and_then(|n| n.checked_add(4096))
                .ok_or_else(malformed)?)?;
            let Value::Binary(body) = rtdb_columnar::read_value(field, values, row)? else {
                return Err(malformed());
            };
            let schema = rtdb_types::OwnedSchema::from_json(&body).map_err(|_| malformed())?;
            declaration = Some((schema, decoded));
        }
    }
    Ok(declaration)
}

pub(crate) async fn open_owned_table_point(
    context: Arc<QueryContext>, table: TableRef, locator: Iid, id: Option<&EntityId>,
    valid: Option<Interval>, columns: Option<&[&str]>, history: bool,
) -> Result<Option<QueryCursor>> {
    open_owned_table_point_with_edges(context, table, locator, id, valid, columns, history, history).await
}

async fn open_owned_table_point_with_edges(
    context: Arc<QueryContext>, table: TableRef, locator: Iid, id: Option<&EntityId>,
    valid: Option<Interval>, columns: Option<&[&str]>, history: bool, edges: bool,
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
    if edges {
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

/// A replayed row keeps its original creation and expiration in its stored
/// validity interval while its physical system revision advances.
pub fn owned_row_lifetime(
    batch: &arrow_array::RecordBatch, row: usize, revision: Timestamp, duration: i64,
) -> Result<(Timestamp, Timestamp)> {
    let invalid = || Anomaly::fault("rtdb.owned/creation-lifetime", "The owned row lacks its exact creation lifetime");
    use arrow_array::Array as _;
    if row >= batch.num_rows() { return Err(invalid()); }
    let edge = |name: &str| -> Result<Timestamp> {
        let array = batch.column_by_name(name).and_then(|array| array.as_any().downcast_ref::<arrow_array::TimestampMicrosecondArray>())
            .filter(|array| array.is_valid(row)).ok_or_else(invalid)?;
        Ok(Timestamp(array.value(row)))
    };
    let created = edge("_valid_from")?;
    let expires = edge("_valid_to")?;
    if created.0 <= 0 || created > revision || duration <= 0 || created.0.checked_add(duration) != Some(expires.0) {
        return Err(invalid());
    }
    Ok((created, expires))
}

fn address_unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/address-unavailable", "No exact owned key is available in this scope")
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

pub async fn read_owned_world_clock(
    context: Arc<QueryContext>, owner: &str, world: Iid,
) -> Result<Option<Timestamp>> {
    let unavailable = rtdb_types::OwnedWorld::unavailable;
    let id = EntityId::Uuid(world.0);
    let Some(mut cursor) = open_owned_table_point(
        Arc::clone(&context), OwnedRelation::world_clock_table(owner), world, Some(&id),
        None, Some(&["clock"]), false,
    ).await? else { return Ok(None); };
    let mut clock = None;
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        if batch.num_rows() == 0 { continue; }
        if clock.is_some() || batch.num_rows() != 1 { return Err(unavailable()); }
        let revision = owned_row_revision(batch, 0, &id, &context.memory)?;
        let (index, field) = batch.schema_ref().column_with_name("clock").ok_or_else(unavailable)?;
        let Value::I64(reading) = rtdb_columnar::read_value(field, batch.column(index).as_ref(), 0)? else {
            return Err(unavailable());
        };
        if reading > revision.0 { return Err(unavailable()); }
        clock = Some(Timestamp(reading));
    }
    Ok(clock)
}

pub async fn read_owned_world(
    context: Arc<QueryContext>, owner: &str, world: rtdb_types::Iid,
) -> Result<Option<(rtdb_types::OwnedWorld, Arc<rtdb_columnar::MemoryReservation>)>> {
    use rtdb_types::{OwnedWorld, StructFields};

    let memory = Arc::clone(&context.memory);
    let id = EntityId::Uuid(world.0);
    let names = ["_id", "pin", "capture", "manifest", "basis_id", "basis_time", "parent_publication", "mode", "held_start", "until", "schemas", "dropped", "suffix"];
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

pub async fn read_owned_root_publication(context: Arc<QueryContext>, owner: &str) -> Result<Option<Timestamp>> {
    let memory = Arc::clone(&context.memory);
    let relation = OwnedRelation::new(rtdb_types::OWNED_ROOT_PUBLICATION_RELATION)?;
    let id = EntityId::String("head".into());
    let valid = Interval::ALL;
    let Some(mut cursor) = open_owned_point(context, owner, &relation, &id, Some(valid), Some(&["_id", "_system_from"])).await?
    else { return Ok(None); };
    let mut publication = None;
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() {
            let revision = owned_row_revision(batch.batch(), row, &id, &memory)?;
            if publication.replace(revision).is_some() { return Err(rtdb_types::OwnedWorld::unavailable()); }
        }
    }
    Ok(publication)
}

pub async fn read_owned_parent_schemas(
    context: Arc<QueryContext>, owner: &str,
) -> Result<(rtdb_types::WorldSchemas, Arc<rtdb_columnar::MemoryReservation>)> {
    if owner.is_empty() { return Err(rtdb_types::OwnedWorld::unavailable()); }
    read_owned_schema_catalog(context, OwnedRelation::relation_catalog(owner)).await
}

pub async fn read_owned_schema_catalog(
    context: Arc<QueryContext>, catalog: TableRef,
) -> Result<(rtdb_types::WorldSchemas, Arc<rtdb_columnar::MemoryReservation>)> {
    use std::collections::BTreeMap;
    use rtdb_types::{OwnedSchema, OwnedWorld, WorldSchemas};

    if !catalog.is_owned_relation_catalog() { return Err(OwnedWorld::unavailable()); }
    let memory = Arc::clone(&context.memory);
    let mut charge = memory.reserve(4096)?;
    let mut bytes = 0usize;
    let mut fields = BTreeMap::new();
    if let Some(mut rows) = open_private_catalog(
        context, catalog, &["_id", "schema"], None,
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
