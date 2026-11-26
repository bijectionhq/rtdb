use std::{collections::{BTreeMap, BTreeSet}, num::NonZeroU32, sync::Arc};

use rtdb_columnar::EntityIdRef;
use rtdb_types::OwnedIdentityScan;
use rtdb_plan::{BinaryOp, Catalog, ColumnId, Expr, Field, QualifiedTable, Relation, Scan, SortExpr, TemporalFilter};
use rtdb_types::{Anomaly, AnomalyCategory, EntityId, Iid, Interval, OwnedRelation, Result, TableRef, TimeUnit, Timestamp, TransactionKey, TxId, Value};

use crate::{CandidateIids, PreparedQuery, QueryContext, QueryCursor, context::OwnedSelection};

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

/// Project an owned point together with its winning event's ordinal.
pub async fn open_owned_assertion_point(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId,
    valid: Option<Interval>, include_edges: bool,
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() || relation.is_world_catalog() { return Err(address_unavailable()); }
    let database = context.catalog.database(context.catalog.default_database())?;
    if let [snapshot] = database.partitions.as_slice() {
        if snapshot.owned_routing_inventory(owner, relation)?.data_exhausted() {
            context.check_cancelled()?;
            return Ok(None);
        }
    }
    open_owned_table_point_with_edges(
        context, relation.table(owner), id.iid(), Some(id), valid, None, false, include_edges, true,
    ).await
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

/// Compare the original winner of a business-valid point by its event position.
pub async fn read_owned_point_version(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId, valid: Interval,
) -> Result<Option<rtdb_types::OwnedPointVersion>> {
    let columns = [rtdb_types::OWNED_EVENT_POSITION_COLUMN];
    let Some(mut cursor) = open_owned_point(Arc::clone(&context), owner, relation, id, Some(valid), Some(&columns)).await? else {
        return Ok(None);
    };
    let mut version = None;
    while let Some(rows) = cursor.next_batch().await? {
        for row in 0..rows.batch().num_rows() {
            let history = owned_row_revision(rows.batch(), row, id, &context.memory)?;
            let position = owned_row_event_position(rows.batch(), row)?;
            if version.replace(rtdb_types::OwnedPointVersion { history, position }).is_some() {
                return Err(address_unavailable());
            }
        }
    }
    Ok(version)
}

/// Whether a stored column belongs to a reader's revealed projection: every
/// column except system time, validity endpoints, private RTDB metadata and
/// the columns the reader could not open.
fn owned_revealed_column(name: &str, hidden: &[String]) -> bool {
    !matches!(name, "_system_from" | "_system_to" | "_valid_from" | "_valid_to" | "_assertion_ordinal")
        && !name.starts_with("_rtdb_")
        && hidden.binary_search_by(|column| column.as_str().cmp(name)).is_err()
}

/// Commit to one row's revealed projection, ordered by column name.
pub fn owned_revealed_digest(batch: &arrow_array::RecordBatch, row: usize, hidden: &[String]) -> Result<[u8; 32]> {
    use sha2::{Digest, Sha256};

    if row >= batch.num_rows() {
        return Err(Anomaly::fault("rtdb.owned/revealed-row", "The revealed point row is outside its batch"));
    }
    let schema = batch.schema_ref();
    let mut columns: Vec<_> = schema.fields().iter().enumerate()
        .filter(|(_, field)| owned_revealed_column(field.name(), hidden))
        .collect();
    columns.sort_by(|(_, left), (_, right)| left.name().cmp(right.name()));
    let mut digest = Sha256::new();
    digest.update(b"rtdb.owned.revealed.v1");
    for (index, field) in columns {
        let value = rtdb_columnar::read_value(field, batch.column(index).as_ref(), row)?;
        let encoded = serde_json::to_vec(&value)
            .map_err(|_| Anomaly::fault("rtdb.owned/revealed-value", "A revealed point value has no canonical encoding"))?;
        digest.update((field.name().len() as u64).to_be_bytes());
        digest.update(field.name().as_bytes());
        digest.update((encoded.len() as u64).to_be_bytes());
        digest.update(&encoded);
    }
    Ok(digest.finalize().into())
}

/// One point's revealed projection over `valid` at the context's cut, as
/// maximal valid-time slices of equal digests; gaps are absence. `None` when
/// a row has no canonical encoding, so the caller cannot compare it.
pub async fn owned_revealed_timeline(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId, valid: Interval,
    hidden: &[String],
) -> Result<Option<Vec<(Timestamp, Timestamp, [u8; 32])>>> {
    use arrow_array::{Array, TimestampMicrosecondArray};

    if owner.is_empty() || relation.is_world_catalog() || valid.is_empty() { return Err(address_unavailable()); }
    let Some(mut cursor) = open_owned_table_point_with_edges(
        Arc::clone(&context), relation.table(owner), id.iid(), Some(id), Some(valid), None, false, true, false,
    ).await? else {
        return Ok(Some(Vec::new()));
    };
    let mut slices = Vec::new();
    while let Some(rows) = cursor.next_batch().await? {
        let batch = rows.batch();
        let edge = |name: &str, row: usize, open: Timestamp| -> Result<Timestamp> {
            let array = batch.column_by_name(name)
                .and_then(|array| array.as_any().downcast_ref::<TimestampMicrosecondArray>())
                .ok_or_else(address_unavailable)?;
            Ok(if array.is_valid(row) { Timestamp(array.value(row)) } else { open })
        };
        for row in 0..batch.num_rows() {
            context.check_cancelled()?;
            let Ok(digest) = owned_revealed_digest(batch, row, hidden) else { return Ok(None); };
            let from = edge("_valid_from", row, Timestamp::MIN)?.max(valid.lower());
            let to = edge("_valid_to", row, Timestamp::MAX)?.min(valid.upper());
            if from < to { slices.push((from, to, digest)); }
        }
    }
    slices.sort();
    let mut merged: Vec<(Timestamp, Timestamp, [u8; 32])> = Vec::with_capacity(slices.len());
    for (from, to, digest) in slices {
        match merged.last_mut() {
            Some(last) if last.1 > from => return Ok(None),
            Some(last) if last.1 == from && last.2 == digest => last.1 = to,
            _ => merged.push((from, to, digest)),
        }
    }
    Ok(Some(merged))
}

/// The single projection a timeline shows throughout `valid`: absence or one
/// digest. `None` when it changes inside `valid`, so the read is undetermined.
pub fn owned_revealed_constant(timeline: &[(Timestamp, Timestamp, [u8; 32])], valid: Interval) -> Option<Option<[u8; 32]>> {
    let inside: Vec<_> = timeline.iter().filter(|(from, to, _)| *from < valid.upper() && valid.lower() < *to).collect();
    match inside.as_slice() {
        [] => Some(None),
        [(from, to, digest)] if *from <= valid.lower() && valid.upper() <= *to => Some(Some(*digest)),
        _ => None,
    }
}

pub fn owned_row_event_position(batch: &arrow_array::RecordBatch, row: usize) -> Result<rtdb_types::EventPosition> {
    use arrow_array::{Array, BinaryArray};

    let invalid = || Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/event-position",
        "The owned point has no original assertion position");
    let column = batch.column_by_name(rtdb_types::OWNED_EVENT_POSITION_COLUMN)
        .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
        .filter(|column| row < column.len() && column.is_valid(row)).ok_or_else(invalid)?;
    let position = rtdb_types::EventPosition::from_bytes(column.value(row).try_into().map_err(|_| invalid())?);
    if position.transaction.0 < 0 { return Err(invalid()); }
    Ok(position)
}

/// Project the original validity endpoints along with a live lifetime point.
pub async fn open_owned_lifetime_point(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId, valid: Interval,
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() || !relation.is_root_data() || valid.is_empty() { return Err(address_unavailable()); }
    open_owned_table_point_with_edges(context, relation.table(owner), id.iid(), Some(id), Some(valid), None, false, true, true).await
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

/// Projects the admitted complete keys at their recorded locators with one cursor.
#[doc(hidden)]
pub async fn open_owned_artefact_candidates(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, valid: Interval, column: &str,
    candidates: impl IntoIterator<Item = (Iid, arrow_buffer::Buffer)>,
) -> Result<Option<QueryCursor>> {
    use crate::context::{OwnedArtefactKeys, OwnedSelection};

    if owner.is_empty() || relation.world().is_some() || relation.is_world_catalog() || valid.is_empty() {
        return Err(address_unavailable());
    }
    let mut by_iid = BTreeMap::<_, Vec<_>>::new();
    let mut reservation = context.memory.reserve(256)?;
    for (iid, key) in candidates {
        let lease = Arc::get_mut(&mut reservation).expect("Owned candidate cursor owns its keys");
        lease.grow_to(lease.bytes().saturating_add(key.len()).saturating_add(128))?;
        by_iid.entry(iid).or_default().push(key);
    }
    let iids = by_iid.keys().copied().collect();
    let selection = OwnedSelection::Artefacts(Arc::new(OwnedArtefactKeys { by_iid, _reservation: reservation }));
    // A candidate names its winning event by position, which a tied
    // transaction's event never shares (T7).
    let mut columns = vec!["_iid", "_id", rtdb_types::OWNED_EVENT_POSITION_COLUMN];
    if !columns.contains(&column) { columns.push(column); }
    open_owned_table_selection(context, relation.table(owner), iids, selection, Some(valid), Some(&columns), false, false, false).await
}

/// Certifies a covered range page from the winning rows at the original held cut.
#[doc(hidden)]
pub async fn try_owned_lifetime_range_expiry(
    context: &Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
    page: &arrow_array::RecordBatch, valid: Interval, duration: i64,
) -> Result<Option<Timestamp>> {
    use crate::context::{OwnedArtefactKeys, OwnedSelection};
    use rtdb_columnar::{ColumnReader, IidView};
    use std::collections::btree_map::Entry;

    // A failed speculative read consumes part of the audience's work allowance.
    if !(2..=128).contains(&page.num_rows()) || owner.is_empty() || !relation.is_root_data()
        || !valid.is_point() || rtdb_storage::audience_work::is_bounded()
    {
        return Ok(None);
    }
    let Some((column, field)) = page.schema_ref().column_with_name("_id") else { return Ok(None); };
    let mut reservation = context.memory.reserve(4096)?;
    let reader = ColumnReader::new(field)?;
    let ids = page.column(column).as_ref();
    let mut candidates = BTreeMap::new();
    let mut by_iid = BTreeMap::new();
    for row in 0..page.num_rows() {
        context.check_cancelled()?;
        let Some(id) = reader.read_owned_entity_id(ids, row)? else { return Ok(None); };
        let iid = id.iid();
        let Entry::Vacant(candidate) = candidates.entry(iid) else { return Ok(None); };
        let bytes = id.owned_payload_bytes().saturating_mul(2).saturating_add(512);
        let lease = Arc::get_mut(&mut reservation).expect("Unpublished range lookup owns its reservation");
        lease.grow_to(lease.bytes().saturating_add(bytes))?;
        candidate.insert((id, None));
        let key = arrow_buffer::Buffer::from_vec(id.into_owned().handle_bytes());
        by_iid.insert(iid, vec![key]);
    }
    let iids = by_iid.keys().copied().collect();
    let selection = OwnedSelection::Artefacts(Arc::new(OwnedArtefactKeys { by_iid, _reservation: Arc::clone(&reservation) }));
    let columns = ["_iid", "_id", "_system_from", "_valid_from", "_valid_to"];
    let Some(mut cursor) = open_owned_table_selection(Arc::clone(context), relation.table(owner), iids,
        selection, Some(valid), Some(&columns), false, false, false).await? else { return Ok(None); };

    let present = valid.lower();
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        let Some(iids) = batch.column_by_name("_iid") else { return Ok(None); };
        let iids = IidView::new(iids.as_ref())?;
        for row in 0..batch.num_rows() {
            context.check_cancelled_at(row)?;
            let Some((id, verified)) = candidates.get_mut(&iids.get(row)?) else { return Ok(None); };
            if verified.is_some() { return Ok(None); }
            let revision = owned_row_revision_for(batch, row, *id, &context.memory)?;
            let (_, cutoff) = owned_row_lifetime(batch, row, revision, duration)?;
            if revision > present || cutoff <= present { return Ok(None); }
            *verified = Some((revision, cutoff));
        }
    }

    let mut earliest = Timestamp::MAX;
    for (_, verified) in candidates.values() {
        let Some((_, cutoff)) = verified else { return Ok(None); };
        earliest = earliest.min(*cutoff);
    }
    Ok(Some(earliest))
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
    open_owned_assertion_point(context, owner, relation, id, Some(valid), false).await
}

/// Resolve one retained point with the specified suffix of one commit masked.
#[doc(hidden)]
pub async fn open_owned_point_with_position_mask(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId,
    valid: Interval, system: Timestamp, mask: (TransactionKey, u32),
) -> Result<Option<QueryCursor>> {
    if !relation.is_root_data() || valid.is_empty() { return Err(address_unavailable()); }
    let context = owned_context_at_position(context, system, mask)?;
    open_owned_assertion_point(context, owner, relation, id, Some(valid), false).await
}

/// Resolve the winning valid-time slices after excluding the same commit suffix.
#[doc(hidden)]
pub async fn open_owned_valid_during_with_position_mask(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId,
    valid: Interval, system: Timestamp, mask: (TransactionKey, u32),
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() || !relation.is_root_data() || valid.is_empty() {
        return Err(address_unavailable());
    }
    let context = owned_context_at_position(context, system, mask)?;
    open_owned_table_point_with_edges(
        context, relation.table(owner), id.iid(), Some(id), Some(valid), None, true, true, true,
    ).await
}

fn owned_context_at_position(
    context: Arc<QueryContext>, system: Timestamp, mask: (TransactionKey, u32),
) -> Result<Arc<QueryContext>> {
    let mut context = owned_context_at_system(context, system)?;
    Arc::make_mut(&mut context).catalog.owned_position_mask = Some(mask);
    Ok(context)
}

fn owned_context_at_system(context: Arc<QueryContext>, system: Timestamp) -> Result<Arc<QueryContext>> {
    let Some(held) = context.owned_held_cut().filter(|held| system <= *held) else {
        return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/history-cut", "Owned history requires an exact retained system cut"));
    };
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
    open_owned_table_point_with_edges(
        context, relation.table(owner), id.iid(), Some(id), Some(valid), None, true, true, true,
    ).await
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

/// Streams the projected members of an owned Root at the caller's admitted cut.
pub async fn open_owned_root_projection(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
    valid: Timestamp, columns: &[&str],
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() || !relation.is_root_data() || columns.is_empty() {
        return Err(address_unavailable());
    }
    open_constraint_projection(context, owner, relation, valid, columns, None, None).await
}

/// A Root projection of the visible rows of the named IIDs.
pub async fn open_owned_root_projection_of(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
    valid: Timestamp, columns: &[&str], iids: CandidateIids,
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() || !relation.is_root_data() || columns.is_empty() {
        return Err(address_unavailable());
    }
    open_constraint_projection(context, owner, relation, valid, columns, None, Some(iids)).await
}

pub async fn open_owned_world_constraint_projection(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
    valid: Timestamp, columns: &[&str],
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() || relation.root_relation().is_none() || columns.is_empty() {
        return Err(address_unavailable());
    }
    open_constraint_projection(context, owner, relation, valid, columns, None, None).await
}

/// Restrict a receiver projection to one complete installed identity prefix.
pub async fn open_owned_root_prefix_projection(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
    valid: Timestamp, columns: &[&str], prefix: EntityIdRef<'_>,
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() || !relation.is_root_data()
        || matches!(prefix, EntityIdRef::String(text) if text.is_empty() || !text.bytes().all(|byte|
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'/' || byte == b'-'))
        || columns.first() != Some(&"_id")
    {
        return Err(address_unavailable());
    }
    open_constraint_projection(context, owner, relation, valid, columns, Some(prefix), None).await
}

async fn open_constraint_projection(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
    valid: Timestamp, columns: &[&str], prefix: Option<EntityIdRef<'_>>, iids: Option<CandidateIids>,
) -> Result<Option<QueryCursor>> {
    let name = QualifiedTable {
        database: context.catalog.default_database().to_owned(),
        table: relation.table(owner),
    };
    let selection = match iids {
        Some(iids) => OwnedSelection::Candidates(iids),
        None if columns.contains(&rtdb_types::OWNED_EVENT_POSITION_COLUMN) => OwnedSelection::PositionedProjection,
        None => OwnedSelection::Relation,
    };
    let context = context.for_owned_selection(name.clone(), selection);
    let Some(table) = context.catalog.table(&name)? else { return Ok(None); };
    let columns = columns.iter().enumerate().map(|(index, name)| Field {
        id: ColumnId(index as u32), name: (*name).into(), qualifier: None, qualified_only: false,
        unnamed: false, data_type: table.scan_column_type(name),
    }).collect::<Vec<_>>();
    let mut predicates = Vec::new();
    if let Some(prefix) = prefix {
        if matches!(prefix, EntityIdRef::Binary(_)) && !OwnedIdentityScan::binary(None).accepts(&columns[0].data_type) {
            return Err(address_unavailable());
        }
        predicates.extend(identity_prefix_predicates(columns[0].expression(), prefix)?);
    }
    let scan = Arc::new(Relation::scan(Scan {
        table, columns,
        valid_time: TemporalFilter::At(Expr::Literal(Value::Timestamp {
            value: valid.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()),
        })),
        system_time: TemporalFilter::Default, clamp_valid_time: false,
        iids: None, predicates, matches: Vec::new(), top_k: None, ordered_top_k: None,
    })?);
    PreparedQuery::new_in(scan, &context.memory)?.execute(context, Vec::new()).await.map(Some)
}

/// Opens a bounded page of visible identities in complete key order.
pub async fn open_owned_identity_page(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, system: Timestamp,
    valid: Timestamp, after: &OwnedIdentityScan<'_>, fetch: i64,
) -> Result<Option<QueryCursor>> {
    if relation.world().is_some() || relation.is_world_catalog() {
        return Err(address_unavailable());
    }
    open_owned_identity_candidates(context, owner, relation, system, valid, after, None, fetch).await
}

/// Read root identities with any assertion current at `system` across all
/// valid time, in key order. A key repeats once per current valid slice; the
/// caller collapses adjacent repeats.
pub async fn open_owned_identity_page_all_valid(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, system: Timestamp,
    after: &OwnedIdentityScan<'_>, fetch: i64,
) -> Result<Option<QueryCursor>> {
    if relation.world().is_some() || relation.is_world_catalog() {
        return Err(address_unavailable());
    }
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
    if !after.accepts(&id.data_type) { return Err(address_unavailable()); }
    let order = id.expression();
    let predicates: Vec<_> = after.bound().map(|after| Expr::binary(
        BinaryOp::Gt, order.clone(), Expr::Literal(after),
    )).into_iter().collect();
    let scan = Arc::new(Relation::scan(Scan {
        table,
        columns: vec![id],
        valid_time: TemporalFilter::All,
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

/// Read complete keys only within the installed protected carrier prefix.
pub async fn open_owned_identity_prefix(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
    at: Timestamp, after: &OwnedIdentityScan<'_>, prefix: EntityIdRef<'_>, fetch: i64,
) -> Result<Option<QueryCursor>> {
    if relation.world().is_some() || relation.is_world_catalog() || !prefix_accepts_bound(prefix, after)
    {
        return Err(address_unavailable());
    }
    open_owned_identity_candidates(context, owner, relation, at, at, after, Some(prefix), fetch).await
}

fn prefix_accepts_bound(prefix: EntityIdRef<'_>, after: &OwnedIdentityScan<'_>) -> bool {
    match (prefix, after) {
        (EntityIdRef::String(prefix), OwnedIdentityScan::Text(after)) =>
            !prefix.is_empty() && prefix.is_ascii() && !prefix.ends_with('\x7f')
                && after.as_deref().is_none_or(|after| after.starts_with(prefix)),
        (EntityIdRef::Binary(prefix), OwnedIdentityScan::Binary(after)) =>
            !prefix.is_empty() && after.as_deref().is_none_or(|after| after.starts_with(prefix)),
        _ => false,
    }
}

fn identity_has_prefix(prefix: EntityIdRef<'_>, id: EntityIdRef<'_>) -> bool {
    match (prefix, id) {
        (EntityIdRef::String(prefix), EntityIdRef::String(id)) => id.starts_with(prefix),
        (EntityIdRef::Binary(prefix), EntityIdRef::Binary(id)) => id.starts_with(prefix),
        _ => false,
    }
}

fn identity_prefix_bounds(prefix: EntityIdRef<'_>) -> Result<(Value, Option<Value>)> {
    let bounds = match prefix {
        EntityIdRef::String(prefix) if prefix.is_ascii() && !prefix.ends_with('\x7f') => {
            let mut upper = prefix.to_owned();
            let last = upper.pop().ok_or_else(address_unavailable)?;
            upper.push(char::from(last as u8 + 1));
            (Value::Utf8(prefix.to_owned()), Some(Value::Utf8(upper)))
        }
        EntityIdRef::Binary(prefix) if !prefix.is_empty() => {
            let upper = prefix.iter().rposition(|&byte| byte != u8::MAX).map(|last| {
                let mut upper = prefix[..=last].to_vec();
                upper[last] += 1;
                Value::Binary(upper)
            });
            (Value::Binary(prefix.to_vec()), upper)
        }
        _ => return Err(address_unavailable()),
    };
    Ok(bounds)
}

fn identity_prefix_predicates(id: Expr, prefix: EntityIdRef<'_>) -> Result<Vec<Expr>> {
    let (lower, upper) = identity_prefix_bounds(prefix)?;
    let mut predicates = vec![Expr::binary(BinaryOp::GtEq, id.clone(), Expr::Literal(lower))];
    predicates.extend(upper.map(|upper| Expr::binary(BinaryOp::Lt, id, Expr::Literal(upper))));
    Ok(predicates)
}

async fn open_owned_identity_candidates(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, system: Timestamp,
    valid: Timestamp, after: &OwnedIdentityScan<'_>, prefix: Option<EntityIdRef<'_>>, fetch: i64,
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
    if !after.accepts(&id.data_type) { return Err(address_unavailable()); }
    let order = id.expression();
    let mut predicates: Vec<_> = after.bound().map(|after| Expr::binary(
        BinaryOp::Gt, order.clone(), Expr::Literal(after),
    )).into_iter().collect();
    if let Some(prefix) = prefix {
        predicates.extend(identity_prefix_predicates(order.clone(), prefix)?);
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

/// A bounded page of ordered keys with retained memory ownership.
pub struct OwnedIdentityCandidates {
    pub keys: Vec<EntityId>,
    _charges: Vec<Arc<rtdb_columnar::MemoryReservation>>,
}

impl OwnedIdentityCandidates {
    pub fn into_parts(self) -> (Vec<EntityId>, Vec<Arc<rtdb_columnar::MemoryReservation>>) {
        (self.keys, self._charges)
    }
}

/// Reads root identities or local shadow candidates in one exact native cut.
pub async fn owned_identity_candidates(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, system: Timestamp,
    valid: Timestamp, after: &OwnedIdentityScan<'_>, fetch: usize,
) -> Result<OwnedIdentityCandidates> {
    owned_identity_candidates_in_prefix(context, owner, relation, system, valid, after, fetch, None).await
}

/// Read an exact bounded identity page in a caller-admitted opaque scope.
pub async fn owned_identity_candidates_in_prefix(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, system: Timestamp,
    valid: Timestamp, after: &OwnedIdentityScan<'_>, fetch: usize, prefix: Option<EntityIdRef<'_>>,
) -> Result<OwnedIdentityCandidates> {
    let unavailable = || Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/world-identities",
        "The held world identity candidates exceed their exact coverage or byte bound");
    if !(1..=129).contains(&fetch) || relation.is_world_catalog()
        || prefix.is_some_and(|prefix| !prefix_accepts_bound(prefix, after))
    {
        return Err(unavailable());
    }
    let context = owned_context_at_system(context, system)?;
    let mut charges = vec![context.memory.reserve(fetch * 128)?];
    let mut keys: Vec<EntityId> = Vec::with_capacity(fetch);
    let mut remaining: usize = 8 << 20;
    let bound = after.bound().map(|key| key.owned_entity_id()).transpose()?;
    if let Some(mut cursor) = open_owned_identity_candidates(
        Arc::clone(&context), owner, relation, system, valid, after, prefix, fetch as i64,
    ).await? {
        while let Some(batch) = cursor.next_batch().await? {
            let batch = batch.batch();
            let (column, field) = batch.schema_ref().column_with_name("_id").ok_or_else(unavailable)?;
            let array = batch.column(column).as_ref();
            for row in 0..batch.num_rows() {
                let size = rtdb_columnar::estimate_read_value_bytes(field, array, row)?;
                remaining = remaining.checked_sub(size).ok_or_else(unavailable)?;
                charges.push(context.memory.reserve(size)?);
                let id = rtdb_columnar::read_owned_entity_id(field, array, row)
                    .map_err(|_| unavailable())?.into_owned();
                if keys.len() == fetch
                    || prefix.is_some_and(|prefix| !identity_has_prefix(prefix, EntityIdRef::from(&id)))
                    || keys.last().or(bound.as_ref()).is_some_and(|prior| prior >= &id)
                {
                    return Err(unavailable());
                }
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
                keys.push(id);
            }
        }
    }
    context.check_cancelled()?;
    Ok(OwnedIdentityCandidates { keys, _charges: charges })
}

/// Project winning base facts, including when a decision suffix is hidden.
pub async fn open_owned_unique_window(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    column: &str,
    key: Option<&[u8]>,
    valid: Interval,
    system: Timestamp,
    mask: Option<(TransactionKey, u32)>,
    fetch: usize,
) -> Result<Option<QueryCursor>> {
    if !relation.is_root_data() {
        return Err(address_unavailable());
    }
    let context = match mask {
        Some(mask) => owned_context_at_position(context, system, mask)?,
        None => owned_context_at_system(context, system)?,
    };
    open_unique_window(context, owner, relation, column, key, valid, fetch).await
}

pub async fn open_staged_unique_window(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    column: &str,
    key: &[u8],
    valid: Interval,
    fetch: usize,
) -> Result<Option<QueryCursor>> {
    if !context.catalog.transaction_scoped() || relation.root_relation().is_none() {
        return Err(address_unavailable());
    }
    open_unique_window(context, owner, relation, column, Some(key), valid, fetch).await
}

/// Scan a frozen writer's claims at its already admitted historical system cut.
pub async fn open_archived_world_unique_window(
    context: Arc<QueryContext>,
    owner: &str,
    shadow: &OwnedRelation,
    column: &str,
    key: &[u8],
    valid: Interval,
    fetch: usize,
) -> Result<Option<QueryCursor>> {
    if shadow.root_relation().is_none() {
        return Err(address_unavailable());
    }
    if context.owned_held_cut().is_none() {
        return Err(address_unavailable());
    }
    open_unique_window(context, owner, shadow, column, Some(key), valid, fetch).await
}

async fn open_unique_window(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    column: &str,
    key: Option<&[u8]>,
    valid: Interval,
    fetch: usize,
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() || valid.is_empty()
        || valid.lower() == Timestamp::MIN || valid.upper() == Timestamp::MAX
        || !(1..=1025).contains(&fetch)
        || key.is_some_and(|key| key.is_empty() || key.len() > crate::OWNED_UNIQUE_KEY_MAX)
    {
        return Err(address_unavailable());
    }
    let name = QualifiedTable {
        database: context.catalog.default_database().to_owned(),
        table: relation.table(owner),
    };
    let context = context.for_owned_relation(name.clone(), None);
    let Some(table) = context.catalog.table(&name)? else { return Ok(None); };
    let columns = ["_id", "_valid_from", "_valid_to", "_system_from", column]
        .into_iter().enumerate().map(|(index, name)| Field {
            id: ColumnId(index as u32), name: name.into(), qualifier: None, qualified_only: false,
            unnamed: false, data_type: table.scan_column_type(name),
        }).collect::<Vec<_>>();
    let key_field = columns[4].expression();
    let predicate = match key {
        Some(key) => Expr::binary(BinaryOp::Eq, key_field, Expr::Literal(Value::Binary(key.to_vec()))),
        None => Expr::Unary { op: rtdb_plan::UnaryOp::IsNotNull, expr: Box::new(key_field) },
    };
    let timestamp = |time: Timestamp| Expr::Literal(Value::Timestamp {
        value: time.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()),
    });
    let scan = Arc::new(Relation::scan(Scan {
        table, columns,
        valid_time: TemporalFilter::FromTo { from: timestamp(valid.lower()), to: timestamp(valid.upper()) },
        system_time: TemporalFilter::Default, clamp_valid_time: true,
        iids: None, predicates: vec![predicate], matches: Vec::new(), top_k: None, ordered_top_k: None,
    })?);
    let query = Arc::new(scan.limit(None, Some(Expr::Literal(Value::I64(fetch as i64))))?);
    PreparedQuery::new_in(query, &context.memory)?.execute(context, Vec::new()).await.map(Some)
}

/// Resolve the winning key projection over a valid window at this snapshot.
pub async fn open_owned_key_timeline(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId,
    valid: Interval, column: &str,
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() || valid.is_empty() { return Err(address_unavailable()); }
    open_owned_table_point(
        context, relation.table(owner), id.iid(), Some(id), Some(valid), Some(&[column]), true,
    ).await
}

/// Read the private policy or an exact valid instant of a unique claim.
pub async fn open_owned_unique_point(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId,
    valid: Option<Interval>, columns: &[&str],
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() { return Err(address_unavailable()); }
    open_owned_table_point(context, relation.unique_table(owner), id.iid(), Some(id), valid, Some(columns), false).await
}

/// Resolve all winning holders of one complete key in the requested period.
pub async fn open_owned_unique_timeline(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId,
    valid: Interval,
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() || valid.is_empty() { return Err(address_unavailable()); }
    open_owned_table_point(
        context, relation.unique_table(owner), id.iid(), Some(id), Some(valid), Some(&["holder"]), true,
    ).await
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

/// Reads the adoption edge at the supplied physical prefix.
#[doc(hidden)]
pub async fn read_owned_adoption(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
) -> Result<Option<(OwnedRelation, rtdb_types::TransactionKey)>> {
    let id = EntityId::String(relation.name().into());
    let columns = ["predecessor", "activation_tx", "activation_micros"];
    let Some(mut cursor) = open_owned_table_point(
        context, OwnedRelation::adopted_catalog(owner), id.iid(), Some(&id), None, Some(&columns), false,
    ).await? else { return Ok(None); };
    let malformed = || Anomaly::fault("rtdb.owned/adoption-catalog", "Invalid stored relation adoption");
    let mut result = None;
    while let Some(rows) = cursor.next_batch().await? {
        let batch = rows.batch();
        for row in 0..batch.num_rows() {
            let cell = |name| rtdb_columnar::ArrowCell::named(batch, name, row).ok_or_else(malformed);
            let source = cell("predecessor")?.utf8().ok_or_else(malformed)?;
            if result.is_some() || source == relation.name() { return Err(malformed()); }
            result = Some((OwnedRelation::new(source)?, rtdb_types::TransactionKey {
                tx_id: rtdb_types::TxId(cell("activation_tx")?.int64().ok_or_else(malformed)?),
                system_time: Timestamp(cell("activation_micros")?.int64().ok_or_else(malformed)?),
            }));
        }
    }
    Ok(result)
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

/// Reads a Root relation's retirement, if one fenced it at the caller's admitted cut.
pub async fn read_owned_retirement(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
) -> Result<Option<rtdb_types::OwnedRetirement>> {
    if owner.is_empty() {
        return Err(Anomaly::incorrect("rtdb.owned/no-owner", "Owned relations require an authenticated principal"));
    }
    let id = EntityId::String(relation.name().into());
    let Some(cursor) = open_owned_table_point(
        context, OwnedRelation::retired_catalog(owner), id.iid(), Some(&id), None,
        Some(&["_id", "retired_at", "retain_micros"]), false,
    ).await? else { return Ok(None); };
    let mut retired = read_retirements(cursor).await?;
    match retired.pop() {
        None => Ok(None),
        Some((name, retirement)) if retired.is_empty() && &name == relation => Ok(Some(retirement)),
        Some(_) => Err(Anomaly::fault("rtdb.owned/retired-catalog", "Invalid stored owned retirement")),
    }
}

/// Reads every current retirement in one principal's retired catalog, for collection.
pub async fn read_owned_retirements(
    context: Arc<QueryContext>, catalog: TableRef,
) -> Result<Vec<(OwnedRelation, rtdb_types::OwnedRetirement)>> {
    if !catalog.is_owned_retired_catalog() {
        return Err(Anomaly::incorrect("rtdb.owned/retired-catalog", "Expected an owned retired catalog"));
    }
    let Some(cursor) = open_private_catalog(context, catalog, &["_id", "retired_at", "retain_micros"], None).await? else {
        return Ok(Vec::new());
    };
    read_retirements(cursor).await
}

async fn read_retirements(mut cursor: QueryCursor) -> Result<Vec<(OwnedRelation, rtdb_types::OwnedRetirement)>> {
    let malformed = || Anomaly::fault("rtdb.owned/retired-catalog", "Invalid stored owned retirement");
    let mut retired = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        for row in 0..batch.num_rows() {
            let mut record = rtdb_types::StructFields::new();
            for name in ["_id", "retired_at", "retain_micros"] {
                let (index, field) = batch.schema_ref().column_with_name(name).ok_or_else(malformed)?;
                record.insert(name.into(), rtdb_columnar::read_value(field, batch.column(index).as_ref(), row)?);
            }
            retired.push(rtdb_types::OwnedRetirement::decode(&record).map_err(|_| malformed())?);
        }
    }
    Ok(retired)
}

/// Reads every current registration in one principal's world catalog, for collection.
pub async fn read_owned_worlds(context: Arc<QueryContext>, catalog: TableRef) -> Result<Vec<rtdb_types::OwnedWorld>> {
    use rtdb_types::{OwnedWorld, StructFields};

    if catalog.owned_world_catalog().as_ref() != Some(&catalog) { return Err(OwnedWorld::unavailable()); }
    let names = ["_id", "pin", "capture", "manifest", "basis_id", "basis_time", "parent_publication", "mode", "held_start", "until", "schemas", "dropped", "suffix", "ancestors", "fork_lineage"];
    let memory = Arc::clone(&context.memory);
    let Some(mut cursor) = open_private_catalog(context, catalog, &names, None).await? else { return Ok(Vec::new()); };
    let mut worlds = Vec::new();
    let mut reservations = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        for row in 0..batch.num_rows() {
            let mut fields = StructFields::new();
            for name in names {
                let (index, field) = batch.schema_ref().column_with_name(name).ok_or_else(OwnedWorld::unavailable)?;
                let values = batch.column(index).as_ref();
                reservations.push(memory.reserve(rtdb_columnar::estimate_read_value_bytes(field, values, row)?)?);
                fields.insert(name.into(), rtdb_columnar::read_value(field, values, row)?);
            }
            worlds.push(OwnedWorld::from_record(&fields)?);
        }
    }
    Ok(worlds)
}

pub(crate) async fn open_owned_table_point(
    context: Arc<QueryContext>, table: TableRef, locator: Iid, id: Option<&EntityId>,
    valid: Option<Interval>, columns: Option<&[&str]>, history: bool,
) -> Result<Option<QueryCursor>> {
    open_owned_table_point_with_edges(context, table, locator, id, valid, columns, history, history, false).await
}

async fn open_owned_table_point_with_edges(
    context: Arc<QueryContext>, table: TableRef, locator: Iid, id: Option<&EntityId>,
    valid: Option<Interval>, columns: Option<&[&str]>, history: bool, edges: bool, assertion: bool,
) -> Result<Option<QueryCursor>> {
    use crate::context::OwnedSelection;
    let selection = id.cloned().map_or(OwnedSelection::Relation, OwnedSelection::Point);
    open_owned_table_selection(context, table, [locator].into(), selection, valid, columns, history, edges, assertion).await
}

async fn open_owned_table_selection(
    context: Arc<QueryContext>, table: TableRef, locators: BTreeSet<Iid>, selection: crate::context::OwnedSelection,
    valid: Option<Interval>, columns: Option<&[&str]>, history: bool, edges: bool, assertion: bool,
) -> Result<Option<QueryCursor>> {
    let name = QualifiedTable {
        database: context.catalog.default_database().to_owned(),
        table,
    };
    let context = context.for_owned_selection(name.clone(), selection);
    let Some(table) = context.catalog.table(&name)? else { return Ok(None); };
    let mut fields: Vec<&str> = columns.map_or_else(|| table.columns.keys().map(String::as_str).collect(), |names| names.to_vec());
    for required in ["_id", "_system_from"] {
        if !fields.contains(&required) { fields.push(required); }
    }
    if assertion && !fields.contains(&"_assertion_ordinal") {
        fields.push("_assertion_ordinal");
    }
    if assertion && !fields.contains(&rtdb_types::OWNED_EVENT_POSITION_COLUMN) {
        fields.push(rtdb_types::OWNED_EVENT_POSITION_COLUMN);
    }
    if assertion && name.table.table.split_once('-')
        .is_some_and(|(_, relation)| relation == rtdb_types::OWNED_ATTEMPT_RELATION)
    {
        fields.push(rtdb_types::HOSTED_RECEIPT_TX_ID);
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
        iids: Some(locators), predicates: Vec::new(), matches: Vec::new(), top_k: None, ordered_top_k: None,
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

fn typed_owned_cell(cell: rtdb_columnar::ArrowCell<'_>) -> Option<rtdb_columnar::ArrowCell<'_>> {
    let selected = cell.physical().ok()?;
    if cell.field.data_type() != selected.field.data_type() {
        let logical = rtdb_columnar::from_field(cell.field).ok()?;
        let canonical = rtdb_columnar::to_field(cell.field.name(), &logical).ok()?;
        if canonical.data_type() != cell.field.data_type() { return None; }
    }
    Some(selected)
}

#[doc(hidden)]
pub fn owned_micros(cell: rtdb_columnar::ArrowCell<'_>) -> Option<Timestamp> {
    use arrow_array::{Array, TimestampMicrosecondArray};
    let cell = typed_owned_cell(cell)?;
    if cell.field.metadata().contains_key(rtdb_columnar::EXTENSION_NAME_KEY) { return None; }
    let array = cell.array.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
    array.is_valid(cell.row).then(|| Timestamp(array.value(cell.row)))
}

/// Interprets the winning revision only after checking the complete stored identity.
pub fn owned_row_revision(
    batch: &arrow_array::RecordBatch,
    row: usize,
    expected_id: &rtdb_types::EntityId,
    memory: &Arc<rtdb_columnar::MemoryBudget>,
) -> Result<Timestamp> {
    owned_row_revision_for(batch, row, EntityIdRef::from(expected_id), memory)
}

#[doc(hidden)]
pub fn owned_row_revision_for(
    batch: &arrow_array::RecordBatch, row: usize, expected_id: EntityIdRef<'_>,
    memory: &Arc<rtdb_columnar::MemoryBudget>,
) -> Result<Timestamp> {
    let schema = batch.schema_ref();
    let (id_column, id_field) = schema.column_with_name("_id")
        .ok_or_else(|| Anomaly::fault("rtdb.owned/row-identity", "Owned read omitted its identity column"))?;
    let id = batch.column(id_column).as_ref();
    let bound = rtdb_columnar::estimate_read_value_bytes(id_field, id, row)?;
    let _scratch = memory.reserve(bound.checked_add(4096)
        .ok_or_else(|| Anomaly::fault("rtdb.owned/row-identity", "Owned identity allocation overflow"))?)?;
    let typed = typed_owned_cell(rtdb_columnar::ArrowCell { field: id_field, array: id, row })
        .and_then(|cell| rtdb_columnar::read_owned_entity_id(cell.field, cell.array, cell.row).ok());
    let matches = match typed {
        Some(id) => id == expected_id,
        None => EntityIdRef::from(&rtdb_columnar::read_value(id_field, id, row)?.owned_entity_id()?) == expected_id,
    };
    if !matches {
        return Err(Anomaly::new(rtdb_types::AnomalyCategory::Conflict, "rtdb.owned/iid-collision", "Owned row locator resolves to a different complete identity"));
    }
    let (revision_column, revision_field) = schema.column_with_name("_system_from")
        .ok_or_else(|| Anomaly::fault("rtdb.owned/row-revision", "Owned read omitted its revision column"))?;
    let array = batch.column(revision_column).as_ref();
    if let Some(revision) = owned_micros(rtdb_columnar::ArrowCell { field: revision_field, array, row }) {
        return Ok(revision);
    }
    match rtdb_columnar::read_value(revision_field, array, row)? {
        Value::Timestamp { value, unit: TimeUnit::Microsecond, .. } => Ok(Timestamp(value)),
        _ => Err(Anomaly::fault("rtdb.owned/row-revision", "Owned row revision is not a microsecond timestamp")),
    }
}

/// The source transaction of the winning assertion, read from the event header.
pub fn owned_receipt_transaction(batch: &arrow_array::RecordBatch, row: usize) -> Result<rtdb_types::TxId> {
    use arrow_array::{Array, Int64Array};

    let unavailable = || Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/receipt-position",
        "The retained receipt has no original transaction position");
    let array = batch.column_by_name(rtdb_types::HOSTED_RECEIPT_TX_ID)
        .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
        .filter(|array| row < array.len() && array.is_valid(row)).ok_or_else(unavailable)?;
    let position = array.value(row);
    if position < 0 { return Err(unavailable()); }
    Ok(rtdb_types::TxId(position))
}

pub fn owned_row_assertion_ordinal(batch: &arrow_array::RecordBatch, row: usize) -> Result<Option<NonZeroU32>> {
    use arrow_array::{Array, Int64Array};

    let Some(column) = batch.column_by_name("_assertion_ordinal") else { return Ok(None); };
    let invalid = || Anomaly::fault("rtdb.owned/assertion-ordinal", "Owned read returned an invalid assertion ordinal");
    let array = column.as_any().downcast_ref::<Int64Array>()
        .filter(|array| row < array.len() && array.is_valid(row)).ok_or_else(invalid)?;
    let position = u32::try_from(array.value(row)).map_err(|_| invalid())?;
    Ok(NonZeroU32::new(position))
}

#[derive(Clone, Copy)]
pub enum OwnedSpatialProjection<'a> {
    Identities,
    Documents,
    SelectedDocuments(&'a [&'a str]),
}

impl OwnedSpatialProjection<'_> {
    pub fn has_documents(self) -> bool { !matches!(self, Self::Identities) }

    pub fn includes(self, field: &str) -> bool {
        match self {
            Self::Identities => false,
            Self::Documents => true,
            Self::SelectedDocuments(columns) =>
                matches!(field, "_id" | "_system_from") || columns.contains(&field),
        }
    }
}

pub struct OwnedSpatialScan {
    pub cursor: QueryCursor,
    pub document_columns: Vec<usize>,
}

enum SpatialScanIids {
    Gather(BTreeSet<Iid>),
    Refine(CandidateIids),
}

/// Scans resolved rows of a held owned relation for spatial document gather.
pub async fn open_owned_spatial_scan(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
    geometry: Option<&str>, partition: &[String], valid_at: Timestamp,
    iids: BTreeSet<Iid>, projection: OwnedSpatialProjection<'_>, include_iid: bool,
) -> Result<Option<OwnedSpatialScan>> {
    open_spatial_scan(context, owner, relation, geometry, partition, valid_at,
        SpatialScanIids::Gather(iids), projection, include_iid).await
}

/// Refines a retained physical IID selection against the held owned relation.
pub async fn open_owned_spatial_refinement(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
    geometry: Option<&str>, partition: &[String], valid_at: Timestamp,
    iids: CandidateIids, projection: OwnedSpatialProjection<'_>, include_iid: bool,
) -> Result<Option<OwnedSpatialScan>> {
    open_spatial_scan(context, owner, relation, geometry, partition, valid_at,
        SpatialScanIids::Refine(iids), projection, include_iid).await
}

async fn open_spatial_scan(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
    geometry: Option<&str>, partition: &[String], valid_at: Timestamp,
    iids: SpatialScanIids, projection: OwnedSpatialProjection<'_>, include_iid: bool,
) -> Result<Option<OwnedSpatialScan>> {
    if owner.is_empty() || relation.world().is_some() || relation.is_world_catalog() {
        return Err(address_unavailable());
    }
    let empty = match &iids {
        SpatialScanIids::Gather(iids) => iids.is_empty(),
        SpatialScanIids::Refine(iids) => iids.is_empty(),
    };
    if empty && !projection.has_documents() { return Ok(None); }
    let name = QualifiedTable {
        database: context.catalog.default_database().to_owned(), table: relation.table(owner),
    };
    let (selection, iids) = match iids {
        SpatialScanIids::Gather(iids) => (OwnedSelection::Relation, Some(iids)),
        SpatialScanIids::Refine(iids) => (OwnedSelection::Candidates(iids), None),
    };
    let mut context = context.for_owned_selection(name.clone(), selection);
    Arc::make_mut(&mut context).spatial_refinement = include_iid && !projection.has_documents();
    let Some(table) = context.catalog.table(&name)? else { return Ok(None); };
    let mut fields = vec!["_id"];
    fields.extend(geometry);
    fields.extend(partition.iter().map(String::as_str));
    if include_iid { fields.push("_iid"); }
    if projection.has_documents() {
        for name in table.columns.keys().map(String::as_str).chain(["_system_from"]) {
            if projection.includes(name) && !fields.contains(&name) { fields.push(name); }
        }
    }
    let document_columns = match projection {
        OwnedSpatialProjection::Identities => Vec::new(),
        OwnedSpatialProjection::Documents => (0..fields.len()).collect(),
        OwnedSpatialProjection::SelectedDocuments(_) => {
            let mut indices = vec![0];
            indices.extend(table.columns.keys().filter(|name| {
                name.as_str() != "_id" && name.as_str() != "_system_from" && projection.includes(name)
            }).filter_map(|name| fields.iter().position(|field| *field == name.as_str())));
            if let Some(revision) = fields.iter().position(|field| *field == "_system_from") {
                indices.push(revision);
            }
            indices
        }
    };
    let columns = fields.into_iter().enumerate().map(|(index, name)| Field {
            id: ColumnId(index as u32), name: name.into(), qualifier: None, qualified_only: false,
            unnamed: false, data_type: table.scan_column_type(name),
        }).collect::<Vec<_>>();
    let scan = Arc::new(Relation::scan(Scan {
        table, columns,
        valid_time: TemporalFilter::At(Expr::Literal(Value::Timestamp {
            value: valid_at.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()),
        })),
        system_time: TemporalFilter::Default, clamp_valid_time: false, iids,
        predicates: Vec::new(), matches: Vec::new(), top_k: None, ordered_top_k: None,
    })?);
    let cursor = PreparedQuery::new_in(scan, &context.memory)?.execute(context, Vec::new()).await?;
    Ok(Some(OwnedSpatialScan { cursor, document_columns }))
}

/// Reads the fixed protected path catalog shape for engine maintenance and resolution.
pub async fn open_owned_path_catalog(
    context: Arc<QueryContext>,
    catalog: &TableRef,
    path: Option<&str>,
) -> Result<Option<QueryCursor>> {
    if !catalog.is_owned_path_catalog() && !catalog.is_owned_search_catalog() && !catalog.is_owned_spatial_catalog() && !catalog.is_owned_vector_catalog() {
        return Err(Anomaly::incorrect("rtdb.owned/path-catalog", "Expected an owned path catalog"));
    }
    let fields: &[&str] = if catalog.is_owned_spatial_catalog() {
        &["_id", "schema_name", "table_name", "columns", "version", "geometry_encoding"]
    } else if catalog.is_owned_vector_catalog() {
        &["_id", "schema_name", "table_name", "columns", "version", "dimensions", "recall"]
    } else if catalog.is_owned_search_catalog() {
        // A search path that uses the simple analyzer stores none.
        &["_id", "schema_name", "table_name", "columns", "version", "analyzer"]
    } else {
        &["_id", "schema_name", "table_name", "columns", "version"]
    };
    open_private_catalog(
        context, catalog.clone(), fields,
        path.map(|name| EntityId::String(name.into()).iid()),
    ).await
}

pub async fn open_native_spatial_builds(context: Arc<QueryContext>, id: Option<&str>) -> Result<Option<QueryCursor>> {
    let catalog = TableRef::new("xt", rtdb_types::rtdb_v1::SPATIAL_BUILD_CATALOG);
    open_private_catalog(context, catalog, &[
        "_id", "index_name", "schema_name", "table_name", "columns", "version", "geometry_encoding",
    ], id.map(|id| EntityId::string_iid(id))).await
}

async fn open_private_catalog(
    context: Arc<QueryContext>, catalog: TableRef, fields: &[&str], iid: Option<rtdb_types::Iid>,
) -> Result<Option<QueryCursor>> {
    let ordered = catalog.is_owned_path_catalog();
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
    if ordered {
        query.execute_columns(context, Vec::new()).await.map(Some)
    } else {
        query.execute(context, Vec::new()).await.map(Some)
    }
}

pub async fn read_owned_world_clock(
    context: Arc<QueryContext>, owner: &str, world: Iid,
) -> Result<Option<Timestamp>> {
    Ok(read_owned_world_clock_publication(context, owner, world).await?.map(|(clock, _)| clock))
}

/// Returns the logical reading together with the exact transaction that published it.
pub async fn read_owned_world_clock_publication(
    context: Arc<QueryContext>, owner: &str, world: Iid,
) -> Result<Option<(Timestamp, TransactionKey)>> {
    let unavailable = rtdb_types::OwnedWorld::unavailable;
    let id = EntityId::Uuid(world.0);
    let Some(mut cursor) = open_owned_table_point(
        Arc::clone(&context), OwnedRelation::world_clock_table(owner), world, Some(&id),
        None, Some(&["clock", rtdb_types::OWNED_EVENT_POSITION_COLUMN]), false,
    ).await? else { return Ok(None); };
    let mut clock = None;
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        if batch.num_rows() == 0 { continue; }
        if clock.is_some() || batch.num_rows() != 1 { return Err(unavailable()); }
        let system_time = owned_row_revision(batch, 0, &id, &context.memory)?;
        let position = owned_row_event_position(batch, 0)?;
        let (index, field) = batch.schema_ref().column_with_name("clock").ok_or_else(unavailable)?;
        let Value::I64(reading) = rtdb_columnar::read_value(field, batch.column(index).as_ref(), 0)? else {
            return Err(unavailable());
        };
        if !(Timestamp::MIN.0 < reading && reading < Timestamp::MAX.0) { return Err(unavailable()); }
        clock = Some((Timestamp(reading), TransactionKey { tx_id: position.transaction, system_time }));
    }
    Ok(clock)
}

/// An exact accepted Held clock read at a cut of the original overlay.
pub struct ArchivedHeldClock {
    world: Iid,
    pin: Iid,
    media_end: TransactionKey,
    source_root: TransactionKey,
    publication: TransactionKey,
    reading: Timestamp,
}

impl ArchivedHeldClock {
    pub fn publication(&self) -> TransactionKey { self.publication }

    pub fn reading(
        &self, ancestor: &rtdb_types::OwnedWorldAncestor, system: Timestamp,
    ) -> Result<Timestamp> {
        if self.world.0 != ancestor.world || self.pin.0 != ancestor.pin
            || self.publication.tx_id > self.media_end.tx_id
            || self.media_end != ancestor.basis || self.source_root != ancestor.source_root
            || self.publication.system_time != system
            || !ancestor.covers(self.publication, self.reading)
        {
            return Err(rtdb_types::OwnedWorld::unavailable());
        }
        Ok(self.reading)
    }
}

pub async fn read_archived_held_clock(
    context: &Arc<QueryContext>, owner: &str, world: Iid, through: TransactionKey,
    system: Timestamp, exact: Option<TransactionKey>,
) -> Result<ArchivedHeldClock> {
    let unavailable = rtdb_types::OwnedWorld::unavailable;
    let database = context.catalog.database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else { return Err(unavailable()); };
    let media_end = snapshot.tx_basis().filter(|end| snapshot.system_time_cap() == Some(end.system_time))
        .ok_or_else(unavailable)?;
    let selected = context.with_owned_transaction_cut(through, &context.cancellation)?;
    let publication = if let Some(publication) = exact {
        if publication.system_time != system || !publication.at_or_before(through) {
            return Err(unavailable());
        }
        publication
    } else {
        let at = selected.with_owned_system_cut(system, &selected.cancellation)?;
        read_owned_world_clock_publication(at, owner, world).await?
            .map(|(_, publication)| publication)
            .filter(|publication| publication.system_time == system)
            .ok_or_else(unavailable)?
    };
    let (source, reading, _charge) = read_held_world_publication(&selected, owner, world, publication).await?;
    Ok(ArchivedHeldClock {
        world, pin: source.pin, media_end, source_root: source.basis, publication, reading,
    })
}

/// Open the source registration and the actual clock event at the selected publication.
pub async fn read_held_world_publication(
    context: &Arc<QueryContext>, owner: &str, world: Iid, publication: TransactionKey,
) -> Result<(rtdb_types::OwnedWorld, Timestamp, Arc<rtdb_columnar::MemoryReservation>)> {
    let unavailable = rtdb_types::OwnedWorld::unavailable;
    let at = context.with_owned_transaction_cut(publication, &context.cancellation)?;
    let (source, charge) = read_owned_world(Arc::clone(&at), owner, world).await?
        .filter(|(source, _)| !source.dropped
            && matches!(source.mode, rtdb_types::WorldClockMode::Held { .. }))
        .ok_or_else(unavailable)?;
    let (reading, _) = read_owned_world_clock_publication(at, owner, world).await?
        .filter(|(reading, event)| *event == publication && source.admits_clock_publication(*reading, publication.system_time))
        .ok_or_else(unavailable)?;
    Ok((source, reading, charge))
}

/// Select an accepted Held or Proposal reading within a certified physical prefix.
pub async fn select_owned_world_clock(
    context: &Arc<QueryContext>, owner: &str, world: Iid,
    floor: TransactionKey, initial: Timestamp, requested: Timestamp,
) -> Result<Option<(TransactionKey, Timestamp)>> {
    let unavailable = rtdb_types::OwnedWorld::unavailable;
    let cap = context.owned_held_cut().ok_or_else(unavailable)?;
    let Some((latest, publication)) = read_owned_world_clock_publication(Arc::clone(context), owner, world).await?
    else { return Ok(None); };
    if floor.tx_id.0 < 0 || floor.tx_id >= publication.tx_id || publication.system_time > cap || initial >= latest {
        return Err(unavailable());
    }
    if latest <= requested {
        return Ok(Some((publication, latest)));
    }

    let mut low = floor.tx_id.0;
    let mut high = publication.tx_id.0;
    let mut selected = None::<(TransactionKey, Timestamp)>;
    while high - low > 1 {
        let mid = TxId(low + (high - low) / 2);
        let probe = Arc::clone(context).with_owned_position_cut(mid)?;
        match read_owned_world_clock_publication(probe, owner, world).await? {
            Some((reading, revision)) => {
                if !(floor.tx_id < revision.tx_id && revision.tx_id <= mid && initial < reading && reading < latest)
                    || selected.is_some_and(|(prior, prior_reading)| revision.tx_id < prior.tx_id || reading < prior_reading
                        || (revision == prior) != (reading == prior_reading))
                {
                    return Err(unavailable());
                }
                if reading <= requested {
                    selected = Some((revision, reading));
                    low = mid.0;
                } else {
                    high = revision.tx_id.0;
                    if high <= low { return Err(unavailable()); }
                }
            }
            None if selected.is_none() => low = mid.0,
            None => return Err(unavailable()),
        }
    }
    Ok(selected)
}

pub async fn read_owned_world(
    context: Arc<QueryContext>, owner: &str, world: rtdb_types::Iid,
) -> Result<Option<(rtdb_types::OwnedWorld, Arc<rtdb_columnar::MemoryReservation>)>> {
    Ok(read_owned_world_with_revision(context, owner, world)
        .await?.map(|(record, _, retained)| (record, retained)))
}

pub async fn read_owned_world_at(
    context: Arc<QueryContext>, owner: &str, world: rtdb_types::Iid, system: Timestamp,
) -> Result<Option<(rtdb_types::OwnedWorld, Timestamp, Arc<rtdb_columnar::MemoryReservation>)>> {
    read_owned_world_with_revision(owned_context_at_system(context, system)?, owner, world).await
}

pub async fn read_owned_world_with_revision(
    context: Arc<QueryContext>, owner: &str, world: rtdb_types::Iid,
) -> Result<Option<(rtdb_types::OwnedWorld, Timestamp, Arc<rtdb_columnar::MemoryReservation>)>> {
    use rtdb_types::{OwnedWorld, StructFields};

    let memory = Arc::clone(&context.memory);
    let id = EntityId::Uuid(world.0);
    let names = ["_id", "pin", "capture", "manifest", "basis_id", "basis_time", "parent_publication", "mode", "held_start", "until", "schemas", "dropped", "suffix", "ancestors", "fork_lineage"];
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
            let registered = owned_row_revision(batch, row, &id, &memory)?;
            record = Some((decoded, registered, retained));
        }
    }
    Ok(record)
}

pub async fn read_owned_root_publication(
    context: Arc<QueryContext>, owner: &str,
) -> Result<Option<TransactionKey>> {
    let relation = OwnedRelation::new(rtdb_types::OWNED_ROOT_PUBLICATION_RELATION)?;
    let id = EntityId::String("head".into());
    Ok(read_owned_point_version(context, owner, &relation, &id, Interval::ALL).await?
        .map(|point| TransactionKey { tx_id: point.position.transaction, system_time: point.history }))
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
    pub publication: TransactionKey,
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
    read_owned_journal_cell(context, owner, relation, revision, "fixed_assertion_keys").await
}

async fn read_owned_journal_cell(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, revision: i64, cell: &str,
) -> Result<OwnedChangeEntry> {
    use rtdb_types::change_feed_uncovered;

    let id = EntityId::Integer(revision);
    let memory = Arc::clone(&context.memory);
    let journal = relation.change_journal(owner)?;
    let Some(mut cursor) = open_owned_table_point(context, journal, id.iid(), Some(&id), None, Some(&[cell, "through_events", rtdb_types::OWNED_EVENT_POSITION_COLUMN]), false).await? else {
        return Err(change_feed_uncovered());
    };
    let mut entry = None;
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        for row in 0..batch.num_rows() {
            if entry.is_some() { return Err(change_feed_uncovered()); }
            let system_time = owned_row_revision(batch, row, &id, &memory)?;
            let position = owned_row_event_position(batch, row)?;
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
                publication: TransactionKey { tx_id: position.transaction, system_time },
                through_events: through, value: rtdb_columnar::read_value(field, values, row)?, retained,
            });
        }
    }
    entry.ok_or_else(change_feed_uncovered)
}

#[cfg(test)]
mod owned_point_revision_tests {
    use super::*;
    use arrow_array::{Array, RecordBatch, TimestampMicrosecondArray};
    use arrow_schema::{Field, Schema};
    use rtdb_columnar::{ArrowCell, MemoryBudget};

    #[test]
    fn owned_point_revision_reads_a_selected_union_id_and_checks_the_complete_key() {
        let ids = [
            rtdb_columnar::EntityIdRef::Integer(-1),
            rtdb_columnar::EntityIdRef::String("key"),
        ];
        let array = rtdb_columnar::entity_id_array(ids).unwrap();
        let revisions = TimestampMicrosecondArray::from(vec![Some(3), Some(7)]);
        let schema = Arc::new(Schema::new(vec![
            rtdb_columnar::to_field("_id", &rtdb_columnar::entity_id_type()).unwrap(),
            Field::new("_system_from", revisions.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![array, Arc::new(revisions)]).unwrap().slice(1, 1);
        let memory = MemoryBudget::new(1 << 20);
        assert_eq!(owned_micros(ArrowCell::named(&batch, "_system_from", 0).unwrap()), Some(Timestamp(7)));
        assert_eq!(owned_row_revision(&batch, 0, &EntityId::String("key".into()), &memory).unwrap(), Timestamp(7));
        assert_eq!(owned_row_revision(&batch, 0, &EntityId::Integer(-1), &memory).unwrap_err().code, "rtdb.owned/iid-collision");
        assert_eq!(memory.used_bytes(), 0);
    }
}
