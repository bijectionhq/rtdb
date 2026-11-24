use std::sync::Arc;

use rtdb_columnar::{ArrowCell, EntityIdRef};
use rtdb_query::QueryContext;
use rtdb_storage::sealing::PlaintextView;
use rtdb_types::{Anomaly, AnomalyCategory, EntityId, Interval, OwnedRelation, Result, Timestamp};

pub(crate) async fn needs_rank_family(
    context: &Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
) -> Result<bool> {
    if !PlaintextView::tracks_rank_inventory() || !relation.name().ends_with("_search") {
        return Ok(false);
    }
    let (schema, _) = rtdb_query::read_owned_relation_schema(Arc::clone(context), owner, relation).await?
        .ok_or_else(unavailable)?;
    if relation.admits_world_ciphertext_points(&schema) { return Ok(true); }
    if schema.has_opaque_columns() { return Err(unavailable()); }
    Ok(false)
}

pub(crate) async fn root_contributor(
    context: &Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
    id: EntityIdRef<'_>, valid: Timestamp,
) -> Result<()> {
    use rtdb_query::owned_row_revision;

    let EntityIdRef::String(key) = id else { return Err(unavailable()); };
    if key.is_empty() || key.len() > 512 { return Err(unavailable()); }
    PlaintextView::charge_rank_carrier()?;
    let carrier_relation = OwnedRelation::new("fact_unit_column_payloads")?;
    let carrier = EntityId::String(format!("row/{key}"));
    let Some(mut cursor) = rtdb_query::open_owned_assertion_point(
        Arc::clone(context), owner, &carrier_relation, &carrier, Some(Interval::at(valid)?), false,
    ).await? else { return Err(unavailable()); };
    let mut selected = None;
    while let Some(page) = cursor.next_batch().await? {
        if page.batch().num_rows() == 0 { continue; }
        if page.batch().num_rows() != 1 || selected.replace(page).is_some() { return Err(unavailable()); }
    }
    let page = selected.ok_or_else(unavailable)?;
    let row = page.batch();
    let payload = ArrowCell::named(row, "sealed_payload", 0).and_then(ArrowCell::binary).ok_or_else(unavailable)?;
    if payload.starts_with(b"BJRP1") {
        return Ok(());
    }
    let body = payload.strip_prefix(b"BJRL1").ok_or_else(unavailable)?;
    let (family, body) = body.split_first_chunk::<16>().ok_or_else(unavailable)?;
    let (stage, body) = body.split_first_chunk::<40>().ok_or_else(unavailable)?;
    let (member, body) = body.split_first_chunk::<32>().ok_or_else(unavailable)?;
    let (_, body) = body.split_first_chunk::<{ rtdb_types::RootLandingTarget::WIDTH }>().ok_or_else(unavailable)?;
    let (link, envelope) = body.split_first_chunk::<32>().ok_or_else(unavailable)?;
    if *family == [0; 16] || *link == [0; 32] || envelope.is_empty() { return Err(unavailable()); }

    let publication = u64::try_from(owned_row_revision(row, 0, &carrier, &context.memory)?.0)
        .map_err(|_| unavailable())?;
    let ordinal = rtdb_query::owned_row_assertion_ordinal(row, 0)?.ok_or_else(unavailable)?;
    let mut entry = Vec::with_capacity(109 + relation.name().len() + key.len());
    entry.push(0);
    entry.extend_from_slice(family);
    entry.extend_from_slice(member);
    entry.extend_from_slice(stage);
    entry.extend_from_slice(&publication.to_be_bytes());
    entry.extend_from_slice(&u64::from(ordinal.get()).to_be_bytes());
    for field in [relation.name().as_bytes(), key.as_bytes()] {
        entry.extend_from_slice(&(field.len() as u16).to_be_bytes());
        entry.extend_from_slice(field);
    }
    PlaintextView::track_rank_contributor(&entry)
}

fn unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/rank-custody",
        "The held rank cannot authenticate its original Root carrier")
}
