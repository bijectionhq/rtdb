//! The protected decision reads only installed scoped membership metadata.

use super::*;

pub(super) const PROTECTED_MEMBERSHIP: &str = "protected_membership";
const FENCES: &str = "fact_unit_fences";
const CARRIERS: &str = "fact_unit_column_payloads";
const UNTIL: &str = "membership_until";

pub(super) enum ProtectedProjection<'a> {
    Rows(&'a str),
    SharedUnit { carriers: &'a str, fence: &'a str },
}

fn lowercase_hex(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

impl<'a> ProtectedProjection<'a> {
    pub(super) fn decode(encoded: &'a [u8]) -> Result<Self> {
        let (&kind, encoded) = encoded.split_first().ok_or_else(uncovered)?;
        let image = std::str::from_utf8(encoded).map_err(|_| uncovered())?;
        match kind {
            0 if image.len() == 32 && lowercase_hex(image) => Ok(Self::Rows(image)),
            1 => {
                let (carriers, fence) = image.split_once('\0').ok_or_else(uncovered)?;
                if carriers.len() != 212 || !lowercase_hex(carriers)
                    || !carriers.starts_with("4246435032") || fence.len() != 64 || !lowercase_hex(fence)
                {
                    return Err(uncovered());
                }
                Ok(Self::SharedUnit { carriers, fence })
            }
            _ => Err(uncovered()),
        }
    }

    pub(super) async fn count(&self, context: Arc<QueryContext>, owner: &str, journal: &OwnedRelation, ordered: Timestamp) -> Result<i64> {
        let fences = OwnedRelation::new(FENCES)?;
        let carriers = OwnedRelation::new(CARRIERS)?;
        match self {
            Self::Rows(table) => {
                let id_prefix = format!("row/0b{table}");
                let mut count = 0i64;
                if let Some(mut cursor) = rtdb_query::open_owned_root_prefix_projection(
                    Arc::clone(&context), owner, &carriers, ordered, &["_id"], &id_prefix,
                ).await? {
                    while let Some(batch) = cursor.next_batch().await? {
                        let rows = batch.batch();
                        for row in 0..rows.num_rows() {
                            context.check_cancelled_at(row)?;
                            let EntityId::String(id) = occupancy_value_at(&context.memory, rows, row, "_id", uncovered)?
                                .entity_id()? else { return Err(uncovered()); };
                            let position = row_membership_id(&id_prefix, &id)?;
                            if !point_exists(&context, owner, &fences, &EntityId::String(id), ordered).await?
                                && position_survives(&context, owner, journal, &EntityId::String(position), ordered).await?
                            {
                                count = count.checked_add(1).ok_or_else(overflow)?;
                            }
                        }
                    }
                }
                Ok(count)
            }
            Self::SharedUnit { carriers: prefix, fence } => {
                if point_exists(&context, owner, &fences, &EntityId::String((*fence).to_owned()), ordered).await? {
                    return Ok(0);
                }
                let Some(mut cursor) = rtdb_query::open_owned_root_prefix_projection(
                    Arc::clone(&context), owner, &carriers, ordered, &["_id"], prefix,
                ).await? else { return Ok(0); };
                let mut count = 0i64;
                while let Some(batch) = cursor.next_batch().await? {
                    count = count.checked_add(i64::try_from(batch.batch().num_rows()).map_err(|_| overflow())?)
                        .ok_or_else(overflow)?;
                }
                Ok(count)
            }
        }
    }
}

fn row_membership_id(table_prefix: &str, carrier: &str) -> Result<String> {
    if !carrier.starts_with(table_prefix) {
        return Err(uncovered());
    }
    Ok(format!("row-position/{carrier}"))
}

async fn point_exists(
    context: &Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId, ordered: Timestamp,
) -> Result<bool> {
    let upper = Timestamp(ordered.0.checked_add(1).ok_or_else(overflow)?);
    let Some(mut cursor) = rtdb_query::open_owned_point(
        Arc::clone(context), owner, relation, id, Some(Interval::new(ordered, upper)?), Some(&["_id"]),
    ).await? else { return Ok(false); };
    while let Some(batch) = cursor.next_batch().await? {
        if batch.batch().num_rows() != 0 { return Ok(true); }
    }
    Ok(false)
}

async fn position_survives(
    context: &Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId, ordered: Timestamp,
) -> Result<bool> {
    let upper = Timestamp(ordered.0.checked_add(1).ok_or_else(overflow)?);
    let mut cursor = rtdb_query::open_owned_point(
        Arc::clone(context), owner, relation, id, Some(Interval::new(ordered, upper)?), Some(&["_id", UNTIL]),
    ).await?.ok_or_else(uncovered)?;
    let mut membership = None;
    while let Some(batch) = cursor.next_batch().await? {
        let rows = batch.batch();
        for row in 0..rows.num_rows() {
            let Value::Binary(until) = occupancy_value_at(&context.memory, rows, row, UNTIL, uncovered)?
                else { return Err(uncovered()); };
            let until = i64::from_be_bytes(until.try_into().map_err(|_| uncovered())?);
            if until <= 0 || membership.replace(ordered.0 < until).is_some() { return Err(uncovered()); }
        }
    }
    membership.ok_or_else(uncovered)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sealed_membership_names_the_same_incarnation_as_the_carrier() -> Result<()> {
        let prefix = "row/0b11111111111111111111111111111111";
        let carrier = format!("{prefix}cafe");
        assert_eq!(row_membership_id(prefix, &carrier)?, format!("row-position/{carrier}"));
        assert!(row_membership_id("row/0b22222222222222222222222222222222", &carrier).is_err());
        Ok(())
    }
}
