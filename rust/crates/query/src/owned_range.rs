//! Validate a bounded range against its original event frontier and later ordered changes.

use std::sync::Arc;

use rtdb_index::Snapshot;
use rtdb_types::{
    FixedRangeEvidence, FixedRangeKey, OwnedChange, OwnedEntityRegion, OwnedRelation, Result,
    TransactionKey, Value, FIXED_RANGE_MAX_GROUPS, FIXED_RANGE_MAX_IDS, OWNED_CHANGE_PAGE_GROUPS,
};

use crate::{QueryContext, owned_range_order::ordered_range_group};

fn ordered_group(previous: TransactionKey, group: TransactionKey, head: TransactionKey) -> bool {
    let coordinates = |key: TransactionKey| (key.tx_id.0, key.system_time.0);
    ordered_range_group(coordinates(previous), coordinates(group), coordinates(head))
}

pub async fn verify_owned_fixed_range(
    context: &Arc<QueryContext>,
    base: &Snapshot,
    owner: &str,
    relation: &OwnedRelation,
    basis: TransactionKey,
    frontier: i64,
    head_frontier: i64,
    evidence: &FixedRangeEvidence,
) -> Result<bool> {
    let journal_frontier = base.relation_event_frontier(&relation.change_journal(owner)?)?;
    let Some(head) = journal_frontier.checked_sub(1) else {
        return Ok(false);
    };
    if evidence.revision < 0
        || evidence.revision >= head
        || head - evidence.revision > FIXED_RANGE_MAX_GROUPS
    {
        return Ok(false);
    }
    let rule =
        crate::read_owned_fixed_range_rule(Arc::clone(context), owner, relation).await?;
    if rule.as_ref() != Some(&evidence.rule) {
        return Ok(false);
    }
    let anchor = crate::read_owned_change_entry(
        Arc::clone(context),
        owner,
        relation,
        evidence.revision,
    )
    .await?;
    if anchor.through_events != frontier
        || (evidence.revision == 0 && (frontier != 0 || anchor.value != Value::Null))
        || (evidence.revision != 0
            && (!anchor.publication.at_or_before(basis)
                || !matches!(anchor.value, Value::List(touches) if !touches.is_empty())))
    {
        return Ok(false);
    }
    let mut through = frontier;
    let mut previous = basis;
    let Some(head_publication) = base.tx_basis() else {
        return Ok(false);
    };
    for revision in evidence.revision + 1..=head {
        context.check_cancelled()?;
        let entry = crate::read_owned_fixed_range_changes(
            Arc::clone(context),
            owner,
            relation,
            revision,
        )
        .await?;
        let Value::Struct(fields) = entry.value else {
            return Ok(false);
        };
        let (Some(Value::I64(events)), Some(Value::List(sides))) =
            (fields.get("event_count"), fields.get("sides"))
        else {
            return Ok(false);
        };
        if *events <= 0
            || !ordered_group(previous, entry.publication, head_publication)
            || through.checked_add(*events) != Some(entry.through_events)
            || sides.len() > FIXED_RANGE_MAX_IDS
        {
            return Ok(false);
        }
        for side in sides.iter() {
            let Value::List(pair) = side.as_ref() else {
                return Ok(false);
            };
            if pair.len() != 2 {
                return Ok(false);
            }
            for key in pair.iter() {
                if *key != Value::Null && evidence.contains(&FixedRangeKey::from_value(key.as_ref())?) {
                    return Ok(false);
                }
            }
        }
        through = entry.through_events;
        previous = entry.publication;
    }
    Ok(through == head_frontier)
}

pub async fn verify_owned_entity_region(
    context: &Arc<QueryContext>,
    base: &Snapshot,
    owner: &str,
    relation: &OwnedRelation,
    basis: TransactionKey,
    frontier: i64,
    head_frontier: i64,
    anchor: i64,
    region: &OwnedEntityRegion,
) -> Result<bool> {
    let journal = relation.change_journal(owner)?;
    let head = base.relation_event_frontier(&journal)?.saturating_sub(1);
    if region.valid.is_empty()
        || anchor < 0
        || anchor >= head
        || head - anchor > OWNED_CHANGE_PAGE_GROUPS as i64
    {
        return Ok(false);
    }
    let Some(head_publication) = base.tx_basis() else {
        return Ok(false);
    };
    let entry = crate::read_owned_change_entry(Arc::clone(context), owner, relation, anchor).await?;
    let anchored = if anchor == 0 {
        frontier == 0 && entry.value == Value::Null
    } else {
        entry.publication.at_or_before(basis) && matches!(entry.value, Value::List(touches) if !touches.is_empty())
    };
    if !anchored || entry.through_events != frontier {
        return Ok(false);
    }

    let mut previous = basis;
    let mut through = frontier;
    for revision in anchor + 1..=head {
        context.check_cancelled()?;
        let entry = crate::read_owned_change_entry(Arc::clone(context), owner, relation, revision).await?;
        let Value::List(touches) = entry.value else { return Ok(false); };
        let count = i64::try_from(touches.len()).map_err(|_| rtdb_types::change_feed_uncovered())?;
        if count == 0 || !ordered_group(previous, entry.publication, head_publication)
            || through.checked_add(count) != Some(entry.through_events)
        {
            return Ok(false);
        }
        for touch in touches.iter() {
            if region.intersects(&OwnedChange::from_value(touch.as_ref())?) {
                return Ok(false);
            }
        }
        previous = entry.publication;
        through = entry.through_events;
    }
    Ok(through == head_frontier)
}
