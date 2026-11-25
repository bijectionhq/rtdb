//! Validate a bounded range against its original event frontier and later ordered changes.

use std::sync::Arc;

use rtdb_index::Snapshot;
use rtdb_types::{
    FixedRangeEvidence, FixedRangeKey, Interval, OwnedChange, OwnedChangeKind, OwnedEntityRegion, OwnedRelation, OwnedRevealed, Result,
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
        let entry = crate::read_owned_change_entry(
            Arc::clone(context),
            owner,
            relation,
            revision,
        )
        .await?;
        let Value::List(touches) = entry.value else {
            return Ok(false);
        };
        let events = i64::try_from(touches.len()).map_err(|_| rtdb_types::change_feed_uncovered())?;
        if events == 0 || !ordered_group(previous, entry.publication, head_publication)
            || through.checked_add(events) != Some(entry.through_events)
        {
            return Ok(false);
        }
        let keys = crate::read_owned_fixed_range_changes(Arc::clone(context), owner, relation, revision).await?;
        if keys.publication != entry.publication || keys.through_events != entry.through_events {
            return Ok(false);
        }
        let Value::Struct(fields) = keys.value else {
            return Ok(false);
        };
        let (Some(Value::I64(events)), Some(Value::List(sides))) =
            (fields.get("event_count"), fields.get("sides"))
        else {
            return Ok(false);
        };
        if fields.len() != 2 || usize::try_from(*events).ok() != Some(touches.len())
            || sides.len() > FIXED_RANGE_MAX_IDS
        {
            return Ok(false);
        }
        let mut sides = sides.iter();
        for (slot, touch) in touches.iter().enumerate() {
            let touch = OwnedChange::from_value(touch.as_ref())?;
            if !touch.valid.contains(evidence.rule.valid_at) {
                continue;
            }
            let Some(side) = sides.next() else {
                return Ok(false);
            };
            let Value::Struct(side) = side.as_ref() else {
                return Ok(false);
            };
            let (Some(Value::I64(event_slot)), Some(Value::I64(valid_at)), Some(before), Some(after)) =
                (side.get("event_slot"), side.get("valid_at"), side.get("before"), side.get("after"))
            else {
                return Ok(false);
            };
            if side.len() != 4 || usize::try_from(*event_slot).ok() != Some(slot)
                || *valid_at != evidence.rule.valid_at.0
            {
                return Ok(false);
            }
            for key in [before, after] {
                if *key != Value::Null {
                    let key = FixedRangeKey::from_value(key)?;
                    if !key.belongs_to(&touch.id) || evidence.contains(&key) {
                        return Ok(false);
                    }
                }
            }
        }
        if sides.next().is_some() {
            return Ok(false);
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

/// Validate a range by what its reader revealed after its relation frontier
/// moved. Each later change must leave the touched row's revealed projection
/// the same at the reader's cut and now, so a change to columns the reader
/// could not open keeps the range valid; a row entering, leaving or changing
/// visibly aborts it. With a clock interval, a touched row must also keep one
/// projection throughout it. Erasure, redaction and expiry always count as changes.
pub async fn verify_owned_revealed_range(
    context: &Arc<QueryContext>,
    base: &Snapshot,
    owner: &str,
    relation: &OwnedRelation,
    basis: TransactionKey,
    frontier: i64,
    head_frontier: i64,
    revealed: &OwnedRevealed,
) -> Result<bool> {
    let journal = relation.change_journal(owner)?;
    let head = base.relation_event_frontier(&journal)?.saturating_sub(1);
    let Some(head_publication) = base.tx_basis() else {
        return Ok(false);
    };
    // The newest journal entry at the reader's frontier anchors the walk.
    let mut anchor = None;
    for revision in (head.saturating_sub(OWNED_CHANGE_PAGE_GROUPS as i64).max(0)..=head).rev() {
        context.check_cancelled()?;
        let entry = crate::read_owned_change_entry(Arc::clone(context), owner, relation, revision).await?;
        if entry.through_events < frontier { break; }
        if entry.through_events != frontier { continue; }
        let anchored = if revision == 0 {
            frontier == 0 && entry.value == Value::Null
        } else {
            entry.publication.at_or_before(basis) && matches!(entry.value, Value::List(ref touches) if !touches.is_empty())
        };
        if anchored { anchor = Some(revision); }
        break;
    }
    let Some(anchor) = anchor.filter(|anchor| *anchor < head) else {
        return Ok(false);
    };
    let then = context.with_owned_system_cut(basis.system_time, &context.cancellation)?;
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
            let touch = OwnedChange::from_value(touch.as_ref())?;
            if matches!(touch.kind, OwnedChangeKind::Erase | OwnedChangeKind::Redacted | OwnedChangeKind::Expired(_)) {
                return Ok(false);
            }
            let original = crate::owned_revealed_timeline(
                Arc::clone(&then), owner, relation, &touch.id, touch.valid, &revealed.hidden,
            ).await?;
            let current = crate::owned_revealed_timeline(
                Arc::clone(context), owner, relation, &touch.id, touch.valid, &revealed.hidden,
            ).await?;
            let (Some(original), Some(current)) = (original, current) else { return Ok(false); };
            if original != current { return Ok(false); }
            if let Some(clock) = revealed.clock {
                let lower = clock.lower().max(touch.valid.lower());
                let upper = clock.upper().min(touch.valid.upper());
                if lower < upper && crate::owned_revealed_constant(&current, Interval::new(lower, upper)?).is_none() {
                    return Ok(false);
                }
            }
        }
        previous = entry.publication;
        through = entry.through_events;
    }
    Ok(through == head_frontier)
}
