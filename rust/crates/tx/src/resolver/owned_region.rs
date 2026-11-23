use rtdb_types::{OwnedChange, OwnedEntityRegion, OwnedRelation, OWNED_CHANGE_PAGE_GROUPS};

use super::*;

impl OpenTransaction<'_> {
    pub(super) async fn verify_owned_entity_region(
        &self,
        base: &Snapshot,
        owner: &str,
        relation: &OwnedRelation,
        basis: Timestamp,
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
        let Some(head_time) = base.tx_basis().map(|key| key.system_time) else {
            return Ok(false);
        };
        let context = self.context.query_context(
            base.clone(), self.tx_key().system_time, &self.options.default_tz, self.trace_parent,
        )?;
        let entry = rtdb_query::read_owned_change_entry(Arc::clone(&context), owner, relation, anchor).await?;
        let anchored = if anchor == 0 {
            frontier == 0 && entry.value == Value::Null
        } else {
            entry.system_time <= basis && matches!(entry.value, Value::List(touches) if !touches.is_empty())
        };
        if !anchored || entry.through_events != frontier {
            return Ok(false);
        }

        let mut previous_time = basis;
        let mut through = frontier;
        for revision in anchor + 1..=head {
            self.context.check_cancelled()?;
            let entry = rtdb_query::read_owned_change_entry(Arc::clone(&context), owner, relation, revision).await?;
            let Value::List(touches) = entry.value else { return Ok(false); };
            let count = i64::try_from(touches.len()).map_err(|_| rtdb_types::change_feed_uncovered())?;
            if count == 0 || entry.system_time <= previous_time || entry.system_time > head_time
                || through.checked_add(count) != Some(entry.through_events)
            {
                return Ok(false);
            }
            for touch in touches.iter() {
                if region.intersects(&OwnedChange::from_value(touch.as_ref())?) {
                    return Ok(false);
                }
            }
            previous_time = entry.system_time;
            through = entry.through_events;
        }
        Ok(through == head_frontier)
    }
}
