use rtdb_types::{
    EntityId, FixedRangeKey, FixedRangeRule, OwnedRelation,
};

use super::*;

pub(super) const MAX_SIDE_BYTES: usize = 256 * 1024;

impl OpenTransaction<'_> {
    pub(super) async fn declare_fixed_range(
        &mut self,
        relation: &OwnedRelation,
        rule: &FixedRangeRule,
    ) -> Result<()> {
        OwnedRelation::new(relation.name())?;
        let rule = FixedRangeRule::from_value(&rule.to_value())?;
        let owner = self
            .options
            .user
            .as_deref()
            .filter(|owner| !owner.is_empty())
            .ok_or_else(no_owned_principal)?;
        let storage = relation.fixed_range_table(owner);
        let snapshot = self.snapshot()?;
        let table = relation.table(owner);
        if snapshot.contains_table(&storage) {
            let context = self.context.query_context(
                snapshot, self.tx_key().system_time, &self.options.default_tz, self.trace_parent,
            )?;
            let current = rtdb_query::read_owned_fixed_range_rule(context, owner, relation).await?;
            return if current.as_ref() == Some(&rule) {
                Ok(())
            } else {
                Err(Anomaly::incorrect(
                    "rtdb.owned/fixed-range-bootstrap",
                    "The owned fixed range already has another rule",
                ))
            };
        }
        if snapshot.relation_event_frontier(&table)? != 0
            || !snapshot.trie_entries(&table).is_empty()
        {
            return Err(Anomaly::incorrect(
                "rtdb.owned/fixed-range-bootstrap",
                "A fixed range requires a new rule and no prior fact events",
            ));
        }
        let schema = self.read_owned_schema(owner, relation).await?;
        if schema
            .as_ref()
            .and_then(|(schema, _)| schema.path_column_type(&rule.column))
            != Some(ScalarType::Binary)
        {
            return Err(Anomaly::incorrect(
                "rtdb.owned/fixed-range-column",
                "A fixed range requires an authored Binary column",
            ));
        }
        let path = OwnedRelation::new(rule.path.as_str())?;
        self.declare_owned_path(relation, &path, &[rule.column.clone(), "_id".into()], OwnedPathKind::Ordered)
            .await?;
        self.buffer.put(
            storage,
            StructFields::from([
                ("_id".into(), Value::Utf8("rule".into())),
                ("fixed_rule".into(), rule.to_value()),
            ]),
            Interval::ALL,
        )
    }

    pub(super) async fn fixed_range_before(
        &self, owner: &str, relation: &OwnedRelation, id: &EntityId, valid: Interval,
    ) -> Result<Option<(FixedRangeRule, Value)>> {
        let rules = relation.fixed_range_table(owner);
        if !self.buffer.base_snapshot().contains_table(&rules) && !self.buffer.has_pending_table(&rules) {
            return Ok(None);
        }
        let context = self.context.query_context(
            self.snapshot()?, self.tx_key().system_time, &self.options.default_tz, self.trace_parent,
        )?;
        let rule = rtdb_query::read_owned_fixed_range_rule(context, owner, relation).await?
            .ok_or_else(rtdb_types::change_feed_uncovered)?;
        if !valid.contains(rule.valid_at) {
            return Ok(None);
        }
        let previous = self.owned_changes.get(relation.name())
            .and_then(|group| group.fixed_sides.as_ref())
            .and_then(|sides| sides.iter().rev().find_map(|(_, side)| (&side.id == id).then_some(side)));
        let before = match previous {
            Some(side) => side.after.clone(),
            None => self.fixed_key_at(
                self.buffer.base_snapshot(), owner, relation, id, &rule.column, Interval::at(rule.valid_at)?,
            ).await?,
        };
        Ok(Some((rule, before)))
    }

    pub(super) fn fixed_range_pair(
        &self, id: &EntityId, before: Result<Option<(FixedRangeRule, Value)>>,
        projected: impl FnOnce(&str) -> Result<Option<FixedRangeKey>>,
    ) -> Result<Option<(super::FixedRangeSide, Arc<MemoryReservation>)>> {
        let Some((rule, before)) = before? else { return Ok(None); };
        let bytes = match id {
            EntityId::String(text) => text.len(),
            EntityId::Binary(bytes) => bytes.len(),
            _ => return Err(rtdb_types::change_feed_uncovered()),
        };
        if bytes > rtdb_types::FIXED_RANGE_COMPONENT_BYTES {
            return Err(rtdb_types::change_feed_uncovered());
        }
        let after = projected(&rule.column)?.map_or(Value::Null, |key| key.to_value());
        let size = before.retained_bytes()?.checked_add(after.retained_bytes()?)
            .and_then(|size| size.checked_add(bytes))
            .and_then(|size| size.checked_add(512)).ok_or_else(overflow)?;
        if size > MAX_SIDE_BYTES {
            return Err(rtdb_types::change_feed_uncovered());
        }
        let charge = self.context.memory.reserve(size)?;
        Ok(Some((super::FixedRangeSide {
            id: id.clone(), valid_at: rule.valid_at, before, after,
        }, charge)))
    }

    async fn fixed_key_at(
        &self,
        snapshot: Snapshot,
        owner: &str,
        relation: &OwnedRelation,
        id: &EntityId,
        column: &str,
        valid: Interval,
    ) -> Result<Value> {
        let context = self.context.query_context(
            snapshot,
            self.tx_key().system_time,
            &self.options.default_tz,
            self.trace_parent,
        )?;
        let cursor = rtdb_query::open_owned_point(
            context,
            owner,
            relation,
            id,
            Some(valid),
            Some(&[column]),
        )
        .await?;
        let Some(value) = self
            .read_occupancy_cell(cursor, id, column, rtdb_types::change_feed_uncovered)
            .await?
        else {
            return Ok(Value::Null);
        };
        Ok(FixedRangeKey::from_cells(&value, id)?.to_value())
    }
}
