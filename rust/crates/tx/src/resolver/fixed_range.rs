use std::collections::BTreeSet;

use rtdb_types::{
    EntityId, FIXED_RANGE_MAX_GROUPS, FIXED_RANGE_MAX_IDS, FixedRangeEvidence, FixedRangeKey,
    FixedRangeRule, OwnedChange, OwnedRelation,
};

use super::*;

const MAX_SIDE_BYTES: usize = 256 * 1024;

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

    pub(super) async fn fixed_range_keys(
        &self,
        base: &Snapshot,
        next: &Snapshot,
        owner: &str,
        relation: &OwnedRelation,
        touches: &[Value],
    ) -> Result<Option<(Value, Vec<Arc<MemoryReservation>>)>> {
        let context = self.context.query_context(
            next.clone(),
            self.tx_key().system_time,
            &self.options.default_tz,
            self.trace_parent,
        )?;
        let Some(rule) = rtdb_query::read_owned_fixed_range_rule(context, owner, relation).await?
        else {
            return Ok(None);
        };

        let mut ids = BTreeSet::new();
        let mut reservations = Vec::new();
        for touch in touches {
            let touch = OwnedChange::from_value(touch)?;
            if !touch.valid.contains(rule.valid_at) || ids.contains(&touch.id) {
                continue;
            }
            if ids.len() == FIXED_RANGE_MAX_IDS
                || !matches!(&touch.id, EntityId::String(id) if id.len() <= rtdb_types::FIXED_RANGE_COMPONENT_BYTES)
            {
                return Ok(None);
            }
            reservations.push(
                self.context.memory.reserve(
                    touch
                        .id
                        .as_value()
                        .retained_bytes()?
                        .checked_add(512)
                        .ok_or_else(overflow)?,
                )?,
            );
            ids.insert(touch.id);
        }
        let valid = Interval::at(rule.valid_at)?;
        let mut sides = Vec::with_capacity(ids.len());
        let mut bytes = 0usize;
        for id in ids {
            let before = self
                .fixed_key_at(base.clone(), owner, relation, &id, &rule.column, valid)
                .await?;
            let after = self
                .fixed_key_at(next.clone(), owner, relation, &id, &rule.column, valid)
                .await?;
            let pair = Value::list(vec![before, after]);
            let size = pair
                .retained_bytes()?
                .checked_add(128)
                .ok_or_else(overflow)?;
            bytes = bytes.checked_add(size).ok_or_else(overflow)?;
            if bytes > MAX_SIDE_BYTES {
                return Ok(None);
            }
            reservations.push(self.context.memory.reserve(size)?);
            sides.push(pair);
        }
        let event_count = i64::try_from(touches.len()).map_err(|_| overflow())?;
        Ok(Some((
            Value::Struct(StructFields::from([
                ("event_count".into(), Value::I64(event_count)),
                ("sides".into(), Value::list(sides)),
            ])),
            reservations,
        )))
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

    pub(super) async fn verify_fixed_range(
        &self,
        base: &Snapshot,
        owner: &str,
        relation: &OwnedRelation,
        basis: Timestamp,
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
        let context = self.context.query_context(
            base.clone(),
            self.tx_key().system_time,
            &self.options.default_tz,
            self.trace_parent,
        )?;
        let rule =
            rtdb_query::read_owned_fixed_range_rule(Arc::clone(&context), owner, relation).await?;
        if rule.as_ref() != Some(&evidence.rule) {
            return Ok(false);
        }
        let anchor = rtdb_query::read_owned_change_entry(
            Arc::clone(&context),
            owner,
            relation,
            evidence.revision,
        )
        .await?;
        if anchor.through_events != frontier
            || (evidence.revision == 0 && (frontier != 0 || anchor.value != Value::Null))
            || (evidence.revision != 0
                && (anchor.system_time > basis
                    || !matches!(anchor.value, Value::List(touches) if !touches.is_empty())))
        {
            return Ok(false);
        }
        let mut through = frontier;
        let mut previous_time = basis;
        let Some(head_time) = base.tx_basis().map(|key| key.system_time) else {
            return Ok(false);
        };
        for revision in evidence.revision + 1..=head {
            self.context.check_cancelled()?;
            let entry = rtdb_query::read_owned_fixed_range_changes(
                Arc::clone(&context),
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
                || entry.system_time <= previous_time
                || entry.system_time > head_time
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
            previous_time = entry.system_time;
        }
        Ok(through == head_frontier)
    }
}
