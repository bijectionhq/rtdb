use std::collections::BTreeSet;

use rtdb_types::{EntityId, OwnedRelation};

use super::{
    owned_unique::{MAX_KEY_BYTES, unique_id},
    *,
};

const RULE: &str = "!rule";

#[derive(Default)]
pub(super) struct OwnedCapacityChanges {
    relations: BTreeMap<OwnedRelation, Option<TouchedRule>>,
    reservations: Vec<Arc<MemoryReservation>>,
}

struct TouchedRule {
    rule: CapacityRule,
    ids: BTreeSet<EntityId>,
    writes: i64,
}

struct CapacityRule {
    column: String,
    prefix_bytes: usize,
    cap: i64,
    through_events: i64,
}

impl CapacityRule {
    fn new(column: String, prefix_bytes: i64, cap: i64, through_events: i64) -> Result<Self> {
        let prefix_bytes = usize::try_from(prefix_bytes)
            .ok()
            .filter(|n| (1..=MAX_KEY_BYTES).contains(n))
            .ok_or_else(invalid_rule)?;
        if column.is_empty()
            || column.len() > 96
            || column.starts_with('_')
            || cap <= 0
            || through_events < 0
        {
            return Err(invalid_rule());
        }
        Ok(Self {
            column,
            prefix_bytes,
            cap,
            through_events,
        })
    }

    fn document(&self) -> StructFields<Value> {
        StructFields::from([
            ("_id".into(), Value::Utf8(RULE.into())),
            (
                "policy".into(),
                Value::Struct(StructFields::from([
                    ("column".into(), Value::Utf8(self.column.clone())),
                    ("prefix_bytes".into(), Value::I64(self.prefix_bytes as i64)),
                    ("cap".into(), Value::I64(self.cap)),
                    ("through_events".into(), Value::I64(self.through_events)),
                ])),
            ),
        ])
    }

    fn decode(value: Value) -> Result<Self> {
        let Value::Struct(policy) = value else {
            return Err(uncovered());
        };
        let (
            Some(Value::Utf8(column)),
            Some(Value::I64(prefix_bytes)),
            Some(Value::I64(cap)),
            Some(Value::I64(through_events)),
        ) = (
            policy.get("column"),
            policy.get("prefix_bytes"),
            policy.get("cap"),
            policy.get("through_events"),
        )
        else {
            return Err(uncovered());
        };
        Self::new(column.clone(), *prefix_bytes, *cap, *through_events).map_err(|_| uncovered())
    }
}

#[derive(Default)]
struct Delta {
    releases: i64,
    claims: i64,
}

impl OpenTransaction<'_> {
    pub(super) async fn declare_owned_capacity(
        &mut self,
        relation: &OwnedRelation,
        column: &str,
        prefix_bytes: i64,
        cap: i64,
    ) -> Result<()> {
        OwnedRelation::new(relation.name())?;
        let owner = self
            .options
            .user
            .as_deref()
            .filter(|owner| !owner.is_empty())
            .ok_or_else(no_owned_principal)?;
        let rule = CapacityRule::new(column.into(), prefix_bytes, cap, 0)?;
        let storage = relation.capacity_table(owner);
        let target = relation.table(owner);
        let snapshot = self.snapshot()?;
        if snapshot.contains_table(&storage) {
            return Err(Anomaly::new(
                AnomalyCategory::Conflict,
                "rtdb.owned/capacity-exists",
                "The owned relation already has a capacity rule",
            ));
        }
        if snapshot.relation_event_frontier(&target)? != 0
            || !snapshot.trie_entries(&target).is_empty()
        {
            return Err(Anomaly::incorrect(
                "rtdb.owned/capacity-bootstrap",
                "A capacity rule requires a relation with no prior events",
            ));
        }
        if let Some((schema, _)) = self.read_owned_schema(owner, relation).await?
            && schema.column_type(column) != Some(ScalarType::Binary)
        {
            return Err(invalid_rule());
        }
        self.capacity_changes
            .reservations
            .push(self.context.memory.reserve(1024)?);
        self.buffer.put(storage, rule.document(), Interval::ALL)?;
        self.capacity_changes.relations.insert(
            relation.clone(),
            Some(TouchedRule {
                rule,
                ids: BTreeSet::new(),
                writes: 0,
            }),
        );
        Ok(())
    }

    pub(super) async fn track_capacity_mutation(
        &mut self,
        owner: &str,
        relation: &OwnedRelation,
        id: &EntityId,
        valid: Interval,
    ) -> Result<()> {
        if relation.world().is_some() {
            return Ok(());
        }
        if !self.capacity_changes.relations.contains_key(relation) {
            let snapshot = self.buffer.base_snapshot();
            let rule = if snapshot.contains_table(&relation.capacity_table(owner)) {
                let id = EntityId::String(RULE.into());
                let value = self
                    .capacity_cell(snapshot, owner, relation, &id, "policy")
                    .await?
                    .ok_or_else(uncovered)?;
                Some(TouchedRule {
                    rule: CapacityRule::decode(value)?,
                    ids: BTreeSet::new(),
                    writes: 0,
                })
            } else {
                None
            };
            self.capacity_changes
                .reservations
                .push(self.context.memory.reserve(1024)?);
            self.capacity_changes
                .relations
                .insert(relation.clone(), rule);
        }
        let Some(touched) = self
            .capacity_changes
            .relations
            .get_mut(relation)
            .and_then(Option::as_mut)
        else {
            return Ok(());
        };
        if valid.lower() != self.buffer.tx_key().system_time || valid.upper() != Timestamp::MAX {
            return Err(Anomaly::incorrect(
                "rtdb.owned/capacity-valid-time",
                "Capacity writes require validity from ordered present without an end",
            ));
        }
        touched.writes = touched.writes.checked_add(1).ok_or_else(overflow)?;
        if !touched.ids.contains(id) {
            let bytes = id
                .as_value()
                .retained_bytes()?
                .checked_add(1024)
                .ok_or_else(overflow)?;
            self.capacity_changes
                .reservations
                .push(self.context.memory.reserve(bytes)?);
            touched.ids.insert(id.clone());
        }
        Ok(())
    }

    pub(super) async fn finish_owned_capacity(&mut self) -> Result<()> {
        let changes = std::mem::take(&mut self.capacity_changes);
        if changes
            .relations
            .values()
            .flatten()
            .all(|touched| touched.writes == 0)
        {
            return Ok(());
        }
        let owner = self
            .options
            .user
            .as_deref()
            .filter(|owner| !owner.is_empty())
            .ok_or_else(no_owned_principal)?;
        let base = self.buffer.base_snapshot();
        let next = self.snapshot()?;
        for (relation, touched) in changes.relations {
            let Some(TouchedRule {
                mut rule,
                ids,
                writes,
            }) = touched
            else {
                continue;
            };
            if writes == 0 {
                continue;
            }
            let target = relation.table(owner);
            let through = rule
                .through_events
                .checked_add(writes)
                .ok_or_else(overflow)?;
            if base.relation_event_frontier(&target)? != rule.through_events
                || next.relation_event_frontier(&target)? != through
            {
                return Err(uncovered());
            }
            let mut deltas = BTreeMap::<EntityId, Delta>::new();
            let mut allocations = Vec::new();
            for id in ids {
                for (snapshot, final_state) in [(&base, false), (&next, true)] {
                    let context = self.context.query_context(
                        snapshot.clone(),
                        self.tx_key().system_time,
                        &self.options.default_tz,
                        self.trace_parent,
                    )?;
                    let cursor = rtdb_query::open_owned_point(
                        context,
                        owner,
                        &relation,
                        &id,
                        None,
                        Some(&[rule.column.as_str()]),
                    )
                    .await?;
                    let Some(key) = self
                        .read_occupancy_cell(cursor, &id, &rule.column, uncovered)
                        .await?
                    else {
                        continue;
                    };
                    let Value::Binary(key) = key else {
                        return Err(invalid_key());
                    };
                    let prefix = key
                        .get(..rule.prefix_bytes)
                        .filter(|_| key.len() <= MAX_KEY_BYTES)
                        .ok_or_else(invalid_key)?;
                    allocations.push(self.context.memory.reserve(prefix.len() * 4 + 512)?);
                    let delta = deltas.entry(unique_id(prefix)).or_default();
                    let count = if final_state {
                        &mut delta.claims
                    } else {
                        &mut delta.releases
                    };
                    *count = count.checked_add(1).ok_or_else(overflow)?;
                }
            }
            let storage = relation.capacity_table(owner);
            let mut locators = BTreeSet::from([EntityId::String(RULE.into()).iid()]);
            for (id, delta) in deltas {
                self.context.check_cancelled()?;
                if !locators.insert(id.iid()) {
                    return Err(uncovered());
                }
                let count = match self
                    .capacity_cell(base.clone(), owner, &relation, &id, "count")
                    .await?
                {
                    None => 0,
                    Some(Value::I64(count)) if (1..=rule.cap).contains(&count) => count,
                    Some(_) => return Err(uncovered()),
                };
                if delta.releases > count {
                    return Err(uncovered());
                }
                let final_count = (count - delta.releases)
                    .checked_add(delta.claims)
                    .ok_or_else(overflow)?;
                if final_count > rule.cap {
                    return Err(Anomaly::new(
                        AnomalyCategory::Conflict,
                        "rtdb.owned/capacity-exceeded",
                        "An owned prefix exceeds its capacity",
                    ));
                }
                if final_count == count {
                    continue;
                }
                if final_count == 0 {
                    self.buffer.delete(storage.clone(), &id, Interval::ALL)?;
                } else {
                    self.buffer.put(
                        storage.clone(),
                        StructFields::from([
                            ("_id".into(), id.as_value()),
                            ("count".into(), Value::I64(final_count)),
                        ]),
                        Interval::ALL,
                    )?;
                }
            }
            rule.through_events = through;
            self.buffer.put(storage, rule.document(), Interval::ALL)?;
        }
        Ok(())
    }

    pub(super) async fn check_capacity_schema(
        &self,
        snapshot: Snapshot,
        owner: &str,
        relation: &OwnedRelation,
        schema: &rtdb_types::OwnedSchema,
    ) -> Result<()> {
        if snapshot.contains_table(&relation.capacity_table(owner)) {
            let id = EntityId::String(RULE.into());
            let policy = self
                .capacity_cell(snapshot, owner, relation, &id, "policy")
                .await?
                .ok_or_else(uncovered)?;
            let rule = CapacityRule::decode(policy)?;
            if schema.column_type(&rule.column) != Some(ScalarType::Binary) {
                return Err(invalid_rule());
            }
        }
        Ok(())
    }

    async fn capacity_cell(
        &self,
        snapshot: Snapshot,
        owner: &str,
        relation: &OwnedRelation,
        id: &EntityId,
        column: &str,
    ) -> Result<Option<Value>> {
        let context = self.context.query_context(
            snapshot,
            self.tx_key().system_time,
            &self.options.default_tz,
            self.trace_parent,
        )?;
        let cursor =
            rtdb_query::open_owned_capacity_point(context, owner, relation, id, &[column]).await?;
        self.read_occupancy_cell(cursor, id, column, uncovered)
            .await
    }
}

fn invalid_rule() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.owned/capacity-rule",
        "A capacity rule requires a Binary column, a prefix length of 1 to 4096, and a positive cap",
    )
}

fn invalid_key() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.owned/capacity-key",
        "A capacity key must be Binary and contain its declared prefix within 4096 bytes",
    )
}

fn uncovered() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.owned/capacity-uncovered",
        "The owned relation cannot verify its capacity accounting",
    )
}
