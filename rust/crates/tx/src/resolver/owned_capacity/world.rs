use rtdb_plan::Catalog;
use rtdb_types::{OwnedWorld, PlacedAttemptScope, WorldClockMode};

use super::*;

/// The placed archive owner opens the registered parent and runs the check
/// inside its storage admission. All policy and overlay inputs come from the resolver.
#[async_trait]
pub trait WorldConstraintArchive: Send + Sync {
    async fn check_capacity(&self, check: ArchivedCapacityCheck<'_>) -> Result<()>;
}

pub struct ArchivedCapacityCheck<'a> {
    pub database: &'a str,
    pub placement: PlacedAttemptScope,
    pub owner: &'a str,
    pub world: &'a OwnedWorld,
    pub ordered: Timestamp,
    pub memory: &'a Arc<MemoryBudget>,
    pub cache: Arc<rtdb_storage::cache::CacheBudget>,
    pub cancellation: &'a CancellationToken,
    relation: &'a OwnedRelation,
    valid: Timestamp,
    purge_frontier: i64,
    before: BTreeSet<EntityId>,
    after: BTreeSet<EntityId>,
    rules: Vec<RuleCounts<'a>>,
}

struct RuleCounts<'a> {
    rule: &'a CapacityRule,
    qualifiers: Vec<(&'a str, &'a [u8])>,
    buckets: BTreeMap<Vec<u8>, [i64; 2]>,
}

impl RuleCounts<'_> {
    fn add(&mut self, value: impl FnMut(&str) -> Result<Value>, membership: [bool; 2]) -> Result<()> {
        if let Some(key) = self.rule.key(&self.qualifiers, value)?
            && let Some(counts) = self.buckets.get_mut(&key)
        {
            for (count, included) in counts.iter_mut().zip(membership) {
                if included { *count = count.checked_add(1).ok_or_else(overflow)?; }
            }
        }
        Ok(())
    }
}

impl ArchivedCapacityCheck<'_> {
    pub async fn validate(mut self, parent: Arc<QueryContext>) -> Result<()> {
        let database = parent.catalog.database(parent.catalog.default_database())?;
        let [snapshot] = database.partitions.as_slice() else { return Err(uncovered()); };
        if snapshot.tx_basis() != Some(self.world.basis)
            || snapshot.contains_table(&OwnedRelation::obsolete_purge_table(self.owner))
            || snapshot.relation_event_frontier(&self.relation.purge_table(self.owner))? != self.purge_frontier
        {
            return Err(uncovered());
        }
        let declared = rtdb_query::read_owned_relation_schema(
            Arc::clone(&parent), self.owner, self.relation,
        ).await?;
        if declared.as_ref().map(|(schema, _)| schema) != self.world.schemas.relation(self.relation) {
            return Err(uncovered());
        }
        let names = projected_columns(&self.rules, false);
        if let Some(mut cursor) = rtdb_query::open_owned_constraint_projection(
            Arc::clone(&parent), self.owner, self.relation, self.valid, &names,
        ).await? {
            while let Some(batch) = cursor.next_batch().await? {
                let rows = batch.batch();
                for row in 0..rows.num_rows() {
                    parent.check_cancelled_at(row)?;
                    let read = |name: &str| occupancy_value_at(&parent.memory, rows, row, name, uncovered);
                    let id = read("_id")?.entity_id()?;
                    let inherited = [!self.before.contains(&id), !self.after.contains(&id)];
                    if inherited.iter().any(|included| *included) {
                        for rule in &mut self.rules {
                            rule.add(read, inherited)?;
                        }
                    }
                }
            }
        }
        parent.check_cancelled()?;
        for rule in self.rules {
            if rule.buckets.values().any(|&[before, after]| after > before && after > rule.rule.cap) {
                return Err(Anomaly::new(
                    AnomalyCategory::Conflict, "rtdb.owned/capacity-exceeded",
                    "An owned world prefix exceeds its current Root capacity",
                ));
            }
        }
        Ok(())
    }
}

fn projected_columns<'r>(rules: &[RuleCounts<'r>], overlay: bool) -> Vec<&'r str> {
    let mut columns = BTreeSet::from(["_id"]);
    if overlay { columns.insert(WorldShadow::FIELDS); }
    for rule in rules {
        columns.extend(rule.rule.columns(&rule.qualifiers));
    }
    columns.into_iter().collect()
}

impl OpenTransaction<'_> {
    pub(super) async fn check_inherited_capacity(
        &self, operations: &[Operation], archive: &dyn WorldConstraintArchive,
    ) -> Result<()> {
        let _groups = self.context.memory.reserve(operations.len().saturating_mul(128))?;
        let mut groups = BTreeMap::<&OwnedRelation, Vec<&crate::Documents>>::new();
        for write in operations.iter().filter_map(crate::hosted_control::proposal_overlay_write) {
            if write.documents.rows().iter().any(|row| !WorldShadow::is_tombstone(row)) {
                groups.entry(write.relation).or_default().push(write.documents);
            }
        }
        if groups.is_empty() { return Ok(()); }
        let owner = self.options.user.as_deref().filter(|owner| !owner.is_empty()).ok_or_else(no_owned_principal)?;
        let base = self.buffer.base_snapshot();
        let final_state = self.snapshot()?;
        if final_state.contains_table(&OwnedRelation::obsolete_purge_table(owner)) {
            return Err(uncovered());
        }
        for (overlay, writes) in groups {
            self.context.check_cancelled()?;
            let root = overlay.root_relation().ok_or_else(uncovered)?;
            let policy = self.capacity_policy(final_state.clone(), owner, &root).await?;
            if policy.rules.is_empty() { continue; }
            if final_state.relation_event_frontier(&root.table(owner))? != policy.through_events {
                return Err(uncovered());
            }
            let world = overlay.world().and_then(|id| self.owned_worlds.get(&id))
                .map(|(world, _)| world).ok_or_else(uncovered)?;
            let schema = world.schemas.relation(&root).ok_or_else(uncovered)?;
            let mut reservations = vec![self.context.memory.reserve(policy.retained_bytes())?];
            let mut rules = Vec::new();
            for rule in &policy.rules {
                if !rule.admits(schema)? { return Err(uncovered()); }
                let mut counts = RuleCounts { rule, qualifiers: predicates(&rule.when)?, buckets: BTreeMap::new() };
                for row in writes.iter().flat_map(|batch| batch.rows()) {
                    if WorldShadow::is_tombstone(row) { continue; }
                    if let Some(key) = rule.key(&counts.qualifiers, |name| Ok(row.get(name).cloned().unwrap_or(Value::Undefined)))?
                        && !counts.buckets.contains_key(&key)
                    {
                        reservations.push(self.context.memory.reserve(key.capacity().saturating_add(192))?);
                        counts.buckets.insert(key, [0, 0]);
                    }
                }
                if !counts.buckets.is_empty() { rules.push(counts); }
            }
            if rules.is_empty() { continue; }
            // An open-ended assertion cannot introduce a second membership
            // change after the world clock used for this count.
            if !schema.has_acceptance_validity() {
                return Err(Anomaly::incorrect("rtdb.owned/world-capacity-uncovered",
                    "World capacity charges require acceptance validity"));
            }
            let ordered = self.tx_key().system_time;
            let valid = match world.mode {
                WorldClockMode::Proposal => ordered,
                WorldClockMode::Held { .. } => self.owned_world_clock(owner, world.id).await?
                    .unwrap_or(world.initial_clock()).next()?,
                WorldClockMode::Retained => return Err(uncovered()),
            };
            let mut masks = [BTreeSet::new(), BTreeSet::new()];
            for (side, snapshot) in [&base, &final_state].into_iter().enumerate() {
                let context = self.context.query_context(snapshot.clone(), ordered, &self.options.default_tz, self.trace_parent)?;
                let names = projected_columns(&rules, true);
                if let Some(mut cursor) = rtdb_query::open_owned_world_constraint_projection(
                    Arc::clone(&context), owner, overlay, valid, &names,
                ).await? {
                    while let Some(batch) = cursor.next_batch().await? {
                        let rows = batch.batch();
                        for row in 0..rows.num_rows() {
                            context.check_cancelled_at(row)?;
                            let read = |name: &str| occupancy_value_at(&context.memory, rows, row, name, uncovered);
                            let id = read("_id")?.entity_id()?;
                            reservations.push(self.context.memory.reserve(id.as_value().retained_bytes()?.saturating_add(192))?);
                            if !masks[side].insert(id) { return Err(uncovered()); }
                            match read(WorldShadow::FIELDS)? {
                                Value::Null => (),
                                Value::List(_) => {
                                    for rule in &mut rules {
                                        rule.add(read, [side == 0, side == 1])?;
                                    }
                                }
                                _ => return Err(uncovered()),
                            }
                        }
                    }
                }
            }
            let [before, after] = masks;
            archive.check_capacity(ArchivedCapacityCheck {
                database: &self.context.database, placement: self.context.owned_witness_scope.ok_or_else(uncovered)?,
                owner, world, ordered, memory: &self.context.memory, cache: Arc::clone(self.context.storage.budget()),
                cancellation: &self.context.cancellation,
                relation: &root, valid, purge_frontier: final_state.relation_event_frontier(&root.purge_table(owner))?,
                before, after, rules,
            }).await?;
            drop(reservations);
        }
        Ok(())
    }
}
