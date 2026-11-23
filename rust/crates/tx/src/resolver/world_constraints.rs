use rtdb_plan::Catalog;
use rtdb_types::{EntityId, OwnedRelation, OwnedWorld, PlacedAttemptScope, WorldClockMode, WorldShadow};

use super::{
    owned_capacity::{CapacityRule, predicates, uncovered},
    owned_unique::{invalid_key as invalid_unique_key, occupancy_value_at, unique_key, unique_taken, unique_uncovered},
    *,
};

/// The placed archive owner opens the registered parent and runs the check
/// inside its storage admission. All policy and overlay inputs come from the resolver.
#[async_trait]
pub trait WorldConstraintArchive: Send + Sync {
    async fn check(&self, check: ArchivedWorldConstraints<'_>) -> Result<()>;
}

pub struct ArchivedWorldConstraints<'a> {
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
    unique: Vec<UniqueClaims<'a>>,
}

struct RuleCounts<'a> {
    rule: &'a CapacityRule,
    qualifiers: Vec<(&'a str, &'a [u8])>,
    buckets: BTreeMap<Vec<u8>, [i64; 2]>,
}

struct UniqueClaims<'a> {
    path: &'a OwnedRelation,
    column: &'a str,
    keys: BTreeMap<Vec<u8>, bool>,
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

impl ArchivedWorldConstraints<'_> {
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
        if self.unique.iter().any(|claim| claim.keys.values().any(|active| *active)) {
            let table = self.relation.table(self.owner);
            let frontier = snapshot.relation_event_frontier(&table)?;
            if snapshot.contains_table(&self.relation.unique_table(self.owner)) {
                self.require_unique_claims(&parent, frontier).await?;
            } else if frontier != 0
                || !snapshot.trie_entries(&table).is_empty()
                || snapshot.iter_segments(&table).any(|segment| segment.row_count() != 0)
                || snapshot.table_catalog().metadata(&table).is_some_and(|metadata| metadata.row_count != 0)
            {
                return Err(unique_uncovered());
            }
        }
        let names = projected_columns(&self.rules, &[], false);
        if !self.rules.is_empty() && let Some(mut cursor) = rtdb_query::open_owned_root_projection(
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

    async fn require_unique_claims(&self, parent: &Arc<QueryContext>, frontier: i64) -> Result<()> {
        let policy = rtdb_query::read_owned_unique_policy(
            Arc::clone(parent), self.owner, self.relation,
        ).await?;
        if policy.through_events != frontier { return Err(unique_uncovered()); }
        for claim in &self.unique {
            if claim.keys.values().all(|active| !*active) { continue; }
            let Some(column) = policy.paths.get(claim.path) else {
                if frontier == 0 { continue; }
                return Err(unique_uncovered());
            };
            if column != claim.column { return Err(unique_uncovered()); }
            for (key, active) in &claim.keys {
                if !*active { continue; }
                if let Some(hit) = rtdb_query::lookup_owned_unique(
                    Arc::clone(parent), self.owner, self.relation, claim.path, claim.column, key, self.valid,
                ).await? {
                    let holder = occupancy_value_at(&parent.memory, hit.row.batch(), 0, "_id", unique_uncovered)?
                        .entity_id()?;
                    if !self.after.contains(&holder) { return Err(unique_taken()); }
                }
            }
        }
        Ok(())
    }
}

fn projected_columns<'r>(
    rules: &[RuleCounts<'r>], unique: &[UniqueClaims<'r>], overlay: bool,
) -> Vec<&'r str> {
    let mut columns = BTreeSet::from(["_id"]);
    if overlay { columns.insert(WorldShadow::FIELDS); }
    for rule in rules {
        columns.extend(rule.rule.columns(&rule.qualifiers));
    }
    columns.extend(unique.iter().map(|claim| claim.column));
    columns.into_iter().collect()
}

impl OpenTransaction<'_> {
    pub(super) async fn check_world_constraints(
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
            let unique_policy = self.unique_policy(final_state.clone(), owner, &root).await?;
            if policy.rules.is_empty() && unique_policy.paths.is_empty() { continue; }
            let frontier = final_state.relation_event_frontier(&root.table(owner))?;
            if !policy.rules.is_empty() && frontier != policy.through_events {
                return Err(uncovered());
            }
            if !unique_policy.paths.is_empty() && frontier != unique_policy.through_events {
                return Err(unique_uncovered());
            }
            let world = overlay.world().and_then(|id| self.owned_worlds.get(&id))
                .map(|(world, _)| world).ok_or_else(uncovered)?;
            let schema = world.schemas.relation(&root).ok_or_else(uncovered)?;
            let mut reservations = vec![self.context.memory.reserve(policy.retained_bytes())?];
            if !unique_policy.paths.is_empty() {
                reservations.push(self.context.memory.reserve(1024 + unique_policy.paths.len() * 256)?);
            }
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
            let mut unique = Vec::with_capacity(unique_policy.paths.len());
            for (path, column) in &unique_policy.paths {
                if schema.path_column_type(column) != Some(ScalarType::Binary) {
                    return Err(invalid_unique_key());
                }
                let mut keys = BTreeMap::new();
                for row in writes.iter().flat_map(|batch| batch.rows()) {
                    if WorldShadow::is_tombstone(row) { continue; }
                    let key = unique_key(row.get(column).cloned().unwrap_or(Value::Undefined))?;
                    if !keys.contains_key(&key) {
                        reservations.push(self.context.memory.reserve(key.capacity().saturating_add(192))?);
                        keys.insert(key, false);
                    }
                }
                unique.push(UniqueClaims { path, column, keys });
            }
            if rules.is_empty() && unique.is_empty() { continue; }
            // An open-ended assertion cannot introduce a second membership
            // change after the world clock used for this count.
            if !schema.has_acceptance_validity() {
                return Err(if unique.is_empty() {
                    Anomaly::incorrect("rtdb.owned/world-capacity-uncovered",
                        "World capacity charges require acceptance validity")
                } else {
                    Anomaly::incorrect("rtdb.owned/world-unique-uncovered",
                        "World unique claims require acceptance validity")
                });
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
                if side == 0 && rules.is_empty() { continue; }
                let context = self.context.query_context(snapshot.clone(), ordered, &self.options.default_tz, self.trace_parent)?;
                let names = projected_columns(&rules, if side == 1 { &unique } else { &[] }, true);
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
                                    if side == 1 {
                                        for claim in &mut unique {
                                            let key = unique_key(read(claim.column)?)?;
                                            if let Some(active) = claim.keys.get_mut(&key)
                                                && std::mem::replace(active, true)
                                            {
                                                return Err(unique_taken());
                                            }
                                        }
                                    }
                                }
                                _ => return Err(uncovered()),
                            }
                        }
                    }
                }
            }
            let [before, after] = masks;
            archive.check(ArchivedWorldConstraints {
                database: &self.context.database, placement: self.context.owned_witness_scope.ok_or_else(uncovered)?,
                owner, world, ordered, memory: &self.context.memory, cache: Arc::clone(self.context.storage.budget()),
                cancellation: &self.context.cancellation,
                relation: &root, valid, purge_frontier: final_state.relation_event_frontier(&root.purge_table(owner))?,
                before, after, rules, unique,
            }).await?;
            drop(reservations);
        }
        Ok(())
    }
}
