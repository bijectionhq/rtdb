use rtdb_plan::Catalog;
use rtdb_types::{EntityId, OwnedRelation, OwnedWorld, PlacedAttemptScope, WorldClockMode, WorldShadow};

use super::{
    owned_capacity::{CapacityRule, predicates, uncovered},
    owned_unique::{
        invalid_key as invalid_unique_key, merge_windows, occupancy_value_at, ordered_segments,
        unique_key, unique_segment_interval, unique_taken, unique_uncovered, Segment,
    },
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
    finite_unique: Option<FiniteUniqueClaims<'a>>,
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

struct FiniteUniquePath<'a> {
    path: &'a OwnedRelation,
    column: &'a str,
    claims: BTreeMap<Vec<u8>, Vec<Segment<EntityId>>>,
}

struct FiniteUniqueClaims<'a> {
    overlay: &'a OwnedRelation,
    local: Arc<QueryContext>,
    paths: Vec<FiniteUniquePath<'a>>,
    budget: WindowBudget,
}

struct WindowBudget {
    remaining: usize,
    reservations: Vec<Arc<MemoryReservation>>,
}

impl WindowBudget {
    fn fetch(&self) -> usize {
        self.remaining.min(1024) + 1
    }

    fn consume(&mut self, limit: &mut usize) -> Result<()> {
        *limit = limit.checked_sub(1).ok_or_else(unique_uncovered)?;
        self.remaining = self.remaining.checked_sub(1).ok_or_else(unique_uncovered)?;
        Ok(())
    }

    async fn holders(
        &mut self,
        context: &QueryContext,
        cursor: Option<QueryCursor>,
        column: &str,
        key: &[u8],
        window: Interval,
    ) -> Result<Vec<Segment<EntityId>>> {
        let Some(mut cursor) = cursor else { return Ok(Vec::new()); };
        let mut limit = self.fetch() - 1;
        let mut holders = Vec::new();
        while let Some(batch) = cursor.next_batch().await? {
            let rows = batch.batch();
            for row in 0..rows.num_rows() {
                self.consume(&mut limit)?;
                let read = |name| occupancy_value_at(&context.memory, rows, row, name, unique_uncovered);
                let id = read("_id")?.entity_id()?;
                rtdb_query::owned_row_revision(rows, row, &id, &context.memory)?;
                if unique_key(read(column)?)?.as_slice() != key {
                    return Err(unique_uncovered());
                }
                let valid = unique_segment_interval(rows, row, window)?;
                self.reservations.push(context.memory.reserve(id.as_value().retained_bytes()?.saturating_add(256))?);
                holders.push(Segment { valid, value: id });
            }
        }
        Ok(holders)
    }

    async fn shadow_covers(
        &mut self,
        context: &Arc<QueryContext>,
        owner: &str,
        overlay: &OwnedRelation,
        id: &EntityId,
        window: Interval,
    ) -> Result<bool> {
        // Deletion shadows also keep a physical _id, with their original valid interval.
        let Some(mut cursor) = rtdb_query::open_owned_key_timeline(
            Arc::clone(context), owner, overlay, id, window, "_id",
        ).await? else { return Ok(false); };
        let mut through = window.lower();
        let mut limit = self.fetch() - 1;
        while let Some(batch) = cursor.next_batch().await? {
            let rows = batch.batch();
            for row in 0..rows.num_rows() {
                self.consume(&mut limit)?;
                rtdb_query::owned_row_revision(rows, row, id, &context.memory)?;
                let valid = unique_segment_interval(rows, row, window)?;
                if valid.lower() < through { return Err(unique_uncovered()); }
                if valid.lower() > through { return Ok(false); }
                through = valid.upper();
            }
        }
        Ok(through == window.upper())
    }
}

impl FiniteUniqueClaims<'_> {
    async fn validate(
        mut self,
        parent: &Arc<QueryContext>,
        owner: &str,
        relation: &OwnedRelation,
        basis: Timestamp,
        policy: &rtdb_query::OwnedUniquePolicy,
        frontier: i64,
    ) -> Result<()> {
        for path in self.paths {
            let Some(column) = policy.paths.get(path.path) else {
                if frontier == 0 { continue; }
                return Err(unique_uncovered());
            };
            if column != path.column { return Err(unique_uncovered()); }
            for (key, local) in path.claims {
                for window in merge_windows(local.iter().map(|part| part.valid).collect())? {
                    let cursor = rtdb_query::open_owned_unique_window(
                        Arc::clone(parent), owner, relation, column, Some(&key), window, basis,
                        None, self.budget.fetch(),
                    ).await?;
                    let mut inherited = self.budget.holders(parent, cursor, column, &key, window).await?;
                    ordered_segments(&mut inherited, unique_uncovered)?;
                    for holder in inherited {
                        let start = local.partition_point(|part| part.valid.upper() <= holder.valid.lower());
                        for claim in local[start..].iter().take_while(|claim| claim.valid.lower() < holder.valid.upper()) {
                            let overlap = claim.valid.intersection(holder.valid).ok_or_else(unique_uncovered)?;
                            if !self.budget.shadow_covers(&self.local, owner, self.overlay, &holder.value, overlap).await? {
                                return Err(unique_taken());
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
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
        if self.finite_unique.is_some()
            || self.unique.iter().any(|claim| claim.keys.values().any(|active| *active))
        {
            let table = self.relation.table(self.owner);
            let frontier = snapshot.relation_event_frontier(&table)?;
            if snapshot.contains_table(&self.relation.unique_table(self.owner)) {
                let policy = rtdb_query::read_owned_unique_policy(
                    Arc::clone(&parent), self.owner, self.relation,
                ).await?;
                if policy.through_events != frontier { return Err(unique_uncovered()); }
                self.require_unique_claims(&parent, &policy, frontier).await?;
                if let Some(finite) = self.finite_unique.take() {
                    finite.validate(
                        &parent, self.owner, self.relation, self.world.basis.system_time, &policy, frontier,
                    ).await?;
                }
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

    async fn require_unique_claims(
        &self,
        parent: &Arc<QueryContext>,
        policy: &rtdb_query::OwnedUniquePolicy,
        frontier: i64,
    ) -> Result<()> {
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

async fn prepare_finite_unique_claims<'a>(
    local: Arc<QueryContext>,
    owner: &str,
    overlay: &'a OwnedRelation,
    schema: &rtdb_types::OwnedSchema,
    policy: &'a rtdb_query::OwnedUniquePolicy,
    writes: &[crate::hosted_control::ProposalOverlayWrite<'_>],
) -> Result<FiniteUniqueClaims<'a>> {
    let mut budget = WindowBudget { remaining: 4096, reservations: Vec::new() };
    let mut paths = Vec::new();
    for (path, column) in &policy.paths {
        if schema.path_column_type(column) != Some(ScalarType::Binary) {
            return Err(invalid_unique_key());
        }
        let mut affected = BTreeMap::<Vec<u8>, Vec<Interval>>::new();
        for write in writes {
            schema.require_root_write_period(write.from, write.to)?;
            let valid = Interval::new(
                write.from.ok_or_else(unique_uncovered)?,
                write.to.ok_or_else(unique_uncovered)?,
            )?;
            for row in write.documents.rows() {
                if WorldShadow::is_tombstone(row) { continue; }
                let key = unique_key(row.get(column.as_str()).cloned().unwrap_or(Value::Undefined))?;
                budget.reservations.push(local.memory.reserve(key.capacity().saturating_add(192))?);
                affected.entry(key).or_default().push(valid);
            }
        }
        let mut claims = BTreeMap::new();
        for (key, windows) in affected {
            let mut holders = Vec::new();
            for window in merge_windows(windows)? {
                let cursor = rtdb_query::open_staged_unique_window(
                    Arc::clone(&local), owner, overlay, column, &key, window, budget.fetch(),
                ).await?;
                holders.extend(budget.holders(&local, cursor, column, &key, window).await?);
            }
            ordered_segments(&mut holders, unique_taken)?;
            if !holders.is_empty() {
                claims.insert(key, holders);
            }
        }
        if !claims.is_empty() {
            paths.push(FiniteUniquePath { path, column, claims });
        }
    }
    Ok(FiniteUniqueClaims { overlay, local, paths, budget })
}

impl OpenTransaction<'_> {
    pub(super) async fn check_world_constraints(
        &self, operations: &[Operation], archive: &dyn WorldConstraintArchive,
    ) -> Result<()> {
        let _groups = self.context.memory.reserve(operations.len().saturating_mul(128))?;
        let mut groups = BTreeMap::<&OwnedRelation, Vec<crate::hosted_control::ProposalOverlayWrite<'_>>>::new();
        for write in operations.iter().filter_map(crate::hosted_control::proposal_overlay_write) {
            if write.documents.rows().iter().any(|row| !WorldShadow::is_tombstone(row)) {
                groups.entry(write.relation).or_default().push(write);
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
                for row in writes.iter().flat_map(|write| write.documents.rows()) {
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
            if schema.requires_bounded_period() {
                if !rules.is_empty() {
                    return Err(Anomaly::incorrect(
                        "rtdb.owned/world-capacity-uncovered",
                        "World capacity charges require acceptance validity",
                    ));
                }
                if unique_policy.paths.is_empty() { continue; }
                if schema.has_opaque_columns() || schema.lifetime_micros().is_some() || !world.mode.is_writable() {
                    return Err(unique_uncovered());
                }
                let ordered = self.tx_key().system_time;
                let local = self.context.query_context(
                    final_state.clone(), ordered, &self.options.default_tz, self.trace_parent,
                )?;
                let finite = prepare_finite_unique_claims(
                    local, owner, overlay, schema, &unique_policy, &writes,
                ).await?;
                if !finite.paths.is_empty() {
                    archive.check(ArchivedWorldConstraints {
                        database: &self.context.database,
                        placement: self.context.owned_witness_scope.ok_or_else(unique_uncovered)?,
                        owner, world, ordered, memory: &self.context.memory,
                        cache: Arc::clone(self.context.storage.budget()),
                        cancellation: &self.context.cancellation,
                        relation: &root,
                        valid: ordered,
                        purge_frontier: final_state.relation_event_frontier(&root.purge_table(owner))?,
                        before: BTreeSet::new(), after: BTreeSet::new(),
                        rules, unique: Vec::new(), finite_unique: Some(finite),
                    }).await?;
                }
                continue;
            }
            let mut unique = Vec::with_capacity(unique_policy.paths.len());
            for (path, column) in &unique_policy.paths {
                if schema.path_column_type(column) != Some(ScalarType::Binary) {
                    return Err(invalid_unique_key());
                }
                let mut keys = BTreeMap::new();
                for row in writes.iter().flat_map(|write| write.documents.rows()) {
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
                WorldClockMode::Held { .. } => self.held_overlay_reading(owner, world).await?,
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
                before, after, rules, unique, finite_unique: None,
            }).await?;
            drop(reservations);
        }
        Ok(())
    }
}
