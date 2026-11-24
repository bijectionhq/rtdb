use rtdb_plan::Catalog;
use rtdb_types::{EntityId, OwnedRelation, OwnedWorld, PlacedAttemptScope, WorldClockMode, WorldShadow};

use super::{
    owned_capacity::{CapacityCondition, CapacityRule, PERIOD_MEMBERSHIP, SAMPLE_MEMBERSHIP, SamplePopulation, SampleProjection, capacity_conditions, invalid_key as invalid_capacity_key, count_bound_exceeded, period_population, predicates, uncovered},
    owned_unique::{
        invalid_key as invalid_unique_key, merge_windows, occupancy_value_at, ordered_segments,
        unique_key, unique_key_bytes, unique_segment_interval, unique_taken, unique_uncovered, Segment,
    },
    *,
};

/// The placed archive owner opens the registered parent and runs the check
/// inside its storage admission. All policy and overlay inputs come from the resolver.
#[async_trait]
pub trait WorldConstraintArchive: Send + Sync {
    async fn check(&self, check: ArchivedWorldConstraints<'_>) -> Result<()>;

    async fn claim_assertion_family_fence(&self, _claim: AssertionFenceClaim<'_>) -> Result<()> {
        Err(Anomaly::unsupported("rtdb.owned/assertion-family-fence-unavailable",
            "The original assertion authority does not admit ordered family fences"))
    }
}

pub struct AssertionFenceClaim<'a> {
    pub database: &'a str,
    pub attempt: rtdb_types::PlacedAttempt,
    pub durable_predecessor: Option<rtdb_types::TransactionKey>,
    pub family: [u8; 16],
    pub marker: rtdb_types::AssertionFamilyFenceMarker,
    pub retirement_world: Option<Iid>,
    pub root_erases: &'a BTreeSet<(&'a OwnedRelation, &'a EntityId)>,
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
    period_count: Option<(i64, i64)>,
    sample_counts: Vec<(SampleProjection<'a>, SamplePopulation, i64)>,
}

struct RuleCounts<'a> {
    rule: &'a CapacityRule,
    qualifiers: Vec<(&'a str, &'a [u8])>,
    buckets: BTreeMap<Vec<u8>, [i64; 2]>,
    final_bounds: BTreeMap<&'a [u8], i64>,
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

    async fn shadow_windows(
        &mut self,
        context: &Arc<QueryContext>,
        owner: &str,
        overlay: &OwnedRelation,
        id: &EntityId,
        window: Interval,
        windows: &mut Vec<Interval>,
    ) -> Result<()> {
        // Deletion shadows also keep a physical _id, with their original valid interval.
        let Some(mut cursor) = rtdb_query::open_owned_key_timeline(
            Arc::clone(context), owner, overlay, id, window, "_id",
        ).await? else { return Ok(()); };
        let mut through = window.lower();
        let mut limit = self.fetch() - 1;
        while let Some(batch) = cursor.next_batch().await? {
            let rows = batch.batch();
            for row in 0..rows.num_rows() {
                self.consume(&mut limit)?;
                rtdb_query::owned_row_revision(rows, row, id, &context.memory)?;
                let valid = unique_segment_interval(rows, row, window)?;
                if valid.lower() < through { return Err(unique_uncovered()); }
                through = valid.upper();
                self.reservations.push(context.memory.reserve(64)?);
                windows.push(valid);
            }
        }
        Ok(())
    }
}

// The archive owner admits these layers in descending shadow precedence.
type FrozenLayer = (Arc<QueryContext>, OwnedRelation);

impl FiniteUniqueClaims<'_> {
    async fn validate(
        mut self,
        parent: &Arc<QueryContext>,
        frozen: &[FrozenLayer],
        owner: &str,
        relation: &OwnedRelation,
        basis: Timestamp,
        policy: Option<&rtdb_query::OwnedUniquePolicy>,
        frontier: i64,
    ) -> Result<()> {
        for path in self.paths {
            let root_column = policy.and_then(|policy| policy.paths.get(path.path));
            if root_column.map(String::as_str).is_some_and(|column| column != path.column)
                || root_column.is_none() && frontier != 0
            {
                return Err(unique_uncovered());
            }
            for (key, local) in path.claims {
                for window in merge_windows(local.iter().map(|part| part.valid).collect())? {
                    for (depth, (context, shadow)) in frozen.iter().enumerate() {
                        let cursor = rtdb_query::open_archived_world_unique_window(
                            Arc::clone(context), owner, shadow, path.column, &key, window, self.budget.fetch(),
                        ).await?;
                        let holders = self.budget.holders(context, cursor, path.column, &key, window).await?;
                        self.budget.require_masked_holders(
                            owner, &self.local, self.overlay, &frozen[..depth], &local, holders,
                        ).await?;
                    }
                    if let Some(column) = root_column {
                        let cursor = rtdb_query::open_owned_unique_window(
                            Arc::clone(parent), owner, relation, column, Some(&key), window, basis,
                            None, self.budget.fetch(),
                        ).await?;
                        let holders = self.budget.holders(parent, cursor, column, &key, window).await?;
                        self.budget.require_masked_holders(
                            owner, &self.local, self.overlay, frozen, &local, holders,
                        ).await?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl WindowBudget {
    async fn require_masked_holders(
        &mut self,
        owner: &str,
        local: &Arc<QueryContext>,
        overlay: &OwnedRelation,
        newer: &[FrozenLayer],
        claims: &[Segment<EntityId>],
        mut holders: Vec<Segment<EntityId>>,
    ) -> Result<()> {
        ordered_segments(&mut holders, unique_uncovered)?;
        for holder in holders {
            let start = claims.partition_point(|part| part.valid.upper() <= holder.valid.lower());
            for claim in claims[start..].iter().take_while(|claim| claim.valid.lower() < holder.valid.upper()) {
                let overlap = claim.valid.intersection(holder.valid).ok_or_else(unique_uncovered)?;
                let mut windows = Vec::new();
                self.shadow_windows(local, owner, overlay, &holder.value, overlap, &mut windows).await?;
                if windows_cover(&mut windows, overlap) { continue; }
                for (context, shadow) in newer {
                    self.shadow_windows(context, owner, shadow, &holder.value, overlap, &mut windows).await?;
                }
                if !windows_cover(&mut windows, overlap) { return Err(unique_taken()); }
            }
        }
        Ok(())
    }
}

fn windows_cover(windows: &mut [Interval], target: Interval) -> bool {
    windows.sort_unstable();
    let mut through = target.lower();
    for window in windows {
        if window.lower() > through { return false; }
        through = through.max(window.upper());
        if through >= target.upper() { return true; }
    }
    false
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
    pub fn relation(&self) -> &OwnedRelation {
        self.relation
    }

    pub async fn validate(self, parent: Arc<QueryContext>) -> Result<()> {
        self.validate_layers(parent, &[]).await
    }

    pub async fn validate_layers(
        mut self,
        parent: Arc<QueryContext>,
        frozen: &[(Arc<QueryContext>, OwnedRelation)],
    ) -> Result<()> {
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
        if !frozen.is_empty() && (self.period_count.is_some() || !self.sample_counts.is_empty()) {
            return Err(uncovered());
        }
        let active_unique = self.unique.iter().any(|claim| claim.keys.values().any(|active| *active));
        let mut frozen_masks = BTreeSet::new();
        let mut mask_reservations = Vec::new();
        if !self.rules.is_empty() || active_unique {
            let names = projected_columns(&self.rules, &self.unique, true);
            for (context, shadow) in frozen {
                if let Some(mut cursor) = rtdb_query::open_owned_world_constraint_projection(
                    Arc::clone(context), self.owner, shadow, self.valid, &names,
                ).await? {
                    while let Some(batch) = cursor.next_batch().await? {
                        let rows = batch.batch();
                        for row in 0..rows.num_rows() {
                            context.check_cancelled_at(row)?;
                            let read = |name: &str| occupancy_value_at(&context.memory, rows, row, name, uncovered);
                            let id = read("_id")?.entity_id()?;
                            if frozen_masks.contains(&id) { continue; }
                            let included = [!self.before.contains(&id), !self.after.contains(&id)];
                            mask_reservations.push(self.memory.reserve(id.as_value().retained_bytes()?.saturating_add(192))?);
                            frozen_masks.insert(id);
                            match read(WorldShadow::FIELDS)? {
                                Value::Null => (),
                                Value::List(_) => {
                                    for rule in &mut self.rules {
                                        rule.add(read, included)?;
                                    }
                                    if active_unique && included[1] {
                                        for claim in &self.unique {
                                            if claim.keys.values().all(|active| !*active) { continue; }
                                            let key = unique_key(read(claim.column)?)?;
                                            if claim.keys.get(&key) == Some(&true) {
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
                context.check_cancelled()?;
            }
        }
        if self.finite_unique.is_some() || active_unique {
            let table = self.relation.table(self.owner);
            let frontier = snapshot.relation_event_frontier(&table)?;
            let policy = if snapshot.contains_table(&self.relation.unique_table(self.owner)) {
                let policy = rtdb_query::read_owned_unique_policy(
                    Arc::clone(&parent), self.owner, self.relation,
                ).await?;
                if policy.through_events != frontier { return Err(unique_uncovered()); }
                self.require_unique_claims(&parent, &policy, frontier, &frozen_masks).await?;
                Some(policy)
            } else {
                if frontier != 0
                    || !snapshot.trie_entries(&table).is_empty()
                    || snapshot.iter_segments(&table).any(|segment| segment.row_count() != 0)
                    || snapshot.table_catalog().metadata(&table).is_some_and(|metadata| metadata.row_count != 0)
                {
                    return Err(unique_uncovered());
                }
                None
            };
            if let Some(finite) = self.finite_unique.take() {
                finite.validate(
                    &parent, frozen, self.owner, self.relation, self.world.basis.system_time, policy.as_ref(), frontier,
                ).await?;
            }
        }
        if let Some((local_members, maximum)) = self.period_count {
            let inherited = period_population(
                Arc::clone(&parent), self.owner, self.relation, self.valid, &self.after,
            ).await?;
            if inherited.checked_add(local_members).ok_or_else(overflow)? > maximum {
                return Err(count_bound_exceeded());
            }
        }
        for (projection, mut population, maximum) in self.sample_counts {
            population.collect(Arc::clone(&parent), self.owner, self.relation, self.valid, &projection).await?;
            if population.count(&parent)? > maximum { return Err(count_bound_exceeded()); }
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
                    if frozen_masks.contains(&id) { continue; }
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
            if rule.final_bounds.iter().any(|(&key, maximum)|
                rule.buckets.get(key).is_some_and(|counts| counts[1] > *maximum))
            {
                return Err(Anomaly::new(
                    AnomalyCategory::Conflict, "rtdb.owned/count-bound-exceeded",
                    "The final owned world prefix count exceeds its transaction bound",
                ));
            }
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
        frozen_masks: &BTreeSet<EntityId>,
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
                    if !self.after.contains(&holder) && !frozen_masks.contains(&holder) {
                        return Err(unique_taken());
                    }
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
                if row.is_tombstone()? { continue; }
                let key = unique_key_bytes(row.binary(column, invalid_unique_key)?)?;
                budget.reservations.push(local.memory.reserve(key.len().saturating_add(192))?);
                affected.entry(key.to_vec()).or_default().push(valid);
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
            if write.documents.rows().try_fold(false, |present, row| row.is_tombstone().map(|deleted| present || !deleted))? {
                groups.entry(write.relation).or_default().push(write);
            }
        }
        let mut bounds = BTreeMap::<&OwnedRelation, Vec<CapacityCondition<'_>>>::new();
        for bound in capacity_conditions(operations).filter(|bound| bound.relation.world().is_some()) {
            groups.entry(bound.relation).or_default();
            bounds.entry(bound.relation).or_default().push(bound);
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
            let mut asserted = bounds.remove(overlay).unwrap_or_default();
            let period_maximum = asserted.iter().filter(|bound| bound.column == PERIOD_MEMBERSHIP)
                .map(|bound| bound.at_most).min();
            let mut sample_bounds = BTreeMap::new();
            for bound in asserted.iter().filter(|bound| bound.column == SAMPLE_MEMBERSHIP) {
                sample_bounds.entry(bound.prefix).and_modify(|maximum: &mut i64| *maximum = (*maximum).min(bound.at_most))
                    .or_insert(bound.at_most);
            }
            asserted.retain(|bound| !matches!(bound.column, PERIOD_MEMBERSHIP | SAMPLE_MEMBERSHIP));
            if policy.rules.is_empty() && unique_policy.paths.is_empty()
                && asserted.is_empty() && period_maximum.is_none() && sample_bounds.is_empty() { continue; }
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
            if period_maximum.is_some() && (!schema.requires_bounded_period() || schema.has_opaque_columns()
                || world.mode != WorldClockMode::Proposal)
            {
                return Err(uncovered());
            }
            let mut reservations = vec![self.context.memory.reserve(policy.retained_bytes())?];
            if !unique_policy.paths.is_empty() {
                reservations.push(self.context.memory.reserve(1024 + unique_policy.paths.len() * 256)?);
            }
            if !asserted.is_empty()
                && (world.mode != WorldClockMode::Proposal || !schema.has_acceptance_validity()
                    || schema.has_opaque_columns() || schema.lifetime_micros().is_some()
                    || asserted.iter().any(|bound| !policy.rules.iter().any(|rule| {
                        rule.matches_prefix_assertion(bound.column, bound.prefix)
                    })))
            {
                return Err(uncovered());
            }
            let mut rules = Vec::new();
            for rule in &policy.rules {
                if !rule.admits(schema)? { return Err(uncovered()); }
                let mut counts = RuleCounts { rule, qualifiers: predicates(&rule.when)?,
                    buckets: BTreeMap::new(), final_bounds: BTreeMap::new() };
                for bound in asserted.iter().filter(|bound| rule.matches_prefix_assertion(bound.column, bound.prefix))
                {
                    reservations.push(self.context.memory.reserve(bound.prefix.len().saturating_add(192))?);
                    counts.buckets.entry(bound.prefix.to_vec()).or_insert([0, 0]);
                    counts.final_bounds.entry(bound.prefix)
                        .and_modify(|maximum| *maximum = (*maximum).min(bound.at_most))
                        .or_insert(bound.at_most);
                }
                for row in writes.iter().flat_map(|write| write.documents.rows()) {
                    if row.is_tombstone()? { continue; }
                    if let Some(key) = rule.borrowed_key(&counts.qualifiers, |name| row.binary(name, invalid_capacity_key))?
                        && !counts.buckets.contains_key(key)
                    {
                        reservations.push(self.context.memory.reserve(key.len().saturating_add(192))?);
                        counts.buckets.insert(key.to_vec(), [0, 0]);
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
                if unique_policy.paths.is_empty() && period_maximum.is_none() { continue; }
                if schema.has_opaque_columns() || schema.lifetime_micros().is_some() || !world.mode.is_writable() {
                    return Err(unique_uncovered());
                }
                let ordered = self.tx_key().system_time;
                let local = self.context.query_context(
                    final_state.clone(), ordered, &self.options.default_tz, self.trace_parent,
                )?;
                let mut after = BTreeSet::new();
                let mut members = 0i64;
                if period_maximum.is_some() && let Some(mut cursor) = rtdb_query::open_owned_world_constraint_projection(
                    Arc::clone(&local), owner, overlay, ordered, &["_id", WorldShadow::FIELDS],
                ).await? {
                    while let Some(batch) = cursor.next_batch().await? {
                        let rows = batch.batch();
                        for row in 0..rows.num_rows() {
                            local.check_cancelled_at(row)?;
                            let read = |name| occupancy_value_at(&local.memory, rows, row, name, uncovered);
                            let id = read("_id")?.entity_id()?;
                            reservations.push(local.memory.reserve(id.as_value().retained_bytes()?.saturating_add(192))?);
                            if !after.insert(id) { return Err(uncovered()); }
                            match read(WorldShadow::FIELDS)? {
                                Value::Null => (),
                                Value::List(_) => members = members.checked_add(1).ok_or_else(overflow)?,
                                _ => return Err(uncovered()),
                            }
                        }
                    }
                }
                let finite = prepare_finite_unique_claims(
                    local, owner, overlay, schema, &unique_policy, &writes,
                ).await?;
                if !finite.paths.is_empty() || period_maximum.is_some() {
                    archive.check(ArchivedWorldConstraints {
                        database: &self.context.database,
                        placement: self.context.owned_witness_scope.ok_or_else(unique_uncovered)?,
                        owner, world, ordered, memory: &self.context.memory,
                        cache: Arc::clone(self.context.storage.budget()),
                        cancellation: &self.context.cancellation,
                        relation: &root,
                        valid: ordered,
                        purge_frontier: final_state.relation_event_frontier(&root.purge_table(owner))?,
                        before: BTreeSet::new(), after,
                        rules, unique: Vec::new(), finite_unique: (!finite.paths.is_empty()).then_some(finite),
                        period_count: period_maximum.map(|maximum| (members, maximum)), sample_counts: Vec::new(),
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
                    if row.is_tombstone()? { continue; }
                    let key = unique_key_bytes(row.binary(column, invalid_unique_key)?)?;
                    if !keys.contains_key(key) {
                        reservations.push(self.context.memory.reserve(key.len().saturating_add(192))?);
                        keys.insert(key.to_vec(), false);
                    }
                }
                unique.push(UniqueClaims { path, column, keys });
            }
            if rules.is_empty() && unique.is_empty() && sample_bounds.is_empty() { continue; }
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
            let mut sample_counts = Vec::new();
            for (prefix, maximum) in sample_bounds {
                let projection = SampleProjection::decode(prefix)?;
                if world.mode != WorldClockMode::Proposal || schema.has_opaque_columns()
                    || schema.lifetime_micros().is_some()
                    || projection.fields.iter().any(|name| schema.path_column_type(name) != Some(ScalarType::Binary))
                {
                    return Err(uncovered());
                }
                let local = self.context.query_context(
                    final_state.clone(), ordered, &self.options.default_tz, self.trace_parent,
                )?;
                let mut population = SamplePopulation::default();
                population.collect(local, owner, overlay, valid, &projection).await?;
                sample_counts.push((projection, population, maximum));
            }
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
                before, after, rules, unique, finite_unique: None, period_count: None, sample_counts,
            }).await?;
            drop(reservations);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finite_claim_checks_union_of_newer_writers() -> Result<()> {
        let claim = Interval::new(Timestamp(10), Timestamp(40))?;
        let local = Interval::new(Timestamp(10), Timestamp(20))?;
        let newer = Interval::new(Timestamp(20), Timestamp(30))?;
        let older = Interval::new(Timestamp(28), Timestamp(40))?;
        let late = Interval::new(Timestamp(31), Timestamp(40))?;
        assert!(!windows_cover(&mut [local], claim));
        assert!(windows_cover(&mut [newer, older, local], claim));
        assert!(!windows_cover(&mut [local, newer, late], claim));
        Ok(())
    }
}
