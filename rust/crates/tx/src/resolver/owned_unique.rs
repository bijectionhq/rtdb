use std::collections::{BTreeMap, BTreeSet};

use rtdb_query::{
    OWNED_UNIQUE_PATHS_MAX, OWNED_UNIQUE_POLICY_ID, OwnedUniquePolicy, owned_unique_path_key,
};

use rtdb_types::{EntityId, OwnedRelation};

use super::{replay_constraints::{ConstraintMember, ConstraintMembers}, *};
use crate::hosted_control::ReplayConstraintKind;

pub(super) use rtdb_query::{
    OWNED_UNIQUE_KEY_MAX as MAX_KEY_BYTES, owned_unique_key_id as unique_id,
};

#[derive(Default)]
pub(super) struct OwnedUniqueChanges {
    paths: BTreeMap<OwnedRelation, Option<TouchedPath>>,
    reservations: Vec<Arc<MemoryReservation>>,
}

struct TouchedPath {
    policy: OwnedUniquePolicy,
    writes: i64,
    affected: BTreeMap<EntityId, Vec<Interval>>,
}

#[derive(Default)]
struct KeyChange {
    releases: Vec<Segment<EntityId>>,
    claims: Vec<Segment<EntityId>>,
}

pub(super) struct Segment<T> {
    pub(super) valid: Interval,
    pub(super) value: T,
}

impl OpenTransaction<'_> {
    pub(super) async fn declare_owned_unique_path(
        &mut self,
        relation: &OwnedRelation,
        name: &OwnedRelation,
        column: &str,
    ) -> Result<()> {
        let owner = self
            .options
            .user
            .as_deref()
            .filter(|owner| !owner.is_empty())
            .ok_or_else(no_owned_principal)?;
        let target = relation.table(owner);
        let storage = relation.unique_table(owner);
        let snapshot = self.snapshot()?;
        let frontier = snapshot.relation_event_frontier(&target)?;
        let mut policy = self
            .unique_policy(snapshot.clone(), owner, relation)
            .await?;
        if !policy.paths.is_empty() && policy.through_events != frontier {
            return Err(unique_uncovered());
        }
        if let Some(existing) = policy.paths.get(name) {
            if existing == column {
                return Ok(());
            }
            return Err(Anomaly::new(
                AnomalyCategory::Conflict,
                "rtdb.owned/unique-path-exists",
                "The owned path already has another projection",
            ));
        }
        if frontier != 0
            || snapshot
                .iter_segments(&target)
                .any(|segment| segment.row_count() != 0)
            || !snapshot.trie_entries(&target).is_empty()
            || snapshot
                .table_catalog()
                .metadata(&target)
                .is_some_and(|metadata| metadata.row_count != 0)
        {
            return Err(Anomaly::incorrect(
                "rtdb.owned/unique-bootstrap",
                "A unique path must be declared before writing relation data",
            ));
        }
        if policy.paths.len() >= OWNED_UNIQUE_PATHS_MAX
            || column.is_empty()
            || column.len() > 96
            || column.starts_with('_')
        {
            return Err(invalid_key());
        }
        if let Some((schema, _)) = self.read_owned_schema(owner, relation).await? {
            if schema.lifetime_micros().is_some() || schema.path_column_type(column) != Some(ScalarType::Binary) {
                return Err(invalid_key());
            }
        } else if snapshot.contains_table(&target) {
            let types = snapshot.column_types_for(&target, [column.to_owned()]);
            if types.get(column) != Some(&LogicalType::Scalar(ScalarType::Binary)) {
                return Err(invalid_key());
            }
        }
        policy.paths.insert(name.clone(), column.into());
        self.unique_changes.reservations.push(
            self.context
                .memory
                .reserve(1024 + policy.paths.len() * 256)?,
        );
        self.buffer.put(storage, policy.document(), Interval::ALL)?;
        self.unique_changes.paths.insert(
            relation.clone(),
            Some(TouchedPath {
                policy,
                writes: 0,
                affected: BTreeMap::new(),
            }),
        );
        Ok(())
    }

    pub(super) async fn track_unique_mutation(
        &mut self,
        owner: &str,
        relation: &OwnedRelation,
        id: &EntityId,
        valid: Interval,
    ) -> Result<()> {
        if !self.unique_changes.paths.contains_key(relation) {
            let snapshot = self.snapshot()?;
            let policy = self.unique_policy(snapshot, owner, relation).await?;
            self.unique_changes.reservations.push(
                self.context
                    .memory
                    .reserve(1024 + policy.paths.len() * 256)?,
            );
            let path = (!policy.paths.is_empty()).then_some(TouchedPath {
                policy,
                writes: 0,
                affected: BTreeMap::new(),
            });
            self.unique_changes.paths.insert(relation.clone(), path);
        }
        if let Some(path) = self
            .unique_changes
            .paths
            .get_mut(relation)
            .and_then(Option::as_mut)
        {
            if valid.is_empty() {
                return Err(invalid_key());
            }
            path.writes = path.writes.checked_add(1).ok_or_else(overflow)?;
            let new_id = if path.affected.contains_key(id) {
                0
            } else {
                id.as_value().retained_bytes()?.checked_add(1024).ok_or_else(overflow)?
            };
            self.unique_changes.reservations.push(self.context.memory.reserve(new_id + 64)?);
            path.affected.entry(id.clone()).or_default().push(valid);
        }
        Ok(())
    }

    pub(super) async fn finish_owned_unique(&mut self) -> Result<()> {
        let changes = std::mem::take(&mut self.unique_changes);
        if changes
            .paths
            .values()
            .flatten()
            .all(|path| path.affected.is_empty())
        {
            return Ok(());
        }
        let owner = self
            .options
            .user
            .as_deref()
            .filter(|owner| !owner.is_empty())
            .ok_or_else(no_owned_principal)?
            .to_owned();
        let base = self.buffer.base_snapshot();
        let next = self.snapshot()?;
        for (relation, path) in changes.paths {
            let Some(TouchedPath {
                mut policy,
                affected,
                writes,
            }) = path
            else {
                continue;
            };
            if writes == 0 {
                continue;
            }
            let target = relation.table(&owner);
            let through_events = policy
                .through_events
                .checked_add(writes)
                .ok_or_else(overflow)?;
            if base.relation_event_frontier(&target)? != policy.through_events
                || next.relation_event_frontier(&target)? != through_events
            {
                return Err(unique_uncovered());
            }
            let _locators = self
                .context
                .memory
                .reserve(64 + affected.len() * policy.paths.len() * 128)?;
            let mut locators =
                BTreeSet::from([EntityId::String(OWNED_UNIQUE_POLICY_ID.into()).iid()]);
            let mut proof = self.hosted_replay.as_ref().map(|_| ConstraintMembers::default());
            for (path, column) in &policy.paths {
                self.apply_unique_path(
                    &owner,
                    &relation,
                    path,
                    column,
                    &affected,
                    &base,
                    &next,
                    &mut locators,
                    proof.as_mut(),
                )
                .await?;
            }
            if let (Some(replay), Some(proof)) = (&mut self.hosted_replay, proof) {
                replay.match_constraint(
                    &relation, ReplayConstraintKind::Unique, writes, through_events, proof.finish(), 0,
                )?;
            }
            policy.through_events = through_events;
            self.buffer.put(
                relation.unique_table(&owner),
                policy.document(),
                Interval::ALL,
            )?;
        }
        Ok(())
    }

    async fn apply_unique_path(
        &mut self,
        owner: &str,
        relation: &OwnedRelation,
        path: &OwnedRelation,
        column: &str,
        affected: &BTreeMap<EntityId, Vec<Interval>>,
        base: &Snapshot,
        next: &Snapshot,
        locators: &mut BTreeSet<Iid>,
        mut proof: Option<&mut ConstraintMembers>,
    ) -> Result<()> {
        let mut keys = BTreeMap::<Vec<u8>, KeyChange>::new();
        let mut allocations = Vec::new();
        for (id, windows) in affected {
            for valid in merge_windows(windows.clone())? {
                for (snapshot, final_state) in [(base, false), (next, true)] {
                    let context = self.context.query_context(
                        snapshot.clone(), self.tx_key().system_time, &self.options.default_tz, self.trace_parent,
                    )?;
                    let cursor = rtdb_query::open_owned_key_timeline(
                        context, owner, relation, id, valid, column,
                    ).await?;
                    for part in self.read_unique_segments(cursor, id, valid, column, &mut allocations).await? {
                        let key = unique_key(part.value)?;
                        allocations.push(self.context.memory.reserve(id.as_value().retained_bytes()?.saturating_add(256))?);
                        let change = keys.entry(key).or_default();
                        let side = if final_state { &mut change.claims } else { &mut change.releases };
                        side.push(Segment { valid: part.valid, value: id.clone() });
                    }
                }
            }
        }
        let storage = relation.unique_table(owner);
        for (key, change) in keys {
            self.context.check_cancelled()?;
            let locator = owned_unique_path_key(path, &key);
            if !locators.insert(locator.iid()) {
                return Err(unique_uncovered());
            }
            let windows = change.releases.iter().chain(&change.claims).map(|part| part.valid).collect();
            let mut holders = Vec::new();
            for valid in merge_windows(windows)? {
                let context = self.context.query_context(
                    base.clone(), self.tx_key().system_time, &self.options.default_tz, self.trace_parent,
                )?;
                let cursor = rtdb_query::open_owned_unique_timeline(context, owner, relation, &locator, valid).await?;
                for part in self.read_unique_segments(cursor, &locator, valid, "holder", &mut allocations).await? {
                    holders.push(Segment { valid: part.valid, value: part.value.entity_id().map_err(|_| unique_uncovered())? });
                }
            }
            for Segment { valid, value: holder } in final_holders(change, holders)? {
                if let Some(proof) = proof.as_deref_mut() {
                    proof.insert(&locator, ConstraintMember::Occupancy { valid, holder: holder.as_ref() }, &self.context.memory)?;
                }
                if let Some(holder) = holder {
                    self.buffer.put(
                        storage.clone(),
                        StructFields::from([
                            ("_id".into(), locator.as_value()),
                            ("holder".into(), holder.as_value()),
                        ]),
                        valid,
                    )?;
                } else {
                    self.buffer.delete(storage.clone(), &locator, valid)?;
                }
            }
        }
        Ok(())
    }

    async fn read_unique_segments(
        &self,
        cursor: Option<QueryCursor>,
        id: &EntityId,
        window: Interval,
        column: &str,
        allocations: &mut Vec<Arc<MemoryReservation>>,
    ) -> Result<Vec<Segment<Value>>> {
        let Some(mut cursor) = cursor else { return Ok(Vec::new()); };
        let mut segments = Vec::new();
        let mut through = window.lower();
        while let Some(batch) = cursor.next_batch().await? {
            self.context.check_cancelled()?;
            let batch = batch.batch();
            for row in 0..batch.num_rows() {
                rtdb_query::owned_row_revision(batch, row, id, &self.context.memory)?;
                let valid = unique_segment_interval(batch, row, window)?;
                if valid.lower() < through {
                    return Err(unique_uncovered());
                }
                through = valid.upper();
                let value = self.read_occupancy_value_at(batch, row, column, unique_uncovered)?;
                allocations.push(self.context.memory.reserve(value.retained_bytes()?.saturating_add(512))?);
                segments.push(Segment { valid, value });
            }
        }
        Ok(segments)
    }

    pub(super) async fn check_unique_schema(
        &self,
        snapshot: Snapshot,
        owner: &str,
        relation: &OwnedRelation,
        schema: &rtdb_types::OwnedSchema,
    ) -> Result<()> {
        let policy = self.unique_policy(snapshot, owner, relation).await?;
        if policy
            .paths
            .values()
            .any(|column| schema.path_column_type(column) != Some(ScalarType::Binary))
        {
            return Err(invalid_key());
        }
        Ok(())
    }

    pub(super) async fn unique_policy(
        &self,
        snapshot: Snapshot,
        owner: &str,
        relation: &OwnedRelation,
    ) -> Result<OwnedUniquePolicy> {
        if !snapshot.contains_table(&relation.unique_table(owner)) {
            return Ok(OwnedUniquePolicy::default());
        }
        let context = self
            .context
            .query_context(
                snapshot,
                self.tx_key().system_time,
                &self.options.default_tz,
                self.trace_parent,
            )?
            .with_accelerators(Arc::from([]));
        rtdb_query::read_owned_unique_policy(context, owner, relation).await
    }

    pub(super) async fn read_occupancy_cell(&self, cursor: Option<QueryCursor>, id: &EntityId, column: &str, uncovered: fn() -> Anomaly) -> Result<Option<Value>> {
        self.read_occupancy_row(cursor, id, uncovered).await?
            .map(|row| self.read_occupancy_value(row.batch(), column, uncovered)).transpose()
    }

    pub(super) async fn read_occupancy_row(&self, cursor: Option<QueryCursor>, id: &EntityId, uncovered: fn() -> Anomaly) -> Result<Option<OwnedBatch>> {
        let Some(mut cursor) = cursor else { return Ok(None); };
        let mut row = None;
        while let Some(owned) = cursor.next_batch().await? {
            let batch = owned.batch();
            if batch.num_rows() == 0 { continue; }
            if row.is_some() || batch.num_rows() != 1 { return Err(uncovered()); }
            rtdb_query::owned_row_revision(batch, 0, id, &self.context.memory)?;
            row = Some(owned);
        }
        Ok(row)
    }

    pub(super) fn read_occupancy_value(&self, row: &RecordBatch, column: &str, uncovered: fn() -> Anomaly) -> Result<Value> {
        self.read_occupancy_value_at(row, 0, column, uncovered)
    }

    pub(super) fn read_occupancy_value_at(&self, rows: &RecordBatch, row: usize, column: &str, uncovered: fn() -> Anomaly) -> Result<Value> {
        occupancy_value_at(&self.context.memory, rows, row, column, uncovered)
    }
}

pub(super) fn unique_segment_interval(
    batch: &RecordBatch, row: usize, window: Interval,
) -> Result<Interval> {
    let edge = |name: &str| {
        batch.column_by_name(name)
            .and_then(|array| array.as_any().downcast_ref::<arrow_array::TimestampMicrosecondArray>())
            .ok_or_else(unique_uncovered)
    };
    let start = edge("_valid_from")?;
    let end = edge("_valid_to")?;
    if start.is_null(row) {
        return Err(unique_uncovered());
    }
    let lower = Timestamp(start.value(row));
    let upper = if end.is_null(row) { Timestamp::MAX } else { Timestamp(end.value(row)) };
    if lower < window.lower() || lower >= upper || upper > window.upper() {
        return Err(unique_uncovered());
    }
    Interval::new(lower, upper)
}

pub(super) fn merge_windows(mut windows: Vec<Interval>) -> Result<Vec<Interval>> {
    windows.sort_unstable();
    let mut merged: Vec<Interval> = Vec::new();
    for window in windows {
        if let Some(last) = merged.last_mut().filter(|last| window.lower() <= last.upper()) {
            *last = Interval::new(last.lower(), last.upper().max(window.upper()))?;
        } else {
            merged.push(window);
        }
    }
    Ok(merged)
}

fn final_holders(
    mut change: KeyChange,
    mut existing: Vec<Segment<EntityId>>,
) -> Result<Vec<Segment<Option<EntityId>>>> {
    ordered_segments(&mut change.claims, unique_taken)?;
    ordered_segments(&mut change.releases, unique_uncovered)?;
    ordered_segments(&mut existing, unique_uncovered)?;
    let edges: BTreeSet<_> = change.claims.iter().chain(&change.releases).chain(&existing)
        .flat_map(|part| [part.valid.lower(), part.valid.upper()]).collect();
    let mut updates: Vec<Segment<Option<EntityId>>> = Vec::new();
    let (mut old, mut releases, mut claims) = (existing.as_slice(), change.releases.as_slice(), change.claims.as_slice());
    for (lower, upper) in edges.iter().copied().zip(edges.iter().copied().skip(1)) {
        let holder = holder_at(&mut old, lower);
        let release = holder_at(&mut releases, lower);
        let claim = holder_at(&mut claims, lower);
        if release.is_some() && release != holder {
            return Err(unique_uncovered());
        }
        if claim.is_some() && release.is_none() && holder.is_some() {
            return Err(unique_taken());
        }
        if (release.is_none() && claim.is_none()) || holder == claim {
            continue;
        }
        if let Some(last) = updates.last_mut().filter(|last| {
            last.valid.upper() == lower && last.value.as_ref() == claim
        }) {
            last.valid = Interval::new(last.valid.lower(), upper)?;
        } else {
            updates.push(Segment { valid: Interval::new(lower, upper)?, value: claim.cloned() });
        }
    }
    Ok(updates)
}

pub(super) fn ordered_segments(segments: &mut [Segment<EntityId>], overlap: fn() -> Anomaly) -> Result<()> {
    segments.sort_unstable_by_key(|part| part.valid);
    if segments.windows(2).any(|pair| pair[0].valid.intersects(pair[1].valid)) {
        return Err(overlap());
    }
    Ok(())
}

fn holder_at<'a>(parts: &mut &'a [Segment<EntityId>], at: Timestamp) -> Option<&'a EntityId> {
    while let Some((head, tail)) = (*parts).split_first() {
        if head.valid.upper() > at {
            return head.valid.contains(at).then_some(&head.value);
        }
        *parts = tail;
    }
    None
}

pub(super) fn occupancy_value_at(
    memory: &Arc<MemoryBudget>, rows: &RecordBatch, row: usize, column: &str, uncovered: fn() -> Anomaly,
) -> Result<Value> {
    let (slot, field) = rows.schema_ref().column_with_name(column).ok_or_else(uncovered)?;
    let column = rows.column(slot).as_ref();
    let bytes = rtdb_columnar::estimate_read_value_bytes(field, column, row)?;
    let _read = memory.reserve(bytes.saturating_add(4096))?;
    rtdb_columnar::read_value(field, column, row)
}

pub(super) fn unique_taken() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/unique-taken", "The owned unique key is already occupied")
}

pub(super) fn unique_uncovered() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/unique-uncovered", "The owned unique path cannot verify its occupancy")
}

pub(super) fn unique_key(value: Value) -> Result<Vec<u8>> {
    match value {
        Value::Binary(key) if (1..=MAX_KEY_BYTES).contains(&key.len()) => Ok(key),
        _ => Err(invalid_key()),
    }
}

pub(super) fn unique_key_bytes(value: Option<&[u8]>) -> Result<&[u8]> {
    value.filter(|key| (1..=MAX_KEY_BYTES).contains(&key.len())).ok_or_else(invalid_key)
}

pub(super) fn invalid_key() -> Anomaly {
    Anomaly::incorrect("rtdb.owned/unique-key", "The unique path requires a nonempty Binary key of at most 4096 bytes")
}
