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
    ids: BTreeSet<EntityId>,
}

#[derive(Default)]
struct KeyChange {
    release: Option<EntityId>,
    claim: Option<EntityId>,
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
            if schema.path_column_type(column) != Some(ScalarType::Binary) {
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
                ids: BTreeSet::new(),
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
                ids: BTreeSet::new(),
            });
            self.unique_changes.paths.insert(relation.clone(), path);
        }
        let valid_at = self.owned_constraint_present();
        if let Some(path) = self
            .unique_changes
            .paths
            .get_mut(relation)
            .and_then(Option::as_mut)
        {
            if valid.lower() != valid_at || valid.upper() != Timestamp::MAX || valid.is_empty() {
                return Err(Anomaly::incorrect(
                    "rtdb.owned/unique-valid-time",
                    "Unique writes require validity from ordered present without an end",
                ));
            }
            path.writes = path.writes.checked_add(1).ok_or_else(overflow)?;
            if !path.ids.contains(id) {
                let bytes = id
                    .as_value()
                    .retained_bytes()?
                    .checked_add(1024)
                    .ok_or_else(overflow)?;
                self.unique_changes
                    .reservations
                    .push(self.context.memory.reserve(bytes)?);
                path.ids.insert(id.clone());
            }
        }
        Ok(())
    }

    pub(super) async fn finish_owned_unique(&mut self) -> Result<()> {
        let changes = std::mem::take(&mut self.unique_changes);
        if changes
            .paths
            .values()
            .flatten()
            .all(|path| path.ids.is_empty())
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
                ids,
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
                .reserve(64 + ids.len() * policy.paths.len() * 128)?;
            let mut locators =
                BTreeSet::from([EntityId::String(OWNED_UNIQUE_POLICY_ID.into()).iid()]);
            let mut proof = self.hosted_replay.as_ref().map(|_| ConstraintMembers::default());
            for (path, column) in &policy.paths {
                self.apply_unique_path(
                    &owner,
                    &relation,
                    path,
                    column,
                    &ids,
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
        ids: &BTreeSet<EntityId>,
        base: &Snapshot,
        next: &Snapshot,
        locators: &mut BTreeSet<Iid>,
        mut proof: Option<&mut ConstraintMembers>,
    ) -> Result<()> {
        let mut keys = BTreeMap::<Vec<u8>, KeyChange>::new();
        let mut allocations = Vec::new();
        for id in ids {
            for (snapshot, final_state) in [(base, false), (next, true)] {
                let Some(key) = self
                    .read_unique_key(snapshot.clone(), owner, relation, id, column)
                    .await?
                else {
                    continue;
                };
                allocations.push(
                    self.context
                        .memory
                        .reserve((key.len() + path.name().len()) * 4 + 512)?,
                );
                let change = keys.entry(key).or_default();
                let slot = if final_state {
                    &mut change.claim
                } else {
                    &mut change.release
                };
                if slot.replace(id.clone()).is_some() {
                    return Err(if final_state {
                        unique_taken()
                    } else {
                        unique_uncovered()
                    });
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
            let occupant = self
                .read_unique_cell(base.clone(), owner, relation, &locator, "holder")
                .await?
                .map(|value| value.entity_id().map_err(|_| unique_uncovered()))
                .transpose()?;
            match (occupant.as_ref(), change.release.as_ref()) {
                (Some(_), None) => return Err(unique_taken()),
                (actual, expected) if actual != expected => return Err(unique_uncovered()),
                _ => (),
            }
            if change.claim == change.release {
                continue;
            }
            if let Some(proof) = proof.as_deref_mut() {
                let member = change.claim.as_ref().map_or(ConstraintMember::Removed, ConstraintMember::Holder);
                proof.insert(&locator, member, &self.context.memory)?;
            }
            if let Some(holder) = change.claim {
                self.buffer.put(
                    storage.clone(),
                    StructFields::from([
                        ("_id".into(), locator.as_value()),
                        ("holder".into(), holder.as_value()),
                    ]),
                    Interval::ALL,
                )?;
            } else {
                self.buffer
                    .delete(storage.clone(), &locator, Interval::ALL)?;
            }
        }
        Ok(())
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

    async fn unique_policy(
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

    async fn read_unique_key(
        &self, snapshot: Snapshot, owner: &str, relation: &OwnedRelation, id: &EntityId, column: &str,
    ) -> Result<Option<Vec<u8>>> {
        let context = self.context.query_context(snapshot, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
        let valid = Interval::at(self.owned_constraint_present())?;
        let cursor = rtdb_query::open_owned_point(context, owner, relation, id, Some(valid), Some(&[column])).await?;
        match self.read_occupancy_cell(cursor, id, column, unique_uncovered).await? {
            None => Ok(None),
            Some(Value::Binary(key)) if !key.is_empty() && key.len() <= MAX_KEY_BYTES => Ok(Some(key)),
            Some(_) => Err(invalid_key()),
        }
    }

    async fn read_unique_cell(
        &self, snapshot: Snapshot, owner: &str, relation: &OwnedRelation, id: &EntityId, column: &str,
    ) -> Result<Option<Value>> {
        let context = self.context.query_context(snapshot, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
        let cursor = rtdb_query::open_owned_unique_point(context, owner, relation, id, &[column]).await?;
        self.read_occupancy_cell(cursor, id, column, unique_uncovered).await
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

pub(super) fn occupancy_value_at(
    memory: &Arc<MemoryBudget>, rows: &RecordBatch, row: usize, column: &str, uncovered: fn() -> Anomaly,
) -> Result<Value> {
    let (slot, field) = rows.schema_ref().column_with_name(column).ok_or_else(uncovered)?;
    let column = rows.column(slot).as_ref();
    let bytes = rtdb_columnar::estimate_read_value_bytes(field, column, row)?;
    let _read = memory.reserve(bytes.saturating_add(4096))?;
    rtdb_columnar::read_value(field, column, row)
}

fn unique_taken() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/unique-taken", "The owned unique key is already occupied")
}

fn unique_uncovered() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/unique-uncovered", "The owned unique path cannot verify its occupancy")
}

fn invalid_key() -> Anomaly {
    Anomaly::incorrect("rtdb.owned/unique-key", "The unique path requires a nonempty Binary key of at most 4096 bytes")
}
