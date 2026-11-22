use std::collections::{BTreeMap, BTreeSet};

use rtdb_query::OwnedUniqueRule;

use rtdb_types::{EntityId, OwnedRelation};

use super::*;

pub(super) use rtdb_query::{OWNED_UNIQUE_KEY_MAX as MAX_KEY_BYTES, owned_unique_key_id as unique_id};

#[derive(Default)]
pub(super) struct OwnedUniqueChanges {
    paths: BTreeMap<OwnedRelation, Option<TouchedPath>>,
    reservations: Vec<Arc<MemoryReservation>>,
}

struct TouchedPath {
    rule: OwnedUniqueRule,
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
        &mut self, relation: &OwnedRelation, name: &OwnedRelation, column: &str,
    ) -> Result<()> {
        let owner = self.options.user.as_deref().filter(|owner| !owner.is_empty()).ok_or_else(no_owned_principal)?;
        let target = relation.table(owner);
        let storage = relation.unique_table(owner);
        let snapshot = self.snapshot()?;
        if snapshot.contains_table(&storage) {
            let frontier = snapshot.relation_event_frontier(&target)?;
            let context = self.context.query_context(snapshot, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?
                .with_accelerators(Arc::from([]));
            let existing = rtdb_query::read_owned_unique_rule(context, owner, relation).await?;
            if &existing.path == name && existing.column == column && existing.through_events == frontier {
                return Ok(());
            }
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/unique-path-exists", "The owned relation already has a different or uncertified unique path"));
        }
        if snapshot.relation_event_frontier(&target)? != 0
            || snapshot.iter_segments(&target).any(|segment| segment.row_count() != 0)
            || !snapshot.trie_entries(&target).is_empty()
            || snapshot.table_catalog().metadata(&target).is_some_and(|metadata| metadata.row_count != 0)
        {
            return Err(Anomaly::incorrect("rtdb.owned/unique-bootstrap", "A unique path must be declared before writing relation data"));
        }
        if column.is_empty() || column.len() > 96 || column.starts_with('_') {
            return Err(invalid_key());
        }
        if let Some((schema, _)) = self.read_owned_schema(owner, relation).await? {
            if schema.column_type(column) != Some(ScalarType::Binary) {
                return Err(invalid_key());
            }
        } else if snapshot.contains_table(&target) {
            let types = snapshot.column_types_for(&target, [column.to_owned()]);
            if types.get(column) != Some(&LogicalType::Scalar(ScalarType::Binary)) {
                return Err(invalid_key());
            }
        }
        let rule = OwnedUniqueRule { path: name.clone(), column: column.into(), through_events: 0 };
        self.buffer.put(storage, rule.document(), Interval::ALL)?;
        self.unique_changes.paths.insert(relation.clone(), Some(TouchedPath { rule, writes: 0, ids: BTreeSet::new() }));
        Ok(())
    }

    pub(super) async fn track_unique_mutation(
        &mut self, owner: &str, relation: &OwnedRelation, id: &EntityId, valid: Interval,
    ) -> Result<()> {
        if !self.unique_changes.paths.contains_key(relation) {
            let snapshot = self.snapshot()?;
            let path = if snapshot.contains_table(&relation.unique_table(owner)) {
                let context = self.context.query_context(snapshot, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
                let rule = rtdb_query::read_owned_unique_rule(context, owner, relation).await?;
                Some(TouchedPath { rule, writes: 0, ids: BTreeSet::new() })
            } else {
                None
            };
            self.unique_changes.reservations.push(self.context.memory.reserve(1024)?);
            self.unique_changes.paths.insert(relation.clone(), path);
        }
        let ordered = self.tx_key().system_time;
        if let Some(path) = self.unique_changes.paths.get_mut(relation).and_then(Option::as_mut) {
            if valid.lower() != ordered || valid.upper() != Timestamp::MAX || valid.is_empty() {
                return Err(Anomaly::incorrect("rtdb.owned/unique-valid-time", "Unique writes require validity from ordered present without an end"));
            }
            path.writes = path.writes.checked_add(1).ok_or_else(overflow)?;
            if !path.ids.contains(id) {
                let bytes = id.as_value().retained_bytes()?.checked_add(1024).ok_or_else(overflow)?;
                self.unique_changes.reservations.push(self.context.memory.reserve(bytes)?);
                path.ids.insert(id.clone());
            }
        }
        Ok(())
    }

    pub(super) async fn finish_owned_unique(&mut self) -> Result<()> {
        let changes = std::mem::take(&mut self.unique_changes);
        if changes.paths.values().flatten().all(|path| path.ids.is_empty()) { return Ok(()); }
        let owner = self.options.user.as_deref().filter(|owner| !owner.is_empty()).ok_or_else(no_owned_principal)?;
        let base = self.buffer.base_snapshot();
        let next = self.snapshot()?;
        for (relation, path) in changes.paths {
            let Some(TouchedPath { mut rule, ids, writes }) = path else { continue; };
            if writes == 0 { continue; }
            let target = relation.table(owner);
            let through_events = rule.through_events.checked_add(writes).ok_or_else(overflow)?;
            if base.relation_event_frontier(&target)? != rule.through_events
                || next.relation_event_frontier(&target)? != through_events {
                return Err(unique_uncovered());
            }

            let mut keys = BTreeMap::<Vec<u8>, KeyChange>::new();
            let mut allocations = Vec::new();
            for id in ids {
                for (snapshot, final_state) in [(&base, false), (&next, true)] {
                    let Some(key) = self.read_unique_key(snapshot.clone(), owner, &relation, &id, &rule.column).await? else { continue; };
                    allocations.push(self.context.memory.reserve(key.len().saturating_mul(4).saturating_add(512))?);
                    let change = keys.entry(key).or_default();
                    let slot = if final_state { &mut change.claim } else { &mut change.release };
                    if slot.replace(id.clone()).is_some() {
                        return Err(if final_state { unique_taken() } else { unique_uncovered() });
                    }
                }
            }
            let storage = relation.unique_table(owner);
            let mut locators = BTreeSet::new();
            for (key, change) in keys {
                self.context.check_cancelled()?;
                let locator = unique_id(&key);
                if !locators.insert(locator.iid()) {
                    return Err(unique_uncovered());
                }
                let occupant = self.read_unique_cell(base.clone(), owner, &relation, &locator, "holder").await?
                    .map(|value| value.entity_id().map_err(|_| unique_uncovered())).transpose()?;
                match (occupant.as_ref(), change.release.as_ref()) {
                    (Some(_), None) => return Err(unique_taken()),
                    (actual, expected) if actual != expected => return Err(unique_uncovered()),
                    _ => (),
                }
                if change.claim == change.release { continue; }
                if let Some(holder) = change.claim {
                    self.buffer.put(storage.clone(), StructFields::from([
                        ("_id".into(), locator.as_value()),
                        ("holder".into(), holder.as_value()),
                    ]), Interval::ALL)?;
                } else {
                    self.buffer.delete(storage.clone(), &locator, Interval::ALL)?;
                }
            }
            rule.through_events = through_events;
            self.buffer.put(storage, rule.document(), Interval::ALL)?;
        }
        Ok(())
    }

    async fn read_unique_key(
        &self, snapshot: Snapshot, owner: &str, relation: &OwnedRelation, id: &EntityId, column: &str,
    ) -> Result<Option<Vec<u8>>> {
        let context = self.context.query_context(snapshot, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
        let cursor = rtdb_query::open_owned_point(context, owner, relation, id, None, Some(&[column])).await?;
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
        let (slot, field) = row.schema_ref().column_with_name(column).ok_or_else(uncovered)?;
        let column = row.column(slot).as_ref();
        let bytes = rtdb_columnar::estimate_read_value_bytes(field, column, 0)?;
        let _read = self.context.memory.reserve(bytes.saturating_add(4096))?;
        rtdb_columnar::read_value(field, column, 0)
    }
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
