use std::{collections::{BTreeMap, BTreeSet}, fmt::Write};

use rtdb_types::{EntityId, OwnedRelation};

use super::*;

pub(super) const MAX_KEY_BYTES: usize = 4096;
const DECLARATION_ID: &str = "!path";

#[derive(Default)]
pub(super) struct OwnedUniqueChanges {
    paths: BTreeMap<OwnedRelation, Option<TouchedPath>>,
    reservations: Vec<Arc<MemoryReservation>>,
}

struct TouchedPath {
    column: String,
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
            let declaration = EntityId::String(DECLARATION_ID.into());
            let existing_name = self.read_unique_cell(snapshot.clone(), owner, relation, &declaration, "path").await?;
            let existing_column = self.read_unique_cell(snapshot, owner, relation, &declaration, "column").await?;
            if existing_name == Some(Value::Utf8(name.name().into()))
                && existing_column == Some(Value::Utf8(column.into()))
            {
                return Ok(());
            }
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/unique-path-exists", "The owned relation already has a different unique path"));
        }
        if snapshot.iter_segments(&target).any(|segment| segment.row_count() != 0)
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
        let document = StructFields::from([
            ("_id".into(), Value::Utf8(DECLARATION_ID.into())),
            ("path".into(), Value::Utf8(name.name().into())),
            ("column".into(), Value::Utf8(column.into())),
        ]);
        self.buffer.put(storage, document, Interval::ALL)?;
        self.unique_changes.paths.insert(relation.clone(), Some(TouchedPath { column: column.into(), ids: BTreeSet::new() }));
        Ok(())
    }

    pub(super) async fn track_unique_mutation(
        &mut self, owner: &str, relation: &OwnedRelation, id: &EntityId, valid: Interval,
    ) -> Result<()> {
        if !self.unique_changes.paths.contains_key(relation) {
            let snapshot = self.snapshot()?;
            let path = if snapshot.contains_table(&relation.unique_table(owner)) {
                let id = EntityId::String(DECLARATION_ID.into());
                let Some(Value::Utf8(column)) = self.read_unique_cell(snapshot, owner, relation, &id, "column").await? else {
                    return Err(unique_uncovered());
                };
                Some(TouchedPath { column, ids: BTreeSet::new() })
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
            let Some(path) = path else { continue; };
            if path.ids.is_empty() { continue; }
            let mut keys = BTreeMap::<Vec<u8>, KeyChange>::new();
            let mut allocations = Vec::new();
            for id in path.ids {
                for (snapshot, final_state) in [(&base, false), (&next, true)] {
                    let Some(key) = self.read_unique_key(snapshot.clone(), owner, &relation, &id, &path.column).await? else { continue; };
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
        let Some(mut cursor) = cursor else { return Ok(None); };
        let mut value = None;
        while let Some(batch) = cursor.next_batch().await? {
            let batch = batch.batch();
            if batch.num_rows() == 0 { continue; }
            if value.is_some() || batch.num_rows() != 1 { return Err(uncovered()); }
            rtdb_query::owned_row_revision(batch, 0, id, &self.context.memory)?;
            let (slot, field) = batch.schema_ref().column_with_name(column).ok_or_else(uncovered)?;
            let column = batch.column(slot).as_ref();
            let bytes = rtdb_columnar::estimate_read_value_bytes(field, column, 0)?;
            let _read = self.context.memory.reserve(bytes.saturating_add(4096))?;
            value = Some(rtdb_columnar::read_value(field, column, 0)?);
        }
        Ok(value)
    }
}

pub(super) fn unique_id(key: &[u8]) -> EntityId {
    let mut name = String::with_capacity(1 + key.len() * 2);
    name.push('k');
    for byte in key { write!(&mut name, "{byte:02x}").expect("writing to a string cannot fail"); }
    EntityId::String(name)
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
