use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

use rtdb_columnar::MemoryBudget;
use rtdb_format::data::StoredOperation;
use rtdb_index::{IndexBatch, TableWrite};
use rtdb_types::{EventOp, Iid, Interval, OWNED_ATTEMPT_RELATION, OWNED_ROOT_PUBLICATION_RELATION, OwnedRelation, OwnedWorld, Result, StructFields, TableRef, Timestamp, TransactionKey, Value};

use crate::{TxOp, source::Operation};
use super::invalid;

pub(crate) const WORLD_EFFECTS: &str = "owned_world_effects";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind { Root, Overlay, Clock, Fork, Drop }

const ROOT: Iid = Iid([0; 16]);

impl Kind {
    fn writes_data(self) -> bool {
        matches!(self, Self::Root | Self::Overlay)
    }

    fn name(self) -> &'static str {
        match self {
            Self::Root => "root",
            Self::Overlay => "overlay",
            Self::Clock => "clock",
            Self::Fork => "fork",
            Self::Drop => "drop",
        }
    }

    fn parse(name: &str) -> Result<Self> {
        match name {
            "root" => Ok(Self::Root),
            "overlay" => Ok(Self::Overlay),
            "clock" => Ok(Self::Clock),
            "fork" => Ok(Self::Fork),
            "drop" => Ok(Self::Drop),
            _ => Err(mismatch()),
        }
    }
}

pub(crate) fn source_inventory(operations: &[Operation]) -> Result<Vec<Value>> {
    let mut selected = BTreeMap::<Iid, BTreeSet<&str>>::new();
    for operation in operations {
        match operation {
            Operation::Public(TxOp::OwnedInsertDocs { relation, documents, .. } | TxOp::OwnedReplaceDocs { relation, documents, .. })
            | Operation::OwnedWriteIids { relation, documents, .. } if !documents.rows().is_empty() => {
                selected.entry(relation.world().unwrap_or(ROOT)).or_default().insert(relation.name());
            }
            Operation::Public(TxOp::OwnedDelete { relation, .. }) => {
                selected.entry(ROOT).or_default().insert(relation.name());
            }
            _ => (),
        }
    }
    operations.iter().filter_map(|operation| match operation {
        Operation::Public(TxOp::OwnedForkWorld { record, attempt, fingerprint }) => Some(
            match record.get("_id") {
                Some(Value::Uuid(world)) => Ok((Iid(*world), Kind::Fork, attempt, fingerprint)),
                _ => Err(mismatch()),
            }),
        Operation::Public(TxOp::OwnedPublishRoot { attempt, fingerprint }) =>
            Some(Ok((ROOT, Kind::Root, attempt, fingerprint))),
        Operation::Public(TxOp::OwnedPublishWorld { world, attempt, fingerprint, advance_to, drop_world, .. }) => {
            let kind = if *drop_world { Kind::Drop } else if advance_to.is_some() { Kind::Clock } else { Kind::Overlay };
            Some(Ok((*world, kind, attempt, fingerprint)))
        }
        _ => None,
    }).map(|effect| {
        let (world, kind, attempt, fingerprint) = effect?;
        let relations = selected.remove(&world).unwrap_or_default();
        if relations.is_empty() == kind.writes_data() { return Err(mismatch()); }
        Ok(Value::Struct(StructFields::from([
            ("world".into(), Value::Uuid(world.0)),
            ("kind".into(), Value::Utf8(kind.name().to_owned())),
            ("attempt".into(), Value::Utf8(attempt.clone())),
            ("fingerprint".into(), Value::Binary(fingerprint.to_vec())),
            ("relations".into(), Value::List(relations.into_iter().map(|name| Value::Utf8(name.to_owned())).collect())),
        ])))
    }).collect()
}

pub(super) fn verify(
    tables: &BTreeMap<TableRef, TableWrite>, effects: &[Value], key: TransactionKey,
    memory: &Arc<MemoryBudget>,
) -> Result<()> {
    if !(1..=9).contains(&effects.len()) { return Err(mismatch()); }
    let mut worlds = BTreeMap::new();
    let mut attempts = BTreeMap::new();
    let mut expected_data = BTreeSet::new();
    let mut common_fingerprint: Option<&[u8]> = None;
    for effect in effects {
        let Value::Struct(fields) = effect else { return Err(mismatch()); };
        let (Some(Value::Uuid(world)), Some(Value::Utf8(kind)), Some(Value::Utf8(attempt)), Some(Value::Binary(fingerprint)), Some(Value::List(relations))) =
            (fields.get("world"), fields.get("kind"), fields.get("attempt"), fields.get("fingerprint"), fields.get("relations"))
        else { return Err(mismatch()); };
        let kind = Kind::parse(kind)?;
        let world = Iid(*world);
        if fields.len() != 5 || relations.is_empty() == kind.writes_data()
            || (world == ROOT) != (kind == Kind::Root) || fingerprint.len() != 32
            || attempt.len() != 22 || !attempt.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || (effects.len() > 1 && kind == Kind::Fork)
            || common_fingerprint.is_some_and(|first| first != fingerprint.as_slice())
            || worlds.insert(world, kind).is_some()
            || attempts.insert(attempt.as_str(), fingerprint.as_slice()).is_some()
        {
            return Err(mismatch());
        }
        for relation in relations {
            let Value::Utf8(name) = relation else { return Err(mismatch()); };
            let relation = OwnedRelation::from_source(name.clone()).map_err(|_| mismatch())?;
            if relation.is_world_catalog() || matches!(name.as_str(), OWNED_ATTEMPT_RELATION | OWNED_ROOT_PUBLICATION_RELATION)
                || relation.world().unwrap_or(ROOT) != world
                || !expected_data.insert(name.as_str()) || expected_data.len() > 4096
            {
                return Err(mismatch());
            }
        }
        common_fingerprint = Some(fingerprint);
    }
    let has_root = worlds.contains_key(&ROOT);
    if has_root && worlds.len() == 1 || worlds.len() - usize::from(has_root) > 8 {
        return Err(mismatch());
    }
    if has_root {
        expected_data.insert(OWNED_ROOT_PUBLICATION_RELATION);
    }
    let mut root_head = false;
    let mut data = BTreeSet::new();
    let mut journals = BTreeSet::new();
    let mut overlays = BTreeSet::new();
    let mut clocks = BTreeSet::new();
    let mut registrations = BTreeSet::new();
    for (table, write) in tables {
        let Some(name) = table.owned_storage_name() else { continue; };
        let batch = write.batch().filter(|batch| !batch.is_empty()).ok_or_else(mismatch)?;
        if name == OWNED_ATTEMPT_RELATION {
            records(batch, key, memory, |row| {
                let (Some(Value::Utf8(id)), Some(Value::Binary(fingerprint))) = (row.get("_id"), row.get("fingerprint"))
                else { return Err(mismatch()); };
                if row.len() != 2 || attempts.remove(id.as_str()) != Some(fingerprint.as_slice()) {
                    return Err(mismatch());
                }
                Ok(())
            })?;
        } else if table.is_owned_world_clock_table() {
            records(batch, key, memory, |row| {
                let (Some(Value::Uuid(world)), Some(Value::I64(clock))) = (row.get("_id"), row.get("clock"))
                else { return Err(mismatch()); };
                let world = Iid(*world);
                if row.len() != 2 || *clock <= Timestamp::MIN.0 || *clock > key.system_time.0
                    || !matches!(worlds.get(&world), Some(Kind::Overlay | Kind::Clock))
                    || !clocks.insert(world)
                {
                    return Err(mismatch());
                }
                Ok(())
            })?;
        } else if table.owned_world_scope().0 {
            records(batch, key, memory, |row| {
                let record = OwnedWorld::from_record(row).map_err(|_| mismatch())?;
                if record.basis.system_time >= key.system_time || record.until <= key.system_time
                    || worlds.get(&record.id) != Some(&if record.dropped { Kind::Drop } else { Kind::Fork })
                    || !registrations.insert(record.id)
                {
                    return Err(mismatch());
                }
                Ok(())
            })?;
        } else if let Some(relation) = table.owned_root_constraint_relation() {
            if !has_root || !expected_data.contains(relation) || relation == OWNED_ROOT_PUBLICATION_RELATION
                || OwnedRelation::new(relation).is_err()
            {
                return Err(mismatch());
            }
            event_headers(batch, key, true)?;
        } else {
            let relation = table.owned_effect_relation_name().ok_or_else(mismatch)?;
            let is_data = name == relation;
            let world = OwnedRelation::from_source(relation.to_owned()).map_err(|_| mismatch())?.world();
            if let Some(world) = world {
                if worlds.get(&world) != Some(&Kind::Overlay) { return Err(mismatch()); }
                if is_data { overlays.insert(world); }
            }
            if is_data {
                if relation == OWNED_ROOT_PUBLICATION_RELATION {
                    if !has_root || batch.len() != 1 { return Err(mismatch()); }
                    records(batch, key, memory, |row| {
                        if row.len() != 1 || !matches!(row.get("_id"), Some(Value::Utf8(id)) if id == "head") {
                            return Err(mismatch());
                        }
                        Ok(())
                    })?;
                    root_head = true;
                }
                data.insert(relation);
            } else if table.is_owned_change_journal() {
                journals.insert(relation);
            }
            let allows_delete = is_data && world.is_none() && relation != OWNED_ROOT_PUBLICATION_RELATION;
            event_headers(batch, key, allows_delete)?;
        }
    }
    if !attempts.is_empty() || root_head != has_root || data != expected_data
        || !journals.remove(OWNED_ATTEMPT_RELATION) || journals != expected_data
        || worlds.iter().any(|(world, kind)| match kind {
            Kind::Root => false,
            Kind::Overlay => !overlays.contains(world) || !clocks.contains(world),
            Kind::Clock => !clocks.contains(world),
            Kind::Fork | Kind::Drop => !registrations.contains(world),
        })
    {
        return Err(mismatch());
    }
    Ok(())
}

fn event_headers(batch: &IndexBatch, key: TransactionKey, allows_delete: bool) -> Result<()> {
    for index in 0..batch.len() {
        let header = batch.header(index)?;
        if header.system_from != key.system_time
            || !(header.op == EventOp::Put || allows_delete && header.op == EventOp::Delete)
        {
            return Err(mismatch());
        }
    }
    Ok(())
}

fn records(
    batch: &IndexBatch, key: TransactionKey, memory: &Arc<MemoryBudget>,
    mut inspect: impl FnMut(&StructFields<Value>) -> Result<()>,
) -> Result<()> {
    if batch.len() > 9 { return Err(mismatch()); }
    let bound = batch.arrow().get_array_memory_size().checked_mul(4)
        .and_then(|bytes| bytes.checked_add(8192)).ok_or_else(mismatch)?;
    let _decode = memory.reserve(bound)?;
    for index in 0..batch.len() {
        let event = batch.view().read_event(index)?;
        let StoredOperation::Put(row) = event.operation else { return Err(mismatch()); };
        if event.system_from != key.system_time || event.valid_time != Interval::ALL { return Err(mismatch()); }
        inspect(&row)?;
    }
    Ok(())
}

fn mismatch() -> rtdb_types::Anomaly {
    invalid("Resolved world effects disagree with their ordered placed inventory")
}
