//! Exact ordinary Root rank effects ordered under a held Original receipt.

use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

use arrow_array::{Array, Float32Array, StructArray};
use rtdb_columnar::{ArrowCell, MemoryBudget};
use rtdb_index::TableWrite;
use rtdb_types::{
    EntityId, EventOp, Interval, ListView, Result, SEALED_SEARCH_UNIT_ROWS, TableRef,
    Timestamp, TransactionKey, Value, is_rank_row_address, sealed_search_inventory_row,
};
use sha2::{Digest, Sha256};

use super::replay_invalid;

pub(crate) const MARKERS: &str = "fact_sealed_search_erasures";
const REGISTRY: &str = "fact_sealed_search_units";

pub(crate) fn rank_base(relation: &str) -> Option<&str> {
    let base = relation.strip_suffix("_search")?;
    let digest = base.strip_prefix("fact_")?;
    (digest.len() == 64 && digest.bytes().all(|c| c.is_ascii_digit() || matches!(c, b'a'..=b'f')))
        .then_some(base)
}

pub(crate) fn is_rank_relation(relation: &str) -> bool {
    rank_base(relation).is_some() || matches!(relation, MARKERS | REGISTRY | SEALED_SEARCH_UNIT_ROWS)
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ReplayRank {
    count: usize,
    digest: [u8; 32],
    scope: [u8; 32],
    shared_erase_alias: Option<[u8; 32]>,
}

impl ReplayRank {
    pub(crate) fn count(&self) -> usize { self.count }

    pub(crate) fn shared_erase_alias(&self) -> Option<[u8; 32]> { self.shared_erase_alias }

    pub(crate) fn decode(body: &[u8], replay: &super::HostedReplay) -> Result<Self> {
        let (count, remaining) = body.split_first_chunk::<4>().ok_or_else(replay_invalid)?;
        let (digest, alias) = remaining.split_first_chunk::<32>().ok_or_else(replay_invalid)?;
        let shared_erase_alias = if alias.is_empty() { None }
            else { Some(alias.try_into().map_err(|_| replay_invalid())?) };
        let count = u32::from_be_bytes(*count) as usize;
        if !(1..=4096).contains(&count) {
            return Err(replay_invalid());
        }
        Ok(Self { count, digest: *digest, scope: replay.rank_scope(), shared_erase_alias })
    }
}

pub(crate) fn verify(
    expected: Option<&ReplayRank>, tables: &BTreeMap<TableRef, TableWrite>,
    key: TransactionKey, memory: &Arc<MemoryBudget>,
) -> Result<()> {
    let max = expected.map_or(0, |rank| rank.count);
    let _scratch = memory.reserve(max * 160)?;
    let mut effects = Vec::with_capacity(max);
    let mut erases = BTreeMap::<(&str, &str), BTreeSet<&[u8]>>::new();
    let mut namespace = None;
    let mut rank_markers = BTreeSet::new();
    for (table, write) in tables {
        let Some(name) = table.owned_storage_name() else { continue; };
        let Some(batch) = write.batch().filter(|batch| !batch.is_empty()) else { continue; };
        if let Some(root) = table.owned_world_root_table()
            && root.owned_storage_name().is_some_and(|name| is_rank_relation(name) || name.ends_with("_search"))
        {
            return Err(replay_invalid());
        }
        if !is_rank_relation(name) { continue; }
        let (owner, _) = table.table.split_once('-').ok_or_else(replay_invalid)?;
        if namespace.replace(owner).is_some_and(|previous| previous != owner) {
            return Err(replay_invalid());
        }
        let view = batch.view();
        let union = view.operations()?;
        let documents = view.put_document()?;
        for row in 0..batch.len() {
            if effects.len() == max { return Err(replay_invalid()); }
            let header = batch.header(row)?;
            let handle = view.entity_key(row)?.ok_or_else(replay_invalid)?;
            let (tag, address) = handle.split_first().ok_or_else(replay_invalid)?;
            let derived = rank_base(name).is_some();
            let binary = derived || name == SEALED_SEARCH_UNIT_ROWS;
            let shape = if derived {
                is_rank_row_address(address)
            } else if binary {
                address.split_first_chunk::<32>().is_some_and(|(alias, _)| {
                    sealed_search_inventory_row(alias, address).is_some_and(|(relation, _)| rank_base(relation).is_some())
                })
            } else {
                std::str::from_utf8(address).is_ok()
            };
            if header.system_from != key.system_time || !shape || *tag != if binary { 4 } else { 0 } {
                return Err(replay_invalid());
            }
            let mut hash = Sha256::new();
            hash.update(b"rtdb.archive-rank-effect/v1\0");
            field_bytes(&mut hash, name.as_bytes());
            field_bytes(&mut hash, address);
            let op = match header.op {
                EventOp::Put => 1,
                EventOp::Erase if derived && header.valid_time == Interval::ALL => {
                    if !erases.entry((owner, name)).or_default().insert(handle) {
                        return Err(replay_invalid());
                    }
                    2
                },
                EventOp::Redacted if derived && header.commit_position != 0 => 3,
                EventOp::Delete | EventOp::Erase | EventOp::Redacted => return Err(replay_invalid()),
            };
            if !derived && header.valid_time != Interval::ALL { return Err(replay_invalid()); }
            hash.update([op]);
            hash.update(header.valid_time.lower().0.to_be_bytes());
            hash.update(header.valid_time.upper().0.to_be_bytes());
            hash.update(header.commit_position.to_be_bytes());
            if op == 3 {
                hash.update(view.origin(row)?.ok_or_else(replay_invalid)?.0);
            }
            if op == 1 {
                let id = view.borrow_event(row)?.put_identity()?.ok_or_else(replay_invalid)?;
                match id {
                    rtdb_columnar::EntityIdRef::Binary(value) if binary && value == address => (),
                    rtdb_columnar::EntityIdRef::String(value) if !binary && value.as_bytes() == address => (),
                    _ => return Err(replay_invalid()),
                }
                let text_key = (!binary).then(|| std::str::from_utf8(address)).transpose().map_err(|_| replay_invalid())?;
                if name == MARKERS {
                    let relation = text_key.filter(|name| rank_base(name).is_some()).ok_or_else(replay_invalid)?;
                    if !rank_markers.insert((owner, relation)) { return Err(replay_invalid()); }
                }
                hash_put(&mut hash, name, text_key, documents.ok_or_else(replay_invalid)?, union.value_offset(row))?;
            }
            effects.push(<[u8; 32]>::from(hash.finalize()));
        }
    }
    if effects.len() != max || !erases.keys().copied().eq(rank_markers) { return Err(replay_invalid()); }
    if let Some(expected) = expected {
        effects.sort_unstable();
        let mut hash = Sha256::new();
        hash.update(b"rtdb.archive-rank-inventory/v1\0");
        hash.update(expected.scope);
        if let Some(alias) = expected.shared_erase_alias {
            if erases.is_empty() { return Err(replay_invalid()); }
            hash.update(alias);
        }
        for effect in effects {
            hash.update(effect);
        }
        if <[u8; 32]>::from(hash.finalize()) != expected.digest { return Err(replay_invalid()); }
    }
    verify_purge_floors(tables, key, erases, memory)
}

fn hash_put(
    hash: &mut Sha256, relation: &str, text_key: Option<&str>, documents: &StructArray, row: usize,
) -> Result<()> {
    let registry_key = if relation == REGISTRY {
        let (unit, destination) = text_key.and_then(|key| key.split_once('/')).ok_or_else(replay_invalid)?;
        if unit.len() != 64 || !unit.bytes().all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            || rank_base(destination).is_none()
        {
            return Err(replay_invalid());
        }
        Some((unit, destination))
    } else {
        None
    };
    let mut registry_fields = 0;
    let fields = documents.fields().iter().zip(documents.columns())
        .filter(|(field, _)| field.name() != "_id")
        .map(|(field, array)| (field.name().as_str(), ArrowCell { field, array: array.as_ref(), row }))
        .collect::<BTreeMap<_, _>>();
    for (name, cell) in fields {
        if cell.is_undefined()? { continue; }
        if cell.is_null()? {
            // Persisted layout unions carry explicit null separately from an
            // absent field. Ordinary rank payload declarations are nonnull.
            return Err(replay_invalid());
        }
        if let Some((unit, destination)) = registry_key {
            let observed = cell.binary().ok_or_else(replay_invalid)?;
            let matches = match name {
                "unit" => observed.len() == 32 && observed.iter().zip(unit.as_bytes().chunks_exact(2))
                    .all(|(byte, pair)| {
                        let hex = b"0123456789abcdef";
                        [hex[usize::from(*byte >> 4)], hex[usize::from(*byte & 15)]].as_slice() == pair
                    }),
                "relation" => observed == destination.as_bytes(),
                _ => false,
            };
            if !matches { return Err(replay_invalid()); }
            registry_fields += 1;
        } else if rank_base(relation).is_none() || name == "sealed_world_projection" || name.starts_with('_') {
            return Err(replay_invalid());
        }
        field_bytes(hash, name.as_bytes());
        if let Some(value) = cell.utf8() {
            hash.update([1]);
            field_bytes(hash, value.as_bytes());
        } else if let Some(value) = cell.binary() {
            hash.update([2]);
            field_bytes(hash, value);
        } else if let Some(values) = cell.list() {
            hash.update([3]);
            hash.update(u32::try_from(values.len()).map_err(|_| replay_invalid())?.to_be_bytes());
            for value in values {
                let value = value.physical()?;
                let bits = value.array.as_any().downcast_ref::<Float32Array>()
                    .filter(|array| array.is_valid(value.row)).ok_or_else(replay_invalid)?
                    .value(value.row).to_bits();
                hash.update(bits.to_be_bytes());
            }
        } else {
            return Err(replay_invalid());
        }
    }
    if registry_key.is_some() && registry_fields != 2 { return Err(replay_invalid()); }
    Ok(())
}

fn field_bytes(hash: &mut Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u32).to_be_bytes());
    hash.update(bytes);
}

fn verify_purge_floors(
    tables: &BTreeMap<TableRef, TableWrite>, key: TransactionKey,
    erases: BTreeMap<(&str, &str), BTreeSet<&[u8]>>, memory: &Arc<MemoryBudget>,
) -> Result<()> {
    let mut markers = BTreeSet::new();
    let mut coverage = BTreeSet::new();
    let mut covered_floors = BTreeMap::<_, BTreeSet<_>>::new();
    for (table, write) in tables {
        let Some((name, floor)) = table.owned_purge_relation() else { continue; };
        if rank_base(name).is_none() { continue; }
        let Some(batch) = write.batch() else { continue; };
        let (owner, _) = table.table.split_once('-').ok_or_else(replay_invalid)?;
        let relation = (owner, name);
        let expected = erases.get(&relation).ok_or_else(replay_invalid)?;
        for index in 0..batch.len() {
            let header = batch.header(index)?;
            if header.system_from != key.system_time || header.valid_time != Interval::ALL {
                return Err(replay_invalid());
            }
            let _row = memory.reserve(batch.view().borrow_event(index)?.document_allocation_bound()?)?;
            let rtdb_format::data::StoredOperation::Put(document) = batch.view().read_event(index)?.operation
                else { return Err(replay_invalid()); };
            if !floor {
                if document.get("_id") != Some(&Value::I64(key.tx_id.0)) || !markers.insert(relation) {
                    return Err(replay_invalid());
                }
                continue;
            }
            let id = document.get("_id").ok_or_else(replay_invalid)?.entity_id()?;
            let EntityId::Uuid(bucket) = id else { return Err(replay_invalid()); };
            if bucket == [0; 16] {
                if !matches!(document.get("through_events"), Some(Value::I64(through)) if *through > 0)
                    || !coverage.insert(relation)
                {
                    return Err(replay_invalid());
                }
                continue;
            }
            let Some(ListView::Values(floors)) = document.get("floors").and_then(Value::list_view)
                else { return Err(replay_invalid()); };
            let mut current_floor = false;
            for floor in floors {
                let Value::Struct(floor) = floor else { return Err(replay_invalid()); };
                let (Some(Value::Binary(handle)), Some(Value::I64(at))) = (floor.get("handle"), floor.get("at"))
                    else { return Err(replay_invalid()); };
                let Some(address) = handle.strip_prefix(&[4]).filter(|address| is_rank_row_address(address))
                    else { return Err(replay_invalid()); };
                let mut digest = Sha256::new();
                digest.update(b"b");
                digest.update(address);
                let mut expected: [u8; 16] = digest.finalize()[..16].try_into().map_err(|_| replay_invalid())?;
                expected[0] |= 0x80;
                if bucket != expected || *at <= 0 || *at > key.system_time.0 { return Err(replay_invalid()); }
                if Timestamp(*at) == key.system_time {
                    let handle = expected.get(handle.as_slice()).copied().ok_or_else(replay_invalid)?;
                    covered_floors.entry(relation).or_default().insert(handle);
                    current_floor = true;
                }
            }
            if !current_floor { return Err(replay_invalid()); }
        }
    }
    if !erases.keys().copied().eq(markers) || !erases.keys().copied().eq(coverage)
        || erases != covered_floors
    {
        return Err(replay_invalid());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rtdb_format::data::{StoredEvent, StoredOperation};
    use rtdb_index::{PartitionState, TransactionBuffer};
    use rtdb_types::{OwnedRelation, StructFields, TxId};

    use super::*;

    const SCOPE: [u8; 32] = [41; 32];

    fn commitment(relation: &str, address: &[u8], zero: f32) -> ReplayRank {
        let mut effect = Sha256::new();
        effect.update(b"rtdb.archive-rank-effect/v1\0");
        field_bytes(&mut effect, relation.as_bytes());
        field_bytes(&mut effect, address);
        effect.update([1]);
        effect.update(3i64.to_be_bytes());
        effect.update(i64::MAX.to_be_bytes());
        effect.update(7u32.to_be_bytes());
        field_bytes(&mut effect, b"v_vector");
        effect.update([3]);
        effect.update(2u32.to_be_bytes());
        effect.update(zero.to_bits().to_be_bytes());
        effect.update(0x3f80_0001u32.to_be_bytes());
        let mut hash = Sha256::new();
        hash.update(b"rtdb.archive-rank-inventory/v1\0");
        hash.update(SCOPE);
        hash.update(effect.finalize());
        ReplayRank { count: 1, digest: hash.finalize().into(), scope: SCOPE, shared_erase_alias: None }
    }

    #[test]
    fn resolved_rank_replay_matches_payload_key_and_original_position() -> Result<()> {
        let memory = MemoryBudget::new(1 << 23);
        let partition = PartitionState::empty(Default::default())?;
        let key = TransactionKey { tx_id: TxId(5), system_time: Timestamp(9) };
        let relation = OwnedRelation::new(format!("fact_{}_search", "a".repeat(64)))?;
        let table = relation.table("owner");
        let address = [&[11][..], &[8; 16], &[9, b'x', 0, 255, 0, 1]].concat();
        let original = commitment(relation.name(), &address, -0.0);
        for (position, zero, bytes, valid) in [
            (7, -0.0, address.clone(), true),
            (8, -0.0, address.clone(), false),
            (7, 0.0, address.clone(), false),
            (7, -0.0, [&[11][..], &[9; 16], &[9, b'x', 0, 255, 0, 1]].concat(), false),
            (7, -0.0, [b"BJFR2".as_slice(), address.as_slice()].concat(), false),
        ] {
            let identity = EntityId::Binary(bytes);
            let mut source = TransactionBuffer::new(partition.snapshot(), key, Arc::clone(&memory))?;
            source.retain_archived_owned_positions(vec![position])?;
            source.begin_direct_archive_effect(table.clone(), position)?;
            source.append_to(&table, StoredEvent {
                position: None, origin: None, commit_position: 0,
                entity_key: Some(identity.handle_bytes().into()), iid: identity.iid(),
                system_from: key.system_time, valid_time: Interval::new(Timestamp(3), Timestamp::MAX)?,
                operation: StoredOperation::Put(StructFields::from([
                    ("_id".into(), identity.as_value()),
                    ("v_vector".into(), Value::list(vec![Value::F32(zero), Value::F32(f32::from_bits(0x3f80_0001))])),
                ])),
                recency: None,
            })?;
            source.finish_direct_archive_effect()?;
            let staged = source.stage()?;
            assert_eq!(verify(Some(&original), staged.tables(), key, &memory).is_ok(), valid);
            assert!(verify(None, staged.tables(), key, &memory).is_err());
        }
        Ok(())
    }

    #[test]
    fn resolved_rank_erase_requires_the_full_binary_purge_floor() -> Result<()> {
        let memory = MemoryBudget::new(1 << 23);
        let partition = PartitionState::empty(Default::default())?;
        let key = TransactionKey { tx_id: TxId(5), system_time: Timestamp(9) };
        let relation = OwnedRelation::new(format!("fact_{}_search", "a".repeat(64)))?;
        let identity = EntityId::Binary([&[11][..], &[8; 16], &[9, b'x', 0, 255, 0, 1]].concat());
        let mut bucket = identity.iid().0;
        bucket[0] |= 0x80;
        for (include_floor, coverage, correct_key, valid) in [
            (true, true, true, true), (true, true, false, false),
            (false, true, true, false), (true, false, true, false),
        ] {
            let mut source = TransactionBuffer::new(partition.snapshot(), key, Arc::clone(&memory))?;
            source.put(relation.purge_table("owner"), StructFields::from([
                ("_id".into(), Value::I64(key.tx_id.0)),
            ]), Interval::ALL)?;
            let floors = relation.purge_floor_table("owner");
            if coverage {
                source.put(floors.clone(), rtdb_query::purge_coverage_document(1), Interval::ALL)?;
            }
            let handle = identity.handle_bytes();
            if include_floor {
                let mut retained = handle.clone();
                if !correct_key { retained[3] ^= 1; }
                source.put(floors, StructFields::from([
                    ("_id".into(), Value::Uuid(bucket)),
                    ("floors".into(), Value::list(vec![Value::Struct(StructFields::from([
                        ("handle".into(), Value::Binary(retained)), ("at".into(), Value::I64(key.system_time.0)),
                    ]))])),
                ]), Interval::ALL)?;
            }
            let staged = source.stage()?;
            let physical = relation.table("owner");
            let (owner, name) = physical.table.split_once('-').unwrap();
            let erases = BTreeMap::from([((owner, name), BTreeSet::from([handle.as_slice()]))]);
            assert_eq!(verify_purge_floors(staged.tables(), key, erases, &memory).is_ok(), valid);
        }
        Ok(())
    }
}
