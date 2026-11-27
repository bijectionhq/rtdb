//! Logical byte balances of the ordered store at a retained cut.

use std::sync::Arc;

use parking_lot::Mutex;
use rtdb_types::{Anomaly, EntityId, Result, TableRef, Timestamp, rtdb_v1::{At, Basis, Layer, MAIN_BRANCH}};

use super::{BranchEvent, BranchView, store_derive, store_table, value_of, STORE_RELATION, STORE_SCHEMA};
use crate::branches::{BranchEffect, BranchIndex, Map};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum SizeFamily {
    Row = 16,
    Index = 17,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SizeBucket {
    pub family: SizeFamily,
    pub tablet: [u8; 16],
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SizeBalance {
    pub key_bytes: u64,
    pub value_bytes: u64,
    pub presences: u64,
}

pub struct StorageSizes {
    pub at: Basis,
    buckets: Map<SizeBucket, SizeBalance>,
}

impl StorageSizes {
    pub fn buckets(&self) -> impl Iterator<Item = (&SizeBucket, &SizeBalance)> {
        self.buckets.iter()
    }
}

#[derive(Clone, Debug, Default)]
pub(in crate::branches) struct Rollup {
    // A cut can share its cache with other cuts while both see the same byte balances.
    cached: Arc<Mutex<Option<Projection>>>,
    reconciled: Arc<Mutex<Option<u64>>>,
}

#[derive(Clone, Debug)]
struct Projection {
    epoch: Option<u64>,
    buckets: Map<SizeBucket, SizeBalance>,
    valid_from: Timestamp,
    valid_before: Timestamp,
}

impl Projection {
    fn valid_at(&self, now: Timestamp) -> bool {
        self.valid_from <= now && (now < self.valid_before || now == self.valid_from)
    }

    fn replace(&mut self, key: SizedKey, before: Option<u64>, after: Option<u64>) -> Result<()> {
        let current = self.buckets.get(&key.bucket).copied().unwrap_or_default();
        let replace = |total: u64, before: u64, after: u64| {
            total.checked_sub(before).and_then(|total| total.checked_add(after))
                .ok_or_else(|| Anomaly::fault("rtdb.store/size-balance", "Storage size balance is out of range"))
        };
        let next = SizeBalance {
            key_bytes: replace(current.key_bytes, u64::from(before.is_some()) * key.bytes, u64::from(after.is_some()) * key.bytes)?,
            value_bytes: replace(current.value_bytes, before.unwrap_or_default(), after.unwrap_or_default())?,
            presences: replace(current.presences, u64::from(key.presence && before.is_some()), u64::from(key.presence && after.is_some()))?,
        };
        if next == SizeBalance::default() {
            self.buckets.remove(&key.bucket);
        } else {
            self.buckets.insert(key.bucket, next);
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(in crate::branches) struct SizedKey {
    bucket: SizeBucket,
    bytes: u64,
    presence: bool,
}

fn sized_key(key: &[u8]) -> Option<SizedKey> {
    let prefix = store_derive::partition_prefix_len(key)?;
    if &key[2..prefix] == b"host" { return None; }
    let family = match *key.get(prefix)? {
        byte if byte == SizeFamily::Row as u8 => SizeFamily::Row,
        byte if byte == SizeFamily::Index as u8 => SizeFamily::Index,
        _ => return None,
    };
    let (_, world_end) = store_derive::place_bounds(key)?;
    decode_id(&key[prefix + 1..world_end - 2])?;
    let tablet_end = store_derive::segment_end(key, world_end)?;
    let tablet = decode_id(&key[world_end..tablet_end - 2])?;
    let address_end = store_derive::segment_end(key, tablet_end)?;
    Some(SizedKey {
        bucket: SizeBucket { family, tablet },
        bytes: u64::try_from(key.len()).ok()?,
        presence: family == SizeFamily::Row && key[address_end..] == [0],
    })
}

fn decode_id(mut encoded: &[u8]) -> Option<[u8; 16]> {
    let mut id = [0; 16];
    for byte in &mut id {
        let (&next, rest) = encoded.split_first()?;
        *byte = next;
        encoded = if next == 0 { rest.strip_prefix(&[0xff])? } else { rest };
    }
    encoded.is_empty().then_some(id)
}

fn next_boundary(event: &BranchEvent, now: Timestamp) -> Timestamp {
    [Some(event.valid.lower()), Some(event.valid.upper()), event.image.as_ref().and_then(|image| image.expires_at)]
        .into_iter().flatten().filter(|&time| time > now).min().unwrap_or(Timestamp::MAX)
}

fn value_bytes(view: &BranchView<'_>, table: &TableRef, id: &EntityId) -> Result<Option<u64>> {
    view.row(table, id, view.cut.system_time)
        .map(|event| value_of(event).map(|value| value.len() as u64)).transpose()
}

fn census(view: &BranchView<'_>, epoch: Option<u64>, check: impl Fn() -> Result<()>) -> Result<Projection> {
    let now = view.cut.system_time;
    let mut projection = Projection { epoch, buckets: Map::new(), valid_from: now, valid_before: Timestamp::MAX };
    let table = store_table();
    let Some(rows) = view.data.tables.get(&table) else { return Ok(projection); };
    let Some(definition) = view.table(&table) else { return Ok(projection); };
    for (id, history) in &rows.events {
        check()?;
        let EntityId::Binary(bytes) = id else { continue; };
        let Some(key) = sized_key(bytes) else { continue; };
        if let Some(after) = value_bytes(view, &table, id)? {
            projection.replace(key, None, Some(after))?;
        }
        for (generation, event) in history.iter() {
            check()?;
            if *generation == definition.generation {
                projection.valid_before = projection.valid_before.min(next_boundary(event, now));
            }
        }
    }
    Ok(projection)
}

impl Rollup {
    fn replace_cached(&mut self, next: Option<Projection>) {
        if let Some(cache) = Arc::get_mut(&mut self.cached) {
            *cache.get_mut() = next;
        } else {
            self.cached = Arc::new(Mutex::new(next));
        }
    }

    fn snapshot(&self, view: &BranchView<'_>, epoch: Option<u64>, check: impl Fn() -> Result<()>) -> Result<StorageSizes> {
        let mut reconciled = self.reconciled.lock();
        check()?;
        let target = (*reconciled).max(epoch);
        let cached = self.cached.lock().clone();
        let projection = if let Some(projection) = cached.as_ref()
            .filter(|projection| projection.epoch == target && projection.valid_at(view.cut.system_time)) {
            projection.clone()
        } else {
            let projection = census(view, target, &check)?;
            check()?;
            if cached.is_none_or(|cached| cached.epoch != target || cached.valid_from <= projection.valid_from) {
                *self.cached.lock() = Some(projection.clone());
            }
            *reconciled = target;
            projection
        };
        Ok(StorageSizes { at: view.cut.basis, buckets: projection.buckets })
    }
}

pub(in crate::branches) enum Change {
    Unchanged,
    Invalidate,
    Entry { id: EntityId, key: SizedKey, before: Option<u64>, boundary: Timestamp },
}

impl BranchIndex {
    pub fn storage_sizes(&self, now: Timestamp, epoch: Option<u64>, check: impl Fn() -> Result<()>) -> Result<StorageSizes> {
        let view = self.view(MAIN_BRANCH, At::Latest, Layer::Merged, now)?;
        self.store_sizes.snapshot(&view, epoch, check)
    }

    pub(in crate::branches) fn prepare_store_size_change(&self, effect: &BranchEffect) -> Change {
        let is_store = |table: &TableRef| table.schema == STORE_SCHEMA && table.table == STORE_RELATION;
        let (table, id, boundary) = match effect {
            BranchEffect::Declare { table, .. } if is_store(table) => return Change::Invalidate,
            BranchEffect::Write { branch, table, event } if branch == MAIN_BRANCH && is_store(table) =>
                (table, &event.id, next_boundary(event, self.head.system_time)),
            BranchEffect::Erase { branch, table, id } if branch.as_deref().is_none_or(|branch| branch == MAIN_BRANCH) && is_store(table) =>
                (table, id, Timestamp::MAX),
            _ => return Change::Unchanged,
        };
        let EntityId::Binary(bytes) = id else { return Change::Unchanged; };
        let Some(key) = sized_key(bytes) else { return Change::Unchanged; };
        let now = self.head.system_time;
        if !self.store_sizes.cached.lock().as_ref().is_some_and(|projection| projection.valid_at(now)) {
            return Change::Invalidate;
        }
        match self.view(MAIN_BRANCH, At::Latest, Layer::Merged, self.head.system_time).and_then(|view| value_bytes(&view, table, id)) {
            Ok(before) => Change::Entry { id: id.clone(), key, before, boundary },
            Err(_) => Change::Invalidate,
        }
    }

    pub(in crate::branches) fn finish_store_size_change(&mut self, change: Change) {
        let (id, key, before, boundary) = match change {
            Change::Unchanged => return,
            Change::Invalidate => return self.store_sizes.replace_cached(None),
            Change::Entry { id, key, before, boundary } => (id, key, before, boundary),
        };
        let after = self.view(MAIN_BRANCH, At::Latest, Layer::Merged, self.head.system_time)
            .and_then(|view| value_bytes(&view, &store_table(), &id));
        let now = self.head.system_time;
        let cached = self.store_sizes.cached.lock().as_ref().filter(|projection| projection.valid_at(now)).cloned();
        let next = cached.zip(after.ok()).and_then(|(mut projection, after)| {
            projection.replace(key, before, after).ok()?;
            projection.valid_from = now;
            projection.valid_before = projection.valid_before.min(boundary);
            Some(projection)
        });
        self.store_sizes.replace_cached(next);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use rtdb_types::{Interval, StructFields, Value};

    use super::*;
    use crate::branches::{BranchCut, RowImage, WrittenVersion, store::VALUE_COLUMN};

    const INCARNATION: [u8; 16] = [7; 16];
    const TABLET: [u8; 16] = [0; 16];

    fn key(partition: &[u8], family: SizeFamily, world: u8, tail: &[u8]) -> Vec<u8> {
        let mut key = (partition.len() as u16).to_be_bytes().to_vec();
        key.extend_from_slice(partition);
        key.push(family as u8);
        key.extend_from_slice(&[world; 16]);
        key.extend_from_slice(&[0, 1]);
        for _ in TABLET {
            key.extend_from_slice(&[0, 0xff]);
        }
        key.extend_from_slice(&[0, 1, b'a', 0, 1]);
        key.extend_from_slice(tail);
        key
    }

    fn advance(index: &mut BranchIndex, position: u64) -> Result<()> {
        index.advance(BranchCut {
            basis: Basis::new(index.head().basis.incarnation(), position),
            system_time: Timestamp(position as i64 * 10),
        })
    }

    fn write(index: &mut BranchIndex, key: &[u8], bytes: Option<usize>, expiry: Option<Timestamp>) -> Result<()> {
        let cut = index.head();
        index.apply(BranchEffect::Write {
            branch: MAIN_BRANCH.into(),
            table: store_table(),
            event: BranchEvent {
                id: EntityId::Binary(key.to_vec()),
                valid: Interval::new(cut.system_time, Timestamp::MAX)?,
                cut,
                written: WrittenVersion::Branch,
                image: bytes.map(|bytes| Arc::new(RowImage {
                    cells: StructFields::from_iter([(VALUE_COLUMN.to_owned(), Value::Binary(vec![0; bytes]))]),
                    expires_at: expiry,
                    custody_group: None,
                    custody_payload: None,
                })),
            },
        })
    }

    fn balance(sizes: &StorageSizes, family: SizeFamily) -> Option<SizeBalance> {
        sizes.buckets.get(&SizeBucket { family, tablet: TABLET }).copied()
    }

    #[test]
    fn the_rollup_counts_only_well_formed_row_and_index_keys_outside_the_host_partition() -> Result<()> {
        let mut index = BranchIndex::new(INCARNATION);
        advance(&mut index, 1)?;
        let presence = key(b"p", SizeFamily::Row, b'w', &[0]);
        let history = key(b"q", SizeFamily::Row, b'z', &[0, 1]);
        let posting = key(b"p", SizeFamily::Index, b'z', &[0]);
        let host = key(b"host", SizeFamily::Row, b'w', &[0]);
        let mut malformed = presence.clone();
        malformed.truncate(malformed.len() - 2);
        for key in [&presence, &history, &posting, &host, &malformed] {
            write(&mut index, key, Some(2), None)?;
        }
        let sizes = index.storage_sizes(Timestamp(10), None, || Ok(()))?;
        assert_eq!(sizes.at, index.head().basis);
        assert_eq!(sizes.buckets.len(), 2);
        assert_eq!(balance(&sizes, SizeFamily::Row), Some(SizeBalance {
            key_bytes: (presence.len() + history.len()) as u64, value_bytes: 4, presences: 1,
        }));
        assert_eq!(balance(&sizes, SizeFamily::Index), Some(SizeBalance {
            key_bytes: posting.len() as u64, value_bytes: 2, presences: 0,
        }));
        Ok(())
    }

    #[test]
    fn replacements_deletes_erasure_and_restore_follow_the_selected_cut() -> Result<()> {
        let mut index = BranchIndex::new(INCARNATION);
        let row = key(b"p", SizeFamily::Row, b'w', &[0]);
        advance(&mut index, 1)?;
        write(&mut index, &row, Some(8), None)?;
        let first = index.storage_sizes(Timestamp(10), Some(4), || Ok(()))?;
        let old = index.clone();

        advance(&mut index, 2)?;
        write(&mut index, &row, None, None)?;
        write(&mut index, &row, Some(0), None)?;
        write(&mut index, &row, Some(3), None)?;
        let checks = Cell::new(0);
        let latest = index.storage_sizes(Timestamp(20), Some(4), || { checks.set(checks.get() + 1); Ok(()) })?;
        assert_eq!(checks.get(), 1);
        assert_eq!(latest.at.position(), 2);
        let expected = Some(SizeBalance { key_bytes: row.len() as u64, value_bytes: 3, presences: 1 });
        assert_eq!(balance(&latest, SizeFamily::Row), expected);
        assert_eq!(balance(&old.storage_sizes(Timestamp(10), None, || Ok(()))?, SizeFamily::Row), balance(&first, SizeFamily::Row));

        let mut restored = index.checkpoint_at(latest.at)?;
        restored.restore_in([9; 16])?;
        let rebuilt = restored.storage_sizes(Timestamp(20), Some(0), || Ok(()))?;
        assert_eq!(rebuilt.at, Basis::new([9; 16], 2));
        assert_eq!(balance(&rebuilt, SizeFamily::Row), expected);

        advance(&mut index, 3)?;
        write(&mut index, &row, None, None)?;
        assert_eq!(index.storage_sizes(Timestamp(30), None, || Ok(()))?.buckets.len(), 0);
        advance(&mut restored, 3)?;
        restored.apply(BranchEffect::Erase { branch: None, table: store_table(), id: EntityId::Binary(row) })?;
        assert_eq!(restored.storage_sizes(Timestamp(30), None, || Ok(()))?.buckets.len(), 0);
        Ok(())
    }

    #[test]
    fn reconciliation_rebuilds_once_per_epoch_and_observes_time_boundaries() -> Result<()> {
        let mut index = BranchIndex::new(INCARNATION);
        let row = key(b"p", SizeFamily::Row, b'w', &[0]);
        advance(&mut index, 1)?;
        write(&mut index, &row, Some(0), Some(Timestamp(20)))?;
        let checks = Cell::new(0);
        let sample = |index: &BranchIndex, epoch| index.storage_sizes(index.head().system_time, Some(epoch), || {
            checks.set(checks.get() + 1);
            Ok(())
        });
        for epoch in [0, 5] {
            checks.set(0);
            assert_eq!(sample(&index, epoch)?.buckets.len(), 1);
            assert!(checks.get() > 1);
        }
        for epoch in [5, 4] {
            checks.set(0);
            assert_eq!(sample(&index, epoch)?.buckets.len(), 1);
            assert_eq!(checks.get(), 1);
        }
        let old = index.clone();
        advance(&mut index, 2)?;
        assert_eq!(sample(&index, 5)?.buckets.len(), 0);
        assert_eq!(sample(&old, 5)?.buckets.len(), 1);
        checks.set(0);
        assert_eq!(sample(&index, 5)?.buckets.len(), 0);
        assert_eq!(checks.get(), 1);
        Ok(())
    }
}
