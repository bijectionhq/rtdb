//! The ordered-bytes store contract over the main branch.
//!
//! One relation holds the whole store: each entry's id is its key bytes and its
//! `v` cell holds the value bytes. Versions are the branch's commit bases. The
//! store knows no labels, worlds or indexes; its callers encode them as keys.
//!
//! An append's writes may carry stamp placeholders ([`StoreWrite`]): 16 zero
//! bytes the resolver fills with the commit's [`StoreStamp`] at the ordering
//! point ([`stamp_writes`]). The placeholder rules are Bijection's
//! (`fact_store::persistence::stamp`), checked the same way here.

use std::{collections::BTreeMap, ops::Bound};

use rtdb_types::{Anomaly, EntityId, Result, TableRef, Value, rtdb_v1::Basis};

use super::{BranchCut, BranchEvent, BranchView};

pub use store_derive;
pub use store_filter::ValueFilter;

/// SHA-256 of Bijection's `crates/store_read/src/derive.rs`, vendored verbatim
/// as `rust/vendor/bijection-store-derive/src/lib.rs`.
pub const DERIVE_DIGEST: &str = "d8edd58771bd1fede395f4f40d4fad5bc7cdd3440b3435ff293ba26e4de4ebac";

pub const STORE_SCHEMA: &str = "store";
pub const STORE_RELATION: &str = "entries";
pub const VALUE_COLUMN: &str = "v";
pub const MAX_PAGE_ENTRIES: usize = 4096;

pub fn store_table() -> TableRef { TableRef::new(STORE_SCHEMA, STORE_RELATION) }

/// A half-open key range `[start, end)`. `None` leaves the end unbounded.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KeyRange {
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
}

impl KeyRange {
    pub fn check(&self) -> Result<()> {
        if self.end.as_ref().is_some_and(|end| *end <= self.start) {
            return Err(Anomaly::incorrect("rtdb.store/empty-range", "A key range must be nonempty"));
        }
        Ok(())
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && self.end.as_ref().is_none_or(|end| key < end.as_slice())
    }

    /// The ids of this range past `after` in the read's direction: above it
    /// ascending, below it descending.
    fn ids(&self, after: Option<&[u8]>, reverse: bool) -> (Bound<EntityId>, Bound<EntityId>) {
        let after = after.filter(|after| self.contains(after)).map(|after| Bound::Excluded(EntityId::Binary(after.to_vec())));
        let lower = match after.clone().filter(|_| !reverse) {
            Some(after) => after,
            None => Bound::Included(EntityId::Binary(self.start.clone())),
        };
        let upper = match (after.filter(|_| reverse), &self.end) {
            (Some(after), _) => after,
            (None, Some(end)) => Bound::Excluded(EntityId::Binary(end.clone())),
            // Binary sorts last among id kinds, so every key lies below this bound.
            (None, None) => Bound::Unbounded,
        };
        (lower, upper)
    }
}

/// The encoded width of a stamp, and of a placeholder.
pub const STAMP_BYTES: usize = 16;

/// A store commit's stamp: the writer's clock sampled at the ordering point,
/// in microseconds, and the commit's position.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct StoreStamp {
    pub micros: i64,
    pub version: u64,
}

impl StoreStamp {
    /// Micros big-endian with the sign bit flipped, then the version
    /// big-endian: byte order is stamp order.
    pub fn encode(self) -> [u8; STAMP_BYTES] {
        let mut out = [0; STAMP_BYTES];
        out[..8].copy_from_slice(&((self.micros as u64) ^ (1 << 63)).to_be_bytes());
        out[8..].copy_from_slice(&self.version.to_be_bytes());
        out
    }
}

/// One write of a store append: `value: None` deletes the key. A placeholder
/// is the offset of 16 zero bytes the resolver overwrites with the stamp.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoreWrite {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub key_stamp: Option<u32>,
    pub value_stamp: Option<u32>,
}

/// One atomic bounded-counter update of a store append over a 64-bit signed
/// big-endian integer stored at `key` (absent key = `0`). At the append's
/// version, the resolver adds `delta` and refuses the whole append if the sum
/// leaves `[min, max]` or overflows `i64`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoreBound {
    pub key: Vec<u8>,
    pub delta: i64,
    pub min: i64,
    pub max: i64,
}

/// Validate `bounds` before anything is ordered: every bound must have a
/// non-empty range (`min <= max`).
pub fn check_bounds(bounds: &[StoreBound]) -> Result<()> {
    for bound in bounds {
        if bound.min > bound.max {
            return Err(Anomaly::incorrect("rtdb.store/invalid-bound", "A store bound's min must not exceed its max"));
        }
    }
    Ok(())
}

/// Decode a stored 8-byte signed big-endian counter (`None` = `0`).
pub fn decode_bound_value(value: Option<&[u8]>) -> Result<i64> {
    match value {
        None => Ok(0),
        Some(bytes) => {
            let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
                Anomaly::fault("rtdb.store/invalid-bound-value", "A store bound entry must hold an 8-byte big-endian i64")
            })?;
            Ok(i64::from_be_bytes(bytes))
        }
    }
}

fn invalid_placeholder(message: &'static str) -> Anomaly {
    Anomaly::incorrect("rtdb.store/invalid-stamp-placeholder", message)
}

/// The length of the partition prefix `key` starts with: a big-endian `u16`
/// length, then that many bytes.
fn partition_prefix_len(key: &[u8]) -> Option<usize> {
    let len = usize::from(u16::from_be_bytes(key.get(..2)?.try_into().ok()?));
    (key.len() >= 2 + len).then_some(2 + len)
}

fn placeholder(bytes: &[u8], offset: u32, floor: usize) -> Result<std::ops::Range<usize>> {
    let start = offset as usize;
    let end = start.checked_add(STAMP_BYTES).ok_or_else(|| invalid_placeholder("A stamp offset overflows"))?;
    if start < floor { return Err(invalid_placeholder("A key stamp placeholder lies inside the partition prefix")); }
    if end > bytes.len() { return Err(invalid_placeholder("A stamp placeholder lies outside its bytes")); }
    if bytes[start..end].iter().any(|byte| *byte != 0) {
        return Err(invalid_placeholder("A stamp placeholder is not 16 zero bytes"));
    }
    Ok(start..end)
}

fn key_region(write: &StoreWrite) -> Option<std::ops::Range<usize>> {
    write.key_stamp.map(|offset| offset as usize..offset as usize + STAMP_BYTES)
}

/// Whether `a` and `b`, one of them stamped, could land on one key: equal
/// lengths and equal bytes outside both placeholder regions.
fn could_collide(a: &StoreWrite, b: &StoreWrite) -> bool {
    let (ra, rb) = (key_region(a), key_region(b));
    let masked = |index: &usize| ra.as_ref().is_some_and(|r| r.contains(index)) || rb.as_ref().is_some_and(|r| r.contains(index));
    a.key.len() == b.key.len() && (0..a.key.len()).filter(|index| !masked(index)).all(|index| a.key[index] == b.key[index])
}

/// Every placeholder rule, decided from the request alone and before
/// anything is judged: in bounds, all zero, a key placeholder at or past the
/// partition prefix, a value placeholder on a value, and no stamped key that
/// could land on another write's key whatever the stamp.
pub fn check_placeholders(writes: &[StoreWrite]) -> Result<()> {
    let mut offsets = Vec::new();
    for write in writes {
        if let Some(offset) = write.key_stamp {
            let prefix = partition_prefix_len(&write.key)
                .ok_or_else(|| invalid_placeholder("A stamped key starts with a whole partition prefix"))?;
            placeholder(&write.key, offset, prefix)?;
            offsets.push(offset as usize);
        }
        if let Some(offset) = write.value_stamp {
            let value = write.value.as_deref().ok_or_else(|| invalid_placeholder("A value stamp placeholder needs a value"))?;
            placeholder(value, offset, 0)?;
        }
    }
    if offsets.is_empty() { return Ok(()); }
    offsets.sort_unstable();
    offsets.dedup();
    let mut buckets = BTreeMap::<Vec<u8>, Vec<&StoreWrite>>::new();
    for write in writes {
        let mut masked = write.key.clone();
        for offset in &offsets {
            if let Some(region) = masked.get_mut(*offset..offset + STAMP_BYTES) { region.fill(0); }
        }
        buckets.entry(masked).or_default().push(write);
    }
    for bucket in buckets.values() {
        for (index, a) in bucket.iter().enumerate() {
            for b in &bucket[index + 1..] {
                if (a.key_stamp.is_some() || b.key_stamp.is_some()) && could_collide(a, b) {
                    return Err(invalid_placeholder("A stamped key could collide with another write of the append"));
                }
            }
        }
    }
    Ok(())
}

/// `writes` with every placeholder filled with `stamp`, as `(key, value)`.
pub fn stamp_writes(writes: &[StoreWrite], stamp: StoreStamp) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
    check_placeholders(writes)?;
    let encoded = stamp.encode();
    Ok(writes.iter().map(|write| {
        let mut key = write.key.clone();
        if let Some(region) = key_region(write) { key[region].copy_from_slice(&encoded); }
        let value = write.value.clone().map(|mut value| {
            if let Some(offset) = write.value_stamp {
                let start = offset as usize;
                value[start..start + STAMP_BYTES].copy_from_slice(&encoded);
            }
            value
        });
        (key, value)
    }).collect())
}

fn invalid_derive(message: &'static str) -> Anomaly {
    Anomaly::incorrect("rtdb.store/invalid-derive", message)
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < u8::MAX {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

/// Decode and validate every derived write of a store append before ordering:
/// partition/world confinement, placeholder bounds and non-overlap with plain
/// writes.
pub fn check_derives(derives: &[Vec<u8>], writes: &[StoreWrite]) -> Result<Vec<store_derive::Derive>> {
    let mut decoded = Vec::with_capacity(derives.len());
    for raw in derives {
        let derive = store_derive::Derive::decode(raw).map_err(|error| invalid_derive(error.message))?;
        store_derive::check(&derive).map_err(|error| invalid_derive(error.message))?;
        decoded.push(derive);
    }
    store_derive::check_plain_overlap(&decoded, writes.iter().map(|write| write.key.as_slice()))
        .map_err(|error| invalid_derive(error.message))?;
    Ok(decoded)
}

/// Resolve every derived write against `view` at `stamp`, returning the
/// concrete `(key, value)` mutations to apply before plain writes (T3).
pub fn resolve_derives(
    view: &BranchView<'_>,
    derives: &[store_derive::Derive],
    stamp: StoreStamp,
) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
    if derives.is_empty() {
        return Ok(Vec::new());
    }
    let stamp_bytes = stamp.encode();
    let mut out: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
    let mut ordinal = store_derive::DERIVE_ORDINAL_BASE;
    let mut pending_manifests: BTreeMap<Vec<u8>, Vec<(Vec<u8>, store_derive::Manifest)>> = BTreeMap::new();
    let mut purged_prefixes = std::collections::BTreeSet::<Vec<u8>>::new();

    for derive in derives {
        match &derive.mode {
            store_derive::DeriveMode::Replace { interval, entries } => {
                let valid = store_derive::resolve_interval(*interval, stamp.micros);
                let mut previous = store_derive::Manifest::empty();
                for level in &derive.levels {
                    if level.through.is_none() {
                        if let Some(list) = pending_manifests.get(&level.prefix)
                            && let Some((_, last)) = list.last()
                        {
                            previous = last.clone();
                            break;
                        }
                        if purged_prefixes.contains(&level.prefix) {
                            break;
                        }
                    }
                    let range = match &level.through {
                        None => KeyRange { start: level.prefix.clone(), end: prefix_successor(&level.prefix) },
                        Some(through) => KeyRange {
                            start: level.prefix.clone(),
                            end: Some([level.prefix.as_slice(), through.as_slice(), &[0xff; 8], &[0x00]].concat()),
                        },
                    };
                    let page = view.store_page(&StoreRead {
                        range,
                        after: None,
                        limit: 1,
                        projection: Projection::Values,
                        filter: None,
                        reverse: true,
                    })?;
                    if let Some(raw) = page.entries.into_iter().next().and_then(|entry| entry.value) {
                        previous = store_derive::Manifest::decode(&raw)
                            .map_err(|error| Anomaly::fault("rtdb.store/corrupt-derive-manifest", error.message))?;
                        break;
                    }
                }
                // Total: the outcome never depends on the manifest (P8).
                let applied = store_derive::apply(&derive.manifest_prefix, &previous, valid, entries, stamp_bytes, ordinal);
                ordinal = applied.next_ordinal;
                for write in applied.writes {
                    out.push((write.key, write.value));
                }
                let manifest_key = store_derive::stamped_key(&derive.manifest_prefix, &stamp_bytes, ordinal);
                ordinal = ordinal.wrapping_add(1);
                out.push((manifest_key.clone(), Some(applied.manifest.encode())));
                pending_manifests.entry(derive.manifest_prefix.clone()).or_default().push((manifest_key, applied.manifest));
            },
            store_derive::DeriveMode::Purge => {
                let mut stored = Vec::new();
                if !purged_prefixes.contains(&derive.manifest_prefix) {
                    let range = KeyRange { start: derive.manifest_prefix.clone(), end: prefix_successor(&derive.manifest_prefix) };
                    let mut after = None;
                    loop {
                        let page = view.store_page(&StoreRead {
                            range: range.clone(),
                            after: after.take(),
                            limit: MAX_PAGE_ENTRIES,
                            projection: Projection::Values,
                            filter: None,
                            reverse: false,
                        })?;
                        for entry in page.entries {
                            if let Some(raw) = entry.value {
                                let manifest = store_derive::Manifest::decode(&raw)
                                    .map_err(|error| Anomaly::fault("rtdb.store/corrupt-derive-manifest", error.message))?;
                                stored.push((entry.key, manifest));
                            }
                        }
                        match page.resume {
                            Some(resume) => after = Some(resume),
                            None => break,
                        }
                    }
                }
                if let Some(in_append) = pending_manifests.remove(&derive.manifest_prefix) {
                    stored.extend(in_append);
                }
                let (emitted, next_ordinal) = store_derive::purge(&derive.manifest_prefix, &stored, stamp_bytes, ordinal);
                ordinal = next_ordinal;
                for write in emitted {
                    out.push((write.key, write.value));
                }
                purged_prefixes.insert(derive.manifest_prefix.clone());
            },
        }
    }
    Ok(out)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreRead {
    pub range: KeyRange,
    /// Resume strictly past this key, in the read's direction.
    pub after: Option<Vec<u8>>,
    pub limit: usize,
    pub projection: Projection,
    /// Return only entries whose whole value passes this filter, evaluated
    /// before the projection; `limit` counts returned entries.
    /// Bijection defines the filter; this crate only runs it.
    pub filter: Option<store_filter::ValueFilter>,
    /// Descend by key; `limit` then counts from the range's end. A reverse
    /// page equals the ascending page of the same range reversed.
    pub reverse: bool,
}

/// What a read returns of each value. Each equals projecting the whole value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Projection {
    #[default]
    Values,
    Keys,
    /// The bytes `[start, start + len)` of each value, clipped to its length.
    Slice { start: u32, len: u32 },
}

impl Projection {
    pub fn apply(self, value: &[u8]) -> Option<Vec<u8>> {
        match self {
            Self::Values => Some(value.to_vec()),
            Self::Keys => None,
            Self::Slice { start, len } => {
                let start = (start as usize).min(value.len());
                let end = start.saturating_add(len as usize).min(value.len());
                Some(value[start..end].to_vec())
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub written: Basis,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorePage {
    pub cut: BranchCut,
    pub entries: Vec<StoreEntry>,
    /// Present when the limit stopped the page: resume past this key (the
    /// last returned, so the smallest on a reverse read).
    pub resume: Option<Vec<u8>>,
}

pub fn invalid_store_value() -> Anomaly {
    Anomaly::fault("rtdb.store/invalid-entry", "A store entry must hold a binary id and a binary value")
}

fn value_of(event: &BranchEvent) -> Result<&[u8]> {
    match event.image.as_ref().and_then(|image| image.cells.get(VALUE_COLUMN)) {
        Some(Value::Binary(value)) => Ok(value),
        _ => Err(invalid_store_value()),
    }
}

impl BranchView<'_> {
    /// One page of live entries in `read.range` at this view's cut, ascending
    /// or (`read.reverse`) descending by key.
    pub fn store_page(&self, read: &StoreRead) -> Result<StorePage> {
        read.range.check()?;
        let table = store_table();
        let limit = read.limit.clamp(1, MAX_PAGE_ENTRIES);
        let at = self.cut.system_time;
        let mut entries = Vec::new();
        let mut resume = None;
        let Some(rows) = self.data.tables.get(&table) else {
            return Ok(StorePage { cut: self.cut, entries, resume });
        };
        let ids = rows.events.range(read.range.ids(read.after.as_deref(), read.reverse));
        let ids: Box<dyn Iterator<Item = _> + '_> = if read.reverse { Box::new(ids.rev()) } else { Box::new(ids) };
        for (id, _) in ids {
            let EntityId::Binary(key) = id else { return Err(invalid_store_value()); };
            let Some(event) = self.row(&table, id, at) else { continue; };
            let whole = match (&read.filter, read.projection) {
                (None, Projection::Keys) => None,
                _ => Some(value_of(event)?),
            };
            if let (Some(filter), Some(whole)) = (&read.filter, &whole)
                && !filter.matches(whole)
            {
                continue;
            }
            if entries.len() == limit {
                resume = entries.last().map(|entry: &StoreEntry| entry.key.clone());
                break;
            }
            let written = event.written_basis().ok_or_else(invalid_store_value)?;
            let value = match (read.projection, whole) {
                (Projection::Keys, _) | (_, None) => None,
                (projection, Some(whole)) => projection.apply(whole),
            };
            entries.push(StoreEntry { key: key.clone(), value, written });
        }
        Ok(StorePage { cut: self.cut, entries, resume })
    }

    /// The live value of `key` in the ordered-bytes store at this view's cut,
    /// or `None` if absent.
    pub fn store_value(&self, key: &[u8]) -> Result<Option<&[u8]>> {
        let table = store_table();
        let id = EntityId::Binary(key.to_vec());
        match self.row(&table, &id, self.cut.system_time) {
            Some(event) => Ok(Some(value_of(event)?)),
            None => Ok(None),
        }
    }

    /// The first key in `range` written or erased after `since`, if any.
    /// This is the whole conflict condition of a store append.
    pub fn store_written_since(&self, range: &KeyRange, since: Basis) -> Result<Option<Vec<u8>>> {
        range.check()?;
        let table = store_table();
        let Some(rows) = self.data.tables.get(&table) else { return Ok(None); };
        for (id, events) in rows.events.range(range.ids(None, false)) {
            let EntityId::Binary(key) = id else { return Err(invalid_store_value()); };
            if events.iter().any(|(_, event)| event.cut.basis > since) {
                return Ok(Some(key.clone()));
            }
        }
        let (lower, upper) = range.ids(None, false);
        for (id, erased) in rows.erased.range((lower, upper)) {
            if *erased > since {
                let EntityId::Binary(key) = id else { return Err(invalid_store_value()); };
                return Ok(Some(key.clone()));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rtdb_types::{Interval, StructFields, Timestamp, rtdb_v1::{At, Layer, MAIN_BRANCH}};

    use super::*;
    use crate::branches::{BranchEffect, BranchIndex, RowImage, WrittenVersion};

    const INCARNATION: [u8; 16] = [7; 16];

    /// The SHA-256 of Bijection's `crates/fact_store/src/persistence/filter.rs`.
    /// Bijection's `fact_store` pins the same digest of its own copy
    /// (`persistence::tests::the_store_filter_matches_rtdbs_vendored_copy`),
    /// so a change on either side alone fails a test. Change the filter in
    /// Bijection, copy it to `vendor/bijection-store-filter/src/lib.rs` and
    /// update both pins in the paired commits.
    const BIJECTION_STORE_FILTER_SHA256: &str = "2c3d799315a33af3f086db6cb9a461a304293f6f567ab354870e960bdc3d191d";

    #[test]
    fn the_vendored_store_filter_is_bijections_pinned_copy() {
        use sha2::{Digest, Sha256};
        let source = include_bytes!("../../../../vendor/bijection-store-filter/src/lib.rs");
        let digest = Sha256::digest(source).iter().map(|byte| format!("{byte:02x}")).collect::<String>();
        assert_eq!(digest, BIJECTION_STORE_FILTER_SHA256, "vendor/bijection-store-filter drifted from Bijection's filter.rs");
    }

    #[test]
    fn the_vendored_store_derive_is_bijections_pinned_copy() {
        use sha2::{Digest, Sha256};
        let source = include_bytes!("../../../../vendor/bijection-store-derive/src/lib.rs");
        let digest = Sha256::digest(source).iter().map(|byte| format!("{byte:02x}")).collect::<String>();
        assert_eq!(digest, DERIVE_DIGEST, "vendor/bijection-store-derive drifted from Bijection's store_read/src/derive.rs");
    }

    fn cut(position: u64) -> BranchCut {
        BranchCut { basis: Basis::new(INCARNATION, position), system_time: Timestamp(position as i64 * 10) }
    }

    fn put(index: &mut BranchIndex, at: BranchCut, key: &[u8], value: &[u8]) -> Result<()> {
        let cells = StructFields::from_iter([(VALUE_COLUMN.to_owned(), Value::Binary(value.to_vec()))]);
        index.apply(BranchEffect::Write {
            branch: MAIN_BRANCH.into(), table: store_table(),
            event: BranchEvent {
                id: EntityId::Binary(key.to_vec()), valid: Interval::new(at.system_time, Timestamp::MAX)?,
                cut: at, written: WrittenVersion::Branch,
                image: Some(Arc::new(RowImage { cells, expires_at: None, custody_group: None, custody_payload: None })),
            },
        })
    }

    #[test]
    fn pages_are_ordered_versioned_and_conflicts_are_range_intersection() -> Result<()> {
        let mut index = BranchIndex::new(INCARNATION);
        index.advance(cut(1))?;
        put(&mut index, cut(1), b"a1", b"x")?;
        put(&mut index, cut(1), b"b1", b"y")?;
        index.advance(cut(2))?;
        put(&mut index, cut(2), b"a2", b"z")?;

        let all = KeyRange { start: b"a".to_vec(), end: Some(b"b".to_vec()) };
        let read = StoreRead { range: all.clone(), after: None, limit: 10, projection: Projection::Values, filter: None, reverse: false };
        let early = index.view(MAIN_BRANCH, At::Exactly(cut(1).basis), Layer::Merged, cut(2).system_time)?.store_page(&read)?;
        assert_eq!(early.entries.iter().map(|entry| entry.key.as_slice()).collect::<Vec<_>>(), [b"a1".as_slice()]);
        let late = index.view(MAIN_BRANCH, At::Latest, Layer::Merged, cut(2).system_time)?;
        let page = late.store_page(&StoreRead { limit: 1, ..read })?;
        assert_eq!(page.entries[0].value.as_deref(), Some(b"x".as_slice()));
        assert_eq!(page.resume.as_deref(), Some(b"a1".as_slice()));
        let sliced = late.store_page(&StoreRead {
            range: all.clone(), after: None, limit: 10, projection: Projection::Slice { start: 0, len: 0 }, filter: None, reverse: false,
        })?;
        assert!(sliced.entries.iter().all(|entry| entry.value.as_deref() == Some(b"".as_slice())));
        // A pushed-down filter returns what filtering the unfiltered page returns.
        let only_z = ValueFilter::Prefix { field: store_filter::Field::Whole, prefix: b"z".to_vec() };
        let filtered = late.store_page(&StoreRead {
            range: all.clone(), after: None, limit: 10, projection: Projection::Keys, filter: Some(only_z), reverse: false,
        })?;
        assert_eq!(filtered.entries.iter().map(|entry| entry.key.as_slice()).collect::<Vec<_>>(), [b"a2".as_slice()]);
        assert!(filtered.entries[0].value.is_none() && filtered.resume.is_none());
        // A reverse page descends, counts its limit from the end and resumes below its key.
        let newest = late.store_page(&StoreRead { range: all.clone(), after: None, limit: 1, projection: Projection::Keys, filter: None, reverse: true })?;
        assert_eq!(newest.entries.iter().map(|entry| entry.key.as_slice()).collect::<Vec<_>>(), [b"a2".as_slice()]);
        assert_eq!(newest.resume.as_deref(), Some(b"a2".as_slice()));
        let older = late.store_page(&StoreRead {
            range: all.clone(), after: newest.resume, limit: 10, projection: Projection::Keys, filter: None, reverse: true,
        })?;
        assert_eq!(older.entries.iter().map(|entry| entry.key.as_slice()).collect::<Vec<_>>(), [b"a1".as_slice()]);
        assert!(older.resume.is_none());

        assert_eq!(late.store_written_since(&all, cut(1).basis)?.as_deref(), Some(b"a2".as_slice()));
        assert_eq!(late.store_written_since(&all, cut(2).basis)?, None);
        // A write outside every read range never fails the reader.
        let other = KeyRange { start: b"b".to_vec(), end: None };
        assert_eq!(late.store_written_since(&other, cut(1).basis)?, None);
        Ok(())
    }

    fn stamped(at: u32) -> StoreWrite {
        // Partition `p`, then three bytes with a placeholder spliced in at `at`.
        let mut key = vec![0, 1, b'p'];
        key.extend_from_slice(b"row");
        key.splice(3 + at as usize..3 + at as usize, [0; STAMP_BYTES]);
        StoreWrite { key, value: Some(b"v".to_vec()), key_stamp: Some(3 + at), value_stamp: None }
    }

    #[test]
    fn placeholders_receive_the_stamp_and_misplaced_ones_are_refused() -> Result<()> {
        let stamp = StoreStamp { micros: 5, version: 9 };
        let [(key, value)] = <[_; 1]>::try_from(stamp_writes(&[stamped(0)], stamp)?).expect("one write");
        assert_eq!(&key[3..3 + STAMP_BYTES], &stamp.encode());
        assert_eq!(value.as_deref(), Some(b"v".as_slice()));
        assert!(StoreStamp { micros: -1, version: 9 }.encode() < stamp.encode());
        let mut nonzero = stamped(0);
        nonzero.key[4] = 1;
        for refused in [
            vec![StoreWrite { key_stamp: Some(1), ..stamped(0) }],
            vec![StoreWrite { key_stamp: Some(u32::MAX), ..stamped(0) }],
            vec![nonzero],
            vec![StoreWrite { value: None, value_stamp: Some(0), ..stamped(0) }],
            vec![stamped(0), stamped(0)],
            vec![stamped(0), stamped(1)],
        ] {
            assert!(check_placeholders(&refused).is_err(), "{refused:?}");
        }
        assert!(check_placeholders(&[stamped(0), StoreWrite { key: b"row".to_vec(), value: None, key_stamp: None, value_stamp: None }]).is_ok());
        Ok(())
    }

    #[test]
    fn ranges_are_half_open_and_nonempty() {
        let range = KeyRange { start: b"a".to_vec(), end: Some(b"c".to_vec()) };
        assert!(range.contains(b"a"));
        assert!(range.contains(b"b\xff"));
        assert!(!range.contains(b"c"));
        assert!(KeyRange { start: b"c".to_vec(), end: Some(b"c".to_vec()) }.check().is_err());
        assert!(KeyRange { start: b"c".to_vec(), end: None }.contains(b"\xff\xff"));
    }
}
