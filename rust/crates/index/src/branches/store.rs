//! The ordered-bytes store contract over the main branch.
//!
//! One relation holds the whole store: each entry's id is its key bytes and its
//! `v` cell holds the value bytes. Versions are the branch's commit bases. The
//! store knows no labels, worlds or indexes; its callers encode them as keys.

use std::ops::Bound;

use rtdb_types::{Anomaly, EntityId, Result, TableRef, Value, rtdb_v1::Basis};

use super::{BranchCut, BranchEvent, BranchView};

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

    fn ids(&self, after: Option<&[u8]>) -> (Bound<EntityId>, Bound<EntityId>) {
        let lower = match after {
            Some(after) if after >= self.start.as_slice() => Bound::Excluded(EntityId::Binary(after.to_vec())),
            _ => Bound::Included(EntityId::Binary(self.start.clone())),
        };
        let upper = match &self.end {
            Some(end) => Bound::Excluded(EntityId::Binary(end.clone())),
            // Binary sorts last among id kinds, so every key lies below this bound.
            None => Bound::Unbounded,
        };
        (lower, upper)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreRead {
    pub range: KeyRange,
    /// Resume strictly after this key.
    pub after: Option<Vec<u8>>,
    pub limit: usize,
    pub projection: Projection,
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
    /// Present when the limit stopped the page: resume after this key.
    pub resume: Option<Vec<u8>>,
}

pub fn invalid_store_value() -> Anomaly {
    Anomaly::fault("rtdb.store/invalid-entry", "A store entry must hold a binary id and a binary value")
}

fn value_of(event: &BranchEvent) -> Result<Vec<u8>> {
    match event.image.as_ref().and_then(|image| image.cells.get(VALUE_COLUMN)) {
        Some(Value::Binary(value)) => Ok(value.clone()),
        _ => Err(invalid_store_value()),
    }
}

impl BranchView<'_> {
    /// One ascending page of live entries in `read.range` at this view's cut.
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
        for (id, _) in rows.events.range(read.range.ids(read.after.as_deref())) {
            let EntityId::Binary(key) = id else { return Err(invalid_store_value()); };
            let Some(event) = self.row(&table, id, at) else { continue; };
            if entries.len() == limit {
                resume = entries.last().map(|entry: &StoreEntry| entry.key.clone());
                break;
            }
            let written = event.written_basis().ok_or_else(invalid_store_value)?;
            let value = match read.projection {
                Projection::Keys => None,
                projection => projection.apply(&value_of(event)?),
            };
            entries.push(StoreEntry { key: key.clone(), value, written });
        }
        Ok(StorePage { cut: self.cut, entries, resume })
    }

    /// The first key in `range` written or erased after `since`, if any.
    /// This is the whole conflict condition of a store append.
    pub fn store_written_since(&self, range: &KeyRange, since: Basis) -> Result<Option<Vec<u8>>> {
        range.check()?;
        let table = store_table();
        let Some(rows) = self.data.tables.get(&table) else { return Ok(None); };
        for (id, events) in rows.events.range(range.ids(None)) {
            let EntityId::Binary(key) = id else { return Err(invalid_store_value()); };
            if events.iter().any(|(_, event)| event.cut.basis > since) {
                return Ok(Some(key.clone()));
            }
        }
        let (lower, upper) = range.ids(None);
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
        let read = StoreRead { range: all.clone(), after: None, limit: 10, projection: Projection::Values };
        let early = index.view(MAIN_BRANCH, At::Exactly(cut(1).basis), Layer::Merged, cut(2).system_time)?.store_page(&read)?;
        assert_eq!(early.entries.iter().map(|entry| entry.key.as_slice()).collect::<Vec<_>>(), [b"a1".as_slice()]);
        let late = index.view(MAIN_BRANCH, At::Latest, Layer::Merged, cut(2).system_time)?;
        let page = late.store_page(&StoreRead { limit: 1, ..read })?;
        assert_eq!(page.entries[0].value.as_deref(), Some(b"x".as_slice()));
        assert_eq!(page.resume.as_deref(), Some(b"a1".as_slice()));
        let sliced = late.store_page(&StoreRead {
            range: all.clone(), after: None, limit: 10, projection: Projection::Slice { start: 0, len: 0 },
        })?;
        assert!(sliced.entries.iter().all(|entry| entry.value.as_deref() == Some(b"".as_slice())));

        assert_eq!(late.store_written_since(&all, cut(1).basis)?.as_deref(), Some(b"a2".as_slice()));
        assert_eq!(late.store_written_since(&all, cut(2).basis)?, None);
        // A write outside every read range never fails the reader.
        let other = KeyRange { start: b"b".to_vec(), end: None };
        assert_eq!(late.store_written_since(&other, cut(1).basis)?, None);
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
