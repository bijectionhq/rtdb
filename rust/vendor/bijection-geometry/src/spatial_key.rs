//! Hilbert key ranges shared by the spatial posting recipe.
use crate::{GeometryRefusal, GeometryWork, cells::{self, CellId}};

/// Quadtree levels of a leaf cell.
pub const CELL_LEVELS: u32 = cells::MAX_LEVEL;
/// The key ranges that find every multi-cell cover meeting `cover`: each
/// cell's descendants, and each of its ancestors as one exact key. Merged.
pub(crate) fn cover_ranges(
    class: u8,
    cover: &[CellId],
    work: &mut GeometryWork,
) -> Result<Vec<KeyRange>, GeometryRefusal> {
    let mut ranges = Vec::with_capacity(cover.len().saturating_mul(CELL_LEVELS as usize + 1));
    for cell in cover {
        ranges.push(descendants(class, *cell));
        for level in 0..cell.level() {
            work.charge(1)?;
            let ancestor = key(class, cell.parent(level));
            ranges.push(KeyRange {
                start: ancestor,
                end: ancestor,
            });
        }
    }
    Ok(merge(ranges))
}

/// Sorted, overlapping and adjacent ranges joined.
pub(crate) fn merge(mut ranges: Vec<KeyRange>) -> Vec<KeyRange> {
    ranges.sort_unstable_by_key(|range| range.start);
    let mut merged: Vec<KeyRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if let Some(last) = merged.last_mut()
            && range.start <= last.end.saturating_add(1)
        {
            last.end = last.end.max(range.end);
        } else {
            merged.push(range);
        }
    }
    merged
}

pub(crate) fn key(class: u8, cell: CellId) -> i64 {
    key_raw(class, cell.raw())
}

fn key_raw(class: u8, raw: u64) -> i64 {
    ((u64::from(class) << cells::ID_BITS) | raw) as i64
}

/// Inclusive bounds on private spatial keys. These are candidate supersets;
/// the native predicate remains the authority for every returned row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyRange {
    pub start: i64,
    pub end: i64,
}

/// Every key of a class under a cell, the cell's own included.
pub(crate) fn descendants(class: u8, cell: CellId) -> KeyRange {
    let (first, last) = cell.range();
    KeyRange {
        start: key_raw(class, first),
        end: key_raw(class, last),
    }
}
