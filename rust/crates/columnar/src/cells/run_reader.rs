use std::sync::atomic::{AtomicI32, Ordering};

use super::native_scalar::ScalarStorageRef;
use super::*;

#[derive(Debug)]
pub(super) struct RunStorage {
    pub logical_offset: usize,
    pub ends: Arc<PhysicalNode>,
    pub values: Arc<PhysicalNode>,
}

#[derive(Debug)]
pub(super) struct RunEndReader { storage: RunStorage, hint: AtomicI32 }

impl RunEndReader {
    pub(super) fn new(storage: RunStorage) -> Self { Self { storage, hint: AtomicI32::new(0) } }
    pub(super) fn ends(&self) -> &Arc<PhysicalNode> { &self.storage.ends }
    pub(super) fn values(&self) -> &Arc<PhysicalNode> { &self.storage.values }
    pub(super) fn logical_offset(&self) -> usize { self.storage.logical_offset }

    pub(super) fn index(&self, row: PhysicalIndex) -> Result<PhysicalIndex> {
        let logical = row.0.wrapping_add(i32::try_from(self.storage.logical_offset).map_err(|_| index_error())?);
        let count = self.ends().count.get();
        let hint = self.hint.load(Ordering::Relaxed);
        let next = hint.wrapping_add(1);
        let index = if hint < count && logical >= self.start(hint)? && logical < self.end(hint)? {
            hint
        } else if next < count && logical >= self.start(next)? && logical < self.end(next)? {
            next
        } else {
            let (mut lo, mut hi) = (0i32, count);
            while lo < hi {
                let mid = ((lo.wrapping_add(hi) as u32) >> 1) as i32;
                if self.end(mid)? <= logical { lo = mid.wrapping_add(1); } else { hi = mid; }
            }
            lo
        };
        self.hint.store(index, Ordering::Relaxed);
        Ok(PhysicalIndex(index))
    }

    fn start(&self, run: i32) -> Result<i32> { if run == 0 { Ok(0) } else { self.end(run.wrapping_sub(1)) } }
    fn end(&self, run: i32) -> Result<i32> { Ok(read_long(self.ends(), PhysicalIndex(run))? as i32) }
}

fn read_long(mut node: &PhysicalNode, mut row: PhysicalIndex) -> Result<i64> {
    while let NodeLayout::RunEnd(reader) = &node.storage {
        row = reader.index(row)?;
        node = reader.values();
    }
    if !node.descriptor.scalar().is_some_and(|scalar| getters::Getter::Long.admits(scalar.logical_type())) { return Err(unsupported("getLong")); }
    if node.null(row)? { return Err(Anomaly::fault("xtdb.error/unknown", format!("null at index {}", row.0))); }
    let cell = ResolvedCell { node, row }.payload()?;
    getters::long(ScalarStorageRef::node(node)?, node.scalar_field()?.data_type(), cell.row)
}

#[cfg(test)]
mod tests;
