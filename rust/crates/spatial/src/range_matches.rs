use std::sync::Arc;

use arrow_array::{Array, FixedSizeBinaryArray, LargeBinaryArray, UInt32Array};
use arrow_buffer::{Buffer, OffsetBuffer};
use rtdb_columnar::{EntityIdRef, MemoryBudget, MemoryReservation};
use rtdb_types::{Iid, Result};

use crate::postings::work_limit;

const MAX_PROBE_MATCHES: usize = 4096;
const MAX_PROBE_MATCH_BYTES: usize = 512 << 10;
const PROBE_MATCH_OVERHEAD: usize = 4096;
const IID_BYTES: usize = 16;
const ROW_BYTES: usize = size_of::<i64>() + IID_BYTES + size_of::<u32>();

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Retention {
    Range,
    Probe,
}

pub(super) struct MatchBuilder {
    keys: Vec<u8>,
    offsets: Vec<i64>,
    iids: Vec<u8>,
    reservation: Arc<MemoryReservation>,
    retention: Retention,
}

impl MatchBuilder {
    pub(super) fn new(memory: &Arc<MemoryBudget>, retention: Retention) -> Result<Self> {
        let overhead = match retention {
            Retention::Range => size_of::<i64>(),
            Retention::Probe => PROBE_MATCH_OVERHEAD,
        };
        let reservation = memory.reserve(overhead)?;
        Ok(Self {
            keys: Vec::new(),
            offsets: vec![0],
            iids: Vec::new(),
            reservation,
            retention,
        })
    }

    pub(super) fn push(&mut self, id: EntityIdRef<'_>, iid: Iid) -> Result<()> {
        let next_count = self.offsets.len();
        if self.retention == Retention::Probe && next_count > MAX_PROBE_MATCHES {
            return Err(work_limit());
        }
        let integer;
        let (tag, payload): (u8, &[u8]) = match &id {
            EntityIdRef::String(value) => (0, value.as_bytes()),
            EntityIdRef::Keyword(value) => (1, value.as_bytes()),
            EntityIdRef::Integer(value) => {
                integer = ((*value as u64) ^ (1 << 63)).to_be_bytes();
                (2, &integer)
            }
            EntityIdRef::Uuid(value) => (3, value),
            EntityIdRef::Binary(value) => (4, value),
        };
        let end = self.keys.len().checked_add(1)
            .and_then(|len| len.checked_add(payload.len()))
            .ok_or_else(work_limit)?;
        let offset = i64::try_from(end).map_err(|_| work_limit())?;
        let rows_capacity = next_count.next_power_of_two().max(8);
        let (overhead, byte_limit) = match self.retention {
            Retention::Range => (size_of::<i64>(), usize::MAX),
            Retention::Probe => (PROBE_MATCH_OVERHEAD, MAX_PROBE_MATCH_BYTES),
        };
        let fixed = overhead + rows_capacity * ROW_BYTES;
        let current = self.keys.capacity();
        let minimum = current.max(end);
        let limit = byte_limit.checked_sub(fixed)
            .filter(|&limit| minimum <= limit)
            .ok_or_else(work_limit)?;
        let preferred = if end > current {
            end.max(current.saturating_mul(2)).min(limit)
        } else {
            current
        };
        let reservation = Arc::get_mut(&mut self.reservation)
            .expect("match builder owns its reservation");
        let mut keys_capacity = preferred;
        if fixed + preferred > reservation.bytes()
            && let Err(error) = reservation.grow_to(fixed + preferred)
        {
            if preferred == minimum || error.code != "rtdb/memory-budget-exceeded" {
                return Err(error);
            }
            reservation.grow_to(fixed + minimum)?;
            keys_capacity = minimum;
        }
        self.keys.reserve_exact(keys_capacity - self.keys.len());
        self.offsets.reserve_exact(rows_capacity + 1 - self.offsets.len());
        self.iids.reserve_exact(rows_capacity * IID_BYTES - self.iids.len());
        self.keys.push(tag);
        self.keys.extend_from_slice(payload);
        self.offsets.push(offset);
        self.iids.extend_from_slice(iid.as_bytes());
        Ok(())
    }

    pub(super) fn finish(self, check: impl Fn() -> Result<()>) -> Result<RetainedMatches> {
        check()?;
        let Self { keys, offsets, iids, reservation, .. } = self;
        let count = u32::try_from(offsets.len() - 1).map_err(|_| work_limit())?;
        let keys = LargeBinaryArray::new(
            OffsetBuffer::new(offsets.into()), Buffer::from_vec(keys), None,
        );
        let iids = FixedSizeBinaryArray::new(IID_BYTES as i32, Buffer::from_vec(iids), None);
        let mut order = (0..count).collect::<Vec<_>>();
        // The handle tag and sign-biased integer payload preserve typed identity order bytewise.
        order.sort_unstable_by(|&left, &right| keys.value(left as usize).cmp(keys.value(right as usize)));
        check()?;
        Ok(RetainedMatches { keys, iids, order: UInt32Array::from(order), _reservation: reservation })
    }
}

pub(super) struct RetainedMatches {
    keys: LargeBinaryArray,
    iids: FixedSizeBinaryArray,
    order: UInt32Array,
    _reservation: Arc<MemoryReservation>,
}

impl RetainedMatches {
    pub(super) fn len(&self) -> usize {
        self.order.len()
    }

    pub(super) fn has_ties(&self) -> bool {
        self.order.values().windows(2).any(|rows| {
            self.keys.value(rows[0] as usize) == self.keys.value(rows[1] as usize)
        })
    }

    pub(super) fn seek_after(&self, after: EntityIdRef<'_>) -> Result<usize> {
        let mut lower = 0;
        let mut upper = self.len();
        while lower < upper {
            let mid = lower + (upper - lower) / 2;
            let row = self.order.value(mid) as usize;
            if EntityIdRef::from_handle_bytes(self.keys.value(row))? <= after {
                lower = mid + 1;
            } else {
                upper = mid;
            }
        }
        Ok(lower)
    }

    pub(super) fn get(&self, position: usize) -> Result<(EntityIdRef<'_>, Iid)> {
        let row = self.order.value(position) as usize;
        let id = EntityIdRef::from_handle_bytes(self.keys.value(row))?;
        let iid = Iid::from_slice(self.iids.value(row))?;
        Ok((id, iid))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use EntityIdRef::{Integer, Keyword, String, Uuid};

    #[test]
    fn retained_matches_seek_in_typed_order_and_keep_physical_iids() -> Result<()> {
        let memory = MemoryBudget::new(16 << 10);
        let ids = [
            Integer(i64::MAX), Keyword("same"), Uuid([0; 16]), String("same"),
            Integer(-1), String("sam"), Integer(i64::MIN),
        ];
        let mut builder = MatchBuilder::new(&memory, Retention::Range)?;
        for (row, id) in ids.into_iter().enumerate() {
            builder.push(id, Iid([row as u8; 16]))?;
        }

        let matches = builder.finish(|| Ok(()))?;
        assert!(!matches.has_ties());
        for (position, row) in [5, 3, 1, 6, 4, 0, 2].into_iter().enumerate() {
            assert_eq!(matches.get(position)?, (ids[row], Iid([row as u8; 16])));
            assert_eq!(matches.seek_after(ids[row])?, position + 1);
        }
        assert_eq!(matches.seek_after(String(""))?, 0);
        assert_eq!(matches.seek_after(Integer(0))?, 5);
        assert!(memory.used_bytes() > 0);
        drop(matches);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    #[test]
    fn retained_matches_detect_complete_identity_ties_and_seek_past_them() -> Result<()> {
        let memory = MemoryBudget::new(16 << 10);
        let mut builder = MatchBuilder::new(&memory, Retention::Probe)?;
        builder.push(String("same"), Iid::MIN)?;
        builder.push(Keyword("same"), Iid::MIN)?;
        builder.push(String("same"), Iid::MAX)?;

        let matches = builder.finish(|| Ok(()))?;
        assert!(matches.has_ties());
        assert_eq!(matches.seek_after(String("same"))?, 2);
        assert_eq!(matches.get(2)?, (Keyword("same"), Iid::MIN));
        Ok(())
    }

    #[test]
    fn optional_probe_matches_respect_count_and_byte_limits() -> Result<()> {
        let memory = MemoryBudget::new(MAX_PROBE_MATCH_BYTES * 2);
        let mut builder = MatchBuilder::new(&memory, Retention::Probe)?;
        for i in 0..MAX_PROBE_MATCHES {
            builder.push(Integer(i as i64), Iid::MIN)?;
        }
        assert!(builder.push(Integer(-1), Iid::MIN).is_err());
        let matches = builder.finish(|| Ok(()))?;
        assert_eq!(matches.len(), MAX_PROBE_MATCHES);
        drop(matches);
        assert_eq!(memory.used_bytes(), 0);

        let mut builder = MatchBuilder::new(&memory, Retention::Probe)?;
        let text = "a".repeat(MAX_PROBE_MATCH_BYTES);
        assert!(builder.push(String(&text), Iid::MIN).is_err());
        assert_eq!(builder.finish(|| Ok(()))?.len(), 0);

        let memory = MemoryBudget::new(PROBE_MATCH_OVERHEAD);
        let mut builder = MatchBuilder::new(&memory, Retention::Probe)?;
        assert_eq!(builder.push(Integer(1), Iid::MIN).unwrap_err().code, "rtdb/memory-budget-exceeded");
        drop(builder);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    #[test]
    fn match_buffers_can_grow_to_exact_need_under_memory_pressure() -> Result<()> {
        let limit = size_of::<i64>() + 8 * ROW_BYTES + 8;
        let memory = MemoryBudget::new(limit);
        let mut builder = MatchBuilder::new(&memory, Retention::Range)?;
        builder.push(String("abcde"), Iid::MIN)?;
        builder.push(String("z"), Iid::MAX)?;
        assert_eq!(memory.used_bytes(), limit);
        assert_eq!(builder.push(String("a"), Iid::MIN).unwrap_err().code, "rtdb/memory-budget-exceeded");

        let matches = builder.finish(|| Ok(()))?;
        assert_eq!(matches.len(), 2);
        assert_eq!(matches.get(1)?, (String("z"), Iid::MAX));
        Ok(())
    }
}
