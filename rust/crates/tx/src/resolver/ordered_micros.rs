//! Fill a placed Root fact's binary timestamp slots before staging its row.

use std::sync::Arc;

use rtdb_columnar::MemoryBudget;
use rtdb_types::{Anomaly, Result, StructFields, Timestamp, Value};

use crate::Documents;

pub(super) const FIELD: &str = "_ordered_micros";
const MAX_SLOTS: usize = 1_024;
const WIDTH: usize = std::mem::size_of::<i64>();
const SORT_KEY_SLOT: u32 = 1 << 31;
const SYSTEM_REVISION_SLOT: u32 = 1 << 30;

pub(super) fn fill(
    documents: &Documents,
    ordered: Timestamp,
    allows_key_slot: impl Fn(&str) -> bool,
    memory: &Arc<MemoryBudget>,
) -> Result<Documents> {
    let bytes = documents.rows().iter().try_fold(4096usize, |bytes, row| {
        let retained = Value::document_retained_bytes(row)?;
        bytes.checked_add(retained).ok_or_else(invalid)
    })?;
    let _scratch = memory.reserve(bytes.checked_mul(2).ok_or_else(invalid)?)?;
    let mut rows = documents.rows().to_vec();
    for row in &mut rows {
        match row.remove(FIELD) {
            Some(Value::Binary(slots)) => fill_row(row, &slots, ordered, &allows_key_slot)?,
            None | Some(Value::Null | Value::Undefined) => (),
            _ => return Err(invalid()),
        }
    }
    let columns = documents.columns().iter().filter(|name| name.as_str() != FIELD).cloned().collect();
    Documents::with_column_order(rows, columns, memory)
}

fn fill_row(
    row: &mut StructFields<Value>,
    slots: &[u8],
    ordered: Timestamp,
    allows_key_slot: impl Fn(&str) -> bool,
) -> Result<()> {
    fill_slots(slots, ordered, allows_key_slot, |column, start, bytes| {
        let Some(Value::Binary(value)) = row.get_mut(column) else { return Err(invalid()); };
        fill_bytes(value, start, bytes)
    })
}

pub(super) fn fill_bytes(value: &mut [u8], start: usize, bytes: [u8; WIDTH]) -> Result<()> {
    let end = start.checked_add(WIDTH).ok_or_else(invalid)?;
    let slot = value.get_mut(start..end).ok_or_else(invalid)?;
    if slot.iter().any(|byte| *byte != 0) { return Err(invalid()); }
    slot.copy_from_slice(&bytes);
    Ok(())
}

pub(super) fn fill_slots(
    slots: &[u8],
    ordered: Timestamp,
    allows_key_slot: impl Fn(&str) -> bool,
    mut fill: impl FnMut(&str, usize, [u8; WIDTH]) -> Result<()>,
) -> Result<()> {
    let (count, mut remaining) = slots.split_first_chunk::<2>().ok_or_else(invalid)?;
    let count = usize::from(u16::from_be_bytes(*count));
    if !(1..=MAX_SLOTS).contains(&count) {
        return Err(invalid());
    }
    let mut previous = ("", 0);
    for _ in 0..count {
        let (&width, rest) = remaining.split_first().ok_or_else(invalid)?;
        let (column, rest) = rest.split_at_checked(usize::from(width)).ok_or_else(invalid)?;
        let column = std::str::from_utf8(column).map_err(|_| invalid())?;
        let (start, rest) = rest.split_first_chunk::<4>().ok_or_else(invalid)?;
        let start = u32::from_be_bytes(*start);
        let sort_key = start & SORT_KEY_SLOT != 0;
        let system_revision = start & SYSTEM_REVISION_SLOT != 0;
        let start = usize::try_from(start & !(SORT_KEY_SLOT | SYSTEM_REVISION_SLOT)).map_err(|_| invalid())?;
        let end = start.checked_add(WIDTH).ok_or_else(invalid)?;
        let (delta, rest) = rest.split_first_chunk::<8>().ok_or_else(invalid)?;
        if column.is_empty() || column.starts_with('_')
            || column < previous.0 || (column == previous.0 && start < previous.1)
            || (sort_key && !allows_key_slot(column))
        {
            return Err(invalid());
        }
        let delta = i64::from_be_bytes(*delta);
        let bits = if system_revision {
            if delta != 0 { return Err(invalid()); }
            let revision = u64::try_from(ordered.0).ok()
                .and_then(|micros| micros.checked_mul(1_000)).ok_or_else(invalid)?;
            if sort_key && i64::try_from(revision).is_err() { return Err(invalid()); }
            revision
        } else {
            ordered.0.checked_add(delta).ok_or_else(invalid)? as u64
        };
        let bits = if sort_key { bits ^ (1 << 63) } else { bits };
        fill(column, start, bits.to_be_bytes())?;
        previous = (column, end);
        remaining = rest;
    }
    if !remaining.is_empty() {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn invalid() -> Anomaly {
    Anomaly::incorrect("rtdb.owned/ordered-micros", "Invalid ordered binary time slots")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slots(entries: &[(&str, u32, i64)]) -> Vec<u8> {
        let mut out = u16::try_from(entries.len()).unwrap().to_be_bytes().to_vec();
        for &(column, start, delta) in entries {
            out.push(u8::try_from(column.len()).unwrap());
            out.extend_from_slice(column.as_bytes());
            out.extend_from_slice(&start.to_be_bytes());
            out.extend_from_slice(&delta.to_be_bytes());
        }
        out
    }

    #[test]
    fn fills_the_signed_offset_at_acceptance() {
        let mut row = StructFields::from([("payload".into(), Value::Binary(vec![0; 18]))]);
        let slots = slots(&[("payload", 1, -7), ("payload", 10, 3)]);
        fill_row(&mut row, &slots, Timestamp(123), |_| false).unwrap();
        let Value::Binary(bytes) = &row["payload"] else { panic!("Expected binary payload"); };
        assert_eq!(&bytes[1..9], &116i64.to_be_bytes());
        assert_eq!(&bytes[10..18], &126i64.to_be_bytes());
    }

    #[test]
    fn fills_sort_keys_in_signed_time_order_from_the_same_acceptance() {
        let mut row = StructFields::from([
            ("body".into(), Value::Binary(vec![0; 8])),
            ("p_a".into(), Value::Binary(vec![0; 16])),
        ]);
        let slots = slots(&[("body", 0, -7), ("p_a", SORT_KEY_SLOT, -7), ("p_a", SORT_KEY_SLOT | 8, 4)]);
        assert!(fill_row(&mut row.clone(), &slots, Timestamp(3), |_| false).is_err());
        fill_row(&mut row, &slots, Timestamp(3), |column| column == "p_a").unwrap();
        assert_eq!(row["body"], Value::Binary((-4i64).to_be_bytes().to_vec()));
        let Value::Binary(key) = &row["p_a"] else { panic!("Expected binary key"); };
        assert_eq!(&key[..8], &(((-4i64) as u64) ^ (1 << 63)).to_be_bytes());
        assert_eq!(&key[8..], &(7u64 ^ (1 << 63)).to_be_bytes());
        assert!(key[..8] < key[8..]);
    }

    #[test]
    fn rejects_overlapping_slots_and_micros_overflow() {
        let row = StructFields::from([("payload".into(), Value::Binary(vec![0; 16]))]);
        let overlapping = slots(&[("payload", 0, 0), ("payload", 7, 0)]);
        assert!(fill_row(&mut row.clone(), &overlapping, Timestamp(0), |_| false).is_err());
        let overflow = slots(&[("payload", 0, 1)]);
        assert!(fill_row(&mut row.clone(), &overflow, Timestamp(i64::MAX), |_| false).is_err());
    }

    #[test]
    fn fills_revision_nanos_and_acceptance_micros_without_changing_length() {
        let mut row = StructFields::from([
            ("body".into(), Value::Binary(vec![0; 16])),
            ("key".into(), Value::Binary(vec![0; 8])),
        ]);
        let recipe = slots(&[
            ("body", SYSTEM_REVISION_SLOT, 0),
            ("body", 8, -7),
            ("key", SYSTEM_REVISION_SLOT | SORT_KEY_SLOT, 0),
        ]);
        fill_row(&mut row, &recipe, Timestamp(123), |column| column == "key").unwrap();
        let Value::Binary(bytes) = &row["body"] else { panic!("Expected binary payload"); };
        assert_eq!(&bytes[..8], &123_000u64.to_be_bytes());
        assert_eq!(&bytes[8..], &116i64.to_be_bytes());
        assert_eq!(row["key"], Value::Binary((123_000u64 ^ (1 << 63)).to_be_bytes().to_vec()));

        let mut row = StructFields::from([("body".into(), Value::Binary(vec![0; 8]))]);
        let with_offset = slots(&[("body", SYSTEM_REVISION_SLOT, 1)]);
        assert!(fill_row(&mut row, &with_offset, Timestamp(1), |_| false).is_err());
        let revision = slots(&[("body", SYSTEM_REVISION_SLOT, 0)]);
        assert!(fill_row(&mut row, &revision, Timestamp(i64::MAX), |_| false).is_err());
    }
}
