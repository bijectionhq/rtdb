#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == match previous {
        Some(last) => last < candidate,
        None => true,
    },
))]
pub(crate) fn advances(previous: Option<i64>, candidate: i64) -> bool {
    match previous {
        Some(last) => last < candidate,
        None => true,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == match previous {
        Some(last) if candidate <= last =>
            if last == i64::MAX { None } else { Some((last + 1) as i64) },
        _ => Some(candidate),
    },
))]
pub(crate) fn automatic_time(previous: Option<i64>, candidate: i64) -> Option<i64> {
    match previous {
        Some(last) if !advances(previous, candidate) => last.checked_add(1),
        _ => Some(candidate),
    }
}

/// A supplied system time is admitted only when it advances the history and does not run ahead of now.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (match previous {
        Some(last) => last < candidate,
        None => true,
    } && candidate <= now),
))]
pub(crate) fn admits_supplied_time(previous: Option<i64>, candidate: i64, now: i64) -> bool {
    advances(previous, candidate) && candidate <= now
}

/// An external writer's time: its supplied time when admitted, otherwise the
/// earlier of that time and now, stepped past the predecessor.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == match supplied {
        Some(candidate) if match previous { Some(last) => last < candidate, None => true } && candidate <= now =>
            Some(candidate),
        Some(candidate) => match previous {
            Some(last) if (if candidate < now { candidate } else { now }) <= last =>
                if last < i64::MAX { Some((last + 1) as i64) } else { None },
            _ => Some(if candidate < now { candidate } else { now }),
        },
        None => match previous {
            Some(last) if now <= last => if last < i64::MAX { Some((last + 1) as i64) } else { None },
            _ => Some(now),
        },
    },
))]
pub(crate) fn external_time(previous: Option<i64>, supplied: Option<i64>, now: i64) -> Option<i64> {
    match supplied {
        Some(candidate) if admits_supplied_time(previous, candidate, now) => Some(candidate),
        Some(candidate) => automatic_time(previous, if candidate < now { candidate } else { now }),
        None => automatic_time(previous, now),
    }
}

const CLOCK_SLOT_WIDTH: i64 = 1_000_000;
const CLOCK_SLOT_LEAD: i64 = 500_000;

#[cfg(not(verus_keep_ghost))]
pub const MAX_CLOCK_SLOT_WAIT_MICROS: i64 = CLOCK_SLOT_WIDTH + CLOCK_SLOT_LEAD;

#[cfg(not(verus_keep_ghost))]
pub fn clock_slot_reading(observed: rtdb_types::Timestamp) -> Option<rtdb_types::Timestamp> {
    next_grid_reading(observed.0).map(rtdb_types::Timestamp)
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures match out {
        Some(reading) => observed >= 0 && reading as int >= observed as int + 500000
            && (reading as int) < observed as int + 1500000
            && reading % 1000000 == 0 && reading <= i64::MAX - 1000000,
        None => true,
    },
))]
pub(crate) fn next_grid_reading(observed: i64) -> Option<i64> {
    if observed < 0 { return None; }
    let candidate = observed.checked_add(CLOCK_SLOT_LEAD)?;
    let remainder = candidate % CLOCK_SLOT_WIDTH;
    let adjustment = if remainder == 0 { 0 } else { CLOCK_SLOT_WIDTH - remainder };
    let reading = candidate.checked_add(adjustment)?;
    reading.checked_add(CLOCK_SLOT_WIDTH)?;
    Some(reading)
}

#[cfg_attr(verus_keep_ghost, verus_spec(live =>
    ensures live == (observed < exclusive_deadline),
))]
pub(crate) fn owned_deadline_is_live(observed: i64, exclusive_deadline: i64) -> bool {
    observed < exclusive_deadline
}

/// Selects the origin for explicit acceptance-relative terms. Local receivers
/// without hosted clock evidence use the original append sample.
#[cfg_attr(verus_keep_ghost, verus_spec(selected =>
    ensures selected == match (slot, observed) {
        (Some(reading), _) | (None, Some(reading)) => reading,
        (None, None) => appended,
    },
))]
pub(crate) fn acceptance_relative_time(slot: Option<i64>, observed: Option<i64>, appended: i64) -> i64 {
    match (slot, observed) {
        (Some(reading), _) | (None, Some(reading)) => reading,
        (None, None) => appended,
    }
}

/// Compares the full assertion identity. History is authenticated separately
/// because it still supplies the business-time projection used by some guards.
#[cfg_attr(verus_keep_ghost, verus_spec(matches =>
    ensures matches == (actual == expected),
))]
pub(crate) fn owned_point_matches(
    actual: Option<(i64, u64, i64)>, expected: Option<(i64, u64, i64)>,
) -> bool {
    match (actual, expected) {
        (None, None) => true,
        (Some(actual), Some(expected)) =>
            actual.0 == expected.0 && actual.1 == expected.1 && actual.2 == expected.2,
        _ => false,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(admitted =>
    ensures admitted == (through <= predecessor && reading <= appended && reading <= observed
        && reading >= 0 && reading % 1000000 == 0
        && reading as int + 1000000 <= i64::MAX as int && (observed as int) < reading as int + 1000000),
))]
pub(crate) fn clock_slot_admits(
    through: i64, predecessor: i64, reading: i64, appended: i64, observed: i64,
) -> bool {
    through <= predecessor && reading <= appended && clock_slot_contains(reading, observed)
}

#[cfg_attr(verus_keep_ghost, verus_spec(admitted =>
    ensures admitted == (reading >= 0 && reading % 1000000 == 0
        && reading <= observed && reading as int + 1000000 <= i64::MAX as int
        && (observed as int) < reading as int + 1000000),
))]
pub(crate) fn clock_slot_contains(reading: i64, observed: i64) -> bool {
    let Some(until) = reading.checked_add(CLOCK_SLOT_WIDTH) else { return false; };
    reading >= 0 && reading % CLOCK_SLOT_WIDTH == 0 && reading <= observed && observed < until
}

#[cfg_attr(verus_keep_ghost, verus_spec(admitted =>
    ensures admitted == (same_domain && earlier_epoch <= epoch && earlier_seq <= seq && earlier_time <= time
        && (earlier_seq == seq ==> earlier_epoch == epoch && earlier_time == time)),
))]
pub(crate) fn accepts_clock_lineage(
    same_domain: bool, epoch: u64, earlier_epoch: u64, seq: u64, earlier_seq: u64, time: i64, earlier_time: i64,
) -> bool {
    same_domain && earlier_epoch <= epoch && earlier_seq <= seq && earlier_time <= time
        && (earlier_seq != seq || (earlier_epoch == epoch && earlier_time == time))
}

// The receipt carries the same booking commitment as the ordered seat.
#[cfg_attr(verus_keep_ghost, verus_spec(admitted => ensures
    admitted == (receipt == reading && reading as int + 1_000_000 == seat_until as int
        && seat_check && same_booking),
))]
pub(crate) fn original_funded_seat(
    reading: i64, receipt: i64, seat_until: i64, seat_check: bool, same_booking: bool,
) -> bool {
    receipt == reading && reading.checked_add(CLOCK_SLOT_WIDTH) == Some(seat_until)
        && seat_check && same_booking
}

#[cfg(not(verus_keep_ghost))]
pub(crate) fn clock_slot_time(
    slot: crate::OwnedClockSlot,
    head: Option<rtdb_types::TransactionKey>,
    appended: rtdb_types::Timestamp,
    observed: crate::DecisionObservation,
) -> Option<rtdb_types::Timestamp> {
    let head = head?;
    (observed.covers(slot.issued) && clock_slot_reading(slot.issued.instant) == Some(slot.reading)
        && clock_slot_admits(slot.through.0, head.tx_id.0, slot.reading.0, appended.0, observed.instant.0))
        .then_some(slot.reading)
}

#[cfg(all(test, not(verus_keep_ghost)))]
mod tests {
    use rtdb_types::{Timestamp, TransactionKey, TxId};

    use crate::{DecisionObservation, OwnedClockSlot};
    use super::{clock_slot_reading, clock_slot_time};

    #[test]
    fn clock_slot_tests_position_and_independent_half_open_window() {
        let reading = clock_slot_reading(Timestamp(100)).unwrap();
        assert_eq!(reading, Timestamp(1_000_000));
        let issued = DecisionObservation { domain: [3; 16], epoch: 2, sequence: 4, instant: Timestamp(100) };
        let slot = OwnedClockSlot { through: TxId(8), reading, issued, funded: false };
        let observe = |instant| DecisionObservation { sequence: 5, instant, ..issued };
        let behind = TransactionKey { tx_id: TxId(7), system_time: Timestamp(100) };
        let carried = TransactionKey { tx_id: TxId(9), system_time: Timestamp(301) };
        let later_history = TransactionKey { tx_id: TxId(9), system_time: Timestamp(1_100_000) };

        assert_eq!(clock_slot_time(slot, Some(carried), reading, observe(reading)), Some(reading));
        assert_eq!(clock_slot_time(slot, Some(behind), reading, observe(reading)), None);
        assert_eq!(clock_slot_time(slot, Some(carried), Timestamp(500_099), observe(reading)), None);
        assert_eq!(clock_slot_time(slot, Some(carried), reading, observe(Timestamp(500_099))), None);
        assert_eq!(clock_slot_time(slot, Some(carried), reading, observe(Timestamp(2_000_000))), None);
        assert_eq!(clock_slot_time(slot, Some(later_history), reading, observe(reading)), Some(reading));
        let foreign = DecisionObservation { domain: [7; 16], ..observe(reading) };
        assert_eq!(clock_slot_time(slot, Some(carried), reading, foreign), None);
        let successor = DecisionObservation { epoch: 3, ..observe(reading) };
        assert_eq!(clock_slot_time(slot, Some(carried), reading, successor), Some(reading));
    }
}
