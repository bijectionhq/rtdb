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

/// A client may name a system time that advances the history and does not run ahead of the log's append time.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (match previous {
        Some(last) => last < candidate,
        None => true,
    } && candidate <= log_time),
))]
pub(crate) fn admits_client_time(previous: Option<i64>, candidate: i64, log_time: i64) -> bool {
    advances(previous, candidate) && candidate <= log_time
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

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == match previous {
        Some(last) if logged < last => last,
        _ => logged,
    },
))]
pub(crate) fn historical_time(previous: Option<i64>, logged: i64) -> i64 {
    match previous {
        Some(last) if logged < last => last,
        _ => logged,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (match previous {
        Some(last) => last <= requested,
        None => true,
    } && requested <= logged),
))]
pub(crate) fn admits_historical_time(previous: Option<i64>, requested: i64, logged: i64) -> bool {
    historical_time(previous, requested) == requested && requested <= logged
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

#[cfg(not(verus_keep_ghost))]
pub(crate) fn clock_slot_time(
    slot: crate::OwnedClockSlot,
    head: Option<rtdb_types::TransactionKey>,
    appended: rtdb_types::Timestamp,
    observed: rtdb_types::Timestamp,
) -> Option<rtdb_types::Timestamp> {
    let head = head?;
    clock_slot_admits(
        slot.through.0, head.tx_id.0, slot.reading.0, appended.0, observed.0,
    ).then_some(slot.reading)
}

#[cfg(not(verus_keep_ghost))]
pub(crate) enum SourceTime {
    Automatic,
    ExactSystem { requested: i64, require_current_append: bool },
    ExactOwnedPublication(i64),
}

#[cfg(not(verus_keep_ghost))]
pub(crate) enum TimeRejection {
    BehindHistory(i64),
    RepeatedOwnedPublication(i64),
    AheadOfAppend(i64),
    BehindAppend(i64),
}

#[cfg(not(verus_keep_ghost))]
impl SourceTime {
    pub(crate) fn select(
        self, previous: Option<i64>, appended: i64, automatic: i64,
    ) -> std::result::Result<i64, TimeRejection> {
        match self {
            Self::Automatic => Ok(automatic),
            Self::ExactSystem { requested, .. } if requested < historical_time(previous, requested) => {
                Err(TimeRejection::BehindHistory(requested))
            }
            Self::ExactSystem { requested, require_current_append } => {
                if !admits_historical_time(previous, requested, appended) {
                    return Err(TimeRejection::AheadOfAppend(requested));
                }
                require_append_bound(requested, appended, require_current_append)?;
                Ok(requested)
            }
            Self::ExactOwnedPublication(requested) => {
                require_append_bound(requested, appended, false)?;
                if !advances(previous, requested) {
                    return Err(TimeRejection::RepeatedOwnedPublication(requested));
                }
                Ok(requested)
            }
        }
    }
}

#[cfg(not(verus_keep_ghost))]
fn require_append_bound(
    requested: i64, appended: i64, exact: bool,
) -> std::result::Result<(), TimeRejection> {
    if !admits_client_time(None, requested, appended) {
        return Err(TimeRejection::AheadOfAppend(requested));
    }
    if exact && requested < appended {
        return Err(TimeRejection::BehindAppend(requested));
    }
    Ok(())
}

#[cfg(all(test, not(verus_keep_ghost)))]
mod tests {
    use rtdb_types::{Timestamp, TransactionKey, TxId};

    use crate::OwnedClockSlot;
    use super::{clock_slot_reading, clock_slot_time};

    #[test]
    fn clock_slot_tests_position_and_independent_half_open_window() {
        let reading = clock_slot_reading(Timestamp(100)).unwrap();
        assert_eq!(reading, Timestamp(1_000_000));
        let slot = OwnedClockSlot { through: TxId(8), reading };
        let behind = TransactionKey { tx_id: TxId(7), system_time: Timestamp(100) };
        let carried = TransactionKey { tx_id: TxId(9), system_time: Timestamp(301) };
        let later_history = TransactionKey { tx_id: TxId(9), system_time: Timestamp(1_100_000) };

        assert_eq!(clock_slot_time(slot, Some(carried), reading, reading), Some(reading));
        assert_eq!(clock_slot_time(slot, Some(behind), reading, reading), None);
        assert_eq!(clock_slot_time(slot, Some(carried), Timestamp(500_099), reading), None);
        assert_eq!(clock_slot_time(slot, Some(carried), reading, Timestamp(500_099)), None);
        assert_eq!(clock_slot_time(slot, Some(carried), reading, Timestamp(2_000_000)), None);
        assert_eq!(clock_slot_time(slot, Some(later_history), reading, reading), Some(reading));
    }
}
