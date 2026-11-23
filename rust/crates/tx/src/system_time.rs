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

#[cfg(not(verus_keep_ghost))]
pub fn clock_slot_reading(tick: rtdb_types::Timestamp) -> Option<rtdb_types::Timestamp> {
    tick.0.checked_add(500_000).map(rtdb_types::Timestamp)
}

#[cfg(not(verus_keep_ghost))]
pub(crate) fn clock_slot_time(
    tick: rtdb_types::TransactionKey,
    head: Option<rtdb_types::TransactionKey>,
    appended: rtdb_types::Timestamp,
) -> Option<rtdb_types::Timestamp> {
    const GRACE_MICROS: i64 = 1_000_000;
    let head = head?;
    let reading = clock_slot_reading(tick.system_time)?;
    (head.tx_id.0 >= tick.tx_id.0
        && head.system_time >= tick.system_time
        && head.system_time < reading
        && appended >= reading
        && appended.0 < reading.0.saturating_add(GRACE_MICROS))
        .then_some(reading)
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

    use super::clock_slot_time;

    #[test]
    fn clock_slot_survives_earlier_orders_and_admits_only_its_append_window() {
        let tick = TransactionKey { tx_id: TxId(8), system_time: Timestamp(100) };
        let earlier = TransactionKey { tx_id: TxId(9), system_time: Timestamp(101) };
        let used = TransactionKey { tx_id: TxId(9), system_time: Timestamp(500_100) };

        assert_eq!(clock_slot_time(tick, Some(earlier), Timestamp(500_100)), Some(Timestamp(500_100)));
        assert_eq!(clock_slot_time(tick, Some(tick), Timestamp(500_099)), None);
        assert_eq!(clock_slot_time(tick, Some(tick), Timestamp(1_500_100)), None);
        assert_eq!(clock_slot_time(tick, Some(used), Timestamp(500_100)), None);
    }
}
