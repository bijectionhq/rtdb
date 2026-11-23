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

#[cfg(not(verus_keep_ghost))]
pub(crate) enum SourceTime {
    Automatic,
    ExactSystem { requested: i64, require_current_append: bool },
    ObservedAppend(i64),
}

#[cfg(not(verus_keep_ghost))]
pub(crate) enum TimeRejection {
    BehindHistory(i64),
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
            Self::ExactSystem { requested, .. } if !advances(previous, requested) => {
                Err(TimeRejection::BehindHistory(requested))
            }
            Self::ExactSystem { requested, require_current_append } => {
                require_append_bound(requested, appended, require_current_append)?;
                Ok(requested)
            }
            Self::ObservedAppend(requested) => {
                require_append_bound(requested, appended, true)?;
                Ok(automatic)
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
