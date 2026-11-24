#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(matches => ensures matches == (
    base == prepared_base && block == prepared_block && assigned == prepared
)))]
pub(super) fn same_stage_order(
    base: Option<(i64, i64)>, prepared_base: Option<(i64, i64)>,
    block: i64, prepared_block: i64, assigned: (i64, i64), prepared: (i64, i64),
) -> bool {
    base == prepared_base && block == prepared_block && assigned == prepared
}

/// Application ordinals stay first. Order metadata stays in the same transaction.
#[cfg_attr(verus_keep_ghost, verus_spec(shifted => ensures
    (shifted is Some) == (event matches Some((transaction, ordinal))
        && transaction == assigned && ordinal as int + application_rows as int <= u64::MAX),
    shifted is Some ==> shifted->Some_0 == (assigned, ((event->Some_0).1 as int + application_rows as int) as u64),
))]
pub(super) fn shift_metadata_position(
    event: Option<(i64, u64)>, assigned: i64, application_rows: u64,
) -> Option<(i64, u64)> {
    let (transaction, ordinal) = event?;
    if transaction != assigned {
        return None;
    }
    Some((transaction, ordinal.checked_add(application_rows)?))
}

/// Within a landing, each authored assertion receives the next distinct position.
#[cfg_attr(verus_keep_ghost, verus_spec(next => ensures
    (next is Some) == (prior < u32::MAX),
    next is Some ==> next->Some_0 as int == prior as int + 1,
))]
pub(super) fn next_landing_position(prior: u32) -> Option<u32> {
    prior.checked_add(1)
}
