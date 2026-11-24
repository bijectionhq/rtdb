#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(valid => ensures
    valid == (start == next && (!resuming || exact_previous_cut)),
))]
pub(super) fn starts_at(next: u64, start: u64, resuming: bool, exact_previous_cut: bool) -> bool {
    start == next && (!resuming || exact_previous_cut)
}

#[cfg_attr(verus_keep_ghost, verus_spec(valid => ensures
    valid == (from as int + 1 == offered as int && offered <= through
        && (offered < through || terminal_matches)),
))]
pub(super) fn next_in_hold(from: u64, offered: u64, through: u64, terminal_matches: bool) -> bool {
    from.checked_add(1) == Some(offered) && offered <= through
        && (offered < through || terminal_matches)
}

#[cfg_attr(verus_keep_ghost, verus_spec(valid => ensures
    valid == (opened as int + 1 == through as int && base <= through
        && match checkpoint {
            None => genesis && base == 0,
            Some(boundary) => !genesis && exact_root && boundary as int + 1 == base as int,
        }),
))]
pub(crate) fn recovery_coordinates_match(
    opened: i64, through: u64, base: u64, genesis: bool, checkpoint: Option<i64>, exact_root: bool,
) -> bool {
    opened as i128 + 1 == through as i128 && base <= through && match checkpoint {
        None => genesis && base == 0,
        Some(boundary) => !genesis && exact_root && boundary as i128 + 1 == base as i128,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(selected => ensures
    selected == if exact_checkpoint && exact_through && exact_record_cut
        && observed_time == prepared_time && observed_bytes == sealed_bytes
    { Some(record) } else { None },
))]
pub(super) fn select_record<T>(
    record: T,
    exact_checkpoint: bool,
    exact_through: bool,
    exact_record_cut: bool,
    observed_time: i64,
    prepared_time: i64,
    observed_bytes: usize,
    sealed_bytes: usize,
) -> Option<T> {
    if exact_checkpoint && exact_through && exact_record_cut
        && observed_time == prepared_time && observed_bytes == sealed_bytes
    {
        Some(record)
    } else {
        None
    }
}
