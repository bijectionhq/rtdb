#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out.is_some() <==> (after as int) + 1 < first_retained as int,
        out.is_some() ==> out.unwrap() as int == (after as int) + 1,
))]
pub(crate) fn missing_replay_offset(after: i64, first_retained: i64) -> Option<i64> {
    let Some(requested) = after.checked_add(1) else { return None; };
    if requested < first_retained { Some(requested) } else { None }
}
