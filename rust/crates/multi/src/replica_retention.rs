#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out.is_some() <==> after < authorized_through < i64::MAX,
        out.is_some() ==> out.unwrap().0 as int == after as int + 1,
        out.is_some() ==> out.unwrap().1 as int == authorized_through as int + 1,
))]
pub(crate) fn excluded_replay_start(after: i64, authorized_through: i64) -> Option<(i64, i64)> {
    if after >= authorized_through { return None; }
    let first_retained = authorized_through.checked_add(1)?;
    Some((after + 1, first_retained))
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out.is_some() <==> 0 < checkpoint && 0 < first_required,
        out.is_some() ==> 0 <= out.unwrap() < checkpoint && out.unwrap() < first_required,
        out.is_some() ==> (out.unwrap() as int) + 1 == checkpoint || (out.unwrap() as int) + 1 == first_required,
))]
pub(crate) fn replica_trim_through(checkpoint: i64, first_required: i64) -> Option<i64> {
    let first_retained = checkpoint.min(first_required);
    if first_retained <= 0 { return None; }
    Some(first_retained - 1)
}
