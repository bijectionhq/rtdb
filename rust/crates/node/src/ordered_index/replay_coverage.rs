#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out == Some(false) <==> rows == 0 && (next.is_none() || block < next.unwrap().0),
        out == Some(true) <==> rows > 0 && next == Some((block, Some(rows))),
        out.is_none() <==>
            !(rows == 0 && (next.is_none() || block < next.unwrap().0))
                && !(rows > 0 && next == Some((block, Some(rows)))),
))]
pub(super) fn classify_replay_block(next: Option<(i64, Option<i64>)>, block: i64, rows: i64) -> Option<bool> {
    match next {
        Some((expected_block, Some(expected_rows))) if rows > 0 && (block, rows) == (expected_block, expected_rows) => Some(true),
        Some((expected_block, _)) if rows == 0 && block < expected_block => Some(false),
        None if rows == 0 => Some(false),
        _ => None,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (exhausted && observed == expected),
))]
pub(super) fn replay_complete(exhausted: bool, observed: i64, expected: i64) -> bool {
    exhausted && observed == expected
}
