#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

/// A confirmation or decision observation must identify the requested
/// transaction and history revision in the same unique row.
#[cfg_attr(verus_keep_ghost, verus_spec(matches => ensures
    matches == (rows == 1 && !seen && position == Some(requested_position)
        && revision == Some(requested_revision)),
))]
pub(super) fn exact_transaction_row(
    rows: usize, seen: bool, position: Option<i64>, revision: Option<i64>,
    requested_position: i64, requested_revision: i64,
) -> bool {
    rows == 1 && !seen && position == Some(requested_position)
        && revision == Some(requested_revision)
}
