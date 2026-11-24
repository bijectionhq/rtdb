use crate::transaction_evidence::exact_transaction_row;
use vstd::prelude::*;

verus! {

pub fn transaction_evidence_has_original_coordinate(
    rows: usize, seen: bool, position: Option<i64>, revision: Option<i64>,
    requested_position: i64, requested_revision: i64,
) -> (accepted: bool)
    ensures accepted == (rows == 1 && !seen && position == Some(requested_position)
        && revision == Some(requested_revision)),
{
    exact_transaction_row(rows, seen, position, revision, requested_position, requested_revision)
}

}
