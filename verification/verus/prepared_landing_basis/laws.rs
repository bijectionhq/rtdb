use crate::prepared_landing_basis::{generation_matches, LandingBasis};
use vstd::prelude::*;

verus! {

/// Both prepared outcomes can be installed only over the original physical
/// and transaction basis with the original assigned clock.
pub(crate) fn switch_preserves_prepared_basis(
    prepared: LandingBasis,
    installed: LandingBasis,
    prepared_time: i64,
    assigned: i64,
) -> (admitted: bool)
    ensures
        admitted == (prepared == installed && prepared_time == assigned),
        admitted ==> prepared.predecessor_id == installed.predecessor_id
            && prepared.predecessor_time == installed.predecessor_time
            && prepared.replica_epoch == installed.replica_epoch
            && prepared.replica_through == installed.replica_through,
{
    generation_matches(prepared, installed, prepared_time, assigned)
}

}
