use crate::prepared_order::{next_landing_position, same_stage_order, shift_metadata_position};
use crate::prepared_landing_basis::{generation_matches, prepared_authority_matches, select_generation, LandingBasis};
use vstd::{laws_eq::obeys_concrete_eq, prelude::*};

verus! {

/// Both prepared outcomes can be installed only over the original physical
/// and transaction basis at the exact assigned source position and time.
pub(crate) fn switch_preserves_prepared_basis(
    prepared: LandingBasis,
    installed: LandingBasis,
    prepared_key: (i64, i64),
    assigned: (i64, i64),
) -> (admitted: bool)
    ensures
        admitted == (prepared == installed && prepared_key == assigned),
        admitted ==> prepared.predecessor_id == installed.predecessor_id
            && prepared.predecessor_time == installed.predecessor_time
            && prepared.replica_epoch == installed.replica_epoch
            && prepared.replica_through == installed.replica_through,
{
    generation_matches(prepared, installed, prepared_key, assigned)
}

pub(crate) fn select_original_staged_landing<Clock: Eq, Scope: Eq, Writes>(
    prepared: LandingBasis,
    installed: LandingBasis,
    prepared_key: (i64, i64),
    assigned: (i64, i64),
    prepared_clock: Clock,
    prepared_scope: Scope,
    clock: Option<Clock>,
    scope: Option<Scope>,
    receiver: Option<Scope>,
    writes: Writes,
    refused: bool,
) -> (selected: Option<(Writes, bool)>)
    requires obeys_concrete_eq::<Clock>(), obeys_concrete_eq::<Scope>(),
    ensures
        selected == if prepared == installed && prepared_key == assigned
            && clock == Some(prepared_clock) && scope == Some(prepared_scope)
            && receiver == Some(prepared_scope)
        {
            Some((writes, refused))
        } else {
            None
        },
{
    if !prepared_authority_matches(prepared_clock, prepared_scope, clock, scope, receiver) {
        return None;
    }
    select_generation(prepared, installed, prepared_key, assigned, writes, refused)
}

pub(crate) fn apply_one_landing_order() -> (reachable: bool)
    ensures reachable,
{
    let basis = Some((4, 20));
    same_stage_order(basis, basis, 3, 3, (5, 21), (5, 21))
        && next_landing_position(0) == Some(1) && next_landing_position(1) == Some(2)
        && shift_metadata_position(Some((5, 0)), 5, 2) == Some((5, 2))
        && shift_metadata_position(Some((5, 1)), 5, 2) == Some((5, 3))
}

}
