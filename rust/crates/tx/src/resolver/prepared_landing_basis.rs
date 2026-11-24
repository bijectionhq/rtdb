#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::{laws_eq::obeys_concrete_eq, prelude::*};

/// The complete data and catalog basis of a prepared generation.
#[derive(Clone, Copy)]
pub(super) struct LandingBasis {
    pub(super) predecessor_id: i64,
    pub(super) predecessor_time: i64,
    pub(super) block: i64,
    pub(super) storage_version: i32,
    pub(super) storage_epoch: i32,
    pub(super) replica_epoch: i32,
    pub(super) replica_through: i64,
}

#[cfg_attr(verus_keep_ghost, verus_spec(admitted => ensures
    admitted == (prepared == installed && prepared_key == assigned_key),
))]
pub(super) fn generation_matches(
    prepared: LandingBasis,
    installed: LandingBasis,
    prepared_key: (i64, i64),
    assigned_key: (i64, i64),
) -> bool {
    prepared.predecessor_id == installed.predecessor_id
        && prepared.predecessor_time == installed.predecessor_time
        && prepared.block == installed.block
        && prepared.storage_version == installed.storage_version
        && prepared.storage_epoch == installed.storage_epoch
        && prepared.replica_epoch == installed.replica_epoch
        && prepared.replica_through == installed.replica_through
        && prepared_key.0 == assigned_key.0
        && prepared_key.1 == assigned_key.1
}

#[cfg_attr(verus_keep_ghost, verus_spec(admitted => requires
    obeys_concrete_eq::<Clock>(), obeys_concrete_eq::<Scope>(),
    ensures admitted == (
        clock == Some(prepared_clock) && scope == Some(prepared_scope)
            && receiver == Some(prepared_scope)
    ),
))]
pub(super) fn prepared_authority_matches<Clock: Eq, Scope: Eq>(
    prepared_clock: Clock, prepared_scope: Scope,
    clock: Option<Clock>, scope: Option<Scope>, receiver: Option<Scope>,
) -> bool {
    #[cfg(verus_keep_ghost)]
    proof! { reveal(obeys_concrete_eq); }
    matches!(clock, Some(slot) if Clock::eq(&slot, &prepared_clock))
        && matches!(scope, Some(scope) if Scope::eq(&scope, &prepared_scope))
        && matches!(receiver, Some(scope) if Scope::eq(&scope, &prepared_scope))
}

#[cfg_attr(verus_keep_ghost, verus_spec(selected => ensures
    selected == if prepared == installed && prepared_key == assigned_key {
        Some((writes, refused))
    } else {
        None
    },
))]
pub(super) fn select_generation<Writes>(
    prepared: LandingBasis,
    installed: LandingBasis,
    prepared_key: (i64, i64),
    assigned_key: (i64, i64),
    writes: Writes,
    refused: bool,
) -> Option<(Writes, bool)> {
    if generation_matches(prepared, installed, prepared_key, assigned_key) {
        Some((writes, refused))
    } else {
        None
    }
}
