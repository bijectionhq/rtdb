#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

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
    admitted == (prepared == installed && prepared_position == assigned_position),
))]
pub(super) fn generation_matches(
    prepared: LandingBasis,
    installed: LandingBasis,
    prepared_position: i64,
    assigned_position: i64,
) -> bool {
    prepared.predecessor_id == installed.predecessor_id
        && prepared.predecessor_time == installed.predecessor_time
        && prepared.block == installed.block
        && prepared.storage_version == installed.storage_version
        && prepared.storage_epoch == installed.storage_epoch
        && prepared.replica_epoch == installed.replica_epoch
        && prepared.replica_through == installed.replica_through
        && prepared_position == assigned_position
}
