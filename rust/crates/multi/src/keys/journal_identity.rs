#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use crate::{Grant, Placement, Uuid};
#[cfg(verus_keep_ghost)]
use vstd::prelude::*;
#[cfg(not(verus_keep_ghost))]
use ordered_logkeeper::ordered::{Grant, Placement};
#[cfg(not(verus_keep_ghost))]
use uuid::Uuid;

/// The payload key survives writer replacement and changes only on rotation.
#[derive(Clone, Copy)]
pub struct JournalKey {
    pub(super) deployment: Uuid,
    pub(super) incarnation: Uuid,
    pub(super) generation: u64,
}

#[cfg(not(verus_keep_ghost))]
impl JournalKey {
    pub fn new(placement: Placement, generation: u64) -> Self {
        journal_key(placement, generation)
    }

    pub(crate) fn for_grant(placement: Placement, grant: Grant) -> Self {
        journal_key_for_grant(placement, grant)
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(key =>
    ensures key.deployment == placement.deployment, key.incarnation == placement.incarnation,
        key.generation == generation,
))]
fn journal_key(placement: Placement, generation: u64) -> JournalKey {
    JournalKey {
        deployment: placement.deployment,
        incarnation: placement.incarnation,
        generation,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(key =>
    ensures key.deployment == placement.deployment, key.incarnation == placement.incarnation,
        key.generation == grant.key_generation,
))]
pub(crate) fn journal_key_for_grant(placement: Placement, grant: Grant) -> JournalKey {
    journal_key(placement, grant.key_generation)
}
