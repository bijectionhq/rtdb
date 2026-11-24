use ordered_logkeeper::ordered::{Grant, Placement};
use uuid::Uuid;

/// The payload key survives writer replacement and changes only on rotation.
#[derive(Clone, Copy)]
pub struct JournalKey {
    pub(super) deployment: Uuid,
    pub(super) incarnation: Uuid,
    pub(super) generation: u64,
}

impl JournalKey {
    pub fn new(placement: Placement, generation: u64) -> Self {
        journal_key(placement, generation)
    }

    pub(crate) fn for_grant(placement: Placement, grant: Grant) -> Self {
        journal_key_for_grant(placement, grant)
    }
}

fn journal_key(placement: Placement, generation: u64) -> JournalKey {
    JournalKey {
        deployment: placement.deployment,
        incarnation: placement.incarnation,
        generation,
    }
}

pub(crate) fn journal_key_for_grant(placement: Placement, grant: Grant) -> JournalKey {
    journal_key(placement, grant.key_generation)
}
