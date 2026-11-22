use crate::production::{may_prune_manifest, next_manifest_sequence};
use vstd::prelude::*;

verus! {

// LAW REFERENCE-MANIFEST-1.
pub fn a_manifest_successor_is_strict(current: u64) {
    match next_manifest_sequence(current) {
        Some(next) => assert(next > current),
        None => assert(current == u64::MAX),
    }
}

// LAW REFERENCE-MANIFEST-2.
pub fn a_late_prune_preserves_the_current_head(slot: u64, confirmed: u64, current: u64)
    requires confirmed <= current,
{
    let prunes = may_prune_manifest(slot, confirmed);
    assert(prunes ==> slot < current);
}

}
