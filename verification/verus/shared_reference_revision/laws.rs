use crate::production::{advance_revision, may_publish_fork, may_publish_reference, may_settle_reference_disposal, may_settle_exact_owned};
use vstd::prelude::*;

verus! {

// LAW REFERENCE-REVISION-1.
pub fn stale_parent_cannot_publish(parent: u64, expected: u64)
    requires parent != expected,
{
    let accepted = may_publish_fork(parent, expected, true, true);
    assert(!accepted);
}

// LAW REFERENCE-REVISION-2.
pub fn foreign_lineage_cannot_publish(parent: u64) {
    let accepted = may_publish_fork(parent, parent, true, false);
    assert(!accepted);
}

// LAW REFERENCE-REVISION-3.
pub fn refused_parent_cannot_publish(parent: u64) {
    let accepted = may_publish_fork(parent, parent, false, true);
    assert(!accepted);
}

// LAW REFERENCE-REVISION-4.
pub fn an_advance_is_strict_and_does_not_wrap(current: u64) {
    let next = advance_revision(current);
    match next {
        Some(next) => assert(next > current),
        None => assert(current == u64::MAX),
    }
}

// LAW REFERENCE-REVISION-5.
pub fn a_closed_lineage_refuses_its_current_preparation(current: u64) {
    let accepted = may_publish_reference(current, false, Some(current));
    assert(!accepted);
}

// LAW REFERENCE-REVISION-6.
pub fn an_open_lineage_refuses_a_stale_preparation(current: u64, prepared_at: Option<u64>)
    requires prepared_at != Some(current),
{
    let accepted = may_publish_reference(current, true, prepared_at);
    assert(!accepted);
}

// LAW REFERENCE-REVISION-7.
pub fn settlement_requires_the_exact_revision_and_residence(current: u64, expected: u64, scope_matches: bool) {
    let accepted = may_settle_reference_disposal(current, expected, scope_matches);
    assert(accepted ==> current == expected && scope_matches);
}

// LAW REFERENCE-REVISION-8.
pub fn exact_settlement_consumes_the_authorized_owner_fence(authorized: bool, same_owner: bool, matching_fence: bool) {
    let allowed = may_settle_exact_owned(authorized, same_owner, matching_fence);
    assert(allowed ==> authorized && same_owner && matching_fence);
}

}
