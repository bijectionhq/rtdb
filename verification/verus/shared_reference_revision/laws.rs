use crate::production::{advance_revision, may_publish_fork};
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

}
