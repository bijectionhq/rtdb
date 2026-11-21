use crate::production::admits;
use vstd::prelude::*;

verus! {

// LAW RETAIN-ADMIT.
pub fn matching_open_owner_is_admitted(epoch: i32) {
    let admitted = admits(true, true, true, epoch, epoch);
    assert(admitted);
}

// LAW RETAIN-SERVING.
pub fn a_nonserving_deployment_refuses(epoch: i32) {
    let admitted = admits(false, true, true, epoch, epoch);
    assert(!admitted);
}

// LAW RETAIN-OWNER.
pub fn a_closed_owner_refuses(epoch: i32) {
    let admitted = admits(true, false, true, epoch, epoch);
    assert(!admitted);
}

// LAW RETAIN-RESIDENCE.
pub fn a_different_residence_refuses(epoch: i32) {
    let admitted = admits(true, true, false, epoch, epoch);
    assert(!admitted);
}

// LAW RETAIN-JOURNAL.
pub fn a_different_journal_refuses(current: i32, opened: i32)
    requires current != opened,
{
    let admitted = admits(true, true, true, current, opened);
    assert(!admitted);
}

}
