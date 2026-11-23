use crate::production::{admits, admits_after_floor};
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

// LAW RETAIN-FLOOR-OLDER.
pub fn an_older_capture_refuses(cut: i64, floor: i64)
    requires cut < floor,
{
    let admitted = admits_after_floor(Some(cut), Some(floor));
    assert(!admitted);
}

// LAW RETAIN-FLOOR-OPAQUE.
pub fn an_opaque_capture_refuses_when_a_floor_exists(floor: i64) {
    let admitted = admits_after_floor(None, Some(floor));
    assert(!admitted);
}

// LAW RETAIN-FLOOR-AT.
pub fn a_capture_at_or_after_the_floor_is_admitted(cut: i64, floor: i64)
    requires cut >= floor,
{
    let admitted = admits_after_floor(Some(cut), Some(floor));
    assert(admitted);
}

// LAW RETAIN-FLOOR-EMPTY.
pub fn every_capture_is_admitted_without_a_floor(cut: Option<i64>) {
    let admitted = admits_after_floor(cut, None);
    assert(admitted);
}

}
