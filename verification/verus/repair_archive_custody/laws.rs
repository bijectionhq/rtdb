use crate::production::{covers_archive, may_read_archive};
use vstd::prelude::*;

verus! {

// LAW DUR-2-ARCHIVE-DESTINATION.
pub fn a_destination_remains_held(destination: u128, repair: Option<u128>) {
    let covered = covers_archive(Some(destination), repair, destination);
    assert(covered);
}

// LAW DUR-2-ARCHIVE-REPAIR.
pub fn a_repair_is_held_independently(repair: u128, destination: Option<u128>) {
    let covered = covers_archive(destination, Some(repair), repair);
    assert(covered);
}

// LAW GC-1-UNRELATED-ARCHIVE.
pub fn a_region_does_not_retain_an_unrelated_archive(destination: u128, repair: u128, archive: u128)
    requires archive != destination, archive != repair,
{
    let covered = covers_archive(Some(destination), Some(repair), archive);
    assert(!covered);
}

// LAW GC-1-CLOSED-ARCHIVE.
pub fn a_closed_region_cannot_authorize_a_read(capture: u128) {
    let covered = covers_archive(None, Some(capture), capture);
    let admitted = may_read_archive(covered, true);
    assert(!admitted);
}

}
