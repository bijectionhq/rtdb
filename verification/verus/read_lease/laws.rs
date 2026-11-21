use crate::production::{collection_waited, lease_admits, lease_deadline};
use vstd::prelude::*;

verus! {

// LAW LEASE-READ-PRECEDES-DELETION.
// Real time is the collector's clock; the reader's clock is within `skew` of it.
// The reader read its anchor before the retirement of the file was published, and the collector observed that retirement after it was published.
pub fn an_admitted_read_finishes_before_collection(
    anchor: i64, lease: i64, margin: i64, skew: i64, io_bound: i64,
    first_observed: i64, reader_start: i64, start: i64, finish: i64, deletion: i64,
)
    requires
        0 <= skew, 0 <= io_bound, 2 * skew + io_bound <= margin, margin < lease,
        anchor as int - skew as int <= first_observed as int,
        start as int - skew as int <= reader_start as int <= start as int + skew as int,
        start <= finish, finish as int <= start as int + io_bound as int,
{
    if let Some(deadline) = lease_deadline(anchor, lease, margin) {
        if lease_admits(reader_start, deadline) && collection_waited(first_observed, lease, deletion) {
            assert(finish < deletion);
        }
    }
}

// LAW LEASE-EXPIRY-REFUSES.
pub fn a_read_at_its_deadline_is_refused(deadline: i64) {
    let admitted = lease_admits(deadline, deadline);
    assert(!admitted);
}

}
