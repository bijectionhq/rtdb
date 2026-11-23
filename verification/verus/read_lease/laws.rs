use crate::production::{collection_waited, lease_admits, lease_deadline};
use vstd::prelude::*;

verus! {

// LAW LEASE-READ-PRECEDES-DELETION.
// Each node measures elapsed time on its own monotonic clock; `*_mono` values are such readings and `*_real` values are real time.
// Over any interval of up to one lease, a clock's measured elapsed time differs from the real one by at most `drift`.
// The reader takes its anchor before it reads the replica tip, and the collector first observes the retirement after it was appended above that tip.
pub fn an_admitted_read_finishes_before_collection(
    lease: i64, margin: i64, drift: i64, io_bound: i64,
    anchor_mono: i64, start_mono: i64, first_mono: i64, deletion_mono: i64,
    anchor_real: int, start_real: int, finish_real: int, first_real: int, deletion_real: int,
)
    requires
        0 <= drift, 0 <= io_bound, 2 * drift + io_bound <= margin, margin < lease,
        anchor_real <= first_real,
        anchor_mono <= start_mono, first_mono <= deletion_mono,
        (start_mono as int - anchor_mono as int) < lease as int ==> start_real - anchor_real <= (start_mono as int - anchor_mono as int) + drift as int,
        (deletion_mono as int - first_mono as int) >= lease as int ==> deletion_real - first_real >= lease as int - drift as int,
        start_real <= finish_real <= start_real + io_bound as int,
{
    if let Some(deadline) = lease_deadline(anchor_mono, lease, margin) {
        if lease_admits(start_mono, deadline) && collection_waited(first_mono, lease, deletion_mono) {
            assert(finish_real < deletion_real);
        }
    }
}

// LAW LEASE-EXPIRY-REFUSES.
pub fn a_read_at_its_deadline_is_refused(deadline: i64) {
    let admitted = lease_admits(deadline, deadline);
    assert(!admitted);
}

}
