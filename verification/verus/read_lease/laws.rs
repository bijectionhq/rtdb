use crate::production::lease_admits;
use vstd::prelude::*;

verus! {

// LAW LEASE-EXPIRY-REFUSES.
pub fn a_read_at_its_deadline_is_refused(deadline: i64) {
    let admitted = lease_admits(deadline, deadline);
    assert(!admitted);
}

}
