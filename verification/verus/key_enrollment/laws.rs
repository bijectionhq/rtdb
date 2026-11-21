use crate::production::may_enroll;
use vstd::prelude::*;

verus! {

// LAW KEY-ADMIT.
pub fn current_pristine_writer_enrolls() {
    let allowed = may_enroll(true, true, false);
    assert(allowed);
}

// LAW KEY-WRITER.
pub fn a_nonwriter_cannot_enroll() {
    let allowed = may_enroll(false, true, false);
    assert(!allowed);
}

// LAW KEY-RESIDENCE.
pub fn a_derived_residence_cannot_enroll() {
    let allowed = may_enroll(true, false, false);
    assert(!allowed);
}

// LAW KEY-HISTORY.
pub fn an_existing_history_prevents_enrollment() {
    let allowed = may_enroll(true, true, true);
    assert(!allowed);
}

}
