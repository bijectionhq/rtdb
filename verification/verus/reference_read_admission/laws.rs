use crate::production::may_admit_reference_read;
use vstd::prelude::*;

verus! {

// LAW REFERENCE-READ-1.
pub fn a_fenced_owner_rejects_even_an_exact_reference() {
    let admitted = may_admit_reference_read(false, true, true);
    assert(!admitted);
}

// LAW REFERENCE-READ-2.
pub fn a_reference_requires_the_physical_residence_and_namespace(open: bool, same_residence: bool, same_media: bool) {
    let admitted = may_admit_reference_read(open, same_residence, same_media);
    assert(admitted ==> same_residence && same_media);
}

// LAW REFERENCE-READ-3.
pub fn an_exact_reference_to_an_open_owner_is_admitted() {
    let admitted = may_admit_reference_read(true, true, true);
    assert(admitted);
}

}
