use crate::production::base4_bucket;
use vstd::prelude::*;

verus! {

// LAW COMPACT-1-FINAL-IID-DIGIT.
pub fn every_iid_digit_can_be_selected(iid: &[u8; 16]) {
    let bucket = base4_bucket(iid, 63);
    assert(bucket is Some);
}

// LAW COMPACT-1-BUCKET-COVERAGE.
pub fn a_high_leading_byte_cannot_belong_to_bucket_zero(iid: &[u8; 16])
    requires iid@[0] >= 64,
{
    let bucket = base4_bucket(iid, 0);
    assert(bucket is Some);
    assert(bucket != Some(0u8));
}

}
