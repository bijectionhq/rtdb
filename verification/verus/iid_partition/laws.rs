use crate::partition_step::{base4_bucket, compare_base4_path, iid_digit};
use std::cmp::Ordering;
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

pub fn child_membership_extends_parent_membership(iid: &[u8; 16], parent: &[u8], child: &[u8])
    requires
        parent@.len() < 64,
        child@.len() == parent@.len() + 1,
        child@.take(parent@.len() as int) == parent@,
{
    let parent_order = compare_base4_path(iid, parent);
    let child_order = compare_base4_path(iid, child);
    let bucket = base4_bucket(iid, parent.len()).unwrap();
    proof {
        let depth = parent@.len() as int;
        assert(iid_digit(iid@, depth) == bucket as int);
        if child_order == Ordering::Equal {
            assert(iid_digit(iid@, depth) == child@[depth] as int);
            if parent_order != Ordering::Equal {
                let first = choose |i: int| 0 <= i < depth
                    && iid_digit(iid@, i) != parent@[i] as int;
                assert(child@[first] == parent@[first]);
                assert(iid_digit(iid@, first) == child@[first] as int);
                assert(false);
            }
        } else if parent_order == Ordering::Equal {
            let first = choose |i: int| 0 <= i < child@.len()
                && iid_digit(iid@, i) != child@[i] as int;
            if first < depth {
                assert(child@[first] == parent@[first]);
                assert(iid_digit(iid@, first) == parent@[first] as int);
                assert(false);
            }
            assert(first == depth);
            assert(child@[depth] != bucket);
        }
        assert((child_order == Ordering::Equal)
            == (parent_order == Ordering::Equal && child@[depth] == bucket));
    }
}

}
