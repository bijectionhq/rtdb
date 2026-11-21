use crate::production::{ordered, RowRank};
use vstd::prelude::*;
use vstd::relations::{sorted_by, total_ordering};
use vstd::seq_lib::lemma_sorted_unique;

verus! {

pub(crate) proof fn multiplicities_determine_the_ordered_row_sequence(
    actual: Seq<u32>, expected: Seq<u32>, rank: RowRank,
)
    requires
        ordered(actual, rank),
        ordered(expected, rank),
        actual.to_multiset() == expected.to_multiset(),
        forall |a: u32, b: u32| #![trigger rank(a), rank(b)] rank(a) == rank(b) ==> a == b,
    ensures actual == expected,
{
    let precedes = |a: u32, b: u32| rank(a) <= rank(b);
    assert(total_ordering(precedes));
    assert(sorted_by(actual, precedes));
    assert(sorted_by(expected, precedes));
    lemma_sorted_unique(actual, expected, precedes);
}

spec fn pow256(bytes: nat) -> int
    decreases bytes,
{
    if bytes == 0 { 1 } else { 256 * pow256((bytes - 1) as nat) }
}

spec fn unsigned_bytes(bytes: Seq<u8>) -> int
    decreases bytes.len(),
{
    if bytes.len() == 0 {
        0
    } else {
        (bytes[0] as int) * pow256((bytes.len() - 1) as nat) + unsigned_bytes(bytes.drop_first())
    }
}

spec fn lex_less(left: Seq<u8>, right: Seq<u8>) -> bool
    decreases left.len(),
{
    if left.len() == 0 {
        right.len() > 0
    } else if right.len() == 0 {
        false
    } else {
        left[0] < right[0] || left[0] == right[0] && lex_less(left.drop_first(), right.drop_first())
    }
}

proof fn unsigned_bytes_fits(bytes: Seq<u8>)
    ensures 0 <= unsigned_bytes(bytes) < pow256(bytes.len()),
    decreases bytes.len(),
{
    if bytes.len() > 0 {
        unsigned_bytes_fits(bytes.drop_first());
        let cap = pow256((bytes.len() - 1) as nat);
        let rest = unsigned_bytes(bytes.drop_first());
        let head = bytes[0] as int;
        assert(bytes.drop_first().len() == bytes.len() - 1);
        assert(pow256(bytes.len()) == 256 * cap);
        assert(unsigned_bytes(bytes) == head * cap + rest);
        assert(0 <= head * cap + rest < 256 * cap) by (nonlinear_arith)
            requires 0 <= head < 256, 0 <= rest < cap;
    }
}

proof fn unsigned_bytes_preserves_order(left: Seq<u8>, right: Seq<u8>)
    requires left.len() == right.len(),
    ensures
        (unsigned_bytes(left) < unsigned_bytes(right)) == lex_less(left, right),
        (unsigned_bytes(left) == unsigned_bytes(right)) == (left == right),
    decreases left.len(),
{
    if left.len() == 0 {
        assert(left =~= right);
    } else {
        let cap = pow256((left.len() - 1) as nat);
        let left_tail = left.drop_first();
        let right_tail = right.drop_first();
        let left_head = left[0] as int;
        let right_head = right[0] as int;
        let left_value = unsigned_bytes(left_tail);
        let right_value = unsigned_bytes(right_tail);
        unsigned_bytes_fits(left_tail);
        unsigned_bytes_fits(right_tail);
        assert(left_tail.len() == right_tail.len() == left.len() - 1);
        assert(unsigned_bytes(left) == left_head * cap + left_value);
        assert(unsigned_bytes(right) == right_head * cap + right_value);
        if left_head == right_head {
            unsigned_bytes_preserves_order(left_tail, right_tail);
            assert(left == right <==> left_tail == right_tail) by {
                if left_tail == right_tail {
                    assert forall |i: int| 0 <= i < left.len() implies
                        #[trigger] left[i] == #[trigger] right[i] by {
                        if i > 0 {
                            assert(left_tail[i - 1] == left[i]);
                            assert(right_tail[i - 1] == right[i]);
                        }
                    }
                    assert(left =~= right);
                }
            }
        } else if left_head < right_head {
            assert(left_head * cap + left_value < right_head * cap + right_value) by (nonlinear_arith)
                requires 0 <= left_value < cap, 0 <= right_value < cap, left_head < right_head;
        } else {
            assert(right_head * cap + right_value < left_head * cap + left_value) by (nonlinear_arith)
                requires 0 <= left_value < cap, 0 <= right_value < cap, right_head < left_head;
        }
    }
}

spec fn row_rank(iid: Seq<u8>, index: u32) -> int {
    unsigned_bytes(iid) * (u32::MAX as int + 1) + (u32::MAX as int - index as int)
}

proof fn rank_is_unsigned_iid_then_descending_index(left: Seq<u8>, right: Seq<u8>, left_index: u32, right_index: u32)
    requires left.len() == right.len(),
    ensures
        (row_rank(left, left_index) < row_rank(right, right_index)) == (lex_less(left, right) || left == right && left_index > right_index),
        (row_rank(left, left_index) == row_rank(right, right_index)) == (left == right && left_index == right_index),
{
    unsigned_bytes_preserves_order(left, right);
    if left != right {
        unsigned_bytes_preserves_order(right, left);
    }
}

}
