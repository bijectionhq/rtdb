#![allow(unexpected_cfgs)]

use std::cmp::Ordering;

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg(verus_keep_ghost)]
verus! {
pub(crate) type RowRank = spec_fn(u32) -> int;

broadcast use {vstd::seq_lib::group_seq_properties, vstd::multiset::group_multiset_properties};

pub(crate) open spec fn ordered(rows: Seq<u32>, rank: RowRank) -> bool {
    forall |i: int, j: int| 0 <= i < j < rows.len() ==> rank(#[trigger] rows[i]) <= rank(#[trigger] rows[j])
}

spec fn before(prefix: Seq<u32>, suffix: Seq<u32>, rank: RowRank) -> bool {
    forall |i: int, j: int| 0 <= i < prefix.len() && 0 <= j < suffix.len() ==>
        rank(#[trigger] prefix[i]) <= rank(#[trigger] suffix[j])
}

proof fn append_sorted(prefix: Seq<u32>, suffix: Seq<u32>, rank: RowRank)
    requires ordered(prefix, rank), ordered(suffix, rank), before(prefix, suffix, rank),
    ensures ordered(prefix + suffix, rank),
{
    assert forall |i: int, j: int| 0 <= i < j < (prefix + suffix).len() implies
        rank(#[trigger] (prefix + suffix)[i]) <= rank(#[trigger] (prefix + suffix)[j]) by {}
}

proof fn push_minimum(prefix: Seq<u32>, chosen: Seq<u32>, other: Seq<u32>, rank: RowRank)
    requires
        chosen.len() > 0,
        other.len() > 0,
        ordered(prefix, rank),
        ordered(chosen, rank),
        ordered(other, rank),
        before(prefix, chosen, rank),
        before(prefix, other, rank),
        rank(chosen[0]) <= rank(other[0]),
    ensures
        ordered(prefix.push(chosen[0]), rank),
        before(prefix.push(chosen[0]), chosen.drop_first(), rank),
        before(prefix.push(chosen[0]), other, rank),
{
    let single = seq![chosen[0]];
    assert(before(prefix, single, rank));
    assert(ordered(single, rank));
    append_sorted(prefix, single, rank);
    assert(prefix.push(chosen[0]) =~= prefix + single);
    assert forall |i: int, j: int| 0 <= i < prefix.push(chosen[0]).len() && 0 <= j < chosen.drop_first().len()
        implies rank(#[trigger] prefix.push(chosen[0])[i]) <= rank(#[trigger] chosen.drop_first()[j]) by {
        assert(chosen.drop_first()[j] == chosen[j + 1]);
    }
    assert forall |i: int, j: int| 0 <= i < prefix.push(chosen[0]).len() && 0 <= j < other.len()
        implies rank(#[trigger] prefix.push(chosen[0])[i]) <= rank(#[trigger] other[j]) by {
        if j > 0 {
            assert(rank(other[0]) <= rank(other[j]));
        }
    }
}
}

#[cfg_attr(verus_keep_ghost, verus_spec(result =>
    ensures
        (result == Ordering::Less) ==
            (crate::laws::row_rank(left_iid@, left_row) < crate::laws::row_rank(right_iid@, right_row)),
        (result == Ordering::Equal) == (left_iid@ == right_iid@ && left_row == right_row),
))]
pub(crate) fn compare_row_keys(
    left_iid: &[u8; 16], left_row: u32, right_iid: &[u8; 16], right_row: u32,
) -> Ordering {
    #[cfg(verus_keep_ghost)]
    proof! {
        crate::laws::rank_is_unsigned_iid_then_descending_index(left_iid@, right_iid@, left_row, right_row);
    }
    match left_iid.cmp(right_iid) {
        Ordering::Equal => right_row.cmp(&left_row),
        order => order,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(result =>
    with Ghost(rank): Ghost<RowRank>
    requires
        left@.len() + right@.len() <= usize::MAX,
        ordered(left@, rank),
        ordered(right@, rank),
        forall |l: u32, r: u32| #[trigger] compare.requires((l, r)),
        forall |l: u32, r: u32, result: Result<Ordering, E>|
            #[trigger] compare.ensures((l, r), result) && result is Ok
                ==> (result->Ok_0 == Ordering::Less) == (rank(l) < rank(r)),
    ensures
        result is Ok ==> ordered(result->Ok_0@, rank) &&
            result->Ok_0@.to_multiset() == (left@ + right@).to_multiset(),
))]
pub(crate) fn merge_sorted_rows<F, E>(left: &[u32], right: &[u32], compare: F) -> Result<Vec<u32>, E>
where
    F: Fn(u32, u32) -> Result<Ordering, E>,
{
    let mut merged = Vec::with_capacity(left.len() + right.len());
    let mut li = 0;
    let mut ri = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            li <= left@.len(),
            ri <= right@.len(),
            ordered(left@, rank),
            ordered(right@, rank),
            ordered(merged@, rank),
            before(merged@, left@.skip(li as int), rank),
            before(merged@, right@.skip(ri as int), rank),
            (merged@ + left@.skip(li as int) + right@.skip(ri as int)).to_multiset() ==
                (left@ + right@).to_multiset(),
            forall |l: u32, r: u32| #[trigger] compare.requires((l, r)),
            forall |l: u32, r: u32, result: Result<Ordering, E>|
                #[trigger] compare.ensures((l, r), result) && result is Ok
                    ==> (result->Ok_0 == Ordering::Less) == (rank(l) < rank(r)),
        decreases (left@.len() - li) + (right@.len() - ri),
    ))]
    while li < left.len() && ri < right.len() {
        let compared = compare(left[li], right[ri]);
        #[cfg(verus_keep_ghost)]
        proof! {
            assert(compare.ensures((left@[li as int], right@[ri as int]), compared));
        }
        let order = match compared {
            Ok(order) => order,
            Err(error) => return Err(error),
        };
        #[cfg(verus_keep_ghost)]
        proof_decl! {
            let ghost l = left@.skip(li as int);
            let ghost r = right@.skip(ri as int);
            let ghost old_prefix = merged@;
        }
        #[cfg(verus_keep_ghost)]
        proof! {
            assert((order == Ordering::Less) == (rank(l[0]) < rank(r[0])));
            assert(ordered(l, rank));
            assert(ordered(r, rank));
        }
        if let Ordering::Less = order {
            #[cfg(verus_keep_ghost)]
            proof! {
                push_minimum(old_prefix, l, r, rank);
                assert(l =~= seq![left@[li as int]] + left@.skip(li as int + 1));
            }
            merged.push(left[li]);
            li += 1;
            #[cfg(verus_keep_ghost)]
            proof! {
                assert(merged@ + left@.skip(li as int) + r =~= old_prefix + l + r);
            }
        } else {
            #[cfg(verus_keep_ghost)]
            proof! {
                push_minimum(old_prefix, r, l, rank);
                assert(r =~= seq![right@[ri as int]] + right@.skip(ri as int + 1));
            }
            merged.push(right[ri]);
            ri += 1;
            #[cfg(verus_keep_ghost)]
            proof! {
                let single = seq![r[0]];
                let tail = right@.skip(ri as int);
                vstd::seq_lib::lemma_seq_union_to_multiset_commutative(single, l);
                assert(merged@ + l + tail =~= old_prefix + (single + l) + tail);
                assert(old_prefix + l + r =~= old_prefix + (l + single) + tail);
                vstd::seq_lib::lemma_multiset_commutative(old_prefix, single + l);
                vstd::seq_lib::lemma_multiset_commutative(old_prefix, l + single);
                vstd::seq_lib::lemma_multiset_commutative(old_prefix + (single + l), tail);
                vstd::seq_lib::lemma_multiset_commutative(old_prefix + (l + single), tail);
                assert((merged@ + l + tail).to_multiset()
                    =~= (old_prefix + l + r).to_multiset());
            }
        }
    }
    #[cfg(verus_keep_ghost)]
    proof_decl! {
        let ghost prefix = merged@;
    }
    merged.extend_from_slice(&left[li..]);
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(merged@ =~= prefix + left@.skip(li as int));
        assert(ordered(left@.skip(li as int), rank));
        append_sorted(prefix, left@.skip(li as int), rank);
    }
    merged.extend_from_slice(&right[ri..]);
    #[cfg(verus_keep_ghost)]
    proof! {
        let l = left@.skip(li as int);
        let r = right@.skip(ri as int);
        assert(l.len() == 0 || r.len() == 0);
        assert(ordered(r, rank));
        assert(before(prefix + l, r, rank));
        append_sorted(prefix + l, r, rank);
        assert(merged@ =~= prefix + l + r);
    }
    Ok(merged)
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::convert::Infallible;

    use super::merge_sorted_rows;

    #[test]
    fn merge_counts_every_occurrence_and_keeps_the_remaining_suffix() {
        let merged = merge_sorted_rows(&[9, 7, 7, 5], &[10, 7, 3, 1], |left, right| {
            Ok::<_, Infallible>(right.cmp(&left))
        });
        assert_eq!(merged.unwrap(), [10, 9, 7, 7, 7, 5, 3, 1]);
    }

    #[test]
    fn merge_propagates_a_comparison_error_at_the_current_heads() {
        let compared = RefCell::new(Vec::new());
        let merged = merge_sorted_rows(&[2, 4, 7], &[1, 5, 8], |left, right| {
            compared.borrow_mut().push((left, right));
            if right == 5 { Err(right) } else { Ok(left.cmp(&right)) }
        });
        assert_eq!(merged, Err(5));
        assert_eq!(compared.into_inner(), [(2, 1), (2, 5)]);
    }
}
