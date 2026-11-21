#![allow(unexpected_cfgs)]

use std::cmp::Ordering;

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::multiset::Multiset;

#[cfg(verus_keep_ghost)]
verus! {
pub(crate) type RowRank = spec_fn(u32) -> int;
pub(crate) type IidAt = spec_fn(u32) -> Seq<u8>;

#[verifier::opaque]
pub(crate) closed spec fn iid_rank(iid_at: IidAt) -> RowRank {
    |row| crate::laws::row_rank(iid_at(row), row)
}

pub(crate) proof fn iid_rank_exposes_the_row_key(iid_at: IidAt, row: u32)
    ensures iid_rank(iid_at)(row) == crate::laws::row_rank(iid_at(row), row),
{
    reveal(iid_rank);
}

pub(crate) closed spec fn declined_row<F, E>(
    left: Seq<u32>, right: Seq<u32>, reader: &F, error: E,
) -> bool
where F: Fn(u32) -> Result<[u8; 16], E>
{
    exists |row: u32| (left.contains(row) || right.contains(row))
        && #[trigger] reader.ensures((row,), Err(error))
}

pub(crate) proof fn record_iid_error<F, E>(left: Seq<u32>, right: Seq<u32>, reader: &F, row: u32, error: E)
where F: Fn(u32) -> Result<[u8; 16], E>
requires
    left.contains(row) || right.contains(row),
    reader.ensures((row,), Err(error)),
ensures declined_row(left, right, reader, error),
{
}

pub(crate) proof fn widen_iid_error<F, E>(
    left: Seq<u32>, right: Seq<u32>, allowed: Seq<u32>, reader: &F, error: E,
)
where F: Fn(u32) -> Result<[u8; 16], E>
requires
    declined_row(left, right, reader, error),
    forall |row: u32| #![trigger left.contains(row)] #![trigger right.contains(row)]
        (left.contains(row) || right.contains(row))
        ==> allowed.contains(row),
ensures declined_row(allowed, Seq::empty(), reader, error),
{
    let row = choose |row: u32| (left.contains(row) || right.contains(row))
        && #[trigger] reader.ensures((row,), Err(error));
    assert(allowed.contains(row));
}

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

spec fn total_bag(prefix: Seq<u32>, left: Seq<u32>, right: Seq<u32>) -> Multiset<u32> {
    prefix.to_multiset().add(left.to_multiset()).add(right.to_multiset())
}

proof fn transfer_head(prefix: Seq<u32>, chosen: Seq<u32>, other: Seq<u32>)
    requires chosen.len() > 0,
    ensures
        total_bag(prefix.push(chosen[0]), chosen.drop_first(), other)
            == total_bag(prefix, chosen, other),
        total_bag(prefix.push(chosen[0]), other, chosen.drop_first())
            == total_bag(prefix, other, chosen),
{
    broadcast use vstd::multiset::group_multiset_axioms;

    let head = seq![chosen[0]];
    assert(prefix.push(chosen[0]) =~= prefix + head);
    assert(chosen =~= head + chosen.drop_first());
    vstd::seq_lib::lemma_multiset_commutative(prefix, head);
    vstd::seq_lib::lemma_multiset_commutative(head, chosen.drop_first());
    assert(total_bag(prefix.push(chosen[0]), chosen.drop_first(), other)
        =~= total_bag(prefix, chosen, other));
    assert(total_bag(prefix.push(chosen[0]), other, chosen.drop_first())
        =~= total_bag(prefix, other, chosen));
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
    with Ghost(iid_at): Ghost<IidAt>
    requires
        left@.len() + right@.len() <= usize::MAX,
        ordered(left@, iid_rank(iid_at)),
        ordered(right@, iid_rank(iid_at)),
        forall |row: u32| #[trigger] read_iid.requires((row,)),
        forall |row: u32, found: Result<[u8; 16], E>|
            #[trigger] read_iid.ensures((row,), found) && found is Ok ==> found->Ok_0@ == iid_at(row),
    ensures
        result is Ok ==> ordered(result->Ok_0@, iid_rank(iid_at)) &&
            result->Ok_0@.to_multiset() == (left@ + right@).to_multiset(),
        result is Err ==> declined_row(left@, right@, &read_iid, result->Err_0),
))]
pub(crate) fn merge_sorted_rows<F, E>(left: &[u32], right: &[u32], read_iid: F) -> Result<Vec<u32>, E>
where
    F: Fn(u32) -> Result<[u8; 16], E>,
{
    #[cfg(verus_keep_ghost)]
    proof_decl! {
        let ghost rank = iid_rank(iid_at);
    }
    let mut merged = Vec::with_capacity(left.len() + right.len());
    let mut li = 0;
    let mut ri = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            rank == iid_rank(iid_at),
            li <= left@.len(),
            ri <= right@.len(),
            ordered(left@, rank),
            ordered(right@, rank),
            ordered(merged@, rank),
            before(merged@, left@.skip(li as int), rank),
            before(merged@, right@.skip(ri as int), rank),
            total_bag(merged@, left@.skip(li as int), right@.skip(ri as int))
                == left@.to_multiset().add(right@.to_multiset()),
            forall |row: u32| #[trigger] read_iid.requires((row,)),
            forall |row: u32, found: Result<[u8; 16], E>|
                #[trigger] read_iid.ensures((row,), found) && found is Ok ==> found->Ok_0@ == iid_at(row),
        decreases (left@.len() - li) + (right@.len() - ri),
    ))]
    while li < left.len() && ri < right.len() {
        let left_row = left[li];
        let right_row = right[ri];
        let read_left = read_iid(left_row);
        #[cfg(verus_keep_ghost)]
        proof! {
            assert(read_iid.ensures((left_row,), read_left));
            assert(left@.contains(left_row));
        }
        let left_iid = match read_left {
            Ok(iid) => iid,
            Err(error) => {
                #[cfg(verus_keep_ghost)]
                proof! {
                    assert(read_iid.ensures((left_row,), Err(error)));
                    assert(declined_row(left@, right@, &read_iid, error));
                }
                return Err(error);
            }
        };
        let read_right = read_iid(right_row);
        #[cfg(verus_keep_ghost)]
        proof! {
            assert(read_iid.ensures((right_row,), read_right));
            assert(right@.contains(right_row));
        }
        let right_iid = match read_right {
            Ok(iid) => iid,
            Err(error) => {
                #[cfg(verus_keep_ghost)]
                proof! {
                    assert(read_iid.ensures((right_row,), Err(error)));
                    assert(declined_row(left@, right@, &read_iid, error));
                }
                return Err(error);
            }
        };
        let order = compare_row_keys(&left_iid, left_row, &right_iid, right_row);
        #[cfg(verus_keep_ghost)]
        proof_decl! {
            let ghost l = left@.skip(li as int);
            let ghost r = right@.skip(ri as int);
            let ghost old_prefix = merged@;
        }
        #[cfg(verus_keep_ghost)]
        proof! {
            assert(read_left == Ok(left_iid));
            assert(read_right == Ok(right_iid));
            assert(left_iid@ == iid_at(left_row));
            assert(right_iid@ == iid_at(right_row));
            assert(l[0] == left_row);
            assert(r[0] == right_row);
            iid_rank_exposes_the_row_key(iid_at, left_row);
            iid_rank_exposes_the_row_key(iid_at, right_row);
            assert(rank(left_row) == crate::laws::row_rank(left_iid@, left_row));
            assert(rank(right_row) == crate::laws::row_rank(right_iid@, right_row));
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
                assert(merged@ =~= old_prefix.push(l[0]));
                assert(left@.skip(li as int) =~= l.drop_first());
                transfer_head(old_prefix, l, r);
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
                assert(merged@ =~= old_prefix.push(r[0]));
                assert(right@.skip(ri as int) =~= r.drop_first());
                transfer_head(old_prefix, r, l);
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
        vstd::seq_lib::lemma_multiset_commutative(prefix, l);
        vstd::seq_lib::lemma_multiset_commutative(prefix + l, r);
        vstd::seq_lib::lemma_multiset_commutative(left@, right@);
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
        let merged = merge_sorted_rows(&[9, 7, 7, 5], &[10, 7, 3, 1], |_| {
            Ok::<_, Infallible>([0; 16])
        });
        assert_eq!(merged.unwrap(), [10, 9, 7, 7, 7, 5, 3, 1]);
    }

    #[test]
    fn merge_stops_on_the_first_failed_iid_read() {
        let visited = RefCell::new(Vec::new());
        let merged = merge_sorted_rows(&[2, 4, 7], &[1, 5, 8], |row| {
            visited.borrow_mut().push(row);
            if row == 5 {
                return Err(row);
            }
            let mut iid = [0; 16];
            iid[..4].copy_from_slice(&row.to_be_bytes());
            Ok(iid)
        });
        assert_eq!(merged, Err(5));
        assert_eq!(visited.into_inner(), [2, 1, 2, 5]);
    }
}
