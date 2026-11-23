#![allow(unexpected_cfgs)]

use std::cmp::Ordering;

use crate::leaf_merge_step::compare_row_keys;

#[cfg(verus_keep_ghost)]
use crate::leaf_merge_step::{
    declined_row, iid_rank, iid_rank_exposes_the_row_key, ordered, record_iid_error, IidAt, RowRank,
};
#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg(verus_keep_ghost)]
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

pub(crate) closed spec fn rejected_inversion<C, E>(
    rows: Seq<u32>, rank: RowRank, order_error: &C, error: E,
) -> bool
where C: FnOnce() -> E
{
    order_error.ensures((), error)
        && exists |j: int| 0 < j < rows.len()
            && ordered(rows.take(j), rank)
            && rank(#[trigger] rows[j - 1]) > rank(#[trigger] rows[j])
}

proof fn extend_ordered_prefix(rows: Seq<u32>, next: int, rank: RowRank)
    requires
        0 < next < rows.len(),
        ordered(rows.take(next), rank),
        rank(rows[next - 1]) <= rank(rows[next]),
    ensures ordered(rows.take(next + 1), rank),
{
    let prefix = rows.take(next);
    let extended = rows.take(next + 1);
    assert forall |i: int, j: int| 0 <= i < j < extended.len() implies
        rank(#[trigger] extended[i]) <= rank(#[trigger] extended[j]) by {
        if j < next {
            assert(rank(prefix[i]) <= rank(prefix[j]));
        } else if i < next - 1 {
            assert(rank(prefix[i]) <= rank(prefix[next - 1]));
        }
    }
}
}

#[cfg_attr(verus_keep_ghost, verus_spec(result =>
    with Ghost(iid_at): Ghost<IidAt>
    requires
        forall |row: u32| #[trigger] read_iid.requires((row,)),
        forall |row: u32, found: Result<[u8; 16], E>|
            #[trigger] read_iid.ensures((row,), found) && found is Ok ==> found->Ok_0@ == iid_at(row),
        order_error.requires(()),
    ensures
        result is Ok ==> ordered(rows@, iid_rank(iid_at)),
        result is Err ==> declined_row(rows@, Seq::empty(), &read_iid, result->Err_0)
            || rejected_inversion(rows@, iid_rank(iid_at), &order_error, result->Err_0),
))]
pub(crate) fn validate_persisted_order<F, C, E>(
    rows: &[u32], read_iid: F, order_error: C,
) -> Result<(), E>
where
    F: Fn(u32) -> Result<[u8; 16], E>,
    C: FnOnce() -> E,
{
    if rows.len() < 2 {
        return Ok(());
    }
    #[cfg(verus_keep_ghost)]
    proof_decl! { let ghost rank = iid_rank(iid_at); }
    let mut next = 1;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            1 <= next <= rows.len(),
            rank == iid_rank(iid_at),
            ordered(rows@.take(next as int), rank),
            forall |row: u32| #[trigger] read_iid.requires((row,)),
            forall |row: u32, found: Result<[u8; 16], E>|
                #[trigger] read_iid.ensures((row,), found) && found is Ok ==> found->Ok_0@ == iid_at(row),
            order_error.requires(()),
        decreases rows.len() - next,
    ))]
    while next < rows.len() {
        let left = rows[next - 1];
        let right = rows[next];
        let seen_left = read_iid(left);
        let left_iid = match seen_left {
            Ok(iid) => iid,
            Err(error) => {
                #[cfg(verus_keep_ghost)]
                proof! {
                    assert(rows@.contains(left));
                    assert(read_iid.ensures((left,), Err(error)));
                    record_iid_error(rows@, Seq::empty(), &read_iid, left, error);
                }
                return Err(error);
            }
        };
        let seen_right = read_iid(right);
        let right_iid = match seen_right {
            Ok(iid) => iid,
            Err(error) => {
                #[cfg(verus_keep_ghost)]
                proof! {
                    assert(rows@.contains(right));
                    assert(read_iid.ensures((right,), Err(error)));
                    record_iid_error(rows@, Seq::empty(), &read_iid, right, error);
                }
                return Err(error);
            }
        };
        let compared = compare_row_keys(&left_iid, left, &right_iid, right);
        #[cfg(verus_keep_ghost)]
        proof! {
            assert(read_iid.ensures((left,), Ok(left_iid)));
            assert(read_iid.ensures((right,), Ok(right_iid)));
            iid_rank_exposes_the_row_key(iid_at, left);
            iid_rank_exposes_the_row_key(iid_at, right);
            crate::laws::rank_is_unsigned_iid_then_descending_index(left_iid@, right_iid@, left, right);
            assert((compared == Ordering::Greater) == (rank(left) > rank(right)));
        }
        if let Ordering::Greater = compared {
            let error = order_error();
            #[cfg(verus_keep_ghost)]
            proof! { assert(rejected_inversion(rows@, rank, &order_error, error)); }
            return Err(error);
        }
        #[cfg(verus_keep_ghost)]
        proof! { extend_ordered_prefix(rows@, next as int, rank); }
        next += 1;
    }
    #[cfg(verus_keep_ghost)]
    proof! { assert(rows@.take(next as int) =~= rows@); }
    Ok(())
}
