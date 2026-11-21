#![allow(unexpected_cfgs)]

use std::cmp::Ordering;

use crate::leaf_merge_step::compare_row_keys;

#[cfg(verus_keep_ghost)]
use crate::laws::{ordered_by, row_rank};
#[cfg(verus_keep_ghost)]
use crate::leaf_merge_step::{
    declined_row, iid_rank, iid_rank_exposes_the_row_key, ordered, record_iid_error, widen_iid_error,
    IidAt,
};
#[cfg(verus_keep_ghost)]
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::std_specs::iter::{ExactSizeIteratorSpec, IteratorSpec};
#[cfg(verus_keep_ghost)]
use vstd::std_specs::cmp::{OrdSpec, OrdSpecImpl, PartialOrdSpecImpl, PartialEqSpecImpl};

#[derive(Clone, Copy)]
pub struct KeyedRow(pub [u8; 16], pub u32);

impl PartialEq for KeyedRow {
    fn eq(&self, other: &Self) -> bool {
        matches!(compare_keyed_rows(self, other), Ordering::Equal)
    }
}

impl Eq for KeyedRow {}

impl PartialOrd for KeyedRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyedRow {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_keyed_rows(self, other)
    }
}

#[cfg(verus_keep_ghost)]
verus! {
pub open spec fn key_rank(row: KeyedRow) -> int {
    row_rank(row.0@, row.1)
}

pub open spec fn rank_cmp(left: KeyedRow, right: KeyedRow) -> Ordering {
    if key_rank(left) < key_rank(right) {
        Ordering::Less
    } else if key_rank(left) == key_rank(right) {
        Ordering::Equal
    } else {
        Ordering::Greater
    }
}

impl PartialEqSpecImpl for KeyedRow {
    open spec fn obeys_eq_spec() -> bool { true }
    open spec fn eq_spec(&self, other: &Self) -> bool {
        key_rank(*self) == key_rank(*other)
    }
}

impl PartialOrdSpecImpl for KeyedRow {
    open spec fn obeys_partial_cmp_spec() -> bool { true }
    open spec fn partial_cmp_spec(&self, other: &Self) -> Option<Ordering> {
        Some(rank_cmp(*self, *other))
    }
}

impl OrdSpecImpl for KeyedRow {
    open spec fn obeys_cmp_spec() -> bool { true }
    open spec fn cmp_spec(&self, other: &Self) -> Ordering {
        rank_cmp(*self, *other)
    }
}

broadcast use {vstd::seq_lib::group_seq_properties, vstd::multiset::group_multiset_properties};

spec fn row_projection(keys: Seq<KeyedRow>) -> Seq<u32> {
    keys.map_values(|key: KeyedRow| key.1)
}

spec fn carries_stable_iids(keys: Seq<KeyedRow>, iid_at: IidAt) -> bool {
    forall |i: int| 0 <= i < keys.len() ==> #[trigger] keys[i].0@ == iid_at(keys[i].1)
}

proof fn stable_projection_count(keys: Seq<KeyedRow>, iid_at: IidAt, wanted: KeyedRow)
    requires
        carries_stable_iids(keys, iid_at),
        wanted.0@ == iid_at(wanted.1),
    ensures row_projection(keys).to_multiset().count(wanted.1) == keys.to_multiset().count(wanted),
    decreases keys.len(),
{
    broadcast use vstd::multiset::group_multiset_axioms;
    if keys.len() > 0 {
        let head = keys[0];
        let tail = keys.drop_first();
        assert forall |i: int| 0 <= i < tail.len() implies #[trigger] tail[i].0@ == iid_at(tail[i].1) by {
            assert(tail[i] == keys[i + 1]);
        }
        stable_projection_count(tail, iid_at, wanted);
        if head.1 == wanted.1 {
            assert(head.0@ == wanted.0@);
            assert(head.0 =~= wanted.0);
        }
        assert((head == wanted) == (head.1 == wanted.1));
        assert(keys =~= seq![head] + tail);
        assert(row_projection(keys) =~= seq![head.1] + row_projection(tail));
        vstd::seq_lib::lemma_multiset_commutative(seq![head], tail);
        vstd::seq_lib::lemma_multiset_commutative(seq![head.1], row_projection(tail));
        assert(keys.to_multiset().count(wanted) ==
            keys.drop_first().to_multiset().count(wanted) + if head == wanted { 1nat } else { 0nat });
        assert(row_projection(keys).to_multiset().count(wanted.1) ==
            row_projection(tail).to_multiset().count(wanted.1) + if head.1 == wanted.1 { 1nat } else { 0nat });
    } else {
        assert(row_projection(keys) =~= Seq::<u32>::empty());
        assert(keys =~= Seq::<KeyedRow>::empty());
    }
}

proof fn matching_bags_preserve_ordinal_multiplicity(
    before: Seq<KeyedRow>, after: Seq<KeyedRow>, iid_at: IidAt,
)
requires
    before.to_multiset() == after.to_multiset(),
    carries_stable_iids(before, iid_at),
    carries_stable_iids(after, iid_at),
ensures row_projection(before).to_multiset() == row_projection(after).to_multiset(),
{
    broadcast use vstd::multiset::group_multiset_axioms;
    assert forall |row: u32| row_projection(before).to_multiset().count(row) ==
        row_projection(after).to_multiset().count(row) by {
        if row_projection(before).contains(row) {
            let i = choose |i: int| 0 <= i < row_projection(before).len()
                && #[trigger] row_projection(before)[i] == row;
            let key = before[i];
            stable_projection_count(before, iid_at, key);
            stable_projection_count(after, iid_at, key);
        } else if row_projection(after).contains(row) {
            let i = choose |i: int| 0 <= i < row_projection(after).len()
                && #[trigger] row_projection(after)[i] == row;
            let key = after[i];
            stable_projection_count(before, iid_at, key);
            stable_projection_count(after, iid_at, key);
        }
    }
    assert(row_projection(before).to_multiset() =~= row_projection(after).to_multiset());
}

proof fn merge_uses_original_rows<F, E>(
    left: Seq<u32>, right: Seq<u32>, left_source: Seq<u32>, right_source: Seq<u32>,
    read_iid: &F, result: &Result<Vec<u32>, E>,
)
where F: Fn(u32) -> Result<[u8; 16], E>
requires
    left.to_multiset() == left_source.to_multiset(),
    right.to_multiset() == right_source.to_multiset(),
    result is Ok ==> result->Ok_0@.to_multiset() == (left + right).to_multiset(),
    result is Err ==> declined_row(left, right, read_iid, result->Err_0),
ensures
    result is Ok ==> result->Ok_0@.to_multiset() == (left_source + right_source).to_multiset(),
    result is Err ==> declined_row(left_source + right_source, Seq::empty(), read_iid, result->Err_0),
{
    match result {
        Ok(_) => {
            vstd::seq_lib::lemma_multiset_commutative(left, right);
            vstd::seq_lib::lemma_multiset_commutative(left_source, right_source);
        }
        Err(error) => {
            assert forall |row: u32| #![trigger left.contains(row)] #![trigger right.contains(row)]
                (left.contains(row) || right.contains(row)) implies
                    (left_source + right_source).contains(row) by {
                if left.contains(row) {
                    assert(left.to_multiset().count(row) > 0);
                    assert(left_source.contains(row));
                } else {
                    assert(right.to_multiset().count(row) > 0);
                    assert(right_source.contains(row));
                }
            }
            widen_iid_error(left, right, left_source + right_source, read_iid, *error);
        }
    }
}

}

#[cfg_attr(verus_keep_ghost, verus_spec(result =>
    ensures
        (result == Ordering::Less) == (key_rank(*left) < key_rank(*right)),
        (result == Ordering::Equal) == (key_rank(*left) == key_rank(*right)),
))]
fn compare_keyed_rows(left: &KeyedRow, right: &KeyedRow) -> Ordering {
    #[cfg(verus_keep_ghost)]
    proof! {
        crate::laws::rank_is_unsigned_iid_then_descending_index(left.0@, right.0@, left.1, right.1);
    }
    compare_row_keys(&left.0, left.1, &right.0, right.1)
}

#[cfg_attr(verus_keep_ghost, verus_spec(
    ensures
        ordered_by(final(rows)@, |row: KeyedRow| key_rank(row)),
        final(rows)@.to_multiset() == old(rows)@.to_multiset(),
))]
fn order_keyed_run(rows: &mut [KeyedRow]) {
    #[cfg(verus_keep_ghost)]
    proof! {
        use vstd::laws_cmp::*;
        use vstd::laws_eq::*;
        assert(obeys_cmp::<KeyedRow>()) by {
            reveal(obeys_eq_spec_properties);
            reveal(obeys_partial_cmp_spec_properties);
            reveal(obeys_cmp_partial_ord);
            reveal(obeys_cmp_ord);
        }
    }
    rows.sort_unstable();
    #[cfg(verus_keep_ghost)]
    proof! {
        let run = rows@;
        assert forall |i: int, j: int| #![trigger run[i], run[j]]
            0 <= i < j < run.len() implies key_rank(run[i]) <= key_rank(run[j]) by {
            assert(OrdSpec::cmp_spec(&run[i], &run[j]) != Ordering::Greater);
        }
    }

}

#[cfg_attr(verus_keep_ghost, verus_spec(row => ensures row == key.1))]
fn row_index(key: KeyedRow) -> u32 {
    key.1
}

#[cfg_attr(verus_keep_ghost, verus_spec(result =>
    ensures result@ == row_projection(keys@),
))]
fn project_row_indices(keys: Vec<KeyedRow>) -> Vec<u32> {
    keys.into_iter().map(row_index).collect()
}

#[cfg_attr(verus_keep_ghost, verus_spec(result =>
    with Ghost(source): Ghost<Seq<u32>>, Ghost(iid_at): Ghost<IidAt>
    requires
        rows.obeys_prophetic_iter_laws(),
        rows.remaining() == source,
        forall |row: u32| #[trigger] read_iid.requires((row,)),
        forall |row: u32, found: Result<[u8; 16], E>|
            #[trigger] read_iid.ensures((row,), found) && found is Ok ==> found->Ok_0@ == iid_at(row),
    ensures
        result is Ok ==> carries_stable_iids(result->Ok_0@, iid_at)
            && row_projection(result->Ok_0@) == source,
        result is Err ==> declined_row(source, Seq::empty(), &read_iid, result->Err_0),
))]
fn read_keyed_run<I, F, E>(mut rows: I, read_iid: F) -> Result<Vec<KeyedRow>, E>
where
    I: ExactSizeIterator<Item = u32>,
    F: Fn(u32) -> Result<[u8; 16], E>,
{
    #[cfg(verus_keep_ghost)]
    let _ = rows.len();
    let mut keyed = Vec::new();
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            rows.obeys_prophetic_iter_laws(),
            rows.exact_len() == rows.remaining().len(),
            row_projection(keyed@) + rows.remaining() == source,
            carries_stable_iids(keyed@, iid_at),
            forall |row: u32| #[trigger] read_iid.requires((row,)),
            forall |row: u32, found: Result<[u8; 16], E>|
                #[trigger] read_iid.ensures((row,), found) && found is Ok ==> found->Ok_0@ == iid_at(row),
        decreases rows.exact_len(),
    ))]
    loop {
        #[cfg(verus_keep_ghost)]
        proof_decl! {
            let ghost remaining = rows.remaining();
            let ghost previous = keyed@;
        }
        let Some(row) = rows.next() else {
            #[cfg(verus_keep_ghost)]
            proof! { assert(remaining.len() == 0); }
            return Ok(keyed);
        };
        #[cfg(verus_keep_ghost)]
        let _ = rows.len();
        let observed = read_iid(row);
        #[cfg(verus_keep_ghost)]
        proof! {
            assert(remaining =~= seq![row] + rows.remaining());
            assert(read_iid.ensures((row,), observed));
        }
        let key = match observed {
            Ok(bytes) => KeyedRow(bytes, row),
            Err(error) => {
                #[cfg(verus_keep_ghost)]
                proof! {
                    assert(source.contains(row));
                    assert(read_iid.ensures((row,), Err(error)));
                    record_iid_error(source, Seq::empty(), &read_iid, row, error);
                }
                return Err(error);
            }
        };
        keyed.push(key);
        #[cfg(verus_keep_ghost)]
        proof! {
            assert(row_projection(keyed@) =~= row_projection(previous).push(row));
            assert(carries_stable_iids(keyed@, iid_at));
        }
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(result =>
    with Ghost(source): Ghost<Seq<u32>>, Ghost(iid_at): Ghost<IidAt>
    requires
        rows.obeys_prophetic_iter_laws(),
        rows.remaining() == source,
        forall |row: u32| #[trigger] read_iid.requires((row,)),
        forall |row: u32, found: Result<[u8; 16], E>|
            #[trigger] read_iid.ensures((row,), found) && found is Ok ==> found->Ok_0@ == iid_at(row),
    ensures
        result is Ok ==> ordered(result->Ok_0@, iid_rank(iid_at))
            && result->Ok_0@.to_multiset() == source.to_multiset(),
        result is Err ==> declined_row(source, Seq::empty(), &read_iid, result->Err_0),
))]
fn sort_rows_by_iid<I, F, E>(rows: I, read_iid: F) -> Result<Vec<u32>, E>
where
    I: ExactSizeIterator<Item = u32>,
    F: Fn(u32) -> Result<[u8; 16], E>,
{
    #[cfg(verus_keep_ghost)]
    proof_with! { Ghost(source), Ghost(iid_at) }
    let run = read_keyed_run(rows, read_iid);
    let mut keyed = match run {
        Ok(keyed) => keyed,
        Err(error) => return Err(error),
    };
    #[cfg(verus_keep_ghost)]
    proof_decl! { let ghost before = keyed@; }
    order_keyed_run(keyed.as_mut_slice());
    #[cfg(verus_keep_ghost)]
    proof! {
        assert forall |i: int| 0 <= i < keyed@.len() implies #[trigger] keyed@[i].0@ == iid_at(keyed@[i].1) by {
            let key = keyed@[i];
            assert(keyed@.contains(key));
            assert(keyed@.to_multiset().count(key) > 0);
            assert(before.contains(key));
            let j = choose |j: int| 0 <= j < before.len() && #[trigger] before[j] == key;
            assert(before[j].0@ == iid_at(before[j].1));
        }
        matching_bags_preserve_ordinal_multiplicity(before, keyed@, iid_at);
        assert forall |i: int, j: int| 0 <= i < j < keyed@.len() implies
            iid_rank(iid_at)(#[trigger] row_projection(keyed@)[i]) <=
                iid_rank(iid_at)(#[trigger] row_projection(keyed@)[j]) by {
            let l = keyed@[i];
            let r = keyed@[j];
            assert(key_rank(l) <= key_rank(r));
            iid_rank_exposes_the_row_key(iid_at, l.1);
            iid_rank_exposes_the_row_key(iid_at, r.1);
        }
        assert(ordered(row_projection(keyed@), iid_rank(iid_at)));
    }
    Ok(project_row_indices(keyed))
}

#[cfg_attr(verus_keep_ghost, verus_spec(result =>
    with Ghost(source): Ghost<Seq<u32>>, Ghost(iid_at): Ghost<IidAt>
    requires
        pending.obeys_prophetic_iter_laws(),
        pending.remaining() == source,
        ordered(stored@, iid_rank(iid_at)),
        stored@.len() + source.len() <= usize::MAX,
        forall |row: u32| #[trigger] read_iid.requires((row,)),
        forall |row: u32, found: Result<[u8; 16], E>|
            #[trigger] read_iid.ensures((row,), found) && found is Ok ==> found->Ok_0@ == iid_at(row),
    ensures
        result is Ok ==> ordered(result->Ok_0@, iid_rank(iid_at))
            && result->Ok_0@.to_multiset() == (stored@ + source).to_multiset(),
        result is Err ==> declined_row(stored@ + source, Seq::empty(), &read_iid, result->Err_0),
))]
pub(crate) fn merge_pending_rows<I, F, E>(stored: &[u32], pending: I, read_iid: F) -> Result<Vec<u32>, E>
where
    I: ExactSizeIterator<Item = u32>,
    F: Copy + Fn(u32) -> Result<[u8; 16], E>,
{
    #[cfg(verus_keep_ghost)]
    proof_with! { Ghost(source), Ghost(iid_at) }
    let sorted = sort_rows_by_iid(pending, read_iid);
    let sorted = match sorted {
        Ok(sorted) => sorted,
        Err(error) => {
            #[cfg(verus_keep_ghost)]
            proof! {
                widen_iid_error(source, Seq::empty(), stored@ + source, &read_iid, error);
            }
            return Err(error);
        }
    };
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(sorted@.len() == sorted@.to_multiset().len());
        assert(source.len() == source.to_multiset().len());
    }
    #[cfg(verus_keep_ghost)]
    proof_with! { Ghost(iid_at) }
    let result = crate::leaf_merge_step::merge_sorted_rows(stored, &sorted, read_iid);
    #[cfg(verus_keep_ghost)]
    proof! {
        merge_uses_original_rows(stored@, sorted@, stored@, source, &read_iid, &result);
    }
    result
}

#[cfg_attr(verus_keep_ghost, verus_spec(result =>
    with Ghost(pending_source): Ghost<Seq<u32>>, Ghost(inserted_source): Ghost<Seq<u32>>,
        Ghost(iid_at): Ghost<IidAt>
    requires
        pending.obeys_prophetic_iter_laws(),
        pending.remaining() == pending_source,
        inserted.obeys_prophetic_iter_laws(),
        inserted.remaining() == inserted_source,
        ordered(stored@, iid_rank(iid_at)),
        stored@.len() + pending_source.len() + inserted_source.len() <= usize::MAX,
        forall |row: u32| #[trigger] read_iid.requires((row,)),
        forall |row: u32, found: Result<[u8; 16], E>|
            #[trigger] read_iid.ensures((row,), found) && found is Ok ==> found->Ok_0@ == iid_at(row),
    ensures
        result is Ok ==> ordered(result->Ok_0@, iid_rank(iid_at))
            && result->Ok_0@.to_multiset() == (stored@ + (pending_source + inserted_source)).to_multiset(),
        result is Err ==>
            declined_row(stored@ + (pending_source + inserted_source), Seq::empty(), &read_iid, result->Err_0),
))]
pub(crate) fn merge_overflow_rows<P, I, F, E>(
    stored: &[u32], pending: P, inserted: I, read_iid: F,
) -> Result<Vec<u32>, E>
where
    P: ExactSizeIterator<Item = u32>,
    I: ExactSizeIterator<Item = u32>,
    F: Copy + Fn(u32) -> Result<[u8; 16], E>,
{
    #[cfg(verus_keep_ghost)]
    proof_decl! { let ghost all_pending = pending_source + inserted_source; }
    #[cfg(verus_keep_ghost)]
    proof_with! { Ghost(inserted_source), Ghost(iid_at) }
    let sorted_inserted = sort_rows_by_iid(inserted, read_iid);
    let sorted_inserted = match sorted_inserted {
        Ok(sorted) => sorted,
        Err(error) => {
            #[cfg(verus_keep_ghost)]
            proof! {
                widen_iid_error(inserted_source, Seq::empty(), stored@ + all_pending, &read_iid, error);
            }
            return Err(error);
        }
    };
    #[cfg(verus_keep_ghost)]
    proof_with! { Ghost(pending_source), Ghost(iid_at) }
    let sorted_pending = sort_rows_by_iid(pending, read_iid);
    let sorted_pending = match sorted_pending {
        Ok(sorted) => sorted,
        Err(error) => {
            #[cfg(verus_keep_ghost)]
            proof! {
                widen_iid_error(pending_source, Seq::empty(), stored@ + all_pending, &read_iid, error);
            }
            return Err(error);
        }
    };
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(sorted_pending@.len() == sorted_pending@.to_multiset().len());
        assert(pending_source.len() == pending_source.to_multiset().len());
        assert(sorted_inserted@.len() == sorted_inserted@.to_multiset().len());
        assert(inserted_source.len() == inserted_source.to_multiset().len());
    }
    #[cfg(verus_keep_ghost)]
    proof_with! { Ghost(iid_at) }
    let combined = crate::leaf_merge_step::merge_sorted_rows(&sorted_pending, &sorted_inserted, read_iid);
    #[cfg(verus_keep_ghost)]
    proof! {
        merge_uses_original_rows(
            sorted_pending@, sorted_inserted@, pending_source, inserted_source, &read_iid, &combined,
        );
    }
    let combined = match combined {
        Ok(combined) => combined,
        Err(error) => {
            #[cfg(verus_keep_ghost)]
            proof! {
                widen_iid_error(all_pending, Seq::empty(), stored@ + all_pending, &read_iid, error);
            }
            return Err(error);
        }
    };
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(combined@.len() == combined@.to_multiset().len());
        assert(all_pending.len() == all_pending.to_multiset().len());
    }
    #[cfg(verus_keep_ghost)]
    proof_with! { Ghost(iid_at) }
    let result = crate::leaf_merge_step::merge_sorted_rows(stored, &combined, read_iid);
    #[cfg(verus_keep_ghost)]
    proof! {
        merge_uses_original_rows(stored@, combined@, stored@, all_pending, &read_iid, &result);
    }
    result
}
