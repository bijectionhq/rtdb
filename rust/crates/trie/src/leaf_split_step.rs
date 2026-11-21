#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use crate::partition_step::{bucket_rows, iid_digit, read_prefix, GroupingInput};
#[cfg(verus_keep_ghost)]
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::std_specs::iter::{ExactSizeIteratorSpec, IteratorSpec};

pub(crate) enum SplitContents<N> {
    Leaf(Vec<u32>),
    Branch([Option<N>; 4]),
}

#[cfg(verus_keep_ghost)]
verus! {
broadcast use {vstd::seq_lib::group_seq_properties, vstd::multiset::group_multiset_properties};

pub(crate) struct StoredRun {
    pub path: Seq<u8>,
    pub rows: Seq<u32>,
}

pub(crate) open spec fn child_runs<N>(child: Option<N>, runs: spec_fn(N) -> Seq<StoredRun>) -> Seq<StoredRun> {
    match child { Some(node) => runs(node), None => Seq::empty() }
}

pub(crate) open spec fn stored_runs<N>(path: Seq<u8>, contents: SplitContents<N>, runs: spec_fn(N) -> Seq<StoredRun>) -> Seq<StoredRun> {
    match contents {
        SplitContents::Leaf(rows) => seq![StoredRun { path, rows: rows@ }],
        SplitContents::Branch(children) => child_runs(children@[0], runs) + child_runs(children@[1], runs)
            + child_runs(children@[2], runs) + child_runs(children@[3], runs),
    }
}

pub(crate) open spec fn valid_path(path: Seq<u8>) -> bool {
    path.len() <= 64 && forall |i: int| 0 <= i < path.len() ==> #[trigger] path[i] < 4
}

#[verifier::reject_recursive_types(N)]
pub(crate) struct SplitModel<E, N> {
    pub read: spec_fn(u32) -> Result<[u8;16], E>,
    pub iid_at: spec_fn(u32) -> Seq<u8>,
    pub rank: spec_fn(u32) -> int,
    pub node_path: spec_fn(N) -> Seq<u8>,
    pub runs: spec_fn(N) -> Seq<StoredRun>,
}

pub(crate) open spec fn ordered(rows: Seq<u32>, rank: spec_fn(u32) -> int) -> bool {
    forall |i: int, j: int| 0 <= i < j < rows.len() ==>
        rank(#[trigger] rows[i]) <= rank(#[trigger] rows[j])
}

pub(crate) open spec fn row_in_path(row: u32, path: Seq<u8>, iid_at: spec_fn(u32) -> Seq<u8>) -> bool {
    forall |level: int| 0 <= level < path.len() ==> #[trigger] iid_digit(iid_at(row), level) == path[level]
}

pub(crate) open spec fn valid_run<E, N>(rows: Seq<u32>, path: Seq<u8>, model: SplitModel<E, N>) -> bool {
    valid_path(path) && ordered(rows, model.rank) &&
        forall |i: int| 0 <= i < rows.len() ==> row_in_path(#[trigger] rows[i], path, model.iid_at)
}

pub(crate) open spec fn valid_leaves<E, N>(runs: Seq<StoredRun>, root: Seq<u8>, model: SplitModel<E, N>) -> bool {
    forall |i: int| 0 <= i < runs.len() ==>
        root.is_prefix_of(#[trigger] runs[i].path) && valid_run(runs[i].rows, runs[i].path, model)
}

pub(crate) open spec fn fits_split_mode(
    runs: Seq<StoredRun>, root_depth: nat, page_limit: usize, recursive: bool,
) -> bool {
    forall |i: int| 0 <= i < runs.len() ==> {
        let run = #[trigger] runs[i];
        if recursive { run.rows.len() <= page_limit || run.path.len() == 64 }
        else { run.path.len() <= root_depth + 1 }
    }
}

pub(crate) open spec fn count_rows(runs: Seq<StoredRun>, wanted: u32) -> nat
    decreases runs.len(),
{
    if runs.len() == 0 { 0 } else {
        count_rows(runs.drop_last(), wanted) + runs.last().rows.to_multiset().count(wanted)
    }
}

proof fn counts_add(left: Seq<StoredRun>, right: Seq<StoredRun>, wanted: u32)
    ensures count_rows(left + right, wanted) == count_rows(left, wanted) + count_rows(right, wanted),
    decreases right.len(),
{
    if right.len() == 0 {
        assert(left + right =~= left);
    } else {
        assert((left + right).drop_last() =~= left + right.drop_last());
        counts_add(left, right.drop_last(), wanted);
    }
}

spec fn in_bucket<E>(input: GroupingInput<E>, depth: int, bucket: int) -> spec_fn(u32) -> bool {
    |row: u32| match (input.read)(row) {
        Ok(iid) => iid_digit(iid@, depth) == bucket,
        Err(_) => false,
    }
}

proof fn bucket_output_preserves_the_run<E, N>(
    input: GroupingInput<E>, model: SplitModel<E, N>, path: Seq<u8>, bucket: u8, rows: Seq<u32>,
)
    requires
        bucket < 4, path.len() < 64,
        input.read == model.read,
        valid_run(input.rows, path, model),
        rows == bucket_rows(input, path.len() as int, input.rows.len(), bucket as int),
        forall |row:u32| (#[trigger] (model.read)(row)) is Ok ==>
            (model.read)(row)->Ok_0@ == (model.iid_at)(row),
    ensures
        valid_run(rows, path.push(bucket), model),
        forall |wanted: u32| #[trigger] rows.to_multiset().count(wanted) ==
            if in_bucket(input, path.len() as int, bucket as int)(wanted) {
                input.rows.to_multiset().count(wanted)
            } else { 0 },
{
    let keep = in_bucket(input, path.len() as int, bucket as int);
    assert(input.rows.take(input.rows.len() as int) =~= input.rows);
    assert(rows == input.rows.filter(keep));
    filtering_preserves_rank_order(input.rows, keep, model.rank);
    assert forall |wanted:u32| #[trigger] rows.to_multiset().count(wanted) ==
        if keep(wanted) { input.rows.to_multiset().count(wanted) } else { 0 } by {
        filtering_keeps_every_selected_occurrence(input.rows, keep, wanted);
    }
    broadcast use vstd::seq_lib::group_filter_ensures;
    assert forall |i: int| 0 <= i < rows.len() implies
        row_in_path(#[trigger] rows[i], path.push(bucket), model.iid_at) by {
        filtered_position_origin(input.rows, keep, i);
        let origin = choose |n:int| 0 <= n < input.rows.len() && #[trigger] input.rows[n] == rows[i];
        let row = input.rows[origin];
        assert(row_in_path(row, path, model.iid_at));
        assert(keep(rows[i]));
        assert((model.read)(row) is Ok);
        assert(iid_digit((model.iid_at)(row), path.len() as int) == bucket as int);
        assert forall |j:int| 0 <= j < path.push(bucket).len() implies
            #[trigger] iid_digit((model.iid_at)(row), j) == path.push(bucket)[j] by {
            if j < path.len() { assert(path.push(bucket)[j] == path[j]); }
        }
    }
    assert(valid_path(path.push(bucket)));
}

proof fn leaves_extend_root<E, N>(runs: Seq<StoredRun>, root: Seq<u8>, wider: Seq<u8>, model: SplitModel<E, N>)
    requires valid_leaves(runs, root, model), wider.is_prefix_of(root),
    ensures valid_leaves(runs, wider, model),
{
    assert forall |i:int| 0 <= i < runs.len() implies
        wider.is_prefix_of(#[trigger] runs[i].path) && valid_run(runs[i].rows, runs[i].path, model) by {
        assert(root.is_prefix_of(runs[i].path));
        assert(wider.is_prefix_of(runs[i].path));
    }
}

proof fn append_valid_leaves<E, N>(
    left: Seq<StoredRun>, right: Seq<StoredRun>, root: Seq<u8>, model: SplitModel<E, N>,
    page_limit: usize, recursive: bool,
)
    requires
        valid_leaves(left, root, model), valid_leaves(right, root, model),
        fits_split_mode(left, root.len(), page_limit, recursive),
        fits_split_mode(right, root.len(), page_limit, recursive),
    ensures
        valid_leaves(left + right, root, model),
        fits_split_mode(left + right, root.len(), page_limit, recursive),
{
    assert forall |i:int| 0 <= i < (left + right).len() implies
        root.is_prefix_of(#[trigger] (left + right)[i].path) &&
            valid_run((left + right)[i].rows, (left + right)[i].path, model) by {
        if i < left.len() {
            assert((left + right)[i] == left[i]);
        } else {
            assert((left + right)[i] == right[i - left.len()]);
        }
    }
    assert forall |i: int| 0 <= i < (left + right).len() implies {
        let run = #[trigger] (left + right)[i];
        if recursive { run.rows.len() <= page_limit || run.path.len() == 64 }
        else { run.path.len() <= root.len() + 1 }
    } by {
        if i < left.len() {
            assert((left + right)[i] == left[i]);
        } else {
            assert((left + right)[i] == right[i - left.len()]);
        }
    }
}

proof fn sum_bucket_counts<E>(
    input: GroupingInput<E>, depth: int, first: Seq<u32>, second: Seq<u32>, third: Seq<u32>, fourth: Seq<u32>,
)
    requires
        0 <= depth < 64,
        read_prefix(input, input.rows.len() as int),
        forall |row:u32| #[trigger] first.to_multiset().count(row) ==
            if in_bucket(input, depth, 0)(row) { input.rows.to_multiset().count(row) } else { 0 },
        forall |row:u32| #[trigger] second.to_multiset().count(row) ==
            if in_bucket(input, depth, 1)(row) { input.rows.to_multiset().count(row) } else { 0 },
        forall |row:u32| #[trigger] third.to_multiset().count(row) ==
            if in_bucket(input, depth, 2)(row) { input.rows.to_multiset().count(row) } else { 0 },
        forall |row:u32| #[trigger] fourth.to_multiset().count(row) ==
            if in_bucket(input, depth, 3)(row) { input.rows.to_multiset().count(row) } else { 0 },
    ensures forall |row: u32| #![trigger input.rows.to_multiset().count(row)]
        first.to_multiset().count(row) + second.to_multiset().count(row)
           + third.to_multiset().count(row) + fourth.to_multiset().count(row) == input.rows.to_multiset().count(row),
{
    assert forall |row: u32| #![trigger input.rows.to_multiset().count(row)]
        first.to_multiset().count(row) + second.to_multiset().count(row)
           + third.to_multiset().count(row) + fourth.to_multiset().count(row) == input.rows.to_multiset().count(row) by {
        if input.rows.to_multiset().count(row) > 0 {
            assert(input.rows.contains(row));
            let i = choose |i:int| 0 <= i < input.rows.len() && #[trigger] input.rows[i] == row;
            assert((input.read)(row) is Ok);
            let digit = iid_digit((input.read)(row)->Ok_0@, depth);
            assert(0 <= digit < 4);
        }
    }
}

pub(crate) open spec fn callback_matches<F, E, N>(reader: F, model: SplitModel<E, N>) -> bool
where F: Fn(u32) -> Result<[u8;16],E>
{
    (forall |row:u32| #[trigger] reader.requires((row,))) &&
    (forall |row:u32, result:Result<[u8;16],E>| #[trigger] reader.ensures((row,), result) ==>
        result == (model.read)(row)) &&
    (forall |row:u32| (#[trigger] (model.read)(row)) is Ok ==>
        (model.read)(row)->Ok_0@ == (model.iid_at)(row))
}

pub(crate) open spec fn factory_matches<C, E, N>(store: C, model: SplitModel<E, N>) -> bool
where C: Fn(Vec<u8>, SplitContents<N>) -> N
{
    (forall |path: Vec<u8>, contents: SplitContents<N>| #[trigger] store.requires((path, contents))) &&
    (forall |path: Vec<u8>, contents: SplitContents<N>, node: N| #[trigger] store.ensures((path, contents),node)
        ==> (model.node_path)(node) == path@ && (model.runs)(node) == stored_runs(path@, contents, model.runs))
}

proof fn filtered_position_origin<T>(rows: Seq<T>, keep: spec_fn(T) -> bool, at: int)
    requires 0 <= at < rows.filter(keep).len(),
    ensures exists |origin: int| 0 <= origin < rows.len() && #[trigger] rows[origin] == rows.filter(keep)[at],
    decreases rows.len(),
{
    reveal(Seq::filter);
    assert(rows.len() > 0);
    let earlier = rows.drop_last();
    let filtered = earlier.filter(keep);
    if at < filtered.len() {
        filtered_position_origin(earlier, keep, at);
        let origin = choose |k: int| 0 <= k < earlier.len() && #[trigger] earlier[k] == filtered[at];
        assert(rows[origin] == rows.filter(keep)[at]);
    } else {
        assert(at == filtered.len());
        assert(rows[rows.len() - 1] == rows.filter(keep)[at]);
    }
}

proof fn filter_keeps_relative_positions<T>(rows: Seq<T>, keep: spec_fn(T) -> bool, i: int, j: int)
    requires 0 <= i < j < rows.filter(keep).len(),
    ensures exists |first: int, second: int| 0 <= first < second < rows.len()
        && #[trigger] rows[first] == rows.filter(keep)[i] && #[trigger] rows[second] == rows.filter(keep)[j],
    decreases rows.len(),
{
    reveal(Seq::filter);
    assert(rows.len() > 0);
    let earlier = rows.drop_last();
    let filtered = earlier.filter(keep);
    if j < filtered.len() {
        filter_keeps_relative_positions(earlier, keep, i, j);
        let (first, second) = choose |first: int, second: int| 0 <= first < second < earlier.len()
            && #[trigger] earlier[first] == filtered[i] && #[trigger] earlier[second] == filtered[j];
        assert(rows[first] == rows.filter(keep)[i] && rows[second] == rows.filter(keep)[j]);
    } else {
        assert(j == filtered.len());
        filtered_position_origin(earlier, keep, i);
        let first = choose |first: int| 0 <= first < earlier.len() && #[trigger] earlier[first] == filtered[i];
        let second = rows.len() - 1;
        assert(rows[first] == rows.filter(keep)[i] && rows[second] == rows.filter(keep)[j]);
        assert(0 <= first < second < rows.len());
    }
}

proof fn filtering_preserves_rank_order<T>(rows: Seq<T>, keep: spec_fn(T) -> bool, rank: spec_fn(T) -> int)
    requires forall |i: int, j: int| 0 <= i < j < rows.len() ==>
        rank(#[trigger] rows[i]) <= rank(#[trigger] rows[j]),
    ensures forall |i: int, j: int| 0 <= i < j < rows.filter(keep).len() ==>
        rank(#[trigger] rows.filter(keep)[i]) <= rank(#[trigger] rows.filter(keep)[j]),
{
    assert forall |i: int, j: int| 0 <= i < j < rows.filter(keep).len() implies
        rank(#[trigger] rows.filter(keep)[i]) <= rank(#[trigger] rows.filter(keep)[j]) by {
        filter_keeps_relative_positions(rows, keep, i, j);
        let (first, second) = choose |first: int, second: int| 0 <= first < second < rows.len()
            && #[trigger] rows[first] == rows.filter(keep)[i] && #[trigger] rows[second] == rows.filter(keep)[j];
        assert(rank(rows[first]) <= rank(rows[second]));
    }
}

proof fn filtering_keeps_every_selected_occurrence<T>(
    rows: Seq<T>, keep: spec_fn(T) -> bool, row: T,
)
    ensures rows.filter(keep).to_multiset().count(row) ==
        if keep(row) { rows.to_multiset().count(row) } else { 0 },
    decreases rows.len(),
{
    reveal(Seq::filter);
    broadcast use vstd::multiset::group_multiset_axioms;
    if rows.len() == 0 {
        assert(rows.filter(keep) =~= Seq::<T>::empty());
    } else {
        let prefix = rows.drop_last();
        let last = rows.last();
        let filtered = prefix.filter(keep);
        filtering_keeps_every_selected_occurrence(prefix, keep, row);
        assert(rows =~= prefix + seq![last]);
        vstd::seq_lib::lemma_multiset_commutative(prefix, seq![last]);
        assert(rows.to_multiset().count(row) ==
            prefix.to_multiset().count(row) + if row == last { 1nat } else { 0nat });
        if keep(last) {
            assert(rows.filter(keep) =~= filtered + seq![last]);
            vstd::seq_lib::lemma_multiset_commutative(filtered, seq![last]);
            assert(rows.filter(keep).to_multiset().count(row) ==
                filtered.to_multiset().count(row) + if row == last { 1nat } else { 0nat });
        } else {
            assert(rows.filter(keep) == filtered);
        }
    }
}
}

#[cfg_attr(verus_keep_ghost, verus_spec(result =>
    with Ghost(model): Ghost<SplitModel<E, N>>
    requires valid_run(rows@, path@, model), callback_matches(read_iid, model), factory_matches(store, model),
    ensures
        result is Ok ==> (model.node_path)(result->Ok_0) == path@
            && valid_leaves((model.runs)(result->Ok_0), path@, model)
            && fits_split_mode((model.runs)(result->Ok_0), path@.len(), page_limit, recursive)
            && forall |row:u32| #[trigger] count_rows((model.runs)(result->Ok_0), row) == rows@.to_multiset().count(row),
    decreases 64 - path@.len(), 1nat,
))]
pub(crate) fn split_subtree<N, E, F, D, C>(
    path: Vec<u8>, rows: Vec<u32>, read_iid: F, depth_error: D, store: C,
    page_limit: usize, recursive: bool,
) -> Result<N, E>
where
    F: Copy + Fn(u32) -> Result<[u8; 16], E>,
    D: Copy + FnOnce() -> E,
    C: Copy + Fn(Vec<u8>, SplitContents<N>) -> N,
{
    #[cfg(verus_keep_ghost)]
    proof_decl! {
        let ghost root = path@;
        let ghost input = GroupingInput { rows: rows@, read: model.read };
    }
    let depth = path.len();
    if rows.len() <= page_limit || depth == 64 {
        let leaf = store(path, SplitContents::Leaf(rows));
        #[cfg(verus_keep_ghost)]
        proof! {
            assert(valid_leaves((model.runs)(leaf), root, model));
            assert(fits_split_mode((model.runs)(leaf), root.len(), page_limit, recursive));
            assert forall |row:u32| #[trigger] count_rows((model.runs)(leaf),row) == input.rows.to_multiset().count(row) by {
                assert((model.runs)(leaf).drop_last() =~= Seq::<StoredRun>::empty());
                reveal_with_fuel(count_rows, 2);
            }
        }
        return Ok(leaf);
    }
    let iter = rows.into_iter();
    #[cfg(verus_keep_ghost)]
    proof_with! { Ghost(input) }
    let [first, second, third, fourth] = crate::partition_step::group_base4_rows(iter, depth, read_iid, depth_error)?;
    #[cfg(verus_keep_ghost)]
    proof_decl! {
        let ghost first_rows = first@;
        let ghost second_rows = second@;
        let ghost third_rows = third@;
        let ghost fourth_rows = fourth@;
    }
    #[cfg(verus_keep_ghost)]
    proof! {
        bucket_output_preserves_the_run(input, model, root, 0, first_rows);
        bucket_output_preserves_the_run(input, model, root, 1, second_rows);
        bucket_output_preserves_the_run(input, model, root, 2, third_rows);
        bucket_output_preserves_the_run(input, model, root, 3, fourth_rows);
        sum_bucket_counts(input, depth as int, first_rows, second_rows, third_rows, fourth_rows);
    }
    #[cfg(verus_keep_ghost)]
    proof_with! { Ghost(model) }
    let first_node = split_child(&path, 0, first, read_iid, depth_error, store, page_limit, recursive)?;
    #[cfg(verus_keep_ghost)]
    proof_with! { Ghost(model) }
    let second_node = split_child(&path, 1, second, read_iid, depth_error, store, page_limit, recursive)?;
    #[cfg(verus_keep_ghost)]
    proof_with! { Ghost(model) }
    let third_node = split_child(&path, 2, third, read_iid, depth_error, store, page_limit, recursive)?;
    #[cfg(verus_keep_ghost)]
    proof_with! { Ghost(model) }
    let fourth_node = split_child(&path, 3, fourth, read_iid, depth_error, store, page_limit, recursive)?;
    #[cfg(verus_keep_ghost)]
    proof_decl! {
        let ghost first_runs = child_runs(first_node, model.runs);
        let ghost second_runs = child_runs(second_node, model.runs);
        let ghost third_runs = child_runs(third_node, model.runs);
        let ghost fourth_runs = child_runs(fourth_node, model.runs);
    }
    let children = [first_node, second_node, third_node, fourth_node];
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(valid_leaves(child_runs(children@[0], model.runs), root.push(0), model));
        assert(valid_leaves(child_runs(children@[1], model.runs), root.push(1), model));
        assert(valid_leaves(child_runs(children@[2], model.runs), root.push(2), model));
        assert(valid_leaves(child_runs(children@[3], model.runs), root.push(3), model));
    }
    let result = store(path, SplitContents::Branch(children));
    #[cfg(verus_keep_ghost)]
    proof! {
        leaves_extend_root(first_runs, root.push(0), root, model);
        leaves_extend_root(second_runs, root.push(1), root, model);
        leaves_extend_root(third_runs, root.push(2), root, model);
        leaves_extend_root(fourth_runs, root.push(3), root, model);
        append_valid_leaves(first_runs, second_runs, root, model, page_limit, recursive);
        append_valid_leaves(first_runs + second_runs, third_runs, root, model, page_limit, recursive);
        append_valid_leaves(first_runs + second_runs + third_runs, fourth_runs, root, model, page_limit, recursive);
        assert forall |row:u32| #[trigger] count_rows((model.runs)(result),row) == input.rows.to_multiset().count(row) by {
            counts_add(first_runs, second_runs, row);
            counts_add(first_runs + second_runs, third_runs, row);
            counts_add(first_runs + second_runs + third_runs, fourth_runs, row);
            assert(input.rows.to_multiset().count(row) == first_rows.to_multiset().count(row) + second_rows.to_multiset().count(row)
                + third_rows.to_multiset().count(row) + fourth_rows.to_multiset().count(row));
        }
    }
    Ok(result)
}

#[cfg_attr(verus_keep_ghost, verus_spec(result =>
    with Ghost(model): Ghost<SplitModel<E, N>>
    requires
        path@.len() < 64, valid_path(path@), bucket < 4,
        valid_run(rows@, path@.push(bucket), model), callback_matches(read_iid, model), factory_matches(store, model),
    ensures
        result is Ok ==> valid_leaves(child_runs(result->Ok_0, model.runs), path@.push(bucket), model)
            && fits_split_mode(child_runs(result->Ok_0, model.runs), path@.len(), page_limit, recursive)
            && (result->Ok_0 is None <==> rows@.len() == 0)
            && (result->Ok_0 is Some ==> (model.node_path)(result->Ok_0->Some_0) == path@.push(bucket))
            && forall |row:u32| #[trigger] count_rows(child_runs(result->Ok_0, model.runs), row) == rows@.to_multiset().count(row),
    decreases 64 - path@.len(), 0nat,
))]
fn split_child<N, E, F, D, C>(
    path: &[u8], bucket: u8, rows: Vec<u32>, read_iid: F, depth_error: D, store: C,
    page_limit: usize, recursive: bool,
) -> Result<Option<N>, E>
where
    F: Copy + Fn(u32) -> Result<[u8; 16], E>,
    D: Copy + FnOnce() -> E,
    C: Copy + Fn(Vec<u8>, SplitContents<N>) -> N,
{
    #[cfg(verus_keep_ghost)]
    proof_decl! {
        let ghost source = rows@;
        let ghost parent = path@;
    }
    if rows.is_empty() { return Ok(None); }
    let mut child_path = Vec::with_capacity(path.len() + 1);
    child_path.extend_from_slice(path);
    child_path.push(bucket);
    #[cfg(verus_keep_ghost)]
    proof_decl! { let ghost root = child_path@; }
    let result = if recursive {
        #[cfg(verus_keep_ghost)]
        proof_with! { Ghost(model) }
        split_subtree(child_path, rows, read_iid, depth_error, store, page_limit, true)?
    } else {
        let leaf = store(child_path, SplitContents::Leaf(rows));
        #[cfg(verus_keep_ghost)]
        proof! {
            assert(valid_leaves((model.runs)(leaf), root, model));
            assert(fits_split_mode((model.runs)(leaf), parent.len(), page_limit, false));
            assert forall |row:u32| #[trigger] count_rows((model.runs)(leaf),row) == source.to_multiset().count(row) by {
                assert((model.runs)(leaf).drop_last() =~= Seq::<StoredRun>::empty());
                reveal_with_fuel(count_rows, 2);
            }
        }
        leaf
    };
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(root == parent.push(bucket));
        assert(fits_split_mode((model.runs)(result), parent.len(), page_limit, recursive));
    }
    Ok(Some(result))
}
