#![allow(unexpected_cfgs)]

use crate::Timestamp;
#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg(verus_keep_ghost)]
verus! {
    pub open spec fn increasing(bounds: Seq<Timestamp>) -> bool {
        forall|i: int, j: int| 0 <= i < j < bounds.len()
            ==> #[trigger] bounds[i].0 < #[trigger] bounds[j].0
    }

    pub open spec fn valid_ceiling(bounds: Seq<Timestamp>, ceilings: Seq<Timestamp>) -> bool {
        bounds.len() == ceilings.len() + 1
            && bounds.len() >= 2
            && bounds[0].0 == i64::MIN && bounds[bounds.len() - 1].0 == i64::MAX
            && increasing(bounds)
    }

    pub open spec fn covers(bounds: Seq<Timestamp>, index: int, point: i64) -> bool {
        0 <= index && index + 1 < bounds.len()
            && bounds[index].0 <= point < bounds[index + 1].0
    }

    pub open spec fn before(bound: Timestamp, at: Timestamp, inclusive: bool) -> bool {
        bound.0 < at.0 || (inclusive && bound.0 == at.0)
    }

    pub open spec fn meets(bounds: Seq<Timestamp>, cell: int, lower: Timestamp, upper: Timestamp) -> bool {
        bounds[cell].0 < upper.0 && lower.0 < bounds[cell + 1].0
    }

    pub open spec fn contains(span: (Timestamp, Timestamp), point: i64) -> bool {
        span.0.0 <= point < span.1.0
    }

    pub proof fn disjoint_cells(bounds: Seq<Timestamp>, left: int, right: int, point: i64)
        requires increasing(bounds), covers(bounds, left, point), covers(bounds, right, point)
        ensures left == right
    {
        if left < right {
            assert(bounds[left + 1].0 <= bounds[right].0);
        } else if right < left {
            assert(bounds[right + 1].0 <= bounds[left].0);
        }
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    requires increasing(bounds@),
    ensures
        out <= bounds@.len(),
        forall|i: int| 0 <= i < out ==> before(#[trigger] bounds@[i], at, inclusive),
        forall|i: int| out <= i < bounds@.len() ==> !before(#[trigger] bounds@[i], at, inclusive),
))]
fn boundary_index(bounds: &[Timestamp], at: Timestamp, inclusive: bool) -> usize {
    let mut lower = 0;
    let mut upper = bounds.len();
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            increasing(bounds@),
            lower <= upper <= bounds@.len(),
            forall|i: int| 0 <= i < lower ==> before(#[trigger] bounds@[i], at, inclusive),
            forall|i: int| upper <= i < bounds@.len() ==> !before(#[trigger] bounds@[i], at, inclusive),
        decreases upper - lower,
    ))]
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        let bound = bounds[middle];
        if bound.0 < at.0 || (inclusive && bound.0 == at.0) {
            lower = middle + 1;
        } else {
            upper = middle;
        }
    }
    lower
}

/// The first and last cells needed for the view. An empty interval still uses one cell.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    requires valid_ceiling(bounds@, ceilings@), lower.0 <= upper.0,
    ensures
        out.0 <= out.1 < ceilings@.len(),
        lower.0 == upper.0 ==> out.0 == out.1
            && (if lower.0 == i64::MAX { out.0 + 1 == ceilings@.len() }
                else { covers(bounds@, out.0 as int, lower.0) }),
        lower.0 < upper.0 ==> forall|cell: int| 0 <= cell < ceilings@.len() ==>
            (out.0 <= cell <= out.1) == #[trigger] meets(bounds@, cell, lower, upper),
))]
pub(crate) fn cell_window(
    bounds: &[Timestamp], ceilings: &[Timestamp], lower: Timestamp, upper: Timestamp,
) -> (usize, usize) {
    let lower_cut = boundary_index(bounds, lower, true);
    let preceding = lower_cut - 1;
    let last_cell = ceilings.len() - 1;
    let first = if preceding < last_cell { preceding } else { last_cell };
    if lower.0 == upper.0 {
        return (first, first);
    }
    let upper_cut = boundary_index(bounds, upper, false);
    let last = upper_cut - 1;
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(before(bounds@[first as int], upper, false));
        assert(!before(bounds@[bounds@.len() - 1], upper, false));
        assert(first <= last);
        assert forall|cell: int| 0 <= cell < ceilings@.len()
            implies (first <= cell <= last) == #[trigger] meets(bounds@, cell, lower, upper) by {
            assert(before(bounds@[cell], upper, false) == (cell < upper_cut));
            assert(before(bounds@[cell + 1], lower, true) == (cell + 1 < lower_cut));
        }
    }
    (first, last)
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    requires cell.0.0 <= cell.1.0, query.0.0 <= query.1.0, cell.0.0 <= query.1.0, query.0.0 <= cell.1.0,
    ensures
        out.0.0 <= out.1.0,
        out.0.0 == if cell.0.0 > query.0.0 { cell.0.0 } else { query.0.0 },
        out.1.0 == if cell.1.0 < query.1.0 { cell.1.0 } else { query.1.0 },
        forall|point: i64| #[trigger] contains(out, point) == (contains(cell, point) && contains(query, point)),
))]
pub(crate) fn clip_cell(cell: (Timestamp, Timestamp), query: (Timestamp, Timestamp)) -> (Timestamp, Timestamp) {
    let (cell_lower, cell_upper) = cell;
    let (query_lower, query_upper) = query;
    let lower = if cell_lower.0 > query_lower.0 { cell_lower } else { query_lower };
    let upper = if cell_upper.0 < query_upper.0 { cell_upper } else { query_upper };
    (lower, upper)
}

#[cfg_attr(verus_keep_ghost, verus_spec(
    requires start <= end <= old(values)@.len(),
    ensures
        final(values)@.len() == old(values)@.len() - (end - start),
        forall|i: int| 0 <= i < final(values)@.len() ==> #[trigger] final(values)@[i]
            == old(values)@[if i < start { i } else { i + (end - start) }],
))]
fn discard_span(values: &mut Vec<Timestamp>, start: usize, end: usize) {
    if start == end { return; }
    let gap = end - start;
    let original_len = values.len();
    let retained_len = original_len - gap;
    let mut cursor = start;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            original_len == old(values)@.len(),
            gap == end - start && 0 < gap,
            retained_len == original_len - gap,
            start <= cursor <= retained_len,
            values@.len() == original_len,
            forall|i: int| 0 <= i < cursor ==> #[trigger] values@[i]
                == old(values)@[if i < start { i } else { i + gap }],
            forall|i: int| cursor <= i < original_len ==> #[trigger] values@[i] == old(values)@[i],
        decreases retained_len - cursor,
    ))]
    while cursor < retained_len {
        values[cursor] = values[cursor + gap];
        cursor += 1;
    }
    values.truncate(retained_len);
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    requires valid_ceiling(old(bounds)@, old(ceilings)@),
    ensures
        valid_ceiling(final(bounds)@, final(ceilings)@),
        out < final(bounds)@.len() && final(bounds)@[out as int].0 == at.0,
        forall|i: int| 0 <= i < old(bounds)@.len() && (#[trigger] old(bounds)@[i]).0 < at.0
            ==> i < final(bounds)@.len() && final(bounds)@[i] == old(bounds)@[i],
        forall|point: i64, i: int, j: int|
            #[trigger] covers(old(bounds)@, i, point) && #[trigger] covers(final(bounds)@, j, point)
            ==> old(ceilings)@[i] == final(ceilings)@[j],
))]
fn split_at(bounds: &mut Vec<Timestamp>, ceilings: &mut Vec<Timestamp>, at: Timestamp) -> usize {
    let index = boundary_index(bounds.as_slice(), at, false);
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(!before(bounds@[bounds@.len() - 1], at, false));
    }
    if bounds[index].0 != at.0 {
        #[cfg(verus_keep_ghost)]
        proof! {
            assert(bounds@[0].0 <= at.0);
        }
        let ceiling = ceilings[index - 1];
        bounds.insert(index, at);
        ceilings.insert(index, ceiling);
        #[cfg(verus_keep_ghost)]
        proof! {
            characterize_insert(old(bounds)@, bounds@, index as int, at);
            characterize_insert(old(ceilings)@, ceilings@, index as int, ceiling);
            split_preserves(old(bounds)@, old(ceilings)@, bounds@, ceilings@, index as int, at);
        }
    }
    index
}

/// Overwrites the valid-time span and retains the ceilings on either side.
#[cfg_attr(verus_keep_ghost, verus_spec(
    requires valid_ceiling(old(bounds)@, old(ceilings)@), lower.0 <= upper.0,
    ensures
        valid_ceiling(final(bounds)@, final(ceilings)@),
        forall|point: i64, j: int| lower.0 <= point < upper.0 && #[trigger] covers(final(bounds)@, j, point)
            ==> final(ceilings)@[j] == system_from,
        forall|point: i64, i: int, j: int|
            (point < lower.0 || point >= upper.0)
            && #[trigger] covers(old(bounds)@, i, point) && #[trigger] covers(final(bounds)@, j, point)
            ==> old(ceilings)@[i] == final(ceilings)@[j],
))]
pub(crate) fn overwrite(
    bounds: &mut Vec<Timestamp>, ceilings: &mut Vec<Timestamp>,
    lower: Timestamp, upper: Timestamp, system_from: Timestamp,
) {
    if lower.0 == upper.0 { return; }
    let start = split_at(bounds, ceilings, lower);
    #[cfg(verus_keep_ghost)]
    proof_decl! { let ghost lower_bounds = bounds@; let ghost lower_ceilings = ceilings@; }
    let end = split_at(bounds, ceilings, upper);
    #[cfg(verus_keep_ghost)]
    proof_decl! { let ghost split_bounds = bounds@; let ghost split_ceilings = ceilings@; }
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(bounds@[start as int].0 == lower.0);
        assert(bounds@[end as int].0 == upper.0);
        assert(start < end);
    }
    ceilings[start] = system_from;
    discard_span(bounds, start + 1, end);
    discard_span(ceilings, start + 1, end);
    #[cfg(verus_keep_ghost)]
    proof! {
        overwrite_preserves(
            old(bounds)@, old(ceilings)@, lower_bounds, lower_ceilings,
            split_bounds, split_ceilings, bounds@, ceilings@, start as int, end as int, system_from,
        );
    }
}

#[cfg(verus_keep_ghost)]
verus! {
    spec fn inserted(before: Seq<Timestamp>, after: Seq<Timestamp>, index: int, value: Timestamp) -> bool {
        after.len() == before.len() + 1
            && after[index] == value
            && forall|i: int| 0 <= i < after.len() && i != index
                ==> #[trigger] after[i] == before[if i < index { i } else { i - 1 }]
    }

    proof fn characterize_insert(before: Seq<Timestamp>, after: Seq<Timestamp>, index: int, value: Timestamp)
        requires 0 <= index <= before.len(), after == before.insert(index, value)
        ensures inserted(before, after, index, value)
    {
        before.insert_ensures(index, value);
    }

    proof fn split_keeps_order(before_bounds: Seq<Timestamp>, after_bounds: Seq<Timestamp>, index: int, at: Timestamp)
        requires
            increasing(before_bounds),
            0 < index < before_bounds.len(),
            before_bounds[index - 1].0 < at.0 < before_bounds[index].0,
            inserted(before_bounds, after_bounds, index, at),
        ensures increasing(after_bounds)
    {
        assert forall|i: int, j: int| 0 <= i < j < after_bounds.len()
            implies #[trigger] after_bounds[i].0 < #[trigger] after_bounds[j].0 by {
            if i == index {
                assert(before_bounds[index].0 <= before_bounds[j - 1].0);
            } else if j == index {
                assert(before_bounds[i].0 <= before_bounds[index - 1].0);
            } else {
                let old_i = if i < index { i } else { i - 1 };
                let old_j = if j < index { j } else { j - 1 };
                assert(old_i < old_j);
                assert(before_bounds[old_i].0 < before_bounds[old_j].0);
            }
        }
    }

    proof fn split_preserves(
        before_bounds: Seq<Timestamp>, before_ceilings: Seq<Timestamp>,
        after_bounds: Seq<Timestamp>, after_ceilings: Seq<Timestamp>, index: int, at: Timestamp,
    )
        requires
            valid_ceiling(before_bounds, before_ceilings),
            0 < index < before_bounds.len(),
            before_bounds[index - 1].0 < at.0 < before_bounds[index].0,
            inserted(before_bounds, after_bounds, index, at),
            inserted(before_ceilings, after_ceilings, index, before_ceilings[index - 1]),
        ensures
            valid_ceiling(after_bounds, after_ceilings),
            forall|i: int| 0 <= i < before_bounds.len() && (#[trigger] before_bounds[i]).0 < at.0
                ==> i < after_bounds.len() && after_bounds[i] == before_bounds[i],
            forall|point: i64, i: int, j: int|
                #[trigger] covers(before_bounds, i, point) && #[trigger] covers(after_bounds, j, point)
                ==> before_ceilings[i] == after_ceilings[j],
    {
        split_keeps_order(before_bounds, after_bounds, index, at);
        assert(after_bounds.len() == before_bounds.len() + 1);
        assert(after_ceilings.len() == before_ceilings.len() + 1);
        assert forall|i: int| 0 <= i < before_bounds.len() && (#[trigger] before_bounds[i]).0 < at.0
            implies i < after_bounds.len() && after_bounds[i] == before_bounds[i] by {
            if i >= index {
                assert(before_bounds[index].0 <= before_bounds[i].0);
            }
        }
        assert forall|point: i64, i: int, j: int|
            #[trigger] covers(before_bounds, i, point) && #[trigger] covers(after_bounds, j, point)
            implies before_ceilings[i] == after_ceilings[j] by {
            let source = if j < index { j } else { j - 1 };
            assert(before_bounds[source].0 <= after_bounds[j].0);
            assert(after_bounds[j + 1].0 <= before_bounds[source + 1].0);
            assert(covers(before_bounds, source, point));
            disjoint_cells(before_bounds, i, source, point);
        }
    }

    proof fn point_cell(bounds: Seq<Timestamp>, point: i64, low: int, high: int) -> (cell: int)
        requires 0 <= low < high < bounds.len(), bounds[low].0 <= point < bounds[high].0
        ensures covers(bounds, cell, point)
        decreases high - low
    {
        if point < bounds[low + 1].0 {
            low
        } else {
            point_cell(bounds, point, low + 1, high)
        }
    }

    proof fn overwrite_preserves(
        original_bounds: Seq<Timestamp>, original_ceilings: Seq<Timestamp>,
        lower_bounds: Seq<Timestamp>, lower_ceilings: Seq<Timestamp>,
        split_bounds: Seq<Timestamp>, split_ceilings: Seq<Timestamp>,
        bounds: Seq<Timestamp>, ceilings: Seq<Timestamp>,
        start: int, end: int, system_from: Timestamp,
    )
        requires
            valid_ceiling(original_bounds, original_ceilings),
            valid_ceiling(lower_bounds, lower_ceilings),
            valid_ceiling(split_bounds, split_ceilings),
            0 <= start < end < split_bounds.len(),
            bounds.len() == split_bounds.len() - (end - start - 1),
            ceilings.len() == split_ceilings.len() - (end - start - 1),
            forall|i: int| 0 <= i < bounds.len() ==> #[trigger] bounds[i]
                == split_bounds[if i <= start { i } else { i + end - start - 1 }],
            forall|i: int| 0 <= i < ceilings.len() ==> #[trigger] ceilings[i]
                == if i == start { system_from }
                    else { split_ceilings[if i < start { i } else { i + end - start - 1 }] },
            forall|p: i64, i: int, j: int|
                #[trigger] covers(original_bounds, i, p) && #[trigger] covers(lower_bounds, j, p)
                ==> original_ceilings[i] == lower_ceilings[j],
            forall|p: i64, i: int, j: int|
                #[trigger] covers(lower_bounds, i, p) && #[trigger] covers(split_bounds, j, p)
                ==> lower_ceilings[i] == split_ceilings[j],
        ensures
            valid_ceiling(bounds, ceilings),
            forall|p: i64, j: int|
                split_bounds[start].0 <= p < split_bounds[end].0 && #[trigger] covers(bounds, j, p)
                ==> ceilings[j] == system_from,
            forall|p: i64, i: int, j: int|
                (p < split_bounds[start].0 || p >= split_bounds[end].0)
                && #[trigger] covers(original_bounds, i, p) && #[trigger] covers(bounds, j, p)
                ==> original_ceilings[i] == ceilings[j],
    {
        assert forall|i: int, j: int| 0 <= i < j < bounds.len()
            implies #[trigger] bounds[i].0 < #[trigger] bounds[j].0 by {
            let left = if i <= start { i } else { i + end - start - 1 };
            let right = if j <= start { j } else { j + end - start - 1 };
            assert(left < right);
            assert(split_bounds[left].0 < split_bounds[right].0);
        }
        assert(valid_ceiling(bounds, ceilings));
        assert(bounds[start].0 == split_bounds[start].0);
        assert(bounds[start + 1].0 == split_bounds[end].0);
        assert forall|p: i64, j: int|
            split_bounds[start].0 <= p < split_bounds[end].0 && #[trigger] covers(bounds, j, p)
            implies ceilings[j] == system_from by {
            assert(covers(bounds, start, p));
            disjoint_cells(bounds, j, start, p);
        }
        assert forall|p: i64, i: int, j: int|
            (p < split_bounds[start].0 || p >= split_bounds[end].0)
            && #[trigger] covers(original_bounds, i, p) && #[trigger] covers(bounds, j, p)
            implies original_ceilings[i] == ceilings[j] by {
            assert(!covers(bounds, start, p));
            assert(j != start);
            let source = if j < start { j } else { j + end - start - 1 };
            assert(covers(split_bounds, source, p));
            let mid = point_cell(lower_bounds, p, 0, lower_bounds.len() - 1);
            assert(original_ceilings[i] == lower_ceilings[mid]);
            assert(lower_ceilings[mid] == split_ceilings[source]);
        }
    }
}
