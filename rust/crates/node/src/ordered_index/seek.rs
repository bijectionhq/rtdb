#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg(verus_keep_ghost)]
verus! {

pub(super) open spec fn splits<F: Fn(usize) -> bool>(length: usize, before: F, at: usize) -> bool {
    &&& at <= length
    &&& forall |i: usize, answer: bool| i < length && #[trigger] call_ensures(before, (i,), answer)
        ==> answer == (i < at)
}

}

/// Find the boundary of a monotone prefix without evaluating the rest of the page.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    requires
        forall |i: usize| i < length ==> #[trigger] call_requires(before, (i,)),
        exists |at: usize| #[trigger] splits(length, before, at),
    ensures
        out <= length,
        splits(length, before, out),
))]
pub(super) fn partition<F: Fn(usize) -> bool>(length: usize, before: F) -> usize {
    let mut lower = 0;
    let mut upper = length;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            lower <= upper <= length,
            forall |i: usize| i < length ==> #[trigger] call_requires(before, (i,)),
            exists |at: usize| lower <= at <= upper && #[trigger] splits(length, before, at),
        decreases upper - lower,
    ))]
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        if before(middle) {
            #[cfg(verus_keep_ghost)]
            proof! {
                let at = choose |at: usize| lower <= at <= upper && splits(length, before, at);
                assert(call_ensures(before, (middle,), true));
                assert(middle < at);
                assert(exists |at: usize| middle + 1 <= at <= upper && #[trigger] splits(length, before, at));
            }
            lower = middle + 1;
        } else {
            #[cfg(verus_keep_ghost)]
            proof! {
                let at = choose |at: usize| lower <= at <= upper && splits(length, before, at);
                assert(call_ensures(before, (middle,), false));
                assert(at <= middle);
                assert(exists |at: usize| lower <= at <= middle && #[trigger] splits(length, before, at));
            }
            upper = middle;
        }
    }
    #[cfg(verus_keep_ghost)]
    proof! {
        let at = choose |at: usize| lower <= at <= upper && splits(length, before, at);
        assert(at == lower);
    }
    lower
}

/// Convert the number consumed in seek direction into an index in a sorted page.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures match out {
        None => consumed >= length,
        Some(slot) => consumed < length && slot < length &&
            slot as int == if reverse { length as int - 1 - consumed as int } else { consumed as int },
    },
))]
pub(super) fn slot(length: usize, consumed: usize, reverse: bool) -> Option<usize> {
    if consumed >= length {
        return None;
    }
    Some(if reverse {
        length - 1 - consumed
    } else {
        consumed
    })
}
