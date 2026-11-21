#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg(verus_keep_ghost)]
verus! {
    pub open spec fn observations_agree<F: Fn(usize) -> bool>(observer: F, observations: Seq<bool>) -> bool {
        &&& forall|index: usize| index < observations.len() ==> #[trigger] call_requires(observer, (index,))
        &&& forall|index: usize, observed: bool| index < observations.len()
            && #[trigger] call_ensures(observer, (index,), observed) ==> observed == observations[index as int]
    }

    pub open spec fn observation_count(readers: usize) -> int {
        readers as int
    }

    pub open spec fn has_reference(observations: Seq<bool>, limit: int) -> bool {
        exists|index: int| 0 <= index < limit && #[trigger] observations[index]
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    requires observations@.len() == readers, observations_agree(observes, observations@),
    ensures out == (checkpoint_requires || has_reference(observations@, observation_count(readers))),
))]
pub(crate) fn any_reference_requires<F: Fn(usize) -> bool>(
    checkpoint_requires: bool, readers: usize, observes: F, #[cfg(verus_keep_ghost)] observations: Ghost<Seq<bool>>,
) -> bool {
    if checkpoint_requires { return true; }
    let end = readers;
    let mut index = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            !checkpoint_requires,
            index <= end <= readers,
            end as int == observation_count(readers),
            observations@.len() == readers,
            observations_agree(observes, observations@),

            !has_reference(observations@, index as int),
        decreases end - index,
    ))]
    while index < end {
        if observes(index) {
            #[cfg(verus_keep_ghost)]
            proof! {
                assert(observations@[index as int]);
                assert(has_reference(observations@, end as int));
            }
            return true;
        }
        index += 1;
    }
    false
}
