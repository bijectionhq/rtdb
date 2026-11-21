use crate::production::{any_reference_requires, observations_agree};
use vstd::prelude::*;

verus! {

// LAW RETENTION-LIVE-REFERENCE.
pub fn any_generation_keeps_its_dependencies(values: Vec<bool>, reader: usize)
    requires reader < values.len(), values@[reader as int],
{
    let count = values.len();
    let callback = |index: usize| -> (observed: bool)
        requires index < values.len()
        ensures observed == values@[index as int]
    { values[index] };
    proof! { assert(observations_agree(callback, values@)); }
    let required = any_reference_requires(false, count, callback, Ghost(values@));
    assert(required);
}

// LAW RETENTION-CHECKPOINT-REFERENCE.
pub fn a_checkpoint_alone_keeps_its_dependencies() {
    let ghost observations = Seq::<bool>::empty();
    let callback = |_index: usize| -> bool
        requires false,
    { false };
    assert(observations_agree(callback, observations));
    let required = any_reference_requires(true, 0, callback, Ghost(observations));
    assert(required);
}

}
