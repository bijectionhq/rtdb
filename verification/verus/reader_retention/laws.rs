use crate::production::{any_reader_requires, observations_agree};
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
    let required = any_reader_requires(count, callback, Ghost(values@));
    assert(required);
}

}
