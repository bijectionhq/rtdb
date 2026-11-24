use crate::production::{may_check_fork_outcome, settled_fork_outcome};
use vstd::prelude::*;

verus! {

fn retained_preparation_cannot_release(preparation_held: bool) {
    let check = may_check_fork_outcome(preparation_held);
    assert(check == !preparation_held);
}

spec fn settled_fork_outcome_reference(published: bool, disposed: bool) -> Option<bool> {
    if published {
        Some(true)
    } else if disposed {
        Some(false)
    } else {
        None
    }
}

fn release_requires_a_settled_outcome(has_stage: bool, deleted: bool) {
    let outcome = settled_fork_outcome(has_stage, deleted);
    assert(outcome == settled_fork_outcome_reference(!has_stage, deleted));
}

}
