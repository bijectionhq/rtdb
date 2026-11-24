use vstd::prelude::*;

use crate::production;

verus! {

pub fn retained_control_keeps_exact_position(
    captured: Option<i64>, inherited: Option<i64>, requested: i64,
) -> (selected: Option<i64>)
    ensures
        selected matches Some(at) ==> {
            &&& at == requested
            &&& at >= 0
            &&& captured matches Some(head) && at <= head
            &&& (inherited matches Some(prior) ==> at <= prior)
        },
        0 <= requested && (captured matches Some(head) && requested <= head)
            && (inherited matches Some(prior) ==> requested <= prior)
            ==> selected == Some(requested),
{
    production::admit(captured, inherited, requested)
}

pub fn retained_control_refuses_to_widen_a_tied_band() {
    let selected = production::admit(Some(12), Some(10), 11);
    assert(selected is None);
    let original = production::admit(Some(12), Some(10), 10);
    assert(original == Some(10i64));
}

}
