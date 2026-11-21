use crate::production::confirms_removal;
use vstd::prelude::*;

verus! {

// LAW RETIRE-RECEIPT-RECOGNIZED.
pub fn an_unrecognized_prefix_refuses(through: i64) {
    let confirmed = confirms_removal(false, through, through, false);
    assert(!confirmed);
}

// LAW RETIRE-RECEIPT-INSTALLED.
pub fn a_prefix_before_the_removal_refuses(installed: i64, removal: i64)
    requires installed < removal,
{
    let confirmed = confirms_removal(true, installed, removal, false);
    assert(!confirmed);
}

// LAW RETIRE-RECEIPT-EXCLUSION.
pub fn a_required_trie_refuses(through: i64) {
    let confirmed = confirms_removal(true, through, through, true);
    assert(!confirmed);
}

// LAW RETIRE-RECEIPT-INCLUSIVE.
pub fn an_installed_removal_confirms(through: i64) {
    let confirmed = confirms_removal(true, through, through, false);
    assert(confirmed);
}

}
