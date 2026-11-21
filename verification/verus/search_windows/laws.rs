use crate::production::{document_range, extend_impact, window_of};
use vstd::prelude::*;

verus! {

// LAW FTS-2-WINDOW-ASSIGNMENT.
pub fn exactly_the_assigned_window_reads_the_document(doc: u32, window: u32)
    requires doc < u32::MAX,
{
    let assigned = window_of(doc);
    let (start, end) = document_range(window);
    assert((assigned == window) == (start <= doc && doc < end));
}

// LAW FTS-2-POSTING-IMPACT.
pub fn impact_covers_both_postings(
    old_frequency: u32, old_length: u32, new_frequency: u32, new_length: u32,
) {
    let (frequency, length) = extend_impact(old_frequency, old_length, new_frequency, new_length);
    assert(old_frequency <= frequency && new_frequency <= frequency
        && length <= old_length && length <= new_length);
}

}
