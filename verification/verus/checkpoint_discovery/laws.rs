use crate::production::{accepts_published_checkpoint, newer_checkpoint};
use vstd::prelude::*;

verus! {

// LAW DUR-2-CHECKPOINT-NO-ROLLBACK.
pub fn same_or_older_checkpoint_cannot_retry(attempted: i64, listed: i64)
    requires listed <= attempted,
{
    let retry = newer_checkpoint(attempted, Some(listed));
    assert(retry == None);
}

// LAW DUR-2-CHECKPOINT-REPLACEMENT.
pub fn a_newer_checkpoint_can_be_tried(attempted: i64, listed: i64)
    requires attempted < listed,
{
    let retry = newer_checkpoint(attempted, Some(listed));
    assert(retry == Some(listed));
}

// LAW DUR-2-CHECKPOINT-NO-REINITIALIZATION.
pub fn an_empty_listing_cannot_retry(attempted: i64)
{
    let retry = newer_checkpoint(attempted, None);
    assert(retry == None);
}

// LAW DUR-2-ANCHOR-BOUNDARY.
pub fn the_published_block_requires_its_exact_boundary(block: i64, published: i64, stored: i64)
    requires published >= 0, stored != published,
{
    let accepted = accepts_published_checkpoint(block, published, block, Some(stored));
    assert(!accepted);
}

// LAW DUR-2-ANCHOR-ADVANCE.
pub fn a_later_block_requires_a_later_boundary(block: i64, next: i64, published: i64, stored: i64)
    requires next > block, published >= 0, stored <= published,
{
    let accepted = accepts_published_checkpoint(block, published, next, Some(stored));
    assert(!accepted);
}

// LAW DUR-2-ANCHOR-JOURNAL.
pub fn a_candidate_without_a_boundary_in_the_journal_is_refused(block: i64, next: i64, published: i64)
{
    let accepted = accepts_published_checkpoint(block, published, next, None);
    assert(!accepted);
}

// LAW DUR-2-ANCHOR-ACCEPT.
pub fn an_exact_match_and_an_advancing_successor_are_admitted(block: i64, next: i64, published: i64, stored: i64)
    requires published >= 0, next > block, stored > published,
{
    let exact = accepts_published_checkpoint(block, published, block, Some(published));
    assert(exact);
    let newer = accepts_published_checkpoint(block, published, next, Some(stored));
    assert(newer);
}

}
