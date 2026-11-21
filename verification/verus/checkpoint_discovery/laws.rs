use crate::production::newer_checkpoint;
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

}
