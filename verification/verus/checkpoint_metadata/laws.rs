use crate::production::checkpoint_metadata_collectable;
use vstd::prelude::*;

verus! {

// LAW DUR-2-METADATA-PUBLISHED.
pub fn the_published_checkpoint_is_never_collectable(published: i64) {
    let collect = checkpoint_metadata_collectable(published, Some(published), None);
    assert(!collect);
}

// LAW DUR-2-METADATA-HOLD.
pub fn held_checkpoint_metadata_is_never_collectable(block: i64, oldest: i64, published: i64)
    requires oldest <= block < published,
{
    let collect = checkpoint_metadata_collectable(block, Some(published), Some(oldest));
    assert(!collect);
}

// LAW DUR-2-METADATA-FORWARD.
pub fn metadata_below_both_bounds_can_be_collected(block: i64, oldest: i64, published: i64)
    requires block < oldest, block < published,
{
    let collect = checkpoint_metadata_collectable(block, Some(published), Some(oldest));
    assert(collect);
}

// LAW DUR-2-METADATA-ORIGIN.
pub fn no_checkpoint_allows_no_metadata_collection(block: i64) {
    let collect = checkpoint_metadata_collectable(block, None, None);
    assert(!collect);
}

}
