use crate::production::{checkpoint_metadata_admissible, checkpoint_metadata_collectable, checkpoint_pack_collectable};
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

// LAW DUR-2-PACK-ALL-MEMBERS.
pub fn collection_respects_the_bound_for_every_pack_member(
    pack: i64, member: i64, youngest: i64, published: i64, oldest: i64,
)
    requires member <= youngest, oldest <= member,
{
    let collect = checkpoint_pack_collectable(pack, youngest, Some(published), Some(oldest));
    assert(!collect);
}

// LAW DUR-2-PACK-PROGRESS.
pub fn fully_obsolete_checkpoint_metadata_can_be_collected(
    pack: i64, youngest: i64, published: i64, oldest: i64,
)
    requires pack < published, youngest < published, pack < oldest, youngest < oldest,
{
    let collect = checkpoint_pack_collectable(pack, youngest, Some(published), Some(oldest));
    assert(collect);
}

// LAW DUR-2-CAPTURE-CURRENT.
pub fn current_checkpoint_can_be_admitted(block: i64) {
    let admit = checkpoint_metadata_admissible(block, Some(block), None);
    assert(admit);
}

// LAW DUR-2-CAPTURE-HANDOFF.
pub fn a_protected_older_checkpoint_can_be_admitted(block: i64, published: i64, first: i64)
    requires first <= block < published,
{
    let admit = checkpoint_metadata_admissible(block, Some(published), Some(first));
    assert(admit);
}

// LAW DUR-2-CAPTURE-PAST.
pub fn an_unheld_older_checkpoint_is_refused(block: i64, published: i64)
    requires block < published,
{
    let admit = checkpoint_metadata_admissible(block, Some(published), None);
    assert(!admit);
}

// LAW DUR-2-CAPTURE-FUTURE.
pub fn a_future_checkpoint_cannot_inherit_protection(block: i64, published: i64, first: i64)
    requires first <= published < block,
{
    let admit = checkpoint_metadata_admissible(block, Some(published), Some(first));
    assert(!admit);
}

}
