use crate::production::{checkpoint_metadata_admissible, checkpoint_metadata_collectable, checkpoint_pack_collectable, replay_archive_covers_pack};
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

pub fn a_replay_archive_can_cover_an_older_pack_inside_the_certified_prefix(
    pack: i64, published: i64, boundary: i64, first: i64, through: i64, published_boundary: i64,
)
    requires pack < published, 0 <= first <= boundary < through < published_boundary,
{
    let covered = replay_archive_covers_pack(pack, published, boundary, first, through, published_boundary);
    assert(covered);
}

pub fn a_replay_archive_never_retires_the_servable_boundary_or_the_published_pack(
    pack: i64, published: i64, boundary: i64, first: i64, through: i64, published_boundary: i64,
)
    requires pack >= published || boundary >= through || through >= published_boundary || boundary < first,
{
    let covered = replay_archive_covers_pack(pack, published, boundary, first, through, published_boundary);
    assert(!covered);
}

pub fn a_retirable_pack_cannot_be_newly_admitted_as_a_historical_checkpoint(
    pack: i64, published: i64, boundary: i64, first: i64, through: i64, published_boundary: i64,
) {
    let covered = replay_archive_covers_pack(pack, published, boundary, first, through, published_boundary);
    let admit = checkpoint_metadata_admissible(pack, Some(published), None);
    assert(covered ==> !admit);
}

}
