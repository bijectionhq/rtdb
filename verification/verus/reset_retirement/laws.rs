use crate::production::reset_allows_retirement;
use vstd::prelude::*;

verus! {

pub fn a_checkpoint_from_the_pre_reset_epoch_cannot_authorize_release(
    required_block: i64, required_epoch: u64, adopted_block: i64, adopted_epoch: u64,
)
    requires adopted_epoch < required_epoch,
{
    let admitted = reset_allows_retirement(required_block, required_epoch, adopted_block, adopted_epoch, false);
    assert(!admitted);
}

pub fn a_block_before_the_planned_reset_cannot_authorize_release(
    required_block: i64, required_epoch: u64, adopted_block: i64, adopted_epoch: u64,
)
    requires adopted_block < required_block,
{
    let admitted = reset_allows_retirement(required_block, required_epoch, adopted_block, adopted_epoch, false);
    assert(!admitted);
}

pub fn a_retained_name_cannot_be_released(
    required_block: i64, required_epoch: u64, adopted_block: i64, adopted_epoch: u64,
) {
    let admitted = reset_allows_retirement(required_block, required_epoch, adopted_block, adopted_epoch, true);
    assert(!admitted);
}

pub fn a_covering_adoption_authorizes_a_withdrawn_name(
    required_block: i64, required_epoch: u64, adopted_block: i64, adopted_epoch: u64,
)
    requires 0 <= required_block <= adopted_block, 0 < required_epoch <= adopted_epoch,
{
    let admitted = reset_allows_retirement(required_block, required_epoch, adopted_block, adopted_epoch, false);
    assert(admitted);
}

}
