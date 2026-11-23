use crate::production::{next_compaction_epoch, recovery_file_epoch};
use vstd::prelude::*;

verus! {

pub fn recovered_pairs_never_use_a_compactor_output_epoch(current: u64, block: i64) {
    let compactor = next_compaction_epoch(current);
    let recovered = recovery_file_epoch(block);
    if let (Some(normal), Some(staged)) = (compactor, recovered) {
        assert(normal < staged);
    }
}

pub fn different_installation_blocks_do_not_share_recovery_names(first: i64, second: i64)
    requires first < second,
{
    let old = recovery_file_epoch(first);
    let new = recovery_file_epoch(second);
    if let (Some(prior), Some(later)) = (old, new) {
        assert(prior < later);
    }
}

pub fn replay_pair_names_fit_the_persisted_signed_epoch(block: i64) {
    if let Some(epoch) = recovery_file_epoch(block) {
        assert(epoch < 0x8000_0000_0000_0000u64);
    }
}

}
