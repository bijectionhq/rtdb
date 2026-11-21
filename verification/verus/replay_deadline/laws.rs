use crate::production::replay_cutoff;
use vstd::prelude::*;

#[verus_spec(out =>
    requires first < later && later < latest,
    ensures out == first,
)]
fn two_later_writers_share_the_first_cutoff(first: u64, later: u64, latest: u64) -> u64 {
    let deadline = replay_cutoff(None, first);
    let replacement = replay_cutoff(Some(deadline), later);
    replay_cutoff(Some(replacement), latest)
}
