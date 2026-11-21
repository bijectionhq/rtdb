use crate::production::missing_replay_offset;
use vstd::prelude::*;

verus! {

// LAW DUR-2-REPLAY-FLOOR.
pub fn replay_can_start_at_the_first_retained_record(after: i64, floor: i64)
    requires after as int + 1 == floor as int,
{
    let missing = missing_replay_offset(after, floor);
    assert(missing.is_none());
}

// LAW DUR-2-REPLAY-GAP.
pub fn a_trimmed_prefix_reports_its_first_required_record(after: i64, floor: i64)
    requires after as int + 1 < floor as int,
{
    let missing = missing_replay_offset(after, floor);
    assert(missing.is_some() && missing.unwrap() as int == after as int + 1);
}

// LAW DUR-2-REPLAY-ORIGIN.
pub fn empty_checkpoint_with_untrimmed_log_can_replay() {
    let missing = missing_replay_offset(-1, 0);
    assert(missing.is_none());
}

// LAW DUR-2-REPLAY-EXHAUSTION.
pub fn an_exhausted_offset_cannot_be_below_a_retained_prefix(floor: i64) {
    let missing = missing_replay_offset(i64::MAX, floor);
    assert(missing.is_none());
}

}
