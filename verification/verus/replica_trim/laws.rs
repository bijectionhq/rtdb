use crate::production::{excluded_replay_start, replica_trim_through};
use vstd::prelude::*;

verus! {

// LAW DUR-2-TRIM-FIRST-RETAINED.
pub fn a_cursor_may_start_on_the_first_retained_record(through: i64)
    requires 0 <= through < i64::MAX,
{
    let result = excluded_replay_start(through, through);
    assert(result.is_none());
}

// LAW DUR-2-TRIM-PENDING.
pub fn an_authorized_trim_excludes_every_required_record_it_covers(after: i64, through: i64)
    requires -1 <= after < through < i64::MAX,
{
    let result = excluded_replay_start(after, through);
    assert(result.is_some());
    assert(result.unwrap().0 as int == after as int + 1);
    assert(result.unwrap().1 as int == through as int + 1);
}

// LAW DUR-2-TRIM-REPLAY-HOLD.
pub fn the_earliest_required_record_caps_the_trim(checkpoint: i64, first: i64)
    requires 0 < first < checkpoint,
{
    let through = replica_trim_through(checkpoint, first);
    assert(through.is_some() && through.unwrap() as int + 1 == first as int);
}

// LAW DUR-2-TRIM-CHECKPOINT.
pub fn the_checkpoint_caps_the_trim_when_a_hold_is_later(checkpoint: i64, first: i64)
    requires 0 < checkpoint <= first,
{
    let through = replica_trim_through(checkpoint, first);
    assert(through.is_some() && through.unwrap() as int + 1 == checkpoint as int);
}

}
