use crate::Cut;
use crate::production::{checkpoint_next_lsn, retention_actions, trim_target};
use vstd::prelude::*;

verus! {

fn retention_attempts_disposal_before_trim() {
    let actions = retention_actions();
    assert(actions@[0] is DisposeExpired && actions@[1] is RetainSelected && actions@[2] is Publish);
}

fn checkpoint_next_lsn_is_boundary_successor(boundary: i64) {
    let next = checkpoint_next_lsn(boundary);
    assert(match next {
        Some(lsn) => lsn as int == boundary as int + 1,
        None => boundary < -1 || boundary == i64::MAX,
    });
}

fn trim_uses_selected_cut(floor: Cut, selected: Cut) {
    let through = trim_target(floor, selected);
    assert(match through {
        Some(cut) => cut == selected && floor.next_lsn < cut.next_lsn,
        None => selected.next_lsn <= floor.next_lsn,
    });
}

fn native_trim_stops_at_boundary_successor(floor: Cut, selected: Cut, boundary: i64)
    requires selected.next_lsn as int == boundary as int + 1,
{
    if let Some(through) = trim_target(floor, selected) {
        assert(through.next_lsn as int <= boundary as int + 1);
    }
}

}
