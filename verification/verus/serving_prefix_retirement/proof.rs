#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/multi/src/journal/prefix_retirement.rs"]
mod retirement;

use retirement::{leases_expired, may_retire, superseded};
use vstd::prelude::*;

verus! {

fn the_current_or_a_later_grant_is_never_superseded(epoch: u64, writer: u64)
    requires epoch >= writer, epoch < u64::MAX,
{
    let later = superseded(epoch, writer, None);
    let current = superseded(epoch, epoch + 1, Some(epoch));
    assert(!later && !current);
}

fn a_fenced_earlier_grant_is_superseded(epoch: u64, writer: u64, current: u64)
    requires epoch < writer, current != epoch,
{
    let fenced = superseded(epoch, writer, Some(current));
    assert(fenced);
}

fn leases_are_outstanding_until_the_whole_horizon_after_the_fence(observed: i64, horizon: i64, now: i64)
    requires horizon >= 0, observed <= i64::MAX - horizon, now < observed + horizon,
{
    let expired = leases_expired(now, observed, horizon);
    assert(!expired);
}

fn an_unbounded_observation_never_expires(now: i64, horizon: i64)
    requires horizon > 0,
{
    let expired = leases_expired(now, i64::MAX, horizon);
    assert(!expired);
}

fn any_missing_piece_of_evidence_refuses(a: bool, b: bool, c: bool, d: bool, e: bool, f: bool, g: bool, h: bool) {
    let unfenced = may_retire(false, b, c, d, e, f, g, h);
    let foreign = may_retire(a, false, c, d, e, f, g, h);
    let genesis = may_retire(a, b, false, d, e, f, g, h);
    let untrimmed = may_retire(a, b, c, false, e, f, g, h);
    let retained = may_retire(a, b, c, d, true, f, g, h);
    let preparing = may_retire(a, b, c, d, e, true, g, h);
    let writing = may_retire(a, b, c, d, e, f, false, h);
    let unqualified = may_retire(a, b, c, d, e, f, g, false);
    assert(!unfenced && !foreign && !genesis && !untrimmed && !retained && !preparing && !writing && !unqualified);
}

}

fn main() {}
