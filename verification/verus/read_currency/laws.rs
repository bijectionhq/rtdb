use crate::production::{confirm, observe};
use vstd::prelude::*;

verus! {

// LAW READ-CURRENCY-PENDING.
pub fn an_older_or_equal_tip_keeps_its_predecessor(observed: i64, prior_tip: i64, at: i64, tip: i64)
    requires tip <= prior_tip,
{
    let pending = Some((observed, prior_tip));
    let found = observe(pending, at, tip);
    assert(found == pending);
}

// LAW READ-CURRENCY-INSTALLED.
pub fn an_uninstalled_tip_cannot_change_the_anchor(
    anchor: Option<i64>, observed_at: i64, tip: i64, installed: i64,
)
    requires installed < tip,
{
    let pending = Some((observed_at, tip));
    let result = confirm(anchor, pending, Some(installed));
    assert(result == (anchor, pending));
}

// LAW READ-CURRENCY-WITNESS.
pub fn reaching_a_new_observation_makes_it_the_anchor(observed_at: i64, tip: i64, installed: i64)
    requires installed >= tip,
{
    let result = confirm(None, Some((observed_at, tip)), Some(installed));
    assert(result == (Some(observed_at), None));
}

// LAW READ-CURRENCY-MONOTONIC.
pub fn a_delayed_observation_cannot_rewind_the_anchor(
    previous: i64, observed_at: i64, tip: i64, installed: i64,
)
    requires observed_at < previous, installed >= tip,
{
    let result = confirm(Some(previous), Some((observed_at, tip)), Some(installed));
    assert(result == (Some(previous), None));
}

}
