use crate::production::{advances_frontier, keeps_epoch, sample_is_fresh};
use vstd::prelude::*;

verus! {

pub fn a_stalled_publisher_cannot_refresh(previous: i64, expiry: i64, observed: i64)
    requires observed <= previous,
{
    let refreshed = advances_frontier(previous, expiry, true, observed);
    assert(!refreshed);
}

pub fn takeover_requires_the_exclusive_prior_expiry(previous: i64, expiry: i64, observed: i64)
    requires previous < expiry,
{
    let published = advances_frontier(previous, expiry, false, observed);
    assert(published == (observed >= expiry));
    if published {
        let kept = keeps_epoch(expiry, false, observed);
        assert(!kept);
        assert(observed > previous);
    }
}

pub fn a_future_or_expired_sample_cannot_decide(at: i64, now: i64, expiry: i64) {
    let fresh = sample_is_fresh(at, now, expiry);
    if now < at || now >= expiry {
        assert(!fresh);
    }
}

}
