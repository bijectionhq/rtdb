#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

const NEGATIVE_ZERO: u64 = 0x8000_0000_0000_0000;
const POSITIVE_INFINITY: u64 = 0x7ff0_0000_0000_0000;

#[cfg(verus_keep_ghost)]
verus! {
    pub open spec fn score_key(bits: u64) -> Option<u64> {
        if bits == 0x8000_0000_0000_0000 { Some(0) }
        else if bits <= 0x7ff0_0000_0000_0000 { Some(bits) }
        else { None }
    }

    pub open spec fn bound_is_lower(bound: Option<u64>, score: u64) -> bool {
        match bound {
            None => true,
            Some(bits) => match score_key(bits) {
                None => false,
                Some(key) => key < score,
            },
        }
    }
}

// The nonnegative binary64 encodings are ordered by their unsigned bits.
// Both signed zeroes compare equal as scores.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == score_key(bits),
))]
fn rank_key(bits: u64) -> Option<u64> {
    if bits == NEGATIVE_ZERO {
        Some(0)
    } else if bits <= POSITIVE_INFINITY {
        Some(bits)
    } else {
        None
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == bound_is_lower(bound, kth),
))]
fn strictly_below(bound: Option<u64>, kth: u64) -> bool {
    match bound {
        None => true,
        Some(bits) => match rank_key(bits) {
            None => false,
            Some(key) => key < kth,
        },
    }
}

/// A scored candidate or an unread window can block stopping at the kth score.
/// A missing bound denotes an empty source. An invalid score or bound refuses the cutoff.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == match score_key(kth) {
        None => false,
        Some(key) => bound_is_lower(pending, key) && bound_is_lower(unread, key),
    },
))]
pub(crate) fn can_stop(kth: u64, pending: Option<u64>, unread: Option<u64>) -> bool {
    match rank_key(kth) {
        None => false,
        Some(key) => strictly_below(pending, key) && strictly_below(unread, key),
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RankKey {
    pub score: f64,
    pub id: Option<i64>,
}

impl RankKey {
    pub fn compare(self, other: Self) -> std::cmp::Ordering {
        self.score_order(other).then_with(|| other.id.cmp(&self.id))
    }

    pub fn score_order(self, other: Self) -> std::cmp::Ordering {
        let score = |score: f64| rank_key(score.to_bits()).unwrap_or(u64::MAX);
        score(self.score).cmp(&score(other.score))
    }

    pub fn beats(self, bound: Self) -> bool {
        let (Some(winner), Some(ceiling)) = (rank_key(self.score.to_bits()), rank_key(bound.score.to_bits())) else {
            return false;
        };
        winner > ceiling || (winner == ceiling && self.id.zip(bound.id).is_some_and(|(id, best)| id < best))
    }
}

#[cfg(test)]
mod tests {
    use super::{RankKey, can_stop};

    #[test]
    fn integer_tie_break_requires_a_strict_proof() {
        let winner = RankKey { score: -0.0, id: Some(-1) };
        assert!(winner.beats(RankKey { score: 0.0, id: Some(0) }));
        assert!(!winner.beats(RankKey { score: 0.0, id: Some(-1) }));
        assert!(!winner.beats(RankKey { score: 0.0, id: None }));
        assert!(!winner.beats(RankKey { score: f64::NAN, id: Some(0) }));
    }

    #[test]
    fn both_frontiers_can_block_the_cutoff() {
        let kth = 1.0f64.to_bits();
        let lower = 0.5f64.to_bits();
        let higher = 2.0f64.to_bits();
        assert!(can_stop(kth, Some(lower), Some(lower)));
        assert!(can_stop(kth, None, None));
        for blocker in [kth, higher] {
            assert!(!can_stop(kth, Some(blocker), None));
            assert!(!can_stop(kth, None, Some(blocker)));
        }
    }

    #[test]
    fn the_cutoff_handles_binary64_boundaries() {
        let zero = 0.0f64.to_bits();
        let negative_zero = (-0.0f64).to_bits();
        let infinity = f64::INFINITY.to_bits();
        assert!(!can_stop(zero, Some(negative_zero), None));
        assert!(!can_stop(negative_zero, None, Some(zero)));
        assert!(can_stop(infinity, Some(zero), None));
        assert!(!can_stop(infinity, None, Some(infinity)));
        for invalid in [f64::NAN.to_bits(), (-1.0f64).to_bits(), f64::NEG_INFINITY.to_bits()] {
            assert!(!can_stop(invalid, None, None));
            assert!(!can_stop(infinity, Some(invalid), None));
            assert!(!can_stop(infinity, None, Some(invalid)));
        }
    }
}
