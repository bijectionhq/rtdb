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

    pub open spec fn uuid_bound_loses(bound: Option<(u64, Option<u128>)>, score: u64, iid: u128) -> bool {
        match bound {
            None => true,
            Some((bits, first)) => match score_key(bits) {
                None => false,
                Some(key) => key < score || (key == score && match first {
                    None => false,
                    Some(first) => iid < first,
                }),
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
pub fn can_stop(kth: u64, pending: Option<u64>, unread: Option<u64>) -> bool {
    match rank_key(kth) {
        None => false,
        Some(key) => strictly_below(pending, key) && strictly_below(unread, key),
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == uuid_bound_loses(bound, kth, iid),
))]
fn worse_than_uuid(bound: Option<(u64, Option<u128>)>, kth: u64, iid: u128) -> bool {
    match bound {
        None => true,
        Some((score, first)) => match rank_key(score) {
            None => false,
            Some(key) => key < kth || (key == kth && match first {
                Some(first) => iid < first,
                None => false,
            }),
        },
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == match score_key(kth) {
        None => false,
        Some(key) => uuid_bound_loses(pending, key, iid) && uuid_bound_loses(unread, key, iid),
    },
))]
pub(crate) fn can_stop_with_uuid(
    kth: u64, iid: u128, pending: Option<(u64, Option<u128>)>, unread: Option<(u64, Option<u128>)>,
) -> bool {
    match rank_key(kth) {
        None => false,
        Some(key) => worse_than_uuid(pending, key, iid) && worse_than_uuid(unread, key, iid),
    }
}

#[cfg(not(verus_keep_ghost))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum RankIdentity {
    Integer(i64),
    Uuid([u8; 16]),
}

#[cfg(not(verus_keep_ghost))]
#[derive(Clone, Copy, Debug)]
pub struct RankKey {
    pub score: f64,
    id: Option<RankIdentity>,
}

#[cfg(not(verus_keep_ghost))]
impl RankKey {
    pub fn integer(score: f64, id: Option<i64>) -> Self {
        Self { score, id: id.map(RankIdentity::Integer) }
    }

    pub fn uuid(score: f64, iid: [u8; 16]) -> Self {
        Self { score, id: Some(RankIdentity::Uuid(iid)) }
    }

    pub fn has_id(self) -> bool { self.id.is_some() }

    pub fn integer_id(self) -> Option<i64> {
        match self.id { Some(RankIdentity::Integer(id)) => Some(id), _ => None }
    }

    pub fn score_only(self) -> Self { Self { id: None, ..self } }

    pub fn compare(self, other: Self) -> std::cmp::Ordering {
        self.score_order(other).then_with(|| other.id.cmp(&self.id))
    }

    pub fn score_order(self, other: Self) -> std::cmp::Ordering {
        let score = |score: f64| rank_key(score.to_bits()).unwrap_or(u64::MAX);
        score(self.score).cmp(&score(other.score))
    }

    pub fn beats(self, bound: Self) -> bool {
        let score = self.score.to_bits();
        let ceiling = bound.score.to_bits();
        match (self.id, bound.id) {
            (Some(RankIdentity::Uuid(id)), other) => {
                let floor = match other { Some(RankIdentity::Uuid(best)) => Some(u128::from_be_bytes(best)), _ => None };
                can_stop_with_uuid(score, u128::from_be_bytes(id), Some((ceiling, floor)), None)
            }
            (Some(RankIdentity::Integer(id)), Some(RankIdentity::Integer(best))) => {
                let (Some(score), Some(ceiling)) = (rank_key(score), rank_key(ceiling)) else { return false; };
                score > ceiling || (score == ceiling && id < best)
            }
            _ => can_stop(score, Some(ceiling), None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RankKey, can_stop};

    #[test]
    fn integer_tie_break_requires_a_strict_proof() {
        let winner = RankKey::integer(-0.0, Some(-1));
        assert!(winner.beats(RankKey::integer(0.0, Some(0))));
        assert!(!winner.beats(RankKey::integer(0.0, Some(-1))));
        assert!(!winner.beats(RankKey::integer(0.0, None)));
        assert!(!winner.beats(RankKey::integer(f64::NAN, Some(0))));
    }

    #[test]
    fn uuid_tie_break_uses_the_complete_unsigned_identity() {
        let mut first = [0; 16];
        first[15] = 255;
        let mut next = [0; 16];
        next[14] = 1;
        let winner = RankKey::uuid(-0.0, first);
        let bound = RankKey::uuid(0.0, next);
        assert!(winner.compare(bound).is_gt());
        assert!(winner.beats(bound));
        assert!(!winner.beats(RankKey::uuid(0.0, first)));
        assert!(!winner.beats(RankKey::integer(0.0, None)));
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
