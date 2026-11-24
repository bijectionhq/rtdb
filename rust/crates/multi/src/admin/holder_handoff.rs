#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (predecessor < settled && settled == observed),
))]
pub(super) fn exact_holder_handoff(predecessor: u64, settled: u64, observed: u64) -> bool {
    predecessor < settled && settled == observed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_later_holding_on_the_same_member_cannot_confirm_an_earlier_move() {
        assert!(exact_holder_handoff(17, 19, 19));
        assert!(!exact_holder_handoff(17, 19, 21));
        assert!(!exact_holder_handoff(17, 17, 17));
    }
}
