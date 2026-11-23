#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == !installed,
))]
pub(crate) fn directory_route_allowed(installed: bool) -> bool {
    !installed
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == !(installed && moving),
))]
pub(crate) fn directory_move_park_allowed(installed: bool, moving: bool) -> bool {
    !(installed && moving)
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (directory_holding && !protected && resident),
))]
pub(crate) fn pressure_candidate(directory_holding: bool, protected: bool, resident: bool) -> bool {
    directory_holding && !protected && resident
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn move_park_refuses_an_installed_name() {
        assert!(!directory_move_park_allowed(true, true));
        assert!(directory_move_park_allowed(true, false));
    }

    #[test]
    fn pressure_skips_journal_holdings() {
        assert!(!pressure_candidate(false, false, true));
        assert!(pressure_candidate(true, false, true));
    }
}
