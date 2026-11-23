pub(crate) fn directory_route_allowed(installed: bool) -> bool {
    !installed
}

pub(crate) fn directory_move_park_allowed(installed: bool, moving: bool) -> bool {
    !(installed && moving)
}

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
