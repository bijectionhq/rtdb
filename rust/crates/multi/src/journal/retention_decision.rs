use ordered_logkeeper::ordered::Cut;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RetentionAction {
    DisposeExpired,
    RetainSelected,
    Publish,
}

pub(crate) fn retention_actions() -> [RetentionAction; 3] {
    [
        RetentionAction::DisposeExpired,
        RetentionAction::RetainSelected,
        RetentionAction::Publish,
    ]
}

pub(super) fn checkpoint_next_lsn(boundary: i64) -> Option<u64> {
    match boundary.checked_add(1) {
        Some(offset) if offset >= 0 => Some(offset as u64),
        _ => None,
    }
}

pub(super) fn trim_target(floor: Cut, selected: Cut) -> Option<Cut> {
    if floor.next_lsn < selected.next_lsn {
        Some(selected)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_attempts_disposal_before_both_opportunities_to_trim() {
        assert_eq!(
            retention_actions(),
            [
                RetentionAction::DisposeExpired,
                RetentionAction::RetainSelected,
                RetentionAction::Publish
            ],
        );
    }

    #[test]
    fn trim_uses_the_exact_boundary_successor_cut() {
        let successor = checkpoint_next_lsn(41).unwrap();
        let floor = Cut {
            next_lsn: 40,
            history: [1; 32],
        };
        let selected = Cut {
            next_lsn: successor,
            history: [2; 32],
        };
        assert_eq!(trim_target(floor, selected), Some(selected));
        assert_eq!(trim_target(selected, selected), None);
        assert_eq!(checkpoint_next_lsn(i64::MAX), None);
        assert_eq!(checkpoint_next_lsn(-2), None);
        assert_eq!(checkpoint_next_lsn(-1), Some(0));
    }
}
