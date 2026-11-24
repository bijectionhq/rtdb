#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum VersionStatus {
    Absent,
    WouldExposeOlderData,
    SafeToRemove,
}

pub(super) fn exact_version_status(target_latest: Option<bool>, other_data: bool) -> VersionStatus {
    match target_latest {
        None => VersionStatus::Absent,
        Some(true) if other_data => VersionStatus::WouldExposeOlderData,
        Some(_) => VersionStatus::SafeToRemove,
    }
}

pub(super) fn matches_terminal_target(same_address: bool, same_condition: bool) -> bool {
    same_address && same_condition
}
