#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(not(verus_keep_ghost), derive(Clone, Copy, PartialEq, Eq))]
pub(super) enum VersionStatus {
    Absent,
    WouldExposeOlderData,
    SafeToRemove,
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == if target_latest is None { VersionStatus::Absent }
        else if target_latest == Some(true) && other_data { VersionStatus::WouldExposeOlderData }
        else { VersionStatus::SafeToRemove },
))]
pub(super) fn exact_version_status(target_latest: Option<bool>, other_data: bool) -> VersionStatus {
    match target_latest {
        None => VersionStatus::Absent,
        Some(true) if other_data => VersionStatus::WouldExposeOlderData,
        Some(_) => VersionStatus::SafeToRemove,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (same_address && same_condition),
))]
pub(super) fn matches_terminal_target(same_address: bool, same_condition: bool) -> bool {
    same_address && same_condition
}
