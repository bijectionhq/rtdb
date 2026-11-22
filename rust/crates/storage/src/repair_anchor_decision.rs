#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

pub(crate) enum AnchorInstallation {
    Replace,
    Confirm,
    Refuse,
}

pub(crate) enum RecoveryCollection {
    Keep,
    Remove,
    SynchronizeAndRemove,
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (block >= 0 && boundary >= 0),
))]
pub(crate) fn valid_anchor(block: i64, boundary: i64) -> bool {
    block >= 0 && boundary >= 0
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        (matches!(out, AnchorInstallation::Replace)) == (
            block >= 0 && boundary >= 0 && match prior {
                None => true,
                Some((old_block, old_boundary)) =>
                    old_block < block && old_boundary < boundary
                        && old_boundary >> 48 == boundary >> 48,
            }
        ),
        (matches!(out, AnchorInstallation::Confirm)) == (
            block >= 0 && boundary >= 0
                && prior == Some((block, boundary)) && same_digest
        ),
))]
pub(crate) fn install_anchor(
    prior: Option<(i64, i64)>, block: i64, boundary: i64, same_digest: bool,
) -> AnchorInstallation {
    if !valid_anchor(block, boundary) {
        return AnchorInstallation::Refuse;
    }
    let Some((old_block, old_boundary)) = prior else {
        return AnchorInstallation::Replace;
    };
    if old_block == block && old_boundary == boundary && same_digest {
        AnchorInstallation::Confirm
    } else if old_block < block && old_boundary < boundary && old_boundary >> 48 == boundary >> 48 {
        AnchorInstallation::Replace
    } else {
        AnchorInstallation::Refuse
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        (matches!(out, RecoveryCollection::Keep)) == (
            anchor.is_some() && block >= anchor.unwrap()
        ),
        (matches!(out, RecoveryCollection::Remove)) == anchor.is_none(),
        (matches!(out, RecoveryCollection::SynchronizeAndRemove)) == (
            anchor.is_some() && block < anchor.unwrap()
        ),
))]
pub(crate) fn collect_recovery_file(anchor: Option<i64>, block: i64) -> RecoveryCollection {
    match anchor {
        Some(first) if block >= first => RecoveryCollection::Keep,
        Some(_) => RecoveryCollection::SynchronizeAndRemove,
        None => RecoveryCollection::Remove,
    }
}
