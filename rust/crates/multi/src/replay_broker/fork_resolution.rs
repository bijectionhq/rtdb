pub(super) fn may_check_fork_outcome(preparation_held: bool) -> bool {
    !preparation_held
}

pub(super) fn settled_fork_outcome(has_stage: bool, deleted: bool) -> Option<bool> {
    if !has_stage {
        Some(true)
    } else if deleted {
        Some(false)
    } else {
        None
    }
}
