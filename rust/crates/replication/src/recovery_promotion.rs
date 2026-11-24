#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(can_promote => ensures
    can_promote == (recognized_prefix && may_lead && !already_leading && holds_grant),
))]
pub(super) fn may_promote(
    recognized_prefix: bool, may_lead: bool, already_leading: bool, holds_grant: bool,
) -> bool {
    recognized_prefix && may_lead && !already_leading && holds_grant
}
