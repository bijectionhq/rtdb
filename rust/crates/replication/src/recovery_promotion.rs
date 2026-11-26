#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(can_promote => ensures
    can_promote == (recognized_prefix && image_complete && may_lead && !already_leading && holds_grant),
))]
pub(super) fn may_promote(
    recognized_prefix: bool, image_complete: bool, may_lead: bool, already_leading: bool, holds_grant: bool,
) -> bool {
    recognized_prefix && image_complete && may_lead && !already_leading && holds_grant
}

#[cfg_attr(verus_keep_ghost, verus_spec(published => ensures
    published == if recognized_prefix && image_complete { completed_through } else { None },
))]
pub(super) fn publish_installed_prefix(
    recognized_prefix: bool, completed_through: Option<i64>, image_complete: bool,
) -> Option<i64> {
    if recognized_prefix && image_complete {
        completed_through
    } else {
        None
    }
}

#[derive(Clone, Copy)]
pub(super) enum WriterActivation {
    Recovering,
    Active,
}

#[cfg_attr(verus_keep_ghost, verus_spec(activation => ensures
    activation is Active <==> !fenced,
))]
pub(super) fn writer_activation(fenced: bool) -> WriterActivation {
    if fenced { WriterActivation::Recovering } else { WriterActivation::Active }
}

impl WriterActivation {
    #[cfg_attr(verus_keep_ghost, verus_spec(active => ensures active == (self is Active)))]
    pub(super) fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }

    /// Recovery writes can run before activation. Source resolution and the
    /// public writer role wait for read-back and content-complete installation
    /// of the authenticated prefix (including every control row and receipt).
    #[cfg_attr(verus_keep_ghost, verus_spec(next => ensures
        next is Active <==> self is Active || (confirmed && recognized && image_complete && installed == Some(received)),
    ))]
    pub(super) fn installed(
        self, confirmed: bool, recognized: bool, image_complete: bool, received: i64, installed: Option<i64>,
    ) -> Self {
        if self.is_active() || (confirmed && recognized && image_complete && installed == Some(received)) {
            Self::Active
        } else {
            Self::Recovering
        }
    }
}
