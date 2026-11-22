#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(admitted =>
    ensures admitted <==> open && same_residence && same_media,
))]
pub(super) fn may_admit_reference_read(open: bool, same_residence: bool, same_media: bool) -> bool {
    open && same_residence && same_media
}
