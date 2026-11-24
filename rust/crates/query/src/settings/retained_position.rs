#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(selected => ensures
    selected == if requested >= 0
        && (captured matches Some(position) && requested <= position)
        && (inherited matches Some(position) ==> requested <= position)
    { Some(requested) } else { None },
))]
pub(super) fn admit(captured: Option<i64>, inherited: Option<i64>, requested: i64) -> Option<i64> {
    let captured = captured?;
    if requested < 0 || requested > captured {
        return None;
    }
    if let Some(prior) = inherited {
        if requested > prior {
            return None;
        }
    }
    Some(requested)
}
