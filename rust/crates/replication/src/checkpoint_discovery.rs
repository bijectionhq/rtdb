#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out == match listed {
            Some(index) if index > attempted => Some(index),
            _ => None,
        },
        out.is_some() ==> (i64::MAX as int) - (out.unwrap() as int)
            < (i64::MAX as int) - (attempted as int),
))]
pub(crate) fn newer_checkpoint(attempted: i64, listed: Option<i64>) -> Option<i64> {
    match listed {
        Some(index) if index > attempted => Some(index),
        _ => None,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out <==> published_boundary >= 0 && match loaded_boundary {
        Some(boundary) => (loaded_block == published_block && boundary == published_boundary)
            || (loaded_block > published_block && boundary > published_boundary),
        None => false,
    },
))]
pub(crate) fn accepts_published_checkpoint(
    published_block: i64, published_boundary: i64, loaded_block: i64, loaded_boundary: Option<i64>,
) -> bool {
    published_boundary >= 0 && match loaded_boundary {
        Some(boundary) if loaded_block == published_block => boundary == published_boundary,
        Some(boundary) => loaded_block > published_block && boundary > published_boundary,
        None => false,
    }
}
