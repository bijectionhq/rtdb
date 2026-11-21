#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out <==> published.is_some() && block < published.unwrap()
            && (oldest.is_none() || block < oldest.unwrap()),
))]
pub(crate) fn checkpoint_metadata_collectable(block: i64, published: Option<i64>, oldest: Option<i64>) -> bool {
    let Some(published) = published else { return false; };
    match oldest {
        Some(oldest) => block < published && block < oldest,
        None => block < published,
    }
}
