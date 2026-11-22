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

/// A past root can inherit protection only from a hold that already covers it.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out <==> published.is_some() && block <= published.unwrap()
        && (block == published.unwrap() || (oldest.is_some() && oldest.unwrap() <= block)),
))]
pub(crate) fn checkpoint_metadata_admissible(block: i64, published: Option<i64>, oldest: Option<i64>) -> bool {
    let Some(published) = published else { return false; };
    block == published || (block < published && match oldest {
        Some(first) => first <= block,
        None => false,
    })
}


/// Packs carry one recoverable checkpoint and may cover other checkpoint
/// metadata. All covered blocks must lie below both retention bounds.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out <==> published.is_some() && pack < published.unwrap() && youngest < published.unwrap()
        && (oldest.is_none() || (pack < oldest.unwrap() && youngest < oldest.unwrap())),
))]
pub(crate) fn checkpoint_pack_collectable(pack: i64, youngest: i64, published: Option<i64>, oldest: Option<i64>) -> bool {
    checkpoint_metadata_collectable(pack, published, oldest)
        && checkpoint_metadata_collectable(youngest, published, oldest)
}
