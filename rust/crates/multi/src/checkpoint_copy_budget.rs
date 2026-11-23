#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out.is_some() <==> (plaintext as int) + (overhead as int) <= remaining as int,
        out.is_some() ==> (out.unwrap() as int) + (plaintext as int) + (overhead as int) == remaining as int,
))]
pub(crate) fn remaining_after_sealing(remaining: u64, plaintext: u64, overhead: u64) -> Option<u64> {
    if overhead > remaining { return None; }
    let remaining = remaining - overhead;
    if plaintext > remaining { return None; }
    Some(remaining - plaintext)
}
