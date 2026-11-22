#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(accepted =>
    ensures accepted <==> version@.len() > 0
        && !(version@.len() == 4 && version@[0] == 110 && version@[1] == 117 && version@[2] == 108 && version@[3] == 108),
))]
pub fn valid_object_version(version: &[u8]) -> bool {
    !version.is_empty() && !(version.len() == 4 && version[0] == b'n' && version[1] == b'u' && version[2] == b'l' && version[3] == b'l')
}
