#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

pub const RECOVERY_BASE: u64 = 0x4000_0000_0000_0000;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out.is_some() == (installed < RECOVERY_BASE - 1),
        out.is_some() ==> out.unwrap() == installed + 1 && out.unwrap() < RECOVERY_BASE,
))]
pub(crate) fn next_compaction_epoch(installed: u64) -> Option<u64> {
    if installed < RECOVERY_BASE - 1 { Some(installed + 1) } else { None }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out.is_some() == (0 <= block && (block as int) < RECOVERY_BASE as int),
        out.is_some() ==> out.unwrap() as int == RECOVERY_BASE as int + block as int && out.unwrap() < 0x8000_0000_0000_0000u64,
))]
pub(crate) fn recovery_file_epoch(block: i64) -> Option<u64> {
    if block >= 0 && (block as u64) < RECOVERY_BASE { Some(RECOVERY_BASE + block as u64) } else { None }
}
