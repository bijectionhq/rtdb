#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out == (!already_erased && (erase || (match system_upper {
            Some(upper) => system_from < upper,
            None => true,
        }))),
))]
pub(crate) fn applies(already_erased: bool, erase: bool, system_from: i64, system_upper: Option<i64>) -> bool {
    !already_erased && (erase || match system_upper {
        Some(upper) => system_from < upper,
        None => true,
    })
}
