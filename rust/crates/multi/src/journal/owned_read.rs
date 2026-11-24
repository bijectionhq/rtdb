#![allow(unexpected_cfgs)]

#[cfg(not(verus_keep_ghost))]
use ordered_logkeeper::ordered::{Command, Hold, service::wire::Operation};
#[cfg(verus_keep_ghost)]
use crate::{Command, Hold, Operation};
#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(command =>
    ensures command == (Command::ProtectCheckpoint { root, from, through }),
))]
pub(super) fn protect_snapshot(root: [u8; 32], from: u64, through: u64) -> Command {
    Command::ProtectCheckpoint { root, from, through }
}

#[cfg_attr(verus_keep_ghost, verus_spec(operation =>
    ensures operation == (Operation::Release { hold }),
))]
pub(super) fn release_on_close(hold: Hold) -> Operation {
    Operation::Release { hold }
}
