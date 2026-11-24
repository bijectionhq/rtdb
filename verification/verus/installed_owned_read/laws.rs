use crate::{Command, Hold, Operation};
use crate::production::{protect_snapshot, release_on_close};
use vstd::prelude::*;

verus! {

fn protected_snapshot_names_its_exact_scope(root: [u8; 32], from: u64, through: u64) {
    let command = protect_snapshot(root, from, through);
    assert(command == (Command::ProtectCheckpoint { root, from, through }));
}

fn close_releases_the_original_hold(hold: Hold) {
    let operation = release_on_close(hold);
    assert(operation == (Operation::Release { hold }));
}

}
