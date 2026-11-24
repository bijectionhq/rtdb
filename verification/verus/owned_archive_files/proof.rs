#![feature(proc_macro_hygiene)]

use vstd::prelude::*;

#[path = "../../../rust/crates/multi/src/archive/inventory_identity.rs"]
mod production;
#[path = "../../../rust/crates/index/src/trie_membership.rs"]
mod membership;

verus! {

/// The accepted stage manifest and selected transaction of a reopened
/// checkpoint must be those used to inspect the inventory.
fn reopened_stage_matches_inspection(
    manifest: &[u8],
    observed_manifest: &[u8],
    checkpoint_at: i64,
    recovered_id: i64,
    recovered_at: i64,
    snapshot: Option<(i64, i64)>,
) -> (exact: bool)
    ensures exact ==> observed_manifest@ == manifest@
        && snapshot == Some((recovered_id, checkpoint_at))
        && recovered_id >= 0,
{
    production::matches_bytes(observed_manifest, manifest)
        && production::stage_transaction_matches(checkpoint_at, recovered_id, recovered_at, snapshot)
}

/// Digests identify the sealed object within the accepted manifest. The
/// hash and the authenticated decryption belong to the archive provider.
fn opened_object_has_manifest_identity(
    name: &[u8],
    observed_name: &[u8],
    bytes: u64,
    observed_bytes: u64,
    digest: &[u8],
    observed_digest: &[u8],
) -> (exact: bool)
    ensures exact == (observed_name@ == name@
        && observed_bytes == bytes
        && observed_digest@ == digest@),
{
    observed_bytes == bytes
        && production::matches_bytes(observed_name, name)
        && production::matches_bytes(observed_digest, digest)
}

/// Each successful write advances one exact slot of the same live trie
/// vector whose length the relation header advertises.
fn owned_file_stream_step<T>(selected: &[T], index: usize) -> (entry: Option<(&T, usize)>)
    requires index <= selected@.len(),
    ensures
        entry is None <==> index == selected@.len(),
        entry matches Some((file, next)) ==> *file == selected@[index as int]
            && next == index + 1 <= selected@.len(),
{
    production::next_owned_file(selected, index)
}

fn archive_trie_membership<T: Copy, At: Copy>(inventory: &[(T, membership::TrieState<At>)])
    -> (selected: Vec<T>)
    ensures
        selected@ == inventory@.filter(|entry: (T, membership::TrieState<At>)| entry.1 is Live)
            .map_values(|entry: (T, membership::TrieState<At>)| entry.0),
{
    membership::select_live(inventory)
}

} // verus!

fn main() {}
