//! Which log records use a feature XTDB does not implement, and so are framed for XTDB to refuse.
//! Behavioural spec: dev/doc/db.allium (XtdbRefusesRtdbExtensionRecords).

use super::{ReplicaBody, SourceBody};
use crate::keys::TrieKey;

/// A source record XTDB would otherwise skip or apply without the guard it carries:
/// a native rtdb.v1 transaction, a transaction with an idempotency, replay or bounded-retry guard,
/// or a tries notification naming a trie of a later naming epoch.
pub fn source_uses_extension(body: &SourceBody) -> bool {
    match body {
        SourceBody::ServingTransact(_) => true,
        SourceBody::Tx(tx) => tx.idempotency_guard.is_some() || tx.replay_submission.is_some() || tx.bounded_retry.is_some(),
        SourceBody::TriesAdded(added) => added.tries.iter().any(|trie| names_epoch_trie(trie.trie_key.as_deref())),
        _ => false,
    }
}

/// A replica record XTDB would otherwise skip or misapply:
/// a table retirement, a trie of a later naming epoch, the deletion of an L0 trie,
/// or a block whose catalog names its compaction outputs in a later epoch.
pub fn replica_uses_extension(body: &ReplicaBody) -> bool {
    match body {
        ReplicaBody::TablesRetired(_) => true,
        ReplicaBody::TriesAdded(added) => added.tries.iter().any(|trie| names_epoch_trie(trie.trie_key.as_deref())),
        ReplicaBody::TriesDeleted(deleted) => deleted.trie_keys.iter().any(|key| {
            key.parse::<TrieKey>().is_ok_and(|key| key.epoch() != 0 || key.level() == 0)
        }),
        ReplicaBody::BlockUploaded(upload) => upload.naming_epoch.is_some_and(|epoch| epoch > 0)
            || upload.tries.iter().any(|trie| names_epoch_trie(trie.trie_key.as_deref())),
        _ => false,
    }
}

fn names_epoch_trie(key: Option<&str>) -> bool {
    key.and_then(|key| key.parse::<TrieKey>().ok()).is_some_and(|key| key.epoch() != 0)
}
