//! Which log records and block files use a feature XTDB does not implement, and so are framed for XTDB to refuse.
//! Every field the fork adds to a message XTDB shares is classified here or named in `tests/xtdb_refusal.rs` as an
//! annotation that XTDB loses nothing by ignoring.
//! Behavioural spec: dev/doc/db.allium (XtdbRefusesRtdbExtensionRecords).

use rtdb_types::TableRef;

use super::{ADMITTED_OFFLOAD_STORAGE, ReplicaBody, SourceBody};
use crate::generated::xtdb::block::proto as block_pb;
use crate::keys::TrieKey;
use crate::log::proto::{ResolvedTx, TrieDetails, Tx};

/// A source record XTDB would otherwise skip or apply without the feature it carries:
/// a native rtdb.v1 transaction, an artefact record, a transaction with an RTDB-only field,
/// or a tries notification naming an RTDB-only trie.
pub fn source_uses_extension(body: &SourceBody) -> bool {
    match body {
        SourceBody::ServingTransact(_) | SourceBody::ArtefactsAdded(_) | SourceBody::ArtefactsRemoved(_) => true,
        SourceBody::Tx(tx) => tx_uses_extension(tx),
        SourceBody::TriesAdded(added) => added.tries.iter().any(trie_uses_extension),
        SourceBody::FlushBlock(_) | SourceBody::AttachDatabase(_) | SourceBody::DetachDatabase(_) => false,
    }
}

/// A transaction with an idempotency, replay or bounded-retry guard, or a placed attempt seed.
pub fn tx_uses_extension(tx: &Tx) -> bool {
    tx.idempotency_guard.is_some() || tx.replay_submission.is_some() || tx.bounded_retry.is_some()
        || tx.placed_attempt_seed.is_some()
}

/// A replica record XTDB would otherwise skip or misapply: a table retirement, an artefact record,
/// a resolved transaction or no-op carrying an RTDB-only field or writing an RTDB-only table,
/// a trie of a later naming epoch, of an RTDB-only table or with artefacts, the deletion of such a trie or of an L0 trie,
/// a block whose catalog names its compaction outputs in a later epoch, or an offload bound to an object publication.
pub fn replica_uses_extension(body: &ReplicaBody) -> bool {
    match body {
        ReplicaBody::TablesRetired(_) | ReplicaBody::ArtefactsAdded(_) | ReplicaBody::ArtefactsRemoved(_) => true,
        ReplicaBody::ResolvedTx(tx) => resolved_uses_extension(tx),
        ReplicaBody::NoOp(noop) => noop.placed_slot_reservation.is_some() || noop.serving_record.is_some()
            || noop.clock_lease_micros.is_some(),
        ReplicaBody::TriesAdded(added) => added.tries.iter().any(trie_uses_extension),
        ReplicaBody::TriesDeleted(deleted) => deleted.table_name.as_deref().is_some_and(rtdb_only_table)
            || deleted.trie_keys.iter().any(|key| rtdb_only_trie_key(key, |key| key.level() == 0)),
        ReplicaBody::BlockUploaded(upload) => upload.naming_epoch.is_some_and(|epoch| epoch > 0)
            || upload.tries.iter().any(trie_uses_extension),
        ReplicaBody::OversizedMessage(oversized) => oversized.publication.is_some()
            || oversized.storage_version == Some(ADMITTED_OFFLOAD_STORAGE),
        ReplicaBody::BlockBoundary(_) => false,
    }
}

fn resolved_uses_extension(tx: &ResolvedTx) -> bool {
    tx.placed_attempt.is_some() || tx.acceptance_stage.is_some() || tx.search_delta.is_some()
        || tx.placed_slot_terminal.is_some() || tx.table_data.keys().any(|table| rtdb_only_table(table))
}

/// A block catalog XTDB would open without the feature it records: a later naming epoch, a serving checkpoint,
/// or a table XTDB does not have.
pub fn block_uses_extension(block: &block_pb::Block) -> bool {
    block.naming_epoch.is_some_and(|epoch| epoch > 0)
        || block.serving_checkpoint.is_some() || block.serving_checkpoint_object.is_some()
        || block.tables.iter().any(|entry| rtdb_only_table_ref(&TableRef::new(
            entry.schema_name.as_deref().unwrap_or_default(), entry.table_name.as_deref().unwrap_or_default())))
        || block.table_names.iter().any(|name| rtdb_only_table(name))
}

/// A table block XTDB would open without the feature it records: search runs, term or entity directories,
/// a relation retirement, or an RTDB-only trie.
pub fn table_block_uses_extension(table: &block_pb::TableBlock) -> bool {
    !table.search_runs.is_empty() || !table.retired_search_runs.is_empty() || table.retired_as_of_micros.is_some()
        || !table.term_directories.is_empty() || !table.retired_term_directories.is_empty() || table.entity_directory.is_some() || !table.retired_entity_directories.is_empty()
        || table.tries.iter().chain(table.partitions.iter().flat_map(|partition| &partition.tries)).any(trie_uses_extension)
}

/// A trie of a later naming epoch or an unparseable key, of an RTDB-only table, or carrying derived artefacts.
fn trie_uses_extension(trie: &TrieDetails) -> bool {
    !trie.artefacts.is_empty()
        || trie.table_name.as_deref().is_some_and(rtdb_only_table)
        || trie.trie_key.as_deref().is_some_and(|key| rtdb_only_trie_key(key, |_| false))
}

/// A trie key of a later naming epoch, one `also` picks out, or one that does not parse: XTDB parses every trie key it reads,
/// so it must refuse a record naming a key it cannot parse rather than be let through by default.
fn rtdb_only_trie_key(key: &str, also: impl FnOnce(&TrieKey) -> bool) -> bool {
    key.parse::<TrieKey>().map_or(true, |key| key.epoch() != 0 || also(&key))
}

/// A `schema/table` name, as log records spell it, of a table only RTDB writes.
fn rtdb_only_table(name: &str) -> bool {
    let (schema, table) = name.split_once('/').unwrap_or(("public", name));
    rtdb_only_table_ref(&TableRef::new(schema, table))
}

/// The engine's transaction-metadata tables and the owned (rtdb.v1) schemas.
fn rtdb_only_table_ref(table: &TableRef) -> bool {
    table.stores_expiring_transaction_metadata() || TableRef::owned_schema(&table.schema)
}
