use std::borrow::{Borrow, Cow};
use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use bytes::Bytes;
use rtdb_format::{catalog::{BlockCatalog, CheckpointContents, TableBlock, legacy_table_ref, replay_artefact_file}, envelope::{ReplicaBody, ReplicaEnvelope}, keys::{TableSlug, block_key}, log::proto};
use rtdb_storage::ObjectKey;
use rtdb_types::{Anomaly, BlockIndex, Result, StorageEpoch, StorageVersion, TableRef};

use crate::BlockStore;
use crate::blocks::validate_uploaded_root;
use crate::driver::artefact_entries;

#[macro_use]
#[path = "replay_closure_algorithm.rs"]
mod replay_closure_algorithm;

macro_rules! each_replay_input {
    ($keys:ident, $items:expr, $item:ident => $body:block, [$($spec:tt)*]) => {
        for $item in $items $body
    };
}

type Slugs = BTreeMap<TableRef, BTreeSet<TableSlug>>;
type AdoptedCatalogs = BTreeMap<BlockIndex, BlockCatalog>;

/// The installed checkpoint that a replica suffix replays onto, and the storage root its objects live under.
#[derive(Clone, Copy, Debug)]
pub struct ReplayBase<'a> {
    pub storage_version: StorageVersion,
    pub storage_epoch: StorageEpoch,
    pub checkpoint: Option<&'a BlockCatalog>,
}

/// Reads the objects a replica suffix names before its dependencies can be enumerated.
#[async_trait]
pub trait ReplayDependencySources: Send + Sync {
    async fn oversized_payload(&self, key: &ObjectKey) -> Result<Bytes>;
    async fn adopted_checkpoint(&self, block: BlockIndex) -> Result<(BlockCatalog, BTreeMap<TableRef, TableBlock>)>;
}

/// Reads payloads under this store's root, not from a delivery's replica-object custody scope.
#[async_trait]
impl ReplayDependencySources for BlockStore {
    async fn oversized_payload(&self, key: &ObjectKey) -> Result<Bytes> { Ok(self.object_store().get_fresh(key).await?) }
    async fn adopted_checkpoint(&self, block: BlockIndex) -> Result<(BlockCatalog, BTreeMap<TableRef, TableBlock>)> { self.load(block).await }
}

/// Every object that replaying onto this checkpoint can read: its root, its table blocks, and the trie files and artefacts they name.
pub fn checkpoint_dependencies(catalog: &BlockCatalog, tables: &BTreeMap<TableRef, TableBlock>) -> Result<BTreeSet<ObjectKey>> {
    checkpoint_files(catalog, tables, CheckpointContents::Replay)
}

/// Enumerates the physical files admitted by the specified checkpoint route.
pub(crate) fn checkpoint_files(
    catalog: &BlockCatalog, tables: &BTreeMap<TableRef, TableBlock>, contents: CheckpointContents,
) -> Result<BTreeSet<ObjectKey>> {
    checkpoint_dependency_keys!(catalog, tables, contents; each_replay_input)
}

/// Every object that replaying the admitted suffix onto `base` can read, beyond the base checkpoint's own dependencies.
///
/// An oversized frame's payload is fetched before the driver filters stale records, so it is always included.
/// Deletions and staleness are not applied: they can only shrink what the replay reads.
pub async fn tail_dependencies<E: Borrow<ReplicaEnvelope>>(base: ReplayBase<'_>, suffix: impl IntoIterator<Item = E>,
    sources: &dyn ReplayDependencySources) -> Result<BTreeSet<ObjectKey>> {
    let suffix = suffix.into_iter().collect::<Vec<_>>();
    macro_rules! each_resolved {
        ($keys:ident, $bodies:ident, $items:expr, $item:ident => $body:block) => { for $item in $items $body };
    }
    macro_rules! each_adopted {
        ($keys:ident, $slugs:ident, $adopted:ident, $items:expr, $base:ident, $item:ident => $body:block) => { for $item in $items $body };
    }
    macro_rules! each_body {
        ($keys:ident, $slugs:ident, $items:expr, $item:ident => $body:block, [$($spec:tt)*]) => { for $item in $items $body };
    }
    macro_rules! borrow_envelope { ($item:ident) => { $item.borrow() }; }
    macro_rules! read_payload { ($key:expr) => { sources.oversized_payload($key).await }; }
    macro_rules! resolve {
        ($envelope:ident, $base:ident, $keys:ident, $bodies:ident) => {
            oversized_dependency!($envelope, $base, $keys, $bodies, read_payload)
        };
    }
    macro_rules! read_catalog { ($block:expr) => { sources.adopted_checkpoint($block).await }; }
    tail_dependency_keys!(base, suffix, base.checkpoint;
        each_resolved, each_adopted, each_body, borrow_envelope, resolve, read_catalog)
}

fn record_slugs(slugs: &mut Slugs, catalog: &BlockCatalog) -> Result<()> {
    macro_rules! insert {
        ($slugs:ident, $entry:ident) => { $slugs.entry($entry.table).or_default().insert($entry.slug) };
    }
    record_catalog_slugs!(slugs, catalog; each_replay_input, insert)
}

fn adopt_catalog(catalog: &BlockCatalog, tables: &BTreeMap<TableRef, TableBlock>, keys: &mut BTreeSet<ObjectKey>, block: BlockIndex) -> Result<()> {
    adopt_dependency!(catalog, tables, keys, block);
    Ok(())
}

fn record_dependency_keys(base: ReplayBase<'_>, body: &ReplicaBody, slugs: &Slugs, keys: &mut BTreeSet<ObjectKey>) -> Result<()> {
    record_dependency_keys!(base, body, slugs, keys; each_replay_input);
    Ok(())
}

fn upload_block(upload: &proto::BlockUploaded) -> BlockIndex { BlockIndex(upload.block_index.unwrap_or_default()) }

fn adopted_upload<'a>(base: &ReplayBase<'_>, body: &'a ReplicaBody) -> Option<&'a proto::BlockUploaded> {
    let installed = base.checkpoint.map_or(-1, |checkpoint| checkpoint.block_index().0);
    new_upload!(base, body, installed)
}

fn same_storage(base: &ReplayBase<'_>, version: Option<i32>, epoch: Option<i32>) -> bool {
    matching_replay_storage!(version, epoch, base.storage_version.0, base.storage_epoch.0)
}

fn added_trie_keys(slugs: &Slugs, tries: &[proto::TrieDetails], keys: &mut BTreeSet<ObjectKey>) -> Result<()> {
    added_trie_dependency_keys!(slugs, tries, keys; each_replay_input)
}

fn insert_object_keys(keys: &mut BTreeSet<ObjectKey>, files: BTreeSet<String>) -> Result<()> {
    checked_object_keys!(keys, files; each_replay_input)
}

fn legacy_table_ref_from_wire(name: &Option<String>) -> Result<TableRef> {
    legacy_table_ref(name.as_deref().unwrap_or_default())
}

fn table_slugs<'a>(slugs: &'a Slugs, table: &TableRef) -> Result<&'a BTreeSet<TableSlug>> {
    slugs.get(table).ok_or_else(|| invalid(format!("No replayed checkpoint records table {}", table.schema_and_table())))
}

fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::fault("rtdb.replication/replay-closure", message) }
