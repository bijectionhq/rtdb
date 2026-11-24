use std::borrow::{Borrow, Cow};
use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};

use async_trait::async_trait;
use bytes::Bytes;
use rtdb_format::{catalog::{BlockCatalog, CheckpointContents, TableBlock, legacy_table_ref, replay_artefact_file}, envelope::{ReplicaBody, ReplicaEnvelope}, keys::{TableSlug, block_key}, log::proto};
use rtdb_storage::ObjectKey;
use rtdb_types::{Anomaly, BlockIndex, Result, StorageEpoch, StorageVersion, TableRef};

use crate::BlockStore;
use crate::blocks::validate_uploaded_root;
use crate::driver::artefact_entries;

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
    let block = catalog.block_index();
    let mut keys = BTreeSet::from([ObjectKey::new(block_key(block)?)?]);
    for entry in catalog.table_entries()? {
        let table = tables.get(&entry.table).ok_or_else(|| invalid("A checkpoint table has no table block"))?;
        keys.insert(ObjectKey::new(entry.slug.block_key(block)?)?);
        for file in table.checkpoint_trie_files(&entry, CheckpointContents::Replay)? {
            keys.insert(ObjectKey::new(file)?);
        }
    }
    Ok(keys)
}

/// Every object that replaying the admitted suffix onto `base` can read, beyond the base checkpoint's own dependencies.
///
/// An oversized frame's payload is fetched before the driver filters stale records, so it is always included.
/// Deletions and staleness are not applied: they can only shrink what the replay reads.
pub async fn tail_dependencies<E: Borrow<ReplicaEnvelope>>(base: ReplayBase<'_>, suffix: impl IntoIterator<Item = E>,
    sources: &dyn ReplayDependencySources) -> Result<BTreeSet<ObjectKey>> {
    let suffix = suffix.into_iter().collect::<Vec<_>>();
    let mut keys = BTreeSet::new();
    let mut bodies = Vec::with_capacity(suffix.len());
    for envelope in &suffix {
        let envelope = envelope.borrow();
        match envelope.oversized_payload(base.storage_version, base.storage_epoch)? {
            Some(path) => {
                let key = ObjectKey::new(path)?;
                let payload = sources.oversized_payload(&key).await?;
                bodies.push(Cow::Owned(envelope.unwrap_payload(&payload)?.message));
                keys.insert(key);
            }
            None => bodies.push(Cow::Borrowed(&envelope.message)),
        }
    }

    let mut slugs = BTreeMap::<TableRef, BTreeSet<TableSlug>>::new();
    let mut record_slugs = |catalog: &BlockCatalog| -> Result<()> {
        for entry in catalog.table_entries()? { slugs.entry(entry.table).or_default().insert(entry.slug); }
        Ok(())
    };
    if let Some(checkpoint) = base.checkpoint { record_slugs(checkpoint)?; }
    let mut adopted = BTreeMap::new();
    for upload in bodies.iter().filter_map(|body| adopted_upload(&base, body)) {
        let block = BlockIndex(upload.block_index.unwrap_or_default());
        let catalog = match adopted.entry(block) {
            Entry::Occupied(adopted) => adopted.into_mut(),
            Entry::Vacant(slot) => {
                let (catalog, tables) = sources.adopted_checkpoint(block).await?;
                if catalog.block_index() != block { return Err(invalid("An adopted checkpoint has another block index")); }
                keys.append(&mut checkpoint_dependencies(&catalog, &tables)?);
                record_slugs(&catalog)?;
                slot.insert(catalog)
            }
        };
        validate_uploaded_root(catalog, upload, false)?;
    }

    for body in &bodies {
        match body.as_ref() {
            ReplicaBody::BlockUploaded(upload) => {
                if adopted_upload(&base, body).is_some() { added_trie_keys(&slugs, &upload.tries, &mut keys)?; }
            }
            ReplicaBody::TriesAdded(message) => {
                if same_storage(&base, message.storage_version, message.storage_epoch) { added_trie_keys(&slugs, &message.tries, &mut keys)?; }
            }
            ReplicaBody::ArtefactsAdded(message) => {
                if same_storage(&base, message.storage_version, message.storage_epoch)
                    && let Ok(table) = legacy_table_ref(message.table_name.as_deref().unwrap_or_default())
                {
                    for slug in table_slugs(&slugs, &table)? {
                        for (trie, record) in artefact_entries(&message.entries) {
                            if let Some(key) = replay_artefact_file(slug, &trie, &record)? {
                                keys.insert(ObjectKey::new(key)?);
                            }
                        }
                    }
                }
            }
            ReplicaBody::ResolvedTx(_) | ReplicaBody::NoOp(_) | ReplicaBody::BlockBoundary(_)
                | ReplicaBody::TriesDeleted(_) | ReplicaBody::TablesRetired(_) | ReplicaBody::ArtefactsRemoved(_) => {}
            ReplicaBody::OversizedMessage(_) => return Err(invalid("An oversized payload holds another oversized message")),
        }
    }
    Ok(keys)
}

fn adopted_upload<'a>(base: &ReplayBase<'_>, body: &'a ReplicaBody) -> Option<&'a proto::BlockUploaded> {
    let ReplicaBody::BlockUploaded(upload) = body else { return None; };
    let installed = base.checkpoint.map_or(-1, |checkpoint| checkpoint.block_index().0);
    (same_storage(base, upload.storage_version, upload.storage_epoch) && upload.block_index.unwrap_or_default() > installed).then_some(upload)
}

fn same_storage(base: &ReplayBase<'_>, version: Option<i32>, epoch: Option<i32>) -> bool {
    StorageVersion(version.unwrap_or_default()) == base.storage_version && StorageEpoch(epoch.unwrap_or_default()) == base.storage_epoch
}

fn added_trie_keys(slugs: &BTreeMap<TableRef, BTreeSet<TableSlug>>, tries: &[proto::TrieDetails], keys: &mut BTreeSet<ObjectKey>) -> Result<()> {
    for trie in tries {
        let table = legacy_table_ref(trie.table_name.as_deref().unwrap_or_default())?;
        for slug in table_slugs(slugs, &table)? {
            for file in trie.checkpoint_files(slug, CheckpointContents::Replay)? { keys.insert(ObjectKey::new(file)?); }
        }
    }
    Ok(())
}

fn table_slugs<'a>(slugs: &'a BTreeMap<TableRef, BTreeSet<TableSlug>>, table: &TableRef) -> Result<&'a BTreeSet<TableSlug>> {
    slugs.get(table).ok_or_else(|| invalid(format!("No replayed checkpoint records table {}", table.schema_and_table())))
}

fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::fault("rtdb.replication/replay-closure", message) }
