use rtdb_directory::{DeploymentRecord, Directory, Lifecycle};
use rtdb_format::{catalog::BlockCatalog, keys::{TrieKey, block_key}};
use rtdb_s3::{NeverVersionedRetirement, ObservedTerminalObject};
use rtdb_storage::{Error, ObjectKey, ObjectStore};
use rtdb_types::{BlockIndex, PhysicalObjectVersion, Result, StorageEpoch};

use crate::archive::{self, ArchivedCheckpoint};
use crate::cipher::Cipher;
use crate::deployment::CellStorage;
use crate::keys::KeyService;
use crate::packing::{PACK_DIRECTORY, PackEntry, PackIndex, block_pack, observe_pack_index, pack_block, read_pack_index_with};
use crate::reference::{Base, References, key};

use super::checkpoint_metadata_block;

mod hosted;
pub(super) use hosted::reap_hosted;

const MAXIMUM_MEMBER_BYTES: u64 = 64 * 1024 * 1024;

pub(super) fn eligible(record: &DeploymentRecord) -> bool {
    let Some(checkpoint) = record.checkpoint.filter(|checkpoint| checkpoint.root_sha256.is_some()) else { return false; };
    let Some(capture) = record.serving_replay_capture.and_then(|id| record.checkpoint_captures.get(&id))
        else { return false; };
    record.lifecycle == Lifecycle::Live
        && record.holder.is_none() && record.parent.is_none() && record.move_from.is_none()
        && record.storage_regions.is_empty() && record.catalog_readers.is_empty()
        && capture.checkpoint == checkpoint && capture.media_bound && record.recovery_manifest_sha256.is_some()
        && record.replica_anchor.as_ref().is_some_and(|anchor| {
            anchor.root_incarnation == record.incarnation && anchor.interval[0] >= 0
                && anchor.interval[0] < anchor.interval[1] && anchor.interval[1] < checkpoint.boundary
        })
}

pub(super) async fn has_older_pack(bucket: &dyn ObjectStore, record: &DeploymentRecord) -> Result<bool> {
    let Some(checkpoint) = record.checkpoint.filter(|_| eligible(record)) else { return Ok(false); };
    if !archive::has_local_replay_archive(record)? { return Ok(false); }
    let prefix = format!("{}/{}/", record.storage_prefix(), archive::checkpoint_storage_root()?);
    Ok(bucket.list(Some(&key(&format!("{prefix}{PACK_DIRECTORY}"))?)).await?.iter().any(|pack| {
        pack.key.as_str().strip_prefix(&prefix).and_then(pack_block).is_some_and(|block| block < checkpoint.block)
    }))
}

pub(super) async fn reap(
    directory: &dyn Directory,
    storage: &CellStorage,
    keys: &dyn KeyService,
    bucket: &dyn ObjectStore,
    record: &DeploymentRecord,
    references: &References,
    threshold: i64,
) -> Result<usize> {
    let store = format!("{}/{}", record.storage_prefix(), archive::checkpoint_storage_root()?);
    if !Base::load(bucket, &store).await?.objects.is_empty() { return Ok(0); }
    let Some(checkpoint) = record.checkpoint else { return Ok(0); };
    let prefix = format!("{store}/");
    let mut packs = bucket.list(Some(&key(&format!("{prefix}{PACK_DIRECTORY}"))?)).await?;
    packs.sort_unstable_by(|a, b| b.key.cmp(&a.key));
    let published = block_pack(&block_key(BlockIndex(checkpoint.block))?);
    if !packs.iter().any(|pack| pack.key.as_str().strip_prefix(&prefix) == Some(&published)) {
        return Ok(0);
    }
    let candidates = packs.into_iter().filter_map(|pack| {
        let block = pack_block(pack.key.as_str().strip_prefix(&prefix)?)?;
        (block < checkpoint.block && !references.has_declared_reference(&pack.key)).then_some((block, pack.key))
    }).collect::<Vec<_>>();
    if candidates.is_empty() { return Ok(0); }

    Ok(archive::with_replay_archive(directory, storage, keys, record, None, |admitted, _owner, archive| async move {
        let Some(anchor) = record.replica_anchor.as_deref() else { return Ok(0); };
        if record.required_replica_offsets(rtdb_directory::now_ms()).any(|first| first <= anchor.interval[1]) {
            return Ok(0);
        }
        let Some(material) = keys.existing_key(&record.key_id()).await? else { return Ok(0); };
        let cipher = Cipher::new(&material)?;
        let mut erased = 0;
        for (block, pack) in candidates {
            let observed = ObservedPack::raw(bucket, &pack).await?;
            let Some(boundary) = covered_pack(&observed, block, threshold, record.log_epoch, &cipher, &archive).await?
                else { continue; };
            if !crate::checkpoint_retention::replay_archive_covers_pack(block, checkpoint.block, boundary,
                anchor.interval[0], anchor.interval[1], checkpoint.boundary)
            {
                continue;
            }
            if !directory.deployment(&record.id).await?.is_some_and(|latest| latest.version == admitted.version) {
                break;
            }
            // New serving admissions start at the published cut and carry its archive.
            // Raw copy operations still need the maintenance lock and lineage reservation held by this sweep.
            bucket.delete(&pack).await?;
            erased += 1;
        }
        Ok(erased)
    }).await?.unwrap_or(0))
}

async fn covered_pack(
    pack: &ObservedPack<'_>, block: i64, threshold: i64, log_epoch: i32,
    cipher: &Cipher, archive: &ArchivedCheckpoint,
) -> Result<Option<i64>> {
    let index = &pack.index;
    if index.written_at_ms.is_none_or(|written| written > threshold) { return Ok(None); }
    let root = block_key(BlockIndex(block))?;
    let Some(entry) = index.entries.get(&root).filter(|entry| entry.length <= MAXIMUM_MEMBER_BYTES)
        else { return Ok(None); };
    let bytes = pack.read_member(entry).await?;
    let catalog = BlockCatalog::decode(&cipher.open(&root, &bytes)?)?;
    if catalog.block_index().0 != block { return Ok(None); }
    let Some(boundary) = catalog.boundary_replica_position() else { return Ok(None); };
    let boundary = boundary.after_offset(StorageEpoch(log_epoch));
    if boundary < 0 { return Ok(None); }

    let mut has_l0 = false;
    for (name, entry) in &index.entries {
        if checkpoint_metadata_block(name) == Some(block) { continue; }
        if !is_block_l0(name, block) || entry.length > MAXIMUM_MEMBER_BYTES { return Ok(None); }
        let original = pack.read_member(entry).await?;
        if !archive.matches_original(&key(name)?, &original).await? { return Ok(None); }
        has_l0 = true;
    }
    Ok(has_l0.then_some(boundary))
}

struct ObservedPack<'a> {
    key: &'a ObjectKey,
    index: PackIndex,
    storage: PackStorage<'a>,
}

enum PackStorage<'a> {
    Raw { bucket: &'a dyn ObjectStore, version: Option<PhysicalObjectVersion> },
    Terminal { retirement: &'a NeverVersionedRetirement<'a>, publication: ObservedTerminalObject },
}

impl<'a> ObservedPack<'a> {
    async fn raw(bucket: &'a dyn ObjectStore, key: &'a ObjectKey) -> rtdb_storage::Result<Self> {
        let observed = observe_pack_index(bucket, key).await?;
        Ok(Self { key, index: observed.value, storage: PackStorage::Raw { bucket, version: observed.version } })
    }

    async fn terminal(retirement: &'a NeverVersionedRetirement<'a>, key: &'a ObjectKey) -> Result<Option<Self>> {
        let Some(publication) = retirement.observe_readable(key).await? else { return Ok(None); };
        let read = |range| retirement.read_observed_range(key, &publication, range);
        let index = match read_pack_index_with(key, publication.size(), MAXIMUM_MEMBER_BYTES as usize, read).await {
            Err(error) if error.code == "rtdb.storage/message-too-large" => return Ok(None),
            result => result?,
        };
        Ok(Some(Self { key, index, storage: PackStorage::Terminal { retirement, publication } }))
    }

    async fn read_member(&self, entry: &PackEntry) -> Result<bytes::Bytes> {
        let range = entry.offset..entry.offset + entry.length;
        match &self.storage {
            PackStorage::Raw { bucket, version } => {
                let observed = bucket.observe_range(self.key, range).await?;
                if observed.version.as_ref() != version.as_ref() || observed.value.len() as u64 != entry.length {
                    return Err(Error::ConflictingObject(self.key.to_string()).into());
                }
                Ok(observed.value)
            }
            PackStorage::Terminal { retirement, publication } => retirement.read_observed_range(self.key, publication, range).await,
        }
    }
}

fn is_block_l0(name: &str, block: i64) -> bool {
    let Some((_, file)) = name.strip_prefix("tables/").and_then(|name| name.split_once('/')) else { return false; };
    file.strip_prefix("data/").or_else(|| file.strip_prefix("meta/"))
        .and_then(|file| file.strip_suffix(".arrow"))
        .and_then(|name| name.parse::<TrieKey>().ok())
        .is_some_and(|trie| trie.level() == 0 && trie.block_index().0 == block)
}
