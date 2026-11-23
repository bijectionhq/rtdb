//! Immutable, ciphertext-only captures of published checkpoints.
use std::{
    collections::BTreeMap,
    ops::Range,
    path::Path,
    sync::Arc,
};

use base64::{
    engine::general_purpose::URL_SAFE_NO_PAD,
    Engine,
};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use rtdb_directory::{
    ArchiveRetirement,
    Checkpoint,
    CheckpointCapture,
    CheckpointRecovery,
    DeploymentRecord,
    Directory,
    Lifecycle,
};
use rtdb_format::{
    catalog::{
        BlockCatalog,
        CheckpointContents,
        TableBlock,
        publication_digest,
    },
    keys::{
        block_key,
        StorageRoot,
        CURRENT_STORAGE_VERSION,
    },
};
use rtdb_storage::{
    object_store::StoredObject,
    Error,
    IoRegion,
    ObjectKey,
    ObjectObservation,
    ObjectStore,
};
use rtdb_types::{
    Anomaly,
    AnomalyCategory,
    BlockIndex,
    PhysicalObjectVersion,
    ReplicaPosition,
    Result,
    StorageEpoch,
};
use serde::{
    Deserialize,
    Serialize,
};
use uuid::Uuid;

use crate::{
    admin::{administration_stream, await_storage_drain, update_installation},
    cipher::Cipher,
    deployment::CellStorage,
    keys::KeyService,
    packing::PackDiscovery,
    reference::{PrefixedStore, key},
    store::{
        EncryptingStore, ROOT_MARKER, open_deployment_store, open_deployment_store_with_archive,
        wall_clock,
    },
};

mod stage;
mod reader;
pub use stage::stage_checkpoint;
pub(crate) use stage::discard_checkpoint_stage;
mod retirement;
mod replica_artifact;
mod replica_suffix;
mod replay_handoff;
pub(crate) use replica_suffix::{capture as capture_replica_suffix, find_held_publication};
mod read_world;
mod writable_world;
pub(crate) use writable_world::prepare_checkpoint_world_overlay;
pub(crate) use writable_world::{finish_checkpoint_replay_parents, prepare_checkpoint_replay_parent};
pub(crate) use writable_world::retain_replayed_checkpoint_world;
pub(crate) use writable_world::{commit_checkpoint_world_batch, validate_checkpoint_world, fork_checkpoint_world_selected, HeldCheckpointWorld, WorldArchive, open_held_checkpoint_world};
pub(crate) use writable_world::{WorldFact, WorldPoint};
pub use read_world::{CheckpointPoint, CheckpointReadResources, CheckpointReadWorld, read_checkpoint_owned_point};
pub(crate) use retirement::{finish_capture_retirement, obsolete_fork_input, obsolete_world_suffix, require_capture_open, retire_capture, retire_obsolete_fork_input, retire_world_suffix};
pub use reader::{StagedReaderSettings, serve_staged_checkpoint};
pub(crate) use reader::{activate_checkpoint_transfer_target, seed_checkpoint_transfer_target, verify_checkpoint_transfer_target, StageOwnedRelations, StageSelector, inspect_staged_owned_relations, stream_staged_owned_event_files, tunnel_staged_checkpoint};

const MAXIMUM_BYTES: u64 = 1 << 30;
const MAXIMUM_MANIFEST_BYTES: u64 = 16 << 20;

pub(crate) fn checkpoint_storage_root() -> Result<String> {
    Ok(StorageRoot::new(CURRENT_STORAGE_VERSION, StorageEpoch(0))?.key())
}

fn checkpoint_transfer_prefix(record: &DeploymentRecord, operation: Uuid) -> String {
    format!("{}/checkpoint-transfers/{operation}/v2", record.storage_prefix())
}

pub(crate) fn admits_checkpoint_transfer_object(
    record: &DeploymentRecord, stage: &rtdb_directory::CheckpointStage, name: &str,
) -> bool {
    let prefix = checkpoint_transfer_prefix(record, stage.intent.operation);
    if name == format!("{prefix}/manifest") { return true; }
    if ["segments", "payloads"].into_iter().any(|kind| {
        name.strip_prefix(&format!("{prefix}/{kind}/")).is_some_and(|suffix| {
            suffix.parse::<i64>().is_ok_and(|first| {
                first > stage.checkpoint.boundary && suffix == format!("{first:020}")
            })
        })
    }) {
        return true;
    }
    checkpoint_storage_root().ok().is_some_and(|root| {
        name.strip_prefix(&format!("{}/{root}/", record.storage_prefix()))
            .is_some_and(|relative| admits_seeded_transfer_payload(record, stage, relative))
    })
}

fn admits_seeded_transfer_payload(
    record: &DeploymentRecord, stage: &rtdb_directory::CheckpointStage, relative: &str,
) -> bool {
    record.checkpoint_transfer_replica.as_deref().is_some_and(|replica| {
        record.checkpoint_transfer_bootstrap.as_deref() == Some(&replica.bootstrap)
            && &replica.bootstrap.terminal.fence.destination == stage
    }) && relative.strip_prefix("oversized/").filter(|file| !file.contains('/'))
        .and_then(rtdb_format::keys::oversized_block_index)
        .is_some_and(|block| block.0 >= stage.checkpoint.block)
}

fn digest(ciphertext: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(ring::digest::digest(&ring::digest::SHA256, ciphertext))
}

async fn read_object(
    files: &dyn ObjectStore,
    name: &ObjectKey,
    object: &ArchivedObject,
) -> rtdb_storage::Result<Bytes> {
    if files.head(name).await?.size != object.bytes {
        return Err(Error::ConflictingObject(name.to_string()));
    }
    let ciphertext = files.get_range(name, 0..object.bytes).await?;
    if !object.matches_sealed(name, &ciphertext) {
        return Err(Error::ConflictingObject(name.to_string()));
    }
    Ok(ciphertext)
}

async fn observe_archive_object(
    files: &dyn ObjectStore,
    name: &ObjectKey,
    object: &ArchivedObject,
) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
    let length = usize::try_from(object.bytes).map_err(|_| Error::ConflictingObject(name.to_string()))?;
    let ciphertext = files.observe_fresh_exact(name, length).await?;
    if !object.matches_sealed(name, &ciphertext.value) {
        return Err(Error::ConflictingObject(name.to_string()));
    }
    Ok(ciphertext)
}

/// The accepted manifest supplies the immutable objects covered by a repair.
/// Other reads continue through the current serving store.
pub(crate) struct ArchivedCheckpoint {
    files: PrefixedStore,
    audience_transport: bool,
    objects: BTreeMap<ObjectKey, ArchivedObject>,
    archive_cipher: Cipher,
    serving_cipher: Cipher,
}

/// Select the repair in the same directory observation that authorizes its I/O.
fn completed_repair(record: &DeploymentRecord) -> Result<Option<&CheckpointRecovery>> {
    let repair = record.checkpoint_recovery.as_deref().filter(|repair| repair.completed_at_ms.is_some());
    if let Some(repair) = repair {
        require_capture_open(record, repair.operation)?;
    }
    Ok(repair)
}

#[derive(PartialEq, Eq)]
struct ServingArchive<'a> {
    point: CapturePoint,
    archive: &'a str,
    manifest: &'a str,
}

impl ServingArchive<'_> {
    fn media(&self) -> Result<CellStorage> {
        serde_json::from_str(self.archive).map_err(|_| inconsistent("Invalid retained archive binding"))
    }
}

fn serving_archive(record: &DeploymentRecord) -> Result<Option<ServingArchive<'_>>> {
    if let Some(operation) = record.serving_replay_capture {
        require_capture_open(record, operation)?;
        let capture = record.checkpoint_captures.get(&operation)
            .filter(|capture| capture.media_bound)
            .ok_or_else(|| inconsistent("The serving replay source has no retained capture"))?;
        let deployment = record.id.parse().map_err(|_| inconsistent("Invalid retained archive owner"))?;
        let point = capture_point(record, deployment, operation, &capture.archive)?;
        if !same_incarnation(record, &point) {
            return Err(inconsistent("The serving replay source belongs to another incarnation"));
        }
        return Ok(Some(ServingArchive {
            point,
            archive: &capture.archive,
            manifest: capture.manifest_sha256.as_deref().ok_or_else(|| inconsistent("The serving replay source has no accepted manifest"))?,
        }));
    }
    let Some(recovery) = completed_repair(record)? else { return Ok(None); };
    let deployment = record.id.parse().map_err(|_| inconsistent("Invalid retained archive owner"))?;
    let point = capture_point(record, deployment, recovery.operation, &recovery.archive)?;
    if !same_incarnation(record, &point) || point.checkpoint != recovery.checkpoint {
        return Err(inconsistent("The recovery archive differs from its accepted checkpoint"));
    }
    Ok(Some(ServingArchive { point, archive: &recovery.archive, manifest: &recovery.manifest_sha256 }))
}

pub(crate) fn serving_archive_operation(record: &DeploymentRecord) -> Result<Option<Uuid>> {
    Ok(serving_archive(record)?.map(|source| source.point.operation))
}

pub(crate) fn has_local_replay_archive(record: &DeploymentRecord) -> Result<bool> {
    if record.serving_replay_capture.is_none() { return Ok(false); }
    let Some(selected) = serving_archive(record)? else { return Ok(false); };
    Ok(matches!(selected.media()?, CellStorage::Local { .. }))
}

pub(crate) async fn with_local_replay_archive<T, F, Fut>(
    directory: &dyn Directory,
    storage: &CellStorage,
    keys: &dyn KeyService,
    record: &DeploymentRecord,
    run: F,
) -> Result<Option<T>>
where
    F: FnOnce(DeploymentRecord, ArchivedCheckpoint) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let Some(operation) = record.serving_replay_capture else { return Ok(None); };
    let Some(selected) = serving_archive(record)? else { return Ok(None); };
    if record.checkpoint != Some(selected.point.checkpoint) { return Ok(None); }
    let archive = selected.media()?;
    if !matches!(archive, CellStorage::Local { .. }) || same_storage(storage, &archive) || storage.overlaps(&archive)? {
        return Ok(None);
    }
    stage::with_region(
        directory, keys, &record.id, &archive, None, Some((operation, None)), None,
        |current| {
            if current.version != record.version || serving_archive(current)?.as_ref() != Some(&selected) {
                return Err(unavailable("The installation changed before opening its replay archive"));
            }
            Ok(None)
        },
        |admitted, owner, io| async move {
            let archive = ArchivedCheckpoint::load(&admitted, keys, owner, io, false).await?
                .ok_or_else(|| inconsistent("The installation lost its retained replay archive"))?;
            run(admitted, archive).await.map(Some)
        },
    ).await
}

impl ArchivedCheckpoint {
    pub(crate) async fn load(
        record: &DeploymentRecord,
        keys: &dyn KeyService,
        owner: Uuid,
        region: Arc<IoRegion>,
        audience: bool,
    ) -> Result<Option<Self>> {
        let Some(source) = serving_archive(record)? else { return Ok(None); };
        let operation = source.point.operation;
        if !record.storage_regions.get(&owner).is_some_and(|hold| hold.admits_archive_read(operation)) {
            return Err(inconsistent("Opening the serving archive requires its own storage admission"));
        }
        let archive = source.media()?;
        let audience_transport = audience && matches!(&archive, CellStorage::S3 { .. });
        let bucket = archive.open_bucket(region, audience_transport).await?;
        let prefix = format!("checkpoint-captures/{}/{operation}", record.id);
        let scope = PrefixedStore::new(bucket.clone(), prefix.clone());
        let capture = read::<Capture>(&scope, &key("capture.json")?)
            .await?
            .ok_or_else(|| unavailable("The recovery archive is unavailable"))?;
        if capture.point != source.point || capture.manifest_sha256()? != source.manifest
            || (record.serving_replay_capture.is_some() && capture.format != CheckpointContents::Replay)
        {
            return Err(inconsistent(
                "The recovery archive differs from its accepted manifest",
            ));
        }
        let objects = capture
            .objects
            .into_iter()
            .map(|object| Ok((key(&object.name)?, object)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(Some(Self {
            files: PrefixedStore::new(bucket, format!("{prefix}/objects")),
            audience_transport,
            objects,
            archive_cipher: Cipher::new(&archive_key(keys, &capture.point).await?)?,
            serving_cipher: Cipher::new(&keys.existing_key(&record.key_id()).await?
                .ok_or_else(|| unavailable("The current installation key is unavailable"))?)?,
        }))
    }

    pub(crate) fn supports_audience_reads(&self) -> bool { self.audience_transport }

    async fn read_plaintext(&self, name: &ObjectKey, object: &ArchivedObject) -> rtdb_storage::Result<Bytes> {
        let ciphertext = read_object(&self.files, name, object).await?;
        self.archive_cipher.open_object(name, &ciphertext)
    }

    async fn seal_for_serving(&self, name: &ObjectKey, object: &ArchivedObject) -> rtdb_storage::Result<Bytes> {
        let plaintext = self.read_plaintext(name, object).await?;
        self.serving_cipher.seal_object(name, &plaintext)
    }

    async fn observe_frame_range(&self, name: &ObjectKey, object: &ArchivedObject, range: Range<u64>) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
        use crate::cipher::object::{FramedObject, SEALED_HEADER_BYTES, sealed_size};

        let header_range = 0..SEALED_HEADER_BYTES as u64;
        let header = self.files.observe_range(name, header_range.clone()).await?;
        if object.frame_header_sha256.as_ref().is_none_or(|expected| *expected != digest(&header.value)) {
            return Err(Error::ConflictingObject(name.to_string()));
        }
        let layout = FramedObject::open_header(&self.archive_cipher, name, &header.value)?;
        if sealed_size(name, layout.length())? as u64 != object.bytes {
            return Err(Error::ConflictingObject(name.to_string()));
        }
        if range == header_range {
            return Ok(ObjectObservation { value: layout.seal_header(&self.serving_cipher, name)?, version: header.version });
        }

        let frames = layout.selected_frames(name, &range)?;
        let archived = self.files.observe_range(name, range.clone()).await?;
        if archived.version != header.version {
            return Err(Error::ConflictingObject(name.to_string()));
        }
        let plaintext = layout.open_frames(&self.archive_cipher, name, frames.clone(), &archived.value)?;
        let mut value = Vec::with_capacity(archived.value.len());
        layout.seal_frames(&self.serving_cipher, name, frames.start, &plaintext, &mut value)?;
        Ok(ObjectObservation { value: value.into(), version: archived.version })
    }

    pub(crate) async fn matches_original(&self, name: &ObjectKey, original: &Bytes) -> rtdb_storage::Result<bool> {
        let Some(object) = self.objects.get(name) else { return Ok(false); };
        let plaintext = match self.serving_cipher.open_object(name, original) {
            Err(_) if crate::cipher::object::framed(name) => self.serving_cipher.open(name.as_str(), original)?,
            result => result?,
        };
        if object.bytes != crate::cipher::object::sealed_size(name, plaintext.len())? as u64 { return Ok(false); }
        Ok(self.read_plaintext(name, object).await? == plaintext)
    }

    pub(crate) fn with_serving(self, serving: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(CheckpointStore {
            serving,
            archive: self,
        })
    }

    /// Resolve the archived part of a transferred store before its destination
    /// accepts the history or the source can release its archive.
    pub(crate) async fn materialize(
        &self,
        record: &DeploymentRecord,
        destination: Arc<dyn ObjectStore>,
    ) -> Result<()> {
        let checkpoint = record
            .checkpoint
            .ok_or_else(|| inconsistent("Recovered installation lost its checkpoint"))?;
        let version = checkpoint_storage_root()?;
        let root = format!("{}/{version}", record.storage_prefix());
        let physical = PrefixedStore::new(destination.clone(), root.clone());
        let serving = open_deployment_store(
            destination,
            &root,
            PackDiscovery::From(checkpoint.block),
            wall_clock(),
        )
        .await?;
        let mut roots = Vec::new();
        for (name, object) in &self.objects {
            let plaintext = self.read_plaintext(name, object).await?;
            match serving.get(name).await {
                Ok(bytes) if self.serving_cipher.open_object(name, &bytes)? == plaintext => continue,
                Ok(_) => return Err(Error::ConflictingObject(name.to_string()).into()),
                Err(Error::MissingObject(_)) => {},
                Err(error) => return Err(error.into()),
            }
            let ciphertext = self.serving_cipher.seal_object(name, &plaintext)?;
            if name.as_str().starts_with("blocks/") && rtdb_format::keys::block_index_from_key(name.as_str()).is_ok() {
                roots.push((name, ciphertext));
            } else {
                physical.put(name, ciphertext).await?;
            }
        }
        // The move validates the packed root after its required objects are present.
        for (name, ciphertext) in roots {
            serving.put(name, ciphertext).await?;
        }
        Ok(())
    }
}

/// Transfer the repair overlay under a temporary admission in its original
/// custody. The destination uses its separate publication region.
pub(crate) async fn materialize_serving_archive(
    directory: &dyn Directory,
    keys: &dyn KeyService,
    source: &DeploymentRecord,
    target: &DeploymentRecord,
    destination: Arc<dyn ObjectStore>,
    hosted_task: Option<&rtdb_directory::HostedTask>,
) -> Result<()> {
    let Some(selected) = serving_archive(source)? else { return Ok(()); };
    let archive: CellStorage = serde_json::from_str(selected.archive)
        .map_err(|_| inconsistent("Invalid retained archive binding"))?;
    stage::with_region(
        directory, keys, &source.id, &archive, hosted_task, Some((selected.point.operation, None)), None,
        |current| {
            if !matches!(current.lifecycle, Lifecycle::Moving)
                || current.epoch != source.epoch
                || current.incarnation != source.incarnation
                || current.log_epoch != source.log_epoch
                || current.storage_prefix() != source.storage_prefix()
                || serving_archive(current)?.as_ref() != Some(&selected)
            {
                return Err(inconsistent("The source no longer admits its repair for transfer"));
            }
            Ok(None)
        },
        |record, owner, io| async move {
            let repair = ArchivedCheckpoint::load(&record, keys, owner, io, false).await?
                .ok_or_else(|| inconsistent("The source lost its admitted repair"))?;
            repair.materialize(target, destination).await
        },
    ).await
}

struct CheckpointStore {
    serving: Arc<dyn ObjectStore>,
    archive: ArchivedCheckpoint,
}

#[async_trait::async_trait]
impl ObjectStore for CheckpointStore {
    fn supports_scan_ranges(&self, key: &ObjectKey) -> bool { !self.archive.objects.contains_key(key) && self.serving.supports_scan_ranges(key) }
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        match self.archive.objects.get(key) {
            Some(object) => Ok(StoredObject { key: key.clone(), size: object.bytes }),
            None => self.serving.head(key).await,
        }
    }

    async fn head_fresh(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        Ok(self.observe_head(key).await?.value)
    }

    async fn observe_head(&self, key: &ObjectKey) -> rtdb_storage::Result<ObjectObservation<StoredObject>> {
        let Some(object) = self.archive.objects.get(key) else {
            return self.serving.observe_head(key).await;
        };
        let metadata = self.archive.files.observe_head(key).await?;
        if metadata.value.size != object.bytes {
            return Err(Error::ConflictingObject(key.to_string()));
        }
        Ok(metadata)
    }

    async fn observe_heads(&self, keys: &[ObjectKey]) -> rtdb_storage::Result<BTreeMap<ObjectKey, ObjectObservation<StoredObject>>> {
        let (archived, serving): (Vec<_>, Vec<_>) = keys.iter().cloned()
            .partition(|key| self.archive.objects.contains_key(key));
        let mut observed = self.serving.observe_heads(&serving).await?;
        let archived = futures::stream::iter(archived.into_iter().map(|key| async move {
            Ok::<_, Error>((key.clone(), self.observe_head(&key).await?))
        })).buffer_unordered(16);
        futures::pin_mut!(archived);
        while let Some((key, head)) = archived.try_next().await? { observed.insert(key, head); }
        Ok(observed)
    }

    async fn observe_fresh_exact(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
        let Some(object) = self.archive.objects.get(key) else {
            return self.serving.observe_fresh_exact(key, length).await;
        };
        if object.bytes != length as u64 {
            return Err(Error::ConflictingObject(key.to_string()));
        }
        let observed = observe_archive_object(&self.archive.files, key, object).await?;
        let plaintext = self.archive.archive_cipher.open_object(key, &observed.value)?;
        let value = self.archive.serving_cipher.seal_object(key, &plaintext)?;
        Ok(ObjectObservation { value, version: observed.version })
    }

    async fn observe_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
        let Some(object) = self.archive.objects.get(key) else {
            return self.serving.observe_range(key, range).await;
        };
        if crate::cipher::object::framed(key) {
            self.archive.observe_frame_range(key, object, range).await
        } else {
            Ok(ObjectObservation { value: self.get_range(key, range).await?, version: None })
        }
    }

    async fn retirement_version(&self, key: &ObjectKey) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectVersion>> {
        if self.archive.objects.contains_key(key) { return Ok(None); }
        self.serving.retirement_version(key).await
    }

    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> {
        match self.archive.objects.get(key) {
            Some(object) => self.archive.seal_for_serving(key, object).await,
            None => self.serving.get(key).await,
        }
    }

    async fn get_fresh(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> {
        match self.archive.objects.get(key) {
            Some(object) => self.archive.seal_for_serving(key, object).await,
            None => self.serving.get_fresh(key).await,
        }
    }

    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
        let Some(object) = self.archive.objects.get(key) else {
            return self.serving.get_range(key, range).await;
        };
        if range.start > range.end || range.end > object.bytes {
            return Err(Error::InvalidRange {
                key: key.to_string(),
                start: range.start,
                end: range.end,
                length: object.bytes,
            });
        }
        let bytes = self.archive.seal_for_serving(key, object).await?;
        Ok(bytes.slice(range.start as usize..range.end as usize))
    }

    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
        let mut objects = self
            .serving
            .list(directory)
            .await?
            .into_iter()
            .map(|object| (object.key.clone(), object))
            .collect::<BTreeMap<_, _>>();
        for (key, object) in &self.archive.objects {
            if directory.is_none_or(|directory| key.is_under(directory)) {
                objects.insert(key.clone(), StoredObject {
                    key: key.clone(),
                    size: object.bytes,
                });
            }
        }
        Ok(objects.into_values().collect())
    }

    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> {
        if let Some(object) = self.archive.objects.get(key) {
            let plaintext = self.archive.serving_cipher.open(key.as_str(), &bytes)?;
            if self.archive.read_plaintext(key, object).await? != plaintext {
                return Err(Error::ConflictingObject(key.to_string()));
            }
        }
        self.serving.put(key, bytes).await
    }

    async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> {
        if self.archive.objects.contains_key(key) {
            return self
                .put(
                    key,
                    Bytes::from(
                        tokio::fs::read(source)
                            .await
                            .map_err(|error| Error::Task(error.to_string()))?,
                    ),
                )
                .await;
        }
        self.serving.put_file(key, source).await
    }

    async fn put_with_publication(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectPublication>> {
        if self.archive.objects.contains_key(key) {
            self.put(key, bytes).await?;
            return Ok(None);
        }
        self.serving.put_with_publication(key, bytes).await
    }
    async fn put_file_with_publication(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectPublication>> {
        if self.archive.objects.contains_key(key) {
            self.put_file(key, source).await?;
            return Ok(None);
        }
        self.serving.put_file_with_publication(key, source).await
    }
    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
        self.serving.delete(key).await
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CapturePoint {
    pub operation: Uuid,
    pub deployment: Uuid,
    pub incarnation: u64,
    pub log_epoch: i32,
    pub checkpoint: Checkpoint,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArchivedObject {
    pub name: String,
    pub bytes: u64,
    pub sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frame_header_sha256: Option<String>,
}

impl ArchivedObject {
    fn from_sealed(name: &ObjectKey, sealed: &[u8]) -> rtdb_storage::Result<Self> {
        let frame_header_sha256 = if crate::cipher::object::framed(name) {
            let header = sealed.get(..crate::cipher::object::SEALED_HEADER_BYTES)
                .ok_or_else(|| Error::ConflictingObject(name.to_string()))?;
            Some(digest(header))
        } else {
            None
        };
        Ok(Self { name: name.to_string(), bytes: sealed.len() as u64, sha256: digest(sealed), frame_header_sha256 })
    }

    fn matches_sealed(&self, name: &ObjectKey, sealed: &[u8]) -> bool {
        Self::from_sealed(name, sealed).is_ok_and(|observed| observed == *self)
    }
}

fn primary_contents(contents: &CheckpointContents) -> bool { *contents == CheckpointContents::Primary }

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Capture {
    pub point: CapturePoint,
    pub objects: Vec<ArchivedObject>,
    #[serde(default, skip_serializing_if = "primary_contents")]
    format: CheckpointContents,
}

impl Capture {
    /// The identity of the canonical manifest stored in the independent archive.
    pub fn manifest_sha256(&self) -> Result<String> {
        Ok(digest(&encode(self)?))
    }

    async fn verify(
        &self,
        bucket: Arc<dyn ObjectStore>,
        prefix: &str,
        material: &[u8; crate::keys::KEY_BYTES],
    ) -> Result<()> {
        let files = Arc::new(PrefixedStore::new(bucket, format!("{prefix}/objects")));
        let plaintext = EncryptingStore::new(files.clone(), material)?;
        let closure = checkpoint_objects(&plaintext, &self.point, self.format).await?;
        let mut bytes = 0u64;
        let objects = self.objects.iter()
            .map(|object| {
                bytes = bytes.checked_add(object.bytes)
                    .filter(|total| *total <= MAXIMUM_BYTES)
                    .ok_or_else(|| unavailable("Checkpoint exceeds the archive byte budget"))?;
                Ok((key(&object.name)?, object))
            })
            .collect::<Result<std::collections::BTreeMap<_, _>>>()?;
        if objects.len() != self.objects.len() || objects.keys().ne(closure.keys()) {
            return Err(inconsistent(
                "The archive object list differs from the checkpoint closure",
            ));
        }
        let cipher = Cipher::new(material)?;
        for (name, object) in objects {
            let ciphertext = read_object(files.as_ref(), &name, object).await?;
            let bytes = cipher.open_object(&name, &ciphertext)?;
            require_bound_metadata(closure[&name], &bytes)?;
        }
        Ok(())
    }
}

fn unavailable(message: &'static str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/capture", message)
}

fn inconsistent(message: &'static str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/capture", message)
}

fn encode(value: &impl Serialize) -> Result<Bytes> {
    serde_json::to_vec(value).map(Bytes::from).map_err(|_| {
        Anomaly::fault(
            "rtdb.multi/capture",
            "Cannot serialize the checkpoint capture",
        )
    })
}

async fn read<T: serde::de::DeserializeOwned>(
    archive: &dyn ObjectStore,
    name: &ObjectKey,
) -> Result<Option<T>> {
    let bytes = match archive.head(name).await {
        Err(Error::MissingObject(_)) => return Ok(None),
        Err(error) => return Err(error.into()),
        Ok(metadata) if metadata.size > MAXIMUM_MANIFEST_BYTES => {
            return Err(inconsistent(
                "Checkpoint capture metadata exceeds its bound",
            ));
        },
        Ok(metadata) => archive.get_range(name, 0..metadata.size).await?,
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| inconsistent("Invalid checkpoint capture metadata"))
}

fn same_incarnation(record: &DeploymentRecord, point: &CapturePoint) -> bool {
    record.id == point.deployment.to_string()
        && record.incarnation == point.incarnation
        && record.log_epoch == point.log_epoch
}

fn same_lifetime(record: &DeploymentRecord, point: &CapturePoint) -> bool {
    same_incarnation(record, point)
        && matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
}

fn same_storage(storage: &CellStorage, destination: &CellStorage) -> bool {
    match (storage, destination) {
        (CellStorage::S3 { config: source }, CellStorage::S3 { config: archive }) => {
            source.bucket == archive.bucket && source.endpoint == archive.endpoint
        },
        (CellStorage::Local { root: source }, CellStorage::Local { root: archive }) => {
            source == archive
                || matches!((source.canonicalize(), archive.canonicalize()),
                (Ok(source), Ok(archive)) if source == archive)
        },
        (CellStorage::S3 { .. }, CellStorage::Local { .. })
        | (CellStorage::Local { .. }, CellStorage::S3 { .. }) => false,
    }
}

pub(crate) fn require_independent_world_archive(source: &CellStorage, archive: &CellStorage) -> Result<()> {
    if same_storage(source, archive) || source.overlaps(archive)? {
        return Err(inconsistent("Hosted forks require an independent archive"));
    }
    archive_binding(archive)?;
    Ok(())
}

fn archive_binding(archive: &CellStorage) -> Result<String> {
    match archive {
        CellStorage::S3 { config } if config.credentials.is_some() || config.remote.is_some() => {
            return Err(inconsistent("Checkpoint archives use operator-managed credentials"));
        },
        CellStorage::Local { root } if !root.is_absolute() => {
            return Err(inconsistent("A local checkpoint archive requires an absolute path"));
        },
        _ => {},
    }
    serde_json::to_string(archive).map_err(|_| inconsistent("Cannot retain the archive binding"))
}

fn capture_point(record: &DeploymentRecord, deployment: Uuid, operation: Uuid, archive: &str) -> Result<CapturePoint> {
    let registration = record.checkpoint_captures.get(&operation)
        .filter(|registration| registration.archive == archive)
        .ok_or_else(|| inconsistent("The checkpoint operation names another archive"))?;
    Ok(CapturePoint {
        operation,
        deployment,
        incarnation: registration.incarnation,
        log_epoch: registration.log_epoch,
        checkpoint: registration.checkpoint,
    })
}

pub(crate) fn capture_receipt(
    record: &DeploymentRecord,
    archive: &CellStorage,
    deployment: Uuid,
    operation: Uuid,
) -> Result<Option<(CapturePoint, String)>> {
    let Some(registered) = record.checkpoint_captures.get(&operation) else {
        return Ok(None);
    };
    let point = capture_point(record, deployment, operation, &archive_binding(archive)?)?;
    let Some(manifest) = &registered.manifest_sha256 else {
        return Ok(None);
    };
    if !registered.media_bound {
        return Err(inconsistent("The capture receipt has no archive media binding"));
    }
    Ok(Some((point, manifest.clone())))
}

/// Re-observe the accepted closure in independent custody, without
/// opening the serving deployment or selecting a new checkpoint.
pub(crate) async fn verify_checkpoint_capture(
    directory: &dyn Directory,
    source: &DeploymentRecord,
    archive: &CellStorage,
    keys: &dyn KeyService,
    deployment: Uuid,
    operation: Uuid,
    manifest: &str,
    hosted_task: Option<&rtdb_directory::HostedTask>,
) -> Result<CapturePoint> {
    let (point, manifest) = capture_receipt(source, archive, deployment, operation)?
        .filter(|(_, retained)| retained == manifest)
        .ok_or_else(|| inconsistent("The capture does not retain the supplied manifest"))?;
    stage::with_region(
        directory, keys, &source.id, archive, hosted_task, Some((operation, None)), None,
        |record| {
            require_capture_open(record, operation)?;
            if capture_receipt(record, archive, deployment, operation)?.as_ref() != Some(&(point.clone(), manifest.clone()))
            {
                return Err(inconsistent("The capture no longer admits archive access"));
            }
            Ok(None)
        },
        |_, _, region| async {
            let bucket = archive.bucket(region).await?;
            if read::<CapturePoint>(bucket.as_ref(), &archive_witness(deployment, operation)?).await?.as_ref() != Some(&point) {
                return Err(inconsistent("The archive belongs to another captured checkpoint"));
            }
            verified_archive(bucket, &point, &manifest, keys).await?;
            Ok(())
        },
    ).await?;
    Ok(point)
}

async fn verified_archive(
    bucket: Arc<dyn ObjectStore>,
    point: &CapturePoint,
    manifest: &str,
    keys: &dyn KeyService,
) -> Result<(Capture, [u8; crate::keys::KEY_BYTES])> {
    let prefix = format!("checkpoint-captures/{}/{}", point.deployment, point.operation);
    let metadata = PrefixedStore::new(bucket.clone(), &prefix);
    let capture = read::<Capture>(&metadata, &key("capture.json")?).await?
        .ok_or_else(|| unavailable("The captured manifest is unavailable"))?;
    if &capture.point != point
        || capture.manifest_sha256()? != manifest
        || read::<CapturePoint>(&metadata, &key("intent.json")?).await?.as_ref() != Some(point)
    {
        return Err(inconsistent("The archive differs from its accepted capture"));
    }
    let material = archive_key(keys, point).await?;
    capture.verify(bucket, &prefix, &material).await?;
    Ok((capture, material))
}

pub async fn retain_serving_replay(
    directory: &dyn Directory,
    storage: &CellStorage,
    archive: &CellStorage,
    keys: &dyn KeyService,
    installation: &DeploymentRecord,
    operation: Uuid,
    manifest: &str,
    hosted_task: Option<&rtdb_directory::HostedTask>,
) -> Result<CapturePoint> {
    if same_storage(storage, archive) || storage.overlaps(archive)? {
        return Err(inconsistent("Serving replay requires an independent archive"));
    }
    let deployment = installation.id.parse().map_err(|_| inconsistent("Invalid retained archive owner"))?;
    let (point, manifest) = capture_receipt(installation, archive, deployment, operation)?
        .filter(|(_, retained)| retained == manifest)
        .ok_or_else(|| inconsistent("The capture does not retain the supplied manifest"))?;
    let admit = |record: &DeploymentRecord| {
        require_capture_open(record, operation)?;
        let exact_receipt = capture_receipt(record, archive, deployment, operation)?
            .is_some_and(|(registered, digest)| registered == point && digest == manifest);
        if !same_lifetime(record, &point)
            || record.storage_prefix() != installation.storage_prefix()
            || record.parent.is_some()
            || !exact_receipt
        {
            return Err(inconsistent("The installation changed before archive handoff"));
        }
        if record.serving_replay_capture == Some(operation) {
            return Ok(());
        }
        let previous = serving_archive(record)?.map(|source| source.point.checkpoint.block);
        let parked = record.holder.is_none() && record.checkpoint_recovery.is_none() && record.move_from.is_none();
        let exact_checkpoint = record.checkpoint == Some(point.checkpoint)
            && point.checkpoint.root_sha256.is_some() && record.recovery_manifest_sha256.is_some();
        let same_selection = record.epoch == installation.epoch
            && record.serving_replay_capture == installation.serving_replay_capture;
        if !replay_handoff::may_install(parked, exact_checkpoint, same_selection, previous, point.checkpoint.block) {
            return Err(inconsistent("Archive handoff requires the exact parked checkpoint and a newer replay source"));
        }
        Ok(())
    };
    stage::with_region(
        directory, keys, &installation.id, archive, hosted_task, Some((operation, None)), None,
        |record| {
            admit(record)?;
            Ok(None)
        },
        |record, owner, region| {
            let (point, manifest, admit) = (&point, manifest.as_str(), &admit);
            async move {
                if record.serving_replay_capture == Some(operation) {
                    return Ok(());
                }
                let bucket = archive.bucket(region).await?;
                if read::<CapturePoint>(bucket.as_ref(), &archive_witness(deployment, operation)?).await?.as_ref() != Some(point) {
                    return Err(inconsistent("The archive belongs to another captured checkpoint"));
                }
                let (capture, material) = verified_archive(Arc::clone(&bucket), point, manifest, keys).await?;
                if capture.format != CheckpointContents::Replay {
                    return Err(inconsistent("Serving replay requires a complete replay archive"));
                }
                keys.existing_key(&record.key_id()).await?
                    .ok_or_else(|| unavailable("The current installation key is unavailable"))?;
                let files = PrefixedStore::new(bucket, format!("checkpoint-captures/{deployment}/{operation}/objects"));
                let plaintext = EncryptingStore::new(Arc::new(files), &material)?;
                let digest = recovery_manifest_sha256(&plaintext, point.checkpoint, StorageEpoch(point.log_epoch)).await?;
                update_installation(directory, &installation.id, |current| {
                    admit(current)?;
                    if !current.storage_regions.get(&owner).is_some_and(|hold| hold.admits_archive_read(operation)) {
                        return Err(inconsistent("The archive handoff lost its storage admission"));
                    }
                    if current.serving_replay_capture != Some(operation) {
                        current.serving_replay_capture = Some(operation);
                        current.recovery_manifest_sha256 = Some(digest);
                    }
                    Ok(())
                }).await?;
                Ok(())
            }
        },
    ).await?;
    Ok(point)
}

fn archive_key_id(point: &CapturePoint) -> String {
    format!("checkpoint-{}-{}-{}", point.deployment, point.incarnation, point.operation)
}

async fn archive_key(keys: &dyn KeyService, point: &CapturePoint) -> Result<[u8; crate::keys::KEY_BYTES]> {
    keys.existing_key(&archive_key_id(point)).await?
        .ok_or_else(|| unavailable("The checkpoint archive key is unavailable"))
}

fn archive_witness(deployment: Uuid, operation: Uuid) -> Result<ObjectKey> {
    key(&format!("checkpoint-authorities/{deployment}/{operation}.json")).map_err(Into::into)
}

fn verify_cut(catalog: &BlockCatalog, checkpoint: Checkpoint, epoch: StorageEpoch) -> Result<()> {
    if catalog.block_index().0 != checkpoint.block
        || checkpoint.root_sha256.is_some_and(|digest| catalog.publication_sha256() != Some(digest))
        || catalog
            .boundary_replica_position()
            .map_or(-1, |position| position.after_offset(epoch))
            != checkpoint.boundary
        || catalog
            .latest_source_position()
            .map_or(-1, |position| position.after_offset(epoch))
            != checkpoint.source
        || catalog
            .latest_completed_tx()
            .map_or(i64::MIN, |tx| tx.system_time.0)
            != checkpoint.system_time
        || !catalog.secondary_databases().is_empty()
    {
        return Err(inconsistent(
            "The block is not the selected installation checkpoint",
        ));
    }
    Ok(())
}

type CheckpointObjects = BTreeMap<ObjectKey, Option<[u8; 32]>>;
const OBSERVATION_BATCH_MEMBERS: usize = 256;

async fn observe_checkpoint_batch(
    store: &dyn ObjectStore, batch: Vec<(ObjectKey, Option<[u8; 32]>)>,
) -> Result<Vec<CheckpointCopySource>> {
    let names = batch.iter().map(|(name, _)| name.clone()).collect::<Vec<_>>();
    let mut heads = store.observe_heads(&names).await?;
    batch.into_iter().map(|(name, digest)| {
        let head = heads.remove(&name).ok_or_else(|| inconsistent("A checkpoint inventory member has no storage observation"))?;
        CheckpointCopySource::from_head(name, digest, head)
    }).collect()
}

fn require_bound_metadata(expected: Option<[u8; 32]>, bytes: &[u8]) -> Result<()> {
    if expected.is_some_and(|expected| publication_digest(bytes) != expected) {
        return Err(inconsistent("Checkpoint metadata changed after dependency discovery"));
    }
    Ok(())
}

async fn checkpoint_objects(plaintext: &dyn ObjectStore, point: &CapturePoint, contents: CheckpointContents) -> Result<CheckpointObjects> {
    let catalog_key = key(&block_key(BlockIndex(point.checkpoint.block))?)?;
    let catalog = BlockCatalog::decode(&plaintext.get_fresh(&catalog_key).await?)?;
    verify_cut(&catalog, point.checkpoint, StorageEpoch(point.log_epoch))?;
    catalog_objects(plaintext, &catalog, contents).await
}

async fn catalog_objects(plaintext: &dyn ObjectStore, catalog: &BlockCatalog, contents: CheckpointContents) -> Result<CheckpointObjects> {
    let block = catalog.block_index();
    let root_digest = catalog.publication_sha256().ok_or_else(|| inconsistent("Checkpoint root bytes were not observed"))?;
    let mut closure = BTreeMap::from([(key(&block_key(block)?)?, Some(root_digest))]);
    for table in catalog.table_entries()? {
        let path = key(&table.slug.block_key(block)?)?;
        let bytes = plaintext.get_fresh(&path).await?;
        let observed = publication_digest(&bytes);
        if !catalog.matches_table_block(table.oid, &observed) {
            return Err(inconsistent("Checkpoint table bytes do not match the root manifest"));
        }
        let metadata = TableBlock::decode(&bytes)?;
        if closure.insert(path, Some(observed)).is_some() {
            return Err(inconsistent("Two checkpoint tables use the same storage path"));
        }
        for object in metadata.checkpoint_trie_files(&table.slug, contents)? {
            closure.entry(key(&object)?).or_insert(None);
        }
    }
    Ok(closure)
}

pub(crate) async fn recovery_manifest_sha256(
    store: &dyn ObjectStore, checkpoint: Checkpoint, epoch: StorageEpoch,
) -> Result<[u8; 32]> {
    let root = checkpoint.root_sha256.ok_or_else(|| inconsistent("A recovery manifest requires a checkpoint root digest"))?;
    let first_required = checkpoint.boundary.checked_add(1)
        .filter(|first| *first > 0)
        .ok_or_else(|| inconsistent("A recovery manifest requires a replica boundary"))?;
    let path = key(&block_key(BlockIndex(checkpoint.block))?)?;
    let catalog = BlockCatalog::decode(&store.get_fresh(&path).await?)?;
    verify_cut(&catalog, checkpoint, epoch)?;
    let mut closure = catalog_objects(store, &catalog, CheckpointContents::Replay).await?.into_iter();

    let mut manifest = ring::digest::Context::new(&ring::digest::SHA256);
    manifest.update(b"rtdb/recovery-manifest/v1\0");
    manifest.update(&encode(&(root, epoch.0, first_required))?);
    loop {
        let batch = closure.by_ref().take(OBSERVATION_BATCH_MEMBERS).collect::<Vec<_>>();
        if batch.is_empty() { break; }
        let members = observe_checkpoint_batch(store, batch).await?;
        let observations = futures::stream::iter(members.into_iter().map(|source| async move {
            if source.metadata_digest.is_some() { source.read(store).await?; }
            encode(&(source.name.as_str(), source.logical_bytes as u64, source.version, source.metadata_digest))
        })).buffered(16);
        futures::pin_mut!(observations);
        while let Some(member) = observations.try_next().await? { manifest.update(&member); }
    }
    Ok(manifest.finish().as_ref().try_into().expect("SHA-256 has 32 bytes"))
}

/// The exact body observation expected for a logical source member. A packed
/// member names its containing publication, and a repaired member names its
/// archived ciphertext. The observation does not confer retirement authority.
struct CheckpointCopySource {
    name: ObjectKey,
    logical_bytes: usize,
    version: Option<PhysicalObjectVersion>,
    metadata_digest: Option<[u8; 32]>,
}

impl CheckpointCopySource {
    async fn observe(
        store: &dyn ObjectStore,
        name: ObjectKey,
        metadata_digest: Option<[u8; 32]>,
    ) -> Result<Self> {
        let observed = store.observe_head(&name).await?;
        Self::from_head(name, metadata_digest, observed)
    }

    fn from_head(name: ObjectKey, metadata_digest: Option<[u8; 32]>, observed: ObjectObservation<StoredObject>) -> Result<Self> {
        if observed.value.key != name { return Err(inconsistent("Checkpoint storage observed a different logical member")); }
        let logical_bytes = usize::try_from(observed.value.size)
            .map_err(|_| unavailable("Checkpoint object exceeds addressable length"))?;
        Ok(Self { name, logical_bytes, version: observed.version, metadata_digest })
    }

    async fn read(&self, store: &dyn ObjectStore) -> Result<Bytes> {
        let observed = store.observe_fresh_exact(&self.name, self.logical_bytes).await?;
        if observed.version != self.version {
            return Err(inconsistent("Checkpoint source publication changed during copying"));
        }
        if observed.value.len() != self.logical_bytes {
            return Err(inconsistent("Checkpoint source length changed during copying"));
        }
        require_bound_metadata(self.metadata_digest, &observed.value)?;
        Ok(observed.value)
    }
}

async fn plan_checkpoint_copy(
    store: &dyn ObjectStore,
    closure: CheckpointObjects,
    mut archive_budget: Option<u64>,
) -> Result<Vec<CheckpointCopySource>> {
    let mut sources = Vec::with_capacity(closure.len());
    let mut closure = closure.into_iter();
    loop {
        let batch = closure.by_ref().take(OBSERVATION_BATCH_MEMBERS).collect::<Vec<_>>();
        if batch.is_empty() { break; }
        for source in observe_checkpoint_batch(store, batch).await? {
            if let Some(remaining) = &mut archive_budget {
                let sealed = crate::cipher::object::sealed_size(&source.name, source.logical_bytes)?;
                *remaining = crate::checkpoint_copy_budget::remaining_after_sealing(
                    *remaining, source.logical_bytes as u64, (sealed - source.logical_bytes) as u64,
                ).ok_or_else(|| unavailable("Checkpoint exceeds the archive byte budget"))?;
            }
            sources.push(source);
        }
    }
    Ok(sources)
}

/// Generic deployment forks cannot adopt governed custody. The exact hosted
/// world and terminal transfer owners carry the required source admissions.
pub(crate) async fn require_plain_fork_cut(
    bucket: Arc<dyn ObjectStore>, source: &DeploymentRecord, source_key: &[u8], selected: i64,
) -> Result<()> {
    let root = format!("{}/{}", source.storage_prefix(), checkpoint_storage_root()?);
    let marker = bucket.get_fresh(&key(&format!("{root}/{ROOT_MARKER}"))?).await?;
    if marker.as_ref() != source.id.as_bytes() {
        return Err(inconsistent("The checkpoint belongs to another source"));
    }
    let store = open_deployment_store(bucket, &root, PackDiscovery::From(selected), wall_clock()).await?;
    let plaintext = EncryptingStore::new(store, source_key)?;
    let catalog = BlockCatalog::decode(&plaintext.get_fresh(&key(&block_key(BlockIndex(selected))?)?).await?)?;
    if catalog.block_index().0 != selected || !catalog.secondary_databases().is_empty() {
        return Err(inconsistent("The fork checkpoint has another catalog or secondary databases"));
    }
    if let Some(checkpoint) = source.checkpoint.filter(|checkpoint| checkpoint.block == selected) {
        verify_cut(&catalog, checkpoint, StorageEpoch(source.log_epoch))?;
    }
    if catalog.table_entries()?.iter().any(|entry| rtdb_types::TableRef::owned_schema(&entry.table.schema)) {
        return Err(unavailable("Generic deployment forks cannot copy governed relation custody"));
    }
    Ok(())
}

pub(crate) async fn copy_fork_checkpoint(
    bucket: Arc<dyn ObjectStore>,
    source: &DeploymentRecord,
    source_key: &[u8],
    child: &DeploymentRecord,
    child_key: &[u8],
    at_block: Option<i64>,
) -> Result<()> {
    let selected = at_block.or_else(|| source.checkpoint.map(|checkpoint| checkpoint.block))
        .ok_or_else(|| unavailable("The source has no checkpoint to copy"))?;
    if source.checkpoint_recovery.is_some() || source.serving_replay_capture.is_some() {
        return Err(inconsistent("A fork requires serving history outside checkpoint repair"));
    }
    let version = checkpoint_storage_root()?;
    let source_root = format!("{}/{version}", source.storage_prefix());
    let destination_root = format!("{}/{version}", child.storage_prefix());
    let owner = bucket.get(&key(&format!("{source_root}/{ROOT_MARKER}"))?).await?;
    if owner.as_ref() != source.id.as_bytes() {
        return Err(inconsistent("The checkpoint belongs to another source"));
    }
    let source_store = open_deployment_store(
        bucket.clone(), &source_root, PackDiscovery::From(selected), wall_clock(),
    ).await?;
    let plaintext = EncryptingStore::new(source_store, source_key)?;
    let block = BlockIndex(selected);
    let catalog_key = key(&block_key(block)?)?;
    let root_source = CheckpointCopySource::observe(&plaintext, catalog_key.clone(), None).await?;
    let catalog_bytes = root_source.read(&plaintext).await?;
    let catalog = BlockCatalog::decode(&catalog_bytes)?;
    if catalog.block_index() != block || !catalog.secondary_databases().is_empty() {
        return Err(inconsistent("The selected fork checkpoint has a different block or owns secondary databases"));
    }
    if let Some(checkpoint) = source.checkpoint.filter(|checkpoint| checkpoint.block == selected) {
        verify_cut(&catalog, checkpoint, StorageEpoch(source.log_epoch))?;
    }
    let mut closure = catalog_objects(&plaintext, &catalog, CheckpointContents::Replay).await?;
    closure.remove(&catalog_key);
    let sources = plan_checkpoint_copy(&plaintext, closure, None).await?;
    let physical = Arc::new(PrefixedStore::new(bucket, destination_root));
    crate::store::check_root(physical.as_ref(), &child.id, true).await?;
    let packed = crate::packing::PackingStore::open(physical, PackDiscovery::List).await?;
    let cipher = Cipher::new(child_key)?;
    for source in sources {
        let content = source.read(&plaintext).await?;
        packed.put(&source.name, cipher.seal_object(&source.name, &content)?).await?;
    }
    packed.put(&catalog_key, cipher.seal_object(&catalog_key, &catalog_bytes)?).await?;
    Ok(())
}

/// Copies the closure of one already published checkpoint. A capture ID
/// fixes the checkpoint before any payload copy. An interrupted copy can
/// resume from its accepted archive or while a source admission still
/// protects its original checkpoint metadata. A flush receipt fixes the exact
/// adopted root. The admission that binds its archive also pins that root.
pub async fn capture(
    directory: Arc<dyn Directory>,
    storage: &CellStorage,
    destination: &CellStorage,
    keys: &dyn KeyService,
    deployment: Uuid,
    operation: Uuid,
    hosted_task: Option<rtdb_directory::HostedTask>,
    flushed: Option<&DeploymentRecord>,
) -> Result<Capture> {
    capture_with_purpose(directory, storage, destination, keys, deployment, operation, hosted_task, flushed, CapturePurpose::Explicit).await
}

#[derive(Clone, Copy)]
enum CapturePurpose {
    Explicit,
    AutomaticFork,
    ReplayedWorldBase([u8; 32]),
}

async fn capture_with_purpose(
    directory: Arc<dyn Directory>,
    storage: &CellStorage,
    destination: &CellStorage,
    keys: &dyn KeyService,
    deployment: Uuid,
    operation: Uuid,
    hosted_task: Option<rtdb_directory::HostedTask>,
    flushed: Option<&DeploymentRecord>,
    purpose: CapturePurpose,
) -> Result<Capture> {
    let lineage = if matches!(storage, CellStorage::S3 { .. }) {
        Some(directory.deployment(&deployment.to_string()).await?.ok_or_else(|| unavailable("The source installation no longer exists"))?.lineage)
    } else { None };
    crate::store::with_source_lineage(directory.as_ref(), lineage.as_deref(), hosted_task.as_ref(),
        Box::pin(capture_checkpoint(directory.clone(), storage, destination, keys, deployment, operation, hosted_task.clone(), flushed, purpose))).await
}

async fn capture_checkpoint(
    directory: Arc<dyn Directory>,
    storage: &CellStorage,
    destination: &CellStorage,
    keys: &dyn KeyService,
    deployment: Uuid,
    operation: Uuid,
    hosted_task: Option<rtdb_directory::HostedTask>,
    flushed: Option<&DeploymentRecord>,
    purpose: CapturePurpose,
) -> Result<Capture> {
    let automatic_fork = matches!(purpose, CapturePurpose::AutomaticFork);
    let world_replay = match purpose {
        CapturePurpose::ReplayedWorldBase(binding) => Some(binding),
        _ => None,
    };
    if deployment.is_nil() || operation.is_nil() || same_storage(storage, destination)
        || (automatic_fork && flushed.is_none())
    {
        return Err(inconsistent(
            "Capture requires one deployment, operation and independent archive",
        ));
    }
    let binding = archive_binding(destination)?;
    let id = deployment.to_string();
    let region = Uuid::new_v4();
    let local = matches!((storage, destination), (CellStorage::Local { .. }, CellStorage::Local { .. }));
    if !local && hosted_task.is_none() {
        return Err(inconsistent("Remote checkpoint capture requires its installed ECS task"));
    }
    let witness = if local { keys.hold_local_storage_region(region)? } else { None };
    let witnessed = witness.is_some();
    let mut io = IoRegion::new().child_holding(witness.map(|file| Arc::new(file) as Arc<dyn Send + Sync>));
    if matches!(destination, CellStorage::S3 { .. }) {
        crate::store::track_publications(&mut io, directory.clone(), id.clone(), region, None);
    }
    let admitted = update_installation(directory.as_ref(), &id, |record| {
        require_capture_open(record, operation)?;
        if !matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
            || record.move_from.is_some()
        {
            return Err(unavailable("The installation cannot be captured"));
        }
        let selected = if let Some(flushed) = flushed {
            let cut = flushed.checkpoint.filter(|cut| cut.root_sha256.is_some())
                .ok_or_else(|| unavailable("The selected source cut has no exact published root"))?;
            if record.id != flushed.id || record.incarnation != flushed.incarnation
                || record.log_epoch != flushed.log_epoch
            {
                return Err(unavailable("The selected source cut belongs to another installation lifetime"));
            }
            Some(cut)
        } else {
            None
        };
        if let Some(registered) = record.checkpoint_captures.get(&operation) {
            if registered.archive != binding || registered.incarnation != record.incarnation
                || registered.log_epoch != record.log_epoch
                || registered.automatic_fork != automatic_fork || registered.world_replay != world_replay
            {
                return Err(inconsistent("The capture operation already names another archive or lifetime"));
            }
            if selected.is_some_and(|cut| cut != registered.checkpoint) {
                return Err(inconsistent("The capture operation already names another checkpoint"));
            }
        } else {
            if let Some(cut) = selected
                && (record.checkpoint != Some(cut)
                    || flushed.is_some_and(|source| record.epoch != source.epoch || record.holder != source.holder))
            {
                return Err(unavailable("The selected source cut changed before archive admission"));
            }
            if record.checkpoint_repair_receipts.contains_key(&operation) {
                return Err(inconsistent("The capture operation has a repair in another residence"));
            }
            let checkpoint = record.checkpoint
                .ok_or_else(|| unavailable("The installation has not published a checkpoint"))?;
            if let Some(binding) = world_replay {
                if binding == [0; 32] || record.checkpoint_captures.values()
                    .any(|capture| capture.world_replay.is_some())
                {
                    return Err(inconsistent("A destination can prepare one replay parent base"));
                }
                crate::hosted_recovery::replay_start(record, checkpoint.boundary)?;
            }
            record.checkpoint_captures.insert(operation, CheckpointCapture {
                archive: binding.clone(),
                incarnation: record.incarnation,
                log_epoch: record.log_epoch,
                checkpoint,
                automatic_fork,
                world_replay,
                media_bound: false,
                manifest_sha256: None,
                remote_disposal: None,
            });
        }
        if automatic_fork && record.storage_regions.values().any(|region| region.holds_archive(operation)) {
            return Err(Anomaly::new(
                AnomalyCategory::Busy, "rtdb.multi/fork-capture-active",
                "The exact fork input already has an admitted copy",
            ));
        }
        let checkpoint = record.checkpoint_captures[&operation].checkpoint;
        let checkpoint_metadata_from = capture_metadata_floor(record, checkpoint);
        let repair_archive = serving_archive_operation(record)?;
        let admission = if local {
            rtdb_directory::StorageRegion::Local {
                archive: Some(operation),
                checkpoint_suffix: None,
                repair_archive,
                tracks_ordered_index_pages: true,
                checkpoint_metadata_from,
                checkpoint_replay_from: None,
            }
        } else {
            let mut admission = crate::store::remote_storage_region(Some(operation), hosted_task.clone());
            if let rtdb_directory::StorageRegion::Remote { checkpoint_metadata_from: floor, repair_archive: held_repair, .. } = &mut admission {
                *floor = checkpoint_metadata_from;
                *held_repair = repair_archive;
            }
            admission
        };
        record.storage_regions.insert(region, admission);
        Ok(())
    }).await;
    let record = match admitted {
        Ok(record) => record,
        Err(error) => {
            // No provider request was sent. Cancel any admission whose
            // directory response was lost, including a late CAS.
            drop(io);
            if error.code != "rtdb.multi/capture" {
                crate::store::release_region(directory.as_ref(), &id, region, true).await?;
            }
            if witnessed {
                keys.forget_local_storage_region(region)?;
            }
            return Err(error);
        },
    };
    let result = async {
        let point = capture_point(&record, deployment, operation, &binding)?;
        let capture = capture_registered(
            directory.as_ref(), storage, destination, keys, &record, point, region, io.clone(),
        ).await?;
        let manifest = capture.manifest_sha256()?;
        update_installation(directory.as_ref(), &id, |current| {
            require_capture_open(current, operation)?;
            if !same_incarnation(current, &capture.point)
                || !current.storage_regions.contains_key(&region)
                || capture_point(current, deployment, operation, &binding)? != capture.point
            {
                return Err(inconsistent("The capture lost its original storage admission"));
            }
            let bound = current.checkpoint_captures.get_mut(&operation)
                .filter(|registered| registered.media_bound)
                .ok_or_else(|| inconsistent("The capture has no archive media binding"))?;
            match &bound.manifest_sha256 {
                Some(prior) if prior == &manifest => Ok(()),
                Some(_) => Err(inconsistent("The original capture has another manifest")),
                None => {
                    bound.manifest_sha256 = Some(manifest.clone());
                    Ok(())
                }
            }
        }).await?;
        Ok(capture)
    }.await;
    io.close_and_wait().await;
    drop(io);
    crate::store::release_region(directory.as_ref(), &id, region, false).await?;
    if witnessed {
        keys.forget_local_storage_region(region)?;
    }
    result
}

fn capture_metadata_floor(record: &DeploymentRecord, checkpoint: Checkpoint) -> Option<i64> {
    let published = record.checkpoint?;
    if published.block == checkpoint.block && published != checkpoint { return None; }
    let oldest = record.storage_regions.values()
        .filter_map(rtdb_directory::StorageRegion::checkpoint_metadata_from)
        .min();
    crate::checkpoint_retention::checkpoint_metadata_admissible(checkpoint.block, Some(published.block), oldest)
        .then_some(checkpoint.block)
}

async fn capture_registered(
    directory: &dyn Directory,
    storage: &CellStorage,
    destination: &CellStorage,
    keys: &dyn KeyService,
    record: &DeploymentRecord,
    point: CapturePoint,
    region: Uuid,
    io: Arc<IoRegion>,
) -> Result<Capture> {
    let CapturePoint { deployment, operation, .. } = point;
    let archive_bucket = destination.bucket(io.clone()).await?;
    archive_bucket.put(&archive_witness(deployment, operation)?, encode(&point)?).await?;
    update_installation(directory, &record.id, |current| {
        require_capture_open(current, operation)?;
        if !same_lifetime(current, &point) {
            return Err(unavailable("The installation changed before binding archive media"));
        }
        let registration = current.checkpoint_captures.get_mut(&operation)
            .filter(|registration| record.checkpoint_captures.get(&operation)
                .is_some_and(|accepted| accepted.archive == registration.archive))
            .ok_or_else(|| inconsistent("The checkpoint archive binding changed"))?;
        registration.media_bound = true;
        Ok(())
    }).await?;
    let prefix = format!("checkpoint-captures/{deployment}/{operation}");
    let archive = PrefixedStore::new(archive_bucket.clone(), &prefix);
    let spec = key("intent.json")?;
    let receipt = key("capture.json")?;
    archive.put(&spec, encode(&point)?).await?;
    if let Some(completed) = read::<Capture>(&archive, &receipt).await? {
        if completed.point != point {
            return Err(inconsistent("The completed archive has another checkpoint"));
        }
        completed.verify(archive_bucket, &prefix, &archive_key(keys, &point).await?).await?;
        return Ok(completed);
    }
    if !same_lifetime(record, &point)
        || record.storage_regions.get(&region)
            .and_then(rtdb_directory::StorageRegion::checkpoint_metadata_from) != Some(point.checkpoint.block)
    {
        return Err(unavailable("The capture's original checkpoint metadata is no longer retained"));
    }

    let serving_material = keys.existing_key(&record.key_id()).await?
        .ok_or_else(|| unavailable("The source key is unavailable"))?;
    let archive_material = keys.key(&archive_key_id(&point)).await?;
    let archive_cipher = Cipher::new(&archive_material)?;
    let recovery = ArchivedCheckpoint::load(record, keys, region, io.clone(), false).await?;
    // A local sweep and this source read share the maintenance witness. The
    // provider keeps its clone during a read even if this copy is cancelled.
    let maintenance = storage.try_local_maintenance()?;
    let source_io = io.child_holding(maintenance.map(|lock| Arc::new(lock) as Arc<dyn Send + Sync>));
    let source = storage.bucket(source_io).await?;
    let version = checkpoint_storage_root()?;
    let root = format!("{}/{version}", record.storage_prefix());
    let owner = source.get(&key(&format!("{root}/{ROOT_MARKER}"))?).await?;
    if owner.as_ref() != record.id.as_bytes() {
        return Err(inconsistent("The checkpoint belongs to another source"));
    }
    let sealed = open_deployment_store_with_archive(
        source,
        &root,
        PackDiscovery::From(point.checkpoint.block),
        wall_clock(),
        recovery,
    )
    .await?;
    let plaintext = EncryptingStore::new(sealed as Arc<dyn ObjectStore>, &serving_material)?;
    let format = CheckpointContents::Replay;
    let closure = checkpoint_objects(&plaintext, &point, format).await?;

    let sources = plan_checkpoint_copy(&plaintext, closure, Some(MAXIMUM_BYTES)).await?;
    let mut objects = Vec::with_capacity(sources.len());
    for source in sources {
        let content = source.read(&plaintext).await?;
        let object = source.name;
        let ciphertext = archive_cipher.seal_object(&object, &content)?;
        let archived = ArchivedObject::from_sealed(&object, &ciphertext)?;
        archive
            .put(&key(&format!("objects/{}", object.as_str()))?, ciphertext)
            .await?;
        objects.push(archived);
    }
    let capture = Capture { point, objects, format };
    let encoded = encode(&capture)?;
    if encoded.len() as u64 > MAXIMUM_MANIFEST_BYTES {
        return Err(unavailable(
            "Checkpoint exceeds the archive metadata budget",
        ));
    }
    capture.verify(archive_bucket, &prefix, &archive_material).await?;
    if !directory
        .deployment(&deployment.to_string())
        .await?
        .is_some_and(|record| same_lifetime(&record, &capture.point))
    {
        return Err(unavailable("The installation changed during capture"));
    }
    archive.put(&receipt, encoded).await?;
    Ok(capture)
}

/// Reinstalls the exact checkpoint the live directory already requires.
/// The old writer is fenced and no placement is admitted while files are
/// being installed. The next holder must still recover the ordered tail.
pub async fn restore(
    directory: &dyn Directory,
    storage: &CellStorage,
    archive: &CellStorage,
    keys: &dyn KeyService,
    deployment: Uuid,
    operation: Uuid,
    maximum_message_bytes: usize,
    hosted_task: Option<&rtdb_directory::HostedTask>,
) -> Result<CheckpointRecovery> {
    restore_checkpoint(directory, storage, archive, keys, deployment, operation, maximum_message_bytes, None, hosted_task).await
}

/// Reopen the authenticated source root before authorizing any destination writes.
async fn admit_lifetime_restore(
    directory: &dyn Directory, archive: &CellStorage, keys: &dyn KeyService,
    point: &CapturePoint, manifest: &str, hosted_task: Option<&rtdb_directory::HostedTask>,
    allow_lifetime_floors: bool,
    require_replay: bool,
) -> Result<()> {
    let mut node = rtdb_node::NodeOptions::default();
    node.owned_relations = true;
    let memory = rtdb_columnar::MemoryBudget::new(node.execution_memory_bytes);
    let cache = rtdb_storage::cache::CacheBudget::new(node.cache_bytes);
    let cancellation = tokio_util::sync::CancellationToken::new();
    read_world::with_registered_capture_context(
        directory, archive, keys, hosted_task,
        read_world::CaptureSelection { deployment: point.deployment, capture: point.operation,
            manifest, suffix: None, expected_basis: None, require_replay },
        CheckpointReadResources { node: &node, memory: &memory, cache, cancellation: &cancellation },
        |context, basis| async move {
            if basis.system_time.0 != point.checkpoint.system_time {
                return Err(inconsistent("The authenticated source has another restore cut"));
            }
            let floors = read_archive_lifetime_floors(context, basis).await?;
            if !allow_lifetime_floors && !floors.is_empty() {
                return Err(unavailable("Fresh checkpoint replay has no original lifetime-floor transfer authority"));
            }
            Ok(())
        },
    ).await
}

async fn read_archive_lifetime_floors(
    context: Arc<rtdb_query::QueryContext>, basis: rtdb_types::TransactionKey,
) -> Result<BTreeMap<rtdb_types::TableRef, rtdb_types::OwnedLifetimeFloor>> {
    use rtdb_plan::Catalog;

    let database = context.catalog.database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else {
        return Err(inconsistent("The original checkpoint has no exact lifetime inventory"));
    };
    let _inventory = context.memory.reserve(snapshot.schema_allocation_bound(context.catalog.default_database())?)?;
    let tables = snapshot.tables();
    let mut markers = BTreeMap::new();
    let mut floors = BTreeMap::new();
    for table in &tables {
        let Some(relation) = table.owned_lifetime_relation() else { continue; };
        let catalog = table.owned_lifetime_schema_catalog()
            .ok_or_else(|| inconsistent("The checkpoint has no original lifetime declaration catalog"))?;
        if markers.len() >= 4096 {
            return Err(unavailable("The restore cannot retain the original lifetime floors"));
        }
        markers.insert((catalog, relation.to_owned()), table);
    }
    for catalog in tables.iter().filter(|table| table.is_owned_relation_catalog()) {
        context.check_cancelled()?;
        let (schemas, _charge) =
            rtdb_query::read_owned_schema_catalog(Arc::clone(&context), catalog.clone()).await?;
        for (relation, schema) in schemas.relations() {
            let Some(duration) = schema.lifetime_micros() else { continue; };
            let table = markers.remove(&(catalog.clone(), relation.to_owned()))
                .ok_or_else(|| inconsistent("The checkpoint lifetime declaration has no trusted floor"))?;
            let floor = rtdb_query::read_owned_lifetime_floor_table(Arc::clone(&context), table.clone()).await?;
            floor.check(duration, basis.system_time)?;
            floors.insert(table.clone(), floor);
        }
    }
    if !markers.is_empty() {
        return Err(inconsistent("The checkpoint lifetime floor has no original declaration"));
    }
    Ok(floors)
}

async fn archive_lifetime_floors_digest(
    context: Arc<rtdb_query::QueryContext>, basis: rtdb_types::TransactionKey,
) -> Result<[u8; 32]> {
    let floors = read_archive_lifetime_floors(context, basis).await?;
    let records: Vec<_> = floors.into_iter().map(|(table, floor)|
        (table, floor.duration, floor.expired_created_through, floor.swept_present_through)
    ).collect();
    Ok(sha256(&encode(&records)?))
}

pub(crate) async fn stage_checkpoint_transfer(
    directory: Arc<dyn Directory>, archive: &CellStorage, storage: &CellStorage, keys: &dyn KeyService,
    request: rtdb_directory::CheckpointStageIntent, hosted_task: Option<rtdb_directory::HostedTask>,
) -> Result<rtdb_directory::CheckpointStage> {
    stage::stage_checkpoint_for(directory, archive, storage, keys, request, hosted_task, true).await
}

/// Issue a terminal receipt only for a previously accepted, private target
/// stage under the original source installation's recovery authority.
pub(crate) async fn issue_checkpoint_transfer(
    directory: Arc<dyn Directory>, storage: &CellStorage, archive: &CellStorage, keys: &dyn KeyService,
    selector: StageSelector, hosted_task: Option<rtdb_directory::HostedTask>,
) -> Result<rtdb_directory::CheckpointTransferTerminal> {
    let (source, capture, org) = selector.source.as_ref()
        .ok_or_else(|| inconsistent("Terminal transfer requires authenticated source authority"))?;
    let (source, capture) = (*source, *capture);
    let id = source.to_string();
    let target_id = selector.destination.to_string();
    let original = directory.deployment(&id).await?
        .filter(|record| record.org == *org && record.parent.is_none())
        .ok_or_else(|| inconsistent("The original transfer source is unavailable"))?;
    if let Some(terminal) = original.checkpoint_transfer_terminal.as_deref() {
        let stage = &terminal.fence.destination;
        if original.checkpoint_transfer_fence.as_deref() != Some(&terminal.fence)
            || original.lifecycle != Lifecycle::Restoring || original.epoch != terminal.fence.fenced_writer_epoch
            || stage.intent.capture != capture || stage.intent.destination != selector.destination
            || stage.intent.operation != selector.operation
            || stage.staged_manifest_sha256.as_deref() != Some(selector.manifest_sha256.as_str())
            || stage.media != crate::admin::cell_media(storage, keys)?
        {
            return Err(inconsistent("The source already issued another terminal receipt"));
        }
        return Ok(terminal.clone());
    }
    let stage = reader::admit_transfer_destination(
        Arc::clone(&directory), storage, keys, selector, hosted_task.clone(),
    ).await?;
    let point = verify_checkpoint_capture(
        directory.as_ref(), &original, archive, keys, source, capture,
        &stage.intent.source_manifest_sha256, hosted_task.as_ref(),
    ).await?;
    if point.checkpoint != stage.checkpoint || point.incarnation != stage.source_incarnation
        || point.log_epoch != stage.source_log_epoch
    {
        return Err(inconsistent("The target stage has another source checkpoint"));
    }
    let source_version = directory.deployment(&id).await?
        .ok_or_else(|| unavailable("The original transfer source is unavailable"))?.version;
    let destination_version = directory.deployment(&target_id).await?
        .ok_or_else(|| unavailable("The private transfer target is unavailable"))?.version;
    let fence = directory.fence_checkpoint_transfer(&rtdb_directory::CheckpointTransferSelection {
        stage, source_version, destination_version,
    }).await?;
    seal_checkpoint_transfer(directory.as_ref(), storage, archive, keys, hosted_task.as_ref(), &fence).await
}

/// Complete an already ordered source transfer fence. The result is a
/// retained source receipt, and does not activate or release either residence.
pub(crate) async fn seal_checkpoint_transfer(
    directory: &dyn Directory, storage: &CellStorage, archive: &CellStorage, keys: &dyn KeyService,
    hosted_task: Option<&rtdb_directory::HostedTask>,
    fence: &rtdb_directory::CheckpointTransferFence,
) -> Result<rtdb_directory::CheckpointTransferTerminal> {
    let stage = &fence.destination;
    let intent = &stage.intent;
    let source = directory.deployment(&intent.source.to_string()).await?
        .filter(|record| record.checkpoint_transfer_fence.as_deref() == Some(fence)
            && record.lifecycle == Lifecycle::Restoring && record.epoch == fence.fenced_writer_epoch)
        .ok_or_else(|| inconsistent("The source no longer owns this transfer fence"))?;
    let destination = directory.deployment(&intent.destination.to_string()).await?
        .filter(|record| record.checkpoint_transfer_fence.as_deref() == Some(fence)
            && record.lifecycle == Lifecycle::Restoring && record.checkpoint_stage.as_deref() == Some(stage))
        .ok_or_else(|| inconsistent("The original destination stage no longer owns this transfer fence"))?;
    if destination.holder.is_some() { return Err(inconsistent("A transfer destination already has a writer")); }
    if let Some(terminal) = source.checkpoint_transfer_terminal.as_deref() {
        return Ok(terminal.clone());
    }
    await_storage_drain(directory, storage, keys, &source).await?;
    let stream = administration_stream(directory, &source, 4 << 20).await?;
    let sealed = rtdb_logkeeper::seal_recover_committed(&stream, fence.fenced_writer_epoch).await?.0;
    let first = stage.checkpoint.boundary.checked_add(1)
        .ok_or_else(|| inconsistent("The checkpoint has no replica successor"))?;
    let certificate = if sealed >= first {
        Some(rtdb_logkeeper::certify_committed_range(&stream, first, sealed).await?)
    } else {
        if sealed != stage.checkpoint.boundary {
            return Err(inconsistent("The sealed replica history ends before the checkpoint"));
        }
        None
    };
    let replica_sha256 = certificate.as_ref().map(|certificate|
        certificate.digest_bytes().ok_or_else(|| inconsistent("The terminal interval has no canonical digest"))
    ).transpose()?;

    let mut node = rtdb_node::NodeOptions::default();
    node.owned_relations = true;
    let memory = rtdb_columnar::MemoryBudget::new(node.execution_memory_bytes);
    let cache = rtdb_storage::cache::CacheBudget::new(node.cache_bytes);
    let cancellation = tokio_util::sync::CancellationToken::new();
    let (basis, source_through, lifetime_floors_sha256) = read_world::with_registered_capture_replay(
        directory, archive, keys, hosted_task,
        read_world::CaptureSelection {
            deployment: intent.source, capture: intent.capture,
            manifest: &intent.source_manifest_sha256, suffix: None, expected_basis: None, require_replay: false,
        },
        CheckpointReadResources { node: &node, memory: &memory, cache, cancellation: &cancellation },
        Some(read_world::TerminalReplay { storage, fence, stream: &stream, certificate: certificate.as_ref(), through: sealed }),
        |context, basis, source_through| async move {
            let source_through = source_through.ok_or_else(|| inconsistent("Terminal replay produced no source coordinate"))?;
            Ok((basis, source_through, archive_lifetime_floors_digest(context, basis).await?))
        },
    ).await?;
    directory.record_checkpoint_transfer_terminal(&rtdb_directory::CheckpointTransferTerminal {
        fence: fence.clone(), replica_through: sealed, source_through, basis,
        replica_sha256, lifetime_floors_sha256,
    }).await
}

pub(crate) async fn restore_checkpoint(
    directory: &dyn Directory,
    storage: &CellStorage,
    archive: &CellStorage,
    keys: &dyn KeyService,
    deployment: Uuid,
    operation: Uuid,
    maximum_message_bytes: usize,
    expected_manifest: Option<&str>,
    hosted_task: Option<&rtdb_directory::HostedTask>,
) -> Result<CheckpointRecovery> {
    let require_manifest = |actual: &str| {
        if expected_manifest.is_some_and(|expected| expected != actual) {
            return Err(inconsistent("The restore manifest differs from the accepted capture"));
        }
        Ok(())
    };
    if deployment.is_nil() || operation.is_nil() || same_storage(storage, archive) {
        return Err(inconsistent(
            "Restore requires one deployment, operation and independent archive",
        ));
    }
    let id = deployment.to_string();
    let archive_binding = archive_binding(archive)?;
    let current = directory
        .deployment(&id)
        .await?
        .ok_or_else(|| unavailable("The selected installation is unavailable"))?;
    let read_receipt = |record: &DeploymentRecord| -> Result<Option<CheckpointRecovery>> {
        let Some(recovery) = record.checkpoint_repair_receipts.get(&operation).or_else(|| {
            record.checkpoint_recovery.as_deref().filter(|recovery| {
                recovery.operation == operation && recovery.completed_at_ms.is_some()
            })
        }) else {
            return Ok(None);
        };
        if recovery.operation != operation || recovery.archive != archive_binding
            || recovery.completed_at_ms.is_none()
        {
            return Err(inconsistent("The restore operation has another archive or receipt"));
        }
        require_manifest(&recovery.manifest_sha256)?;
        Ok(Some(recovery.clone()))
    };
    if let Some(receipt) = read_receipt(&current)? {
        return Ok(receipt);
    }
    let (point, manifest_sha256) = capture_receipt(&current, archive, deployment, operation)?
        .ok_or_else(|| unavailable("The checkpoint capture has no retained manifest receipt"))?;
    require_manifest(&manifest_sha256)?;
    if !same_incarnation(&current, &point) || current.checkpoint != Some(point.checkpoint) {
        return Err(inconsistent("The archive is not the current durable checkpoint"));
    }
    keys.existing_key(&current.key_id()).await?
        .ok_or_else(|| unavailable("The current installation key is unavailable"))?;
    // The Directory CAS below still requires this same current checkpoint after
    // validating its authenticated source root. Recovery retains its ordered tail.
    admit_lifetime_restore(
        directory, archive, keys, &point, &manifest_sha256, hosted_task, true, current.serving_replay_capture.is_some(),
    ).await?;

    let fenced = update_installation(directory, &id, |record| {
        if read_receipt(record)?.is_some() {
            return Ok(());
        }
        require_capture_open(record, operation)?;
        if record.has_live_suffix_pins(rtdb_directory::now_ms()) {
            return Err(unavailable("A live suffix pin still retains serving history"));
        }
        if !same_incarnation(record, &point)
            || record.checkpoint != Some(point.checkpoint)
            || record.move_from.is_some()
            || record.serving_replay_capture != current.serving_replay_capture
            || capture_receipt(record, archive, deployment, operation)?
                != Some((point.clone(), manifest_sha256.clone()))
        {
            return Err(inconsistent("The installation changed before restore"));
        }
        if let Some(recovery) = &record.checkpoint_recovery
            && recovery.operation == operation
            && recovery.checkpoint == point.checkpoint
            && recovery.archive == archive_binding
            && recovery.manifest_sha256 == manifest_sha256
        {
            if record.lifecycle == Lifecycle::Restoring && record.epoch == recovery.epoch
                && recovery.completed_at_ms.is_none()
            {
                return Ok(());
            }
        }
        if !matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined) {
            return Err(inconsistent(
                "The installation is under another lifecycle operation",
            ));
        }
        let epoch = record
            .epoch
            .checked_add(1)
            .ok_or_else(|| inconsistent("No writer epochs remain"))?;
        record.checkpoint_recovery = Some(Box::new(CheckpointRecovery {
            operation,
            checkpoint: point.checkpoint,
            epoch,
            previous_lifecycle: record.lifecycle,
            archive: archive_binding.clone(),
            manifest_sha256: manifest_sha256.clone(),
            completed_at_ms: None,
        }));
        record.lifecycle = Lifecycle::Restoring;
        record.epoch = epoch;
        record.holder = None;
        record.move_intent = None;
        Ok(())
    })
    .await?;
    if let Some(receipt) = read_receipt(&fenced)? {
        return Ok(receipt);
    }
    let pending = fenced
        .checkpoint_recovery
        .as_deref()
        .filter(|recovery| {
            recovery.operation == operation && recovery.archive == archive_binding
                && recovery.manifest_sha256 == manifest_sha256 && recovery.epoch == fenced.epoch
                && recovery.completed_at_ms.is_none() && fenced.lifecycle == Lifecycle::Restoring
        })
        .ok_or_else(|| inconsistent("Missing the accepted checkpoint restore"))?;
    await_storage_drain(directory, storage, keys, &fenced).await?;
    let source = administration_stream(directory, &fenced, maximum_message_bytes).await?;
    let sealed = rtdb_logkeeper::seal(&source, pending.epoch).await?;
    let cutoff = ReplicaPosition::from_epoch_offset(StorageEpoch(fenced.log_epoch), sealed.0)?;
    if cutoff.0 < pending.checkpoint.boundary {
        return Err(inconsistent("The sealed replica history ends before the archived checkpoint"));
    }

    let completed = update_installation(directory, &id, |record| {
        if read_receipt(record)?.is_some() {
            return Ok(());
        }
        if record.lifecycle != Lifecycle::Restoring || record.epoch != pending.epoch {
            return Err(inconsistent("The restore lost its fence"));
        }
        let recovery = record
            .checkpoint_recovery
            .as_deref_mut()
            .filter(|recovery| &**recovery == pending)
            .ok_or_else(|| inconsistent("Another checkpoint restore replaced this operation"))?;
        recovery.completed_at_ms = Some(rtdb_directory::now_ms());
        record.lifecycle = recovery.previous_lifecycle;
        record.compaction_replay_cutoff = Some(cutoff.0);
        record.recovery_manifest_sha256 = None;
        record.serving_replay_capture = record.serving_replay_capture.map(|_| operation);
        record.checkpoint_repair_receipts.insert(operation, recovery.clone());
        Ok(())
    })
    .await?;
    read_receipt(&completed)?
        .ok_or_else(|| inconsistent("The completed restore has no receipt"))
}

/// The recovery owner authorizes withdrawal only after the serving
/// installation has been disposed and its independent retention holds end.
pub async fn begin_retirement(directory: &dyn Directory, deployment: Uuid) -> Result<ArchiveRetirement> {
    begin_retirement_at(directory, deployment, None).await
}

pub(crate) async fn begin_retirement_at(
    directory: &dyn Directory,
    deployment: Uuid,
    incarnation: Option<u64>,
) -> Result<ArchiveRetirement> {
    let record = update_installation(directory, &deployment.to_string(), |record| {
        if incarnation.is_some_and(|expected| expected != record.incarnation) {
            return Err(inconsistent("The archive retirement names another residence"));
        }
        if record.lifecycle != Lifecycle::Deleted || record.disposal.is_none() {
            return Err(inconsistent("Recovery retirement requires a serving disposal receipt"));
        }
        if record.has_live_suffix_pins(rtdb_directory::now_ms()) {
            return Err(unavailable("A live suffix pin retains an archived checkpoint"));
        }
        if record.checkpoint_worlds.values().any(|world| world.until.0 > rtdb_directory::now_ms().saturating_mul(1000)) {
            return Err(unavailable("A live world still retains an archived checkpoint"));
        }
        if record.checkpoint_capture_retirements.values().any(|release| release.observed_empty_at_ms.is_none()) {
            return Err(unavailable("An individual checkpoint release is still pending"));
        }
        if record.archive_retirement.is_none() {
            record.archive_retirement = Some(ArchiveRetirement {
                deployment: record.id.clone(),
                epoch: record.epoch,
                authorized_at_ms: rtdb_directory::now_ms(),
                observed_empty_at_ms: None,
            });
        }
        Ok(())
    }).await?;
    record.archive_retirement.ok_or_else(|| inconsistent("Missing the accepted recovery retirement"))
}

/// One registered S3 namespace. The installed provider must verify the
/// media witness and observe all versions and unfinished uploads absent.
#[derive(Serialize)]
pub(crate) struct RemoteArchive {
    pub operation: Uuid,
    pub bucket: String,
    pub region: String,
    pub root: String,
    pub witness_size: u64,
    pub witness_sha256: [u8; 32],
    pub inventory: [u8; 32],
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .try_into()
        .expect("SHA-256 has 32 bytes")
}

fn retirement_fence(record: &DeploymentRecord) -> Result<&ArchiveRetirement> {
    record
        .archive_retirement
        .as_ref()
        .filter(|retirement| {
            record.lifecycle == Lifecycle::Deleted
                && record.disposal.is_some()
                && record.storage_regions.is_empty()
                && retirement.epoch == record.epoch
        })
        .ok_or_else(|| inconsistent("The installation is not fenced for archive retirement"))
}

fn remote_archive(
    record: &DeploymentRecord,
    deployment: Uuid,
    operation: Uuid,
    registration: &CheckpointCapture,
) -> Result<Option<RemoteArchive>> {
    if !registration.media_bound {
        return Ok(None);
    }
    let archive: CellStorage = serde_json::from_str(&registration.archive)
        .map_err(|_| inconsistent("Invalid retained archive binding"))?;
    let CellStorage::S3 { config } = archive else {
        return Ok(None);
    };
    let epoch = retirement_fence(record)?;
    let point = capture_point(record, deployment, operation, &registration.archive)?;
    let witness = encode(&point)?;
    let inventory = encode(&(
        epoch.epoch,
        epoch.authorized_at_ms,
        &registration.archive,
        &point,
    ))?;
    let root = CellStorage::s3_root(&config)?.to_string();
    if config.endpoint.is_some() || config.credentials.is_some() || config.remote.is_some() {
        return Err(unavailable(
            "The archive requires its own provider's disposal authority",
        ));
    }
    Ok(Some(RemoteArchive {
        operation,
        bucket: config.bucket,
        region: config
            .region
            .ok_or_else(|| inconsistent("The archive registration has no AWS region"))?,
        root,
        witness_size: witness.len() as u64,
        witness_sha256: sha256(&witness),
        inventory: sha256(&inventory),
    }))
}

pub(crate) async fn next_remote_archive(
    directory: &dyn Directory,
    deployment: Uuid,
) -> Result<Option<RemoteArchive>> {
    let record = directory
        .deployment(&deployment.to_string())
        .await?
        .ok_or_else(|| unavailable("The selected installation is unavailable"))?;
    retirement_fence(&record)?;
    for (&operation, registration) in &record.checkpoint_captures {
        if record.checkpoint_capture_retirements.get(&operation).is_some_and(|release| release.observed_empty_at_ms.is_some()) {
            continue;
        }
        if registration.remote_disposal.is_none()
            && let Some(work) = remote_archive(&record, deployment, operation, registration)?
        {
            return Ok(Some(work));
        }
    }
    Ok(None)
}

pub(crate) async fn confirm_remote_archive(
    directory: &dyn Directory,
    deployment: Uuid,
    epoch: u64,
    operation: Uuid,
    inventory: [u8; 32],
) -> Result<()> {
    update_installation(directory, &deployment.to_string(), |record| {
        if retirement_fence(record)?.epoch != epoch {
            return Err(inconsistent(
                "The archive observation names another retirement fence",
            ));
        }
        let registration = record
            .checkpoint_captures
            .get(&operation)
            .ok_or_else(|| inconsistent("The archive observation names an unknown operation"))?;
        let work = remote_archive(record, deployment, operation, registration)?
            .filter(|work| work.inventory == inventory)
            .ok_or_else(|| inconsistent("The archive observation names another inventory"))?;
        let registration = record
            .checkpoint_captures
            .get_mut(&operation)
            .expect("the archive registration exists");
        if registration
            .remote_disposal
            .is_some_and(|accepted| accepted != work.inventory)
        {
            return Err(inconsistent("The archive observation changed"));
        }
        registration.remote_disposal = Some(work.inventory);
        Ok(())
    })
    .await?;
    Ok(())
}

/// Remove registered local archives and require a provider observation for
/// each registered remote one before recording the whole recovery receipt.
pub async fn finish_retirement(
    directory: &dyn Directory,
    keys: &dyn KeyService,
    deployment: Uuid,
) -> Result<ArchiveRetirement> {
    let id = deployment.to_string();
    let record = directory.deployment(&id).await?
        .ok_or_else(|| unavailable("The selected installation is unavailable"))?;
    let accepted = retirement_fence(&record)?.clone();
    if accepted.observed_empty_at_ms.is_some() {
        return Ok(accepted);
    }
    let mut remote = false;
    for (&operation, registration) in &record.checkpoint_captures {
        if !registration.media_bound || record.checkpoint_capture_retirements.get(&operation)
            .is_some_and(|release| release.observed_empty_at_ms.is_some()) {
            continue;
        }
        let archive: CellStorage = serde_json::from_str(&registration.archive)
            .map_err(|_| inconsistent("Invalid retained archive binding"))?;
        if !matches!(archive, CellStorage::Local { .. }) {
            let work = remote_archive(&record, deployment, operation, registration)?
                .ok_or_else(|| inconsistent("Missing remote archive inventory"))?;
            remote |= registration.remote_disposal != Some(work.inventory);
            continue;
        }
        let bucket = archive.bucket(IoRegion::new()).await?;
        let point = capture_point(&record, deployment, operation, &registration.archive)?;
        let witness = archive_witness(deployment, operation)?;
        if read::<CapturePoint>(bucket.as_ref(), &witness).await?.as_ref() != Some(&point) {
            return Err(unavailable("Cannot identify the retained checkpoint archive media"));
        }
        let prefix = key(&format!("checkpoint-captures/{id}/{operation}"))?;
        for object in bucket.list(Some(&prefix)).await? {
            bucket.delete(&object.key).await?;
        }
        if !bucket.list(Some(&prefix)).await?.is_empty() {
            return Err(unavailable("The local checkpoint archive still has objects"));
        }
    }
    if remote {
        return Err(unavailable("The archive provider must account for retained versions and uploads"));
    }

    for (&operation, registration) in &record.checkpoint_captures {
        if registration.media_bound && !record.checkpoint_capture_retirements.get(&operation)
            .is_some_and(|release| release.observed_empty_at_ms.is_some()) {
            let point = capture_point(&record, deployment, operation, &registration.archive)?;
            keys.destroy(&archive_key_id(&point)).await?;
        }
    }

    let settled = update_installation(directory, &id, |current| {
        retirement_fence(current)?;
        if current.checkpoint_captures != record.checkpoint_captures {
            return Err(inconsistent("The archive retirement inventory changed"));
        }
        let retirement = current.archive_retirement.as_mut()
            .filter(|retirement| retirement.epoch == accepted.epoch
                && retirement.authorized_at_ms == accepted.authorized_at_ms)
            .ok_or_else(|| inconsistent("The archive retirement lost its fence"))?;
        retirement.observed_empty_at_ms.get_or_insert_with(|| {
            rtdb_directory::now_ms().max(accepted.authorized_at_ms)
        });
        Ok(())
    }).await?;
    settled.archive_retirement.ok_or_else(|| inconsistent("Missing the archive retirement receipt"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_directory::{DisposalReceipt, MemoryDirectory};

    struct ReplayFixture {
        point: CapturePoint,
        root: ObjectKey,
        table: ObjectKey,
        artefact: ObjectKey,
        retained: [ObjectKey; 2],
        files: BTreeMap<ObjectKey, Bytes>,
    }

    fn replay_fixture() -> Result<ReplayFixture> {
        use prost::Message;
        use rtdb_format::{block::proto, catalog::TableEntry, keys::TrieKey, log::proto::{ArtefactRecord, ArtefactScope, TrieDetails, TrieState}};
        let selected = BlockIndex(2);
        let entry = TableEntry::new(16_384, rtdb_types::TableRef::new("public", "docs"))?;
        let l0 = TrieKey::l0(BlockIndex(0))?;
        let member = TrieKey::new(2, None, vec![0], selected)?;
        let shared = ArtefactRecord { kind: Some("search".into()), definition_id: Some("body".into()), definition_version: Some(1),
            scope: Some(ArtefactScope::PartGroup as i32), ..Default::default() };
        let table_bytes = proto::TableBlock {
            row_count: Some(1),
            tries: vec![
                TrieDetails { trie_key: Some(l0.to_string()), trie_state: Some(TrieState::Garbage as i32), ..Default::default() },
                TrieDetails { trie_key: Some(member.to_string()), trie_state: Some(TrieState::Live as i32), artefacts: vec![shared.clone()], ..Default::default() },
            ],
            ..Default::default()
        }.encode_to_vec();
        let root_bytes = proto::Block {
            block_index: Some(selected.0), boundary_replica_msg_id: Some(8), latest_processed_msg_id: Some(5),
            tables: vec![entry.to_proto()],
            table_block_manifest: Some(proto::TableBlockManifest { sha256_by_oid: BTreeMap::from([(entry.oid, publication_digest(&table_bytes).to_vec())]) }),
            ..Default::default()
        }.encode_to_vec();
        let point = CapturePoint { deployment: Uuid::new_v4(), operation: Uuid::new_v4(), incarnation: 1, log_epoch: 0,
            checkpoint: Checkpoint { block: selected.0, boundary: 8, source: 5, system_time: i64::MIN, root_sha256: Some(publication_digest(&root_bytes)) } };
        let root = key(&block_key(selected)?)?;
        let table = key(&entry.slug.block_key(selected)?)?;
        let artefact = key(&entry.slug.recorded_artefact_key(&member, &shared)?)?;
        let retained = [key(&entry.slug.data_key(&l0))?, key(&entry.slug.metadata_key(&l0))?];
        let files = BTreeMap::from([
            (root.clone(), Bytes::from(root_bytes)), (table.clone(), Bytes::from(table_bytes)),
            (artefact.clone(), Bytes::from_static(b"shared search file")),
            (retained[0].clone(), Bytes::from_static(b"retained rows")), (retained[1].clone(), Bytes::from_static(b"retained metadata")),
            (key(&entry.slug.data_key(&member))?, Bytes::from_static(b"current rows")),
            (key(&entry.slug.metadata_key(&member))?, Bytes::from_static(b"current metadata")),
        ]);
        Ok(ReplayFixture { point, root, table, artefact, retained, files })
    }

    #[tokio::test]
    async fn replay_archives_require_all_dependencies_and_open_old_primary_manifests() -> Result<()> {
        let fixture = replay_fixture()?;
        let bucket: Arc<dyn ObjectStore> = Arc::new(rtdb_storage::MemoryStore::default());
        let files = PrefixedStore::new(bucket.clone(), "capture/objects");
        let material = [3; crate::keys::KEY_BYTES];
        let cipher = Cipher::new(&material)?;
        let mut objects = Vec::new();
        for (name, bytes) in &fixture.files {
            let ciphertext = cipher.seal_object(name, bytes)?;
            objects.push(ArchivedObject::from_sealed(name, &ciphertext)?);
            files.put(name, ciphertext).await?;
        }
        let capture = Capture { point: fixture.point, format: CheckpointContents::Replay, objects };
        capture.verify(bucket.clone(), "capture", &material).await?;
        let mut omitted = capture.clone();
        omitted.objects.retain(|object| object.name != fixture.artefact.as_str());
        let error = omitted.verify(bucket.clone(), "capture", &material).await.err();
        assert_eq!(error.and_then(|error| error.message).as_deref(), Some("The archive object list differs from the checkpoint closure"),
            "a replay manifest must name the shared artefact");
        files.delete(&fixture.artefact).await?;
        let error = capture.verify(bucket.clone(), "capture", &material).await.unwrap_err();
        assert_eq!(error.code, "rtdb.storage/missing-object");

        let mut primary = omitted;
        primary.objects.retain(|object| fixture.retained.iter().all(|name| name.as_str() != object.name));
        primary.format = CheckpointContents::Primary;
        let legacy = encode(&primary)?;
        assert!(serde_json::from_slice::<serde_json::Value>(&legacy).unwrap().get("format").is_none());
        let reopened: Capture = serde_json::from_slice(&legacy).unwrap();
        assert_eq!(encode(&reopened)?, legacy);
        reopened.verify(bucket, "capture", &material).await?;
        Ok(())
    }

    #[tokio::test]
    async fn serving_recovery_manifest_requires_the_complete_inventory_and_stable_versions() -> Result<()> {
        let fixture = replay_fixture()?;
        let store = crate::packing::tests::VersionedPacks::default();
        let checkpoint = fixture.point.checkpoint;
        let epoch = StorageEpoch(fixture.point.log_epoch);
        for (object, bytes) in &fixture.files {
            store.put(object, bytes.clone()).await?;
        }
        let published = recovery_manifest_sha256(&store, checkpoint, epoch).await?;
        assert_eq!(recovery_manifest_sha256(&store, checkpoint, epoch).await?, published);

        for required in [fixture.artefact, fixture.retained[0].clone()] {
            store.delete(&required).await?;
            assert_eq!(recovery_manifest_sha256(&store, checkpoint, epoch).await.unwrap_err().code,
                "rtdb.storage/missing-object");
            store.put(&required, fixture.files[&required].clone()).await?;
        }
        assert_ne!(recovery_manifest_sha256(&store, checkpoint, epoch).await?, published);
        Ok(())
    }

    #[tokio::test]
    async fn a_checkpoint_copy_plan_shares_a_pack_observation() -> Result<()> {
        let raw = Arc::new(crate::packing::tests::VersionedPacks::default());
        let store = crate::packing::PackingStore::open(raw.clone(), PackDiscovery::List).await?;
        let root = key(&block_key(BlockIndex(0))?)?;
        let table = key("tables/t/blocks/b00.binpb")?;
        store.put(&table, Bytes::from_static(b"table")).await?;
        store.put(&root, Bytes::from_static(b"root")).await?;
        let before = raw.head_observations();
        let plan = plan_checkpoint_copy(store.as_ref(), BTreeMap::from([(root, None), (table, None)]), None).await?;
        assert_eq!(raw.head_observations() - before, 1);
        assert!(plan[0].version.is_some());
        assert_eq!(plan[0].version, plan[1].version);
        Ok(())
    }

    #[tokio::test]
    async fn a_checkpoint_copy_checks_the_planned_physical_publication() -> Result<()> {
        use std::sync::atomic::Ordering::SeqCst;

        let raw = Arc::new(crate::packing::tests::VersionedPacks::default());
        let storage = PrefixedStore::new(raw.clone(), "serving/v06");
        let store = EncryptingStore::new(Arc::new(storage), &[5; crate::keys::KEY_BYTES])?;
        let name = key("tables/t/data/example.arrow")?;
        let bytes = Bytes::from_static(b"observed bytes");
        store.put(&name, bytes.clone()).await?;
        let closure = BTreeMap::from([(name.clone(), Some(publication_digest(&bytes)))]);
        let budget = crate::cipher::object::sealed_size(&name, bytes.len())? as u64;
        let plan = plan_checkpoint_copy(&store, closure, Some(budget)).await?;
        assert_eq!(plan[0].version.as_ref().map(|identity| identity.key.as_str()),
            Some("serving/v06/tables/t/data/example.arrow"),
            "the source plan must retain the physical storage identity");
        assert_eq!(plan[0].read(&store).await?, bytes);

        raw.change_on_exact.store(true, SeqCst);
        let error = plan[0].read(&store).await.err().and_then(|err| err.message);
        assert_eq!(error.as_deref(), Some("Checkpoint source publication changed during copying"),
            "an identical body under another version cannot satisfy a source plan");
        Ok(())
    }

    #[tokio::test]
    async fn retained_archive_serves_an_authenticated_trie_range_with_its_manifest() -> Result<()> {
        use crate::cipher::object::FRAME_BYTES;

        let provider = Arc::new(crate::packing::tests::VersionedPacks::default());
        provider.limit_read_size((2 * (FRAME_BYTES + crate::cipher::SEALED_OVERHEAD)) as u64);
        let raw: Arc<dyn ObjectStore> = provider;
        let name = key("tables/orders/data/l00-rc-b00.arrow")?;
        let plaintext = vec![5; FRAME_BYTES * 4 + 19];
        let archive_cipher = Cipher::new(&[3; crate::keys::KEY_BYTES])?;
        let material = [7; crate::keys::KEY_BYTES];
        let sealed = archive_cipher.seal_object(&name, &plaintext)?;
        let files = PrefixedStore::new(Arc::clone(&raw), "capture/objects");
        files.put(&name, sealed.clone()).await?;
        let archived = ArchivedObject::from_sealed(&name, &sealed)?;
        let read = |object| -> Result<EncryptingStore> {
            let archive = ArchivedCheckpoint {
                files: PrefixedStore::new(Arc::clone(&raw), "capture/objects"),
                audience_transport: true,
                objects: BTreeMap::from([(name.clone(), object)]),
                archive_cipher: Cipher::new(&[3; crate::keys::KEY_BYTES])?,
                serving_cipher: Cipher::new(&material)?,
            };
            EncryptingStore::new(archive.with_serving(Arc::new(rtdb_storage::MemoryStore::default())), &material)
        };
        let range = FRAME_BYTES - 11..FRAME_BYTES + 17;
        let store = read(archived.clone())?;
        let received = store.get_range(&name, range.start as u64..range.end as u64).await?;
        assert_eq!(received.as_ref(), &plaintext[range.clone()]);
        assert!(matches!(store.get(&name).await, Err(Error::PermissionDenied { .. })));

        let substituted = ArchivedObject { frame_header_sha256: Some("different".into()), ..archived };
        let store = read(substituted)?;
        assert!(matches!(store.get_range(&name, range.start as u64..range.end as u64).await,
            Err(Error::ConflictingObject(_))));
        Ok(())
    }

    #[tokio::test]
    async fn a_legacy_original_matches_the_same_plaintext_in_a_framed_archive() -> Result<()> {
        let name = key("tables/t/data/l00-rc-b00.arrow")?;
        let archive_cipher = Cipher::new(&[3; crate::keys::KEY_BYTES])?;
        let serving_cipher = Cipher::new(&[7; crate::keys::KEY_BYTES])?;
        let ciphertext = archive_cipher.seal_object(&name, b"first input")?;
        let files = PrefixedStore::new(Arc::new(rtdb_storage::MemoryStore::default()), "archive/objects");
        files.put(&name, ciphertext.clone()).await?;
        let original = serving_cipher.seal_deterministic(name.as_str(), b"first input")?;
        let substituted = serving_cipher.seal_deterministic(name.as_str(), b"other input")?;
        let archive = ArchivedCheckpoint {
            files, audience_transport: false,
            objects: BTreeMap::from([(name.clone(), ArchivedObject::from_sealed(&name, &ciphertext)?)]),
            archive_cipher, serving_cipher,
        };
        assert!(archive.matches_original(&name, &original).await?);
        assert!(!archive.matches_original(&name, &substituted).await?);
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_copy_plans_from_the_repair_archive_custody() -> Result<()> {
        let raw: Arc<dyn ObjectStore> = Arc::new(crate::packing::tests::VersionedPacks::default());
        let files = PrefixedStore::new(raw, "accepted-archive/objects");
        let name = key("covered")?;
        let material = [7; crate::keys::KEY_BYTES];
        let archive_cipher = Cipher::new(&[3; crate::keys::KEY_BYTES])?;
        let bytes = Bytes::from_static(b"accepted");
        let ciphertext = archive_cipher.seal_deterministic(name.as_str(), &bytes)?;
        files.put(&name, ciphertext.clone()).await?;
        let archive = ArchivedCheckpoint {
            files,
            audience_transport: false,
            objects: BTreeMap::from([(name.clone(), ArchivedObject {
                name: name.to_string(), bytes: ciphertext.len() as u64, sha256: digest(&ciphertext), frame_header_sha256: None,
            })]),
            archive_cipher,
            serving_cipher: Cipher::new(&material)?,
        };
        let store = EncryptingStore::new(
            archive.with_serving(Arc::new(rtdb_storage::MemoryStore::default())), &material,
        )?;
        let source = CheckpointCopySource::observe(&store, name.clone(), None).await?;
        assert_eq!(source.version.as_ref().map(|version| version.key.as_str()),
            Some("accepted-archive/objects/covered"), "repair bytes must name archive custody");
        assert_eq!(source.read(&store).await?, bytes);
        assert_eq!(store.retirement_version(&name).await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn archive_discovery_refuses_substituted_table_bytes() -> Result<()> {
        let fixture = replay_fixture()?;
        let store = rtdb_storage::MemoryStore::default();
        store.put(&fixture.root, fixture.files[&fixture.root].clone()).await?;
        let different = Bytes::new();
        assert_eq!(TableBlock::decode(&different)?.row_count(), 0);
        store.put(&fixture.table, different).await?;
        let error = checkpoint_objects(&store, &fixture.point, CheckpointContents::Replay).await.err();
        assert_eq!(error.and_then(|error| error.message).as_deref(), Some("Checkpoint table bytes do not match the root manifest"),
            "archive discovery must authenticate a decodable table block before using its dependencies");
        Ok(())
    }

    #[tokio::test]
    async fn an_independent_fork_copies_the_replay_closure_before_its_root() -> Result<()> {
        let fixture = replay_fixture()?;
        let bucket: Arc<dyn ObjectStore> = Arc::new(rtdb_storage::MemoryStore::default());
        let mut source = DeploymentRecord::new("source", "org", Vec::new(), 0);
        source.checkpoint = Some(fixture.point.checkpoint);
        let child = DeploymentRecord::new("child", "other", Vec::new(), 0);
        let incomplete = DeploymentRecord::new("incomplete", "other", Vec::new(), 0);
        let version = checkpoint_storage_root()?;
        let scope = |record: &DeploymentRecord| format!("{}/{version}", record.storage_prefix());
        let source_physical = PrefixedStore::new(bucket.clone(), scope(&source));
        crate::store::check_root(&source_physical, &source.id, true).await?;
        let source_material = [3; crate::keys::KEY_BYTES];
        let child_material = [7; crate::keys::KEY_BYTES];
        let serving = open_deployment_store(bucket.clone(), &scope(&source), PackDiscovery::List, wall_clock()).await?;
        let source_store = EncryptingStore::new(serving, &source_material)?;
        for (name, bytes) in fixture.files.iter().filter(|(name, _)| *name != &fixture.root) {
            source_store.put(name, bytes.clone()).await?;
        }
        source_store.put(&fixture.root, fixture.files[&fixture.root].clone()).await?;
        copy_fork_checkpoint(bucket.clone(), &source, &source_material, &child, &child_material, None).await?;
        let serving = open_deployment_store(bucket.clone(), &scope(&child), PackDiscovery::List, wall_clock()).await?;
        let copied = EncryptingStore::new(serving, &child_material)?;
        for (name, bytes) in &fixture.files {
            assert_eq!(&copied.get_fresh(name).await.expect("the fork must contain every replay dependency"), bytes);
        }

        source_physical.delete(&fixture.artefact).await?;
        let failure = copy_fork_checkpoint(bucket.clone(), &source, &source_material, &incomplete, &child_material, None).await;
        assert_eq!(failure.unwrap_err().code, "rtdb.storage/missing-object");
        let serving = open_deployment_store(bucket, &scope(&incomplete), PackDiscovery::List, wall_clock()).await?;
        assert!(matches!(serving.head_fresh(&fixture.root).await, Err(Error::MissingObject(_))),
            "a fork with an unavailable dependency must have no root");
        Ok(())
    }

    #[tokio::test]
    async fn a_capture_can_inherit_only_continuously_retained_source_metadata() -> Result<()> {
        let fixture = replay_fixture()?;
        let serving = tempfile::tempdir().unwrap();
        let archived = tempfile::tempdir().unwrap();
        let key_root = tempfile::tempdir().unwrap();
        let storage = CellStorage::Local { root: serving.path().to_owned() };
        let archive = CellStorage::Local { root: archived.path().to_owned() };
        let keys = crate::keys::LocalKeyService::new(key_root.path())?;
        let directory = rtdb_directory::MemoryDirectory::new();
        let mut record = DeploymentRecord::new("captured", "acme", Vec::new(), 0);
        let deployment = record.id.parse::<Uuid>().unwrap();
        let checkpoint = fixture.point.checkpoint;
        record.checkpoint = Some(Checkpoint {
            block: checkpoint.block + 1,
            boundary: checkpoint.boundary + 1,
            root_sha256: None,
            ..checkpoint
        });
        let operation = Uuid::new_v4();
        record.checkpoint_captures.insert(operation, CheckpointCapture {
            archive: archive_binding(&archive)?,
            incarnation: record.incarnation,
            log_epoch: record.log_epoch,
            checkpoint,
            automatic_fork: false,
            world_replay: None,
            media_bound: false,
            manifest_sha256: None,
            remote_disposal: None,
        });
        let bucket = storage.bucket(IoRegion::new()).await?;
        let root = format!("{}/{}", record.storage_prefix(), checkpoint_storage_root()?);
        let physical = PrefixedStore::new(bucket.clone(), &root);
        crate::store::check_root(&physical, &record.id, true).await?;
        let packed = open_deployment_store(bucket, &root, PackDiscovery::List, wall_clock()).await?;
        let material = keys.key(&record.key_id()).await?;
        let source = EncryptingStore::new(packed, &material)?;
        for (name, bytes) in fixture.files.iter().filter(|(name, _)| *name != &fixture.root) {
            source.put(name, bytes.clone()).await?;
        }
        source.put(&fixture.root, fixture.files[&fixture.root].clone()).await?;
        directory.create_deployment(record).await?;
        let attempt = || capture(directory.clone(), &storage, &archive, &keys, deployment, operation, None, None);

        let refusal = attempt().await.err().and_then(|err| err.message);
        assert_eq!(refusal.as_deref(), Some("The capture's original checkpoint metadata is no longer retained"),
            "a capture must acquire source metadata custody before opening an older root");

        let predecessor = Uuid::new_v4();
        let id = deployment.to_string();
        update_installation(directory.as_ref(), &id, |record| {
            record.storage_regions.insert(predecessor, rtdb_directory::StorageRegion::Local {
                archive: None, checkpoint_suffix: None, repair_archive: None,
                tracks_ordered_index_pages: false,
                checkpoint_metadata_from: Some(checkpoint.block),
                checkpoint_replay_from: None,
            });
            Ok(())
        }).await?;
        let completed = attempt().await.expect("a live predecessor may hand off source metadata custody");
        assert_eq!(completed.point.checkpoint, checkpoint);
        let current = directory.deployment(&id).await?.unwrap();
        assert!(current.storage_regions.keys().eq([&predecessor]),
            "the capture releases only its own protection after the copy completes");
        update_installation(directory.as_ref(), &id, |record| {
            record.storage_regions.remove(&predecessor);
            Ok(())
        }).await?;
        assert_eq!(attempt().await?, completed, "a durable receipt can be reopened after the source hold ends");
        {
            let _maintenance = storage.try_local_maintenance()?.unwrap();
            let busy = capture(directory.clone(), &storage, &archive, &keys, deployment, Uuid::new_v4(), None, None)
                .await.err().map(|error| error.code);
            assert_eq!(busy.as_deref(), Some("rtdb.multi/storage-maintenance-busy"),
                "a local capture must share the source maintenance authority");
        }
        assert!(directory.deployment(&id).await?.unwrap().storage_regions.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn repaired_reads_verify_and_reseal_the_accepted_archive() -> Result<()> {
        let archive_bucket = Arc::new(rtdb_storage::MemoryStore::default());
        let files = PrefixedStore::new(archive_bucket, "archive");
        let serving = Arc::new(rtdb_storage::MemoryStore::default());
        let covered = key("covered")?;
        let newer = key("newer")?;
        let archive_cipher = Cipher::new(&[3; crate::keys::KEY_BYTES])?;
        let serving_cipher = Cipher::new(&[7; crate::keys::KEY_BYTES])?;
        let ciphertext = archive_cipher.seal_deterministic(covered.as_str(), b"accepted")?;
        let want = serving_cipher.seal_deterministic(covered.as_str(), b"accepted")?;
        files.put(&covered, ciphertext.clone()).await?;
        serving.put(&covered, Bytes::from_static(b"rewritten")).await?;
        serving.put(&newer, Bytes::from_static(b"current tail")).await?;
        let archived = ArchivedCheckpoint {
            files,
            audience_transport: false,
            objects: [(covered.clone(), ArchivedObject {
                name: covered.to_string(), bytes: ciphertext.len() as u64, sha256: digest(&ciphertext), frame_header_sha256: None,
            })].into(),
            archive_cipher,
            serving_cipher,
        };
        let store = archived.with_serving(serving);
        assert_ne!(ciphertext, want);
        assert_eq!(store.head(&covered).await?.size, want.len() as u64);
        assert_eq!(store.head_fresh(&covered).await?.size, want.len() as u64);
        assert_eq!(store.get(&covered).await?, want);
        assert_eq!(store.get_range(&covered, 2..6).await?, want.slice(2..6));
        assert_eq!(store.list(None).await?[0].size, want.len() as u64);
        assert_eq!(store.get(&newer).await?.as_ref(), b"current tail");
        Ok(())
    }

    #[tokio::test]
    async fn s3_archive_retirement_requires_the_observation_for_its_registered_inventory()
    -> Result<()> {
        let directory = MemoryDirectory::new();
        let vault = tempfile::tempdir().unwrap();
        let keys = crate::keys::LocalKeyService::new(vault.path())?;
        let mut record = DeploymentRecord::new("archive-fixture", "acme", Vec::new(), 0);
        let deployment = Uuid::parse_str(&record.id).expect("deployment ID is a UUID");
        let operation = Uuid::new_v4();
        let archive =
            CellStorage::parse("s3://archive-fixture/histories/example?region=us-east-1")?;
        record.epoch = 4;
        record.lifecycle = Lifecycle::Deleted;
        record.disposal = Some(Box::new(DisposalReceipt {
            deployment: record.id.clone(),
            incarnation: record.incarnation,
            epoch: record.epoch,
            journals: Default::default(),
            journals_retired_at_ms: 1,
            key_withdrawn_at_ms: 1,
            serving_prefix_observed_empty_at_ms: 1,
            serving_inventory: rtdb_directory::ServingInventory::S3VersionsAndUploads,
        }));
        record.checkpoint_captures.insert(
            operation,
            CheckpointCapture {
                archive: archive_binding(&archive)?,
                incarnation: record.incarnation,
                log_epoch: record.log_epoch,
                checkpoint: Checkpoint {
                    block: 1,
                    boundary: 3,
                    source: 2,
                    system_time: 100,
                    root_sha256: None,
                },
                automatic_fork: false,
                world_replay: None,
                media_bound: true,
                manifest_sha256: None,
                remote_disposal: None,
            },
        );
        let point = capture_point(&record, deployment, operation, &archive_binding(&archive)?)?;
        let capture_key = archive_key_id(&point);
        let material = keys.key(&capture_key).await?;
        keys.destroy(&record.key_id()).await?;
        assert_eq!(keys.existing_key(&capture_key).await?, Some(material));
        directory.create_deployment(record).await?;

        let scheduled = begin_retirement(directory.as_ref(), deployment).await?;
        assert!(
            finish_retirement(directory.as_ref(), &keys, deployment)
                .await
                .is_err()
        );
        let work = next_remote_archive(directory.as_ref(), deployment)
            .await?
            .expect("provider work");
        assert_eq!(work.operation, operation);
        assert_eq!(work.bucket, "archive-fixture");
        assert_eq!(work.region, "us-east-1");
        assert_eq!(work.root, "histories/example/cell");
        assert!(
            confirm_remote_archive(
                directory.as_ref(),
                deployment,
                scheduled.epoch + 1,
                operation,
                work.inventory
            )
            .await
            .is_err()
        );
        assert!(
            confirm_remote_archive(
                directory.as_ref(),
                deployment,
                scheduled.epoch,
                operation,
                [0; 32]
            )
            .await
            .is_err()
        );
        for _ in 0..2 {
            confirm_remote_archive(
                directory.as_ref(),
                deployment,
                scheduled.epoch,
                operation,
                work.inventory,
            )
            .await?;
        }
        assert!(
            next_remote_archive(directory.as_ref(), deployment)
                .await?
                .is_none()
        );
        let settled = finish_retirement(directory.as_ref(), &keys, deployment).await?;
        assert!(keys.existing_key(&capture_key).await.is_err());
        assert_eq!(settled.authorized_at_ms, scheduled.authorized_at_ms);
        assert!(settled.observed_empty_at_ms.is_some());
        assert_eq!(
            finish_retirement(directory.as_ref(), &keys, deployment).await?,
            settled
        );
        Ok(())
    }
}
