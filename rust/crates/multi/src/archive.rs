//! Immutable, ciphertext-only captures of published checkpoints.
use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    ops::Range,
    path::Path,
    sync::Arc,
};

use base64::{
    engine::general_purpose::URL_SAFE_NO_PAD,
    Engine,
};
use bytes::Bytes;
use rtdb_directory::{
    Checkpoint,
    CheckpointRecovery,
    DeploymentRecord,
    Directory,
    Lifecycle,
};
use rtdb_format::{
    catalog::{
        BlockCatalog,
        TableBlock,
    },
    keys::{
        block_key,
        StorageRoot,
        TrieKey,
        CURRENT_STORAGE_VERSION,
    },
    log::proto::TrieState,
};
use rtdb_storage::{
    object_store::StoredObject,
    Error,
    IoRegion,
    ObjectKey,
    ObjectStore,
};
use rtdb_types::{
    Anomaly,
    AnomalyCategory,
    BlockIndex,
    Result,
    StorageEpoch,
};
use serde::{
    Deserialize,
    Serialize,
};
use uuid::Uuid;

use crate::{
    admin::{
        administration_stream,
        update_installation,
    },
    cipher::Cipher,
    deployment::CellStorage,
    keys::KeyService,
    packing::PackDiscovery,
    reference::{
        key,
        PrefixedStore,
    },
    store::{
        open_deployment_store_with_archive,
        wall_clock,
        EncryptingStore,
        ROOT_MARKER,
    },
};

const MAXIMUM_BYTES: u64 = 1 << 30;
const MAXIMUM_MANIFEST_BYTES: u64 = 16 << 20;

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
    if digest(&ciphertext) != object.sha256 {
        return Err(Error::ConflictingObject(name.to_string()));
    }
    Ok(ciphertext)
}

/// Restored checkpoints are kept in their immutable archive. Only the
/// directory's accepted manifest can supply a missing serving object.
pub(crate) struct ArchivedCheckpoint {
    files: PrefixedStore,
    objects: BTreeMap<ObjectKey, ArchivedObject>,
}

impl ArchivedCheckpoint {
    pub(crate) async fn load(
        record: &DeploymentRecord,
        region: Arc<IoRegion>,
    ) -> Result<Option<Self>> {
        let Some(recovery) = record
            .checkpoint_recovery
            .as_ref()
            .filter(|restore| restore.completed_at_ms.is_some())
        else {
            return Ok(None);
        };
        let archive: CellStorage = serde_json::from_str(&recovery.archive)
            .map_err(|_| inconsistent("Invalid retained archive binding"))?;
        let bucket = archive.bucket(region).await?;
        let prefix = format!("checkpoint-captures/{}/{}", record.id, recovery.operation);
        let scope = PrefixedStore::new(bucket.clone(), prefix.clone());
        let capture = read::<Capture>(&scope, &key("capture.json")?)
            .await?
            .ok_or_else(|| unavailable("The recovery archive is unavailable"))?;
        if !same_incarnation(record, &capture.point)
            || capture.point.operation != recovery.operation
            || capture.point.checkpoint != recovery.checkpoint
            || digest(&encode(&capture)?) != recovery.manifest_sha256
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
            objects,
        }))
    }

    pub(crate) fn with_serving(self, serving: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(CheckpointStore {
            serving,
            archive: self,
        })
    }
}

struct CheckpointStore {
    serving: Arc<dyn ObjectStore>,
    archive: ArchivedCheckpoint,
}

impl CheckpointStore {
    fn entry(&self, key: &ObjectKey) -> rtdb_storage::Result<&ArchivedObject> {
        self.archive
            .objects
            .get(key)
            .ok_or_else(|| Error::MissingObject(key.to_string()))
    }

    async fn archived(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> {
        read_object(&self.archive.files, key, self.entry(key)?).await
    }
}

#[async_trait::async_trait]
impl ObjectStore for CheckpointStore {
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        match self.serving.head(key).await {
            Err(Error::MissingObject(_)) => Ok(StoredObject {
                key: key.clone(),
                size: self.entry(key)?.bytes,
            }),
            result => result,
        }
    }

    async fn head_fresh(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        match self.serving.head_fresh(key).await {
            Err(Error::MissingObject(_)) => {
                let metadata = self.archive.files.head_fresh(key).await?;
                if metadata.size != self.entry(key)?.bytes {
                    return Err(Error::ConflictingObject(key.to_string()));
                }
                Ok(metadata)
            },
            result => result,
        }
    }

    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> {
        match self.serving.get(key).await {
            Err(Error::MissingObject(_)) => self.archived(key).await,
            result => result,
        }
    }

    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
        match self.serving.get_range(key, range.clone()).await {
            Err(Error::MissingObject(_)) => {
                let bytes = self.archived(key).await?;
                if range.start > range.end || range.end > bytes.len() as u64 {
                    return Err(Error::InvalidRange {
                        key: key.to_string(),
                        start: range.start,
                        end: range.end,
                        length: bytes.len() as u64,
                    });
                }
                Ok(bytes.slice(range.start as usize..range.end as usize))
            },
            result => result,
        }
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
                objects.entry(key.clone()).or_insert_with(|| StoredObject {
                    key: key.clone(),
                    size: object.bytes,
                });
            }
        }
        Ok(objects.into_values().collect())
    }

    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> {
        if let Some(object) = self.archive.objects.get(key) {
            if digest(&bytes) != object.sha256 {
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
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Capture {
    pub point: CapturePoint,
    pub objects: Vec<ArchivedObject>,
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

fn verify_cut(catalog: &BlockCatalog, point: &CapturePoint) -> Result<()> {
    let checkpoint = point.checkpoint;
    let epoch = StorageEpoch(point.log_epoch);
    if catalog.block_index().0 != checkpoint.block
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

async fn checkpoint_objects(
    plaintext: &dyn ObjectStore,
    point: &CapturePoint,
) -> Result<BTreeSet<ObjectKey>> {
    let block = BlockIndex(point.checkpoint.block);
    let catalog_key = key(&block_key(block)?)?;
    let catalog = BlockCatalog::decode(&plaintext.get(&catalog_key).await?)?;
    verify_cut(&catalog, point)?;
    let mut closure = BTreeSet::from([catalog_key]);
    for table in catalog.table_entries()? {
        let path = key(&table.slug.block_key(block)?)?;
        let metadata = TableBlock::decode(&plaintext.get(&path).await?)?;
        closure.insert(path);
        let legacy = if metadata.partitions().is_empty() {
            metadata.as_proto().tries.as_slice()
        } else {
            &[]
        };
        for detail in metadata
            .partitions()
            .iter()
            .flat_map(|partition| &partition.tries)
            .chain(legacy)
        {
            if detail.trie_state == Some(TrieState::Garbage as i32) {
                continue;
            }
            let trie: TrieKey = detail.trie_key.as_deref().unwrap_or_default().parse()?;
            closure.insert(key(&table.slug.data_key(&trie))?);
            closure.insert(key(&table.slug.metadata_key(&trie))?);
        }
    }
    Ok(closure)
}

/// Copies the closure of one already published checkpoint. A capture ID
/// fixes the checkpoint before any payload copy. An interrupted copy can
/// resume while its original inputs remain available.
pub async fn capture(
    directory: &dyn Directory,
    storage: &CellStorage,
    destination: &CellStorage,
    keys: &dyn KeyService,
    deployment: Uuid,
    operation: Uuid,
) -> Result<Capture> {
    if deployment.is_nil() || operation.is_nil() || same_storage(storage, destination) {
        return Err(inconsistent(
            "Capture requires one deployment, operation and independent archive",
        ));
    }
    let io = IoRegion::new();
    let archive = PrefixedStore::new(
        destination.bucket(io.clone()).await?,
        format!("checkpoint-captures/{deployment}/{operation}"),
    );
    let spec = key("intent.json")?;
    let receipt = key("capture.json")?;
    let record = directory
        .deployment(&deployment.to_string())
        .await?
        .ok_or_else(|| unavailable("The selected installation is unavailable"))?;

    let point = match read::<CapturePoint>(&archive, &spec).await? {
        Some(point) => point,
        None => {
            let checkpoint = record
                .checkpoint
                .ok_or_else(|| unavailable("The installation has not published a checkpoint"))?;
            let proposed = CapturePoint {
                operation,
                deployment,
                incarnation: record.incarnation,
                log_epoch: record.log_epoch,
                checkpoint,
            };
            if !same_lifetime(&record, &proposed) {
                return Err(unavailable("The installation cannot be captured"));
            }
            match archive.put(&spec, encode(&proposed)?).await {
                Ok(()) => proposed,
                Err(Error::ConflictingObject(_)) => {
                    read(&archive, &spec).await?.ok_or_else(|| {
                        unavailable("The retained capture intent could not be recovered")
                    })?
                },
                Err(error) => return Err(error.into()),
            }
        },
    };
    if point.deployment != deployment || point.operation != operation {
        return Err(inconsistent("The archive belongs to another capture"));
    }
    if let Some(completed) = read::<Capture>(&archive, &receipt).await? {
        if completed.point != point {
            return Err(inconsistent("The completed archive has another checkpoint"));
        }
        return Ok(completed);
    }
    if !same_lifetime(&record, &point) {
        return Err(unavailable(
            "The source is no longer available for the capture",
        ));
    }

    let recovery = ArchivedCheckpoint::load(&record, io.clone()).await?;
    let source = storage.bucket(io).await?;
    let version = StorageRoot::new(CURRENT_STORAGE_VERSION, StorageEpoch(0))?.key();
    let root = format!("{}/{version}", record.storage_prefix());
    let owner = source.get(&key(&format!("{root}/{ROOT_MARKER}"))?).await?;
    if owner.as_ref() != record.id.as_bytes() {
        return Err(inconsistent("The checkpoint belongs to another source"));
    }
    let material = keys
        .existing_key(&record.id)
        .await?
        .ok_or_else(|| unavailable("The source key is unavailable"))?;
    let sealed = open_deployment_store_with_archive(
        source,
        &root,
        PackDiscovery::From(point.checkpoint.block),
        wall_clock(),
        recovery,
    )
    .await?;
    let plaintext = EncryptingStore::new(sealed.clone() as Arc<dyn ObjectStore>, &material)?;

    let closure = checkpoint_objects(&plaintext, &point).await?;
    drop(plaintext);

    let mut bytes = 0u64;
    let mut objects = Vec::with_capacity(closure.len());
    for object in closure {
        let metadata = sealed.head(&object).await?;
        bytes = bytes
            .checked_add(metadata.size)
            .filter(|total| *total <= MAXIMUM_BYTES)
            .ok_or_else(|| unavailable("Checkpoint exceeds the archive byte budget"))?;
        let ciphertext = sealed.get(&object).await?;
        if ciphertext.len() as u64 != metadata.size {
            return Err(inconsistent("Checkpoint object changed during capture"));
        }
        let digest =
            URL_SAFE_NO_PAD.encode(ring::digest::digest(&ring::digest::SHA256, &ciphertext));
        archive
            .put(&key(&format!("objects/{}", object.as_str()))?, ciphertext)
            .await?;
        objects.push(ArchivedObject {
            name: object.to_string(),
            bytes: metadata.size,
            sha256: digest,
        });
    }
    let capture = Capture { point, objects };
    let encoded = encode(&capture)?;
    if encoded.len() as u64 > MAXIMUM_MANIFEST_BYTES {
        return Err(unavailable(
            "Checkpoint exceeds the archive metadata budget",
        ));
    }
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
) -> Result<CheckpointRecovery> {
    if deployment.is_nil() || operation.is_nil() || same_storage(storage, archive) {
        return Err(inconsistent(
            "Restore requires one deployment, operation and independent archive",
        ));
    }
    let id = deployment.to_string();
    let archive_binding = serde_json::to_string(archive)
        .map_err(|_| inconsistent("Cannot retain the archive binding"))?;
    let current = directory
        .deployment(&id)
        .await?
        .ok_or_else(|| unavailable("The selected installation is unavailable"))?;
    if let Some(recovery) = &current.checkpoint_recovery {
        if recovery.operation == operation && recovery.completed_at_ms.is_some() {
            if recovery.archive != archive_binding {
                return Err(inconsistent("The restore operation has another archive"));
            }
            return Ok(recovery.clone());
        }
    }
    let region = IoRegion::new();
    let archive_bucket = archive.bucket(region.clone()).await?;
    let prefix = format!("checkpoint-captures/{deployment}/{operation}");
    let metadata = PrefixedStore::new(archive_bucket.clone(), prefix.clone());
    let capture = read::<Capture>(&metadata, &key("capture.json")?)
        .await?
        .ok_or_else(|| unavailable("The checkpoint capture is incomplete"))?;
    let manifest_sha256 = digest(&encode(&capture)?);
    let point = capture.point;
    let intent = read::<CapturePoint>(&metadata, &key("intent.json")?).await?;
    if intent.as_ref() != Some(&point)
        || point.deployment != deployment
        || point.operation != operation
    {
        return Err(inconsistent("The archive belongs to another capture"));
    }
    if !same_incarnation(&current, &point) || current.checkpoint != Some(point.checkpoint) {
        return Err(inconsistent(
            "The archive is not the current durable checkpoint",
        ));
    }
    let material = keys
        .existing_key(&id)
        .await?
        .ok_or_else(|| unavailable("The current installation key is unavailable"))?;
    let files = Arc::new(PrefixedStore::new(
        archive_bucket,
        format!("{prefix}/objects"),
    ));
    let plaintext = EncryptingStore::new(files.clone(), &material)?;
    let closure = checkpoint_objects(&plaintext, &point).await?;
    let mut bytes = 0u64;
    let objects = capture
        .objects
        .iter()
        .map(|object| {
            bytes = bytes
                .checked_add(object.bytes)
                .filter(|total| *total <= MAXIMUM_BYTES)
                .ok_or_else(|| unavailable("Checkpoint exceeds the archive byte budget"))?;
            Ok((key(&object.name)?, object))
        })
        .collect::<Result<std::collections::BTreeMap<_, _>>>()?;
    if objects.len() != capture.objects.len() || objects.keys().ne(closure.iter()) {
        return Err(inconsistent(
            "The archive object list differs from the checkpoint closure",
        ));
    }
    let cipher = Cipher::new(&material)?;
    for (name, object) in objects {
        let ciphertext = read_object(files.as_ref(), &name, object).await?;
        cipher.open(name.as_str(), &ciphertext)?;
    }

    let fenced = update_installation(directory, &id, |record| {
        if !same_incarnation(record, &point)
            || record.checkpoint != Some(point.checkpoint)
            || record.move_from.is_some()
        {
            return Err(inconsistent("The installation changed before restore"));
        }
        if let Some(recovery) = &record.checkpoint_recovery
            && recovery.operation == operation
            && recovery.checkpoint == point.checkpoint
            && recovery.archive == archive_binding
            && recovery.manifest_sha256 == manifest_sha256
        {
            if recovery.completed_at_ms.is_some()
                || record.lifecycle == Lifecycle::Restoring && record.epoch == recovery.epoch
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
        record.checkpoint_recovery = Some(CheckpointRecovery {
            operation,
            checkpoint: point.checkpoint,
            epoch,
            previous_lifecycle: record.lifecycle,
            archive: archive_binding.clone(),
            manifest_sha256: manifest_sha256.clone(),
            completed_at_ms: None,
        });
        record.lifecycle = Lifecycle::Restoring;
        record.epoch = epoch;
        record.holder = None;
        record.move_intent = None;
        Ok(())
    })
    .await?;
    let pending = fenced
        .checkpoint_recovery
        .clone()
        .ok_or_else(|| inconsistent("Missing the accepted checkpoint restore"))?;
    if pending.completed_at_ms.is_some() {
        return Ok(pending);
    }
    let source = administration_stream(directory, &fenced, maximum_message_bytes).await?;
    rtdb_logkeeper::seal(&source, pending.epoch).await?;

    let completed = update_installation(directory, &id, |record| {
        let recovery = record
            .checkpoint_recovery
            .as_mut()
            .filter(|recovery| recovery.operation == operation && recovery.epoch == pending.epoch)
            .ok_or_else(|| inconsistent("Another checkpoint restore replaced this operation"))?;
        if recovery.completed_at_ms.is_none() {
            if record.lifecycle != Lifecycle::Restoring || record.epoch != pending.epoch {
                return Err(inconsistent("The restore lost its fence"));
            }
            recovery.completed_at_ms = Some(rtdb_directory::now_ms());
            record.lifecycle = recovery.previous_lifecycle;
        }
        Ok(())
    })
    .await?;
    completed
        .checkpoint_recovery
        .ok_or_else(|| inconsistent("The completed restore has no receipt"))
}
