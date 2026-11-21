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
    admin::{administration_stream, await_storage_drain, update_installation},
    cipher::Cipher,
    deployment::{CellStorage, S3_CELL_ROOT},
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
pub use reader::{StagedReaderSettings, serve_staged_checkpoint};
pub(crate) use reader::{StageSelector, tunnel_staged_checkpoint};

const MAXIMUM_BYTES: u64 = 1 << 30;
const MAXIMUM_MANIFEST_BYTES: u64 = 16 << 20;

pub(crate) fn checkpoint_storage_root() -> Result<String> {
    Ok(StorageRoot::new(CURRENT_STORAGE_VERSION, StorageEpoch(0))?.key())
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
    if ciphertext.len() as u64 != object.bytes || digest(&ciphertext) != object.sha256 {
        return Err(Error::ConflictingObject(name.to_string()));
    }
    Ok(ciphertext)
}

/// The accepted manifest supplies the immutable objects covered by a repair.
/// Other reads continue through the current serving store.
pub(crate) struct ArchivedCheckpoint {
    files: PrefixedStore,
    objects: BTreeMap<ObjectKey, ArchivedObject>,
    archive_cipher: Cipher,
    serving_cipher: Cipher,
}

impl ArchivedCheckpoint {
    pub(crate) async fn load(
        record: &DeploymentRecord,
        keys: &dyn KeyService,
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
        let deployment = record.id.parse::<Uuid>()
            .map_err(|_| inconsistent("Invalid retained archive owner"))?;
        let expected = capture_point(record, deployment, recovery.operation, &recovery.archive)?;
        let bucket = archive.bucket(region).await?;
        let prefix = format!("checkpoint-captures/{}/{}", record.id, recovery.operation);
        let scope = PrefixedStore::new(bucket.clone(), prefix.clone());
        let capture = read::<Capture>(&scope, &key("capture.json")?)
            .await?
            .ok_or_else(|| unavailable("The recovery archive is unavailable"))?;
        if capture.point != expected || !same_incarnation(record, &capture.point)
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
            archive_cipher: Cipher::new(&archive_key(keys, &capture.point).await?)?,
            serving_cipher: Cipher::new(&keys.existing_key(&record.key_id()).await?
                .ok_or_else(|| unavailable("The current installation key is unavailable"))?)?,
        }))
    }

    async fn read_plaintext(&self, name: &ObjectKey, object: &ArchivedObject) -> rtdb_storage::Result<Bytes> {
        let ciphertext = read_object(&self.files, name, object).await?;
        self.archive_cipher.open(name.as_str(), &ciphertext)
    }

    async fn seal_for_serving(&self, name: &ObjectKey, object: &ArchivedObject) -> rtdb_storage::Result<Bytes> {
        let plaintext = self.read_plaintext(name, object).await?;
        self.serving_cipher.seal_deterministic(name.as_str(), &plaintext)
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
        for (name, object) in &self.objects {
            let plaintext = self.read_plaintext(name, object).await?;
            match serving.get(name).await {
                Ok(bytes) if self.serving_cipher.open(name.as_str(), &bytes)? == plaintext => continue,
                Ok(_) => return Err(Error::ConflictingObject(name.to_string()).into()),
                Err(Error::MissingObject(_)) => {},
                Err(error) => return Err(error.into()),
            }
            let ciphertext = self.serving_cipher.seal_deterministic(name.as_str(), &plaintext)?;
            physical.put(name, ciphertext).await?;
        }
        Ok(())
    }
}

struct CheckpointStore {
    serving: Arc<dyn ObjectStore>,
    archive: ArchivedCheckpoint,
}

#[async_trait::async_trait]
impl ObjectStore for CheckpointStore {
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        match self.archive.objects.get(key) {
            Some(object) => Ok(StoredObject { key: key.clone(), size: object.bytes }),
            None => self.serving.head(key).await,
        }
    }

    async fn head_fresh(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        let Some(object) = self.archive.objects.get(key) else {
            return self.serving.head_fresh(key).await;
        };
        let metadata = self.archive.files.head_fresh(key).await?;
        if metadata.size != object.bytes {
            return Err(Error::ConflictingObject(key.to_string()));
        }
        Ok(metadata)
    }

    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> {
        match self.archive.objects.get(key) {
            Some(object) => self.archive.seal_for_serving(key, object).await,
            None => self.serving.get(key).await,
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
        let closure = checkpoint_objects(&plaintext, &self.point).await?;
        let mut bytes = 0u64;
        let objects = self.objects.iter()
            .map(|object| {
                bytes = bytes.checked_add(object.bytes)
                    .filter(|total| *total <= MAXIMUM_BYTES)
                    .ok_or_else(|| unavailable("Checkpoint exceeds the archive byte budget"))?;
                Ok((key(&object.name)?, object))
            })
            .collect::<Result<std::collections::BTreeMap<_, _>>>()?;
        if objects.len() != self.objects.len() || objects.keys().ne(closure.iter()) {
            return Err(inconsistent(
                "The archive object list differs from the checkpoint closure",
            ));
        }
        let cipher = Cipher::new(material)?;
        for (name, object) in objects {
            let ciphertext = read_object(files.as_ref(), &name, object).await?;
            cipher.open(name.as_str(), &ciphertext)?;
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
    directory: Arc<dyn Directory>,
    storage: &CellStorage,
    destination: &CellStorage,
    keys: &dyn KeyService,
    deployment: Uuid,
    operation: Uuid,
    hosted_task: Option<rtdb_directory::HostedTask>,
) -> Result<Capture> {
    if deployment.is_nil() || operation.is_nil() || same_storage(storage, destination) {
        return Err(inconsistent(
            "Capture requires one deployment, operation and independent archive",
        ));
    }
    let binding = archive_binding(destination)?;
    let id = deployment.to_string();
    let region = Uuid::new_v4();
    let local = matches!((storage, destination), (CellStorage::Local { .. }, CellStorage::Local { .. }));
    let witness = if local { keys.hold_local_storage_region(region)? } else { None };
    let witnessed = witness.is_some();
    let mut io = IoRegion::new().child_holding(witness.map(|file| Arc::new(file) as Arc<dyn Send + Sync>));
    if matches!(destination, CellStorage::S3 { .. }) {
        crate::store::track_publications(&mut io, directory.clone(), id.clone(), region, None);
    }
    let admitted = update_installation(directory.as_ref(), &id, |record| {
        if !matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
            || record.move_from.is_some() || record.archive_retirement.is_some()
        {
            return Err(unavailable("The installation cannot be captured"));
        }
        if let Some(registered) = record.checkpoint_captures.get(&operation) {
            if registered.archive != binding || registered.incarnation != record.incarnation
                || registered.log_epoch != record.log_epoch
            {
                return Err(inconsistent("The capture operation already names another archive or lifetime"));
            }
        } else {
            if record.checkpoint_repair_receipts.contains_key(&operation) {
                return Err(inconsistent("The capture operation has a repair in another residence"));
            }
            let checkpoint = record.checkpoint
                .ok_or_else(|| unavailable("The installation has not published a checkpoint"))?;
            record.checkpoint_captures.insert(operation, CheckpointCapture {
                archive: binding.clone(),
                incarnation: record.incarnation,
                log_epoch: record.log_epoch,
                checkpoint,
                media_bound: false,
                manifest_sha256: None,
                remote_disposal: None,
            });
        }
        record.storage_regions.insert(region, if local {
            rtdb_directory::StorageRegion::Local
        } else {
            crate::store::remote_storage_region(Some(operation), hosted_task.clone())
        });
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
            directory.as_ref(), storage, destination, keys, &record, point, io.clone(),
        ).await?;
        let manifest = capture.manifest_sha256()?;
        update_installation(directory.as_ref(), &id, |current| {
            if !same_incarnation(current, &capture.point)
                || !current.storage_regions.contains_key(&region)
                || current.archive_retirement.is_some()
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

async fn capture_registered(
    directory: &dyn Directory,
    storage: &CellStorage,
    destination: &CellStorage,
    keys: &dyn KeyService,
    record: &DeploymentRecord,
    point: CapturePoint,
    io: Arc<IoRegion>,
) -> Result<Capture> {
    let CapturePoint { deployment, operation, .. } = point;
    let archive_bucket = destination.bucket(io.clone()).await?;
    archive_bucket.put(&archive_witness(deployment, operation)?, encode(&point)?).await?;
    update_installation(directory, &record.id, |current| {
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
    if !same_lifetime(record, &point) {
        return Err(unavailable(
            "The source is no longer available for the capture",
        ));
    }

    let serving_material = keys.existing_key(&record.key_id()).await?
        .ok_or_else(|| unavailable("The source key is unavailable"))?;
    let archive_material = keys.key(&archive_key_id(&point)).await?;
    let archive_cipher = Cipher::new(&archive_material)?;
    let recovery = ArchivedCheckpoint::load(record, keys, io.clone()).await?;
    let source = storage.bucket(io).await?;
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
    let closure = checkpoint_objects(&plaintext, &point).await?;

    let mut bytes = 0u64;
    let mut objects = Vec::with_capacity(closure.len());
    for object in closure {
        let metadata = plaintext.head(&object).await?;
        let ciphertext_size = metadata.size
            .checked_add(crate::cipher::SEALED_OVERHEAD as u64)
            .ok_or_else(|| unavailable("Checkpoint object exceeds the archive byte budget"))?;
        bytes = bytes
            .checked_add(ciphertext_size)
            .filter(|total| *total <= MAXIMUM_BYTES)
            .ok_or_else(|| unavailable("Checkpoint exceeds the archive byte budget"))?;
        let content = plaintext.get(&object).await?;
        if content.len() as u64 != metadata.size {
            return Err(inconsistent("Checkpoint object changed during capture"));
        }
        let ciphertext = archive_cipher.seal_deterministic(object.as_str(), &content)?;
        let digest = digest(&ciphertext);
        archive
            .put(&key(&format!("objects/{}", object.as_str()))?, ciphertext)
            .await?;
        objects.push(ArchivedObject {
            name: object.to_string(),
            bytes: ciphertext_size,
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
) -> Result<CheckpointRecovery> {
    restore_checkpoint(directory, storage, archive, keys, deployment, operation, maximum_message_bytes, None).await
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
        let Some(recovery) = record.checkpoint_repair_receipts.get(&operation) else {
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
    let region = IoRegion::new();
    let archive_bucket = archive.bucket(region.clone()).await?;
    let prefix = format!("checkpoint-captures/{deployment}/{operation}");
    let metadata = PrefixedStore::new(archive_bucket.clone(), prefix.clone());
    let capture = read::<Capture>(&metadata, &key("capture.json")?)
        .await?
        .ok_or_else(|| unavailable("The checkpoint capture is incomplete"))?;
    let intent = read::<CapturePoint>(&metadata, &key("intent.json")?).await?;
    if intent.as_ref() != Some(&point) || capture.point != point
        || capture.manifest_sha256()? != manifest_sha256
    {
        return Err(inconsistent("The archive differs from its retained manifest receipt"));
    }
    keys.existing_key(&current.key_id()).await?
        .ok_or_else(|| unavailable("The current installation key is unavailable"))?;
    capture.verify(archive_bucket, &prefix, &archive_key(keys, &point).await?).await?;

    let fenced = update_installation(directory, &id, |record| {
        if read_receipt(record)?.is_some() {
            return Ok(());
        }
        if !same_incarnation(record, &point)
            || record.checkpoint != Some(point.checkpoint)
            || record.move_from.is_some()
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
    if let Some(receipt) = read_receipt(&fenced)? {
        return Ok(receipt);
    }
    let pending = fenced
        .checkpoint_recovery
        .as_ref()
        .filter(|recovery| {
            recovery.operation == operation && recovery.archive == archive_binding
                && recovery.manifest_sha256 == manifest_sha256 && recovery.epoch == fenced.epoch
                && recovery.completed_at_ms.is_none() && fenced.lifecycle == Lifecycle::Restoring
        })
        .ok_or_else(|| inconsistent("Missing the accepted checkpoint restore"))?;
    await_storage_drain(directory, storage, keys, &fenced).await?;
    let source = administration_stream(directory, &fenced, maximum_message_bytes).await?;
    rtdb_logkeeper::seal(&source, pending.epoch).await?;

    let completed = update_installation(directory, &id, |record| {
        if read_receipt(record)?.is_some() {
            return Ok(());
        }
        if record.lifecycle != Lifecycle::Restoring || record.epoch != pending.epoch {
            return Err(inconsistent("The restore lost its fence"));
        }
        let recovery = record
            .checkpoint_recovery
            .as_mut()
            .filter(|recovery| &**recovery == pending)
            .ok_or_else(|| inconsistent("Another checkpoint restore replaced this operation"))?;
        recovery.completed_at_ms = Some(rtdb_directory::now_ms());
        record.lifecycle = recovery.previous_lifecycle;
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
    let root = config.object_prefix(&key(S3_CELL_ROOT)?)?.to_string();
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
        if !registration.media_bound {
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
        if registration.media_bound {
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
            objects: [(covered.clone(), ArchivedObject {
                name: covered.to_string(), bytes: ciphertext.len() as u64, sha256: digest(&ciphertext),
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
        record.disposal = Some(DisposalReceipt {
            deployment: record.id.clone(),
            incarnation: record.incarnation,
            epoch: record.epoch,
            journals: BTreeSet::new(),
            journals_retired_at_ms: 1,
            key_withdrawn_at_ms: 1,
            serving_prefix_observed_empty_at_ms: 1,
            serving_inventory: rtdb_directory::ServingInventory::S3VersionsAndUploads,
        });
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
                },
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
