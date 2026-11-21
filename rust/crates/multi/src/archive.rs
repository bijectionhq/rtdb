//! Immutable, ciphertext-only captures of published checkpoints.
use std::{
    collections::BTreeSet,
    sync::Arc,
};

use base64::{
    engine::general_purpose::URL_SAFE_NO_PAD,
    Engine,
};
use bytes::Bytes;
use rtdb_directory::{
    Checkpoint,
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
    deployment::CellStorage,
    keys::KeyService,
    packing::PackDiscovery,
    reference::{
        key,
        PrefixedStore,
    },
    store::{
        open_deployment_store,
        wall_clock,
        EncryptingStore,
        ROOT_MARKER,
    },
};

const MAXIMUM_BYTES: u64 = 1 << 30;
const MAXIMUM_MANIFEST_BYTES: u64 = 16 << 20;

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

fn same_lifetime(record: &DeploymentRecord, point: &CapturePoint) -> bool {
    record.id == point.deployment.to_string()
        && record.incarnation == point.incarnation
        && record.log_epoch == point.log_epoch
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
    let sealed = open_deployment_store(
        source,
        &root,
        PackDiscovery::From(point.checkpoint.block),
        wall_clock(),
    )
    .await?;
    let plaintext = EncryptingStore::new(sealed.clone() as Arc<dyn ObjectStore>, &material)?;

    let block = BlockIndex(point.checkpoint.block);
    let catalog_key = key(&block_key(block)?)?;
    let catalog = BlockCatalog::decode(&plaintext.get(&catalog_key).await?)?;
    verify_cut(&catalog, &point)?;
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
