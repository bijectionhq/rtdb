use std::{collections::BTreeMap, ops::Range, path::Path, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use rtdb_columnar::{MemoryBudget, OwnedBatch};
use rtdb_directory::{Directory, HostedTask};
use rtdb_node::NodeOptions;
use rtdb_query::{
    CatalogSnapshot, DatabaseSnapshot, QueryContext, open_owned_point, owned_row_revision,
};
use rtdb_replication::{BlockDiscovery, BlockStore, PublishedCheckpoint};
use rtdb_storage::{
    Error, ObjectKey, ObjectStore, StoredObject,
    cache::{BufferPool, CacheBudget},
};
use rtdb_types::{
    BlockIndex, EntityId, Interval, OwnedRelation, ReplicaPosition, Result, SourcePosition,
    StorageEpoch, TransactionKey,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ArchivedObject, CellStorage, Cipher, KeyService, PrefixedStore, archive_witness,
    capture_receipt, inconsistent, key, read, read_object, require_capture_open, stage,
    unavailable, verified_archive,
};

/// Reopenable locator for one independently retained published cut.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointReadWorld {
    pub deployment: Uuid,
    pub capture: Uuid,
    pub manifest_sha256: String,
    pub basis: TransactionKey,
}

pub struct CheckpointPoint<'a> {
    /// The authenticated owner supplied by the calling host.
    pub owner: &'a str,
    pub relation: &'a OwnedRelation,
    pub id: &'a EntityId,
    pub valid: Option<Interval>,
}

pub struct CheckpointReadResources<'a> {
    pub node: &'a NodeOptions,
    pub memory: &'a Arc<MemoryBudget>,
    pub cache: Arc<CacheBudget>,
    pub cancellation: &'a CancellationToken,
}

/// Read an owned point at the capture's exact transaction. The archive
/// admission remains held until the cursor and its storage have drained.
pub async fn read_checkpoint_owned_point(
    directory: &dyn Directory,
    archive: &CellStorage,
    keys: &dyn KeyService,
    hosted_task: Option<&HostedTask>,
    world: &CheckpointReadWorld,
    point: CheckpointPoint<'_>,
    resources: CheckpointReadResources<'_>,
) -> Result<Option<Vec<OwnedBatch>>> {
    if !resources.node.owned_relations {
        return Err(unavailable(
            "Owned checkpoint reads require the owned-relation capability",
        ));
    }
    let deployment = world.deployment.to_string();
    let admit = |record: &rtdb_directory::DeploymentRecord| {
        require_capture_open(record, world.capture)?;
        let (capture, manifest) =
            capture_receipt(record, archive, world.deployment, world.capture)?
                .ok_or_else(|| unavailable("The read world has no retained checkpoint receipt"))?;
        if manifest != world.manifest_sha256
            || capture.checkpoint.system_time != world.basis.system_time.0
            || capture.checkpoint.boundary < 0
            || capture.checkpoint.root_sha256.is_none()
        {
            return Err(inconsistent(
                "The read world does not name an exact published cut",
            ));
        }
        Ok(capture)
    };
    stage::with_region(
        directory,
        keys,
        &deployment,
        archive,
        hosted_task,
        Some(world.capture),
        None,
        |record| admit(record).map(|_| ()),
        |record, _, region| async {
            let capture = admit(&record)?;
            let bucket = archive.bucket(region).await?;
            if read(
                bucket.as_ref(),
                &archive_witness(world.deployment, world.capture)?,
            )
            .await?
            .as_ref()
                != Some(&capture)
            {
                return Err(inconsistent(
                    "The archive belongs to another captured checkpoint",
                ));
            }
            let (manifest, material) =
                verified_archive(bucket.clone(), &capture, &world.manifest_sha256, keys).await?;
            let store: Arc<dyn ObjectStore> = Arc::new(CapturedObjects {
                files: PrefixedStore::new(
                    bucket,
                    format!(
                        "checkpoint-captures/{}/{}/objects",
                        world.deployment, world.capture
                    ),
                ),
                objects: manifest
                    .objects
                    .into_iter()
                    .map(|object| Ok((key(&object.name)?, object)))
                    .collect::<Result<_>>()?,
                cipher: Cipher::new(&material)?,
            });
            let memory = resources.node.query.query_memory(resources.memory);
            let storage = Arc::new(BufferPool::with_metadata_budget(
                store.clone(),
                resources.cache,
                memory.clone(),
            ));
            let blocks = BlockStore::new(
                store,
                storage.clone(),
                memory.clone(),
                resources.node.file_size_target,
            )?;
            let epoch = StorageEpoch(capture.log_epoch);
            let checkpoint = PublishedCheckpoint {
                block: BlockIndex(capture.checkpoint.block),
                boundary: ReplicaPosition::from_epoch_offset(epoch, capture.checkpoint.boundary)?,
                root_sha256: capture.checkpoint.root_sha256,
            };
            let source_floor = SourcePosition::from_epoch_offset(epoch, capture.checkpoint.source)?;
            let recovered = blocks
                .recover(
                    resources.node.trie,
                    source_floor,
                    world.basis.system_time,
                    BlockDiscovery::AtPublished(checkpoint),
                )
                .await?;
            let snapshot = recovered.partition.snapshot();
            if snapshot.tx_basis() != Some(world.basis) {
                return Err(inconsistent(
                    "The retained checkpoint has a different transaction key",
                ));
            }
            let name = record.name;
            let catalog = CatalogSnapshot::new(
                &name,
                BTreeMap::from([(
                    name.clone(),
                    DatabaseSnapshot {
                        partitions: vec![snapshot],
                        storage,
                    },
                )]),
                true,
            )?;
            let context = QueryContext::new(
                catalog,
                memory,
                world.basis.system_time,
                "UTC",
                resources.node.query.clone(),
                resources.cancellation,
            )?;
            let Some(mut cursor) = open_owned_point(
                context,
                point.owner,
                point.relation,
                point.id,
                point.valid,
                None,
            )
            .await?
            else {
                return Ok(None);
            };
            let mut batches = Vec::new();
            while let Some(batch) = cursor.next_batch().await? {
                for row in 0..batch.batch().num_rows() {
                    owned_row_revision(batch.batch(), row, point.id, resources.memory)?;
                }
                batches.push(batch);
            }
            Ok(Some(batches))
        },
    )
    .await
}

/// The accepted inventory is the only source of bytes for a world read.
struct CapturedObjects {
    files: PrefixedStore,
    objects: BTreeMap<ObjectKey, ArchivedObject>,
    cipher: Cipher,
}

impl CapturedObjects {
    fn object(&self, name: &ObjectKey) -> rtdb_storage::Result<&ArchivedObject> {
        self.objects
            .get(name)
            .ok_or_else(|| Error::MissingObject(name.to_string()))
    }

    fn write_denied(name: &ObjectKey) -> Error {
        Error::PermissionDenied {
            key: name.to_string(),
            message: "Captured checkpoint is read-only".into(),
        }
    }
}

#[async_trait]
impl ObjectStore for CapturedObjects {
    fn supports_scan_ranges(&self, _: &ObjectKey) -> bool {
        false
    }

    async fn head(&self, name: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        let size = self
            .object(name)?
            .bytes
            .checked_sub(crate::cipher::SEALED_OVERHEAD as u64)
            .ok_or_else(|| Error::ConflictingObject(name.to_string()))?;
        Ok(StoredObject {
            key: name.clone(),
            size,
        })
    }

    async fn get(&self, name: &ObjectKey) -> rtdb_storage::Result<Bytes> {
        let sealed = read_object(&self.files, name, self.object(name)?).await?;
        self.cipher.open(name.as_str(), &sealed)
    }

    async fn get_range(&self, name: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
        let length = self.head(name).await?.size;
        if range.start > range.end || range.end > length {
            return Err(Error::InvalidRange {
                key: name.to_string(),
                start: range.start,
                end: range.end,
                length,
            });
        }
        Ok(self
            .get(name)
            .await?
            .slice(range.start as usize..range.end as usize))
    }

    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
        let mut objects = Vec::new();
        let prefix = directory.map(|directory| format!("{}/", directory.as_str()));
        for name in self.objects.keys().filter(|name| {
            prefix
                .as_ref()
                .is_none_or(|prefix| name.as_str().starts_with(prefix))
        }) {
            objects.push(self.head(name).await?);
        }
        Ok(objects)
    }

    async fn put(&self, name: &ObjectKey, _: Bytes) -> rtdb_storage::Result<()> {
        Err(Self::write_denied(name))
    }

    async fn put_file(&self, name: &ObjectKey, _: &Path) -> rtdb_storage::Result<()> {
        Err(Self::write_denied(name))
    }

    async fn delete(&self, name: &ObjectKey) -> rtdb_storage::Result<()> {
        Err(Self::write_denied(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn checkpoint_world_store_reads_only_authenticated_inventory_members() -> Result<()> {
        let archive: Arc<dyn ObjectStore> = Arc::new(rtdb_storage::MemoryStore::default());
        let name = key("member")?;
        let other = key("outside-manifest")?;
        let material = [9; crate::keys::KEY_BYTES];
        let bytes = Bytes::from_static(b"checkpoint object");
        let files = PrefixedStore::new(archive.clone(), "world/objects");
        let cipher = Cipher::new(&material)?;
        let sealed = cipher.seal_deterministic(name.as_str(), &bytes)?;
        files.put(&name, sealed.clone()).await?;
        files.put(&other, sealed.clone()).await?;
        let object = ArchivedObject {
            name: name.to_string(),
            bytes: sealed.len() as u64,
            sha256: super::super::digest(&sealed),
        };
        let store = CapturedObjects {
            files,
            objects: BTreeMap::from([(name.clone(), object)]),
            cipher,
        };
        assert_eq!(store.get(&name).await?, bytes);
        assert!(matches!(
            store.get(&other).await,
            Err(Error::MissingObject(_))
        ));
        assert!(matches!(
            store.delete(&name).await,
            Err(Error::PermissionDenied { .. })
        ));
        let reopened = CapturedObjects {
            files: PrefixedStore::new(archive, "world/objects"),
            objects: store.objects,
            cipher: Cipher::new(&material)?,
        };
        assert_eq!(reopened.get(&name).await?, bytes);
        Ok(())
    }
}
