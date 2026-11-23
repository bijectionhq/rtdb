use super::prefix_retirement::{DESCRIPTOR, GrantDescriptor, local_lifetime};
use super::{Routes, genesis, invalid, material};
use crate::deployment::CellStorage;
use crate::journal::{
    InstalledJournal,
    custody::{CheckpointCustody, Preparation},
};
use crate::store::{EncryptingStore, ScopedCellWrites};
use async_trait::async_trait;
use bytes::Bytes;
use ordered_logkeeper::ordered::{Grant, Placement};
use prost_types::Any;
use rtdb_node::{OpenedRecoveryStore, RemoteStorageFactory};
use rtdb_replication::{BlockDiscovery, PublishedCheckpoint};
use rtdb_storage::{
    Error, IoRegion, ObjectKey, ObjectObservation, ObjectStore,
    log::ReplicaObjectScope,
    object_store::{ObjectReadAllocation, ReadReservation, RetainedBytes, StoredObject},
};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, ReplicaPosition, Result};
use std::collections::BTreeMap;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

pub(crate) struct StoreFactory {
    pub routes: Arc<Routes>,
    pub storage: CellStorage,
    pub keys: Arc<dyn crate::keys::KeyService>,
    pub scoped_writes: Option<Arc<ScopedCellWrites>>,
    pub releases: tokio_util::task::TaskTracker,
}

struct WriteScope {
    journal: Arc<InstalledJournal>,
    grant: Grant,
}

#[async_trait]
impl rtdb_s3::WriteAuthorization for WriteScope {
    async fn authorize(&self, _: Option<SystemTime>) -> Result<()> {
        let route = self.journal.route().await?;
        if route.writer.grant != Some(self.grant) {
            return Err(Anomaly::new(
                AnomalyCategory::Unavailable,
                rtdb_node::NOT_HELD,
                "The journal fenced this storage writer",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl RemoteStorageFactory for StoreFactory {
    fn use_disk_cache(&self) -> bool {
        false
    }

    async fn open(
        &self,
        configuration: &Any,
        root: &ObjectKey,
        io: Arc<IoRegion>,
    ) -> Result<Arc<dyn ObjectStore>> {
        Ok(self.open_for_recovery(configuration, root, io).await?.store)
    }

    async fn open_for_recovery(
        &self,
        configuration: &Any,
        root: &ObjectKey,
        io: Arc<IoRegion>,
    ) -> Result<OpenedRecoveryStore> {
        use rtdb_s3::WriteAuthorization;
        let (placement, attachment) = self.routes.resolve(configuration)?;
        let grant = attachment.admission.grant;
        let hold = attachment.admission.recovery;
        let scope = Arc::new(WriteScope {
            journal: Arc::clone(&placement.worker),
            grant,
        });
        scope.authorize(None).await?;
        placement.worker.replay_head(hold, hold.start).await?;
        let protection = placement.pin_root(hold, &self.releases).await?;
        let material = material(self.keys.as_ref(), &placement.record).await?;
        let (lower, blocks) = if genesis(hold.checkpoint) && attachment.base.checkpoint.is_none() {
            (None, BlockDiscovery::List)
        } else {
            let custody = placement
                .checkpoint_custody(Arc::clone(&io), &material)
                .await?;
            let opened = custody.open(hold.checkpoint).await?;
            let checkpoint = opened.checkpoint;
            if attachment.base.checkpoint != Some(checkpoint) {
                return Err(invalid(
                    "The database attachment names a different protected checkpoint",
                ));
            }
            let published = PublishedCheckpoint {
                block: BlockIndex(checkpoint.block),
                boundary: ReplicaPosition::from_epoch_offset(
                    placement.worker.log_epoch(),
                    checkpoint.boundary,
                )?,
                root_sha256: checkpoint.root_sha256,
            };
            (Some(opened.store), BlockDiscovery::AtPublished(published))
        };
        let storage_prefix = placement.record.storage_prefix();
        let installed = placement.worker.placement();
        let io = local_lifetime(&self.storage, &grant_prefix(&storage_prefix, installed.generation, grant.epoch), &io)?;
        let bucket = self.storage.bucket(Arc::clone(&io)).await?;
        let prefix = source_prefix(&storage_prefix, installed.generation, grant.epoch, root);
        let writer = match (&self.storage, &self.scoped_writes) {
            (CellStorage::S3 { config }, Some(scoped)) => {
                let authorization: Arc<dyn rtdb_s3::WriteAuthorization> = scope.clone();
                Some(
                    scoped
                        .ordered_writer(config, &prefix, authorization, Arc::clone(&io))
                        .await?,
                )
            }
            (CellStorage::S3 { .. }, None) => {
                return Err(invalid("An ordered S3 writer requires a scoped write role"));
            }
            (CellStorage::Local { .. }, _) => None,
        };
        let upper = Arc::new(
            crate::reference::PrefixedStore::new(Arc::clone(&bucket), prefix).with_writer(writer),
        );
        crate::store::check_root(upper.as_ref(), &placement.record.id, true).await?;
        let descriptor = serde_json::to_vec(&GrantDescriptor::new(&placement, grant))
            .map_err(|error| invalid(&error.to_string()))?;
        upper.put(&ObjectKey::new(DESCRIPTOR)?, Bytes::from(descriptor)).await?;
        let upper: Arc<dyn ObjectStore> = Arc::new(EncryptingStore::new(upper, &material)?);
        let store = Arc::new(ServingStore {
            lower,
            upper,
            scope: Some(scope),
            replica_objects: Some(ReplicaObjects {
                installed,
                storage_prefix,
                root: root.clone(),
                bucket,
                storage: self.storage.clone(),
                io,
                material,
            }),
            _protection: Some(protection),
        });
        Ok(OpenedRecoveryStore {
            store,
            blocks,
            audience: None,
            checkpoint_metadata: None,
            fragment_storage: None,
        })
    }
}

pub(super) fn grant_prefix(storage_prefix: &str, generation: u64, writer_epoch: u64) -> String {
    format!("{storage_prefix}/~ordered/{generation}/{writer_epoch}")
}

fn source_prefix(
    storage_prefix: &str,
    generation: u64,
    writer_epoch: u64,
    root: &ObjectKey,
) -> String {
    format!("{}/{root}", grant_prefix(storage_prefix, generation, writer_epoch))
}

pub(super) struct ReplicaObjects {
    installed: Placement,
    storage_prefix: String,
    root: ObjectKey,
    bucket: Arc<dyn ObjectStore>,
    storage: CellStorage,
    io: Arc<IoRegion>,
    material: Vec<u8>,
}

impl ReplicaObjects {
    pub(super) async fn for_custodian(
        placement: &super::Placement,
        storage: &CellStorage,
        material: &[u8],
        io: Arc<IoRegion>,
    ) -> Result<Self> {
        Ok(Self {
            installed: placement.worker.placement(),
            storage_prefix: placement.record.storage_prefix(),
            root: ObjectKey::new(crate::archive::checkpoint_storage_root()?)?,
            bucket: storage.bucket(Arc::clone(&io)).await?,
            storage: storage.clone(),
            io,
            material: material.to_vec(),
        })
    }

    async fn open(&self, scope: ReplicaObjectScope) -> rtdb_storage::Result<Arc<dyn ObjectStore>> {
        if scope.deployment != self.installed.deployment
            || scope.incarnation != self.installed.incarnation
            || scope.group != self.installed.group
            || scope.generation != self.installed.generation
            || scope.writer_epoch == 0
        {
            return Err(Error::Task(
                "The replica object scope belongs to a different installed placement".into(),
            ));
        }
        self.open_writer(scope.writer_epoch).await
    }

    pub(super) async fn open_writer(
        &self,
        epoch: u64,
    ) -> rtdb_storage::Result<Arc<dyn ObjectStore>> {
        let prefix = source_prefix(
            &self.storage_prefix,
            self.installed.generation,
            epoch,
            &self.root,
        );
        let bucket = match &self.storage {
            CellStorage::Local { .. } => {
                let scope = grant_prefix(&self.storage_prefix, self.installed.generation, epoch);
                let lifetime = local_lifetime(&self.storage, &scope, &self.io)
                    .map_err(|error| Error::Task(error.to_string()))?;
                self.storage.bucket(lifetime).await.map_err(|error| Error::Task(error.to_string()))?
            }
            CellStorage::S3 { .. } => Arc::clone(&self.bucket),
        };
        let bucket = Arc::new(crate::reference::PrefixedStore::new(bucket, prefix));
        let owner = self.installed.deployment.to_string();
        let marker = ObjectKey::new(crate::store::ROOT_MARKER)?;
        if bucket.get_fresh_exact(&marker, owner.len()).await?.as_ref() != owner.as_bytes() {
            return Err(Error::Task(
                "The replica object namespace has a different owner".into(),
            ));
        }
        let reader = EncryptingStore::new(bucket, &self.material)
            .map_err(|error| Error::Task(error.to_string()))?;
        Ok(Arc::new(reader))
    }
}

pub(super) async fn reopen_source(
    placement: &super::Placement,
    storage: &CellStorage,
    custody: &CheckpointCustody,
    key: &[u8],
    io: &Arc<IoRegion>,
    origin: Preparation,
) -> Result<Arc<dyn ObjectStore>> {
    let root = ObjectKey::new(crate::archive::checkpoint_storage_root()?)?;
    let storage_prefix = placement.record.storage_prefix();
    let generation = placement.worker.placement().generation;
    let prefix = source_prefix(&storage_prefix, generation, origin.writer.epoch, &root);
    let io = local_lifetime(storage, &grant_prefix(&storage_prefix, generation, origin.writer.epoch), io)?;
    let bucket = storage.bucket(io).await?;
    let upper = Arc::new(crate::reference::PrefixedStore::new(bucket, prefix));
    crate::store::check_root(upper.as_ref(), &placement.record.id, false).await?;
    let upper = Arc::new(EncryptingStore::new(upper, key)?);
    let lower = if genesis(origin.base) {
        None
    } else {
        Some(custody.open(origin.base).await?.store)
    };
    Ok(Arc::new(ServingStore {
        lower,
        upper,
        scope: None,
        replica_objects: None,
        _protection: None,
    }))
}

/// The writer publishes only below its own scoped epoch prefix. The
/// inherited checkpoint remains an independently retained immutable base.
struct ServingStore {
    lower: Option<Arc<dyn ObjectStore>>,
    upper: Arc<dyn ObjectStore>,
    scope: Option<Arc<WriteScope>>,
    replica_objects: Option<ReplicaObjects>,
    _protection: Option<Arc<crate::journal::Held>>,
}

impl ServingStore {
    async fn inherited(&self, key: &ObjectKey) -> rtdb_storage::Result<Option<&dyn ObjectStore>> {
        let Some(lower) = &self.lower else {
            return Ok(None);
        };
        match lower.head(key).await {
            Ok(_) => Ok(Some(lower.as_ref())),
            Err(Error::MissingObject(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn reading(&self, key: &ObjectKey) -> rtdb_storage::Result<&dyn ObjectStore> {
        Ok(self.inherited(key).await?.unwrap_or(self.upper.as_ref()))
    }

    async fn authorize(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
        use rtdb_s3::WriteAuthorization;
        self.scope
            .as_ref()
            .ok_or_else(|| Error::Remote {
                key: key.to_string(),
                message: "A checkpoint preparation source is read-only".into(),
            })?
            .authorize(None)
            .await
            .map_err(|error| Error::Remote {
                key: key.to_string(),
                message: error.to_string(),
            })
    }
}

#[async_trait]
impl ObjectStore for ServingStore {
    async fn open_replica_objects(
        &self,
        scope: ReplicaObjectScope,
    ) -> rtdb_storage::Result<Arc<dyn ObjectStore>> {
        let owner = self
            .replica_objects
            .as_ref()
            .ok_or_else(|| Error::Task("The store has no installed replica object scope".into()))?;
        owner.open(scope).await
    }

    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        self.reading(key).await?.head_fresh(key).await
    }

    async fn observe_head(
        &self,
        key: &ObjectKey,
    ) -> rtdb_storage::Result<ObjectObservation<StoredObject>> {
        self.reading(key).await?.observe_head(key).await
    }

    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> {
        self.reading(key).await?.get_fresh(key).await
    }

    async fn observe_fresh_exact(
        &self,
        key: &ObjectKey,
        length: usize,
    ) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
        self.reading(key)
            .await?
            .observe_fresh_exact(key, length)
            .await
    }

    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
        self.reading(key).await?.get_range(key, range).await
    }

    async fn observe_range(
        &self,
        key: &ObjectKey,
        range: Range<u64>,
    ) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
        self.reading(key).await?.observe_range(key, range).await
    }

    fn supports_scan_ranges(&self, key: &ObjectKey) -> bool {
        self.lower
            .as_ref()
            .is_none_or(|lower| lower.supports_scan_ranges(key))
            && self.upper.supports_scan_ranges(key)
    }

    fn whole_object_allocation(
        &self,
        key: &ObjectKey,
        length: usize,
    ) -> rtdb_storage::Result<ObjectReadAllocation> {
        self.upper.whole_object_allocation(key, length)
    }

    fn scan_range_allocation(
        &self,
        key: &ObjectKey,
        length: usize,
    ) -> rtdb_storage::Result<ObjectReadAllocation> {
        self.upper.scan_range_allocation(key, length)
    }

    async fn get_reserved_object(
        &self,
        key: &ObjectKey,
        reservation: ReadReservation,
    ) -> rtdb_storage::Result<RetainedBytes> {
        self.reading(key)
            .await?
            .get_reserved_object(key, reservation)
            .await
    }

    async fn get_reserved_scan_range(
        &self,
        key: &ObjectKey,
        range: Range<u64>,
        reservation: ReadReservation,
    ) -> rtdb_storage::Result<RetainedBytes> {
        self.reading(key)
            .await?
            .get_reserved_scan_range(key, range, reservation)
            .await
    }

    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> {
        self.authorize(key).await?;
        if let Some(lower) = self.inherited(key).await? {
            return if lower.get_fresh_exact(key, bytes.len()).await? == bytes {
                Ok(())
            } else {
                Err(Error::ConflictingObject(key.to_string()))
            };
        }
        self.upper.put(key, bytes).await
    }

    async fn put_file(&self, key: &ObjectKey, path: &Path) -> rtdb_storage::Result<()> {
        self.authorize(key).await?;
        if self.inherited(key).await?.is_some() {
            return Err(Error::ConflictingObject(key.to_string()));
        }
        self.upper.put_file(key, path).await
    }

    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
        // The writer's published objects can still be the source of an
        // ordered checkpoint or a read whose grant has been replaced.
        // Retention of this immutable source is owned by its installation.
        self.authorize(key).await
    }

    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
        let mut objects: BTreeMap<_, _> = self
            .upper
            .list(directory)
            .await?
            .into_iter()
            .map(|object| (object.key.clone(), object))
            .collect();
        if let Some(lower) = &self.lower {
            objects.extend(
                lower
                    .list(directory)
                    .await?
                    .into_iter()
                    .map(|object| (object.key.clone(), object)),
            );
        }
        Ok(objects.into_values().collect())
    }
}
