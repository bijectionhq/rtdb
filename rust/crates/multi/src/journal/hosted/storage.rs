use super::prefix_retirement::{DESCRIPTOR, GrantDescriptor, local_lifetime};
use super::reclamation::{ReclamationQueue, queue_prefix};
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
use std::time::{Duration, Instant, SystemTime};

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
    lease: ServingLease,
}

/// The worker serves bytes only within a journal-issued lease. Once a withdrawn
/// worker's lease and the clock drift bound have passed, the journal may dispose
/// of its holds, so every read checks the lease on the worker's monotonic clock.
#[derive(Default)]
struct ServingLease {
    /// The lease deadline and the instant after which a read renews it.
    state: std::sync::Mutex<Option<(Instant, Instant)>>,
    renewal: tokio::sync::Mutex<()>,
}

impl WriteScope {
    async fn serving(&self) -> Result<()> {
        if let Some((_, renew_after)) = self.lease_state() && Instant::now() < renew_after {
            return Ok(());
        }
        let _renewing = self.lease.renewal.lock().await;
        if let Some((_, renew_after)) = self.lease_state() && Instant::now() < renew_after {
            return Ok(());
        }
        let sent = Instant::now();
        match self.journal.lease(self.grant).await {
            Ok(length) => {
                let length = Duration::from_millis(length);
                let state = (sent + length, sent + length / 2);
                *self.lease.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(state);
                Ok(())
            }
            Err(_) if self.lease_state().is_some_and(|(deadline, _)| Instant::now() < deadline) => Ok(()),
            Err(error) => Err(Anomaly::new(
                AnomalyCategory::Unavailable,
                rtdb_node::NOT_HELD,
                format!("The installed writer has no current serving lease: {error}"),
            )),
        }
    }

    fn lease_state(&self) -> Option<(Instant, Instant)> {
        *self.lease.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
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
            lease: ServingLease::default(),
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
        let grant_prefix = grant_prefix(&storage_prefix, installed.generation, grant.epoch);
        let io = local_lifetime(&self.storage, &grant_prefix, &io)?;
        let bucket = self.storage.bucket(Arc::clone(&io)).await?;
        let prefix = source_prefix(&storage_prefix, installed.generation, grant.epoch, root);
        let queue_prefix = queue_prefix(&grant_prefix);
        let (writer, queue_writer) = match (&self.storage, &self.scoped_writes) {
            (CellStorage::S3 { config }, Some(scoped)) => {
                let authorization: Arc<dyn rtdb_s3::WriteAuthorization> = scope.clone();
                (
                    Some(scoped.ordered_writer(config, &prefix, Arc::clone(&authorization), Arc::clone(&io)).await?),
                    Some(scoped.ordered_writer(config, &queue_prefix, authorization, Arc::clone(&io)).await?),
                )
            }
            (CellStorage::S3 { .. }, None) => {
                return Err(invalid("An ordered S3 writer requires a scoped write role"));
            }
            (CellStorage::Local { .. }, _) => (None, None),
        };
        let grant = GrantDescriptor::new(&placement, grant);
        let descriptor = serde_json::to_vec(&grant).map_err(|error| invalid(&error.to_string()))?;
        let reclamation = ReclamationQueue::new(
            Arc::clone(&bucket), queue_writer, queue_prefix, prefix.clone(), grant,
            self.storage.custody_namespace()?,
        );
        let upper = Arc::new(
            crate::reference::PrefixedStore::new(Arc::clone(&bucket), prefix).with_writer(writer),
        );
        crate::store::check_root(upper.as_ref(), &placement.record.id, true).await?;
        upper.put(&ObjectKey::new(DESCRIPTOR)?, Bytes::from(descriptor)).await?;
        let upper: Arc<dyn ObjectStore> = Arc::new(EncryptingStore::new(upper, &material)?);
        let store = Arc::new(ServingStore {
            lower,
            upper,
            writer: Some(ServingWriter {
                scope,
                reclamation,
                replica_objects: ReplicaObjects {
                    installed,
                    storage_prefix,
                    root: root.clone(),
                    bucket,
                    storage: self.storage.clone(),
                    io,
                    material,
                },
                _protection: protection,
            }),
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
        writer: None,
    }))
}

/// The writer publishes only below its own scoped epoch prefix. The
/// inherited checkpoint remains an independently retained immutable base.
struct ServingStore {
    lower: Option<Arc<dyn ObjectStore>>,
    upper: Arc<dyn ObjectStore>,
    writer: Option<ServingWriter>,
}

struct ServingWriter {
    scope: Arc<WriteScope>,
    reclamation: ReclamationQueue,
    replica_objects: ReplicaObjects,
    _protection: Arc<crate::journal::Held>,
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
        if let Some(writer) = &self.writer {
            writer.scope.serving().await.map_err(|error| Error::Remote {
                key: key.to_string(),
                message: error.to_string(),
            })?;
        }
        Ok(self.inherited(key).await?.unwrap_or(self.upper.as_ref()))
    }

    async fn authorize(&self, key: &ObjectKey) -> rtdb_storage::Result<&ServingWriter> {
        use rtdb_s3::WriteAuthorization;
        let writer = self.writer
            .as_ref()
            .ok_or_else(|| Error::Remote {
                key: key.to_string(),
                message: "A checkpoint preparation source is read-only".into(),
            })?;
        writer.scope.authorize(None)
            .await
            .map_err(|error| Error::Remote {
                key: key.to_string(),
                message: error.to_string(),
            })?;
        Ok(writer)
    }
}

#[async_trait]
impl ObjectStore for ServingStore {
    fn queues_offloaded_payload_reclamation(&self) -> bool {
        self.writer.is_some()
    }

    async fn open_replica_objects(
        &self,
        scope: ReplicaObjectScope,
    ) -> rtdb_storage::Result<Arc<dyn ObjectStore>> {
        let owner = self
            .writer
            .as_ref()
            .ok_or_else(|| Error::Task("The store has no installed replica object scope".into()))?;
        owner.replica_objects.open(scope).await
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
        let writer = self.authorize(key).await?;
        writer.reclamation.enqueue(self.upper.as_ref(), self.lower.as_deref(), key).await
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
