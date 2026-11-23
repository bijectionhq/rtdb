use super::prefix_retirement::{DESCRIPTOR, GrantDescriptor, local_lifetime};
use super::reclamation::{ReclamationQueue, queue_prefix};
use super::{Routes, genesis, invalid};
use crate::deployment::CellStorage;
use crate::journal::{
    InstalledJournal,
    custody::{CheckpointCustody, Preparation},
};
use crate::keys::{KeyService, UnitLedger, UnitScope};
use crate::store::{EncryptingStore, ScopedCellWrites};
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use ordered_logkeeper::ordered::{Grant, Placement};
use prost_types::Any;
use rtdb_format::trie_retirement::InstalledRetirement;
use rtdb_node::{OpenedRecoveryStore, RemoteStorageFactory};
use rtdb_replication::{BlockDiscovery, PublishedCheckpoint};
use rtdb_storage::{
    Error, IoRegion, ObjectKey, ObjectObservation, ObjectStore, ReadLease,
    log::{ReplicaObjectCustody, ReplicaObjectScope, ReplicaPayload},
    object_store::{ObjectReadAllocation, ReadReservation, RetainedBytes, StoredObject},
};
use rtdb_types::{
    Anomaly, AnomalyCategory, BlockIndex, PhysicalObjectPublication, PhysicalObjectVersion,
    ReplicaPosition, Result,
};
use std::collections::BTreeMap;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

pub(crate) struct StoreFactory {
    pub routes: Arc<Routes>,
    pub storage: CellStorage,
    pub keys: Arc<dyn KeyService>,
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
    async fn serving(&self) -> Result<Instant> {
        if let Some((deadline, renew_after)) = self.lease_state()
            && Instant::now() < renew_after
        {
            return Ok(deadline);
        }
        let _renewing = self.lease.renewal.lock().await;
        if let Some((deadline, renew_after)) = self.lease_state()
            && Instant::now() < renew_after
        {
            return Ok(deadline);
        }
        let sent = Instant::now();
        match self.journal.lease(self.grant).await {
            Ok(length) => {
                let length = Duration::from_millis(length);
                let deadline = sent
                    .checked_add(length)
                    .filter(|deadline| Instant::now() < *deadline)
                    .ok_or_else(|| {
                        invalid("The installed writer received an expired serving lease")
                    })?;
                *self
                    .lease
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    Some((deadline, sent + length / 2));
                Ok(deadline)
            }
            Err(error) => self
                .lease_state()
                .map(|(deadline, _)| deadline)
                .filter(|deadline| Instant::now() < *deadline)
                .ok_or_else(|| {
                    Anomaly::new(
                        AnomalyCategory::Unavailable,
                        rtdb_node::NOT_HELD,
                        format!("The installed writer has no current serving lease: {error}"),
                    )
                }),
        }
    }

    fn lease_state(&self) -> Option<(Instant, Instant)> {
        *self
            .lease
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
        let (lower, blocks) = if genesis(hold.checkpoint) && attachment.base.checkpoint.is_none() {
            (None, BlockDiscovery::List)
        } else {
            let custody = placement
                .checkpoint_custody(Arc::clone(&io), Arc::clone(&self.keys))
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
                    Some(
                        scoped
                            .ordered_writer(
                                config,
                                &prefix,
                                Arc::clone(&authorization),
                                Arc::clone(&io),
                            )
                            .await?,
                    ),
                    Some(
                        scoped
                            .ordered_writer(config, &queue_prefix, authorization, Arc::clone(&io))
                            .await?,
                    ),
                )
            }
            (CellStorage::S3 { .. }, None) => {
                return Err(invalid("An ordered S3 writer requires a scoped write role"));
            }
            (CellStorage::Local { .. }, _) => (None, None),
        };
        let grant = GrantDescriptor::new(&placement, grant);
        let descriptor = serde_json::to_vec(&grant).map_err(|error| invalid(&error.to_string()))?;
        let provider = self.storage.custody_namespace()?;
        let reclamation = ReclamationQueue::new(
            Arc::clone(&bucket),
            queue_writer,
            queue_prefix,
            prefix.clone(),
            grant,
            provider,
        );
        let upper = Arc::new(
            crate::reference::PrefixedStore::new(Arc::clone(&bucket), prefix.clone())
                .with_writer(writer),
        );
        crate::store::check_root(upper.as_ref(), &placement.record.id, true).await?;
        upper
            .put(&ObjectKey::new(DESCRIPTOR)?, Bytes::from(descriptor))
            .await?;
        let ledger = serving_ledger(
            Arc::clone(&self.keys),
            installed,
            provider,
            &prefix,
            Arc::clone(&io),
        );
        let upper: Arc<dyn ObjectStore> = Arc::new(EncryptingStore::with_units(upper, Arc::new(ledger)));
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
                    keys: Arc::clone(&self.keys),
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

pub(super) fn source_prefix(
    storage_prefix: &str,
    generation: u64,
    writer_epoch: u64,
    root: &ObjectKey,
) -> String {
    format!(
        "{}/{root}",
        grant_prefix(storage_prefix, generation, writer_epoch)
    )
}

pub(super) fn serving_ledger(
    keys: Arc<dyn KeyService>,
    installed: Placement,
    provider: [u8; 32],
    prefix: &str,
    io: Arc<IoRegion>,
) -> UnitLedger {
    let mut residence = provider.to_vec();
    residence.extend_from_slice(prefix.as_bytes());
    UnitLedger::new(keys, UnitScope::new(installed, b"serving", &residence), io)
}

pub(super) struct ReplicaObjects {
    installed: Placement,
    storage_prefix: String,
    root: ObjectKey,
    bucket: Arc<dyn ObjectStore>,
    storage: CellStorage,
    io: Arc<IoRegion>,
    keys: Arc<dyn KeyService>,
}

impl ReplicaObjects {
    pub(super) async fn for_custodian(
        placement: &super::Placement,
        storage: &CellStorage,
        keys: Arc<dyn KeyService>,
        io: Arc<IoRegion>,
    ) -> Result<Self> {
        Ok(Self {
            installed: placement.worker.placement(),
            storage_prefix: placement.record.storage_prefix(),
            root: ObjectKey::new(crate::archive::checkpoint_storage_root()?)?,
            bucket: storage.bucket(Arc::clone(&io)).await?,
            storage: storage.clone(),
            io,
            keys,
        })
    }

    async fn open(
        &self,
        scope: ReplicaObjectScope,
        deadline: Instant,
    ) -> rtdb_storage::Result<Arc<dyn ObjectStore>> {
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
        self.open_writer_with_grant(scope.writer_epoch, Some(deadline))
            .await
    }

    pub(super) async fn open_writer(
        &self,
        epoch: u64,
    ) -> rtdb_storage::Result<Arc<dyn ObjectStore>> {
        self.open_writer_with_grant(epoch, None).await
    }

    async fn open_writer_with_grant(
        &self,
        epoch: u64,
        deadline: Option<Instant>,
    ) -> rtdb_storage::Result<Arc<dyn ObjectStore>> {
        let prefix = source_prefix(
            &self.storage_prefix,
            self.installed.generation,
            epoch,
            &self.root,
        );
        let (bucket, io) = match &self.storage {
            CellStorage::Local { .. } => {
                let scope = grant_prefix(&self.storage_prefix, self.installed.generation, epoch);
                let lifetime = local_lifetime(&self.storage, &scope, &self.io)
                    .map_err(|error| Error::Task(error.to_string()))?;
                let bucket = self
                    .storage
                    .bucket(Arc::clone(&lifetime))
                    .await
                    .map_err(|error| Error::Task(error.to_string()))?;
                (bucket, lifetime)
            }
            CellStorage::S3 { .. } => (Arc::clone(&self.bucket), Arc::clone(&self.io)),
        };
        let provider = self
            .storage
            .custody_namespace()
            .map_err(|error| Error::Task(error.to_string()))?;
        let ledger = serving_ledger(
            Arc::clone(&self.keys),
            self.installed,
            provider,
            &prefix,
            io,
        );
        let ledger = match deadline {
            Some(deadline) => ledger.bounded(deadline),
            None => ledger,
        };
        let bucket = Arc::new(crate::reference::PrefixedStore::new(bucket, prefix));
        let owner = self.installed.deployment.to_string();
        let marker = ObjectKey::new(crate::store::ROOT_MARKER)?;
        if bucket.get_fresh_exact(&marker, owner.len()).await?.as_ref() != owner.as_bytes() {
            return Err(Error::Task(
                "The replica object namespace has a different owner".into(),
            ));
        }
        Ok(Arc::new(EncryptingStore::with_units(bucket, Arc::new(ledger))))
    }
}

pub(super) async fn reopen_source(
    placement: &super::Placement,
    storage: &CellStorage,
    custody: &CheckpointCustody,
    keys: Arc<dyn KeyService>,
    io: &Arc<IoRegion>,
    origin: Preparation,
) -> Result<Arc<dyn ObjectStore>> {
    let root = ObjectKey::new(crate::archive::checkpoint_storage_root()?)?;
    let storage_prefix = placement.record.storage_prefix();
    let generation = placement.worker.placement().generation;
    let prefix = source_prefix(&storage_prefix, generation, origin.writer.epoch, &root);
    let io = local_lifetime(
        storage,
        &grant_prefix(&storage_prefix, generation, origin.writer.epoch),
        io,
    )?;
    let bucket = storage.bucket(Arc::clone(&io)).await?;
    let ledger = serving_ledger(
        keys,
        placement.worker.placement(),
        storage.custody_namespace()?,
        &prefix,
        io,
    );
    let upper = Arc::new(crate::reference::PrefixedStore::new(bucket, prefix));
    crate::store::check_root(upper.as_ref(), &placement.record.id, false).await?;
    let upper = Arc::new(EncryptingStore::with_units(upper, Arc::new(ledger)));
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

    async fn read<'a, T: Send>(
        &'a self,
        key: &'a ObjectKey,
        read: impl FnOnce(&'a dyn ObjectStore) -> BoxFuture<'a, rtdb_storage::Result<T>> + Send,
    ) -> rtdb_storage::Result<T> {
        let work = async { read(self.inherited(key).await?.unwrap_or(self.upper.as_ref())).await };
        let Some(writer) = &self.writer else {
            return work.await;
        };
        self.with_serving_lease(writer, |_| work).await
    }

    async fn with_serving_lease<
        T: Send,
        F: std::future::Future<Output = rtdb_storage::Result<T>> + Send,
    >(
        &self,
        writer: &ServingWriter,
        work: impl FnOnce(Instant) -> F + Send,
    ) -> rtdb_storage::Result<T> {
        let deadline = writer
            .scope
            .serving()
            .await
            .map_err(|error| Error::Remote {
                key: "installed serving grant".into(),
                message: error.to_string(),
            })?;
        let started = Instant::now();
        let micros = |length: Duration| length.as_micros().min(i64::MAX as u128) as i64;
        let length = micros(deadline.saturating_duration_since(started));
        let lease = Arc::new(ReadLease::new(
            Some(length),
            Arc::new(move || micros(started.elapsed())),
        ));
        let result = tokio::time::timeout_at(
            deadline.into(),
            IoRegion::leased(Some(Arc::clone(&lease)), work(deadline)),
        )
        .await;
        lease.check()?;
        result.map_err(|_| Error::Closed("The installed serving lease expired".into()))?
    }

    async fn authorize(&self, key: &ObjectKey) -> rtdb_storage::Result<&ServingWriter> {
        use rtdb_s3::WriteAuthorization;
        let writer = self.writer.as_ref().ok_or_else(|| Error::Remote {
            key: key.to_string(),
            message: "A checkpoint preparation source is read-only".into(),
        })?;
        writer
            .scope
            .authorize(None)
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
    fn defers_trie_releases(&self) -> bool {
        self.writer.is_some()
    }

    fn releases_receipt_l0(&self) -> bool {
        self.writer.is_some()
    }

    // Settlement still belongs to the recovery dependency owner. The active
    // writer can only authenticate the installed removal and queue its files.
    async fn release_trie_files(&self, receipt: &ObjectKey, contents: Bytes) -> Result<bool> {
        let installed = InstalledRetirement::decode(receipt.as_str(), &contents)?;
        let writer = self.authorize(receipt).await?;
        if self.get_fresh_exact(receipt, contents.len()).await? != contents {
            return Err(invalid("The installed grant trie release differs from its stored receipt"));
        }
        let mut files = installed.files().iter().collect::<Vec<_>>();
        files.sort_by_key(|file| file.release_order());
        for file in files {
            let key = ObjectKey::new(&file.key)?;
            if rtdb_format::keys::is_receipt_trie_file(key.as_str()) {
                let version = file.version.as_ref().filter(|_| file.matches_publication);
                if let Some(version) = version {
                    writer.reclamation.enqueue_receipt(
                        self.upper.as_ref(), self.lower.as_deref(), &key, receipt, installed.cut(), version,
                    ).await?;
                }
            } else {
                writer.reclamation.enqueue(self.upper.as_ref(), self.lower.as_deref(), &key).await?;
            }
        }
        Ok(true)
    }

    fn queues_offloaded_payload_reclamation(&self) -> bool {
        self.writer.is_some()
    }

    async fn read_replica_payload(
        &self, custody: &ReplicaObjectCustody, memory: &Arc<rtdb_columnar::MemoryBudget>,
    ) -> rtdb_storage::Result<ReplicaPayload> {
        let owner = self.writer.as_ref()
            .ok_or_else(|| Error::Task("The store has no installed replica payload broker".into()))?;
        let scope = custody.installed_scope()
            .ok_or_else(|| Error::Task("The payload permit does not name an installed placement".into()))?;
        self.with_serving_lease(owner, |deadline| async move {
            let store = owner.replica_objects.open(scope, deadline).await?;
            custody.read_from(store.as_ref(), memory).await
        }).await
    }

    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        self.read(key, |store| store.head_fresh(key)).await
    }

    async fn observe_head(
        &self,
        key: &ObjectKey,
    ) -> rtdb_storage::Result<ObjectObservation<StoredObject>> {
        self.read(key, |store| store.observe_head(key)).await
    }

    async fn retirement_version(&self, key: &ObjectKey) -> rtdb_storage::Result<Option<PhysicalObjectVersion>> {
        self.read(key, |store| store.retirement_version(key)).await
    }

    async fn retirement_publication(&self, key: &ObjectKey) -> rtdb_storage::Result<Option<PhysicalObjectPublication>> {
        self.read(key, |store| store.retirement_publication(key)).await
    }

    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> {
        self.read(key, |store| store.get_fresh(key)).await
    }

    async fn observe_fresh_exact(
        &self,
        key: &ObjectKey,
        length: usize,
    ) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
        self.read(key, |store| store.observe_fresh_exact(key, length))
            .await
    }

    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
        self.read(key, |store| store.get_range(key, range)).await
    }

    async fn observe_range(
        &self,
        key: &ObjectKey,
        range: Range<u64>,
    ) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
        self.read(key, |store| store.observe_range(key, range))
            .await
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
        self.read(key, |store| store.get_reserved_object(key, reservation))
            .await
    }

    async fn get_reserved_scan_range(
        &self,
        key: &ObjectKey,
        range: Range<u64>,
        reservation: ReadReservation,
    ) -> rtdb_storage::Result<RetainedBytes> {
        self.read(key, |store| {
            store.get_reserved_scan_range(key, range, reservation)
        })
        .await
    }

    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> {
        self.put_with_publication(key, bytes).await.map(drop)
    }

    async fn put_with_publication(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<Option<PhysicalObjectPublication>> {
        self.authorize(key).await?;
        if let Some(lower) = self.inherited(key).await? {
            if lower.get_fresh_exact(key, bytes.len()).await? != bytes {
                return Err(Error::ConflictingObject(key.to_string()));
            }
            return lower.retirement_publication(key).await;
        }
        self.upper.put_with_publication(key, bytes).await
    }

    async fn put_file(&self, key: &ObjectKey, path: &Path) -> rtdb_storage::Result<()> {
        self.put_file_with_publication(key, path).await.map(drop)
    }

    async fn put_file_with_publication(&self, key: &ObjectKey, path: &Path) -> rtdb_storage::Result<Option<PhysicalObjectPublication>> {
        self.authorize(key).await?;
        if self.inherited(key).await?.is_some() {
            return Err(Error::ConflictingObject(key.to_string()));
        }
        self.upper.put_file_with_publication(key, path).await
    }

    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
        let writer = self.authorize(key).await?;
        writer
            .reclamation
            .enqueue(self.upper.as_ref(), self.lower.as_deref(), key)
            .await
    }

    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
        let work = async {
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
        };
        match &self.writer {
            Some(writer) => self.with_serving_lease(writer, |_| work).await,
            None => work.await,
        }
    }
}
