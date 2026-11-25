mod snapshot;

use super::prefix_retirement::{DESCRIPTOR, GrantDescriptor, local_lifetime};
use super::reclamation::{ReclamationQueue, queue_prefix};
use super::{Attachment, AttachmentAdmission, Routes, genesis, invalid};
use crate::deployment::CellStorage;
use crate::journal::{
    InstalledJournal,
    custody::{CheckpointAuthority, CheckpointCustody, Preparation},
};
use crate::keys::{KeyService, UnitLedger, UnitScope};
use crate::store::{EncryptingStore, ScopedCellWrites};
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use ordered_logkeeper::ordered::{Grant, Hold, Placement};
use prost_types::Any;
use rtdb_format::trie_retirement::InstalledRetirement;
use rtdb_node::{AudienceReadStorage, OpenedRecoveryStore, OpenedServingStores, RemoteStorageFactory};
use rtdb_replication::{BlockDiscovery, PublishedCheckpoint};
use rtdb_storage::{
    Error, IoRegion, ObjectKey, ObjectObservation, ObjectStore, ReadLease,
    log::{ReplicaObjectCustody, ReplicaObjectScope, ReplicaPayload},
    object_store::{ObjectReadAllocation, ReadReservation, RetainedBytes, StoredObject},
    remote::RemoteStore,
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
pub(super) struct ServingLease {
    /// The lease deadline and the instant after which a read renews it.
    state: std::sync::Mutex<Option<(Instant, Instant)>>,
    renewal: tokio::sync::Mutex<()>,
}

impl ServingLease {
    pub(super) async fn serving(&self, renew: impl std::future::Future<Output = Result<u64>>) -> Result<Instant> {
        if let Some((deadline, renew_after)) = self.lease_state() && Instant::now() < renew_after {
            return Ok(deadline);
        }
        let _renewing = self.renewal.lock().await;
        if let Some((deadline, renew_after)) = self.lease_state() && Instant::now() < renew_after {
            return Ok(deadline);
        }
        let sent = Instant::now();
        match renew.await {
            Ok(length) => {
                let length = Duration::from_millis(length);
                let deadline = sent.checked_add(length).filter(|deadline| crate::journal::read_selection::read_lease_admits(Instant::now(), *deadline))
                    .ok_or_else(|| invalid("The installed serving lease arrived expired"))?;
                *self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner) =
                    Some((deadline, sent + length / 2));
                Ok(deadline)
            }
            Err(error) => self.lease_state().map(|(deadline, _)| deadline)
                .filter(|deadline| crate::journal::read_selection::read_lease_admits(Instant::now(), *deadline)).ok_or(error),
        }
    }

    fn lease_state(&self) -> Option<(Instant, Instant)> {
        *self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

struct ReaderScope {
    journal: Arc<InstalledJournal>,
    protection: Arc<crate::journal::Held>,
    epoch: u64,
    lease: ServingLease,
}

impl ReaderScope {
    async fn serving(&self) -> Result<Instant> {
        self.lease.serving(self.journal.read_lease(self.protection.scope(), self.epoch)).await
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

    async fn wrap_stores(
        &self, configuration: &Any, ordinary: Arc<dyn ObjectStore>, audience: Option<Arc<dyn ObjectStore>>,
        _: Option<rtdb_storage::sealing::PublicationSealing>,
    ) -> Result<OpenedServingStores> {
        let audience = if let Some(store) = audience {
            let (placement, _) = self.routes.resolve(configuration)?;
            Some(AudienceReadStorage { source: placement.worker.deployment(), store })
        } else {
            None
        };
        Ok(OpenedServingStores { ordinary, audience })
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
        let custody = placement.hold_worker_custody(&self.releases).await?;
        let io = io.child_holding(custody);
        let (grant, hold) = match attachment.admission {
            AttachmentAdmission::Writer { admission } => (admission.grant, admission.recovery),
            AttachmentAdmission::Reader { epoch, hold, .. } => {
                return self.open_reader(placement, &attachment, root, io, epoch, hold).await;
            }
        };
        let scope = Arc::new(WriteScope {
            journal: Arc::clone(&placement.worker),
            grant,
            lease: ServingLease::default(),
        });
        scope.authorize(None).await?;
        placement.worker.replay_head(hold, hold.start).await?;
        let protection = placement.pin_root(hold, &self.releases).await?;
        let (lower, blocks) = self.open_checkpoint(&placement, &attachment, hold, Arc::clone(&io)).await?;
        let storage_prefix = placement.record.storage_prefix();
        let installed = placement.worker.placement();
        let grant_prefix = grant_prefix(&storage_prefix, installed.generation, grant.epoch);
        let io = local_lifetime(&self.storage, &grant_prefix, &io)?;
        let (bucket, s3) = serving_bucket(&self.storage, Arc::clone(&io)).await?;
        let prefix = source_prefix(&storage_prefix, installed.generation, grant.epoch, root);
        let queue_prefix = queue_prefix(&grant_prefix);
        let (writer, queue_writer): (Option<crate::reference::Bucket>, Option<crate::reference::Bucket>) = match (&self.storage, &self.scoped_writes, s3) {
            (CellStorage::S3 { config }, Some(scoped), Some(s3)) => {
                let authorization: Arc<dyn rtdb_s3::WriteAuthorization> = scope.clone();
                let writer = scoped.ordered_writer(config, &prefix, Arc::clone(&authorization), Arc::clone(&io)).await?;
                (
                    Some(Arc::new(Arc::unwrap_or_clone(writer).inherit_read_drain(&s3)?)),
                    Some(
                        scoped
                            .ordered_writer(config, &queue_prefix, authorization, Arc::clone(&io))
                            .await?,
                    ),
                )
            }
            (CellStorage::S3 { .. }, _, _) => {
                return Err(invalid("An ordered S3 writer requires a scoped write role"));
            }
            (CellStorage::Local { .. }, _, _) => (None, None),
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
        let store: Arc<dyn ObjectStore> = Arc::new(ServingStore {
            lower,
            upper,
            snapshot: Some(snapshot::Source {
                placement, keys: Arc::clone(&self.keys), io: Arc::clone(&io), tasks: self.releases.clone(),
                ordered_index_arrow_only: self.routes.is_runtime(),
            }),
            reader: None,
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
        let audience = Some(Arc::clone(&store));
        Ok(OpenedRecoveryStore {
            store,
            blocks,
            audience,
            checkpoint_metadata: None,
            fragment_storage: None,
            publication_sealing: None,
        })
    }
}

impl StoreFactory {
    async fn open_checkpoint(
        &self, placement: &super::Placement, attachment: &Attachment, hold: Hold, io: Arc<IoRegion>,
    ) -> Result<(Option<Arc<dyn ObjectStore>>, BlockDiscovery)> {
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
            let discovery = match opened.authority {
                CheckpointAuthority::Upload => BlockDiscovery::AtPublished(published),
                CheckpointAuthority::Erasure(_) => BlockDiscovery::AtErasure(published),
            };
            (Some(opened.store as Arc<dyn ObjectStore>), discovery)
        };
        Ok((lower, blocks))
    }

    async fn open_reader(
        &self, placement: Arc<super::Placement>, attachment: &Attachment,
        root: &ObjectKey, io: Arc<IoRegion>, epoch: u64, hold: Hold,
    ) -> Result<OpenedRecoveryStore> {
        let protection = placement.retained_reader(hold)?;
        let scope = ReaderScope {
            journal: Arc::clone(&placement.worker), protection, epoch, lease: ServingLease::default(),
        };
        let deadline = scope.serving().await?;
        let route = placement.worker.route().await?;
        if route.writer.epoch != epoch {
            return Err(invalid("The ordered reader epoch changed during attachment"));
        }
        let (lower, blocks) = with_read_lease(deadline, |_| {
            self.open_checkpoint(&placement, attachment, hold, Arc::clone(&io))
        }).await?;
        let objects = ReplicaObjects {
            installed: placement.worker.placement(), storage_prefix: placement.record.storage_prefix(),
            root: root.clone(), bucket: serving_bucket(&self.storage, Arc::clone(&io)).await?.0,
            io: Arc::clone(&io), storage: self.storage.clone(), keys: Arc::clone(&self.keys),
        };
        let upper: Arc<dyn ObjectStore> = if hold.start == hold.through && route.writer.grant.is_none() {
            Arc::new(rtdb_storage::MemoryStore::default())
        } else {
            with_read_lease(deadline, |_| objects.open_writer(epoch)).await?
        };
        let store = Arc::new(ServingStore {
            lower, upper, writer: None, reader: Some(ServingReader { scope, objects }),
            snapshot: Some(snapshot::Source {
                placement, keys: Arc::clone(&self.keys), io, tasks: self.releases.clone(),
                ordered_index_arrow_only: self.routes.is_runtime(),
            }),
        });
        Ok(OpenedRecoveryStore { store, blocks, audience: None, checkpoint_metadata: None, fragment_storage: None, publication_sealing: None })
    }
}

pub(super) async fn serving_bucket(
    storage: &CellStorage, io: Arc<IoRegion>,
) -> Result<(crate::reference::Bucket, Option<Arc<RemoteStore>>)> {
    let CellStorage::S3 { config } = storage else { return Ok((storage.bucket(io).await?, None)); };
    let s3 = Box::pin(config.open_cell(&config.cell_root()?, &BTreeMap::new(), io)).await?;
    let s3 = Arc::new(Arc::unwrap_or_clone(s3).with_read_drain(super::reclamation::read_census_scope));
    Ok((s3.clone(), Some(s3)))
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

#[derive(Clone)]
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
            bucket: serving_bucket(storage, Arc::clone(&io)).await?.0,
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
    let (bucket, _) = serving_bucket(storage, Arc::clone(&io)).await?;
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
        Some(custody.open(origin.base).await?.store as Arc<dyn ObjectStore>)
    };
    Ok(Arc::new(ServingStore {
        lower,
        upper,
        writer: None,
        reader: None,
        snapshot: None,
    }))
}

/// The writer publishes only below its own scoped epoch prefix. The
/// inherited checkpoint remains an independently retained immutable base.
struct ServingStore {
    lower: Option<Arc<dyn ObjectStore>>,
    upper: Arc<dyn ObjectStore>,
    writer: Option<ServingWriter>,
    reader: Option<ServingReader>,
    snapshot: Option<snapshot::Source>,
}

struct ServingReader {
    scope: ReaderScope,
    objects: ReplicaObjects,
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
        match self.serving_deadline().await? {
            Some(deadline) => with_read_lease(deadline, |_| work).await,
            None => work.await,
        }
    }

    async fn serving_deadline(&self) -> rtdb_storage::Result<Option<Instant>> {
        let deadline = if let Some(writer) = &self.writer {
            writer.scope.lease.serving(writer.scope.journal.lease(writer.scope.grant)).await
        } else if let Some(reader) = &self.reader {
            reader.scope.serving().await
        } else {
            return Ok(None);
        };
        deadline.map(Some).map_err(|error| Error::Remote {
            key: "installed serving lease".into(), message: error.to_string(),
        })
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

pub(super) async fn with_read_lease<T, F, E>(deadline: Instant, work: impl FnOnce(Instant) -> F) -> std::result::Result<T, E>
where
    F: std::future::Future<Output = std::result::Result<T, E>>,
    E: From<Error>,
{
    let lease = Arc::new(ReadLease::until(deadline));
    let result = tokio::time::timeout_at(deadline.into(), IoRegion::leased(Some(Arc::clone(&lease)), work(deadline))).await;
    lease.check()?;
    result.map_err(|_| E::from(Error::Closed("The installed serving lease expired".into())))?
}

#[async_trait]
impl ObjectStore for ServingStore {
    fn ordered_index_arrow_only(&self) -> bool {
        self.snapshot.as_ref().is_some_and(|source| source.ordered_index_arrow_only)
    }

    async fn catalog_delivery_lease(&self) -> Result<Option<Arc<ReadLease>>> {
        Ok(self.serving_deadline().await?.map(|deadline| Arc::new(ReadLease::until(deadline))))
    }

    async fn open_log_snapshot(
        &self, authority: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<Arc<dyn ObjectStore>> {
        let source = self.snapshot.as_ref().ok_or_else(|| invalid("This serving store has no snapshot owner"))?;
        let files = authority.downcast::<crate::journal::log::SnapshotFiles>()
            .map_err(|_| invalid("The exact snapshot hold belongs to a different provider"))?;
        let objects = self.writer.as_ref().map(|writer| &writer.replica_objects)
            .or_else(|| self.reader.as_ref().map(|reader| &reader.objects))
            .ok_or_else(|| invalid("The exact snapshot has no serving namespace"))?;
        source.open(files, objects.clone(), Arc::clone(&self.upper)).await
    }

    async fn retain_anchored_catalog(
        &self, cut: rtdb_types::CatalogCut, base: rtdb_types::CatalogCheckpoint,
        _paths: Option<Arc<[String]>>,
    ) -> Result<Option<Arc<dyn Send + Sync>>> {
        let source = self.snapshot.as_ref().ok_or_else(|| invalid("This serving store has no snapshot owner"))?;
        source.retain(cut, base).await.map(Some)
    }

    async fn replica_publication_address(&self, key: &ObjectKey) -> rtdb_storage::Result<Option<(rtdb_types::S3ObjectNamespace, String)>> {
        self.read(key, |store| store.replica_publication_address(key)).await
    }
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
            if super::reclamation::can_fence_trie_file(key.as_str()) {
                writer.reclamation.enqueue_receipt(
                    self.upper.as_ref(), self.lower.as_deref(), &key, receipt, installed.cut(), file,
                ).await?;
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
        let objects = self.writer.as_ref().map(|writer| &writer.replica_objects)
            .or_else(|| self.reader.as_ref().map(|reader| &reader.objects))
            .ok_or_else(|| Error::Task("The store has no installed replica payload broker".into()))?;
        let scope = custody.installed_scope()
            .ok_or_else(|| Error::Task("The payload permit does not name an installed placement".into()))?;
        let deadline = self.serving_deadline().await?.ok_or_else(|| Error::Closed("The replay lease is unavailable".into()))?;
        with_read_lease(deadline, |deadline| async move {
            let store = objects.open(scope, deadline).await?;
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

    async fn get_fresh_exact(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<Bytes> {
        Ok(self.observe_fresh_exact(key, length).await?.value)
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

    fn exact_object_allocation(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<ObjectReadAllocation> {
        self.upper.exact_object_allocation(key, length)
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
            if rtdb_format::keys::is_ordered_index_object(key.as_str()) {
                return Ok(None);
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
        match self.serving_deadline().await? {
            Some(deadline) => with_read_lease(deadline, |_| work).await,
            None => work.await,
        }
    }
}
