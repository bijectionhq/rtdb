//! A deployment's object store as a node sees it (contract `ObjectStorage`): fenced by the holder's epoch, sealed under individually enrolled publication slots, and unable to delete.
//!
//! - `FencedStore` checks current directory authority before admitting each write (`ZombiesCannotDamageStorage`).
//! - `EncryptingStore` seals objects with AES-256-GCM with provider-owned keys bound to the publication and object name.
//! - `GarbageStore` turns a delete into a garbage record; only the reaper erases an object, once it has been garbage for the grace period (`OnlyTheReaperDeletes`, `ReapUnreachable`).

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use prost_types::Any;
use rtdb_directory::{Cas, CatalogAdmissionFloor, DeploymentRecord, Directory, Lifecycle, LineageChange, LineageRetirement, MemberId, MemberPool, MemberRole, MemberState, HostedTask, ReplayCustody, RetirementWork, StoragePublication, StorageRegion, now_ms};
use rtdb_format::{keys::StorageRoot, trie_retirement::InstalledRetirement};
use rtdb_node::{AudienceReadStorage, OpenedRecoveryStore, OpenedServingStores, RemoteStorageFactory};
use rtdb_storage::{Error, IoRegion, ObjectKey, ObjectObservation, ObjectStore, ReadReservation, RetainedBytes, ObjectReadAllocation, object_store::{IndexPageLifecycle, StoredObject}, remote::PublicationJournal};
use rtdb_types::{Anomaly, CatalogCut, PhysicalObjectVersion, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use crate::cipher::{Cipher, object};
use crate::catalog_readers::CatalogReaders;
use crate::deployment::CellStorage;
use crate::keys::{DeploymentDelivery, KeyService, UnitLedger, UnitOperation, UnitOutput, UnitScope};
pub use rtdb_storage::sealing::EncryptingStore;
use rtdb_storage::sealing::{ObjectCipher, PublicationSealing};
use crate::checkpoint_retention::checkpoint_metadata_collectable;
use crate::ordered_index_storage::{self, HostedIndexPages};
use crate::reference::custody::ReferenceReads;

#[path = "key_enrollment.rs"]
mod key_enrollment;
pub(crate) mod exact_retirement;
mod replay_retirement;
mod publication_retirement;

/// The protobuf type name the cell's storage factory is registered under.
pub const CELL_STORAGE_TYPE: &str = "rtdb.multi.CellStorage";
pub const GARBAGE_DIRECTORY: &str = "~garbage";
const ORDERED_INDEX_RELEASE: &str = "-ordered-index";
const TRIE_RETIREMENT_RELEASE: &str = "-trie-retirement";

pub(crate) fn belongs_to_ordered_index(name: &str) -> bool {
    rtdb_format::keys::is_ordered_index_object(name)
        || name.strip_prefix(GARBAGE_DIRECTORY)
            .is_some_and(|record| record.starts_with('/') && record.ends_with(ORDERED_INDEX_RELEASE))
}

pub(crate) fn stays_in_source_residence(name: &str) -> bool {
    name.starts_with("oversized/") || belongs_to_ordered_index(name) || rtdb_format::keys::is_trie_retirement_object(name)
        || name.strip_prefix(GARBAGE_DIRECTORY)
            .is_some_and(|record| record.starts_with("/0-replica-")
                || (record.starts_with('/') && record.ends_with(TRIE_RETIREMENT_RELEASE)))
}

pub(crate) fn directory_publications(keys: &dyn KeyService, record: &DeploymentRecord, region: Arc<IoRegion>) -> Result<PublicationSealing> {
    let ledger = keys.retained_unit_ledger(UnitScope::directory_serving(&record.org, &record.lineage), region)?;
    let origin = UnitScope::publication_origin(&record.storage_prefix());
    Ok(PublicationSealing::new(ledger, origin, |name| {
        (!name.as_str().starts_with('~') || rtdb_format::keys::is_trie_retirement_object(name.as_str()))
            && ordered_index_storage::page_generation(name.as_str()).is_none()
    }))
}


/// The deployment store a node opens, carried in its `DatabaseConfig`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellStoreConfig {
    /// The cell's object storage, and the deployment's prefix within it.
    pub storage: crate::deployment::CellStorage,
    pub prefix: String,
    pub deployment: String,
    /// The epoch a holder opened the store at; absent for a reader, which never writes.
    pub writer_epoch: Option<u64>,
    /// A compaction worker's store, which may write compaction outputs and nothing else.
    #[serde(default)]
    pub compactor: bool,
    /// A snapshot reader's store, which recovers exactly this block.
    #[serde(default)]
    pub snapshot_block: Option<i64>,
}

impl CellStoreConfig {
    fn cache_identity(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(&("directory-publication-envelope-v1", &self.storage, &self.prefix, &self.deployment))
            .map_err(|error| Anomaly::fault("rtdb.multi/cache-identity", error.to_string()))
    }
}

pub struct CellStoreFactory {
    pub directory: Arc<dyn Directory>,
    pub keys: Arc<dyn KeyService>,
    pub storage_namespaces: rtdb_directory::StorageNamespaces,
    pub(crate) hosted_task: Option<HostedTask>,
    pub(crate) audience_source: Option<Uuid>,
    pub(crate) storage_regions: TaskTracker,
    pub(crate) scoped_writes: Option<Arc<ScopedCellWrites>>,
    pub(crate) local_witnesses: Option<Arc<LocalWitnessPool>>,
    pub(crate) replay_broker: Option<crate::replay_broker::Client>,
}

enum LocalWitnessSlot {
    Empty,
    Preparing,
    Ready(Uuid, File),
}

pub(crate) struct LocalWitnessPool {
    keys: Arc<dyn KeyService>,
    slot: Mutex<LocalWitnessSlot>,
}

impl LocalWitnessPool {
    pub(crate) fn new(keys: Arc<dyn KeyService>, tasks: &TaskTracker) -> Arc<Self> {
        let pool = Arc::new(Self { keys, slot: Mutex::new(LocalWitnessSlot::Empty) });
        let _ = pool.take(tasks);
        pool
    }

    fn take(self: &Arc<Self>, tasks: &TaskTracker) -> Option<(Uuid, File)> {
        let ready = match std::mem::replace(&mut *self.slot.lock(), LocalWitnessSlot::Preparing) {
            LocalWitnessSlot::Preparing => return None,
            LocalWitnessSlot::Empty => None,
            LocalWitnessSlot::Ready(id, file) => Some((id, file)),
        };
        let pool = Arc::clone(self);
        tasks.spawn_blocking(move || {
            let id = Uuid::new_v4();
            let prepared = match pool.keys.hold_local_storage_region(id) {
                Ok(Some(file)) => LocalWitnessSlot::Ready(id, file),
                Ok(None) => LocalWitnessSlot::Empty,
                Err(error) => {
                    eprintln!("Could not prepare a standby storage witness: {}", error.code);
                    LocalWitnessSlot::Empty
                }
            };
            *pool.slot.lock() = prepared;
        });
        ready
    }
}

impl Drop for LocalWitnessPool {
    fn drop(&mut self) {
        if let LocalWitnessSlot::Ready(id, file) = std::mem::replace(self.slot.get_mut(), LocalWitnessSlot::Empty) {
            drop(file);
            if let Err(error) = self.keys.forget_local_storage_region(id) {
                eprintln!("Could not remove standby storage witness {id}: {}", error.code);
            }
        }
    }
}

pub(crate) struct ScopedCellWrites {
    issuer: rtdb_s3::ScopedS3Writes,
    member: OnceLock<MemberId>,
}

impl ScopedCellWrites {
    pub fn new(issuer: rtdb_s3::ScopedS3Writes) -> Self { Self { issuer, member: OnceLock::new() } }
    pub fn set_member(&self, member: MemberId) { self.member.set(member).expect("a storage factory has one member"); }

    pub(crate) async fn ordered_writer(
        &self,
        storage: &rtdb_s3::S3Config,
        prefix: &str,
        authorization: Arc<dyn rtdb_s3::WriteAuthorization>,
        io: Arc<IoRegion>,
    ) -> Result<Arc<rtdb_storage::remote::RemoteStore>> {
        let cell = storage.cell_root()?;
        let root = ObjectKey::new(format!("{cell}/{prefix}"))?;
        self.issuer.open_remote(storage, &cell, &root, authorization, io).await
    }
}

struct ScopedWriter {
    directory: Arc<dyn Directory>,
    deployment: String,
    prefix: String,
    epoch: Option<u64>,
    member: MemberId,
}

#[async_trait]
impl rtdb_s3::WriteAuthorization for ScopedWriter {
    async fn authorize(&self, expires: Option<SystemTime>) -> Result<()> {
        let inactive = || Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, rtdb_node::NOT_HELD, "The deployment's write grant is fenced");
        let lease_until = expires.map(|expires| {
            let expires = expires.checked_add(rtdb_s3::SCOPED_S3_CLOCK_ALLOWANCE)
                .ok_or_else(|| Anomaly::fault("rtdb.multi/storage-lease", "The storage lease expiration is out of range"))?;
            let elapsed = expires.duration_since(UNIX_EPOCH).map_err(|error| Anomaly::fault("rtdb.multi/storage-lease", error.to_string()))?;
            i64::try_from(elapsed.as_millis()).map_err(|error| Anomaly::fault("rtdb.multi/storage-lease", error.to_string()))
        }).transpose()?;
        for _ in 0..8 {
            let (current, member) = tokio::try_join!(self.directory.deployment(&self.deployment), self.directory.member(&self.member))?;
            let (mut current, member) = current.zip(member).ok_or_else(&inactive)?;
            let now = now_ms();
            let admitted = current.storage_prefix() == self.prefix && match self.epoch {
                Some(epoch) => current.epoch == epoch && current.holder.as_ref() == Some(&self.member) && current.may_hold_on(&member, now),
                None => current.lifecycle == Lifecycle::Live && member.role == MemberRole::Node && member.pool == MemberPool::Compaction
                    && member.state == MemberState::Eligible && member.is_live(now) && current.permits_engine(&member),
            };
            if !admitted { return Err(inactive()); }
            let Some(lease_until) = lease_until.filter(|deadline| current.s3_write_lease_until_ms.is_none_or(|existing| *deadline > existing))
                else { return Ok(()); };
            current.s3_write_lease_until_ms = Some(lease_until);
            let version = current.version;
            if let Cas::Applied(_) = self.directory.replace_deployment(current, version).await? { return Ok(()); }
        }
        Err(Anomaly::new(rtdb_types::AnomalyCategory::Busy, "rtdb.multi/storage-lease-contended", "The deployment kept changing while recording a write session"))
    }
}

#[async_trait]
impl RemoteStorageFactory for CellStoreFactory {
    fn table_history(&self, configuration: &Any) -> Result<Option<Arc<dyn rtdb_maintenance::TableHistorySource>>> {
        let config: CellStoreConfig = serde_json::from_slice(&configuration.value)
            .map_err(|error| Anomaly::incorrect("rtdb.multi/storage-config", format!("Cell storage configuration is malformed: {error}")))?;
        if config.writer_epoch.is_none() { return Ok(None); }
        Ok(Some(Arc::new(crate::archive::table_history::HostedTableReplay::new(
            Arc::clone(&self.directory), Arc::clone(&self.keys), self.hosted_task.clone(), config, self.replay_broker.clone(),
        ))))
    }

    async fn open(&self, configuration: &Any, storage_root: &ObjectKey, io: Arc<IoRegion>) -> Result<Arc<dyn ObjectStore>> {
        Ok(self.open_for_recovery(configuration, storage_root, io).await?.store)
    }

    async fn open_for_recovery(&self, configuration: &Any, storage_root: &ObjectKey, io: Arc<IoRegion>) -> Result<OpenedRecoveryStore> {
        let config: CellStoreConfig = serde_json::from_slice(&configuration.value)
            .map_err(|error| Anomaly::incorrect("rtdb.multi/storage-config", format!("Cell storage configuration is malformed: {error}")))?;
        if (config.writer_epoch.is_some() || config.compactor) && self.scoped_writes.is_none()
            && matches!(&config.storage, CellStorage::S3 { config } if config.terminal_scope.is_some())
        {
            return Err(Anomaly::incorrect("rtdb.multi/terminal-storage", "A terminal cell writer requires a scoped S3 role"));
        }
        let uses_replay_archive = storage_root.as_str() == crate::archive::checkpoint_storage_root()?;
        let (record, region, owner, release, checkpoint_metadata) = self.admit_region(&config, io, uses_replay_archive).await?;
        let audience_writer = self.audience_writer(&config, &record).is_some();
        let blocks = discovery_from_admitted_record(&config, &record)?;
        let fragment_storage = crate::fragments::storage_identity(self.storage_namespaces.objects, &record);
        let checkpoint_metadata: Arc<dyn Send + Sync> = Arc::new(checkpoint_metadata);
        let (opened, audience_reads, publications) = IoRegion::reading(rtdb_storage::HeldView::recovery(Arc::clone(&checkpoint_metadata)), async {
            let recovery = if uses_replay_archive {
                crate::archive::ArchivedCheckpoint::load(&record, self.keys.as_ref(), owner, region.clone(), audience_writer).await?
            } else {
                None
            };
            let mut publications = directory_publications(self.keys.as_ref(), &record, Arc::clone(&region))?;
            if let Some(recovery) = &recovery { publications = publications.with_archive(recovery.object_cipher()); }
            if record.storage_namespaces.is_some() {
                crate::deployment::require_storage_namespaces(&record, self.storage_namespaces)?;
            }
            let bucket = config.storage.open_bucket(Arc::clone(&region), audience_writer).await?;
            let store = format!("{}/{}", config.prefix, storage_root.as_str());
            if let Some(journal) = region.publication_journal_owner() {
                publications = publications.with_attempts(journal, store.clone());
            }
            let writer = match (&self.scoped_writes, &config.storage) {
                (Some(scoped), crate::deployment::CellStorage::S3 { config: storage }) if config.writer_epoch.is_some() || config.compactor => {
                    let cell_root = storage.cell_root()?;
                    let root = ObjectKey::new(format!("{cell_root}/{store}"))?;
                    let member = scoped.member.get().cloned()
                        .ok_or_else(|| Anomaly::fault("rtdb.multi/write-member-missing", "The node has no registered identity for scoped writes"))?;
                    let authorization = Arc::new(ScopedWriter { directory: Arc::clone(&self.directory), deployment: config.deployment.clone(),
                        prefix: config.prefix.clone(), epoch: config.writer_epoch, member });
                    Some(scoped.issuer.open(storage, &cell_root, &root, authorization, region).await?)
                }
                _ => None,
            };
            let local = Arc::new(crate::reference::PrefixedStore::new(Arc::clone(&bucket), store.clone()).with_writer(writer));
            let root: Arc<dyn ObjectStore> = Arc::new(FencedStore {
                inner: local,
                directory: self.directory.clone(),
                deployment: config.deployment.clone(),
                prefix: config.prefix.clone(),
                fence: Fence::from(&config),
                local_publications: matches!(config.storage, CellStorage::Local { .. }).then_some(owner),
                _release: Some(release),
            });
            check_root(root.as_ref(), &config.deployment, config.writer_epoch.is_some()).await?;
            let pages = HostedIndexPages::new(
                self.directory.clone(), config.deployment, store.clone(), owner, config.writer_epoch, self.storage_regions.clone(),
            );
            let custody = ReferenceReads::new(
                self.directory.clone(), self.keys.clone(), self.storage_regions.clone(), &record,
                storage_root.as_str(), self.storage_namespaces,
                if matches!(config.storage, CellStorage::Local { .. }) { ReplayCustody::Local }
                else { ReplayCustody::Hosted { owner: self.hosted_task.clone() } },
            );
            let (mut opened, audience_reads) = open_cell_store(bucket, &store, root, &record, wall_clock(), recovery, custody).await?;
            let audience_reads = audience_reads && audience_writer;
            if audience_reads {
                let packing = opened.packing.as_ref().expect("opened cell stores own their pack locations");
                packing.prepare_audience_reads().await?;
            }
            if let Some(expected) = record.recovery_manifest_sha256 {
                let checkpoint = record.checkpoint.ok_or_else(|| storage_region_error("A recovery manifest has no checkpoint"))?;
                let plaintext = EncryptingStore::published(opened.clone(), publications.clone());
                if !crate::archive::matches_recovery_manifest(&plaintext, checkpoint, rtdb_types::StorageEpoch(record.log_epoch), expected).await? {
                    return Err(storage_region_error("The recovery inventory does not match the directory receipt"));
                }
            }
            let serving = Arc::get_mut(&mut opened).expect("the new store has one owner");
            serving.index_pages = Some(pages);
            serving.requires_search_publication = matches!(config.storage, CellStorage::S3 { .. });
            serving.catalog_readers = Some(CatalogReaders::new(
                self.directory.clone(), self.storage_regions.clone(), &record, storage_root.to_string(), self.storage_namespaces, owner, config.writer_epoch,
            ));
            Ok::<_, Anomaly>((opened, audience_reads, publications))
        }).await?;
        let store: Arc<dyn ObjectStore> = opened;
        let audience = audience_reads.then(|| Arc::clone(&store));
        Ok(OpenedRecoveryStore { store, audience, blocks, checkpoint_metadata: Some(checkpoint_metadata), fragment_storage: Some(fragment_storage), publication_sealing: Some(publications) })
    }

    fn cache_identity<'a>(&self, configuration: &'a Any) -> Result<std::borrow::Cow<'a, [u8]>> {
        let store: CellStoreConfig = serde_json::from_slice(&configuration.value)
            .map_err(|error| Anomaly::incorrect("rtdb.multi/storage-config", format!("Cell storage configuration is malformed: {error}")))?;
        Ok(std::borrow::Cow::Owned(store.cache_identity()?))
    }

    /// Both views use the same admitted publication slots. The disk cache holds ciphertext.
    async fn wrap_stores(&self, configuration: &Any, cached: Arc<dyn ObjectStore>, audience: Option<Arc<dyn ObjectStore>>, publications: Option<PublicationSealing>) -> Result<OpenedServingStores> {
        let config: CellStoreConfig = serde_json::from_slice(&configuration.value)
            .map_err(|error| Anomaly::incorrect("rtdb.multi/storage-config", format!("Cell storage configuration is malformed: {error}")))?;
        let record = self.directory.deployment(&config.deployment).await?
            .filter(|record| record.storage_prefix() == config.prefix)
            .ok_or_else(|| storage_region_error("The cached store belongs to another residence"))?;
        let id = record.key_id();
        let material = match self.keys.existing_key(&id).await? {
            Some(material) => material,
            None => self.enroll_serving_key(&config, &id).await?,
        };
        let keys: Arc<dyn ObjectCipher> = Arc::new(ServingKeys {
            keys: Arc::clone(&self.keys),
            residence: record.storage_prefix(),
            directory: Arc::clone(&self.directory),
            installation: record.id.clone(),
            writer_epoch: config.writer_epoch,
            io: IoRegion::new(),
            deployment: Cipher::new(&material)?,
            delivery: DeploymentDelivery::new(self.keys.clone(), id, &material),
        });
        let publications = match publications {
            Some(publications) => publications,
            None => directory_publications(self.keys.as_ref(), &record, IoRegion::new())?,
        };
        let ordinary = Arc::new(EncryptingStore::with_units(cached, Arc::clone(&keys)).with_publications(publications.clone()));
        let audience = audience.zip(self.audience_writer(&config, &record)).map(|(store, source)| {
            let store = Arc::new(AudienceObjectStore(EncryptingStore::with_units(store, keys).with_publications(publications)));
            AudienceReadStorage { source, store }
        });
        Ok(OpenedServingStores { ordinary, audience })
    }
}

struct ServingKeys {
    keys: Arc<dyn KeyService>,
    residence: String,
    directory: Arc<dyn Directory>,
    installation: String,
    writer_epoch: Option<u64>,
    io: Arc<IoRegion>,
    deployment: Cipher,
    delivery: Arc<DeploymentDelivery>,
}

#[async_trait]
impl ObjectCipher for ServingKeys {
    async fn operate(&self, name: &ObjectKey, operation: UnitOperation) -> rtdb_storage::Result<UnitOutput> {
        if let Some((index, generation)) = ordered_index_storage::page_generation(name.as_str()) {
            if operation.provisions() {
                let epoch = self.writer_epoch.ok_or_else(|| Error::Closed("The index writer is fenced".into()))?;
                let definition = format!("/{}/{index}", rtdb_format::keys::ORDERED_INDEX_ROOT);
                let current = self.directory.deployment(&self.installation).await
                    .map_err(|error| publication_error(name, error))?;
                let admitted = current.is_some_and(|record| {
                    record.storage_prefix() == self.residence && record.epoch == epoch && record.holder.is_some()
                        && matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
                        && record.ordered_indexes.iter().any(|(name, state)| {
                            name.ends_with(&definition) && state.admits(generation)
                                && state.generations.get(&generation).is_some_and(|users| !users.is_empty())
                        })
                });
                if !admitted {
                    return Err(Error::Closed("The directory has no admitted index page generation".into()));
                }
            }
            let scope = UnitScope::ordered_index(&self.residence, index, generation);
            return UnitLedger::new(Arc::clone(&self.keys), scope, Arc::clone(&self.io))
                .operate(name, operation).await;
        }
        if matches!(operation, UnitOperation::Grant) {
            return self.delivery.operate(name, operation).await;
        }
        operation.apply(&self.deployment, name)
    }
}

impl CellStoreFactory {
    fn audience_writer(&self, config: &CellStoreConfig, record: &DeploymentRecord) -> Option<Uuid> {
        self.audience_source.filter(|source| Uuid::parse_str(&record.id).ok() == Some(*source)
            && matches!(&config.storage, CellStorage::S3 { .. })
            && config.writer_epoch == Some(record.epoch) && record.holder.is_some()
            && record.linearizable_reads && record.lifecycle == Lifecycle::Live
            && !config.compactor && config.snapshot_block.is_none())
    }

    async fn enroll_serving_key(&self, config: &CellStoreConfig, allow_id: &str) -> Result<[u8; crate::keys::KEY_BYTES]> {
        let unavailable = || Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.multi/key-missing", "The serving key is unavailable");
        let record = self.directory.deployment(&config.deployment).await?
            .filter(|record| record.storage_prefix() == config.prefix && record.key_id() == allow_id)
            .ok_or_else(unavailable)?;
        let current_writer = config.writer_epoch == Some(record.epoch) && record.holder.is_some()
            && matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined);
        let original_residence = record.incarnation == 1 && record.log_epoch == 0 && record.lineage == record.id
            && record.parent.is_none() && record.move_from.is_none() && record.cell_transfer.is_none()
            && record.storage_generation.is_none() && record.retired_residences.is_empty();
        let observed_history = record.checkpoint.is_some() || record.checkpoint_recovery.is_some()
            || record.checkpoint_stage.is_some() || record.replica_anchor.is_some() || !record.replica_journals.is_empty()
            || !record.log_copy_servers.is_empty();
        if !key_enrollment::may_enroll(current_writer, original_residence, observed_history) {
            return Err(unavailable());
        }
        self.keys.key(allow_id).await
    }
}

pub(crate) const CHECKPOINT_ADVANCED: &str = "rtdb.multi/checkpoint-advanced";

fn discovery_from_admitted_record(config: &CellStoreConfig, record: &DeploymentRecord) -> Result<rtdb_replication::BlockDiscovery> {
    use rtdb_replication::{BlockDiscovery, PublishedCheckpoint};
    use rtdb_types::{BlockIndex, ReplicaPosition, StorageEpoch};
    let Some(checkpoint) = record.checkpoint else { return Ok(BlockDiscovery::List); };
    let block = BlockIndex(checkpoint.block);
    let exact = config.snapshot_block.is_some();
    if !exact && let Some(cutoff) = record.compaction_replay_cutoff {
        return Ok(BlockDiscovery::FromRestored { checkpoint: block, compaction_cutoff: ReplicaPosition(cutoff), root_sha256: checkpoint.root_sha256 });
    }
    if exact && record.compaction_replay_cutoff.is_some() && checkpoint.root_sha256.is_none() {
        return Ok(BlockDiscovery::At(block));
    }
    if checkpoint.boundary < 0 { return Err(storage_region_error("The published checkpoint has no replica boundary")); }
    let published = PublishedCheckpoint {
        block, boundary: ReplicaPosition::from_epoch_offset(StorageEpoch(record.log_epoch), checkpoint.boundary)?,
        root_sha256: checkpoint.root_sha256,
    };
    Ok(if exact { BlockDiscovery::AtPublished(published) } else { BlockDiscovery::FromPublished(published) })
}

fn checkpoint_startup_replay(record: &DeploymentRecord) -> Result<Option<i64>> {
    crate::hosted_recovery::checkpoint_decision(record)?;
    let Some(checkpoint) = record.checkpoint.filter(|checkpoint| checkpoint.root_sha256.is_some()) else { return Ok(None); };
    if checkpoint.boundary < 0 { return Err(storage_region_error("The published checkpoint has an invalid replay boundary")); }
    Ok(Some(crate::hosted_recovery::replay_start(record, checkpoint.boundary)?))
}

impl CellStoreFactory {
    async fn admit_region(
        &self,
        config: &CellStoreConfig,
        parent: Arc<IoRegion>,
        uses_replay_archive: bool,
    ) -> Result<(DeploymentRecord, Arc<IoRegion>, Uuid, oneshot::Sender<()>, oneshot::Sender<()>)> {
        let directory = self.directory.clone();
        let keys = self.keys.clone();
        let config = config.clone();
        let hosted_task = self.hosted_task.clone();
        let local = matches!(config.storage, CellStorage::Local { .. });
        let prepared = self.local_witnesses.as_ref().filter(|_| local).and_then(|pool| pool.take(&self.storage_regions));
        let id = prepared.as_ref().map_or_else(Uuid::new_v4, |(id, _)| *id);
        let (release, mut dropped) = oneshot::channel();
        let (metadata, read) = oneshot::channel();
        let (ready, admitted) = oneshot::channel();
        // The worker owns both the admission and its release. Dropping the
        // opening future cannot strand a request that has reached the directory.
        self.storage_regions.spawn(async move {
            let witness = if let Some((_, file)) = prepared {
                Some(file)
            } else if local {
                match keys.hold_local_storage_region(id) {
                    Ok(witness) => witness,
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                }
            } else {
                None
            };
            let witnessed = witness.is_some();
            let mut io = parent.child_holding(witness.map(|file| Arc::new(file) as Arc<dyn Send + Sync>));
            if config.writer_epoch.is_some() || config.compactor {
                if local && !witnessed {
                    let _ = ready.send(Err(storage_region_error("A local serving writer requires its volume witness")));
                    return;
                }
                track_publications(&mut io, directory.clone(), config.deployment.clone(), id, config.writer_epoch);
            }
            let accepted = crate::admin::update_installation(directory.as_ref(), &config.deployment, |record| {
                if !matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
                    || record.storage_prefix() != config.prefix
                    || config.writer_epoch.is_some_and(|epoch| epoch != record.epoch || record.holder.is_none())
                {
                    return Err(Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, rtdb_node::NOT_HELD, format!("Installation {} no longer authorizes this store", config.deployment)));
                }
                if let Some(requested) = config.snapshot_block
                    && !record.checkpoint.is_some_and(|published| published.block == requested)
                {
                    return Err(Anomaly::new(rtdb_types::AnomalyCategory::Conflict, CHECKPOINT_ADVANCED, "The snapshot's checkpoint changed before storage admission"));
                }
                let checkpoint_metadata_from = Some(record.checkpoint.map_or(0, |checkpoint| checkpoint.block));
                let checkpoint_replay_from = if config.snapshot_block.is_none() && record.compaction_replay_cutoff.is_none() {
                    checkpoint_startup_replay(record)?
                } else { None };
                let repair = if uses_replay_archive { crate::archive::serving_archive_operation(record)? } else { None };
                record.storage_regions.insert(id, if local {
                    StorageRegion::Local { archive: None, checkpoint_suffix: None, repair_archive: repair, tracks_ordered_index_pages: true, checkpoint_metadata_from, checkpoint_replay_from }
                } else {
                    StorageRegion::Remote { archive_source: None, tracks_ordered_index_pages: true, checkpoint_metadata_from, checkpoint_replay_from, archive: None, checkpoint_suffix: None, repair_archive: repair, owner: hosted_task.clone(),
                        closed: false, publications: BTreeMap::new() }
                });
                Ok(())
            }).await;
            let opened = accepted.is_ok();
            // The admission predicate and missing installation are local
            // refusals. Other directory errors may hide an applied CAS.
            let uncertain = accepted.as_ref().err().is_some_and(|error| !matches!(
                error.code.as_str(), rtdb_node::NOT_HELD | "rtdb.multi/unknown-installation" | CHECKPOINT_ADVANCED
                    | "rtdb.storage/log-range-unavailable" | "rtdb.multi/storage-region"
            ));
            if ready.send(accepted.map(|record| (record, io.clone()))).is_ok() && opened {
                tokio::select! {
                    _ = &mut dropped => {}
                    _ = read => loop {
                        let result = crate::admin::update_installation(directory.as_ref(), &config.deployment, |record| {
                            if let Some(region) = record.storage_regions.get_mut(&id) {
                                region.release_checkpoint_startup();
                            }
                            Ok(())
                        }).await;
                        if result.is_ok() { let _ = dropped.await; break; }
                        tokio::select! {
                            _ = &mut dropped => break,
                            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                        }
                    }
                }
            }
            io.close_and_wait().await;
            drop(io);
            // A failed registration may already be committed. Compare and
            // replace even if absent to fence that attempt's old version.
            if opened || uncertain {
                loop {
                    match release_region(directory.as_ref(), &config.deployment, id, uncertain).await {
                        Ok(()) => break,
                        Err(error) => eprintln!("Storage region {id} still open in {}: {}", config.deployment, error.code),
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
            if witnessed && let Err(error) = keys.forget_local_storage_region(id) {
                eprintln!("Could not remove storage region witness {id}: {}", error.code);
            }
        });
        let (record, region) = admitted.await.map_err(|_| Anomaly::new(
            rtdb_types::AnomalyCategory::Unavailable,
            "rtdb.multi/storage-admission",
            "The serving storage region could not be admitted",
        ))??;
        Ok((record, region, id, release, metadata))
    }
}

pub(crate) fn remote_storage_region(archive: Option<Uuid>, owner: Option<HostedTask>) -> StorageRegion {
    StorageRegion::Remote { archive_source: None, tracks_ordered_index_pages: true, checkpoint_metadata_from: None, checkpoint_replay_from: None, archive, checkpoint_suffix: None, repair_archive: None, owner, closed: false, publications: BTreeMap::new() }
}

pub(crate) fn track_publications(
    io: &mut Arc<IoRegion>,
    directory: Arc<dyn Directory>,
    deployment: String,
    region: Uuid,
    writer_epoch: Option<u64>,
) {
    Arc::get_mut(io).expect("the region has not been exposed").track_publications(Arc::new(
        DirectoryPublications { directory, deployment, region, writer_epoch, owner: PublicationOwner::Serving },
    ));
}

pub(crate) fn track_transfer_publications(
    io: &mut Arc<IoRegion>,
    directory: Arc<dyn Directory>,
    deployment: String,
    region: Uuid,
    epoch: u64,
) {
    Arc::get_mut(io).expect("the region has not been exposed").track_publications(Arc::new(
        DirectoryPublications { directory, deployment, region, writer_epoch: Some(epoch), owner: PublicationOwner::Transfer },
    ));
}

pub(crate) fn track_checkpoint_stage_publications(
    io: &mut Arc<IoRegion>,
    directory: Arc<dyn Directory>,
    deployment: String,
    region: Uuid,
    operation: Uuid,
    bootstrap: Option<[u8; 32]>,
) {
    Arc::get_mut(io).expect("the region has not been exposed").track_publications(Arc::new(
        DirectoryPublications { directory, deployment, region, writer_epoch: None, owner: PublicationOwner::CheckpointStage { operation, bootstrap } },
    ));
}

pub(crate) fn track_checkpoint_transfer_publications(
    io: &mut Arc<IoRegion>,
    directory: Arc<dyn Directory>,
    deployment: String,
    region: Uuid,
    operation: Uuid,
) {
    Arc::get_mut(io).expect("the region has not been exposed").track_publications(Arc::new(
        DirectoryPublications { directory, deployment, region, writer_epoch: None, owner: PublicationOwner::CheckpointTransfer(operation) },
    ));
}

pub(crate) fn track_hosted_fork_publications(io: &mut Arc<IoRegion>, directory: Arc<dyn Directory>, deployment: String, region: Uuid) {
    Arc::get_mut(io).expect("the region has not been exposed").track_publications(Arc::new(
        DirectoryPublications { directory, deployment, region, writer_epoch: None, owner: PublicationOwner::HostedFork },
    ));
}

enum PublicationOwner {
    Serving,
    Transfer,
    CheckpointStage { operation: Uuid, bootstrap: Option<[u8; 32]> },
    CheckpointTransfer(Uuid),
    HostedFork,
}

struct DirectoryPublications {
    directory: Arc<dyn Directory>,
    deployment: String,
    region: Uuid,
    writer_epoch: Option<u64>,
    owner: PublicationOwner,
}

async fn publication_members(key: &ObjectKey, body: &[u8]) -> rtdb_storage::Result<Vec<(String, rtdb_types::ServingPublication)>> {
    use rtdb_types::ServingPublication;

    if !crate::packing::is_pack_key(key.as_str())
        && !key.as_str().contains(&format!("/{}/", crate::packing::PACK_DIRECTORY))
    {
        return Ok(ServingPublication::from_prefix(body).map(|unit| (key.to_string(), unit)).into_iter().collect());
    }
    let index = crate::packing::read_pack_index_with(key, body.len() as u64, 16 << 20, |range| {
        let bytes = body.get(range.start as usize..range.end as usize)
            .ok_or_else(|| Error::ConflictingObject(key.to_string())).map(Bytes::copy_from_slice);
        std::future::ready(bytes)
    }).await?;
    Ok(index.entries.into_iter().filter_map(|(name, extent)| {
        let start = usize::try_from(extent.offset).ok()?;
        let end = start.checked_add(ServingPublication::BYTES)?;
        if extent.length < ServingPublication::BYTES as u64 { return None; }
        let unit = ServingPublication::from_prefix(body.get(start..end)?)?;
        Some((name, unit))
    }).collect())
}

impl DirectoryPublications {
    async fn attach_pack(&self, pack: &ObjectKey, root: &str, body: &[u8]) -> rtdb_storage::Result<Option<Uuid>> {
        let members = publication_members(pack, body).await?;
        let fresh = Uuid::new_v4();
        for _ in 0..8 {
            let mut record = self.directory.deployment(&self.deployment).await
                .map_err(|error| publication_error(pack, error))?
                .ok_or_else(|| Error::Closed("The pack residence is absent".into()))?;
            let origin = UnitScope::publication_origin(&record.storage_prefix());
            require_publication_key(&record, None, pack).map_err(|error| publication_error(pack, error))?;
            let Some(StorageRegion::Remote { archive: None, closed: false, publications, .. }) =
                record.storage_regions.get_mut(&self.region)
            else { return Err(Error::Closed("The pack publication region is closed".into())); };
            let mut attached = None;
            let mut participants = Vec::new();
            for (name, unit) in &members {
                if unit.origin != origin { continue; }
                let Some(member) = publications.get(&unit.attempt) else { continue; };
                if member.unit.as_ref().is_none_or(|(logical, expected)| logical != name || expected != unit)
                    || member.key != format!("{root}/{name}") || !member.upload_id.is_empty() || member.rejected
                {
                    return Err(Error::Closed("A pack member has a different admitted attempt".into()));
                }
                if let Some(chosen) = member.container {
                    if attached.is_some_and(|prior| prior != chosen) {
                        return Err(Error::Closed("Pack members already belong to different uploads".into()));
                    }
                    attached = Some(chosen);
                }
                participants.push(unit.attempt);
            }
            if participants.is_empty() { return Ok(None); }
            if let Some(attempt) = attached {
                if publications.get(&attempt).is_none_or(|record| record.key != pack.as_str() || record.rejected)
                    || participants.iter().any(|id| publications[id].container != Some(attempt))
                    || publications.iter().any(|(id, member)| member.unit.is_some()
                        && member.container == Some(attempt) && !participants.contains(id))
                {
                    return Err(Error::Closed("A previous attempt already owns this pack member set".into()));
                }
                return Ok(Some(attempt));
            }
            for id in participants {
                publications.get_mut(&id).expect("member was observed").container = Some(fresh);
            }
            publications.insert(fresh, StoragePublication {
                key: pack.to_string(), upload_id: String::new(), unit: None,
                rejected: false, container: Some(fresh),
            });
            let version = record.version;
            if let Cas::Applied(_) = self.directory.replace_deployment(record, version).await
                .map_err(|error| publication_error(pack, error))?
            {
                return Ok(Some(fresh));
            }
        }
        Err(publication_error(pack, storage_region_error("The pack attempt inventory kept changing")))
    }
}

#[async_trait]
impl PublicationJournal for DirectoryPublications {
    async fn allocate_unit(&self, key: &ObjectKey, logical: &ObjectKey, unit: rtdb_types::ServingPublication) -> rtdb_storage::Result<()> {
        if !matches!(self.owner, PublicationOwner::Serving) { return Ok(()); }
        crate::admin::update_installation(self.directory.as_ref(), &self.deployment, |record| {
            if unit.origin != UnitScope::publication_origin(&record.storage_prefix())
                || self.writer_epoch.is_some_and(|epoch| record.epoch != epoch)
                || !matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
            { return Err(storage_region_error("The serving attempt belongs to a closed residence")); }
            require_publication_key(record, None, key)?;
            if matches!(record.storage_regions.get(&self.region), Some(StorageRegion::Local { archive: None, .. })) {
                let selected = (logical.to_string(), unit);
                let attempts = record.local_publications.entry(self.region).or_default();
                match attempts.entry(unit.attempt) {
                    std::collections::btree_map::Entry::Vacant(entry) => { entry.insert(selected); }
                    std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &selected => (),
                    _ => return Err(storage_region_error("The local slot belongs to another serving attempt")),
                }
                return Ok(());
            }
            let Some(StorageRegion::Remote { archive: None, closed: false, publications, .. }) = record.storage_regions.get_mut(&self.region)
                else { return Err(storage_region_error("The serving attempt has no open storage region")); };
            let publication = StoragePublication { key: key.to_string(), upload_id: String::new(), unit: Some((logical.to_string(), unit)), rejected: false, container: None };
            match publications.entry(unit.attempt) {
                std::collections::btree_map::Entry::Vacant(entry) => { entry.insert(publication); },
                std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &publication => (),
                _ => return Err(storage_region_error("The serving attempt already has a different publication")),
            }
            Ok(())
        }).await.map(drop).map_err(|error| publication_error(key, error))
    }

    async fn unit_attempt(&self, key: &ObjectKey, body: &[u8]) -> rtdb_storage::Result<Option<Uuid>> {
        if !matches!(self.owner, PublicationOwner::Serving) { return Ok(None); }
        if let Some((root, _)) = key.as_str().rsplit_once(&format!("/{}/", crate::packing::PACK_DIRECTORY)) {
            return self.attach_pack(key, root, body).await;
        }
        let Some(unit) = rtdb_types::ServingPublication::from_prefix(body).filter(|unit| *unit != rtdb_types::ServingPublication::ARCHIVE)
            else { return Ok(None); };
        let Some(record) = self.directory.deployment(&self.deployment).await.map_err(|error| publication_error(key, error))?
            else { return Err(Error::Closed("The serving attempt's residence is absent".into())); };
        if unit.origin != UnitScope::publication_origin(&record.storage_prefix()) { return Ok(None); }
        let Some(StorageRegion::Remote { publications, .. }) = record.storage_regions.get(&self.region)
            else { return Err(Error::Closed("The serving attempt's publication region is absent".into())); };
        match publications.get(&unit.attempt) {
            Some(record) if record.key == key.as_str() && record.container.is_none()
                && !record.rejected && record.unit.as_ref().is_some_and(|(_, admitted)| *admitted == unit) =>
                Ok(Some(unit.attempt)),
            // Raw relocations carry the slot of an already selected publication.
            None => Ok(None),
            Some(_) => Err(Error::Closed("The serving body identifies a different attempt".into())),
        }
    }

    async fn active_unit_upload(&self, key: &ObjectKey, attempt: Uuid) -> rtdb_storage::Result<Option<String>> {
        let record = self.directory.deployment(&self.deployment).await.map_err(|error| publication_error(key, error))?
            .ok_or_else(|| Error::Closed("The serving attempt's residence is absent".into()))?;
        let Some(StorageRegion::Remote { publications, closed: false, .. }) = record.storage_regions.get(&self.region)
            else { return Err(Error::Closed("The serving attempt's region is closed".into())); };
        let publication = publications.get(&attempt).filter(|publication| publication.key == key.as_str() && !publication.rejected)
            .ok_or_else(|| Error::Closed("The serving attempt is unavailable".into()))?;
        Ok((!publication.upload_id.is_empty()).then(|| publication.upload_id.clone()))
    }

    async fn unit_outcome(&self, key: &ObjectKey, attempt: Uuid, upload: &str, published: bool) -> rtdb_storage::Result<()> {
        crate::admin::update_installation(self.directory.as_ref(), &self.deployment, |record| {
            let Some(StorageRegion::Remote { publications, .. }) = record.storage_regions.get_mut(&self.region) else { return Ok(()); };
            if let Some(publication) = publications.get_mut(&attempt) {
                if publication.key != key.as_str() || publication.upload_id != upload
                    || (publication.unit.is_none() && publication.container != Some(attempt))
                {
                    return Err(storage_region_error("The publication receipt identifies another serving attempt"));
                }
                if published {
                    publications.retain(|id, retained| *id != attempt && retained.container != Some(attempt));
                } else {
                    publication.rejected = true;
                }
            }
            Ok(())
        }).await.map(drop).map_err(|error| publication_error(key, error))
    }

    async fn admit(&self, key: &ObjectKey, attempt: Uuid, upload: &str) -> rtdb_storage::Result<()> {
        if attempt.is_nil() || upload.is_empty() {
            return Err(publication_error(key, storage_region_error("The provider upload has no identity")));
        }
        let publication = StoragePublication { key: key.to_string(), upload_id: upload.to_owned(), unit: None, rejected: false, container: None };
        crate::admin::update_installation(self.directory.as_ref(), &self.deployment, |record| {
            let accepts_writes = match &self.owner {
                PublicationOwner::Serving => matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined),
                PublicationOwner::Transfer => record.lifecycle == Lifecycle::Moving && record.move_from.is_some()
                    && record.storage_generation.is_some_and(|generation| !record.abandoned_generations.contains_key(&generation)),
                PublicationOwner::CheckpointStage { operation, bootstrap } => record.lifecycle == Lifecycle::Restoring
                    && record.checkpoint_stage.as_deref().is_some_and(|stage| {
                        if stage.intent.operation != *operation { return false; }
                        match bootstrap {
                            None => stage.staged_manifest_sha256.is_none()
                                && ObjectKey::new(crate::archive::stage::staged_storage_root(record))
                                    .is_ok_and(|root| key.is_under(&root)),
                            Some(digest) => record.checkpoint_transfer_replica.as_deref().is_some_and(|replica| {
                                replica.bootstrap.ciphertext_sha256 == *digest
                                    && &replica.bootstrap.terminal.fence.destination == stage
                                    && crate::archive::checkpoint_storage_root().is_ok_and(|version| {
                                        ObjectKey::new(format!("{}/{version}", record.storage_prefix()))
                                            .is_ok_and(|root| key.is_under(&root))
                                    })
                            }),
                        }
                    }),
                PublicationOwner::CheckpointTransfer(operation) => record.lifecycle == Lifecycle::Restoring
                    && record.checkpoint_transfer_fence.as_deref().is_some_and(|fence| {
                        let intent = &fence.destination.intent;
                        intent.operation == *operation && intent.destination.to_string() == record.id
                            && record.checkpoint_stage.as_deref() == Some(&fence.destination)
                            && crate::archive::admits_checkpoint_transfer_object(record, &fence.destination, key.as_str())
                    }),
                PublicationOwner::HostedFork => record.lifecycle == Lifecycle::Restoring
                    && record.fork_stage.as_ref().is_some_and(|stage| stage.preparation.token == self.region),
            };
            if !accepts_writes || self.writer_epoch.is_some_and(|epoch| record.epoch != epoch)
            {
                return Err(storage_region_error("The installation no longer admits provider publications"));
            }
            let Some(StorageRegion::Remote { archive, closed: false, .. }) = record.storage_regions.get(&self.region) else {
                return Err(storage_region_error("The remote storage region is closed or absent"));
            };
            if let Some(capture) = archive {
                crate::archive::require_capture_open(record, *capture)?;
            }
            require_publication_key(record, *archive, key)?;
            if record.storage_regions.values().any(|region| region.retires_reference(key.as_str())) {
                return Err(storage_region_error("The original publication is fenced for retirement"));
            }
            if matches!(self.owner, PublicationOwner::Serving) && archive.is_none() {
                ordered_index_storage::require_page_publication(record, self.region, key)?;
            }
            let Some(StorageRegion::Remote { publications, .. }) = record.storage_regions.get_mut(&self.region) else {
                unreachable!("the publication region was just checked");
            };
            match publications.entry(attempt) {
                std::collections::btree_map::Entry::Vacant(entry) => { entry.insert(publication.clone()); }
                std::collections::btree_map::Entry::Occupied(mut entry) if (entry.get().unit.is_some() || entry.get().container == Some(attempt))
                    && !entry.get().rejected && entry.get().key == key.as_str()
                    && (entry.get().upload_id.is_empty() || entry.get().upload_id == upload) => {
                    entry.get_mut().upload_id = upload.to_owned();
                }
                std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &publication => {}
                _ => return Err(storage_region_error("The publication identifies another object")),
            }
            Ok(())
        }).await.map_err(|error| publication_error(key, error))?;
        Ok(())
    }

    async fn settle(&self, key: &ObjectKey, attempt: Uuid, upload: &str, submitted: bool) -> rtdb_storage::Result<()> {
        for _ in 0..8 {
            let Some(mut record) = self.directory.deployment(&self.deployment).await
                .map_err(|error| publication_error(key, error))? else { return Ok(()); };
            let Some(StorageRegion::Remote { publications, .. }) = record.storage_regions.get_mut(&self.region)
            else { return Ok(()); };
            match publications.get(&attempt) {
                Some(retained) if !submitted && retained.key == key.as_str()
                    && (retained.unit.is_some() || retained.container == Some(attempt)) => return Ok(()),
                Some(retained) if retained.key != key.as_str() || retained.upload_id != upload => {
                    return Err(publication_error(key, storage_region_error("The publication identifies another object")));
                }
                None if submitted => return Ok(()),
                _ => {}
            }
            publications.remove(&attempt);
            // A cancellation can race a committed admission whose reply was
            // lost. Even when absent, bump this record's version so that
            // admission cannot land after we confirm it was never sent.
            let version = record.version;
            if let Cas::Applied(_) = self.directory.replace_deployment(record, version).await
                .map_err(|error| publication_error(key, error))?
            {
                return Ok(());
            }
        }
        Err(publication_error(key, storage_region_error("The publication inventory kept changing")))
    }
}

fn publication_error(key: &ObjectKey, error: Anomaly) -> Error {
    Error::Remote { key: key.to_string(), message: error.code.into() }
}

pub(crate) async fn release_region(directory: &dyn Directory, deployment: &str, region: Uuid, uncertain: bool) -> Result<()> {
    for _ in 0..8 {
        let Some(mut record) = directory.deployment(deployment).await? else { return Ok(()); };
        let readers = record.catalog_readers.len();
        record.catalog_readers.retain(|_, reader| reader.owner != region);
        let removed_readers = record.catalog_readers.len() != readers;
        match record.storage_regions.get_mut(&region) {
            Some(StorageRegion::Remote { closed, .. } | StorageRegion::ReplicaPublication { closed, .. }) if *closed => {
                if !removed_readers { return Ok(()); }
            }
            Some(StorageRegion::ReplicaPublication { closed, draft: Some(_), .. }) => *closed = true,
            Some(StorageRegion::Remote { closed, publications, .. } | StorageRegion::ReplicaPublication { closed, publications, .. }) if !publications.is_empty() => *closed = true,
            None if !uncertain && !removed_readers => return Ok(()),
            _ => { record.storage_regions.remove(&region); }
        }
        let version = record.version;
        if let Cas::Applied(_) = directory.replace_deployment(record, version).await? {
            return Ok(());
        }
    }
    Err(Anomaly::new(
        rtdb_types::AnomalyCategory::Conflict,
        "rtdb.multi/storage-release-conflict",
        "The installation changed while closing a storage region",
    ))
}

/// A local lock or ECS stop record can witness process death. Before
/// releasing a remote admission, retire its exact provider upload IDs.
pub(crate) async fn reconcile_storage_regions(
    directory: &dyn Directory,
    storage: &CellStorage,
    keys: &dyn KeyService,
    record: &DeploymentRecord,
) -> Result<()> {
    if let Some(stage) = &record.checkpoint_stage {
        if crate::admin::cell_media(storage, keys)? != stage.media {
            return Err(storage_region_error("Checkpoint staging uses different provider custody"));
        }
    }
    for (&region, state) in &record.storage_regions {
        match state {
            StorageRegion::Local { .. }
            | StorageRegion::ReplicaReplay { custody: ReplayCustody::Local, .. }
            | StorageRegion::ReferenceRead { custody: ReplayCustody::Local, .. }
                if (matches!(storage, CellStorage::Local { .. })
                    || matches!(state, StorageRegion::Local { archive: Some(_), .. } | StorageRegion::Local { repair_archive: Some(_), .. }))
                    && keys.local_storage_region_drained(region)? => {
                release_region(directory, &record.id, region, false).await?;
                keys.forget_local_storage_region(region)?;
            }
            StorageRegion::ReplicaReplay { custody: ReplayCustody::Hosted { owner: Some(owner) }, .. }
            | StorageRegion::ReferenceRead { custody: ReplayCustody::Hosted { owner: Some(owner) }, .. }
                if directory.hosted_task_stopped(owner).await? => {
                release_region(directory, &record.id, region, false).await?;
            }
            StorageRegion::ExactRetirement { .. } | StorageRegion::ReplayPackRetirement { .. }
                if directory.lineage_state(&record.lineage).await?.retirement().is_none_or(|retirement| retirement.token != region) => {
                release_region(directory, &record.id, region, false).await?;
            }
            StorageRegion::Remote { owner, closed, .. } => {
                let fenced = if *closed {
                    record.clone()
                } else {
                    let Some(owner) = owner else { continue; };
                    if !directory.hosted_task_stopped(owner).await? { continue; }
                    crate::admin::update_installation(directory, &record.id, |current| {
                        if let Some(StorageRegion::Remote { owner: current_owner, closed, .. }) = current.storage_regions.get_mut(&region)
                            && current_owner.as_ref() == Some(owner)
                        {
                            *closed = true;
                        }
                        Ok(())
                    }).await?
                };
                let Some(state @ StorageRegion::Remote { archive, publications, closed: true, .. }) = fenced.storage_regions.get(&region)
                    else { continue; };
                if !publications.is_empty() {
                    abort_remote_publications(storage, keys, &fenced, *archive, publications).await?;
                }
                crate::admin::update_installation(directory, &record.id, |current| {
                    if current.storage_regions.get(&region) == Some(state) {
                        current.storage_regions.remove(&region);
                        current.catalog_readers.retain(|_, reader| reader.owner != region);
                    }
                    Ok(())
                }).await?;
            }
            _ => {}
        }
    }
    Ok(())
}

async fn abort_remote_publications(
    serving: &CellStorage,
    keys: &dyn KeyService,
    record: &DeploymentRecord,
    operation: Option<Uuid>,
    publications: &BTreeMap<Uuid, StoragePublication>,
) -> Result<()> {
    let unrecognized = || storage_region_error("A remote region has no matching storage owner");
    let archive = operation.map(|operation| -> Result<CellStorage> {
        let registration = record.checkpoint_captures.get(&operation).ok_or_else(unrecognized)?;
        serde_json::from_str(&registration.archive).map_err(|_| unrecognized())
    }).transpose()?;
    let CellStorage::S3 { config } = archive.as_ref().unwrap_or(serving) else {
        return Err(unrecognized());
    };
    if record.fork_stage.is_some() {
        let bucket = serving.bucket(IoRegion::new()).await?;
        let namespace = crate::deployment::existing_storage_namespace(bucket.as_ref()).await?;
        if namespace.is_none() || namespace != record.storage_namespaces.map(|binding| binding.objects) {
            return Err(storage_region_error("Hosted fork staging uses different provider custody"));
        }
    }
    let inventory = CellStorage::s3_inventory(config).await?;
    for (&attempt, publication) in publications {
        let key = ObjectKey::new(&publication.key)?;
        if attempt.is_nil() { return Err(unrecognized()); }
        require_publication_key(record, operation, &key)?;

        if let Some(container) = publication.container.filter(|container| *container != attempt) {
            if publication.unit.is_none() || !publication.upload_id.is_empty()
                || publications.get(&container).is_none_or(|pack| pack.unit.is_some() || pack.container != Some(container))
            {
                return Err(unrecognized());
            }
            continue;
        }
        if !publication.upload_id.is_empty() {
            inventory.abort_publication(&key, &publication.upload_id).await?;
        }
        let members = if publication.container == Some(attempt) {
            let Some((root, _)) = key.as_str().rsplit_once(&format!("/{}/", crate::packing::PACK_DIRECTORY))
                else { return Err(unrecognized()); };
            let mut members = Vec::new();
            for (&id, member) in publications.iter().filter(|(id, member)| **id != attempt && member.container == Some(attempt)) {
                let (logical, unit) = member.unit.as_ref().ok_or_else(unrecognized)?;
                if member.key != format!("{root}/{logical}") || !member.upload_id.is_empty() || unit.attempt != id {
                    return Err(unrecognized());
                }
                members.push((ObjectKey::new(logical)?, *unit));
            }
            if members.is_empty() || publication.unit.is_some() { return Err(unrecognized()); }
            members
        } else if let Some((logical, unit)) = &publication.unit {
            let logical = ObjectKey::new(logical)?;
            if unit.attempt != attempt || !key.as_str().ends_with(&format!("/{logical}")) {
                return Err(unrecognized());
            }
            vec![(logical, *unit)]
        } else {
            if publication.upload_id.is_empty() { return Err(unrecognized()); }
            continue;
        };

        let namespace = record.storage_namespaces.ok_or_else(unrecognized)?;
        let origin = UnitScope::publication_origin(&record.storage_prefix());
        if operation.is_some() || members.iter().any(|(_, unit)| unit.origin != origin)
            || keys.namespace_id().await? != namespace.keys
        {
            return Err(unrecognized());
        }

        let io = IoRegion::new();
        if publication.upload_id.is_empty() || publication.rejected {
            for (logical, unit) in members {
                let slot = UnitScope::directory_publication_slot(&record.org, &record.lineage, unit, &logical);
                keys.withdraw_unit_key(slot, &io).await?;
            }
        } else {
            let bucket = serving.bucket(Arc::clone(&io)).await?;
            let sealing = directory_publications(keys, record, io)?;
            let pack = if publication.container.is_some() {
                match replay_retirement::ObservedPack::raw(bucket.as_ref(), &key).await {
                    Ok(pack) => Some(pack),
                    Err(Error::MissingObject(_)) => return Err(orphan_outcome_pending()),
                    Err(error) => return Err(error.into()),
                }
            } else {
                None
            };
            let header_bytes = (rtdb_types::ServingPublication::BYTES + crate::cipher::object::SEALED_HEADER_BYTES) as u64;
            for (logical, unit) in members {
                let header = if let Some(pack) = &pack {
                    let entry = pack.index.entries.get(logical.as_str()).filter(|entry| entry.length >= header_bytes)
                        .ok_or_else(orphan_outcome_pending)?;
                    pack.read_range(entry.offset..entry.offset + header_bytes).await?
                } else {
                    match bucket.observe_range(&key, 0..header_bytes).await {
                        Ok(header) => header.value,
                        Err(Error::MissingObject(_)) => return Err(orphan_outcome_pending()),
                        Err(error) => return Err(error.into()),
                    }
                };
                if sealing.header(&logical, header).await?.1 != unit {
                    return Err(orphan_outcome_pending());
                }
            }
        }
    }
    Ok(())
}

fn orphan_outcome_pending() -> Anomaly {
    storage_region_error("The fenced serving attempt has an unresolved publication outcome")
}

fn require_publication_key(record: &DeploymentRecord, operation: Option<Uuid>, key: &ObjectKey) -> Result<()> {
    let allowed = if let Some(operation) = operation {
        let prefix = ObjectKey::new(format!("checkpoint-captures/{}/{operation}", record.id))?;
        let witness = format!("checkpoint-authorities/{}/{operation}.json", record.id);
        record.checkpoint_captures.contains_key(&operation)
            && (key.is_under(&prefix) || key.as_str() == witness)
    } else {
        key.is_under(&ObjectKey::new(record.storage_prefix())?)
    };
    if allowed { return Ok(()); }
    Err(storage_region_error("The publication key is outside its storage scope"))
}

fn storage_region_error(message: &str) -> Anomaly {
    Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.multi/storage-region", message)
}

#[derive(Clone, Copy)]
enum Fence {
    Writer { epoch: u64 },
    /// May write a compaction output, whose name its job fixes and whose effect waits on the holder's adoption.
    Compactor,
    Reader,
}

impl From<&CellStoreConfig> for Fence {
    fn from(config: &CellStoreConfig) -> Self {
        match (config.writer_epoch, config.compactor) {
            (Some(epoch), _) => Self::Writer { epoch },
            (None, true) => Self::Compactor,
            (None, false) => Self::Reader,
        }
    }
}

/// Whether `key` is a compaction output: a trie data or metadata file above level 0.
pub fn compaction_output(key: &str) -> bool {
    key.starts_with("tables/") && (key.contains("/data/l") || key.contains("/meta/l")) && !key.contains("/data/l00-") && !key.contains("/meta/l00-")
}

/// Milliseconds since the epoch, as a store stamps the releases and packs it writes.
pub type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

/// `time`, made strictly increasing: a fork at a past block keeps the releases stamped before the block's pack, so a release and a pack must never share a stamp.
pub fn stamps(time: impl Fn() -> i64 + Send + Sync + 'static) -> Clock {
    let last = std::sync::atomic::AtomicI64::new(i64::MIN);
    Arc::new(move || {
        let now = time();
        let previous = last.fetch_update(std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst, |previous| Some(previous.saturating_add(1).max(now))).expect("the update always applies");
        previous.saturating_add(1).max(now)
    })
}

/// The wall clock's stamps, one sequence for the whole process.
/// Stamps run ahead of the wall clock through a burst of releases, which only defers their reaping; the reaper reads its time from them too, so that in one process nothing is stamped after the time it reaps as of.
pub fn wall_clock() -> Clock {
    static WALL: std::sync::OnceLock<Clock> = std::sync::OnceLock::new();
    Arc::clone(WALL.get_or_init(|| stamps(now_ms)))
}

/// One store of a deployment in the cell's `bucket` (a prefix and a storage version), as a node opens it below its disk cache: its own objects and those its manifest references, packed, with deletes as releases.
pub async fn open_deployment_store(bucket: crate::reference::Bucket, store: &str, discovery: crate::packing::PackDiscovery, clock: Clock) -> Result<Arc<GarbageStore>> {
    open_deployment_store_with_archive(bucket, store, discovery, clock, None).await
}

pub(crate) async fn open_deployment_store_with_archive(
    bucket: crate::reference::Bucket,
    store: &str,
    discovery: crate::packing::PackDiscovery,
    clock: Clock,
    archive: Option<crate::archive::ArchivedCheckpoint>,
) -> Result<Arc<GarbageStore>> {
    let local: Arc<dyn ObjectStore> = Arc::new(crate::reference::PrefixedStore::new(Arc::clone(&bucket), store.to_owned()));
    let base = crate::reference::Base::load(bucket.as_ref(), store).await?;
    open_with_base(bucket, local, base, discovery, clock, archive, None).await
}

async fn open_cell_store(
    bucket: crate::reference::Bucket,
    store: &str,
    local: Arc<dyn ObjectStore>,
    record: &DeploymentRecord,
    clock: Clock,
    recovery: Option<crate::archive::ArchivedCheckpoint>,
    custody: ReferenceReads,
) -> Result<(Arc<GarbageStore>, bool)> {
    let discovery = record.checkpoint.map_or(crate::packing::PackDiscovery::List, |checkpoint| {
        crate::packing::PackDiscovery::From(checkpoint.block)
    });
    let base = if record.parent.is_none() || record.storage_generation.is_some() {
        crate::reference::Base::default()
    } else {
        crate::reference::Base::load(bucket.as_ref(), store).await?
    };
    // A legacy reference opens its own custody record at read time. A serving archive has
    // already been retained by this attachment and only needs its dedicated object transport.
    let audience_reads = base.objects.is_empty() && recovery.as_ref().is_none_or(|archive| archive.supports_audience_reads());
    let store = open_with_base(bucket, local, base, discovery, clock, recovery, Some(custody)).await?;
    Ok((store, audience_reads))
}

async fn open_with_base(
    bucket: crate::reference::Bucket,
    local: Arc<dyn ObjectStore>,
    base: crate::reference::Base,
    discovery: crate::packing::PackDiscovery,
    clock: Clock,
    archive: Option<crate::archive::ArchivedCheckpoint>,
    custody: Option<ReferenceReads>,
) -> Result<Arc<GarbageStore>> {
    let own: Arc<dyn ObjectStore> = if base.objects.is_empty() {
        local
    } else {
        let references = crate::reference::ReferencingStore::new(local, Arc::clone(&bucket), base);
        Arc::new(match custody {
            Some(custody) => references.with_custody(custody),
            None => references,
        })
    };
    let packed = crate::packing::PackingStore::open_with_clock(own, discovery, Arc::clone(&clock)).await?;
    let store: Arc<dyn ObjectStore> = match archive {
        Some(archive) => archive.with_serving(packed.clone()),
        None => packed.clone(),
    };
    let mut garbage = GarbageStore::new(store, clock, Some(packed));
    garbage.release_inventory = Some(bucket);
    Ok(Arc::new(garbage))
}

struct FencedStore {
    inner: Arc<dyn ObjectStore>,
    directory: Arc<dyn Directory>,
    deployment: String,
    prefix: String,
    fence: Fence,
    local_publications: Option<Uuid>,
    _release: Option<oneshot::Sender<()>>,
}

impl FencedStore {
    async fn check(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
        let read_only = || Error::ReadOnlyLog(PathBuf::from(key.as_str()));
        if matches!(self.fence, Fence::Reader)
            || matches!(self.fence, Fence::Compactor) && !compaction_output(key.as_str())
        {
            return Err(read_only());
        }
        let current = self.directory.deployment(&self.deployment).await
            .map_err(|error| Error::Remote { key: key.to_string(), message: error.code.into() })?
            .ok_or_else(|| Error::Remote { key: key.to_string(), message: "Deployment authority is missing".into() })?;
        if current.storage_prefix() != self.prefix
            || self.local_publications.is_some_and(|region| {
                !matches!(current.storage_regions.get(&region), Some(StorageRegion::Local { archive: None, .. }))
            })
        { return Err(read_only()); }
        match self.fence {
            Fence::Writer { epoch } if current.epoch == epoch && current.holder.is_some()
                && matches!(current.lifecycle, Lifecycle::Live | Lifecycle::Quarantined) => Ok(()),
            Fence::Writer { epoch } => Err(Error::Deposed { epoch, promised: current.epoch }),
            Fence::Compactor if current.lifecycle == Lifecycle::Live => Ok(()),
            Fence::Reader | Fence::Compactor => Err(read_only()),
        }
    }
    async fn put_bytes(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectPublication>> {
        self.check(key).await?;
        let Some(region) = &self.local_publications else {
            return self.inner.put_with_publication(key, bytes).await;
        };
        let members = publication_members(key, &bytes).await?;
        let origin = UnitScope::publication_origin(&self.prefix);
        let selected = self.inner.put_with_publication(key, bytes).await?;
        if !members.iter().any(|(_, unit)| unit.origin == origin) { return Ok(selected); }
        crate::admin::update_installation(self.directory.as_ref(), &self.deployment, |record| {
            if record.storage_prefix() != self.prefix
                || !matches!(record.storage_regions.get(region), Some(StorageRegion::Local { archive: None, .. }))
            {
                return Err(storage_region_error("The local publication has lost its region"));
            }
            if let Some(attempts) = record.local_publications.get_mut(region) {
                for (logical, unit) in &members {
                    if attempts.get(&unit.attempt).is_some_and(|(name, registered)| name == logical && registered == unit) {
                        attempts.remove(&unit.attempt);
                    }
                }
                if attempts.is_empty() { record.local_publications.remove(region); }
            }
            Ok(())
        }).await.map_err(|error| publication_error(key, error))?;
        Ok(selected)
    }

}

#[async_trait]
impl ObjectStore for FencedStore {
    fn permits_shared_read_cache(&self) -> bool { self.inner.permits_shared_read_cache() }
    fn supports_scan_ranges(&self, key: &ObjectKey) -> bool { self.inner.supports_scan_ranges(key) }
    fn supports_observed_scan_ranges(&self, key: &ObjectKey) -> bool { self.inner.supports_observed_scan_ranges(key) }
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head(key).await }
    async fn head_fresh(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head_fresh(key).await }
    async fn observe_head(&self, key: &ObjectKey) -> rtdb_storage::Result<rtdb_storage::ObjectObservation<StoredObject>> {
        self.inner.observe_head(key).await
    }
    async fn observe_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<rtdb_storage::ObjectObservation<Bytes>> {
        self.inner.observe_range(key, range).await
    }
    async fn retirement_version(&self, key: &ObjectKey) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectVersion>> {
        self.inner.retirement_version(key).await
    }
    async fn retirement_publication(&self, key: &ObjectKey) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectPublication>> {
        self.inner.retirement_publication(key).await
    }
    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.inner.get(key).await }
    async fn get_fresh(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.inner.get_fresh(key).await }
    async fn get_fresh_exact(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<Bytes> { self.inner.get_fresh_exact(key, length).await }
    async fn observe_fresh_exact(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
        self.inner.observe_fresh_exact(key, length).await
    }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> { self.inner.get_range(key, range).await }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> { self.put_bytes(key, bytes).await.map(drop) }
    async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> { self.check(key).await?; self.inner.put_file(key, source).await }
    async fn put_with_publication(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectPublication>> {
        self.put_bytes(key, bytes).await
    }
    async fn put_file_with_publication(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectPublication>> {
        self.check(key).await?;
        self.inner.put_file_with_publication(key, source).await
    }
    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> { self.inner.list(directory).await }
    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
        Err(Error::ReadOnlyLog(PathBuf::from(key.as_str())))
    }
}

/// The scheduled reader only follows exact keys in its pinned catalog.
struct AudienceObjectStore(EncryptingStore);

fn audience_read_only() -> Error { Error::Closed("The audience object view requires a reserved read".into()) }

#[async_trait]
impl ObjectStore for AudienceObjectStore {
    fn index_page_lifecycle(&self) -> Option<&dyn IndexPageLifecycle> {
        self.0.index_page_lifecycle()
    }
    async fn retain_catalog(&self, cut: Option<rtdb_types::CatalogCut>) -> Result<Option<Arc<dyn Send + Sync>>> {
        self.0.retain_catalog(cut).await
    }
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        IoRegion::audience_object_read(self.0.head(key)).await
    }
    async fn head_fresh(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        IoRegion::audience_object_read(self.0.head_fresh(key)).await
    }
    async fn observe_head(&self, key: &ObjectKey) -> rtdb_storage::Result<ObjectObservation<StoredObject>> {
        IoRegion::audience_object_read(self.0.observe_head(key)).await
    }
    async fn observe_heads(&self, keys: &[ObjectKey]) -> rtdb_storage::Result<BTreeMap<ObjectKey, ObjectObservation<StoredObject>>> {
        IoRegion::audience_object_read(self.0.observe_heads(keys)).await
    }
    async fn get(&self, _: &ObjectKey) -> rtdb_storage::Result<Bytes> { Err(audience_read_only()) }
    async fn get_range(&self, _: &ObjectKey, _: Range<u64>) -> rtdb_storage::Result<Bytes> { Err(audience_read_only()) }
    fn whole_object_allocation(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<ObjectReadAllocation> {
        let overflow = || Error::Task("Audience object allocation size overflow".into());
        let sealed = object::sealed_size(key, length)?;
        // A held archive can hold the original ciphertext, the decrypted body and its
        // resealed serving copy at once. Budget all three until provider I/O drains.
        let scratch_bytes = sealed.checked_mul(3).ok_or_else(overflow)?;
        Ok(ObjectReadAllocation { retained_bytes: sealed, scratch_bytes })
    }
    async fn get_reserved_object(&self, key: &ObjectKey, reservation: ReadReservation) -> rtdb_storage::Result<RetainedBytes> {
        let length = reservation.length();
        if !reservation.covers(&self.whole_object_allocation(key, length)?) {
            return Err(audience_read_only());
        }
        let read = IoRegion::audience_object_read(self.0.observe_fresh_exact(key, length));
        let observed = reservation.retaining_io(read).await?;
        reservation.finish(observed.value)
    }
    fn scan_range_allocation(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<ObjectReadAllocation> {
        self.0.scan_range_allocation(key, length)
    }
    async fn get_reserved_scan_range(&self, key: &ObjectKey, range: Range<u64>, reservation: ReadReservation) -> rtdb_storage::Result<RetainedBytes> {
        if !self.supports_scan_ranges(key) { return Err(audience_read_only()); }
        IoRegion::audience_object_read(self.0.get_reserved_scan_range(key, range, reservation)).await
    }
    fn supports_scan_ranges(&self, key: &ObjectKey) -> bool { self.0.supports_scan_ranges(key) }
    async fn put(&self, _: &ObjectKey, _: Bytes) -> rtdb_storage::Result<()> { Err(audience_read_only()) }
    async fn put_file(&self, _: &ObjectKey, _: &Path) -> rtdb_storage::Result<()> { Err(audience_read_only()) }
    async fn list(&self, _: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> { Err(audience_read_only()) }
    async fn delete(&self, _: &ObjectKey) -> rtdb_storage::Result<()> { Err(audience_read_only()) }
}

/// A garbage record: an object released by its deployment, which the reaper may erase once `released_at_ms` is older than its grace period.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GarbageRecord {
    pub key: String,
    pub released_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    trie_floor: Option<CatalogAdmissionFloor>,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum StoredRelease {
    Exact {
        record: GarbageRecord,
        version: PhysicalObjectVersion,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pack: Option<String>,
    },
    Terminal {
        record: GarbageRecord,
        terminal: rtdb_types::TerminalObjectPublication,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pack: Option<String>,
    },
    Path(GarbageRecord),
}

impl StoredRelease {
    fn into_parts(self) -> Option<(GarbageRecord, Option<PhysicalObjectVersion>)> {
        match self {
            Self::Exact { record, version, .. } => Some((record, Some(version))),
            Self::Terminal { record, .. } => Some((record, None)),
            Self::Path(record) => Some((record, None)),
        }
    }
}

/// Only a listing needs to know what has been released, so the released set is read on the first one, not on open.
pub struct GarbageStore {
    inner: Arc<dyn ObjectStore>,
    packing: Option<Arc<crate::packing::PackingStore>>,
    index_pages: Option<HostedIndexPages>,
    catalog_readers: Option<CatalogReaders>,
    requires_search_publication: bool,
    release_inventory: Option<crate::reference::Bucket>,
    loaded_releases: tokio::sync::OnceCell<()>,
    released: Mutex<BTreeMap<ObjectKey, Vec<PhysicalObjectVersion>>>,
    clock: Clock,
}

impl GarbageStore {
    pub async fn open(inner: Arc<dyn ObjectStore>) -> Result<Arc<Self>> { Self::open_with_clock(inner, wall_clock()).await }

    pub async fn open_with_clock(inner: Arc<dyn ObjectStore>, clock: Clock) -> Result<Arc<Self>> { Ok(Arc::new(Self::new(inner, clock, None))) }

    fn new(inner: Arc<dyn ObjectStore>, clock: Clock, packing: Option<Arc<crate::packing::PackingStore>>) -> Self {
        Self { inner, packing, index_pages: None, catalog_readers: None, requires_search_publication: false, release_inventory: None,
            loaded_releases: tokio::sync::OnceCell::new(), released: Mutex::new(BTreeMap::new()), clock }
    }

    pub(crate) async fn physical_member(&self, member: &ObjectKey) -> rtdb_storage::Result<ObjectKey> {
        match &self.packing {
            Some(packing) => packing.physical_member(member).await,
            None => Ok(member.clone()),
        }
    }

    async fn load_releases(&self) -> rtdb_storage::Result<()> {
        self.loaded_releases.get_or_try_init(|| async {
            let mut loaded = Vec::new();
            for object in self.inner.list(Some(&ObjectKey::new(GARBAGE_DIRECTORY).map_err(|error| Error::Task(error.to_string()))?)).await? {
                if let Ok(release) = serde_json::from_slice::<StoredRelease>(&self.inner.get(&object.key).await?) {
                    let Some((record, version)) = release.into_parts() else { continue; };
                    if record.key.starts_with("oversized/") { continue; }
                    if let Some(readers) = &self.catalog_readers {
                        match &record.trie_floor {
                            Some(floor) if !readers.matches_release(floor) => continue,
                            None if is_collected_trie_file(&record.key) => continue,
                            _ => (),
                        }
                    }
                    loaded.push((ObjectKey::new(record.key).map_err(|error| Error::Task(error.to_string()))?, version));
                }
            }
            let mut released = self.released.lock();
            for (key, version) in loaded { remember_release(&mut released, key, version); }
            Ok(())
        }).await?;
        Ok(())
    }
    async fn write_release(&self, key: &ObjectKey, trie_floor: Option<CatalogAdmissionFloor>, version: Option<&PhysicalObjectVersion>) -> rtdb_storage::Result<()> {
        self.write_release_record(key, trie_floor, version, None, None).await
    }

    async fn write_release_record(&self, key: &ObjectKey, trie_floor: Option<CatalogAdmissionFloor>, version: Option<&PhysicalObjectVersion>,
        pack: Option<&ObjectKey>, terminal: Option<&rtdb_types::TerminalObjectPublication>) -> rtdb_storage::Result<()> {
        if self.released.lock().get(key).is_some_and(|known| version.is_none_or(|version| known.contains(version))) { return Ok(()); }
        let record = GarbageRecord { key: key.as_str().to_owned(), released_at_ms: (self.clock)(), trie_floor };
        let suffix = if rtdb_format::keys::is_ordered_index_object(key.as_str()) { ORDERED_INDEX_RELEASE }
            else if version.is_some() || terminal.is_some() || rtdb_format::keys::is_trie_retirement_object(key.as_str()) { TRIE_RETIREMENT_RELEASE } else { "" };
        let name = ObjectKey::new(format!("{GARBAGE_DIRECTORY}/{:020}-{}{suffix}", record.released_at_ms, uuid::Uuid::new_v4().simple()))
            .map_err(|error| Error::Task(error.to_string()))?;
        let release = match (version, terminal) {
            (Some(version), _) => StoredRelease::Exact { record, version: version.clone(), pack: pack.map(ObjectKey::to_string) },
            (None, Some(terminal)) => StoredRelease::Terminal { record, terminal: terminal.clone(), pack: pack.map(ObjectKey::to_string) },
            (None, None) => StoredRelease::Path(record),
        };
        self.inner.put(&name, Bytes::from(serde_json::to_vec(&release).expect("a record serializes"))).await?;
        remember_release(&mut self.released.lock(), key.clone(), version.cloned());
        Ok(())
    }
}

fn remember_release(released: &mut BTreeMap<ObjectKey, Vec<PhysicalObjectVersion>>, key: ObjectKey, version: Option<PhysicalObjectVersion>) {
    let known = released.entry(key).or_default();
    if let Some(version) = version && !known.contains(&version) { known.push(version); }
}

fn is_collected_trie_file(key: &str) -> bool {
    rtdb_format::search_manifest::is_run_key(key) || rtdb_format::keys::is_receipt_l0_file(key) || rtdb_format::keys::is_compacted_primary_file(key)
        || rtdb_format::entity_directory::page_digest(key).is_some()
        || rtdb_format::term_directory::page_digest(key).is_some()
}

fn permits_release(record: &DeploymentRecord, store: &str, release: &GarbageRecord, exact: bool) -> bool {
    if release.key.starts_with("oversized/") { return false; }
    let Some(required) = &release.trie_floor else {
        return record.storage_namespaces.is_none() || !is_collected_trie_file(&release.key);
    };
    let cut = required.cut;
    let Ok(root) = StorageRoot::new(cut.storage_version, cut.storage_epoch) else { return false; };
    let root = root.key();
    let residence = record.storage_prefix();
    required.residence == residence
        && store == format!("{residence}/{root}")
        && record.storage_namespaces.is_some_and(|value| value.objects == required.object_namespace)
        && cut.replica_epoch.0 == record.log_epoch
        && crate::catalog_readers::readers_allow(record, &root, cut,
            (exact && rtdb_format::keys::is_catalog_retirement_file(&release.key)).then_some(release.key.as_str()))
        && record.catalog_admission_floors.get(&root).is_some_and(|floor| {
            floor.residence == required.residence && floor.object_namespace == required.object_namespace
                && floor.cut.storage_version == cut.storage_version && floor.cut.storage_epoch == cut.storage_epoch
                && floor.cut.replica_epoch == cut.replica_epoch && floor.cut.through >= cut.through
        })
}

#[async_trait]
impl ObjectStore for GarbageStore {
    fn permits_shared_read_cache(&self) -> bool { self.inner.permits_shared_read_cache() }
    async fn release_entity_directory_page(&self, key: &ObjectKey, cut: Option<rtdb_types::CatalogCut>) -> Result<bool> {
        let Some(readers) = &self.catalog_readers else { self.delete(key).await?; return Ok(true); };
        let Some(cut) = cut else { return Ok(false); };
        let Some(publication) = self.retirement_publication(key).await? else {
            return Ok(matches!(self.inner.head_fresh(key).await, Err(Error::MissingObject(_))));
        };
        let (floor, allowed) = readers.close_admission(cut, &[None]).await?;
        if !allowed[0] { return Ok(false); }
        match publication {
            rtdb_types::PhysicalObjectPublication::Version(version) =>
                self.write_release_record(key, Some(floor), Some(&version), None, None).await?,
            rtdb_types::PhysicalObjectPublication::Terminal(terminal) =>
                self.write_release_record(key, Some(floor), None, None, Some(&terminal)).await?,
        }
        Ok(true)
    }

    fn whole_object_allocation(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<ObjectReadAllocation> {
        self.inner.whole_object_allocation(key, length)
    }
    async fn get_reserved_object(&self, key: &ObjectKey, reservation: ReadReservation) -> rtdb_storage::Result<RetainedBytes> {
        self.inner.get_reserved_object(key, reservation).await
    }
    async fn observe_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
        self.inner.observe_range(key, range).await
    }
    async fn observe_head(&self, key: &ObjectKey) -> rtdb_storage::Result<ObjectObservation<StoredObject>> {
        self.inner.observe_head(key).await
    }
    async fn observe_heads(&self, keys: &[ObjectKey]) -> rtdb_storage::Result<BTreeMap<ObjectKey, ObjectObservation<StoredObject>>> {
        self.inner.observe_heads(keys).await
    }
    fn index_page_lifecycle(&self) -> Option<&dyn IndexPageLifecycle> {
        self.index_pages.as_ref().map(|pages| pages as &dyn IndexPageLifecycle)
    }
    async fn retain_catalog(&self, cut: Option<rtdb_types::CatalogCut>) -> Result<Option<Arc<dyn Send + Sync>>> {
        let Some(readers) = &self.catalog_readers else { return Ok(None); };
        readers.retain(cut).await.map(Some)
    }
    async fn retain_catalog_with_paths(&self, cut: Option<rtdb_types::CatalogCut>, paths: Arc<[String]>)
        -> Result<Option<Arc<dyn Send + Sync>>> {
        let Some(readers) = &self.catalog_readers else { return Ok(None); };
        readers.retain_with_paths(cut, Some(paths)).await.map(Some)
    }
    fn defers_trie_releases(&self) -> bool { self.catalog_readers.is_some() }
    fn releases_receipt_l0(&self) -> bool { self.catalog_readers.is_some() }
    async fn advance_catalog_floor(&self, installed: rtdb_types::CatalogCut) -> Result<bool> {
        let Some(readers) = &self.catalog_readers else { return Ok(true); };
        readers.advance(installed).await
    }
    async fn release_trie_files(&self, receipt: &ObjectKey, contents: Bytes) -> Result<bool> {
        let readers = self.catalog_readers.as_ref().ok_or_else(|| Anomaly::unsupported(
            "rtdb.multi/trie-release", "This store has no coordinated catalog floor"))?;
        let installed = InstalledRetirement::decode(receipt.as_str(), &contents)?;
        let mut files: Vec<_> = installed.files().iter().collect();
        files.sort_by_key(|file| file.release_order());
        let paths = files.iter().map(|file| {
            let exact = file.terminal_publication.is_some() || file.matches_publication && file.version.is_some();
            (exact && rtdb_format::keys::is_compacted_catalog_file(&file.key)).then_some(file.key.as_str())
        }).collect::<Vec<_>>();
        let (floor, allowed) = readers.close_admission(installed.cut(), &paths).await?;
        for (file, allowed) in files.into_iter().zip(&allowed) {
            if !allowed { continue; }
            let version = file.version.as_ref().filter(|_| file.matches_publication);
            self.write_release_record(&ObjectKey::new(&file.key)?, Some(floor.clone()), version, None, file.terminal_publication.as_ref()).await?;
        }
        Ok(allowed.into_iter().all(|allowed| allowed))
    }
    async fn search_staging_owner(&self) -> Result<Option<rtdb_format::search_manifest::SearchStagingOwner>> {
        Ok(self.catalog_readers.as_ref().and_then(CatalogReaders::search_staging_owner))
    }
    async fn search_staging_drained(&self, owner: rtdb_format::search_manifest::SearchStagingOwner) -> Result<bool> {
        let Some(readers) = &self.catalog_readers else { return Ok(false); };
        readers.search_staging_drained(owner).await
    }
    fn requires_search_publication(&self) -> bool { self.requires_search_publication }
    fn releases_exact_search_runs(&self) -> bool { self.catalog_readers.is_some() }
    async fn permits_search_release(&self, key: &ObjectKey, publication: Option<&rtdb_format::log::proto::PublishedObjectVersion>,
        cut: Option<CatalogCut>) -> Result<bool> {
        let Some(readers) = &self.catalog_readers else { return Ok(true); };
        let Some((bucket, cut)) = self.release_inventory.as_deref().zip(cut) else { return Ok(false); };
        readers.permits_search_release(bucket, cut, key, publication).await
    }
    async fn release_search_run(&self, key: &ObjectKey, published: Option<&rtdb_format::log::proto::PublishedObjectVersion>,
        cut: Option<rtdb_types::CatalogCut>) -> Result<bool> {
        let Some(readers) = &self.catalog_readers else { self.delete(key).await?; return Ok(true); };
        let Some(cut) = cut else { return Ok(false); };
        let terminal = published.and_then(rtdb_format::log::proto::PublishedObjectVersion::terminal_publication);
        let version = self.retirement_version(key).await?.filter(|version| published.is_some_and(|published| {
            *published == rtdb_format::log::proto::PublishedObjectVersion::from(version.clone())
        }));
        if version.is_none() && terminal.is_none() { return Ok(false); }
        let path = rtdb_format::search_manifest::is_run_key(key.as_str()).then_some(key.as_str());
        let (floor, allowed) = readers.close_admission(cut, &[path]).await?;
        if !allowed[0] { return Ok(false); }
        self.write_release_record(key, Some(floor), version.as_ref(), None, terminal.as_ref()).await?;
        Ok(true)
    }
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head(key).await }
    async fn head_fresh(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head_fresh(key).await }
    async fn retirement_version(&self, key: &ObjectKey) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectVersion>> {
        self.inner.retirement_version(key).await
    }
    async fn retirement_publication(&self, key: &ObjectKey) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectPublication>> {
        self.inner.retirement_publication(key).await
    }
    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.inner.get(key).await }
    async fn get_fresh(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.inner.get_fresh(key).await }
    async fn get_fresh_exact(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<Bytes> { self.inner.get_fresh_exact(key, length).await }
    async fn observe_fresh_exact(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
        self.inner.observe_fresh_exact(key, length).await
    }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> { self.inner.get_range(key, range).await }
    fn supports_scan_ranges(&self, key: &ObjectKey) -> bool { self.inner.supports_scan_ranges(key) }
    fn supports_observed_scan_ranges(&self, key: &ObjectKey) -> bool { self.inner.supports_observed_scan_ranges(key) }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> { self.inner.put(key, bytes).await }
    async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> { self.inner.put_file(key, source).await }
    async fn put_with_publication(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectPublication>> {
        self.inner.put_with_publication(key, bytes).await
    }
    async fn put_file_with_publication(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectPublication>> {
        self.inner.put_file_with_publication(key, source).await
    }
    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
        self.load_releases().await?;
        let objects = self.inner.list(directory).await?;
        let released = self.released.lock();
        Ok(objects.into_iter().filter(|object| !object.key.as_str().starts_with(GARBAGE_DIRECTORY) && !released.contains_key(&object.key)).collect())
    }
    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
        if self.index_pages.is_some() && ordered_index_storage::page_generation(key.as_str()).is_some() {
            return Err(Error::Closed("The index page generation owns its retirement".into()));
        }
        if key.as_str().starts_with("oversized/") {
            return Err(Error::Remote { key: key.to_string(), message: "An offloaded replica payload belongs to the retained journal".into() });
        }
        if self.catalog_readers.is_some() && is_collected_trie_file(key.as_str()) {
            return Err(Error::Remote { key: key.to_string(), message: "A hosted trie file requires an installed retirement receipt".into() });
        }
        if checkpoint_metadata_block(key.as_str()).is_some()
            && let Some(packing) = &self.packing
            && let Some((pack, publication)) = packing.retirement_pack(key).await?
        {
            return match publication {
                rtdb_types::PhysicalObjectPublication::Version(version) =>
                    self.write_release_record(key, None, Some(&version), Some(&pack), None).await,
                rtdb_types::PhysicalObjectPublication::Terminal(terminal) =>
                    self.write_release_record(key, None, None, Some(&pack), Some(&terminal)).await,
            };
        }
        self.write_release(key, None, None).await
    }
}

pub const ROOT_MARKER: &str = "~root";

/// Verifies the store's owner and lets a holder claim a new root.
pub(crate) async fn check_root(store: &dyn ObjectStore, deployment: &str, claim: bool) -> Result<()> {
    let marker = ObjectKey::new(ROOT_MARKER)?;
    let owner = match store.get(&marker).await {
        Ok(owner) => owner,
        Err(Error::MissingObject(_)) if !claim => return Ok(()),
        Err(Error::MissingObject(_)) => match store.put(&marker, Bytes::copy_from_slice(deployment.as_bytes())).await {
            Ok(()) => return Ok(()),
            Err(Error::ConflictingObject(_)) => store.get(&marker).await?,
            Err(error) => return Err(error.into()),
        },
        Err(error) => return Err(error.into()),
    };
    if owner.as_ref() != deployment.as_bytes() {
        return Err(Anomaly::incorrect("rtdb.multi/foreign-root", format!("This store belongs to deployment {}, not {deployment}", String::from_utf8_lossy(&owner))));
    }
    Ok(())
}

/// What a reaper pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reaped { pub erased: usize, pub waiting: usize }

#[cfg(test)]
async fn reap(bucket: &dyn ObjectStore, record: &DeploymentRecord, grace: Duration, references: &crate::reference::References) -> Result<Reaped> {
    reap_as_of(bucket, record, wall_clock()(), grace, references).await
}

#[cfg(test)]
pub(crate) async fn reap_as_of(bucket: &dyn ObjectStore, record: &DeploymentRecord, now_ms: i64, grace: Duration, references: &crate::reference::References) -> Result<Reaped> {
    reap_path_releases(bucket, record, now_ms, grace, ReleaseSweep::Local(references)).await
}

/// Runs the missing-reference control against an in-memory cell.
#[doc(hidden)]
pub async fn simulate_reap_without_references(bucket: &rtdb_storage::MemoryStore, record: &DeploymentRecord, now_ms: i64,
    grace: Duration) -> Result<Reaped> {
    reap_path_releases(bucket, record, now_ms, grace, ReleaseSweep::Local(&crate::reference::References::default())).await
}

/// Exercises physical collection and reference transfer on the simulator's in-memory cells.
#[doc(hidden)]
pub async fn simulate_reap_cell(bucket: &rtdb_storage::MemoryStore, deployments: &[DeploymentRecord], now_ms: i64,
    grace: Duration) -> Result<Vec<Result<Reaped>>> {
    let references = crate::reference::References::scan(bucket, deployments).await?;
    let mut outcomes = Vec::new();
    for record in deployments.iter().filter(|record| can_reap(record)) {
        outcomes.push(reap_path_releases(bucket, record, now_ms, grace, ReleaseSweep::Local(&references)).await);
    }
    Ok(outcomes)
}

enum ReleaseSweep<'a> {
    Local(&'a crate::reference::References),
    Hosted,
}

fn recovery_covers_release(owner: &DeploymentRecord, release: &GarbageRecord) -> bool {
    let receipt_l0 = rtdb_format::keys::is_receipt_l0_file(&release.key);
    let Some(floor) = &release.trie_floor else { return !receipt_l0; };
    let through = floor.cut.through.after_offset(floor.cut.replica_epoch);
    if receipt_l0 && (through < 0 || !owner.replica_anchor.as_ref().is_some_and(|anchor| {
        anchor.interval[0] >= 0 && anchor.interval[1] >= through
            && matches!(crate::hosted_recovery::replay_start(owner, through - 1),
                Err(Error::UnavailableLogRange { .. }))
    })) {
        return false;
    }
    through >= 0 && owner.log_epoch == floor.cut.replica_epoch.0 && owner.recovery_manifest_sha256.is_some()
        && owner.checkpoint.is_some_and(|checkpoint| checkpoint.root_sha256.is_some() && checkpoint.boundary >= through)
        && owner.storage_regions.values().all(|region| match region.first_required_replica() {
            Some(first) => first > through,
            None => region.checkpoint_metadata_from().is_none(),
        })
}

async fn reap_path_releases(bucket: &dyn ObjectStore, record: &DeploymentRecord, now_ms: i64, grace: Duration,
    sweep: ReleaseSweep<'_>) -> Result<Reaped> {
    use crate::reference::{Base, physical_objects};
    let local = match sweep {
        ReleaseSweep::Local(references) => Some(references),
        ReleaseSweep::Hosted => None,
    };
    let mut reaped = Reaped::default();
    if !matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined) { return Ok(reaped); }
    let published = record.checkpoint.map(|checkpoint| checkpoint.block);
    let oldest = record.storage_regions.values().filter_map(StorageRegion::checkpoint_metadata_from).min();
    let threshold = now_ms - grace.as_millis() as i64;
    for store in crate::reference::stores(bucket, &record.storage_prefix()).await? {
        let before = Base::load(bucket, &store).await?;
        let mut own: BTreeSet<String> = physical_objects(bucket, &store).await?.into_iter().collect();
        // Where an object physically is: in this store, or in the one its manifest names.
        let physical = |own: &BTreeSet<String>, name: &str| -> Option<String> {
            if own.contains(name) { return Some(format!("{store}/{name}")); }
            before.objects.get(name).map(|owner| format!("{owner}/{name}"))
        };
        let garbage = format!("{GARBAGE_DIRECTORY}/");
        let records: BTreeSet<String> = own.iter().chain(before.objects.keys()).filter(|name| name.starts_with(&garbage)).cloned().collect();
        // A released object is either its own object or one entry of a pack; a pack goes once every entry in it has been released for the grace period.
        let mut due: BTreeMap<String, String> = BTreeMap::new();
        let mut exact = BTreeSet::new();
        for name in records {
            let Some(location) = physical(&own, &name) else { continue };
            let Ok(bytes) = bucket.get(&crate::reference::key(&location)?).await else { continue };
            let Ok(release) = serde_json::from_slice::<StoredRelease>(&bytes) else { continue };
            let released = match release {
                StoredRelease::Exact { record, .. } | StoredRelease::Terminal { record, .. } => { exact.insert(record.key); reaped.waiting += 1; continue; }
                StoredRelease::Path(record) => record,
            };
            if ordered_index_storage::page_generation(&released.key).is_some()
                || !permits_release(record, &store, &released, false) || !recovery_covers_release(record, &released)
                || !oversized_payload_collectable(record, &released.key, now_ms)
                || released.released_at_ms > threshold || checkpoint_metadata_block(&released.key)
                .is_some_and(|block| !checkpoint_metadata_collectable(block, published, oldest))
            {
                reaped.waiting += 1;
                continue;
            }
            if local.is_none() && own.contains(&released.key) {
                reaped.waiting += 1;
                continue;
            }
            due.insert(released.key, name);
        }
        due.retain(|key, _| !exact.contains(key));
        let mut forgotten = BTreeSet::new();
        let packs: BTreeSet<String> = own.iter().chain(before.objects.keys()).filter(|name| name.starts_with(&format!("{}/", crate::packing::PACK_DIRECTORY))).cloned().collect();
        // The latest block's pack names every other pack, so a cold start finds them all through it: it stays.
        let latest = packs.iter().filter(|pack| pack.starts_with(&format!("{}/b", crate::packing::PACK_DIRECTORY))).max().cloned();
        let walked = |pack: &str| crate::packing::pack_block(pack)
            .is_some_and(|block| !checkpoint_metadata_collectable(block, published, oldest));
        let mut packed = BTreeSet::new();
        let mut obsolete_packs = BTreeSet::new();
        for pack in packs {
            let Some(location) = physical(&own, &pack) else { continue };
            let index = match crate::packing::read_index(bucket, &crate::reference::key(&location)?).await {
                Err(Error::MissingObject(_)) if !own.contains(&pack) => {
                    // The owner can copy this pack into the fork after its inventory was read.
                    match crate::packing::read_index(bucket, &crate::reference::key(&format!("{store}/{pack}"))?).await {
                        Ok(index) => { own.insert(pack.clone()); index }
                        Err(Error::MissingObject(_)) => { forgotten.insert(pack); continue; }
                        Err(error) => return Err(error.into()),
                    }
                }
                result => result?,
            };
            if (local.is_some() || !own.contains(&pack)) && Some(&pack) != latest.as_ref()
                && !walked(&pack) && index.entries.keys().all(|name| due.contains_key(name))
            {
                obsolete_packs.insert(pack);
            } else {
                packed.extend(index.entries.into_keys());
            }
        }
        if let Some(references) = local {
            // Withdraw fallback edges before physical removal can expose them.
            let shadowed = obsolete_packs.iter().chain(due.keys())
                .filter(|name| own.contains(*name) && before.objects.contains_key(*name)).cloned().collect();
            before.forget_and_prune_locally(bucket, &store, &shadowed).await?;
            for name in obsolete_packs.iter().chain(due.keys()).filter(|name| own.contains(*name)) {
                references.erase(bucket, &store, name).await?;
            }
        }
        forgotten.extend(obsolete_packs.into_iter().chain(due.keys().cloned())
            .filter(|name| !own.contains(name) && before.objects.contains_key(name)));
        let mut local_records = Vec::new();
        for (name, garbage_record) in &due {
            if !packed.contains(name) {
                if own.contains(garbage_record) {
                    if local.is_none() { reaped.waiting += 1; continue; }
                    local_records.push(garbage_record);
                }
                forgotten.insert(garbage_record.clone());
                reaped.erased += 1;
            } else { reaped.waiting += 1; }
        }
        if let Some(references) = local {
            before.forget_and_prune_locally(bucket, &store, &forgotten).await?;
            for garbage_record in local_records { references.erase(bucket, &store, garbage_record).await?; }
        } else {
            before.forget(bucket, &store, &forgotten).await?;
        }
    }
    Ok(reaped)
}

fn checkpoint_metadata_block(key: &str) -> Option<i64> {
    let file = if let Some(file) = key.strip_prefix("blocks/") {
        file
    } else {
        let (_, relative) = key.strip_prefix("tables/")?.split_once('/')?;
        relative.strip_prefix("blocks/")?
    };
    if file.contains('/') { return None; }
    rtdb_format::keys::block_index_from_key(file).ok().map(|index| index.0)
}

fn oversized_payload_collectable(record: &DeploymentRecord, key: &str, now: i64) -> bool {
    key.strip_prefix("oversized/")
        .and_then(rtdb_format::keys::oversized_block_index)
        .is_none_or(|block| record.required_oversized_payload_block(now).is_none_or(|held| block.0 < held))
}

/// When a garbage record's name says it was released, in milliseconds since the epoch.
pub fn garbage_released_at(name: &str) -> Option<i64> {
    name.strip_prefix(&format!("{GARBAGE_DIRECTORY}/"))?.split('-').next()?.parse().ok()
}

pub async fn run_reaper(
    directory: Arc<dyn Directory>,
    storage: crate::deployment::CellStorage,
    keys: Option<Arc<dyn KeyService>>,
    hosted_task: Option<HostedTask>,
    interval: Duration,
    cancellation: tokio_util::sync::CancellationToken,
) {
    let Ok(bucket) = storage.bucket(IoRegion::new()).await else { return };
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = tokio::time::sleep(interval) => {
                let Ok(maintenance) = storage.try_local_maintenance() else { continue; };
                let maintenance = maintenance.map(Arc::new);
                if let Ok(deployments) = directory.deployments().await {
                    if let Some(keys) = keys.as_deref() {
                        for record in &deployments {
                            if record.ordered_indexes.is_empty() && record.ordered_index_catalogs.is_empty()
                                && !record.storage_regions.values().any(|region| {
                                    matches!(region, StorageRegion::Remote { .. } | StorageRegion::ReplicaReplay { .. }
                                        | StorageRegion::ReferenceRead { .. } | StorageRegion::ExactRetirement { .. }
                                        | StorageRegion::ReplayPackRetirement { .. }
                                        | StorageRegion::Local { archive: Some(_), .. } | StorageRegion::Local { repair_archive: Some(_), .. })
                                        || region.checkpoint_metadata_from().is_some()
                                })
                            {
                                continue;
                            }
                            if cancellation.is_cancelled() { return; }
                            if let Err(error) = reconcile_storage_regions(directory.as_ref(), &storage, keys, record).await {
                                eprintln!("Storage publications for {} are pending: {}", record.id, error.code);
                            }
                        }
                        for record in &deployments {
                            if !matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
                                || !crate::admin::has_pending_disposal(record)
                            {
                                continue;
                            }
                            if cancellation.is_cancelled() { return; }
                            if let Err(error) = crate::admin::dispose_generations(directory.as_ref(), &storage, keys, &record.id).await {
                                eprintln!("Closed transfer generations of {} are pending: {}", record.id, error.code);
                            }
                        }
                    }
                    for record in &deployments {
                        let Some(keys) = keys.as_deref() else { break; };
                        if !matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
                            || (record.ordered_indexes.is_empty() && record.ordered_index_catalogs.is_empty())
                        {
                            continue;
                        }
                        // Read again after process-death reconciliation confirmed its I/O drained.
                        let Some(Ok(Some(current))) = cancellation.run_until_cancelled(directory.deployment(&record.id)).await else {
                            if cancellation.is_cancelled() { return; }
                            continue;
                        };
                        match cancellation.run_until_cancelled(
                            ordered_index_storage::reap(directory.as_ref(), &storage, bucket.as_ref(), &current, keys)
                        ).await {
                            None => return,
                            Some(Ok(())) => {}
                            Some(Err(error)) => eprintln!("Ordered index pages for {} are retained: {}", record.id, error.code),
                        }
                    }
                    if let Some(maintenance) = &maintenance {
                        for record in &deployments {
                            if cancellation.is_cancelled() { return; }
                            if let Err(error) = reap_local(directory.as_ref(), &storage, maintenance.clone(), record, keys.as_deref()).await {
                                eprintln!("Local garbage for {} is pending: {}", record.id, error.code);
                            }
                        }
                    } else {
                        let references = crate::reference::References::scan(bucket.as_ref(), &deployments).await;
                        if let CellStorage::S3 { config } = &storage {
                            match CellStorage::s3_inventory(config).await {
                                Ok(provider) => {
                                    for record in &deployments {
                                        if cancellation.is_cancelled() { return; }
                                        if let Some(task) = hosted_task.as_ref()
                                            && let Err(error) = exact_retirement::withdraw_inherited_release(
                                                directory.as_ref(), bucket.as_ref(), record, task,
                                            ).await
                                        {
                                            eprintln!("Inherited release for {} is pending: {}", record.id, error.code);
                                        }
                                        if let Err(error) = exact_retirement::reap(directory.as_ref(), bucket.as_ref(), &provider, record, references.as_ref().ok(), keys.as_deref()).await {
                                            eprintln!("Exact garbage for {} is pending: {}", record.id, error.code);
                                        }
                                        if let (Some(keys), Some(task)) = (keys.as_deref(), hosted_task.as_ref())
                                            && let Err(error) = replay_retirement::reap_hosted(
                                                directory.as_ref(), &storage, keys, task, bucket.as_ref(), &provider, record,
                                            ).await
                                        {
                                            eprintln!("Original replay packs for {} are pending: {}", record.id, error.code);
                                        }
                                    }
                                }
                                Err(error) => eprintln!("The reaper could not open the provider inventory: {}", error.code),
                            }
                        }
                    }
                    drop(maintenance);
                    if let Some(keys) = keys.as_deref() {
                        for record in &deployments {
                            match cancellation.run_until_cancelled(crate::admin::close_abandoned_fork(directory.as_ref(), record)).await {
                                None => return,
                                Some(Err(error)) => eprintln!("Fork {} is retained: {}", record.id, error.code),
                                Some(Ok(())) => {},
                            }
                        }
                        for record in &deployments {
                            if let Some(capture) = record.checkpoint_captures.keys().copied()
                                .find(|capture| crate::archive::obsolete_fork_input(record, *capture))
                            {
                                let disposal = crate::archive::retire_obsolete_fork_input(
                                    directory.as_ref(), record, &storage, keys, capture,
                                );
                                match cancellation.run_until_cancelled(disposal).await {
                                    None => return,
                                    Some(Err(error)) => eprintln!("Automatic fork input {capture} is retained: {}", error.code),
                                    Some(Ok(())) => {}
                                }
                            }
                            for (&capture, release) in &record.checkpoint_capture_retirements {
                                if release.observed_empty_at_ms.is_some() { continue; }
                                let disposal = crate::archive::finish_capture_retirement(
                                    directory.as_ref(), &storage, keys, &record.id, capture,
                                );
                                match cancellation.run_until_cancelled(disposal).await {
                                    None => return,
                                    Some(Err(error)) => eprintln!("Capture {capture} disposal is pending: {}", error.code),
                                    Some(Ok(_)) => {},
                                }
                            }
                            for (&suffix, _) in record.checkpoint_suffix_pins.iter()
                                .filter(|(id, pin)| crate::archive::obsolete_world_suffix(record, **id, pin))
                                .take(8)
                            {
                                let disposal = crate::archive::retire_world_suffix(directory.as_ref(), &record.id, suffix);
                                match cancellation.run_until_cancelled(disposal).await {
                                    None => return,
                                    Some(Err(error)) => eprintln!("World suffix {suffix} disposal is pending: {}", error.code),
                                    Some(Ok(())) => {},
                                }
                            }
                        }
                        for record in deployments.iter().filter(|record| {
                            record.lifecycle == Lifecycle::Deleting || move_cleanup_due(record)
                        }) {
                            let disposal = crate::admin::finish_disposal_at(
                                directory.as_ref(), &storage, keys, &record.id, Some(record.incarnation), 4 << 20,
                            );
                            match cancellation.run_until_cancelled(disposal).await {
                                None => return,
                                Some(Err(error)) => eprintln!("Disposal of {} is pending: {}", record.id, error.code),
                                Some(Ok(_)) => {}
                            }
                        }
                        for record in deployments.iter().filter(|record| record.archive_retirement.as_ref()
                            .is_some_and(|retirement| retirement.observed_empty_at_ms.is_none()))
                        {
                            let Ok(id) = record.id.parse::<Uuid>() else { continue; };
                            if !record.storage_regions.is_empty() {
                                match cancellation.run_until_cancelled(reconcile_storage_regions(directory.as_ref(), &storage, keys, record)).await {
                                    None => return,
                                    Some(Err(error)) => {
                                        eprintln!("Archive readers for {id} are pending: {}", error.code);
                                        continue;
                                    }
                                    Some(Ok(())) => {}
                                }
                            }
                            match cancellation.run_until_cancelled(crate::archive::finish_retirement(directory.as_ref(), keys, id)).await {
                                None => return,
                                Some(Err(error)) => eprintln!("Archive retirement of {id} is pending: {}", error.code),
                                Some(Ok(_)) => {},
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Routing has selected an independent destination. The original owner
/// can now check drain and settle the closed source residence.
pub fn move_cleanup_due(record: &DeploymentRecord) -> bool {
    record.lifecycle == Lifecycle::Deleted && record.disposal.is_none() && record.moved_at_ms.is_some()
}

fn can_reap(record: &DeploymentRecord) -> bool {
    matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
        && !record.storage_regions.values().any(|region| matches!(region, StorageRegion::Remote { archive: None, closed: true, .. }))
}

/// Recovers local admissions while the caller holds the named volume's maintenance lock.
pub(crate) async fn recover_local_lineage(directory: &dyn Directory, lineage: &str, local_volume: [u8; 32]) -> Result<rtdb_directory::LineageState> {
    let mut state = directory.lineage_state(lineage).await?;
    if let Some(previous) = state.retirement().cloned() {
        if previous.work != (RetirementWork::Local { local_volume }) { return Ok(state); }
        // The same volume lock covers every local sweep and stays held by any detached I/O.
        state = directory.change_lineage(lineage, LineageChange::SettleRetirement { retirement: previous }).await?;
    }
    let abandoned = state.local_preparations(local_volume).collect::<Vec<_>>();
    for preparation in abandoned {
        state = directory.change_lineage(lineage, LineageChange::ReleaseFork { preparation }).await?;
    }
    Ok(state)
}

pub(crate) async fn recover_hosted_lineage(directory: &dyn Directory, lineage: &str) -> Result<rtdb_directory::LineageState> {
    let mut state = directory.lineage_state(lineage).await?;
    let preparations = state.preparations().filter_map(|(preparation, owner)| match owner {
        rtdb_directory::PreparationOwner::Hosted { task: Some(task) } => Some((preparation, task.clone())),
        _ => None,
    }).collect::<Vec<_>>();
    for (preparation, task) in preparations {
        if directory.hosted_task_stopped(&task).await? {
            state = directory.change_lineage(lineage, LineageChange::ReleaseFork { preparation }).await?;
        }
    }
    Ok(state)
}

async fn reap_local(directory: &dyn Directory, storage: &CellStorage, maintenance: Arc<File>,
    candidate: &DeploymentRecord, keys: Option<&dyn KeyService>) -> Result<Reaped> {
    let lineage = &candidate.lineage;
    let source = candidate.storage_prefix();
    let local_volume = storage.custody_namespace()?;
    let state = recover_local_lineage(directory, lineage, local_volume).await?;
    let Some(keys) = keys else { return Ok(Reaped::default()); };
    if state.open_generation().is_none() { return Ok(Reaped::default()); }
    let orphans = candidate.local_publications.keys().any(|region| !candidate.storage_regions.contains_key(region));
    if !orphans && !replay_retirement::eligible(candidate) { return Ok(Reaped::default()); }
    let io = IoRegion::new().child_holding(Some(maintenance.clone()));
    let bucket = storage.bucket(io.clone()).await?;
    if !orphans && !replay_retirement::has_older_pack(bucket.as_ref(), candidate).await? {
        return Ok(Reaped::default());
    }
    let generation = state.retirement_generation();
    let retirement = LineageRetirement { token: Uuid::new_v4(), source, work: RetirementWork::Local { local_volume } };
    directory.change_lineage(lineage, LineageChange::ReserveRetirement {
        retirement: retirement.clone(), observed_references: state.reference_revision(), generation,
    }).await?;
    let result = async {
        let deployments = directory.deployments().await?;
        let Some(record) = deployments.iter().find(|record| record.id == candidate.id && record.storage_prefix() == retirement.source)
            else { return Ok(Reaped::default()); };
        let withdrawn = reap_local_attempts(directory, keys, bucket.as_ref(), &io, record, &deployments).await?;
        if !replay_retirement::eligible(record) {
            return Ok(Reaped { erased: withdrawn, waiting: 0 });
        }
        let references = crate::reference::References::scan(bucket.as_ref(), &deployments).await?;
        let erased = replay_retirement::reap(directory, storage, keys, bucket.as_ref(), record, &references).await? + withdrawn;
        Ok(Reaped { erased, waiting: 0 })
    }.await;
    io.close_and_wait().await;
    directory.change_lineage(lineage, LineageChange::SettleRetirement { retirement }).await?;
    result
}

async fn reap_local_attempts(
    directory: &dyn Directory, keys: &dyn KeyService, bucket: &dyn ObjectStore, io: &Arc<IoRegion>,
    record: &DeploymentRecord, deployments: &[DeploymentRecord],
) -> Result<usize> {
    let mut withdrawn = 0;
    let census = publication_retirement::PublicationCensus::new(keys, deployments, io);
    let origin = UnitScope::publication_origin(&record.storage_prefix());
    let outside_lineage = ObjectKey::new("~local-serving-attempt-census")?;
    for (region, attempts) in &record.local_publications {
        if record.storage_regions.contains_key(region) { continue; }
        crate::deployment::require_storage_namespaces(record, crate::deployment::storage_namespaces(bucket, keys).await?)?;
        for (id, (name, unit)) in attempts {
            if *id != unit.attempt || unit.origin != origin {
                return Err(storage_region_error("The local attempt belongs to another residence"));
            }
            let logical = ObjectKey::new(name)?;
            if !census.has_another_copy(bucket, record, &outside_lineage, &logical, *unit, true).await? {
                let slot = UnitScope::directory_publication_slot(&record.org, &record.lineage, *unit, &logical);
                keys.withdraw_unit_key(slot, io).await?;
                withdrawn += 1;
            }
        }
        crate::admin::update_installation(directory, &record.id, |current| {
            if current.storage_prefix() != record.storage_prefix() || current.storage_regions.contains_key(region)
                || current.local_publications.get(region).is_some_and(|pending| pending != attempts)
            {
                return Err(storage_region_error("The drained local serving region has changed"));
            }
            current.local_publications.remove(region);
            Ok(())
        }).await?;
    }
    Ok(withdrawn)
}

pub(crate) async fn with_source_lineage<T>(directory: &dyn Directory, lineage: Option<&str>,
    task: Option<&HostedTask>, work: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    let Some(lineage) = lineage else { return work.await; };
    let generation = directory.lineage_state(lineage).await?.retirement_generation();
    let preparation = rtdb_directory::ForkPreparation { token: Uuid::new_v4(), generation };
    directory.change_lineage(lineage, LineageChange::PrepareHostedFork { preparation, task: task.cloned() }).await?;
    let result = work.await;
    let released = directory.change_lineage(lineage, LineageChange::ReleaseFork { preparation }).await;
    result.and_then(|value| released.map(|_| value))
}

/// Exercises the retired path-only policy for simulation and negative controls.
/// Production maintenance takes its authority from exact grants.
///
/// ```compile_fail
/// use rtdb_multi::store::reap_as_of;
/// ```
///
/// ```compile_fail
/// use std::time::Duration;
/// use rtdb_directory::DeploymentRecord;
/// use rtdb_multi::store::simulate_reap_without_references;
/// use rtdb_storage::ObjectStore;
///
/// fn run(bucket: &dyn ObjectStore, record: &DeploymentRecord) {
///     let _ = simulate_reap_without_references(bucket, record, 0, Duration::ZERO);
/// }
/// ```
///
/// ```compile_fail
/// use std::time::Duration;
/// use rtdb_multi::store::simulate_reap_cell;
/// use rtdb_storage::ObjectStore;
///
/// fn run(bucket: &dyn ObjectStore) {
///     let _ = simulate_reap_cell(bucket, &[], 0, Duration::ZERO);
/// }
/// ```
#[doc(hidden)]
pub async fn reaper_pass(bucket: &dyn ObjectStore, deployments: &[DeploymentRecord], now_ms: i64, grace: Duration) -> Result<Vec<Result<Reaped>>> {
    Ok(reap_remote_paths(bucket, deployments, now_ms, grace).await)
}

async fn reap_remote_paths(bucket: &dyn ObjectStore, deployments: &[DeploymentRecord], now_ms: i64, grace: Duration) -> Vec<Result<Reaped>> {
    let mut reaped = Vec::new();
    for record in deployments {
        if !can_reap(record) { continue; }
        reaped.push(reap_path_releases(bucket, record, now_ms, grace, ReleaseSweep::Hosted).await);
    }
    reaped
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn search_release_waits_for_fork_preparation_archive_hold_and_declared_reference() {
        use rtdb_directory::{ArchiveSourceHold, ForkPreparation, MemoryDirectory, StorageNamespaces};
        use rtdb_format::{keys::CURRENT_STORAGE_VERSION, log::proto::PublishedObjectVersion};
        use rtdb_storage::MemoryStore;
        use rtdb_types::{PhysicalObjectPublication, ReplicaPosition, S3ObjectNamespace, StorageEpoch, TerminalObjectPublication};

        let directory = MemoryDirectory::new();
        let owner = Uuid::new_v4();
        let namespaces = StorageNamespaces { objects: Uuid::new_v4(), keys: [0; 32] };
        let epoch = StorageEpoch(0);
        let root = StorageRoot::new(CURRENT_STORAGE_VERSION, epoch).unwrap().key();
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        record.holder = Some(MemberId::fresh());
        record.storage_namespaces = Some(namespaces);
        record.storage_regions.insert(owner, remote_storage_region(None, None));
        let record = directory.create_deployment(record).await.unwrap();
        let scope = format!("{}/{root}", record.storage_prefix());
        let bucket: crate::reference::Bucket = Arc::new(MemoryStore::default());
        let mut store = open_deployment_store(bucket.clone(), &scope, crate::packing::PackDiscovery::List, wall_clock()).await.unwrap();
        Arc::get_mut(&mut store).unwrap().catalog_readers = Some(CatalogReaders::new(
            directory.clone(), TaskTracker::new(), &record, root.clone(), namespaces, owner, Some(record.epoch),
        ));
        let store = EncryptingStore::new(store, &[7; 32]).unwrap();
        let cut = CatalogCut { storage_version: CURRENT_STORAGE_VERSION, storage_epoch: epoch, replica_epoch: epoch, through: ReplicaPosition(0) };
        let body = key(&format!("tables/public$docs/search-runs/{}.f16.arrow", "a".repeat(64)));
        let physical = key(&format!("{scope}/{body}"));
        let namespace = S3ObjectNamespace { bucket: "cell".into(), endpoint: None, region: None };
        let publication = PublishedObjectVersion::from(PhysicalObjectPublication::Terminal(TerminalObjectPublication {
            namespace: namespace.clone(), key: physical.to_string(), allocation: namespaces.objects, etag: "etag".into(),
        }));
        store.put(&body, Bytes::from_static(b"body")).await.unwrap();
        assert!(store.permits_search_release(&body, Some(&publication), Some(cut)).await.unwrap());

        let preparation = ForkPreparation { token: Uuid::new_v4(), generation: 0 };
        directory.change_lineage(&record.lineage, LineageChange::PrepareHostedFork { preparation, task: None }).await.unwrap();
        assert!(!store.permits_search_release(&body, Some(&publication), Some(cut)).await.unwrap());

        let archive = Uuid::new_v4();
        crate::admin::update_installation(directory.as_ref(), &record.id, |record| {
            record.storage_regions.insert(archive, StorageRegion::ReferenceRead {
                object: Some(physical.to_string()),
                custody: ReplayCustody::Hosted { owner: None },
                archive_source: Some(ArchiveSourceHold::new(namespace.clone(), BTreeMap::from([(physical.to_string(), "version".into())]))),
            });
            Ok(())
        }).await.unwrap();
        directory.change_lineage(&record.lineage, LineageChange::ReleaseFork { preparation }).await.unwrap();
        assert!(!store.permits_search_release(&body, Some(&publication), Some(cut)).await.unwrap());
        let versioned = |version: &str| PublishedObjectVersion::from(PhysicalObjectVersion {
            namespace: namespace.clone(), key: physical.to_string(), version: version.into(),
        });
        assert!(!store.permits_search_release(&body, Some(&versioned("version")), Some(cut)).await.unwrap());
        assert!(store.permits_search_release(&body, Some(&versioned("replacement")), Some(cut)).await.unwrap());

        crate::admin::update_installation(directory.as_ref(), &record.id, |record| {
            record.storage_regions.remove(&archive);
            Ok(())
        }).await.unwrap();
        assert!(store.permits_search_release(&body, Some(&publication), Some(cut)).await.unwrap());

        let mut child = DeploymentRecord::new("child", "acme", vec![], 0);
        child.lineage.clone_from(&record.lineage);
        let child = directory.create_deployment(child).await.unwrap();
        let child_store = format!("{}/{root}", child.storage_prefix());
        let base = crate::reference::Base { objects: BTreeMap::from([(body.to_string(), scope)]), ..Default::default() };
        base.initialize(bucket.as_ref(), &child_store).await.unwrap();
        assert!(!store.permits_search_release(&body, Some(&publication), Some(cut)).await.unwrap());
        assert_eq!(store.get(&body).await.unwrap(), Bytes::from_static(b"body"));

        base.forget(bucket.as_ref(), &child_store, &BTreeSet::from([body.to_string()])).await.unwrap();
        assert!(store.permits_search_release(&body, Some(&publication), Some(cut)).await.unwrap());
        assert!(store.release_search_run(&body, Some(&publication), Some(cut)).await.unwrap());
        assert!(store.list(Some(&body)).await.unwrap().is_empty());
        assert!(bucket.head(&physical).await.is_ok());
    }

    #[test]
    fn a_receipt_l0_needs_a_recovery_exclusion_and_a_successor() {
        use rtdb_directory::{CatalogAdmissionFloor, Checkpoint, JournalAnchor};
        use rtdb_types::{CatalogCut, JournalEndpoint, ReplicaPosition, StorageEpoch, StorageVersion};

        let mut owner = DeploymentRecord::new("shop", "acme", vec![], 0);
        owner.checkpoint = Some(Checkpoint {
            block: 2, boundary: 8, source: 4, system_time: 100, root_sha256: Some([1; 32]),
        });
        owner.recovery_manifest_sha256 = Some([2; 32]);
        let floor = CatalogAdmissionFloor {
            residence: owner.storage_prefix(),
            object_namespace: Uuid::new_v4(),
            cut: CatalogCut {
                storage_version: StorageVersion(6), storage_epoch: StorageEpoch(0),
                replica_epoch: StorageEpoch(0), through: ReplicaPosition(6),
            },
        };
        let mut release = GarbageRecord {
            key: "tables/xt$bounded_retry_receipts/data/l00-rc-b00.arrow".into(),
            released_at_ms: 0, trie_floor: None,
        };
        assert!(!recovery_covers_release(&owner, &release));
        release.trie_floor = Some(floor);
        assert!(!recovery_covers_release(&owner, &release));
        owner.replica_anchor = Some(Box::new(JournalAnchor {
            root_incarnation: owner.incarnation, interval: [0, 5], digest: [3; 32],
        }));
        assert!(!recovery_covers_release(&owner, &release));
        owner.replica_anchor.as_mut().unwrap().interval[1] = 6;
        owner.replica_journals.insert(JournalEndpoint::Address("unavailable".into()));
        assert!(recovery_covers_release(&owner, &release));
        owner.checkpoint.as_mut().unwrap().boundary = 5;
        assert!(!recovery_covers_release(&owner, &release));
    }

    #[tokio::test]
    async fn drained_local_attempt_retires_only_the_unplaced_slot() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let root = tempfile::tempdir().unwrap();
        let storage = CellStorage::Local { root: root.path().join("objects") };
        let keys = crate::keys::LocalKeyService::new(root.path().join("keys")).unwrap();
        let bucket = storage.bucket(IoRegion::new()).await.unwrap();
        let region = Uuid::new_v4();
        let witness = keys.hold_local_storage_region(region).unwrap().unwrap();
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        record.storage_namespaces = Some(crate::deployment::storage_namespaces(bucket.as_ref(), &keys).await.unwrap());
        record.storage_regions.insert(region, StorageRegion::Local {
            archive: None, checkpoint_suffix: None, repair_archive: None, tracks_ordered_index_pages: false,
            checkpoint_metadata_from: Some(0), checkpoint_replay_from: None,
        });
        let record = directory.create_deployment(record).await.unwrap();
        let prefix = format!("{}/v06", record.storage_prefix());
        let journal = Arc::new(DirectoryPublications {
            directory: directory.clone(), deployment: record.id.clone(), region,
            writer_epoch: None, owner: PublicationOwner::Serving,
        });
        let cipher = directory_publications(&keys, &record, IoRegion::new()).unwrap()
            .with_attempts(journal, prefix.clone());
        let name = key("blocks/b00.binpb");
        let abandoned = cipher.seal(&name, Bytes::from_static(b"abandoned")).await.unwrap();
        let selected = cipher.seal(&name, Bytes::from_static(b"selected")).await.unwrap();
        assert_ne!(&abandoned[..rtdb_types::ServingPublication::BYTES], &selected[..rtdb_types::ServingPublication::BYTES]);
        let (pack, _) = crate::packing::encode_pack(&BTreeMap::from([(name.to_string(), selected.clone())]), vec![], 0);
        bucket.put(&key(&format!("{prefix}/~packs/b00.pack")), pack).await.unwrap();
        let active = directory.deployment(&record.id).await.unwrap().unwrap();
        let lock = Arc::new(storage.try_local_maintenance().unwrap().unwrap());
        let kept = reap_local(directory.as_ref(), &storage, lock.clone(), &active, Some(&keys)).await.unwrap();
        assert_eq!(kept.erased, 0);
        drop(witness);
        reconcile_storage_regions(directory.as_ref(), &storage, &keys, &active).await.unwrap();
        let drained = directory.deployment(&record.id).await.unwrap().unwrap();
        let reaped = reap_local(directory.as_ref(), &storage, lock, &drained, Some(&keys)).await.unwrap();
        assert_eq!(reaped.erased, 1);
        assert_eq!(cipher.open(&name, selected).await.unwrap().as_ref(), b"selected");
        assert!(matches!(cipher.open(&name, abandoned).await, Err(Error::ErasedUnderView)));
    }

    #[tokio::test]
    async fn local_maintenance_retains_a_path_release_without_an_exact_grant() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let root = tempfile::tempdir().unwrap();
        let storage = CellStorage::Local { root: root.path().to_owned() };
        let bucket = storage.bucket(IoRegion::new()).await.unwrap();
        let parent = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let stale = directory.deployments().await.unwrap().remove(0);
        let mut child = DeploymentRecord::new("branch", "acme", vec![], 0);
        child.parent = Some(parent.id.clone());
        child.lineage.clone_from(&parent.lineage);
        let source_prefix = format!("{}/v06", parent.storage_prefix());
        let file = key("tables/orders/data/l01-rc-b00.arrow");
        let source = open_deployment_store(bucket.clone(), &source_prefix, crate::packing::PackDiscovery::List, wall_clock()).await.unwrap();
        source.put(&file, Bytes::from_static(b"original")).await.unwrap();
        crate::reference::simulate_fork(bucket.as_ref(), &parent.storage_prefix(), &child.storage_prefix(), None).await.unwrap();
        let child = directory.create_fork(child, parent.version).await.unwrap();

        let preparation = rtdb_directory::ForkPreparation { token: Uuid::new_v4(), generation: 0 };
        let local_volume = storage.custody_namespace().unwrap();
        directory.change_lineage(&parent.lineage, LineageChange::PrepareFork { preparation, local_volume }).await.unwrap();
        let lock = Arc::new(storage.try_local_maintenance().unwrap().unwrap());
        assert_eq!(reap_local(directory.as_ref(), &storage, lock.clone(), &stale, None).await.unwrap(), Reaped::default());
        let state = directory.lineage_state(&parent.lineage).await.unwrap();
        assert!(state.local_preparations(local_volume).next().is_none());
        assert_eq!(state.retirement_generation(), 0);

        source.delete(&file).await.unwrap();
        let outcome = reap_local(directory.as_ref(), &storage, lock, &stale, None).await.unwrap();

        let state = directory.lineage_state(&parent.lineage).await.unwrap();
        assert_eq!(state.retirement_generation(), 0);
        assert!(state.retirement().is_none());
        assert!(state.local_preparations(local_volume).next().is_none());
        assert_eq!(outcome, Reaped::default());
        assert!(bucket.head(&key(&format!("{source_prefix}/{file}"))).await.is_ok());
        let child_view = open_deployment_store(bucket, &format!("{}/v06", child.storage_prefix()),
            crate::packing::PackDiscovery::List, wall_clock()).await.unwrap();
        assert_eq!(child_view.get(&file).await.unwrap(), Bytes::from_static(b"original"));
    }

    #[tokio::test]
    async fn search_release_refuses_head_only_absence_and_staged_absence_requires_drain() {
        use rtdb_directory::{MemoryDirectory, StorageNamespaces};
        use rtdb_format::{keys::CURRENT_STORAGE_VERSION, search_manifest::SearchStagingOwner};
        use rtdb_storage::{MemoryStore, object_store::DrainedSearchUpload};
        use rtdb_types::{CatalogCut, ReplicaPosition, StorageEpoch};

        let directory = MemoryDirectory::new();
        let old_owner = Uuid::new_v4();
        let owner = Uuid::new_v4();
        let namespaces = StorageNamespaces { objects: Uuid::new_v4(), keys: [0; 32] };
        let epoch = StorageEpoch(0);
        let root = StorageRoot::new(CURRENT_STORAGE_VERSION, epoch).unwrap().key();
        let mut record = DeploymentRecord::new("shop", "acme", Vec::new(), 0);
        record.epoch = 2;
        record.holder = Some(MemberId::fresh());
        record.storage_namespaces = Some(namespaces);
        record.storage_regions.insert(owner, remote_storage_region(None, None));
        record.storage_regions.insert(old_owner, remote_storage_region(None, None));
        let record = directory.create_deployment(record).await.unwrap();
        let mut store = GarbageStore::new(Arc::new(MemoryStore::default()), wall_clock(), None);
        store.catalog_readers = Some(CatalogReaders::new(directory.clone(), TaskTracker::new(), &record,
            root, namespaces, owner, Some(record.epoch)));
        let run = key(&format!("tables/docs/search-runs/{}.f16.arrow", "ab".repeat(32)));
        let cut = CatalogCut { storage_version: CURRENT_STORAGE_VERSION, storage_epoch: epoch, replica_epoch: epoch, through: ReplicaPosition(0) };
        let staged = SearchStagingOwner::Directory { region: old_owner, writer_epoch: 1 };
        assert!(matches!(store.head_fresh(&run).await, Err(Error::MissingObject(_))));
        assert!(!store.release_search_run(&run, None, Some(cut)).await.unwrap());
        assert!(store.head_drained_search_upload(&run, staged).await.unwrap().is_none());

        crate::admin::update_installation(directory.as_ref(), &record.id, |current| {
            current.storage_regions.remove(&old_owner);
            Ok(())
        }).await.unwrap();
        assert!(matches!(store.head_drained_search_upload(&run, staged).await.unwrap(), Some(DrainedSearchUpload::Absent)));
        assert!(!store.release_search_run(&run, None, Some(cut)).await.unwrap());
    }

    #[tokio::test]
    async fn trie_release_waits_for_its_installed_catalog_floor() {
        use rtdb_directory::{MemoryDirectory, StorageNamespaces};
        use rtdb_format::keys::CURRENT_STORAGE_VERSION;
        use rtdb_format::trie_retirement::{RetirementFile, RetirementIntent, intent_digest, journal_directory};
        use rtdb_storage::MemoryStore;
        use rtdb_types::{CatalogCut, ReplicaPosition, StorageEpoch, TableRef};

        let directory = MemoryDirectory::new();
        let owner = Uuid::new_v4();
        let namespaces = StorageNamespaces { objects: Uuid::new_v4(), keys: [0; 32] };
        let epoch = StorageEpoch(0);
        let root = rtdb_format::keys::StorageRoot::new(CURRENT_STORAGE_VERSION, epoch).unwrap().key();
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        record.holder = Some(MemberId::fresh());
        record.storage_namespaces = Some(namespaces);
        record.storage_regions.insert(owner, StorageRegion::Local {
            archive: None, checkpoint_suffix: None, repair_archive: None, tracks_ordered_index_pages: true,
            checkpoint_metadata_from: None, checkpoint_replay_from: None,
        });
        let record = directory.create_deployment(record).await.unwrap();
        let scope = format!("{}/{root}", record.storage_prefix());
        let bucket: crate::reference::Bucket = Arc::new(MemoryStore::default());
        let mut store = open_deployment_store(bucket.clone(), &scope, crate::packing::PackDiscovery::List, wall_clock()).await.unwrap();
        let tasks = TaskTracker::new();
        let readers = CatalogReaders::new(directory.clone(), tasks.clone(), &record, root, namespaces, owner, Some(record.epoch));
        Arc::get_mut(&mut store).unwrap().catalog_readers = Some(readers);
        let store = Arc::new(EncryptingStore::new(store, &[7; 32]).unwrap());
        let buffer = rtdb_storage::cache::BufferPool::new(store.clone(), 1 << 20);
        let cut = CatalogCut { storage_version: CURRENT_STORAGE_VERSION, storage_epoch: epoch, replica_epoch: epoch, through: ReplicaPosition(0) };
        let hold = buffer.hold_catalog(Some(cut)).await.unwrap();
        let renewed = buffer.renew_catalog(&hold).await.unwrap();
        assert_eq!(directory.deployment(&record.id).await.unwrap().unwrap().catalog_readers.len(), 2);
        let file = key("tables/orders/data/l01-rc-b00.arrow");
        let physical = key(&format!("{scope}/{file}"));
        store.put(&file, Bytes::from_static(b"admitted")).await.unwrap();
        let ancestor = "parent/v06";
        let ancestor_copy = key(&format!("{ancestor}/{file}"));
        bucket.copy(&physical, &ancestor_copy).await.unwrap();
        let base = crate::reference::Base { objects: BTreeMap::from([(file.to_string(), ancestor.into())]), ..Default::default() };
        let manifest = key(&format!("{scope}/{}/{:020}", crate::reference::BASE_MANIFEST, 0));
        bucket.put(&manifest, serde_json::to_vec(&base).unwrap().into()).await.unwrap();

        assert!(store.delete(&file).await.is_err());
        let legacy = GarbageRecord { key: file.to_string(), released_at_ms: 0, trie_floor: None };
        bucket.put(&key(&format!("{scope}/{GARBAGE_DIRECTORY}/legacy")), serde_json::to_vec(&legacy).unwrap().into()).await.unwrap();

        let intent = RetirementIntent {
            selected: cut, table: TableRef::new("public", "orders"), tries: vec!["l01-rc-b00".into()],
            files: vec![RetirementFile { key: file.to_string(), version: None, terminal_publication: None, matches_publication: false }],
            after_reset: None,
        };
        let installed = ReplicaPosition(1);
        let floor = intent.installed_cut(installed).unwrap();
        let plan = serde_json::to_vec(&intent).unwrap();
        let receipt = key(&format!("{}/installed/{}", journal_directory(cut), intent_digest(&plan)));
        let receipt_bytes = Bytes::from([installed.0.to_be_bytes().as_slice(), &plan].concat());
        store.put(&receipt, receipt_bytes.clone()).await.unwrap();
        assert!(!store.advance_catalog_floor(floor).await.unwrap());
        assert!(!store.release_trie_files(&receipt, receipt_bytes.clone()).await.unwrap());

        let held = directory.deployment(&record.id).await.unwrap().unwrap();
        assert_eq!(held.catalog_readers.len(), 2);
        let refs = crate::reference::References::default();
        reap_as_of(bucket.as_ref(), &held, i64::MAX, Duration::ZERO, &refs).await.unwrap();
        assert_eq!(store.get(&file).await.unwrap(), Bytes::from_static(b"admitted"));
        drop(renewed);
        // The original cohort still prevents advancement after the renewal drains.
        assert!(!store.advance_catalog_floor(floor).await.unwrap());
        drop(hold);
        tasks.close();
        tasks.wait().await;
        let mut forged = receipt_bytes.to_vec();
        forged[7] = 2;
        assert!(store.release_trie_files(&receipt, forged.into()).await.is_err());
        assert!(store.release_trie_files(&receipt, receipt_bytes).await.unwrap());
        reap_as_of(bucket.as_ref(), &held, i64::MAX, Duration::ZERO, &refs).await.unwrap();
        assert!(bucket.head(&physical).await.is_ok());
        let mut current = directory.deployment(&record.id).await.unwrap().unwrap();
        current.checkpoint = Some(rtdb_directory::Checkpoint {
            block: 1, boundary: 1, source: 0, system_time: 0, root_sha256: Some([1; 32]),
        });
        current.recovery_manifest_sha256 = Some([1; 32]);
        let version = current.version;
        let current = directory.replace_deployment(current, version).await.unwrap().applied().unwrap();
        let mut foreign = current.clone();
        foreign.storage_namespaces.as_mut().unwrap().objects = Uuid::new_v4();
        reap_as_of(bucket.as_ref(), &foreign, i64::MAX, Duration::ZERO, &refs).await.unwrap();
        assert!(bucket.head(&physical).await.is_ok());
        assert_eq!(crate::reference::Base::load(bucket.as_ref(), &scope).await.unwrap(), base,
            "an unrecognized catalog floor cannot withdraw a fallback");
        reap_as_of(bucket.as_ref(), &current, i64::MAX, Duration::ZERO, &refs).await.unwrap();
        assert!(matches!(bucket.head(&physical).await, Err(Error::MissingObject(_))));
        assert!(crate::reference::Base::load(bucket.as_ref(), &scope).await.unwrap().objects.is_empty(),
            "a recognized release withdraws the fallback before deleting the local copy");
        assert!(bucket.head(&ancestor_copy).await.is_ok());
    }

    #[tokio::test]
    async fn an_inflight_local_read_retains_the_maintenance_witness_after_cancellation() {
        let root = tempfile::tempdir().unwrap();
        let storage = CellStorage::Local { root: root.path().to_owned() };
        let lock = storage.try_local_maintenance().unwrap().unwrap();
        let owner = IoRegion::new();
        let io = owner.child_holding(Some(Arc::new(lock)));
        let (started, admitted) = oneshot::channel();
        let (finish, resume) = std::sync::mpsc::channel();
        let reader = tokio::spawn(async move {
            io.run_blocking(move || {
                started.send(()).unwrap();
                resume.recv().unwrap();
                Ok(())
            }).await
        });
        admitted.await.unwrap();
        reader.abort();
        assert!(reader.await.unwrap_err().is_cancelled());
        let busy = storage.try_local_maintenance().err().map(|error| error.code);
        assert_eq!(busy.as_deref(), Some("rtdb.multi/storage-maintenance-busy"),
            "provider work retains the maintenance witness after its caller is cancelled");
        finish.send(()).unwrap();
        owner.close_and_wait().await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match storage.try_local_maintenance() {
                    Ok(Some(_)) => break,
                    Err(error) if error.code == "rtdb.multi/storage-maintenance-busy" => {
                        tokio::task::yield_now().await;
                    }
                    result => panic!("expected a local maintenance witness: {result:?}"),
                }
            }
        }).await.unwrap();
    }

    #[test]
    fn a_new_storage_generation_uses_an_independent_cache_identity() {
        let mut config = CellStoreConfig {
            storage: crate::deployment::CellStorage::Local { root: PathBuf::from("/cell/objects") },
            prefix: "acme/deployments/installation/1".into(),
            deployment: "installation".into(),
            writer_epoch: Some(8),
            compactor: false,
            snapshot_block: None,
        };
        let original = config.cache_identity().unwrap();
        config.writer_epoch = Some(9);
        assert_eq!(config.cache_identity().unwrap(), original);
        config.writer_epoch = None;
        config.compactor = true;
        assert_eq!(config.cache_identity().unwrap(), original);
        config.compactor = false;
        config.snapshot_block = Some(7);
        assert_eq!(config.cache_identity().unwrap(), original);
        config.prefix.push_str("/generation-2");
        assert_ne!(config.cache_identity().unwrap(), original);
    }

    #[tokio::test]
    async fn a_cache_restart_isolates_unframed_ciphertext_from_the_current_envelope() -> Result<()> {
        use rtdb_storage::disk_cache::DiskCache;
        let directory = tempfile::tempdir().unwrap();
        let config = CellStoreConfig {
            storage: CellStorage::Local { root: directory.path().join("serving") },
            prefix: "acme/deployments/installation/1".into(), deployment: "installation".into(),
            writer_epoch: None, compactor: false, snapshot_block: None,
        };
        let legacy_identity = serde_json::to_vec(&(&config.storage, &config.prefix, &config.deployment)).unwrap();
        let namespace = |identity: &[u8]| ObjectKey::new(base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD, ring::digest::digest(&ring::digest::SHA256, identity),
        )).unwrap();
        let archive_cache = directory.path().join("cache");
        let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        let file = ObjectKey::new("tables/t/data/l00-rc-b00.arrow")?;
        let plaintext = Bytes::from(vec![7; 64 << 10]);
        let material = [3; crate::keys::KEY_BYTES];
        let legacy = Arc::new(rtdb_storage::MemoryStore::default());
        legacy.put(&file, Cipher::new(&material)?.seal_deterministic(file.as_str(), &plaintext)?).await?;
        let cache = DiskCache::open(&archive_cache, 1 << 20, memory.clone())?;
        let legacy_store = cache.scope(namespace(&legacy_identity), legacy)?;
        assert_eq!(legacy_store.get(&file).await?.len(), plaintext.len() + crate::cipher::SEALED_OVERHEAD);
        drop(legacy_store);
        cache.close_and_wait().await;
        drop(cache);

        let framed = Arc::new(rtdb_storage::MemoryStore::default());
        EncryptingStore::new(framed.clone(), &material)?.put(&file, plaintext.clone()).await?;
        let cache = DiskCache::open(&archive_cache, 1 << 20, memory)?;
        let current_namespace = namespace(&config.cache_identity()?);
        let current = cache.scope(current_namespace.clone(), framed.clone())?;
        let serving = EncryptingStore::new(current, &material)?;
        assert_eq!(serving.get(&file).await?, plaintext);
        drop(serving);
        cache.close_and_wait().await;
        drop(cache);

        let sealed_length = object::sealed_size(&file, plaintext.len())?;
        let memory = rtdb_columnar::MemoryBudget::new(plaintext.len() + sealed_length + (4 << 10));
        let cache = DiskCache::open(&archive_cache, 1 << 20, memory.clone())?;
        let current = cache.scope(current_namespace, framed)?;
        let serving = Arc::new(EncryptingStore::new(current, &material)?);
        let pool = rtdb_storage::cache::BufferPool::with_budget(serving, rtdb_storage::cache::CacheBudget::with_memory(memory));
        assert_eq!(pool.get(&file).await?.as_slice(), plaintext.as_ref());
        Ok(())
    }

    #[tokio::test]
    async fn a_prepared_local_witness_keeps_its_lock_through_admission_and_only_unused_witnesses_are_discarded() {
        let directory = tempfile::tempdir().unwrap();
        let keys = Arc::new(crate::keys::LocalKeyService::new(directory.path()).unwrap());
        let tasks = TaskTracker::new();
        let pool = LocalWitnessPool::new(keys.clone(), &tasks);
        tasks.close();
        tasks.wait().await;

        let (id, file) = pool.take(&tasks).expect("the standby witness is ready");
        assert!(!keys.local_storage_region_drained(id).unwrap());
        tasks.wait().await;
        let standby = match *pool.slot.lock() {
            LocalWitnessSlot::Ready(id, _) => id,
            _ => panic!("the standby witness was replenished"),
        };
        assert_ne!(id, standby);
        assert!(directory.path().join(format!("{standby}.storage-region")).exists());
        drop(pool);
        assert!(!directory.path().join(format!("{standby}.storage-region")).exists());

        assert!(!keys.local_storage_region_drained(id).unwrap());
        drop(file);
        assert!(keys.local_storage_region_drained(id).unwrap());
        keys.forget_local_storage_region(id).unwrap();
        assert!(std::fs::read_dir(directory.path()).unwrap().next().is_none());
    }
    use rtdb_storage::LocalStore;
    use rtdb_storage::MemoryStore;

    fn key(name: &str) -> ObjectKey { ObjectKey::new(name).unwrap() }

    #[tokio::test]
    async fn terminal_writers_and_compactors_require_scoped_credentials_before_storage_admission() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let root = tempfile::tempdir().unwrap();
        let local = CellStorage::Local { root: root.path().join("objects") };
        let keys = Arc::new(crate::keys::LocalKeyService::new(root.path().join("keys")).unwrap());
        let bucket = local.bucket(IoRegion::new()).await.unwrap();
        let storage_namespaces = crate::deployment::storage_namespaces(bucket.as_ref(), keys.as_ref()).await.unwrap();
        let allocation = uuid::Uuid::new_v4();
        let storage = CellStorage::parse(&format!("s3://fixture?terminal={allocation}.{}", "A".repeat(43))).unwrap();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", Vec::new(), 0)).await.unwrap();
        let factory = CellStoreFactory {
            directory: directory.clone(), keys, storage_namespaces, storage_regions: TaskTracker::new(),
            scoped_writes: None, hosted_task: None, local_witnesses: None, replay_broker: None, audience_source: None,
        };
        for (writer_epoch, compactor) in [(Some(0), false), (None, true)] {
            let configuration = CellStoreConfig {
                storage: storage.clone(), prefix: record.storage_prefix(), deployment: record.id.clone(),
                writer_epoch, compactor, snapshot_block: None,
            };
            let configuration = Any { type_url: CELL_STORAGE_TYPE.into(), value: serde_json::to_vec(&configuration).unwrap() };
            let result = factory.open(&configuration, &key("v06"), IoRegion::new()).await;
            assert!(matches!(result, Err(error) if error.code == "rtdb.multi/terminal-storage"));
            assert!(directory.deployment(&record.id).await.unwrap().unwrap().storage_regions.is_empty());
        }
    }

    #[tokio::test]
    async fn an_attach_after_the_deletion_fence_does_not_create_a_root_marker() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let root = tempfile::tempdir().unwrap();
        let storage = crate::deployment::CellStorage::Local { root: root.path().join("objects") };
        let keys = Arc::new(crate::keys::LocalKeyService::new(root.path().join("keys")).unwrap());
        let bucket = storage.bucket(IoRegion::new()).await.unwrap();
        let storage_namespaces = crate::deployment::storage_namespaces(bucket.as_ref(), keys.as_ref()).await.unwrap();
        let mut deleting = DeploymentRecord::new("shop", "acme", Vec::new(), 0);
        deleting.lifecycle = Lifecycle::Deleting;
        let deleting = directory.create_deployment(deleting).await.unwrap();
        let config = CellStoreConfig { storage, prefix: deleting.storage_prefix(), deployment: deleting.id, writer_epoch: None, compactor: false, snapshot_block: None };
        let factory = CellStoreFactory { directory, keys, storage_namespaces, storage_regions: TaskTracker::new(), scoped_writes: None, hosted_task: None, local_witnesses: None, replay_broker: None, audience_source: None };
        let config = Any { type_url: CELL_STORAGE_TYPE.into(), value: serde_json::to_vec(&config).unwrap() };
        let result = factory.open(&config, &key("v06"), IoRegion::new()).await;
        assert!(matches!(result, Err(error) if error.code == rtdb_node::NOT_HELD));
        assert_eq!(bucket.list(None).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn only_the_current_pristine_writer_can_enroll_a_missing_serving_key() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let root = tempfile::tempdir().unwrap();
        let storage = CellStorage::Local { root: root.path().join("objects") };
        let keys = Arc::new(crate::keys::LocalKeyService::new(root.path().join("keys")).unwrap());
        let bucket = storage.bucket(IoRegion::new()).await.unwrap();
        let storage_namespaces = crate::deployment::storage_namespaces(bucket.as_ref(), keys.as_ref()).await.unwrap();
        let factory = CellStoreFactory {
            directory: directory.clone(), keys: keys.clone(), storage_namespaces,
            storage_regions: TaskTracker::new(), scoped_writes: None, hosted_task: None, local_witnesses: None, replay_broker: None, audience_source: None,
        };
        let mut record = DeploymentRecord::new("shop", "acme", Vec::new(), 0);
        record.epoch = 2;
        record.holder = Some(MemberId("writer".into()));
        let record = directory.create_deployment(record).await.unwrap();
        let configuration = |writer_epoch| Any { type_url: CELL_STORAGE_TYPE.into(), value: serde_json::to_vec(&CellStoreConfig {
            storage: storage.clone(), prefix: record.storage_prefix(), deployment: record.id.clone(),
            writer_epoch, compactor: false, snapshot_block: None,
        }).unwrap() };
        let cached: Arc<dyn ObjectStore> = Arc::new(MemoryStore::default());
        for writer_epoch in [None, Some(1)] {
            let result = factory.wrap_stores(&configuration(writer_epoch), cached.clone(), None, None).await;
            assert!(matches!(result, Err(error) if error.code == "rtdb.multi/key-missing"));
        }
        assert_eq!(keys.existing_key(&record.key_id()).await.unwrap(), None);
        factory.wrap_stores(&configuration(Some(2)), cached.clone(), None, None).await.unwrap();
        assert!(keys.existing_key(&record.key_id()).await.unwrap().is_some());
        factory.wrap_stores(&configuration(None), cached.clone(), None, None).await.unwrap();

        std::fs::remove_file(root.path().join("keys").join(format!("{}.key", record.key_id()))).unwrap();
        crate::admin::update_installation(directory.as_ref(), &record.id, |record| {
            record.replica_journals.insert("replica".into());
            Ok(())
        }).await.unwrap();
        let result = factory.wrap_stores(&configuration(Some(2)), cached, None, None).await;
        assert!(matches!(result, Err(error) if error.code == "rtdb.multi/key-missing"));
        assert_eq!(keys.existing_key(&record.key_id()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn audience_storage_reads_uncached_objects_for_the_exact_writer() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let root = tempfile::tempdir().unwrap();
        let local = CellStorage::Local { root: root.path().join("objects") };
        let keys = Arc::new(crate::keys::LocalKeyService::new(root.path().join("keys")).unwrap());
        let bucket = local.bucket(IoRegion::new()).await.unwrap();
        let storage_namespaces = crate::deployment::storage_namespaces(bucket.as_ref(), keys.as_ref()).await.unwrap();
        let mut record = DeploymentRecord::new("shop", "acme", Vec::new(), 0);
        record.epoch = 2;
        record.holder = Some(MemberId("writer".into()));
        record.linearizable_reads = true;
        let record = directory.create_deployment(record).await.unwrap();
        let factory = CellStoreFactory {
            directory, keys: keys.clone(), storage_namespaces, storage_regions: TaskTracker::new(),
            scoped_writes: None, hosted_task: None, local_witnesses: None, replay_broker: None,
            audience_source: Some(Uuid::parse_str(&record.id).unwrap()),
        };
        let storage: CellStorage = serde_json::from_value(serde_json::json!({
            "kind": "s3",
            "config": { "bucket": "cell-objects" }
        })).unwrap();
        let configuration = |writer_epoch| Any { type_url: CELL_STORAGE_TYPE.into(), value: serde_json::to_vec(&CellStoreConfig {
            storage: storage.clone(), prefix: record.storage_prefix(), deployment: record.id.clone(),
            writer_epoch, compactor: false, snapshot_block: None,
        }).unwrap() };
        let cached: Arc<dyn ObjectStore> = Arc::new(MemoryStore::default());
        let uncached: Arc<dyn ObjectStore> = Arc::new(MemoryStore::default());
        let stores = factory.wrap_stores(&configuration(Some(2)), cached.clone(), Some(uncached.clone()), None).await.unwrap();
        let audience = stores.audience.unwrap();
        assert_eq!(audience.source, Uuid::parse_str(&record.id).unwrap());
        let publications = directory_publications(keys.as_ref(), &record, IoRegion::new()).unwrap();
        let object = key("blocks/b00.binpb");
        let bytes = Bytes::from_static(b"catalog");
        EncryptingStore::published(uncached.clone(), publications).put(&object, bytes.clone()).await.unwrap();
        assert!(matches!(stores.ordinary.get(&object).await, Err(Error::MissingObject(_))));
        let allocation = audience.store.whole_object_allocation(&object, bytes.len()).unwrap();
        let peak = allocation.retained_bytes + allocation.scratch_bytes;
        let too_small = rtdb_storage::cache::BufferPool::new(audience.store.clone(), peak - 1);
        assert!(matches!(too_small.get(&object).await, Err(Error::MemoryLimit { .. })));
        let pool = rtdb_storage::cache::BufferPool::new(audience.store.clone(), peak);
        let lease = pool.get(&object).await.unwrap();
        assert_eq!(lease.as_slice(), bytes.as_ref());
        assert_eq!(pool.pool_retained_bytes(), allocation.retained_bytes);
        assert!(!pool.holds(&object));
        assert!(pool.admit(1).is_none());
        drop(lease);
        assert_eq!(pool.pool_retained_bytes(), 0);
        assert!(matches!(audience.store.get(&object).await, Err(Error::Closed(_))));

        let unsupported = factory.wrap_stores(&configuration(Some(2)), cached.clone(), None, None).await.unwrap();
        assert!(unsupported.audience.is_none());
        let stale = factory.wrap_stores(&configuration(Some(1)), cached, Some(uncached), None).await.unwrap();
        assert!(stale.audience.is_none());
    }

    #[tokio::test]
    async fn exact_encrypted_posting_reads_admit_scratch_and_require_the_recorded_size() {
        let store = Arc::new(EncryptingStore::new(Arc::new(MemoryStore::default()), &[7; 32]).unwrap());
        let key = key("tables/orders/artefacts/l00-b00/spatial/geo-v1.bin");
        let bytes = Bytes::from_static(b"spatial postings");
        store.put(&key, bytes.clone()).await.unwrap();
        let allocation = store.exact_object_allocation(&key, bytes.len()).unwrap();
        let peak = allocation.retained_bytes + allocation.scratch_bytes;
        let fallback = rtdb_columnar::MemoryBudget::new(0);
        let too_small = rtdb_storage::cache::BufferPool::new(store.clone(), peak - 1);
        assert!(matches!(too_small.get_exact_or(&key, bytes.len(), &fallback).await, Err(Error::MemoryLimit { .. })));
        let pool = rtdb_storage::cache::BufferPool::new(store, peak);
        assert_eq!(pool.get_exact_or(&key, bytes.len(), &fallback).await.unwrap().as_slice(), bytes.as_ref());
        assert!(pool.get_exact_or(&key, bytes.len() - 1, &fallback).await.is_err());
        assert_eq!(pool.pool_retained_bytes(), 0);
    }

    #[tokio::test]
    async fn a_compactor_follows_writer_changes_and_refuses_a_deleting_prefix() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", Vec::new(), 0)).await.unwrap();
        let store = FencedStore {
            inner: Arc::new(MemoryStore::default()),
            fence: Fence::Compactor,
            deployment: record.id.clone(),
            prefix: record.storage_prefix(),
            directory: directory.clone(),
            local_publications: None,
            _release: None,
        };
        store.put(&key("tables/orders/data/l01-before"), Bytes::from_static(b"data")).await.unwrap();
        let mut next = record.clone();
        next.epoch += 1;
        let next = directory.replace_deployment(next, record.version).await.unwrap().applied().unwrap();
        store.put(&key("tables/orders/data/l01-after-move"), Bytes::from_static(b"data")).await.unwrap();
        let mut deleting = next.clone();
        deleting.lifecycle = Lifecycle::Deleting;
        directory.replace_deployment(deleting, next.version).await.unwrap().applied().unwrap();
        assert!(matches!(store.put(&key("tables/orders/data/l01-after-fence"), Bytes::from_static(b"data")).await, Err(Error::ReadOnlyLog(_))));
        assert_eq!(store.list(None).await.unwrap().len(), 2);
    }

    struct StoreFaults {
        inner: MemoryStore,
        fail: std::sync::atomic::AtomicBool,
        deny_list: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl ObjectStore for StoreFaults {
        async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head(key).await }
        async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.inner.get(key).await }
        async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> { self.inner.get_range(key, range).await }
        async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> {
            if self.fail.swap(false, std::sync::atomic::Ordering::Relaxed) {
                return Err(Error::Task("injected object-store write failure".into()));
            }
            self.inner.put(key, bytes).await
        }
        async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> { self.inner.put_file(key, source).await }
        async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
            if self.deny_list.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(Error::Task("listing is disabled".into()));
            }
            self.inner.list(directory).await
        }
        async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> { self.inner.delete(key).await }
    }

    #[tokio::test]
    async fn a_failed_root_marker_write_is_returned_to_activation() {
        let inner = StoreFaults { inner: MemoryStore::default(), fail: true.into(), deny_list: false.into() };
        assert_eq!(check_root(&inner, "shop", true).await.unwrap_err().code, "rtdb.storage/task");
        assert!(matches!(inner.get(&key(ROOT_MARKER)).await, Err(Error::MissingObject(_))));
        check_root(&inner, "shop", true).await.unwrap();
    }

    #[tokio::test]
    async fn a_failed_release_is_visible_and_can_be_retried() {
        let inner = Arc::new(StoreFaults { inner: MemoryStore::default(), fail: false.into(), deny_list: false.into() });
        let store = GarbageStore::open(inner.clone()).await.unwrap();
        let object = key("tables/t/data/a.arrow");
        store.put(&object, Bytes::from_static(b"rows")).await.unwrap();
        assert_eq!(store.list(None).await.unwrap().len(), 1);
        inner.fail.store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(store.delete(&object).await.is_err());
        assert_eq!(store.list(None).await.unwrap().len(), 1);
        store.delete(&object).await.unwrap();
        assert!(GarbageStore::open(inner).await.unwrap().list(None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn objects_are_unreadable_without_their_deployments_key() {
        let raw: Arc<dyn ObjectStore> = Arc::new(MemoryStore::default());
        let store = EncryptingStore::new(Arc::clone(&raw), &[7; 32]).unwrap();
        store.put(&key("blocks/b00.binpb"), Bytes::from_static(b"secret rows")).await.unwrap();
        assert_eq!(store.get(&key("blocks/b00.binpb")).await.unwrap().as_ref(), b"secret rows");
        assert_eq!(store.get_range(&key("blocks/b00.binpb"), 7..11).await.unwrap().as_ref(), b"rows");
        assert_eq!(store.head(&key("blocks/b00.binpb")).await.unwrap().size, 11, "sizes are the plaintext's");
        assert!(!raw.get(&key("blocks/b00.binpb")).await.unwrap().windows(6).any(|window| window == b"secret"), "nothing is stored in the clear");
        assert!(EncryptingStore::new(Arc::clone(&raw), &[8; 32]).unwrap().get(&key("blocks/b00.binpb")).await.is_err(), "another key cannot read it");
        store.put(&key("blocks/b00.binpb"), Bytes::from_static(b"secret rows")).await.unwrap();
    }

    #[tokio::test]
    async fn the_reaper_preserves_replica_payloads_named_by_legacy_age_based_releases() {
        let bucket = Arc::new(MemoryStore::default());
        let record = DeploymentRecord::new("shop", "acme", vec![], 0);
        let scope = format!("{}/v06", record.storage_prefix());
        let store = open_deployment_store(bucket.clone(), &scope, crate::packing::PackDiscovery::List, wall_clock()).await.unwrap();
        let payload = key("oversized/b00-payload.binpb");
        store.put(&payload, Bytes::from_static(b"retained replica payload")).await.unwrap();
        assert!(store.delete(&payload).await.is_err());

        let release = GarbageRecord { key: payload.to_string(), released_at_ms: 0, trie_floor: None };
        let marker = key(&format!("{scope}/{GARBAGE_DIRECTORY}/{:020}-{}", 0, Uuid::nil().simple()));
        bucket.put(&marker, serde_json::to_vec(&release).unwrap().into()).await.unwrap();

        let refs = crate::reference::References::default();
        assert_eq!(reap_as_of(bucket.as_ref(), &record, 1, Duration::ZERO, &refs).await.unwrap().erased, 0);
        assert_eq!(store.get(&payload).await.unwrap(), Bytes::from_static(b"retained replica payload"));
        assert!(store.list(Some(&key("oversized"))).await.unwrap().iter().any(|object| object.key == payload));
    }

    #[tokio::test]
    async fn a_node_delete_releases_an_object_and_only_the_reaper_erases_it() {
        let directory = tempfile::tempdir().unwrap();
        let record = DeploymentRecord::new("shop", "acme", vec![], 0);
        let root = directory.path().join(record.storage_prefix()).join("v06");
        let store = GarbageStore::open(Arc::new(LocalStore::open(&root).unwrap())).await.unwrap();
        store.put(&key("tables/t/data/a.arrow"), Bytes::from_static(b"a")).await.unwrap();
        store.delete(&key("tables/t/data/a.arrow")).await.unwrap();
        assert!(root.join("tables/t/data/a.arrow").exists(), "a node's delete erases nothing");
        assert!(store.list(Some(&key("tables"))).await.unwrap().is_empty(), "a released object is no longer listed");
        assert_eq!(GarbageStore::open(Arc::new(LocalStore::open(&root).unwrap())).await.unwrap().list(Some(&key("tables"))).await.unwrap().len(), 0, "and stays unlisted across a reopen");
        let bucket = LocalStore::open(directory.path()).unwrap();
        assert_eq!(reap(&bucket, &record, Duration::from_secs(3600), &Default::default()).await.unwrap(), Reaped { erased: 0, waiting: 1 }, "within the grace period it waits");
        assert_eq!(reap(&bucket, &record, Duration::ZERO, &Default::default()).await.unwrap(), Reaped { erased: 1, waiting: 0 });
        assert!(!root.join("tables/t/data/a.arrow").exists());
    }

    /// Opens the store of `record`'s storage version under `root` as a node would, references and all.
    async fn open_store(root: &Path, record: &DeploymentRecord) -> (PathBuf, crate::reference::ReferencingStore) {
        let bucket: crate::reference::Bucket = Arc::new(LocalStore::open(root).unwrap());
        let store = format!("{}/v06", record.storage_prefix());
        let own: Arc<dyn ObjectStore> = Arc::new(crate::reference::PrefixedStore::new(Arc::clone(&bucket), store.clone()));
        let base = crate::reference::Base::load(bucket.as_ref(), &store).await.unwrap();
        (root.join(&store), crate::reference::ReferencingStore::new(own, bucket, base))
    }

    #[tokio::test]
    async fn a_fork_reads_what_it_references_until_it_releases_it_and_keeps_it_when_its_parent_does() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let parent = DeploymentRecord::new("shop", "acme", vec![], 0);
        let child = DeploymentRecord::new("shop-preview", "acme", vec![], 0);
        let (parent_store, raw) = open_store(root, &parent).await;
        let parent_view = GarbageStore::open(Arc::new(raw)).await.unwrap();
        for name in ["tables/t/data/kept.arrow", "tables/t/data/parent-drops.arrow", "tables/t/data/child-drops.arrow"] { parent_view.put(&key(name), Bytes::from_static(b"rows")).await.unwrap(); }
        let bucket = LocalStore::open(root).unwrap();
        crate::reference::simulate_fork(&bucket, &parent.storage_prefix(), &child.storage_prefix(), None).await.unwrap();
        let (child_store, raw) = open_store(root, &child).await;
        let child_view = GarbageStore::open(Arc::new(raw)).await.unwrap();
        assert_eq!(child_view.list(Some(&key("tables"))).await.unwrap().len(), 3, "a fork lists what it references");
        assert_eq!(child_view.get(&key("tables/t/data/kept.arrow")).await.unwrap().as_ref(), b"rows");
        child_view.put(&key("tables/t/data/kept.arrow"), Bytes::from_static(b"rows")).await.unwrap();
        assert!(child_view.put(&key("tables/t/data/kept.arrow"), Bytes::from_static(b"other")).await.is_err(), "put-if-absent holds across a reference");

        parent_view.delete(&key("tables/t/data/parent-drops.arrow")).await.unwrap();
        child_view.delete(&key("tables/t/data/child-drops.arrow")).await.unwrap();
        let records = [parent.clone(), child.clone()];
        let references = crate::reference::References::scan(&bucket, &records).await.unwrap();
        reap(&bucket, &parent, Duration::ZERO, &references).await.unwrap();
        reap(&bucket, &child, Duration::ZERO, &references).await.unwrap();
        assert!(!parent_store.join("tables/t/data/parent-drops.arrow").exists(), "the parent's release erases its object");
        assert!(child_store.join("tables/t/data/parent-drops.arrow").exists(), "after copying it into the fork that still uses it");
        assert!(parent_store.join("tables/t/data/child-drops.arrow").exists(), "the fork's release leaves its parent's object alone");
        let (_, raw) = open_store(root, &child).await;
        let reopened = GarbageStore::open(Arc::new(raw)).await.unwrap();
        let mut listed: Vec<String> = reopened.list(Some(&key("tables"))).await.unwrap().into_iter().map(|object| object.key.as_str().to_owned()).collect();
        listed.sort();
        assert_eq!(listed, ["tables/t/data/kept.arrow", "tables/t/data/parent-drops.arrow"], "and the fork no longer references it");
    }

    #[tokio::test]
    async fn a_writer_checks_the_directory_on_every_mutation() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let mut record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        record.epoch = 1;
        record.holder = Some(rtdb_directory::MemberId("holder".into()));
        record = directory.replace_deployment(record.clone(), record.version).await.unwrap().applied().unwrap();
        let store = FencedStore {
            inner: Arc::new(MemoryStore::default()), fence: Fence::Writer { epoch: 1 },
            directory: directory.clone(), deployment: record.id.clone(), prefix: record.storage_prefix(), local_publications: None, _release: None,
        };
        store.put(&key("blocks/a"), Bytes::from_static(b"a")).await.unwrap();
        record.epoch = 2;
        directory.replace_deployment(record.clone(), record.version).await.unwrap().applied().unwrap();
        assert!(matches!(store.put(&key("blocks/b"), Bytes::from_static(b"b")).await, Err(Error::Deposed { epoch: 1, promised: 2 })));
    }

    #[tokio::test]
    async fn a_holders_store_refuses_writes_once_the_directory_moves_past_its_epoch() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        record.epoch = 1;
        record.holder = Some(rtdb_directory::MemberId("holder".into()));
        let mut record = directory.create_deployment(record).await.unwrap();
        let store = FencedStore {
            inner: Arc::new(MemoryStore::default()), fence: Fence::Writer { epoch: 1 },
            directory: directory.clone(), deployment: record.id.clone(), prefix: record.storage_prefix(), local_publications: None, _release: None,
        };
        store.put(&key("blocks/a"), Bytes::from_static(b"a")).await.unwrap();
        record.epoch = 2;
        directory.replace_deployment(record.clone(), record.version).await.unwrap();
        assert!(matches!(store.put(&key("blocks/b"), Bytes::from_static(b"b")).await, Err(Error::Deposed { epoch: 1, promised: 2 })), "a deposed holder writes nothing");
        assert!(store.delete(&key("blocks/a")).await.is_err(), "and releases nothing");
        assert_eq!(store.get(&key("blocks/a")).await.unwrap().as_ref(), b"a", "reads still serve its snapshots");
    }

    #[tokio::test]
    async fn pack_members_follow_the_exact_provider_attempt() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let region = Uuid::new_v4();
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        record.storage_regions.insert(region, StorageRegion::Remote {
            tracks_ordered_index_pages: false, checkpoint_metadata_from: None, checkpoint_replay_from: None,
            archive: None, archive_source: None, checkpoint_suffix: None, repair_archive: None,
            owner: None, closed: false, publications: BTreeMap::new(),
        });
        let record = directory.create_deployment(record).await.unwrap();
        let journal = DirectoryPublications {
            directory: directory.clone(), deployment: record.id.clone(), region,
            writer_epoch: None, owner: PublicationOwner::Serving,
        };
        let root = format!("{}/v06", record.storage_prefix());
        let group = key(&format!("{root}/~packs/b00.pack"));
        let other = key(&format!("{root}/~packs/b01.pack"));
        let mut members = BTreeMap::new();
        for name in ["blocks/b00.binpb", "tables/abc/blocks/b00.binpb"] {
            let unit = rtdb_types::ServingPublication {
                origin: UnitScope::publication_origin(&record.storage_prefix()), attempt: Uuid::new_v4(),
            };
            journal.allocate_unit(&key(&format!("{root}/{name}")), &key(name), unit).await.unwrap();
            members.insert(name.to_owned(), Bytes::copy_from_slice(&unit.prefix()));
        }
        let (bytes, _) = crate::packing::encode_pack(&members, vec![], 0);
        let attempt = journal.unit_attempt(&group, &bytes).await.unwrap().unwrap();
        assert_eq!(journal.unit_attempt(&group, &bytes).await.unwrap(), Some(attempt));
        assert!(journal.unit_attempt(&other, &bytes).await.is_err());
        journal.admit(&group, attempt, "exact-upload").await.unwrap();
        assert_eq!(journal.active_unit_upload(&group, attempt).await.unwrap().as_deref(), Some("exact-upload"));
        assert!(journal.admit(&group, attempt, "second-upload").await.is_err());
        journal.unit_outcome(&group, attempt, "exact-upload", true).await.unwrap();
        let current = directory.deployment(&record.id).await.unwrap().unwrap();
        assert!(matches!(&current.storage_regions[&region], StorageRegion::Remote { publications, .. } if publications.is_empty()));
    }

    #[tokio::test]
    async fn a_compactors_store_refuses_outputs_after_disposal_is_ordered() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let installation = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let store = FencedStore {
            inner: Arc::new(MemoryStore::default()),
            directory: directory.clone(),
            deployment: installation.id.clone(),
            prefix: installation.storage_prefix(),
            fence: Fence::Compactor,
            local_publications: None,
            _release: None,
        };
        let output = key("tables/t/data/l01-first.arrow");
        store.put(&output, Bytes::from_static(b"first")).await.unwrap();

        crate::admin::begin_disposal(directory.as_ref(), &installation.id).await.unwrap();
        let late_output = key("tables/t/data/l01-late.arrow");
        assert!(matches!(
            store.put(&late_output, Bytes::from_static(b"late")).await,
            Err(Error::ReadOnlyLog(_))
        ));
    }

    #[tokio::test]
    async fn retirement_waits_for_storage_work_after_its_caller_is_cancelled() {
        let files = tempfile::tempdir().unwrap();
        let keys = Arc::new(crate::keys::LocalKeyService::new(files.path().join("keys")).unwrap());
        let storage = crate::deployment::CellStorage::Local { root: files.path().join("objects") };
        let directory = rtdb_directory::MemoryDirectory::new();
        let bucket = storage.bucket(IoRegion::new()).await.unwrap();
        let storage_namespaces = crate::deployment::storage_namespaces(bucket.as_ref(), keys.as_ref()).await.unwrap();
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        record.storage_namespaces = Some(storage_namespaces);
        let record = directory.create_deployment(record).await.unwrap();
        let key = keys.key(&record.key_id()).await.unwrap();
        let factory = CellStoreFactory {
            directory: directory.clone(),
            keys: keys.clone(),
            storage_regions: TaskTracker::new(),
            local_witnesses: None, replay_broker: None, audience_source: None,
            storage_namespaces,
            scoped_writes: None,
            hosted_task: None,
        };
        let config = CellStoreConfig {
            storage: storage.clone(),
            prefix: record.storage_prefix(),
            deployment: record.id.clone(),
            writer_epoch: None,
            compactor: false,
            snapshot_block: None,
        };
        let (admitted, region, _, attachment, _metadata) = factory.admit_region(&config, IoRegion::new(), true).await.unwrap();
        assert_eq!(admitted.storage_regions.len(), 1);

        let (entered, active) = oneshot::channel();
        let (complete, wait) = std::sync::mpsc::channel();
        let caller = tokio::spawn(async move {
            region.run_blocking(move || {
                entered.send(()).unwrap();
                wait.recv().unwrap();
                Ok(())
            }).await
        });
        active.await.unwrap();
        caller.abort();
        let _ = caller.await;
        drop(attachment);
        crate::admin::begin_disposal(directory.as_ref(), &record.id).await.unwrap();
        let pending = crate::admin::finish_disposal(directory.as_ref(), &storage, keys.as_ref(), &record.id, 4 << 20)
            .await.unwrap_err();
        assert_eq!(pending.code, "rtdb.multi/storage-not-drained");
        assert_eq!(keys.key(&record.key_id()).await.unwrap(), key);
        assert!(factory.admit_region(&config, IoRegion::new(), true).await.is_err());

        complete.send(()).unwrap();
        factory.storage_regions.close();
        factory.storage_regions.wait().await;
        let receipt = crate::admin::finish_disposal(directory.as_ref(), &storage, keys.as_ref(), &record.id, 4 << 20)
            .await.unwrap();
        assert_eq!(receipt.deployment, record.id);
        assert!(keys.key(&record.key_id()).await.is_err());
    }

    #[tokio::test]
    async fn a_hosted_path_sweep_withdraws_inherited_names_and_preserves_provider_objects() {
        let bucket: crate::reference::Bucket = Arc::new(MemoryStore::default());
        let parent = DeploymentRecord::new("shop", "acme", vec![], 0);
        let child = DeploymentRecord::new("preview", "acme", vec![], 0);
        let parent_store = format!("{}/v06", parent.storage_prefix());
        let child_store = format!("{}/v06", child.storage_prefix());
        let parent_object = |name| key(&format!("{parent_store}/objects/{name}"));
        for name in ["inherited", "shadowed"] {
            bucket.put(&parent_object(name), Bytes::from_static(b"parent")).await.unwrap();
        }
        crate::reference::simulate_fork(bucket.as_ref(), &parent.storage_prefix(), &child.storage_prefix(), None).await.unwrap();
        let own: crate::reference::Bucket = Arc::new(crate::reference::PrefixedStore::new(bucket.clone(), &child_store));
        let writer = GarbageStore::open(own.clone()).await.unwrap();
        for name in ["shadowed", "owned"] {
            writer.put(&key(&format!("objects/{name}")), Bytes::from_static(b"child")).await.unwrap();
        }
        for name in ["inherited", "shadowed", "owned"] {
            writer.delete(&key(&format!("objects/{name}"))).await.unwrap();
        }
        let markers = own.list(Some(&key(GARBAGE_DIRECTORY))).await.unwrap();
        assert_eq!(markers.len(), 3);

        for outcome in reaper_pass(bucket.as_ref(), &[parent, child], i64::MAX, Duration::ZERO).await.unwrap() {
            outcome.unwrap();
        }
        let base = crate::reference::Base::load(bucket.as_ref(), &child_store).await.unwrap();
        assert_eq!(base.objects, BTreeMap::from([("objects/shadowed".to_owned(), parent_store.clone())]));
        assert_eq!(bucket.get(&parent_object("inherited")).await.unwrap(), Bytes::from_static(b"parent"));
        assert_eq!(own.get(&key("objects/shadowed")).await.unwrap(), Bytes::from_static(b"child"));
        assert_eq!(own.get(&key("objects/owned")).await.unwrap(), Bytes::from_static(b"child"));
        for marker in markers {
            assert!(own.head(&marker.key).await.is_ok());
        }
        let manifests = key(&format!("{child_store}/{}", crate::reference::BASE_MANIFEST));
        assert_eq!(bucket.list(Some(&manifests)).await.unwrap().len(), 2);
        let reopened = GarbageStore::open(own).await.unwrap();
        assert!(reopened.list(Some(&key("objects"))).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_remote_region_records_the_write_before_submission_and_keeps_it_after_close() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let region = Uuid::new_v4();
        let attempt = Uuid::new_v4();
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        record.storage_regions.insert(region, remote_storage_region(None, None));
        let record = directory.create_deployment(record).await.unwrap();
        let object = key(&format!("{}/v06/data", record.storage_prefix()));
        let journal = DirectoryPublications {
            directory: directory.clone(), deployment: record.id.clone(), region, writer_epoch: Some(record.epoch), owner: PublicationOwner::Serving,
        };
        assert!(journal.admit(&key("another-installation/data"), attempt, "upload").await.is_err());
        journal.admit(&object, attempt, "upload").await.unwrap();
        let before = directory.deployment(&record.id).await.unwrap().unwrap();
        assert_eq!(before.storage_regions[&region], StorageRegion::Remote {
            archive_source: None,
            tracks_ordered_index_pages: true,
            checkpoint_metadata_from: None, checkpoint_replay_from: None,
            archive: None, checkpoint_suffix: None, repair_archive: None, owner: None, closed: false, publications: BTreeMap::from([(attempt, StoragePublication { key: object.to_string(), upload_id: "upload".to_owned(), unit: None, rejected: false, container: None })]),
        });
        release_region(directory.as_ref(), &record.id, region, false).await.unwrap();
        release_region(directory.as_ref(), &record.id, region, false).await.unwrap();
        assert!(journal.admit(&object, Uuid::new_v4(), "upload").await.is_err());
        let retained = directory.deployment(&record.id).await.unwrap().unwrap();
        assert_eq!(retained.storage_regions[&region], StorageRegion::Remote {
            archive_source: None,
            tracks_ordered_index_pages: true,
            checkpoint_metadata_from: None, checkpoint_replay_from: None,
            archive: None, checkpoint_suffix: None, repair_archive: None, owner: None, closed: true, publications: BTreeMap::from([(attempt, StoragePublication { key: object.to_string(), upload_id: "upload".to_owned(), unit: None, rejected: false, container: None })]),
        });
    }

    #[tokio::test]
    async fn a_recovered_hosted_fork_aborts_its_exact_upload_before_disposal() {
        use rtdb_directory::{DeletionObservation, ForkPreparation, ForkPublication};
        use rtdb_storage::remote::PublicationUploads;

        let Ok(endpoint) = std::env::var("RTDB_S3_ENDPOINT") else { return };
        if !endpoint.starts_with("http://127.0.0.1:") { return; }
        let bucket_name = std::env::var("RTDB_S3_BUCKET").expect("a fixture bucket");
        let mut config = rtdb_s3::S3Config::new(bucket_name);
        config.endpoint = Some(endpoint);
        config.region = Some("us-east-1".into());
        config.path_style_access_enabled = true;
        config.prefix = Some(format!("fork-upload-{}", Uuid::new_v4()));
        let mut wrong_config = config.clone();
        wrong_config.prefix = Some(format!("other-storage-{}", Uuid::new_v4()));
        let wrong_storage = CellStorage::S3 { config: wrong_config };
        let root = CellStorage::s3_root(&config).unwrap();
        let inventory = CellStorage::s3_inventory(&config).await.unwrap();
        let storage = CellStorage::S3 { config };
        let bucket = storage.bucket(IoRegion::new()).await.unwrap();
        let data = tempfile::tempdir().unwrap();
        let keys = crate::keys::LocalKeyService::new(data.path()).unwrap();

        let directory = rtdb_directory::MemoryDirectory::new();
        let mut parent = DeploymentRecord::new("shop", "acme", vec![], 0);
        parent.storage_namespaces = Some(crate::deployment::storage_namespaces(bucket.as_ref(), &keys).await.unwrap());
        let parent = directory.create_deployment(parent).await.unwrap();
        let mut child = DeploymentRecord::new("preview", "acme", vec![], 0);
        child.parent = Some(parent.id.clone());
        child.lineage.clone_from(&parent.lineage);
        child.storage_namespaces = parent.storage_namespaces;
        let preparation = ForkPreparation { token: Uuid::new_v4(), generation: 0 };
        directory.change_lineage(&parent.lineage, LineageChange::PrepareHostedFork { preparation, task: None }).await.unwrap();
        directory.stage_fork(child.clone(), preparation).await.unwrap();
        let prefix = key(&child.storage_prefix());
        let object = key(&format!("{prefix}/v06/rows"));
        let target = format!("{root}/{object}").into();
        let attempt = Uuid::new_v4();
        let upload = inventory.reserve(&target, attempt).await.unwrap();
        let journal = DirectoryPublications {
            directory: directory.clone(), deployment: child.id.clone(), region: preparation.token,
            writer_epoch: None, owner: PublicationOwner::HostedFork,
        };
        assert!(journal.admit(&key(&format!("{}/v06/rows", parent.storage_prefix())), attempt, &upload).await.is_err());
        journal.admit(&object, attempt, &upload).await.unwrap();
        assert_eq!(directory.publish_fork(child.clone(), parent.version, Some(ForkPublication::Hosted(preparation))).await.unwrap_err().code,
            "rtdb.directory/fork-stage-changed");

        directory.change_lineage(&parent.lineage, LineageChange::ReleaseFork { preparation }).await.unwrap();
        let stage = directory.deployment(&child.id).await.unwrap().unwrap();
        crate::admin::close_abandoned_fork(directory.as_ref(), &stage).await.unwrap();
        let closed = directory.deployment(&child.id).await.unwrap().unwrap();
        assert!(matches!(&closed.storage_regions[&preparation.token], StorageRegion::Remote { closed: true, publications, .. }
            if publications[&attempt].upload_id == upload));
        assert!(journal.admit(&object, Uuid::new_v4(), &upload).await.is_err());
        assert_eq!(reconcile_storage_regions(directory.as_ref(), &wrong_storage, &keys, &closed).await.unwrap_err().code,
            "rtdb.multi/storage-region");
        assert!(!inventory.is_empty(&prefix).await.unwrap());

        let finish = || crate::admin::finish_disposal(directory.as_ref(), &storage, &keys, &child.id, 4 << 20);
        assert_eq!(finish().await.unwrap_err().code, "rtdb.multi/storage-lease-active");
        assert!(directory.deployment(&child.id).await.unwrap().unwrap().storage_regions.is_empty());
        assert!(inventory.is_empty(&prefix).await.unwrap());
        let replace_observation = |observation| crate::admin::update_installation(directory.as_ref(), &child.id, move |current| {
            current.deletion_observation = Some(observation);
            Ok(())
        });
        replace_observation(DeletionObservation::Fenced { leases_expire_at_ms: 0 }).await.unwrap();
        assert_eq!(finish().await.unwrap_err().code, "rtdb.multi/deletion-verification-pending");
        let horizon = (rtdb_s3::SCOPED_S3_WRITE_TTL + rtdb_s3::SCOPED_S3_CLOCK_ALLOWANCE).as_millis() as i64;
        replace_observation(DeletionObservation::Empty { observed_at_ms: now_ms() - horizon }).await.unwrap();
        let receipt = finish().await.unwrap();
        assert_eq!(receipt.deployment, child.id);
        assert!(inventory.complete(&target, &upload, Bytes::from_static(b"late publication")).await.is_err());
        assert!(inventory.is_empty(&prefix).await.unwrap());
    }

    #[tokio::test]
    async fn ordered_index_reaper_aborts_a_superseded_provider_upload() {
        use rtdb_directory::OrderedIndexStorage;
        use rtdb_storage::remote::PublicationUploads;

        let Ok(endpoint) = std::env::var("RTDB_S3_ENDPOINT") else { return };
        if !endpoint.starts_with("http://127.0.0.1:") { return; }
        let bucket_name = std::env::var("RTDB_S3_BUCKET").expect("a versioned fixture bucket");
        let mut config = rtdb_s3::S3Config::new(&bucket_name);
        config.endpoint = Some(endpoint);
        config.region = Some("us-east-1".into());
        config.path_style_access_enabled = true;
        config.prefix = Some(format!("ordered-index-upload-{}", Uuid::new_v4()));
        let root = CellStorage::s3_root(&config).unwrap();
        let inventory = CellStorage::s3_inventory(&config).await.unwrap();
        let storage = CellStorage::S3 { config };
        let bucket = storage.bucket(IoRegion::new()).await.unwrap();

        let directory = rtdb_directory::MemoryDirectory::new();
        let vault = tempfile::tempdir().unwrap();
        let keys = crate::keys::LocalKeyService::new(vault.path()).unwrap();
        let region = Uuid::new_v4();
        let attempt = Uuid::new_v4();
        let mut record = DeploymentRecord::new("shop", "acme", Vec::new(), 0);
        record.epoch = 1;
        record.storage_regions.insert(region, remote_storage_region(None, None));
        let index = format!(
            "{}/v06/{}/0000000000000001-{}", record.storage_prefix(),
            rtdb_format::keys::ORDERED_INDEX_ROOT, "a".repeat(64),
        );
        record.ordered_indexes.insert(index.clone(), OrderedIndexStorage {
            generations: BTreeMap::from([(0, BTreeMap::from([(Uuid::new_v4(), region)]))]),
            ..Default::default()
        });
        let record = directory.create_deployment(record).await.unwrap();
        let old = key(&format!("{index}/generations/0000000000000000"));
        let replacement = key(&format!("{index}/generations/0000000000000002"));
        let object = key(&format!("{old}/pages/pending"));
        let upload_path = format!("{root}/{object}").into();
        let upload = inventory.reserve(&upload_path, attempt).await.unwrap();
        let journal = DirectoryPublications {
            directory: directory.clone(), deployment: record.id.clone(), region,
            writer_epoch: Some(record.epoch), owner: PublicationOwner::Serving,
        };
        journal.admit(&object, attempt, &upload).await.unwrap();
        bucket.put(&key(&format!("{old}/pages/visible")), Bytes::from_static(b"old")).await.unwrap();
        bucket.put(&key(&format!("{replacement}/pages/visible")), Bytes::from_static(b"replacement")).await.unwrap();

        let closed = crate::admin::update_installation(directory.as_ref(), &record.id, |current| {
            current.epoch = 2;
            current.ordered_indexes.get_mut(&index).unwrap().retire_earlier(2);
            Ok(())
        }).await.unwrap();
        assert!(journal.admit(&object, Uuid::new_v4(), &upload).await.is_err());
        ordered_index_storage::reap(directory.as_ref(), &storage, bucket.as_ref(), &closed, &keys).await.unwrap();
        assert!(!inventory.is_empty(&old).await.unwrap());

        release_region(directory.as_ref(), &record.id, region, false).await.unwrap();
        let drained = directory.deployment(&record.id).await.unwrap().unwrap();
        ordered_index_storage::reap(directory.as_ref(), &storage, bucket.as_ref(), &drained, &keys).await.unwrap();
        assert!(inventory.complete(&upload_path, &upload, Bytes::from_static(b"late")).await.is_err());
        assert!(inventory.is_empty(&old).await.unwrap());
        assert!(!inventory.is_empty(&replacement).await.unwrap());
        let reaped = directory.deployment(&record.id).await.unwrap().unwrap();
        assert!(!reaped.ordered_indexes[&index].generations.contains_key(&0));
        assert!(matches!(&reaped.storage_regions[&region], StorageRegion::Remote { publications, .. }
            if !publications.contains_key(&attempt)));
        inventory.purge(&replacement).await.unwrap();
    }

    #[tokio::test]
    async fn the_local_reaper_recovers_a_region_after_its_owner_drops_the_witness() {
        let files = tempfile::tempdir().unwrap();
        let keys = crate::keys::LocalKeyService::new(files.path().join("keys")).unwrap();
        let storage = crate::deployment::CellStorage::Local { root: files.path().join("objects") };
        let directory = rtdb_directory::MemoryDirectory::new();
        let region = Uuid::new_v4();
        let witness = keys.hold_local_storage_region(region).unwrap().unwrap();
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        let bucket = storage.bucket(IoRegion::new()).await.unwrap();
        record.storage_namespaces = Some(crate::deployment::storage_namespaces(bucket.as_ref(), &keys).await.unwrap());
        record.storage_regions.insert(region, StorageRegion::Local { archive: None, checkpoint_suffix: None, repair_archive: None, tracks_ordered_index_pages: true, checkpoint_metadata_from: None, checkpoint_replay_from: None, });
        let record = directory.create_deployment(record).await.unwrap();
        crate::admin::begin_disposal(directory.as_ref(), &record.id).await.unwrap();

        let pending = crate::admin::finish_disposal(directory.as_ref(), &storage, &keys, &record.id, 4 << 20)
            .await.unwrap_err();
        assert_eq!(pending.code, "rtdb.multi/storage-not-drained");
        drop(witness);
        let receipt = crate::admin::finish_disposal(directory.as_ref(), &storage, &keys, &record.id, 4 << 20)
            .await.unwrap();
        assert_eq!(receipt.deployment, record.id);
        assert!(!keys.local_storage_region_drained(region).unwrap(), "the consumed witness is removed");
    }

    #[tokio::test]
    async fn the_local_reaper_leaves_other_recovery_holds_in_place() {
        let files = tempfile::tempdir().unwrap();
        let keys = crate::keys::LocalKeyService::new(files.path().join("keys")).unwrap();
        let storage = CellStorage::Local { root: files.path().join("objects") };
        let directory = rtdb_directory::MemoryDirectory::new();
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        let stopped = Uuid::new_v4();
        let running = Uuid::new_v4();
        let stopped_witness = keys.hold_local_storage_region(stopped).unwrap().unwrap();
        let running_witness = keys.hold_local_storage_region(running).unwrap().unwrap();
        for id in [stopped, running] {
            record.storage_regions.insert(id, StorageRegion::ReplicaReplay { first_required: 5, through: 5, custody: rtdb_directory::ReplayCustody::Local });
        }
        let record = directory.create_deployment(record).await.unwrap();
        drop(stopped_witness);
        reconcile_storage_regions(directory.as_ref(), &storage, &keys, &record).await.unwrap();
        let current = directory.deployment(&record.id).await.unwrap().unwrap();
        assert_eq!(current.storage_regions.len(), 1);
        assert!(current.storage_regions.contains_key(&running));
        assert!(!keys.local_storage_region_drained(stopped).unwrap());
        drop(running_witness);
        reconcile_storage_regions(directory.as_ref(), &storage, &keys, &current).await.unwrap();
        assert!(directory.deployment(&record.id).await.unwrap().unwrap().storage_regions.is_empty());
    }

    #[tokio::test]
    async fn the_reaper_erases_a_pack_once_every_object_in_it_is_released() {
        let directory = tempfile::tempdir().unwrap();
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        record.checkpoint = Some(rtdb_directory::Checkpoint { block: 1, boundary: 2, source: 1, system_time: 0, root_sha256: None });
        let root = directory.path().join(record.storage_prefix()).join("v06");
        let local: Arc<dyn ObjectStore> = Arc::new(LocalStore::open(&root).unwrap());
        let store = GarbageStore::open(crate::packing::PackingStore::open(local, crate::packing::PackDiscovery::List).await.unwrap()).await.unwrap();
        for name in ["tables/t/data/l00-rc-b00.arrow", "tables/t/meta/l00-rc-b00.arrow"] { store.put(&key(name), Bytes::from_static(b"x")).await.unwrap(); }
        store.put(&key("blocks/b00.binpb"), Bytes::from_static(b"catalog")).await.unwrap();
        store.put(&key("blocks/b01.binpb"), Bytes::from_static(b"catalog")).await.unwrap();
        let packs = || std::fs::read_dir(root.join(crate::packing::PACK_DIRECTORY)).map(|entries| entries.count()).unwrap_or(0);
        assert_eq!(packs(), 2, "each block has its pack");
        store.delete(&key("tables/t/data/l00-rc-b00.arrow")).await.unwrap();
        let bucket = LocalStore::open(directory.path()).unwrap();
        assert_eq!(reap(&bucket, &record, Duration::ZERO, &Default::default()).await.unwrap(), Reaped { erased: 0, waiting: 1 }, "half a pack released keeps the pack");
        assert_eq!(packs(), 2);
        store.delete(&key("tables/t/meta/l00-rc-b00.arrow")).await.unwrap();
        store.delete(&key("blocks/b00.binpb")).await.unwrap();
        assert_eq!(reap(&bucket, &record, Duration::ZERO, &Default::default()).await.unwrap().erased, 3);
        assert_eq!(packs(), 1, "a fully released pack is erased, and the latest block's, which names every pack, stays");
    }

    #[test]
    fn checkpoint_startup_refuses_a_trim_authorization_ahead_of_its_receipt() {
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        record.checkpoint = Some(rtdb_directory::Checkpoint {
            block: 4, boundary: 5, source: 1, system_time: 0, root_sha256: Some([42; 32]),
        });
        record.replica_anchor = Some(Box::new(rtdb_directory::JournalAnchor { root_incarnation: 1, interval: [0, 5], digest: [1; 32] }));
        assert_eq!(checkpoint_startup_replay(&record).unwrap(), Some(6));
        record.replica_anchor.as_mut().unwrap().interval[1] = 6;
        let error = checkpoint_startup_replay(&record).unwrap_err();
        assert_eq!(error.code, "rtdb.storage/log-range-unavailable", "a committed trim intent excludes this startup receipt");
        assert_eq!(error.context["requested"], 6);
        assert_eq!(error.context["first-retained"], 7);
    }

    #[tokio::test]
    async fn an_admitted_checkpoint_keeps_metadata_and_replay_until_provider_reads_drain() {
        use rtdb_directory::{Checkpoint, MemoryDirectory};
        use rtdb_replication::BlockDiscovery;
        use rtdb_types::BlockIndex;

        let files = tempfile::tempdir().unwrap();
        let keys = Arc::new(crate::keys::LocalKeyService::new(files.path().join("keys")).unwrap());
        let storage = CellStorage::Local { root: files.path().join("objects") };
        let directory = MemoryDirectory::new();
        let io = IoRegion::new();
        let bucket = storage.bucket(IoRegion::new()).await.unwrap();
        let storage_namespaces = crate::deployment::storage_namespaces(bucket.as_ref(), keys.as_ref()).await.unwrap();
        let checkpoint = |block| Checkpoint { block, boundary: block + 1, source: block, system_time: 0, root_sha256: Some([42; 32]) };
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        record.storage_namespaces = Some(storage_namespaces);
        record.checkpoint = Some(checkpoint(0));
        let record = directory.create_deployment(record).await.unwrap();
        let scope = format!("{}/v06", record.storage_prefix());
        let physical = |relative| key(&format!("{scope}/{relative}"));
        let core = "blocks/b00.binpb";
        let packed = "tables/packed/blocks/b00.binpb";
        let loose = "tables/loose/blocks/b00.binpb";
        let spill = "~packs/p000.pack";
        let writer = open_deployment_store(bucket.clone(), &scope, crate::packing::PackDiscovery::List, wall_clock()).await.unwrap();
        writer.put(&key(core), Bytes::from_static(b"root")).await.unwrap();
        let (bytes, _) = crate::packing::encode_pack(&BTreeMap::from([(packed.to_owned(), Bytes::from_static(b"table"))]), vec![], 0);
        bucket.put(&physical(spill), bytes).await.unwrap();
        bucket.put(&physical(loose), Bytes::from_static(b"loose")).await.unwrap();
        let writer = open_deployment_store(bucket.clone(), &scope, crate::packing::PackDiscovery::List, wall_clock()).await.unwrap();
        writer.put(&key("blocks/b01.binpb"), Bytes::from_static(b"new root")).await.unwrap();
        for name in [core, packed, loose] { writer.delete(&key(name)).await.unwrap(); }

        let tasks = TaskTracker::new();
        let storage_factory = CellStoreFactory {
            directory: directory.clone(), keys, storage_namespaces, hosted_task: None,
            storage_regions: tasks.clone(), scoped_writes: None, local_witnesses: None, replay_broker: None, audience_source: None,
        };
        let configuration = |snapshot_block| Any { type_url: String::new(), value: serde_json::to_vec(&CellStoreConfig {
            storage: storage.clone(), prefix: record.storage_prefix(), deployment: record.id.clone(),
            writer_epoch: None, compactor: false, snapshot_block,
        }).unwrap() };
        let mut opened = storage_factory.open_for_recovery(&configuration(None), &key("v06"), io.clone()).await.unwrap();
        assert_eq!(opened.blocks, BlockDiscovery::FromPublished(rtdb_replication::PublishedCheckpoint {
            block: BlockIndex(0), boundary: rtdb_types::ReplicaPosition(1), root_sha256: Some([42; 32]),
        }));
        let mut current = directory.deployment(&record.id).await.unwrap().unwrap();
        assert_eq!(current.storage_regions.values().filter_map(StorageRegion::checkpoint_metadata_from).collect::<Vec<_>>(), [0]);
        let first_required = current.storage_regions.values().filter_map(StorageRegion::first_required_replica).min();
        assert_eq!(first_required, Some(2), "startup must admit the checkpoint and its replica suffix together");
        assert_eq!(crate::replica_retention::replica_trim_through(8, first_required.unwrap()), Some(1));
        let version = current.version;
        current.checkpoint = Some(checkpoint(1));
        let held = directory.replace_deployment(current, version).await.unwrap().applied().unwrap();
        let stale = storage_factory.open_for_recovery(&configuration(Some(0)), &key("v06"), IoRegion::new()).await.err().unwrap();
        assert_eq!(stale.code, CHECKPOINT_ADVANCED);
        let refs = crate::reference::References::default();
        reap_as_of(bucket.as_ref(), &held, i64::MAX, Duration::ZERO, &refs).await.unwrap();
        assert!(bucket.head(&physical("~packs/b00.pack")).await.is_ok(), "an admitted checkpoint root must survive collection");
        for (name, expected) in [(core, b"root".as_slice()), (packed, b"table"), (loose, b"loose")] {
            assert_eq!(opened.store.get(&key(name)).await.unwrap().as_ref(), expected);
        }

        let hold = opened.checkpoint_metadata.take().unwrap();
        let (started, reading) = oneshot::channel();
        let (resume, complete) = std::sync::mpsc::channel();
        let provider = io.clone();
        let reader = tokio::spawn(IoRegion::retaining(hold, async move {
            provider.run_blocking(move || { started.send(()).unwrap(); complete.recv().unwrap(); Ok(()) }).await
        }));
        reading.await.unwrap();
        reader.abort();
        assert!(reader.await.unwrap_err().is_cancelled());
        let current = directory.deployment(&record.id).await.unwrap().unwrap();
        assert!(current.storage_regions.values().any(|region| {
            region.checkpoint_metadata_from() == Some(0) && region.first_required_replica() == Some(2)
        }), "both holds must outlive cancellation of a provider read");
        resume.send(()).unwrap();
        let released = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let current = directory.deployment(&record.id).await.unwrap().unwrap();
                if current.storage_regions.values().all(|region| region.checkpoint_metadata_from().is_none()) { break current; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.unwrap();
        assert_eq!(released.storage_regions.len(), 1);
        assert!(released.storage_regions.values().all(|region| region.first_required_replica().is_none()),
            "startup must release both holds after the effects drain");
        reap_as_of(bucket.as_ref(), &released, i64::MAX, Duration::ZERO, &refs).await.unwrap();
        for name in ["~packs/b00.pack", spill, loose] {
            assert!(matches!(bucket.head(&physical(name)).await, Err(Error::MissingObject(_))));
        }
        assert!(bucket.head(&physical("~packs/b01.pack")).await.is_ok());
        drop(opened);
        tasks.close();
        tasks.wait().await;
    }

    #[tokio::test]
    async fn a_fork_at_a_past_block_keeps_an_object_released_in_the_same_millisecond_after_the_block() {
        let bucket: crate::reference::Bucket = Arc::new(MemoryStore::default());
        let clock = stamps(|| 1_000);
        let store = open_deployment_store(Arc::clone(&bucket), "p/v06", crate::packing::PackDiscovery::List, Arc::clone(&clock)).await.unwrap();
        store.put(&key("tables/t/data/l01-rc-b00.arrow"), Bytes::from_static(b"compacted")).await.unwrap();
        store.put(&key("blocks/b00.binpb"), Bytes::from_static(b"catalog")).await.unwrap();
        store.delete(&key("tables/t/data/l01-rc-b00.arrow")).await.unwrap();
        crate::reference::simulate_fork(bucket.as_ref(), "p", "c", Some(0)).await.unwrap();
        let fork = open_deployment_store(Arc::clone(&bucket), "c/v06", crate::packing::PackDiscovery::List, clock).await.unwrap();
        let listed: Vec<String> = fork.list(Some(&key("tables"))).await.unwrap().into_iter().map(|object| object.key.as_str().to_owned()).collect();
        assert_eq!(listed, ["tables/t/data/l01-rc-b00.arrow"], "the release came after the block, though within its millisecond");
    }

    #[test]
    fn stamps_rise_strictly_though_the_clock_stands_still_or_goes_back() {
        let time = Arc::new(std::sync::atomic::AtomicI64::new(5));
        let source = Arc::clone(&time);
        let clock = stamps(move || source.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!([clock(), clock()], [5, 6]);
        time.store(3, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(clock(), 7);
        time.store(20, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(clock(), 20);
    }

    #[tokio::test]
    async fn an_attachment_with_legacy_references_cannot_admit_audience_reads() {
        let bucket: crate::reference::Bucket = Arc::new(MemoryStore::default());
        let parent = DeploymentRecord::new("shop", "acme", vec![], 0);
        let mut child = DeploymentRecord::new("preview", "acme", vec![], 0);
        child.parent = Some(parent.id.clone());
        let parent_store = format!("{}/v06", parent.storage_prefix());
        let child_store = format!("{}/v06", child.storage_prefix());
        bucket.put(&key(&format!("{parent_store}/objects/body")), Bytes::from_static(b"parent")).await.unwrap();
        crate::reference::simulate_fork(bucket.as_ref(), &parent.storage_prefix(), &child.storage_prefix(), None).await.unwrap();

        let local = Arc::new(crate::reference::PrefixedStore::new(bucket.clone(), &child_store));
        let files = tempfile::tempdir().unwrap();
        let keys = Arc::new(crate::keys::LocalKeyService::new(files.path().join("keys")).unwrap());
        let custody = ReferenceReads::new(rtdb_directory::MemoryDirectory::new(), keys, TaskTracker::new(), &child,
            "v06", rtdb_directory::StorageNamespaces { objects: Uuid::new_v4(), keys: [0; 32] }, ReplayCustody::Local);
        let (_, audience_reads) = open_cell_store(bucket, &child_store, local, &child, wall_clock(), None, custody).await.unwrap();
        assert!(!audience_reads);
    }

    #[tokio::test]
    async fn a_checkpointed_root_recovers_the_latest_block_after_reaping_with_listing_disabled() {
        let raw = Arc::new(StoreFaults { inner: MemoryStore::default(), fail: false.into(), deny_list: false.into() });
        let bucket: crate::reference::Bucket = raw.clone();
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        let store = format!("{}/v06", record.storage_prefix());
        let open = |discovery| open_deployment_store(Arc::clone(&bucket), &store, discovery, wall_clock());
        let writer = open(crate::packing::PackDiscovery::List).await.unwrap();
        for block in 0..3i64 {
            writer.put(&key(&format!("tables/t/data/l00-rc-b0{block}.arrow")), Bytes::from(format!("rows {block}"))).await.unwrap();
            writer.put(&key(&rtdb_format::keys::block_key(rtdb_types::BlockIndex(block)).unwrap()), Bytes::from_static(b"catalog")).await.unwrap();
        }
        record.checkpoint = Some(rtdb_directory::Checkpoint { block: 0, boundary: 0, source: 0, system_time: 0, root_sha256: None });
        for name in ["tables/t/data/l00-rc-b01.arrow", "blocks/b01.binpb"] { writer.delete(&key(name)).await.unwrap(); }
        reap(bucket.as_ref(), &record, Duration::ZERO, &Default::default()).await.unwrap();
        raw.deny_list.store(true, std::sync::atomic::Ordering::Relaxed);
        let local = Arc::new(crate::reference::PrefixedStore::new(Arc::clone(&bucket), &store));
        let files = tempfile::tempdir().unwrap();
        let keys = Arc::new(crate::keys::LocalKeyService::new(files.path().join("keys")).unwrap());
        let custody = ReferenceReads::new(rtdb_directory::MemoryDirectory::new(), keys, TaskTracker::new(), &record,
            "v06", rtdb_directory::StorageNamespaces { objects: Uuid::new_v4(), keys: [0; 32] }, ReplayCustody::Local);
        let (cold, audience_reads) = open_cell_store(bucket, &store, local, &record, wall_clock(), None, custody).await.unwrap();
        assert!(audience_reads, "the cold checkpoint uses its own immutable storage");
        assert_eq!(cold.get(&key("tables/t/data/l00-rc-b02.arrow")).await.unwrap().as_ref(), b"rows 2", "a block after a fully released one is found from the checkpoint");
    }

    #[tokio::test]
    async fn a_store_refuses_a_deployment_it_was_not_created_for() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalStore::open(directory.path()).unwrap();
        check_root(&store, "one", true).await.unwrap();
        check_root(&store, "one", true).await.unwrap();
        assert_eq!(check_root(&store, "two", true).await.unwrap_err().code, "rtdb.multi/foreign-root");
    }

    #[tokio::test]
    async fn a_read_only_attach_verifies_root_ownership_without_writing() {
        let store = MemoryStore::default();
        check_root(&store, "one", false).await.unwrap();
        assert!(matches!(store.head(&key(ROOT_MARKER)).await, Err(Error::MissingObject(_))));
        check_root(&store, "two", true).await.unwrap();
        assert_eq!(check_root(&store, "one", false).await.unwrap_err().code, "rtdb.multi/foreign-root");
        check_root(&store, "two", false).await.unwrap();
    }

    #[tokio::test]
    async fn storage_grant_refresh_requires_the_current_writer_or_live_compactor() {
        use rtdb_s3::WriteAuthorization;

        let directory = rtdb_directory::MemoryDirectory::new();
        let register = |id: &str, pool| rtdb_directory::MemberRecord {
            id: MemberId(id.into()), role: MemberRole::Node, address: String::new(), control_address: None,
            availability_zone: "local".into(), engine_version: "test".into(), lease_expires_at_ms: now_ms() + 60_000,
            state: MemberState::Eligible, drain_target_engine: None, pool, storage_namespaces: None, version: 0,
        };
        let mut holder = directory.register_member(register("holder", MemberPool::General)).await.unwrap();
        let worker = directory.register_member(register("compactor", MemberPool::Compaction)).await.unwrap();
        let mut initial = DeploymentRecord::new("shop", "acme", vec![], 0);
        initial.epoch = 1;
        initial.holder = Some(holder.id.clone());
        let mut record = directory.create_deployment(initial).await.unwrap();
        let writer = ScopedWriter { directory: directory.clone(), deployment: record.id.clone(),
            prefix: record.storage_prefix(), epoch: Some(record.epoch), member: holder.id.clone() };
        let compactor = ScopedWriter { directory: directory.clone(), deployment: record.id.clone(),
            prefix: record.storage_prefix(), epoch: None, member: worker.id };
        let latest = SystemTime::now() + Duration::from_secs(60);
        writer.authorize(Some(latest)).await.unwrap();
        record = directory.deployment(&record.id).await.unwrap().unwrap();
        let recorded = record.s3_write_lease_until_ms;
        assert_eq!(recorded, Some((latest + rtdb_s3::SCOPED_S3_CLOCK_ALLOWANCE).duration_since(UNIX_EPOCH).unwrap().as_millis() as i64));
        compactor.authorize(Some(latest - Duration::from_secs(30))).await.unwrap();
        assert_eq!(directory.deployment(&record.id).await.unwrap().unwrap().s3_write_lease_until_ms, recorded);

        holder.lease_expires_at_ms = 0;
        directory.replace_member(holder.clone(), holder.version).await.unwrap().applied().unwrap();
        assert_eq!(writer.authorize(None).await.unwrap_err().code, rtdb_node::NOT_HELD);

        record.epoch += 1;
        record = directory.replace_deployment(record.clone(), record.version).await.unwrap().applied().unwrap();
        assert_eq!(writer.authorize(None).await.unwrap_err().code, rtdb_node::NOT_HELD);
        compactor.authorize(None).await.unwrap();

        record.lifecycle = Lifecycle::Deleting;
        directory.replace_deployment(record.clone(), record.version).await.unwrap().applied().unwrap();
        assert_eq!(compactor.authorize(Some(latest + Duration::from_secs(30))).await.unwrap_err().code, rtdb_node::NOT_HELD);
        assert_eq!(directory.deployment(&record.id).await.unwrap().unwrap().s3_write_lease_until_ms, recorded);
    }
}
