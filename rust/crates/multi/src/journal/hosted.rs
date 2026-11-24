//! Attachment of an installed journal to RTDB's database factories.

mod custody_work;
mod discovery;
pub mod erasure;
pub(crate) mod prefix_retirement;
mod reclamation;
mod storage;

use super::{
    Authority, Held, InstalledJournal, InstalledRoute, JournalLog, WriterAdmission,
    custody::{CheckpointAuthority, CheckpointCustody, Preparation, Reservation},
};
use crate::keys::{JournalKey, KeyService};
use async_trait::async_trait;
use ordered_logkeeper::ordered::{
    Checkpoint, Command, Cut, Enrollment, Hold, Outcome, Refusal, Request, Window,
    service::Route,
};
use prost_types::Any;
use rtdb_columnar::MemoryBudget;
use rtdb_directory::{Checkpoint as RtdbCheckpoint, DeploymentRecord, now_ms};
use rtdb_format::{
    catalog::BlockCatalog,
    database::proto::{DatabaseConfig, RemoteStorage, database_config},
    envelope::{ReplicaBody, ReplicaEnvelope},
};
use rtdb_node::{LogAccess, LogFactory, OpenedLogs};
use rtdb_storage::{IoRegion, ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, Result, StorageEpoch};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
pub(crate) use storage::StoreFactory;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use uuid::Uuid;

pub(crate) const LOG_TYPE: &str = "rtdb.multi.OrderedJournal";
pub(crate) const STORE_TYPE: &str = "rtdb.multi.OrderedStorage";
const MAX_MANIFEST_BYTES: u64 = 16 << 20;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Definition {
    deployment: DeploymentRecord,
    worker: InstalledRoute,
    custody: Option<InstalledRoute>,
    custody_work: Option<custody_work::Installation>,
    archive: crate::deployment::CellStorage,
    archive_prefix: String,
    journal_window_bytes: Option<u64>,
    endpoints: Vec<WorkerEndpoint>,
    #[serde(default)]
    checkpoint_reservations: Vec<Reservation>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkerEndpoint {
    pub enrollment: Enrollment,
    pub control: String,
    pub pgwire: String,
}

/// An immutable discovery projection for a deployment already installed in
/// the journal. Writer selection and the current checkpoint come from that
/// journal rather than the discovery record.
pub(crate) struct Placement {
    pub record: DeploymentRecord,
    pub worker: Arc<InstalledJournal>,
    pub custodian: Option<Arc<InstalledJournal>>,
    custody_work: Option<custody_work::WorkOwner>,
    archive: crate::deployment::CellStorage,
    archive_prefix: ObjectKey,
    journal_window_bytes: Option<u64>,
    nomination: tokio::sync::Mutex<Option<Request>>,
    worker_disposal: tokio::sync::Mutex<Option<Request>>,
    reader_opening: parking_lot::Mutex<std::sync::Weak<Held>>,
    checkpoint_reservations: BTreeMap<u64, Reservation>,
    publication: tokio::sync::Mutex<Option<(Checkpoint, Cut)>>,
}

#[derive(Default)]
pub(crate) struct Routes(BTreeMap<String, Arc<Placement>>);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GatewayDefinition {
    name: String,
    deployment: Uuid,
    journal: InstalledRoute,
    endpoints: Vec<WorkerEndpoint>,
}

pub struct GatewayRoutes(BTreeMap<String, GatewayPlacement>);

pub(crate) struct GatewayPlacement {
    pub deployment: Uuid,
    pub journal: Arc<InstalledJournal>,
    pub endpoints: BTreeMap<Enrollment, WorkerEndpoint>,
}

impl GatewayRoutes {
    pub fn read(path: &Path) -> Result<Self> {
        let entries: Vec<GatewayDefinition> = read_manifest(path)?;
        let mut routes = BTreeMap::new();
        for entry in entries {
            let journal = InstalledJournal::open(entry.journal)?;
            if !matches!(journal.authority(), Authority::Routing)
                || journal.deployment() != entry.deployment
            {
                return Err(invalid(
                    "A gateway route requires an exact read-only journal credential",
                ));
            }
            let endpoints = worker_endpoints(entry.endpoints)?;
            if routes
                .insert(
                    entry.name,
                    GatewayPlacement {
                        deployment: entry.deployment,
                        journal,
                        endpoints,
                    },
                )
                .is_some()
            {
                return Err(invalid("An installed gateway name occurs twice"));
            }
        }
        Ok(Self(routes))
    }

    pub(crate) fn by_name(&self, name: &str) -> Option<&GatewayPlacement> {
        self.0.get(name)
    }

    pub async fn reserve_deployments(&self, directory: &dyn rtdb_directory::Directory) -> Result<()> {
        for placement in self.0.values() {
            directory.reserve_installed_journal(placement.deployment).await?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
struct Base {
    checkpoint: Option<RtdbCheckpoint>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Attachment {
    name: String,
    deployment: Uuid,
    admission: AttachmentAdmission,
    base: Base,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case")]
enum AttachmentAdmission {
    Writer { admission: WriterAdmission },
    Reader { placement: ordered_logkeeper::ordered::Placement, epoch: u64, hold: Hold },
}

pub(crate) fn attachment_identity(value: &Any) -> Result<(String, Option<u64>)> {
    let attachment: Attachment =
        serde_json::from_slice(&value.value).map_err(|error| invalid(&error.to_string()))?;
    let epoch = match attachment.admission {
        AttachmentAdmission::Writer { admission } => Some(admission.grant.epoch),
        AttachmentAdmission::Reader { .. } => None,
    };
    Ok((attachment.deployment.to_string(), epoch))
}

pub(crate) fn reader_attachment(value: &Any) -> Result<Option<(ordered_logkeeper::ordered::Placement, u64, Enrollment)>> {
    let attachment: Attachment =
        serde_json::from_slice(&value.value).map_err(|error| invalid(&error.to_string()))?;
    Ok(match attachment.admission {
        AttachmentAdmission::Reader { placement, epoch, hold } => Some((placement, epoch, hold.owner)),
        AttachmentAdmission::Writer { .. } => None,
    })
}

impl Routes {
    pub(crate) fn read(path: &Path, serving: &crate::deployment::CellStorage, broker: Option<&crate::replay_broker::Client>) -> Result<Self> {
        let entries: Vec<Definition> = read_manifest(path)?;
        let mut routes = BTreeMap::new();
        for entry in entries {
            if serving.overlaps(&entry.archive)? {
                return Err(invalid(
                    "The checkpoint archive overlaps the serving storage",
                ));
            }
            let worker = InstalledJournal::open(entry.worker)?;
            let custodian = entry.custody.map(InstalledJournal::open).transpose()?;
            let Authority::Worker { enrollment } = worker.authority() else {
                return Err(invalid("A node needs its exact enrolled worker credential"));
            };
            if !worker.binds(&entry.deployment)
                || custodian.as_ref().is_some_and(|custodian| {
                    !custodian.binds(&entry.deployment)
                        || custodian.placement() != worker.placement()
                        || !matches!(custodian.authority(), Authority::Custody)
                })
            {
                return Err(invalid(
                    "The installed route addresses a different deployment or residence",
                ));
            }
            let endpoints = worker_endpoints(entry.endpoints)?;
            if !endpoints.contains_key(&enrollment) {
                return Err(invalid("The local journal worker has no installed route"));
            }
            if matches!(&entry.archive, crate::deployment::CellStorage::S3 { config } if config.terminal_scope.is_none())
            {
                return Err(invalid(
                    "An ordered S3 checkpoint archive needs enrolled terminal storage",
                ));
            }
            let name = entry.deployment.name.clone();
            let mut checkpoint_reservations = BTreeMap::new();
            for reservation in entry.checkpoint_reservations {
                CheckpointCustody::copy_budget(&reservation)?;
                if reservation.custody.revision <= 1
                    || reservation.custody.allocation.is_nil()
                    || checkpoint_reservations
                        .insert(reservation.custody.revision, reservation)
                        .is_some()
                {
                    return Err(invalid(
                        "Each provider checkpoint reservation needs one exact next revision",
                    ));
                }
            }
            if entry.journal_window_bytes == Some(0) {
                return Err(invalid(
                    "The installed journal window size must be positive",
                ));
            }
            let custody_work = entry.custody_work.map(|installation| {
                let broker = broker.cloned().ok_or_else(|| invalid("The installed custody domain needs its broker route"))?;
                custody_work::WorkOwner::open(installation, broker, worker.placement())
            }).transpose()?;
            let placement = Arc::new(Placement {
                record: entry.deployment,
                worker,
                custodian,
                archive: entry.archive,
                archive_prefix: ObjectKey::new(entry.archive_prefix)?,
                journal_window_bytes: entry.journal_window_bytes,
                custody_work,
                nomination: Default::default(),
                worker_disposal: Default::default(),
                reader_opening: Default::default(),
                checkpoint_reservations,
                publication: Default::default(),
            });
            if routes.insert(name, placement).is_some() {
                return Err(invalid("An installed deployment name occurs twice"));
            }
        }
        Ok(Self(routes))
    }

    pub fn by_name(&self, name: &str) -> Option<&Arc<Placement>> {
        self.0.get(name)
    }

    pub(crate) async fn reserve_deployments(&self, directory: &dyn rtdb_directory::Directory) -> Result<()> {
        for placement in self.0.values() {
            directory.reserve_installed_journal(placement.worker.deployment()).await?;
        }
        Ok(())
    }

    pub(crate) fn custody_placements(&self) -> impl Iterator<Item = (&str, &Placement)> {
        self.0.iter().filter_map(|(name, placement)| {
            placement
                .custodian
                .as_ref()
                .map(|_| (name.as_str(), placement.as_ref()))
        })
    }

    fn resolve(&self, configuration: &Any) -> Result<(Arc<Placement>, Attachment)> {
        let attachment: Attachment = serde_json::from_slice(&configuration.value)
            .map_err(|error| invalid(&error.to_string()))?;
        let placement = self.by_name(&attachment.name).filter(|placement| {
            if placement.worker.deployment() != attachment.deployment { return false; }
            let Authority::Worker { enrollment } = placement.worker.authority() else { return false; };
            match attachment.admission {
                AttachmentAdmission::Writer { admission } => {
                    admission.grant.enrollment == enrollment && admission.recovery.owner == enrollment
                }
                AttachmentAdmission::Reader { placement: expected, hold, .. } => {
                    placement.worker.placement() == expected && hold.owner == enrollment
                }
            }
        }).ok_or_else(|| invalid("The database attachment addresses a different installed journal worker"))?;
        Ok((Arc::clone(placement), attachment))
    }
}

/// Resolves the installed physical archive and its exact checkpoint prefix.
pub async fn archive_identity(
    storage: &crate::deployment::CellStorage,
    prefix: &str,
) -> Result<[u8; 32]> {
    let prefix = ObjectKey::new(prefix)?;
    let bucket = storage.bucket(IoRegion::new()).await?;
    archive_owner(storage, bucket.as_ref(), &prefix).await
}

async fn archive_owner(
    storage: &crate::deployment::CellStorage,
    bucket: &dyn ObjectStore,
    prefix: &ObjectKey,
) -> Result<[u8; 32]> {
    let namespace = crate::deployment::existing_storage_namespace(bucket)
        .await?
        .filter(|namespace| !namespace.is_nil())
        .ok_or_else(|| invalid("The checkpoint archive has no installed physical namespace"))?;
    let mut owner = ring::digest::Context::new(&ring::digest::SHA256);
    owner.update(b"rtdb.multi.ordered-checkpoint-provider/v1");
    owner.update(namespace.as_bytes());
    owner.update(&storage.custody_namespace()?);
    owner.update(prefix.as_str().as_bytes());
    Ok(owner
        .finish()
        .as_ref()
        .try_into()
        .expect("SHA-256 has 32 bytes"))
}

pub fn read_application_route(
    path: &Path,
) -> Result<ordered_logkeeper::ordered::daemon::ApplicationRoute> {
    read_manifest(path)
}

fn read_manifest<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let mut body = Vec::new();
    File::open(path)
        .and_then(|file| file.take(MAX_MANIFEST_BYTES + 1).read_to_end(&mut body))
        .map_err(|error| invalid(&error.to_string()))?;
    if body.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(invalid("The installed route manifest is too large"));
    }
    serde_json::from_slice(&body).map_err(|error| invalid(&error.to_string()))
}

fn worker_endpoints(peers: Vec<WorkerEndpoint>) -> Result<BTreeMap<Enrollment, WorkerEndpoint>> {
    let mut endpoints = BTreeMap::new();
    for peer in peers {
        if peer.control.is_empty()
            || peer.pgwire.is_empty()
            || peer.enrollment.worker.is_nil()
            || peer.enrollment.serial == 0
            || endpoints.insert(peer.enrollment, peer).is_some()
        {
            return Err(invalid(
                "Each eligible journal worker needs one exact control and PGWire route",
            ));
        }
    }
    if endpoints.is_empty() {
        return Err(invalid("The installed route has no worker endpoints"));
    }
    Ok(endpoints)
}

impl Placement {
    pub(crate) async fn checkpoint_precedes_erasure(
        &self, status: &rtdb_replication::DriverStatus, candidate: Option<RtdbCheckpoint>,
    ) -> Result<bool> {
        let (Some(owner), Some(marker)) = (&self.custody_work, status.first_erasure) else { return Ok(true); };
        owner.record_sql(crate::replay_broker::domain::SqlIntent {
            placement: self.worker.placement(),
            transaction: marker.transaction,
            position: marker.position,
        }).await?;
        Ok(candidate.is_none_or(|candidate| candidate.boundary < marker.position.after_offset(self.worker.log_epoch())))
    }

    pub(crate) async fn stage_sql_erasure(
        &self, node: &rtdb_node::Node, database: &rtdb_node::Database, admission: WriterAdmission,
        broker: &crate::replay_broker::Client, keys: &Arc<dyn KeyService>, request: erasure::SqlErasureRequest,
        cancellation: &CancellationToken,
    ) -> Result<Checkpoint> {
        use crate::replay_broker::domain;

        let custodian = self.custodian.as_ref().ok_or_else(|| invalid("This placement has no erasure custodian"))?;
        let placement = custodian.placement();
        let operation = request.operation;
        if self.custody_work.as_ref().map(custody_work::WorkOwner::domain) != Some(operation.domain) {
            return Err(invalid("The selected SQL writer is outside the installed custody domain"));
        }
        if domain::sql_target(placement, request.transaction, request.position)? != operation.target {
            return Err(invalid("The SQL erasure marker differs from the broker's target"));
        }
        broker.custody_domain(operation.domain, domain::Action::AuthorizeRoot {
            operation, root: placement.deployment, history: domain::installed_history(placement)?,
        }).await?;

        let _publication = self.publication.lock().await;
        let _transactions = cancellation.run_until_cancelled(database.pause_transactions()).await
            .ok_or_else(|| invalid("SQL erasure staging was cancelled"))??;
        let route = custodian.route().await?;
        if route.writer.grant != Some(admission.grant) {
            return Err(invalid("The SQL erasure writer no longer holds the installed journal"));
        }
        let revision = route.checkpoint.custody.revision.checked_add(1)
            .ok_or_else(|| invalid("The checkpoint custody revision space is exhausted"))?;
        let reservation = self.checkpoint_reservations.get(&revision).copied()
            .ok_or_else(|| invalid("The next erasure checkpoint allocation is unfunded"))?;
        let provider = self.checkpoint_custody(IoRegion::new(), Arc::clone(keys)).await?;
        let epoch = self.worker.log_epoch();
        let position = |next: u64| {
            let offset = i64::try_from(next).ok().and_then(|next| next.checked_sub(1))
                .ok_or_else(|| invalid("The checkpoint exceeds the native replica position space"))?;
            rtdb_types::ReplicaPosition::from_epoch_offset(epoch, offset)
        };
        let checkpoint = if genesis(route.checkpoint) {
            None
        } else {
            let opened = provider.open(route.checkpoint).await?;
            Some(rtdb_replication::PublishedCheckpoint {
                block: rtdb_types::BlockIndex(opened.checkpoint.block),
                boundary: position(route.checkpoint.cut.next_lsn)?,
                root_sha256: Some(route.checkpoint.root),
            })
        };
        let cut = rtdb_types::CatalogCut {
            storage_version: rtdb_format::keys::CURRENT_STORAGE_VERSION,
            storage_epoch: StorageEpoch(0),
            replica_epoch: epoch,
            through: position(route.head.next_lsn)?,
        };
        let selection = node.sql_erasure_source(database, cut, checkpoint,
            request.transaction, request.position, cancellation).await?;
        let coordinator = erasure::ErasureCoordinator::new(&self.worker, custodian, &provider, keys.as_ref(), broker, operation)?;
        let staged = coordinator.stage_survivor(erasure::Erasure {
            placement, operation: operation.erase, custody_domain: operation.domain,
            incumbent: admission.grant, timing: erasure::ErasureTiming::OperationalTarget,
        }, request.rotation, reservation, &selection, cancellation).await?;
        Ok(staged.checkpoint)
    }

    pub(crate) async fn hold_worker_custody(&self, tasks: &TaskTracker) -> Result<Option<Arc<dyn Send + Sync>>> {
        match &self.custody_work {
            Some(owner) => owner.hold(erasure::CustodyWork::Plaintext, tasks).await.map(Some),
            None => Ok(None),
        }
    }

    async fn hold_query_custody(&self, tasks: &TaskTracker) -> Result<Option<Arc<dyn Send + Sync>>> {
        let Some(owner) = &self.custody_work else { return Ok(None); };
        let plaintext = owner.hold(erasure::CustodyWork::Plaintext, tasks).await?;
        let egress = owner.hold(erasure::CustodyWork::Egress, tasks).await?;
        Ok(Some(Arc::new((plaintext, egress))))
    }

    /// Replenish only bytes the installed voters have already paid for. The
    /// ordered decision recomputes the limit against concurrent appends.
    pub(crate) async fn replenish(&self, route: &Route) -> Result<()> {
        let Some(custodian) = &self.custodian else {
            return Ok(());
        };
        let remaining = route.window.bytes - route.window_spent;
        let funded = self
            .journal_window_bytes
            .unwrap_or(route.reusable_bytes)
            .min(route.reusable_bytes);
        let watermark = (crate::process::MAXIMUM_LOG_MESSAGE_BYTES as u64)
            .saturating_add(ordered_logkeeper::ordered::APPEND_RECORD_CHARGE_BYTES)
            .max(route.window.bytes / 4);
        if remaining > watermark || funded <= remaining {
            return Ok(());
        }
        let serial = route
            .window
            .serial
            .checked_add(1)
            .ok_or_else(|| invalid("The journal window serial space is exhausted"))?;
        let window = Window {
            allocation: Uuid::new_v4(),
            serial,
            bytes: funded,
        };
        custodian.fund(route, window).await
    }

    pub(crate) async fn publish_checkpoint(
        &self,
        keys: &Arc<dyn KeyService>,
        offered: Option<(WriterAdmission, RtdbCheckpoint)>,
        serving: &crate::deployment::CellStorage,
        memory: &Arc<MemoryBudget>,
        releases: &TaskTracker,
    ) -> Result<Option<RtdbCheckpoint>> {
        let mut inspected = self.publication.lock().await;
        let custodian = self
            .custodian
            .as_ref()
            .ok_or_else(|| invalid("This placement has no checkpoint custodian"))?;
        let initial = custodian.route().await?;
        if offered.is_none()
            && initial.preparing.is_none()
            && initial.last_maintenance.is_none_or(|through| {
                through.next_lsn <= initial.checkpoint.cut.next_lsn
                    || *inspected == Some((initial.checkpoint, through))
            })
        {
            return Ok(None);
        }
        let io = IoRegion::new();
        let provider = self.checkpoint_custody(Arc::clone(&io), Arc::clone(keys)).await?;
        let requested = offered
            .map(|(admission, checkpoint)| {
                let next = checkpoint
                    .boundary
                    .checked_add(1)
                    .and_then(|offset| u64::try_from(offset).ok())
                    .ok_or_else(|| {
                        invalid("The adopted checkpoint has no ordered replica boundary")
                    })?;
                Ok::<_, Anomaly>((admission, checkpoint, next))
            })
            .transpose()?;

        loop {
            let current = custodian.route().await?;
            let published = current.checkpoint;
            if current.preparing.is_some_and(|pending| pending.erasure) {
                return if requested.is_some() {
                    Err(invalid("An erasure owner is preparing the next checkpoint"))
                } else {
                    Ok(None)
                };
            }
            if let Some((_, checkpoint, next)) = requested {
                if published.cut.next_lsn > next {
                    return Ok(Some(provider.open(published).await?.checkpoint));
                }
                if published.cut.next_lsn == next {
                    return if checkpoint.root_sha256 == Some(published.root) {
                        Ok(Some(checkpoint))
                    } else {
                        Err(invalid(
                            "The adopted checkpoint differs from the installed root",
                        ))
                    };
                }
            }
            let revision = published
                .custody
                .revision
                .checked_add(1)
                .ok_or_else(|| invalid("The checkpoint custody revision space is exhausted"))?;
            let Some(&reservation) = self.checkpoint_reservations.get(&revision) else {
                return if requested.is_some() || current.preparing.is_some() {
                    Err(invalid("The next exact checkpoint allocation is unfunded"))
                } else {
                    Ok(None)
                };
            };
            let origin = match (current.preparing, requested) {
                (Some(chosen), _) => Preparation::recover(chosen, reservation)?,
                (None, Some((admission, checkpoint, next))) => Preparation {
                    authority: CheckpointAuthority::Upload,
                    writer: admission.grant,
                    base: admission.recovery.checkpoint,
                    through: custodian.checkpoint_cut(next).await?,
                    checkpoint,
                },
                (None, None) => {
                    let Some(through) = current
                        .last_maintenance
                        .filter(|cut| cut.next_lsn > published.cut.next_lsn)
                    else {
                        return Ok(None);
                    };
                    let cut = (published, through);
                    if *inspected == Some(cut) {
                        return Ok(None);
                    }
                    let objects = storage::ReplicaObjects::for_custodian(
                        self,
                        serving,
                        Arc::clone(keys),
                        Arc::clone(&io),
                    )
                    .await?;
                    let Some(candidate) = self.checkpoint_copy(published, releases, discovery::discover(
                        self, custodian, &provider, cut, &objects, (keys.as_ref(), &io), memory,
                    )).await?
                    else {
                        *inspected = Some(cut);
                        return Ok(None);
                    };
                    candidate
                }
            };
            let staged = self.checkpoint_copy(origin.base, releases, async {
                if current.preparing.is_none() {
                    let source =
                        storage::reopen_source(self, serving, &provider, Arc::clone(keys), &io, origin)
                            .await?;
                    let references = provider
                        .preflight(&reservation, origin, source.as_ref())
                        .await?;
                    custodian
                        .prepare_checkpoint(&current, origin.ordered(reservation)?, references)
                        .await?;
                }
                let origin = provider.begin(&reservation, origin).await?;
                match provider.prepared(reservation, origin).await? {
                    Some(prepared) => Ok(prepared),
                    None => {
                        self.validate_checkpoint_boundary(
                            custodian,
                            published,
                            reservation,
                            origin,
                            keys,
                            serving,
                            &io,
                            memory,
                        )
                        .await?;
                        let source =
                            storage::reopen_source(self, serving, &provider, Arc::clone(keys), &io, origin)
                                .await?;
                        provider
                            .prepare(&reservation, origin, source.as_ref())
                            .await
                    }
                }
            }).await?;
            let authority = custodian.route().await?;
            provider.publish(custodian, &authority, staged).await?;
            if requested.is_none_or(|(_, _, next)| staged.cut.next_lsn >= next) {
                return Ok(Some(origin.checkpoint));
            }
        }
    }

    /// A copy uses the same durable checkpoint hold as serving reads.
    /// Provider tasks also inherit the hold when cancellation drops their caller.
    async fn checkpoint_copy<T>(
        &self,
        checkpoint: Checkpoint,
        releases: &TaskTracker,
        work: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        let route = self.worker.route().await?;
        let request = self.worker.request(&route, Command::ProtectCheckpoint {
            root: checkpoint.root,
            from: checkpoint.cut.next_lsn,
            through: checkpoint.cut.next_lsn,
        })?;
        let held = Held::acquire(&self.worker, request, releases).await?;
        if held.scope().checkpoint != checkpoint {
            return Err(invalid("The ordered copy hold selected a different checkpoint"));
        }
        let custody = match &self.custody_work {
            Some(custody) => Some(custody.hold(erasure::CustodyWork::Copy, releases).await?),
            None => None,
        };
        let mut work = std::pin::pin!(IoRegion::retaining(Arc::new((Arc::clone(&held), custody)), work));
        loop {
            let sent = std::time::Instant::now();
            let length = self.worker.read_lease(held.scope(), route.writer.epoch).await?;
            let length = std::time::Duration::from_millis(length);
            let renew = sent.checked_add(length / 2)
                .filter(|renew| *renew > std::time::Instant::now())
                .ok_or_else(|| invalid("The checkpoint copy lease arrived expired"))?;
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(renew.into()) => (),
                result = &mut work => return result,
            }
        }
    }

    async fn validate_checkpoint_boundary(
        &self,
        custodian: &InstalledJournal,
        base: Checkpoint,
        reservation: Reservation,
        origin: Preparation,
        keys: &Arc<dyn KeyService>,
        serving: &crate::deployment::CellStorage,
        io: &Arc<IoRegion>,
        memory: &Arc<MemoryBudget>,
    ) -> Result<()> {
        let preparation = origin.ordered(reservation)?;
        let previous = origin
            .through
            .next_lsn
            .checked_sub(1)
            .ok_or_else(|| invalid("A checkpoint must advance the chosen data cut"))?;
        let from = custodian.checkpoint_cut(previous).await?;
        let maximum = crate::process::MAXIMUM_LOG_MESSAGE_BYTES as u64
            + ordered_logkeeper::ordered::APPEND_RECORD_CHARGE_BYTES;
        let _page = memory.reserve((maximum as usize) * 5)?;
        let page = custodian
            .checkpoint_replay(preparation, base, from, maximum)
            .await?;
        let [boundary] = page.records.as_slice() else {
            return Err(invalid(
                "The prepared checkpoint has no single chosen boundary",
            ));
        };
        let key = JournalKey::new(custodian.placement(), boundary.grant.key_generation);
        let bytes = IoRegion::retaining(
            _page.clone(),
            key.open(keys.as_ref(), io, boundary.payload.clone()),
        ).await?;
        let envelope = ReplicaEnvelope::decode(&bytes)?
            .ok_or_else(|| invalid("The prepared checkpoint has an unknown boundary record"))?;
        let _offloaded;
        let envelope = if matches!(&envelope.message, ReplicaBody::OversizedMessage(_)) {
            let objects =
                storage::ReplicaObjects::for_custodian(self, serving, Arc::clone(keys), Arc::clone(io))
                    .await?;
            let (resolved, charge) =
                discovery::resolve_offload(envelope, boundary.grant.epoch, &objects, memory)
                    .await?;
            _offloaded = charge;
            resolved
        } else {
            envelope
        };
        let ReplicaBody::BlockBoundary(chosen) = envelope.message else {
            return Err(invalid(
                "The prepared checkpoint does not end at a chosen native boundary",
            ));
        };
        let source = chosen.latest_processed_msg_id.map_or(-1, |source| {
            rtdb_types::SourcePosition(source).after_offset(self.worker.log_epoch())
        });
        if chosen.block_index != Some(origin.checkpoint.block) || source != origin.checkpoint.source
        {
            return Err(invalid(
                "The prepared catalog differs from its chosen boundary",
            ));
        }
        Ok(())
    }

    pub(crate) async fn dispose_withdrawn_workers(&self) -> Result<()> {
        let Some(custodian) = &self.custodian else {
            return Ok(());
        };
        let mut pending = self.worker_disposal.lock().await;
        Self::settle_worker_disposal(custodian, &mut pending).await?;
        let mut after = None;
        loop {
            let workers = custodian.withdrawn_workers(after).await?;
            let Some(last) = workers.last() else {
                return Ok(());
            };
            after = Some(last.enrollment);
            let mut route = custodian.route().await?;
            for worker in workers {
                let now = u64::try_from(now_ms()).unwrap_or_default();
                if worker
                    .eligible_at
                    .is_some_and(|eligible| eligible <= now.max(route.time))
                {
                    *pending = Some(custodian.request(
                        &route,
                        Command::DisposeExpired {
                            enrollment: worker.enrollment,
                        },
                    )?);
                    Self::settle_worker_disposal(custodian, &mut pending).await?;
                    route = custodian.route().await?;
                }
            }
        }
    }

    async fn settle_worker_disposal(
        custodian: &InstalledJournal,
        pending: &mut Option<Request>,
    ) -> Result<()> {
        let Some(request) = pending.as_ref() else {
            return Ok(());
        };
        let Command::DisposeExpired { enrollment } = request.command else {
            return Err(invalid("The pending request is not an expiry disposal"));
        };
        let result = custodian.execute(request).await;
        if result
            .as_ref()
            .is_err_and(|error| error.code == "rtdb.multi/ordered-journal")
        {
            return result.map(|_| ());
        }
        *pending = None;
        match result?.receipt.outcome {
            Ok(Outcome::WorkerDisposed(disposed)) if disposed == enrollment => Ok(()),
            Err(Refusal::Unavailable | Refusal::Expired) => Ok(()),
            Err(reason) => Err(super::refused(reason)),
            Ok(_) => Err(invalid(
                "The journal disposed a different worker enrollment",
            )),
        }
    }

    pub(crate) async fn retain_selected(&self) -> Result<()> {
        let Some(custodian) = &self.custodian else {
            return Ok(());
        };
        let mut route = custodian.route().await?;
        if route.floor.next_lsn < route.checkpoint.cut.next_lsn
            && custodian.trim(&route, route.checkpoint.cut).await.is_ok()
        {
            route = custodian.route().await?;
        }
        self.replenish(&route).await
    }

    pub(crate) async fn retire_unheld_checkpoint(
        &self,
        keys: &Arc<dyn KeyService>,
    ) -> Result<()> {
        let Some(custodian) = self.custodian.as_ref() else {
            return Ok(());
        };
        let route = custodian.route().await?;
        let Some(retired) = route.retirement else {
            return Ok(());
        };
        let provider = self.checkpoint_custody(IoRegion::new(), Arc::clone(keys)).await?;
        let closed_by_erasure = retired.checkpoint.cut.next_lsn < route.erasure_floor;
        if closed_by_erasure
            && !erasure::retirement_admitted(&provider, route.placement, route.erasure_floor).await?
        {
            return Ok(());
        }
        if !retired.closed || closed_by_erasure {
            custodian
                .close_checkpoint(&route, retired.checkpoint)
                .await?;
        }
        let disposition = match &self.archive {
            crate::deployment::CellStorage::S3 { config } => {
                provider.retire_s3(config, retired.checkpoint).await?
            }
            crate::deployment::CellStorage::Local { .. } => {
                provider.retire_local(retired.checkpoint).await?
            }
        };
        let current = custodian.route().await?;
        if retired.checkpoint.cut.next_lsn < current.erasure_floor {
            if !erasure::retirement_admitted(&provider, current.placement, current.erasure_floor).await? {
                return Ok(());
            }
            erasure::record_checkpoint_disposition(
                &provider, current.placement, current.erasure_floor, retired.checkpoint, disposition,
            ).await?;
        }
        custodian
            .settle_checkpoint(&current, retired.checkpoint, disposition)
            .await?;
        Ok(())
    }

    pub(crate) async fn settle_serving_receipts(
        &self, storage: &crate::deployment::CellStorage, keys: &Arc<dyn KeyService>,
    ) -> Result<()> {
        reclamation::settle_receipts(self, storage, keys).await
    }

    /// Serving prefixes are separate from checkpoint archives: each belongs
    /// to one writer grant and is retired against the journal's evidence.
    pub(crate) async fn retire_serving_prefixes(
        &self,
        storage: &crate::deployment::CellStorage,
        keys: &Arc<dyn KeyService>,
    ) -> Result<Vec<(u64, prefix_retirement::Outcome)>> {
        prefix_retirement::retire(self, storage, keys).await
    }

    async fn pin_root(&self, source: Hold, tasks: &TaskTracker) -> Result<Arc<Held>> {
        let route = self.worker.route().await?;
        let request = self
            .worker
            .request(&route, Command::ProtectRoot { source })?;
        Held::acquire(&self.worker, request, tasks).await
    }
    pub async fn nominate(&self, route: &Route) -> Result<WriterAdmission> {
        let mut pending = self.nomination.lock().await;
        let request = match pending.as_ref() {
            Some(prior) => prior.clone(),
            None => {
                let request = self.worker.nomination(route)?;
                *pending = Some(request.clone());
                request
            }
        };
        let result = self.worker.grant(&request).await;
        if !result
            .as_ref()
            .is_err_and(|error| error.code == "rtdb.multi/ordered-journal")
        {
            *pending = None;
        }
        result
    }

    pub(crate) async fn checkpoint_custody(
        &self,
        io: Arc<IoRegion>,
        keys: Arc<dyn KeyService>,
    ) -> Result<CheckpointCustody> {
        let bucket = self.archive.bucket(Arc::clone(&io)).await?;
        let owner = archive_owner(&self.archive, bucket.as_ref(), &self.archive_prefix).await?;
        let custody = CheckpointCustody::new(
            bucket,
            self.archive_prefix.clone(),
            self.worker.placement(),
            self.worker.log_epoch(),
            owner,
            keys,
            io,
        );
        match &self.archive {
            crate::deployment::CellStorage::S3 { .. } => Ok(custody),
            crate::deployment::CellStorage::Local { root } => {
                custody.on_local_volume(root)
            }
        }
    }

    pub(crate) async fn configuration(
        &self,
        admission: WriterAdmission,
        keys: &Arc<dyn KeyService>,
    ) -> Result<DatabaseConfig> {
        let chosen = admission.recovery.checkpoint;
        let checkpoint = if genesis(chosen) {
            None
        } else {
            let provider = self.checkpoint_custody(IoRegion::new(), Arc::clone(keys)).await?;
            Some(provider.open(chosen).await?.checkpoint)
        };
        let attachment = Attachment {
            name: self.record.name.clone(),
            deployment: self.worker.deployment(),
            admission: AttachmentAdmission::Writer { admission },
            base: Base { checkpoint },
        };
        Self::database_configuration(attachment, false)
    }

    pub(crate) async fn reader_configuration(
        &self, route: &Route, keys: &Arc<dyn KeyService>, tasks: &TaskTracker,
    ) -> Result<(DatabaseConfig, Arc<Held>)> {
        let request = self.worker.request(route, Command::ProtectRange {
            from: route.checkpoint.cut.next_lsn,
            through: route.head.next_lsn,
        })?;
        let recovery = Held::acquire(&self.worker, request, tasks).await?;
        let hold = recovery.scope();
        if hold.checkpoint != route.checkpoint || hold.start != route.checkpoint.cut || hold.through != route.head {
            return Err(invalid("The requested cold reader basis changed"));
        }
        let deadline = storage::ServingLease::default()
            .serving(self.worker.read_lease(hold, route.writer.epoch)).await?;
        let checkpoint = if genesis(hold.checkpoint) { None } else {
            Some(storage::with_read_lease(deadline, |_| async {
                let provider = self.checkpoint_custody(IoRegion::new(), Arc::clone(keys)).await?;
                Ok::<_, Anomaly>(provider.open(hold.checkpoint).await?.checkpoint)
            }).await?)
        };
        self.reader_opening.lock().clone_from(&Arc::downgrade(&recovery));
        let config = Self::database_configuration(Attachment {
            name: self.record.name.clone(),
            deployment: self.worker.deployment(),
            admission: AttachmentAdmission::Reader { placement: route.placement, epoch: route.writer.epoch, hold },
            base: Base { checkpoint },
        }, true)?;
        Ok((config, recovery))
    }

    fn retained_reader(&self, hold: Hold) -> Result<Arc<Held>> {
        self.reader_opening.lock().upgrade().filter(|active| active.scope() == hold)
            .ok_or_else(|| invalid("The attachment lost its ordered recovery hold"))
    }

    fn database_configuration(attachment: Attachment, read_only: bool) -> Result<DatabaseConfig> {
        let encoded =
            serde_json::to_vec(&attachment).map_err(|error| invalid(&error.to_string()))?;
        Ok(DatabaseConfig {
            log: Some(database_config::Log::OtherLog(Any {
                type_url: format!("type.googleapis.com/{LOG_TYPE}"),
                value: encoded.clone(),
            })),
            storage: Some(database_config::Storage::RemoteStorage(RemoteStorage {
                object_store: Some(Any {
                    type_url: format!("type.googleapis.com/{STORE_TYPE}"),
                    value: encoded,
                }),
                epoch: Some(0),
            })),
            mode: read_only.then_some(1),
            ..Default::default()
        })
    }
}

pub(crate) struct Logs {
    pub routes: Arc<Routes>,
    pub keys: Arc<dyn crate::keys::KeyService>,
    pub releases: TaskTracker,
}

#[async_trait]
impl LogFactory for Logs {
    async fn open(
        &self,
        configuration: &Any,
        access: LogAccess,
        cancellation: CancellationToken,
    ) -> Result<OpenedLogs> {
        let (placement, attachment) = self.routes.resolve(configuration)?;
        let custody = placement.hold_worker_custody(&self.releases).await?;
        let material = material(self.keys.as_ref(), &placement.record).await?;
        let max_frame = crate::process::MAXIMUM_LOG_MESSAGE_BYTES;
        let admission = match attachment.admission {
            AttachmentAdmission::Writer { admission } if access == LogAccess::ReadWrite => admission,
            AttachmentAdmission::Reader { epoch, hold, .. } if access == LogAccess::ReadOnly => {
                let replica = JournalLog::open_reader(Arc::clone(&placement.worker), Arc::clone(&self.keys),
                    epoch, placement.retained_reader(hold)?, max_frame, self.releases.clone(), custody).await?;
                return crate::logs::ordered_reader_opened(replica, &material, &placement.record, epoch,
                    attachment.base.checkpoint, max_frame);
            }
            _ => return Err(invalid("The log access does not match its installed attachment")),
        };
        let replica = JournalLog::open(
            Arc::clone(&placement.worker),
            Arc::clone(&self.keys),
            admission,
            Held::pin(
                Arc::clone(&placement.worker),
                admission.recovery,
                &self.releases,
            ),
            max_frame,
            placement.journal_window_bytes,
            self.releases.clone(),
            custody,
        )
        .await?;
        crate::logs::ordered_opened(
            replica,
            &material,
            &placement.record,
            admission,
            attachment.base.checkpoint,
            max_frame,
            cancellation,
        )
        .await
    }
}

async fn material(
    keys: &dyn KeyService,
    record: &DeploymentRecord,
) -> Result<Vec<u8>> {
    keys.existing_key(&record.key_id())
        .await?
        .map(|key| key.to_vec())
        .ok_or_else(|| invalid("The installed deployment's serving key is unavailable"))
}

pub(crate) fn checkpoint_for_block(
    block: &BlockCatalog,
    log_epoch: StorageEpoch,
) -> Option<RtdbCheckpoint> {
    let boundary = block.boundary_replica_position()?.after_offset(log_epoch);
    if boundary < 0 {
        return None;
    }
    Some(RtdbCheckpoint {
        block: block.block_index().0,
        boundary,
        source: block
            .latest_source_position()
            .map_or(-1, |position| position.after_offset(log_epoch)),
        system_time: block
            .latest_completed_tx()
            .map_or(i64::MIN, |tx| tx.system_time.0),
        root_sha256: Some(block.publication_sha256()?),
    })
}

fn genesis(checkpoint: Checkpoint) -> bool {
    checkpoint.cut.next_lsn == 0
        && checkpoint.custody.revision == 1
        && checkpoint.custody.provider == [0; 32]
        && checkpoint.root == [0; 32]
        && checkpoint.manifest == [0; 32]
}

fn invalid(message: &str) -> Anomaly {
    Anomaly::incorrect("rtdb.multi/installed-journal", message.to_owned())
}
