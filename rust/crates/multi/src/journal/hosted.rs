//! Attachment of an installed journal to RTDB's database factories.

mod storage;

use super::{
    Authority, Held, InstalledJournal, InstalledRoute, JournalLog, WriterAdmission,
    custody::{CheckpointCustody, Reservation},
};
use async_trait::async_trait;
use ordered_logkeeper::ordered::{
    Checkpoint, Command, Enrollment, Grant, Hold, Request, service::Route,
};
use prost_types::Any;
use rtdb_directory::{Checkpoint as RtdbCheckpoint, DeploymentRecord};
use rtdb_format::database::proto::{DatabaseConfig, RemoteStorage, database_config};
use rtdb_node::{LogAccess, LogFactory, OpenedLogs};
use rtdb_storage::{IoRegion, ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, Result};
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
    archive: crate::deployment::CellStorage,
    archive_prefix: String,
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
    archive: crate::deployment::CellStorage,
    archive_prefix: ObjectKey,
    endpoints: BTreeMap<Enrollment, WorkerEndpoint>,
    nomination: tokio::sync::Mutex<Option<Request>>,
    checkpoint_reservations: BTreeMap<u64, Reservation>,
    publication: tokio::sync::Mutex<()>,
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
}

#[derive(Clone, Copy, Serialize, Deserialize)]
struct Base {
    checkpoint: Option<RtdbCheckpoint>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Attachment {
    name: String,
    deployment: Uuid,
    admission: WriterAdmission,
    base: Base,
}

pub(crate) fn attachment_identity(value: &Any) -> Result<(String, u64)> {
    let attachment: Attachment =
        serde_json::from_slice(&value.value).map_err(|error| invalid(&error.to_string()))?;
    Ok((
        attachment.deployment.to_string(),
        attachment.admission.grant.epoch,
    ))
}

impl Routes {
    pub fn read(path: &Path) -> Result<Self> {
        let entries: Vec<Definition> = read_manifest(path)?;
        let mut routes = BTreeMap::new();
        for entry in entries {
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
            let name = entry.deployment.name.clone();
            let mut checkpoint_reservations = BTreeMap::new();
            for reservation in entry.checkpoint_reservations {
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
            let placement = Arc::new(Placement {
                record: entry.deployment,
                worker,
                custodian,
                archive: entry.archive,
                archive_prefix: ObjectKey::new(entry.archive_prefix)?,
                endpoints,
                nomination: Default::default(),
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

    fn resolve(&self, configuration: &Any) -> Result<(Arc<Placement>, Attachment)> {
        let attachment: Attachment = serde_json::from_slice(&configuration.value)
            .map_err(|error| invalid(&error.to_string()))?;
        let placement = self.by_name(&attachment.name).filter(|placement| {
            placement.worker.deployment() == attachment.deployment
               && matches!(placement.worker.authority(), Authority::Worker { enrollment } if enrollment == attachment.admission.grant.enrollment)
               && attachment.admission.recovery.owner == attachment.admission.grant.enrollment
        }).ok_or_else(|| invalid("The database attachment addresses a different installed journal worker"))?;
        Ok((Arc::clone(placement), attachment))
    }
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
    pub(crate) async fn publish_checkpoint(
        &self,
        keys: &dyn crate::keys::KeyService,
        grant: Grant,
        checkpoint: RtdbCheckpoint,
        source: &dyn ObjectStore,
        tasks: &TaskTracker,
    ) -> Result<()> {
        let _publication = self.publication.lock().await;
        let custodian = self
            .custodian
            .as_ref()
            .ok_or_else(|| invalid("This placement has no checkpoint custodian"))?;
        let current = self.worker.route().await?;
        if current.writer.grant != Some(grant) {
            return Err(invalid("The checkpoint writer grant has been replaced"));
        }
        let next_lsn = checkpoint
            .boundary
            .checked_add(1)
            .and_then(|offset| u64::try_from(offset).ok())
            .ok_or_else(|| invalid("The adopted checkpoint has no ordered replica boundary"))?;
        let published = current.checkpoint;
        if published.cut.next_lsn > next_lsn {
            return Err(invalid(
                "A later checkpoint already governs the installed journal",
            ));
        }
        if published.cut.next_lsn == next_lsn {
            return if checkpoint.root_sha256 == Some(published.root) {
                Ok(())
            } else {
                Err(invalid(
                    "The adopted checkpoint differs from the installed root",
                ))
            };
        }
        let revision = published
            .custody
            .revision
            .checked_add(1)
            .ok_or_else(|| invalid("The checkpoint custody revision space is exhausted"))?;
        let reservation = self
            .checkpoint_reservations
            .get(&revision)
            .ok_or_else(|| invalid("The next exact checkpoint allocation is unfunded"))?;
        let intent = self.worker.request(
            &current,
            Command::ProtectRange {
                grant,
                from: next_lsn,
                through: next_lsn,
            },
        )?;
        let through = Held::acquire(&self.worker, intent, tasks).await?;
        let material = material(keys, &self.record).await?;
        let io = IoRegion::new();
        let provider = self.checkpoint_custody(Arc::clone(&io), &material).await?;
        let staged = provider
            .prepare(reservation, source, through.scope().through, checkpoint)
            .await?;
        let current = custodian.route().await?;
        provider.publish(custodian, &current, grant, staged).await
    }
    async fn pin_root(&self, source: Hold, tasks: &TaskTracker) -> Result<Arc<Held>> {
        let route = self.worker.route().await?;
        let request = self
            .worker
            .request(&route, Command::ProtectRoot { source })?;
        Held::acquire(&self.worker, request, tasks).await
    }
    pub fn endpoint(&self, enrollment: Enrollment) -> Option<&WorkerEndpoint> {
        self.endpoints.get(&enrollment)
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
        material: &[u8],
    ) -> Result<CheckpointCustody> {
        let bucket = self.archive.bucket(io).await?;
        let store: Arc<dyn ObjectStore> =
            Arc::new(crate::store::EncryptingStore::new(bucket, material)?);
        Ok(CheckpointCustody::new(
            store,
            self.archive_prefix.clone(),
            self.worker.placement(),
            self.worker.log_epoch(),
        ))
    }

    pub(crate) async fn configuration(
        &self,
        admission: WriterAdmission,
        keys: &dyn crate::keys::KeyService,
    ) -> Result<DatabaseConfig> {
        let chosen = admission.recovery.checkpoint;
        let checkpoint = if genesis(chosen) {
            None
        } else {
            let material = material(keys, &self.record).await?;
            let provider = self.checkpoint_custody(IoRegion::new(), &material).await?;
            Some(provider.open(chosen).await?.checkpoint)
        };
        let attachment = Attachment {
            name: self.record.name.clone(),
            deployment: self.worker.deployment(),
            admission,
            base: Base { checkpoint },
        };
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
        if access != LogAccess::ReadWrite {
            return Err(invalid(
                "An installed writer attachment requires write log access",
            ));
        }
        let (placement, attachment) = self.routes.resolve(configuration)?;
        let material = material(self.keys.as_ref(), &placement.record).await?;
        let max_frame = crate::process::MAXIMUM_LOG_MESSAGE_BYTES;
        let replica = JournalLog::open(
            Arc::clone(&placement.worker),
            attachment.admission,
            Held::pin(
                Arc::clone(&placement.worker),
                attachment.admission.recovery,
                &self.releases,
            ),
            max_frame,
            self.releases.clone(),
        )
        .await?;
        crate::logs::ordered_opened(
            replica,
            &material,
            &placement.record,
            attachment.admission,
            attachment.base.checkpoint,
            max_frame,
            cancellation,
        )
        .await
    }
}

async fn material(
    keys: &dyn crate::keys::KeyService,
    record: &DeploymentRecord,
) -> Result<Vec<u8>> {
    keys.existing_key(&record.key_id())
        .await?
        .map(|key| key.to_vec())
        .ok_or_else(|| invalid("The installed deployment's serving key is unavailable"))
}

fn genesis(checkpoint: Checkpoint) -> bool {
    checkpoint.cut.next_lsn == 0
        && checkpoint.custody.revision == 1
        && checkpoint.root == [0; 32]
        && checkpoint.manifest == [0; 32]
}

fn invalid(message: &str) -> Anomaly {
    Anomaly::incorrect("rtdb.multi/installed-journal", message.to_owned())
}
