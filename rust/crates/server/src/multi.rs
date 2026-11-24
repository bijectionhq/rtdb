use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{self, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};

use journal_contract::{
    Identity, Journal, JournalError, StreamId,
    reference::{FileJournal, Limits},
};
use rtdb_auth::{AuthConfig, Authenticator};
use rtdb_columnar::MemoryBudget;
use rtdb_crypt::{FileKms, Generation, Kms};
use rtdb_db::{
    DeploymentRetirement, JournalBatchAppender, MultiDatabase, MultiOptions, Role, discover_nodes,
};
use rtdb_format::database::proto::{DatabaseConfig, LocalStorage, database_config::Storage};
use rtdb_node::{
    Clock, DatabaseStorage, DeploymentTransfer, LogAccess, ManagedCheckpointRead,
    ManagedCheckpointSource, MultiServing, NativeResources, Node, OpenedLogs, ResourceFactory,
    SystemClock, serving::ServingDriver,
};
use rtdb_storage::{IoRegion, LocalStore, ObjectStore};
use rtdb_stream_journal::{Credentials, StreamJournal};
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp,
    rtdb_v1::{Basis, ManagedCheckpointReservation, ManagedCheckpointTarget, PinnedCheckpointIdentity},
};
use sha2::{Digest, Sha256};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use uuid::Uuid;

use crate::config::NodeConfiguration;

pub use rtdb_flight_sql::NativeFollowerAssignment;

mod catalog;
mod managed_transfer;
mod native_checkpoint;
mod peer;
use catalog::{Status, TransferExport};

const WRITER_LEASE_MICROS: i64 = 5_000_000;
const CLOCK_DRIFT_MICROS: i64 = 250_000;
pub const DEVELOPMENT_DATABASE: &str = "xtdb";

type ServingDatabases = Arc<RwLock<BTreeMap<String, Arc<MultiServing>>>>;

pub struct DevelopmentStreamJournal {
    url: String,
    credentials: Credentials,
}

impl DevelopmentStreamJournal {
    pub fn new(url: &str, credentials: Credentials) -> Result<Self> {
        let invalid_url = || invalid("Multi development requires exactly three distinct loopback Logkeeper TLS addresses");
        let addresses = url.strip_prefix("logkeeper+tls://").ok_or_else(invalid_url)?.split(',');
        let endpoints = addresses.clone().map(str::parse::<SocketAddr>)
            .collect::<std::result::Result<BTreeSet<_>, _>>().map_err(|_| invalid_url())?;
        if addresses.count() != 3 || endpoints.len() != 3
            || endpoints.iter().any(|endpoint| !endpoint.ip().is_loopback() || endpoint.port() == 0)
        {
            return Err(invalid_url());
        }
        Ok(Self { url: url.to_owned(), credentials })
    }

    async fn open(&self, path: &Path) -> Result<(Arc<StreamJournal>, Arc<File>)> {
        let expected: [u8; 32] = Sha256::digest(self.url.as_bytes()).into();
        if load_or_create(path, || expected)? != expected {
            return Err(invalid("Multi development data belongs to another journal URL"));
        }
        let lock = OpenOptions::new().read(true).write(true).open(path).map_err(io_error)?;
        match fs2::FileExt::try_lock_exclusive(&lock) {
            Ok(()) => (),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb.multi/development-journal",
                    "Multi development data already has a local writer"));
            }
            Err(error) => return Err(io_error(error)),
        }
        let journal = connect_stream_journal(&self.url, &self.credentials).await?;
        Ok((journal, Arc::new(lock)))
    }
}

#[derive(Clone)]
pub struct MultiRuntime {
    databases: ServingDatabases,
    catalog: Arc<catalog::Catalog>,
    bootstrap: Arc<BTreeMap<String, Uuid>>,
    configuration: DatabaseConfig,
    memory: Arc<MemoryBudget>,
    kms: Arc<dyn Kms>,
    backends: Backends,
    peer: Option<Arc<peer::Peer>>,
    managed_sources: Arc<BTreeMap<Uuid, Arc<dyn ManagedCheckpointSource>>>,
    native_checkpoint_source: Option<native_checkpoint::NativeCheckpointSource>,
    managed_operations: TaskTracker,
    follower: Option<NativeFollowerAssignment>,
}

impl MultiRuntime {
    /// Binds an existing local replica to a private listener for the lifetime of one cell assignment.
    /// The cell supplies an assignment-specific authenticator and TLS identity and owns listener shutdown.
    pub async fn bind_native_follower(
        &self, node: &Node, assignment: NativeFollowerAssignment, address: std::net::SocketAddr,
        authenticator: Arc<Authenticator>, tls: tokio_rustls::rustls::ServerConfig,
    ) -> Result<(rtdb_flight_sql::ServerHandle, rtdb_flight_sql::ServerDriver)> {
        validate_deployment_name(&assignment.database)?;
        if !node.catalog().is_managed() || self.follower.as_ref().is_some_and(|installed| installed != &assignment) {
            return Err(follower_unavailable());
        }
        require_ready_follower(&self.catalog.read().await?, &assignment)?;
        let attachment = node.catalog().database(&assignment.database).map_err(|_| follower_unavailable())?;
        let driver = self.follower_driver(&assignment)?;
        let reader = driver.assign_follower().await?;
        rtdb_flight_sql::ServerDriver::bind_follower(address, node.clone(), authenticator, tls, assignment, &attachment, reader).await
    }

    pub async fn development(
        path: &Path,
        config: &mut NodeConfiguration,
        memory: Arc<MemoryBudget>,
    ) -> Result<Self> {
        Self::development_with_incarnation(path, config, memory, None, None).await
    }

    pub async fn development_with_incarnation(
        path: &Path,
        config: &mut NodeConfiguration,
        memory: Arc<MemoryBudget>,
        expected: Option<Uuid>,
        stream_journal: Option<DevelopmentStreamJournal>,
    ) -> Result<Self> {
        if expected.is_some_and(|id| id.is_nil()) {
            return Err(invalid("Multi development requires a nonzero incarnation"));
        }
        fs::create_dir_all(path).map_err(io_error)?;
        let vault = path.join("keys");
        let database = Uuid::from_bytes(load_or_create(&path.join("database-id"), || {
            *expected.unwrap_or_else(Uuid::new_v4).as_bytes()
        })?);
        if expected.is_some_and(|expected| database != expected) {
            return Err(invalid("Multi development data belongs to another incarnation"));
        }
        let journal_path = path.join("journal");
        let (journal, batch, writer_lock): (Arc<dyn Journal>, Option<Arc<dyn JournalBatchAppender>>, _) =
            match stream_journal {
                Some(stream) => {
                    let (journal, lock) = stream.open(&journal_path).await?;
                    let batch = Arc::new(StreamJournalBatch(Arc::clone(&journal)));
                    (journal, Some(batch), Some(lock))
                }
                None => {
                    let journal = FileJournal::open(journal_path, Limits::default()).map_err(io_error)?;
                    let identity = Identity::from(format!("urn:rtdb:dev:{}", Uuid::new_v4()));
                    (Arc::new(journal.handle(identity)), None, None)
                }
            };
        let io = IoRegion::new();
        let store = Arc::new(LocalStore::open_in(path.join("objects"), Arc::clone(&io))?);
        let path = path
            .join("catalog")
            .into_os_string()
            .into_string()
            .map_err(|_| invalid("--data must be a UTF-8 path"))?;
        config.primary = rtdb_format::config::default_database_config();
        config.primary.storage = Some(Storage::LocalStorage(LocalStorage {
            path: Some(path),
            epoch: Some(0),
        }));
        let backends = Backends {
            journal,
            batch,
            store,
            io,
            vault,
            node: Ipv4Addr::LOCALHOST.into(),
            peers: None,
            _journal_lock: writer_lock,
        };
        Self::open(
            config,
            memory,
            BTreeMap::from([(DEVELOPMENT_DATABASE.into(), database)]),
            backends,
            None,
        )
        .await
    }

    pub async fn await_development_writer(&self, node: &Node, incarnation: Uuid) -> Result<u64> {
        if self.bootstrap.get(DEVELOPMENT_DATABASE) != Some(&incarnation)
            || self.backends.peers.is_some()
        {
            return Err(invalid(
                "Multi development readiness requires the exact local bootstrap incarnation",
            ));
        }
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                match self.ready_driver(DEVELOPMENT_DATABASE, incarnation).await {
                    Ok(driver) => {
                        if let Some(epoch) = driver.local_writer_epoch().await {
                            let attachment = node.catalog().database(DEVELOPMENT_DATABASE)?;
                            if attachment.wire_incarnation().await? != incarnation {
                                return Err(invalid("Multi development listener attached another incarnation"));
                            }
                            return Ok(epoch);
                        }
                    }
                    Err(error) if error.code == "rtdb/unknown-deployment" || retryable(&error) => (),
                    Err(error) => return Err(error),
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .map_err(|_| {
            Anomaly::new(
                AnomalyCategory::Unavailable,
                "rtdb.multi/development-writer",
                "Multi development writer did not become ready",
            )
        })?
    }

    #[cfg(feature = "s3")]
    pub async fn production(
        config: &mut NodeConfiguration,
        memory: Arc<MemoryBudget>,
    ) -> Result<Self> {
        Self::production_mode(config, memory, None).await
    }

    #[cfg(feature = "s3")]
    pub async fn production_follower(
        config: &mut NodeConfiguration,
        memory: Arc<MemoryBudget>,
        assignment: NativeFollowerAssignment,
    ) -> Result<Self> {
        Self::production_mode(config, memory, Some(assignment)).await
    }

    #[cfg(feature = "s3")]
    async fn production_mode(
        config: &mut NodeConfiguration,
        memory: Arc<MemoryBudget>,
        follower: Option<NativeFollowerAssignment>,
    ) -> Result<Self> {
        let node = installed_node_ip().await?;
        let peers = if follower.is_none() { Some(required("RTDB_PEERS")?) } else { None };
        let databases = serde_json::from_str(&required("RTDB_DATABASES")?)
            .map_err(|_| invalid("RTDB_DATABASES must map database names to UUIDs"))?;
        let vault = PathBuf::from(required("RTDB_KEY_DIRECTORY")?);
        let slots = required("RTDB_JOURNAL_SLOTS")?
            .parse::<usize>()
            .ok()
            .filter(|slots| (1..=8).contains(slots))
            .ok_or_else(|| invalid("RTDB_JOURNAL_SLOTS must be between 1 and 8"))?;
        let (slot, lock) = claim_credential_slot(&vault, slots)?;
        let bucket = required("RTDB_BUCKET")?;
        let mut s3 = rtdb_s3::S3Config::new(bucket);
        s3.region = Some(required("AWS_REGION")?);
        let journal_url = required("RTDB_JOURNAL_URL")?;
        let directory = tempfile::tempdir().map_err(io_error)?;
        let credentials = Credentials {
            certificate: credential(
                directory.path(),
                "cert.pem",
                &format!("RTDB_JOURNAL_{slot}_CERTIFICATE"),
            )?,
            private_key: credential(
                directory.path(),
                "key.pem",
                &format!("RTDB_JOURNAL_{slot}_PRIVATE_KEY"),
            )?,
            ca: credential(
                directory.path(),
                "ca.pem",
                &format!("RTDB_JOURNAL_{slot}_CA_CERTIFICATE"),
            )?,
        };
        let journal = connect_stream_journal(&journal_url, &credentials).await?;
        let io = IoRegion::new();
        let store = s3
            .open(
                &rtdb_storage::ObjectKey::new("multi")?,
                &BTreeMap::new(),
                Arc::clone(&io),
            )
            .await?;
        let backends = Backends {
            batch: follower.is_none().then(|| {
                Arc::new(StreamJournalBatch(Arc::clone(&journal))) as Arc<dyn JournalBatchAppender>
            }),
            journal,
            store,
            io,
            vault,
            node,
            peers,
            _journal_lock: Some(Arc::new(lock)),
        };
        let mut runtime = Self::open(config, memory, databases, backends, follower).await?;
        if runtime.follower.is_none() {
            runtime.native_checkpoint_source = Some(native_checkpoint::NativeCheckpointSource);
        }
        Ok(runtime)
    }

    async fn open(
        config: &mut NodeConfiguration,
        memory: Arc<MemoryBudget>,
        databases: BTreeMap<String, Uuid>,
        backends: Backends,
        follower: Option<NativeFollowerAssignment>,
    ) -> Result<Self> {
        if !databases.contains_key("xtdb")
            || databases.values().any(Uuid::is_nil)
            || databases.keys().any(|name| name.is_empty())
            || databases.values().collect::<BTreeSet<_>>().len() != databases.len()
        {
            return Err(invalid(
                "Multi requires xtdb and distinct nonzero database IDs",
            ));
        }
        if follower.is_none() {
            FileKms::create_directory(&backends.vault)?;
        }
        let kms: Arc<dyn Kms> = Arc::new(FileKms::open(&backends.vault, || SystemClock.now().0)?);
        let control = catalog::identity(b"catalog", databases["xtdb"], "");
        if databases.values().any(|id| *id == control) {
            return Err(invalid(
                "A bootstrap database cannot use the deployment catalog identity",
            ));
        }
        let control_open = if follower.is_some() { DatabaseOpen::Prepared } else { DatabaseOpen::Required };
        let control = open_database(&backends, &kms, control, control_open).await?;
        let workload_profile = assignment_peer_profile(&config.auth);
        let peer = backends.peers.as_ref().map(|_| {
            peer::Peer::new(
                backends.node,
                control.token_authority(),
                Arc::clone(&memory),
                workload_profile,
            )
        });
        let catalog = catalog::Catalog::new(control, peer.clone());
        let observed = catalog.read().await?;
        let mut serving = BTreeMap::new();
        for (name, id) in &databases {
            if observed
                .get(name)
                .is_some_and(|entry| entry.database != *id)
                || observed
                    .iter()
                    .any(|(other, entry)| other != name && entry.database == *id)
            {
                return Err(invalid(
                    "The bootstrap map conflicts with the ordered deployment catalog",
                ));
            }
        }
        let initial = if let Some(assignment) = &follower {
            require_ready_follower(&observed, assignment)?;
            BTreeMap::from([(assignment.database.clone(), assignment.incarnation)])
        } else {
            databases
                .iter()
                .filter(|(name, _)| {
                    observed
                        .get(*name)
                        .is_none_or(|entry| matches!(entry.status, Status::Reserved | Status::Ready))
                })
                .map(|(name, id)| (name.clone(), *id))
                .chain(
                    observed
                        .iter()
                        .filter(|(_, entry)| entry.status.ready())
                        .map(|(name, entry)| (name.clone(), entry.database)),
                )
                .collect()
        };
        let database_open = if follower.is_some() { DatabaseOpen::Prepared } else { DatabaseOpen::Startup };
        for (name, id) in initial {
            let database = open_database(&backends, &kms, id, database_open).await?;
            serving.insert(
                name.clone(),
                serving_driver(database, &name, &memory, peer.as_ref()),
            );
        }
        config.fixed_databases = Some(
            serving
                .keys()
                .map(|name| (name.clone(), config.primary.clone()))
                .collect(),
        );
        config.server = None;
        config.indexer.enabled = false;
        config.garbage.enabled = false;
        config.playground = false;
        Ok(Self {
            databases: Arc::new(RwLock::new(serving)),
            catalog: Arc::new(catalog),
            bootstrap: Arc::new(databases),
            configuration: config.primary.clone(),
            memory,
            kms,
            backends,
            peer,
            managed_sources: Arc::default(),
            native_checkpoint_source: None,
            managed_operations: TaskTracker::new(),
            follower,
        })
    }

    pub(crate) fn resource_override(
        &self,
    ) -> Box<dyn FnOnce(NativeResources) -> Arc<dyn ResourceFactory> + Send> {
        let runtime = self.clone();
        Box::new(move |native| Arc::new(ServingResources { native, runtime }))
    }

    pub(crate) fn validate_assignment_peer_auth(&self, auth: &AuthConfig) -> Result<()> {
        if let Some(peer) = &self.peer
            && peer.workload_profile() != assignment_peer_profile(auth).as_ref()
        {
            return Err(invalid("Multi authentication must match the assignment peer profile"));
        }
        Ok(())
    }

    async fn provision(&self, name: &str, expected: Option<Uuid>) -> Result<(DatabaseConfig, Uuid)> {
        validate_deployment_name(name)?;
        let bootstrap = self.bootstrap.get(name).copied();
        let registered = self.catalog.read().await?.get(name).map(|entry| entry.database);
        let id = expected
            .or(bootstrap)
            .or(registered)
            .unwrap_or_else(|| self.catalog.deployment_id(name));
        if bootstrap.is_some_and(|configured| configured != id)
            || self.bootstrap.iter().any(|(owner, configured)| owner != name && *configured == id)
        {
            return Err(catalog::conflict());
        }
        let selected = self.catalog.reserve(name, id).await?;
        match selected.status {
            Status::Reserved | Status::Ready => (),
            _ => return Err(catalog::closed()),
        }
        let driver = self.ensure_driver(name, id).await?;
        if driver.deployment_retirement().await?.is_some() {
            return Err(catalog::closed());
        }
        if selected.status == Status::Reserved {
            self.catalog.mark_ready(name, id).await?;
        }
        Ok((self.configuration.clone(), id))
    }

    async fn export(
        &self,
        name: &str,
        pin: Uuid,
        basis: Basis,
        until: Option<Timestamp>,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>> {
        let entry = self
            .catalog
            .read()
            .await?
            .remove(name)
            .filter(|entry| entry.status.ready())
            .ok_or_else(unknown_deployment)?;
        self.publish_export(name, entry.database, pin, basis, until, cancellation)
            .await
    }

    async fn publish_export(
        &self,
        name: &str,
        database: Uuid,
        pin: Uuid,
        basis: Basis,
        until: Option<Timestamp>,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>> {
        if let Some(peer) = &self.peer
            && let Some(ticket) = cancellation
                .run_until_cancelled(peer.forward_export(name, database, pin, basis, until))
                .await
                .ok_or_else(transfer_interrupted)??
        {
            return Ok(ticket);
        }
        self.ready_driver(name, database)
            .await?
            .export_checkpoint(pin, basis, until, cancellation)
            .await
    }

    async fn ready_driver(&self, name: &str, database: Uuid) -> Result<Arc<MultiServing>> {
        let entry = self
            .catalog
            .read()
            .await?
            .remove(name)
            .filter(|entry| entry.status.ready() && entry.database == database)
            .ok_or_else(unknown_deployment)?;
        self.ensure_driver(name, entry.database).await
    }

    async fn transfer(
        &self,
        source: &str,
        destination: &str,
        transfer: DeploymentTransfer,
        cancellation: &CancellationToken,
    ) -> Result<(DatabaseConfig, Basis)> {
        validate_deployment_name(source)?;
        validate_deployment_name(destination)?;
        if source == destination {
            return Err(invalid(
                "A checkpoint transfer requires separate source and destination deployments",
            ));
        }
        let request = transfer_request_digest(source, &transfer);
        let mut rows = self.catalog.read().await?;
        let selected = match rows.remove(destination) {
            Some(entry) => entry,
            None => {
                let original = rows
                    .remove(source)
                    .filter(|entry| entry.status.ready())
                    .ok_or_else(unknown_deployment)?;
                let export = match transfer {
                    DeploymentTransfer::Restore { ticket } => {
                        let driver = self.ensure_driver(source, original.database).await?;
                        let export = driver
                            .open_checkpoint_export(ticket.clone(), cancellation)
                            .await?;
                        TransferExport::Ticket {
                            manifest: export.manifest_digest(),
                            ticket,
                        }
                    }
                    DeploymentTransfer::Fork {
                        pin,
                        basis,
                        retain_until,
                    } => {
                        if basis.incarnation() != *original.database.as_bytes() {
                            return Err(invalid(
                                "The fork basis belongs to a different source incarnation",
                            ));
                        }
                        let native = self.native_checkpoint_source.is_some()
                            && !self.managed_sources.contains_key(&original.database)
                            && self.ready_driver(source, original.database).await?.has_native_checkpoint_custody().await?;
                        if native {
                            if retain_until.is_some() || pin.is_nil() {
                                return Err(Anomaly::incorrect("rtdb/native-checkpoint-pin", "A native custody fork requires a nonzero pin without a deadline"));
                            }
                            TransferExport::Managed { pin, basis, manifest: None }
                        } else {
                            TransferExport::Fork { pin, basis, retain_until }
                        }
                    }
                    DeploymentTransfer::ManagedFork { pin, basis } => {
                        if pin.is_nil() || basis.incarnation() != *original.database.as_bytes() {
                            return Err(managed_transfer::reservation_conflict());
                        }
                        self.require_managed_source(original.database)?;
                        if !self.managed_sources.contains_key(&original.database)
                            && !self.ready_driver(source, original.database).await?.has_native_checkpoint_custody().await?
                        {
                            return Err(native_checkpoint::inventory_required());
                        }
                        TransferExport::Managed { pin, basis, manifest: None }
                    }
                };
                self.catalog
                    .reserve_transfer(
                        destination,
                        self.catalog.deployment_id(destination),
                        catalog::Transfer {
                            request,
                            source: source.into(),
                            incarnation: original.database,
                            export,
                        },
                    )
                    .await?
            }
        };
        let basis = match selected.status {
            Status::TransferReady {
                request: prior,
                basis,
            } if prior == request => basis,
            Status::ManagedTransferReady { transfer, basis }
                if transfer.reservation.target.request == request => basis,
            Status::TransferPending(pending) if pending.request == request
                && matches!(pending.export, TransferExport::Managed { .. }) =>
                self.finish_managed_transfer(destination, selected.database, request, cancellation).await?,
            Status::TransferPending(pending) if pending.request == request => {
                self.finish_transfer(destination, selected.database, &pending, cancellation)
                    .await?
            }
            _ => {
                return Err(Anomaly::new(
                    AnomalyCategory::Conflict,
                    "rtdb/deployment-transfer-conflict",
                    "The destination is already assigned to a different operation",
                ));
            }
        };
        Ok((self.configuration.clone(), basis))
    }

    async fn finish_transfer(
        &self,
        name: &str,
        database: Uuid,
        pending: &catalog::Transfer,
        cancellation: &CancellationToken,
    ) -> Result<Basis> {
        let bound;
        let pending = match &pending.export {
            TransferExport::Fork {
                pin,
                basis,
                retain_until,
            } => {
                let source = self.transfer_source(pending).await?;
                let ticket = self
                    .publish_export(
                        &pending.source,
                        pending.incarnation,
                        *pin,
                        *basis,
                        *retain_until,
                        cancellation,
                    )
                    .await?;
                let export = source
                    .open_checkpoint_export(ticket.clone(), cancellation)
                    .await?;
                bound = self
                    .catalog
                    .bind_transfer(
                        name,
                        database,
                        pending.request,
                        export.manifest_digest(),
                        ticket,
                    )
                    .await?;
                match &bound {
                    Status::TransferReady { basis, .. } => return Ok(*basis),
                    Status::TransferPending(bound) => bound,
                    _ => return Err(invalid("The ordered transfer lost its bound reservation")),
                }
            }
            TransferExport::Ticket { .. } => pending,
            TransferExport::Managed { .. } => return Err(managed_transfer::reservation_conflict()),
        };
        let remote = if let Some(peer) = &self.peer {
            cancellation
                .run_until_cancelled(peer.forward_restore(name, database, pending.request))
                .await
                .ok_or_else(transfer_interrupted)??
        } else {
            None
        };
        let basis = match remote {
            Some(basis) => basis,
            None => {
                self.install_transfer(name, database, pending, cancellation)
                    .await?
            }
        };
        self.catalog
            .mark_transfer_ready(name, database, pending.request, basis)
            .await?;
        Ok(basis)
    }

    async fn install_peer_transfer(
        &self,
        name: &str,
        database: Uuid,
        request: [u8; 32],
    ) -> Result<Basis> {
        let entry = self
            .catalog
            .read()
            .await?
            .remove(name)
            .filter(|entry| entry.database == database)
            .ok_or_else(unknown_deployment)?;
        match &entry.status {
            Status::TransferReady {
                request: prior,
                basis,
            } if *prior == request => Ok(*basis),
            Status::TransferPending(pending) if pending.request == request => {
                self.install_transfer(name, database, pending, &CancellationToken::new())
                    .await
            }
            _ => Err(Anomaly::new(
                AnomalyCategory::Conflict,
                "rtdb/deployment-transfer-conflict",
                "The destination has no matching ordered checkpoint reservation",
            )),
        }
    }

    async fn install_transfer(
        &self,
        name: &str,
        database: Uuid,
        pending: &catalog::Transfer,
        cancellation: &CancellationToken,
    ) -> Result<Basis> {
        let TransferExport::Ticket { manifest, ticket } = &pending.export else {
            return Err(invalid(
                "The ordered transfer is missing its bound checkpoint",
            ));
        };
        let destination = self.ensure_driver(name, database).await?;
        let basis = match destination.restored_checkpoint_basis(*manifest).await? {
            Some(basis) => basis,
            None => {
                let source = self.transfer_source(pending).await?;
                let export = source
                    .open_checkpoint_export(ticket.clone(), cancellation)
                    .await?;
                if export.manifest_digest() != *manifest {
                    return Err(invalid(
                        "The source pin no longer names the reserved transfer",
                    ));
                }
                destination
                    .restore_checkpoint(export, cancellation)
                    .await?
                    .basis
            }
        };
        Ok(basis)
    }

    async fn transfer_source(&self, pending: &catalog::Transfer) -> Result<Arc<MultiServing>> {
        let original = self
            .catalog
            .read()
            .await?
            .remove(&pending.source)
            .filter(|entry| entry.status.ready() && entry.database == pending.incarnation)
            .ok_or_else(unknown_deployment)?;
        self.ensure_driver(&pending.source, original.database).await
    }

    async fn erase(&self, name: &str) -> Result<(Vec<u8>, Timestamp)> {
        let selected = self
            .catalog
            .read()
            .await?
            .remove(name)
            .ok_or_else(unknown_deployment)?;
        let selected = match &selected.status {
            Status::Reserved | Status::TransferPending(_) => {
                return Err(Anomaly::new(
                    AnomalyCategory::Busy,
                    "rtdb/deployment-provisioning",
                    "The deployment is waiting for provisioning",
                ));
            }
            Status::Ready | Status::TransferReady { .. } | Status::ManagedTransferReady { .. } => {
                let remote = if let Some(peer) = &self.peer {
                    peer.forward_retirement(name, selected.database).await?
                } else {
                    None
                };
                let event = match remote {
                    Some(event) => event,
                    None => {
                        self.ensure_driver(name, selected.database)
                            .await?
                            .retire_deployment()
                            .await?
                    }
                };
                self.record_retirement(name, event).await?
            }
            _ => selected,
        };
        let selected = self.settle_retirement(name, selected).await?;
        self.catalog.erasure_evidence(name, selected).await
    }

    async fn record_retirement(
        &self,
        name: &str,
        event: DeploymentRetirement,
    ) -> Result<catalog::Deployment> {
        let destroy_after = Timestamp(event.lease_until.saturating_add(CLOCK_DRIFT_MICROS));
        self.catalog.mark_retiring(name, event, destroy_after).await
    }

    async fn settle_retirement(
        &self,
        name: &str,
        entry: catalog::Deployment,
    ) -> Result<catalog::Deployment> {
        let Status::Retiring(retirement) = entry.status else {
            return Ok(entry);
        };
        let generation = Generation {
            database: entry.database,
            number: retirement.generation,
        };
        self.kms
            .retire(generation, retirement.destroy_after.0)
            .await?;
        if SystemClock.now() < retirement.destroy_after {
            return Err(Anomaly::new(
                AnomalyCategory::Busy,
                "rtdb/deployment-retirement-pending",
                "The deployment key retirement is waiting for its cutoff",
            ));
        }
        self.kms.destroy(generation).await?;
        let stream = StreamId(entry.database.as_u128());
        match self.backends.journal.delete(stream).await {
            Ok(()) | Err(JournalError::Deleted) | Err(JournalError::Unavailable) => (),
            Err(error) => {
                return Err(Anomaly::fault(
                    "rtdb/deployment-journal-delete",
                    error.to_string(),
                ));
            }
        }
        match self.backends.journal.head(stream).await {
            Err(JournalError::Deleted) => (),
            Ok(_) | Err(JournalError::Unavailable) => {
                return Err(Anomaly::new(
                    AnomalyCategory::Unavailable,
                    "rtdb/deployment-journal-delete",
                    "The permanent journal deletion has not been observed",
                ));
            }
            Err(error) => {
                return Err(Anomaly::fault(
                    "rtdb/deployment-journal-delete",
                    error.to_string(),
                ));
            }
        }
        let confirmed = Timestamp(
            SystemClock
                .now()
                .0
                .saturating_add(CLOCK_DRIFT_MICROS)
                .max(retirement.destroy_after.0),
        );
        self.catalog.mark_erased(name, retirement, confirmed).await
    }

    #[cfg(feature = "s3")]
    pub(crate) fn follower_assignment(&self) -> Option<&NativeFollowerAssignment> {
        self.follower.as_ref()
    }

    fn follower_driver(&self, assignment: &NativeFollowerAssignment) -> Result<Arc<MultiServing>> {
        self.databases.read().expect("Multi serving catalog").get(&assignment.database)
            .filter(|driver| driver.incarnation() == *assignment.incarnation.as_bytes())
            .cloned().ok_or_else(follower_unavailable)
    }

    pub async fn drive(&self, node: &Node, stop: &CancellationToken) -> Result<()> {
        if let Some(assignment) = &self.follower {
            let replica = self.follower_driver(assignment)?;
            return drive_multi(stop, async || match replica.replay_follower().await {
                Err(error) if retryable(&error) => Ok(()),
                result => result,
            }).await;
        }
        tokio::select! {
            result = self.drive_databases(node, stop) => result,
            result = self.drive_managed_transfers(stop), if self.native_checkpoint_source.is_some() || !self.managed_sources.is_empty() => result,
            result = async { self.peer.as_ref().expect("peer enabled").serve(self, node, stop).await },
                if self.peer.is_some() => result,
        }
    }

    async fn drive_databases(&self, node: &Node, stop: &CancellationToken) -> Result<()> {
        let maintenance = stop.child_token();
        let result = tokio::try_join!(
            async {
                let result = drive_multi(stop, async || self.tick(node).await).await;
                maintenance.cancel();
                result
            },
            drive_multi(&maintenance, async || {
                let databases = self.databases.read().expect("Multi serving catalog")
                    .values().cloned().collect::<Vec<_>>();
                for driver in databases {
                    match driver.maintain_ranked_catalog(&maintenance).await {
                        Err(error) if !matches!(error.code.as_str(), "rtdb/no-writer" | "rtdb/interrupted") => {
                            eprintln!("Multi ranked catalogue publication deferred: {error}");
                        }
                        _ => (),
                    }
                }
                Ok(())
            }),
        );
        result.map(|_| ())
    }

    async fn tick(&self, node: &Node) -> Result<()> {
        let live = match &self.backends.peers {
            None => BTreeSet::from([self.backends.node]),
            Some(host) => match discover_nodes(host).await {
                Ok(live) => live,
                Err(error) if error.category == AnomalyCategory::Unavailable => return Ok(()),
                Err(error) => return Err(error),
            },
        };
        if let Some(peer) = &self.peer {
            peer.observe(&live);
        }
        match self.reconcile(node, &live).await {
            Ok(()) => (),
            Err(error) if retryable(&error) => (),
            Err(error) => return Err(error),
        }
        let databases = self
            .databases
            .read()
            .expect("Multi serving catalog")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for driver in databases {
            match driver.tick(&live).await {
                Ok(_) => (),
                Err(error) if retryable(&error) => (),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    async fn reconcile(&self, node: &Node, live: &BTreeSet<IpAddr>) -> Result<()> {
        let writer = matches!(self.catalog.tick(live).await?, Role::Writer(_));
        let mut rows = self.catalog.read().await?;
        if writer {
            for (name, id) in self.bootstrap.iter() {
                if !rows.contains_key(name) {
                    rows.insert(name.clone(), self.catalog.reserve(name, *id).await?);
                }
            }
        }
        for (name, entry) in rows {
            match self.reconcile_entry(node, &name, entry, writer).await {
                Ok(()) => (),
                Err(error) if retryable(&error) => (),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    async fn reconcile_entry(
        &self,
        node: &Node,
        name: &str,
        entry: catalog::Deployment,
        writer: bool,
    ) -> Result<()> {
        match &entry.status {
            Status::Reserved if !writer => return Ok(()),
            Status::TransferPending(pending) if matches!(pending.export, TransferExport::Managed { .. }) => return Ok(()),
            Status::TransferPending(pending) => {
                if writer {
                    let cancellation = CancellationToken::new();
                    match self
                        .finish_transfer(name, entry.database, pending, &cancellation)
                        .await
                    {
                        Ok(_) => self.attach(node, name).await?,
                        Err(error)
                            if retryable(&error)
                                || matches!(
                                    error.category,
                                    AnomalyCategory::Incorrect
                                        | AnomalyCategory::NotFound
                                        | AnomalyCategory::Forbidden
                                        | AnomalyCategory::Unsupported
                                ) =>
                        {
                            ()
                        }
                        Err(error) => return Err(error),
                    }
                }
                return Ok(());
            }
            Status::Retiring(_) | Status::Erased { .. } => {
                self.databases
                    .write()
                    .expect("Multi serving catalog")
                    .remove(name);
                if node.catalog().database(name).is_ok() {
                    node.detach_local(name.to_owned()).await?;
                }
                if writer {
                    self.settle_retirement(name, entry).await?;
                }
                return Ok(());
            }
            _ => (),
        }
        let driver = self.ensure_driver(name, entry.database).await?;
        if let Some(event) = driver.deployment_retirement().await? {
            if writer {
                self.record_retirement(name, event).await?;
            }
            return Ok(());
        }
        if entry.status == Status::Reserved {
            self.catalog.mark_ready(name, entry.database).await?;
        }
        self.attach(node, name).await
    }

    async fn ensure_driver(&self, name: &str, id: Uuid) -> Result<Arc<MultiServing>> {
        if let Some(driver) = self
            .databases
            .read()
            .expect("Multi serving catalog")
            .get(name)
        {
            check_incarnation(driver, id)?;
            return Ok(Arc::clone(driver));
        }
        let mut database = open_database(&self.backends, &self.kms, id, DatabaseOpen::Required).await?;
        if database.deployment_retirement().await?.is_none() {
            database.token_authority().issuer().await?;
        }
        let mut databases = self.databases.write().expect("Multi serving catalog");
        let driver = databases
            .entry(name.to_owned())
            .or_insert_with(|| serving_driver(database, name, &self.memory, self.peer.as_ref()));
        check_incarnation(driver, id)?;
        Ok(Arc::clone(driver))
    }

    async fn attach(&self, node: &Node, name: &str) -> Result<()> {
        node.database_or_attach(name.to_owned(), self.configuration.clone())
            .await?
            .ok_or_else(|| {
                invalid("The selected deployment was skipped during local attachment")
            })?;
        Ok(())
    }

    pub async fn close(self) {
        self.managed_operations.close();
        self.managed_operations.wait().await;
        drop(self.databases);
        self.backends.io.close_and_wait().await;
    }
}

#[derive(Clone)]
struct Backends {
    journal: Arc<dyn Journal>,
    batch: Option<Arc<dyn JournalBatchAppender>>,
    store: Arc<dyn ObjectStore>,
    io: Arc<IoRegion>,
    vault: PathBuf,
    node: IpAddr,
    peers: Option<String>,
    _journal_lock: Option<Arc<File>>,
}

fn assignment_peer_profile(auth: &AuthConfig) -> Option<peer::WorkloadProfile> {
    let (issuer, grants) = match auth {
        AuthConfig::OpenIdConnect(oidc) => (&oidc.issuer_url, oidc.workload_grants.as_ref()?),
        AuthConfig::ManagedWorkloadOidc(config) => {
            let assignment = config.assignment();
            (assignment.issuer_url(), assignment.grants())
        }
        _ => return None,
    };
    Some(peer::WorkloadProfile::new(
        grants.assignment()?.clone(), issuer.as_str(), grants.audience(), grants.client_id_prefix(),
    ))
}

fn serving_driver(
    database: MultiDatabase,
    name: &str,
    memory: &Arc<MemoryBudget>,
    peer: Option<&Arc<peer::Peer>>,
) -> Arc<MultiServing> {
    let driver = MultiServing::new(database, Arc::clone(memory));
    Arc::new(match peer {
        Some(peer) => driver.with_write_route(peer.database(name)),
        None => driver,
    })
}

#[derive(Clone, Copy)]
enum DatabaseOpen {
    Startup,
    Required,
    Prepared,
}

async fn open_database(
    backends: &Backends,
    kms: &Arc<dyn Kms>,
    id: Uuid,
    mode: DatabaseOpen,
) -> Result<MultiDatabase> {
    let mut database = MultiDatabase::new(
        MultiOptions {
            database: id,
            node: backends.node,
            lease_micros: WRITER_LEASE_MICROS,
            drift_micros: CLOCK_DRIFT_MICROS,
        },
        Arc::clone(&backends.journal),
        Arc::clone(&backends.store),
        Arc::clone(kms),
        Arc::new(|| SystemClock.now()),
    )?;
    if !matches!(mode, DatabaseOpen::Prepared) {
        database = database.with_row_custody()?;
    }
    if let Some(batch) = &backends.batch {
        database = database.with_batch_appender(Arc::clone(batch));
    }
    match backends.journal.head(StreamId(id.as_u128())).await {
        Ok(head) if head.0 > 0 => (),
        Ok(_) | Err(JournalError::NotFound) if matches!(mode, DatabaseOpen::Prepared) => {
            return Err(follower_unavailable());
        }
        Ok(_) | Err(JournalError::NotFound) => {
            // Concurrent catalog followers must open the same initial token key.
            let path = backends.vault.join(format!("bootstrap-{id}.lock"));
            let _bootstrap = tokio::task::spawn_blocking(move || {
                let file = OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(path)?;
                fs2::FileExt::lock_exclusive(&file)?;
                Ok::<_, io::Error>(file)
            })
            .await
            .map_err(|error| Anomaly::fault("rtdb/multi-open", error.to_string()))?
            .map_err(io_error)?;
            database.create().await?;
        }
        Err(JournalError::Unavailable) if matches!(mode, DatabaseOpen::Startup) => (),
        Err(JournalError::Unavailable) => {
            return Err(Anomaly::new(
                AnomalyCategory::Unavailable,
                "rtdb/journal-unavailable",
                "The deployment journal is unavailable",
            ));
        }
        Err(error) => return Err(Anomaly::fault("rtdb/multi-open", error.to_string())),
    }
    Ok(database)
}

async fn drive_multi(
    stop: &CancellationToken,
    mut tick: impl AsyncFnMut() -> Result<()>,
) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    stop.run_until_cancelled(async {
        loop {
            interval.tick().await;
            tick().await?;
        }
    })
    .await
    .unwrap_or(Ok(()))
}

fn require_ready_follower(
    observed: &BTreeMap<String, catalog::Deployment>,
    assignment: &NativeFollowerAssignment,
) -> Result<()> {
    if !observed.get(&assignment.database)
        .is_some_and(|entry| entry.status.ready() && entry.database == assignment.incarnation)
    {
        return Err(follower_unavailable());
    }
    Ok(())
}

fn follower_unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb/follower-unavailable", "The assigned deployment has no prepared local Multi replica")
}

fn check_incarnation(driver: &MultiServing, id: Uuid) -> Result<()> {
    if driver.incarnation() != *id.as_bytes() {
        return Err(invalid(
            "The local database incarnation differs from its ordered registration",
        ));
    }
    Ok(())
}

fn validate_deployment_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 255 || name.chars().any(char::is_control) {
        return Err(invalid("Invalid deployment name"));
    }
    Ok(())
}

fn unknown_deployment() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::NotFound,
        "rtdb/unknown-deployment",
        "The deployment is absent from the ordered catalog",
    )
}

fn transfer_interrupted() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Interrupted,
        "rtdb/deployment-transfer",
        "The deployment transfer was interrupted",
    )
}

fn transfer_request_digest(source: &str, transfer: &DeploymentTransfer) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"rtdb/multi/deployment-transfer/v1\0");
    hash.update((source.len() as u64).to_be_bytes());
    hash.update(source);
    match transfer {
        DeploymentTransfer::Restore { ticket } => {
            hash.update([0]);
            hash.update(ticket);
        }
        DeploymentTransfer::Fork {
            pin,
            basis,
            retain_until,
        } => {
            hash.update([1]);
            hash.update(pin.as_bytes());
            hash.update(basis.to_bytes());
            if let Some(deadline) = retain_until {
                hash.update(deadline.0.to_be_bytes());
            }
        }
        DeploymentTransfer::ManagedFork { pin, basis } => {
            hash.update([2]);
            hash.update(pin.as_bytes());
            hash.update(basis.to_bytes());
        }
    }
    hash.finalize().into()
}

fn retryable(error: &Anomaly) -> bool {
    error.code == "rtdb/unknown-database"
        || matches!(
            error.category,
            AnomalyCategory::Unavailable
                | AnomalyCategory::Busy
                | AnomalyCategory::Conflict
                | AnomalyCategory::Interrupted
        )
}

async fn connect_stream_journal(url: &str, credentials: &Credentials) -> Result<Arc<StreamJournal>> {
    StreamJournal::connect(url, credentials, Duration::from_secs(5)).await
        .map(Arc::new)
        .map_err(|error| Anomaly::new(AnomalyCategory::Unavailable, "rtdb/journal-connect", error.to_string()))
}

struct StreamJournalBatch(Arc<StreamJournal>);

impl JournalBatchAppender for StreamJournalBatch {
    fn append_batch(
        &self,
        stream: StreamId,
        epoch: journal_contract::Epoch,
        records: Vec<(journal_contract::Lsn, bytes::Bytes)>,
    ) -> journal_contract::BoxFuture<'_, journal_contract::Result<journal_contract::Lsn>> {
        Box::pin(self.0.append_batch(stream, epoch, records))
    }
}

struct ServingResources {
    native: NativeResources,
    runtime: MultiRuntime,
}

#[async_trait::async_trait]
impl ResourceFactory for ServingResources {
    async fn provision_managed_database(&self, name: &str, expected: Option<Uuid>) -> Result<(DatabaseConfig, Uuid)> {
        self.runtime.provision(name, expected).await
    }

    async fn export_managed_database(
        &self,
        name: &str,
        pin: Uuid,
        basis: Basis,
        until: Option<Timestamp>,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>> {
        self.runtime
            .export(name, pin, basis, until, cancellation)
            .await
    }

    async fn transfer_managed_database(
        &self,
        source: &str,
        destination: &str,
        transfer: DeploymentTransfer,
        cancellation: &CancellationToken,
    ) -> Result<(DatabaseConfig, Basis)> {
        self.runtime
            .transfer(source, destination, transfer, cancellation)
            .await
    }

    async fn check_managed_checkpoint_read(
        &self, name: &str, incarnation: [u8; 16], read: ManagedCheckpointRead<'_>,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        self.runtime.check_managed_read(name, incarnation, read, cancellation).await
    }

    async fn erase_managed_database(&self, name: &str) -> Result<(Vec<u8>, Timestamp)> {
        self.runtime.erase(name).await
    }

    fn serving_driver(
        &self,
        name: &str,
        _: &DatabaseConfig,
        _: &CancellationToken,
    ) -> Result<Option<Arc<dyn ServingDriver>>> {
        if let Some(assignment) = &self.runtime.follower {
            if assignment.database != name {
                return Err(follower_unavailable());
            }
            return Ok(Some(self.runtime.follower_driver(assignment)?));
        }
        Ok(self
            .runtime
            .databases
            .read()
            .expect("Multi serving catalog")
            .get(name)
            .cloned()
            .map(|driver| driver as Arc<dyn ServingDriver>))
    }

    fn journal_native_sql(&self) -> bool {
        true
    }

    fn token_kms(&self) -> Option<Arc<dyn Kms>> {
        Some(Arc::clone(&self.runtime.kms))
    }

    fn validate(&self, name: &str, config: &DatabaseConfig) -> Result<()> {
        self.native.validate(name, config)
    }

    async fn open_storage(&self, name: &str, config: &DatabaseConfig) -> Result<DatabaseStorage> {
        let storage = self.native.open_storage(name, config).await?;
        let install = async {
            let driver = self
                .runtime
                .databases
                .read()
                .expect("Multi serving catalog")
                .get(name)
                .cloned()
                .ok_or_else(|| invalid("Database is outside the Multi catalog"))?;
            let incarnation = driver.incarnation();
            let marker = rtdb_storage::ObjectKey::new(format!(
                "~rtdb-v1/{:x}/incarnation",
                Sha256::digest(name)
            ))?;
            let observed = match storage
                .store
                .get_fresh_exact(&marker, incarnation.len())
                .await
            {
                Ok(observed) => observed,
                Err(rtdb_storage::Error::MissingObject(_)) if self.runtime.follower.is_some() => {
                    return Err(follower_unavailable());
                }
                Err(rtdb_storage::Error::MissingObject(_)) => {
                    let write = storage
                        .store
                        .put(&marker, bytes::Bytes::copy_from_slice(&incarnation))
                        .await;
                    match storage
                        .store
                        .get_fresh_exact(&marker, incarnation.len())
                        .await
                    {
                        Ok(observed) => observed,
                        Err(error) => {
                            write?;
                            return Err(Anomaly::from(error));
                        }
                    }
                }
                Err(error) => return Err(Anomaly::from(error)),
            };
            if observed.as_ref() != incarnation {
                return Err(invalid(
                    "The catalog already names a different Multi incarnation",
                ));
            }
            Ok(())
        }
        .await;
        if let Err(error) = install {
            storage.io.close_and_wait().await;
            return Err(error);
        }
        Ok(storage)
    }

    async fn open_logs(
        &self,
        name: &str,
        config: &DatabaseConfig,
        access: LogAccess,
        stop: CancellationToken,
        storage: &DatabaseStorage,
    ) -> Result<OpenedLogs> {
        self.native
            .open_logs(name, config, access, stop, storage)
            .await
    }

    async fn close(&self) -> Result<()> {
        self.native.close().await
    }
}

fn load_or_create<const N: usize>(
    path: &Path,
    generate: impl FnOnce() -> [u8; N],
) -> Result<[u8; N]> {
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path.with_extension("lock"))
        .map_err(io_error)?;
    lock.lock().map_err(io_error)?;
    match fs::read(path) {
        Ok(bytes) => bytes
            .try_into()
            .map_err(|_| invalid("Invalid durable Multi identity or claim key")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let parent = path
                .parent()
                .ok_or_else(|| invalid("Multi identity path must have a parent"))?;
            let bytes = generate();
            let mut file = tempfile::NamedTempFile::new_in(parent).map_err(io_error)?;
            file.write_all(&bytes)
                .and_then(|()| file.as_file().sync_all())
                .map_err(io_error)?;
            file.persist(path).map_err(|error| io_error(error.error))?;
            File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(io_error)?;
            Ok(bytes)
        }
        Err(error) => Err(io_error(error)),
    }
}

#[cfg(feature = "s3")]
fn claim_credential_slot(vault: &Path, slots: usize) -> Result<(usize, File)> {
    for slot in 0..slots {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(vault.join(format!("node-slot-{slot}.lock")))
            .map_err(io_error)?;
        match fs2::FileExt::try_lock_exclusive(&file) {
            Ok(()) => return Ok((slot, file)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => (),
            Err(error) => return Err(io_error(error)),
        }
    }
    Err(Anomaly::new(
        AnomalyCategory::Busy,
        "rtdb.multi/credential-slot",
        "All installed journal identities are in use",
    ))
}

#[cfg(feature = "s3")]
async fn installed_node_ip() -> Result<IpAddr> {
    if let Ok(value) = std::env::var("RTDB_NODE_IP") {
        return value
            .parse()
            .map_err(|_| invalid("RTDB_NODE_IP must be an IP address"));
    }
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    struct Metadata {
        networks: Vec<Network>,
    }
    #[derive(serde::Deserialize)]
    struct Network {
        #[serde(rename = "IPv4Addresses")]
        addresses: Vec<IpAddr>,
    }
    let url = required("ECS_CONTAINER_METADATA_URI_V4")?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .map_err(metadata_error)?;
    let response = client
        .get(url)
        .send()
        .await
        .and_then(|response| response.error_for_status())
        .map_err(metadata_error)?;
    let bytes = response.bytes().await.map_err(metadata_error)?;
    let metadata: Metadata = serde_json::from_slice(&bytes).map_err(metadata_error)?;
    metadata
        .networks
        .into_iter()
        .flat_map(|network| network.addresses)
        .next()
        .ok_or_else(|| invalid("ECS has no RTDB node address"))
}

#[cfg(feature = "s3")]
fn metadata_error(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::fault("rtdb/node-metadata", error.to_string())
}

#[cfg(feature = "s3")]
fn credential(path: &Path, file: &str, env: &str) -> Result<PathBuf> {
    let path = path.join(file);
    let value = zeroize::Zeroizing::new(required(env)?);
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(&path)
        .and_then(|mut file| file.write_all(value.as_bytes()))
        .map_err(io_error)?;
    Ok(path)
}

#[cfg(feature = "s3")]
fn required(name: &str) -> Result<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| invalid(format!("{name} is required for an installed Multi node")))
}

fn invalid(message: impl Into<String>) -> Anomaly {
    Anomaly::incorrect("rtdb.multi/configuration", message)
}
fn io_error(error: io::Error) -> Anomaly {
    Anomaly::fault("rtdb.multi/local-io", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_flight_sql::{
        DeploymentAdminAction, DeploymentAdminAuthority,
        admin_client::DeploymentAdminClient,
        rtdb::{
            admin::v1::{
                CreateDeploymentRequest, ExportCheckpointRequest, RestoreDeploymentRequest,
            },
            v1::Basis as WireBasis,
        },
    };
    use rtdb_tx::transact::{Cell, Op, Outcome, Row, Transact};
    use rtdb_types::{EntityId, TableRef, Value};

    #[test]
    fn managed_workload_oidc_reuses_the_assignment_peer_profile() {
        use rtdb_auth::{ManagedWorkloadOidcConfig, WorkloadGrantConfig, WorkloadIssuerConfig};

        let create_grants = WorkloadGrantConfig::new("create-cell".into(), "create-".into()).unwrap();
        let assignment_grants = WorkloadGrantConfig::new("assignment-cell".into(), "assignment-".into()).unwrap();
        let point = rtdb_tx::transact::workload::Point::new("public.app_installation", "authority".into(), Timestamp(0)).unwrap();
        let create = WorkloadIssuerConfig::new("https://create.example.invalid/".parse().unwrap(), "create CA".into(), create_grants).unwrap();
        let assignment = WorkloadIssuerConfig::new("https://assignment.example.invalid/".parse().unwrap(), "assignment CA".into(), assignment_grants.with_assignment(point.clone())).unwrap();
        let config = AuthConfig::ManagedWorkloadOidc(Box::new(ManagedWorkloadOidcConfig::new(create, assignment).unwrap()));
        let profile = assignment_peer_profile(&config).unwrap();
        assert!(profile == peer::WorkloadProfile::new(point.clone(), "https://assignment.example.invalid/", "assignment-cell", "assignment-"));
        assert!(profile != peer::WorkloadProfile::new(point, "https://create.example.invalid/", "assignment-cell", "assignment-"));
    }

    #[test]
    fn development_stream_journal_requires_three_distinct_loopback_tls_endpoints() {
        let credentials = Credentials {
            certificate: "certificate.pem".into(),
            private_key: "key.pem".into(),
            ca: "ca.pem".into(),
        };
        let valid = "logkeeper+tls://127.0.0.1:5448,127.0.0.1:5449,127.0.0.1:5450";
        assert!(DevelopmentStreamJournal::new(valid, credentials.clone()).is_ok());
        for invalid in [
            valid.replace("logkeeper+tls", "logkeeper"),
            valid.replace("127.0.0.1:5450", "192.0.2.1:5450"),
            valid.replace("127.0.0.1:5450", "localhost:5450"),
            valid.replace(":5450", ":5449"),
            valid.replace(":5450", ":0"),
            valid.replace(",127.0.0.1:5450", ""),
            format!("{valid},127.0.0.1:5448"),
        ] {
            assert!(DevelopmentStreamJournal::new(&invalid, credentials.clone()).is_err(), "{invalid}");
        }
    }

    #[tokio::test]
    async fn prepared_follower_open_does_not_bootstrap_a_missing_journal() {
        let data = tempfile::tempdir().unwrap();
        let vault = data.path().join("keys");
        FileKms::create_directory(&vault).unwrap();
        let kms: Arc<dyn Kms> = Arc::new(FileKms::open(&vault, || 0).unwrap());
        let journal = FileJournal::open(data.path().join("journal"), Limits::default()).unwrap();
        let io = IoRegion::new();
        let backends = Backends {
            journal: Arc::new(journal.handle(Identity::from_static("urn:rtdb:test:follower"))),
            batch: None,
            store: Arc::new(LocalStore::open_in(data.path().join("objects"), io.clone()).unwrap()),
            io,
            vault,
            node: Ipv4Addr::LOCALHOST.into(),
            peers: None,
            _journal_lock: None,
        };
        let incarnation = Uuid::from_u128(1);
        let stream = StreamId(incarnation.as_u128());
        let missing = backends.journal.head(stream).await;
        assert!(matches!(&missing, Err(JournalError::NotFound)));
        let result = open_database(&backends, &kms, incarnation, DatabaseOpen::Prepared).await;
        assert_eq!(result.err().unwrap().code, "rtdb/follower-unavailable");
        assert!(matches!(backends.journal.head(stream).await, Err(JournalError::NotFound)));
        assert!(!backends.vault.join(format!("bootstrap-{incarnation}.lock")).exists());
    }

    #[test]
    fn multi_dev_readiness_waits_for_the_exact_ordered_writer() {
        std::thread::Builder::new()
            .stack_size(rtdb_node::WORKER_STACK_BYTES)
            .spawn(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .thread_stack_size(rtdb_node::WORKER_STACK_BYTES)
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(exact_development_writer());
            })
            .unwrap()
            .join()
            .unwrap();
    }

    async fn exact_development_writer() {
        let data = tempfile::tempdir().unwrap();
        let limit = 256 << 20;
        let mut config = NodeConfiguration::from_yaml_with_environment("", &|_| None, false).unwrap();
        config.flight_sql = None;
        let memory = MemoryBudget::new(config.node_options(limit).unwrap().execution_memory_bytes);
        let incarnation = Uuid::from_u128(1);
        let wrong = Uuid::from_u128(2);
        let runtime = MultiRuntime::development_with_incarnation(
            data.path(), &mut config, memory.clone(), Some(incarnation), None,
        ).await.unwrap();
        let (handle, driver) = crate::ServerDriver::open_with_multi(
            config, limit, memory.clone(), &runtime,
        ).await.unwrap();
        assert!(runtime.await_development_writer(handle.node(), wrong).await.is_err());
        assert!(tokio::time::timeout(
            Duration::from_millis(25), runtime.await_development_writer(handle.node(), incarnation),
        ).await.is_err());

        let server = tokio::spawn(driver.run());
        let stop = CancellationToken::new();
        let epoch = tokio::select! {
            result = runtime.await_development_writer(handle.node(), incarnation) => result.unwrap(),
            result = runtime.drive(handle.node(), &stop) => panic!("Multi stopped before readiness: {result:?}"),
        };
        assert!(epoch > 0);
        let attachment = handle.node().catalog().database(DEVELOPMENT_DATABASE).unwrap();
        let driver = attachment.serving_driver().unwrap();
        let feature = rtdb_tx::transact::ROW_CUSTODY;
        assert!(driver.features().contains(&feature));
        let group = [17; 16];
        let receipt = driver.transact(Transact {
            custody_request_digest: None,
            request_memory: None,
            key: vec![19; 16],
            require: vec![feature.into()],
            window: Default::default(),
            guards: vec![],
            requires: vec![],
            otherwise: vec![],
            ops: vec![Op::CreateCustodyGroup { group }],
        }, &stop).await.unwrap();
        assert_eq!(receipt.outcome, Outcome::Then);
        assert!(driver.current_custody(&stop).await.unwrap().custody_group_live(group).unwrap());
        drop(attachment);
        let identity = data.path().join("database-id");
        assert_eq!(fs::read(&identity).unwrap().as_slice(), incarnation.as_bytes());
        handle.shutdown();
        server.await.unwrap().unwrap();
        drop(handle);
        runtime.close().await;

        let mut config = NodeConfiguration::from_yaml_with_environment("", &|_| None, false).unwrap();
        let rejected = MultiRuntime::development_with_incarnation(
            data.path(), &mut config, memory, Some(wrong), None,
        ).await;
        assert!(rejected.is_err());
        assert_eq!(fs::read(&identity).unwrap().as_slice(), incarnation.as_bytes());
    }

    #[test]
    fn multi_dev_reopens_an_applied_receipt_and_reads_it_over_native_sql() {
        std::thread::Builder::new()
            .stack_size(rtdb_node::WORKER_STACK_BYTES)
            .spawn(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .thread_stack_size(rtdb_node::WORKER_STACK_BYTES)
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(restart());
            })
            .unwrap()
            .join()
            .unwrap();
    }

    async fn restart() {
        #[derive(Debug)]
        struct ReportProvisioner;

        impl DeploymentAdminAuthority for ReportProvisioner {
            fn authorize(
                &self,
                principal: &rtdb_auth::Principal,
                action: DeploymentAdminAction<'_>,
            ) -> Result<()> {
                if principal.user_id() == Some("xtdb")
                    && matches!(
                        action,
                        DeploymentAdminAction::Create {
                            deployment: "reports" | "duplicate"
                        } | DeploymentAdminAction::Export { deployment: "xtdb" }
                            | DeploymentAdminAction::Restore {
                                source: "xtdb",
                                destination: "copy"
                            }
                    )
                {
                    return Ok(());
                }
                Err(Anomaly::new(
                    AnomalyCategory::Forbidden,
                    "rtdb.test/admin",
                    "The fixture denies administration outside its selected names",
                ))
            }
        }

        let data = tempfile::tempdir().unwrap();
        let mut expected = None;
        let mut interrupted_restore = None;
        let key = vec![37; 16];
        for reopen in [false, true] {
            let limit = 256 << 20;
            let mut config = NodeConfiguration::from_yaml_with_environment("", &|_| None, false)
                .unwrap()
                .with_deployment_admin(Arc::new(ReportProvisioner));
            config.auth = rtdb_auth::AuthConfig::SingleRootUser {
                password: Some(rtdb_auth::Secret::new("secret")),
            };
            config.flight_sql.as_mut().unwrap().host = "127.0.0.1".into();
            config.flight_sql.as_mut().unwrap().port = 0;
            let memory =
                MemoryBudget::new(config.node_options(limit).unwrap().execution_memory_bytes);
            let runtime = MultiRuntime::development(data.path(), &mut config, Arc::clone(&memory))
                .await
                .unwrap();
            let (handle, server) =
                crate::ServerDriver::open_with_multi(config, limit, memory, &runtime)
                    .await
                    .unwrap();
            let server = tokio::spawn(server.run());
            let stop = CancellationToken::new();
            let database = handle.node().catalog().database("xtdb").unwrap();
            let driver = database.serving_driver().unwrap();
            let endpoint = format!("http://{}", handle.addresses().flight_sql.unwrap());
            let mut admin = DeploymentAdminClient::connect(endpoint, "Basic eHRkYjpzZWNyZXQ=")
                .await
                .unwrap();
            if reopen {
                let reports = handle.node().catalog().database("reports").unwrap();
                assert!(reports.serving_driver().is_some());
                let receipt = driver.tx_status(&key, &stop).await.unwrap();
                assert_eq!(Some(receipt.basis), expected);
                let user = handle.authenticator().authenticate_trusted("xtdb").unwrap();
                let mut session = handle.node().session("xtdb", user).unwrap();
                let mut query = session
                    .query_sql("SELECT value FROM docs", vec![])
                    .await
                    .unwrap();
                let batch = query.next_batch().await.unwrap().unwrap();
                assert_eq!(
                    rtdb_columnar::read_value(
                        batch.batch().schema().field(0),
                        batch.batch().column(0).as_ref(),
                        0
                    )
                    .unwrap(),
                    Value::I64(42)
                );
                assert!(handle.node().catalog().database("copy").is_err());
                let (request, applied) = interrupted_restore.take().unwrap();
                tokio::time::timeout(Duration::from_secs(15), async {
                    loop {
                        runtime.tick(handle.node()).await.unwrap();
                        if handle.node().catalog().database("copy").is_ok() {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                })
                .await
                .unwrap();
                let recovered = admin.restore(request).await.unwrap();
                assert_eq!(recovered.incarnation.as_slice(), applied.incarnation().as_slice());
                assert_eq!(
                    recovered.basis.unwrap().value.as_slice(),
                    &applied.to_bytes()
                );
                let user = handle.authenticator().authenticate_trusted("xtdb").unwrap();
                let mut session = handle.node().session("copy", user).unwrap();
                let mut query = session
                    .query_sql("SELECT value FROM docs", vec![])
                    .await
                    .unwrap();
                let batch = query.next_batch().await.unwrap().unwrap();
                assert_eq!(
                    rtdb_columnar::read_value(
                        batch.batch().schema().field(0),
                        batch.batch().column(0).as_ref(),
                        0,
                    )
                    .unwrap(),
                    Value::I64(42)
                );
            } else {
                runtime.tick(handle.node()).await.unwrap();
                let rejected = admin
                    .create(CreateDeploymentRequest {
                        deployment: "another".into(),
                        incarnation: None,
                    })
                    .await
                    .unwrap_err();
                assert_eq!(rejected.category, AnomalyCategory::Forbidden);
                let incarnation = Uuid::from_u128(0x7f_a7);
                let create = CreateDeploymentRequest {
                    deployment: "reports".into(),
                    incarnation: Some(incarnation.as_bytes().to_vec()),
                };
                let first = admin.create(create.clone()).await.unwrap();
                assert_eq!(first.deployment, "reports");
                assert_eq!(first.incarnation, *incarnation.as_bytes());
                assert_eq!(admin.create(create).await.unwrap(), first);
                assert_eq!(
                    admin.create(CreateDeploymentRequest {
                        deployment: "reports".into(),
                        incarnation: None,
                    }).await.unwrap(),
                    first,
                );
                for request in [
                    CreateDeploymentRequest {
                        deployment: "reports".into(),
                        incarnation: Some(vec![2; 16]),
                    },
                    CreateDeploymentRequest {
                        deployment: "duplicate".into(),
                        incarnation: Some(incarnation.as_bytes().to_vec()),
                    },
                ] {
                    let error = admin.create(request).await.unwrap_err();
                    assert_eq!(error.category, AnomalyCategory::Conflict);
                }
                let receipt = driver
                    .transact(
                        Transact {
                            custody_request_digest: None,
                            request_memory: None,
                            key: key.clone(),
                            require: vec![],
                            window: Default::default(),
                            guards: vec![],
                            requires: vec![],
                            otherwise: vec![],
                            ops: vec![Op::Put {
                                branch: "main".into(),
                                table: TableRef::new("public", "docs"),
                                if_absent: false,
                                rows: vec![Row {
                                    id: EntityId::Integer(1),
                                    valid: None,
                                    custody_group: None,
                                    protected_columns: Vec::new(),
                                    custody_payload: None,
                                    cells: [("value".into(), Cell::Value(Value::I64(42)))]
                                        .into_iter()
                                        .collect(),
                                }],
                            }],
                        },
                        &stop,
                    )
                    .await
                    .unwrap();
                assert_eq!(receipt.outcome, Outcome::Then);
                expected = Some(receipt.basis);
                let checkpoint = admin
                    .export(ExportCheckpointRequest {
                        deployment: "xtdb".into(),
                        basis: Some(WireBasis {
                            value: receipt.basis.to_bytes().to_vec(),
                        }),
                        pin: Uuid::new_v4().as_bytes().to_vec(),
                        retain_until_micros: None,
                    })
                    .await
                    .unwrap()
                    .checkpoint;
                let request = RestoreDeploymentRequest {
                    from: "xtdb".into(),
                    deployment: "copy".into(),
                    checkpoint: checkpoint.clone(),
                };
                let pending = admin.restore(request.clone()).await.unwrap_err();
                assert_eq!(pending.category, AnomalyCategory::Unavailable);
                assert!(handle.node().catalog().database("copy").is_err());
                let target = runtime
                    .ensure_driver("copy", runtime.catalog.deployment_id("copy"))
                    .await
                    .unwrap();
                target
                    .tick(&BTreeSet::from([runtime.backends.node]))
                    .await
                    .unwrap();
                let export = driver
                    .open_checkpoint_export(checkpoint.clone(), &stop)
                    .await
                    .unwrap();
                let applied = target
                    .restore_checkpoint(export, &stop)
                    .await
                    .unwrap()
                    .basis;
                assert!(handle.node().catalog().database("copy").is_err());
                handle
                    .node()
                    .catalog()
                    .release_deployment_checkpoint("xtdb", checkpoint, &stop)
                    .await
                    .unwrap();
                interrupted_restore = Some((request, applied));
            }
            drop(database);
            handle.shutdown();
            server.await.unwrap().unwrap();
            drop(handle);
            runtime.close().await;
        }
    }

    #[cfg(feature = "s3")]
    #[test]
    fn journal_credential_slots_exclude_other_live_processes() {
        let directory = tempfile::tempdir().unwrap();
        let (first, lock) = claim_credential_slot(directory.path(), 2).unwrap();
        let (second, _other) = claim_credential_slot(directory.path(), 2).unwrap();
        assert_ne!(first, second);
        assert!(
            matches!(claim_credential_slot(directory.path(), 2), Err(error) if error.category == AnomalyCategory::Busy)
        );
        drop(lock);
        assert_eq!(claim_credential_slot(directory.path(), 2).unwrap().0, first);
    }
}
