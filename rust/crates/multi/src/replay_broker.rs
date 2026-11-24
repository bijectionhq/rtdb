//! The directory replay broker owns detached body storage and its unit-key vault.

use std::{path::Path, sync::Arc, time::Duration};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use rtdb_directory::{Cas, DeploymentRecord, Directory, Lifecycle, ReplayCustody, ReplicaPayloadCandidate, ReplicaPreparedPublication, ReplicaPublicationRoute as PublishRoute, StoragePublication, StorageRegion};
use rtdb_format::{envelope::{ReplicaBody, ReplicaEnvelope}, keys::{CURRENT_STORAGE_VERSION, oversized_block_index, oversized_key}};
use rtdb_storage::{HeldView, IoRegion, ObjectKey, ObjectStore, remote::PublicationJournal, log::{ReplicaPayload, ReplicaPayloadReference}, sealing::{DeliveryIo, PlaintextView}};
use rtdb_types::{Anomaly, AnomalyCategory, MemoryBudget, PhysicalObjectVersion, Result, StorageEpoch};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::JoinSet,
};
use tokio_rustls::{
    TlsAcceptor, TlsConnector,
    rustls::{self, ClientConfig, RootCertStore, ServerConfig, pki_types::{CertificateDer, ServerName, pem::PemObject}},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use uuid::Uuid;

use crate::{
    deployment::CellStorage,
    install::Token,
    keys::{KeyService, UnitLedger, UnitScope},
    reference::PrefixedStore,
    store::EncryptingStore,
};

pub mod domain;
mod staging;
mod transfer;

const MAX_HEADER: usize = 16 << 10;
const MAX_BODY: usize = 64 << 20;
const SOCKET_LIMIT: Duration = Duration::from_secs(30);
const DELIVERY_INTERRUPTED: &str = "rtdb.multi/replay-broker-delivery-interrupted";
const CAS_ATTEMPTS: usize = 8;

#[derive(Clone, Copy, Deserialize, Serialize)]
pub(crate) struct History {
    pub deployment: Uuid,
    pub incarnation: u64,
    pub log_epoch: i32,
    pub storage_generation: Option<Uuid>,
}

impl History {
    pub(crate) fn of(record: &DeploymentRecord) -> Result<Self> {
        Ok(Self {
            deployment: Uuid::parse_str(&record.id).map_err(|_| refused())?,
            incarnation: record.incarnation,
            log_epoch: record.log_epoch,
            storage_generation: record.storage_generation,
        })
    }

    fn matches(self, record: &DeploymentRecord) -> bool {
        record.incarnation == self.incarnation && record.log_epoch == self.log_epoch
            && record.storage_generation == self.storage_generation
            && Uuid::parse_str(&record.id).ok() == Some(self.deployment)
    }
}

#[derive(Deserialize, Serialize)]
struct Request {
    token: String,
    history: Option<History>,
    action: Action,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Action {
    Read { ticket: Uuid, position: i64, reference: String },
    Publish { route: PublishRoute, object: String, length: usize },
    AdmitAppend { epoch: u64, reference: String },
    ObserveAppend { position: i64, reference: String },
    ConfirmTransfer { route: PublishRoute, object: String },
    Domain { domain: Uuid, command: domain::Action },
}

impl Action {
    fn uses_custody_token(&self) -> bool {
        matches!(self, Self::Domain { command, .. } if command.requires_custodian())
    }

    fn uses_transfer_token(&self) -> bool {
        matches!(self, Self::Publish { route, .. } | Self::ConfirmTransfer { route, .. }
            if route.transfer_position().is_some())
    }
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Response {
    Payload { length: usize },
    Publication { version: Option<PhysicalObjectVersion> },
    AppendAdmitted,
    Domain { view: domain::View },
    Refused { category: AnomalyCategory, code: String },
}

#[derive(Clone)]
pub struct Client(Arc<ClientRoute>);

enum ClientRoute {
    Remote(Remote),
    Standalone { service: Arc<Service>, tasks: TaskTracker },
}

struct Remote {
    endpoint: Arc<str>,
    server_name: ServerName<'static>,
    tls: Arc<ClientConfig>,
    token: Arc<str>,
    transfer_token: Option<Arc<str>>,
    custody_token: Option<Arc<str>>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.as_ref() {
            ClientRoute::Remote(remote) => formatter.debug_struct("ReplayBrokerClient").field("endpoint", &remote.endpoint).finish_non_exhaustive(),
            ClientRoute::Standalone { .. } => formatter.write_str("StandaloneReplayBroker"),
        }
    }
}

impl Client {
    pub fn from_files(
        endpoint: &str, server_name: &str, ca: &Path, token: &Path, transfer_token: Option<&Path>,
        custody_token: Option<&Path>,
    ) -> Result<Self> {
        let mut roots = RootCertStore::empty();
        for certificate in CertificateDer::pem_file_iter(ca).map_err(transport)? {
            roots.add(certificate.map_err(transport)?).map_err(transport)?;
        }
        if roots.is_empty() {
            return Err(Anomaly::incorrect("rtdb.multi/replay-broker-ca", "The replay broker requires a trusted TLS root"));
        }
        let tls = ClientConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
            .with_safe_default_protocol_versions().map_err(transport)?
            .with_root_certificates(roots).with_no_client_auth();
        Ok(Self(Arc::new(ClientRoute::Remote(Remote {
            endpoint: Arc::from(endpoint),
            server_name: ServerName::try_from(server_name.to_owned()).map_err(transport)?,
            tls: Arc::new(tls),
            token: Arc::from(Token::read(token)?),
            transfer_token: transfer_token.map(Token::read).transpose()?.map(Arc::from),
            custody_token: custody_token.map(Token::read).transpose()?.map(Arc::from),
        }))))
    }

    pub(crate) fn standalone(
        directory: Arc<dyn Directory>, storage: CellStorage, serving: &CellStorage, keys: Arc<dyn KeyService>, tasks: TaskTracker,
    ) -> Result<Self> {
        let service = Service::open(directory, storage, serving, keys, None, None, None, None)?;
        Ok(Self(Arc::new(ClientRoute::Standalone { service, tasks })))
    }

    /// Send a custody-domain command through the original broker's route.
    pub async fn custody_domain(&self, domain: Uuid, command: domain::Action) -> Result<domain::View> {
        let ClientRoute::Remote(remote) = self.0.as_ref() else { return Err(refused()); };
        let mut socket = remote.connect(None, Action::Domain { domain, command }).await?;
        let Response::Domain { view } = socket_work(read_response(&mut socket)).await? else { return Err(invalid_frame()); };
        Ok(view)
    }

    pub(crate) async fn read(
        &self, history: History, ticket: Uuid, position: i64, frame: &[u8], memory: &Arc<MemoryBudget>,
    ) -> Result<ReplicaPayload> {
        let ClientRoute::Remote(remote) = self.0.as_ref() else {
            let ClientRoute::Standalone { service, tasks } = self.0.as_ref() else { unreachable!(); };
            let service = Arc::clone(service);
            let memory = Arc::clone(memory);
            let reference = Bytes::copy_from_slice(frame);
            return tasks.spawn(async move {
                service.read_for_ticket(history, ticket, position, &reference, &memory, None).await
            }).await.map_err(transport)?;
        };
        let reference = decode_reference(frame)?;
        let action = Action::Read { ticket, position, reference: URL_SAFE_NO_PAD.encode(frame) };
        let mut socket = remote.connect(Some(history), action).await?;
        socket_work(async {
            let Response::Payload { length } = read_response(&mut socket).await? else {
                return Err(invalid_frame());
            };
            let (bytes, charge) = receive_body(&mut socket, length, memory).await?;
            Ok(reference.verify_body(bytes, charge)?)
        }).await
    }

    pub(crate) async fn publish(
        &self, history: History, epoch: u64, object: &ObjectKey, bytes: Bytes,
    ) -> Result<Option<PhysicalObjectVersion>> {
        self.publish_with(history, PublishRoute::Writer { epoch }, object, bytes).await
    }

    pub(crate) async fn admit_append(&self, history: History, epoch: u64, frame: &[u8]) -> Result<()> {
        let ClientRoute::Remote(remote) = self.0.as_ref() else { return Ok(()); };
        let action = Action::AdmitAppend { epoch, reference: URL_SAFE_NO_PAD.encode(frame) };
        let mut socket = remote.connect(Some(history), action).await?;
        match socket_work(read_response(&mut socket)).await? {
            Response::AppendAdmitted => Ok(()),
            _ => Err(invalid_frame()),
        }
    }

    pub(crate) async fn observe_append(&self, history: History, position: i64, frame: &[u8]) -> Result<()> {
        let ClientRoute::Remote(remote) = self.0.as_ref() else { return Ok(()); };
        let action = Action::ObserveAppend { position, reference: URL_SAFE_NO_PAD.encode(frame) };
        let mut socket = remote.connect(Some(history), action).await?;
        match socket_work(read_response(&mut socket)).await? {
            Response::AppendAdmitted => Ok(()),
            _ => Err(invalid_frame()),
        }
    }

    pub(crate) async fn seed(
        &self, target: &DeploymentRecord, replica: &rtdb_directory::CheckpointTransferReplica,
        position: i64, object: &ObjectKey, bytes: Bytes,
    ) -> Result<Option<PhysicalObjectVersion>> {
        let route = PublishRoute::Transfer {
            operation: replica.bootstrap.terminal.fence.destination.intent.operation,
            bootstrap: replica.bootstrap.ciphertext_sha256,
            position,
        };
        self.publish_with(History::of(target)?, route, object, bytes).await
    }

    pub(crate) async fn copy(
        &self, target: &DeploymentRecord, position: i64, object: &ObjectKey, bytes: Bytes,
    ) -> Result<Option<PhysicalObjectVersion>> {
        let generation = target.storage_generation.ok_or_else(refused)?;
        let route = PublishRoute::CellTransfer { generation, position };
        self.publish_with(History::of(target)?, route, object, bytes).await
    }

    pub(crate) async fn confirm_seed(
        &self, target: &DeploymentRecord, replica: &rtdb_directory::CheckpointTransferReplica,
        position: i64, object: &ObjectKey,
    ) -> Result<()> {
        self.confirm_transfer(target, PublishRoute::Transfer {
            operation: replica.bootstrap.terminal.fence.destination.intent.operation,
            bootstrap: replica.bootstrap.ciphertext_sha256,
            position,
        }, object).await
    }

    pub(crate) async fn confirm_copy(&self, target: &DeploymentRecord, position: i64, object: &ObjectKey) -> Result<()> {
        let generation = target.storage_generation.ok_or_else(refused)?;
        self.confirm_transfer(target, PublishRoute::CellTransfer { generation, position }, object).await
    }

    async fn confirm_transfer(&self, target: &DeploymentRecord, route: PublishRoute, object: &ObjectKey) -> Result<()> {
        let ClientRoute::Remote(remote) = self.0.as_ref() else { return Ok(()); };
        let action = Action::ConfirmTransfer { route, object: object.to_string() };
        let mut socket = remote.connect(Some(History::of(target)?), action).await?;
        match socket_work(read_response(&mut socket)).await? {
            Response::AppendAdmitted => Ok(()),
            _ => Err(invalid_frame()),
        }
    }

    async fn publish_with(
        &self, history: History, route: PublishRoute, object: &ObjectKey, bytes: Bytes,
    ) -> Result<Option<PhysicalObjectVersion>> {
        check_length(bytes.len())?;
        let ClientRoute::Remote(remote) = self.0.as_ref() else {
            let ClientRoute::Standalone { service, tasks } = self.0.as_ref() else { unreachable!(); };
            let service = Arc::clone(service);
            let name = object.clone();
            let charge = service.memory.reserve(bytes.len())?;
            return tasks.spawn(async move {
                service.publish_body(history, route, &name, bytes, charge).await
            }).await.map_err(transport)?;
        };
        let action = Action::Publish { route, object: object.to_string(), length: bytes.len() };
        let mut socket = remote.connect(Some(history), action).await?;
        socket_work(async {
            socket.write_all(&bytes).await.map_err(transport)?;
            socket.flush().await.map_err(transport)?;
            let Response::Publication { version } = read_response(&mut socket).await? else {
                return Err(invalid_frame());
            };
            Ok(version)
        }).await
    }
}

impl Remote {
    async fn connect(&self, history: Option<History>, action: Action) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
        let token = if action.uses_custody_token() {
            self.custody_token.as_ref().ok_or_else(refused)?
        } else if action.uses_transfer_token() {
            self.transfer_token.as_ref().ok_or_else(refused)?
        } else {
            &self.token
        };
        socket_work(async {
            let stream = TcpStream::connect(self.endpoint.as_ref()).await.map_err(transport)?;
            let mut stream = TlsConnector::from(Arc::clone(&self.tls))
                .connect(self.server_name.clone(), stream).await.map_err(transport)?;
            send_header(&mut stream, &Request { token: token.to_string(), history, action }).await?;
            Ok(stream)
        }).await
    }
}

pub struct Service {
    directory: Arc<dyn Directory>,
    storage: CellStorage,
    namespace: [u8; 32],
    keys: Arc<dyn KeyService>,
    token: Option<Token>,
    transfer_token: Option<Token>,
    custody_token: Option<Token>,
    custody: ReplayCustody,
    memory: Arc<MemoryBudget>,
}

struct BrokerDuty {
    authority: Uuid,
    generation: u64,
    id: Uuid,
    duty: domain::Duty,
    signer: ring::signature::Ed25519KeyPair,
}

impl Service {
    pub fn new(
        directory: Arc<dyn Directory>, storage: CellStorage, serving: &CellStorage, keys: Arc<dyn KeyService>,
        token: &Path, transfer_token: Option<&Path>, custody_token: Option<&Path>, task: Option<rtdb_directory::HostedTask>,
    ) -> Result<Arc<Self>> {
        let ordinary = Token::read(token)?;
        let transfer = transfer_token.map(Token::read).transpose()?;
        let custody = custody_token.map(Token::read).transpose()?;
        if transfer.as_ref() == Some(&ordinary) || custody.as_ref() == Some(&ordinary)
            || custody.is_some() && custody == transfer
        {
            return Err(Anomaly::incorrect("rtdb.multi/replay-broker-credential", "Transfer, erasure custody and replay require distinct credentials"));
        }
        Self::open(directory, storage, serving, keys, Some(Token::new(&ordinary)?),
            transfer.as_deref().map(Token::new).transpose()?, custody.as_deref().map(Token::new).transpose()?, task)
    }

    fn open(
        directory: Arc<dyn Directory>, storage: CellStorage, serving: &CellStorage, keys: Arc<dyn KeyService>,
        token: Option<Token>, transfer_token: Option<Token>, custody_token: Option<Token>, task: Option<rtdb_directory::HostedTask>,
    ) -> Result<Arc<Self>> {
        let shares_media = match (&storage, serving) {
            (CellStorage::S3 { config: payload }, CellStorage::S3 { config: serving }) => payload.bucket == serving.bucket,
            (CellStorage::Local { root: payload }, CellStorage::Local { root: serving }) =>
                payload.starts_with(serving) || serving.starts_with(payload),
            _ => false,
        };
        if shares_media {
            return Err(Anomaly::incorrect("rtdb.multi/replay-broker-storage",
                "The detached replay broker requires an object namespace outside serving storage"));
        }
        let namespace = storage.custody_namespace()?;
        let custody = match (&storage, task) {
            (CellStorage::Local { .. }, None) => ReplayCustody::Local,
            (_, owner) => ReplayCustody::Hosted { owner },
        };
        Ok(Arc::new(Self {
            directory, storage, namespace, keys, custody, token, transfer_token, custody_token,
            memory: MemoryBudget::new(rtdb_node::NodeOptions::default().execution_memory_bytes),
        }))
    }

    pub async fn serve(
        self: Arc<Self>, listener: TcpListener, tls: Arc<ServerConfig>, cancellation: CancellationToken,
    ) -> Result<()> {
        let reclaim_stop = cancellation.child_token();
        let reclaim = tokio::spawn(Arc::clone(&self).run_reclamation(reclaim_stop.clone()));
        let result = self.serve_connections(listener, tls, cancellation).await;
        reclaim_stop.cancel();
        reclaim.await.map_err(transport)?;
        result
    }

    async fn serve_connections(
        self: Arc<Self>, listener: TcpListener, tls: Arc<ServerConfig>, cancellation: CancellationToken,
    ) -> Result<()> {
        let acceptor = TlsAcceptor::from(tls);
        let slots = Arc::new(Semaphore::new(16));
        let mut connections = JoinSet::new();
        let result = loop {
            let accepted = tokio::select! {
                _ = cancellation.cancelled() => break Ok(()),
                _ = connections.join_next(), if !connections.is_empty() => continue,
                accepted = listener.accept() => accepted,
            };
            let (socket, _) = match accepted {
                Ok(accepted) => accepted,
                Err(error) => break Err(transport(error)),
            };
            let Ok(slot) = Arc::clone(&slots).try_acquire_owned() else { continue; };
            let acceptor = acceptor.clone();
            let service = Arc::clone(&self);
            connections.spawn(async move {
                let _slot = slot;
                let Ok(mut socket) = socket_work(async { acceptor.accept(DeliveryIo::new(socket)).await.map_err(transport) }).await else { return; };
                if let Err(error) = service.dispatch(&mut socket).await && error.code != DELIVERY_INTERRUPTED {
                    let response = Response::Refused { category: error.category, code: error.code };
                    let _ = socket_work(send_header(&mut socket, &response)).await;
                }
            });
        };
        while connections.join_next().await.is_some() {}
        result
    }

    async fn run_reclamation(self: Arc<Self>, cancellation: CancellationToken) {
        let CellStorage::S3 { config } = &self.storage else { return; };
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return,
                _ = interval.tick() => {}
            }
            if let Err(error) = self.reclaim(config, &cancellation).await {
                tracing::warn!(code = %error.code, "Detached replay reclamation will retry");
            }
        }
    }

    async fn reclaim(&self, config: &rtdb_s3::S3Config, cancellation: &CancellationToken) -> Result<()> {
        let inventory = CellStorage::s3_inventory(config).await?;
        inventory.require_versioned().await?;
        for record in self.directory.deployments().await? {
            if cancellation.is_cancelled() { break; }
            for (&id, region) in &record.storage_regions {
                if let StorageRegion::ReplicaPublication { draft: Some(_), .. } = region {
                    if let Err(error) = self.reclaim_staged(&inventory, &record, id, region).await {
                        tracing::warn!(installation = %record.id, stage = %id, code = %error.code, "Detached staging awaits settlement");
                    }
                }
            }
            let Some(work) = record.replica_payload_trim.as_deref().filter(|work| work.belongs_to(&record)) else { continue; };
            for candidate in &work.candidates {
                if let Err(error) = self.reclaim_one(&inventory, &record, candidate).await {
                    tracing::warn!(installation = %record.id, position = candidate.position, code = %error.code, "Detached publication is awaiting settlement");
                }
            }
        }
        Ok(())
    }

    async fn reclaim_one(
        &self, inventory: &rtdb_s3::VersionedInventory, record: &DeploymentRecord, candidate: &ReplicaPayloadCandidate,
    ) -> Result<()> {
        let logical = ObjectKey::new(&candidate.key)?;
        check_publish_key(&logical)?;
        let residence = residence(record);
        let physical = ObjectKey::new(format!("{residence}/{logical}"))?;
        if !inventory.owns_publication(&physical, &candidate.version)
            || !inventory.exact_retirement_available(&physical, &candidate.version).await?
        {
            return Ok(());
        }
        self.settle_candidate(record, candidate, logical, physical, |key, version| async move {
            inventory.retire_observed_version(&key, &version).await
        }).await
    }

    async fn settle_candidate<R: Future<Output = Result<()>>>(
        &self, record: &DeploymentRecord, candidate: &ReplicaPayloadCandidate, logical: ObjectKey, physical: ObjectKey,
        remove: impl FnOnce(ObjectKey, PhysicalObjectVersion) -> R,
    ) -> Result<()> {
        let history = History::of(record)?;
        let residence = residence(record);
        let token = Uuid::new_v4();
        let owner = crate::admin::update_installation(self.directory.as_ref(), &record.id, |current| {
            if !history.matches(current) || !permits_release(current, candidate, rtdb_directory::now_ms())? {
                return Ok(());
            }
            if let Some(chosen) = current.replica_payload_trim.as_mut().and_then(|work|
                work.candidates.iter_mut().find(|chosen| same_candidate(candidate, chosen)))
            {
                chosen.grant.get_or_insert(token);
            }
            Ok(())
        }).await?;
        let sampled = rtdb_directory::now_ms();
        if !history.matches(&owner) || !permits_release(&owner, candidate, sampled)? { return Ok(()); }
        let Some(grant) = owner.replica_payload_trim.as_ref().and_then(|work| work.candidates.iter()
            .find(|chosen| same_candidate(candidate, chosen))).and_then(|chosen| chosen.grant)
        else { return Ok(()); };
        let anchor = crate::hosted_recovery::body_anchor(&owner).ok_or_else(refused)?;
        let decision = crate::hosted_recovery::checkpoint_decision(&owner)?.ok_or_else(refused)?;
        let effect = || serde_json::json!({
            "lineage": owner.lineage, "installation": owner.id, "incarnation": owner.incarnation,
            "epoch": owner.log_epoch, "stream": owner.stream("replica"), "generation": owner.storage_generation,
            "revision": owner.version, "residence": residence, "retirement": grant, "key": physical.as_str(),
            "publication": candidate.version, "position": candidate.position, "frame": candidate.frame_sha256,
        });
        crate::residence_trace::record("retirement-recovery-dependency", || {
            let mut observation = effect();
            observation["interval"] = serde_json::json!(anchor.interval);
            observation["anchor"] = serde_json::json!(anchor.digest);
            observation["checkpoint"] = serde_json::json!(owner.checkpoint);
            observation["manifest"] = serde_json::json!(owner.recovery_manifest_sha256);
            observation["decision"] = serde_json::json!(decision.certificate.statement.fingerprint());
            observation["requires"] = serde_json::json!(owner.replica_dependency_intervals(sampled).collect::<Vec<_>>());
            observation["uploads"] = serde_json::json!(publication_retaining(&owner, &candidate.key).collect::<Vec<_>>());
            observation
        });
        self.withdraw_body_key(&owner, &logical).await?;
        crate::residence_trace::record("withdraw-detached-key", || effect());
        remove(physical.clone(), candidate.version.clone()).await?;
        crate::residence_trace::record("remove-version", || effect());
        let settled = crate::admin::update_installation(self.directory.as_ref(), &record.id, |current| {
            if history.matches(current)
                && let Some(work) = current.replica_payload_trim.as_mut()
            {
                work.candidates.retain(|chosen| chosen.grant != Some(grant) || !same_candidate(candidate, chosen));
            }
            Ok(())
        }).await?;
        if history.matches(&settled) && settled.replica_payload_trim.as_ref().is_none_or(|work| {
            work.candidates.iter().all(|chosen| chosen.grant != Some(grant) || !same_candidate(candidate, chosen))
        }) {
            crate::residence_trace::record("settle-detached-version", || {
                let mut observation = effect();
                observation["settled_revision"] = serde_json::json!(settled.version);
                observation
            });
        }
        Ok(())
    }

    async fn dispatch(&self, socket: &mut tokio_rustls::server::TlsStream<DeliveryIo<TcpStream>>) -> Result<()> {
        let request: Request = socket_work(receive_header(socket)).await?;
        let verifier = if request.action.uses_custody_token() {
            &self.custody_token
        } else if request.action.uses_transfer_token() {
            &self.transfer_token
        } else {
            &self.token
        };
        if !verifier.as_ref().is_some_and(|token| token.accepts(&request.token)) { return Err(refused()); }
        let Request { action, history, .. } = request;
        let action = match action {
            Action::Domain { domain, command } if history.is_none() => {
                let io = IoRegion::new();
                let store = self.storage.bucket(io.clone()).await?;
                let view = domain::execute(store.as_ref(), domain, command).await;
                io.close_and_wait().await;
                return socket_work(send_header(socket, &Response::Domain { view: view? })).await;
            }
            action => action,
        };
        let history = history.ok_or_else(refused)?;
        if matches!(action, Action::Domain { .. }) { return Err(refused()); }
        let duty = if self.custody_token.is_some() {
            let kind = match action {
                Action::Read { .. } => crate::journal::hosted::erasure::CustodyWork::Egress,
                _ => crate::journal::hosted::erasure::CustodyWork::Copy,
            };
            Some(self.admit_domain_work(history, kind).await?)
        } else {
            None
        };
        let result = self.dispatch_work(socket, history, action).await;
        if let Some(duty) = duty
            && let Err(error) = self.join_domain_work(duty).await
        {
            tracing::warn!(code = %error.code, "Replay custody drain remains unsettled");
            return Err(Anomaly::new(AnomalyCategory::Unavailable, DELIVERY_INTERRUPTED,
                "The detached replay duty has no durable join"));
        }
        result
    }

    async fn dispatch_work(&self, socket: &mut tokio_rustls::server::TlsStream<DeliveryIo<TcpStream>>, history: History, action: Action) -> Result<()> {
        match action {
            Action::Domain { .. } => Err(refused()),
            Action::Read { ticket, position, reference } => {
                let frame = URL_SAFE_NO_PAD.decode(reference).map_err(|_| invalid_frame())?;
                self.read_for_ticket(history, ticket, position, &frame, &self.memory, Some(socket)).await?;
                Ok(())
            }
            Action::AdmitAppend { epoch, reference } => {
                let reference = URL_SAFE_NO_PAD.decode(reference).map_err(|_| invalid_frame())?;
                self.admit_staged_append(history, epoch, &reference).await?;
                socket_work(send_header(socket, &Response::AppendAdmitted)).await
            }
            Action::ObserveAppend { position, reference } => {
                let reference = URL_SAFE_NO_PAD.decode(reference).map_err(|_| invalid_frame())?;
                self.observe_staged_append(history, position, &reference).await?;
                socket_work(send_header(socket, &Response::AppendAdmitted)).await
            }
            Action::ConfirmTransfer { route, object } if route.transfer_position().is_some() => {
                let key = ObjectKey::new(object)?;
                self.confirm_transfer(history, route, &key).await?;
                socket_work(send_header(socket, &Response::AppendAdmitted)).await
            }
            Action::ConfirmTransfer { .. } => Err(refused()),
            Action::Publish { route, object, length } => {
                let key = ObjectKey::new(object)?;
                require_publication(history, route, &self.current(history).await?, &key)?;
                let (body, charge) = socket_work(receive_body(socket, length, &self.memory)).await?;
                let version = self.publish_body(history, route, &key, body, charge).await?;
                socket_work(send_header(socket, &Response::Publication { version })).await
            }
        }
    }

    async fn admit_domain_work(
        &self, history: History, kind: crate::journal::hosted::erasure::CustodyWork,
    ) -> Result<BrokerDuty> {
        use ring::signature::KeyPair;

        let record = self.current(history).await?;
        let authority = domain::directory_domain(self.namespace, &record.org, &record.lineage)?;
        let io = IoRegion::new();
        let store = self.storage.bucket(io.clone()).await?;
        let view = domain::execute(store.as_ref(), authority, domain::Action::Status).await?;
        let signer = self.domain_signer().await?;
        let duty = domain::Duty {
            root: history.deployment, history: domain::directory_history(history)?, kind,
            verification_key: signer.public_key().as_ref().try_into().expect("Ed25519 key width"),
        };
        let id = Uuid::new_v4();
        let admitted = domain::execute(store.as_ref(), authority,
            domain::Action::Admit { generation: view.generation, id, duty }).await;
        io.close_and_wait().await;
        admitted?;
        Ok(BrokerDuty { authority, generation: view.generation, id, duty, signer })
    }

    async fn join_domain_work(&self, work: BrokerDuty) -> Result<()> {
        let observation: [u8; 32] = ring::digest::digest(&ring::digest::SHA256, work.id.as_bytes())
            .as_ref().try_into().expect("SHA-256 width");
        let message = domain::WorkJoin::signing_bytes(work.authority, work.generation, work.id, work.duty, observation)?;
        let join = domain::WorkJoin { observation, signature: work.signer.sign(&message).as_ref().to_vec() };
        let io = IoRegion::new();
        let store = self.storage.bucket(io.clone()).await?;
        let result = domain::execute(store.as_ref(), work.authority,
            domain::Action::Join { generation: work.generation, id: work.id, join }).await;
        io.close_and_wait().await;
        result.map(drop)
    }

    async fn domain_signer(&self) -> Result<ring::signature::Ed25519KeyPair> {
        let mut seed = self.keys.key(".rtdb-custody-domain-drain-signer").await?;
        let signer = ring::signature::Ed25519KeyPair::from_seed_unchecked(&seed).map_err(|_| refused());
        seed.fill(0);
        signer
    }

    async fn read_for_ticket(
        &self, history: History, ticket: Uuid, position: i64, frame: &[u8], memory: &Arc<MemoryBudget>,
        socket: Option<&mut tokio_rustls::server::TlsStream<DeliveryIo<TcpStream>>>,
    ) -> Result<ReplicaPayload> {
        let reference = decode_reference(frame)?;
        let region = Uuid::new_v4();
        let local = match self.custody {
            ReplayCustody::Local => self.keys.hold_local_storage_region(region)?,
            ReplayCustody::Hosted { .. } => None,
        };
        let mut attempted = false;
        let result = async {
            let record = Arc::new(self.admit_read(history, ticket, position, region, &mut attempted).await?);
            let io = IoRegion::new();
            let held = HeldView::recovery(Arc::new((record.clone(), region, position)));
            let read = IoRegion::reading(held, async {
                let plaintext = PlaintextView::new(memory.clone());
                let (payload, sealed_frame) = plaintext.reading(
                    self.read_body(&record, position, frame, &reference, Arc::clone(&io), memory),
                ).await?;
                let grant = plaintext.grant().await?;
                if let Some(socket) = socket {
                    grant.deliver(socket_work(async {
                        send_header(socket, &Response::Payload { length: payload.as_ref().len() }).await?;
                        socket.write_all(payload.as_ref()).await.map_err(transport)?;
                        socket.flush().await.map_err(transport)
                    })).await.map_err(|_| Anomaly::new(AnomalyCategory::Unavailable, DELIVERY_INTERRUPTED,
                        "The detached replay body did not reach its socket within its grant"))?;
                } else {
                    grant.check()?;
                }
                crate::residence_trace::record("read-retained-payload", || serde_json::json!({
                    "lineage": record.lineage, "installation": record.id, "incarnation": record.incarnation,
                    "revision": record.version, "stream": record.stream("replica"), "epoch": record.log_epoch,
                    "hold": region, "residence": residence(&record), "position": position,
                    "key": format!("{}/{}", residence(&record), reference.object()),
                    "publication": reference.publication(), "frame": sealed_frame,
                    "source_hold": record.storage_regions.get(&ticket)
                        .filter(|source| matches!(source, StorageRegion::ReplicaReplay { .. }))
                        .map(|_| serde_json::json!({ "hold": ticket, "residence": record.storage_prefix() })),
                }));
                Ok(payload)
            }).await;
            io.close_and_wait().await;
            read
        }.await;
        if attempted {
            self.release(history, region).await;
        }
        if local.is_some() {
            drop(local);
            self.keys.forget_local_storage_region(region)?;
        }
        result
    }

    async fn publish_body(
        &self, history: History, route: PublishRoute, key: &ObjectKey, body: Bytes,
        charge: Arc<rtdb_types::MemoryReservation>,
    ) -> Result<Option<PhysicalObjectVersion>> {
        check_publish_key(key)?;
        let envelope = ReplicaEnvelope::decode(&body)?.ok_or_else(invalid_frame)?;
        if matches!(envelope.message, ReplicaBody::OversizedMessage(_)) { return Err(invalid_frame()); }
        let body_sha256 = ring::digest::digest(&ring::digest::SHA256, &body)
            .as_ref().try_into().expect("SHA-256 width");
        let pointer = route.transfer_position().map(|_| transfer::transfer_pointer(key, &envelope, &body)).transpose()?;
        if let (CellStorage::S3 { config }, Some(pointer)) = (&self.storage, &pointer) {
            let inventory = CellStorage::s3_inventory(config).await?;
            if let Some(version) = self.reuse_transfer_upload(&inventory, history, route, key, pointer, body_sha256).await? {
                return Ok(Some(version));
            }
        }
        let region = Uuid::new_v4();
        let local = match self.custody {
            ReplayCustody::Local => self.keys.hold_local_storage_region(region)?,
            ReplayCustody::Hosted { owner: Some(_) } => None,
            ReplayCustody::Hosted { owner: None } => return Err(refused()),
        };
        let deployment = history.deployment.to_string();
        let mut attempted = false;
        let result = async {
            attempted = true;
            let record = crate::admin::update_installation(self.directory.as_ref(), &deployment, |record| {
                require_publication(history, route, record, key)?;
                if record.replica_payload_trim.as_deref().is_some_and(|trim| trim.belongs_to(record)
                    && trim.candidates.iter().any(|candidate| candidate.key == key.as_str() && candidate.grant.is_some()))
                {
                    return Err(refused());
                }
                let draft = matches!(self.storage, CellStorage::S3 { .. })
                    .then(|| staging::reserve_body(record, key, route, body_sha256)).transpose()?;
                record.storage_regions.insert(region, StorageRegion::ReplicaPublication {
                    key: key.to_string(), custody: self.custody.clone(), closed: false, publications: Default::default(), draft,
                });
                Ok(())
            }).await?;
            let mut io = IoRegion::new();
            if matches!(self.storage, CellStorage::S3 { .. }) {
                Arc::get_mut(&mut io).expect("the publication region is private").track_publications(Arc::new(BrokerUpload {
                    directory: Arc::clone(&self.directory), history, route, region,
                    logical: key.clone(), physical: ObjectKey::new(format!("{}/{key}", residence(&record)))?,
                }));
            }
            let version: Result<_> = async {
                let files = self.open_store(&record, Arc::clone(&io)).await?;
                let written = IoRegion::retaining(charge, files.put_with_publication(key, body)).await?;
                let version = written.and_then(rtdb_types::PhysicalObjectPublication::into_version);
                if version.is_none() && matches!(self.storage, CellStorage::S3 { .. }) {
                    return Err(Anomaly::incorrect("rtdb.multi/replay-broker-version", "Detached S3 bodies require versioned custody"));
                }
                Ok(version)
            }.await;
            io.close_and_wait().await;
            let version = version?;
            if let Some(version) = &version {
                self.finish_staged_upload(history, route, region, key, version, pointer.as_ref()).await?;
            } else {
                require_publication(history, route, &self.current(history).await?, key)?;
            }
            Ok(version)
        }.await;
        if attempted {
            self.release(history, region).await;
        }
        if local.is_some() {
            drop(local);
            self.keys.forget_local_storage_region(region)?;
        }
        result
    }

    async fn current(&self, history: History) -> Result<DeploymentRecord> {
        self.directory.deployment(&history.deployment.to_string()).await?
            .filter(|record| history.matches(record)).ok_or_else(refused)
    }

    async fn admit_read(
        &self, history: History, ticket: Uuid, position: i64, region: Uuid, attempted: &mut bool,
    ) -> Result<DeploymentRecord> {
        let after = position.checked_sub(1).filter(|after| *after >= -1).ok_or_else(refused)?;
        for _ in 0..CAS_ATTEMPTS {
            let mut record = self.current(history).await?;
            if matches!(record.lifecycle, Lifecycle::Deleting | Lifecycle::Deleted) || transfer::current_generation_abandoned(&record)
                || !record.storage_regions.get(&ticket).is_some_and(|hold| admits_position(&record, hold, position))
            {
                return Err(refused());
            }
            // Redeem a reference under an existing dependency. The logical
            // fence closes new dependencies, while the old ticket prevents the
            // collector from moving this position's key or physical journal.
            crate::hosted_recovery::journal_replay_start(&record, after)?;
            if record.replica_payload_trim.as_ref().is_some_and(|work| work.belongs_to(&record)
                && work.candidates.iter().any(|entry| entry.position == position && entry.grant.is_some()))
            {
                return Err(refused());
            }
            let version = record.version;
            record.storage_regions.insert(region, StorageRegion::ReplicaReplay {
                first_required: position, through: position, custody: self.custody.clone(),
            });
            *attempted = true;
            if let Cas::Applied(stored) = self.directory.replace_deployment(record, version).await? {
                crate::residence_trace::record("admit-recovery", || serde_json::json!({
                    "lineage": stored.lineage, "installation": stored.id, "incarnation": stored.incarnation,
                    "revision": stored.version, "stream": stored.stream("replica"), "epoch": stored.log_epoch,
                    "hold": region, "residence": residence(&stored),
                    "first_required": position, "through": position,
                }));
                return Ok(stored);
            }
        }
        Err(Anomaly::new(AnomalyCategory::Busy, "rtdb.multi/replay-broker-contended",
            "The directory kept changing during broker admission"))
    }

    async fn read_body(
        &self, record: &DeploymentRecord, position: i64, frame: &[u8],
        reference: &ReplicaPayloadReference, io: Arc<IoRegion>, memory: &Arc<MemoryBudget>,
    ) -> Result<(ReplicaPayload, [u8; 32])> {
        let stream = crate::admin::administration_stream(self.directory.as_ref(), record, crate::process::MAXIMUM_LOG_MESSAGE_BYTES).await?;
        let proof = rtdb_logkeeper::certify_committed_range(&stream, position, position).await?;
        let mut reader = rtdb_logkeeper::CertifiedRangeReader::new(&stream, &proof)?;
        let page = reader.next_page().await?.ok_or_else(refused)?;
        if page.len() != 1 || page[0].lsn != position
            || !crate::logs::commits_payload_reference(&page[0].payload, frame)
            || reader.next_page().await?.is_some()
        {
            return Err(refused());
        }
        if reference.publication().is_none() && matches!(self.storage, CellStorage::S3 { .. }) {
            return Err(refused());
        }
        let files = self.open_store(record, io).await?;
        check_length(usize::try_from(files.head_fresh(reference.object()).await?.size).map_err(transport)?)?;
        let sealed_frame: [u8; 32] = ring::digest::digest(&ring::digest::SHA256, &page[0].payload)
            .as_ref().try_into().expect("SHA-256 width");
        crate::residence_trace::record("broker-certified-pointer", || serde_json::json!({
            "lineage": record.lineage, "installation": record.id, "incarnation": record.incarnation,
            "revision": record.version, "stream": record.stream("replica"), "epoch": record.log_epoch,
            "residence": residence(record), "position": position, "range": proof.interval,
            "range_digest": proof.digest_bytes(), "frame": sealed_frame,
            "key": format!("{}/{}", residence(record), reference.object()),
            "publication": reference.publication(),
        }));
        Ok((reference.read_at(&files, memory).await?, sealed_frame))
    }

    async fn open_store(&self, record: &DeploymentRecord, io: Arc<IoRegion>) -> Result<EncryptingStore> {
        let residence = residence(record);
        let bucket = self.storage.bucket(Arc::clone(&io)).await?;
        let files = Arc::new(PrefixedStore::new(bucket, &residence));
        let ledger = UnitLedger::new(Arc::clone(&self.keys), UnitScope::replay_objects(self.namespace, &residence, payload_log_epoch(record)), io);
        Ok(EncryptingStore::with_units(files, Arc::new(ledger)))
    }

    async fn release(&self, history: History, region: Uuid) {
        let deployment = history.deployment.to_string();
        loop {
            match crate::store::release_region(self.directory.as_ref(), &deployment, region, true).await {
                Ok(()) => {
                    crate::residence_trace::record("finish-recovery", || serde_json::json!({
                        "installation": deployment, "stream": format!("{deployment}/{}/replica", history.incarnation),
                        "epoch": history.log_epoch, "hold": region,
                    }));
                    return;
                }
                Err(error) => tracing::warn!(%deployment, %region, code = %error.code, "Broker read region is awaiting release"),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

fn same_candidate(left: &ReplicaPayloadCandidate, right: &ReplicaPayloadCandidate) -> bool {
    left.position == right.position && left.key == right.key
        && left.frame_sha256 == right.frame_sha256 && left.version == right.version
}

fn publication_retaining<'a>(record: &'a DeploymentRecord, logical: &'a str) -> impl Iterator<Item = Uuid> + 'a {
    record.storage_regions.iter().filter_map(move |(&id, region)| {
        matches!(region, StorageRegion::ReplicaPublication { key, .. } if key == logical).then_some(id)
    })
}

fn permits_release(record: &DeploymentRecord, candidate: &ReplicaPayloadCandidate, now_ms: i64) -> Result<bool> {
    Ok(matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
        && candidate.position >= 0 && candidate.frame_sha256 != [0; 32]
        && crate::hosted_recovery::checkpoint_decision(record)?.is_some()
        && record.checkpoint.is_some_and(|root| root.boundary > candidate.position)
        && crate::hosted_recovery::body_anchor(record).is_some_and(|anchor|
            anchor.root_incarnation == record.incarnation && (anchor.interval[0]..=anchor.interval[1]).contains(&candidate.position))
        && record.replica_payload_trim.as_ref().is_some_and(|work| work.belongs_to(record)
            && work.candidates.iter().any(|chosen| same_candidate(candidate, chosen)))
        && !record.requires_replica_offset(candidate.position, now_ms)
        && publication_retaining(record, &candidate.key).next().is_none())
}

struct BrokerUpload {
    directory: Arc<dyn Directory>,
    history: History,
    route: PublishRoute,
    region: Uuid,
    logical: ObjectKey,
    physical: ObjectKey,
}

#[async_trait::async_trait]
impl PublicationJournal for BrokerUpload {
    async fn admit(&self, key: &ObjectKey, attempt: Uuid, upload: &str) -> rtdb_storage::Result<()> {
        if key != &self.physical || attempt.is_nil() || upload.is_empty() {
            return Err(rtdb_storage::Error::Task("The detached upload has another provider identity".into()));
        }
        let publication = StoragePublication { key: key.to_string(), upload_id: upload.to_owned(), unit: None, rejected: false };
        crate::admin::update_installation(self.directory.as_ref(), &self.history.deployment.to_string(), |record| {
            require_publication(self.history, self.route, record, &self.logical)?;
            let Some(StorageRegion::ReplicaPublication { key, closed: false, publications, draft, .. }) =
                record.storage_regions.get_mut(&self.region) else { return Err(refused()); };
            if key.as_str() != self.logical.as_str() || publications.get(&attempt).is_some_and(|prior| prior != &publication) {
                return Err(refused());
            }
            if let Some(draft) = draft {
                if draft.route != self.route || draft.attempt.is_some_and(|prior| prior != attempt) { return Err(refused()); }
                draft.attempt = Some(attempt);
            }
            publications.insert(attempt, publication.clone());
            Ok(())
        }).await.map_err(storage_error)?;
        Ok(())
    }

    async fn settle(&self, key: &ObjectKey, attempt: Uuid, upload: &str, _submitted: bool) -> rtdb_storage::Result<()> {
        if key != &self.physical { return Err(rtdb_storage::Error::Task("The detached upload has another provider identity".into())); }
        crate::admin::update_installation(self.directory.as_ref(), &self.history.deployment.to_string(), |record| {
            let Some(StorageRegion::ReplicaPublication { publications, .. }) = record.storage_regions.get_mut(&self.region)
                else { return Ok(()); };
            if publications.get(&attempt).is_some_and(|prior| prior.key != key.as_str() || prior.upload_id != upload) {
                return Err(refused());
            }
            publications.remove(&attempt);
            Ok(())
        }).await.map_err(storage_error)?;
        Ok(())
    }
}

fn storage_error(error: Anomaly) -> rtdb_storage::Error {
    rtdb_storage::Error::Remote { key: "detached replay broker".into(), message: error.to_string() }
}

pub(crate) fn residence(record: &DeploymentRecord) -> String {
    format!("replay-bodies/{}/{}", record.storage_prefix(), payload_log_epoch(record))
}

pub(crate) fn admits_position(record: &DeploymentRecord, hold: &StorageRegion, position: i64) -> bool {
    record.storage_region_replica_interval(hold).is_some_and(|[first, through]| {
        first >= 0 && (first..=through).contains(&position)
    })
}

fn payload_log_epoch(record: &DeploymentRecord) -> i32 {
    record.checkpoint_stage.as_ref().filter(|stage| stage.transfer_only)
        .map_or(record.log_epoch, |stage| stage.source_log_epoch)
}

fn require_publication(
    history: History, route: PublishRoute, record: &DeploymentRecord, key: &ObjectKey,
) -> Result<()> {
    if !history.matches(record) { return Err(refused()); }
    match route {
        PublishRoute::Writer { epoch } => require_writer(history, epoch, record),
        PublishRoute::Transfer { operation, bootstrap, position } => {
            let replica = record.checkpoint_transfer_replica.as_deref().ok_or_else(refused)?;
            let receipt = &replica.bootstrap;
            let terminal = &receipt.terminal;
            let stage = &terminal.fence.destination;
            if record.lifecycle != Lifecycle::Restoring || record.holder.is_some()
                || record.checkpoint_stage.as_deref() != Some(stage)
                || record.checkpoint_transfer_fence.as_deref() != Some(&terminal.fence)
                || record.checkpoint_transfer_bootstrap.as_deref() != Some(receipt)
                || stage.intent.operation != operation || receipt.ciphertext_sha256 != bootstrap
                || position <= stage.checkpoint.boundary || position > terminal.replica_through
                || !crate::archive::admits_seeded_transfer_payload(record, stage, key.as_str())
            {
                return Err(refused());
            }
            Ok(())
        }
        PublishRoute::CellTransfer { generation, position } => {
            let transfer = record.cell_transfer.as_deref().ok_or_else(refused)?;
            let attempt = transfer.attempt.as_ref().ok_or_else(refused)?;
            if record.lifecycle != Lifecycle::Moving || record.holder.is_some()
                || transfer.destination_incarnation != record.incarnation || transfer.epoch != record.epoch
                || record.move_from.as_ref().is_none_or(Vec::is_empty)
                || record.storage_generation != Some(generation) || attempt.generation != generation
                || record.abandoned_generations.contains_key(&generation)
                || position < 0
            {
                return Err(refused());
            }
            Ok(())
        }
    }
}

fn require_writer(history: History, epoch: u64, record: &DeploymentRecord) -> Result<()> {
    if !history.matches(record) || epoch == 0 || record.epoch != epoch || record.holder.is_none() || record.is_resident_idle()
        || !matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
    {
        return Err(refused());
    }
    Ok(())
}

fn decode_reference(frame: &[u8]) -> Result<ReplicaPayloadReference> {
    Ok(ReplicaPayloadReference::from_frame(frame, CURRENT_STORAGE_VERSION, StorageEpoch(0))?.ok_or_else(invalid_frame)?)
}

fn check_publish_key(key: &ObjectKey) -> Result<()> {
    let block = oversized_block_index(key.as_str()).ok_or_else(invalid_frame)?;
    let allocation = key.as_str().split_once('-').map(|(_, tail)| tail)
        .and_then(|tail| tail.strip_suffix(".binpb"))
        .and_then(|id| Uuid::parse_str(id).ok()).filter(|id| !id.is_nil()).ok_or_else(invalid_frame)?;
    if oversized_key(block, &allocation.to_string())? != key.as_str() { return Err(invalid_frame()); }
    Ok(())
}

async fn socket_work<T>(work: impl Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout(SOCKET_LIMIT, work).await.map_err(transport)?
}

async fn send_header(socket: &mut (impl AsyncWrite + Unpin), header: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(header).map_err(transport)?;
    if bytes.len() > MAX_HEADER { return Err(invalid_frame()); }
    socket.write_u32(bytes.len() as u32).await.map_err(transport)?;
    socket.write_all(&bytes).await.map_err(transport)?;
    socket.flush().await.map_err(transport)
}

async fn receive_header<T: DeserializeOwned>(socket: &mut (impl AsyncRead + Unpin)) -> Result<T> {
    let length = socket.read_u32().await.map_err(transport)? as usize;
    if length == 0 || length > MAX_HEADER { return Err(invalid_frame()); }
    let mut bytes = vec![0; length];
    socket.read_exact(&mut bytes).await.map_err(transport)?;
    serde_json::from_slice(&bytes).map_err(|_| invalid_frame())
}

async fn read_response(socket: &mut (impl AsyncRead + Unpin)) -> Result<Response> {
    match receive_header(socket).await? {
        Response::Refused { category, code } => Err(Anomaly::new(category, code, "The detached replay broker refused the operation")),
        response => Ok(response),
    }
}

fn check_length(length: usize) -> Result<()> {
    if length == 0 || length > MAX_BODY {
        return Err(Anomaly::incorrect("rtdb.multi/replay-broker-size", "The detached replay body exceeds the broker's allocation bound"));
    }
    Ok(())
}

async fn receive_body(
    socket: &mut (impl AsyncRead + Unpin), length: usize, memory: &Arc<MemoryBudget>,
) -> Result<(Bytes, Arc<rtdb_types::MemoryReservation>)> {
    check_length(length)?;
    let charge = memory.reserve(length)?;
    let mut bytes = vec![0; length];
    socket.read_exact(&mut bytes).await.map_err(transport)?;
    Ok((bytes.into(), charge))
}

fn refused() -> Anomaly {
    Anomaly::new(AnomalyCategory::Forbidden, "rtdb.multi/replay-broker-refused", "The current replay route does not authorize this operation")
}

fn invalid_frame() -> Anomaly {
    Anomaly::incorrect("rtdb.multi/replay-broker-frame", "Invalid detached replay broker frame")
}

fn transport(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/replay-broker-transport", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn detached_version_settles_under_its_certified_fence_before_physical_trim() {
        use crate::{hosted_recovery::{ReplicaTrimPlan, observe_checkpoint}, keys::LocalKeyService, lease::LeaseOptions,
            logs::JournalUnits, process};
        use rtdb_directory::{Checkpoint, CheckpointDecision, MemberId, MemoryDirectory};
        use rtdb_format::log::proto::NoOp;
        use rtdb_storage::log::{Log, LogOffset};
        use rtdb_types::{BlockIndex, S3ObjectNamespace, Term, TermSequence, Timestamp};

        let data = tempfile::tempdir().unwrap();
        let directory = MemoryDirectory::new();
        let journal = process::start_logkeeper(directory.clone(), process::LogkeeperSettings {
            listen: "127.0.0.1:0".parse().unwrap(), advertise_host: None, authority: None,
            data_dir: data.path().join("journal"), fsync: true, cache_bytes: 1 << 20,
            availability_zone: "local".into(), lease: LeaseOptions::default(),
        }).await.unwrap();
        let mut record = crate::admin::create_deployment(directory.as_ref(), "shop", "acme", 1).await.unwrap();
        record.holder = Some(MemberId("writer".into()));
        record.epoch = 1;
        record.lifecycle = Lifecycle::Live;
        let record = directory.replace_deployment(record.clone(), record.version).await.unwrap();
        let Cas::Applied(mut record) = record else { panic!("the directory refused the test holder"); };
        let stream = crate::admin::administration_stream(directory.as_ref(), &record, process::MAXIMUM_LOG_MESSAGE_BYTES).await.unwrap();
        let shutdown = CancellationToken::new();
        let writer = rtdb_logkeeper::open_writer(stream, record.epoch, shutdown.clone()).await.unwrap();
        let keys = Arc::new(LocalKeyService::new(data.path().join("journal-keys")).unwrap());
        keys.key(&record.key_id()).await.unwrap();
        let io = IoRegion::new();
        let slots = JournalUnits::open(keys.as_ref(), &record.key_id(), &record.replica_seal_scope(), io.clone()).await.unwrap();

        let allocation = Uuid::new_v4().to_string();
        let relative = oversized_key(BlockIndex(0), &allocation).unwrap();
        let key = ObjectKey::new(format!("{}/{relative}", residence(&record))).unwrap();
        let version = PhysicalObjectVersion {
            namespace: S3ObjectNamespace { bucket: "detached-fixture".into(), endpoint: None, region: None },
            key: key.to_string(), version: "exact-version".into(),
        };
        let data_frame = ReplicaEnvelope {
            term: Term(1), term_sequence: Some(TermSequence(0)), message: ReplicaBody::NoOp(NoOp::default()),
        };
        let body = Bytes::from(data_frame.encode());
        let broker = Service::open(directory.clone(), CellStorage::Local { root: data.path().join("detached") },
            &CellStorage::Local { root: data.path().join("serving") },
            Arc::new(LocalKeyService::new(data.path().join("detached-keys")).unwrap()), None, None, None, None).unwrap();
        let relative = ObjectKey::new(relative).unwrap();
        let opened = broker.open_store(&record, io.clone()).await.unwrap();
        opened.put(&relative, body.clone()).await.unwrap();
        let mut pointer = data_frame.offloaded(CURRENT_STORAGE_VERSION, StorageEpoch(0), BlockIndex(0), allocation, &body);
        pointer.bind_offload_publication(version.clone()).unwrap();
        for (lsn, frame) in [Bytes::from(pointer.encode()), body.clone(), body.clone()].into_iter().enumerate() {
            let sealed = slots.seed(lsn as i64, frame).await.unwrap();
            let receipt = writer.enqueue(sealed, Timestamp(lsn as i64)).await.unwrap().durable().await.unwrap();
            assert_eq!(receipt.offset, LogOffset(lsn as i64));
        }
        let read = writer.read_index(record.epoch).await.unwrap();
        let decision = read.decision().unwrap();
        let checkpoint = Checkpoint { block: 0, boundary: 1, source: -1, system_time: 0, root_sha256: Some([9; 32]) };
        let manifest = [7; 32];
        let fingerprint = decision.certificate.statement.fingerprint();
        record.checkpoint = Some(checkpoint);
        record.recovery_manifest_sha256 = Some(manifest);
        record.checkpoint_decision = Some(Box::new(CheckpointDecision {
            checkpoint, manifest, stable: prost::Message::encode_to_vec(decision),
        }));
        let installed = directory.replace_deployment(record.clone(), record.version).await.unwrap();
        let Cas::Applied(record) = installed else { panic!("the directory refused the checkpoint"); };
        observe_checkpoint(&record, checkpoint, manifest, Some(fingerprint));

        let mut held = record.clone();
        held.storage_regions.insert(Uuid::new_v4(), StorageRegion::ReplicaReplay {
            first_required: 0, through: 0, custody: ReplayCustody::Local,
        });
        assert!(ReplicaTrimPlan::new(held, checkpoint.boundary, -1, 0).is_none());
        let members = directory.members().await.unwrap().into_iter().map(|member| (member.id.clone(), member)).collect();
        let plan = ReplicaTrimPlan::new(record.clone(), checkpoint.boundary, -1, 0).unwrap();
        let _trim = plan.authorize(directory.as_ref(), &members, keys.as_ref(), true).await.unwrap().unwrap();

        let later = Uuid::new_v4();
        let record = crate::admin::update_installation(directory.as_ref(), &record.id, |record| {
            record.storage_regions.insert(later, StorageRegion::ReplicaReplay {
                first_required: 1, through: 1, custody: ReplayCustody::Local,
            });
            Ok(())
        }).await.unwrap();
        let candidate = &record.replica_payload_trim.as_ref().unwrap().candidates[0];
        assert_eq!(candidate.version, version);
        broker.settle_candidate(&record, candidate, relative.clone(), key.clone(), |selected_key, selected_version| {
            let (broker, io, opened, relative) = (&broker, &io, &opened, &relative);
            async move {
                assert_eq!(selected_key, key);
                assert_eq!(selected_version, version);
                assert!(matches!(opened.get(relative).await, Err(rtdb_storage::Error::ErasedUnderView)));
                let store = broker.storage.bucket(io.clone()).await?;
                store.delete(&selected_key).await?;
                Ok(())
            }
        }).await.unwrap();
        let settled = directory.deployment(&record.id).await.unwrap().unwrap();
        assert!(settled.replica_payload_trim.unwrap().candidates.is_empty());
        assert_eq!(writer.read_range(LogOffset(0), LogOffset(1)).await.unwrap().len(), 1);
        io.close_and_wait().await;
        shutdown.cancel();
        journal.stop().await.unwrap();
    }

    #[test]
    fn broker_publication_follows_the_exact_installed_epoch_and_residence() {
        let mut record = DeploymentRecord::new("shop", "acme", Vec::new(), 0);
        record.holder = Some(rtdb_directory::MemberId("writer".into()));
        record.epoch = 7;
        record.lifecycle = Lifecycle::Live;
        let history = History::of(&record).unwrap();
        assert!(require_writer(history, 7, &record).is_ok());
        assert!(require_writer(history, 6, &record).is_err());
        record.lifecycle = Lifecycle::Moving;
        assert!(require_writer(history, 7, &record).is_err());
        record.lifecycle = Lifecycle::Live;
        record.incarnation += 1;
        assert!(require_writer(history, 7, &record).is_err());
    }
}
