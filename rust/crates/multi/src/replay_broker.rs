//! The directory replay broker owns detached body storage and its unit-key vault.

use std::{path::Path, sync::Arc, time::Duration};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use rtdb_directory::{Cas, DeploymentRecord, Directory, Lifecycle, ReplayCustody, StorageRegion};
use rtdb_format::{envelope::{ReplicaBody, ReplicaEnvelope}, keys::{CURRENT_STORAGE_VERSION, oversized_block_index, oversized_key}};
use rtdb_storage::{IoRegion, ObjectKey, ObjectStore, log::{ReplicaPayload, ReplicaPayloadReference}};
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
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    deployment::CellStorage,
    install::Token,
    keys::{KeyService, UnitLedger, UnitScope},
    reference::PrefixedStore,
    store::EncryptingStore,
};

const MAX_HEADER: usize = 16 << 10;
const MAX_BODY: usize = 64 << 20;
const SOCKET_LIMIT: Duration = Duration::from_secs(30);
const CAS_ATTEMPTS: usize = 8;

#[derive(Clone, Copy, Deserialize, Serialize)]
pub(crate) struct History {
    pub deployment: Uuid,
    pub incarnation: u64,
    pub log_epoch: i32,
}

impl History {
    pub(crate) fn of(record: &DeploymentRecord) -> Result<Self> {
        Ok(Self {
            deployment: Uuid::parse_str(&record.id).map_err(|_| refused())?,
            incarnation: record.incarnation,
            log_epoch: record.log_epoch,
        })
    }

    fn matches(self, record: &DeploymentRecord) -> bool {
        record.incarnation == self.incarnation && record.log_epoch == self.log_epoch
            && Uuid::parse_str(&record.id).ok() == Some(self.deployment)
    }
}

#[derive(Deserialize, Serialize)]
struct Request {
    token: String,
    history: History,
    action: Action,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Action {
    Read { ticket: Uuid, position: i64, reference: String },
    Publish { epoch: u64, object: String, length: usize },
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Response {
    Payload { length: usize },
    Publication { version: Option<PhysicalObjectVersion> },
    Refused { category: AnomalyCategory, code: String },
}

#[derive(Clone)]
pub struct Client {
    endpoint: Arc<str>,
    server_name: ServerName<'static>,
    tls: Arc<ClientConfig>,
    token: Arc<str>,
}

impl Client {
    pub fn from_files(endpoint: &str, server_name: &str, ca: &Path, token: &Path) -> Result<Self> {
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
        Ok(Self {
            endpoint: Arc::from(endpoint),
            server_name: ServerName::try_from(server_name.to_owned()).map_err(transport)?,
            tls: Arc::new(tls),
            token: Arc::from(Token::read(token)?),
        })
    }

    async fn connect(&self, history: History, action: Action) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
        socket_work(async {
            let stream = TcpStream::connect(self.endpoint.as_ref()).await.map_err(transport)?;
            let mut stream = TlsConnector::from(Arc::clone(&self.tls))
                .connect(self.server_name.clone(), stream).await.map_err(transport)?;
            send_header(&mut stream, &Request { token: self.token.to_string(), history, action }).await?;
            Ok(stream)
        }).await
    }

    pub(crate) async fn read(
        &self, history: History, ticket: Uuid, position: i64, frame: &[u8], memory: &Arc<MemoryBudget>,
    ) -> Result<ReplicaPayload> {
        let reference = decode_reference(frame)?;
        let action = Action::Read { ticket, position, reference: URL_SAFE_NO_PAD.encode(frame) };
        let mut socket = self.connect(history, action).await?;
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
        check_length(bytes.len())?;
        let action = Action::Publish { epoch, object: object.to_string(), length: bytes.len() };
        let mut socket = self.connect(history, action).await?;
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

pub struct Service {
    directory: Arc<dyn Directory>,
    storage: CellStorage,
    namespace: [u8; 32],
    keys: Arc<dyn KeyService>,
    token: Token,
    custody: ReplayCustody,
    memory: Arc<MemoryBudget>,
}

impl Service {
    pub fn new(
        directory: Arc<dyn Directory>, storage: CellStorage, serving: &CellStorage, keys: Arc<dyn KeyService>,
        token: &Path, task: Option<rtdb_directory::HostedTask>,
    ) -> Result<Arc<Self>> {
        if storage.overlaps(serving)? {
            return Err(Anomaly::incorrect("rtdb.multi/replay-broker-storage",
                "The detached replay broker requires an object namespace outside serving storage"));
        }
        let namespace = storage.custody_namespace()?;
        let custody = match (&storage, task) {
            (CellStorage::Local { .. }, None) => ReplayCustody::Local,
            (_, owner) => ReplayCustody::Hosted { owner },
        };
        Ok(Arc::new(Self {
            directory, storage, namespace, keys, custody, token: Token::new(&Token::read(token)?)?,
            memory: MemoryBudget::new(rtdb_node::NodeOptions::default().execution_memory_bytes),
        }))
    }

    pub async fn serve(
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
                let Ok(mut socket) = socket_work(async { acceptor.accept(socket).await.map_err(transport) }).await else { return; };
                if let Err(error) = service.dispatch(&mut socket).await {
                    let response = Response::Refused { category: error.category, code: error.code };
                    let _ = socket_work(send_header(&mut socket, &response)).await;
                }
            });
        };
        while connections.join_next().await.is_some() {}
        result
    }

    async fn dispatch(&self, socket: &mut tokio_rustls::server::TlsStream<TcpStream>) -> Result<()> {
        let request: Request = socket_work(receive_header(socket)).await?;
        if !self.token.accepts(&request.token) { return Err(refused()); }
        let history = request.history;
        match request.action {
            Action::Read { ticket, position, reference } => {
                let frame = URL_SAFE_NO_PAD.decode(reference).map_err(|_| invalid_frame())?;
                let reference = decode_reference(&frame)?;
                let region = Uuid::new_v4();
                let local = match self.custody {
                    ReplayCustody::Local => self.keys.hold_local_storage_region(region)?,
                    ReplayCustody::Hosted { .. } => None,
                };
                let mut attempted = false;
                let result = async {
                    let record = self.admit_read(history, ticket, position, region, &mut attempted).await?;
                    let io = IoRegion::new();
                    let read = async {
                        let payload = self.read_body(&record, position, &frame, &reference, Arc::clone(&io)).await?;
                        socket_work(async {
                            send_header(socket, &Response::Payload { length: payload.as_ref().len() }).await?;
                            socket.write_all(payload.as_ref()).await.map_err(transport)?;
                            socket.flush().await.map_err(transport)
                        }).await
                    }.await;
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
            Action::Publish { epoch, object, length } => {
                let key = ObjectKey::new(object)?;
                check_publish_key(&key)?;
                let owner = history.deployment.to_string();
                let task = match &self.custody {
                    ReplayCustody::Local => None,
                    ReplayCustody::Hosted { owner } => owner.as_ref(),
                };
                // Reserve the broker's provider admission before reading the
                // body. A concurrent custody closure can now fence or drain it.
                let body_stream = &mut *socket;
                let version = crate::archive::stage::with_region(
                    self.directory.as_ref(), self.keys.as_ref(), &owner, &self.storage, task,
                    None, None,
                    |record| {
                        require_writer(history, epoch, record)?;
                        Ok(None)
                    },
                    |record, _, io| async move {
                        socket_work(async {
                            let (body, charge) = receive_body(body_stream, length, &self.memory).await?;
                            let envelope = ReplicaEnvelope::decode(&body)?.ok_or_else(invalid_frame)?;
                            if matches!(envelope.message, ReplicaBody::OversizedMessage(_)) {
                                return Err(invalid_frame());
                            }
                            let current = self.writer(history, epoch).await?;
                            if current.storage_prefix() != record.storage_prefix() {
                                return Err(refused());
                            }
                            let files = self.open(&record, io).await?;
                            let published = IoRegion::retaining(charge, files.put_with_publication(&key, body)).await?;
                            let version = published.and_then(rtdb_types::PhysicalObjectPublication::into_version);
                            if version.is_none() && matches!(self.storage, CellStorage::S3 { .. }) {
                                return Err(Anomaly::incorrect("rtdb.multi/replay-broker-version", "Detached S3 bodies require versioned custody"));
                            }
                            Ok(version)
                        }).await
                    },
                ).await?;
                self.writer(history, epoch).await?;
                socket_work(send_header(socket, &Response::Publication { version })).await
            }
        }
    }

    async fn current(&self, history: History) -> Result<DeploymentRecord> {
        self.directory.deployment(&history.deployment.to_string()).await?
            .filter(|record| history.matches(record)).ok_or_else(refused)
    }

    async fn writer(&self, history: History, epoch: u64) -> Result<DeploymentRecord> {
        let record = self.current(history).await?;
        require_writer(history, epoch, &record)?;
        Ok(record)
    }

    async fn admit_read(
        &self, history: History, ticket: Uuid, position: i64, region: Uuid, attempted: &mut bool,
    ) -> Result<DeploymentRecord> {
        let after = position.checked_sub(1).filter(|after| *after >= -1).ok_or_else(refused)?;
        for _ in 0..CAS_ATTEMPTS {
            let mut record = self.current(history).await?;
            if matches!(record.lifecycle, Lifecycle::Deleting | Lifecycle::Deleted)
                || !record.storage_regions.get(&ticket).is_some_and(|hold| hold.requires_replica_offset(position))
            {
                return Err(refused());
            }
            crate::hosted_recovery::replay_start(&record, after)?;
            let version = record.version;
            record.storage_regions.insert(region, StorageRegion::ReplicaReplay {
                first_required: position, through: position, custody: self.custody.clone(),
            });
            *attempted = true;
            if let Cas::Applied(stored) = self.directory.replace_deployment(record, version).await? {
                return Ok(stored);
            }
        }
        Err(Anomaly::new(AnomalyCategory::Busy, "rtdb.multi/replay-broker-contended",
            "The directory kept changing during broker admission"))
    }

    async fn read_body(
        &self, record: &DeploymentRecord, position: i64, frame: &[u8],
        reference: &ReplicaPayloadReference, io: Arc<IoRegion>,
    ) -> Result<ReplicaPayload> {
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
        let files = self.open(record, io).await?;
        check_length(usize::try_from(files.head_fresh(reference.object()).await?.size).map_err(transport)?)?;
        Ok(reference.read_at(&files, &self.memory).await?)
    }

    async fn open(&self, record: &DeploymentRecord, io: Arc<IoRegion>) -> Result<EncryptingStore> {
        let residence = format!("replay-bodies/{}/{}", record.storage_prefix(), record.log_epoch);
        let bucket = self.storage.bucket(Arc::clone(&io)).await?;
        let files = Arc::new(PrefixedStore::new(bucket, &residence));
        let ledger = UnitLedger::new(Arc::clone(&self.keys), UnitScope::replay_objects(self.namespace, &residence, record.log_epoch), io);
        Ok(EncryptingStore::with_units(files, Arc::new(ledger)))
    }

    async fn release(&self, history: History, region: Uuid) {
        let deployment = history.deployment.to_string();
        loop {
            match crate::store::release_region(self.directory.as_ref(), &deployment, region, true).await {
                Ok(()) => return,
                Err(error) => tracing::warn!(%deployment, %region, code = %error.code, "Broker read region is awaiting release"),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

fn require_writer(history: History, epoch: u64, record: &DeploymentRecord) -> Result<()> {
    if !history.matches(record) || epoch == 0 || record.epoch != epoch || record.holder.is_none()
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
