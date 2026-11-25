use std::{
    collections::BTreeSet,
    ffi::OsString,
    fs::File,
    io::Read,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use ring::signature::{ED25519, UnparsedPublicKey};
use rtdb_db::{ClassicInstallation, classic_import_basis, discover_nodes, preferred};
use rtdb_tx::transact::TransactState;
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp, TransactionKey, rtdb_v1::Basis};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::JoinSet,
};
use tokio_rustls::{TlsAcceptor, rustls::ServerConfig};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{CLOCK_DRIFT_MICROS, ClassicCandidate, MultiRuntime};
use rtdb_node::{Clock, SystemClock};

pub const MAX_CONTROL_BYTES: usize = 16 << 10;
pub const MAX_CHECKPOINT_BYTES: usize = 64 << 20;
const MAX_GRANT_MICROS: i64 = 5 * 60 * 1_000_000;
const MAX_CONFIG_BYTES: usize = 64 << 10;
const IO_DEADLINE: Duration = Duration::from_secs(30);
const MAX_CONNECTIONS: usize = 8;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub begin_sha256: [u8; 32],
    pub request_sha256: [u8; 32],
    pub catalog: Uuid,
    pub name: String,
    pub destination: Uuid,
    pub capture: Uuid,
    pub scratch: Uuid,
    pub terminal: TransactionKey,
    pub terminal_sha256: [u8; 32],
    pub artifact_sha256: [u8; 32],
    pub projection_sha256: [u8; 32],
    pub checkpoint_sha256: [u8; 32],
    pub checkpoint_bytes: u32,
    pub basis: Basis,
}

impl Binding {
    pub fn identity(&self) -> Result<[u8; 32]> {
        commitment(b"rtdb.multi/classic-candidate-binding/v1\0", self)
    }

    fn validate(&self) -> Result<()> {
        super::validate_deployment_name(&self.name)?;
        if self.catalog.is_nil()
            || self.destination.is_nil()
            || self.capture.is_nil()
            || self.scratch.is_nil()
            || self.scratch == self.destination
            || self.destination == self.catalog
            || !(1..=MAX_CHECKPOINT_BYTES).contains(&(self.checkpoint_bytes as usize))
            || [
                self.begin_sha256,
                self.request_sha256,
                self.terminal_sha256,
                self.artifact_sha256,
                self.projection_sha256,
                self.checkpoint_sha256,
            ]
            .contains(&[0; 32])
            || self.basis
                != classic_import_basis(*self.destination.as_bytes(), self.terminal.tx_id)?
        {
            return Err(denied());
        }
        Ok(())
    }

    fn matches_candidate(&self, candidate: &ClassicCandidate) -> bool {
        candidate.reserved_at.incarnation() == *self.catalog.as_bytes()
            && candidate.reserved_at.position() > 0
            && candidate.projection_sha256 == self.projection_sha256
            && candidate.installation.as_ref().is_none_or(|installed| {
                installed.basis == self.basis
                    && installed.system_time == self.terminal.system_time
                    && installed.checkpoint_sha256 == self.checkpoint_sha256
            })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    Stage,
    Install,
    Status { classic_cut: Option<TransactionKey> },
    HoldReady { ready: Box<ReadySelection> },
    HeldReadyStatus { ready: Box<ReadySelection> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadySelection {
    pub endpoint: String,
    pub placement: HeldAssignment,
    pub classic_cut: TransactionKey,
    pub native_basis: Basis,
    pub reserved_at: Basis,
    pub installation: ClassicInstallation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeldAssignment {
    pub epoch: u64,
    pub assignment: [u8; 16],
    pub holder: [u8; 16],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeldReady {
    pub binding_sha256: [u8; 32],
    pub ready: ReadySelection,
    pub recorded_at: Basis,
}

impl ReadySelection {
    pub(super) fn valid_for(&self, catalog: Uuid, destination: Uuid) -> bool {
        self.endpoint
            .strip_prefix("https://")
            .is_some_and(|authority| {
                let host = authority.strip_suffix(":443").unwrap_or(authority);
                !host.is_empty()
                    && host.len() <= 253
                    && host
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
            })
            && self.placement.epoch != 0
            && self.placement.assignment != [0; 16]
            && self.placement.holder != [0; 16]
            && self.reserved_at.incarnation() == *catalog.as_bytes()
            && self.reserved_at.position() != 0
            && self.installation.basis.incarnation() == *destination.as_bytes()
            && self.native_basis <= self.installation.basis
            && self.classic_cut.system_time <= self.installation.system_time
            && classic_import_basis(*destination.as_bytes(), self.classic_cut.tx_id)
                .is_ok_and(|basis| self.native_basis == basis)
    }

    fn matches_binding(&self, binding: &Binding) -> bool {
        self.valid_for(binding.catalog, binding.destination)
            && self.classic_cut.at_or_before(binding.terminal)
            && self.installation.basis == binding.basis
            && self.installation.system_time == binding.terminal.system_time
            && self.installation.checkpoint_sha256 == binding.checkpoint_sha256
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    pub binding: Binding,
    pub operation: Operation,
    pub expires_at_micros: i64,
}

impl Grant {
    pub fn signing_digest(&self) -> Result<[u8; 32]> {
        commitment(b"rtdb.multi/classic-candidate-approval/v1\0", self)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Authorization {
    pub grant: Grant,
    pub signature: Vec<u8>,
}

impl Authorization {
    pub fn verify(&self, approval: [u8; 32], now: Timestamp) -> Result<()> {
        self.grant.binding.validate()?;
        let deadline = self.grant.expires_at_micros;
        if now.0.saturating_add(CLOCK_DRIFT_MICROS) >= deadline
            || deadline > now.0.saturating_add(MAX_GRANT_MICROS)
        {
            return Err(denied());
        }
        match &self.grant.operation {
            Operation::Status {
                classic_cut: Some(cut),
            } if !cut.at_or_before(self.grant.binding.terminal) => return Err(denied()),
            Operation::HoldReady { ready } | Operation::HeldReadyStatus { ready }
                if !ready.matches_binding(&self.grant.binding) =>
            {
                return Err(denied());
            }
            _ => (),
        }
        verify(approval, &self.grant.signing_digest()?, &self.signature)
    }

    pub fn source_digest(&self) -> Result<[u8; 32]> {
        commitment(b"rtdb.multi/classic-candidate-original/v1\0", self)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub authorization: Authorization,
    pub source_signature: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum Response {
    Observed {
        binding_sha256: [u8; 32],
        candidate: Option<ClassicCandidate>,
        mapped_cut: Option<(TransactionKey, Basis)>,
        #[serde(skip_serializing_if = "Option::is_none")]
        held_ready: Option<Box<HeldReady>>,
    },
    Redirect {
        writer: IpAddr,
        binding_sha256: [u8; 32],
    },
    Refused,
    Deferred,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    port: u16,
    certificate_file: PathBuf,
    private_key_file: PathBuf,
    approval_public_key: [u8; 32],
    source_public_key: [u8; 32],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SecretConfiguration {
    certificate_pem: String,
    private_key_pem: String,
    approval_public_key: [u8; 32],
    source_public_key: [u8; 32],
}

pub(super) struct Ingress {
    port: u16,
    tls: Arc<ServerConfig>,
    approval: [u8; 32],
    source: [u8; 32],
}

impl Ingress {
    pub(super) fn installed() -> Result<Option<Arc<Self>>> {
        Self::configured(
            std::env::var_os("RTDB_CLASSIC_CANDIDATE_INGRESS_FILE"),
            std::env::var_os("RTDB_CLASSIC_CANDIDATE_INGRESS_CONFIG"),
            std::env::var_os("RTDB_CLASSIC_CANDIDATE_INGRESS_PORT"),
        )
    }

    fn configured(
        file: Option<OsString>,
        secret: Option<OsString>,
        port: Option<OsString>,
    ) -> Result<Option<Arc<Self>>> {
        match (file, secret, port) {
            (None, None, None) => Ok(None),
            (Some(path), None, None) => Self::from_file(Path::new(&path)).map(Some),
            (None, Some(secret), Some(port)) => Self::from_secret(secret, port).map(Some),
            _ => Err(invalid_configuration()),
        }
    }

    fn from_file(path: &Path) -> Result<Arc<Self>> {
        let configuration: Configuration =
            serde_json::from_slice(&read_file(path, MAX_CONTROL_BYTES)?)
                .map_err(|_| invalid_configuration())?;
        let certificate = read_file(&configuration.certificate_file, MAX_CONFIG_BYTES)?;
        let key = zeroize::Zeroizing::new(read_file(
            &configuration.private_key_file,
            MAX_CONFIG_BYTES,
        )?);
        Self::from_identity(
            configuration.port,
            configuration.approval_public_key,
            configuration.source_public_key,
            &certificate,
            &key,
        )
    }

    fn from_secret(secret: OsString, port: OsString) -> Result<Arc<Self>> {
        let secret =
            zeroize::Zeroizing::new(secret.into_string().map_err(|_| invalid_configuration())?);
        if secret.len() > MAX_CONFIG_BYTES {
            return Err(invalid_configuration());
        }
        let configuration: SecretConfiguration =
            serde_json::from_str(&secret).map_err(|_| invalid_configuration())?;
        let key = zeroize::Zeroizing::new(configuration.private_key_pem);
        let port = port
            .to_str()
            .and_then(|port| port.parse::<u16>().ok())
            .filter(|port| *port >= 1024 && *port != 3000)
            .ok_or_else(invalid_configuration)?;
        Self::from_identity(
            port,
            configuration.approval_public_key,
            configuration.source_public_key,
            configuration.certificate_pem.as_bytes(),
            key.as_bytes(),
        )
    }

    fn from_identity(
        port: u16,
        approval: [u8; 32],
        source: [u8; 32],
        certificate: &[u8],
        key: &[u8],
    ) -> Result<Arc<Self>> {
        if port == 0
            || port == super::peer::PORT
            || approval == [0; 32]
            || source == [0; 32]
            || approval == source
        {
            return Err(invalid_configuration());
        }
        let tls = crate::tls::from_pem_identity(certificate, key)?;
        Ok(Arc::new(Self {
            port,
            tls: Arc::new(tls),
            approval,
            source,
        }))
    }

    pub(super) async fn serve(
        self: &Arc<Self>,
        runtime: &MultiRuntime,
        stop: &CancellationToken,
    ) -> Result<()> {
        let address = SocketAddr::new(runtime.backends.node, self.port);
        let listener = TcpListener::bind(address).await.map_err(transport)?;
        let operations = Arc::new(Semaphore::new(1));
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                _ = connections.join_next(), if !connections.is_empty() => (),
                socket = listener.accept(), if connections.len() < MAX_CONNECTIONS => {
                    let (socket, _) = socket.map_err(transport)?;
                    let owner = Arc::clone(self);
                    let runtime = runtime.clone();
                    let operations = Arc::clone(&operations);
                    connections.spawn(async move { owner.exchange(&runtime, socket, &operations).await });
                }
            }
        }
        drop(listener);
        while connections.join_next().await.is_some() {}
        Ok(())
    }

    async fn exchange(&self, runtime: &MultiRuntime, socket: TcpStream, operations: &Semaphore) {
        let Ok(Ok(mut tls)) = tokio::time::timeout(
            IO_DEADLINE,
            TlsAcceptor::from(self.tls.clone()).accept(socket),
        )
        .await
        else {
            return;
        };
        let outcome = match tokio::time::timeout(IO_DEADLINE, read_control(&mut tls)).await {
            Ok(Ok(request)) => self.admit(runtime, request, &mut tls, operations).await,
            Ok(Err(error)) => Err(error),
            Err(_) => Err(unavailable()),
        };
        let reply = match outcome {
            Ok(reply) => reply,
            Err(error)
                if matches!(
                    error.category,
                    AnomalyCategory::Forbidden
                        | AnomalyCategory::Incorrect
                        | AnomalyCategory::Conflict
                ) =>
            {
                Response::Refused
            }
            Err(_) => Response::Deferred,
        };
        let _ = tokio::time::timeout(IO_DEADLINE, write_control(&mut tls, &reply)).await;
    }

    fn authenticate(&self, runtime: &MultiRuntime, request: &Request) -> Result<()> {
        if runtime.peer.is_none() {
            return Err(denied());
        }
        authenticate_request(
            runtime.catalog.incarnation(),
            self.approval,
            self.source,
            request,
            SystemClock.now(),
        )
    }

    async fn admit(
        &self,
        runtime: &MultiRuntime,
        request: Request,
        stream: &mut (impl AsyncRead + Unpin),
        operations: &Semaphore,
    ) -> Result<Response> {
        self.authenticate(runtime, &request)?;
        let _operation = operations.try_acquire().map_err(|_| unavailable())?;
        let live = discover_nodes(runtime.backends.peers.as_deref().ok_or_else(denied)?).await?;
        if !live.contains(&runtime.backends.node) {
            return Err(unavailable());
        }
        let grant = &request.authorization.grant;
        let bytes = tokio::time::timeout(IO_DEADLINE, read_checkpoint(stream, runtime, grant))
            .await
            .map_err(|_| unavailable())??;
        if grant.operation == Operation::Install
            && <[u8; 32]>::from(Sha256::digest(bytes.0.as_slice()))
                != grant.binding.checkpoint_sha256
        {
            return Err(denied());
        }
        self.authenticate(runtime, &request)?;
        self.execute(runtime, grant, &live, &bytes.0).await
    }

    async fn execute(
        &self,
        runtime: &MultiRuntime,
        grant: &Grant,
        live: &BTreeSet<IpAddr>,
        bytes: &[u8],
    ) -> Result<Response> {
        let binding = &grant.binding;
        let binding_sha256 = binding.identity()?;
        let existing = runtime
            .classic_candidate_status(&binding.name, binding.destination, binding_sha256)
            .await?;
        if existing
            .as_ref()
            .is_some_and(|candidate| !binding.matches_candidate(candidate))
        {
            return Err(denied());
        }
        let mut mapped_cut = None;
        let mut held_ready = None;
        let candidate = match &grant.operation {
            Operation::Stage => Some(
                runtime
                    .stage_classic_candidate(
                        &binding.name,
                        binding.destination,
                        binding_sha256,
                        binding.projection_sha256,
                    )
                    .await?,
            ),
            Operation::Install => {
                let candidate = existing.ok_or_else(denied)?;
                if candidate.installation.is_some() {
                    Some(candidate)
                } else {
                    let writer = preferred(binding.destination, live).ok_or_else(unavailable)?;
                    if writer != runtime.backends.node {
                        return Ok(Response::Redirect {
                            writer,
                            binding_sha256,
                        });
                    }
                    let mut cut = TransactState::new(*binding.destination.as_bytes())
                        .index()
                        .head();
                    cut.basis = binding.basis;
                    cut.system_time = binding.terminal.system_time;
                    let transactions = TransactState::decode_checkpoint(bytes, cut)?;
                    if transactions.encode_checkpoint()?.as_ref() != bytes {
                        return Err(denied());
                    }
                    Some(
                        runtime
                            .install_classic_candidate(
                                &binding.name,
                                binding.destination,
                                binding_sha256,
                                binding.projection_sha256,
                                &transactions,
                            )
                            .await?,
                    )
                }
            }
            Operation::Status { classic_cut } => {
                if let Some(cut) = *classic_cut
                    && existing
                        .as_ref()
                        .is_some_and(|candidate| candidate.installation.is_some())
                {
                    let basis = runtime
                        .classic_candidate_cut(
                            &binding.name,
                            binding.destination,
                            binding_sha256,
                            cut,
                        )
                        .await?;
                    mapped_cut = Some((cut, basis));
                }
                existing
            }
            Operation::HoldReady { ready } | Operation::HeldReadyStatus { ready } => {
                let candidate = existing.ok_or_else(denied)?;
                let record = matches!(&grant.operation, Operation::HoldReady { .. });
                if record && let Some(peer) = &runtime.peer {
                    peer.observe(live);
                }
                held_ready = runtime
                    .classic_held_ready(
                        &binding.name,
                        binding.destination,
                        binding_sha256,
                        &candidate,
                        ready,
                        record,
                    )
                    .await?
                    .map(Box::new);
                mapped_cut = Some((ready.classic_cut, ready.native_basis));
                Some(candidate)
            }
        };
        if candidate
            .as_ref()
            .is_some_and(|candidate| !binding.matches_candidate(candidate))
        {
            return Err(denied());
        }
        Ok(Response::Observed {
            binding_sha256,
            candidate,
            mapped_cut,
            held_ready,
        })
    }
}

pub async fn read_control<T: serde::de::DeserializeOwned>(
    stream: &mut (impl AsyncRead + Unpin),
) -> Result<T> {
    let len = stream.read_u32().await.map_err(transport)? as usize;
    if !(1..=MAX_CONTROL_BYTES).contains(&len) {
        return Err(denied());
    }
    let mut bytes = vec![0; len];
    stream.read_exact(&mut bytes).await.map_err(transport)?;
    serde_json::from_slice(&bytes).map_err(|_| denied())
}

pub async fn write_control(
    stream: &mut (impl AsyncWrite + Unpin),
    value: &impl Serialize,
) -> Result<()> {
    let bytes = serde_json::to_vec(value).map_err(|_| denied())?;
    if bytes.len() > MAX_CONTROL_BYTES {
        return Err(denied());
    }
    stream
        .write_u32(bytes.len() as u32)
        .await
        .map_err(transport)?;
    stream.write_all(&bytes).await.map_err(transport)?;
    stream.flush().await.map_err(transport)
}

async fn read_checkpoint(
    stream: &mut (impl AsyncRead + Unpin),
    runtime: &MultiRuntime,
    grant: &Grant,
) -> Result<(
    zeroize::Zeroizing<Vec<u8>>,
    Arc<rtdb_types::MemoryReservation>,
)> {
    let len = stream.read_u32().await.map_err(transport)?;
    let expected = if grant.operation == Operation::Install {
        grant.binding.checkpoint_bytes
    } else {
        0
    };
    if len != expected {
        return Err(denied());
    }
    let len = len as usize;
    let charge = runtime.memory.reserve(len * 6 + MAX_CONTROL_BYTES)?;
    let mut bytes = zeroize::Zeroizing::new(vec![0; len]);
    stream.read_exact(&mut bytes).await.map_err(transport)?;
    Ok((bytes, charge))
}

fn read_file(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)
        .and_then(|file| file.take(limit as u64 + 1).read_to_end(&mut bytes))
        .map_err(|_| invalid_configuration())?;
    if bytes.len() > limit {
        return Err(invalid_configuration());
    }
    Ok(bytes)
}

fn authenticate_request(
    catalog: Uuid,
    approval: [u8; 32],
    source: [u8; 32],
    request: &Request,
    now: Timestamp,
) -> Result<()> {
    let authorization = &request.authorization;
    if authorization.grant.binding.catalog != catalog {
        return Err(denied());
    }
    authorization.verify(approval, now)?;
    verify(
        source,
        &authorization.source_digest()?,
        &request.source_signature,
    )
}

fn verify(key: [u8; 32], digest: &[u8; 32], signature: &[u8]) -> Result<()> {
    UnparsedPublicKey::new(&ED25519, key)
        .verify(digest, signature)
        .map_err(|_| denied())
}

fn commitment(domain: &[u8], value: &impl Serialize) -> Result<[u8; 32]> {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(serde_json::to_vec(value).map_err(|_| denied())?);
    Ok(hash.finalize().into())
}

fn invalid_configuration() -> Anomaly {
    Anomaly::incorrect(
        "rtdb/classic-candidate-ingress-config",
        "Invalid private Classic candidate ingress configuration",
    )
}

fn denied() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Forbidden,
        "rtdb/classic-candidate-ingress",
        "The Classic candidate authorization does not cover this operation",
    )
}

fn unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/classic-candidate-ingress",
        "The enrolled Classic candidate ingress is unavailable",
    )
}

fn transport(_: std::io::Error) -> Anomaly {
    unavailable()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use rtdb_types::TxId;

    #[test]
    fn candidate_listener_is_opt_in_and_requires_one_complete_configuration_source() {
        assert!(matches!(Ingress::configured(None, None, None), Ok(None)));
        assert!(Ingress::configured(None, None, Some("7443".into())).is_err());
        assert!(Ingress::configured(None, Some("{}".into()), None).is_err());
        assert!(
            Ingress::configured(
                Some("classic-ingress.json".into()),
                Some("{}".into()),
                Some("7443".into())
            )
            .is_err()
        );
    }

    fn binding() -> Binding {
        let destination = Uuid::from_bytes([4; 16]);
        Binding {
            begin_sha256: [1; 32],
            request_sha256: [2; 32],
            catalog: Uuid::from_bytes([3; 16]),
            name: "tenant".into(),
            destination,
            capture: Uuid::from_bytes([5; 16]),
            scratch: Uuid::from_bytes([6; 16]),
            terminal: TransactionKey {
                tx_id: TxId(7),
                system_time: Timestamp(200),
            },
            terminal_sha256: [8; 32],
            artifact_sha256: [9; 32],
            projection_sha256: [10; 32],
            checkpoint_sha256: [11; 32],
            checkpoint_bytes: 1024,
            basis: Basis::new(*destination.as_bytes(), 8),
        }
    }

    fn sign(grant: Grant, approval: &Ed25519KeyPair, source: &Ed25519KeyPair) -> Request {
        let signature = approval
            .sign(&grant.signing_digest().unwrap())
            .as_ref()
            .to_vec();
        let authorization = Authorization { grant, signature };
        let source_signature = source
            .sign(&authorization.source_digest().unwrap())
            .as_ref()
            .to_vec();
        Request {
            authorization,
            source_signature,
        }
    }

    #[test]
    fn candidate_approval_has_a_stable_wire_transcript() {
        let grant = Grant {
            binding: binding(),
            operation: Operation::Install,
            expires_at_micros: 5_000_000,
        };
        assert_eq!(
            grant.binding.identity().unwrap(),
            [
                197, 22, 137, 196, 90, 253, 199, 236, 133, 168, 213, 172, 98, 106, 222, 157, 159,
                241, 249, 110, 216, 165, 202, 47, 83, 165, 85, 159, 24, 118, 102, 199
            ]
        );
        assert_eq!(
            grant.signing_digest().unwrap(),
            [
                117, 118, 10, 9, 116, 72, 96, 252, 14, 18, 254, 95, 2, 192, 238, 208, 83, 228, 76,
                123, 145, 254, 225, 121, 226, 200, 37, 246, 164, 187, 252, 94
            ]
        );
    }

    #[test]
    fn held_ready_approval_binds_placement_application_cut_install_and_operation() {
        let binding = binding();
        let ready = Box::new(ReadySelection {
            endpoint: "https://managed.internal".into(),
            placement: HeldAssignment {
                epoch: 12,
                assignment: [30; 16],
                holder: [31; 16],
            },
            classic_cut: binding.terminal,
            native_basis: binding.basis,
            reserved_at: Basis::new(*binding.catalog.as_bytes(), 1),
            installation: ClassicInstallation {
                basis: binding.basis,
                system_time: binding.terminal.system_time,
                checkpoint_lsn: 2,
                checkpoint_writer_epoch: 1,
                key_generation: 1,
                checkpoint_sha256: binding.checkpoint_sha256,
                manifest: "manifest".into(),
                manifest_ciphertext_sha256: [32; 32],
            },
        });
        let approval = Ed25519KeyPair::from_seed_unchecked(&[50; 32]).unwrap();
        let source = Ed25519KeyPair::from_seed_unchecked(&[51; 32]).unwrap();
        let mut grant = Grant {
            binding: binding.clone(),
            operation: Operation::HoldReady {
                ready: ready.clone(),
            },
            expires_at_micros: 5_000_000,
        };
        assert_eq!(
            grant.signing_digest().unwrap(),
            [
                7, 236, 188, 219, 84, 117, 96, 135, 174, 17, 138, 232, 223, 174, 254, 195, 218, 5,
                169, 201, 22, 172, 154, 191, 221, 240, 122, 17, 237, 84, 15, 241
            ]
        );
        let check = |request: &Request| {
            authenticate_request(
                binding.catalog,
                approval.public_key().as_ref().try_into().unwrap(),
                source.public_key().as_ref().try_into().unwrap(),
                request,
                Timestamp(1_000_000),
            )
        };
        assert!(check(&sign(grant.clone(), &approval, &source)).is_ok());
        let changes: [fn(&mut ReadySelection); 6] = [
            |ready| ready.endpoint = "https://other.internal".into(),
            |ready| ready.placement.epoch += 1,
            |ready| ready.placement.assignment[0] ^= 1,
            |ready| ready.placement.holder[0] ^= 1,
            |ready| ready.classic_cut.system_time.0 += 1,
            |ready| ready.installation.manifest_ciphertext_sha256[0] ^= 1,
        ];
        for change in changes {
            let mut request = sign(grant.clone(), &approval, &source);
            let Operation::HoldReady { ready } = &mut request.authorization.grant.operation else {
                unreachable!()
            };
            change(ready);
            assert!(check(&request).is_err());
        }
        let mut switched = sign(grant.clone(), &approval, &source);
        switched.authorization.grant.operation = Operation::HeldReadyStatus {
            ready: ready.clone(),
        };
        assert!(check(&switched).is_err());
        grant.operation = Operation::HeldReadyStatus { ready };
        assert_eq!(
            grant.signing_digest().unwrap(),
            [
                227, 38, 150, 195, 231, 172, 109, 47, 76, 134, 145, 108, 74, 192, 65, 136, 31, 255,
                171, 252, 105, 217, 80, 82, 116, 134, 80, 162, 162, 128, 51, 193
            ]
        );
        assert!(check(&sign(grant, &approval, &source)).is_ok());
    }

    #[test]
    fn candidate_approval_binds_both_authorities_and_all_install_coordinates() {
        let binding = binding();
        let approval = Ed25519KeyPair::from_seed_unchecked(&[50; 32]).unwrap();
        let source = Ed25519KeyPair::from_seed_unchecked(&[51; 32]).unwrap();
        let grant = Grant {
            binding: binding.clone(),
            operation: Operation::Install,
            expires_at_micros: 5_000_000,
        };
        let check = |request: &Request| {
            authenticate_request(
                binding.catalog,
                approval.public_key().as_ref().try_into().unwrap(),
                source.public_key().as_ref().try_into().unwrap(),
                request,
                Timestamp(1_000_000),
            )
        };
        let mut request = sign(grant.clone(), &approval, &source);
        assert!(check(&request).is_ok());
        request.source_signature.clear();
        assert!(check(&request).is_err());
        let wrong_owner = sign(grant.clone(), &source, &approval);
        assert!(check(&wrong_owner).is_err());
        let changes: [fn(&mut Binding); 7] = [
            |binding: &mut Binding| binding.begin_sha256[0] ^= 1,
            |binding: &mut Binding| binding.request_sha256[0] ^= 1,
            |binding: &mut Binding| binding.destination = Uuid::from_bytes([17; 16]),
            |binding: &mut Binding| binding.terminal_sha256[0] ^= 1,
            |binding: &mut Binding| binding.artifact_sha256[0] ^= 1,
            |binding: &mut Binding| binding.projection_sha256[0] ^= 1,
            |binding: &mut Binding| binding.checkpoint_sha256[0] ^= 1,
        ];
        for change in changes {
            let mut substituted = sign(grant.clone(), &approval, &source);
            change(&mut substituted.authorization.grant.binding);
            assert!(check(&substituted).is_err());
        }
        let status = Grant {
            binding: binding.clone(),
            operation: Operation::Status { classic_cut: None },
            expires_at_micros: 6_000_000,
        };
        let mut replaced = sign(grant, &approval, &source);
        let granted_status = sign(status, &approval, &source);
        assert_eq!(
            replaced.authorization.grant.binding.identity().unwrap(),
            granted_status
                .authorization
                .grant
                .binding
                .identity()
                .unwrap()
        );
        replaced.authorization = granted_status.authorization.clone();
        assert!(check(&replaced).is_err());
        assert!(check(&granted_status).is_ok());

        let mut expired = granted_status.authorization.grant.clone();
        expired.expires_at_micros = 1_000_000;
        assert!(check(&sign(expired, &approval, &source)).is_err());

        let mut later_cut = granted_status.authorization.grant.clone();
        later_cut.operation = Operation::Status {
            classic_cut: Some(TransactionKey {
                tx_id: TxId(8),
                system_time: Timestamp(201),
            }),
        };
        assert!(check(&sign(later_cut, &approval, &source)).is_err());
    }
}
