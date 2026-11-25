use std::{
    fs::File,
    io::Read,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use bytes::{Buf, BufMut, Bytes};
use rtdb_db::discover_nodes;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::JoinSet,
};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{
        self,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    },
};
use tokio_util::sync::CancellationToken;
use tonic::{
    Status,
    client::Grpc,
    codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
    transport::{Certificate, ClientTlsConfig, Endpoint, Identity},
};

use super::{Decision, HeldReady, Reply, Transition, Witness, denied, unavailable};
use crate::multi::{
    MultiRuntime,
    classic_ingress::{MAX_CONTROL_BYTES, read_control, write_control},
};

const DEADLINE: Duration = Duration::from_secs(10);
const VERIFY: &str = "/bijection.native.v1.Administration/VerifyHostedHistoryOpening";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    port: u16,
    certificate_file: PathBuf,
    private_key_file: PathBuf,
    client_ca_file: PathBuf,
    operator_certificate_sha256: Option<[u8; 32]>,
    reconciler_certificate_sha256: [u8; 32],
    verifier: Option<VerifierConfiguration>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VerifierConfiguration {
    endpoint: String,
    history_id: [u8; 16],
    target: u64,
    ca_file: PathBuf,
    client_certificate_file: PathBuf,
    client_private_key_file: PathBuf,
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    OpenClassicMigrationDestination {
        decision: Decision,
        held_ready: HeldReady,
    },
    ExcludeClassicMigrationOpening {
        decision: Decision,
        held_ready: HeldReady,
    },
    ClassicMigrationOpeningStatus {
        decision: Decision,
        held_ready: HeldReady,
    },
}

#[derive(Serialize)]
struct VerifyRequest<'a> {
    format: u32,
    history_id: [u8; 16],
    target: u64,
    expected: &'a Decision,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VerifyReply {
    format: u32,
    history_id: [u8; 16],
    source: u64,
    result: VerifyResult,
}

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum VerifyResult {
    HostedHistoryOpeningCurrent { opening: Box<Witness> },
    Refused,
    Unknown,
}

struct Verifier {
    channel: tonic::transport::Channel,
    history_id: [u8; 16],
    target: u64,
}

impl Verifier {
    fn from_configuration(config: VerifierConfiguration) -> Result<Self> {
        if config.history_id == [0; 16]
            || config.target == 0
            || !config.endpoint.starts_with("https://")
        {
            return Err(invalid());
        }
        let ca = Certificate::from_pem(read_file(&config.ca_file)?);
        let certificate = read_file(&config.client_certificate_file)?;
        let key = zeroize::Zeroizing::new(read_file(&config.client_private_key_file)?);
        let identity = Identity::from_pem(certificate, key.as_slice());
        let endpoint = Endpoint::from_shared(config.endpoint)
            .map_err(|_| invalid())?
            .tls_config(ClientTlsConfig::new().ca_certificate(ca).identity(identity))
            .map_err(|_| invalid())?
            .connect_timeout(DEADLINE)
            .timeout(DEADLINE);
        Ok(Self {
            channel: endpoint.connect_lazy(),
            history_id: config.history_id,
            target: config.target,
        })
    }

    async fn verify(&self, witness: &Witness) -> Result<()> {
        if witness.decision.applied_at.history_id != self.history_id {
            return Err(denied());
        }
        let request = VerifyRequest {
            format: 1,
            history_id: self.history_id,
            target: self.target,
            expected: &witness.decision,
        };
        let request = Bytes::from(serde_json::to_vec(&request).map_err(|_| denied())?);
        let mut client = Grpc::new(self.channel.clone())
            .max_encoding_message_size(MAX_CONTROL_BYTES)
            .max_decoding_message_size(MAX_CONTROL_BYTES);
        let reply = tokio::time::timeout(DEADLINE, async {
            client.ready().await.map_err(|_| unavailable())?;
            client
                .unary(
                    tonic::Request::new(request),
                    http::uri::PathAndQuery::from_static(VERIFY),
                    JsonWire,
                )
                .await
                .map(tonic::Response::into_inner)
                .map_err(|_| unavailable())
        })
        .await
        .map_err(|_| unavailable())??;
        if reply.format != 1 || reply.history_id != self.history_id || reply.source != self.target {
            return Err(denied());
        }
        match reply.result {
            VerifyResult::HostedHistoryOpeningCurrent { opening }
                if opening.as_ref() == witness =>
            {
                Ok(())
            }
            VerifyResult::Unknown => Err(unavailable()),
            _ => Err(denied()),
        }
    }
}

struct JsonWire;

impl Codec for JsonWire {
    type Encode = Bytes;
    type Decode = VerifyReply;
    type Encoder = Self;
    type Decoder = Self;

    fn encoder(&mut self) -> Self {
        Self
    }
    fn decoder(&mut self) -> Self {
        Self
    }
}

impl Encoder for JsonWire {
    type Item = Bytes;
    type Error = Status;

    fn encode(
        &mut self,
        item: Bytes,
        buffer: &mut EncodeBuf<'_>,
    ) -> std::result::Result<(), Status> {
        buffer.put(item);
        Ok(())
    }
}

impl Decoder for JsonWire {
    type Item = VerifyReply;
    type Error = Status;

    fn decode(
        &mut self,
        buffer: &mut DecodeBuf<'_>,
    ) -> std::result::Result<Option<VerifyReply>, Status> {
        let bytes = buffer.copy_to_bytes(buffer.remaining());
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|_| Status::data_loss("Invalid Bijection opening reply"))
    }
}

pub(in crate::multi) struct Ingress {
    port: u16,
    tls: TlsAcceptor,
    operator: Option<[u8; 32]>,
    reconciler: [u8; 32],
    verifier: Option<Verifier>,
    mutations: Semaphore,
}

impl Ingress {
    pub(in crate::multi) fn installed() -> Result<Option<Arc<Self>>> {
        let Some(path) = std::env::var_os("RTDB_CLASSIC_OPENING_INGRESS_FILE") else {
            return Ok(None);
        };
        let config: Configuration =
            serde_json::from_slice(&read_file(Path::new(&path))?).map_err(|_| invalid())?;
        if config.port == 0
            || config.port == crate::multi::peer::PORT
            || config.operator_certificate_sha256 == Some([0; 32])
            || config.reconciler_certificate_sha256 == [0; 32]
            || config.operator_certificate_sha256 == Some(config.reconciler_certificate_sha256)
            || config.operator_certificate_sha256.is_some() != config.verifier.is_some()
        {
            return Err(invalid());
        }
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let mut roots = rustls::RootCertStore::empty();
        let ca = read_file(&config.client_ca_file)?;
        for cert in CertificateDer::pem_slice_iter(&ca) {
            roots
                .add(cert.map_err(|_| invalid())?)
                .map_err(|_| invalid())?;
        }
        let clients = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::clone(&provider),
        )
        .build()
        .map_err(|_| invalid())?;
        let cert = read_file(&config.certificate_file)?;
        let chain = CertificateDer::pem_slice_iter(&cert)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| invalid())?;
        let key = zeroize::Zeroizing::new(read_file(&config.private_key_file)?);
        let key = PrivateKeyDer::from_pem_slice(&key).map_err(|_| invalid())?;
        let tls = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|_| invalid())?
            .with_client_cert_verifier(clients)
            .with_single_cert(chain, key)
            .map_err(|_| invalid())?;
        let verifier = config
            .verifier
            .map(Verifier::from_configuration)
            .transpose()?;
        Ok(Some(Arc::new(Self {
            port: config.port,
            tls: TlsAcceptor::from(Arc::new(tls)),
            operator: config.operator_certificate_sha256,
            reconciler: config.reconciler_certificate_sha256,
            verifier,
            mutations: Semaphore::new(1),
        })))
    }

    pub(in crate::multi) async fn serve(
        self: &Arc<Self>,
        runtime: &MultiRuntime,
        stop: &CancellationToken,
    ) -> Result<()> {
        let listener = TcpListener::bind(SocketAddr::new(runtime.backends.node, self.port))
            .await
            .map_err(|_| unavailable())?;
        let mut running = JoinSet::new();
        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                _ = running.join_next(), if !running.is_empty() => (),
                connection = listener.accept(), if running.len() < 8 => {
                    let (socket, _) = connection.map_err(|_| unavailable())?;
                    let ingress = Arc::clone(self);
                    let runtime = runtime.clone();
                    running.spawn(async move { ingress.exchange(&runtime, socket).await });
                }
            }
        }
        drop(listener);
        while running.join_next().await.is_some() {}
        Ok(())
    }

    async fn exchange(&self, runtime: &MultiRuntime, socket: TcpStream) {
        let Ok(Ok(mut socket)) = tokio::time::timeout(DEADLINE, self.tls.accept(socket)).await
        else {
            return;
        };
        let Some(cert) = socket
            .get_ref()
            .1
            .peer_certificates()
            .and_then(|certs| certs.first())
        else {
            return;
        };
        let certificate = <[u8; 32]>::from(Sha256::digest(cert.as_ref()));
        if Some(certificate) != self.operator && certificate != self.reconciler {
            return;
        }
        let Ok(Ok(request)) = tokio::time::timeout(DEADLINE, read_control(&mut socket)).await
        else {
            return;
        };
        let reply = match self.execute(runtime, certificate, request).await {
            Ok(reply) => reply,
            Err(error)
                if matches!(
                    error.category,
                    AnomalyCategory::Forbidden
                        | AnomalyCategory::Conflict
                        | AnomalyCategory::Incorrect
                ) =>
            {
                Reply::Refused
            }
            Err(_) => Reply::Unresolved,
        };
        let _ = tokio::time::timeout(DEADLINE, write_control(&mut socket, &reply)).await;
    }

    async fn execute(
        &self,
        runtime: &MultiRuntime,
        certificate: [u8; 32],
        request: Request,
    ) -> Result<Reply> {
        let (witness, mutation) = match request {
            Request::OpenClassicMigrationDestination {
                decision,
                held_ready,
            } => (
                Witness {
                    decision,
                    held_ready,
                },
                Some(false),
            ),
            Request::ExcludeClassicMigrationOpening {
                decision,
                held_ready,
            } => (
                Witness {
                    decision,
                    held_ready,
                },
                Some(true),
            ),
            Request::ClassicMigrationOpeningStatus {
                decision,
                held_ready,
            } => (
                Witness {
                    decision,
                    held_ready,
                },
                None,
            ),
        };
        if !witness.valid(runtime.catalog.incarnation())
            || (mutation.is_some() && Some(certificate) != self.operator)
        {
            return Err(denied());
        }
        let peer = runtime.peer.as_ref().ok_or_else(denied)?;
        let live = discover_nodes(runtime.backends.peers.as_deref().ok_or_else(denied)?).await?;
        if !live.contains(&runtime.backends.node) {
            return Err(unavailable());
        }
        let Some(exclude) = mutation else {
            return Ok(runtime
                .catalog
                .classic_opening(&witness, Transition::Observe)
                .await?
                .map_or(Reply::Unresolved, |(record, at)| record.reply(at)));
        };
        let _mutation = self.mutations.try_acquire().map_err(|_| unavailable())?;
        self.verifier
            .as_ref()
            .ok_or_else(denied)?
            .verify(&witness)
            .await?;
        peer.observe(&live);
        if exclude {
            let (record, at) = runtime
                .catalog
                .classic_opening(&witness, Transition::Exclude)
                .await?
                .ok_or_else(denied)?;
            Ok(record.reply(at))
        } else {
            runtime.open_classic_workload(&witness, &live).await
        }
    }
}

fn read_file(path: &Path) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)
        .and_then(|file| file.take(65537).read_to_end(&mut bytes))
        .map_err(|_| invalid())?;
    if bytes.len() > 65536 {
        return Err(invalid());
    }
    Ok(bytes)
}

fn invalid() -> Anomaly {
    Anomaly::incorrect(
        "rtdb/classic-opening-ingress-config",
        "Invalid private Classic opening ingress configuration",
    )
}
