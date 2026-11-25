use std::{collections::BTreeSet, convert::Infallible, net::SocketAddr, path::PathBuf};

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full, Limited};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use rtdb_node::Node;
use rtdb_tx::transact::{Outcome, native_dependent_disposition_key};
use rtdb_types::TableRef;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::JoinSet,
};
use tokio_rustls::{TlsAcceptor, rustls};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    client::{json_content_type, read_chain, read_key, read_roots},
    inherited::{InheritedProfile, InheritedReadRequest, InheritedReleaseRequest},
    *,
};
use crate::multi::{DatabaseOpen, open_database, retryable};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Profile {
    port: u16,
    certificate_file: PathBuf,
    private_key_file: PathBuf,
    client_ca_file: PathBuf,
    #[serde(default)]
    operators: Vec<OperatorProfile>,
    #[serde(default)]
    metadata: Vec<MetadataProfile>,
    #[serde(default)]
    inherited: Vec<InheritedProfile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OperatorProfile {
    client_certificate_sha256: [u8; 32],
    source_incarnation: Uuid,
    destination_incarnation: Uuid,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataProfile {
    client_certificate_sha256: [u8; 32],
    source_incarnation: Uuid,
    destination_incarnation: Uuid,
    destination: String,
    relation: TableRef,
    body_column: String,
}

pub(in crate::multi) struct Ingress {
    port: u16,
    tls: TlsAcceptor,
    operators: Vec<OperatorProfile>,
    metadata: Vec<MetadataProfile>,
    inherited: Vec<InheritedProfile>,
    settlements: Arc<Semaphore>,
}

impl Ingress {
    pub(super) fn from_profile(profile: Profile) -> Result<Self> {
        if profile.port == 0
            || profile.port == 3000
            || profile.port == crate::multi::peer::PORT
            || profile.operators.is_empty()
                && profile.metadata.is_empty()
                && profile.inherited.is_empty()
        {
            return Err(invalid());
        }

        let mut operators = BTreeSet::new();
        for operator in &profile.operators {
            if !valid_binding(
                operator.client_certificate_sha256,
                operator.source_incarnation,
                operator.destination_incarnation,
            ) || !operators.insert((
                operator.client_certificate_sha256,
                operator.destination_incarnation,
            )) {
                return Err(invalid());
            }
        }
        let mut metadata = BTreeSet::new();
        for reader in &profile.metadata {
            if !valid_binding(
                reader.client_certificate_sha256,
                reader.source_incarnation,
                reader.destination_incarnation,
            ) || operators
                .iter()
                .any(|(cert, _)| *cert == reader.client_certificate_sha256)
                || !metadata.insert((
                    reader.client_certificate_sha256,
                    reader.destination_incarnation,
                ))
                || reader.destination.is_empty()
                || reader.relation.schema.is_empty()
                || reader.relation.table.is_empty()
                || reader.body_column.is_empty()
                || reader.body_column == "_id"
            {
                return Err(invalid());
            }
        }
        let mut inherited = BTreeSet::new();
        for reader in &profile.inherited {
            let certificate = reader.client_certificate_sha256;
            if !valid_binding(
                certificate,
                reader.source_incarnation,
                reader.destination_incarnation,
            ) || operators
                .iter()
                .chain(&metadata)
                .any(|(cert, _)| *cert == certificate)
                || !inherited.insert((certificate, reader.destination_incarnation))
                || reader.destination.is_empty()
                || reader.owner == [0; 32]
                || reader.application_scope == [0; 32]
            {
                return Err(invalid());
            }
        }
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let clients = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(read_roots(&profile.client_ca_file)?),
            Arc::clone(&provider),
        )
        .build()
        .map_err(|_| invalid())?;
        let mut tls = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|_| invalid())?
            .with_client_cert_verifier(clients)
            .with_single_cert(
                read_chain(&profile.certificate_file)?,
                read_key(&profile.private_key_file)?,
            )
            .map_err(|_| invalid())?;
        tls.alpn_protocols = vec![b"http/1.1".to_vec()];
        tls.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
        tls.send_tls13_tickets = 0;
        Ok(Self {
            port: profile.port,
            tls: TlsAcceptor::from(Arc::new(tls)),
            operators: profile.operators,
            metadata: profile.metadata,
            inherited: profile.inherited,
            settlements: Arc::new(Semaphore::new(1)),
        })
    }

    pub(super) fn port(&self) -> u16 {
        self.port
    }

    pub(in crate::multi) async fn serve(
        self: &Arc<Self>,
        runtime: &MultiRuntime,
        node: &Node,
        stop: &CancellationToken,
    ) -> Result<()> {
        let listener = TcpListener::bind(SocketAddr::new(runtime.backends.node, self.port))
            .await
            .map_err(|_| invalid())?;
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                _ = connections.join_next(), if !connections.is_empty() => (),
                accepted = listener.accept(), if connections.len() < 8 => {
                    let (socket, _) = accepted.map_err(|_| pending())?;
                    let Ok(memory) = runtime.memory.reserve(6 * MAX_BYTES) else { continue; };
                    let runtime = runtime.clone();
                    let node = node.clone();
                    let stop = stop.child_token();
                    let ingress = Arc::clone(self);
                    connections.spawn(async move {
                        let _memory = memory;
                        let _ = tokio::time::timeout(DEADLINE, stop.run_until_cancelled(ingress.exchange(runtime, node, socket, &stop))).await;
                    });
                }
            }
        }
        while connections.join_next().await.is_some() {}
        Ok(())
    }

    async fn exchange(
        self: Arc<Self>,
        runtime: MultiRuntime,
        node: Node,
        socket: TcpStream,
        stop: &CancellationToken,
    ) {
        let Ok(socket) = self.tls.accept(socket).await else {
            return;
        };
        let Some(leaf) = socket
            .get_ref()
            .1
            .peer_certificates()
            .and_then(|chain| chain.first())
        else {
            return;
        };
        let certificate = digest(leaf.as_ref());
        if !self
            .operators
            .iter()
            .any(|operator| operator.client_certificate_sha256 == certificate)
            && !self
                .metadata
                .iter()
                .any(|reader| reader.client_certificate_sha256 == certificate)
            && !self
                .inherited
                .iter()
                .any(|reader| reader.client_certificate_sha256 == certificate)
        {
            return;
        }
        let mut http = http1::Builder::new();
        http.timer(TokioTimer::new())
            .header_read_timeout(DEADLINE)
            .max_headers(32)
            .max_buf_size(16 * 1024)
            .keep_alive(false);
        let _ = http
            .serve_connection(
                TokioIo::new(socket),
                service_fn(move |request| {
                    let ingress = Arc::clone(&self);
                    let runtime = runtime.clone();
                    let node = node.clone();
                    let stop = stop.clone();
                    async move {
                        Ok::<_, Infallible>(
                            ingress
                                .respond(&runtime, &node, certificate, request, &stop)
                                .await,
                        )
                    }
                }),
            )
            .await;
    }

    async fn respond(
        &self,
        runtime: &MultiRuntime,
        node: &Node,
        certificate: [u8; 32],
        request: Request<Incoming>,
        stop: &CancellationToken,
    ) -> Response<Full<Bytes>> {
        let (parts, body) = request.into_parts();
        if parts.method != Method::POST || parts.uri.query().is_some() {
            return empty(StatusCode::NOT_FOUND);
        }
        let path = parts.uri.path();
        let role = match path {
            METADATA => self
                .metadata
                .iter()
                .any(|reader| reader.client_certificate_sha256 == certificate),
            SETTLE | STATUS => self
                .operators
                .iter()
                .any(|operator| operator.client_certificate_sha256 == certificate),
            ENROLLMENT_SOURCE | INHERITED_READ | INHERITED_RELEASE => self
                .inherited
                .iter()
                .any(|reader| reader.client_certificate_sha256 == certificate),
            _ => return empty(StatusCode::NOT_FOUND),
        };
        if !role {
            return empty(StatusCode::FORBIDDEN);
        }
        if !json_content_type(&parts.headers) {
            return empty(StatusCode::UNSUPPORTED_MEDIA_TYPE);
        }
        let Ok(body) = Limited::new(body, MAX_BYTES).collect().await else {
            return empty(StatusCode::PAYLOAD_TOO_LARGE);
        };
        let body = body.to_bytes();
        let wire = if path == METADATA {
            let Ok(request) = serde_json::from_slice::<MetadataRequest>(&body) else {
                return empty(StatusCode::BAD_REQUEST);
            };
            self.metadata(runtime, certificate, request)
                .await
                .and_then(|result| reply(&body, result))
        } else if matches!(path, SETTLE | STATUS) {
            let Ok(request) = serde_json::from_slice::<SettlementRequest>(&body) else {
                return empty(StatusCode::BAD_REQUEST);
            };
            self.settlement(runtime, certificate, path == SETTLE, request)
                .await
                .and_then(|result| reply(&body, result))
        } else {
            self.inherited(runtime, node, certificate, path, &body, stop)
                .await
        };
        match wire {
            Ok(wire) => Response::builder()
                .header(http::header::CONTENT_TYPE, "application/json")
                .body(Full::new(Bytes::from(wire)))
                .expect("fixed private JSON response"),
            Err(error) if error.category == AnomalyCategory::Unavailable => {
                empty(StatusCode::SERVICE_UNAVAILABLE)
            }
            Err(_) => empty(StatusCode::CONFLICT),
        }
    }

    async fn inherited(
        &self,
        runtime: &MultiRuntime,
        node: &Node,
        certificate: [u8; 32],
        path: &str,
        body: &[u8],
        stop: &CancellationToken,
    ) -> Result<Vec<u8>> {
        let profile = |version, scope| {
            if version != VERSION {
                return Err(refused());
            }
            self.inherited
                .iter()
                .find(|profile| {
                    profile.client_certificate_sha256 == certificate
                        && matches_scope(
                            profile.source_incarnation,
                            profile.destination_incarnation,
                            scope,
                        )
                })
                .ok_or_else(refused)
        };
        let result = match path {
            ENROLLMENT_SOURCE => {
                let request =
                    serde_json::from_slice::<MetadataRequest>(body).map_err(|_| refused())?;
                profile(request.version, request.scope)?
                    .source_generation(runtime, request.scope)
                    .await?
            }
            INHERITED_READ => {
                let request =
                    serde_json::from_slice::<InheritedReadRequest>(body).map_err(|_| refused())?;
                profile(request.version, request.read.scope)?
                    .read(runtime, node, request.read, stop)
                    .await?
            }
            INHERITED_RELEASE => {
                let request = serde_json::from_slice::<InheritedReleaseRequest>(body)
                    .map_err(|_| refused())?;
                profile(request.version, request.release.proof.read.scope)?
                    .release(runtime, node, request.release, stop)
                    .await?
            }
            _ => return Err(refused()),
        };
        reply(body, result)
    }

    async fn metadata(
        &self,
        runtime: &MultiRuntime,
        certificate: [u8; 32],
        request: MetadataRequest,
    ) -> Result<MetadataResult> {
        let scope = request.scope;
        if request.version != VERSION {
            return Err(refused());
        }
        let profile = self
            .metadata
            .iter()
            .find(|profile| {
                profile.client_certificate_sha256 == certificate
                    && matches_scope(
                        profile.source_incarnation,
                        profile.destination_incarnation,
                        scope,
                    )
            })
            .ok_or_else(refused)?;
        let selected = runtime
            .managed_entry(&profile.destination, profile.destination_incarnation)
            .await?;
        if selected.managed_transfer()?.reservation != scope.reservation {
            return Err(refused());
        }
        let mut database = match open_database(
            &runtime.backends,
            &runtime.kms,
            profile.destination_incarnation,
            DatabaseOpen::Prepared,
        )
        .await
        {
            Ok(database) => database,
            Err(error) if retryable(&error) => return Ok(MetadataResult::Pending),
            Err(error) => return Err(error),
        };
        if database
            .restored_checkpoint_basis(scope.reservation.source.manifest_sha256)
            .await?
            .is_none()
        {
            return Ok(MetadataResult::Pending);
        }
        let (basis, view, generation) = database
            .require_managed_checkpoint_restore(scope.reservation)
            .await?;
        if basis != scope.destination {
            return Err(refused());
        }
        let metadata = view.native_dependent_checkpoint_metadata(
            scope,
            &profile.relation,
            &profile.body_column,
            MAX_BYTES,
        )?;
        if metadata.rows.iter().any(|row| {
            serde_json::from_slice::<&serde_json::value::RawValue>(&row.body)
                .map_or(true, |raw| !raw.get().starts_with('{'))
        }) {
            return Err(refused());
        }
        database.check_generation(generation).await?;
        let selected = runtime
            .managed_entry(&profile.destination, profile.destination_incarnation)
            .await?;
        if selected.managed_transfer()?.reservation != scope.reservation {
            return Err(refused());
        }
        Ok(MetadataResult::Ready { metadata })
    }

    async fn settlement(
        &self,
        runtime: &MultiRuntime,
        certificate: [u8; 32],
        settle: bool,
        request: SettlementRequest,
    ) -> Result<SettlementResult> {
        let Settlement {
            destination,
            scope,
            fence,
        } = request.settlement;
        if request.version != VERSION
            || !self.operators.iter().any(|profile| {
                profile.client_certificate_sha256 == certificate
                    && matches_scope(
                        profile.source_incarnation,
                        profile.destination_incarnation,
                        scope,
                    )
            })
        {
            return Err(refused());
        }
        let (source, expected) = runtime
            .native_dependent_source(&destination, scope.reservation, scope.group)
            .await?;
        if scope != expected {
            return Err(refused());
        }
        let key = native_dependent_disposition_key(scope);
        let terminal = if settle {
            let permit = Arc::clone(&self.settlements)
                .try_acquire_owned()
                .map_err(|_| pending())?;
            let owner = runtime.clone();
            let result = runtime
                .managed_operations
                .spawn(async move {
                    let _permit = permit;
                    owner
                        .settle_native_dependent_scope(
                            &source,
                            scope,
                            &fence,
                            &CancellationToken::new(),
                        )
                        .await
                })
                .await
                .map_err(|_| pending())?;
            match result {
                Ok(receipt) => Some(receipt),
                Err(error) if error.category == AnomalyCategory::Unavailable => None,
                Err(error) => return Err(error),
            }
        } else {
            source
                .native_dependent_custody_status(scope, &fence)
                .await?
        };
        match terminal {
            Some(receipt)
                if receipt.outcome == Outcome::Then && receipt.key.as_slice() == key.as_slice() =>
            {
                Ok(SettlementResult::Settled {
                    key,
                    basis: receipt.basis,
                })
            }
            Some(_) => Err(refused()),
            None => Ok(SettlementResult::Pending { key }),
        }
    }
}

fn valid_binding(certificate: [u8; 32], source: Uuid, destination: Uuid) -> bool {
    certificate != [0; 32] && !source.is_nil() && !destination.is_nil() && source != destination
}

fn matches_scope(source: Uuid, destination: Uuid, scope: NativeDependentCustodyScope) -> bool {
    *source.as_bytes() == scope.reservation.source.basis.incarnation()
        && *destination.as_bytes() == scope.reservation.target.incarnation
}

fn reply(request: &[u8], result: impl Serialize) -> Result<Vec<u8>> {
    encode(&Reply {
        version: VERSION,
        request_sha256: digest(request),
        result,
    })
}

fn empty(status: StatusCode) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::new()))
        .expect("fixed private error response")
}
