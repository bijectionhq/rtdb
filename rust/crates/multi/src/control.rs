//! The control protocol between the cell and a node's agent uses newline-delimited JSON and can hand off to Pgwire or an Arrow IPC body.
//! In a cell with a secret, every request carries a grant signed with it, and a node refuses one that does not.

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_query::FragmentBytes;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, watch};
use tokio_util::sync::CancellationToken;

pub(crate) const NODE_UNDER_PRESSURE: &str = "rtdb.multi/node-under-pressure";
pub(crate) const NOT_CLAIMABLE: &str = "rtdb.multi/not-claimable";
pub(crate) const CLAIM_CONTENDED: &str = "rtdb.multi/claim-contended";
pub(crate) const ACTIVATION_RATE_LIMITED: &str = "rtdb.multi/activation-rate-limited";

pub(crate) const WRITER_PGWIRE_UNREACHABLE: &str = "rtdb.multi/writer-pgwire-unreachable";
pub(crate) const WRITER_CONTROL_UNREACHABLE: &str = "rtdb.multi/writer-control-unreachable";
const CONTROL_IO: &str = "rtdb.multi/control-io";
const CONTROL_TIMEOUT: &str = "rtdb.multi/control-timeout";

pub(crate) fn unreachable_control(code: &str) -> bool { matches!(code, CONTROL_IO | CONTROL_TIMEOUT) }

pub(crate) fn retryable_activation(code: &str) -> bool { matches!(code, NODE_UNDER_PRESSURE | CLAIM_CONTENDED | ACTIVATION_RATE_LIMITED | WRITER_PGWIRE_UNREACHABLE | WRITER_CONTROL_UNREACHABLE) }

pub(crate) async fn retry_activation<T, F: Future<Output = Result<T>>>(window: Duration, mut attempt: impl FnMut() -> F) -> Result<T> {
    let latest_start = tokio::time::Instant::now() + window;
    loop {
        let result = attempt().await;
        let Err(error) = &result else { return result; };
        if !retryable_activation(&error.code) { return result; }
        let delay = if matches!(error.code.as_ref(), ACTIVATION_RATE_LIMITED | WRITER_PGWIRE_UNREACHABLE | WRITER_CONTROL_UNREACHABLE) { Duration::from_millis(100) } else { Duration::from_millis(25) };
        let retry_at = tokio::time::Instant::now() + delay;
        if retry_at >= latest_start { return result; }
        tokio::time::sleep_until(retry_at).await;
        if tokio::time::Instant::now() >= latest_start { return result; }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "op")]
pub enum Request {
    /// Become the deployment's holder, or name who is.
    Activate { name: String },
    ActivateOrdered { name: String, activation: OrderedActivation },
    ActivateForMove { name: String, ticket: MoveTicket },
    WriterHandoff { name: String, command: WriterHandoffCommand },
    /// Flush, release the placement, and stop running the deployment.
    Park { name: String },
    ParkForMove { name: String, epoch: u64, ticket: MoveTicket },
    /// Answers Done while the agent is serving: the controller's liveness probe.
    Ping,
    OpenPgwire,
    FlightEndpoint,
    HoldingLoads { #[serde(default)] measure_query_cpu: bool },
    ReadLoads,
    /// A client is using the deployment through a gateway.
    Touch { name: String },
    /// Serve the deployment's reads from a log-fed replica on this node.
    Subscribe { name: String },
    /// Serve a deployment using a read index before capturing an implicit basis.
    SubscribeFresh { name: String },
    SubscribeOrderedFresh {
        name: String,
        placement: ordered_logkeeper::ordered::Placement,
        enrollment: ordered_logkeeper::ordered::Enrollment,
        epoch: u64,
    },
    /// Serve the deployment's reads as of its checkpoint, from object storage alone.
    Snapshot { name: String },
    /// Flush the deployment this node holds and wait for the block's adoption, keeping the holding.
    Flush { name: String },
    StageSqlErasure { name: String, request: crate::journal::hosted::erasure::SqlErasureRequest },
    EnrollInstalledFork { parent: String, child: String },
    /// Pin the named captured checkpoint through this writer's exact installed basis.
    PinCheckpointSuffix {
        name: String,
        deployment: uuid::Uuid,
        lease: uuid::Uuid,
        capture: uuid::Uuid,
        basis: rtdb_types::TransactionKey,
        expires_at_ms: i64,
    },
    /// Publish a compaction a worker produced, as the deployment's holder; the notification is an encoded TriesAdded.
    PublishCompaction { name: String, notification: Vec<u8>, grant: String },
    /// Read a fragment of another node's scan of the deployment, at the fragment's basis (`RefineKeyForLargeDeployments`).
    #[serde(rename = "scan_scoped_fragment")]
    ScanFragment { name: String, fragment: rtdb_query::ScanFragment, grant: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderedPredecessor {
    pub epoch: u64,
    pub enrollment: Option<ordered_logkeeper::ordered::Enrollment>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderedActivation {
    pub predecessor: OrderedPredecessor,
    pub target: ordered_logkeeper::ordered::Enrollment,
    pub takeover: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) placement: Option<ordered_logkeeper::ordered::Placement>,
    nonce: uuid::Uuid,
    proof: [u8; 32],
}

impl OrderedActivation {
    pub(crate) fn issue(
        secret: &crate::capability::CellSecret, name: &str, placement: ordered_logkeeper::ordered::Placement,
        predecessor: OrderedPredecessor, target: ordered_logkeeper::ordered::Enrollment, takeover: bool,
    ) -> Self {
        let mut activation = Self { predecessor, target, takeover, placement: Some(placement), nonce: uuid::Uuid::new_v4(), proof: [0; 32] };
        activation.proof = secret.sign_route(&activation.forward(name, placement));
        activation
    }

    pub(crate) fn authentic(
        &self, secret: &crate::capability::CellSecret, name: &str, placement: ordered_logkeeper::ordered::Placement,
    ) -> bool {
        !self.nonce.is_nil() && secret.verify_route(&self.forward(name, placement), &self.proof)
    }

    fn forward(&self, name: &str, placement: ordered_logkeeper::ordered::Placement) -> Vec<u8> {
        serde_json::to_vec(&("rtdb.multi/ordered-activation/v1", name, placement,
            self.predecessor, self.target, self.takeover, self.nonce)).expect("ordered activation serializes")
    }

    pub(crate) fn reply_binding(
        &self, placement: ordered_logkeeper::ordered::Placement, epoch: u64, address: &str,
    ) -> Vec<u8> {
        serde_json::to_vec(&("rtdb.multi/ordered-ready/v1", placement, self.target, self.nonce,
            epoch, address)).expect("ordered readiness serializes")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriterHandoffAction { Prepare, Switch, Read, Abandon }

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterHandoffCommand {
    pub(crate) plan: rtdb_directory::WriterHandoffPlan,
    pub(crate) action: WriterHandoffAction,
    nonce: uuid::Uuid,
    proof: [u8; 32],
}

impl WriterHandoffCommand {
    pub(crate) fn issue(
        secret: &crate::capability::CellSecret, name: &str,
        plan: rtdb_directory::WriterHandoffPlan, action: WriterHandoffAction,
    ) -> Self {
        let mut command = Self { plan, action, nonce: uuid::Uuid::new_v4(), proof: [0; 32] };
        command.proof = secret.sign_route(&command.binding(name, None));
        command
    }

    pub(crate) fn authentic(&self, secret: &crate::capability::CellSecret, name: &str) -> bool {
        !self.nonce.is_nil() && secret.verify_route(&self.binding(name, None), &self.proof)
    }

    pub(crate) fn binding(&self, name: &str, witness: Option<&[u8]>) -> Vec<u8> {
        serde_json::to_vec(&("rtdb.multi/writer-handoff/v1", name, &self.plan, self.action, self.nonce, witness))
            .expect("writer handoff binding serializes")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveTicket {
    pub deployment: String,
    pub request: uuid::Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome")]
pub enum Response {
    /// This member holds the deployment under `epoch`; clients reach it at `address`.
    Held { epoch: u64, address: String },
    OrderedHeld { epoch: u64, address: String, proof: [u8; 32] },
    WriterHandoff { witness: Vec<u8>, proof: [u8; 32] },
    /// Another live member holds it; clients reach that one at `address`.
    Redirect { holder: String, address: String },
    /// This member serves the deployment's reads from a replica; clients reach it at `address`.
    Subscribed { address: String },
    OrderedRead { address: String, flight: Option<String>, proof: [u8; 32] },
    FlightEndpoint { address: String },
    HoldingLoads { holdings: Vec<crate::load::HoldingLoad> },
    ReadLoads { loads: Vec<crate::read_width::ReadLoad> },
    Done,
    Flushed { checkpoint: rtdb_directory::Checkpoint },
    InstalledForkEnrolled,
    SqlErasureStaged { checkpoint: ordered_logkeeper::ordered::Checkpoint },
    CheckpointSuffixPinned { pin: rtdb_directory::CheckpointSuffixPin },
    Published { position: i64 },
    /// A fragment's rows: length-prefixed Arrow IPC streams, in base64.
    Fragment { batches: String },
    FragmentIpc { bytes: u64 },
    Refused { code: String, message: String },
}

pub(crate) fn ordered_read_binding(
    name: &str, placement: ordered_logkeeper::ordered::Placement,
    enrollment: ordered_logkeeper::ordered::Enrollment, epoch: u64, address: &str, flight: Option<&str>,
) -> Vec<u8> {
    serde_json::to_vec(&("rtdb.multi/ordered-reader/v1", name, placement, enrollment, epoch, address, flight))
        .expect("ordered reader coordinates encode")
}

/// The secret a cell's members sign their control requests with; a cell without one sends them unsigned, to nodes that take them unsigned.
#[derive(Clone, Default)]
pub struct ControlKey(pub Option<Arc<crate::capability::CellSecret>>);

impl std::fmt::Debug for ControlKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str(if self.0.is_some() { "ControlKey(signed)" } else { "ControlKey(unsigned)" }) }
}

const CONTROL_SCOPE: &str = "~control";

const FRAGMENT_LITERAL_VERSION: u8 = 2;
const LEGACY_FRAGMENT_LITERAL_VERSION: u8 = 1;
/// The longest request a node reads.
const MAXIMUM_REQUEST_BYTES: u64 = 1 << 20;
/// The longest response a caller reads: a fragment's rows, in base64, are the largest.
pub const MAXIMUM_RESPONSE_BYTES: u64 = 96 << 20;
pub(crate) const MAXIMUM_FRAGMENT_BYTES: u64 = MAXIMUM_RESPONSE_BYTES / 4 * 3 - 1024;

impl ControlKey {
    fn grant(&self) -> Option<String> { self.0.as_ref().map(|secret| secret.mint(CONTROL_SCOPE, "~cell-member", false, Duration::from_secs(30))) }
    fn admit(&self, grant: Option<&str>) -> Result<()> {
        let Some(secret) = &self.0 else { return Ok(()) };
        let grant = grant.ok_or_else(|| Anomaly::new(AnomalyCategory::Forbidden, "rtdb.multi/control-unsigned", "This cell's control requests must be signed with its secret"))?;
        use rtdb_pgwire::CapabilityVerifier;
        secret.verify(grant, CONTROL_SCOPE).map(|_| ())
    }
}

#[derive(Serialize, Deserialize)]
struct Envelope {
    /// Named apart from any field of a request, which share its JSON object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    control_grant: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fragment_ipc: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fragment_arguments: Option<FragmentArguments>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fragment_literal_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fragment_literal_version: Option<u8>,
    #[serde(flatten)]
    request: Request,
}

#[derive(Serialize, Deserialize)]
struct FragmentArguments {
    length: usize,
    predicates: Vec<(usize, rtdb_plan::Expr)>,
}

#[derive(Serialize, Deserialize)]
struct Reply {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    startup_probe_overflows: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fragment_literal_version: Option<u8>,
    #[serde(flatten)]
    response: Response,
}

impl Response {
    pub fn refused(error: &Anomaly) -> Self { Self::Refused { code: error.code.to_string(), message: error.message.clone().unwrap_or_default() } }
}

#[async_trait]
pub trait Handler: Send + Sync + 'static {
    async fn handle(&self, request: Request) -> Response;

    async fn fragment_ipc(&self, _name: &str, _fragment: &rtdb_query::ScanFragment, _grant: &str, _arguments: Option<rtdb_query::BoundParameters>)
        -> Option<Result<rtdb_query::FragmentBytes>> { None }
}

pub async fn serve(listener: TcpListener, handler: Arc<dyn Handler>, key: ControlKey, cancellation: CancellationToken) -> Result<()> {
    serve_on(listener, handler, key, cancellation, None).await
}

#[derive(Clone)]
struct PgwireEndpoint {
    address: SocketAddr,
    relay_slots: Option<Arc<Semaphore>>,
    status: watch::Receiver<rtdb_pgwire::ServerStatus>,
}

pub async fn serve_with_pgwire(listener: TcpListener, handler: Arc<dyn Handler>, key: ControlKey, cancellation: CancellationToken,
                               mut address: SocketAddr, relay_limit: Option<usize>, status: watch::Receiver<rtdb_pgwire::ServerStatus>) -> Result<()> {
    if address.ip().is_unspecified() {
        address.set_ip(if address.is_ipv4() { IpAddr::V4(Ipv4Addr::LOCALHOST) } else { IpAddr::V6(Ipv6Addr::LOCALHOST) });
    }
    let relay_slots = relay_limit.map(|limit| Arc::new(Semaphore::new(limit.min(Semaphore::MAX_PERMITS))));
    serve_on(listener, handler, key, cancellation, Some(PgwireEndpoint { address, relay_slots, status })).await
}

async fn write_message(stream: &mut TcpStream, value: &impl Serialize) -> std::io::Result<()> {
    let mut bytes = serde_json::to_vec(value).expect("a control message serializes");
    bytes.push(b'\n');
    stream.write_all(&bytes).await
}

async fn serve_on(listener: TcpListener, handler: Arc<dyn Handler>, key: ControlKey, cancellation: CancellationToken,
                  pgwire: Option<PgwireEndpoint>) -> Result<()> {
    loop {
        let (mut stream, _) = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            accepted = listener.accept() => accepted.map_err(io)?,
        };
        let (handler, key, pgwire) = (Arc::clone(&handler), key.clone(), pgwire.clone());
        let stop = cancellation.clone();
        tokio::spawn(async move {
            let request = stop.run_until_cancelled(read_negotiated_request(&mut stream, &key)).await;
            let Some(request) = request else { return; };
            let mut startup_probe_overflows = None;
            let response = match request {
                Ok((envelope, arguments, _literals)) => match envelope.request {
                        Request::OpenPgwire => {
                            if let Some(PgwireEndpoint { address, relay_slots: Some(slots), .. }) = pgwire.filter(|_| key.0.is_some()) {
                                if let Ok(_slot) = slots.try_acquire_owned() {
                                    if let Ok(Ok(mut destination)) = tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(address)).await {
                                        let _ = stream.set_nodelay(true);
                                        let _ = destination.set_nodelay(true);
                                        if write_message(&mut stream, &Response::Done).await.is_ok() {
                                            stop.run_until_cancelled(tokio::io::copy_bidirectional(&mut stream, &mut destination)).await;
                                        }
                                        return;
                                    }
                                }
                            }
                            Response::Refused { code: "rtdb.multi/pgwire-relay-unavailable".into(), message: "This control endpoint cannot open a Pgwire connection".into() }
                        }
                        request => {
                            if let Request::ScanFragment { name, fragment, grant } = &request
                                && let Some(result) = handler.fragment_ipc(name, fragment, grant, arguments).await
                            {
                                match result {
                                    Ok(batches) if envelope.fragment_ipc != Some(true) => {
                                        let response = Response::Fragment { batches: STANDARD.encode(batches.as_slice()) };
                                        let _ = write_message(&mut stream, &Reply { startup_probe_overflows: None, fragment_literal_version: None, response }).await;
                                        return;
                                    }
                                    Ok(batches) => {
                                        let bytes = batches.as_slice();
                                        let response = Reply { startup_probe_overflows: None, fragment_literal_version: None, response: Response::FragmentIpc { bytes: bytes.len() as u64 } };
                                        if write_message(&mut stream, &response).await.is_ok() {
                                            let _ = stop.run_until_cancelled(stream.write_all(bytes)).await;
                                        }
                                        return;
                                    }
                                    Err(error) => {
                                        let _ = write_message(&mut stream, &Reply { startup_probe_overflows: None, fragment_literal_version: None, response: Response::refused(&error) }).await;
                                        return;
                                    }
                                }
                            }
                            let reports_probe = matches!(request, Request::Ping | Request::Activate { .. } | Request::Subscribe { .. }
                                | Request::SubscribeFresh { .. } | Request::SubscribeOrderedFresh { .. } | Request::Snapshot { .. });
                            let response = handler.handle(request).await;
                            if reports_probe {
                                startup_probe_overflows = pgwire.as_ref().map(|endpoint| endpoint.status.borrow().startup_probe_overflows);
                            }
                            response
                        }
                    },
                Err(error) => Response::refused(&error),
            };
            let _ = write_message(&mut stream, &Reply { startup_probe_overflows, fragment_literal_version: None, response }).await;
        });
    }
}

type ReceivedRequest = (Envelope, Option<rtdb_query::BoundParameters>, Option<Arc<MemoryReservation>>);

async fn read_negotiated_request(stream: &mut TcpStream, key: &ControlKey) -> Result<ReceivedRequest> {
    let request = read_request(stream, key, false).await?;
    if request.0.request != Request::Ping || request.0.fragment_ipc != Some(true) {
        return Ok(request);
    }
    // An older agent sees a normal Ping and closes its connection. A positive reply upgrades only this socket.
    let version = if request.0.fragment_literal_version == Some(FRAGMENT_LITERAL_VERSION) {
        FRAGMENT_LITERAL_VERSION
    } else {
        LEGACY_FRAGMENT_LITERAL_VERSION
    };
    write_message(stream, &Reply { startup_probe_overflows: None,
        fragment_literal_version: Some(version), response: Response::Done }).await.map_err(io)?;
    tokio::time::timeout(Duration::from_secs(5), read_request(stream, key, true)).await
        .map_err(|_| Anomaly::incorrect("rtdb.multi/control-request", "Fragment request frame timed out"))?
}

async fn read_request(stream: &mut TcpStream, key: &ControlKey, literal_frames: bool) -> Result<ReceivedRequest> {
    let malformed = |message| Anomaly::incorrect("rtdb.multi/control-request", message);
    let mut reader = BufReader::new(stream.take(MAXIMUM_REQUEST_BYTES));
    let mut line = String::new();
    reader.read_line(&mut line).await.map_err(io)?;
    if !line.ends_with('\n') { return Err(malformed("Invalid control request frame")); }
    let mut envelope = serde_json::from_str::<Envelope>(&line)
        .map_err(|error| Anomaly::incorrect("rtdb.multi/control-request", error.to_string()))?;
    key.admit(envelope.control_grant.as_deref())?;
    if let Some(length) = envelope.fragment_literal_bytes.take() {
        if !literal_frames || envelope.fragment_ipc != Some(true) || envelope.fragment_arguments.is_some()
            || length as u64 > MAXIMUM_REQUEST_BYTES.saturating_sub(line.len() as u64)
        {
            return Err(malformed("Fragment arguments exceed their request frame"));
        }
        let Request::ScanFragment { fragment, .. } = &mut envelope.request else {
            return Err(malformed("Unexpected fragment arguments"));
        };
        let memory = MemoryBudget::new(16 << 20);
        let bytes = read_arguments(&mut reader, length, &memory).await?;
        let reservation = fragment.bind_wire_literals(bytes.as_slice(), &memory)?;
        return Ok((envelope, None, Some(reservation)));
    }
    let Some(arguments) = envelope.fragment_arguments.take() else { return Ok((envelope, None, None)); };
    let Request::ScanFragment { fragment, .. } = &mut envelope.request else {
        return Err(malformed("Unexpected fragment arguments"));
    };
    if envelope.fragment_ipc != Some(true) || arguments.length == 0 || arguments.predicates.is_empty()
        || arguments.length as u64 > MAXIMUM_REQUEST_BYTES.saturating_sub(line.len() as u64)
    {
        return Err(malformed("Fragment arguments exceed their request frame"));
    }
    let memory = MemoryBudget::new(16 << 20);
    let bytes = read_arguments(&mut reader, arguments.length, &memory).await?;
    let args = fragment.bind_wire_predicates(arguments.predicates, bytes, &memory)?;
    Ok((envelope, Some(args), None))
}

async fn read_arguments(reader: &mut (impl tokio::io::AsyncRead + Unpin), length: usize, memory: &Arc<MemoryBudget>)
    -> Result<FragmentBytes> {
    tokio::time::timeout(Duration::from_secs(5), FragmentBytes::read_from(reader, length, memory)).await
        .map_err(|_| Anomaly::incorrect("rtdb.multi/control-request", "Fragment argument frame timed out"))?
}

async fn exchange(address: &str, request: Request, timeout: Duration, key: &ControlKey) -> Result<(TcpStream, Reply)> {
    let transfer = async {
        let mut stream = TcpStream::connect(address).await.map_err(io)?;
        write_message(&mut stream, &Envelope { control_grant: key.grant(), fragment_ipc: None, fragment_arguments: None,
            fragment_literal_bytes: None, fragment_literal_version: None, request }).await.map_err(io)?;
        let response = read_reply(&mut BufReader::new(&mut stream), address).await?;
        Ok((stream, response))
    };
    tokio::time::timeout(timeout, transfer).await
        .map_err(|_| Anomaly::new(AnomalyCategory::Unavailable, CONTROL_TIMEOUT, format!("No answer from {address}")))?
}

async fn read_reply(reader: &mut (impl tokio::io::AsyncBufRead + Unpin), address: &str) -> Result<Reply> {
    let mut line = String::new();
    reader.take(MAXIMUM_RESPONSE_BYTES).read_line(&mut line).await.map_err(io)?;
    if !line.ends_with('\n') { return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/control-response-size", format!("{address} answered with more than the largest response a caller reads"))); }
    serde_json::from_str(&line).map_err(|error| Anomaly::fault("rtdb.multi/control-response", error.to_string()))
}

pub(crate) async fn start_fragment(
    address: &str, mut request: Request, deadline: tokio::time::Instant, key: &ControlKey, memory: &Arc<MemoryBudget>,
) -> Result<TcpStream> {
    let send = async {
        let mut stream = TcpStream::connect(address).await.map_err(io)?;
        let mut arguments = None;
        if let Request::ScanFragment { fragment, .. } = &mut request {
            write_message(&mut stream, &Envelope { control_grant: key.grant(), fragment_ipc: Some(true),
                fragment_arguments: None, fragment_literal_bytes: None, fragment_literal_version: Some(FRAGMENT_LITERAL_VERSION),
                request: Request::Ping }).await.map_err(io)?;
            let reply = read_reply(&mut BufReader::new(&mut stream), address).await?;
            match reply {
                Reply { response: Response::Done, fragment_literal_version: Some(FRAGMENT_LITERAL_VERSION), .. } => {
                    let (encoded, bytes) = fragment.for_wire(memory)?;
                    *fragment = encoded;
                    arguments = Some(bytes);
                }
                Reply { response: Response::Done, fragment_literal_version: Some(LEGACY_FRAGMENT_LITERAL_VERSION), .. } => {
                    let (encoded, bytes) = fragment.for_wire_v1(memory)?;
                    *fragment = encoded;
                    arguments = Some(bytes);
                }
                Reply { response: Response::Done, .. } => stream = TcpStream::connect(address).await.map_err(io)?,
                Reply { response: Response::Refused { code, message }, .. } => {
                    return Err(Anomaly::new(AnomalyCategory::Unavailable, code, message));
                }
                _ => return Err(Anomaly::fault("rtdb.multi/fragment-response", "A reader did not answer fragment negotiation")),
            }
        }
        let mut header = serde_json::to_vec(&Envelope { control_grant: key.grant(), fragment_ipc: Some(true),
            fragment_arguments: None, fragment_literal_bytes: arguments.as_ref().map(|bytes| bytes.as_slice().len()),
            fragment_literal_version: None, request })
            .map_err(|error| Anomaly::fault("rtdb.multi/control-request", error.to_string()))?;
        header.push(b'\n');
        let payload = arguments.as_ref().map_or(&[][..], FragmentBytes::as_slice);
        if header.len().saturating_add(payload.len()) as u64 > MAXIMUM_REQUEST_BYTES {
            return Err(Anomaly::incorrect("rtdb.multi/control-request", "Control request exceeds its frame limit"));
        }
        stream.write_all(&header).await.map_err(io)?;
        if !payload.is_empty() { stream.write_all(payload).await.map_err(io)?; }
        Ok(stream)
    };
    tokio::time::timeout_at(deadline, send).await
        .map_err(|_| Anomaly::new(AnomalyCategory::Unavailable, CONTROL_TIMEOUT, format!("No answer from {address}")))?
}

pub(crate) async fn receive_fragment(stream: TcpStream, address: &str, deadline: tokio::time::Instant, memory: &Arc<MemoryBudget>) -> Result<FragmentBytes> {
    let receive = async {
        let mut reader = BufReader::new(stream);
        match read_reply(&mut reader, address).await?.response {
            Response::FragmentIpc { bytes } if bytes <= MAXIMUM_FRAGMENT_BYTES => {
                FragmentBytes::read_from(&mut reader, bytes as usize, memory).await
            }
            Response::FragmentIpc { .. } => Err(Anomaly::incorrect("rtdb.multi/fragment-response", "A fragment exceeds the response byte limit")),
            Response::Fragment { batches } => {
                let bytes = STANDARD.decode(batches).map_err(|error| Anomaly::fault("rtdb.multi/fragment-response", error.to_string()))?;
                FragmentBytes::from_vec(bytes, memory)
            }
            Response::Refused { code, message } => Err(Anomaly::new(AnomalyCategory::Unavailable, code, message)),
            other => Err(Anomaly::fault("rtdb.multi/fragment-response", format!("A reader answered a fragment with {other:?}"))),
        }
    };
    tokio::time::timeout_at(deadline, receive).await
        .map_err(|_| Anomaly::new(AnomalyCategory::Unavailable, CONTROL_TIMEOUT, format!("No answer from {address}")))?
}

pub async fn call(address: &str, request: &Request, timeout: Duration, key: &ControlKey) -> Result<Response> {
    exchange(address, request.clone(), timeout, key).await.map(|(_, reply)| reply.response)
}

pub(crate) async fn call_with_pgwire_probe(address: &str, request: Request, timeout: Duration, key: &ControlKey) -> Result<(Response, Option<u64>)> {
    exchange(address, request, timeout, key).await.map(|(_, reply)| (reply.response, reply.startup_probe_overflows))
}

pub(crate) async fn ping(address: &str, timeout: Duration, key: &ControlKey) -> Result<Option<u64>> {
    let (response, overflows) = call_with_pgwire_probe(address, Request::Ping, timeout, key).await?;
    if response == Response::Done { return Ok(overflows); }
    Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/probe-refused", "The node's control endpoint did not answer its liveness probe"))
}

pub(crate) async fn open_pgwire(address: &str, timeout: Duration, key: &ControlKey) -> Result<TcpStream> {
    if key.0.is_some() {
        let (stream, response) = exchange(address, Request::OpenPgwire, timeout, key).await?;
        if response.response == Response::Done {
            stream.set_nodelay(true).map_err(io)?;
            return Ok(stream);
        }
    }
    Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/pgwire-relay-unavailable", "No signed Pgwire relay is available"))
}

fn io(error: std::io::Error) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, CONTROL_IO, error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn activation_retries_temporary_refusals_and_reports_a_terminal_failure() {
        let mut calls = 0;
        let result = retry_activation(Duration::from_secs(1), || {
            calls += 1;
            let error = match calls {
                1 => Anomaly::new(AnomalyCategory::Unavailable, NODE_UNDER_PRESSURE, "reclaiming memory"),
                2 => Anomaly::new(AnomalyCategory::Conflict, CLAIM_CONTENDED, "another directory update won"),
                3 => Anomaly::new(AnomalyCategory::Busy, ACTIVATION_RATE_LIMITED, "the activation window is full"),
                _ => Anomaly::incorrect("test/deployment-gone", "the deployment was deleted"),
            };
            std::future::ready(Err::<(), _>(error))
        }).await;
        assert_eq!(result.unwrap_err().code, "test/deployment-gone");
        assert_eq!(calls, 4);
    }

    #[tokio::test]
    async fn an_activation_started_within_the_retry_window_keeps_its_result() {
        let started = Arc::new(tokio::sync::Notify::new());
        let finish = Arc::new(tokio::sync::Notify::new());
        let task = {
            let (started, finish) = (started.clone(), finish.clone());
            tokio::spawn(async move {
                let mut calls = 0;
                retry_activation(Duration::from_millis(500), || {
                    calls += 1;
                    let (calls, started, finish) = (calls, started.clone(), finish.clone());
                    async move {
                        if calls == 1 { return Err(Anomaly::new(AnomalyCategory::Unavailable, NODE_UNDER_PRESSURE, "reclaiming memory")); }
                        started.notify_one();
                        finish.notified().await;
                        Ok(())
                    }
                }).await
            })
        };
        tokio::time::timeout(Duration::from_secs(5), started.notified()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(550)).await;
        assert!(!task.is_finished());
        finish.notify_one();
        task.await.unwrap().unwrap();
    }

    struct Answers;
    #[async_trait]
    impl Handler for Answers { async fn handle(&self, _: Request) -> Response { Response::Done } }

    async fn call_fragment(address: &str, request: Request, timeout: Duration, key: &ControlKey, memory: &Arc<MemoryBudget>) -> Result<FragmentBytes> {
        let deadline = tokio::time::Instant::now() + timeout;
        let stream = start_fragment(address, request, deadline, key, memory).await?;
        receive_fragment(stream, address, deadline, memory).await
    }

    #[tokio::test]
    async fn fragment_negotiation_accepts_binary_and_legacy_replies() {
        #[derive(Deserialize)]
        struct LegacyEnvelope {
            #[serde(flatten)]
            request: Request,
        }

        let arrow = &[0xff, 0, b'\n', 0x80, 1];
        for (response, body) in [
            (Response::FragmentIpc { bytes: arrow.len() as u64 }, arrow.as_slice()),
            (Response::Fragment { batches: STANDARD.encode(arrow) }, &[]),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap().to_string();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = String::new();
                BufReader::new(&mut stream).read_line(&mut request).await.unwrap();
                assert_eq!(serde_json::from_str::<Envelope>(&request).unwrap().fragment_ipc, Some(true));
                assert_eq!(serde_json::from_str::<LegacyEnvelope>(&request).unwrap().request, Request::Ping);
                let reply = Reply { startup_probe_overflows: None, fragment_literal_version: None, response };
                let mut bytes = serde_json::to_vec(&reply).unwrap();
                bytes.push(b'\n');
                bytes.extend_from_slice(body);
                stream.write_all(&bytes).await.unwrap();
            });
            let memory = MemoryBudget::new(1024);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            let stream = start_fragment(&address, Request::Ping, deadline, &ControlKey::default(), &MemoryBudget::new(1 << 20)).await.unwrap();
            assert_eq!(memory.used_bytes(), 0);
            let received = receive_fragment(stream, &address, deadline, &memory).await.unwrap();
            assert_eq!(received.as_slice(), arrow);
            assert!(memory.used_bytes() >= arrow.len());
            drop(received);
            assert_eq!(memory.used_bytes(), 0);
            server.await.unwrap();
        }
    }

    fn literal_fragment() -> Request {
        use rtdb_plan::{Expr, QualifiedTable, Scan, TableBinding, TemporalFilter};
        use rtdb_types::{Interval, TableRef, TemporalBounds, Timestamp, TransactionKey, TxId, Value};

        let fragment = rtdb_query::ScanFragment {
            scan: Scan {
                table: TableBinding { name: QualifiedTable { database: "db".into(), table: TableRef::new("public", "docs") },
                    oid: None, columns: Default::default() },
                columns: Vec::new(), valid_time: TemporalFilter::All, system_time: TemporalFilter::All,
                clamp_valid_time: false, iids: None, predicates: vec![
                    Expr::Literal(Value::Utf8("literal-on-arrow".into())),
                    Expr::Literal(Value::Time { value: i64::MAX, unit: rtdb_types::TimeUnit::Millisecond, bit_width: 32 }),
                ],
                top_k: None, ordered_top_k: None, matches: Vec::new(),
            },
            prefix: rtdb_query::TriePrefix::new(Vec::new()).unwrap(),
            basis: TransactionKey { tx_id: TxId(1), system_time: Timestamp(1) },
            residence: None,
            bounds: TemporalBounds { valid_time: Interval::ALL, system_time: Interval::from_micros(i64::MIN, 2).unwrap() },
            timezone: "UTC".into(),
        };
        Request::ScanFragment { name: "db".into(), fragment, grant: "grant".into() }
    }

    struct FragmentAnswers;

    #[async_trait]
    impl Handler for FragmentAnswers {
        async fn handle(&self, request: Request) -> Response {
            assert_eq!(request, Request::Ping);
            Response::Done
        }

        async fn fragment_ipc(&self, name: &str, fragment: &rtdb_query::ScanFragment, grant: &str,
            arguments: Option<rtdb_query::BoundParameters>) -> Option<Result<FragmentBytes>> {
            let Request::ScanFragment { fragment: expected, .. } = literal_fragment() else { unreachable!() };
            assert_eq!(fragment, &expected);
            assert_eq!((name, grant), ("db", "grant"));
            assert!(arguments.is_none());
            Some(FragmentBytes::from_vec(vec![0xff, b'\n', 1], &MemoryBudget::new(1024)))
        }
    }

    #[tokio::test]
    async fn negotiated_fragments_use_one_socket_and_a_binary_reply() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let stop = CancellationToken::new();
        let server = tokio::spawn(serve(listener, Arc::new(FragmentAnswers), ControlKey::default(), stop.clone()));
        let memory = MemoryBudget::new(1 << 20);
        let answer = call_fragment(&address, literal_fragment(), Duration::from_secs(5), &ControlKey::default(), &memory).await.unwrap();
        assert_eq!(answer.as_slice(), &[0xff, b'\n', 1]);

        let old = call(&address, &literal_fragment(), Duration::from_secs(5), &ControlKey::default()).await.unwrap();
        let Response::Fragment { batches } = old else { panic!("old coordinator's reply encoding") };
        assert_eq!(STANDARD.decode(batches).unwrap(), answer.as_slice());

        let mut socket = TcpStream::connect(&address).await.unwrap();
        write_message(&mut socket, &Envelope { control_grant: None, fragment_ipc: Some(true), fragment_arguments: None,
            fragment_literal_bytes: None, fragment_literal_version: None, request: Request::Ping }).await.unwrap();
        let reply = read_reply(&mut BufReader::new(&mut socket), &address).await.unwrap();
        assert_eq!(reply.fragment_literal_version, Some(1));
        let Request::ScanFragment { name, fragment, grant } = literal_fragment() else { unreachable!() };
        let (fragment, body) = fragment.for_wire_v1(&memory).unwrap();
        write_message(&mut socket, &Envelope { control_grant: None, fragment_ipc: Some(true), fragment_arguments: None,
            fragment_literal_bytes: Some(body.as_slice().len()), fragment_literal_version: None,
            request: Request::ScanFragment { name, fragment, grant } }).await.unwrap();
        socket.write_all(body.as_slice()).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let v1 = receive_fragment(socket, &address, deadline, &memory).await.unwrap();
        assert_eq!(v1.as_slice(), answer.as_slice());

        let mut socket = TcpStream::connect(&address).await.unwrap();
        write_message(&mut socket, &Envelope { control_grant: None, fragment_ipc: Some(true),
            fragment_arguments: None, fragment_literal_bytes: Some(0), fragment_literal_version: None, request: literal_fragment() }).await.unwrap();
        let refused = read_reply(&mut BufReader::new(socket), &address).await.unwrap();
        assert!(matches!(refused.response, Response::Refused { code, .. } if code == "rtdb.multi/control-request"));
        stop.cancel();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_v1_agent_receives_a_v1_literal_page_on_the_negotiated_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = String::new();
            BufReader::new(&mut stream).read_line(&mut header).await.unwrap();
            let probe = serde_json::from_str::<Envelope>(&header).unwrap();
            assert_eq!(probe.request, Request::Ping);
            assert_eq!(probe.fragment_literal_version, Some(2));
            write_message(&mut stream, &Reply { startup_probe_overflows: None, fragment_literal_version: Some(1),
                response: Response::Done }).await.unwrap();

            let mut reader = BufReader::new(&mut stream);
            header.clear();
            reader.read_line(&mut header).await.unwrap();
            let request = serde_json::from_str::<Envelope>(&header).unwrap();
            let Request::ScanFragment { name, fragment, grant } = literal_fragment() else { unreachable!() };
            let (fragment, expected) = fragment.for_wire_v1(&MemoryBudget::new(1 << 20)).unwrap();
            assert_eq!(request.request, Request::ScanFragment { name, fragment, grant });
            assert_eq!(request.fragment_literal_bytes, Some(expected.as_slice().len()));
            let body = read_arguments(&mut reader, expected.as_slice().len(), &MemoryBudget::new(1 << 20)).await.unwrap();
            assert_eq!(body.as_slice(), expected.as_slice());
            write_message(&mut stream, &Response::FragmentIpc { bytes: 1 }).await.unwrap();
            stream.write_all(&[0xff]).await.unwrap();
        });
        let memory = MemoryBudget::new(1 << 20);
        let answer = call_fragment(&address, literal_fragment(), Duration::from_secs(5), &ControlKey::default(), &memory).await.unwrap();
        assert_eq!(answer.as_slice(), &[0xff]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn a_legacy_agent_receives_only_the_original_request_form() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut probe, _) = listener.accept().await.unwrap();
            let mut header = String::new();
            BufReader::new(&mut probe).read_line(&mut header).await.unwrap();
            assert_eq!(serde_json::from_str::<Envelope>(&header).unwrap().request, Request::Ping);
            write_message(&mut probe, &Response::Done).await.unwrap();
            drop(probe);

            let (mut stream, _) = listener.accept().await.unwrap();
            header.clear();
            BufReader::new(&mut stream).read_line(&mut header).await.unwrap();
            let request = serde_json::from_str::<Envelope>(&header).unwrap();
            assert_eq!(request.request, literal_fragment());
            assert!(request.fragment_arguments.is_none() && request.fragment_literal_bytes.is_none());
            write_message(&mut stream, &Response::Fragment { batches: STANDARD.encode([0xff, b'\n', 1]) }).await.unwrap();
        });
        let memory = MemoryBudget::new(1 << 20);
        let answer = call_fragment(&address, literal_fragment(), Duration::from_secs(5), &ControlKey::default(), &memory).await.unwrap();
        assert_eq!(answer.as_slice(), &[0xff, b'\n', 1]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn fragment_length_is_admitted_before_reading_its_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = String::new();
            BufReader::new(&mut stream).read_line(&mut request).await.unwrap();
            write_message(&mut stream, &Reply { startup_probe_overflows: None, fragment_literal_version: None,
                response: Response::FragmentIpc { bytes: MAXIMUM_FRAGMENT_BYTES } }).await.unwrap();
        });
        let memory = MemoryBudget::new(1024);
        let error = call_fragment(&address, Request::Ping, Duration::from_secs(5), &ControlKey::default(), &memory).await.unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(memory.used_bytes(), 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn a_node_reads_no_more_of_a_request_than_the_largest_it_takes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, Arc::new(Answers), ControlKey::default(), CancellationToken::new()));
        let mut stream = TcpStream::connect(address).await.unwrap();
        let _ = stream.write_all(&vec![b'x'; 2 << 20]).await;
        let mut answer = String::new();
        BufReader::new(stream).read_line(&mut answer).await.unwrap();
        assert!(matches!(serde_json::from_str::<Response>(&answer).unwrap(), Response::Refused { .. }), "an oversized request is refused, not read whole: {answer}");
        assert!(matches!(call(&address.to_string(), &Request::Ping, Duration::from_secs(5), &ControlKey::default()).await.unwrap(), Response::Done));
    }
}
