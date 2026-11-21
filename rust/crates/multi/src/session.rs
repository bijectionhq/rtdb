//! A gateway session: the client's connection outlives the backend it is spliced to (`MovesAreInvisible`).
//!
//! The gateway reads pgwire frames in both directions.
//! It learns the session's transaction status from every ReadyForQuery, and remembers the startup message and every named prepared statement.
//! Whenever the session is idle and its backend is gone or no longer the deployment's holder, it reconnects to the current holder, replays the startup and the prepared statements, swallows their replies, and carries on.
//! A backend lost in the middle of a command is replayed only if the command was keyed and none of its replies reached the client; otherwise it may have committed, so the client is told and the session ends.
//! A reconnection carries no password: once the first holder has authenticated the client, each reconnection's startup carries a capability the gateway mints instead, which needs the gateway to hold the cell's secret, or backends that trust the gateway.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use rtdb_directory::{DeploymentRecord, Directory, Lifecycle};
use rtdb_pgwire::{GATEWAY_SESSION_STATE_TAG, GATEWAY_SETTINGS_KIND, GATEWAY_SETTINGS_PARAMETER, GATEWAY_SQL_PREPARE_KIND};
use rtdb_types::{Anomaly, AnomalyCategory, MemoryBudget, MemoryReservation, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{mpsc, watch};
use tokio_util::task::AbortOnDropHandle;

const MAXIMUM_FRAME_BYTES: usize = 1 << 30;
pub(crate) const DEFAULT_GATEWAY_FRAME_BYTES: usize = 1 << 30;
const FRAME_MEMORY: &str = "rtdb.multi/gateway-frame-memory";
pub(crate) const STARTUP_WITHIN: Duration = Duration::from_secs(35);
const STARTUP_TIMEOUT: &str = "rtdb.multi/startup-timeout";
const RECONNECT_WITHIN: Duration = Duration::from_secs(30);
const RECONNECT_RETRY_DELAY: Duration = Duration::from_millis(100);
/// A capability serves one reconnection, so it only has to outlive the startup it is sent in.
const CAPABILITY_TTL: std::time::Duration = std::time::Duration::from_secs(30);

/// One pgwire message after startup: its type byte and its body.
#[derive(Clone, Debug)]
pub struct Frame { pub tag: u8, pub body: Bytes }

impl Frame {
    pub fn encode(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(5 + self.body.len());
        bytes.put_u8(self.tag);
        bytes.put_i32((self.body.len() + 4) as i32);
        bytes.extend_from_slice(&self.body);
        bytes.freeze()
    }
}

struct BufferedBody {
    bytes: Vec<u8>,
    _reservation: Arc<MemoryReservation>,
}

impl AsRef<[u8]> for BufferedBody {
    fn as_ref(&self) -> &[u8] { &self.bytes }
}

async fn read_frame(reader: &mut (impl AsyncRead + Unpin), memory: &Arc<MemoryBudget>) -> std::io::Result<Option<Frame>> {
    let tag = match reader.read_u8().await {
        Ok(tag) => tag,
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    };
    let length = reader.read_i32().await?;
    let length = usize::try_from(length).ok().filter(|length| (4..=MAXIMUM_FRAME_BYTES).contains(length))
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "frame length out of range"))?;
    let reservation = memory.reserve(length * 2 + 128)
        .map_err(|cause| std::io::Error::new(std::io::ErrorKind::OutOfMemory, cause.to_string()))?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length - 4).map_err(|cause| std::io::Error::new(std::io::ErrorKind::OutOfMemory, cause))?;
    bytes.resize(length - 4, 0);
    reader.read_exact(&mut bytes).await?;
    Ok(Some(Frame { tag, body: Bytes::from_owner(BufferedBody { bytes, _reservation: reservation }) }))
}

/// Reads frames off `reader` into a channel, so the session can wait on both directions without losing a partly read frame.
fn pump(mut reader: impl AsyncRead + Unpin + Send + 'static, memory: Arc<MemoryBudget>) -> (mpsc::Receiver<std::io::Result<Frame>>, AbortOnDropHandle<()>) {
    let (sender, receiver) = mpsc::channel(64);
    let task = tokio::spawn(async move {
        loop {
            match read_frame(&mut reader, &memory).await {
                Ok(Some(frame)) => if sender.send(Ok(frame)).await.is_err() { return },
                Ok(None) => return,
                Err(error) => { let _ = sender.send(Err(error)).await; return; }
            }
        }
    });
    (receiver, AbortOnDropHandle::new(task))
}

/// Where a session is connected, and how to reconnect it.
#[async_trait::async_trait]
pub trait Router: Send + Sync {
    /// A connection to the deployment's current holder, the control address to touch it at, and the epoch it holds under.
    async fn route(&self) -> Result<(TcpStream, Option<String>, u64)>;
    /// Keeps a routed backend warm for the duration of its connection.
    fn start_touching(&self, _address: String) -> Option<Arc<dyn Send + Sync>> { None }
}

struct Backend {
    writer: OwnedWriteHalf,
    frames: mpsc::Receiver<std::io::Result<Frame>>,
    _reader: AbortOnDropHandle<()>,
    heartbeat: Option<Arc<dyn Send + Sync>>,
    epoch: u64,
    drain_deadline: Option<tokio::time::Instant>,
}

impl Backend {
    fn new(stream: TcpStream, epoch: u64, memory: Arc<MemoryBudget>) -> Self {
        let (read, writer) = stream.into_split();
        let (frames, reader) = pump(read, memory);
        Self { writer, frames, _reader: reader, heartbeat: None, epoch, drain_deadline: None }
    }

    async fn dispatch(&mut self, frame: &Frame) -> bool {
        if self.drain_deadline.is_some() { return false; }
        if self.writer.write_all(&frame.encode()).await.is_ok() { return true; }
        // The reader may still hold replies that decide whether the command can be retried.
        self.drain_deadline = Some(tokio::time::Instant::now() + RECONNECT_WITHIN);
        let _ = self.writer.shutdown().await;
        false
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Status { Idle, InTransaction, Busy }

/// What a gateway needs to mint a session's capabilities: the cell's secret and the session's startup.
pub struct Grants { pub secret: Arc<crate::capability::CellSecret>, pub parameters: std::collections::BTreeMap<String, String>, pub read_only: bool }

struct RememberedStatement { order: u64, frame: Frame }

pub struct Session {
    startup: Bytes,
    frame_memory: Arc<MemoryBudget>,
    installation_status: Option<Frame>,
    grants: Option<Grants>,
    /// The principal the first backend authenticated and reported it may be vouched for, for whom the gateway mints its reconnections' capabilities.
    authenticated: Option<String>,
    /// What the first backend's greeting has reported so far: its authentication, the session's user, and whether that user may be vouched for.
    greeting: Greeting,
    router: Arc<dyn Router>,
    record: watch::Receiver<Option<DeploymentRecord>>,
    prepared: BTreeMap<String, RememberedStatement>,
    next_statement_order: u64,
    await_token: Option<Frame>,
    settings: Option<Frame>,
    status: Status,
    /// The frames of the command sent since the session was last idle, kept until it is answered, so a refusal before it ran can resend it.
    pending: Vec<Frame>,
    prepared_responses: usize,
    /// Whether any reply to the pending command has reached the client; once one has, the command is never resent.
    answered: bool,
    /// Whether the pending command went to a backend lost before any reply reached the client, so that it may have run there.
    lost_unanswered: bool,
    /// Discarding a refused command's replies through its ReadyForQuery, before resending it.
    discarding: bool,
    /// Whether the gateway keys the session's commands, so that one whose holder went away can be resent without running twice.
    keyed: bool,
    /// The key of the pending command, which began while the session was idle.
    key: Option<String>,
    /// ReadyForQuery replies still owed to the gateway's own statements, swallowed rather than passed to the client.
    swallowing: usize,
}

impl Session {
    pub fn new(startup: Bytes, router: Arc<dyn Router>, directory: Arc<dyn Directory>, deployment: String, grants: Option<Grants>, keyed: bool) -> Self {
        let record = rtdb_directory::watch_deployment(directory, deployment, RECONNECT_RETRY_DELAY);
        Self { startup, frame_memory: MemoryBudget::new(DEFAULT_GATEWAY_FRAME_BYTES), installation_status: None, grants, authenticated: None, greeting: Greeting::default(), router, record, prepared: BTreeMap::new(), next_statement_order: 0, await_token: None, settings: None, status: Status::Busy, pending: Vec::new(), prepared_responses: 0, answered: false, lost_unanswered: false, discarding: false, keyed, key: None, swallowing: 0 }
    }

    pub(crate) fn with_frame_memory(mut self, memory: Arc<MemoryBudget>) -> Self {
        self.frame_memory = memory;
        self
    }

    pub(crate) fn with_installation(mut self, installation: Option<uuid::Uuid>) -> Self {
        self.installation_status = installation.map(|id| {
            let mut body = BytesMut::from(&b"rtdb_installation_id\0"[..]);
            body.extend_from_slice(id.simple().to_string().as_bytes());
            body.put_u8(0);
            Frame { tag: b'S', body: body.freeze() }
        });
        self
    }

    /// Reconnects to the current holder, retrying through a failover: until the dead holder's lease expires, no other node may claim.
    async fn connect(&mut self) -> Result<Backend> {
        let result = tokio::time::timeout(RECONNECT_WITHIN, async {
            loop {
                let previous = placement(&self.record.borrow());
                match self.connect_once().await {
                    Ok(backend) => return Ok(backend),
                    Err(error) if !matches!(error.category, AnomalyCategory::Unavailable | AnomalyCategory::Conflict) => return Err(error),
                    Err(_) => wait_for_placement(&mut self.record, previous, RECONNECT_RETRY_DELAY).await,
                }
            }
        }).await.unwrap_or_else(|_| Err(lost("timed out reconnecting to the holder")));
        result.map_err(|error| {
            if self.lost_unanswered {
                lost(&format!("{}; the command's outcome is unknown", error.message.as_deref().unwrap_or(&error.code)))
            } else {
                error
            }
        })
    }

    async fn connect_once(&mut self) -> Result<Backend> {
        let (mut stream, control, epoch) = self.router.route().await?;
        stream.write_all(&self.reconnection_startup()).await.map_err(io)?;
        let mut backend = Backend::new(stream, epoch, Arc::clone(&self.frame_memory));
        let features = replay_ready(&mut backend.frames).await?;
        if self.await_token.is_some() && !features.await_token {
            return Err(Anomaly::new(AnomalyCategory::Unsupported, "rtdb.multi/reconnect-basis", "The new holder cannot restore the session's await token"));
        }
        if self.settings.is_some() && !features.settings {
            return Err(Anomaly::new(AnomalyCategory::Unsupported, "rtdb.multi/reconnect-settings", "The new holder cannot restore the session's settings"));
        }
        let mut definitions: Vec<_> = self.prepared.values().collect();
        if !features.sql_prepare && definitions.iter().any(|definition| definition.frame.tag == GATEWAY_SESSION_STATE_TAG) {
            return Err(Anomaly::new(AnomalyCategory::Unsupported, "rtdb.multi/reconnect-sql-prepare", "The new holder cannot restore SQL prepared statements"));
        }
        definitions.sort_unstable_by_key(|definition| definition.order);
        if self.settings.is_some() || self.await_token.is_some() || !definitions.is_empty() {
            let send = async {
                for frame in self.settings.iter().chain(self.await_token.iter()).chain(definitions.into_iter().map(|definition| &definition.frame)) {
                    backend.writer.write_all(&frame.encode()).await.map_err(io)?;
                }
                backend.writer.write_all(&[b'S', 0, 0, 0, 4]).await.map_err(io)
            };
            tokio::try_join!(send, replay_ready(&mut backend.frames))?;
        }
        backend.heartbeat = control.and_then(|address| self.router.start_touching(address));
        Ok(backend)
    }

    /// The startup a reconnection sends: the client's, with a fresh capability once the client has authenticated.
    fn reconnection_startup(&self) -> Bytes {
        let (Some(grants), Some(user)) = (&self.grants, &self.authenticated) else { return self.startup.clone() };
        let Some(deployment) = grants.parameters.get("database").or_else(|| grants.parameters.get("user")) else { return self.startup.clone() };
        let mut parameters = grants.parameters.clone();
        // The capability names the principal the backend authenticated, whatever user the client claimed.
        parameters.insert("user".into(), user.clone());
        parameters.insert(rtdb_pgwire::CAPABILITY_PARAMETER.into(), grants.secret.mint(deployment, user, grants.read_only, CAPABILITY_TTL));
        Bytes::from(crate::gateway::encode_startup(&parameters))
    }

    fn stale(&self, backend: &Backend) -> bool {
        self.record.borrow().as_ref().is_some_and(|record| record.epoch > backend.epoch)
    }

    /// Runs the session over an already-routed first backend whose greeting the client has not yet seen.
    pub async fn run(self, client: impl AsyncRead + AsyncWrite + Unpin + Send + 'static, first: TcpStream, first_control: Option<String>, first_epoch: u64) -> Result<()> {
        self.run_before(client, first, first_control, first_epoch, tokio::time::Instant::now() + STARTUP_WITHIN).await
    }

    pub(crate) async fn run_before(mut self, client: impl AsyncRead + AsyncWrite + Unpin + Send + 'static, first: TcpStream, first_control: Option<String>, first_epoch: u64,
                                   startup_deadline: tokio::time::Instant) -> Result<()> {
        let (client_read, mut client_write) = tokio::io::split(client);
        let (mut requests, mut client_reader) = pump(client_read, Arc::clone(&self.frame_memory));
        let mut copying_in = false;
        let mut first = Backend::new(first, first_epoch, Arc::clone(&self.frame_memory));
        first.heartbeat = first_control.and_then(|address| self.router.start_touching(address));
        let mut backend = Some(first);
        enum Event {
            Request(Option<std::io::Result<Frame>>),
            Reply(Option<std::io::Result<Frame>>),
            Retired,
        }
        let outcome = async {
            tokio::time::timeout_at(startup_deadline, backend.as_mut().expect("just set").writer.write_all(&self.startup))
                .await.map_err(|_| startup_timeout())?.map_err(io)?;
            loop {
                let event = {
                    let Some(current) = backend.as_mut() else { break Ok(()) };
                    let waiting_for_ready = self.keyed && !copying_in && self.pending.last().is_some_and(|frame| matches!(frame.tag, b'Q' | b'S'));
                    tokio::select! {
                        biased;
                        _ = self.record.wait_for(|record| record.as_ref().is_some_and(|record| {
                            matches!(record.lifecycle, Lifecycle::Deleting | Lifecycle::Restoring)
                                || record.lifecycle == Lifecycle::Deleted && record.moved_at_ms.is_none()
                        })) => Event::Retired,
                        _ = tokio::time::sleep_until(startup_deadline), if !self.greeting.done => return Err(startup_timeout()),
                        event = async {
                            if let Some(deadline) = current.drain_deadline {
                                let reply = tokio::time::timeout_at(deadline, current.frames.recv()).await
                                    .map_err(|_| lost("timed out draining the failed connection; the command's outcome is unknown"))?;
                                return Ok::<_, Anomaly>(Event::Reply(reply));
                            }
                            tokio::select! {
                                request = requests.recv(), if !waiting_for_ready => Ok(Event::Request(request)),
                                _ = &mut client_reader, if waiting_for_ready => Ok(Event::Request(None)),
                                reply = current.frames.recv() => Ok(Event::Reply(reply)),
                            }
                        } => event?,
                    }
                };
                match event {
                    Event::Retired => {
                        client_write.write_all(&error_frame("08006", "The installation has retired").encode()).await.map_err(io)?;
                        client_write.flush().await.map_err(io)?;
                        break Ok(());
                    }
                    Event::Request(None) => break Ok(()),
                    Event::Request(Some(request)) => {
                        let request = request.map_err(io)?;
                        if request.tag == GATEWAY_SESSION_STATE_TAG {
                            break Err(Anomaly::incorrect("rtdb.multi/private-pgwire-frame", "A client cannot send a gateway session state frame"));
                        }
                        if request.tag == b'X' {
                            if let Some(current) = backend.as_mut() { let _ = current.writer.write_all(&request.encode()).await; }
                            break Ok(());
                        }
                        if !self.greeting.done {
                            tokio::time::timeout_at(startup_deadline, backend.as_mut().expect("greeting backend").writer.write_all(&request.encode()))
                                .await.map_err(|_| startup_timeout())?.map_err(io)?;
                            continue;
                        }
                        if copying_in && matches!(request.tag, b'd' | b'c' | b'f' | b'S' | b'H') {
                            if matches!(request.tag, b'd' | b'c' | b'f') {
                                backend.as_mut().expect("copy backend").dispatch(&request).await;
                                copying_in = request.tag == b'd';
                            }
                            continue;
                        }
                        if request.tag == b'H' {
                            backend.as_mut().expect("connected").dispatch(&request).await;
                            if self.pending.last().is_some_and(|frame| !matches!(frame.tag, b'Q' | b'S' | b'H')) {
                                self.pending.push(request);
                            }
                            continue;
                        }
                        let starts = self.status == Status::Idle;
                        if starts { self.answered = false; self.lost_unanswered = false; self.key = self.keyed.then(|| uuid::Uuid::new_v4().simple().to_string()); }
                        if starts && backend.as_ref().is_some_and(|current| self.stale(current)) { backend = None; }
                        if backend.is_none() { backend = Some(self.connect().await?); }
                        let current = backend.as_mut().expect("connected");
                        if starts { if let Some(key) = self.key.clone() { self.send_key(current, &key).await; } }
                        current.dispatch(&request).await;
                        self.pending.push(request);
                        self.status = Status::Busy;
                    }
                    Event::Reply(Some(Ok(frame))) => {
                        if frame.tag == GATEWAY_SESSION_STATE_TAG {
                            if self.greeting.done && !self.discarding {
                                match frame.body.first().copied() {
                                    Some(0) => self.await_token = None,
                                    Some(1) => self.await_token = Some(frame),
                                    Some(GATEWAY_SETTINGS_KIND) if frame.body.len() > 1 => self.settings = Some(frame),
                                    Some(GATEWAY_SQL_PREPARE_KIND) if frame.body.len() > 1 => {
                                        let name = frame.body[1..].split(|byte| *byte == 0).next().and_then(|name| std::str::from_utf8(name).ok())
                                            .filter(|name| !name.is_empty()).ok_or_else(|| lost("the holder sent an invalid prepared name"))?.to_owned();
                                        self.remember_statement(name, frame);
                                    }
                                    _ => {}
                                }
                            }
                            continue;
                        }
                        if !self.greeting.done {
                            if frame.tag == b'S' && frame.body.split(|byte| *byte == 0).next() == Some(GATEWAY_SETTINGS_PARAMETER.as_bytes()) {
                                continue;
                            }
                            self.greeting.observe(&frame, &mut self.authenticated);
                            tokio::time::timeout_at(startup_deadline, async {
                                if self.greeting.done {
                                    self.status = Status::Idle;
                                    if let Some(installation) = self.installation_status.take() {
                                        client_write.write_all(&installation.encode()).await.map_err(io)?;
                                    }
                                }
                                client_write.write_all(&frame.encode()).await.map_err(io)?;
                                client_write.flush().await.map_err(io)?;
                                Ok::<_, Anomaly>(())
                            }).await.map_err(|_| startup_timeout())??;
                            if frame.tag == b'E' { break Ok(()); }
                            continue;
                        }
                        if self.swallowing > 0 {
                            // A key the backend did not take leaves the command unkeyed, and an unkeyed command is never resent.
                            if frame.tag == b'E' { self.key = None; }
                            if frame.tag == b'Z' { self.swallowing -= 1; }
                            continue;
                        }
                        if !self.pending.is_empty() && !self.discarding && frame.tag == b'E' && ingestion_stopped(&frame) {
                            if self.answered || self.key.is_none() {
                                break Err(lost("the holder's driver stopped during the command, whose outcome is unknown"));
                            }
                            self.lost_unanswered = true;
                            self.discarding = true;
                        }
                        if !self.answered && !self.discarding && frame.tag == b'E' && not_held(&frame) { self.discarding = true; }
                        if self.discarding {
                            if frame.tag == b'Z' {
                                self.discarding = false;
                                backend = Some(self.connect().await?);
                                self.resend(backend.as_mut().expect("connected")).await;
                            }
                            continue;
                        }
                        if self.lost_unanswered && frame.tag == b'E' {
                            break Err(lost("the command's outcome is unknown because its holder stopped and its successor could not confirm it"));
                        }
                        self.answered = true;
                        match frame.tag {
                            b'1' | b'3' => self.confirm_prepared_change(frame.tag),
                            b'C' if frame.body.as_ref() == b"DISCARD ALL\0" => self.prepared.clear(),
                            b'G' => {
                                copying_in = true;
                                if self.pending.last().is_some_and(|frame| frame.tag == b'S') {
                                    self.pending.pop();
                                }
                            }
                            b'E' => copying_in = false,
                            b'Z' => {
                                if let Some(end) = self.pending.iter().position(|request| matches!(request.tag, b'Q' | b'S')) {
                                    self.pending.drain(..=end);
                                }
                                self.prepared_responses = 0;
                                self.status = match frame.body.first() {
                                    Some(b'I') if self.pending.is_empty() => Status::Idle,
                                    Some(b'T' | b'E') => Status::InTransaction,
                                    _ => Status::Busy,
                                };
                            }
                            _ => {}
                        }
                        client_write.write_all(&frame.encode()).await.map_err(io)?;
                        client_write.flush().await.map_err(io)?;
                    }
                    Event::Reply(Some(Err(error))) if error.kind() == std::io::ErrorKind::OutOfMemory => {
                        if self.status != Status::Idle && self.greeting.done {
                            break Err(lost("the holder's answer exceeded the gateway's frame memory budget and the command's outcome is unknown"));
                        }
                        break Err(io(error));
                    }
                    Event::Reply(Some(Err(_)) | None) => {
                        if !self.greeting.done { break Err(lost("the holder closed during session startup")); }
                        backend = None;
                        // A refusal whose ReadyForQuery never came is over with its backend: the resent command's replies are the client's.
                        self.discarding = false;
                        if self.status != Status::Idle {
                            // A keyed command none of whose replies reached the client is resent under its key: whatever of it committed is not run again.
                            if self.key.is_none() || self.answered { break Err(lost("the holder went away during a command, whose outcome is unknown")); }
                            self.lost_unanswered = true;
                            self.swallowing = 0;
                            backend = Some(self.connect().await?);
                            self.resend(backend.as_mut().expect("connected")).await;
                            continue;
                        }
                        backend = Some(self.connect().await?);
                    }
                }
            }
        }.await;
        if let Err(error) = &outcome {
            let code = match error.code.as_ref() {
                STARTUP_TIMEOUT => "57P03",
                FRAME_MEMORY => "53200",
                _ => "08006",
            };
            let reply = error_frame(code, error.message.as_deref().unwrap_or(error.code.as_ref())).encode();
            if error.code == STARTUP_TIMEOUT {
                let _ = tokio::time::timeout(Duration::from_millis(100), client_write.write_all(&reply)).await;
            } else {
                let _ = client_write.write_all(&reply).await;
            }
            let _ = client_write.flush().await;
        }
        outcome
    }

    /// Keys the transactions of the command that follows on `backend` by `key`, swallowing the statement's replies.
    async fn send_key(&mut self, backend: &mut Backend, key: &str) {
        let mut body = format!("SET {} = '{key}'", rtdb_node::IDEMPOTENCY_KEY_PARAMETER).into_bytes();
        body.push(0);
        if backend.dispatch(&Frame { tag: b'Q', body: Bytes::from(body) }).await { self.swallowing += 1; }
    }

    /// Sends the pending command again on a new backend, under its key if it has one.
    async fn resend(&mut self, backend: &mut Backend) {
        if let Some(key) = self.key.clone() { self.send_key(backend, &key).await; }
        for request in &self.pending {
            if !backend.dispatch(request).await { break; }
        }
    }

    fn remember_statement(&mut self, name: String, frame: Frame) {
        self.prepared.insert(name, RememberedStatement { order: self.next_statement_order, frame });
        self.next_statement_order = self.next_statement_order.wrapping_add(1);
    }

    fn confirm_prepared_change(&mut self, response: u8) {
        let Some((position, request)) = self.pending.iter().enumerate().skip(self.prepared_responses)
            .take_while(|(_, request)| !matches!(request.tag, b'Q' | b'S'))
            .find(|(_, request)| matches!(request.tag, b'P' | b'C')) else { return };
        self.prepared_responses = position + 1;
        match (response, request.tag) {
            (b'1', b'P') => {
                let name = parse_name(request);
                if !name.is_empty() { self.remember_statement(name, request.clone()); }
            }
            (b'3', b'C') if request.body.first() == Some(&b'S') => {
                let name = request.body[1..].split(|byte| *byte == 0).next().map(|name| String::from_utf8_lossy(name).into_owned()).unwrap_or_default();
                self.prepared.remove(&name);
            }
            _ => {}
        }
    }
}

#[derive(Default)]
struct ReplayFeatures { await_token: bool, settings: bool, sql_prepare: bool }

async fn replay_ready(frames: &mut mpsc::Receiver<std::io::Result<Frame>>) -> Result<ReplayFeatures> {
    let mut features = ReplayFeatures::default();
    loop {
        let frame = frames.recv().await.ok_or_else(|| lost("the new backend closed while restoring the session"))?.map_err(io)?;
        match frame.tag {
            b'E' => return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/reconnect-refused", "The new holder refused the session's replay")),
            b'R' if frame.body.len() >= 4 && frame.body[..4] != [0, 0, 0, 0] => return Err(Anomaly::new(AnomalyCategory::Unsupported, "rtdb.multi/reconnect-auth", "The new holder asks for a password the gateway does not hold")),
            GATEWAY_SESSION_STATE_TAG => match frame.body.first().copied() {
                Some(0 | 1) => features.await_token = true,
                Some(GATEWAY_SETTINGS_KIND) => features.settings = true,
                Some(GATEWAY_SQL_PREPARE_KIND) => features.sql_prepare = true,
                _ => {}
            },
            b'Z' => return Ok(features),
            _ => {}
        }
    }
}

fn placement(record: &Option<DeploymentRecord>) -> Option<(u64, Lifecycle)> {
    record.as_ref().map(|record| (record.epoch, record.lifecycle))
}

async fn wait_for_placement(record: &mut watch::Receiver<Option<DeploymentRecord>>, before: Option<(u64, Lifecycle)>, delay: Duration) {
    let retry_at = tokio::time::Instant::now() + delay;
    let watch_closed = tokio::time::timeout_at(retry_at, record.wait_for(|current| placement(current) != before)).await
        .is_ok_and(|update| update.is_err());
    if watch_closed { tokio::time::sleep_until(retry_at).await; }
}

/// Whether an ErrorResponse is a placed node's refusal of a database it no longer holds, raised before the command ran.
fn not_held(frame: &Frame) -> bool {
    backend_anomaly_code(frame).map_or_else(
        || frame.body.windows(rtdb_node::NOT_HELD.len()).any(|window| window == rtdb_node::NOT_HELD.as_bytes()),
        |code| code == rtdb_node::NOT_HELD.as_bytes(),
    )
}

fn backend_anomaly_code(frame: &Frame) -> Option<&[u8]> {
    frame.body.split(|byte| *byte == 0).find_map(|field| field.strip_prefix(&[rtdb_pgwire::ANOMALY_CODE_FIELD]))
}

fn ingestion_stopped(frame: &Frame) -> bool {
    #[derive(serde::Deserialize)]
    struct ErrorDetail<'a> { code: &'a str }
    if let Some(code) = backend_anomaly_code(frame) { return code == b"xtdb/ingestion-stopped"; }
    frame.body.split(|byte| *byte == 0).find_map(|field| field.strip_prefix(b"D"))
        .is_some_and(|detail| serde_json::from_slice::<ErrorDetail<'_>>(detail).is_ok_and(|anomaly| anomaly.code == "xtdb/ingestion-stopped"))
}

fn parse_name(frame: &Frame) -> String {
    frame.body.split(|byte| *byte == 0).next().map(|name| String::from_utf8_lossy(name).into_owned()).unwrap_or_default()
}

/// What the first backend's greeting, everything before its first ReadyForQuery, has reported.
/// Only the greeting counts: a parameter status later in the session reports a `SET`, which the client chose.
#[derive(Default)]
struct Greeting { done: bool, authenticated: bool, user: Option<String>, delegable: bool }

impl Greeting {
    fn observe(&mut self, frame: &Frame, vouched: &mut Option<String>) {
        if self.done { return; }
        match frame.tag {
            b'R' if frame.body.get(..4) == Some(&[0, 0, 0, 0]) => self.authenticated = true,
            b'S' => {
                let mut fields = frame.body.split(|byte| *byte == 0).map(|field| String::from_utf8_lossy(field).into_owned());
                if let (Some(key), Some(value)) = (fields.next(), fields.next()) {
                    if key == "user" { self.user = Some(value); } else if key == rtdb_pgwire::DELEGABLE_PARAMETER { self.delegable = value == "on"; }
                }
            }
            b'Z' => {
                self.done = true;
                if self.authenticated && self.delegable { *vouched = self.user.clone(); }
            }
            _ => {}
        }
    }
}

pub fn error_frame(sqlstate: &str, message: &str) -> Frame {
    let mut body = BytesMut::new();
    for (tag, value) in [(b'S', "FATAL"), (b'V', "FATAL"), (b'C', sqlstate), (b'M', message)] {
        body.put_u8(tag);
        body.extend_from_slice(value.as_bytes());
        body.put_u8(0);
    }
    body.put_u8(0);
    Frame { tag: b'E', body: body.freeze() }
}

fn lost(message: &str) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/holder-lost", message) }
fn startup_timeout() -> Anomaly { Anomaly::new(AnomalyCategory::Busy, STARTUP_TIMEOUT, "The holder did not finish session startup before the deadline") }
fn io(error: std::io::Error) -> Anomaly {
    if error.kind() == std::io::ErrorKind::OutOfMemory {
        return Anomaly::new(AnomalyCategory::Busy, FRAME_MEMORY, "The gateway's frame memory budget is exhausted");
    }
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/session-io", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn send(stream: &mut TcpStream, tag: u8, body: &[u8]) { stream.write_all(&Frame { tag, body: Bytes::copy_from_slice(body) }.encode()).await.unwrap(); }

    async fn receive(stream: &mut TcpStream) -> (u8, Vec<u8>) {
        let tag = stream.read_u8().await.unwrap();
        let mut body = vec![0; stream.read_i32().await.unwrap() as usize - 4];
        stream.read_exact(&mut body).await.unwrap();
        (tag, body)
    }

    async fn greet(stream: &mut TcpStream) {
        let mut startup = vec![0; stream.read_i32().await.unwrap() as usize - 4];
        stream.read_exact(&mut startup).await.unwrap();
        send(stream, b'R', &[0, 0, 0, 0]).await;
        send(stream, b'Z', b"I").await;
    }

    /// Reads a query, answering a keying statement and returning its key, or returning the query's text.
    async fn query(stream: &mut TcpStream) -> String {
        let (tag, body) = receive(stream).await;
        assert_eq!(tag, b'Q');
        String::from_utf8(body[..body.len() - 1].to_vec()).unwrap()
    }

    struct Next(tokio::sync::Mutex<Option<TcpStream>>);

    #[async_trait::async_trait]
    impl Router for Next {
        async fn route(&self) -> Result<(TcpStream, Option<String>, u64)> {
            Ok((self.0.lock().await.take().expect("routed once"), None, 0))
        }
    }

    async fn pair(listener: &TcpListener) -> (TcpStream, TcpStream) {
        let near = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
        (near, listener.accept().await.unwrap().0)
    }

    #[tokio::test]
    async fn a_gateway_refuses_a_frame_from_its_header_when_its_budget_cannot_hold_the_body() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut client, gateway) = pair(&listener).await;
        let (first, mut backend) = pair(&listener).await;
        let router = Arc::new(Next(tokio::sync::Mutex::new(None)));
        let memory = MemoryBudget::new(1024);
        let session = Session::new(Bytes::from_static(b"\0\0\0\x08\0\x03\0\0"), router, directory, record.id, None, true)
            .with_frame_memory(Arc::clone(&memory));
        let running = tokio::spawn(session.run(gateway, first, None, 0));

        greet(&mut backend).await;
        assert_eq!(receive(&mut client).await.0, b'R');
        assert_eq!(receive(&mut client).await.0, b'Z');
        client.write_u8(b'Q').await.unwrap();
        client.write_i32(32_768).await.unwrap();
        let (tag, body) = tokio::time::timeout(Duration::from_secs(5), receive(&mut client)).await.unwrap();
        assert_eq!(tag, b'E');
        assert!(body.split(|byte| *byte == 0).any(|field| field == b"C53200"));
        assert_eq!(running.await.unwrap().unwrap_err().code, FRAME_MEMORY);
        assert_eq!(backend.read_u8().await.unwrap_err().kind(), std::io::ErrorKind::UnexpectedEof);
        assert!(memory.peak_bytes() < 1024);
    }

    #[tokio::test]
    async fn a_pipelined_command_retries_under_its_own_key_after_the_previous_command_replies() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut client, gateway) = pair(&listener).await;
        let (first, mut dying) = pair(&listener).await;
        let (second, mut successor) = pair(&listener).await;
        let router = Arc::new(Next(tokio::sync::Mutex::new(Some(second))));
        let session = Session::new(Bytes::from_static(b"\0\0\0\x08\0\x03\0\0"), router, directory, record.id, None, true);
        let running = tokio::spawn(session.run(gateway, first, None, 0));

        tokio::time::timeout(Duration::from_secs(5), async {
            greet(&mut dying).await;
            assert_eq!(receive(&mut client).await.0, b'R');
            assert_eq!(receive(&mut client).await.0, b'Z');
            send(&mut client, b'Q', b"UPDATE counters SET n = n + 1\0").await;
            send(&mut client, b'Q', b"UPDATE counters SET n = n + 2\0").await;

            let first_key = query(&mut dying).await;
            assert!(first_key.starts_with("SET rtdb_idempotency_key = "));
            assert_eq!(query(&mut dying).await, "UPDATE counters SET n = n + 1");
            send(&mut dying, b'C', b"SET\0").await;
            send(&mut dying, b'Z', b"I").await;
            send(&mut dying, b'C', b"UPDATE 1\0").await;
            send(&mut dying, b'Z', b"I").await;

            let second_key = query(&mut dying).await;
            assert!(second_key.starts_with("SET rtdb_idempotency_key = "), "{second_key}");
            assert_ne!(second_key, first_key);
            assert_eq!(query(&mut dying).await, "UPDATE counters SET n = n + 2");
            assert_eq!(receive(&mut client).await, (b'C', b"UPDATE 1\0".to_vec()));
            assert_eq!(receive(&mut client).await.0, b'Z');
            drop(dying);

            greet(&mut successor).await;
            assert_eq!(query(&mut successor).await, second_key);
            assert_eq!(query(&mut successor).await, "UPDATE counters SET n = n + 2");
            send(&mut successor, b'C', b"SET\0").await;
            send(&mut successor, b'Z', b"I").await;
            send(&mut successor, b'C', b"UPDATE 2\0").await;
            send(&mut successor, b'Z', b"I").await;
            assert_eq!(receive(&mut client).await, (b'C', b"UPDATE 2\0".to_vec()));
            assert_eq!(receive(&mut client).await.0, b'Z');
            send(&mut client, b'X', b"").await;
            running.await.unwrap().unwrap();
        }).await.expect("the two pipelined commands finish through reconnect");
    }

    #[tokio::test]
    async fn copy_input_can_stream_before_ready_and_the_next_update_gets_a_fresh_key() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut client, gateway) = pair(&listener).await;
        let (first, mut backend) = pair(&listener).await;
        let router = Arc::new(Next(tokio::sync::Mutex::new(None)));
        let session = Session::new(Bytes::from_static(b"\0\0\0\x08\0\x03\0\0"), router, directory, record.id, None, true);
        let running = tokio::spawn(session.run(gateway, first, None, 0));

        tokio::time::timeout(Duration::from_secs(5), async {
            greet(&mut backend).await;
            assert_eq!(receive(&mut client).await.0, b'R');
            assert_eq!(receive(&mut client).await.0, b'Z');
            send(&mut client, b'Q', b"COPY counters FROM STDIN\0").await;
            let copy_key = query(&mut backend).await;
            assert!(copy_key.starts_with("SET rtdb_idempotency_key = "));
            assert_eq!(query(&mut backend).await, "COPY counters FROM STDIN");
            send(&mut backend, b'C', b"SET\0").await;
            send(&mut backend, b'Z', b"I").await;
            send(&mut backend, b'G', &[0, 0, 1, 0, 0]).await;
            assert_eq!(receive(&mut client).await.0, b'G');

            send(&mut client, b'd', b"1\n").await;
            send(&mut client, b'c', b"").await;
            send(&mut client, b'Q', b"UPDATE counters SET n = n + 1\0").await;
            assert_eq!(receive(&mut backend).await, (b'd', b"1\n".to_vec()));
            assert_eq!(receive(&mut backend).await, (b'c', Vec::new()));
            send(&mut backend, b'C', b"COPY 1\0").await;
            send(&mut backend, b'Z', b"I").await;
            assert_eq!(receive(&mut client).await, (b'C', b"COPY 1\0".to_vec()));
            assert_eq!(receive(&mut client).await.0, b'Z');

            let update_key = query(&mut backend).await;
            assert!(update_key.starts_with("SET rtdb_idempotency_key = "), "{update_key}");
            assert_ne!(update_key, copy_key);
            assert_eq!(query(&mut backend).await, "UPDATE counters SET n = n + 1");
            send(&mut client, b'X', b"").await;
            send(&mut backend, b'C', b"SET\0").await;
            send(&mut backend, b'Z', b"I").await;
            send(&mut backend, b'C', b"UPDATE 1\0").await;
            send(&mut backend, b'Z', b"I").await;
            running.await.unwrap().unwrap();
        }).await.expect("COPY runs to completion before the next update");
    }

    #[tokio::test]
    async fn a_backend_that_accepts_the_connection_but_never_greets_expires_startup() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut client, gateway) = pair(&listener).await;
        let (first, _silent_backend) = pair(&listener).await;
        let router = Arc::new(Next(tokio::sync::Mutex::new(None)));
        let session = Session::new(Bytes::from_static(b"startup"), router, directory, record.id, None, true);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(40);
        let running = tokio::spawn(session.run_before(gateway, first, None, 0, deadline));

        let (tag, body) = tokio::time::timeout(Duration::from_secs(3), receive(&mut client)).await.unwrap();
        assert_eq!(tag, b'E');
        assert!(body.split(|byte| *byte == 0).any(|field| field == b"C57P03"));
        assert_eq!(running.await.unwrap().unwrap_err().code, STARTUP_TIMEOUT);
    }

    #[tokio::test]
    async fn a_placement_change_wakes_a_reconnect_before_its_fallback_timer() {
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        let (changes, mut watched) = watch::channel(Some(record.clone()));
        let original = placement(&watched.borrow());
        let retry = tokio::spawn(async move { wait_for_placement(&mut watched, original, Duration::from_secs(30)).await });
        record.version += 1;
        changes.send_replace(Some(record.clone()));
        tokio::task::yield_now().await;
        assert!(!retry.is_finished());

        record.epoch += 1;
        changes.send_replace(Some(record));
        tokio::time::timeout(Duration::from_secs(5), retry).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_session_keeps_a_route_newer_than_its_directory_watch() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let mut record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (stream, _peer) = pair(&listener).await;
        let backend = Backend::new(stream, 1, MemoryBudget::new(DEFAULT_GATEWAY_FRAME_BYTES));
        let router = Arc::new(Next(tokio::sync::Mutex::new(None)));
        let mut session = Session::new(Bytes::new(), router, directory.clone(), record.id.clone(), None, false);
        drop(session.record.wait_for(|snapshot| snapshot.is_some()).await.unwrap());
        assert!(!session.stale(&backend));

        record.epoch = 2;
        let version = record.version;
        directory.replace_deployment(record, version).await.unwrap().applied().unwrap();
        drop(session.record.wait_for(|snapshot| snapshot.as_ref().is_some_and(|snapshot| snapshot.epoch == 2)).await.unwrap());
        assert!(session.stale(&backend));
    }

    #[tokio::test]
    async fn a_command_whose_backend_goes_away_unanswered_is_resent_under_the_same_key() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut client, gateway_side) = pair(&listener).await;
        let (first, mut dying) = pair(&listener).await;
        let (second, mut successor) = pair(&listener).await;
        let mut startup = 196608i32.to_be_bytes().to_vec();
        startup.extend_from_slice(b"user\0xtdb\0database\0shop\0\0");
        let mut framed = ((startup.len() + 4) as i32).to_be_bytes().to_vec();
        framed.extend_from_slice(&startup);
        let session = Session::new(Bytes::from(framed), Arc::new(Next(tokio::sync::Mutex::new(Some(second)))), directory, record.id.clone(), None, true);
        let running = tokio::spawn(session.run(gateway_side, first, None, 0));

        greet(&mut dying).await;
        assert_eq!(receive(&mut client).await.0, b'R');
        assert_eq!(receive(&mut client).await.0, b'Z');
        send(&mut client, b'Q', b"UPDATE counters SET n = n + 1\0").await;
        let key = query(&mut dying).await;
        assert!(key.starts_with(&format!("SET {} = '", rtdb_node::IDEMPOTENCY_KEY_PARAMETER)), "{key}");
        send(&mut dying, b'C', b"SET\0").await;
        send(&mut dying, b'Z', b"I").await;
        assert_eq!(query(&mut dying).await, "UPDATE counters SET n = n + 1");
        drop(dying);

        greet(&mut successor).await;
        assert_eq!(query(&mut successor).await, key, "the command is resent under the key it was first sent with");
        send(&mut successor, b'C', b"SET\0").await;
        send(&mut successor, b'Z', b"I").await;
        assert_eq!(query(&mut successor).await, "UPDATE counters SET n = n + 1");
        send(&mut successor, b'C', b"UPDATE 1\0").await;
        send(&mut successor, b'Z', b"I").await;
        assert_eq!(receive(&mut client).await, (b'C', b"UPDATE 1\0".to_vec()), "and the client sees only its own command's reply");
        assert_eq!(receive(&mut client).await.0, b'Z');
        drop(client);
        let _ = running.await;
    }

    #[tokio::test]
    async fn an_unanswered_keyed_command_is_replayed_when_the_driver_stops() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut client, gateway) = pair(&listener).await;
        let (first, mut source) = pair(&listener).await;
        let (second, mut target) = pair(&listener).await;
        let mut startup = 196608i32.to_be_bytes().to_vec();
        startup.extend_from_slice(b"user\0xtdb\0database\0shop\0\0");
        let mut framed = ((startup.len() + 4) as i32).to_be_bytes().to_vec();
        framed.extend_from_slice(&startup);
        let session = Session::new(Bytes::from(framed), Arc::new(Next(tokio::sync::Mutex::new(Some(second)))), directory, record.id, None, true);
        let running = tokio::spawn(session.run(gateway, first, None, 0));

        greet(&mut source).await;
        receive(&mut client).await;
        receive(&mut client).await;
        send(&mut client, b'Q', b"UPDATE counters SET n = n + 1\0").await;
        let key = query(&mut source).await;
        send(&mut source, b'C', b"SET\0").await;
        send(&mut source, b'Z', b"I").await;
        assert_eq!(query(&mut source).await, "UPDATE counters SET n = n + 1");
        let stopped = format!("SERROR\0C57014\0MThe replication driver has stopped\0{}xtdb/ingestion-stopped\0\0", char::from(rtdb_pgwire::ANOMALY_CODE_FIELD));
        send(&mut source, b'E', stopped.as_bytes()).await;
        send(&mut source, b'Z', b"I").await;

        greet(&mut target).await;
        assert_eq!(query(&mut target).await, key);
        send(&mut target, b'C', b"SET\0").await;
        send(&mut target, b'Z', b"I").await;
        assert_eq!(query(&mut target).await, "UPDATE counters SET n = n + 1");
        send(&mut target, b'C', b"UPDATE 1\0").await;
        send(&mut target, b'Z', b"I").await;
        assert_eq!(receive(&mut client).await, (b'C', b"UPDATE 1\0".to_vec()));
        assert_eq!(receive(&mut client).await.0, b'Z');
        drop(client);
        running.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_reset_during_dispatch_retries_the_whole_command_under_the_same_key() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut client, gateway_side) = pair(&listener).await;
        let (first, mut dying) = pair(&listener).await;
        let (second, mut successor) = pair(&listener).await;
        let startup = crate::gateway::encode_startup(&BTreeMap::from([("user".into(), "xtdb".into())]));
        let session = Session::new(Bytes::from(startup), Arc::new(Next(tokio::sync::Mutex::new(Some(second)))), directory, record.id, None, true);
        let running = tokio::spawn(session.run(gateway_side, first, None, 0));
        greet(&mut dying).await;
        assert_eq!(receive(&mut client).await.0, b'R');
        assert_eq!(receive(&mut client).await.0, b'Z');

        let mut command = b"UPDATE counters SET n = n + 1".to_vec();
        command.resize(8 << 20, b' ');
        command.push(0);
        send(&mut client, b'Q', &command).await;
        let key = query(&mut dying).await;
        send(&mut dying, b'C', b"SET\0").await;
        send(&mut dying, b'Z', b"I").await;
        assert_eq!(dying.read_u8().await.unwrap(), b'Q');
        dying.set_zero_linger().unwrap();
        drop(dying);

        tokio::select! {
            () = greet(&mut successor) => {}
            (tag, body) = receive(&mut client) => panic!("client received {tag}: {}", String::from_utf8_lossy(&body)),
            () = tokio::time::sleep(std::time::Duration::from_secs(5)) => panic!("gateway did not reconnect"),
        }
        assert_eq!(query(&mut successor).await, key);
        send(&mut successor, b'C', b"SET\0").await;
        send(&mut successor, b'Z', b"I").await;
        let (tag, retried) = receive(&mut successor).await;
        assert_eq!(tag, b'Q');
        assert!(retried == command);
        send(&mut successor, b'C', b"UPDATE 1\0").await;
        send(&mut successor, b'Z', b"I").await;
        assert_eq!(receive(&mut client).await, (b'C', b"UPDATE 1\0".to_vec()));
        assert_eq!(receive(&mut client).await.0, b'Z');
        drop(client);
        running.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_replayed_extended_exchange_flushes_before_the_client_sends_sync() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut client, gateway_side) = pair(&listener).await;
        let (first, mut dying) = pair(&listener).await;
        let (second, mut successor) = pair(&listener).await;
        let startup = crate::gateway::encode_startup(&BTreeMap::from([("user".into(), "xtdb".into())]));
        let session = Session::new(Bytes::from(startup), Arc::new(Next(tokio::sync::Mutex::new(Some(second)))), directory, record.id, None, true);
        let running = tokio::spawn(session.run(gateway_side, first, None, 0));
        greet(&mut dying).await;
        assert_eq!(receive(&mut client).await.0, b'R');
        assert_eq!(receive(&mut client).await.0, b'Z');

        let parse = b"stmt\0SELECT 1\0\0\0";
        send(&mut client, b'P', parse).await;
        send(&mut client, b'H', b"").await;
        let key = query(&mut dying).await;
        send(&mut dying, b'C', b"SET\0").await;
        send(&mut dying, b'Z', b"I").await;
        assert_eq!(receive(&mut dying).await, (b'P', parse.to_vec()));
        assert_eq!(receive(&mut dying).await, (b'H', Vec::new()));
        drop(dying);

        greet(&mut successor).await;
        assert_eq!(query(&mut successor).await, key);
        send(&mut successor, b'C', b"SET\0").await;
        send(&mut successor, b'Z', b"I").await;
        assert_eq!(receive(&mut successor).await, (b'P', parse.to_vec()));
        let flushed = tokio::time::timeout(std::time::Duration::from_secs(5), receive(&mut successor)).await.unwrap();
        assert_eq!(flushed, (b'H', Vec::new()));
        send(&mut successor, b'1', b"").await;
        assert_eq!(receive(&mut client).await, (b'1', Vec::new()));
        send(&mut client, b'S', b"").await;
        assert_eq!(receive(&mut successor).await, (b'S', Vec::new()));
        send(&mut successor, b'Z', b"I").await;
        assert_eq!(receive(&mut client).await.0, b'Z');
        drop(client);
        running.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_command_refused_by_a_backend_lost_before_its_ready_for_query_is_answered_by_the_next() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut client, gateway_side) = pair(&listener).await;
        let (first, mut refusing) = pair(&listener).await;
        let (second, mut successor) = pair(&listener).await;
        let mut startup = 196608i32.to_be_bytes().to_vec();
        startup.extend_from_slice(b"user\0xtdb\0database\0shop\0\0");
        let mut framed = ((startup.len() + 4) as i32).to_be_bytes().to_vec();
        framed.extend_from_slice(&startup);
        let session = Session::new(Bytes::from(framed), Arc::new(Next(tokio::sync::Mutex::new(Some(second)))), directory, record.id.clone(), None, true);
        let running = tokio::spawn(session.run(gateway_side, first, None, 0));
        greet(&mut refusing).await;
        receive(&mut client).await;
        receive(&mut client).await;
        send(&mut client, b'Q', b"UPDATE counters SET n = n + 1\0").await;
        let key = query(&mut refusing).await;
        send(&mut refusing, b'C', b"SET\0").await;
        send(&mut refusing, b'Z', b"I").await;
        query(&mut refusing).await;
        send(&mut refusing, b'E', format!("SERROR\0C57P03\0M{}\0\0", rtdb_node::NOT_HELD).as_bytes()).await;
        drop(refusing);

        greet(&mut successor).await;
        assert_eq!(query(&mut successor).await, key);
        send(&mut successor, b'C', b"SET\0").await;
        send(&mut successor, b'Z', b"I").await;
        assert_eq!(query(&mut successor).await, "UPDATE counters SET n = n + 1");
        send(&mut successor, b'C', b"UPDATE 1\0").await;
        send(&mut successor, b'Z', b"I").await;
        let answer = tokio::time::timeout(std::time::Duration::from_secs(5), receive(&mut client)).await.expect("the successor's answer reaches the client");
        assert_eq!(answer, (b'C', b"UPDATE 1\0".to_vec()), "rather than being discarded as the refused command's");
        drop(client);
        let _ = running.await;
    }

    #[tokio::test]
    async fn a_resent_command_refused_part_way_has_an_unknown_outcome_for_the_lost_attempt_may_have_committed() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut client, gateway_side) = pair(&listener).await;
        let (first, mut crashing) = pair(&listener).await;
        let (second, mut successor) = pair(&listener).await;
        let mut startup = 196608i32.to_be_bytes().to_vec();
        startup.extend_from_slice(b"user\0xtdb\0database\0shop\0\0");
        let mut framed = ((startup.len() + 4) as i32).to_be_bytes().to_vec();
        framed.extend_from_slice(&startup);
        let session = Session::new(Bytes::from(framed), Arc::new(Next(tokio::sync::Mutex::new(Some(second)))), directory, record.id.clone(), None, true);
        let running = tokio::spawn(session.run(gateway_side, first, None, 0));
        greet(&mut crashing).await;
        receive(&mut client).await;
        receive(&mut client).await;
        let command = "BEGIN; UPDATE counters SET n = n + 1; COMMIT";
        send(&mut client, b'Q', format!("{command}\0").as_bytes()).await;
        query(&mut crashing).await;
        send(&mut crashing, b'C', b"SET\0").await;
        send(&mut crashing, b'Z', b"I").await;
        assert_eq!(query(&mut crashing).await, command);
        // It commits, and its replies are lost with it.
        drop(crashing);

        greet(&mut successor).await;
        query(&mut successor).await;
        send(&mut successor, b'C', b"SET\0").await;
        send(&mut successor, b'Z', b"I").await;
        assert_eq!(query(&mut successor).await, command);
        send(&mut successor, b'C', b"BEGIN\0").await;
        send(&mut successor, b'C', b"UPDATE 1\0").await;
        send(&mut successor, b'E', format!("SERROR\0C57P03\0M{}\0\0", rtdb_node::NOT_HELD).as_bytes()).await;
        send(&mut successor, b'Z', b"E").await;
        assert_eq!(receive(&mut client).await.1, b"BEGIN\0");
        assert_eq!(receive(&mut client).await.1, b"UPDATE 1\0");
        let (tag, body) = receive(&mut client).await;
        assert_eq!(tag, b'E');
        assert!(String::from_utf8_lossy(&body).contains("outcome is unknown"), "the client is not told the command failed: {}", String::from_utf8_lossy(&body));
        let _ = running.await;
    }

    fn status(key: &str, value: &str) -> Frame { Frame { tag: b'S', body: Bytes::from(format!("{key}\0{value}\0")) } }
    fn frame(tag: u8, body: &[u8]) -> Frame { Frame { tag, body: Bytes::copy_from_slice(body) } }

    #[test]
    fn a_gateway_vouches_only_for_the_principal_its_backend_authenticated_and_reported_delegable() {
        let greet = |frames: Vec<Frame>| { let (mut greeting, mut vouched) = (Greeting::default(), None); for frame in &frames { greeting.observe(frame, &mut vouched); } vouched };
        let ok = frame(b'R', &[0, 0, 0, 0]);
        assert_eq!(greet(vec![ok.clone(), status("user", "alice"), status(rtdb_pgwire::DELEGABLE_PARAMETER, "on"), frame(b'Z', b"I")]).as_deref(), Some("alice"));
        assert_eq!(greet(vec![ok.clone(), status("user", "alice"), frame(b'Z', b"I")]), None, "a principal whose tokens must be revalidated is never vouched for");
        assert_eq!(greet(vec![status("user", "alice"), status(rtdb_pgwire::DELEGABLE_PARAMETER, "on"), frame(b'Z', b"I")]), None, "nor one never authenticated");
        assert_eq!(greet(vec![ok, status("user", "alice"), status(rtdb_pgwire::DELEGABLE_PARAMETER, "on"), frame(b'Z', b"I"), status("user", "mallory")]).as_deref(), Some("alice"),
            "a user the client sets after the greeting is not the one vouched for");
    }

    #[tokio::test]
    async fn a_command_whose_key_the_backend_refused_is_not_resent() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut client, gateway_side) = pair(&listener).await;
        let (first, mut dying) = pair(&listener).await;
        let (second, _unused) = pair(&listener).await;
        let mut startup = 196608i32.to_be_bytes().to_vec();
        startup.extend_from_slice(b"user\0xtdb\0database\0shop\0\0");
        let mut framed = ((startup.len() + 4) as i32).to_be_bytes().to_vec();
        framed.extend_from_slice(&startup);
        let session = Session::new(Bytes::from(framed), Arc::new(Next(tokio::sync::Mutex::new(Some(second)))), directory, record.id.clone(), None, true);
        let running = tokio::spawn(session.run(gateway_side, first, None, 0));
        greet(&mut dying).await;
        receive(&mut client).await;
        receive(&mut client).await;
        send(&mut client, b'Q', b"UPDATE counters SET n = n + 1\0").await;
        query(&mut dying).await;
        send(&mut dying, b'E', b"SERROR\0C53200\0Mout of memory\0\0").await;
        send(&mut dying, b'Z', b"I").await;
        query(&mut dying).await;
        drop(dying);
        let (tag, body) = receive(&mut client).await;
        assert_eq!(tag, b'E', "the client is told the outcome is unknown, rather than the command being run again unkeyed");
        assert!(String::from_utf8_lossy(&body).contains("outcome is unknown"), "{}", String::from_utf8_lossy(&body));
        let _ = running.await;
    }
}
