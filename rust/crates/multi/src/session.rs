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
use rtdb_pgwire::{GATEWAY_ANSWER_DEADLINE_KIND, GATEWAY_KEY_CONFIRM_KIND, GATEWAY_KEY_KIND, GATEWAY_NAMED_PARSE_KIND, GATEWAY_REPLAY_PERMIT_KIND, GATEWAY_SESSION_STATE_TAG, GATEWAY_SETTINGS_KIND, GATEWAY_SETTINGS_PARAMETER, GATEWAY_SQL_PREPARE_KIND, GatewayReplayMode, GatewayReplayPermit};
use rtdb_storage::sealing::{DeliveryDeadline, DeliveryIo};
use rtdb_types::{Anomaly, AnomalyCategory, MemoryBudget, MemoryReservation, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{mpsc, watch};
use tokio_util::task::AbortOnDropHandle;

const MAXIMUM_FRAME_BYTES: usize = 1 << 30;
pub(crate) const DEFAULT_GATEWAY_FRAME_BYTES: usize = 1 << 30;
const ANSWER_EXPIRED: &str = "rtdb.multi/answer-delivery-expired";
const FRAME_MEMORY: &str = "rtdb.multi/gateway-frame-memory";
pub(crate) const STARTUP_WITHIN: Duration = Duration::from_secs(35);
const STARTUP_TIMEOUT: &str = "rtdb.multi/startup-timeout";
const RECONNECT_WITHIN: Duration = Duration::from_secs(30);
const COMMAND_KEY_AGE: Duration = Duration::from_secs(30 * 60);
const RECONNECT_RETRY_DELAY: Duration = Duration::from_millis(100);
const REPLAY_TIMEOUT: &str = "rtdb.multi/replay-timeout";
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

struct AnswerRelay {
    forward_stamp: bool,
    next: Option<(Frame, DeliveryDeadline)>,
}

impl AnswerRelay {
    fn new(forward_stamp: bool) -> Self { Self { forward_stamp, next: None } }

    fn stamp(&mut self, frame: &Frame) -> Result<bool> {
        if frame.tag != GATEWAY_SESSION_STATE_TAG || frame.body.first() != Some(&GATEWAY_ANSWER_DEADLINE_KIND) {
            return Ok(false);
        }
        let cutoff = frame.body[1..].try_into().ok().map(u64::from_be_bytes)
            .and_then(DeliveryDeadline::from_wire_nanos).ok_or_else(answer_expired)?;
        if self.next.is_some() { return Err(answer_expired()); }
        self.next = Some((frame.clone(), cutoff));
        Ok(true)
    }

    async fn write(&mut self, client: &mut (impl AsyncWrite + Unpin), frame: &Frame) -> Result<()> {
        let pending = self.next.take();
        let send = async {
            if let Some((stamp, _)) = &pending && self.forward_stamp {
                client.write_all(&stamp.encode()).await?;
            }
            client.write_all(&frame.encode()).await?;
            client.flush().await
        };
        if let Some((_, cutoff)) = &pending {
            // A partly written frame poisons the connection, whatever the underlying I/O error.
            cutoff.writing(send).await.map_err(|_| answer_expired())
        } else {
            send.await.map_err(io)
        }
    }
}

/// Preserves the original answer cutoff at the outermost gateway's socket.
pub(crate) async fn relay_cell(
    client: impl AsyncRead + AsyncWrite + Unpin,
    backend: TcpStream,
    memory: &Arc<MemoryBudget>,
    forward_stamp: bool,
) -> Result<()> {
    let (mut requests, mut client) = tokio::io::split(DeliveryIo::new(client));
    let (mut responses, mut backend) = backend.into_split();
    let upload = async {
        tokio::io::copy(&mut requests, &mut backend).await.map_err(io)?;
        backend.shutdown().await.map_err(io)
    };
    let download = async {
        let mut relay = AnswerRelay::new(forward_stamp);
        while let Some(frame) = read_frame(&mut responses, memory).await.map_err(io)? {
            if !relay.stamp(&frame)? { relay.write(&mut client, &frame).await?; }
        }
        if relay.next.is_some() { return Err(answer_expired()); }
        Ok(())
    };
    let mut download = std::pin::pin!(download);
    tokio::select! {
        result = &mut download => result,
        result = upload => { result?; download.await }
    }
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
    /// A connection to an eligible backend, its control address, and its placement epoch.
    async fn route(&self) -> Result<(TcpStream, Option<String>, u64)>;
    async fn route_holder(&self) -> Result<(TcpStream, Option<String>, u64)>;
    /// Keeps a routed backend warm for the duration of its connection.
    fn start_touching(&self, _address: String) -> Option<Arc<dyn Send + Sync>> { None }
}

#[derive(Clone, Copy, Default)]
struct KeyProtocol { guarded: bool, confirmed: bool, replay_permit: bool }

impl KeyProtocol {
    fn observe(&mut self, body: &[u8]) {
        match body {
            [GATEWAY_KEY_KIND] => self.guarded = true,
            [GATEWAY_KEY_CONFIRM_KIND] => self.confirmed = true,
            [GATEWAY_REPLAY_PERMIT_KIND] => self.replay_permit = true,
            _ => {}
        }
    }
}

#[derive(Clone, Copy)]
enum RestoreKey { None, BeforeDispatch, Replay }

impl RestoreKey {
    fn mode(self) -> GatewayReplayMode {
        match self {
            Self::None => GatewayReplayMode::Dispatch,
            Self::BeforeDispatch => GatewayReplayMode::ConfirmDispatch,
            Self::Replay => GatewayReplayMode::Replay,
        }
    }
}

struct CommandKey {
    nonce: uuid::Uuid,
    permit: Option<ReplayCutoff>,
}

struct ReplayCutoff {
    local: tokio::time::Instant,
    writer: rtdb_types::Timestamp,
}

impl CommandKey {
    fn new(first_tag: u8, age: Duration) -> Self {
        let permit = if first_tag == b'Q' {
            let now = rtdb_directory::now_ms().checked_mul(1_000);
            let micros = i64::try_from(age.as_micros()).ok();
            now.zip(micros).and_then(|(now, micros)| crate::replay_admission::cutoff_micros(now, micros)).map(|until| ReplayCutoff {
                local: tokio::time::Instant::now() + age,
                writer: rtdb_types::Timestamp(until),
            })
        } else { None };
        Self { nonce: uuid::Uuid::new_v4(), permit }
    }

    fn replay_frame(&self, mode: GatewayReplayMode) -> Option<Frame> {
        let permit = self.permit.as_ref()?;
        let body = GatewayReplayPermit { nonce: self.nonce, not_after: permit.writer, mode }.encode();
        Some(Frame { tag: GATEWAY_SESSION_STATE_TAG, body: Bytes::copy_from_slice(&body) })
    }
}

fn key_frame(kind: u8, key: &str) -> Frame {
    let body = [&[kind], key.as_bytes()].concat();
    Frame { tag: GATEWAY_SESSION_STATE_TAG, body: Bytes::from(body) }
}

struct Backend {
    writer: OwnedWriteHalf,
    frames: mpsc::Receiver<std::io::Result<Frame>>,
    _reader: AbortOnDropHandle<()>,
    heartbeat: Option<Arc<dyn Send + Sync>>,
    epoch: u64,
    keys: KeyProtocol,
    drain_deadline: Option<tokio::time::Instant>,
}

impl Backend {
    fn new(stream: TcpStream, epoch: u64, memory: Arc<MemoryBudget>) -> Self {
        let (read, writer) = stream.into_split();
        let (frames, reader) = pump(read, memory);
        Self { writer, frames, _reader: reader, heartbeat: None, epoch, keys: KeyProtocol::default(), drain_deadline: None }
    }

    async fn prepare_key(&mut self, key: &CommandKey) -> bool {
        if self.keys.guarded {
            let frame = if self.keys.replay_permit && let Some(frame) = key.replay_frame(GatewayReplayMode::Dispatch) {
                frame
            } else { key_frame(GATEWAY_KEY_KIND, &key.nonce.simple().to_string()) };
            return self.dispatch(&frame).await;
        }
        matches!(tokio::time::timeout(RECONNECT_WITHIN, install_key(self, key, RestoreKey::BeforeDispatch)).await, Ok(Ok(())))
    }

    async fn forward(&mut self, frame: &Frame, deadline: Option<tokio::time::Instant>) -> Result<()> {
        if let Some(deadline) = deadline {
            tokio::time::timeout_at(deadline, self.dispatch(frame)).await.map_err(|_| replay_timeout())?;
        } else {
            self.dispatch(frame).await;
        }
        Ok(())
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
    replay_window: Duration,
    command_key_age: Duration,
    expected_installation: Option<uuid::Uuid>,
    grants: Option<Grants>,
    /// The principal the first backend authenticated and reported it may be vouched for, for whom the gateway mints its reconnections' capabilities.
    authenticated: Option<String>,
    /// What the first backend's greeting has reported so far: its authentication, the session's user, and whether that user may be vouched for.
    greeting: Greeting,
    router: Arc<dyn Router>,
    record: Option<watch::Receiver<Option<DeploymentRecord>>>,
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
    prefer_holder: bool,
    /// Whether the gateway assigns a command key on this route.
    keyed: bool,
    /// The key of the pending command, which began while the session was idle.
    key: Option<CommandKey>,
}

impl Session {
    pub fn new(startup: Bytes, router: Arc<dyn Router>, directory: Arc<dyn Directory>, deployment: String, grants: Option<Grants>, keyed: bool) -> Self {
        let record = rtdb_directory::watch_deployment(directory, deployment, RECONNECT_RETRY_DELAY);
        Self::with_authority(startup, router, Some(record), grants, keyed)
    }

    pub(crate) fn new_ordered(startup: Bytes, router: Arc<dyn Router>, grants: Option<Grants>, keyed: bool) -> Self {
        Self::with_authority(startup, router, None, grants, keyed)
    }

    fn with_authority(startup: Bytes, router: Arc<dyn Router>, record: Option<watch::Receiver<Option<DeploymentRecord>>>, grants: Option<Grants>, keyed: bool) -> Self {
        Self { startup, frame_memory: MemoryBudget::new(DEFAULT_GATEWAY_FRAME_BYTES), replay_window: RECONNECT_WITHIN, command_key_age: COMMAND_KEY_AGE, expected_installation: None, grants, authenticated: None, greeting: Greeting::default(), router, record, prepared: BTreeMap::new(), next_statement_order: 0, await_token: None, settings: None, status: Status::Busy, pending: Vec::new(), prepared_responses: 0, answered: false, lost_unanswered: false, prefer_holder: false, keyed, key: None }
    }

    pub(crate) fn with_frame_memory(mut self, memory: Arc<MemoryBudget>) -> Self {
        self.frame_memory = memory;
        self
    }

    pub(crate) fn with_installation(mut self, installation: Option<uuid::Uuid>) -> Self {
        self.expected_installation = installation;
        self
    }

    /// Reconnects to the current holder, retrying through a failover: until the dead holder's lease expires, no other node may claim.
    async fn connect(&mut self, supersede: Option<u64>, restore_key: RestoreKey) -> Result<Backend> {
        let result = tokio::time::timeout(RECONNECT_WITHIN, async {
            loop {
                let previous = self.record.as_ref().and_then(|record| placement(&record.borrow()));
                let attempt = async {
                    let mut backend = self.connect_once(supersede).await?;
                    if !matches!(restore_key, RestoreKey::None) && let Some(key) = &self.key {
                        if matches!(restore_key, RestoreKey::Replay) && (!backend.keys.confirmed || key.permit.is_some() && !backend.keys.replay_permit) {
                            return Err(Anomaly::unsupported("rtdb.multi/replay-key-unsupported", "The replacement writer cannot protect a replay key"));
                        }
                        install_key(&mut backend, key, restore_key).await?;
                    }
                    Ok::<_, Anomaly>(backend)
                }.await;
                match attempt {
                    Ok(backend) => return Ok(backend),
                    Err(error) if !matches!(error.category, AnomalyCategory::Unavailable | AnomalyCategory::Conflict) => return Err(error),
                    Err(_) => wait_for_placement(self.record.as_mut(), previous, RECONNECT_RETRY_DELAY).await,
                }
            }
        }).await.unwrap_or_else(|_| Err(lost("timed out reconnecting to the holder")));
        result.map_err(|error| {
            if self.lost_unanswered || (matches!(restore_key, RestoreKey::Replay) && self.key.is_some()) {
                lost(&format!("{}; the command's outcome is unknown", error.message.as_deref().unwrap_or(&error.code)))
            } else {
                error
            }
        })
    }

    async fn connect_once(&mut self, supersede: Option<u64>) -> Result<Backend> {
        let (mut stream, control, epoch) = if self.prefer_holder { self.router.route_holder().await? } else { self.router.route().await? };
        if supersede.is_some_and(|previous| epoch <= previous) {
            return Err(lost("routing still points to the holder that closed storage"));
        }
        stream.write_all(&self.reconnection_startup()).await.map_err(io)?;
        let mut backend = Backend::new(stream, epoch, Arc::clone(&self.frame_memory));
        let features = replay_ready(&mut backend.frames).await?;
        backend.keys = features.keys;
        if self.expected_installation.is_some_and(|expected| features.installation != Some(expected)) {
            return Err(Anomaly::incorrect("rtdb.multi/install-identity", "The replacement holder did not verify the requested installation"));
        }
        if self.await_token.is_some() && !features.await_token {
            return Err(Anomaly::new(AnomalyCategory::Unsupported, "rtdb.multi/reconnect-basis", "The new holder cannot restore the session's await token"));
        }
        if self.settings.is_some() && !features.settings {
            return Err(Anomaly::new(AnomalyCategory::Unsupported, "rtdb.multi/reconnect-settings", "The new holder cannot restore the session's settings"));
        }
        let mut definitions: Vec<_> = self.prepared.values().collect();
        if definitions.iter().any(|definition| definition.frame.tag == GATEWAY_SESSION_STATE_TAG && match definition.frame.body.first().copied() {
            Some(GATEWAY_SQL_PREPARE_KIND) => !features.sql_prepare,
            Some(GATEWAY_NAMED_PARSE_KIND) => !features.named_parse,
            _ => false,
        }) {
            return Err(Anomaly::new(AnomalyCategory::Unsupported, "rtdb.multi/reconnect-prepared", "The new holder cannot restore the session's prepared definitions"));
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
        self.record.as_ref().is_some_and(|watch| watch.borrow().as_ref().is_some_and(|record| record.epoch > backend.epoch))
    }

    /// Runs the session over an already-routed first backend whose greeting the client has not yet seen.
    pub async fn run(self, client: impl AsyncRead + AsyncWrite + Unpin + Send + 'static, first: TcpStream, first_control: Option<String>, first_epoch: u64) -> Result<()> {
        self.run_before(client, first, first_control, first_epoch, tokio::time::Instant::now() + STARTUP_WITHIN, false).await
    }

    pub(crate) async fn run_before(mut self, client: impl AsyncRead + AsyncWrite + Unpin + Send + 'static, first: TcpStream, first_control: Option<String>, first_epoch: u64,
                                   startup_deadline: tokio::time::Instant, forward_stamp: bool) -> Result<()> {
        let (client_read, mut client_write) = tokio::io::split(DeliveryIo::new(client));
        let mut answers = AnswerRelay::new(forward_stamp);
        let (mut requests, mut client_reader) = pump(client_read, Arc::clone(&self.frame_memory));
        let mut copying_in = false;
        let mut replay_deadline = None;
        enum Refusal { Placement, Replica(Frame) }
        let mut refusal = None;
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
                        _ = wait_for_retirement(self.record.as_mut()) => Event::Retired,
                        _ = tokio::time::sleep_until(startup_deadline), if !self.greeting.done => return Err(startup_timeout()),
                        _ = wait_until(replay_deadline) => return Err(replay_timeout()),
                        _ = wait_until(answers.next.as_ref().map(|(_, cutoff)| cutoff.instant().into())) => return Err(answer_expired()),
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
                            if let Some(current) = backend.as_mut() { current.forward(&request, replay_deadline).await?; }
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
                            backend.as_mut().expect("connected").forward(&request, replay_deadline).await?;
                            if self.pending.last().is_some_and(|frame| !matches!(frame.tag, b'Q' | b'S' | b'H')) {
                                self.pending.push(request);
                            }
                            continue;
                        }
                        let starts = self.status == Status::Idle;
                        if starts { self.answered = false; self.lost_unanswered = false; self.key = self.keyed.then(|| CommandKey::new(request.tag, self.command_key_age)); }
                        if starts && backend.as_ref().is_some_and(|current| self.stale(current)) { backend = None; }
                        if backend.is_none() { backend = Some(self.connect(None, RestoreKey::None).await?); }
                        if starts && let Some(key) = &self.key
                            && !backend.as_mut().expect("connected").prepare_key(key).await
                        {
                            backend = None;
                        }
                        if backend.is_none() { backend = Some(self.connect(None, RestoreKey::BeforeDispatch).await?); }
                        backend.as_mut().expect("connected").forward(&request, replay_deadline).await?;
                        self.pending.push(request);
                        self.status = Status::Busy;
                    }
                    Event::Reply(Some(Ok(frame))) => {
                        if answers.stamp(&frame)? { continue; }
                        if frame.tag == GATEWAY_SESSION_STATE_TAG {
                            if !self.greeting.done {
                                backend.as_mut().expect("greeting backend").keys.observe(&frame.body);
                            }
                            if self.greeting.done && refusal.is_none() {
                                match frame.body.first().copied() {
                                    Some(0) => self.await_token = None,
                                    Some(1) => self.await_token = Some(frame),
                                    Some(GATEWAY_SETTINGS_KIND) if frame.body.len() > 1 => self.settings = Some(frame),
                                    Some(GATEWAY_SQL_PREPARE_KIND | GATEWAY_NAMED_PARSE_KIND) if frame.body.len() > 1 => {
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
                            if let Some((name, value)) = parameter_status(&frame) {
                                if name == rtdb_pgwire::VERIFIED_INSTALLATION_PARAMETER {
                                    self.greeting.installation = uuid::Uuid::parse_str(value).ok();
                                    continue;
                                }
                                if matches!(name, GATEWAY_SETTINGS_PARAMETER | rtdb_pgwire::GATEWAY_REPLICA_READ_PARAMETER | rtdb_pgwire::REQUIRED_INSTALLATION_PARAMETER) { continue; }
                            }
                            if frame.tag == b'Z' && self.expected_installation.is_some_and(|expected| self.greeting.installation != Some(expected)) {
                                client_write.write_all(&error_frame("3D000", "The holder did not verify the requested installation").encode()).await.map_err(io)?;
                                client_write.flush().await.map_err(io)?;
                                break Ok(());
                            }
                            self.greeting.observe(&frame, &mut self.authenticated);
                            tokio::time::timeout_at(startup_deadline, async {
                                if self.greeting.done {
                                    self.status = Status::Idle;
                                    if let Some(installation) = self.expected_installation {
                                        let mut body = BytesMut::from(&b"rtdb_installation_id\0"[..]);
                                        body.extend_from_slice(installation.simple().to_string().as_bytes());
                                        body.put_u8(0);
                                        client_write.write_all(&Frame { tag: b'S', body: body.freeze() }.encode()).await.map_err(io)?;
                                    }
                                }
                                client_write.write_all(&frame.encode()).await.map_err(io)?;
                                client_write.flush().await.map_err(io)?;
                                Ok::<_, Anomaly>(())
                            }).await.map_err(|_| startup_timeout())??;
                            if frame.tag == b'E' { break Ok(()); }
                            continue;
                        }
                        if !self.pending.is_empty() && refusal.is_none() && frame.tag == b'E' {
                            let code = backend_error_code(&frame);
                            if matches!(code, Some("xtdb/ingestion-stopped" | "rtdb.storage/closed")) {
                                if self.answered || !backend.as_ref().is_some_and(|current| self.replayable(current.keys)) {
                                    break Err(lost("the holder stopped during the command, whose outcome is unknown"));
                                }
                                self.lost_unanswered = true;
                                let deadline = self.start_replay(&mut replay_deadline);
                                if code == Some("rtdb.storage/closed") {
                                    let epoch = backend.take().expect("connected").epoch;
                                    backend = Some(self.replay(Some(epoch), deadline).await?);
                                    continue;
                                }
                                refusal = Some(Refusal::Placement);
                            }
                        }
                        if !self.pending.is_empty() && !self.answered && refusal.is_none() && frame.tag == b'E' && not_held(&frame) {
                            if self.keyed && !backend.as_ref().is_some_and(|current| self.replayable(current.keys)) {
                                break Err(lost("the command cannot be safely replayed; its outcome is unknown"));
                            }
                            self.lost_unanswered = self.keyed;
                            self.start_replay(&mut replay_deadline);
                            refusal = Some(Refusal::Placement);
                        }
                        if !self.keyed && !self.pending.is_empty() && !self.answered && refusal.is_none()
                            && frame.tag == b'E' && backend_error_code(&frame) == Some(rtdb_node::REPLICA_BEHIND)
                        {
                            self.start_replay(&mut replay_deadline);
                            refusal = Some(Refusal::Replica(frame));
                            continue;
                        }
                        if let Some(rejected) = refusal.take() {
                            if frame.tag != b'Z' {
                                refusal = Some(rejected);
                                continue;
                            }
                            match rejected {
                                Refusal::Replica(error) if frame.body.first() != Some(&b'I') => {
                                    client_write.write_all(&error.encode()).await.map_err(io)?;
                                }
                                rejected => {
                                    if self.keyed && !backend.as_ref().is_some_and(|current| self.replayable(current.keys)) {
                                        break Err(lost("the command cannot be safely replayed; its outcome is unknown"));
                                    }
                                    self.prefer_holder |= matches!(rejected, Refusal::Replica(_));
                                    backend = Some(self.replay(None, replay_deadline.expect("a refused command has a deadline")).await?);
                                    continue;
                                }
                            }
                        }
                        if self.lost_unanswered && frame.tag == b'E' {
                            break Err(lost("the command's outcome is unknown because its holder stopped and its successor could not confirm it"));
                        }
                        self.answered = true;
                        replay_deadline = None;
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
                        answers.write(&mut client_write, &frame).await?;
                    }
                    Event::Reply(Some(Err(error))) if error.kind() == std::io::ErrorKind::OutOfMemory => {
                        if self.status != Status::Idle && self.greeting.done {
                            break Err(lost("the holder's answer exceeded the gateway's frame memory budget and the command's outcome is unknown"));
                        }
                        break Err(io(error));
                    }
                    Event::Reply(Some(Err(_)) | None) => {
                        if answers.next.is_some() { return Err(answer_expired()); }
                        if !self.greeting.done { break Err(lost("the holder closed during session startup")); }
                        let protected = backend.take().is_some_and(|current| self.replayable(current.keys));
                        // A refusal whose ReadyForQuery never came is over with its backend: the resent command's replies are the client's.
                        if matches!(refusal.take(), Some(Refusal::Replica(_))) {
                            break Err(lost("the replica closed before confirming the transaction state"));
                        }
                        if self.status != Status::Idle {
                            if !protected || self.answered {
                                break Err(lost("the holder went away during a command, whose outcome is unknown"));
                            }
                            self.lost_unanswered = true;
                            let deadline = self.start_replay(&mut replay_deadline);
                            backend = Some(self.replay(None, deadline).await?);
                            continue;
                        }
                        backend = Some(self.connect(None, RestoreKey::None).await?);
                    }
                }
            }
        }.await;
        if let Err(error) = &outcome && error.code != ANSWER_EXPIRED {
            let code = match error.code.as_ref() {
                STARTUP_TIMEOUT => "57P03",
                FRAME_MEMORY => "53200",
                _ => "08006",
            };
            let reply = error_frame(code, error.message.as_deref().unwrap_or(error.code.as_ref())).encode();
            let send_error = async {
                client_write.write_all(&reply).await?;
                client_write.flush().await
            };
            if matches!(error.code.as_ref(), STARTUP_TIMEOUT | REPLAY_TIMEOUT) {
                let _ = tokio::time::timeout(Duration::from_millis(100), send_error).await;
            } else {
                let _ = send_error.await;
            }
        }
        outcome
    }

    fn replayable(&self, keys: KeyProtocol) -> bool {
        let Some(key) = &self.key else { return false };
        if !keys.confirmed || self.pending.is_empty() { return false; }
        if matches!(self.pending.as_slice(), [frame] if frame.tag == b'Q') {
            let before_cutoff = key.permit.as_ref().is_some_and(|permit| tokio::time::Instant::now() < permit.local);
            crate::replay_admission::may_send_simple_replay(keys.replay_permit, before_cutoff)
        } else {
            self.pending.iter().all(|frame| crate::replay_admission::is_setup_frame(frame.tag))
        }
    }

    fn start_replay(&self, deadline: &mut Option<tokio::time::Instant>) -> tokio::time::Instant {
        let proposed = tokio::time::Instant::now() + self.replay_window;
        let proposed = self.key.as_ref().and_then(|key| key.permit.as_ref()).map_or(proposed, |permit| proposed.min(permit.local));
        let cutoff = crate::replay_deadline::replay_cutoff(*deadline, proposed);
        *deadline = Some(cutoff);
        cutoff
    }

    async fn replay(&mut self, supersede: Option<u64>, deadline: tokio::time::Instant) -> Result<Backend> {
        tokio::time::timeout_at(deadline, async {
            let mut backend = self.connect(supersede, RestoreKey::Replay).await?;
            for request in &self.pending {
                if !backend.dispatch(request).await { break; }
            }
            Ok(backend)
        }).await.map_err(|_| replay_timeout())?
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
struct ReplayFeatures { await_token: bool, settings: bool, sql_prepare: bool, named_parse: bool, keys: KeyProtocol, installation: Option<uuid::Uuid> }

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
                Some(GATEWAY_NAMED_PARSE_KIND) => features.named_parse = true,
                Some(GATEWAY_KEY_KIND | GATEWAY_KEY_CONFIRM_KIND | GATEWAY_REPLAY_PERMIT_KIND) => features.keys.observe(&frame.body),
                _ => {}
            },
            b'S' => {
                if let Some((rtdb_pgwire::VERIFIED_INSTALLATION_PARAMETER, value)) = parameter_status(&frame) {
                    features.installation = uuid::Uuid::parse_str(value).ok();
                }
            }
            b'Z' => return Ok(features),
            _ => {}
        }
    }
}

async fn install_key(backend: &mut Backend, key: &CommandKey, restore: RestoreKey) -> Result<()> {
    let frame = if backend.keys.replay_permit && let Some(frame) = key.replay_frame(restore.mode()) {
        frame
    } else if backend.keys.confirmed {
        key_frame(GATEWAY_KEY_CONFIRM_KIND, &key.nonce.simple().to_string())
    } else {
        let key = key.nonce.simple();
        let mut body = format!("SET {} = '{key}'", rtdb_node::IDEMPOTENCY_KEY_PARAMETER).into_bytes();
        body.push(0);
        Frame { tag: b'Q', body: Bytes::from(body) }
    };
    if !backend.dispatch(&frame).await {
        return Err(lost("the holder closed while installing the idempotency key"));
    }
    replay_ready(&mut backend.frames).await.map_err(|error| {
        if error.code == "rtdb.multi/reconnect-refused" {
            Anomaly::incorrect("rtdb.multi/key-refused", "the holder refused to install the idempotency key")
        } else {
            error
        }
    })?;
    Ok(())
}

fn parameter_status(frame: &Frame) -> Option<(&str, &str)> {
    if frame.tag != b'S' { return None; }
    let mut fields = frame.body.split(|byte| *byte == 0);
    Some((std::str::from_utf8(fields.next()?).ok()?, std::str::from_utf8(fields.next()?).ok()?))
}

fn placement(record: &Option<DeploymentRecord>) -> Option<(u64, Lifecycle)> {
    record.as_ref().map(|record| (record.epoch, record.lifecycle))
}

async fn wait_for_retirement(record: Option<&mut watch::Receiver<Option<DeploymentRecord>>>) {
    let Some(record) = record else { return std::future::pending().await; };
    let _ = record.wait_for(|record| record.as_ref().is_some_and(|record| {
        matches!(record.lifecycle, Lifecycle::Deleting | Lifecycle::Restoring)
            || record.lifecycle == Lifecycle::Deleted && record.moved_at_ms.is_none()
    })).await;
}

async fn wait_for_placement(record: Option<&mut watch::Receiver<Option<DeploymentRecord>>>, before: Option<(u64, Lifecycle)>, delay: Duration) {
    let Some(record) = record else { return tokio::time::sleep(delay).await; };
    let retry_at = tokio::time::Instant::now() + delay;
    let watch_closed = tokio::time::timeout_at(retry_at, record.wait_for(|current| placement(current) != before)).await
        .is_ok_and(|update| update.is_err());
    if watch_closed { tokio::time::sleep_until(retry_at).await; }
}

/// Whether an ErrorResponse reports that the node no longer holds the placement.
fn not_held(frame: &Frame) -> bool {
    backend_anomaly_code(frame).map_or_else(
        || frame.body.windows(rtdb_node::NOT_HELD.len()).any(|window| window == rtdb_node::NOT_HELD.as_bytes()),
        |code| code == rtdb_node::NOT_HELD.as_bytes(),
    )
}

fn backend_anomaly_code(frame: &Frame) -> Option<&[u8]> {
    frame.body.split(|byte| *byte == 0).find_map(|field| field.strip_prefix(&[rtdb_pgwire::ANOMALY_CODE_FIELD]))
}

fn backend_error_code(frame: &Frame) -> Option<&str> {
    #[derive(serde::Deserialize)]
    struct ErrorDetail<'a> { code: &'a str }
    if let Some(code) = backend_anomaly_code(frame) { return std::str::from_utf8(code).ok(); }
    frame.body.split(|byte| *byte == 0).find_map(|field| field.strip_prefix(b"D"))
        .and_then(|detail| serde_json::from_slice::<ErrorDetail<'_>>(detail).ok()).map(|detail| detail.code)
}

fn parse_name(frame: &Frame) -> String {
    frame.body.split(|byte| *byte == 0).next().map(|name| String::from_utf8_lossy(name).into_owned()).unwrap_or_default()
}

/// What the first backend's greeting, everything before its first ReadyForQuery, has reported.
/// Only the greeting counts: a parameter status later in the session reports a `SET`, which the client chose.
#[derive(Default)]
struct Greeting { done: bool, authenticated: bool, user: Option<String>, delegable: bool, installation: Option<uuid::Uuid> }

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
fn answer_expired() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, ANSWER_EXPIRED, "The PostgreSQL answer did not reach its socket within its delivery grant")
}
fn replay_timeout() -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, REPLAY_TIMEOUT, "Timed out recovering the command, whose outcome is unknown") }

async fn wait_until(deadline: Option<tokio::time::Instant>) {
    if let Some(deadline) = deadline {
        tokio::time::sleep_until(deadline).await;
    } else {
        std::future::pending::<()>().await;
    }
}

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

    #[tokio::test]
    async fn relay_preserves_the_sender_cutoff_and_consumes_it_on_the_public_socket() {
        let cutoff = DeliveryDeadline::answer_window().wire_nanos().unwrap();
        let mut body = BytesMut::new();
        body.put_u8(GATEWAY_ANSWER_DEADLINE_KIND);
        body.put_u64(cutoff);
        let stamp = Frame { tag: GATEWAY_SESSION_STATE_TAG, body: body.freeze() };
        let row = Frame { tag: b'D', body: Bytes::from_static(b"data") };
        for forward in [true, false] {
            let mut relay = AnswerRelay::new(forward);
            assert!(relay.stamp(&stamp).unwrap());
            let mut wire = Vec::new();
            relay.write(&mut DeliveryIo::new(&mut wire), &row).await.unwrap();
            let expected = if forward { [stamp.encode(), row.encode()].concat() } else { row.encode().to_vec() };
            assert_eq!(wire, expected);
        }

        let mut expired = BytesMut::new();
        expired.put_u8(GATEWAY_ANSWER_DEADLINE_KIND);
        expired.put_u64(0);
        let expired = Frame { tag: GATEWAY_SESSION_STATE_TAG, body: expired.freeze() };
        let mut relay = AnswerRelay::new(false);
        assert_eq!(relay.stamp(&expired).unwrap_err().code, ANSWER_EXPIRED);
    }

    #[tokio::test]
    async fn an_installed_route_has_no_directory_retirement_signal() {
        assert!(tokio::time::timeout(Duration::from_millis(5), wait_for_retirement(None)).await.is_err());
    }

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
        send(stream, GATEWAY_SESSION_STATE_TAG, &[GATEWAY_KEY_CONFIRM_KIND]).await;
        send(stream, GATEWAY_SESSION_STATE_TAG, &[GATEWAY_REPLAY_PERMIT_KIND]).await;
        send(stream, b'Z', b"I").await;
    }

    async fn query(stream: &mut TcpStream) -> String {
        let (tag, body) = receive(stream).await;
        if tag == GATEWAY_SESSION_STATE_TAG {
            if let Some(permit) = GatewayReplayPermit::decode(&body) {
                return format!("SET {} = '{}@{}'", rtdb_node::IDEMPOTENCY_KEY_PARAMETER, permit.nonce.simple(), permit.not_after.0);
            }
            assert_eq!(body[0], GATEWAY_KEY_CONFIRM_KIND);
            let key = std::str::from_utf8(&body[1..]).unwrap();
            return format!("SET {} = '{key}'", rtdb_node::IDEMPOTENCY_KEY_PARAMETER);
        }
        assert_eq!(tag, b'Q');
        String::from_utf8(body[..body.len() - 1].to_vec()).unwrap()
    }

    struct Next(tokio::sync::Mutex<Option<TcpStream>>);

    #[async_trait::async_trait]
    impl Router for Next {
        async fn route(&self) -> Result<(TcpStream, Option<String>, u64)> {
            Ok((self.0.lock().await.take().expect("routed once"), None, 0))
        }
        async fn route_holder(&self) -> Result<(TcpStream, Option<String>, u64)> { self.route().await }
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
            send(&mut dying, b'C', b"SET\0").await;
            send(&mut dying, b'Z', b"I").await;
            assert_eq!(query(&mut dying).await, "UPDATE counters SET n = n + 1");
            send(&mut dying, b'C', b"UPDATE 1\0").await;
            send(&mut dying, b'Z', b"I").await;

            let second_key = query(&mut dying).await;
            assert!(second_key.starts_with("SET rtdb_idempotency_key = "), "{second_key}");
            assert_ne!(second_key, first_key);
            send(&mut dying, b'C', b"SET\0").await;
            send(&mut dying, b'Z', b"I").await;
            assert_eq!(query(&mut dying).await, "UPDATE counters SET n = n + 2");
            assert_eq!(receive(&mut client).await, (b'C', b"UPDATE 1\0".to_vec()));
            assert_eq!(receive(&mut client).await.0, b'Z');
            drop(dying);

            greet(&mut successor).await;
            assert_eq!(query(&mut successor).await, second_key);
            send(&mut successor, b'C', b"SET\0").await;
            send(&mut successor, b'Z', b"I").await;
            assert_eq!(query(&mut successor).await, "UPDATE counters SET n = n + 2");
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
            send(&mut backend, b'C', b"SET\0").await;
            send(&mut backend, b'Z', b"I").await;
            assert_eq!(query(&mut backend).await, "COPY counters FROM STDIN");
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
            send(&mut backend, b'C', b"SET\0").await;
            send(&mut backend, b'Z', b"I").await;
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
        let running = tokio::spawn(session.run_before(gateway, first, None, 0, deadline, false));

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
        let retry = tokio::spawn(async move { wait_for_placement(Some(&mut watched), original, Duration::from_secs(30)).await });
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
        drop(session.record.as_mut().unwrap().wait_for(|snapshot| snapshot.is_some()).await.unwrap());
        assert!(!session.stale(&backend));

        record.epoch = 2;
        let version = record.version;
        directory.replace_deployment(record, version).await.applied().unwrap();
        drop(session.record.as_mut().unwrap().wait_for(|snapshot| snapshot.as_ref().is_some_and(|snapshot| snapshot.epoch == 2)).await.unwrap());
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
    async fn an_execute_received_while_draining_a_refusal_is_never_replayed_as_setup() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut client, gateway) = pair(&listener).await;
        let (first, mut refusing) = pair(&listener).await;
        let router = Arc::new(Next(tokio::sync::Mutex::new(None)));
        let startup = Bytes::from_static(b"\0\0\0\x08\0\x03\0\0");
        let session = Session::new(startup, router, directory, record.id, None, true);
        let running = tokio::spawn(session.run(gateway, first, None, 0));

        tokio::time::timeout(Duration::from_secs(5), async {
            greet(&mut refusing).await;
            assert_eq!(receive(&mut client).await.0, b'R');
            assert_eq!(receive(&mut client).await.0, b'Z');
            let parse = b"stmt\0UPDATE counters SET n = n + 1\0\0\0";
            send(&mut client, b'P', parse).await;
            assert!(query(&mut refusing).await.starts_with("SET rtdb_idempotency_key"));
            send(&mut refusing, b'Z', b"I").await;
            assert_eq!(receive(&mut refusing).await, (b'P', parse.to_vec()));
            send(&mut refusing, b'E', b"SERROR\0C57P03\0yrtdb.node/database-not-held\0\0").await;
            send(&mut client, b'E', b"\0\0\0\0\0").await;
            assert_eq!(receive(&mut refusing).await.0, b'E');
            send(&mut refusing, b'Z', b"I").await;

            let (tag, body) = receive(&mut client).await;
            assert_eq!(tag, b'E');
            assert!(String::from_utf8_lossy(&body).contains("outcome is unknown"));
            assert!(running.await.unwrap().is_err());
        }).await.unwrap();
    }

    #[tokio::test]
    async fn a_writer_that_advertises_guarded_keys_gets_the_key_and_command_without_an_acknowledgement() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut client, gateway) = pair(&listener).await;
        let (first, mut writer) = pair(&listener).await;
        let router = Arc::new(Next(tokio::sync::Mutex::new(None)));
        let startup = Bytes::from_static(b"\0\0\0\x08\0\x03\0\0");
        let session = Session::new(startup, router, directory, record.id, None, true);
        let running = tokio::spawn(session.run(gateway, first, None, 0));

        tokio::time::timeout(Duration::from_secs(5), async {
            writer.read_exact(&mut [0; 8]).await.unwrap();
            send(&mut writer, b'R', &[0; 4]).await;
            send(&mut writer, GATEWAY_SESSION_STATE_TAG, &[GATEWAY_KEY_KIND]).await;
            send(&mut writer, b'Z', b"I").await;
            assert_eq!(receive(&mut client).await.0, b'R');
            assert_eq!(receive(&mut client).await.0, b'Z');

            send(&mut client, b'Q', b"UPDATE counters SET n = n + 1\0").await;
            let (tag, key) = receive(&mut writer).await;
            assert_eq!(tag, GATEWAY_SESSION_STATE_TAG);
            assert_eq!(key[0], GATEWAY_KEY_KIND);
            assert_eq!(key.len(), 33);
            assert_eq!(query(&mut writer).await, "UPDATE counters SET n = n + 1");

            send(&mut writer, b'C', b"UPDATE 1\0").await;
            send(&mut writer, b'Z', b"I").await;
            assert_eq!(receive(&mut client).await, (b'C', b"UPDATE 1\0".to_vec()));
            assert_eq!(receive(&mut client).await.0, b'Z');
            drop(client);
            running.await.unwrap().unwrap();
        }).await.unwrap();
    }

    #[tokio::test]
    async fn a_command_refused_by_a_backend_lost_before_its_ready_for_query_is_answered_by_the_next() {
        assert_retry_after_backend_error(BackendFailure::NotHeld { replacement: ReplacementKey::Accept }).await;
    }

    #[tokio::test]
    async fn a_storage_close_during_handoff_retries_the_unanswered_command_with_its_original_key() {
        assert_retry_after_backend_error(BackendFailure::StorageClosed { replacement: ReplacementKey::Accept }).await;
    }

    #[tokio::test]
    async fn a_replacement_that_refuses_the_key_does_not_receive_the_pending_write() {
        assert_retry_after_backend_error(BackendFailure::StorageClosed { replacement: ReplacementKey::Refuse }).await;
    }

    #[tokio::test]
    async fn a_pending_command_times_out_across_successive_replacements() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut client, gateway) = pair(&listener).await;
        let (first, mut dying) = pair(&listener).await;
        let (second, mut replacement) = pair(&listener).await;
        let (third, mut stalled) = pair(&listener).await;
        let router = Arc::new(Routes(tokio::sync::Mutex::new([(second, 1), (third, 2)].into())));
        let startup = Bytes::from_static(b"\0\0\0\x08\0\x03\0\0");
        let mut session = Session::new(startup, router, directory, record.id, None, true);
        session.replay_window = Duration::from_secs(2);
        let running = tokio::spawn(session.run(gateway, first, None, 0));

        greet(&mut dying).await;
        assert_eq!(receive(&mut client).await.0, b'R');
        assert_eq!(receive(&mut client).await.0, b'Z');
        let command = "UPDATE counters SET n = n + 1";
        send(&mut client, b'Q', format!("{command}\0").as_bytes()).await;
        let key = query(&mut dying).await;
        send(&mut dying, b'C', b"SET\0").await;
        send(&mut dying, b'Z', b"I").await;
        assert_eq!(query(&mut dying).await, command);
        drop(dying);

        let (tag, error) = tokio::time::timeout(Duration::from_secs(5), async {
            greet(&mut replacement).await;
            assert_eq!(query(&mut replacement).await, key);
            send(&mut replacement, b'C', b"SET\0").await;
            send(&mut replacement, b'Z', b"I").await;
            assert_eq!(query(&mut replacement).await, command);
            tokio::time::sleep(Duration::from_millis(1250)).await;
            drop(replacement);
            tokio::time::timeout(Duration::from_millis(1300), async {
                greet(&mut stalled).await;
                assert_eq!(query(&mut stalled).await, key);
                send(&mut stalled, b'C', b"SET\0").await;
                send(&mut stalled, b'Z', b"I").await;
                assert_eq!(query(&mut stalled).await, command);
                receive(&mut client).await
            }).await.expect("the third writer must inherit the first recovery cutoff")
        }).await.unwrap();
        assert_eq!(tag, b'E');
        assert!(error.windows(b"C08006".len()).any(|field| field == b"C08006"));
        assert!(String::from_utf8_lossy(&error).contains("outcome is unknown"));
        assert_eq!(running.await.unwrap().unwrap_err().code, REPLAY_TIMEOUT);
    }

    #[tokio::test]
    async fn a_writer_advertising_the_old_permit_cannot_originate_a_replay() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut client, gateway) = pair(&listener).await;
        let (first, mut legacy) = pair(&listener).await;
        let router = Arc::new(Next(tokio::sync::Mutex::new(None)));
        let startup = Bytes::from_static(b"\0\0\0\x08\0\x03\0\0");
        let session = Session::new(startup, router, directory, record.id, None, true);
        let running = tokio::spawn(session.run(gateway, first, None, 0));

        tokio::time::timeout(Duration::from_secs(5), async {
            legacy.read_exact(&mut [0; 8]).await.unwrap();
            send(&mut legacy, b'R', &[0; 4]).await;
            send(&mut legacy, GATEWAY_SESSION_STATE_TAG, &[GATEWAY_KEY_CONFIRM_KIND]).await;
            send(&mut legacy, GATEWAY_SESSION_STATE_TAG, &[7]).await;
            send(&mut legacy, b'Z', b"I").await;
            assert_eq!(receive(&mut client).await.0, b'R');
            assert_eq!(receive(&mut client).await.0, b'Z');

            let command = "UPDATE counters SET n = n + 1";
            send(&mut client, b'Q', format!("{command}\0").as_bytes()).await;
            assert!(query(&mut legacy).await.starts_with("SET rtdb_idempotency_key"));
            send(&mut legacy, b'C', b"SET\0").await;
            send(&mut legacy, b'Z', b"I").await;
            assert_eq!(query(&mut legacy).await, command);
            drop(legacy);

            let (tag, error) = receive(&mut client).await;
            assert_eq!(tag, b'E');
            assert!(String::from_utf8_lossy(&error).contains("outcome is unknown"));
            assert!(running.await.unwrap().is_err());
        }).await.unwrap();
    }

    #[tokio::test]
    async fn a_legacy_replacement_receives_neither_the_key_nor_the_pending_write() {
        assert_retry_after_backend_error(BackendFailure::StorageClosed { replacement: ReplacementKey::Unsupported }).await;
    }

    #[tokio::test]
    async fn a_replacement_that_only_confirms_legacy_keys_never_receives_the_write() {
        assert_retry_after_backend_error(BackendFailure::StorageClosed { replacement: ReplacementKey::WithoutPermit }).await;
    }

    #[tokio::test]
    async fn a_replacement_advertising_the_old_permit_never_receives_the_write() {
        assert_retry_after_backend_error(BackendFailure::StorageClosed { replacement: ReplacementKey::LegacyPermit }).await;
    }

    #[tokio::test]
    async fn a_not_held_reply_followed_by_an_expired_permit_has_an_unknown_outcome() {
        assert_retry_after_backend_error(BackendFailure::NotHeld { replacement: ReplacementKey::Expired }).await;
    }

    enum BackendFailure { NotHeld { replacement: ReplacementKey }, StorageClosed { replacement: ReplacementKey } }
    #[derive(Clone, Copy)]
    enum ReplacementKey { Accept, Refuse, Unsupported, WithoutPermit, LegacyPermit, Expired }

    struct Routes(tokio::sync::Mutex<std::collections::VecDeque<(TcpStream, u64)>>);

    #[async_trait::async_trait]
    impl Router for Routes {
        async fn route(&self) -> Result<(TcpStream, Option<String>, u64)> {
            let (stream, epoch) = self.0.lock().await.pop_front().expect("a route is available");
            Ok((stream, None, epoch))
        }
        async fn route_holder(&self) -> Result<(TcpStream, Option<String>, u64)> { self.route().await }
    }

    async fn assert_retry_after_backend_error(failure: BackendFailure) {
        let replacement = match &failure {
            BackendFailure::NotHeld { replacement } | BackendFailure::StorageClosed { replacement } => *replacement,
        };
        let directory = rtdb_directory::MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut client, gateway_side) = pair(&listener).await;
        let (first, mut refusing) = pair(&listener).await;
        let (second, mut successor) = pair(&listener).await;
        let mut routes = std::collections::VecDeque::new();
        let stale = if matches!(failure, BackendFailure::StorageClosed { .. }) {
            let (route, backend) = pair(&listener).await;
            routes.push_back((route, 0));
            Some(backend)
        } else {
            None
        };
        routes.push_back((second, 1));
        let mut startup = 196608i32.to_be_bytes().to_vec();
        startup.extend_from_slice(b"user\0xtdb\0database\0shop\0\0");
        let mut framed = ((startup.len() + 4) as i32).to_be_bytes().to_vec();
        framed.extend_from_slice(&startup);
        let session = Session::new(Bytes::from(framed), Arc::new(Routes(tokio::sync::Mutex::new(routes))), directory.clone(), record.id.clone(), None, true);
        let running = tokio::spawn(session.run(gateway_side, first, None, 0));
        greet(&mut refusing).await;
        receive(&mut client).await;
        receive(&mut client).await;
        send(&mut client, b'Q', b"UPDATE counters SET n = n + 1\0").await;
        let key = query(&mut refusing).await;
        send(&mut refusing, b'C', b"SET\0").await;
        send(&mut refusing, b'Z', b"I").await;
        query(&mut refusing).await;
        if let Some(mut stale) = stale {
            send(&mut refusing, b'E', b"SERROR\0CXX000\0MI/O region is closed\0yrtdb.storage/closed\0\0").await;
            let mut byte = [0];
            assert_eq!(tokio::time::timeout(Duration::from_secs(5), stale.read(&mut byte)).await.unwrap().unwrap(), 0);
            let mut next = record.clone();
            next.epoch += 1;
            directory.replace_deployment(next, record.version).await.applied().unwrap();
        } else {
            send(&mut refusing, b'E', format!("SERROR\0C57P03\0M{}\0\0", rtdb_node::NOT_HELD).as_bytes()).await;
            if matches!(replacement, ReplacementKey::Expired) { send(&mut refusing, b'Z', b"I").await; }
        }
        drop(refusing);

        if matches!(replacement, ReplacementKey::Unsupported | ReplacementKey::WithoutPermit | ReplacementKey::LegacyPermit) {
            let mut startup = vec![0; successor.read_i32().await.unwrap() as usize - 4];
            successor.read_exact(&mut startup).await.unwrap();
            send(&mut successor, b'R', &[0; 4]).await;
            if matches!(replacement, ReplacementKey::WithoutPermit | ReplacementKey::LegacyPermit) {
                send(&mut successor, GATEWAY_SESSION_STATE_TAG, &[GATEWAY_KEY_CONFIRM_KIND]).await;
            }
            if matches!(replacement, ReplacementKey::LegacyPermit) {
                send(&mut successor, GATEWAY_SESSION_STATE_TAG, &[7]).await;
            }
            send(&mut successor, b'Z', b"I").await;
            let (tag, body) = receive(&mut client).await;
            assert_eq!(tag, b'E');
            assert!(String::from_utf8_lossy(&body).contains("outcome is unknown"));
            assert_eq!(successor.read(&mut [0]).await.unwrap(), 0);
            assert!(running.await.unwrap().is_err());
            return;
        }
        greet(&mut successor).await;
        assert_eq!(query(&mut successor).await, key);
        if matches!(replacement, ReplacementKey::Refuse) {
            send(&mut successor, b'E', b"SERROR\0C53200\0MOut of memory\0\0").await;
            let (tag, body) = receive(&mut client).await;
            assert_eq!(tag, b'E');
            assert!(String::from_utf8_lossy(&body).contains("the command's outcome is unknown"));
            assert_eq!(successor.read(&mut [0]).await.unwrap(), 0);
            assert!(running.await.unwrap().is_err());
            return;
        }
        send(&mut successor, b'C', b"SET\0").await;
        send(&mut successor, b'Z', b"I").await;
        assert_eq!(query(&mut successor).await, "UPDATE counters SET n = n + 1");
        if matches!(replacement, ReplacementKey::Expired) {
            send(&mut successor, b'E', b"SERROR\0C57014\0MReplay permit elapsed\0yrtdb.node/replay-window-elapsed\0\0").await;
            let (tag, body) = receive(&mut client).await;
            assert_eq!(tag, b'E');
            assert!(String::from_utf8_lossy(&body).contains("outcome is unknown"));
            assert!(running.await.unwrap().is_err());
            return;
        }
        send(&mut successor, b'C', b"UPDATE 1\0").await;
        send(&mut successor, b'Z', b"I").await;
        let answer = tokio::time::timeout(std::time::Duration::from_secs(5), receive(&mut client)).await.expect("the successor's answer reaches the client");
        assert_eq!(answer, (b'C', b"UPDATE 1\0".to_vec()), "rather than being discarded as the refused command's");
        drop(client);
        let _ = running.await;
    }

    #[tokio::test]
    async fn a_replacement_that_refuses_the_command_shape_leaves_the_original_outcome_unknown() {
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
        drop(crashing);

        greet(&mut successor).await;
        query(&mut successor).await;
        send(&mut successor, b'C', b"SET\0").await;
        send(&mut successor, b'Z', b"I").await;
        assert_eq!(query(&mut successor).await, command);
        send(&mut successor, b'E', b"SERROR\0C0A000\0MUnsupported replay command\0\0").await;
        let (tag, body) = receive(&mut client).await;
        assert_eq!(tag, b'E');
        assert!(String::from_utf8_lossy(&body).contains("outcome is unknown"), "the client is not told the command failed: {}", String::from_utf8_lossy(&body));
        let _ = running.await;
    }

    #[tokio::test]
    async fn an_unanswered_write_that_outlives_its_original_permit_is_not_replayed() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut client, gateway) = pair(&listener).await;
        let (first, mut dying) = pair(&listener).await;
        let router = Arc::new(Next(tokio::sync::Mutex::new(None)));
        let startup = Bytes::from_static(b"\0\0\0\x08\0\x03\0\0");
        let mut session = Session::new(startup, router, directory, record.id, None, true);
        session.command_key_age = Duration::from_millis(50);
        let running = tokio::spawn(session.run(gateway, first, None, 0));

        tokio::time::timeout(Duration::from_secs(5), async {
            greet(&mut dying).await;
            assert_eq!(receive(&mut client).await.0, b'R');
            assert_eq!(receive(&mut client).await.0, b'Z');
            send(&mut client, b'Q', b"UPDATE counters SET n = n + 1\0").await;
            let (tag, bytes) = receive(&mut dying).await;
            assert_eq!(tag, GATEWAY_SESSION_STATE_TAG);
            assert_eq!(GatewayReplayPermit::decode(&bytes).unwrap().mode, GatewayReplayMode::ConfirmDispatch);
            send(&mut dying, b'Z', b"I").await;
            assert_eq!(query(&mut dying).await, "UPDATE counters SET n = n + 1");
            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(dying);
            let (tag, body) = receive(&mut client).await;
            assert_eq!(tag, b'E');
            assert!(String::from_utf8_lossy(&body).contains("outcome is unknown"));
            assert!(running.await.unwrap().is_err());
        }).await.unwrap();
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
    async fn a_command_waits_for_a_key_after_the_original_backend_refuses_it() {
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
        receive(&mut client).await;
        receive(&mut client).await;
        send(&mut client, b'Q', b"UPDATE counters SET n = n + 1\0").await;
        let key = query(&mut dying).await;
        send(&mut dying, b'E', b"SERROR\0C53200\0Mout of memory\0\0").await;
        assert_eq!(dying.read(&mut [0]).await.unwrap(), 0);
        greet(&mut successor).await;
        assert_eq!(query(&mut successor).await, key);
        send(&mut successor, b'C', b"SET\0").await;
        send(&mut successor, b'Z', b"I").await;
        assert_eq!(query(&mut successor).await, "UPDATE counters SET n = n + 1");
        send(&mut successor, b'C', b"UPDATE 1\0").await;
        send(&mut successor, b'Z', b"I").await;
        assert_eq!(receive(&mut client).await, (b'C', b"UPDATE 1\0".to_vec()));
        assert_eq!(receive(&mut client).await.0, b'Z');
        drop(client);
        running.await.unwrap().unwrap();
    }
}
