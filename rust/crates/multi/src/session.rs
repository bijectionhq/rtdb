//! A gateway session: the client's connection outlives the backend it is spliced to (`MovesAreInvisible`).
//!
//! The gateway reads pgwire frames in both directions.
//! It learns the session's transaction status from every ReadyForQuery, and remembers the startup message and every named prepared statement.
//! Whenever the session is idle and its backend is gone or no longer the deployment's holder, it reconnects to the current holder, replays the startup and the prepared statements, swallows their replies, and carries on.
//! A backend lost in the middle of a command is replayed only if the command was keyed and none of its replies reached the client; otherwise it may have committed, so the client is told and the session ends.
//! A reconnection carries no password: once the first holder has authenticated the client, each reconnection's startup carries a capability the gateway mints instead, which needs the gateway to hold the cell's secret, or backends that trust the gateway.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use rtdb_directory::{DeploymentRecord, Directory};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, watch};

const MAXIMUM_FRAME_BYTES: usize = 1 << 30;
const RECONNECT_WITHIN: std::time::Duration = std::time::Duration::from_secs(30);
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

async fn read_frame(reader: &mut OwnedReadHalf) -> std::io::Result<Option<Frame>> {
    let tag = match reader.read_u8().await {
        Ok(tag) => tag,
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    };
    let length = reader.read_i32().await?;
    let length = usize::try_from(length).ok().filter(|length| (4..=MAXIMUM_FRAME_BYTES).contains(length))
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "frame length out of range"))?;
    let mut body = vec![0; length - 4];
    reader.read_exact(&mut body).await?;
    Ok(Some(Frame { tag, body: Bytes::from(body) }))
}

/// Reads frames off `reader` into a channel, so the session can wait on both directions without losing a partly read frame.
fn pump(mut reader: OwnedReadHalf) -> (mpsc::Receiver<std::io::Result<Frame>>, tokio::task::JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel(64);
    let task = tokio::spawn(async move {
        loop {
            match read_frame(&mut reader).await {
                Ok(Some(frame)) => if sender.send(Ok(frame)).await.is_err() { return },
                Ok(None) => return,
                Err(error) => { let _ = sender.send(Err(error)).await; return; }
            }
        }
    });
    (receiver, task)
}

/// Where a session is connected, and how to reconnect it.
#[async_trait::async_trait]
pub trait Router: Send + Sync {
    /// A connection to the deployment's current holder, the control address to touch it at, and the epoch it holds under.
    async fn route(&self) -> Result<(TcpStream, Option<String>, u64)>;
}

struct Backend { writer: OwnedWriteHalf, frames: mpsc::Receiver<std::io::Result<Frame>>, reader: tokio::task::JoinHandle<()>, epoch: u64 }

impl Drop for Backend { fn drop(&mut self) { self.reader.abort(); } }

impl Backend {
    fn new(stream: TcpStream, epoch: u64) -> Self {
        let (read, writer) = stream.into_split();
        let (frames, reader) = pump(read);
        Self { writer, frames, reader, epoch }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Status { Idle, InTransaction, Busy }

/// What a gateway needs to mint a session's capabilities: the cell's secret and the session's startup.
pub struct Grants { pub secret: Arc<crate::capability::CellSecret>, pub parameters: std::collections::BTreeMap<String, String>, pub read_only: bool }

pub struct Session {
    startup: Bytes,
    grants: Option<Grants>,
    /// The principal the first backend authenticated and reported it may be vouched for, for whom the gateway mints its reconnections' capabilities.
    authenticated: Option<String>,
    /// What the first backend's greeting has reported so far: its authentication, the session's user, and whether that user may be vouched for.
    greeting: Greeting,
    router: Arc<dyn Router>,
    record: watch::Receiver<Option<DeploymentRecord>>,
    prepared: BTreeMap<String, Frame>,
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
        let record = rtdb_directory::watch_deployment(directory, deployment, std::time::Duration::from_millis(100));
        Self { startup, grants, authenticated: None, greeting: Greeting::default(), router, record, prepared: BTreeMap::new(), status: Status::Busy, pending: Vec::new(), prepared_responses: 0, answered: false, lost_unanswered: false, discarding: false, keyed, key: None, swallowing: 0 }
    }

    /// Reconnects to the current holder, retrying through a failover: until the dead holder's lease expires, no other node may claim.
    async fn connect(&mut self) -> Result<Backend> {
        let result = tokio::time::timeout(RECONNECT_WITHIN, async {
            loop {
                match self.connect_once().await {
                    Ok(backend) => return Ok(backend),
                    Err(error) if !matches!(error.category, AnomalyCategory::Unavailable | AnomalyCategory::Conflict) => return Err(error),
                    Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
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
        let (mut stream, _, epoch) = self.router.route().await?;
        stream.write_all(&self.reconnection_startup()).await.map_err(io)?;
        let mut backend = Backend::new(stream, epoch);
        let mut replay = BytesMut::new();
        for frame in self.prepared.values() { replay.extend_from_slice(&frame.encode()); }
        if !replay.is_empty() { replay.extend_from_slice(&Frame { tag: b'S', body: Bytes::new() }.encode()); }
        let mut ready_expected = 1 + usize::from(!replay.is_empty());
        let mut replayed = false;
        while ready_expected > 0 {
            let frame = backend.frames.recv().await.ok_or_else(|| lost("the new backend closed during its greeting"))?.map_err(io)?;
            match frame.tag {
                b'E' => return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/reconnect-refused", "The new holder refused the session's replay")),
                b'R' if frame.body.len() >= 4 && frame.body[..4] != [0, 0, 0, 0] => return Err(Anomaly::new(AnomalyCategory::Unsupported, "rtdb.multi/reconnect-auth", "The new holder asks for a password the gateway does not hold")),
                b'Z' => {
                    ready_expected -= 1;
                    if !replayed && !replay.is_empty() { backend.writer.write_all(&replay).await.map_err(io)?; replayed = true; }
                }
                _ => {}
            }
        }
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
        self.record.borrow().as_ref().is_some_and(|record| record.epoch != backend.epoch)
    }

    /// Runs the session over an already-routed first backend whose greeting the client has not yet seen.
    pub async fn run(mut self, client: TcpStream, first: TcpStream, first_epoch: u64) -> Result<()> {
        let (client_read, mut client_write) = client.into_split();
        let (mut requests, client_reader) = pump(client_read);
        let mut backend = Some(Backend::new(first, first_epoch));
        enum Event { Request(Option<std::io::Result<Frame>>), Reply(Option<std::io::Result<Frame>>) }
        let outcome = async {
            backend.as_mut().expect("just set").writer.write_all(&self.startup).await.map_err(io)?;
            loop {
                let event = {
                    let Some(current) = backend.as_mut() else { break Ok(()) };
                    tokio::select! {
                        request = requests.recv() => Event::Request(request),
                        reply = current.frames.recv() => Event::Reply(reply),
                    }
                };
                match event {
                    Event::Request(None) => break Ok(()),
                    Event::Request(Some(request)) => {
                        let request = request.map_err(io)?;
                        if request.tag == b'X' {
                            if let Some(current) = backend.as_mut() { let _ = current.writer.write_all(&request.encode()).await; }
                            break Ok(());
                        }
                        if !self.greeting.done {
                            backend.as_mut().expect("greeting backend").writer.write_all(&request.encode()).await.map_err(io)?;
                            continue;
                        }
                        if request.tag == b'H' {
                            backend.as_mut().expect("connected").writer.write_all(&request.encode()).await.map_err(io)?;
                            continue;
                        }
                        let starts = self.status == Status::Idle;
                        if starts { self.answered = false; self.lost_unanswered = false; self.key = self.keyed.then(|| uuid::Uuid::new_v4().simple().to_string()); }
                        if starts && backend.as_ref().is_some_and(|current| self.stale(current)) { backend = None; }
                        if backend.is_none() { backend = Some(self.connect().await?); }
                        if starts { if let Some(key) = self.key.clone() { self.send_key(backend.as_mut().expect("connected"), &key).await?; } }
                        self.pending.push(request.clone());
                        self.status = Status::Busy;
                        let current = backend.as_mut().expect("connected");
                        if current.writer.write_all(&request.encode()).await.is_err() {
                            break Err(lost("the holder went away while a command was being sent"));
                        }
                    }
                    Event::Reply(Some(Ok(frame))) => {
                        if !self.greeting.done {
                            self.greeting.observe(&frame, &mut self.authenticated);
                            if self.greeting.done { self.status = Status::Idle; }
                            client_write.write_all(&frame.encode()).await.map_err(io)?;
                            if frame.tag == b'E' { break Ok(()); }
                            continue;
                        }
                        if self.swallowing > 0 {
                            // A key the backend did not take leaves the command unkeyed, and an unkeyed command is never resent.
                            if frame.tag == b'E' { self.key = None; }
                            if frame.tag == b'Z' { self.swallowing -= 1; }
                            continue;
                        }
                        if !self.answered && !self.discarding && frame.tag == b'E' && not_held(&frame) { self.discarding = true; }
                        if self.discarding {
                            if frame.tag == b'Z' {
                                self.discarding = false;
                                backend = Some(self.connect().await?);
                                self.resend(backend.as_mut().expect("connected")).await?;
                            }
                            continue;
                        }
                        // A refusal part way through a resent command says nothing of the attempt that was lost, which may have committed.
                        if self.answered && self.lost_unanswered && frame.tag == b'E' && not_held(&frame) {
                            break Err(lost("the holder went away during a command, and its successor refused the command part way: its outcome is unknown"));
                        }
                        self.answered = true;
                        match frame.tag {
                            b'1' | b'3' => self.confirm_prepared_change(frame.tag),
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
                            self.resend(backend.as_mut().expect("connected")).await?;
                            continue;
                        }
                        backend = Some(self.connect().await?);
                    }
                }
            }
        }.await;
        client_reader.abort();
        if let Err(error) = &outcome {
            let _ = client_write.write_all(&error_frame("08006", error.message.as_deref().unwrap_or(error.code.as_ref())).encode()).await;
        }
        outcome
    }

    /// Keys the transactions of the command that follows on `backend` by `key`, swallowing the statement's replies.
    async fn send_key(&mut self, backend: &mut Backend, key: &str) -> Result<()> {
        let mut body = format!("SET {} = '{key}'", rtdb_node::IDEMPOTENCY_KEY_PARAMETER).into_bytes();
        body.push(0);
        backend.writer.write_all(&Frame { tag: b'Q', body: Bytes::from(body) }.encode()).await.map_err(io)?;
        self.swallowing += 1;
        Ok(())
    }

    /// Sends the pending command again on a new backend, under its key if it has one.
    async fn resend(&mut self, backend: &mut Backend) -> Result<()> {
        if let Some(key) = self.key.clone() { self.send_key(backend, &key).await?; }
        for request in &self.pending { backend.writer.write_all(&request.encode()).await.map_err(io)?; }
        Ok(())
    }

    fn confirm_prepared_change(&mut self, response: u8) {
        let Some((position, request)) = self.pending.iter().enumerate().skip(self.prepared_responses)
            .take_while(|(_, request)| !matches!(request.tag, b'Q' | b'S'))
            .find(|(_, request)| matches!(request.tag, b'P' | b'C')) else { return };
        self.prepared_responses = position + 1;
        match (response, request.tag) {
            (b'1', b'P') => {
                let name = parse_name(request);
                if !name.is_empty() { self.prepared.insert(name, request.clone()); }
            }
            (b'3', b'C') if request.body.first() == Some(&b'S') => {
                let name = request.body[1..].split(|byte| *byte == 0).next().map(|name| String::from_utf8_lossy(name).into_owned()).unwrap_or_default();
                self.prepared.remove(&name);
            }
            _ => {}
        }
    }
}

/// Whether an ErrorResponse is a placed node's refusal of a database it no longer holds, raised before the command ran.
fn not_held(frame: &Frame) -> bool { frame.body.windows(rtdb_node::NOT_HELD.len()).any(|window| window == rtdb_node::NOT_HELD.as_bytes()) }

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
fn io(error: std::io::Error) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/session-io", error.to_string()) }

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
        let running = tokio::spawn(session.run(gateway_side, first, 0));

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
        let running = tokio::spawn(session.run(gateway_side, first, 0));
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
        let running = tokio::spawn(session.run(gateway_side, first, 0));
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
        let running = tokio::spawn(session.run(gateway_side, first, 0));
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
