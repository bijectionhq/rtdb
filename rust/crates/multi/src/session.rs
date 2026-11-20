//! A gateway session: the client's connection outlives the backend it is spliced to (`MovesAreInvisible`).
//!
//! The gateway reads pgwire frames in both directions.
//! It learns the session's transaction status from every ReadyForQuery, and remembers the startup message and every named prepared statement.
//! Whenever the session is idle and its backend is gone or no longer the deployment's holder, it reconnects to the current holder, replays the startup and the prepared statements, swallows their replies, and carries on.
//! A backend lost in the middle of a command is never replayed: the command may have committed, so the client is told and the session ends.
//! Transparent reconnection replays the startup without a password exchange, so it applies to backends that trust the gateway.

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

pub struct Session {
    startup: Bytes,
    router: Arc<dyn Router>,
    record: watch::Receiver<Option<DeploymentRecord>>,
    prepared: BTreeMap<String, Frame>,
    status: Status,
    /// The frames of the command sent since the session was last idle, kept until it is answered, so a refusal before it ran can resend it.
    pending: Vec<Frame>,
    /// Whether any reply to the pending command has reached the client; once one has, the command is never resent.
    answered: bool,
    /// Discarding a refused command's replies through its ReadyForQuery, before resending it.
    discarding: bool,
}

impl Session {
    pub fn new(startup: Bytes, router: Arc<dyn Router>, directory: Arc<dyn Directory>, deployment: String) -> Self {
        let record = rtdb_directory::watch_deployment(directory, deployment, std::time::Duration::from_millis(100));
        Self { startup, router, record, prepared: BTreeMap::new(), status: Status::Busy, pending: Vec::new(), answered: false, discarding: false }
    }

    /// Reconnects to the current holder, retrying through a failover: until the dead holder's lease expires, no other node may claim.
    async fn connect(&mut self) -> Result<Backend> {
        let deadline = tokio::time::Instant::now() + RECONNECT_WITHIN;
        loop {
            match self.connect_once().await {
                Ok(backend) => return Ok(backend),
                Err(error) if tokio::time::Instant::now() >= deadline || error.category != AnomalyCategory::Unavailable && error.category != AnomalyCategory::Conflict => return Err(error),
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            }
        }
    }

    async fn connect_once(&mut self) -> Result<Backend> {
        let (mut stream, _, epoch) = self.router.route().await?;
        stream.write_all(&self.startup).await.map_err(io)?;
        let mut backend = Backend::new(stream, epoch);
        let mut replay = BytesMut::new();
        for frame in self.prepared.values() { replay.extend_from_slice(&frame.encode()); }
        if !self.prepared.is_empty() { replay.extend_from_slice(&Frame { tag: b'S', body: Bytes::new() }.encode()); }
        // Swallow the backend's greeting, and the replies to the replayed statements, through to the last ReadyForQuery.
        let mut ready_expected = 1 + usize::from(!self.prepared.is_empty());
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

    fn stale(&self, backend: &Backend) -> bool {
        self.record.borrow().as_ref().is_some_and(|record| record.epoch != backend.epoch)
    }

    /// Runs the session over an already-routed first backend whose greeting the client has not yet seen.
    pub async fn run(mut self, client: TcpStream, first: TcpStream, first_epoch: u64) -> Result<()> {
        let (client_read, mut client_write) = client.into_split();
        let (mut requests, client_reader) = pump(client_read);
        let mut backend = Some(Backend::new(first, first_epoch));
        backend.as_mut().expect("just set").writer.write_all(&self.startup).await.map_err(io)?;
        enum Event { Request(Option<std::io::Result<Frame>>), Reply(Option<std::io::Result<Frame>>) }
        let outcome = async {
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
                        if self.status == Status::Idle && backend.as_ref().is_some_and(|current| self.stale(current)) { backend = None; }
                        if backend.is_none() { backend = Some(self.connect().await?); }
                        self.remember(&request);
                        if self.status == Status::Idle { self.pending.clear(); self.answered = false; }
                        self.pending.push(request.clone());
                        self.status = Status::Busy;
                        let current = backend.as_mut().expect("connected");
                        if current.writer.write_all(&request.encode()).await.is_err() {
                            break Err(lost("the holder went away while a command was being sent"));
                        }
                    }
                    Event::Reply(Some(Ok(frame))) => {
                        if !self.answered && !self.discarding && frame.tag == b'E' && not_held(&frame) { self.discarding = true; }
                        if self.discarding {
                            if frame.tag == b'Z' {
                                self.discarding = false;
                                backend = Some(self.connect().await?);
                                let current = backend.as_mut().expect("connected");
                                for request in &self.pending { current.writer.write_all(&request.encode()).await.map_err(io)?; }
                            }
                            continue;
                        }
                        self.answered = true;
                        if frame.tag == b'Z' {
                            self.status = match frame.body.first() { Some(b'I') => Status::Idle, Some(b'T' | b'E') => Status::InTransaction, _ => Status::Busy };
                        }
                        client_write.write_all(&frame.encode()).await.map_err(io)?;
                    }
                    Event::Reply(Some(Err(_)) | None) => {
                        if self.status != Status::Idle { break Err(lost("the holder went away during a command, whose outcome is unknown")); }
                        backend = None;
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

    fn remember(&mut self, request: &Frame) {
        match request.tag {
            b'P' => {
                let name = request.body.split(|byte| *byte == 0).next().map(|name| String::from_utf8_lossy(name).into_owned()).unwrap_or_default();
                if !name.is_empty() { self.prepared.insert(name, request.clone()); }
            }
            b'C' if request.body.first() == Some(&b'S') => {
                let name = request.body[1..].split(|byte| *byte == 0).next().map(|name| String::from_utf8_lossy(name).into_owned()).unwrap_or_default();
                self.prepared.remove(&name);
            }
            _ => {}
        }
    }
}

/// Whether an ErrorResponse is a placed node's refusal of a database it no longer holds, raised before the command ran.
fn not_held(frame: &Frame) -> bool { frame.body.windows(rtdb_node::NOT_HELD.len()).any(|window| window == rtdb_node::NOT_HELD.as_bytes()) }

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
