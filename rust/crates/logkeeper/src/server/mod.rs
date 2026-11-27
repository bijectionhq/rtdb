//! The logkeeper server: many named streams on one data directory, served over one multiplexed protocol.

mod journal;
mod payload;
pub(crate) mod state;
mod store;

pub use journal::{JournalDigest, inspect_retained, inspect_retained_with_prefix};
use crate::local::LocalListener;

use crate::protocol::{AUTHORITY_FRAME_BYTES, FrameParts, MAXIMUM_FRAME_BYTES, WIRE_BATCH_FRAMES, JournalIdentity, Request, RequestBody, Response, ResponseBody, encode_frame, read_bounded_frame, read_sized_frame};
use bytes::{Bytes, BytesMut};
use journal::{Journal, MemberIdentity};
use rtdb_types::{Anomaly, AnomalyCategory, MemoryBudget, MemoryReservation, Result};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use store::{Outcome, Store, failed};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub struct ServerOptions {
    pub data_dir: PathBuf,
    /// Syncs every round before acknowledging it; only tests turn this off.
    pub fsync: bool,
    /// How durable syncs flush to storage on Apple platforms when `fsync` is enabled.
    pub apple_sync: rtdb_storage::AppleSync,
    /// The most bytes of recently written or read payloads the server keeps in memory, across every stream; the rest are read from the segments.
    pub cache_bytes: usize,
    pub connections: ConnectionLimits,
}

impl ServerOptions {
    pub const DEFAULT_CACHE_BYTES: usize = 64 * 1024 * 1024;

    pub fn new(data_dir: PathBuf, fsync: bool, cache_bytes: usize) -> Self {
        Self { data_dir, fsync, apple_sync: rtdb_storage::AppleSync::Full, cache_bytes, connections: ConnectionLimits::default() }
    }

    pub fn with_apple_sync(mut self, apple_sync: rtdb_storage::AppleSync) -> Self {
        self.apple_sync = apple_sync;
        self
    }
}

/// What the server's client connections may hold between deciding a reply and writing it.
pub struct ConnectionLimits {
    /// Charged, through a child per connection, for every connection's decided and unwritten replies.
    /// A reply it refuses closes its connection, so the client sees an unknown outcome rather than a missing reply.
    pub memory: Arc<MemoryBudget>,
    /// The most bytes of decided, unwritten replies one connection holds before it stops reading requests.
    pub reply_bytes: usize,
    /// The most requests one connection has in flight on tasks of their own.
    pub requests: usize,
}

impl ConnectionLimits {
    /// Room for two replies of the largest frame at once, across every connection.
    pub const DEFAULT_MEMORY_BYTES: usize = 2 * MAXIMUM_FRAME_BYTES;
    pub const DEFAULT_REPLY_BYTES: usize = 64 * 1024 * 1024;
    pub const DEFAULT_REQUESTS: usize = 1024;

    fn validate(&self) -> Result<()> {
        if self.reply_bytes == 0 || self.reply_bytes > u32::MAX as usize || self.reply_bytes > self.memory.limit_bytes()
            || !(1..=Semaphore::MAX_PERMITS).contains(&self.requests)
        {
            return Err(Anomaly::incorrect("rtdb.logkeeper/connection-limits",
                "A connection needs room for at least one reply byte, within the server's reply memory, and at least one request in flight"));
        }
        Ok(())
    }
}

impl Default for ConnectionLimits {
    fn default() -> Self {
        Self { memory: MemoryBudget::new(Self::DEFAULT_MEMORY_BYTES), reply_bytes: Self::DEFAULT_REPLY_BYTES, requests: Self::DEFAULT_REQUESTS }
    }
}

/// A finite read-only listener for the installed deployment's replica authority.
pub struct AuthorityListener {
    pub socket: TcpListener,
    pub source: uuid::Uuid,
    pub connections: usize,
}

impl AuthorityListener {
    fn admits(&self, stream: &str) -> bool {
        let Some((source, rest)) = stream.split_once('/') else { return false; };
        let Some((incarnation, kind)) = rest.split_once('/') else { return false; };
        uuid::Uuid::parse_str(source).ok() == Some(self.source)
            && !incarnation.is_empty() && kind == "replica"
    }
}

/// A recovered journal holding the exclusive data-directory lock.
pub struct PreparedJournal {
    journal: Journal,
    recovered: Vec<journal::Recovered>,
    identity: Arc<str>,
    connections: ConnectionLimits,
}

impl PreparedJournal {
    pub async fn open(options: ServerOptions) -> Result<Self> {
        Self::open_with_identity(options, None).await
    }

    /// Opens a member journal and recovers its ID before it can be advertised.
    pub async fn open_for_member(options: ServerOptions) -> Result<(Self, String)> {
        let journal = Self::open_with_identity(options, Some(MemberIdentity::Open)).await?;
        let identity = journal.identity.to_string();
        Ok((journal, identity))
    }

    /// Requires the retained journal identity before replay or registration.
    pub async fn reopen_member(options: ServerOptions, expected: String) -> Result<Self> {
        Self::open_with_identity(options, Some(MemberIdentity::Reopen(expected))).await
    }

    async fn open_with_identity(
        options: ServerOptions,
        member: Option<MemberIdentity>,
    ) -> Result<Self> {
        let ServerOptions { data_dir, fsync, apple_sync, cache_bytes, connections } = options;
        connections.validate()?;
        let (journal, recovered, identity) = tokio::task::spawn_blocking(move || {
            Journal::open_with_sync(&data_dir, fsync, apple_sync, cache_bytes, member)
        }).await.map_err(|error| Anomaly::fault("rtdb.logkeeper/recovery", error.to_string()))??;
        Ok(Self { journal, recovered, identity: identity.into(), connections })
    }

    pub async fn serve(self, listener: TcpListener, cancellation: CancellationToken) -> Result<()> {
        self.serve_with_authority(listener, None, cancellation).await
    }

    pub async fn serve_with_authority(self, listener: TcpListener, authority: Option<AuthorityListener>, cancellation: CancellationToken) -> Result<()> {
        if authority.as_ref().is_some_and(|config| config.source.is_nil() || !(1..=Semaphore::MAX_PERMITS).contains(&config.connections)) {
            return Err(Anomaly::incorrect("rtdb.logkeeper/authority-listener", "The authority listener requires an exact source and a finite connection count"));
        }
        serve_prepared(Listener::Tcp(listener), authority, self, cancellation).await
    }

    pub async fn serve_local(self, listener: LocalListener, cancellation: CancellationToken) -> Result<()> {
        serve_prepared(Listener::Local(listener), None, self, cancellation).await
    }
}

/// Serves until `cancellation`, then closes every connection and drains the journal.
/// A failed journal write stops the server, since its later acknowledgements could no longer be trusted.
pub async fn serve(listener: TcpListener, options: ServerOptions, cancellation: CancellationToken) -> Result<()> {
    PreparedJournal::open(options).await?.serve(listener, cancellation).await
}

enum Listener {
    Tcp(TcpListener),
    Local(LocalListener),
}

enum Socket {
    Tcp(TcpStream),
    Local(DuplexStream),
}

impl Listener {
    async fn accept(&mut self) -> std::io::Result<Socket> {
        match self {
            Self::Tcp(listener) => listener.accept().await.map(|(socket, _)| Socket::Tcp(socket)),
            Self::Local(listener) => listener.accept().await.map(Socket::Local),
        }
    }
}

async fn serve_prepared(
    mut listener: Listener, authority: Option<AuthorityListener>, opened: PreparedJournal, cancellation: CancellationToken,
) -> Result<()> {
    let PreparedJournal { journal, recovered, identity, connections: limits } = opened;
    let limits = Arc::new(limits);
    let journal = Arc::new(journal);
    let (queue, work) = mpsc::unbounded_channel();
    let has_dropped_streams = recovered.iter().any(|stream| stream.state.dropped);
    let store = Arc::new(Store::new(Arc::clone(&journal), recovered, queue));
    if has_dropped_streams { store.compact_if_mostly_dead(); }
    let mut journal_task = tokio::spawn(journal::run(journal, Arc::downgrade(&store), work));
    let connections = TaskTracker::new();
    let stopping = cancellation.child_token();
    let authority = authority.map(Arc::new);
    let authority_permits = authority.as_ref().map(|listener| Arc::new(Semaphore::new(listener.connections)));
    let accepting_authority = || async {
        match &authority {
            Some(listener) => listener.socket.accept().await,
            None => std::future::pending().await,
        }
    };
    let result = loop {
        tokio::select! {
            _ = cancellation.cancelled() => break Ok(()),
            result = &mut journal_task => break result.map_err(|error| Anomaly::fault("rtdb.logkeeper/journal-task", error.to_string())).and_then(|result| result.and(Err(Anomaly::fault("rtdb.logkeeper/journal-stopped", "the journal stopped")))),
            accepted = accepting_authority() => match accepted {
                Ok((socket, _)) => {
                    let listener = authority.as_ref().expect("authority listener accepted");
                    let permits = authority_permits.as_ref().expect("authority connection limit");
                    if let Ok(permit) = Arc::clone(permits).try_acquire_owned() {
                        let (store, identity, listener, stopping) =
                            (Arc::clone(&store), Arc::clone(&identity), Arc::clone(listener), stopping.clone());
                        connections.spawn(async move {
                            let _permit = permit;
                            let _ = stopping.run_until_cancelled(authority_connection(socket, store, identity, listener)).await;
                        });
                    }
                }
                Err(error) => break Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.logkeeper/authority-accept", error.to_string())),
            },
            accepted = listener.accept() => match accepted {
                Ok(Socket::Tcp(socket)) => {
                    let _ = socket.set_nodelay(true);
                    let (reader, writer) = socket.into_split();
                    connections.spawn(connection(reader, writer, Arc::clone(&store), Arc::clone(&identity), Arc::clone(&limits), connections.clone(), stopping.child_token()));
                }
                Ok(Socket::Local(socket)) => {
                    let (reader, writer) = tokio::io::split(socket);
                    connections.spawn(connection(reader, writer, Arc::clone(&store), Arc::clone(&identity), Arc::clone(&limits), connections.clone(), stopping.child_token()));
                }
                Err(error) => break Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.logkeeper/accept", error.to_string())),
            },
        }
    };
    stopping.cancel();
    drop(listener);
    connections.close();
    connections.wait().await;
    drop(store);
    if result.is_err() { return result; }
    journal_task.await.map_err(|error| Anomaly::fault("rtdb.logkeeper/journal-task", error.to_string()))?
}

async fn authority_connection(
    mut socket: TcpStream, store: Arc<Store>, identity: Arc<str>, listener: Arc<AuthorityListener>,
) -> std::io::Result<()> {
    socket.set_nodelay(true)?;
    let mut buffer = BytesMut::new();
    loop {
        let Ok(incoming) = tokio::time::timeout(
            Duration::from_secs(180), read_bounded_frame::<Request>(&mut socket, AUTHORITY_FRAME_BYTES),
        ).await else { return Ok(()); };
        let Some((request, _)) = incoming? else { return Ok(()); };
        let Some(RequestBody::ConfirmAuthority(check)) = request.body else { return Ok(()); };
        if request.target_journal != *identity || !listener.admits(&check.stream) {
            return Ok(());
        }
        let reply = Response { id: request.id, body: Some(store.confirm_authority(check)) };
        encode_frame(&reply, &mut buffer);
        let Ok(sent) = tokio::time::timeout(Duration::from_secs(2), socket.write_all(&buffer)).await else { return Ok(()); };
        sent?;
        buffer.clear();
    }
}

/// A reply's room in its connection's outbox and its charge to the server's reply memory, both released once it is written.
struct Room {
    _outbox: OwnedSemaphorePermit,
    _memory: Arc<MemoryReservation>,
}

/// Replies decided and not yet written, bounded by their encoded bytes. A
/// producer waits for room, so a client that does not read its replies stops
/// the reader taking more of its requests.
#[derive(Clone)]
struct Outbox {
    sender: mpsc::UnboundedSender<(Response, Room)>,
    room: Arc<Semaphore>,
    bytes: usize,
    memory: Arc<MemoryBudget>,
    cancellation: CancellationToken,
}

impl Outbox {
    /// `cancellation` is this connection's own: the outbox cancels it when the reply memory refuses a reply.
    fn open(bytes: usize, memory: Arc<MemoryBudget>, cancellation: CancellationToken) -> (Self, mpsc::UnboundedReceiver<(Response, Room)>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (Self { sender, room: Arc::new(Semaphore::new(bytes)), bytes, memory, cancellation }, receiver)
    }

    /// Queues the reply once there is room for it; a reply larger than the
    /// whole outbox waits for it to empty. Once the connection is cancelled a
    /// reply is still queued if there is room, and dropped otherwise, since
    /// the connection closes without reading more requests.
    async fn send(&self, reply: Response) {
        let encoded = prost::Message::encoded_len(&reply);
        let charge = u32::try_from(encoded.clamp(1, self.bytes)).unwrap_or(u32::MAX);
        let permit = match Arc::clone(&self.room).try_acquire_many_owned(charge) {
            Ok(permit) => permit,
            Err(_) => match self.cancellation.run_until_cancelled(Arc::clone(&self.room).acquire_many_owned(charge)).await {
                Some(Ok(permit)) => permit,
                Some(Err(_)) | None => return,
            },
        };
        let Ok(memory) = self.memory.reserve(encoded) else {
            self.cancellation.cancel();
            return;
        };
        let _ = self.sender.send((reply, Room { _outbox: permit, _memory: memory }));
    }
}

/// Admits one more request to a task of its own, waiting while the connection
/// has `ConnectionLimits::requests` in flight.
async fn admit(requests: &Arc<Semaphore>, cancellation: &CancellationToken) -> Option<OwnedSemaphorePermit> {
    cancellation.run_until_cancelled(Arc::clone(requests).acquire_owned()).await?.ok()
}

/// One task reads and decides, another writes replies; a reply that waits on durability or a long poll gets a short-lived task of its own.
async fn connection(
    mut reader: impl AsyncRead + Unpin,
    writer: impl AsyncWrite + Unpin + Send + 'static,
    store: Arc<Store>,
    identity: Arc<str>,
    limits: Arc<ConnectionLimits>,
    tasks: TaskTracker,
    cancellation: CancellationToken,
) {
    let (replies, outbox) = Outbox::open(limits.reply_bytes, limits.memory.scope(), cancellation.clone());
    let requests = Arc::new(Semaphore::new(limits.requests));
    let writing = tasks.spawn(write_replies(writer, outbox));
    loop {
        let (request, frame_bytes) = tokio::select! {
            _ = cancellation.cancelled() => break,
            request = read_sized_frame::<Request>(&mut reader) => match request { Ok(Some(request)) => request, Ok(None) | Err(_) => break },
        };
        let id = request.id;
        let reply = |body: ResponseBody| Response { id, body: Some(body) };
        if !request.target_journal.is_empty() && request.target_journal != *identity {
            replies.send(reply(failed("the operation addresses another journal"))).await;
            continue;
        }
        if let Some(RequestBody::AppendObserved(append)) = &request.body
            && let [entry] = append.entries.as_slice()
            && let Some(origin) = &entry.timed_origin
            && (request.target_journal.is_empty() || !origin.journals.iter().any(|journal| journal == identity.as_ref()))
        {
            replies.send(reply(failed("a timed original append must address a member of its ensemble"))).await;
            continue;
        }
        match request.body {
            None => { replies.send(reply(failed("a request without a body"))).await; }
            Some(RequestBody::Identify(request)) => {
                let body = match <&[u8; 16]>::try_from(request.nonce.as_ref()) {
                    Ok(nonce) => {
                        let (public_key, signature) = store.prove_identity(nonce);
                        ResponseBody::JournalIdentity(JournalIdentity {
                            journal: identity.to_string(),
                            public_key: Bytes::copy_from_slice(&public_key),
                            signature: Bytes::copy_from_slice(&signature),
                        })
                    }
                    Err(_) => failed("invalid journal identity challenge"),
                };
                replies.send(reply(body)).await;
            }
            Some(RequestBody::Fetch(fetch)) => {
                match store.fetch(&fetch.stream, fetch.from_lsn, fetch.to_lsn, fetch.max_entries as usize, fetch.max_bytes as usize).map(|selected| selected.ready()) {
                    Ok(Ok(body)) | Err(body) => { replies.send(reply(body)).await; }
                    Ok(Err(selected)) => {
                        let Some(admitted) = admit(&requests, &cancellation).await else { break };
                        let (store, replies) = (Arc::clone(&store), replies.clone());
                        tasks.spawn(async move {
                            let _admitted = admitted;
                            let body = store.entries(selected).await;
                            replies.send(Response { id, body: Some(body) }).await;
                        });
                    }
                }
            }
            Some(RequestBody::ConfirmAuthority(_)) => {
                replies.send(reply(failed("writer authority requires its dedicated listener"))).await;
            }
            Some(RequestBody::Check(check)) => { replies.send(reply(store.check(&check.stream, check.epoch))).await; }
            Some(RequestBody::Retire(retire)) => {
                let Some(admitted) = admit(&requests, &cancellation).await else { break };
                let (store, replies) = (Arc::clone(&store), replies.clone());
                tasks.spawn(async move {
                    let _admitted = admitted;
                    let body = store.retire(retire, frame_bytes).await;
                    replies.send(Response { id, body: Some(body) }).await;
                });
            }
            Some(RequestBody::VoteDecision(request)) => {
                let Some(admitted) = admit(&requests, &cancellation).await else { break };
                let (store, replies) = (Arc::clone(&store), replies.clone());
                tasks.spawn(async move {
                    let _admitted = admitted;
                    let body = store.vote_decision(request).await;
                    replies.send(Response { id, body: Some(body) }).await;
                });
            }
            Some(RequestBody::DigestPage(page)) => {
                let Some(admitted) = admit(&requests, &cancellation).await else { break };
                let (store, replies) = (Arc::clone(&store), replies.clone());
                tasks.spawn(async move {
                    let _admitted = admitted;
                    let body = store.digest_page(page).await;
                    replies.send(Response { id, body: Some(body) }).await;
                });
            }
            Some(RequestBody::Read(read)) => {
                let Some(admitted) = admit(&requests, &cancellation).await else { break };
                let (store, replies, cancellation) = (Arc::clone(&store), replies.clone(), cancellation.clone());
                tasks.spawn(async move {
                    let _admitted = admitted;
                    let body = store.read(&read.stream, read.from_lsn, read.max_entries as usize, read.max_bytes as usize, Duration::from_millis(read.wait_ms.into()), &cancellation).await;
                    replies.send(Response { id, body: Some(body) }).await;
                });
            }
            Some(mutation) => match tokio::select! {
                _ = cancellation.cancelled() => break,
                outcome = store.mutate(mutation, frame_bytes) => outcome,
            } {
                Outcome::Now(body) => { replies.send(reply(body)).await; }
                Outcome::Durable(durable, body) => {
                    let Some(admitted) = admit(&requests, &cancellation).await else { break };
                    let replies = replies.clone();
                    tasks.spawn(async move {
                        let _admitted = admitted;
                        let body = match durable.await {
                            Ok(Ok(())) => body,
                            Ok(Err(error)) => failed(format!("journal write failed: {error}")),
                            Err(_) => failed("the journal stopped"),
                        };
                        replies.send(Response { id, body: Some(body) }).await;
                    });
                }
            },
        }
    }
    drop(replies);
    let _ = writing.await;
}

async fn write_replies(mut writer: impl AsyncWrite + Unpin, mut outbox: mpsc::UnboundedReceiver<(Response, Room)>) {
    let mut buffer = FrameParts::default();
    let mut written = Vec::with_capacity(WIRE_BATCH_FRAMES);
    while let Some((reply, room)) = outbox.recv().await {
        buffer.push_wire(&reply);
        written.push(room);
        for _ in 1..WIRE_BATCH_FRAMES {
            if buffer.wire_batch_full() { break; }
            let Ok((reply, room)) = outbox.try_recv() else { break };
            buffer.push_wire(&reply);
            written.push(room);
        }
        if buffer.send_wire(&mut writer).await.is_err() { return; }
        written.clear();
    }
    let _ = writer.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Seal, read_frame};

    #[tokio::test]
    async fn a_request_addressed_to_another_journal_cannot_seal_this_one() {
        let disk = tempfile::tempdir().unwrap();
        let options = ServerOptions::new(disk.path().to_owned(), true, 0);
        let (journal, expected) = PreparedJournal::open_for_member(options).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stop = CancellationToken::new();
        let task = tokio::spawn(journal.serve(listener, stop.clone()));
        let mut socket = TcpStream::connect(address).await.unwrap();
        for (id, epoch, target_journal) in [(1, 9, uuid::Uuid::new_v4().to_string()), (2, 1, expected)] {
            let request = Request { id, target_journal, body: Some(RequestBody::Seal(Seal { stream: "a".into(), epoch, nonce: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()) })) };
            let mut frame = BytesMut::new();
            encode_frame(&request, &mut frame);
            socket.write_all(&frame).await.unwrap();
            let response = read_frame::<Response>(&mut socket).await.unwrap().unwrap();
            assert_eq!(response.id, id);
            if id == 1 {
                assert!(matches!(response.body, Some(ResponseBody::Failed(_))));
            } else {
                assert!(matches!(response.body, Some(ResponseBody::Sealed(sealed)) if sealed.promised == 1));
            }
        }
        stop.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_full_outbox_holds_its_producer_until_the_writer_takes_a_reply() {
        let cancellation = CancellationToken::new();
        let (outbox, mut written) = Outbox::open(1, MemoryBudget::new(1024), cancellation.clone());
        let reply = |id| Response { id, body: Some(failed("a reply")) };
        outbox.send(reply(1)).await;
        let mut second = std::pin::pin!(outbox.send(reply(2)));
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut second).await.is_err(), "no room until the first reply is written");
        let (first, room) = written.recv().await.unwrap();
        assert_eq!(first.id, 1);
        drop(room);
        second.await;
        let (second, _room) = written.recv().await.unwrap();
        assert_eq!(second.id, 2);

        cancellation.cancel();
        outbox.send(reply(3)).await;
        assert!(written.try_recv().is_err(), "a cancelled connection drops a reply it has no room for");
    }

    #[tokio::test]
    async fn replies_are_charged_to_the_server_memory_until_written_and_a_refused_one_closes_the_connection() {
        let server = MemoryBudget::new(64);
        let connection = server.scope();
        let cancellation = CancellationToken::new();
        let (outbox, mut written) = Outbox::open(1024, Arc::clone(&connection), cancellation.clone());
        let reply = |id, message: &str| Response { id, body: Some(failed(message)) };
        outbox.send(reply(1, "a reply")).await;
        assert!(connection.used_bytes() > 0 && server.used_bytes() == connection.used_bytes());
        let (first, room) = written.recv().await.unwrap();
        assert_eq!(first.id, 1);
        drop(room);
        assert_eq!(server.used_bytes(), 0);

        outbox.send(reply(2, &"x".repeat(128))).await;
        assert!(written.try_recv().is_err(), "a reply the server memory refuses is not queued");
        assert!(cancellation.is_cancelled(), "and its connection closes");
    }

    #[tokio::test]
    async fn connection_limits_must_fit_within_the_reply_memory() {
        let limits = |reply_bytes, requests| ConnectionLimits { memory: MemoryBudget::new(1024), reply_bytes, requests };
        assert!(limits(1024, 1).validate().is_ok());
        assert!(limits(0, 1).validate().is_err());
        assert!(limits(1025, 1).validate().is_err());
        assert!(limits(1024, 0).validate().is_err());
        assert!(ConnectionLimits::default().validate().is_ok());
    }
}
