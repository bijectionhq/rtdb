//! The logkeeper server: many named streams on one data directory, served over one multiplexed protocol.

mod journal;
mod payload;
pub(crate) mod state;
mod store;

use crate::local::LocalListener;
use crate::protocol::{Request, RequestBody, Response, ResponseBody, read_frame, encode_frame};
use bytes::BytesMut;
use journal::{Journal, MemberIdentity};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use store::{Outcome, Store, failed};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub struct ServerOptions {
    pub data_dir: PathBuf,
    /// Syncs every round before acknowledging it; only tests turn this off.
    pub fsync: bool,
    /// The most bytes of recently written or read payloads the server keeps in memory, across every stream; the rest are read from the segments.
    pub cache_bytes: usize,
}

impl ServerOptions {
    pub const DEFAULT_CACHE_BYTES: usize = 64 * 1024 * 1024;
}

/// A recovered journal holding the exclusive data-directory lock.
pub struct PreparedJournal {
    journal: Journal,
    recovered: Vec<journal::Recovered>,
}

impl PreparedJournal {
    pub async fn open(options: ServerOptions) -> Result<Self> {
        Ok(Self::open_with_identity(options, None, false).await?.0)
    }

    /// Opens a member journal and recovers its ID before it can be advertised.
    pub async fn open_for_member(options: ServerOptions, candidate: String) -> Result<(Self, String)> {
        let (journal, identity) = Self::open_with_identity(options, Some(candidate), false).await?;
        let identity = identity.ok_or_else(|| Anomaly::fault("rtdb.logkeeper/identity", "The opened journal did not retain a member ID"))?;
        Ok((journal, identity))
    }

    /// Requires the retained journal identity before replay or registration.
    pub async fn reopen_member(options: ServerOptions, expected: String) -> Result<Self> {
        Ok(Self::open_with_identity(options, Some(expected), true).await?.0)
    }

    async fn open_with_identity(
        options: ServerOptions,
        identity: Option<String>,
        reopen: bool,
    ) -> Result<(Self, Option<String>)> {
        let (journal, recovered, identity) = tokio::task::spawn_blocking(move || {
            let member = identity.as_deref().map(|id| {
                if reopen { MemberIdentity::Reopen(id) } else { MemberIdentity::Open(id) }
            });
            Journal::open(&options.data_dir, options.fsync, options.cache_bytes, member)
        }).await.map_err(|error| Anomaly::fault("rtdb.logkeeper/recovery", error.to_string()))??;
        Ok((Self { journal, recovered }, identity))
    }

    pub async fn serve(self, listener: TcpListener, cancellation: CancellationToken) -> Result<()> {
        serve_prepared(Listener::Tcp(listener), self, cancellation).await
    }

    pub async fn serve_local(self, listener: LocalListener, cancellation: CancellationToken) -> Result<()> {
        serve_prepared(Listener::Local(listener), self, cancellation).await
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

async fn serve_prepared(mut listener: Listener, opened: PreparedJournal, cancellation: CancellationToken) -> Result<()> {
    let PreparedJournal { journal, recovered } = opened;
    let journal = Arc::new(journal);
    let (queue, work) = mpsc::unbounded_channel();
    let store = Arc::new(Store::new(Arc::clone(&journal), recovered, queue));
    let mut journal_task = tokio::spawn(journal::run(journal, work));
    let connections = TaskTracker::new();
    let stopping = cancellation.child_token();
    let result = loop {
        tokio::select! {
            _ = cancellation.cancelled() => break Ok(()),
            result = &mut journal_task => break result.map_err(|error| Anomaly::fault("rtdb.logkeeper/journal-task", error.to_string())).and_then(|result| result.and(Err(Anomaly::fault("rtdb.logkeeper/journal-stopped", "the journal stopped")))),
            accepted = listener.accept() => match accepted {
                Ok(Socket::Tcp(socket)) => {
                    let _ = socket.set_nodelay(true);
                    let (reader, writer) = socket.into_split();
                    connections.spawn(connection(reader, writer, Arc::clone(&store), connections.clone(), stopping.clone()));
                }
                Ok(Socket::Local(socket)) => {
                    let (reader, writer) = tokio::io::split(socket);
                    connections.spawn(connection(reader, writer, Arc::clone(&store), connections.clone(), stopping.clone()));
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

/// One task reads and decides, another writes replies; a reply that waits on durability or a long poll gets a short-lived task of its own.
async fn connection(
    mut reader: impl AsyncRead + Unpin,
    writer: impl AsyncWrite + Unpin + Send + 'static,
    store: Arc<Store>,
    tasks: TaskTracker,
    cancellation: CancellationToken,
) {
    let (replies, outbox) = mpsc::unbounded_channel::<Response>();
    let writing = tasks.spawn(write_replies(writer, outbox));
    loop {
        let request = tokio::select! {
            _ = cancellation.cancelled() => break,
            request = read_frame::<Request>(&mut reader) => match request { Ok(Some(request)) => request, Ok(None) | Err(_) => break },
        };
        let id = request.id;
        let reply = |body: ResponseBody| Response { id, body: Some(body) };
        match request.body {
            None => { let _ = replies.send(reply(failed("a request without a body"))); }
            Some(RequestBody::Fetch(fetch)) => {
                match store.fetch(&fetch.stream, fetch.from_lsn, fetch.to_lsn, fetch.max_entries as usize, fetch.max_bytes as usize).ready() {
                    Ok(body) => { let _ = replies.send(reply(body)); }
                    Err(selected) => {
                        let (store, replies) = (Arc::clone(&store), replies.clone());
                        tasks.spawn(async move {
                            let body = store.entries(selected).await;
                            let _ = replies.send(Response { id, body: Some(body) });
                        });
                    }
                }
            }
            Some(RequestBody::Check(check)) => { let _ = replies.send(reply(store.check(&check.stream, check.epoch))); }
            Some(RequestBody::Retire(retire)) => {
                let (store, replies) = (Arc::clone(&store), replies.clone());
                tasks.spawn(async move {
                    let body = store.retire(retire).await;
                    let _ = replies.send(Response { id, body: Some(body) });
                });
            }
            Some(RequestBody::Read(read)) => {
                let (store, replies, cancellation) = (Arc::clone(&store), replies.clone(), cancellation.clone());
                tasks.spawn(async move {
                    let body = store.read(&read.stream, read.from_lsn, read.max_entries as usize, read.max_bytes as usize, Duration::from_millis(read.wait_ms.into()), &cancellation).await;
                    let _ = replies.send(Response { id, body: Some(body) });
                });
            }
            Some(mutation) => match store.mutate(mutation) {
                Outcome::Now(body) => { let _ = replies.send(reply(body)); }
                Outcome::Durable(durable, body) => {
                    let replies = replies.clone();
                    tasks.spawn(async move {
                        let body = match durable.await {
                            Ok(Ok(())) => body,
                            Ok(Err(error)) => failed(format!("journal write failed: {error}")),
                            Err(_) => failed("the journal stopped"),
                        };
                        let _ = replies.send(Response { id, body: Some(body) });
                    });
                }
            },
        }
    }
    drop(replies);
    let _ = writing.await;
}

async fn write_replies(mut writer: impl AsyncWrite + Unpin, mut outbox: mpsc::UnboundedReceiver<Response>) {
    let mut buffer = BytesMut::new();
    while let Some(reply) = outbox.recv().await {
        encode_frame(&reply, &mut buffer);
        while let Ok(reply) = outbox.try_recv() { encode_frame(&reply, &mut buffer); }
        if writer.write_all(&buffer).await.is_err() { return; }
        buffer.clear();
    }
    let _ = writer.shutdown().await;
}
