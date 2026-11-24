//! One multiplexed connection per server address in this process, shared by every stream that names it.
//! Sending never waits: a request joins the session's outbox in call order, and its reply is handed to a callback or a future.
//! A failed session fails every outstanding request, and the next request opens a new one.

use crate::protocol::{Identify, Request, RequestBody, Response, ResponseBody, encode_frame, read_frame};
use bytes::BytesMut;
use parking_lot::Mutex;
use rtdb_storage::{Error, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Weak};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
pub(crate) const CALL_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) type Callback = Box<dyn FnOnce(Result<ResponseBody>) + Send>;

static POOL: LazyLock<Mutex<HashMap<String, Weak<Connection>>>> = LazyLock::new(Default::default);

pub(crate) struct Connection {
    address: String,
    session: Mutex<Option<Session>>,
}

struct Session {
    outbox: mpsc::UnboundedSender<QueuedRequest>,
    waiters: Arc<Waiters>,
}

#[derive(Clone)]
pub(crate) enum Binding {
    Expected(Arc<str>),
    Discover { slot: usize, journals: Arc<Mutex<Vec<Option<Arc<str>>>>> },
}

impl Binding {
    fn admits(&self, observed: &Arc<str>) -> bool {
        match self {
            Self::Expected(expected) => expected == observed,
            Self::Discover { slot, journals } => {
                let mut journals = journals.lock();
                if let Some(expected) = &journals[*slot] { return expected == observed; }
                if journals.contains(&Some(Arc::clone(observed))) { return false; }
                journals[*slot] = Some(Arc::clone(observed));
                true
            }
        }
    }
}

struct QueuedRequest {
    id: u64,
    body: RequestBody,
    binding: Binding,
}

enum Waiting {
    Open(HashMap<u64, Callback>),
    Failed(String),
}

struct Waiters {
    address: String,
    next_id: AtomicU64,
    waiting: Mutex<Waiting>,
}

impl Waiters {
    fn take(&self, id: u64) -> Option<Callback> {
        match &mut *self.waiting.lock() { Waiting::Open(waiting) => waiting.remove(&id), Waiting::Failed(_) => None }
    }

    fn fail(&self, reason: String) {
        let failed = std::mem::replace(&mut *self.waiting.lock(), Waiting::Failed(reason.clone()));
        if let Waiting::Open(waiting) = failed {
            for (_, callback) in waiting { callback(Err(remote(&self.address, reason.clone()))); }
        }
    }

    fn failed(&self) -> bool { matches!(*self.waiting.lock(), Waiting::Failed(_)) }
}

pub(crate) fn connection(address: &str) -> Arc<Connection> {
    let mut pool = POOL.lock();
    if let Some(connection) = pool.get(address).and_then(Weak::upgrade) { return connection; }
    pool.retain(|_, connection| connection.strong_count() > 0);
    let connection = Arc::new(Connection { address: address.to_owned(), session: Mutex::new(None) });
    pool.insert(address.to_owned(), Arc::downgrade(&connection));
    connection
}

impl Connection {
    pub fn address(&self) -> &str { &self.address }

    /// The callback runs on the connection's own task, or on a task of its own when the session has already failed, never on the caller's: callers send while holding locks their callbacks take.
    pub fn send(&self, binding: Binding, body: RequestBody, callback: Callback) {
        let (outbox, waiters) = {
            let mut session = self.session.lock();
            if session.as_ref().is_none_or(|session| session.waiters.failed() || session.outbox.is_closed()) {
                *session = Some(Session::open(self.address.clone()));
            }
            let session = session.as_ref().expect("opened above");
            (session.outbox.clone(), Arc::clone(&session.waiters))
        };
        let id = waiters.next_id.fetch_add(1, Ordering::Relaxed);
        let refused = match &mut *waiters.waiting.lock() {
            Waiting::Open(waiting) => { waiting.insert(id, callback); None }
            Waiting::Failed(reason) => Some((callback, reason.clone())),
        };
        if let Some((callback, reason)) = refused {
            let error = remote(&self.address, reason);
            tokio::spawn(async move { callback(Err(error)) });
            return;
        }
        if outbox.send(QueuedRequest { id, body, binding }).is_err()
            && let Some(callback) = waiters.take(id) {
            let error = remote(&self.address, "the connection closed");
            tokio::spawn(async move { callback(Err(error)) });
        }
    }

    pub async fn call(&self, binding: Binding, body: RequestBody) -> Result<ResponseBody> {
        let (sender, receiver) = oneshot::channel();
        self.send(binding, body, Box::new(move |result| { let _ = sender.send(result); }));
        match tokio::time::timeout(CALL_TIMEOUT, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(remote(&self.address, "the connection dropped a request")),
            Err(_) => Err(remote(&self.address, "request timed out")),
        }
    }
}

pub(crate) fn remote(address: &str, message: impl Into<String>) -> Error {
    Error::Remote { key: address.to_owned(), message: message.into() }
}

impl Session {
    fn open(address: String) -> Self {
        let (outbox, requests) = mpsc::unbounded_channel();
        let waiters = Arc::new(Waiters { address: address.clone(), next_id: AtomicU64::new(1), waiting: Mutex::new(Waiting::Open(HashMap::new())) });
        tokio::spawn(run(address, requests, Arc::clone(&waiters)));
        Self { outbox, waiters }
    }
}

/// Ends when the connection fails or every sender of the outbox is gone.
async fn run(address: String, requests: mpsc::UnboundedReceiver<QueuedRequest>, waiters: Arc<Waiters>) {
    if crate::local::is_local(&address) {
        match tokio::time::timeout(CONNECT_TIMEOUT, crate::local::connect(&address)).await {
            Ok(Ok(socket)) => {
                let (reader, writer) = tokio::io::split(socket);
                run_session(reader, writer, address, requests, waiters).await;
            }
            Ok(Err(error)) => waiters.fail(format!("cannot open local journal {address}: {error}")),
            Err(_) => waiters.fail(format!("timed out opening local journal {address}")),
        }
        return;
    }

    let socket = match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(&address)).await {
        Ok(Ok(socket)) => socket,
        Ok(Err(error)) => return waiters.fail(format!("cannot connect to {address}: {error}")),
        Err(_) => return waiters.fail(format!("timed out connecting to {address}")),
    };
    let _ = socket.set_nodelay(true);
    let (reader, writer) = socket.into_split();
    run_session(reader, writer, address, requests, waiters).await;
}

async fn run_session(
    mut reader: impl AsyncRead + Unpin,
    mut writer: impl AsyncWrite + Unpin,
    address: String,
    mut requests: mpsc::UnboundedReceiver<QueuedRequest>,
    waiters: Arc<Waiters>,
) {
    let nonce = *uuid::Uuid::new_v4().as_bytes();
    let identified = tokio::time::timeout(CONNECT_TIMEOUT, async {
        let mut frame = BytesMut::new();
        encode_frame(&Request { id: 0, body: Some(RequestBody::Identify(Identify { nonce: bytes::Bytes::copy_from_slice(&nonce) })), target_journal: String::new() }, &mut frame);
        writer.write_all(&frame).await?;
        read_frame::<Response>(&mut reader).await
    }).await;
    let identity: Arc<str> = match identified {
        Ok(Ok(Some(Response { id: 0, body: Some(ResponseBody::JournalIdentity(identity)) })))
            if crate::attestation::proves_identity(&identity.journal, &identity.public_key, &identity.signature, &nonce) => identity.journal.into(),
        Ok(Err(error)) => return waiters.fail(format!("could not identify the journal: {error}")),
        Err(_) => return waiters.fail("timed out identifying the journal".to_owned()),
        _ => return waiters.fail("the server did not advertise a journal identity".to_owned()),
    };
    let writing = async {
        let mut buffer = BytesMut::new();
        let encode = |queued: QueuedRequest, buffer: &mut BytesMut| {
            if queued.binding.admits(&identity) {
                encode_frame(&Request { id: queued.id, body: Some(queued.body), target_journal: identity.to_string() }, buffer);
            } else if let Some(callback) = waiters.take(queued.id) {
                callback(Err(remote(&address, format!("journal {identity} does not own this ensemble slot"))));
            }
        };
        while let Some(queued) = requests.recv().await {
            encode(queued, &mut buffer);
            while let Ok(queued) = requests.try_recv() { encode(queued, &mut buffer); }
            if buffer.is_empty() { continue; }
            writer.write_all(&buffer).await.map_err(|error| error.to_string())?;
            buffer.clear();
        }
        Ok::<_, String>(())
    };
    let reading = async {
        loop {
            match read_frame::<Response>(&mut reader).await {
                Ok(Some(response)) => {
                    let Some(callback) = waiters.take(response.id) else { continue };
                    callback(response.body.ok_or_else(|| remote(&address, "a reply without a body")));
                }
                Ok(None) => return "the server closed the connection".to_owned(),
                Err(error) => return error.to_string(),
            }
        }
    };
    let reason = tokio::select! {
        result = writing => result.err().unwrap_or_else(|| "the connection is no longer used".to_owned()),
        reason = reading => reason,
    };
    waiters.fail(reason);
}
