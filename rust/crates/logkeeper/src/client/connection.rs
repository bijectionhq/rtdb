//! One multiplexed connection per server address in this process, shared by every stream that names it.
//! Sending never waits: a request joins the session's outbox in call order, and its reply is handed to a callback or a future.
//! A failed session fails every outstanding request, and the next request opens a new one.

use crate::protocol::{Request, RequestBody, Response, ResponseBody, encode_frame, read_frame};
use bytes::BytesMut;
use parking_lot::Mutex;
use rtdb_storage::{Error, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Weak};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
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
    outbox: mpsc::UnboundedSender<Request>,
    waiters: Arc<Waiters>,
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

    /// The callback runs on the connection's own task, or on the caller's when the session has already failed.
    pub fn send(&self, body: RequestBody, callback: Callback) {
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
        if let Some((callback, reason)) = refused { return callback(Err(remote(&self.address, reason))); }
        if outbox.send(Request { id, body: Some(body) }).is_err()
            && let Some(callback) = waiters.take(id) {
            callback(Err(remote(&self.address, "the connection closed")));
        }
    }

    pub async fn call(&self, body: RequestBody) -> Result<ResponseBody> {
        let (sender, receiver) = oneshot::channel();
        self.send(body, Box::new(move |result| { let _ = sender.send(result); }));
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
async fn run(address: String, mut requests: mpsc::UnboundedReceiver<Request>, waiters: Arc<Waiters>) {
    let socket = match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(&address)).await {
        Ok(Ok(socket)) => socket,
        Ok(Err(error)) => return waiters.fail(format!("cannot connect to {address}: {error}")),
        Err(_) => return waiters.fail(format!("timed out connecting to {address}")),
    };
    let _ = socket.set_nodelay(true);
    let (mut reader, mut writer) = socket.into_split();
    let writing = async {
        let mut buffer = BytesMut::new();
        while let Some(request) = requests.recv().await {
            encode_frame(&request, &mut buffer);
            while let Ok(request) = requests.try_recv() { encode_frame(&request, &mut buffer); }
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
