//! Any `Directory` over TCP, for multi-process local cells and tests.
//!
//! The protocol is newline-delimited JSON: each request carries an id, each response names the request it answers, and responses come back in completion order.
//! An `Anomaly` crosses the wire whole, so a server-side Conflict is a Conflict at the client.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::{Cas, DeploymentRecord, Directory, MemberId, MemberRecord, Version};

/// A longer line closes the connection rather than growing the buffer without bound.
const MAX_LINE: u64 = 64 << 20;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Op {
    CreateDeployment { record: DeploymentRecord },
    Deployment { id: String },
    DeploymentByName { name: String },
    Deployments,
    DeploymentsHeldBy { member: MemberId },
    ReplaceDeployment { record: DeploymentRecord, expected: Version },
    RegisterMember { record: MemberRecord },
    Member { id: MemberId },
    Members,
    ReplaceMember { record: MemberRecord, expected: Version },
}

impl Op {
    /// A read may be resent after a lost connection; a write may already have applied.
    fn is_read(&self) -> bool {
        matches!(self, Self::Deployment { .. } | Self::DeploymentByName { .. } | Self::Deployments | Self::DeploymentsHeldBy { .. } | Self::Member { .. } | Self::Members)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Reply {
    Deployment(DeploymentRecord),
    OptionalDeployment(Option<DeploymentRecord>),
    Deployments(Vec<DeploymentRecord>),
    DeploymentCas(Cas<DeploymentRecord>),
    Member(MemberRecord),
    OptionalMember(Option<MemberRecord>),
    Members(Vec<MemberRecord>),
    MemberCas(Cas<MemberRecord>),
}

#[derive(Deserialize)]
struct Request { id: u64, op: Op }

/// A request as sent: the op is borrowed, so a resend needs no clone.
#[derive(Serialize)]
struct Outgoing<'a> { id: u64, op: &'a Op }

#[derive(Deserialize)]
struct RequestId { id: u64 }

#[derive(Serialize, Deserialize)]
struct Response { id: u64, result: std::result::Result<Reply, Anomaly> }

async fn dispatch(directory: &dyn Directory, op: Op) -> Result<Reply> {
    Ok(match op {
        Op::CreateDeployment { record } => Reply::Deployment(directory.create_deployment(record).await?),
        Op::Deployment { id } => Reply::OptionalDeployment(directory.deployment(&id).await?),
        Op::DeploymentByName { name } => Reply::OptionalDeployment(directory.deployment_by_name(&name).await?),
        Op::Deployments => Reply::Deployments(directory.deployments().await?),
        Op::DeploymentsHeldBy { member } => Reply::Deployments(directory.deployments_held_by(&member).await?),
        Op::ReplaceDeployment { record, expected } => Reply::DeploymentCas(directory.replace_deployment(record, expected).await?),
        Op::RegisterMember { record } => Reply::Member(directory.register_member(record).await?),
        Op::Member { id } => Reply::OptionalMember(directory.member(&id).await?),
        Op::Members => Reply::Members(directory.members().await?),
        Op::ReplaceMember { record, expected } => Reply::MemberCas(directory.replace_member(record, expected).await?),
    })
}

/// Reads one line of at most `MAX_LINE` bytes into `line`; `false` at end of stream or on an over-long line.
async fn read_line<R: tokio::io::AsyncBufRead + Unpin>(reader: &mut R, line: &mut String) -> std::io::Result<bool> {
    line.clear();
    let read = reader.take(MAX_LINE).read_line(line).await?;
    Ok(read > 0 && line.ends_with('\n'))
}

/// Serves `directory` on `listener` until `cancellation` fires, then waits for the requests already being answered.
/// Requests on one connection are answered concurrently.
pub async fn serve(listener: TcpListener, directory: Arc<dyn Directory>, cancellation: CancellationToken) -> Result<()> {
    let tracker = TaskTracker::new();
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => { tracker.spawn(serve_connection(stream, directory.clone(), cancellation.clone(), tracker.clone())); }
                // Typically descriptor exhaustion, which clears as connections close.
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            },
        }
    }
    tracker.close();
    tracker.wait().await;
    Ok(())
}

async fn serve_connection(stream: TcpStream, directory: Arc<dyn Directory>, cancellation: CancellationToken, tracker: TaskTracker) {
    let _ = stream.set_nodelay(true);
    let (read_half, write_half) = stream.into_split();
    let (responses, mut outgoing) = mpsc::unbounded_channel::<String>();
    let writer_cancellation = cancellation.clone();
    tracker.spawn(async move {
        let mut writer = BufWriter::new(write_half);
        loop {
            let line = tokio::select! {
                _ = writer_cancellation.cancelled() => return,
                line = outgoing.recv() => match line { Some(line) => line, None => return },
            };
            if writer.write_all(line.as_bytes()).await.is_err() { return; }
            if outgoing.is_empty() && writer.flush().await.is_err() { return; }
        }
    });

    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    loop {
        let more = tokio::select! {
            _ = cancellation.cancelled() => return,
            more = read_line(&mut reader, &mut line) => more,
        };
        if !matches!(more, Ok(true)) { return; }
        let request = match serde_json::from_str::<Request>(&line) {
            Ok(request) => request,
            Err(error) => {
                let Ok(RequestId { id }) = serde_json::from_str(&line) else { return };
                let refusal = Anomaly::incorrect("rtdb.directory/malformed-request", format!("Malformed directory request: {error}"));
                let _ = responses.send(encode(&Response { id, result: Err(refusal) }));
                continue;
            }
        };
        let (directory, responses) = (directory.clone(), responses.clone());
        tracker.spawn(async move {
            let result = dispatch(directory.as_ref(), request.op).await;
            let _ = responses.send(encode(&Response { id: request.id, result }));
        });
    }
}

fn encode<T: Serialize>(value: &T) -> String {
    let mut line = serde_json::to_string(value).expect("directory messages serialize");
    line.push('\n');
    line
}

fn unavailable(code: &str, message: impl Into<String>) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, code, message) }

fn lost(write: bool) -> Anomaly {
    let message = if write { "The directory connection was lost; the write may or may not have applied" } else { "The directory connection was lost" };
    unavailable("rtdb.directory/connection-lost", message)
}

/// One connection to a directory server.
struct Connection {
    outgoing: mpsc::UnboundedSender<String>,
    /// Requests awaiting their response; drained, under the same lock, when `closed` fires.
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Reply>>>>,
    closed: CancellationToken,
}

impl Connection {
    async fn open(address: &str, closed: CancellationToken) -> Result<Arc<Self>> {
        let stream = TcpStream::connect(address).await
            .map_err(|error| unavailable("rtdb.directory/connect", format!("Cannot reach the directory at {address}: {error}")).with_context("address", address.into()))?;
        let _ = stream.set_nodelay(true);
        let (read_half, write_half) = stream.into_split();
        let (outgoing, mut lines) = mpsc::unbounded_channel::<String>();
        let connection = Arc::new(Self { outgoing, pending: Mutex::new(HashMap::new()), closed });

        let writer_closed = connection.closed.clone();
        tokio::spawn(async move {
            let mut writer = BufWriter::new(write_half);
            loop {
                let line = tokio::select! {
                    _ = writer_closed.cancelled() => return,
                    line = lines.recv() => match line { Some(line) => line, None => return },
                };
                let written = async { writer.write_all(line.as_bytes()).await?; if lines.is_empty() { writer.flush().await?; } std::io::Result::Ok(()) }.await;
                if written.is_err() { writer_closed.cancel(); return; }
            }
        });

        let reader_connection = connection.clone();
        tokio::spawn(async move {
            let connection = reader_connection;
            let mut reader = BufReader::new(read_half);
            let mut line = String::new();
            loop {
                let more = tokio::select! {
                    _ = connection.closed.cancelled() => break,
                    more = read_line(&mut reader, &mut line) => more,
                };
                if !matches!(more, Ok(true)) { break; }
                let Ok(response) = serde_json::from_str::<Response>(&line) else { break };
                if let Some(waiter) = connection.pending.lock().remove(&response.id) { let _ = waiter.send(response.result); }
            }
            connection.close();
        });
        Ok(connection)
    }

    fn close(&self) {
        let drained = {
            let mut pending = self.pending.lock();
            self.closed.cancel();
            std::mem::take(&mut *pending)
        };
        // Dropping each sender tells its waiter the connection is gone.
        drop(drained);
    }
}

/// Removes a request's waiter if the request is abandoned, as on a timeout.
struct Waiting<'a> { connection: &'a Connection, id: u64 }

impl Drop for Waiting<'_> {
    fn drop(&mut self) { self.connection.pending.lock().remove(&self.id); }
}

enum Attempt {
    Answered(Result<Reply>),
    /// The request never left this process, so it may be resent whatever it is.
    NotSent,
    /// The connection closed after the request was handed to it.
    Lost,
}

/// A `Directory` served by `serve` in another process.
/// One connection is shared by every caller and requests on it are pipelined; a lost connection is replaced on the next request.
pub struct RemoteDirectory {
    address: String,
    request_timeout: Duration,
    next_id: AtomicU64,
    connection: tokio::sync::Mutex<Option<Arc<Connection>>>,
    /// Parent of every connection's `closed`, so dropping the client closes its connection.
    shutdown: CancellationToken,
}

impl RemoteDirectory {
    pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

    pub async fn connect(address: &str) -> Result<Arc<Self>> { Self::connect_with_timeout(address, Self::DEFAULT_REQUEST_TIMEOUT).await }

    /// Connects now, so an unreachable address fails here; `request_timeout` bounds each call, including any reconnect it makes.
    pub async fn connect_with_timeout(address: &str, request_timeout: Duration) -> Result<Arc<Self>> {
        let directory = Self { address: address.to_owned(), request_timeout, next_id: AtomicU64::new(1), connection: tokio::sync::Mutex::new(None),
            shutdown: CancellationToken::new() };
        tokio::time::timeout(request_timeout, directory.connection()).await.map_err(|_| directory.timed_out(false))??;
        Ok(Arc::new(directory))
    }

    async fn connection(&self) -> Result<Arc<Connection>> {
        let mut current = self.connection.lock().await;
        if let Some(connection) = current.as_ref().filter(|connection| !connection.closed.is_cancelled()) { return Ok(connection.clone()); }
        let connection = Connection::open(&self.address, self.shutdown.child_token()).await?;
        *current = Some(connection.clone());
        Ok(connection)
    }

    fn timed_out(&self, write: bool) -> Anomaly {
        let message = if write { "The directory did not answer in time; the write may or may not have applied" } else { "The directory did not answer in time" };
        unavailable("rtdb.directory/timeout", message).with_context("address", self.address.clone().into())
    }

    async fn attempt(&self, connection: &Connection, op: &Op) -> Attempt {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        {
            let mut pending = connection.pending.lock();
            if connection.closed.is_cancelled() { return Attempt::NotSent; }
            pending.insert(id, sender);
        }
        let waiting = Waiting { connection, id };
        if connection.outgoing.send(encode(&Outgoing { id, op })).is_err() { return Attempt::NotSent; }
        let answer = receiver.await;
        drop(waiting);
        match answer { Ok(result) => Attempt::Answered(result), Err(_) => Attempt::Lost }
    }

    async fn call(&self, op: Op) -> Result<Reply> {
        let write = !op.is_read();
        let exchange = async {
            let mut resent = false;
            loop {
                let connection = self.connection().await?;
                match self.attempt(&connection, &op).await {
                    Attempt::Answered(result) => return result,
                    Attempt::NotSent => continue,
                    Attempt::Lost if !write && !resent => resent = true,
                    Attempt::Lost => return Err(lost(write)),
                }
            }
        };
        tokio::time::timeout(self.request_timeout, exchange).await.map_err(|_| self.timed_out(write))?
    }
}

impl Drop for RemoteDirectory {
    fn drop(&mut self) { self.shutdown.cancel(); }
}

fn protocol(reply: Reply) -> Anomaly {
    Anomaly::fault("rtdb.directory/protocol", format!("The directory answered with an unexpected reply: {reply:?}"))
}

#[async_trait]
impl Directory for RemoteDirectory {
    async fn create_deployment(&self, record: DeploymentRecord) -> Result<DeploymentRecord> {
        match self.call(Op::CreateDeployment { record }).await? { Reply::Deployment(record) => Ok(record), other => Err(protocol(other)) }
    }
    async fn deployment(&self, id: &str) -> Result<Option<DeploymentRecord>> {
        match self.call(Op::Deployment { id: id.to_owned() }).await? { Reply::OptionalDeployment(record) => Ok(record), other => Err(protocol(other)) }
    }
    async fn deployment_by_name(&self, name: &str) -> Result<Option<DeploymentRecord>> {
        match self.call(Op::DeploymentByName { name: name.to_owned() }).await? { Reply::OptionalDeployment(record) => Ok(record), other => Err(protocol(other)) }
    }
    async fn deployments(&self) -> Result<Vec<DeploymentRecord>> {
        match self.call(Op::Deployments).await? { Reply::Deployments(records) => Ok(records), other => Err(protocol(other)) }
    }
    async fn deployments_held_by(&self, member: &MemberId) -> Result<Vec<DeploymentRecord>> {
        match self.call(Op::DeploymentsHeldBy { member: member.clone() }).await? { Reply::Deployments(records) => Ok(records), other => Err(protocol(other)) }
    }
    async fn replace_deployment(&self, record: DeploymentRecord, expected: Version) -> Result<Cas<DeploymentRecord>> {
        match self.call(Op::ReplaceDeployment { record, expected }).await? { Reply::DeploymentCas(cas) => Ok(cas), other => Err(protocol(other)) }
    }
    async fn register_member(&self, record: MemberRecord) -> Result<MemberRecord> {
        match self.call(Op::RegisterMember { record }).await? { Reply::Member(record) => Ok(record), other => Err(protocol(other)) }
    }
    async fn member(&self, id: &MemberId) -> Result<Option<MemberRecord>> {
        match self.call(Op::Member { id: id.clone() }).await? { Reply::OptionalMember(record) => Ok(record), other => Err(protocol(other)) }
    }
    async fn members(&self) -> Result<Vec<MemberRecord>> {
        match self.call(Op::Members).await? { Reply::Members(records) => Ok(records), other => Err(protocol(other)) }
    }
    async fn replace_member(&self, record: MemberRecord, expected: Version) -> Result<Cas<MemberRecord>> {
        match self.call(Op::ReplaceMember { record, expected }).await? { Reply::MemberCas(cas) => Ok(cas), other => Err(protocol(other)) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{contract, MemoryDirectory};

    struct Served { address: String, cancellation: CancellationToken, server: tokio::task::JoinHandle<Result<()>> }

    impl Served {
        async fn stop(self) {
            self.cancellation.cancel();
            self.server.await.expect("server task").expect("server");
        }
    }

    async fn serve_on(listener: TcpListener, directory: Arc<dyn Directory>) -> Served {
        let address = listener.local_addr().unwrap().to_string();
        let cancellation = CancellationToken::new();
        let server = tokio::spawn(serve(listener, directory, cancellation.clone()));
        Served { address, cancellation, server }
    }

    async fn served(directory: Arc<dyn Directory>) -> Served { serve_on(TcpListener::bind("127.0.0.1:0").await.unwrap(), directory).await }

    #[tokio::test]
    async fn a_served_memory_directory_meets_the_contract() {
        let served = served(MemoryDirectory::new()).await;
        let remote = RemoteDirectory::connect(&served.address).await.unwrap();
        contract::run(remote.as_ref()).await;
        drop(remote);
        served.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_claims_over_the_network_have_one_winner_per_epoch() {
        let served = served(MemoryDirectory::new()).await;
        let mut clients: Vec<Arc<dyn Directory>> = Vec::new();
        for _ in 0..4 { clients.push(RemoteDirectory::connect(&served.address).await.unwrap()); }
        contract::concurrent_claims(&clients, 16, 25).await;
        drop(clients);
        served.stop().await;
    }

    #[tokio::test]
    async fn a_server_side_conflict_stays_a_conflict() {
        let served = served(MemoryDirectory::new()).await;
        let remote = RemoteDirectory::connect(&served.address).await.unwrap();
        remote.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let refused = remote.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap_err();
        assert_eq!(refused, crate::exists("Deployment name", "shop"));
        drop(remote);
        served.stop().await;
    }

    #[tokio::test]
    async fn a_client_reconnects_after_the_server_restarts() {
        let directory = MemoryDirectory::new();
        let served = served(directory.clone()).await;
        let address = served.address.clone();
        let remote = RemoteDirectory::connect(&address).await.unwrap();
        let created = remote.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        served.stop().await;

        assert!(remote.deployment(&created.id).await.is_err(), "nothing answers while the server is down");
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_reuseaddr(true).unwrap();
        socket.bind(address.parse().unwrap()).unwrap();
        let restarted = serve_on(socket.listen(64).unwrap(), directory).await;
        assert_eq!(remote.deployment(&created.id).await.unwrap(), Some(created));
        drop(remote);
        restarted.stop().await;
    }

    #[tokio::test]
    async fn a_silent_server_times_out_as_unavailable() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let silent = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(60)).await;
            drop(stream);
        });
        let remote = RemoteDirectory::connect_with_timeout(&address, Duration::from_millis(100)).await.unwrap();
        let refused = remote.deployment("absent").await.unwrap_err();
        assert_eq!((refused.category, refused.code.as_str()), (AnomalyCategory::Unavailable, "rtdb.directory/timeout"));
        silent.abort();
    }
}
