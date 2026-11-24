//! Any `Directory` over TCP, for multi-process local cells and tests.
//!
//! The protocol is newline-delimited JSON: each request carries an id, each response names the request it answers, and responses come back in completion order.
//! An `Anomaly` crosses the wire whole, so a server-side Conflict is a Conflict at the client.
//! A subscription to the server's change feed has a connection of its own: every current deployment record, then each change as it applies, until the connection closes.

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
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::{Cas, ChangeFeed, DeploymentRecord, Directory, ForkPreparation, ForkPublication, ReferenceDisposal, LineageChange, LineageRetirement, LineageState, MemberId, MemberRecord, Version};

/// A longer line closes the connection rather than growing the buffer without bound.
const MAX_LINE: u64 = 64 << 20;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Op {
    ReceiverClock { domain: uuid::Uuid },
    PublishReceiverClock { domain: uuid::Uuid, publisher: uuid::Uuid, observation: rtdb_types::Timestamp },
    CreateDeployment { record: DeploymentRecord },
    StageHostedFork { child: DeploymentRecord, preparation: ForkPreparation },
    CreateFork { child: DeploymentRecord, expected_parent: Version, #[serde(default)] preparation: Option<ForkPublication> },
    SharedReferenceRevision { lineage: String },
    LineageState { lineage: String },
    BeginReferenceDisposal { id: String, expected: Version },
    AuthorizeOwnedRetirement { id: String, expected: Version, retirement: LineageRetirement },
    SettleOwnedVersion { id: String, expected: Version, retirement: LineageRetirement },
    SettleReferenceDisposal { permit: ReferenceDisposal, record: DeploymentRecord, expected: Version },
    ChangeLineage { lineage: String, change: LineageChange },
    ReserveInstalledJournal { id: uuid::Uuid },
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
        matches!(self, Self::ReceiverClock { .. } | Self::Deployment { .. } | Self::DeploymentByName { .. } | Self::Deployments | Self::DeploymentsHeldBy { .. } | Self::Member { .. } | Self::Members | Self::SharedReferenceRevision { .. } | Self::LineageState { .. })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Reply {
    ReceiverClock(Option<crate::ReceiverClockSample>),
    ReceiverClockPublished,
    InstalledJournalReserved,
    Deployment(DeploymentRecord),
    OptionalDeployment(Option<DeploymentRecord>),
    Deployments(Vec<DeploymentRecord>),
    DeploymentCas(Cas<DeploymentRecord>),
    SharedReferenceRevision(Version),
    LineageState(LineageState),
    ReferenceDisposal(ReferenceDisposal),
    Member(MemberRecord),
    OptionalMember(Option<MemberRecord>),
    Members(Vec<MemberRecord>),
    MemberCas(Cas<MemberRecord>),
}

#[derive(Deserialize)]
struct Request { id: u64, op: Op }

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Feed { Deployments }

#[derive(Serialize, Deserialize)]
struct Subscribe { id: u64, subscribe: Feed }

#[derive(Deserialize)]
#[serde(untagged)]
enum Incoming { Request(Request), Subscribe(Subscribe) }

/// One message of a subscription; an error ends it, and the subscriber resubscribes to start again from a fresh snapshot.
#[derive(Serialize, Deserialize)]
struct Changed { id: u64, result: std::result::Result<DeploymentRecord, Anomaly> }

/// A request as sent: the op is borrowed, so a resend needs no clone.
#[derive(Serialize)]
struct Outgoing<'a> { id: u64, op: &'a Op }

#[derive(Deserialize)]
struct RequestId { id: u64 }

#[derive(Serialize, Deserialize)]
struct Response { id: u64, result: std::result::Result<Reply, Anomaly> }

async fn dispatch(directory: &dyn Directory, op: Op) -> Result<Reply> {
    Ok(match op {
        Op::ReceiverClock { domain } => Reply::ReceiverClock(directory.receiver_clock(domain).await?),
        Op::PublishReceiverClock { domain, publisher, observation } => {
            directory.publish_receiver_clock(domain, publisher, observation).await?;
            Reply::ReceiverClockPublished
        }
        Op::ReserveInstalledJournal { id } => {
            directory.reserve_installed_journal(id).await?;
            Reply::InstalledJournalReserved
        }
        Op::CreateDeployment { record } => Reply::Deployment(directory.create_deployment(record).await?),
        Op::StageHostedFork { child, preparation } => Reply::Deployment(directory.stage_hosted_fork(child, preparation).await?),
        Op::CreateFork { child, expected_parent, preparation } => Reply::Deployment(directory.publish_fork(child, expected_parent, preparation).await?),
        Op::SharedReferenceRevision { lineage } => Reply::SharedReferenceRevision(directory.shared_reference_revision(&lineage).await?),
        Op::LineageState { lineage } => Reply::LineageState(directory.lineage_state(&lineage).await?),
        Op::BeginReferenceDisposal { id, expected } => Reply::ReferenceDisposal(directory.begin_reference_disposal(&id, expected).await?),
        Op::AuthorizeOwnedRetirement { id, expected, retirement } => Reply::LineageState(directory.authorize_owned_retirement(&id, expected, retirement).await?),
        Op::SettleOwnedVersion { id, expected, retirement } => Reply::DeploymentCas(directory.settle_owned_version(&id, expected, retirement).await?),
        Op::SettleReferenceDisposal { permit, record, expected } => Reply::DeploymentCas(directory.settle_reference_disposal(&permit, record, expected).await?),
        Op::ChangeLineage { lineage, change } => Reply::LineageState(directory.change_lineage(&lineage, change).await?),
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

    let connection_closed = cancellation.child_token();
    let _closes_subscriptions = connection_closed.clone().drop_guard();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    loop {
        let more = tokio::select! {
            _ = cancellation.cancelled() => return,
            more = read_line(&mut reader, &mut line) => more,
        };
        if !matches!(more, Ok(true)) { return; }
        let request = match serde_json::from_str::<Incoming>(&line) {
            Ok(Incoming::Request(request)) => request,
            Ok(Incoming::Subscribe(Subscribe { id, subscribe: Feed::Deployments })) => {
                tracker.spawn(forward_changes(directory.clone(), id, responses.clone(), connection_closed.clone()));
                continue;
            }
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

/// Sends every current deployment record, then each change, until the connection closes or the feed lags.
/// Subscribing before the snapshot means no change between the two is lost; one that arrives older than the snapshot is dropped by the subscriber's `ChangeFeed`.
async fn forward_changes(directory: Arc<dyn Directory>, id: u64, responses: mpsc::UnboundedSender<String>, closed: CancellationToken) {
    let send = |result| responses.send(encode(&Changed { id, result })).is_ok();
    let Some(mut changes) = directory.deployment_changes() else {
        send(Err(Anomaly::unsupported("rtdb.directory/no-change-feed", "This directory has no change feed")));
        return;
    };
    match directory.deployments().await {
        Ok(records) => for record in records { if !send(Ok(record)) { return; } },
        Err(error) => { send(Err(error)); return; }
    }
    loop {
        let change = tokio::select! {
            _ = closed.cancelled() => return,
            change = changes.recv() => change,
        };
        let sent = match change {
            Ok(record) => send(Ok(record)),
            Err(broadcast::error::RecvError::Lagged(missed)) => {
                send(Err(Anomaly::new(AnomalyCategory::Busy, "rtdb.directory/changes-lost", format!("The subscriber fell {missed} changes behind"))));
                return;
            }
            Err(broadcast::error::RecvError::Closed) => { send(Err(unavailable("rtdb.directory/feed-closed", "The directory's change feed ended"))); return; }
        };
        if !sent { return; }
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
    /// Parent of every connection's `closed`, so dropping the client closes its connection and ends its subscription.
    shutdown: CancellationToken,
    /// The server's feed, relayed by a subscription started on the first `deployment_changes`.
    changes: Arc<ChangeFeed>,
    subscribed: Mutex<bool>,
}

impl RemoteDirectory {
    pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

    pub async fn connect(address: &str) -> Result<Arc<Self>> { Self::connect_with_timeout(address, Self::DEFAULT_REQUEST_TIMEOUT).await }

    /// Connects now, so an unreachable address fails here; `request_timeout` bounds each call, including any reconnect it makes.
    pub async fn connect_with_timeout(address: &str, request_timeout: Duration) -> Result<Arc<Self>> {
        let directory = Self { address: address.to_owned(), request_timeout, next_id: AtomicU64::new(1), connection: tokio::sync::Mutex::new(None),
            shutdown: CancellationToken::new(), changes: Arc::new(ChangeFeed::new()), subscribed: Mutex::new(false) };
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

const RESUBSCRIBE_MIN: Duration = Duration::from_millis(50);
const RESUBSCRIBE_MAX: Duration = Duration::from_secs(2);

/// Relays the server's feed into `changes`, resubscribing with backoff whenever the subscription ends, until `shutdown`.
/// Each subscription opens with a snapshot, so whatever changed while it was down reaches `changes` as the latest version.
async fn relay_changes(address: String, request_timeout: Duration, changes: Arc<ChangeFeed>, shutdown: CancellationToken) {
    let mut backoff = RESUBSCRIBE_MIN;
    loop {
        let received = tokio::select! {
            _ = shutdown.cancelled() => return,
            received = follow_changes(&address, request_timeout, &changes) => received,
        };
        if received { backoff = RESUBSCRIBE_MIN; }
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(RESUBSCRIBE_MAX);
    }
}

/// Follows one subscription until it ends; whether it delivered anything.
async fn follow_changes(address: &str, request_timeout: Duration, changes: &ChangeFeed) -> bool {
    let Ok(Ok(stream)) = tokio::time::timeout(request_timeout, TcpStream::connect(address)).await else { return false };
    let _ = stream.set_nodelay(true);
    let (read_half, mut write_half) = stream.into_split();
    if write_half.write_all(encode(&Subscribe { id: 0, subscribe: Feed::Deployments }).as_bytes()).await.is_err() { return false; }
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    let mut received = false;
    while let Ok(true) = read_line(&mut reader, &mut line).await {
        match serde_json::from_str::<Changed>(&line) {
            Ok(Changed { result: Ok(record), .. }) => { received = true; changes.publish(record); }
            _ => break,
        }
    }
    received
}

impl Drop for RemoteDirectory {
    fn drop(&mut self) { self.shutdown.cancel(); }
}

fn protocol(reply: Reply) -> Anomaly {
    Anomaly::fault("rtdb.directory/protocol", format!("The directory answered with an unexpected reply: {reply:?}"))
}

#[async_trait]
impl Directory for RemoteDirectory {
    async fn receiver_clock(&self, domain: uuid::Uuid) -> Result<Option<crate::ReceiverClockSample>> {
        match self.call(Op::ReceiverClock { domain }).await? {
            Reply::ReceiverClock(sample) => Ok(sample),
            other => Err(protocol(other)),
        }
    }

    async fn publish_receiver_clock(&self, domain: uuid::Uuid, publisher: uuid::Uuid, observation: rtdb_types::Timestamp) -> Result<()> {
        match self.call(Op::PublishReceiverClock { domain, publisher, observation }).await? {
            Reply::ReceiverClockPublished => Ok(()),
            other => Err(protocol(other)),
        }
    }

    async fn reserve_installed_journal(&self, id: uuid::Uuid) -> Result<()> {
        match self.call(Op::ReserveInstalledJournal { id }).await? {
            Reply::InstalledJournalReserved => Ok(()),
            other => Err(protocol(other)),
        }
    }
    async fn create_deployment(&self, record: DeploymentRecord) -> Result<DeploymentRecord> {
        match self.call(Op::CreateDeployment { record }).await? { Reply::Deployment(record) => Ok(record), other => Err(protocol(other)) }
    }
    async fn stage_hosted_fork(&self, child: DeploymentRecord, preparation: ForkPreparation) -> Result<DeploymentRecord> {
        match self.call(Op::StageHostedFork { child, preparation }).await? { Reply::Deployment(record) => Ok(record), other => Err(protocol(other)) }
    }
    async fn publish_fork(&self, child: DeploymentRecord, expected_parent: Version, preparation: Option<ForkPublication>) -> Result<DeploymentRecord> {
        match self.call(Op::CreateFork { child, expected_parent, preparation }).await? { Reply::Deployment(record) => Ok(record), other => Err(protocol(other)) }
    }
    async fn shared_reference_revision(&self, lineage: &str) -> Result<Version> {
        match self.call(Op::SharedReferenceRevision { lineage: lineage.to_owned() }).await? { Reply::SharedReferenceRevision(version) => Ok(version), other => Err(protocol(other)) }
    }
    async fn lineage_state(&self, lineage: &str) -> Result<LineageState> {
        match self.call(Op::LineageState { lineage: lineage.to_owned() }).await? { Reply::LineageState(state) => Ok(state), other => Err(protocol(other)) }
    }
    async fn change_lineage(&self, lineage: &str, change: LineageChange) -> Result<LineageState> {
        match self.call(Op::ChangeLineage { lineage: lineage.to_owned(), change }).await? { Reply::LineageState(state) => Ok(state), other => Err(protocol(other)) }
    }
    async fn authorize_owned_retirement(&self, id: &str, expected: Version, retirement: LineageRetirement) -> Result<LineageState> {
        match self.call(Op::AuthorizeOwnedRetirement { id: id.to_owned(), expected, retirement }).await? {
            Reply::LineageState(state) => Ok(state), other => Err(protocol(other)),
        }
    }
    async fn settle_owned_version(&self, id: &str, expected: Version, retirement: LineageRetirement) -> Result<Cas<DeploymentRecord>> {
        match self.call(Op::SettleOwnedVersion { id: id.to_owned(), expected, retirement }).await? {
            Reply::DeploymentCas(result) => Ok(result), other => Err(protocol(other)),
        }
    }
    async fn begin_reference_disposal(&self, id: &str, expected: Version) -> Result<ReferenceDisposal> {
        match self.call(Op::BeginReferenceDisposal { id: id.to_owned(), expected }).await? { Reply::ReferenceDisposal(permit) => Ok(permit), other => Err(protocol(other)) }
    }
    async fn settle_reference_disposal(&self, permit: &ReferenceDisposal, record: DeploymentRecord, expected: Version) -> Result<Cas<DeploymentRecord>> {
        match self.call(Op::SettleReferenceDisposal { permit: permit.clone(), record, expected }).await? { Reply::DeploymentCas(result) => Ok(result), other => Err(protocol(other)) }
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
    /// Outside a Tokio runtime there is nothing to relay the server's feed on, so there is no feed.
    fn change_feed(&self) -> Option<&ChangeFeed> {
        let mut subscribed = self.subscribed.lock();
        if !*subscribed {
            let runtime = tokio::runtime::Handle::try_current().ok()?;
            runtime.spawn(relay_changes(self.address.clone(), self.request_timeout, self.changes.clone(), self.shutdown.clone()));
            *subscribed = true;
        }
        Some(&self.changes)
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

    /// Waits until `receiver` has seen every one of `records`, which shows the subscription is established.
    async fn await_versions(receiver: &mut broadcast::Receiver<DeploymentRecord>, records: &[DeploymentRecord]) {
        let mut awaited = records.iter().map(|record| (record.id.clone(), record.version)).collect::<HashMap<_, _>>();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !awaited.is_empty() {
                let seen = receiver.recv().await.expect("the feed runs");
                if awaited.get(&seen.id).is_some_and(|version| seen.version >= *version) { awaited.remove(&seen.id); }
            }
        }).await.expect("the records arrive");
    }

    async fn await_version(receiver: &mut broadcast::Receiver<DeploymentRecord>, record: &DeploymentRecord) {
        await_versions(receiver, std::slice::from_ref(record)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_network_feed_delivers_every_change_in_version_order() {
        use std::collections::BTreeMap;
        let directory = MemoryDirectory::new();
        let served = served(directory.clone()).await;
        let remote = RemoteDirectory::connect(&served.address).await.unwrap();
        let mut records = Vec::new();
        for index in 0..4 { records.push(remote.create_deployment(DeploymentRecord::new(format!("shop-{index}"), "acme", vec![], 0)).await.unwrap()); }
        let mut receiver = remote.deployment_changes().expect("the remote directory relays the server's feed");
        await_versions(&mut receiver, &records).await;

        let seen = records.iter().map(|record| (record.id.clone(), record.version)).collect::<BTreeMap<_, _>>();
        let last = crate::feed::tests::replace_concurrently(remote.clone(), records, 50).await;
        crate::feed::tests::expect_every_change(&mut receiver, seen, &last).await;
        drop(remote);
        served.stop().await;
    }

    #[tokio::test]
    async fn the_network_feed_resubscribes_and_catches_up_after_the_server_restarts() {
        let directory = MemoryDirectory::new();
        let served = served(directory.clone()).await;
        let address = served.address.clone();
        let remote = RemoteDirectory::connect(&address).await.unwrap();
        let created = remote.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let mut receiver = remote.deployment_changes().unwrap();
        await_version(&mut receiver, &created).await;
        served.stop().await;

        let mut changed = created.clone();
        changed.epoch = 7;
        let changed = directory.replace_deployment(changed, created.version).await.unwrap().applied().unwrap();
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_reuseaddr(true).unwrap();
        socket.bind(address.parse().unwrap()).unwrap();
        let restarted = serve_on(socket.listen(64).unwrap(), directory).await;
        let seen = tokio::time::timeout(Duration::from_secs(10), receiver.recv()).await.expect("the change made while down arrives").unwrap();
        assert_eq!(seen, changed);
        drop(remote);
        restarted.stop().await;
    }

    #[tokio::test]
    async fn a_watch_over_the_network_sees_a_change_within_milliseconds() {
        let served = served(MemoryDirectory::new()).await;
        let remote = RemoteDirectory::connect(&served.address).await.unwrap();
        let created = remote.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let mut probe = remote.deployment_changes().unwrap();
        await_version(&mut probe, &created).await;
        let mut watched = crate::watch_deployment(remote.clone(), created.id.clone(), Duration::from_secs(60));
        tokio::time::timeout(Duration::from_secs(5), watched.changed()).await.unwrap().unwrap();

        let mut changed = created.clone();
        changed.epoch = 1;
        let changed = remote.replace_deployment(changed, created.version).await.unwrap().applied().unwrap();
        tokio::time::timeout(Duration::from_millis(500), watched.changed()).await.expect("the change arrives long before the next poll").unwrap();
        assert_eq!(watched.borrow().as_ref().unwrap().version, changed.version);
        drop(watched);
        drop(remote);
        served.stop().await;
    }
}
