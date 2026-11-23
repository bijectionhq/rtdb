//! The quorum client: a `Log` over a stream replicated on an ensemble of logkeepers.

mod audit;
mod connection;
mod ensemble;
pub(crate) mod recovery;
mod writer;

use async_trait::async_trait;
use bytes::Bytes;
use ensemble::{Ack, Ensemble, MAXIMUM_BATCH_BYTES, MAXIMUM_BATCH_ENTRIES, check_retained};
use futures::stream::{FuturesUnordered, StreamExt};
use prost::Message;
use rtdb_storage::log::{AppendReceipt, Log, LogMetadata, LogOffset, LogRange, LogRecord, LogReplayTail, LogTail, ReplayHead};
use rtdb_storage::object_store::ReadReservation;
use rtdb_storage::{Error, Result};
use rtdb_types::{Anomaly, JournalEndpoint, Timestamp, valid_journal_identity};
use std::collections::{BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use writer::{Tip, Writer};

pub use audit::{CertifiedRangeReader, RetainedAudit, audit_retained, certify_committed_prefix, certify_committed_range, copy_certified_range, verify_certified_range};

/// Matches the read-side charge `LogRecord::retain` requires beyond the payload.
const RECORD_OVERHEAD_BYTES: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamConfig {
    pub servers: Vec<JournalEndpoint>,
    pub stream: String,
    pub write_quorum: usize,
    pub maximum_message_bytes: usize,
}

/// One ensemble's span of a stream: it holds the stream's lsns from `start_lsn` up to the next loglet's `start_lsn` in a chain.
/// A chain lists a stream's loglets oldest first; one stream name names the stream on every ensemble, and a server may belong to several of them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Loglet {
    pub servers: Vec<JournalEndpoint>,
    pub start_lsn: i64,
}

/// Where a moved stream's new loglet starts holding its history.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyFrom {
    /// The previous loglet's start, or its trim point when that is higher: the new ensemble holds everything the previous one held.
    Retained,
    /// This lsn, kept between the previous loglet's trim point and one past its recovered tail.
    Lsn(i64),
}

pub struct QuorumLog { shared: Arc<Shared> }

struct Shared {
    ensemble: Arc<Ensemble>,
    /// The first lsn the current loglet holds; lower lsns are read from `earlier`.
    start: i64,
    earlier: Vec<Earlier>,
    role: Role,
    cancellation: CancellationToken,
}

/// A loglet before the current one, read for lsns in `[start, end)`, every one of them committed.
struct Earlier {
    ensemble: Ensemble,
    start: i64,
    end: i64,
    reader: Reader,
}

enum Role {
    Writer(Arc<Writer>),
    Reader(Reader),
}

struct Reader {
    /// The highest commit lsn any server has reported.
    latest: AtomicI64,
    preferred: AtomicUsize,
}

fn validate(config: &StreamConfig) -> rtdb_types::Result<()> {
    if config.servers.is_empty() || config.stream.is_empty() {
        return Err(Anomaly::incorrect("rtdb.logkeeper/invalid-stream-config", "a stream needs a name and at least one server"));
    }
    if config.servers.iter().map(JournalEndpoint::address).collect::<BTreeSet<_>>().len() != config.servers.len() {
        return Err(Anomaly::incorrect("rtdb.logkeeper/invalid-stream-config", "a server address can occupy only one slot per ensemble"));
    }
    let mut journals = BTreeSet::new();
    for journal in config.servers.iter().filter_map(JournalEndpoint::journal) {
        if !valid_journal_identity(journal) || !journals.insert(journal) {
            return Err(Anomaly::incorrect("rtdb.logkeeper/invalid-stream-config", "each member slot needs a distinct journal identity"));
        }
    }
    if !journals.is_empty() && journals.len() != config.servers.len() {
        return Err(Anomaly::incorrect("rtdb.logkeeper/invalid-stream-config", "a member ensemble needs an identity for every slot"));
    }
    if config.write_quorum == 0 || config.write_quorum > config.servers.len() {
        return Err(Anomaly::incorrect("rtdb.logkeeper/invalid-stream-config", format!("write quorum {} must be between 1 and the {} servers", config.write_quorum, config.servers.len())));
    }
    Ok(())
}

/// Seals every server at `epoch`, recovers the stream's committed history onto a write quorum, and returns the stream's only writer at that epoch.
/// A later `open_writer` or `seal` at a higher epoch deposes it: its pending and later appends then fail with `Error::Deposed`.
pub async fn open_writer(config: StreamConfig, epoch: u64, cancellation: CancellationToken) -> rtdb_types::Result<Arc<QuorumLog>> {
    validate(&config)?;
    if epoch == 0 { return Err(Anomaly::incorrect("rtdb.logkeeper/invalid-epoch", "writer epochs start at 1")); }
    let ensemble = Arc::new(Ensemble::new(config));
    let writer = Writer::open(Arc::clone(&ensemble), epoch).await?;
    Ok(Arc::new(QuorumLog { shared: Arc::new(Shared { ensemble, start: 0, earlier: Vec::new(), role: Role::Writer(writer), cancellation }) }))
}

fn loglet_config(loglet: &Loglet, like: &StreamConfig) -> rtdb_types::Result<StreamConfig> {
    let config = StreamConfig { servers: loglet.servers.clone(), ..like.clone() };
    validate(&config)?;
    Ok(config)
}

fn validate_chain(chain: &[Loglet]) -> rtdb_types::Result<()> {
    if chain.is_empty() { return Err(Anomaly::incorrect("rtdb.logkeeper/invalid-chain", "a chain needs at least one loglet")); }
    if chain.windows(2).any(|pair| pair[0].start_lsn >= pair[1].start_lsn) || chain[0].start_lsn < 0 {
        return Err(Anomaly::incorrect("rtdb.logkeeper/invalid-chain", "a chain's loglets start at rising lsns from 0"));
    }
    Ok(())
}

/// The loglets of `chain` below `start`, each read up to its successor's start or `start`.
fn earlier(chain: &[Loglet], start: i64, like: &StreamConfig) -> rtdb_types::Result<Vec<Earlier>> {
    let kept: Vec<&Loglet> = chain.iter().take_while(|loglet| loglet.start_lsn < start).collect();
    kept.iter().enumerate().map(|(index, loglet)| {
        let end = kept.get(index + 1).map_or(start, |next| next.start_lsn);
        let reader = Reader { latest: AtomicI64::new(end - 1), preferred: AtomicUsize::new(0) };
        Ok(Earlier { ensemble: Ensemble::new(loglet_config(loglet, like)?), start: loglet.start_lsn, end, reader })
    }).collect()
}

/// Moves the stream to `new.servers` at `epoch`: seals the last loglet of `chain` at `epoch`, recovers its committed tail, copies it onto the new ensemble from where `from` says, and returns the stream's only writer at that epoch, which appends to the new ensemble alone.
/// `chain` lists the stream's earlier loglets oldest first; each is read with `new`'s stream name and write quorum.
/// Lsns stay dense across the move, and [`QuorumLog::loglets`] gives the chain to record and hand to readers.
/// An `open_writer` or `seal` of the old ensemble at `epoch` or below is refused afterwards.
pub async fn open_writer_on(chain: Vec<Loglet>, new: StreamConfig, epoch: u64, from: CopyFrom, cancellation: CancellationToken) -> rtdb_types::Result<Arc<QuorumLog>> {
    validate(&new)?;
    validate_chain(&chain)?;
    if epoch == 0 { return Err(Anomaly::incorrect("rtdb.logkeeper/invalid-epoch", "writer epochs start at 1")); }
    let previous_loglet = chain.last().expect("validated non-empty");
    let previous = Ensemble::new(loglet_config(previous_loglet, &new)?);
    let ensemble = Arc::new(Ensemble::new(new.clone()));
    let (writer, start) = Writer::open_moved(&previous, previous_loglet.start_lsn, Arc::clone(&ensemble), epoch, from).await?;
    let earlier = earlier(&chain, start, &new)?;
    Ok(Arc::new(QuorumLog { shared: Arc::new(Shared { ensemble, start, earlier, role: Role::Writer(writer), cancellation }) }))
}

/// Copy a fenced stream into a fresh physical stream without changing its
/// offsets. Success requires a destination quorum to commit the copied tail.
pub async fn copy_retained(source: StreamConfig, destination: StreamConfig, epoch: u64) -> rtdb_types::Result<()> {
    validate(&source)?;
    validate(&destination)?;
    if epoch == 0 || source.stream == destination.stream {
        return Err(Anomaly::incorrect("rtdb.logkeeper/copy-identity", "A copy requires a new stream and nonzero fence"));
    }
    Writer::copy_retained(&Ensemble::new(source), &Ensemble::new(destination), epoch).await?;
    Ok(())
}

/// Quorum-commits one page of a reserved target genesis. A replayed page is
/// accepted only if its stored overlap agrees byte for byte. Only the terminal
/// page asserts that the destination ends at this page's last offset.
pub async fn bootstrap_committed_page(
    config: &StreamConfig, epoch: u64, checkpoint: LogOffset, first: LogOffset,
    entries: &[crate::protocol::Entry], terminal: bool,
) -> rtdb_types::Result<()> {
    validate(config)?;
    let bytes = entries.iter().try_fold(0usize, |total, entry| total.checked_add(entry.encoded_len() + 16));
    if epoch == 0 || checkpoint.0 < -1 || first.0 <= checkpoint.0 || entries.len() > 4096
        || bytes.is_none_or(|bytes| bytes as u64 > MAXIMUM_BATCH_BYTES)
        || first.0.checked_add(entries.len() as i64).is_none() || (entries.is_empty() && !terminal)
        || entries.iter().enumerate().any(|(offset, entry)|
            entry.lsn != first.0 + offset as i64 || entry.epoch == 0
                || entry.epoch > epoch || entry.payload.len() > config.maximum_message_bytes)
    {
        return Err(Anomaly::incorrect("rtdb.logkeeper/bootstrap", "The replica genesis page does not follow its checkpoint"));
    }
    Writer::bootstrap_committed_page(
        &Ensemble::new(config.clone()), epoch, checkpoint.0, first.0, entries, terminal,
    ).await?;
    Ok(())
}

/// Reads committed entries from whichever reachable server has the highest commit lsn, failing over on error.
pub async fn open_reader(config: StreamConfig, cancellation: CancellationToken) -> rtdb_types::Result<Arc<QuorumLog>> {
    validate(&config)?;
    let current = Loglet { servers: config.servers.clone(), start_lsn: 0 };
    open_reader_on(vec![current], config, cancellation).await
}

/// Reads a stream across its whole `chain`, oldest first: an lsn below a loglet's successor's start is read from that loglet.
/// `config` names the stream and its last loglet's servers, which must be the chain's last.
pub async fn open_reader_on(chain: Vec<Loglet>, config: StreamConfig, cancellation: CancellationToken) -> rtdb_types::Result<Arc<QuorumLog>> {
    validate(&config)?;
    validate_chain(&chain)?;
    let current = chain.last().expect("validated non-empty");
    if current.servers != config.servers {
        return Err(Anomaly::incorrect("rtdb.logkeeper/invalid-chain", "a reader's servers are its chain's last loglet's"));
    }
    let (start, earlier) = (current.start_lsn, earlier(&chain, current.start_lsn, &config)?);
    let ensemble = Arc::new(Ensemble::new(config));
    let probes: Vec<_> = (0..ensemble.len()).map(|server| { let ensemble = &ensemble; async move { (server, ensemble.read(server, 0, 0, Duration::ZERO).await) } })
        .collect::<FuturesUnordered<_>>().collect().await;
    let mut best: Option<(usize, i64)> = None;
    let mut failures = Vec::new();
    for (server, probe) in probes {
        match probe {
            Ok(entries) if best.is_none_or(|(_, commit)| entries.commit_lsn > commit) => best = Some((server, entries.commit_lsn)),
            Ok(_) => (),
            Err(error) => failures.push(error.to_string()),
        }
    }
    let (preferred, latest) = best.ok_or_else(|| Anomaly::from(Error::Remote { key: ensemble.config.stream.clone(), message: format!("no server reachable: {}", failures.join("; ")) }))?;
    let reader = Reader { latest: AtomicI64::new(latest), preferred: AtomicUsize::new(preferred) };
    Ok(Arc::new(QuorumLog { shared: Arc::new(Shared { ensemble, start, earlier, role: Role::Reader(reader), cancellation }) }))
}

/// Fences the stream at `epoch` without becoming its writer; returns the highest lsn a write quorum of servers holds.
pub async fn seal(config: &StreamConfig, epoch: u64) -> rtdb_types::Result<LogOffset> {
    validate(config)?;
    if epoch == 0 { return Err(Anomaly::incorrect("rtdb.logkeeper/invalid-epoch", "epochs start at 1")); }
    let ensemble = Ensemble::new(config.clone());
    let replies = writer::seal_quorum(&ensemble, epoch).await?;
    Ok(LogOffset(replies.iter().map(|(_, sealed)| sealed.last_lsn).max().unwrap_or(-1)))
}

/// Fences the original stream and settles exactly the history its normal
/// recovery would select. The settled epoch has no activated append writer.
pub async fn seal_recover_committed(config: &StreamConfig, epoch: u64) -> rtdb_types::Result<LogOffset> {
    validate(config)?;
    if epoch == 0 { return Err(Anomaly::incorrect("rtdb.logkeeper/invalid-epoch", "epochs start at 1")); }
    let ensemble = Ensemble::new(config.clone());
    Ok(LogOffset(Writer::seal_recover_committed(&ensemble, epoch).await?))
}

/// Lets every server drop entries at or below `through`, which must be committed.
pub async fn trim(config: &StreamConfig, through: LogOffset) -> rtdb_types::Result<()> {
    validate(config)?;
    let ensemble = Ensemble::new(config.clone());
    let replies: Vec<_> = (0..ensemble.len()).map(|server| ensemble.trim(server, through.0)).collect::<FuturesUnordered<_>>().collect().await;
    let trimmed = replies.iter().filter(|reply| matches!(reply, Ok(Ack::Accepted(_)))).count();
    if trimmed < config.write_quorum {
        return Err(Error::Remote { key: config.stream.clone(), message: format!("trimmed {trimmed} servers, a write quorum is {}", config.write_quorum) }.into());
    }
    Ok(())
}

/// Fences and permanently drops the stream on every configured server.
/// A retry at the same epoch completes the drop if a server missed the first attempt.
pub async fn drop_stream(config: &StreamConfig, epoch: u64) -> rtdb_types::Result<()> {
    validate(config)?;
    if epoch == 0 { return Err(Anomaly::incorrect("rtdb.logkeeper/invalid-epoch", "epochs start at 1")); }
    let ensemble = Ensemble::new(config.clone());
    let mut pending: FuturesUnordered<_> = (0..ensemble.len()).map(|server| {
        let ensemble = &ensemble;
        async move { (server, ensemble.drop_stream(server, epoch).await) }
    }).collect();
    let mut failures = Vec::new();
    while let Some((server, reply)) = pending.next().await {
        let failure = match reply {
            Ok(Ack::Accepted(_)) => continue,
            Ok(Ack::Refused(refused)) => format!("promised epoch {}", refused.promised),
            Err(error) => error.to_string(),
        };
        failures.push(format!("{}: {failure}", config.servers[server].address()));
    }
    if failures.is_empty() { return Ok(()); }
    Err(Error::Remote { key: config.stream.clone(), message: format!("stream drop is incomplete: {}", failures.join("; ")) }.into())
}

/// Retires a whole stream on every named journal. Each acknowledgment
/// follows the durable fence and deletion of the superseded segments.
pub async fn retire(config: &StreamConfig, epoch: u64) -> rtdb_types::Result<()> {
    validate(config)?;
    if epoch == 0 { return Err(Anomaly::incorrect("rtdb.logkeeper/invalid-epoch", "epochs start at 1")); }
    let ensemble = Ensemble::new(config.clone());
    let replies: Vec<_> = (0..ensemble.len()).map(|server| ensemble.retire(server, epoch)).collect::<FuturesUnordered<_>>().collect().await;
    let retired = replies.iter().filter(|reply| matches!(reply, Ok(Ack::Accepted(_)))).count();
    if retired != config.servers.len() {
        return Err(Error::Remote { key: config.stream.clone(), message: format!("retired {retired} of {} journals", config.servers.len()) }.into());
    }
    Ok(())
}

impl QuorumLog {
    /// Bounds writes completed before this call on the exact active ensemble.
    /// The caller must wait for committed application through the returned position.
    pub async fn read_index(&self, epoch: u64) -> Result<LogOffset> {
        let ensemble = &self.shared.ensemble;
        let quorum = ensemble.config.write_quorum;
        let unavailable = |message: &str| Error::Remote { key: ensemble.config.stream.clone(), message: message.into() };
        if quorum <= ensemble.len() / 2 { return Err(unavailable("read indexes require intersecting write quorums")); }
        let mut replies: FuturesUnordered<_> = (0..ensemble.len()).map(|server| ensemble.read_index(server, epoch)).collect();
        let mut confirmed = 0;
        let mut last = -1;
        loop {
            let reply = tokio::select! {
                _ = self.shared.cancellation.cancelled() => return Err(Error::Closed("quorum log closed".into())),
                reply = replies.next() => reply,
            };
            let Some(reply) = reply else { break };
            match reply {
                Ok(Ack::Accepted(held)) => {
                    confirmed += 1;
                    last = last.max(held.last_lsn);
                    if confirmed == quorum { return Ok(LogOffset(last)); }
                }
                Ok(Ack::Refused(refused)) if refused.promised > epoch => return Err(Error::Deposed { epoch, promised: refused.promised }),
                _ => (),
            }
            if confirmed + replies.len() < quorum { break; }
        }
        Err(unavailable("a write quorum did not confirm the active loglet"))
    }

    /// The stream's chain as this log reads it, oldest first, ending with the loglet it reads or appends to now.
    pub fn loglets(&self) -> Vec<Loglet> {
        let shared = &self.shared;
        shared.earlier.iter().map(|loglet| Loglet { servers: loglet.ensemble.config.servers.clone(), start_lsn: loglet.start })
            .chain([Loglet { servers: shared.ensemble.config.servers.clone(), start_lsn: shared.start }])
            .collect()
    }

    /// For the stream's writer: succeeds once a write quorum has confirmed the writer's promised and synced epoch in a round that started after this call.
    /// A read served afterwards from what the writer had committed is linearizable, since no successor can have committed before the call began; this involves no clock.
    /// Concurrent callers share one round, so a burst costs at most two; a server reporting a higher promise deposes the writer, failing this and its appends with `Error::Deposed`.
    pub async fn confirm_leadership(&self) -> rtdb_types::Result<()> {
        self.shared.check_open()?;
        let Role::Writer(writer) = &self.shared.role else { return Err(Error::ReadOnlyLog(PathBuf::from(&self.shared.ensemble.config.stream)).into()) };
        tokio::select! {
            _ = self.shared.cancellation.cancelled() => Err(Error::Closed("shutdown".into()).into()),
            confirmed = writer.confirm_leadership() => confirmed.map_err(Into::into),
        }
    }

    /// Confirms only the active epoch, using the installed read-only journal port.
    pub async fn confirm_authority(&self, port: u16) -> rtdb_types::Result<()> {
        self.shared.check_open()?;
        let Role::Writer(writer) = &self.shared.role else { return Err(Error::ReadOnlyLog(PathBuf::from(&self.shared.ensemble.config.stream)).into()) };
        tokio::select! {
            _ = self.shared.cancellation.cancelled() => Err(Error::Closed("shutdown".into()).into()),
            confirmed = writer.confirm_authority(port) => confirmed.map_err(Into::into),
        }
    }
}

fn record(entry: crate::protocol::Entry) -> LogRecord {
    LogRecord { metadata: LogMetadata { offset: LogOffset(entry.lsn), timestamp: Timestamp(entry.timestamp) }, payload: entry.payload }
}

fn unavailable(stream: &str, message: impl Into<String>) -> Error { Error::Remote { key: stream.to_owned(), message: message.into() } }

impl Shared {
    fn check_open(&self) -> Result<()> {
        if self.cancellation.is_cancelled() { return Err(Error::Closed("shutdown".into())); }
        Ok(())
    }

    fn latest(&self) -> Result<i64> {
        self.check_open()?;
        match &self.role {
            Role::Writer(writer) => writer.latest().map(|offset| offset.0),
            Role::Reader(reader) => Ok(reader.latest.load(Ordering::Acquire)),
        }
    }

    async fn await_tip(&self, writer: &Writer, after: i64, wait: Duration) -> Result<i64> {
        let mut tip = writer.tip();
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let current = match &*tip.borrow_and_update() {
                Tip::Open(commit) => *commit,
                Tip::Deposed { promised } => return Err(Error::Deposed { epoch: writer.epoch(), promised: *promised }),
            };
            if current > after || wait.is_zero() { return Ok(current); }
            tokio::select! {
                _ = self.cancellation.cancelled() => return Err(Error::Closed("shutdown".into())),
                changed = tokio::time::timeout_at(deadline, tip.changed()) => match changed {
                    Ok(Ok(())) => (),
                    Ok(Err(_)) => return Err(Error::Closed("writer dropped".into())),
                    Err(_) => return Ok(current),
                },
            }
        }
    }

    /// Committed records from `from` and below `to`, waiting up to `wait` for the first; empty means `from` is past the committed tip.
    async fn read(&self, from: i64, to: i64, max_entries: u32, wait: Duration) -> Result<Vec<LogRecord>> {
        self.check_open()?;
        if from < self.start {
            let Some(loglet) = self.earlier.iter().rev().find(|loglet| loglet.start <= from) else {
                return Err(Error::UnavailableLogRange { requested: from, first_retained: self.earlier.first().map_or(self.start, |loglet| loglet.start) });
            };
            let records = self.read_committed(&loglet.ensemble, &loglet.reader, from, to.min(loglet.end), max_entries, Duration::ZERO).await?;
            if records.is_empty() && from < to { return Err(unavailable(&self.ensemble.config.stream, format!("no server of the loglet starting at {} holds committed lsn {from}", loglet.start))); }
            return Ok(records);
        }
        match &self.role {
            Role::Writer(writer) => {
                let tip = self.await_tip(writer, from - 1, wait).await?;
                let to = to.min(tip + 1);
                if to <= from { return Ok(Vec::new()); }
                if let Some(records) = writer.buffered(from, to, max_entries as usize, MAXIMUM_BATCH_BYTES as usize) { return Ok(records); }
                let mut failure = None;
                for server in writer.live() {
                    match self.ensemble.fetch(server, from, to, max_entries).await {
                        Ok(fetched) => {
                            check_retained(from, &fetched)?;
                            if fetched.entries.first().is_some_and(|entry| entry.lsn == from) { return Ok(fetched.entries.into_iter().map(record).collect()); }
                        }
                        Err(error) => failure = Some(error),
                    }
                }
                Err(failure.unwrap_or_else(|| unavailable(&self.ensemble.config.stream, format!("no live server holds committed lsn {from}"))))
            }
            Role::Reader(reader) => self.read_committed(&self.ensemble, reader, from, to, max_entries, wait).await,
        }
    }

    /// Committed records of `ensemble` from `from` and below `to`, from the preferred server or, failing that, the others in turn.
    async fn read_committed(&self, ensemble: &Ensemble, reader: &Reader, from: i64, to: i64, max_entries: u32, wait: Duration) -> Result<Vec<LogRecord>> {
        let servers = ensemble.len();
        let start = reader.preferred.load(Ordering::Relaxed);
        let mut failure = None;
        let mut reached = false;
        for attempt in 0..servers {
            let server = (start + attempt) % servers;
            let wait = if attempt == 0 { wait } else { Duration::ZERO };
            let read = tokio::select! {
                _ = self.cancellation.cancelled() => return Err(Error::Closed("shutdown".into())),
                read = ensemble.read(server, from, max_entries, wait) => read,
            };
            let entries = match read { Ok(entries) => entries, Err(error) => { failure = Some(error); continue } };
            reached = true;
            check_retained(from, &entries)?;
            let latest = reader.latest.fetch_max(entries.commit_lsn, Ordering::AcqRel).max(entries.commit_lsn);
            if entries.entries.is_empty() && entries.commit_lsn < latest && from <= latest { continue; }
            reader.preferred.store(server, Ordering::Relaxed);
            return Ok(entries.entries.into_iter().take_while(|entry| entry.lsn < to).map(record).collect());
        }
        if reached { Ok(Vec::new()) } else { Err(failure.unwrap_or_else(|| unavailable(&ensemble.config.stream, "no server reachable"))) }
    }

    /// The trim point across the chain: the current loglet's, unless it leaves lsns below its start to an earlier loglet, whose own trim point then counts.
    async fn chain_trim(&self, current_trim: i64) -> Result<i64> {
        if current_trim >= self.start || self.earlier.is_empty() { return Ok(current_trim); }
        for loglet in &self.earlier {
            let mut failure = None;
            let mut trim = None;
            for server in 0..loglet.ensemble.len() {
                match loglet.ensemble.read(server, loglet.end, 0, Duration::ZERO).await {
                    Ok(entries) => { trim = Some(entries.trim_lsn); break; }
                    Err(error) => failure = Some(error),
                }
            }
            let trim = trim.ok_or_else(|| failure.unwrap_or_else(|| unavailable(&loglet.ensemble.config.stream, "no server reachable")))?;
            if trim < loglet.end - 1 { return Ok(trim.max(loglet.start - 1)); }
        }
        Ok(self.start - 1)
    }

    /// The committed tip and the trim point, asked of a server.
    async fn bounds(&self) -> Result<(i64, i64)> {
        self.check_open()?;
        let tip = self.latest()?;
        let servers = match &self.role {
            Role::Writer(writer) => writer.live(),
            Role::Reader(reader) => { let start = reader.preferred.load(Ordering::Relaxed); (0..self.ensemble.len()).map(|attempt| (start + attempt) % self.ensemble.len()).collect() }
        };
        let mut failure = None;
        for server in servers {
            let probe = match &self.role {
                Role::Writer(_) => self.ensemble.fetch(server, tip + 1, tip + 1, 0).await,
                Role::Reader(_) => self.ensemble.read(server, tip + 1, 0, Duration::ZERO).await,
            };
            match probe {
                Ok(entries) => {
                    let tip = match &self.role {
                        Role::Writer(_) => tip,
                        Role::Reader(reader) => reader.latest.fetch_max(entries.commit_lsn, Ordering::AcqRel).max(entries.commit_lsn),
                    };
                    return Ok((tip, self.chain_trim(entries.trim_lsn).await?));
                }
                Err(error) => failure = Some(error),
            }
        }
        Err(failure.unwrap_or_else(|| unavailable(&self.ensemble.config.stream, "no server reachable")))
    }

    fn allocation_bound(&self) -> usize { self.ensemble.config.maximum_message_bytes.saturating_add(RECORD_OVERHEAD_BYTES) }
}

#[async_trait]
impl Log for QuorumLog {
    async fn enqueue(&self, payload: Bytes, timestamp: Timestamp) -> Result<AppendReceipt> {
        let maximum = self.shared.ensemble.config.maximum_message_bytes;
        if payload.len() > maximum { return Err(Error::MessageTooLarge { size: payload.len(), maximum }); }
        self.shared.check_open()?;
        match &self.shared.role {
            Role::Writer(writer) => writer.enqueue(payload, timestamp),
            Role::Reader(_) => Err(Error::ReadOnlyLog(PathBuf::from(&self.shared.ensemble.config.stream))),
        }
    }

    fn latest(&self) -> Result<LogOffset> { self.shared.latest().map(LogOffset) }

    async fn retained_floor(&self) -> Result<Option<LogOffset>> {
        let (_, trim) = self.shared.bounds().await?;
        let first = trim.checked_add(1).ok_or_else(|| Error::Closed("log positions exhausted".into()))?;
        Ok(Some(LogOffset(first)))
    }

    async fn recovery_tail(&self, after: LogOffset) -> Result<Option<Box<dyn LogTail>>> {
        self.tail(after).await.map(Some)
    }

    async fn tail(&self, after: LogOffset) -> Result<Box<dyn LogTail>> {
        let next = after.0.checked_add(1).ok_or_else(|| Error::Closed("log positions exhausted".into()))?;
        let (tip, trim) = self.shared.bounds().await?;
        if next <= trim || next > tip + 1 { return Err(Error::UnavailableLogRange { requested: next, first_retained: trim + 1 }); }
        Ok(Box::new(QuorumTail { shared: Arc::clone(&self.shared), after: after.0, buffered: VecDeque::new() }))
    }

    async fn range(&self, from: LogOffset, to: LogOffset) -> Result<Box<dyn LogRange>> {
        if to < from { return Err(Error::Task("log range end precedes its start".into())); }
        let (tip, trim) = self.shared.bounds().await?;
        if from.0 <= trim { return Err(Error::UnavailableLogRange { requested: from.0, first_retained: trim + 1 }); }
        Ok(Box::new(QuorumRange { shared: Arc::clone(&self.shared), next: from.0, end: to.0.min(tip + 1), buffered: VecDeque::new() }))
    }

    async fn read_range(&self, from: LogOffset, to: LogOffset) -> Result<Vec<LogRecord>> {
        if to < from { return Err(Error::Task("log range end precedes its start".into())); }
        let (tip, trim) = self.shared.bounds().await?;
        if from.0 <= trim || from.0 > tip + 1 { return Err(Error::UnavailableLogRange { requested: from.0, first_retained: trim + 1 }); }
        let end = to.0.min(tip + 1);
        let mut records = Vec::new();
        let mut next = from.0;
        while next < end {
            let batch = self.shared.read(next, end, MAXIMUM_BATCH_ENTRIES, Duration::ZERO).await?;
            let Some(last) = batch.last() else { return Err(Error::UnavailableLogRange { requested: next, first_retained: trim + 1 }) };
            next = last.metadata.offset.0 + 1;
            records.extend(batch);
        }
        Ok(records)
    }
}

struct QuorumRange { shared: Arc<Shared>, next: i64, end: i64, buffered: VecDeque<LogRecord> }

#[async_trait]
impl LogRange for QuorumRange {
    fn allocation_bound(&self) -> usize { self.shared.allocation_bound() }

    async fn next(&mut self, reservation: ReadReservation) -> Result<Option<LogRecord>> {
        if reservation.length() < self.allocation_bound() { return Err(Error::Task("log read reservation is too small".into())); }
        if self.next >= self.end { return Ok(None); }
        if self.buffered.is_empty() {
            self.buffered.extend(self.shared.read(self.next, self.end, MAXIMUM_BATCH_ENTRIES, Duration::ZERO).await?);
        }
        let record = self.buffered.pop_front().ok_or(Error::UnavailableLogRange { requested: self.next, first_retained: self.end })?;
        self.next = record.metadata.offset.0 + 1;
        record.retain(reservation).map(Some)
    }
}

/// Delivers records it has already fetched before fetching more, so `poll` and replay may be mixed.
struct QuorumTail { shared: Arc<Shared>, after: i64, buffered: VecDeque<LogRecord> }

impl QuorumTail {
    async fn fill(&mut self, timeout: Duration) -> Result<()> {
        if self.buffered.is_empty() {
            self.buffered.extend(self.shared.read(self.after + 1, i64::MAX, MAXIMUM_BATCH_ENTRIES, timeout).await?);
        }
        Ok(())
    }
}

fn replay_head(record: &LogRecord) -> ReplayHead {
    ReplayHead { metadata: record.metadata, payload_len: record.payload.len(), allocation_bound: record.payload.len().saturating_add(RECORD_OVERHEAD_BYTES) }
}

#[async_trait]
impl LogTail for QuorumTail {
    async fn poll(&mut self, timeout: Duration) -> Result<Vec<LogRecord>> {
        self.fill(timeout).await?;
        let records: Vec<LogRecord> = self.buffered.drain(..).collect();
        if let Some(last) = records.last() { self.after = last.metadata.offset.0; }
        Ok(records)
    }

    fn replay(&mut self) -> Option<&mut dyn LogReplayTail> { Some(self) }
}

#[async_trait]
impl LogReplayTail for QuorumTail {
    async fn poll_head(&mut self, timeout: Duration) -> Result<Option<ReplayHead>> {
        self.fill(timeout).await?;
        Ok(self.buffered.front().map(replay_head))
    }

    async fn read_head(&mut self, head: ReplayHead, reservation: ReadReservation) -> Result<LogRecord> {
        self.shared.check_open()?;
        let record = self.buffered.front().ok_or_else(|| Error::Task("log replay head was not prepared".into()))?;
        if replay_head(record) != head { return Err(Error::Task("log replay head does not match the next record".into())); }
        if reservation.length() < head.allocation_bound { return Err(Error::Task("log read reservation is too small".into())); }
        let record = self.buffered.pop_front().expect("checked above").retain(reservation)?;
        self.after = record.metadata.offset.0;
        Ok(record)
    }
}
