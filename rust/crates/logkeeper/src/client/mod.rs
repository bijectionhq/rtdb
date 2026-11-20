//! The quorum client: a `Log` over a stream replicated on an ensemble of logkeepers.

mod connection;
mod ensemble;
mod recovery;
mod writer;

use async_trait::async_trait;
use bytes::Bytes;
use ensemble::{Ack, Ensemble, MAXIMUM_BATCH_BYTES, MAXIMUM_BATCH_ENTRIES, check_retained};
use futures::stream::{FuturesUnordered, StreamExt};
use rtdb_storage::log::{AppendReceipt, Log, LogMetadata, LogOffset, LogRange, LogRecord, LogReplayTail, LogTail, ReplayHead};
use rtdb_storage::object_store::ReadReservation;
use rtdb_storage::{Error, Result};
use rtdb_types::{Anomaly, Timestamp};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use writer::{Tip, Writer};

/// Matches the read-side charge `LogRecord::retain` requires beyond the payload.
const RECORD_OVERHEAD_BYTES: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamConfig {
    pub servers: Vec<String>,
    pub stream: String,
    pub write_quorum: usize,
    pub maximum_message_bytes: usize,
}

pub struct QuorumLog { shared: Arc<Shared> }

struct Shared {
    ensemble: Arc<Ensemble>,
    role: Role,
    cancellation: CancellationToken,
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
    Ok(Arc::new(QuorumLog { shared: Arc::new(Shared { ensemble, role: Role::Writer(writer), cancellation }) }))
}

/// Reads committed entries from whichever reachable server has the highest commit lsn, failing over on error.
pub async fn open_reader(config: StreamConfig, cancellation: CancellationToken) -> rtdb_types::Result<Arc<QuorumLog>> {
    validate(&config)?;
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
    Ok(Arc::new(QuorumLog { shared: Arc::new(Shared { ensemble, role: Role::Reader(reader), cancellation }) }))
}

/// Fences the stream at `epoch` without becoming its writer; returns the highest lsn a write quorum of servers holds.
pub async fn seal(config: &StreamConfig, epoch: u64) -> rtdb_types::Result<LogOffset> {
    validate(config)?;
    if epoch == 0 { return Err(Anomaly::incorrect("rtdb.logkeeper/invalid-epoch", "epochs start at 1")); }
    let ensemble = Ensemble::new(config.clone());
    let replies = writer::seal_quorum(&ensemble, epoch).await?;
    Ok(LogOffset(replies.iter().map(|(_, sealed)| sealed.last_lsn).max().unwrap_or(-1)))
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

impl QuorumLog {
    /// For the stream's writer: succeeds once a write quorum of servers has confirmed, in a round that started after this call, that the stream is not sealed beyond the writer's epoch.
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
            Role::Reader(reader) => {
                let servers = self.ensemble.len();
                let start = reader.preferred.load(Ordering::Relaxed);
                let mut failure = None;
                let mut reached = false;
                for attempt in 0..servers {
                    let server = (start + attempt) % servers;
                    let wait = if attempt == 0 { wait } else { Duration::ZERO };
                    let read = tokio::select! {
                        _ = self.cancellation.cancelled() => return Err(Error::Closed("shutdown".into())),
                        read = self.ensemble.read(server, from, max_entries, wait) => read,
                    };
                    let entries = match read { Ok(entries) => entries, Err(error) => { failure = Some(error); continue } };
                    reached = true;
                    check_retained(from, &entries)?;
                    let latest = reader.latest.fetch_max(entries.commit_lsn, Ordering::AcqRel).max(entries.commit_lsn);
                    if entries.entries.is_empty() && entries.commit_lsn < latest && from <= latest { continue; }
                    reader.preferred.store(server, Ordering::Relaxed);
                    return Ok(entries.entries.into_iter().take_while(|entry| entry.lsn < to).map(record).collect());
                }
                if reached { Ok(Vec::new()) } else { Err(failure.unwrap_or_else(|| unavailable(&self.ensemble.config.stream, "no server reachable"))) }
            }
        }
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
                    return Ok((tip, entries.trim_lsn));
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
