//! The writer of one stream at one epoch: recovery on open, pipelined appends, quorum commit, lazy repair of lagging servers.

use super::ensemble::{Ack, Ensemble, MAXIMUM_BATCH_ENTRIES};
use super::recovery::{Holding, plan_repair, winner};
use crate::protocol::{Append, Commit, Entry, RequestBody, Sealed};
use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use rtdb_storage::log::{AppendReceipt, LogMetadata, LogOffset, LogRecord};
use rtdb_storage::{Error, Result};
use rtdb_types::Timestamp;
use std::collections::VecDeque;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, watch};

const REPAIR_BACKOFF: Duration = Duration::from_millis(200);
const SEAL_GRACE: Duration = Duration::from_millis(100);
/// Committed entries kept for the writer's own tails, beyond those still awaiting a quorum.
const RECENT_ENTRIES: usize = 4096;
const RECENT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) enum Tip {
    Open(i64),
    Deposed { promised: u64 },
}

enum Link {
    Live,
    Repairing,
    Down(Instant),
}

struct Replica {
    /// The highest lsn this server has acknowledged durable at this writer's epoch.
    acked: i64,
    link: Link,
}

struct Buffered {
    entry: Entry,
    receipt: Option<oneshot::Sender<Result<LogMetadata>>>,
}

struct Pipeline {
    next_lsn: i64,
    commit: i64,
    history: Holding,
    /// Entry `i` has lsn `buffer_first + i`; it holds every uncommitted entry and some recent committed ones.
    buffer_first: i64,
    buffer: VecDeque<Buffered>,
    buffered_bytes: usize,
    replicas: Vec<Replica>,
}

enum State {
    Open(Pipeline),
    Deposed { promised: u64 },
}

pub(crate) struct Writer {
    ensemble: Arc<Ensemble>,
    epoch: u64,
    state: Mutex<State>,
    tip: watch::Sender<Tip>,
}

fn record(entry: &Entry) -> LogRecord {
    LogRecord { metadata: LogMetadata { offset: LogOffset(entry.lsn), timestamp: Timestamp(entry.timestamp) }, payload: entry.payload.clone() }
}

/// Seals every server and gathers a write quorum of replies, plus any that arrive shortly after.
pub(crate) async fn seal_quorum(ensemble: &Ensemble, epoch: u64) -> Result<Vec<(usize, Sealed)>> {
    let mut pending: FuturesUnordered<_> = (0..ensemble.len()).map(|server| async move { (server, ensemble.seal(server, epoch).await) }).collect();
    let mut replies = Vec::new();
    let mut failures = Vec::new();
    let mut grace = None;
    loop {
        let next = match grace {
            None => pending.next().await,
            Some(deadline) => match tokio::time::timeout_at(deadline, pending.next()).await { Ok(next) => next, Err(_) => None },
        };
        let Some((server, reply)) = next else { break };
        match reply {
            Ok(sealed) if sealed.promised > epoch => return Err(Error::Deposed { epoch, promised: sealed.promised }),
            Ok(sealed) => replies.push((server, sealed)),
            Err(error) => failures.push(error.to_string()),
        }
        if replies.len() >= ensemble.config.write_quorum && grace.is_none() { grace = Some(tokio::time::Instant::now() + SEAL_GRACE); }
    }
    if replies.len() < ensemble.config.write_quorum {
        return Err(Error::Remote { key: ensemble.config.stream.clone(), message: format!("sealed {} of {} servers, a write quorum is {}: {}", replies.len(), ensemble.len(), ensemble.config.write_quorum, failures.join("; ")) });
    }
    Ok(replies)
}

fn deposed_by(epoch: u64, ack: Ack) -> Result<i64> {
    match ack {
        Ack::Accepted(accepted) => Ok(accepted.last_lsn),
        Ack::Refused(refused) if refused.promised > epoch => Err(Error::Deposed { epoch, promised: refused.promised }),
        Ack::Refused(refused) => Err(Error::Remote { key: String::new(), message: format!("refused at promised epoch {}, last lsn {}", refused.promised, refused.last_lsn) }),
    }
}

/// Copies `source`'s entries `[from, to]` onto `server` from `from`, marking it synced with its last chunk.
async fn copy(ensemble: &Ensemble, epoch: u64, source: usize, server: usize, mut from: i64, to: i64) -> Result<()> {
    loop {
        let entries = if from > to { Vec::new() } else {
            let fetched = ensemble.fetch(source, from, to + 1, MAXIMUM_BATCH_ENTRIES).await?;
            super::ensemble::check_retained(from, &fetched)?;
            if fetched.entries.first().is_none_or(|entry| entry.lsn != from) {
                return Err(Error::Remote { key: ensemble.config.stream.clone(), message: format!("the recovery source no longer holds lsn {from}") });
            }
            fetched.entries
        };
        let last = entries.last().map_or(from - 1, |entry| entry.lsn);
        let complete = last >= to;
        let acked = deposed_by(epoch, ensemble.overwrite(server, epoch, from, entries, complete).await?)?;
        if complete { return Ok(()); }
        from = acked + 1;
    }
}

/// Brings one sealed server to exactly the recovered history and syncs it at this epoch.
async fn sync_server(ensemble: &Ensemble, epoch: u64, server: usize, sealed: &Sealed, source: usize, history: &Holding) -> Result<()> {
    let plan = plan_repair(&Holding::from(sealed), history).map_err(|message| Error::Remote { key: ensemble.config.stream.clone(), message })?;
    if let Some(through) = plan.trim_through { deposed_by(epoch, ensemble.trim(server, through).await?)?; }
    copy(ensemble, epoch, source, server, plan.from, history.last).await
}

impl Writer {
    pub async fn open(ensemble: Arc<Ensemble>, epoch: u64) -> Result<Arc<Self>> {
        let replies = seal_quorum(&ensemble, epoch).await?;
        let sealed: Vec<Sealed> = replies.iter().map(|(_, sealed)| sealed.clone()).collect();
        let (source, winning) = &replies[winner(&sealed)];
        let history = Holding::from(winning);
        let recovered = history.last;
        let synced: Vec<(usize, Result<()>)> = replies.iter()
            .map(|(server, sealed)| { let (ensemble, history) = (&ensemble, &history); async move { (*server, sync_server(ensemble, epoch, *server, sealed, *source, history).await) } })
            .collect::<FuturesUnordered<_>>().collect().await;
        if let Some(promised) = synced.iter().find_map(|(_, result)| match result { Err(Error::Deposed { promised, .. }) => Some(*promised), _ => None }) {
            return Err(Error::Deposed { epoch, promised });
        }
        let synced: Vec<usize> = synced.into_iter().filter_map(|(server, result)| result.ok().map(|_| server)).collect();
        if synced.len() < ensemble.config.write_quorum {
            return Err(Error::Remote { key: ensemble.config.stream.clone(), message: format!("recovered onto {} servers, a write quorum is {}", synced.len(), ensemble.config.write_quorum) });
        }
        let commits: Vec<Result<Ack>> = synced.iter().map(|&server| ensemble.commit(server, epoch, recovered)).collect::<FuturesUnordered<_>>().collect().await;
        for ack in commits.into_iter().flatten() { deposed_by(epoch, ack)?; }
        let long_ago = Instant::now().checked_sub(REPAIR_BACKOFF).unwrap_or_else(Instant::now);
        let replicas = (0..ensemble.len()).map(|server| if synced.contains(&server) {
            Replica { acked: recovered, link: Link::Live }
        } else {
            Replica { acked: -1, link: Link::Down(long_ago) }
        }).collect();
        let pipeline = Pipeline { next_lsn: recovered + 1, commit: recovered, history, buffer_first: recovered + 1, buffer: VecDeque::new(), buffered_bytes: 0, replicas };
        let writer = Arc::new(Self { ensemble, epoch, state: Mutex::new(State::Open(pipeline)), tip: watch::channel(Tip::Open(recovered)).0 });
        {
            let mut state = writer.state.lock();
            if let State::Open(pipeline) = &mut *state { writer.repair_down(pipeline); }
        }
        Ok(writer)
    }

    pub fn tip(&self) -> watch::Receiver<Tip> { self.tip.subscribe() }

    pub fn latest(&self) -> Result<LogOffset> {
        match &*self.tip.borrow() {
            Tip::Open(commit) => Ok(LogOffset(*commit)),
            Tip::Deposed { promised } => Err(Error::Deposed { epoch: self.epoch, promised: *promised }),
        }
    }

    pub fn enqueue(self: &Arc<Self>, payload: Bytes, timestamp: Timestamp) -> Result<AppendReceipt> {
        let mut state = self.state.lock();
        let pipeline = match &mut *state {
            State::Open(pipeline) => pipeline,
            State::Deposed { promised } => return Err(Error::Deposed { epoch: self.epoch, promised: *promised }),
        };
        let lsn = pipeline.next_lsn;
        pipeline.next_lsn = lsn.checked_add(1).ok_or_else(|| Error::Closed("log positions exhausted".into()))?;
        // The stream owns its records as a durable log owns its file: a payload carrying its producer's budget charge is copied, so the charge ends with the append.
        let entry = Entry { lsn, epoch: self.epoch, timestamp: timestamp.0, payload: Bytes::copy_from_slice(&payload) };
        pipeline.history.push(lsn, self.epoch);
        let (receipt, durable) = oneshot::channel();
        pipeline.buffered_bytes += entry.payload.len();
        pipeline.buffer.push_back(Buffered { entry: entry.clone(), receipt: Some(receipt) });
        for server in 0..pipeline.replicas.len() {
            if matches!(pipeline.replicas[server].link, Link::Live) {
                let body = RequestBody::Append(Append { stream: self.ensemble.stream(), epoch: self.epoch, first_lsn: lsn, entries: vec![entry.clone()], commit_lsn: pipeline.commit });
                self.send(server, body);
            }
        }
        self.repair_down(pipeline);
        Ok(AppendReceipt::new(durable))
    }

    fn send(self: &Arc<Self>, server: usize, body: RequestBody) {
        let writer = Arc::downgrade(self);
        let ensemble = Arc::clone(&self.ensemble);
        self.ensemble.links[server].send(body, Box::new(move |reply| {
            if let Some(writer) = Weak::upgrade(&writer) { writer.acknowledged(server, ensemble.ack(server, reply)); }
        }));
    }

    fn acknowledged(self: &Arc<Self>, server: usize, ack: Result<Ack>) {
        let mut state = self.state.lock();
        if let Ok(Ack::Refused(refused)) = ack && refused.promised > self.epoch { return self.depose(&mut state, refused.promised); }
        let State::Open(pipeline) = &mut *state else { return };
        match ack {
            Ok(Ack::Accepted(accepted)) => {
                let replica = &mut pipeline.replicas[server];
                replica.acked = replica.acked.max(accepted.last_lsn);
                self.advance(pipeline);
            }
            Ok(Ack::Refused(_)) | Err(_) => {
                if matches!(pipeline.replicas[server].link, Link::Live) {
                    pipeline.replicas[server].link = Link::Repairing;
                    self.spawn_repair(server);
                }
            }
        }
    }

    fn advance(self: &Arc<Self>, pipeline: &mut Pipeline) {
        let mut acked: Vec<i64> = pipeline.replicas.iter().map(|replica| replica.acked).collect();
        acked.sort_unstable_by(|left, right| right.cmp(left));
        let quorum = acked[self.ensemble.config.write_quorum - 1].min(pipeline.next_lsn - 1);
        if quorum <= pipeline.commit { return; }
        let previous = pipeline.commit;
        pipeline.commit = quorum;
        self.tip.send_replace(Tip::Open(quorum));
        let first = (previous + 1 - pipeline.buffer_first).max(0) as usize;
        let last = (quorum - pipeline.buffer_first) as usize;
        for buffered in pipeline.buffer.range_mut(first..=last) {
            if let Some(receipt) = buffered.receipt.take() {
                let _ = receipt.send(Ok(record(&buffered.entry).metadata));
            }
        }
        if pipeline.commit == pipeline.next_lsn - 1 {
            for server in 0..pipeline.replicas.len() {
                if matches!(pipeline.replicas[server].link, Link::Live) {
                    self.send(server, RequestBody::Commit(Commit { stream: self.ensemble.stream(), epoch: self.epoch, commit_lsn: quorum }));
                }
            }
        }
        while pipeline.buffer.front().is_some_and(|buffered| buffered.entry.lsn <= pipeline.commit)
            && (pipeline.buffer.len() > RECENT_ENTRIES || pipeline.buffered_bytes > RECENT_BYTES) {
            let dropped = pipeline.buffer.pop_front().expect("checked above");
            pipeline.buffered_bytes -= dropped.entry.payload.len();
            pipeline.buffer_first += 1;
        }
    }

    fn depose(&self, state: &mut State, promised: u64) {
        let deposed = std::mem::replace(state, State::Deposed { promised });
        if let State::Open(pipeline) = deposed {
            for buffered in pipeline.buffer {
                if let Some(receipt) = buffered.receipt { let _ = receipt.send(Err(Error::Deposed { epoch: self.epoch, promised })); }
            }
        }
        self.tip.send_replace(Tip::Deposed { promised });
    }

    fn repair_down(self: &Arc<Self>, pipeline: &mut Pipeline) {
        for server in 0..pipeline.replicas.len() {
            if let Link::Down(since) = pipeline.replicas[server].link && since.elapsed() >= REPAIR_BACKOFF {
                pipeline.replicas[server].link = Link::Repairing;
                self.spawn_repair(server);
            }
        }
    }

    fn spawn_repair(self: &Arc<Self>, server: usize) {
        let writer = Arc::clone(self);
        tokio::spawn(async move {
            let result = writer.repair(server).await;
            let mut state = writer.state.lock();
            match result {
                Err(Error::Deposed { promised, .. }) => writer.depose(&mut state, promised),
                Err(_) => if let State::Open(pipeline) = &mut *state && matches!(pipeline.replicas[server].link, Link::Repairing) {
                    pipeline.replicas[server].link = Link::Down(Instant::now());
                },
                Ok(()) => (),
            }
        });
    }

    /// Matches the server against this writer's history, copies what it lacks from a live peer, and rejoins it to the pipeline with an overwrite carrying the buffered tail, sent under the lock so later appends follow it.
    async fn repair(self: &Arc<Self>, server: usize) -> Result<()> {
        let epoch = self.epoch;
        let ensemble = &self.ensemble;
        let sealed = ensemble.seal(server, epoch).await?;
        if sealed.promised > epoch { return Err(Error::Deposed { epoch, promised: sealed.promised }); }
        let history = match &*self.state.lock() { State::Open(pipeline) => pipeline.history.clone(), State::Deposed { promised } => return Err(Error::Deposed { epoch, promised: *promised }) };
        let plan = plan_repair(&Holding::from(&sealed), &history).map_err(|message| Error::Remote { key: ensemble.config.stream.clone(), message })?;
        if let Some(through) = plan.trim_through { deposed_by(epoch, ensemble.trim(server, through).await?)?; }
        let mut from = plan.from;
        let mut empty_fetches = 0;
        loop {
            let (sources, buffer_first) = {
                let mut state = self.state.lock();
                let pipeline = match &mut *state { State::Open(pipeline) => pipeline, State::Deposed { promised } => return Err(Error::Deposed { epoch, promised: *promised }) };
                if from >= pipeline.buffer_first {
                    let entries = pipeline.buffer.iter().skip((from - pipeline.buffer_first) as usize).map(|buffered| buffered.entry.clone()).collect();
                    let body = ensemble.overwrite_request(epoch, from, entries, true, pipeline.commit);
                    pipeline.replicas[server].link = Link::Live;
                    self.send(server, body);
                    return Ok(());
                }
                let sources: Vec<usize> = (0..pipeline.replicas.len()).filter(|&peer| peer != server && matches!(pipeline.replicas[peer].link, Link::Live)).collect();
                (sources, pipeline.buffer_first)
            };
            let mut chunk = None;
            for source in sources {
                let Ok(fetched) = ensemble.fetch(source, from, buffer_first, MAXIMUM_BATCH_ENTRIES).await else { continue };
                if from <= fetched.trim_lsn {
                    deposed_by(epoch, ensemble.trim(server, fetched.trim_lsn).await?)?;
                    from = fetched.trim_lsn + 1;
                    chunk = Some(Vec::new());
                    break;
                }
                if fetched.entries.first().is_some_and(|entry| entry.lsn == from) { chunk = Some(fetched.entries); break; }
            }
            match chunk {
                Some(entries) if entries.is_empty() => continue,
                Some(entries) => from = deposed_by(epoch, ensemble.overwrite(server, epoch, from, entries, false).await?)? + 1,
                None if empty_fetches < 50 => { empty_fetches += 1; tokio::time::sleep(Duration::from_millis(20)).await; }
                None => return Err(Error::Remote { key: ensemble.config.stream.clone(), message: format!("no live server holds lsn {from} to repair from") }),
            }
        }
    }

    /// Committed entries in `[from, to)` from the buffer, or `None` when it no longer holds `from`.
    pub fn buffered(&self, from: i64, to: i64, max_entries: usize, max_bytes: usize) -> Option<Vec<LogRecord>> {
        let state = self.state.lock();
        let State::Open(pipeline) = &*state else { return Some(Vec::new()) };
        if from < pipeline.buffer_first { return None; }
        let to = to.min(pipeline.commit + 1);
        let mut records = Vec::new();
        let mut bytes = 0usize;
        for buffered in pipeline.buffer.iter().skip((from - pipeline.buffer_first) as usize).take_while(|buffered| buffered.entry.lsn < to).take(max_entries) {
            bytes = bytes.saturating_add(buffered.entry.payload.len());
            if bytes > max_bytes && !records.is_empty() { break; }
            records.push(record(&buffered.entry));
        }
        Some(records)
    }

    /// Servers synced to this writer, to read its committed history from.
    pub fn live(&self) -> Vec<usize> {
        match &*self.state.lock() {
            State::Open(pipeline) => (0..pipeline.replicas.len()).filter(|&server| matches!(pipeline.replicas[server].link, Link::Live)).collect(),
            State::Deposed { .. } => Vec::new(),
        }
    }

    pub fn epoch(&self) -> u64 { self.epoch }
}
