//! The writer of one stream at one epoch: recovery on open, pipelined appends, quorum commit, lazy repair of lagging servers.

use super::connection::CALL_TIMEOUT;
use super::ensemble::{Ack, Ensemble, MAXIMUM_BATCH_ENTRIES};
use super::CopyFrom;
use super::guarded_recovery::{
    JournalEvidence, TailDecision, covers_recovered_commit, decide_from_sealed_probes, guarded_probe_evidence,
    predecessor_commit_target, recover_guarded_tail, recovery_commit_quorum, sealed_journal_evidence, valid_guarded_source,
};
use super::recovery::{Holding, plan_repair, winner};
use crate::protocol::{Accepted, Commit, Entry, RequestBody, Sealed};
use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use rtdb_storage::log::{AppendReceipt, GuardedAppend, LogMetadata, LogOffset, LogRecord};
use rtdb_storage::{Error, Result};
use rtdb_types::Timestamp;
use std::collections::VecDeque;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, watch};

const REPAIR_BACKOFF: Duration = Duration::from_millis(200);
const SEAL_GRACE: Duration = Duration::from_millis(100);
const RECOVERY_GRACE: Duration = Duration::from_millis(2);
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
    /// The highest decision this journal confirmed durable.
    installed: i64,
    /// The latest ordinary publication queued on this connection.
    installing: i64,
    link: Link,
}

struct Buffered {
    entry: Entry,
    receipt: Option<oneshot::Sender<Result<LogMetadata>>>,
}

pub(super) enum AppendTime<'a> {
    Ordinary(Timestamp),
    Guarded(GuardedAppend<'a>),
}

/// One leadership confirmation, by identity: a reply counts towards it only if its request was sent while it was the round in flight.
struct Round {
    outcome: watch::Sender<Option<Confirmation>>,
}

#[derive(Clone, Debug)]
enum Confirmation {
    Confirmed,
    Deposed { promised: u64 },
    Unavailable(String),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mark {
    Pending,
    Confirmed,
    Failed,
}

/// The round in flight, what each server has told it, and the round its late callers wait on, which starts when this one ends.
struct Confirming {
    round: Arc<Round>,
    marks: Vec<Mark>,
    next: Option<Arc<Round>>,
}

/// A guarded entry is the only pending entry. It first needs a quorum of
/// original observed appends, then a quorum storing that commit decision.
struct GuardedFlight {
    lsn: i64,
    originals: Vec<Mark>,
    witnesses: Option<Vec<Mark>>,
}

#[derive(Clone, Copy)]
enum GuardedReply { Original(i64), Commit(i64) }

impl GuardedFlight {
    fn record_reply(&mut self, server: usize, reply: Option<GuardedReply>, ack: &Result<Ack>) {
        let (marks, committed) = match reply {
            Some(GuardedReply::Original(lsn)) if lsn == self.lsn => (&mut self.originals, false),
            Some(GuardedReply::Commit(lsn)) if lsn == self.lsn => {
                let Some(witnesses) = &mut self.witnesses else { return };
                (witnesses, true)
            }
            _ => return,
        };
        if marks[server] == Mark::Pending {
            let exact = matches!(ack, Ok(Ack::Accepted(reply)) if reply.last_lsn == self.lsn
                && (!committed || reply.commit_lsn == self.lsn));
            marks[server] = if exact { Mark::Confirmed } else { Mark::Failed };
        }
    }

    fn has_quorum(marks: &[Mark], quorum: usize) -> bool {
        marks.iter().filter(|&&mark| mark == Mark::Confirmed).count() >= quorum
    }

    fn begin_commit(&mut self, quorum: usize) -> bool {
        if self.witnesses.is_some() || !Self::has_quorum(&self.originals, quorum) { return false; }
        self.witnesses = Some(self.originals.iter().map(|&mark| {
            if mark == Mark::Failed { Mark::Failed } else { Mark::Pending }
        }).collect());
        true
    }

    fn releasable(&self, quorum: usize) -> bool {
        self.witnesses.as_ref().is_some_and(|witnesses| Self::has_quorum(witnesses, quorum))
    }

    fn can_reach_quorum(&self, quorum: usize) -> bool {
        self.witnesses.as_ref().unwrap_or(&self.originals).iter().filter(|&&mark| mark != Mark::Failed).count() >= quorum
    }
}

impl Round {
    fn new() -> Arc<Self> { Arc::new(Self { outcome: watch::channel(None).0 }) }
}

struct Pipeline {
    next_lsn: i64,
    commit: i64,
    publication: Option<i64>,
    history: Holding,
    /// Entry `i` has lsn `buffer_first + i`; it holds every uncommitted entry and some recent committed ones.
    buffer_first: i64,
    buffer: VecDeque<Buffered>,
    buffered_bytes: usize,
    replicas: Vec<Replica>,
    confirming: Option<Confirming>,
    guarded: Option<GuardedFlight>,
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
    LogRecord::new(LogMetadata { offset: LogOffset(entry.lsn), timestamp: Timestamp(entry.timestamp) }, entry.payload.clone())
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

fn deposed_by(epoch: u64, ack: Ack) -> Result<Accepted> {
    match ack {
        Ack::Accepted(accepted) => Ok(accepted),
        Ack::Refused(refused) if refused.promised > epoch => Err(Error::Deposed { epoch, promised: refused.promised }),
        Ack::Refused(refused) => Err(Error::Remote { key: String::new(), message: format!("refused at promised epoch {}, last lsn {}", refused.promised, refused.last_lsn) }),
    }
}

/// Copies `source`'s entries `[from, to]` onto `target` from `from`, marking it synced with its last chunk.
async fn copy(source: (&Ensemble, usize), target: (&Ensemble, usize), epoch: u64, mut from: i64, to: i64) -> Result<Accepted> {
    let ((source, source_server), (target, server)) = (source, target);
    loop {
        let entries = if from > to { Vec::new() } else {
            let fetched = source.fetch(source_server, from, to + 1, MAXIMUM_BATCH_ENTRIES).await?;
            super::ensemble::check_retained(from, &fetched)?;
            if fetched.entries.first().is_none_or(|entry| entry.lsn != from) {
                return Err(Error::Remote { key: target.config.stream.clone(), message: format!("the recovery source no longer holds lsn {from}") });
            }
            fetched.entries
        };
        let last = entries.last().map_or(from - 1, |entry| entry.lsn);
        let complete = last >= to;
        let acked = deposed_by(epoch, target.overwrite(server, epoch, from, entries, complete).await?)?;
        if complete { return Ok(acked); }
        from = acked.last_lsn + 1;
    }
}

/// Where the recovered history comes from and where the writer's ensemble starts holding it.
struct Recovered<'a> {
    source: (&'a Ensemble, usize),
    history: Holding,
    /// A server holding no entries starts here rather than at the history's trim point; lower lsns stay with an earlier loglet.
    start: i64,
}

/// Brings one sealed server to exactly the recovered history from `recovered.start` or below, and syncs it at this epoch.
async fn sync_server(ensemble: &Ensemble, epoch: u64, server: usize, sealed: &Sealed, recovered: &Recovered<'_>) -> Result<Accepted> {
    let plan = plan_repair(&Holding::from(sealed), &recovered.history).map_err(|message| Error::Remote { key: ensemble.config.stream.clone(), message })?;
    if let Some(through) = plan.trim_through { deposed_by(epoch, ensemble.trim(server, through).await?)?; }
    let holds_nothing = sealed.last_lsn <= sealed.trim_lsn.max(plan.trim_through.unwrap_or(sealed.trim_lsn));
    let from = if holds_nothing && plan.from < recovered.start {
        deposed_by(epoch, ensemble.trim(server, recovered.start - 1).await?)?;
        recovered.start
    } else {
        plan.from
    };
    copy(recovered.source, (ensemble, server), epoch, from, recovered.history.last).await
}

/// Syncs and commits the recovered history on a write quorum, including other replicas that have already caught up.
async fn sync_quorum(ensemble: &Ensemble, epoch: u64, replies: &[(usize, Sealed)], recovered: &Recovered<'_>, commit: i64) -> Result<Vec<usize>> {
    let quorum = ensemble.config.write_quorum;
    let mut syncing = replies.iter()
        .map(|(server, sealed)| async move { (*server, sync_server(ensemble, epoch, *server, sealed, recovered).await) })
        .collect::<FuturesUnordered<_>>();
    let mut committing = FuturesUnordered::new();
    let mut synced = 0;
    let mut uncommitted = Vec::new();
    let mut committed = Vec::with_capacity(replies.len());
    let mut grace = None;
    loop {
        if recovery_commit_quorum(committed.len(), quorum) {
            grace.get_or_insert_with(|| tokio::time::Instant::now() + RECOVERY_GRACE);
        } else if synced >= quorum {
            for server in uncommitted.drain(..) {
                committing.push(async move { (server, ensemble.commit(server, epoch, commit).await) });
            }
        }

        if syncing.is_empty() && committing.is_empty() { break; }
        tokio::select! {
            biased;
            Some((server, reply)) = syncing.next(), if !syncing.is_empty() => match reply {
                Ok(ack) => {
                    synced += 1;
                    if covers_recovered_commit(ack.commit_lsn, commit) { committed.push(server); }
                    else { uncommitted.push(server); }
                }
                Err(Error::Deposed { promised, .. }) => return Err(Error::Deposed { epoch, promised }),
                Err(_) => (),
            },
            Some((server, reply)) = committing.next(), if !committing.is_empty() => {
                if let Ok(reply) = reply {
                    let ack = deposed_by(epoch, reply)?;
                    if covers_recovered_commit(ack.commit_lsn, commit) { committed.push(server); }
                }
            }
            _ = async {
                match grace {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => break,
        }
    }
    if !recovery_commit_quorum(committed.len(), quorum) {
        return Err(Error::Remote { key: ensemble.config.stream.clone(), message: format!(
            "recovered on {synced} servers and confirmed commit on {}, a write quorum is {quorum}", committed.len()) });
    }
    Ok(committed)
}

fn winning_seal(replies: &[(usize, Sealed)]) -> &(usize, Sealed) {
    let sealed: Vec<Sealed> = replies.iter().map(|(_, sealed)| sealed.clone()).collect();
    &replies[winner(&sealed)]
}

async fn settle_guarded_tail(ensemble: &Ensemble, epoch: u64, source: usize, history: &Holding, replies: &[(usize, Sealed)]) -> Result<TailDecision> {
    let offset = history.last;
    let unknown = || Error::Remote { key: ensemble.stream(), message: format!("guarded tail at position {offset} has an unknown outcome") };
    let entry_epoch = history.epoch_at(offset).filter(|entry_epoch| *entry_epoch < epoch).ok_or_else(unknown)?;
    let quorum = ensemble.config.write_quorum;
    let mut evidence = vec![JournalEvidence::Unavailable; ensemble.len()];
    for (server, sealed) in replies {
        // The sole writer assigns an entry at most once at this epoch and position.
        let trimmed = sealed.trim_lsn >= offset;
        let state = sealed_journal_evidence(trimmed,
            !trimmed && Holding::from(sealed).epoch_at(offset) == Some(entry_epoch),
            covers_recovered_commit(sealed.commit_lsn, offset));
        if matches!(state, JournalEvidence::CommitWitness) {
            return Ok(recover_guarded_tail(true, 0, 0, quorum));
        }
        evidence[*server] = state;
    }
    let Ok(exact) = ensemble.guarded_digest(source, offset, epoch).await else { return Ok(TailDecision::Unknown); };
    if !valid_guarded_source(offset, exact.through_lsn, exact.digest.len(), exact.guarded_before,
        exact.guarded_before.is_some() && ensemble.matches_timed_origin(&exact.timed_origin_identity)) {
        return Ok(TailDecision::Unknown);
    }
    evidence[source] = guarded_probe_evidence(true, exact.original_append, exact.synced_clock_upper_micros, exact.guarded_before);
    let mut pending: FuturesUnordered<_> = replies.iter().map(|(server, _)| *server)
        .filter(|&server| server != source && matches!(evidence[server], JournalEvidence::Holding))
        .map(|server| async move { (server, ensemble.guarded_digest(server, offset, epoch).await) })
        .collect();
    loop {
        let decision = decide_from_sealed_probes(&evidence, quorum);
        if !matches!(decision, TailDecision::Unknown) { return Ok(decision); }
        let Some((server, reply)) = pending.next().await else { return Ok(decision); };
        if let Ok(proof) = reply {
            let matches = proof.through_lsn == offset && proof.digest == exact.digest
                && proof.guarded_before == exact.guarded_before && proof.timed_origin_identity == exact.timed_origin_identity;
            evidence[server] = guarded_probe_evidence(matches, proof.original_append, proof.synced_clock_upper_micros, exact.guarded_before);
        }
    }
}

/// An unsettled guarded flight is exclusive at the tip. Unlike an ordinary
/// suffix, recovery needs its original quorum or definite exclusion before
/// transferring it. Availability can require journals beyond the seal quorum.
struct SealedHistory {
    source: usize,
    history: Holding,
    guarded_commit: Option<i64>,
}

async fn sealed_history(ensemble: &Ensemble, epoch: u64, replies: &mut Vec<(usize, Sealed)>) -> Result<SealedHistory> {
    let quorum = ensemble.config.write_quorum;
    let mut checked_remaining = false;
    loop {
        let (source, winning) = winning_seal(replies);
        let source = *source;
        let mut sealed = SealedHistory { source, history: Holding::from(winning), guarded_commit: None };
        if !winning.guarded_tail { return Ok(sealed); }
        let witness = covers_recovered_commit(winning.commit_lsn, sealed.history.last);
        let resolution = if witness {
            recover_guarded_tail(witness, 0, 0, quorum)
        } else {
            if !ensemble.supports_guarded() { return Err(Error::GuardedAppendUnsupported); }
            settle_guarded_tail(ensemble, epoch, source, &sealed.history, replies).await?
        };
        match resolution {
            TailDecision::Keep => {
                sealed.guarded_commit = Some(sealed.history.last);
                return Ok(sealed);
            }
            TailDecision::Discard => {
                sealed.history.discard_last();
                return Ok(sealed);
            }
            TailDecision::Unknown if !checked_remaining && replies.len() < ensemble.len() => {
                let mut pending: FuturesUnordered<_> = (0..ensemble.len())
                    .filter(|server| !replies.iter().any(|(replied, _)| replied == server))
                    .map(|server| async move { (server, ensemble.seal(server, epoch).await) }).collect();
                while let Some((server, sealed)) = pending.next().await {
                    if let Ok(sealed) = sealed {
                        if sealed.promised > epoch { return Err(Error::Deposed { epoch, promised: sealed.promised }); }
                        replies.push((server, sealed));
                    }
                }
                checked_remaining = true;
            }
            TailDecision::Unknown => return Err(Error::Remote {
                key: ensemble.stream(), message: format!("guarded tail at position {} has an unknown outcome", sealed.history.last),
            }),
        }
    }
}

async fn bootstrap_replica_page(
    ensemble: &Ensemble, server: usize, sealed: &Sealed, epoch: u64,
    checkpoint: i64, first: i64, entries: &[Entry], terminal: bool,
) -> Result<()> {
    let through = first + entries.len() as i64 - 1;
    let conflict = || Error::ConflictingObject(ensemble.config.stream.clone());
    let unavailable = || Error::Remote { key: ensemble.config.stream.clone(), message: "the replica is missing an earlier genesis page".into() };
    if sealed.promised > epoch { return Err(Error::Deposed { epoch, promised: sealed.promised }); }
    if sealed.promised != epoch || (terminal && sealed.last_lsn > through) {
        return Err(conflict());
    }
    if sealed.trim_lsn != checkpoint {
        if sealed.trim_lsn != -1 || sealed.last_lsn != -1 || sealed.commit_lsn != -1 {
            return Err(conflict());
        }
        if first != checkpoint + 1 { return Err(unavailable()); }
        deposed_by(epoch, ensemble.trim(server, checkpoint).await?)?;
    } else if sealed.commit_lsn < first - 1 || sealed.last_lsn < first - 1 {
        return Err(unavailable());
    }

    let overlap = sealed.last_lsn.min(through);
    let mut next = first;
    while next <= overlap {
        let prefix = ensemble.fetch(server, next, overlap + 1, MAXIMUM_BATCH_ENTRIES).await?;
        let begin = (next - first) as usize;
        let end = begin.saturating_add(prefix.entries.len());
        if prefix.trim_lsn != checkpoint || prefix.entries.is_empty()
            || prefix.entries.last().is_some_and(|entry| entry.lsn > overlap)
            || entries.get(begin..end) != Some(prefix.entries.as_slice())
        {
            return Err(conflict());
        }
        next += prefix.entries.len() as i64;
    }

    let missing = &entries[(next - first) as usize..];
    if let Some(entry) = missing.first() {
        // Each append also establishes the peer's sync at this epoch. An
        // overlapping writer can only append next or fail and re-observe.
        deposed_by(epoch, ensemble.overwrite(server, epoch, entry.lsn, missing.to_vec(), true).await?)?;
    } else if sealed.synced != epoch {
        if sealed.last_lsn > through { return Err(conflict()); }
        deposed_by(epoch, ensemble.overwrite(server, epoch, through + 1, Vec::new(), true).await?)?;
    }
    let committed = deposed_by(epoch, ensemble.commit(server, epoch, through).await?)?;
    if terminal {
        let final_state = ensemble.seal(server, epoch).await?;
        if final_state.promised > epoch { return Err(Error::Deposed { epoch, promised: final_state.promised }); }
        if final_state.promised != epoch || final_state.synced != epoch || final_state.trim_lsn != checkpoint
            || final_state.last_lsn != through || final_state.commit_lsn != through
        {
            return Err(conflict());
        }
    } else if committed.last_lsn < through || committed.commit_lsn < through {
        return Err(unavailable());
    }
    Ok(())
}

impl Writer {
    pub async fn open(ensemble: Arc<Ensemble>, epoch: u64) -> Result<Arc<Self>> {
        let mut replies = seal_quorum(&ensemble, epoch).await?;
        let SealedHistory { source, history, .. } = sealed_history(&ensemble, epoch, &mut replies).await?;
        let recovered = Recovered { source: (&ensemble, source), start: history.trim + 1, history };
        Self::establish(Arc::clone(&ensemble), epoch, &replies, recovered).await
    }

    pub(super) async fn seal_recover_committed(ensemble: &Ensemble, epoch: u64) -> Result<i64> {
        let mut replies = seal_quorum(ensemble, epoch).await?;
        let SealedHistory { source, history, .. } = sealed_history(ensemble, epoch, &mut replies).await?;
        let last = history.last;
        let recovered = Recovered { source: (ensemble, source), start: history.trim + 1, history };
        sync_quorum(ensemble, epoch, &replies, &recovered, last).await?;
        Ok(last)
    }

    /// Recovers the same history used by a local ensemble move and by a
    /// copy to a different physical stream.
    async fn recover_moved(previous: &Ensemble, previous_start: i64, epoch: u64, from: CopyFrom) -> Result<Recovered<'_>> {
        let mut previous_replies = seal_quorum(previous, epoch).await?;
        let SealedHistory { source, history, guarded_commit } =
            sealed_history(previous, epoch, &mut previous_replies).await?;
        let start = match from { CopyFrom::Retained => previous_start, CopyFrom::Lsn(lsn) => lsn }.clamp(history.trim + 1, history.last + 1);
        let in_place = Recovered { source: (previous, source), start: history.trim + 1, history: history.clone() };
        // A move can stop before the destination is durable or recorded. The
        // predecessor quorum must already retain the guarded outcome.
        let commit = predecessor_commit_target(guarded_commit, start - 1);
        sync_quorum(previous, epoch, &previous_replies, &in_place, commit).await?;
        Ok(Recovered { source: (previous, source), history, start })
    }

    /// Copies the recovered suffix into another ensemble and opens its writer.
    pub async fn open_moved(previous: &Ensemble, previous_start: i64, ensemble: Arc<Ensemble>, epoch: u64, from: CopyFrom) -> Result<(Arc<Self>, i64)> {
        let recovered = Self::recover_moved(previous, previous_start, epoch, from).await?;
        let start = recovered.start;
        let replies = seal_quorum(&ensemble, epoch).await?;
        Ok((Self::establish(ensemble, epoch, &replies, recovered).await?, start))
    }

    /// Commits the retained history in its new journal without opening a writer.
    pub(super) async fn copy_retained(previous: &Ensemble, destination: &Ensemble, epoch: u64) -> Result<()> {
        let recovered = Self::recover_moved(previous, 0, epoch, CopyFrom::Retained).await?;
        let replies = seal_quorum(destination, epoch).await?;
        sync_quorum(destination, epoch, &replies, &recovered, recovered.history.last).await?;
        Ok(())
    }

    pub(super) async fn bootstrap_committed_page(
        ensemble: &Ensemble, epoch: u64, checkpoint: i64, first: i64, entries: &[Entry], terminal: bool,
    ) -> Result<()> {
        let replies = seal_quorum(ensemble, epoch).await?;
        let mut seeds = replies.into_iter().map(|(server, mut sealed)| async move {
            // Lost replies or an identical concurrent bootstrap can race an
            // overwrite. Re-read the peer before retrying any append.
            for _ in 0..8 {
                match bootstrap_replica_page(ensemble, server, &sealed, epoch, checkpoint, first, entries, terminal).await {
                    Ok(()) => return Ok(()),
                    Err(error @ (Error::Deposed { .. } | Error::ConflictingObject(_))) => return Err(error),
                    Err(_) => sealed = ensemble.seal(server, epoch).await?,
                }
            }
            Err(Error::Remote { key: ensemble.config.stream.clone(), message: "replica genesis page did not settle".into() })
        }).collect::<FuturesUnordered<_>>();
        let mut committed = 0;
        while let Some(reply) = seeds.next().await {
            match reply {
                Ok(()) => committed += 1,
                Err(error @ (Error::Deposed { .. } | Error::ConflictingObject(_))) => return Err(error),
                Err(_) => (),
            }
        }
        if committed < ensemble.config.write_quorum {
            return Err(Error::Remote { key: ensemble.config.stream.clone(),
                message: "a quorum did not commit the reserved replica genesis page".into() });
        }
        Ok(())
    }

    /// Recovers and activates only journals that confirmed the selected decision.
    async fn establish(ensemble: Arc<Ensemble>, epoch: u64, replies: &[(usize, Sealed)], recovered: Recovered<'_>) -> Result<Arc<Self>> {
        let last = recovered.history.last;
        let synced = sync_quorum(&ensemble, epoch, replies, &recovered, last).await?;
        // Predecessor recovery also syncs replicas at this epoch. Only the writer's own ensemble activates.
        let ensemble_ref = &ensemble;
        let mut activating: FuturesUnordered<_> = synced.iter().map(|&server| async move { (server, ensemble_ref.activate(server, epoch).await) }).collect();
        let mut active = Vec::new();
        while let Some((server, reply)) = activating.next().await {
            match reply {
                Ok(Ack::Accepted(_)) => active.push(server),
                Ok(Ack::Refused(refused)) if refused.promised > epoch => return Err(Error::Deposed { epoch, promised: refused.promised }),
                _ => (),
            }
            if active.len() >= ensemble.config.write_quorum { break; }
        }
        drop(activating);
        if active.len() < ensemble.config.write_quorum {
            return Err(Error::Remote { key: ensemble.config.stream.clone(), message: format!(
                "activated {} journals, a write quorum is {}", active.len(), ensemble.config.write_quorum) });
        }
        let long_ago = Instant::now().checked_sub(REPAIR_BACKOFF).unwrap_or_else(Instant::now);
        let replicas = (0..ensemble.len()).map(|server| if active.contains(&server) {
            Replica { acked: last, installed: last, installing: last, link: Link::Live }
        } else {
            Replica { acked: -1, installed: -1, installing: -1, link: Link::Down(long_ago) }
        }).collect();
        let pipeline = Pipeline { next_lsn: last + 1, commit: last, publication: None, history: recovered.history, buffer_first: last + 1, buffer: VecDeque::new(), buffered_bytes: 0, replicas, confirming: None, guarded: None };
        let writer = Arc::new(Self { ensemble, epoch, state: Mutex::new(State::Open(pipeline)), tip: watch::channel(Tip::Open(last)).0 });
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

    pub fn enqueue(self: &Arc<Self>, payload: Bytes, time: AppendTime<'_>) -> Result<AppendReceipt> {
        let mut state = self.state.lock();
        let pipeline = match &mut *state {
            State::Open(pipeline) => pipeline,
            State::Deposed { promised } => return Err(Error::Deposed { epoch: self.epoch, promised: *promised }),
        };
        let lsn = pipeline.next_lsn;
        let next = lsn.checked_add(1).ok_or_else(|| Error::Closed("log positions exhausted".into()))?;
        if pipeline.guarded.is_some() { return Err(self.guarded_outcome_unknown()); }
        let guarded = if matches!(time, AppendTime::Guarded(_)) {
            if !self.ensemble.supports_guarded() { return Err(Error::GuardedAppendUnsupported); }
            let ready = pipeline.replicas.iter().filter(|replica| {
                matches!(replica.link, Link::Live) && replica.acked >= pipeline.commit
            }).count();
            if pipeline.commit != lsn - 1 || ready < self.ensemble.config.write_quorum {
                self.repair_down(pipeline);
                return Err(Error::GuardedAppendUnavailable);
            }
            Some(GuardedFlight { lsn, originals: pipeline.replicas.iter().map(|replica| {
                if matches!(replica.link, Link::Live) { Mark::Pending } else { Mark::Failed }
            }).collect(), witnesses: None })
        } else { None };
        // The stream owns its records as a durable log owns its file: a payload carrying its producer's budget charge is copied, so the charge ends with the append.
        let payload = Bytes::copy_from_slice(&payload);
        let (timestamp, guarded_before, timed_origin) = match time {
            AppendTime::Ordinary(timestamp) => (timestamp, None, None),
            AppendTime::Guarded(guard) => {
                let cutoff = guard.journal_before();
                let before = cutoff.map(|cutoff| cutoff.before().0);
                let origin = cutoff.map(|cutoff| self.ensemble.timed_origin(cutoff.certificate_sha256()));
                (guard.sample()?, before, origin)
            }
        };
        pipeline.guarded = guarded;
        pipeline.next_lsn = next;
        let entry = Entry { lsn, epoch: self.epoch, timestamp: timestamp.0, guarded: pipeline.guarded.is_some(), guarded_before, timed_origin, payload };
        pipeline.history.push(lsn, self.epoch);
        let (receipt, durable) = oneshot::channel();
        pipeline.buffered_bytes += entry.payload.len();
        pipeline.buffer.push_back(Buffered { entry: entry.clone(), receipt: Some(receipt) });
        for server in 0..pipeline.replicas.len() {
            let eligible = pipeline.guarded.as_ref().map_or_else(
                || matches!(pipeline.replicas[server].link, Link::Live),
                |flight| flight.originals[server] == Mark::Pending,
            );
            if eligible {
                let body = self.ensemble.append_request(self.epoch, entry.clone(), pipeline.commit);
                self.send(pipeline, server, body);
            }
        }
        let receipt = AppendReceipt::new(durable);
        if pipeline.guarded.is_some() {
            // A lost reply bounds only the wait. The dispatched entry still
            // owns the tail and must be settled by its evidence.
            let due = tokio::time::Instant::now() + CALL_TIMEOUT;
            let unknown = self.guarded_outcome_unknown();
            Ok(AppendReceipt::observed(async move {
                tokio::time::timeout_at(due, receipt.durable()).await.unwrap_or(Err(unknown))
            }))
        } else {
            self.repair_down(pipeline);
            Ok(receipt)
        }
    }

    fn guarded_outcome_unknown(&self) -> Error {
        Error::Remote { key: self.ensemble.stream(), message: "guarded append outcome is unknown after dispatch".into() }
    }

    /// Sends a pipeline request; its reply also answers the confirmation round in flight when it was sent.
    fn send(self: &Arc<Self>, pipeline: &Pipeline, server: usize, body: RequestBody) {
        let writer = Arc::downgrade(self);
        let ensemble = Arc::clone(&self.ensemble);
        let round = pipeline.confirming.as_ref().map(|confirming| Arc::clone(&confirming.round));
        let guarded_reply = match &body {
            RequestBody::AppendObserved(append) => Some(GuardedReply::Original(append.first_lsn)),
            RequestBody::Commit(commit) if pipeline.guarded.as_ref().is_some_and(|flight| flight.lsn == commit.commit_lsn) => {
                Some(GuardedReply::Commit(commit.commit_lsn))
            }
            _ => None,
        };
        self.ensemble.links[server].send(body, Box::new(move |reply| {
            if let Some(writer) = Weak::upgrade(&writer) { writer.acknowledged(server, ensemble.ack(server, reply), round, guarded_reply); }
        }));
    }

    fn send_check(self: &Arc<Self>, server: usize, round: &Arc<Round>) {
        let writer = Arc::downgrade(self);
        let ensemble = Arc::clone(&self.ensemble);
        let round = Arc::clone(round);
        self.ensemble.links[server].send(self.ensemble.check_synced_request(self.epoch), Box::new(move |reply| {
            let Some(writer) = Weak::upgrade(&writer) else { return };
            let mut state = writer.state.lock();
            match ensemble.ack(server, reply) {
                Ok(Ack::Refused(refused)) if refused.promised > writer.epoch => writer.depose(&mut state, refused.promised),
                ack => if let State::Open(pipeline) = &mut *state { writer.mark(pipeline, &round, server, matches!(ack, Ok(Ack::Accepted(_)))); },
            }
        }));
    }

    fn acknowledged(self: &Arc<Self>, server: usize, ack: Result<Ack>, round: Option<Arc<Round>>, guarded_reply: Option<GuardedReply>) {
        let mut state = self.state.lock();
        if let Ok(Ack::Refused(refused)) = ack && refused.promised > self.epoch { return self.depose(&mut state, refused.promised); }
        let State::Open(pipeline) = &mut *state else { return };
        if let Some(round) = &round { self.mark(pipeline, round, server, matches!(ack, Ok(Ack::Accepted(_)))); }
        if let Some(flight) = &mut pipeline.guarded {
            flight.record_reply(server, guarded_reply, &ack);
            if !flight.can_reach_quorum(self.ensemble.config.write_quorum)
                && let Some(buffered) = pipeline.buffer.back_mut()
                && let Some(receipt) = buffered.receipt.take()
            {
                let _ = receipt.send(Err(self.guarded_outcome_unknown()));
            }
        }
        match ack {
            Ok(Ack::Accepted(accepted)) => {
                let replica = &mut pipeline.replicas[server];
                replica.acked = replica.acked.max(accepted.last_lsn);
                replica.installed = replica.installed.max(accepted.commit_lsn);
                self.advance(pipeline);
            }
            Ok(Ack::Refused(_)) | Err(_) => {
                let replica = &mut pipeline.replicas[server];
                replica.installing = replica.installed;
                if matches!(replica.link, Link::Live) {
                    pipeline.replicas[server].link = Link::Repairing;
                    self.spawn_repair(server);
                }
            }
        }
    }

    fn advance(self: &Arc<Self>, pipeline: &mut Pipeline) {
        let write_quorum = self.ensemble.config.write_quorum;
        if let Some(flight) = &mut pipeline.guarded && flight.begin_commit(write_quorum) {
            let flight = pipeline.guarded.as_ref().expect("started under the writer lock");
            let witnesses = flight.witnesses.as_ref().expect("commit started");
            for (server, mark) in witnesses.iter().enumerate() {
                if *mark == Mark::Pending {
                    self.send(pipeline, server, RequestBody::Commit(Commit {
                        stream: self.ensemble.stream(), epoch: self.epoch, commit_lsn: flight.lsn,
                    }));
                }
            }
            return;
        }
        if let Some(flight) = &pipeline.guarded {
            if flight.releasable(write_quorum) {
                let through = flight.lsn;
                pipeline.guarded = None;
                self.release(pipeline, through);
                self.install_through(pipeline, through);
                self.repair_down(pipeline);
            }
            return;
        }

        if let Some(through) = pipeline.publication
            && pipeline.replicas.iter().filter(|replica| replica.installed >= through).count() >= write_quorum
        {
            self.release(pipeline, through);
            pipeline.publication = None;
        }
        let committed = pipeline.commit;
        let through = match pipeline.publication {
            Some(through) => through,
            None => {
                let mut held: Vec<_> = pipeline.replicas.iter().map(|replica| replica.acked).collect();
                held.select_nth_unstable_by(write_quorum - 1, |left, right| right.cmp(left));
                let through = held[write_quorum - 1].min(pipeline.next_lsn - 1);
                if through <= pipeline.commit {
                    self.install_through(pipeline, committed);
                    return;
                }
                pipeline.publication = Some(through);
                through
            }
        };
        self.install_through(pipeline, through);
        if through > pipeline.commit {
            self.install_through(pipeline, committed);
        }
    }

    fn install_through(self: &Arc<Self>, pipeline: &mut Pipeline, through: i64) {
        for server in 0..pipeline.replicas.len() {
            let replica = &mut pipeline.replicas[server];
            if matches!(replica.link, Link::Live) && replica.acked >= through
                && replica.installed < through && replica.installing < through
            {
                replica.installing = through;
                self.send(pipeline, server, RequestBody::Commit(Commit {
                    stream: self.ensemble.stream(), epoch: self.epoch, commit_lsn: through,
                }));
            }
        }
    }

    fn release(&self, pipeline: &mut Pipeline, quorum: i64) {
        rtdb_types::residence_trace::logkeeper_publication(
            &self.ensemble.config.stream, self.epoch, self.ensemble.identity(), quorum,
            self.ensemble.config.write_quorum,
            || pipeline.replicas.iter().enumerate().filter_map(|(peer, replica)|
                (replica.installed >= quorum).then_some(peer)).collect(),
        );
        let previous = pipeline.commit;
        pipeline.commit = quorum;
        self.tip.send_replace(Tip::Open(quorum));
        let first = (previous + 1 - pipeline.buffer_first).max(0) as usize;
        let last = (quorum - pipeline.buffer_first) as usize;
        for buffered in pipeline.buffer.range_mut(first..=last) {
            if let Some(receipt) = buffered.receipt.take()
                && receipt.send(Ok(record(&buffered.entry).metadata)).is_ok()
            {
                rtdb_types::residence_trace::logkeeper_receipt(
                    &self.ensemble.config.stream, self.epoch, self.ensemble.identity(), buffered.entry.lsn, quorum,
                );
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
            if let Some(confirming) = pipeline.confirming {
                for round in std::iter::once(confirming.round).chain(confirming.next) { round.outcome.send_replace(Some(Confirmation::Deposed { promised })); }
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
                    if pipeline.guarded.is_some() { return Err(self.guarded_outcome_unknown()); }
                    let entries = pipeline.buffer.iter().skip((from - pipeline.buffer_first) as usize).map(|buffered| buffered.entry.clone()).collect();
                    let body = ensemble.repair_request(epoch, from, entries, pipeline.commit);
                    pipeline.replicas[server].link = Link::Live;
                    self.send(pipeline, server, body);
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
                Some(entries) => from = deposed_by(epoch, ensemble.overwrite(server, epoch, from, entries, false).await?)?.last_lsn + 1,
                None if empty_fetches < 50 => { empty_fetches += 1; tokio::time::sleep(Duration::from_millis(20)).await; }
                None => return Err(Error::Remote { key: ensemble.config.stream.clone(), message: format!("no live server holds lsn {from} to repair from") }),
            }
        }
    }

    /// Succeeds once a write quorum has confirmed its stream is synced to this writer's promised epoch in a round that started after this call.
    /// So no successor had committed anything when the call began: a successor commits only after sealing a write quorum, which meets the confirming one.
    /// Concurrent callers share a round, at most one is in flight, and callers arriving during it wait for the next.
    /// A busy writer's round is answered by the replies to its appends and commits sent after it started; an idle writer's sends each server a check.
    pub async fn confirm_leadership(self: &Arc<Self>) -> Result<()> {
        let mut outcome = {
            let mut state = self.state.lock();
            match &mut *state {
                State::Open(pipeline) => self.join_confirmation(pipeline),
                State::Deposed { promised } => return Err(Error::Deposed { epoch: self.epoch, promised: *promised }),
            }
        };
        let confirmation = outcome.wait_for(Option::is_some).await.map_err(|_| Error::Closed("writer dropped".into()))?.clone().expect("waited for an outcome");
        match confirmation {
            Confirmation::Confirmed => Ok(()),
            Confirmation::Deposed { promised } => Err(Error::Deposed { epoch: self.epoch, promised }),
            Confirmation::Unavailable(message) => Err(Error::Remote { key: self.ensemble.config.stream.clone(), message }),
        }
    }

    /// Confirms the durable epoch on a fresh quorum without submitting disk or append work.
    pub async fn confirm_authority(&self, port: u16) -> Result<()> {
        let still_open = || match &*self.tip.borrow() {
            Tip::Open(_) => Ok(()),
            Tip::Deposed { promised } => Err(Error::Deposed { epoch: self.epoch, promised: *promised }),
        };
        still_open()?;
        let quorum = self.ensemble.config.write_quorum;
        if quorum <= self.ensemble.len() - quorum {
            return Err(Error::Remote {
                key: self.ensemble.config.stream.clone(),
                message: "durable writer authority requires an intersecting write quorum".into(),
            });
        }
        let mut replies: FuturesUnordered<_> = (0..self.ensemble.len())
            .map(|server| self.ensemble.confirm_authority(server, self.epoch, port)).collect();
        let mut confirmed = 0;
        while let Some(reply) = replies.next().await {
            match reply {
                Ok(None) => {
                    confirmed += 1;
                    if confirmed == quorum { return still_open(); }
                }
                Ok(Some(promised)) if promised > self.epoch => {
                    self.depose(&mut self.state.lock(), promised);
                    return Err(Error::Deposed { epoch: self.epoch, promised });
                }
                _ => {}
            }
            if confirmed + replies.len() < quorum { break; }
        }
        Err(Error::Remote { key: self.ensemble.config.stream.clone(), message: "a write quorum did not confirm durable writer authority".into() })
    }

    fn join_confirmation(self: &Arc<Self>, pipeline: &mut Pipeline) -> watch::Receiver<Option<Confirmation>> {
        match &mut pipeline.confirming {
            Some(confirming) => confirming.next.get_or_insert_with(Round::new).outcome.subscribe(),
            None => {
                let round = Round::new();
                let outcome = round.outcome.subscribe();
                self.start_round(pipeline, round);
                outcome
            }
        }
    }

    /// A live server with an append in flight is answered by the commit that follows once the pipeline reaches its tip, or by later appends; every other server is sent a check.
    fn start_round(self: &Arc<Self>, pipeline: &mut Pipeline, round: Arc<Round>) {
        // Every server is asked directly: a reply to an append in flight could otherwise be the round's only answer from a server that then fails, leaving the round unsettled.
        pipeline.confirming = Some(Confirming { round: Arc::clone(&round), marks: vec![Mark::Pending; pipeline.replicas.len()], next: None });
        for server in 0..pipeline.replicas.len() { self.send_check(server, &round); }
    }

    /// Records one server's answer to `round` if it is still the round in flight, and ends the round once its outcome is settled.
    fn mark(self: &Arc<Self>, pipeline: &mut Pipeline, round: &Arc<Round>, server: usize, confirmed: bool) {
        let Some(confirming) = &mut pipeline.confirming else { return };
        if !Arc::ptr_eq(&confirming.round, round) { return; }
        let mark = &mut confirming.marks[server];
        if confirmed { *mark = Mark::Confirmed; } else if *mark == Mark::Pending { *mark = Mark::Failed; }
        let quorum = self.ensemble.config.write_quorum;
        let count = |wanted: Mark| confirming.marks.iter().filter(|mark| **mark == wanted).count();
        let outcome = if count(Mark::Confirmed) >= quorum {
            Confirmation::Confirmed
        } else if count(Mark::Failed) > confirming.marks.len() - quorum {
            Confirmation::Unavailable(format!("{} of {} servers failed to confirm leadership, a write quorum is {quorum}", count(Mark::Failed), confirming.marks.len()))
        } else {
            return;
        };
        let finished = pipeline.confirming.take().expect("matched above");
        finished.round.outcome.send_replace(Some(outcome));
        if let Some(next) = finished.next { self.start_round(pipeline, next); }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guarded_quorum_requires_exact_original_replies_from_distinct_dispatched_journals() {
        let marks = vec![Mark::Pending, Mark::Pending, Mark::Failed];
        let mut flight = GuardedFlight { lsn: 12, originals: marks.clone(), witnesses: None };
        let accepted = |last_lsn, commit_lsn| Ok(Ack::Accepted(Accepted { last_lsn, commit_lsn }));
        let original = Some(GuardedReply::Original(12));
        let commit = Some(GuardedReply::Commit(12));

        flight.record_reply(0, original, &accepted(12, 11));
        flight.record_reply(0, original, &accepted(12, 11));
        flight.record_reply(1, None, &accepted(12, 11));
        flight.record_reply(1, commit, &accepted(12, 12));
        flight.record_reply(1, Some(GuardedReply::Original(11)), &accepted(12, 11));
        flight.record_reply(2, original, &accepted(12, 11));
        assert!(!flight.begin_commit(2));
        assert!(flight.can_reach_quorum(2));

        flight.record_reply(1, original, &accepted(12, 11));
        assert!(flight.begin_commit(2));
        assert!(!flight.releasable(2));
        flight.record_reply(0, commit, &accepted(12, 12));
        flight.record_reply(0, commit, &accepted(12, 12));
        flight.record_reply(1, original, &accepted(12, 12));
        flight.record_reply(2, commit, &accepted(12, 12));
        assert!(!flight.releasable(2));
        flight.record_reply(1, commit, &accepted(12, 12));
        assert!(flight.releasable(2));

        let mut flight = GuardedFlight { lsn: 12, originals: marks, witnesses: None };
        flight.record_reply(1, original, &accepted(13, 11));
        assert!(!flight.can_reach_quorum(2));
    }

    #[tokio::test]
    async fn guarded_recovery_excludes_a_minority_and_requires_original_evidence_to_commit() {
        use crate::client::{Loglet, StreamConfig, certify_committed_range, copy_certified_range, open_writer_on,
            original_sync_observations, timely_original_sync_upper};
        use crate::server::{PreparedJournal, ServerOptions};
        use rtdb_types::JournalEndpoint;
        use tokio::net::TcpListener;
        use tokio_util::sync::CancellationToken;

        let cancellation = CancellationToken::new();
        let mut endpoints = Vec::new();
        let mut directories = Vec::new();
        let mut servers = Vec::new();
        for _ in 0..4 {
            let directory = tempfile::tempdir().unwrap();
            let options = ServerOptions { data_dir: directory.path().to_owned(), fsync: true, cache_bytes: 0 };
            let (provider, journal) = PreparedJournal::open_for_member(options, uuid::Uuid::new_v4().to_string()).await.unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            endpoints.push(JournalEndpoint::Member { address: listener.local_addr().unwrap().to_string(), journal });
            servers.push(tokio::spawn(provider.serve(listener, cancellation.clone())));
            directories.push(directory);
        }

        for (stream, originals, copies, guarded_before, expected) in [
            ("minority", 1, 0, None, -1), ("quorum", 2, 0, Some(i64::MAX), 0),
            ("copied", 1, 1, None, -1), ("late", 3, 0, Some(1), -1),
            ("move-recovered-tail", 2, 0, Some(i64::MAX), 0),
        ] {
            let ensemble = Ensemble::new(StreamConfig { servers: endpoints[..3].to_vec(), stream: stream.to_owned(), write_quorum: 2, maximum_message_bytes: 1024 });
            let timed_origin = guarded_before.map(|_| ensemble.timed_origin([7; 32]));
            let entry = Entry { lsn: 0, epoch: 1, timestamp: 1, payload: Bytes::from_static(b"guarded"), guarded: true, guarded_before, timed_origin };
            for server in 0..3 {
                deposed_by(1, ensemble.overwrite(server, 1, 0, Vec::new(), true).await.unwrap()).unwrap();
                deposed_by(1, ensemble.activate(server, 1).await.unwrap()).unwrap();
                if server < originals {
                    let (done, durable) = oneshot::channel();
                    let request = ensemble.append_request(1, entry.clone(), -1);
                    ensemble.links[server].send(request, Box::new(move |reply| { let _ = done.send(reply); }));
                    let reply = tokio::time::timeout(CALL_TIMEOUT, durable).await.unwrap().unwrap();
                    if guarded_before == Some(1) {
                        assert!(matches!(ensemble.ack(server, reply), Err(Error::Remote { message, .. }) if message.contains("exclusive cutoff")));
                    } else {
                        deposed_by(1, ensemble.ack(server, reply).unwrap()).unwrap();
                    }
                } else if server < originals + copies {
                    let sealed = ensemble.seal(server, 2).await.unwrap();
                    assert_eq!(sealed.last_lsn, -1);
                    // Partial recovery can leave a newer epoch's copy without any original observation.
                    deposed_by(2, ensemble.overwrite(server, 2, 0, vec![entry.clone()], false).await.unwrap()).unwrap();
                }
            }
            if stream == "move-recovered-tail" {
                let ready_to_copy = Writer::recover_moved(&ensemble, 0, 3, CopyFrom::Retained).await.unwrap();
                assert_eq!(ready_to_copy.history.last, expected);
                let mut predecessor_witnesses = 0;
                for server in 0..ensemble.len() {
                    let sealed = ensemble.seal(server, 3).await.unwrap();
                    predecessor_witnesses += usize::from(sealed.commit_lsn == expected);
                }
                assert!(predecessor_witnesses >= ensemble.config.write_quorum);

                let destination = StreamConfig {
                    servers: vec![endpoints[3].clone()], write_quorum: 1, ..ensemble.config.clone()
                };
                let chain = vec![Loglet {
                    servers: ensemble.config.servers.clone(), start_lsn: 0, write_quorum: ensemble.config.write_quorum,
                }];
                let moved = open_writer_on(chain, destination.clone(), 4, CopyFrom::Retained, cancellation.clone()).await.unwrap();
                let certificate = certify_committed_range(&destination, 0, 0).await.unwrap();
                let copied = copy_certified_range(&destination, &certificate, 1024, 1).await.unwrap();
                assert_eq!(copied, [entry.clone()]);
                assert!(original_sync_observations(&certificate, &copied[0], destination.servers.clone(), 1024)
                    .await.unwrap().is_empty());
                let original = original_sync_observations(&certificate, &copied[0], endpoints.clone(), 1024).await.unwrap();
                assert_eq!(original.len(), 2);
                assert!(timely_original_sync_upper(&copied[0], &original).is_some());
                drop(moved);
            } else {
                assert_eq!(Writer::seal_recover_committed(&ensemble, 3).await.unwrap(), expected);
            }
        }

        cancellation.cancel();
        for server in servers { server.await.unwrap().unwrap(); }
        drop(directories);
    }

    #[test]
    fn an_original_without_a_sample_is_possible_but_has_no_positive_vote() {
        let evidence = [
            guarded_probe_evidence(true, Some(true), Some(10), None),
            guarded_probe_evidence(true, Some(true), None, None),
        ];
        assert!(matches!(decide_from_sealed_probes(&evidence, 2), TailDecision::Unknown));
    }
}
