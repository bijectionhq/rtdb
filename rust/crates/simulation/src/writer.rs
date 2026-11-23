//! A message-driven model of `rtdb_logkeeper::client::writer::Writer`: open (seal quorum, recovery, sync, commit), the append pipeline, quorum commit, and lazy repair of lagging servers.
//!
//! The real writer is async code over tokio and its connection pool, so it is modelled rather than driven; each await point of the real code is a request here whose reply resumes the step after it.
//! The decisions themselves are the real ones: `winner` and `plan_repair` pick the history and where each server diverges from it.
//! What the model changes is scale: the batch and recent-entry limits are small so that chunked copies and repair from a peer happen in short runs.

use std::collections::{BTreeMap, VecDeque};

use bytes::Bytes;
use rtdb_logkeeper::simulation::{Entries, Entry, Holding, ResponseBody, Sealed, plan_repair, winner};

use crate::server::{Body, refused_by};
use crate::Bug;

pub(crate) const WRITE_QUORUM: usize = 2;
pub(crate) const SERVERS: usize = 3;
/// `connection::CALL_TIMEOUT`, scaled.
const CALL_TIMEOUT: i64 = 1000;
/// `writer::SEAL_GRACE`, scaled.
const SEAL_GRACE: i64 = 20;
/// `writer::REPAIR_BACKOFF`, scaled.
const REPAIR_BACKOFF: i64 = 40;
/// The 20 ms sleep between a repair's empty fetches, scaled.
const REPAIR_SLEEP: i64 = 5;
const MAXIMUM_EMPTY_FETCHES: u32 = 50;
/// `ensemble::MAXIMUM_BATCH_ENTRIES` is 1024; small here so copies take several chunks.
const BATCH_ENTRIES: usize = 2;
/// `writer::RECENT_ENTRIES` is 4096; small here so repairs fetch from a peer.
const RECENT_ENTRIES: usize = 3;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Fail {
    Deposed(u64),
    Unavailable(String),
}

/// What the writer tells the world in a step.
#[derive(Debug)]
pub(crate) enum Note {
    /// A write quorum of seal replies has been gathered at this epoch: each server, its trim point, and what it held when it sealed.
    SealQuorum(Vec<(usize, i64, Vec<Entry>)>),
    /// The recovered history, taken from the winning seal reply's server at the moment it sealed, and the servers it was synced onto.
    Opened { history: Vec<Entry>, trim: i64, last: i64, synced: Vec<usize> },
    /// Entries newly committed by the pipeline; `acknowledged` are those a client is told about.
    Committed { entries: Vec<Entry>, acknowledged: Vec<Entry> },
    Ended(Fail),
}

/// Requests and notes produced by one call into the writer.
pub(crate) struct Io {
    pub now: i64,
    pub next_id: u64,
    pub sends: Vec<(usize, u64, Body)>,
    pub notes: Vec<Note>,
}

impl Io {
    fn send(&mut self, server: usize, body: Body) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.sends.push((server, id, body));
        id
    }
}

/// A reply as the writer receives it: the response, or the failure of its connection or its timeout.
pub(crate) type Reply = Result<(ResponseBody, Option<Vec<Entry>>), String>;

#[derive(Clone, Debug)]
enum Purpose {
    OpenSeal(usize),
    SyncTrim(usize),
    SyncFetch(usize),
    SyncOverwrite(usize),
    OpenCommit(usize),
    /// An append, commit or rejoining overwrite: `Writer::acknowledged`.
    Pipeline(usize),
    RepairSeal(usize),
    RepairTrim(usize),
    RepairFetch { server: usize, remaining: Vec<usize> },
    RepairOverwrite(usize),
    /// A checkpoint's trim; its outcome changes nothing here.
    Trim,
}

struct Call {
    purpose: Purpose,
    /// `call` has a timeout; a pipeline `send` has none.
    deadline: Option<i64>,
}

enum SealResult {
    Waiting,
    Sealed(Sealed, Vec<Entry>),
    Failed,
}

enum Sync {
    Trimming { from: i64 },
    Fetching { from: i64 },
    Overwriting { complete: bool },
    Done(Result<(), Fail>),
}

enum Link {
    Live,
    Repairing,
    Down(i64),
}

struct Replica {
    acked: i64,
    link: Link,
}

struct Buffered {
    entry: Entry,
    receipt: bool,
}

/// One repair in flight, as the locals of `Writer::repair`.
struct Repair {
    from: i64,
    empty_fetches: u32,
    buffer_first: i64,
    sleeping_until: Option<i64>,
}

struct Pipeline {
    next_lsn: i64,
    commit: i64,
    history: Holding,
    buffer_first: i64,
    buffer: VecDeque<Buffered>,
    replicas: Vec<Replica>,
    repairs: Vec<Option<Repair>>,
}

enum Phase {
    Sealing { results: Vec<SealResult>, grace: Option<i64> },
    Syncing { history: Holding, content: Vec<Entry>, source: usize, sealed: Vec<(usize, Sealed)>, syncs: BTreeMap<usize, Sync> },
    Committing { history: Holding, content: Vec<Entry>, synced: Vec<usize>, waiting: usize, failure: Option<Fail> },
    Serving(Pipeline),
    Ended,
}

pub(crate) struct Writer {
    pub epoch: u64,
    phase: Phase,
    calls: BTreeMap<u64, Call>,
    bug: Option<Bug>,
}

fn deposed_by(epoch: u64, body: &ResponseBody) -> Result<i64, Fail> {
    match body {
        ResponseBody::Accepted(accepted) => Ok(accepted.last_lsn),
        _ if refused_by(body, epoch).is_some() => Err(Fail::Deposed(refused_by(body, epoch).unwrap())),
        other => Err(Fail::Unavailable(format!("{other:?}"))),
    }
}

/// The broken variant of `winner`: the reply whose last entry has the greatest epoch, then the longest.
fn winner_by_last_entry_epoch(replies: &[Sealed]) -> usize {
    (0..replies.len()).max_by_key(|&index| (replies[index].runs.last().map_or(0, |run| run.epoch), replies[index].last_lsn)).expect("a quorum of replies")
}

impl Writer {
    /// `Writer::open`: seals every server and waits for a write quorum of replies.
    pub fn open(epoch: u64, bug: Option<Bug>, io: &mut Io) -> Self {
        let mut writer = Self { epoch, phase: Phase::Sealing { results: (0..SERVERS).map(|_| SealResult::Waiting).collect(), grace: None }, calls: BTreeMap::new(), bug };
        for server in 0..SERVERS { writer.call(io, server, Body::Seal { epoch }, Purpose::OpenSeal(server)); }
        writer
    }

    pub fn serving(&self) -> bool { matches!(self.phase, Phase::Serving(_)) }

    pub fn ended(&self) -> bool { matches!(self.phase, Phase::Ended) }

    pub fn commit(&self) -> Option<i64> { match &self.phase { Phase::Serving(pipeline) => Some(pipeline.commit), _ => None } }

    /// Whether every enqueued entry has committed.
    pub fn caught_up(&self) -> bool { match &self.phase { Phase::Serving(pipeline) => pipeline.commit == pipeline.next_lsn - 1, _ => false } }

    fn call(&mut self, io: &mut Io, server: usize, body: Body, purpose: Purpose) {
        let id = io.send(server, body);
        self.calls.insert(id, Call { purpose, deadline: Some(io.now + CALL_TIMEOUT) });
    }

    fn send(&mut self, io: &mut Io, server: usize, body: Body) {
        let id = io.send(server, body);
        self.calls.insert(id, Call { purpose: Purpose::Pipeline(server), deadline: None });
    }

    fn end(&mut self, io: &mut Io, fail: Fail) {
        if matches!(self.phase, Phase::Ended) { return; }
        self.phase = Phase::Ended;
        self.calls.clear();
        io.notes.push(Note::Ended(fail));
    }

    /// Fires call timeouts, the seal grace, repair sleeps and backoffs that are due.
    pub fn tick(&mut self, io: &mut Io) {
        let expired: Vec<u64> = self.calls.iter().filter(|(_, call)| call.deadline.is_some_and(|deadline| deadline <= io.now)).map(|(id, _)| *id).collect();
        for id in expired { self.reply(io, id, Err("request timed out".into())); }
        if let Phase::Sealing { grace: Some(deadline), .. } = &self.phase && *deadline <= io.now { self.seal_quorum_done(io); }
        if let Phase::Serving(pipeline) = &mut self.phase {
            let woken: Vec<usize> = (0..SERVERS).filter(|&server| pipeline.repairs[server].as_ref().is_some_and(|repair| repair.sleeping_until.is_some_and(|until| until <= io.now))).collect();
            for server in woken {
                if let Phase::Serving(pipeline) = &mut self.phase { pipeline.repairs[server].as_mut().unwrap().sleeping_until = None; }
                self.repair_loop(io, server);
            }
        }
    }

    pub fn reply(&mut self, io: &mut Io, id: u64, reply: Reply) {
        let Some(call) = self.calls.remove(&id) else { return };
        if matches!(self.phase, Phase::Ended) { return; }
        match call.purpose {
            Purpose::OpenSeal(server) => self.on_open_seal(io, server, reply),
            Purpose::SyncTrim(server) => self.on_sync_trim(io, server, reply),
            Purpose::SyncFetch(server) => self.on_sync_fetch(io, server, reply),
            Purpose::SyncOverwrite(server) => self.on_sync_overwrite(io, server, reply),
            Purpose::OpenCommit(server) => self.on_open_commit(io, server, reply),
            Purpose::Pipeline(server) => self.acknowledged(io, server, reply),
            Purpose::RepairSeal(server) => self.on_repair_seal(io, server, reply),
            Purpose::RepairTrim(server) => self.on_repair_step(io, server, reply, None),
            Purpose::RepairFetch { server, remaining } => self.on_repair_fetch(io, server, remaining, reply),
            Purpose::RepairOverwrite(server) => self.on_repair_step(io, server, reply, Some(())),
            Purpose::Trim => (),
        }
    }

    // ---- open: seal_quorum ----

    fn on_open_seal(&mut self, io: &mut Io, server: usize, reply: Reply) {
        let epoch = self.epoch;
        let Phase::Sealing { results, grace } = &mut self.phase else { return };
        match reply {
            Ok((ResponseBody::Sealed(sealed), _)) if sealed.promised > epoch => { let promised = sealed.promised; return self.end(io, Fail::Deposed(promised)); }
            Ok((ResponseBody::Sealed(sealed), oracle)) => results[server] = SealResult::Sealed(sealed, oracle.expect("a seal reply carries its server's entries")),
            _ => results[server] = SealResult::Failed,
        }
        let sealed = results.iter().filter(|result| matches!(result, SealResult::Sealed(..))).count();
        if sealed >= WRITE_QUORUM && grace.is_none() { *grace = Some(io.now + SEAL_GRACE); }
        if results.iter().all(|result| !matches!(result, SealResult::Waiting)) { self.seal_quorum_done(io); }
    }

    /// The end of `seal_quorum`: every server answered or the grace after a quorum ran out.
    fn seal_quorum_done(&mut self, io: &mut Io) {
        let Phase::Sealing { results, .. } = &mut self.phase else { return };
        let replies: Vec<(usize, Sealed, Vec<Entry>)> = std::mem::take(results).into_iter().enumerate()
            .filter_map(|(server, result)| match result { SealResult::Sealed(sealed, oracle) => Some((server, sealed, oracle)), _ => None }).collect();
        self.calls.retain(|_, call| !matches!(call.purpose, Purpose::OpenSeal(_)));
        if replies.len() < WRITE_QUORUM { return self.end(io, Fail::Unavailable(format!("sealed {} of {SERVERS} servers", replies.len()))); }
        io.notes.push(Note::SealQuorum(replies.iter().map(|(server, sealed, held)| (*server, sealed.trim_lsn, held.clone())).collect()));
        let sealed: Vec<Sealed> = replies.iter().map(|(_, sealed, _)| sealed.clone()).collect();
        let chosen = match self.bug { Some(Bug::WinnerByLastEntryEpoch) => winner_by_last_entry_epoch(&sealed), _ => winner(&sealed) };
        let (source, winning, oracle) = &replies[chosen];
        let history = Holding::from(winning);
        let content = oracle.clone();
        let sealed: Vec<(usize, Sealed)> = replies.iter().map(|(server, sealed, _)| (*server, sealed.clone())).collect();
        self.phase = Phase::Syncing { history: history.clone(), content, source: *source, sealed: sealed.clone(), syncs: BTreeMap::new() };
        for (server, sealed) in sealed { self.sync_server(io, server, &sealed, &history); }
        self.sync_quorum_check(io);
    }

    // ---- open: sync_quorum / sync_server / copy ----

    fn set_sync(&mut self, server: usize, sync: Sync) {
        if let Phase::Syncing { syncs, .. } = &mut self.phase { syncs.insert(server, sync); }
    }

    fn sync_server(&mut self, io: &mut Io, server: usize, sealed: &Sealed, history: &Holding) {
        let plan = match plan_repair(&Holding::from(sealed), history) {
            Ok(plan) => plan,
            Err(message) => return self.set_sync(server, Sync::Done(Err(Fail::Unavailable(message)))),
        };
        // `sync_server`'s branch for a server holding nothing below `recovered.start` applies only to a moved stream, whose start lies above the history's trim point.
        if let Some(through) = plan.trim_through {
            self.set_sync(server, Sync::Trimming { from: plan.from });
            self.call(io, server, Body::Trim { through }, Purpose::SyncTrim(server));
        } else {
            self.copy_step(io, server, plan.from);
        }
    }

    fn on_sync_trim(&mut self, io: &mut Io, server: usize, reply: Reply) {
        let Phase::Syncing { syncs, .. } = &self.phase else { return };
        let Some(Sync::Trimming { from }) = syncs.get(&server) else { return };
        let from = *from;
        match reply.map_err(Fail::Unavailable).and_then(|(body, _)| deposed_by(self.epoch, &body)) {
            Ok(_) => self.copy_step(io, server, from),
            Err(fail) => { self.set_sync(server, Sync::Done(Err(fail))); self.sync_quorum_check(io); }
        }
    }

    /// One turn of `copy`'s loop: fetch the next chunk from the source, or finish with an empty complete overwrite.
    fn copy_step(&mut self, io: &mut Io, server: usize, from: i64) {
        let Phase::Syncing { history, source, .. } = &self.phase else { return };
        let (to, source, epoch) = (history.last, *source, self.epoch);
        if from > to {
            self.set_sync(server, Sync::Overwriting { complete: true });
            self.call(io, server, Body::Overwrite { epoch, from, entries: Vec::new(), complete: true, commit: -1 }, Purpose::SyncOverwrite(server));
        } else {
            self.set_sync(server, Sync::Fetching { from });
            self.call(io, source, Body::Fetch { from, to: to + 1, max_entries: BATCH_ENTRIES }, Purpose::SyncFetch(server));
        }
    }

    fn on_sync_fetch(&mut self, io: &mut Io, server: usize, reply: Reply) {
        let Phase::Syncing { history, syncs, .. } = &self.phase else { return };
        let Some(Sync::Fetching { from }) = syncs.get(&server) else { return };
        let (from, to, epoch) = (*from, history.last, self.epoch);
        let entries = match reply {
            Ok((ResponseBody::Entries(fetched), _)) if from <= fetched.trim_lsn => Err(format!("lsn {from} is trimmed")),
            Ok((ResponseBody::Entries(fetched), _)) if fetched.entries.first().is_none_or(|entry| entry.lsn != from) => Err(format!("the recovery source no longer holds lsn {from}")),
            Ok((ResponseBody::Entries(fetched), _)) => Ok(fetched.entries),
            Ok((other, _)) => Err(format!("{other:?}")),
            Err(error) => Err(error),
        };
        match entries {
            Err(message) => { self.set_sync(server, Sync::Done(Err(Fail::Unavailable(message)))); self.sync_quorum_check(io); }
            Ok(entries) => {
                let last = entries.last().map_or(from - 1, |entry| entry.lsn);
                let complete = last >= to;
                self.set_sync(server, Sync::Overwriting { complete });
                self.call(io, server, Body::Overwrite { epoch, from, entries, complete, commit: -1 }, Purpose::SyncOverwrite(server));
            }
        }
    }

    fn on_sync_overwrite(&mut self, io: &mut Io, server: usize, reply: Reply) {
        let Phase::Syncing { syncs, .. } = &self.phase else { return };
        let Some(Sync::Overwriting { complete }) = syncs.get(&server) else { return };
        let complete = *complete;
        match reply.map_err(Fail::Unavailable).and_then(|(body, _)| deposed_by(self.epoch, &body)) {
            Ok(_) if complete => { self.set_sync(server, Sync::Done(Ok(()))); self.sync_quorum_check(io); }
            Ok(acked) => self.copy_step(io, server, acked + 1),
            Err(fail) => { self.set_sync(server, Sync::Done(Err(fail))); self.sync_quorum_check(io); }
        }
    }

    /// The end of `sync_quorum`'s first await, once every sealed server's sync has finished: commit on those that took it.
    fn sync_quorum_check(&mut self, io: &mut Io) {
        let epoch = self.epoch;
        let Phase::Syncing { history, content, sealed, syncs, .. } = &mut self.phase else { return };
        if sealed.iter().any(|(server, _)| !matches!(syncs.get(server), Some(Sync::Done(_)))) { return; }
        if let Some(promised) = syncs.values().find_map(|sync| match sync { Sync::Done(Err(Fail::Deposed(promised))) => Some(*promised), _ => None }) {
            return self.end(io, Fail::Deposed(promised));
        }
        let synced: Vec<usize> = syncs.iter().filter(|(_, sync)| matches!(sync, Sync::Done(Ok(())))).map(|(server, _)| *server).collect();
        if synced.len() < WRITE_QUORUM { return self.end(io, Fail::Unavailable(format!("recovered onto {} servers", synced.len()))); }
        let (history, content) = (history.clone(), std::mem::take(content));
        let commit = history.last;
        self.phase = Phase::Committing { history, content, synced: synced.clone(), waiting: synced.len(), failure: None };
        for server in synced { self.call(io, server, Body::Commit { epoch, commit }, Purpose::OpenCommit(server)); }
    }

    fn on_open_commit(&mut self, io: &mut Io, _server: usize, reply: Reply) {
        let epoch = self.epoch;
        let Phase::Committing { waiting, failure, .. } = &mut self.phase else { return };
        *waiting -= 1;
        // Only a refusal fails the open: a commit that failed to arrive, or failed at the server, is dropped by `commits.into_iter().flatten()`.
        if let Ok((body @ ResponseBody::Refused(_), _)) = &reply && let Err(fail) = deposed_by(epoch, body) && failure.is_none() { *failure = Some(fail); }
        if *waiting > 0 { return; }
        if let Some(fail) = failure.take() { return self.end(io, fail); }
        let Phase::Committing { history, content, synced, .. } = std::mem::replace(&mut self.phase, Phase::Ended) else { unreachable!() };
        self.establish(io, history, content, synced);
    }

    fn establish(&mut self, io: &mut Io, history: Holding, content: Vec<Entry>, synced: Vec<usize>) {
        let last = history.last;
        io.notes.push(Note::Opened { history: content, trim: history.trim, last, synced: synced.clone() });
        let replicas = (0..SERVERS).map(|server| if synced.contains(&server) {
            Replica { acked: last, link: Link::Live }
        } else {
            Replica { acked: -1, link: Link::Down(io.now - REPAIR_BACKOFF) }
        }).collect();
        self.phase = Phase::Serving(Pipeline { next_lsn: last + 1, commit: last, history, buffer_first: last + 1, buffer: VecDeque::new(), replicas, repairs: (0..SERVERS).map(|_| None).collect() });
        self.repair_down(io);
    }

    // ---- the pipeline ----

    /// `Writer::enqueue`; the lsn the entry was given.
    pub fn enqueue(&mut self, io: &mut Io, payload: Bytes, timestamp: i64) -> Option<i64> {
        let epoch = self.epoch;
        let Phase::Serving(pipeline) = &mut self.phase else { return None };
        let lsn = pipeline.next_lsn;
        pipeline.next_lsn += 1;
        let entry = Entry { lsn, epoch, timestamp, payload, guarded: false, guarded_before: None };
        pipeline.history.push(lsn, epoch);
        pipeline.buffer.push_back(Buffered { entry: entry.clone(), receipt: true });
        let commit = pipeline.commit;
        let live: Vec<usize> = (0..SERVERS).filter(|&server| matches!(pipeline.replicas[server].link, Link::Live)).collect();
        for server in live { self.send(io, server, Body::Append { epoch, first: lsn, entries: vec![entry.clone()], commit }); }
        self.repair_down(io);
        Some(lsn)
    }

    fn acknowledged(&mut self, io: &mut Io, server: usize, reply: Reply) {
        let epoch = self.epoch;
        if let Ok((body, _)) = &reply && let Some(promised) = refused_by(body, epoch) { return self.end(io, Fail::Deposed(promised)); }
        let Phase::Serving(pipeline) = &mut self.phase else { return };
        match reply {
            Ok((ResponseBody::Accepted(accepted), _)) => {
                let replica = &mut pipeline.replicas[server];
                replica.acked = replica.acked.max(accepted.last_lsn);
                self.advance(io);
            }
            _ => if matches!(pipeline.replicas[server].link, Link::Live) {
                pipeline.replicas[server].link = Link::Repairing;
                self.spawn_repair(io, server);
            },
        }
    }

    fn advance(&mut self, io: &mut Io) {
        let epoch = self.epoch;
        let Phase::Serving(pipeline) = &mut self.phase else { return };
        let mut acked: Vec<i64> = pipeline.replicas.iter().map(|replica| replica.acked).collect();
        acked.sort_unstable_by(|left, right| right.cmp(left));
        let quorum = acked[WRITE_QUORUM - 1].min(pipeline.next_lsn - 1);
        if quorum <= pipeline.commit { return; }
        let previous = pipeline.commit;
        pipeline.commit = quorum;
        let first = (previous + 1 - pipeline.buffer_first).max(0) as usize;
        let last = (quorum - pipeline.buffer_first) as usize;
        let mut entries = Vec::new();
        let mut acknowledged = Vec::new();
        for buffered in pipeline.buffer.range_mut(first..=last) {
            entries.push(buffered.entry.clone());
            if buffered.receipt { buffered.receipt = false; acknowledged.push(buffered.entry.clone()); }
        }
        io.notes.push(Note::Committed { entries, acknowledged });
        if pipeline.commit == pipeline.next_lsn - 1 {
            let live: Vec<usize> = (0..SERVERS).filter(|&server| matches!(pipeline.replicas[server].link, Link::Live)).collect();
            for server in live { self.send(io, server, Body::Commit { epoch, commit: quorum }); }
        }
        let Phase::Serving(pipeline) = &mut self.phase else { return };
        while pipeline.buffer.front().is_some_and(|buffered| buffered.entry.lsn <= pipeline.commit) && pipeline.buffer.len() > RECENT_ENTRIES {
            pipeline.buffer.pop_front();
            pipeline.buffer_first += 1;
        }
    }

    fn repair_down(&mut self, io: &mut Io) {
        let Phase::Serving(pipeline) = &mut self.phase else { return };
        let due: Vec<usize> = (0..SERVERS).filter(|&server| matches!(pipeline.replicas[server].link, Link::Down(since) if io.now - since >= REPAIR_BACKOFF)).collect();
        for server in due {
            if let Phase::Serving(pipeline) = &mut self.phase { pipeline.replicas[server].link = Link::Repairing; }
            self.spawn_repair(io, server);
        }
    }

    // ---- repair ----

    fn spawn_repair(&mut self, io: &mut Io, server: usize) {
        let epoch = self.epoch;
        let Phase::Serving(pipeline) = &mut self.phase else { return };
        pipeline.repairs[server] = Some(Repair { from: 0, empty_fetches: 0, buffer_first: 0, sleeping_until: None });
        self.call(io, server, Body::Seal { epoch }, Purpose::RepairSeal(server));
    }

    /// The end of `spawn_repair`'s task with an error.
    fn repair_failed(&mut self, io: &mut Io, server: usize, fail: Fail) {
        match fail {
            Fail::Deposed(promised) => self.end(io, Fail::Deposed(promised)),
            Fail::Unavailable(_) => if let Phase::Serving(pipeline) = &mut self.phase {
                pipeline.repairs[server] = None;
                if matches!(pipeline.replicas[server].link, Link::Repairing) { pipeline.replicas[server].link = Link::Down(io.now); }
            },
        }
    }

    fn on_repair_seal(&mut self, io: &mut Io, server: usize, reply: Reply) {
        let epoch = self.epoch;
        let sealed = match reply {
            Ok((ResponseBody::Sealed(sealed), _)) if sealed.promised > epoch => return self.end(io, Fail::Deposed(sealed.promised)),
            Ok((ResponseBody::Sealed(sealed), _)) => sealed,
            Ok((other, _)) => return self.repair_failed(io, server, Fail::Unavailable(format!("{other:?}"))),
            Err(error) => return self.repair_failed(io, server, Fail::Unavailable(error)),
        };
        let Phase::Serving(pipeline) = &mut self.phase else { return };
        let plan = match plan_repair(&Holding::from(&sealed), &pipeline.history) {
            Ok(plan) => plan,
            Err(message) => return self.repair_failed(io, server, Fail::Unavailable(message)),
        };
        pipeline.repairs[server].as_mut().expect("a repair in flight").from = plan.from;
        match plan.trim_through {
            Some(through) => self.call(io, server, Body::Trim { through }, Purpose::RepairTrim(server)),
            None => self.repair_loop(io, server),
        }
    }

    /// A trim or a partial overwrite of the repair answered: on to the next turn of its loop.
    fn on_repair_step(&mut self, io: &mut Io, server: usize, reply: Reply, overwrote: Option<()>) {
        match reply.map_err(Fail::Unavailable).and_then(|(body, _)| deposed_by(self.epoch, &body)) {
            Err(fail) => self.repair_failed(io, server, fail),
            Ok(acked) => {
                if overwrote.is_some() && let Phase::Serving(pipeline) = &mut self.phase { pipeline.repairs[server].as_mut().expect("a repair in flight").from = acked + 1; }
                self.repair_loop(io, server);
            }
        }
    }

    /// The top of `repair`'s loop: rejoin from the buffer, or fetch the next chunk from a live peer.
    fn repair_loop(&mut self, io: &mut Io, server: usize) {
        let epoch = self.epoch;
        let Phase::Serving(pipeline) = &mut self.phase else { return };
        let Some(repair) = pipeline.repairs[server].as_mut() else { return };
        let from = repair.from;
        if from >= pipeline.buffer_first {
            let entries = pipeline.buffer.iter().skip((from - pipeline.buffer_first) as usize).map(|buffered| buffered.entry.clone()).collect();
            let commit = pipeline.commit;
            pipeline.replicas[server].link = Link::Live;
            pipeline.repairs[server] = None;
            return self.send(io, server, Body::Overwrite { epoch, from, entries, complete: true, commit });
        }
        repair.buffer_first = pipeline.buffer_first;
        let sources: Vec<usize> = (0..SERVERS).filter(|&peer| peer != server && matches!(pipeline.replicas[peer].link, Link::Live)).collect();
        self.fetch_from(io, server, sources);
    }

    fn fetch_from(&mut self, io: &mut Io, server: usize, mut sources: Vec<usize>) {
        let Phase::Serving(pipeline) = &mut self.phase else { return };
        let Some(repair) = pipeline.repairs[server].as_mut() else { return };
        if sources.is_empty() {
            if repair.empty_fetches < MAXIMUM_EMPTY_FETCHES {
                repair.empty_fetches += 1;
                repair.sleeping_until = Some(io.now + REPAIR_SLEEP);
                return;
            }
            let from = repair.from;
            return self.repair_failed(io, server, Fail::Unavailable(format!("no live server holds lsn {from} to repair from")));
        }
        let source = sources.remove(0);
        let (from, to) = (repair.from, repair.buffer_first);
        self.call(io, source, Body::Fetch { from, to, max_entries: BATCH_ENTRIES }, Purpose::RepairFetch { server, remaining: sources });
    }

    fn on_repair_fetch(&mut self, io: &mut Io, server: usize, remaining: Vec<usize>, reply: Reply) {
        let epoch = self.epoch;
        let Phase::Serving(pipeline) = &mut self.phase else { return };
        let Some(repair) = pipeline.repairs[server].as_mut() else { return };
        let from = repair.from;
        let fetched: Entries = match reply {
            Ok((ResponseBody::Entries(fetched), _)) => fetched,
            _ => return self.fetch_from(io, server, remaining),
        };
        if from <= fetched.trim_lsn {
            repair.from = fetched.trim_lsn + 1;
            return self.call(io, server, Body::Trim { through: fetched.trim_lsn }, Purpose::RepairTrim(server));
        }
        if fetched.entries.first().is_some_and(|entry| entry.lsn == from) {
            return self.call(io, server, Body::Overwrite { epoch, from, entries: fetched.entries, complete: false, commit: -1 }, Purpose::RepairOverwrite(server));
        }
        self.fetch_from(io, server, remaining)
    }

    /// A checkpoint's trim of every server, as `rtdb_logkeeper::trim`.
    pub fn trim(&mut self, io: &mut Io, through: i64) {
        for server in 0..SERVERS { self.call(io, server, Body::Trim { through }, Purpose::Trim); }
    }
}
