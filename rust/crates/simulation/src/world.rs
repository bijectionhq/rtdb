//! The simulated cell: members, three log servers, the directory, the network between them, and a seeded scheduler that picks one event per step.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use bytes::Bytes;
use futures::executor::block_on;
use rtdb_directory::{DeploymentRecord, Directory, MemberId, MemberPool, MemberRecord, MemberRole, MemberState, MemoryDirectory, ReplaceOutcome, claim, park};
use rtdb_logkeeper::simulation::{Entry, ResponseBody};

use crate::check::{Checker, Snapshot};
use crate::rng::Rng;
use crate::server::{Body, Outcome, Return, Server, held};
use crate::writer::{Io, Note, Reply, SERVERS, Writer};
use crate::{Config, Summary, Violation};

const MEMBERS: usize = 3;
/// `member_lease`, scaled.
const LEASE: i64 = 100;
/// A connection that cannot reach its server fails after this long.
const CONNECT_TIMEOUT: i64 = 60;
/// How far each member's clock may be from true time.
const CLOCK_SKEW: i64 = 15;
const TRACE_LENGTH: usize = 200;

struct Session {
    id: u64,
    requests: VecDeque<(u64, Body)>,
    outstanding: BTreeSet<u64>,
    blocked_since: Option<i64>,
}

struct InFlight {
    to: Return,
    server: usize,
    body: ResponseBody,
    snapshot: Option<Vec<Entry>>,
}

/// A request a server will still read and decide although its sender's connection or process is gone; its reply goes nowhere.
struct Stale {
    process: usize,
    server: usize,
    body: Body,
}

struct Process {
    id: usize,
    member: MemberId,
    writer: Option<Writer>,
    /// The system time of the last transaction this process resolved or recovered.
    last_time: i64,
    skew: i64,
}

struct Slot {
    process: Option<Process>,
    restart_at: i64,
    generation: u32,
    /// Until when each link is cut; a link is cut while this is in the future.
    cut_from_server: [i64; SERVERS],
    cut_from_directory: i64,
}

pub(crate) struct World {
    config: Config,
    rng: Rng,
    now: i64,
    step: usize,
    directory: Arc<MemoryDirectory>,
    deployment: String,
    servers: Vec<Server>,
    server_restart_at: [i64; SERVERS],
    slots: Vec<Slot>,
    sessions: BTreeMap<(usize, usize), Session>,
    replies: Vec<InFlight>,
    stale: Vec<Stale>,
    next_id: u64,
    next_session: u64,
    next_process: usize,
    next_payload: u64,
    checker: Checker,
    trace: VecDeque<String>,
    summary: Summary,
    stormy: bool,
}

type Step = Result<(), String>;

impl World {
    pub fn new(config: Config) -> Self {
        let directory = MemoryDirectory::new();
        let deployment = block_on(directory.create_deployment(DeploymentRecord::new("sim", "org", vec![], 0))).expect("create the deployment").id;
        let rng = Rng::new(config.seed);
        let slots = (0..MEMBERS).map(|_| Slot { process: None, restart_at: 0, generation: 0, cut_from_server: [0; SERVERS], cut_from_directory: 0 }).collect();
        let summary = Summary { seed: config.seed, ..Summary::default() };
        Self {
            config, rng, now: 0, step: 0, directory, deployment, servers: (0..SERVERS).map(|_| Server::new()).collect(), server_restart_at: [0; SERVERS], slots,
            sessions: BTreeMap::new(), replies: Vec::new(), stale: Vec::new(), next_id: 1, next_session: 1, next_process: 0, next_payload: 1,
            checker: Checker::new(), trace: VecDeque::new(), summary, stormy: false,
        }
    }

    fn note(&mut self, event: String) {
        if self.trace.len() == TRACE_LENGTH { self.trace.pop_front(); }
        self.trace.push_back(format!("#{} t={} {event}", self.step, self.now));
    }

    fn fault(&mut self, event: String) {
        self.summary.faults += 1;
        self.note(event);
    }

    pub fn run(mut self) -> Result<Summary, Violation> {
        for step in 0..self.config.steps {
            self.step = step;
            if let Err(message) = self.step_once() {
                return Err(Violation { seed: self.config.seed, step, message, trace: self.trace.into_iter().collect() });
            }
        }
        self.summary.steps = self.config.steps;
        self.summary.committed = self.checker.committed_count();
        Ok(self.summary)
    }

    fn step_once(&mut self) -> Step {
        self.now += 1;
        self.restart_due()?;
        if self.rng.below(1000) < 3 {
            self.stormy = !self.stormy;
            self.note(format!("the weather turns {}", if self.stormy { "stormy" } else { "calm" }));
        }
        // Faults take 2% of steps in calm weather and 12% in a storm; the rest are the protocol's own events.
        let faults = if self.stormy { 120 } else { 20 };
        let roll = self.rng.below(1000);
        if roll < faults {
            match self.rng.below(76) {
                0..12 => self.crash_member()?,
                12..20 => self.crash_server()?,
                20..45 => self.partition()?,
                45..60 => self.break_session()?,
                60..66 => self.suspect()?,
                _ => self.duplicate_reply(),
            }
        } else if self.config.stale_redelivery && self.rng.chance(10) {
            if self.rng.chance(30) { self.break_session()?; } else { self.deliver_stale()?; }
        } else {
            match self.rng.below(1000) {
                0..330 => self.deliver_request()?,
                330..610 => self.deliver_reply()?,
                610..750 => self.sync_server()?,
                750..850 => self.submit()?,
                850..920 => self.heartbeat()?,
                920..950 => self.claim()?,
                950..985 => { let jump = self.rng.range(2, 20); self.now += jump; }
                985..995 => self.checkpoint()?,
                _ => self.park()?,
            }
        }
        self.expire_blocked_sessions()?;
        self.tick_writers()
    }

    // ---- processes and the directory ----

    fn live(&self) -> Vec<usize> { (0..MEMBERS).filter(|&slot| self.slots[slot].process.is_some()).collect() }

    fn pick_live(&mut self) -> Option<usize> {
        let live = self.live();
        (!live.is_empty()).then(|| live[self.rng.index(live.len())])
    }

    fn restart_due(&mut self) -> Step {
        self.restart_servers()?;
        for slot in 0..MEMBERS {
            if self.slots[slot].process.is_some() || self.now < self.slots[slot].restart_at { continue; }
            let generation = self.slots[slot].generation;
            self.slots[slot].generation += 1;
            let member = MemberId(format!("m{slot}.{generation}"));
            let record = MemberRecord { id: member.clone(), role: MemberRole::Node, address: String::new(), control_address: None, availability_zone: format!("az{slot}"),
                engine_version: "0".into(), lease_expires_at_ms: self.now + LEASE, state: MemberState::Eligible, drain_target_engine: None, pool: MemberPool::General, storage_namespaces: None, decommission: None, eligibility_epoch: 0, version: 0 };
            block_on(self.directory.register_member(record)).map_err(|error| error.to_string())?;
            let skew = self.rng.range(-CLOCK_SKEW, CLOCK_SKEW);
            self.slots[slot].process = Some(Process { id: self.next_process, member: member.clone(), writer: None, last_time: i64::MIN, skew });
            self.next_process += 1;
            self.note(format!("{member} starts as process {}", self.next_process - 1));
        }
        Ok(())
    }

    fn members(&self) -> BTreeMap<MemberId, MemberRecord> {
        block_on(self.directory.members()).expect("members").into_iter().map(|member| (member.id.clone(), member)).collect()
    }

    fn record(&self) -> DeploymentRecord { block_on(self.directory.deployment(&self.deployment)).expect("read").expect("the deployment exists") }

    fn heartbeat(&mut self) -> Step {
        let Some(slot) = self.pick_live() else { return Ok(()) };
        if self.slots[slot].cut_from_directory > self.now { return Ok(()); }
        let member = self.slots[slot].process.as_ref().unwrap().member.clone();
        let current = block_on(self.directory.member(&member)).expect("read").expect("registered");
        if current.state != MemberState::Eligible {
            self.note(format!("{member} learns it is {:?} and exits", current.state));
            return self.crash(slot);
        }
        let mut renewed = current.clone();
        renewed.lease_expires_at_ms = self.now + LEASE;
        block_on(self.directory.replace_member(renewed, current.version)).expect("renew");
        Ok(())
    }

    /// `ActivateOnDemand` / `ReclaimOnDemand`: a member that holds nothing tries to claim the deployment.
    fn claim(&mut self) -> Step {
        let Some(slot) = self.pick_live() else { return Ok(()) };
        if self.slots[slot].cut_from_directory > self.now { return Ok(()); }
        let process = self.slots[slot].process.as_ref().unwrap();
        if process.writer.as_ref().is_some_and(|writer| !writer.ended()) { return Ok(()); }
        let member = process.member.clone();
        let record = self.record();
        let Some(next) = claim(&record, &member, &self.members(), self.now) else { return Ok(()) };
        match block_on(self.directory.replace_deployment(next, record.version)) {
            ReplaceOutcome::Applied(stored) => {
                self.checker.claimed(stored.epoch, member.as_str())?;
                self.summary.claims += 1;
                self.note(format!("{member} claims epoch {}", stored.epoch));
                let bug = self.config.bug;
                self.with_writer(slot, |writer, io| { *writer = Some(Writer::open(stored.epoch, bug, io)); })?;
            }
            ReplaceOutcome::Conflict(_) => self.note(format!("{member} loses a claim race")),
            ReplaceOutcome::Refused(error) | ReplaceOutcome::Unknown(error) => panic!("claim: {error}"),
        }
        Ok(())
    }

    /// `Park`, once everything the holder enqueued has committed.
    fn park(&mut self) -> Step {
        let Some(slot) = self.pick_live() else { return Ok(()) };
        if self.slots[slot].cut_from_directory > self.now { return Ok(()); }
        let process = self.slots[slot].process.as_ref().unwrap();
        let Some(writer) = process.writer.as_ref().filter(|writer| writer.caught_up()) else { return Ok(()) };
        let (member, epoch) = (process.member.clone(), writer.epoch);
        let record = self.record();
        let Some(next) = park(&record, &member, epoch) else { return Ok(()) };
        if let ReplaceOutcome::Applied(stored) = block_on(self.directory.replace_deployment(next, record.version)) {
            self.note(format!("{member} parks epoch {epoch}; the record is at epoch {}", stored.epoch));
            self.slots[slot].process.as_mut().unwrap().writer = None;
        }
        Ok(())
    }

    /// `PublishCheckpoint`: the holder publishes a committed lsn, conditional on still holding, then trims the log through it.
    fn checkpoint(&mut self) -> Step {
        let Some(slot) = self.pick_live() else { return Ok(()) };
        if self.slots[slot].cut_from_directory > self.now { return Ok(()); }
        let process = self.slots[slot].process.as_ref().unwrap();
        let Some((epoch, commit)) = process.writer.as_ref().and_then(|writer| writer.commit().map(|commit| (writer.epoch, commit))) else { return Ok(()) };
        if commit <= self.checker.checkpoint { return Ok(()); }
        let record = self.record();
        if record.epoch != epoch || record.holder.as_ref() != Some(&process.member) { return Ok(()); }
        let lsn = self.rng.range(self.checker.checkpoint + 1, commit);
        self.checker.checkpointed(lsn)?;
        self.note(format!("epoch {epoch} checkpoints through {lsn} and trims"));
        self.with_writer(slot, |writer, io| { if let Some(writer) = writer { writer.trim(io, lsn); } })
    }

    fn submit(&mut self) -> Step {
        let serving: Vec<usize> = (0..MEMBERS).filter(|&slot| self.slots[slot].process.as_ref().is_some_and(|process| process.writer.as_ref().is_some_and(Writer::serving))).collect();
        if serving.is_empty() { return Ok(()); }
        let slot = serving[self.rng.index(serving.len())];
        let payload = self.next_payload;
        self.next_payload += 1;
        let now = self.now;
        let process = self.slots[slot].process.as_mut().unwrap();
        // `ResolveAndAppend`: system time rises past the last resolved, whatever this member's clock says.
        let time = (now + process.skew).max(process.last_time.saturating_add(1));
        process.last_time = time;
        let mut lsn = None;
        self.with_writer(slot, |writer, io| { lsn = writer.as_mut().and_then(|writer| writer.enqueue(io, Bytes::copy_from_slice(&payload.to_be_bytes()), time)); })?;
        if let Some(lsn) = lsn { self.note(format!("slot {slot} enqueues #{payload} at lsn {lsn}")); }
        Ok(())
    }

    // ---- faults ----

    fn crash(&mut self, slot: usize) -> Step {
        let Some(process) = self.slots[slot].process.take() else { return Ok(()) };
        self.slots[slot].restart_at = self.now + self.rng.range(20, 200);
        let keys: Vec<(usize, usize)> = self.sessions.keys().filter(|(owner, _)| *owner == process.id).copied().collect();
        for key in keys {
            let session = self.sessions.remove(&key).unwrap();
            self.strand(key, session);
        }
        Ok(())
    }

    /// A broken connection's unread requests: each may still be read and decided by its server, if stale redelivery is simulated.
    fn strand(&mut self, (process, server): (usize, usize), session: Session) {
        if !self.config.stale_redelivery { return; }
        for (_, body) in session.requests {
            if self.rng.chance(50) { self.stale.push(Stale { process, server, body }); }
        }
    }

    /// A live member, more often one with a writer: faults aimed at the writer are the ones that exercise recovery.
    fn pick_target(&mut self) -> Option<usize> {
        let writing: Vec<usize> = self.live().into_iter().filter(|&slot| self.slots[slot].process.as_ref().is_some_and(|process| process.writer.as_ref().is_some_and(|writer| !writer.ended()))).collect();
        if !writing.is_empty() && self.rng.chance(70) { return Some(writing[self.rng.index(writing.len())]); }
        self.pick_live()
    }

    fn crash_member(&mut self) -> Step {
        let Some(slot) = self.pick_target() else { return Ok(()) };
        let member = self.slots[slot].process.as_ref().unwrap().member.clone();
        self.fault(format!("{member} crashes"));
        self.crash(slot)
    }

    fn crash_server(&mut self) -> Step {
        let server = self.rng.index(SERVERS);
        if !self.servers[server].up { return Ok(()); }
        let kept = self.rng.range(0, self.servers[server].pending_events() as i64) as usize;
        self.fault(format!("server {server} crashes keeping {kept} of {} unsynced events", self.servers[server].pending_events()));
        self.servers[server].crash(kept);
        self.server_restart_at[server] = self.now + self.rng.range(5, 150);
        self.replies.retain(|reply| reply.server != server);
        self.stale.retain(|stale| stale.server != server);
        let keys: Vec<(usize, usize)> = self.sessions.keys().filter(|(_, to)| *to == server).copied().collect();
        for key in keys { self.fail_session(key, false)?; }
        self.check_server(server, None)
    }

    fn partition(&mut self) -> Step {
        let Some(slot) = self.pick_target() else { return Ok(()) };
        let target = self.rng.index(SERVERS + 1);
        let until = self.now + self.rng.range(10, 400);
        let cut = if target == SERVERS { &mut self.slots[slot].cut_from_directory } else { &mut self.slots[slot].cut_from_server[target] };
        *cut = until;
        let what = if target == SERVERS { "the directory".to_owned() } else { format!("server {target}") };
        self.fault(format!("slot {slot} is cut from {what} until t={until}"));
        Ok(())
    }

    fn break_session(&mut self) -> Step {
        if self.sessions.is_empty() { return Ok(()); }
        let key = *self.sessions.keys().nth(self.rng.index(self.sessions.len())).unwrap();
        self.fault(format!("connection of process {} to server {} breaks", key.0, key.1));
        self.fail_session(key, true)
    }

    /// `ConfirmSuspicion` on a wrong verdict: a live member is declared dead.
    fn suspect(&mut self) -> Step {
        let Some(slot) = self.pick_target() else { return Ok(()) };
        let member = self.slots[slot].process.as_ref().unwrap().member.clone();
        let current = block_on(self.directory.member(&member)).expect("read").expect("registered");
        if current.state != MemberState::Eligible { return Ok(()); }
        let mut dead = current.clone();
        dead.state = MemberState::Dead;
        dead.follow(&current);
        block_on(self.directory.replace_member(dead, current.version)).expect("suspect");
        self.fault(format!("{member} is wrongly confirmed dead"));
        Ok(())
    }

    /// A reply arriving twice; the connection matches replies to requests by id, so the second goes nowhere.
    fn duplicate_reply(&mut self) {
        if self.replies.is_empty() { return; }
        let index = self.rng.index(self.replies.len());
        let reply = &self.replies[index];
        let copy = InFlight { to: reply.to, server: reply.server, body: reply.body.clone(), snapshot: reply.snapshot.clone() };
        self.replies.push(copy);
        self.summary.faults += 1;
    }

    // ---- the network ----

    fn slot_of(&self, process: usize) -> Option<usize> {
        (0..MEMBERS).find(|&slot| self.slots[slot].process.as_ref().is_some_and(|live| live.id == process))
    }

    fn reachable(&self, process: usize, server: usize) -> bool {
        self.servers[server].up && self.slot_of(process).is_some_and(|slot| self.slots[slot].cut_from_server[server] <= self.now)
    }

    fn fail_session(&mut self, key: (usize, usize), strand: bool) -> Step {
        let Some(session) = self.sessions.remove(&key) else { return Ok(()) };
        let outstanding: Vec<u64> = session.outstanding.iter().copied().collect();
        if strand { self.strand(key, session); }
        self.replies.retain(|reply| !(reply.to.process == key.0 && reply.server == key.1));
        let Some(slot) = self.slot_of(key.0) else { return Ok(()) };
        for id in outstanding {
            self.with_writer(slot, |writer, io| { if let Some(writer) = writer { writer.reply(io, id, Err("the connection failed".into())); } })?;
        }
        Ok(())
    }

    fn expire_blocked_sessions(&mut self) -> Step {
        let keys: Vec<(usize, usize)> = self.sessions.keys().copied().collect();
        for key in keys {
            let reachable = self.reachable(key.0, key.1);
            let now = self.now;
            let Some(session) = self.sessions.get_mut(&key) else { continue };
            if reachable { session.blocked_since = None; continue; }
            let since = *session.blocked_since.get_or_insert(now);
            if now - since > CONNECT_TIMEOUT { self.fail_session(key, true)?; }
        }
        Ok(())
    }

    fn deliver_request(&mut self) -> Step {
        let ready: Vec<(usize, usize)> = self.sessions.iter().filter(|(key, session)| !session.requests.is_empty() && self.reachable(key.0, key.1)).map(|(key, _)| *key).collect();
        if ready.is_empty() { return Ok(()); }
        let key = ready[self.rng.index(ready.len())];
        let session = self.sessions.get_mut(&key).unwrap();
        let (id, body) = session.requests.pop_front().unwrap();
        let to = Return { process: key.0, session: session.id, id };
        self.decide(key.1, to, body, false)
    }

    fn deliver_stale(&mut self) -> Step {
        let ready: Vec<usize> = (0..self.stale.len()).filter(|&index| self.servers[self.stale[index].server].up).collect();
        if ready.is_empty() { return Ok(()); }
        let stale = self.stale.remove(ready[self.rng.index(ready.len())]);
        self.summary.faults += 1;
        self.decide(stale.server, Return { process: stale.process, session: 0, id: 0 }, stale.body, true)
    }

    fn decide(&mut self, server: usize, to: Return, body: Body, stale: bool) -> Step {
        let description = body.describe();
        let outcome = self.servers[server].decide(body, to, self.config.bug);
        let reply = match &outcome { Outcome::Now(reply) => format!(" -> {}", describe_reply(reply)), Outcome::Durable(_) => " (pending durability)".into() };
        self.note(format!("server {server} decides {}{description} from process {}{reply}", if stale { "stale " } else { "" }, to.process));
        match outcome {
            Outcome::Now(body) => if !stale { self.replies.push(InFlight { to, server, body, snapshot: None }); },
            Outcome::Durable(changed) => self.check_server(server, changed)?,
        }
        Ok(())
    }

    fn check_server(&self, server: usize, changed: Option<i64>) -> Step {
        let state = &self.servers[server].accepted;
        self.checker.server(server, state, changed.unwrap_or(i64::MIN))
    }

    fn sync_server(&mut self) -> Step {
        let ready: Vec<usize> = (0..SERVERS).filter(|&server| self.servers[server].up && self.servers[server].has_pending()).collect();
        if ready.is_empty() { return Ok(()); }
        let server = ready[self.rng.index(ready.len())];
        let synced = self.servers[server].sync();
        self.note(format!("server {server} syncs {} decisions", synced.replies.len()));
        for (process, epoch) in synced.writers { self.checker.accepted(epoch, process)?; }
        for (to, body, snapshot) in synced.replies {
            if to.session != 0 { self.replies.push(InFlight { to, server, body, snapshot }); }
        }
        Ok(())
    }

    fn deliver_reply(&mut self) -> Step {
        let ready: Vec<usize> = (0..self.replies.len()).filter(|&index| self.reachable(self.replies[index].to.process, self.replies[index].server)).collect();
        if ready.is_empty() { return Ok(()); }
        let reply = self.replies.swap_remove(ready[self.rng.index(ready.len())]);
        let key = (reply.to.process, reply.server);
        let Some(session) = self.sessions.get_mut(&key).filter(|session| session.id == reply.to.session) else { return Ok(()) };
        if !session.outstanding.remove(&reply.to.id) { return Ok(()); }
        let Some(slot) = self.slot_of(reply.to.process) else { return Ok(()) };
        self.note(format!("process {} receives {} from server {}", reply.to.process, describe_reply(&reply.body), reply.server));
        let answer: Reply = Ok((reply.body, reply.snapshot));
        self.with_writer(slot, |writer, io| { if let Some(writer) = writer { writer.reply(io, reply.to.id, answer); } })
    }

    fn tick_writers(&mut self) -> Step {
        for slot in self.live() {
            self.with_writer(slot, |writer, io| { if let Some(writer) = writer { writer.tick(io); } })?;
        }
        Ok(())
    }

    /// Runs `act` on the slot's writer, sends what it asked to send, and checks what it reports.
    fn with_writer(&mut self, slot: usize, act: impl FnOnce(&mut Option<Writer>, &mut Io)) -> Step {
        let mut io = Io { now: self.now, next_id: self.next_id, sends: Vec::new(), notes: Vec::new() };
        let Some(process) = self.slots[slot].process.as_mut() else { return Ok(()) };
        act(&mut process.writer, &mut io);
        self.next_id = io.next_id;
        let process_id = process.id;
        let epoch = process.writer.as_ref().map_or(0, |writer| writer.epoch);
        for (server, id, body) in io.sends {
            let next_session = &mut self.next_session;
            let session = self.sessions.entry((process_id, server)).or_insert_with(|| {
                *next_session += 1;
                Session { id: *next_session, requests: VecDeque::new(), outstanding: BTreeSet::new(), blocked_since: None }
            });
            session.requests.push_back((id, body));
            session.outstanding.insert(id);
        }
        for note in io.notes {
            match note {
                Note::SealQuorum(sealed) => {
                    let servers: Vec<usize> = sealed.iter().map(|(server, ..)| *server).collect();
                    self.note(format!("epoch {epoch} has a seal quorum {servers:?}"));
                    self.checker.seal_quorum(epoch, sealed.into_iter().map(|(server, trim, held)| Snapshot { server, trim, entries: held.into_iter().map(|entry| (entry.lsn, entry)).collect() }).collect());
                }
                Note::Opened { history, trim, last, synced } => {
                    self.note(format!("epoch {epoch} opens on {synced:?} with history ({trim}, {last}]"));
                    self.summary.opened += 1;
                    self.checker.opened(epoch, &history, trim, last)?;
                    let recovered = history.iter().map(|entry| entry.timestamp).max().unwrap_or(i64::MIN);
                    let process = self.slots[slot].process.as_mut().unwrap();
                    process.last_time = process.last_time.max(recovered);
                }
                Note::Committed { entries, acknowledged } => {
                    self.summary.acknowledged += acknowledged.len();
                    self.note(format!("epoch {epoch} commits through {}", entries.last().map_or(-1, |entry| entry.lsn)));
                    self.checker.pipeline_committed(epoch, &entries)?;
                }
                Note::Ended(fail) => self.note(format!("epoch {epoch} ends: {fail:?}")),
            }
        }
        Ok(())
    }

    fn restart_servers(&mut self) -> Step {
        for server in 0..SERVERS {
            if !self.servers[server].up && self.now >= self.server_restart_at[server] {
                self.servers[server].up = true;
                self.note(format!("server {server} restarts holding {} entries", held(&self.servers[server].durable).len()));
            }
        }
        Ok(())
    }
}

fn describe_reply(body: &ResponseBody) -> String {
    match body {
        ResponseBody::Accepted(accepted) => format!("accepted (last {}, commit {})", accepted.last_lsn, accepted.commit_lsn),
        ResponseBody::Refused(refused) => format!("refused (promised {}, last {})", refused.promised, refused.last_lsn),
        ResponseBody::Sealed(sealed) => format!("sealed (promised {}, synced {}, trim {}, last {}, commit {})", sealed.promised, sealed.synced, sealed.trim_lsn, sealed.last_lsn, sealed.commit_lsn),
        ResponseBody::Entries(entries) => format!("entries {:?} (trim {}, last {})", entries.entries.iter().map(|entry| entry.lsn).collect::<Vec<_>>(), entries.trim_lsn, entries.last_lsn),
        ResponseBody::Failed(failed) => format!("failed: {}", failed.message),
        ResponseBody::PageDigest(page) => format!("digest through {}", page.through_lsn),
        ResponseBody::JournalIdentity(identity) => format!("journal {}", identity.journal),
        ResponseBody::ReadIndex(_) => "read index".into(),
        ResponseBody::DecisionVote(_) => "decision vote".into(),
        ResponseBody::DecisionInstallation(_) => "decision installation".into(),
    }
}
