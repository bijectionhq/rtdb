//! A deterministic simulation of gateway sessions over backends that fail: the real `session::Session` splices a client to stub backends over loopback TCP, through a `Router` that hands out a new backend on each reconnection.
//!
//! The backends are stubs that speak just enough pgwire (startup, `SET rtdb_idempotency_key`, simple queries and a named prepared read) over one shared deployment state, with the node's deduplication: a command's transactions are keyed `key/0`, `key/1`, ..., and a key already committed commits nothing and is answered as the original.
//! At seeded moments a backend dies before executing a statement, after executing it and before replying, between replies, during its greeting or while idle, or refuses with the not-held error before executing.
//!
//! Every action is sequenced: the client sends a command only once the previous one is answered, a backend acts only on the frame it was sent, and a backend that dies half-closes and drains, so no read races a reset.
//! Each backend draws from its own generator, seeded by the run's seed and the order it was connected in, so the schedule is a function of the seed although the client, the gateway and the backends are separate tasks.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use rtdb_pgwire::{GATEWAY_KEY_CONFIRM_KIND, GATEWAY_REPLAY_PERMIT_KIND, GATEWAY_SESSION_STATE_TAG, GatewayReplayMode, GatewayReplayPermit};
use rtdb_directory::{DeploymentRecord, Directory, MemoryDirectory};
use rtdb_multi::session::{Frame, Router, Session};
use rtdb_node::NOT_HELD;
use rtdb_types::{Anomaly, AnomalyCategory};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::Violation;
use crate::rng::Rng;

const TRACE_LENGTH: usize = 200;
/// A guard against a hang, never reached by a run that behaves.
const STALL: Duration = Duration::from_secs(60);

/// A deliberately broken variant, to show the checker finds what it is meant to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bug {
    /// A backend commits a transaction whose key already committed, as if it were new.
    NodeIgnoresKeys,
    /// A backend answers a transaction whose key already committed with an empty result, rather than as the original.
    RepeatAnsweredDifferently,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub seed: u64,
    /// Commands the client issues.
    pub steps: usize,
    pub bug: Option<Bug>,
}

impl Config {
    pub fn new(seed: u64, steps: usize) -> Self { Self { seed, steps, bug: None } }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Summary {
    pub seed: u64,
    pub steps: usize,
    pub sessions: usize,
    pub backends: usize,
    pub succeeded: usize,
    /// Commands the client was told have an unknown outcome.
    pub unknown: usize,
    /// Commands the client was told failed.
    pub failed: usize,
    /// Backends that died or refused.
    pub faults: usize,
    pub greeting_faults: usize,
    pub prepared_reads: usize,
    /// Keys sent to more than one backend: commands the gateway resent.
    pub resent: usize,
}

/// What every backend shares: the deployment's state, and the run's record.
struct Shared {
    seed: u64,
    bug: Option<Bug>,
    /// How many committed transactions each command's writes are in.
    executions: BTreeMap<u64, u32>,
    committed: BTreeSet<String>,
    /// Each session key the backends were sent, in the order first sent, and how many backends it reached.
    keys: BTreeMap<String, (usize, usize)>,
    connections: u64,
    /// Greetings backends have sent.
    greeted: u64,
    /// Whether the backend that sent the last ReadyForQuery to a command dies after it, mid-transaction or not, and after how many greetings: the client waits for the gateway to tell it, or to reconnect.
    /// The last such backend is the one the gateway passed the client's answer from: a backend the gateway gave up on sent its own earlier, and no backend sends one after it until the client's next command.
    idle_death: Option<(bool, u64)>,
    trace: VecDeque<String>,
    faults: usize,
    greeting_faults: usize,
}

type World = Arc<Mutex<Shared>>;

fn note(world: &World, event: String) {
    let mut shared = world.lock().expect("unpoisoned");
    if shared.trace.len() == TRACE_LENGTH { shared.trace.pop_front(); }
    shared.trace.push_back(event);
}

fn mix(seed: u64, index: u64) -> u64 { seed ^ index.wrapping_add(1).wrapping_mul(0xA24B_AED4_963E_E407) }

fn frame(tag: u8, body: &[u8]) -> Frame { Frame { tag, body: Bytes::copy_from_slice(body) } }

fn text(text: &str) -> Frame {
    let mut body = text.as_bytes().to_vec();
    body.push(0);
    frame(b'C', &body)
}

fn ready(status: u8) -> Frame { frame(b'Z', &[status]) }

fn same(left: &Frame, right: &Frame) -> bool { left.tag == right.tag && left.body == right.body }

fn show(frame: &Frame) -> String { format!("{}:{}", frame.tag as char, String::from_utf8_lossy(&frame.body).replace('\0', "|")) }

fn not_held() -> Frame {
    let mut body = BytesMut::new();
    for (tag, value) in [(b'S', "ERROR"), (b'V', "ERROR"), (b'C', "57P03"), (b'M', &*format!("{NOT_HELD}: this node no longer holds shop"))] {
        body.put_u8(tag);
        body.extend_from_slice(value.as_bytes());
        body.put_u8(0);
    }
    body.put_u8(0);
    Frame { tag: b'E', body: body.freeze() }
}

fn contains(frame: &Frame, needle: &str) -> bool { frame.body.windows(needle.len()).any(|window| window == needle.as_bytes()) }

async fn read_frame(stream: &mut TcpStream) -> Option<Frame> {
    let tag = stream.read_u8().await.ok()?;
    let length = usize::try_from(stream.read_i32().await.ok()?).ok()?.checked_sub(4)?;
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.ok()?;
    Some(Frame { tag, body: Bytes::from(body) })
}

/// A connection over loopback, whose ends reset rather than linger when closed: a sweep opens tens of thousands, more than the ports a lingering close leaves free.
/// The far end of a client's connection is the gateway's, which closes after its last word to the client and so must not reset it away.
async fn pair(listener: &TcpListener, far_resets: bool) -> std::io::Result<(TcpStream, TcpStream)> {
    let address = listener.local_addr()?;
    let (near, (far, _)) = tokio::try_join!(TcpStream::connect(address), listener.accept())?;
    near.set_nodelay(true)?;
    far.set_nodelay(true)?;
    near.set_zero_linger()?;
    if far_resets { far.set_zero_linger()?; }
    Ok((near, far))
}

const UPDATE: &str = "UPDATE counters SET n = n + 1 WHERE id = ";
const SELECT: &str = "SELECT n FROM counters WHERE id = ";
const PREPARE: &[u8] = b"sim_read\0SELECT n FROM counters WHERE id = $1\0\0\0";
const BIND_PREFIX: &[u8] = b"\0sim_read\0\0\x01\0\x01\0\x01\0\0\0\x08";
const SYNC: u8 = b'S';

/// One backend's session: its key and transaction, as a node's pgwire session keeps them.
struct Backend {
    index: u64,
    world: World,
    rng: Rng,
    key: Option<(String, u64)>,
    transaction: Option<Vec<u64>>,
    failed: bool,
    holder: bool,
    initial: bool,
}

/// How a backend fails the command it was just sent: at which of its events (executing statement i is event 2i, replying to it 2i + 1, ReadyForQuery the last), by refusing statement i, and perhaps dying before the ReadyForQuery that follows, or by crashing after executing statement `executed` with only the replies before statement `delivered` reaching the gateway, the rest lost in flight.
enum Fault { None { idle_death: bool }, DieAt(usize), Refuse { at: usize, dies: bool }, Crash { executed: usize, delivered: usize } }

impl Backend {
    fn status(&self) -> u8 { if self.failed { b'E' } else if self.transaction.is_some() { b'T' } else { b'I' } }

    fn note(&self, event: String) { note(&self.world, format!("b{} {event}", self.index)); }

    /// Commits a transaction holding `ids`' writes under the session key's next sequence, unless that key committed already.
    /// Returns whether its reply is to be empty, which only the broken variant answers a repeat with.
    fn commit(&mut self, ids: &[u64]) -> bool {
        let key = self.key.as_mut().map(|(key, sequence)| { *sequence += 1; format!("{key}/{}", *sequence - 1) });
        let mut shared = self.world.lock().expect("unpoisoned");
        // Keys are minted at random; the trace names them by the order they were first sent in.
        let shown = match &self.key { Some((key, sequence)) => format!("k{}/{}", shared.keys.get(key).map_or(0, |(ordinal, _)| *ordinal), sequence - 1), None => "no key".to_owned() };
        let repeated = key.as_ref().is_some_and(|key| shared.committed.contains(key)) && shared.bug != Some(Bug::NodeIgnoresKeys);
        if !repeated {
            for id in ids.iter().collect::<BTreeSet<_>>() { *shared.executions.entry(*id).or_default() += 1; }
            if let Some(key) = &key { shared.committed.insert(key.clone()); }
        }
        drop(shared);
        self.note(format!("{} {ids:?} under {shown}", if repeated { "answers a repeat of" } else { "commits" }));
        repeated && self.world.lock().expect("unpoisoned").bug == Some(Bug::RepeatAnsweredDifferently)
    }

    fn execute(&mut self, statement: &str) -> Vec<Frame> {
        if statement == "BEGIN" { self.transaction = Some(Vec::new()); return vec![text("BEGIN")]; }
        if statement == "ROLLBACK" { self.transaction = None; self.failed = false; return vec![text("ROLLBACK")]; }
        if statement == "COMMIT" {
            let ids = self.transaction.take().unwrap_or_default();
            let tag = if self.commit(&ids) { "COMMIT".to_owned() } else { format!("COMMIT #{ids:?}") };
            return vec![text(&tag)];
        }
        if let Some(id) = statement.strip_prefix(UPDATE).and_then(|id| id.parse::<u64>().ok()) {
            let empty = match &mut self.transaction { Some(writes) => { writes.push(id); false } None => self.commit(&[id]) };
            return vec![text(&if empty { "UPDATE 0".to_owned() } else { format!("UPDATE 1 #{id}") })];
        }
        if let Some(id) = statement.strip_prefix(SELECT).and_then(|id| id.parse::<u64>().ok()) {
            return vec![frame(b'T', b"n"), frame(b'D', format!("{id}").as_bytes()), text(&format!("SELECT 1 #{id}"))];
        }
        vec![text("UNKNOWN")]
    }

    async fn send(stream: &mut TcpStream, frames: &[Frame]) -> bool {
        let mut bytes = BytesMut::new();
        for frame in frames { bytes.extend_from_slice(&frame.encode()); }
        stream.write_all(&bytes).await.is_ok()
    }

    /// Dies: half-closes, so the gateway reads every reply sent before the end, and drains what it still sends, so nothing is reset.
    async fn die(&self, stream: &mut TcpStream, when: &str) {
        self.note(format!("dies {when}"));
        self.world.lock().expect("unpoisoned").faults += 1;
        let _ = stream.shutdown().await;
        while read_frame(stream).await.is_some() {}
    }

    fn draw(&mut self, statements: usize) -> Fault {
        let roll = self.rng.below(100);
        match roll {
            0..8 => Fault::Refuse { at: self.rng.index(statements), dies: self.rng.chance(15) },
            8..26 => Fault::DieAt(self.rng.index(2 * statements + 1)),
            26..30 => { let executed = self.rng.index(statements); Fault::Crash { executed, delivered: self.rng.index(executed + 1) } }
            _ => Fault::None { idle_death: self.rng.chance(6) },
        }
    }

    async fn run(mut self, mut stream: TcpStream, reconnected: Arc<Notify>) {
        let Ok(length) = stream.read_i32().await else { return };
        let mut startup = vec![0; usize::try_from(length).unwrap_or(4).saturating_sub(4)];
        if stream.read_exact(&mut startup).await.is_err() { return; }
        if Rng::new(mix(self.world.lock().expect("unpoisoned").seed ^ 0x37, self.index)).chance(2) {
            self.world.lock().expect("unpoisoned").greeting_faults += 1;
            if !Self::send(&mut stream, &[frame(b'R', &[0, 0, 0, 0])]).await { return; }
            return self.die(&mut stream, "during its greeting").await;
        }
        self.note("greets".to_owned());
        if !Self::send(&mut stream, &[
            frame(b'R', &[0, 0, 0, 0]), frame(GATEWAY_SESSION_STATE_TAG, &[GATEWAY_KEY_CONFIRM_KIND]),
            frame(GATEWAY_SESSION_STATE_TAG, &[GATEWAY_REPLAY_PERMIT_KIND]), ready(b'I'),
        ]).await { return; }
        self.world.lock().expect("unpoisoned").greeted += 1;
        reconnected.notify_one();
        let mut prepared = false;
        let mut prepared_now = false;
        let mut bound = None;
        let mut replay_simple = false;
        loop {
            let Some(request) = read_frame(&mut stream).await else { return };
            match request.tag {
                b'X' => return,
                b'P' if request.body.as_ref() == PREPARE && !prepared => {
                    prepared = true;
                    prepared_now = true;
                    if !Self::send(&mut stream, &[frame(b'1', &[])]).await { return; }
                    continue;
                }
                b'B' if prepared => {
                    let arg = request.body.strip_prefix(BIND_PREFIX).and_then(|body| body.strip_suffix(&[0, 0]));
                    let Some(arg) = arg.and_then(|bytes| bytes.try_into().ok()) else { return };
                    bound = Some(u64::from_be_bytes(arg));
                    if !Self::send(&mut stream, &[frame(b'2', &[])]).await { return; }
                    continue;
                }
                b'E' => {
                    let Some(id) = bound.take() else { return };
                    if !Self::send(&mut stream, &[frame(b'D', format!("{id}").as_bytes()), text(&format!("SELECT 1 #{id}"))]).await { return; }
                    continue;
                }
                SYNC => {
                    let dies_idle = prepared_now && self.initial;
                    prepared_now = false;
                    if dies_idle {
                        let mut shared = self.world.lock().expect("unpoisoned");
                        shared.idle_death = Some((false, shared.greeted));
                    }
                    if !Self::send(&mut stream, &[ready(self.status())]).await { return; }
                    if dies_idle { return self.die(&mut stream, "after preparing a statement").await; }
                    continue;
                }
                GATEWAY_SESSION_STATE_TAG if matches!(request.body.first().copied(), Some(GATEWAY_KEY_CONFIRM_KIND | GATEWAY_REPLAY_PERMIT_KIND)) => {
                    let (key, acknowledge) = if let Some(permit) = GatewayReplayPermit::decode(&request.body) {
                        replay_simple = permit.mode == GatewayReplayMode::Replay;
                        (permit.nonce.simple().to_string(), permit.mode != GatewayReplayMode::Dispatch)
                    } else {
                        let Ok(key) = std::str::from_utf8(&request.body[1..]) else { return };
                        (key.to_owned(), true)
                    };
                    if self.rng.chance(4) { return self.die(&mut stream, "on being keyed").await; }
                    {
                        let mut shared = self.world.lock().expect("unpoisoned");
                        let ordinal = shared.keys.len();
                        shared.keys.entry(key.to_owned()).or_insert((ordinal, 0)).1 += 1;
                    }
                    if self.key.as_ref().is_none_or(|(current, _)| current != &key) { self.key = Some((key, 0)); }
                    if acknowledge && !Self::send(&mut stream, &[ready(self.status())]).await { return; }
                    continue;
                }
                b'Q' => {}
                _ => return,
            }
            let query = String::from_utf8_lossy(request.body.strip_suffix(b"\0").unwrap_or(&request.body)).into_owned();
            let statements: Vec<&str> = query.split("; ").collect();
            if std::mem::take(&mut replay_simple) && (statements.len() != 1 || !query.starts_with(UPDATE) && !query.starts_with(SELECT)) {
                let _ = Self::send(&mut stream, &[frame(b'E', b"SERROR\0C0A000\0MUnsupported replay command\0\0")]).await;
                return;
            }
            // A rollback ends the session's transaction on the node, whether it still holds the deployment or not.
            let fault = match self.draw(statements.len()) {
                Fault::Refuse { .. } | Fault::None { .. } if statements[0] == "ROLLBACK" => Fault::None { idle_death: false },
                _ if !self.holder && statements[0] != "ROLLBACK" => Fault::Refuse { at: 0, dies: false },
                drawn => drawn,
            };
            let mut event = 0;
            let mut refused = false;
            for (position, statement) in statements.iter().enumerate() {
                if let Fault::Refuse { at, dies } = fault && at == position {
                    self.holder = false;
                    if self.transaction.is_some() { self.failed = true; }
                    self.note(format!("refuses {statement:?} as no longer the holder"));
                    self.world.lock().expect("unpoisoned").faults += 1;
                    if !Self::send(&mut stream, &[not_held()]).await { return; }
                    if dies { return self.die(&mut stream, "having refused, before its ReadyForQuery").await; }
                    refused = true;
                    break;
                }
                if matches!(fault, Fault::DieAt(at) if at == event) { return self.die(&mut stream, &format!("before executing {statement:?}")).await; }
                event += 1;
                let replies = self.execute(statement);
                if matches!(fault, Fault::DieAt(at) if at == event) { return self.die(&mut stream, &format!("having executed {statement:?}, before replying")).await; }
                event += 1;
                if let Fault::Crash { executed, delivered } = fault {
                    if position < delivered && !Self::send(&mut stream, &replies).await { return; }
                    if position == executed { return self.die(&mut stream, &format!("having executed {statement:?}, its replies from statement {delivered} on lost")).await; }
                    continue;
                }
                if !Self::send(&mut stream, &replies).await { return; }
            }
            if !refused && matches!(fault, Fault::DieAt(at) if at == event) { return self.die(&mut stream, "before its ReadyForQuery").await; }
            let dies_idle = matches!(fault, Fault::None { idle_death: true }) && !refused;
            {
                let mut shared = self.world.lock().expect("unpoisoned");
                shared.idle_death = dies_idle.then(|| (self.transaction.is_some(), shared.greeted));
            }
            if !Self::send(&mut stream, &[ready(self.status())]).await { return; }
            if dies_idle { return self.die(&mut stream, "while idle").await; }
        }
    }
}

fn spawn_backend(world: &World, stream: TcpStream, reconnected: &Arc<Notify>, initial: bool) -> JoinHandle<()> {
    let (index, seed) = {
        let mut shared = world.lock().expect("unpoisoned");
        shared.connections += 1;
        (shared.connections - 1, shared.seed)
    };
    let backend = Backend { index, world: Arc::clone(world), rng: Rng::new(mix(seed, index)), key: None, transaction: None, failed: false, holder: true, initial };
    tokio::spawn(backend.run(stream, Arc::clone(reconnected)))
}

struct Backends { listener: TcpListener, world: World, reconnected: Arc<Notify>, tasks: Mutex<Vec<JoinHandle<()>>> }

#[async_trait::async_trait]
impl Router for Backends {
    async fn route(&self) -> rtdb_types::Result<(TcpStream, Option<String>, u64)> {
        let (near, far) = pair(&self.listener, true).await.map_err(|error| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.simulation/connect", error.to_string()))?;
        let task = spawn_backend(&self.world, far, &self.reconnected, false);
        self.tasks.lock().expect("unpoisoned").push(task);
        Ok((near, None, 0))
    }

    async fn route_holder(&self) -> rtdb_types::Result<(TcpStream, Option<String>, u64)> { self.route().await }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome { Succeeded, Unknown, Failed }

struct Client {
    config: Config,
    rng: Rng,
    world: World,
    backends: Arc<Backends>,
    clients: TcpListener,
    directory: Arc<MemoryDirectory>,
    deployment: String,
    session: Option<(TcpStream, JoinHandle<rtdb_types::Result<()>>)>,
    outcomes: BTreeMap<u64, Outcome>,
    /// Whether any reply to the current command has reached the client.
    answered: bool,
    prepared: bool,
    summary: Summary,
}

type Step = Result<(), String>;

impl Client {
    fn note(&self, event: String) { note(&self.world, format!("client {event}")); }

    fn startup() -> Bytes {
        let mut startup = 196_608i32.to_be_bytes().to_vec();
        startup.extend_from_slice(b"user\0xtdb\0database\0shop\0\0");
        let mut framed = ((startup.len() + 4) as i32).to_be_bytes().to_vec();
        framed.extend_from_slice(&startup);
        Bytes::from(framed)
    }

    async fn start_session(&mut self) -> Step {
        let keyed = self.rng.chance(90);
        loop {
            let (mut client, gateway) = pair(&self.clients, false).await.map_err(|error| error.to_string())?;
            let (first, far) = pair(&self.backends.listener, true).await.map_err(|error| error.to_string())?;
            self.note(format!("opens a session, {}", if keyed { "keyed" } else { "unkeyed" }));
            let task = spawn_backend(&self.world, far, &self.backends.reconnected, true);
            self.backends.tasks.lock().expect("unpoisoned").push(task);
            let directory: Arc<dyn Directory> = self.directory.clone();
            let router: Arc<dyn Router> = self.backends.clone();
            let session = Session::new(Self::startup(), router, directory, self.deployment.clone(), None, keyed);
            let running = tokio::spawn(session.run(gateway, first, None, 0));
            let greeting = read_frame(&mut client).await;
            if !greeting.as_ref().is_some_and(|greeting| same(greeting, &frame(b'R', &[0, 0, 0, 0]))) {
                return Err(format!("unexpected authentication reply {:?}", greeting.as_ref().map(show)));
            }
            match read_frame(&mut client).await {
                Some(greeting) if same(&greeting, &ready(b'I')) => {
                    self.session = Some((client, running));
                    self.summary.sessions += 1;
                    break;
                }
                Some(error) if error.tag == b'E' && contains(&error, "closed during session startup") => {
                    drop(client);
                    let _ = running.await;
                }
                other => return Err(format!("unexpected startup reply {:?}", other.as_ref().map(show))),
            }
        }
        self.prepared = self.rng.chance(25);
        if self.prepared {
            let outcome = self.exchange_frames(0, "prepare", &[frame(b'P', PREPARE), frame(SYNC, &[])], &[frame(b'1', &[]), ready(b'I')]).await?;
            if outcome.is_some() { return Err("preparing a statement failed".into()); }
        }
        Ok(())
    }

    async fn exchange(&mut self, id: u64, query: &str, expected: &[Frame]) -> Result<Option<Outcome>, String> {
        let mut body = query.as_bytes().to_vec();
        body.push(0);
        self.exchange_frames(id, query, &[frame(b'Q', &body)], expected).await
    }

    async fn exchange_frames(&mut self, id: u64, query: &str, requests: &[Frame], expected: &[Frame]) -> Result<Option<Outcome>, String> {
        let (stream, _) = self.session.as_mut().expect("a session is open");
        let mut bytes = BytesMut::new();
        for request in requests { bytes.extend_from_slice(&request.encode()); }
        if stream.write_all(&bytes).await.is_err() { return Err(format!("c{id}: the gateway stopped reading")); }
        let mut position = 0;
        loop {
            let Some(reply) = read_frame(stream).await else { return Err(format!("c{id}: the session closed without telling the client what became of {query:?}")) };
            if reply.tag == b'E' {
                if contains(&reply, "08006") {
                    if !contains(&reply, "outcome is unknown") { return Err(format!("c{id}: the session ended with {}", show(&reply))); }
                    return Ok(Some(Outcome::Unknown));
                }
                if !contains(&reply, NOT_HELD) { return Err(format!("c{id}: an unexpected error {}", show(&reply))); }
                if !self.answered { return Err(format!("c{id}: a refusal before any reply reached the client, which the gateway should have resent")); }
                let status = match read_frame(stream).await {
                    Some(status) if status.tag == b'Z' => status,
                    Some(lost) if lost.tag == b'E' && contains(&lost, "08006") && contains(&lost, "outcome is unknown") => return Ok(Some(Outcome::Unknown)),
                    other => return Err(format!("c{id}: {:?} after a refusal, not a ReadyForQuery", other.as_ref().map(show))),
                };
                if status.body.as_ref() == b"E" {
                    let mut rollback = b"ROLLBACK".to_vec();
                    rollback.push(0);
                    if stream.write_all(&frame(b'Q', &rollback).encode()).await.is_err() { return Err(format!("c{id}: the gateway stopped reading")); }
                    for expected in [text("ROLLBACK"), ready(b'I')] {
                        match read_frame(stream).await {
                            Some(reply) if same(&reply, &expected) => {}
                            Some(reply) if reply.tag == b'E' && contains(&reply, "08006") && contains(&reply, "outcome is unknown") => return Ok(Some(Outcome::Unknown)),
                            other => return Err(format!("c{id}: rolling back got {:?}, not {}", other.as_ref().map(show), show(&expected))),
                        }
                    }
                }
                return Ok(Some(Outcome::Failed));
            }
            let Some(wanted) = expected.get(position) else { return Err(format!("c{id}: {} after its command's replies were complete", show(&reply))) };
            if !same(&reply, wanted) { return Err(format!("c{id}: the client saw {} where its command's reply is {}", show(&reply), show(wanted))); }
            position += 1;
            self.answered = true;
            if reply.tag == b'Z' { break; }
        }
        let dying = self.world.lock().expect("unpoisoned").idle_death.take();
        match dying {
            // The holder went away mid-transaction: the gateway ends the session, and says so.
            Some((true, _)) => {
                let (stream, _) = self.session.as_mut().expect("a session is open");
                match read_frame(stream).await {
                    Some(reply) if reply.tag == b'E' && contains(&reply, "outcome is unknown") => Ok(Some(Outcome::Unknown)),
                    other => Err(format!("c{id}: after its holder died mid-transaction the client got {:?}", other.as_ref().map(show))),
                }
            }
            // The holder went away while the session was idle: the gateway reconnects before reading the next command.
            Some((false, greeted)) => {
                while self.world.lock().expect("unpoisoned").greeted <= greeted { self.backends.reconnected.notified().await; }
                Ok(None)
            }
            None => Ok(None),
        }
    }

    async fn command(&mut self, id: u64) -> Step {
        if self.session.is_none() { self.start_session().await?; }
        if self.prepared && id % 5 == 0 {
            let mut bind = BytesMut::from(BIND_PREFIX);
            bind.put_u64(id);
            bind.put_u16(0);
            let requests = [frame(b'B', &bind), frame(b'E', &[0; 5]), frame(SYNC, &[])];
            let expected = [frame(b'2', &[]), frame(b'D', format!("{id}").as_bytes()), text(&format!("SELECT 1 #{id}")), ready(b'I')];
            if self.exchange_frames(id, "execute prepared read", &requests, &expected).await?.is_some() {
                return Err(format!("c{id}: executing the prepared read failed"));
            }
            self.summary.prepared_reads += 1;
            self.summary.succeeded += 1;
            return self.check();
        }
        let update = format!("{UPDATE}{id}");
        let (kind, exchanges): (&str, Vec<(String, Vec<Frame>)>) = match self.rng.below(10) {
            0..4 => ("a write", vec![(update.clone(), vec![text(&format!("UPDATE 1 #{id}")), ready(b'I')])]),
            4..6 => ("a transaction in one query", vec![(format!("BEGIN; {update}; {update}; COMMIT"),
                vec![text("BEGIN"), text(&format!("UPDATE 1 #{id}")), text(&format!("UPDATE 1 #{id}")), text(&format!("COMMIT #{:?}", [id, id])), ready(b'I')])]),
            6..8 => ("a transaction in three queries", vec![
                ("BEGIN".to_owned(), vec![text("BEGIN"), ready(b'T')]),
                (update.clone(), vec![text(&format!("UPDATE 1 #{id}")), ready(b'T')]),
                ("COMMIT".to_owned(), vec![text(&format!("COMMIT #{:?}", [id])), ready(b'I')]),
            ]),
            _ => ("a read", vec![(format!("{SELECT}{id}"), vec![frame(b'T', b"n"), frame(b'D', format!("{id}").as_bytes()), text(&format!("SELECT 1 #{id}")), ready(b'I')])]),
        };
        let writes = kind != "a read";
        self.note(format!("sends c{id}, {kind}"));
        let mut outcome = Outcome::Succeeded;
        self.answered = false;
        for (query, expected) in &exchanges {
            if let Some(ended) = self.exchange(id, query, expected).await? { outcome = ended; break; }
        }
        let ran = self.world.lock().expect("unpoisoned").executions.get(&id).copied().unwrap_or(0);
        self.note(format!("is told c{id} {}; it ran {ran} times", match outcome { Outcome::Succeeded => "succeeded", Outcome::Unknown => "has an unknown outcome", Outcome::Failed => "failed" }));
        match outcome {
            Outcome::Succeeded => { self.summary.succeeded += 1; if ran != u32::from(writes) { return Err(format!("c{id} succeeded, and ran {ran} times")); } }
            Outcome::Failed => { self.summary.failed += 1; if ran != 0 { return Err(format!("c{id} failed, yet ran {ran} times")); } }
            Outcome::Unknown => {
                self.summary.unknown += 1;
                if ran > 1 { return Err(format!("c{id} has an unknown outcome, and ran {ran} times")); }
                let (client, running) = self.session.take().expect("a session is open");
                drop(client);
                let _ = running.await;
            }
        }
        self.outcomes.insert(id, outcome);
        self.check()
    }

    /// No command ran twice; what succeeded ran once, and what failed never.
    fn check(&self) -> Step {
        let shared = self.world.lock().expect("unpoisoned");
        if let Some((id, ran)) = shared.executions.iter().find(|(_, ran)| **ran > 1) { return Err(format!("c{id} ran {ran} times")); }
        for (id, outcome) in &self.outcomes {
            let ran = shared.executions.get(id).copied().unwrap_or(0);
            if *outcome == Outcome::Failed && ran > 0 { return Err(format!("c{id} failed, yet has run")); }
        }
        Ok(())
    }

    async fn run(mut self) -> Result<Summary, (usize, String)> {
        for id in 0..self.config.steps as u64 {
            self.command(id).await.map_err(|message| (id as usize, message))?;
        }
        if let Some((mut stream, running)) = self.session.take() {
            let _ = stream.write_all(&frame(b'X', &[]).encode()).await;
            drop(stream);
            let _ = running.await;
        }
        let tasks: Vec<JoinHandle<()>> = std::mem::take(&mut *self.backends.tasks.lock().expect("unpoisoned"));
        for task in tasks { let _ = task.await; }
        self.check().map_err(|message| (self.config.steps, format!("once every backend has stopped: {message}")))?;
        let shared = self.world.lock().expect("unpoisoned");
        self.summary.steps = self.config.steps;
        self.summary.backends = shared.connections as usize;
        self.summary.faults = shared.faults;
        self.summary.greeting_faults = shared.greeting_faults;
        self.summary.resent = shared.keys.values().filter(|(_, reached)| *reached > 1).count();
        drop(shared);
        Ok(self.summary)
    }
}

async fn simulate(config: Config) -> Result<Summary, Violation> {
    let world: World = Arc::new(Mutex::new(Shared {
        seed: config.seed, bug: config.bug, executions: BTreeMap::new(), committed: BTreeSet::new(), keys: BTreeMap::new(), connections: 0, greeted: 0, idle_death: None, trace: VecDeque::new(), faults: 0, greeting_faults: 0,
    }));
    let seed = config.seed;
    let violation = |step: usize, message: String| Violation { seed, step, message, trace: world.lock().expect("unpoisoned").trace.iter().cloned().collect() };
    let listen = || async { TcpListener::bind("127.0.0.1:0").await.map_err(|error| violation(0, format!("cannot listen on loopback: {error}"))) };
    let (backends, clients) = (listen().await?, listen().await?);
    let directory = MemoryDirectory::new();
    let mut record = DeploymentRecord::new("shop", "acme", Vec::new(), 0);
    record.id = "shop".to_owned();
    let record = directory.create_deployment(record).await.map_err(|error| violation(0, error.to_string()))?;
    let client = Client {
        rng: Rng::new(config.seed), world: Arc::clone(&world), backends: Arc::new(Backends { listener: backends, world: Arc::clone(&world), reconnected: Arc::new(Notify::new()), tasks: Mutex::new(Vec::new()) }),
        clients, directory, deployment: record.id, session: None, outcomes: BTreeMap::new(), answered: false, prepared: false, summary: Summary { seed: config.seed, ..Summary::default() }, config,
    };
    match tokio::time::timeout(STALL, client.run()).await {
        Ok(Ok(summary)) => Ok(summary),
        Ok(Err((step, message))) => Err(violation(step, message)),
        Err(_) => Err(violation(0, format!("the simulation stalled for {STALL:?}"))),
    }
}

pub fn run_with(config: Config) -> Result<Summary, Violation> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a current-thread runtime builds");
    runtime.block_on(simulate(config))
}
