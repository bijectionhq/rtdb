use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use rtdb_storage::{Error, ObjectKey, ObjectStore, Result,
    object_store::{ReadReservation, RetainedBytes, StoredObject},
    log::{AppendReceipt, Log, LogOffset, LogRange, LogRecord, LogReplayTail, LogTail, ReplayHead}};
use rtdb_types::Timestamp;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, future::Future, ops::Range, path::Path, sync::Arc, time::Duration};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation { Enqueue, DurableReceipt, RecoveryTail, Tail, Poll, ReplayPrepare, ReplayDelivery, Range, RangeNext, ReadRange, Head, Get, GetRange, Put, PutFile, List, Delete, Copy }

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReplayMode { Batch, Selected }

impl Operation {
    fn replay_mode(self) -> Option<ReplayMode> {
        match self {
            Self::Poll => Some(ReplayMode::Batch),
            Self::ReplayPrepare | Self::ReplayDelivery => Some(ReplayMode::Selected),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase { Before, After }

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum KeyMatch { Any, Exact(String), Prefix(String) }

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Point {
    pub owner: String,
    pub operation: Operation,
    pub phase: Phase,
    pub key: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Action { Fail(String), Hold(String) }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Rule {
    pub owner: String,
    pub operation: Operation,
    pub phase: Phase,
    pub key: KeyMatch,
    pub occurrence: usize,
    pub action: Action,
}

impl Rule {
    fn matches(&self, point: &Point) -> bool {
        self.owner == point.owner && self.operation == point.operation && self.phase == point.phase
            && match &self.key {
                KeyMatch::Any => true,
                KeyMatch::Exact(key) => point.key.as_ref() == Some(key),
                KeyMatch::Prefix(prefix) => point.key.as_ref().is_some_and(|key| key.starts_with(prefix)),
            }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind { Entered, Passed, InjectedFailure, Waiting, Released, Abandoned, ProviderStarted, ProviderReturned, ProviderFailure, ProviderAbandoned }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub sequence: usize,
    pub hit: usize,
    pub point: Point,
    pub kind: EventKind,
    pub rule: Option<usize>,
    pub detail: Option<String>,
}

#[derive(Clone, Copy)]
enum GateState { Waiting, Released, Closed }

struct Gate { state: watch::Sender<GateState> }
impl Gate {
    fn new() -> Self { Self { state: watch::channel(GateState::Waiting).0 } }
    async fn wait(&self) -> Result<()> {
        let mut state = self.state.subscribe();
        loop {
            match *state.borrow_and_update() {
                GateState::Released => return Ok(()),
                GateState::Closed => return Err(Error::Closed("fault scenario closed".into())),
                GateState::Waiting => {},
            }
            state.changed().await.map_err(|_| Error::Closed("fault gate ended".into()))?;
        }
    }
}

struct State {
    counts: Vec<usize>,
    fired: Vec<bool>,
    next_hit: usize,
    active: usize,
    events: Vec<Event>,
    overflow: bool,
    closed: bool,
}

pub struct FaultSchedule {
    rules: Vec<Rule>,
    gates: BTreeMap<String, Gate>,
    state: Mutex<State>,
    progress: watch::Sender<usize>,
    maximum_events: usize,
}

impl FaultSchedule {
    /// Rules count matching calls from one; overlapping rules that fire at the same point are rejected at that point.
    /// Each log owner can schedule batch polling or selected replay.
    /// Selected replay rules require `KeyMatch::Any`, and delivery rules require `Phase::Before`.
    /// The scenario owner closes the schedule and joins all users before collecting its final trace.
    pub fn new(rules: Vec<Rule>, maximum_events: usize) -> Result<Arc<Self>> {
        if maximum_events == 0 || rules.iter().any(|rule| rule.occurrence == 0 || rule.owner.is_empty()) {
            return Err(Error::Task("fault rules need an owner, positive occurrence, and bounded trace capacity".into()));
        }
        Self::validate_replay_rules(&rules)?;
        let gates = rules.iter().filter_map(|rule| match &rule.action {
            Action::Hold(gate) => Some((gate.clone(), Gate::new())), Action::Fail(_) => None,
        }).collect();
        let count = rules.len();
        Ok(Arc::new(Self { rules, gates, maximum_events, progress: watch::channel(0).0,
            state: Mutex::new(State { counts: vec![0; count], fired: vec![false; count], next_hit: 0, active: 0,
                events: Vec::new(), overflow: false, closed: false }) }))
    }

    fn validate_replay_rules(rules: &[Rule]) -> Result<()> {
        let mut modes = BTreeMap::new();
        for rule in rules {
            let Some(mode) = rule.operation.replay_mode() else { continue; };
            if mode == ReplayMode::Selected && rule.key != KeyMatch::Any {
                return Err(Error::Task("selected replay fault rules require KeyMatch::Any".into()));
            }
            if rule.operation == Operation::ReplayDelivery && rule.phase == Phase::After {
                return Err(Error::Task("selected replay delivery fault rules require Phase::Before".into()));
            }
            if modes.insert(rule.owner.as_str(), mode).is_some_and(|previous| previous != mode) {
                return Err(Error::Task(format!("fault log '{}' mixes poll and selected replay rules", rule.owner)));
            }
        }
        Ok(())
    }

    fn replay_mode(&self, owner: &str) -> ReplayMode {
        if self.rules.iter().any(|rule| rule.owner == owner && rule.operation.replay_mode() == Some(ReplayMode::Selected)) {
            ReplayMode::Selected
        } else {
            ReplayMode::Batch
        }
    }

    fn record(&self, state: &mut State, hit: usize, point: &Point, kind: EventKind, rule: Option<usize>, detail: Option<String>) -> Result<()> {
        if state.events.len() == self.maximum_events {
            state.overflow = true;
            return Err(Error::Task("fault trace capacity exhausted".into()));
        }
        state.events.push(Event { sequence: state.events.len(), hit, point: point.clone(), kind, rule, detail });
        Ok(())
    }

    fn begin(self: &Arc<Self>, point: Point) -> Result<(Hit, Option<Action>)> {
        let mut state = self.state.lock();
        if state.closed { return Err(Error::Closed("fault scenario closed".into())); }
        let hit = state.next_hit;
        state.next_hit = state.next_hit.checked_add(1).ok_or_else(|| Error::Task("fault hit sequence overflow".into()))?;
        let mut selected = None;
        for (index, rule) in self.rules.iter().enumerate() {
            if rule.matches(&point) {
                state.counts[index] = state.counts[index].saturating_add(1);
                if state.counts[index] == rule.occurrence {
                    if selected.is_some() { return Err(Error::Task("overlapping fault actions fired at one point".into())); }
                    selected = Some(index);
                }
            }
        }
        self.record(&mut state, hit, &point, EventKind::Entered, selected, None)?;
        if let Some(index) = selected { state.fired[index] = true; }
        state.active += 1;
        self.progress.send_replace(state.next_hit);
        let action = selected.map(|index| self.rules[index].action.clone());
        Ok((Hit { schedule: Arc::clone(self), point, hit, rule: selected, finished: false }, action))
    }

    pub async fn hit(self: &Arc<Self>, point: Point) -> Result<()> {
        let (mut hit, action) = self.begin(point)?;
        match action {
            None => hit.finish(EventKind::Passed, None),
            Some(Action::Fail(reason)) => {
                hit.finish(EventKind::InjectedFailure, Some(reason.clone()))?;
                Err(Error::Task(format!("injected fault: {reason}")))
            }
            Some(Action::Hold(name)) => {
                hit.record(EventKind::Waiting, Some(name.clone()))?;
                self.gates[&name].wait().await?;
                hit.finish(EventKind::Released, Some(name))
            }
        }
    }

    pub fn release(&self, name: &str) -> Result<()> {
        let gate = self.gates.get(name).ok_or_else(|| Error::Task(format!("unknown fault gate: {name}")))?;
        let state = self.state.lock();
        if state.closed { return Err(Error::Closed("fault scenario closed".into())); }
        gate.state.send_replace(GateState::Released);
        Ok(())
    }

    pub async fn wait_for_rule(&self, index: usize) -> Result<()> {
        let mut progress = self.progress.subscribe();
        loop {
            {
                let state = self.state.lock();
                if state.fired.get(index).copied().ok_or_else(|| Error::Task("unknown fault rule".into()))? { return Ok(()); }
                if state.closed { return Err(Error::Closed("fault scenario closed before rule fired".into())); }
            }
            progress.changed().await.map_err(|_| Error::Closed("fault scenario monitor ended".into()))?;
        }
    }

    pub fn close(&self) {
        let mut state = self.state.lock();
        state.closed = true;
        for gate in self.gates.values() { gate.state.send_replace(GateState::Closed); }
        self.progress.send_replace(state.next_hit);
    }

    pub fn assert_exhausted(&self) -> Result<()> {
        let state = self.state.lock();
        if state.overflow || state.active != 0 || state.fired.iter().any(|fired| !fired) {
            return Err(Error::Task("fault scenario has unconsumed rules, active calls, or incomplete trace".into()));
        }
        Ok(())
    }

    pub fn events(&self) -> Vec<Event> { self.state.lock().events.clone() }

    pub(crate) fn event_count(&self) -> usize { self.state.lock().events.len() }

    fn begin_call(self: &Arc<Self>) -> Result<Call> {
        let mut state = self.state.lock();
        if state.closed { return Err(Error::Closed("fault scenario closed".into())); }
        state.active += 1;
        Ok(Call { schedule: Arc::clone(self), phase: CallPhase::Checkpoint })
    }

    async fn around<T: Send>(self: &Arc<Self>, point: Point, action: impl Future<Output = Result<T>> + Send) -> Result<T> {
        let mut call = self.begin_call()?;
        self.hit(point.clone()).await?;
        call.start_provider(&point)?;
        let result = action.await;
        match result {
            Ok(value) => {
                call.finish_provider(EventKind::ProviderReturned, None)?;
                self.hit(Point { phase: Phase::After, ..point }).await?;
                Ok(value)
            }
            Err(error) => {
                call.finish_provider(EventKind::ProviderFailure, Some(error.to_string()))?;
                Err(error)
            }
        }
    }
}

enum CallPhase { Checkpoint, Provider { hit: usize, point: Point } }
struct Call { schedule: Arc<FaultSchedule>, phase: CallPhase }
impl Call {
    fn start_provider(&mut self, point: &Point) -> Result<()> {
        let mut state = self.schedule.state.lock();
        let hit = state.next_hit;
        state.next_hit = hit.checked_add(1).ok_or_else(|| Error::Task("fault hit sequence overflow".into()))?;
        self.schedule.record(&mut state, hit, point, EventKind::ProviderStarted, None, None)?;
        self.phase = CallPhase::Provider { hit, point: point.clone() };
        Ok(())
    }
    fn finish_provider(&mut self, kind: EventKind, detail: Option<String>) -> Result<()> {
        let CallPhase::Provider { hit, point } = std::mem::replace(&mut self.phase, CallPhase::Checkpoint) else {
            return Err(Error::Task("fault provider completion has no started call".into()));
        };
        self.schedule.record(&mut self.schedule.state.lock(), hit, &point, kind, None, detail)
    }
}
impl Drop for Call {
    fn drop(&mut self) {
        let mut state = self.schedule.state.lock();
        if let CallPhase::Provider { hit, point } = &self.phase {
            let _ = self.schedule.record(&mut state, *hit, point, EventKind::ProviderAbandoned, None, None);
        }
        state.active -= 1;
    }
}

struct Hit { schedule: Arc<FaultSchedule>, point: Point, hit: usize, rule: Option<usize>, finished: bool }
impl Hit {
    fn record(&mut self, kind: EventKind, detail: Option<String>) -> Result<()> {
        self.schedule.record(&mut self.schedule.state.lock(), self.hit, &self.point, kind, self.rule, detail)
    }
    fn finish(&mut self, kind: EventKind, detail: Option<String>) -> Result<()> {
        let mut state = self.schedule.state.lock();
        let result = self.schedule.record(&mut state, self.hit, &self.point, kind, self.rule, detail);
        state.active -= 1;
        self.finished = true;
        result
    }
}
impl Drop for Hit {
    fn drop(&mut self) {
        if !self.finished {
            let mut state = self.schedule.state.lock();
            let _ = self.schedule.record(&mut state, self.hit, &self.point, EventKind::Abandoned, self.rule, None);
            state.active -= 1;
        }
    }
}

#[derive(Clone)]
struct FaultEndpoint { name: String, schedule: Arc<FaultSchedule> }
impl FaultEndpoint {
    fn point(&self, operation: Operation, key: Option<&ObjectKey>) -> Point {
        Point { owner: self.name.clone(), operation, phase: Phase::Before, key: key.map(ToString::to_string) }
    }
    async fn around<T: Send>(&self, operation: Operation, key: Option<&ObjectKey>, action: impl Future<Output = Result<T>> + Send) -> Result<T> {
        self.schedule.around(self.point(operation, key), action).await
    }
    fn require_replay<'a>(&self, tail: &'a mut dyn LogTail) -> Result<&'a mut dyn LogReplayTail> {
        tail.replay().ok_or_else(|| Error::Task(format!("fault log '{}' does not support selected replay", self.name)))
    }
}

pub struct FaultStore { inner: Arc<dyn ObjectStore>, owner: FaultEndpoint }
impl FaultStore {
    pub fn new(inner: Arc<dyn ObjectStore>, name: String, schedule: Arc<FaultSchedule>) -> Arc<Self> {
        Arc::new(Self { inner, owner: FaultEndpoint { name, schedule } })
    }
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn head(&self, key: &ObjectKey) -> Result<StoredObject> { self.owner.around(Operation::Head, Some(key), self.inner.head(key)).await }
    async fn head_fresh(&self, key: &ObjectKey) -> Result<StoredObject> { self.owner.around(Operation::Head, Some(key), self.inner.head_fresh(key)).await }
    async fn get(&self, key: &ObjectKey) -> Result<Bytes> { self.owner.around(Operation::Get, Some(key), self.inner.get(key)).await }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<Bytes> {
        self.owner.around(Operation::GetRange, Some(key), self.inner.get_range(key, range)).await
    }
    async fn get_reserved_range(&self, key: &ObjectKey, range: Range<u64>, reservation: ReadReservation) -> Result<RetainedBytes> {
        self.owner.around(Operation::GetRange, Some(key), self.inner.get_reserved_range(key, range, reservation)).await
    }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> Result<()> { self.owner.around(Operation::Put, Some(key), self.inner.put(key, bytes)).await }
    async fn put_file(&self, key: &ObjectKey, source: &Path) -> Result<()> { self.owner.around(Operation::PutFile, Some(key), self.inner.put_file(key, source)).await }
    async fn list(&self, directory: Option<&ObjectKey>) -> Result<Vec<StoredObject>> { self.owner.around(Operation::List, directory, self.inner.list(directory)).await }
    async fn delete(&self, key: &ObjectKey) -> Result<()> { self.owner.around(Operation::Delete, Some(key), self.inner.delete(key)).await }
    async fn copy(&self, source: &ObjectKey, destination: &ObjectKey) -> Result<()> { self.owner.around(Operation::Copy, Some(destination), self.inner.copy(source, destination)).await }
}

pub struct FaultLog { inner: Arc<dyn Log>, owner: FaultEndpoint }
impl FaultLog {
    pub fn new(inner: Arc<dyn Log>, name: String, schedule: Arc<FaultSchedule>) -> Arc<Self> {
        Arc::new(Self { inner, owner: FaultEndpoint { name, schedule } })
    }

    fn wrap_tail(&self, mut inner: Box<dyn LogTail>) -> Result<Box<dyn LogTail>> {
        let mode = self.owner.schedule.replay_mode(&self.owner.name);
        if mode == ReplayMode::Selected { self.owner.require_replay(inner.as_mut())?; }
        Ok(Box::new(FaultTail { inner, owner: self.owner.clone(), mode }))
    }
}
#[async_trait]
impl Log for FaultLog {
    async fn enqueue(&self, payload: Bytes, timestamp: Timestamp) -> Result<AppendReceipt> {
        let receipt = self.owner.around(Operation::Enqueue, None, self.inner.enqueue(payload, timestamp)).await?;
        let owner = self.owner.clone();
        Ok(AppendReceipt::observed(async move { owner.around(Operation::DurableReceipt, None, receipt.durable()).await }))
    }
    fn latest(&self) -> Result<LogOffset> { self.inner.latest() }
    async fn retained_floor(&self) -> Result<Option<LogOffset>> { self.inner.retained_floor().await }
    async fn recovery_tail(&self, after: LogOffset) -> Result<Option<Box<dyn LogTail>>> {
        let tail = self.owner.around(Operation::RecoveryTail, None, self.inner.recovery_tail(after)).await?;
        tail.map(|tail| self.wrap_tail(tail)).transpose()
    }
    async fn tail(&self, after: LogOffset) -> Result<Box<dyn LogTail>> {
        self.wrap_tail(self.owner.around(Operation::Tail, None, self.inner.tail(after)).await?)
    }
    async fn range(&self, from: LogOffset, to: LogOffset) -> Result<Box<dyn LogRange>> {
        let inner = self.owner.around(Operation::Range, None, self.inner.range(from, to)).await?;
        Ok(Box::new(FaultRange { inner, owner: self.owner.clone() }))
    }
    async fn read_range(&self, from: LogOffset, to: LogOffset) -> Result<Vec<LogRecord>> {
        self.owner.around(Operation::ReadRange, None, self.inner.read_range(from, to)).await
    }
}

struct FaultTail { inner: Box<dyn LogTail>, owner: FaultEndpoint, mode: ReplayMode }
#[async_trait]
impl LogTail for FaultTail {
    async fn poll(&mut self, timeout: Duration) -> Result<Vec<LogRecord>> {
        self.owner.around(Operation::Poll, None, self.inner.poll(timeout)).await
    }

    fn replay(&mut self) -> Option<&mut dyn LogReplayTail> {
        match self.mode {
            ReplayMode::Batch => None,
            ReplayMode::Selected => Some(self),
        }
    }
}

#[async_trait]
impl LogReplayTail for FaultTail {
    async fn poll_head(&mut self, timeout: Duration) -> Result<Option<ReplayHead>> {
        let replay = self.owner.require_replay(self.inner.as_mut())?;
        self.owner.around(Operation::ReplayPrepare, None, replay.poll_head(timeout)).await
    }

    async fn read_head(&mut self, head: ReplayHead, reservation: ReadReservation) -> Result<LogRecord> {
        let _call = self.owner.schedule.begin_call()?;
        // A failed checkpoint after delivery would leave the caller behind the advanced cursor.
        self.owner.schedule.hit(self.owner.point(Operation::ReplayDelivery, None)).await?;
        self.owner.require_replay(self.inner.as_mut())?.read_head(head, reservation).await
    }
}

struct FaultRange { inner: Box<dyn LogRange>, owner: FaultEndpoint }
#[async_trait]
impl LogRange for FaultRange {
    fn allocation_bound(&self) -> usize { self.inner.allocation_bound() }
    async fn next(&mut self, reservation: ReadReservation) -> Result<Option<LogRecord>> {
        self.owner.around(Operation::RangeNext, None, self.inner.next(reservation)).await
    }
}

pub struct FaultResources { inner: Arc<dyn rtdb_node::ResourceFactory>, schedule: Arc<FaultSchedule> }
impl FaultResources {
    /// Names checkpoints `database/source`, `database/replica`, and `database/store` while forwarding the original drivers and shutdown.
    pub fn new(inner: Arc<dyn rtdb_node::ResourceFactory>, schedule: Arc<FaultSchedule>) -> Arc<Self> {
        Arc::new(Self { inner, schedule })
    }
}
#[async_trait]
impl rtdb_node::ResourceFactory for FaultResources {
    fn validate(&self, name: &str, configuration: &rtdb_format::database::proto::DatabaseConfig) -> rtdb_types::Result<()> {
        self.inner.validate(name, configuration)
    }
    async fn open_storage(&self, name: &str, configuration: &rtdb_format::database::proto::DatabaseConfig) -> rtdb_types::Result<rtdb_node::DatabaseStorage> {
        let mut resources = self.inner.open_storage(name, configuration).await?;
        resources.store = FaultStore::new(resources.store, format!("{name}/store"), Arc::clone(&self.schedule));
        Ok(resources)
    }
    async fn open_logs(&self, name: &str, configuration: &rtdb_format::database::proto::DatabaseConfig, access: rtdb_node::LogAccess, cancellation: CancellationToken, storage: &rtdb_node::DatabaseStorage) -> rtdb_types::Result<rtdb_node::OpenedLogs> {
        let mut logs = self.inner.open_logs(name, configuration, access, cancellation, storage).await?;
        logs.source = FaultLog::new(logs.source, format!("{name}/source"), Arc::clone(&self.schedule));
        logs.replica = FaultLog::new(logs.replica, format!("{name}/replica"), Arc::clone(&self.schedule));
        Ok(logs)
    }
    async fn close(&self) -> rtdb_types::Result<()> { self.inner.close().await }
}
