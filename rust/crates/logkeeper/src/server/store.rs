//! Every stream a server holds, and the order in which their mutations reach the journal.

use super::journal::{Captured, Journal, Recovered, Work, Write};
use super::payload::{Item, Source, Stored};
use super::state::{Decision, StreamState, Window};
use crate::protocol::{Entries, Entry, Failed, RequestBody, ResponseBody};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

const COMPACTION_SLACK_BYTES: u64 = 1 << 20;

/// A stream's state; a stream with no waiting reader owns no task and no timer.
pub(crate) struct Slot {
    /// Names the stream in the journal's records.
    pub id: u64,
    pub name: String,
    pub state: Mutex<StreamState<Stored>>,
    /// Woken when the commit lsn or trim point moves.
    pub progressed: Notify,
}

pub(crate) struct Store {
    journal: Arc<Journal>,
    streams: Mutex<HashMap<String, Arc<Slot>>>,
    queue: mpsc::UnboundedSender<Work>,
}

pub(crate) enum Outcome {
    Now(ResponseBody),
    /// The reply, to be sent once the journal reports the decision durable.
    Durable(oneshot::Receiver<Result<(), String>>, ResponseBody),
}

type Compaction = oneshot::Receiver<Result<(), String>>;

impl Store {
    pub fn new(journal: Arc<Journal>, recovered: Vec<Recovered>, queue: mpsc::UnboundedSender<Work>) -> Self {
        let streams = recovered.into_iter().map(|stream| {
            let slot = Slot { id: stream.id, name: stream.name.clone(), state: Mutex::new(stream.state), progressed: Notify::new() };
            (stream.name, Arc::new(slot))
        }).collect();
        Self { journal, streams: Mutex::new(streams), queue }
    }

    fn slot(&self, name: &str) -> Arc<Slot> {
        let mut streams = self.streams.lock();
        if let Some(slot) = streams.get(name) { return Arc::clone(slot); }
        let slot = Arc::new(Slot { id: self.journal.allocate_id(), name: name.to_owned(), state: Mutex::new(StreamState::default()), progressed: Notify::new() });
        streams.insert(name.to_owned(), Arc::clone(&slot));
        slot
    }

    fn existing(&self, name: &str) -> Option<Arc<Slot>> { self.streams.lock().get(name).cloned() }

    /// Decides a mutation and queues its events, both under the stream's lock, so the journal sees the stream's decisions in order.
    pub fn mutate(&self, body: RequestBody) -> Outcome {
        let (stream, decide): (String, Box<dyn FnOnce(&mut StreamState<Stored>) -> Decision>) = match body {
            RequestBody::Append(append) => (append.stream, Box::new(move |state| state.append(append.epoch, append.first_lsn, append.entries, append.commit_lsn))),
            RequestBody::Overwrite(overwrite) => (overwrite.stream, Box::new(move |state| state.overwrite(overwrite.epoch, overwrite.from_lsn, overwrite.entries, overwrite.complete, overwrite.commit_lsn))),
            RequestBody::Commit(commit) => (commit.stream, Box::new(move |state| state.commit(commit.epoch, commit.commit_lsn))),
            RequestBody::Seal(seal) => (seal.stream, Box::new(move |state| state.seal(seal.epoch))),
            RequestBody::Trim(trim) => (trim.stream, Box::new(move |state| state.trim(trim.through_lsn))),
            RequestBody::Retire(retire) => (retire.stream, Box::new(move |state| state.retire(retire.epoch))),
            RequestBody::Read(_) | RequestBody::Fetch(_) | RequestBody::Check(_) => return Outcome::Now(failed("not a mutation")),
        };
        let slot = self.slot(&stream);
        let (outcome, trimmed) = {
            let mut state = slot.state.lock();
            let before = (state.commit, state.trim);
            let decision = decide(&mut state);
            if matches!(decision.reply, ResponseBody::Refused(_) | ResponseBody::Failed(_)) { return Outcome::Now(decision.reply); }
            let (done, durable) = oneshot::channel();
            if self.queue.send(Work { write: Write::Events(Arc::clone(&slot), decision.events), done }).is_err() {
                return Outcome::Now(failed("the journal has stopped"));
            }
            if (state.commit, state.trim) != before { slot.progressed.notify_waiters(); }
            (Outcome::Durable(durable, decision.reply), state.trim != before.1)
        };
        if trimmed { self.compact_if_mostly_dead(); }
        outcome
    }

    /// Queues a compaction once the segments hold more than twice what the streams retain.
    /// Every stream is captured under its lock, all held together while the compaction is queued, so each capture sits exactly where it belongs in its stream's order.
    fn compact_if_mostly_dead(&self) {
        let disk = self.journal.bytes();
        if disk <= COMPACTION_SLACK_BYTES { return; }
        let slots: Vec<Arc<Slot>> = self.streams.lock().values().cloned().collect();
        let live: u64 = slots.iter().map(|slot| slot.state.lock().retained_bytes() as u64).sum();
        if disk <= 2 * live + COMPACTION_SLACK_BYTES { return; }
        let _ = self.queue_compaction();
    }

    fn queue_compaction(&self) -> Result<Option<Compaction>, String> {
        if !self.journal.claim_compaction() { return Ok(None); }
        let streams = self.streams.lock();
        let locked: Vec<_> = streams.values().map(|slot| (slot, slot.state.lock())).collect();
        let captured = locked.iter().map(|(slot, state)| Captured { slot: Arc::clone(slot), state: (**state).clone() }).collect();
        let (done, finished) = oneshot::channel();
        if self.queue.send(Work { write: Write::Compact(captured), done }).is_err() {
            self.journal.release_compaction();
            return Err("the journal has stopped".into());
        }
        Ok(Some(finished))
    }

    /// After the fence and trim are durable, wait for a base that contains
    /// neither acknowledged nor uncommitted payloads of the retired stream.
    pub async fn retire(&self, body: crate::protocol::Retire) -> ResponseBody {
        if !self.journal.is_durable() { return failed("retirement requires a durable journal"); }
        let slot = self.slot(&body.stream);
        let epoch = body.epoch;
        let accepted = match self.mutate(RequestBody::Retire(body)) {
            Outcome::Now(reply) => return reply,
            Outcome::Durable(durable, reply) => match durable.await {
                Ok(Ok(())) => reply,
                Ok(Err(error)) => return failed(error),
                Err(_) => return failed("the journal has stopped"),
            },
        };
        let ResponseBody::Accepted(position) = &accepted else { return failed("unexpected retirement decision"); };
        loop {
            let released = self.journal.compaction_finished();
            if self.journal.compacted(slot.id, epoch, position.last_lsn) { return accepted; }
            match self.queue_compaction() {
                Ok(Some(finished)) => return match finished.await {
                    Ok(Ok(())) => accepted,
                    Ok(Err(error)) => failed(error),
                    Err(_) => failed("the journal has stopped"),
                },
                Ok(None) => released.await,
                Err(error) => return failed(error),
            }
        }
    }

    pub fn check(&self, stream: &str, epoch: u64) -> ResponseBody {
        match self.existing(stream) { Some(slot) => slot.state.lock().check(epoch), None => StreamState::<Stored>::default().check(epoch) }
    }

    /// Selects entries in `[from, to]` and where to find each payload, under the stream's lock.
    fn select(&self, state: &StreamState<Stored>, from: i64, to: i64, max_entries: usize, max_bytes: usize) -> Window<Item> {
        state.window(from, to, max_entries, max_bytes, |lsn, held| Item { lsn, epoch: held.epoch, timestamp: held.timestamp, source: self.journal.source(&held.payload) })
    }

    /// Held entries in `[from, to)`, committed or not; a selection with payloads still on disk is resolved by `entries`.
    pub fn fetch(&self, stream: &str, from: i64, to: i64, max_entries: usize, max_bytes: usize) -> Selected {
        let Some(slot) = self.existing(stream) else { return Selected(self.select(&StreamState::default(), from, -1, 0, 0)); };
        let state = slot.state.lock();
        let to = to.saturating_sub(1).min(state.last());
        Selected(self.select(&state, from, to, max_entries, max_bytes))
    }

    /// Reads whatever payloads a selection still needs from the segments, off the async threads.
    pub async fn entries(&self, selected: Selected) -> ResponseBody {
        let window = match selected.ready() { Ok(body) => return body, Err(Selected(window)) => window };
        let journal = Arc::clone(&self.journal);
        match tokio::task::spawn_blocking(move || journal.resolve(window)).await {
            Ok(Ok(entries)) => ResponseBody::Entries(entries),
            Ok(Err(error)) => failed(format!("reading the segments failed: {error}")),
            Err(error) => failed(format!("reading the segments failed: {error}")),
        }
    }

    /// Long-polls up to `wait` for a committed entry at `from`.
    pub async fn read(&self, stream: &str, from: i64, max_entries: usize, max_bytes: usize, wait: Duration, cancellation: &CancellationToken) -> ResponseBody {
        let read = |state: &StreamState<Stored>| Selected(self.select(state, from, state.commit, max_entries, max_bytes));
        if wait.is_zero() {
            let selected = match self.existing(stream) { Some(slot) => read(&slot.state.lock()), None => read(&StreamState::default()) };
            return self.entries(selected).await;
        }
        let slot = self.slot(stream);
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let progressed = slot.progressed.notified();
            tokio::pin!(progressed);
            progressed.as_mut().enable();
            let selected = read(&slot.state.lock());
            if !selected.0.items.is_empty() || from <= selected.0.trim_lsn { return self.entries(selected).await; }
            tokio::select! {
                _ = progressed => (),
                _ = tokio::time::sleep_until(deadline) => return self.entries(selected).await,
                _ = cancellation.cancelled() => return self.entries(selected).await,
            }
        }
    }
}

/// Entries selected under their stream's lock, some of whose payloads may still have to be read from the segments.
pub(crate) struct Selected(Window<Item>);

impl Selected {
    /// The reply, if every payload is already in hand.
    pub fn ready(self) -> std::result::Result<ResponseBody, Selected> {
        if self.0.items.iter().any(|item| matches!(item.source, Source::Disk(..))) { return Err(self); }
        let Window { items, commit_lsn, trim_lsn, last_lsn } = self.0;
        let entries = items.into_iter().map(|item| {
            let Source::Ready(payload) = item.source else { unreachable!("checked above") };
            Entry { lsn: item.lsn, epoch: item.epoch, timestamp: item.timestamp, payload }
        }).collect();
        Ok(ResponseBody::Entries(Entries { entries, commit_lsn, trim_lsn, last_lsn }))
    }
}

pub(crate) fn failed(message: impl Into<String>) -> ResponseBody { ResponseBody::Failed(Failed { message: message.into() }) }
