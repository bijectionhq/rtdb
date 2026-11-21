//! Every stream a server holds, and the order in which their mutations reach the journal.

use super::journal::{Captured, Journal, Recovered, Work, Write};
use super::payload::{Item, Source, Stored};
use super::state::{Decision, StreamState, Window};
use crate::protocol::{Entries, Entry, Failed, MAXIMUM_FRAME_BYTES, RequestBody, ResponseBody};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

const COMPACTION_SLACK_BYTES: u64 = 1 << 20;
const PENDING_WRITE_BYTES: usize = MAXIMUM_FRAME_BYTES + 1024;
const MINIMUM_WRITE_CHARGE: usize = 1024;

/// A stream's state; a stream with no waiting reader owns no task and no timer.
pub(crate) struct Slot {
    /// Names the stream in the journal's records.
    pub id: u64,
    pub name: String,
    pub state: Mutex<StreamState<Stored>>,
    /// Woken when the commit lsn or trim point moves, or the stream is dropped.
    pub progressed: Notify,
}

pub(crate) struct Store {
    journal: Arc<Journal>,
    streams: Mutex<HashMap<String, Arc<Slot>>>,
    queue: mpsc::UnboundedSender<Work>,
    pending_writes: Arc<Semaphore>,
}

pub(crate) enum Outcome {
    Now(ResponseBody),
    /// The reply, to be sent once the journal reports the decision durable.
    Durable(oneshot::Receiver<Result<(), String>>, ResponseBody),
}

impl Store {
    pub fn new(journal: Arc<Journal>, recovered: Vec<Recovered>, queue: mpsc::UnboundedSender<Work>) -> Self {
        let streams = recovered.into_iter().map(|stream| {
            let slot = Slot { id: stream.id, name: stream.name.clone(), state: Mutex::new(stream.state), progressed: Notify::new() };
            (stream.name, Arc::new(slot))
        }).collect();
        Self { journal, streams: Mutex::new(streams), queue, pending_writes: Arc::new(Semaphore::new(PENDING_WRITE_BYTES)) }
    }

    fn slot(&self, name: &str) -> Arc<Slot> {
        let mut streams = self.streams.lock();
        if let Some(slot) = streams.get(name) { return Arc::clone(slot); }
        let slot = Arc::new(Slot { id: self.journal.allocate_id(), name: name.to_owned(), state: Mutex::new(StreamState::default()), progressed: Notify::new() });
        streams.insert(name.to_owned(), Arc::clone(&slot));
        slot
    }

    fn existing(&self, name: &str) -> Option<Arc<Slot>> { self.streams.lock().get(name).cloned() }

    /// Reserves journal capacity before deciding the mutation.
    /// The decision and submission share the stream lock, so decisions reach the journal in order.
    pub async fn mutate(&self, body: RequestBody, frame_bytes: usize) -> Outcome {
        let charge = frame_bytes.saturating_add(MINIMUM_WRITE_CHARGE).min(PENDING_WRITE_BYTES) as u32;
        let Ok(admission) = self.pending_writes.clone().acquire_many_owned(charge).await else {
            return Outcome::Now(failed("the journal has stopped"));
        };
        if self.queue.is_closed() { return Outcome::Now(failed("the journal has stopped")); }
        let (stream, decide): (String, Box<dyn FnOnce(&mut StreamState<Stored>) -> Decision>) = match body {
            RequestBody::Append(append) => (append.stream, Box::new(move |state| state.append(append.epoch, append.first_lsn, append.entries, append.commit_lsn))),
            RequestBody::Overwrite(overwrite) => (overwrite.stream, Box::new(move |state| state.overwrite(overwrite.epoch, overwrite.from_lsn, overwrite.entries, overwrite.complete, overwrite.commit_lsn))),
            RequestBody::Commit(commit) => (commit.stream, Box::new(move |state| state.commit(commit.epoch, commit.commit_lsn))),
            RequestBody::Seal(seal) => (seal.stream, Box::new(move |state| state.seal(seal.epoch))),
            RequestBody::Trim(trim) => (trim.stream, Box::new(move |state| state.trim(trim.through_lsn))),
            RequestBody::DropStream(drop) => (drop.stream, Box::new(move |state| state.drop_stream(drop.epoch))),
            RequestBody::Read(_) | RequestBody::Fetch(_) | RequestBody::Check(_) => return Outcome::Now(failed("not a mutation")),
        };
        let slot = self.slot(&stream);
        let mut state = slot.state.lock();
        let before = (state.commit, state.trim, state.dropped);
        let decision = decide(&mut state);
        if matches!(decision.reply, ResponseBody::Refused(_) | ResponseBody::Failed(_)) { return Outcome::Now(decision.reply); }
        let (done, durable) = oneshot::channel();
        if self.queue.send(Work { write: Write::Events(Arc::clone(&slot), decision.events), done, _admission: Some(admission) }).is_err() {
            return Outcome::Now(failed("the journal has stopped"));
        }
        if (state.commit, state.trim, state.dropped) != before { slot.progressed.notify_waiters(); }
        Outcome::Durable(durable, decision.reply)
    }

    /// Queues a compaction once the segments hold more than twice what the streams retain.
    /// Every stream is captured under its lock, all held together while the compaction is queued, so each capture sits exactly where it belongs in its stream's order.
    pub fn compact_if_mostly_dead(&self) {
        let disk = self.journal.bytes();
        if disk <= COMPACTION_SLACK_BYTES { return; }
        let slots: Vec<Arc<Slot>> = self.streams.lock().values().cloned().collect();
        let live: u64 = slots.iter().map(|slot| slot.state.lock().retained_bytes() as u64).sum();
        if disk <= 2 * live + COMPACTION_SLACK_BYTES || !self.journal.claim_compaction() { return; }
        let streams = self.streams.lock();
        let locked: Vec<_> = streams.values().map(|slot| (slot, slot.state.lock())).collect();
        let captured = locked.iter().map(|(slot, state)| Captured { slot: Arc::clone(slot), state: (**state).clone() }).collect();
        if self.queue.send(Work { write: Write::Compact(captured), done: oneshot::channel().0, _admission: None }).is_err() { self.journal.release_compaction(); }
    }

    pub fn check(&self, stream: &str, epoch: u64) -> ResponseBody {
        match self.existing(stream) { Some(slot) => slot.state.lock().check(epoch), None => StreamState::<Stored>::default().check(epoch) }
    }

    /// Selects entries in `[from, to]` and where to find each payload, under the stream's lock.
    fn select(&self, state: &StreamState<Stored>, from: i64, to: i64, max_entries: usize, max_bytes: usize) -> Window<Item> {
        state.window(from, to, max_entries, max_bytes, |lsn, held| Item { lsn, epoch: held.epoch, timestamp: held.timestamp, source: self.journal.source(&held.payload) })
    }

    /// Held entries in `[from, to)`, committed or not; a selection with payloads still on disk is resolved by `entries`.
    pub fn fetch(&self, stream: &str, from: i64, to: i64, max_entries: usize, max_bytes: usize) -> Result<Selected, ResponseBody> {
        let Some(slot) = self.existing(stream) else { return Ok(Selected(self.select(&StreamState::default(), from, -1, 0, 0))); };
        let state = slot.state.lock();
        if state.dropped { return Err(failed("the stream was dropped")); }
        let to = to.saturating_sub(1).min(state.last());
        Ok(Selected(self.select(&state, from, to, max_entries, max_bytes)))
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
        let read = |state: &StreamState<Stored>| {
            if state.dropped { return Err(failed("the stream was dropped")); }
            Ok(Selected(self.select(state, from, state.commit, max_entries, max_bytes)))
        };
        if wait.is_zero() {
            let selected = match self.existing(stream) { Some(slot) => read(&slot.state.lock()), None => read(&StreamState::default()) };
            return match selected { Ok(selected) => self.entries(selected).await, Err(reply) => reply };
        }
        let slot = self.slot(stream);
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let progressed = slot.progressed.notified();
            tokio::pin!(progressed);
            progressed.as_mut().enable();
            let selected = match read(&slot.state.lock()) { Ok(selected) => selected, Err(reply) => return reply };
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{DropStream, Overwrite};
    use bytes::Bytes;

    #[tokio::test]
    async fn a_full_journal_queue_delays_the_decision_until_capacity_is_available() {
        let directory = tempfile::tempdir().unwrap();
        let (journal, recovered) = Journal::open(directory.path(), false, 0).unwrap();
        let (queue, mut work) = mpsc::unbounded_channel();
        let store = Store::new(Arc::new(journal), recovered, queue);
        let overwrite = |stream: &str| RequestBody::Overwrite(Overwrite {
            stream: stream.into(), epoch: 1, from_lsn: 0, entries: Vec::new(), complete: true, commit_lsn: -1,
        });
        assert!(matches!(store.mutate(overwrite("first"), MAXIMUM_FRAME_BYTES).await, Outcome::Durable(_, _)));
        let mut second = Box::pin(store.mutate(overwrite("second"), 32));
        assert!(futures::poll!(&mut second).is_pending());
        assert!(store.existing("second").is_none());
        let pending = work.recv().await.unwrap();
        assert!(futures::poll!(&mut second).is_pending());
        drop(pending);
        assert!(matches!(second.await, Outcome::Durable(_, _)));
        assert!(store.existing("second").is_some());
    }

    #[tokio::test]
    async fn a_drop_queued_after_an_older_compaction_schedules_reclamation_again() {
        let directory = tempfile::tempdir().unwrap();
        let (journal, recovered) = Journal::open(directory.path(), true, 0).unwrap();
        let journal = Arc::new(journal);
        let (queue, work) = mpsc::unbounded_channel();
        let store = Arc::new(Store::new(Arc::clone(&journal), recovered, queue));
        assert!(matches!(store.mutate(RequestBody::Overwrite(Overwrite {
            stream: "drop-me".into(),
            epoch: 1,
            from_lsn: 0,
            entries: vec![Entry { lsn: 0, epoch: 1, timestamp: 0, payload: Bytes::from(vec![1; 2 << 20]) }],
            complete: true,
            commit_lsn: 0,
        }), 2 << 20).await, Outcome::Durable(_, _)));
        let slot = store.existing("drop-me").unwrap();
        let capture = Captured { slot: Arc::clone(&slot), state: slot.state.lock().clone() };
        assert!(journal.claim_compaction());
        let first_compaction = Work { write: Write::Compact(vec![capture]), done: oneshot::channel().0, _admission: None };
        assert!(store.queue.send(first_compaction).is_ok());
        let Outcome::Durable(dropped, _) = store.mutate(RequestBody::DropStream(DropStream { stream: "drop-me".into(), epoch: 2 }), 32).await else {
            panic!("the drop was refused");
        };
        let task = tokio::spawn(super::super::journal::run(Arc::clone(&journal), Arc::downgrade(&store), work));
        dropped.await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while journal.bytes() >= COMPACTION_SLACK_BYTES { tokio::time::sleep(Duration::from_millis(10)).await; }
        }).await.expect("a follow-up compaction reclaimed the drop");
        drop(store);
        task.await.unwrap().unwrap();
    }
}
