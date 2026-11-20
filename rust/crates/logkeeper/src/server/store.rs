//! Every stream a server holds, and the order in which their mutations reach the journal.

use super::journal::{FileInfo, Journal, Recovered, Work, Write};
use super::state::{Decision, StreamState};
use crate::protocol::{Entries, Failed, RequestBody, ResponseBody};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

const REWRITE_SLACK_BYTES: u64 = 1 << 20;

/// A stream's state and file; a stream with no waiting reader owns no task and no timer.
pub(crate) struct Slot {
    pub name: String,
    pub path: PathBuf,
    pub state: Mutex<StreamState>,
    pub file: Mutex<FileInfo>,
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

impl Store {
    pub fn new(journal: Arc<Journal>, recovered: Vec<Recovered>, queue: mpsc::UnboundedSender<Work>) -> Self {
        let streams = recovered.into_iter().map(|stream| {
            let slot = Slot { name: stream.name.clone(), path: stream.path, state: Mutex::new(stream.state), file: Mutex::new(FileInfo { exists: true, bytes: stream.bytes }), progressed: Notify::new() };
            (stream.name, Arc::new(slot))
        }).collect();
        Self { journal, streams: Mutex::new(streams), queue }
    }

    fn slot(&self, name: &str) -> Arc<Slot> {
        let mut streams = self.streams.lock();
        if let Some(slot) = streams.get(name) { return Arc::clone(slot); }
        let slot = Arc::new(Slot { name: name.to_owned(), path: self.journal.path_for(name), state: Mutex::new(StreamState::default()), file: Mutex::new(FileInfo::default()), progressed: Notify::new() });
        streams.insert(name.to_owned(), Arc::clone(&slot));
        slot
    }

    fn existing(&self, name: &str) -> Option<Arc<Slot>> { self.streams.lock().get(name).cloned() }

    /// Decides a mutation and queues its events, both under the stream's lock, so the journal sees the stream's decisions in order.
    pub fn mutate(&self, body: RequestBody) -> Outcome {
        let (stream, decide): (String, Box<dyn FnOnce(&mut StreamState) -> Decision>) = match body {
            RequestBody::Append(append) => (append.stream, Box::new(move |state| state.append(append.epoch, append.first_lsn, append.entries, append.commit_lsn))),
            RequestBody::Overwrite(overwrite) => (overwrite.stream, Box::new(move |state| state.overwrite(overwrite.epoch, overwrite.from_lsn, overwrite.entries, overwrite.complete, overwrite.commit_lsn))),
            RequestBody::Commit(commit) => (commit.stream, Box::new(move |state| state.commit(commit.epoch, commit.commit_lsn))),
            RequestBody::Seal(seal) => (seal.stream, Box::new(move |state| state.seal(seal.epoch))),
            RequestBody::Trim(trim) => (trim.stream, Box::new(move |state| state.trim(trim.through_lsn))),
            RequestBody::Read(_) | RequestBody::Fetch(_) | RequestBody::Check(_) => return Outcome::Now(failed("not a mutation")),
        };
        let slot = self.slot(&stream);
        let mut state = slot.state.lock();
        let before = (state.commit, state.trim);
        let decision = decide(&mut state);
        if matches!(decision.reply, ResponseBody::Refused(_) | ResponseBody::Failed(_)) { return Outcome::Now(decision.reply); }
        let trimmed = state.trim != before.1;
        let write = if trimmed && slot.file.lock().bytes > 2 * state.retained_bytes() as u64 + REWRITE_SLACK_BYTES {
            Write::Rewrite(state.clone())
        } else {
            Write::Events(decision.events)
        };
        let (done, durable) = oneshot::channel();
        if self.queue.send(Work { slot: Arc::clone(&slot), write, done }).is_err() {
            return Outcome::Now(failed("the journal has stopped"));
        }
        if (state.commit, state.trim) != before { slot.progressed.notify_waiters(); }
        Outcome::Durable(durable, decision.reply)
    }

    pub fn check(&self, stream: &str, epoch: u64) -> ResponseBody {
        match self.existing(stream) { Some(slot) => slot.state.lock().check(epoch), None => StreamState::default().check(epoch) }
    }

    pub fn fetch(&self, stream: &str, from: i64, to: i64, max_entries: usize, max_bytes: usize) -> ResponseBody {
        let Some(slot) = self.existing(stream) else { return ResponseBody::Entries(StreamState::default().entries(from, -1, 0, 0)); };
        let state = slot.state.lock();
        let to = to.saturating_sub(1).min(state.last());
        ResponseBody::Entries(state.entries(from, to, max_entries, max_bytes))
    }

    /// Long-polls up to `wait` for a committed entry at `from`.
    pub async fn read(&self, stream: &str, from: i64, max_entries: usize, max_bytes: usize, wait: Duration, cancellation: &CancellationToken) -> ResponseBody {
        let read = |state: &StreamState| state.entries(from, state.commit, max_entries, max_bytes);
        if wait.is_zero() {
            return ResponseBody::Entries(match self.existing(stream) { Some(slot) => read(&slot.state.lock()), None => StreamState::default().entries(from, -1, 0, 0) });
        }
        let slot = self.slot(stream);
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let progressed = slot.progressed.notified();
            tokio::pin!(progressed);
            progressed.as_mut().enable();
            let entries: Entries = read(&slot.state.lock());
            if !entries.entries.is_empty() || from <= entries.trim_lsn { return ResponseBody::Entries(entries); }
            tokio::select! {
                _ = progressed => (),
                _ = tokio::time::sleep_until(deadline) => return ResponseBody::Entries(entries),
                _ = cancellation.cancelled() => return ResponseBody::Entries(entries),
            }
        }
    }
}

pub(crate) fn failed(message: impl Into<String>) -> ResponseBody { ResponseBody::Failed(Failed { message: message.into() }) }
