//! Every stream a server holds, and the order in which their mutations reach the journal.

use super::journal::{Captured, Journal, Recovered, Work, Write};
use super::payload::{Item, Source, Stored};
use super::state::{Decision, StreamState, Window, WriterAuthority};
use crate::history::RetainedChain;
use crate::protocol::{AuthorityConfirmed, DigestPage, LogletCheck, Refused, Entries, Entry, Failed, MAXIMUM_FRAME_BYTES, PageDigest, RequestBody, ResponseBody};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

const COMPACTION_SLACK_BYTES: u64 = 1 << 20;
const PENDING_WRITE_BYTES: usize = MAXIMUM_FRAME_BYTES + 1024;
const MINIMUM_WRITE_CHARGE: usize = 1024;
const DIGEST_PAGE_ENTRIES: usize = 1024;
const DIGEST_PAGE_BYTES: usize = 4 << 20;
const CONCURRENT_DIGEST_PAGES: usize = 2;

/// A stream's state; a stream with no waiting reader owns no task and no timer.
pub(crate) struct Slot {
    /// Names the stream in the journal's records.
    pub id: u64,
    pub name: String,
    pub state: Mutex<StreamState<Stored>>,
    authority: Arc<Mutex<AuthorityProjection>>,
    /// Woken when the commit lsn or trim point moves, or the stream is dropped.
    pub progressed: Notify,
}

impl Slot {
    pub fn new(id: u64, name: String, state: StreamState<Stored>) -> Self {
        let authority = Arc::new(Mutex::new(AuthorityProjection {
            accepted: state.writer_authority(), revision: Some(0), durable_revision: 0,
        }));
        Self { id, name, state: Mutex::new(state), authority, progressed: Notify::new() }
    }

    pub fn publish_authority_revision(&self, revision: u64) {
        self.authority.lock().durable_revision = revision;
    }
}

struct AuthorityProjection {
    accepted: WriterAuthority,
    revision: Option<u64>,
    durable_revision: u64,
}

impl AuthorityProjection {
    fn record(&mut self, authority: WriterAuthority) -> Option<u64> {
        self.accepted = authority;
        self.revision = self.revision.and_then(|revision| revision.checked_add(1));
        self.revision
    }

    fn confirm(&self, epoch: u64, ensemble: [u8; 32]) -> ResponseBody {
        if self.revision == Some(self.durable_revision) && self.accepted.confirms(epoch, ensemble) {
            ResponseBody::AuthorityConfirmed(AuthorityConfirmed {})
        } else {
            // A pending higher promise also fences the earlier writer.
            ResponseBody::Refused(Refused { promised: self.accepted.promised, last_lsn: -1 })
        }
    }
}

pub(crate) struct Store {
    journal: Arc<Journal>,
    streams: Mutex<HashMap<String, Arc<Slot>>>,
    authorities: Mutex<HashMap<String, Arc<Mutex<AuthorityProjection>>>>,
    queue: mpsc::UnboundedSender<Work>,
    pending_writes: Arc<Semaphore>,
    digest_pages: Semaphore,
}

pub(crate) enum Outcome {
    Now(ResponseBody),
    /// The reply, to be sent once the journal reports the decision durable.
    Durable(oneshot::Receiver<Result<(), String>>, ResponseBody),
}

type Compaction = oneshot::Receiver<Result<(), String>>;

impl Store {
    pub fn new(journal: Arc<Journal>, recovered: Vec<Recovered>, queue: mpsc::UnboundedSender<Work>) -> Self {
        let streams: HashMap<_, _> = recovered.into_iter().map(|stream| {
            let slot = Slot::new(stream.id, stream.name.clone(), stream.state);
            (stream.name, Arc::new(slot))
        }).collect();
        let authorities = streams.iter().map(|(name, slot)| (name.clone(), Arc::clone(&slot.authority))).collect();
        Self { journal, streams: Mutex::new(streams), authorities: Mutex::new(authorities), queue, pending_writes: Arc::new(Semaphore::new(PENDING_WRITE_BYTES)), digest_pages: Semaphore::new(CONCURRENT_DIGEST_PAGES) }
    }

    fn slot(&self, name: &str) -> Arc<Slot> {
        let mut streams = self.streams.lock();
        if let Some(slot) = streams.get(name) { return Arc::clone(slot); }
        let slot = Arc::new(Slot::new(self.journal.allocate_id(), name.to_owned(), StreamState::default()));
        self.authorities.lock().insert(name.to_owned(), Arc::clone(&slot.authority));
        streams.insert(name.to_owned(), Arc::clone(&slot));
        slot
    }

    fn existing(&self, name: &str) -> Option<Arc<Slot>> { self.streams.lock().get(name).cloned() }

    /// Reserves journal capacity before deciding the mutation.
    /// The decision and submission share the stream lock, so decisions reach the journal in order.
    pub async fn mutate(&self, body: RequestBody, frame_bytes: usize) -> Outcome {
        let observed_lsn = match &body {
            RequestBody::AppendObserved(append) => {
                if !self.journal.is_durable() || append.entries.len() != 1 {
                    return Outcome::Now(failed("a guarded sync observation requires one entry on a durable journal"));
                }
                Some(append.first_lsn)
            }
            _ => None,
        };
        let charge = frame_bytes.saturating_add(MINIMUM_WRITE_CHARGE).min(PENDING_WRITE_BYTES) as u32;
        let Ok(admission) = self.pending_writes.clone().acquire_many_owned(charge).await else {
            return Outcome::Now(failed("the journal has stopped"));
        };
        if self.queue.is_closed() { return Outcome::Now(failed("the journal has stopped")); }
        let (stream, decide): (String, Box<dyn FnOnce(&mut StreamState<Stored>) -> Decision>) = match body {
            RequestBody::Append(append) | RequestBody::AppendObserved(append) => (append.stream, Box::new(move |state| {
                state.append(append.epoch, append.first_lsn, append.entries, append.commit_lsn)
            })),
            RequestBody::Overwrite(overwrite) if overwrite.activate_ensemble.is_empty() => (overwrite.stream, Box::new(move |state| {
                state.overwrite(overwrite.epoch, overwrite.from_lsn, overwrite.entries, overwrite.complete, overwrite.commit_lsn)
            })),
            RequestBody::Overwrite(overwrite) => {
                let Ok(ensemble) = overwrite.activate_ensemble.as_ref().try_into() else { return Outcome::Now(failed("invalid ensemble digest")); };
                if !overwrite.complete { return Outcome::Now(failed("activation requires a complete overwrite")); }
                (overwrite.stream, Box::new(move |state| state.repair(overwrite.epoch, overwrite.from_lsn, overwrite.entries, overwrite.commit_lsn, ensemble)))
            }
            RequestBody::Commit(commit) => (commit.stream, Box::new(move |state| state.commit(commit.epoch, commit.commit_lsn))),
            RequestBody::Seal(seal) => (seal.stream, Box::new(move |state| state.seal(seal.epoch))),
            RequestBody::Trim(trim) => (trim.stream, Box::new(move |state| state.trim(trim.through_lsn))),
            RequestBody::TrimDurable(trim) => {
                if !self.journal.is_durable() { return Outcome::Now(failed("durable trim requires a journal with fsync")); }
                (trim.stream, Box::new(move |state| state.trim_durable(trim.through_lsn)))
            }
            RequestBody::DropStream(drop) => (drop.stream, Box::new(move |state| state.drop_stream(drop.epoch))),
            RequestBody::CheckSynced(check) => return self.check_after_durability(&check.stream, admission, |state| state.check_synced(check.epoch)),
            RequestBody::ActivateLoglet(check) => {
                let Ok(ensemble) = check.ensemble.as_ref().try_into() else { return Outcome::Now(failed("invalid ensemble digest")); };
                (check.stream, Box::new(move |state| state.activate(check.epoch, ensemble)))
            }
            RequestBody::ReadIndex(check) => {
                if !self.journal.is_durable() { return Outcome::Now(failed("read indexes require a durable journal")); }
                let Ok(ensemble) = check.ensemble.as_ref().try_into() else { return Outcome::Now(failed("invalid ensemble digest")); };
                return self.check_after_durability(&check.stream, admission, |state| state.read_index(check.epoch, ensemble));
            }
            RequestBody::Read(_) | RequestBody::Fetch(_) | RequestBody::Check(_) | RequestBody::Retire(_) | RequestBody::DigestPage(_) | RequestBody::Identify(_) | RequestBody::ConfirmAuthority(_) => return Outcome::Now(failed("not a direct mutation")),
        };
        let slot = self.slot(&stream);
        let mut state = slot.state.lock();
        let before = (state.commit, state.trim, state.dropped);
        let previous_authority = state.writer_authority();
        let decision = decide(&mut state);
        if matches!(decision.reply, ResponseBody::Refused(_) | ResponseBody::Failed(_)) { return Outcome::Now(decision.reply); }
        let observed_append = observed_lsn.map(|lsn| {
            let sample = Arc::new(OnceLock::new());
            assert!(state.track_sync(lsn, Arc::clone(&sample)), "admitted append holds its new entry");
            (lsn, sample)
        });
        let authority = state.writer_authority();
        let authority_revision = if authority == previous_authority {
            None
        } else {
            slot.authority.lock().record(authority)
        };
        let (done, durable) = oneshot::channel();
        if self.queue.send(Work { write: Write::Events(Arc::clone(&slot), decision.events), done,
            _admission: Some(admission), authority_revision, observed_append }).is_err() {
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
        if disk <= 2 * live + COMPACTION_SLACK_BYTES { return; }
        let _ = self.queue_compaction();
    }

    fn queue_compaction(&self) -> Result<Option<Compaction>, String> {
        if !self.journal.claim_compaction() { return Ok(None); }
        let streams = self.streams.lock();
        let locked: Vec<_> = streams.values().map(|slot| (slot, slot.state.lock())).collect();
        let captured = locked.iter().map(|(slot, state)| Captured { slot: Arc::clone(slot), state: (**state).clone() }).collect();
        let (done, finished) = oneshot::channel();
        if self.queue.send(Work { write: Write::Compact(captured), done, _admission: None, authority_revision: None, observed_append: None }).is_err() {
            self.journal.release_compaction();
            return Err("the journal has stopped".into());
        }
        Ok(Some(finished))
    }

    /// After the fence and trim are durable, wait for a base that contains
    /// neither acknowledged nor uncommitted payloads of the retired stream.
    pub async fn retire(&self, body: crate::protocol::Retire, frame_bytes: usize) -> ResponseBody {
        if !self.journal.is_durable() { return failed("retirement requires a durable journal"); }
        let slot = self.slot(&body.stream);
        let epoch = body.epoch;
        let accepted = match self.mutate(RequestBody::DropStream(crate::protocol::DropStream { stream: body.stream, epoch }), frame_bytes).await {
            Outcome::Now(reply) => return reply,
            Outcome::Durable(durable, reply) => match durable.await {
                Ok(Ok(())) => reply,
                Ok(Err(error)) => return failed(error),
                Err(_) => return failed("the journal has stopped"),
            },
        };
        if !matches!(accepted, ResponseBody::Accepted(_)) { return accepted; }
        loop {
            let released = self.journal.compaction_finished();
            if self.journal.compacted_retirement(slot.id, epoch) { return accepted; }
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

    pub fn confirm_authority(&self, check: LogletCheck) -> ResponseBody {
        if !self.journal.is_durable() || self.queue.is_closed() { return failed("durable writer authority is unavailable"); }
        let Ok(ensemble) = check.ensemble.as_ref().try_into() else { return failed("invalid ensemble digest"); };
        let authority = self.authorities.lock().get(&check.stream).cloned();
        let Some(authority) = authority else { return failed("the authority stream was not found"); };
        let reply = authority.lock().confirm(check.epoch, ensemble);
        reply
    }

    fn check_after_durability(
        &self, stream: &str, admission: tokio::sync::OwnedSemaphorePermit, check: impl FnOnce(&StreamState<Stored>) -> ResponseBody,
    ) -> Outcome {
        let Some(slot) = self.existing(stream) else {
            return Outcome::Now(check(&StreamState::default()));
        };
        let state = slot.state.lock();
        let reply = check(&state);
        if !matches!(reply, ResponseBody::Accepted(_)) { return Outcome::Now(reply); }

        // The reply's position includes accepted mutations that the journal may still be syncing.
        let (done, durable) = oneshot::channel();
        let barrier = Work { write: Write::Events(Arc::clone(&slot), Vec::new()), done, _admission: Some(admission), authority_revision: None, observed_append: None };
        if self.queue.send(barrier).is_err() { return Outcome::Now(failed("the journal has stopped")); }
        Outcome::Durable(durable, reply)
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

    pub async fn digest_page(&self, page: DigestPage) -> ResponseBody {
        if !self.journal.is_durable() { return failed("history proofs require a durable journal"); }
        if page.from_lsn < 0 || page.through_lsn < page.from_lsn {
            return failed("invalid digest interval");
        }
        let mut chain = if page.preceding_digest.is_empty() {
            RetainedChain::new(&page.stream, page.from_lsn)
        } else if let Ok(digest) = <[u8; 32]>::try_from(page.preceding_digest.as_ref()) {
            RetainedChain::from_bytes(digest)
        } else {
            return failed("invalid preceding digest");
        };
        let Ok(_page) = self.digest_pages.try_acquire() else { return failed("history proof capacity exhausted"); };
        let Some(slot) = self.existing(&page.stream) else { return failed("the stream was not found"); };
        let (window, durable, observation) = {
            let state = slot.state.lock();
            if state.dropped || page.from_lsn <= state.trim || page.through_lsn > state.commit {
                return failed("the digest interval is outside the committed retained window");
            }
            let window = self.select(&state, page.from_lsn, page.through_lsn, DIGEST_PAGE_ENTRIES, DIGEST_PAGE_BYTES);
            let observation = (page.from_lsn == page.through_lsn)
                .then(|| state.sync_observation(page.from_lsn)).flatten();
            let (done, durable) = oneshot::channel();
            // A selected commit can still be queued for fsync. The barrier is ordered after that commit.
            if self.queue.send(Work { write: Write::Events(Arc::clone(&slot), Vec::new()), done, _admission: None, authority_revision: None, observed_append: None }).is_err() {
                return failed("the journal has stopped");
            }
            (window, durable, observation)
        };
        if !matches!(durable.await, Ok(Ok(()))) { return failed("the digest interval was not made durable"); }
        let journal = Arc::clone(&self.journal);
        match tokio::task::spawn_blocking(move || {
            let entries = match journal.resolve(window) {
                Ok(entries) => entries.entries,
                Err(error) => return failed(error.to_string()),
            };
            let Some(last) = entries.last() else { return failed("the digest page is empty"); };
            for (offset, entry) in entries.iter().enumerate() {
                if page.from_lsn.checked_add(offset as i64) != Some(entry.lsn) {
                    return failed("the digest page is not contiguous");
                }
                chain.append(entry);
            }
            ResponseBody::PageDigest(PageDigest { through_lsn: last.lsn,
                digest: Bytes::copy_from_slice(&chain.bytes()),
                synced_clock_upper_micros: observation.and_then(|sample| sample.get().copied()) })
        }).await {
            Ok(reply) => reply,
            Err(error) => failed(error.to_string()),
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
    use crate::protocol::{Check, DropStream, Overwrite};
    use crate::server::state::Event;
    use bytes::Bytes;

    #[tokio::test]
    async fn a_full_journal_queue_delays_the_decision_until_capacity_is_available() {
        let directory = tempfile::tempdir().unwrap();
        let (journal, recovered, _) = Journal::open(directory.path(), false, 0, None).unwrap();
        let (queue, mut work) = mpsc::unbounded_channel();
        let store = Store::new(Arc::new(journal), recovered, queue);
        let overwrite = |stream: &str| RequestBody::Overwrite(Overwrite {
            stream: stream.into(), epoch: 1, from_lsn: 0, entries: Vec::new(), complete: true, commit_lsn: -1, activate_ensemble: Bytes::new(),
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
    async fn a_synced_check_waits_for_a_queued_epoch_install() {
        let directory = tempfile::tempdir().unwrap();
        let (journal, recovered, _) = Journal::open(directory.path(), true, 0, None).unwrap();
        let (queue, mut work) = mpsc::unbounded_channel();
        let store = Store::new(Arc::new(journal), recovered, queue);
        let check = || RequestBody::CheckSynced(Check { stream: "pending-sync".into(), epoch: 2 });

        assert!(matches!(store.mutate(check(), 32).await, Outcome::Now(ResponseBody::Refused(_))));
        let incoming = RequestBody::Overwrite(Overwrite {
            stream: "pending-sync".into(), epoch: 2, from_lsn: 0,
            entries: Vec::new(), complete: true, commit_lsn: -1, activate_ensemble: Bytes::new(),
        });
        let Outcome::Durable(installed, ResponseBody::Accepted(_)) = store.mutate(incoming, 32).await else {
            panic!("the complete overwrite must wait for the journal");
        };
        let Outcome::Durable(mut confirmed, ResponseBody::Accepted(_)) = store.mutate(check(), 32).await else {
            panic!("the check must wait behind the overwrite");
        };
        assert!(futures::poll!(&mut confirmed).is_pending());
        let install = work.recv().await.unwrap();
        assert!(matches!(&install.write, Write::Events(_, events) if events.contains(&Event::Synced(Some(2)))));
        install.done.send(Ok(())).unwrap();
        installed.await.unwrap().unwrap();
        let barrier = work.recv().await.unwrap();
        assert!(matches!(&barrier.write, Write::Events(_, events) if events.is_empty()));
        barrier.done.send(Ok(())).unwrap();
        confirmed.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn authority_confirms_only_a_durable_epoch_without_admitting_an_append() {
        let directory = tempfile::tempdir().unwrap();
        let (journal, recovered, _) = Journal::open(directory.path(), true, 0, None).unwrap();
        let journal = Arc::new(journal);
        let (queue, work) = mpsc::unbounded_channel();
        let store = Arc::new(Store::new(Arc::clone(&journal), recovered, queue));
        let check = |ensemble| LogletCheck { stream: "authority".into(), epoch: 2, ensemble };
        let ensemble = Bytes::from_static(&[7; 32]);
        let Outcome::Durable(installed, _) = store.mutate(RequestBody::Overwrite(Overwrite {
            stream: "authority".into(), epoch: 2, from_lsn: 0, entries: Vec::new(),
            complete: true, commit_lsn: -1, activate_ensemble: ensemble.clone(),
        }), 32).await else { panic!("the ensemble needs a journal write"); };
        assert!(matches!(store.confirm_authority(check(ensemble.clone())), ResponseBody::Refused(_)));

        let driver = tokio::spawn(crate::server::journal::run(journal, Arc::downgrade(&store), work));
        installed.await.unwrap().unwrap();
        let all_writes = store.pending_writes.clone().acquire_many_owned(PENDING_WRITE_BYTES as u32).await.unwrap();
        assert!(matches!(store.confirm_authority(check(ensemble)), ResponseBody::AuthorityConfirmed(_)));
        assert!(matches!(store.confirm_authority(check(Bytes::from_static(&[8; 32]))), ResponseBody::Refused(_)));
        drop(all_writes);
        drop(store);
        driver.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_drop_queued_after_an_older_compaction_schedules_reclamation_again() {
        let directory = tempfile::tempdir().unwrap();
        let (journal, recovered, _) = Journal::open(directory.path(), true, 0, None).unwrap();
        let journal = Arc::new(journal);
        let (queue, work) = mpsc::unbounded_channel();
        let store = Arc::new(Store::new(Arc::clone(&journal), recovered, queue));
        assert!(matches!(store.mutate(RequestBody::Overwrite(Overwrite {
            stream: "drop-me".into(),
            epoch: 1,
            from_lsn: 0,
            entries: vec![Entry { lsn: 0, epoch: 1, timestamp: 0, payload: Bytes::from(vec![1; 2 << 20]) }],
            complete: true,
            commit_lsn: 0, activate_ensemble: Bytes::new(),
        }), 2 << 20).await, Outcome::Durable(_, _)));
        let slot = store.existing("drop-me").unwrap();
        let capture = Captured { slot: Arc::clone(&slot), state: slot.state.lock().clone() };
        assert!(journal.claim_compaction());
        let first_compaction = Work { write: Write::Compact(vec![capture]), done: oneshot::channel().0, _admission: None, authority_revision: None, observed_append: None };
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
