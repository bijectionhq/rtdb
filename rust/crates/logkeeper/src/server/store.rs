//! Every stream a server holds, and the order in which their mutations reach the journal.

#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(matches =>
    ensures matches == (certified_digest@ =~= retained_digest@
        && sample.is_some()
        && match origin_stream { Some(stream) => stream@ == requested_stream@, None => false }),
))]
fn original_lookup_matches(
    certified_digest: &[u8], retained_digest: &[u8], sample: Option<i64>,
    origin_stream: Option<&str>, requested_stream: &str,
) -> bool {
    sample.is_some() && retained_digest == certified_digest && match origin_stream {
        Some(stream) => stream == requested_stream,
        None => false,
    }
}

#[cfg(not(verus_keep_ghost))]
use super::journal::{Captured, Journal, ObservedAppend, Recovered, Work, Write};
#[cfg(not(verus_keep_ghost))]
use super::payload::{Item, Source, Stored};
#[cfg(not(verus_keep_ghost))]
use super::state::{Decision, StreamState, Window};
#[cfg(not(verus_keep_ghost))]
use crate::decision::{DecisionCertificate, DecisionVoteRequest, Handoff};
#[cfg(not(verus_keep_ghost))]
use crate::history::RetainedChain;
#[cfg(not(verus_keep_ghost))]
use crate::protocol::{DigestPage, Refused, Entries, Entry, Failed, MAXIMUM_FRAME_BYTES, PageDigest, ReadIndexReply, RequestBody, ResponseBody};
#[cfg(not(verus_keep_ghost))]
use bytes::Bytes;
#[cfg(not(verus_keep_ghost))]
use parking_lot::Mutex;
#[cfg(not(verus_keep_ghost))]
use std::collections::HashMap;
#[cfg(not(verus_keep_ghost))]
use std::sync::{Arc, OnceLock};
#[cfg(not(verus_keep_ghost))]
use std::time::Duration;
#[cfg(not(verus_keep_ghost))]
use tokio::sync::{Notify, Semaphore, mpsc, oneshot};
#[cfg(not(verus_keep_ghost))]
use tokio_util::sync::CancellationToken;

#[cfg(not(verus_keep_ghost))]
const COMPACTION_SLACK_BYTES: u64 = 1 << 20;
#[cfg(not(verus_keep_ghost))]
const PENDING_WRITE_BYTES: usize = MAXIMUM_FRAME_BYTES + 1024;
#[cfg(not(verus_keep_ghost))]
const MINIMUM_WRITE_CHARGE: usize = 1024;
#[cfg(not(verus_keep_ghost))]
const DIGEST_PAGE_ENTRIES: usize = 1024;
#[cfg(not(verus_keep_ghost))]
const DIGEST_PAGE_BYTES: usize = 4 << 20;
#[cfg(not(verus_keep_ghost))]
const CONCURRENT_DIGEST_PAGES: usize = 2;

#[cfg(not(verus_keep_ghost))]
/// A stream's state; a stream with no waiting reader owns no task and no timer.
pub(crate) struct Slot {
    /// Names the stream in the journal's records.
    pub id: u64,
    pub name: String,
    pub state: Mutex<StreamState<Stored>>,
    /// Woken when the commit lsn or trim point moves, or the stream is dropped.
    pub progressed: Notify,
}

#[cfg(not(verus_keep_ghost))]
impl Slot {
    pub fn new(id: u64, name: String, state: StreamState<Stored>) -> Self {
        Self { id, name, state: Mutex::new(state), progressed: Notify::new() }
    }
}

#[cfg(not(verus_keep_ghost))]
pub(crate) struct Store {
    journal: Arc<Journal>,
    streams: Mutex<HashMap<String, Arc<Slot>>>,
    queue: mpsc::UnboundedSender<Work>,
    pending_writes: Arc<Semaphore>,
    digest_pages: Semaphore,
}

#[cfg(not(verus_keep_ghost))]
pub(crate) enum Outcome {
    Now(ResponseBody),
    /// The reply, to be sent once the journal reports the decision durable.
    Durable(oneshot::Receiver<Result<(), String>>, ResponseBody),
}

#[cfg(not(verus_keep_ghost))]
type Compaction = oneshot::Receiver<Result<(), String>>;

#[cfg(not(verus_keep_ghost))]
impl Store {
    pub fn new(journal: Arc<Journal>, recovered: Vec<Recovered>, queue: mpsc::UnboundedSender<Work>) -> Self {
        let streams: HashMap<_, _> = recovered.into_iter().map(|stream| {
            let slot = Slot::new(stream.id, stream.name.clone(), stream.state);
            (stream.name, Arc::new(slot))
        }).collect();
        Self { journal, streams: Mutex::new(streams), queue, pending_writes: Arc::new(Semaphore::new(PENDING_WRITE_BYTES)), digest_pages: Semaphore::new(CONCURRENT_DIGEST_PAGES) }
    }

    pub(super) fn prove_identity(&self, nonce: &[u8; 16]) -> ([u8; 32], [u8; 64]) {
        let signer = &self.journal.signer;
        (signer.public_key(), crate::attestation::sign_challenge(signer, nonce))
    }

    fn slot(&self, name: &str) -> Arc<Slot> {
        let mut streams = self.streams.lock();
        if let Some(slot) = streams.get(name) { return Arc::clone(slot); }
        let slot = Arc::new(Slot::new(self.journal.allocate_id(), name.to_owned(), StreamState::default()));
        streams.insert(name.to_owned(), Arc::clone(&slot));
        slot
    }

    fn existing(&self, name: &str) -> Option<Arc<Slot>> { self.streams.lock().get(name).cloned() }

    /// Reserves journal capacity before deciding the mutation.
    /// The decision and submission share the stream lock, so decisions reach the journal in order.
    pub async fn mutate(&self, body: RequestBody, frame_bytes: usize) -> Outcome {
        let observation = match &body {
            RequestBody::AppendObserved(append) => {
                let [entry] = append.entries.as_slice() else {
                    return Outcome::Now(failed("a guarded sync observation requires one guarded entry"));
                };
                if !self.journal.is_durable() || !entry.guarded || !entry.valid_guard() || append.first_lsn.checked_sub(1) != Some(append.commit_lsn) {
                    return Outcome::Now(failed("a guarded sync observation requires one guarded entry after a committed prefix"));
                }
                if entry.timed_origin.as_ref().is_some_and(|origin| origin.stream != append.stream) {
                    return Outcome::Now(failed("the timed entry must name its original stream"));
                }
                Some((append.first_lsn, entry.guarded_before))
            }
            RequestBody::Append(append) if !append.guarded_ensemble.is_empty() || append.entries.iter().any(|entry| entry.guarded || !entry.valid_guard()) => {
                return Outcome::Now(failed("guarded metadata requires the observed append request"));
            }
            _ => None,
        };
        let charge = frame_bytes.saturating_add(MINIMUM_WRITE_CHARGE).min(PENDING_WRITE_BYTES) as u32;
        let Ok(admission) = self.pending_writes.clone().acquire_many_owned(charge).await else {
            return Outcome::Now(failed("the journal has stopped"));
        };
        if self.queue.is_closed() { return Outcome::Now(failed("the journal has stopped")); }
        let (stream, decide): (String, Box<dyn FnOnce(&mut StreamState<Stored>) -> Decision + '_>) = match body {
            RequestBody::Append(append) => (append.stream, Box::new(move |state| {
                state.append(append.epoch, append.first_lsn, append.entries, append.commit_lsn)
            })),
            RequestBody::AppendObserved(append) => {
                let Ok(ensemble) = append.guarded_ensemble.as_ref().try_into() else {
                    return Outcome::Now(failed("invalid guarded ensemble identity"));
                };
                (append.stream, Box::new(move |state| {
                    state.append_observed(append.epoch, append.first_lsn, append.entries, append.commit_lsn, ensemble)
                }))
            }
            RequestBody::Overwrite(overwrite) if overwrite.activate_ensemble.is_empty() => (overwrite.stream, Box::new(move |state| {
                state.overwrite(overwrite.epoch, overwrite.from_lsn, overwrite.entries, overwrite.complete, overwrite.commit_lsn)
            })),
            RequestBody::Overwrite(overwrite) => {
                let Ok(ensemble) = overwrite.activate_ensemble.as_ref().try_into() else { return Outcome::Now(failed("invalid ensemble digest")); };
                if !overwrite.complete { return Outcome::Now(failed("activation requires a complete overwrite")); }
                (overwrite.stream, Box::new(move |state| state.repair(overwrite.epoch, overwrite.from_lsn, overwrite.entries, overwrite.commit_lsn, ensemble, overwrite.certified)))
            }
            RequestBody::Commit(commit) => {
                let Ok(ensemble) = commit.ensemble.as_ref().try_into() else { return Outcome::Now(failed("invalid decision ensemble")); };
                (commit.stream, Box::new(move |state| state.commit_bound(commit.epoch, commit.commit_lsn, ensemble)))
            }
            RequestBody::Publish(commit) => {
                let Ok(ensemble) = commit.ensemble.as_ref().try_into() else { return Outcome::Now(failed("invalid publication ensemble")); };
                (commit.stream, Box::new(move |state| state.publish(commit.epoch, commit.commit_lsn, ensemble)))
            }
            RequestBody::InstallDecision(certificate) => {
                if !self.journal.is_durable() || !certificate.authentic() {
                    return Outcome::Now(failed("invalid portable decision certificate"));
                }
                let reply = certificate.statement.installation(&self.journal.signer);
                (certificate.statement.scope.stream.clone(), Box::new(move |state| state.install_certificate(&certificate, reply)))
            }
            RequestBody::PublishDecision(proof) => {
                if !self.journal.is_durable() || !proof.authentic() {
                    return Outcome::Now(failed("invalid quorum installation proof"));
                }
                let key = self.journal.signer.public_key();
                let replay = if proof.certificate.votes.iter().any(|vote| vote.key.as_ref() == key) {
                    true
                } else {
                    match self.holds_decision_payload(&proof.certificate).await {
                        Ok(held) => held,
                        Err(error) => return Outcome::Now(failed(error)),
                    }
                };
                (proof.certificate.statement.scope.stream.clone(), Box::new(move |state| state.expose_certificate(proof, replay)))
            }
            RequestBody::Seal(seal) => {
                if seal.nonce.len() != 16 { return Outcome::Now(failed("invalid seal nonce")); }
                let signer = &self.journal.signer;
                (seal.stream.clone(), Box::new(move |state| {
                    let mut decision = state.seal(seal.epoch);
                    if let ResponseBody::Sealed(reply) = &mut decision.reply {
                        reply.sign(&seal, signer);
                    }
                    decision
                }))
            }
            RequestBody::Trim(trim) => (trim.stream, Box::new(move |state| state.trim(trim.through_lsn))),
            RequestBody::TrimDurable(trim) => {
                if !self.journal.is_durable() { return Outcome::Now(failed("durable trim requires a journal with fsync")); }
                (trim.stream, Box::new(move |state| state.trim_durable(trim.through_lsn)))
            }
            RequestBody::DropStream(drop) => (drop.stream, Box::new(move |state| state.drop_stream(drop.epoch))),
            RequestBody::CheckSynced(check) => return self.check_after_durability(&check.stream, admission, |state| state.check_synced(check.epoch)),
            RequestBody::ExportHandoff(handoff) => {
                let journal = crate::attestation::journal_id(&self.journal.signer.public_key()).to_string();
                if !self.journal.is_durable() || !handoff.valid()
                    || handoff.source.journals.binary_search(&journal).is_err()
                    || !self.check_handoff_range(&handoff).await
                {
                    return Outcome::Now(failed("the sealed journal cannot attest this handoff"));
                }
                let receipt = handoff.export(&self.journal.signer);
                (handoff.source.stream.clone(), Box::new(move |state| state.export_handoff(handoff, receipt)))
            }
            RequestBody::ActivateLoglet(check) => {
                let Ok(ensemble) = check.ensemble.as_ref().try_into() else { return Outcome::Now(failed("invalid ensemble digest")); };
                if let Some(proof) = check.handoff {
                    let journal = crate::attestation::journal_id(&self.journal.signer.public_key()).to_string();
                    if !self.journal.is_durable() || !check.certified || !proof.authentic()
                        || proof.handoff.destination.stream != check.stream
                        || proof.handoff.destination.epoch != check.epoch
                        || proof.handoff.destination.ensemble() != Some(ensemble)
                        || proof.handoff.destination.journals.binary_search(&journal).is_err()
                        || !self.check_handoff_range(&proof.handoff).await
                    {
                        return Outcome::Now(failed("the successor cannot install this handoff"));
                    }
                    (check.stream, Box::new(move |state| state.activate_handoff(proof, check.epoch, ensemble)))
                } else {
                    (check.stream, Box::new(move |state| state.activate(check.epoch, ensemble, check.certified)))
                }
            }
            RequestBody::ReadIndex(request) => {
                if !self.journal.is_durable() { return Outcome::Now(failed("read indexes require a durable journal")); }
                let Some(check) = request.loglet else { return Outcome::Now(failed("a read index must name a loglet")); };
                if request.nonce.len() != 16 { return Outcome::Now(failed("invalid read index nonce")); }
                let Ok(ensemble) = check.ensemble.as_ref().try_into() else { return Outcome::Now(failed("invalid ensemble digest")); };
                return self.check_after_durability(&check.stream, admission, |state| {
                    match state.read_index(check.epoch, ensemble, check.certified) {
                        ResponseBody::Accepted(accepted) => {
                            let mut reply = ReadIndexReply {
                            nonce: request.nonce, decided: state.installed, visible: accepted.commit_lsn,
                            certificate: state.certificate.as_ref().filter(|certificate| {
                                certificate.statement.scope.epoch == check.epoch
                                    && certificate.statement.scope.ensemble() == Some(ensemble)
                            }).cloned(),
                            stability: state.stability.as_ref().filter(|proof| {
                                proof.certificate.statement.scope.epoch == check.epoch
                                    && proof.certificate.statement.scope.ensemble() == Some(ensemble)
                            }).cloned(),
                            key: Bytes::new(),
                            signature: Bytes::new(),
                            handoff: state.handoff.as_ref().filter(|proof| {
                                proof.handoff.destination.epoch == check.epoch
                                    && proof.handoff.destination.ensemble() == Some(ensemble)
                            }).cloned(),
                            };
                            reply.sign(&check, &self.journal.signer);
                            ResponseBody::ReadIndex(reply)
                        }
                        other => other,
                    }
                });
            }
            RequestBody::VoteDecision(_) | RequestBody::Read(_) | RequestBody::Fetch(_) | RequestBody::Check(_) | RequestBody::Retire(_) | RequestBody::DigestPage(_) | RequestBody::Identify(_) => return Outcome::Now(failed("not a direct mutation")),
        };
        let slot = self.slot(&stream);
        let mut state = slot.state.lock();
        let before = (state.commit, state.trim, state.dropped);
        let decision = decide(&mut state);
        if matches!(decision.reply, ResponseBody::Refused(_) | ResponseBody::Failed(_)) { return Outcome::Now(decision.reply); }
        let observed_append = observation.map(|(lsn, before)| {
            let sample = Arc::new(OnceLock::new());
            assert!(state.track_sync(lsn, Arc::clone(&sample)), "admitted append holds its new entry");
            ObservedAppend { lsn, sample, before }
        });
        let (done, durable) = oneshot::channel();
        if self.queue.send(Work { write: Write::Events(Arc::clone(&slot), decision.events), done,
            _admission: Some(admission), observed_append }).is_err() {
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
        if self.queue.send(Work { write: Write::Compact(captured), done, _admission: None, observed_append: None }).is_err() {
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

    fn check_after_durability(
        &self, stream: &str, admission: tokio::sync::OwnedSemaphorePermit, check: impl FnOnce(&StreamState<Stored>) -> ResponseBody,
    ) -> Outcome {
        let Some(slot) = self.existing(stream) else {
            return Outcome::Now(check(&StreamState::default()));
        };
        let state = slot.state.lock();
        let reply = check(&state);
        if !matches!(reply, ResponseBody::Accepted(_) | ResponseBody::ReadIndex(_)) { return Outcome::Now(reply); }

        // The reply's position includes accepted mutations that the journal may still be syncing.
        let (done, durable) = oneshot::channel();
        let barrier = Work { write: Write::Events(Arc::clone(&slot), Vec::new()), done, _admission: Some(admission), observed_append: None };
        if self.queue.send(barrier).is_err() { return Outcome::Now(failed("the journal has stopped")); }
        Outcome::Durable(durable, reply)
    }

    /// Selects entries in `[from, to]` and where to find each payload, under the stream's lock.
    fn select(&self, state: &StreamState<Stored>, from: i64, to: i64, max_entries: usize, max_bytes: usize) -> Window<Item> {
        state.window(from, to, max_entries, max_bytes, |lsn, held| Item { lsn, epoch: held.epoch, timestamp: held.timestamp, guarded: held.guarded, guarded_before: held.guarded_before, timed_origin: held.timed_origin.as_deref().cloned(), source: self.journal.source(&held.payload) })
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

    async fn check_handoff_range(&self, transfer: &Handoff) -> bool {
        let Ok(_permit) = self.digest_pages.try_acquire() else { return false; };
        let mut digest = RetainedChain::new(&transfer.source.stream, transfer.first);
        let mut next = transfer.first;
        while next <= transfer.through {
            let Some(slot) = self.existing(&transfer.source.stream) else { return false; };
            let selected = {
                let state = slot.state.lock();
                if state.promised != transfer.destination.epoch || state.synced != Some(state.promised)
                    || state.trim >= next || state.last() < transfer.through
                {
                    return false;
                }
                self.select(&state, next, transfer.through, DIGEST_PAGE_ENTRIES, DIGEST_PAGE_BYTES)
            };
            let journal = Arc::clone(&self.journal);
            let Ok(Ok(entries)) = tokio::task::spawn_blocking(move || journal.resolve(selected)).await else { return false; };
            if entries.entries.is_empty() { return false; }
            for entry in entries.entries {
                if entry.lsn != next { return false; }
                digest.append(&entry);
                next += 1;
            }
        }
        digest.bytes().as_slice() == transfer.digest.as_ref()
    }

    /// Whether this journal holds the certificate's exact cut, read a digest page at a time.
    /// A repaired journal can establish replay from the exact digest. A copy
    /// remains ineligible to supply an original vote for a timed decision.
    /// An `Err` means the cut could not be checked now, which is not a "no".
    async fn holds_decision_payload(&self, certificate: &DecisionCertificate) -> Result<bool, String> {
        let Ok(_permit) = self.digest_pages.try_acquire() else { return Err("decision proof capacity exhausted".into()); };
        let statement = &certificate.statement;
        let cut = &statement.prefix;
        let predecessor = <[u8; 32]>::try_from(cut.predecessor_digest.as_ref()).expect("validated decision digest");
        let mut chain = RetainedChain::from_bytes(predecessor);
        let mut next = cut.predecessor + 1;
        while next <= cut.through {
            let Some(slot) = self.existing(&statement.scope.stream) else { return Ok(false); };
            let selection = {
                let state = slot.state.lock();
                if !state.admits_decision(statement) || next <= state.trim || cut.through > state.last() {
                    return Ok(false);
                }
                self.select(&state, next, cut.through, DIGEST_PAGE_ENTRIES, DIGEST_PAGE_BYTES)
            };
            let journal = Arc::clone(&self.journal);
            let entries = match tokio::task::spawn_blocking(move || journal.resolve(selection)).await {
                Ok(Ok(entries)) => entries.entries,
                Ok(Err(error)) => return Err(format!("reading the decision payload failed: {error}")),
                Err(error) => return Err(format!("reading the decision payload failed: {error}")),
            };
            if entries.is_empty() { return Ok(false); }
            for entry in &entries {
                if entry.lsn != next { return Ok(false); }
                chain.append(entry);
                next += 1;
            }
        }
        Ok(cut.digest.as_ref() == chain.bytes())
    }

    pub async fn vote_decision(&self, request: DecisionVoteRequest) -> ResponseBody {
        let statement = request.statement;
        let prefix = &statement.prefix;
        if !self.journal.is_durable() || !statement.valid() {
            return failed("invalid decision statement");
        }
        let own_id = crate::attestation::journal_id(&self.journal.signer.public_key()).to_string();
        if statement.scope.journals.binary_search(&own_id).is_err() {
            return failed("the receiving journal is outside the decision scope");
        }
        let previous = request.previous;
        if let Some(proof) = &previous {
            let earlier = &proof.certificate.statement;
            if !proof.authentic() || earlier.scope != statement.scope
                || earlier.prefix.through != prefix.predecessor
                || earlier.prefix.digest != prefix.predecessor_digest
            {
                return failed("invalid certified predecessor");
            }
        }
        let Ok(_permit) = self.digest_pages.try_acquire() else { return failed("decision proof capacity exhausted"); };
        let Some(slot) = self.existing(&statement.scope.stream) else { return failed("the decision stream was not found"); };
        let first = prefix.predecessor + 1;
        let (selection, durable, original) = {
            let state = slot.state.lock();
            if state.promised > statement.scope.epoch {
                return ResponseBody::Refused(Refused { promised: state.promised, last_lsn: state.last() });
            }
            let origin = state.handoff.as_ref().filter(|proof| proof.handoff.destination == statement.scope)
                .map_or_else(|| statement.origin_digest(), |proof| proof.handoff.fingerprint());
            if previous.is_none() && prefix.predecessor_digest.as_ref() != origin {
                return failed("invalid activation predecessor digest");
            }
            if !state.admits_decision(&statement)
                || (previous.is_none() && (state.certificate.as_ref().is_some_and(|cut| {
                    cut.statement.scope == statement.scope && cut.statement != statement
                }) || state.commit != prefix.predecessor))
                || first <= state.trim || prefix.through > state.last()
            {
                return failed("the journal does not hold the exact candidate suffix");
            }
            let original = prefix.guarded.then(|| state.sync_observation(prefix.through)).flatten();
            let selection = self.select(&state, first, prefix.through, DIGEST_PAGE_ENTRIES, DIGEST_PAGE_BYTES);
            let (done, durable) = oneshot::channel();
            let work = Work { write: Write::Events(Arc::clone(&slot), Vec::new()), done,
                _admission: None, observed_append: None };
            if self.queue.send(work).is_err() {
                return failed("the journal has stopped");
            }
            (selection, durable, original)
        };
        if !matches!(durable.await, Ok(Ok(()))) {
            return failed("the candidate suffix is unavailable");
        }
        let journal = Arc::clone(&self.journal);
        match tokio::task::spawn_blocking(move || {
            let prefix = &statement.prefix;
            let original_upper = original.as_ref().and_then(|sample| sample.get().copied());
            if prefix.guarded && !original_upper.is_some_and(|upper| crate::protocol::before_guarded_cutoff(upper, prefix.guarded_before)) {
                return failed("the guarded decision requires this journal's timely original observation");
            }
            let Ok(entries) = journal.resolve(selection) else { return failed("could not read the candidate suffix"); };
            if entries.entries.first().map(|entry| entry.lsn) != Some(first)
                || entries.entries.last().map(|entry| entry.lsn) != Some(prefix.through)
            {
                return failed("the candidate suffix exceeds one decision page");
            }
            let mut chain = RetainedChain::from_bytes(prefix.predecessor_digest.as_ref().try_into().expect("validated decision digest"));
            for (ordinal, entry) in entries.entries.iter().enumerate() {
                if first.checked_add(ordinal as i64) != Some(entry.lsn)
                    || entry.epoch != statement.scope.epoch
                    || entry.guarded != prefix.guarded
                    || entry.guarded_before != prefix.guarded_before
                    || entry.timed_origin.as_ref().is_some_and(|origin| {
                        origin.stream != statement.scope.stream || origin.journals != statement.scope.journals
                            || origin.write_quorum != u64::from(statement.scope.quorum)
                            || !origin.original_entry_sha256.is_empty()
                    })
                {
                    return failed("the candidate suffix does not match the decision admission");
                }
                chain.append(entry);
            }
            if prefix.digest.as_ref() != chain.bytes() {
                return failed("the candidate suffix does not match the decision digest");
            }
            ResponseBody::DecisionVote(statement.vote(&journal.signer, original_upper))
        }).await {
            Ok(reply) => reply,
            Err(error) => failed(error.to_string()),
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
        let exact = page.from_lsn == page.through_lsn;
        let lookup_original = !page.certified_entry_digest.is_empty();
        if lookup_original && (!exact || !page.preceding_digest.is_empty() || page.sealed_epoch.is_some() || page.certified_entry_digest.len() != 32) {
            return failed("an original observation requires one certified entry");
        }
        let (window, durable, observation) = {
            let state = slot.state.lock();
            let observation = exact.then(|| state.sync_observation(page.from_lsn)).flatten();
            let admitted = match page.sealed_epoch {
                None if lookup_original => observation.as_ref().is_some_and(|sample| sample.get().is_some()),
                None => page.through_lsn <= state.commit,
                Some(epoch) => exact && page.preceding_digest.is_empty() && state.guarded_at_seal(epoch, page.from_lsn),
            };
            if state.dropped || page.from_lsn <= state.trim || !admitted {
                return failed("the digest interval is outside the requested retained window");
            }
            let window = self.select(&state, page.from_lsn, page.through_lsn, DIGEST_PAGE_ENTRIES, DIGEST_PAGE_BYTES);
            let (done, durable) = oneshot::channel();
            // A selected commit can still be queued for fsync. The barrier is ordered after that commit.
            if self.queue.send(Work { write: Write::Events(Arc::clone(&slot), Vec::new()), done, _admission: None, observed_append: None }).is_err() {
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
            let digest = chain.bytes();
            let timed_origin = exact.then_some(last.timed_origin.as_ref()).flatten();
            let synced_clock_upper_micros = observation.as_ref().and_then(|sample| sample.get().copied());
            if lookup_original && !original_lookup_matches(
                &page.certified_entry_digest, &digest, synced_clock_upper_micros,
                timed_origin.map(|origin| origin.stream.as_str()), &page.stream,
            )
            {
                return failed("this journal does not hold the certified original");
            }
            ResponseBody::PageDigest(PageDigest { through_lsn: last.lsn,
                digest: Bytes::copy_from_slice(&digest),
                original_append: (exact && last.guarded).then_some(observation.is_some()),
                synced_clock_upper_micros,
                guarded_before: exact.then_some(last.guarded_before).flatten(),
                timed_origin_identity: timed_origin.map_or_else(Bytes::new, |origin| Bytes::copy_from_slice(&origin.fingerprint())),
            })
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

#[cfg(not(verus_keep_ghost))]
/// Entries selected under their stream's lock, some of whose payloads may still have to be read from the segments.
pub(crate) struct Selected(Window<Item>);

#[cfg(not(verus_keep_ghost))]
impl Selected {
    /// The reply, if every payload is already in hand.
    pub fn ready(self) -> std::result::Result<ResponseBody, Selected> {
        if self.0.items.iter().any(|item| matches!(item.source, Source::Disk(..))) { return Err(self); }
        let Window { items, commit_lsn, trim_lsn, last_lsn } = self.0;
        let entries = items.into_iter().map(|item| {
            let Source::Ready(payload) = item.source else { unreachable!("checked above") };
            Entry { lsn: item.lsn, epoch: item.epoch, timestamp: item.timestamp, guarded: item.guarded, guarded_before: item.guarded_before, timed_origin: item.timed_origin, payload }
        }).collect();
        Ok(ResponseBody::Entries(Entries { entries, commit_lsn, trim_lsn, last_lsn }))
    }
}

#[cfg(not(verus_keep_ghost))]
pub(crate) fn failed(message: impl Into<String>) -> ResponseBody { ResponseBody::Failed(Failed { message: message.into() }) }

#[cfg(all(test, not(verus_keep_ghost)))]
mod tests {
    use super::*;
    use crate::protocol::{Check, DropStream, Overwrite};
    use crate::server::state::Event;
    use bytes::Bytes;

    fn certified_stream(store: &Store, entries: usize) -> DecisionCertificate {
        use crate::decision::{DecisionPrefix, DecisionScope, DecisionStatement};
        let key = rtdb_storage::attestation::SigningKey::open(&rtdb_storage::attestation::SigningKey::generate().unwrap()).unwrap();
        let scope = DecisionScope {
            stream: "decided".into(), epoch: 1, quorum: 1,
            journals: vec![crate::attestation::journal_id(&key.public_key()).to_string()],
        };
        let entries: Vec<Entry> = (0..entries as i64).map(|lsn| Entry {
            lsn, epoch: 1, timestamp: lsn, guarded: false, guarded_before: None, timed_origin: None, payload: Bytes::from(lsn.to_string()),
        }).collect();
        let predecessor = [3; 32];
        let mut chain = RetainedChain::from_bytes(predecessor);
        for entry in &entries { chain.append(entry); }
        let prefix = DecisionPrefix {
            predecessor: -1, predecessor_digest: Bytes::copy_from_slice(&predecessor),
            through: entries.len() as i64 - 1, digest: Bytes::copy_from_slice(&chain.bytes()), guarded: false, guarded_before: None,
        };
        let ensemble = scope.ensemble().unwrap();
        let slot = store.slot("decided");
        let mut state = slot.state.lock();
        state.seal(1);
        assert!(matches!(state.overwrite(1, 0, entries, true, -1).reply, ResponseBody::Accepted(_)));
        assert!(matches!(state.activate(1, ensemble, true).reply, ResponseBody::Accepted(_)));
        DecisionCertificate { statement: DecisionStatement { scope, prefix }, votes: Vec::new() }
    }

    #[tokio::test]
    async fn a_held_cut_longer_than_one_digest_page_is_eligible_for_replay() {
        let directory = tempfile::tempdir().unwrap();
        let (journal, recovered, _) = Journal::open(directory.path(), false, 0, None).unwrap();
        let store = Store::new(Arc::new(journal), recovered, mpsc::unbounded_channel().0);
        let certificate = certified_stream(&store, DIGEST_PAGE_ENTRIES + 7);
        assert_eq!(store.holds_decision_payload(&certificate).await, Ok(true));

        let mut foreign = certificate;
        foreign.statement.prefix.digest = Bytes::from_static(&[9; 32]);
        assert_eq!(store.holds_decision_payload(&foreign).await, Ok(false));
    }

    #[tokio::test]
    async fn a_cut_that_cannot_be_checked_now_is_an_error_not_a_refusal() {
        let directory = tempfile::tempdir().unwrap();
        let (journal, recovered, _) = Journal::open(directory.path(), false, 0, None).unwrap();
        let store = Store::new(Arc::new(journal), recovered, mpsc::unbounded_channel().0);
        let certificate = certified_stream(&store, 3);
        let busy = store.digest_pages.try_acquire_many(CONCURRENT_DIGEST_PAGES as u32).unwrap();
        assert!(store.holds_decision_payload(&certificate).await.is_err());
        drop(busy);
        assert_eq!(store.holds_decision_payload(&certificate).await, Ok(true));
    }

    #[tokio::test]
    async fn a_full_journal_queue_delays_the_decision_until_capacity_is_available() {
        let directory = tempfile::tempdir().unwrap();
        let (journal, recovered, _) = Journal::open(directory.path(), false, 0, None).unwrap();
        let (queue, mut work) = mpsc::unbounded_channel();
        let store = Store::new(Arc::new(journal), recovered, queue);
        let overwrite = |stream: &str| RequestBody::Overwrite(Overwrite {
            stream: stream.into(), epoch: 1, from_lsn: 0, entries: Vec::new(), complete: true, commit_lsn: -1, activate_ensemble: Bytes::new(), certified: false,
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
            entries: Vec::new(), complete: true, commit_lsn: -1, activate_ensemble: Bytes::new(), certified: false,
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
            entries: vec![Entry { lsn: 0, epoch: 1, timestamp: 0, guarded: false, guarded_before: None, timed_origin: None, payload: Bytes::from(vec![1; 2 << 20]) }],
            complete: true,
            commit_lsn: 0, activate_ensemble: Bytes::new(), certified: false,
        }), 2 << 20).await, Outcome::Durable(_, _)));
        let slot = store.existing("drop-me").unwrap();
        let capture = Captured { slot: Arc::clone(&slot), state: slot.state.lock().clone() };
        assert!(journal.claim_compaction());
        let first_compaction = Work { write: Write::Compact(vec![capture]), done: oneshot::channel().0, _admission: None, observed_append: None };
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
