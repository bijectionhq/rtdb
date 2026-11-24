//! One stream's state on one server, as a pure state machine.
//! A mutation is decided against the accepted state, which already includes every earlier decision of the stream, and yields the events to persist and the reply to send once they are durable.
//! Replay applies the same events on restart.
//! An entry's payload is held as a `Payload`: the bytes themselves, or a handle the server resolves to them; the machine decides on lengths alone.

#[cfg(any(test, feature = "simulation"))]
use crate::protocol::Entries;
use crate::decision::{DecisionCertificate, DecisionInstallation, DecisionStatement, Handoff, HandoffCertificate, StableDecision};
use crate::protocol::{Accepted, Entry, EpochRun, Refused, ResponseBody, Sealed, TimedJournalOrigin};
use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::{Arc, OnceLock};

const RETAINED_ENTRY_OVERHEAD: usize = 32;
const SYNC_OBSERVATION_OVERHEAD: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Promise(u64),
    Synced(Option<u64>),
    Activate { epoch: u64, ensemble: [u8; 32], certified: bool },
    /// Drops every entry at or above the lsn.
    Truncate(i64),
    Append(Entry),
    Install(i64),
    InstallCertificate(DecisionCertificate),
    ExportHandoff(Handoff),
    InstallHandoff(HandoffCertificate),
    ExposeCertificate { proof: StableDecision, replay: bool },
    Commit(i64),
    Trim(i64),
    Drop(u64),
}

/// How a held entry's payload is kept.
pub trait Payload: Clone {
    /// Holds a payload just decided, before anything is written.
    fn decided(payload: &Bytes) -> Self;
    fn bytes(&self) -> usize;
}

impl Payload for Bytes {
    fn decided(payload: &Bytes) -> Self { payload.clone() }
    fn bytes(&self) -> usize { self.len() }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Held<P = Bytes> {
    pub epoch: u64,
    pub timestamp: i64,
    pub guarded: bool,
    pub guarded_before: Option<i64>,
    pub timed_origin: Option<Box<TimedJournalOrigin>>,
    pub payload: P,
    pub(super) observed_sync: Option<Arc<OnceLock<i64>>>,
}

impl<P: Payload> Held<P> {
    fn retained_origin_bytes(&self) -> usize {
        self.timed_origin.as_deref().map_or(0, |origin| {
            <TimedJournalOrigin as prost::Message>::encoded_len(origin) + 8
        })
    }

    fn retained_size(&self) -> usize {
        self.payload.bytes() + RETAINED_ENTRY_OVERHEAD + self.retained_origin_bytes()
            + usize::from(self.observed_sync.is_some()) * SYNC_OBSERVATION_OVERHEAD
    }
}

/// Entries selected by `StreamState::window`, with the stream's positions as of the selection.
pub struct Window<T> {
    pub items: Vec<T>,
    pub commit_lsn: i64,
    pub trim_lsn: i64,
    pub last_lsn: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamState<P = Bytes> {
    pub promised: u64,
    pub dropped: bool,
    /// The epoch whose writer last brought this server up to that writer's whole history.
    pub synced: Option<u64>,
    pub(super) active_ensemble: Option<[u8; 32]>,
    pub(super) certified: bool,
    pub(super) last_ensemble: Option<[u8; 32]>,
    pub(super) export: Option<Handoff>,
    pub(super) handoff: Option<HandoffCertificate>,
    /// Entries at or below this lsn are dropped.
    pub trim: i64,
    pub commit: i64,
    pub(super) installed: i64,
    pub(super) certificate: Option<DecisionCertificate>,
    pub(super) stability: Option<StableDecision>,
    /// Entry `i` has lsn `trim + 1 + i`.
    pub entries: VecDeque<Held<P>>,
    pub(super) epoch_runs: VecDeque<EpochRun>,
    pub(super) retained_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct WriterAuthority {
    pub(super) promised: u64,
    synced: Option<u64>,
    active_ensemble: Option<[u8; 32]>,
    dropped: bool,
}

impl WriterAuthority {
    pub(super) fn confirms(self, epoch: u64, ensemble: [u8; 32]) -> bool {
        !self.dropped && self.promised == epoch && self.synced == Some(epoch)
            && self.active_ensemble == Some(ensemble)
    }
}

pub struct Decision {
    pub events: Vec<Event>,
    pub reply: ResponseBody,
}

fn invalid(message: impl Into<String>) -> Decision {
    Decision { events: Vec::new(), reply: ResponseBody::Failed(crate::protocol::Failed { message: message.into() }) }
}

fn dense(first: i64, entries: &[Entry]) -> bool {
    entries.iter().zip(first..).all(|(entry, lsn)| entry.lsn == lsn)
}

impl<P> Default for StreamState<P> {
    fn default() -> Self {
        Self { promised: 0, dropped: false, synced: None, active_ensemble: None, certified: false, last_ensemble: None, export: None, handoff: None, trim: -1, commit: -1, installed: -1, certificate: None, stability: None, entries: VecDeque::new(), epoch_runs: VecDeque::new(), retained_bytes: 0 }
    }
}

impl<P: Payload> StreamState<P> {
    pub(super) fn writer_authority(&self) -> WriterAuthority {
        WriterAuthority { promised: self.promised, synced: self.synced, active_ensemble: self.active_ensemble, dropped: self.dropped }
    }

    pub fn last(&self) -> i64 { self.trim + self.entries.len() as i64 }

    pub fn retained_bytes(&self) -> usize { self.retained_bytes }

    pub(super) fn append_held(&mut self, lsn: i64, held: Held<P>) {
        debug_assert_eq!(lsn, self.last() + 1);
        if self.epoch_runs.back().is_none_or(|run| run.epoch != held.epoch) {
            self.epoch_runs.push_back(EpochRun { start_lsn: lsn, epoch: held.epoch });
        }
        self.retained_bytes += held.retained_size();
        self.entries.push_back(held);
    }

    pub fn held_mut(&mut self, lsn: i64) -> Option<&mut Held<P>> {
        if lsn <= self.trim { return None; }
        self.entries.get_mut((lsn - self.trim - 1) as usize)
    }

    pub(super) fn held(&self, lsn: i64) -> Option<&Held<P>> {
        if lsn <= self.trim { return None; }
        self.entries.get((lsn - (self.trim + 1)) as usize)
    }

    pub(super) fn guarded_at_seal(&self, epoch: u64, lsn: i64) -> bool {
        self.promised == epoch && self.held(lsn).is_some_and(|held| held.guarded && held.epoch < epoch)
    }

    pub(super) fn sync_observation(&self, lsn: i64) -> Option<Arc<OnceLock<i64>>> {
        self.held(lsn)?.observed_sync.clone()
    }

    pub(super) fn track_sync(&mut self, lsn: i64, observation: Arc<OnceLock<i64>>) -> bool {
        let Some(held) = self.held_mut(lsn).filter(|held| held.guarded && held.observed_sync.is_none()) else { return false; };
        held.observed_sync = Some(observation);
        self.retained_bytes += SYNC_OBSERVATION_OVERHEAD;
        true
    }

    pub fn apply(&mut self, event: &Event) {
        match event {
            Event::Promise(epoch) => {
                if *epoch > self.promised {
                    self.promised = *epoch;
                    self.active_ensemble = None;
                    self.certified = false;
                }
            }
            Event::Synced(epoch) => self.synced = *epoch,
            Event::Activate { epoch, ensemble, certified } => {
                debug_assert_eq!(*epoch, self.promised);
                self.active_ensemble = Some(*ensemble);
                self.last_ensemble = Some(*ensemble);
                self.certified = *certified;
            }
            Event::Truncate(from) => {
                let keep = (from - self.trim - 1).clamp(0, self.entries.len() as i64);
                self.retained_bytes -= self.entries.drain(keep as usize..).map(|held| held.retained_size()).sum::<usize>();
                let last = self.last();
                while self.epoch_runs.back().is_some_and(|run| run.start_lsn > last) { self.epoch_runs.pop_back(); }
                self.commit = self.commit.min(last);
                self.installed = self.installed.min(last);
                if let Some(certificate) = &self.certificate {
                    self.installed = self.installed.max(certificate.statement.prefix.through);
                }
            }
            Event::Append(entry) => {
                self.append_held(entry.lsn, Held { epoch: entry.epoch, timestamp: entry.timestamp,
                    guarded: entry.guarded, guarded_before: entry.guarded_before, timed_origin: entry.timed_origin.clone().map(Box::new),
                    payload: P::decided(&entry.payload), observed_sync: None });
            }
            Event::Install(lsn) => self.installed = self.installed.max((*lsn).min(self.last())),
            Event::ExportHandoff(handoff) => self.export = Some(handoff.clone()),
            Event::InstallHandoff(handoff) => self.handoff = Some(handoff.clone()),
            Event::InstallCertificate(certificate) => {
                self.installed = self.installed.max(certificate.statement.prefix.through);
                self.certificate = Some(certificate.clone());
            }
            Event::ExposeCertificate { proof, replay } => {
                let through = proof.certificate.statement.prefix.through;
                self.stability = Some(proof.clone());
                if *replay && self.last() >= through {
                    self.commit = self.commit.max(through);
                }
            }
            Event::Commit(lsn) => {
                self.commit = self.commit.max((*lsn).min(self.last()));
                self.installed = self.installed.max(self.commit);
            }
            Event::Trim(through) => {
                if *through <= self.trim { return; }
                if *through >= self.last() {
                    if *through > self.last() { self.synced = None; }
                    self.entries.clear();
                    self.epoch_runs.clear();
                    self.retained_bytes = 0;
                } else {
                    self.retained_bytes -= self.entries.drain(..(through - self.trim) as usize).map(|held| held.retained_size()).sum::<usize>();
                    let first = through + 1;
                    while self.epoch_runs.get(1).is_some_and(|run| run.start_lsn <= first) { self.epoch_runs.pop_front(); }
                    self.epoch_runs.front_mut().expect("retained entries have an epoch").start_lsn = first;
                }
                self.trim = *through;
                self.commit = self.commit.max(*through);
                self.installed = self.installed.max(*through);
            }
            Event::Drop(epoch) => {
                *self = Self { promised: self.promised.max(*epoch), dropped: true, ..Self::default() };
            }
        }
    }

    fn persist(&mut self, events: Vec<Event>, reply: impl FnOnce(&Self) -> ResponseBody) -> Decision {
        for event in &events { self.apply(event); }
        Decision { reply: reply(self), events }
    }

    fn accepted(&self) -> ResponseBody { ResponseBody::Accepted(Accepted { last_lsn: self.last(), commit_lsn: self.commit }) }

    fn refused(&self) -> ResponseBody { ResponseBody::Refused(Refused { promised: self.promised, last_lsn: self.last() }) }

    /// Refuses a writer that is not the one this server is synced to.
    fn check_writer(&self, epoch: u64) -> Option<ResponseBody> {
        (self.dropped || self.promised != epoch || self.synced != Some(epoch)).then(|| self.refused())
    }

    fn commit_event(&self, commit: i64, last: i64) -> Option<Event> {
        (commit.min(last) > self.commit).then_some(Event::Commit(commit))
    }

    pub fn append(&mut self, epoch: u64, first: i64, entries: Vec<Entry>, commit: i64) -> Decision {
        if let Some(refusal) = self.check_writer(epoch) { return Decision { events: Vec::new(), reply: refusal }; }
        if first != self.last() + 1 { return Decision { events: Vec::new(), reply: self.refused() }; }
        if !dense(first, &entries) || entries.iter().any(|entry| entry.epoch != epoch) {
            return invalid("an append's entries must be dense from its first lsn and carry its epoch");
        }
        let last = first + entries.len() as i64 - 1;
        let commit = if self.certified { None } else { self.commit_event(commit, last) };
        let events = entries.into_iter().map(Event::Append).chain(commit).collect();
        self.persist(events, Self::accepted)
    }

    pub(super) fn append_observed(&mut self, epoch: u64, first: i64, entries: Vec<Entry>, commit: i64, ensemble: [u8; 32]) -> Decision {
        if !self.writer_authority().confirms(epoch, ensemble) {
            return Decision { events: Vec::new(), reply: self.refused() };
        }
        if entries.iter().any(|entry| !entry.valid_guard()
            || entry.timed_origin.as_ref().is_some_and(|origin| origin.ensemble_identity() != ensemble
                || !origin.original_entry_sha256.is_empty()))
        {
            return invalid("the timed entry must name the activated original ensemble");
        }
        self.append(epoch, first, entries, commit)
    }

    pub fn overwrite(&mut self, epoch: u64, from: i64, entries: Vec<Entry>, complete: bool, commit: i64) -> Decision {
        if epoch == 0 { return invalid("epochs start at 1"); }
        if self.promised > epoch { return Decision { events: Vec::new(), reply: self.refused() }; }
        if self.dropped { return invalid("the stream was dropped"); }
        if from <= self.trim || from > self.last() + 1 || from <= self.commit {
            return invalid(format!("an overwrite at {from} must start above trim {} and commit {} and at most one past last {}", self.trim, self.commit, self.last()));
        }
        if !dense(from, &entries) || entries.iter().any(|entry| entry.epoch > epoch || !entry.valid_guard()) {
            return invalid("an overwrite's entries must be dense from its first lsn and no later than its epoch");
        }
        // A writer's own overwrites never cut what it placed, so one that would is a request delivered after newer ones: deciding it would drop entries those may have committed.
        let own = |held: &Held<P>| held.epoch == epoch;
        if from <= self.last() && (self.synced == Some(epoch) || self.entries.iter().skip((from - self.trim - 1) as usize).any(own)) {
            return invalid(format!("an overwrite at epoch {epoch} from {from} would drop entries its own writer placed, up to {}", self.last()));
        }
        // The same entry already held here is never repaired by replacing it.
        // That would turn this journal's original append into a copy.
        if from <= self.last() && entries.iter().any(|entry| {
            self.held(entry.lsn).is_some_and(|held| held.epoch == entry.epoch && held.observed_sync.is_some())
        }) {
            return invalid("an overwrite would replace the original guarded append with a copy");
        }
        let mut events = Vec::new();
        if epoch > self.promised { events.push(Event::Promise(epoch)); }
        if from <= self.last() { events.push(Event::Truncate(from)); }
        let last = from + entries.len() as i64 - 1;
        events.extend(entries.into_iter().map(Event::Append));
        // A partial copy keeps the epoch the server was synced at: it still holds everything it held of that writer's committed history, so recovery must still count it as that writer's witness.
        if complete && self.synced != Some(epoch) { events.push(Event::Synced(Some(epoch))); }
        if complete && !self.certified { events.extend(self.commit_event(commit, last)); }
        self.persist(events, Self::accepted)
    }

    pub fn commit(&mut self, epoch: u64, commit: i64) -> Decision {
        if let Some(refusal) = self.check_writer(epoch) { return Decision { events: Vec::new(), reply: refusal }; }
        let events = if self.active_ensemble.is_some() {
            (commit.min(self.last()) > self.installed).then_some(Event::Install(commit))
        } else {
            self.commit_event(commit, self.last())
        };
        self.persist(events.into_iter().collect(), |state| {
            ResponseBody::Accepted(Accepted { last_lsn: state.last(), commit_lsn: state.installed })
        })
    }

    pub(super) fn commit_bound(&mut self, epoch: u64, through: i64, ensemble: [u8; 32]) -> Decision {
        if self.certified || self.active_ensemble.is_some_and(|active| active != ensemble) {
            return Decision { events: Vec::new(), reply: self.refused() };
        }
        self.commit(epoch, through)
    }

    pub(super) fn publish(&mut self, epoch: u64, through: i64, ensemble: [u8; 32]) -> Decision {
        if let Some(refusal) = self.check_writer(epoch) { return Decision { events: Vec::new(), reply: refusal }; }
        if self.certified || self.active_ensemble != Some(ensemble) || self.installed < through {
            return Decision { events: Vec::new(), reply: self.refused() };
        }
        let events = self.commit_event(through, self.last()).into_iter().collect();
        self.persist(events, Self::accepted)
    }

    pub(super) fn admits_decision(&self, statement: &DecisionStatement) -> bool {
        self.certified && statement.scope.ensemble().is_some_and(|ensemble| {
            self.writer_authority().confirms(statement.scope.epoch, ensemble)
        })
    }

    fn certificate_event(&self, certificate: &DecisionCertificate) -> Result<Option<Event>, ()> {
        let statement = &certificate.statement;
        if !self.admits_decision(statement) {
            return Err(());
        }
        if let Some(current) = self.certificate.as_ref()
            && current.statement.scope == statement.scope
        {
            let old = current.statement.prefix.through;
            let next = statement.prefix.through;
            if old == next && current.statement != *statement {
                return Err(());
            }
            if old >= next {
                return Ok(None);
            }
        }
        Ok(Some(Event::InstallCertificate(certificate.clone())))
    }

    pub(super) fn install_certificate(&mut self, certificate: &DecisionCertificate, reply: DecisionInstallation) -> Decision {
        let Ok(event) = self.certificate_event(certificate) else {
            return Decision { events: Vec::new(), reply: self.refused() };
        };
        self.persist(event.into_iter().collect(), |_| ResponseBody::DecisionInstallation(reply))
    }

    pub(super) fn expose_certificate(&mut self, proof: StableDecision, replay: bool) -> Decision {
        let Ok(event) = self.certificate_event(&proof.certificate) else {
            return Decision { events: Vec::new(), reply: self.refused() };
        };
        if let Some(current) = &self.stability
            && current.certificate.statement.scope == proof.certificate.statement.scope
        {
            let old = current.certificate.statement.prefix.through;
            let next = proof.certificate.statement.prefix.through;
            if old == next && current.certificate.statement != proof.certificate.statement {
                return Decision { events: Vec::new(), reply: self.refused() };
            }
            if old > next || (old == next && self.commit >= next) {
                return Decision { events: Vec::new(), reply: self.accepted() };
            }
        }
        self.persist(event.into_iter().chain([Event::ExposeCertificate { proof, replay }]).collect(), Self::accepted)
    }

    pub fn repair(&mut self, epoch: u64, from: i64, entries: Vec<Entry>, commit: i64, ensemble: [u8; 32], certified: bool) -> Decision {
        if self.last_ensemble.is_some_and(|last| last != ensemble)
            || self.promised == epoch && self.active_ensemble.is_some_and(|active| active != ensemble || self.certified != certified)
        {
            return Decision { events: Vec::new(), reply: self.refused() };
        }
        let mut overwrite = self.overwrite(epoch, from, entries, true, commit);
        if matches!(overwrite.reply, ResponseBody::Accepted(_)) {
            overwrite.events.extend(self.activate(epoch, ensemble, certified).events);
        }
        overwrite
    }

    pub fn seal(&mut self, epoch: u64) -> Decision {
        if epoch == 0 { return invalid("epochs start at 1"); }
        if self.dropped { return invalid("the stream was dropped"); }
        let events = (epoch > self.promised).then_some(Event::Promise(epoch)).into_iter().collect();
        self.persist(events, Self::sealed)
    }

    pub fn trim(&mut self, through: i64) -> Decision {
        if self.dropped { return invalid("the stream was dropped"); }
        let events = (through > self.trim).then_some(Event::Trim(through)).into_iter().collect();
        self.persist(events, Self::accepted)
    }

    pub fn trim_durable(&mut self, through: i64) -> Decision {
        if self.dropped { return invalid("the stream was dropped"); }
        // A no-op decision produces no journal bytes to sync.
        self.persist(vec![Event::Trim(self.trim.max(through))], Self::accepted)
    }

    pub fn check(&self, epoch: u64) -> ResponseBody {
        if self.promised > epoch { self.refused() }
        else if self.dropped { invalid("the stream was dropped").reply }
        else { self.accepted() }
    }

    pub fn check_synced(&self, epoch: u64) -> ResponseBody {
        self.check_writer(epoch).unwrap_or_else(|| self.accepted())
    }

    pub fn activate(&mut self, epoch: u64, ensemble: [u8; 32], certified: bool) -> Decision {
        if self.last_ensemble.is_some_and(|last| last != ensemble)
            || self.active_ensemble.is_some_and(|active| active != ensemble || self.certified != certified) || self.check_writer(epoch).is_some() {
            return Decision { events: Vec::new(), reply: self.refused() };
        }
        let events = self.active_ensemble.is_none().then_some(Event::Activate { epoch, ensemble, certified }).into_iter().collect();
        self.persist(events, Self::accepted)
    }

    pub(super) fn export_handoff(&mut self, handoff: Handoff, vote: DecisionInstallation) -> Decision {
        let epoch = handoff.source.epoch;
        let source = handoff.source.ensemble();
        if self.check_writer(epoch).is_some() || self.active_ensemble.is_some()
            || (self.last_ensemble != source && !(self.last_ensemble.is_none() && handoff.through == -1))
            || self.last() != handoff.through || self.trim >= handoff.first || self.installed > handoff.through
            || self.export.as_ref().is_some_and(|old| old.source.epoch == epoch && old != &handoff)
            || self.certificate.as_ref().is_some_and(|current| {
                current.statement.scope.ensemble() == source && handoff.decided.as_ref().is_none_or(|selected| {
                    current.statement.prefix.through > selected.statement.prefix.through
                        || (current.statement.prefix.through == selected.statement.prefix.through && current.statement != selected.statement)
                })
            })
        {
            return Decision { events: Vec::new(), reply: self.refused() };
        }
        let events = (self.export.as_ref() != Some(&handoff)).then_some(Event::ExportHandoff(handoff));
        self.persist(events.into_iter().collect(), |_| ResponseBody::DecisionInstallation(vote))
    }

    pub(super) fn activate_handoff(&mut self, proof: HandoffCertificate, epoch: u64, ensemble: [u8; 32]) -> Decision {
        let handoff = &proof.handoff;
        if self.check_writer(epoch).is_some() || handoff.destination.epoch != epoch
            || handoff.destination.ensemble() != Some(ensemble) || self.last() < handoff.through
            || self.commit < handoff.through
            || self.active_ensemble.is_some_and(|active| active != ensemble || !self.certified)
            || self.handoff.as_ref().is_some_and(|old| {
                old.handoff.destination.epoch == epoch && old.handoff != *handoff
            })
        {
            return Decision { events: Vec::new(), reply: self.refused() };
        }
        let events = if self.handoff.as_ref().is_some_and(|old| old.handoff == *handoff) {
            Vec::new()
        } else {
            vec![Event::InstallHandoff(proof), Event::Activate { epoch, ensemble, certified: true }]
        };
        self.persist(events, Self::accepted)
    }

    pub fn read_index(&self, epoch: u64, ensemble: [u8; 32], certified: bool) -> ResponseBody {
        if self.active_ensemble == Some(ensemble) && self.certified == certified { self.check_synced(epoch) } else { self.refused() }
    }

    pub fn drop_stream(&mut self, epoch: u64) -> Decision {
        if epoch == 0 { return invalid("epochs start at 1"); }
        if self.promised > epoch { return Decision { events: Vec::new(), reply: self.refused() }; }
        let events = (!self.dropped).then_some(Event::Drop(epoch)).into_iter().collect();
        self.persist(events, Self::accepted)
    }

    pub fn sealed(&self) -> ResponseBody {
        ResponseBody::Sealed(Sealed {
            promised: self.promised,
            synced: self.synced.unwrap_or(0),
            trim_lsn: self.trim,
            last_lsn: self.last(),
            commit_lsn: self.installed,
            runs: self.runs(),
            guarded_tail: self.entries.back().is_some_and(|entry| entry.guarded),
            certificate: self.certificate.clone(),
            stability: self.stability.clone(),
            key: Bytes::new(),
            signature: Bytes::new(),
        })
    }

    pub fn runs(&self) -> Vec<EpochRun> { self.epoch_runs.iter().copied().collect() }

    /// Entries in `[from, to]`, the first always included when present, then while within the limits.
    pub fn window<T>(&self, from: i64, to: i64, max_entries: usize, max_bytes: usize, mut item: impl FnMut(i64, &Held<P>) -> T) -> Window<T> {
        let mut items = Vec::new();
        if from > self.trim {
            let mut bytes = 0usize;
            let skip = (from - self.trim - 1) as usize;
            for (held, lsn) in self.entries.iter().skip(skip).zip(from..=to).take(max_entries) {
                bytes = bytes.saturating_add(held.payload.bytes()).saturating_add(held.retained_origin_bytes());
                if bytes > max_bytes && !items.is_empty() { break; }
                items.push(item(lsn, held));
            }
        }
        Window { items, commit_lsn: self.commit, trim_lsn: self.trim, last_lsn: self.last() }
    }
}

#[cfg(any(test, feature = "simulation"))]
impl StreamState<Bytes> {
    pub fn entries(&self, from: i64, to: i64, max_entries: usize, max_bytes: usize) -> Entries {
        let window = self.window(from, to, max_entries, max_bytes, |lsn, held| Entry { lsn, epoch: held.epoch, timestamp: held.timestamp, guarded: held.guarded, guarded_before: held.guarded_before, timed_origin: held.timed_origin.as_deref().cloned(), payload: held.payload.clone() });
        Entries { entries: window.items, commit_lsn: window.commit_lsn, trim_lsn: window.trim_lsn, last_lsn: window.last_lsn }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(lsn: i64, epoch: u64) -> Entry { Entry { lsn, epoch, timestamp: lsn, guarded: false, guarded_before: None, timed_origin: None, payload: Bytes::from(format!("{lsn}@{epoch}")) } }

    fn synced(epoch: u64) -> StreamState {
        let mut state = <StreamState>::default();
        state.seal(epoch);
        state.overwrite(epoch, 0, Vec::new(), true, -1);
        state
    }

    fn reply(decision: Decision) -> ResponseBody { decision.reply }

    #[test]
    fn appends_need_a_synced_writer_at_the_promised_epoch() {
        let mut state = <StreamState>::default();
        assert_eq!(reply(state.append(1, 0, vec![entry(0, 1)], -1)), ResponseBody::Refused(Refused { promised: 0, last_lsn: -1 }));
        state.seal(1);
        assert!(matches!(reply(state.append(1, 0, vec![entry(0, 1)], -1)), ResponseBody::Refused(_)), "sealed is not synced");
        state.overwrite(1, 0, Vec::new(), true, -1);
        assert_eq!(reply(state.append(1, 0, vec![entry(0, 1), entry(1, 1)], -1)), ResponseBody::Accepted(Accepted { last_lsn: 1, commit_lsn: -1 }));
        assert_eq!(reply(state.append(1, 3, vec![entry(3, 1)], -1)), ResponseBody::Refused(Refused { promised: 1, last_lsn: 1 }), "a gap");
        assert_eq!(reply(state.append(1, 1, vec![entry(1, 1)], -1)), ResponseBody::Refused(Refused { promised: 1, last_lsn: 1 }), "a repeat");
    }

    #[test]
    fn an_observed_append_requires_this_ensemble_at_the_current_activation() {
        let mut state = synced(1);
        let guarded = Entry { guarded: true, ..entry(0, 1) };
        assert!(matches!(reply(state.append_observed(1, 0, vec![guarded.clone()], -1, [1; 32])), ResponseBody::Refused(_)));
        state.activate(1, [1; 32], false);
        assert!(matches!(reply(state.append_observed(1, 0, vec![guarded.clone()], -1, [2; 32])), ResponseBody::Refused(_)));
        assert!(matches!(reply(state.append_observed(1, 0, vec![guarded.clone()], -1, [1; 32])), ResponseBody::Accepted(_)));
        state.seal(2);
        let next = Entry { lsn: 1, ..guarded };
        assert!(matches!(reply(state.append_observed(1, 1, vec![next], 0, [1; 32])), ResponseBody::Refused(Refused { promised: 2, .. })));
    }

    #[test]
    fn a_seal_deposes_the_previous_writer_and_reports_epochs_per_lsn() {
        let mut state = synced(1);
        state.append(1, 0, vec![entry(0, 1), entry(1, 1)], 0);
        let ResponseBody::Sealed(sealed) = reply(state.seal(2)) else { panic!() };
        assert_eq!((sealed.promised, sealed.synced, sealed.last_lsn, sealed.commit_lsn), (2, 1, 1, 0));
        assert_eq!(sealed.runs, vec![EpochRun { start_lsn: 0, epoch: 1 }]);
        assert_eq!(reply(state.append(1, 2, vec![entry(2, 1)], 1)), ResponseBody::Refused(Refused { promised: 2, last_lsn: 1 }));
        assert_eq!(state.seal(1).events, Vec::new(), "a lower seal promises nothing");
        assert_eq!(state.promised, 2);
    }

    #[test]
    fn epoch_runs_and_retained_bytes_follow_every_tail_change() {
        let mut state = <StreamState>::default();
        let events = (0..10).map(|lsn| Event::Append(entry(lsn, 1 + lsn as u64 / 3)))
            .chain([Event::Trim(1), Event::Trim(5), Event::Truncate(9), Event::Truncate(8), Event::Append(entry(8, 8)),
                Event::Trim(8), Event::Append(entry(9, 9)), Event::Trim(11), Event::Append(entry(12, 10)), Event::Drop(11)]);
        for event in events {
            state.apply(&event);
            let actual_runs = crate::protocol::runs_of(state.entries.iter().zip(state.trim + 1..).map(|(held, lsn)| (lsn, held.epoch)));
            let actual_bytes = state.entries.iter().map(|held| held.payload.len() + RETAINED_ENTRY_OVERHEAD).sum::<usize>();
            assert_eq!(state.runs(), actual_runs, "{event:?}");
            assert_eq!(state.retained_bytes(), actual_bytes, "{event:?}");
        }
    }

    #[test]
    fn an_overwrite_truncates_an_uncommitted_suffix_and_syncs_at_its_epoch_only_when_complete() {
        let mut state = synced(1);
        state.append(1, 0, vec![entry(0, 1), entry(1, 1), entry(2, 1)], 0);
        state.seal(2);
        assert!(matches!(reply(state.overwrite(2, 0, vec![entry(0, 2)], true, -1)), ResponseBody::Failed(_)), "never below commit");
        assert!(matches!(reply(state.overwrite(1, 1, Vec::new(), true, -1)), ResponseBody::Refused(_)), "never from a deposed writer");
        state.overwrite(2, 1, vec![entry(1, 1)], false, 1);
        assert_eq!((state.last(), state.synced, state.commit), (1, Some(1), 0), "a partial copy commits nothing and keeps the epoch the server was synced at");
        state.overwrite(2, 2, vec![entry(2, 2)], true, 2);
        assert_eq!((state.last(), state.synced, state.commit), (2, Some(2), 2));
        assert_eq!(state.runs(), vec![EpochRun { start_lsn: 0, epoch: 1 }, EpochRun { start_lsn: 2, epoch: 2 }]);
    }

    #[test]
    fn an_overwrite_never_drops_what_its_own_writer_placed() {
        let mut state = synced(1);
        state.append(1, 0, vec![entry(0, 1), entry(1, 1), entry(2, 1)], 0);
        assert!(matches!(reply(state.overwrite(1, 1, Vec::new(), true, 0)), ResponseBody::Failed(_)), "a rejoin delivered after the appends that followed it");
        assert!(matches!(reply(state.overwrite(1, 3, vec![entry(3, 1)], true, 0)), ResponseBody::Accepted(_)));
        let mut copying = <StreamState>::default();
        copying.seal(2);
        copying.overwrite(2, 0, vec![entry(0, 1), entry(1, 2)], false, -1);
        assert!(matches!(reply(copying.overwrite(2, 1, vec![entry(1, 2)], false, -1)), ResponseBody::Failed(_)), "a chunk delivered after the one that followed it");
        assert!(matches!(reply(copying.overwrite(2, 2, vec![entry(2, 2)], true, -1)), ResponseBody::Accepted(_)));
        let mut stale = synced(1);
        stale.append(1, 0, vec![entry(0, 1), entry(1, 1)], -1);
        stale.seal(2);
        assert!(matches!(reply(stale.overwrite(2, 1, vec![entry(1, 2)], true, -1)), ResponseBody::Accepted(_)), "a later writer still truncates an earlier writer's uncommitted suffix");
    }

    #[test]
    fn reads_return_only_committed_entries_and_report_the_trim_point() {
        let mut state = synced(1);
        state.append(1, 0, (0..4).map(|lsn| entry(lsn, 1)).collect(), 1);
        let read = state.entries(0, state.commit, 10, usize::MAX);
        assert_eq!(read.entries.iter().map(|entry| entry.lsn).collect::<Vec<_>>(), [0, 1]);
        state.trim(1);
        assert_eq!((state.trim, state.last(), state.entries.len()), (1, 3, 2));
        let below = state.entries(1, 3, 10, usize::MAX);
        assert!(below.entries.is_empty());
        assert_eq!(below.trim_lsn, 1);
        assert_eq!(state.entries(2, 3, 10, 1).entries.len(), 1, "a byte limit still admits one entry");
    }

    #[test]
    fn a_trim_beyond_the_last_entry_unsyncs_and_moves_last() {
        let mut state = synced(1);
        state.append(1, 0, vec![entry(0, 1)], 0);
        state.trim(5);
        assert_eq!((state.trim, state.last(), state.commit, state.synced), (5, 5, 5, None));
    }

    #[test]
    fn a_check_reports_only_a_promise_beyond_the_epoch() {
        let mut state = synced(2);
        assert!(matches!(state.check(2), ResponseBody::Accepted(_)));
        assert!(matches!(state.check(3), ResponseBody::Accepted(_)), "an unsealed or unsynced server is not sealed beyond the epoch");
        state.seal(3);
        assert_eq!(state.check(2), ResponseBody::Refused(Refused { promised: 3, last_lsn: -1 }));
    }

    #[test]
    fn replaying_the_events_rebuilds_the_state() {
        let mut state = <StreamState>::default();
        let mut events = Vec::new();
        events.extend(state.seal(3).events);
        events.extend(state.overwrite(3, 0, vec![entry(0, 2), entry(1, 3)], true, 0).events);
        events.extend(state.append(3, 2, vec![entry(2, 3)], 2).events);
        events.extend(state.trim(0).events);
        let mut replayed = <StreamState>::default();
        for event in &events { replayed.apply(event); }
        assert_eq!(replayed, state);
    }
}

#[cfg(test)]
mod certificates {
    use super::*;
    use crate::decision::{DecisionPrefix, DecisionScope, Handoff, HandoffCertificate};
    use crate::attestation::journal_id;
    use rtdb_storage::attestation::SigningKey;

    #[test]
    fn handoff_authenticates_a_disjoint_destination_and_exact_cut() {
        let key = || SigningKey::open(&SigningKey::generate().unwrap()).unwrap();
        let predecessor = key();
        let successor = key();
        let scope = |signer: &SigningKey| DecisionScope {
            stream: "history".into(), epoch: 2, quorum: 1,
            journals: vec![journal_id(&signer.public_key()).to_string()],
        };
        let handoff = Handoff {
            source: scope(&predecessor), destination: scope(&successor),
            first: 1, through: 4, digest: Bytes::from_static(&[7; 32]),
            decided: None, checkpoint: None,
        };
        let mut proof = HandoffCertificate { exports: vec![handoff.export(&predecessor)], handoff };
        assert!(proof.authentic());
        let original = proof.clone();
        proof.handoff.destination = scope(&key());
        assert!(!proof.authentic());
        let mut proof = original;
        proof.handoff.through += 1;
        assert!(!proof.authentic());
    }

    #[test]
    fn certificate_installation_needs_no_local_payload_and_rejects_substitution() {
        let keys: Vec<_> = (0..3).map(|_| SigningKey::open(&SigningKey::generate().unwrap()).unwrap()).collect();
        let mut journals: Vec<_> = keys.iter().map(|key| journal_id(&key.public_key()).to_string()).collect();
        journals.sort_unstable();
        let scope = DecisionScope { stream: "history".into(), epoch: 1, journals, quorum: 2 };
        let prefix = DecisionPrefix {
            predecessor: -1, predecessor_digest: Bytes::from_static(&[3; 32]),
            through: 0, digest: Bytes::from_static(&[4; 32]), guarded: true, guarded_before: Some(100),
        };
        let statement = DecisionStatement { scope, prefix };
        let timely = statement.vote(&keys[0], Some(99));
        let other = statement.vote(&keys[1], Some(98));
        let certificate = DecisionCertificate { statement: statement.clone(), votes: vec![timely.clone(), other] };
        assert!(certificate.authentic());

        let mut duplicate = certificate.clone();
        duplicate.votes[1] = timely;
        assert!(!duplicate.authentic());
        let mut late = certificate.clone();
        late.votes[1] = statement.vote(&keys[1], Some(100));
        assert!(!late.authentic());
        let mut foreign = certificate.clone();
        foreign.statement.prefix.digest = Bytes::from_static(&[9; 32]);
        assert!(!foreign.authentic());

        let mut lagging = StreamState::<Bytes>::default();
        let ensemble = statement.scope.ensemble().unwrap();
        lagging.seal(1);
        lagging.overwrite(1, 0, Vec::new(), true, -1);
        lagging.activate(1, ensemble, true);
        let installation = statement.installation(&keys[2]);
        assert!(matches!(lagging.install_certificate(&certificate, installation.clone()).reply,
            ResponseBody::DecisionInstallation(observed) if observed == installation));
        let ResponseBody::Sealed(sealed) = lagging.sealed() else { panic!("expected durable seal"); };
        assert_eq!(sealed.last_lsn, -1);
        assert_eq!(sealed.certificate, Some(certificate));
    }
}
