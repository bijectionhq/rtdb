//! One stream's state on one server, as a pure state machine.
//! A mutation is decided against the accepted state, which already includes every earlier decision of the stream, and yields the events to persist and the reply to send once they are durable.
//! Replay applies the same events on restart.

use crate::protocol::{Accepted, Entries, Entry, EpochRun, Refused, ResponseBody, Sealed, runs_of};
use bytes::Bytes;
use std::collections::VecDeque;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Event {
    Promise(u64),
    Synced(Option<u64>),
    /// Drops every entry at or above the lsn.
    Truncate(i64),
    Append(Entry),
    Commit(i64),
    Trim(i64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Held {
    pub epoch: u64,
    pub timestamp: i64,
    pub payload: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StreamState {
    pub promised: u64,
    /// The epoch whose writer last brought this server up to that writer's whole history.
    pub synced: Option<u64>,
    /// Entries at or below this lsn are dropped.
    pub trim: i64,
    pub commit: i64,
    /// Entry `i` has lsn `trim + 1 + i`.
    pub entries: VecDeque<Held>,
}

pub(crate) struct Decision {
    pub events: Vec<Event>,
    pub reply: ResponseBody,
}

fn invalid(message: impl Into<String>) -> Decision {
    Decision { events: Vec::new(), reply: ResponseBody::Failed(crate::protocol::Failed { message: message.into() }) }
}

fn dense(first: i64, entries: &[Entry]) -> bool {
    entries.iter().zip(first..).all(|(entry, lsn)| entry.lsn == lsn)
}

impl Default for StreamState {
    fn default() -> Self { Self { promised: 0, synced: None, trim: -1, commit: -1, entries: VecDeque::new() } }
}

impl StreamState {
    pub fn last(&self) -> i64 { self.trim + self.entries.len() as i64 }

    pub fn retained_bytes(&self) -> usize { self.entries.iter().map(|held| held.payload.len() + 32).sum() }

    pub fn apply(&mut self, event: &Event) {
        match event {
            Event::Promise(epoch) => self.promised = self.promised.max(*epoch),
            Event::Synced(epoch) => self.synced = *epoch,
            Event::Truncate(from) => {
                let keep = (from - self.trim - 1).clamp(0, self.entries.len() as i64);
                self.entries.truncate(keep as usize);
                self.commit = self.commit.min(self.last());
            }
            Event::Append(entry) => {
                debug_assert_eq!(entry.lsn, self.last() + 1);
                self.entries.push_back(Held { epoch: entry.epoch, timestamp: entry.timestamp, payload: entry.payload.clone() });
            }
            Event::Commit(lsn) => self.commit = self.commit.max((*lsn).min(self.last())),
            Event::Trim(through) => {
                if *through <= self.trim { return; }
                if *through >= self.last() {
                    if *through > self.last() { self.synced = None; }
                    self.entries.clear();
                } else {
                    self.entries.drain(..(through - self.trim) as usize);
                }
                self.trim = *through;
                self.commit = self.commit.max(*through);
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
        (self.promised != epoch || self.synced != Some(epoch)).then(|| self.refused())
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
        let commit = self.commit_event(commit, last);
        let events = entries.into_iter().map(Event::Append).chain(commit).collect();
        self.persist(events, Self::accepted)
    }

    pub fn overwrite(&mut self, epoch: u64, from: i64, entries: Vec<Entry>, complete: bool, commit: i64) -> Decision {
        if epoch == 0 { return invalid("epochs start at 1"); }
        if self.promised > epoch { return Decision { events: Vec::new(), reply: self.refused() }; }
        if from <= self.trim || from > self.last() + 1 || from <= self.commit {
            return invalid(format!("an overwrite at {from} must start above trim {} and commit {} and at most one past last {}", self.trim, self.commit, self.last()));
        }
        if !dense(from, &entries) || entries.iter().any(|entry| entry.epoch > epoch) {
            return invalid("an overwrite's entries must be dense from its first lsn and no later than its epoch");
        }
        // A writer's own overwrites never cut what it placed, so one that would is a request delivered after newer ones: deciding it would drop entries those may have committed.
        let own = |held: &Held| held.epoch == epoch;
        if from <= self.last() && (self.synced == Some(epoch) || self.entries.iter().skip((from - self.trim - 1) as usize).any(own)) {
            return invalid(format!("an overwrite at epoch {epoch} from {from} would drop entries its own writer placed, up to {}", self.last()));
        }
        let mut events = Vec::new();
        if epoch > self.promised { events.push(Event::Promise(epoch)); }
        if from <= self.last() { events.push(Event::Truncate(from)); }
        let last = from + entries.len() as i64 - 1;
        events.extend(entries.into_iter().map(Event::Append));
        // A partial copy keeps the epoch the server was synced at: it still holds everything it held of that writer's committed history, so recovery must still count it as that writer's witness.
        if complete && self.synced != Some(epoch) { events.push(Event::Synced(Some(epoch))); }
        if complete { events.extend(self.commit_event(commit, last)); }
        self.persist(events, Self::accepted)
    }

    pub fn commit(&mut self, epoch: u64, commit: i64) -> Decision {
        if let Some(refusal) = self.check_writer(epoch) { return Decision { events: Vec::new(), reply: refusal }; }
        let events = self.commit_event(commit, self.last()).into_iter().collect();
        self.persist(events, Self::accepted)
    }

    pub fn seal(&mut self, epoch: u64) -> Decision {
        if epoch == 0 { return invalid("epochs start at 1"); }
        let events = (epoch > self.promised).then_some(Event::Promise(epoch)).into_iter().collect();
        self.persist(events, Self::sealed)
    }

    pub fn trim(&mut self, through: i64) -> Decision {
        let events = (through > self.trim).then_some(Event::Trim(through)).into_iter().collect();
        self.persist(events, Self::accepted)
    }

    pub fn check(&self, epoch: u64) -> ResponseBody {
        if self.promised > epoch { self.refused() } else { self.accepted() }
    }

    pub fn sealed(&self) -> ResponseBody {
        ResponseBody::Sealed(Sealed {
            promised: self.promised,
            synced: self.synced.unwrap_or(0),
            trim_lsn: self.trim,
            last_lsn: self.last(),
            commit_lsn: self.commit,
            runs: self.runs(),
        })
    }

    pub fn runs(&self) -> Vec<EpochRun> { runs_of(self.entries.iter().zip(self.trim + 1..).map(|(held, lsn)| (lsn, held.epoch))) }

    /// Entries in `[from, to]`, the first always included when present, then while within the limits.
    pub fn entries(&self, from: i64, to: i64, max_entries: usize, max_bytes: usize) -> Entries {
        let mut entries = Vec::new();
        if from > self.trim {
            let mut bytes = 0usize;
            let skip = (from - self.trim - 1) as usize;
            for (held, lsn) in self.entries.iter().skip(skip).zip(from..=to).take(max_entries) {
                bytes = bytes.saturating_add(held.payload.len());
                if bytes > max_bytes && !entries.is_empty() { break; }
                entries.push(Entry { lsn, epoch: held.epoch, timestamp: held.timestamp, payload: held.payload.clone() });
            }
        }
        Entries { entries, commit_lsn: self.commit, trim_lsn: self.trim, last_lsn: self.last() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(lsn: i64, epoch: u64) -> Entry { Entry { lsn, epoch, timestamp: lsn, payload: Bytes::from(format!("{lsn}@{epoch}")) } }

    fn synced(epoch: u64) -> StreamState {
        let mut state = StreamState::default();
        state.seal(epoch);
        state.overwrite(epoch, 0, Vec::new(), true, -1);
        state
    }

    fn reply(decision: Decision) -> ResponseBody { decision.reply }

    #[test]
    fn appends_need_a_synced_writer_at_the_promised_epoch() {
        let mut state = StreamState::default();
        assert_eq!(reply(state.append(1, 0, vec![entry(0, 1)], -1)), ResponseBody::Refused(Refused { promised: 0, last_lsn: -1 }));
        state.seal(1);
        assert!(matches!(reply(state.append(1, 0, vec![entry(0, 1)], -1)), ResponseBody::Refused(_)), "sealed is not synced");
        state.overwrite(1, 0, Vec::new(), true, -1);
        assert_eq!(reply(state.append(1, 0, vec![entry(0, 1), entry(1, 1)], -1)), ResponseBody::Accepted(Accepted { last_lsn: 1, commit_lsn: -1 }));
        assert_eq!(reply(state.append(1, 3, vec![entry(3, 1)], -1)), ResponseBody::Refused(Refused { promised: 1, last_lsn: 1 }), "a gap");
        assert_eq!(reply(state.append(1, 1, vec![entry(1, 1)], -1)), ResponseBody::Refused(Refused { promised: 1, last_lsn: 1 }), "a repeat");
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
        let mut copying = StreamState::default();
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
        let mut state = StreamState::default();
        let mut events = Vec::new();
        events.extend(state.seal(3).events);
        events.extend(state.overwrite(3, 0, vec![entry(0, 2), entry(1, 3)], true, 0).events);
        events.extend(state.append(3, 2, vec![entry(2, 3)], 2).events);
        events.extend(state.trim(0).events);
        let mut replayed = StreamState::default();
        for event in &events { replayed.apply(event); }
        assert_eq!(replayed, state);
    }
}
