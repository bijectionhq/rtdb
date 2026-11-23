//! One log server holding the simulated stream: the real per-stream state machine, behind the real server's durability rule.
//!
//! As in `rtdb_logkeeper::server::store`, a mutation is decided against the accepted state at once; a refusal or failure is answered at once, and anything else is answered only once its events are durable.
//! Fetches read the accepted state.
//! A crash keeps the durable state plus a prefix of the events not yet synced (the journal cuts a torn tail at a record boundary), and loses every reply still waiting on them.

use rtdb_logkeeper::simulation::{Accepted, Entry, Event, Refused, ResponseBody, StreamState};

use crate::Bug;

#[derive(Clone, Debug)]
pub(crate) enum Body {
    Seal { epoch: u64 },
    Append { epoch: u64, first: i64, entries: Vec<Entry>, commit: i64 },
    Overwrite { epoch: u64, from: i64, entries: Vec<Entry>, complete: bool, commit: i64 },
    Commit { epoch: u64, commit: i64 },
    Trim { through: i64 },
    /// Held entries in `[from, to)`, committed or not.
    Fetch { from: i64, to: i64, max_entries: usize },
}

impl Body {
    pub fn describe(&self) -> String {
        match self {
            Body::Seal { epoch } => format!("seal@{epoch}"),
            Body::Append { epoch, first, entries, commit } => format!("append@{epoch} [{first}..{}] commit {commit}", first + entries.len() as i64 - 1),
            Body::Overwrite { epoch, from, entries, complete, commit } => format!("overwrite@{epoch} from {from} +{}{} commit {commit}", entries.len(), if *complete { " complete" } else { "" }),
            Body::Commit { epoch, commit } => format!("commit@{epoch} {commit}"),
            Body::Trim { through } => format!("trim {through}"),
            Body::Fetch { from, to, .. } => format!("fetch [{from}, {to})"),
        }
    }
}

/// Where a reply goes once it may be sent.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Return {
    pub process: usize,
    pub session: u64,
    pub id: u64,
}

struct Pending {
    events: Vec<Event>,
    reply: Return,
    body: ResponseBody,
    /// What a seal saw, for the checker: the entries held when the seal was decided.
    snapshot: Option<Vec<Entry>>,
    /// The epoch of an accepted append, overwrite or commit.
    writer: Option<u64>,
}

/// What became durable in a sync: the replies now sent, and which process wrote at which epoch.
pub(crate) struct Synced {
    pub replies: Vec<(Return, ResponseBody, Option<Vec<Entry>>)>,
    pub writers: Vec<(usize, u64)>,
}

pub(crate) enum Outcome {
    /// Answered at once: a refusal, a failure or a fetch.
    Now(ResponseBody),
    /// Answered once durable; the accepted state changed from this lsn, everything when `None`.
    Durable(Option<i64>),
}

pub(crate) fn held(state: &StreamState) -> Vec<Entry> {
    state.entries.iter().zip(state.trim + 1..).map(|(held, lsn)| Entry { lsn, epoch: held.epoch, timestamp: held.timestamp, payload: held.payload.clone() }).collect()
}

pub(crate) struct Server {
    pub durable: StreamState,
    pub accepted: StreamState,
    pending: Vec<Pending>,
    pub up: bool,
}

impl Server {
    pub fn new() -> Self { Self { durable: StreamState::default(), accepted: StreamState::default(), pending: Vec::new(), up: true } }

    pub fn has_pending(&self) -> bool { !self.pending.is_empty() }

    pub fn decide(&mut self, body: Body, reply: Return, bug: Option<Bug>) -> Outcome {
        let state = &mut self.accepted;
        let (decision, sealing, writer) = match body {
            Body::Fetch { from, to, max_entries } => {
                let to = to.saturating_sub(1).min(state.last());
                return Outcome::Now(ResponseBody::Entries(state.entries(from, to, max_entries, usize::MAX)));
            }
            Body::Seal { epoch } => (state.seal(epoch), true, None),
            Body::Trim { through } => (state.trim(through), false, None),
            Body::Commit { epoch, commit } => (state.commit(epoch, commit), false, Some(epoch)),
            Body::Overwrite { epoch, from, entries, complete, commit } => (state.overwrite(epoch, from, entries, complete, commit), false, Some(epoch)),
            Body::Append { epoch, first, entries, commit } => {
                let decision = state.append(epoch, first, entries.clone(), commit);
                let decision = match (&decision.reply, bug) {
                    // The broken variant: a server that checks the position but not the writer's promise.
                    (ResponseBody::Refused(_), Some(Bug::AppendIgnoresPromise)) if first == state.last() + 1 => {
                        let events: Vec<Event> = entries.into_iter().map(Event::Append).collect();
                        for event in &events { state.apply(event); }
                        rtdb_logkeeper::simulation::Decision { events, reply: ResponseBody::Accepted(Accepted { last_lsn: state.last(), commit_lsn: state.commit }) }
                    }
                    _ => decision,
                };
                (decision, false, Some(epoch))
            }
        };
        if matches!(decision.reply, ResponseBody::Refused(_) | ResponseBody::Failed(_)) {
            debug_assert!(decision.events.is_empty());
            return Outcome::Now(decision.reply);
        }
        let changed = decision.events.iter().map(|event| match event {
            Event::Truncate(from) => Some(*from),
            Event::Append(entry) => Some(entry.lsn),
            Event::Promise(_) | Event::Commit(_) => Some(i64::MAX),
            Event::Synced(_) | Event::Activate { .. } | Event::Trim(_) | Event::Drop(_) => None,
        }).try_fold(i64::MAX, |low, from| from.map(|from| low.min(from)));
        let snapshot = sealing.then(|| held(&self.accepted));
        self.pending.push(Pending { events: decision.events, reply, body: decision.reply, snapshot, writer });
        Outcome::Durable(changed)
    }

    /// One journal round: every pending decision becomes durable, in order, and its reply may be sent.
    pub fn sync(&mut self) -> Synced {
        let mut synced = Synced { replies: Vec::new(), writers: Vec::new() };
        for pending in self.pending.drain(..) {
            for event in &pending.events { self.durable.apply(event); }
            if let Some(epoch) = pending.writer { synced.writers.push((pending.reply.process, epoch)); }
            synced.replies.push((pending.reply, pending.body, pending.snapshot));
        }
        debug_assert_eq!(self.durable, self.accepted);
        synced
    }

    /// Keeps the first `kept` pending events (a torn round), drops the rest with their replies, and replays to the durable state.
    pub fn crash(&mut self, kept: usize) {
        let mut remaining = kept;
        for pending in self.pending.drain(..) {
            for event in pending.events {
                if remaining == 0 { break; }
                self.durable.apply(&event);
                remaining -= 1;
            }
        }
        self.accepted = self.durable.clone();
        self.up = false;
    }

    pub fn pending_events(&self) -> usize { self.pending.iter().map(|pending| pending.events.len()).sum() }
}

pub(crate) fn refused_by(body: &ResponseBody, epoch: u64) -> Option<u64> {
    match body { ResponseBody::Refused(Refused { promised, .. }) if *promised > epoch => Some(*promised), _ => None }
}
