#![allow(unexpected_cfgs)]

#[cfg(not(verus_keep_ghost))]
use rtdb_types::{Anomaly, Result, Term, TermSequence};
#[cfg(verus_keep_ghost)]
use crate::{Anomaly, Result, term_types::{Term, TermSequence}};
#[cfg(not(verus_keep_ghost))]
use serde::{Deserialize, Serialize};

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

pub(crate) enum SequenceDecision {
    Discard,
    Apply,
    Void,
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (observed < highest),
))]
pub(crate) fn is_stale_term(highest: i64, observed: i64) -> bool {
    observed < highest
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out is Apply <==> (observed is Some && observed->Some_0 == expected),
        out is Void <==> (observed is Some && expected < observed->Some_0),
))]
pub(crate) fn classify_sequence(expected: i64, observed: Option<i64>) -> SequenceDecision {
    match observed {
        Some(sequence) if sequence == expected => SequenceDecision::Apply,
        Some(sequence) if sequence > expected => SequenceDecision::Void,
        _ => SequenceDecision::Discard,
    }
}

#[derive(Clone, Copy)]
#[cfg_attr(not(verus_keep_ghost), derive(Debug, PartialEq, Eq, Serialize, Deserialize))]
pub enum TermPosition { Unsequenced, Unknown, Expecting(TermSequence), Voided }

#[derive(Clone, Copy)]
#[cfg_attr(not(verus_keep_ghost), derive(Debug, PartialEq, Eq, Serialize, Deserialize))]
pub enum Verdict { Apply, Fenced, Discarded, Voided }

#[derive(Clone, Copy)]
#[cfg_attr(not(verus_keep_ghost), derive(Debug, PartialEq, Eq))]
pub struct Admission { pub verdict: Verdict, pub previous_term: Term, pub highest_term: Term }

#[cfg(not(verus_keep_ghost))]
impl Admission {
    /// A pending claim is adjudicated using the term before folding its own record.
    pub fn opens_term(self) -> bool { self.highest_term > self.previous_term && self.verdict == Verdict::Apply }
}

#[cfg_attr(not(verus_keep_ghost), derive(Clone, Debug, PartialEq, Eq))]
pub struct TermFence { highest_term: Term, position: TermPosition }

#[cfg(verus_keep_ghost)]
verus! {
impl TermFence {
    pub closed spec fn observed_highest(&self) -> i64 { self.highest_term.0 }
    pub closed spec fn observed_position(&self) -> TermPosition { self.position }
}
}

impl TermFence {
    /// The replica reader owns this value across every leader/follower role change.
    pub fn from_boundary(term: Term, sequence: Option<TermSequence>) -> Result<Self> {
        let position = match sequence {
            Some(sequence) => TermPosition::Expecting(sequence.next()?),
            None if term.0 == 0 => TermPosition::Unsequenced,
            None => TermPosition::Unknown,
        };
        Ok(Self { highest_term: term, position })
    }

    #[cfg_attr(verus_keep_ghost, verus_spec(out =>
        ensures out.0 == self.observed_highest(),
    ))]
    pub fn highest_term(&self) -> Term { self.highest_term }
    pub fn position(&self) -> TermPosition { self.position }
    #[cfg_attr(verus_keep_ghost, verus_spec(out =>
        ensures match out {
            Ok(next) => next.0 == self.observed_highest() as int + 1,
            Err(_) => self.observed_highest() == i64::MAX,
        },
    ))]
    pub fn claim_term(&self) -> Result<Term> {
        match self.highest_term.0.checked_add(1) {
            Some(next) => Ok(Term(next)),
            None => Err(Anomaly::fault("rtdb.replication/term-overflow", "Leader term exhausted")),
        }
    }

    /// Call for every delivered record, including claims and records later discarded by a role.
    /// Only `Apply` permits the caller to change the live index or applied-progress monitor.
    #[cfg_attr(verus_keep_ghost, verus_spec(out =>
        ensures
            term.0 < old(self).observed_highest() ==> (out matches Ok(admission)
                && admission.verdict is Fenced
                && final(self).observed_position() == old(self).observed_position()),
            match out {
                Ok(admission) => admission.previous_term.0 == old(self).observed_highest()
                    && admission.highest_term.0 == final(self).observed_highest()
                    && final(self).observed_highest() ==
                        (if term.0 > old(self).observed_highest() { term.0 } else { old(self).observed_highest() }),
                Err(_) => true,
            },
            out is Err ==> final(self).observed_highest() == old(self).observed_highest()
                && final(self).observed_position() == old(self).observed_position(),
    ))]
    pub fn admit(&mut self, term: Term, sequence: Option<TermSequence>) -> Result<Admission> {
        let previous_term = self.highest_term;
        let (highest_term, position, verdict) = if is_stale_term(self.highest_term.0, term.0) {
            (self.highest_term, self.position, Verdict::Fenced)
        } else if term.0 > self.highest_term.0 {
            match sequence {
                None => (term, TermPosition::Unsequenced, Verdict::Apply),
                Some(TermSequence(0)) => (term, TermPosition::Expecting(TermSequence(1)), Verdict::Apply),
                Some(_) => (term, TermPosition::Voided, Verdict::Voided),
            }
        } else {
            let (position, verdict) = match self.position {
                TermPosition::Unsequenced => (self.position, Verdict::Apply),
                TermPosition::Voided => (self.position, Verdict::Discarded),
                TermPosition::Unknown => match sequence {
                    None => (TermPosition::Unsequenced, Verdict::Apply),
                    Some(TermSequence(0)) => (self.position, Verdict::Discarded),
                    Some(sequence) => (TermPosition::Expecting(sequence.next()?), Verdict::Apply),
                },
                TermPosition::Expecting(next) => match classify_sequence(next.0, match sequence {
                    Some(sequence) => Some(sequence.0),
                    None => None,
                }) {
                    SequenceDecision::Discard => (self.position, Verdict::Discarded),
                    SequenceDecision::Apply => (TermPosition::Expecting(next.next()?), Verdict::Apply),
                    SequenceDecision::Void => (TermPosition::Voided, Verdict::Voided),
                },
            };
            (self.highest_term, position, verdict)
        };
        self.highest_term = highest_term;
        self.position = position;
        Ok(Admission { verdict, previous_term, highest_term })
    }
}
