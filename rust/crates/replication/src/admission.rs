use rtdb_format::catalog::BlockCatalog;
use rtdb_types::{Anomaly, Result, Term, TermSequence};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TermPosition { Unsequenced, Unknown, Expecting(TermSequence), Voided }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict { Apply, Fenced, Discarded, Voided }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Admission { pub verdict: Verdict, pub previous_term: Term, pub highest_term: Term }

impl Admission {
    /// A pending claim is adjudicated using the term before folding its own record.
    pub fn opens_term(self) -> bool { self.highest_term > self.previous_term && self.verdict == Verdict::Apply }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TermFence { highest_term: Term, position: TermPosition }

impl TermFence {
    /// The replica reader owns this value across every leader/follower role change.
    pub fn from_boundary(term: Term, sequence: Option<TermSequence>) -> Result<Self> {
        let position = match sequence {
            Some(sequence) => TermPosition::Expecting(sequence.next()?),
            None if term == Term(0) => TermPosition::Unsequenced,
            None => TermPosition::Unknown,
        };
        Ok(Self { highest_term: term, position })
    }

    pub fn from_catalog(block: &BlockCatalog) -> Result<Self> { Self::from_boundary(block.boundary_term(), block.boundary_term_sequence()) }
    pub fn highest_term(&self) -> Term { self.highest_term }
    pub fn position(&self) -> TermPosition { self.position }
    pub fn claim_term(&self) -> Result<Term> {
        self.highest_term.0.checked_add(1).map(Term).ok_or_else(|| Anomaly::fault("rtdb.replication/term-overflow", "Leader term exhausted"))
    }

    /// Call for every delivered record, including claims and records later discarded by a role.
    /// Only `Apply` permits the caller to change the live index or applied-progress monitor.
    pub fn admit(&mut self, term: Term, sequence: Option<TermSequence>) -> Result<Admission> {
        let previous_term = self.highest_term;
        let (highest_term, position, verdict) = if term < self.highest_term {
            (self.highest_term, self.position, Verdict::Fenced)
        } else if term > self.highest_term {
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
                TermPosition::Expecting(next) => match sequence {
                    None => (self.position, Verdict::Discarded),
                    Some(sequence) if sequence < next => (self.position, Verdict::Discarded),
                    Some(sequence) if sequence == next => (TermPosition::Expecting(next.next()?), Verdict::Apply),
                    Some(_) => (TermPosition::Voided, Verdict::Voided),
                },
            };
            (self.highest_term, position, verdict)
        };
        self.highest_term = highest_term;
        self.position = position;
        Ok(Admission { verdict, previous_term, highest_term })
    }
}
