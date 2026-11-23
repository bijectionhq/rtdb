#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[derive(Clone, Copy)]
pub(super) enum JournalEvidence {
    Unavailable,
    Excluded,
    Holding,
    TimelyOriginal,
    CommitWitness,
}

pub(super) enum TailDecision {
    Keep,
    Discard,
    Unknown,
}

#[cfg_attr(verus_keep_ghost, verus_spec(evidence =>
    ensures evidence == if trimmed { JournalEvidence::Unavailable }
        else if !holds_entry { JournalEvidence::Excluded }
        else if committed { JournalEvidence::CommitWitness }
        else { JournalEvidence::Holding },
))]
pub(super) fn sealed_journal_evidence(trimmed: bool, holds_entry: bool, committed: bool) -> JournalEvidence {
    if trimmed {
        JournalEvidence::Unavailable
    } else if !holds_entry {
        JournalEvidence::Excluded
    } else if committed {
        JournalEvidence::CommitWitness
    } else {
        JournalEvidence::Holding
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(valid =>
    ensures valid == (offset == through && digest_len == 32 && (cutoff.is_none() || origin_matches)),
))]
pub(super) fn valid_guarded_source(offset: i64, through: i64, digest_len: usize, cutoff: Option<i64>, origin_matches: bool) -> bool {
    offset == through && digest_len == 32 && (cutoff.is_none() || origin_matches)
}

#[cfg_attr(verus_keep_ghost, verus_spec(evidence =>
    ensures evidence == if exact && original == Some(true) && upper.is_some() && upper.unwrap() > 0
        && (cutoff.is_none() || upper.unwrap() < cutoff.unwrap()) {
        JournalEvidence::TimelyOriginal
    } else if exact && ((original == Some(false) && upper.is_none())
        || (original == Some(true) && upper.is_some() && upper.unwrap() > 0
            && cutoff.is_some() && upper.unwrap() >= cutoff.unwrap())) {
        JournalEvidence::Excluded
    } else {
        JournalEvidence::Holding
    },
))]
pub(super) fn guarded_probe_evidence(exact: bool, original: Option<bool>, upper: Option<i64>, cutoff: Option<i64>) -> JournalEvidence {
    if !exact {
        return JournalEvidence::Holding;
    }
    match (original, upper) {
        (Some(true), Some(upper)) if upper > 0 => match cutoff {
            Some(cutoff) if upper >= cutoff => JournalEvidence::Excluded,
            _ => JournalEvidence::TimelyOriginal,
        },
        (Some(false), None) => JournalEvidence::Excluded,
        _ => JournalEvidence::Holding,
    }
}

#[cfg(verus_keep_ghost)]
verus! {
    pub(super) open spec fn eligible_slots(journals: Seq<JournalEvidence>, end: int, possible: bool) -> nat
        decreases end,
    {
        if end <= 0 {
            0
        } else {
            let slot = journals[end - 1];
            let eligible = if possible { !(slot is Excluded) } else { slot is TimelyOriginal };
            eligible_slots(journals, end - 1, possible) + if eligible { 1nat } else { 0nat }
        }
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(counts =>
    ensures
        counts.0 == eligible_slots(journals@, journals@.len() as int, false),
        counts.1 == eligible_slots(journals@, journals@.len() as int, true),
))]
pub(super) fn count_guarded_journals(journals: &[JournalEvidence]) -> (usize, usize) {
    let mut timely = 0;
    let mut possible = 0;
    let mut next = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            0 <= next <= journals@.len(),
            timely <= next,
            possible <= next,
            timely == eligible_slots(journals@, next as int, false),
            possible == eligible_slots(journals@, next as int, true),
        decreases journals@.len() - next,
    ))]
    while next < journals.len() {
        let evidence = journals[next];
        if matches!(evidence, JournalEvidence::TimelyOriginal) {
            timely += 1;
        }
        if !matches!(evidence, JournalEvidence::Excluded) {
            possible += 1;
        }
        next += 1;
    }
    (timely, possible)
}

#[cfg_attr(verus_keep_ghost, verus_spec(decision =>
    ensures decision == if committed || timely >= quorum { TailDecision::Keep }
        else if possible < quorum { TailDecision::Discard }
        else { TailDecision::Unknown },
))]
pub(super) fn recover_guarded_tail(committed: bool, timely: usize, possible: usize, quorum: usize) -> TailDecision {
    if committed || timely >= quorum {
        TailDecision::Keep
    } else if possible < quorum {
        TailDecision::Discard
    } else {
        TailDecision::Unknown
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(decision =>
    ensures decision == if eligible_slots(journals@, journals@.len() as int, false) >= quorum {
        TailDecision::Keep
    } else if eligible_slots(journals@, journals@.len() as int, true) < quorum {
        TailDecision::Discard
    } else {
        TailDecision::Unknown
    },
))]
pub(super) fn decide_from_sealed_probes(journals: &[JournalEvidence], quorum: usize) -> TailDecision {
    let (timely, possible) = count_guarded_journals(journals);
    recover_guarded_tail(false, timely, possible, quorum)
}

#[cfg_attr(verus_keep_ghost, verus_spec(target =>
    ensures target == if guarded_tip.is_some() { guarded_tip.unwrap() } else { prefix },
))]
pub(super) fn predecessor_commit_target(guarded_tip: Option<i64>, prefix: i64) -> i64 {
    guarded_tip.unwrap_or(prefix)
}

#[cfg_attr(verus_keep_ghost, verus_spec(covers =>
    ensures covers == (acknowledged >= target),
))]
pub(super) fn covers_recovered_commit(acknowledged: i64, target: i64) -> bool {
    acknowledged >= target
}

#[cfg_attr(verus_keep_ghost, verus_spec(ready =>
    ensures ready == (quorum > 0 && committed >= quorum),
))]
pub(super) fn recovery_commit_quorum(committed: usize, quorum: usize) -> bool {
    quorum > 0 && committed >= quorum
}
