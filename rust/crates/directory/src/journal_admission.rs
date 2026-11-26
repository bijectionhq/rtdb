//! The pure decisions behind the journal reference fence and journal copy release, checked by Verus (`verification/verus/journal_custody`).

#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

/// Whether a member write numbered `next` may replace one numbered `stored`: a changed state or decommission needs a later epoch, and an unchanged one never goes back.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (if eligibility_changed { stored < next } else { stored <= next }),
))]
pub fn epoch_follows(stored: u64, next: u64, eligibility_changed: bool) -> bool {
    if eligibility_changed { stored < next } else { stored <= next }
}

/// What a deployment write observed of one logkeeper: whether the write newly names it, and whether it still accepts references.
#[derive(Clone, Copy)]
#[cfg_attr(not(verus_keep_ghost), derive(Debug, PartialEq, Eq))]
pub struct JournalObservation {
    pub newly_named: bool,
    pub accepts_references: bool,
}

/// Which logkeepers a write must fence, one flag per observation: exactly the ones it newly names.
/// None when one it newly names accepts no references, so the write is refused.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out matches Some(fenced) ==> fenced@.len() == observations@.len()
            && (forall|i: int| 0 <= i < observations@.len() ==> fenced@[i] == observations@[i].newly_named)
            && (forall|i: int| 0 <= i < observations@.len() && observations@[i].newly_named ==> observations@[i].accepts_references),
        out is None ==> exists|i: int| 0 <= i < observations@.len() && observations@[i].newly_named && !observations@[i].accepts_references,
))]
pub fn fenced_journals(observations: &[JournalObservation]) -> Option<Vec<bool>> {
    let mut fenced = Vec::new();
    let mut index = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(invariant
        index <= observations@.len(),
        fenced@.len() == index,
        forall|i: int| 0 <= i < index ==> fenced@[i] == observations@[i].newly_named,
        forall|i: int| 0 <= i < index && observations@[i].newly_named ==> observations@[i].accepts_references,
        decreases observations@.len() - index,
    ))]
    while index < observations.len() {
        let observation = observations[index];
        if observation.newly_named && !observation.accepts_references { return None; }
        fenced.push(observation.newly_named);
        index += 1;
    }
    Some(fenced)
}

/// What a release observed of one retained copy and of everything that could still depend on it.
#[derive(Clone, Copy)]
#[cfg_attr(not(verus_keep_ghost), derive(Debug, PartialEq, Eq))]
pub struct CopyObservation {
    /// The journal is a draining logkeeper.
    pub journal_draining: bool,
    /// The record's copy custody names the journal.
    pub retained: bool,
    /// The journal is in the serving ensemble.
    pub serving: bool,
    /// The current ensemble was installed by a recorded handoff, so it and the checkpoint hold every acknowledged entry.
    pub durable_elsewhere: bool,
    /// A field outside copy custody still names the journal: a pending placement, a transfer source, a writer handoff or a hold.
    pub otherwise_named: bool,
    /// The checkpoint's journal decision counts the journal's vote.
    pub checkpoint_decision: bool,
    /// A journal placement, move, cell transfer, checkpoint transfer or writer handoff is in flight.
    pub handoff_in_flight: bool,
    /// A recovery, replay capture, replay region, staged checkpoint, unapplied payload trim or suffix pin is pending.
    pub recovery_or_replay: bool,
    /// The record is a staged fork, or a staged fork of it is pending.
    pub fork: bool,
    /// The deployment is being deleted, so its disposal must retire every retained copy.
    pub erasure: bool,
    /// The deployment's journals are in installed-journal custody.
    pub installed: bool,
}

/// Why a release is refused: the first unmet condition, in the order the fields are declared.
#[derive(Clone, Copy)]
#[cfg_attr(not(verus_keep_ghost), derive(Debug, PartialEq, Eq))]
pub enum CopyDependent {
    NotDraining,
    NotRetained,
    Serving,
    NotDurable,
    OtherReference,
    CheckpointDecision,
    HandoffInFlight,
    RecoveryOrReplay,
    Fork,
    Erasure,
    Installed,
}

#[cfg(verus_keep_ghost)]
verus! {

pub open spec fn releasable(o: CopyObservation) -> bool {
    o.journal_draining && o.retained && !o.serving && o.durable_elsewhere && !o.otherwise_named && !o.checkpoint_decision
        && !o.handoff_in_flight && !o.recovery_or_replay && !o.fork && !o.erasure && !o.installed
}

}

/// None when the copy may leave custody; otherwise the dependent that keeps it.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures (out is None) == releasable(*observation),
))]
pub fn copy_dependent(observation: &CopyObservation) -> Option<CopyDependent> {
    if !observation.journal_draining { return Some(CopyDependent::NotDraining); }
    if !observation.retained { return Some(CopyDependent::NotRetained); }
    if observation.serving { return Some(CopyDependent::Serving); }
    if !observation.durable_elsewhere { return Some(CopyDependent::NotDurable); }
    if observation.otherwise_named { return Some(CopyDependent::OtherReference); }
    if observation.checkpoint_decision { return Some(CopyDependent::CheckpointDecision); }
    if observation.handoff_in_flight { return Some(CopyDependent::HandoffInFlight); }
    if observation.recovery_or_replay { return Some(CopyDependent::RecoveryOrReplay); }
    if observation.fork { return Some(CopyDependent::Fork); }
    if observation.erasure { return Some(CopyDependent::Erasure); }
    if observation.installed { return Some(CopyDependent::Installed); }
    None
}
