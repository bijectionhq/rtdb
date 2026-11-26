//! The pure decisions behind the journal reference fence, checked by Verus (`verification/verus/journal_custody`).

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
