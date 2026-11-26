use crate::production::{CopyDependent, CopyObservation, JournalObservation, copy_dependent, epoch_follows, fenced_journals};
use vstd::prelude::*;

verus! {

/// A member write that changes its state or decommission is admitted only with a later eligibility epoch.
fn a_changed_eligibility_moves_the_epoch_on(stored: u64, next: u64) -> (out: bool)
    ensures out ==> stored < next,
{
    epoch_follows(stored, next, true)
}

/// No admitted member write takes the eligibility epoch back, so an observed epoch recurs only while the eligibility is unchanged.
fn an_eligibility_epoch_never_goes_back(stored: u64, next: u64, changed: bool) -> (out: bool)
    ensures out ==> stored <= next,
{
    epoch_follows(stored, next, changed)
}

/// A deployment write that newly names a logkeeper certified for decommission is refused.
fn a_write_naming_a_certified_journal_is_refused(observations: &[JournalObservation], j: usize) -> (out: Option<Vec<bool>>)
    requires j < observations@.len(), observations@[j as int].newly_named, !observations@[j as int].accepts_references,
    ensures out is None,
{
    fenced_journals(observations)
}

/// Every logkeeper a write newly names is held at its observed epoch in the commit.
fn every_newly_named_journal_is_fenced(observations: &[JournalObservation], j: usize) -> (out: Option<Vec<bool>>)
    requires j < observations@.len(), observations@[j as int].newly_named,
    ensures out matches Some(fenced) ==> fenced@[j as int],
{
    fenced_journals(observations)
}

/// An admitted release leaves no dependent on the copy: the journal is draining and out of the ensemble, which a recorded handoff installed.
fn a_released_copy_has_no_dependent(observation: &CopyObservation) -> (out: Option<CopyDependent>)
    ensures out is None ==> observation.journal_draining && observation.retained && !observation.serving && observation.durable_elsewhere
        && !observation.otherwise_named && !observation.checkpoint_decision && !observation.handoff_in_flight
        && !observation.recovery_or_replay && !observation.fork && !observation.erasure && !observation.installed,
{
    copy_dependent(observation)
}

/// A copy a pending fork, recovery, replay hold, retained basis, erasure obligation or in-flight handoff depends on stays in custody.
fn a_depended_on_copy_stays(observation: &CopyObservation) -> (out: Option<CopyDependent>)
    requires observation.fork || observation.recovery_or_replay || observation.erasure || observation.handoff_in_flight
        || observation.checkpoint_decision || observation.installed || observation.otherwise_named,
    ensures out is Some,
{
    copy_dependent(observation)
}

/// A copy is never released while it serves or before the ensemble holds every acknowledged entry.
fn a_copy_stays_until_its_entries_are_durable_elsewhere(observation: &CopyObservation) -> (out: Option<CopyDependent>)
    requires observation.serving || !observation.durable_elsewhere,
    ensures out is Some,
{
    copy_dependent(observation)
}

}
