//! The journal reference fence (`JournalReferenceRequiresCurrentEligibility`).
//!
//! The directory has no transaction spanning a member record and a deployment record, so a writer's view of a logkeeper can be stale by the time its deployment write lands.
//! Each member therefore carries an eligibility epoch that moves on with every change to its state or decommission, and a deployment write that names a logkeeper journal its stored record did not name commits only while that logkeeper accepts references at the epoch the directory observed.
//! A decommission certificate is such a change, so no reference written after it can land, and the certifying scan has nothing left to miss.

use std::collections::BTreeSet;

use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde_json::Value;

use crate::journal_admission::{JournalObservation, epoch_follows, fenced_journals};
use crate::{DeploymentRecord, MemberId, MemberRecord};

/// Every string a deployment record names, as a value or as a field name.
/// Matching the whole record keeps every custody field in scope, including ones added after this check.
pub(crate) struct Mentions(BTreeSet<String>);

impl Mentions {
    pub(crate) fn of(record: &DeploymentRecord) -> Self {
        fn collect(value: Value, into: &mut BTreeSet<String>) {
            match value {
                Value::String(text) => { into.insert(text); }
                Value::Array(items) => items.into_iter().for_each(|item| collect(item, into)),
                Value::Object(fields) => for (key, field) in fields {
                    collect(field, into);
                    into.insert(key);
                },
                Value::Null | Value::Bool(_) | Value::Number(_) => {}
            }
        }
        let mut strings = BTreeSet::new();
        let mut value = serde_json::to_value(record).expect("a deployment record serializes");
        // Evidence of a handoff or a release names journals the record no longer retains; the ensemble names the handoff's destination.
        if let Value::Object(fields) = &mut value {
            fields.remove("journal_releases");
            fields.remove("ensemble_handoff");
        }
        collect(value, &mut strings);
        Self(strings)
    }

    pub(crate) fn names(&self, journal: &MemberRecord) -> bool {
        self.0.contains(journal.id.as_str()) || !journal.address.is_empty() && self.0.contains(&journal.address)
    }

    fn adds_to(&self, current: Option<&Self>) -> bool {
        current.is_none_or(|current| !self.0.is_subset(&current.0))
    }
}

impl DeploymentRecord {
    /// Whether any field of this record names `journal`'s identity or address.
    pub fn names_journal(&self, journal: &MemberRecord) -> bool { Mentions::of(self).names(journal) }
}

/// Whether `next` names any string that `current` does not, so that it may add a journal reference.
pub fn may_add_journal_references(current: Option<&DeploymentRecord>, next: &DeploymentRecord) -> bool {
    Mentions::of(next).adds_to(current.map(Mentions::of).as_ref())
}

/// The logkeepers that `next` names and `current` does not, with the eligibility epoch each was observed at.
/// Refuses when one of them no longer accepts references, which a decommission certificate ends.
/// A directory commits `next` only while each returned member still holds the returned epoch.
pub fn journal_fence<'a>(
    current: Option<&DeploymentRecord>, next: &DeploymentRecord, members: impl IntoIterator<Item = &'a MemberRecord>,
) -> Result<Vec<(MemberId, u64)>> {
    let logkeepers: Vec<_> = members.into_iter().filter(|member| member.role == crate::MemberRole::Logkeeper).collect();
    if logkeepers.is_empty() { return Ok(Vec::new()); }
    let named = Mentions::of(next);
    let current = current.map(Mentions::of);
    if !named.adds_to(current.as_ref()) { return Ok(Vec::new()); }
    let observations: Vec<_> = logkeepers.iter().map(|journal| JournalObservation {
        newly_named: named.names(journal) && !current.as_ref().is_some_and(|current| current.names(journal)),
        accepts_references: journal.accepts_journal_references(),
    }).collect();
    match fenced_journals(&observations) {
        Some(fenced) => Ok(fenced.into_iter().zip(&logkeepers).filter(|(fenced, _)| *fenced)
            .map(|(_, journal)| (journal.id.clone(), journal.eligibility_epoch)).collect()),
        None => {
            let refused = observations.iter().zip(&logkeepers).find(|(observation, _)| observation.newly_named && !observation.accepts_references)
                .map(|(_, journal)| *journal).expect("a refused fence names a journal that accepts no references");
            Err(journal_retiring(&next.id, refused))
        }
    }
}

pub(crate) fn journal_retiring(deployment: &str, journal: &MemberRecord) -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.directory/journal-retiring",
        format!("Deployment {deployment} cannot gain a reference to logkeeper {}: it is certified for decommission", journal.id))
}

/// Refuses a member write that changes its state or decommission without moving its eligibility epoch on.
pub(crate) fn require_epoch_follows(stored: &MemberRecord, next: &MemberRecord) -> Result<()> {
    if epoch_follows(stored.eligibility_epoch, next.eligibility_epoch, !stored.same_eligibility(next)) { return Ok(()); }
    Err(Anomaly::incorrect("rtdb.directory/eligibility-epoch",
        format!("Member {} changed its state or decommission without moving its eligibility epoch on from {}", next.id, stored.eligibility_epoch)))
}
