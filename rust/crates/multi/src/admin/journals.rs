//! Durability placement: which logkeeper journals hold a deployment's stream (`PlaceJournals`).
//!
//! A placement is a directory decision; the deployment's next holder copies the stream onto it under a handoff certificate and records the new ensemble.

use std::collections::{BTreeMap, BTreeSet};

use rtdb_directory::{DeploymentRecord, Directory, JournalPlacement, MemberId, MemberRecord, MemberRole, MemberState, effective_holder, now_ms};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

use crate::control::{ControlKey, Request};

use super::{control_address, ensure_held, expect, members, record, update};

/// Records `destination` as the journals of the deployment named `name`, and has its next holder move the stream there.
/// A held deployment is parked and activated again so the move happens now rather than at its next claim.
pub async fn place_journals(directory: &dyn Directory, name: &str, destination: Vec<MemberId>, control: &ControlKey) -> Result<DeploymentRecord> {
    let members = members(directory).await?;
    let current = record(directory, name).await?;
    validate_destination(&current, &destination, &members, now_ms())?;
    let operation = uuid::Uuid::new_v4();
    let placed = update(directory, name, |record| {
        require_movable(record)?;
        record_placement(record, operation, &destination);
        Ok(())
    }).await?;
    let Some(placement) = placed.journal_placement.clone() else { return Ok(placed) };
    if let Some(holder) = effective_holder(&placed, &members, now_ms()) && !placed.is_resident_idle() {
        expect(&control_address(&members, holder)?, Request::Park { name: name.to_owned() }, control).await?;
    }
    ensure_held(directory, name, control).await?;
    let settled = record(directory, name).await?;
    if settled.journal_placement.as_ref().is_some_and(|pending| pending.operation == placement.operation) {
        return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/journal-placement-pending",
            format!("{name} is held but has not moved to its recorded journals; its next claim retries the move")));
    }
    Ok(settled)
}

/// Sets the placement on `record`, or clears it when the ensemble already names the destination.
/// A pending placement for the same destination keeps its operation.
pub(super) fn record_placement(record: &mut DeploymentRecord, operation: uuid::Uuid, destination: &[MemberId]) {
    let placement = JournalPlacement { operation, destination: destination.to_vec() };
    if placement.reached_by(&record.ensemble) {
        record.journal_placement = None;
    } else if !record.journal_placement.as_ref().is_some_and(|pending| pending.reached_by(destination)) {
        record.journal_placement = Some(placement);
    }
}

/// A placement changes the journals of a live residence only, outside a transfer, recovery, staged checkpoint or fork.
pub(super) fn require_movable(record: &DeploymentRecord) -> Result<()> {
    if record.writer_pool().is_none() || record.ensemble.is_empty() || record.move_from.is_some() || record.cell_transfer.is_some()
        || record.checkpoint_recovery.is_some() || record.checkpoint_stage.is_some() || record.fork_stage.is_some()
    {
        return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/journal-placement-unavailable",
            format!("{} cannot change its journals in its current state", record.name)));
    }
    Ok(())
}

/// The destination keeps the ensemble's width with distinct eligible journals at distinct addresses, spread over at least as many zones as the ensemble.
pub(super) fn validate_destination(record: &DeploymentRecord, destination: &[MemberId], members: &BTreeMap<MemberId, MemberRecord>, now: i64) -> Result<()> {
    let invalid = |message: String| Err(Anomaly::incorrect("rtdb.multi/journal-placement", message));
    if destination.len() != record.ensemble.len() {
        return invalid(format!("{} keeps {} journals, and {} were named", record.name, record.ensemble.len(), destination.len()));
    }
    let mut ids = BTreeSet::new();
    let mut addresses = BTreeSet::new();
    let mut zones = BTreeSet::new();
    for id in destination {
        let Some(member) = members.get(id).filter(|member| member.role == MemberRole::Logkeeper && member.state == MemberState::Eligible && member.is_live(now)) else {
            return invalid(format!("{id} is not an eligible live logkeeper"));
        };
        if !ids.insert(id) || !addresses.insert(member.address.as_str()) {
            return invalid(format!("{id} repeats a journal or an address in the destination"));
        }
        zones.insert(member.availability_zone.as_str());
    }
    let spread = record.ensemble.iter().filter_map(|id| members.get(id)).map(|member| member.availability_zone.as_str()).collect::<BTreeSet<_>>().len();
    if zones.len() < spread {
        return invalid(format!("The destination spans {} availability zones and the ensemble spans {spread}", zones.len()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn logkeeper(id: &str, address: &str, zone: &str) -> MemberRecord {
        MemberRecord {
            id: MemberId(id.into()), role: MemberRole::Logkeeper, address: address.into(), control_address: None,
            availability_zone: zone.into(), engine_version: "test".into(), lease_expires_at_ms: i64::MAX, state: MemberState::Eligible,
            drain_target_engine: None, pool: Default::default(), storage_namespaces: None, version: 0,
        }
    }

    fn cell() -> (DeploymentRecord, BTreeMap<MemberId, MemberRecord>) {
        let members: BTreeMap<_, _> = [("a", "a:1", "z1"), ("b", "b:1", "z2"), ("c", "c:1", "z3"), ("d", "d:1", "z1"), ("e", "e:1", "z1")]
            .into_iter().map(|(id, address, zone)| (MemberId(id.into()), logkeeper(id, address, zone))).collect();
        let record = DeploymentRecord::new("shop", "acme", ["a", "b", "c"].map(|id| MemberId(id.into())).to_vec(), 0);
        (record, members)
    }

    fn ids(names: &[&str]) -> Vec<MemberId> { names.iter().map(|id| MemberId((*id).into())).collect() }

    #[test]
    fn a_destination_keeps_the_width_and_the_zone_spread_of_the_ensemble() {
        let (record, members) = cell();
        assert!(validate_destination(&record, &ids(&["d", "b", "c"]), &members, 0).is_ok());
        assert!(validate_destination(&record, &ids(&["d", "b"]), &members, 0).is_err());
        assert!(validate_destination(&record, &ids(&["d", "e", "c"]), &members, 0).is_err());
        assert!(validate_destination(&record, &ids(&["d", "d", "c"]), &members, 0).is_err());
        assert!(validate_destination(&record, &ids(&["d", "b", "x"]), &members, 0).is_err());
    }

    #[test]
    fn a_draining_logkeeper_is_not_a_destination() {
        let (record, mut members) = cell();
        members.get_mut(&MemberId("d".into())).unwrap().state = MemberState::Draining;
        assert!(validate_destination(&record, &ids(&["d", "b", "c"]), &members, 0).is_err());
    }

    #[test]
    fn a_placement_the_ensemble_already_names_is_cleared_and_a_repeated_one_keeps_its_operation() {
        let (mut record, _) = cell();
        let first = uuid::Uuid::new_v4();
        record_placement(&mut record, first, &ids(&["d", "b", "c"]));
        record_placement(&mut record, uuid::Uuid::new_v4(), &ids(&["c", "d", "b"]));
        assert_eq!(record.journal_placement.as_ref().map(|placement| placement.operation), Some(first));
        record_placement(&mut record, uuid::Uuid::new_v4(), &ids(&["c", "a", "b"]));
        assert_eq!(record.journal_placement, None);
    }
}
