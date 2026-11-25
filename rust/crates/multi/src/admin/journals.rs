//! Durability placement: which logkeeper journals hold a deployment's stream (`PlaceJournals`, `BalanceJournals`).
//!
//! A placement is a directory decision; the deployment's next holder copies the stream onto it under a handoff certificate and records the new ensemble.

use std::collections::{BTreeMap, BTreeSet};

use rtdb_directory::{Cas, DeploymentRecord, Directory, JournalPlacement, MemberId, MemberRecord, MemberRole, MemberState, effective_holder, now_ms};
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

/// Records at most `moves` journal placements: first off draining logkeepers, then from the most to the least loaded eligible one.
/// It never parks a holder; each placement applies at the deployment's next claim, and its destination counts as the deployment's load meanwhile.
pub async fn balance_journals(directory: &dyn Directory, moves: usize) -> Result<usize> {
    if moves == 0 { return Ok(0); }
    let members = members(directory).await?;
    let deployments = directory.deployments().await?;
    let mut placed = 0;
    for (index, destination) in plan_journal_placements(&deployments, &members, now_ms(), moves) {
        let current = &deployments[index];
        let mut next = current.clone();
        record_placement(&mut next, uuid::Uuid::new_v4(), &destination);
        if next == *current { continue; }
        if let Cas::Applied(_) = directory.replace_deployment(next, current.version).await? { placed += 1; }
    }
    Ok(placed)
}

/// The journals a deployment will be on: its pending placement's destination, else its ensemble.
fn planned_journals(record: &DeploymentRecord) -> &[MemberId] {
    record.journal_placement.as_ref().map_or(&record.ensemble, |placement| &placement.destination)
}

/// The placements to record, as indexes into `deployments` and their destinations.
pub(super) fn plan_journal_placements(
    deployments: &[DeploymentRecord], members: &BTreeMap<MemberId, MemberRecord>, now: i64, moves: usize,
) -> Vec<(usize, Vec<MemberId>)> {
    let eligible = |id: &MemberId| members.get(id).is_some_and(|member| member.role == MemberRole::Logkeeper && member.state == MemberState::Eligible && member.is_live(now));
    let draining = |id: &MemberId| members.get(id).is_some_and(|member| member.role == MemberRole::Logkeeper && member.state == MemberState::Draining);
    let mut load: BTreeMap<&MemberId, usize> = members.keys().filter(|id| eligible(*id)).map(|id| (id, 0)).collect();
    let movable: Vec<usize> = (0..deployments.len()).filter(|&index| require_movable(&deployments[index]).is_ok()).collect();
    for &index in &movable {
        for id in planned_journals(&deployments[index]) {
            if let Some(count) = load.get_mut(id) { *count += 1; }
        }
    }
    let mut plans = Vec::new();
    let mut planned = BTreeSet::new();
    for &index in &movable {
        if plans.len() == moves { return plans; }
        let record = &deployments[index];
        let mut next = planned_journals(record).to_vec();
        if !next.iter().any(|id| draining(id)) { continue; }
        for slot in 0..next.len() {
            if !draining(&next[slot]) { continue; }
            let mut candidates: Vec<_> = load.iter().filter(|&(&id, _)| !next.contains(id)).map(|(id, count)| (*count, *id)).collect();
            candidates.sort();
            let replacement = candidates.into_iter().map(|(_, id)| id).find(|id| {
                let mut trial = next.clone();
                trial[slot] = (*id).clone();
                trial.iter().all(|journal| eligible(journal) || draining(journal))
                    && distinct_addresses(&trial, members) && zone_count(&trial, members) >= zone_count(&record.ensemble, members)
            });
            let Some(replacement) = replacement else { break };
            next[slot] = replacement.clone();
            *load.get_mut(replacement).expect("a candidate is eligible") += 1;
        }
        if validate_destination(record, &next, members, now).is_ok() {
            plans.push((index, next));
            planned.insert(index);
        }
    }
    while plans.len() < moves {
        let mut ranked: Vec<_> = load.iter().map(|(id, count)| (*count, *id)).collect();
        ranked.sort();
        let chosen = ranked.iter().rev().find_map(|&(busiest, source)| {
            ranked.iter().take_while(|(count, _)| *count + 1 < busiest).find_map(|&(_, target)| {
                movable.iter().filter(|index| !planned.contains(*index)).find_map(|&index| {
                    let journals = planned_journals(&deployments[index]);
                    if !journals.contains(source) || journals.contains(target) { return None; }
                    let next: Vec<_> = journals.iter().map(|id| if id == source { target.clone() } else { id.clone() }).collect();
                    validate_destination(&deployments[index], &next, members, now).is_ok().then(|| (index, source, target, next))
                })
            })
        });
        let Some((index, source, target, next)) = chosen else { break };
        *load.get_mut(source).expect("ranked") -= 1;
        *load.get_mut(target).expect("ranked") += 1;
        plans.push((index, next));
        planned.insert(index);
    }
    plans
}

fn distinct_addresses(journals: &[MemberId], members: &BTreeMap<MemberId, MemberRecord>) -> bool {
    let addresses: BTreeSet<_> = journals.iter().filter_map(|id| members.get(id)).map(|member| member.address.as_str()).collect();
    addresses.len() == journals.len()
}

fn zone_count(journals: &[MemberId], members: &BTreeMap<MemberId, MemberRecord>) -> usize {
    journals.iter().filter_map(|id| members.get(id)).map(|member| member.availability_zone.as_str()).collect::<BTreeSet<_>>().len()
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
    fn a_draining_logkeeper_is_evacuated_onto_the_least_loaded_journal_in_its_zone_spread() {
        let (record, mut members) = cell();
        members.get_mut(&MemberId("a".into())).unwrap().state = MemberState::Draining;
        members.insert(MemberId("f".into()), logkeeper("f", "f:1", "z4"));
        let plans = plan_journal_placements(&[record], &members, 0, 8);
        assert_eq!(plans.len(), 1);
        let (_, destination) = &plans[0];
        assert!(!destination.contains(&MemberId("a".into())));
        assert!(destination.contains(&MemberId("b".into())) && destination.contains(&MemberId("c".into())));
    }

    #[test]
    fn balancing_moves_one_journal_from_the_busiest_to_an_idle_logkeeper_and_then_settles() {
        let (record, mut members) = cell();
        members.remove(&MemberId("e".into()));
        members.get_mut(&MemberId("d".into())).unwrap().availability_zone = "z4".into();
        let deployments: Vec<_> = (0..4).map(|_| record.clone()).collect();
        let plans = plan_journal_placements(&deployments, &members, 0, 8);
        assert!(!plans.is_empty());
        let mut load: BTreeMap<MemberId, usize> = BTreeMap::new();
        for (index, deployment) in deployments.iter().enumerate() {
            let journals = plans.iter().find(|(planned, _)| *planned == index).map_or(&deployment.ensemble, |(_, next)| next);
            for id in journals { *load.entry(id.clone()).or_default() += 1; }
        }
        let (most, least) = (load.values().max().unwrap(), members.keys().map(|id| load.get(id).copied().unwrap_or(0)).min().unwrap());
        assert!(most - least <= 1, "{load:?}");
        assert!(plans.iter().all(|(_, next)| validate_destination(&record, next, &members, 0).is_ok()));
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
