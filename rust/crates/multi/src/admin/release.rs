//! Releasing a live deployment's retained journal copy (`JournalCopyLeavesCustodyOnlyByRelease`).
//!
//! Copy custody keeps every journal a deployment's stream may have reached until disposal, so an evacuated logkeeper would otherwise wait for every deployment it served to be deleted.
//! A release ends that custody early for one draining journal the deployment has moved off: it retires the copy on that journal, then drops the journal from the record under a release certificate in one conditional write.

use rtdb_directory::{
    CopyDependent, CopyObservation, DeploymentRecord, Directory, EnsembleHandoff, JournalCopyRelease, Lifecycle, LogkeeperDecommission,
    ReplaceOutcome,
    MemberId, MemberRecord, MemberRole, MemberState, StorageRegion, copy_dependent, now_ms,
};
use rtdb_logkeeper::StreamConfig;
use rtdb_types::{Anomaly, AnomalyCategory, JournalEndpoint, Result};

/// A retirement carries no payload, so this only bounds the session's frames.
const RETIREMENT_MESSAGE_BYTES: usize = 4 << 20;

fn names(endpoint: &JournalEndpoint, journal: &MemberRecord) -> bool {
    endpoint.journal() == Some(journal.id.as_str()) || !journal.address.is_empty() && endpoint.address() == journal.address
}

fn retains_copy(record: &DeploymentRecord, journal: &MemberRecord) -> bool {
    record.replica_journals.iter().chain(&record.log_copy_servers).any(|endpoint| names(endpoint, journal))
}

fn without_copy(record: &DeploymentRecord, journal: &MemberRecord) -> DeploymentRecord {
    let mut next = record.clone();
    next.replica_journals.retain(|endpoint| !names(endpoint, journal));
    next.log_copy_servers.retain(|endpoint| !names(endpoint, journal));
    next
}

/// The recorded handoff that installed the current ensemble, so that it and the checkpoint hold every acknowledged entry.
fn durable_handoff(record: &DeploymentRecord) -> Option<&EnsembleHandoff> {
    let mut ensemble = record.ensemble.clone();
    ensemble.sort();
    record.ensemble_handoff.as_ref().filter(|handoff| handoff.destination == ensemble && handoff.epoch <= record.epoch
        && handoff.checkpoint_boundary.is_none_or(|boundary| record.checkpoint.is_some_and(|checkpoint| checkpoint.boundary >= boundary)))
}

/// What could still need `journal`'s copy of `record`'s stream, among the cell's `deployments` and installed-journal reservations.
pub(crate) fn observe(record: &DeploymentRecord, journal: &MemberRecord, deployments: &[DeploymentRecord], installed: &[uuid::Uuid]) -> CopyObservation {
    let checkpoint_decision = match crate::hosted_recovery::checkpoint_decision(record) {
        Ok(None) => false,
        Ok(Some(proof)) => proof.certificate.statement.scope.journals.iter().any(|id| id == journal.id.as_str()),
        Err(_) => true,
    };
    CopyObservation {
        journal_draining: journal.role == MemberRole::Logkeeper && journal.state == MemberState::Draining,
        retained: retains_copy(record, journal),
        serving: record.ensemble.contains(&journal.id),
        durable_elsewhere: durable_handoff(record).is_some(),
        otherwise_named: without_copy(record, journal).names_journal(journal),
        checkpoint_decision,
        handoff_in_flight: record.journal_placement.is_some() || record.placement_move.is_some() || record.move_intent.is_some()
            || !record.writer_handoffs.is_empty() || record.move_from.is_some() || record.cell_transfer.is_some() || record.cell_move.is_some()
            || rtdb_directory::classic_flight::commission(record).is_some() || record.classic_catalog_drain.is_some() || record.checkpoint_transfer_fence.is_some()
            || record.checkpoint_transfer_terminal.is_some() || record.checkpoint_transfer_bootstrap.is_some()
            || record.checkpoint_transfer_replica.is_some() || record.checkpoint_transfer_activation.is_some(),
        recovery_or_replay: record.checkpoint_recovery.as_ref().is_some_and(|recovery| recovery.completed_at_ms.is_none())
            || record.serving_replay_capture.is_some() || record.checkpoint_stage.is_some() || record.transfer_replay_namespace.is_some()
            || record.storage_regions.values().any(|region| matches!(region, StorageRegion::ReplicaReplay { .. }))
            || record.replica_payload_trim.as_ref().is_some_and(|trim| !trim.applied) || !record.checkpoint_suffix_pins.is_empty(),
        fork: record.fork_stage.is_some()
            || deployments.iter().any(|other| other.parent.as_deref() == Some(record.id.as_str()) && other.fork_stage.is_some()),
        erasure: !matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined) || record.deletion_record.is_some()
            || record.deletion_observation.is_some() || record.disposal.is_some(),
        installed: uuid::Uuid::parse_str(&record.id).is_ok_and(|id| installed.contains(&id)),
    }
}

/// Retires `journal`'s copy of deployment `id`'s stream and drops the journal from its copy custody.
/// Refuses while anything still depends on the copy; a copy already released is returned unchanged.
pub async fn release_journal_copy(directory: &dyn Directory, id: &str, journal: &MemberId) -> Result<DeploymentRecord> {
    let unknown_member = || Anomaly::incorrect("rtdb.multi/unknown-member", format!("No member {journal}"));
    let member = directory.member(journal).await?.ok_or_else(unknown_member)?;
    let mut retired = None;
    for _ in 0..8 {
        let record = directory.deployment(id).await?
            .ok_or_else(|| Anomaly::incorrect("rtdb.multi/unknown-installation", format!("No installation has ID {id}")))?;
        let current = directory.member(journal).await?.ok_or_else(unknown_member)?;
        if current.eligibility_epoch != member.eligibility_epoch || current.address != member.address {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/journal-release-changed",
                format!("Logkeeper {journal} changed its eligibility while its copy of {id} was being released")));
        }
        let deployments = directory.deployments().await?;
        let installed = directory.installed_journal_reservations().await?;
        let observation = observe(&record, &member, &deployments, &installed);
        match copy_dependent(&observation) {
            None => {}
            Some(CopyDependent::NotRetained) if record.journal_releases.iter().any(|release| release.journal == member.id) => return Ok(record),
            Some(dependent) => return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/journal-copy-dependent",
                format!("Deployment {id} still depends on its copy on logkeeper {journal}: {dependent:?}"))),
        }
        let handoff = durable_handoff(&record).cloned().expect("a releasable copy has a durable handoff");
        let stream = record.stream("replica");
        if retired.as_ref() != Some(&(stream.clone(), record.epoch)) {
            if record.epoch > 0 {
                let endpoint = JournalEndpoint::Member { address: member.address.clone(), journal: member.id.0.clone() };
                let config = StreamConfig { servers: vec![endpoint], stream: stream.clone(), write_quorum: 1, maximum_message_bytes: RETIREMENT_MESSAGE_BYTES };
                rtdb_logkeeper::retire(&config, record.epoch).await?;
            }
            retired = Some((stream, record.epoch));
        }
        let mut next = without_copy(&record, &member);
        next.journal_releases.push(JournalCopyRelease {
            journal: member.id.clone(), address: member.address.clone(), incarnation: record.incarnation, epoch: record.epoch,
            journal_epoch: member.eligibility_epoch, handoff, released_at_ms: now_ms(),
        });
        match directory.replace_deployment(next, record.version).await {
            ReplaceOutcome::Applied(stored) => return Ok(stored),
            ReplaceOutcome::Conflict(_) => {}
            ReplaceOutcome::Refused(error) | ReplaceOutcome::Unknown(error) => return Err(error),
        }
    }
    Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/journal-release-contended", format!("Deployment {id} kept changing while its copy on {journal} was being released")))
}

/// Releases up to `limit` retained copies on draining logkeepers from deployments that have moved off them, skipping any a dependent still keeps.
pub async fn release_journal_copies(directory: &dyn Directory, limit: usize) -> Result<usize> {
    let draining: Vec<_> = directory.members().await?.into_iter().filter(|member| member.role == MemberRole::Logkeeper
        && member.state == MemberState::Draining && !matches!(member.decommission, Some(LogkeeperDecommission::Decommissioned { .. }))).collect();
    if draining.is_empty() || limit == 0 { return Ok(0); }
    let mut released = 0;
    for record in directory.deployments().await? {
        for journal in draining.iter().filter(|journal| retains_copy(&record, journal) && !record.ensemble.contains(&journal.id)) {
            if released == limit { return Ok(released); }
            if release_journal_copy(directory, &record.id, &journal.id).await.is_ok() { released += 1; }
        }
    }
    Ok(released)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_directory::{Checkpoint, CheckpointRecovery, ForkPreparation, ForkStage, JournalPlacement, MemoryDirectory, ReplicaPayloadTrim};

    fn logkeeper(id: &str, state: MemberState) -> MemberRecord {
        MemberRecord {
            id: MemberId(id.into()), role: MemberRole::Logkeeper, address: format!("{id}:7000"), control_address: None,
            availability_zone: "z1".into(), engine_version: "test".into(), lease_expires_at_ms: i64::MAX, state,
            drain_target_engine: None, pool: Default::default(), storage_namespaces: None, decommission: None, eligibility_epoch: 0, version: 0,
        }
    }

    /// A deployment that moved from {a, b, d} onto {a, b, c} under a recorded handoff, keeping its copy on d.
    fn evacuated() -> DeploymentRecord {
        let ids = |ids: &[&str]| ids.iter().map(|id| MemberId((*id).into())).collect::<Vec<_>>();
        let mut record = DeploymentRecord::new("shop", "acme", ids(&["a", "b", "c"]), 0);
        record.epoch = 2;
        for id in ["a", "b", "c", "d"] {
            record.replica_journals.insert(JournalEndpoint::Member { address: format!("{id}:7000"), journal: id.into() });
        }
        record.ensemble_handoff = Some(EnsembleHandoff { epoch: 2, source: ids(&["a", "b", "d"]), destination: ids(&["a", "b", "c"]), first: 0, through: 9, checkpoint_boundary: None });
        record
    }

    fn dependent(record: &DeploymentRecord, journal: &MemberRecord) -> Option<CopyDependent> {
        copy_dependent(&observe(record, journal, std::slice::from_ref(record), &[]))
    }

    #[test]
    fn an_evacuated_copy_with_no_dependents_may_leave_custody() {
        assert_eq!(dependent(&evacuated(), &logkeeper("d", MemberState::Draining)), None);
    }

    #[test]
    fn every_dependent_keeps_the_copy() {
        let draining = logkeeper("d", MemberState::Draining);
        assert_eq!(dependent(&evacuated(), &logkeeper("d", MemberState::Eligible)), Some(CopyDependent::NotDraining));
        assert_eq!(dependent(&evacuated(), &logkeeper("e", MemberState::Draining)), Some(CopyDependent::NotRetained));

        let mut serving = evacuated();
        serving.ensemble.push(MemberId("d".into()));
        assert_eq!(dependent(&serving, &draining), Some(CopyDependent::Serving));

        let mut unattested = evacuated();
        unattested.ensemble_handoff = None;
        assert_eq!(dependent(&unattested, &draining), Some(CopyDependent::NotDurable), "without a recorded handoff the ensemble may not hold every entry");
        let mut stale = evacuated();
        stale.ensemble_handoff.as_mut().unwrap().destination = vec![MemberId("a".into())];
        assert_eq!(dependent(&stale, &draining), Some(CopyDependent::NotDurable), "a handoff to another ensemble attests nothing about this one");
        let mut regressed = evacuated();
        regressed.ensemble_handoff.as_mut().unwrap().checkpoint_boundary = Some(5);
        assert_eq!(dependent(&regressed, &draining), Some(CopyDependent::NotDurable), "the checkpoint the handoff relied on is gone");

        let mut source = evacuated();
        source.move_from = Some(vec![JournalEndpoint::Member { address: "d:7000".into(), journal: "d".into() }]);
        assert_eq!(dependent(&source, &draining), Some(CopyDependent::OtherReference), "a transfer source still reads the copy");

        let mut placement = evacuated();
        placement.journal_placement = Some(JournalPlacement { operation: uuid::Uuid::new_v4(), destination: placement.ensemble.clone() });
        assert_eq!(dependent(&placement, &draining), Some(CopyDependent::HandoffInFlight));

        let mut recovering = evacuated();
        recovering.checkpoint = Some(Checkpoint { block: 1, boundary: 1, source: 1, system_time: 0, root_sha256: None });
        recovering.checkpoint_recovery = Some(Box::new(CheckpointRecovery {
            operation: uuid::Uuid::new_v4(), checkpoint: recovering.checkpoint.unwrap(), epoch: 2, previous_lifecycle: Lifecycle::Live,
            archive: "archive".into(), manifest_sha256: String::new(), completed_at_ms: None,
        }));
        assert_eq!(dependent(&recovering, &draining), Some(CopyDependent::RecoveryOrReplay), "a pending recovery may replay the copy");
        let mut trimming = evacuated();
        trimming.replica_payload_trim = Some(Box::new(ReplicaPayloadTrim {
            incarnation: 0, log_epoch: 0, through: 3, generation: None, applied: false, body_anchor: None, candidates: Vec::new(), journal_units: Default::default(),
        }));
        assert_eq!(dependent(&trimming, &draining), Some(CopyDependent::RecoveryOrReplay), "an unapplied trim keeps the retained basis");

        let parent = evacuated();
        let mut child = DeploymentRecord::new("shop-fork", "acme", parent.ensemble.clone(), 0);
        child.parent = Some(parent.id.clone());
        let with_child = |child: &DeploymentRecord| copy_dependent(&observe(&parent, &draining, &[parent.clone(), child.clone()], &[]));
        assert_eq!(with_child(&child), None, "a published fork has its own stream");
        child.fork_stage = Some(ForkStage { source_lineage: parent.lineage.clone(), preparation: ForkPreparation { token: uuid::Uuid::new_v4(), generation: 0 } });
        assert_eq!(with_child(&child), Some(CopyDependent::Fork), "a staged fork may still read its parent's stream");

        let mut deleting = evacuated();
        deleting.lifecycle = Lifecycle::Deleting;
        assert_eq!(dependent(&deleting, &draining), Some(CopyDependent::Erasure), "disposal retires every retained copy");

        let installed = uuid::Uuid::new_v4();
        let mut reserved = evacuated();
        reserved.id = installed.to_string();
        assert_eq!(copy_dependent(&observe(&reserved, &draining, &[], &[installed])), Some(CopyDependent::Installed));
    }

    #[tokio::test]
    async fn a_release_drops_the_copy_under_a_certificate_and_lets_the_journal_be_certified() {
        let directory = MemoryDirectory::new();
        for member in ["a", "b", "c"].map(|id| logkeeper(id, MemberState::Eligible)).into_iter().chain([logkeeper("d", MemberState::Draining)]) {
            directory.register_member(member).await.unwrap();
        }
        let mut record = evacuated();
        record.epoch = 0;
        record.ensemble_handoff.as_mut().unwrap().epoch = 0;
        let record = directory.create_deployment(record).await.unwrap();
        let journal = MemberId("d".into());
        assert_eq!(super::super::certify_logkeeper_decommissions(&*directory).await.unwrap(), 0);

        let released = release_journal_copy(&*directory, &record.id, &journal).await.unwrap();
        assert!(!released.replica_journals.iter().any(|endpoint| endpoint.journal() == Some("d")));
        let [certificate] = released.journal_releases.as_slice() else { panic!("one release certificate") };
        assert_eq!((&certificate.journal, certificate.address.as_str(), certificate.incarnation), (&journal, "d:7000", record.incarnation));
        assert_eq!(release_journal_copy(&*directory, &record.id, &journal).await.unwrap().version, released.version, "a release is idempotent");

        assert_eq!(super::super::certify_logkeeper_decommissions(&*directory).await.unwrap(), 1);
        super::super::decommission_logkeeper(&*directory, &journal).await.unwrap();
    }

    #[tokio::test]
    async fn the_controller_pass_releases_only_copies_nothing_keeps() {
        let directory = MemoryDirectory::new();
        for member in ["a", "b", "c"].map(|id| logkeeper(id, MemberState::Eligible)).into_iter().chain([logkeeper("d", MemberState::Draining)]) {
            directory.register_member(member).await.unwrap();
        }
        let mut free = evacuated();
        free.epoch = 0;
        free.ensemble_handoff.as_mut().unwrap().epoch = 0;
        let mut kept = free.clone();
        kept.id = uuid::Uuid::new_v4().to_string();
        kept.name = "kept".into();
        kept.ensemble_handoff = None;
        directory.create_deployment(free).await.unwrap();
        let kept = directory.create_deployment(kept).await.unwrap();
        assert_eq!(release_journal_copies(&*directory, 8).await.unwrap(), 1);
        assert_eq!(directory.deployment(&kept.id).await.unwrap().unwrap().version, kept.version, "a copy without a durable handoff stays");
        assert_eq!(super::super::certify_logkeeper_decommissions(&*directory).await.unwrap(), 0);
    }
}
