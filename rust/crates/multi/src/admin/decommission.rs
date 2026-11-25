//! Logkeeper decommission (`CertifyLogkeeperDecommission`, `LogkeeperDecommissionRequiresNoCustody`).
//!
//! A draining logkeeper leaves the cell only under a certificate that no deployment record retains custody of its journal.

use rtdb_directory::{Cas, DecommissionCertificate, DeploymentRecord, Directory, Lifecycle, LogkeeperDecommission, MemberId, MemberRecord, MemberRole, MemberState, now_ms};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde_json::Value;

/// Whether `record` retains custody of `journal`'s stream: any mention of its identity or address, unless the record has finished disposal.
/// Matching the whole record keeps every custody field in scope, including ones added after this check.
pub(super) fn retains_custody(record: &DeploymentRecord, journal: &MemberRecord) -> bool {
    if record.lifecycle == Lifecycle::Deleted && record.disposal.is_some() { return false; }
    let value = serde_json::to_value(record).expect("a deployment record serializes");
    mentions(&value, journal.id.as_str(), &journal.address)
}

fn mentions(value: &Value, id: &str, address: &str) -> bool {
    let matches = |text: &str| text == id || !address.is_empty() && text == address;
    match value {
        Value::String(text) => matches(text),
        Value::Array(items) => items.iter().any(|item| mentions(item, id, address)),
        Value::Object(fields) => fields.iter().any(|(key, field)| matches(key) || mentions(field, id, address)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn checked(deployments: &[DeploymentRecord]) -> ([u8; 32], u64) {
    let mut versions: Vec<_> = deployments.iter().map(|record| (record.id.as_str(), record.version)).collect();
    versions.sort_unstable();
    let mut context = ring::digest::Context::new(&ring::digest::SHA256);
    for (id, version) in &versions {
        context.update(id.as_bytes());
        context.update(&[0]);
        context.update(&version.to_be_bytes());
    }
    let digest = context.finish().as_ref().try_into().expect("SHA-256 has 32 bytes");
    (digest, versions.len() as u64)
}

/// Certifies each draining logkeeper that no deployment record retains, and withdraws the certificate of one that a record references again.
/// Returns how many draining logkeepers hold a certificate after the pass.
pub async fn certify_logkeeper_decommissions(directory: &dyn Directory) -> Result<usize> {
    let draining: Vec<_> = directory.members().await?.into_iter()
        .filter(|member| member.role == MemberRole::Logkeeper && member.state == MemberState::Draining).collect();
    if draining.is_empty() { return Ok(0); }
    let deployments = directory.deployments().await?;
    let (deployments_sha256, deployments_checked) = checked(&deployments);
    let mut certified = 0;
    for member in draining {
        let certificate = (!deployments.iter().any(|record| retains_custody(record, &member))).then(|| DecommissionCertificate {
            journal: member.id.clone(), address: member.address.clone(), deployments_sha256, deployments_checked, certified_at_ms: now_ms(),
        });
        if record_certificate(directory, member, certificate).await? { certified += 1; }
    }
    Ok(certified)
}

/// Sets or clears the certificate on `member` while it stays the same draining journal; lease renewals in between are kept.
async fn record_certificate(directory: &dyn Directory, mut member: MemberRecord, certificate: Option<DecommissionCertificate>) -> Result<bool> {
    let journal = (member.id.clone(), member.address.clone());
    for _ in 0..8 {
        if member.state != MemberState::Draining || (member.id.clone(), member.address.clone()) != journal
            || matches!(member.decommission, Some(LogkeeperDecommission::Decommissioned { .. }))
        {
            return Ok(false);
        }
        let unchanged = match (&member.decommission, &certificate) {
            (None, None) => true,
            (Some(LogkeeperDecommission::Certified(held)), Some(fresh)) => held.deployments_sha256 == fresh.deployments_sha256
                && held.deployments_checked == fresh.deployments_checked && held.journal == fresh.journal && held.address == fresh.address,
            _ => false,
        };
        if unchanged { return Ok(certificate.is_some()); }
        let mut next = member.clone();
        next.decommission = certificate.clone().map(LogkeeperDecommission::Certified);
        match directory.replace_member(next, member.version).await? {
            Cas::Applied(_) => return Ok(certificate.is_some()),
            Cas::Conflict(Some(stored)) => member = stored,
            Cas::Conflict(None) => return Ok(false),
        }
    }
    Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/decommission-contended", format!("Logkeeper {} kept changing while being certified", journal.0)))
}

/// Retires a certified logkeeper for good, after checking again that no deployment record retains its journal.
/// Its process loses its lease, and its journal identity can never register again; its storage may then be destroyed.
pub async fn decommission_logkeeper(directory: &dyn Directory, id: &MemberId) -> Result<MemberRecord> {
    let unknown = || Anomaly::incorrect("rtdb.multi/unknown-member", format!("No member {id}"));
    let mut member = directory.member(id).await?.ok_or_else(unknown)?;
    let certificate = certified(&member)?;
    let Some(certificate) = certificate else { return Ok(member) };
    if let Some(record) = directory.deployments().await?.iter().find(|record| retains_custody(record, &member)) {
        return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/journal-custody",
            format!("Deployment {} retains custody of logkeeper {id}; its certificate is stale", record.id)));
    }
    for _ in 0..8 {
        if certified(&member)?.as_ref() != Some(&certificate) {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/decommission-changed",
                format!("Logkeeper {id} changed its state or certificate during decommission")));
        }
        let decommissioned_at_ms = now_ms();
        let mut next = member.clone();
        next.state = MemberState::Retired;
        next.lease_expires_at_ms = next.lease_expires_at_ms.min(decommissioned_at_ms);
        next.decommission = Some(LogkeeperDecommission::Decommissioned { certificate: certificate.clone(), decommissioned_at_ms });
        match directory.replace_member(next, member.version).await? {
            Cas::Applied(stored) => return Ok(stored),
            Cas::Conflict(Some(stored)) => member = stored,
            Cas::Conflict(None) => return Err(unknown()),
        }
    }
    Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/decommission-contended", format!("Logkeeper {id} kept changing while being decommissioned")))
}

/// The certificate a decommission rests on, or None once the logkeeper is already decommissioned.
fn certified(member: &MemberRecord) -> Result<Option<DecommissionCertificate>> {
    if member.role != MemberRole::Logkeeper {
        return Err(Anomaly::incorrect("rtdb.multi/not-a-logkeeper", format!("Member {} is not a logkeeper", member.id)));
    }
    match &member.decommission {
        Some(LogkeeperDecommission::Decommissioned { .. }) => Ok(None),
        Some(LogkeeperDecommission::Certified(certificate))
            if member.state == MemberState::Draining && certificate.journal == member.id && certificate.address == member.address => Ok(Some(certificate.clone())),
        _ => Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/decommission-uncertified",
            format!("Logkeeper {} has no decommission certificate: drain it and let the controller certify that no deployment retains its journal", member.id))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_directory::{DisposalReceipt, JournalPlacement, MemoryDirectory, ServingInventory};
    use rtdb_types::JournalEndpoint;

    fn logkeeper(id: &str, state: MemberState) -> MemberRecord {
        MemberRecord {
            id: MemberId(id.into()), role: MemberRole::Logkeeper, address: format!("{id}:7000"), control_address: None,
            availability_zone: "z1".into(), engine_version: "test".into(), lease_expires_at_ms: i64::MAX, state,
            drain_target_engine: None, pool: Default::default(), storage_namespaces: None, decommission: None, version: 0,
        }
    }

    fn deployment(name: &str, ensemble: &[&str]) -> DeploymentRecord {
        DeploymentRecord::new(name, "acme", ensemble.iter().map(|id| MemberId((*id).into())).collect(), 0)
    }

    #[test]
    fn every_custody_reference_to_a_journal_retains_it_until_disposal() {
        let journal = logkeeper("d", MemberState::Draining);
        assert!(!retains_custody(&deployment("shop", &["a", "b", "c"]), &journal));
        assert!(retains_custody(&deployment("shop", &["a", "b", "d"]), &journal));
        let mut placed = deployment("shop", &["a", "b", "c"]);
        placed.journal_placement = Some(JournalPlacement { operation: uuid::Uuid::new_v4(), destination: vec![MemberId("d".into())] });
        assert!(retains_custody(&placed, &journal));
        let mut copied = deployment("shop", &["a", "b", "c"]);
        copied.replica_journals.insert(JournalEndpoint::Member { address: "d:7000".into(), journal: "d".into() });
        assert!(retains_custody(&copied, &journal));
        let mut legacy = deployment("shop", &["a", "b", "c"]);
        legacy.log_copy_servers.push(JournalEndpoint::Address("d:7000".into()));
        assert!(retains_custody(&legacy, &journal), "an unbound legacy address still names the journal");
    }

    async fn cell(records: Vec<DeploymentRecord>) -> std::sync::Arc<MemoryDirectory> {
        let directory = MemoryDirectory::new();
        for member in [logkeeper("a", MemberState::Eligible), logkeeper("d", MemberState::Draining)] {
            directory.register_member(member).await.unwrap();
        }
        for record in records { directory.create_deployment(record).await.unwrap(); }
        directory
    }

    #[tokio::test]
    async fn a_logkeeper_without_a_certificate_cannot_be_decommissioned() {
        let directory = cell(vec![]).await;
        let refused = decommission_logkeeper(&*directory, &MemberId("d".into())).await.unwrap_err();
        assert_eq!(refused.code, "rtdb.multi/decommission-uncertified");
        assert_eq!(certify_logkeeper_decommissions(&*directory).await.unwrap(), 1);
        let refused = decommission_logkeeper(&*directory, &MemberId("a".into())).await.unwrap_err();
        assert_eq!(refused.code, "rtdb.multi/decommission-uncertified", "an eligible logkeeper is never certified");
    }

    #[tokio::test]
    async fn a_referenced_draining_logkeeper_is_not_certified() {
        let directory = cell(vec![deployment("shop", &["a", "d"])]).await;
        assert_eq!(certify_logkeeper_decommissions(&*directory).await.unwrap(), 0);
        assert_eq!(directory.member(&MemberId("d".into())).await.unwrap().unwrap().decommission, None);
    }

    #[tokio::test]
    async fn a_reference_after_certification_withdraws_the_certificate_and_refuses_the_decommission() {
        let directory = cell(vec![deployment("shop", &["a"])]).await;
        assert_eq!(certify_logkeeper_decommissions(&*directory).await.unwrap(), 1);
        let mut current = directory.deployment_by_name("shop").await.unwrap().unwrap();
        let version = current.version;
        current.log_copy_servers.push(JournalEndpoint::Member { address: "d:7000".into(), journal: "d".into() });
        directory.replace_deployment(current, version).await.unwrap().applied().unwrap();
        let refused = decommission_logkeeper(&*directory, &MemberId("d".into())).await.unwrap_err();
        assert_eq!(refused.code, "rtdb.multi/journal-custody");
        assert_eq!(certify_logkeeper_decommissions(&*directory).await.unwrap(), 0);
        assert_eq!(directory.member(&MemberId("d".into())).await.unwrap().unwrap().decommission, None);
    }

    #[tokio::test]
    async fn a_certified_logkeeper_is_decommissioned_for_good() {
        let mut disposed = deployment("gone", &["d"]);
        disposed.lifecycle = Lifecycle::Deleted;
        disposed.disposal = Some(Box::new(DisposalReceipt {
            deployment: disposed.id.clone(), incarnation: 0, epoch: 0,
            journals: [JournalEndpoint::Member { address: "d:7000".into(), journal: "d".into() }].into(),
            journals_retired_at_ms: 1, key_withdrawn_at_ms: 1, serving_prefix_observed_empty_at_ms: 1,
            serving_inventory: ServingInventory::S3VersionsAndUploads,
        }));
        let directory = cell(vec![deployment("shop", &["a"])]).await;
        assert!(!retains_custody(&disposed, &logkeeper("d", MemberState::Draining)), "a disposed record keeps only evidence");
        assert_eq!(certify_logkeeper_decommissions(&*directory).await.unwrap(), 1);
        let id = MemberId("d".into());
        let retired = decommission_logkeeper(&*directory, &id).await.unwrap();
        assert_eq!(retired.state, MemberState::Retired);
        assert!(matches!(retired.decommission, Some(LogkeeperDecommission::Decommissioned { .. })));
        assert!(!retired.is_live(now_ms()));
        assert_eq!(decommission_logkeeper(&*directory, &id).await.unwrap().version, retired.version, "decommission is idempotent");
        assert_eq!(certify_logkeeper_decommissions(&*directory).await.unwrap(), 0);
        let destination = super::super::journals::validate_destination(
            &deployment("shop", &["a"]), &[id.clone()], &[(id.clone(), retired)].into_iter().collect(), now_ms());
        assert!(destination.is_err(), "a decommissioned logkeeper is no placement destination");
    }
}
