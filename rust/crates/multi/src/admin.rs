//! Cell administration: creating deployments (`CreateDeployment`) and confirming dead members (`ExpireLease`).

use std::sync::Arc;
use std::time::Duration;

use rtdb_directory::{Cas, DeploymentRecord, Directory, MemberRecord, MemberRole, MemberState, now_ms};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use tokio_util::sync::CancellationToken;

/// Creates a deployment, choosing its ensemble from the live logkeepers in its affinity order.
/// Allocates identity and nothing else: no objects, no log state, no compute.
pub async fn create_deployment(directory: &dyn Directory, name: &str, org: &str, replicas: usize) -> Result<DeploymentRecord> {
    let now = now_ms();
    let logkeepers: Vec<MemberRecord> = directory.members().await?.into_iter().filter(|member| member.role == MemberRole::Logkeeper && member.is_live(now)).collect();
    if logkeepers.len() < replicas {
        return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/too-few-logkeepers", format!("{replicas} logkeepers are needed and {} are live", logkeepers.len())));
    }
    let mut record = DeploymentRecord::new(name, org, Vec::new(), now);
    let ranked = rtdb_affinity::rank(&record.storage_identity(), &logkeepers, |member| member.id.as_str());
    record.ensemble = ranked.into_iter().take(replicas).map(|member| member.id.clone()).collect();
    directory.create_deployment(record).await
}

/// Starts scaling a node in: it keeps serving what it holds, receives no new placements, and parks everything it holds so that each deployment's next request activates it elsewhere.
pub async fn drain(directory: &dyn Directory, member: &rtdb_directory::MemberId) -> Result<()> {
    for _ in 0..8 {
        let current = directory.member(member).await?.ok_or_else(|| Anomaly::incorrect("rtdb.multi/unknown-member", format!("No member {member}")))?;
        if current.state != MemberState::Eligible { return Ok(()); }
        let mut draining = current.clone();
        draining.state = MemberState::Draining;
        if let Cas::Applied(_) = directory.replace_member(draining, current.version).await? { return Ok(()); }
    }
    Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/drain-contended", "The member kept changing while being drained"))
}

/// Marks every member whose lease expired more than `grace` ago as dead.
/// Placements need no write: a dead member's holdings stop counting through `effective_holder`; this keeps the member list honest.
pub async fn reap_members(directory: &dyn Directory, grace: Duration) -> Result<usize> {
    let mut reaped = 0;
    let threshold = now_ms() - grace.as_millis() as i64;
    for member in directory.members().await? {
        if matches!(member.state, MemberState::Dead | MemberState::Retired) || member.lease_expires_at_ms > threshold { continue; }
        let mut dead = member.clone();
        dead.state = MemberState::Dead;
        if let Cas::Applied(_) = directory.replace_member(dead, member.version).await? { reaped += 1; }
    }
    Ok(reaped)
}

pub async fn run_controller(directory: Arc<dyn Directory>, interval: Duration, grace: Duration, cancellation: CancellationToken) {
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = tokio::time::sleep(interval) => { let _ = reap_members(directory.as_ref(), grace).await; }
        }
    }
}
