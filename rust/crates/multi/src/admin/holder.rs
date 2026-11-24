//! Scoped directory evidence for an operator observing a live placement.

use std::collections::BTreeMap;

use rtdb_directory::{DeploymentRecord, Directory, MemberId, effective_holder, now_ms};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HolderObservation {
    pub authority: uuid::Uuid,
    pub incarnation: u64,
    pub log_epoch: i32,
    pub epoch: u64,
    pub holder: MemberId,
    pub engine_version: String,
    pub directory_version: u64,
    pub member_version: u64,
    pub observed_at_ms: i64,
    pub lease_expires_at_ms: i64,
}

/// Observe one exact database incarnation without enumerating the cell.
/// Confirm that placement did not change while opening its member lease.
pub async fn observe_holder(
    directory: &dyn Directory,
    authority: uuid::Uuid,
    incarnation: u64,
) -> Result<HolderObservation> {
    observe_holder_record(directory, authority, incarnation).await.map(|(holder, _)| holder)
}

pub(crate) async fn observe_holder_record(
    directory: &dyn Directory,
    authority: uuid::Uuid,
    incarnation: u64,
) -> Result<(HolderObservation, DeploymentRecord)> {
    let id = authority.to_string();
    let missing = || unavailable("The original database placement is unavailable");
    for _ in 0..4 {
        let before = directory.deployment(&id).await?.ok_or_else(missing)?;
        if before.id != id || before.incarnation != incarnation {
            return Err(unavailable(
                "The directory holds a different database incarnation",
            ));
        }
        let holder = before.holder.as_ref().ok_or_else(missing)?;
        let member = directory.member(holder).await?.ok_or_else(missing)?;
        let after = directory.deployment(&id).await?.ok_or_else(missing)?;
        let observed_at_ms = now_ms();
        if before.id != after.id
            || before.incarnation != after.incarnation
            || before.log_epoch != after.log_epoch
            || before.epoch != after.epoch
            || before.holder != after.holder
            || before.lifecycle != after.lifecycle
        {
            continue;
        }
        let members = BTreeMap::from([(member.id.clone(), member)]);
        if after.epoch == 0 || effective_holder(&after, &members, observed_at_ms) != Some(holder) {
            return Err(missing());
        }
        let member = &members[holder];
        let observed = HolderObservation {
            authority,
            incarnation,
            log_epoch: after.log_epoch,
            epoch: after.epoch,
            holder: holder.clone(),
            engine_version: member.engine_version.clone(),
            directory_version: after.version,
            member_version: member.version,
            observed_at_ms,
            lease_expires_at_ms: member.lease_expires_at_ms,
        };
        return Ok((observed, after));
    }
    Err(unavailable(
        "The writer placement changed while observing its original lease",
    ))
}

fn unavailable(message: &'static str) -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.multi/holder-observation",
        message,
    )
}
