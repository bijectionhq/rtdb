use rtdb_types::{EntityId, Iid, OwnedWorld, Result, Timestamp, TransactionKey};
use serde::Deserialize;

use super::{HostedControlWrite, NATIVE_FRONTIER, RESTORE, control_body, replay_invalid};

const PREFIX: &str = "native-world-admitted/";
const ROOT_ADMISSION: &str = "bj_fact_root_admission";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorldReadmission {
    pub(crate) binding: [u8; 32],
    pub(crate) world: [u8; 16],
    pub(crate) pin: [u8; 16],
    pub(crate) capture: [u8; 16],
    pub(crate) suffix: Option<[u8; 16]>,
    pub(crate) manifest: String,
    pub(crate) basis: TransactionKey,
    pub(crate) until: Timestamp,
    pub(crate) source_cut: Timestamp,
}

pub(crate) fn world_readmission_control<'a>(
    controls: impl Iterator<Item = &'a HostedControlWrite> + Clone,
) -> Result<Option<WorldReadmission>> {
    let mut candidates = controls.clone().filter(|control| is_readmission(control));
    let Some(marker) = candidates.next() else {
        return Ok(None);
    };
    let invalid = replay_invalid;
    let body = control_body(marker, 4096).ok_or_else(invalid)?;
    let admission: WorldReadmission = serde_json::from_slice(body).map_err(|_| invalid())?;
    if candidates.next().is_some()
        || marker.expected().is_some()
        || marker.id() != &EntityId::String(format!("{PREFIX}{}", Iid(admission.world)))
        || admission.binding == [0; 32]
        || admission.world == [0; 16]
        || admission.pin == [0; 16]
        || admission.capture == [0; 16]
        || admission.suffix == Some([0; 16])
        || admission.basis.tx_id.0 < 0
        || admission.source_cut.0 <= 0
        || admission.basis.system_time <= admission.source_cut
        || admission.until <= admission.basis.system_time
        || admission.until == Timestamp::MAX
        || admission.manifest.len() != 43
        || !admission
            .manifest
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(invalid());
    }
    for (relation, id, exists) in [
        ("bj_installation", "authority", true),
        (RESTORE, "restore", true),
        (RESTORE, "archive", true),
        (RESTORE, NATIVE_FRONTIER, true),
        (RESTORE, "replayed", false),
        (RESTORE, "released", false),
    ] {
        if !controls.clone().any(|control| {
            control.name() == relation
                && control.is_check()
                && control.expected().is_some() == exists
                && matches!(control.id(), EntityId::String(key) if key == id)
        }) {
            return Err(invalid());
        }
    }
    if controls.clone().any(|control| {
        !control.is_check() && control.name() != ROOT_ADMISSION && !is_readmission(control)
    }) {
        return Err(invalid());
    }
    Ok(Some(admission))
}

fn is_readmission(control: &HostedControlWrite) -> bool {
    control.name() == RESTORE
        && !control.is_check()
        && matches!(control.id(), EntityId::String(id) if id.starts_with(PREFIX))
}

impl WorldReadmission {
    pub(crate) fn require_application<'a>(
        &self,
        original: &OwnedWorld,
        controls: impl Iterator<Item = &'a HostedControlWrite> + Clone,
    ) -> Result<()> {
        let writes = controls
            .clone()
            .filter(|control| control.name() == ROOT_ADMISSION && !control.is_check());
        if original.mode != rtdb_types::WorldClockMode::Proposal {
            return if writes.count() == 0 {
                Ok(())
            } else {
                Err(replay_invalid())
            };
        }
        use sha2::{Digest, Sha256};
        let world = format!("{:x}", Sha256::digest(self.world));
        let owner = format!("root_proposal:{world}");
        let head = format!("root_proposal_head:{world}");
        let mut quota = false;
        let mut owner_seen = false;
        let mut head_seen = false;
        for control in writes {
            let EntityId::String(id) = control.id() else {
                return Err(replay_invalid());
            };
            match id {
                id if id == &owner => owner_seen = true,
                id if id == &head => head_seen = true,
                id if id
                    .strip_prefix("root_proposal_quota:")
                    .is_some_and(|digest| {
                        digest.len() == 64
                            && digest
                                .bytes()
                                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
                    })
                    && !quota =>
                {
                    quota = true
                }
                _ => return Err(replay_invalid()),
            }
            let original_id = format!("original_{id}");
            if control.expected().is_some()
                || !controls.clone().any(|control| {
                    control.name() == ROOT_ADMISSION
                        && control.is_check()
                        && control.expected().is_some()
                        && matches!(control.id(), EntityId::String(id) if id == &original_id)
                })
            {
                return Err(replay_invalid());
            }
        }
        if !owner_seen || !head_seen || !quota {
            return Err(replay_invalid());
        }
        Ok(())
    }
}
