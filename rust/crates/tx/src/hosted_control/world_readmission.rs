use rtdb_types::{EntityId, Iid, OwnedWorld, OwnedWorldAncestor, Result, Timestamp, TransactionKey};
use serde::{Deserialize, Serialize};

use super::{HostedControlWrite, NATIVE_FRONTIER, RESTORE, control_body, replay_invalid};

const PREFIX: &str = "native-world-admitted/";
const ROOT_ADMISSION: &str = "bj_fact_root_admission";
const AUTHORED_HOLDERS: &str = "native_authored_root_versions";
const ORIGINAL_AUTHORED_HOLDERS: &str = "original_native_authored_root_versions";

#[derive(Deserialize, Serialize)]
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
    pub(crate) original_parent: TransactionKey,
    pub(crate) original_until: Timestamp,
    pub(crate) ancestor: Option<RelocatedAncestor>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RelocatedAncestor {
    pub(crate) original: TransactionKey,
    pub(crate) selected: TransactionKey,
    pub(crate) retained: OwnedWorldAncestor,
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
        || admission.original_parent.tx_id.0 < 0
        || admission.original_parent.system_time.0 <= 0
        || admission.original_parent.system_time > admission.source_cut
        || admission.source_cut >= admission.original_until
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
    pub(crate) fn retained_ancestor(&self, world: &OwnedWorld) -> Result<Option<&OwnedWorldAncestor>> {
        match (&world.ancestor, &self.ancestor) {
            (None, None) => Ok(None),
            (Some(old), Some(relocated))
                if relocated.original == old.basis
                    && relocated.selected.system_time == old.system
                    && relocated.selected.tx_id.0 >= 0
                    && (relocated.selected == old.basis
                        || (relocated.selected.tx_id < old.basis.tx_id && old.system < old.basis.system_time))
                    && old.basis.system_time <= self.source_cut
                    && (old.system == old.basis.system_time)
                        == (relocated.retained.system == relocated.retained.basis.system_time)
                    && relocated.retained.system > self.basis.system_time
                    && relocated.retained.system <= relocated.retained.basis.system_time
                    && relocated.retained.world == old.world
                    && relocated.retained.pin == old.pin
                    && relocated.retained.logical == old.logical
                    && relocated.retained.source_root == old.source_root
                    && relocated.retained.basis.tx_id > self.basis.tx_id
                    && relocated.retained.basis.system_time > self.basis.system_time
                    && relocated.retained.basis.system_time < self.until =>
            {
                Ok(Some(&relocated.retained))
            }
            _ => Err(replay_invalid()),
        }
    }

    pub(crate) fn require_relocation(
        &self,
        prior: &OwnedWorld,
        next: &OwnedWorld,
        logical: Timestamp,
    ) -> Result<()> {
        if self.binding == [0; 32]
            || prior.id.0 != self.world
            || next.id != prior.id
            || prior.pin.0 != self.pin
            || next.pin != prior.pin
            || prior.dropped
            || next.dropped
            || next.mode != prior.mode
            || next.schemas != prior.schemas
            || next.fork_lineage != prior.fork_lineage
            || next.parent_publication != Some(prior.root_clock())
            || self.retained_ancestor(prior)? != next.ancestor.as_ref()
            || next.ancestor.as_ref().is_some_and(|ancestor| ancestor.basis.system_time >= logical)
            || prior.basis != self.original_parent
            || prior.until != self.original_until
            || prior.basis.system_time > self.source_cut
            || self.source_cut >= prior.until
            || next.basis != self.basis
            || next.until != self.until
            || next.capture.0 != self.capture
            || next.suffix.map(|suffix| suffix.0) != self.suffix
            || next.manifest != self.manifest
            || self.source_cut >= next.basis.system_time
            || next.basis.system_time >= logical
            || logical >= next.until
        {
            return Err(replay_invalid());
        }
        Ok(())
    }

    pub(crate) fn require_application<'a>(
        &self,
        original: &OwnedWorld,
        controls: impl Iterator<Item = &'a HostedControlWrite> + Clone,
    ) -> Result<()> {
        let writes = controls
            .clone()
            .filter(|control| control.name() == ROOT_ADMISSION && !control.is_check());
        if writes.clone().any(|control| control.id() == &EntityId::String(AUTHORED_HOLDERS.into()))
            && (matches!(original.mode, rtdb_types::WorldClockMode::Held { .. })
                || !controls.clone().any(|control| {
                    control.name() == ROOT_ADMISSION && control.is_check() && control.expected().is_some()
                        && matches!(control.id(), EntityId::String(id) if id == ORIGINAL_AUTHORED_HOLDERS)
                }))
        {
            return Err(replay_invalid());
        }
        let writes = writes.filter(
            |control| !matches!(control.id(), EntityId::String(id) if id == AUTHORED_HOLDERS),
        );
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


#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::{TxId, WorldClockMode, WorldSchemas};

    fn position(tx_id: i64, system_time: i64) -> TransactionKey {
        TransactionKey { tx_id: TxId(tx_id), system_time: Timestamp(system_time) }
    }

    #[test]
    fn retained_ancestor_relocation_preserves_the_original_overlay_identity() -> Result<()> {
        let original = OwnedWorldAncestor {
            world: [2; 16],
            pin: [3; 16],
            capture: [4; 16],
            suffix: [5; 16],
            manifest: "a".repeat(43),
            basis: position(2, 20),
            system: Timestamp(20),
            source_root: position(1, 10),
            logical: Timestamp(17),
        };
        let world = OwnedWorld {
            id: Iid([1; 16]),
            pin: Iid([1; 16]),
            capture: Iid([6; 16]),
            suffix: None,
            manifest: "b".repeat(43),
            basis: position(1, 10),
            parent_publication: Some(Timestamp(8)),
            mode: WorldClockMode::Retained,
            until: Timestamp(100),
            schemas: WorldSchemas::from_bytes(b"{}")?,
            ancestor: Some(original.clone()),
            fork_lineage: vec![rtdb_types::OwnedWorldForkLink {
                world: original.world, pin: original.pin, registered: Timestamp(16),
                system: original.system, logical: original.logical,
            }],
            dropped: false,
        };
        let mut retained = original.clone();
        retained.basis = position(102, 120);
        retained.system = Timestamp(120);
        let mut admission = WorldReadmission {
            binding: [9; 32],
            world: world.id.0,
            pin: world.pin.0,
            capture: [7; 16],
            suffix: None,
            manifest: "c".repeat(43),
            basis: position(101, 110),
            until: Timestamp(200),
            source_cut: Timestamp(90),
            original_parent: world.basis,
            original_until: world.until,
            ancestor: Some(RelocatedAncestor { original: original.basis, selected: original.basis, retained: retained.clone() }),
        };
        assert_eq!(admission.retained_ancestor(&world)?, Some(&retained));
        let moved = admission.ancestor.as_mut().expect("fixture has a relocated ancestor");
        moved.selected = position(1, original.system.0);
        assert!(admission.retained_ancestor(&world).is_err());
        let moved = admission.ancestor.as_mut().expect("fixture has a relocated ancestor");
        moved.selected = original.basis;
        moved.retained.source_root = position(101, 110);
        assert!(admission.retained_ancestor(&world).is_err());
        admission.ancestor = None;
        assert!(admission.retained_ancestor(&world).is_err());
        Ok(())
    }
}
