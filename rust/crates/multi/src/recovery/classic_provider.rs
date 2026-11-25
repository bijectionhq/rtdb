use std::{fmt::Write as _, num::NonZeroU64};

use rtdb_directory::{
    CheckpointStageIntent, CheckpointTransferFence, ClassicCatalogProvider, ClassicMigration,
    ClassicMigrationSource, DeploymentRecord, Lifecycle, Version,
};
use rtdb_types::{Anomaly, Result, TransactionKey};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{Reply, classic_destination::scratch_name, valid_manifest};
use crate::{archive, keys::{FamilyReleaseRegistration, FamilyResponsePart}};

pub(super) fn observe_ordinary_flight_fence<'a>(
    source: &'a DeploymentRecord, intent: &CheckpointStageIntent,
) -> Result<Option<&'a CheckpointTransferFence>> {
    let migration = intent.classic_migration.ok_or_else(mismatch)?;
    if source.id != intent.source.to_string()
        || source.org != intent.org
        || source.checkpoint_stage.is_some()
        || source.incarnation != migration.source.incarnation
        || source.log_epoch != migration.source.log_epoch
    {
        return Err(mismatch());
    }
    let Some(fence) = source.checkpoint_transfer_fence.as_deref() else {
        if source.epoch != migration.source.writer_epoch || source.holder.is_none() {
            return Err(mismatch());
        }
        return Ok(None);
    };
    let stage = &fence.destination;
    if stage.intent != *intent
        || !stage.transfer_only
        || !stage.staged_manifest_sha256.as_deref().is_some_and(valid_manifest)
        || stage.source_incarnation != source.incarnation
        || stage.source_log_epoch != source.log_epoch
        || source.checkpoint != Some(stage.checkpoint)
        || fence.drained_source_through.is_none_or(|through| through < stage.checkpoint.source)
        || fence.source_writer_epoch != migration.source.writer_epoch
        || fence.source_writer_epoch.checked_add(1) != Some(fence.fenced_writer_epoch)
        || source.epoch != fence.fenced_writer_epoch
        || source.holder.is_some()
        || source.lifecycle != Lifecycle::Restoring
    {
        return Err(mismatch());
    }
    Ok(Some(fence))
}

pub(super) fn ordinary_flight_fence_reply(
    scratch: &DeploymentRecord, fence: &CheckpointTransferFence,
) -> Result<Reply> {
    let stage = &fence.destination;
    let intent = &stage.intent;
    let migration = intent.classic_migration.ok_or_else(mismatch)?;
    if scratch.id != intent.destination.to_string()
        || scratch.lineage != scratch.id
        || scratch.incarnation != 1
        || scratch.name != intent.name
        || scratch.org != intent.org
        || scratch.lifecycle != Lifecycle::Restoring
        || scratch.holder.is_some()
        || scratch.parent.is_some()
        || scratch.checkpoint_stage.as_deref() != Some(stage)
        || scratch.checkpoint_transfer_fence.as_deref() != Some(fence)
    {
        return Err(mismatch());
    }
    let fence_sha256 = archive::sha256(&archive::encode(&(
        "rtdb.multi/classic-ordinary-flight-fence/v1", fence,
    ))?);
    Ok(Reply::ClassicMigrationOrdinaryFlightFence {
        authority: intent.source, capture: intent.capture, destination: migration.destination,
        stage_operation: intent.operation, source: migration.source,
        fenced_writer_epoch: fence.fenced_writer_epoch, fence_sha256,
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FlightFenceSelection {
    destination: Uuid,
    stage_operation: Uuid,
    source_manifest_sha256: String,
    source: ClassicMigrationSource,
}

impl FlightFenceSelection {
    pub(super) fn intent(self, authority: Uuid, capture: Uuid, org: &str) -> Result<CheckpointStageIntent> {
        if self.destination.is_nil()
            || self.stage_operation.is_nil()
            || self.stage_operation == authority
            || self.source.incarnation == 0
            || self.source.log_epoch < 0
            || self.source.writer_epoch == 0
            || !valid_manifest(&self.source_manifest_sha256)
        {
            return Err(mismatch());
        }
        Ok(CheckpointStageIntent {
            operation: self.stage_operation, source: authority, capture,
            source_manifest_sha256: self.source_manifest_sha256, destination: self.stage_operation,
            name: scratch_name(self.stage_operation), org: org.to_owned(),
            classic_migration: Some(ClassicMigration { source: self.source, destination: self.destination }),
        })
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleaseSelection {
    pub(crate) locator: String,
    pub(crate) address: Vec<u8>,
}

#[derive(Deserialize)]
#[serde(tag = "population", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Population {
    LocalCatalog,
    HostedCatalog,
    FamilyRelease { registration: ReleaseSelection },
    OrdinaryFreshFlight {},
}

impl Population {
    pub(super) fn catalog_provider(&self) -> Option<ClassicCatalogProvider> {
        match self {
            Self::LocalCatalog => Some(ClassicCatalogProvider::Local),
            Self::HostedCatalog => Some(ClassicCatalogProvider::HostedS3),
            Self::FamilyRelease { .. } | Self::OrdinaryFreshFlight {} => None,
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "population", rename_all = "snake_case")]
pub(super) enum Terminal {
    LocalCatalog(CatalogDrain),
    HostedCatalog(CatalogDrain),
    FamilyRelease(ReleaseDrain),
}

#[derive(Serialize)]
pub(super) struct ApplicationClose {
    pub(super) classic_cut: TransactionKey,
    pub(super) sql_cutoff: TransactionKey,
    pub(super) epoch: u64,
    pub(super) assignment: [u8; 16],
    pub(super) custody_sha256: [u8; 32],
    pub(super) selection_sha256: [u8; 32],
}

impl ReleaseSelection {
    pub(crate) fn registration(&self) -> Result<FamilyReleaseRegistration> {
        if self.locator.len() != 278 {
            return Err(mismatch());
        }
        let mut fields = self.locator.split('/');
        let origin = hex(fields.next())?;
        let attempt = hex(fields.next())?;
        let part = FamilyResponsePart {
            cell_history: hex(fields.next())?,
            usher: hex(fields.next())?,
            conductor: hex(fields.next())?,
            response: hex(fields.next())?,
            ordinal: NonZeroU64::new(u64::from_be_bytes(hex(fields.next())?))
                .ok_or_else(mismatch)?,
        };
        if fields.next().is_some() {
            return Err(mismatch());
        }
        FamilyReleaseRegistration::from_address(origin, attempt, part, &self.address)
    }
}

fn hex<const N: usize>(field: Option<&str>) -> Result<[u8; N]> {
    let field = field
        .filter(|field| {
            field.len() == N * 2
                && field
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        .ok_or_else(mismatch)?;
    let mut bytes = [0; N];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&field[index * 2..index * 2 + 2], 16).map_err(|_| mismatch())?;
    }
    Ok(bytes)
}

pub(crate) fn locator_digest(locator: &str) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in crate::archive::sha256(locator.as_bytes()) {
        write!(encoded, "{byte:02x}").expect("writing to a string");
    }
    encoded
}

#[derive(Serialize)]
pub(super) struct CatalogDrain {
    pub(super) fenced_at: Version,
    pub(super) drained_at: Version,
    pub(super) residence: String,
    pub(super) object_namespace: Uuid,
    pub(super) readers: Vec<Uuid>,
}

#[derive(Serialize)]
pub(super) struct ReleaseDrain {
    pub(super) locator: String,
    pub(super) address_sha256: [u8; 32],
    pub(super) settled_at: TransactionKey,
    pub(super) settlement_sha256: [u8; 32],
    pub(super) disposition: &'static str,
    pub(super) terminal_sha256: [u8; 32],
}

pub(crate) fn mismatch() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.multi/classic-provider-drain",
        "The selected Classic provider evidence differs from the Original cut",
    )
}
