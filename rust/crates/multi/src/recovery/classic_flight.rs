use std::{fs::File, io::Read, path::Path};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::signature::{ED25519, UnparsedPublicKey};
use rtdb_directory::{CheckpointStageIntent, DeploymentRecord, Directory, classic_flight::{self, Commission, Generation, Role}};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{Reply, classic_provider::{flight_fence_digest, observe_ordinary_flight_fence}};
use crate::archive;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Authority {
    coverage_key: [u8; 32],
    disposal_key: [u8; 32],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Coverage {
    coverage_sha256: [u8; 32],
    begin_sha256: [u8; 32],
    original_catalog_sha256: [u8; 32],
    retained_path_sha256: [u8; 32],
    prior_generations: Vec<Generation>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Disposition {
    fence_sha256: [u8; 32],
    generation: Uuid,
    role: Role,
    disposal_sha256: [u8; 32],
}

impl Authority {
    pub(super) fn from_file(path: &Path) -> Result<Self> {
        const MAX_KEY_FILE: u64 = 1024;
        let mut bytes = Vec::new();
        File::open(path).and_then(|file| file.take(MAX_KEY_FILE + 1).read_to_end(&mut bytes))
            .map_err(|_| configuration())?;
        if bytes.len() > MAX_KEY_FILE as usize { return Err(configuration()); }
        let authority: Self = serde_json::from_slice(&bytes).map_err(|_| configuration())?;
        if authority.coverage_key == [0; 32] || authority.disposal_key == [0; 32]
            || authority.coverage_key == authority.disposal_key
        {
            return Err(configuration());
        }
        Ok(authority)
    }

    pub(super) async fn commission(
        &self, directory: &dyn Directory, original: &DeploymentRecord, intent: CheckpointStageIntent,
        coverage: Coverage, signature: &str,
    ) -> Result<Reply> {
        let Coverage { coverage_sha256, begin_sha256, original_catalog_sha256, retained_path_sha256, prior_generations } = coverage;
        if prior_generations.len() > classic_flight::MAX_GENERATIONS
            || prior_generations.windows(2).any(|pair| pair[0].generation >= pair[1].generation)
            || !prior_generations.iter().any(|owner| owner.role == Role::Gateway)
            || !prior_generations.iter().any(|owner| owner.role == Role::Receiver)
        {
            return Err(refused());
        }
        let commission = Commission {
            intent, coverage_sha256, begin_sha256, original_catalog_sha256, retained_path_sha256, prior_generations,
        };
        verify(&self.coverage_key, signature, &(
            "rtdb.multi/classic-ordinary-flight-commission/v1", &original.name, &commission,
        ))?;
        classic_flight::install(directory, &commission).await?;
        Ok(Reply::ClassicMigrationOrdinaryFlightCommissioned { coverage_sha256 })
    }

    pub(super) async fn join(
        &self, directory: &dyn Directory, original: &DeploymentRecord, capture: Uuid,
        disposition: Disposition, signature: &str,
    ) -> Result<Reply> {
        let Some(commission) = classic_flight::commission(original) else { return Ok(Reply::Deferred); };
        if capture != commission.intent.capture { return Err(refused()); }
        let Some(fence) = observe_ordinary_flight_fence(original, &commission.intent)? else { return Ok(Reply::Deferred); };
        if flight_fence_digest(fence)? != disposition.fence_sha256 { return Err(refused()); }
        verify(&self.disposal_key, signature, &(
            "rtdb.multi/classic-ordinary-flight-generation-join/v1", &original.name, commission, &disposition,
        ))?;
        let Some(scratch) = directory.deployment(&commission.intent.destination.to_string()).await? else {
            return Ok(Reply::Deferred);
        };
        super::classic_provider::ordinary_flight_fence_reply(&scratch, fence)?;
        classic_flight::join(directory, commission, disposition.generation, disposition.role, disposition.disposal_sha256).await?;
        Ok(Reply::ClassicMigrationOrdinaryFlightOwnerJoined {
            generation: disposition.generation, fence_sha256: disposition.fence_sha256,
        })
    }
}

fn verify(key: &[u8; 32], signature: &str, body: &impl Serialize) -> Result<()> {
    if signature.len() != 86 { return Err(refused()); }
    let signature = URL_SAFE_NO_PAD.decode(signature).map_err(|_| refused())?;
    UnparsedPublicKey::new(&ED25519, key).verify(&archive::encode(body)?, &signature).map_err(|_| refused())
}

fn configuration() -> Anomaly {
    Anomaly::incorrect("rtdb.multi/classic-flight-authority", "The installed Classic Flight verification keys are invalid")
}

fn refused() -> Anomaly {
    Anomaly::new(AnomalyCategory::Forbidden, "rtdb.multi/classic-flight-authority",
        "The installed authority did not approve the exact Classic Flight operation")
}
