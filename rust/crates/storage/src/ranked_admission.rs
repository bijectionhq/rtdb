use bytes::Bytes;
use rtdb_format::journal::{ObjectId, RankedCatalog};
use rtdb_types::PhysicalObjectVersion;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RankedAttempt {
    pub epoch: u64,
    pub position: u64,
    pub system_time: i64,
    pub key_generation: u64,
}

impl From<RankedCatalog> for RankedAttempt {
    fn from(catalogue: RankedCatalog) -> Self {
        Self {
            epoch: catalogue.manifest.epoch,
            position: catalogue.position,
            system_time: catalogue.system_time,
            key_generation: catalogue.key_generation,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RankedAdmissionScope {
    pub incarnation: Uuid,
    pub activation_sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClosedRankedAdmission {
    pub scope: RankedAdmissionScope,
    pub admitted: u64,
    pub inventory_sha256: [u8; 32],
}

#[derive(Debug)]
pub struct RankedAdmissionPage {
    pub catalogues: Vec<RankedCatalog>,
    pub next_after: Option<u64>,
}

#[async_trait::async_trait]
pub trait RankedAdmissionWriter: Send + Sync {
    fn scope(&self) -> RankedAdmissionScope;

    async fn prepared(&self, attempt: RankedAttempt) -> rtdb_types::Result<Option<RankedCatalog>>;

    /// Stages ciphertext at the scope's original provider and returns its exact version.
    async fn stage(&self, object: ObjectId, ciphertext: Bytes) -> rtdb_types::Result<PhysicalObjectVersion>;

    async fn admit(&self, catalogue: RankedCatalog) -> rtdb_types::Result<()>;
}
