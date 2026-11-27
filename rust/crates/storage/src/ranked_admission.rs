use rtdb_format::journal::RankedCatalog;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
