use std::collections::{BTreeMap, BTreeSet};

use ring::{aead, digest, rand::{SecureRandom, SystemRandom}};
use serde::{Deserialize, Serialize};

use crate::{Anomaly, Iid, Result, TableRef, Timestamp};

pub type Branch = String;
pub const MAIN_BRANCH: &str = "main";
pub const MAX_BRANCH_BYTES: usize = 255;
pub const WRITTEN_BASIS_COLUMN: &str = "_rtdb_written_basis";
pub const QUERY_WRITTEN_BASIS: &str = "query.written-basis";

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelScope {
    pub read: BTreeSet<Vec<u8>>,
    pub write: BTreeSet<Vec<u8>>,
}

impl LabelScope {
    pub fn main_read_incarnation(&self, database_incarnation: [u8; 16]) -> [u8; 16] {
        let mut hash = digest::Context::new(&digest::SHA256);
        hash.update(b"rtdb.v1/main-merged-visibility");
        hash.update(&database_incarnation);
        for label in &self.read {
            hash.update(&(label.len() as u64).to_be_bytes());
            hash.update(label);
        }
        hash.finish().as_ref()[..16].try_into().expect("SHA-256 contains 16 bytes")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Basis {
    incarnation: [u8; 16],
    position: u64,
}

impl Basis {
    pub const WIDTH: usize = 24;

    pub fn new(incarnation: [u8; 16], position: u64) -> Self {
        Self { incarnation, position }
    }

    pub fn incarnation(self) -> [u8; 16] { self.incarnation }
    pub fn position(self) -> u64 { self.position }

    pub fn to_bytes(self) -> [u8; Self::WIDTH] {
        let mut bytes = [0; Self::WIDTH];
        bytes[..16].copy_from_slice(&self.incarnation);
        bytes[16..].copy_from_slice(&self.position.to_be_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let (incarnation, rest) = bytes.split_first_chunk::<16>().ok_or_else(invalid_basis)?;
        let position = rest.try_into().map_err(|_| invalid_basis())?;
        Ok(Self::new(*incarnation, u64::from_be_bytes(position)))
    }
}

fn invalid_basis() -> Anomaly {
    Anomaly::incorrect("rtdb/invalid-basis", "Invalid basis encoding")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedCheckpointIdentity {
    pub basis: Basis,
    pub pin: [u8; 16],
    pub manifest_sha256: [u8; 32],
    pub until: Option<Timestamp>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedCheckpointTarget {
    pub incarnation: [u8; 16],
    pub request: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedCheckpointReservation {
    pub source: PinnedCheckpointIdentity,
    pub target: ManagedCheckpointTarget,
    pub reserved_at: Basis,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum At {
    #[default]
    Latest,
    AtLeast(Basis),
    Exactly(Basis),
    Linearizable,
    AsOf(Timestamp),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Layer {
    #[default]
    Merged,
    Local,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claim(Vec<u8>);

impl Claim {
    pub fn from_bytes(bytes: Vec<u8>) -> Self { Self(bytes) }
    pub fn as_bytes(&self) -> &[u8] { &self.0 }
    pub fn into_bytes(self) -> Vec<u8> { self.0 }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimScope {
    pub branch: Branch,
    pub basis: Basis,
    pub layer: Layer,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ClaimRegion {
    Table(TableRef),
    Row { table: TableRef, iid: Iid },
    IidRange { table: TableRef, lower: Iid, upper: Iid },
    IndexRange { table: TableRef, index: String, lower: Option<Vec<u8>>, upper: Option<Vec<u8>> },
}

impl ClaimRegion {
    pub fn table(&self) -> &TableRef {
        match self {
            Self::Table(table) | Self::Row { table, .. } | Self::IidRange { table, .. } | Self::IndexRange { table, .. } => table,
        }
    }
}

const MAX_CLAIM_TABLES: usize = 32;
pub const MAX_REGIONS_PER_TABLE: usize = 16;
const MAX_REGION_BYTES: usize = 1024;
const MAX_FOOTPRINT_BYTES: usize = 16 * 1024;
pub const MAX_CLAIM_PAYLOAD_BYTES: usize = MAX_FOOTPRINT_BYTES + 2048;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimFootprint(Option<BTreeSet<ClaimRegion>>);

impl ClaimFootprint {
    pub fn from_regions(regions: impl IntoIterator<Item = ClaimRegion>) -> Self {
        let mut tables = BTreeMap::<TableRef, Option<BTreeSet<ClaimRegion>>>::new();
        for region in regions {
            let table = region.table();
            if table.schema.len().saturating_add(table.table.len()) > MAX_REGION_BYTES
                || (tables.len() == MAX_CLAIM_TABLES && !tables.contains_key(table))
            {
                return Self::default();
            }
            let entry = tables.entry(table.clone()).or_insert_with(|| Some(BTreeSet::new()));
            let Some(parts) = entry.as_mut() else { continue; };
            if matches!(region, ClaimRegion::Table(_)) || oversized_or_invalid(&region) {
                *entry = None;
                continue;
            }
            parts.insert(region);
            if parts.len() > MAX_REGIONS_PER_TABLE {
                *entry = None;
            }
        }
        let coarse = tables.keys().cloned().map(ClaimRegion::Table).collect();
        let mut precise = BTreeSet::new();
        for (table, parts) in tables {
            match parts {
                Some(parts) => precise.extend(parts),
                None => { precise.insert(ClaimRegion::Table(table)); }
            }
        }
        if fits_footprint(&precise) {
            return Self(Some(precise));
        }
        if fits_footprint(&coarse) { Self(Some(coarse)) } else { Self::default() }
    }

    pub fn regions(&self) -> Option<&BTreeSet<ClaimRegion>> { self.0.as_ref() }
}

fn oversized_or_invalid(region: &ClaimRegion) -> bool {
    if matches!(region, ClaimRegion::IidRange { lower, upper, .. } if lower > upper) { return true; }
    if let ClaimRegion::IndexRange { index, lower, upper, .. } = region
        && (index.len().saturating_add(lower.as_ref().map_or(0, Vec::len))
            .saturating_add(upper.as_ref().map_or(0, Vec::len)) > MAX_REGION_BYTES
            || lower.as_ref().zip(upper.as_ref()).is_some_and(|(lower, upper)| lower > upper))
    {
        return true;
    }
    serde_json::to_vec(region).is_ok_and(|bytes| bytes.len() > MAX_REGION_BYTES)
}

fn fits_footprint(regions: &BTreeSet<ClaimRegion>) -> bool {
    serde_json::to_vec(regions).is_ok_and(|bytes| bytes.len() <= MAX_FOOTPRINT_BYTES)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimEvidence {
    pub scope: ClaimScope,
    pub footprint: ClaimFootprint,
}

pub trait ClaimVerifier: Send + Sync {
    fn verify(&self, claim: &Claim) -> Result<ClaimEvidence>;
}

const CLAIM_DOMAIN: &[u8] = b"rtdb.v1/branch-claim/1";
const NONCE_BYTES: usize = 12;
const CLAIM_HEADER_BYTES: usize = Basis::WIDTH + 1;
const MAX_CLAIM_BYTES: usize = NONCE_BYTES + CLAIM_HEADER_BYTES + MAX_BRANCH_BYTES + 16;

/// Seals a conservative whole-branch footprint using the incarnation's stable claim key.
pub struct ClaimSealer {
    key: aead::LessSafeKey,
    incarnation: [u8; 16],
}

impl ClaimVerifier for ClaimSealer {
    fn verify(&self, claim: &Claim) -> Result<ClaimEvidence> {
        Ok(ClaimEvidence { scope: self.open(claim)?, footprint: ClaimFootprint::default() })
    }
}

impl ClaimSealer {
    pub fn scoped_verifier<'a>(&'a self, scope: &'a LabelScope) -> impl ClaimVerifier + 'a {
        struct Scoped<'a>(&'a ClaimSealer, &'a LabelScope);
        impl ClaimVerifier for Scoped<'_> {
            fn verify(&self, claim: &Claim) -> Result<ClaimEvidence> {
                Ok(ClaimEvidence { scope: self.0.open_scoped(claim, self.1)?, footprint: Default::default() })
            }
        }
        Scoped(self, scope)
    }
    pub fn new(incarnation: [u8; 16], key: [u8; 32]) -> Self {
        let key = aead::UnboundKey::new(&aead::AES_256_GCM, &key)
            .expect("AES-256-GCM keys are 32 bytes");
        Self { key: aead::LessSafeKey::new(key), incarnation }
    }

    pub fn seal(&self, scope: &ClaimScope) -> Result<Claim> {
        self.seal_for(scope, CLAIM_DOMAIN)
    }

    pub fn seal_scoped(&self, scope: &ClaimScope, labels: &LabelScope) -> Result<Claim> {
        self.seal_for(scope, &labels.main_read_incarnation(self.incarnation))
    }

    fn seal_for(&self, scope: &ClaimScope, domain: &[u8]) -> Result<Claim> {
        if scope.basis.incarnation() != self.incarnation || !valid_branch_name(&scope.branch) {
            return Err(invalid_claim());
        }
        let mut nonce = [0; NONCE_BYTES];
        SystemRandom::new().fill(&mut nonce)
            .map_err(|_| Anomaly::fault("rtdb/claim-randomness", "Could not generate a claim nonce"))?;
        let mut payload = scope.basis.to_bytes().to_vec();
        payload.push(match scope.layer { Layer::Merged => 0, Layer::Local => 1 });
        payload.extend_from_slice(scope.branch.as_bytes());
        self.key.seal_in_place_append_tag(
            aead::Nonce::assume_unique_for_key(nonce), aead::Aad::from(domain), &mut payload,
        ).map_err(|_| Anomaly::fault("rtdb/claim-seal", "Could not seal query claim"))?;
        let mut bytes = nonce.to_vec();
        bytes.extend(payload);
        Ok(Claim(bytes))
    }

    pub fn open(&self, claim: &Claim) -> Result<ClaimScope> {
        self.open_for(claim, CLAIM_DOMAIN)
    }

    pub fn open_scoped(&self, claim: &Claim, labels: &LabelScope) -> Result<ClaimScope> {
        self.open_for(claim, &labels.main_read_incarnation(self.incarnation))
    }

    fn open_for(&self, claim: &Claim, domain: &[u8]) -> Result<ClaimScope> {
        if claim.0.len() > MAX_CLAIM_BYTES {
            return Err(invalid_claim());
        }
        let (nonce, payload) = claim.0.split_first_chunk::<NONCE_BYTES>().ok_or_else(invalid_claim)?;
        let mut payload = payload.to_vec();
        let plaintext = self.key.open_in_place(
            aead::Nonce::assume_unique_for_key(*nonce), aead::Aad::from(domain), &mut payload,
        ).map_err(|_| invalid_claim())?;
        let (basis, rest) = plaintext.split_first_chunk::<{ Basis::WIDTH }>().ok_or_else(invalid_claim)?;
        let basis = Basis::from_bytes(basis)?;
        let (layer, branch) = rest.split_first().ok_or_else(invalid_claim)?;
        let branch = std::str::from_utf8(branch).map_err(|_| invalid_claim())?;
        if basis.incarnation() != self.incarnation || !valid_branch_name(branch) {
            return Err(invalid_claim());
        }
        let layer = match layer {
            0 => Layer::Merged,
            1 => Layer::Local,
            _ => return Err(invalid_claim()),
        };
        Ok(ClaimScope { branch: branch.into(), basis, layer })
    }
}

pub fn valid_branch_name(branch: &str) -> bool {
    !branch.is_empty() && branch.len() <= MAX_BRANCH_BYTES && !branch.chars().any(char::is_control)
}

fn invalid_claim() -> Anomaly {
    Anomaly::incorrect("rtdb/invalid-claim", "Invalid query claim")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rtdb_v1_claim_round_trips_and_rejects_substitution() {
        let scope = ClaimScope { branch: "preview".into(), basis: Basis::new([1; 16], 256), layer: Layer::Local };
        assert_eq!(Basis::from_bytes(&scope.basis.to_bytes()).unwrap(), scope.basis);
        assert!(Basis::new([1; 16], 255).to_bytes() < scope.basis.to_bytes());
        let sealer = ClaimSealer::new([1; 16], [2; 32]);
        let claim = sealer.seal(&scope).unwrap();
        assert_eq!(sealer.open(&claim).unwrap(), scope);
        assert_ne!(sealer.seal(&scope).unwrap(), claim);
        assert!(ClaimSealer::new([2; 16], [2; 32]).open(&claim).is_err());
        assert!(ClaimSealer::new([1; 16], [3; 32]).open(&claim).is_err());
        let mut corrupt = claim.into_bytes();
        corrupt[NONCE_BYTES] ^= 1;
        assert!(sealer.open(&Claim::from_bytes(corrupt)).is_err());
    }
}
