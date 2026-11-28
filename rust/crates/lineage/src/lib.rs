//! Offline settlement of append authorizations against the published lineage.

use journal_contract::{Epoch, Lsn, StreamId};
use rtdb_stream_journal::authenticated::{CatalogRoot, Hash, Head, Inclusion, Leaf, Timing};
use rtdb_types::{Anomaly, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// The activation protocol whose records publish lineage.
pub const LINEAGE_PROTOCOL: u8 = 12;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub incarnation: Uuid,
    pub revision: u64,
}

pub fn validate_deployment_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 255 || name.chars().any(char::is_control) {
        return Err(Anomaly::incorrect("rtdb.multi/configuration", "Invalid deployment name"));
    }
    Ok(())
}

/// The control stream that orders one database's activation records.
pub fn activation_stream_id(authority: Uuid, name: &str) -> Result<StreamId> {
    validate_deployment_name(name)?;
    let mut hash = Sha256::new();
    hash.update(b"rtdb.activation.v1");
    hash.update(authority.as_bytes());
    hash.update(name.as_bytes());
    let digest: [u8; 32] = hash.finalize().into();
    Ok(StreamId(u128::from_be_bytes(digest[..16].try_into().unwrap())))
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LineageSegment {
    pub authority: Uuid,
    pub database: String,
    pub predecessor: Binding,
    pub successor: Binding,
    pub source_catalog: Uuid,
    pub source_root: CatalogRoot,
    pub source_stream: StreamId,
    pub source_head: Head,
    pub end: Lsn,
    pub previous: Option<Hash>,
}

impl LineageSegment {
    pub fn valid(&self) -> bool {
        !self.authority.is_nil()
            && !self.predecessor.incarnation.is_nil()
            && !self.successor.incarnation.is_nil()
            && self.predecessor.incarnation != self.successor.incarnation
            && self.predecessor.revision > 0
            && self.predecessor.revision.checked_add(1) == Some(self.successor.revision)
            && self.source_stream == StreamId(self.predecessor.incarnation.as_u128())
            && self.source_root.0 != [0; 32]
            && self.source_head.valid()
            && self.source_head.catalog == self.source_catalog
            && self.source_head.stream == self.source_stream
            && self.source_head.size == self.end.0
            && self.previous != Some([0; 32])
            && activation_stream_id(self.authority, &self.database).is_ok()
    }

    pub fn digest(&self) -> Hash {
        let mut digest = Sha256::new();
        digest.update(b"rtdb/activation/lineage-segment/v1\0");
        digest.update(self.authority.as_bytes());
        digest.update((self.database.len() as u64).to_be_bytes());
        digest.update(self.database.as_bytes());
        for binding in [self.predecessor, self.successor] {
            digest.update(binding.incarnation.as_bytes());
            digest.update(binding.revision.to_be_bytes());
        }
        digest.update(self.source_catalog.as_bytes());
        digest.update(self.source_root.0);
        digest.update(self.source_stream.0.to_be_bytes());
        digest.update(self.source_head.statement());
        digest.update(self.end.0.to_be_bytes());
        match self.previous {
            Some(previous) => {
                digest.update([1]);
                digest.update(previous);
            }
            None => digest.update([0]),
        }
        digest.finalize().into()
    }

    pub fn follows(&self, previous: &Self) -> bool {
        self.authority == previous.authority
            && self.database == previous.database
            && self.predecessor == previous.successor
            && self.previous == Some(previous.digest())
    }
}

/// The serving incarnation's published prefix. Every later cut of this
/// incarnation keeps it, so an append inside it is part of the lineage.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LineageFloor {
    pub binding: Binding,
    pub source_root: CatalogRoot,
    pub source_head: Head,
}

impl LineageFloor {
    pub fn valid(&self) -> bool {
        !self.binding.incarnation.is_nil()
            && self.binding.revision > 0
            && self.source_root.0 != [0; 32]
            && self.source_head.valid()
            && self.source_head.size > 0
            && self.source_head.stream == StreamId(self.binding.incarnation.as_u128())
    }

    pub fn end(&self) -> Lsn {
        Lsn(self.source_head.size)
    }
}

/// Where the published lineage of one source stream ends: closed at a fence
/// or cut that names the successor, or open at the serving incarnation's floor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Published {
    Closed(LineageSegment),
    Open(LineageFloor),
}

/// The fields of an activation record that publish lineage. The control
/// inclusion authenticates the whole record, so a verifier ignores the
/// fields it does not settle against, including those of later releases.
#[derive(Deserialize)]
struct LineageRecord {
    version: u8,
    binding: Binding,
    #[serde(default)]
    publication: Option<LineagePublication>,
    #[serde(default)]
    floor: Option<LineageFloor>,
}

#[derive(Deserialize)]
struct LineagePublication {
    predecessor: Binding,
    #[serde(default)]
    segment: Option<LineageSegment>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationProof {
    pub control_record: Vec<u8>,
    pub control_inclusion: Inclusion,
}

impl PublicationProof {
    pub fn verify(
        &self,
        authority: Uuid,
        database: &str,
        control_root: CatalogRoot,
    ) -> Result<LineageSegment> {
        match self.published(authority, database, control_root, None)? {
            Published::Closed(segment) => Ok(segment),
            Published::Open(_) => Err(invalid("The activation record publishes an open lineage floor")),
        }
    }

    /// The lineage this control record publishes for the `source` stream: its
    /// open floor, or else its closed segment.
    pub fn published(
        &self,
        authority: Uuid,
        database: &str,
        control_root: CatalogRoot,
        source: Option<StreamId>,
    ) -> Result<Published> {
        let state = self.record(authority, database, control_root)?;
        if state.version != LINEAGE_PROTOCOL {
            return Err(invalid("The control record is not a lineage activation record"));
        }
        if let Some(floor) = state.floor.filter(|floor| Some(floor.source_head.stream) == source) {
            if !floor.valid() || floor.binding != state.binding {
                return Err(invalid("The activation record does not publish this lineage floor"));
            }
            return Ok(Published::Open(floor));
        }
        let publication = state
            .publication
            .ok_or_else(|| invalid("The control record has no activation publication"))?;
        let segment = publication
            .segment
            .ok_or_else(|| invalid("The activation publication has no lineage segment"))?;
        if !segment.valid()
            || segment.authority != authority
            || segment.database != database
            || segment.predecessor != publication.predecessor
            || segment.successor != state.binding
        {
            return Err(invalid(
                "The activation publication does not select this lineage segment",
            ));
        }
        Ok(Published::Closed(segment))
    }

    fn record(&self, authority: Uuid, database: &str, control_root: CatalogRoot) -> Result<LineageRecord> {
        let stream = activation_stream_id(authority, database)?;
        let leaf = self
            .control_inclusion
            .verify(control_root)
            .map_err(|_| invalid("The control catalog inclusion did not verify"))?;
        let digest: Hash = Sha256::digest(&self.control_record).into();
        if leaf.stream != stream || leaf.payload_sha256 != digest {
            return Err(invalid(
                "The control leaf does not bind this activation publication",
            ));
        }
        serde_json::from_slice(&self.control_record)
            .map_err(|_| invalid("The control record is not a supported activation record"))
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Authorization {
    pub catalog: Uuid,
    pub stream: StreamId,
    pub writer_epoch: Epoch,
    pub at: Lsn,
    pub payload_sha256: Hash,
    pub timing: Timing,
}

impl Authorization {
    fn valid(self) -> bool {
        !self.catalog.is_nil()
            && self.stream.0 != 0
            && self.writer_epoch.0 != 0
            && self.at.0 != 0
            && self.timing.exclusive_cutoff_micros > 0
            && self.timing.receiver_certificate_sha256 != [0; 32]
    }

    fn identifies(self, leaf: &Leaf) -> bool {
        leaf.stream == self.stream
            && leaf.at == self.at
            && leaf.writer_epoch == self.writer_epoch
            && leaf.payload_sha256 == self.payload_sha256
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Settlement {
    Pending,
    Capture,
    Release,
}

#[derive(Clone, Copy, Debug)]
pub struct SettlementCut {
    pub predecessor: Binding,
    pub successor: Binding,
    pub end: Lsn,
}

impl SettlementCut {
    fn matches(self, segment: &LineageSegment) -> bool {
        self.predecessor == segment.predecessor
            && self.successor == segment.successor
            && self.end == segment.end
    }
}

pub fn verify_settlement(
    authority: Uuid,
    database: &str,
    control_root: CatalogRoot,
    application_root: CatalogRoot,
    authorization: Authorization,
    publication: Option<&PublicationProof>,
    inclusion: Option<&Inclusion>,
) -> Result<Settlement> {
    verify_settlement_at(
        authority, database, control_root, application_root, authorization, publication, inclusion, None,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn verify_settlement_at(
    authority: Uuid,
    database: &str,
    control_root: CatalogRoot,
    application_root: CatalogRoot,
    authorization: Authorization,
    publication: Option<&PublicationProof>,
    inclusion: Option<&Inclusion>,
    selected: Option<SettlementCut>,
) -> Result<Settlement> {
    if !authorization.valid() {
        return Err(invalid("Invalid funded append authorization"));
    }
    if let Some(cut) = selected {
        activation_stream_id(authority, database)?;
        if authority.is_nil()
            || control_root.0 == [0; 32]
            || application_root.0 == [0; 32]
            || control_root == application_root
            || cut.predecessor.incarnation.is_nil()
            || cut.successor.incarnation.is_nil()
            || cut.predecessor.incarnation == cut.successor.incarnation
            || cut.predecessor.revision == 0
            || cut.predecessor.revision.checked_add(1) != Some(cut.successor.revision)
            || authorization.stream != StreamId(cut.predecessor.incarnation.as_u128())
        {
            return Err(invalid("Invalid independently pinned catalogs or settlement binding"));
        }
    }
    let Some(publication) = publication else {
        return Ok(Settlement::Pending);
    };
    let control_catalog = publication.control_inclusion.signed_head.head.catalog;
    let (source_root, source_head, end, closed) =
        match publication.published(authority, database, control_root, Some(authorization.stream))? {
            Published::Closed(segment) => {
                if selected.is_some_and(|cut| !cut.matches(&segment)) {
                    return Err(invalid("The publication differs from the selected predecessor, successor, or end"));
                }
                (segment.source_root, segment.source_head, segment.end, true)
            }
            Published::Open(floor) => {
                if selected.is_some() {
                    return Err(invalid("The publication is an open floor, not the selected cut"));
                }
                (floor.source_root, floor.source_head, floor.end(), false)
            }
        };
    if source_root == control_root || source_head.catalog == control_catalog {
        return Err(invalid(
            "Lineage requires independent control and application catalogs",
        ));
    }
    if source_head.stream != authorization.stream
        || source_head.catalog != authorization.catalog
        || source_root != application_root
    {
        return Err(invalid("The lineage segment covers another journal"));
    }
    if end < authorization.at {
        // Only a closed segment's end is final; an open floor still grows.
        return Ok(if closed { Settlement::Release } else { Settlement::Pending });
    }
    let Some(inclusion) = inclusion else {
        return Ok(Settlement::Pending);
    };
    let head = inclusion.signed_head.head;
    if head.catalog != source_head.catalog
        || head.stream != source_head.stream
        || head.root != source_head.root
        || head.size != end.0
    {
        return Err(invalid(
            "The application proof does not open the published head",
        ));
    }
    let leaf = inclusion
        .verify(application_root)
        .map_err(|_| invalid("The application catalog inclusion did not verify"))?;
    if leaf.at != authorization.at {
        return Err(invalid("The application proof opens a different position"));
    }
    if !authorization.identifies(leaf) {
        return Ok(Settlement::Release);
    }
    match leaf.timing {
        None => Ok(Settlement::Pending),
        Some(timing) if timing != authorization.timing => {
            Err(invalid("The authorized append has conflicting timing evidence"))
        }
        Some(timing) if leaf.quorum_durable_upper_micros < timing.exclusive_cutoff_micros => {
            Ok(Settlement::Capture)
        }
        Some(_) => Err(invalid("The authorized append has an invalid durability bound")),
    }
}

fn invalid(message: &str) -> Anomaly {
    Anomaly::incorrect("rtdb/lineage-proof", message)
}

#[cfg(test)]
mod tests;
