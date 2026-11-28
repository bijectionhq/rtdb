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

/// A signed head of one stream, with the root of the catalog that signed it.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthenticatedCut {
    pub head: Head,
    pub catalog_root: CatalogRoot,
}

impl AuthenticatedCut {
    pub fn matches(self, incarnation: [u8; 16], at: Lsn) -> bool {
        self.catalog_root.0 != [0; 32] && self.head.valid()
            && self.head.stream == StreamId(u128::from_be_bytes(incarnation))
            && self.head.size == at.0
    }

    pub fn end(self) -> Lsn {
        Lsn(self.head.size)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "SegmentRecord", into = "SegmentRecord")]
pub struct LineageSegment {
    pub authority: Uuid,
    pub database: String,
    pub predecessor: Binding,
    pub successor: Binding,
    pub source: AuthenticatedCut,
    pub previous: Option<Hash>,
}

/// The published form of a segment. Every release reads and writes it, so
/// it still repeats the source head's catalog, stream and size.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SegmentRecord {
    authority: Uuid,
    database: String,
    predecessor: Binding,
    successor: Binding,
    source_catalog: Uuid,
    source_root: CatalogRoot,
    source_stream: StreamId,
    source_head: Head,
    end: Lsn,
    previous: Option<Hash>,
}

impl TryFrom<SegmentRecord> for LineageSegment {
    type Error = &'static str;

    fn try_from(record: SegmentRecord) -> std::result::Result<Self, Self::Error> {
        let head = record.source_head;
        if head.catalog != record.source_catalog || head.stream != record.source_stream || head.size != record.end.0 {
            return Err("the lineage segment disagrees with its source head");
        }
        Ok(Self {
            authority: record.authority,
            database: record.database,
            predecessor: record.predecessor,
            successor: record.successor,
            source: AuthenticatedCut { head, catalog_root: record.source_root },
            previous: record.previous,
        })
    }
}

impl From<LineageSegment> for SegmentRecord {
    fn from(segment: LineageSegment) -> Self {
        let head = segment.source.head;
        Self {
            authority: segment.authority,
            database: segment.database,
            predecessor: segment.predecessor,
            successor: segment.successor,
            source_catalog: head.catalog,
            source_root: segment.source.catalog_root,
            source_stream: head.stream,
            source_head: head,
            end: segment.source.end(),
            previous: segment.previous,
        }
    }
}

impl LineageSegment {
    pub fn valid(&self) -> bool {
        !self.authority.is_nil()
            && !self.predecessor.incarnation.is_nil()
            && !self.successor.incarnation.is_nil()
            && self.predecessor.incarnation != self.successor.incarnation
            && self.predecessor.revision > 0
            && self.predecessor.revision.checked_add(1) == Some(self.successor.revision)
            && self.source.matches(*self.predecessor.incarnation.as_bytes(), self.end())
            && self.previous != Some([0; 32])
            && activation_stream_id(self.authority, &self.database).is_ok()
    }

    /// The inclusive position the segment ends at.
    pub fn end(&self) -> Lsn {
        self.source.end()
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
        let head = self.source.head;
        digest.update(head.catalog.as_bytes());
        digest.update(self.source.catalog_root.0);
        digest.update(head.stream.0.to_be_bytes());
        digest.update(head.statement());
        digest.update(head.size.to_be_bytes());
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
#[serde(from = "FloorRecord", into = "FloorRecord")]
pub struct LineageFloor {
    pub binding: Binding,
    pub source: AuthenticatedCut,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FloorRecord {
    binding: Binding,
    source_root: CatalogRoot,
    source_head: Head,
}

impl From<FloorRecord> for LineageFloor {
    fn from(record: FloorRecord) -> Self {
        Self { binding: record.binding, source: AuthenticatedCut { head: record.source_head, catalog_root: record.source_root } }
    }
}

impl From<LineageFloor> for FloorRecord {
    fn from(floor: LineageFloor) -> Self {
        Self { binding: floor.binding, source_root: floor.source.catalog_root, source_head: floor.source.head }
    }
}

impl LineageFloor {
    pub fn valid(&self) -> bool {
        !self.binding.incarnation.is_nil()
            && self.binding.revision > 0
            && self.end().0 > 0
            && self.source.matches(*self.binding.incarnation.as_bytes(), self.end())
    }

    pub fn end(&self) -> Lsn {
        self.source.end()
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
        if let Some(floor) = state.floor.filter(|floor| Some(floor.source.head.stream) == source) {
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
            && self.end == segment.end()
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
    let (source, closed) =
        match publication.published(authority, database, control_root, Some(authorization.stream))? {
            Published::Closed(segment) => {
                if selected.is_some_and(|cut| !cut.matches(&segment)) {
                    return Err(invalid("The publication differs from the selected predecessor, successor, or end"));
                }
                (segment.source, true)
            }
            Published::Open(floor) => {
                if selected.is_some() {
                    return Err(invalid("The publication is an open floor, not the selected cut"));
                }
                (floor.source, false)
            }
        };
    let (source_root, source_head, end) = (source.catalog_root, source.head, source.end());
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
