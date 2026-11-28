//! Offline settlement of append authorizations against the published lineage.

use journal_contract::{Epoch, Lsn, StreamId};
use rtdb_stream_journal::authenticated::{CatalogRoot, Hash, Head, Inclusion, Leaf, Timing};
use rtdb_types::{Anomaly, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{Binding, LINEAGE_PROTOCOL, State, activation_stream_id};

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
    pub(super) fn valid(&self) -> bool {
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
        let state: State = serde_json::from_slice(&self.control_record)
            .map_err(|_| invalid("The control record is not a supported activation record"))?;
        let publication = state
            .publication
            .ok_or_else(|| invalid("The control record has no activation publication"))?;
        let segment = publication
            .segment
            .ok_or_else(|| invalid("The activation publication has no lineage segment"))?;
        if state.version != LINEAGE_PROTOCOL
            || !segment.valid()
            || segment.authority != authority
            || segment.database != database
            || segment.predecessor != publication.predecessor
            || segment.successor != state.binding
        {
            return Err(invalid(
                "The activation publication does not select this lineage segment",
            ));
        }
        Ok(segment)
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

    fn matches(self, leaf: &Leaf) -> bool {
        leaf.stream == self.stream
            && leaf.at == self.at
            && leaf.writer_epoch == self.writer_epoch
            && leaf.payload_sha256 == self.payload_sha256
            && leaf.timing == Some(self.timing)
            && leaf.quorum_durable_upper_micros < self.timing.exclusive_cutoff_micros
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Settlement {
    Pending,
    Capture,
    Release,
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
    if !authorization.valid() {
        return Err(invalid("Invalid funded append authorization"));
    }
    let Some(publication) = publication else {
        return Ok(Settlement::Pending);
    };
    let segment = publication.verify(authority, database, control_root)?;
    if segment.source_root == control_root
        || segment.source_catalog == publication.control_inclusion.signed_head.head.catalog
    {
        return Err(invalid(
            "Lineage requires independent control and application catalogs",
        ));
    }
    if segment.source_stream != authorization.stream
        || segment.source_catalog != authorization.catalog
        || segment.source_root != application_root
    {
        return Err(invalid("The lineage segment covers another journal"));
    }
    if segment.end < authorization.at {
        return Ok(Settlement::Release);
    }
    let Some(inclusion) = inclusion else {
        return Ok(Settlement::Pending);
    };
    let head = inclusion.signed_head.head;
    if head.catalog != segment.source_catalog
        || head.stream != segment.source_stream
        || head.root != segment.source_head.root
        || head.size != segment.end.0
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
    Ok(if authorization.matches(leaf) {
        Settlement::Capture
    } else {
        Settlement::Release
    })
}

fn invalid(message: &str) -> Anomaly {
    Anomaly::incorrect("rtdb/lineage-proof", message)
}

#[cfg(test)]
mod tests;
