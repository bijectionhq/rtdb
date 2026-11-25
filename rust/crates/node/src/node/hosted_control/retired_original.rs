use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rtdb_columnar::ArrowCell;
use rtdb_query::owned_row_event_position;
use rtdb_types::Timestamp;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::*;

const MAX_HANDOFF_BYTES: usize = 4096;

/// Coordinates selected by the authenticated hosting owner for a single Original archive handoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetiredOriginalFamilySource {
    pub deployment: Uuid,
    pub history: [u8; 32],
    pub installation: Uuid,
    pub target_installation: Uuid,
    pub audience_tablet: [u8; 16],
    pub row_custody: [u8; 32],
    pub handoff: [u8; 32],
    pub carrier: TransactionKey,
    pub fence: TransactionKey,
    pub through: TransactionKey,
    pub captured: TransactionKey,
}

impl RetiredOriginalFamilySource {
    fn floor(&self, history: [u8; 32]) -> Result<AppliedReadFloor> {
        if self.history != history || self.deployment.is_nil()
            || self.installation.is_nil() || self.target_installation.is_nil()
            || self.installation == self.target_installation
            || self.audience_tablet == [0; 16] || self.row_custody == [0; 32] || self.handoff == [0; 32]
            || self.carrier.system_time.0 <= 0
            || self.carrier == self.fence || !self.carrier.at_or_before(self.fence)
            || !self.fence.at_or_before(self.through) || !self.through.at_or_before(self.captured)
        {
            return Err(hosted_source_unavailable());
        }
        AppliedReadFloor::new(self.deployment, vec![self.carrier, self.fence, self.through, self.captured])
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HandoffScope {
    domain: String,
    authority: Uuid,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchiveHandoff {
    source: HandoffScope,
    target: HandoffScope,
    capture: Uuid,
    placement_epoch: u64,
    audience_tablet: String,
    assignment: Option<Uuid>,
}

impl ArchiveHandoff {
    fn commitment(&self, fence: TransactionKey) -> [u8; 32] {
        let assignment = self.assignment.map(|id| id.to_string()).unwrap_or_default();
        Sha256::digest(format!(
            "bijection.rtdb.archive-source-handoff.v3\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}",
            self.source.domain, self.source.authority, self.target.domain, self.target.authority,
            self.capture, self.placement_epoch, assignment, fence.tx_id.0, fence.system_time.0,
            self.audience_tablet,
        )).into()
    }

    fn require_selected(&self, source: &RetiredOriginalFamilySource) -> Result<()> {
        let valid_domain = |domain: &str| !domain.is_empty() && domain.len() <= 256 && !domain.contains('\0');
        if self.source.authority != source.installation || self.target.authority != source.target_installation
            || !valid_domain(&self.source.domain) || !valid_domain(&self.target.domain)
            || self.source.domain == self.target.domain || self.capture.is_nil() || self.placement_epoch == 0
            || self.assignment.is_some_and(|id| id.is_nil())
            || self.audience_tablet != URL_SAFE_NO_PAD.encode(source.audience_tablet)
            || self.commitment(source.fence) != source.handoff
        {
            return Err(hosted_source_unavailable());
        }
        Ok(())
    }
}

fn row(point: &HostedControlPoint) -> Result<(&rtdb_columnar::OwnedBatch, Timestamp)> {
    point.row.as_ref().map(|(row, revision)| (row, *revision)).ok_or_else(hosted_source_unavailable)
}

impl CurrentAssertionFamilyRead {
    async fn require_retired(&self, source: &RetiredOriginalFamilySource) -> Result<()> {
        let unavailable = hosted_source_unavailable;
        let marker = self.carrier_point(RESTORE, ARCHIVE_FENCE).await?;
        let (fence, revision) = row(&marker)?;
        if revision != source.fence.system_time || marker.receipt_tx_id != Some(source.fence.tx_id.0) {
            return Err(unavailable());
        }
        let body = ArrowCell::named(fence.batch(), "binding", 0).and_then(ArrowCell::binary)
            .filter(|body| body.len() <= MAX_HANDOFF_BYTES).ok_or_else(unavailable)?;
        let handoff: ArchiveHandoff = serde_json::from_slice(body).map_err(|_| unavailable())?;
        handoff.require_selected(source)?;

        let installation = self.carrier_journal_installation().await?;
        let (installation, closed_at) = row(&installation)?;
        let cell = |name| ArrowCell::named(installation.batch(), name, 0);
        if closed_at != revision
            || owned_row_event_position(installation.batch(), 0)?.transaction != source.fence.tx_id
            || cell("kind").and_then(ArrowCell::utf8) != Some("hosted")
            || cell("incarnation").and_then(ArrowCell::binary) != Some(source.installation.as_bytes().as_slice())
            || cell("writer").and_then(ArrowCell::utf8) != Some("")
            || cell("placement_assignment").and_then(ArrowCell::binary) != Some(&[][..])
            || cell("placement_epoch").and_then(ArrowCell::utf8) != Some(format!("{:020}", handoff.placement_epoch).as_str())
        {
            return Err(unavailable());
        }

        let custody = self.carrier_point("bj_rtdb_custody_binding", "authority").await?;
        let (custody, _) = row(&custody)?;
        if ArrowCell::named(custody.batch(), "fingerprint", 0).and_then(ArrowCell::binary) != Some(source.row_custody.as_slice()) {
            return Err(unavailable());
        }
        let owner = self.carrier_point("bj_fact_root_owner", "root").await?;
        let (owner, _) = row(&owner)?;
        if ArrowCell::named(owner.batch(), "audience_tablet", 0).and_then(ArrowCell::binary) != Some(source.audience_tablet.as_slice()) {
            return Err(unavailable());
        }
        self.context.check_cancelled()
    }
}

pub struct RetiredOriginalFamilyRead {
    current: CurrentAssertionFamilyRead,
    deployment: Uuid,
    installation: Uuid,
    fence: TransactionKey,
}

impl RetiredOriginalFamilyRead {
    pub fn current(&self) -> &CurrentAssertionFamilyRead {
        &self.current
    }

    /// Authenticate every selected publication against the same retired
    /// Original read used to observe the handoff and current family fences.
    pub async fn require_pre_fence_publications(&self, transactions: &[TransactionKey]) -> Result<()> {
        let unavailable = hosted_source_unavailable;
        let applied = self.current.applied_source().ok_or_else(unavailable)?;
        if transactions.is_empty() || transactions.iter().any(|transaction| {
            transaction.system_time.0 <= 0 || *transaction == self.fence || !transaction.at_or_before(self.fence)
        }) {
            return Err(unavailable());
        }
        for chunk in transactions.chunks(4) {
            let floor = AppliedReadFloor::new(self.deployment, chunk.to_vec())?;
            floor.require_transactions(&self.current.context, applied, None).await?;
        }
        self.current.context.check_cancelled()
    }

    /// Authenticates a pre-fence carrier registration's creator from its exact ordered row and installation.
    pub async fn carrier_creator_commitment_at(&self, key: &str, registered: TransactionKey, address: &[u8]) -> Result<[u8; 32]> {
        let unavailable = hosted_source_unavailable;
        if registered.system_time.0 <= 0 || registered == self.fence || !registered.at_or_before(self.fence) {
            return Err(unavailable());
        }
        let context = &self.current.context;
        let head = self.current.applied_source().ok_or_else(unavailable)?;
        let floor = AppliedReadFloor::new(self.deployment, vec![registered])?;
        floor.require_transactions(context, head, None).await?;
        let at = Arc::clone(context).with_owned_position_cut(registered.tx_id)?;
        let registration = read_carrier_point(&at, "bj_external_key_recovery", key).await?;
        let (registration, revision) = row(&registration)?;
        if revision != registered.system_time
            || owned_row_event_position(registration.batch(), 0)?.transaction != registered.tx_id
            || ArrowCell::named(registration.batch(), "address", 0).and_then(ArrowCell::binary) != Some(address)
        {
            return Err(unavailable());
        }

        let installation = read_carrier_point(&at, "bj_installation", "authority").await?;
        let (installation, revision) = row(&installation)?;
        let cell = |name| ArrowCell::named(installation.batch(), name, 0);
        let writer = cell("writer").and_then(ArrowCell::utf8)
            .filter(|writer| !writer.is_empty() && writer.len() <= 128).ok_or_else(unavailable)?;
        let creator = ArrowCell::named(registration.batch(), "creator", 0).and_then(ArrowCell::utf8);
        if creator != Some(writer) || revision > registered.system_time
            || cell("kind").and_then(ArrowCell::utf8) != Some("hosted")
            || cell("incarnation").and_then(ArrowCell::binary) != Some(self.installation.as_bytes().as_slice())
        {
            return Err(unavailable());
        }
        let mut commitment = Sha256::new();
        commitment.update(b"rtdb.multi/carrier-writer/v1\0");
        commitment.update(self.installation.as_bytes());
        commitment.update(writer.as_bytes());
        context.check_cancelled()?;
        Ok(commitment.finalize().into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::TxId;

    fn key(transaction: i64, micros: i64) -> TransactionKey {
        TransactionKey { tx_id: TxId(transaction), system_time: Timestamp(micros) }
    }

    fn source() -> RetiredOriginalFamilySource {
        RetiredOriginalFamilySource {
            deployment: Uuid::from_bytes([1; 16]),
            history: [2; 32],
            installation: Uuid::from_bytes([3; 16]),
            target_installation: Uuid::from_bytes([4; 16]),
            audience_tablet: [5; 16],
            row_custody: [6; 32],
            handoff: [7; 32],
            carrier: key(5, 50),
            fence: key(10, 100),
            through: key(11, 110),
            captured: key(12, 120),
        }
    }

    #[test]
    fn retired_original_floor_requires_the_original_history_and_ordered_cuts() {
        let mut source = source();
        let floor = source.floor(source.history).unwrap();
        assert_eq!(floor.history(), source.deployment);
        assert_eq!(floor.minimum(), source.captured);
        assert!(source.floor([8; 32]).is_err());
        source.carrier = source.fence;
        assert!(source.floor(source.history).is_err());
        source.carrier = key(5, 50);
        source.through = key(10, 101);
        assert!(source.floor(source.history).is_err());
        source.through = key(13, 110);
        assert!(source.floor(source.history).is_err());
    }

    #[test]
    fn archive_fence_binds_original_successor_and_exact_transaction() {
        let mut source = source();
        let body = br#"{"source":{"domain":"original","authority":"03030303-0303-0303-0303-030303030303"},
                       "target":{"domain":"successor","authority":"04040404-0404-0404-0404-040404040404"},
                       "capture":"08080808-0808-0808-0808-080808080808","placement_epoch":9,
                       "audience_tablet":"BQUFBQUFBQUFBQUFBQUFBQ","assignment":null}"#;
        let marker: ArchiveHandoff = serde_json::from_slice(body).unwrap();
        let material = concat!(
            "bijection.rtdb.archive-source-handoff.v3\0original\0",
            "03030303-0303-0303-0303-030303030303\0successor\0",
            "04040404-0404-0404-0404-040404040404\0",
            "08080808-0808-0808-0808-080808080808\0", "9\0\0", "10\0", "100\0BQUFBQUFBQUFBQUFBQUFBQ"
        );
        source.handoff = Sha256::digest(material).into();
        assert!(marker.require_selected(&source).is_ok());
        source.fence = key(10, 99);
        assert!(marker.require_selected(&source).is_err());
        source.fence = key(10, 100);
        source.audience_tablet = [8; 16];
        assert!(marker.require_selected(&source).is_err());
        source.audience_tablet = [5; 16];
        source.target_installation = Uuid::from_bytes([9; 16]);
        assert!(marker.require_selected(&source).is_err());
    }
}

impl OriginalAssertionFamilyReader {
    /// Checks the exact closed Original attachment and approved archive handoff on its current order.
    /// The hosting caller authenticates the restore grant and archive seal before supplying their coordinates.
    pub async fn current_retired(
        &self, source: &RetiredOriginalFamilySource, cancellation: &CancellationToken,
    ) -> Result<RetiredOriginalFamilyRead> {
        let floor = source.floor(self.history_fingerprint())?;
        let current = self.current_covering(&floor, cancellation).await?;
        current.require_retired(source).await?;
        Ok(RetiredOriginalFamilyRead {
            current, deployment: source.deployment, installation: source.installation, fence: source.fence,
        })
    }
}
