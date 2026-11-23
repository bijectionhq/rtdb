use std::collections::{BTreeMap, BTreeSet};

use rtdb_directory::{Cas, DeploymentRecord, Directory, JournalAnchor, MemberId, MemberRecord, ReplicaPayloadCandidate, ReplicaPayloadTrim};
use rtdb_format::{envelope::ReplicaEnvelope, keys::{CURRENT_STORAGE_VERSION, StorageRoot}};
use rtdb_logkeeper::{JournalDigest, StreamConfig};
use rtdb_storage::{Error, ObjectKey, log::LogOffset};
use rtdb_types::{JournalEndpoint, Result, StorageEpoch};

use crate::deployment::{CellStorage, ensemble_servers, write_quorum};
use crate::replica_retention::{excluded_replay_start, replica_trim_through};

pub(crate) fn replay_start(record: &DeploymentRecord, after: i64) -> rtdb_storage::Result<i64> {
    if let Some(anchor) = &record.replica_anchor {
        if anchor.interval[0] < 0 || anchor.interval[1] < anchor.interval[0] || anchor.interval[1] == i64::MAX {
            return Err(Error::Task("The replica journal has an invalid trim anchor".into()));
        }
        if let Some((requested, first_retained)) = excluded_replay_start(after, anchor.interval[1]) {
            return Err(Error::UnavailableLogRange { requested, first_retained });
        }
    }
    after.checked_add(1).filter(|first| *first >= 0)
        .ok_or_else(|| Error::Task("Invalid first required replica offset".into()))
}

pub(crate) struct ReplicaTrimPlan {
    record: DeploymentRecord,
    through: LogOffset,
}

pub(crate) struct ReplicaTrimGrant {
    stream: StreamConfig,
    through: LogOffset,
    retirement: Option<(String, ReplicaPayloadTrim)>,
}

const REPLICA_PAYLOAD_TRIM_BATCH: i64 = 64;
const MAX_PENDING_REPLICA_PAYLOADS: usize = 128;

fn known_journals(record: &DeploymentRecord, active: &[JournalEndpoint]) -> BTreeSet<JournalEndpoint> {
    record.replica_journals.iter().chain(&record.log_copy_servers)
        .chain(record.move_from.iter().flatten()).chain(active).cloned().collect()
}

impl ReplicaTrimPlan {
    pub(crate) fn new(record: DeploymentRecord, adopted_boundary: i64, trimmed: i64, now_ms: i64) -> Option<Self> {
        let published = record.checkpoint?;
        published.root_sha256?;
        record.recovery_manifest_sha256?;
        replay_start(&record, published.boundary).ok()?;
        let boundary = published.boundary.min(adopted_boundary);
        let oldest = record.required_replica_offsets(now_ms).min().unwrap_or(boundary);
        let through = replica_trim_through(boundary, oldest).filter(|through| *through > trimmed)?;
        Some(Self { record, through: LogOffset(through) })
    }

    pub(crate) async fn authorize(mut self, directory: &dyn Directory, members: &BTreeMap<MemberId, MemberRecord>,
        storage: &CellStorage, keys: &dyn crate::keys::KeyService, enabled: bool) -> Result<Option<ReplicaTrimGrant>> {
        let servers = ensemble_servers(&self.record, members)?;
        let stream = StreamConfig {
            write_quorum: write_quorum(servers.len()), servers,
            stream: self.record.stream("replica"), maximum_message_bytes: 4 << 20,
        };
        let exact = matches!(storage, CellStorage::S3 { .. })
            && (enabled || self.record.replica_payload_trim.as_ref().is_some_and(|trim| trim.belongs_to(&self.record)));
        let mut retirement = None;
        let mut changed = false;
        let anchor = self.record.replica_anchor.as_deref();
        if exact && let Some(pending) = self.record.replica_payload_trim.as_deref()
            .filter(|pending| pending.belongs_to(&self.record) && !pending.applied)
        {
            if pending.through > self.through.0
                || anchor.is_none_or(|anchor| anchor.interval[1] != pending.through)
            { return Ok(None); }
            self.through = LogOffset(pending.through);
            let mut pending = pending.clone();
            let routes = pending.journals.iter().cloned().chain(known_journals(&self.record, &stream.servers)).collect();
            pending.journals = crate::admin::retirement_journals(routes, members)?.into_iter().collect();
            changed = self.record.replica_payload_trim.as_deref() != Some(&pending);
            self.record.replica_payload_trim = Some(Box::new(pending.clone()));
            retirement = Some((self.record.id.clone(), pending));
        } else if anchor.is_none_or(|anchor| anchor.interval[1] < self.through.0) {
            let transferred_first = self.record.checkpoint_transfer_activation.as_deref()
                .and_then(|activation| activation.replica.bootstrap.terminal.fence.destination.checkpoint.boundary.checked_add(1));
            let initial = if exact && anchor.is_none() && transferred_first.is_none() {
                Some(rtdb_logkeeper::certify_committed_prefix(&stream, self.through.0, None).await?)
            } else { None };
            let first = anchor.map(|anchor| anchor.interval[1] + 1).or(transferred_first)
                .or_else(|| initial.as_ref().map(|proof| proof.interval[0])).unwrap_or(0);
            if exact { self.through.0 = self.through.0.min(first.saturating_add(REPLICA_PAYLOAD_TRIM_BATCH - 1)); }
            let prefix = anchor.map(|anchor| JournalDigest::from_digest_bytes(stream.stream.clone(), anchor.interval, anchor.digest));
            let proof = if let Some(activation) = self.record.checkpoint_transfer_activation.as_deref() {
                let stage = &activation.replica.bootstrap.terminal.fence.destination;
                let first = stage.checkpoint.boundary.checked_add(1)
                    .filter(|first| *first >= 0)
                    .ok_or_else(|| Error::Task("The activated transfer has no exact replica genesis".into()))?;
                if self.record.log_epoch != stage.source_log_epoch || self.through.0 < first
                    || anchor.is_some_and(|anchor| anchor.interval[0] != first)
                {
                    return Ok(None);
                }
                if prefix.is_none() {
                    rtdb_logkeeper::certify_committed_range(&stream, first, self.through.0).await?
                } else {
                    rtdb_logkeeper::certify_committed_prefix(&stream, self.through.0, prefix.as_ref()).await?
                }
            } else if let Some(initial) = initial.filter(|proof| proof.interval[1] == self.through.0) {
                initial
            } else {
                rtdb_logkeeper::certify_committed_prefix(&stream, self.through.0, prefix.as_ref()).await?
            };
            let Some(digest) = proof.digest_bytes() else { return Ok(None); };
            if exact {
                let journals = crate::admin::retirement_journals(
                    known_journals(&self.record, &stream.servers).into_iter().collect(), members,
                )?.into_iter().collect();
                let mut candidates = self.record.replica_payload_trim.as_ref()
                    .filter(|trim| trim.belongs_to(&self.record)).map(|trim| trim.candidates.clone()).unwrap_or_default();
                candidates.extend(offload_candidates(&self.record, &stream, storage, keys, first, self.through.0).await?);
                if candidates.len() > MAX_PENDING_REPLICA_PAYLOADS { return Ok(None); }
                let pending = ReplicaPayloadTrim { incarnation: self.record.incarnation, log_epoch: self.record.log_epoch,
                    through: self.through.0, journals, applied: false, candidates };
                self.record.replica_payload_trim = Some(Box::new(pending.clone()));
                retirement = Some((self.record.id.clone(), pending));
            }
            let root_incarnation = anchor.map_or(self.record.incarnation, |anchor| anchor.root_incarnation);
            self.record.replica_anchor = Some(Box::new(JournalAnchor { root_incarnation, interval: proof.interval, digest }));
            changed = true;
        }
        if changed {
            let version = self.record.version;
            if !matches!(directory.replace_deployment(self.record, version).await?, Cas::Applied(_)) {
                return Ok(None);
            }
        }
        Ok(Some(ReplicaTrimGrant { stream, through: self.through, retirement }))
    }
}

impl ReplicaTrimGrant {
    pub(crate) async fn apply(mut self, directory: &dyn Directory) -> Result<i64> {
        if let Some((deployment, pending)) = self.retirement {
            if pending.journals.iter().any(|journal| journal.journal().is_none()) {
                return Err(Error::Task("Replica payload settlement requires every journal's physical identity".into()).into());
            }
            self.stream.servers = pending.journals.iter().cloned().collect();
            self.stream.write_quorum = write_quorum(self.stream.servers.len());
            rtdb_logkeeper::trim_all(&self.stream, self.through).await?;
            crate::admin::update_installation(directory, &deployment, |record| {
                let matches = pending.belongs_to(record)
                    && record.replica_anchor.as_ref().is_some_and(|anchor| anchor.interval[1] == pending.through)
                    && record.replica_payload_trim.as_deref() == Some(&pending)
                    && known_journals(record, &[]).iter().all(|endpoint|
                        pending.journals.iter().any(|journal| endpoint.journal() == journal.journal()));
                if !matches {
                    return Err(rtdb_types::Anomaly::new(rtdb_types::AnomalyCategory::Conflict,
                        "rtdb.multi/replica-trim-changed", "The authorized replica trim changed before settlement"));
                }
                record.replica_payload_trim.as_mut().unwrap().applied = true;
                Ok(())
            }).await?;
        } else {
            rtdb_logkeeper::trim(&self.stream, self.through).await?;
        }
        Ok(self.through.0)
    }
}

async fn offload_candidates(record: &DeploymentRecord, stream: &StreamConfig, storage: &CellStorage,
    keys: &dyn crate::keys::KeyService, first: i64, through: i64) -> Result<Vec<ReplicaPayloadCandidate>> {
    let certificate = rtdb_logkeeper::certify_committed_range(stream, first, through).await?;
    let mut reader = rtdb_logkeeper::CertifiedRangeReader::new(stream, &certificate)?;
    let cipher = crate::cipher::Cipher::new(&keys.existing(&record.key_id()).await?)?;
    let scope = record.replica_seal_scope();
    let mut candidates = Vec::new();
    while let Some(page) = reader.next_page().await? {
        for entry in page {
            let bytes = cipher.open(&scope, &entry.payload)?;
            let Some(envelope) = ReplicaEnvelope::decode(&bytes)? else { continue; };
            let Some((_, version)) = envelope.offload_publication()? else { continue; };
            let relative = envelope.oversized_payload(CURRENT_STORAGE_VERSION, StorageEpoch(0))?
                .ok_or_else(|| Error::Task("A replica publication has no payload key".into()))?;
            candidates.push(ReplicaPayloadCandidate { key: relative, position: entry.lsn, version });
        }
    }
    if candidates.is_empty() { return Ok(candidates); }
    let CellStorage::S3 { config } = storage else { return Ok(Vec::new()); };
    let inventory = CellStorage::s3_inventory(config).await?;
    let root = StorageRoot::new(CURRENT_STORAGE_VERSION, StorageEpoch(0))?.key();
    let prefix = record.storage_prefix();
    candidates.retain(|candidate| ObjectKey::new(format!("{prefix}/{root}/{}", candidate.key))
        .is_ok_and(|key| inventory.owns_publication(&key, &candidate.version)));
    Ok(candidates)
}

#[cfg(test)]
mod tests {
    use rtdb_directory::{Checkpoint, CheckpointSuffixPin, ReplayCustody, StorageRegion};
    use rtdb_types::{Timestamp, TransactionKey, TxId};

    use super::*;

    fn checkpoint(boundary: i64) -> Checkpoint {
        Checkpoint { block: boundary, boundary, source: boundary, system_time: boundary, root_sha256: Some([1; 32]) }
    }

    fn candidate() -> DeploymentRecord {
        let mut record = DeploymentRecord::new("shop", "org", Vec::new(), 0);
        record.checkpoint = Some(checkpoint(10));
        record.recovery_manifest_sha256 = Some([2; 32]);
        record
    }

    #[test]
    fn replica_trim_tracks_journal_and_archived_custody_separately() {
        let mut record = candidate();
        let hold = uuid::Uuid::new_v4();
        let pin = uuid::Uuid::new_v4();
        record.storage_regions.insert(hold, StorageRegion::ReplicaReplay { first_required: 7, custody: ReplayCustody::Local });
        record.checkpoint_suffix_pins.insert(pin, CheckpointSuffixPin {
            capture: uuid::Uuid::new_v4(), manifest_sha256: "root".into(), checkpoint: checkpoint(4),
            custody: rtdb_directory::CheckpointSuffixCustody::Capture,
            incarnation: record.incarnation, log_epoch: record.log_epoch, writer_epoch: 1,
            basis: TransactionKey { tx_id: TxId(6), system_time: Timestamp(6) },
            replica_through: 6, source_through: 6, journal_interval: [5, 6], journal_sha256: [3; 32],
            expires_at_ms: 50, archive_sha256: None,
        });
        let planned = |record| ReplicaTrimPlan::new(record, 10, -1, 10).unwrap().through.0;
        assert_eq!(planned(record.clone()), 4);
        assert_eq!(ReplicaTrimPlan::new(record.clone(), 10, -1, 50).unwrap().through.0, 6);

        record.checkpoint_suffix_pins.get_mut(&pin).unwrap().archive_sha256 = Some("settled".into());
        assert_eq!(planned(record.clone()), 6);
        assert!(record.has_live_suffix_pins(10));
        record.storage_regions.remove(&hold);
        assert_eq!(planned(record), 9);
    }

    #[test]
    fn replay_admission_and_new_trims_share_the_durable_anchor() {
        let mut record = candidate();
        record.replica_anchor = Some(Box::new(JournalAnchor { root_incarnation: 1, interval: [0, 5], digest: [3; 32] }));
        assert!(matches!(replay_start(&record, 4), Err(Error::UnavailableLogRange { requested: 5, first_retained: 6 })));
        assert_eq!(replay_start(&record, 5).unwrap(), 6);

        record.replica_anchor.as_mut().unwrap().interval[1] = i64::MAX;
        assert!(replay_start(&record, 5).is_err());
        assert!(ReplicaTrimPlan::new(record, 10, -1, 10).is_none());
    }
}
