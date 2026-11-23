use std::collections::BTreeMap;

use rtdb_directory::{Cas, DeploymentRecord, Directory, JournalAnchor, MemberId, MemberRecord};
use rtdb_logkeeper::{JournalDigest, StreamConfig};
use rtdb_storage::{Error, log::LogOffset};
use rtdb_types::Result;

use crate::deployment::{ensemble_servers, write_quorum};
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

    pub(crate) async fn authorize(mut self, directory: &dyn Directory, members: &BTreeMap<MemberId, MemberRecord>) -> Result<Option<ReplicaTrimGrant>> {
        let servers = ensemble_servers(&self.record, members)?;
        let stream = StreamConfig {
            write_quorum: write_quorum(servers.len()), servers,
            stream: self.record.stream("replica"), maximum_message_bytes: 4 << 20,
        };
        let anchor = self.record.replica_anchor.as_deref();
        if anchor.is_none_or(|anchor| anchor.interval[1] < self.through.0) {
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
            } else {
                rtdb_logkeeper::certify_committed_prefix(&stream, self.through.0, prefix.as_ref()).await?
            };
            let Some(digest) = proof.digest_bytes() else { return Ok(None); };
            let root_incarnation = anchor.map_or(self.record.incarnation, |anchor| anchor.root_incarnation);
            self.record.replica_anchor = Some(Box::new(JournalAnchor { root_incarnation, interval: proof.interval, digest }));
            let version = self.record.version;
            if !matches!(directory.replace_deployment(self.record, version).await?, Cas::Applied(_)) {
                return Ok(None);
            }
        }
        Ok(Some(ReplicaTrimGrant { stream, through: self.through }))
    }
}

impl ReplicaTrimGrant {
    pub(crate) async fn apply(self) -> Result<i64> {
        rtdb_logkeeper::trim(&self.stream, self.through).await?;
        Ok(self.through.0)
    }
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
