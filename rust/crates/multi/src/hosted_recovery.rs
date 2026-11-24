use std::collections::BTreeMap;

use rtdb_directory::{Cas, Checkpoint, DeploymentRecord, Directory, JournalAnchor, MemberId, MemberRecord, ReplicaJournalUnit, ReplicaPayloadCandidate, ReplicaPayloadTrim};
use rtdb_format::{envelope::ReplicaEnvelope, keys::CURRENT_STORAGE_VERSION};
use rtdb_logkeeper::{JournalDigest, StreamConfig};
use rtdb_storage::{Error, IoRegion, log::LogOffset};
use rtdb_types::{Result, StorageEpoch};

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

/// Restore only checkpoint evidence bound to this adopted catalog and recovery inventory.
pub(crate) fn checkpoint_decision(record: &DeploymentRecord) -> Result<Option<rtdb_logkeeper::StableDecision>> {
    let Some(witness) = record.checkpoint_decision.as_ref() else { return Ok(None); };
    let invalid = || Error::Task("The checkpoint has no matching journal decision".into());
    if witness.stable.len() > 16 << 10 { return Err(invalid().into()); }
    let proof = <rtdb_logkeeper::StableDecision as prost::Message>::decode(witness.stable.as_slice())
        .map_err(|_| invalid())?;
    let statement = &proof.certificate.statement;
    if !proof.authentic() || Some(witness.checkpoint) != record.checkpoint
        || Some(witness.manifest) != record.recovery_manifest_sha256
        || statement.scope.stream != record.stream("replica") || statement.scope.epoch > record.epoch
        || statement.prefix.through < witness.checkpoint.boundary
        || statement.scope.journals.iter().any(|id| {
            !record.replica_journals.iter().any(|journal| journal.journal() == Some(id.as_str()))
                && !record.ensemble.iter().any(|member| member.as_str() == id)
        })
    {
        return Err(invalid().into());
    }
    Ok(Some(proof))
}

pub(crate) fn observe_checkpoint(
    stored: &DeploymentRecord, checkpoint: Checkpoint, manifest: [u8; 32], decision: Option<[u8; 32]>,
) {
    crate::residence_trace::record("publish-checkpoint", || serde_json::json!({
        "lineage": stored.lineage, "installation": stored.id, "incarnation": stored.incarnation,
        "revision": stored.version, "writer_epoch": stored.epoch, "epoch": stored.log_epoch,
        "stream": stored.stream("replica"), "block": checkpoint.block, "through": checkpoint.boundary,
        "root": checkpoint.root_sha256, "manifest": manifest, "decision": decision,
    }));
}

pub(crate) struct ReplicaTrimPlan {
    record: DeploymentRecord,
    through: LogOffset,
}

pub(crate) struct ReplicaTrimGrant {
    stream: StreamConfig,
    through: LogOffset,
    retirement: Option<(String, ReplicaPayloadTrim)>,
    residence_key: String,
    scope: TrimScope,
}

struct TrimScope {
    installation: String,
    incarnation: u64,
    log_epoch: i32,
    revision: rtdb_directory::Version,
    anchor: [u8; 32],
}

const REPLICA_PAYLOAD_TRIM_BATCH: i64 = 64;
const MAX_PENDING_REPLICA_PAYLOADS: usize = 128;

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
        keys: &dyn crate::keys::KeyService, offloads: bool) -> Result<Option<ReplicaTrimGrant>> {
        let servers = ensemble_servers(&self.record, members)?;
        let stream = StreamConfig {
            write_quorum: write_quorum(servers.len()), servers,
            stream: self.record.stream("replica"), maximum_message_bytes: 4 << 20,
        };
        if let Some(proof) = checkpoint_decision(&self.record)? {
            // Keep this decision's exact suffix intact until a subsequent
            // publication leaves a newer authenticated recovery boundary.
            self.through.0 = self.through.0.min(proof.certificate.statement.prefix.predecessor);
            if self.through.0 < 0
                || self.record.replica_anchor.as_ref().is_some_and(|anchor| self.through.0 < anchor.interval[1])
            {
                return Ok(None);
            }
        }
        let mut retirement = None;
        let mut changed = false;
        let anchor = self.record.replica_anchor.as_deref();
        if let Some(pending) = self.record.replica_payload_trim.as_deref()
            .filter(|pending| pending.belongs_to(&self.record) && !pending.applied)
        {
            if pending.through > self.through.0
                || anchor.is_none_or(|anchor| anchor.interval[1] != pending.through)
            { return Ok(None); }
            self.through = LogOffset(pending.through);
            retirement = Some((self.record.id.clone(), pending.clone()));
        } else if anchor.is_none_or(|anchor| anchor.interval[1] < self.through.0) {
            let transferred_first = self.record.checkpoint_transfer_activation.as_deref()
                .and_then(|activation| activation.replica.bootstrap.terminal.fence.destination.checkpoint.boundary.checked_add(1));
            let initial = if anchor.is_none() && transferred_first.is_none() {
                Some(rtdb_logkeeper::certify_committed_prefix(&stream, self.through.0, None).await?)
            } else { None };
            let first = anchor.map(|anchor| anchor.interval[1] + 1).or(transferred_first)
                .or_else(|| initial.as_ref().map(|proof| proof.interval[0])).unwrap_or(0);
            self.through.0 = self.through.0.min(first.saturating_add(REPLICA_PAYLOAD_TRIM_BATCH - 1));
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
            {
                let mut candidates = self.record.replica_payload_trim.as_ref()
                    .filter(|trim| trim.belongs_to(&self.record)).map(|trim| trim.candidates.clone()).unwrap_or_default();
                let (journal_units, detached) = trim_census(
                    &self.record, &stream, keys, first, self.through.0, offloads,
                ).await?;
                candidates.extend(detached);
                if candidates.len() > MAX_PENDING_REPLICA_PAYLOADS { return Ok(None); }
                let pending = ReplicaPayloadTrim { incarnation: self.record.incarnation, log_epoch: self.record.log_epoch,
                    through: self.through.0, generation: self.record.storage_generation, applied: false, candidates, journal_units };
                self.record.replica_payload_trim = Some(Box::new(pending.clone()));
                retirement = Some((self.record.id.clone(), pending));
            }
            let root_incarnation = anchor.map_or(self.record.incarnation, |anchor| anchor.root_incarnation);
            self.record.replica_anchor = Some(Box::new(JournalAnchor { root_incarnation, interval: proof.interval, digest }));
            changed = true;
        }
        let stored = if changed {
            let version = self.record.version;
            let Cas::Applied(stored) = directory.replace_deployment(self.record, version).await? else {
                return Ok(None);
            };
            stored
        } else {
            self.record
        };
        let Some(anchor) = stored.replica_anchor.as_ref().filter(|anchor| anchor.interval[1] >= self.through.0)
            else { return Ok(None); };
        let scope = TrimScope { installation: stored.id.clone(), incarnation: stored.incarnation,
            log_epoch: stored.log_epoch, revision: stored.version, anchor: anchor.digest };
        crate::residence_trace::record("authorize-trim", || serde_json::json!({
            "lineage": stored.lineage, "installation": scope.installation, "incarnation": scope.incarnation,
            "revision": scope.revision, "stream": stream.stream, "epoch": scope.log_epoch, "through": self.through.0,
            "anchor": scope.anchor, "checkpoint": stored.checkpoint, "manifest": stored.recovery_manifest_sha256,
            "first_required": stored.required_replica_offsets(rtdb_directory::now_ms()).collect::<Vec<_>>(),
        }));
        if let Some(work) = stored.replica_payload_trim.as_ref().filter(|work| {
            work.belongs_to(&stored) && work.through == anchor.interval[1] && !work.candidates.is_empty()
        }) {
            let residence = crate::replay_broker::residence(&stored);
            crate::residence_trace::record("detached-recovery-fence", || serde_json::json!({
                "lineage": stored.lineage, "installation": stored.id, "incarnation": stored.incarnation,
                "stream": stream.stream, "epoch": stored.log_epoch, "generation": stored.storage_generation,
                "residence": residence, "revision": stored.version, "interval": anchor.interval,
                "root_incarnation": anchor.root_incarnation, "anchor": anchor.digest,
                "checkpoint": stored.checkpoint, "manifest": stored.recovery_manifest_sha256,
                "candidates": work.candidates.iter().map(|candidate| serde_json::json!({
                    "key": format!("{residence}/{}", candidate.key), "publication": candidate.version,
                    "position": candidate.position, "frame": candidate.frame_sha256,
                })).collect::<Vec<_>>(),
            }));
        }
        Ok(Some(ReplicaTrimGrant {
            stream, through: self.through, retirement, scope, residence_key: stored.key_id(),
        }))
    }
}

impl ReplicaTrimGrant {
    pub(crate) async fn apply(self, directory: &dyn Directory, keys: &dyn crate::keys::KeyService) -> Result<i64> {
        let mut settlement = None;
        if let Some((deployment, pending)) = self.retirement {
            let current = directory.deployment(&deployment).await?;
            if !current.as_ref().is_some_and(|record| {
                pending.belongs_to(record) && record.key_id() == self.residence_key
                    && record.replica_payload_trim.as_deref().is_some_and(|current| current.same_journal_work(&pending))
                    && record.replica_anchor.as_ref().is_some_and(|anchor| anchor.digest == self.scope.anchor)
            }) {
                return Err(Error::Task("The exact journal retirement grant no longer owns this prefix".into()).into());
            }
            if !complete_unit_census(&pending.journal_units, self.through.0) {
                return Err(Error::Task("The journal retirement grant has an incomplete unit census".into()).into());
            }
            rtdb_logkeeper::trim(&self.stream, self.through).await?;
            let labels = pending.journal_units.iter().map(|unit| unit.label.clone()).collect::<Vec<_>>();
            crate::keys::withdraw_archive_records(keys, &self.residence_key, &labels, &IoRegion::new()).await?;
            let stored = crate::admin::update_installation(directory, &deployment, |record| {
                let matches = pending.belongs_to(record)
                    && record.replica_anchor.as_ref().is_some_and(|anchor| anchor.interval[1] == pending.through)
                    && record.replica_payload_trim.as_deref().is_some_and(|current| current.same_journal_work(&pending))
                    && record.key_id() == self.residence_key;
                if !matches {
                    return Err(rtdb_types::Anomaly::new(rtdb_types::AnomalyCategory::Conflict,
                        "rtdb.multi/replica-trim-changed", "The authorized replica trim changed before settlement"));
                }
                record.replica_payload_trim.as_mut().unwrap().applied = true;
                Ok(())
            }).await?;
            settlement = Some(stored.version);
        } else {
            rtdb_logkeeper::trim(&self.stream, self.through).await?;
        }
        crate::residence_trace::record("apply-trim", || serde_json::json!({
            "installation": self.scope.installation, "incarnation": self.scope.incarnation, "epoch": self.scope.log_epoch,
            "revision": self.scope.revision, "anchor": self.scope.anchor, "stream": self.stream.stream,
            "through": self.through.0, "settled_revision": settlement,
            "journals": settlement.map(|_| self.stream.servers.iter()
                .filter_map(|endpoint| endpoint.journal()).collect::<Vec<_>>()),
        }));
        Ok(self.through.0)
    }
}

fn complete_unit_census(units: &[ReplicaJournalUnit], through: i64) -> bool {
    if units.len() as i64 > REPLICA_PAYLOAD_TRIM_BATCH
        || units.last().is_none_or(|unit| unit.position != through)
    {
        return false;
    }
    units.windows(2).all(|pair| pair[0].position.checked_add(1) == Some(pair[1].position))
        && units.iter().all(|unit| unit.position >= 0 && unit.frame_sha256 != [0; 32]
            && unit.label.starts_with("rtdb.multi/journal-unit/") && unit.label.len() <= 256)
}

async fn trim_census(record: &DeploymentRecord, stream: &StreamConfig,
    keys: &dyn crate::keys::KeyService, first: i64, through: i64, offloads: bool,
) -> Result<(Vec<ReplicaJournalUnit>, Vec<ReplicaPayloadCandidate>)> {
    let certificate = rtdb_logkeeper::certify_committed_range(stream, first, through).await?;
    let mut reader = rtdb_logkeeper::CertifiedRangeReader::new(stream, &certificate)?;
    let journal = crate::logs::JournalUnits::open(
        keys, &record.key_id(), &record.replica_seal_scope(), rtdb_storage::IoRegion::new(),
    ).await?;
    let mut units = Vec::new();
    let mut candidates = Vec::new();
    while let Some(page) = reader.next_page().await? {
        for entry in page {
            let bytes = journal.decrypt(entry.payload.clone()).await?;
            let frame_sha256 = ring::digest::digest(&ring::digest::SHA256, &entry.payload);
            let frame_sha256 = frame_sha256.as_ref().try_into().expect("SHA-256 is 32 bytes");
            units.push(ReplicaJournalUnit {
                position: entry.lsn, frame_sha256, label: journal.retirement_label(&entry.payload)?,
            });
            if !offloads { continue; }
            let Some(envelope) = ReplicaEnvelope::decode(&bytes)? else { continue; };
            let Some((_, version)) = envelope.offload_publication()? else { continue; };
            let relative = envelope.oversized_payload(CURRENT_STORAGE_VERSION, StorageEpoch(0))?
                .ok_or_else(|| Error::Task("A replica publication has no payload key".into()))?;
            candidates.push(ReplicaPayloadCandidate { key: relative, position: entry.lsn, frame_sha256, version, grant: None });
        }
    }
    Ok((units, candidates))
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
        record.storage_regions.insert(hold, StorageRegion::ReplicaReplay { first_required: 7, through: 7, custody: ReplayCustody::Local });
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
