use std::collections::BTreeMap;

use rtdb_directory::{Cas, Checkpoint, DeploymentRecord, Directory, JournalAnchor, MemberId, MemberRecord, ReplicaJournalUnit, ReplicaPayloadCandidate, ReplicaPayloadTrim};
use rtdb_format::{envelope::ReplicaEnvelope, keys::CURRENT_STORAGE_VERSION};
use rtdb_logkeeper::{JournalDigest, StreamConfig};
use rtdb_storage::{Error, IoRegion, log::LogOffset};
use rtdb_types::{Result, StorageEpoch};

use crate::deployment::{ensemble_servers, write_quorum};
use crate::replica_retention::{excluded_replay_start, replica_trim_through};

pub(crate) fn replay_start(record: &DeploymentRecord, after: i64) -> rtdb_storage::Result<i64> {
    first_after(after, replay_admission_floor(record)?)
}

pub(crate) fn journal_replay_start(record: &DeploymentRecord, after: i64) -> rtdb_storage::Result<i64> {
    first_after(after, anchor_end(record.replica_anchor.as_deref())?)
}

pub(crate) fn capture_replay_start(record: &DeploymentRecord, capture: &rtdb_directory::CheckpointCapture) -> rtdb_storage::Result<i64> {
    if capture.world_replay.is_some() && capture.incarnation == record.incarnation && capture.log_epoch == record.log_epoch {
        // This capture already holds the source range for a writable
        // recovery. An ordinary archive may open only new eligible replay.
        journal_replay_start(record, capture.checkpoint.boundary)
    } else {
        replay_start(record, capture.checkpoint.boundary)
    }
}

pub(crate) fn replay_admission_floor(record: &DeploymentRecord) -> rtdb_storage::Result<i64> {
    Ok(anchor_end(record.replica_anchor.as_deref())?.max(anchor_end(body_anchor(record))?))
}

pub(crate) fn body_anchor(record: &DeploymentRecord) -> Option<&JournalAnchor> {
    record.replica_payload_trim.as_ref().filter(|work| work.belongs_to(record))
        .and_then(|work| work.body_anchor.as_deref())
}

fn anchor_end(anchor: Option<&JournalAnchor>) -> rtdb_storage::Result<i64> {
    let Some(anchor) = anchor else { return Ok(-1); };
    if anchor.interval[0] < 0 || anchor.interval[1] < anchor.interval[0] || anchor.interval[1] == i64::MAX {
        return Err(Error::Task("The replica history has an invalid recovery anchor".into()));
    }
    Ok(anchor.interval[1])
}

fn first_after(after: i64, floor: i64) -> rtdb_storage::Result<i64> {
    if let Some((requested, first_retained)) = excluded_replay_start(after, floor) {
        return Err(Error::UnavailableLogRange { requested, first_retained });
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

#[derive(Clone, Copy)]
pub(crate) struct ReplicaTrimWitness {
    checkpoint: rtdb_directory::Checkpoint,
    inventory: [u8; 32],
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
    pub(crate) fn new(record: DeploymentRecord, witness: Option<ReplicaTrimWitness>, adopted_boundary: i64, trimmed: i64, now_ms: i64) -> Option<Self> {
        let witness = witness?;
        let published = record.checkpoint?;
        published.root_sha256?;
        if published != witness.checkpoint || record.recovery_manifest_sha256 != Some(witness.inventory) { return None; }
        replay_start(&record, published.boundary).ok()?;
        let boundary = published.boundary.min(adopted_boundary);
        let oldest = record.required_replica_offsets(now_ms).min().unwrap_or(boundary);
        let through = replica_trim_through(boundary, oldest).filter(|through| *through > trimmed)?;
        Some(Self { record, through: LogOffset(through) })
    }

    pub(crate) async fn authorize(mut self, directory: &dyn Directory, members: &BTreeMap<MemberId, MemberRecord>,
        keys: &dyn crate::keys::KeyService, detached: bool) -> Result<Option<ReplicaTrimGrant>> {
        let stream = replica_stream(&self.record, members)?;
        if let Some(proof) = checkpoint_decision(&self.record)? {
            // Keep this decision's exact suffix intact until a subsequent
            // publication leaves a newer authenticated recovery boundary.
            self.through.0 = self.through.0.min(proof.certificate.statement.prefix.predecessor);
        }
        if detached {
            let Some(anchor) = body_anchor(&self.record) else { return Ok(None); };
            self.through.0 = self.through.0.min(anchor.interval[1]);
        }
        if self.through.0 < 0
            || self.record.replica_anchor.as_ref().is_some_and(|anchor| self.through.0 < anchor.interval[1])
        {
            return Ok(None);
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
            let Some((first, next_anchor)) = certified_batch(&self.record, &stream, anchor, self.through.0).await?
                else { return Ok(None); };
            self.through.0 = next_anchor.interval[1];
            let (journal_units, _, _) = trim_census(&self.record, &stream, keys, first, self.through.0, false).await?;
            let mut pending = current_work(&self.record);
            pending.through = self.through.0;
            pending.applied = false;
            pending.journal_units = journal_units;
            self.record.replica_payload_trim = Some(Box::new(pending.clone()));
            retirement = Some((self.record.id.clone(), pending));
            self.record.replica_anchor = Some(Box::new(next_anchor));
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
        Ok(Some(ReplicaTrimGrant {
            stream, through: self.through, retirement, scope, residence_key: stored.key_id(),
        }))
    }
}

fn replica_stream(record: &DeploymentRecord, members: &BTreeMap<MemberId, MemberRecord>) -> Result<StreamConfig> {
    let servers = ensemble_servers(record, members)?;
    Ok(StreamConfig {
        write_quorum: write_quorum(servers.len()), servers,
        stream: record.stream("replica"), maximum_message_bytes: 4 << 20,
    })
}

fn current_work(record: &DeploymentRecord) -> ReplicaPayloadTrim {
    record.replica_payload_trim.as_deref().filter(|work| work.belongs_to(record)).cloned()
        .unwrap_or_else(|| ReplicaPayloadTrim {
            incarnation: record.incarnation, log_epoch: record.log_epoch, generation: record.storage_generation,
            through: -1, applied: true, body_anchor: None, candidates: Vec::new(), journal_units: Vec::new(),
        })
}

async fn certified_batch(record: &DeploymentRecord, stream: &StreamConfig, anchor: Option<&JournalAnchor>, through: i64)
    -> Result<Option<(i64, JournalAnchor)>> {
    let transferred = record.checkpoint_transfer_activation.as_deref()
        .map(|activation| &activation.replica.bootstrap.terminal.fence.destination);
    let transferred_first = if let Some(stage) = transferred {
        let first = stage.checkpoint.boundary.checked_add(1).filter(|first| *first >= 0)
            .ok_or_else(|| Error::Task("The activated transfer has no exact replica genesis".into()))?;
        if record.log_epoch != stage.source_log_epoch || through < first
            || anchor.is_some_and(|anchor| anchor.interval[0] != first)
        {
            return Ok(None);
        }
        Some(first)
    } else {
        None
    };
    let initial = if anchor.is_none() && transferred_first.is_none() {
        Some(rtdb_logkeeper::certify_committed_prefix(stream, through, None).await?)
    } else {
        None
    };
    let first = anchor.map(|anchor| anchor.interval[1] + 1).or(transferred_first)
        .or_else(|| initial.as_ref().map(|proof| proof.interval[0])).unwrap_or(0);
    let through = through.min(first.saturating_add(REPLICA_PAYLOAD_TRIM_BATCH - 1));
    if through < first { return Ok(None); }
    let prefix = anchor.map(|anchor| JournalDigest::from_digest_bytes(stream.stream.clone(), anchor.interval, anchor.digest));
    let proof = if let Some(initial) = initial.filter(|proof| proof.interval[1] == through) {
        initial
    } else if let Some(genesis) = transferred_first.filter(|_| prefix.is_none()) {
        rtdb_logkeeper::certify_committed_range(stream, genesis, through).await?
    } else {
        rtdb_logkeeper::certify_committed_prefix(stream, through, prefix.as_ref()).await?
    };
    let Some(digest) = proof.digest_bytes() else { return Ok(None); };
    let root_incarnation = anchor.map_or(record.incarnation, |anchor| anchor.root_incarnation);
    Ok(Some((first, JournalAnchor { root_incarnation, interval: proof.interval, digest })))
}

/// Closes new body dependencies before physical trim. An already admitted
/// suffix can finish and continues to protect exactly its covered positions.
pub(crate) async fn advance_body_fence(
    directory: &dyn Directory, members: &BTreeMap<MemberId, MemberRecord>, keys: &dyn crate::keys::KeyService,
    record: DeploymentRecord, adopted_boundary: i64,
) -> Result<Option<DeploymentRecord>> {
    let Some(checkpoint) = record.checkpoint else { return Ok(Some(record)); };
    let Some(proof) = checkpoint_decision(&record)? else { return Ok(Some(record)); };
    let through = checkpoint.boundary.min(adopted_boundary).saturating_sub(1)
        .min(proof.certificate.statement.prefix.predecessor);
    let anchor = body_anchor(&record).copied();
    if through < 0 || anchor.is_some_and(|anchor| anchor.interval[1] >= through) {
        return Ok(Some(record));
    }
    let stream = replica_stream(&record, members)?;
    let Some((first, next_anchor)) = certified_batch(&record, &stream, anchor.as_ref(), through).await?
        else { return Ok(Some(record)); };
    let (units, detached, range) = trim_census(&record, &stream, keys, first, next_anchor.interval[1], true).await?;
    let mut work = current_work(&record);
    if work.candidates.len().saturating_add(detached.len()) > MAX_PENDING_REPLICA_PAYLOADS {
        return Ok(Some(record));
    }
    let first_candidate = work.candidates.len();
    work.candidates.extend(detached);
    work.body_anchor = Some(Box::new(next_anchor));
    let version = record.version;
    let mut next = record;
    next.replica_payload_trim = Some(Box::new(work));
    let Cas::Applied(stored) = directory.replace_deployment(next, version).await? else { return Ok(None); };
    let work = stored.replica_payload_trim.as_ref().expect("the CAS stored the detached census");
    let residence = crate::replay_broker::residence(&stored);
    crate::residence_trace::record("detached-recovery-fence", || {
        let publication = |candidate: &ReplicaPayloadCandidate| serde_json::json!({
            "key": format!("{residence}/{}", candidate.key), "publication": candidate.version,
            "position": candidate.position, "frame": candidate.frame_sha256,
        });
        serde_json::json!({
            "lineage": stored.lineage, "installation": stored.id, "incarnation": stored.incarnation,
            "stream": stream.stream, "epoch": stored.log_epoch, "generation": stored.storage_generation,
            "residence": residence, "revision": stored.version, "interval": next_anchor.interval,
            "root_incarnation": next_anchor.root_incarnation, "anchor": next_anchor.digest,
            "previous": anchor, "batch": range.interval, "batch_digest": range.digest_bytes(),
            "checkpoint": stored.checkpoint, "manifest": stored.recovery_manifest_sha256,
            "decision": proof.certificate.statement.fingerprint(),
            "active_journals": stream.servers, "known_journals": stored.replica_journals,
            "holds": stored.storage_regions.iter().filter_map(|(hold, region)| {
                stored.storage_region_replica_interval(region).map(|interval| serde_json::json!({
                    "hold": hold, "interval": interval,
                }))
            }).collect::<Vec<_>>(),
            "frames": units.iter().map(|unit| serde_json::json!({
                "position": unit.position, "frame": unit.frame_sha256,
            })).collect::<Vec<_>>(),
            "discovered": work.candidates[first_candidate..].iter().map(publication).collect::<Vec<_>>(),
            "candidates": work.candidates.iter().map(publication).collect::<Vec<_>>(),
        })
    });
    Ok(Some(stored))
}

impl ReplicaTrimWitness {
    pub(crate) async fn read(record: &DeploymentRecord, store: &dyn rtdb_storage::ObjectStore,
        writes_history: bool, memory: &std::sync::Arc<rtdb_types::MemoryBudget>) -> Result<Option<Self>> {
        let Some((checkpoint, inventory)) = record.checkpoint.zip(record.recovery_manifest_sha256) else { return Ok(None); };
        let epoch = StorageEpoch(record.log_epoch);
        let catalog = crate::archive::read_published_catalog(store, checkpoint, epoch, memory).await?;
        for entry in catalog.table_entries()? {
            if entry.table.schema == "xt" || rtdb_types::TableRef::owned_schema(&entry.table.schema) { continue; }
            let table = crate::archive::table_history::read_table_block(store, &catalog, &entry, memory).await?;
            let wire = table.as_proto();
            if table.row_count() > 0 && (writes_history || wire.entity_directory.is_some() || !wire.search_runs.is_empty())
                && !wire.entity_directory.as_ref().is_some_and(|root|
                    crate::archive::table_history::complete_directory(root, catalog.block_index(), table.row_count()))
            {
                return Ok(None);
            }
        }
        if !crate::archive::matches_recovery_manifest(store, checkpoint, epoch, inventory).await? { return Ok(None); }
        Ok(Some(Self { checkpoint, inventory }))
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
) -> Result<(Vec<ReplicaJournalUnit>, Vec<ReplicaPayloadCandidate>, JournalDigest)> {
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
    Ok((units, candidates, certificate))
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

    fn witness(record: &DeploymentRecord) -> Option<ReplicaTrimWitness> {
        Some(ReplicaTrimWitness { checkpoint: record.checkpoint?, inventory: record.recovery_manifest_sha256? })
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
        let planned = |record: DeploymentRecord| ReplicaTrimPlan::new(record.clone(), witness(&record), 10, -1, 10).unwrap().through.0;
        assert_eq!(planned(record.clone()), 4);
        assert_eq!(ReplicaTrimPlan::new(record.clone(), witness(&record), 10, -1, 50).unwrap().through.0, 6);

        record.checkpoint_suffix_pins.get_mut(&pin).unwrap().archive_sha256 = Some("settled".into());
        assert_eq!(planned(record.clone()), 6);
        assert!(record.has_live_suffix_pins(10));
        record.storage_regions.remove(&hold);
        assert_eq!(planned(record), 9);
    }

    #[test]
    fn detached_census_passes_a_finite_hold_without_closing_its_positions() {
        let mut record = candidate();
        record.storage_regions.insert(uuid::Uuid::new_v4(), StorageRegion::ReplicaReplay {
            first_required: 3, through: 4, custody: ReplayCustody::Local,
        });
        let mut work = current_work(&record);
        work.body_anchor = Some(Box::new(JournalAnchor { root_incarnation: 1, interval: [0, 8], digest: [3; 32] }));
        record.replica_payload_trim = Some(Box::new(work));

        assert!(record.requires_replica_offset(3, 0));
        assert!(!record.requires_replica_offset(5, 0));
        assert!(matches!(replay_start(&record, 4), Err(Error::UnavailableLogRange { first_retained: 9, .. })));
        assert_eq!(journal_replay_start(&record, 2).unwrap(), 3);
        assert_eq!(ReplicaTrimPlan::new(record.clone(), witness(&record), 10, -1, 0).unwrap().through, LogOffset(2));
    }

    #[test]
    fn replay_admission_and_new_trims_share_the_durable_anchor() {
        let mut record = candidate();
        record.replica_anchor = Some(Box::new(JournalAnchor { root_incarnation: 1, interval: [0, 5], digest: [3; 32] }));
        assert!(matches!(replay_start(&record, 4), Err(Error::UnavailableLogRange { requested: 5, first_retained: 6 })));
        assert_eq!(replay_start(&record, 5).unwrap(), 6);

        record.replica_anchor.as_mut().unwrap().interval[1] = i64::MAX;
        assert!(replay_start(&record, 5).is_err());
        assert!(ReplicaTrimPlan::new(record.clone(), witness(&record), 10, -1, 10).is_none());
    }

    #[tokio::test]
    async fn a_semantic_trim_requires_a_directory_in_the_published_table_block() {
        use rtdb_format::{catalog::{BlockCatalog, TableBlock, TableEntry, publication_digest}, keys::block_key};
        use rtdb_storage::{MemoryStore, ObjectStore};
        use rtdb_types::{BlockIndex, TableRef};

        let store = MemoryStore::default();
        let entry = TableEntry::new(1, TableRef::new("public", "docs")).unwrap();
        let table = TableBlock::from_parts(Default::default(), &Default::default(), 1, Vec::new()).unwrap().encode();
        let mut catalog = BlockCatalog::from_proto(rtdb_format::block::proto::Block {
            block_index: Some(0), boundary_replica_msg_id: Some(10), latest_processed_msg_id: Some(10),
            tables: vec![entry.to_proto()],
            table_block_manifest: Some(rtdb_format::block::proto::TableBlockManifest {
                sha256_by_oid: BTreeMap::from([(entry.oid, publication_digest(&table).to_vec())]),
            }),
            ..Default::default()
        }).unwrap();
        let bytes = catalog.encode_for_publication();
        store.put(&ObjectKey::new(block_key(BlockIndex(0)).unwrap()).unwrap(), bytes.into()).await.unwrap();
        store.put(&ObjectKey::new(entry.slug.block_key(BlockIndex(0)).unwrap()).unwrap(), table.into()).await.unwrap();
        let mut record = candidate();
        let checkpoint = rtdb_directory::Checkpoint { block: 0, boundary: 10, source: 10, system_time: i64::MIN,
            root_sha256: catalog.publication_sha256() };
        record.checkpoint = Some(checkpoint);
        record.recovery_manifest_sha256 = Some(crate::archive::recovery_manifest_sha256(&store, checkpoint, StorageEpoch(0)).await.unwrap());
        let memory = rtdb_types::MemoryBudget::new(1 << 20);
        let missing = ReplicaTrimWitness::read(&record, &store, true, &memory).await.unwrap();
        assert!(ReplicaTrimPlan::new(record.clone(), missing, 10, -1, 0).is_none());
        let physical = ReplicaTrimWitness::read(&record, &store, false, &memory).await.unwrap();
        assert!(ReplicaTrimPlan::new(record, physical, 10, -1, 0).is_some());
    }
}
