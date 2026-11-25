use prost::Message;
use rtdb_types::{Anomaly, JournalEndpoint, Result, valid_journal_identity};
use uuid::Uuid;

use crate::{CatalogAdmissionFloor, Checkpoint, DeploymentRecord, JournalAnchor, Lifecycle, MemberId, StorageNamespaces, Version};

const WIRE_VERSION: u8 = 1;
const BOUND_JOURNAL_VERSION: u8 = 2;
const PLACED_RECEIPT_VERSION: u8 = 3;

#[derive(Clone, PartialEq, Message)]
struct ParkedDeployment {
    #[prost(string, tag = "1")]
    org: String,
    #[prost(string, optional, tag = "2")]
    lineage: Option<String>,
    #[prost(uint64, tag = "3")]
    incarnation: u64,
    #[prost(bytes = "vec", tag = "4")]
    ensemble: Vec<u8>,
    #[prost(sint64, tag = "5")]
    created_at_ms: i64,
    #[prost(uint32, tag = "6")]
    read_width: u32,
    #[prost(uint32, tag = "7")]
    activation_crashes: u32,
    #[prost(sint32, tag = "8")]
    log_epoch: i32,
    #[prost(string, optional, tag = "9")]
    parent: Option<String>,
    #[prost(bool, tag = "10")]
    linearizable_reads: bool,
    #[prost(message, optional, tag = "11")]
    checkpoint: Option<PackedCheckpoint>,
    #[prost(bytes = "vec", tag = "12")]
    storage_generation: Vec<u8>,
    #[prost(bytes = "vec", tag = "13")]
    storage_namespaces: Vec<u8>,
    #[prost(bool, tag = "14")]
    quarantined: bool,
    #[prost(sint64, optional, tag = "15")]
    s3_write_lease_until_ms: Option<i64>,
    #[prost(string, optional, tag = "16")]
    engine_pin: Option<String>,
    #[prost(bool, tag = "22")]
    placed_receipt_retirement: bool,
    #[prost(string, repeated, tag = "17")]
    replica_journals: Vec<String>,
    #[prost(sint64, optional, tag = "18")]
    compaction_replay_cutoff: Option<i64>,
    #[prost(message, optional, tag = "19")]
    replica_anchor: Option<PackedJournalAnchor>,
    #[prost(message, repeated, tag = "20")]
    catalog_admission_floors: Vec<PackedCatalogFloor>,
    #[prost(message, repeated, tag = "21")]
    bound_journals: Vec<PackedJournal>,
}

#[derive(Clone, PartialEq, Message)]
struct PackedJournal {
    #[prost(string, tag = "1")]
    address: String,
    #[prost(bytes = "vec", tag = "2")]
    journal: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct PackedCatalogFloor {
    #[prost(string, tag = "1")]
    root: String,
    #[prost(string, tag = "2")]
    residence: String,
    #[prost(bytes = "vec", tag = "3")]
    object_namespace: Vec<u8>,
    #[prost(sint32, tag = "4")]
    storage_version: i32,
    #[prost(sint32, tag = "5")]
    storage_epoch: i32,
    #[prost(sint32, tag = "6")]
    replica_epoch: i32,
    #[prost(sint64, tag = "7")]
    through: i64,
}

#[derive(Clone, PartialEq, Message)]
struct PackedCheckpoint {
    #[prost(sint64, tag = "1")]
    block: i64,
    #[prost(sint64, tag = "2")]
    boundary: i64,
    #[prost(sint64, tag = "3")]
    source: i64,
    #[prost(sint64, tag = "4")]
    system_time: i64,
    #[prost(bytes = "vec", tag = "5")]
    root_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "6")]
    recovery_manifest_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "7")]
    checkpoint_decision: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct PackedJournalAnchor {
    #[prost(uint64, tag = "1")]
    root_incarnation: u64,
    #[prost(sint64, tag = "2")]
    first: i64,
    #[prost(sint64, tag = "3")]
    through: i64,
    #[prost(bytes = "vec", tag = "4")]
    digest: Vec<u8>,
}

pub(super) fn encode(record: &DeploymentRecord) -> Option<Vec<u8>> {
    let DeploymentRecord {
        id, name: _, org, lineage, incarnation, lifecycle, epoch: _, holder, resident_idle: _, ensemble, created_at_ms, read_width,
        activation_crashes, log_epoch, parent, linearizable_reads, checkpoint, recovery_manifest_sha256, checkpoint_decision, replica_anchor, replica_payload_trim, storage_generation, transfer_replay_namespace,
        storage_namespaces, s3_write_lease_until_ms, engine_pin, placed_receipt_retirement, deletion_observation, move_intent, placement_move, journal_placement, writer_handoffs, move_from, log_copy_servers, cell_move, moved_at_ms,
        source_cleanup, deletion_record, replica_journals, storage_regions, local_publications, catalog_readers, classic_catalog_drain, classic_flight, catalog_admission_floors, ordered_indexes, ordered_index_catalogs,
        cell_transfer, abandoned_generations, disposal, checkpoint_recovery, serving_replay_capture,
        checkpoint_stage, checkpoint_transfer_fence, checkpoint_transfer_terminal, checkpoint_transfer_bootstrap, checkpoint_transfer_replica, checkpoint_transfer_activation, fork_stage, checkpoint_repair_receipts, checkpoint_captures, checkpoint_worlds, checkpoint_suffix_pins, checkpoint_capture_retirements,
        archive_retirement, compaction_replay_cutoff, retired_residences, version: _,
    } = record;
    if holder.is_some() || !matches!(lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
        || move_intent.is_some() || placement_move.is_some() || journal_placement.is_some() || !writer_handoffs.is_empty() || move_from.is_some() || cell_move.is_some()
        || moved_at_ms.is_some() || source_cleanup.is_some() || deletion_observation.is_some() || deletion_record.is_some() || !log_copy_servers.is_empty()
        || !storage_regions.is_empty() || !local_publications.is_empty() || !catalog_readers.is_empty() || transfer_replay_namespace.is_some() || cell_transfer.is_some() || !abandoned_generations.is_empty() || disposal.is_some() || checkpoint_recovery.is_some() || serving_replay_capture.is_some()
        || !ordered_indexes.is_empty() || !ordered_index_catalogs.is_empty() || replica_payload_trim.is_some() || classic_catalog_drain.is_some() || classic_flight.is_some()
        || checkpoint_stage.is_some() || checkpoint_transfer_fence.is_some() || checkpoint_transfer_terminal.is_some() || checkpoint_transfer_bootstrap.is_some() || checkpoint_transfer_replica.is_some() || checkpoint_transfer_activation.is_some() || fork_stage.is_some() || !checkpoint_repair_receipts.is_empty() || !checkpoint_capture_retirements.is_empty()
        || !retired_residences.is_empty() || !checkpoint_captures.is_empty() || !checkpoint_worlds.is_empty() || !checkpoint_suffix_pins.is_empty() || archive_retirement.is_some()
    {
        return None;
    }

    let mut members = Vec::with_capacity(ensemble.len() * 16);
    for member in ensemble {
        let uuid = Uuid::try_parse(member.as_str()).ok()?;
        if uuid.hyphenated().encode_lower(&mut [0; 36]) != member.as_str() { return None; }
        members.extend_from_slice(uuid.as_bytes());
    }
    let mut namespaces = Vec::new();
    if let Some(StorageNamespaces { objects, keys }) = storage_namespaces {
        namespaces.extend_from_slice(objects.as_bytes());
        namespaces.extend_from_slice(keys);
    }

    let mut unbound_journals = Vec::new();
    let mut bound_journals = Vec::new();
    for endpoint in replica_journals {
        match endpoint {
            JournalEndpoint::Member { address, journal } => {
                if !valid_journal_identity(journal) { return None; }
                let id = Uuid::try_parse(journal).ok()?;
                bound_journals.push(PackedJournal { address: address.clone(), journal: id.as_bytes().to_vec() });
            }
            JournalEndpoint::Address(address) => unbound_journals.push(address.clone()),
        }
    }
    let wire_version = if *placed_receipt_retirement {
        PLACED_RECEIPT_VERSION
    } else if bound_journals.is_empty() {
        WIRE_VERSION
    } else {
        BOUND_JOURNAL_VERSION
    };
    let packed = ParkedDeployment {
        org: org.clone(),
        lineage: (lineage != id).then(|| lineage.clone()),
        incarnation: *incarnation,
        ensemble: members,
        created_at_ms: *created_at_ms,
        read_width: *read_width,
        activation_crashes: *activation_crashes,
        log_epoch: *log_epoch,
        parent: parent.clone(),
        linearizable_reads: *linearizable_reads,
        checkpoint: checkpoint.map(|Checkpoint { block, boundary, source, system_time, root_sha256 }| PackedCheckpoint {
            block, boundary, source, system_time, root_sha256: root_sha256.map_or_else(Vec::new, |bytes| bytes.to_vec()),
            recovery_manifest_sha256: recovery_manifest_sha256.map_or_else(Vec::new, |bytes| bytes.to_vec()),
            checkpoint_decision: checkpoint_decision.as_ref().filter(|witness| {
                witness.checkpoint == (Checkpoint { block, boundary, source, system_time, root_sha256 })
                    && Some(witness.manifest) == *recovery_manifest_sha256
            }).map_or_else(Vec::new, |witness| witness.stable.clone()),
        }),
        replica_anchor: replica_anchor.as_deref().map(|anchor| PackedJournalAnchor {
            root_incarnation: anchor.root_incarnation, first: anchor.interval[0], through: anchor.interval[1], digest: anchor.digest.to_vec(),
        }),
        storage_generation: storage_generation.map_or_else(Vec::new, |id| id.as_bytes().to_vec()),
        storage_namespaces: namespaces,
        catalog_admission_floors: catalog_admission_floors.iter().map(|(root, floor)| PackedCatalogFloor {
            root: root.clone(),
            residence: floor.residence.clone(),
            object_namespace: floor.object_namespace.as_bytes().to_vec(),
            storage_version: floor.cut.storage_version.0,
            storage_epoch: floor.cut.storage_epoch.0,
            replica_epoch: floor.cut.replica_epoch.0,
            through: floor.cut.through.0,
        }).collect(),
        quarantined: *lifecycle == Lifecycle::Quarantined,
        replica_journals: unbound_journals,
        bound_journals,
        compaction_replay_cutoff: *compaction_replay_cutoff,
        s3_write_lease_until_ms: *s3_write_lease_until_ms,
        engine_pin: engine_pin.clone(),
        placed_receipt_retirement: *placed_receipt_retirement,
    };
    let mut encoded = Vec::with_capacity(1 + packed.encoded_len());
    encoded.push(wire_version);
    packed.encode(&mut encoded).expect("a sized in-memory buffer accepts the record");
    Some(encoded)
}

fn invalid(message: impl Into<String>) -> Anomaly {
    Anomaly::fault("rtdb.directory/parked-record", message)
}

pub(super) fn decode(bytes: &[u8], id: &str, name: &str, epoch: u64, version: Version) -> Result<DeploymentRecord> {
    let Some((&wire_version @ (WIRE_VERSION | BOUND_JOURNAL_VERSION | PLACED_RECEIPT_VERSION), body)) = bytes.split_first() else {
        return Err(invalid("Unknown parked record version"));
    };
    let packed = ParkedDeployment::decode(body).map_err(|error| invalid(error.to_string()))?;
    if packed.placed_receipt_retirement != (wire_version == PLACED_RECEIPT_VERSION) {
        return Err(invalid("Placed receipt retirement requires parked record version 3"));
    }
    if wire_version == WIRE_VERSION && !packed.bound_journals.is_empty() {
        return Err(invalid("Bound journals require parked record version 2"));
    }
    let mut replica_journals: std::collections::BTreeSet<_> = packed.replica_journals.into_iter().map(JournalEndpoint::Address).collect();
    for journal in packed.bound_journals {
        let id = Uuid::from_slice(&journal.journal).map_err(|_| invalid("Invalid journal identifier"))?;
        if id.is_nil() { return Err(invalid("A bound journal needs a nonzero identifier")); }
        replica_journals.insert(JournalEndpoint::Member { address: journal.address, journal: id.to_string() });
    }
    let (members, trailing) = packed.ensemble.as_chunks::<16>();
    if !trailing.is_empty() { return Err(invalid("The logkeeper list has a partial member identifier")); }
    let storage_generation = match packed.storage_generation.as_slice() {
        [] => None,
        bytes => Some(Uuid::from_slice(bytes).map_err(|_| invalid("Invalid storage generation identifier"))?),
    };
    let storage_namespaces = match packed.storage_namespaces.as_slice() {
        [] => None,
        bytes => {
            let (objects, keys) = bytes.split_first_chunk::<16>().ok_or_else(|| invalid("Invalid object namespace identifier"))?;
            Some(StorageNamespaces {
                objects: Uuid::from_bytes(*objects),
                keys: keys.try_into().map_err(|_| invalid("Invalid key namespace identifier"))?,
            })
        }
    };
    let replica_anchor = packed.replica_anchor.map(|anchor| {
        if anchor.root_incarnation == 0 || anchor.root_incarnation > packed.incarnation || anchor.first < 0 || anchor.through < anchor.first {
            return Err(invalid("Invalid replica anchor range or residence"));
        }
        let digest = anchor.digest.try_into().map_err(|_| invalid("Invalid replica anchor digest"))?;
        Ok(Box::new(JournalAnchor { root_incarnation: anchor.root_incarnation, interval: [anchor.first, anchor.through], digest }))
    }).transpose()?;
    let mut catalog_admission_floors = std::collections::BTreeMap::new();
    for floor in packed.catalog_admission_floors {
        let object_namespace = Uuid::from_slice(&floor.object_namespace).map_err(|_| invalid("Invalid catalog floor namespace"))?;
        let cut = rtdb_types::CatalogCut {
            storage_version: rtdb_types::StorageVersion(floor.storage_version),
            storage_epoch: rtdb_types::StorageEpoch(floor.storage_epoch),
            replica_epoch: rtdb_types::StorageEpoch(floor.replica_epoch),
            through: rtdb_types::ReplicaPosition(floor.through),
        };
        let value = CatalogAdmissionFloor { residence: floor.residence, object_namespace, cut };
        if catalog_admission_floors.insert(floor.root, value).is_some() {
            return Err(invalid("A parked record contains multiple catalog floors for a storage root"));
        }
    }
    let (checkpoint, recovery_manifest_sha256, checkpoint_decision) = packed.checkpoint.map(|PackedCheckpoint { block, boundary, source, system_time, root_sha256, recovery_manifest_sha256, checkpoint_decision }| {
        let root_sha256 = if root_sha256.is_empty() { None } else {
            Some(root_sha256.try_into().map_err(|_| invalid("Invalid checkpoint root digest"))?)
        };
        let recovery_manifest_sha256 = if recovery_manifest_sha256.is_empty() { None } else {
            Some(recovery_manifest_sha256.try_into().map_err(|_| invalid("Invalid recovery manifest digest"))?)
        };
        let checkpoint = Checkpoint { block, boundary, source, system_time, root_sha256 };
        let decision = if checkpoint_decision.is_empty() {
            None
        } else {
            let manifest = recovery_manifest_sha256.ok_or_else(|| invalid("A checkpoint decision requires its recovery manifest"))?;
            Some(Box::new(crate::CheckpointDecision { checkpoint, manifest, stable: checkpoint_decision }))
        };
        Ok::<_, Anomaly>((Some(checkpoint), recovery_manifest_sha256, decision))
    }).transpose()?.unwrap_or_default();
    Ok(DeploymentRecord {
        id: id.to_owned(),
        name: name.to_owned(),
        org: packed.org,
        lineage: packed.lineage.unwrap_or_else(|| id.to_owned()),
        incarnation: packed.incarnation,
        lifecycle: if packed.quarantined { Lifecycle::Quarantined } else { Lifecycle::Live },
        epoch,
        holder: None,
        resident_idle: None,
        ensemble: members.iter().map(|&member| MemberId(Uuid::from_bytes(member).to_string())).collect(),
        read_width: packed.read_width,
        activation_crashes: packed.activation_crashes,
        created_at_ms: packed.created_at_ms,
        move_intent: None,
        placement_move: None,
        journal_placement: None,
        writer_handoffs: Vec::new(),
        log_epoch: packed.log_epoch,
        parent: packed.parent,
        move_from: None,
        log_copy_servers: Vec::new(),
        linearizable_reads: packed.linearizable_reads,
        checkpoint,
        recovery_manifest_sha256,
        checkpoint_decision,
        replica_anchor,
        replica_payload_trim: None,
        moved_at_ms: None,
        source_cleanup: None,
        cell_move: None,
        storage_generation,
        transfer_replay_namespace: None,
        storage_namespaces,
        s3_write_lease_until_ms: packed.s3_write_lease_until_ms,
        deletion_observation: None,
        engine_pin: packed.engine_pin,
        placed_receipt_retirement: packed.placed_receipt_retirement,
        deletion_record: None,
        replica_journals,
        storage_regions: Default::default(),
        local_publications: Default::default(),
        catalog_readers: Default::default(),
        classic_catalog_drain: None,
        classic_flight: None,
        catalog_admission_floors,
        ordered_indexes: Default::default(),
        ordered_index_catalogs: Default::default(),
        cell_transfer: None,
        abandoned_generations: Default::default(),
        disposal: None,
        retired_residences: Default::default(),
        checkpoint_recovery: None,
        serving_replay_capture: None,
        compaction_replay_cutoff: packed.compaction_replay_cutoff,
        checkpoint_stage: None,
        checkpoint_transfer_fence: None,
        checkpoint_transfer_terminal: None,
        checkpoint_transfer_bootstrap: None,
        checkpoint_transfer_replica: None,
        checkpoint_transfer_activation: None,
        fork_stage: None,
        checkpoint_repair_receipts: Default::default(),
        checkpoint_captures: Default::default(),
        checkpoint_worlds: Default::default(),
        checkpoint_suffix_pins: Default::default(),
        checkpoint_capture_retirements: Default::default(),
        archive_retirement: None,
        version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parking_preserves_the_replica_anchor_in_binary_form() {
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 1_000);
        record.replica_anchor = Some(Box::new(JournalAnchor { root_incarnation: record.incarnation, interval: [0, 63], digest: [0xd1; 32] }));
        let bytes = encode(&record).unwrap();
        assert_eq!(decode(&bytes, &record.id, &record.name, record.epoch, record.version).unwrap(), record);
        assert!(bytes.windows(32).any(|window| window == [0xd1; 32]));
    }

    #[test]
    fn parking_preserves_the_scoped_write_lease_expiration() {
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 1_000);
        record.s3_write_lease_until_ms = Some(902_000);
        let bytes = encode(&record).unwrap();
        let read_back = decode(&bytes, &record.id, &record.name, record.epoch, record.version).unwrap();
        assert_eq!(read_back, record);
        assert_eq!(read_back.active_s3_write_lease(901_999), Some(902_000));
        assert_eq!(read_back.active_s3_write_lease(902_000), None);
    }

    #[test]
    fn parking_preserves_catalog_admission_floors() {
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 1_000);
        let cut = rtdb_types::CatalogCut {
            storage_version: rtdb_types::StorageVersion(6),
            storage_epoch: rtdb_types::StorageEpoch(2),
            replica_epoch: rtdb_types::StorageEpoch(0),
            through: rtdb_types::ReplicaPosition(63),
        };
        let floor = CatalogAdmissionFloor { residence: record.storage_prefix(), object_namespace: Uuid::new_v4(), cut };
        record.catalog_admission_floors.insert("v06_e02".into(), floor);
        let bytes = encode(&record).unwrap();
        assert_eq!(decode(&bytes, &record.id, &record.name, record.epoch, record.version).unwrap(), record);
    }

    #[test]
    fn parking_preserves_the_engine_pin_and_placed_receipt_format() {
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 1_000);
        record.engine_pin = Some("rtdb-2026.11.2".into());
        record.placed_receipt_retirement = true;
        let mut bytes = encode(&record).unwrap();
        assert_eq!(bytes[0], PLACED_RECEIPT_VERSION);
        assert_eq!(decode(&bytes, &record.id, &record.name, record.epoch, record.version).unwrap(), record);
        bytes[0] = BOUND_JOURNAL_VERSION;
        assert!(decode(&bytes, &record.id, &record.name, record.epoch, record.version).is_err());
    }
}
