use prost::Message;
use rtdb_types::{Anomaly, Result};
use uuid::Uuid;

use crate::{Checkpoint, DeploymentRecord, Lifecycle, MemberId, StorageNamespaces, Version};

const WIRE_VERSION: u8 = 1;

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
}

#[derive(Clone, Copy, PartialEq, Message)]
struct PackedCheckpoint {
    #[prost(sint64, tag = "1")]
    block: i64,
    #[prost(sint64, tag = "2")]
    boundary: i64,
    #[prost(sint64, tag = "3")]
    source: i64,
    #[prost(sint64, tag = "4")]
    system_time: i64,
}

pub(super) fn encode(record: &DeploymentRecord) -> Option<Vec<u8>> {
    let DeploymentRecord {
        id, name: _, org, lineage, incarnation, lifecycle, epoch: _, holder, ensemble, created_at_ms, read_width,
        activation_crashes, log_epoch, parent, linearizable_reads, checkpoint, storage_generation,
        storage_namespaces, s3_write_lease_until_ms, move_intent, placement_move, move_from, log_copy_servers, cell_move, moved_at_ms, source_cleanup,
        deletion_observation, deletion_record, version: _,
    } = record;
    if holder.is_some() || !matches!(lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
        || move_intent.is_some() || placement_move.is_some() || move_from.is_some() || cell_move.is_some()
        || moved_at_ms.is_some() || source_cleanup.is_some() || deletion_observation.is_some() || deletion_record.is_some() || !log_copy_servers.is_empty()
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
        checkpoint: checkpoint.map(|Checkpoint { block, boundary, source, system_time }| PackedCheckpoint { block, boundary, source, system_time }),
        storage_generation: storage_generation.map_or_else(Vec::new, |id| id.as_bytes().to_vec()),
        storage_namespaces: namespaces,
        quarantined: *lifecycle == Lifecycle::Quarantined,
        s3_write_lease_until_ms: *s3_write_lease_until_ms,
    };
    let mut encoded = Vec::with_capacity(1 + packed.encoded_len());
    encoded.push(WIRE_VERSION);
    packed.encode(&mut encoded).expect("a sized in-memory buffer accepts the record");
    Some(encoded)
}

fn invalid(message: impl Into<String>) -> Anomaly {
    Anomaly::fault("rtdb.directory/parked-record", message)
}

pub(super) fn decode(bytes: &[u8], id: &str, name: &str, epoch: u64, version: Version) -> Result<DeploymentRecord> {
    let Some((&WIRE_VERSION, body)) = bytes.split_first() else { return Err(invalid("Unknown parked record version")); };
    let packed = ParkedDeployment::decode(body).map_err(|error| invalid(error.to_string()))?;
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
    Ok(DeploymentRecord {
        id: id.to_owned(),
        name: name.to_owned(),
        org: packed.org,
        lineage: packed.lineage.unwrap_or_else(|| id.to_owned()),
        incarnation: packed.incarnation,
        lifecycle: if packed.quarantined { Lifecycle::Quarantined } else { Lifecycle::Live },
        epoch,
        holder: None,
        ensemble: members.iter().map(|&member| MemberId(Uuid::from_bytes(member).to_string())).collect(),
        read_width: packed.read_width,
        activation_crashes: packed.activation_crashes,
        created_at_ms: packed.created_at_ms,
        move_intent: None,
        placement_move: None,
        log_epoch: packed.log_epoch,
        parent: packed.parent,
        move_from: None,
        log_copy_servers: Vec::new(),
        linearizable_reads: packed.linearizable_reads,
        checkpoint: packed.checkpoint.map(|PackedCheckpoint { block, boundary, source, system_time }| Checkpoint { block, boundary, source, system_time }),
        moved_at_ms: None,
        source_cleanup: None,
        cell_move: None,
        storage_generation,
        storage_namespaces,
        s3_write_lease_until_ms: packed.s3_write_lease_until_ms,
        deletion_observation: None,
        deletion_record: None,
        version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
