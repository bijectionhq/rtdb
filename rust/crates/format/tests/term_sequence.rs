use prost::Message;
use rtdb_format::catalog::BlockCatalog;
use rtdb_format::envelope::{ReplicaBody, ReplicaEnvelope};
use rtdb_format::generated::xtdb::{block::proto::Block, log::proto as pb};
use rtdb_types::{Term, TermSequence};

#[test]
fn every_message_kind_carries_its_term_position_through_the_codec() {
    let messages = [
        ReplicaBody::ResolvedTx(pb::ResolvedTx { tx_id: Some(1), system_time_micros: Some(0), committed: Some(true), src_msg_id: Some(1), ..Default::default() }),
        ReplicaBody::TriesAdded(pb::TriesAdded { storage_version: Some(1), storage_epoch: Some(0), source_msg_id: Some(1), ..Default::default() }),
        ReplicaBody::BlockBoundary(pb::BlockBoundary { block_index: Some(0), latest_processed_msg_id: Some(1), ..Default::default() }),
        ReplicaBody::BlockUploaded(pb::BlockUploaded { storage_version: Some(1), storage_epoch: Some(0), block_index: Some(0), latest_processed_msg_id: Some(1), ..Default::default() }),
        ReplicaBody::NoOp(pb::NoOp::default()),
        ReplicaBody::TriesDeleted(pb::TriesDeleted { table_name: Some("public/foo".into()), trie_keys: vec!["trie".into()] }),
        ReplicaBody::OversizedMessage(pb::OversizedMessage { storage_version: Some(1), storage_epoch: Some(0), block_index: Some(0), payload_id: Some("payload".into()) }),
    ];
    for message in messages {
        let stamped = ReplicaEnvelope { term: Term(3), term_sequence: Some(TermSequence(5)), message };
        assert_eq!(ReplicaEnvelope::decode(&stamped.encode()).unwrap(), Some(stamped));
    }
}

#[test]
fn a_claim_position_of_zero_reads_back_as_zero_distinct_from_none() {
    let legacy = ReplicaEnvelope { term: Term(3), term_sequence: None, message: ReplicaBody::NoOp(pb::NoOp::default()) };
    let claim = legacy.clone().with_sequence(TermSequence(0));
    assert_eq!(legacy.encode(), [3, 0x2a, 0, 0x38, 3]);
    assert_eq!(claim.encode(), [3, 0x2a, 0, 0x38, 3, 0x48, 0]);
    assert_eq!(ReplicaEnvelope::decode(&legacy.encode()).unwrap().unwrap().term_sequence, None);
    assert_eq!(ReplicaEnvelope::decode(&claim.encode()).unwrap().unwrap().term_sequence, Some(TermSequence(0)));
}

#[test]
fn the_catalog_reads_back_the_boundary_term_position_from_the_block() {
    for sequence in [None, Some(0), Some(17)] {
        let block = Block { block_index: Some(0), latest_processed_msg_id: Some(100), boundary_replica_msg_id: Some(42), term_id: Some(3), boundary_term_seq: sequence, ..Default::default() };
        let catalog = BlockCatalog::decode(&block.encode_to_vec()).unwrap();
        assert_eq!(catalog.boundary_term_sequence(), sequence.map(TermSequence));
        assert_eq!(catalog.boundary_term(), Term(3));
    }
}

#[test]
fn an_oversized_payload_cannot_replace_an_admitted_term_or_sequence() {
    let envelope = ReplicaEnvelope { term: Term(3), term_sequence: Some(TermSequence(4)), message: ReplicaBody::OversizedMessage(pb::OversizedMessage {
        storage_version: Some(6), storage_epoch: Some(0), block_index: Some(1), payload_id: Some("uuid".into()),
    }) };
    let payload = ReplicaEnvelope { term: Term(4), term_sequence: Some(TermSequence(4)), message: ReplicaBody::NoOp(pb::NoOp::default()) };
    assert_eq!(envelope.unwrap_payload(&payload.encode()).unwrap_err().code, "rtdb.format/oversized-term-mismatch");
}
