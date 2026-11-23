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
        ReplicaBody::OversizedMessage(pb::OversizedMessage { storage_version: Some(1), storage_epoch: Some(0), block_index: Some(0), payload_id: Some("payload".into()), ..Default::default() }),
        ReplicaBody::ArtefactsAdded(pb::ArtefactsAdded { storage_version: Some(1), storage_epoch: Some(0), table_name: Some("public/foo".into()), source_msg_id: Some(1),
            entries: vec![pb::ArtefactEntry { trie_key: Some("l01-rc-b00".into()), record: Some(pb::ArtefactRecord { kind: Some("search".into()), definition_id: Some("docs_body".into()), definition_version: Some(1), stats: Some(vec![1, 2]), scope: None, file_size: None, published_version: None }) }] }),
    ];
    for message in messages {
        let stamped = ReplicaEnvelope { term: Term(3), term_sequence: Some(TermSequence(5)), message };
        assert_eq!(stamped.encoded_len(), stamped.encode().len());
        assert_eq!(ReplicaEnvelope::decode(&stamped.encode()).unwrap(), Some(stamped));
    }
}

#[test]
fn envelope_sizes_keep_zero_presence_and_signed_varint_widths() {
    for term in [-1, 0, 127, 128, i64::MAX] {
        for sequence in [None, Some(-1), Some(0), Some(128), Some(i64::MAX)] {
            let envelope = ReplicaEnvelope { term: Term(term), term_sequence: sequence.map(TermSequence), message: ReplicaBody::NoOp(pb::NoOp::default()) };
            assert_eq!(envelope.encoded_len(), envelope.encode().len());
        }
    }
    let messages = [
        rtdb_format::envelope::SourceMessage::LegacyTransaction(vec![0xff, 0, 1]),
        rtdb_format::envelope::SourceMessage::Protobuf(rtdb_format::envelope::SourceBody::FlushBlock(pb::FlushBlock { expected_block_idx: Some(-1) })),
    ];
    for message in messages { assert_eq!(message.encoded_len(), message.encode().unwrap().len()); }
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
        storage_version: Some(6), storage_epoch: Some(0), block_index: Some(1), payload_id: Some("uuid".into()), ..Default::default()
    }) };
    let payload = ReplicaEnvelope { term: Term(4), term_sequence: Some(TermSequence(4)), message: ReplicaBody::NoOp(pb::NoOp::default()) };
    assert_eq!(envelope.unwrap_payload(&payload.encode()).unwrap_err().code, "rtdb.format/oversized-term-mismatch");
}

#[test]
fn a_collectable_offload_binds_an_exact_provider_version() {
    use rtdb_types::{BlockIndex, PhysicalObjectVersion, S3ObjectNamespace, StorageEpoch, StorageVersion};

    let body = ReplicaEnvelope { term: Term(3), term_sequence: Some(TermSequence(4)), message: ReplicaBody::NoOp(pb::NoOp::default()) };
    let bytes = body.encode();
    let mut pointer = body.offloaded(StorageVersion(6), StorageEpoch(0), BlockIndex(1), "payload".into(), &bytes);
    let version = PhysicalObjectVersion {
        namespace: S3ObjectNamespace { bucket: "replica".into(), endpoint: None, region: None },
        key: "objects/oversized/b01-payload.binpb".into(), version: "exact-allocation".into(),
    };
    assert_eq!(pointer.offload_publication().unwrap(), None);
    pointer.bind_offload_publication(version.clone()).unwrap();
    let encoded = pointer.encode();
    assert!(rtdb_format::envelope::ReplicaFrame::has_external_payload(&encoded).unwrap());
    assert!(!rtdb_format::envelope::ReplicaFrame::has_external_payload(&bytes).unwrap());
    let decoded = ReplicaEnvelope::decode(&encoded).unwrap().unwrap();
    assert_eq!(decoded.offload_publication().unwrap(), Some((StorageVersion(6), version)));
    assert_eq!(decoded.oversized_payload(StorageVersion(6), StorageEpoch(0)).unwrap().as_deref(), Some("oversized/b01-payload.binpb"));
    assert_eq!(decoded.unwrap_payload(&bytes).unwrap(), body);
}
