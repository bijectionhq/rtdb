use prost::{Message, bytes::Bytes};
use rtdb_format::catalog::BlockCatalog;
use rtdb_format::envelope::{ReplicaBody, ReplicaEnvelope, ReplicaFrame, ReplicaPeek, ReplicaPeekBody, replica_uses_extension};
use rtdb_format::{PROTOBUF_LOG_HEADER, RTDB_EXTENSION_LOG_HEADER, XTDB_REFUSAL_PREFIX};
use rtdb_format::generated::xtdb::{block::proto::Block, log::proto as pb};
use rtdb_types::{BlockIndex, PlacedAttemptScope, SourcePosition, StorageEpoch, StorageVersion, Term, TermSequence, Timestamp};

#[test]
fn arrow_byte_path_every_message_kind_keeps_prost_envelope_bytes() {
    let messages = [
        ReplicaBody::ResolvedTx(pb::ResolvedTx { tx_id: Some(1), system_time_micros: Some(0), committed: Some(true), src_msg_id: Some(1), ..Default::default() }),
        ReplicaBody::TriesAdded(pb::TriesAdded { storage_version: Some(1), storage_epoch: Some(0), source_msg_id: Some(1), ..Default::default() }),
        ReplicaBody::BlockBoundary(pb::BlockBoundary { block_index: Some(0), latest_processed_msg_id: Some(1), ..Default::default() }),
        ReplicaBody::BlockUploaded(pb::BlockUploaded { storage_version: Some(1), storage_epoch: Some(0), block_index: Some(0), latest_processed_msg_id: Some(1), ..Default::default() }),
        ReplicaBody::NoOp(pb::NoOp::default()),
        ReplicaBody::TriesDeleted(pb::TriesDeleted { table_name: Some("public/foo".into()), trie_keys: vec!["trie".into()] }),
        ReplicaBody::OversizedMessage(pb::OversizedMessage { storage_version: Some(1), storage_epoch: Some(0), block_index: Some(0), payload_id: Some("payload".into()), ..Default::default() }),
        ReplicaBody::ArtefactsAdded(pb::ArtefactsAdded { storage_version: Some(1), storage_epoch: Some(0), table_name: Some("public/foo".into()), source_msg_id: Some(1),
            entries: vec![pb::ArtefactEntry { trie_key: Some("l01-rc-b00".into()), record: Some(pb::ArtefactRecord { kind: Some("search".into()), definition_id: Some("docs_body".into()), definition_version: Some(1), stats: Some(vec![1, 2]), scope: None, file_size: None, published_version: None, pack_members: Vec::new() }) }] }),
    ];
    for message in messages {
        let stamped = ReplicaEnvelope { term: Term(3), term_sequence: Some(TermSequence(5)), message: message.clone() };
        assert_eq!(stamped.encoded_len(), stamped.encode().len());
        // An RTDB-only record keeps the same prost payload behind the header and prefix XTDB refuses.
        let mut expected = if replica_uses_extension(&message) {
            [&[RTDB_EXTENSION_LOG_HEADER][..], &XTDB_REFUSAL_PREFIX].concat()
        } else {
            vec![PROTOBUF_LOG_HEADER]
        };
        pb::ReplicaLogMessage { term_id: Some(3), term_seq: Some(5), message: Some(stamped.message.clone()) }.encode(&mut expected).unwrap();
        assert_eq!(stamped.encode(), expected);
        assert_eq!(ReplicaEnvelope::decode(&stamped.encode()).unwrap(), Some(stamped));
    }
}

#[test]
fn arrow_byte_path_replica_wire_projection_and_prost_share_the_payload() {
    let ipc = Bytes::from(vec![0xa5; 16_000]);
    let delta = Bytes::from_static(b"binary\0delta");
    let body = ReplicaBody::ResolvedTx(pb::ResolvedTx {
        tx_id: Some(-3), src_msg_id: Some(0), system_time_micros: Some(-2), search_delta: Some(delta.clone()),
        table_data: [("public/test".into(), ipc.clone())].into(), ..Default::default()
    });
    let encoded = Bytes::from(ReplicaEnvelope { term: Term(9), term_sequence: Some(TermSequence(0)), message: body }.encode());
    let peek = ReplicaPeek::read(&encoded).unwrap();
    assert_eq!((peek.term, peek.term_sequence), (Term(9), Some(TermSequence(0))));
    let ReplicaPeekBody::Resolved { tx_id, source, time, search_delta: Some(found) } = peek.body else { panic!("expected projected transaction") };
    assert_eq!((tx_id, source, time, found), (Some(-3), Some(0), Some(-2), delta.as_ref()));
    let ReplicaBody::ResolvedTx(decoded) = ReplicaFrame::decode_bytes(encoded.clone()).unwrap().message.unwrap() else { panic!("expected decoded transaction") };
    assert_eq!(decoded.search_delta.unwrap().as_ptr(), found.as_ptr());
    let offset = encoded.windows(ipc.len()).position(|window| window == ipc.as_ref()).unwrap();
    assert_eq!(decoded.table_data["public/test"].as_ptr(), encoded[offset..].as_ptr());

    let mut bad_key = encoded.to_vec();
    let key_offset = bad_key.windows(11).position(|window| window == b"public/test").unwrap();
    bad_key[key_offset] = 0xff;
    assert!(ReplicaPeek::read(&bad_key).is_none());
    assert!(ReplicaFrame::decode(&bad_key).is_err());
}

#[test]
fn a_strict_replica_record_preserves_and_checks_its_original_staging_sample() {
    let source = SourcePosition(1);
    let placement = PlacedAttemptScope::new("replica", 1).unwrap().issue()
        .identify(source, Some("owner")).unwrap();
    let tx = pb::ResolvedTx { tx_id: Some(source.0), system_time_micros: Some(100),
        src_msg_id: Some(source.0), committed: Some(true), placed_attempt: Some(placement.encode().to_vec().into()),
        acceptance_stage: Some(pb::AcceptanceStage { clock_upper_micros: Some(198), cutoff_micros: Some(200), ..Default::default() }),
        ..Default::default() };
    let encode = |tx| ReplicaEnvelope { term: Term(5), term_sequence: Some(TermSequence(3)), message: ReplicaBody::ResolvedTx(tx) }.encode();
    let encoded = encode(tx.clone());
    let strict = ReplicaEnvelope::decode(&encoded).unwrap().unwrap();
    let ReplicaBody::ResolvedTx(decoded) = &strict.message else {
        panic!("strict committed transaction should decode");
    };
    let stage = decoded.checked_acceptance_stage().unwrap().unwrap();
    assert_eq!(stage.source_position, source);
    assert_eq!(stage.ordered_at, Timestamp(100));
    assert_eq!(stage.clock_upper, Timestamp(198));
    assert_eq!(stage.cutoff, Timestamp(200));

    let proxy = strict.offloaded(StorageVersion(6), StorageEpoch(0), BlockIndex(1), "strict".into(), &encoded);
    assert_eq!(
        proxy.unwrap_payload_bytes(Bytes::copy_from_slice(&encoded)).unwrap_err().message.as_deref(),
        Some("Strict acceptance requires an inline replica record"),
    );

    let mut preflight = tx.clone();
    preflight.acceptance_stage.as_mut().unwrap().clock_upper_micros = Some(0);
    assert_eq!(encode(preflight).len(), encoded.len());

    let mut at_cutoff = tx.clone();
    at_cutoff.acceptance_stage.as_mut().unwrap().clock_upper_micros = Some(200);
    let at_cutoff_bytes = encode(at_cutoff);
    assert_eq!(at_cutoff_bytes.len(), encoded.len());
    assert!(ReplicaEnvelope::decode(&at_cutoff_bytes).is_err());
    let mut aborted = tx;
    aborted.committed = Some(false);
    assert!(ReplicaEnvelope::decode(&encode(aborted)).is_err());
}

#[test]
fn a_physical_acceptance_stage_seals_its_journal_cutoff_with_the_clock_certificate() {
    let source = SourcePosition(1);
    let placement = PlacedAttemptScope::new("replica", 1).unwrap().issue()
        .identify(source, Some("owner")).unwrap();
    let stage = |journal_cutoff_micros, clock_certificate_sha256| pb::ResolvedTx {
        tx_id: Some(source.0), system_time_micros: Some(100), src_msg_id: Some(source.0), committed: Some(true),
        placed_attempt: Some(placement.encode().to_vec().into()),
        acceptance_stage: Some(pb::AcceptanceStage {
            clock_upper_micros: Some(198), cutoff_micros: Some(200), journal_cutoff_micros, clock_certificate_sha256,
        }),
        ..Default::default()
    };
    let journal = stage(Some(210), Some(vec![7; 32])).checked_acceptance_stage().unwrap().unwrap().journal.unwrap();
    assert_eq!(journal.before(), Timestamp(210));
    assert_eq!(journal.certificate_sha256(), [7; 32]);
    assert!(stage(None, None).checked_acceptance_stage().unwrap().unwrap().journal.is_none());
    for (cutoff, certificate) in [
        (Some(210), None), (None, Some(vec![7; 32])), (Some(199), Some(vec![7; 32])),
        (Some(210), Some(vec![0; 32])), (Some(210), Some(vec![7; 31])),
    ] {
        assert!(stage(cutoff, certificate).checked_acceptance_stage().is_err());
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
        rtdb_format::envelope::SourceMessage::LegacyTransaction(vec![0xff, 0, 1].into()),
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
    assert_eq!(envelope.unwrap_payload_bytes(payload.encode().into()).unwrap_err().code, "rtdb.format/oversized-term-mismatch");
    assert_eq!(
        envelope.unwrap_payload_bytes(envelope.encode().into()).unwrap_err().message.as_deref(),
        Some("Nested oversized replica messages are invalid"),
    );
}

#[test]
fn a_collectable_offload_binds_an_exact_provider_version() {
    use rtdb_types::{PhysicalObjectVersion, S3ObjectNamespace};

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
    let mut other_bytes = bytes;
    other_bytes.extend([0xa0, 0x06, 0x01]);
    assert_eq!(ReplicaEnvelope::decode(&other_bytes).unwrap().unwrap(), body);
    assert_eq!(decoded.unwrap_payload(&other_bytes).unwrap_err().code, "rtdb.format/oversized-payload-mismatch");
    assert_eq!(decoded.unwrap_payload_bytes(other_bytes.into()).unwrap_err().code, "rtdb.format/oversized-payload-mismatch");
}

#[test]
fn replica_decoding_consumes_unknown_fields_and_rejects_malformed_suffixes() {
    let original = ReplicaEnvelope { term: Term(7), term_sequence: Some(TermSequence(4)),
        message: ReplicaBody::NoOp(pb::NoOp::default()) };
    let mut stored = original.encode();
    stored.extend([0xa0, 0x06, 0x81, 0x00]);
    assert_eq!(ReplicaEnvelope::decode(&stored).unwrap(), Some(original));
    stored.push(0);
    assert!(ReplicaEnvelope::decode(&stored).is_err());
}

#[test]
fn external_payload_detection_checks_fields_after_the_reference() {
    use rtdb_format::{PROTOBUF_LOG_HEADER, envelope::ReplicaFrame};

    let mut stored = vec![PROTOBUF_LOG_HEADER, 0x42, 0, 0x38, 7];
    assert!(ReplicaFrame::has_external_payload(&stored).unwrap());
    stored.push(0x38);
    assert!(ReplicaFrame::has_external_payload(&stored).is_err());

    let nested_reference = [PROTOBUF_LOG_HEADER, 0x63, 0x42, 0, 0x64];
    assert!(!ReplicaFrame::has_external_payload(&nested_reference).unwrap());
}

#[test]
fn replica_outer_group_traversal_caps_nesting_at_one_hundred() {
    use rtdb_format::{PROTOBUF_LOG_HEADER, envelope::ReplicaFrame};

    for depth in [100, 101] {
        let mut stored = vec![PROTOBUF_LOG_HEADER];
        stored.extend(vec![0x63; depth]);
        stored.extend(vec![0x64; depth]);
        assert_eq!(ReplicaFrame::has_external_payload(&stored).is_ok(), depth == 100);
    }
}
