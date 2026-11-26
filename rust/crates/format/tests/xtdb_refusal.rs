use prost::{Message, bytes::Bytes};
use rtdb_format::envelope::{
    ReplicaBody, ReplicaEnvelope, ReplicaFrame, SourceBody, SourceMessage, replica_uses_extension, source_uses_extension,
};
use rtdb_format::generated::xtdb::log::proto as pb;
use rtdb_format::{PROTOBUF_LOG_HEADER, RTDB_EXTENSION_LOG_HEADER, RTDB_EXTENSION_STORAGE_VERSION, XTDB_REFUSAL_SUFFIX};
use rtdb_types::{BlockIndex, StorageEpoch, StorageVersion, Term, TermSequence};

fn trie(key: &str) -> pb::TrieDetails {
    pb::TrieDetails { table_name: Some("public/docs".into()), trie_key: Some(key.into()), data_file_size: Some(1), ..Default::default() }
}

fn tries_added(key: &str) -> pb::TriesAdded {
    pb::TriesAdded { storage_version: Some(6), storage_epoch: Some(0), source_msg_id: Some(1), tries: vec![trie(key)] }
}

fn envelope(message: ReplicaBody) -> ReplicaEnvelope {
    ReplicaEnvelope { term: Term(3), term_sequence: Some(TermSequence(5)), message }
}

/// What a reader that ignores the replica header byte, as XTDB's does, decodes from the protobuf that follows it.
fn read_as_xtdb(encoded: &[u8]) -> pb::ReplicaLogMessage {
    pb::ReplicaLogMessage::decode(&encoded[1..]).unwrap()
}

fn extension_replica_bodies() -> Vec<ReplicaBody> {
    vec![
        ReplicaBody::TablesRetired(pb::TablesRetired { table_names: vec!["rtdb_owned_v1/docs".into()], as_of_micros: Some(7) }),
        ReplicaBody::TriesAdded(tries_added("l01-rc-b00-e01")),
        ReplicaBody::TriesDeleted(pb::TriesDeleted { table_name: Some("public/docs".into()), trie_keys: vec!["l01-rc-b00-e01".into()] }),
        ReplicaBody::TriesDeleted(pb::TriesDeleted { table_name: Some("xt/idempotency_receipts".into()), trie_keys: vec!["l00-rc-b02".into()] }),
        ReplicaBody::BlockUploaded(pb::BlockUploaded {
            storage_version: Some(6), storage_epoch: Some(0), block_index: Some(2), latest_processed_msg_id: Some(9),
            naming_epoch: Some(1), ..Default::default()
        }),
    ]
}

#[test]
fn records_that_use_no_rtdb_extension_keep_xtdb_framing_byte_for_byte() {
    let plain = [
        ReplicaBody::ResolvedTx(pb::ResolvedTx { tx_id: Some(1), system_time_micros: Some(0), committed: Some(true), src_msg_id: Some(1), ..Default::default() }),
        ReplicaBody::TriesAdded(tries_added("l01-rc-b00")),
        ReplicaBody::TriesDeleted(pb::TriesDeleted { table_name: Some("public/docs".into()), trie_keys: vec!["l01-rc-b00".into()] }),
        ReplicaBody::BlockUploaded(pb::BlockUploaded {
            storage_version: Some(6), storage_epoch: Some(0), block_index: Some(2), latest_processed_msg_id: Some(9),
            tries: vec![trie("l00-rc-b02")], ..Default::default()
        }),
        ReplicaBody::ArtefactsAdded(pb::ArtefactsAdded { storage_version: Some(6), storage_epoch: Some(0), table_name: Some("public/docs".into()), ..Default::default() }),
    ];
    for message in plain {
        assert!(!replica_uses_extension(&message));
        let stamped = envelope(message);
        let mut expected = vec![PROTOBUF_LOG_HEADER];
        pb::ReplicaLogMessage { term_id: Some(3), term_seq: Some(5), message: Some(stamped.message.clone()) }.encode(&mut expected).unwrap();
        assert_eq!(stamped.encode(), expected);
        assert_eq!(stamped.encoded_len(), expected.len());
    }

    for body in [
        SourceBody::Tx(pb::Tx { tx_ops: Some(vec![1, 2].into()), default_tz: Some("UTC".into()), placed_attempt_seed: Some(vec![0; 8].into()), ..Default::default() }),
        SourceBody::TriesAdded(tries_added("l01-rc-b00")),
        SourceBody::ArtefactsAdded(pb::ArtefactsAdded::default()),
    ] {
        assert!(!source_uses_extension(&body));
        assert_eq!(SourceMessage::Protobuf(body).encode().unwrap()[0], PROTOBUF_LOG_HEADER);
    }
}

#[test]
fn an_extension_replica_record_reads_as_a_foreign_storage_version_to_xtdb_and_as_itself_to_rtdb() {
    for message in extension_replica_bodies() {
        assert!(replica_uses_extension(&message), "{message:?}");
        let stamped = envelope(message);
        let encoded = stamped.encode();
        assert_eq!(encoded[0], RTDB_EXTENSION_LOG_HEADER);
        assert!(encoded.ends_with(&XTDB_REFUSAL_SUFFIX));
        assert_eq!(stamped.encoded_len(), encoded.len());

        let mut body = Vec::new();
        ReplicaEnvelope::encode_body(stamped.term, stamped.term_sequence, &stamped.message, &mut body);
        assert_eq!(body, encoded);

        let xtdb = read_as_xtdb(&encoded);
        let Some(ReplicaBody::OversizedMessage(oversized)) = xtdb.message else { panic!("XTDB must read an oversized message") };
        assert_eq!(oversized.storage_version, Some(RTDB_EXTENSION_STORAGE_VERSION));
        assert_eq!((xtdb.term_id, xtdb.term_seq), (Some(3), Some(5)));

        assert_eq!(ReplicaEnvelope::decode(&encoded).unwrap(), Some(stamped.clone()));
        assert_eq!(ReplicaFrame::decode_bytes(Bytes::from(encoded.clone())).unwrap().into_envelope(), Some(stamped.clone()));
        assert!(!ReplicaFrame::has_external_payload(&encoded).unwrap());
    }
}

#[test]
fn the_refusal_suffix_names_the_rtdb_storage_version() {
    let suffix = pb::ReplicaLogMessage::decode(XTDB_REFUSAL_SUFFIX.as_slice()).unwrap();
    assert_eq!(suffix, pb::ReplicaLogMessage {
        message: Some(ReplicaBody::OversizedMessage(pb::OversizedMessage {
            storage_version: Some(RTDB_EXTENSION_STORAGE_VERSION), ..Default::default()
        })),
        ..Default::default()
    });
    assert_eq!(RTDB_EXTENSION_STORAGE_VERSION.to_be_bytes(), *b"RTDB");
}

#[test]
fn replica_records_framed_before_the_extension_header_still_decode() {
    for message in extension_replica_bodies() {
        let stamped = envelope(message);
        let mut legacy = vec![PROTOBUF_LOG_HEADER];
        pb::ReplicaLogMessage { term_id: Some(3), term_seq: Some(5), message: Some(stamped.message.clone()) }.encode(&mut legacy).unwrap();
        assert_eq!(ReplicaEnvelope::decode(&legacy).unwrap(), Some(stamped));
    }
}

#[test]
fn an_extension_replica_header_without_its_refusal_suffix_is_rejected() {
    let encoded = envelope(extension_replica_bodies().remove(0)).encode();
    let truncated = &encoded[..encoded.len() - 1];
    assert!(ReplicaEnvelope::decode(truncated).is_err());
    assert!(ReplicaFrame::decode_bytes(Bytes::copy_from_slice(truncated)).is_err());
    assert!(ReplicaFrame::has_external_payload(truncated).is_err());
    assert!(ReplicaEnvelope::decode(&[RTDB_EXTENSION_LOG_HEADER]).is_err());
}

#[test]
fn an_offloaded_extension_record_unwraps_from_its_proxy() {
    let payload = envelope(extension_replica_bodies().remove(0));
    let bytes = payload.encode();
    let proxy = payload.offloaded(StorageVersion(6), StorageEpoch(0), BlockIndex(1), "retired".into(), &bytes);
    assert!(!replica_uses_extension(&proxy.message));
    assert_eq!(proxy.encode()[0], PROTOBUF_LOG_HEADER);
    assert_eq!(proxy.unwrap_payload(&bytes).unwrap(), payload);
    assert_eq!(proxy.unwrap_payload_bytes(Bytes::from(bytes)).unwrap(), payload);
}

#[test]
fn an_extension_source_record_takes_a_header_xtdb_rejects() {
    for body in [
        SourceBody::ServingTransact(vec![9; 16].into()),
        SourceBody::Tx(pb::Tx {
            tx_ops: Some(vec![1].into()), default_tz: Some("UTC".into()),
            idempotency_guard: Some(pb::IdempotencyGuard { submitted_at_micros: Some(1), after_bootstrap: Some(false) }), ..Default::default()
        }),
        SourceBody::Tx(pb::Tx {
            tx_ops: Some(vec![1].into()), default_tz: Some("UTC".into()),
            bounded_retry: Some(pb::BoundedRetry { nonce: Some(vec![b'0'; 32]), not_after_micros: Some(2) }), ..Default::default()
        }),
        SourceBody::TriesAdded(tries_added("l01-rc-b00-e01")),
    ] {
        assert!(source_uses_extension(&body), "{body:?}");
        let message = SourceMessage::Protobuf(body.clone());
        let encoded = message.encode().unwrap();
        assert_eq!(encoded[0], RTDB_EXTENSION_LOG_HEADER);
        assert!(![0xff, 2, 3].contains(&encoded[0]), "XTDB's source decoder accepts only these headers");
        assert_eq!(encoded.len(), message.encoded_len());

        let mut plain = vec![PROTOBUF_LOG_HEADER];
        pb::SourceLogMessage { message: Some(body) }.encode(&mut plain).unwrap();
        assert_eq!(encoded[1..], plain[1..]);

        assert_eq!(SourceMessage::decode(&encoded).unwrap(), Some(message.clone()));
        assert_eq!(SourceMessage::decode_bytes(Bytes::from(encoded.clone())).unwrap(), Some(message.clone()));
        assert_eq!(SourceMessage::decode(&plain).unwrap(), Some(message.clone()));
        assert_eq!(
            SourceMessage::is_transaction_encoded(&encoded).unwrap(),
            matches!(message, SourceMessage::Protobuf(SourceBody::ServingTransact(_) | SourceBody::Tx(_))),
        );
    }
}
