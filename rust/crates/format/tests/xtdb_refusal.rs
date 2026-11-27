//! Behavioural spec: dev/doc/db.allium (XtdbRefusesRtdbExtensionRecords).
use std::collections::{BTreeMap, BTreeSet};

use prost::{Message, bytes::Bytes};
use rtdb_format::block::proto as block_pb;
use rtdb_format::catalog::{BlockCatalog, TableBlock};
use rtdb_format::envelope::{
    ReplicaBody, ReplicaEnvelope, ReplicaFrame, SourceBody, SourceMessage, replica_uses_extension, source_uses_extension,
};
use rtdb_format::generated::xtdb::log::proto as pb;
use rtdb_format::{PROTOBUF_LOG_HEADER, RTDB_EXTENSION_LOG_HEADER, XTDB_REFUSAL_PREFIX};
use rtdb_types::{BlockIndex, StorageEpoch, StorageVersion, Term, TermSequence};

const LOG_PROTO: &str = include_str!("../../../../core/src/main/proto/xtdb/log/proto/log.proto");
const REPLICA_PROTO: &str = include_str!("../../../../core/src/main/proto/xtdb/log/proto/replica-log.proto");
const BLOCK_PROTO: &str = include_str!("../../../../core/src/main/proto/xtdb/meta/block/proto/block.proto");

/// The field numbers of each message the fork shares with XTDB, as upstream (0b9fa440f2) declares them.
const UPSTREAM: &[(&str, &str, &[u32])] = &[
    (LOG_PROTO, "SourceLogMessage", &[1, 2, 3, 4, 6]),
    (LOG_PROTO, "Tx", &[1, 2, 3, 4, 5, 6]),
    (LOG_PROTO, "BlockUploaded", &[1, 2, 3, 4, 5, 6]),
    (LOG_PROTO, "FlushBlock", &[2]),
    (LOG_PROTO, "TriesAdded", &[1, 2, 3, 4]),
    (LOG_PROTO, "TemporalMetadata", &[1, 2, 3, 4, 5, 6]),
    (LOG_PROTO, "TrieMetadata", &[1, 2, 8]),
    (LOG_PROTO, "TrieDetails", &[1, 2, 3, 4, 5, 6]),
    (LOG_PROTO, "AttachDatabase", &[1, 2]),
    (LOG_PROTO, "DetachDatabase", &[1]),
    (REPLICA_PROTO, "ReplicaLogMessage", &[1, 2, 3, 4, 5, 6, 7, 8, 9]),
    (REPLICA_PROTO, "TriesDeleted", &[1, 2]),
    (REPLICA_PROTO, "ResolvedTx", &[1, 2, 3, 4, 5, 6, 7, 8, 9]),
    (REPLICA_PROTO, "BlockBoundary", &[1, 2, 3]),
    (REPLICA_PROTO, "NoOp", &[1]),
    (REPLICA_PROTO, "OversizedMessage", &[1, 2, 3, 4]),
    (BLOCK_PROTO, "Partition", &[1, 2, 3, 4, 5]),
    (BLOCK_PROTO, "TableBlock", &[1, 2, 3, 4, 5, 6]),
    (BLOCK_PROTO, "TxKey", &[1, 2]),
    (BLOCK_PROTO, "TableEntry", &[1, 2, 3, 4]),
    (BLOCK_PROTO, "Block", &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fork {
    /// Setting the field frames the record or file for XTDB to refuse.
    Framed,
    /// XTDB loses nothing by ignoring the field, so a record or file carrying only it stays readable by XTDB.
    Annotation(&'static str),
}

/// Every field the fork adds to a message it shares with XTDB.
const FORK_FIELDS: &[(&str, u32, Fork)] = &[
    ("SourceLogMessage", 7, Fork::Framed),
    ("SourceLogMessage", 8, Fork::Framed),
    ("SourceLogMessage", 9, Fork::Framed),
    ("Tx", 7, Fork::Framed),
    ("Tx", 8, Fork::Framed),
    ("Tx", 9, Fork::Framed),
    ("Tx", 10, Fork::Framed),
    ("BlockUploaded", 7, Fork::Annotation("the digest of the block file, which XTDB reads by its key")),
    ("BlockUploaded", 1000, Fork::Framed),
    ("TrieDetails", 7, Fork::Framed),
    ("TrieDetails", 8, Fork::Annotation("the object versions of immutable files, which XTDB reads by their keys")),
    ("ReplicaLogMessage", 10, Fork::Framed),
    ("ReplicaLogMessage", 11, Fork::Framed),
    ("ReplicaLogMessage", 12, Fork::Framed),
    ("ResolvedTx", 10, Fork::Framed),
    ("ResolvedTx", 11, Fork::Framed),
    ("ResolvedTx", 12, Fork::Framed),
    ("ResolvedTx", 13, Fork::Framed),
    ("NoOp", 2, Fork::Framed),
    ("NoOp", 3, Fork::Framed),
    ("NoOp", 4, Fork::Framed),
    ("OversizedMessage", 5, Fork::Annotation("the digest of the payload, which XTDB fetches by its key")),
    ("OversizedMessage", 6, Fork::Framed),
    ("TableBlock", 7, Fork::Framed),
    ("TableBlock", 8, Fork::Framed),
    ("TableBlock", 9, Fork::Framed),
    ("TableBlock", 12, Fork::Framed),
    ("TableBlock", 13, Fork::Framed),
    ("TableBlock", 14, Fork::Framed),
    ("TableBlock", 15, Fork::Framed),
    ("Block", 11, Fork::Framed),
    ("Block", 12, Fork::Annotation("the digests of the table block files, which XTDB reads by their keys")),
    ("Block", 13, Fork::Framed),
    ("Block", 14, Fork::Framed),
];

/// The field numbers a `.proto` file declares for `message`, oneof members included.
fn declared_fields(proto: &str, message: &str) -> BTreeSet<u32> {
    let opening = format!("message {message} {{");
    let start = proto.find(&opening).unwrap_or_else(|| panic!("{message} is not declared")) + opening.len();
    let mut depth = 1;
    let end = start + proto[start..].char_indices().find(|&(_, c)| {
        match c { '{' => depth += 1, '}' => depth -= 1, _ => {} }
        depth == 0
    }).unwrap().0;
    proto[start..end].lines().map(str::trim)
        .filter(|line| !line.starts_with("//") && !line.starts_with("reserved") && !line.starts_with("option") && line.ends_with(';'))
        .filter_map(|line| line.split('[').next()?.split_once('=').map(|(_, number)| number.trim().trim_end_matches(';').trim().parse::<u32>().unwrap()))
        .collect()
}

#[test]
fn every_field_the_fork_adds_to_a_shared_message_is_framed_or_a_named_annotation() {
    let mut classified = BTreeMap::<&str, BTreeSet<u32>>::new();
    for (message, field, _) in FORK_FIELDS { assert!(classified.entry(*message).or_default().insert(*field)); }
    for (proto, message, upstream) in UPSTREAM {
        let declared = declared_fields(proto, message);
        let upstream = upstream.iter().copied().collect::<BTreeSet<_>>();
        assert!(declared.is_superset(&upstream), "{message} lost an upstream field");
        let added = declared.difference(&upstream).copied().collect::<BTreeSet<_>>();
        assert_eq!(added, classified.remove(message).unwrap_or_default(), "{message}: classify each added field in FORK_FIELDS");
    }
    assert!(classified.is_empty(), "classified fields of unshared messages: {classified:?}");
}

fn framed_fields() -> BTreeSet<(&'static str, u32)> {
    FORK_FIELDS.iter().filter(|(_, _, fork)| *fork == Fork::Framed).map(|(message, field, _)| (*message, *field)).collect()
}

fn annotation_fields() -> BTreeSet<(&'static str, u32)> {
    FORK_FIELDS.iter().filter(|(_, _, fork)| matches!(fork, Fork::Annotation(_))).map(|(message, field, _)| (*message, *field)).collect()
}

fn trie(key: &str) -> pb::TrieDetails {
    pb::TrieDetails { table_name: Some("public/docs".into()), trie_key: Some(key.into()), data_file_size: Some(1), ..Default::default() }
}

fn with_artefact(mut trie: pb::TrieDetails) -> pb::TrieDetails {
    trie.artefacts = vec![pb::ArtefactRecord { kind: Some("entity-history".into()), ..Default::default() }];
    trie
}

fn tries_added(tries: Vec<pb::TrieDetails>) -> pb::TriesAdded {
    pb::TriesAdded { storage_version: Some(6), storage_epoch: Some(0), source_msg_id: Some(1), tries }
}

fn resolved(edit: impl FnOnce(&mut pb::ResolvedTx)) -> ReplicaBody {
    let mut tx = pb::ResolvedTx { tx_id: Some(1), system_time_micros: Some(0), committed: Some(true), src_msg_id: Some(1), ..Default::default() };
    edit(&mut tx);
    ReplicaBody::ResolvedTx(tx)
}

fn uploaded(edit: impl FnOnce(&mut pb::BlockUploaded)) -> ReplicaBody {
    let mut upload = pb::BlockUploaded {
        storage_version: Some(6), storage_epoch: Some(0), block_index: Some(2), latest_processed_msg_id: Some(9), ..Default::default()
    };
    edit(&mut upload);
    ReplicaBody::BlockUploaded(upload)
}

fn oversized(edit: impl FnOnce(&mut pb::OversizedMessage)) -> ReplicaBody {
    let mut message = pb::OversizedMessage {
        storage_version: Some(6), storage_epoch: Some(0), block_index: Some(1), payload_id: Some("payload".into()), ..Default::default()
    };
    edit(&mut message);
    ReplicaBody::OversizedMessage(message)
}

fn artefacts_added() -> pb::ArtefactsAdded {
    pb::ArtefactsAdded { storage_version: Some(6), storage_epoch: Some(0), table_name: Some("public/docs".into()), ..Default::default() }
}

fn envelope(message: ReplicaBody) -> ReplicaEnvelope {
    ReplicaEnvelope { term: Term(3), term_sequence: Some(TermSequence(5)), message }
}

/// Replica records that use an RTDB-only feature, each with the fork field it exercises, if any.
fn extension_replica_bodies() -> Vec<(Option<(&'static str, u32)>, ReplicaBody)> {
    vec![
        (Some(("ReplicaLogMessage", 10)), ReplicaBody::ArtefactsAdded(artefacts_added())),
        (Some(("ReplicaLogMessage", 11)), ReplicaBody::ArtefactsRemoved(pb::ArtefactsRemoved::default())),
        (Some(("ReplicaLogMessage", 12)), ReplicaBody::TablesRetired(pb::TablesRetired { table_names: vec!["rtdb_owned_v1/docs".into()], as_of_micros: Some(7) })),
        (Some(("ResolvedTx", 10)), resolved(|tx| tx.placed_attempt = Some(vec![1; 96].into()))),
        (Some(("ResolvedTx", 12)), resolved(|tx| tx.search_delta = Some(vec![1].into()))),
        (Some(("ResolvedTx", 13)), resolved(|tx| tx.placed_slot_terminal = Some(vec![1].into()))),
        (None, resolved(|tx| tx.table_data = [("xt/idempotency_receipts".to_owned(), vec![1].into())].into_iter().collect())),
        (Some(("NoOp", 2)), ReplicaBody::NoOp(pb::NoOp { placed_slot_reservation: Some(vec![1].into()), ..Default::default() })),
        (Some(("NoOp", 3)), ReplicaBody::NoOp(pb::NoOp { src_msg_id: Some(4), serving_record: Some(vec![1].into()), ..Default::default() })),
        (Some(("NoOp", 4)), ReplicaBody::NoOp(pb::NoOp { clock_lease_micros: Some(9), ..Default::default() })),
        (None, ReplicaBody::TriesAdded(tries_added(vec![trie("l01-rc-b00-e01")]))),
        (Some(("TrieDetails", 7)), ReplicaBody::TriesAdded(tries_added(vec![with_artefact(trie("l01-rc-b00"))]))),
        (None, ReplicaBody::TriesAdded(tries_added(vec![pb::TrieDetails { table_name: Some("xt/bounded_retry_receipts".into()), ..trie("l00-rc-b02") }]))),
        (None, ReplicaBody::TriesDeleted(pb::TriesDeleted { table_name: Some("public/docs".into()), trie_keys: vec!["l01-rc-b00-e01".into()] })),
        (None, ReplicaBody::TriesDeleted(pb::TriesDeleted { table_name: Some("public/docs".into()), trie_keys: vec!["l00-rc-b02".into()] })),
        (None, ReplicaBody::TriesDeleted(pb::TriesDeleted { table_name: Some("rtdb_owned_v1/docs".into()), trie_keys: vec!["l01-rc-b00".into()] })),
        (Some(("BlockUploaded", 1000)), uploaded(|upload| upload.naming_epoch = Some(1))),
        (None, uploaded(|upload| upload.tries = vec![with_artefact(trie("l00-rc-b02"))])),
        (Some(("OversizedMessage", 6)), oversized(|message| {
            message.storage_version = Some(-1);
            message.publication = Some(pb::ReplicaPayloadPublication { storage_version: Some(6), ..Default::default() });
        })),
    ]
}

/// Source records that use an RTDB-only feature, each with the fork field it exercises, if any.
fn extension_source_bodies() -> Vec<(Option<(&'static str, u32)>, SourceBody)> {
    let tx = || pb::Tx { tx_ops: Some(vec![1].into()), default_tz: Some("UTC".into()), ..Default::default() };
    vec![
        (Some(("SourceLogMessage", 7)), SourceBody::ArtefactsAdded(artefacts_added())),
        (Some(("SourceLogMessage", 8)), SourceBody::ArtefactsRemoved(pb::ArtefactsRemoved::default())),
        (Some(("SourceLogMessage", 9)), SourceBody::ServingTransact(vec![9; 16].into())),
        (Some(("Tx", 7)), SourceBody::Tx(pb::Tx { idempotency_guard: Some(pb::IdempotencyGuard { submitted_at_micros: Some(1), after_bootstrap: Some(false) }), ..tx() })),
        (Some(("Tx", 8)), SourceBody::Tx(pb::Tx { replay_submission: Some(pb::ReplaySubmission::default()), ..tx() })),
        (Some(("Tx", 9)), SourceBody::Tx(pb::Tx { placed_attempt_seed: Some(vec![0; 8].into()), ..tx() })),
        (Some(("Tx", 10)), SourceBody::Tx(pb::Tx { bounded_retry: Some(pb::BoundedRetry { nonce: Some(vec![b'0'; 32]), not_after_micros: Some(2) }), ..tx() })),
        (None, SourceBody::TriesAdded(tries_added(vec![trie("l01-rc-b00-e01")]))),
        (None, SourceBody::TriesAdded(tries_added(vec![with_artefact(trie("l01-rc-b00"))]))),
    ]
}

fn table_block(edit: impl FnOnce(TableBlock) -> TableBlock) -> Vec<u8> {
    edit(TableBlock::decode(&[]).unwrap()).encode()
}

fn block(edit: impl FnOnce(&mut block_pb::Block)) -> Vec<u8> {
    let mut wire = block_pb::Block { block_index: Some(1), ..Default::default() };
    edit(&mut wire);
    BlockCatalog::from_proto(wire).unwrap().encode()
}

/// Encoded block files that record an RTDB-only feature, each with the fork field it exercises, if any.
fn extension_block_files() -> Vec<(Option<(&'static str, u32)>, Vec<u8>)> {
    vec![
        (Some(("TableBlock", 7)), table_block(|table| table.with_search_runs(vec![pb::SearchRun::default()], vec![]))),
        (Some(("TableBlock", 8)), table_block(|table| table.with_search_runs(vec![], vec![pb::RetiredSearchRun::default()]))),
        (Some(("TableBlock", 9)), table_block(|table| table.with_retired_as_of(Some(7)))),
        (Some(("TableBlock", 12)), table_block(|table| table.with_term_directories(vec![pb::TermDirectoryRoot::default()]))),
        (Some(("TableBlock", 13)), table_block(|table| table.with_entity_directory(Some(pb::EntityDirectoryRoot::default()), vec![]))),
        (Some(("TableBlock", 14)), table_block(|table| table.with_entity_directory(None, vec![pb::EntityDirectoryRoot::default()]))),
        (Some(("TableBlock", 15)), table_block(|table| table.with_retired_term_directories(vec![pb::RetiredTermDirectory::default()]))),
        (None, TableBlock::decode(&block_pb::TableBlock { tries: vec![trie("l01-rc-b00-e01")], ..Default::default() }.encode_to_vec()).unwrap().encode()),
        (None, TableBlock::decode(&block_pb::TableBlock {
            partitions: vec![block_pb::Partition { tries: vec![with_artefact(trie("l01-rc-b00"))], ..Default::default() }], ..Default::default()
        }.encode_to_vec()).unwrap().encode()),
        (Some(("Block", 11)), block(|wire| wire.naming_epoch = Some(1))),
        (Some(("Block", 13)), block(|wire| wire.serving_checkpoint = Some(vec![1].into()))),
        (Some(("Block", 14)), block(|wire| wire.serving_checkpoint_object = Some("checkpoint".into()))),
        (None, block(|wire| wire.tables = vec![block_pb::TableEntry {
            oid: Some(16_384), schema_name: Some("xt".into()), table_name: Some("idempotency_receipts".into()), slug: Some("xt$idempotency_receipts".into()),
        }])),
    ]
}

#[test]
fn every_framed_fork_field_has_an_extension_sample() {
    let covered = extension_replica_bodies().into_iter().map(|(field, _)| field)
        .chain(extension_source_bodies().into_iter().map(|(field, _)| field))
        .chain(extension_block_files().into_iter().map(|(field, _)| field))
        .chain([Some(("ResolvedTx", 11))])
        .flatten().collect::<BTreeSet<_>>();
    assert_eq!(covered, framed_fields());
}

#[test]
fn a_resolved_transaction_with_an_acceptance_stage_is_framed() {
    // RTDB's decoder checks the stage against the rest of the record, so this sample stays out of the round trip.
    assert!(replica_uses_extension(&resolved(|tx| tx.acceptance_stage = Some(pb::AcceptanceStage::default()))));
}

#[test]
fn records_and_files_with_only_annotations_keep_xtdb_framing_byte_for_byte() {
    let published = pb::TrieFileVersions {
        data: Some(pb::PublishedObjectVersion { key: Some("tables/public$docs/data/l00-rc-b02.arrow".into()), version: Some("v1".into()), ..Default::default() }),
        metadata: None,
    };
    let replica = [
        (None, resolved(|_| ())),
        (None, ReplicaBody::TriesAdded(tries_added(vec![trie("l01-rc-b00")]))),
        (None, ReplicaBody::TriesDeleted(pb::TriesDeleted { table_name: Some("public/docs".into()), trie_keys: vec!["l01-rc-b00".into()] })),
        (Some(("BlockUploaded", 7)), uploaded(|upload| { upload.tries = vec![trie("l00-rc-b02")]; upload.catalog_sha256 = Some(vec![7; 32].into()); })),
        (Some(("TrieDetails", 8)), uploaded(|upload| upload.tries = vec![pb::TrieDetails { published_files: Some(published.clone()), ..trie("l00-rc-b02") }])),
        (None, uploaded(|upload| upload.naming_epoch = Some(0))),
        (Some(("OversizedMessage", 5)), oversized(|message| message.payload_sha256 = Some(vec![5; 32].into()))),
        (None, ReplicaBody::NoOp(pb::NoOp { src_msg_id: Some(4), ..Default::default() })),
    ];
    let mut covered = BTreeSet::new();
    for (field, message) in replica {
        covered.extend(field);
        assert!(!replica_uses_extension(&message), "{message:?}");
        let stamped = envelope(message);
        let mut expected = vec![PROTOBUF_LOG_HEADER];
        pb::ReplicaLogMessage { term_id: Some(3), term_seq: Some(5), message: Some(stamped.message.clone()) }.encode(&mut expected).unwrap();
        assert_eq!(stamped.encode(), expected);
        assert_eq!(stamped.encoded_len(), expected.len());
    }

    for body in [
        SourceBody::Tx(pb::Tx { tx_ops: Some(vec![1, 2].into()), default_tz: Some("UTC".into()), ..Default::default() }),
        SourceBody::TriesAdded(tries_added(vec![trie("l01-rc-b00")])),
        SourceBody::FlushBlock(pb::FlushBlock { expected_block_idx: Some(1) }),
    ] {
        assert!(!source_uses_extension(&body));
        assert_eq!(SourceMessage::Protobuf(body).encode().unwrap()[0], PROTOBUF_LOG_HEADER);
    }

    let manifest = block_pb::Block { block_index: Some(1), table_block_manifest: Some(block_pb::TableBlockManifest::default()), ..Default::default() };
    assert_eq!(BlockCatalog::from_proto(manifest.clone()).unwrap().encode(), manifest.encode_to_vec());
    covered.insert(("Block", 12));
    let plain = block_pb::TableBlock { row_count: Some(1), tries: vec![trie("l01-rc-b00")], ..Default::default() }.encode_to_vec();
    let table = TableBlock::decode(&plain).unwrap();
    assert_eq!(table.encode(), plain);
    assert_eq!(table.encoded_len(), plain.len());

    assert_eq!(covered, annotation_fields());
}

#[test]
fn an_extension_replica_record_fails_xtdbs_parser_and_reads_as_itself_to_rtdb() {
    for (_, message) in extension_replica_bodies() {
        assert!(replica_uses_extension(&message), "{message:?}");
        let stamped = envelope(message);
        let encoded = stamped.encode();
        assert_eq!(encoded[..2], [RTDB_EXTENSION_LOG_HEADER, XTDB_REFUSAL_PREFIX[0]]);
        assert_eq!(stamped.encoded_len(), encoded.len());

        let mut body = Vec::new();
        ReplicaEnvelope::encode_body(stamped.term, stamped.term_sequence, &stamped.message, &mut body);
        assert_eq!(body, encoded);

        // XTDB skips the header byte and parses the rest; a zero tag fails any protobuf parser.
        assert!(pb::ReplicaLogMessage::decode(&encoded[1..]).is_err());

        assert_eq!(ReplicaEnvelope::decode(&encoded).unwrap(), Some(stamped.clone()));
        assert_eq!(ReplicaFrame::decode_bytes(Bytes::from(encoded.clone())).unwrap().into_envelope(), Some(stamped.clone()));
        assert_eq!(ReplicaFrame::has_external_payload(&encoded).unwrap(), matches!(stamped.message, ReplicaBody::OversizedMessage(_)));
    }
}

#[test]
fn replica_records_framed_before_the_extension_header_still_decode() {
    for (_, message) in extension_replica_bodies() {
        let stamped = envelope(message);
        let mut legacy = vec![PROTOBUF_LOG_HEADER];
        pb::ReplicaLogMessage { term_id: Some(3), term_seq: Some(5), message: Some(stamped.message.clone()) }.encode(&mut legacy).unwrap();
        assert_eq!(ReplicaEnvelope::decode(&legacy).unwrap(), Some(stamped));
    }
}

#[test]
fn an_extension_replica_header_without_its_refusal_prefix_is_rejected() {
    let (_, message) = extension_replica_bodies().remove(2);
    let mut unprefixed = vec![RTDB_EXTENSION_LOG_HEADER];
    pb::ReplicaLogMessage { term_id: Some(3), term_seq: Some(5), message: Some(message) }.encode(&mut unprefixed).unwrap();
    assert!(ReplicaEnvelope::decode(&unprefixed).is_err());
    assert!(ReplicaFrame::decode_bytes(Bytes::copy_from_slice(&unprefixed)).is_err());
    assert!(ReplicaFrame::has_external_payload(&unprefixed).is_err());
    assert!(ReplicaEnvelope::decode(&[RTDB_EXTENSION_LOG_HEADER]).is_err());
    assert!(ReplicaFrame::decode_bytes(Bytes::from_static(&[RTDB_EXTENSION_LOG_HEADER])).is_err());
}

#[test]
fn an_offloaded_extension_record_unwraps_from_its_proxy() {
    let (_, message) = extension_replica_bodies().remove(2);
    let payload = envelope(message);
    let bytes = payload.encode();
    let proxy = payload.offloaded(StorageVersion(6), StorageEpoch(0), BlockIndex(1), "retired".into(), &bytes);
    assert!(!replica_uses_extension(&proxy.message));
    assert_eq!(proxy.encode()[0], PROTOBUF_LOG_HEADER);
    assert_eq!(proxy.unwrap_payload(&bytes).unwrap(), payload);
    assert_eq!(proxy.unwrap_payload_bytes(Bytes::from(bytes)).unwrap(), payload);
}

#[test]
fn an_extension_source_record_takes_a_header_xtdb_rejects() {
    for (_, body) in extension_source_bodies() {
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

#[test]
fn an_extension_block_file_fails_xtdbs_parser_and_reads_as_itself_to_rtdb() {
    for (field, encoded) in extension_block_files() {
        assert_eq!(encoded[0], XTDB_REFUSAL_PREFIX[0], "{field:?}");
        let table = field.is_none_or(|(message, _)| message == "TableBlock") && block_pb::TableBlock::decode(&encoded[1..]).is_ok()
            && TableBlock::decode(&encoded).is_ok_and(|table| table.encode() == encoded);
        let root = BlockCatalog::decode(&encoded).is_ok_and(|block| block.encode() == encoded);
        assert!(table || root, "{field:?}");
        assert!(block_pb::Block::decode(encoded.as_slice()).is_err());
        assert!(block_pb::TableBlock::decode(encoded.as_slice()).is_err());
    }
    let unframed = block_pb::TableBlock { retired_as_of_micros: Some(7), ..Default::default() }.encode_to_vec();
    assert_eq!(TableBlock::decode(&unframed).unwrap().as_proto().retired_as_of_micros, Some(7));
    assert_eq!(TableBlock::decode(&unframed).unwrap().encoded_len(), 1 + unframed.len());
}

/// The same bytes are pinned against the Kotlin decoders in `core/src/test/kotlin/xtdb/api/log/RtdbExtensionRefusalTest.kt`.
#[test]
fn xtdb_reads_these_exact_bytes() {
    let retired = envelope(ReplicaBody::TablesRetired(pb::TablesRetired { table_names: vec!["a/b".into()], as_of_micros: Some(7) }));
    assert_eq!(retired.encode(), [0x04, 0x00, 0x62, 0x07, 0x0a, 0x03, b'a', b'/', b'b', 0x10, 0x07, 0x38, 0x03, 0x48, 0x05]);
    assert_eq!(SourceMessage::Protobuf(SourceBody::ServingTransact(vec![9, 9].into())).encode().unwrap(), [0x04, 0x4a, 0x02, 0x09, 0x09]);
    assert_eq!(block(|wire| wire.naming_epoch = Some(1)), [0x00, 0x08, 0x01, 0x58, 0x01]);
    assert_eq!(TableBlock::decode(&[0x10, 0x01]).unwrap().with_retired_as_of(Some(7)).encode(), [0x00, 0x10, 0x01, 0x48, 0x07]);
}
