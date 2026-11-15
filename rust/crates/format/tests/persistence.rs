use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::Array;
use prost::Message;
use rtdb_format::{basis, columns, data::{EventBatch, StoredEvent, StoredOperation, build_event_batch}, envelope::SourceMessage, ipc, keys::*};
use rtdb_format::catalog::{BlockCatalog, FIRST_TABLE_OID, TableBlock};
use rtdb_format::generated::xtdb::{block::proto::Block, log::proto as pb};
use rtdb_types::{BlockIndex, EventOp, Iid, Instant, Interval, ListKind, LogicalType, RunEndWidth, ScalarType, StorageEpoch, StorageVersion, TableRef, TimeBasis, Timestamp, TxBasis, TxId, Value};

#[test]
fn partition_roots_preserve_the_single_partition_namespace() {
    assert_eq!(storage_root(StorageVersion(6), StorageEpoch(0), 0, 1).unwrap(), "v06");
    assert_eq!(storage_root(StorageVersion(6), StorageEpoch(16), 0, 1).unwrap(), "v06_e110");
    assert_eq!(storage_root(StorageVersion(6), StorageEpoch(0), 2, 4).unwrap(), "parts/2/v06");
    assert!(storage_root(StorageVersion(6), StorageEpoch(0), 2, 2).is_err());
    assert!(storage_root(StorageVersion(6), StorageEpoch(0), 0, 0).is_err());
}

#[test]
fn lexicographic_hex_preserves_numeric_order_and_width_boundaries() {
    let values = [0, 1, 15, 16, 255, 256, 4095, 4096, i64::MAX as u64];
    let encoded: Vec<_> = values.into_iter().map(lex_hex).collect();
    assert_eq!(encoded[..6], ["00", "01", "0f", "110", "1ff", "2100"]);
    assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
    for (value, encoded) in values.into_iter().zip(encoded) { assert_eq!(parse_lex_hex(&encoded).unwrap(), value); }
    for bad in ["", "0", "000", "1f", "100", "g0"] { assert!(parse_lex_hex(bad).is_err(), "{bad}"); }
}

#[test]
fn trie_keys_and_recorded_slugs_retain_existing_object_paths() {
    let table = TableRef::new("public", "a.b/c");
    let slug = TableSlug::for_unrecorded_table(&table).unwrap();
    assert_eq!(slug.as_str(), "public$a$b$c");
    let key: TrieKey = "l02-r20260501-p03-b110".parse().unwrap();
    assert_eq!(key.level(), 2);
    assert_eq!(key.part(), [0, 3]);
    assert_eq!(key.block_index(), BlockIndex(16));
    assert_eq!(key.to_string(), "l02-r20260501-p03-b110");
    assert_eq!(slug.data_key(&key), "tables/public$a$b$c/data/l02-r20260501-p03-b110.arrow");
    assert_eq!(slug.block_key(BlockIndex(16)).unwrap(), "tables/public$a$b$c/blocks/b110.binpb");
    assert_eq!(oversized_block_index("oversized/b110-a-b.binpb"), Some(BlockIndex(16)));
    assert!(TableSlug::recorded("../other").is_err());
}

#[test]
fn old_source_headers_and_empty_flush_requests_remain_ignored() {
    assert_eq!(SourceMessage::decode(&[2]).unwrap(), None);
    let empty_flush = pb::SourceLogMessage { message: Some(pb::source_log_message::Message::FlushBlock(pb::FlushBlock { expected_block_idx: None })) };
    let mut bytes = vec![3];
    bytes.extend(empty_flush.encode_to_vec());
    assert_eq!(SourceMessage::decode(&bytes).unwrap(), None);
    let legacy = vec![0xff, 0xff, 0xff, 0xff, 1, 2, 3];
    assert_eq!(SourceMessage::decode(&legacy).unwrap().unwrap().encode().unwrap(), legacy);
    assert!(SourceMessage::decode(&[]).is_err());
}

#[test]
fn legacy_block_names_allocate_oids_in_name_order() {
    let block = Block { block_index: Some(1), table_names: vec!["public/z".into(), "public/a".into()], ..Default::default() };
    let catalog = BlockCatalog::decode(&block.encode_to_vec()).unwrap();
    let entries = catalog.table_entries().unwrap();
    assert_eq!(entries[0].oid, FIRST_TABLE_OID);
    assert_eq!(entries[0].table, TableRef::new("public", "a"));
    assert_eq!(entries[0].slug.as_str(), "public$a");
    assert_eq!(entries[1].oid, FIRST_TABLE_OID + 1);
}

#[test]
fn basis_tokens_keep_partition_order_and_nanoseconds() {
    let tx = TxBasis { databases: BTreeMap::from([("xtdb".into(), vec![TxId(0), TxId(42)])]) };
    assert_eq!(basis::decode_tx_basis(&basis::encode_tx_basis(&tx)).unwrap(), tx);
    let time = TimeBasis { databases: BTreeMap::from([("xtdb".into(), vec![None, Some(Instant::new(-1, 999_999_999).unwrap()), Some(Instant::new(1, 1).unwrap())])]) };
    assert_eq!(basis::decode_time_basis(&basis::encode_time_basis(&time)).unwrap(), time);
    let epoch = TimeBasis { databases: BTreeMap::from([("xtdb".into(), vec![Some(Instant::new(0, 0).unwrap())])]) };
    assert_eq!(basis::decode_time_basis(&basis::encode_time_basis(&epoch)).unwrap().databases["xtdb"], [None]);
    assert!(basis::decode_tx_basis("invalid%%").is_err());
}

#[test]
fn nested_column_types_keep_bottom_null_and_missing_distinct() {
    let ty = LogicalType::Struct(BTreeMap::from([
        ("bottom".into(), LogicalType::Nothing),
        ("nullable".into(), LogicalType::Scalar(ScalarType::I64).maybe()),
        ("null".into(), LogicalType::Null),
        ("mixed".into(), LogicalType::from_legs([LogicalType::Scalar(ScalarType::I64), LogicalType::Scalar(ScalarType::Utf8)])),
        ("set".into(), LogicalType::List { kind: ListKind::Set, element: Box::new(LogicalType::Scalar(ScalarType::Keyword)) }),
    ]));
    let encoded = columns::column_from_type(&ty, Some(vec![1, 2, 3])).unwrap();
    assert_eq!(columns::logical_type(&encoded).unwrap(), ty);
    assert_eq!(encoded.hll.as_deref(), Some([1, 2, 3].as_slice()));
    let block = TableBlock::from_parts(BTreeMap::from([("doc".into(), encoded)]), 3, Vec::new()).unwrap();
    let decoded = TableBlock::decode(&block.encode()).unwrap();
    assert_eq!(decoded.column_types().unwrap()["doc"], ty);
    assert_eq!(decoded.column_hlls()["doc"], [1, 2, 3]);
}

fn events() -> Vec<StoredEvent> {
    vec![
        StoredEvent { iid: Iid([1; 16]), system_from: Timestamp(3), valid_time: Interval::ALL, operation: StoredOperation::Erase, recency: None },
        StoredEvent { iid: Iid([1; 16]), system_from: Timestamp(2), valid_time: Interval::from_micros(10, 20).unwrap(), operation: StoredOperation::Delete, recency: None },
        StoredEvent { iid: Iid([1; 16]), system_from: Timestamp(1), valid_time: Interval::from_micros(0, 100).unwrap(), operation: StoredOperation::Put(BTreeMap::from([("price".into(), Value::I64(12))])), recency: None },
        StoredEvent { iid: Iid([2; 16]), system_from: Timestamp(1), valid_time: Interval::from_micros(0, 100).unwrap(), operation: StoredOperation::Put(BTreeMap::from([("price".into(), Value::I64(7))])), recency: None },
    ]
}

#[test]
fn every_iid_run_row_survives_arrow_file_and_stream_roundtrips() {
    let events = events();
    for encoding in [None, Some(RunEndWidth::I16), Some(RunEndWidth::I32), Some(RunEndWidth::I64)] {
        let batch = build_event_batch(&events, Some(BTreeMap::from([("price".into(), LogicalType::Scalar(ScalarType::I64))])), encoding).unwrap();
        let bytes = ipc::write_file(batch.schema().as_ref(), &[batch.clone()]).unwrap();
        let mut file = ipc::ArrowFile::open(bytes.into(), None).unwrap();
        let decoded = EventBatch::new(file.read_page(0).unwrap()).unwrap();
        assert_eq!(decoded.header(0).unwrap().op, EventOp::Erase);
        assert_eq!(decoded.header(1).unwrap().op, EventOp::Delete);
        for (row, event) in events.iter().enumerate() { assert_eq!(&decoded.read_event(row).unwrap(), event); }
        let bytes = ipc::write_stream(batch.schema().as_ref(), &[batch]).unwrap();
        assert_eq!(&bytes[bytes.len() - 4..], &[0, 0, 0, 0]);
        let (_, pages) = ipc::read_stream(Arc::from(bytes)).unwrap();
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].num_rows(), 4);
        assert_eq!(pages[0].column(0).len(), 4);
    }
}

#[test]
fn committed_jvm_data_files_are_read_through_the_event_schema() {
    let bytes = include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../../src/test/resources/xtdb/compactor-reset-node/objects/v06/tables/public$foo/data/l00-rc-b00.arrow"));
    let mut file = ipc::ArrowFile::open(Arc::from(bytes.as_slice()), None).unwrap();
    assert!(file.page_count() > 0);
    for page in 0..file.page_count() {
        let batch = EventBatch::new(file.read_page(page).unwrap()).unwrap();
        for row in 0..batch.len() { batch.read_event(row).unwrap(); }
    }
}
