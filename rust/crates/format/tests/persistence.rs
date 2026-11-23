use rtdb_types::StructFields;
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
    assert_eq!(rtdb_format::keys::StorageRoot::new(StorageVersion(6), StorageEpoch(16)).unwrap().key(), "v06_e110");
    assert_eq!(rtdb_format::keys::StorageRoot::under_prefix(Some("tenants/acme"), "v06"), "tenants/acme/v06");
    assert_eq!(rtdb_format::keys::StorageRoot::under_prefix(Some(""), "v06"), "v06", "an empty prefix is no prefix");
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
fn a_tries_artefacts_live_under_one_prefix_of_its_own() {
    let slug = TableSlug::recorded("public$docs").unwrap();
    let key: TrieKey = "l01-rc-b03".parse().unwrap();
    let artefact = slug.artefact_key(&key, "search", "docs_body", 2).unwrap();
    assert_eq!(artefact, "tables/public$docs/artefacts/l01-rc-b03/search/docs_body-v2.bin");
    assert!(artefact.starts_with(&format!("{}/", slug.artefact_prefix(&key))));
    let other: TrieKey = "l02-rc-p0-b03".parse().unwrap();
    assert!(!slug.artefact_key(&other, "search", "docs_body", 2).unwrap().starts_with(&format!("{}/", slug.artefact_prefix(&key))));
    for bad in ["", ".", "..", "a/b", "a\\b"] {
        assert!(slug.artefact_key(&key, bad, "docs_body", 2).is_err(), "{bad:?}");
        assert!(slug.artefact_key(&key, "search", bad, 2).is_err(), "{bad:?}");
    }
}

#[test]
fn an_artefact_key_names_the_trie_kind_definition_and_version_it_was_made_from() {
    let slug = TableSlug::recorded("public$docs").unwrap();
    let key: TrieKey = "l01-rc-b03".parse().unwrap();
    for (id, version) in [("docs_body~a1f3", 2), ("with-v-inside", 7), ("docs", -1)] {
        let artefact = slug.artefact_key(&key, "search", id, version).unwrap();
        assert!(artefact.starts_with(&format!("{}/", slug.artefacts_prefix())));
        assert_eq!(slug.parse_artefact_key(&artefact), Some((ArtefactOwner::Trie(key.clone()), "search".into(), id.into(), version)), "{artefact}");
    }
    let group = "l03-rc-p21-b07".parse::<TrieKey>().unwrap().part_group().unwrap();
    let artefact = slug.owner_artefact_key(&ArtefactOwner::Group(group.clone()), "search", "docs", 3).unwrap();
    assert_eq!(artefact, "tables/public$docs/artefacts/g03-rc-p2-b07/search/docs-v3.bin");
    assert_eq!(slug.parse_artefact_key(&artefact), Some((ArtefactOwner::Group(group), "search".into(), "docs".into(), 3)));
    for stray in ["tables/public$docs/artefacts/l01-rc-b03/search/docs-v2.tmp", "tables/public$docs/artefacts/l01-rc-b03/docs-v2.bin",
        "tables/public$docs/artefacts/l01-rc-b03/search/x/docs-v2.bin", "tables/public$other/artefacts/l01-rc-b03/search/docs-v2.bin",
        "tables/public$docs/artefacts/not-a-trie/search/docs-v2.bin", "tables/public$docs/artefacts/l01-rc-b03/search/docs-v02.bin",
        "tables/public$docs/artefacts/g01-rc-b03/search/docs-v2.bin", "tables/public$docs/artefacts/g02-r20200106-b03/search/docs-v2.bin"] {
        assert_eq!(slug.parse_artefact_key(stray), None, "{stray}");
    }
}

#[test]
fn a_tiered_trie_belongs_to_the_partition_group_of_its_siblings() {
    let group = |key: &str| key.parse::<TrieKey>().unwrap().part_group();
    let l2c = group("l02-rc-p2-b05").unwrap();
    assert_eq!(l2c.to_string(), "g02-rc-b05");
    assert_eq!(l2c.members().iter().map(ToString::to_string).collect::<Vec<_>>(), ["l02-rc-p0-b05", "l02-rc-p1-b05", "l02-rc-p2-b05", "l02-rc-p3-b05"]);
    assert_eq!(l2c.to_string().parse::<PartGroup>().unwrap(), l2c);
    for member in l2c.members() { assert_eq!(member.part_group().as_ref(), Some(&l2c)); }
    let l3h = group("l03-r20200106-p1-b12a").unwrap();
    assert_eq!(l3h.to_string(), "g03-r20200106-b12a");
    assert_eq!(l3h.to_string().parse::<PartGroup>().unwrap(), l3h);
    assert_eq!(group("l04-rc-p013-b01").unwrap().members()[3].to_string(), "l04-rc-p013-b01");
    for untiered in ["l00-rc-b00", "l01-rc-b03", "l01-r20200106-b03", "l02-r20200106-b03"] { assert_eq!(group(untiered), None, "{untiered}"); }
    for bad in ["g00-rc-b00", "g01-rc-b03", "g02-r20200106-b03", "l02-rc-b05", "g02-rc-p-b05"] { assert!(bad.parse::<PartGroup>().is_err(), "{bad}"); }
}

#[test]
fn a_record_names_its_tries_object_or_its_groups() {
    let slug = TableSlug::recorded("public$docs").unwrap();
    let record = |scope: pb::ArtefactScope| pb::ArtefactRecord { kind: Some("search".into()), definition_id: Some("docs".into()), definition_version: Some(1), stats: None, scope: Some(scope as i32), file_size: None, published_version: None, pack_members: Vec::new() };
    let member: TrieKey = "l02-rc-p1-b05".parse().unwrap();
    assert_eq!(slug.recorded_artefact_key(&member, &record(pb::ArtefactScope::Trie)).unwrap(), "tables/public$docs/artefacts/l02-rc-p1-b05/search/docs-v1.bin");
    assert_eq!(slug.recorded_artefact_key(&member, &record(pb::ArtefactScope::PartGroup)).unwrap(), "tables/public$docs/artefacts/g02-rc-b05/search/docs-v1.bin");
    assert_eq!(slug.recorded_artefact_key(&member, &pb::ArtefactRecord { scope: None, ..record(pb::ArtefactScope::Trie) }).unwrap(), "tables/public$docs/artefacts/l02-rc-p1-b05/search/docs-v1.bin", "a record without a scope is its trie's");
    assert!(slug.recorded_artefact_key(&"l01-rc-b05".parse().unwrap(), &record(pb::ArtefactScope::PartGroup)).is_err(), "a trie outside every group has no group object");
}

#[test]
fn trie_recency_keys_preserve_the_nonnegative_java_calendar_range() {
    use rtdb_types::CivilDate;
    for (date, text) in [
        (CivilDate::from_ymd(0, 1, 1).unwrap(), "l01-r00000101-b00"),
        (CivilDate::from_ymd(280_000, 2, 29).unwrap(), "l01-r2800000229-b00"),
        (CivilDate::MAX, "l01-r9999999991231-b00"),
    ] {
        let key = TrieKey::new(1, Some(date), vec![], BlockIndex(0)).unwrap();
        assert_eq!(key.to_string(), text);
        assert_eq!(text.parse::<TrieKey>().unwrap(), key);
        assert_eq!(serde_json::to_value(&key).unwrap()["recency"], serde_json::Value::String(date.to_string()));
    }
    assert!(TrieKey::new(1, Some(CivilDate::MIN), vec![], BlockIndex(0)).is_err());
    for key in ["l01-r10000000000101-b00", "l01-r-00010101-b00"] {
        assert!(key.parse::<TrieKey>().is_err(), "{key}");
    }
}

#[test]
fn trie_recency_parser_keeps_lenient_year_width_and_smart_month_ends() {
    use rtdb_types::CivilDate;
    for (encoded, year) in [("0", 0), ("1", 1), ("01", 1), ("001", 1), ("0001", 1), ("00001", 1), ("280000", 280_000), ("999999999", 999_999_999)] {
        for sign in ["", "+"] {
            let key: TrieKey = format!("l01-r{sign}{encoded}0101-b00").parse().unwrap();
            assert_eq!(key.recency(), Some(CivilDate::from_ymd(year, 1, 1).unwrap()));
            assert_eq!(key.to_string(), format!("l01-r{year:04}0101-b00"));
        }
    }
    for (input, normalized) in [("20200230", "20200229"), ("20210229", "20210228"), ("19000229", "19000228"),
        ("20000231", "20000229"), ("20200431", "20200430"), ("2800000231", "2800000229")] {
        let key: TrieKey = format!("l01-r{input}-b00").parse().unwrap();
        assert_eq!(key.to_string(), format!("l01-r{normalized}-b00"));
    }
    for recency in ["0101", "00000000010101", "20200001", "20201301", "20200100", "20200132", "20200299", "2020+101", "202001+1", "++20200101", "-20200101", "+", " 20200101", "20200101 ", "2020a101"] {
        assert!(format!("l01-r{recency}-b00").parse::<TrieKey>().is_err(), "{recency}");
    }
}

#[test]
fn old_source_headers_and_empty_flush_requests_remain_ignored() {
    assert_eq!(SourceMessage::decode(&[2]).unwrap(), None);
    let empty_flush = pb::SourceLogMessage { message: Some(pb::source_log_message::Message::FlushBlock(pb::FlushBlock { expected_block_idx: None })) };
    let mut bytes = vec![3];
    bytes.extend(empty_flush.encode_to_vec());
    assert_eq!(SourceMessage::decode(&bytes).unwrap(), None);
    assert_eq!(SourceMessage::decode_wire(&bytes).unwrap(), empty_flush.message.map(SourceMessage::Protobuf));
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
    let ty = LogicalType::Struct(StructFields::from([
        ("bottom".into(), LogicalType::Nothing),
        ("nullable".into(), LogicalType::Scalar(ScalarType::I64).maybe()),
        ("null".into(), LogicalType::Null),
        ("mixed".into(), LogicalType::from_legs([LogicalType::Scalar(ScalarType::I64), LogicalType::Scalar(ScalarType::Utf8)])),
        ("set".into(), LogicalType::List { kind: ListKind::Set, element: Box::new(LogicalType::Scalar(ScalarType::Keyword)) }),
    ]));
    let encoded = columns::column_from_type(&ty, Some(vec![1, 2, 3])).unwrap();
    assert_eq!(columns::logical_type(&encoded).unwrap(), ty);
    assert_eq!(encoded.hll.as_deref(), Some([1, 2, 3].as_slice()));
    let block = TableBlock::from_parts(BTreeMap::from([("doc".into(), encoded)]), &Default::default(), 3, Vec::new()).unwrap();
    let decoded = TableBlock::decode(&block.encode()).unwrap();
    assert_eq!(decoded.column_types().unwrap()["doc"], ty);
    assert_eq!(decoded.column_hlls()["doc"], [1, 2, 3]);
}

fn events() -> Vec<StoredEvent> {
    vec![
        StoredEvent { origin: None, commit_position: 0, entity_key: None, iid: Iid([1; 16]), system_from: Timestamp(3), valid_time: Interval::ALL, operation: StoredOperation::Erase, recency: None },
        StoredEvent { origin: None, commit_position: 0, entity_key: None, iid: Iid([1; 16]), system_from: Timestamp(2), valid_time: Interval::from_micros(10, 20).unwrap(), operation: StoredOperation::Delete, recency: None },
        StoredEvent { origin: None, commit_position: 0, entity_key: None, iid: Iid([1; 16]), system_from: Timestamp(1), valid_time: Interval::from_micros(0, 100).unwrap(), operation: StoredOperation::Put(StructFields::from([("price".into(), Value::I64(12))])), recency: None },
        StoredEvent { origin: None, commit_position: 0, entity_key: None, iid: Iid([2; 16]), system_from: Timestamp(1), valid_time: Interval::from_micros(0, 100).unwrap(), operation: StoredOperation::Put(StructFields::from([("price".into(), Value::I64(7))])), recency: None },
    ]
}

#[test]
fn every_iid_run_row_survives_arrow_file_and_stream_roundtrips() {
    let events = events();
    for encoding in [None, Some(RunEndWidth::I16), Some(RunEndWidth::I32), Some(RunEndWidth::I64)] {
        let batch = build_event_batch(&events, Some(StructFields::from([("price".into(), LogicalType::Scalar(ScalarType::I64))])), encoding).unwrap();
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
    let mut projected = ipc::ArrowFile::open(Arc::from(bytes.as_slice()), Some(vec![0])).unwrap();
    assert_eq!(projected.schema().fields().len(), 1);
    let page = projected.read_page(0).unwrap();
    assert_eq!(page.num_columns(), 1);
    page.column(0).to_data().validate_full().unwrap();
}

fn uuid_fields(field: &arrow_schema::Field) -> Vec<arrow_schema::Field> {
    use arrow_schema::DataType;
    let children: Vec<_> = match field.data_type() {
        DataType::Struct(fields) => fields.iter().cloned().collect(),
        DataType::Union(fields, _) => fields.iter().map(|(_, field)| Arc::clone(field)).collect(),
        DataType::List(field) | DataType::Map(field, _) => vec![Arc::clone(field)],
        _ => vec![],
    };
    let own = (field.metadata().get(rtdb_columnar::EXTENSION_NAME_KEY).map(String::as_str) == Some("uuid")).then(|| field.clone());
    own.into_iter().chain(children.iter().flat_map(|child| uuid_fields(child))).collect()
}

fn assert_jvm_uuid_field(field: &arrow_schema::Field) {
    assert_eq!(field.data_type(), &arrow_schema::DataType::FixedSizeBinary(16));
    assert_eq!(field.metadata().get(rtdb_columnar::EXTENSION_METADATA_KEY).map(String::as_str), Some(""));
}

#[test]
fn uuid_columns_round_trip_through_arrow_file_and_stream() {
    let logical = LogicalType::Scalar(ScalarType::Uuid).maybe();
    let cells = vec![Value::Uuid([0; 16]), Value::Null, Value::Uuid([0xff; 16]), Value::Uuid(std::array::from_fn(|index| index as u8))];
    let field = rtdb_columnar::to_field("id", &logical).unwrap();
    assert_jvm_uuid_field(&field);
    let schema = arrow_schema::Schema::new(vec![field]);
    let page = arrow_array::RecordBatch::try_new(Arc::new(schema.clone()), vec![rtdb_columnar::build_array(&logical, &cells).unwrap()]).unwrap();
    let read_back = |page: &arrow_array::RecordBatch| {
        assert_eq!(uuid_fields(page.schema().field(0)).len(), 1);
        assert_jvm_uuid_field(page.schema().field(0));
        assert_eq!(rtdb_columnar::from_field(page.schema().field(0)).unwrap(), logical);
        (0..page.num_rows()).map(|row| rtdb_columnar::read_value(page.schema().field(0), page.column(0).as_ref(), row).unwrap()).collect::<Vec<_>>()
    };
    let mut file = ipc::ArrowFile::open(ipc::write_file(&schema, &[page.clone()]).unwrap().into(), None).unwrap();
    assert_eq!(read_back(&file.read_page(0).unwrap()), cells);
    let (stream_schema, pages) = ipc::read_stream(Arc::from(ipc::write_stream(&schema, &[page]).unwrap())).unwrap();
    assert_jvm_uuid_field(stream_schema.field(0));
    assert_eq!(pages.len(), 1);
    assert_eq!(read_back(&pages[0]), cells);
}

#[test]
fn uuid_put_documents_are_read_back_through_the_event_schema() {
    let uuid = Value::Uuid(std::array::from_fn(|index| 0xa0 | index as u8));
    let put = |iid: u8, id: Value| StoredEvent { origin: None, commit_position: 0, entity_key: None, iid: Iid([iid; 16]), system_from: Timestamp(1), valid_time: Interval::ALL, operation: StoredOperation::Put(StructFields::from([("ref".into(), id)])), recency: None };
    let events = vec![put(1, uuid), put(2, Value::Null), put(3, Value::Uuid([0; 16]))];
    let fields = BTreeMap::from([("ref".into(), LogicalType::Scalar(ScalarType::Uuid).maybe())]);
    let batch = build_event_batch(&events, Some(fields.into()), None).unwrap();
    let bytes = ipc::write_file(batch.schema().as_ref(), &[batch]).unwrap();
    let mut file = ipc::ArrowFile::open(bytes.into(), None).unwrap();
    let uuids: Vec<_> = file.schema().fields().iter().flat_map(|field| uuid_fields(field)).collect();
    assert_eq!(uuids.len(), 1);
    assert_jvm_uuid_field(&uuids[0]);
    let decoded = EventBatch::new(file.read_page(0).unwrap()).unwrap();
    for (row, event) in events.iter().enumerate() { assert_eq!(&decoded.read_event(row).unwrap(), event); }
}

fn operation_legs(batch: &arrow_array::RecordBatch) -> Vec<String> {
    let arrow_schema::DataType::Union(legs, _) = batch.schema().field_with_name("op").unwrap().data_type().clone() else { panic!("op is a union") };
    legs.iter().map(|(_, leg)| leg.name().clone()).collect()
}

#[test]
fn live_pages_carry_the_put_leg_last_and_only_once_a_document_is_written() {
    let memory = rtdb_types::MemoryBudget::new(1 << 24);
    let fields = || Some(StructFields::from([("price".into(), LogicalType::Scalar(ScalarType::I64))]));
    let live = rtdb_format::data::owned_event_batch(&events(), fields(), false, &memory).unwrap();
    assert_eq!(operation_legs(live.batch()), ["delete", "erase", "put"]);
    let decoded = EventBatch::new(live.batch().clone()).unwrap();
    for (row, event) in events().iter().enumerate() { assert_eq!(&decoded.read_event(row).unwrap(), event); }

    let deletes: Vec<_> = events().into_iter().filter(|event| !matches!(event.operation, StoredOperation::Put(_))).collect();
    let without_puts = rtdb_format::data::owned_event_batch(&deletes, Some(StructFields::new()), false, &memory).unwrap();
    assert_eq!(operation_legs(without_puts.batch()), ["delete", "erase"]);

    assert_eq!(operation_legs(&build_event_batch(&events(), fields(), None).unwrap()), ["put", "delete", "erase"], "compacted pages declare put first");
}

#[test]
fn put_columns_keep_the_order_their_fields_were_first_seen() {
    let fields = StructFields::from([("_id".into(), LogicalType::Scalar(ScalarType::I64)), ("system_time".into(), LogicalType::Scalar(ScalarType::I64)), ("committed".into(), LogicalType::Scalar(ScalarType::Bool))]);
    let put = StoredEvent { origin: None, commit_position: 0, entity_key: None, iid: Iid([1; 16]), system_from: Timestamp(1), valid_time: Interval::ALL, recency: None,
        operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(1)), ("system_time".into(), Value::I64(2)), ("committed".into(), Value::Bool(true))])) };
    let batch = build_event_batch(&[put], Some(fields), None).unwrap();
    let arrow_schema::DataType::Union(legs, _) = batch.schema().field_with_name("op").unwrap().data_type().clone() else { panic!("op is a union") };
    let (_, put) = legs.iter().find(|(_, leg)| leg.name() == "put").unwrap();
    let arrow_schema::DataType::Struct(columns) = put.data_type() else { panic!("put is a struct") };
    assert_eq!(columns.iter().map(|column| column.name().as_str()).collect::<Vec<_>>(), ["_id", "system_time", "committed"]);
}
