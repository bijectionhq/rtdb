use rtdb_types::StructFields;
use std::collections::BTreeMap;
use std::sync::Arc;

use rtdb_format::bloom::{BloomFilter, hashes};
use rtdb_format::metadata::{LeafColumns, MetadataFile, MetadataNode};
use rtdb_types::{LogicalType, ScalarType, Timestamp, Value};

#[test]
fn bloom_matches_portable_roaring_without_run_containers() {
    let mut bloom = BloomFilter::default();
    bloom.insert_hashes(1, 1);
    bloom.insert_hashes(1, 1);
    let bytes = bloom.to_bytes().unwrap();
    assert_eq!(bytes, [58, 48, 0, 0, 1, 0, 0, 0, 0, 0, 4, 0, 16, 0, 0, 0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0]);
    assert_eq!(BloomFilter::from_bytes(&bytes).unwrap(), bloom);
    assert!(bloom.contains_hashes(1, 1));
    assert!(!bloom.contains_hashes(2, 2));
    assert_eq!(bloom.cardinality(), 5);
    assert_eq!(hashes(-1, i32::MAX), [4_194_303, 4_194_302, 4_194_301, 4_194_300, 4_194_299]);
}

fn columns() -> Vec<Value> {
    vec![Value::Struct(StructFields::from([
        ("col-name".into(), Value::Utf8("_iid".into())),
        ("root-col?".into(), Value::Bool(true)),
        ("count".into(), Value::I64(2)),
        ("bytes".into(), Value::Struct(StructFields::from([("bloom".into(), Value::Binary(BloomFilter::default().to_bytes().unwrap()))]))),
    ]))]
}

#[test]
fn metadata_roundtrip_retains_iid_and_recency_branch_order() {
    let nodes = vec![
        MetadataNode::Leaf { data_page_index: 0, columns: LeafColumns::new(columns()) },
        MetadataNode::Leaf { data_page_index: 1, columns: LeafColumns::new(columns()) },
        MetadataNode::IidBranch(vec![None, Some(0), None, Some(1)]),
        MetadataNode::Leaf { data_page_index: 2, columns: LeafColumns::new(columns()) },
        MetadataNode::RecencyBranch(vec![(Timestamp(100), 2), (Timestamp::MAX, 3)]),
    ];
    let original = MetadataFile::from_nodes(nodes.clone(), true).unwrap();
    let loaded = MetadataFile::read(Arc::from(original.write().unwrap())).unwrap();
    assert_eq!(loaded.nodes(), &nodes);
    assert_eq!(loaded.leaves(), [(vec![1], 0), (vec![3], 1), (vec![], 2)]);
    assert_eq!(loaded.schema(), original.schema());
    assert_eq!(loaded.write().unwrap(), original.write().unwrap());
}

#[test]
fn imported_metadata_rewrites_raw_decimal_cells_without_declared_precision_validation() {
    for precision in [-1, 1, 256, i32::MAX] {
        let mut columns = columns();
        let Value::Struct(column) = &mut columns[0] else { panic!("Expected column metadata") };
        column.insert("decimal-value".into(), Value::Decimal(rtdb_types::DecimalValue::new(123i128.to_be_bytes().to_vec(), precision, i32::MAX, 128).unwrap()));
        let original = MetadataFile::from_nodes(vec![MetadataNode::Leaf { data_page_index: 0, columns: LeafColumns::new(columns) }], false).unwrap();
        let bytes = original.write().unwrap();
        let loaded = MetadataFile::read(bytes.clone().into()).unwrap();
        assert_eq!(loaded.nodes(), original.nodes());
        assert_eq!(loaded.schema(), original.schema());
        assert_eq!(loaded.write().unwrap(), bytes);
    }
}

#[test]
fn metadata_rejects_forward_children_and_unavailable_recency_leg() {
    assert!(MetadataFile::from_nodes(vec![MetadataNode::IidBranch(vec![Some(1), None, None, None])], false).is_err());
    assert!(MetadataFile::from_nodes(vec![MetadataNode::Nil, MetadataNode::RecencyBranch(vec![(Timestamp(100), 0)])], false).is_err());
}

#[test]
fn single_entity_pages_may_share_the_same_recency_ceiling() {
    let nodes = vec![MetadataNode::Nil, MetadataNode::Nil, MetadataNode::RecencyBranch(vec![(Timestamp(100), 0), (Timestamp(100), 1)])];
    let metadata = MetadataFile::from_nodes(nodes.clone(), true).unwrap();
    assert_eq!(MetadataFile::read(metadata.write().unwrap().into()).unwrap().nodes(), &nodes);
    assert!(MetadataFile::from_nodes(vec![MetadataNode::Nil, MetadataNode::Nil, MetadataNode::RecencyBranch(vec![(Timestamp(100), 0), (Timestamp(99), 1)])], true).is_err());
}

#[test]
fn metadata_counts_masked_struct_children_and_keeps_numeric_bounds() {
    let ty = LogicalType::Struct(StructFields::from([("n".into(), LogicalType::Scalar(ScalarType::I64).maybe())])).maybe();
    let values = [Value::Null, Value::Struct(StructFields::from([("n".into(), Value::I64(-4))])), Value::Struct(StructFields::from([("n".into(), Value::I64(7))]))];
    let field = rtdb_columnar::to_field("doc", &ty).unwrap();
    let array = rtdb_columnar::build_array(&ty, &values).unwrap();
    let mut columns = Vec::new();
    let root = rtdb_format::metadata::column_metadata(&field, array.as_ref(), true, false, &mut columns).unwrap();
    assert_eq!(root, 1);
    assert_eq!(columns[0].field("count"), Some(&Value::I64(2)));
    assert_eq!(columns[0].field("root-col?"), Some(&Value::Bool(false)));
    assert_eq!(columns[0].field("numbers").unwrap().field("min"), Some(&Value::F64(-4.0)));
    assert_eq!(columns[0].field("numbers").unwrap().field("max"), Some(&Value::F64(7.0)));
    assert_eq!(columns[root].field("struct"), Some(&Value::list(vec![Value::I32(0)])));
    assert_eq!(columns[root].field("count"), Some(&Value::I64(2)));
}

#[test]
fn iid_metadata_builds_bloom_when_other_columns_do_not() {
    let ty = LogicalType::Scalar(ScalarType::FixedSizeBinary(16));
    let values = [Value::FixedSizeBinary(vec![0; 16]), Value::FixedSizeBinary(vec![1; 16])];
    let field = rtdb_columnar::to_field("_iid", &ty).unwrap();
    let array = rtdb_columnar::build_array(&ty, &values).unwrap();
    let mut columns = Vec::new();
    rtdb_format::metadata::column_metadata(&field, array.as_ref(), true, false, &mut columns).unwrap();
    let Value::Binary(bytes) = columns[0].field("bytes").unwrap().field("bloom").unwrap() else { panic!("IID bloom is required") };
    let bloom = BloomFilter::from_bytes(bytes).unwrap();
    assert!(bloom.contains_cell(&field, array.as_ref(), 0).unwrap());
    assert!(bloom.contains_cell(&field, array.as_ref(), 1).unwrap());
}

#[test]
fn metadata_pages_keep_their_summaries_after_releasing_page_scratch() {
    use rtdb_columnar::MemoryBudget;
    use rtdb_format::data::{StoredEvent, StoredOperation, build_event_batch};
    use rtdb_format::metadata::MetadataBuilder;
    use rtdb_types::{EntityId, Interval};

    let events = (0..1024).map(|id| StoredEvent { position: None,
        origin: None,
        commit_position: 0,
        entity_key: None,
        iid: EntityId::Integer(id).iid(), system_from: Timestamp(1), valid_time: Interval::ALL,
        operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(id))])), recency: None,
    }).collect::<Vec<_>>();
    let fields = BTreeMap::from([("_id".into(), LogicalType::Scalar(ScalarType::I64))]);
    let page = build_event_batch(&events, Some(fields.into()), None).unwrap();
    let memory = MemoryBudget::new(64 << 20);
    let mut builder = MetadataBuilder::new(false, true);
    let leaves = (0..64).map(|_| builder.write_leaf_reserved(&page, &memory).unwrap().1).collect::<Vec<_>>();
    assert!(memory.used_bytes() < 8 << 20);
    let serialization = memory.reserve(builder.serialization_bound().unwrap()).unwrap();
    let (metadata, summary) = builder.finish().unwrap();
    let bytes = metadata.write().unwrap();
    assert_eq!(summary.row_count, Some(65_536));
    assert_eq!(MetadataFile::read(bytes.into()).unwrap().nodes().len(), 64);
    drop(metadata);
    drop(serialization);
    drop(leaves);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn metadata_builder_rebases_nested_column_indices_across_data_pages() {
    use rtdb_format::data::{StoredEvent, StoredOperation, build_event_batch};
    use rtdb_format::metadata::MetadataBuilder;
    use rtdb_types::{Iid, Interval};

    let document = BTreeMap::from([("nested".into(), Value::Struct(StructFields::from([("n".into(), Value::I64(5))])))]);
    let event = StoredEvent { position: None, origin: None, commit_position: 0, entity_key: None, iid: Iid([1; 16]), system_from: Timestamp(10), valid_time: Interval::from_micros(0, 100).unwrap(), operation: StoredOperation::Put(document.clone().into()), recency: None };
    let fields = document.iter().map(|(name, value)| (name.clone(), value.logical_type())).collect();
    let batch = build_event_batch(&[event], Some(fields), None).unwrap();
    let mut writer = MetadataBuilder::new(false, false);
    let first = writer.write_leaf(&batch).unwrap();
    let second = writer.write_leaf(&batch).unwrap();
    writer.write_iid_branch([Some(first), None, None, Some(second)]).unwrap();
    let (file, summary) = writer.finish().unwrap();
    let MetadataNode::Leaf { columns: first_columns, .. } = &file.nodes()[first] else { panic!("First node must be a leaf") };
    let MetadataNode::Leaf { columns: second_columns, .. } = &file.nodes()[second] else { panic!("Second node must be a leaf") };
    let nested = |columns: &[Value]| columns.iter().find(|column| column.field("col-name") == Some(&Value::Utf8("nested".into()))).unwrap().field("struct").unwrap().clone();
    assert_eq!(nested(&first_columns.values().unwrap()), Value::list(vec![Value::I32(4)]));
    assert_eq!(nested(&second_columns.values().unwrap()), Value::list(vec![Value::I32(4 + first_columns.values().unwrap().len() as i32)]));
    assert_eq!(summary.row_count, Some(2));
    let temporal = summary.temporal_metadata.unwrap();
    assert_eq!(temporal.min_valid_from, Some(0));
    assert_eq!(temporal.max_valid_to, Some(100));
    assert_eq!(MetadataFile::read(Arc::from(file.write().unwrap())).unwrap().leaves(), [(vec![0], 0), (vec![3], 1)]);
}

#[test]
fn a_sliced_pages_temporal_bounds_are_those_of_its_own_rows() {
    use rtdb_format::data::{StoredEvent, StoredOperation, build_event_batch};
    use rtdb_format::metadata::MetadataBuilder;
    use rtdb_types::{Iid, Interval};

    let event = |n: i64| StoredEvent { position: None, origin: None, commit_position: 0, entity_key: None, iid: Iid([n as u8; 16]), system_from: Timestamp(10 * n), valid_time: Interval::from_micros(n, 100 * n).unwrap(), operation: StoredOperation::Delete, recency: None };
    let batch = build_event_batch(&[event(1), event(2), event(3), event(4)], None, None).unwrap().slice(1, 2);
    let mut writer = MetadataBuilder::new(false, false);
    writer.write_leaf(&batch).unwrap();
    let temporal = writer.finish().unwrap().1.temporal_metadata.unwrap();
    assert_eq!((temporal.min_valid_from, temporal.max_valid_from, temporal.min_valid_to, temporal.max_valid_to), (Some(2), Some(3), Some(200), Some(300)));
    assert_eq!((temporal.min_system_from, temporal.max_system_from), (Some(20), Some(30)));
}

#[test]
fn a_read_metadata_file_retains_its_stored_nodes_rather_than_their_decoded_statistics() {
    use rtdb_format::data::{StoredEvent, StoredOperation, build_event_batch};
    use rtdb_format::ipc::inspect_file_reserved;
    use rtdb_format::metadata::MetadataBuilder;
    use rtdb_types::{EntityId, Interval};

    let mut builder = MetadataBuilder::new(false, true);
    let mut written = Vec::new();
    for page in 0..64i64 {
        let events = (page * 1024..page * 1024 + 1024).map(|id| StoredEvent { position: None,
            origin: None,
            commit_position: 0,
            entity_key: None,
            iid: EntityId::Integer(id).iid(), system_from: Timestamp(1), valid_time: Interval::ALL,
            operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(id)), ("sku".into(), Value::Utf8(format!("sku-{id}")))])), recency: None,
        }).collect::<Vec<_>>();
        let fields = BTreeMap::from([("_id".into(), LogicalType::Scalar(ScalarType::I64)), ("sku".into(), LogicalType::Scalar(ScalarType::Utf8))]);
        builder.write_leaf(&build_event_batch(&events, Some(fields.into()), None).unwrap()).unwrap();
    }
    let (metadata, _) = builder.finish().unwrap();
    for node in metadata.nodes() { if let MetadataNode::Leaf { columns, .. } = node { written.push(columns.values().unwrap().into_owned()); } }
    let bytes = metadata.write().unwrap();
    let mut cursor = std::io::Cursor::new(bytes.clone());
    let bound = inspect_file_reserved(&mut cursor, |bytes| rtdb_columnar::MemoryBudget::new(1 << 40).reserve(bytes)).unwrap().full_allocation_bound().unwrap();
    let read = MetadataFile::read(bytes.into()).unwrap();
    assert!(read.stored_bytes() * 2 < bound, "a read file retains {} of the {bound} its decoding is bounded by", read.stored_bytes());
    let decoded = read.nodes().iter().filter_map(|node| match node { MetadataNode::Leaf { columns, .. } => Some(columns.values().unwrap().into_owned()), _ => None }).collect::<Vec<_>>();
    for (page, node) in read.nodes().iter().enumerate() {
        let MetadataNode::Leaf { columns, .. } = node else { continue; };
        let statistics = columns.statistics().unwrap();
        assert_eq!(statistics.root_column("_id").unwrap().bound("numbers", "min"), Some((page * 1024) as f64));
        let sku = statistics.root_column("sku").unwrap();
        assert_eq!(sku.count(), Some(1024));
        let bloom = rtdb_format::bloom::BloomFilter::from_bytes(sku.bloom().unwrap()).unwrap();
        assert!(bloom.contains_value(&Value::Utf8(format!("sku-{}", page * 1024))).unwrap());
    }
    let present = |columns: &[Value]| columns.iter().map(|column| match column {
        Value::Struct(fields) => Value::Struct(fields.iter().filter(|(_, value)| !matches!(value, Value::Null)).map(|(key, value)| (key.clone(), value.clone())).collect()),
        other => other.clone(),
    }).collect::<Vec<_>>();
    assert_eq!(decoded.iter().map(|columns| present(columns)).collect::<Vec<_>>(), written.iter().map(|columns| present(columns)).collect::<Vec<_>>(), "a statistic a page lacks reads back as null");
}
