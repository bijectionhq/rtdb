use std::sync::Arc;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use rtdb_columnar::MemoryBudget;
use rtdb_server::inspect::{self, FileKind};
use rtdb_types::Value;

fn inspect_bytes(kind: FileKind, bytes: &[u8], budget: usize) -> rtdb_types::Result<serde_json::Value> {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("input");
    std::fs::write(&path, bytes).unwrap();
    let memory = MemoryBudget::new(budget);
    let mut output = Vec::new();
    let result = inspect::file(kind, &path, &memory, &mut output);
    assert_eq!(memory.used_bytes(), 0);
    result?;
    Ok(serde_json::from_slice(&output).unwrap())
}

#[test]
fn arrow_inspection_preserves_duplicate_columns_typed_nulls_and_file_batches() {
    let schema = Arc::new(Schema::new(vec![Field::new("duplicate", DataType::Int64, true), Field::new("duplicate", DataType::Int64, true)]));
    let columns = vec![Arc::new(Int64Array::from(vec![Some(7), None])) as ArrayRef, Arc::new(Int64Array::from(vec![None, Some(9)])) as ArrayRef];
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let bytes = rtdb_format::ipc::write_file(&schema, &[batch.clone(), batch.clone()]).unwrap();
    let inspected = inspect_bytes(FileKind::ArrowFile, &bytes, 1 << 24).unwrap();
    assert_eq!(inspected["batches"].as_array().unwrap().len(), 2);
    assert_eq!(inspected["schema"]["fields"][0]["name"], "duplicate");
    assert_eq!(inspected["schema"]["fields"][1]["name"], "duplicate");
    assert_eq!(serde_json::from_value::<Vec<Vec<Value>>>(inspected["batches"][0].clone()).unwrap(), vec![vec![Value::I64(7), Value::Null], vec![Value::Null, Value::I64(9)]]);
    let stream = rtdb_format::ipc::write_stream(&schema, &[batch.clone(), batch]).unwrap();
    let inspected = inspect_bytes(FileKind::ArrowStream, &stream, 1 << 24).unwrap();
    assert_eq!(inspected["batch"].as_array().unwrap().len(), 2);
    assert!(inspect_bytes(FileKind::ArrowFile, &bytes, 8).is_err());
}

#[test]
fn decimal_inspection_renders_real_wire_scale_without_native_annotation() {
    for scale in [i32::MIN, i32::MAX] {
        let ty = rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Decimal { precision: 3, scale, bit_width: 128 });
        let value = Value::Decimal(rtdb_types::DecimalValue::new((-123i128).to_be_bytes().to_vec(), 3, scale, 128).unwrap());
        let schema = Arc::new(Schema::new(vec![rtdb_columnar::to_field("value", &ty).unwrap()]));
        let page = RecordBatch::try_new(schema.clone(), vec![rtdb_columnar::build_array(&ty, std::slice::from_ref(&value)).unwrap()]).unwrap();
        for (kind, bytes) in [(FileKind::ArrowFile, rtdb_format::ipc::write_file(&schema, std::slice::from_ref(&page)).unwrap()), (FileKind::ArrowStream, rtdb_format::ipc::write_stream(&schema, std::slice::from_ref(&page)).unwrap())] {
            let inspected = inspect_bytes(kind, &bytes, 1 << 24).unwrap();
            assert_eq!(inspected["schema"]["fields"][0]["data_type"]["Decimal128"][1], serde_json::Value::from(scale));
            assert!(inspected["schema"]["fields"][0]["metadata"].get(rtdb_columnar::INTERNAL_DECIMAL_SCALE_KEY).is_none());
            let batch = if kind == FileKind::ArrowFile { &inspected["batches"][0] } else { &inspected["batch"] };
            assert_eq!(serde_json::from_value::<Vec<Vec<Value>>>(batch.clone()).unwrap(), vec![vec![value.clone()]]);
        }
    }
}

#[test]
fn block_inspection_preserves_absent_and_zero_term_sequences() {
    for sequence in [None, Some(rtdb_types::TermSequence(0))] {
        let wire = rtdb_format::block::proto::Block { block_index: Some(0), boundary_term_seq: sequence.map(|sequence| sequence.0), ..Default::default() };
        let block = rtdb_format::catalog::BlockCatalog::from_proto(wire).unwrap();
        let inspected = inspect_bytes(FileKind::Block, &block.encode(), 1 << 20).unwrap();
        assert_eq!(inspected["boundary-term-seq"], sequence.map_or(serde_json::Value::Null, |sequence| sequence.0.into()));
    }
}

#[test]
fn hash_trie_inspection_retains_empty_children_and_root_path() {
    use rtdb_trie::{PersistedNode, PersistedTrie};
    let trie = PersistedTrie { log_limit: 2, page_limit: 4, root_path: vec![1], root: PersistedNode::Branch(vec![None, Some(PersistedNode::Leaf(vec![2, 1])), None, None]) };
    let inspected = inspect_bytes(FileKind::HashTrie, &rtdb_format::memory_trie::encode(&trie).unwrap(), 1 << 20).unwrap();
    assert_eq!(inspected["root-path"], serde_json::json!([1]));
    assert_eq!(inspected["tree"], serde_json::json!(["branch", null, ["leaf", [2, 1]], null, null]));
    assert!(inspect_bytes(FileKind::HashTrie, &[0xff], 1 << 20).is_err());
}
