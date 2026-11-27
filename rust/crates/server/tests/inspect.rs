use std::sync::Arc;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use rtdb_columnar::MemoryBudget;
use rtdb_format::{block::proto::Block, catalog::{BlockCatalog, FIRST_TABLE_OID, TableEntry}, keys::TableSlug};
use rtdb_server::inspect::{self, FileKind};
use rtdb_types::{TableRef, Value};
use serde_json::json;

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
fn decimal_inspection_renders_real_wire_parameters_without_native_annotations() {
    for (precision, scale) in [(3, i32::MIN), (3, i32::MAX), (i32::MIN, 0), (256, 129), (i32::MAX, i32::MAX)] {
        let ty = rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Decimal { precision, scale, bit_width: 128 });
        let value = Value::Decimal(rtdb_types::DecimalValue::new((-123i128).to_be_bytes().to_vec(), precision, scale, 128).unwrap());
        let schema = Arc::new(Schema::new(vec![rtdb_columnar::to_field("value", &ty).unwrap()]));
        let page = RecordBatch::try_new(schema.clone(), vec![rtdb_columnar::copy_array(&ty, std::slice::from_ref(&value)).unwrap()]).unwrap();
        for (kind, bytes) in [(FileKind::ArrowFile, rtdb_format::ipc::write_file(&schema, std::slice::from_ref(&page)).unwrap()), (FileKind::ArrowStream, rtdb_format::ipc::write_stream(&schema, std::slice::from_ref(&page)).unwrap())] {
            let inspected = inspect_bytes(kind, &bytes, 1 << 24).unwrap();
            assert_eq!(inspected["schema"]["fields"][0]["data_type"]["Decimal128"][1], serde_json::Value::from(scale));
            assert_eq!(inspected["schema"]["fields"][0]["data_type"]["Decimal128"][0], serde_json::Value::from(precision));
            assert!(inspected["schema"]["fields"][0]["metadata"].get(rtdb_columnar::INTERNAL_DECIMAL_SCALE_KEY).is_none());
            assert!(inspected["schema"]["fields"][0]["metadata"].get(rtdb_columnar::INTERNAL_DECIMAL_PRECISION_KEY).is_none());
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

fn classic_oid_preflight(wire: Block, batch: &[TableRef]) -> serde_json::Value {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("block.binpb");
    let bytes = BlockCatalog::from_proto(wire).unwrap().encode();
    std::fs::write(&path, &bytes).unwrap();
    let memory = MemoryBudget::new(1 << 20);
    let mut output = Vec::new();
    inspect::classic_oid_preflight(&path, batch, &memory, &mut output).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert_eq!(memory.used_bytes(), 0);
    serde_json::from_slice(&output).unwrap()
}

#[test]
fn oid_preflight_compares_the_names_only_wire_order_under_both_decoders() {
    let wire = Block { block_index: Some(7), table_names: vec!["public/t𐐀".into(), "public/tＡ".into()], ..Default::default() };
    let report = classic_oid_preflight(wire, &[]);
    assert_eq!(report["block_index"], 7);
    assert_eq!(report["registry_source"], "names_only");
    assert_eq!(report["checkpoint_registry"], json!({
        "eligible": false,
        "relations": [
            {"table": {"schema": "public", "table": "tＡ"}, "native_oid": FIRST_TABLE_OID, "xtdb_oid": FIRST_TABLE_OID + 1, "xtdb_slug": "public$tＡ"},
            {"table": {"schema": "public", "table": "t𐐀"}, "native_oid": FIRST_TABLE_OID + 1, "xtdb_oid": FIRST_TABLE_OID, "xtdb_slug": "public$t𐐀"},
        ],
        "xtdb_slug_collisions": {},
    }));
    assert!(report["allocation_batch"].is_null());
}

#[test]
fn oid_preflight_preserves_explicit_identity_and_compares_only_fresh_batch_names() {
    let existing = TableRef::new("public", "existing");
    let recorded = TableEntry { table: existing.clone(), oid: 20_000, slug: TableSlug::recorded("original-locator").unwrap() };
    let wire = Block {
        tables: vec![recorded.to_proto()],
        table_names: vec!["public/would_sort_first".into()],
        ..Default::default()
    };
    let bmp = TableRef::new("public", "tＡ");
    let supplementary = TableRef::new("public", "t𐐀");
    let report = classic_oid_preflight(wire.clone(), &[bmp.clone(), existing, supplementary, bmp.clone()]);
    assert_eq!(report["registry_source"], "recorded");
    assert_eq!(report["checkpoint_registry"], json!({
        "eligible": true,
        "relations": [
            {"table": {"schema": "public", "table": "existing"}, "native_oid": 20_000, "xtdb_oid": 20_000, "xtdb_slug": "original-locator"},
        ],
        "xtdb_slug_collisions": {},
    }));
    assert_eq!(report["allocation_batch"], json!({
        "eligible": false,
        "relations": [
            {"table": {"schema": "public", "table": "t𐐀"}, "native_oid": 20_004, "xtdb_oid": 20_003, "xtdb_slug": "public$t𐐀"},
            {"table": {"schema": "public", "table": "tＡ"}, "native_oid": 20_003, "xtdb_oid": 20_004, "xtdb_slug": "public$tＡ"},
        ],
        "xtdb_slug_collisions": {},
    }));
    let single = classic_oid_preflight(wire, &[bmp]);
    assert_eq!(single["allocation_batch"]["eligible"], true);
    assert_eq!(single["allocation_batch"]["relations"][0]["native_oid"], 20_003);
}

#[test]
fn oid_preflight_flags_slug_aliases_in_legacy_roots_and_against_recorded_locations() {
    let legacy = classic_oid_preflight(Block {
        table_names: vec!["public/a.b".into(), "public/a$b".into()], ..Default::default()
    }, &[]);
    assert_eq!(legacy["checkpoint_registry"]["eligible"], false);
    assert_eq!(legacy["checkpoint_registry"]["xtdb_slug_collisions"], json!({
        "public$a$b": [{"schema": "public", "table": "a$b"}, {"schema": "public", "table": "a.b"}],
    }));
    let anchor = TableEntry { table: TableRef::new("public", "renamed"), oid: 21_000, slug: TableSlug::recorded("public$a$b").unwrap() };
    let report = classic_oid_preflight(Block { tables: vec![anchor.to_proto()], ..Default::default() }, &[TableRef::new("public", "a.b")]);
    assert_eq!(report["checkpoint_registry"]["eligible"], true);
    assert_eq!(report["allocation_batch"]["eligible"], false);
    assert_eq!(report["allocation_batch"]["xtdb_slug_collisions"], json!({
        "public$a$b": [{"schema": "public", "table": "a.b"}, {"schema": "public", "table": "renamed"}],
    }));
}

#[test]
fn oid_preflight_marks_a_native_only_root_ineligible_even_when_oids_agree() {
    let report = classic_oid_preflight(Block { naming_epoch: Some(1), ..Default::default() }, &[TableRef::new("public", "a")]);
    assert_eq!(report["rtdb_only_root"], true);
    assert_eq!(report["checkpoint_registry"]["eligible"], false);
    assert_eq!(report["allocation_batch"]["eligible"], false);
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
