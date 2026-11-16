use rtdb_columnar::MemoryBudget;
use rtdb_format::envelope::{SourceBody, SourceMessage};
use rtdb_tx::{Documents, OperationValue, SourceTransaction, SubmitOptions, TxOp, decode_operation_rows};
use rtdb_types::{TableRef, Value};

#[test]
fn log_tables_test_tx_ops_decoded_retains_custom_union_tags_and_omits_null_struct_fields() {
    let memory = MemoryBudget::new(128 * 1024 * 1024);
    let source = SourceTransaction::new(vec![TxOp::PutDocs {
        table: TableRef::new("public", "docs"),
        documents: Documents::new(vec![[("_id".into(), Value::I64(1)), ("missing".into(), Value::Null)].into()], &memory).unwrap(),
        valid_from: None, valid_to: None,
    }], SubmitOptions::default(), &memory).unwrap();
    let encoded = source.encode().unwrap();
    let Some(SourceMessage::Protobuf(SourceBody::Tx(tx))) = SourceMessage::decode(encoded.as_slice()).unwrap() else { panic!() };
    let baseline = memory.used_bytes();
    let rows = decode_operation_rows(tx.tx_ops.as_deref().unwrap(), &memory).unwrap();
    assert_eq!(rows.rows().len(), 1);
    let OperationValue::Struct(fields) = &rows.rows()[0] else { panic!() };
    let OperationValue::Tagged { tag, value } = &fields["tx-ops"] else { panic!() };
    assert_eq!(tag, "put-docs");
    let OperationValue::Struct(fields) = value.as_ref() else { panic!() };
    assert!(!fields.contains_key("_valid_from"));
    assert!(!fields.contains_key("_valid_to"));
    let OperationValue::Tagged { tag, value } = &fields["documents"] else { panic!() };
    assert_eq!(tag, "public/docs");
    let OperationValue::List { values, .. } = value.as_ref() else { panic!() };
    let OperationValue::Struct(document) = &values[0] else { panic!() };
    assert!(!document.contains_key("missing"));
    assert!(memory.used_bytes() > baseline);
    drop(rows); assert_eq!(memory.used_bytes(), baseline);
}
