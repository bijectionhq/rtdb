use rtdb_types::{EntityId, Interval, ListKind, LogicalType, ScalarType, StructFields, Timestamp, Value};
use rtdb_format::data::{EventBatch, StoredEvent, StoredOperation, build_event_batch_with_recency, copy_encoded_batch_with_recency, copy_event_batch_with_recency};
use rtdb_format::ipc::write_file;

#[test]
fn page_events_encode_to_the_bytes_their_decoded_events_encode_to() {
    let doc = |id: i64, extra: Option<Value>| {
        let mut fields = StructFields::from([("_id".into(), Value::I64(id)), ("name".into(), Value::Utf8(format!("n{id}")))]);
        if let Some(extra) = extra { fields.insert("tags".into(), extra); }
        StoredOperation::Put(fields)
    };
    let events = (0..12i64).map(|id| StoredEvent {
        iid: EntityId::Integer(id).iid(), system_from: Timestamp(10 + id), valid_time: Interval::new(Timestamp(id), Timestamp::MAX).unwrap(),
        operation: match id % 4 {
            0 => doc(id, Some(Value::List(vec![Value::Utf8("a".into()), Value::Utf8(format!("t{id}"))]))),
            1 => doc(id, None),
            2 => if id == 2 { StoredOperation::Delete } else { StoredOperation::Erase },
            _ => doc(id, Some(Value::Null)),
        },
        recency: Some(Timestamp(100 + id)),
    }).collect::<Vec<_>>();
    let list = LogicalType::List { kind: ListKind::List, element: Box::new(LogicalType::Scalar(ScalarType::Utf8)) };
    let stored = StructFields::from([("_id".into(), LogicalType::Scalar(ScalarType::I64)), ("name".into(), LogicalType::Scalar(ScalarType::Utf8)), ("tags".into(), list.clone().maybe())]);
    let page = build_event_batch_with_recency(&events, Some(stored.clone()), None, true).unwrap();
    let batch = EventBatch::new(page).unwrap();
    let mut output = stored.clone();
    output.insert("absent".into(), LogicalType::Scalar(ScalarType::F64).maybe());
    let decoded = (0..batch.len()).map(|row| batch.read_event(row)).collect::<Result<Vec<_>, _>>().unwrap();
    let expected = copy_event_batch_with_recency(&decoded, Some(output.clone()), None, true).unwrap();
    let actual = copy_encoded_batch_with_recency(&batch.page_events().unwrap(), Some(output), None, true).unwrap();
    assert_eq!(write_file(actual.schema().as_ref(), &[actual]).unwrap(), write_file(expected.schema().as_ref(), &[expected]).unwrap());
}
