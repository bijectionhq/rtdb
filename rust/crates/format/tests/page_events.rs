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
            0 => doc(id, Some(Value::list(vec![Value::Utf8("a".into()), Value::Utf8(format!("t{id}"))]))),
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

#[test]
fn flat_columns_copied_from_several_pages_encode_to_the_bytes_their_decoded_events_encode_to() {
    use rtdb_types::{DecimalValue, TimeUnit};
    let scalar = LogicalType::Scalar;
    let timestamp = ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) };
    let decimal = ScalarType::Decimal { precision: 10, scale: 2, bit_width: 128 };
    let full = StructFields::from([
        ("_id".into(), scalar(ScalarType::I64)), ("name".into(), scalar(ScalarType::Utf8).maybe()), ("amount".into(), scalar(ScalarType::F64).maybe()),
        ("at".into(), scalar(timestamp).maybe()), ("flag".into(), scalar(ScalarType::Bool).maybe()), ("day".into(), scalar(ScalarType::DateDay).maybe()),
        ("blob".into(), scalar(ScalarType::Binary).maybe()), ("code".into(), scalar(ScalarType::FixedSizeBinary(4)).maybe()),
        ("price".into(), scalar(decimal).maybe()), ("small".into(), scalar(ScalarType::I32).maybe()),
    ]);
    let cell = |name: &str, id: i64| -> Value {
        if id % 5 == 3 { return Value::Null; }
        match name {
            "name" => Value::Utf8(format!("name-{id}")), "amount" => Value::F64(id as f64 * 1.5),
            "at" => Value::Timestamp { value: id * 1_000, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) },
            "flag" => Value::Bool(id % 2 == 0), "day" => Value::DateDay(id as i32), "blob" => Value::Binary(vec![id as u8; (id % 3) as usize]),
            "code" => Value::FixedSizeBinary(vec![id as u8; 4]),
            "price" => Value::Decimal(DecimalValue::new(i128::from(id * 101).to_be_bytes().to_vec(), 10, 2, 128).unwrap()),
            _ => Value::I32(id as i32),
        }
    };
    let page = |ids: std::ops::Range<i64>, columns: &[&str]| {
        let stored: StructFields<LogicalType> = full.iter().filter(|(name, _)| *name == "_id" || columns.contains(&name.as_str())).map(|(name, ty)| (name.clone(), ty.clone())).collect();
        let events = ids.map(|id| StoredEvent {
            iid: EntityId::Integer(id).iid(), system_from: Timestamp(id), valid_time: Interval::new(Timestamp(id), Timestamp::MAX).unwrap(),
            operation: if id % 7 == 6 { StoredOperation::Delete } else {
                let mut document = StructFields::from([("_id".into(), Value::I64(id))]);
                for name in columns { document.insert((*name).into(), cell(name, id)); }
                StoredOperation::Put(document)
            },
            recency: None,
        }).collect::<Vec<_>>();
        EventBatch::new(build_event_batch_with_recency(&events, Some(stored), None, false).unwrap()).unwrap()
    };
    let all = ["name", "amount", "at", "flag", "day", "blob", "code", "price", "small"];
    let pages = [page(0..20, &all), page(20..30, &["name", "amount", "flag"]), page(30..45, &all)];
    let mut encoded = Vec::new();
    let mut decoded = Vec::new();
    for batch in &pages {
        encoded.extend(batch.page_events().unwrap());
        decoded.extend((0..batch.len()).map(|row| batch.read_event(row).unwrap()));
    }
    let order: Vec<usize> = (0..encoded.len()).map(|index| (index * 17) % encoded.len()).collect();
    let encoded: Vec<_> = order.iter().map(|index| encoded[*index].clone()).collect();
    let decoded: Vec<_> = order.iter().map(|index| decoded[*index].clone()).collect();
    let expected = copy_event_batch_with_recency(&decoded, Some(full.clone()), None, false).unwrap();
    let actual = copy_encoded_batch_with_recency(&encoded, Some(full), None, false).unwrap();
    assert_eq!(write_file(actual.schema().as_ref(), &[actual]).unwrap(), write_file(expected.schema().as_ref(), &[expected]).unwrap());
}
