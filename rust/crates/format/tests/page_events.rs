use rtdb_types::{EntityId, EventHeader, Interval, ListKind, LogicalType, ScalarType, StructFields, Timestamp, Value};
use rtdb_format::data::{EncodedEvent, EventBatch, PageEvent, PutColumn, StoredEvent, StoredOperation, build_event_batch_with_recency,
    copy_encoded_batch_with_recency, copy_event_batch_with_recency, owned_encoded_batch_with_recency};
use rtdb_format::ipc::write_file;

#[test]
fn page_events_encode_to_the_bytes_their_decoded_events_encode_to() {
    let doc = |id: i64, extra: Option<Value>| {
        let mut fields = StructFields::from([("_id".into(), Value::I64(id)), ("name".into(), Value::Utf8(format!("n{id}")))]);
        if let Some(extra) = extra { fields.insert("tags".into(), extra); }
        StoredOperation::Put(fields)
    };
    let events = (0..12i64).map(|id| StoredEvent {
        commit_position: 0,
        entity_key: None,
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
            commit_position: 0,
            entity_key: None,
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

struct ArrowEvent(PageEvent);

impl EncodedEvent for ArrowEvent {
    fn header(&self) -> EventHeader { self.0.header() }
    fn recency(&self) -> Option<Timestamp> { self.0.recency() }
    fn put_within(&self, fields: &StructFields<LogicalType>) -> bool { self.0.put_within(fields) }
    fn put_cell(&self, name: &str, _: &LogicalType) -> rtdb_types::Result<std::borrow::Cow<'_, Value>> {
        panic!("Decoded Arrow column {name}")
    }
    fn put_column(&self, name: &str) -> Option<PutColumn<'_>> { self.0.put_column(name) }
}

#[test]
fn compaction_copies_evolving_nested_layouts_from_selected_arrow_positions() {
    let width = 1 << 20;
    let integer = LogicalType::Scalar(ScalarType::I64);
    let number = integer.join(&LogicalType::Scalar(ScalarType::FixedSizeBinary(width)));
    let left = LogicalType::list(LogicalType::Struct(StructFields::from([("value".into(), number)])));
    let right = LogicalType::list(LogicalType::Struct(StructFields::from([("extra".into(), LogicalType::Scalar(ScalarType::Utf8))])));
    let record = |name: &str, value| Value::list(vec![Value::Struct(StructFields::from([(name.into(), value)]))]);
    let event = |id, value: Option<Value>| {
        let mut document = StructFields::from([("_id".into(), Value::I64(id))]);
        if let Some(value) = value { document.insert("v".into(), value); }
        StoredEvent {
            commit_position: 0,
            entity_key: None,
            iid: EntityId::Integer(id).iid(), system_from: Timestamp(id), valid_time: Interval::ALL,
            operation: StoredOperation::Put(document), recency: Some(Timestamp(100 + id)),
        }
    };
    let left_rows = [event(1, Some(record("value", Value::FixedSizeBinary(vec![17; width as usize])))),
        event(2, Some(record("value", Value::I64(42))))];
    let right_rows = [event(3, Some(record("extra", Value::Utf8("kept".into()))))];
    let absent = [event(4, None)];
    let fields = |ty| {
        let mut fields = StructFields::from([("_id".into(), integer.clone())]);
        if let Some(ty) = ty { fields.insert("v".into(), ty); }
        fields
    };
    let page = |rows, ty| {
        let batch = build_event_batch_with_recency(rows, Some(fields(ty)), None, true).unwrap();
        EventBatch::new(batch).unwrap().page_events().unwrap()
    };
    let left_page = page(&left_rows, Some(left.clone()));
    let right_page = page(&right_rows, Some(right.clone()));
    let absent_page = page(&absent, None);
    let events = [ArrowEvent(right_page[0].clone()), ArrowEvent(absent_page[0].clone()), ArrowEvent(left_page[1].clone())];
    let output = fields(Some(left.join(&right).maybe()));
    let memory = rtdb_columnar::MemoryBudget::new(1 << 18);

    let actual = owned_encoded_batch_with_recency(&events, Some(output.clone()), None, true, &memory).unwrap();

    let expected = copy_event_batch_with_recency(&[right_rows[0].clone(), absent[0].clone(), left_rows[1].clone()],
        Some(output), None, true).unwrap();
    assert_eq!(write_file(actual.batch().schema().as_ref(), &[actual.batch().clone()]).unwrap(),
        write_file(expected.schema().as_ref(), &[expected]).unwrap());
    assert!(memory.peak_bytes() < width as usize);
    assert!(memory.used_bytes() > 0);
    drop(actual);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn compaction_admits_typed_null_padding_before_allocating_it() {
    let event = StoredEvent { commit_position: 0, entity_key: None, iid: EntityId::Integer(1).iid(), system_from: Timestamp(1), valid_time: Interval::ALL,
        operation: StoredOperation::Put(StructFields::new()), recency: None };
    let page = build_event_batch_with_recency(&[event], Some(StructFields::new()), None, false).unwrap();
    let page = EventBatch::new(page).unwrap().page_events().unwrap();
    let output = StructFields::from([("v".into(), LogicalType::Scalar(ScalarType::FixedSizeBinary(1 << 20)).maybe())]);
    let memory = rtdb_columnar::MemoryBudget::new(1 << 18);

    let error = owned_encoded_batch_with_recency(&[ArrowEvent(page[0].clone())], Some(output), None, false, &memory).unwrap_err();

    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn compaction_preserves_ipc_bytes_of_unions_below_null_parents() {
    let integer = LogicalType::Scalar(ScalarType::I64);
    let text = LogicalType::Scalar(ScalarType::Utf8);
    let union = integer.join(&text).maybe();
    let fields = StructFields::from([
        ("record".into(), LogicalType::Struct(StructFields::from([("x".into(), union.clone())])).maybe()),
        ("fixed".into(), LogicalType::List { kind: ListKind::FixedSizeList(1), element: Box::new(union) }.maybe()),
    ]);
    let event = |id, record, fixed| StoredEvent {
        commit_position: 0,
        entity_key: None,
        iid: EntityId::Integer(id).iid(), system_from: Timestamp(id), valid_time: Interval::ALL, recency: None,
        operation: StoredOperation::Put(StructFields::from([("record".into(), record), ("fixed".into(), fixed)])),
    };
    let record = |value| Value::Struct(StructFields::from([("x".into(), value)]));
    let fixed = |value| Value::FixedSizeList(vec![value].into());
    let events = [
        event(1, record(Value::I64(7)), fixed(Value::Utf8("a".into()))),
        event(2, Value::Null, Value::Null),
        event(3, record(Value::Null), fixed(Value::Null)),
    ];
    let input = build_event_batch_with_recency(&events, Some(fields.clone()), None, false).unwrap();
    let input = EventBatch::new(input).unwrap().page_events().unwrap();
    let selection = [ArrowEvent(input[2].clone()), ArrowEvent(input[1].clone()), ArrowEvent(input[0].clone())];
    let memory = rtdb_columnar::MemoryBudget::new(1 << 18);

    let actual = owned_encoded_batch_with_recency(&selection, Some(fields.clone()), None, false, &memory).unwrap();
    let expected = copy_event_batch_with_recency(&[events[2].clone(), events[1].clone(), events[0].clone()], Some(fields), None, false).unwrap();

    assert_eq!(write_file(actual.batch().schema().as_ref(), std::slice::from_ref(actual.batch())).unwrap(),
        write_file(expected.schema().as_ref(), &[expected]).unwrap());
}

#[test]
fn compaction_preserves_bottom_fields_when_widening_a_visible_struct_to_a_union() {
    let integer = LogicalType::Scalar(ScalarType::I64);
    let text = LogicalType::Scalar(ScalarType::Utf8);
    let document = |ty| StructFields::from([("v".into(), LogicalType::Struct(StructFields::from([("x".into(), ty)])))]);
    let target = document(integer.join(&text).maybe());
    let event = |id, value| StoredEvent {
        commit_position: 0,
        entity_key: None,
        iid: EntityId::Integer(id).iid(), system_from: Timestamp(id), valid_time: Interval::ALL, recency: None,
        operation: StoredOperation::Put(StructFields::from([("v".into(), Value::Struct(StructFields::from([("x".into(), value)])))])),
    };
    let events = [event(1, Value::Undefined), event(2, Value::Utf8("present".into())), event(3, Value::Null)];
    let encode = |events, fields| {
        let batch = build_event_batch_with_recency(events, Some(fields), None, false).unwrap();
        EventBatch::new(batch).unwrap().page_events().unwrap()
    };
    let bottom = encode(&events[..1], document(LogicalType::Nothing));
    let defined = encode(&events[1..], target.clone());
    let selection = [ArrowEvent(bottom[0].clone()), ArrowEvent(defined[0].clone()), ArrowEvent(defined[1].clone())];
    let memory = rtdb_columnar::MemoryBudget::new(1 << 19);

    let actual = owned_encoded_batch_with_recency(&selection, Some(target.clone()), None, false, &memory).unwrap();
    let expected = copy_event_batch_with_recency(&events, Some(target), None, false).unwrap();

    assert_eq!(write_file(actual.batch().schema_ref(), std::slice::from_ref(actual.batch())).unwrap(),
        write_file(expected.schema_ref(), std::slice::from_ref(&expected)).unwrap());
}

#[test]
fn compaction_retries_nested_undefined_encoding_without_allocating_inactive_union_legs() {
    let width = 1 << 20;
    let integer = LogicalType::Scalar(ScalarType::I64);
    let elements = integer.join(&LogicalType::Scalar(ScalarType::FixedSizeBinary(width)));
    let fixed = LogicalType::List { kind: ListKind::FixedSizeList(1), element: Box::new(elements) };
    let union = fixed.join(&integer).maybe();
    let fields = StructFields::from([("v".into(), LogicalType::Struct(StructFields::from([("x".into(), union)])).maybe())]);
    let event = |id, value| StoredEvent {
        commit_position: 0,
        entity_key: None,
        iid: EntityId::Integer(id).iid(), system_from: Timestamp(id), valid_time: Interval::ALL, recency: None,
        operation: StoredOperation::Put(StructFields::from([("v".into(), value)])),
    };
    let events = [
        event(1, Value::Struct(StructFields::from([("x".into(), Value::FixedSizeList(vec![Value::I64(42)].into()))]))),
        event(2, Value::Null),
    ];
    let batch = copy_event_batch_with_recency(&events, Some(fields), None, false).unwrap();
    let reference = write_file(batch.schema_ref(), std::slice::from_ref(&batch)).unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let memory = rtdb_columnar::MemoryBudget::new(1 << 19);
    let mut writer = rtdb_format::ipc::ArrowFileWriter::new_reserved(file.reopen().unwrap(), batch.schema_ref(), &memory).unwrap();
    let body = rtdb_format::ipc::schema_translation_bound(batch.schema_ref()).unwrap()
        + 4 * batch.columns().iter().map(|column| column.get_array_memory_size()).sum::<usize>();
    let fill = memory.reserve(memory.limit_bytes() - memory.used_bytes() - body - 512).unwrap();
    let position = writer.bytes_written();
    let held = memory.used_bytes();

    let error = writer.write_event_page(&batch, &memory).unwrap_err();

    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    assert_eq!(writer.bytes_written(), position);
    assert_eq!(memory.used_bytes(), held);
    drop(fill);
    writer.write_event_page(&batch, &memory).unwrap();
    writer.finish().unwrap();

    assert_eq!(std::fs::read(file.path()).unwrap(), reference);
    assert!(memory.peak_bytes() < width as usize);
    assert_eq!(memory.used_bytes(), 0);
}
