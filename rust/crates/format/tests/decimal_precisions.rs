use rtdb_types::StructFields;
use std::{collections::BTreeMap, io::{Read, Seek, SeekFrom}, sync::Arc};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use rtdb_columnar::{MemoryBudget, copy_array, from_field, read_value, to_field};
use rtdb_format::{columns, ipc};
use rtdb_types::{DecimalValue, LogicalType, RunEndWidth, ScalarType, Value};

fn decimal(precision: i32, scale: i32, width: u16) -> (LogicalType, Value) {
    let logical = LogicalType::Scalar(ScalarType::Decimal { precision, scale, bit_width: width });
    let bytes = arrow_buffer::i256::from_i128(-123).to_be_bytes();
    let value = Value::Decimal(DecimalValue::new(bytes[32-usize::from(width/8)..].to_vec(), precision, scale, width).unwrap());
    (logical, value)
}
fn batch(logical: &LogicalType, values: &[Value]) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(vec![to_field("value", logical).unwrap()])), vec![copy_array(logical, values).unwrap()]).unwrap()
}
fn check(page: &RecordBatch, logical: &LogicalType, values: &[Value]) {
    assert_eq!(from_field(page.schema().field(0)).unwrap(), *logical);
    assert_eq!(page.num_rows(), values.len());
    page.column(0).to_data().validate_full().unwrap();
    for (row, expected) in values.iter().enumerate() { assert_eq!(read_value(page.schema().field(0), page.column(0).as_ref(), row).unwrap(), *expected); }
}

#[test]
fn column_meta_and_arrow_preserve_signed_precision_and_raw_coefficients() {
    for width in [32, 64, 128, 256] {
        for precision in [i32::MIN, -1, 0, 77, 255, 256, i32::MAX] {
            for scale in [0, i32::MIN, i32::MAX] {
                let (logical, value) = decimal(precision, scale, width); let logical = logical.maybe();
                assert_eq!(columns::type_from_proto(&columns::type_to_proto(&logical).unwrap()).unwrap(), logical);
                for values in [vec![], vec![Value::Null], vec![Value::Null, value]] {
                    let page = batch(&logical, &values); let schema = page.schema();
                    let bytes = ipc::write_stream(&schema, std::slice::from_ref(&page)).unwrap();
                    let header = ipc::write_schema_message(&schema).unwrap();
                    let wire = arrow_ipc::root_as_message(&header[8..]).unwrap().header_as_schema().unwrap().fields().unwrap().get(0).type_as_decimal().unwrap();
                    assert_eq!((wire.precision(), wire.scale(), wire.bitWidth()), (precision, scale, i32::from(width)));
                    let streamed = ipc::write_stream_to(&schema, [&page], Vec::new()).unwrap();
                    check(&ipc::ArrowStream::open(streamed.into(), None).unwrap().next().unwrap().unwrap(), &logical, &values);
                    check(&ipc::ArrowStream::open(bytes.into(), None).unwrap().next().unwrap().unwrap(), &logical, &values);
                    let bytes = ipc::write_file(&schema, std::slice::from_ref(&page)).unwrap();
                    let mut file = ipc::ArrowFile::open(bytes.clone().into(), None).unwrap();
                    check(&file.read_page(0).unwrap(), &logical, &values);
                    let memory = MemoryBudget::new(16 << 20);
                    let mut writer = ipc::ArrowFileWriter::new_reserved(tempfile::tempfile().unwrap(), &schema, &memory).unwrap();
                    assert!(memory.used_bytes() > 0); writer.write(&page).unwrap();
                    let mut sink = writer.finish().unwrap(); assert_eq!(memory.used_bytes(), 0);
                    sink.seek(SeekFrom::Start(0)).unwrap(); let mut actual = Vec::new(); sink.read_to_end(&mut actual).unwrap();
                    assert_eq!(actual, bytes);
                }
            }
        }
    }
}

#[test]
fn nested_unions_and_run_ends_preserve_independent_precision_carriers() {
    let (first_type, first) = decimal(256, i32::MIN, 128);
    let (second_type, second) = decimal(i32::MAX, i32::MAX, 128);
    let union = LogicalType::from_legs([first_type, second_type, LogicalType::Null]);
    let structure = LogicalType::Struct(StructFields::from([("items".into(), LogicalType::list(union))]));
    let logical = LogicalType::RunEndEncoded { width: RunEndWidth::I32, value: Box::new(structure) };
    let value = Value::Struct(StructFields::from([("items".into(), Value::List(vec![first, Value::Null, second]))]));
    let values = [value.clone(), value]; let page = batch(&logical, &values);
    let bytes = ipc::write_stream(&page.schema(), &[page]).unwrap();
    check(&ipc::ArrowStream::open(bytes.into(), None).unwrap().next().unwrap().unwrap(), &logical, &values);
}

#[test]
fn differing_full_precisions_are_rejected_before_incremental_writer_effects() {
    let nested = |precision| {
        let (logical, value) = decimal(precision, 129, 128);
        batch(&LogicalType::Struct(StructFields::from([("decimal".into(), logical)])), &[Value::Struct(StructFields::from([("decimal".into(), value)]))])
    };
    let declared = nested(256); let different = nested(257); let schema = declared.schema();
    assert!(ipc::write_file(&schema, std::slice::from_ref(&different)).is_err());
    assert!(ipc::write_stream(&schema, std::slice::from_ref(&different)).is_err());
    assert!(ipc::write_stream_to(&schema, [&different], Vec::new()).is_err());
    let memory = MemoryBudget::new(16 << 20);
    let mut writer = ipc::ArrowFileWriter::new_reserved(tempfile::tempfile().unwrap(), &schema, &memory).unwrap();
    let before = (writer.bytes_written(), memory.used_bytes());
    assert!(writer.write(&different).is_err()); assert_eq!((writer.bytes_written(), memory.used_bytes()), before);
    writer.write(&declared).unwrap(); drop(writer.finish().unwrap()); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn narrow_precision_schema_and_stream_bytes_match_arrow_writer() {
    for precision in [0, 1, 38, 77, 255] {
        let (logical, _) = decimal(precision, 1, 128); let page = batch(&logical.maybe(), &[Value::Null]);
        let options = arrow_ipc::writer::IpcWriteOptions::default();
        let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(Vec::new(), &page.schema(), options.clone()).unwrap();
        writer.write(&page).unwrap(); writer.finish().unwrap();
        assert_eq!(ipc::write_arrow_stream(&page.schema(), &[page], options).unwrap(), writer.into_inner().unwrap());
    }
}

#[test]
fn stored_event_copy_keeps_decimal_metadata_and_rejects_relabeling() {
    use rtdb_format::data::{EventBatch, StoredEvent, StoredOperation, build_event_batch, copy_event_batch, copy_event_batch_with_recency};
    use rtdb_types::{Iid, Interval, Timestamp};
    for precision in [i32::MIN, 0, 1, 256, i32::MAX] {
        let (logical, value) = decimal(precision, i32::MIN, 128);
        let fields = BTreeMap::from([("value".into(), logical.clone())]);
        let events = [StoredEvent { commit_position: 0, entity_key: None, iid: Iid([42; 16]), system_from: Timestamp(10), valid_time: Interval::ALL,
            operation: StoredOperation::Put(StructFields::from([("value".into(), value)])), recency: None }];
        if precision < 3 { assert!(build_event_batch(&events, Some(fields.clone().into()), None).is_err()); }
        let page = copy_event_batch(&events, Some(fields.clone().into()), Some(RunEndWidth::I32)).unwrap();
        assert_eq!(EventBatch::new(page).unwrap().read_event(0).unwrap(), events[0]);
        let empty = copy_event_batch_with_recency(&[], Some(fields.clone().into()), None, true).unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert!(empty.schema().index_of("_recency").is_ok());
        let wrong = BTreeMap::from([("value".into(), LogicalType::Scalar(ScalarType::Decimal { precision: precision.wrapping_add(1), scale: i32::MIN, bit_width: 128 }))]);
        assert!(copy_event_batch(&events, Some(wrong.into()), None).is_err());
        assert!(copy_event_batch(&events, Some(StructFields::new()), None).is_err());
    }
}
