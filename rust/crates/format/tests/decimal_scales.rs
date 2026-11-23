use rtdb_types::StructFields;
use std::{io::{Read, Seek, SeekFrom}, sync::Arc};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use rtdb_columnar::{build_array, from_field, read_value, to_field};
use rtdb_format::{columns, ipc};
use rtdb_types::{DecimalValue, ListKind, LogicalType, ScalarType, Value};

fn decimal(width: u16, scale: i32) -> (LogicalType, Value) {
    let ty = LogicalType::Scalar(ScalarType::Decimal { precision: 3, scale, bit_width: width });
    let coefficient = arrow_buffer::i256::from_i128(-123).to_be_bytes();
    let value = Value::Decimal(DecimalValue::new(coefficient[32-usize::from(width/8)..].to_vec(), 3, scale, width).unwrap());
    (ty, value)
}
fn batch(ty: &LogicalType, values: &[Value]) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(vec![to_field("value", ty).unwrap()])), vec![build_array(ty, values).unwrap()]).unwrap()
}
fn check(batch: &RecordBatch, ty: &LogicalType, expected: &[Value]) {
    assert_eq!(from_field(batch.schema().field(0)).unwrap(), *ty);
    assert_eq!(batch.num_rows(), expected.len());
    batch.column(0).to_data().validate_full().unwrap();
    for (row, value) in expected.iter().enumerate() { assert_eq!(read_value(batch.schema().field(0), batch.column(0).as_ref(), row).unwrap(), *value); }
}

#[test]
fn decimal_vector_all_widths_retain_full_scale_across_buffered_and_streaming_ipc() {
    for width in [32, 64, 128, 256] {
        for scale in [i32::MIN, -129, -128, 0, 127, 128, i32::MAX] {
            let (ty, value) = decimal(width, scale); let ty = ty.maybe();
            let values = vec![Value::Null, value]; let page = batch(&ty, &values);
            let schema = page.schema(); let pages = [page.clone(), page.clone()];
            let bytes = ipc::write_file(&schema, &pages).unwrap();
            let header = arrow_ipc::root_as_message(&bytes[16..]).unwrap();
            assert_eq!(header.header_as_schema().unwrap().fields().unwrap().get(0).type_as_decimal().unwrap().scale(), scale);
            let length = arrow_ipc::reader::read_footer_length(bytes[bytes.len()-10..].try_into().unwrap()).unwrap();
            let footer = arrow_ipc::root_as_footer(&bytes[bytes.len()-10-length..bytes.len()-10]).unwrap();
            assert_eq!(footer.schema().unwrap().fields().unwrap().get(0).type_as_decimal().unwrap().scale(), scale);
            assert!(ipc::file_allocation_bound(&bytes).unwrap() > bytes.len());
            let mut file = ipc::ArrowFile::open(bytes.clone().into(), Some(vec![0])).unwrap();
            assert_eq!(file.page_count(), 2); check(&file.read_page(1).unwrap(), &ty, &values); check(&file.read_page(0).unwrap(), &ty, &values);
            let mut sink = ipc::ArrowFileWriter::new(tempfile::tempfile().unwrap(), &schema).unwrap();
            for page in &pages { sink.write(page).unwrap(); }
            let mut sink = sink.finish().unwrap(); sink.seek(SeekFrom::Start(0)).unwrap();
            let mut actual = Vec::new(); sink.read_to_end(&mut actual).unwrap(); assert_eq!(actual, bytes);
            let bytes = ipc::write_stream(&schema, &pages).unwrap();
            let mut streamed = ipc::ArrowStream::open(ipc::write_stream_to(&schema, &pages, Vec::new()).unwrap().into(), None).unwrap();
            check(&streamed.next().unwrap().unwrap(), &ty, &values); check(&streamed.next().unwrap().unwrap(), &ty, &values); assert!(streamed.next().is_none());
            assert!(ipc::stream_allocation_bound(&bytes).unwrap() > bytes.len());
            let mut stream = ipc::ArrowStream::open(bytes.into(), None).unwrap();
            check(&stream.next().unwrap().unwrap(), &ty, &values); check(&stream.next().unwrap().unwrap(), &ty, &values); assert!(stream.next().is_none());
            assert_eq!(columns::type_from_proto(&columns::type_to_proto(&ty).unwrap()).unwrap(), ty);
        }
    }
}

#[test]
fn nested_union_list_and_struct_decimals_retain_independent_scales() {
    let (first_type, first) = decimal(128, i32::MIN); let (second_type, second) = decimal(128, i32::MAX);
    let union = LogicalType::from_legs([first_type, second_type, LogicalType::Null]);
    let ty = LogicalType::Struct(StructFields::from([("items".into(), LogicalType::List { kind: ListKind::List, element: Box::new(union) })]));
    let values = vec![Value::Struct(StructFields::from([("items".into(), Value::list(vec![first, Value::Null, second]))]))];
    let page = batch(&ty, &values);
    let bytes = ipc::write_stream(&page.schema(), &[page]).unwrap();
    check(&ipc::ArrowStream::open(bytes.into(), None).unwrap().next().unwrap().unwrap(), &ty, &values);
}

#[test]
fn narrow_scale_standard_stream_bytes_are_unchanged() {
    for scale in [-128, -1, 0, 1, 127] {
        let (ty, value) = decimal(128, scale); let page = batch(&ty, &[value]);
        let options = arrow_ipc::writer::IpcWriteOptions::default();
        let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(Vec::new(), &page.schema(), options.clone()).unwrap();
        writer.write(&page).unwrap(); writer.finish().unwrap();
        assert_eq!(ipc::write_arrow_stream(&page.schema(), std::slice::from_ref(&page), options).unwrap(), writer.into_inner().unwrap());
    }
}

#[test]
fn zero_precision_wide_scale_empty_and_null_pages_preserve_metadata() {
    for scale in [i32::MIN, i32::MAX] {
        let ty = LogicalType::Scalar(ScalarType::Decimal { precision: 0, scale, bit_width: 128 }).maybe();
        for values in [vec![], vec![Value::Null]] {
            let page = batch(&ty, &values); let bytes = ipc::write_stream(&page.schema(), &[page]).unwrap();
            check(&ipc::ArrowStream::open(bytes.into(), None).unwrap().next().unwrap().unwrap(), &ty, &values);
        }
    }
}

#[test]
fn nested_wide_scale_pages_cannot_be_reinterpreted_under_another_schema() {
    let nested = |scale| {
        let (ty, value) = decimal(128, scale);
        let ty = LogicalType::Struct(StructFields::from([("value".into(), ty)]));
        batch(&ty, &[Value::Struct(StructFields::from([("value".into(), value)]))])
    };
    let declared = nested(129); let different = nested(130);
    let schema = declared.schema();
    assert!(ipc::write_file(&schema, std::slice::from_ref(&different)).is_err());
    assert!(ipc::write_stream(&schema, std::slice::from_ref(&different)).is_err());
    assert!(ipc::write_stream_to(&schema, [&declared, &different], Vec::new()).is_err());
    let mut writer = ipc::ArrowFileWriter::new(tempfile::tempfile().unwrap(), &schema).unwrap();
    writer.write(&declared).unwrap(); let before = writer.bytes_written();
    assert!(writer.write(&different).is_err()); assert_eq!(writer.bytes_written(), before);
    let file = writer.finish().unwrap(); assert_eq!(ipc::ArrowFile::from_reader(file, None).unwrap().page_count(), 1);
}
