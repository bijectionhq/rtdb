use std::sync::Arc;
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, ListArray, RecordBatch, RunArray, StringViewArray, StructArray, builder::StringDictionaryBuilder, types::{Int8Type, Int32Type, Int64Type}};
use arrow_buffer::NullBuffer;
use arrow_ipc::{CompressionType, writer::IpcWriteOptions};
use arrow_schema::{Field, Schema};
use rtdb_types::{LogicalType, ScalarType, Value};
use crate::ipc;

fn batch() -> RecordBatch {
    let integers: ArrayRef = Arc::new(Int64Array::from((0..9).map(|row| (row % 2 == 0).then_some(row as i64)).collect::<Vec<_>>()));
    let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>((0..9).map(|row| {
        (row % 3 != 0).then_some(vec![Some(row as i64), None])
    })));
    let fields = vec![Field::new("integer", integers.data_type().clone(), true), Field::new("list", lists.data_type().clone(), true)];
    let records: ArrayRef = Arc::new(StructArray::new(fields.into(), vec![integers, lists], Some(NullBuffer::from((0..9).map(|row| row % 4 != 0).collect::<Vec<_>>()))));
    let union_type = LogicalType::from_legs([LogicalType::Scalar(ScalarType::I64), LogicalType::Scalar(ScalarType::Utf8), LogicalType::Null]);
    let values = (0..9).map(|row| match row % 3 { 0 => Value::I64(row), 1 => Value::Utf8(row.to_string()), _ => Value::Null }).collect::<Vec<_>>();
    let unions = rtdb_columnar::build_array(&union_type, &values).unwrap();
    let mut dictionary = StringDictionaryBuilder::<Int8Type>::new();
    for row in 0..9 { if row % 2 == 0 { dictionary.append("entry").unwrap(); } else { dictionary.append_null(); } }
    let dictionary: ArrayRef = Arc::new(dictionary.finish());
    let views: ArrayRef = Arc::new(StringViewArray::from((0..9).map(|row| (row % 2 == 0).then_some("external-string-buffer-value")).collect::<Vec<_>>()));
    let runs: ArrayRef = Arc::new(RunArray::<Int32Type>::try_new(&Int32Array::from(vec![4, 9]), &Int64Array::from(vec![Some(42), None])).unwrap());
    let fields = vec![Field::new("record", records.data_type().clone(), true), rtdb_columnar::to_field("union", &union_type).unwrap(), Field::new("dictionary", dictionary.data_type().clone(), true),
        Field::new("views", views.data_type().clone(), true), Field::new("runs", runs.data_type().clone(), false)];
    RecordBatch::try_new(Arc::new(Schema::new(fields)), vec![records, unions, dictionary, views, runs]).unwrap()
}

fn replace_null_counts(mut bytes: Vec<u8>, count: i64) -> Vec<u8> {
    let mut position = 0;
    let mut patches = Vec::new();
    loop {
        let mut length = i32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
        position += 4;
        if length == -1 { length = i32::from_le_bytes(bytes[position..position + 4].try_into().unwrap()); position += 4; }
        if length == 0 { break; }
        let end = position + length as usize;
        let message = arrow_ipc::root_as_message(&bytes[position..end]).unwrap();
        if let Some(batch) = message.header_as_record_batch().or_else(|| message.header_as_dictionary_batch().and_then(|dictionary| dictionary.data())) {
            for node in batch.nodes().unwrap() { patches.push(node.0.as_ptr() as usize - bytes.as_ptr() as usize + 8); }
        }
        position = end + message.bodyLength() as usize;
    }
    for offset in patches { bytes[offset..offset + 8].copy_from_slice(&count.to_le_bytes()); }
    bytes
}

#[test]
fn unknown_null_counts_are_derived_for_nested_compressed_and_dictionary_buffers() {
    let batch = batch();
    for compression in [None, Some(CompressionType::LZ4_FRAME), Some(CompressionType::ZSTD)] {
        let options = IpcWriteOptions::default().try_with_compression(compression).unwrap();
        let bytes = ipc::write_arrow_stream(batch.schema().as_ref(), &[batch.clone()], options).unwrap();
        let bytes = replace_null_counts(bytes, -1);
        let memory = rtdb_columnar::MemoryBudget::new(72 << 20);
        let owned = ipc::OwnedStream::read(&bytes, &memory).unwrap();
        assert_eq!(owned.pages()[0].batch(), &batch, "{compression:?}");
        drop(owned);
        assert_eq!(memory.used_bytes(), 0);
        let (_, pages) = ipc::read_stream(bytes.into()).unwrap();
        assert_eq!(pages, vec![batch.clone()], "{compression:?}");
        for column in pages[0].columns() { column.to_data().validate_full().unwrap(); }
    }
}

#[test]
fn null_counts_below_the_unknown_sentinel_remain_invalid() {
    let batch = batch();
    let bytes = ipc::write_arrow_stream(batch.schema().as_ref(), &[batch], IpcWriteOptions::default()).unwrap();
    assert!(ipc::read_stream(replace_null_counts(bytes, -2).into()).is_err());
}

#[test]
fn zero_length_compressed_validity_represents_an_omitted_bitmap() {
    use std::collections::HashMap;
    use arrow_buffer::Buffer;
    use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator};
    let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3), None]));
    let schema = Arc::new(Schema::new(vec![Field::new("value", array.data_type().clone(), true)]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
    for codec in [CompressionType::LZ4_FRAME, CompressionType::ZSTD] {
        let options = IpcWriteOptions::default().try_with_compression(Some(codec)).unwrap();
        let (_, mut encoded) = IpcDataGenerator::default().encoded_batch(&batch, &mut DictionaryTracker::new(false), &options).unwrap();
        let message = arrow_ipc::root_as_message(&encoded.ipc_message).unwrap();
        let record = message.header_as_record_batch().unwrap();
        let node = record.nodes().unwrap().get(0);
        let count_at = node.0.as_ptr() as usize - encoded.ipc_message.as_ptr() as usize + 8;
        let validity_at = record.buffers().unwrap().get(0).offset() as usize;
        encoded.ipc_message[count_at..count_at + 8].copy_from_slice(&(-1i64).to_le_bytes());
        encoded.arrow_data[validity_at..validity_at + 8].copy_from_slice(&0i64.to_le_bytes());
        super::normalize(&mut encoded.ipc_message, &encoded.arrow_data, &schema).unwrap();
        let message = arrow_ipc::root_as_message(&encoded.ipc_message).unwrap();
        let decoded = arrow_ipc::reader::read_record_batch(&Buffer::from(encoded.arrow_data), message.header_as_record_batch().unwrap(), Arc::clone(&schema), &HashMap::new(), None, &message.version()).unwrap();
        assert_eq!(decoded.num_rows(), 4);
        assert_eq!(decoded.column(0).null_count(), 0);
        decoded.column(0).to_data().validate_full().unwrap();
    }
}
