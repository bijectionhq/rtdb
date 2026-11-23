use rtdb_types::StructFields;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use rtdb_format::ipc::{ArrowFileWriter, inspect_file_reserved, write_file, write_stream, write_stream_to};
use rtdb_types::Anomaly;

fn page() -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])), vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap()
}

#[test]
fn owned_ipc_reuses_aligned_message_values_and_retains_their_reservation() {
    use arrow_buffer::Buffer;
    use rtdb_columnar::MemoryBudget;
    use rtdb_format::ipc::OwnedStream;

    let batch = page();
    let encoded = Buffer::from(write_stream(batch.schema_ref(), std::slice::from_ref(&batch)).unwrap());
    let start = encoded.as_ptr() as usize;
    let within = start..start + encoded.len();
    let memory = MemoryBudget::new(1 << 20);
    let stream = OwnedStream::read_buffer(encoded.clone(), &memory).unwrap();
    let values = Arc::clone(stream.pages()[0].batch().column(0));
    drop(stream);
    let values_view = values.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(within.contains(&(values_view.values().as_ptr() as usize)));
    assert_eq!(values_view.values().as_ref(), &[1, 2, 3]);
    assert!(memory.used_bytes() > 0);
    drop(values);
    assert_eq!(memory.used_bytes(), 0);
    assert!(OwnedStream::read_buffer(encoded.slice_with_length(0, encoded.len() - 8), &memory).is_err());
}

#[test]
fn owned_stream_admits_wide_arrow_batches_without_row_value_materialization() {
    use arrow_array::ArrayRef;
    use rtdb_columnar::MemoryBudget;
    use rtdb_format::ipc::OwnedStream;

    let fields = (0..1000).map(|index| Field::new(format!("v{index}"), DataType::Int64, true)).collect::<Vec<_>>();
    let columns = (0..1000).map(|index| Arc::new(Int64Array::from_iter((0..100).map(|row| (row == index / 10).then_some(i64::from(index))))) as ArrayRef).collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let bytes = write_stream(&batch.schema(), std::slice::from_ref(&batch)).unwrap();
    let memory = MemoryBudget::new(1 << 25);
    let stream = OwnedStream::read(&bytes, &memory).unwrap();
    let column = Arc::clone(stream.pages()[0].batch().column(992));
    drop(stream);
    assert_eq!(column.as_any().downcast_ref::<Int64Array>().unwrap().value(99), 992);
    assert!(memory.used_bytes() > 0);
    drop(column);
    assert_eq!(memory.used_bytes(), 0);
    assert!(OwnedStream::read(&bytes[..bytes.len() - 8], &memory).is_err());
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn file_and_stream_decoding_admit_logical_nulls_in_nested_dense_unions() {
    use std::collections::BTreeMap;
    use rtdb_format::{data::{EventBatch, StoredEvent, StoredOperation, build_event_batch}, ipc::{ArrowFile, ArrowStream}};
    use rtdb_types::{EntityId, Interval, LogicalType, ScalarType, Timestamp, Value};

    let events = [Value::Null, Value::I64(7), Value::Utf8("mixed".into())].into_iter().enumerate()
        .map(|(index, value)| StoredEvent { position: None,
            origin: None,
            commit_position: 0,
            entity_key: None,
            iid: EntityId::Integer(index as i64).iid(),
            system_from: Timestamp(100),
            valid_time: Interval::ALL,
            operation: StoredOperation::Put(StructFields::from([("v".into(), value)])),
            recency: None,
        }).collect::<Vec<_>>();
    let field_type = LogicalType::from_legs([LogicalType::Null, LogicalType::Scalar(ScalarType::I64), LogicalType::Scalar(ScalarType::Utf8)]);
    let fields = BTreeMap::from([("v".into(), field_type)]);
    let batch = build_event_batch(&events, Some(fields.into()), None).unwrap();
    let schema = batch.schema();
    let DataType::Union(operations, _) = schema.field_with_name("op").unwrap().data_type() else { panic!("event operations") };
    let put = operations.iter().find(|(_, field)| field.name() == "put").unwrap().1;
    let DataType::Struct(fields) = put.data_type() else { panic!("event document") };
    assert!(!fields.find("v").unwrap().1.is_nullable());

    let mut file = ArrowFile::open(Arc::from(write_file(&schema, std::slice::from_ref(&batch)).unwrap()), None).unwrap();
    let mut stream = ArrowStream::open(Arc::from(write_stream(&schema, &[batch]).unwrap()), None).unwrap();
    for (decoded, admitted_schema) in [(file.read_page(0).unwrap(), file.schema()), (stream.next().unwrap().unwrap(), stream.schema())] {
        assert_eq!(decoded.schema(), admitted_schema);
        let decoded = EventBatch::new(decoded).unwrap();
        for (index, expected) in events.iter().enumerate() {
            assert_eq!(&decoded.read_event(index).unwrap(), expected);
        }
    }
}

#[test]
fn decoding_a_map_preserves_the_nonnullable_union_key_field() {
    use arrow_array::{Array, MapArray, NullArray, StringArray, StructArray, UnionArray};
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    use arrow_schema::{Fields, UnionFields};
    use rtdb_format::ipc::{ArrowFile, ArrowStream};
    use rtdb_types::Value;

    let legs = UnionFields::try_new([1, 4, 7], [Field::new("null", DataType::Null, true), Field::new("i64", DataType::Int64, false), Field::new("utf8", DataType::Utf8, false)]).unwrap();
    let keys = UnionArray::try_new(legs, ScalarBuffer::from(vec![4, 7]), Some(ScalarBuffer::from(vec![0, 0])), vec![
        Arc::new(NullArray::new(0)), Arc::new(Int64Array::from(vec![7])), Arc::new(StringArray::from(vec!["key"])),
    ]).unwrap();
    let fields = Fields::from(vec![Field::new("key", keys.data_type().clone(), false), Field::new("value", DataType::Int64, false)]);
    let entry_field = Arc::new(Field::new("entries", DataType::Struct(fields.clone()), false));
    let entries = StructArray::try_new(fields, vec![Arc::new(keys), Arc::new(Int64Array::from(vec![1, 2]))], None).unwrap();
    let map = MapArray::try_new(entry_field, OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])), entries, None, false).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("m", map.data_type().clone(), false)]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(map)]).unwrap();
    let mut file = ArrowFile::open(Arc::from(write_file(&schema, std::slice::from_ref(&batch)).unwrap()), None).unwrap();
    let mut stream = ArrowStream::open(Arc::from(write_stream(&schema, &[batch]).unwrap()), None).unwrap();
    for batch in [file.read_page(0).unwrap(), stream.next().unwrap().unwrap()] {
        assert_eq!(batch.schema(), schema);
        assert_eq!(rtdb_columnar::read_value(schema.field(0), batch.column(0).as_ref(), 0).unwrap(), Value::Map {
            entries: vec![(Value::I64(7), Value::I64(1)), (Value::Utf8("key".into()), Value::I64(2))], sorted: false,
        });
    }
}

#[test]
fn incremental_sinks_match_buffered_jvm_end_markers() {
    let batch = page();
    let expected = write_file(&batch.schema(), std::slice::from_ref(&batch)).unwrap();
    let mut writer = ArrowFileWriter::new(tempfile::tempfile().unwrap(), &batch.schema()).unwrap();
    writer.write(&batch).unwrap();
    let mut file = writer.finish().unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut actual = Vec::new();
    file.read_to_end(&mut actual).unwrap();
    assert_eq!(actual, expected);
    let streamed = write_stream_to(&batch.schema(), [&batch], Vec::new()).unwrap();
    assert_eq!(&streamed[streamed.len() - 4..], [0; 4], "the stream ends with the reference's single zero word");
    let mut decoded = rtdb_format::ipc::ArrowStream::open(streamed.into(), None).unwrap();
    assert_eq!(decoded.next().unwrap().unwrap(), batch);
    assert!(decoded.next().is_none());
}

#[test]
fn output_limit_rejects_growth_before_touching_the_file() {
    let batch = page();
    let file = tempfile::tempfile().unwrap();
    let observer = file.try_clone().unwrap();
    let mut writer = ArrowFileWriter::new(file, &batch.schema()).unwrap();
    let initial = writer.bytes_written();
    writer.set_byte_limit(initial).unwrap();
    assert!(writer.write(&batch).is_err());
    assert_eq!(observer.metadata().unwrap().len(), initial);
}

#[test]
fn reserved_file_writer_precharges_schema_inventory_and_footer_before_writes() {
    let ty = rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Decimal { precision: 1, scale: i32::MAX, bit_width: 128 });
    let schema = Schema::new(vec![rtdb_columnar::to_field("value", &ty).unwrap()]);
    let schema_bound = rtdb_format::ipc::schema_translation_bound(&schema).unwrap();
    let memory = rtdb_columnar::MemoryBudget::new(schema_bound - 1);
    let file = tempfile::tempfile().unwrap(); let observer = file.try_clone().unwrap();
    assert!(ArrowFileWriter::new_reserved(file, &schema, &memory).is_err());
    assert_eq!(observer.metadata().unwrap().len(), 0); assert_eq!(memory.used_bytes(), 0);
    let memory = rtdb_columnar::MemoryBudget::new(schema_bound + 512);
    let mut writer = ArrowFileWriter::new_reserved(tempfile::tempfile().unwrap(), &schema, &memory).unwrap();
    assert_eq!(memory.used_bytes(), schema_bound);
    let page = RecordBatch::new_empty(Arc::new(schema.clone())); writer.write(&page).unwrap();
    assert_eq!(memory.used_bytes(), schema_bound + 512);
    assert!(writer.finish().is_err()); assert_eq!(memory.used_bytes(), 0);
    let memory = rtdb_columnar::MemoryBudget::new(8 * schema_bound);
    let mut writer = ArrowFileWriter::new_reserved(tempfile::tempfile().unwrap(), &schema, &memory).unwrap();
    writer.write(&page).unwrap(); let file = writer.finish().unwrap();
    assert_eq!(memory.used_bytes(), 0);
    let decoded = rtdb_format::ipc::ArrowFile::from_reader(file, None).unwrap();
    assert_eq!(rtdb_columnar::from_field(decoded.schema().field(0)).unwrap(), ty);
}

#[test]
fn inspection_must_acquire_a_guard_before_allocating_a_footer() {
    let batch = page();
    let bytes = write_file(&batch.schema(), &[batch]).unwrap();
    let mut calls = 0;
    let result = inspect_file_reserved(&mut std::io::Cursor::new(bytes), |size| -> rtdb_types::Result<()> {
        assert!(size > 0);
        calls += 1;
        Err(Anomaly::fault("test/reservation", "budget exhausted"))
    });
    assert_eq!(result.unwrap_err().code, "test/reservation");
    assert_eq!(calls, 1);
}

#[test]
fn allocation_inspection_rejects_truncated_file_blocks_and_stream_messages() {
    let batch = page();
    let file = write_file(&batch.schema(), std::slice::from_ref(&batch)).unwrap();
    let stream = write_stream(&batch.schema(), &[batch]).unwrap();
    assert!(rtdb_format::ipc::file_allocation_bound(&file).unwrap() > file.len());
    assert!(rtdb_format::ipc::stream_allocation_bound(&stream).unwrap() > stream.len());
    for length in [0, 1, 3, 9, file.len() - 1] { assert!(rtdb_format::ipc::file_allocation_bound(&file[..length]).is_err()); }
    assert!(rtdb_format::ipc::stream_allocation_bound(&[0xff, 0xff, 0xff, 0xff]).is_err());
    assert!(rtdb_format::ipc::stream_allocation_bound(&[0xff, 0xff, 0xff, 0x7f]).is_err());
}

#[test]
fn compressed_buffer_declared_size_is_budgeted_before_decoding() {
    use arrow_ipc::writer::{FileWriter, IpcWriteOptions};
    let batch = page();
    let options = IpcWriteOptions::default().try_with_compression(Some(arrow_ipc::CompressionType::ZSTD)).unwrap();
    let mut writer = FileWriter::try_new_with_options(Vec::new(), &batch.schema(), options).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    let mut bytes = writer.into_inner().unwrap();
    assert!(rtdb_format::ipc::file_allocation_bound(&bytes).unwrap() > bytes.len());
    let layout = inspect_file_reserved(&mut std::io::Cursor::new(&bytes), |_| Ok(())).unwrap();
    assert!(layout.page_decoding_bounds[0] >= 1 << 20);
    let prefix = {
        let footer_length = arrow_ipc::reader::read_footer_length(bytes[bytes.len() - 10..].try_into().unwrap()).unwrap();
        let footer = arrow_ipc::root_as_footer(&bytes[bytes.len() - 10 - footer_length..bytes.len() - 10]).unwrap();
        let block = footer.recordBatches().unwrap().get(0);
        let metadata_start = block.offset() as usize;
        let body_start = metadata_start + block.metaDataLength() as usize;
        let prefix_size = if bytes[metadata_start..].starts_with(&[0xff; 4]) { 8 } else { 4 };
        let message = arrow_ipc::root_as_message(&bytes[metadata_start + prefix_size..body_start]).unwrap();
        let buffer = message.header_as_record_batch().unwrap().buffers().unwrap().iter().find(|buffer| buffer.length() > 0).unwrap();
        body_start + buffer.offset() as usize
    };
    bytes[prefix..prefix + 8].copy_from_slice(&i64::MAX.to_le_bytes());
    assert!(rtdb_format::ipc::file_allocation_bound(&bytes).is_err());
    assert!(inspect_file_reserved(&mut std::io::Cursor::new(bytes), |_| Ok(())).is_err());
}

#[test]
fn file_bounds_include_padding_between_the_message_header_and_body() {
    let batch = page();
    let mut bytes = write_file(&batch.schema(), std::slice::from_ref(&batch)).unwrap();
    let original = inspect_file_reserved(&mut std::io::Cursor::new(&bytes), |_| Ok(())).unwrap();
    let original_total = rtdb_format::ipc::file_allocation_bound(&bytes).unwrap();
    let padding = 1 << 20;
    let (body_start, block_position, mut block) = {
        let footer_length = arrow_ipc::reader::read_footer_length(bytes[bytes.len() - 10..].try_into().unwrap()).unwrap();
        let footer = arrow_ipc::root_as_footer(&bytes[bytes.len() - 10 - footer_length..bytes.len() - 10]).unwrap();
        let block = footer.recordBatches().unwrap().get(0);
        (block.offset() as usize + block.metaDataLength() as usize, block.0.as_ptr() as usize - bytes.as_ptr() as usize, *block)
    };
    bytes.splice(body_start..body_start, std::iter::repeat_n(0, padding));
    block.set_metaDataLength(block.metaDataLength() + padding as i32);
    bytes[block_position + padding..block_position + padding + block.0.len()].copy_from_slice(&block.0);
    let padded = inspect_file_reserved(&mut std::io::Cursor::new(&bytes), |_| Ok(())).unwrap();
    assert_eq!(padded.page_decoding_bounds[0] - original.page_decoding_bounds[0], padding * 16);
    assert_eq!(rtdb_format::ipc::file_allocation_bound(&bytes).unwrap() - original_total, padding * 16);
    let mut reader = rtdb_format::ipc::ArrowFile::open(bytes.into(), None).unwrap();
    assert_eq!(reader.read_page(0).unwrap(), batch);
}

#[test]
fn compressed_file_bounds_cover_dictionaries_and_pages_before_decoding() {
    use arrow_array::{builder::StringDictionaryBuilder, types::Int32Type};
    use arrow_ipc::writer::{FileWriter, IpcWriteOptions};
    use rtdb_format::ipc::ArrowFile;
    for codec in [arrow_ipc::CompressionType::LZ4_FRAME, arrow_ipc::CompressionType::ZSTD] {
        let mut dictionary = StringDictionaryBuilder::<Int32Type>::new();
        for _ in 0..1024 { dictionary.append("repeated value").unwrap(); }
        let batch = RecordBatch::try_from_iter(vec![("value", Arc::new(dictionary.finish()) as arrow_array::ArrayRef)]).unwrap();
        let options = IpcWriteOptions::default().try_with_compression(Some(codec)).unwrap();
        let mut writer = FileWriter::try_new_with_options(Vec::new(), &batch.schema(), options).unwrap();
        writer.write(&batch).unwrap(); writer.finish().unwrap();
        let bytes = writer.into_inner().unwrap();
        let layout = inspect_file_reserved(&mut std::io::Cursor::new(&bytes), |_| Ok(())).unwrap();
        assert!(!layout.uncompressed);
        assert!(layout.dictionary_decoding_bound > 0);
        assert_eq!(layout.page_decoding_bounds.len(), 1);
        let opening = layout.opening_allocation_bound().unwrap();
        let page_bound = layout.page_decoding_bounds[0];
        let memory = rtdb_columnar::MemoryBudget::new(opening + page_bound);
        let opening_charge = memory.reserve(opening).unwrap();
        let mut reader = ArrowFile::from_inspected_reader(std::io::Cursor::new(bytes), None, layout).unwrap();
        assert_eq!(reader.page_allocation_bound(0).unwrap(), page_bound);
        let page_charge = memory.reserve(page_bound).unwrap();
        let decoded = reader.read_page(0).unwrap();
        assert_eq!(decoded, batch);
        drop(decoded); drop(page_charge); drop(reader); drop(opening_charge);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn lz4_payload_cannot_exceed_a_forged_small_arrow_length() {
    use arrow_ipc::writer::{FileWriter, IpcWriteOptions};
    let batch = RecordBatch::try_from_iter(vec![("value", Arc::new(Int64Array::from(vec![0; 65_536])) as arrow_array::ArrayRef)]).unwrap();
    let options = IpcWriteOptions::default().try_with_compression(Some(arrow_ipc::CompressionType::LZ4_FRAME)).unwrap();
    let mut writer = FileWriter::try_new_with_options(Vec::new(), &batch.schema(), options).unwrap();
    writer.write(&batch).unwrap(); writer.finish().unwrap();
    let mut bytes = writer.into_inner().unwrap();
    let prefix = {
        let footer_length = arrow_ipc::reader::read_footer_length(bytes[bytes.len() - 10..].try_into().unwrap()).unwrap();
        let footer = arrow_ipc::root_as_footer(&bytes[bytes.len() - 10 - footer_length..bytes.len() - 10]).unwrap();
        let block = footer.recordBatches().unwrap().get(0);
        let start = block.offset() as usize; let body = start + block.metaDataLength() as usize;
        let header = start + if bytes[start..].starts_with(&[0xff; 4]) { 8 } else { 4 };
        let message = arrow_ipc::root_as_message(&bytes[header..body]).unwrap();
        let buffer = message.header_as_record_batch().unwrap().buffers().unwrap().iter().find(|buffer| buffer.length() > 0).unwrap();
        body + buffer.offset() as usize
    };
    bytes[prefix..prefix + 8].copy_from_slice(&1i64.to_le_bytes());
    let mut reader = rtdb_format::ipc::ArrowFile::open(bytes.into(), None).unwrap();
    assert!(reader.read_page(0).unwrap_err().message.as_deref().is_some_and(|message| message.contains("LZ4 data exceeds its declared Arrow buffer length")));
}

#[test]
fn empty_dense_union_stream_keeps_offset_buffers_aligned() {
    let fields = arrow_schema::UnionFields::try_new([0], [Field::new("n", DataType::Int64, true)]).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("op", DataType::Union(fields, arrow_schema::UnionMode::Dense), false)]));
    let batch = RecordBatch::new_empty(Arc::clone(&schema));
    let bytes = write_stream(&schema, &[batch]).unwrap();
    let mut reader = rtdb_format::ipc::ArrowStream::open(Arc::from(bytes), None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.schema(), schema);
    assert!(reader.next().is_none());
}

#[test]
fn spatial_arrow_projects_iids_and_put_children_from_the_same_file() {
    use arrow_buffer::Buffer;
    use rtdb_format::{data::{EncodedEvent, EventBatch, PutColumn, StoredEvent, StoredOperation, build_event_batch}, ipc::{ArrowFile, PageDecoder}};
    use rtdb_types::{Iid, Interval, LogicalType, RunEndWidth, ScalarType, Timestamp, Value};

    let events = [
        StoredEvent {
            position: None, origin: None, commit_position: 0, entity_key: None, iid: Iid([42; 16]),
            system_from: Timestamp(10), valid_time: Interval::ALL, recency: None,
            operation: StoredOperation::Put(StructFields::from([
                ("geometry".into(), Value::Binary(vec![1, 2, 3])),
                ("payload".into(), Value::Utf8("irrelevant document payload".repeat(8192))),
            ])),
        },
        StoredEvent {
            position: None, origin: None, commit_position: 0, entity_key: None, iid: Iid([42; 16]),
            system_from: Timestamp(9), valid_time: Interval::ALL, recency: None,
            operation: StoredOperation::Delete,
        },
    ];
    let put = StructFields::from([
        ("geometry".into(), LogicalType::Scalar(ScalarType::Binary)),
        ("payload".into(), LogicalType::Scalar(ScalarType::Utf8)),
    ]);
    let batch = build_event_batch(&events, Some(put), Some(RunEndWidth::I32)).unwrap();
    let bytes = write_file(&batch.schema(), &[batch]).unwrap();
    let tail = bytes.len() - 10;
    let footer = tail - arrow_ipc::reader::read_footer_length(bytes[tail..].try_into().unwrap()).unwrap();
    let decoder = PageDecoder::from_footer(&bytes[footer..tail], footer as u64, None).unwrap();
    let offset = decoder.page_range(0).unwrap();
    let projected = decoder.decode_iid_page(0, Buffer::from(bytes[offset.start as usize..offset.end as usize].to_vec())).unwrap();
    assert_eq!(projected.num_columns(), 1);
    let iids = rtdb_columnar::IidView::new(projected.column(0).as_ref()).unwrap();
    assert!(matches!(iids, rtdb_columnar::IidView::Run32(_)));
    assert_eq!(iids.get(1).unwrap(), events[1].iid);

    let mut reader = ArrowFile::open(bytes.into(), None).unwrap();
    let projected = EventBatch::new(reader.read_event_page(0, &["geometry".into()]).unwrap()).unwrap();
    let event = projected.borrow_event(0).unwrap();
    assert!(matches!(event.put_column("payload"), Some(PutColumn::Absent)));
    let Some(PutColumn::Stored(field, array, row)) = event.put_column("geometry") else {
        panic!("projected geometry column");
    };
    assert_eq!(rtdb_columnar::binary_cell(field, array, row).unwrap(), Some(&[1, 2, 3][..]));
    assert_eq!(projected.header(1).unwrap().op, rtdb_types::EventOp::Delete);
}

#[test]
fn spatial_arrow_hashing_writer_keeps_the_complete_ipc_bytes() {
    use sha2::{Digest, Sha256};

    let batch = page();
    let expected = write_file(&batch.schema(), std::slice::from_ref(&batch)).unwrap();
    let output = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::OpenOptions::new().write(true).open(output.path()).unwrap();
    let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
    let mut writer = ArrowFileWriter::new_hashed_bounded_reserved(file, &batch.schema(), expected.len() as u64, &memory).unwrap();
    writer.write(&batch).unwrap();
    let (length, digest) = writer.finish_hashed().unwrap();
    assert_eq!(length, expected.len() as u64);
    assert_eq!(digest.as_slice(), Sha256::digest(&expected).as_slice());
    assert_eq!(std::fs::read(output.path()).unwrap(), expected);
}

#[test]
fn spatial_arrow_iid_projection_skips_an_unselected_dictionary() {
    use arrow_array::{builder::{FixedSizeBinaryBuilder, StringDictionaryBuilder}, types::Int8Type};
    use arrow_buffer::Buffer;
    use arrow_ipc::writer::FileWriter;
    use rtdb_format::ipc::PageDecoder;

    let mut iid = FixedSizeBinaryBuilder::new(16);
    iid.append_value([9; 16]).unwrap();
    let mut payload = StringDictionaryBuilder::<Int8Type>::new();
    payload.append("unselected").unwrap();
    let batch = RecordBatch::try_from_iter([
        ("_iid", Arc::new(iid.finish()) as arrow_array::ArrayRef),
        ("payload", Arc::new(payload.finish())),
    ]).unwrap();
    let mut sink = Vec::new();
    let mut writer = FileWriter::try_new(&mut sink, &batch.schema()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    let tail = sink.len() - 10;
    let footer = tail - arrow_ipc::reader::read_footer_length(sink[tail..].try_into().unwrap()).unwrap();
    let decoder = PageDecoder::from_footer(&sink[footer..tail], footer as u64, None).unwrap();
    assert_eq!(decoder.dictionary_count(), 1);
    let block = decoder.page_range(0).unwrap();
    let bytes = Buffer::from(sink[block.start as usize..block.end as usize].to_vec());
    assert!(decoder.decode_page(0, bytes.clone()).is_err());
    let projected = decoder.decode_iid_page(0, bytes).unwrap();
    assert_eq!(projected.column(0), batch.column(0));
}
