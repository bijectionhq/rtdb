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
    assert_eq!(write_stream_to(&batch.schema(), [&batch], Vec::new()).unwrap(), write_stream(&batch.schema(), &[batch]).unwrap());
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
    assert_eq!(padded.page_decoding_bounds[0] - original.page_decoding_bounds[0], padding * 64);
    assert_eq!(rtdb_format::ipc::file_allocation_bound(&bytes).unwrap() - original_total, padding * 64);
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
    assert!(reader.read_page(0).unwrap_err().message.contains("LZ4 data exceeds its declared Arrow buffer length"));
}
