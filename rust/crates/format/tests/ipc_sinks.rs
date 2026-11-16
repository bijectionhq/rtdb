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
}
