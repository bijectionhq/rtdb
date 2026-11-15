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
