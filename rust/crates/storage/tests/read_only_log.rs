use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::time::Duration;
use bytes::Bytes;
use rtdb_storage::{Error, log::{LocalLog, LocalLogOptions, Log, LogOffset, ReadOnlyLocalLog, ReadOnlyLocalLogOptions}};
use rtdb_types::Timestamp;
use tokio_util::sync::CancellationToken;

fn options() -> ReadOnlyLocalLogOptions {
    ReadOnlyLocalLogOptions { maximum_message_bytes: 1024, maximum_poll_bytes: 2048, maximum_poll_records: 2, poll_interval: Duration::from_millis(1) }
}

fn append(path: &Path, payload: &[u8], timestamp: i64) -> LogOffset {
    let mut file = OpenOptions::new().create(true).append(true).open(path).unwrap();
    let offset = file.seek(SeekFrom::End(0)).unwrap();
    let mut bytes = vec![0x1e];
    bytes.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    bytes.extend_from_slice(&timestamp.to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes.extend_from_slice(&(offset as i64).to_be_bytes());
    file.write_all(&bytes).unwrap();
    file.sync_all().unwrap();
    LogOffset(offset as i64)
}

fn overwrite(path: &Path, offset: u64, bytes: &[u8]) {
    let mut file = OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

fn assert_corrupt_at<T>(result: rtdb_storage::Result<T>, expected_path: &Path, expected_offset: LogOffset, expected_reason: &str) {
    match result.err().expect("expected a corrupt log error") {
        Error::CorruptLog { path, offset, reason } => {
            assert_eq!(path.as_path(), expected_path);
            assert_eq!(offset, expected_offset.0 as u64);
            assert_eq!(reason, expected_reason);
        }
        other => panic!("expected a corrupt log error, got {other:?}"),
    }
}

#[tokio::test]
async fn local_cursors_resume_after_a_multichunk_frame_and_reach_the_same_tip() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("LOG");
    let maximum = 2 * 8 * 1024 + 3;
    let first = append(&path, &vec![42; maximum], -3);
    let second = append(&path, b"", 9);
    assert_eq!(second, LogOffset((maximum + 21) as i64));
    let (writable, driver) = LocalLog::open(&path, LocalLogOptions { maximum_message_bytes: maximum, queue_capacity: 1 }).unwrap();
    let read_only = ReadOnlyLocalLog::open(&path, ReadOnlyLocalLogOptions {
        maximum_message_bytes: maximum,
        maximum_poll_bytes: maximum,
        ..options()
    }).unwrap();
    let logs: [&dyn Log; 2] = [writable.as_ref(), read_only.as_ref()];
    for log in logs {
        assert_eq!(log.latest().unwrap(), second);
        let mut tail = log.tail(first).await.unwrap();
        let records = tail.poll(Duration::ZERO).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].metadata.offset, second);
        assert_eq!(records[0].metadata.timestamp, Timestamp(9));
        assert!(records[0].payload.is_empty());
        assert!(tail.poll(Duration::ZERO).await.unwrap().is_empty());
        let mut at_tip = log.tail(second).await.unwrap();
        assert!(at_tip.poll(Duration::ZERO).await.unwrap().is_empty());
    }
    drop(driver);
    writable.close_and_wait().await;
}

#[tokio::test]
async fn local_log_inspection_reports_corrupt_resume_and_tip_frames_at_their_byte_offsets() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("LOG");
    let first = append(&path, b"first", 1);
    let second = append(&path, b"second", 2);
    overwrite(&path, second.0 as u64 - 8, &(first.0 + 1).to_be_bytes());

    let writable = LocalLog::open(&path, LocalLogOptions { maximum_message_bytes: 1024, queue_capacity: 1 });
    assert_corrupt_at(writable, &path, first, "record trailer does not name its start offset");

    let read_only = ReadOnlyLocalLog::open(&path, options()).unwrap();
    assert_eq!(read_only.latest().unwrap(), second);
    assert_corrupt_at(read_only.tail(first).await, &path, first, "record trailer does not name its start offset");
    let mut tail = read_only.tail(second).await.unwrap();
    assert!(tail.poll(Duration::ZERO).await.unwrap().is_empty());

    overwrite(&path, second.0 as u64, &[0]);
    assert_corrupt_at(read_only.latest(), &path, second, "expected record separator 0x1e");
    assert_corrupt_at(tail.poll(Duration::ZERO).await, &path, second, "expected record separator 0x1e");
}

#[tokio::test]
async fn missing_log_is_observed_without_creating_a_file_or_directory() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("external");
    let path = root.join("LOG");
    let log = ReadOnlyLocalLog::open(&path, options()).unwrap();
    assert_eq!(log.latest().unwrap(), LogOffset(-1));
    let mut tail = log.tail(LogOffset(-1)).await.unwrap();
    assert!(tail.poll(Duration::ZERO).await.unwrap().is_empty());
    assert!(matches!(log.enqueue(Bytes::from_static(b"forbidden"), Timestamp(0)).await, Err(Error::ReadOnlyLog(_))));
    assert!(!root.exists());
    std::fs::create_dir(&root).unwrap();
    append(&path, b"external", 9);
    let records = tail.poll(Duration::from_secs(1)).await.unwrap();
    assert_eq!(records[0].payload.as_ref(), b"external");
    assert_eq!(records[0].metadata.timestamp, Timestamp(9));
}

#[tokio::test]
async fn polling_observes_appends_after_waiting_and_keeps_batch_positions() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("REPLICA_LOG");
    append(&path, b"one", 1);
    let log = ReadOnlyLocalLog::open(&path, options()).unwrap();
    let mut tail = log.tail(LogOffset(-1)).await.unwrap();
    assert_eq!(tail.poll(Duration::ZERO).await.unwrap()[0].payload.as_ref(), b"one");
    let pending = tokio::spawn(async move { let records = tail.poll(Duration::from_secs(1)).await; (tail, records) });
    tokio::task::yield_now().await;
    for payload in [b"two", b"tri", b"for"] { append(&path, payload, 2); }
    let (mut tail, records) = pending.await.unwrap();
    assert_eq!(records.unwrap().len(), 2);
    assert_eq!(tail.poll(Duration::ZERO).await.unwrap()[0].payload.as_ref(), b"for");
    assert!(tail.poll(Duration::ZERO).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_read_only_reader_does_not_take_the_local_writer_lock() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("LOG");
    let (writer, driver) = LocalLog::open(&path, LocalLogOptions { maximum_message_bytes: 1024, queue_capacity: 8 }).unwrap();
    let reader = ReadOnlyLocalLog::open(&path, options()).unwrap();
    let mut tail = reader.tail(LogOffset(-1)).await.unwrap();
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(driver.run(cancellation.clone()));
    let durable = writer.enqueue(Bytes::from_static(b"record"), Timestamp(4)).await.unwrap().durable().await.unwrap();
    assert_eq!(tail.poll(Duration::ZERO).await.unwrap()[0].metadata, durable);
    cancellation.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn partial_or_truncated_external_appends_are_errors_never_empty_polls() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("LOG");
    append(&path, b"first", 1);
    let reader = ReadOnlyLocalLog::open(&path, options()).unwrap();
    let mut tail = reader.tail(LogOffset(-1)).await.unwrap();
    tail.poll(Duration::ZERO).await.unwrap();
    let mut file = OpenOptions::new().write(true).append(true).open(&path).unwrap();
    file.write_all(&[0x1e, 0, 0]).unwrap();
    file.sync_all().unwrap();
    assert!(matches!(reader.latest(), Err(Error::CorruptLog { .. })));
    assert!(matches!(tail.poll(Duration::ZERO).await, Err(Error::CorruptLog { .. })));
    file.set_len(0).unwrap();
    assert!(matches!(tail.poll(Duration::ZERO).await, Err(Error::CorruptLog { .. })));
    std::fs::remove_file(&path).unwrap();
    assert!(matches!(tail.poll(Duration::ZERO).await, Err(Error::CorruptLog { .. })));
}

#[tokio::test]
async fn ranges_validate_the_start_and_never_truncate_at_the_batch_bound() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("LOG");
    let first = append(&path, b"a", 1);
    let second = append(&path, b"b", 2);
    let third = append(&path, b"c", 3);
    let reader = ReadOnlyLocalLog::open(&path, options()).unwrap();
    assert_eq!(reader.read_range(first, third).await.unwrap().len(), 2);
    assert!(reader.read_range(LogOffset(1), second).await.is_err());
    assert!(matches!(reader.read_range(first, LogOffset(i64::MAX)).await, Err(Error::LogReadLimit { .. })));
    assert!(reader.read_range(second, first).await.is_err());
    assert!(reader.read_range(LogOffset(66), LogOffset(66)).await.unwrap().is_empty());
}
