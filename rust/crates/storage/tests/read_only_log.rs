use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use rtdb_storage::{Error, IoRegion, log::{LocalLog, LocalLogOptions, Log, LogMetadata, LogOffset, ReadOnlyLocalLog, ReadOnlyLocalLogOptions}, object_store::ReadReservation};
use rtdb_types::{MemoryBudget, Timestamp};
use tokio_util::sync::CancellationToken;

fn options() -> ReadOnlyLocalLogOptions {
    ReadOnlyLocalLogOptions { maximum_message_bytes: 1024, maximum_poll_bytes: 2048, maximum_poll_records: 2, poll_interval: Duration::from_millis(1) }
}

fn append(path: &Path, payload: &[u8], timestamp: i64) -> LogOffset {
    let mut file = OpenOptions::new().create(true).append(true).open(path).unwrap();
    let offset = LogOffset(file.seek(SeekFrom::End(0)).unwrap() as i64);
    file.write_all(&frame(offset, payload, timestamp)).unwrap();
    file.sync_all().unwrap();
    offset
}

fn frame(offset: LogOffset, payload: &[u8], timestamp: i64) -> Vec<u8> {
    let mut bytes = vec![0x1e];
    bytes.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    bytes.extend_from_slice(&timestamp.to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes.extend_from_slice(&offset.0.to_be_bytes());
    bytes
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

fn reserve(memory: &Arc<MemoryBudget>, bytes: usize) -> ReadReservation {
    ReadReservation::new(bytes, memory.reserve(bytes).unwrap())
}

#[tokio::test]
async fn regional_replay_delivers_a_new_file_with_an_exact_reservation_after_refusal() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("LOG");
    let reads = IoRegion::new();
    let log = ReadOnlyLocalLog::open_in(&path, options(), Arc::clone(&reads)).unwrap();
    let mut tail = log.tail(LogOffset(-1)).await.unwrap();
    let replay = tail.replay().unwrap();
    assert_eq!(replay.poll_head(Duration::ZERO).await.unwrap(), None);
    assert!(!path.exists());

    let offset = append(&path, b"first", 9);
    let head = replay.poll_head(Duration::ZERO).await.unwrap().unwrap();
    assert_eq!(head.metadata, LogMetadata { offset, timestamp: Timestamp(9) });
    assert_eq!(head.payload_len, 5);
    assert_eq!(head.allocation_bound, 261);

    let memory = MemoryBudget::new(head.allocation_bound);
    let short = reserve(&memory, head.allocation_bound - 1);
    assert!(matches!(replay.read_head(head, short).await, Err(Error::Task(reason)) if reason == "log read reservation is too small"));
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(replay.poll_head(Duration::ZERO).await.unwrap(), Some(head));

    let record = replay.read_head(head, reserve(&memory, head.allocation_bound)).await.unwrap();
    assert_eq!(record.metadata, head.metadata);
    let payload = record.payload.clone();
    drop(record);
    assert_eq!(payload.as_ref(), b"first");
    assert_eq!(memory.used_bytes(), head.allocation_bound);
    drop(payload);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(replay.poll_head(Duration::ZERO).await.unwrap(), None);
    reads.close_and_wait().await;
}

#[tokio::test]
async fn regional_replay_delivers_the_captured_prefix_before_checking_an_incomplete_append() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("REPLICA_LOG");
    let first = append(&path, b"first", 1);
    let second = append(&path, b"second", 2);
    let reads = IoRegion::new();
    let log = ReadOnlyLocalLog::open_in(&path, options(), Arc::clone(&reads)).unwrap();
    let mut tail = log.tail(LogOffset(-1)).await.unwrap();
    let replay = tail.replay().unwrap();
    let head = replay.poll_head(Duration::ZERO).await.unwrap().unwrap();
    assert_eq!(head.metadata.offset, first);

    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    let third = LogOffset(file.seek(SeekFrom::End(0)).unwrap() as i64);
    let appended = frame(third, b"third", 3);
    file.write_all(&appended[..3]).unwrap();
    file.sync_all().unwrap();
    let memory = MemoryBudget::new(1024);
    let record = replay.read_head(head, reserve(&memory, head.allocation_bound)).await.unwrap();
    assert_eq!(record.payload.as_ref(), b"first");
    drop(record);
    assert_corrupt_at(replay.poll_head(Duration::ZERO).await, &path, LogOffset(third.0 + 3 - 8), "final offset is outside the log");

    file.write_all(&appended[3..]).unwrap();
    file.sync_all().unwrap();
    for (offset, payload) in [(second, b"second".as_slice()), (third, b"third".as_slice())] {
        let head = replay.poll_head(Duration::ZERO).await.unwrap().unwrap();
        assert_eq!(head.metadata.offset, offset);
        let record = replay.read_head(head, reserve(&memory, head.allocation_bound)).await.unwrap();
        assert_eq!(record.payload.as_ref(), payload);
    }
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(replay.poll_head(Duration::ZERO).await.unwrap(), None);
    reads.close_and_wait().await;
}

#[tokio::test]
async fn regional_replay_rejects_a_captured_tip_whose_payload_extends_into_a_new_append() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("REPLICA_LOG");
    let first = append(&path, b"first", 1);
    let captured_tip = append(&path, b"second", 2);
    let reads = IoRegion::new();
    let log = ReadOnlyLocalLog::open_in(&path, options(), Arc::clone(&reads)).unwrap();
    let mut tail = log.tail(LogOffset(-1)).await.unwrap();
    let replay = tail.replay().unwrap();
    let head = replay.poll_head(Duration::ZERO).await.unwrap().unwrap();
    assert_eq!(head.metadata.offset, first);

    let appended = append(&path, &[42; 32], 3);
    assert_eq!(appended, LogOffset(53));
    overwrite(&path, captured_tip.0 as u64 + 1, &24i32.to_be_bytes());
    let memory = MemoryBudget::new(head.allocation_bound);
    assert_corrupt_at(
        replay.read_head(head, reserve(&memory, head.allocation_bound)).await,
        &path, captured_tip, "incomplete record ends at 71, file length 53",
    );
    assert_eq!(memory.used_bytes(), 0);

    overwrite(&path, captured_tip.0 as u64 + 1, &6i32.to_be_bytes());
    let record = replay.read_head(head, reserve(&memory, head.allocation_bound)).await.unwrap();
    assert_eq!(record.metadata.offset, first);
    assert_eq!(record.payload.as_ref(), b"first");
    drop(record);
    assert_eq!(replay.poll_head(Duration::ZERO).await.unwrap().unwrap().metadata.offset, captured_tip);
    reads.close_and_wait().await;
}

#[tokio::test]
async fn regional_replay_rechecks_the_observed_tip_and_reports_disappearance_and_truncation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("REPLICA_LOG");
    let first = append(&path, b"first", 1);
    let second = append(&path, b"second", 2);
    let reads = IoRegion::new();
    let log = ReadOnlyLocalLog::open_in(&path, options(), Arc::clone(&reads)).unwrap();
    let mut tail = log.tail(LogOffset(-1)).await.unwrap();
    let replay = tail.replay().unwrap();
    let head = replay.poll_head(Duration::ZERO).await.unwrap().unwrap();
    assert_eq!(head.metadata.offset, first);
    let memory = MemoryBudget::new(head.allocation_bound);
    let read = || reserve(&memory, head.allocation_bound);

    overwrite(&path, second.0 as u64, &[0]);
    assert_corrupt_at(replay.read_head(head, read()).await, &path, second, "invalid local log record separator");
    overwrite(&path, second.0 as u64, &[0x1e]);
    overwrite(&path, second.0 as u64 - 8, &1i64.to_be_bytes());
    assert_corrupt_at(replay.read_head(head, read()).await, &path, first, "record trailer does not name its start offset");
    overwrite(&path, second.0 as u64 - 8, &first.0.to_be_bytes());
    assert_eq!(replay.poll_head(Duration::ZERO).await.unwrap(), Some(head));

    OpenOptions::new().write(true).open(&path).unwrap().set_len(second.0 as u64).unwrap();
    assert_corrupt_at(replay.read_head(head, read()).await, &path, second, "observed log was truncated");
    assert_corrupt_at(replay.poll_head(Duration::ZERO).await, &path, second, "observed log was truncated");
    std::fs::remove_file(&path).unwrap();
    assert_corrupt_at(replay.read_head(head, read()).await, &path, first, "observed log disappeared");
    assert_corrupt_at(replay.poll_head(Duration::ZERO).await, &path, first, "observed log disappeared");
    assert_eq!(memory.used_bytes(), 0);
    reads.close_and_wait().await;
}

#[test]
fn cancelled_regional_replay_keeps_its_charge_until_the_undelivered_record_is_dropped() {
    use std::{future::{Future, poll_fn}, task::Poll};
    use rtdb_types::MemoryReservation;
    use tokio::sync::oneshot;

    struct ReportRelease {
        reservation: Option<Arc<MemoryReservation>>,
        released: Option<oneshot::Sender<()>>,
    }
    impl Drop for ReportRelease {
        fn drop(&mut self) {
            drop(self.reservation.take());
            let _ = self.released.take().unwrap().send(());
        }
    }

    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).max_blocking_threads(1).enable_all().build().unwrap();
    runtime.block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("LOG");
        append(&path, b"payload", 1);
        let reads = IoRegion::new();
        let log = ReadOnlyLocalLog::open_in(&path, options(), Arc::clone(&reads)).unwrap();
        let mut tail = log.tail(LogOffset(-1)).await.unwrap();
        let replay = tail.replay().unwrap();
        let head = replay.poll_head(Duration::ZERO).await.unwrap().unwrap();

        let (started, waiting) = oneshot::channel();
        let (resume, paused) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            paused.recv().unwrap();
        });
        waiting.await.unwrap();
        let memory = MemoryBudget::new(head.allocation_bound);
        let (released, release) = oneshot::channel();
        let lease = ReportRelease { reservation: Some(memory.reserve(head.allocation_bound).unwrap()), released: Some(released) };
        let reservation = ReadReservation::new(head.allocation_bound, lease);
        let mut delivery = Box::pin(replay.read_head(head, reservation));
        poll_fn(|context| {
            assert!(delivery.as_mut().poll(context).is_pending());
            Poll::Ready(())
        }).await;
        drop(delivery);
        assert_eq!(memory.used_bytes(), head.allocation_bound);
        resume.send(()).unwrap();
        blocker.await.unwrap();
        release.await.unwrap();
        assert_eq!(memory.used_bytes(), 0);
        assert_eq!(replay.poll_head(Duration::ZERO).await.unwrap(), Some(head));
        reads.close_and_wait().await;
        assert!(matches!(replay.poll_head(Duration::ZERO).await, Err(Error::Closed(_))));
    });
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
    assert_corrupt_at(read_only.latest(), &path, second, "invalid local log record separator");
    assert_corrupt_at(tail.poll(Duration::ZERO).await, &path, second, "invalid local log record separator");
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
