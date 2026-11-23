use bytes::Bytes;
use rtdb_storage::{Error, log::{LocalLog, LocalLogOptions, Log, LogMetadata, LogOffset, LogRecord, MemoryLog}};
use rtdb_types::Timestamp;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn options() -> LocalLogOptions { LocalLogOptions { maximum_message_bytes: 1024, queue_capacity: 10 } }

#[test]
fn cancelled_local_log_reader_offloads_are_joined_after_writer_shutdown() {
    use std::{future::{Future, poll_fn}, pin::Pin, task::Poll};
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).max_blocking_threads(1).enable_all().build().unwrap();
    runtime.block_on(async {
        for mode in 0..3 {
            let directory = tempfile::tempdir().unwrap(); let (log, driver) = LocalLog::open(directory.path().join("LOG"), options()).unwrap();
            let cancellation = CancellationToken::new(); let writer = tokio::spawn(driver.run(cancellation.clone()));
            let first = log.enqueue(Bytes::from_static(b"record"), Timestamp(1)).await.unwrap().durable().await.unwrap();
            let mut tail = log.tail(LogOffset(-1)).await.unwrap();
            let (started, waiting) = tokio::sync::oneshot::channel(); let (resume, paused) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || { started.send(()).unwrap(); paused.recv().unwrap(); }); waiting.await.unwrap();
            let mut read: Pin<Box<dyn Future<Output = rtdb_storage::Result<()>> + '_>> = match mode {
                0 => Box::pin(async { log.tail(first.offset).await.map(|_| ()) }),
                1 => Box::pin(async { log.read_range(LogOffset(0), LogOffset(i64::MAX)).await.map(|_| ()) }),
                _ => Box::pin(async { tail.poll(Duration::ZERO).await.map(|_| ()) }),
            };
            poll_fn(|context| { assert!(read.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await; drop(read);
            cancellation.cancel(); writer.await.unwrap().unwrap();
            let mut close = Box::pin(log.close_and_wait());
            poll_fn(|context| { assert!(close.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
            resume.send(()).unwrap(); blocker.await.unwrap(); close.await;
        }
    });
}

#[tokio::test]
async fn timestamp_source_is_sampled_in_the_writer_after_enqueue() {
    use std::sync::{Arc, atomic::{AtomicI64, Ordering}};
    struct Clock(AtomicI64);
    impl rtdb_storage::log::AppendTimestampSource for Clock {
        fn timestamp(&self, _: &[u8], _: Timestamp) -> rtdb_storage::Result<Timestamp> { Ok(Timestamp(self.0.fetch_add(1, Ordering::SeqCst))) }
    }
    let directory = tempfile::tempdir().unwrap();
    let clock = Arc::new(Clock(AtomicI64::new(1)));
    let (log, driver) = LocalLog::open_with_timestamp_source(directory.path().join("LOG"), options(), clock.clone()).unwrap();
    let first = log.enqueue(Bytes::from_static(b"one"), Timestamp(5)).await.unwrap();
    let second = log.enqueue(Bytes::from_static(b"two"), Timestamp(5)).await.unwrap();
    clock.0.store(100, Ordering::SeqCst);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(driver.run(shutdown.clone()));
    assert_eq!(first.durable().await.unwrap().timestamp, Timestamp(100));
    assert_eq!(second.durable().await.unwrap().timestamp, Timestamp(101));
    shutdown.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn timestamp_failure_rolls_back_every_record_in_the_write_batch() {
    use std::sync::Arc;
    struct FailSecond;
    impl rtdb_storage::log::AppendTimestampSource for FailSecond {
        fn timestamp(&self, payload: &[u8], supplied: Timestamp) -> rtdb_storage::Result<Timestamp> {
            if payload == b"fail" { Err(Error::Task("clock failed".into())) } else { Ok(supplied) }
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("LOG");
    let (log, driver) = LocalLog::open_with_timestamp_source(&path, options(), Arc::new(FailSecond)).unwrap();
    let first = log.enqueue(Bytes::from_static(b"good"), Timestamp(5)).await.unwrap();
    let second = log.enqueue(Bytes::from_static(b"fail"), Timestamp(6)).await.unwrap();
    assert!(driver.run(CancellationToken::new()).await.is_err());
    assert!(first.durable().await.is_err());
    assert!(second.durable().await.is_err());
    assert_eq!(std::fs::metadata(path).unwrap().len(), 0);
}

#[tokio::test]
async fn memory_log_samples_before_publication_and_reuses_a_failed_position() {
    use std::sync::{Arc, atomic::{AtomicI64, Ordering}};

    struct Clock(AtomicI64);
    impl rtdb_storage::log::AppendTimestampSource for Clock {
        fn timestamp(&self, payload: &[u8], _: Timestamp) -> rtdb_storage::Result<Timestamp> {
            if payload == b"fail" { return Err(Error::Task("clock failed".into())); }
            Ok(Timestamp(self.0.fetch_add(1, Ordering::SeqCst)))
        }
    }

    let clock = Arc::new(Clock(AtomicI64::new(30)));
    let log = MemoryLog::with_timestamp_source(1024, clock.clone());
    let first = log.enqueue(Bytes::from_static(b"first"), Timestamp(1)).await.unwrap().durable().await.unwrap();
    assert_eq!(first.offset, LogOffset(0));
    assert_eq!(first.timestamp, Timestamp(30));
    assert!(matches!(log.enqueue(Bytes::from_static(b"fail"), Timestamp(2)).await, Err(Error::Task(_))));
    assert_eq!(log.latest().unwrap(), LogOffset(0));
    let next = log.enqueue(Bytes::from_static(b"next"), Timestamp(3)).await.unwrap().durable().await.unwrap();
    assert_eq!(next.offset, LogOffset(1));
    assert_eq!(next.timestamp, Timestamp(31));
    let records = log.read_range(LogOffset(0), LogOffset(2)).await.unwrap();
    assert_eq!(records.iter().map(|record| record.metadata).collect::<Vec<_>>(), [first, next]);
    log.close();
    assert!(log.enqueue(Bytes::from_static(b"closed"), Timestamp(4)).await.is_err());
    assert_eq!(clock.0.load(Ordering::SeqCst), 32);
}

#[tokio::test]
async fn local_offsets_are_byte_positions_and_reopen_preserves_them() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("LOG");
    let (log, driver) = LocalLog::open(&path, options()).unwrap();
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(driver.run(shutdown.clone()));
    let first = log.enqueue(Bytes::from_static(b"abc"), Timestamp(42)).await.unwrap().durable().await.unwrap();
    let second = log.enqueue(Bytes::from_static(b"xyz12"), Timestamp(43)).await.unwrap().durable().await.unwrap();
    assert_eq!(first.offset, LogOffset(0));
    assert_eq!(second.offset, LogOffset(24));
    let mut tail = log.tail(LogOffset(-1)).await.unwrap();
    let records = tail.poll(Duration::ZERO).await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].metadata.timestamp, Timestamp(42));
    assert_eq!(records[1].payload.as_ref(), b"xyz12");
    assert!(tail.poll(Duration::ZERO).await.unwrap().is_empty());
    shutdown.cancel();
    task.await.unwrap().unwrap();
    drop(log);
    let (reopened, driver) = LocalLog::open(&path, options()).unwrap();
    assert_eq!(reopened.latest().unwrap(), second.offset);
    let mut tail = reopened.tail(first.offset).await.unwrap();
    assert_eq!(tail.poll(Duration::ZERO).await.unwrap()[0].metadata, second);
    drop(driver);
}

#[tokio::test]
async fn local_tail_delivers_a_capped_prefix_and_resumes_at_the_exact_byte_offset() {
    let directory = tempfile::tempdir().unwrap();
    let (log, driver) = LocalLog::open(directory.path().join("LOG"), options()).unwrap();
    let shutdown = CancellationToken::new();
    let writer = tokio::spawn(driver.run(shutdown.clone()));
    let mut tail = log.tail(LogOffset(-1)).await.unwrap();
    let mut appends = Vec::new();
    let mut expected_offset = 0;
    for position in 0..1025 {
        let payload = if position % 2 == 0 { Bytes::from_static(b"a") } else { Bytes::from_static(b"xyz") };
        let timestamp = Timestamp(position);
        let receipt = log.enqueue(payload.clone(), timestamp).await.unwrap();
        let metadata = LogMetadata { offset: LogOffset(expected_offset), timestamp };
        expected_offset += 21 + payload.len() as i64;
        appends.push((receipt, LogRecord::new(metadata, payload)));
    }
    let mut expected = Vec::new();
    for (receipt, record) in appends {
        assert_eq!(receipt.durable().await.unwrap(), record.metadata);
        expected.push(record);
    }

    let first = tail.poll(Duration::ZERO).await.unwrap();
    assert_eq!(first.as_slice(), &expected[..1024]);
    let remainder = tail.poll(Duration::ZERO).await.unwrap();
    assert_eq!(remainder.as_slice(), &expected[1024..]);
    assert!(tail.poll(Duration::ZERO).await.unwrap().is_empty());
    assert_eq!(log.read_range(LogOffset(0), LogOffset(expected_offset)).await.unwrap(), expected);
    shutdown.cancel();
    writer.await.unwrap().unwrap();
    log.close_and_wait().await;
}

#[tokio::test]
async fn local_record_bytes_match_the_jvm_framing() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("REPLICA_LOG");
    let (log, driver) = LocalLog::open(&path, options()).unwrap();
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(driver.run(shutdown.clone()));
    log.enqueue(Bytes::from_static(b"ab"), Timestamp(5)).await.unwrap().durable().await.unwrap();
    let mut expected = vec![0x1e, 0, 0, 0, 2];
    expected.extend_from_slice(&5_i64.to_be_bytes());
    expected.extend_from_slice(b"ab");
    expected.extend_from_slice(&0_i64.to_be_bytes());
    assert_eq!(std::fs::read(&path).unwrap(), expected);
    shutdown.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn an_accepted_enqueue_can_still_fail_before_durable_append() {
    let directory = tempfile::tempdir().unwrap();
    let (log, driver) = LocalLog::open(directory.path().join("LOG"), options()).unwrap();
    let receipt = log.enqueue(Bytes::from_static(b"queued"), Timestamp(1)).await.unwrap();
    assert_eq!(log.latest().unwrap(), LogOffset(-1));
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    driver.run(shutdown).await.unwrap();
    assert!(matches!(receipt.durable().await, Err(Error::Closed(_))));
    assert!(matches!(log.enqueue(Bytes::new(), Timestamp(2)).await, Err(Error::Closed(_))));
}

#[tokio::test]
async fn a_declined_oversized_message_does_not_consume_a_position() {
    let log = MemoryLog::new(4);
    assert!(matches!(log.enqueue(Bytes::from_static(b"12345"), Timestamp(0)).await, Err(Error::MessageTooLarge { .. })));
    let accepted = log.enqueue(Bytes::from_static(b"ok"), Timestamp(1)).await.unwrap().durable().await.unwrap();
    assert_eq!(accepted.offset, LogOffset(0));
}

#[tokio::test]
async fn a_tail_between_polls_keeps_its_exact_position() {
    let log = MemoryLog::new(64);
    let mut tail = log.tail(LogOffset(-1)).await.unwrap();
    assert!(tail.poll(Duration::ZERO).await.unwrap().is_empty());
    log.enqueue(Bytes::from_static(b"a"), Timestamp(1)).await.unwrap().durable().await.unwrap();
    assert_eq!(tail.poll(Duration::ZERO).await.unwrap()[0].payload.as_ref(), b"a");
    log.enqueue(Bytes::from_static(b"b"), Timestamp(2)).await.unwrap().durable().await.unwrap();
    assert_eq!(tail.poll(Duration::ZERO).await.unwrap()[0].payload.as_ref(), b"b");
}

#[tokio::test]
async fn memory_tail_drains_a_capped_prefix_and_detects_truncation_before_its_next_poll() {
    let log = MemoryLog::new(64);
    let mut tail = log.tail(LogOffset(-1)).await.unwrap();
    let mut truncated = log.tail(LogOffset(-1)).await.unwrap();
    for position in 0..1025 {
        log.enqueue(Bytes::new(), Timestamp(position)).await.unwrap().durable().await.unwrap();
    }

    let batch = tail.poll(Duration::ZERO).await.unwrap();
    assert_eq!(batch.len(), 1024);
    assert!(batch.iter().zip(0..1024).all(|(record, position)| {
        record.metadata.offset == LogOffset(position) && record.metadata.timestamp == Timestamp(position) && record.payload.is_empty()
    }));
    assert_eq!(truncated.poll(Duration::ZERO).await.unwrap().len(), 1024);
    let remainder = tail.poll(Duration::ZERO).await.unwrap();
    assert_eq!(remainder.len(), 1);
    assert_eq!(remainder[0].metadata.offset, LogOffset(1024));
    assert!(tail.poll(Duration::ZERO).await.unwrap().is_empty());
    assert_eq!(log.read_range(LogOffset(0), LogOffset(1025)).await.unwrap().len(), 1025);

    log.truncate_before(LogOffset(1025)).unwrap();
    assert!(matches!(truncated.poll(Duration::ZERO).await,
        Err(Error::UnavailableLogRange { requested: 1024, first_retained: 1025 })));
    log.enqueue(Bytes::new(), Timestamp(1025)).await.unwrap().durable().await.unwrap();
    log.close();
    assert!(matches!(tail.poll(Duration::ZERO).await, Err(Error::Closed(_))));
}

#[tokio::test]
async fn missing_retained_records_are_errors_instead_of_empty_polls() {
    let log = MemoryLog::new(64);
    let mut tail = log.tail(LogOffset(-1)).await.unwrap();
    for timestamp in 0..3 {
        log.enqueue(Bytes::from_static(b"x"), Timestamp(timestamp)).await.unwrap().durable().await.unwrap();
    }
    log.truncate_before(LogOffset(2)).unwrap();
    assert!(matches!(tail.poll(Duration::ZERO).await, Err(Error::UnavailableLogRange { .. })));
    assert!(log.tail(LogOffset(-1)).await.is_err());
}

#[test]
fn an_incomplete_local_record_is_reported_with_its_file_and_offset() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("LOG");
    std::fs::write(&path, [0x1e, 0, 0]).unwrap();
    match LocalLog::open(&path, options()) {
        Err(Error::CorruptLog { path: actual_path, offset, .. }) => {
            assert_eq!(actual_path, path);
            assert_eq!(offset, 0);
        }
        _ => panic!("partial record was not diagnosed"),
    }
}

#[tokio::test]
async fn local_ranges_reject_missing_starts_and_allow_the_durable_end() {
    let directory = tempfile::tempdir().unwrap();
    let (log, driver) = LocalLog::open(directory.path().join("LOG"), options()).unwrap();
    assert!(log.read_range(LogOffset(0), LogOffset(0)).await.unwrap().is_empty());
    assert!(matches!(log.read_range(LogOffset(10), LogOffset(10)).await, Err(Error::UnavailableLogRange { .. })));
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(driver.run(shutdown.clone()));
    log.enqueue(Bytes::from_static(b"abc"), Timestamp(0)).await.unwrap().durable().await.unwrap();
    assert_eq!(log.read_range(LogOffset(0), LogOffset(24)).await.unwrap().len(), 1);
    assert!(log.read_range(LogOffset(24), LogOffset(24)).await.unwrap().is_empty());
    assert!(matches!(log.read_range(LogOffset(25), LogOffset(30)).await, Err(Error::UnavailableLogRange { .. })));
    assert!(log.read_range(LogOffset(1), LogOffset(1)).await.is_err());
    assert!(log.read_range(LogOffset(24), LogOffset(0)).await.is_err());
    shutdown.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn memory_ranges_reject_reversed_endpoints() {
    let log = MemoryLog::new(64);
    assert!(log.read_range(LogOffset(0), LogOffset(-1)).await.is_err());
}
