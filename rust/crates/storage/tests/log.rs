use bytes::Bytes;
use rtdb_storage::{Error, log::{LocalLog, LocalLogOptions, Log, LogOffset, MemoryLog}};
use rtdb_types::Timestamp;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn options() -> LocalLogOptions { LocalLogOptions { maximum_message_bytes: 1024, queue_capacity: 10 } }

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
