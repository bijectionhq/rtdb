use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use bytes::Bytes;
use rtdb_storage::{log::{Log, LogOffset, LocalLog, LocalLogOptions, MemoryLog, ReadOnlyLocalLog, ReadOnlyLocalLogOptions}, object_store::ReadReservation};
use rtdb_types::Timestamp;
use tokio_util::sync::CancellationToken;

struct Charge(Arc<AtomicUsize>);
impl Drop for Charge { fn drop(&mut self) { self.0.fetch_sub(1, Ordering::AcqRel); } }
fn reserve(size: usize, live: &Arc<AtomicUsize>) -> ReadReservation {
    live.fetch_add(1, Ordering::AcqRel);
    ReadReservation::new(size, Charge(Arc::clone(live)))
}

#[tokio::test]
async fn local_range_reports_disappearance_of_an_observed_durable_file() {
    let directory = tempfile::tempdir().unwrap(); let path = directory.path().join("LOG");
    let cancel = CancellationToken::new();
    let (log, driver) = LocalLog::open(&path, LocalLogOptions::default()).unwrap();
    let task = tokio::spawn(driver.run(cancel.clone()));
    log.enqueue(Bytes::from_static(b"payload"), Timestamp(0)).await.unwrap().durable().await.unwrap();
    std::fs::remove_file(&path).unwrap();
    assert!(log.range(LogOffset(0), LogOffset(i64::MAX)).await.is_err());
    cancel.cancel(); task.await.unwrap().unwrap();
}

#[tokio::test]
async fn memory_range_is_finite_inclusive_exclusive_and_payload_clones_retain_charge() {
    let log = MemoryLog::new(1024);
    for value in [b"a", b"b", b"c"] { log.enqueue(Bytes::copy_from_slice(value), Timestamp(0)).await.unwrap().durable().await.unwrap(); }
    let mut range = log.range(LogOffset(1), LogOffset(3)).await.unwrap();
    log.enqueue(Bytes::from_static(b"d"), Timestamp(1)).await.unwrap().durable().await.unwrap();
    let live = Arc::new(AtomicUsize::new(0));
    let record = range.next(reserve(range.allocation_bound(), &live)).await.unwrap().unwrap();
    assert_eq!(record.metadata.offset, LogOffset(1));
    let bytes = record.payload.clone();
    drop(record);
    assert_eq!(live.load(Ordering::Acquire), 1);
    drop(bytes);
    assert_eq!(live.load(Ordering::Acquire), 0);
    assert_eq!(range.next(reserve(range.allocation_bound(), &live)).await.unwrap().unwrap().metadata.offset, LogOffset(2));
    assert!(range.next(reserve(range.allocation_bound(), &live)).await.unwrap().is_none());
    assert_eq!(live.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn local_ranges_use_byte_starts_and_capture_only_durable_records() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("LOG");
    let (log, driver) = LocalLog::open(&path, LocalLogOptions { maximum_message_bytes: 1024, queue_capacity: 4 }).unwrap();
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(driver.run(cancellation.clone()));
    let first = log.enqueue(Bytes::from_static(b"first"), Timestamp(1)).await.unwrap().durable().await.unwrap();
    let second = log.enqueue(Bytes::from_static(b"second"), Timestamp(2)).await.unwrap().durable().await.unwrap();
    let reader = ReadOnlyLocalLog::open(&path, ReadOnlyLocalLogOptions { maximum_message_bytes: 1024, ..Default::default() }).unwrap();
    let live = Arc::new(AtomicUsize::new(0));
    for backend in [&*log as &dyn Log, &*reader as &dyn Log] {
        let mut range = backend.range(first.offset, second.offset).await.unwrap();
        let bound = range.allocation_bound();
        assert_eq!(range.next(reserve(bound, &live)).await.unwrap().unwrap().payload.as_ref(), b"first");
        assert!(range.next(reserve(bound, &live)).await.unwrap().is_none());
        assert!(backend.range(LogOffset(1), second.offset).await.is_err());
    }
    let mut captured = reader.range(second.offset, LogOffset(i64::MAX)).await.unwrap();
    log.enqueue(Bytes::from_static(b"third"), Timestamp(3)).await.unwrap().durable().await.unwrap();
    let bound = captured.allocation_bound();
    assert_eq!(captured.next(reserve(bound, &live)).await.unwrap().unwrap().payload.as_ref(), b"second");
    assert!(captured.next(reserve(bound, &live)).await.unwrap().is_none());
    cancellation.cancel(); task.await.unwrap().unwrap();
}

#[tokio::test]
async fn missing_file_range_stays_empty_and_truncated_record_is_an_error() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("LOG");
    let reader = ReadOnlyLocalLog::open(&path, ReadOnlyLocalLogOptions { maximum_message_bytes: 1024, ..Default::default() }).unwrap();
    let mut empty = reader.range(LogOffset(0), LogOffset(i64::MAX)).await.unwrap();
    assert!(!path.exists());
    let live = Arc::new(AtomicUsize::new(0));
    let (log, driver) = LocalLog::open(&path, LocalLogOptions { maximum_message_bytes: 1024, queue_capacity: 4 }).unwrap();
    let cancellation = CancellationToken::new(); let task = tokio::spawn(driver.run(cancellation.clone()));
    log.enqueue(Bytes::from_static(b"first"), Timestamp(1)).await.unwrap().durable().await.unwrap();
    let bound = empty.allocation_bound();
    assert!(empty.next(reserve(bound, &live)).await.unwrap().is_none());
    let mut range = reader.range(LogOffset(0), LogOffset(i64::MAX)).await.unwrap();
    cancellation.cancel(); task.await.unwrap().unwrap();
    std::fs::OpenOptions::new().write(true).open(&path).unwrap().set_len(2).unwrap();
    let bound = range.allocation_bound();
    assert!(range.next(reserve(bound, &live)).await.is_err());
    assert_eq!(live.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn ranges_beyond_the_durable_tip_are_empty_for_inspection() {
    let root = tempfile::tempdir().unwrap();
    let reader = ReadOnlyLocalLog::open(root.path().join("missing"), ReadOnlyLocalLogOptions { maximum_message_bytes: 1024, ..Default::default() }).unwrap();
    let memory = MemoryLog::new(1024);
    let live = Arc::new(AtomicUsize::new(0));
    for log in [&memory as &dyn Log, &*reader as &dyn Log] {
        let mut range = log.range(LogOffset(99_999), LogOffset(100_000)).await.unwrap();
        let bound = range.allocation_bound();
        assert!(range.next(reserve(bound, &live)).await.unwrap().is_none());
    }
}
