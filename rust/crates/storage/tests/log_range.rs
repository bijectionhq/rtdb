use std::io::{Seek, SeekFrom, Write};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::time::Duration;
use bytes::Bytes;
use rtdb_storage::{Error, log::{Log, LogMetadata, LogOffset, LogRecord, LocalLog, LocalLogOptions, MemoryLog, ReadOnlyLocalLog, ReadOnlyLocalLogOptions, SharedLog}, object_store::ReadReservation};
use rtdb_types::{MemoryBudget, Timestamp};
use tokio_util::sync::CancellationToken;

struct Charge(Arc<AtomicUsize>);
impl Drop for Charge { fn drop(&mut self) { self.0.fetch_sub(1, Ordering::AcqRel); } }
fn reserve(size: usize, live: &Arc<AtomicUsize>) -> ReadReservation {
    live.fetch_add(1, Ordering::AcqRel);
    ReadReservation::new(size, Charge(Arc::clone(live)))
}

#[tokio::test]
async fn shared_log_local_replay_charges_the_actual_frame_and_keeps_a_refused_head_available() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("LOG");
    let (log, driver) = LocalLog::open(&path, LocalLogOptions { maximum_message_bytes: 1 << 20, queue_capacity: 4 }).unwrap();
    let cancellation = CancellationToken::new();
    let writer = tokio::spawn(driver.run(cancellation.clone()));
    let shared: &dyn SharedLog = log.as_ref();
    assert_eq!(shared.latest().unwrap(), LogOffset(-1));
    let first = shared.append(Bytes::from_static(b"first"), Timestamp(5)).await.unwrap().durable().await.unwrap();
    let second = shared.append(Bytes::from_static(b""), Timestamp(9)).await.unwrap().durable().await.unwrap();
    assert_eq!(shared.latest().unwrap(), second.offset);
    let mut replay = shared.read_after(LogOffset(-1)).await.unwrap();
    let head = replay.poll_head(Duration::ZERO).await.unwrap().unwrap();
    assert_eq!(head.metadata, first);
    assert_eq!(head.payload_len, 5);
    assert_eq!(head.allocation_bound, 261);

    let memory = MemoryBudget::new(261);
    let undersized = ReadReservation::new(260, memory.reserve(260).unwrap());
    assert!(matches!(replay.read_head(head, undersized).await, Err(Error::Task(_))));
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(replay.poll_head(Duration::ZERO).await.unwrap(), Some(head));

    let exact = ReadReservation::new(261, memory.reserve(261).unwrap());
    let record = replay.read_head(head, exact).await.unwrap();
    assert_eq!(record.metadata, first);
    let clone = record.payload.clone();
    drop(record);
    assert_eq!(clone.as_ref(), b"first");
    assert_eq!(memory.used_bytes(), 261);
    drop(clone);
    assert_eq!(memory.used_bytes(), 0);

    let mut resumed = shared.read_after(first.offset).await.unwrap();
    let head = replay.poll_head(Duration::ZERO).await.unwrap().unwrap();
    assert_eq!(resumed.poll_head(Duration::ZERO).await.unwrap(), Some(head));
    assert_eq!(head.metadata, second);
    assert_eq!(second.offset, LogOffset(26));
    assert_eq!(head.allocation_bound, 256);
    let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    let trailer = second.offset.0 as u64 + 13;
    file.seek(SeekFrom::Start(trailer)).unwrap();
    file.write_all(&0i64.to_be_bytes()).unwrap();
    let exact = ReadReservation::new(256, memory.reserve(256).unwrap());
    assert!(matches!(replay.read_head(head, exact).await, Err(Error::CorruptLog { path: log_path, offset: 26, reason })
        if log_path == path && reason == "record trailer does not name its start offset"));
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(replay.poll_head(Duration::ZERO).await.unwrap(), Some(head));
    file.seek(SeekFrom::Start(trailer)).unwrap();
    file.write_all(&second.offset.0.to_be_bytes()).unwrap();
    drop(file);
    let exact = ReadReservation::new(256, memory.reserve(256).unwrap());
    let record = replay.read_head(head, exact).await.unwrap();
    assert_eq!(record.metadata, second);
    assert!(record.payload.is_empty());
    assert_eq!(memory.used_bytes(), 256);
    drop(record);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(replay.poll_head(Duration::ZERO).await.unwrap(), None);

    let third = shared.append(Bytes::from_static(b"third"), Timestamp(10)).await.unwrap().durable().await.unwrap();
    let head = replay.poll_head(Duration::ZERO).await.unwrap().unwrap();
    assert_eq!(head.metadata, third);
    cancellation.cancel();
    writer.await.unwrap().unwrap();
    let exact = ReadReservation::new(261, memory.reserve(261).unwrap());
    assert!(matches!(replay.read_head(head, exact).await, Err(Error::Closed(_))));
    assert_eq!(memory.used_bytes(), 0);
    log.close_and_wait().await;
}

#[test]
fn cancelled_local_replay_keeps_its_charge_until_the_worker_releases_the_undelivered_record() {
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
        let payload = Bytes::from(vec![b'p'; 64 * 1024 + 1]);
        let (log, driver) = LocalLog::open(directory.path().join("LOG"), LocalLogOptions { maximum_message_bytes: payload.len(), queue_capacity: 4 }).unwrap();
        let cancellation = CancellationToken::new();
        let writer = tokio::spawn(driver.run(cancellation.clone()));
        log.enqueue(payload.clone(), Timestamp(5)).await.unwrap().durable().await.unwrap();
        let mut tail = log.tail(LogOffset(-1)).await.unwrap();
        let replay = tail.replay().unwrap();
        let head = replay.poll_head(Duration::ZERO).await.unwrap().unwrap();
        let memory = MemoryBudget::new(head.allocation_bound);
        let (started, waiting) = oneshot::channel();
        let (resume, paused) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            paused.recv().unwrap();
        });
        waiting.await.unwrap();
        let (released, release) = oneshot::channel();
        let reservation = ReportRelease { reservation: Some(memory.reserve(head.allocation_bound).unwrap()), released: Some(released) };
        let mut read = Box::pin(replay.read_head(head, ReadReservation::new(head.allocation_bound, reservation)));
        poll_fn(|context| {
            assert!(read.as_mut().poll(context).is_pending());
            Poll::Ready(())
        }).await;
        drop(read);
        assert_eq!(memory.used_bytes(), head.allocation_bound);
        resume.send(()).unwrap();
        blocker.await.unwrap();
        release.await.unwrap();
        assert_eq!(memory.used_bytes(), 0);
        assert_eq!(replay.poll_head(Duration::ZERO).await.unwrap(), Some(head));

        let charge = ReadReservation::new(head.allocation_bound, memory.reserve(head.allocation_bound).unwrap());
        let record = replay.read_head(head, charge).await.unwrap();
        assert_eq!(record.payload, payload);
        drop(record);
        assert_eq!(replay.poll_head(Duration::ZERO).await.unwrap(), None);
        cancellation.cancel();
        writer.await.unwrap().unwrap();
        log.close_and_wait().await;
    });
}

#[tokio::test]
async fn memory_replay_delivery_keeps_its_charge_through_truncation_and_the_append_charge_ends_at_append() {
    let log = MemoryLog::new(1 << 20);
    let memory = MemoryBudget::new(1024);
    let append_bound = 256 + 7;
    let append_reservation = ReadReservation::new(append_bound, memory.reserve(append_bound).unwrap());
    let payload = LogRecord::new(LogMetadata { offset: LogOffset(-1), timestamp: Timestamp(0) }, Bytes::from_static(b"payload"))
        .retain(append_reservation).unwrap().payload;
    let first = log.enqueue(payload, Timestamp(5)).await.unwrap().durable().await.unwrap();
    let mut tail = log.tail(LogOffset(-1)).await.unwrap();
    let replay = tail.replay().unwrap();
    let head = replay.poll_head(Duration::ZERO).await.unwrap().unwrap();
    assert_eq!(head.metadata, first);
    assert_eq!(head.payload_len, 7);
    assert_eq!(head.allocation_bound, 256);
    assert_eq!(memory.used_bytes(), 0);

    let exact = ReadReservation::new(256, memory.reserve(256).unwrap());
    let record = replay.read_head(head, exact).await.unwrap();
    assert_eq!(record.metadata, first);
    let clone = record.payload.clone();
    drop(record);
    log.truncate_before(LogOffset(1)).unwrap();
    assert_eq!(clone.as_ref(), b"payload");
    assert_eq!(memory.used_bytes(), 256);
    drop(clone);
    assert_eq!(memory.used_bytes(), 0);

    let second = log.enqueue(Bytes::from_static(b"second"), Timestamp(6)).await.unwrap().durable().await.unwrap();
    let head = replay.poll_head(Duration::ZERO).await.unwrap().unwrap();
    assert_eq!(head.metadata, second);
    log.truncate_before(LogOffset(2)).unwrap();
    let exact = ReadReservation::new(256, memory.reserve(256).unwrap());
    assert!(matches!(replay.read_head(head, exact).await, Err(Error::UnavailableLogRange { requested: 1, first_retained: 2 })));
    assert_eq!(memory.used_bytes(), 0);
    assert!(matches!(replay.poll_head(Duration::ZERO).await, Err(Error::UnavailableLogRange { requested: 1, first_retained: 2 })));
    log.close();
    assert!(matches!(replay.poll_head(Duration::ZERO).await, Err(Error::Closed(_))));
}

#[tokio::test]
async fn local_range_reports_disappearance_of_an_observed_durable_file() {
    let directory = tempfile::tempdir().unwrap(); let path = directory.path().join("LOG");
    let cancel = CancellationToken::new();
    let (log, driver) = LocalLog::open(&path, LocalLogOptions { maximum_message_bytes: 1024, queue_capacity: 4 }).unwrap();
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
