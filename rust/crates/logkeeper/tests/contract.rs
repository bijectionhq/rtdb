mod common;

use bytes::Bytes;
use common::{Ensemble, append};
use rtdb_logkeeper::{open_reader, open_writer};
use rtdb_storage::log::{Log, LogOffset};
use rtdb_storage::{Error, object_store::ReadReservation};
use rtdb_types::{MemoryBudget, Timestamp};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_audit_distinguishes_equal_committed_positions_with_different_payloads() {
    let ensemble = Ensemble::start(2).await;
    for (server, payload) in [(0, "first"), (1, "different")] {
        let config = ensemble.config_of(&[server], "split-history");
        let log = open_writer(config, 1, CancellationToken::new()).await.unwrap();
        assert_eq!(append(log.as_ref(), payload, 42).await.offset, LogOffset(0));
    }
    let stream = ensemble.config("split-history");
    let report = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let report = rtdb_logkeeper::audit_retained(&stream).await.unwrap();
            if report.interval.is_some() { break report; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    assert_eq!(report.interval, Some([0, 0]));
    assert_eq!(report.digests.len(), 2);
    assert_ne!(report.digests[&stream.servers[0]], report.digests[&stream.servers[1]]);
    assert!(report.unavailable.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_journal_matches_the_online_digest_across_epochs_and_preserves_damaged_bytes() {
    use std::io::Write;

    let mut ensemble = Ensemble::start(2).await;
    let stream = ensemble.config("offline-history");
    let original = open_writer(stream.clone(), 1, CancellationToken::new()).await.unwrap();
    append(original.as_ref(), "discarded", 1).await;
    append(original.as_ref(), "first epoch", 2).await;
    let successor = open_writer(stream.clone(), 2, CancellationToken::new()).await.unwrap();
    append(successor.as_ref(), "second epoch", 3).await;

    rtdb_logkeeper::trim(&stream, LogOffset(0)).await.unwrap();
    let online = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let report = rtdb_logkeeper::audit_retained(&stream).await.unwrap();
            if report.interval == Some([1, 2]) { break report; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    assert!(rtdb_logkeeper::inspect_retained(ensemble.directories[0].path(), &stream.stream, None).is_err());
    drop((successor, original));
    ensemble.stop(0).await;
    ensemble.stop(1).await;

    let directory = ensemble.directories[0].path();
    let segments = directory.join("segments");
    let temporary = segments.join("unfinished.tmp");
    std::fs::write(&temporary, b"pending").unwrap();
    let offline = rtdb_logkeeper::inspect_retained(directory, &stream.stream, Some([1, 2])).unwrap().unwrap();
    assert_eq!(offline.interval, [1, 2]);
    assert_eq!(offline.digest, online.digests[&stream.servers[0]]);
    assert_eq!(std::fs::read(&temporary).unwrap(), b"pending");
    assert!(rtdb_logkeeper::inspect_retained(directory, &stream.stream, Some([0, 2])).is_err());

    let segment = std::fs::read_dir(&segments).unwrap().map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "seg")).max().unwrap();
    let sequence: u64 = segment.file_stem().unwrap().to_str().unwrap().parse().unwrap();
    let alias = segments.join(format!("{sequence}.seg"));
    std::fs::copy(&segment, &alias).unwrap();
    assert!(rtdb_logkeeper::inspect_retained(directory, &stream.stream, None).is_err());
    std::fs::remove_file(alias).unwrap();

    std::fs::OpenOptions::new().append(true).open(&segment).unwrap().write_all(b"damaged").unwrap();
    let damaged = std::fs::read(&segment).unwrap();
    assert!(rtdb_logkeeper::inspect_retained(directory, &stream.stream, None).is_err());
    assert_eq!(std::fs::read(&segment).unwrap(), damaged);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offsets_start_at_zero_and_tails_ranges_and_latest_follow_the_committed_tip() {
    let ensemble = Ensemble::start(3).await;
    let log = open_writer(ensemble.config("contract"), 1, CancellationToken::new()).await.unwrap();
    assert_eq!(log.latest().unwrap(), LogOffset(-1));
    assert!(log.read_range(LogOffset(0), LogOffset(0)).await.unwrap().is_empty());
    assert!(matches!(log.read_range(LogOffset(10), LogOffset(10)).await, Err(Error::UnavailableLogRange { .. })));
    assert!(log.read_range(LogOffset(0), LogOffset(-1)).await.is_err());
    let mut tail = log.tail(LogOffset(-1)).await.unwrap();
    assert!(tail.poll(Duration::ZERO).await.unwrap().is_empty());

    let first = append(log.as_ref(), "abc", 42).await;
    let second = append(log.as_ref(), "xyz12", 43).await;
    assert_eq!((first.offset, first.timestamp), (LogOffset(0), Timestamp(42)));
    assert_eq!(second.offset, LogOffset(1));
    assert_eq!(log.latest().unwrap(), LogOffset(1));
    let records = tail.poll(Duration::ZERO).await.unwrap();
    assert_eq!(records.iter().map(|record| record.metadata).collect::<Vec<_>>(), [first, second]);
    assert_eq!(records[1].payload.as_ref(), b"xyz12");
    assert!(tail.poll(Duration::ZERO).await.unwrap().is_empty(), "a tail between polls keeps its exact position");
    let third = append(log.as_ref(), "q", 44).await;
    assert_eq!(tail.poll(Duration::from_secs(1)).await.unwrap()[0].metadata, third);

    assert_eq!(log.read_range(LogOffset(1), LogOffset(10)).await.unwrap().iter().map(|record| record.metadata).collect::<Vec<_>>(), [second, third]);
    assert!(log.read_range(LogOffset(3), LogOffset(3)).await.unwrap().is_empty(), "the end of the durable tip is a valid start");
    assert!(matches!(log.read_range(LogOffset(4), LogOffset(5)).await, Err(Error::UnavailableLogRange { .. })));
    assert!(log.tail(LogOffset(5)).await.is_err());

    let memory = MemoryBudget::new(1 << 20);
    let mut range = log.range(LogOffset(0), LogOffset(2)).await.unwrap();
    let bound = range.allocation_bound();
    let mut ranged = Vec::new();
    while let Some(record) = range.next(ReadReservation::new(bound, memory.reserve(bound).unwrap())).await.unwrap() { ranged.push(record.metadata); }
    assert_eq!(ranged, [first, second]);
    assert!(log.range(LogOffset(9), LogOffset(12)).await.unwrap().next(ReadReservation::new(bound, memory.reserve(bound).unwrap())).await.unwrap().is_none(), "starts beyond the tip are an empty range");
    assert!(log.range(LogOffset(2), LogOffset(1)).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declined_oversized_message_does_not_consume_a_position() {
    let ensemble = Ensemble::start(3).await;
    let log = open_writer(ensemble.config("oversized"), 1, CancellationToken::new()).await.unwrap();
    assert!(matches!(log.enqueue(Bytes::from(vec![0; 1025]), Timestamp(0)).await, Err(Error::MessageTooLarge { size: 1025, maximum: 1024 })));
    assert_eq!(append(log.as_ref(), "ok", 1).await.offset, LogOffset(0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipelined_appends_commit_in_order_and_a_tail_delivers_a_capped_prefix() {
    let ensemble = Ensemble::start(3).await;
    let log = open_writer(ensemble.config("pipelined"), 1, CancellationToken::new()).await.unwrap();
    let mut receipts = Vec::new();
    for position in 0..1500 { receipts.push(log.enqueue(Bytes::from(format!("record {position}")), Timestamp(position)).await.unwrap()); }
    for (position, receipt) in receipts.into_iter().enumerate() {
        assert_eq!(receipt.durable().await.unwrap().offset, LogOffset(position as i64));
    }
    let reader = open_reader(ensemble.config("pipelined"), CancellationToken::new()).await.unwrap();
    let mut tail = reader.tail(LogOffset(-1)).await.unwrap();
    let mut seen = Vec::new();
    while seen.len() < 1500 {
        let batch = tail.poll(Duration::from_secs(5)).await.unwrap();
        assert!(!batch.is_empty() && batch.len() <= 1024);
        seen.extend(batch);
    }
    assert!(seen.iter().enumerate().all(|(position, record)| record.metadata.offset == LogOffset(position as i64) && record.payload == format!("record {position}").as_bytes()));
    assert_eq!(reader.latest().unwrap(), LogOffset(1499));
    assert!(tail.poll(Duration::ZERO).await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reader_cannot_append_and_long_polls_for_the_writers_commits() {
    let ensemble = Ensemble::start(3).await;
    let log = open_writer(ensemble.config("follow"), 1, CancellationToken::new()).await.unwrap();
    let reader = open_reader(ensemble.config("follow"), CancellationToken::new()).await.unwrap();
    assert!(matches!(reader.enqueue(Bytes::from_static(b"no"), Timestamp(0)).await, Err(Error::ReadOnlyLog(_))));
    let mut tail = reader.tail(LogOffset(-1)).await.unwrap();
    let polling = tokio::spawn(async move { tail.poll(Duration::from_secs(5)).await.unwrap() });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let appended = append(log.as_ref(), "late", 7).await;
    assert_eq!(polling.await.unwrap()[0].metadata, appended);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replay_tail_charges_the_record_and_keeps_a_refused_head() {
    let ensemble = Ensemble::start(3).await;
    let log = open_writer(ensemble.config("replay"), 1, CancellationToken::new()).await.unwrap();
    let first = append(log.as_ref(), "first", 5).await;
    let mut tail = log.tail(LogOffset(-1)).await.unwrap();
    let replay = tail.replay().unwrap();
    let head = replay.poll_head(Duration::ZERO).await.unwrap().unwrap();
    assert_eq!((head.metadata, head.payload_len, head.allocation_bound), (first, 5, 261));
    let memory = MemoryBudget::new(261);
    assert!(matches!(replay.read_head(head, ReadReservation::new(260, memory.reserve(260).unwrap())).await, Err(Error::Task(_))));
    assert_eq!(replay.poll_head(Duration::ZERO).await.unwrap(), Some(head));
    let record = replay.read_head(head, ReadReservation::new(261, memory.reserve(261).unwrap())).await.unwrap();
    assert_eq!(memory.used_bytes(), 261);
    drop(record);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(replay.poll_head(Duration::ZERO).await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trimmed_positions_are_unavailable_to_tails_and_ranges() {
    let ensemble = Ensemble::start(3).await;
    let config = ensemble.config("trimmed");
    let log = open_writer(config.clone(), 1, CancellationToken::new()).await.unwrap();
    for position in 0..5 { append(log.as_ref(), "x", position).await; }
    rtdb_logkeeper::trim(&config, LogOffset(2)).await.unwrap();
    let reader = open_reader(config, CancellationToken::new()).await.unwrap();
    assert!(matches!(reader.tail(LogOffset(0)).await, Err(Error::UnavailableLogRange { requested: 1, first_retained: 3 })));
    assert!(matches!(reader.read_range(LogOffset(0), LogOffset(5)).await, Err(Error::UnavailableLogRange { requested: 0, first_retained: 3 })));
    assert_eq!(reader.read_range(LogOffset(3), LogOffset(5)).await.unwrap().len(), 2);
    assert_eq!(reader.tail(LogOffset(2)).await.unwrap().poll(Duration::ZERO).await.unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_log_refuses_appends_and_reads() {
    let ensemble = Ensemble::start(3).await;
    let cancellation = CancellationToken::new();
    let log = open_writer(ensemble.config("cancelled"), 1, cancellation.clone()).await.unwrap();
    append(log.as_ref(), "kept", 1).await;
    cancellation.cancel();
    assert!(matches!(log.enqueue(Bytes::from_static(b"late"), Timestamp(2)).await, Err(Error::Closed(_))));
    assert!(matches!(log.latest(), Err(Error::Closed(_))));
}

#[tokio::test]
async fn one_logkeeper_address_cannot_fill_multiple_quorum_slots() {
    let config = rtdb_logkeeper::StreamConfig {
        servers: vec!["127.0.0.1:9990".into(), "127.0.0.1:9990".into(), "127.0.0.1:9991".into()],
        stream: "duplicate-replica-address".into(),
        write_quorum: 2,
        maximum_message_bytes: 1 << 20,
    };
    let error = rtdb_logkeeper::open_writer(config, 1, tokio_util::sync::CancellationToken::new())
        .await.err().expect("a duplicate address is refused before opening the ensemble");
    assert_eq!(error.code, "rtdb.logkeeper/invalid-stream-config");
}
