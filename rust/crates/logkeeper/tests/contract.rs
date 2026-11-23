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
async fn a_physical_trim_receipt_requires_every_journal_and_survives_a_retry() {
    let mut ensemble = Ensemble::start(3).await;
    let stream = ensemble.config("all-trim");
    let writer = open_writer(stream.clone(), 1, CancellationToken::new()).await.unwrap();
    assert_eq!(append(writer.as_ref(), "offloaded", 1).await.offset, LogOffset(0));
    ensemble.stop(2).await;
    rtdb_logkeeper::trim(&stream, LogOffset(0)).await.unwrap();
    assert!(rtdb_logkeeper::trim_all(&stream, LogOffset(0)).await.is_err());
    ensemble.restart(2).await;
    let stream = ensemble.config("all-trim");
    rtdb_logkeeper::trim_all(&stream, LogOffset(0)).await.unwrap();
    rtdb_logkeeper::trim_all(&stream, LogOffset(0)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_trim_rejects_a_journal_that_accepts_ordinary_trim_without_fsync() {
    let data = tempfile::tempdir().unwrap();
    let server = common::Server::start_with_durability(data.path(), "127.0.0.1:0".parse().unwrap(), 0, false).await;
    let stream = rtdb_logkeeper::StreamConfig {
        servers: vec![server.address.to_string().into()], stream: "nondurable-trim".into(),
        write_quorum: 1, maximum_message_bytes: 1024,
    };
    let writer = open_writer(stream.clone(), 1, CancellationToken::new()).await.unwrap();
    append(writer.as_ref(), "trimmed", 1).await;
    rtdb_logkeeper::trim(&stream, LogOffset(0)).await.unwrap();
    assert!(rtdb_logkeeper::trim_all(&stream, LogOffset(0)).await.is_err());
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_trim_appends_a_fresh_event_for_the_current_floor_even_on_repeat() {
    let mut ensemble = Ensemble::start(1).await;
    let stream = ensemble.config_of(&[0], "durable-trim-retry");
    let writer = open_writer(stream.clone(), 1, CancellationToken::new()).await.unwrap();
    append(writer.as_ref(), "released", 1).await;
    append(writer.as_ref(), "retained", 2).await;
    let directory = ensemble.directories[0].path().to_owned();
    let journal_bytes = || std::fs::read_dir(directory.join("segments")).unwrap()
        .map(|file| file.unwrap().metadata().unwrap().len()).sum::<u64>();
    rtdb_logkeeper::trim(&stream, LogOffset(0)).await.unwrap();
    let ordinary = journal_bytes();
    rtdb_logkeeper::trim(&stream, LogOffset(-1)).await.unwrap();
    assert_eq!(journal_bytes(), ordinary);
    rtdb_logkeeper::trim_all(&stream, LogOffset(-1)).await.unwrap();
    let first = journal_bytes();
    assert!(first > ordinary);
    rtdb_logkeeper::trim_all(&stream, LogOffset(0)).await.unwrap();
    assert!(journal_bytes() > first);
    drop(writer);
    ensemble.stop(0).await;
    let retained = rtdb_logkeeper::inspect_retained(&directory, &stream.stream, None).unwrap().unwrap();
    assert_eq!(retained.interval, [1, 1]);
}

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
    assert_ne!(report.digests[stream.servers[0].address()], report.digests[stream.servers[1].address()]);
    assert!(report.unavailable.is_empty());
    assert_eq!(rtdb_logkeeper::certify_committed_prefix(&stream, 0, None).await.unwrap_err().code, "rtdb.logkeeper/audit-divergence");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reachable_empty_replica_does_not_hide_the_fsynced_quorum_from_an_audit() {
    let ensemble = Ensemble::start(3).await;
    let writer = open_writer(ensemble.config_of(&[0, 1], "audit-quorum"), 1, CancellationToken::new()).await.unwrap();
    append(writer.as_ref(), "first", 42).await;
    append(writer.as_ref(), "second", 43).await;
    let all = ensemble.config("audit-quorum");
    let report = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let report = rtdb_logkeeper::audit_retained(&all).await.unwrap();
            if report.interval == Some([0, 1]) { break report; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    assert_eq!(report.digests.len(), 2);
    assert_eq!(report.digests[all.servers[0].address()], report.digests[all.servers[1].address()]);
    assert_eq!(report.windows[all.servers[2].address()], None);
    assert!(report.unavailable.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_durable_quorum_proves_multiple_pages_and_continues_the_prefix_after_trim() {
    use rtdb_logkeeper::{certify_committed_prefix, inspect_retained, trim};

    let mut ensemble = Ensemble::start(3).await;
    let stream = ensemble.config("durable-prefix");
    let writer = open_writer(stream.clone(), 1, CancellationToken::new()).await.unwrap();
    let mut receipts = Vec::new();
    for position in 0..1050 {
        receipts.push(writer.enqueue(Bytes::from(position.to_string()), Timestamp(position)).await.unwrap());
    }
    for receipt in receipts { receipt.durable().await.unwrap(); }
    let wait_for_prefix = |through, prefix| {
        let stream = &stream;
        async move {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    match certify_committed_prefix(stream, through, prefix).await {
                        Ok(proof) => break proof,
                        Err(error) if error.code == "rtdb.logkeeper/audit-quorum" => tokio::time::sleep(Duration::from_millis(10)).await,
                        Err(error) => panic!("{error}"),
                    }
                }
            }).await.unwrap()
        }
    };
    let prefix = wait_for_prefix(1024, None).await;
    let full = wait_for_prefix(1049, None).await;
    assert_eq!(prefix.interval, [0, 1024]);
    assert_eq!(full.interval, [0, 1049]);
    trim(&stream, LogOffset(1024)).await.unwrap();
    assert_eq!(wait_for_prefix(1049, Some(&prefix)).await, full);

    let mut wrong = prefix.clone();
    wrong.stream = "another-deployment".into();
    assert_eq!(certify_committed_prefix(&stream, 1049, Some(&wrong)).await.unwrap_err().code, "rtdb.logkeeper/audit-prefix");
    wrong.stream.clone_from(&stream.stream);
    wrong.interval[1] -= 1;
    assert_eq!(certify_committed_prefix(&stream, 1049, Some(&wrong)).await.unwrap_err().code, "rtdb.logkeeper/audit-quorum");
    drop(writer);
    ensemble.stop(0).await;
    ensemble.stop(1).await;
    let suffix = rtdb_logkeeper::inspect_retained_with_prefix(ensemble.directories[0].path(), &stream.stream, None, Some(&prefix)).unwrap().unwrap();
    assert_eq!(suffix, full);
    assert_eq!(inspect_retained(ensemble.directories[0].path(), &stream.stream, None).unwrap().unwrap().interval, [1025, 1049]);
    assert_eq!(certify_committed_prefix(&stream, 1049, Some(&prefix)).await.unwrap_err().code, "rtdb.logkeeper/audit-quorum");
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
    assert_eq!(offline.digest, online.digests[stream.servers[0].address()]);
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
async fn a_saved_prefix_verifies_the_same_chain_after_trim_and_an_epoch_change() {
    let mut ensemble = Ensemble::start(3).await;
    let config = ensemble.config("continued-history");
    let first = open_writer(config.clone(), 1, CancellationToken::new()).await.unwrap();
    append(first.as_ref(), "first", 1).await;
    let second = open_writer(config.clone(), 2, CancellationToken::new()).await.unwrap();
    append(second.as_ref(), "second", 2).await;
    append(second.as_ref(), "third", 3).await;
    let initial = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let report = rtdb_logkeeper::audit_retained(&config).await.unwrap();
            if report.interval == Some([0, 2]) && report.digests.contains_key(config.servers[0].address()) {
                return report.digests[config.servers[0].address()].clone();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.unwrap();
    drop((first, second));
    ensemble.stop(0).await;
    let directory = ensemble.directories[0].path();
    let prefix = rtdb_logkeeper::inspect_retained(directory, &config.stream, Some([0, 0])).unwrap().unwrap();

    ensemble.restart(0).await;
    let node = ensemble.config_of(&[0], &config.stream);
    rtdb_logkeeper::trim(&node, LogOffset(0)).await.unwrap();
    ensemble.stop(0).await;
    let directory = ensemble.directories[0].path();
    assert!(rtdb_logkeeper::inspect_retained(directory, &config.stream, Some([0, 2])).is_err());
    let continued = rtdb_logkeeper::inspect_retained_with_prefix(directory, &config.stream, None, Some(&prefix)).unwrap().unwrap();
    assert_eq!(continued.interval, [0, 2]);
    assert_eq!(continued.digest, initial);
    assert!(rtdb_logkeeper::inspect_retained_with_prefix(directory, &config.stream, Some([2, 2]), Some(&prefix)).is_err());
    let mut wrong_stream = prefix.clone();
    wrong_stream.stream = "another".into();
    assert!(rtdb_logkeeper::inspect_retained_with_prefix(directory, &config.stream, None, Some(&wrong_stream)).is_err());
    let mut wrong_digest = prefix;
    wrong_digest.digest = "00".repeat(32);
    let mismatching = rtdb_logkeeper::inspect_retained_with_prefix(directory, &config.stream, None, Some(&wrong_digest)).unwrap().unwrap();
    assert_ne!(mismatching.digest, initial);
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
    assert_eq!(log.retained_floor().await.unwrap(), Some(LogOffset(0)));
    rtdb_logkeeper::trim(&config, LogOffset(2)).await.unwrap();
    let reader = open_reader(config, CancellationToken::new()).await.unwrap();
    assert_eq!(reader.retained_floor().await.unwrap(), Some(LogOffset(3)));
    assert_eq!(log.retained_floor().await.unwrap(), Some(LogOffset(3)));
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
