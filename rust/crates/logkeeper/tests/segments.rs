mod common;

use common::{Server, append};
use rtdb_logkeeper::{ServerOptions, StreamConfig, drop_stream, open_reader, open_writer, retire, seal, trim};
use rtdb_storage::Error;
use rtdb_storage::log::{Log, LogOffset};
use rtdb_types::AnomalyCategory;
use std::path::Path;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn segments(data_dir: &Path) -> (usize, u64) {
    let files: Vec<_> = std::fs::read_dir(data_dir.join("segments")).unwrap().filter_map(|entry| {
        match entry.unwrap().metadata() {
            Ok(metadata) => Some(metadata.len()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => panic!("{error}"),
        }
    }).collect();
    (files.len(), files.iter().sum())
}

fn config(server: &Server, stream: usize) -> StreamConfig {
    StreamConfig { servers: vec![server.address.to_string().into()], stream: format!("deployment-{stream}/1/replica"), write_quorum: 1, maximum_message_bytes: 64 * 1024 }
}

async fn payloads(log: &dyn Log, from: i64) -> Vec<String> {
    log.read_range(LogOffset(from), LogOffset(i64::MAX)).await.unwrap().iter().map(|record| String::from_utf8(record.payload.to_vec()).unwrap()).collect()
}

async fn start(data_dir: &Path, cache_bytes: usize) -> Server { Server::start_with(data_dir, "127.0.0.1:0".parse().unwrap(), cache_bytes).await }

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retirement_rewrites_every_journal_and_permanently_closes_the_stream() {
    let mut ensemble = common::Ensemble::start(3).await;
    let disposed = ensemble.config("disposal-target");
    let retained = ensemble.config("unrelated");
    let writer = open_writer(disposed.clone(), 1, CancellationToken::new()).await.unwrap();
    let other = open_writer(retained.clone(), 1, CancellationToken::new()).await.unwrap();
    const PAYLOAD: &str = "remove this exact payload from the journals";
    append(writer.as_ref(), PAYLOAD, 0).await;
    append(other.as_ref(), "retain another stream", 0).await;
    drop((writer, other));

    ensemble.stop(2).await;
    assert!(retire(&disposed, 2).await.is_err(), "a quorum cannot certify every journal");
    ensemble.restart(2).await;
    retire(&disposed, 2).await.unwrap();
    for index in 0..3 {
        let segments = std::fs::read_dir(ensemble.directories[index].path().join("segments")).unwrap();
        for segment in segments {
            let bytes = std::fs::read(segment.unwrap().path()).unwrap();
            assert!(!bytes.windows(PAYLOAD.len()).any(|window| window == PAYLOAD.as_bytes()));
        }
        ensemble.stop(index).await;
        ensemble.restart(index).await;
    }
    retire(&disposed, 2).await.unwrap();
    assert!(open_writer(disposed, 3, CancellationToken::new()).await.is_err());
    let other = open_reader(retained, CancellationToken::new()).await.unwrap();
    assert_eq!(payloads(other.as_ref(), 0).await, ["retain another stream"]);
    for index in 0..3 { ensemble.stop(index).await; }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_share_segments_that_a_trim_compacts_and_a_restart_replays() {
    share_compact_and_replay(ServerOptions::DEFAULT_CACHE_BYTES).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_server_caching_almost_nothing_serves_every_entry_from_its_segments_across_a_compaction_and_a_restart() {
    share_compact_and_replay(4 * 1024).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_reclaims_dropped_payloads_and_keeps_the_terminal_record() {
    let directory = tempfile::tempdir().unwrap();
    let server = start(directory.path(), 4 * 1024).await;
    let dropped = config(&server, 1);
    let writer = open_writer(dropped.clone(), 1, CancellationToken::new()).await.unwrap();
    let other = open_writer(config(&server, 2), 1, CancellationToken::new()).await.unwrap();
    let payload = "x".repeat(40 * 1024);
    for position in 0..40 { append(writer.as_ref(), &payload, position).await; }
    append(other.as_ref(), "live neighbor", 40).await;
    drop((writer, other));

    let before = segments(directory.path()).1;
    drop_stream(&dropped, 2).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (count, bytes) = segments(directory.path());
            if count == 1 && bytes < before / 2 { return; }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.expect("the shared journal reclaims the dropped stream's payloads");
    server.stop().await;

    let server = start(directory.path(), 4 * 1024).await;
    let dropped = config(&server, 1);
    assert!(open_reader(dropped.clone(), CancellationToken::new()).await.is_err());
    assert!(open_writer(dropped, 3, CancellationToken::new()).await.is_err());
    let other = open_reader(config(&server, 2), CancellationToken::new()).await.unwrap();
    assert_eq!(payloads(other.as_ref(), 0).await, ["live neighbor"]);
    server.stop().await;
}

async fn share_compact_and_replay(cache_bytes: usize) {
    let directory = tempfile::tempdir().unwrap();
    let server = start(directory.path(), cache_bytes).await;
    let streams = 50;
    let mut writers = Vec::new();
    for stream in 0..streams {
        let writer = open_writer(config(&server, stream), 2, CancellationToken::new()).await.unwrap();
        for position in 0..3 { append(writer.as_ref(), &format!("{stream}:{position}"), position).await; }
        writers.push(writer);
    }
    assert_eq!(segments(directory.path()).0, 1, "every stream's records go to one shared segment");

    let large = "x".repeat(40 * 1024);
    for position in 3..43 { append(writers[0].as_ref(), &format!("{position}{large}"), position).await; }
    let reader = open_reader(config(&server, 0), CancellationToken::new()).await.unwrap();
    let all = payloads(reader.as_ref(), 0).await;
    assert_eq!((all.len(), all[2].as_str(), all[42].len()), (43, "0:2", 2 + large.len()), "a reader is served entries the cache no longer holds");
    drop(reader);
    let before = segments(directory.path()).1;
    trim(&config(&server, 0), LogOffset(40)).await.unwrap();
    append(writers[0].as_ref(), "after the trim", 43).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let reader = open_reader(config(&server, 0), CancellationToken::new()).await.unwrap();
            if reader.latest().unwrap() == LogOffset(43) { break; }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }).await.expect("the reader can see the appended entry before the journal stops");
    let mut compacted = segments(directory.path());
    for _ in 0..100 {
        if compacted.1 < before / 2 { break; }
        tokio::time::sleep(Duration::from_millis(20)).await;
        compacted = segments(directory.path());
    }
    assert!(compacted.1 < before / 2 && compacted.0 == 1, "a trim leaving the segments mostly dead compacts them: {before} bytes became {compacted:?}");
    drop(writers);
    let (_, address) = server.stop().await;

    // The streams' address-identified ensembles bind their journal, so it comes back at the same address.
    let server = Server::start_with(directory.path(), address, cache_bytes).await;
    for stream in 1..streams {
        let reader = open_reader(config(&server, stream), CancellationToken::new()).await.unwrap();
        assert_eq!(payloads(reader.as_ref(), 0).await, (0..3).map(|position| format!("{stream}:{position}")).collect::<Vec<_>>());
    }
    let reader = open_reader(config(&server, 0), CancellationToken::new()).await.unwrap();
    assert!(matches!(reader.read_range(LogOffset(0), LogOffset(44)).await, Err(Error::UnavailableLogRange { requested: 0, first_retained: 41 })));
    let tail = payloads(reader.as_ref(), 41).await;
    assert_eq!(tail.len(), 3);
    assert_eq!(tail[2], "after the trim");
    assert_eq!(seal(&config(&server, 7), 1).await.unwrap_err().category, AnomalyCategory::Conflict, "promises survive the restart");
    let writer = open_writer(config(&server, 0), 3, CancellationToken::new()).await.unwrap();
    assert_eq!(append(writer.as_ref(), "next", 44).await.offset, LogOffset(44));
    drop(writer);
    server.stop().await;
}
