mod common;

use common::{Server, append};
use rtdb_logkeeper::{StreamConfig, open_reader, open_writer, seal, trim};
use rtdb_storage::Error;
use rtdb_storage::log::{Log, LogOffset};
use rtdb_types::AnomalyCategory;
use std::path::Path;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn segments(data_dir: &Path) -> (usize, u64) {
    let files: Vec<_> = std::fs::read_dir(data_dir.join("segments")).unwrap().map(|entry| entry.unwrap().metadata().unwrap().len()).collect();
    (files.len(), files.iter().sum())
}

fn config(server: &Server, stream: usize) -> StreamConfig {
    StreamConfig { servers: vec![server.address.to_string()], stream: format!("deployment-{stream}/1/replica"), write_quorum: 1, maximum_message_bytes: 64 * 1024 }
}

async fn payloads(log: &dyn Log, from: i64) -> Vec<String> {
    log.read_range(LogOffset(from), LogOffset(i64::MAX)).await.unwrap().iter().map(|record| String::from_utf8(record.payload.to_vec()).unwrap()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_share_segments_that_a_trim_compacts_and_a_restart_replays() {
    let directory = tempfile::tempdir().unwrap();
    let server = Server::start(directory.path()).await;
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
    let before = segments(directory.path()).1;
    trim(&config(&server, 0), LogOffset(40)).await.unwrap();
    append(writers[0].as_ref(), "after the trim", 43).await;
    let mut compacted = segments(directory.path());
    for _ in 0..100 {
        if compacted.1 < before / 2 { break; }
        tokio::time::sleep(Duration::from_millis(20)).await;
        compacted = segments(directory.path());
    }
    assert!(compacted.1 < before / 2 && compacted.0 == 1, "a trim leaving the segments mostly dead compacts them: {before} bytes became {compacted:?}");
    drop(writers);
    server.stop().await;

    let server = Server::start(directory.path()).await;
    for stream in 1..streams {
        let reader = open_reader(config(&server, stream), CancellationToken::new()).await.unwrap();
        assert_eq!(payloads(reader.as_ref(), 0).await, (0..3).map(|position| format!("{stream}:{position}")).collect::<Vec<_>>());
    }
    let reader = open_reader(config(&server, 0), CancellationToken::new()).await.unwrap();
    assert!(matches!(reader.read_range(LogOffset(0), LogOffset(44)).await, Err(Error::UnavailableLogRange { requested: 0, first_retained: 41 })));
    let tail = payloads(reader.as_ref(), 41).await;
    assert_eq!((tail.len(), tail[2].as_str()), (3, "after the trim"));
    assert_eq!(seal(&config(&server, 7), 1).await.unwrap_err().category, AnomalyCategory::Conflict, "promises survive the restart");
    let writer = open_writer(config(&server, 0), 3, CancellationToken::new()).await.unwrap();
    assert_eq!(append(writer.as_ref(), "next", 44).await.offset, LogOffset(44));
    drop(writer);
    server.stop().await;
}
