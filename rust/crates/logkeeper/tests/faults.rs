mod common;

use bytes::Bytes;
use common::{Ensemble, Server, append};
use rtdb_logkeeper::{StreamConfig, drop_stream, open_reader, open_writer, seal};
use rtdb_storage::Error;
use rtdb_storage::log::{Log, LogOffset, LogRecord};
use rtdb_types::{AnomalyCategory, Timestamp};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn payloads(records: &[LogRecord]) -> Vec<String> {
    records.iter().map(|record| String::from_utf8(record.payload.to_vec()).unwrap()).collect()
}

async fn committed(log: &dyn Log) -> Vec<String> {
    payloads(&log.read_range(LogOffset(0), LogOffset(i64::MAX)).await.unwrap())
}

async fn wait_for_logkeeper(config: StreamConfig) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if open_reader(config.clone(), CancellationToken::new()).await.is_ok() { return; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("the restarted logkeeper serves reads");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deposed_writer_fails_its_next_append_and_its_successor_holds_everything_it_acknowledged() {
    let ensemble = Ensemble::start(3).await;
    let a = open_writer(ensemble.config("deposed"), 1, CancellationToken::new()).await.unwrap();
    for position in 0..10 { append(a.as_ref(), &format!("a{position}"), position).await; }
    let b = open_writer(ensemble.config("deposed"), 2, CancellationToken::new()).await.unwrap();
    assert_eq!(b.latest().unwrap(), LogOffset(9));
    // `a` may already have heard of the successor from its own background commit traffic, so the refusal can arrive at enqueue.
    let refused = match a.enqueue(Bytes::from_static(b"late"), Timestamp(10)).await {
        Ok(receipt) => receipt.durable().await.map(|_| ()),
        Err(error) => Err(error),
    };
    assert!(matches!(refused, Err(Error::Deposed { epoch: 1, promised: 2 })), "{refused:?}");
    let next = a.enqueue(Bytes::from_static(b"later"), Timestamp(11)).await;
    let anomaly = rtdb_types::Anomaly::from(next.err().unwrap());
    assert_eq!((anomaly.category, anomaly.code.as_str()), (AnomalyCategory::Conflict, "rtdb.storage/deposed"));
    assert_eq!(append(b.as_ref(), "b10", 12).await.offset, LogOffset(10));
    let expected: Vec<String> = (0..10).map(|position| format!("a{position}")).chain(["b10".to_owned()]).collect();
    assert_eq!(committed(b.as_ref()).await, expected);
    let reader = open_reader(ensemble.config("deposed"), CancellationToken::new()).await.unwrap();
    assert_eq!(committed(reader.as_ref()).await, expected);
    assert!(matches!(open_writer(ensemble.config("deposed"), 1, CancellationToken::new()).await, Err(anomaly) if anomaly.category == AnomalyCategory::Conflict));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn appends_commit_with_one_server_down_and_the_returning_server_is_repaired() {
    let mut ensemble = Ensemble::start(3).await;
    let writer = open_writer(ensemble.config("degraded"), 1, CancellationToken::new()).await.unwrap();
    for position in 0..5 { append(writer.as_ref(), &format!("e{position}"), position).await; }
    ensemble.stop(2).await;
    for position in 5..20 { append(writer.as_ref(), &format!("e{position}"), position).await; }
    ensemble.restart(2).await;
    let mut position = 20;
    let only_returned = ensemble.config_of(&[2], "degraded");
    let caught_up = loop {
        append(writer.as_ref(), &format!("e{position}"), position).await;
        position += 1;
        if let Ok(reader) = open_reader(only_returned.clone(), CancellationToken::new()).await
            && reader.latest().unwrap() == writer.latest().unwrap() { break reader; }
        assert!(position < 200, "the returned server was not repaired");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let expected: Vec<String> = (0..position).map(|position| format!("e{position}")).collect();
    assert_eq!(committed(caught_up.as_ref()).await, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_only_one_server_held_is_dropped_by_recovery_and_lsns_stay_dense() {
    let mut ensemble = Ensemble::start(3).await;
    let a = open_writer(ensemble.config("minority"), 1, CancellationToken::new()).await.unwrap();
    for position in 0..3 { append(a.as_ref(), &format!("a{position}"), position).await; }
    ensemble.stop(1).await;
    ensemble.stop(2).await;
    let stranded = a.enqueue(Bytes::from_static(b"stranded"), Timestamp(3)).await.unwrap();
    let only_first = StreamConfig { write_quorum: 1, ..ensemble.config_of(&[0], "minority") };
    tokio::time::sleep(Duration::from_millis(100)).await;
    ensemble.stop(0).await;
    ensemble.restart(1).await;
    ensemble.restart(2).await;

    let b = open_writer(ensemble.config("minority"), 2, CancellationToken::new()).await.unwrap();
    assert_eq!(b.latest().unwrap(), LogOffset(2));
    assert_eq!(append(b.as_ref(), "b3", 4).await.offset, LogOffset(3));
    ensemble.restart(0).await;
    let mut position = 4;
    let repaired = loop {
        append(b.as_ref(), &format!("b{position}"), position).await;
        position += 1;
        if let Ok(reader) = open_reader(only_first.clone(), CancellationToken::new()).await
            && reader.latest().unwrap() == b.latest().unwrap() { break reader; }
        assert!(position < 200, "the divergent server was not repaired");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let expected: Vec<String> = ["a0", "a1", "a2"].into_iter().map(str::to_owned).chain((3..position).map(|position| format!("b{position}"))).collect();
    assert_eq!(committed(repaired.as_ref()).await, expected);
    let all = open_reader(ensemble.config("minority"), CancellationToken::new()).await.unwrap();
    assert_eq!(committed(all.as_ref()).await, expected);
    drop(a);
    assert!(stranded.durable().await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_server_keeps_its_promise_entries_and_commit() {
    let directory = tempfile::tempdir().unwrap();
    let server = Server::start(directory.path()).await;
    let config = |server: &Server| StreamConfig { servers: vec![server.address.to_string().into()], stream: "restart".into(), write_quorum: 1, maximum_message_bytes: 1024 };
    let writer = open_writer(config(&server), 3, CancellationToken::new()).await.unwrap();
    for position in 0..4 { append(writer.as_ref(), &format!("r{position}"), position).await; }
    let told = open_reader(config(&server), CancellationToken::new()).await.unwrap();
    let mut tail = told.tail(LogOffset(2)).await.unwrap();
    assert_eq!(tail.poll(Duration::from_secs(5)).await.unwrap()[0].metadata.offset, LogOffset(3), "the server has been told the last commit");
    drop(writer);
    server.stop().await;

    let server = Server::start(directory.path()).await;
    let reader = open_reader(config(&server), CancellationToken::new()).await.unwrap();
    assert_eq!(reader.latest().unwrap(), LogOffset(3));
    assert_eq!(committed(reader.as_ref()).await, ["r0", "r1", "r2", "r3"]);
    let refused = seal(&config(&server), 2).await.unwrap_err();
    assert_eq!(refused.category, AnomalyCategory::Conflict);
    assert_eq!(seal(&config(&server), 4).await.unwrap(), LogOffset(3));
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drop_is_retryable_after_one_replica_misses_it_and_survives_restart() {
    let mut ensemble = Ensemble::start(3).await;
    let config = ensemble.config("deleted");
    let writer = open_writer(config.clone(), 1, CancellationToken::new()).await.unwrap();
    append(writer.as_ref(), "erase this", 1).await;
    let other = open_writer(ensemble.config("other"), 1, CancellationToken::new()).await.unwrap();
    append(other.as_ref(), "keep this", 2).await;
    drop(other);

    ensemble.stop(2).await;
    assert!(drop_stream(&config, 2).await.is_err());
    for server in [0, 1] {
        let dropped = ensemble.config_of(&[server], "deleted");
        assert!(open_reader(dropped.clone(), CancellationToken::new()).await.is_err());
        assert!(open_writer(dropped, 3, CancellationToken::new()).await.is_err());
    }
    let refused = writer.enqueue(Bytes::from_static(b"late"), Timestamp(3)).await.unwrap().durable().await;
    assert!(matches!(refused, Err(Error::Deposed { epoch: 1, promised: 2 })), "{refused:?}");
    drop(writer);
    ensemble.restart(2).await;
    wait_for_logkeeper(ensemble.config_of(&[2], "deleted")).await;
    drop_stream(&config, 2).await.unwrap();
    for server in 0..3 {
        ensemble.stop(server).await;
        ensemble.restart(server).await;
        wait_for_logkeeper(ensemble.config_of(&[server], "other")).await;
        let dropped = ensemble.config_of(&[server], "deleted");
        assert!(open_reader(dropped.clone(), CancellationToken::new()).await.is_err());
        assert!(open_writer(dropped, 3, CancellationToken::new()).await.is_err());
    }
    assert!(drop_stream(&config, 1).await.is_err());
    drop_stream(&config, 2).await.unwrap();
    let other = open_reader(ensemble.config("other"), CancellationToken::new()).await.unwrap();
    assert_eq!(committed(other.as_ref()).await, ["keep this"]);
}

#[test]
fn idle_streams_cost_the_server_no_tasks() {
    let server_runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let (address, cancellation, served) = server_runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let options = rtdb_logkeeper::ServerOptions { data_dir: directory.path().to_path_buf(), fsync: false, cache_bytes: rtdb_logkeeper::ServerOptions::DEFAULT_CACHE_BYTES };
        (address, cancellation.clone(), tokio::spawn(rtdb_logkeeper::serve(listener, options, cancellation)))
    });
    let settle = |runtime: &tokio::runtime::Runtime| {
        for _ in 0..100 {
            std::thread::sleep(Duration::from_millis(10));
            if runtime.metrics().num_alive_tasks() <= 4 { break; }
        }
        runtime.metrics().num_alive_tasks()
    };
    let client_runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
    let config = |stream: usize| StreamConfig { servers: vec![address.to_string().into()], stream: format!("deployment-{stream}/1/replica"), write_quorum: 1, maximum_message_bytes: 1024 };
    let writers = client_runtime.block_on(async {
        let first = open_writer(config(0), 1, CancellationToken::new()).await.unwrap();
        append(first.as_ref(), "one", 0).await;
        vec![first]
    });
    let one_stream = settle(&server_runtime);
    let writers = client_runtime.block_on(async move {
        let mut writers = writers;
        for stream in 1..1000 {
            let writer = open_writer(config(stream), 1, CancellationToken::new()).await.unwrap();
            append(writer.as_ref(), "one", 0).await;
            writers.push(writer);
        }
        writers
    });
    let thousand_streams = settle(&server_runtime);
    assert_eq!(thousand_streams, one_stream, "idle streams own no server task");
    drop(writers);
    cancellation.cancel();
    server_runtime.block_on(served).unwrap().unwrap();
}
