mod common;

use bytes::Bytes;
use common::{CountingProxy, Ensemble, append};
use rtdb_logkeeper::{StreamConfig, open_reader, open_writer, seal};
use rtdb_storage::log::{Log, LogOffset};
use rtdb_types::{AnomalyCategory, Timestamp};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writer_confirms_its_leadership_while_idle_and_while_appending() {
    let ensemble = Ensemble::start(3).await;
    let log = open_writer(ensemble.config("confirmed"), 1, CancellationToken::new()).await.unwrap();
    log.confirm_leadership().await.unwrap();
    append(log.as_ref(), "one", 1).await;
    log.confirm_leadership().await.unwrap();
    let receipts: Vec<_> = futures::future::join_all((0..200).map(|position| log.enqueue(Bytes::from(format!("busy {position}")), Timestamp(position)))).await;
    let confirmations = futures::future::join_all((0..5).map(|_| log.confirm_leadership())).await;
    assert!(confirmations.iter().all(Result::is_ok), "{confirmations:?}");
    for receipt in receipts { receipt.unwrap().durable().await.unwrap(); }
    log.confirm_leadership().await.unwrap();
    let reader = open_reader(ensemble.config("confirmed"), CancellationToken::new()).await.unwrap();
    assert!(matches!(reader.confirm_leadership().await, Err(anomaly) if anomaly.code.as_str() == "rtdb.storage/read-only-log"), "only a writer confirms");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writer_sealed_beyond_its_epoch_fails_to_confirm_without_appending() {
    let ensemble = Ensemble::start(3).await;
    let config = ensemble.config("sealed");
    let log = open_writer(config.clone(), 1, CancellationToken::new()).await.unwrap();
    append(log.as_ref(), "kept", 1).await;
    log.confirm_leadership().await.unwrap();
    seal(&config, 2).await.unwrap();
    let refused = log.confirm_leadership().await.unwrap_err();
    assert_eq!((refused.category, refused.code.as_str()), (AnomalyCategory::Conflict, "rtdb.storage/deposed"));
    let anomaly = rtdb_types::Anomaly::from(log.latest().unwrap_err());
    assert_eq!(anomaly.code.as_str(), "rtdb.storage/deposed", "the failed confirmation deposes the writer");
    assert!(log.confirm_leadership().await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_append_already_on_the_wire_cannot_commit_after_its_successor_seals() {
    let ensemble = Ensemble::start(3).await;
    let config = ensemble.config("paused-append");
    let initial = open_writer(config.clone(), 1, CancellationToken::new()).await.unwrap();
    assert_eq!(append(initial.as_ref(), "committed", 1).await.offset, LogOffset(0));
    drop(initial);

    let mut proxies = Vec::new();
    for address in &ensemble.addresses { proxies.push(CountingProxy::start(*address).await); }
    let proxied = StreamConfig { servers: proxies.iter().map(|proxy| proxy.address.to_string()).collect(), ..config.clone() };
    let old = open_writer(proxied, 2, CancellationToken::new()).await.unwrap();
    for proxy in &proxies { proxy.hold(true); }
    let stranded = old.enqueue(Bytes::from_static(b"stranded"), Timestamp(2)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while proxies.iter().filter(|proxy| proxy.appends() > 0).count() < config.write_quorum {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }).await.expect("a quorum of proxies captured the old append");

    let successor = open_writer(config.clone(), 3, CancellationToken::new()).await.unwrap();
    assert_eq!(successor.latest().unwrap(), LogOffset(0));
    for proxy in &proxies { proxy.hold(false); }
    let refused = tokio::time::timeout(Duration::from_secs(5), stranded.durable()).await.unwrap();
    assert!(matches!(refused, Err(rtdb_storage::Error::Deposed { epoch: 2, promised: 3 })), "{refused:?}");
    tokio::time::timeout(Duration::from_secs(5), async {
        while !proxies.iter().any(|proxy| proxy.append_refusal_epoch() == 3) {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }).await.expect("a captured append received an epoch-3 refusal");
    assert_eq!(append(successor.as_ref(), "successor", 3).await.offset, LogOffset(1));
    let reader = open_reader(config, CancellationToken::new()).await.unwrap();
    let records = reader.read_range(LogOffset(0), LogOffset(2)).await.unwrap();
    assert_eq!(records.into_iter().map(|record| record.payload).collect::<Vec<_>>(),
        [Bytes::from_static(b"committed"), Bytes::from_static(b"successor")]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_confirmations_share_rounds() {
    let ensemble = Ensemble::start(3).await;
    let mut proxies = Vec::new();
    for address in &ensemble.addresses { proxies.push(CountingProxy::start(*address).await); }
    let config = StreamConfig { servers: proxies.iter().map(|proxy| proxy.address.to_string()).collect(), stream: "batched".into(), write_quorum: 2, maximum_message_bytes: 1024 };
    let log = open_writer(config, 1, CancellationToken::new()).await.unwrap();
    append(log.as_ref(), "one", 1).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let before: usize = proxies.iter().map(CountingProxy::requests).sum();
    for proxy in &proxies { proxy.hold(true); }
    let callers = 500;
    let tasks: Vec<_> = (0..callers).map(|_| { let log = Arc::clone(&log); tokio::spawn(async move { log.confirm_leadership().await }) }).collect();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    for proxy in &proxies { proxy.hold(false); }
    for task in tasks { task.await.unwrap().unwrap(); }
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let requests = proxies.iter().map(CountingProxy::requests).sum::<usize>() - before;
    assert_eq!(requests, 6, "{callers} concurrent confirmations take the round in flight and one more, a check per server each");
    assert_eq!(log.latest().unwrap(), LogOffset(0));
}
