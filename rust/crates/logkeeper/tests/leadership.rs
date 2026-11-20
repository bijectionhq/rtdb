mod common;

use bytes::Bytes;
use common::{CountingProxy, Ensemble, append};
use rtdb_logkeeper::{StreamConfig, open_reader, open_writer, seal};
use rtdb_storage::log::{Log, LogOffset};
use rtdb_types::{AnomalyCategory, Timestamp};
use std::sync::Arc;
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
