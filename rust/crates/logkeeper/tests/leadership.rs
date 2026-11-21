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
async fn a_successor_skips_commit_requests_when_the_synced_quorum_has_committed_the_tip() {
    let ensemble = Ensemble::start(3).await;
    let config = ensemble.config("committed-tip");
    let previous = open_writer(config.clone(), 1, CancellationToken::new()).await.unwrap();
    assert_eq!(append(previous.as_ref(), "first", 1).await.offset, LogOffset(0));
    tokio::time::timeout(Duration::from_secs(5), async {
        for server in 0..ensemble.addresses.len() {
            let reader = open_reader(ensemble.config_of(&[server], "committed-tip"), CancellationToken::new()).await.unwrap();
            while reader.read_range(LogOffset(0), LogOffset(1)).await.unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
    }).await.expect("each replica has exposed the committed entry");
    drop(previous);

    let mut proxies = Vec::new();
    for address in &ensemble.addresses { proxies.push(CountingProxy::start(*address).await); }
    let proxied = StreamConfig { servers: proxies.iter().map(|proxy| proxy.address.to_string()).collect(), ..config };
    let successor = open_writer(proxied, 2, CancellationToken::new()).await.unwrap();
    assert_eq!(successor.latest().unwrap(), LogOffset(0));
    assert_eq!(proxies.iter().map(CountingProxy::commits).sum::<usize>(), 0);
    assert_eq!(append(successor.as_ref(), "second", 2).await.offset, LogOffset(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_replica_does_not_delay_a_durable_recovery_quorum() {
    let ensemble = Ensemble::start(3).await;
    let config = ensemble.config("slow-recovery");
    let previous = open_writer(config.clone(), 1, CancellationToken::new()).await.unwrap();
    append(previous.as_ref(), "first", 1).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let audit = rtdb_logkeeper::audit_retained(&config).await.unwrap();
            if audit.digests.len() == 3 { break; }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }).await.unwrap();

    let slow = CountingProxy::start(ensemble.addresses[2]).await;
    slow.hold_overwrites(true);
    let mut proxied = config.clone();
    proxied.servers[2] = slow.address.to_string();
    let recovering = tokio::spawn(open_writer(proxied, 2, CancellationToken::new()));
    tokio::time::timeout(Duration::from_secs(5), async {
        while slow.held_overwrites() == 0 { tokio::time::sleep(Duration::from_millis(2)).await; }
    }).await.unwrap();
    let successor = tokio::time::timeout(Duration::from_millis(500), recovering).await
        .expect("the held overwrite left a complete recovery quorum").unwrap().unwrap();
    assert_eq!(append(successor.as_ref(), "second", 2).await.offset, LogOffset(1));

    slow.hold_overwrites(false);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let audit = rtdb_logkeeper::audit_retained(&config).await.unwrap();
            if audit.interval == Some([0, 1]) && audit.digests.len() == 3 {
                assert!(audit.digests.values().all(|digest| digest == &audit.digests[&config.servers[0]]));
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_successor_publishes_a_recovered_tip_when_the_previous_commit_requests_were_lost() {
    let ensemble = Ensemble::start(3).await;
    let config = ensemble.config("unpublished-tip");
    let mut before = Vec::new();
    let mut after = Vec::new();
    for address in &ensemble.addresses {
        before.push(CountingProxy::start(*address).await);
        after.push(CountingProxy::start(*address).await);
    }
    let initial = StreamConfig { servers: before.iter().map(|proxy| proxy.address.to_string()).collect(), ..config.clone() };
    let previous = open_writer(initial, 1, CancellationToken::new()).await.unwrap();
    for proxy in &before { proxy.drop_commits(); }
    assert_eq!(append(previous.as_ref(), "first", 1).await.offset, LogOffset(0));
    let reader = open_reader(config.clone(), CancellationToken::new()).await.unwrap();
    assert_eq!(reader.latest().unwrap(), LogOffset(-1));

    let proxied = StreamConfig { servers: after.iter().map(|proxy| proxy.address.to_string()).collect(), ..config.clone() };
    after[2].drop_commits();
    let successor = tokio::time::timeout(Duration::from_millis(500), open_writer(proxied, 2, CancellationToken::new())).await
        .expect("two surviving commit acknowledgments complete recovery").unwrap();
    assert_eq!(successor.latest().unwrap(), LogOffset(0));
    assert!(after.iter().map(CountingProxy::commits).sum::<usize>() >= 2);
    let reader = open_reader(config, CancellationToken::new()).await.unwrap();
    let records = reader.read_range(LogOffset(0), LogOffset(1)).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload, Bytes::from_static(b"first"));
}

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
