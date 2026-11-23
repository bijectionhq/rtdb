mod common;

use bytes::Bytes;
use common::{CountingProxy, Ensemble, Server, append};
use rtdb_logkeeper::{CopyFrom, JournalEndpoint, Loglet, StreamConfig, open_reader, open_writer, open_writer_on, retire, seal};
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
    let proxied = StreamConfig { servers: proxies.iter().map(|proxy| proxy.address.to_string().into()).collect(), ..config };
    let successor = open_writer(proxied, 2, CancellationToken::new()).await.unwrap();
    assert_eq!(successor.latest().unwrap(), LogOffset(0));
    assert_eq!(proxies.iter().map(CountingProxy::commits).sum::<usize>(), 0);
    assert_eq!(append(successor.as_ref(), "second", 2).await.offset, LogOffset(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_replica_does_not_delay_a_durable_recovery_quorum() {
    let mut ensemble = Ensemble::start(3).await;
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
    proxied.servers[2] = slow.address.to_string().into();
    let recovering = tokio::spawn(open_writer(proxied.clone(), 2, CancellationToken::new()));
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
                assert!(audit.digests.values().all(|digest| digest == &audit.digests[config.servers[0].address()]));
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }).await.unwrap();
    ensemble.stop(0).await;
    let reader = open_reader(proxied, CancellationToken::new()).await.unwrap();
    assert_eq!(reader.read_index(2).await.unwrap(), LogOffset(1));
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
    let initial = StreamConfig { servers: before.iter().map(|proxy| proxy.address.to_string().into()).collect(), ..config.clone() };
    let previous = open_writer(initial.clone(), 1, CancellationToken::new()).await.unwrap();
    for proxy in &before { proxy.drop_commits(); }
    assert_eq!(append(previous.as_ref(), "first", 1).await.offset, LogOffset(0));
    let reader = open_reader(initial, CancellationToken::new()).await.unwrap();
    assert_eq!(reader.latest().unwrap(), LogOffset(-1));
    assert_eq!(reader.read_index(1).await.unwrap(), LogOffset(0));

    let proxied = StreamConfig { servers: after.iter().map(|proxy| proxy.address.to_string().into()).collect(), ..config.clone() };
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
async fn an_active_read_index_ignores_an_append_held_only_by_a_minority() {
    let ensemble = Ensemble::start(3).await;
    let stream = "minority-read-index";
    let config = ensemble.config(stream);
    let mut proxies = Vec::new();
    for address in &ensemble.addresses { proxies.push(CountingProxy::start(*address).await); }
    let proxied = StreamConfig { servers: proxies.iter().map(|proxy| proxy.address.to_string().into()).collect(), ..config };
    let writer = open_writer(proxied.clone(), 1, CancellationToken::new()).await.unwrap();
    let committed = append(writer.as_ref(), "committed", 1).await.offset;
    let reader = open_reader(proxied, CancellationToken::new()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        for slot in 0..3 {
            let replica = open_reader(ensemble.config_of(&[slot], stream), CancellationToken::new()).await.unwrap();
            while replica.read_range(committed, LogOffset(committed.0 + 1)).await.unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }).await.expect("each journal received the committed prefix");
    let accepted = proxies[0].accepted_appends();

    for proxy in &proxies[1..] { proxy.drop_appends(); }
    let in_flight = writer.enqueue(Bytes::from_static(b"partial"), Timestamp(2)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while proxies[0].accepted_appends() == accepted {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }).await.expect("the first journal durably accepted the partial append");

    assert_eq!(writer.latest().unwrap(), committed);
    assert_eq!(reader.read_index(1).await.unwrap(), committed);
    assert_eq!(reader.read_range(LogOffset(0), LogOffset(2)).await.unwrap()[0].payload, "committed");
    drop(in_flight);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_recovered_predecessor_cannot_confirm_a_successor_read_index() {
    let ensemble = Ensemble::start(4).await;
    let config = |servers: &[usize]| StreamConfig {
        write_quorum: 2, ..ensemble.config_of(servers, "moved-read-index")
    };
    let previous_config = config(&[0, 1, 2]);
    let previous = open_writer(previous_config.clone(), 1, CancellationToken::new()).await.unwrap();
    let previous_reader = open_reader(previous_config.clone(), CancellationToken::new()).await.unwrap();
    assert_eq!(previous_reader.read_index(1).await.unwrap(), LogOffset(-1));
    let first = append(previous.as_ref(), "first", 1).await.offset;

    let current_config = config(&[1, 2, 3]);
    let chain = vec![Loglet { servers: previous_config.servers.clone(), start_lsn: 0 }];
    let moved = open_writer_on(chain, current_config, 2, CopyFrom::Retained, CancellationToken::new()).await.unwrap();
    assert!(previous_reader.read_index(1).await.is_err());
    assert!(previous_reader.read_index(2).await.is_err());

    let current = open_reader(config(&[3, 1, 2]), CancellationToken::new()).await.unwrap();
    assert_eq!(current.read_index(2).await.unwrap(), first);
    let second = append(moved.as_ref(), "second", 2).await.offset;
    assert_eq!(current.read_index(2).await.unwrap(), second);
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
async fn a_writer_cannot_confirm_with_replacement_journals_that_lack_its_epoch() {
    let mut ensemble = Ensemble::start(3).await;
    let config = ensemble.config("journal-votes");
    let log = open_writer(config, 1, CancellationToken::new()).await.unwrap();
    append(log.as_ref(), "committed", 1).await;
    log.confirm_leadership().await.unwrap();

    for replica in [0, 1] {
        ensemble.stop(replica).await;
        ensemble.directories[replica] = tempfile::tempdir().unwrap();
        ensemble.restart(replica).await;
    }

    let error = tokio::time::timeout(Duration::from_secs(5), log.confirm_leadership())
        .await.unwrap().unwrap_err();
    assert_eq!(error.code.as_str(), "rtdb.storage/remote");
    assert!(error.message.as_deref().is_some_and(|message| message.contains("failed to confirm leadership")), "{error:?}");
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
    let proxied = StreamConfig { servers: proxies.iter().map(|proxy| proxy.address.to_string().into()).collect(), ..config.clone() };
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
    let config = StreamConfig { servers: proxies.iter().map(|proxy| proxy.address.to_string().into()).collect(), stream: "batched".into(), write_quorum: 2, maximum_message_bytes: 1024 };
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_journal_cannot_fill_two_quorum_slots_through_endpoint_aliases() {
    let ensemble = Ensemble::start(1).await;
    let first = CountingProxy::start(ensemble.addresses[0]).await;
    let second = CountingProxy::start(ensemble.addresses[0]).await;
    let config = StreamConfig {
        servers: vec![first.address.to_string().into(), second.address.to_string().into()],
        ..ensemble.config("distinct-journal-quorum")
    };
    let writer = tokio::time::timeout(
        Duration::from_secs(5),
        open_writer(config, 1, CancellationToken::new()),
    ).await.expect("opening a loglet with aliased journals must finish");
    assert!(writer.is_err(), "one journal provided two recovery votes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replacement_with_the_same_history_has_no_authority_to_confirm_or_retire() {
    let mut ensemble = Ensemble::start(1).await;
    let discovered = ensemble.config_of(&[0], "rebound");
    open_reader(discovered.clone(), CancellationToken::new()).await.unwrap();
    let journal = std::fs::read_to_string(ensemble.directories[0].path().join("JOURNAL_ID")).unwrap();
    let address = ensemble.addresses[0];
    let bound = StreamConfig { servers: vec![JournalEndpoint::Member { address: address.to_string(), journal: journal.clone() }], ..discovered.clone() };
    let writer = open_writer(bound.clone(), 1, CancellationToken::new()).await.unwrap();
    let offset = append(writer.as_ref(), "original", 1).await.offset;
    let reader = open_reader(bound.clone(), CancellationToken::new()).await.unwrap();
    assert_eq!(reader.read_index(1).await.unwrap(), offset);
    drop(writer);
    ensemble.stop(0).await;

    let substitute = tempfile::tempdir().unwrap();
    let original_segments = ensemble.directories[0].path().join("segments");
    let substitute_segments = substitute.path().join("segments");
    std::fs::create_dir(&substitute_segments).unwrap();
    for file in std::fs::read_dir(original_segments).unwrap() {
        let file = file.unwrap();
        std::fs::copy(file.path(), substitute_segments.join(file.file_name())).unwrap();
    }
    let replacement = Server::start_at(substitute.path(), address).await;
    let replacement_reader = open_reader(discovered, CancellationToken::new()).await.unwrap();
    assert_eq!(replacement_reader.latest().unwrap(), offset);
    assert!(reader.read_index(1).await.is_err());
    assert!(open_reader(bound.clone(), CancellationToken::new()).await.is_err());
    assert!(retire(&bound, 2).await.is_err());
    assert_eq!(replacement_reader.read_range(LogOffset(0), LogOffset(1)).await.unwrap()[0].payload, "original");
    replacement.stop().await;

    ensemble.restart(0).await;
    let route = CountingProxy::start(address).await;
    let rerouted = StreamConfig { servers: vec![JournalEndpoint::Member { address: route.address.to_string(), journal }], ..bound };
    let reader = open_reader(rerouted, CancellationToken::new()).await.unwrap();
    assert_eq!(reader.read_index(1).await.unwrap(), offset);
    ensemble.stop(0).await;
}
