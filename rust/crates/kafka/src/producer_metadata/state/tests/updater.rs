use super::*;
use crate::node_selection::ConnectionObservations;

#[derive(Debug, PartialEq, Eq)]
enum Demand {
    Offset,
    Ready,
    Channel,
    Capacity,
    Count,
    Preparing,
    Connectable,
    Connecting,
    Connect,
    Send(RequestScope),
    Fatal,
}

type SendFixture<'a> = &'a mut dyn FnMut(MetadataRequest, i64);

#[derive(Default)]
struct Network<'a> {
    demands: Vec<Demand>,
    draws: usize,
    ready_node: Option<i32>,
    capacity: bool,
    in_flight: i32,
    preparing: bool,
    connecting: bool,
    connectable: bool,
    unsupported_version: bool,
    reject_fatal: bool,
    publish_fatal: bool,
    send: Option<SendFixture<'a>>,
}

impl ConnectionObservations<Node> for Network<'_> {
    fn next_offset(&mut self, bound: i32) -> rtdb_types::Result<i32> {
        assert_eq!(bound, 1);
        self.demands.push(Demand::Offset);
        self.draws += 1;
        Ok(0)
    }

    fn connection_ready(&mut self, node: &Node, _: i64) -> rtdb_types::Result<bool> {
        self.demands.push(Demand::Ready);
        Ok(self.ready_node == Some(node.id))
    }

    fn channel_ready(&mut self, _: &Node) -> rtdb_types::Result<bool> {
        self.demands.push(Demand::Channel);
        Ok(true)
    }

    fn can_send_more(&mut self, _: &Node) -> rtdb_types::Result<bool> {
        self.demands.push(Demand::Capacity);
        Ok(self.capacity)
    }

    fn in_flight_count(&mut self, _: &Node) -> rtdb_types::Result<i32> {
        self.demands.push(Demand::Count);
        Ok(self.in_flight)
    }

    fn preparing_connection(&mut self, _: &Node) -> rtdb_types::Result<bool> {
        self.demands.push(Demand::Preparing);
        Ok(self.preparing)
    }

    fn can_connect(&mut self, _: &Node, _: i64) -> rtdb_types::Result<bool> {
        self.demands.push(Demand::Connectable);
        Ok(self.connectable)
    }

    fn last_connect_attempt_ms(&mut self, _: &Node) -> rtdb_types::Result<i64> {
        panic!("single-item selection has no connectable tie")
    }
}

impl MetadataNetwork for Network<'_> {
    fn is_connecting(&mut self, _: &Node) -> rtdb_types::Result<bool> {
        self.demands.push(Demand::Connecting);
        Ok(self.connecting)
    }

    fn initiate_connect(&mut self, _: &Node, _: i64) -> rtdb_types::Result<()> {
        self.demands.push(Demand::Connect);
        Ok(())
    }

    fn send_metadata(&mut self, _: &Node, request: MetadataRequest, now_ms: i64) -> rtdb_types::Result<SendMetadata> {
        self.demands.push(Demand::Send(request.scope()));
        if self.unsupported_version {
            Ok(SendMetadata::UnsupportedVersion(Anomaly::unsupported("fixture/metadata-version", "No usable Metadata version")))
        } else {
            if let Some(send) = &mut self.send { send(request, now_ms); }
            Ok(SendMetadata::Accepted)
        }
    }

    fn fatal_error(&mut self, cause: Anomaly, target: RequestFatal<'_>) -> std::result::Result<(), UpdateFailure> {
        self.demands.push(Demand::Fatal);
        if self.reject_fatal {
            Err(Anomaly::unsupported("fixture/metadata-fatal", "Fatal publication unavailable").into())
        } else if self.publish_fatal {
            target.publish(128, || Ok(cause)).map_err(UpdateFailure::Metadata)
        } else {
            Ok(())
        }
    }
}

#[derive(Default)]
struct Math {
    powers: usize,
}

impl JavaBackoffObservations for Math {
    fn log(&mut self, value: f64) -> f64 {
        assert_eq!(value, 2.0);
        std::f64::consts::LN_2
    }

    fn pow(&mut self, base: f64, exponent: f64) -> f64 {
        assert_eq!(base, 2.0);
        assert_eq!(exponent, 0.0);
        self.powers += 1;
        1.0
    }

    fn next_double(&mut self, origin: f64, bound: f64) -> f64 {
        assert_eq!((origin, bound), (0.8, 1.2));
        1.0
    }
}

#[derive(Default)]
struct Shuffle {
    acquisitions: usize,
}

impl JavaShuffleObservations for Shuffle {
    fn begin(&mut self, _: &mut ShuffleAdmission<'_>) -> rtdb_types::Result<()> {
        self.acquisitions += 1;
        Ok(())
    }

    fn next_int(&mut self, _: i32, _: &mut ShuffleAdmission<'_>) -> rtdb_types::Result<i32> {
        panic!("empty and singleton lists only acquire the generator")
    }
}

fn producer_with_cluster(math: &mut Math, shuffle: &mut Shuffle, bootstrap: bool) -> ProducerMetadata {
    let mut producer = ProducerMetadata::new_with_cluster_nodes(
        Settings { refresh_backoff_ms: 10, refresh_backoff_max_ms: 20, metadata_expire_ms: 1000, metadata_idle_ms: 100 },
        MemoryBudget::new(1 << 20), math, &mut NoIdentityWitness, shuffle,
    ).unwrap();
    if bootstrap {
        let addresses = producer.prepare_bootstrap_addresses(&[ResolvedEndpoint { host: "boot.example", port: 9092 }]).unwrap();
        producer.bootstrap(addresses, shuffle, &mut NoIdentityWitness).unwrap();
    } else {
        producer.request_full(false).unwrap();
    }
    producer
}

fn updater() -> MetadataUpdater {
    MetadataUpdater::new(UpdateSettings {
        request_timeout_ms: 300, reconnect_backoff_ms: 50,
        bootstrap_enabled: true, rebootstrap_enabled: true,
    })
}

fn tick(
    updater: &mut MetadataUpdater, producer: &mut ProducerMetadata, now_ms: i64, math: &mut Math,
    shuffle: &mut Shuffle, network: &mut Network<'_>,
) -> i64 {
    updater.maybe_update(producer, now_ms, math, &mut NoIdentityWitness, shuffle, network).unwrap()
}

#[test]
fn observed_response_retires_the_request_and_publishes_the_matching_scope_once() {
    use std::time::{Duration, Instant};
    use crate::refresh::{ConnectionBackoffs, ConnectionSettings};
    use rdkafka::{mocking::MockCluster, native_byte_account::NativeByteAccount, producer::Producer};
    use super::observed_exchange::{open_native, ready_observation};

    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("original", 2, 1).unwrap();
    let endpoint = cluster.bootstrap_servers();
    let (host, port) = endpoint.rsplit_once(':').unwrap();
    let account = NativeByteAccount::new(4 << 20, ()).unwrap();
    let native = open_native(&endpoint, account.clone());
    assert_eq!(native.client().fetch_metadata(None, Duration::from_secs(5)).unwrap().brokers().len(), 1);

    let mut math = Math::default();
    let mut shuffle = Shuffle::default();
    let mut producer = producer_with_cluster(&mut math, &mut shuffle, true);
    producer.add_topic("original", 90, &mut NoIdentityWitness).unwrap();
    let request_timeout_ms = 5000;
    let mut updater = MetadataUpdater::new(UpdateSettings {
        request_timeout_ms, reconnect_backoff_ms: 50, bootstrap_enabled: true, rebootstrap_enabled: true,
    });
    let expected_scope = producer.request(100).unwrap().scope();
    let monitor = MetadataMonitor::new(producer).unwrap();
    let queue_budget = MemoryBudget::new(1 << 16);
    let mut flights = InFlightQueue::new(1, queue_budget.clone()).unwrap();
    let port = port.parse().unwrap();
    {
        let mut send = |request: MetadataRequest, sent_ms| {
            let observation = ready_observation(&native, host, port);
            request.submit_in_flight(&mut flights, observation, 65536, sent_ms, request_timeout_ms).unwrap();
        };
        let mut network = Network { ready_node: Some(-1), capacity: true, send: Some(&mut send), ..Network::default() };
        assert_eq!(monitor.drive_update(&mut updater, 100, &mut math, &mut NoIdentityWitness, &mut shuffle, &mut network).unwrap(), i64::from(request_timeout_ms));
    }
    assert_eq!(updater.in_progress(), Some(expected_scope));
    let connection_backoffs = ConnectionBackoffs::new(ConnectionSettings {
        reconnect_ms: 10, reconnect_max_ms: 10, setup_ms: 20, setup_max_ms: 20,
    }, &mut math);
    let mut connection = connection_backoffs.connecting::<()>(99, &mut math);
    connection.ready(&connection_backoffs, &mut math);

    assert!(flights.complete_send().unwrap().is_none());
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut poll = |flights: &mut InFlightQueue<MetadataExchange>| {
        flights.poll_metadata(&mut connection, &monitor, &mut updater, 102, &mut NoIdentityWitness, &mut shuffle)
    };
    loop {
        if let Some(result) = poll(&mut flights).unwrap() {
            assert!(matches!(result, ResponseAction::Published));
            break;
        }
        assert_eq!(flights.count(), 1);
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(flights.count(), 0);
    assert!(monitor.snapshot().unwrap().partition("original", 1).is_some());
    assert!(poll(&mut flights).unwrap().is_none());
    assert!(updater.in_progress().is_none());
    drop(flights);
    assert_eq!(queue_budget.used_bytes(), 0);
    drop(native);
    assert_eq!(account.used(), 0);
}

#[tokio::test]
async fn monitored_sender_delivers_unsupported_version_and_disconnect_fatal_errors() {
    use std::{future::Future, pin::pin, task::Poll, time::Duration};

    for disconnect in [false, true] {
        let mut math = Math::default();
        let mut shuffle = Shuffle::default();
        let producer = producer_with_cluster(&mut math, &mut shuffle, true);
        let version = producer.refresh().update_version();
        let monitor = MetadataMonitor::new(producer).unwrap();
        let mut wait = MetadataWait::new(version, 60_000, || Ok(100)).unwrap();
        let mut waiter = pin!(monitor.await_update(&mut wait, || Ok(100)));
        std::future::poll_fn(|cx| {
            assert!(waiter.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        }).await;

        let mut updater = updater();
        let mut network = Network { ready_node: Some(-1), capacity: true, unsupported_version: true, publish_fatal: true, ..Network::default() };
        let expected = if disconnect {
            let cause = Anomaly::unsupported("fixture/authentication", "Authentication failed");
            monitor.drive_disconnect(&mut updater, 100, Some(cause), &mut math, &mut network).unwrap();
            assert!(updater.in_progress().is_none());
            "fixture/authentication"
        } else {
            monitor.drive_update(&mut updater, 100, &mut math, &mut NoIdentityWitness, &mut shuffle, &mut network).unwrap();
            assert!(updater.in_progress().is_some());
            "fixture/metadata-version"
        };
        let observed = tokio::time::timeout(Duration::from_secs(1), waiter).await.unwrap().unwrap();
        assert!(matches!(observed, WaitCompletion::Fatal(error) if error.cause().code == expected));
    }
}

#[tokio::test]
async fn monitored_disconnect_notifies_when_fatal_admission_ends_the_capability() {
    use std::{future::Future, pin::pin, task::Poll, time::Duration};

    let mut math = Math::default();
    let mut shuffle = Shuffle::default();
    let producer = producer_with_cluster(&mut math, &mut shuffle, false);
    let memory = producer.budget.0.clone();
    let version = producer.refresh().update_version();
    let monitor = MetadataMonitor::new(producer).unwrap();
    let full = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
    let mut wait = MetadataWait::new(version, 60_000, || Ok(100)).unwrap();
    let mut waiter = pin!(monitor.await_update(&mut wait, || Ok(100)));
    std::future::poll_fn(|cx| {
        assert!(waiter.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    }).await;

    let mut updater = updater();
    let mut network = Network { publish_fatal: true, ..Network::default() };
    let cause = Anomaly::unsupported("fixture/authentication", "Authentication failed");
    let result = monitor.drive_disconnect(&mut updater, 100, Some(cause), &mut math, &mut network);
    assert!(matches!(result, Err(UpdateFailure::Metadata(Failure::Native(_)))));
    let observed = tokio::time::timeout(Duration::from_secs(1), waiter).await.unwrap();
    assert!(matches!(observed, Err(Failure::CapabilityFailed)));
    drop(full);
}

#[test]
fn maybe_update_rechecks_send_capacity_and_observes_backoff_while_in_progress() {
    let mut math = Math::default();
    let mut shuffle = Shuffle::default();
    let mut producer = producer_with_cluster(&mut math, &mut shuffle, true);
    let mut network = Network { ready_node: Some(-1), capacity: true, ..Network::default() };
    let mut updater = updater();
    let scope = producer.request(100).unwrap().scope();
    assert_eq!(tick(&mut updater, &mut producer, 100, &mut math, &mut shuffle, &mut network), 300);
    assert_eq!(updater.in_progress(), Some(scope));
    assert_eq!(network.demands, [
        Demand::Offset, Demand::Ready, Demand::Channel, Demand::Ready, Demand::Channel,
        Demand::Capacity, Demand::Count, Demand::Ready, Demand::Channel, Demand::Capacity,
        Demand::Send(scope),
    ]);
    assert_eq!(math.powers, 1);
    assert!(!updater.is_update_due(&mut producer, 101, &mut math).unwrap());
    assert_eq!(math.powers, 1);
    assert_eq!(tick(&mut updater, &mut producer, 101, &mut math, &mut shuffle, &mut network), 300);
    assert_eq!(math.powers, 2);
    assert_eq!(network.draws, 1);
}

#[test]
fn rebootstrap_resets_attempt_time_and_selects_again_from_the_new_cluster() {
    let mut math = Math::default();
    let mut shuffle = Shuffle::default();
    let mut producer = producer_with_cluster(&mut math, &mut shuffle, true);
    let response = Wire::body(12, &[(6, b"learned.example", 9092)], &[], 0);
    let scope = producer.request(20).unwrap().scope();
    assert_eq!(producer.apply_with_cluster_nodes(
        RawMetadataView::parse(12, &response).unwrap(), scope, 20, true, &mut NoIdentityWitness, &mut shuffle,
    ).unwrap(), ResponseAction::Published);
    producer.request_full(true).unwrap();
    let before = producer.snapshot();
    let mut network = Network { ready_node: Some(-1), capacity: true, ..Network::default() };
    let mut updater = updater();
    assert_eq!(tick(&mut updater, &mut producer, 100, &mut math, &mut shuffle, &mut network), 300);
    assert_eq!(shuffle.acquisitions, 4);
    assert_eq!(network.draws, 2);
    assert_eq!(&network.demands[..5], &[
        Demand::Offset, Demand::Ready, Demand::Ready, Demand::Preparing, Demand::Connectable,
    ]);
    assert!(matches!(network.demands.last(), Some(Demand::Send(_))));
    assert!(!producer.snapshot().0.same_object(&before.0));
    assert_eq!(producer.refresh().update_version(), 3);
    assert!(!updater.needs_rebootstrap(120, 20));
    assert!(updater.needs_rebootstrap(121, 20));
}

#[test]
fn readiness_without_capacity_or_a_candidate_preserves_the_generation() {
    let mut math = Math::default();
    let mut shuffle = Shuffle::default();
    let mut producer = producer_with_cluster(&mut math, &mut shuffle, true);
    let mut network = Network { ready_node: Some(-1), ..Network::default() };
    let mut updater = updater();
    let before = producer.snapshot();
    assert_eq!(tick(&mut updater, &mut producer, 100, &mut math, &mut shuffle, &mut network), 50);
    assert!(producer.snapshot().0.same_object(&before.0));
    assert_eq!(network.draws, 1);
    assert_eq!(shuffle.acquisitions, 2);
    assert_eq!(&network.demands[network.demands.len() - 2..], &[Demand::Preparing, Demand::Connectable]);
    assert!(updater.in_progress().is_none());
}

#[test]
fn connecting_peer_short_circuits_a_new_connection_attempt() {
    for connecting in [false, true] {
        let mut math = Math::default();
        let mut shuffle = Shuffle::default();
        let mut producer = producer_with_cluster(&mut math, &mut shuffle, true);
        let mut network = Network { connecting, connectable: !connecting, preparing: connecting, ..Network::default() };
        let mut updater = updater();
        assert_eq!(tick(&mut updater, &mut producer, 100, &mut math, &mut shuffle, &mut network), 50);
        if connecting {
            assert_eq!(network.demands.last(), Some(&Demand::Connecting));
            assert!(!network.demands.contains(&Demand::Connectable));
        } else {
            assert_eq!(&network.demands[network.demands.len() - 3..], &[Demand::Connecting, Demand::Connectable, Demand::Connect]);
        }
        assert!(updater.in_progress().is_none());
    }
}

#[test]
fn preparing_without_connecting_or_reconnect_capacity_waits_for_an_event() {
    let mut math = Math::default();
    let mut shuffle = Shuffle::default();
    let mut producer = producer_with_cluster(&mut math, &mut shuffle, true);
    let mut network = Network { preparing: true, ..Network::default() };
    let mut updater = updater();
    assert_eq!(tick(&mut updater, &mut producer, 100, &mut math, &mut shuffle, &mut network), i64::MAX);
    assert_eq!(network.demands, [
        Demand::Offset, Demand::Ready, Demand::Ready, Demand::Preparing,
        Demand::Ready, Demand::Connecting, Demand::Connectable,
    ]);
}

#[test]
fn unsupported_version_handles_failure_before_publishing_the_in_progress_scope() {
    let mut math = Math::default();
    let mut shuffle = Shuffle::default();
    let mut producer = producer_with_cluster(&mut math, &mut shuffle, true);
    let mut network = Network {
        ready_node: Some(-1), capacity: true, unsupported_version: true, ..Network::default()
    };
    let mut updater = updater();
    let scope = producer.request(100).unwrap().scope();
    assert_eq!(tick(&mut updater, &mut producer, 100, &mut math, &mut shuffle, &mut network), 300);
    assert_eq!(&network.demands[network.demands.len() - 2..], &[Demand::Send(scope), Demand::Fatal]);
    assert_eq!(producer.time_to_next_update(100, &mut math).unwrap(), 10);
    assert_eq!(updater.in_progress(), Some(scope));
}

#[test]
fn disconnect_and_failed_request_keep_their_distinct_fatal_prefixes() {
    for disconnect in [false, true] {
        let mut math = Math::default();
        let mut shuffle = Shuffle::default();
        let mut producer = producer_with_cluster(&mut math, &mut shuffle, false);
        let mut network = Network { reject_fatal: true, ..Network::default() };
        let mut updater = updater();
        let fatal = Some(Anomaly::unsupported("fixture/fatal", "Source fatal error"));
        let result = if disconnect {
            updater.disconnect(&mut producer, 100, fatal, &mut math, &mut network)
        } else {
            updater.failed_request(&mut producer, 100, fatal, &mut network)
        };
        assert!(matches!(result, Err(UpdateFailure::Network(error)) if error.code == "fixture/metadata-fatal"));
        assert_eq!(network.demands, [Demand::Fatal]);
        assert_eq!(producer.time_to_next_update(100, &mut math).unwrap(), if disconnect { 10 } else { 0 });
    }
}

#[test]
fn response_completion_clears_scope_and_preserves_the_source_attempt_timer() {
    for expected in [ResponseAction::RetryEmptyBrokers, ResponseAction::Rebootstrap, ResponseAction::Published] {
        let mut math = Math::default();
        let mut shuffle = Shuffle::default();
        let mut producer = producer_with_cluster(&mut math, &mut shuffle, true);
        let mut network = Network { ready_node: Some(-1), capacity: true, ..Network::default() };
        let mut updater = updater();
        assert_eq!(tick(&mut updater, &mut producer, 100, &mut math, &mut shuffle, &mut network), 300);
        let scope = updater.in_progress().unwrap();
        let bytes = match expected {
            ResponseAction::RetryEmptyBrokers => Wire::body(13, &[], &[], 0),
            ResponseAction::Rebootstrap => Wire::body(13, &[], &[], 129),
            ResponseAction::Published => Wire::body(13, &[(1, b"host", 9092)], &[], 0),
        };
        let action = producer.apply_with_cluster_nodes(
            RawMetadataView::parse(13, &bytes).unwrap(), scope, 105, true, &mut NoIdentityWitness, &mut shuffle,
        ).unwrap();
        assert_eq!(action, expected);
        updater.response_completed(action);
        assert!(updater.in_progress().is_none());
        assert_eq!(updater.needs_rebootstrap(105, 10), action == ResponseAction::Rebootstrap);
        assert_eq!(updater.needs_rebootstrap(111, 10), action != ResponseAction::Published);
    }
}
