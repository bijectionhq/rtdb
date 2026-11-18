use std::{
    cell::RefCell,
    collections::VecDeque,
    future::Future,
    pin::pin,
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
    task::{Context, Poll, Wake, Waker},
};

use super::*;

#[derive(Debug, PartialEq, Eq)]
enum Sample {
    Milliseconds(i64),
    Nanoseconds(i64),
    Wake,
}

struct Clock<'a>(&'a RefCell<VecDeque<Sample>>);

impl MetadataClock for Clock<'_> {
    fn milliseconds(&mut self) -> rtdb_types::Result<i64> {
        let Some(Sample::Milliseconds(value)) = self.0.borrow_mut().pop_front() else {
            panic!("expected a source millisecond sample");
        };
        Ok(value)
    }

    fn nanoseconds(&mut self) -> rtdb_types::Result<i64> {
        let Some(Sample::Nanoseconds(value)) = self.0.borrow_mut().pop_front() else {
            panic!("expected a source nanosecond sample");
        };
        Ok(value)
    }
}

fn wait_request(partition: Option<i32>, maximum_wait_ms: i64) -> TopicWaitRequest<'static> {
    TopicWaitRequest { topic: "events", partition, started_ms: 10, maximum_wait_ms }
}

#[tokio::test]
async fn cached_partitions_skip_wakeup_and_all_wait_clock_samples() {
    let mut producer = producer(MemoryBudget::new(1 << 20));
    producer.add_topic("events", 0, &mut NoIdentityWitness).unwrap();
    let topics = [topic(b"events", 1, &[(0, 1, 1, 0), (1, 1, 1, 0)])];
    assert_eq!(apply(&mut producer, 12, &topics, 1).unwrap(), ResponseAction::Published);
    let monitor = MetadataMonitor::new(producer).unwrap();
    let samples = RefCell::new(VecDeque::new());
    let output = monitor.wait_for_topic(
        wait_request(Some(1), 0), &mut Clock(&samples), &mut NoIdentityWitness, || panic!("cached metadata precedes wakeup"),
    ).await.unwrap();
    assert_eq!((output.elapsed_ms(), output.elapsed_ns()), (0, None));
    assert_eq!(output.snapshot().partitions("events").len(), 2);
}

#[tokio::test]
async fn cached_invalid_topic_precedes_the_wait_loop_and_fatal_slots() {
    let mut producer = producer(MemoryBudget::new(1 << 20));
    producer.add_topic("events", 0, &mut NoIdentityWitness).unwrap();
    let topics = [Topic { error: 17, ..topic(b"events", 1, &[]) }];
    assert_eq!(apply(&mut producer, 12, &topics, 1).unwrap(), ResponseAction::Published);
    let monitor = MetadataMonitor::new(producer).unwrap();
    monitor.publish_fatal(FatalKind::Bootstrap, 128, || Ok(Anomaly::incorrect("fixture/bootstrap", "Bootstrap failed"))).unwrap();
    let samples = RefCell::new(VecDeque::new());
    let result = monitor.wait_for_topic(
        wait_request(None, 10), &mut Clock(&samples), &mut NoIdentityWitness, || panic!("cached invalid topic precedes wakeup"),
    ).await;
    assert!(matches!(result, Err(TopicWaitFailure::Topic(MetadataTopicError::Recoverable(17)))));
}

#[tokio::test]
async fn initial_miss_wakes_the_sender_before_timer_creation_and_measures_a_successful_wait() {
    let mut producer = producer(MemoryBudget::new(1 << 20));
    producer.request_full(false).unwrap();
    let scope = producer.request(10).unwrap().scope();
    let monitor = MetadataMonitor::new(producer).unwrap();
    let samples = RefCell::new(VecDeque::from([
        Sample::Nanoseconds(100), Sample::Wake, Sample::Milliseconds(20),
        Sample::Milliseconds(22), Sample::Nanoseconds(140),
    ]));
    let topics = [topic(b"events", 1, &[(0, 1, 1, 0), (1, 1, 1, 0)])];
    let body = Wire::body(13, &[(1, b"broker", 9092)], &topics, 0);
    let seeds = JavaRandomSeeds::new();
    let mut slot = None;
    let mut shuffle = JavaShuffleRandom::new(&mut slot, &seeds, || Ok(0));
    let received = monitor.wait_for_topic(wait_request(Some(1), 100), &mut Clock(&samples), &mut NoIdentityWitness, || {
        assert_eq!(samples.borrow_mut().pop_front(), Some(Sample::Wake));
        assert_eq!(monitor.apply_response(
            RawMetadataView::parse(13, &body).unwrap(), scope, 20, true, &mut NoIdentityWitness, &mut shuffle,
        ).unwrap(), ResponseAction::Published);
        Ok(())
    }).await.unwrap();
    assert_eq!((received.elapsed_ms(), received.elapsed_ns()), (12, Some(40)));
    assert_eq!(received.snapshot().partitions("events").len(), 2);
    assert!(samples.borrow().is_empty());
}

#[tokio::test]
async fn timer_timeout_keeps_any_topic_error_and_the_original_wait_budget() {
    let mut producer = producer(MemoryBudget::new(1 << 20));
    producer.add_topic("events", 0, &mut NoIdentityWitness).unwrap();
    let topics = [Topic { error: 29, ..topic(b"events", 1, &[]) }];
    assert_eq!(apply(&mut producer, 12, &topics, 1).unwrap(), ResponseAction::Published);
    let monitor = MetadataMonitor::new(producer).unwrap();
    let samples = RefCell::new(VecDeque::from([
        Sample::Nanoseconds(100), Sample::Wake, Sample::Milliseconds(50), Sample::Milliseconds(50),
    ]));
    let result = monitor.wait_for_topic(wait_request(None, 0), &mut Clock(&samples), &mut NoIdentityWitness, || {
        assert_eq!(samples.borrow_mut().pop_front(), Some(Sample::Wake));
        Ok(())
    }).await;
    assert!(matches!(result, Err(TopicWaitFailure::TimedOut {
        known_partitions: None, maximum_wait_ms: 0, cause: Some(29),
    })));
    assert!(samples.borrow().is_empty());
}

#[tokio::test]
async fn elapsed_budget_after_update_keeps_only_a_source_retriable_cause() {
    for (code, expected_cause) in [(3, Some(3)), (29, None), (137, None)] {
        let mut producer = producer(MemoryBudget::new(1 << 20));
        producer.request_full(false).unwrap();
        let scope = producer.request(10).unwrap().scope();
        let monitor = MetadataMonitor::new(producer).unwrap();
        let samples = RefCell::new(VecDeque::from([
            Sample::Nanoseconds(100), Sample::Wake, Sample::Milliseconds(12), Sample::Milliseconds(15),
        ]));
        let topics = [Topic { error: code, ..topic(b"events", 1, &[]) }];
        let body = Wire::body(13, &[(1, b"broker", 9092)], &topics, 0);
        let seeds = JavaRandomSeeds::new();
        let mut slot = None;
        let mut shuffle = JavaShuffleRandom::new(&mut slot, &seeds, || Ok(0));
        let result = monitor.wait_for_topic(wait_request(None, 5), &mut Clock(&samples), &mut NoIdentityWitness, || {
            assert_eq!(samples.borrow_mut().pop_front(), Some(Sample::Wake));
            assert_eq!(monitor.apply_response(
                RawMetadataView::parse(13, &body).unwrap(), scope, 12, true, &mut NoIdentityWitness, &mut shuffle,
            ).unwrap(), ResponseAction::Published);
            Ok(())
        }).await;
        assert!(matches!(result, Err(TopicWaitFailure::TimedOut {
            known_partitions: None, maximum_wait_ms: 5, cause,
        }) if cause == expected_cause));
        assert!(samples.borrow().is_empty());
    }
}

#[tokio::test]
async fn retry_uses_remaining_time_and_reports_the_last_partition_count() {
    let mut producer = producer(MemoryBudget::new(1 << 20));
    producer.add_topic("events", 0, &mut NoIdentityWitness).unwrap();
    let initial = [topic(b"events", 1, &[(0, 1, 1, 0)])];
    assert_eq!(apply(&mut producer, 12, &initial, 1).unwrap(), ResponseAction::Published);
    producer.request_full(false).unwrap();
    let scope = producer.request(10).unwrap().scope();
    let monitor = MetadataMonitor::new(producer).unwrap();
    let samples = RefCell::new(VecDeque::from([
        Sample::Nanoseconds(100), Sample::Wake, Sample::Milliseconds(12), Sample::Milliseconds(13),
        Sample::Wake, Sample::Milliseconds(15), Sample::Milliseconds(22),
    ]));
    let topics = [topic(b"events", 1, &[(0, 1, 1, 0), (1, 1, 1, 0)])];
    let body = Wire::body(13, &[(1, b"broker", 9092)], &topics, 0);
    let seeds = JavaRandomSeeds::new();
    let mut slot = None;
    let mut shuffle = JavaShuffleRandom::new(&mut slot, &seeds, || Ok(0));
    let mut wakes = 0;
    let result = monitor.wait_for_topic(wait_request(Some(2), 10), &mut Clock(&samples), &mut NoIdentityWitness, || {
        assert_eq!(samples.borrow_mut().pop_front(), Some(Sample::Wake));
        wakes += 1;
        if wakes == 1 {
            assert_eq!(monitor.apply_response(
                RawMetadataView::parse(13, &body).unwrap(), scope, 12, true, &mut NoIdentityWitness, &mut shuffle,
            ).unwrap(), ResponseAction::Published);
        }
        Ok(())
    }).await;
    assert!(matches!(result, Err(TopicWaitFailure::TimedOut {
        known_partitions: Some(2), maximum_wait_ms: 10, cause: None,
    })));
    assert_eq!(wakes, 2);
    assert!(samples.borrow().is_empty());
}

#[tokio::test]
async fn topic_admission_failure_notifies_a_parked_metadata_waiter() {
    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) { self.wake_by_ref(); }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let memory = MemoryBudget::new(1 << 20);
    let monitor = MetadataMonitor::new(producer(memory.clone())).unwrap();
    let full = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
    let mut wait = MetadataWait::new(0, 60_000, || Ok(0)).unwrap();
    let mut parked = pin!(monitor.await_update(&mut wait, || Ok(0)));
    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let mut context = Context::from_waker(&waker);
    assert!(matches!(parked.as_mut().poll(&mut context), Poll::Pending));

    let samples = RefCell::new(VecDeque::new());
    let result = monitor.wait_for_topic(wait_request(None, 0), &mut Clock(&samples), &mut NoIdentityWitness, || {
        panic!("topic admission precedes wakeup");
    }).await;
    assert!(matches!(result, Err(TopicWaitFailure::Metadata(Failure::Native(_)))));
    assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
    assert!(matches!(parked.as_mut().poll(&mut context), Poll::Ready(Err(Failure::CapabilityFailed))));
    drop(full);
}
