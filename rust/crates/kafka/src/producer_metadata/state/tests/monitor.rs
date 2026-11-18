use std::{
    cell::Cell,
    future::Future,
    pin::{Pin, pin},
    task::{Context, Poll, Waker},
    time::Duration,
};

use super::*;

fn clock() -> rtdb_types::Result<i64> { Ok(0) }

fn pending<T>(future: Pin<&mut impl Future<Output = T>>) {
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(future.poll(&mut context), Poll::Pending));
}

#[tokio::test]
async fn close_wakes_every_pending_waiter_and_releases_the_monitor() {
    let memory = MemoryBudget::new(1 << 20);
    let monitor = MetadataMonitor::new(producer(memory.clone())).unwrap();
    let closer = monitor.clone();
    {
        let mut first_wait = MetadataWait::new(0, 60_000, || Ok(0)).unwrap();
        let mut second_wait = MetadataWait::new(0, 60_000, || Ok(0)).unwrap();
        let mut first = pin!(monitor.await_update(&mut first_wait, clock));
        let mut second = pin!(monitor.await_update(&mut second_wait, clock));
        pending(first.as_mut());
        pending(second.as_mut());
        closer.close().unwrap();
        for waiter in [first, second] {
            let result = tokio::time::timeout(Duration::from_secs(1), waiter).await.unwrap().unwrap();
            assert!(matches!(result, WaitCompletion::Closed));
        }
    }
    drop(closer);
    drop(monitor);
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn ordinary_fatal_wakes_waiters_once_and_bootstrap_fatal_remains_available() {
    let memory = MemoryBudget::new(1 << 20);
    let monitor = MetadataMonitor::new(producer(memory.clone())).unwrap();
    {
        let mut first_wait = MetadataWait::new(0, 60_000, || Ok(0)).unwrap();
        let mut second_wait = MetadataWait::new(0, 60_000, || Ok(0)).unwrap();
        let mut first = pin!(monitor.await_update(&mut first_wait, || Ok(0)));
        let mut second = pin!(monitor.await_update(&mut second_wait, || Ok(0)));
        pending(first.as_mut());
        pending(second.as_mut());
        monitor.publish_fatal(FatalKind::Request, 128, || Ok(Anomaly::incorrect("fixture/request", "Request failed"))).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), first).await.unwrap().unwrap();
        assert!(matches!(result, WaitCompletion::Fatal(error) if error.cause().code == "fixture/request"));
        pending(second.as_mut());
        monitor.publish_fatal(FatalKind::Bootstrap, 128, || Ok(Anomaly::incorrect("fixture/bootstrap", "Bootstrap failed"))).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), second).await.unwrap().unwrap();
        let WaitCompletion::Fatal(first_bootstrap) = result else { panic!("expected bootstrap fatal"); };
        let mut next_wait = MetadataWait::new(0, 0, || Ok(0)).unwrap();
        let result = monitor.await_update(&mut next_wait, || panic!("retained bootstrap failure precedes clock")).await.unwrap();
        let WaitCompletion::Fatal(next_bootstrap) = result else { panic!("expected retained bootstrap fatal"); };
        assert!(std::ptr::eq(first_bootstrap.cause(), next_bootstrap.cause()));
    }
    drop(monitor);
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn empty_response_leaves_the_wait_parked_and_publication_wakes_it() {
    let memory = MemoryBudget::new(1 << 20);
    let mut producer = producer(memory.clone());
    let scope = producer.request(0).unwrap().scope();
    let monitor = MetadataMonitor::new(producer).unwrap();
    {
        let samples = Cell::new(0);
        let mut wait = MetadataWait::new(0, 60_000, clock).unwrap();
        let mut parked = pin!(monitor.await_update(&mut wait, || {
            samples.set(samples.get() + 1);
            clock()
        }));
        pending(parked.as_mut());
        let empty = Wire::body(13, &[], &[], 0);
        let seeds = JavaRandomSeeds::new();
        let mut slot = None;
        let mut shuffle = JavaShuffleRandom::new(&mut slot, &seeds, clock);
        assert_eq!(monitor.apply_response(
            RawMetadataView::parse(13, &empty).unwrap(), scope, 1, true, &mut NoIdentityWitness, &mut shuffle,
        ).unwrap(), ResponseAction::RetryEmptyBrokers);
        pending(parked.as_mut());
        assert_eq!(samples.get(), 1);
        let body = Wire::body(13, &[(1, b"broker", 9092)], &[], 0);
        assert_eq!(monitor.apply_response(
            RawMetadataView::parse(13, &body).unwrap(), scope, 2, true, &mut NoIdentityWitness, &mut shuffle,
        ).unwrap(), ResponseAction::Published);
        let result = tokio::time::timeout(Duration::from_secs(1), parked).await.unwrap().unwrap();
        assert!(matches!(result, WaitCompletion::Updated));
        assert_eq!(samples.get(), 1);
        assert_eq!(monitor.snapshot().unwrap().cluster_nodes().unwrap().len(), 1);
    }
    drop(monitor);
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn terminal_admission_failure_wakes_a_parked_waiter_without_building_the_error() {
    let memory = MemoryBudget::new(1 << 20);
    let monitor = MetadataMonitor::new(producer(memory.clone())).unwrap();
    let full = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
    {
        let mut wait = MetadataWait::new(0, 60_000, clock).unwrap();
        let mut parked = pin!(monitor.await_update(&mut wait, clock));
        pending(parked.as_mut());
        let result = monitor.publish_fatal(FatalKind::Request, 128, || panic!("payload must be admitted first"));
        assert!(matches!(result, Err(Failure::Native(_))));
        let result = tokio::time::timeout(Duration::from_secs(1), parked).await.unwrap();
        assert!(matches!(result, Err(Failure::CapabilityFailed)));
    }
    drop(full);
    drop(monitor);
    assert_eq!(memory.used_bytes(), 0);
}
