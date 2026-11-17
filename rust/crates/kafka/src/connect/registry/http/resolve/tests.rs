use super::*;
use std::{sync::{atomic::{AtomicBool, Ordering}, mpsc}, time::Duration};
use tokio::{sync::oneshot, time::timeout};

async fn released(budget: &MemoryBudget) {
    timeout(Duration::from_secs(5), async {
        while budget.used_bytes() != 0 { tokio::task::yield_now().await; }
    }).await.unwrap();
}

#[tokio::test]
async fn numeric_addresses_need_no_resolver_job_or_memory_admission() {
    let budget = MemoryBudget::new(0);
    for host in ["127.0.0.1", "::1", "2001:db8::7"] {
        let address = resolve_with(host, 8081, &budget, |_, _| panic!("numeric address dispatched a lookup")).await.unwrap();
        assert_eq!(address, SocketAddr::new(host.parse().unwrap(), 8081));
    }
    assert_eq!(budget.used_bytes(), 0);
}

#[tokio::test]
async fn resolver_admission_failure_precedes_dispatch() {
    let budget = MemoryBudget::new(LOOKUP_ALLOCATION_BOUND - 1);
    let called = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&called);
    let result = resolve_with("registry.invalid", 8081, &budget, move |_, _| {
        observed.store(true, Ordering::Release);
        Ok("127.0.0.1:8081".parse().unwrap())
    }).await;
    assert!(result.is_err());
    assert!(!called.load(Ordering::Acquire));
    assert_eq!(budget.used_bytes(), 0);
}

#[tokio::test]
async fn completed_and_failed_lookups_release_their_admission() {
    let budget = MemoryBudget::new(LOOKUP_ALLOCATION_BOUND * 2);
    for succeeds in [false, true] {
        let result = resolve_with("registry.invalid", 8081, &budget, move |host, port| {
            assert_eq!(host, "registry.invalid");
            assert_eq!(port, 8081);
            if succeeds { Ok("127.0.0.1:8081".parse().unwrap()) }
            else { Err(io::Error::new(io::ErrorKind::NotFound, "fixture lookup failed")) }
        }).await;
        assert_eq!(result.is_ok(), succeeds);
        assert_eq!(budget.used_bytes(), 0);
    }
}

#[tokio::test]
async fn cancelling_a_running_lookup_retains_its_charge_until_the_resolver_finishes() {
    let budget = MemoryBudget::new(LOOKUP_ALLOCATION_BOUND * 2);
    let (started, ready) = oneshot::channel();
    let (release, wait) = mpsc::channel();
    let mut lookup = Box::pin(resolve_with("registry.invalid", 8081, &budget, move |_, _| {
        started.send(()).unwrap();
        wait.recv_timeout(Duration::from_secs(5)).unwrap();
        Ok("127.0.0.1:8081".parse().unwrap())
    }));
    tokio::select! {
        result = &mut lookup => panic!("lookup finished before release: {}", result.is_ok()),
        ready = timeout(Duration::from_secs(5), ready) => ready.unwrap().unwrap(),
    }
    let retained = budget.used_bytes();
    assert!(retained >= LOOKUP_ALLOCATION_BOUND);
    drop(lookup);
    assert_eq!(budget.used_bytes(), retained);
    let rejected = resolve_with("retry.invalid", 8081, &budget, |_, _| panic!("cancelled running job lost its admission")).await.unwrap_err();
    assert_eq!(rejected.to_string(), "HTTP name resolution memory limit exceeded");
    release.send(()).unwrap();
    released(&budget).await;
}

#[test]
fn cancelling_a_queued_lookup_aborts_before_the_resolver_runs() {
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).max_blocking_threads(1).enable_all().build().unwrap();
    let (started, ready) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let occupied = runtime.spawn_blocking(move || {
        started.send(()).unwrap();
        wait.recv_timeout(Duration::from_secs(5)).unwrap();
    });
    ready.recv_timeout(Duration::from_secs(5)).unwrap();
    let budget = MemoryBudget::new(LOOKUP_ALLOCATION_BOUND * 2);
    let called = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&called);
    runtime.block_on(async {
        let mut lookup = Box::pin(resolve_with("registry.invalid", 8081, &budget, move |_, _| {
            observed.store(true, Ordering::Release);
            Ok("127.0.0.1:8081".parse().unwrap())
        }));
        assert!(futures::poll!(&mut lookup).is_pending());
        assert!(budget.used_bytes() >= LOOKUP_ALLOCATION_BOUND);
        drop(lookup);
        release.send(()).unwrap();
        occupied.await.unwrap();
        released(&budget).await;
    });
    assert!(!called.load(Ordering::Acquire));
}
