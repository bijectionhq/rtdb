use std::sync::{Arc, Barrier, atomic::{AtomicUsize, Ordering}};

use rtdb_types::MemoryBudget;

/// The most unspent credit a credited budget can keep across every thread shard it may have.
const MAXIMUM_IDLE_CREDIT: usize = 64 * (16 << 10);

#[test]
fn a_credited_child_admits_small_reservations_from_thread_credit_and_reports_only_what_they_retain() {
    let root = MemoryBudget::new(1 << 20);
    let child = root.credited();
    let held = (0..100).map(|_| child.reserve(10).unwrap()).collect::<Vec<_>>();
    assert_eq!(child.used_bytes(), 1000);
    assert!(root.used_bytes() >= 1000 && root.used_bytes() <= 1000 + MAXIMUM_IDLE_CREDIT);
    let large = child.reserve(100_000).unwrap();
    assert_eq!(child.used_bytes(), 101_000, "a large reservation is admitted exactly");
    drop(large);
    drop(held);
    assert_eq!(child.used_bytes(), 0);
    assert!(root.used_bytes() <= MAXIMUM_IDLE_CREDIT);
    drop(child);
    assert_eq!(root.used_bytes(), 0, "a dropped credited budget returns its unspent credit");
}

#[test]
fn unspent_credit_never_refuses_a_reservation_that_fits() {
    let limit = 64 << 10;
    let root = MemoryBudget::new(limit);
    let child = root.credited();
    let workers = (0..8).map(|_| {
        let child = Arc::clone(&child);
        std::thread::spawn(move || drop(child.reserve(100).unwrap()))
    }).collect::<Vec<_>>();
    for worker in workers { worker.join().unwrap(); }
    assert_eq!(child.used_bytes(), 0);

    let whole = root.reserve(limit).expect("idle credit is reclaimed for a reservation on the root");
    assert_eq!(root.used_bytes(), limit);
    assert_eq!(child.reserve(1).unwrap_err().code, "rtdb/memory-budget-exceeded");
    drop(whole);

    let through_child = child.scope().reserve(limit).expect("idle credit is reclaimed for a reservation through the child");
    assert_eq!(root.reserve(1).unwrap_err().code, "rtdb/memory-budget-exceeded");
    drop(through_child);
    drop(child);
    assert_eq!(root.used_bytes(), 0);
}

#[test]
fn small_reservations_fill_a_credited_budget_exactly_to_its_limit() {
    let limit = 40_000;
    let root = MemoryBudget::new(limit);
    let child = root.credited();
    let held = (0..limit / 100).map(|_| child.reserve(100).expect("every reservation up to the limit fits")).collect::<Vec<_>>();
    assert_eq!(child.used_bytes(), limit);
    assert_eq!(root.used_bytes(), limit);
    assert_eq!(child.reserve(1).unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert_eq!(root.reserve(1).unwrap_err().code, "rtdb/memory-budget-exceeded");
    drop(held);
    drop(child);
    assert_eq!(root.used_bytes(), 0);
}

#[test]
fn concurrent_reservations_through_a_credited_budget_never_exceed_the_limit_and_leak_nothing() {
    let limit = 256 << 10;
    let root = MemoryBudget::new(limit);
    let child = root.credited();
    let live = Arc::new(AtomicUsize::new(0));
    let refused = Arc::new(AtomicUsize::new(0));
    let threads = 16;
    let start = Arc::new(Barrier::new(threads));
    let workers = (0..threads).map(|thread| {
        let (child, live, refused, start) = (Arc::clone(&child), Arc::clone(&live), Arc::clone(&refused), Arc::clone(&start));
        std::thread::spawn(move || {
            let scope = child.scope();
            let mut held = Vec::new();
            start.wait();
            for step in 0..20_000usize {
                let bytes = 1 + (step * 7919 + thread * 104_729) % 12_000;
                match scope.reserve(bytes) {
                    Ok(reservation) => {
                        let total = live.fetch_add(bytes, Ordering::SeqCst) + bytes;
                        assert!(total <= limit, "{total} retained bytes exceed the {limit} byte limit");
                        held.push(reservation);
                    }
                    Err(error) => {
                        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
                        refused.fetch_add(1, Ordering::Relaxed);
                    }
                }
                if held.len() > 8 || step % 3 == 0 {
                    if let Some(reservation) = held.pop() {
                        live.fetch_sub(reservation.bytes(), Ordering::SeqCst);
                        drop(reservation);
                    }
                }
            }
            for reservation in held.drain(..) { live.fetch_sub(reservation.bytes(), Ordering::SeqCst); }
            assert_eq!(scope.used_bytes(), 0);
        })
    }).collect::<Vec<_>>();
    for worker in workers { worker.join().unwrap(); }
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert_eq!(child.used_bytes(), 0);
    assert!(root.used_bytes() <= MAXIMUM_IDLE_CREDIT, "idle credit is bounded by the shards, not the work: {}", root.used_bytes());
    assert!(root.peak_bytes() <= limit);
    let whole = root.reserve(limit).expect("after the work, the whole limit fits again");
    drop(whole);
    drop(child);
    assert_eq!(root.used_bytes(), 0);
}

#[test]
fn a_reservation_that_does_not_fit_is_refused_under_concurrent_credit() {
    let limit = 128 << 10;
    let root = MemoryBudget::new(limit);
    let child = root.credited();
    let held = root.reserve(limit - 1000).unwrap();
    let start = Arc::new(Barrier::new(8));
    let workers = (0..8).map(|_| {
        let (child, start) = (Arc::clone(&child), Arc::clone(&start));
        std::thread::spawn(move || {
            start.wait();
            (0..1000).filter_map(|_| child.reserve(64).ok()).map(|reservation| reservation.bytes()).max()
        })
    }).collect::<Vec<_>>();
    for worker in workers { worker.join().unwrap(); }
    assert!(root.reserve(1001).is_err(), "only 1000 bytes remain beyond the held reservation");
    assert!(root.used_bytes() <= limit);
    drop(held);
    drop(child);
    assert_eq!(root.used_bytes(), 0);
}

#[test]
fn growing_a_credited_reservation_is_admitted_exactly() {
    let root = MemoryBudget::new(20_000);
    let child = root.credited();
    let mut reservation = child.reserve(100).unwrap();
    Arc::get_mut(&mut reservation).unwrap().grow_to(20_000).expect("growth that fits reclaims the reservation's own credit");
    assert_eq!(child.used_bytes(), 20_000);
    assert_eq!(Arc::get_mut(&mut reservation).unwrap().grow_to(20_001).unwrap_err().code, "rtdb/memory-budget-exceeded");
    Arc::get_mut(&mut reservation).unwrap().shrink_to(10).unwrap();
    assert_eq!(child.used_bytes(), 10);
    drop(reservation);
    drop(child);
    assert_eq!(root.used_bytes(), 0);
}
