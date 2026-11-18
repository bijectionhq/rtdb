use super::*;
use rtdb_columnar::MemoryBudget;

#[test]
fn overlapping_captures_share_one_thread_recycler_and_keep_version_specific_returns() {
    for (profile, expected) in [(Profile::Jackson216, 4000), (Profile::Jackson221, 6000)] {
        let mut context = ConversionContext::new(MemoryBudget::new(usize::MAX));
        let pool = ThreadTokenPool::new(profile, &mut context).unwrap();
        let mut outer = pool.buffers();
        let mut inner = pool.buffers();
        outer.acquire(&mut context).unwrap();
        let large = outer.take(6000, &mut context).unwrap();
        inner.acquire(&mut context).unwrap();
        let (Capture::Active(left), Capture::Active(right)) = (&outer.capture, &inner.capture) else { panic!("captures") };
        assert!(Arc::ptr_eq(left, right));
        let small = inner.take(0, &mut context).unwrap();
        assert_eq!(small.len(), 4000);
        outer.give(large, &mut context).unwrap();
        outer.close(&mut context).unwrap();
        inner.give(small, &mut context).unwrap();
        inner.close(&mut context).unwrap();
        let mut later = pool.buffers();
        later.acquire(&mut context).unwrap();
        let buffer = later.take(0, &mut context).unwrap();
        assert_eq!(buffer.len(), expected);
        later.give(buffer, &mut context).unwrap();
        later.close(&mut context).unwrap();
        assert_eq!(pool.entries.lock().unwrap().values.len(), 1);
    }
}

#[test]
fn domain_and_native_thread_boundaries_keep_distinct_slots() {
    let budget = MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let pool = ThreadTokenPool::new(Profile::Jackson221, &mut context).unwrap();
    let other = ThreadTokenPool::new(Profile::Jackson221, &mut context).unwrap();
    let mut buffers = pool.buffers();
    buffers.acquire(&mut context).unwrap();
    let buffer = buffers.take(6000, &mut context).unwrap();
    buffers.give(buffer, &mut context).unwrap();
    buffers.close(&mut context).unwrap();
    let mut isolated = other.buffers();
    isolated.acquire(&mut context).unwrap();
    let buffer = isolated.take(0, &mut context).unwrap();
    assert_eq!(buffer.len(), 4000);
    isolated.give(buffer, &mut context).unwrap();
    isolated.close(&mut context).unwrap();
    let worker_pool = Arc::clone(&pool);
    let worker_budget = Arc::clone(&budget);
    std::thread::spawn(move || {
        let mut context = ConversionContext::new(worker_budget);
        let mut buffers = worker_pool.buffers();
        buffers.acquire(&mut context).unwrap();
        let buffer = buffers.take(0, &mut context).unwrap();
        assert_eq!(buffer.len(), 4000);
        buffers.give(buffer, &mut context).unwrap();
        buffers.close(&mut context).unwrap();
    }).join().unwrap();
    assert_eq!(pool.entries.lock().unwrap().values.len(), 2);
    buffers.acquire(&mut context).unwrap();
    let buffer = buffers.take(0, &mut context).unwrap();
    assert_eq!(buffer.len(), 6000);
    buffers.give(buffer, &mut context).unwrap();
    buffers.close(&mut context).unwrap();
    drop(buffers);
    drop(isolated);
    drop(context);
    assert!(budget.used_bytes() > 0);
    drop(pool);
    drop(other);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn fresh_context_retains_shared_slot_storage_without_a_second_physical_charge() {
    let budget = MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let pool = ThreadTokenPool::new(Profile::Jackson221, &mut context).unwrap();
    let mut first = pool.buffers();
    first.acquire(&mut context).unwrap();
    let buffer = first.take(6000, &mut context).unwrap();
    first.give(buffer, &mut context).unwrap();
    first.close(&mut context).unwrap();
    drop(first);
    drop(context);
    let used = budget.used_bytes();
    let mut denied = ConversionContext::with_limit(Arc::clone(&budget), 0);
    let mut later = pool.buffers();
    assert!(later.acquire(&mut denied).is_err());
    assert!(matches!(later.capture, Capture::Idle));
    let mut next = ConversionContext::new(Arc::clone(&budget));
    later.acquire(&mut next).unwrap();
    let buffer = later.take(0, &mut next).unwrap();
    assert_eq!(buffer.len(), 6000);
    assert!(next.retains(&pool.charge));
    later.give(buffer, &mut next).unwrap();
    later.close(&mut next).unwrap();
    assert_eq!(budget.used_bytes(), used);
    drop(later);
    drop(pool);
    assert!(budget.used_bytes() > 0);
    drop(next);
    drop(denied);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn thread_index_growth_is_admitted_before_reallocation_and_rejected_insert_stays_absent() {
    let budget = MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let pool = ThreadTokenPool::new(Profile::Jackson221, &mut context).unwrap();
    let threads = (0..5).map(|_| std::thread::spawn(|| std::thread::current().id()).join().unwrap()).collect::<Vec<_>>();
    for thread in &threads[..4] { pool.acquire(*thread, &mut context).unwrap(); }
    let (backing, capacity) = {
        let entries = pool.entries.lock().unwrap();
        (entries.charge.bytes(), entries.values.capacity())
    };
    assert_eq!(capacity, 4);
    let limit = pool.charge.bytes() + backing + shared_bound::<Slot>().unwrap();
    let mut denied = ConversionContext::with_limit(Arc::clone(&budget), limit);
    assert!(pool.acquire(threads[4], &mut denied).is_err());
    {
        let entries = pool.entries.lock().unwrap();
        assert_eq!(entries.values.len(), 4);
        assert_eq!(entries.values.capacity(), capacity);
        assert!(!entries.values.iter().any(|entry| entry.thread == threads[4]));
    }
    pool.acquire(threads[4], &mut context).unwrap();
    let entries = pool.entries.lock().unwrap();
    assert_eq!(entries.values.len(), 5);
    assert!(entries.values.capacity() >= 8);
}
