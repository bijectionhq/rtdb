use super::*;

#[test]
fn latest_send_completion_and_maximum_gate_capacity_while_receives_are_fifo() {
    let mut queue = InFlightQueue::new(2, MemoryBudget::new(1 << 20)).unwrap();
    assert!(queue.can_send_more());
    assert!(queue.complete_send().is_err());
    queue.add(PendingRequest::new("first", 10, 10, true)).unwrap();
    assert!(!queue.can_send_more());
    assert!(queue.complete_send().unwrap().is_none());
    assert!(queue.can_send_more());
    queue.add(PendingRequest::new("second", 11, 10, true)).unwrap();
    assert_eq!(queue.last_sent().map(PendingRequest::payload), Some(&"second"));
    assert!(queue.complete_send().unwrap().is_none());
    assert_eq!(queue.count(), 2);
    assert!(!queue.can_send_more());
    assert_eq!(queue.complete_next().unwrap().into_payload(), "first");
    assert!(queue.can_send_more());
    assert_eq!(queue.complete_next().unwrap().into_payload(), "second");
    assert!(queue.complete_next().is_none());
}

#[test]
fn no_response_completes_the_newest_and_disconnect_drains_the_oldest_first() {
    let memory = MemoryBudget::new(1 << 20);
    let mut queue = InFlightQueue::new(3, memory.clone()).unwrap();
    queue.add(PendingRequest::new(1, 10, 10, true)).unwrap();
    assert!(queue.complete_send().unwrap().is_none());
    queue.add(PendingRequest::new(2, 11, 10, false)).unwrap();
    assert_eq!(queue.complete_send().unwrap().unwrap().into_payload(), 2);
    queue.add(PendingRequest::new(3, 12, 10, true)).unwrap();

    let mut cleared = queue.clear_all();
    assert_eq!(cleared.next().unwrap().into_payload(), 1);
    assert_eq!(cleared.next().unwrap().into_payload(), 3);
    assert!(cleared.next().is_none());
    assert!(memory.used_bytes() > 0);
    drop(cleared);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn strict_timeout_uses_wrapping_elapsed_and_only_existing_requests_accumulate_throttle() {
    let mut queue = InFlightQueue::new(2, MemoryBudget::new(1 << 20)).unwrap();
    queue.add(PendingRequest::new(1, i64::MAX - 5, 10, true)).unwrap();
    assert!(!queue.has_expired(i64::MAX - 6));
    assert!(!queue.has_expired(i64::MIN + 4));
    assert!(queue.has_expired(i64::MIN + 5));
    queue.increment_throttle(5);
    assert!(!queue.has_expired(i64::MIN + 9));
    assert!(queue.has_expired(i64::MIN + 10));
    queue.add(PendingRequest::new(2, i64::MIN + 6, 1, true)).unwrap();
    assert!(!queue.has_expired(i64::MIN + 7));
    assert!(queue.has_expired(i64::MIN + 8));
}

#[test]
fn capacity_growth_admits_while_the_old_queue_is_live_and_denial_keeps_its_entries() {
    let memory = MemoryBudget::new(1 << 20);
    let mut queue = InFlightQueue::new(1, memory.clone()).unwrap();
    queue.add(PendingRequest::new(0, 1, 10, true)).unwrap();
    let capacity = queue.storage.as_ref().unwrap().requests.capacity();
    for id in 1..capacity {
        queue.add(PendingRequest::new(id, 1, 10, true)).unwrap();
    }
    let old = memory.used_bytes();
    let measured = Budget(memory.clone()).reserve(bytes::<PendingRequest<usize>>(capacity * 2).unwrap()).unwrap();
    let growth = memory.used_bytes() - old;
    drop(measured);
    let full = memory.reserve(memory.limit_bytes() - old - growth + 1).unwrap();
    assert!(queue.add(PendingRequest::new(capacity, 1, 10, true)).is_err());
    assert_eq!(queue.count() as usize, capacity);
    assert_eq!(queue.last_sent().map(PendingRequest::payload), Some(&(capacity - 1)));
    drop(full);
    assert_eq!(memory.used_bytes(), old);
    queue.add(PendingRequest::new(capacity, 1, 10, true)).unwrap();
    assert_eq!(memory.used_bytes(), growth);
    assert_eq!(queue.count() as usize, capacity + 1);
    assert_eq!(queue.complete_next().unwrap().into_payload(), 0);
    drop(queue);
    assert_eq!(memory.used_bytes(), 0);
}
