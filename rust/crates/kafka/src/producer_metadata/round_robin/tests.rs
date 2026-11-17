use super::*;

#[test]
fn available_ids_follow_snapshot_order_without_sorting_or_renumbering() {
    let partitions = [
        PartitionChoice { id: 40, has_leader: true },
        PartitionChoice { id: 2, has_leader: false },
        PartitionChoice { id: -7, has_leader: true },
        PartitionChoice { id: 11, has_leader: true },
    ];
    let choices = PartitionChoices::new(&partitions).unwrap();
    let mut counter = RoundRobinCounter::new();
    assert_eq!((0..7).map(|_| counter.select(choices).unwrap().id()).collect::<Vec<_>>(), [40, -7, 11, 40, -7, 11, 40]);
}

#[test]
fn no_leaders_selects_an_index_and_empty_topic_still_consumes_counter() {
    let partitions = [
        PartitionChoice { id: 40, has_leader: false },
        PartitionChoice { id: 99, has_leader: false },
        PartitionChoice { id: -7, has_leader: false },
    ];
    let choices = PartitionChoices::new(&partitions).unwrap();
    let mut counter = RoundRobinCounter::new();
    assert_eq!(counter.select(PartitionChoices::new(&[]).unwrap()), Err(EmptyTopic));
    assert_eq!((0..4).map(|_| counter.select(choices).unwrap().id()).collect::<Vec<_>>(), [1, 2, 0, 1]);
}

#[test]
fn signed_counter_wrap_masks_the_sign_bit_without_absolute_value() {
    let partitions = [
        PartitionChoice { id: 7, has_leader: true },
        PartitionChoice { id: 9, has_leader: true },
        PartitionChoice { id: 5, has_leader: true },
    ];
    let choices = PartitionChoices::new(&partitions).unwrap();
    let mut counter = RoundRobinCounter { next: i32::MAX - 1 };
    assert_eq!((0..4).map(|_| counter.select(choices).unwrap().id()).collect::<Vec<_>>(), [7, 9, 7, 9]);
    counter.next = -2;
    assert_eq!((0..4).map(|_| counter.select(choices).unwrap().id()).collect::<Vec<_>>(), [7, 9, 7, 9]);
}

#[test]
fn topic_counters_and_selected_sends_survive_snapshot_changes_independently() {
    let first = [PartitionChoice { id: 12, has_leader: true }, PartitionChoice { id: 4, has_leader: true }];
    let second = [PartitionChoice { id: 88, has_leader: true }, PartitionChoice { id: 77, has_leader: true }];
    let mut a = RoundRobinCounter::new();
    let mut b = RoundRobinCounter::new();
    let selected = a.select(PartitionChoices::new(&first).unwrap()).unwrap();
    assert_eq!(selected.id(), 12);
    assert_eq!(b.select(PartitionChoices::new(&second).unwrap()).unwrap().id(), 88);
    assert_eq!(selected.id(), 12);
    assert_eq!(a.select(PartitionChoices::new(&second).unwrap()).unwrap().id(), 77);
    assert_eq!(b.select(PartitionChoices::new(&first).unwrap()).unwrap().id(), 4);
    assert_eq!(selected.id(), 12);
}
