use super::PolygonView;
use rtdb_types::{Interval, Timestamp};

#[kani::proof]
#[kani::unwind(6)]
fn one_split_projects_the_exact_ordered_overlaps_of_a_nonempty_interval() {
    let split = Timestamp(kani::any());
    kani::assume(Timestamp::MIN < split && split < Timestamp::MAX);
    let valid_times = [Timestamp::MIN, split, Timestamp::MAX];
    let ceilings = [Timestamp(kani::any()), Timestamp(kani::any())];
    let lower = Timestamp(kani::any());
    let upper = Timestamp(kani::any());
    kani::assume(lower < upper);
    let interval = Interval::new(lower, upper).unwrap();

    let view = PolygonView::new(&valid_times, &ceilings, interval);
    let mut ranges = view.ranges();
    if lower < split {
        assert_eq!(ranges.next(), Some((lower, upper.min(split), ceilings[0])));
    }
    if split < upper {
        assert_eq!(ranges.next(), Some((lower.max(split), upper, ceilings[1])));
    }
    assert_eq!(ranges.next(), None);
}

#[kani::proof]
#[kani::unwind(6)]
fn one_split_projects_an_empty_interval_at_every_representable_position() {
    let split = Timestamp(kani::any());
    kani::assume(Timestamp::MIN < split && split < Timestamp::MAX);
    let valid_times = [Timestamp::MIN, split, Timestamp::MAX];
    let ceilings = [Timestamp(kani::any()), Timestamp(kani::any())];
    let point = Timestamp(kani::any());
    let interval = Interval::new(point, point).unwrap();

    let view = PolygonView::new(&valid_times, &ceilings, interval);
    let mut ranges = view.ranges();
    let expected = if point < split { ceilings[0] } else { ceilings[1] };
    assert_eq!(ranges.next(), Some((point, point, expected)));
    assert_eq!(ranges.next(), None);
}
