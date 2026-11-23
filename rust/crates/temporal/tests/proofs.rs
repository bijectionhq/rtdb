#![allow(unexpected_cfgs)]
#![cfg(kani)]

use rtdb_temporal::Polygon;
use rtdb_types::{Interval, Timestamp};

fn symbolic_interval() -> Interval {
    let lower: i64 = kani::any();
    let upper: i64 = kani::any();
    kani::assume(lower <= upper);
    Interval::from_micros(lower, upper).unwrap()
}

#[kani::proof]
fn interval_intersection_is_exact_at_every_point() {
    let left = symbolic_interval();
    let right = symbolic_interval();
    let point = Timestamp(kani::any());
    let intersection = left.intersection(right);
    let lower = left.lower().max(right.lower());
    let upper = left.upper().min(right.upper());
    assert_eq!(intersection.is_some(), lower < upper);
    assert_eq!(intersection.is_some(), left.intersects(right));
    if let Some(range) = intersection {
        assert_eq!((range.lower(), range.upper()), (lower, upper));
    }
    assert_eq!(
        intersection.is_some_and(|range| range.contains(point)),
        lower <= point && point < upper,
    );
    assert_eq!(left.contains(point), left.lower() <= point && point < left.upper());
    assert_eq!(right.contains(point), right.lower() <= point && point < right.upper());
}

#[kani::proof]
#[kani::unwind(5)]
fn three_range_recency_matches_the_direct_formula() {
    let [start, first, second, end] = kani::any::<[i64; 4]>();
    let [left, middle, right] = kani::any::<[i64; 3]>();
    kani::assume(start <= first && first <= second && second <= end);
    let polygon = Polygon::new(
        vec![Timestamp(start), Timestamp(first), Timestamp(second), Timestamp(end)],
        vec![Timestamp(left), Timestamp(middle), Timestamp(right)],
    ).unwrap();
    assert_eq!(
        polygon.recency(),
        Timestamp(left.min(first).max(middle.min(second)).max(right.min(end))),
    );
}
