use rtdb_temporal::{Ceiling, HistoryResolver, Polygon, PolygonCalculator};
use rtdb_types::{EventHeader, EventOp, Iid, Interval, TemporalBounds, Timestamp};

fn times(values: &[i64]) -> Vec<Timestamp> { values.iter().copied().map(Timestamp).collect() }
fn interval(lower: i64, upper: i64) -> Interval { Interval::from_micros(lower, upper).unwrap() }
fn event(iid: u8, system_from: i64, lower: i64, upper: i64, op: EventOp) -> EventHeader {
    EventHeader { iid: Iid([iid; 16]), system_from: Timestamp(system_from), valid_time: interval(lower, upper), op }
}
fn polygon(valid_times: &[i64], ceilings: &[i64]) -> Polygon {
    Polygon::new(times(valid_times), times(ceilings)).unwrap()
}

#[test]
fn malformed_polygon_shapes_are_rejected_before_recency_access() {
    assert!(Polygon::new(times(&[1]), times(&[2])).is_err());
    assert!(Polygon::new(times(&[]), times(&[2])).is_err());
    assert_eq!(Polygon::new(vec![], vec![]).unwrap().recency(), Timestamp::MIN);
}

#[test]
fn earlier_event_resolves_against_only_its_own_entity_ceiling() {
    let mut calc = PolygonCalculator::new(None);
    calc.calculate(&event(1, 2, 10, 20, EventOp::Put)).unwrap();
    assert_eq!(calc.calculate(&event(1, 1, 0, 100, EventOp::Put)).unwrap(),
        Some(polygon(&[0, 10, 20, 100], &[i64::MAX, 2, i64::MAX])));
    assert_eq!(calc.calculate(&event(2, 1, 0, 100, EventOp::Put)).unwrap(),
        Some(polygon(&[0, 100], &[i64::MAX])));
}

#[test]
fn erase_suppresses_earlier_events_of_only_that_entity() {
    let mut calc = PolygonCalculator::new(None);
    calc.calculate(&event(1, 2, 0, i64::MAX, EventOp::Erase)).unwrap();
    assert_eq!(calc.calculate(&event(1, 1, 0, 100, EventOp::Put)).unwrap(), None);
    assert_eq!(calc.calculate(&event(2, 1, 0, 100, EventOp::Put)).unwrap(), Some(polygon(&[0, 100], &[i64::MAX])));
}

#[test]
fn erase_after_query_system_time_still_suppresses() {
    let mut calc = PolygonCalculator::new(Some(TemporalBounds { valid_time: Interval::ALL, system_time: interval(1, 2) }));
    calc.calculate(&event(1, 5, 0, i64::MAX, EventOp::Erase)).unwrap();
    assert_eq!(calc.calculate(&event(1, 1, 0, 100, EventOp::Put)).unwrap(), None);
}

#[test]
fn put_after_query_system_time_neither_resolves_nor_constrains() {
    let mut calc = PolygonCalculator::new(Some(TemporalBounds { valid_time: Interval::ALL, system_time: interval(1, 3) }));
    assert_eq!(calc.calculate(&event(1, 5, 10, 20, EventOp::Put)).unwrap(), None);
    assert_eq!(calc.calculate(&event(1, 1, 0, 100, EventOp::Put)).unwrap(), Some(polygon(&[0, 100], &[i64::MAX])));
}

#[test]
fn ceiling_replaces_ranges_with_exact_reference_breakpoints() {
    let mut ceiling = Ceiling::default();
    assert_eq!(ceiling.valid_times(), times(&[i64::MIN, i64::MAX]));
    assert_eq!(ceiling.system_time_ceilings(), times(&[i64::MAX]));
    for (system, lower, upper, expected_bounds, expected_ceilings) in [
        (4, 4, i64::MAX, vec![i64::MIN, 4, i64::MAX], vec![i64::MAX, 4]),
        (3, 2, i64::MAX, vec![i64::MIN, 2, i64::MAX], vec![i64::MAX, 3]),
        (2, 1, 4, vec![i64::MIN, 1, 4, i64::MAX], vec![i64::MAX, 2, 3]),
        (1, 1, 4, vec![i64::MIN, 1, 4, i64::MAX], vec![i64::MAX, 1, 3]),
        (0, 0, 6, vec![i64::MIN, 0, 6, i64::MAX], vec![i64::MAX, 0, 3]),
    ] {
        ceiling.apply_event(Timestamp(system), interval(lower, upper));
        assert_eq!(ceiling.valid_times(), times(&expected_bounds));
        assert_eq!(ceiling.system_time_ceilings(), times(&expected_ceilings));
    }
}

#[test]
fn polygon_intersections_preserve_each_upstream_overlap_case() {
    for (new_from, new_to, old_from, old_to, expected_times, expected_ceilings) in [
        (2005, 2009, 2010, 2020, vec![2010, 2020], vec![i64::MAX]),
        (2010, 2020, 2015, 2025, vec![2015, 2020, 2025], vec![1, i64::MAX]),
        (2010, 2020, 2010, 2025, vec![2010, 2020, 2025], vec![1, i64::MAX]),
        (2015, 2020, 2010, 2025, vec![2010, 2015, 2020, 2025], vec![i64::MAX, 1, i64::MAX]),
        (2010, 2025, 2010, 2020, vec![2010, 2020], vec![1]),
        (2015, 2025, 2010, 2025, vec![2010, 2015, 2025], vec![i64::MAX, 1]),
        (2015, 2025, 2010, 2020, vec![2010, 2015, 2020], vec![i64::MAX, 1]),
        (2005, 2010, 2010, 2020, vec![2010, 2020], vec![i64::MAX]),
        (2010, 2020, 2005, 2010, vec![2005, 2010], vec![i64::MAX]),
        (2010, 2020, 2005, 2009, vec![2005, 2009], vec![i64::MAX]),
    ] {
        let mut ceiling = Ceiling::default();
        ceiling.apply_event(Timestamp(1), interval(new_from, new_to));
        assert_eq!(ceiling.polygon(interval(old_from, old_to)), polygon(&expected_times, &expected_ceilings));
    }
}

#[test]
fn recency_matches_all_reference_rectangles_and_polygons() {
    for (valid_times, ceilings, expected) in [
        (vec![3, i64::MAX], vec![i64::MAX], i64::MAX),
        (vec![4, 10], vec![i64::MAX], 10),
        (vec![6, 10], vec![4], 4), (vec![6, 10], vec![6], 6),
        (vec![6, 10], vec![8], 8), (vec![6, 10], vec![10], 10),
        (vec![6, 10], vec![12], 10),
        (vec![3, 5, i64::MAX], vec![i64::MAX, 5], 5),
        (vec![3, 5, i64::MAX], vec![i64::MAX, 6], 6),
        (vec![3, 7, i64::MAX], vec![i64::MAX, 6], 7),
        (vec![1, 4], vec![5], 4),
        (vec![10, 12, 15, 18], vec![8, 6, 3], 8),
        (vec![10, 12, 15, 18], vec![6, 8, 3], 8),
        (vec![0, 2, 5, 8], vec![7, 4, 2], 4),
        (vec![100, 100, 5, 8], vec![100, 9, 6], 6),
    ] {
        assert_eq!(polygon(&valid_times, &ceilings).recency(), Timestamp(expected));
    }
}

#[test]
fn a_correction_failing_the_value_predicate_still_supersedes_the_old_price() {
    let bounds = TemporalBounds { valid_time: interval(15, 16), system_time: interval(3, 4) };
    let mut resolver = HistoryResolver::new(bounds, false);
    let mut rows = resolver.resolve(&event(1, 3, 10, 20, EventOp::Put), 9).unwrap();
    rows.extend(resolver.resolve(&event(1, 1, 0, 100, EventOp::Put), 12).unwrap());
    assert_eq!(rows.iter().map(|row| row.source).collect::<Vec<_>>(), vec![9]);
    assert!(rows.into_iter().filter(|row| row.source > 10).collect::<Vec<_>>().is_empty());
}

#[test]
fn delete_preserves_earlier_history_while_erase_removes_it() {
    let bounds = TemporalBounds { valid_time: interval(15, 16), system_time: interval(1, 2) };
    for (op, count) in [(EventOp::Delete, 1), (EventOp::Erase, 0)] {
        let mut resolver = HistoryResolver::new(bounds, false);
        assert!(resolver.resolve(&event(1, 3, 0, i64::MAX, op), 0).unwrap().is_empty());
        assert_eq!(resolver.resolve(&event(1, 1, 0, 100, EventOp::Put), 12).unwrap().len(), count);
    }
}

#[test]
fn a_post_erase_reinsertion_survives_and_does_not_restore_older_rows() {
    let mut resolver = HistoryResolver::new(TemporalBounds::default(), false);
    let mut rows = resolver.resolve(&event(1, 5, 50, 100, EventOp::Put), 15).unwrap();
    rows.extend(resolver.resolve(&event(1, 3, 0, i64::MAX, EventOp::Erase), 0).unwrap());
    rows.extend(resolver.resolve(&event(1, 1, 0, 100, EventOp::Put), 12).unwrap());
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].source, 15);
    assert_eq!(rows[0].valid_time, interval(50, 100));
}

#[test]
fn resolver_rejects_out_of_order_input_before_corrupting_visibility() {
    let mut calc = PolygonCalculator::new(None);
    calc.calculate(&event(2, 3, 0, 100, EventOp::Put)).unwrap();
    assert!(calc.calculate(&event(1, 2, 0, 100, EventOp::Put)).is_err());
    assert!(calc.calculate(&event(2, 4, 0, 100, EventOp::Put)).is_err());
}
