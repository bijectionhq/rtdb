use std::{cell::Cell, sync::Arc};

use rtdb_temporal::{AdmittedPolygonCalculator, Ceiling, HistoryResolver, Polygon, PolygonCalculator, ResolvedRow};
use rtdb_types::{Anomaly, EventHeader, EventOp, Iid, Interval, MemoryBudget, TemporalBounds, Timestamp};

fn times(values: &[i64]) -> Vec<Timestamp> { values.iter().copied().map(Timestamp).collect() }
fn interval(lower: i64, upper: i64) -> Interval { Interval::from_micros(lower, upper).unwrap() }
fn event(iid: u8, system_from: i64, lower: i64, upper: i64, op: EventOp) -> EventHeader {
    EventHeader { position: None, commit_position: 0, iid: Iid([iid; 16]), system_from: Timestamp(system_from), valid_time: interval(lower, upper), op }
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
fn ceiling_windows_agree_with_pointwise_overwrites_at_sentinels_and_interior_points() {
    let endpoints = [i64::MIN, -3, 0, 1, 4, i64::MAX];
    let intervals: Vec<_> = endpoints.iter().enumerate()
        .flat_map(|(index, &lower)| endpoints[index..].iter().map(move |&upper| interval(lower, upper)))
        .collect();
    let points = [i64::MIN, i64::MIN + 1, -4, -3, -2, -1, 0, 1, 2, 4, 5, i64::MAX - 1, i64::MAX].map(Timestamp);
    for &first in &intervals {
        for &second in &intervals {
            let mut ceiling = Ceiling::default();
            ceiling.apply_event(Timestamp(20), first);
            ceiling.apply_event(Timestamp(10), second);
            assert!(ceiling.valid_times().windows(2).all(|bounds| bounds[0] < bounds[1]));
            for &requested in &intervals {
                let polygon = ceiling.polygon(requested);
                assert_eq!(polygon.valid_times().first(), Some(&requested.lower()));
                assert_eq!(polygon.valid_times().last(), Some(&requested.upper()));
                if requested.is_empty() {
                    let point = Timestamp(requested.lower().0.min(i64::MAX - 1));
                    let upper = if second.contains(point) { Timestamp(10) }
                        else if first.contains(point) { Timestamp(20) }
                        else { Timestamp::MAX };
                    assert_eq!(polygon.system_time_ceilings(), &[upper]);
                }
                for point in points {
                    let expected = requested.contains(point).then(|| {
                        if second.contains(point) { Timestamp(10) }
                        else if first.contains(point) { Timestamp(20) }
                        else { Timestamp::MAX }
                    });
                    let mut visible = polygon.valid_times().windows(2).zip(polygon.system_time_ceilings())
                        .filter_map(|(bounds, &upper)| (bounds[0] <= point && point < bounds[1]).then_some(upper));
                    assert_eq!(visible.next(), expected, "{first:?}, {second:?}, {requested:?} at {point:?}");
                    assert!(visible.next().is_none());
                }
            }
        }
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
fn fragment_admission_checks_both_axes_and_clips_only_valid_time() {
    let bounds = TemporalBounds { valid_time: interval(6, 9), system_time: interval(5, 10) };
    for clamp_valid in [false, true] {
        let mut resolver = HistoryResolver::new(bounds, clamp_valid);
        let mut fragments = Vec::new();
        for system in [8, 3, 1] {
            resolver.resolve_fragments(&event(1, system, 4, 12, EventOp::Put), |valid, system| {
                fragments.push((valid, system));
                Ok(())
            }).unwrap();
        }
        let valid = if clamp_valid { interval(6, 9) } else { interval(4, 12) };
        assert_eq!(fragments, [(valid, interval(8, i64::MAX)), (valid, interval(3, 8))]);
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

#[test]
fn history_growth_keeps_the_previous_charge_and_reserves_before_cloning() {
    struct CountedSource<'a>(&'a Cell<usize>);
    impl Clone for CountedSource<'_> {
        fn clone(&self) -> Self {
            self.0.set(self.0.get() + 1);
            Self(self.0)
        }
    }

    let slot = std::mem::size_of::<ResolvedRow<CountedSource<'_>>>();
    let previous = 2 * slot + 256;
    let replacement = 4 * slot + 256;
    let delete = event(1, 200, 10, 20, EventOp::Delete);
    let put = event(1, 100, 0, 30, EventOp::Put);
    for fits in [false, true] {
        let limit = previous + replacement - usize::from(!fits);
        let root = MemoryBudget::new(limit);
        let budget = root.scope();
        let clones = Cell::new(0);
        let mut resolver = HistoryResolver::new(TemporalBounds::default(), false);
        assert!(resolver.resolve_in(&delete, CountedSource(&clones), &budget).unwrap().as_slice().is_empty());
        assert_eq!(root.peak_bytes(), 0);

        let result = resolver.resolve_in(&put, CountedSource(&clones), &budget);
        if fits {
            let history = result.unwrap();
            let ranges: Vec<_> = history.as_slice().iter().map(|row| (row.valid_time, row.system_time)).collect();
            assert_eq!(ranges, [
                (interval(0, 10), interval(100, i64::MAX)),
                (interval(10, 20), interval(100, 200)),
                (interval(20, 30), interval(100, i64::MAX)),
            ]);
            assert_eq!(clones.get(), 3);
            assert_eq!(budget.used_bytes(), replacement);
            assert_eq!(root.used_bytes(), replacement);
            assert_eq!(root.peak_bytes(), limit);
            drop(history);
        } else {
            let error = result.err().unwrap();
            assert_eq!(error.code, "rtdb/memory-budget-exceeded");
            let expected = format!("Cannot reserve {replacement} bytes from budget {limit} with {previous} retained");
            assert_eq!(error.message.as_deref(), Some(expected.as_str()));
            assert_eq!(clones.get(), 2);
        }
        assert_eq!(budget.used_bytes(), 0);
        assert_eq!(root.used_bytes(), 0);
    }
}

#[test]
fn history_filters_events_before_reserving_and_still_reports_unsorted_input() {
    let budget = MemoryBudget::new(0);
    let bounds = TemporalBounds { valid_time: interval(40, 50), system_time: interval(0, 200) };
    let mut resolver = HistoryResolver::new(bounds, false);
    for header in [event(1, 200, 40, 50, EventOp::Put), event(1, 100, 0, 30, EventOp::Put)] {
        assert!(resolver.resolve_in(&header, 1, &budget).unwrap().as_slice().is_empty());
    }
    let error = resolver.resolve_in(&event(1, 101, 40, 50, EventOp::Put), 1, &budget).err().unwrap();
    assert_eq!(error.code, "xtdb/unsorted-events");
    assert_eq!(budget.peak_bytes(), 0);
}

#[test]
fn ceiling_growth_reserves_both_buffers_before_visiting_and_can_retry() {
    let timestamp_bytes = std::mem::size_of::<Timestamp>();
    let previous = (2 * 3 + 1) * timestamp_bytes + 256;
    let replacement = (2 * 6 + 1) * timestamp_bytes + 256;
    let limit = previous + replacement;
    let root = MemoryBudget::new(limit);
    let budget = root.scope();
    let mut calculator = AdmittedPolygonCalculator::new(None, Arc::clone(&budget));
    let delete = event(1, 200, 10, 20, EventOp::Delete);
    let put = event(1, 100, 0, 30, EventOp::Put);
    calculator.visit(&delete, |view| {
        assert_eq!(view.ranges().collect::<Vec<_>>(), [(Timestamp(10), Timestamp(20), Timestamp::MAX)]);
        Ok(())
    }).unwrap().unwrap();
    assert_eq!(budget.used_bytes(), 0, "an entity's first event defers its ceiling to the entity's next event");
    let pressure = root.reserve(1).unwrap();
    let denied = calculator.visit(&put, |_| -> rtdb_types::Result<()> { panic!("refused ceiling growth reached the visitor") }).unwrap_err();
    assert_eq!(denied.code, "rtdb/memory-budget-exceeded");
    let retained = previous + 1;
    let expected = format!("Cannot reserve {replacement} bytes from budget {limit} with {retained} retained");
    assert_eq!(denied.message.as_deref(), Some(expected.as_str()));
    assert_eq!(root.used_bytes(), retained);
    assert_eq!(budget.used_bytes(), previous);
    drop(pressure);

    let ranges = calculator.visit(&put, |view| {
        assert_eq!(root.used_bytes(), limit);
        assert_eq!(budget.used_bytes(), limit);
        Ok(view.ranges().collect::<Vec<_>>())
    }).unwrap().unwrap();
    assert_eq!(ranges, [
        (Timestamp(0), Timestamp(10), Timestamp::MAX),
        (Timestamp(10), Timestamp(20), Timestamp(200)),
        (Timestamp(20), Timestamp(30), Timestamp::MAX),
    ]);
    assert_eq!(root.peak_bytes(), limit);
    assert_eq!(root.used_bytes(), replacement);
    assert_eq!(budget.used_bytes(), replacement);
    calculator.reset();
    assert_eq!(root.used_bytes(), 0);

    calculator.visit(&delete, |_| Ok(())).unwrap().unwrap();
    assert_eq!(root.used_bytes(), 0);
    drop(calculator);
    assert_eq!(budget.used_bytes(), 0);
    assert_eq!(root.used_bytes(), 0);
}

#[test]
fn an_entitys_first_event_resolves_from_the_initial_ceiling_without_admitting_memory() {
    let root = MemoryBudget::new(0);
    let mut calculator = AdmittedPolygonCalculator::new(None, Arc::clone(&root));
    for iid in 1..=3 {
        calculator.visit(&event(iid, 100, 10, 20, EventOp::Put), |view| {
            assert_eq!(view.ranges().collect::<Vec<_>>(), [(Timestamp(10), Timestamp(20), Timestamp::MAX)]);
            Ok(())
        }).unwrap().unwrap();
    }
    assert_eq!(root.peak_bytes(), 0);
}

#[test]
fn visitor_failure_consumes_the_event_it_was_given() {
    let ceiling = 7 * std::mem::size_of::<Timestamp>() + 256;
    let root = MemoryBudget::new(ceiling);
    let mut calculator = AdmittedPolygonCalculator::new(None, Arc::clone(&root));
    let failure = Anomaly::incorrect("test/visitor", "Cannot emit ranges");
    assert_eq!(calculator.visit(&event(1, 200, 10, 20, EventOp::Delete), |_| Err::<(), _>(failure.clone())).unwrap_err(), failure);
    calculator.visit(&event(1, 199, 10, 20, EventOp::Put), |view| {
        assert_eq!(view.ranges().collect::<Vec<_>>(), [(Timestamp(10), Timestamp(20), Timestamp(200))]);
        Ok(())
    }).unwrap().unwrap();
    assert_eq!(root.used_bytes(), ceiling);
    calculator.reset();
    assert_eq!(root.used_bytes(), 0);
}

#[test]
fn a_refused_ceiling_leaves_the_event_unconsumed() {
    let ceiling = 7 * std::mem::size_of::<Timestamp>() + 256;
    let root = MemoryBudget::new(ceiling);
    let mut calculator = AdmittedPolygonCalculator::new(None, Arc::clone(&root));
    calculator.visit(&event(1, 200, 10, 20, EventOp::Put), |_| Ok(())).unwrap();
    let pressure = root.reserve(ceiling).unwrap();
    let second = event(1, 199, 10, 20, EventOp::Put);
    let refused = calculator.visit(&second, |_| -> rtdb_types::Result<()> { panic!("a refused event reached the visitor") }).unwrap_err();
    assert_eq!(refused.code, "rtdb/memory-budget-exceeded");
    drop(pressure);
    calculator.visit(&second, |view| {
        assert_eq!(view.ranges().collect::<Vec<_>>(), [(Timestamp(10), Timestamp(20), Timestamp(200))]);
        Ok(())
    }).unwrap().unwrap();
}

#[test]
fn borrowed_ranges_and_recency_match_owned_polygons_at_boundaries_and_erases() {
    let bounds = Some(TemporalBounds { valid_time: Interval::ALL, system_time: interval(0, 500) });
    let memory = MemoryBudget::new(4096);
    let mut legacy = PolygonCalculator::new(bounds);
    let mut admitted = AdmittedPolygonCalculator::new(bounds, Arc::clone(&memory));
    let filtered = event(1, 500, 0, 30, EventOp::Put);
    assert!(legacy.calculate(&filtered).unwrap().is_none());
    assert!(admitted.visit(&filtered, |_| Ok(())).unwrap().is_none());
    assert_eq!(memory.used_bytes(), 0);

    for header in [
        event(1, 499, 10, 20, EventOp::Delete),
        event(1, 499, 0, 30, EventOp::Put),
        event(1, 498, i64::MIN, i64::MIN, EventOp::Put),
        event(1, 497, i64::MAX, i64::MAX, EventOp::Put),
        event(1, 496, 10, 20, EventOp::Put),
        event(1, 495, i64::MIN, i64::MAX, EventOp::Erase),
        event(1, 495, 0, 30, EventOp::Put),
        event(2, 500, i64::MIN, i64::MAX, EventOp::Erase),
        event(2, 499, 0, 30, EventOp::Put),
        event(3, 490, 0, 30, EventOp::Put),
    ] {
        let expected = legacy.calculate(&header).unwrap().map(|polygon| {
            let ranges = polygon.valid_times().windows(2).zip(polygon.system_time_ceilings())
                .map(|(valid, &system_to)| (valid[0], valid[1], system_to)).collect::<Vec<_>>();
            (ranges, polygon.recency())
        });
        let actual = admitted.visit(&header, |view| Ok((view.ranges().collect::<Vec<_>>(), view.recency()))).unwrap();
        assert_eq!(actual, expected);
    }
    drop(admitted);
    assert_eq!(memory.used_bytes(), 0);
}
