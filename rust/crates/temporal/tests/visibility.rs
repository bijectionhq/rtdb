use rtdb_temporal::{HistoryResolver, Rectangle, resolve_ordered, visibility};
use rtdb_types::{EventHeader, EventOp, Iid, Interval, MemoryBudget, TemporalBounds, Timestamp};
use std::sync::Arc;

struct Seeded(u64);
impl Seeded {
    fn below(&mut self, bound: i64) -> i64 { self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); ((self.0 >> 33) % bound as u64) as i64 }
}

/// A history of a few entities, IID ascending and system time descending, with puts, deletes and erases over overlapping valid times, some sharing a system time.
fn history(random: &mut Seeded) -> Vec<EventHeader> {
    let mut events = Vec::new();
    for iid in 0..1 + random.below(3) {
        let mut system = 100;
        for _ in 0..1 + random.below(12) {
            system -= random.below(3);
            let lower = random.below(40);
            let upper = if random.below(5) == 0 { i64::MAX } else { lower + 1 + random.below(30) };
            let op = match random.below(20) { 0 => EventOp::Erase, 1..=4 => EventOp::Delete, _ => EventOp::Put };
            events.push(EventHeader { commit_position: 0, iid: Iid([iid as u8; 16]), system_from: Timestamp(system), valid_time: Interval::from_micros(lower, upper).unwrap(), op });
        }
    }
    events
}

fn bounds(random: &mut Seeded) -> TemporalBounds {
    let mut dimension = |span: i64| {
        let lower = random.below(span);
        let upper = match random.below(3) { 0 => lower + 1, 1 => lower + 1 + random.below(span), _ => i64::MAX };
        Interval::from_micros(lower, upper).unwrap()
    };
    TemporalBounds { valid_time: dimension(60), system_time: dimension(110) }
}

#[test]
fn the_production_resolver_agrees_with_the_reference_trace() {
    let number = |text: &str| text.parse::<i64>().unwrap();
    let iid = |text: &str| Iid([u8::try_from(number(text)).unwrap(); 16]);
    let span = |from, to| Interval::from_micros(number(from), number(to)).unwrap();
    let mut events: Vec<(EventHeader, Option<i64>)> = Vec::new();
    let mut name = "";
    let mut observations = 0;
    let memory = MemoryBudget::new(1 << 20);
    let input = std::env::var_os("RTDB_TEMPORAL_TRACE")
        .map(|path| std::fs::read_to_string(path).unwrap())
        .unwrap_or_else(|| include_str!("../../../../verification/traces/temporal.tsv").to_string());
    for line in input.lines() {
        let fields: Vec<_> = line.split('\t').collect();
        let event = |system, key, valid_time, op| EventHeader {
            commit_position: 0,
            system_from: Timestamp(number(system)), iid: iid(key), valid_time, op,
        };
        match fields.as_slice() {
            ["case", label] => { events.clear(); name = label; }
            ["put", system, key, lower, upper, value] => {
                events.push((event(system, key, span(lower, upper), EventOp::Put), Some(number(value))));
            }
            ["delete", system, key, lower, upper] => {
                events.push((event(system, key, span(lower, upper), EventOp::Delete), None));
            }
            ["erase", system, key] => {
                events.push((event(system, key, Interval::ALL, EventOp::Erase), None));
            }
            ["expect", horizon, system, key, valid, expected] => {
                let (horizon, system, valid) = (number(horizon), number(system), number(valid));
                let bounds = TemporalBounds {
                    valid_time: Interval::from_micros(valid, valid + 1).unwrap(),
                    system_time: Interval::from_micros(system, system + 1).unwrap(),
                };
                let mut input: Vec<_> = events.iter().rev()
                    .filter(|(event, _)| event.system_from.0 <= horizon && event.iid == iid(key))
                    .collect();
                input.sort_by_key(|(event, _)| std::cmp::Reverse(event.system_from));
                let sources = || input.iter().map(|(event, value)| (event, *value));
                let expected: Vec<_> = (*expected != "none").then(|| number(expected)).into_iter().collect();
                let actual: Vec<_> = resolve_ordered(sources(), bounds, false).unwrap().into_iter()
                    .map(|row| row.source.expect("the resolver emitted a put")).collect();
                assert_eq!(actual, expected, "ordinary ceiling, {name}: {line}");
                let mut admitted = HistoryResolver::new_in(bounds, false, Arc::clone(&memory));
                let mut actual = Vec::new();
                for (event, value) in sources() {
                    let rows = admitted.resolve_in(event, value, &memory).unwrap();
                    actual.extend(rows.as_slice().iter().map(|row| row.source.expect("the resolver emitted a put")));
                }
                assert_eq!(actual, expected, "admitted ceiling, {name}: {line}");
                let actual: Vec<_> = visibility(sources()).unwrap().into_iter()
                    .filter(|(_, rectangle)| rectangle.meets(bounds))
                    .map(|(value, _)| value.expect("the resolver emitted a put")).collect();
                assert_eq!(actual, expected, "unbounded rectangles, {name}: {line}");
                observations += 1;
            }
            _ => panic!("{name}: malformed trace row {line}"),
        }
    }
    assert!(observations > 0);
}

#[test]
fn a_scan_at_any_bounds_sees_exactly_the_events_whose_rectangles_meet_them() {
    let mut random = Seeded(17);
    for case in 0..3_000 {
        let events = history(&mut random);
        let sources: Vec<_> = events.iter().enumerate().map(|(index, header)| (header, index)).collect();
        let rectangles = visibility(sources.iter().copied()).unwrap();
        for _ in 0..8 {
            let bounds = bounds(&mut random);
            let scanned: std::collections::BTreeSet<usize> = resolve_ordered(sources.iter().copied(), bounds, false).unwrap().into_iter().map(|row| row.source).collect();
            let indexed: std::collections::BTreeSet<usize> = rectangles.iter().filter(|(_, rectangle)| rectangle.meets(bounds)).map(|(index, _)| *index).collect();
            assert_eq!(indexed, scanned, "case {case} at {bounds:?} over {events:?}");
        }
    }
}

#[test]
fn at_any_point_each_entity_shows_the_one_event_whose_rectangle_holds_it() {
    let mut random = Seeded(29);
    for case in 0..3_000 {
        let events = history(&mut random);
        let sources: Vec<_> = events.iter().enumerate().map(|(index, header)| (header, index)).collect();
        let rectangles: Vec<(usize, Rectangle)> = visibility(sources.iter().copied()).unwrap();
        for _ in 0..8 {
            let (valid, system) = (random.below(60), random.below(110));
            let point = TemporalBounds { valid_time: Interval::from_micros(valid, valid + 1).unwrap(), system_time: Interval::from_micros(system, system + 1).unwrap() };
            let scanned: Vec<usize> = resolve_ordered(sources.iter().copied(), point, false).unwrap().into_iter().map(|row| row.source).collect();
            let mut indexed: Vec<usize> = rectangles.iter().filter(|(_, rectangle)| rectangle.meets(point)).map(|(index, _)| *index).collect();
            indexed.dedup();
            assert_eq!(indexed, scanned, "case {case} at ({valid}, {system}) over {events:?}");
            for iid in events.iter().map(|header| header.iid) {
                assert!(scanned.iter().filter(|index| events[**index].iid == iid).count() <= 1);
            }
        }
    }
}

#[test]
fn rectangles_never_overlap_within_an_entity() {
    let mut random = Seeded(5);
    for _ in 0..2_000 {
        let events = history(&mut random);
        let rectangles = visibility(events.iter().map(|header| (header, header.iid))).unwrap();
        for (index, (iid, first)) in rectangles.iter().enumerate() {
            for (other, second) in &rectangles[index + 1..] {
                if iid == other { assert!(!(first.valid_time.intersects(second.valid_time) && first.system_time.intersects(second.system_time)), "{first:?} and {second:?} of {events:?}"); }
            }
        }
    }
}
