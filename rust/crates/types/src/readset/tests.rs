use super::*;

type Entry = ReadEntry<u8, Iid, Iid>;
type Change = Touch<u8, Iid, Iid>;

const PUBLIC: u8 = 0;
const ADMITTED: u8 = 1;
const HIDDEN: u8 = 2;
const RELATION: RelationId = RelationId(Iid([1; 16]));
const CELL: Cell = Cell {
    relation: RELATION,
    id: Iid([2; 16]),
    part: Part::Presence,
};
const PATH: Path = Path {
    relation: RELATION,
    id: Iid([3; 16]),
};
const GENERATION: PathGeneration = PathGeneration {
    ty: 2,
    protection: 1,
};

fn entry(target: ReadTarget<u8, Iid>) -> Entry {
    Entry {
        target,
        label: ADMITTED,
        until: None,
        exclusions: Arc::from([]),
    }
}

fn meets(entry: &Entry, touch: &Change) -> bool {
    intersects(entry, touch, &|from: &u8, to: &u8| from <= to)
}

fn cell(old: Option<u8>, new: Option<u8>, period: Option<Period>) -> Change {
    Change::Cell {
        cell: CELL,
        old,
        new,
        period,
    }
}

fn key(value: u8) -> PathKey {
    PathKey(vec![value])
}

fn key_change(old: Option<(u8, u8)>, new: Option<(u8, u8)>) -> Change {
    Change::Key {
        path: PATH,
        generation: GENERATION,
        old: old.map(|(label, value)| (label, key(value))),
        new: new.map(|(label, value)| (label, key(value))),
        period: None,
    }
}

fn path_read(span: Span) -> PathRead<u8> {
    PathRead {
        path: PATH,
        generation: GENERATION,
        partitions: Arc::from([PUBLIC]),
        span,
        valid_at: None,
    }
}

#[test]
fn point_matches_each_flowing_side_and_omits_hidden_changes() {
    let entry = entry(ReadTarget::Point {
        cell: CELL,
        partitions: Arc::from([PUBLIC]),
        valid_at: None,
    });
    assert!(meets(&entry, &cell(Some(ADMITTED), Some(HIDDEN), None)));
    assert!(meets(&entry, &cell(Some(HIDDEN), Some(ADMITTED), None)));
    assert!(!meets(&entry, &cell(Some(HIDDEN), Some(HIDDEN), None)));
}

#[test]
fn descending_cut_includes_last_key_and_preserves_upper_boundary() {
    let span = Span {
        lo: Bound::Included(key(1)),
        hi: Bound::Excluded(key(9)),
    };
    let scanned = span.through_last(Direction::Descending, key(5)).unwrap();
    let entry = entry(ReadTarget::Interval(path_read(scanned)));
    assert!(meets(&entry, &key_change(Some((ADMITTED, 5)), None)));
    assert!(meets(&entry, &key_change(None, Some((ADMITTED, 8)))));
    assert!(!meets(&entry, &key_change(None, Some((ADMITTED, 4)))));
    assert!(!meets(&entry, &key_change(None, Some((ADMITTED, 9)))));
    assert!(!meets(&entry, &key_change(None, Some((HIDDEN, 8)))));
}

#[test]
fn adjacent_valid_time_correction_respects_end_at_nanosecond_precision() {
    let earlier = Instant::new(-1, 999_999_999).unwrap();
    let at = Instant::new(0, 0).unwrap();
    let later = Instant::new(0, 1).unwrap();
    let entry = entry(ReadTarget::Point {
        cell: CELL,
        partitions: Arc::from([PUBLIC]),
        valid_at: Some(at),
    });
    assert!(!meets(
        &entry,
        &cell(
            Some(PUBLIC),
            None,
            Some(Period {
                from: earlier,
                to: Some(at)
            })
        )
    ));
    assert!(meets(
        &entry,
        &cell(
            Some(PUBLIC),
            None,
            Some(Period {
                from: at,
                to: Some(later)
            })
        )
    ));
    assert!(meets(
        &entry,
        &cell(
            Some(PUBLIC),
            None,
            Some(Period {
                from: earlier,
                to: None
            })
        )
    ));
}

#[test]
fn charged_bound_meets_only_an_increase_in_its_exact_partition() {
    let read = path_read(Span {
        lo: Bound::Unbounded,
        hi: Bound::Unbounded,
    });
    let entry = entry(ReadTarget::Bound {
        read,
        max: 4,
        charged: Some(ADMITTED),
    });
    assert!(meets(
        &entry,
        &key_change(Some((PUBLIC, 1)), Some((ADMITTED, 1)))
    ));
    assert!(!meets(&entry, &key_change(None, Some((PUBLIC, 1)))));
    assert!(!meets(&entry, &key_change(Some((ADMITTED, 1)), None)));
    assert!(!meets(
        &entry,
        &key_change(Some((ADMITTED, 1)), Some((ADMITTED, 2)))
    ));
}

#[test]
fn exclusions_invalidate_returned_tags_and_flowing_aggregate_partitions() {
    let tag = Iid([4; 16]);
    let mut point = entry(ReadTarget::Point {
        cell: CELL,
        partitions: Arc::from([PUBLIC]),
        valid_at: None,
    });
    point.exclusions = Arc::from([tag]);
    let hidden = Change::Exclusion {
        tag,
        partitions: Arc::from([(RELATION, HIDDEN)]),
    };
    let admitted = Change::Exclusion {
        tag,
        partitions: Arc::from([(RELATION, ADMITTED)]),
    };
    let read = path_read(Span {
        lo: Bound::Unbounded,
        hi: Bound::Unbounded,
    });
    let count = entry(ReadTarget::Count {
        read: read.clone(),
        observed: 1,
    });
    let bound = entry(ReadTarget::Bound {
        read,
        max: 4,
        charged: None,
    });
    assert!(meets(&point, &admitted));
    assert!(!meets(&count, &hidden));
    assert!(meets(&count, &admitted));
    assert!(!meets(&bound, &admitted));
}

#[test]
fn world_matching_keeps_the_entire_handle() {
    let entry = ReadEntry::<u8, [u8; 32], Iid> {
        target: ReadTarget::World { world: [0; 32] },
        label: PUBLIC,
        until: None,
        exclusions: Arc::from([]),
    };
    let mut other = [0; 32];
    other[31] = 1;
    let order = |from: &u8, to: &u8| from <= to;
    assert!(!intersects(&entry, &Touch::World { world: other }, &order));
    assert!(intersects(&entry, &Touch::World { world: [0; 32] }, &order));
}
