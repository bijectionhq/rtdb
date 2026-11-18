use std::convert::Infallible;

use rtdb_trie::{Bucketer, ColumnSummary, Comparison, EventMerge, EventRow, MemoryHashTrie, PageSummary, Path, PersistedNode, PersistedTrie, PruningOptions, ScalarBounds, TemporalSummary, TrieConfig, select_pages};
use rtdb_types::{EventHeader, EventOp, Iid, Interval, TemporalBounds, Timestamp, Value};

fn iid(value: u32) -> Iid {
    let mut iid = [0; 16];
    iid[..4].copy_from_slice(&value.to_be_bytes());
    Iid(iid)
}

#[test]
fn unsigned_physical_extrema_never_prune_signed_execution_predicates() {
    for (minimum, maximum, probes) in [
        (Value::U8(0), Value::U8(255), vec![Value::I8(-1), Value::I8(127), Value::U8(255)]),
        (Value::U16(32767), Value::U16(32768), vec![Value::I16(i16::MIN), Value::I16(i16::MAX)]),
        (Value::U32(0), Value::U32(u32::MAX), vec![Value::I32(-1), Value::I32(0)]),
        (Value::U64(1 << 63), Value::U64(u64::MAX), vec![Value::I64(i64::MIN), Value::I64(-1), Value::U64(0)]),
    ] {
        let summary = ColumnSummary { bounds: Some(ScalarBounds { minimum, maximum }), null_count: Some(0), value_count: Some(2) };
        for probe in probes {
            for comparison in [Comparison::Equal, Comparison::NotEqual, Comparison::Less, Comparison::LessEqual, Comparison::Greater, Comparison::GreaterEqual] {
                assert!(summary.may_match(comparison, &probe));
            }
        }
        assert!(!summary.may_be_null());
    }
}

fn indices(trie: &MemoryHashTrie, iids: &[Iid]) -> Vec<u32> {
    let mut rows = trie.sorted_rows(iids).unwrap();
    rows.sort_unstable();
    rows
}

#[test]
fn iid_buckets_reconstruct_unsigned_bytes_at_every_width() {
    let iid = Iid([0xff, 0x80, 0x7f, 0x00, 0x55, 0xaa, 0x10, 0xfe, 0xab, 0xcd, 0x23, 0x45, 0x67, 0x89, 0x32, 0x10]);
    for width in [2, 4, 8] {
        let bucketer = Bucketer::new(width).unwrap();
        let path = Path::new((0..bucketer.max_depth()).map(|level| bucketer.bucket(iid, level).unwrap()).collect(), bucketer).unwrap();
        assert_eq!(bucketer.start_iid(&path).unwrap(), iid);
        assert!(bucketer.contains(&path, iid).unwrap());
        assert!(bucketer.bucket(iid, bucketer.max_depth()).is_err());
    }
}

#[test]
fn incrementing_last_path_has_no_successor() {
    let bucketer = Bucketer::DEFAULT;
    assert!(bucketer.next_path(&Path::new(vec![3, 3], bucketer).unwrap()).unwrap().is_none());
    assert_eq!(bucketer.next_path(&Path::new(vec![1, 3], bucketer).unwrap()).unwrap().unwrap().as_slice(), &[2, 0]);
    assert!(Path::new(vec![4], bucketer).is_err());
}

#[test]
fn rows_sort_by_iid_then_descending_row_index() {
    let iids = vec![iid(2), iid(1), iid(2), iid(1), iid(3)];
    let trie = MemoryHashTrie::default().add_range(&iids, 0, 5).unwrap();
    assert_eq!(trie.sorted_rows(&iids).unwrap(), vec![3, 1, 2, 0, 4]);
}

#[test]
fn borrowed_leaves_follow_depth_first_bucket_order_and_stop_on_error() {
    let iids = vec![iid(0xc000_0000), iid(0x1000_0000), iid(0x4000_0000), iid(0), iid(0x8000_0000)];
    let trie = MemoryHashTrie::new(TrieConfig::new(1, 1).unwrap()).add_range(&iids, 0, 5).unwrap();
    let expected: &[&[u8]] = &[&[0, 0], &[0, 1], &[1], &[2], &[3]];
    let mut visited = Vec::new();
    trie.try_visit_leaves::<Infallible>(|leaf| {
        visited.push(leaf);
        Ok(())
    }).unwrap();
    let paths: Vec<_> = visited.iter().map(|leaf| leaf.path().as_slice()).collect();
    assert_eq!(paths.as_slice(), expected);
    let collected = trie.leaves();
    assert_eq!(collected.len(), visited.len());
    assert!(collected.into_iter().zip(visited).all(|(left, right)| std::ptr::eq(left, right)));

    let mut visited = Vec::new();
    let result = trie.try_visit_leaves(|leaf| {
        visited.push(leaf.path().as_slice());
        if visited.len() == 2 { return Err("stop traversal"); }
        Ok(())
    });
    assert_eq!(result, Err("stop traversal"));
    assert_eq!(visited.as_slice(), &expected[..2]);
}

#[test]
fn independent_derivations_cannot_overwrite_each_others_pending_rows() {
    let iids = vec![iid(0), iid(1), iid(2), iid(3)];
    let base = MemoryHashTrie::default().add(&iids, 0).unwrap();
    let first = base.add_all(&iids, &[1, 2]).unwrap();
    let second = base.add(&iids, 3).unwrap();
    assert_eq!(first.sorted_rows(&iids).unwrap(), vec![0, 1, 2]);
    assert_eq!(second.sorted_rows(&iids).unwrap(), vec![0, 3]);
    assert_eq!(base.sorted_rows(&iids).unwrap(), vec![0]);
}

#[test]
fn add_range_preserves_the_same_multiset_as_folded_add_across_thresholds() {
    let iids: Vec<_> = (0..200).map(|i| iid((i * 67) % 139)).collect();
    for count in [0, 6, 8, 20, 200] {
        let empty = MemoryHashTrie::new(TrieConfig::new(8, 32).unwrap());
        let batch = empty.add_range(&iids, 0, count).unwrap();
        let individual = (0..count).fold(empty, |trie, row| trie.add(&iids, row).unwrap());
        assert_eq!(indices(&batch, &iids), indices(&individual, &iids));
        assert_eq!(batch.sorted_rows(&iids).unwrap(), individual.sorted_rows(&iids).unwrap());
    }
}

#[test]
fn batch_chunks_and_one_batch_contain_identical_rows() {
    let iids: Vec<_> = (0..150).map(|i| iid(i * 17)).collect();
    let empty = MemoryHashTrie::new(TrieConfig::new(8, 32).unwrap());
    let chunks = empty.add_range(&iids, 0, 7).unwrap().add_range(&iids, 7, 50).unwrap().add_range(&iids, 57, 93).unwrap();
    assert_eq!(chunks.sorted_rows(&iids).unwrap(), empty.add_range(&iids, 0, 150).unwrap().sorted_rows(&iids).unwrap());
}

#[test]
fn maximum_depth_leaf_retains_every_version_of_one_iid() {
    let iids = vec![iid(42); 50_000];
    let trie = MemoryHashTrie::new(TrieConfig::new(8, 32).unwrap()).add_range(&iids, 0, 50_000).unwrap();
    let leaves = trie.leaves();
    assert_eq!(leaves.len(), 1);
    assert_eq!(leaves[0].path().len(), 64);
    assert_eq!(Bucketer::DEFAULT.start_iid(leaves[0].path()).unwrap(), iid(42));
    assert_eq!(trie.sorted_rows(&iids).unwrap(), (0..50_000).rev().collect::<Vec<_>>());
}

#[test]
fn persisted_trie_contains_pending_rows_and_reconstructs_paths() {
    let iids = vec![iid(0), iid(u32::MAX), iid(1), iid(u32::MAX)];
    let trie = MemoryHashTrie::new(TrieConfig::new(2, 2).unwrap()).add_range(&iids, 0, 4).unwrap();
    let decoded = MemoryHashTrie::from_persisted(trie.persisted(&iids).unwrap(), &iids).unwrap();
    assert_eq!(decoded.sorted_rows(&iids).unwrap(), vec![0, 2, 3, 1]);
    assert!(decoded.leaves().iter().all(|leaf| leaf.pending().is_empty()));
    for leaf in decoded.leaves() { for row in leaf.data() { assert!(Bucketer::DEFAULT.contains(leaf.path(), iids[*row as usize]).unwrap()); } }
}

#[test]
fn persisted_rows_outside_their_path_or_order_are_rejected() {
    let iids = vec![iid(0), iid(u32::MAX)];
    let bad_order = PersistedTrie { log_limit: 2, page_limit: 2, root_path: vec![], root: PersistedNode::Leaf(vec![1, 0]) };
    assert!(MemoryHashTrie::from_persisted(bad_order, &iids).is_err());
    let bad_path = PersistedTrie { log_limit: 2, page_limit: 2, root_path: vec![0], root: PersistedNode::Leaf(vec![1]) };
    assert!(MemoryHashTrie::from_persisted(bad_path, &iids).is_err());
}

fn event(entity: u32, system: i64, source: &'static str) -> EventRow<&'static str> {
    EventRow { header: EventHeader { iid: iid(entity), system_from: Timestamp(system), valid_time: Interval::ALL, op: EventOp::Put }, source }
}

#[test]
fn segment_merge_orders_histories_and_retains_equal_time_precedence() {
    let first = vec![event(1, 3, "a3"), event(1, 2, "a2-first"), event(2, 9, "b9")];
    let second = vec![event(1, 2, "a2-second"), event(1, 1, "a1"), event(3, 1, "c1")];
    let merged = EventMerge::new(vec![&first, &second]).unwrap().map(|row| row.source).collect::<Vec<_>>();
    assert_eq!(merged, vec!["a3", "a2-first", "a2-second", "a1", "b9", "c1"]);
    assert!(EventMerge::new(vec![&[event(1, 1, "older"), event(1, 2, "newer")]]).is_err());
}

fn page(index: u32, valid: (i64, i64), system: (i64, i64)) -> PageSummary {
    PageSummary { page_index: index, temporal: Some(TemporalSummary {
        min_valid_from: Timestamp(valid.0), max_valid_to: Timestamp(valid.1), min_system_from: Timestamp(system.0), max_system_from: Timestamp(system.1),
    }), recency: Some(Timestamp::MAX), iid_range: None }
}

fn options(valid: (i64, i64), system: (i64, i64)) -> PruningOptions {
    PruningOptions { bounds: TemporalBounds { valid_time: Interval::from_micros(valid.0, valid.1).unwrap(), system_time: Interval::from_micros(system.0, system.1).unwrap() }, projects_temporal_columns: false, clamp_valid_time: false, bypass_temporal_pruning: false, iid: None }
}

#[test]
fn later_correction_failing_value_predicate_still_supersedes_candidate() {
    let pages = vec![page(0, (0, 100), (1, 1)), page(1, (10, 20), (2, 2))];
    assert_eq!(select_pages(&pages, options((15, 16), (2, 3)), |page| page.page_index == 0).iter().map(|p| p.page_index).collect::<Vec<_>>(), vec![0, 1]);
}

#[test]
fn erase_page_survives_a_query_window_before_the_erase() {
    let pages = vec![page(0, (0, 100), (1, 1)), page(1, (i64::MIN, i64::MAX), (20, 20))];
    assert_eq!(select_pages(&pages, options((5, 6), (1, 2)), |page| page.page_index == 0).iter().map(|p| p.page_index).collect::<Vec<_>>(), vec![0, 1]);
}

#[test]
fn projected_temporal_bounds_retain_disjoint_valid_time_constraints() {
    let pages = vec![page(0, (0, 100), (1, 1)), page(1, (10, 20), (2, 2))];
    let mut opts = options((50, 51), (3, 4));
    assert_eq!(select_pages(&pages, opts, |p| p.page_index == 0).len(), 1);
    opts.projects_temporal_columns = true;
    assert_eq!(select_pages(&pages, opts, |p| p.page_index == 0).len(), 2);
    opts.clamp_valid_time = true;
    assert_eq!(select_pages(&pages, opts, |p| p.page_index == 0).len(), 1);
}

#[test]
fn missing_metadata_retains_pages_and_bypass_keeps_iid_filter() {
    let pages = vec![page(0, (0, 10), (1, 1)), PageSummary { page_index: 1, temporal: None, recency: None, iid_range: None }];
    assert_eq!(select_pages(&pages, options((100, 101), (100, 101)), |_| false).len(), 2);
    let mut pages = pages;
    pages[0].iid_range = Some((iid(1), iid(2)));
    let mut opts = options((100, 101), (100, 101));
    opts.bypass_temporal_pruning = true;
    opts.iid = Some(iid(3));
    assert_eq!(select_pages(&pages, opts, |_| false).iter().map(|p| p.page_index).collect::<Vec<_>>(), vec![1]);
}

#[test]
fn unknown_or_incomparable_statistics_cannot_reject_a_page() {
    let stats = ColumnSummary { bounds: Some(ScalarBounds { minimum: Value::F64(f64::NAN), maximum: Value::F64(3.0) }), null_count: None, value_count: Some(5) };
    assert!(stats.may_match(Comparison::Greater, &Value::F64(10.0)));
    assert!(stats.may_match(Comparison::Equal, &Value::I64(1)));
    assert!(stats.may_be_null());
}
