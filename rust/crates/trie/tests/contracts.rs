use std::{cell::RefCell, cmp::Reverse, convert::Infallible, sync::Arc};

use rtdb_trie::{Bucketer, ColumnSummary, Comparison, EventMerge, EventRow, IidReader, MemoryHashTrie, PageSummary, Path, PersistedNode, PersistedTrie, PruningOptions, ScalarBounds, TemporalSummary, TrieConfig, TrieNode, select_pages};
use rtdb_types::{EventHeader, EventOp, Iid, Interval, Result, TemporalBounds, Timestamp, Value};

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
fn base_four_path_comparison_matches_unsigned_lexicographic_prefix_order() {
    let last = Iid([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let iids = [iid(0), last, iid(0x4000_0000), iid(0x8000_0000), Iid([u8::MAX; 16])];
    let digits = |iid: Iid| -> Vec<_> {
        iid.0.iter().flat_map(|&byte| [byte / 64, byte / 16 % 4, byte / 4 % 4, byte % 4]).collect()
    };
    let bucketer = Bucketer::DEFAULT;
    for left in iids {
        let left_digits = digits(left);
        for right in iids {
            let right_digits = digits(right);
            for depth in 0..=left_digits.len() {
                let path = Path::new(right_digits[..depth].to_vec(), bucketer).unwrap();
                assert_eq!(
                    bucketer.compare_path(left, &path).unwrap(),
                    left_digits[..depth].cmp(&right_digits[..depth]),
                );
            }
        }
    }
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
fn adding_a_range_stops_at_the_first_outside_iid_and_checks_overflow_before_lookup() {
    struct Reader<'a> {
        iids: &'a [Iid],
        visited: RefCell<Vec<u32>>,
    }

    impl IidReader for Reader<'_> {
        fn iid(&self, row: u32) -> Result<Iid> {
            self.visited.borrow_mut().push(row);
            self.iids.iid(row)
        }
    }

    let trie = MemoryHashTrie::with_root_path(TrieConfig::new(1, 1).unwrap(), Path::new(vec![0], Bucketer::DEFAULT).unwrap()).unwrap();
    let iids = [iid(0), iid(0x1000_0000), iid(0x4000_0000), iid(0x2000_0000)];
    let reader = Reader { iids: &iids, visited: RefCell::new(Vec::new()) };

    let unchanged = trie.add_range(&reader, u32::MAX, 0).unwrap();
    assert!(Arc::ptr_eq(trie.root(), unchanged.root()));
    let overflow = trie.add_range(&reader, u32::MAX, 1).unwrap_err();
    assert_eq!(overflow.code, "rtdb/trie-row-overflow");
    assert!(reader.visited.borrow().is_empty());

    let outside = trie.add_range(&reader, 1, 3).unwrap_err();
    assert_eq!(outside.code, "rtdb/trie-root-path");
    assert_eq!(reader.visited.borrow().as_slice(), &[1, 2]);
    assert!(trie.root().as_ref().path().as_slice() == [0]);
    assert!(trie.sorted_rows(&iids[..]).unwrap().is_empty());
}

#[test]
fn a_single_append_routes_with_one_iid_read_and_shares_other_branches() {
    struct Reader<'a> {
        iids: &'a [Iid],
        visited: RefCell<Vec<u32>>,
    }

    impl IidReader for Reader<'_> {
        fn iid(&self, row: u32) -> Result<Iid> {
            self.visited.borrow_mut().push(row);
            self.iids.iid(row)
        }
    }

    let iids = [0, 0x0100_0000, 0x4000_0000, 0x8000_0000, 0xc000_0000, 0x0100_0000].map(iid);
    let prefix = MemoryHashTrie::new(TrieConfig::new(4, 1).unwrap()).add_range(&iids[..], 0, 5).unwrap();
    let reader = Reader { iids: &iids, visited: RefCell::new(Vec::new()) };
    let next = prefix.add_range(&reader, 5, 1).unwrap();
    assert_eq!(reader.visited.into_inner(), [5]);

    let (TrieNode::Branch { children: before, .. }, TrieNode::Branch { children: after, .. }) =
        (prefix.root().as_ref(), next.root().as_ref()) else { panic!("Expected IIDs in distinct branches"); };
    for bucket in 1..4 {
        assert!(Arc::ptr_eq(before[bucket].as_ref().unwrap(), after[bucket].as_ref().unwrap()));
    }
    assert_eq!(prefix.sorted_rows(&iids[..]).unwrap(), [0, 1, 2, 3, 4]);
    let restored = MemoryHashTrie::from_persisted(next.persisted(&iids[..]).unwrap(), &iids[..]).unwrap();
    assert_eq!(restored.sorted_rows(&iids[..]).unwrap(), [0, 5, 1, 2, 3, 4]);
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
fn mixed_insertions_and_compaction_match_flat_history_for_every_iid() {
    let iids = [
        iid(0xc000_0000),
        iid(0),
        iid(0x8000_0000),
        iid(0),
        iid(0x4000_0000),
        Iid([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
        iid(0x8000_0000),
        iid(0x4000_0000),
    ];
    let assert_history = |trie: &MemoryHashTrie, rows: &[u32]| {
        let mut expected = rows.to_vec();
        expected.sort_by_key(|&row| (iids[row as usize].0, Reverse(row)));
        assert_eq!(trie.sorted_rows(&iids[..]).unwrap(), expected);

        for target in iids.into_iter().chain([iid(u32::MAX)]) {
            let mut node = Some(trie.root().as_ref());
            while let Some(TrieNode::Branch { path, children }) = node {
                let depth = path.len();
                let bucket = (target.0[depth / 4] >> (6 - 2 * (depth % 4))) & 3;
                node = children[usize::from(bucket)].as_deref();
            }
            let matching = |&row: &u32| iids[row as usize] == target;
            let actual: Vec<_> = match node {
                Some(TrieNode::Leaf(leaf)) => leaf.sorted_rows(&iids[..]).unwrap()
                    .iter().copied().filter(matching).collect(),
                None => Vec::new(),
                Some(TrieNode::Branch { .. }) => unreachable!(),
            };
            assert_eq!(actual, expected.iter().copied().filter(matching).collect::<Vec<_>>());
        }
    };

    for (log_limit, page_limit) in [(1, 1), (2, 1), (3, 2), (5, 8), (64, 1024)] {
        let empty = MemoryHashTrie::new(TrieConfig::new(log_limit, page_limit).unwrap());
        let prefix = empty.add(&iids[..], 0).unwrap().add_range(&iids[..], 1, 4).unwrap();
        let mixed = prefix.add_all(&iids[..], &[7, 2, 5, 2, 6]).unwrap();
        let history = [0, 1, 2, 3, 4, 7, 2, 5, 2, 6];
        let single = history.iter().fold(empty, |trie, &row| trie.add(&iids[..], row).unwrap());

        assert_history(&prefix, &history[..5]);
        assert_history(&mixed, &history);
        assert_history(&single, &history);
        assert_history(&mixed.compact_logs(&iids[..]).unwrap(), &history);
        let restored = MemoryHashTrie::from_persisted(mixed.persisted(&iids[..]).unwrap(), &iids[..]).unwrap();
        assert_history(&restored, &history);
    }
}

#[test]
fn leaf_order_matches_unsigned_iids_and_descending_row_indices() {
    let iids: Vec<_> = (0..16)
        .rev()
        .flat_map(|position| (u8::MIN..=u8::MAX).rev().map(move |byte| {
            let mut iid = [0; 16];
            iid[position] = byte;
            Iid(iid)
        }))
        .collect();
    let rows = iids.len() as u32;
    let mut trie = MemoryHashTrie::new(TrieConfig::new(64, iids.len()).unwrap());
    for start in (0..rows).step_by(53) {
        trie = trie.add_range(&iids, start, (rows - start).min(53)).unwrap();
    }
    let mut expected: Vec<_> = (0..rows).collect();
    expected.sort_unstable_by_key(|&row| (u128::from_be_bytes(iids[row as usize].0), Reverse(row)));
    assert_eq!(trie.leaves().len(), 1);
    assert_eq!(trie.sorted_rows(&iids).unwrap(), expected);
}

#[test]
fn failed_merge_lookup_can_be_retried_before_the_leaf_is_cached() {
    struct Reader<'a> {
        iids: &'a [Iid],
        rejected: Option<u32>,
        visited: RefCell<Vec<u32>>,
    }

    impl IidReader for Reader<'_> {
        fn iid(&self, row: u32) -> Result<Iid> {
            self.visited.borrow_mut().push(row);
            if self.rejected == Some(row) {
                return Err(rtdb_types::Anomaly::incorrect("test/iid-read", "IID lookup failed"));
            }
            self.iids.iid(row)
        }
    }

    let iids = [iid(1), iid(3), iid(2)];
    let persisted = PersistedTrie {
        log_limit: 4,
        page_limit: 4,
        root_path: vec![],
        root: PersistedNode::Leaf(vec![0, 1]),
    };
    let trie = MemoryHashTrie::from_persisted(persisted, &iids[..]).unwrap()
        .add(&iids[..], 2).unwrap();
    let mut reader = Reader { iids: &iids, rejected: Some(0), visited: RefCell::new(Vec::new()) };

    assert_eq!(trie.sorted_rows(&reader).unwrap_err().code, "test/iid-read");
    assert_eq!(reader.visited.get_mut().as_slice(), &[2, 0]);

    reader.rejected = None;
    assert_eq!(trie.sorted_rows(&reader).unwrap(), [0, 2, 1]);

    reader.rejected = Some(0);
    reader.visited.get_mut().clear();
    assert_eq!(trie.sorted_rows(&reader).unwrap(), [0, 2, 1]);
    assert!(reader.visited.get_mut().is_empty());
}

#[test]
fn pending_and_bulk_sorts_preserve_duplicate_rows_after_a_lookup_retry() {
    struct Reader<'a> {
        iids: &'a [Iid],
        reject_at: usize,
        visited: RefCell<Vec<u32>>,
    }

    impl IidReader for Reader<'_> {
        fn iid(&self, row: u32) -> Result<Iid> {
            let mut visited = self.visited.borrow_mut();
            visited.push(row);
            if visited.len() == self.reject_at {
                return Err(rtdb_types::Anomaly::incorrect("test/iid-read", "IID lookup failed"));
            }
            self.iids.iid(row)
        }
    }

    let iids = [iid(2), iid(0), iid(2), iid(1), iid(3)];
    let persisted = PersistedTrie {
        log_limit: 5,
        page_limit: 10,
        root_path: vec![],
        root: PersistedNode::Leaf(vec![3, 4]),
    };
    let base = MemoryHashTrie::from_persisted(persisted, &iids[..]).unwrap()
        .add_all(&iids[..], &[2, 0, 2]).unwrap();
    let mut reader = Reader { iids: &iids, reject_at: 2, visited: RefCell::new(Vec::new()) };

    assert_eq!(base.sorted_rows(&reader).unwrap_err().code, "test/iid-read");
    assert_eq!(reader.visited.get_mut().as_slice(), &[2, 0]);
    assert_eq!(base.sorted_rows(&iids[..]).unwrap(), [3, 2, 2, 0, 4]);

    reader.reject_at = 5;
    reader.visited.get_mut().clear();
    let inserted = [1, 2, 1];
    assert_eq!(base.add_all(&reader, &inserted).unwrap_err().code, "test/iid-read");
    assert_eq!(reader.visited.get_mut().as_slice(), &[1, 2, 1, 1, 2]);
    assert_eq!(base.sorted_rows(&iids[..]).unwrap(), [3, 2, 2, 0, 4]);
    let retried = base.add_all(&iids[..], &inserted).unwrap();
    assert_eq!(retried.sorted_rows(&iids[..]).unwrap(), [1, 1, 3, 2, 2, 2, 0, 4]);
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
fn one_level_and_recursive_leaf_splits_keep_path_order_and_repeated_rows() {
    let iids = [0, 1, 0, 3].map(|last| {
        let mut bytes = [0; 16];
        bytes[15] = last;
        Iid(bytes)
    });
    let rows = [2, 0, 3, 1, 3, 0];
    let path = Path::new(vec![0, 0], Bucketer::DEFAULT).unwrap();
    let one_level = MemoryHashTrie::with_root_path(TrieConfig::new(8, 2).unwrap(), path.clone())
        .unwrap().add_all(&iids[..], &rows).unwrap().compact_logs(&iids[..]).unwrap();
    let recursive = MemoryHashTrie::with_root_path(TrieConfig::new(2, 2).unwrap(), path)
        .unwrap().add_all(&iids[..], &rows).unwrap();

    assert_eq!(one_level.leaves().len(), 1);
    assert_eq!(one_level.leaves()[0].path().as_slice(), &[0, 0, 0]);
    assert_eq!(recursive.leaves().iter().map(|leaf| leaf.path().len()).max(), Some(64));
    assert_eq!(recursive.leaves().len(), 3);
    for trie in [&one_level, &recursive] {
        assert_eq!(trie.sorted_rows(&iids[..]).unwrap(), [2, 0, 0, 1, 3, 3]);
        for leaf in trie.leaves() {
            assert!(leaf.pending().is_empty());
            for row in leaf.data() {
                assert!(Bucketer::DEFAULT.contains(leaf.path(), iids[*row as usize]).unwrap());
            }
        }
    }
}

#[test]
fn a_read_failure_inside_a_recursive_split_leaves_the_original_trie_unchanged() {
    struct Reader {
        visited: RefCell<Vec<u32>>,
    }

    impl IidReader for Reader {
        fn iid(&self, row: u32) -> Result<Iid> {
            let mut visited = self.visited.borrow_mut();
            visited.push(row);
            if visited.len() == 13 {
                return Err(rtdb_types::Anomaly::incorrect("test/iid-read", "IID lookup failed"));
            }
            Ok(iid(row))
        }
    }

    let trie = MemoryHashTrie::new(TrieConfig::new(1, 1).unwrap());
    let reader = Reader { visited: RefCell::new(Vec::new()) };
    let rows = [3, 2, 1, 0];
    let error = trie.add_all(&reader, &rows).unwrap_err();
    assert_eq!(error.code, "test/iid-read");
    assert_eq!(reader.visited.into_inner(), [3, 2, 1, 0, 3, 2, 1, 0, 0, 1, 2, 3, 0]);

    let iids = [0, 1, 2, 3].map(iid);
    assert!(trie.sorted_rows(&iids[..]).unwrap().is_empty());
    let retried = trie.add_all(&iids[..], &rows).unwrap();
    assert_eq!(retried.sorted_rows(&iids[..]).unwrap(), [0, 1, 2, 3]);
}

#[test]
fn persisted_rows_outside_their_path_or_order_are_rejected() {
    let iids = vec![iid(0), iid(u32::MAX)];
    let bad_order = PersistedTrie { log_limit: 2, page_limit: 2, root_path: vec![], root: PersistedNode::Leaf(vec![1, 0]) };
    assert!(MemoryHashTrie::from_persisted(bad_order, &iids).is_err());
    let bad_path = PersistedTrie { log_limit: 2, page_limit: 2, root_path: vec![0], root: PersistedNode::Leaf(vec![1]) };
    assert!(MemoryHashTrie::from_persisted(bad_path, &iids).is_err());
}

#[test]
fn persisted_leaf_preserves_duplicates_and_rejects_an_inverted_final_pair() {
    let iids = vec![iid(1); 4];
    let persisted = |rows| PersistedTrie {
        log_limit: 4,
        page_limit: 4,
        root_path: vec![],
        root: PersistedNode::Leaf(rows),
    };
    let restored = MemoryHashTrie::from_persisted(persisted(vec![3, 3, 2]), &iids).unwrap();
    assert_eq!(restored.sorted_rows(&iids).unwrap(), [3, 3, 2]);
    let error = MemoryHashTrie::from_persisted(persisted(vec![3, 3, 2, 3]), &iids).unwrap_err();
    assert_eq!(error.code, "rtdb/trie-row-order");
}

#[test]
fn persisted_leaf_stops_on_an_iid_error_during_order_validation() {
    struct Reader {
        visited: RefCell<Vec<u32>>,
    }

    impl IidReader for Reader {
        fn iid(&self, row: u32) -> Result<Iid> {
            let mut visited = self.visited.borrow_mut();
            visited.push(row);
            if visited.len() == 5 {
                return Err(rtdb_types::Anomaly::incorrect("test/iid-read", "IID lookup failed"));
            }
            Ok(iid(row))
        }
    }

    let reader = Reader { visited: RefCell::new(Vec::new()) };
    let persisted = PersistedTrie {
        log_limit: 4,
        page_limit: 4,
        root_path: vec![],
        root: PersistedNode::Leaf(vec![0, 1, 2]),
    };
    let error = MemoryHashTrie::from_persisted(persisted, &reader).unwrap_err();
    assert_eq!(error.code, "test/iid-read");
    assert_eq!(reader.visited.into_inner(), [0, 1, 2, 0, 1]);
}

fn event(entity: u32, system: i64, source: &'static str) -> EventRow<&'static str> {
    EventRow { header: EventHeader { position: None, commit_position: 0, iid: iid(entity), system_from: Timestamp(system), valid_time: Interval::ALL, op: EventOp::Put }, source }
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
