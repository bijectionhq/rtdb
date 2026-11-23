use super::*;
use std::collections::BTreeMap;
use rtdb_columnar::MemoryBudget;
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{Anomaly,ObjectSort,Timestamp};
use tokio_util::sync::CancellationToken;
use crate::{CatalogSnapshot,DatabaseSnapshot,QueryContext,QueryOptions};

fn context(object_sort: ObjectSort) -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    QueryContext::new(catalog,MemoryBudget::new(1 << 24),Timestamp(0),"UTC",QueryOptions { object_sort,..Default::default() },&CancellationToken::new()).unwrap()
}

#[test]
fn ordinal_sort_preserves_each_configured_comparison_schedule() {
    for (algorithm, expected) in [
        (ObjectSort::TimSort, [(1, 0), (2, 1), (3, 2), (4, 3), (5, 4), (6, 5)]),
        (ObjectSort::LegacyMergeSort, [(0, 1), (1, 2), (3, 4), (4, 5), (5, 6), (2, 3)]),
    ] {
        let context = context(algorithm);
        let mut trace = Vec::new();
        let indices = sort_ordinals(7, &context, |left, right| {
            trace.push((left, right));
            Ok(left.cmp(&right))
        }).unwrap();
        assert_eq!(indices.values, [0, 1, 2, 3, 4, 5, 6]);
        assert_eq!(trace, expected);
        drop(indices);
        assert_eq!(context.memory.used_bytes(), 0);
    }
}

#[test]
fn ordinal_sort_propagates_the_first_comparator_error_and_releases_scratch() {
    for algorithm in [ObjectSort::TimSort, ObjectSort::LegacyMergeSort] {
        let context = context(algorithm);
        let mut comparisons = 0;
        let error = sort_ordinals(4, &context, |_, _| {
            comparisons += 1;
            Err(Anomaly::fault("test/first-comparison", "original failure"))
        }).err().unwrap();
        assert_eq!((comparisons, error.code.as_str(), error.message.as_deref()), (1, "test/first-comparison", Some("original failure")));
        assert_eq!(context.memory.used_bytes(), 0);
    }
}

#[test]
fn ordinal_sort_reserves_scratch_before_the_first_comparison() {
    for algorithm in [ObjectSort::TimSort, ObjectSort::LegacyMergeSort] {
        let context = context(algorithm);
        let blocker = context.memory.reserve(context.memory.limit_bytes()).unwrap();
        let error = sort_ordinals(3, &context, |_, _| panic!("sort compared before reserving its scratch")).err().unwrap();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        drop(blocker);
        assert_eq!(context.memory.used_bytes(), 0);
    }
}
