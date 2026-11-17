use super::*;
use std::collections::BTreeMap;
use rtdb_columnar::MemoryBudget;
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{Anomaly,ObjectSort,Timestamp};
use tokio_util::sync::CancellationToken;
use crate::{CatalogSnapshot,DatabaseSnapshot,QueryContext,QueryOptions};

fn context(object_sort: ObjectSort,sort_memory_bytes: usize) -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    QueryContext::new(catalog,MemoryBudget::new(1 << 24),Timestamp(0),"UTC",QueryOptions { object_sort,sort_memory_bytes,..Default::default() },&CancellationToken::new()).unwrap()
}
fn records(context: &Arc<QueryContext>,keys: &[i64]) -> RecordStore {
    let mut buffer = RecordBuffer::new(Arc::clone(context)).unwrap();
    for key in keys { buffer.push(OwnedRow::new(vec![Value::I64(*key)],&context.memory).unwrap()).unwrap(); }
    buffer.finish().unwrap()
}
fn key(values: &[Value]) -> i64 { let Value::I64(value) = values[0] else { panic!("expected integer key") };value }

#[test]
fn configured_sorter_preserves_its_comparison_schedule_across_physical_spills() {
    for algorithm in [ObjectSort::TimSort,ObjectSort::LegacyMergeSort] {
        let expected = match algorithm {
            ObjectSort::TimSort => vec![(1,0),(2,1),(3,2),(4,3),(5,4),(6,5)],
            ObjectSort::LegacyMergeSort => vec![(0,1),(1,2),(3,4),(4,5),(5,6),(2,3)],
        };
        for threshold in [1,usize::MAX] {
            let context = context(algorithm,threshold);let baseline = context.memory.used_bytes();
            let records = records(&context,&[0,1,2,3,4,5,6]);let mut trace = Vec::new();
            assert_eq!(context.spill_bytes() > 0,threshold == 1);
            let indices = sort_records(&records,&context,|left,right| { trace.push((key(left),key(right)));Ok(key(left).cmp(&key(right))) }).unwrap();
            assert_eq!(indices.values,[0,1,2,3,4,5,6]);assert_eq!(trace,expected);
            drop(indices);drop(records);
            assert_eq!(context.memory.used_bytes(),baseline);assert_eq!(context.spill_bytes(),0);
        }
    }
}

#[test]
fn configured_sorter_propagates_first_failure_and_releases_scratch() {
    for algorithm in [ObjectSort::TimSort,ObjectSort::LegacyMergeSort] {
        for threshold in [1,usize::MAX] {
            let context = context(algorithm,threshold);let baseline = context.memory.used_bytes();
            let records = records(&context,&[4,1,3,2]);let retained = context.memory.used_bytes();let mut comparisons = 0;
            let error = sort_records(&records,&context,|_,_| { comparisons += 1;Err(Anomaly::fault("test/first-comparison","original failure")) }).err().unwrap();
            assert_eq!((comparisons,error.code.as_str(),error.message.as_str()),(1,"test/first-comparison","original failure"));
            assert_eq!(context.memory.used_bytes(),retained);drop(records);
            assert_eq!(context.memory.used_bytes(),baseline);assert_eq!(context.spill_bytes(),0);
        }
    }
}

#[test]
fn auxiliary_array_is_reserved_before_the_first_comparison() {
    for algorithm in [ObjectSort::TimSort,ObjectSort::LegacyMergeSort] {
        let context = context(algorithm,usize::MAX);let baseline = context.memory.used_bytes();
        let records = records(&context,&[3,1,2]);let retained = context.memory.used_bytes();
        let remaining = context.memory.limit_bytes() - retained;
        let blocker = context.memory.reserve(remaining).unwrap();let mut comparisons = 0;
        assert!(sort_records(&records,&context,|_,_| { comparisons += 1;Ok(Ordering::Equal) }).is_err());
        assert_eq!(comparisons,0);drop(blocker);assert_eq!(context.memory.used_bytes(),retained);drop(records);
        assert_eq!(context.memory.used_bytes(),baseline);
    }
}
