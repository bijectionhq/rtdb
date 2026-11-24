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

#[tokio::test]
async fn arrow_wave4_sort_matches_scalar_keys_for_decimals_in_memory_and_spill() {
    use std::collections::VecDeque;
    use async_trait::async_trait;
    use rtdb_plan::{ColumnId, Field};
    use rtdb_types::ScalarType;
    use crate::{context::OuterRows, parameters::BoundParameters};
    use crate::columnar::{BatchCursor, Column, ColumnBatch};

    struct Batches(VecDeque<ColumnBatch>);
    #[async_trait]
    impl BatchCursor for Batches {
        async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
            Ok(self.0.pop_front())
        }
    }

    for sort_memory_bytes in [usize::MAX, 1] {
        let context = context(ObjectSort::TimSort);
        let mut execution = Execution {
            parameters: Arc::new(BoundParameters::infer(vec![], &context.memory).unwrap()),
            outer: OuterRows::empty(&context.memory).unwrap(),
            context, bindings: None, analysis: None,
        };
        Arc::get_mut(&mut execution.context).unwrap().options.sort_memory_bytes = sort_memory_bytes;
        let context = &execution.context;
        let baseline = context.memory.used_bytes();
        let decimal = |unscaled: i128| Value::Decimal(rtdb_types::DecimalValue {
            unscaled: unscaled.to_be_bytes().to_vec(), precision: 38, scale: 1, bit_width: 128,
        });
        let ty = decimal(0).logical_type().maybe();
        let field = |data_type| Field { id: ColumnId(0), name: "value".into(), qualifier: None,
            qualified_only: false, unnamed: false, data_type };
        let key = field(ty.clone());
        let ordinal = Field { id: ColumnId(1), name: "ordinal".into(), ..field(LogicalType::Scalar(ScalarType::I64)) };
        let schema = Schema::new(vec![key.clone(), ordinal.clone()]).unwrap();
        let input = [
            decimal(90_071_992_547_409_920), Value::Null, decimal(-1),
            decimal(90_071_992_547_409_921), decimal(5), decimal(5),
        ];
        for descending in [false, true] {
            for nulls_first in [false, true] {
                let order = vec![SortExpr { expr: key.expression(), descending, nulls_first }];
                let plan = SortPlan { schema: schema.clone(), order: order.clone(), limit: u64::MAX, execution: execution.clone() };
                let mut expected = (0..input.len()).collect::<Vec<_>>();
                expected.sort_by(|&a, &b| plan.compare(&input[a..=a], &input[b..=b]).unwrap());
                let mut batches = VecDeque::new();
                for (chunk, values) in input.chunks(3).enumerate() {
                    let value_array = rtdb_columnar::copy_array(&ty, values).unwrap();
                    let ord_ty = &ordinal.data_type;
                    let ids = (chunk * 3..chunk * 3 + values.len()).map(|id| Value::I64(id as i64)).collect::<Vec<_>>();
                    let columns = [
                        (&key, value_array),
                        (&ordinal, rtdb_columnar::copy_array(ord_ty, &ids).unwrap()),
                    ].into_iter().map(|(field, array)| Column::Arrow {
                        field: Arc::new(rtdb_columnar::to_field(&field.name, &field.data_type).unwrap()),
                        array, logical: Some(field.data_type.clone()),
                    }).collect();
                    batches.push_back(ColumnBatch::settled(columns, values.len(), context.memory.reserve(4096).unwrap()).unwrap());
                }
                let mut sort = batches::BatchSort::new(Box::new(Batches(batches)), schema.clone(), order, execution.clone(), false, 0, u64::MAX).unwrap();
                let mut actual = Vec::new();
                while let Some(batch) = sort.next_batch().await.unwrap() {
                    for row in 0..batch.len {
                        let Value::I64(id) = batch.columns[1].value(row).unwrap() else { panic!("expected an ordinal") };
                        let id = id as usize;
                        assert_eq!(batch.columns[0].value(row).unwrap(), input[id]);
                        actual.push(id);
                    }
                }
                assert_eq!(actual, expected);
                drop(sort);
                assert_eq!(context.memory.used_bytes(), baseline);
                assert_eq!(context.spill_bytes(), 0);
            }
        }
    }
}
