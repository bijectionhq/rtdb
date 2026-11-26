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

#[tokio::test]
async fn sorted_output_keeps_equal_key_order_and_memory_and_spill_page_sizes() {
    use async_trait::async_trait;
    use rtdb_plan::{ColumnId, Field};
    use rtdb_types::ScalarType;
    use std::collections::VecDeque;
    use crate::{columnar::{BatchCursor, Column, ColumnBatch}, context::OuterRows, parameters::BoundParameters};

    struct Batches(VecDeque<ColumnBatch>);
    #[async_trait]
    impl BatchCursor for Batches {
        async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> { Ok(self.0.pop_front()) }
    }

    for (sort_memory_bytes, preserving_pages, page_sizes) in [
        (usize::MAX, false, &[2, 2, 2][..]),
        (1, false, &[2, 1, 2, 1][..]),
        (usize::MAX, true, &[6][..]),
        (1, true, &[6][..]),
    ] {
        let mut context = context(ObjectSort::TimSort);
        let options = &mut Arc::get_mut(&mut context).unwrap().options;
        options.batch_rows = 2;
        options.sort_memory_bytes = sort_memory_bytes;
        let execution = Execution {
            parameters: Arc::new(BoundParameters::infer(vec![], &context.memory).unwrap()),
            outer: OuterRows::empty(&context.memory).unwrap(),
            context: Arc::clone(&context), bindings: None, analysis: None,
        };
        let baseline = context.memory.used_bytes();
        let logical = LogicalType::Scalar(ScalarType::I64);
        let fields = ["key", "ordinal"].into_iter().enumerate().map(|(id, name)| Field {
            id: ColumnId(id as u32), name: name.into(), qualifier: None,
            qualified_only: false, unnamed: false, data_type: logical.clone(),
        }).collect();
        let schema = Schema::new(fields).unwrap();
        let order = vec![SortExpr { expr: schema.fields()[0].expression(), descending: false, nulls_first: false }];
        let make_page = |first: i64| {
            let columns = [vec![1; 3], (first..first + 3).collect()].into_iter().zip(schema.fields()).map(|(values, field)| Column::Arrow {
                array: Arc::new(arrow_array::Int64Array::from(values)),
                field: Arc::new(rtdb_columnar::to_field(&field.name, &logical).unwrap()),
                logical: Some(logical.clone()),
            }).collect();
            ColumnBatch::settled(columns, 3, context.memory.reserve(4096).unwrap()).unwrap()
        };
        let input = Batches(VecDeque::from([make_page(0), make_page(3)]));
        let sort = batches::BatchSort::new(Box::new(input), schema, order, execution.clone(), false, 0, u64::MAX).unwrap();
        let mut sort = if preserving_pages { sort.preserving_pages() } else { sort };
        let mut actual = Vec::new();
        let mut sizes = Vec::new();
        while let Some(page) = sort.next_batch().await.unwrap() {
            sizes.push(page.len);
            actual.extend((0..page.len).map(|row| page.columns[1].value(row).unwrap()));
        }
        assert_eq!(sizes.as_slice(), page_sizes);
        assert_eq!(actual, (0..6).map(Value::I64).collect::<Vec<_>>());
        assert_eq!(context.spill_peak_bytes() > 0, sort_memory_bytes == 1);
        drop(sort);
        assert_eq!(context.spill_bytes(), 0);
        assert_eq!(context.memory.used_bytes(), baseline);
    }
}

mod limited {
    use super::*;
    use std::collections::VecDeque;
    use async_trait::async_trait;
    use rtdb_plan::{ColumnId, Field};
    use rtdb_types::ScalarType;
    use crate::{columnar::{BatchCursor, Column, ColumnBatch}, context::OuterRows, parameters::BoundParameters};

    struct Batches(VecDeque<ColumnBatch>);
    #[async_trait]
    impl BatchCursor for Batches {
        async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> { Ok(self.0.pop_front()) }
    }

    fn execution(sort_memory_bytes: usize) -> Execution {
        let mut context = context(ObjectSort::TimSort);
        Arc::get_mut(&mut context).unwrap().options.sort_memory_bytes = sort_memory_bytes;
        Execution {
            parameters: Arc::new(BoundParameters::infer(vec![], &context.memory).unwrap()),
            outer: OuterRows::empty(&context.memory).unwrap(),
            context, bindings: None, analysis: None,
        }
    }

    fn field(id: u32, name: &str, data_type: LogicalType) -> Field {
        Field { id: ColumnId(id), name: name.into(), qualifier: None, qualified_only: false, unnamed: false, data_type }
    }

    /// Pages of (key, ordinal) rows, the ordinal being each row's input position.
    fn pages(execution: &Execution, key: &Field, keys: &[Value], page_rows: usize) -> Batches {
        let ordinal = field(1, "ordinal", LogicalType::Scalar(ScalarType::I64));
        Batches(keys.chunks(page_rows).enumerate().map(|(page, values)| {
            let ids = (0..values.len()).map(|row| Value::I64((page * page_rows + row) as i64)).collect::<Vec<_>>();
            let columns = [(key, values), (&ordinal, &ids[..])].into_iter().map(|(field, values)| Column::Arrow {
                field: Arc::new(rtdb_columnar::to_field(&field.name, &field.data_type).unwrap()),
                array: rtdb_columnar::copy_array(&field.data_type, values).unwrap(),
                logical: Some(field.data_type.clone()),
            }).collect();
            ColumnBatch::settled(columns, values.len(), execution.context.memory.reserve(1 << 16).unwrap()).unwrap()
        }).collect())
    }

    async fn sorted(execution: &Execution, key: Field, keys: &[Value], order: SortExpr, limit: u64) -> Result<Vec<usize>> {
        let ordinal = field(1, "ordinal", LogicalType::Scalar(ScalarType::I64));
        let schema = Schema::new(vec![key.clone(), ordinal]).unwrap();
        let input = pages(execution, &key, keys, 64);
        let mut sort = batches::BatchSort::new(Box::new(input), schema, vec![order], execution.clone(), false, 0, limit)?;
        let mut ids = Vec::new();
        while let Some(batch) = sort.next_batch().await? {
            for row in 0..batch.len {
                let Value::I64(id) = batch.columns[1].value(row)? else { panic!("expected an ordinal") };
                ids.push(id as usize);
            }
        }
        Ok(ids)
    }

    #[tokio::test]
    async fn a_limited_sort_returns_the_prefix_of_the_full_sort_with_ties_in_input_order() {
        let key = field(0, "key", LogicalType::Scalar(ScalarType::I64).maybe());
        let keys: Vec<Value> = (0..1000_i64).map(|i| if i % 37 == 0 { Value::Null } else { Value::I64((i * 7919) % 13) }).collect();
        for sort_memory_bytes in [usize::MAX, 1] {
            let execution = execution(sort_memory_bytes);
            for (descending, nulls_first) in [(false, false), (true, true), (true, false)] {
                let order = SortExpr { expr: key.expression(), descending, nulls_first };
                let full = sorted(&execution, key.clone(), &keys, order.clone(), u64::MAX).await.unwrap();
                for limit in [1, 10, 100] {
                    let limited = sorted(&execution, key.clone(), &keys, order.clone(), limit).await.unwrap();
                    assert_eq!(limited, full[..limit as usize], "desc {descending}, nulls first {nulls_first}, limit {limit}");
                }
            }
        }
    }

    #[tokio::test]
    async fn a_limited_sort_still_compares_intervals_with_months() {
        let key = field(0, "key", LogicalType::Scalar(ScalarType::Interval(rtdb_types::IntervalUnit::MonthDayNano)).maybe());
        let mut keys = vec![Value::Null; 1000];
        keys[500] = Value::IntervalMonthDayNano { months: 1, days: 0, nanoseconds: 0 };
        keys[900] = Value::IntervalMonthDayNano { months: 0, days: 2, nanoseconds: 0 };
        let order = SortExpr { expr: key.expression(), descending: false, nulls_first: true };
        let execution = execution(usize::MAX);
        let full = sorted(&execution, key.clone(), &keys, order.clone(), u64::MAX).await.unwrap_err();
        let limited = sorted(&execution, key, &keys, order, 5).await.unwrap_err();
        assert_eq!(limited.code, full.code);
        assert_eq!(limited.code, "xtdb.expression/cannot-compare-mdn-interval-with-months");
    }
}
