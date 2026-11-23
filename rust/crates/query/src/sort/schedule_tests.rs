use super::*;
use std::{collections::{BTreeMap, VecDeque}, sync::atomic::{AtomicUsize, Ordering as AtomicOrdering}};
use rtdb_columnar::MemoryBudget;
use rtdb_plan::{ColumnId, Expr, Field};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{Anomaly, ScalarType, TimeUnit, Timestamp};
use tokio_util::sync::CancellationToken;
use crate::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions, context::OuterRows, parameters::BoundParameters};

fn execution() -> Execution { execution_with_spill_limit(1024 * 1024 * 1024) }
fn execution_with_spill_limit(spill_limit_bytes: u64) -> Execution {
    let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    let context = QueryContext::new(catalog, MemoryBudget::new(1 << 29), Timestamp(0), "UTC", QueryOptions { sort_memory_bytes: usize::MAX, spill_limit_bytes, ..Default::default() }, &CancellationToken::new()).unwrap();
    Execution { parameters: Arc::new(BoundParameters::infer(vec![], &context.memory).unwrap()), outer: OuterRows::empty(&context.memory).unwrap(), context, bindings: None, analysis: None }
}
fn field(ty: LogicalType) -> Field { Field { id: ColumnId(0), name: "value".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: ty } }
struct Input { pages: VecDeque<Vec<Value>>, fail_after: bool, calls: Arc<AtomicUsize>, context: Arc<QueryContext> }
#[async_trait]
impl PageCursor for Input {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.calls.fetch_add(1, AtomicOrdering::Relaxed);
        let Some(values) = self.pages.pop_front() else {
            return if self.fail_after { Err(Anomaly::fault("test/input", "Later input failure")) } else { Ok(None) };
        };
        let mut page = PageBuilder::new(values.len(), &self.context.memory)?;
        for value in values { page.push(OwnedRow::new(vec![value], &self.context.memory)?)?; }
        Ok(Some(page.finish()))
    }
}
fn cursor(execution: &Execution, pages: Vec<Vec<Value>>, key: Expr, limit: u64, fail_after: bool) -> (SortCursor, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let input = Input { pages: pages.into(), fail_after, calls: Arc::clone(&calls), context: Arc::clone(&execution.context) };
    let schema = Schema::new(vec![field(LogicalType::Scalar(ScalarType::I64))]).unwrap();
    let order = vec![SortExpr { expr: key, descending: false, nulls_first: false }];
    (SortCursor::with_row_limit(Box::new(input), schema, order, limit, execution.clone()), calls)
}

#[tokio::test]
async fn comparator_factory_waits_for_the_complete_initial_accumulation() {
    let execution = execution();
    let baseline = execution.context.memory.used_bytes();
    let key = Expr::Literal(Value::list(vec![]));
    let (mut cursor, calls) = cursor(&execution, vec![vec![Value::I64(1)]], key, i64::MAX as u64, true);
    assert_eq!(cursor.next_page().await.err().unwrap().code, "test/input");
    assert_eq!(calls.load(AtomicOrdering::Relaxed), 2);
    assert!(cursor.next_page().await.unwrap().is_none());
    drop(cursor);
    assert_eq!(execution.context.memory.used_bytes(), baseline);
    assert_eq!(execution.context.spill_bytes(), 0);
}

#[tokio::test]
async fn a_threshold_crossing_top_k_reduction_precedes_the_next_input_pull() {
    let execution = execution();
    let baseline = execution.context.memory.used_bytes();
    let key = Expr::Literal(Value::list(vec![]));
    let (mut cursor, calls) = cursor(&execution, vec![vec![Value::I64(1); 102_401]], key, 1, true);
    assert_eq!(cursor.next_page().await.err().unwrap().code, "xtdb.expression/function-type-mismatch");
    assert_eq!(calls.load(AtomicOrdering::Relaxed), 1);
    assert!(cursor.next_page().await.unwrap().is_none());
    drop(cursor);
    assert_eq!(execution.context.memory.used_bytes(), baseline);
    assert_eq!(execution.context.spill_bytes(), 0);
}

#[tokio::test]
async fn zero_limit_preserves_a_small_accumulations_empty_callback() {
    let execution = execution();
    let baseline = execution.context.memory.used_bytes();
    for (pages, present) in [(vec![vec![Value::I64(1)]], true), (vec![vec![]], false), (vec![], false)] {
        let key = field(LogicalType::Scalar(ScalarType::I64)).expression();
        let (mut cursor, _) = cursor(&execution, pages, key, 0, false);
        let output = cursor.next_page().await.unwrap();
        assert_eq!(output.as_ref().map(OwnedPage::len), present.then_some(0));
        assert!(cursor.next_page().await.unwrap().is_none());
        drop(output);
        drop(cursor);
        assert_eq!(execution.context.memory.used_bytes(), baseline);
        assert_eq!(execution.context.spill_bytes(), 0);
    }
}

fn plan(execution: &Execution, ty: LogicalType, limit: u64) -> SortPlan {
    let field = field(ty);
    SortPlan { schema: Schema::new(vec![field.clone()]).unwrap(), order: vec![SortExpr { expr: field.expression(), descending: false, nulls_first: false }], limit, execution: execution.clone() }
}
fn run(plan: &SortPlan, values: Vec<Value>, labels: Vec<i64>) -> SortedRun {
    let mut accumulator = plan.accumulator().unwrap();
    for (value, label) in values.into_iter().zip(labels) {
        accumulator.keys.observe(std::slice::from_ref(&value), plan).unwrap();
        accumulator.records.push(OwnedRow::new(vec![value, Value::I64(label)], &plan.execution.context.memory).unwrap()).unwrap();
    }
    plan.write_run(accumulator).unwrap()
}

#[test]
fn four_way_merge_ties_follow_priority_queue_reinsertion_order() {
    let execution = execution();
    let baseline = execution.context.memory.used_bytes();
    {
        let plan = plan(&execution, LogicalType::Scalar(ScalarType::I64), i64::MAX as u64);
        let runs = (0..4).map(|index| run(&plan, vec![Value::I64(0), Value::I64(0)], vec![index * 10, index * 10 + 1])).collect::<Vec<_>>();
        let result = plan.merge(&runs).unwrap();
        let mut reader = result.file.reader(Arc::clone(&execution.context.memory)).unwrap();
        let mut labels = Vec::new();
        while let Some(row) = reader.next().unwrap() { labels.push(row.values[1].clone()); }
        assert_eq!(labels, [0, 30, 1, 31, 20, 10, 21, 11].map(Value::I64));
    }
    assert_eq!(execution.context.memory.used_bytes(), baseline);
    assert_eq!(execution.context.spill_bytes(), 0);
}

#[test]
fn merge_reinserts_the_final_selected_rows_successor_before_stopping() {
    let execution = execution();
    let baseline = execution.context.memory.used_bytes();
    {
        let ty = LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Second, timezone: None });
        let ty = ty.join(&LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Nanosecond, timezone: None }));
        let full = plan(&execution, ty.clone(), i64::MAX as u64);
        let timestamp = |value, unit| Value::Timestamp { value, unit, timezone: None };
        let make_run = |values: Vec<Value>| {
            let mut accumulator = full.accumulator().unwrap();
            for value in values {
                accumulator.keys.observe(std::slice::from_ref(&value), &full).unwrap();
                accumulator.records.push(OwnedRow::new(vec![value.clone(), value], &execution.context.memory).unwrap()).unwrap();
            }
            full.write_run(accumulator).unwrap()
        };
        let runs = vec![make_run(vec![timestamp(0, TimeUnit::Second), timestamp(i64::MAX, TimeUnit::Second)]), make_run(vec![timestamp(1, TimeUnit::Nanosecond)])];
        let limited = plan(&execution, ty, 1);
        let expected = rtdb_expr::ordered_compare_in_timezone(&timestamp(i64::MAX, TimeUnit::Second), &timestamp(1, TimeUnit::Nanosecond), false, "UTC").unwrap_err();
        let error = limited.merge(&runs).err().unwrap();
        assert_eq!((error.category, error.code, error.message), (expected.category, expected.code, expected.message));
    }
    assert_eq!(execution.context.memory.used_bytes(), baseline);
    assert_eq!(execution.context.spill_bytes(), 0);
}

#[test]
fn merge_heap_stops_at_the_first_comparator_failure() {
    let mut heap = MergeHeap::new();
    for run in 0..4 { heap.push(run, |_, _| Ok(Ordering::Equal)).unwrap(); }
    let mut calls = Vec::new();
    let failure = Anomaly::fault("test/comparator", "First comparator failure");
    let actual = heap.pop(|left, right| { calls.push((left, right)); Err(failure.clone()) }).unwrap_err();
    assert_eq!(actual, failure);
    assert_eq!(calls, [(1, 2)]);
}

#[tokio::test]
async fn external_sort_finishes_input_before_comparing_different_runs() {
    let execution = execution();
    let baseline = execution.context.memory.used_bytes();
    let timestamp = |value, unit| Value::Timestamp { value, unit, timezone: None };
    let seconds = timestamp(i64::MAX, TimeUnit::Second);
    let nanos = timestamp(1, TimeUnit::Nanosecond);
    assert!(rtdb_expr::ordered_compare_in_timezone(&seconds, &nanos, false, "UTC").is_err());
    let calls = Arc::new(AtomicUsize::new(0));
    let input = Input { pages: vec![vec![seconds.clone(); BLOCK_ROWS + 1], vec![nanos.clone(); BLOCK_ROWS + 1]].into(), fail_after: true, calls: Arc::clone(&calls), context: Arc::clone(&execution.context) };
    let field = field(seconds.logical_type().join(&nanos.logical_type()));
    let order = vec![SortExpr { expr: field.expression(), descending: false, nulls_first: false }];
    let mut cursor = SortCursor::from_pages(Box::new(input), Schema::new(vec![field]).unwrap(), order, execution.clone());
    assert_eq!(cursor.next_page().await.err().unwrap().code, "test/input");
    assert_eq!(calls.load(AtomicOrdering::Relaxed), 3);
    assert!(cursor.next_page().await.unwrap().is_none());
    drop(cursor);
    assert_eq!(execution.context.memory.used_bytes(), baseline);
    assert_eq!(execution.context.spill_bytes(), 0);
}

#[tokio::test]
async fn merge_pass_releases_consumed_groups_within_the_spill_quota() {
    let unit_bytes = {
        let execution = execution();
        let plan = plan(&execution, LogicalType::Scalar(ScalarType::I64), i64::MAX as u64);
        let run = run(&plan, vec![Value::I64(0)], vec![0]);
        let bytes = execution.context.spill_bytes();
        drop(run);
        assert_eq!(execution.context.spill_bytes(), 0);
        bytes
    };
    for quota_units in [11, 12] {
        let execution = execution_with_spill_limit(unit_bytes * quota_units);
        let baseline = execution.context.memory.used_bytes();
        {
            let plan = plan(&execution, LogicalType::Scalar(ScalarType::I64), i64::MAX as u64);
            let mut runs = RunList::new(&plan).unwrap();
            let mut files = Vec::new();
            for _ in 0..8 {
                let run = run(&plan, vec![Value::I64(0)], vec![0]);
                files.push(Arc::downgrade(&run.file));
                runs.push(run, &plan).unwrap();
            }
            assert_eq!(execution.context.spill_bytes(), unit_bytes * 8);
            let result = plan.merge_pass(runs).await;
            assert!(files.iter().all(|file| file.upgrade().is_none()));
            if quota_units == 12 {
                let next = result.unwrap();
                assert_eq!(next.values.len(), 2);
                assert_eq!(execution.context.spill_bytes(), unit_bytes * 8);
                assert_eq!(execution.context.spill_peak_bytes(), unit_bytes * 12);
                drop(next);
            } else {
                assert_eq!(result.err().unwrap().code, "rtdb.query/spill-limit");
            }
        }
        assert_eq!(execution.context.spill_bytes(), 0);
        assert_eq!(execution.context.memory.used_bytes(), baseline);
    }
}
