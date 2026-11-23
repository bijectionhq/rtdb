use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::MemoryBudget;
use rtdb_plan::{BinaryOp, ColumnId, Expr, Field, OrdinalityScope, Relation, RelationNode, Schema};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType, Result, ScalarType, Timestamp, Value};
use tokio_util::sync::CancellationToken;
use crate::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions, context::{Execution, OuterRows}, columnar::{BoxBatchCursor, ColumnBatch}, operators::build_column_pages, parameters::BoundParameters, runtime_schema::EmittedRelation};

fn execution(batch_rows: usize) -> Execution {
    let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    let context = QueryContext::new(catalog, MemoryBudget::new(1 << 27), Timestamp(0), "UTC", QueryOptions { batch_rows, list_batch_rows: 3, ..Default::default() }, &CancellationToken::new()).unwrap();
    Execution { parameters: Arc::new(BoundParameters::infer(vec![], &context.memory).unwrap()), outer: OuterRows::empty(&context.memory).unwrap(), context, bindings: None, analysis: None }
}
fn list(expr: Expr) -> Arc<Relation> {
    let types = expr.data_type().legs().into_iter().map(|ty| match ty { LogicalType::List { element, .. } => *element, _ => LogicalType::Null }).collect::<Vec<_>>();
    let element = Field { id: ColumnId(0), name: "a".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::join_all(&types) };
    Arc::new(Relation::new(Schema::new(vec![element.clone()]).unwrap(), RelationNode::List { expr, element, page_rows: None }).unwrap())
}
fn series(start: i64, end: i64, step: i64, inclusive: bool) -> Expr {
    let args = vec![Expr::Literal(Value::I64(start)), Expr::Literal(Value::I64(end)), Expr::Literal(Value::I64(step)), Expr::Literal(Value::Bool(inclusive))];
    Expr::Call { name: "generate_series".into(), return_type: rtdb_plan::function_type_with_args("generate_series", &args).unwrap(), args }
}
async fn open(relation: Arc<Relation>, execution: &Execution) -> Result<BoxBatchCursor> {
    let runtime = EmittedRelation::derive(Arc::clone(&relation), &execution.context.catalog, execution.parameters.types(), &execution.context.memory)?;
    Ok(build_column_pages(runtime, execution.clone()).await?.unwrap())
}
fn rows(batch: &ColumnBatch) -> Vec<Vec<Value>> {
    (0..batch.len).map(|row| batch.columns.iter().map(|column| column.value(row).unwrap()).collect()).collect()
}
fn numbers(values: &[i64]) -> Vec<Vec<Value>> { values.iter().map(|value| vec![Value::I64(*value)]).collect() }

#[tokio::test]
async fn list_pages_preserve_the_reference_batch_boundaries_independently_of_client_batches() {
    for batch_rows in [1, 2048] {
        for (end, inclusive, expected) in [
            (3, false, vec![vec![1,2]]), (4, false, vec![vec![1,2,3]]),
            (7, false, vec![vec![1,2,3],vec![4,5,6]]), (6, false, vec![vec![1,2,3],vec![4,5]]),
            (1, false, vec![]), (1, true, vec![vec![1]]),
        ] {
            let execution = execution(batch_rows);
            let baseline = execution.context.memory.used_bytes();
            let mut cursor = open(list(series(1, end, 1, inclusive)), &execution).await.unwrap();
            let mut pages = Vec::new();
            while let Some(page) = cursor.next_batch().await.unwrap() { pages.push(rows(&page)); }
            assert_eq!(pages, expected.iter().map(|page| numbers(page)).collect::<Vec<_>>());
            drop(cursor);
            assert_eq!(execution.context.memory.used_bytes(), baseline);
        }
    }
}

#[tokio::test]
async fn page_ordinality_has_i64_cells_and_restarts_for_each_semantic_page() {
    let execution = execution(1);
    let input = list(series(1, 8, 1, false));
    let field = Field { id: ColumnId(1), name: "n".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) };
    let relation = Arc::new(input.with_ordinality(field, OrdinalityScope::Page).unwrap());
    let mut cursor = open(relation, &execution).await.unwrap();
    for expected in [vec![(1,1),(2,2),(3,3)],vec![(4,1),(5,2),(6,3)],vec![(7,1)]] {
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(rows(&page), expected.into_iter().map(|(a,n)| vec![Value::I64(a),Value::I64(n)]).collect::<Vec<_>>());
    }
    assert!(cursor.next_batch().await.unwrap().is_none());
}

#[tokio::test]
async fn relation_ordinality_includes_rows_in_pages_skipped_by_the_consumer() {
    let execution = execution(1);
    let input = list(series(1, 8, 1, false));
    let field = Field { id: ColumnId(1), name: "n".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) };
    let relation = Arc::new(input.with_ordinality(field, OrdinalityScope::Relation).unwrap());
    let mut cursor = open(relation, &execution).await.unwrap();
    assert_eq!(cursor.skip_complete_pages(4).unwrap(), 3);
    for expected in [&[4, 5, 6][..], &[7][..]] {
        let page = cursor.next_batch().await.unwrap().unwrap();
        let expected = expected.iter().map(|value| vec![Value::I64(*value), Value::I64(*value)]).collect::<Vec<_>>();
        assert_eq!(rows(&page), expected);
    }
    assert!(cursor.next_batch().await.unwrap().is_none());
}

fn division_by_zero() -> Expr { Expr::binary(BinaryOp::Divide, Expr::Literal(Value::I64(1)), Expr::Literal(Value::I64(0))) }

#[tokio::test]
async fn list_cells_are_demanded_only_in_complete_emitted_pages() {
    let execution = execution(1);
    let baseline = execution.context.memory.used_bytes();
    for good in [2,3] {
        let mut expressions = (1..=good).map(|value| Expr::Literal(Value::I64(value))).collect::<Vec<_>>();
        expressions.push(division_by_zero());
        let mut cursor = open(list(Expr::List(expressions)), &execution).await.unwrap();
        if good == 3 {
            let page = cursor.next_batch().await.unwrap().unwrap();
            assert_eq!(rows(&page), numbers(&[1,2,3]));
        }
        assert_eq!(cursor.next_batch().await.err().unwrap().code, "xtdb.expression/division-by-zero");
        drop(cursor);
        assert_eq!(execution.context.memory.used_bytes(), baseline);
    }
    let mut cursor = open(list(series(1, 2_000_000_000, 1, false)), &execution).await.unwrap();
    drop(cursor.next_batch().await.unwrap().unwrap());
    execution.context.cancellation.cancel();
    assert_eq!(cursor.next_batch().await.err().unwrap().code, "xtdb/query-cancelled");
    drop(cursor);
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

#[tokio::test]
async fn integer_list_fast_forward_preserves_full_and_partial_pages() {
    let execution = execution(1);
    let baseline = execution.context.memory.used_bytes();
    let mut cursor = open(list(series(8, 1, -1, true)), &execution).await.unwrap();

    assert_eq!(cursor.skip_complete_pages(4).unwrap(), 3);
    let page = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(rows(&page), numbers(&[5, 4, 3]));
    drop(page);
    assert_eq!(cursor.skip_complete_pages(1).unwrap(), 0);
    assert_eq!(cursor.skip_complete_pages(2).unwrap(), 2);
    assert!(cursor.next_batch().await.unwrap().is_none());

    drop(cursor);
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

#[tokio::test]
async fn fast_forward_preserves_errors_in_lists_and_computed_projections() {
    let execution = execution(1);
    let mut lazy = open(list(Expr::List(vec![division_by_zero()])), &execution).await.unwrap();
    assert_eq!(lazy.skip_complete_pages(3).unwrap(), 0);
    assert_eq!(lazy.next_batch().await.err().unwrap().code, "xtdb.expression/division-by-zero");

    let input = list(series(0, 9, 1, false));
    let column = input.schema().fields()[0].clone();
    let original = column.expression();
    let computed = Arc::new(input.project(vec![rtdb_plan::NamedExpr {
        field: column,
        expr: Expr::binary(BinaryOp::Divide, Expr::Literal(Value::I64(1)), original),
    }]).unwrap());
    let limited = Arc::new(computed.limit(Some(Expr::Literal(Value::I64(3))), Some(Expr::Literal(Value::I64(1)))).unwrap());
    let mut cursor = open(limited, &execution).await.unwrap();
    assert_eq!(cursor.next_batch().await.err().unwrap().code, "xtdb.expression/division-by-zero");
}

#[tokio::test]
async fn list_descriptor_errors_happen_while_the_cursor_opens() {
    let execution = execution(1);
    let args = vec![division_by_zero(), Expr::Literal(Value::I64(5)), Expr::Literal(Value::I64(1)), Expr::Literal(Value::Bool(false))];
    let expression = Expr::Call { name: "generate_series".into(), return_type: rtdb_plan::function_type_with_args("generate_series", &args).unwrap(), args };
    assert_eq!(open(list(expression), &execution).await.err().unwrap().code, "xtdb.expression/division-by-zero");
}

#[tokio::test]
async fn emitted_definition_survives_its_caller_and_output_survives_the_cursor() {
    let execution = execution(1);
    let baseline = execution.context.memory.used_bytes();
    let relation = list(series(1, 4, 1, false));
    let definition = Arc::downgrade(&relation);
    let emitted = EmittedRelation::derive(Arc::clone(&relation), &execution.context.catalog, execution.parameters.types(), &execution.context.memory).unwrap();
    drop(relation);
    assert!(definition.upgrade().is_some());
    let mut cursor = build_column_pages(emitted, execution.clone()).await.unwrap().unwrap();
    let page = cursor.next_batch().await.unwrap().unwrap();
    drop(cursor);
    assert!(definition.upgrade().is_none());
    assert_eq!(rows(&page), numbers(&[1,2,3]));
    drop(page);
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

#[test]
fn emissions_of_one_definition_retain_their_own_complete_parameter_fields() {
    let execution = execution(1);
    let baseline = execution.context.memory.used_bytes();
    let declared = LogicalType::Scalar(ScalarType::I64);
    let field = Field { id: ColumnId(0), name: "a".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: declared.clone() };
    let relation = Arc::new(Arc::new(Relation::unit()).project(vec![rtdb_plan::NamedExpr { field, expr: Expr::Parameter { index: 0, data_type: declared.clone() } }]).unwrap());
    {
        let narrow = LogicalType::Scalar(ScalarType::I32).maybe();
        let first = EmittedRelation::derive(Arc::clone(&relation), &execution.context.catalog, std::slice::from_ref(&narrow), &execution.context.memory).unwrap();
        let second = EmittedRelation::derive(Arc::clone(&relation), &execution.context.catalog, std::slice::from_ref(&declared), &execution.context.memory).unwrap();
        assert!(Arc::ptr_eq(first.relation(), second.relation()));
        assert_eq!(first.schema().fields()[0].data_type, narrow);
        assert_eq!(second.schema().fields()[0].data_type, declared);
        assert_eq!(relation.schema().fields()[0].data_type, declared);
        let output = first.output();
        drop(first);
        assert_eq!(output.schema().fields()[0].data_type, narrow);
    }
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

#[tokio::test]
async fn list_batches_keep_arrow_ordinals_after_skipping_semantic_pages() {
    let execution = execution(1);
    let input = list(series(1, 2_000_000_000, 1, false));
    let field = Field { id: ColumnId(1), name: "n".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) };
    let relation = Arc::new(input.with_ordinality(field, OrdinalityScope::Relation).unwrap());
    let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], &execution.context.memory).unwrap();
    let mut batches = crate::operators::build_columns(runtime, execution).await.unwrap().unwrap();
    assert_eq!(batches.skip_complete_pages(1_500_000_001).unwrap(), 1_500_000_000);
    let batch = batches.next_batch().await.unwrap().unwrap();
    assert_eq!(batch.len, 3);
    for column in &batch.columns {
        let crate::columnar::Column::Arrow { array, .. } = column else { panic!("expected Arrow series and ordinality"); };
        let values = array.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert_eq!(values.values().as_ref(), [1_500_000_001, 1_500_000_002, 1_500_000_003]);
    }
}
