use super::*;
use crate::columnar::{BoxBatchCursor, ColumnBatch};
use rtdb_columnar::MemoryBudget;
use rtdb_plan::{BinaryOp,ColumnId,Field};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{LogicalType,ScalarType,Timestamp};
use tokio_util::sync::CancellationToken;
use crate::{CatalogSnapshot,DatabaseSnapshot,QueryContext,QueryOptions,context::OuterRows,parameters::BoundParameters,runtime_schema::EmittedRelation};

fn execution(batch_rows: usize) -> Execution {
    execution_threshold(batch_rows,100_000)
}
fn execution_threshold(batch_rows: usize,join_build_rows: i64) -> Execution {
    let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    let context = QueryContext::new(catalog,MemoryBudget::new(1 << 26),Timestamp(0),"UTC",QueryOptions { batch_rows,join_build_rows,..Default::default() },&CancellationToken::new()).unwrap();
    Execution { parameters: Arc::new(BoundParameters::infer(vec![],&context.memory).unwrap()),outer: OuterRows::empty(&context.memory).unwrap(),context,bindings: None,analysis: None }
}
fn field(id: u32,data_type: LogicalType) -> Field { Field { id: ColumnId(id),name: format!("c{id}"),qualifier: None,qualified_only: false,unnamed: false,data_type } }
fn column(id: u32) -> Expr { Expr::column(ColumnId(id),LogicalType::Scalar(ScalarType::I64)) }
fn literal(value: i64) -> Expr { Expr::Literal(Value::I64(value)) }
fn table(id: u32,values: &[i64]) -> Arc<Relation> {
    Arc::new(Relation::new(Schema::new(vec![field(id,LogicalType::Scalar(ScalarType::I64))]).unwrap(),RelationNode::Values { rows: values.iter().map(|value|vec![literal(*value)]).collect() }).unwrap())
}
fn union(left: Arc<Relation>,right: Arc<Relation>) -> Arc<Relation> {
    Arc::new(Relation::new(left.schema().clone(),RelationNode::Set { left,right,operator: SetOperator::Union,all: true }).unwrap())
}
fn mark(input: Arc<Relation>,dependent: Arc<Relation>,comparison: Expr,correlated: bool) -> Arc<Relation> {
    let output = field(2,LogicalType::Scalar(ScalarType::Bool).maybe());
    let schema = input.schema().concat(&Schema::new(vec![output.clone()]).unwrap()).unwrap();
    Arc::new(Relation::new(schema,RelationNode::Mark { input,dependent,arguments: if correlated { vec![(0,column(0))] } else { vec![] },condition: None,comparison,output }).unwrap())
}
async fn open(relation: Arc<Relation>,execution: &Execution) -> BoxBatchCursor {
    let runtime = EmittedRelation::derive(Arc::clone(&relation),&execution.context.catalog,execution.parameters.types(),&execution.context.memory).unwrap();
    build_column_pages(runtime,execution.clone()).await.unwrap().unwrap()
}
fn inverse_equals_one(column: Expr) -> Expr { Expr::binary(BinaryOp::Eq,Expr::binary(BinaryOp::Divide,literal(1),column),literal(1)) }

#[tokio::test]
async fn correlated_mark_adds_a_nullable_boolean_to_the_original_arrow_input() {
    let execution = execution(1);
    let relation = mark(table(0, &[1, 2]), table(1, &[1]), Expr::binary(BinaryOp::Eq, column(0), column(1)), true);
    let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], &execution.context.memory).unwrap();
    let mut cursor = crate::operators::build_columns(runtime, execution).await.unwrap().expect("Expected an Arrow mark");
    let batch = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(batch.len, 2);
    assert!(batch.columns.iter().all(|column| matches!(column, crate::columnar::Column::Arrow { .. })));
    assert_eq!(batch.columns[1].value(0).unwrap(), Value::Bool(true));
    assert_eq!(batch.columns[1].value(1).unwrap(), Value::Bool(false));
    assert!(cursor.next_batch().await.unwrap().is_none());
}

#[tokio::test]
async fn correlated_mark_admits_argument_layout_only_when_an_outer_row_is_reached() {
    let execution = execution(2);
    let relation = |rows: &[i64]| {
        let input = table(0, rows);
        let output = field(2, LogicalType::Scalar(ScalarType::Bool).maybe());
        let schema = input.schema().concat(&Schema::new(vec![output.clone()]).unwrap()).unwrap();
        Arc::new(Relation::new(schema, RelationNode::Mark {
            input,
            dependent: table(1, &[1]),
            arguments: vec![(usize::MAX, column(0))],
            condition: None,
            comparison: Expr::binary(BinaryOp::Eq, column(0), column(1)),
            output,
        }).unwrap())
    };
    let mut empty = open(relation(&[]), &execution).await;
    while let Some(page) = empty.next_batch().await.unwrap() {
        assert_eq!(page.len, 0);
    }

    let mut reached = open(relation(&[1]), &execution).await;
    let error = reached.next_batch().await.err().unwrap();
    assert_eq!(error.code, "rtdb.query/parameter-count");
}

#[tokio::test]
async fn uncorrelated_arrow_membership_handles_null_and_empty_builds() {
    let execution = execution(2);
    let schema = |id| Schema::new(vec![field(id, LogicalType::Scalar(ScalarType::I64).maybe())]).unwrap();
    let values = |id, values: &[Value]| Arc::new(Relation::new(schema(id), RelationNode::Values {
        rows: values.iter().cloned().map(|value| vec![Expr::Literal(value)]).collect(),
    }).unwrap());
    let input = [Value::I64(1), Value::I64(4), Value::Null];
    for (candidates, expected) in [
        (vec![Value::I64(1), Value::Null], [Value::Bool(true), Value::Null, Value::Null]),
        (vec![Value::I64(1)], [Value::Bool(true), Value::Bool(false), Value::Null]),
        (vec![], [Value::Bool(false), Value::Bool(false), Value::Bool(false)]),
    ] {
        let relation = mark(values(0, &input), values(1, &candidates),
            Expr::binary(BinaryOp::Eq, schema(0).fields()[0].expression(), schema(1).fields()[0].expression()), false);
        let runtime = EmittedRelation::derive(Arc::clone(&relation), &execution.context.catalog, &[], &execution.context.memory).unwrap();
        let mut columns = crate::operators::build_columns(runtime, execution.clone()).await.unwrap().expect("Expected Arrow membership");
        let batch = columns.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.len, 3);
        assert!(batch.columns.iter().all(|column| matches!(column, crate::columnar::Column::Arrow { .. })));
        assert_eq!((0..3).map(|row| batch.columns[1].value(row).unwrap()).collect::<Vec<_>>(), expected);
        assert!(columns.next_batch().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn membership_spill_keeps_probe_payloads_and_global_null_facts() {
    let execution = execution_threshold(1, 1);
    let data_type = LogicalType::Scalar(ScalarType::I64).maybe();
    let candidate = field(1, data_type);
    let dependent = Arc::new(Relation::new(Schema::new(vec![candidate.clone()]).unwrap(), RelationNode::Values {
        rows: vec![vec![literal(1)], vec![literal(2)], vec![Expr::Literal(Value::Null)]],
    }).unwrap());
    let relation = mark(table(0, &[1, 3, 2]), dependent,
        Expr::binary(BinaryOp::Eq, column(0), candidate.expression()), false);
    let mut pages = open(Arc::clone(&relation), &execution).await;
    let mut expected = Vec::new();
    while let Some(page) = pages.next_batch().await.unwrap() {
        expected.extend(page_rows(page));
    }
    drop(pages);
    let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], &execution.context.memory).unwrap();
    let mut columns = crate::operators::build_columns(runtime, execution.clone()).await.unwrap().expect("Expected Arrow membership spill");
    let mut actual = Vec::new();
    while let Some(batch) = columns.next_batch().await.unwrap() {
        assert!(execution.context.spill_bytes() > 0);
        assert!(batch.columns.iter().all(|column| !matches!(column, crate::columnar::Column::Values(_))));
        actual.extend((0..batch.len).map(|row| batch.columns.iter().map(|column| column.value(row).unwrap()).collect::<Vec<_>>()));
    }
    assert_eq!(actual, expected);
    assert!(actual.contains(&vec![Value::I64(3), Value::Null]));
    assert_eq!(actual.len(), 3);
    assert_eq!(execution.context.spill_bytes(), 0);
}

#[tokio::test]
async fn scalar_membership_marks_the_probe_key_at_its_payload_position() {
    let ty = LogicalType::Scalar(ScalarType::I64).maybe();
    let probe_key = field(0, ty.clone());
    let build_key = field(1, ty.clone());
    let values = |payload, key: &Field, rows: &[(i64, Value)]| {
        Arc::new(Relation::new(
            Schema::new(vec![field(payload, ty.clone()), key.clone()]).unwrap(),
            RelationNode::Values {
                rows: rows.iter().map(|(payload, key)| vec![literal(*payload), Expr::Literal(key.clone())]).collect(),
            },
        ).unwrap())
    };
    let input = values(3, &probe_key, &[(7, Value::I64(1)), (1, Value::I64(3)), (9, Value::Null)]);
    let dependent = values(4, &build_key, &[(3, Value::I64(1)), (9, Value::Null)]);
    let comparison = Expr::binary(BinaryOp::Eq, probe_key.expression(), build_key.expression());
    let relation = mark(input, dependent, comparison, false);
    for threshold in [0, 10] {
        let execution = execution_threshold(32, threshold);
        let runtime = EmittedRelation::derive(Arc::clone(&relation), &execution.context.catalog, &[], &execution.context.memory).unwrap();
        let mut cursor = crate::operators::build_columns(runtime, execution.clone()).await.unwrap().unwrap();
        let mut rows = Vec::new();
        while let Some(page) = cursor.next_batch().await.unwrap() {
            rows.extend(page_rows(page));
        }
        assert_eq!(execution.context.spill_peak_bytes() > 0, threshold == 0);
        assert_eq!(rows.len(), 3);
        assert!(rows.contains(&vec![Value::I64(7), Value::I64(1), Value::Bool(true)]));
        assert!(rows.contains(&vec![Value::I64(1), Value::I64(3), Value::Null]));
        assert!(rows.contains(&vec![Value::I64(9), Value::Null, Value::Null]));
    }
}

#[tokio::test]
async fn correlated_scalar_apply_keeps_computed_dependent_vectors() {
    use rtdb_plan::{ApplyKind, NamedExpr};
    let execution = execution(2);
    let input = table(0, &[1, 2]);
    let value = field(2, LogicalType::Scalar(ScalarType::I64));
    let dependent = Arc::new(table(1, &[10]).project(vec![NamedExpr { field: value.clone(), expr: Expr::binary(
        BinaryOp::Add, column(1), Expr::Parameter { index: 0, data_type: LogicalType::Scalar(ScalarType::I64) },
    ) }]).unwrap());
    let output = field(3, value.data_type.maybe());
    let schema = input.schema().concat(&Schema::new(vec![output.clone()]).unwrap()).unwrap();
    let relation = Arc::new(Relation::new(schema, RelationNode::Apply {
        input, dependent, arguments: vec![(0, column(0))], kind: ApplyKind::Scalar, output: Some(output),
    }).unwrap());
    let runtime = EmittedRelation::derive(Arc::clone(&relation), &execution.context.catalog, &[], &execution.context.memory).unwrap();
    let mut columns = crate::operators::build_columns(runtime, execution.clone()).await.unwrap().expect("Expected Arrow Apply");
    let batch = columns.next_batch().await.unwrap().unwrap();
    assert_eq!(batch.len, 2);
    assert!(batch.columns.iter().all(|column| !matches!(column, crate::columnar::Column::Values(_))));
    let actual = (0..batch.len).map(|row| batch.columns[1].value(row).unwrap()).collect::<Vec<_>>();
    assert_eq!(actual, [Value::I64(11), Value::I64(12)]);
    assert!(columns.next_batch().await.unwrap().is_none());
}

#[tokio::test]
async fn correlated_mark_checks_every_comparison_in_a_coalesced_dependent_page() {
    let execution = execution(100);
    let dependent = union(table(1, &[0; 100]), union(table(1, &[1]), table(1, &[2])));
    let dependent = Arc::new(dependent.filter(Expr::Literal(Value::Bool(true))).unwrap());
    let eq = |value| Expr::binary(BinaryOp::Eq, column(1), literal(value));
    let comparison = Expr::Case {
        branches: vec![(eq(0), Expr::Literal(Value::Bool(false))), (eq(1), Expr::Literal(Value::Bool(true)))],
        otherwise: Box::new(inverse_equals_one(Expr::binary(BinaryOp::Subtract, column(1), literal(2)))),
    };
    let relation = mark(table(0, &[1]), dependent, comparison, true);
    let runtime = EmittedRelation::derive(Arc::clone(&relation), &execution.context.catalog, &[], &execution.context.memory).unwrap();
    let mut columns = crate::operators::build_columns(runtime, execution.clone()).await.unwrap().expect("Expected Arrow membership output");
    let error = columns.next_batch().await.err().unwrap();
    assert_eq!(error.code, "xtdb.expression/division-by-zero");
}

#[tokio::test]
async fn mark_checks_eligibility_and_keeps_unknown_marks_in_memory_and_on_disk() {
    let data_type = LogicalType::Scalar(ScalarType::I64).maybe();
    let left_field = field(0,data_type.clone());
    let left_schema = Schema::new(vec![left_field.clone()]).unwrap();
    let input = Arc::new(Relation::new(left_schema.clone(),RelationNode::Values {
        rows: [Value::I64(1),Value::I64(2),Value::I64(3),Value::Null].into_iter().map(|value|vec![Expr::Literal(value)]).collect(),
    }).unwrap());
    let right_key = field(1,data_type.clone());
    let right_value = field(2,data_type);
    let dependent = Arc::new(Relation::new(Schema::new(vec![right_key.clone(),right_value.clone()]).unwrap(),RelationNode::Values {
        rows: vec![
            vec![literal(2),literal(2)], vec![literal(3),Expr::Literal(Value::Null)],
            vec![literal(5),literal(7)], vec![Expr::Literal(Value::Null),literal(9)],
        ],
    }).unwrap());
    let condition = Expr::binary(BinaryOp::Eq,Expr::binary(BinaryOp::Add,left_field.expression(),literal(1)),right_key.expression());
    let comparison = Expr::binary(BinaryOp::Gt,right_value.expression(),literal(0));
    let output = field(3,LogicalType::Scalar(ScalarType::Bool).maybe());
    let schema = left_schema.concat(&Schema::new(vec![output.clone()]).unwrap()).unwrap();
    let relation = Arc::new(Relation::new(schema,RelationNode::Mark {
        input,dependent,arguments: vec![],condition: Some(condition),comparison,output,
    }).unwrap());
    for threshold in [1,100] {
        let execution = execution_threshold(1,threshold);
        let baseline = execution.context.memory.used_bytes();
        let mut cursor = open(Arc::clone(&relation),&execution).await;
        let mut results = std::collections::BTreeMap::new();
        while let Some(page) = cursor.next_batch().await.unwrap() {
            if threshold == 1 { assert!(execution.context.spill_bytes() > 0); }
            for row in page_rows(page) {
                let [probe,mark] = <[Value;2]>::try_from(row).unwrap();
                let key = match probe { Value::I64(value) => Some(value),Value::Null => None,_ => panic!("Unexpected mark input") };
                assert!(results.insert(key,mark).is_none());
            }
        }
        assert_eq!(results,std::collections::BTreeMap::from([
            (Some(1),Value::Bool(true)),(Some(2),Value::Null),(Some(3),Value::Bool(false)),(None,Value::Bool(false)),
        ]));
        drop(cursor);
        assert_eq!(execution.context.memory.used_bytes(),baseline);
        assert_eq!(execution.context.spill_bytes(),0);
    }
}

#[tokio::test]
async fn mark_publishes_one_complete_page_for_each_nonempty_probe_page() {
    for batch_rows in [1,1024] {
        for correlated in [false,true] {
            let execution = execution(batch_rows);let baseline = execution.context.memory.used_bytes();
            let input = union(table(0,&[1,2]),table(0,&[3]));
            let relation = mark(input,table(1,&[1,3]),Expr::binary(BinaryOp::Eq,column(0),column(1)),correlated);
            let mut cursor = open(relation,&execution).await;
            let first = cursor.next_batch().await.unwrap().unwrap();
            let second = cursor.next_batch().await.unwrap().unwrap();
            assert_eq!(page_rows(first), [[Value::I64(1), Value::Bool(true)], [Value::I64(2), Value::Bool(false)]]);
            assert_eq!(page_rows(second), [[Value::I64(3), Value::Bool(true)]]);
            assert!(cursor.next_batch().await.unwrap().is_none());
            drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
        }
    }
}

#[tokio::test]
async fn correlated_mark_finishes_current_dependent_page_then_skips_later_pages_after_true() {
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    let separate = mark(table(0,&[1]),union(table(1,&[1]),table(1,&[0])),inverse_equals_one(column(1)),true);
    let mut cursor = open(separate,&execution).await;
    assert_eq!(page_rows(cursor.next_batch().await.unwrap().unwrap()),[[Value::I64(1),Value::Bool(true)]]);
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
    let together = mark(table(0,&[1]),table(1,&[1,0]),inverse_equals_one(column(1)),true);
    let mut cursor = open(together,&execution).await;
    assert_eq!(cursor.next_batch().await.err().unwrap().code,"xtdb.expression/division-by-zero");
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

#[tokio::test]
async fn limit_cannot_hide_later_probe_row_failure_in_mark_page() {
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    for correlated in [false,true] {
        let input = mark(table(0,&[1,0]),table(1,&[1]),inverse_equals_one(column(0)),correlated);
        let relation = Arc::new(Relation::new(input.schema().clone(),RelationNode::Limit { input,offset: None,fetch: Some(literal(1)) }).unwrap());
        let mut cursor = open(relation,&execution).await;
        assert_eq!(cursor.next_batch().await.err().unwrap().code,"xtdb.expression/division-by-zero");
        drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
    }
}

#[tokio::test]
async fn empty_values_produce_no_mark_page_for_correlated_or_shared_candidates() {
    let execution = execution(1);
    for correlated in [false,true] {
        let relation = mark(table(0,&[]),table(1,&[1]),Expr::binary(BinaryOp::Eq,column(0),column(1)),correlated);
        let mut cursor = open(relation,&execution).await;
        assert!(cursor.next_batch().await.unwrap().is_none());
        assert!(cursor.next_batch().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn shared_mark_stops_after_true_within_loaded_candidate_rows() {
    let execution = execution(1);
    let relation = mark(table(0,&[1]),table(1,&[1,0]),inverse_equals_one(column(1)),false);
    let mut cursor = open(relation,&execution).await;
    assert_eq!(page_rows(cursor.next_batch().await.unwrap().unwrap()),[[Value::I64(1),Value::Bool(true)]]);
}

fn typed_table(id: u32,values: &[Value],data_type: LogicalType) -> Arc<Relation> {
    Arc::new(Relation::new(Schema::new(vec![field(id,data_type)]).unwrap(),RelationNode::Values { rows: values.iter().cloned().map(|value|vec![Expr::Literal(value)]).collect() }).unwrap())
}
fn page_rows(page: ColumnBatch) -> Vec<Vec<Value>> {
    (0..page.len).map(|row| page.columns.iter().map(|column| column.value(row).unwrap()).collect()).collect()
}

#[tokio::test]
async fn spilled_scalar_mark_uses_build_then_probe_partition_schedule() {
    for batch_rows in [1,1024] {
        let execution = execution_threshold(batch_rows,0);let baseline = execution.context.memory.used_bytes();
        let dependent = union(table(1,&[0,1,2,3]),table(1,&[4,5,6,7]));
        let input = union(union(table(0,&[0,1]),table(0,&[2,3])),union(table(0,&[4,5]),table(0,&[6,7])));
        let mut cursor = open(mark(input,dependent,Expr::binary(BinaryOp::Eq,column(0),column(1)),false),&execution).await;
        let mut actual = Vec::new();while let Some(page) = cursor.next_batch().await.unwrap() { actual.push(page_rows(page)); }
        let mut expected = Vec::new();
        for build_part in 0..4 { for probe_group in 0..2 {
            let rows = (0i64..8).filter(|value|crate::join_storage::hash_values([&Value::I64(*value)]).unwrap() as usize & 7 == probe_group * 4 + build_part).map(|value|vec![Value::I64(value),Value::Bool(true)]).collect::<Vec<_>>();
            if !rows.is_empty() { expected.push(rows); }
        } }
        assert_eq!(actual,expected);assert!(execution.context.spill_peak_bytes() > 0);
        drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
    }
}

#[tokio::test]
async fn spilled_scalar_mark_retains_null_facts_from_other_partitions() {
    let execution = execution_threshold(1,0);let baseline = execution.context.memory.used_bytes();
    let ty = LogicalType::Scalar(ScalarType::I64).maybe();
    let dependent = union(typed_table(1,&[Value::I64(1)],ty.clone()),typed_table(1,&[Value::Null],ty.clone()));
    let input = typed_table(0,&[Value::I64(1),Value::I64(2),Value::Null],ty);
    let mut cursor = open(mark(input,dependent,Expr::binary(BinaryOp::Eq,column(0),column(1)),false),&execution).await;
    let mut actual = Vec::new();while let Some(page) = cursor.next_batch().await.unwrap() { actual.extend(page_rows(page)); }
    assert_eq!(actual.len(),3);
    for expected in [vec![Value::I64(1),Value::Bool(true)],vec![Value::I64(2),Value::Null],vec![Value::Null,Value::Null]] { assert!(actual.contains(&expected)); }
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

#[tokio::test]
async fn scalar_membership_traces_replay_and_peak_rows_across_build_partitions() {
    use rtdb_plan::Tracer;
    use Value::{Bool, I64, Null};

    let ty = LogicalType::Scalar(ScalarType::I64).maybe();
    let repeated = vec![I64(1); 600];
    let dependent = union(
        union(
            typed_table(1, &repeated, ty.clone()),
            typed_table(1, &repeated, ty.clone()),
        ),
        typed_table(1, &[Null], ty.clone()),
    );
    let input = typed_table(0, &[I64(1), Null], ty);
    let relation = mark(input, dependent, Expr::binary(BinaryOp::Eq, column(0), column(1)), false);

    for (threshold, tracing, drain) in [(1, true, true), (1, true, false), (1, false, true), (10_000, true, true)] {
        let mut execution = execution_threshold(32, threshold);
        let (tracer, mut receiver) = rtdb_metrics::TraceRecorder::new(MemoryBudget::new(1 << 20), 16, tracing, false).unwrap();
        let parent = tracer.start(None, "xtdb.query", &[]).unwrap();
        execution.context = Arc::clone(&execution.context).with_tracer(Some(tracer.clone()), Some(parent.context()));
        let mut cursor = open(Arc::clone(&relation), &execution).await;
        let mut rows = Vec::new();
        while let Some(batch) = cursor.next_batch().await.unwrap() {
            rows.extend(page_rows(batch));
            if !drain { break; }
        }
        let matched = vec![I64(1), Bool(true)];
        let unknown = vec![Null, Null];
        let spilled = threshold == 1;
        let expected = match (spilled, drain) {
            (true, true) => vec![unknown, matched],
            (true, false) => vec![unknown],
            (false, _) => vec![matched, unknown],
        };
        assert_eq!(rows, expected);
        drop((cursor, parent));
        assert_eq!(execution.context.spill_bytes(), 0);
        assert_eq!(execution.context.spill_peak_bytes() > 0, spilled);
        tracer.close();

        let mut replay = BTreeMap::new();
        while let Some(span) = receiver.next().await {
            for (name, value) in span.attributes().filter(|(name, _)| name.starts_with("cursor.join_spill.")) {
                assert_eq!(span.name, "query.cursor.mark-join");
                assert!(replay.insert(name.to_owned(), value.parse::<u64>().unwrap()).is_none());
            }
        }
        if tracing {
            let expected = match (spilled, drain) {
                (true, true) => [3, 2, 1200],
                (true, false) => [1, 1, 1],
                (false, _) => [0, 0, 0],
            };
            assert_eq!([
                replay["cursor.join_spill.build_replay_frames"],
                replay["cursor.join_spill.probe_replay_frames"],
                replay["cursor.join_spill.peak_build_partition_rows"],
            ], expected);
        } else {
            assert!(replay.is_empty());
        }
    }
}

#[tokio::test]
async fn spilled_nested_mark_keeps_probe_pages_and_compares_all_build_partitions() {
    let execution = execution_threshold(1,0);let baseline = execution.context.memory.used_bytes();
    let nested = |left,right|Value::list(vec![Value::I64(left),right]);
    let ty = nested(1,Value::Null).logical_type().join(&nested(2,Value::I64(3)).logical_type()).maybe();
    let dependent = union(typed_table(1,&[nested(1,Value::Null)],ty.clone()),typed_table(1,&[nested(2,Value::I64(3))],ty.clone()));
    let input = union(typed_table(0,&[nested(1,Value::I64(2)),nested(2,Value::I64(3))],ty.clone()),typed_table(0,&[Value::Null],ty));
    let mut cursor = open(mark(input,dependent,Expr::binary(BinaryOp::Eq,column(0),column(1)),false),&execution).await;
    assert_eq!(page_rows(cursor.next_batch().await.unwrap().unwrap()),[vec![nested(1,Value::I64(2)),Value::Null],vec![nested(2,Value::I64(3)),Value::Bool(true)]]);
    assert_eq!(page_rows(cursor.next_batch().await.unwrap().unwrap()),[vec![Value::Null,Value::Null]]);
    assert!(cursor.next_batch().await.unwrap().is_none());
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

#[tokio::test]
async fn spilled_keyless_mark_preserves_first_true_short_circuit_between_partitions() {
    let execution = execution_threshold(1,0);let baseline = execution.context.memory.used_bytes();
    let relation = mark(union(table(0,&[1,2]),table(0,&[3])),union(table(1,&[1]),table(1,&[0])),inverse_equals_one(column(1)),false);
    let mut cursor = open(relation,&execution).await;
    let retained = cursor.next_batch().await.unwrap().unwrap();assert_eq!(retained.len,2);
    execution.context.cancellation.cancel();assert_eq!(cursor.next_batch().await.err().unwrap().code,"xtdb/query-cancelled");
    drop(cursor);assert_eq!(execution.context.spill_bytes(),0);assert!(execution.context.memory.used_bytes() > baseline);
    assert_eq!(page_rows(retained),[vec![Value::I64(1),Value::Bool(true)],vec![Value::I64(2),Value::Bool(true)]]);
    assert_eq!(execution.context.memory.used_bytes(),baseline);
}

#[tokio::test]
async fn arrow_vector_membership_preserves_unknown_and_signed_zero_across_candidate_pages() {
    let vector = |value| Value::list(vec![value]);
    let probe = [vector(Value::F32(-0.0)), vector(Value::F32(4.0)), vector(Value::Null), Value::Null];
    let build = [vector(Value::F64(0.0)), vector(Value::Null)];
    let values = |id, rows: &[Value]| {
        let ty = LogicalType::join_all(rows.iter().map(Value::logical_type).collect::<Vec<_>>().iter());
        Arc::new(Relation::new(Schema::new(vec![field(id, ty)]).unwrap(), RelationNode::Values {
            rows: rows.iter().cloned().map(|value| vec![Expr::Literal(value)]).collect(),
        }).unwrap())
    };
    for threshold in [0, 10] {
        let execution = execution_threshold(32, threshold);
        let input = values(0, &probe);
        let dependent = values(1, &build);
        let comparison = Expr::binary(BinaryOp::Eq, input.schema().fields()[0].expression(), dependent.schema().fields()[0].expression());
        let relation = mark(input, dependent, comparison, false);
        let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], &execution.context.memory).unwrap();
        let mut cursor = crate::operators::build_columns(runtime, execution).await.unwrap().unwrap();
        let mut marks = Vec::new();
        while let Some(batch) = cursor.next_batch().await.unwrap() {
            marks.extend((0..batch.len).map(|row| batch.columns[1].value(row).unwrap()));
        }
        assert_eq!(marks, [Value::Bool(true), Value::Null, Value::Null, Value::Null]);
    }
}
