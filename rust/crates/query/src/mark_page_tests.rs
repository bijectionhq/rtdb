use super::*;
use rtdb_columnar::MemoryBudget;
use rtdb_plan::{BinaryOp,ColumnId,Field};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{LogicalType,ScalarType,Timestamp};
use tokio_util::sync::CancellationToken;
use crate::{CatalogSnapshot,DatabaseSnapshot,QueryOptions,context::OuterRows,parameters::BoundParameters,runtime_schema::EmittedRelation};

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
async fn open(relation: Arc<Relation>,execution: &Execution) -> BoxPageCursor {
    let runtime = EmittedRelation::derive(Arc::clone(&relation),&execution.context.catalog,execution.parameters.types(),&execution.context.memory).unwrap();
    build_pages(Arc::clone(&runtime),execution.clone()).await.unwrap()
}
fn inverse_equals_one(column: Expr) -> Expr { Expr::binary(BinaryOp::Eq,Expr::binary(BinaryOp::Divide,literal(1),column),literal(1)) }

#[tokio::test]
async fn mark_hashes_eligibility_keys_and_keeps_unknown_marks_in_memory_and_on_disk() {
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
        while let Some(mut page) = cursor.next_page().await.unwrap() {
            if threshold == 1 { assert!(execution.context.spill_bytes() > 0); }
            while let Some(row) = page.next() {
                let [probe,mark] = <[Value;2]>::try_from(row.values).unwrap();
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
            let mut first = cursor.next_page().await.unwrap().unwrap();assert_eq!(first.len(),2);
            assert_eq!(first.next().unwrap().values,[Value::I64(1),Value::Bool(true)]);
            assert_eq!(first.next().unwrap().values,[Value::I64(2),Value::Bool(false)]);
            let mut second = cursor.next_page().await.unwrap().unwrap();assert_eq!(second.len(),1);
            assert_eq!(second.next().unwrap().values,[Value::I64(3),Value::Bool(true)]);
            assert!(cursor.next_page().await.unwrap().is_none());
            drop(first);drop(second);drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
        }
    }
}

#[tokio::test]
async fn correlated_mark_finishes_current_dependent_page_then_skips_later_pages_after_true() {
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    let separate = mark(table(0,&[1]),union(table(1,&[1]),table(1,&[0])),inverse_equals_one(column(1)),true);
    let mut cursor = open(separate,&execution).await;
    assert_eq!(cursor.next_page().await.unwrap().unwrap().next().unwrap().values,[Value::I64(1),Value::Bool(true)]);
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
    let together = mark(table(0,&[1]),table(1,&[1,0]),inverse_equals_one(column(1)),true);
    let mut cursor = open(together,&execution).await;
    assert_eq!(cursor.next_page().await.err().unwrap().code,"xtdb.expression/division-by-zero");
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

#[tokio::test]
async fn limit_cannot_hide_later_probe_row_failure_in_mark_page() {
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    for correlated in [false,true] {
        let input = mark(table(0,&[1,0]),table(1,&[1]),inverse_equals_one(column(0)),correlated);
        let relation = Arc::new(Relation::new(input.schema().clone(),RelationNode::Limit { input,offset: None,fetch: Some(literal(1)) }).unwrap());
        let mut cursor = open(relation,&execution).await;
        assert_eq!(cursor.next_page().await.err().unwrap().code,"xtdb.expression/division-by-zero");
        drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
    }
}

#[tokio::test]
async fn empty_values_produce_no_mark_page_for_correlated_or_shared_candidates() {
    let execution = execution(1);
    for correlated in [false,true] {
        let relation = mark(table(0,&[]),table(1,&[1]),Expr::binary(BinaryOp::Eq,column(0),column(1)),correlated);
        let mut cursor = open(relation,&execution).await;
        assert!(cursor.next_page().await.unwrap().is_none());
        assert!(cursor.next_page().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn shared_mark_stops_after_true_within_loaded_candidate_rows() {
    let execution = execution(1);
    let relation = mark(table(0,&[1]),table(1,&[1,0]),inverse_equals_one(column(1)),false);
    let mut cursor = open(relation,&execution).await;
    assert_eq!(cursor.next_page().await.unwrap().unwrap().next().unwrap().values,[Value::I64(1),Value::Bool(true)]);
}

fn typed_table(id: u32,values: &[Value],data_type: LogicalType) -> Arc<Relation> {
    Arc::new(Relation::new(Schema::new(vec![field(id,data_type)]).unwrap(),RelationNode::Values { rows: values.iter().cloned().map(|value|vec![Expr::Literal(value)]).collect() }).unwrap())
}
fn page_rows(page: OwnedPage) -> Vec<Vec<Value>> { page.rows().iter().map(|row|row.values.clone()).collect() }

#[tokio::test]
async fn spilled_scalar_mark_uses_build_then_probe_partition_schedule() {
    for batch_rows in [1,1024] {
        let execution = execution_threshold(batch_rows,0);let baseline = execution.context.memory.used_bytes();
        let dependent = union(table(1,&[0,1,2,3]),table(1,&[4,5,6,7]));
        let input = union(union(table(0,&[0,1]),table(0,&[2,3])),union(table(0,&[4,5]),table(0,&[6,7])));
        let mut cursor = open(mark(input,dependent,Expr::binary(BinaryOp::Eq,column(0),column(1)),false),&execution).await;
        let mut actual = Vec::new();while let Some(page) = cursor.next_page().await.unwrap() { actual.push(page_rows(page)); }
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
    let mut actual = Vec::new();while let Some(page) = cursor.next_page().await.unwrap() { actual.extend(page_rows(page)); }
    assert_eq!(actual.len(),3);
    for expected in [vec![Value::I64(1),Value::Bool(true)],vec![Value::I64(2),Value::Null],vec![Value::Null,Value::Null]] { assert!(actual.contains(&expected)); }
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

#[tokio::test]
async fn spilled_nested_mark_keeps_probe_pages_and_compares_all_build_partitions() {
    let execution = execution_threshold(1,0);let baseline = execution.context.memory.used_bytes();
    let nested = |left,right|Value::list(vec![Value::I64(left),right]);
    let ty = nested(1,Value::Null).logical_type().join(&nested(2,Value::I64(3)).logical_type()).maybe();
    let dependent = union(typed_table(1,&[nested(1,Value::Null)],ty.clone()),typed_table(1,&[nested(2,Value::I64(3))],ty.clone()));
    let input = union(typed_table(0,&[nested(1,Value::I64(2)),nested(2,Value::I64(3))],ty.clone()),typed_table(0,&[Value::Null],ty));
    let mut cursor = open(mark(input,dependent,Expr::binary(BinaryOp::Eq,column(0),column(1)),false),&execution).await;
    assert_eq!(page_rows(cursor.next_page().await.unwrap().unwrap()),[vec![nested(1,Value::I64(2)),Value::Null],vec![nested(2,Value::I64(3)),Value::Bool(true)]]);
    assert_eq!(page_rows(cursor.next_page().await.unwrap().unwrap()),[vec![Value::Null,Value::Null]]);
    assert!(cursor.next_page().await.unwrap().is_none());
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

#[tokio::test]
async fn spilled_keyless_mark_preserves_first_true_short_circuit_between_partitions() {
    let execution = execution_threshold(1,0);let baseline = execution.context.memory.used_bytes();
    let relation = mark(union(table(0,&[1,2]),table(0,&[3])),union(table(1,&[1]),table(1,&[0])),inverse_equals_one(column(1)),false);
    let mut cursor = open(relation,&execution).await;
    let retained = cursor.next_page().await.unwrap().unwrap();assert_eq!(retained.len(),2);
    execution.context.cancellation.cancel();assert_eq!(cursor.next_page().await.err().unwrap().code,"xtdb/query-cancelled");
    drop(cursor);assert_eq!(execution.context.spill_bytes(),0);assert!(execution.context.memory.used_bytes() > baseline);
    assert_eq!(page_rows(retained),[vec![Value::I64(1),Value::Bool(true)],vec![Value::I64(2),Value::Bool(true)]]);
    assert_eq!(execution.context.memory.used_bytes(),baseline);
}
