use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{BinaryOp, ColumnId, Expr, Field, JoinKind, Relation, RelationNode, Schema};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType, Result, ScalarType, TimeUnit, Timestamp, Value};
use tokio_util::sync::CancellationToken;

fn context(cancel: &CancellationToken) -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 26), Timestamp(0), "UTC", QueryOptions { join_build_rows: i64::MAX, batch_rows: 1, ..Default::default() }, cancel).unwrap()
}
fn numbers(id: u32, name: &str, values: &[i64]) -> Arc<Relation> {
    let field = Field { id: ColumnId(id), name: name.into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) };
    Arc::new(Relation::new(Schema::new(vec![field]).unwrap(), RelationNode::Values { rows: values.iter().map(|value| vec![Expr::Literal(Value::I64(*value))]).collect() }).unwrap())
}
fn join(left: Arc<Relation>, right: Arc<Relation>, kind: JoinKind, condition: Expr) -> PreparedQuery {
    let schema = match kind {
        JoinKind::Left => left.schema().concat(&right.schema().nullable().unwrap()).unwrap(),
        JoinKind::Right => left.schema().nullable().unwrap().concat(right.schema()).unwrap(),
        JoinKind::Full => left.schema().nullable().unwrap().concat(&right.schema().nullable().unwrap()).unwrap(),
        JoinKind::Semi | JoinKind::Anti => left.schema().clone(),
        _ => left.schema().concat(right.schema()).unwrap(),
    };
    PreparedQuery::new(Arc::new(Relation::new(schema, RelationNode::Join { left, right, kind, condition: Some(condition) }).unwrap())).unwrap()
}
async fn collect(query: &PreparedQuery, context: &Arc<QueryContext>) -> Result<Vec<Vec<Value>>> {
    let mut cursor = query.execute(Arc::clone(context), vec![]).await?;
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() {
            rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?);
        }
    }
    Ok(rows)
}

#[tokio::test]
async fn cached_keys_are_removed_from_matched_and_unmatched_join_outputs() {
    for (kind, expected) in [
        (JoinKind::Inner, vec![vec![Value::I64(1), Value::I64(2)]]),
        (JoinKind::Left, vec![vec![Value::I64(1), Value::I64(2)], vec![Value::I64(3), Value::Null]]),
        (JoinKind::Right, vec![vec![Value::I64(1), Value::I64(2)], vec![Value::Null, Value::I64(5)]]),
        (JoinKind::Full, vec![vec![Value::I64(1), Value::I64(2)], vec![Value::Null, Value::I64(5)], vec![Value::I64(3), Value::Null]]),
        (JoinKind::Semi, vec![vec![Value::I64(1)]]),
        (JoinKind::Anti, vec![vec![Value::I64(3)]]),
    ] {
        let left = numbers(0, "l", &[1, 3]); let right = numbers(1, "r", &[2, 5]);
        let plus = Expr::binary(BinaryOp::Add, left.schema().fields()[0].expression(), Expr::Literal(Value::I64(2)));
        let minus = Expr::binary(BinaryOp::Add, right.schema().fields()[0].expression(), Expr::Literal(Value::I64(1)));
        let query = join(left, right, kind, Expr::binary(BinaryOp::Eq, plus, minus));
        let context = context(&CancellationToken::new());
        assert_eq!(collect(&query, &context).await.unwrap(), expected, "{kind:?}");
        assert_eq!(context.spill_bytes(), 0); assert_eq!(context.memory.used_bytes(), 0);
    }
}

#[tokio::test]
async fn computed_build_keys_run_before_an_empty_probe_opens() {
    let left = numbers(0, "l", &[0]); let right = numbers(1, "r", &[]);
    let key = Expr::binary(BinaryOp::Divide, Expr::Literal(Value::I64(1)), left.schema().fields()[0].expression());
    let condition = Expr::binary(BinaryOp::Eq, key, right.schema().fields()[0].expression());
    let context = context(&CancellationToken::new());
    assert!(collect(&join(left, right, JoinKind::Inner, condition), &context).await.is_err());
    assert_eq!(context.spill_bytes(), 0); assert_eq!(context.memory.used_bytes(), 0);
}

#[tokio::test]
async fn public_left_join_uses_declared_row_counts_for_null_equal_build_selection() {
    fn nulls(id: u32,count: usize) -> Arc<Relation> {
        Arc::new(Relation::new(Schema::new(vec![Field { id: ColumnId(id),name: format!("k{id}"),qualifier: None,qualified_only: false,unnamed: false,data_type: LogicalType::Null }]).unwrap(),RelationNode::Values { rows: vec![vec![Expr::Literal(Value::Null)];count] }).unwrap())
    }
    for (left_rows,right_rows,expected) in [(1,2,1),(2,2,4),(2,1,2)] {
        let left = nulls(0,left_rows);let right = nulls(1,right_rows);
        let condition = Expr::binary(BinaryOp::Eq,left.schema().fields()[0].expression(),right.schema().fields()[0].expression());
        let context = context(&CancellationToken::new());
        assert_eq!(collect(&join(left,right,JoinKind::Left,condition),&context).await.unwrap(),vec![vec![Value::Null,Value::Null];expected]);
        assert_eq!(context.memory.used_bytes(),0);assert_eq!(context.spill_bytes(),0);
    }
}

#[tokio::test]
async fn extracted_equality_keys_reject_mismatched_pairs_before_theta_expressions() {
    for (right_value, zero_at, matches) in [(2, 3, false), (1, 2, true)] {
        let left = numbers(0, "l", &[1]); let right = numbers(1, "r", &[right_value]);
        let lhs = left.schema().fields()[0].expression(); let rhs = right.schema().fields()[0].expression();
        let denominator = Expr::binary(BinaryOp::Subtract, Expr::binary(BinaryOp::Add, lhs.clone(), rhs.clone()), Expr::Literal(Value::I64(zero_at)));
        let theta = Expr::binary(BinaryOp::Gt, Expr::binary(BinaryOp::Divide, Expr::Literal(Value::I64(1)), denominator), Expr::Literal(Value::I64(0)));
        let key = Expr::binary(BinaryOp::Eq, Expr::binary(BinaryOp::Add, lhs, Expr::Literal(Value::I64(1))), Expr::binary(BinaryOp::Add, rhs, Expr::Literal(Value::I64(1))));
        let context = context(&CancellationToken::new());
        let actual = collect(&join(left, right, JoinKind::Inner, Expr::binary(BinaryOp::And, theta, key)), &context).await;
        if matches { assert!(actual.is_err()); } else { assert!(actual.unwrap().is_empty()); }
        assert_eq!(context.spill_bytes(), 0); assert_eq!(context.memory.used_bytes(), 0);
    }
}

fn keyed(id: u32, key: Option<bool>) -> Arc<Relation> {
    let fields = vec![
        Field { id: ColumnId(id), name: format!("key{id}"), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::Bool).maybe() },
        Field { id: ColumnId(id + 1), name: format!("value{id}"), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) },
    ];
    Arc::new(Relation::new(Schema::new(fields).unwrap(), RelationNode::Values { rows: vec![vec![Expr::Literal(key.map_or(Value::Null, Value::Bool)), Expr::Literal(Value::I64(0))]] }).unwrap())
}

#[tokio::test]
async fn hash_candidates_skip_unknown_key_pairs_but_anti_join_scans_them() {
    for (kind, expected_rows) in [(JoinKind::Inner, 0), (JoinKind::Semi, 0), (JoinKind::Left, 1), (JoinKind::Right, 1), (JoinKind::Full, 2), (JoinKind::Anti, 0)] {
        for right_key in [Some(true), None] {
            let left = keyed(0, None); let right = keyed(2, right_key);
            let key = Expr::binary(BinaryOp::Eq, left.schema().fields()[0].expression(), right.schema().fields()[0].expression());
            let zero = Expr::binary(BinaryOp::Add, left.schema().fields()[1].expression(), right.schema().fields()[1].expression());
            let theta = Expr::binary(BinaryOp::Gt, Expr::binary(BinaryOp::Divide, Expr::Literal(Value::I64(1)), zero), Expr::Literal(Value::I64(0)));
            let context = context(&CancellationToken::new());
            let result = collect(&join(left, right, kind, Expr::binary(BinaryOp::And, key, theta)), &context).await;
            if kind == JoinKind::Anti || right_key.is_none() { assert!(result.is_err(), "{kind:?} {right_key:?}"); }
            else { assert_eq!(result.unwrap().len(), expected_rows, "{kind:?}"); }
            assert_eq!(context.spill_bytes(), 0); assert_eq!(context.memory.used_bytes(), 0);
        }
    }
}

#[tokio::test]
async fn composite_hashes_follow_equality_pair_order_in_both_input_schemas() {
    let left = keyed(0, Some(true)); let right = keyed(2, Some(true));
    let first = Expr::binary(BinaryOp::Eq, right.schema().fields()[1].expression(), left.schema().fields()[1].expression());
    let second = Expr::binary(BinaryOp::Eq, left.schema().fields()[0].expression(), right.schema().fields()[0].expression());
    let context = context(&CancellationToken::new());
    assert_eq!(collect(&join(left, right, JoinKind::Inner, Expr::binary(BinaryOp::And, first, second)), &context).await.unwrap(), [vec![Value::Bool(true), Value::I64(0), Value::Bool(true), Value::I64(0)]]);
    assert_eq!(context.spill_bytes(), 0); assert_eq!(context.memory.used_bytes(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_a_suspended_build_key_releases_the_input_and_spool() {
    let left = numbers(0, "l", &[1]); let right = numbers(1, "r", &[]);
    let sleep = Expr::Call { name: "sleep".into(), args: vec![Expr::Literal(Value::Duration { value: 60_000, unit: TimeUnit::Millisecond })], return_type: LogicalType::Null };
    let key = Expr::Call { name: "format".into(), args: vec![Expr::Literal(Value::Utf8("%s%s".into())), sleep, left.schema().fields()[0].expression()], return_type: LogicalType::Scalar(ScalarType::Utf8) };
    let right_key = Expr::Call { name: "str".into(), args: vec![right.schema().fields()[0].expression()], return_type: LogicalType::Scalar(ScalarType::Utf8) };
    let condition = Expr::binary(BinaryOp::Eq, key, right_key);
    let cancel = CancellationToken::new(); let context = context(&cancel);
    let query = join(left, right, JoinKind::Inner, condition);
    let mut cursor = query.execute(Arc::clone(&context), vec![]).await.unwrap();
    let (result, ()) = tokio::join!(cursor.next_batch(), async { tokio::task::yield_now().await; cancel.cancel(); });
    assert_eq!(result.err().unwrap().code, "xtdb/query-cancelled");
    drop(cursor); assert_eq!(context.spill_bytes(), 0); assert_eq!(context.memory.used_bytes(), 0);
}

#[tokio::test]
async fn outer_theta_keeps_projected_nonnullable_types_before_synthetic_null_row() {
    let left = numbers(0,"l",&[1]);let right = numbers(1,"r",&[0]);
    let division = Expr::binary(BinaryOp::Divide,Expr::Literal(Value::I64(1)),right.schema().fields()[0].expression());
    let first = Expr::binary(BinaryOp::Eq,division,Expr::Literal(Value::I64(0)));
    let second = Expr::binary(BinaryOp::Eq,left.schema().fields()[0].expression(),right.schema().fields()[0].expression());
    let condition = Expr::Unary { op: rtdb_plan::UnaryOp::IsNull,expr: Box::new(Expr::binary(BinaryOp::Or,first,second)) };
    let context = context(&CancellationToken::new());
    assert_eq!(collect(&join(left,right,JoinKind::Left,condition),&context).await.unwrap(),[vec![Value::I64(1),Value::Null]]);
    assert_eq!(context.memory.used_bytes(),0);assert_eq!(context.spill_bytes(),0);
}

fn measured(id: u32, rows: &[(i64, Option<f64>)]) -> Arc<Relation> {
    let fields = vec![
        Field { id: ColumnId(id), name: format!("k{id}"), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) },
        Field { id: ColumnId(id + 1), name: format!("v{id}"), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::F64).maybe() },
    ];
    let rows = rows.iter().map(|&(key, value)| vec![Expr::Literal(Value::I64(key)), Expr::Literal(value.map_or(Value::Null, Value::F64))]).collect();
    Arc::new(Relation::new(Schema::new(fields).unwrap(), RelationNode::Values { rows }).unwrap())
}

#[tokio::test]
async fn an_inner_join_residual_keeps_pairs_it_holds_for_and_meets_later_conjuncts_only_after_earlier_ones() {
    let left = measured(0, &[(1, Some(1.0)), (2, Some(2.0)), (3, None), (4, Some(4.0)), (4, Some(5.0))]);
    let right = measured(2, &[(1, Some(1.0)), (2, Some(3.0)), (3, Some(5.0)), (4, Some(4.5))]);
    let (lk, lv) = (left.schema().fields()[0].expression(), left.schema().fields()[1].expression());
    let (rk, rv) = (right.schema().fields()[0].expression(), right.schema().fields()[1].expression());
    let key = Expr::binary(BinaryOp::Eq, lk, rk);
    let differs = Expr::binary(BinaryOp::NotEq, lv.clone(), rv.clone());
    let context = context(&CancellationToken::new());
    let rows = collect(&join(left.clone(), right.clone(), JoinKind::Inner, Expr::binary(BinaryOp::And, key.clone(), differs.clone())), &context).await.unwrap();
    let keys: Vec<_> = rows.iter().map(|row| (row[0].clone(), row[1].clone())).collect();
    assert_eq!(keys, [(Value::I64(2), Value::F64(2.0)), (Value::I64(4), Value::F64(4.0)), (Value::I64(4), Value::F64(5.0))]);

    let inverse = Expr::binary(BinaryOp::Gt, Expr::binary(BinaryOp::Divide, Expr::Literal(Value::F64(1.0)), Expr::binary(BinaryOp::Subtract, lv, rv)), Expr::Literal(Value::F64(0.0)));
    let guarded = Expr::binary(BinaryOp::And, key, Expr::binary(BinaryOp::And, differs, inverse));
    let rows = collect(&join(left, right, JoinKind::Inner, guarded), &context).await.unwrap();
    assert_eq!(rows.iter().map(|row| row[1].clone()).collect::<Vec<_>>(), [Value::F64(5.0)], "the equal pair never meets the division");
    assert_eq!(context.memory.used_bytes(), 0);
}

#[tokio::test]
async fn an_inner_join_residual_is_evaluated_whole_for_every_key_matched_pair() {
    for (left_rows, matched) in [(vec![(1, Some(1.0)), (3, None)], true), (vec![(1, Some(1.0))], true)] {
        let left = measured(0, &left_rows);
        let right = measured(2, &[(1, Some(1.0)), (3, Some(5.0))]);
        let (lk, lv) = (left.schema().fields()[0].expression(), left.schema().fields()[1].expression());
        let (rk, rv) = (right.schema().fields()[0].expression(), right.schema().fields()[1].expression());
        let zero = Expr::binary(BinaryOp::Subtract, rk.clone(), rk.clone());
        let divides = Expr::binary(BinaryOp::Gt, Expr::binary(BinaryOp::Divide, Expr::Literal(Value::I64(1)), zero), Expr::Literal(Value::I64(0)));
        let condition = Expr::binary(BinaryOp::And, Expr::binary(BinaryOp::Eq, lk, rk), Expr::binary(BinaryOp::And, Expr::binary(BinaryOp::NotEq, lv, rv), divides));
        let context = context(&CancellationToken::new());
        let result = collect(&join(left, right, JoinKind::Inner, condition), &context).await;
        if matched { assert_eq!(result.unwrap_err().code, "xtdb.expression/division-by-zero"); } else { assert!(result.unwrap().is_empty()); }
        assert_eq!(context.memory.used_bytes(), 0);
    }
}
