use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{BinaryOp, ColumnId, Expr, Field, JoinKind, Relation, RelationNode, Schema};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType, Result, ScalarType, TimeUnit, Timestamp, Value};
use tokio_util::sync::CancellationToken;

fn context(cancel: &CancellationToken) -> Arc<QueryContext> {
    join_context(cancel, i64::MAX)
}

fn join_context(cancel: &CancellationToken, join_build_rows: i64) -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 26), Timestamp(0), "UTC", QueryOptions { join_build_rows, batch_rows: 1, ..Default::default() }, cancel).unwrap()
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

fn typed_rows(first: u32, types: &[ScalarType], rows: Vec<Vec<Value>>) -> Arc<Relation> {
    let fields = types.iter().enumerate().map(|(offset, ty)| {
        let id = first + offset as u32;
        Field { id: ColumnId(id), name: format!("k{id}"), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ty.clone()).maybe() }
    }).collect();
    let rows = rows.into_iter().map(|row| row.into_iter().map(Expr::Literal).collect()).collect();
    Arc::new(Relation::new(Schema::new(fields).unwrap(), RelationNode::Values { rows }).unwrap())
}

#[tokio::test]
async fn anti_equijoin_preserves_nulls_and_rechecks_colliding_keys_in_probe_order() {
    use Value::{I64, Null};

    let large = 1_i64 << 53;
    let probe = typed_rows(0, &[ScalarType::I64], vec![vec![I64(7)], vec![I64(large + 1)], vec![Null],
        vec![I64(31)], vec![I64(large)], vec![I64(large + 1)], vec![I64(11)]]);
    let build = typed_rows(1, &[ScalarType::I64], vec![vec![I64(7)], vec![I64(7)], vec![I64(large)], vec![Null], vec![I64(11)]]);
    let key = Expr::binary(BinaryOp::Eq, probe.schema().fields()[0].expression(), build.schema().fields()[0].expression());
    let context = context(&CancellationToken::new());
    let rows = collect(&join(probe, build, JoinKind::Anti, key), &context).await.unwrap();
    assert_eq!(rows, [vec![I64(large + 1)], vec![Null], vec![I64(31)], vec![I64(large + 1)]]);
    assert_eq!(context.memory.used_bytes(), 0);
}

#[tokio::test]
async fn anti_equijoin_matches_composite_keys_across_widths_and_byte_layouts_in_memory_and_on_disk() {
    use Value::{Binary, Bool, I16, I32, Null, Utf8};

    let probe = typed_rows(0, &[ScalarType::Bool, ScalarType::I16, ScalarType::Utf8, ScalarType::I32], vec![
        vec![Bool(true), I16(-12), Utf8("café".into()), I32(1)],
        vec![Bool(false), I16(-12), Utf8("café".into()), I32(2)],
        vec![Bool(true), I16(25), Utf8("other".into()), I32(3)],
        vec![Bool(false), I16(-12), Utf8("café".into()), I32(4)],
    ]);
    let build = typed_rows(4, &[ScalarType::Bool, ScalarType::I32, ScalarType::Binary], vec![
        vec![Bool(true), I32(-12), Binary("café".as_bytes().to_vec())],
        vec![Bool(true), I32(25), Binary(b"other".to_vec())],
        vec![Null, I32(-12), Binary("café".as_bytes().to_vec())],
    ]);
    let comparisons = [2, 0, 1].map(|index| {
        let mut operands = [probe.schema().fields()[index].expression(), build.schema().fields()[index].expression()];
        if index == 0 { operands.swap(0, 1); }
        let [left, right] = operands;
        Expr::binary(BinaryOp::Eq, left, right)
    });
    let condition = comparisons.into_iter().reduce(|left, right| Expr::binary(BinaryOp::And, left, right)).unwrap();
    let query = join(probe, build, JoinKind::Anti, condition);
    for threshold in [i64::MAX, 1] {
        let context = join_context(&CancellationToken::new(), threshold);
        assert_eq!(collect(&query, &context).await.unwrap(), [
            vec![Bool(false), I16(-12), Utf8("café".into()), I32(2)],
            vec![Bool(false), I16(-12), Utf8("café".into()), I32(4)],
        ]);
        assert_eq!(context.spill_peak_bytes() > 0, threshold == 1);
        assert_eq!(context.spill_bytes(), 0);
        assert_eq!(context.memory.used_bytes(), 0);
    }
}

#[tokio::test]
async fn anti_equijoin_preserves_signed_zero_nan_and_mixed_float_equality() {
    use Value::{F32, F64, I64, Null};

    let large = 1_i64 << 53;
    for (probe_type, probe_keys, build_keys, expected_ids) in [
        (ScalarType::F32, vec![F32(-0.0), F32(0.0), F32(1.5), F32(f32::from_bits(0x7fc0_0000)), Null],
            vec![F64(0.0), F64(1.5), F64(f64::from_bits(0x7ff8_0000_0000_0000)), Null], [3, 4]),
        (ScalarType::I64, vec![I64(0), I64(large), I64(large + 1), I64(large + 2), Null],
            vec![F64(-0.0), F64(large as f64), Null], [3, 4]),
    ] {
        let float_probe = probe_type == ScalarType::F32;
        let probe_rows = probe_keys.into_iter().enumerate().map(|(id, key)| vec![key, I64(id as i64)]).collect();
        let probe = typed_rows(0, &[probe_type, ScalarType::I64], probe_rows);
        let build = typed_rows(2, &[ScalarType::F64], build_keys.into_iter().map(|key| vec![key]).collect());
        let key = Expr::binary(BinaryOp::Eq, probe.schema().fields()[0].expression(), build.schema().fields()[0].expression());
        let query = join(probe, build, JoinKind::Anti, key);
        for threshold in [i64::MAX, 1] {
            let context = join_context(&CancellationToken::new(), threshold);
            let rows = collect(&query, &context).await.unwrap();
            let mut expected = expected_ids;
            if float_probe && threshold == 1 { expected.reverse(); }
            assert_eq!(rows.into_iter().map(|row| row[1].clone()).collect::<Vec<_>>(), expected.map(I64));
            assert_eq!(context.spill_peak_bytes() > 0, threshold == 1);
            assert_eq!(context.spill_bytes(), 0);
            assert_eq!(context.memory.used_bytes(), 0);
        }
    }
}

#[tokio::test]
async fn mixed_numeric_join_spill_keeps_signed_zero_fanout_in_candidate_order() {
    use Value::{F32, F64, I16, I64, Null};

    let build = typed_rows(0, &[ScalarType::F64, ScalarType::I64, ScalarType::I64], vec![
        vec![F64(-0.0), I64(-12), I64(10)],
        vec![F64(f64::from_bits(0x7ff8_0000_0000_0123)), I64(-12), I64(11)],
        vec![Null, I64(-12), I64(12)],
        vec![F64(0.0), I64(-12), I64(13)],
    ]);
    let RelationNode::Values { rows } = build.node() else { unreachable!() };
    let pages = rows.chunks(2).map(<[_]>::to_vec).collect();
    let build = Arc::new(Relation::new(build.schema().clone(), RelationNode::LiteralPages { pages }).unwrap());
    let probe = typed_rows(3, &[ScalarType::F32, ScalarType::I16, ScalarType::I64], vec![
        vec![F32(0.0), I16(-12), I64(20)],
        vec![F32(f32::NAN), I16(-12), I64(21)],
        vec![Null, I16(-12), I64(22)],
        vec![F32(-0.0), I16(-12), I64(23)],
        vec![F32(0.0), I16(-13), I64(24)],
    ]);
    let second_column = Expr::binary(BinaryOp::Eq, probe.schema().fields()[1].expression(), build.schema().fields()[1].expression());
    let first_column = Expr::binary(BinaryOp::Eq, build.schema().fields()[0].expression(), probe.schema().fields()[0].expression());
    let query = join(build, probe, JoinKind::Inner, Expr::binary(BinaryOp::And, second_column, first_column));

    for threshold in [i64::MAX, 1] {
        let context = join_context(&CancellationToken::new(), threshold);
        let rows = collect(&query, &context).await.unwrap();
        let ids = rows.into_iter().map(|row| (row[2].clone(), row[5].clone())).collect::<Vec<_>>();
        assert_eq!(ids, [(I64(10), I64(20)), (I64(13), I64(20)), (I64(10), I64(23)), (I64(13), I64(23))]);
        assert_eq!(context.spill_peak_bytes() > 0, threshold == 1);
        assert_eq!(context.spill_bytes(), 0);
        assert_eq!(context.memory.used_bytes(), 0);
    }
}

#[tokio::test]
async fn fallible_anti_equality_observes_build_order_before_a_matching_key() {
    let duration = |value, unit| Value::Duration { value, unit };
    for (build_values, errors) in [([i64::MAX, 1], true), ([1, i64::MAX], false)] {
        let probe = typed_rows(0, &[ScalarType::Duration(TimeUnit::Nanosecond)], vec![vec![duration(1_000_000_000, TimeUnit::Nanosecond)]]);
        let build = typed_rows(1, &[ScalarType::Duration(TimeUnit::Second)], build_values.into_iter().map(|value| vec![duration(value, TimeUnit::Second)]).collect());
        let key = Expr::binary(BinaryOp::Eq, probe.schema().fields()[0].expression(), build.schema().fields()[0].expression());
        let result = collect(&join(probe, build, JoinKind::Anti, key), &context(&CancellationToken::new())).await;
        if errors {
            assert_eq!(result.unwrap_err().code, "xtdb.expression/overflow-error");
        } else {
            assert!(result.unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn fallible_anti_residuals_observe_native_build_order_within_reached_partitions() {
    use rtdb_types::AnomalyCategory;
    use serde::Deserialize;

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(untagged)]
    enum Outcome {
        Rows { rows: Vec<i64> },
        Error { error: String },
    }

    #[derive(Deserialize)]
    struct Case {
        id: String,
        probe: (Option<i64>, i64, i64),
        build: Vec<(Option<i64>, i64)>,
        #[serde(default)]
        first_build_key_collides: bool,
        native: Outcome,
        native_spill: Option<Outcome>,
    }

    let fixture = include_str!("../../../conformance/fixtures/anti-join-contract.json");
    let cases: Vec<Case> = serde_json::from_str(fixture).unwrap();
    for case in cases {
        let (probe_key, probe_input, probe_id) = case.probe;
        let key_value = |key: Option<i64>| key.map_or(Value::Null, Value::I64);
        if case.first_build_key_collides {
            let build_key = case.build[0].0;
            assert_ne!(probe_key, build_key, "{}", case.id);
            assert_eq!(
                rtdb_columnar::hash_value(&key_value(probe_key), 0).unwrap(),
                rtdb_columnar::hash_value(&key_value(build_key), 0).unwrap(),
                "{}",
                case.id,
            );
        }

        let probe = typed_rows(
            0,
            &[ScalarType::I64, ScalarType::I64, ScalarType::I64],
            vec![vec![
                key_value(probe_key),
                Value::I64(probe_input),
                Value::I64(probe_id),
            ]],
        );
        let build = typed_rows(
            3,
            &[ScalarType::I64, ScalarType::I64],
            case.build
                .into_iter()
                .map(|(key, input)| vec![key_value(key), Value::I64(input)])
                .collect(),
        );
        let key = Expr::binary(
            BinaryOp::Eq,
            probe.schema().fields()[0].expression(),
            build.schema().fields()[0].expression(),
        );
        let denominator = Expr::binary(
            BinaryOp::Add,
            probe.schema().fields()[1].expression(),
            build.schema().fields()[1].expression(),
        );
        let quotient = Expr::binary(
            BinaryOp::Divide,
            Expr::Literal(Value::F64(1.0)),
            denominator,
        );
        let residual = Expr::binary(BinaryOp::Gt, quotient, Expr::Literal(Value::F64(0.0)));
        let query = join(
            probe,
            build,
            JoinKind::Anti,
            Expr::binary(BinaryOp::And, key, residual),
        );
        assert!(
            matches!(
                query.relation().node(),
                RelationNode::Join {
                    kind: JoinKind::Anti,
                    ..
                }
            ),
            "{}",
            case.id
        );

        for (spill, threshold) in [(false, i64::MAX), (true, 1)] {
            let label = format!("{} (spill={spill})", case.id);
            let context = join_context(&CancellationToken::new(), threshold);
            let outcome = match collect(&query, &context).await {
                Ok(rows) => Outcome::Rows {
                    rows: rows
                        .into_iter()
                        .map(|row| match row[2] {
                            Value::I64(id) => id,
                            ref other => panic!("{label}: unexpected probe id {other:?}"),
                        })
                        .collect(),
                },
                Err(error) => {
                    assert_eq!(
                        error.category,
                        AnomalyCategory::Incorrect,
                        "{label}: {error:?}"
                    );
                    Outcome::Error { error: error.code }
                }
            };
            let expected = if spill {
                case.native_spill.as_ref().unwrap_or(&case.native)
            } else {
                &case.native
            };
            assert_eq!(&outcome, expected, "{label}");
            assert_eq!(context.spill_peak_bytes() > 0, spill, "{label}");
            assert_eq!(context.spill_bytes(), 0, "{label}");
            assert_eq!(context.memory.used_bytes(), 0, "{label}");
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
async fn residual_conjuncts_keep_sparse_fanout_in_pair_order_and_exclude_unknown_matches() {
    let left = measured(0, &[(1, Some(1.0)), (1, None), (1, Some(3.0)), (1, Some(4.0)), (1, Some(2.0))]);
    let right = measured(2, &[(1, Some(1.0)), (2, Some(9.0)), (1, Some(2.0))]);
    let (lk, lv) = (left.schema().fields()[0].expression(), left.schema().fields()[1].expression());
    let (rk, rv) = (right.schema().fields()[0].expression(), right.schema().fields()[1].expression());
    let key = Expr::binary(BinaryOp::Eq, lk, rk);
    let differs = Expr::binary(BinaryOp::NotEq, lv.clone(), rv.clone());
    let missing = Expr::Unary { op: rtdb_plan::UnaryOp::IsNull, expr: Box::new(lv.clone()) };
    let delta = Expr::binary(BinaryOp::Subtract, lv, rv);
    let reciprocal = Expr::binary(BinaryOp::Divide, Expr::Literal(Value::F64(1.0)), delta.clone());
    let positive = Expr::binary(BinaryOp::Gt, reciprocal, Expr::Literal(Value::F64(0.0)));
    let bounded = Expr::binary(BinaryOp::Lt, delta, Expr::Literal(Value::F64(3.0)));
    let passes = Expr::binary(BinaryOp::Or, missing, Expr::binary(BinaryOp::And, positive, bounded));
    let condition = Expr::binary(BinaryOp::And, key, Expr::binary(BinaryOp::And, differs, passes));
    let context = context(&CancellationToken::new());
    let rows = collect(&join(left, right, JoinKind::Inner, condition), &context).await.unwrap();
    assert_eq!(rows.into_iter().map(|row| (row[1].clone(), row[3].clone())).collect::<Vec<_>>(), [
        (Value::F64(3.0), Value::F64(1.0)),
        (Value::F64(2.0), Value::F64(1.0)),
        (Value::F64(3.0), Value::F64(2.0)),
        (Value::F64(4.0), Value::F64(2.0)),
    ]);
    assert_eq!(context.spill_bytes(), 0);
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

#[tokio::test]
async fn float_join_residuals_distinguish_unordered_values_from_nulls_and_order_signed_zero() {
    let left = measured(0, &[(1, Some(-0.0)), (2, Some(f64::NAN)), (3, None), (4, Some(f64::NEG_INFINITY))]);
    let right = measured(2, &[(1, Some(0.0)), (2, Some(f64::NAN)), (3, Some(1.0)), (4, Some(f64::INFINITY))]);
    let expr = |op| Expr::binary(BinaryOp::And,
        Expr::binary(BinaryOp::Eq, left.schema().fields()[0].expression(), right.schema().fields()[0].expression()),
        Expr::binary(op, left.schema().fields()[1].expression(), right.schema().fields()[1].expression()),
    );
    let context = context(&CancellationToken::new());
    for (op, expected) in [(BinaryOp::NotEq, vec![2, 4]), (BinaryOp::Lt, vec![1, 4]), (BinaryOp::GtEq, vec![])] {
        let query = join(left.clone(), right.clone(), JoinKind::Inner, expr(op));
        let rows = collect(&query, &context).await.unwrap();
        let keys: Vec<_> = rows.into_iter().map(|row| row[0].clone()).collect();
        assert_eq!(keys, expected.into_iter().map(Value::I64).collect::<Vec<_>>(), "{op:?}");
    }
}

fn residual_input(id: u32, ty: ScalarType, payloads: Vec<Value>) -> Arc<Relation> {
    let fields = [
        ("key", LogicalType::Scalar(ScalarType::I64)),
        ("payload", LogicalType::Scalar(ty).maybe()),
        ("position", LogicalType::Scalar(ScalarType::I64)),
    ].into_iter().enumerate().map(|(offset, (name, data_type))| Field {
        id: ColumnId(id + offset as u32),
        name: format!("{name}{id}"),
        qualifier: None,
        qualified_only: false,
        unnamed: false,
        data_type,
    }).collect();
    let rows = payloads.into_iter().enumerate().map(|(position, payload)| {
        vec![Expr::Literal(Value::I64(1)), Expr::Literal(payload), Expr::Literal(Value::I64(position as i64))]
    }).collect();
    Arc::new(Relation::new(Schema::new(fields).unwrap(), RelationNode::Values { rows }).unwrap())
}

#[tokio::test]
async fn typed_join_residuals_keep_true_pairs_in_candidate_order() {
    let text = |value: &str| Value::Utf8(value.into());
    let timestamp = |value| Value::Timestamp { value, unit: TimeUnit::Second, timezone: Some("UTC".into()) };
    let time = |value| Value::Time { value, unit: TimeUnit::Millisecond, bit_width: 32 };
    let duration = |value| Value::Duration { value, unit: TimeUnit::Nanosecond };
    for (low, high, equal_to_high) in [
        (text("z"), text("é"), text("é")),
        (Value::Bool(false), Value::Bool(true), Value::Bool(true)),
        (Value::DateDay(-1), Value::DateDay(0), Value::DateDay(0)),
        (Value::DateMilli(-1), Value::DateMilli(0), Value::DateMilli(1)),
        (timestamp(i64::MIN), timestamp(i64::MAX), timestamp(i64::MAX)),
        (time(0), time(100), time(100)),
        (duration(-1), duration(0), duration(0)),
        (Value::IntervalYearMonth(2), Value::IntervalYearMonth(3), Value::IntervalYearMonth(3)),
    ] {
        let LogicalType::Scalar(ty) = low.logical_type() else { unreachable!() };
        let left = residual_input(0, ty.clone(), vec![high.clone(), low.clone(), high, Value::Null]);
        let right = residual_input(3, ty.clone(), vec![low.clone(), Value::Null, low]);
        let key = Expr::binary(BinaryOp::Eq, left.schema().fields()[0].expression(), right.schema().fields()[0].expression());
        let left_value = left.schema().fields()[1].expression();
        let right_value = right.schema().fields()[1].expression();
        let greater = Expr::binary(BinaryOp::Gt, left_value.clone(), right_value);
        let matches_literal = Expr::binary(BinaryOp::Eq, left_value, Expr::Literal(equal_to_high));
        let condition = Expr::binary(BinaryOp::And, key, Expr::binary(BinaryOp::And, greater, matches_literal));
        let query = join(left, right, JoinKind::Inner, condition);
        for threshold in [i64::MAX, 1] {
            let context = join_context(&CancellationToken::new(), threshold);
            let rows = collect(&query, &context).await.unwrap();
            let positions: Vec<_> = rows.into_iter().map(|row| (row[2].clone(), row[5].clone())).collect();
            let expected = [(0, 0), (2, 0), (0, 2), (2, 2)].map(|(left, right)| (Value::I64(left), Value::I64(right)));
            assert_eq!(positions, expected, "{ty:?}, spill threshold {threshold}");
            assert_eq!(context.memory.used_bytes(), 0);
            assert_eq!(context.spill_bytes(), 0);
        }
    }
}

#[tokio::test]
async fn mixed_temporal_residuals_rescale_only_for_reached_candidates() {
    let timestamp = |value, unit| Value::Timestamp { value, unit, timezone: None };
    let ty = |unit| ScalarType::Timestamp { unit, timezone: None };
    let left = residual_input(0, ty(TimeUnit::Second), vec![timestamp(1, TimeUnit::Second)]);
    let right = residual_input(3, ty(TimeUnit::Microsecond), vec![timestamp(1_000_000, TimeUnit::Microsecond)]);
    let condition = |left: &Relation, right: &Relation, residual| {
        let key = Expr::binary(BinaryOp::Eq, left.schema().fields()[0].expression(), right.schema().fields()[0].expression());
        Expr::binary(BinaryOp::And, key, residual)
    };
    let differs = Expr::binary(BinaryOp::NotEq, left.schema().fields()[1].expression(), right.schema().fields()[1].expression());
    let context = context(&CancellationToken::new());
    let predicate = condition(&left, &right, differs);
    assert!(collect(&join(left, right.clone(), JoinKind::Inner, predicate), &context).await.unwrap().is_empty());

    let left = residual_input(0, ty(TimeUnit::Second), vec![timestamp(i64::MAX, TimeUnit::Second)]);
    for (guard, reached) in [(Value::Bool(false), false), (Value::Null, true)] {
        let text = Expr::binary(BinaryOp::Gt, Expr::Literal(Value::Utf8("é".into())), Expr::Literal(Value::Utf8("z".into())));
        let guard = Expr::binary(BinaryOp::Eq, text, Expr::Literal(guard));
        let later = Expr::binary(BinaryOp::Gt, left.schema().fields()[1].expression(), right.schema().fields()[1].expression());
        let predicate = condition(&left, &right, Expr::binary(BinaryOp::And, guard, later));
        let rows = collect(&join(left.clone(), right.clone(), JoinKind::Inner, predicate), &context).await;
        if reached {
            assert_eq!(rows.unwrap_err().code, "xtdb.expression/overflow-error");
        } else {
            assert!(rows.unwrap().is_empty());
        }
        assert_eq!(context.memory.used_bytes(), 0);
    }
}

#[tokio::test]
async fn count_of_changed_float_values_in_a_temporal_self_join_matches_across_spill() {
    use rtdb_index::{PartitionState, TransactionBuffer};
    use rtdb_plan::{AggregateExpr, AggregateFunction, Catalog, QualifiedTable, Scan, TemporalFilter};
    use rtdb_trie::TrieConfig;
    use rtdb_types::{Interval, StructFields, TableRef, TransactionKey, TxId};

    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let memory = MemoryBudget::new(1 << 26);
    let table = TableRef::new("public", "accounts");
    for (version, rows) in [
        (1, vec![(1, Some(1.0)), (2, Some(-0.0)), (3, None), (4, Some(4.0))]),
        (2, vec![(1, Some(2.0)), (2, Some(0.0)), (3, Some(3.0)), (5, Some(5.0))]),
    ] {
        let mut tx = TransactionBuffer::new(state.snapshot(), TransactionKey {
            tx_id: TxId(version), system_time: Timestamp(version * 10),
        }, memory.clone()).unwrap();
        for (id, value) in rows {
            tx.put(table.clone(), StructFields::from([
                ("_id".into(), Value::I64(id)), ("balance".into(), value.map_or(Value::Null, Value::F64)),
            ]), Interval::ALL).unwrap();
        }
        state.apply(&tx.stage().unwrap()).unwrap();
    }
    let database = DatabaseSnapshot { partitions: vec![state.snapshot()], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    let binding = catalog.table(&QualifiedTable { database: "xtdb".into(), table }).unwrap().unwrap();
    let scan = |first, system_time| {
        let columns = ["_id", "balance"].into_iter().enumerate().map(|(offset, name)| Field {
            id: ColumnId(first + offset as u32), name: name.into(), qualifier: None, qualified_only: false, unnamed: false,
            data_type: binding.columns[name].clone(),
        }).collect();
        Arc::new(Relation::scan(Scan { table: binding.clone(), columns, valid_time: TemporalFilter::Default, system_time,
            clamp_valid_time: false, iids: None, predicates: vec![], top_k: None, ordered_top_k: None, matches: vec![],
        }).unwrap())
    };
    let current = scan(0, TemporalFilter::Default);
    let past = scan(2, TemporalFilter::At(Expr::Literal(Value::Timestamp {
        value: 10, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()),
    })));
    let key = Expr::binary(BinaryOp::Eq, current.schema().fields()[0].expression(), past.schema().fields()[0].expression());
    let changed = Expr::binary(BinaryOp::NotEq, current.schema().fields()[1].expression(), past.schema().fields()[1].expression());
    let input = Arc::new(Relation::join(current, past, JoinKind::Inner, Some(Expr::binary(BinaryOp::And, key, changed))).unwrap());
    let field = Field { id: ColumnId(4), name: "count".into(), qualifier: None, qualified_only: false, unnamed: false,
        data_type: LogicalType::Scalar(ScalarType::I64) };
    let aggregate = AggregateExpr { field: field.clone(), function: AggregateFunction::CountStar, argument: None,
        distinct: false, filter: None, order_by: vec![] };
    let query = PreparedQuery::new(Arc::new(Relation::new(Schema::new(vec![field]).unwrap(), RelationNode::Aggregate {
        input, groups: vec![], aggregates: vec![aggregate],
    }).unwrap())).unwrap();
    for join_build_rows in [i64::MAX, 1] {
        let context = QueryContext::new(catalog.clone(), memory.clone(), Timestamp(30), "UTC", QueryOptions {
            join_build_rows, ..Default::default()
        }, &CancellationToken::new()).unwrap();
        assert_eq!(collect(&query, &context).await.unwrap(), [vec![Value::I64(1)]]);
        assert_eq!(context.spill_peak_bytes() > 0, join_build_rows == 1);
        assert_eq!(context.spill_bytes(), 0);
    }
}

#[tokio::test]
async fn hashed_probe_reads_the_selected_keys_of_another_join_in_output_order() {
    let build = measured(0, &[(2, Some(20.0)), (1, Some(10.0)), (2, Some(21.0))]);
    let probe = numbers(2, "probe_key", &[2, 1]);
    let condition = Expr::binary(BinaryOp::Eq, build.schema().fields()[0].expression(), probe.schema().fields()[0].expression());
    let selected = Arc::new(Relation::join(build, probe, JoinKind::Inner, Some(condition)).unwrap());
    let limited = Arc::new(Relation::new(selected.schema().clone(), RelationNode::Limit {
        input: selected, offset: None, fetch: Some(Expr::Literal(Value::I64(3))),
    }).unwrap());
    let build = numbers(3, "second_build_key", &[1, 2]);
    let condition = Expr::binary(BinaryOp::Eq, build.schema().fields()[0].expression(), limited.schema().fields()[2].expression());
    let query = join(build, limited, JoinKind::Inner, condition);

    let rows = collect(&query, &context(&CancellationToken::new())).await.unwrap();
    assert_eq!(rows, [
        vec![Value::I64(2), Value::I64(2), Value::F64(20.0), Value::I64(2)],
        vec![Value::I64(2), Value::I64(2), Value::F64(21.0), Value::I64(2)],
        vec![Value::I64(1), Value::I64(1), Value::F64(10.0), Value::I64(1)],
    ]);
}
