use rtdb_types::StructFields;
use rtdb_expr::*;
use rtdb_plan::{BinaryOp as B, Expr, Schema};
use rtdb_types::{DecimalValue, LogicalType as T, ScalarType as S, TimeUnit, Timestamp, Value as V};

fn expr(op: B, left: V, right: V) -> Expr { Expr::binary(op, Expr::Literal(left), Expr::Literal(right)) }
fn run(expr: &Expr) -> V { evaluate(expr, &Schema::default(), &[], &EvalContext::default()).unwrap() }
fn decimal(coefficient: i128, scale: i32) -> V {
    V::Decimal(DecimalValue::new(coefficient.to_be_bytes().to_vec(), 32, scale, 128).unwrap())
}

#[test]
fn pg_format_type_null_modifier_and_oid_dispatch() {
    assert_eq!(scalar_call("format_type", &[V::I32(20), V::Null], &EvalContext::default()).unwrap(), V::Utf8("bigint".into()));
    assert_eq!(scalar_call("format_type", &[V::Oid(1043), V::I32(24)], &EvalContext::default()).unwrap(), V::Utf8("character varying(20)".into()));
    assert_eq!(scalar_call("format_type", &[V::Null, V::I32(-1)], &EvalContext::default()).unwrap(), V::Null);
    assert!(scalar_call("format_type", &[V::Utf8("20".into()), V::Null], &EvalContext::default()).is_err());
}

#[test]
fn patch_keeps_upstream_test_patch_observations() {
    let doc = |fields: Vec<(&str, V)>| V::Struct(fields.into_iter().map(|(key, value)| (key.into(), value)).collect());
    let a = |value: V| doc(vec![("a", value)]);
    for (left, right, expected) in [
        (doc(vec![]), doc(vec![]), doc(vec![])),
        (V::Null, a(V::I64(1)), a(V::I64(1))),
        (a(V::I64(2)), a(V::I64(1)), a(V::I64(1))),
        (doc(vec![("a", V::I64(2)), ("b", V::I64(2))]), a(V::I64(1)), doc(vec![("a", V::I64(1)), ("b", V::I64(2))])),
        (a(V::I64(2)), doc(vec![("a", V::I64(1)), ("b", V::I64(1))]), doc(vec![("a", V::I64(1)), ("b", V::I64(1))])),
        (a(V::I64(2)), doc(vec![]), a(V::I64(2))),
        (a(V::F64(2.4)), a(V::I64(1)), a(V::I64(1))),
        (a(V::I64(1)), a(V::F64(2.4)), a(V::F64(2.4))),
        (a(V::F64(1.0)), a(V::F64(2.4)), a(V::F64(2.4))),
        (a(V::I64(1)), a(V::Null), a(V::I64(1))),
        (a(V::I64(3)), a(V::Utf8("3".into())), a(V::Utf8("3".into()))),
    ] {
        assert_eq!(scalar_call("_patch", &[left, right], &EvalContext::default()).unwrap(), expected);
    }
}

#[test]
fn patch_replaces_nested_values_and_preserves_nullable_field_types() {
    let left = V::Struct(StructFields::from([("nested".into(), V::Struct(StructFields::from([("a".into(), V::I64(1))])))]));
    let right = V::Struct(StructFields::from([("nested".into(), V::Struct(StructFields::from([("b".into(), V::I64(2))])))]));
    assert_eq!(scalar_call("_patch", &[left, right.clone()], &EvalContext::default()).unwrap(), right);
    let left = T::Struct(StructFields::from([("a".into(), T::Scalar(S::I64)), ("keep".into(), T::Scalar(S::Bool))]));
    let right = T::Struct(StructFields::from([("a".into(), T::Scalar(S::Utf8).maybe()), ("new".into(), T::Null)]));
    let expected = T::Struct(StructFields::from([("a".into(), T::Scalar(S::I64).join(&T::Scalar(S::Utf8))), ("keep".into(), T::Scalar(S::Bool)), ("new".into(), T::Null)]));
    assert_eq!(function_type("_patch", &[left, right]).unwrap(), expected);
    assert!(function_type("_patch", &[T::Null, T::Null]).is_err());
}

#[test]
fn fixed_timezone_spellings_normalize_like_java_zone_id() {
    for (input, expected) in [("Z", "Z"), ("+1", "+01:00"), ("-01", "-01:00"), ("+0130", "+01:30"),
        ("+01:30", "+01:30"), ("+013015", "+01:30:15"), ("-01:30:15", "-01:30:15"),
        ("UTC+1", "UTC+01:00"), ("GMT-00", "GMT"), ("UT+00:30", "UT+00:30"), ("-00", "Z"),
        ("+18:00", "+18:00"), ("Europe/Paris", "Europe/Paris"), ("UTC", "UTC")] {
        assert_eq!(normalize_timezone(input).unwrap(), expected);
    }
    for invalid in ["+18:00:01", "-18:01", "+19", "+1:30", "+01:60", "+01:00:60", "UTCZ", "GMT++01", "+٠١", "not-a-zone"] {
        assert_eq!(normalize_timezone(invalid).unwrap_err().code, "xtdb/invalid-timezone", "{invalid}");
    }
}

#[test]
fn fixed_timezones_apply_to_local_timestamp_casts_and_current_time() {
    assert_eq!(parse_timestamp("1970-01-01T00:00:00", "+02:00").unwrap(), Timestamp(-7_200_000_000));
    assert_eq!(parse_timestamp("1970-01-01T00:00:00", "GMT-01:30:15").unwrap(), Timestamp(5_415_000_000));
    assert_eq!(parse_timestamp("1970-01-01T00:00:00", "Z").unwrap(), Timestamp(0));
    let context = EvalContext { timezone: "UTC+02:30", ..Default::default() };
    let value = scalar_call("localtimestamp", &[], &context).unwrap();
    assert_eq!(value, V::Timestamp { value: context.current_time.as_micros().unwrap().0 + 9_000_000_000, unit: TimeUnit::Microsecond, timezone: None });
}

#[test]
fn and_or_keep_all_nine_three_valued_truth_table_cells() {
    let values = [V::Bool(false), V::Bool(true), V::Null];
    let and = [V::Bool(false), V::Bool(false), V::Bool(false), V::Bool(false), V::Bool(true), V::Null, V::Bool(false), V::Null, V::Null];
    let or = [V::Bool(false), V::Bool(true), V::Null, V::Bool(true), V::Bool(true), V::Bool(true), V::Null, V::Bool(true), V::Null];
    for (i, left) in values.iter().enumerate() {
        for (j, right) in values.iter().enumerate() {
            assert_eq!(run(&expr(B::And, left.clone(), right.clone())), and[3 * i + j]);
            assert_eq!(run(&expr(B::Or, left.clone(), right.clone())), or[3 * i + j]);
        }
    }
}

#[test]
fn short_circuit_and_case_do_not_evaluate_unselected_division() {
    let failure = expr(B::Divide, V::I64(1), V::I64(0));
    assert_eq!(run(&Expr::binary(B::And, Expr::Literal(V::Bool(false)), failure.clone())), V::Bool(false));
    assert_eq!(run(&Expr::binary(B::Or, Expr::Literal(V::Bool(true)), failure.clone())), V::Bool(true));
    assert_eq!(run(&Expr::Case { branches: vec![(Expr::Literal(V::Bool(true)), Expr::Literal(V::I64(7)))], otherwise: Box::new(failure) }), V::I64(7));
}

#[test]
fn coalesce_preserves_the_selected_union_leg_and_nullif_observes_null() {
    assert_eq!(run(&Expr::Coalesce(vec![Expr::Literal(V::Null), Expr::Literal(V::Utf8("lit".into())), Expr::Literal(V::I64(10))])), V::Utf8("lit".into()));
    assert_eq!(scalar_call("nullif", &[V::I64(1), V::I64(1)], &EvalContext::default()).unwrap(), V::Null);
    assert_eq!(scalar_call("nullif", &[V::I64(1), V::Null], &EvalContext::default()).unwrap(), V::I64(1));
}

#[test]
fn integer_comparison_does_not_round_through_f64() {
    assert_eq!(sql_eq(&V::I64(i64::MAX), &V::I64(i64::MAX - 1), &equality_scope()).unwrap(), Some(false));
    assert_eq!(compare(&V::I64(i64::MAX), &V::I64(i64::MAX - 1), false).unwrap(), std::cmp::Ordering::Greater);
    assert_eq!(sql_eq(&decimal(i64::MAX as i128, 0), &V::I64(i64::MAX - 1), &equality_scope()).unwrap(), Some(false));
}

#[test]
fn numeric_errors_preserve_division_and_overflow_diagnostics() {
    for left in [V::I64(5), V::F64(5.0)] {
        for right in [V::I64(0), V::F64(0.0)] { assert!(arithmetic(B::Divide, &left, &right).unwrap_err().message.as_deref().is_some_and(|message| message.contains("division by zero"))); }
    }
    assert!(arithmetic(B::Add, &V::I64(i64::MAX), &V::I64(i64::MAX)).unwrap_err().message.as_deref().is_some_and(|message| message.contains("overflow")));
    assert!(arithmetic(B::Multiply, &V::I32(i32::MIN), &V::I32(2)).is_err());
    assert_eq!(arithmetic(B::Divide, &V::I64(7), &V::I64(2)).unwrap(), V::I64(3));
}

#[test]
fn modulo_uses_the_divisors_sign() {
    for (left, right, expected) in [(-5, 3, 1), (5, -3, -1), (-5, -3, -2), (5, 3, 2)] {
        assert_eq!(arithmetic(B::Modulo, &V::I64(left), &V::I64(right)).unwrap(), V::I64(expected));
    }
}

#[test]
fn shifts_keep_the_left_type_and_wrap_its_width() {
    assert_eq!(arithmetic(B::ShiftLeft, &V::I8(127), &V::I64(1)).unwrap(), V::I8(-2));
    assert_eq!(arithmetic(B::ShiftRight, &V::I16(-4), &V::I64(1)).unwrap(), V::I16(-2));
    assert_eq!(arithmetic(B::ShiftLeft, &V::I32(3), &V::I64(64)).unwrap(), V::I32(3));
    assert_eq!(expr(B::ShiftLeft, V::I8(127), V::I64(1)).data_type(), T::Scalar(S::I8));
}

#[test]
fn decimal_arithmetic_retains_reference_precision_scale_and_mixed_integer_promotion() {
    assert_eq!(arithmetic(B::Add, &decimal(101, 2), &decimal(10, 1)).unwrap(), decimal(201, 2));
    assert_eq!(arithmetic(B::Multiply, &decimal(10, 1), &decimal(20, 1)).unwrap(), decimal(200, 2));
    assert_eq!(arithmetic(B::Divide, &decimal(10, 1), &decimal(20, 1)).unwrap(), decimal(500000, 6));
    assert_eq!(arithmetic(B::Multiply, &V::I8(1), &decimal(20, 1)).unwrap(), decimal(20, 1));
    assert_eq!(arithmetic(B::Add, &V::F32(1.0), &decimal(20, 1)).unwrap(), V::F32(3.0));
    let expression = expr(B::Multiply, decimal(10, 1), decimal(20, 1));
    assert_eq!(expression.data_type(), decimal(200, 2).logical_type());
}

#[test]
fn substring_keeps_unicode_and_outside_range_cases() {
    for (text, pos, length, expected) in [
        ("", -1, 0, ""), ("", 0, 0, ""), ("", 1, 0, ""), ("", 1, 1, ""), ("", 1, 2, ""),
        ("a", -1, 0, ""), ("a", -1, 1, ""), ("a", -1, 2, ""), ("a", -1, 3, "a"),
        ("a", 1, 0, ""), ("a", 1, 1, "a"), ("a", 1, 2, "a"),
        ("🌝😎😎", 2, 2, "😎😎"), ("f😎😎bar", 1, 1, "f"), ("f😎😎bar", 2, 1, "😎"), ("f😎😎bar", 2, 100, "😎😎bar"),
        ("1234567890", 3, 8, "34567890"), ("1234567890", 4, 3, "456"), ("string", 2, 2147483646, "tring"), ("string", -10, 2147483646, "string"),
    ] {
        assert_eq!(scalar_call("substring", &[V::Utf8(text.into()), V::I64(pos), V::I64(length)], &EvalContext::default()).unwrap(), V::Utf8(expected.into()));
    }
    assert!(scalar_call("substring", &[V::Utf8("".into()), V::I64(0), V::I64(-1)], &EvalContext::default()).unwrap_err().message.as_deref().is_some_and(|message| message.contains("Negative substring length")));
}

#[test]
fn character_length_counts_code_points_and_like_does_not_match_newline() {
    assert_eq!(scalar_call("character_length", &[V::Utf8("😀".into())], &EvalContext::default()).unwrap(), V::I32(1));
    let like = Expr::Like { expr: Box::new(Expr::Literal(V::Utf8("a\nb".into()))), pattern: Box::new(Expr::Literal(V::Utf8("a%b".into()))), escape: None, case_insensitive: false, negated: false };
    assert_eq!(run(&like), V::Bool(false));
}

#[test]
fn missing_members_and_nulls_do_not_become_boolean_false() {
    let a = V::Struct(StructFields::from([("a".into(), V::I64(1))]));
    let b = V::Struct(StructFields::from([("a".into(), V::I64(1)), ("b".into(), V::Null)]));
    assert_eq!(sql_eq(&a, &b, &equality_scope()).unwrap(), Some(false));
    assert_eq!(sql_eq(&b, &b, &equality_scope()).unwrap(), None);
    assert!(!is_true(&V::Null).unwrap());
    assert_eq!(sql_eq(&V::list(vec![V::I64(1), V::Null]), &V::list(vec![V::I64(1), V::Null]), &equality_scope()).unwrap(), None);
}

#[test]
fn wrong_arity_is_an_anomaly_before_any_argument_indexing() {
    for (name, args) in [("null_eq", vec![V::Null]), ("replace", vec![V::Utf8("a".into())]), ("power", vec![]), ("substring", vec![V::Null])] {
        assert_eq!(scalar_call(name, &args, &EvalContext::default()).unwrap_err().code, "xtdb/invalid-function-arity");
    }
}

#[test]
fn timestamp_conversion_floors_negative_submicrosecond_values_and_resolves_dst() {
    assert_eq!(timestamp_micros(&V::Timestamp { value: -1, unit: TimeUnit::Nanosecond, timezone: Some("UTC".into()) }, "UTC").unwrap(), Timestamp(-1));
    assert_eq!(parse_timestamp("2024-03-31 02:30:00", "Europe/Paris").unwrap(), parse_timestamp("2024-03-31T01:30:00Z", "UTC").unwrap());
    assert_eq!(parse_timestamp("2024-10-27 02:30:00", "Europe/Paris").unwrap(), parse_timestamp("2024-10-27T00:30:00Z", "UTC").unwrap());
    assert_eq!(cast(&V::Utf8("42".into()), &T::Scalar(S::I64), "UTC").unwrap(), V::I64(42));
}

#[test]
fn period_null_endpoints_are_unbounded_and_empty_periods_are_rejected() {
    let context = EvalContext::default();
    let whole = scalar_call("period", &[V::Null, V::Null], &context).unwrap();
    assert_eq!(whole, V::TimestampTzRange { lower: i64::MIN, upper: i64::MAX });
    assert_eq!(scalar_call("upper", &[whole.clone()], &context).unwrap(), V::Null);
    assert_eq!(scalar_call("lower_inf", &[whole], &context).unwrap(), V::Bool(true));
    let at = V::Timestamp { value: 1, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) };
    assert_eq!(scalar_call("period", &[at.clone(), at], &context).unwrap_err().code, "xtdb/invalid-period");
}

#[test]
fn period_constructor_accepts_date_time_pairs_and_preserves_asymmetric_null_dispatch() {
    let context = EvalContext::default();
    assert_eq!(scalar_call("period", &[V::DateDay(0), V::DateDay(1)], &context).unwrap(), V::TimestampTzRange { lower: 0, upper: 86_400_000_000 });
    assert_eq!(scalar_call("period", &[V::DateDay(0), V::Null], &context).unwrap(), V::TimestampTzRange { lower: 0, upper: i64::MAX });
    for to in [V::DateDay(1), V::Timestamp { value: 1, unit: TimeUnit::Microsecond, timezone: None }] {
        assert_eq!(scalar_call("period", &[V::Null, to], &context).unwrap_err().code, "xtdb.expression/function-type-mismatch");
    }
    assert_eq!(scalar_call("period", &[V::Null, V::DateDay(1)], &context).unwrap_err().message.as_deref(), Some("period not applicable to types null and date"));
    assert_eq!(scalar_call("period", &[V::Utf8("2020-01-01".into()), V::Null], &context).unwrap_err().code, "xtdb.expression/function-type-mismatch");
}

#[test]
fn period_constructor_truncates_zoned_timestamp_units_and_floors_local_conversion() {
    let context = EvalContext::default();
    let upper = V::Timestamp { value: 1_000, unit: TimeUnit::Nanosecond, timezone: Some("UTC".into()) };
    for (timezone, lower) in [(Some("UTC".into()), 0), (None, -1)] {
        let from = V::Timestamp { value: -1, unit: TimeUnit::Nanosecond, timezone };
        assert_eq!(scalar_call("period", &[from, upper.clone()], &context).unwrap(), V::TimestampTzRange { lower, upper: 1 });
    }
}

#[test]
fn distinct_from_handles_outer_nulls_and_retains_compound_unknown_comparisons() {
    assert_eq!(run(&expr(B::IsDistinctFrom, V::Null, V::Null)), V::Bool(false));
    assert_eq!(run(&expr(B::IsDistinctFrom, V::Null, V::I64(1))), V::Bool(true));
    let compound = V::Struct(StructFields::from([("a".into(), V::Null)]));
    let comparison = expr(B::IsDistinctFrom, compound.clone(), compound);
    assert_eq!(run(&comparison), V::Null);
    assert!(comparison.data_type().nullable());
}

#[test]
fn null_eq_keeps_reference_nonreflexive_nested_null_comparison() {
    let value = V::Struct(StructFields::from([("nested".into(), V::list(vec![V::Null, V::I64(4)]))]));
    assert_eq!(sql_eq(&value, &value, &equality_scope()).unwrap(), None);
    assert!(!null_eq(&value, &value, &equality_scope()).unwrap());
}

#[test]
fn scalar_comparisons_and_not_preserve_nonnullable_boolean_schemas() {
    let boolean = T::Scalar(S::Bool);
    assert_eq!(expr(B::Eq, V::I64(1), V::I64(2)).data_type(), boolean);
    assert_eq!(expr(B::And, V::Bool(true), V::Bool(false)).data_type(), boolean);
    assert_eq!(Expr::unary(rtdb_plan::UnaryOp::Not, Expr::Literal(V::Bool(true))).data_type(), boolean);
    assert_eq!(expr(B::Eq, V::Null, V::I64(2)).data_type(), T::Null);
    assert_eq!(Expr::binary(B::Eq, Expr::column(rtdb_plan::ColumnId(0), T::Scalar(S::I64).maybe()), Expr::Literal(V::I64(2))).data_type(), boolean.clone().maybe());
    assert_eq!(expr(B::Eq, V::list(vec![V::I64(1)]), V::list(vec![V::I64(1)])).data_type(), boolean.maybe());
}

#[test]
fn batch_evaluation_retains_unobserved_union_legs_and_empty_batch_type() {
    use std::sync::Arc;
    use arrow_array::{ArrayRef, BooleanArray, RecordBatch};
    use rtdb_columnar::{MemoryBudget, from_field, read_value, to_field};
    use rtdb_plan::{ColumnId, Field};

    let ty = T::Scalar(S::Bool);
    let schema = Schema::new(vec![Field { id: ColumnId(0), name: "flag".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: ty.clone() }]).unwrap();
    let expression = Expr::Case { branches: vec![(Expr::column(ColumnId(0), ty.clone()), Expr::Literal(V::I64(1)))], otherwise: Box::new(Expr::Literal(V::Utf8("fallback".into()))) };
    let compiled = CompiledExpr::compile(&expression, &schema).unwrap();
    let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![to_field("flag", &ty).unwrap()]));
    let budget = MemoryBudget::new(1 << 20);
    for values in [vec![true, true], Vec::new()] {
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(BooleanArray::from(values.clone())) as ArrayRef]).unwrap();
        let output = evaluate_batch(&compiled, &batch, &EvalContext::default(), &budget).unwrap();
        assert_eq!(from_field(output.batch().schema().field(0)).unwrap(), expression.data_type());
        assert_eq!(output.batch().num_rows(), values.len());
        if !values.is_empty() { assert_eq!(read_value(output.batch().schema().field(0), output.batch().column(0).as_ref(), 0).unwrap(), V::I64(1)); }
    }
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn temporal_casts_preserve_nanoseconds_and_truncate_direct_unit_conversions() {
    let nanos = T::Scalar(S::Timestamp { unit: TimeUnit::Nanosecond, timezone: Some("UTC".into()) });
    let micros = T::Scalar(S::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) });
    assert_eq!(cast(&V::Utf8("1970-01-01T00:00:00.000000007Z".into()), &nanos, "UTC").unwrap(), V::Timestamp { value: 7, unit: TimeUnit::Nanosecond, timezone: Some("UTC".into()) });
    let before_epoch = V::Timestamp { value: -1, unit: TimeUnit::Nanosecond, timezone: Some("Europe/Paris".into()) };
    assert_eq!(cast(&before_epoch, &nanos, "UTC").unwrap(), V::Timestamp { value: -1, unit: TimeUnit::Nanosecond, timezone: Some("UTC".into()) });
    assert_eq!(cast(&before_epoch, &micros, "UTC").unwrap(), V::Timestamp { value: 0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) });
    assert_eq!(cast(&V::Time { value: 123456789, unit: TimeUnit::Nanosecond, bit_width: 64 }, &T::Scalar(S::Time { unit: TimeUnit::Microsecond, bit_width: 64 }), "UTC").unwrap(), V::Time { value: 123456, unit: TimeUnit::Microsecond, bit_width: 64 });
    assert_eq!(cast(&V::Duration { value: -1, unit: TimeUnit::Nanosecond }, &T::Scalar(S::Duration(TimeUnit::Microsecond)), "UTC").unwrap(), V::Duration { value: 0, unit: TimeUnit::Microsecond });
}

#[test]
fn pre_epoch_casts_between_timestamp_types_floor_to_the_coarser_unit() {
    for (source_timezone, target_timezone) in [(None, Some("UTC".into())), (Some("UTC".into()), None)] {
        let value = V::Timestamp { value: -1, unit: TimeUnit::Nanosecond, timezone: source_timezone };
        let target = T::Scalar(S::Timestamp { unit: TimeUnit::Microsecond, timezone: target_timezone.clone() });
        assert_eq!(cast(&value, &target, "UTC").unwrap(), V::Timestamp { value: -1, unit: TimeUnit::Microsecond, timezone: target_timezone });
    }
}

fn equality_scope() -> rtdb_expr::EvaluationScope {
    rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap()
}
