use std::cmp::Ordering;
use rtdb_expr::{compare, compare_in_timezone, evaluate, function_type, scalar_call, sql_eq, sql_eq_in_timezone, strict_eq, EvalContext};
use rtdb_plan::{BinaryOp as B, Expr, Schema};
use rtdb_types::{LogicalType, TimeUnit as U, Value as V};

fn timestamp(value: i64, unit: U, timezone: Option<&str>) -> V {
    V::Timestamp { value, unit, timezone: timezone.map(str::to_owned) }
}
fn binary(op: B, left: V, right: V, timezone: &str) -> V {
    evaluate(&Expr::binary(op, Expr::Literal(left), Expr::Literal(right)), &Schema::default(), &[], &EvalContext { timezone, ..Default::default() }).unwrap()
}

#[test]
fn date_and_timestamp_equality_uses_session_zone_and_floored_date_millis() {
    let local = timestamp(0, U::Microsecond, None);
    let instant = timestamp(-7_200, U::Second, Some("Pacific/Honolulu"));
    for date in [V::DateDay(0), V::DateMilli(0), V::DateMilli(86_399_999)] {
        assert_eq!(sql_eq_in_timezone(&date, &local, "+02:00", &equality_scope()).unwrap(), Some(true));
        assert_eq!(sql_eq_in_timezone(&date, &instant, "+02:00", &equality_scope()).unwrap(), Some(true));
        assert_eq!(compare_in_timezone(&instant, &date, false, "+02:00").unwrap(), Ordering::Equal);
        assert_eq!(sql_eq(&date, &instant, &equality_scope()).unwrap(), Some(false));
    }
    assert_eq!(sql_eq(&V::DateDay(-2), &V::DateMilli(-86_400_001), &equality_scope()).unwrap(), Some(true));
    assert_eq!(compare(&V::DateMilli(-1), &V::DateMilli(1), false).unwrap(), Ordering::Less);
    assert_eq!(sql_eq_in_timezone(&V::DateDay(0), &timestamp(18_000, U::Second, Some("UTC")), "America/New_York", &equality_scope()).unwrap(), Some(true));
    assert_eq!(sql_eq(&timestamp(1, U::Second, Some("UTC")), &timestamp(1_000_000, U::Microsecond, Some("Asia/Tokyo")), &equality_scope()).unwrap(), Some(true));
}

#[test]
fn temporal_units_check_overflow_before_comparison() {
    for (left, right) in [
        (timestamp(i64::MAX, U::Second, None), timestamp(0, U::Nanosecond, None)),
        (V::Duration { value: i64::MAX, unit: U::Second }, V::Duration { value: 0, unit: U::Microsecond }),
        (V::Time { value: i64::MAX, unit: U::Second, bit_width: 64 }, V::Time { value: 0, unit: U::Microsecond, bit_width: 64 }),
        (V::DateDay(i32::MAX), timestamp(0, U::Microsecond, None)),
    ] {
        assert_eq!(compare(&left, &right, false).unwrap_err().code, "xtdb/numeric-value-out-of-range");
        assert_eq!(sql_eq(&left, &right, &equality_scope()).unwrap_err().code, "xtdb/numeric-value-out-of-range");
    }
    assert_eq!(compare(&timestamp(i64::MAX, U::Second, Some("UTC")), &timestamp(i64::MIN, U::Second, Some("UTC")), false).unwrap(), Ordering::Greater);
    assert!(compare_in_timezone(&timestamp(i64::MAX, U::Second, Some("UTC")), &timestamp(0, U::Second, None), false, "+01:00").is_err());
}

#[test]
fn temporal_inequality_negates_equality_and_preserves_nested_unknown() {
    let second = timestamp(1, U::Second, Some("UTC"));
    let microsecond = timestamp(1, U::Microsecond, Some("UTC"));
    let same_instant = timestamp(1_000_000, U::Microsecond, Some("Asia/Tokyo"));
    let time = |value, unit, bit_width| V::Time { value, unit, bit_width };
    for (left, right, equal) in [
        (second.clone(), microsecond, false),
        (second.clone(), same_instant.clone(), true),
        (V::DateDay(0), timestamp(-7_200, U::Second, Some("UTC")), true),
        (time(1, U::Second, 32), time(1_000, U::Millisecond, 32), true),
        (V::Duration { value: 1, unit: U::Second }, V::Duration { value: 1_000, unit: U::Millisecond }, true),
    ] {
        assert_eq!(binary(B::Eq, left.clone(), right.clone(), "+02:00"), V::Bool(equal));
        assert_eq!(binary(B::NotEq, left.clone(), right.clone(), "+02:00"), V::Bool(!equal));
        assert_eq!(binary(B::IsDistinctFrom, left.clone(), right.clone(), "+02:00"), V::Bool(!equal));
        assert_eq!(binary(B::IsNotDistinctFrom, left, right, "+02:00"), V::Bool(equal));
    }
    let left = V::list(vec![second, V::Null]);
    let right = V::list(vec![same_instant, V::I64(1)]);
    assert_eq!(binary(B::Eq, left.clone(), right.clone(), "UTC"), V::Null);
    assert_eq!(binary(B::NotEq, left, right, "UTC"), V::Null);
    assert_eq!(binary(B::NotEq, V::Null, V::DateDay(0), "UTC"), V::Null);
    assert_eq!(binary(B::IsDistinctFrom, V::Null, V::DateDay(0), "UTC"), V::Bool(true));
}

#[test]
fn strict_equality_preserves_temporal_categories_zone_labels_and_interval_units() {
    assert!(!strict_eq(&V::DateDay(0), &timestamp(0, U::Microsecond, None), &equality_scope()).unwrap());
    assert!(strict_eq(&V::DateDay(-1), &V::DateMilli(-1), &equality_scope()).unwrap());
    assert!(!strict_eq(&timestamp(0, U::Second, Some("UTC")), &timestamp(0, U::Microsecond, Some("Z")), &equality_scope()).unwrap());
    assert!(strict_eq(&timestamp(1, U::Second, Some("UTC")), &timestamp(1_000_000, U::Microsecond, Some("UTC")), &equality_scope()).unwrap());
    assert!(!strict_eq(&V::IntervalYearMonth(12), &V::IntervalMonthDayMicro { months: 12, days: 0, nanoseconds: 0 }, &equality_scope()).unwrap());
    assert!(strict_eq(&V::List(vec![V::Null, V::I32(1)]), &V::List(vec![V::Null, V::I64(1)]), &equality_scope()).unwrap());
    assert!(!strict_eq(&V::I64(1), &V::F64(1.0), &equality_scope()).unwrap());
    assert_eq!(scalar_call("===", &[V::Null, V::Null], &EvalContext::default()).unwrap(), V::Bool(true));
    let period = Expr::Literal(V::TimestampTzRange { lower: 0, upper: i64::MAX });
    assert_eq!(Expr::binary(B::Eq, period.clone(), period).data_type(), LogicalType::Scalar(rtdb_types::ScalarType::Bool));
}

#[test]
fn strict_equality_temporal_and_integer_values_use_raw_read_dispatch() {
    assert!(strict_eq(&V::DateDay(0), &V::I64(0), &equality_scope()).unwrap());
    assert!(strict_eq(&timestamp(1, U::Second, Some("UTC")), &V::I64(1), &equality_scope()).unwrap());
    assert!(!strict_eq(&timestamp(1, U::Second, Some("UTC")), &V::I64(1_000_000), &equality_scope()).unwrap());
}

#[test]
fn duration_time_and_interval_comparisons_keep_reference_units() {
    assert_eq!(sql_eq(&V::Duration { value: 1, unit: U::Second }, &V::Duration { value: 1_000_000_000, unit: U::Nanosecond }, &equality_scope()).unwrap(), Some(true));
    assert_eq!(sql_eq(&V::Time { value: 1, unit: U::Second, bit_width: 32 }, &V::Time { value: 1_000_000, unit: U::Microsecond, bit_width: 64 }, &equality_scope()).unwrap(), Some(true));
    let micro = V::IntervalMonthDayMicro { months: 12, days: 0, nanoseconds: 0 };
    assert_eq!(sql_eq(&V::IntervalYearMonth(12), &micro, &equality_scope()).unwrap(), Some(true));
    assert_eq!(compare(&V::IntervalYearMonth(12), &micro, false).unwrap_err().code, "xtdb.expression/cannot-compare-different-unit-intervals");
    assert_eq!(compare(&micro, &micro, false).unwrap_err().code, "xtdb.expression/cannot-compare-mdn-interval-with-months");
    let interval = |months, days, nanoseconds| V::IntervalMonthDayNano { months, days, nanoseconds };
    assert_eq!(compare(&interval(-1, 0, 1), &interval(0, 0, 2), false).unwrap(), Ordering::Less);
    assert_eq!(compare(&interval(0, 1, 0), &interval(0, 0, 86_400_000_000_000), false).unwrap(), Ordering::Equal);
    assert_eq!(sql_eq(&interval(0, 1, 0), &interval(0, 0, 86_400_000_000_000), &equality_scope()).unwrap(), Some(false));
    assert_eq!(compare(&interval(0, 106_752, 0), &interval(0, 0, 0), false).unwrap(), Ordering::Less);
    assert_eq!(compare(&interval(0, 1, i64::MAX), &interval(0, 0, 0), false).unwrap(), Ordering::Less);
}

#[test]
fn comparison_coerces_string_literals_but_not_parameters_or_distinct_operands() {
    let date = V::DateDay(0);
    let text = V::Utf8("1970-01-01".into());
    assert_eq!(binary(B::Eq, date.clone(), text.clone(), "UTC"), V::Bool(true));
    assert_eq!(binary(B::LtEq, text.clone(), date.clone(), "UTC"), V::Bool(true));
    assert_eq!(binary(B::NotEq, text.clone(), date.clone(), "UTC"), V::Bool(false));
    assert_eq!(binary(B::IsDistinctFrom, text.clone(), date.clone(), "UTC"), V::Bool(true));
    let expr = Expr::binary(B::Eq, Expr::Literal(date), Expr::Parameter { index: 0, data_type: text.logical_type() });
    assert_eq!(evaluate(&expr, &Schema::default(), &[], &EvalContext { parameters: rtdb_expr::Parameters::Values(&[text]), ..Default::default() }).unwrap(), V::Bool(false));
}

#[test]
fn greatest_and_least_choose_right_ties_and_short_circuit_nulls() {
    let local = timestamp(0, U::Microsecond, None);
    let instant = timestamp(-7_200, U::Second, Some("UTC"));
    let context = EvalContext { timezone: "+02:00", ..Default::default() };
    for name in ["greatest", "least"] {
        assert_eq!(scalar_call(name, &[], &context).unwrap(), V::Null);
        assert_eq!(function_type(name, &[]).unwrap(), LogicalType::Null);
        assert_eq!(scalar_call(name, &[local.clone(), instant.clone()], &context).unwrap(), instant);
        assert_eq!(scalar_call(name, &[V::F64(f64::NAN), V::I64(7)], &context).unwrap(), V::I64(7));
        let failure = Expr::binary(B::Divide, Expr::Literal(V::I64(1)), Expr::Literal(V::I64(0)));
        let expr = Expr::Call { name: name.into(), args: vec![Expr::Literal(V::Null), failure], return_type: LogicalType::Null };
        assert_eq!(evaluate(&expr, &Schema::default(), &[], &context).unwrap(), V::Null);
    }
    assert_eq!(scalar_call("nullif", &[local, instant], &context).unwrap(), V::Null);
}

fn equality_scope() -> rtdb_expr::EvaluationScope {
    rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap()
}
