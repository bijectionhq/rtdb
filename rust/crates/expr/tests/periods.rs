use rtdb_expr::{EvalContext, arithmetic, scalar_call};
use rtdb_plan::{BinaryOp, Expr, function_type};
use rtdb_types::{LogicalType as T, ScalarType as S, TimeUnit, Value as V};

fn period(lower: i64, upper: i64) -> V { V::TimestampTzRange { lower, upper } }
fn timestamp(value: i64, unit: TimeUnit, timezone: Option<&str>) -> V { V::Timestamp { value, unit, timezone: timezone.map(str::to_owned) } }

#[test]
fn temporal_period_intersection_uses_half_open_endpoints_and_nullable_type() {
    for (left, right, expected) in [
        (period(1, 4), period(2, 5), period(2, 4)),
        (period(1, 2), period(2, 3), V::Null),
        (period(i64::MIN, i64::MAX), period(2, 3), period(2, 3)),
        (V::Null, period(2, 3), V::Null),
    ] {
        assert_eq!(arithmetic(BinaryOp::Multiply, &left, &right).unwrap(), expected);
        let expression = Expr::binary(BinaryOp::Multiply, Expr::Literal(left), Expr::Literal(right));
        assert!(expression.data_type().nullable());
    }
}

#[test]
fn temporal_period_predicates_and_question_mark_aliases_keep_reference_boundaries() {
    let context = EvalContext::default();
    for (name, left, right, expected) in [
        ("contains", (1, 4), (1, 4), true), ("strictly_contains", (1, 4), (1, 3), false),
        ("strictly_contains", (1, 4), (2, 3), true), ("overlaps", (1, 2), (2, 3), false),
        ("strictly_overlaps", (2, 3), (1, 4), true), ("equals", (1, 4), (1, 4), true),
        ("precedes", (1, 2), (2, 3), true), ("strictly_precedes", (1, 2), (2, 3), false),
        ("immediately_precedes", (1, 2), (2, 3), true), ("succeeds", (2, 3), (1, 2), true),
        ("strictly_succeeds", (2, 3), (1, 2), false), ("immediately_succeeds", (2, 3), (1, 2), true),
        ("leads", (1, 3), (2, 4), true), ("strictly_leads", (1, 3), (2, 4), true),
        ("leads", (1, 2), (3, 4), false), ("immediately_leads", (1, 4), (2, 4), true),
        ("lags", (2, 4), (1, 3), true), ("strictly_lags", (2, 4), (1, 3), true),
        ("immediately_lags", (1, 4), (1, 3), true),
    ] {
        let args = [period(left.0, left.1), period(right.0, right.1)];
        for name in [name.to_owned(), format!("{name}?")] {
            assert_eq!(scalar_call(&name, &args, &context).unwrap(), V::Bool(expected), "{name}");
            assert_eq!(function_type(&name, &args.iter().map(V::logical_type).collect::<Vec<_>>()).unwrap(), T::Scalar(S::Bool));
            assert_eq!(scalar_call(&name, &[V::Null, args[1].clone()], &context).unwrap(), V::Null);
            assert_eq!(function_type(&name, &[T::Null, args[1].logical_type()]).unwrap(), T::Null);
        }
    }
}

#[test]
fn temporal_contains_point_keeps_raw_timestamp_units_and_local_zone_dispatch() {
    let context = EvalContext { timezone: "America/New_York", ..Default::default() };
    let range = period(1, 4);
    for unit in [TimeUnit::Second, TimeUnit::Millisecond, TimeUnit::Microsecond, TimeUnit::Nanosecond] {
        assert_eq!(scalar_call("contains?", &[range.clone(), timestamp(2, unit, Some("UTC"))], &context).unwrap(), V::Bool(true));
    }
    assert_eq!(scalar_call("contains?", &[range, timestamp(4, TimeUnit::Microsecond, Some("UTC"))], &context).unwrap(), V::Bool(false));
    let at_midnight = period(18_000_000_000, 18_000_000_001);
    assert_eq!(scalar_call("contains?", &[at_midnight.clone(), V::DateDay(0)], &context).unwrap(), V::Bool(true));
    assert_eq!(scalar_call("contains?", &[at_midnight, timestamp(0, TimeUnit::Microsecond, None)], &context).unwrap(), V::Bool(true));
    assert!(scalar_call("contains", &[period(1, 4), timestamp(2, TimeUnit::Microsecond, Some("UTC"))], &context).is_err());
}
