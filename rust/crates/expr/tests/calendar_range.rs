use rtdb_expr::{arithmetic, cast, evaluate, evaluate_async, EvalContext};
use rtdb_plan::{BinaryOp, CastTarget, Expr, Schema};
use rtdb_types::{CivilDate, DateUnit, LogicalType, ScalarType, TimeUnit as U, Value as V};

fn target(scalar: ScalarType) -> LogicalType { LogicalType::Scalar(scalar) }

#[test]
fn calendar_rendering_and_casts_cover_physical_dates_and_timestamp_seconds() {
    for days in [i32::MIN, i32::MAX] {
        let value = V::DateDay(days);
        let text = CivilDate::from_epoch_day(i64::from(days)).unwrap().to_string();
        assert_eq!(cast(&value, &target(ScalarType::Utf8), "UTC").unwrap(), V::Utf8(text.clone()));
        assert_eq!(cast(&V::Utf8(text), &target(ScalarType::DateDay), "UTC").unwrap(), value);
    }
    let day = CivilDate::from_ymd(300_000, 7, 1).unwrap().epoch_day();
    let timestamp = target(ScalarType::Timestamp { unit: U::Second, timezone: Some("Europe/Paris".into()) });
    assert_eq!(cast(&V::Utf8("300000-07-01T00:00[Europe/Paris]".into()), &timestamp, "UTC").unwrap(),
        V::Timestamp { value: day * 86_400 - 7200, unit: U::Second, timezone: Some("Europe/Paris".into()) });
    let value = V::Timestamp { value: day * 86_400 - 7200, unit: U::Second, timezone: Some("Europe/Paris".into()) };
    assert_eq!(cast(&value, &target(ScalarType::Utf8), "UTC").unwrap(), V::Utf8("+300000-07-01T00:00+02:00[Europe/Paris]".into()));
}

#[test]
fn date_vector_outputs_apply_their_distinct_narrowing_rules() {
    let mut result = arithmetic(BinaryOp::Add, &V::DateDay(i32::MAX), &V::IntervalYearMonth(12)).unwrap();
    assert!(result.admit_expression_dates().is_err());
    let mut result = cast(&V::Utf8("+999999999-01-01".into()), &target(ScalarType::DateDay), "UTC").unwrap();
    assert!(result.admit_expression_dates().is_err());
    let date = CivilDate::from_epoch_day(i64::MAX / 86_400_000).unwrap();
    let millis = date.epoch_day() * 86_400_000;
    let mut result = arithmetic(BinaryOp::Add, &V::DateMilli(millis), &V::IntervalYearMonth(12)).unwrap();
    assert_eq!(result.admit_expression_dates().unwrap_err().code, "xtdb.expression/overflow-error");
}

fn text(expression: Expr) -> Expr { Expr::Cast { expr: Box::new(expression), target: CastTarget::Fixed(target(ScalarType::Utf8)) } }

#[test]
fn wide_calendar_intermediates_reach_enclosing_operations_before_output_narrowing() {
    let date = CivilDate::from_ymd(999_999_999,1,1).unwrap();
    let literal = Expr::Literal(V::calendar_date(date, DateUnit::Day));
    let context = EvalContext::default();
    assert!(evaluate(&literal,&Schema::default(),&[],&context).is_err());
    assert_eq!(evaluate(&text(literal.clone()),&Schema::default(),&[],&context).unwrap(),V::Utf8(date.to_string()));
    let earlier = Expr::binary(BinaryOp::Subtract,literal,Expr::Literal(V::IntervalYearMonth(12)));
    assert_eq!(evaluate(&text(earlier),&Schema::default(),&[],&context).unwrap(),V::Utf8("+999999998-01-01".into()));
    assert!(evaluate(&Expr::List(vec![Expr::Literal(V::calendar_date(date,DateUnit::Day))]),&Schema::default(),&[],&context).is_err());
}

#[tokio::test(start_paused = true)]
async fn suspended_evaluation_retains_wide_calendar_intermediates_until_the_outer_result() {
    let date = CivilDate::MAX;
    let sleep = Expr::Call { name: "sleep".into(), args: vec![Expr::Literal(V::Duration { value: 1,unit: U::Millisecond })], return_type: LogicalType::Null };
    let expression = text(Expr::Coalesce(vec![sleep,Expr::Literal(V::calendar_date(date,DateUnit::Day))]));
    assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&EvalContext::default()).await.unwrap(),V::Utf8(date.to_string()));
}

#[test]
fn fixed_offset_arithmetic_preserves_checked_source_unit_overflow() {
    let source = V::Timestamp { value: i64::MAX, unit: U::Microsecond, timezone: Some("UTC".into()) };
    assert!(cast(&source, &target(ScalarType::Timestamp { unit: U::Second, timezone: None }), "+01:00").is_err());
    let source = V::Timestamp { value: i64::MAX, unit: U::Second, timezone: Some("UTC".into()) };
    assert!(cast(&source, &target(ScalarType::Utf8), "UTC").is_err());
}
