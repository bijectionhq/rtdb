use rtdb_expr::cast_target;
use rtdb_plan::{CastTarget, CastTimezone, IntervalField as F, IntervalQualifier, TemporalCast, TemporalKind};
use rtdb_types::{LogicalType, ScalarType, TimeUnit as U, Value as V};

fn temporal(kind: TemporalKind, unit: U, precision: Option<i64>) -> CastTarget { CastTarget::Temporal(TemporalCast { kind, unit, precision }) }
fn interval(start: F, end: Option<F>, precision: i64) -> CastTarget { CastTarget::Interval(IntervalQualifier { start, end, leading_precision: Some(2), fractional_precision: Some(precision) }) }
fn mdm(days: i32, nanos: i64) -> V { V::IntervalMonthDayMicro { months: 0, days, nanoseconds: nanos } }

#[test]
fn test_cast_string_and_temporal_fractional_precision_cases() {
    for (text, target, expected) in [
        ("05:34:56.123456789", temporal(TemporalKind::Time, U::Nanosecond, Some(7)), V::Time { value: 20_096_123_456_700, unit: U::Nanosecond, bit_width: 64 }),
        ("PT13M56.123456789S", temporal(TemporalKind::Duration, U::Nanosecond, Some(7)), V::Duration { value: 836_123_456_700, unit: U::Nanosecond }),
        ("1970-01-01T00:00:00.123456789+01:30", temporal(TemporalKind::TimestampZone(CastTimezone::Explicit("UTC".into())), U::Nanosecond, Some(8)), V::Timestamp { value: -5_399_876_543_220, unit: U::Nanosecond, timezone: Some("UTC".into()) }),
        ("1970-01-01T00:00:00.123456789+01:30", temporal(TemporalKind::TimestampLocal, U::Nanosecond, Some(8)), V::Timestamp { value: 123_456_780, unit: U::Nanosecond, timezone: None }),
        ("-PT0.123456789S", temporal(TemporalKind::Duration, U::Nanosecond, Some(2)), V::Duration { value: -130_000_000, unit: U::Nanosecond }),
    ] { assert_eq!(cast_target(&V::Utf8(text.into()), &target, "America/New_York").unwrap(), expected); }
}

#[test]
fn precision_validation_is_source_sensitive_and_null_preserving() {
    let target = temporal(TemporalKind::Duration, U::Nanosecond, Some(10));
    assert_eq!(cast_target(&V::Duration { value: 123, unit: U::Nanosecond }, &target, "UTC").unwrap(), V::Duration { value: 123, unit: U::Nanosecond });
    assert_eq!(cast_target(&V::Null, &target, "UTC").unwrap(), V::Null);
    assert_eq!(cast_target(&V::Utf8("PT1S".into()), &target, "UTC").unwrap_err().code, "xtdb.expression/invalid-fractional-precision");
    assert_eq!(cast_target(&mdm(0, 1), &target, "UTC").unwrap_err().code, "xtdb.expression/invalid-fractional-precision");
    let zoned = temporal(TemporalKind::TimestampZone(CastTimezone::Session), U::Microsecond, None);
    assert_eq!(cast_target(&V::Utf8("1970-01-01T00:00:00".into()), &zoned, "UTC").unwrap_err().code, "xtdb.expression/invalid-temporal-string");
    for zone in ["UTC", "America/New_York"] { assert_eq!(cast_target(&V::Utf8("1970-01-01T00:00:00Z".into()), &zoned, zone).unwrap(), V::Timestamp { value: 0, unit: U::Microsecond, timezone: Some(zone.into()) }); }
}

#[test]
fn time_to_timestamp_uses_query_date_in_session_zone_and_ignores_precision() {
    use rtdb_expr::{EvalContext,evaluate};
    use rtdb_plan::{Expr,Schema};
    let source = V::Time { value: 12_345_678_901, unit: U::Microsecond, bit_width: 64 };
    let expression = Expr::Cast { expr: Box::new(Expr::Literal(source)), target: temporal(TemporalKind::TimestampZone(CastTimezone::Explicit("Europe/Paris".into())), U::Microsecond, Some(0)) };
    let context = EvalContext { current_time: rtdb_types::Timestamp(0).into(), timezone: "America/New_York", ..Default::default() };
    assert_eq!(evaluate(&expression,&Schema::default(),&[],&context).unwrap(),V::Timestamp { value: -56_054_321_099, unit: U::Microsecond, timezone: Some("Europe/Paris".into()) });
}

#[test]
fn timestamp_text_casts_check_epoch_seconds_before_adding_fractional_units() {
    for (kind, suffix) in [
        (TemporalKind::TimestampLocal, ""),
        (TemporalKind::TimestampZone(CastTimezone::Session), "Z"),
    ] {
        let target = temporal(kind, U::Nanosecond, Some(9));
        let fixed = CastTarget::Fixed(target.result_type(&LogicalType::Scalar(ScalarType::Utf8)));
        for target in [target, fixed] {
            let boundary = V::Utf8(format!("1677-09-21T00:12:43.145224192{suffix}"));
            assert_eq!(cast_target(&boundary, &target, "UTC").unwrap_err().code, "xtdb.expression/overflow-error");
            let next_second = V::Utf8(format!("1677-09-21T00:12:44.000000001{suffix}"));
            let value = cast_target(&next_second, &target, "UTC").unwrap();
            assert!(matches!(value, V::Timestamp { value: -9_223_372_035_999_999_999, unit: U::Nanosecond, .. }));
        }
    }
}

#[test]
fn time_to_timestamp_checks_the_query_dates_midnight_before_adding_the_time() {
    use rtdb_expr::{EvalContext, evaluate};
    use rtdb_plan::{Expr, Schema};
    use rtdb_types::Instant;

    for (kind, timezone, offset, time) in [
        (TemporalKind::TimestampLocal, "UTC", 0, 13 * 60),
        (TemporalKind::TimestampZone(CastTimezone::Session), "-01:00", 3_600, 60),
    ] {
        let source = Expr::Literal(V::Time { value: time, unit: U::Second, bit_width: 32 });
        let expression = Expr::Cast { expr: Box::new(source), target: temporal(kind, U::Nanosecond, None) };
        let context = EvalContext { current_time: Instant::new(-9_223_372_800 + offset, 0).unwrap(), timezone, ..Default::default() };
        assert_eq!(evaluate(&expression, &Schema::default(), &[], &context).unwrap_err().code, "xtdb.expression/overflow-error");
        let context = EvalContext { current_time: Instant::new(-9_223_286_400 + offset, 0).unwrap(), timezone, ..Default::default() };
        let value = evaluate(&expression, &Schema::default(), &[], &context).unwrap();
        let V::Timestamp { value, unit: U::Nanosecond, .. } = value else { panic!("expected a nanosecond timestamp") };
        assert_eq!(value, (-9_223_286_400 + time + offset) * 1_000_000_000);
    }
}

#[test]
fn malformed_time_text_reports_the_same_cast_error_for_fixed_and_temporal_targets() {
    let target = temporal(TemporalKind::Time, U::Microsecond, Some(6));
    let fixed = CastTarget::Fixed(target.result_type(&LogicalType::Scalar(ScalarType::Utf8)));
    for target in [target, fixed] {
        let source = V::Utf8("24:00:00".into());
        let error = cast_target(&source, &target, "UTC").unwrap_err();
        assert_eq!(error.code, "xtdb.expression/invalid-temporal-string");
        assert_eq!(error.context.get("date-type"), Some(&serde_json::json!("time without timezone")));
        assert_eq!(error.context.get("s"), Some(&serde_json::json!("24:00:00")));
    }
}

#[test]
fn double_to_duration_uses_clojures_long_conversion_after_scaling() {
    let target = temporal(TemporalKind::Duration, U::Second, None);
    for (input, expected) in [(f64::NAN, 0), (-1.25, -1), (i64::MAX as f64, i64::MAX), (i64::MIN as f64, i64::MIN)] {
        assert_eq!(cast_target(&V::F64(input), &target, "UTC").unwrap(), V::Duration { value: expected, unit: U::Second });
    }
    let error = cast_target(&V::F64(f64::INFINITY), &target, "UTC").unwrap_err();
    assert_eq!(error.code, "xtdb.expression/illegal-argument-exception");
    assert_eq!(error.message.as_deref(), Some("Value out of range for long: Infinity"));
}

#[test]
fn cast_interval_to_duration_precision_precedes_nanos_overflow_and_month_check() {
    let value = V::IntervalMonthDayNano { months: 0, days: 1, nanoseconds: 3_601_111_111_111 };
    assert_eq!(cast_target(&value, &temporal(TemporalKind::Duration, U::Millisecond, Some(2)), "UTC").unwrap(), V::Duration { value: 90_001_110, unit: U::Millisecond });
    let value = V::IntervalMonthDayNano { months: 4, days: 8, nanoseconds: 0 };
    assert_eq!(cast_target(&value, &temporal(TemporalKind::Duration, U::Nanosecond, Some(10)), "UTC").unwrap_err().code, "xtdb.expression/invalid-fractional-precision");
    let value = V::Utf8("PT9223372036.854775808S".into());
    assert_eq!(cast_target(&value, &temporal(TemporalKind::Duration, U::Nanosecond, Some(0)), "UTC").unwrap(), V::Duration { value: 9_223_372_036_000_000_000, unit: U::Nanosecond });
}

#[test]
fn cast_duration_to_interval_normalizes_each_qualifier() {
    let source = V::Duration { value: 130_210_111_111, unit: U::Microsecond };
    for (start, end, precision, expected) in [
        (F::Day,None,0,mdm(1,0)), (F::Day,Some(F::Hour),0,mdm(1,43_200_000_000_000)),
        (F::Day,Some(F::Minute),0,mdm(1,43_800_000_000_000)), (F::Day,Some(F::Second),4,mdm(1,43_810_111_100_000)),
        (F::Hour,None,0,mdm(0,129_600_000_000_000)), (F::Hour,Some(F::Minute),0,mdm(0,130_200_000_000_000)),
        (F::Minute,None,0,mdm(0,130_200_000_000_000)), (F::Second,None,3,mdm(0,130_210_111_000_000)),
    ] { assert_eq!(cast_target(&source, &interval(start,end,precision), "UTC").unwrap(), expected, "{start:?}/{end:?}"); }
}

#[test]
fn cast_utf8_to_interval_single_and_multi_field_precision_metadata() {
    for (text, target, expected) in [
        ("1-10", interval(F::Year,Some(F::Month),0), V::IntervalYearMonth(22)),
        ("1-10trailing", interval(F::Year,Some(F::Month),0), V::IntervalYearMonth(22)),
        ("1 10", interval(F::Day,Some(F::Hour),0), mdm(1,36_000_000_000_000)),
        ("-1 10:10:10.111111111", interval(F::Day,Some(F::Second),7), V::IntervalMonthDayNano { months: 0, days: -1, nanoseconds: -36_610_111_111_100 }),
        ("1.123456789", interval(F::Second,None,0), mdm(0,1_123_456_789)),
        ("1.123456789", interval(F::Second,None,9), mdm(0,1_123_456_789)),
        ("١٢", interval(F::Day,None,0), mdm(12,0)),
    ] {
        assert_eq!(cast_target(&V::Utf8(text.into()), &target, "UTC").unwrap(), expected);
        assert_eq!(target.result_type(&LogicalType::Scalar(ScalarType::Utf8)), expected.logical_type());
    }
}

#[test]
fn interval_cast_integer_ignores_unused_options_and_year_month_keeps_reference_modulo() {
    let target = CastTarget::Interval(IntervalQualifier { start: F::Second, end: None, leading_precision: None, fractional_precision: None });
    assert_eq!(cast_target(&V::I64(3), &target, "UTC").unwrap(), mdm(0,3_000_000_000));
    assert_eq!(cast_target(&V::I64(3), &interval(F::Day,Some(F::Second),6), "UTC").unwrap_err().code, "xtdb.expression/attempting-to-cast-int-to-multi-field-interval");
    assert_eq!(cast_target(&V::IntervalYearMonth(-13), &interval(F::Year,Some(F::Month),6), "UTC").unwrap(), V::IntervalYearMonth(-1));
    assert_eq!(cast_target(&V::IntervalMonthDayNano { months: -1, days: 1, nanoseconds: 0 }, &interval(F::Day,None,0), "UTC").unwrap(), mdm(1,0));
    assert_eq!(cast_target(&mdm(1,0), &interval(F::Year,None,6), "UTC").unwrap_err().code, "xtdb.expression/unsupported-cast");
}

#[test]
fn single_second_interval_duration_precision_keeps_reference_unvalidated_extremes() {
    let source = V::Duration { value: 1_123_456_789, unit: U::Nanosecond };
    assert_eq!(cast_target(&source,&interval(F::Second,None,400),"UTC").unwrap(),V::IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 1_000_000_000 });
    assert_eq!(cast_target(&source,&interval(F::Second,None,-400),"UTC").unwrap(),mdm(0,1_000_000_000));
}

#[test]
fn day_time_qualified_text_checks_positive_duration_before_negating() {
    let target = interval(F::Hour,Some(F::Second),9);
    for text in ["2562047:47:16.854775808","-2562047:47:16.854775808"] {
        assert!(cast_target(&V::Utf8(text.into()),&target,"UTC").is_err(),"{text}");
    }
    assert_eq!(cast_target(&V::Utf8("-2562047:47:16.854775807".into()),&target,"UTC").unwrap(),V::IntervalMonthDayNano { months: 0,days: 0,nanoseconds: -i64::MAX });
    assert_eq!(cast_target(&V::Utf8("-178956970-8".into()),&interval(F::Year,Some(F::Month),0),"UTC").unwrap(),V::IntervalYearMonth(i32::MIN));
}

#[test]
fn duration_text_retains_java_bare_t_case_and_microsecond_division() {
    let target = temporal(TemporalKind::Duration,U::Nanosecond,None);
    assert_eq!(cast_target(&V::Utf8("P1Dt".into()),&target,"UTC").unwrap(),V::Duration { value: 86_400_000_000_000,unit: U::Nanosecond });
    assert!(cast_target(&V::Utf8("P1DT".into()),&target,"UTC").is_err());
    assert_eq!(cast_target(&V::Utf8("2048999.9999999998 microseconds".into()),&target,"UTC").unwrap(),V::Duration { value: 2_048_000_000,unit: U::Nanosecond });
}

#[test]
fn iso_interval_narrows_combined_months_but_checks_week_days_separately() {
    for (text,months) in [("P178956971Y-5M",i32::MAX),("P-178956971Y4M",i32::MIN)] {
        assert_eq!(cast_target(&V::Utf8(text.into()),&CastTarget::IntervalDefault,"UTC").unwrap(),V::IntervalMonthDayMicro { months,days: 0,nanoseconds: 0 });
    }
    for text in ["P178956971Y-4M","P-178956971Y3M","P306783379W-6D"] {
        assert!(cast_target(&V::Utf8(text.into()),&CastTarget::IntervalDefault,"UTC").is_err(),"{text}");
    }
}
