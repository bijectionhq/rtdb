use rtdb_expr::{EvalContext, arithmetic_in_timezone, cast, cast_target, evaluate, scalar_call};
use rtdb_plan::{BinaryOp as B, CastTarget, Expr, Schema, UnaryOp};
use rtdb_types::{IntervalUnit as I, LogicalType as T, ScalarType as S, TimeUnit as U, Value as V};

fn date(text: &str) -> V {
    let value = chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap();
    V::DateDay(value.signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days().try_into().unwrap())
}
fn local(text: &str, unit: U) -> V {
    let value = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f").unwrap().and_utc();
    V::Timestamp { value: value.timestamp() * unit.units_per_second() + i64::from(value.timestamp_subsec_nanos()) / (1_000_000_000 / unit.units_per_second()), unit, timezone: None }
}
fn zoned(text: &str, timezone: &str, unit: U) -> V {
    let value = chrono::DateTime::parse_from_rfc3339(text).unwrap();
    V::Timestamp { value: value.timestamp() * unit.units_per_second() + i64::from(value.timestamp_subsec_nanos()) / (1_000_000_000 / unit.units_per_second()), unit, timezone: Some(timezone.into()) }
}
fn duration(nanos: i64) -> V { V::Duration { value: nanos, unit: U::Nanosecond } }
fn mdm(months: i32, days: i32, nanoseconds: i64) -> V { V::IntervalMonthDayMicro { months, days, nanoseconds } }
fn context(timezone: &str) -> EvalContext<'_> { EvalContext { timezone, ..Default::default() } }
fn arithmetic(op: B, left: V, right: V, expected: V) {
    let expression = Expr::binary(op, Expr::Literal(left), Expr::Literal(right));
    assert_eq!(expression.data_type(), expected.logical_type());
    assert_eq!(evaluate(&expression, &Schema::default(), &[], &context("America/Los_Angeles")).unwrap(), expected);
}

#[test]
fn temporal_arithmetic_preserves_pinned_result_units_and_session_zone() {
    let delta = duration(4_543_342_000_000);
    arithmetic(B::Add, date("2022-08-01"), delta.clone(), local("2022-08-01T01:15:43.342", U::Nanosecond));
    arithmetic(B::Subtract, date("2022-08-01"), delta.clone(), local("2022-07-31T22:44:16.658", U::Nanosecond));
    arithmetic(B::Add, local("2022-08-01T01:15:43.342", U::Microsecond), delta.clone(), local("2022-08-01T02:31:26.684", U::Nanosecond));
    arithmetic(B::Subtract, local("2022-08-01T02:31:26.684", U::Nanosecond), local("2022-08-01T01:15:43.342", U::Microsecond), delta);
    arithmetic(B::Subtract, date("2022-08-01"), zoned("2022-08-01T01:15:43.342+01:00", "Europe/London", U::Microsecond), V::Duration { value: 24_256_658_000, unit: U::Microsecond });
    arithmetic(B::Subtract, zoned("2022-08-01T01:15:43.342+01:00", "Europe/London", U::Microsecond), local("2022-08-01T02:31:26.684", U::Microsecond), V::Duration { value: -33_343_342_000, unit: U::Microsecond });
    arithmetic(B::Subtract, date("2000-03-01"), date("2000-02-28"), V::I32(2));
    arithmetic(B::Subtract, date("2001-03-01"), date("2001-02-28"), V::I32(1));
    arithmetic(B::Subtract, V::DateMilli(-1), V::DateMilli(0), V::I32(-1));
    arithmetic(B::Add, V::DateMilli(-1), V::IntervalYearMonth(0), V::DateMilli(-86_400_000));
    arithmetic(B::Multiply, V::I32(2), duration(180_000_000_000), duration(360_000_000_000));
    arithmetic(B::Divide, V::Duration { value: 3500, unit: U::Millisecond }, V::Duration { value: 1, unit: U::Second }, V::I64(3));
    arithmetic(B::Subtract, V::Time { value: 2, unit: U::Second, bit_width: 32 }, V::Time { value: 500, unit: U::Millisecond, bit_width: 32 }, V::Duration { value: 1500, unit: U::Millisecond });
}

#[test]
fn calendar_intervals_clamp_month_ends_and_keep_duration_on_the_instant_timeline() {
    arithmetic(B::Add, date("2020-01-31"), V::IntervalYearMonth(1), date("2020-02-29"));
    arithmetic(B::Subtract, date("2021-03-31"), V::IntervalYearMonth(1), date("2021-02-28"));
    arithmetic(B::Add, local("2022-08-01T01:15:43.342", U::Microsecond), mdm(1, 3, 4_543_342_000_000), local("2022-09-04T02:31:26.684", U::Microsecond));
    let london = zoned("2021-03-27T12:00:00Z", "Europe/London", U::Microsecond);
    arithmetic(B::Add, london.clone(), mdm(0, 1, 0), zoned("2021-03-28T12:00:00+01:00", "Europe/London", U::Microsecond));
    arithmetic(B::Add, london, mdm(0, 0, 86_400_000_000_000), zoned("2021-03-28T13:00:00+01:00", "Europe/London", U::Microsecond));
    arithmetic(B::Add, zoned("2022-10-29T11:00:00+01:00", "Europe/London", U::Microsecond), mdm(0, 2, 3_600_000_000_000), zoned("2022-10-31T12:00:00Z", "Europe/London", U::Microsecond));
    arithmetic(B::Add, zoned("2021-03-27T01:30:00Z", "Europe/London", U::Microsecond), mdm(0, 1, 0), zoned("2021-03-28T02:30:00+01:00", "Europe/London", U::Microsecond));
    arithmetic(B::Subtract, zoned("2021-11-01T01:30:00Z", "Europe/London", U::Microsecond), mdm(0, 1, 0), zoned("2021-10-31T01:30:00Z", "Europe/London", U::Microsecond));
    arithmetic(B::Add, zoned("2021-10-30T01:30:00+01:00", "Europe/London", U::Microsecond), mdm(0, 1, 0), zoned("2021-10-31T01:30:00+01:00", "Europe/London", U::Microsecond));
}

#[test]
fn interval_subtraction_floors_fractional_chronons_and_checked_units_do_not_wrap() {
    let instant = local("2021-01-01T00:00:00", U::Microsecond);
    arithmetic(B::Add, instant.clone(), mdm(0, 0, 500), instant.clone());
    arithmetic(B::Subtract, instant, mdm(0, 0, 500), local("2020-12-31T23:59:59.999999", U::Microsecond));
    arithmetic(B::Add, V::DateDay(1_000_000_000), V::IntervalYearMonth(0), V::DateDay(1_000_000_000));
    arithmetic(B::Add, V::DateDay(-1_000_000_000), V::IntervalYearMonth(0), V::DateDay(-1_000_000_000));
    for (op, left, right) in [
        (B::Add, V::Duration { value: i64::MAX, unit: U::Second }, V::Duration { value: 1, unit: U::Second }),
        (B::Add, V::Duration { value: i64::MAX, unit: U::Second }, duration(1)),
        (B::Multiply, V::IntervalYearMonth(i32::MAX), V::I64(2)),
        (B::Divide, duration(1), V::I64(0)),
        (B::Add, local("1970-01-01T00:00:00", U::Nanosecond), V::IntervalMonthDayNano { months: 0, days: 0, nanoseconds: i64::MIN }),
        (B::Subtract, local("1970-01-01T00:00:00", U::Nanosecond), V::IntervalMonthDayNano { months: 0, days: 0, nanoseconds: i64::MIN }),
    ] { assert!(arithmetic_in_timezone(op, &left, &right, "UTC").is_err()); }
}

#[test]
fn interval_components_remain_independent_and_mixed_division_is_rejected() {
    arithmetic(B::Add, V::IntervalYearMonth(12), mdm(0, 3, 1), mdm(12, 3, 1));
    arithmetic(B::Subtract, mdm(12, 3, 1), V::IntervalYearMonth(1), mdm(11, 3, 1));
    arithmetic(B::Multiply, V::I64(2), mdm(1, -2, 3), mdm(2, -4, 6));
    arithmetic(B::Divide, V::IntervalYearMonth(-13), V::I64(2), V::IntervalYearMonth(-6));
    assert!(arithmetic_in_timezone(B::Divide, &mdm(1, 2, 0), &V::I64(2), "UTC").is_err());
    let expr = Expr::unary(UnaryOp::Negate, Expr::Literal(mdm(1, -2, 3)));
    assert_eq!(evaluate(&expr, &Schema::default(), &[], &context("UTC")).unwrap(), mdm(-1, 2, -3));
    assert_eq!(scalar_call("abs", &[V::IntervalYearMonth(-13)], &context("UTC")).unwrap(), V::IntervalYearMonth(13));
    assert!(scalar_call("abs", &[mdm(-1, -2, 0)], &context("UTC")).is_err());
}

#[test]
fn minimum_long_temporal_quotients_wrap_and_mixed_intervals_fail_before_zero_division() {
    let minimum = duration(i64::MIN);
    arithmetic(B::Divide, minimum.clone(), V::I64(-1), minimum.clone());
    arithmetic(B::Divide, minimum, duration(-1), V::I64(i64::MIN));
    arithmetic(B::Divide, mdm(0, 0, i64::MIN), V::I64(-1), mdm(0, 0, i64::MIN));
    assert_eq!(arithmetic_in_timezone(B::Divide, &mdm(0, 0, 1), &V::I64(0), "UTC").unwrap_err().code,
        "xtdb.expression/division-by-zero");
    assert_eq!(arithmetic_in_timezone(B::Divide, &mdm(0, 1, 1), &V::I64(0), "UTC").unwrap_err().code,
        "xtdb.expression/cannot-divide-mixed-intervals");
}

#[test]
fn duration_text_casts_preserve_java_iso_and_millisecond_fallback_semantics() {
    let target = T::Scalar(S::Duration(U::Nanosecond));
    for (text, nanos) in [("PT2S", 2_000_000_000), ("1 SECOND", 1_000_000_000), ("1.5 SECOND", 1_500_000_000), ("0.99 second", 990_000_000), ("1 microsecond", 0), ("PT-0.000000001S", -1), ("-PT-1S", 1_000_000_000), ("PT1M-2S", 58_000_000_000), ("PT1,5S", 1_500_000_000)] {
        assert_eq!(cast(&V::Utf8(text.into()), &target, "UTC").unwrap(), duration(nanos), "{text}");
    }
    for text in ["PT", "P", "P1MT", "-1 second", "PT1.1234567890S", "PT9223372036854775807S", "P106751991167301DT-9223372036854775807S"] { assert!(cast(&V::Utf8(text.into()), &target, "UTC").is_err(), "{text}"); }
    for (nanos, text) in [(0, "PT0S"), (-1, "PT-0.000000001S"), (-3_661_500_000_000, "PT-1H-1M-1.5S"), (3_600_000_000_000, "PT1H")] {
        assert_eq!(cast(&duration(nanos), &T::Scalar(S::Utf8), "UTC").unwrap(), V::Utf8(text.into()));
    }
}

#[test]
fn default_interval_casts_preserve_source_metadata_and_pinned_unanchored_text_parser() {
    let target = CastTarget::fixed(T::Scalar(S::Interval(I::YearMonth)));
    for source in [V::IntervalYearMonth(13), mdm(1, 2, 3), V::IntervalMonthDayNano { months: 0, days: 1, nanoseconds: 1 }] {
        assert_eq!(target.result_type(&source.logical_type()), source.logical_type());
        assert_eq!(cast_target(&source, &target, "UTC").unwrap(), source);
    }
    assert_eq!(cast_target(&duration(1), &target, "UTC").unwrap(), V::IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 1 });
    assert_eq!(cast_target(&V::Duration { value: 1, unit: U::Millisecond }, &target, "UTC").unwrap(), mdm(0, 0, 1_000_000));
    for (text, expected) in [("P1Y2M3DT4H5M6.123456789S", mdm(14, 3, 14_706_123_456_000)), ("PT-0.000000001S", mdm(0, 0, -1000)), ("-P12M", mdm(12, 0, 0)), ("invalid", mdm(0, 0, 0)), ("P", mdm(0, 0, 0))] {
        assert_eq!(cast_target(&V::Utf8(text.into()), &target, "UTC").unwrap(), expected, "{text}");
    }
    assert_eq!(cast(&mdm(22, -3, 14_400_000_000_000), &T::Scalar(S::Utf8), "UTC").unwrap(), V::Utf8("P1Y10M-3DT4H".into()));
    assert_eq!(cast(&mdm(0, 0, 0), &T::Scalar(S::Utf8), "UTC").unwrap(), V::Utf8("P".into()));
    assert_eq!(cast(&mdm(0, 1, 3_601_000_000_000), &T::Scalar(S::Duration(U::Second)), "UTC").unwrap(), V::Duration { value: 90_001, unit: U::Second });
    assert!(cast(&mdm(1, 0, 0), &T::Scalar(S::Duration(U::Second)), "UTC").is_err());
    assert_eq!(cast(&mdm(-1, 1, 0), &T::Scalar(S::Duration(U::Second)), "UTC").unwrap(), V::Duration { value: 86_400, unit: U::Second });
    arithmetic(B::Divide, V::Duration { value: 172_800, unit: U::Second }, mdm(-1, 1, 0), V::I64(2));
}

#[test]
fn imported_millisecond_dates_preserve_raw_values_until_cast_to_another_type() {
    for (raw, days) in [(-86_400_001, -2), (-86_400_000, -1), (-86_399_999, -1), (-1, -1), (1, 0), (86_400_001, 1)] {
        let source = V::DateMilli(raw);
        assert_eq!(cast(&source, &T::Scalar(S::DateDay), "UTC").unwrap(), V::DateDay(days));
        assert_eq!(cast(&source, &T::Scalar(S::DateMilli), "UTC").unwrap(), source);
        for timezone in [None, Some("UTC".into())] {
            let target = T::Scalar(S::Timestamp { unit: U::Microsecond, timezone: timezone.clone() });
            assert_eq!(cast(&source, &target, "UTC").unwrap(), V::Timestamp { value: i64::from(days) * 86_400_000_000, unit: U::Microsecond, timezone });
        }
    }
    assert_eq!(cast(&V::DateMilli(86_400_000_000_000_000), &T::Scalar(S::DateDay), "UTC").unwrap(), V::DateDay(1_000_000_000));
}

#[test]
fn date_subtraction_checks_each_epoch_day_and_the_difference() {
    let out_of_range = (i64::from(i32::MAX) + 1) * 86_400_000;
    for (left, right) in [
        (V::DateMilli(out_of_range), V::DateDay(i32::MAX)),
        (V::DateDay(i32::MAX), V::DateMilli(out_of_range)),
        (V::DateDay(i32::MAX), V::DateDay(i32::MIN)),
    ] {
        assert_eq!(rtdb_expr::arithmetic(B::Subtract, &left, &right).unwrap_err().code, "xtdb.expression/overflow-error");
    }
    arithmetic(B::Subtract, V::DateMilli(-1), V::DateDay(0), V::I32(-1));
    arithmetic(B::Subtract, V::DateDay(0), V::DateMilli(-1), V::I32(1));
}

#[test]
fn unsupported_interval_duration_casts_keep_pinned_error_identity() {
    for (source, interval_unit) in [(V::IntervalYearMonth(0), "year-month"), (V::IntervalDayTime { days: 0, milliseconds: 0 }, "day-time")] {
        let error = cast(&source, &T::Scalar(S::Duration(U::Nanosecond)), "UTC").unwrap_err();
        assert_eq!(error.category, rtdb_types::AnomalyCategory::Incorrect);
        assert_eq!(error.code, "xtdb.expression/invalid-cast");
        assert_eq!(error.context.get("interval-unit"), Some(&serde_json::json!(interval_unit)));
    }
}

#[test]
fn a_timestamp_within_an_offset_of_the_long_limit_casts_to_its_local_date_and_time() {
    let limit = V::Timestamp { value: i64::MAX, unit: U::Microsecond, timezone: Some("+05:00".into()) };
    assert_eq!(cast(&limit, &T::Scalar(S::DateDay), "+05:00").unwrap(), V::DateDay(106_751_991));
    let time = T::Scalar(S::Time { unit: U::Microsecond, bit_width: 64 });
    assert_eq!(cast(&limit, &time, "+05:00").unwrap(), V::Time { value: (9 * 3600 + 54) * 1_000_000 + 775_807, unit: U::Microsecond, bit_width: 64 });
}
