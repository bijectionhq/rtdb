use rtdb_columnar::MemoryBudget;
use rtdb_pg_codec::{Format, decode, read_text};
use rtdb_types::{PgType, TimeUnit, Value};

#[test]
fn interval_text_preserves_java_period_and_duration_component_grammar() {
    for text in ["P", "PT", "P0DT", "-PT"] { assert_eq!(read_text(1186, text).unwrap(), Value::IntervalYearMonth(0)); }
    assert_eq!(read_text(1186, "P1Y-2M3W-4DT2H-3M-0.000000001S").unwrap(), Value::IntervalMonthDayNano { months: 10, days: 17, nanoseconds: 7_019_999_999_999 });
    assert_eq!(read_text(1186, "-P1Y2DT-0.1S").unwrap(), Value::IntervalMonthDayMicro { months: -12, days: -2, nanoseconds: 100_000_000 });
    assert_eq!(read_text(1186, "PT1.S").unwrap(), Value::IntervalMonthDayMicro { months: 0, days: 0, nanoseconds: 1_000_000_000 });
    for text in ["P1M1Y", "P1Y1Y", "P1D1W", "PT1S1M", "PT1H1H", "P+1D", "+P1D", "pt1s", "PT.1S", "PT1,1S", "PT1.1234567890S", "P1DTT1S", "PT1e9S", "PT1e-2147483648S"] {
        assert!(read_text(1186, text).is_err(), "{text}");
    }
}

#[test]
fn interval_component_overflow_is_checked_before_cancellation_between_fields() {
    for text in ["P2147483648D", "-P1Y-2147483648M", "P306783379W-7D", "PT153722867280912931M-9223372036854775800S", "PT9223372036.854775808S", "-PT-9223372036.854775808S"] {
        assert!(read_text(1186, text).is_err(), "{text}");
    }
    assert_eq!(read_text(1186, "P178956971Y-5M").unwrap(), Value::IntervalYearMonth(i32::MAX));
    assert_eq!(read_text(1186, "PT-9223372036.854775808S").unwrap(), Value::IntervalMonthDayNano { months: 0, days: 0, nanoseconds: i64::MIN });
    assert_eq!(read_text(1186, "PT9223372036.854775807S").unwrap(), Value::IntervalMonthDayNano { months: 0, days: 0, nanoseconds: i64::MAX });
}

#[test]
fn duration_text_uses_its_own_java_grammar_and_microsecond_vector_admission() {
    for (text, value) in [("+p1dt+2h-3m4,5s", 93_424_500_000), ("P1Dt", 86_400_000_000), ("p1dt", 86_400_000_000), ("PT-0.000000001S", -1), ("-PT-0.000000001S", 0), ("P1000000D", 86_400_000_000_000_000), ("PT9223372036854775807S", i64::MAX.wrapping_mul(1_000_000))] {
        assert_eq!(read_text(11112, text).unwrap(), Value::Duration { value, unit: TimeUnit::Microsecond }, "{text}");
    }
    for text in ["P", "PT", "Pt", "pt", "P1DT", "p1dT", "P1Y", "P1M", "P1W", "PT1S1M", "PT1.1234567890S", "PT-9223372036854775808.1S", "-PT-9223372036854775808S"] {
        assert!(read_text(11112, text).is_err(), "{text}");
    }
}

#[test]
fn interval_decode_releases_its_reservation_on_success_and_failure() {
    let memory = MemoryBudget::new(1 << 20);
    let decoded = decode(PgType::Interval, Format::Text, b"PT0.000000001S", &memory).unwrap();
    assert!(memory.used_bytes() > 0);
    drop(decoded);
    assert_eq!(memory.used_bytes(), 0);
    assert!(decode(PgType::Interval, Format::Text, b"PT1.1234567890S", &memory).is_err());
    assert_eq!(memory.used_bytes(), 0);
}

fn period_duration(period: &str, duration: &str) -> rtdb_types::Result<Value> {
    use rtdb_format::transit::TransitValue as T;
    rtdb_pg_codec::from_transit(T::Tagged { tag: "xtdb/period-duration".into(), representation: Box::new(T::Array(vec![T::String(period.into()), T::String(duration.into())])) })
}

#[test]
fn transit_period_duration_retains_separate_grammars_and_vector_classification() {
    assert_eq!(period_duration("+p1d", "pt+1,5s").unwrap(), Value::IntervalMonthDayMicro { months: 0, days: 1, nanoseconds: 1_500_000_000 });
    assert_eq!(period_duration("P0D", "PT0.000001S").unwrap(), Value::IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 1000 });
    assert_eq!(period_duration("P1M", "PT0.001S").unwrap(), Value::IntervalMonthDayNano { months: 1, days: 0, nanoseconds: 1_000_000 });
    assert_eq!(period_duration("P2147483647Y", "PT0S").unwrap(), Value::IntervalYearMonth(-12));
    assert_eq!(period_duration("P357913941Y4M", "PT0.001S").unwrap(), Value::IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 1_000_000 });
    for (period, duration) in [("P", "PT0S"), ("PT0S", "PT0S"), ("P1D", "P"), ("P1D", "PT"), ("P1D", "P1M"), ("P1M1Y", "PT0S"), ("P1D", "PT1S1M")] {
        assert!(period_duration(period, duration).is_err(), "{period}, {duration}");
    }
    use rtdb_format::transit::TransitValue as T;
    let value = T::Tagged { tag: "xtdb/period-duration".into(), representation: Box::new(T::Array(vec![T::String("P1M".into()), T::String("PT0S".into()), T::Null])) };
    assert_eq!(rtdb_pg_codec::from_transit(value).unwrap(), Value::IntervalYearMonth(1));
}

#[test]
fn transit_period_duration_normalizes_oversized_duration_only_after_type_selection() {
    assert_eq!(period_duration("P1M2D", "P1000000D").unwrap(), Value::IntervalMonthDayNano { months: 1, days: 1_000_002, nanoseconds: 0 });
    assert_eq!(period_duration("P1M", "-P1000000DT0.000000001S").unwrap(), Value::IntervalMonthDayNano { months: 1, days: -1_000_000, nanoseconds: -1 });
    assert_eq!(period_duration("P1M2147483647D", "P1000000D").unwrap(), Value::IntervalMonthDayNano { months: 1, days: -2_146_483_649, nanoseconds: 0 });
    assert!(period_duration("P0D", "P1000000D").is_err());
    assert!(period_duration("P1D", "PT9223372036854775807S").is_err());
}

#[test]
fn transit_interval_returns_null_only_when_the_outer_interval_grammar_does_not_match() {
    use rtdb_format::transit::TransitValue as T;
    let tagged = |text: &str| T::Tagged { tag: "xtdb/interval".into(), representation: Box::new(T::String(text.into())) };
    for text in ["garbage", "p1d", "+P1D", "P1DT1,5S", "P1DTT1S"] {
        assert_eq!(rtdb_pg_codec::from_transit(tagged(text)).unwrap(), Value::Null, "{text}");
        assert!(read_text(1186, text).is_err(), "{text}");
    }
    for text in ["P1M1Y", "PT1S1M", "P-", "PT-."] { assert!(rtdb_pg_codec::from_transit(tagged(text)).is_err(), "{text}"); }
    assert_eq!(rtdb_pg_codec::from_transit(tagged("PT")).unwrap(), Value::IntervalYearMonth(0));
}
