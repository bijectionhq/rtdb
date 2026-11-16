use rtdb_types::{Value, TimeUnit, Timestamp, parse_query_clock, parse_sql_timestamp_literal, parse_sql_date_literal};

#[test]
fn sql_timestamp_literals_preserve_local_time_and_resolve_offset_and_region_suffixes() {
    let local = parse_sql_timestamp_literal("2020-01-01 12:34:56.123456789").unwrap();
    assert_eq!(local, Value::Timestamp { value: 1_577_882_096_123_456, unit: TimeUnit::Microsecond, timezone: None });
    for text in ["2020-01-01T12:34:56.123456789Z", "2020-01-01T13:34:56.123456789+01", "2020-01-01T13:34:56.123456789[Europe/Paris]"] {
        let Value::Timestamp { value, timezone: Some(_), .. } = parse_sql_timestamp_literal(text).unwrap() else { panic!("Expected zoned timestamp") };
        assert_eq!(value, 1_577_882_096_123_456);
    }
    assert_eq!(parse_sql_date_literal("1970-01-01").unwrap(), Value::DateDay(0));
    for text in ["2020-01-01T1:02", "2020-01-01T12:34:60", "2020-01-01T12:34:56.1234567890", "2020-01-01+18:01", "2020-01-01[No/SuchZone]"] { assert!(parse_sql_timestamp_literal(text).is_err(), "{text}"); }
}

#[test]
fn timestamps_resolve_smart_dates_and_end_of_day_while_date_literals_stay_strict() {
    for (text, normalized) in [
        ("2020-02-30", "2020-02-29"), ("2019-02-31", "2019-02-28"), ("2020-04-31", "2020-04-30"),
        ("2020-01-01T24:00", "2020-01-02T00:00"), ("2020-12-31T24:00:00.000000000Z", "2021-01-01T00:00Z"),
        ("2020-02-31T24:00[Europe/Paris]", "2020-03-01T00:00[Europe/Paris]"),
        ("1-01-01", "0001-01-01"), ("+12-01-01", "0012-01-01"), ("-1-01-01", "-0001-01-01"),
        ("-0000-01-01", "0000-01-01"), ("000000001-01-01", "0001-01-01"),
    ] { assert_eq!(parse_sql_timestamp_literal(text).unwrap(), parse_sql_timestamp_literal(normalized).unwrap(), "{text}"); }
    for text in ["2020-02-32", "2020-00-01", "2020-01-00", "2020-01-01T24:01", "2020-01-01T24:00:00.000000001", "2020-01-01T25:00", "0000000001-01-01"] { assert!(parse_sql_timestamp_literal(text).is_err(), "{text}"); }
    for text in ["2020-02-30", "2019-02-29", "+2020-01-01", "-0000-01-01", "-00000-01-01", "10000-01-01", "-001-01-01"] { assert!(parse_sql_date_literal(text).is_err(), "{text}"); }
    for text in ["0000-01-01", "-0001-01-01", "+10000-01-01", "+00001-01-01"] { parse_sql_date_literal(text).unwrap(); }
    assert_eq!(parse_query_clock("2020-02-30T24:00", "UTC").unwrap(), parse_query_clock("2020-03-01Z", "UTC").unwrap());
    assert!(rtdb_types::validate_sql_time_literal("24:00:00").is_err());
}

#[test]
fn query_clock_uses_date_fallback_and_java_dst_resolution() {
    assert_eq!(parse_query_clock("2020-01-01Z", "Europe/Paris").unwrap(), Timestamp(1_577_836_800_000_000).into());
    assert_eq!(parse_query_clock("2020-01-01T12:34:56", "UTC").unwrap(), Timestamp(1_577_836_800_000_000).into());
    assert_eq!(parse_query_clock("2020-01-01T12:34:56Z", "UTC").unwrap(), Timestamp(1_577_882_096_000_000).into());
    let gap = parse_sql_timestamp_literal("2021-03-28T02:30[Europe/Paris]").unwrap();
    let overlap = parse_sql_timestamp_literal("2021-10-31T02:30[Europe/Paris]").unwrap();
    assert!(matches!(gap, Value::Timestamp { value: 1_616_895_000_000_000, .. }));
    assert!(matches!(overlap, Value::Timestamp { value: 1_635_640_200_000_000, .. }));
}

#[test]
fn date_only_region_precedes_offset_but_a_present_clock_preserves_offset_precedence() {
    let midnight = rtdb_types::Instant::parse_iso("2019-12-31T23:00:00Z").unwrap();
    let explicit = rtdb_types::Instant::parse_iso("2020-01-01T00:00:00Z").unwrap();
    for (text,expected) in [("2020-01-01Z[Europe/Paris]",midnight),("2020-01-01T00:00Z[Europe/Paris]",explicit)] {
        assert_eq!(rtdb_types::parse_sql_timestamp_instant(text,"UTC").unwrap(),expected);
        assert_eq!(rtdb_types::parse_sql_zoned_datetime(text).unwrap().0,expected);
        assert_eq!(parse_query_clock(text,"UTC").unwrap(),expected);
        assert!(matches!(parse_sql_timestamp_literal(text).unwrap(), Value::Timestamp { value, .. } if value == expected.as_micros().unwrap().0));
    }
    assert_eq!(rtdb_types::parse_sql_timestamp_instant("2020-01-01+01","UTC").unwrap(),midnight);
}

#[test]
fn clock_date_fallback_resolves_midnight_inside_a_partial_midnight_gap() {
    let expected = rtdb_types::Instant::parse_iso("1919-03-31T05:00:00Z").unwrap();
    assert_eq!(parse_query_clock("1919-03-31","America/Toronto").unwrap(),expected);
    assert_eq!(parse_query_clock("1919-03-31[America/Toronto]","UTC").unwrap(),expected);
    assert_eq!(parse_query_clock("1919-03-31T12:34","America/Toronto").unwrap(),expected);
}

#[test]
fn time_planning_validation_preserves_reference_fraction_and_offset_spelling() {
    for text in ["1:2:3", "01:02:03.123456789", "01:02:03.1234567890", "01:02:03+18:00"] { rtdb_types::validate_sql_time_literal(text).unwrap(); }
    for text in ["01:02", "01:02:03Z", "25:00:00", "01:02:60", "01:02:03.9223372036854775808", "01:02:03+18:01"] { assert!(rtdb_types::validate_sql_time_literal(text).is_err(), "{text}"); }
}

#[test]
fn sql_parser_resolves_wide_calendar_values_before_arrow_narrowing() {
    use rtdb_types::{CivilDate, parse_sql_timestamp_cast};
    let date = CivilDate::from_ymd(300_000, 7, 1).unwrap();
    let expected = i128::from(date.epoch_day() * 86_400 - 7200) * 1_000_000_000;
    assert_eq!(parse_sql_timestamp_cast("300000-07-01T00:00[Europe/Paris]", true).unwrap(), expected);
    assert!(parse_sql_timestamp_literal("300000-07-01Z").is_err());
    for year in [-999_999_999, 999_999_999] {
        let date = CivilDate::from_ymd(year, 1, 1).unwrap();
        assert_eq!(parse_sql_date_literal(&date.to_string()).unwrap(), Value::calendar_date(date, rtdb_types::DateUnit::Day));
    }
    assert_eq!(rtdb_types::parse_sql_local_datetime("2021-03-28T02:30[Europe/Paris]").unwrap().second_of_day(), 9000);
    let local = parse_sql_timestamp_cast("2021-03-28T02:30[Europe/Paris]", false).unwrap();
    assert_eq!(rtdb_types::CivilDateTime::from_epoch_nanos(local).unwrap().second_of_day(), 12_600);
    assert!(rtdb_types::parse_sql_local_datetime("2020-01-01").is_err());
}

#[test]
fn query_clock_string_retains_submicrosecond_fractions_and_negative_instants() {
    for (text,expected) in [("1970-01-01T00:00:00.000000999Z",rtdb_types::Instant::new(0,999).unwrap()),("1969-12-31T23:59:59.999999999Z",rtdb_types::Instant::new(-1,999_999_999).unwrap())] {
        assert_eq!(parse_query_clock(text,"UTC").unwrap(),expected);
    }
}
