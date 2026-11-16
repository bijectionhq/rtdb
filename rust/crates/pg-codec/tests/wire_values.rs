use std::collections::BTreeMap;
use rtdb_columnar::MemoryBudget;
use rtdb_pg_codec::{Format, SessionEnvironment, decode, encode, encode_value, read_text};
use rtdb_types::{PgType, TimeUnit, Value};

fn output(ty: PgType, format: Format, value: &Value) -> Vec<u8> { encode_value(ty, format, value, &SessionEnvironment::default()).unwrap() }

#[test]
fn numeric_base_10000_matches_pinned_numeric_bin_including_negative_zero_sign() {
    for (text, words) in [
        ("12345.6789", vec![3i16, 1, 0, 4, 1, 2345, 6789]),
        ("-0.12", vec![1, -1, 0x4000, 2, 1200]),
        ("0", vec![0, -1, 0x4000, 0]),
        ("1E+4", vec![2, 1, 0, 0, 1, 0]),
    ] {
        let value = read_text(1700, text).unwrap();
        let expected: Vec<u8> = words.into_iter().flat_map(i16::to_be_bytes).collect();
        assert_eq!(output(PgType::Numeric, Format::Binary, &value), expected, "{text}");
        let decoded = decode(PgType::Numeric, Format::Binary, &expected, &MemoryBudget::new(1 << 20)).unwrap();
        assert!(rtdb_expr::sql_eq(&value, decoded.value()).unwrap().unwrap());
    }
}

#[test]
fn numeric_decoding_rejects_special_signs_truncated_digits_and_excessive_scale() {
    let memory = MemoryBudget::new(1 << 20);
    for words in [vec![1i16, 0, -16384, 0, 0], vec![2, 0, 0, 0, 1], vec![1, 0, 0, 128, 1], vec![1, 0, 0, 0, 10000]] {
        let bytes: Vec<_> = words.into_iter().flat_map(i16::to_be_bytes).collect();
        assert!(decode(PgType::Numeric, Format::Binary, &bytes, &memory).is_err());
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn timestamp_binary_writers_keep_local_truncation_and_zoned_flooring() {
    let epoch = 946_684_800_000_000i64;
    let local = Value::Timestamp { value: -1, unit: TimeUnit::Nanosecond, timezone: None };
    let zoned = Value::Timestamp { value: -1, unit: TimeUnit::Nanosecond, timezone: Some("UTC".into()) };
    assert_eq!(output(PgType::Timestamp, Format::Binary, &local), (-epoch).to_be_bytes());
    assert_eq!(output(PgType::TimestampTz, Format::Binary, &zoned), (-epoch - 1).to_be_bytes());
    assert_eq!(output(PgType::TimestampTz, Format::Text, &zoned), b"1969-12-31 23:59:59.999999+00:00");
}

#[test]
fn local_time_date_only_timestamps_and_region_offsets_follow_sql_timestamp_parser() {
    assert_eq!(read_text(1083, "12:34").unwrap(), Value::Time { value: 45_240_000_000_000, unit: TimeUnit::Nanosecond, bit_width: 64 });
    assert_eq!(read_text(1114, "1970-01-01").unwrap(), Value::Timestamp { value: 0, unit: TimeUnit::Microsecond, timezone: None });
    assert_eq!(read_text(1184, "1970-01-01[UTC]").unwrap(), Value::Timestamp { value: 0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) });
    assert_eq!(read_text(1184, "1970-01-01T00:00+01").unwrap(), Value::Timestamp { value: -3_600_000_000, unit: TimeUnit::Microsecond, timezone: Some("+01:00".into()) });
    assert_eq!(read_text(1184, "1970-01-01T00:00-00:01:15").unwrap(), Value::Timestamp { value: 75_000_000, unit: TimeUnit::Microsecond, timezone: Some("-00:01:15".into()) });
    let zone = read_text(1184, "1970-01-01T00:00+01:00[Europe/Paris]").unwrap();
    assert_eq!(output(PgType::TimestampTz, Format::Text, &zone), b"1970-01-01 00:00:00+01:00");
    assert!(read_text(1083, "23:59:60").is_err());
    assert!(read_text(1184, "1970-01-01").is_err());
    assert_eq!(output(PgType::Time, Format::Text, &Value::Time { value: 100_000_000, unit: TimeUnit::Nanosecond, bit_width: 64 }), b"00:00:00.1");
}

#[test]
fn interval_and_oid_binary_output_follow_the_pinned_textual_writers() {
    let interval = Value::IntervalMonthDayNano { months: 14, days: 3, nanoseconds: 1_000_000_999 };
    for format in [Format::Text, Format::Binary] {
        assert_eq!(output(PgType::Interval, format, &interval), b"P1Y2M3DT1S");
        assert_eq!(output(PgType::Interval, format, &Value::IntervalYearMonth(0)), b"P");
        assert_eq!(output(PgType::Oid, format, &Value::Oid(-1)), b"4294967295");
    }
}

#[test]
fn text_arrays_keep_quoted_null_distinct_and_binary_null_read_is_unsupported() {
    let value = Value::List(vec![Value::Utf8("NULL".into()), Value::Null, Value::Utf8("a,\"b\\c".into())]);
    let text = output(PgType::Texts, Format::Text, &value);
    assert_eq!(read_text(1009, std::str::from_utf8(&text).unwrap()).unwrap(), value);
    let binary = output(PgType::Texts, Format::Binary, &value);
    assert!(decode(PgType::Texts, Format::Binary, &binary, &MemoryBudget::new(1 << 20)).is_err());
    assert!(encode_value(PgType::Int8s, Format::Binary, &Value::List(vec![Value::Null]), &SessionEnvironment::default()).is_err());
}

#[test]
fn json_keeps_large_integer_and_decimal_digits_and_jsonb_requires_version_one() {
    let decimal = read_text(1700, "123456789012345678901234567890.1234").unwrap();
    let value = Value::Struct(BTreeMap::from([("n".into(), decimal)]));
    let json = output(PgType::Json, Format::Text, &value);
    assert_eq!(json, br#"{"n":123456789012345678901234567890.1234}"#);
    let memory = MemoryBudget::new(1 << 20);
    let mut binary = vec![1]; binary.extend_from_slice(&json);
    assert_eq!(decode(PgType::Jsonb, Format::Binary, &binary, &memory).unwrap().value(), &value);
    binary[0] = 2;
    assert_eq!(decode(PgType::Jsonb, Format::Binary, &binary, &memory).err().unwrap().code, "xtdb.pgwire/unsupported-jsonb-version");
}

#[test]
fn java_float_text_thresholds_and_subnormal_digits_are_preserved() {
    for (value, expected) in [(0.001, "0.001"), (0.0001, "1.0E-4"), (10_000_000.0, "1.0E7"), (-0.0, "-0.0"), (f64::from_bits(1), "4.9E-324")] {
        assert_eq!(output(PgType::Float8, Format::Text, &Value::F64(value)), expected.as_bytes());
    }
    assert_eq!(output(PgType::Float4, Format::Text, &Value::F32(f32::from_bits(1))), b"1.4E-45");
}

#[test]
fn codec_owners_hold_budget_until_the_payload_is_released() {
    let memory = MemoryBudget::new(1 << 20);
    let decoded = decode(PgType::Text, Format::Text, b"retained", &memory).unwrap();
    let (value, charge) = decoded.into_parts();
    assert!(memory.used_bytes() >= value.retained_bytes().unwrap());
    let encoded = encode(PgType::Text, Format::Binary, &value, &SessionEnvironment::default(), &memory).unwrap().unwrap();
    drop(value); drop(charge);
    assert!(memory.used_bytes() >= encoded.as_slice().len());
    drop(encoded); assert_eq!(memory.used_bytes(), 0);
    assert!(decode(PgType::Text, Format::Text, b"x", &MemoryBudget::new(1)).is_err());
}

#[test]
fn session_environment_retains_copied_parameters_and_selects_fallback() {
    let memory = MemoryBudget::new(1 << 20);
    let params = [("fallback_output_format", Some("transit")), ("datestyle", None)];
    let environment = SessionEnvironment::new(params.into_iter(), "Europe/Paris", &memory).unwrap();
    assert_eq!(environment.parameter("timezone"), Some("Europe/Paris"));
    assert_eq!(environment.fallback(), PgType::Transit);
    assert!(memory.used_bytes() > 0); drop(environment); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn malformed_arrays_and_unbounded_interval_exponents_are_rejected() {
    for value in ["{\"unfinished}", "{{1}}", "{a\\}"] { assert!(read_text(1009, value).is_err()); }
    assert!(read_text(1186, "PT1e999999999S").is_err());
}
