use rtdb_types::StructFields;
use rtdb_columnar::MemoryBudget;
use rtdb_pg_codec::{Format, SessionEnvironment, decode, encode, encode_value, read_text};
use rtdb_types::{PgType, TimeUnit, Value};

fn output(ty: PgType, format: Format, value: &Value) -> Vec<u8> { encode_value(ty, format, value, &SessionEnvironment::default()).unwrap() }

#[test]
fn nullable_error_messages_keep_each_wire_formats_presence_contract() {
    use rtdb_types::{Anomaly, AnomalyCategory};
    use rtdb_format::transit::{decode_anomaly, decode_json, encode_msgpack};
    for message in [None, Some(String::new()), Some("detail".into())] {
        let error = Anomaly::new_nullable(AnomalyCategory::Fault, "test/error", message.clone());
        let json: serde_json::Value = serde_json::from_slice(&rtdb_pg_codec::encode_error(&error, PgType::Json).unwrap()).unwrap();
        assert_eq!(json.get("message"), message.as_ref().map(|message| serde_json::Value::String(message.clone())).as_ref());
        let ld = rtdb_pg_codec::encode_error(&error, PgType::JsonLd).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&ld).unwrap();
        assert_eq!(value["@value"].get("xtdb.error/message"), Some(&serde_json::to_value(&message).unwrap()));
        let Value::Transit(bytes) = read_text(114, std::str::from_utf8(&ld).unwrap()).unwrap() else { panic!("error value") };
        assert_eq!(decode_anomaly(&bytes).unwrap().message, message);
        let transit = decode_json(&rtdb_pg_codec::encode_error(&error, PgType::Transit).unwrap()).unwrap();
        assert_eq!(decode_anomaly(&encode_msgpack(&transit).unwrap()).unwrap(), error);
    }
    for message in ["", ",\"xtdb.error/message\":null", ",\"xtdb.error/message\":42"] {
        let text = format!(r#"{{"@type":"xt:fault","@value":{{"xtdb.error/data":{{}}{message}}}}}"#);
        let Value::Transit(bytes) = read_text(114, &text).unwrap() else { panic!("error value") };
        assert!(decode_anomaly(&bytes).unwrap().message.is_none());
    }
}

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
        assert!(rtdb_expr::sql_eq(&value, decoded.value(), &equality_scope()).unwrap().unwrap());
    }
}

#[test]
fn numeric_decoding_rejects_truncated_digits_and_excessive_coefficients() {
    let memory = MemoryBudget::new(1 << 20);
    for words in [vec![2i16, 0, 0, 0, 1], vec![1, 0, 0, 128, 1]] {
        let bytes: Vec<_> = words.into_iter().flat_map(i16::to_be_bytes).collect();
        assert!(decode(PgType::Numeric, Format::Binary, &bytes, &memory).is_err());
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn numeric_reader_keeps_pinned_short_header_and_digit_semantics() {
    for (words, expected) in [
        (vec![0i16], "0"),
        (vec![0, 123], "0"),
        (vec![-1, 0, 0, -129], "0E+129"),
        (vec![1, 0, -16384, 0, 1], "-1"),
        (vec![1, 0, 0, 0, 10000], "10000"),
        (vec![1, 0, 0, 0, -1], "-1"),
        (vec![1, -33, 0, 129, 1000], "1E-129"),
        (vec![1, -32768, 0, 32767, 1], "0E-32767"),
    ] {
        let bytes: Vec<_> = words.into_iter().flat_map(i16::to_be_bytes).collect();
        let decoded = decode(PgType::Numeric, Format::Binary, &bytes, &MemoryBudget::new(1 << 20)).unwrap();
        assert_eq!(output(PgType::Numeric, Format::Text, decoded.value()), expected.as_bytes());
    }
}

#[test]
fn numeric_output_keeps_full_scale_and_wrapped_wire_fields() {
    let value = read_text(1700, "1E-129").unwrap();
    assert_eq!(output(PgType::Numeric, Format::Binary, &value), [1i16, -33, 0, 129, 1000].into_iter().flat_map(i16::to_be_bytes).collect::<Vec<_>>());
    assert_eq!(output(PgType::Numeric, Format::Text, &value), b"1E-129");
    let value = read_text(1700, "1E-2147483647").unwrap();
    assert_eq!(output(PgType::Numeric, Format::Binary, &value), [1i16, -1, 0, -1, 10].into_iter().flat_map(i16::to_be_bytes).collect::<Vec<_>>());
    assert_eq!(output(PgType::Numeric, Format::Text, &value), b"1E-2147483647");
    let value = read_text(1700, "1E+131072").unwrap();
    let bytes = output(PgType::Numeric, Format::Binary, &value);
    assert_eq!(&bytes[..10], [-32767i16, -32768, 0, 0, 1].into_iter().flat_map(i16::to_be_bytes).collect::<Vec<_>>());
    assert_eq!(bytes.len(), 8 + 2 * 32769);
    assert!(bytes[10..].iter().all(|byte| *byte == 0));
}

#[test]
fn negative_scale_binary_output_reserves_expansion_before_encoding() {
    let value = read_text(1700, "1E+100000").unwrap();
    let small = MemoryBudget::new(16 << 10);
    assert!(encode(PgType::Numeric, Format::Binary, &value, &SessionEnvironment::default(), &small).is_err());
    assert_eq!(small.used_bytes(), 0);
    let memory = MemoryBudget::new(1 << 20);
    let encoded = encode(PgType::Numeric, Format::Binary, &value, &SessionEnvironment::default(), &memory).unwrap().unwrap();
    assert_eq!(encoded.as_slice().len(), 8 + 2 * 25001);
    assert!(memory.used_bytes() >= encoded.as_slice().len());
    drop(encoded);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn decimal_text_and_tagged_values_keep_full_scale_without_zero_expansion() {
    for (text, scale) in [("1E+129", -129), ("1E-129", 129), ("1E+2147483648", i32::MIN), ("1E-2147483647", i32::MAX)] {
        let value = read_text(1700, text).unwrap();
        assert!(matches!(&value, Value::Decimal(value) if value.scale == scale));
        assert_eq!(output(PgType::Numeric, Format::Text, &value), text.as_bytes());
        let tagged = serde_json::json!({"@type":"xt:decimal","@value":text}).to_string();
        assert_eq!(read_text(114, &tagged).unwrap(), value);
        let transit = rtdb_format::transit::TransitValue::Tagged { tag: "f".into(), representation: Box::new(rtdb_format::transit::TransitValue::String(text.into())) };
        assert_eq!(rtdb_pg_codec::from_transit(transit).unwrap(), value);
    }
    for text in ["1E-2147483648", "1E+2147483649", "1_0"] { assert!(read_text(1700, text).is_err()); }
    assert_eq!(read_text(1700, "١.٢٥").unwrap(), read_text(1700, "1.25").unwrap());
    assert_eq!(read_text(1700, "１E+２１４７４８３６４８").unwrap(), read_text(1700, "1E+2147483648").unwrap());
    assert!(read_text(1700, "𝟙").is_err());
    for coefficient in ["0", "1"] {
        let value = read_text(1700, &format!("{coefficient}E+2147483648")).unwrap();
        assert!(encode_value(PgType::Numeric, Format::Binary, &value, &SessionEnvironment::default()).is_err());
    }
}

#[test]
fn binary_timestamps_send_the_microsecond_their_text_form_shows() {
    let epoch = 946_684_800_000_000i64;
    let local = Value::Timestamp { value: -1, unit: TimeUnit::Nanosecond, timezone: None };
    let zoned = Value::Timestamp { value: -1, unit: TimeUnit::Nanosecond, timezone: Some("UTC".into()) };
    assert_eq!(output(PgType::Timestamp, Format::Binary, &local), (-epoch - 1).to_be_bytes());
    assert_eq!(output(PgType::Timestamp, Format::Text, &local), b"1969-12-31 23:59:59.999999");
    assert_eq!(output(PgType::TimestampTz, Format::Binary, &zoned), (-epoch - 1).to_be_bytes());
    assert_eq!(output(PgType::TimestampTz, Format::Text, &zoned), b"1969-12-31 23:59:59.999999+00:00");

    let pre_1970 = Value::Timestamp { value: -2_147_483_647_111_157_001, unit: TimeUnit::Nanosecond, timezone: None };
    assert_eq!(output(PgType::Timestamp, Format::Text, &pre_1970), b"1901-12-13 20:45:52.888842");
    assert_eq!(output(PgType::Timestamp, Format::Binary, &pre_1970), (-2_147_483_647_111_158i64 - epoch).to_be_bytes());
    for (value, unit) in [(-1i64, TimeUnit::Second), (-1, TimeUnit::Millisecond)] {
        let local = Value::Timestamp { value, unit, timezone: None };
        let micros = if unit == TimeUnit::Second { -1_000_000 } else { -1_000 };
        assert_eq!(output(PgType::Timestamp, Format::Binary, &local), (micros - epoch).to_be_bytes());
    }
}

#[test]
fn transit_legacy_dates_accept_epoch_milliseconds_in_json_and_messagepack() {
    use rtdb_format::transit::{TransitValue as T, decode_json, decode_msgpack, encode_msgpack};
    use rtdb_pg_codec::from_transit;

    let tagged = |representation| T::Tagged { tag: "m".into(), representation: Box::new(representation) };
    let value = Value::Timestamp { value: -1000, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) };
    let msgpack = encode_msgpack(&tagged(T::Integer(-1))).unwrap();
    assert_eq!(from_transit(decode_msgpack(&msgpack).unwrap()).unwrap(), value);
    assert_eq!(from_transit(decode_json(br#"["~#'","~m-1"]"#).unwrap()).unwrap(), value);

    for representation in [T::Bool(false), T::Integer(i64::MAX), T::String("not milliseconds".into())] {
        let error = from_transit(tagged(representation)).unwrap_err();
        assert_eq!(error.code, "xtdb.postgres/invalid-column-value");
    }
}

#[test]
fn millisecond_date_text_uses_the_containing_day_in_each_output_family() {
    for (millis, expected) in [(-86_400_001, "1969-12-30"), (-86_399_999, "1969-12-31"), (-1, "1969-12-31"), (0, "1970-01-01"), (86_399_999, "1970-01-01"), (86_400_001, "1970-01-02")] {
        let value = Value::DateMilli(millis);
        assert_eq!(output(PgType::Date, Format::Text, &value), expected.as_bytes());
        for format in [Format::Text, Format::Binary] {
            assert_eq!(output(PgType::Json, format, &value), serde_json::to_vec(expected).unwrap());
            assert_eq!(serde_json::from_slice::<serde_json::Value>(&output(PgType::JsonLd, format, &value)).unwrap(), serde_json::json!({"@type": "xt:date", "@value": expected}));
            assert_eq!(rtdb_format::transit::decode_json(&output(PgType::Transit, format, &value)).unwrap(), rtdb_format::transit::TransitValue::Tagged {
                tag: "time/date".into(), representation: Box::new(rtdb_format::transit::TransitValue::String(expected.into())),
            });
        }
    }
}

#[test]
fn binary_dates_use_checked_postgres_days_for_both_vector_units() {
    let memory = MemoryBudget::new(1 << 20);
    for (millis, unix_day) in [(-86_400_001, -2i32), (-1, -1), (0, 0), (86_400_001, 1)] {
        let bytes = (unix_day - 10_957).to_be_bytes();
        assert_eq!(output(PgType::Date, Format::Binary, &Value::DateMilli(millis)), bytes);
        assert_eq!(output(PgType::Date, Format::Binary, &Value::DateDay(unix_day)), bytes);
    }
    let error = encode(PgType::Date, Format::Binary, &Value::DateMilli(i64::MAX), &SessionEnvironment::default(), &memory).err().unwrap();
    assert_eq!(error.code, "xtdb/date-out-of-range");
    assert_eq!(memory.used_bytes(), 0);
    assert!(encode(PgType::Date, Format::Binary, &Value::Null, &SessionEnvironment::default(), &memory).unwrap().is_none());
    assert_eq!(memory.used_bytes(), 0);
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
fn protocol_calendar_values_cover_wide_dates_and_future_region_rules() {
    let day = rtdb_types::CivilDate::from_ymd(280_000,7,1).unwrap().epoch_day();
    let value = read_text(1184,"280000-07-01T00:00[Europe/Paris]").unwrap();
    assert_eq!(value,Value::Timestamp { value: day * 86_400_000_000 - 7_200_000_000,unit: TimeUnit::Microsecond,timezone: Some("Europe/Paris".into()) });
    assert_eq!(output(PgType::TimestampTz,Format::Text,&value),b"+280000-07-01 00:00:00+02:00");
    for days in [i32::MIN,i32::MAX] {
        let text = rtdb_types::CivilDate::from_epoch_day(i64::from(days)).unwrap().to_string();
        assert_eq!(output(PgType::Date,Format::Text,&Value::DateDay(days)),text.as_bytes());
        assert_eq!(read_text(1082,&text).unwrap(),Value::DateDay(days));
    }
    let date = rtdb_types::CivilDate::from_ymd(999_999_999,1,1).unwrap();
    let mut value = read_text(1082,&date.to_string()).unwrap();
    assert_eq!(value,Value::calendar_date(date,rtdb_types::DateUnit::Day));
    assert!(encode_value(PgType::Date,Format::Text,&value,&SessionEnvironment::default()).is_err());
    let error = rtdb_columnar::admit_execution_value(&mut value).unwrap_err();
    assert_eq!(error.category, rtdb_types::AnomalyCategory::Incorrect);
    assert_eq!(error.code, "xtdb/date-out-of-range");
    assert_eq!(value,Value::calendar_date(date,rtdb_types::DateUnit::Day));
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
    let value = Value::list(vec![Value::Utf8("NULL".into()), Value::Null, Value::Utf8("a,\"b\\c".into())]);
    let text = output(PgType::Texts, Format::Text, &value);
    assert_eq!(read_text(1009, std::str::from_utf8(&text).unwrap()).unwrap(), value);
    let binary = output(PgType::Texts, Format::Binary, &value);
    assert!(decode(PgType::Texts, Format::Binary, &binary, &MemoryBudget::new(1 << 20)).is_err());
    assert!(encode_value(PgType::Int8s, Format::Binary, &Value::list(vec![Value::Null]), &SessionEnvironment::default()).is_err());
}

#[test]
fn json_keeps_large_integer_and_decimal_digits_and_jsonb_requires_version_one() {
    let decimal = read_text(1700, "123456789012345678901234567890.1234").unwrap();
    let value = Value::Struct(StructFields::from([("n".into(), decimal)]));
    let json = output(PgType::Json, Format::Text, &value);
    assert_eq!(json, br#"{"n":123456789012345678901234567890.1234}"#);
    let memory = MemoryBudget::new(1 << 20);
    let mut binary = vec![1]; binary.extend_from_slice(&json);
    assert_eq!(decode(PgType::Jsonb, Format::Binary, &binary, &memory).unwrap().value(), &Value::Struct(StructFields::from([("n".into(), Value::F64(123456789012345678901234567890.1234))])));
    binary[0] = 2;
    assert_eq!(decode(PgType::Jsonb, Format::Binary, &binary, &memory).err().unwrap().code, "xtdb.pgwire/unsupported-jsonb-version");
}

#[test]
fn java_float_text_thresholds_and_subnormal_digits_are_preserved() {
    assert_eq!(output(PgType::Float8, Format::Text, &Value::F64(f64::from_bits(2))), b"9.9E-324");
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

fn equality_scope() -> rtdb_expr::EvaluationScope {
    rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap()
}

#[test]
fn transit_preserves_postgres_reference_types_through_nested_values() {
    use rtdb_format::transit::{TransitValue as T, decode_json, encode_json};
    let value = Value::list(vec![Value::Oid(-12), Value::RegClass(123), Value::RegProc(i32::MIN)]);
    let bytes = encode_json(&rtdb_pg_codec::to_transit(&value).unwrap()).unwrap();
    assert_eq!(rtdb_pg_codec::from_transit(decode_json(&bytes).unwrap()).unwrap(), value);
    let data = T::Tagged { tag: "xt/regclass".into(), representation: Box::new(T::Integer(i64::MAX)) };
    assert!(rtdb_pg_codec::from_transit(data).is_err());
}

#[test]
fn jvm_transit_preserves_nested_lists_and_uuid_words() {
    use rtdb_format::transit::{decode_json, decode_msgpack};
    use rtdb_pg_codec::from_transit;
    let lists = b"\x92\xa6~#list\x92\x92\xa2^0\x91\x01\x92\xa2^0\x90";
    assert_eq!(from_transit(decode_msgpack(lists).unwrap()).unwrap(),
        Value::list(vec![Value::list(vec![Value::I64(1)]), Value::list(vec![])]));
    let uuid = b"\x92\xa3~#'\x92\xa3~#u\x92\xff\xd3\x87\x65\x43\x21\xab\xcd\xef\x09";
    let text = "ffffffff-ffff-ffff-8765-4321abcdef09";
    assert_eq!(from_transit(decode_msgpack(uuid).unwrap()).unwrap(), read_text(2950, text).unwrap());
    assert_eq!(from_transit(decode_json(format!(r#""~u{text}""#).as_bytes()).unwrap()).unwrap(), read_text(2950, text).unwrap());
    for data in [br#"["~#u",[0]]"#.as_slice(), br#"["~#u",[0,"1"]]"#, br#"["~#list",1]"#] {
        assert!(from_transit(decode_json(data).unwrap()).is_err());
    }
}

#[test]
fn uuid_columns_use_the_pg_uuid_type_with_hyphenated_lowercase_text_and_raw_binary() {
    use rtdb_types::{LogicalType, ScalarType};
    let bytes = [0x9f, 0x8e, 0x7d, 0x6c, 0x5b, 0x4a, 0x39, 0x28, 0x17, 0x06, 0xf5, 0xe4, 0xd3, 0xc2, 0xb1, 0xa0];
    let ty = PgType::from_logical(&LogicalType::Scalar(ScalarType::Uuid)).unwrap();
    assert_eq!(ty, PgType::Uuid);
    assert_eq!((ty.descriptor().oid, ty.descriptor().length), (2950, 16));
    assert_eq!(PgType::from_logical(&LogicalType::Scalar(ScalarType::Uuid).maybe()).unwrap(), PgType::Uuid);
    let value = Value::Uuid(bytes);
    assert_eq!(output(ty, Format::Text, &value), b"9f8e7d6c-5b4a-3928-1706-f5e4d3c2b1a0");
    assert_eq!(output(ty, Format::Binary, &value), bytes);
    let memory = MemoryBudget::new(1 << 20);
    assert_eq!(decode(ty, Format::Text, b"9F8E7D6C-5B4A-3928-1706-F5E4D3C2B1A0", &memory).unwrap().value(), &value);
    assert_eq!(decode(ty, Format::Binary, &bytes, &memory).unwrap().value(), &value);
    assert!(decode(ty, Format::Binary, &bytes[..15], &memory).is_err());
    for format in [Format::Text, Format::Binary] {
        assert!(encode(ty, format, &Value::Null, &SessionEnvironment::default(), &memory).unwrap().is_none());
    }
}

#[test]
fn fixed_size_binary_columns_are_bytea_with_lowercase_hex_text_and_raw_binary() {
    use rtdb_types::{LogicalType, ScalarType};
    let ty = PgType::from_logical(&LogicalType::Scalar(ScalarType::FixedSizeBinary(4))).unwrap();
    assert_eq!(ty, PgType::Bytes);
    assert_eq!(ty.descriptor().oid, 17);
    for (bytes, text) in [(vec![0xde, 0xad, 0xbe, 0xef], &b"\\xdeadbeef"[..]), (vec![0, 1, 0x0a, 0xff], b"\\x00010aff")] {
        let value = Value::FixedSizeBinary(bytes.clone());
        assert_eq!(output(ty, Format::Text, &value), text);
        assert_eq!(output(ty, Format::Binary, &value), bytes);
        let memory = MemoryBudget::new(1 << 20);
        assert_eq!(decode(ty, Format::Text, text, &memory).unwrap().value(), &Value::Binary(bytes.clone()));
        assert_eq!(decode(ty, Format::Binary, &bytes, &memory).unwrap().value(), &Value::Binary(bytes));
    }
    assert_eq!(output(ty, Format::Text, &Value::FixedSizeBinary(vec![])), b"\\x");
}

#[test]
fn rendered_structs_omit_their_null_fields_as_the_reference_reads_them() {
    let value = Value::Struct(StructFields::from([("a".into(), Value::I64(1)), ("b".into(), Value::Null), ("c".into(), Value::list(vec![Value::Struct(StructFields::from([("d".into(), Value::Null)]))]))]));
    for format in [Format::Text, Format::Binary] {
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&output(PgType::Json, format, &value)).unwrap(), serde_json::json!({"a": 1, "c": [{}]}));
    }
    let transit = rtdb_pg_codec::to_transit(&value).unwrap();
    assert_eq!(rtdb_format::transit::encode_json(&transit).unwrap(), br#"["^ ","a",1,"c",[["^ "]]]"#);
}
