use arrow_array::{Array, cast::AsArray, types::{Decimal128Type, Decimal256Type, Int32Type, Int64Type, TimestampMicrosecondType}};
use arrow_buffer::i256;
use arrow_schema::{DataType, TimeUnit};
use rtdb_columnar::MemoryBudget;
use rtdb_pg_codec::{Format, decode, decode_argument};
use rtdb_types::PgType;

#[test]
fn binary_timestamptz_bind_uses_the_zero_offset_arrow_timezone() {
    let memory = MemoryBudget::new(1 << 20);
    let binary = (-946_684_800_000_001i64).to_be_bytes();
    let expected = decode(PgType::TimestampTz, Format::Text, b"1969-12-31 23:59:59.999999Z", &memory).unwrap();
    let decoded = decode(PgType::TimestampTz, Format::Binary, &binary, &memory).unwrap();
    assert_eq!(decoded.value(), expected.value());

    let (field, array, _charge) = decode_argument("?_0", PgType::TimestampTz, Format::Binary, &binary, &memory).unwrap();
    assert_eq!(field.data_type(), &DataType::Timestamp(TimeUnit::Microsecond, Some("Z".into())));
    assert_eq!(array.data_type(), field.data_type());
    assert_eq!(array.as_primitive::<TimestampMicrosecondType>().value(0), -1);
}

#[test]
fn numeric_bind_writes_exact_coefficients_with_full_scale_metadata() {
    let input = b"-123456789012345678901234567890123e-200";
    let memory = MemoryBudget::new(8192 + input.len() * 64);
    let (field, array, charge) = decode_argument("?_0", PgType::Numeric, Format::Text, input, &memory).unwrap();
    assert_eq!(field.data_type(), &DataType::Decimal256(64, 1));
    assert_eq!(rtdb_types::decimal_arrow::decimal_scale(&field).unwrap(), Some(200));
    assert_eq!(array.as_primitive::<Decimal256Type>().value(0), i256::from_i128(-123456789012345678901234567890123));
    drop((array, charge));
    assert_eq!(memory.used_bytes(), 0);

    let binary: Vec<u8> = [2i16, 0, 0x4000, 3, 42, 1250].into_iter().flat_map(i16::to_be_bytes).collect();
    let (field, array, _) = decode_argument("?_0", PgType::Numeric, Format::Binary, &binary, &memory).unwrap();
    assert_eq!(field.data_type(), &DataType::Decimal128(32, 3));
    assert_eq!(array.as_primitive::<Decimal128Type>().value(0), -42125);

    let memory = MemoryBudget::new(1 << 20);
    let largest = "9".repeat(64);
    let (_, array, _) = decode_argument("?_0", PgType::Numeric, Format::Text, largest.as_bytes(), &memory).unwrap();
    assert_eq!(array.as_primitive::<Decimal256Type>().value(0), largest.parse().unwrap());
    let too_wide = format!("1{}", "0".repeat(64));
    let error = decode_argument("?_0", PgType::Numeric, Format::Text, too_wide.as_bytes(), &memory).err().unwrap();
    assert_eq!(error.message.as_deref(), Some("Decimal exceeds the pinned value writer precision"));
}

#[test]
fn array_bind_retains_canonical_offsets_validity_and_masked_bytes() {
    let text = b"{NULL,-12,NULL,37}";
    let memory = MemoryBudget::new(8192 + text.len() * 64 + 4 * 4096);
    let (_, array, _) = decode_argument("?_0", PgType::Int4s, Format::Text, text, &memory).unwrap();
    let list = array.as_list::<i32>();
    let child = list.values().as_primitive::<Int32Type>();
    assert_eq!(list.value_offsets(), &[0, 4]);
    assert_eq!(child.values().as_ref(), &[-12, -12, -12, 37]);
    assert_eq!(child.iter().collect::<Vec<_>>(), [None, Some(-12), None, Some(37)]);

    let binary: Vec<u8> = [1i32, 0, 23, 2, 1, 4, i32::MIN, 4, i32::MAX]
        .into_iter().flat_map(i32::to_be_bytes).collect();
    let (_, array, _) = decode_argument("?_0", PgType::Int8s, Format::Binary, &binary, &memory).unwrap();
    assert_eq!(array.as_list::<i32>().values().as_primitive::<Int64Type>().values().as_ref(), &[i64::from(i32::MIN), i64::from(i32::MAX)]);

    for (text, len, nullable) in [(b"{}".as_slice(), 0, false), (b"{NULL,NULL}".as_slice(), 2, true)] {
        let (field, array, _) = decode_argument("?_0", PgType::Int4s, Format::Text, text, &memory).unwrap();
        let DataType::List(item) = field.data_type() else { panic!("expected an array"); };
        assert_eq!(item.data_type(), &DataType::Null);
        assert_eq!(item.is_nullable(), nullable);
        assert_eq!(array.as_list::<i32>().values().len(), len);
    }
}

#[test]
fn typed_bind_keeps_legacy_decode_errors() {
    let memory = MemoryBudget::new(1 << 20);
    let invalid_integer: Vec<u8> = [1i32, 0, 23, 1, 1, -1].into_iter().flat_map(i32::to_be_bytes).collect();
    for (ty, format, bytes) in [
        (PgType::Numeric, Format::Text, b"1.2.3".as_slice()),
        (PgType::Numeric, Format::Binary, b"\0\x02\0\0\0\0\0\0".as_slice()),
        (PgType::Int4s, Format::Text, b"{{1}}".as_slice()),
        (PgType::Int8s, Format::Text, b"{1,9223372036854775808}".as_slice()),
        (PgType::Int4s, Format::Binary, invalid_integer.as_slice()),
    ] {
        let expected = decode(ty, format, bytes, &memory).err().unwrap();
        let actual = decode_argument("?_0", ty, format, bytes, &memory).err().unwrap();
        assert_eq!(actual, expected);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn bind_fixed_layouts_match_decoded_value_arrays_and_keep_their_charge() {
    let binary_interval: Vec<u8> = 1_234_567i64.to_be_bytes().into_iter()
        .chain((-2i32).to_be_bytes()).chain(3i32.to_be_bytes()).collect();
    let cases: &[(PgType, Format, &[u8])] = &[
        (PgType::Bytes, Format::Text, b"\\x0080ff"),
        (PgType::Bytes, Format::Text, "raw é".as_bytes()),
        (PgType::Uuid, Format::Text, b"6ebd7d7e-1f4b-4332-a36e-6048a03e38ed"),
        (PgType::Uuid, Format::Binary, &[0xfe; 16]),
        (PgType::Date, Format::Text, b"+285428-01-03"),
        (PgType::Date, Format::Binary, &(-10_958i32).to_be_bytes()),
        (PgType::Time, Format::Text, b"23:59:59.999999999"),
        (PgType::Time, Format::Binary, &86_399_999_999i64.to_be_bytes()),
        (PgType::Timestamp, Format::Text, b"1969-12-31T23:59:59.999999"),
        (PgType::Timestamp, Format::Binary, &i64::MAX.to_be_bytes()),
        (PgType::TimestampTz, Format::Text, b"1969-12-31T23:59:59.999999-04:00"),
        (PgType::TimestampTz, Format::Binary, &(-946_684_800_000_001i64).to_be_bytes()),
        (PgType::Interval, Format::Text, b"P2Y"),
        (PgType::Interval, Format::Text, b"P1DT0.000001S"),
        (PgType::Interval, Format::Text, b"PT0.000000001S"),
        (PgType::Interval, Format::Binary, &binary_interval),
        (PgType::Duration, Format::Text, b"-PT1.000001S"),
        (PgType::TsTzRange, Format::Text, b"[,1969-12-31T23:59:59.999999Z)"),
        (PgType::TsTzRange, Format::Binary, b"[1970-01-01T00:00:00Z,)"),
        (PgType::TsTzRange, Format::Text, b"empty"),
    ];
    let memory = MemoryBudget::new(1 << 20);
    for &(ty, format, bytes) in cases {
        let decoded = decode(ty, format, bytes, &memory).unwrap();
        let logical = decoded.value().logical_type();
        let expected_field = rtdb_columnar::to_field("?_0", &logical).unwrap();
        let expected_array = rtdb_columnar::build_array(&logical, std::slice::from_ref(decoded.value())).unwrap();
        drop(decoded);

        let (field, array, charge) = decode_argument("?_0", ty, format, bytes, &memory).unwrap();
        assert_eq!(field, expected_field, "{ty:?} {format:?} {bytes:?}");
        assert_eq!(array.to_data(), expected_array.to_data(), "{ty:?} {format:?} {bytes:?}");
        assert!(charge.bytes() >= array.get_array_memory_size() + field.size());
        assert_eq!(memory.used_bytes(), charge.bytes());
        drop((array, charge));
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn bind_fixed_layouts_keep_parse_and_range_errors() {
    let memory = MemoryBudget::new(1 << 20);
    let cases: &[(PgType, Format, &[u8])] = &[
        (PgType::Bytes, Format::Text, b"\\x1"),
        (PgType::Uuid, Format::Text, b"invalid"),
        (PgType::Uuid, Format::Binary, &[0; 15]),
        (PgType::Uuid, Format::Binary, &[0; 17]),
        (PgType::Date, Format::Binary, &i32::MAX.to_be_bytes()),
        (PgType::Time, Format::Binary, &86_400_000_000i64.to_be_bytes()),
        (PgType::Timestamp, Format::Text, b"1970-01-01T00:00:00Z"),
        (PgType::TimestampTz, Format::Text, b"1970-01-01T00:00:00"),
        (PgType::TimestampTz, Format::Binary, &[0; 9]),
        (PgType::Interval, Format::Binary, &i64::MAX.to_be_bytes()),
        (PgType::Duration, Format::Binary, &[0; 8]),
        (PgType::TsTzRange, Format::Text, b"[bad,same)"),
    ];
    for &(ty, format, bytes) in cases {
        let expected = decode(ty, format, bytes, &memory).err().unwrap();
        let actual = decode_argument("?_0", ty, format, bytes, &memory).err().unwrap();
        assert_eq!(actual, expected, "{ty:?} {format:?} {bytes:?}");
        assert_eq!(memory.used_bytes(), 0);
    }
    let error = decode_argument("?_0", PgType::Date, Format::Text, b"+999999999-12-31", &memory).err().unwrap();
    assert_eq!(error.code, "xtdb/date-out-of-range");
    assert_eq!(memory.used_bytes(), 0);
}
