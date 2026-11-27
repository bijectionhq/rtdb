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
