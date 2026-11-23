use std::sync::Arc;

use arrow_array::{Array, Int64Array, StructArray};
use arrow_schema::{DataType, Field, Fields};
use rtdb_columnar::{build_array, hash_cell, hash_value, to_field, xxhash32};
use rtdb_types::{LogicalType, RunEndWidth, ScalarType, TimeUnit, Value};

#[test]
fn xxhash32_known_vectors() {
    assert_eq!(xxhash32(b"", 0), 0x02cc_5d05);
    assert_eq!(xxhash32(b"a", 0), 0x550d_7456);
    assert_eq!(xxhash32(b"abc", 0), 0x32d1_53ff);
}

#[test]
fn numeric_widths_hash_through_double_bits_in_big_endian_order() {
    let expected = xxhash32(&12.0f64.to_bits().to_be_bytes(), 42) as i32;
    for value in [Value::I8(12), Value::I16(12), Value::I32(12), Value::I64(12), Value::F32(12.0), Value::F64(12.0)] {
        assert_eq!(hash_value(&value, 42).unwrap(), expected);
    }
    assert_ne!(hash_value(&Value::F64(0.0), 0).unwrap(), hash_value(&Value::F64(-0.0), 0).unwrap());
}

#[test]
fn times_use_physical_bytes_while_timestamps_use_seconds() {
    let time = Value::Time { value: 1234, unit: TimeUnit::Microsecond, bit_width: 64 };
    assert_eq!(hash_value(&time, 0).unwrap(), xxhash32(&1234i64.to_le_bytes(), 0) as i32);
    let stamp = Value::Timestamp { value: 1234, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) };
    assert_eq!(hash_value(&stamp, 0).unwrap(), hash_value(&Value::F64(0.001234), 0).unwrap());
}

#[test]
fn hash_cell_uses_struct_field_order_and_ignores_names() {
    let fields = Fields::from(vec![Field::new("z", DataType::Int64, false), Field::new("a", DataType::Int64, false)]);
    let array = StructArray::try_new(fields.clone(), vec![Arc::new(Int64Array::from(vec![1])), Arc::new(Int64Array::from(vec![2]))], None).unwrap();
    let field = Field::new("value", DataType::Struct(fields), false);
    let one = hash_value(&Value::I64(1), 0).unwrap();
    let two = hash_value(&Value::I64(2), 0).unwrap();
    assert_eq!(hash_cell(&field, &array, 0, 0).unwrap(), one.wrapping_mul(31).wrapping_add(two));
}

#[test]
fn encoded_and_plain_cells_have_identical_hashes() {
    let plain = LogicalType::Scalar(ScalarType::I64).maybe();
    let encoded = LogicalType::RunEndEncoded { width: RunEndWidth::I32, value: Box::new(plain.clone()) };
    let values = vec![Value::I64(1), Value::I64(1), Value::Null, Value::I64(2)];
    let a = build_array(&plain, &values).unwrap();
    let b = build_array(&encoded, &values).unwrap();
    let af = to_field("value", &plain).unwrap();
    let bf = to_field("value", &encoded).unwrap();
    for row in 0..a.len() { assert_eq!(hash_cell(&af, a.as_ref(), row, 7).unwrap(), hash_cell(&bf, b.as_ref(), row, 7).unwrap()); }
}

#[test]
fn set_hash_is_order_independent_and_list_hash_preserves_order() {
    let a = Value::I64(1);
    let b = Value::I64(2);
    assert_eq!(hash_value(&Value::Set(vec![a.clone(), b.clone()]), 0).unwrap(), hash_value(&Value::Set(vec![b.clone(), a.clone()]), 0).unwrap());
    assert_ne!(hash_value(&Value::list(vec![a.clone(), b.clone()]), 0).unwrap(), hash_value(&Value::list(vec![b, a]), 0).unwrap());
}
#[test]
fn murmur3_x86_vectors_include_nonempty_tails() {
    assert_eq!(rtdb_columnar::murmur3_32(b"", 0), 0);
    assert_eq!(rtdb_columnar::murmur3_32(b"foo", 0), 0xf6a5_c420);
    assert_eq!(rtdb_columnar::murmur3_32(b"hello", 0), 0x248b_fa47);
}

#[test]
fn murmur_vector_hash_uses_signed_widths_ieee_bytes_and_null_zero() {
    use rtdb_columnar::{hash_value_murmur3, murmur3_32};
    assert_eq!(hash_value_murmur3(&Value::Null, 91).unwrap(), 0);
    assert_eq!(hash_value_murmur3(&Value::Bool(true), 91).unwrap(), 17);
    assert_eq!(hash_value_murmur3(&Value::Bool(false), 91).unwrap(), 19);
    assert_eq!(hash_value_murmur3(&Value::Utf8("foo".into()), 0).unwrap() as u32, 0xf6a5_c420);
    let minus_one = murmur3_32(&(-1.0f64).to_bits().to_be_bytes(), 7) as i32;
    for value in [Value::U8(255), Value::U16(65535), Value::U32(u32::MAX), Value::U64(u64::MAX), Value::I64(-1), Value::F64(-1.0)] {
        assert_eq!(hash_value_murmur3(&value, 7).unwrap(), minus_one);
    }
    assert_eq!(hash_value_murmur3(&Value::list(vec![Value::Bool(true), Value::Bool(false)]), 0).unwrap(), 17 * 31 + 19);
    assert_eq!(hash_value_murmur3(&Value::Time { value: 1234, unit: TimeUnit::Microsecond, bit_width: 64 }, 0).unwrap(), murmur3_32(&1234i64.to_le_bytes(), 0) as i32);
    assert_ne!(hash_value_murmur3(&Value::F64(-0.0), 0).unwrap(), hash_value_murmur3(&Value::F64(0.0), 0).unwrap());
}
