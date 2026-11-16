use std::{cmp::Ordering, collections::BTreeMap};
use rtdb_expr::{compare, ordered_compare_in_timezone, validate_order_type};
use rtdb_types::{DecimalValue, IntervalUnit, LogicalType as T, ScalarType as S, Value as V};

fn decimal(value: i128) -> V { V::Decimal(DecimalValue::new(value.to_be_bytes().to_vec(), 20, 0, 128).unwrap()) }

#[test]
fn ordered_decimal_ties_use_double_while_scalar_comparison_remains_exact() {
    let left = decimal(9_007_199_254_740_993);
    let right = decimal(9_007_199_254_740_992);
    assert_eq!(ordered_compare_in_timezone(&left, &right, false, "UTC").unwrap(), Ordering::Equal);
    assert_eq!(compare(&left, &right, false).unwrap(), Ordering::Greater);
    assert_eq!(ordered_compare_in_timezone(&V::I64(9_007_199_254_740_993), &V::I64(9_007_199_254_740_992), false, "UTC").unwrap(), Ordering::Greater);
}

#[test]
fn ordered_double_comparison_canonicalizes_nan_and_distinguishes_signed_zero() {
    assert_eq!(ordered_compare_in_timezone(&V::F64(-0.0), &V::F64(0.0), false, "UTC").unwrap(), Ordering::Less);
    assert_eq!(ordered_compare_in_timezone(&V::F64(f64::from_bits(0xfff8_0000_0000_0001)), &V::F64(f64::from_bits(0x7ff8_0000_0000_0010)), false, "UTC").unwrap(), Ordering::Equal);
    assert_eq!(ordered_compare_in_timezone(&V::F64(f64::NAN), &V::F64(f64::INFINITY), false, "UTC").unwrap(), Ordering::Greater);
    assert_eq!(ordered_compare_in_timezone(&V::Null, &V::F64(f64::NAN), true, "UTC").unwrap(), Ordering::Less);
    assert_eq!(ordered_compare_in_timezone(&V::Null, &V::F64(f64::NAN), false, "UTC").unwrap(), Ordering::Greater);
}

#[test]
fn unsigned_physical_vectors_compare_their_signed_integer_reads() {
    for value in [V::U8(u8::MAX), V::U16(u16::MAX), V::U32(u32::MAX), V::U64(u64::MAX)] {
        assert_eq!(ordered_compare_in_timezone(&value, &V::I64(0), false, "UTC").unwrap(), Ordering::Less);
        assert_eq!(ordered_compare_in_timezone(&value, &V::F64(-1.0), false, "UTC").unwrap(), Ordering::Equal);
        assert_eq!(ordered_compare_in_timezone(&value, &decimal(-1), false, "UTC").unwrap(), Ordering::Equal);
    }
    assert_eq!(ordered_compare_in_timezone(&V::U64(9_007_199_254_740_993), &V::U64(9_007_199_254_740_992), false, "UTC").unwrap(), Ordering::Greater);
}

#[test]
fn comparator_type_factory_checks_every_union_pair_and_rejects_nested_values() {
    for ty in [T::list(T::Null), T::Struct(BTreeMap::new()), V::TimestampTzRange { lower: 0, upper: 1 }.logical_type(), T::Scalar(S::Transit), T::Scalar(S::Oid), T::Scalar(S::F16), T::Scalar(S::I64).join(&T::Scalar(S::Utf8)), T::Scalar(S::Utf8).join(&T::Scalar(S::Uri))] {
        assert_eq!(validate_order_type(&ty).unwrap_err().code, "xtdb.expression/function-type-mismatch");
    }
    for ty in [T::Null, T::Scalar(S::Bool).maybe(), T::Scalar(S::I64).join(&T::Scalar(S::Decimal { precision: 20, scale: 0, bit_width: 128 })).maybe(), T::Scalar(S::Binary).join(&T::Scalar(S::FixedSizeBinary(8)))] {
        validate_order_type(&ty).unwrap();
    }
    assert_eq!(ordered_compare_in_timezone(&V::Binary(vec![0, 255]), &V::FixedSizeBinary(vec![1, 0]), false, "UTC").unwrap(), Ordering::Less);
}

#[test]
fn interval_comparator_factory_rejects_mixed_units_even_when_values_are_absent() {
    let ty = T::Scalar(S::Interval(IntervalUnit::YearMonth)).join(&T::Scalar(S::Interval(IntervalUnit::MonthDayNano)));
    assert_eq!(validate_order_type(&ty).unwrap_err().code, "xtdb.expression/cannot-compare-different-unit-intervals");
    assert!(validate_order_type(&T::Scalar(S::Interval(IntervalUnit::DayTime))).is_err());
    validate_order_type(&T::Scalar(S::IntervalMonthDayMicro)).unwrap();
}
