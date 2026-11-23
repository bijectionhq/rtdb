use rtdb_types::StructFields;
use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{Array, FixedSizeBinaryArray, Int64Array, RecordBatch, RunArray, types::Int32Type};
use arrow_schema::{DataType, Field, Schema, UnionFields, UnionMode};
use rtdb_columnar::{EXTENSION_NAME_KEY, IidView, MemoryBudget, OwnedBatch, Selection, build_array, build_iids, from_field, leg_name, read_value, to_field};
use rtdb_types::{DecimalValue, Iid, IntervalUnit, Keyword, ListKind, LogicalType, RunEndWidth, ScalarType, TimeUnit, Value};

fn scalar(ty: ScalarType) -> LogicalType { LogicalType::Scalar(ty) }

fn round_trip(ty: LogicalType, values: Vec<Value>) {
    let field = to_field("value", &ty).unwrap();
    assert_eq!(from_field(&field).unwrap(), ty);
    let array = build_array(&ty, &values).unwrap();
    array.to_data().validate_full().unwrap();
    assert_eq!(array.data_type(), field.data_type());
    assert_eq!(array.len(), values.len());
    for (row, value) in values.iter().enumerate() { assert_eq!(&read_value(&field, array.as_ref(), row).unwrap(), value); }
}

#[test]
fn bottom_is_not_nullable_null() {
    let bottom = to_field("declared", &LogicalType::Nothing).unwrap();
    let null = to_field("null", &LogicalType::Null).unwrap();
    assert_eq!(bottom.data_type(), &DataType::Null);
    assert!(!bottom.is_nullable());
    assert!(null.is_nullable());
    assert_eq!(from_field(&bottom).unwrap(), LogicalType::Nothing);
    assert_eq!(from_field(&null).unwrap(), LogicalType::Null);
    let array = build_array(&LogicalType::Nothing, &[Value::Undefined]).unwrap();
    assert!(read_value(&bottom, array.as_ref(), 0).is_err());
    assert!(build_array(&LogicalType::Nothing, &[Value::Null]).is_err());
}

#[test]
fn union_nullability_is_a_null_leg() {
    let ty = LogicalType::from_legs([scalar(ScalarType::I64), scalar(ScalarType::Utf8), LogicalType::Null]);
    let field = to_field("x", &ty).unwrap();
    assert!(!field.is_nullable());
    let DataType::Union(children, UnionMode::Dense) = field.data_type() else { panic!("expected dense union") };
    let names: BTreeSet<_> = children.iter().map(|(_, child)| child.name().as_str()).collect();
    assert_eq!(names, BTreeSet::from(["i64", "null", "utf8"]));
    round_trip(ty, vec![Value::I64(5), Value::Null, Value::Utf8("five".into()), Value::I64(-5)]);
}

#[test]
fn absent_struct_children_read_as_null_without_losing_bottom_fields() {
    let ty = LogicalType::Struct(StructFields::from([
        ("declared".into(), LogicalType::Nothing),
        ("maybe".into(), scalar(ScalarType::I64).maybe()),
    ]));
    let values = vec![Value::Struct(StructFields::from([("declared".into(), Value::Undefined), ("maybe".into(), Value::I64(1))])), Value::Struct(StructFields::from([("declared".into(), Value::Undefined)]))];
    let array = build_array(&ty, &values).unwrap();
    let field = to_field("x", &ty).unwrap();
    array.to_data().validate_full().unwrap();
    assert_eq!(read_value(&field, array.as_ref(), 0).unwrap(), Value::Struct(StructFields::from([
        ("declared".into(), Value::Undefined), ("maybe".into(), Value::I64(1)),
    ])));
    assert_eq!(read_value(&field, array.as_ref(), 1).unwrap(), Value::Struct(StructFields::from([
        ("declared".into(), Value::Undefined), ("maybe".into(), Value::Null),
    ])));
}

#[test]
fn scalar_widths_and_extension_names_are_preserved() {
    let cases = vec![
        (ScalarType::Bool, Value::Bool(true)),
        (ScalarType::I8, Value::I8(-128)), (ScalarType::I16, Value::I16(i16::MIN)),
        (ScalarType::I32, Value::I32(i32::MIN)), (ScalarType::I64, Value::I64(i64::MIN)),
        (ScalarType::U8, Value::U8(u8::MAX)), (ScalarType::U16, Value::U16(u16::MAX)),
        (ScalarType::U32, Value::U32(u32::MAX)), (ScalarType::U64, Value::U64(u64::MAX)),
        (ScalarType::F16, Value::F16(0x3c00)), (ScalarType::F32, Value::F32(1.25)), (ScalarType::F64, Value::F64(-1.25)),
        (ScalarType::Utf8, Value::Utf8("é\0花".into())), (ScalarType::Binary, Value::Binary(vec![0, 255, 0])),
        (ScalarType::FixedSizeBinary(2), Value::FixedSizeBinary(vec![0, 255])),
        (ScalarType::DateDay, Value::DateDay(-1)), (ScalarType::DateMilli, Value::DateMilli(-86_400_000)),
        (ScalarType::Keyword, Value::Keyword(Keyword::parse(":app/user").unwrap())),
        (ScalarType::Uuid, Value::Uuid([0xff; 16])), (ScalarType::Uri, Value::Uri("https://example.test/a".into())),
        (ScalarType::Transit, Value::Transit(vec![0x92, 0xa1, b'x', 1])),
        (ScalarType::Oid, Value::Oid(-1)), (ScalarType::RegClass, Value::RegClass(42)), (ScalarType::RegProc, Value::RegProc(43)),
        (ScalarType::Interval(IntervalUnit::YearMonth), Value::IntervalYearMonth(-13)),
        (ScalarType::Interval(IntervalUnit::DayTime), Value::IntervalDayTime { days: -2, milliseconds: 100 }),
        (ScalarType::Interval(IntervalUnit::MonthDayNano), Value::IntervalMonthDayNano { months: 1, days: 2, nanoseconds: 3 }),
        (ScalarType::IntervalMonthDayMicro, Value::IntervalMonthDayMicro { months: 1, days: 2, nanoseconds: 3 }),
    ];
    for (ty, value) in cases { round_trip(scalar(ty).maybe(), vec![Value::Null, value]); }
    for (ty, name) in [
        (ScalarType::Keyword, "xt/clj-keyword"), (ScalarType::Uuid, "uuid"), (ScalarType::Uri, "uri"),
        (ScalarType::Transit, "xt/transit+msgpack"), (ScalarType::Oid, "xt/oid"),
        (ScalarType::RegClass, "xt/regclass"), (ScalarType::RegProc, "xt/regproc"),
        (ScalarType::IntervalMonthDayMicro, "xt/intervalDMMType"),
    ] {
        assert_eq!(to_field("x", &scalar(ty)).unwrap().metadata().get(EXTENSION_NAME_KEY).unwrap(), name);
    }
}

#[test]
fn decimal_coefficients_preserve_signed_big_endian_width_and_scale() {
    for (width, precision, bytes) in [(32, 9, (-1234i32).to_be_bytes().to_vec()), (64, 18, (-1234i64).to_be_bytes().to_vec()),
        (128, 38, (-1234i128).to_be_bytes().to_vec()), (256, 76, arrow_buffer::i256::from_i128(-1234).to_be_bytes().to_vec())] {
        let ty = scalar(ScalarType::Decimal { precision, scale: 2, bit_width: width }).maybe();
        round_trip(ty, vec![Value::Decimal(DecimalValue::new(bytes, precision, 2, width).unwrap()), Value::Null]);
    }
}

#[test]
fn decimal_vector_scale_can_exceed_precision_without_relaxing_coefficient_checks() {
    for width in [32, 64, 128, 256] {
        for scale in [i32::MIN, -129, -128, -3, 3, 127, 128, i32::MAX] {
            let ty = scalar(ScalarType::Decimal { precision: 1, scale, bit_width: width }).maybe();
            for coefficient in [-9i128, 0, 1, 9] {
                let bytes = arrow_buffer::i256::from_i128(coefficient).to_be_bytes();
                let value = Value::Decimal(DecimalValue::new(bytes[32 - usize::from(width / 8)..].to_vec(), 1, scale, width).unwrap());
                round_trip(ty.clone(), vec![Value::Null, value.clone()]);
                let array = build_array(&ty, &[Value::Null, value.clone()]).unwrap().slice(1, 1);
                array.to_data().validate_full().unwrap();
                assert_eq!(read_value(&to_field("x", &ty).unwrap(), array.as_ref(), 0).unwrap(), value);
            }
            for coefficient in [-10i128, 10] {
                let bytes = arrow_buffer::i256::from_i128(coefficient).to_be_bytes();
                let value = Value::Decimal(DecimalValue::new(bytes[32 - usize::from(width / 8)..].to_vec(), 1, scale, width).unwrap());
                assert!(build_array(&ty, &[value]).is_err());
            }
        }
    }
}

#[test]
fn decimal_vector_zero_precision_accepts_only_empty_and_null_arrays() {
    for width in [32, 64, 128, 256] {
        for scale in [-3, 0, 3] {
            let ty = scalar(ScalarType::Decimal { precision: 0, scale, bit_width: width }).maybe();
            round_trip(ty.clone(), vec![]);
            round_trip(ty.clone(), vec![Value::Null]);
            let zero = Value::Decimal(DecimalValue::new(vec![0; usize::from(width / 8)], 0, scale, width).unwrap());
            assert!(build_array(&ty, &[zero]).is_err());
        }
    }
}

#[test]
fn signed_decimal_precision_preserves_metadata_and_accepts_full_signed_coefficients() {
    for (width, bytes, digits) in [
        (32, i32::MIN.to_be_bytes().to_vec(), 10), (64, i64::MIN.to_be_bytes().to_vec(), 19),
        (128, i128::MIN.to_be_bytes().to_vec(), 39), (256, arrow_buffer::i256::MIN.to_be_bytes().to_vec(), 77),
    ] {
        for precision in [digits, 255, 256, i32::MAX] {
            for scale in [0, i32::MAX] {
                let ty = scalar(ScalarType::Decimal { precision, scale, bit_width: width });
                let value = Value::Decimal(DecimalValue::new(bytes.clone(), precision, scale, width).unwrap());
                round_trip(ty, vec![value]);
            }
        }
        let value = Value::Decimal(DecimalValue::new(bytes, digits - 1, 0, width).unwrap());
        assert!(build_array(&value.logical_type(), &[value.clone()]).is_err());
        let copied = rtdb_columnar::copy_array(&value.logical_type(), std::slice::from_ref(&value)).unwrap();
        assert_eq!(read_value(&to_field("value", &value.logical_type()).unwrap(), copied.as_ref(), 0).unwrap(), value);
        for precision in [i32::MIN, -1, 0] {
            let ty = scalar(ScalarType::Decimal { precision, scale: 0, bit_width: width }).maybe();
            round_trip(ty.clone(), vec![Value::Null]);
            let value = Value::Decimal(DecimalValue::new(vec![0; usize::from(width / 8)], precision, 0, width).unwrap());
            assert!(build_array(&ty, std::slice::from_ref(&value)).is_err());
            assert!(rtdb_columnar::copy_array(&ty, &[value]).is_ok());
        }
    }
}

#[test]
fn typed_decimal_copy_rejects_metadata_relabeling_and_invalid_width_at_every_depth() {
    let value = Value::Decimal(DecimalValue::new(123i128.to_be_bytes().to_vec(), 256, 129, 128).unwrap());
    for target in [
        scalar(ScalarType::Decimal { precision: 257, scale: 129, bit_width: 128 }),
        scalar(ScalarType::Decimal { precision: 256, scale: 130, bit_width: 128 }),
        scalar(ScalarType::Decimal { precision: 256, scale: 129, bit_width: 256 }),
    ] {
        assert!(rtdb_columnar::copy_array(&target, std::slice::from_ref(&value)).is_err());
        assert!(rtdb_columnar::copy_array(&LogicalType::list(target), &[Value::List(vec![value.clone()])]).is_err());
    }
    let mut malformed = if let Value::Decimal(value) = value { value } else { unreachable!() };
    malformed.unscaled.pop();
    let malformed = Value::Decimal(malformed);
    assert!(rtdb_columnar::copy_array(&malformed.logical_type(), &[malformed.clone()]).is_err());
    for (key, text) in [(rtdb_columnar::INTERNAL_DECIMAL_PRECISION_KEY, "256"), (rtdb_columnar::INTERNAL_DECIMAL_SCALE_KEY, "129")] {
        let field = arrow_schema::Field::new("value", arrow_schema::DataType::Decimal128(2, 2), false).with_metadata(std::collections::HashMap::from([(key.to_owned(), text.to_owned())]));
        assert!(from_field(&field).is_err());
    }
}

#[test]
fn decimal_hash_uses_java_double_value_at_full_scale_limits() {
    for (scale, expected) in [(i32::MIN, f64::NEG_INFINITY), (i32::MAX, -0.0)] {
        let value = Value::Decimal(DecimalValue::new((-123i128).to_be_bytes().to_vec(), 3, scale, 128).unwrap());
        assert_eq!(rtdb_columnar::hash_value(&value, 0).unwrap(), rtdb_columnar::hash_value(&Value::F64(expected), 0).unwrap());
    }
}

#[test]
fn date_millisecond_hash_uses_the_containing_day() {
    for (millis, day) in [(-86_400_001, -2), (-86_400_000, -1), (-1, -1), (0, 0), (86_400_001, 1)] {
        assert_eq!(rtdb_columnar::hash_value(&Value::DateMilli(millis), 0).unwrap(), rtdb_columnar::hash_value(&Value::DateDay(day), 0).unwrap());
    }
}

#[test]
fn timestamps_and_durations_preserve_unit_and_zone() {
    for unit in [TimeUnit::Second, TimeUnit::Millisecond, TimeUnit::Microsecond, TimeUnit::Nanosecond] {
        for timezone in [None, Some("UTC".to_owned()), Some("Europe/London".to_owned())] {
            let ty = scalar(ScalarType::Timestamp { unit, timezone: timezone.clone() });
            round_trip(ty, vec![Value::Timestamp { value: -123, unit, timezone }]);
        }
        round_trip(scalar(ScalarType::Duration(unit)), vec![Value::Duration { value: -123, unit }]);
        let bit_width = if matches!(unit, TimeUnit::Second | TimeUnit::Millisecond) { 32 } else { 64 };
        round_trip(scalar(ScalarType::Time { unit, bit_width }), vec![Value::Time { value: 123, unit, bit_width }]);
    }
    assert_eq!(leg_name(&scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("Europe/London".into()) })).unwrap(), "timestamp-tz-micro-europe_london");
}

#[test]
fn nested_lists_sets_maps_and_ranges_round_trip() {
    let union = LogicalType::from_legs([scalar(ScalarType::Utf8), scalar(ScalarType::I64), LogicalType::Null]);
    round_trip(LogicalType::List { kind: ListKind::List, element: Box::new(union) }, vec![Value::List(vec![Value::I64(1), Value::Null, Value::Utf8("x".into())]), Value::List(vec![])]);
    round_trip(LogicalType::List { kind: ListKind::Set, element: Box::new(scalar(ScalarType::I64)) }, vec![Value::Set(vec![Value::I64(1), Value::I64(2)])]);
    let entries = LogicalType::Struct(StructFields::from([("key".into(), scalar(ScalarType::Utf8)), ("value".into(), scalar(ScalarType::I64).maybe())]));
    round_trip(LogicalType::List { kind: ListKind::Map { sorted: false }, element: Box::new(entries) }, vec![Value::Map { entries: vec![(Value::Utf8("one".into()), Value::I64(1)), (Value::Utf8("null".into()), Value::Null)], sorted: false }]);
    round_trip(LogicalType::List { kind: ListKind::FixedSizeList(2), element: Box::new(scalar(ScalarType::I64)) }.maybe(), vec![Value::Null, Value::FixedSizeList(vec![Value::I64(1), Value::I64(2)])]);
    round_trip(LogicalType::List { kind: ListKind::TimestampTzRange, element: Box::new(scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) })) }, vec![Value::TimestampTzRange { lower: i64::MIN, upper: i64::MAX }]);
}

#[test]
fn null_struct_masks_non_nullable_children() {
    let ty = LogicalType::Struct(StructFields::from([("x".into(), scalar(ScalarType::I64))])).maybe();
    round_trip(ty, vec![Value::Null, Value::Struct(StructFields::from([("x".into(), Value::I64(7))]))]);
}

#[test]
fn run_end_nulls_live_in_the_values_child() {
    let ty = LogicalType::RunEndEncoded { width: RunEndWidth::I32, value: Box::new(scalar(ScalarType::I64).maybe()) };
    let values = vec![Value::I64(7), Value::I64(7), Value::Null, Value::Null, Value::I64(9)];
    let array = build_array(&ty, &values).unwrap();
    let runs = array.as_any().downcast_ref::<RunArray<Int32Type>>().unwrap();
    assert_eq!(runs.run_ends().values(), &[2, 4, 5]);
    assert_eq!(runs.values().null_count(), 1);
    assert!(!to_field("x", &ty).unwrap().is_nullable());
    round_trip(ty, values);
}

#[test]
fn run_end_slices_and_every_width_read_logical_rows() {
    let iids = vec![Iid([1; 16]), Iid([1; 16]), Iid([1; 16]), Iid([2; 16]), Iid([2; 16]), Iid([3; 16])];
    for width in [None, Some(RunEndWidth::I16), Some(RunEndWidth::I32), Some(RunEndWidth::I64)] {
        let array = build_iids(&iids, width).unwrap();
        let slice = array.slice(1, 4);
        let view = IidView::new(slice.as_ref()).unwrap();
        assert_eq!(view.len(), 4);
        assert_eq!((0..view.len()).map(|row| view.get(row).unwrap()).collect::<Vec<_>>(), iids[1..5]);
        assert!(view.get(4).is_err());
    }
}

#[test]
fn iid_field_keeps_values_nonnullable_in_every_encoding() {
    for width in [RunEndWidth::I16, RunEndWidth::I32, RunEndWidth::I64] {
        let ty = LogicalType::RunEndEncoded { width, value: Box::new(scalar(ScalarType::FixedSizeBinary(16))) };
        let array = build_iids(&[Iid([4; 16])], Some(width)).unwrap();
        assert_eq!(array.data_type(), to_field("_iid", &ty).unwrap().data_type());
    }
    let plain = build_iids(&[], None).unwrap();
    assert_eq!(plain.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap().value_length(), 16);
}

#[test]
fn run_encoding_preserves_signed_zero_and_nan_payloads() {
    let ty = LogicalType::RunEndEncoded { width: RunEndWidth::I32, value: Box::new(scalar(ScalarType::F64)) };
    let values = vec![Value::F64(0.0), Value::F64(-0.0), Value::F64(f64::from_bits(0x7ff8_0000_0000_0012))];
    let array = build_array(&ty, &values).unwrap();
    let field = to_field("x", &ty).unwrap();
    for (row, expected) in [0u64, 1u64 << 63, 0x7ff8_0000_0000_0012].into_iter().enumerate() {
        let Value::F64(actual) = read_value(&field, array.as_ref(), row).unwrap() else { panic!("expected f64") };
        assert_eq!(actual.to_bits(), expected);
    }
}

#[test]
fn encoded_union_legs_are_rejected() {
    let encoded = to_field("encoded", &LogicalType::RunEndEncoded { width: RunEndWidth::I32, value: Box::new(scalar(ScalarType::I64)) }).unwrap();
    let fields = UnionFields::try_new([0, 1], [encoded, Field::new("utf8", DataType::Utf8, false)]).unwrap();
    let field = Field::new("x", DataType::Union(fields, UnionMode::Dense), false);
    assert!(from_field(&field).is_err());
}

fn owned_nullable_batch(budget: &Arc<MemoryBudget>) -> OwnedBatch {
    let array = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
    OwnedBatch::new(RecordBatch::try_new(schema, vec![array]).unwrap(), budget).unwrap()
}

#[test]
fn raw_arrow_clone_keeps_reservation_after_owner_is_dropped() {
    let budget = MemoryBudget::new(1 << 20);
    let owned = owned_nullable_batch(&budget);
    let retained = owned.batch().column(0).clone();
    let used = budget.used_bytes();
    assert!(used > 0);
    drop(owned);
    assert_eq!(budget.used_bytes(), used);
    let slice = retained.slice(1, 1);
    drop(retained);
    assert_eq!(budget.used_bytes(), used);
    assert!(slice.is_null(0));
    drop(slice);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn raw_validity_buffer_keeps_reservation_after_array_is_dropped() {
    let budget = MemoryBudget::new(1 << 20);
    let owned = owned_nullable_batch(&budget);
    let bitmap = owned.batch().column(0).nulls().unwrap().buffer().clone();
    let used = budget.used_bytes();
    drop(owned);
    assert_eq!(budget.used_bytes(), used);
    drop(bitmap);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn selection_preserves_duplicates_nulls_and_budget_lifetime() {
    let budget = MemoryBudget::new(1 << 20);
    let owned = owned_nullable_batch(&budget);
    let selected = owned.select(&Selection::Indices(vec![2, 1, 2, 0])).unwrap();
    let column = selected.batch().column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(column, &Int64Array::from(vec![Some(3), None, Some(3), Some(1)]));
    drop(owned);
    assert!(budget.used_bytes() > 0);
    drop(selected);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn rejected_reservation_does_not_change_budget() {
    let budget = MemoryBudget::new(100);
    let reservation = budget.reserve(80).unwrap();
    assert!(budget.reserve(21).is_err());
    assert_eq!(budget.used_bytes(), 80);
    assert_eq!(budget.peak_bytes(), 80);
    drop(reservation);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn deeply_nested_union_cells_round_trip_promptly() {
    let mut ty = scalar(ScalarType::I64);
    let mut value = Value::I64(7);
    for depth in 0..40 {
        let other = if depth % 2 == 0 { scalar(ScalarType::Utf8) } else { LogicalType::Struct(StructFields::from([("v".into(), scalar(ScalarType::Bool))])) };
        ty = LogicalType::list(LogicalType::from_legs([ty, other, LogicalType::Null]));
        value = Value::List(vec![value, Value::Null]);
    }
    let started = std::time::Instant::now();
    round_trip(ty, vec![value]);
    assert!(started.elapsed() < std::time::Duration::from_secs(60), "took {:?}", started.elapsed());
}

#[test]
fn a_column_reader_reads_every_cell_as_read_value_does() {
    use rtdb_columnar::{ColumnReader, build_array, read_value, read_value_as, to_field};
    use rtdb_types::{LogicalType, Value};
    let values = vec![
        Value::List(vec![Value::I64(1), Value::Utf8("a".into()), Value::Null]),
        Value::Utf8("b".into()),
        Value::List(vec![Value::Bool(true), Value::List(vec![Value::F64(1.5)])]),
        Value::Null,
    ];
    let logical = LogicalType::join_all(values.iter().map(Value::logical_type).collect::<Vec<_>>().iter());
    let field = to_field("v", &logical).unwrap();
    let array = build_array(&logical, &values).unwrap();
    let reader = ColumnReader::new(&field).unwrap();
    for row in 0..values.len() {
        let expected = read_value(&field, array.as_ref(), row).unwrap();
        assert_eq!(reader.read(array.as_ref(), row).unwrap(), expected);
        assert_eq!(read_value_as(reader.logical(), &field, array.as_ref(), row).unwrap(), expected);
    }
    assert!(matches!(reader.logical(), LogicalType::Union(_)));
}
