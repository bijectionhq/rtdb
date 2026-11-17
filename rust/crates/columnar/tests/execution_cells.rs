use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{*, types::{Int16Type, Int32Type, Int64Type}};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit as ArrowTimeUnit, UnionFields};
use rtdb_columnar::{ExecutionVector, MemoryBudget, NullChecks, OwnedBatch, Selection, VectorReadError, to_field};
use rtdb_types::{IntervalUnit, LogicalType, ScalarType, TimeUnit, Value};

fn anomaly(error: VectorReadError) -> rtdb_types::Anomaly {
    match error { VectorReadError::Anomaly(error) => error, other => panic!("expected an existing anomaly, got {other:?}") }
}

fn batch(field: &Field, array: ArrayRef, budget: &Arc<MemoryBudget>) -> OwnedBatch {
    let storage = field.clone().with_nullable(true);
    OwnedBatch::new(RecordBatch::try_new(Arc::new(Schema::new(vec![storage])), vec![array]).unwrap(), budget).unwrap()
}

fn open(field: Field, array: ArrayRef, budget: &Arc<MemoryBudget>) -> ExecutionVector {
    let batch = batch(&field, array, budget);
    ExecutionVector::open(Arc::new(field), batch.column(0).unwrap()).unwrap()
}

fn invalid() -> Option<NullBuffer> { Some(NullBuffer::from(vec![true, false])) }

#[test]
fn nullable_primitives_preserve_nonzero_payload_and_signed_execution_width() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    macro_rules! check {
        ($array:expr, $get:ident, $expected:expr) => {{
            let array: ArrayRef = Arc::new($array);
            let field = Field::new("v", array.data_type().clone(), true);
            let vector = open(field, array, &budget);
            let cell = vector.cell(1).unwrap();
            assert!(cell.is_null().unwrap());
            assert_eq!(cell.$get(NullChecks::Disabled).unwrap(), $expected);
            let error = anomaly(cell.$get(NullChecks::Enabled).unwrap_err());
            assert_eq!(error.code, "xtdb.error/unknown");
            assert_eq!(error.message.as_deref(), Some("null at index 1"));
            assert_eq!(cell.object().unwrap().value(), &Value::Null);
        }};
    }
    check!(BooleanArray::new(BooleanBuffer::from(vec![false, true]), invalid()), get_boolean, true);
    check!(Int8Array::new(vec![0, -7].into(), invalid()), get_byte, -7);
    check!(UInt8Array::new(vec![0, 255].into(), invalid()), get_byte, -1);
    check!(Int16Array::new(vec![0, -707].into(), invalid()), get_short, -707);
    check!(UInt16Array::new(vec![0, 65535].into(), invalid()), get_short, -1);
    check!(Int32Array::new(vec![0, -70007].into(), invalid()), get_int, -70007);
    check!(UInt32Array::new(vec![0, u32::MAX].into(), invalid()), get_int, -1);
    check!(Int64Array::new(vec![0, -70000007].into(), invalid()), get_long, -70000007);
    check!(UInt64Array::new(vec![0, u64::MAX].into(), invalid()), get_long, -1);
    check!(Float32Array::new(vec![0.0, -1.25].into(), invalid()), get_float, -1.25);
    check!(Float64Array::new(vec![0.0, -17.125].into(), invalid()), get_double, -17.125);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn getter_admission_precedes_validity_and_matches_concrete_vector_methods() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    let byte = open(Field::new("v", DataType::Int8, true), Arc::new(Int8Array::new(vec![0, 9].into(), invalid())), &budget);
    assert_eq!(anomaly(byte.cell(1).unwrap().get_long(NullChecks::Enabled).unwrap_err()).code, "rtdb/unsupported-vector-reader");
    let short = open(Field::new("v", DataType::Int16, true), Arc::new(Int16Array::from(vec![17])), &budget);
    assert_eq!(short.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 17);
    assert_eq!(short.cell(0).unwrap().get_long(NullChecks::Enabled).unwrap(), 17);
    let float = open(Field::new("v", DataType::Float32, true), Arc::new(Float32Array::from(vec![1.25])), &budget);
    assert_eq!(float.cell(0).unwrap().get_double(NullChecks::Enabled).unwrap(), 1.25);
    for scalar in [ScalarType::Oid, ScalarType::RegClass, ScalarType::RegProc, ScalarType::DateDay, ScalarType::Time { unit: TimeUnit::Second, bit_width: 32 }, ScalarType::Interval(IntervalUnit::YearMonth)] {
        let field = to_field("v", &LogicalType::Scalar(scalar).maybe()).unwrap();
        let data = arrow_data::ArrayData::builder(field.data_type().clone()).len(2).add_buffer(arrow_buffer::Buffer::from_slice_ref([3i32, 19])).nulls(invalid()).build().unwrap();
        let vector = open(field, make_array(data), &budget);
        assert_eq!(vector.cell(1).unwrap().get_int(NullChecks::Disabled).unwrap(), 19);
        assert_eq!(anomaly(vector.cell(1).unwrap().get_long(NullChecks::Enabled).unwrap_err()).code, "rtdb/unsupported-vector-reader");
    }
    let null = open(Field::new("null", DataType::Null, true), Arc::new(NullArray::new(1)), &budget);
    assert!(null.cell(0).unwrap().is_null().unwrap());
    assert_eq!(anomaly(null.cell(0).unwrap().get_long(NullChecks::Enabled).unwrap_err()).code, "rtdb/unsupported-vector-reader");
    assert_eq!(null.cell(0).unwrap().object().unwrap().value(), &Value::Null);
    let bottom = open(Field::new("bottom", DataType::Null, false), Arc::new(NullArray::new(1)), &budget);
    assert!(bottom.cell(0).unwrap().is_null().unwrap());
    assert_eq!(bottom.cell(0).unwrap().object().unwrap_err().code, "xtdb/zero-leg-read");
}

#[test]
fn ignored_nonnullable_validity_preserves_payload_and_original_array() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    let array: ArrayRef = Arc::new(Int64Array::new(vec![0, 99].into(), invalid()));
    let vector = open(Field::new("v", DataType::Int64, false), Arc::clone(&array), &budget);
    assert!(array.is_null(1));
    assert!(!vector.cell(1).unwrap().is_null().unwrap());
    assert_eq!(vector.cell(1).unwrap().get_long(NullChecks::Enabled).unwrap(), 99);
    assert_eq!(vector.cell(1).unwrap().object().unwrap().value(), &Value::I64(99));
}

#[test]
fn struct_child_nullability_and_composed_selection_remain_independent_of_parent() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    let fields = Fields::from(vec![Field::new("required", DataType::Int64, false), Field::new("optional", DataType::Int64, true)]);
    let children: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::new(vec![11, 22, 33].into(), Some(NullBuffer::from(vec![true, false, false])))),
        Arc::new(Int64Array::new(vec![41, 42, 43].into(), Some(NullBuffer::from(vec![true, false, false])))),
    ];
    let original: ArrayRef = Arc::new(StructArray::new(fields.clone(), children, Some(NullBuffer::from(vec![true, false, false]))));
    for nullable in [true, false] {
        let vector = open(Field::new("s", DataType::Struct(fields.clone()), nullable), Arc::clone(&original), &budget);
        let selected = vector.select(&Selection::Indices(vec![2, 0, 1])).unwrap().select(&Selection::Range { offset: 1, length: 2 }).unwrap().select(&Selection::Indices(vec![1, 0])).unwrap();
        assert_eq!(selected.cell(0).unwrap().getter_index().unwrap(), 1);
        assert_eq!(selected.cell(0).unwrap().is_null().unwrap(), nullable);
        let required = selected.struct_child("required").unwrap().unwrap();
        assert_eq!(required.cell(0).unwrap().get_long(NullChecks::Enabled).unwrap(), 22);
        let optional = selected.struct_child("optional").unwrap().unwrap();
        assert_eq!(optional.cell(0).unwrap().get_long(NullChecks::Disabled).unwrap(), 42);
        assert_eq!(anomaly(optional.cell(0).unwrap().get_long(NullChecks::Enabled).unwrap_err()).message.as_deref(), Some("null at index 1"));
        let raw = selected.cell(0).unwrap().raw_struct_field("required").unwrap().unwrap();
        assert_eq!(raw.as_ref().getter_index().unwrap(), 1);
        assert_eq!(raw.as_ref().get_long(NullChecks::Enabled).unwrap(), 22);
        assert!(selected.struct_child("absent").unwrap().is_none());
    }
    assert!(original.as_any().downcast_ref::<StructArray>().unwrap().column(0).is_null(1));
}

#[test]
fn dense_union_retains_nullable_duration_leaf_and_explicit_null_distinction() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    let fields = UnionFields::try_new([7, 2], vec![
        Arc::new(Field::new("duration-micro", DataType::Duration(ArrowTimeUnit::Microsecond), true)),
        Arc::new(Field::new("null", DataType::Null, true)),
    ]).unwrap();
    let duration: ArrayRef = Arc::new(DurationMicrosecondArray::new(vec![10, 71].into(), invalid()));
    let union = UnionArray::try_new(fields, ScalarBuffer::from(vec![7, 2, 7]), Some(ScalarBuffer::from(vec![1, 0, 0])), vec![duration, Arc::new(NullArray::new(1))]).unwrap();
    let field = Field::new("u", union.data_type().clone(), false);
    let vector = open(field, Arc::new(union), &budget).select(&Selection::Indices(vec![1, 0, 2])).unwrap();
    assert_eq!(vector.logical_type().unwrap(), &LogicalType::Scalar(ScalarType::Duration(TimeUnit::Microsecond)).maybe());
    let duration = vector.cell(1).unwrap();
    assert_eq!(duration.leaf_descriptor().unwrap().name(), "duration-micro");
    assert_eq!(duration.getter_index().unwrap(), 1);
    assert_eq!(duration.get_long(NullChecks::Disabled).unwrap(), 71);
    assert_eq!(anomaly(duration.get_long(NullChecks::Enabled).unwrap_err()).message.as_deref(), Some("null at index 1"));
    assert_eq!(duration.object().unwrap().value(), &Value::Null);
    let null = vector.cell(0).unwrap();
    assert_eq!(null.leaf_descriptor().unwrap().name(), "null");
    assert!(null.is_null().unwrap());
    assert_eq!(anomaly(null.get_long(NullChecks::Disabled).unwrap_err()).code, "rtdb/unsupported-vector-reader");
}

#[test]
fn run_end_slices_and_selections_preserve_physical_getter_index() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    macro_rules! check {
        ($type:ty, $ends:expr) => {{
            let values = Int64Array::new(vec![101, 202].into(), Some(NullBuffer::from(vec![false, true])));
            let runs = RunArray::<$type>::try_new(&$ends, &values).unwrap().slice(1, 3);
            let vector = open(Field::new("runs", runs.data_type().clone(), false), Arc::new(runs), &budget);
            let selected = vector.select(&Selection::Indices(vec![2, 0, 1])).unwrap();
            assert_eq!(selected.cell(1).unwrap().getter_index().unwrap(), 0);
            assert_eq!(selected.cell(1).unwrap().get_long(NullChecks::Disabled).unwrap(), 101);
            assert_eq!(anomaly(selected.cell(1).unwrap().get_long(NullChecks::Enabled).unwrap_err()).message.as_deref(), Some("null at index 0"));
            assert_eq!(selected.cell(0).unwrap().get_long(NullChecks::Enabled).unwrap(), 202);
            assert_eq!(selected.cell(0).unwrap().getter_index().unwrap(), 1);
        }};
    }
    check!(Int16Type, Int16Array::from(vec![2, 5]));
    check!(Int32Type, Int32Array::from(vec![2, 5]));
    check!(Int64Type, Int64Array::from(vec![2, 5]));
}

#[test]
fn sdk_run_end_domain_checks_unused_suffixes_inside_empty_and_nested_arrays() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    for (offset, length) in [(0, 1), (0, 0), (1, 0)] {
        for nesting in 0..3 {
            let runs = RunArray::<Int64Type>::try_new(&Int64Array::from(vec![1, i64::from(i32::MAX) + 1]), &Int64Array::from(vec![42, 73])).unwrap().slice(offset, length);
            let runs: ArrayRef = Arc::new(runs);
            let child = Arc::new(Field::new("runs", runs.data_type().clone(), false));
            let array: ArrayRef = match nesting {
                0 => runs,
                1 => Arc::new(StructArray::new(Fields::from(vec![child]), vec![runs], None)),
                _ => Arc::new(ListArray::new(child, OffsetBuffer::new(vec![0].into()), runs, None)),
            };
            let field = Arc::new(Field::new("root", array.data_type().clone(), false));
            let reservation = budget.reserve(array.get_array_memory_size()).unwrap();
            assert_eq!(ExecutionVector::with_reservation(field, array, reservation).unwrap_err().code, "rtdb/vector-length");
            assert_eq!(budget.used_bytes(), 0);
        }
    }
}

#[test]
fn sdk_run_end_rejection_releases_metadata_without_releasing_caller_column() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    let runs = RunArray::<Int64Type>::try_new(&Int64Array::from(vec![i64::from(i32::MAX) + 1]), &Int64Array::from(vec![42])).unwrap().slice(0, 1);
    let field = Field::new("runs", runs.data_type().clone(), false);
    let source = batch(&field, Arc::new(runs), &budget);
    let before = budget.used_bytes();
    assert!(before > 0);
    assert_eq!(ExecutionVector::open(Arc::new(field), source.column(0).unwrap()).unwrap_err().code, "rtdb/vector-length");
    assert_eq!(budget.used_bytes(), before);
    assert_eq!(source.column(0).unwrap().array().len(), 1);
    drop(source);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn sdk_i32_max_run_coverage_survives_slicing_selection_and_owner_drop() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    for (offset, length) in [(0, 1), (i32::MAX as usize - 1, 1), (i32::MAX as usize, 0)] {
        let runs = RunArray::<Int64Type>::try_new(&Int64Array::from(vec![i64::from(i32::MAX)]), &Int64Array::from(vec![42])).unwrap().slice(offset, length);
        let vector = open(Field::new("runs", runs.data_type().clone(), false), Arc::new(runs), &budget);
        let selected = vector.select(&Selection::All).unwrap();
        drop(vector);
        assert!(budget.used_bytes() > 0);
        assert_eq!(selected.checked_len().unwrap(), length);
        if length == 0 {
            assert!(selected.cell(0).is_err());
        } else {
            assert_eq!(selected.cell(0).unwrap().get_long(NullChecks::Enabled).unwrap(), 42);
            assert_eq!(selected.cell(0).unwrap().getter_index().unwrap(), 0);
        }
        drop(selected);
        assert_eq!(budget.used_bytes(), 0);
    }
}

#[test]
fn raw_run_end_list_access_does_not_grant_reference_nested_methods() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    let values = ListArray::new(Arc::new(Field::new("item", DataType::Int64, true)), OffsetBuffer::new(vec![0, 2, 3].into()), Arc::new(Int64Array::new(vec![11, 29, 31].into(), Some(NullBuffer::from(vec![true, false, true])))), None);
    let runs = RunArray::<Int32Type>::try_new(&Int32Array::from(vec![2, 4]), &values).unwrap();
    let vector = open(Field::new("r", runs.data_type().clone(), false), Arc::new(runs), &budget);
    assert_eq!(vector.list_elements().unwrap_err().code, "rtdb/unsupported-vector-reader");
    assert_eq!(vector.struct_child("item").unwrap_err().code, "rtdb/unsupported-vector-reader");
    let elements = vector.cell(1).unwrap().raw_list_elements().unwrap();
    assert_eq!(elements.value_count(), 2);
    assert_eq!(elements.cell(1).unwrap().get_long(NullChecks::Disabled).unwrap(), 29);
    assert_eq!(anomaly(elements.cell(1).unwrap().get_long(NullChecks::Enabled).unwrap_err()).message.as_deref(), Some("null at index 1"));
    drop(vector);
    assert_eq!(elements.cell(0).unwrap().get_long(NullChecks::Enabled).unwrap(), 11);
}

#[test]
fn fixed_list_slice_element_offsets_use_normalized_child_storage() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    let array = FixedSizeListArray::new(Arc::new(Field::new("item", DataType::Int32, false)), 2, Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])), None).slice(1, 2);
    let vector = open(Field::new("list", array.data_type().clone(), false), Arc::new(array), &budget);
    let elements = vector.cell(1).unwrap().raw_list_elements().unwrap();
    assert_eq!(elements.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 5);
    assert_eq!(elements.cell(0).unwrap().getter_index().unwrap(), 2);
}

#[test]
fn date_milli_getter_truncates_raw_millis_toward_zero() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    let vector = open(Field::new("date", DataType::Date64, false), Arc::new(Date64Array::from(vec![-86_400_001, -86_399_999, 86_399_999, 86_400_001])), &budget);
    for (index, expected) in [-1, 0, 0, 1].into_iter().enumerate() { assert_eq!(vector.cell(index).unwrap().get_long(NullChecks::Enabled).unwrap(), expected); }
    assert_eq!(vector.cell(0).unwrap().object().unwrap().value(), &Value::DateMilli(-86_400_001));
}

#[test]
fn retained_cells_and_objects_release_all_charges_after_final_owner() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    let retained = {
        let vector = open(Field::new("text", DataType::Utf8, true), Arc::new(StringArray::from(vec!["abc", "retained payload"])), &budget);
        vector.select(&Selection::Indices(vec![1, 0])).unwrap().owned_cell(0).unwrap()
    };
    let clone = retained.clone();
    let before_object = budget.used_bytes();
    let value = retained.as_ref().object().unwrap();
    assert!(budget.used_bytes() > before_object);
    drop(retained);
    assert_eq!(clone.as_ref().getter_index().unwrap(), 1);
    drop(clone);
    assert!(budget.used_bytes() > 0);
    assert_eq!(value.value(), &Value::Utf8("retained payload".into()));
    drop(value);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn opening_selection_and_materialization_failures_release_new_reservations() {
    let budget = MemoryBudget::new(256 * 1024);
    let field = Field::new("v", DataType::Int64, true);
    let source = batch(&field, Arc::new(Int64Array::from(vec![1, 2])), &budget);
    let before = budget.used_bytes();
    let held = budget.reserve(budget.limit_bytes() - before).unwrap();
    assert_eq!(ExecutionVector::open(Arc::new(field.clone()), source.column(0).unwrap()).unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert_eq!(budget.used_bytes(), budget.limit_bytes());
    drop(held);
    assert_eq!(budget.used_bytes(), before);
    let invalid = field.clone().with_metadata(HashMap::from([("ARROW:extension:name".into(), "unknown".into())]));
    assert!(ExecutionVector::open(Arc::new(invalid), source.column(0).unwrap()).is_err());
    assert_eq!(budget.used_bytes(), before);
    let vector = ExecutionVector::open(Arc::new(field), source.column(0).unwrap()).unwrap();
    let opened = budget.used_bytes();
    assert!(vector.select(&Selection::Indices(vec![2])).is_err());
    assert_eq!(budget.used_bytes(), opened);
    let held = budget.reserve(budget.limit_bytes() - opened).unwrap();
    assert_eq!(vector.select(&Selection::Indices(vec![1])).unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert_eq!(vector.cell(0).unwrap().object().unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert_eq!(budget.used_bytes(), budget.limit_bytes());
    drop(held);
    drop(vector);
    drop(source);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn half_float_rejection_includes_nested_opening_and_releases_source_owners() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    let array: ArrayRef = Arc::new(Float16Array::from(vec![half::f16::from_bits(0x3c00)]));
    let field = Field::new("half", DataType::Float16, false);
    let source = batch(&field, array, &budget);
    let before = budget.used_bytes();
    assert_eq!(ExecutionVector::open(Arc::new(field), source.column(0).unwrap()).unwrap_err().code, "rtdb/half-vector");
    assert_eq!(budget.used_bytes(), before);
    drop(source);
    assert_eq!(budget.used_bytes(), 0);
    let child = Arc::new(Field::new("half", DataType::Float16, false));
    let values: ArrayRef = Arc::new(Float16Array::from(vec![half::f16::from_bits(0x3c00)]));
    let list = ListArray::new(child, OffsetBuffer::new(vec![0, 1].into()), values, None);
    let field = Field::new("list", list.data_type().clone(), false);
    let source = batch(&field, Arc::new(list), &budget);
    let before = budget.used_bytes();
    assert_eq!(ExecutionVector::open(Arc::new(field), source.column(0).unwrap()).unwrap_err().code, "rtdb/half-vector");
    assert_eq!(budget.used_bytes(), before);
    drop(source);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn temporal_long_getters_read_declared_units_and_retain_nonzero_null_bits() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    let mut types = vec![ScalarType::DateMilli, ScalarType::Time { unit: TimeUnit::Microsecond, bit_width: 64 }, ScalarType::Time { unit: TimeUnit::Nanosecond, bit_width: 64 }];
    for unit in [TimeUnit::Second, TimeUnit::Millisecond, TimeUnit::Microsecond, TimeUnit::Nanosecond] {
        types.push(ScalarType::Timestamp { unit, timezone: None });
        types.push(ScalarType::Timestamp { unit, timezone: Some("UTC".into()) });
        types.push(ScalarType::Duration(unit));
    }
    for scalar in types {
        let expected = if scalar == ScalarType::DateMilli { 3 } else { 259_200_017 };
        let field = to_field("v", &LogicalType::Scalar(scalar).maybe()).unwrap();
        let data = arrow_data::ArrayData::builder(field.data_type().clone()).len(2).add_buffer(arrow_buffer::Buffer::from_slice_ref([0i64, 259_200_017])).nulls(invalid()).build().unwrap();
        let vector = open(field, make_array(data), &budget);
        assert_eq!(vector.cell(1).unwrap().get_long(NullChecks::Disabled).unwrap(), expected);
        assert_eq!(anomaly(vector.cell(1).unwrap().get_long(NullChecks::Enabled).unwrap_err()).message.as_deref(), Some("null at index 1"));
        assert_eq!(vector.cell(1).unwrap().object().unwrap().value(), &Value::Null);
    }
}

#[test]
fn structured_interval_vectors_do_not_expose_primitive_numeric_getters() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    for (scalar, value) in [
        (ScalarType::Interval(IntervalUnit::DayTime), Value::IntervalDayTime { days: 3, milliseconds: 7 }),
        (ScalarType::Interval(IntervalUnit::MonthDayNano), Value::IntervalMonthDayNano { months: 2, days: 3, nanoseconds: 7 }),
        (ScalarType::IntervalMonthDayMicro, Value::IntervalMonthDayMicro { months: 2, days: 3, nanoseconds: 7 }),
    ] {
        let logical = LogicalType::Scalar(scalar).maybe();
        let field = to_field("i", &logical).unwrap();
        let array = rtdb_columnar::copy_array(&logical, &[value, Value::Null]).unwrap();
        let vector = open(field, array, &budget);
        for checks in [NullChecks::Enabled, NullChecks::Disabled] {
            assert_eq!(anomaly(vector.cell(1).unwrap().get_int(checks).unwrap_err()).code, "rtdb/unsupported-vector-reader");
            assert_eq!(anomaly(vector.cell(1).unwrap().get_long(checks).unwrap_err()).code, "rtdb/unsupported-vector-reader");
        }
    }
}

#[test]
fn raw_nested_cells_preserve_dense_union_and_run_end_struct_ownership() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    let fields = Fields::from(vec![Field::new("value", DataType::Int64, true)]);
    let values = StructArray::new(fields, vec![Arc::new(Int64Array::new(vec![1, 73].into(), invalid()))], None);
    let runs = RunArray::<Int32Type>::try_new(&Int32Array::from(vec![2, 5]), &values).unwrap();
    let vector = open(Field::new("r", runs.data_type().clone(), false), Arc::new(runs), &budget);
    let child = vector.cell(4).unwrap().raw_struct_field("value").unwrap().unwrap();
    assert!(vector.struct_child("value").is_err());
    drop(vector);
    assert_eq!(child.as_ref().get_long(NullChecks::Disabled).unwrap(), 73);
    assert_eq!(anomaly(child.as_ref().get_long(NullChecks::Enabled).unwrap_err()).message.as_deref(), Some("null at index 1"));
    drop(child);
    assert_eq!(budget.used_bytes(), 0);

    let elements: ArrayRef = Arc::new(Int64Array::new(vec![5, 83].into(), invalid()));
    let lists = ListArray::new(Arc::new(Field::new("element", DataType::Int64, true)), OffsetBuffer::new(vec![0, 1, 2].into()), elements, None);
    let union_fields = UnionFields::try_new([4, 1], vec![Arc::new(Field::new("list", lists.data_type().clone(), false)), Arc::new(Field::new("null", DataType::Null, true))]).unwrap();
    let union = UnionArray::try_new(union_fields, vec![4i8, 1].into(), Some(vec![1i32, 0].into()), vec![Arc::new(lists), Arc::new(NullArray::new(1))]).unwrap();
    let vector = open(Field::new("u", union.data_type().clone(), false), Arc::new(union), &budget);
    assert!(vector.list_elements().is_err());
    let child = vector.cell(0).unwrap().raw_list_elements().unwrap().owned_cell(0).unwrap();
    drop(vector);
    assert_eq!(child.as_ref().get_long(NullChecks::Disabled).unwrap(), 83);
    assert_eq!(child.as_ref().getter_index().unwrap(), 1);
    drop(child);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn null_parent_lists_keep_raw_offsets_and_nullable_children() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    let values: ArrayRef = Arc::new(Int32Array::new(vec![13, 17].into(), invalid()));
    let lists = ListArray::new(Arc::new(Field::new("item", DataType::Int32, true)), OffsetBuffer::new(vec![0, 1, 2].into()), values, invalid());
    let vector = open(Field::new("l", lists.data_type().clone(), true), Arc::new(lists), &budget);
    let selected = vector.select(&Selection::Indices(vec![1, 0])).unwrap();
    assert_eq!(selected.cell(0).unwrap().object().unwrap().value(), &Value::Null);
    let child = selected.cell(0).unwrap().raw_list_elements().unwrap();
    assert_eq!(child.cell(0).unwrap().getter_index().unwrap(), 1);
    assert_eq!(child.cell(0).unwrap().get_int(NullChecks::Disabled).unwrap(), 17);
    assert_eq!(selected.list_elements().unwrap().value_count(), 2);
}

#[test]
fn map_entries_retain_raw_child_payloads_and_reference_container_access() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    let fields = Fields::from(vec![Field::new("key", DataType::Utf8, false), Field::new("value", DataType::Int64, true)]);
    let entries = StructArray::new(fields, vec![Arc::new(StringArray::from(vec!["a", "b"])), Arc::new(Int64Array::new(vec![3, 29].into(), invalid()))], None);
    let field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    let map = MapArray::new(field, OffsetBuffer::new(vec![0, 1, 2].into()), entries, invalid(), false);
    let vector = open(Field::new("map", map.data_type().clone(), true), Arc::new(map), &budget);
    assert_eq!(vector.cell(1).unwrap().object().unwrap().value(), &Value::Null);
    let entries = vector.cell(1).unwrap().raw_list_elements().unwrap();
    let value = entries.struct_child("value").unwrap().unwrap().owned_cell(0).unwrap();
    assert_eq!(value.as_ref().getter_index().unwrap(), 1);
    assert_eq!(value.as_ref().get_long(NullChecks::Disabled).unwrap(), 29);
    assert_eq!(vector.list_elements().unwrap().value_count(), 2);
    drop(entries);
    drop(vector);
    assert_eq!(anomaly(value.as_ref().get_long(NullChecks::Enabled).unwrap_err()).message.as_deref(), Some("null at index 1"));
    drop(value);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn null_float_nan_payloads_preserve_their_bit_patterns() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    let bits = 0x7fc0_1234u32;
    let vector = open(Field::new("v", DataType::Float32, true), Arc::new(Float32Array::new(vec![0.0, f32::from_bits(bits)].into(), invalid())), &budget);
    assert_eq!(vector.cell(1).unwrap().get_float(NullChecks::Disabled).unwrap().to_bits(), bits);
    assert_eq!(vector.cell(1).unwrap().object().unwrap().value(), &Value::Null);
}

#[test]
fn raw_reserved_opening_normalizes_nested_required_validity_before_publication() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    let child = Arc::new(Field::new("value", DataType::Int64, false));
    let nested = StructArray::new(Fields::from(vec![child]), vec![Arc::new(Int64Array::new(vec![0, 101].into(), invalid()))], invalid());
    let list_child = Arc::new(Field::new("record", nested.data_type().clone(), false));
    let list = FixedSizeListArray::new(list_child, 1, Arc::new(nested), invalid());
    let field = Arc::new(Field::new("list", list.data_type().clone(), false));
    let array: ArrayRef = Arc::new(list);
    let reservation = budget.reserve(array.get_array_memory_size()).unwrap();
    let vector = ExecutionVector::with_reservation(field, Arc::clone(&array), reservation).unwrap();
    assert!(array.is_null(1));
    drop(array);
    assert!(!vector.cell(1).unwrap().is_null().unwrap());
    let element = vector.cell(1).unwrap().raw_list_elements().unwrap().owned_cell(0).unwrap();
    assert!(!element.as_ref().is_null().unwrap());
    let value = element.as_ref().raw_struct_field("value").unwrap().unwrap();
    drop(vector);
    drop(element);
    assert_eq!(value.as_ref().get_long(NullChecks::Enabled).unwrap(), 101);
    drop(value);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn raw_reserved_opening_rejects_insufficient_buffer_coverage_without_leaking() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let reservation = budget.reserve(1).unwrap();
    assert_eq!(ExecutionVector::with_reservation(Arc::new(Field::new("v", DataType::Int64, false)), array, reservation).unwrap_err().code, "rtdb/undersized-reservation");
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn failed_logical_object_decode_releases_scratch_and_null_object_skips_payload_validation() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    let field = to_field("uri", &LogicalType::Scalar(ScalarType::Uri).maybe()).unwrap();
    let array: ArrayRef = Arc::new(StringArray::new(OffsetBuffer::new(vec![0, 1, 2].into()), arrow_buffer::Buffer::from(b"%%".as_slice()), invalid()));
    let vector = open(field, array, &budget);
    let before = budget.used_bytes();
    assert!(vector.cell(0).unwrap().object().is_err());
    assert_eq!(budget.used_bytes(), before);
    assert_eq!(vector.cell(1).unwrap().object().unwrap().value(), &Value::Null);
    assert_eq!(budget.used_bytes(), before);
    drop(vector);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn execution_length_rejects_more_than_the_signed_vector_domain_without_allocating_rows() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    let array: ArrayRef = Arc::new(NullArray::new(i32::MAX as usize + 1));
    let reservation = budget.reserve(array.get_array_memory_size()).unwrap();
    assert_eq!(ExecutionVector::with_reservation(Arc::new(Field::new("null", DataType::Null, true)), array, reservation).unwrap_err().code, "rtdb/vector-length");
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn opened_schema_depth_admission_precedes_recursive_logical_construction() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    let mut field = Field::new("leaf", DataType::Int64, false);
    let mut array: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
    for _ in 0..128 {
        array = Arc::new(ListArray::new(Arc::new(field), OffsetBuffer::new_empty(), array, None));
        field = Field::new("list", array.data_type().clone(), false);
    }
    let reservation = budget.reserve(array.get_array_memory_size()).unwrap();
    assert_eq!(ExecutionVector::with_reservation(Arc::new(field), array, reservation).unwrap_err().code, "rtdb/vector-schema-depth");
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn timestamp_zone_admission_includes_empty_and_nested_vectors() {
    let budget = MemoryBudget::new(16 * 1024 * 1024);
    for timezone in ["Not/A_Zone", "", "+19:00"] {
        for values in [vec![], vec![1i64]] {
            let array: ArrayRef = Arc::new(TimestampSecondArray::from(values).with_timezone(timezone));
            let field = Arc::new(Field::new("ts", array.data_type().clone(), false));
            let reservation = budget.reserve(array.get_array_memory_size()).unwrap();
            assert_eq!(ExecutionVector::with_reservation(field, array, reservation).unwrap_err().code, "xtdb/invalid-timezone");
            assert_eq!(budget.used_bytes(), 0);
        }
        let values: ArrayRef = Arc::new(TimestampSecondArray::from(vec![1i64]).with_timezone(timezone));
        let child = Arc::new(Field::new("item", values.data_type().clone(), false));
        let array: ArrayRef = Arc::new(ListArray::new(child, OffsetBuffer::new(vec![0, 1].into()), values, None));
        let field = Arc::new(Field::new("list", array.data_type().clone(), false));
        let reservation = budget.reserve(array.get_array_memory_size()).unwrap();
        assert_eq!(ExecutionVector::with_reservation(field, array, reservation).unwrap_err().code, "xtdb/invalid-timezone");
        assert_eq!(budget.used_bytes(), 0);
    }
    for timezone in ["UTC", "Europe/Paris", "+01", "GMT+1"] {
        let array: ArrayRef = Arc::new(TimestampSecondArray::from(vec![1i64]).with_timezone(timezone));
        let field = Arc::new(Field::new("ts", array.data_type().clone(), false));
        let reservation = budget.reserve(array.get_array_memory_size()).unwrap();
        let vector = ExecutionVector::with_reservation(field, array, reservation).unwrap();
        assert_eq!(vector.cell(0).unwrap().get_long(NullChecks::Enabled).unwrap(), 1);
        drop(vector);
        assert_eq!(budget.used_bytes(), 0);
    }
}
