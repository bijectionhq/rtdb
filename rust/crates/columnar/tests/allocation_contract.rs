use rtdb_types::StructFields;
use rtdb_columnar::{MemoryBudget, OwnedBatch, build_array, estimate_builder_bytes, to_field};
use rtdb_types::{ListKind, LogicalType, RunEndWidth, ScalarType, Value};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use std::sync::Arc;

fn retains_within_bound(logical: LogicalType, values: Vec<Value>) {
    let bound = estimate_builder_bytes(&logical, &values).unwrap();
    let budget = MemoryBudget::new(bound);
    let reservation = budget.reserve(bound).unwrap();
    let array = build_array(&logical, &values).unwrap();
    let schema = Arc::new(Schema::new(vec![to_field("value", &logical).unwrap()]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    assert!(batch.get_array_memory_size() <= bound);
    let batch = OwnedBatch::with_reservation(batch, reservation).unwrap();
    let retained = batch.batch().clone();
    drop(batch);
    assert_eq!(budget.used_bytes(), bound);
    drop(retained);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn an_arrow_batch_charges_shared_buffer_capacity_once() {
    use arrow_array::{Array, ArrayRef, Int64Array};
    use arrow_schema::{DataType, Field};

    let shared = Int64Array::from_iter_values(0..8192);
    let columns = (0..128).map(|index| Arc::new(shared.slice(index, 32)) as ArrayRef).collect();
    let fields = (0..128).map(|index| Field::new(format!("v{index}"), DataType::Int64, false)).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let memory = MemoryBudget::new(1 << 17);
    assert!(batch.get_array_memory_size() > memory.limit_bytes());
    let error = OwnedBatch::with_reservation(batch.clone(), memory.reserve(1 << 15).unwrap()).unwrap_err();
    assert_eq!(error.code, "rtdb/undersized-reservation");
    let batch = OwnedBatch::with_reservation(batch, memory.reserve(1 << 17).unwrap()).unwrap();
    let column = Arc::clone(batch.batch().column(100));
    let slice = column.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(slice.values().as_ptr(), shared.values()[100..].as_ptr());
    assert_eq!(slice.get_buffer_memory_size(), 32 * std::mem::size_of::<i64>());
    drop(batch);
    assert_eq!(memory.used_bytes(), 1 << 17);
    assert_eq!(column.as_any().downcast_ref::<Int64Array>().unwrap().value(0), 100);
    drop(column);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn scoped_budgets_share_one_limit_and_keep_independent_peaks_until_last_lease() {
    let root = MemoryBudget::new(100);
    let first = root.scope(); let second = root.scope();
    let retained = first.reserve(60).unwrap();
    assert!(second.reserve(41).is_err());
    assert_eq!(second.used_bytes(), 0);
    let temporary = second.reserve(40).unwrap();
    assert_eq!(root.used_bytes(), 100);
    assert_eq!(first.peak_bytes(), 60); assert_eq!(second.peak_bytes(), 40);
    drop(temporary); drop(first);
    assert_eq!(root.used_bytes(), 60);
    let cloned = retained.clone(); drop(retained);
    assert_eq!(root.used_bytes(), 60);
    drop(cloned); assert_eq!(root.used_bytes(), 0); assert_eq!(root.peak_bytes(), 100);
}

#[test]
fn builder_bounds_cover_variable_data_and_run_end_children() {
    let values = vec![Value::Utf8("x".repeat(65_537)), Value::Null, Value::Utf8("y".repeat(65_538))];
    let logical = LogicalType::Scalar(ScalarType::Utf8).maybe();
    retains_within_bound(logical.clone(), values.clone());
    retains_within_bound(LogicalType::RunEndEncoded { width: RunEndWidth::I32, value: Box::new(logical) }, values);
}

#[test]
fn builder_bounds_include_fixed_children_hidden_under_null_parents() {
    let fixed = LogicalType::List { kind: ListKind::FixedSizeList(4096), element: Box::new(LogicalType::Scalar(ScalarType::I64)) };
    let logical = LogicalType::Struct(StructFields::from([("items".into(), fixed)])).maybe();
    retains_within_bound(logical, vec![Value::Null; 3]);
}

#[test]
fn builder_bounds_select_the_matching_nested_union_leg() {
    let list = Value::list(vec![Value::I64(1), Value::Null]);
    let map = Value::Map { entries: vec![(Value::Utf8("key".into()), Value::Binary(vec![1; 32769]))], sorted: false };
    let logical = list.logical_type().join(&map.logical_type());
    retains_within_bound(logical, vec![list, map]);
}

#[test]
fn impossible_fixed_width_products_fail_before_allocation() {
    let mut logical = LogicalType::Scalar(ScalarType::I64);
    for _ in 0..4 {
        logical = LogicalType::List { kind: ListKind::FixedSizeList(i32::MAX), element: Box::new(logical) };
    }
    assert!(estimate_builder_bytes(&logical.maybe(), &[Value::Null]).is_err());
}

#[test]
fn selected_cell_read_bound_ignores_unselected_variable_payloads_and_union_legs() {
    use rtdb_columnar::{estimate_read_value_bytes, read_value};
    let values = vec![Value::Utf8("small".into()), Value::Binary(vec![7; 1 << 20])];
    let ty = values[0].logical_type().join(&values[1].logical_type());
    let field = to_field("value", &ty).unwrap();
    let array = build_array(&ty, &values).unwrap();
    let small = estimate_read_value_bytes(&field, array.as_ref(), 0).unwrap();
    let large = estimate_read_value_bytes(&field, array.as_ref(), 1).unwrap();
    assert!(small < 100_000);
    assert!(large >= 1 << 20);
    assert!(small >= read_value(&field, array.as_ref(), 0).unwrap().retained_bytes().unwrap());
    let slice = array.slice(1, 1);
    assert_eq!(estimate_read_value_bytes(&field, slice.as_ref(), 0).unwrap(), large);
}

#[test]
fn selected_nested_and_run_end_cells_fit_the_read_bound() {
    use rtdb_columnar::{estimate_read_value_bytes, read_value};
    let values = vec![
        Value::Struct(StructFields::from([("list".into(), Value::list(vec![Value::Utf8("abc".into()), Value::Null])), ("map".into(), Value::Map { entries: vec![(Value::Utf8("key".into()), Value::Binary(vec![1; 8192]))], sorted: false })])),
        Value::Struct(StructFields::from([("list".into(), Value::list(vec![])), ("map".into(), Value::Map { entries: vec![], sorted: false })])),
    ];
    let ty = values[0].logical_type().join(&values[1].logical_type());
    for ty in [ty.clone(), LogicalType::RunEndEncoded { width: RunEndWidth::I16, value: Box::new(ty) }] {
        let field = to_field("value", &ty).unwrap();
        let array = build_array(&ty, &values).unwrap();
        for row in 0..2 {
            let bound = estimate_read_value_bytes(&field, array.as_ref(), row).unwrap();
            assert!(bound >= read_value(&field, array.as_ref(), row).unwrap().retained_bytes().unwrap());
        }
        let slice = array.slice(1, 1);
        assert_eq!(estimate_read_value_bytes(&field, slice.as_ref(), 0).unwrap(), estimate_read_value_bytes(&field, array.as_ref(), 1).unwrap());
    }
}

#[test]
fn selected_list_bound_uses_its_offsets_and_rejects_unknown_extensions() {
    use rtdb_columnar::estimate_read_value_bytes;
    let ty = LogicalType::list(LogicalType::Scalar(ScalarType::Binary));
    let field = to_field("value", &ty).unwrap();
    let array = build_array(&ty, &[Value::list(vec![Value::Binary(vec![1; 1 << 20])]), Value::list(vec![Value::Binary(vec![2])])]).unwrap();
    assert!(estimate_read_value_bytes(&field, array.as_ref(), 1).unwrap() < 100_000);
    let slice = array.slice(1, 1);
    assert_eq!(estimate_read_value_bytes(&field, slice.as_ref(), 0).unwrap(), estimate_read_value_bytes(&field, array.as_ref(), 1).unwrap());
    let field = field.with_metadata(std::collections::HashMap::from([(rtdb_columnar::EXTENSION_NAME_KEY.into(), "unknown/extension".into())]));
    assert_eq!(estimate_read_value_bytes(&field, array.as_ref(), 0).unwrap_err().code, "rtdb/unknown-arrow-extension");
}

#[test]
fn columnar_and_foundation_budgets_share_the_same_lease_type_and_charge() {
    let budget: Arc<rtdb_types::MemoryBudget> = MemoryBudget::new(1 << 20);
    let array = std::sync::Arc::new(arrow_array::Int64Array::from(vec![1, 2]));
    let schema = Arc::new(Schema::new(vec![arrow_schema::Field::new("v", arrow_schema::DataType::Int64, false)]));
    let raw = RecordBatch::try_new(schema, vec![array]).unwrap();
    let bytes = raw.get_array_memory_size();
    let reservation: Arc<rtdb_types::MemoryReservation> = budget.reserve(bytes).unwrap();
    let batch = OwnedBatch::with_reservation(raw, reservation).unwrap();
    assert!(Arc::ptr_eq(batch.reservation().budget(), &budget));
    let retained = batch.batch().clone();
    drop(batch);
    assert_eq!(budget.used_bytes(), bytes);
    drop(retained);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn wide_sparse_nested_documents_estimate_their_builders_once_per_type_node() {
    let scalar = |seed: usize| match seed % 3 { 0 => Value::I64(seed as i64), 1 => Value::Utf8(format!("v{seed}")), _ => Value::Bool(seed % 2 == 0) };
    let nested = |seed: usize, depth: usize| (0..depth).fold(scalar(seed), |value, level| Value::Struct(StructFields::from([(format!("n{}_{level}", seed % 7), value)])));
    let values: Vec<_> = (0..40).map(|doc| Value::Struct((0..12).map(|field| (format!("f{}", (doc * 7 + field) % 60), nested(doc + field, 1 + (doc + field) % 5))).collect())).collect();
    let logical = LogicalType::join_all(&values.iter().map(Value::logical_type).collect::<Vec<_>>());
    let bound = estimate_builder_bytes(&logical, &values).unwrap();
    assert!(bound < 128 << 20, "{bound} bytes estimated for 40 sparse documents");
    retains_within_bound(logical, values);
}

#[test]
fn a_cell_estimate_charges_the_column_schema_once_not_per_nested_value() {
    use rtdb_columnar::{build_array, estimate_read_cell_bytes, estimate_read_value_bytes, to_field};
    use rtdb_types::{LogicalType, Value};
    let elements = (0..200).map(|i| if i % 2 == 0 { Value::I64(i) } else { Value::Utf8(format!("v{i}")) }).collect::<Vec<_>>();
    let values = vec![Value::list(elements)];
    let logical = LogicalType::join_all(values.iter().map(Value::logical_type).collect::<Vec<_>>().iter());
    let field = to_field("v", &logical).unwrap();
    let array = build_array(&logical, &values).unwrap();
    let cell = estimate_read_cell_bytes(&field, array.as_ref(), 0).unwrap();
    let with_schema = estimate_read_value_bytes(&field, array.as_ref(), 0).unwrap();
    assert!(with_schema > cell);
    assert!(with_schema - cell < cell, "schema charged once: cell {cell}, with schema {with_schema}");
}

#[test]
fn a_builder_estimate_grows_linearly_with_value_nesting() {
    use rtdb_columnar::estimate_builder_bytes;
    use rtdb_types::Value;
    let nested = |depth: usize| (0..depth).fold(Value::I64(1), |inner, _| Value::list(vec![inner]));
    let estimate = |depth: usize| { let value = nested(depth); estimate_builder_bytes(&value.logical_type(), &[value]).unwrap() };
    let (shallow, deep) = (estimate(8), estimate(16));
    assert!(deep < shallow * 8, "doubling the depth grew the estimate from {shallow} to {deep}");
}
