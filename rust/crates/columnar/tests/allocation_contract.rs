use rtdb_columnar::{MemoryBudget, OwnedBatch, build_array, estimate_builder_bytes, to_field};
use rtdb_types::{ListKind, LogicalType, RunEndWidth, ScalarType, Value};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use std::{collections::BTreeMap, sync::Arc};

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
fn builder_bounds_cover_variable_data_and_run_end_children() {
    let values = vec![Value::Utf8("x".repeat(65_537)), Value::Null, Value::Utf8("y".repeat(65_538))];
    let logical = LogicalType::Scalar(ScalarType::Utf8).maybe();
    retains_within_bound(logical.clone(), values.clone());
    retains_within_bound(LogicalType::RunEndEncoded { width: RunEndWidth::I32, value: Box::new(logical) }, values);
}

#[test]
fn builder_bounds_include_fixed_children_hidden_under_null_parents() {
    let fixed = LogicalType::List { kind: ListKind::FixedSizeList(4096), element: Box::new(LogicalType::Scalar(ScalarType::I64)) };
    let logical = LogicalType::Struct(BTreeMap::from([("items".into(), fixed)])).maybe();
    retains_within_bound(logical, vec![Value::Null; 3]);
}

#[test]
fn builder_bounds_select_the_matching_nested_union_leg() {
    let list = Value::List(vec![Value::I64(1), Value::Null]);
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
