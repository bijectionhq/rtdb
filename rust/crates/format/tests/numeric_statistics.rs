use std::sync::Arc;

use arrow_array::{ArrayRef, Date32Array, Float64Array, Int32Array, TimestampMicrosecondArray};
use arrow_schema::Field;
use rtdb_types::Value;

fn bounds(array: ArrayRef, group: &str) -> (Value, Value) {
    let field = Field::new("v", array.data_type().clone(), true);
    let mut columns = Vec::new();
    rtdb_format::metadata::column_metadata(&field, array.as_ref(), true, false, &mut columns).unwrap();
    let numbers = columns[0].field(group).unwrap_or_else(|| panic!("{group} statistics for {:?}", array.data_type()));
    (numbers.field("min").cloned().unwrap(), numbers.field("max").cloned().unwrap())
}

#[test]
fn numeric_bounds_are_the_least_and_greatest_cells_as_seconds_and_nan_absorbs_them() {
    assert_eq!(bounds(Arc::new(Int32Array::from(vec![Some(5), None, Some(-3), Some(9)])), "numbers"), (Value::F64(-3.0), Value::F64(9.0)));
    let (min, max) = bounds(Arc::new(Float64Array::from(vec![Some(1.5), Some(f64::NAN), Some(-2.0)])), "numbers");
    assert!(matches!((min, max), (Value::F64(min), Value::F64(max)) if min.is_nan() && max.is_nan()));
    assert_eq!(bounds(Arc::new(TimestampMicrosecondArray::from(vec![Some(1_500_000), None, Some(-2_000_000)]).with_timezone("UTC")), "date-times"), (Value::F64(-2.0), Value::F64(1.5)));
    assert_eq!(bounds(Arc::new(Date32Array::from(vec![Some(2), Some(1)])), "date-times"), (Value::F64(86_400.0), Value::F64(172_800.0)));
}
