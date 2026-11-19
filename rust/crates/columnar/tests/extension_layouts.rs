use rtdb_types::StructFields;
use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, ListArray, RecordBatch, UnionArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit as ArrowTimeUnit, UnionFields, UnionMode};
use rtdb_columnar::{EXTENSION_NAME_KEY, MemoryBudget, OwnedBatch, conform_batch, copy_array, from_field, read_value, to_field};
use rtdb_types::{ListKind, LogicalType, RunEndWidth, ScalarType, TimeUnit, Value};

fn extension(name: &str, data_type: DataType, extension: &str) -> Field {
    Field::new(name, data_type, false).with_metadata(HashMap::from([(EXTENSION_NAME_KEY.into(), extension.into())]))
}

fn source(value: &Value) -> RecordBatch {
    let logical = value.logical_type();
    RecordBatch::try_new(Arc::new(Schema::new(vec![to_field("v", &logical).unwrap()])), vec![copy_array(&logical, &[value.clone()]).unwrap()]).unwrap()
}

#[test]
fn set_children_keep_declared_struct_order_names_and_metadata() {
    let memory = MemoryBudget::new(1 << 23);
    let value = Value::Set(vec![Value::Struct(StructFields::from([("a".into(), Value::I64(7)), ("z".into(), Value::Utf8("last".into()))]))]);
    let child = Field::new("members", DataType::Struct(vec![Field::new("z", DataType::Utf8, false), Field::new("a", DataType::Int64, false)].into()), false)
        .with_metadata(HashMap::from([("child-origin".into(), "declared".into())]));
    let target = Schema::new(vec![extension("v", DataType::List(Arc::new(child)), "set")]);
    assert_eq!(from_field(target.field(0)).unwrap(), value.logical_type());
    let page = conform_batch(OwnedBatch::new(source(&value), &memory).unwrap(), &target, &memory).unwrap();
    assert_eq!(page.batch().schema().as_ref(), &target);
    assert_eq!(read_value(target.field(0), page.batch().column(0).as_ref(), 0).unwrap(), value);
    let retained = Arc::clone(page.batch().column(0));
    drop(page);
    assert!(memory.used_bytes() > 0);
    assert_eq!(read_value(target.field(0), retained.as_ref(), 0).unwrap(), value);
    drop(retained);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn set_children_keep_declared_union_ids() {
    let memory = MemoryBudget::new(1 << 23);
    let value = Value::Set(vec![Value::Utf8("first".into()), Value::I64(11)]);
    let legs = UnionFields::try_new([23, 11], [Field::new("utf8", DataType::Utf8, false), Field::new("i64", DataType::Int64, false)]).unwrap();
    let target = Schema::new(vec![extension("v", DataType::List(Arc::new(Field::new("members", DataType::Union(legs, UnionMode::Dense), false))), "set")]);
    assert_eq!(from_field(target.field(0)).unwrap(), value.logical_type());
    let page = conform_batch(OwnedBatch::new(source(&value), &memory).unwrap(), &target, &memory).unwrap();
    assert_eq!(page.batch().schema().as_ref(), &target);
    let list = page.batch().column(0).as_any().downcast_ref::<ListArray>().unwrap();
    let union = list.values().as_any().downcast_ref::<UnionArray>().unwrap();
    assert_eq!(union.type_ids().as_ref(), &[23, 11]);
    assert_eq!(read_value(target.field(0), list, 0).unwrap(), value);
    drop(page);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn timestamp_range_children_keep_declared_names_and_metadata() {
    let memory = MemoryBudget::new(1 << 22);
    let value = Value::TimestampTzRange { lower: i64::MIN, upper: i64::MAX };
    let child = Field::new("endpoints", DataType::Timestamp(ArrowTimeUnit::Microsecond, Some("UTC".into())), false)
        .with_metadata(HashMap::from([("child-origin".into(), "declared".into())]));
    let target = Schema::new(vec![extension("v", DataType::FixedSizeList(Arc::new(child), 2), "xt/tstz-range")]);
    assert_eq!(from_field(target.field(0)).unwrap(), value.logical_type());
    let page = conform_batch(OwnedBatch::new(source(&value), &memory).unwrap(), &target, &memory).unwrap();
    assert_eq!(page.batch().schema().as_ref(), &target);
    assert_eq!(read_value(target.field(0), page.batch().column(0).as_ref(), 0).unwrap(), value);
    drop(page);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn list_extension_type_decoding_preserves_the_child_type_and_nullability() {
    let child = Field::new("endpoints", DataType::Timestamp(ArrowTimeUnit::Nanosecond, Some("Europe/London".into())), true);
    let field = extension("range", DataType::FixedSizeList(Arc::new(child), 2), "xt/tstz-range").with_nullable(true);
    assert_eq!(from_field(&field).unwrap(), LogicalType::List { kind: ListKind::TimestampTzRange,
        element: Box::new(LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Nanosecond, timezone: Some("Europe/London".into()) }).maybe()) }.maybe());
    let field = extension("set", DataType::List(Arc::new(Field::new("unobserved", DataType::Null, false))), "set");
    assert_eq!(from_field(&field).unwrap(), LogicalType::List { kind: ListKind::Set, element: Box::new(LogicalType::Nothing) });
}

#[test]
fn extension_layout_checks_reject_wrong_containers_and_scalar_storage() {
    let child = Arc::new(Field::new("items", DataType::Int64, false));
    for (name, storage) in [
        ("set", DataType::FixedSizeList(Arc::clone(&child), 2)),
        ("xt/tstz-range", DataType::List(Arc::clone(&child))),
        ("xt/tstz-range", DataType::FixedSizeList(Arc::clone(&child), 3)),
        ("uuid", DataType::FixedSizeBinary(15)),
        ("xt/clj-keyword", DataType::Binary),
    ] {
        assert_eq!(from_field(&extension("v", storage, name)).unwrap_err().code, "rtdb/extension-storage-type");
    }
}

#[test]
fn extension_children_still_reject_unknown_extensions_and_encoded_union_legs() {
    let unknown = extension("items", DataType::Int64, "unknown");
    assert_eq!(from_field(&extension("v", DataType::List(Arc::new(unknown)), "set")).unwrap_err().code, "rtdb/unknown-arrow-extension");
    let encoded = to_field("i64", &LogicalType::Scalar(ScalarType::I64).run_end_encoded(RunEndWidth::I32).unwrap()).unwrap();
    let fields = UnionFields::try_new([0, 1], [encoded, Field::new("utf8", DataType::Utf8, false)]).unwrap();
    let child = Field::new("members", DataType::Union(fields, UnionMode::Dense), false);
    assert_eq!(from_field(&extension("v", DataType::List(Arc::new(child)), "set")).unwrap_err().code, "rtdb/encoded-union-leg");
}
