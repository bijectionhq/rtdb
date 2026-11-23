use rtdb_types::OrderedSet;
use rtdb_types::StructFields;
use rtdb_columnar::{admit_execution_value, copy_array, execution_field_type, execution_type, from_field, read_execution_value, read_value, to_field};
use rtdb_types::{ListKind, LogicalType as T, RunEndWidth, ScalarType as S, Value as V};

#[test]
fn unsigned_storage_bits_remain_lossless_and_execution_reads_signed_widths() {
    for (unsigned, signed, raw, expected) in [
        (S::U8, S::I8, V::U8(255), V::I8(-1)),
        (S::U16, S::I16, V::U16(1 << 15), V::I16(i16::MIN)),
        (S::U32, S::I32, V::U32(u32::MAX), V::I32(-1)),
        (S::U64, S::I64, V::U64(1 << 63), V::I64(i64::MIN)),
        (S::U64, S::I64, V::U64(u64::MAX), V::I64(-1)),
        (S::U64, S::I64, V::U64((1 << 53) + 1), V::I64((1 << 53) + 1)),
    ] {
        let physical = T::Scalar(unsigned).maybe();
        let field = to_field("v", &physical).unwrap();
        let array = copy_array(&physical, &[raw.clone(), V::Null]).unwrap();
        assert_eq!(from_field(&field).unwrap(), physical);
        assert_eq!(execution_field_type(&field).unwrap(), T::Scalar(signed).maybe());
        assert_eq!(read_execution_value(&field, array.as_ref(), 0).unwrap(), expected);
        assert_eq!(read_execution_value(&field, array.as_ref(), 1).unwrap(), V::Null);
        assert_eq!(read_value(&field, array.as_ref(), 0).unwrap(), raw);
        assert_eq!(from_field(&field).unwrap(), physical);
        let empty = copy_array(&physical, &[]).unwrap();
        assert_eq!(empty.data_type(), field.data_type());
    }
}

#[test]
fn execution_types_retain_nested_layouts_and_merge_collapsed_union_legs() {
    let union = T::Union(OrderedSet::from([T::Scalar(S::U64), T::Scalar(S::I64), T::Null]));
    assert_eq!(execution_type(&union), T::Scalar(S::I64).maybe());
    let nested = T::Struct(StructFields::from([
        ("values".into(), T::List { kind: ListKind::FixedSizeList(2), element: Box::new(union) }),
        ("encoded".into(), T::Scalar(S::U8).maybe().run_end_encoded(RunEndWidth::I16).unwrap()),
    ]));
    let expected = T::Struct(StructFields::from([
        ("values".into(), T::List { kind: ListKind::FixedSizeList(2), element: Box::new(T::Scalar(S::I64).maybe()) }),
        ("encoded".into(), T::Scalar(S::I8).maybe().run_end_encoded(RunEndWidth::I16).unwrap()),
    ]));
    assert_eq!(execution_type(&nested), expected);
    let mut value = V::Struct(StructFields::from([
        ("values".into(), V::FixedSizeList(vec![V::U64(u64::MAX), V::Null].into())),
        ("encoded".into(), V::U8(255)),
    ]));
    admit_execution_value(&mut value).unwrap();
    assert_eq!(value, V::Struct(StructFields::from([
        ("values".into(), V::FixedSizeList(vec![V::I64(-1), V::Null].into())),
        ("encoded".into(), V::I8(-1)),
    ])));
    let mut map = V::Map { entries: vec![(V::U16(u16::MAX), V::Set(vec![V::U32(u32::MAX)]))], sorted: true };
    let capacity = match &map { V::Map { entries, .. } => entries.capacity(), _ => unreachable!() };
    admit_execution_value(&mut map).unwrap();
    assert_eq!(map, V::Map { entries: vec![(V::I16(-1), V::Set(vec![V::I32(-1)]))], sorted: true });
    assert!(matches!(map, V::Map { entries, .. } if entries.capacity() == capacity));
}

#[test]
fn run_end_and_nested_cells_use_the_execution_view_without_rewriting_arrays() {
    let raw = V::list(vec![V::U8(255), V::U8(128)]);
    let physical = T::list(T::Scalar(S::U8)).run_end_encoded(RunEndWidth::I32).unwrap();
    let field = to_field("v", &physical).unwrap();
    let array = copy_array(&physical, &[raw.clone(), raw.clone()]).unwrap();
    for row in 0..2 {
        assert_eq!(read_execution_value(&field, array.as_ref(), row).unwrap(), V::list(vec![V::I8(-1), V::I8(-128)]));
        assert_eq!(read_value(&field, array.as_ref(), row).unwrap(), raw);
    }
    assert_eq!(from_field(&field).unwrap(), physical);
}
