use rtdb_types::StructFields;
use rtdb_types::{EntityId, Iid, Interval, Keyword, ListKind, LogicalType as T, RunEndWidth, ScalarType as S, SourcePosition, ReplicaPosition, StorageEpoch, TableRef, Timestamp, TxBasis, TxId, Value};
use std::collections::BTreeMap;

fn scalar(scalar: S) -> T { T::Scalar(scalar) }

#[test]
fn decimal_metadata_accepts_signed_precision_and_scale_independently_of_width() {
    for (precision, scale) in [(i32::MIN, i32::MAX), (-1, 0), (0, 3), (1, 3), (32, -128), (32, 127), (39, 0), (256, i32::MIN), (i32::MAX, i32::MAX)] {
        assert!(S::Decimal { precision, scale, bit_width: 128 }.validate().is_ok());
    }
    assert!(S::Decimal { precision: 1, scale: 0, bit_width: 16 }.validate().is_err());
}

#[test]
fn decimal_least_upper_bound_preserves_kotlin_int_wrapping_and_width_selection() {
    let decimal = |precision, scale, bit_width| scalar(S::Decimal { precision, scale, bit_width });
    for (left, right, expected) in [
        (decimal(32, -100, 128), decimal(32, 100, 128), decimal(232, 100, 256)),
        (decimal(32, 0, 128), decimal(32, i32::MAX, 128), decimal(-2_147_483_617, i32::MAX, 32)),
        (decimal(32, i32::MIN, 128), decimal(32, 0, 128), decimal(32, 0, 128)),
        (decimal(i32::MAX, 0, 256), decimal(1, 1, 32), decimal(i32::MIN, 1, 32)),
    ] {
        assert_eq!(T::least_upper_bound(&[left.clone(), right.clone()]), Some(expected));
        assert_eq!(left.join(&right), T::from_legs([left, right]));
    }
}
fn structure(fields: &[(&str, T)]) -> T {
    T::Struct(fields.iter().map(|(name, ty)| ((*name).to_owned(), ty.clone())).collect())
}

#[test]
fn iids_match_existing_arrow_golden_documents() {
    for (id, expected) in [
        (EntityId::String("foo".into()), "d9c7fae2a04e047164936265ba33cf80"),
        (EntityId::String("bar".into()), "9e3f856e68998313827ff18dd4d88e78"),
        (EntityId::String("baz".into()), "47a6245d9effb01c6b67db10b5d9aaa8"),
        (EntityId::Integer(5), "0016cf5ed68e5a5349722594ae8f5926"),
        (EntityId::Integer(28), "001e832ee50bb5bd3dab482eb130abf1"),
        (EntityId::Keyword(Keyword::parse(":solo").unwrap()), "e5c907a6fac1f49cb3f8599a43a7cbf9"),
        (EntityId::uuid("424f5622-c826-4ded-a5db-e2144d665c38").unwrap(), "424f5622c8264deda5dbe2144d665c38"),
    ] {
        assert_eq!(id.iid().to_string(), expected, "{id:?}");
        assert_eq!(expected.parse::<Iid>().unwrap(), id.iid());
    }
}

#[test]
fn signed_integer_width_does_not_change_identity() {
    let ids = [Value::I8(-1), Value::I16(-1), Value::I32(-1), Value::I64(-1)];
    let iid = ids[0].entity_id().unwrap().iid();
    for value in ids { assert_eq!(value.entity_id().unwrap().iid(), iid); }
    assert_ne!(iid, EntityId::String("-1".into()).iid());
    assert!(Value::U64(1).entity_id().is_err());
    assert!(Value::F64(1.0).entity_id().is_err());
    assert!(Value::Null.entity_id().is_err());
}

#[test]
fn logical_join_retains_scalar_legs_instead_of_promoting_numbers() {
    assert_eq!(T::join_all(std::iter::empty()), T::Nothing);
    assert_eq!(scalar(S::Utf8).join(&scalar(S::Utf8)), scalar(S::Utf8));
    assert_eq!(scalar(S::I32).join(&scalar(S::I64)), T::from_legs([scalar(S::I32), scalar(S::I64)]));
    assert_eq!(T::least_upper_bound(&[scalar(S::I32), scalar(S::I64)]), Some(scalar(S::I64)));
    assert_eq!(T::Nothing.join(&scalar(S::I64)), scalar(S::I64));
    assert_eq!(T::Nothing.join(&T::Nothing), T::Nothing);
    assert_eq!(T::Nothing.maybe(), T::Null);
    assert_eq!(T::from_legs([]), T::Null);
    assert_eq!(T::Null.join(&scalar(S::I64)), scalar(S::I64).maybe());
}

#[test]
fn list_and_set_element_types_join_without_collapsing_collection_kinds() {
    let utf8 = T::list(scalar(S::Utf8));
    let integer = T::list(scalar(S::I64));
    assert_eq!(utf8.join(&integer), T::list(T::from_legs([scalar(S::Utf8), scalar(S::I64)])));
    assert_eq!(T::list(T::Null).join(&integer), T::list(scalar(S::I64).maybe()));
    assert_eq!(T::list(T::Nothing).join(&T::list(T::Nothing)), T::list(T::Nothing));
    let set = T::List { kind: ListKind::Set, element: Box::new(scalar(S::I64)) };
    assert_eq!(set.join(&integer), T::from_legs([set.clone(), integer]));
}

#[test]
fn missing_struct_children_contribute_null_and_nested_structs_stay_nested() {
    let left = structure(&[("a", scalar(S::Utf8)), ("b", scalar(S::Utf8))]);
    let right = structure(&[("b", scalar(S::Utf8)), ("c", scalar(S::I64))]);
    assert_eq!(left.join(&right), structure(&[
        ("a", scalar(S::Utf8).maybe()), ("b", scalar(S::Utf8)), ("c", scalar(S::I64).maybe()),
    ]));
    let nested = structure(&[("foo", structure(&[("bibble", scalar(S::Bool))]))]);
    let flat = structure(&[("foo", scalar(S::Utf8)), ("bar", scalar(S::I64))]);
    assert_eq!(nested.join(&flat), structure(&[
        ("foo", T::from_legs([scalar(S::Utf8), structure(&[("bibble", scalar(S::Bool))])])),
        ("bar", scalar(S::I64).maybe()),
    ]));
}

#[test]
fn recursive_struct_joins_preserve_nested_nullability() {
    let mut left = structure(&[("a", scalar(S::I64))]);
    let mut right = structure(&[("b", scalar(S::Utf8))]);
    let mut expected = structure(&[("a", scalar(S::I64).maybe()), ("b", scalar(S::Utf8).maybe())]);
    for _ in 0..16 {
        left = structure(&[("nested", T::list(left))]);
        right = structure(&[("nested", T::list(right))]);
        expected = structure(&[("nested", T::list(expected))]);
    }
    assert_eq!(T::join_all([&left, &right].into_iter()), expected);
}

#[test]
fn joining_encoded_and_plain_values_removes_physical_encoding_recursively() {
    let encoded = scalar(S::I64).run_end_encoded(RunEndWidth::I32).unwrap();
    assert_ne!(encoded, scalar(S::I64));
    assert_eq!(encoded.join(&scalar(S::I64)), scalar(S::I64));
    assert_eq!(structure(&[("x", encoded)]).unencoded(), structure(&[("x", scalar(S::I64))]));
    assert!(T::Nothing.run_end_encoded(RunEndWidth::I32).is_err());
    assert!(T::Null.run_end_encoded(RunEndWidth::I32).is_err());
}

#[test]
fn replacing_outer_run_end_width_retains_nested_layouts_and_nullability() {
    let child = scalar(S::I64).maybe().run_end_encoded(RunEndWidth::I16).unwrap();
    for plain in [T::list(child.clone()), structure(&[("x", child.clone())]).maybe()] {
        let encoded = plain.clone().run_end_encoded(RunEndWidth::I32).unwrap();
        let replaced = encoded.run_end_encoded(RunEndWidth::I64).unwrap();
        assert_eq!(replaced, T::RunEndEncoded { width: RunEndWidth::I64, value: Box::new(plain.clone()) });
        replaced.validate().unwrap();
        assert_eq!(replaced.unencoded(), plain.unencoded());
    }
}

#[test]
fn absent_contribution_depends_on_whether_a_segment_contains_puts() {
    assert_eq!(T::absent_contribution(false), T::Nothing);
    assert_eq!(T::absent_contribution(true), T::Null);
    assert_eq!(Value::list(vec![]).logical_type(), T::list(T::Nothing));
}

#[test]
fn temporal_intervals_are_half_open_and_epoch_parts_use_floor_division() {
    let interval = Interval::from_micros(10, 20).unwrap();
    assert!(interval.contains(Timestamp(10)));
    assert!(!interval.contains(Timestamp(20)));
    assert!(!interval.intersects(Interval::from_micros(20, 30).unwrap()));
    assert!(!interval.intersects(Interval::from_micros(15, 15).unwrap()));
    assert!(Interval::from_micros(20, 10).is_err());
    assert_eq!(Timestamp(-1).epoch_parts(), (-1, 999_999_000));
    assert_eq!(Timestamp::from_epoch_parts(-1, 999_999_000).unwrap(), Timestamp(-1));
    assert_eq!(Timestamp::from_epoch_parts(i64::MIN, 0).unwrap_err().code, "xtdb/time-overflow");
    assert!(Interval::at(Timestamp::MAX).is_err());
}

#[test]
fn table_ref_parser_preserves_the_reference_name_contract() {
    assert_eq!(TableRef::parse("docs").unwrap(), TableRef::new("public", "docs"));
    assert_eq!(TableRef::parse("sales.docs").unwrap().schema_and_table(), "sales/docs");
    for invalid in ["", ".docs", "public.", "public.docs.extra", "public/docs"] {
        assert_eq!(TableRef::parse(invalid).unwrap_err().code, "xtdb.table/invalid-table-ref");
    }
}

#[test]
fn await_basis_merge_keeps_each_database_and_partition() {
    let left = TxBasis { databases: BTreeMap::from([("a".into(), vec![TxId(2), TxId(9)])]) };
    let right = TxBasis { databases: BTreeMap::from([
        ("a".into(), vec![TxId(4), TxId(7)]), ("b".into(), vec![TxId(1)]),
    ]) };
    let merged = left.merge(&right).unwrap();
    assert_eq!(merged.databases["a"], vec![TxId(4), TxId(9)]);
    assert_eq!(merged.databases["b"], vec![TxId(1)]);
    assert!(left.merge(&TxBasis { databases: BTreeMap::from([("a".into(), vec![TxId(1)])]) }).is_err());
}

#[test]
fn message_ids_preserve_epoch_offsets_and_before_first_sentinels() {
    for epoch in [0, 3, 32767] {
        let position = SourcePosition::from_epoch_offset(StorageEpoch(epoch), 123).unwrap();
        assert_eq!(position.epoch(), StorageEpoch(epoch));
        assert_eq!(position.offset(), 123);
        assert_eq!(position.after_offset(StorageEpoch(epoch)), 123);
    }
    assert_eq!(SourcePosition::from_epoch_offset(StorageEpoch(0), 123).unwrap().0, 123);
    assert_eq!(ReplicaPosition::from_epoch_offset(StorageEpoch(1), -1).unwrap().0, (1_i64 << 48) - 1);
    assert_eq!(SourcePosition(-1).after_offset(StorageEpoch(0)), -1);
    assert_eq!(SourcePosition::from_epoch_offset(StorageEpoch(3), 123).unwrap().after_offset(StorageEpoch(4)), -1);
    assert!(SourcePosition::from_epoch_offset(StorageEpoch(0), 1_i64 << 48).is_err());
}

#[test]
fn message_ids_reject_epochs_that_cannot_preserve_signed_order() {
    let maximum = StorageEpoch(32767);
    let last_offset = (1_i64 << 48) - 1;
    let last = ReplicaPosition::from_epoch_offset(maximum, last_offset).unwrap();
    let prior = ReplicaPosition::from_epoch_offset(StorageEpoch(maximum.0 - 1), last_offset).unwrap();
    assert_eq!(last.0, i64::MAX);
    assert!(prior < last);
    assert_eq!(last.after_offset(maximum), last_offset);
    for epoch in [i32::MIN, -1, 32768, 65535, 65536, i32::MAX] {
        for offset in [-1, 0, last_offset] {
            assert_eq!(SourcePosition::from_epoch_offset(StorageEpoch(epoch), offset).unwrap_err().code, "xtdb/invalid-log-epoch");
            assert_eq!(ReplicaPosition::from_epoch_offset(StorageEpoch(epoch), offset).unwrap_err().code, "xtdb/invalid-log-epoch");
        }
    }
}

#[test]
fn fixture_values_preserve_non_finite_float_bits_and_signed_zero() {
    for bits in [0_u64, 1_u64 << 63, f64::INFINITY.to_bits(), f64::NEG_INFINITY.to_bits(), 0x7ff8_0000_0000_0123] {
        let value = Value::F64(f64::from_bits(bits));
        let encoded = serde_json::to_vec(&value).unwrap();
        let decoded: Value = serde_json::from_slice(&encoded).unwrap();
        let Value::F64(decoded) = decoded else { panic!("float64 fixture lost its type"); };
        assert_eq!(decoded.to_bits(), bits);
    }
}

#[test]
fn raw_keyword_names_roundtrip_without_reparsing_their_spelling() {
    for name in ["", ":prefixed", "space inside", "ns/name"] {
        let keyword = Keyword::from_name(name);
        assert_eq!(keyword.as_str(), name);
        let decoded: Keyword = serde_json::from_str(&serde_json::to_string(&keyword).unwrap()).unwrap();
        assert_eq!(decoded, keyword);
    }
}

#[test]
fn unsigned_integers_promote_with_floats_but_not_other_integer_types() {
    assert_eq!(S::U64.promote(&S::F64), Some(S::F64));
    assert_eq!(S::F32.promote(&S::U32), Some(S::F32));
    assert_eq!(S::U64.promote(&S::I64), None);
}

#[test]
fn deeply_nested_union_types_unencode_and_validate_in_linear_time() {
    let mut ty = scalar(S::I64);
    for depth in 0..64 {
        let other = if depth % 2 == 0 { scalar(S::Utf8) } else { T::Struct(StructFields::from([("v".into(), scalar(S::Bool))])) };
        ty = T::list(T::from_legs([ty, other, T::Null]));
    }
    let started = std::time::Instant::now();
    assert_eq!(ty.unencoded(), ty);
    assert_eq!(ty.clone().maybe().unencoded(), ty.clone().maybe());
    ty.validate().unwrap();
    assert!(started.elapsed() < std::time::Duration::from_secs(5), "took {:?}", started.elapsed());
}

#[test]
fn joins_keep_legs_and_struct_fields_in_first_seen_order() {
    let instant = scalar(S::Timestamp { unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) });
    let joined = T::join_all(&[scalar(S::Utf8), scalar(S::I64), scalar(S::Keyword)]);
    let T::Union(legs) = &joined else { panic!("expected a union: {joined:?}") };
    assert_eq!(legs.iter().cloned().collect::<Vec<_>>(), [scalar(S::Utf8), scalar(S::I64), scalar(S::Keyword)]);
    assert_eq!(joined, T::join_all(&[scalar(S::I64), scalar(S::Keyword), scalar(S::Utf8)]), "legs compare as a set");

    let list = |element| T::List { kind: ListKind::List, element: Box::new(element) };
    let lists = T::join(&list(scalar(S::F64).join(&scalar(S::Utf8))), &list(instant.join(&scalar(S::Bool))));
    let T::List { element, .. } = &lists else { panic!("expected a list: {lists:?}") };
    let T::Union(elements) = element.as_ref() else { panic!("expected union elements") };
    assert_eq!(elements.iter().cloned().collect::<Vec<_>>(), [scalar(S::F64), scalar(S::Utf8), instant, scalar(S::Bool)]);

    let merged = T::join(&structure(&[("b", scalar(S::I64)), ("a", scalar(S::Utf8))]), &structure(&[("c", scalar(S::Bool)), ("a", scalar(S::Utf8))]));
    let T::Struct(fields) = &merged else { panic!("expected a struct: {merged:?}") };
    assert_eq!(fields.keys().map(String::as_str).collect::<Vec<_>>(), ["b", "a", "c"]);
}

#[test]
fn struct_values_keep_document_order_and_compare_as_maps() {
    let document = Value::Struct(StructFields::from([("id".into(), Value::I64(1)), ("a".into(), Value::I64(2))]));
    let T::Struct(fields) = document.logical_type() else { panic!("expected a struct type") };
    assert_eq!(fields.keys().map(String::as_str).collect::<Vec<_>>(), ["id", "a"]);
    assert_eq!(document, Value::Struct(StructFields::from([("a".into(), Value::I64(2)), ("id".into(), Value::I64(1))])));
}
