use rtdb_types::StructFields;
use rtdb_types::{LogicalType as L, PgType as P, RunEndWidth, ScalarType as S};

#[test]
fn information_schema_namespace_oids_match_pinned_test_pg_namespace() {
    for (name, oid) in [("xtdb", 1376455703), ("information_schema", 1980112537), ("pg_catalog", 2125819141), ("public", 1106696632), ("xt", 683075021)] {
        assert_eq!(rtdb_types::pg_name_oid(name), oid);
    }
    assert_eq!(rtdb_types::pg_table_oid(Some(42), &rtdb_types::TableRef::new("public", "docs")), 42);
}

#[test]
fn pgtype_values_distinguish_wire_aliases_and_unknown_oids() {
    for ty in P::ALL { assert_eq!(P::from_oid(ty.descriptor().oid), Some(*ty)); }
    assert_eq!(P::from_oid(0), Some(P::Default));
    assert_eq!(P::from_oid(25), Some(P::Text));
    assert_eq!(P::from_oid(114), Some(P::Json));
    assert_eq!(P::from_oid(99999), None);
    assert!(P::Transit.logical_type().is_none());
    assert_eq!(P::Null.logical_type(), Some(L::Null));
}

#[test]
fn pgtype_union_widening_matches_numeric_and_array_families() {
    assert_eq!(P::from_logical(&L::Scalar(S::I32).join(&L::Scalar(S::I64))).unwrap(), P::Int8);
    assert_eq!(P::from_logical(&L::Scalar(S::F32).join(&L::Scalar(S::F64))).unwrap(), P::Float8);
    assert_eq!(P::from_logical(&L::Scalar(S::I32).join(&L::Scalar(S::F64))).unwrap(), P::Default);
    assert_eq!(P::from_logical(&L::list(L::Scalar(S::I32)).join(&L::list(L::Scalar(S::I64)))).unwrap(), P::Default);
    assert_eq!(P::from_logical(&L::Scalar(S::I32).run_end_encoded(RunEndWidth::I16).unwrap()).unwrap(), P::Int4);
    assert!(P::from_logical(&L::Scalar(S::I8)).is_err());
}

#[test]
fn pgwire_bind_compatible_column_types_include_empty_arrays() {
    assert!(P::Default.accepts_result(&L::Scalar(S::I64)).unwrap());
    assert!(P::Int4.accepts_result(&L::Null).unwrap());
    assert!(P::Int4s.accepts_result(&L::list(L::Null)).unwrap());
    assert!(!P::Int4.accepts_result(&L::Scalar(S::I64)).unwrap());
}

#[test]
fn information_schema_format_type_modifiers() {
    assert_eq!(P::format_oid(0, -1), "-");
    assert_eq!(P::format_oid(99999, -1), "???");
    assert_eq!(P::VarChar.format_type(24), "character varying(20)");
    assert_eq!(P::Numeric.format_type((10 << 16) + 2 + 4), "numeric(10,2)");
    assert_eq!(P::TimestampTz.format_type(6), "timestamp(6) with time zone");
    assert_eq!(P::Int4s.data_type(), "ARRAY");
    assert_eq!(P::Keyword.data_type(), "USER-DEFINED");
}

#[test]
fn information_schema_type_edn_preserves_nullable_and_nested_layouts() {
    assert_eq!(L::list(L::Scalar(S::I64)).maybe().to_edn().unwrap(), "[:? :list :i64]");
    assert_eq!(L::Scalar(S::I32).run_end_encoded(RunEndWidth::I16).unwrap().to_edn().unwrap(), "[:ree-i16 :i32]");
    assert_eq!(L::Nothing.to_edn().unwrap(), ":nothing");
    assert_eq!(L::Struct(StructFields::from([("a".into(), L::Scalar(S::Utf8))])).to_edn().unwrap(), "[:struct {\"a\" :utf8}]");
}

#[test]
fn float_array_descriptors_and_result_inference_keep_each_width() {
    for (scalar, array, oid, pg_oid) in [(S::F32, P::Float4s, 700, 1021), (S::F64, P::Float8s, 701, 1022)] {
        assert_eq!(P::from_oid(pg_oid), Some(array));
        assert_eq!(P::from_oid(oid).unwrap().descriptor().array_oid, Some(pg_oid));
        assert_eq!(array.descriptor().element_oid, Some(oid));
        assert_eq!(array.logical_type(), Some(L::list(L::Scalar(scalar.clone()).maybe())));
        let fixed = L::List { kind: rtdb_types::ListKind::FixedSizeList(3), element: Box::new(L::Scalar(scalar).maybe()) };
        assert_eq!(P::from_logical(&fixed.maybe()).unwrap(), array);
    }
    let mixed = L::list(L::from_legs([L::Scalar(S::F32), L::Null, L::Scalar(S::F64)]));
    assert_eq!(P::from_logical(&mixed).unwrap(), P::Float8s);
}
