use rtdb_plan::{Expr,function_type};
use rtdb_types::{ListKind,LogicalType as L,ScalarType as S,Value as V};

#[test]
fn nth_requires_signed_indexes_for_every_nonnull_source_leg() {
    let list = L::list(L::Scalar(S::Utf8));
    for index in [S::I8,S::I16,S::I32,S::I64] {
        assert_eq!(function_type("nth", &[list.clone(),L::Scalar(index)]).unwrap(),L::Scalar(S::Utf8).maybe());
    }
    for index in [S::Bool,S::U8,S::U16,S::U32,S::U64,S::F32,S::F64,S::Utf8,S::Decimal { precision: 10,scale: 0,bit_width: 128 }] {
        for source in [list.clone(),L::Scalar(S::I64)] {
            assert_eq!(function_type("nth", &[source,L::Scalar(index.clone())]).unwrap_err().code,"xtdb.expression/function-type-mismatch");
        }
        assert_eq!(function_type("nth", &[L::Null,L::Scalar(index)]).unwrap(),L::Null);
    }
    assert_eq!(function_type("nth", &[list.clone(),L::Null]).unwrap(),L::Null);
    assert!(function_type("nth", &[list,L::Scalar(S::F64).maybe()]).is_err());
}

#[test]
fn nth_ordinary_lists_have_element_types_and_other_registered_kinds_are_null() {
    for source in [L::Scalar(S::I64),L::Struct(Default::default()),L::List { kind: ListKind::Set,element: Box::new(L::Scalar(S::Utf8)) },L::List { kind: ListKind::Map { sorted: false },element: Box::new(L::Scalar(S::Utf8)) }] {
        assert_eq!(function_type("nth", &[source,L::Scalar(S::I64)]).unwrap(),L::Null);
    }
    let fixed = L::List { kind: ListKind::FixedSizeList(1),element: Box::new(L::Scalar(S::Utf8)) };
    assert!(function_type("nth", &[fixed.clone(),L::Scalar(S::I64)]).is_err());
    assert_eq!(function_type("nth", &[fixed,L::Null]).unwrap(),L::Null);
    let subscript = Expr::Subscript { expr: Box::new(Expr::Literal(V::I64(1))),index: Box::new(Expr::Literal(V::I64(0))),data_type: L::Scalar(S::Utf8).maybe() };
    assert_eq!(subscript.data_type(),L::Null);
    let invalid = Expr::Subscript { expr: Box::new(Expr::List(vec![])),index: Box::new(Expr::Literal(V::F64(0.0))),data_type: L::Null };
    assert_eq!(invalid.validate_columns(&Default::default()).unwrap_err().code,"xtdb.expression/function-type-mismatch");
}
