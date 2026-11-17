use std::collections::BTreeMap;
use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext, EvaluationScope, equality_allocation_bound, evaluate, evaluate_async, function_type, scalar_call, sql_eq, strict_eq};
use rtdb_format::transit::{TransitValue as T, encode_json, encode_msgpack};
use rtdb_plan::{BinaryOp, Expr, Schema};
use rtdb_types::Value as V;

fn tag(name: &str, value: T) -> T { T::Tagged { tag: name.into(), representation: Box::new(value) } }
fn text(value: &str) -> T { T::String(value.into()) }
fn stored(value: &T, json: bool) -> V { V::Transit(if json { encode_json(value).unwrap() } else { encode_msgpack(value).unwrap() }) }
fn call(name: &str, left: V, right: V) -> Expr {
    let args = vec![Expr::Literal(left), Expr::Literal(right)];
    let return_type = function_type(name, &args.iter().map(Expr::data_type).collect::<Vec<_>>()).unwrap();
    Expr::Call { name: name.into(), args, return_type }
}

#[tokio::test]
async fn transit_equality_compares_registered_objects_across_wire_encodings() {
    let cases = [
        (tag("i8", T::Integer(7)), T::Integer(7)),
        (tag("u", text("1-1-1-1-1")), tag("u", T::Array(vec![T::Integer(0x0000000100010001), T::Integer(0x0001000000000001)]))),
        (tag("r", text("HTTP://Example.com/%aa")), tag("r", text("http://example.COM/%AA"))),
        (tag("list", T::Array(vec![T::Integer(7), T::Null])), T::Array(vec![T::Integer(7), T::Null])),
        (T::Map(vec![(text("a"), T::Integer(1)), (text("b"), T::Integer(2))]), T::Map(vec![(text("b"), T::Integer(2)), (text("a"), T::Integer(1))])),
        (tag("set", T::Array(vec![T::Integer(2), T::Integer(1), T::Integer(1)])), tag("set", T::Array(vec![T::Integer(1), T::Integer(2)]))),
    ];
    for (left, right) in cases {
        for (left_json, right_json) in [(false, false), (false, true), (true, false), (true, true)] {
            let (left, right) = (stored(&left, left_json), stored(&right, right_json));
            assert_eq!(sql_eq(&left, &right).unwrap(), Some(true));
            assert!(strict_eq(&left, &right).unwrap());
            for expression in [
                Expr::binary(BinaryOp::Eq, Expr::Literal(left.clone()), Expr::Literal(right.clone())),
                call("===", left.clone(), right.clone()),
                call("null_eq", left.clone(), right.clone()),
            ] {
                assert_eq!(evaluate(&expression, &Schema::default(), &[], &EvalContext::default()).unwrap(), V::Bool(true));
                assert_eq!(evaluate_async(&expression, &Schema::default(), &[], &EvalContext::default()).await.unwrap(), V::Bool(true));
            }
        }
    }
}

#[test]
fn separately_read_identity_objects_are_unequal_even_with_identical_bytes() {
    for object in [
        T::Bytes(vec![1, 2]),
        tag("xtdb/exception-info", T::Array(vec![text("failure"), T::Map(vec![])])),
        tag("link", T::Map(vec![])),
    ] {
        let value = stored(&object, false);
        assert_eq!(sql_eq(&value, &value).unwrap(), Some(false));
        assert!(!strict_eq(&value, &value).unwrap());
    }
    let value = stored(&tag("xtdb/byte-array", text("0x0102")), false);
    assert_eq!(sql_eq(&value, &value).unwrap(), Some(true));
}

#[test]
fn transit_children_use_decoded_equality_inside_native_lists_and_structs() {
    let left = stored(&tag("i8", T::Integer(7)), false);
    let right = stored(&T::Integer(7), true);
    for (left, right) in [
        (V::List(vec![left.clone()]), V::List(vec![right.clone()])),
        (V::Struct(BTreeMap::from([("value".into(), left)])), V::Struct(BTreeMap::from([("value".into(), right)]))),
    ] {
        assert!(equality_allocation_bound(&left, &right).unwrap() > 0);
        assert_eq!(sql_eq(&left, &right).unwrap(), Some(true));
        assert!(strict_eq(&left, &right).unwrap());
    }
}

#[tokio::test]
async fn reached_transit_comparisons_construct_the_complete_registered_graph() {
    let malformed = stored(&T::Map(vec![(text("valid"), T::Integer(1)), (text("invalid"), tag("i8", T::Integer(128)))]), false);
    assert!(sql_eq(&malformed, &malformed).is_err());
    assert!(strict_eq(&malformed, &malformed).is_err());
    for expression in [
        Expr::binary(BinaryOp::Eq, Expr::Literal(malformed.clone()), Expr::Literal(malformed.clone())),
        call("===", malformed.clone(), malformed.clone()),
    ] {
        assert!(evaluate(&expression, &Schema::default(), &[], &EvalContext::default()).is_err());
        assert!(evaluate_async(&expression, &Schema::default(), &[], &EvalContext::default()).await.is_err());
    }
    let expression = Expr::binary(BinaryOp::Eq, Expr::Literal(malformed), Expr::Literal(V::Null));
    assert_eq!(evaluate(&expression, &Schema::default(), &[], &EvalContext::default()).unwrap(), V::Null);
    assert_eq!(evaluate_async(&expression, &Schema::default(), &[], &EvalContext::default()).await.unwrap(), V::Null);
}

#[test]
fn scalar_transit_comparisons_admit_decode_scratch_before_parsing() {
    let value = V::Transit(vec![0; 2048]);
    for name in ["===", "null_eq", "nullif", "distinct_from", "set"] {
        let memory = MemoryBudget::new(1 << 20);
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope), ..Default::default() };
        let failure = scalar_call(name, &[value.clone(), value.clone()], &context).unwrap_err();
        assert_eq!(failure.code, "rtdb/memory-budget-exceeded", "{name}");
        drop(scope);
        assert_eq!(memory.used_bytes(), 0);
    }
}
