use rtdb_expr::{EvalContext, cast_target, evaluate};
use rtdb_plan::{CastTarget, Expr, Schema};
use rtdb_types::{DecimalValue, LogicalType as T, ScalarType as S, Value as V};

fn decimal(coefficient: i128, precision: i32, scale: i32) -> V {
    V::Decimal(DecimalValue::new(coefficient.to_be_bytes().to_vec(), precision, scale, 128).unwrap())
}

#[test]
fn unqualified_decimal_preserves_each_declared_union_leg_and_runtime_value() {
    let values = [decimal(124, 5, 2), decimal(12500, 10, 4), V::Null];
    let source = T::join_all(&values.iter().map(V::logical_type).collect::<Vec<_>>());
    let expression = Expr::Cast { expr: Box::new(Expr::Parameter { index: 0, data_type: source.clone() }), target: CastTarget::DecimalDefault };
    assert_eq!(expression.data_type(), source);
    for value in values {
        assert_eq!(evaluate(&expression, &Schema::default(), &[], &EvalContext { parameters: rtdb_expr::Parameters::Values(&[value.clone()]), ..Default::default() }).unwrap(), value);
    }
}

#[test]
fn decimal_cast_options_apply_recursively_even_when_declared_target_matches_input() {
    let inner = CastTarget::List(Box::new(CastTarget::DecimalDefault));
    let first = cast_target(&V::list(vec![V::F64(1.5), V::Null]), &inner, "UTC").unwrap();
    let target = CastTarget::List(Box::new(CastTarget::decimal(Some(10), Some(2)).unwrap()));
    assert_eq!(cast_target(&first, &target, "UTC").unwrap(), V::list(vec![decimal(150, 10, 2), V::Null]));
    let nested = CastTarget::List(Box::new(target));
    assert_eq!(cast_target(&V::list(vec![first, V::list(vec![])]), &nested, "UTC").unwrap(), V::list(vec![V::list(vec![decimal(150, 10, 2), V::Null]), V::list(vec![])]));
    assert_eq!(nested.result_type(&T::list(T::list(T::Scalar(S::F64).maybe()))), T::list(T::list(T::Scalar(S::Decimal { precision: 10, scale: 2, bit_width: 128 }).maybe())));
}

#[test]
fn scale_without_precision_is_ignored_and_negative_explicit_scale_rounds_half_even() {
    let value = decimal(125, 5, 2);
    assert_eq!(cast_target(&value, &CastTarget::decimal(None, Some(0)).unwrap(), "UTC").unwrap(), value);
    let target = CastTarget::decimal(Some(3), Some(-1)).unwrap();
    assert_eq!(cast_target(&V::I64(125), &target, "UTC").unwrap(), decimal(12, 3, -1));
    assert_eq!(cast_target(&V::I64(135), &target, "UTC").unwrap(), decimal(14, 3, -1));
}

#[test]
fn pg_array_text_cast_retains_nullable_elements_and_quoted_commas() {
    let integers = CastTarget::fixed(T::list(T::Scalar(S::I64)));
    for (input, expected) in [("{1,2,3}", vec![V::I64(1), V::I64(2), V::I64(3)]), ("{1,NULL}", vec![V::I64(1), V::Null]), ("{}", vec![])] {
        assert_eq!(cast_target(&V::Utf8(input.into()), &integers, "UTC").unwrap(), V::list(expected));
    }
    assert_eq!(integers.result_type(&T::Scalar(S::Utf8)), T::list(T::Scalar(S::I64).maybe()));
    let text = CastTarget::fixed(T::list(T::Scalar(S::Utf8)));
    assert_eq!(cast_target(&V::Utf8("{\"a,b\",c,\"NULL\",null}".into()), &text, "UTC").unwrap(), V::list(vec![V::Utf8("a,b".into()), V::Utf8("c".into()), V::Utf8("NULL".into()), V::Null]));
    assert_eq!(cast_target(&V::Utf8("abc".into()), &text, "UTC").unwrap_err().code, "xtdb.expression/malformed-array-literal");
    assert_eq!(cast_target(&V::Utf8("{{1,2},{3,4}}".into()), &integers, "UTC").unwrap_err().code, "xtdb.expression/multi-dimensional-array-literal");
}

#[test]
fn list_union_targets_widen_metadata_without_converting_existing_values() {
    let target_type = T::list(T::Scalar(S::I64).join(&T::Scalar(S::Utf8)).maybe());
    let target = CastTarget::fixed(target_type.clone());
    let value = V::list(vec![V::I64(1), V::Null]);
    assert_eq!(target.result_type(&value.logical_type()), target_type);
    assert_eq!(cast_target(&value, &target, "UTC").unwrap(), value);
    assert!(cast_target(&V::list(vec![V::F64(1.0)]), &target, "UTC").is_err());
}
