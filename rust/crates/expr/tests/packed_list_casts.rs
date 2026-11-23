use std::sync::Arc;

use arrow_array::{ArrayRef, Float32Array, Float64Array, NullArray, UnionArray};
use arrow_schema::{DataType, Field, UnionFields};
use rtdb_expr::{EvalContext, evaluate};
use rtdb_plan::{CastTarget, Expr, Schema};
use rtdb_types::{ListValue, LogicalType as L, ScalarType as S, Value as V};

fn bits(value: &V) -> Option<u64> {
    match value {
        V::F32(value) => Some(u64::from(value.to_bits())),
        V::F64(value) => Some(value.to_bits()),
        V::Null => None,
        value => panic!("Unexpected component {value:?}"),
    }
}

fn assert_casts_like_its_components(list: V) {
    for target in [S::F32, S::F64] {
        let cast = Expr::Cast { expr: Box::new(Expr::Literal(list.clone())), target: CastTarget::List(Box::new(CastTarget::Fixed(L::Scalar(target.clone())))) };
        let whole = evaluate(&cast, &Schema::default(), &[], &EvalContext::default()).unwrap();
        let whole = whole.list_view().unwrap();
        let components = list.list_view().unwrap();
        assert_eq!(whole.len(), components.len());
        for (index, component) in components.iter().enumerate() {
            let expected = rtdb_expr::cast(component.as_ref(), &L::Scalar(target.clone()), "UTC").unwrap();
            assert_eq!(bits(whole.get(index).unwrap().as_ref()), bits(&expected), "{list:?} as {target:?} at {index}");
            assert!(matches!((whole.get(index).unwrap().as_ref(), &target), (V::Null, _) | (V::F32(_), S::F32) | (V::F64(_), S::F64)));
        }
    }
}

#[test]
fn packed_numeric_lists_cast_to_float_elements_with_the_scalar_casts_bits() {
    assert_casts_like_its_components(V::List(ListValue::from_f32(Float32Array::from(vec![Some(1.5), None, Some(f32::NAN), Some(-0.0), Some(f32::MAX), Some(1e-45)]))));
    assert_casts_like_its_components(V::FixedSizeList(ListValue::from_f64(Float64Array::from(vec![Some(0.1), Some(1e300), None, Some(-1e-320), Some(f64::NEG_INFINITY)]))));
    assert_casts_like_its_components(V::list(vec![V::I64(9_007_199_254_740_993), V::Null, V::I64(-16_777_217), V::I64(i64::MIN)]));
    assert_casts_like_its_components(V::list(vec![V::I8(-128), V::I16(32767), V::I32(16_777_217)]));
    assert_casts_like_its_components(V::list(vec![V::F16(half::f16::from_f32(65504.0).to_bits()), V::Null, V::F16(half::f16::from_f32(-0.0).to_bits())]));

    let fields = UnionFields::try_new(vec![0, 1, 2], vec![
        Field::new("f32", DataType::Float32, true), Field::new("f64", DataType::Float64, true), Field::new("null", DataType::Null, true),
    ]).unwrap();
    let union = UnionArray::try_new(fields, vec![0i8, 1, 2, 1, 0].into(), Some(vec![0i32, 0, 0, 1, 1].into()), vec![
        Arc::new(Float32Array::from(vec![Some(1.25), None])) as ArrayRef,
        Arc::new(Float64Array::from(vec![0.1, 3.4028235677973366e38])),
        Arc::new(NullArray::new(1)),
    ]).unwrap();
    assert_casts_like_its_components(V::List(ListValue::from_float_union(union).unwrap()));
}

#[test]
fn lists_without_a_present_component_cast_as_their_components_do() {
    for list in [V::List(ListValue::from_f32(Float32Array::from(vec![None, None]))), V::list(vec![])] {
        let cast = Expr::Cast { expr: Box::new(Expr::Literal(list.clone())), target: CastTarget::List(Box::new(CastTarget::Fixed(L::Scalar(S::F64)))) };
        let whole = evaluate(&cast, &Schema::default(), &[], &EvalContext::default()).unwrap();
        let components = list.list_view().unwrap().iter().map(|component| rtdb_expr::cast(component.as_ref(), &L::Scalar(S::F64), "UTC").unwrap()).collect::<Vec<_>>();
        assert_eq!(whole, V::list(components));
    }
}

#[test]
fn a_nullable_element_target_remaps_union_legs_rather_than_casting_components() {
    let list = V::List(ListValue::from_f64(Float64Array::from(vec![Some(1.5), None])));
    for target in [CastTarget::Fixed(L::Scalar(S::F32).maybe()), CastTarget::Checked { requested: L::Scalar(S::F32).maybe(), target: Box::new(CastTarget::Fixed(L::Scalar(S::F32).maybe())) }] {
        let cast = Expr::Cast { expr: Box::new(Expr::Literal(list.clone())), target: CastTarget::List(Box::new(target)) };
        let error = evaluate(&cast, &Schema::default(), &[], &EvalContext::default()).unwrap_err();
        assert_eq!(error.code, "xtdb/invalid-argument-types");
    }
}
