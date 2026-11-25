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
fn same_width_float_cast_retains_only_a_proportionate_funded_slice() {
    use arrow_array::{RecordBatch, cast::AsArray, types::Float32Type};
    use arrow_schema::Schema as ArrowSchema;
    use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation, OwnedBatch};
    use rtdb_expr::{EvaluationScope, ParameterLookup, Parameters};

    struct Bound(OwnedBatch);
    impl ParameterLookup for Bound {
        fn width(&self) -> usize { 1 }
        fn value(&self, _: usize) -> rtdb_types::Result<Option<&V>> { panic!("Arrow list should stay packed") }
        fn known_value(&self, _: usize) -> Option<&V> { None }
        fn arrow_cell(&self, _: usize) -> Option<ArrowCell<'_>> {
            ArrowCell::named(self.0.batch(), "xs", 0)
        }
        fn arrow_backing(&self, _: usize) -> Option<&Arc<MemoryReservation>> {
            self.0.buffer_backing()
        }
    }

    let element = L::Scalar(S::F32);
    let cast = Expr::Cast {
        expr: Box::new(Expr::Parameter { index: 0, data_type: L::list(element.clone().maybe()) }),
        target: CastTarget::List(Box::new(CastTarget::fixed(element))),
    };
    let nan = f32::from_bits(0x7f80_0100);
    for (prefix, certified, shares) in [(3, true, true), (8193, true, false), (3, false, false)] {
        let mut components = vec![Some(5.0); prefix];
        components.extend([Some(-0.0), None, Some(nan)]);
        let child: ArrayRef = Arc::new(Float32Array::from(components));
        let item = Arc::new(Field::new("$data$", DataType::Float32, true));
        let offsets = vec![prefix as i32, prefix as i32 + 3].into();
        let list = rtdb_columnar::build_list_array(item, offsets, &child, None).unwrap();
        let field = Field::new("xs", list.data_type().clone(), false);
        let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(vec![field])), vec![list]).unwrap();
        let source = MemoryBudget::new(1 << 20);
        let bound = Bound(if certified {
            let grant = source.reserve(batch.get_array_memory_size()).unwrap();
            OwnedBatch::with_reservation(batch, grant).unwrap()
        } else {
            OwnedBatch::new(batch, &source).unwrap()
        });
        let lease = Arc::downgrade(bound.0.reservation());
        let ptr = bound.0.batch().column(0).as_list::<i32>().values()
            .as_primitive::<Float32Type>().values().as_ptr().wrapping_add(prefix);
        let scope = EvaluationScope::new(&MemoryBudget::new(1 << 20)).unwrap();
        let output = {
            let context = EvalContext { parameters: Parameters::Lookup(&bound), allocation: Some(&scope), ..Default::default() };
            evaluate(&cast, &Schema::default(), &[], &context).unwrap()
        };
        drop(bound);
        let V::List(output) = output else { panic!("expected list"); };
        let values = output.array().unwrap().as_primitive::<Float32Type>();
        assert_eq!(values.values().as_ptr() == ptr, shares);
        assert_eq!(lease.upgrade().is_some(), shares);
        assert_eq!(values.iter().map(|value| value.map(f32::to_bits)).collect::<Vec<_>>(),
            [Some((-0.0f32).to_bits()), None, Some(nan.to_bits())]);
        drop(output);
        assert!(lease.upgrade().is_none());
    }
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

#[test]
fn numeric_lists_use_scalar_integer_and_decimal_rounding() {
    let decimal = V::Decimal(rtdb_types::DecimalValue::new(125_i128.to_be_bytes().to_vec(), 5, 2, 128).unwrap());
    let source = V::list(vec![V::I64(42), V::F32(-1.25), V::Null, decimal]);
    for ty in [S::I16, S::Decimal { precision: 10, scale: 1, bit_width: 128 }] {
        let element = CastTarget::Fixed(L::Scalar(ty));
        let expected = V::list(source.list_view().unwrap().iter()
            .map(|value| rtdb_expr::cast_target(&value, &element, "UTC").unwrap()).collect());
        let target = CastTarget::List(Box::new(element));
        let expr = Expr::Cast { expr: Box::new(Expr::Literal(source.clone())), target: target.clone() };
        let output = evaluate(&expr, &Schema::default(), &[], &EvalContext::default()).unwrap();
        assert_eq!(output, expected);
        assert!(output.list_view().unwrap().is_packed_numeric());
        assert_eq!(rtdb_expr::cast_target(&source, &target, "UTC").unwrap(), expected);
    }
}

#[test]
fn packed_text_and_temporal_casts_match_scalar_cells_and_arrow_layout() {
    use rtdb_types::TimeUnit;
    for (source, target) in [
        (V::list(vec![V::F32(-0.0), V::Null, V::F64(f64::INFINITY)]), CastTarget::Fixed(L::Scalar(S::Utf8))),
        (V::list(vec![V::Utf8("1969-12-31 23:59:59.9999999".into()), V::Null, V::Utf8("2000-01-01 04:05:06".into())]),
            CastTarget::Temporal(rtdb_plan::TemporalCast { kind: rtdb_plan::TemporalKind::TimestampLocal, unit: TimeUnit::Microsecond, precision: Some(3) })),
        (V::list(vec![V::Utf8("2000-02-29".into()), V::Null]), CastTarget::Fixed(L::Scalar(S::DateDay))),
        (V::list(vec![V::F64(-0.25), V::Null]), CastTarget::Fixed(L::Scalar(S::Duration(TimeUnit::Millisecond)))),
    ] {
        let expected_scalar = V::list(source.list_view().unwrap().iter()
            .map(|value| rtdb_expr::cast_target(&value, &target, "UTC").unwrap()).collect());
        let mut expected = expected_scalar.clone();
        expected.admit_expression_dates().unwrap();
        let target = CastTarget::List(Box::new(target));
        let expr = Expr::Cast { expr: Box::new(Expr::Literal(source.clone())), target: target.clone() };
        let output = evaluate(&expr, &Schema::default(), &[], &EvalContext::default()).unwrap();
        let Some(rtdb_types::ListView::Scalars(_)) = output.list_view() else { panic!("expected packed scalar result"); };
        assert_eq!(output, expected);
        let ty = expected.logical_type().maybe();
        let actual = rtdb_columnar::copy_array(&ty, &[V::Null, output]).unwrap();
        let expected = rtdb_columnar::copy_array(&ty, &[V::Null, expected]).unwrap();
        assert_eq!(actual.to_data(), expected.to_data());
        let direct = rtdb_expr::cast_target(&source, &target, "UTC").unwrap();
        assert_eq!(direct, expected_scalar);
        assert!(matches!(direct.list_view(), Some(rtdb_types::ListView::Scalars(_))));
    }
}

#[test]
fn date_list_cast_keeps_wide_calendar_values_until_expression_admission() {
    let date = rtdb_types::CivilDate::from_epoch_day(i64::from(i32::MAX) + 1).unwrap();
    let input = V::list(vec![V::Utf8(date.to_string())]);
    let target = CastTarget::List(Box::new(CastTarget::Fixed(L::Scalar(S::DateDay))));
    let value = rtdb_expr::cast_target(&input, &target, "UTC").unwrap();
    assert_eq!(value, V::list(vec![V::calendar_date(date, rtdb_types::DateUnit::Day)]));
    assert!(value.has_calendar_object());

    let expr = Expr::Cast { expr: Box::new(Expr::Literal(input)), target };
    let error = evaluate(&expr, &Schema::default(), &[], &EvalContext::default()).unwrap_err();
    assert_eq!(error.code, "xtdb.expression/overflow-error");
}

#[test]
fn typed_list_casts_report_the_first_scalar_error() {
    let target = CastTarget::Fixed(L::Scalar(S::I16));
    let values = [V::Utf8("99999".into()), V::Utf8("bogus".into())];
    let expected = rtdb_expr::cast_target(&values[0], &target, "UTC").unwrap_err();
    let list_target = CastTarget::List(Box::new(target));
    let list = V::list(values.into());
    let actual = rtdb_expr::cast_target(&list, &list_target, "UTC").unwrap_err();
    assert_eq!((actual.code, actual.message), (expected.code, expected.message));
}

#[tokio::test]
async fn list_cursor_emits_packed_cast_pages_before_later_element_errors() {
    let input = V::list(vec![V::Utf8("2000-02-29".into()), V::Null, V::Utf8("invalid".into())]);
    let expr = Expr::Cast {
        expr: Box::new(Expr::Literal(input)),
        target: CastTarget::List(Box::new(CastTarget::Fixed(L::Scalar(S::DateDay)))),
    };
    let schema = Schema::default();
    let memory = rtdb_types::MemoryBudget::new(8 << 20);
    let scope = rtdb_expr::EvaluationScope::new(&memory).unwrap();
    let context = EvalContext { allocation: Some(&scope), ..Default::default() };
    let invocation = rtdb_expr::ExpressionInvocation::prepare(&expr, &schema, &context).unwrap();
    let mut pages = invocation.open_list(&[], &context, 2).await.unwrap();
    let first = pages.next_batch().await.unwrap().unwrap();
    assert!(matches!(first.values().view(), rtdb_types::ListView::Scalars(_)));
    assert_eq!(first.values().iter().map(std::borrow::Cow::into_owned).collect::<Vec<_>>(), [V::DateDay(11016), V::Null]);
    let error = pages.next_batch().await.err().expect("later page contains an invalid date");
    assert_eq!(error.code, "xtdb.expression/invalid-temporal-string");
}

#[test]
fn mixed_text_and_numeric_casts_write_one_typed_child() {
    let inputs = [V::Utf8("12".into()), V::Null, V::I64(-23), V::F32(4.75)];
    let source = V::list(inputs.to_vec());
    for ty in [S::I16, S::F64, S::Utf8] {
        let element = CastTarget::Fixed(L::Scalar(ty));
        let expected = V::list(inputs.iter().map(|value| rtdb_expr::cast_target(value, &element, "UTC").unwrap()).collect());
        let expr = Expr::Cast { expr: Box::new(Expr::Literal(source.clone())), target: CastTarget::List(Box::new(element)) };
        let output = evaluate(&expr, &Schema::default(), &[], &EvalContext::default()).unwrap();
        let V::List(list) = &output else { panic!("expected list"); };
        assert!(list.packed_column().is_some());
        assert_eq!(output, expected);
    }
}

#[test]
fn time_list_cast_uses_the_query_clock_and_keeps_the_result_in_arrow() {
    use rtdb_plan::{CastTimezone, TemporalCast, TemporalKind};
    use rtdb_types::{Instant, TimeUnit};
    let input = [
        V::Time { value: 9_000_123, unit: TimeUnit::Millisecond, bit_width: 32 },
        V::Null,
        V::DateDay(0),
        V::Utf8("2024-03-09 09:44:23-05:00".into()),
    ];
    let target = CastTarget::Temporal(TemporalCast {
        kind: TemporalKind::TimestampZone(CastTimezone::Session), unit: TimeUnit::Microsecond, precision: Some(4),
    });
    let context = EvalContext {
        timezone: "America/New_York", current_time: Instant::new(1_710_045_000, 0).unwrap(), ..EvalContext::default()
    };
    let schema = Schema::default();
    let scalar = |value| Expr::Cast { expr: Box::new(Expr::Literal(value)), target: target.clone() };
    let expected = V::list(input.iter().cloned().map(|value| evaluate(&scalar(value), &schema, &[], &context).unwrap()).collect());
    let list = Expr::Cast { expr: Box::new(Expr::Literal(V::list(input.to_vec()))), target: CastTarget::List(Box::new(target)) };
    let output = evaluate(&list, &schema, &[], &context).unwrap();
    assert!(matches!(output.list_view(), Some(rtdb_types::ListView::Scalars(_))));
    assert_eq!(output, expected);
}
