use std::ops::Range;

use rtdb_plan::CastTarget;
use rtdb_types::{AnomalyKind, ListValue, ListView, LogicalType, Result, ScalarType};

use super::{EvalValue, Kind, ListReader, PackedList, Target};
use crate::{evaluate::EvalContext, list_cast, scalar_list_cast};

pub(super) fn cast(
    input: &EvalValue<'_>, element: Target<'_>, union_admitted: bool, selection: Range<usize>,
    projection: bool, context: &EvalContext<'_>,
) -> Result<Option<PackedList>> {
    let EvalValue::List(Kind::List | Kind::Fixed, reader) = input else { return Ok(None); };
    let fixed;
    let target = match element {
        Target::Cast(target) => target,
        Target::Fixed(ty) => { fixed = CastTarget::fixed(ty.clone()); &fixed }
    };
    if target.union_target().is_some() {
        if union_admitted
            && let ListReader::Arrow { cell, .. } = reader.as_ref()
            && let Some(span) = cell.numeric_list_span_range(selection)
        {
            let per_component = 2 * std::mem::size_of::<rtdb_types::Value>() + 32;
            context.reserve(span.len().checked_mul(per_component).and_then(|bytes| bytes.checked_add(4096))
                .ok_or_else(crate::numeric::overflow)?)?;
            return Ok(rtdb_columnar::repack_numeric_list(&span, true).map(PackedList::from));
        }
        return Ok(None);
    }
    let reserve = |bytes| context.reserve(bytes);
    let stored;
    let source = match reader.as_ref() {
        ListReader::Stored(list) if selection.end <= list.len() => {
            stored = list.slice_packed(selection.start, selection.len());
            let view = match &stored {
                Some(list) => list.view(),
                None => ListView::Values(&list.scalar_values().expect("unpacked list")[selection.clone()]),
            };
            if let Some((field, values)) = stored.as_ref().and_then(ListValue::packed_column)
                && let Some(list) = crate::float_list_cast::cast(&field, values.as_ref(), target, reserve)?
            {
                return Ok(Some(list.into()));
            }
            if view.is_packed_numeric()
                && let Some(list) = list_cast::numeric(list_cast::Input::Stored(view), target, projection, reserve)?
            {
                return Ok(Some(list.into()));
            }
            scalar_list_cast::Source::Stored(view)
        }
        ListReader::Arrow { cell, backing } => {
            let Some((field, array, range)) = cell.sequence_range() else { return Ok(None); };
            if selection.end > range.len() { return Ok(None); }
            if matches!(target, CastTarget::Fixed(LogicalType::Scalar(ScalarType::Binary)))
                && field.data_type() == &arrow_schema::DataType::Binary
                && field.metadata().is_empty()
            {
                let rows = (range.start + selection.start)..(range.start + selection.end);
                let output = rtdb_columnar::repack_scalar_list(field, array, rows.clone(), true, || {
                    let bound = rows.clone().try_fold(4096usize, |bytes, row| {
                        rtdb_columnar::estimate_copy_cell_bytes(field, array, row)?.checked_mul(4)
                            .and_then(|size| bytes.checked_add(size))
                            .ok_or_else(crate::numeric::overflow)
                    })?;
                    context.reserve(bound)
                });
                return match output {
                    Err(error) if error.is(AnomalyKind::ResourceExhausted) => Ok(None),
                    result => result.map(|list| list.map(PackedList::from)),
                };
            }
            if target_is_float(target) {
                let values = array.slice(range.start + selection.start, selection.len());
                if let Some((values, shared)) = crate::float_list_cast::cast_with_backing(
                    field, values.as_ref(), target, backing.map(|lease| lease.bytes()), reserve,
                )? {
                    let backing = backing.filter(|_| shared).cloned();
                    return Ok(Some(PackedList { values, backing }));
                }
            }
            if let Some(span) = cell.numeric_list_span_range(selection.clone())
                && let Some(list) = list_cast::numeric(list_cast::Input::Opened(span), target, projection, reserve)?
            {
                return Ok(Some(list.into()));
            }
            scalar_list_cast::Source::Arrow(*cell, selection)
        }
        _ => return Ok(None),
    };
    scalar_list_cast::cast_list(source, target, context.timezone, Some(context.current_time), projection, reserve)
        .map(|list| list.map(PackedList::from))
}

fn target_is_float(target: &CastTarget) -> bool {
    matches!(list_cast::scalar_target(target),
        Some(list_cast::Target::Scalar(rtdb_types::ScalarType::F32 | rtdb_types::ScalarType::F64)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use rtdb_columnar::ArrowCell;
    use rtdb_types::Value;

    use super::*;

    #[test]
    fn binary_identity_cast_gathers_only_reached_child_cells() {
        let element = LogicalType::Scalar(ScalarType::Binary).maybe();
        let ty = LogicalType::list(element.clone());
        let items = [Value::Binary(vec![0, 255]), Value::Null, Value::Binary(Vec::new())];
        let lists = rtdb_columnar::build_array(&ty, &[
            Value::list(vec![Value::Binary(vec![42])]),
            Value::list(items.to_vec()),
            Value::list(vec![Value::Binary(vec![43])]),
        ]).unwrap();
        let field = rtdb_columnar::to_field("items", &ty).unwrap();
        let input = EvalValue::List(Kind::List, Arc::new(ListReader::Arrow {
            cell: ArrowCell { array: lists.as_ref(), field: &field, row: 1 }, backing: None,
        }));
        let target = CastTarget::fixed(LogicalType::Scalar(ScalarType::Binary));
        let context = EvalContext::default();
        for projection in [false, true] {
            let list = cast(&input, Target::Cast(&target), false, 0..items.len(), projection, &context)
                .unwrap().unwrap().values;
            let (field, array) = list.packed_column().unwrap();
            assert_eq!(array.data_type(), &arrow_schema::DataType::Binary);
            assert!(field.is_nullable());
            assert_eq!(list.iter().map(|value| value.into_owned()).collect::<Vec<_>>(), items);
        }
        assert!(cast(&input, Target::Cast(&target), false, 1..2, true, &context).unwrap().is_none());
        assert!(cast(&input, Target::Cast(&target), false, 0..4, true, &context).unwrap().is_none());
        let memory = rtdb_columnar::MemoryBudget::new(4096);
        let scope = crate::EvaluationScope::new(&memory).unwrap();
        let limited = context.with_allocation(&scope);
        assert!(cast(&input, Target::Cast(&target), false, 0..items.len(), false, &limited)
            .unwrap().is_none());
        assert_eq!(memory.used_bytes(), 4096);
    }

    #[test]
    fn accepted_borrowed_union_list_cast_packs_only_the_selected_children() {
        let values = ListValue::from(vec![Value::F64(2.0), Value::F32(-0.0), Value::I64(7), Value::Null]);
        let (child_field, child) = values.packed_column().unwrap();
        let lists = rtdb_columnar::build_list_array(child_field, vec![0, 1, 4].into(), &child, None).unwrap();
        let field = Field::new("items", lists.data_type().clone(), false);
        let input = |row| EvalValue::List(Kind::List, Arc::new(ListReader::Arrow {
            cell: ArrowCell { array: lists.as_ref(), field: &field, row }, backing: None,
        }));
        let target = LogicalType::from_legs([LogicalType::Scalar(ScalarType::I64), LogicalType::Scalar(ScalarType::F32), LogicalType::Null]);
        let context = EvalContext::default();
        assert!(cast(&input(0), Target::Fixed(&target), false, 0..1, false, &context).unwrap().is_none());
        let actual = cast(&input(1), Target::Fixed(&target), true, 0..3, false, &context).unwrap().unwrap().values;
        assert!(actual.array().is_some());
        assert_eq!(actual.element_type(), target);
        assert!(matches!(actual.get(0).as_deref(), Some(Value::F32(value)) if value.to_bits() == (-0.0f32).to_bits()));
        assert_eq!(actual.get(1).as_deref(), Some(&Value::I64(7)));
        assert_eq!(actual.get(2).as_deref(), Some(&Value::Null));
    }
}
