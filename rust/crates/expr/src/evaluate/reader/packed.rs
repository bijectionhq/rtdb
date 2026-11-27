use std::ops::Range;

use rtdb_plan::CastTarget;
use rtdb_types::{ListValue, ListView, Result};

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
    use rtdb_types::{LogicalType, ScalarType, Value};

    use super::*;

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
