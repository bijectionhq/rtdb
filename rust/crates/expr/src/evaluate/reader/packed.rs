use std::ops::Range;

use rtdb_plan::CastTarget;
use rtdb_types::{ListValue, ListView, Result};

use super::{EvalValue, Kind, ListReader, Target};
use crate::{evaluate::EvalContext, list_cast, scalar_list_cast};

pub(super) fn cast(
    input: &EvalValue<'_>, element: Target<'_>, selection: Range<usize>, projection: bool, context: &EvalContext<'_>,
) -> Result<Option<ListValue>> {
    let EvalValue::List(Kind::List | Kind::Fixed, reader) = input else { return Ok(None); };
    let fixed;
    let target = match element {
        Target::Cast(target) => target,
        Target::Fixed(ty) => { fixed = CastTarget::fixed(ty.clone()); &fixed }
    };
    if target.union_target().is_some() { return Ok(None); }
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
                return Ok(Some(list));
            }
            if view.is_packed_numeric()
                && let Some(list) = list_cast::numeric(list_cast::Input::Stored(view), target, projection, reserve)?
            {
                return Ok(Some(list));
            }
            scalar_list_cast::Source::Stored(view)
        }
        ListReader::Arrow(cell) => {
            let Some((field, array, range)) = cell.sequence_range() else { return Ok(None); };
            if selection.end > range.len() { return Ok(None); }
            if target_is_float(target) {
                let values = array.slice(range.start + selection.start, selection.len());
                if let Some(list) = crate::float_list_cast::cast(field, values.as_ref(), target, reserve)? {
                    return Ok(Some(list));
                }
            }
            if let Some(span) = cell.numeric_list_span_range(selection.clone())
                && let Some(list) = list_cast::numeric(list_cast::Input::Opened(span), target, projection, reserve)?
            {
                return Ok(Some(list));
            }
            scalar_list_cast::Source::Arrow(*cell, selection)
        }
        _ => return Ok(None),
    };
    scalar_list_cast::cast_list(source, target, context.timezone, Some(context.current_time), projection, reserve)
}

fn target_is_float(target: &CastTarget) -> bool {
    matches!(list_cast::scalar_target(target),
        Some(list_cast::Target::Scalar(rtdb_types::ScalarType::F32 | rtdb_types::ScalarType::F64)))
}
