use std::sync::Arc;

use rtdb_plan::CastTarget;
use rtdb_types::{ListValue, Result};

use super::{EvalValue, Kind, ListReader, Target};
use crate::evaluate::EvalContext;

pub(super) fn float_cast(input: &EvalValue<'_>, element: Target<'_>, context: &EvalContext<'_>) -> Result<Option<ListValue>> {
    let EvalValue::List(Kind::List | Kind::Fixed, reader) = input else { return Ok(None); };
    let fixed;
    let target = match element {
        Target::Cast(target) => target,
        Target::Fixed(ty) => { fixed = CastTarget::fixed(ty.clone()); &fixed }
    };
    let source = match reader.as_ref() {
        ListReader::Stored(list) => crate::float_list_cast::packed(list),
        ListReader::Arrow(cell) => cell.sequence_range()
            .map(|(field, array, range)| (Arc::new(field.clone()), array.slice(range.start, range.len()))),
        _ => None,
    };
    let Some((field, values)) = source else { return Ok(None); };
    crate::float_list_cast::cast(&field, values.as_ref(), target, |bytes| context.reserve(bytes))
}
