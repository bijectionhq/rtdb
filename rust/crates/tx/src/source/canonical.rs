use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::{DataType, Field, UnionMode};
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation, arrow_relayout_bound, from_field, leg_name, relayout_arrow_column};
use rtdb_types::{LogicalType, OrderedSet, Result};

pub(crate) fn selectable(field: &Field) -> bool {
    fn supported(field: &Field, masked: bool) -> bool {
        if field.extension_type_name() == Some("xt/tstz-range") { return false; }
        match field.data_type() {
            DataType::Struct(fields) => fields.iter().all(|child| supported(child, masked || field.is_nullable())),
            DataType::FixedSizeList(child, _) => supported(child, masked || field.is_nullable()),
            DataType::List(child) | DataType::Map(child, _) => supported(child, false),
            DataType::RunEndEncoded(_, child) => supported(child, masked),
            DataType::Union(fields, UnionMode::Dense) => {
                // The row writer can choose a different union tag beneath a null parent.
                if masked { return false; }
                let Ok(LogicalType::Union(legs)) = from_field(field) else { return false; };
                let mut names = OrderedSet::with_capacity(fields.len());
                fields.iter().all(|(_, child)| {
                    from_field(child).is_ok_and(|physical| {
                        leg_name(&physical).is_ok_and(|name| names.insert(name))
                            && legs.iter().find(|leg| leg_name(leg).is_ok_and(|name| name == *child.name()))
                                .is_none_or(|named| *named == physical)
                            && supported(child, false)
                    })
                })
            }
            _ => true,
        }
    }
    supported(field, false)
}

pub(crate) fn align(
    array: &ArrayRef, source: &Field, target: &Field, memory: &Arc<MemoryBudget>,
) -> Result<Option<(ArrayRef, Option<Arc<MemoryReservation>>)>> {
    if source.data_type() == target.data_type() { return Ok(Some((Arc::clone(array), None))); }
    if retags_undefined(source, target) { return Ok(None); }
    let scratch = memory.reserve(arrow_relayout_bound(array.as_ref(), source, target)?)?;
    let Ok((field, array)) = relayout_arrow_column(Arc::clone(array), source, target) else { return Ok(None); };
    Ok((field.data_type() == target.data_type()).then_some((array, Some(scratch))))
}

fn retags_undefined(source: &Field, target: &Field) -> bool {
    match (source.data_type(), target.data_type()) {
        (DataType::Null, DataType::Union(..)) => !source.is_nullable(),
        (DataType::Struct(from), DataType::Struct(to)) => from.iter().any(|child| {
            to.iter().find(|field| field.name() == child.name()).is_some_and(|field| retags_undefined(child, field))
        }),
        (DataType::List(from), DataType::List(to))
        | (DataType::FixedSizeList(from, _), DataType::FixedSizeList(to, _))
        | (DataType::Map(from, _), DataType::Map(to, _)) => retags_undefined(from, to),
        (DataType::RunEndEncoded(_, value), _) => retags_undefined(value, target),
        (DataType::Union(from, UnionMode::Dense), DataType::Union(to, UnionMode::Dense)) => from.iter().any(|(_, child)| {
            let Ok(name) = from_field(child).and_then(|ty| leg_name(&ty)) else { return true; };
            to.iter().find(|(_, field)| *field.name() == name).is_some_and(|(_, field)| retags_undefined(child, field))
        }),
        (_, DataType::Union(to, UnionMode::Dense)) => from_field(source).is_ok_and(|ty| {
            ty.legs().iter().any(|leg| leg_name(leg).is_ok_and(|name| {
                to.iter().find(|(_, field)| *field.name() == name).is_some_and(|(_, field)| retags_undefined(source, field))
            }))
        }),
        _ => false,
    }
}

pub(crate) fn selectable_cell(cell: ArrowCell<'_>, packed_null_bytes: bool) -> bool {
    let Ok(physical) = cell.physical() else { return false; };
    let union = matches!(cell.field.data_type(), DataType::Union(..))
        || matches!(cell.field.data_type(), DataType::RunEndEncoded(_, value) if matches!(value.data_type(), DataType::Union(..)));
    if union
        && physical.field.data_type() != &DataType::Null && physical.array.is_null(physical.row) {
        return false;
    }
    if let Some(children) = physical.structure() {
        return children.into_iter().all(|child| selectable_cell(child, packed_null_bytes));
    }
    if let Some((field, array, range)) = physical.sequence_range() {
        let packed = packed_null_bytes
            && matches!(physical.sequence_kind(), Some(rtdb_types::ListKind::List | rtdb_types::ListKind::FixedSizeList(_)))
            && rtdb_types::ListValue::supports_numeric_arrow(field);
        let map = matches!(physical.field.data_type(), DataType::Map(..));
        return range.map(|row| ArrowCell { field, array, row }).all(|child| {
            if map && child.structure().and_then(|mut fields| fields.next()).is_none_or(|key| !matches!(key.is_null(), Ok(false))) {
                return false;
            }
            (!packed || clean_packed_null(child)) && selectable_cell(child, packed_null_bytes)
        });
    }
    true
}

fn clean_packed_null(cell: ArrowCell<'_>) -> bool {
    let Ok(ArrowCell { field, array, row }) = cell.physical() else { return false; };
    if !array.is_null(row) { return true; }
    let Some(width) = field.data_type().primitive_width() else { return true; };
    let data = array.to_data();
    let start = (row + data.offset()) * width;
    // A single row-writer chunk can retain the original bytes of a packed numeric null.
    data.buffers()[0].as_slice()[start..start + width].iter().all(|byte| *byte == 0)
}
