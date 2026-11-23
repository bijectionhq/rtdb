use std::{ops::Range, sync::Arc};

use arrow_array::{
    Array, ArrayRef, Float32Array, Float64Array, NullArray, UnionArray,
    builder::{Float32Builder, Float64Builder},
    cast::AsArray,
    make_array, new_empty_array,
};
use arrow_buffer::{NullBufferBuilder, ScalarBuffer};
use arrow_schema::{DataType, Field, UnionMode};
use rtdb_columnar::{
    ArrowCell, MemoryBudget, MemoryReservation, copy_selected_cells, estimate_copy_cell_bytes,
    from_field, leg_name,
};
use rtdb_types::{ListKind, ListValue, ListView, LogicalType, Result, ScalarType, Value};

use super::{Column, size_error};

#[derive(Clone)]
pub(crate) struct FloatVector<'a> {
    pub width: Option<i32>,
    values: ListView<'a>,
    pub range: Range<usize>,
}

#[derive(Clone, Copy)]
enum Component {
    F32(f32),
    F64(f64),
    Null,
    Padding,
}

impl<'a> FloatVector<'a> {
    pub fn arrow(cell: ArrowCell<'a>) -> Option<Self> {
        let width = vector_width(cell.field)?;
        let (child, array, range) = cell.sequence_range()?;
        let values = match child.data_type() {
            DataType::Float32 => ListView::F32(array.as_any().downcast_ref::<Float32Array>()?),
            DataType::Float64 => ListView::F64(array.as_any().downcast_ref::<Float64Array>()?),
            DataType::Null if child.is_nullable() || range.is_empty() => ListView::Values(&[]),
            DataType::Union(..) => {
                if !ListValue::supports_numeric_arrow(child) && !unambiguous_children(child) {
                    return None;
                }
                ListView::Union(array.as_any().downcast_ref::<UnionArray>()?)
            }
            _ => return None,
        };
        Some(Self {
            width,
            values,
            range,
        })
    }

    pub fn value(value: &'a Value) -> Option<Self> {
        let (list, width) = match value {
            Value::List(list) => (list, None),
            Value::FixedSizeList(list) => (list, Some(i32::try_from(list.len()).ok()?)),
            _ => return None,
        };
        let values = list.view();
        if !values.is_float()
            && !matches!(values, ListView::Values(values) if values.iter().all(|value| matches!(value, Value::Null | Value::F32(_) | Value::F64(_))))
        {
            return None;
        }
        Some(Self {
            width,
            values,
            range: 0..values.len(),
        })
    }

    fn components(&self) -> impl Iterator<Item = Component> + '_ {
        self.range.clone().map(|row| match self.values {
            ListView::F32(values) if values.is_valid(row) => Component::F32(values.value(row)),
            ListView::F64(values) if values.is_valid(row) => Component::F64(values.value(row)),
            ListView::Union(values) => {
                let child = values.child(values.type_id(row)).as_ref();
                let offset = values.value_offset(row);
                if child.is_null(offset) {
                    return Component::Null;
                }
                match child.data_type() {
                    DataType::Float32 => Component::F32(
                        child
                            .as_primitive::<arrow_array::types::Float32Type>()
                            .value(offset),
                    ),
                    DataType::Float64 => Component::F64(
                        child
                            .as_primitive::<arrow_array::types::Float64Type>()
                            .value(offset),
                    ),
                    _ => Component::Null,
                }
            }
            ListView::Values(values) => match values.get(row) {
                Some(Value::F32(value)) => Component::F32(*value),
                Some(Value::F64(value)) => Component::F64(*value),
                _ => Component::Null,
            },
            _ => Component::Null,
        })
    }

    pub fn float_component(&self, row: usize) -> Option<f64> {
        match self.values {
            ListView::Values(values) => match values.get(row) {
                Some(Value::F32(value)) => Some(f64::from(*value)),
                Some(Value::F64(value)) => Some(*value),
                _ => None,
            },
            values => values.float_component(row),
        }
    }
}

pub(crate) fn logical_vector(ty: &LogicalType) -> bool {
    ty.legs().iter().all(|leg| match leg {
        LogicalType::Null => true,
        LogicalType::List {
            kind: ListKind::List | ListKind::FixedSizeList(_),
            element,
        } => element.legs().iter().all(|leg| {
            matches!(
                leg,
                LogicalType::Null | LogicalType::Scalar(ScalarType::F32 | ScalarType::F64)
            )
        }),
        _ => false,
    })
}

fn vector_width(field: &Field) -> Option<Option<i32>> {
    if field.extension_type_name().is_some() {
        return None;
    }
    match field.data_type() {
        DataType::List(child) if float_elements(child) => Some(None),
        DataType::FixedSizeList(child, width) if float_elements(child) => Some(Some(*width)),
        _ => None,
    }
}

fn float_elements(field: &Field) -> bool {
    field.extension_type_name().is_none()
        && match field.data_type() {
            DataType::Float32 | DataType::Float64 => true,
            DataType::Null => true,
            DataType::Union(children, UnionMode::Dense) => {
                children.iter().all(|(_, field)| match field.data_type() {
                    DataType::Float32 | DataType::Float64 => field.extension_type_name().is_none(),
                    DataType::Null => field.is_nullable() && field.extension_type_name().is_none(),
                    _ => false,
                })
            }
            _ => false,
        }
}

fn unambiguous_children(field: &Field) -> bool {
    let DataType::Union(fields, _) = field.data_type() else {
        return true;
    };
    let Ok(logical) = from_field(field) else {
        return false;
    };
    fields.iter().all(|(_, child)| {
        from_field(child)
            .is_ok_and(|physical| named_leg(&logical, child).is_none_or(|leg| *leg == physical))
    })
}

fn named_leg<'a>(logical: &'a LogicalType, child: &Field) -> Option<&'a LogicalType> {
    let LogicalType::Union(legs) = logical else {
        return None;
    };
    legs.iter()
        .find(|leg| leg_name(leg).is_ok_and(|name| &name == child.name()))
}

pub(crate) fn read_position(column: &Column, row: usize) -> Option<Option<ArrowCell<'_>>> {
    let Some((array, field, logical, row)) = column.arrow_cell(row)? else {
        return Some(None);
    };
    let mut position = ArrowCell { array, field, row };
    let mut physical = match logical {
        Some(ty) if matches!(field.data_type(), DataType::Union(_, UnionMode::Dense)) => ty.clone(),
        logical => {
            let physical = from_field(field).ok()?;
            if logical.is_some_and(|ty| *ty != physical) {
                return None;
            }
            physical
        }
    };
    if physical == LogicalType::Nothing {
        return None;
    }
    while let DataType::Union(fields, UnionMode::Dense) = position.field.data_type() {
        let union = position.array.as_union();
        let id = union.type_id(position.row);
        let (_, child) = fields.iter().find(|(tag, _)| *tag == id)?;
        let child_type = from_field(child).ok()?;
        if named_leg(&physical, child).is_some_and(|leg| *leg != child_type) {
            return None;
        }
        position = ArrowCell {
            array: union.child(id).as_ref(),
            field: child,
            row: union.value_offset(position.row),
        };
        physical = child_type;
    }
    if physical == LogicalType::Nothing
        || matches!(position.field.data_type(),
        DataType::RunEndEncoded(_, values) if matches!(values.data_type(), DataType::Union(..)))
    {
        return None;
    }
    Some(Some(position.physical().ok()?))
}

#[derive(Clone)]
enum OutputRow<'a> {
    Null,
    Vector(FloatVector<'a>),
    Scalar(ArrowCell<'a>),
}

fn row(column: &Column, index: usize) -> Option<OutputRow<'_>> {
    if let Column::Values(values) = column {
        let value = values.get(index)?;
        return if matches!(value, Value::Null | Value::Undefined) {
            Some(OutputRow::Null)
        } else {
            FloatVector::value(value).map(OutputRow::Vector)
        };
    }
    let Some(cell) = read_position(column, index)? else {
        return Some(OutputRow::Null);
    };
    if cell.is_null().ok()? {
        return Some(OutputRow::Null);
    }
    if let Some(vector) = FloatVector::arrow(cell) {
        return Some(OutputRow::Vector(vector));
    }
    (cell.field.extension_type_name().is_none() && super::scalar_layout(cell.field.data_type()))
        .then_some(OutputRow::Scalar(cell))
}

pub(crate) fn relayout(
    column: &Column,
    len: usize,
    field: &Field,
    memory: &Arc<MemoryBudget>,
) -> Result<Option<(ArrayRef, Arc<MemoryReservation>)>> {
    if vector_width(field).is_none()
        && !matches!(field.data_type(), DataType::Union(fields, UnionMode::Dense)
        if fields.iter().any(|(_, field)| vector_width(field).is_some()))
    {
        return Ok(None);
    }
    let bound = field
        .size()
        .checked_mul(8)
        .and_then(|size| size.checked_add(4096))
        .ok_or_else(size_error)?;
    let mut scratch = memory.reserve(bound)?;
    let charge = Arc::get_mut(&mut scratch).expect("A vector gather owns its scratch");
    let row_bytes = len
        .checked_mul(4 * std::mem::size_of::<OutputRow<'_>>())
        .ok_or_else(size_error)?;
    charge.grow_to(bound.checked_add(row_bytes).ok_or_else(size_error)?)?;
    let Some(rows) = (0..len)
        .map(|index| row(column, index))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let buffers = rows.iter().try_fold(0usize, |size, row| {
        let bytes = match row {
            OutputRow::Vector(vector) => {
                vector.range.len().checked_mul(32).ok_or_else(size_error)?
            }
            OutputRow::Null => vector_width(field)
                .flatten()
                .map_or(0, |width| width as usize)
                .checked_mul(32)
                .ok_or_else(size_error)?,
            OutputRow::Scalar(cell) => estimate_copy_cell_bytes(cell.field, cell.array, cell.row)?
                .checked_mul(4)
                .ok_or_else(size_error)?,
        };
        size.checked_add(bytes).ok_or_else(size_error)
    })?;
    charge.grow_to(charge.bytes().checked_add(buffers).ok_or_else(size_error)?)?;
    let Some(array) = gather(field, &rows)? else {
        return Ok(None);
    };
    drop(rows);
    charge.shrink_to(array.get_array_memory_size())?;
    Ok(Some((array, scratch)))
}

fn gather(field: &Field, rows: &[OutputRow<'_>]) -> Result<Option<ArrayRef>> {
    if rows.is_empty() {
        return Ok(Some(new_empty_array(field.data_type())));
    }
    if let DataType::Union(fields, UnionMode::Dense) = field.data_type() {
        let mut ids = Vec::with_capacity(rows.len());
        let mut offsets = Vec::with_capacity(rows.len());
        let mut legs = fields
            .iter()
            .map(|(_, field)| (field, Vec::new()))
            .collect::<Vec<_>>();
        for row in rows {
            let leg = fields.iter().enumerate().find(|(_, (_, field))| match row {
                OutputRow::Vector(vector) => vector_width(field) == Some(vector.width),
                OutputRow::Null => field.data_type() == &DataType::Null && field.is_nullable(),
                OutputRow::Scalar(cell) => {
                    field.data_type() == cell.field.data_type()
                        && field.metadata() == cell.field.metadata()
                }
            });
            let Some((index, (id, _))) = leg else {
                return Ok(None);
            };
            ids.push(id);
            offsets.push(i32::try_from(legs[index].1.len()).map_err(|_| size_error())?);
            legs[index].1.push(row.clone());
        }
        let children = legs
            .iter()
            .map(|(field, rows)| {
                if field.data_type() == &DataType::Null {
                    Ok(Some(Arc::new(NullArray::new(rows.len())) as ArrayRef))
                } else {
                    gather(field, rows)
                }
            })
            .collect::<Result<Option<Vec<ArrayRef>>>>()?;
        let Some(children) = children else {
            return Ok(None);
        };
        return Ok(
            UnionArray::try_new(fields.clone(), ids.into(), Some(offsets.into()), children)
                .ok()
                .map(|array| Arc::new(array) as ArrayRef),
        );
    }
    if super::scalar_layout(field.data_type()) {
        let mut arrays = Vec::with_capacity(rows.len());
        let mut positions = Vec::with_capacity(rows.len());
        for row in rows {
            let OutputRow::Scalar(cell) = row else {
                return Ok(None);
            };
            if cell.field.data_type() != field.data_type() {
                return Ok(None);
            }
            positions.push((arrays.len(), cell.row));
            arrays.push(cell.array);
        }
        return Ok(Some(copy_selected_cells(&arrays, &positions)?));
    }
    let (child, width) = match field.data_type() {
        DataType::List(child) => (child, None),
        DataType::FixedSizeList(child, width) => (child, Some(*width)),
        _ => return Ok(None),
    };
    let mut validity = NullBufferBuilder::new(rows.len());
    let mut offsets = vec![0_i32];
    let mut count = 0usize;
    for row in rows {
        match row {
            OutputRow::Vector(vector) if vector.width == width => {
                count = count
                    .checked_add(vector.range.len())
                    .ok_or_else(size_error)?;
                validity.append_non_null();
            }
            OutputRow::Null if field.is_nullable() => {
                count = count
                    .checked_add(width.unwrap_or(0) as usize)
                    .ok_or_else(size_error)?;
                validity.append_null();
            }
            _ => return Ok(None),
        }
        if width.is_none() {
            offsets.push(i32::try_from(count).map_err(|_| size_error())?);
        }
    }
    let components = rows.iter().flat_map(|row| {
        let vector = match row {
            OutputRow::Vector(vector) => Some(vector),
            _ => None,
        };
        let padding = if vector.is_none() {
            width.unwrap_or(0) as usize
        } else {
            0
        };
        vector
            .into_iter()
            .flat_map(FloatVector::components)
            .chain(std::iter::repeat_n(Component::Padding, padding))
    });
    let Some(values) = gather_components(child, count, components)? else {
        return Ok(None);
    };
    let data = new_empty_array(field.data_type())
        .to_data()
        .into_builder()
        .len(rows.len())
        .nulls(validity.finish())
        .child_data(vec![values.to_data()]);
    let data = if width.is_none() {
        data.buffers(vec![ScalarBuffer::from(offsets).into_inner()])
    } else {
        data
    };
    Ok(data.build().ok().map(make_array))
}

fn gather_components(
    field: &Field,
    count: usize,
    components: impl Iterator<Item = Component>,
) -> Result<Option<ArrayRef>> {
    macro_rules! primitive {
        ($builder:ty, $variant:ident) => {{
            let mut values = <$builder>::with_capacity(count);
            for value in components {
                match value {
                    Component::$variant(value) => values.append_value(value),
                    Component::Padding => values.append_null(),
                    Component::Null if field.is_nullable() => values.append_null(),
                    _ => return Ok(None),
                }
            }
            Some(Arc::new(values.finish()) as ArrayRef)
        }};
    }
    Ok(match field.data_type() {
        DataType::Float32 => primitive!(Float32Builder, F32),
        DataType::Float64 => primitive!(Float64Builder, F64),
        DataType::Null => components
            .into_iter()
            .all(|value| matches!(value, Component::Null | Component::Padding))
            .then(|| Arc::new(NullArray::new(count)) as ArrayRef),
        DataType::Union(fields, UnionMode::Dense) => {
            let id_for = |ty: &DataType| {
                fields
                    .iter()
                    .find_map(|(id, field)| (field.data_type() == ty).then_some(id))
            };
            let (f32_id, f64_id, null_id) = (
                id_for(&DataType::Float32),
                id_for(&DataType::Float64),
                id_for(&DataType::Null),
            );
            let padding_id = fields.iter().next().map(|(id, _)| id);
            let mut f32s = Float32Builder::with_capacity(0);
            let mut f64s = Float64Builder::with_capacity(0);
            let mut nulls = 0usize;
            let mut ids = Vec::with_capacity(count);
            let mut offsets = Vec::with_capacity(count);
            use arrow_array::builder::ArrayBuilder;
            for value in components {
                let (id, offset) = match value {
                    Component::F32(value) => {
                        let offset = f32s.len();
                        f32s.append_value(value);
                        (f32_id, offset)
                    }
                    Component::F64(value) => {
                        let offset = f64s.len();
                        f64s.append_value(value);
                        (f64_id, offset)
                    }
                    Component::Padding if padding_id == f32_id => {
                        let offset = f32s.len();
                        f32s.append_null();
                        (f32_id, offset)
                    }
                    Component::Padding if padding_id == f64_id => {
                        let offset = f64s.len();
                        f64s.append_null();
                        (f64_id, offset)
                    }
                    Component::Null | Component::Padding => {
                        let offset = nulls;
                        nulls += 1;
                        (null_id, offset)
                    }
                };
                let Some(id) = id else {
                    return Ok(None);
                };
                ids.push(id);
                offsets.push(i32::try_from(offset).map_err(|_| size_error())?);
            }
            let f32s = Arc::new(f32s.finish()) as ArrayRef;
            let f64s = Arc::new(f64s.finish()) as ArrayRef;
            let children = fields
                .iter()
                .map(|(id, field)| {
                    if Some(id) == f32_id {
                        Arc::clone(&f32s)
                    } else if Some(id) == f64_id {
                        Arc::clone(&f64s)
                    } else if Some(id) == null_id {
                        Arc::new(NullArray::new(nulls))
                    } else {
                        new_empty_array(field.data_type())
                    }
                })
                .collect::<Vec<_>>();
            UnionArray::try_new(fields.clone(), ids.into(), Some(offsets.into()), children)
                .ok()
                .map(|array| Arc::new(array) as ArrayRef)
        }
        _ => None,
    })
}
