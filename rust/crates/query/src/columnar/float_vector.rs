use std::{ops::Range, sync::Arc};

use arrow_array::{
    Array, ArrayRef, Float32Array, Float64Array, NullArray, UnionArray,
    builder::{Float32Builder, Float64Builder},
    cast::AsArray,
    make_array, new_empty_array,
};
use arrow_buffer::{NullBufferBuilder, ScalarBuffer};
use arrow_schema::{DataType, Field, UnionFields, UnionMode};
use rtdb_columnar::{
    ArrowCell, MemoryBudget, MemoryReservation, copy_nullable_cells, estimate_copy_cell_bytes,
    estimate_null_copy_cell_bytes, from_field, leg_name,
};
use rtdb_types::{ListKind, ListValue, ListView, LogicalType, Result, ScalarType, Value};

use super::{Column, size_error};

#[derive(Clone)]
pub(crate) struct FloatVector<'a> {
    pub width: Option<i32>,
    values: FloatValues<'a>,
    pub range: Range<usize>,
}

#[derive(Clone)]
enum FloatValues<'a> {
    Packed(ListView<'a>),
    Selected { field: &'a Field, array: &'a dyn Array, element: LogicalType },
}

#[derive(Clone, Copy)]
enum Component {
    F32(f32),
    F64(f64),
    Null,
    MaskedF32(f32),
    MaskedF64(f64),
    Padding,
}

impl<'a> FloatVector<'a> {
    pub fn arrow(cell: ArrowCell<'a>, logical: &LogicalType) -> Option<Self> {
        let LogicalType::List { kind, element } = nonnull(logical) else { return None; };
        let (child, array, range) = cell.sequence_range()?;
        let width = match kind {
            ListKind::List => None,
            ListKind::FixedSizeList(_) => Some(i32::try_from(range.len()).ok()?),
            _ => return None,
        };
        let packed = if vector_width(cell.field).is_some()
            && from_field(child).is_ok_and(|physical| physical == **element)
        {
            Self::packed(child, array, &range)
        } else {
            None
        };
        let values = if let Some(packed) = packed {
            FloatValues::Packed(packed)
        } else if range.clone().all(|row| Self::selected_component(child, array, row, element).is_some()) {
            FloatValues::Selected { field: child, array, element: *element.clone() }
        } else {
            return None;
        };
        Some(Self { width, values, range })
    }

    fn packed(child: &Field, array: &'a dyn Array, range: &Range<usize>) -> Option<ListView<'a>> {
        Some(match child.data_type() {
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
        })
    }

    fn selected_component(field: &Field, array: &dyn Array, row: usize, element: &LogicalType) -> Option<Component> {
        let (cell, logical) = (ArrowCell { field, array, row }).logical_position(Some(element)).ok()?;
        if cell.array.is_null(cell.row) || logical == LogicalType::Null {
            return Some(Component::Null);
        }
        match (nonnull(&logical), cell.field.data_type()) {
            (LogicalType::Scalar(ScalarType::F32), DataType::Float32) =>
                Some(Component::F32(cell.array.as_primitive::<arrow_array::types::Float32Type>().value(cell.row))),
            (LogicalType::Scalar(ScalarType::F64), DataType::Float64) =>
                Some(Component::F64(cell.array.as_primitive::<arrow_array::types::Float64Type>().value(cell.row))),
            _ => None,
        }
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
            values: FloatValues::Packed(values),
            range: 0..values.len(),
        })
    }

    fn components(&self) -> impl Iterator<Item = Component> + '_ {
        self.range.clone().map(|row| self.component(row))
    }

    fn component(&self, row: usize) -> Component {
        let values = match &self.values {
            FloatValues::Packed(values) => *values,
            FloatValues::Selected { field, array, element } =>
                return Self::selected_component(field, *array, row, element).expect("Selected float components were checked"),
        };
        match values {
            ListView::F32(values) if values.is_valid(row) => Component::F32(values.value(row)),
            ListView::F64(values) if values.is_valid(row) => Component::F64(values.value(row)),
            ListView::Union(values) => {
                let child = values.child(values.type_id(row)).as_ref();
                let offset = values.value_offset(row);
                match child.data_type() {
                    DataType::Float32 => {
                        let value = child.as_primitive::<arrow_array::types::Float32Type>().value(offset);
                        if child.is_null(offset) { Component::MaskedF32(value) } else { Component::F32(value) }
                    }
                    DataType::Float64 => {
                        let value = child.as_primitive::<arrow_array::types::Float64Type>().value(offset);
                        if child.is_null(offset) { Component::MaskedF64(value) } else { Component::F64(value) }
                    }
                    _ => Component::Null,
                }
            }
            ListView::Values(values) => match values.get(row) {
                Some(Value::F32(value)) => Component::F32(*value),
                Some(Value::F64(value)) => Component::F64(*value),
                _ => Component::Null,
            },
            _ => Component::Null,
        }
    }

    pub fn float_component(&self, row: usize) -> Option<f64> {
        match self.component(row) {
            Component::F32(value) => Some(f64::from(value)),
            Component::F64(value) => Some(value),
            Component::Null | Component::MaskedF32(_) | Component::MaskedF64(_) | Component::Padding => None,
        }
    }
}

fn vector_width(field: &Field) -> Option<Option<i32>> {
    let width = list_width(field)?;
    match field.data_type() {
        DataType::List(child) | DataType::FixedSizeList(child, _) if float_elements(child) => Some(width),
        _ => None,
    }
}

fn list_width(field: &Field) -> Option<Option<i32>> {
    if field.extension_type_name().is_some() {
        return None;
    }
    match field.data_type() {
        DataType::List(_) => Some(None),
        DataType::FixedSizeList(_, width) => Some(Some(*width)),
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

pub(crate) fn read_position(column: &Column, row: usize) -> Option<Option<(ArrowCell<'_>, LogicalType)>> {
    let Some((array, field, logical, row)) = column.arrow_cell(row)? else {
        return Some(None);
    };
    Some(Some(ArrowCell { array, field, row }.logical_position(logical).ok()?))
}

pub(crate) fn nonnull(logical: &LogicalType) -> &LogicalType {
    match logical {
        LogicalType::Maybe(inner) => inner,
        logical => logical,
    }
}

#[derive(Clone)]
enum OutputRow<'a> {
    Null,
    Padding,
    Vector(FloatVector<'a>, Option<(ArrowCell<'a>, LogicalType)>),
    Scalar(ArrowCell<'a>, ScalarType),
    Nested(ArrowCell<'a>, LogicalType),
}

fn row(column: &Column, index: usize) -> Option<OutputRow<'_>> {
    if let Column::Values(values) = column {
        let value = values.get(index)?;
        return if matches!(value, Value::Null | Value::Undefined) {
            Some(OutputRow::Null)
        } else {
            FloatVector::value(value).map(|vector| OutputRow::Vector(vector, None))
        };
    }
    let Some((cell, logical)) = read_position(column, index)? else {
        return Some(OutputRow::Null);
    };
    output_row(cell, logical)
}

fn output_row(cell: ArrowCell<'_>, logical: LogicalType) -> Option<OutputRow<'_>> {
    if logical == LogicalType::Null || cell.array.is_null(cell.row) {
        return Some(OutputRow::Null);
    }
    if let Some(vector) = FloatVector::arrow(cell, &logical) {
        return Some(OutputRow::Vector(vector, Some((cell, logical))));
    }
    let scalar = match (nonnull(&logical), cell.field.data_type()) {
        (LogicalType::Scalar(scalar), _) => scalar,
        (LogicalType::Struct(_), DataType::Struct(_))
        | (LogicalType::List { .. }, DataType::List(_) | DataType::FixedSizeList(_, _) | DataType::Map(_, _)) =>
            return Some(OutputRow::Nested(cell, logical)),
        _ => return None,
    };
    if !cell.has_scalar_storage(scalar) { return None; }
    if *scalar == ScalarType::Uri {
        rtdb_types::UriParts::parse(cell.array.as_string::<i32>().value(cell.row)).ok()?;
    }
    let LogicalType::Scalar(mut scalar) = rtdb_columnar::execution_type(nonnull(&logical)) else { return None; };
    if let (ScalarType::FixedSizeBinary(width), DataType::FixedSizeBinary(actual)) = (&mut scalar, cell.field.data_type()) {
        *width = *actual;
    }
    Some(OutputRow::Scalar(cell, scalar))
}

pub(crate) fn relayout(
    column: &Column,
    len: usize,
    field: &Field,
    memory: &Arc<MemoryBudget>,
) -> Result<Option<(ArrayRef, Arc<MemoryReservation>)>> {
    if !super::scalar_layout(field.data_type()) && !matches!(field.data_type(),
        DataType::Union(_, UnionMode::Dense) | DataType::Struct(_) | DataType::List(_) | DataType::FixedSizeList(_, _) | DataType::Map(_, _)) {
        return Ok(None);
    }
    if matches!(column, Column::Values(_)) && vector_width(field).is_none()
        && !matches!(field.data_type(), DataType::Union(fields, UnionMode::Dense) if fields.iter().any(|(_, field)| vector_width(field).is_some()))
    {
        return Ok(None);
    }
    let bound = field
        .size()
        .checked_mul(8)
        .and_then(|size| size.checked_add(4096))
        .ok_or_else(size_error)?;
    let Ok(mut scratch) = memory.reserve(bound) else { return Ok(None); };
    let charge = Arc::get_mut(&mut scratch).expect("A vector gather owns its scratch");
    let row_bytes = len
        .checked_mul(4 * std::mem::size_of::<OutputRow<'_>>())
        .ok_or_else(size_error)?;
    if charge.grow_to(bound.checked_add(row_bytes).ok_or_else(size_error)?).is_err() {
        return Ok(None);
    }
    let Some(rows) = (0..len)
        .map(|index| row(column, index))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let padding = estimate_null_copy_cell_bytes(field)?.checked_mul(4).ok_or_else(size_error)?;
    let Some(gather) = OutputGather::route(field, rows)? else { return Ok(None); };
    let buffers = gather.buffer_bytes(padding)?;
    if charge.grow_to(charge.bytes().checked_add(buffers).ok_or_else(size_error)?).is_err() {
        return Ok(None);
    }
    let Some(array) = gather.finish()? else { return Ok(None); };
    charge.shrink_to(array.get_array_memory_size())?;
    Ok(Some((array, scratch)))
}

enum OutputGather<'a, 'f> {
    Plain {
        field: &'f Field,
        rows: Vec<OutputRow<'a>>,
    },
    Union {
        fields: &'f UnionFields,
        ids: Vec<i8>,
        offsets: Vec<i32>,
        legs: Vec<Vec<OutputRow<'a>>>,
    },
}

impl<'a, 'f> OutputGather<'a, 'f> {
    fn route(field: &'f Field, rows: Vec<OutputRow<'a>>) -> Result<Option<Self>> {
        let DataType::Union(fields, UnionMode::Dense) = field.data_type() else {
            return Ok(Some(Self::Plain { field, rows }));
        };
        if rows.is_empty() {
            return Ok(Some(Self::Plain { field, rows }));
        }
        let mut ids = Vec::with_capacity(rows.len());
        let mut offsets = Vec::with_capacity(rows.len());
        let mut legs = fields.iter().map(|_| Vec::new()).collect::<Vec<_>>();
        for row in rows {
            let leg = fields.iter().enumerate().find(|(position, (_, field))| match &row {
                OutputRow::Vector(vector, _) => list_width(field) == Some(vector.width),
                OutputRow::Null => field.data_type() == &DataType::Null && field.is_nullable(),
                OutputRow::Padding => *position == 0,
                OutputRow::Scalar(_, scalar) => scalar_leg(field, scalar),
                OutputRow::Nested(_, logical) => from_field(field).is_ok_and(|target|
                    match (nonnull(logical), nonnull(&target)) {
                        (LogicalType::Struct(_), LogicalType::Struct(_)) => true,
                        (LogicalType::List { kind: source, .. }, LogicalType::List { kind: target, .. }) => source == target,
                        _ => false,
                    }),
            });
            let Some((index, (id, _))) = leg else { return Ok(None); };
            ids.push(id);
            offsets.push(i32::try_from(legs[index].len()).map_err(|_| size_error())?);
            legs[index].push(row);
        }
        Ok(Some(Self::Union { fields, ids, offsets, legs }))
    }

    fn buffer_bytes(&self, padding: usize) -> Result<usize> {
        match self {
            Self::Plain { field, rows } => gather_buffer_bytes(field, rows, padding),
            Self::Union { fields, legs, .. } => fields.iter().zip(legs)
                .try_fold(0usize, |size, ((_, field), rows)| {
                    size.checked_add(gather_buffer_bytes(field, rows, padding)?).ok_or_else(size_error)
                }),
        }
    }

    fn finish(self) -> Result<Option<ArrayRef>> {
        let (fields, ids, offsets, legs) = match self {
            Self::Plain { field, rows } => return gather_plain(field, &rows),
            Self::Union { fields, ids, offsets, legs } => (fields, ids, offsets, legs),
        };
        let children = fields.iter().zip(legs)
            .map(|((_, field), rows)| {
                if field.data_type() == &DataType::Null {
                    Ok(Some(Arc::new(NullArray::new(rows.len())) as ArrayRef))
                } else {
                    gather(field, rows)
                }
            })
            .collect::<Result<Option<Vec<ArrayRef>>>>()?;
        let Some(children) = children else { return Ok(None); };
        Ok(UnionArray::try_new(fields.clone(), ids.into(), Some(offsets.into()), children)
            .ok()
            .map(|array| Arc::new(array) as ArrayRef))
    }
}

fn gather_buffer_bytes(field: &Field, rows: &[OutputRow<'_>], padding: usize) -> Result<usize> {
    let per_component = if uses_native_vectors(field, rows) { 32 } else { 512 };
    rows.iter().try_fold(0usize, |size, row| {
        let bytes = match row {
            OutputRow::Vector(vector, _) => vector.range.len().checked_mul(per_component).ok_or_else(size_error)?,
            OutputRow::Null | OutputRow::Padding => padding,
            OutputRow::Scalar(cell, _) => estimate_copy_cell_bytes(cell.field, cell.array, cell.row)?
                .checked_mul(4)
                .ok_or_else(size_error)?,
            OutputRow::Nested(cell, _) => estimate_copy_cell_bytes(cell.field, cell.array, cell.row)?
                .checked_mul(4).and_then(|bytes| bytes.checked_add(padding)).ok_or_else(size_error)?,
        };
        size.checked_add(bytes).ok_or_else(size_error)
    })
}

fn gather(field: &Field, rows: Vec<OutputRow<'_>>) -> Result<Option<ArrayRef>> {
    let Some(gather) = OutputGather::route(field, rows)? else { return Ok(None); };
    gather.finish()
}

fn gather_plain(field: &Field, rows: &[OutputRow<'_>]) -> Result<Option<ArrayRef>> {
    if rows.is_empty() {
        return Ok(Some(new_empty_array(field.data_type())));
    }
    if field.data_type() == &DataType::Null {
        return Ok(rows.iter().all(|row| matches!(row, OutputRow::Padding) || field.is_nullable() && matches!(row, OutputRow::Null))
            .then(|| Arc::new(NullArray::new(rows.len())) as ArrayRef));
    }
    if super::scalar_layout(field.data_type()) {
        let mut converted = Vec::with_capacity(rows.len());
        let mut positions = Vec::with_capacity(rows.len());
        for row in rows {
            if matches!(row, OutputRow::Padding) || matches!(row, OutputRow::Null) && field.is_nullable() {
                positions.push(None);
                converted.push(None);
                continue;
            }
            let OutputRow::Scalar(cell, scalar) = row else {
                return Ok(None);
            };
            if !scalar_leg(field, scalar) {
                return Ok(None);
            }
            if cell.field.data_type() == field.data_type() {
                positions.push(Some((converted.len(), cell.row)));
                converted.push(None);
                continue;
            }
            if matches!(field.data_type(), DataType::FixedSizeBinary(_)) { return Ok(None); }
            let Ok(data) = cell.array.slice(cell.row, 1).to_data().into_builder()
                .data_type(field.data_type().clone()).build() else { return Ok(None); };
            positions.push(Some((converted.len(), 0)));
            converted.push(Some(make_array(data)));
        }
        let empty = new_empty_array(field.data_type());
        let arrays = rows.iter().zip(&converted).map(|(row, converted)| match (row, converted) {
            (_, Some(converted)) => converted.as_ref(),
            (OutputRow::Scalar(cell, _), None) => cell.array,
            _ => empty.as_ref(),
        }).collect::<Vec<_>>();
        return Ok(copy_nullable_cells(&arrays, &positions).ok());
    }
    if let DataType::Struct(fields) = field.data_type() {
        return gather_struct(field, fields, rows);
    }
    if uses_native_vectors(field, rows) {
        return gather_vectors(field, rows);
    }
    gather_sequence(field, rows)
}

fn gather_struct(field: &Field, fields: &arrow_schema::Fields, rows: &[OutputRow<'_>]) -> Result<Option<ArrayRef>> {
    let mut valid = NullBufferBuilder::new(rows.len());
    for row in rows {
        match row {
            OutputRow::Nested(cell, logical) if matches!(nonnull(logical), LogicalType::Struct(_)) => {
                let DataType::Struct(source) = cell.field.data_type() else { return Ok(None); };
                if source.iter().any(|child| fields.find(child.name()).is_none()) { return Ok(None); }
                valid.append_non_null();
            }
            OutputRow::Padding => valid.append_null(),
            OutputRow::Null if field.is_nullable() => valid.append_null(),
            _ => return Ok(None),
        }
    }
    let mut arrays = Vec::with_capacity(fields.len());
    for field in fields {
        let rows = rows.iter().map(|row| {
            let OutputRow::Nested(cell, logical) = row else { return Some(OutputRow::Padding); };
            let LogicalType::Struct(types) = nonnull(logical) else { return None; };
            let DataType::Struct(source) = cell.field.data_type() else { return None; };
            let Some((index, child)) = source.find(field.name()) else { return Some(OutputRow::Null); };
            let child = ArrowCell { field: child, array: cell.array.as_struct().column(index).as_ref(), row: cell.row };
            child_row(child, types.get(field.name())?)
        }).collect::<Option<Vec<_>>>();
        let Some(rows) = rows else { return Ok(None); };
        let Some(array) = gather(field, rows)? else { return Ok(None); };
        arrays.push(array);
    }
    Ok(rtdb_columnar::build_struct_array(fields.clone(), &arrays, rows.len(), valid.finish()).ok())
}

fn child_row<'a>(cell: ArrowCell<'a>, logical: &LogicalType) -> Option<OutputRow<'a>> {
    if *logical == LogicalType::Nothing { return Some(OutputRow::Padding); }
    let (cell, logical) = cell.logical_position(Some(logical)).ok()?;
    output_row(cell, logical)
}

fn gather_sequence(field: &Field, rows: &[OutputRow<'_>]) -> Result<Option<ArrayRef>> {
    let (child, width) = match field.data_type() {
        DataType::List(child) | DataType::Map(child, _) => (child, None),
        DataType::FixedSizeList(child, width) => (child, Some(*width as usize)),
        _ => return Ok(None),
    };
    let Ok(logical) = from_field(field) else { return Ok(None); };
    let LogicalType::List { kind, .. } = nonnull(&logical) else { return Ok(None); };
    let mut valid = NullBufferBuilder::new(rows.len());
    let mut selections = Vec::new();
    let mut offsets = vec![0_i32];
    for row in rows {
        match row {
            OutputRow::Nested(cell, logical) | OutputRow::Vector(_, Some((cell, logical))) => {
                let LogicalType::List { kind: source, element } = nonnull(logical) else { return Ok(None); };
                let Some((field, array, range)) = cell.sequence_range() else { return Ok(None); };
                if source != kind || width.is_some_and(|width| width != range.len()) { return Ok(None); }
                for row in range {
                    if **element == LogicalType::Nothing { return Ok(None); }
                    let Some(mut row) = child_row(ArrowCell { field, array, row }, element) else { return Ok(None); };
                    if *kind == ListKind::TimestampTzRange {
                        let OutputRow::Scalar(_, scalar @ ScalarType::Timestamp { .. }) = &mut row else { return Ok(None); };
                        *scalar = ScalarType::Timestamp {
                            unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()),
                        };
                    }
                    selections.push(row);
                }
                valid.append_non_null();
            }
            row if matches!(row, OutputRow::Padding) || matches!(row, OutputRow::Null) && field.is_nullable() => {
                selections.extend(std::iter::repeat_n(OutputRow::Padding, width.unwrap_or(0)));
                valid.append_null();
            }
            _ => return Ok(None),
        }
        if width.is_none() {
            let Ok(offset) = i32::try_from(selections.len()) else { return Ok(None); };
            offsets.push(offset);
        }
    }
    let Some(values) = gather(child, selections)? else { return Ok(None); };
    if matches!(field.data_type(), DataType::Map(_, _))
        && (values.null_count() != 0 || values.as_struct().column(0).logical_null_count() != 0)
    {
        return Ok(None);
    }
    let mut data = new_empty_array(field.data_type()).to_data().into_builder()
        .len(rows.len()).nulls(valid.finish()).child_data(vec![values.to_data()]);
    if width.is_none() {
        data = data.buffers(vec![ScalarBuffer::from(offsets).into_inner()]);
    }
    Ok(data.build().ok().map(make_array))
}

fn gather_vectors(field: &Field, rows: &[OutputRow<'_>]) -> Result<Option<ArrayRef>> {
    let (child, width) = match field.data_type() {
        DataType::List(child) => (child, None),
        DataType::FixedSizeList(child, width) => (child, Some(*width)),
        _ => return Ok(None),
    };
    let has_masks = matches!(child.data_type(), DataType::Union(_, UnionMode::Dense))
        && rows.iter().any(|row| matches!(row, OutputRow::Vector(vector, _)
            if matches!(&vector.values, FloatValues::Packed(ListView::Union(_)))
                && vector.components().any(|value| matches!(value, Component::MaskedF32(_) | Component::MaskedF64(_)))));
    // A packed union can keep a numeric tag even when its selected child is null.
    if has_masks && rows.iter().any(|row| matches!(row, OutputRow::Vector(vector, _)
        if !matches!(&vector.values, FloatValues::Packed(ListView::Union(array))
            if array.data_type() == child.data_type()
                && matches!(child.data_type(), DataType::Union(fields, _) if fields.iter()
                    .all(|(_, field)| field.data_type() == &DataType::Null || !field.is_nullable())))))
    {
        return Ok(None);
    }

    let mut validity = NullBufferBuilder::new(rows.len());
    let mut offsets = vec![0_i32];
    let mut count = 0usize;
    for row in rows {
        match row {
            OutputRow::Vector(vector, _) if vector.width == width => {
                count = count
                    .checked_add(vector.range.len())
                    .ok_or_else(size_error)?;
                validity.append_non_null();
            }
            row if matches!(row, OutputRow::Padding) || matches!(row, OutputRow::Null) && field.is_nullable() => {
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
            OutputRow::Vector(vector, _) => Some(vector),
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
    }).map(|value| match value {
        Component::MaskedF32(_) | Component::MaskedF64(_) if !has_masks => Component::Null,
        value => value,
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

fn scalar_leg(field: &Field, scalar: &ScalarType) -> bool {
    from_field(field).is_ok_and(|logical| nonnull(&logical) == &LogicalType::Scalar(scalar.clone()))
}

fn uses_native_vectors(field: &Field, rows: &[OutputRow<'_>]) -> bool {
    vector_width(field).is_some() && !rows.iter().any(|row| matches!(row, OutputRow::Nested(..)))
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
                    Component::MaskedF32(value) => {
                        let offset = f32s.len();
                        f32s.append_values(&[value], &[false]);
                        (f32_id, offset)
                    }
                    Component::MaskedF64(value) => {
                        let offset = f64s.len();
                        f64s.append_values(&[value], &[false]);
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
