use std::sync::Arc;

use arrow_array::{Array, ArrayRef, FixedSizeListArray, ListArray, NullArray, UnionArray,
    builder::{ArrayBuilder, Float32Builder, Float64Builder}};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, UnionFields};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{Anomaly, FloatComponent, ListKind, ListView, LogicalType, Result, ScalarType, Value};

use super::{Column, size_error};

pub(super) fn kind(value: &Value) -> Option<ListKind> {
    let (list, kind) = match value {
        Value::List(list) => (list, ListKind::List),
        Value::FixedSizeList(list) =>
            (list, ListKind::FixedSizeList(i32::try_from(list.len()).ok()?)),
        _ => return None,
    };
    let eligible = match list.view() {
        ListView::Scalars(_) => false,
        ListView::Values(values) => values.iter().all(|value| matches!(value, Value::Null | Value::F32(_) | Value::F64(_))),
        ListView::Numeric(_) => list.element_type().legs().iter().all(|leg| {
            matches!(leg, LogicalType::Null | LogicalType::Scalar(ScalarType::F32 | ScalarType::F64))
        }),
        _ => true,
    };
    eligible.then_some(kind)
}

pub(super) fn pack(
    values: &[Value], kind: ListKind, memory: &Arc<MemoryBudget>,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let mut logical = LogicalType::Nothing;
    let mut count = 0usize;
    let width = match kind { ListKind::FixedSizeList(width) => Some(width), _ => None };
    for value in values {
        let length = if value.is_null() {
            width.unwrap_or(0) as usize
        } else if let Value::List(list) | Value::FixedSizeList(list) = value {
            list.len()
        } else {
            return Ok(None);
        };
        count = count.checked_add(length).ok_or_else(size_error)?;
        logical = logical.join(&value.logical_type());
    }
    let field = Arc::new(rtdb_columnar::to_field("value", &logical)?);
    let item = match field.data_type() {
        DataType::List(item) | DataType::FixedSizeList(item, _) => item,
        _ => return Ok(None),
    };
    let union = match item.data_type() {
        DataType::Float32 | DataType::Float64 | DataType::Null => None,
        DataType::Union(fields, _) => Some(fields),
        _ => return Ok(None),
    };
    let union_overhead = if union.is_some() { 5 } else { 0 };
    let bound = count.checked_mul(16 + union_overhead)
        .and_then(|bytes| bytes.checked_add(values.len().checked_mul(8)?))
        .and_then(|bytes| bytes.checked_add(field.size().checked_mul(4)?))
        .and_then(|bytes| bytes.checked_add(8192)).ok_or_else(size_error)?;
    let mut reservation = memory.reserve(bound)?;

    let components = values.iter().flat_map(|value| {
        let view = value.list_view();
        let len = view.map_or(width.unwrap_or(0) as usize, |view| view.len());
        (0..len).map(move |index| {
            view.map_or(FloatComponent::Null, |view| view.typed_float(index).expect("checked float list"))
        })
    });
    let child: ArrayRef = match item.data_type() {
        DataType::Float32 => {
            let mut builder = Float32Builder::with_capacity(count);
            builder.extend(components.map(|component| match component {
                FloatComponent::F32(value) => Some(value),
                FloatComponent::Null => None,
                _ => unreachable!("checked float width"),
            }));
            Arc::new(builder.finish())
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(count);
            builder.extend(components.map(|component| match component {
                FloatComponent::F64(value) => Some(value),
                FloatComponent::Null => None,
                _ => unreachable!("checked float width"),
            }));
            Arc::new(builder.finish())
        }
        DataType::Union(fields, _) => dense_child(fields, components, count)?,
        DataType::Null => Arc::new(NullArray::new(count)),
        _ => unreachable!("checked float layout"),
    };
    let nulls = values.iter().any(Value::is_null)
        .then(|| NullBuffer::from(values.iter().map(|value| !value.is_null()).collect::<Vec<_>>()));
    // XTDB's union field is non-nullable even when one of its selected legs is null.
    let construction_item = if union.is_some() {
        Arc::new(item.as_ref().clone().with_nullable(true))
    } else {
        Arc::clone(item)
    };
    let array: ArrayRef = if let Some(width) = width {
        Arc::new(FixedSizeListArray::try_new_with_length(construction_item, width, child, nulls, values.len()).map_err(arrow_error)?)
    } else {
        let mut offsets = Vec::with_capacity(values.len() + 1);
        offsets.push(0);
        let mut end = 0;
        for value in values {
            let len = value.list_view().map_or(0, |view| view.len());
            end += len;
            offsets.push(i32::try_from(end).map_err(|_| Anomaly::incorrect("rtdb/list-offset-overflow", "List children exceed i32 offsets").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded))?);
        }
        Arc::new(ListArray::try_new(construction_item, OffsetBuffer::new(ScalarBuffer::from(offsets)), child, nulls).map_err(arrow_error)?)
    };
    let array = if array.data_type() == field.data_type() {
        array
    } else {
        let data = array.to_data().into_builder().data_type(field.data_type().clone()).build().map_err(arrow_error)?;
        arrow_array::make_array(data)
    };
    let retained = array.get_array_memory_size().checked_add(field.size()).ok_or_else(size_error)?;
    let custody = Arc::get_mut(&mut reservation).expect("the evaluated column owns its array");
    if retained > custody.bytes() { custody.grow_to(retained)?; } else { custody.shrink_to(retained)?; }
    Ok(Some((Column::Arrow { array, field, logical: Some(logical) }, reservation)))
}

fn dense_child(
    fields: &UnionFields, components: impl Iterator<Item = FloatComponent>, count: usize,
) -> Result<ArrayRef> {
    let id = |ty: &DataType| fields.iter().find_map(|(id, field)| (field.data_type() == ty).then_some(id));
    let f32_id = id(&DataType::Float32).expect("checked float union");
    let f64_id = id(&DataType::Float64).expect("checked float union");
    let null_id = id(&DataType::Null);
    let padding_id = fields.iter().next().expect("checked float union").0;
    let mut f32s = Float32Builder::with_capacity(count);
    let mut f64s = Float64Builder::with_capacity(count);
    let mut nulls = 0;
    let mut tags = Vec::with_capacity(count);
    let mut offsets = Vec::with_capacity(count);
    for component in components {
        let (tag, offset) = match component {
            FloatComponent::F32(value) => {
                f32s.append_value(value);
                (f32_id, f32s.len() - 1)
            }
            FloatComponent::F64(value) => {
                f64s.append_value(value);
                (f64_id, f64s.len() - 1)
            }
            FloatComponent::Null => match null_id {
                Some(null_id) => {
                    nulls += 1;
                    (null_id, nulls - 1)
                }
                // A null fixed-size row still occupies child slots in the first union leg.
                None if padding_id == f32_id => {
                    f32s.append_null();
                    (f32_id, f32s.len() - 1)
                }
                None => {
                    f64s.append_null();
                    (f64_id, f64s.len() - 1)
                }
            },
        };
        tags.push(tag);
        offsets.push(i32::try_from(offset).map_err(|_| Anomaly::incorrect("rtdb/union-offset-overflow", "Union child exceeds i32 offsets").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded))?);
    }
    let children = fields.iter().map(|(_, field)| -> ArrayRef {
        match field.data_type() {
            DataType::Float32 => Arc::new(f32s.finish()),
            DataType::Float64 => Arc::new(f64s.finish()),
            DataType::Null => Arc::new(NullArray::new(nulls)),
            _ => unreachable!("checked float union"),
        }
    }).collect();
    Ok(Arc::new(UnionArray::try_new(fields.clone(), tags.into(), Some(offsets.into()), children).map_err(arrow_error)?))
}

fn arrow_error(error: arrow_schema::ArrowError) -> Anomaly {
    Anomaly::incorrect("rtdb/invalid-arrow", error.to_string())
}
