use std::sync::Arc;
use arrow_array::{Array, ArrayRef, BooleanArray, NullArray, UnionArray, make_array};
use arrow_buffer::ScalarBuffer;
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, UnionFields, UnionMode};
use rtdb_types::{LogicalType as L, Result};
use crate::{from_field, leg_name, to_field};

pub(super) fn additional_bytes(data: &ArrayData, source: &Field, target: &Field) -> Result<usize> {
    let null_bytes = |target: &Field| {
        crate::estimate_builder_bytes_refs(&from_field(target)?.maybe(), std::iter::repeat_n(&rtdb_types::Value::Null, data.len()))
    };
    match (source.data_type(), target.data_type()) {
        (source_type, DataType::Union(targets, UnionMode::Dense)) => {
            let tags = super::multiply(data.len(), 64)?;
            let children = match source_type {
                DataType::Union(sources, UnionMode::Dense) => {
                    targets.iter().try_fold(0, |bytes, (_, target)| {
                        let name = leg_name(&from_field(target)?)?;
                        let Some((index, _, source)) = union_leg(sources, &name) else { return Ok(bytes) };
                        super::add(bytes, additional_bytes(&data.child_data()[index], source, target)?)
                    })?
                }
                _ => {
                    let copy = data.get_slice_memory_size().map_err(crate::arrow_error)?;
                    if let Some(name) = monomorphic_leg(source)?
                        && let Some((_, _, target)) = union_leg(targets, &name) {
                        super::add(copy, additional_bytes(data, source, target)?)?
                    } else { copy }
                }
            };
            super::add(tags, children)
        }
        (DataType::Null, _) => null_bytes(target),
        (DataType::Struct(source), DataType::Struct(target)) => target.iter().try_fold(0, |bytes, field| {
            let extra = match source.iter().position(|original| original.name() == field.name()) {
                Some(index) => additional_bytes(&data.child_data()[index], &source[index], field)?,
                None => null_bytes(field)?,
            };
            super::add(bytes, extra)
        }),
        (DataType::List(source), DataType::List(target))
        | (DataType::FixedSizeList(source, _), DataType::FixedSizeList(target, _))
        | (DataType::Map(source, _), DataType::Map(target, _)) => additional_bytes(&data.child_data()[0], source, target),
        _ => Ok(0),
    }
}

pub(super) fn relayout(array: ArrayRef, source: &Field, target: &Field) -> Result<(Field, ArrayRef)> {
    if let DataType::Union(fields, UnionMode::Dense) = target.data_type() {
        return union(array, source, target, fields);
    }
    if source.data_type() == &DataType::Null && target.data_type() != &DataType::Null {
        let field = target.clone().with_nullable(true);
        return Ok((field, arrow_array::new_null_array(target.data_type(), array.len())));
    }
    let data = array.to_data();
    let mut children = Vec::with_capacity(data.child_data().len());
    let data_type = match (source.data_type(), target.data_type()) {
        (DataType::Struct(source_fields), DataType::Struct(target_fields)) => {
            let fields = target_fields.iter().map(|target| {
                let (field, child) = match source_fields.iter().position(|source| source.name() == target.name()) {
                    Some(index) => relayout(make_array(data.child_data()[index].clone()), &source_fields[index], target)?,
                    None => {
                        let null = Field::new(target.name(), DataType::Null, true);
                        relayout(Arc::new(NullArray::new(data.len())), &null, target)?
                    }
                };
                children.push(child.to_data());
                Ok(field)
            }).collect::<Result<Vec<_>>>()?;
            DataType::Struct(fields.into())
        }
        (DataType::List(source), DataType::List(target)) => {
            let (field, child) = relayout(make_array(data.child_data()[0].clone()), source, target)?;
            children.push(child.to_data());
            DataType::List(Arc::new(field))
        }
        (DataType::FixedSizeList(source, count), DataType::FixedSizeList(target, target_count)) if count == target_count => {
            let (field, child) = relayout(make_array(data.child_data()[0].clone()), source, target)?;
            children.push(child.to_data());
            DataType::FixedSizeList(Arc::new(field), *count)
        }
        (DataType::Map(source, sorted), DataType::Map(target, target_sorted)) if sorted == target_sorted => {
            let (field, child) = relayout(make_array(data.child_data()[0].clone()), source, target)?;
            children.push(child.to_data());
            DataType::Map(Arc::new(field), *sorted)
        }
        (DataType::RunEndEncoded(source_ends, source_values), DataType::RunEndEncoded(target_ends, target_values)) => {
            let (ends, ends_array) = relayout(make_array(data.child_data()[0].clone()), source_ends, target_ends)?;
            let (values, values_array) = relayout(make_array(data.child_data()[1].clone()), source_values, target_values)?;
            children.extend([ends_array.to_data(), values_array.to_data()]);
            DataType::RunEndEncoded(Arc::new(ends), Arc::new(values))
        }
        (source, target) if source == target => target.clone(),
        (DataType::Int8, DataType::UInt8) | (DataType::Int16, DataType::UInt16)
        | (DataType::Int32, DataType::UInt32) | (DataType::Int64, DataType::UInt64) => target.data_type().clone(),
        _ => return Err(incompatible()),
    };
    let field = target.clone().with_data_type(data_type.clone()).with_nullable(target.is_nullable() || source.is_nullable());
    let data = data.into_builder().data_type(data_type).child_data(children).build().map_err(crate::arrow_error)?;
    Ok((field, make_array(data)))
}

fn union(array: ArrayRef, source: &Field, target: &Field, targets: &UnionFields) -> Result<(Field, ArrayRef)> {
    let canonical = array.as_any().downcast_ref::<UnionArray>();
    let source_fields = match source.data_type() { DataType::Union(fields, UnionMode::Dense) => Some(fields), _ => None };
    let source_leg = if source_fields.is_none() { monomorphic_leg(source)? } else { None };
    let valid = array.nulls().filter(|nulls| source_leg.is_some() && nulls.null_count() > 0);
    let mut fields = Vec::with_capacity(targets.len());
    let mut children = Vec::with_capacity(targets.len());
    let mut mappings = Vec::with_capacity(targets.len());
    let mut nonnull_id = None;
    let mut null_id = None;
    for (target_id, target_field) in targets.iter() {
        let logical = from_field(target_field)?;
        let name = leg_name(&logical)?;
        let (source_field, child) = if let Some(sources) = source_fields {
            if let Some((_, source_id, field)) = union_leg(sources, &name) {
                mappings.push((source_id, target_id));
                (field.as_ref().clone(), Arc::clone(canonical.ok_or_else(incompatible)?.child(source_id)))
            } else { (target_field.as_ref().clone(), arrow_array::new_empty_array(target_field.data_type())) }
        } else if logical == L::Null {
            null_id = Some(target_id);
            (to_field(target_field.name(), &L::Null)?, Arc::new(NullArray::new(array.len())) as ArrayRef)
        } else if source_leg.as_deref() == Some(name.as_str()) {
            nonnull_id = Some(target_id);
            let child = match valid {
                Some(valid) => arrow_select::filter::filter(array.as_ref(), &BooleanArray::new(valid.inner().clone(), None)).map_err(crate::arrow_error)?,
                None => Arc::clone(&array),
            };
            (source.clone().with_nullable(false), child)
        } else { (target_field.as_ref().clone(), arrow_array::new_empty_array(target_field.data_type())) };
        let (field, child) = relayout(child, &source_field, target_field)?;
        fields.push((target_id, field));
        children.push(child);
    }
    let mut ids = Vec::with_capacity(array.len());
    let mut offsets = Vec::with_capacity(array.len());
    let mut value_offset = 0;
    for row in 0..array.len() {
        let (id, offset) = if let Some(source) = canonical {
            let id = mappings.iter().find(|(id, _)| *id == source.type_id(row)).map(|(_, target)| *target).ok_or_else(incompatible)?;
            (id, source.value_offset(row))
        } else if array.data_type() == &DataType::Null || array.is_null(row) {
            (null_id.ok_or_else(incompatible)?, row)
        } else {
            let offset = if valid.is_some() { value_offset } else { row };
            value_offset += 1;
            (nonnull_id.ok_or_else(incompatible)?, offset)
        };
        ids.push(id);
        offsets.push(i32::try_from(offset).map_err(|_| super::fault("page-allocation-size", "Union child exceeds i32 offsets"))?);
    }
    let fields = UnionFields::try_new(fields.iter().map(|(id, _)| *id), fields.iter().map(|(_, field)| field.clone())).map_err(crate::arrow_error)?;
    let array = Arc::new(UnionArray::try_new(fields.clone(), ScalarBuffer::from(ids), Some(ScalarBuffer::from(offsets)), children).map_err(crate::arrow_error)?);
    Ok((target.clone().with_data_type(DataType::Union(fields, UnionMode::Dense)), array))
}

fn monomorphic_leg(field: &Field) -> Result<Option<String>> {
    from_field(field)?.legs().into_iter().find(|leg| !matches!(leg, L::Null | L::Nothing)).as_ref().map(leg_name).transpose()
}

fn union_leg<'a>(fields: &'a UnionFields, name: &str) -> Option<(usize, i8, &'a Arc<Field>)> {
    fields.iter().enumerate().find_map(|(index, (id, field))| {
        from_field(field).and_then(|logical| leg_name(&logical)).is_ok_and(|leg| leg == name).then_some((index, id, field))
    })
}

fn incompatible() -> rtdb_types::Anomaly { super::fault("page-outside-declared-type", "Page physical layout differs from its declared type") }
