use std::sync::Arc;
use arrow_array::{Array, ArrayRef, NullArray, UnionArray, make_array};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Field, UnionFields, UnionMode};
use rtdb_types::{LogicalType as L, Result};
use crate::{copy_array, from_field, leg_name, to_field};

pub(super) fn relayout(array: ArrayRef, source: &Field, target: &Field) -> Result<(Field, ArrayRef)> {
    if let DataType::Union(fields, UnionMode::Dense) = target.data_type() {
        return union(array, source, target, fields);
    }
    let data = array.to_data();
    let mut children = Vec::with_capacity(data.child_data().len());
    let data_type = match (source.data_type(), target.data_type()) {
        (DataType::Struct(source_fields), DataType::Struct(target_fields)) => {
            let fields = target_fields.iter().map(|target| {
                let index = source_fields.iter().position(|source| source.name() == target.name()).ok_or_else(incompatible)?;
                let (field, child) = relayout(make_array(data.child_data()[index].clone()), &source_fields[index], target)?;
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
        _ => return Err(incompatible()),
    };
    let field = target.clone().with_data_type(data_type.clone()).with_nullable(target.is_nullable() || source.is_nullable());
    let data = data.into_builder().data_type(data_type).child_data(children).build().map_err(crate::arrow_error)?;
    Ok((field, make_array(data)))
}

fn union(array: ArrayRef, source: &Field, target: &Field, targets: &UnionFields) -> Result<(Field, ArrayRef)> {
    let canonical = array.as_any().downcast_ref::<UnionArray>();
    let source_fields = match source.data_type() { DataType::Union(fields, UnionMode::Dense) => Some(fields), _ => None };
    let source_leg = if source_fields.is_none() {
        from_field(source)?.legs().into_iter().find(|leg| !matches!(leg, L::Null | L::Nothing)).map(|leg| leg_name(&leg)).transpose()?
    } else { None };
    let mut fields = Vec::with_capacity(targets.len());
    let mut children = Vec::with_capacity(targets.len());
    let mut mappings = Vec::with_capacity(targets.len());
    let mut nonnull_id = None;
    let mut null_id = None;
    for (target_id, target_field) in targets.iter() {
        let logical = from_field(target_field)?;
        let name = leg_name(&logical)?;
        let (source_field, child) = if let Some(sources) = source_fields {
            if let Some((source_id, field)) = sources.iter().find(|(_, field)| from_field(field).and_then(|logical| leg_name(&logical)).is_ok_and(|source| source == name)) {
                mappings.push((source_id, target_id));
                (field.as_ref().clone(), Arc::clone(canonical.ok_or_else(incompatible)?.child(source_id)))
            } else { (to_field(target_field.name(), &logical)?, copy_array(&logical, &[])?) }
        } else if logical == L::Null {
            null_id = Some(target_id);
            (to_field(target_field.name(), &L::Null)?, Arc::new(NullArray::new(array.len())) as ArrayRef)
        } else if source_leg.as_deref() == Some(name.as_str()) {
            nonnull_id = Some(target_id);
            (source.clone(), Arc::clone(&array))
        } else { (to_field(target_field.name(), &logical)?, copy_array(&logical, &[])?) };
        let (field, child) = relayout(child, &source_field, target_field)?;
        fields.push((target_id, field));
        children.push(child);
    }
    let mut ids = Vec::with_capacity(array.len());
    let mut offsets = Vec::with_capacity(array.len());
    for row in 0..array.len() {
        let (id, offset) = if let Some(source) = canonical {
            let id = mappings.iter().find(|(id, _)| *id == source.type_id(row)).map(|(_, target)| *target).ok_or_else(incompatible)?;
            (id, source.value_offset(row))
        } else {
            (if array.is_null(row) { null_id } else { nonnull_id }.ok_or_else(incompatible)?, row)
        };
        ids.push(id);
        offsets.push(i32::try_from(offset).map_err(|_| super::fault("page-allocation-size", "Union child exceeds i32 offsets"))?);
    }
    let fields = UnionFields::try_new(fields.iter().map(|(id, _)| *id), fields.iter().map(|(_, field)| field.clone())).map_err(crate::arrow_error)?;
    let array = Arc::new(UnionArray::try_new(fields.clone(), ScalarBuffer::from(ids), Some(ScalarBuffer::from(offsets)), children).map_err(crate::arrow_error)?);
    Ok((target.clone().with_data_type(DataType::Union(fields, UnionMode::Dense)), array))
}

fn incompatible() -> rtdb_types::Anomaly { super::fault("page-outside-declared-type", "Page physical layout differs from its declared type") }
