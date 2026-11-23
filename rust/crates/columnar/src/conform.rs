use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rtdb_types::{Anomaly, LogicalType as L, Result, Value};

use crate::{MemoryBudget, MemoryReservation, OwnedBatch, copy_array, estimate_builder_bytes, estimate_read_value_bytes, from_field, leg_name, read_value, to_field};

mod physical;

/// A page in a declared wire layout, retaining its schema and allocation charges.
/// The safe Rust batch marks null-bearing fields nullable; the declared wire schema preserves the caller's flags.
/// Integer buffers may retain signed execution bits under a same-width unsigned declared field.
/// Decimal precision, scale, and width must match an admitted declared leg.
pub struct DeclaredBatch { batch: OwnedBatch, declared: SchemaRef, _reservation: Arc<MemoryReservation> }

impl DeclaredBatch {
    pub fn batch(&self) -> &RecordBatch { self.batch.batch() }
    pub fn declared_schema(&self) -> &SchemaRef { &self.declared }
}

/// Conforms one page to the declared field order, preserving the reference ConformingUnloader's NULL promotion.
/// Reserves decoded cells, schema copies, and builders before creating them.
/// Extra source fields, missing required fields, and incompatible nonnull values are errors.
/// Declared nested field order, metadata and dense union type IDs are retained.
/// Exact-schema pages retain their existing physical layout without rebuilding.
pub fn conform_batch(batch: OwnedBatch, declared: &Schema, memory: &Arc<MemoryBudget>) -> Result<DeclaredBatch> {
    let metadata_bound = declared.metadata().iter().try_fold(multiply(declared.metadata().capacity(), std::mem::size_of::<(String, String)>())?, |bytes, (key, value)| add(bytes, add(key.capacity(), value.capacity())?))?;
    let schema_bound = declared.fields().iter().chain(batch.batch().schema().fields()).try_fold(add(8192, multiply(metadata_bound, 64)?)?, |bytes, field| add(bytes, multiply(field.size(), 4)?))?;
    let reservation = memory.reserve(schema_bound)?;
    let declared = Arc::new(declared.clone());
    if batch.batch().schema().as_ref() == declared.as_ref() { return Ok(DeclaredBatch { batch, declared, _reservation: reservation }); }
    let source = batch.batch().schema();
    if source.fields().iter().any(|field| !declared.fields().iter().any(|target| target.name() == field.name())) {
        return Err(fault("page-outside-declared-type", "Page contains a field outside its declared schema"));
    }
    let positions = declared.fields().iter().enumerate().map(|(index, field)| {
        if source.fields().get(index).is_some_and(|candidate| candidate.name() == field.name()) { Some(index) }
        else { source.fields().iter().rposition(|candidate| candidate.name() == field.name()) }
    }).collect::<Vec<_>>();
    let direct = declared.fields().iter().zip(&positions).map(|(target, position)| {
        let position = (*position)?;
        let field = source.field(position);
        let array = batch.batch().column(position);
        let direct = conform_float_list(array, field, target)?;
        let physical = target.as_ref().clone().with_nullable(target.is_nullable() || field.is_nullable());
        Some((physical, direct))
    }).collect::<Vec<_>>();
    let mut decoded_bound = multiply(batch.batch().num_rows(), multiply(declared.fields().len(), std::mem::size_of::<Value>())?)?;
    for (position, direct) in positions.iter().zip(&direct) {
        let Some(position) = position.filter(|_| direct.is_none()) else { continue; };
        for row in 0..batch.batch().num_rows() { decoded_bound = add(decoded_bound, estimate_read_value_bytes(source.field(position), batch.batch().column(position).as_ref(), row)?)?; }
    }
    let decoded = memory.reserve(add(decoded_bound, 4096)?)?;
    let mut values = Vec::with_capacity(declared.fields().len());
    let mut fields = Vec::with_capacity(declared.fields().len());
    for ((field, position), direct) in declared.fields().iter().zip(&positions).zip(&direct) {
        if let Some((physical, _)) = direct {
            fields.push(physical.clone());
            values.push(None);
            continue;
        }
        let target = from_field(field)?;
        let source_type = position.map(|position| from_field(source.field(position))).transpose()?.unwrap_or(L::Null);
        if position.is_none() && !target.nullable() { return Err(fault("page-missing-non-nullable", "Page lacks a required declared field")); }
        let ty = layout(&target, &source_type)?;
        let column = (0..batch.batch().num_rows()).map(|row| match position {
            Some(position) => read_value(source.field(*position), batch.batch().column(*position).as_ref(), row),
            None => Ok(Value::Null),
        }).collect::<Result<Vec<_>>>()?;
        let physical = to_field(field.name(), &ty)?.with_metadata(field.metadata().clone());
        fields.push(physical); values.push(Some((ty, column)));
    }
    let bound = values.iter().zip(&direct).try_fold(schema_bound, |bytes, (values, direct)| {
        match (values, direct) {
            (_, Some((_, array))) => add(bytes, array.get_array_memory_size()),
            (Some((ty, values)), None) => add(bytes, multiply(estimate_builder_bytes(ty, values)?, 2)?),
            _ => Err(fault("page-outside-declared-type", "Page conformer lost a field")),
        }
    })?;
    let output = memory.reserve(bound)?;
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(fields.len());
    for (((values, field), target), direct) in values.iter().zip(&mut fields).zip(declared.fields()).zip(direct) {
        if let Some((_, array)) = direct { arrays.push(array); continue; }
        let (ty, values) = values.as_ref().ok_or_else(|| fault("page-outside-declared-type", "Page conformer lost a field"))?;
        let (physical, array) = physical::relayout(copy_array(ty, values)?, field, target)?;
        *field = physical;
        arrays.push(array);
    }
    let schema = Arc::new(Schema::new(fields).with_metadata(declared.metadata().clone()));
    let physical = RecordBatch::try_new_with_options(schema, arrays, &RecordBatchOptions::new().with_row_count(Some(batch.batch().num_rows()))).map_err(crate::arrow_error)?;
    let batch = OwnedBatch::with_reservation(physical, output)?;
    drop(values); drop(decoded);
    Ok(DeclaredBatch { batch, declared, _reservation: reservation })
}

pub fn conform_float_list(array: &ArrayRef, source: &Field, target: &Field) -> Option<ArrayRef> {
    let list_items = match (source.data_type(), target.data_type()) {
        (DataType::List(left), DataType::List(right)) => (left, right),
        (DataType::FixedSizeList(left, width), DataType::FixedSizeList(right, target_width)) if width == target_width => (left, right),
        _ => return None,
    };
    let supported = |field: &Field| field.metadata().is_empty() && (matches!(field.data_type(), DataType::Float32 | DataType::Float64)
        || rtdb_types::ListValue::supports_float_union(field.data_type()));
    if !source.metadata().is_empty() || !target.metadata().is_empty()
        || !supported(list_items.0) || !supported(list_items.1)
        || (!target.is_nullable() && array.null_count() != 0)
    {
        return None;
    }
    if array.data_type() == target.data_type() { return Some(Arc::clone(array)); }
    physical::relayout(Arc::clone(array), source, target).ok()
        .and_then(|(_, array)| (array.data_type() == target.data_type()).then_some(array))
}

pub(crate) fn conform_float_components(array: ArrayRef, target: &Field) -> Option<ArrayRef> {
    if array.data_type() == target.data_type() { return Some(array); }
    let source = Field::new(target.name(), array.data_type().clone(), false);
    physical::relayout(array, &source, target).ok()
        .and_then(|(_, array)| (array.data_type() == target.data_type()).then_some(array))
}

fn layout(target: &L, source: &L) -> Result<L> {
    if target == source { return Ok(target.clone()); }
    if *source == L::Null {
        if matches!(target, L::Union(_)) && !target.nullable() { return Err(fault("page-outside-declared-type", "NULL has no leg in the declared union")); }
        return Ok(target.clone().maybe());
    }
    Ok(match (target, source) {
        (L::Scalar(rtdb_types::ScalarType::U8), L::Scalar(rtdb_types::ScalarType::I8))
        | (L::Scalar(rtdb_types::ScalarType::U16), L::Scalar(rtdb_types::ScalarType::I16))
        | (L::Scalar(rtdb_types::ScalarType::U32), L::Scalar(rtdb_types::ScalarType::I32))
        | (L::Scalar(rtdb_types::ScalarType::U64), L::Scalar(rtdb_types::ScalarType::I64)) => source.clone(),
        (L::Maybe(target), L::Maybe(source)) => layout(target, source)?.maybe(),
        (L::Maybe(target), source) => layout(target, source)?.maybe(),
        (target, L::Maybe(source)) if !matches!(target, L::Union(_)) => layout(target, source)?.maybe(),
        (L::Struct(target), L::Struct(source)) => {
            if source.keys().any(|name| !target.contains_key(name)) { return Err(fault("page-outside-declared-type", "Page struct contains an undeclared field")); }
            L::Struct(target.iter().map(|(name, ty)| {
                let actual = source.get(name);
                if actual.is_none() && !ty.nullable() { return Err(fault("page-missing-non-nullable", "Page struct lacks a required declared field")); }
                Ok((name.clone(), layout(ty, actual.unwrap_or(&L::Null))?))
            }).collect::<Result<_>>()?)
        }
        (L::List { kind, element }, L::List { kind: source_kind, element: source }) if kind == source_kind => L::List { kind: kind.clone(), element: Box::new(layout(element, source)?) },
        (L::Union(target), source) => {
            let source = source.legs();
            for actual in &source {
                let name = leg_name(actual)?;
                if !target.iter().any(|ty| leg_name(ty).is_ok_and(|target| target == name)) { return Err(fault("page-outside-declared-type", "Page type has no declared union leg")); }
            }
            let mut legs = Vec::with_capacity(target.len());
            for ty in target {
                let name = leg_name(ty)?;
                let actual = source.iter().find(|actual| leg_name(actual).is_ok_and(|actual| actual == name));
                legs.push(match actual { Some(actual) => layout(ty, actual)?, None => ty.clone() });
            }
            L::from_legs(legs)
        }
        (L::RunEndEncoded { width, value }, L::RunEndEncoded { value: source, .. }) => L::RunEndEncoded { width: *width, value: Box::new(layout(value, source)?) },
        (L::RunEndEncoded { width, value }, source) => L::RunEndEncoded { width: *width, value: Box::new(layout(value, source)?) },
        (target, L::RunEndEncoded { value, .. }) => layout(target, value)?,
        _ => return Err(fault("page-outside-declared-type", "Page type differs from its declared type")),
    })
}

fn fault(code: &str, message: &str) -> Anomaly { Anomaly::fault(format!("xtdb/{code}"), message) }
fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(|| fault("page-allocation-size", "Page allocation exceeds addressable memory")) }
fn multiply(value: usize, factor: usize) -> Result<usize> { value.checked_mul(factor).ok_or_else(|| fault("page-allocation-size", "Page allocation exceeds addressable memory")) }

#[cfg(test)]
mod fixed_float_tests {
    use super::*;
    use arrow_array::{Array, FixedSizeListArray, Float32Array, Int64Array};

    #[test]
    fn conformed_vector_shares_its_component_buffer_across_a_schema_change() {
        let memory = MemoryBudget::new(1 << 20);
        let original = Float32Array::from(vec![1.0, 2.0, 3.0, 4.0]);
        let address = original.values().as_ptr();
        let item = Arc::new(Field::new("item", DataType::Float32, false));
        let vectors = FixedSizeListArray::try_new(item, 2, Arc::new(original), None).unwrap();
        let source = Schema::new(vec![
            Field::new("embedding", vectors.data_type().clone(), false),
            Field::new("id", DataType::Int64, false),
        ]);
        let batch = RecordBatch::try_new(Arc::new(source), vec![Arc::new(vectors), Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let target = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("embedding", DataType::FixedSizeList(Arc::new(Field::new("element", DataType::Float32, true)), 2), true),
        ]);
        let result = conform_batch(OwnedBatch::new(batch, &memory).unwrap(), &target, &memory).unwrap();
        let list = result.batch().column(1).as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(values.values().as_ptr(), address);
    }
}
