use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{Schema, SchemaRef};
use rtdb_types::{Anomaly, LogicalType as L, Result, Value};

use crate::{MemoryBudget, MemoryReservation, OwnedBatch, copy_evaluated_cells, estimate_read_value_bytes, from_field, leg_name, read_value, to_field};

mod physical;

/// Adapts compatible physical layouts, sharing value buffers when the target can use them.
/// The caller reserves the space reported by `arrow_relayout_bound`.
/// The returned field carries the actual nullability, including nested fields.
pub fn relayout_arrow_column(array: ArrayRef, source: &arrow_schema::Field, target: &arrow_schema::Field) -> Result<(arrow_schema::Field, ArrayRef)> {
    let target_type = from_field(target)?;
    let source_type = from_field(source)?;
    layout(&target_type, &source_type)?;
    physical::relayout(array, source, target, false)
}

/// Bounds the temporary and new allocations needed to relayout an already owned array.
pub fn arrow_relayout_bound(array: &dyn arrow_array::Array, source: &arrow_schema::Field, target: &arrow_schema::Field) -> Result<usize> {
    add(multiply(target.size(), 8)?, physical::additional_bytes(&array.to_data(), source, target, false)?)
}

/// Adapts persisted event layouts while preserving undefined cells in fields of bottom type.
pub fn relayout_event_column(array: ArrayRef, source: &arrow_schema::Field, target: &arrow_schema::Field) -> Result<(arrow_schema::Field, ArrayRef)> {
    let target_type = from_field(target)?;
    if from_field(source)?.unencoded().join(&target_type) != target_type {
        return Err(fault("page-outside-declared-type", "Page type differs from its declared event type"));
    }
    physical::relayout(array, source, target, true)
}

pub fn event_relayout_bound(array: &dyn arrow_array::Array, source: &arrow_schema::Field, target: &arrow_schema::Field) -> Result<usize> {
    add(multiply(target.size(), 8)?, physical::additional_bytes(&array.to_data(), source, target, true)?)
}

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
    let mut output = memory.reserve(schema_bound)?;
    let mut fields = Vec::with_capacity(declared.fields().len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(declared.fields().len());
    for (field, position) in declared.fields().iter().zip(positions) {
        let input = position.map(|position| (source.field(position), batch.batch().column(position)));
        let output = Arc::get_mut(&mut output).expect("Conformance owns the output reservation");
        if let Some((source_field, array)) = input && source_field == field.as_ref() {
            output.grow_to(add(output.bytes(), array.get_array_memory_size())?)?;
            fields.push(field.as_ref().clone());
            arrays.push(Arc::clone(array));
            continue;
        }
        let target = from_field(field)?;
        let source_type = input.map(|(field, _)| from_field(field)).transpose()?.unwrap_or(L::Null);
        if input.is_none() && !target.nullable() { return Err(fault("page-missing-non-nullable", "Page lacks a required declared field")); }
        let ty = layout(&target, &source_type)?;
        let absent;
        let nulls;
        let arrow_input = match input {
            Some(input) => input,
            None => {
                absent = arrow_schema::Field::new(field.name(), arrow_schema::DataType::Null, true);
                nulls = Arc::new(arrow_array::NullArray::new(batch.batch().num_rows())) as ArrayRef;
                (&absent, &nulls)
            }
        };
        let (source_field, array) = arrow_input;
        if let Some(collapse) = physical::collapse_bytes(array.as_ref(), source_field, field).ok().flatten()
            && let Ok(_gather) = memory.reserve(collapse)
            && let Some((leg, leg_field)) = physical::collapse_union(array, source_field, field).ok().flatten()
            && let Ok(bound) = arrow_relayout_bound(leg.as_ref(), &leg_field, field)
            && let Ok(_scratch) = memory.reserve(bound)
            && let Ok((physical, retained)) = physical::relayout(leg, &leg_field, field, false)
        {
            output.grow_to(add(output.bytes(), retained.get_array_memory_size())?)?;
            fields.push(physical);
            arrays.push(retained);
            continue;
        }
        let bound = arrow_relayout_bound(array.as_ref(), source_field, field)?;
        if let Ok(_scratch) = memory.reserve(bound)
            && let Ok((physical, retained)) = physical::relayout(Arc::clone(array), source_field, field, false)
        {
            output.grow_to(add(output.bytes(), retained.get_array_memory_size())?)?;
            fields.push(physical);
            arrays.push(retained);
            continue;
        }
        let rows = batch.batch().num_rows();
        let (column, _scratch) = copy_evaluated_cells(&ty, rows, memory,
            |row| match input {
                Some((field, array)) => estimate_read_value_bytes(field, array.as_ref(), row),
                None => Ok(0),
            },
            |row| match input {
                Some((field, array)) => read_value(field, array.as_ref(), row),
                None => Ok(Value::Null),
            },
        )?;
        let source = to_field(field.name(), &ty)?.with_metadata(field.metadata().clone());
        let _relayout = memory.reserve(arrow_relayout_bound(column.as_ref(), &source, field)?)?;
        let (physical, array) = physical::relayout(column, &source, field, false)?;
        output.grow_to(add(output.bytes(), array.get_array_memory_size())?)?;
        fields.push(physical);
        arrays.push(array);
    }
    let schema = Arc::new(Schema::new(fields).with_metadata(declared.metadata().clone()));
    let physical = RecordBatch::try_new_with_options(schema, arrays, &RecordBatchOptions::new().with_row_count(Some(batch.batch().num_rows()))).map_err(crate::arrow_error)?;
    let batch = OwnedBatch::with_reservation(physical, output)?;
    Ok(DeclaredBatch { batch, declared, _reservation: reservation })
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
