use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{Schema, SchemaRef};
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
    let mut decoded_bound = multiply(batch.batch().num_rows(), multiply(declared.fields().len(), std::mem::size_of::<Value>())?)?;
    for position in positions.iter().flatten() {
        for row in 0..batch.batch().num_rows() { decoded_bound = add(decoded_bound, estimate_read_value_bytes(source.field(*position), batch.batch().column(*position).as_ref(), row)?)?; }
    }
    let decoded = memory.reserve(add(decoded_bound, 4096)?)?;
    let mut values = Vec::with_capacity(declared.fields().len());
    let mut logical = Vec::with_capacity(declared.fields().len());
    let mut fields = Vec::with_capacity(declared.fields().len());
    for (field, position) in declared.fields().iter().zip(&positions) {
        let target = from_field(field)?;
        let source_type = position.map(|position| from_field(source.field(position))).transpose()?.unwrap_or(L::Null);
        if position.is_none() && !target.nullable() { return Err(fault("page-missing-non-nullable", "Page lacks a required declared field")); }
        let ty = layout(&target, &source_type)?;
        let column = (0..batch.batch().num_rows()).map(|row| match position {
            Some(position) => read_value(source.field(*position), batch.batch().column(*position).as_ref(), row),
            None => Ok(Value::Null),
        }).collect::<Result<Vec<_>>>()?;
        let physical = to_field(field.name(), &ty)?.with_metadata(field.metadata().clone());
        fields.push(physical); logical.push(ty); values.push(column);
    }
    let bound = logical.iter().zip(&values).try_fold(schema_bound, |bytes, (ty, values)| add(bytes, multiply(estimate_builder_bytes(ty, values)?, 2)?))?;
    let output = memory.reserve(bound)?;
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(fields.len());
    for (((ty, values), field), target) in logical.iter().zip(&values).zip(&mut fields).zip(declared.fields()) {
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
