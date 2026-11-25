use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, Float32Array, Float64Array, PrimitiveArray, UnionArray,
    cast::AsArray,
    types::{Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type},
};
use arrow_schema::{DataType, Field};
use rtdb_plan::CastTarget;
use rtdb_types::{ListValue, ListView, LogicalType, Result, ScalarType};

use crate::numeric::overflow;

pub(crate) fn cast(
    field: &Field, values: &dyn Array, target: &CastTarget, reserve: impl FnOnce(usize) -> Result<()>,
) -> Result<Option<ListValue>> {
    cast_with_backing(field, values, target, None, reserve).map(|output| output.map(|(list, _)| list))
}

pub(crate) fn cast_with_backing(
    field: &Field, values: &dyn Array, target: &CastTarget, backing: Option<usize>,
    reserve: impl FnOnce(usize) -> Result<()>,
) -> Result<Option<(ListValue, bool)>> {
    Ok(cast_child(field, values, target, backing, reserve)?.map(|(child, shared)| {
        let list = match child {
            FloatChild::F32(values) => ListValue::from_f32(values),
            FloatChild::F64(values) => ListValue::from_f64(values),
        };
        (list, shared)
    }))
}

enum FloatChild { F32(Float32Array), F64(Float64Array) }

fn cast_child(
    field: &Field, values: &dyn Array, target: &CastTarget, backing: Option<usize>,
    reserve: impl FnOnce(usize) -> Result<()>,
) -> Result<Option<(FloatChild, bool)>> {
    let Some((target, validate)) = float_target(target) else { return Ok(None); };
    let Some(source) = source_scalar(field, values) else { return Ok(None); };
    if source.is_empty() { return Ok(None); }
    if let Some(validate) = validate {
        for source in source { validate.validate_source(&LogicalType::Scalar(source))?; }
    }
    let (data_type, width) = if target == ScalarType::F32 { (DataType::Float32, 4) } else { (DataType::Float64, 8) };
    let payload = values.len().checked_mul(width).ok_or_else(overflow)?;
    let validity = values.len().div_ceil(8);
    let visible = if values.null_count() == 0 { payload } else { payload.checked_add(validity).ok_or_else(overflow)? };
    let shared = values.data_type() == &data_type && backing.is_some_and(|bytes| {
        bytes.max(values.get_buffer_memory_size()) <= visible.saturating_mul(2).saturating_add(4096)
    });
    let child = if shared {
        reserve(256)?;
        match target {
            ScalarType::F32 => FloatChild::F32(primitive_view::<Float32Type>(values)),
            _ => FloatChild::F64(primitive_view::<Float64Type>(values)),
        }
    } else {
        reserve(payload.checked_add(validity + 256).ok_or_else(overflow)?)?;
        match target {
            ScalarType::F32 => FloatChild::F32(convert::<Float32Type>(values, |number| number as f32)),
            _ => FloatChild::F64(convert::<Float64Type>(values, |number| number)),
        }
    };
    Ok(Some((child, shared)))
}

/// Returns the converted child and its observed element type for a one-row Arrow list binding.
/// When `backing` is supplied, the source buffers must already carry that complete backing charge.
/// The returned flag identifies a child that shares source buffers.
pub fn cast_bound_float_list(
    cell: rtdb_columnar::ArrowCell<'_>, target: &LogicalType, allocation: &crate::EvaluationScope,
    backing: Option<&Arc<rtdb_columnar::MemoryReservation>>,
) -> Result<Option<(LogicalType, ArrayRef, bool)>> {
    use rtdb_types::ListKind;

    let target = match target { LogicalType::Maybe(target) => target.as_ref(), target => target };
    let LogicalType::List { kind: ListKind::List, element } = target else { return Ok(None); };
    if !matches!(cell.sequence_kind(), Some(ListKind::List | ListKind::FixedSizeList(_))) || cell.is_null()? {
        return Ok(None);
    }
    let Some((field, values, range)) = cell.sequence_range() else { return Ok(None); };
    let values = values.slice(range.start, range.len());
    let target = CastTarget::fixed(element.as_ref().clone());
    let backing = backing.map(|lease| lease.bytes());
    Ok(cast_child(field, values.as_ref(), &target, backing, |bytes| allocation.reserve(bytes))?.map(|(child, shared)| {
        let (scalar, values): (_, ArrayRef) = match child {
            FloatChild::F32(values) => (ScalarType::F32, Arc::new(values)),
            FloatChild::F64(values) => (ScalarType::F64, Arc::new(values)),
        };
        let element = LogicalType::Scalar(scalar);
        let element = if values.null_count() == 0 { element } else { element.maybe() };
        (element, values, shared)
    }))
}

pub(crate) fn packed(list: &ListValue) -> Option<(Arc<Field>, ArrayRef)> {
    match list.view() {
        ListView::Values(_) => None,
        _ => list.packed_column(),
    }
}

fn float_target(target: &CastTarget) -> Option<(ScalarType, Option<&CastTarget>)> {
    let ty = match target {
        CastTarget::Fixed(ty) => ty,
        CastTarget::Checked { target: inner, .. } => match inner.as_ref() {
            CastTarget::Fixed(ty) => ty,
            _ => return None,
        },
        _ => return None,
    };
    if target.union_target().is_some() { return None; }
    match plain_type(ty) {
        LogicalType::Scalar(scalar @ (ScalarType::F32 | ScalarType::F64)) => Some((scalar.clone(), Some(target))),
        _ => None,
    }
}

fn source_scalar(field: &Field, values: &dyn Array) -> Option<Vec<ScalarType>> {
    if !field.metadata().is_empty() { return None; }
    let scalar = |ty: &DataType| Some(match ty {
        DataType::Int8 => ScalarType::I8,
        DataType::Int16 => ScalarType::I16,
        DataType::Int32 => ScalarType::I32,
        DataType::Int64 => ScalarType::I64,
        DataType::Float16 => ScalarType::F16,
        DataType::Float32 => ScalarType::F32,
        DataType::Float64 => ScalarType::F64,
        _ => return None,
    });
    match values.data_type() {
        _ if ListValue::supports_float_union(values.data_type()) => {
            let union = values.as_any().downcast_ref::<UnionArray>()?;
            let mut seen = Vec::with_capacity(2);
            for row in 0..union.len() {
                let child = union.child(union.type_id(row));
                if absent(child.as_ref(), union.value_offset(row)) { continue; }
                let ty = scalar(child.data_type())?;
                if !seen.contains(&ty) { seen.push(ty); }
            }
            Some(seen)
        }
        ty => {
            let scalar = scalar(ty)?;
            Some(if values.null_count() == values.len() { Vec::new() } else { vec![scalar] })
        }
    }
}

fn primitive_view<T: ArrowPrimitiveType>(values: &dyn Array) -> PrimitiveArray<T> {
    let values = values.as_primitive::<T>();
    let nulls = (values.null_count() != 0).then(|| values.nulls().expect("nullable primitive").clone());
    PrimitiveArray::new(values.values().clone(), nulls)
}

/// Maps each component through binary64, as `crate::numeric::number` reads it.
/// A component already of the target width is copied, as the scalar cast returns it unchanged.
fn convert<T: ArrowPrimitiveType>(values: &dyn Array, through: impl Fn(f64) -> T::Native) -> PrimitiveArray<T> {
    fn each<S: ArrowPrimitiveType, T: ArrowPrimitiveType>(values: &dyn Array, cast: impl Fn(S::Native) -> T::Native) -> PrimitiveArray<T> {
        values.as_primitive::<S>().iter().map(|value| value.map(&cast)).collect()
    }
    let same = |child: &dyn Array, row: usize| (child.data_type() == &T::DATA_TYPE).then(|| child.as_primitive::<T>().value(row));
    match values.data_type() {
        ty if *ty == T::DATA_TYPE => each::<T, T>(values, |number| number),
        DataType::Int8 => each::<Int8Type, T>(values, |number| through(f64::from(number))),
        DataType::Int16 => each::<Int16Type, T>(values, |number| through(f64::from(number))),
        DataType::Int32 => each::<Int32Type, T>(values, |number| through(f64::from(number))),
        DataType::Int64 => each::<Int64Type, T>(values, |number| through(number as f64)),
        DataType::Float16 => each::<Float16Type, T>(values, |number| through(number.to_f64())),
        DataType::Float32 => each::<Float32Type, T>(values, |number| through(f64::from(number))),
        DataType::Float64 => each::<Float64Type, T>(values, &through),
        _ => {
            let union = values.as_any().downcast_ref::<UnionArray>().expect("float union source");
            let view = ListView::Union(union);
            (0..union.len()).map(|row| {
                let child = union.child(union.type_id(row));
                let offset = union.value_offset(row);
                if absent(child.as_ref(), offset) { return None; }
                same(child.as_ref(), offset).or_else(|| view.float_component(row).map(&through))
            }).collect()
        }
    }
}

/// A Null-typed leg carries no validity buffer, so its components are absent by type.
fn absent(child: &dyn Array, row: usize) -> bool { child.data_type() == &DataType::Null || child.is_null(row) }

fn plain_type(ty: &LogicalType) -> &LogicalType {
    match ty { LogicalType::RunEndEncoded { value, .. } => plain_type(value), _ => ty }
}
