use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, NullArray, PrimitiveArray, UnionArray, builder::PrimitiveBuilder, types::{ArrowPrimitiveType, Int8Type, Int16Type}};
use arrow_schema::{DataType, Field, UnionFields, UnionMode};
use arrow_select::{interleave::interleave, take::take};
use rtdb_types::{ListValue, Result};

use super::SchemaKind;

pub(super) fn collect<T: ArrowPrimitiveType>(count: usize, cells: impl IntoIterator<Item = Result<Option<T::Native>>>) -> Result<ListValue> {
    let mut builder = PrimitiveBuilder::<T>::with_capacity(count);
    for cell in cells { builder.append_option(cell?); }
    Ok(pack_primitive(builder.finish()))
}

pub(super) fn pack_primitive<T: ArrowPrimitiveType>(array: PrimitiveArray<T>) -> ListValue {
    let name = primitive_name(array.data_type()).expect("Connect numeric primitive");
    if array.null_count() == array.len() {
        return pack(Field::new("null", DataType::Null, true), Arc::new(NullArray::new(array.len())));
    }
    let field = Field::new(name, array.data_type().clone(), array.null_count() != 0);
    pack(field, Arc::new(array))
}

fn primitive_name(data_type: &DataType) -> Option<&'static str> {
    Some(match data_type {
        DataType::Int8 => "i8", DataType::Int16 => "i16", DataType::Int32 => "i32", DataType::Int64 => "i64",
        DataType::UInt8 => "u8", DataType::UInt16 => "u16", DataType::UInt32 => "u32", DataType::UInt64 => "u64",
        DataType::Float16 => "f16", DataType::Float32 => "f32", DataType::Float64 => "f64",
        DataType::Null => "null",
        _ => return None,
    })
}

pub(super) fn pack(field: Field, array: ArrayRef) -> ListValue {
    ListValue::from_numeric_arrow(Arc::new(field), array).expect("validated numeric array layout")
}

pub(super) fn primitive_type(kind: &SchemaKind) -> Option<DataType> {
    Some(match kind {
        SchemaKind::I8 => DataType::Int8,
        SchemaKind::I16 => DataType::Int16,
        SchemaKind::I32 => DataType::Int32,
        SchemaKind::I64 => DataType::Int64,
        SchemaKind::F32 => DataType::Float32,
        SchemaKind::F64 => DataType::Float64,
        _ => return None,
    })
}

pub(super) fn plain_matches(list: &ListValue, kind: &SchemaKind, optional: bool) -> bool {
    let Some(array) = list.array() else { return false; };
    let Some(expected) = primitive_type(kind) else { return false; };
    array.is_empty() || (optional && *array.data_type() == DataType::Null)
        || (*array.data_type() == expected && (optional || array.null_count() == 0))
}

pub(super) fn narrow_ints(list: &ListValue, target: &SchemaKind) -> Option<ListValue> {
    let (field, array) = list.packed_column()?;
    let target = primitive_type(target)?;
    if !matches!(target, DataType::Int8 | DataType::Int16) { return None; }
    if let Some(input) = array.as_any().downcast_ref::<Int32Array>() {
        return Some(if target == DataType::Int8 {
            pack_primitive(input.unary::<_, Int8Type>(|value| value as i8))
        } else {
            pack_primitive(input.unary::<_, Int16Type>(|value| value as i16))
        });
    }
    if let Some(union) = array.as_any().downcast_ref::<UnionArray>() {
        return narrow_union(union, &target);
    }
    if field.metadata().is_empty() && primitive_name(array.data_type()).is_some() {
        let nullable = *array.data_type() == DataType::Null || array.null_count() != 0;
        let field = Field::new(primitive_name(array.data_type())?, array.data_type().clone(), nullable);
        return Some(pack(field, array));
    }
    None
}

fn narrow_union(union: &UnionArray, target: &DataType) -> Option<ListValue> {
    let DataType::Union(fields, UnionMode::Dense) = union.data_type() else { return None; };
    let mut arrays = Vec::with_capacity(fields.len());
    let mut sources = [usize::MAX; 128];
    for (tag, field) in fields.iter() {
        if !field.metadata().is_empty() || primitive_name(field.data_type()).is_none() { return None; }
        let child = union.child(tag);
        let child = if let Some(input) = child.as_any().downcast_ref::<Int32Array>() {
            // Java narrowing wraps, including in mixed numeric children.
            if *target == DataType::Int8 {
                Arc::new(input.unary::<_, Int8Type>(|value| value as i8)) as ArrayRef
            } else {
                Arc::new(input.unary::<_, Int16Type>(|value| value as i16))
            }
        } else { Arc::clone(child) };
        sources[usize::try_from(tag).ok()?] = arrays.len();
        arrays.push(child);
    }
    let mut layouts = Vec::<(DataType, Vec<(usize, usize)>)>::new();
    let mut ids = Vec::with_capacity(union.len());
    let mut offsets = Vec::with_capacity(union.len());
    for row in 0..union.len() {
        let source = sources[union.type_id(row) as usize];
        let offset = union.value_offset(row);
        let array = arrays[source].as_ref();
        let ty = if *array.data_type() == DataType::Null || array.is_null(offset) { &DataType::Null } else { array.data_type() };
        let index = layouts.iter().position(|(layout, _)| layout == ty).unwrap_or(layouts.len());
        if index == layouts.len() { layouts.push((ty.clone(), Vec::new())); }
        let selections = &mut layouts[index].1;
        ids.push(index as i8);
        offsets.push(i32::try_from(selections.len()).ok()?);
        selections.push((source, offset));
    }
    let all_arrays = arrays.iter().map(AsRef::as_ref).collect::<Vec<_>>();
    let mut fields = Vec::with_capacity(layouts.len());
    let mut children = Vec::with_capacity(layouts.len());
    for (ty, selected) in layouts {
        let child = if ty == DataType::Null { Arc::new(NullArray::new(selected.len())) as ArrayRef }
        else {
            let sources = all_arrays.iter().enumerate().filter(|(_, array)| *array.data_type() == ty)
                .collect::<Vec<_>>();
            let indices = selected.into_iter().map(|(source, offset)| {
                Some((sources.iter().position(|(id, _)| *id == source)?, offset))
            }).collect::<Option<Vec<_>>>()?;
            let sources = sources.into_iter().map(|(_, array)| *array).collect::<Vec<_>>();
            interleave(&sources, &indices).ok()?
        };
        fields.push(Field::new(primitive_name(&ty)?, ty.clone(), ty == DataType::Null));
        children.push(child);
    }
    match fields.len() {
        0 => Some(pack(Field::new("null", DataType::Null, true), Arc::new(NullArray::new(0)))),
        1 => Some(pack(fields.pop()?, children.pop()?)),
        2 if fields.iter().any(|field| *field.data_type() == DataType::Null) => {
            let tagged = usize::from(*fields[0].data_type() == DataType::Null);
            let indices = ids.into_iter().zip(offsets)
                .map(|(tag, offset)| (tag as usize == tagged).then_some(offset)).collect::<Int32Array>();
            let child = take(children[tagged].as_ref(), &indices, None).ok()?;
            Some(pack(fields.swap_remove(tagged).with_nullable(true), child))
        }
        _ => {
            let fields = UnionFields::try_new(0..i8::try_from(fields.len()).ok()?, fields).ok()?;
            let array = UnionArray::try_new(fields, ids.into(), Some(offsets.into()), children).ok()?;
            Some(pack(Field::new("$data$", array.data_type().clone(), false), Arc::new(array)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::narrow_ints;
    use crate::connect::SchemaKind;
    use rtdb_types::{ListValue, Value};

    #[test]
    fn narrowing_packed_ints_wraps_and_preserves_canonical_null_and_mixed_legs() {
        for (kind, narrow) in [(SchemaKind::I8, (|v: i32| Value::I8(v as i8)) as fn(i32) -> Value),
            (SchemaKind::I16, |v| Value::I16(v as i16))] {
            let input = [Value::I32(i32::MAX), Value::Null, Value::I32(-32769)];
            let mixed = [Value::F32(f32::from_bits(0x7fc1_2345)), input[0].clone(), narrow(42), Value::Null, Value::F64(-0.0)];
            for values in [&input[..], &mixed[..]] {
                let original = ListValue::from(values.to_vec());
                let narrowed = narrow_ints(&original, &kind).unwrap();
                let expected = ListValue::from(values.iter().cloned().map(|value| match value {
                    Value::I32(value) => narrow(value),
                    value => value,
                }).collect::<Vec<_>>());
                let (field, array) = narrowed.packed_column().unwrap();
                let (expected_field, expected_array) = expected.packed_column().unwrap();
                assert_eq!(field, expected_field);
                assert_eq!(array.to_data(), expected_array.to_data());
            }
        }
    }
}
