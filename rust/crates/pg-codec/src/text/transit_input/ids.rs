use std::sync::Arc;

use arrow_array::{UnionArray, builder::{
    ArrayBuilder, FixedSizeBinaryBuilder, Int8Builder, Int16Builder, Int32Builder, Int64Builder,
    StringBuilder, make_builder,
}};
use arrow_schema::DataType;
use rtdb_columnar::to_field;
use rtdb_types::{ListValue, LogicalType, ScalarType, Value};

use super::{Input, super::{cast_transit_number, parse_uuid, uuid_words}};

pub(super) fn pack(items: &[Input]) -> Option<ListValue> {
    use ScalarType as S;

    let mut legs = Vec::new();
    let mut text_bytes = [0usize; 2];
    for item in items {
        let (ty, bytes) = id_type(item)?;
        if let S::Utf8 | S::Keyword = ty {
            let total = &mut text_bytes[usize::from(ty == S::Keyword)];
            *total = total.checked_add(bytes)?;
            i32::try_from(*total).ok()?;
        }
        if !legs.contains(&ty) { legs.push(ty); }
    }
    if !legs.iter().any(|ty| matches!(ty, S::Utf8 | S::Keyword | S::Uuid)) { return None; }
    i32::try_from(items.len()).ok()?;

    let ty = LogicalType::from_legs(legs.iter().cloned().map(LogicalType::Scalar));
    let field = Arc::new(to_field("value", &ty).ok()?);
    let array = if let DataType::Union(fields, _) = field.data_type() {
        let mut children = fields.iter().map(|(_, field)| make_builder(field.data_type(), 0)).collect::<Vec<_>>();
        let mut tags = Vec::with_capacity(items.len());
        let mut offsets = Vec::with_capacity(items.len());
        for item in items {
            let (ty, _) = id_type(item)?;
            let leg = legs.iter().position(|candidate| *candidate == ty)?;
            let child = &mut children[leg];
            offsets.push(i32::try_from(child.len()).ok()?);
            tags.push(i8::try_from(leg).ok()?);
            append(child.as_mut(), item)?;
        }
        let children = children.iter_mut().map(|child| child.finish()).collect();
        Arc::new(UnionArray::try_new(fields.clone(), tags.into(), Some(offsets.into()), children).ok()?)
    } else {
        let mut builder = make_builder(field.data_type(), items.len());
        for item in items { append(builder.as_mut(), item)?; }
        builder.finish()
    };
    ListValue::from_entity_id_arrow(field, array)
}

fn id_type(input: &Input) -> Option<(ScalarType, usize)> {
    use ScalarType as S;
    Some(match input {
        Input::String(text) => (S::Utf8, text.len()),
        Input::Keyword(name) => (S::Keyword, name.len()),
        Input::Integer(_) => (S::I64, 0),
        Input::Tagged(tag, _) => (match tag.as_str() {
            "u" => S::Uuid,
            "i8" => S::I8,
            "i16" => S::I16,
            "i32" => S::I32,
            "i64" => S::I64,
            _ => return None,
        }, 0),
        _ => return None,
    })
}

fn append(builder: &mut dyn ArrayBuilder, input: &Input) -> Option<()> {
    macro_rules! push {
        ($builder:ty, $value:expr) => { builder.as_any_mut().downcast_mut::<$builder>()?.append_value($value) };
    }
    match input {
        Input::String(text) | Input::Keyword(text) => push!(StringBuilder, text),
        Input::Integer(value) => push!(Int64Builder, *value),
        Input::Tagged(tag, representation) if tag == "u" => {
            let bytes = match representation.as_ref() {
                Input::String(text) => parse_uuid(text).ok()?,
                Input::Array(words) => {
                    let [Input::Integer(high), Input::Integer(low)] = words.as_slice() else { return None; };
                    uuid_words(*high, *low)
                }
                _ => return None,
            };
            builder.as_any_mut().downcast_mut::<FixedSizeBinaryBuilder>()?.append_value(bytes).ok()?;
        }
        Input::Tagged(tag, representation) => {
            match cast_transit_number(tag, representation.as_ref().clone().into_value().ok()?).ok()? {
                Value::I8(value) => push!(Int8Builder, value),
                Value::I16(value) => push!(Int16Builder, value),
                Value::I32(value) => push!(Int32Builder, value),
                Value::I64(value) => push!(Int64Builder, value),
                _ => return None,
            }
        }
        _ => return None,
    }
    Some(())
}
