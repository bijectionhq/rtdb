use std::collections::BTreeMap;
use apache_avro::Schema;
use super::super::{Avro, AvroMap, MapKey};
use super::super::default_json::{DefaultJson as Json, Defaults};
use super::{Kind, Result, error, kind, nesting, overflow, physical};

pub(super) fn bound(schema: &Schema, value: &Json, names: &BTreeMap<String, Schema>, defaults: &Defaults, depth: usize) -> Result<usize> {
    nesting(depth)?;
    let schema = physical(super::super::resolve(schema, names)?);
    let add = |a: usize, b: usize| a.checked_add(b).ok_or_else(overflow);
    match schema {
        Schema::Record(record) => record.fields.iter().try_fold(4096usize, |bytes, field| {
            let value = value.get(&field.name).or_else(|| defaults.get(&record.name.fullname(None), &field.name)).ok_or_else(|| error("avro-resolution", "Record default has a missing field"))?;
            add(add(bytes, field.name.len())?, bound(&field.schema, value, names, defaults, depth + 1)?)
        }),
        Schema::Array(array) => value.as_array().ok_or_else(default_error)?.iter().try_fold(4096usize, |bytes, value| add(bytes, bound(&array.items, value, names, defaults, depth + 1)?)),
        Schema::Map(map) => value.as_object().ok_or_else(default_error)?.iter().try_fold(4096usize, |bytes, (key, value)| add(add(bytes, key.len())?, bound(&map.types, value, names, defaults, depth + 1)?)),
        Schema::Union(union) => add(4096, bound(union.variants().first().ok_or_else(default_error)?, value, names, defaults, depth + 1)?),
        Schema::Fixed(fixed) => add(4096, fixed.size.max(value.as_str().ok_or_else(default_error)?.len()).checked_mul(4).ok_or_else(overflow)?),
        Schema::String | Schema::Bytes | Schema::Enum(_) => add(4096, value.as_str().ok_or_else(default_error)?.len().checked_mul(4).ok_or_else(overflow)?),
        _ => Ok(4096),
    }
}

pub(super) fn read(schema: &Schema, value: &Json, names: &BTreeMap<String, Schema>, defaults: &Defaults, depth: usize) -> Result<Avro> {
    nesting(depth)?;
    let schema = physical(super::super::resolve(schema, names)?);
    Ok(match schema {
        Schema::Record(record) => Avro::Record(record.fields.iter().map(|field| {
            let value = value.get(&field.name).or_else(|| defaults.get(&record.name.fullname(None), &field.name)).ok_or_else(default_error)?;
            Ok((field.name.clone(), read(&field.schema, value, names, defaults, depth + 1)?))
        }).collect::<Result<_>>()?),
        Schema::Array(array) => Avro::Array(value.as_array().ok_or_else(default_error)?.iter().map(|value| read(&array.items, value, names, defaults, depth + 1)).collect::<Result<_>>()?),
        Schema::Map(map) => {
            let entries = value.as_object().ok_or_else(default_error)?;
            Avro::Map(AvroMap::new(entries.len(), entries.iter().map(|(key, value)| Ok((MapKey::Utf8(key.as_bytes().to_vec()), read(&map.types, value, names, defaults, depth + 1)?))).collect::<Result<_>>()?, map.attributes.get("avro.java.string").and_then(serde_json::Value::as_str) == Some("String")))
        }
        Schema::Union(union) => Avro::Union(0, Box::new(read(union.variants().first().ok_or_else(default_error)?, value, names, defaults, depth + 1)?)),
        Schema::Enum(enumeration) => {
            let symbol = value.as_str().ok_or_else(default_error)?;
            let index = enumeration.symbols.iter().position(|candidate| candidate == symbol).ok_or_else(default_error)?;
            Avro::Enum(index as u32, symbol.to_owned())
        }
        Schema::Fixed(fixed) => {
            let mut bytes = latin1(value.as_str().ok_or_else(default_error)?);
            bytes.resize(fixed.size, 0); Avro::Fixed(fixed.size, bytes)
        }
        _ => match kind(schema) {
            Kind::Null if value.is_null() => Avro::Null,
            Kind::Boolean => Avro::Boolean(value.as_bool().ok_or_else(default_error)?),
            Kind::Int => Avro::Int(value.int_value().ok_or_else(default_error)?),
            Kind::Long => Avro::Long(value.long_value().ok_or_else(default_error)?),
            Kind::Float => Avro::Float(value.double_value().ok_or_else(default_error)? as f32),
            Kind::Double => Avro::Double(value.double_value().ok_or_else(default_error)?),
            Kind::String => Avro::String(value.as_str().ok_or_else(default_error)?.to_owned()),
            Kind::Bytes => Avro::Bytes(latin1(value.as_str().ok_or_else(default_error)?)),
            _ => return Err(default_error()),
        },
    })
}
pub(in crate::connect::avro) fn allocation_bound(schema: &Schema, names: &BTreeMap<String, Schema>, defaults: &Defaults) -> Result<usize> {
    fn walk(schema: &Schema, names: &BTreeMap<String, Schema>, defaults: &Defaults, seen: &mut std::collections::BTreeSet<usize>, depth: usize) -> Result<usize> {
        nesting(depth)?; let schema = physical(super::super::resolve(schema, names)?);
        if !seen.insert(schema as *const Schema as usize) { return Ok(0); }
        let mut bytes = 4096usize;
        let mut add = |value: usize| -> Result<()> { bytes = bytes.checked_add(value).ok_or_else(overflow)?; Ok(()) };
        match schema {
            Schema::Record(record) => for field in &record.fields {
                if let Some(value) = defaults.get(&record.name.fullname(None), &field.name) { if let Ok(bound) = bound(&field.schema, value, names, defaults, depth + 1) { add(bound.checked_mul(8).ok_or_else(overflow)?)?; } }
                add(walk(&field.schema, names, defaults, seen, depth + 1)?)?;
            },
            Schema::Array(array) => add(walk(&array.items, names, defaults, seen, depth + 1)?)?,
            Schema::Map(map) => add(walk(&map.types, names, defaults, seen, depth + 1)?)?,
            Schema::Union(union) => for schema in union.variants() { add(walk(schema, names, defaults, seen, depth + 1)?)?; },
            _ => {}
        }
        Ok(bytes)
    }
    walk(schema, names, defaults, &mut Default::default(), 0)
}
fn latin1(text: &str) -> Vec<u8> { text.chars().map(|character| u8::try_from(u32::from(character)).unwrap_or(b'?')).collect() }
fn default_error() -> rtdb_types::Anomaly { error("avro-resolution", "Default does not match its Avro schema") }
