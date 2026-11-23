use std::collections::BTreeMap;
use apache_avro::Schema;
use serde_json::Value as Json;
use rtdb_types::Result;
use super::{Avro, AvroMap, MapKey, AvroDatumPlan, AvroDatum, ConversionContext, error, resolve};
#[cfg(test)] #[path = "migration_tests.rs"] mod tests;

impl AvroDatumPlan {
    pub fn to_json_datum(&self, datum: AvroDatum, context: &mut ConversionContext) -> Result<Json> {
        self.retain_source(context)?;
        datum.plan.retain_source(context)?;
        let bound = super::resolution::value_bound(&datum.value, 0)?.checked_mul(2).and_then(|bytes| bytes.checked_add(datum.plan.schema_bytes.checked_mul(16)?)).ok_or_else(overflow)?;
        context.reserve(bound)?;
        let (schema, value) = unwrap(&datum.plan.root, datum.value, &datum.plan.names)?;
        if matches!(schema, Schema::Map(_)) {
            let inferred = inferred_schema(schema, &value, &datum.plan.names, 0)?;
            return encode_as(&inferred, schema, value, &datum.plan.names, 0);
        }
        encode(schema, value, &datum.plan.names, 0)
    }

    pub fn from_json_datum(self: &std::sync::Arc<Self>, value: Json, context: &mut ConversionContext) -> Result<AvroDatum> {
        self.retain_source(context)?;
        self.validate_reader(Some(self))?;
        context.reserve(json_bound(&value, 0)?.checked_add(self.schema_bytes.checked_mul(16).ok_or_else(overflow)?).ok_or_else(overflow)?)?;
        Ok(AvroDatum { value: decode(&self.root, &value, &self.names, 0)?, plan: std::sync::Arc::clone(self) })
    }
}

fn inferred_schema(schema: &Schema, value: &Avro, names: &BTreeMap<String, Schema>, depth: usize) -> Result<Schema> {
    nesting(depth)?;
    let (schema, value) = super::datum::selected(schema, value, names)?;
    Ok(match (schema, value) {
        (Schema::Map(map), Avro::Map(values)) => {
            let child = if values.is_empty() { Schema::Null } else {
                inferred_schema(&map.types, values.first_value()?.ok_or_else(invalid)?, names, depth + 1)?
            };
            Schema::Map(apache_avro::schema::MapSchema { types: Box::new(child), attributes: Default::default() })
        }
        (_, Avro::Null) => Schema::Null, (_, Avro::Boolean(_)) => Schema::Boolean,
        (_, Avro::Int(_)) => Schema::Int, (_, Avro::Long(_)) => Schema::Long,
        (_, Avro::Float(_)) => Schema::Float, (_, Avro::Double(_)) => Schema::Double,
        (_, Avro::String(_) | Avro::Utf8(_)) => Schema::String, (_, Avro::Bytes(_)) => Schema::Bytes,
        (_, Avro::Record(_) | Avro::Array(_) | Avro::PackedArray(_) | Avro::Enum(_, _) | Avro::Fixed(_, _)) => schema.clone(),
        _ => return Err(invalid()),
    })
}

fn encode_as(target: &Schema, actual: &Schema, value: Avro, names: &BTreeMap<String, Schema>, depth: usize) -> Result<Json> {
    nesting(depth)?;
    let (actual, value) = unwrap(actual, value, names)?;
    let target = physical(resolve(target, names)?);
    let numeric = |value: Avro| -> Result<Avro> {
        Ok(match target {
            Schema::Int | Schema::Date | Schema::TimeMillis => Avro::Int(match value { Avro::Int(value) => value, Avro::Long(value) => value as i32, Avro::Float(value) => value as i32, Avro::Double(value) => value as i32, _ => return Err(invalid()) }),
            Schema::Long | Schema::TimestampMillis => Avro::Long(match value { Avro::Int(value) => i64::from(value), Avro::Long(value) => value, Avro::Float(value) => value as i64, Avro::Double(value) => value as i64, _ => return Err(invalid()) }),
            Schema::Float => Avro::Float(match value { Avro::Int(value) => value as f32, Avro::Long(value) => value as f32, Avro::Float(value) => value, Avro::Double(value) => value as f32, _ => return Err(invalid()) }),
            Schema::Double => Avro::Double(match value { Avro::Int(value) => f64::from(value), Avro::Long(value) => value as f64, Avro::Float(value) => f64::from(value), Avro::Double(value) => value, _ => return Err(invalid()) }),
            _ => value,
        })
    };
    match (target, value) {
        (Schema::Null, _) => Ok(Json::Null),
        (Schema::Union(union), value) => {
            let name = label(actual);
            let target = union.variants().iter().find(|schema| resolve(schema, names).is_ok_and(|schema| label(schema) == name)).ok_or_else(invalid)?;
            if matches!(value, Avro::Null) { Ok(Json::Null) } else { Ok(Json::Object(serde_json::Map::from_iter([(label(resolve(target, names)?), encode_as(target, actual, value, names, depth + 1)?)]))) }
        }
        (Schema::Map(target), Avro::Map(values)) => {
            let Schema::Map(actual) = physical(actual) else { return Err(invalid()); };
            Ok(Json::Object(values.ordered()?.into_iter().map(|(key, value)| Ok((key.into_text(), encode_as(&target.types, &actual.types, value, names, depth + 1)?))).collect::<Result<_>>()?))
        }
        (Schema::Array(_), Avro::PackedArray(values)) => encode_as(target, actual, Avro::Array(values.into_physical_values()), names, depth),
        (Schema::Array(target), Avro::Array(values)) => {
            let Schema::Array(actual) = physical(actual) else { return Err(invalid()); };
            Ok(Json::Array(values.into_iter().map(|value| encode_as(&target.items, &actual.items, value, names, depth + 1)).collect::<Result<_>>()?))
        }
        (Schema::Record(target), Avro::Record(values)) => {
            let Schema::Record(actual) = physical(actual) else { return Err(invalid()); };
            if values.len() < target.fields.len() || actual.fields.len() < target.fields.len() { return Err(invalid()); }
            Ok(Json::Object(target.fields.iter().zip(actual.fields.iter().zip(values)).map(|(target, (actual, (_, value)))| Ok((target.name.clone(), encode_as(&target.schema, &actual.schema, value, names, depth + 1)?))).collect::<Result<_>>()?))
        }
        (Schema::Fixed(fixed), Avro::Fixed(_, mut value)) => {
            if value.len() < fixed.size { return Err(invalid()); } value.truncate(fixed.size);
            Ok(byte_text(value).into())
        }
        (Schema::Enum(enumeration), Avro::Enum(_, value)) if enumeration.symbols.contains(&value) => Ok(value.into()),
        (_, value) => encode(target, numeric(value)?, names, depth),
    }
}

pub(super) fn unwrap<'a>(schema: &'a Schema, value: Avro, names: &'a BTreeMap<String, Schema>) -> Result<(&'a Schema, Avro)> {
    let schema = resolve(schema, names)?;
    if let (Schema::Union(union), Avro::Union(index, _)) = (schema, &value) {
        let selected = union.variants().get(*index as usize).ok_or_else(invalid)?;
        let Avro::Union(_, value) = value else { unreachable!() };
        return unwrap(selected, *value, names);
    }
    Ok((schema, value))
}

fn encode(schema: &Schema, value: Avro, names: &BTreeMap<String, Schema>, depth: usize) -> Result<Json> {
    nesting(depth)?;
    let schema = physical(resolve(schema, names)?);
    Ok(match (schema, value) {
        (Schema::Union(union), Avro::Union(index, value)) => {
            let schema = union.variants().get(index as usize).ok_or_else(invalid)?;
            if matches!(value.as_ref(), Avro::Null) { Json::Null }
            else { Json::Object(serde_json::Map::from_iter([(label(resolve(schema, names)?), encode(schema, *value, names, depth + 1)?)])) }
        }
        (Schema::Record(record), Avro::Record(values)) => {
            if record.fields.len() != values.len() { return Err(invalid()); }
            let mut output = serde_json::Map::new();
            for (field, (name, value)) in record.fields.iter().zip(values) {
                if field.name != name { return Err(invalid()); }
                output.insert(name, encode(&field.schema, value, names, depth + 1)?);
            }
            Json::Object(output)
        }
        (Schema::Array(_), Avro::PackedArray(values)) => return encode(schema, Avro::Array(values.into_physical_values()), names, depth),
        (Schema::Array(array), Avro::Array(values)) => Json::Array(values.into_iter().map(|value| encode(&array.items, value, names, depth + 1)).collect::<Result<_>>()?),
        (Schema::Map(map), Avro::Map(values)) => Json::Object(values.ordered()?.into_iter().map(|(name, value)| Ok((name.into_text(), encode(&map.types, value, names, depth + 1)?))).collect::<Result<_>>()?),
        (Schema::Null, Avro::Null) => Json::Null,
        (Schema::Boolean, Avro::Boolean(value)) => value.into(),
        (Schema::Int | Schema::Date | Schema::TimeMillis, Avro::Int(value)) => value.into(),
        (Schema::Long | Schema::TimestampMillis, Avro::Long(value)) => value.into(),
        (Schema::Float, Avro::Float(value)) => floating(f64::from(value), Some(value))?,
        (Schema::Double, Avro::Double(value)) => floating(value, None)?,
        (Schema::String, Avro::String(value)) => value.into(),
        (Schema::String, Avro::Utf8(value)) => super::datum::java_utf8(&value).into(),
        (Schema::Enum(schema), Avro::Enum(index, value)) if schema.symbols.get(index as usize) == Some(&value) => value.into(),
        (Schema::Bytes, Avro::Bytes(value)) => byte_text(value).into(),
        (Schema::Fixed(schema), Avro::Fixed(size, value)) if schema.size == size && value.len() == size => byte_text(value).into(),
        _ => return Err(invalid()),
    })
}

fn decode(schema: &Schema, value: &Json, names: &BTreeMap<String, Schema>, depth: usize) -> Result<Avro> {
    nesting(depth)?;
    let schema = physical(resolve(schema, names)?);
    Ok(match schema {
        Schema::Union(union) => {
            let (label_name, value) = if value.is_null() { ("null", value) } else {
                let entries = value.as_object().ok_or_else(invalid)?;
                let (name, value) = entries.iter().next().ok_or_else(invalid)?;
                (name.as_str(), value)
            };
            let index = union.variants().iter().position(|schema| resolve(schema, names).is_ok_and(|schema| label(schema) == label_name)).ok_or_else(invalid)?;
            Avro::Union(index as u32, Box::new(decode(&union.variants()[index], value, names, depth + 1)?))
        }
        Schema::Record(record) => {
            let input = value.as_object().ok_or_else(invalid)?;
            let mut remaining = input.iter(); let mut saved = BTreeMap::new(); let mut output = Vec::with_capacity(record.fields.len());
            for field in &record.fields {
                let value = match saved.remove(field.name.as_str()) {
                    Some(value) => value,
                    None => loop {
                        let (name, value) = remaining.next().ok_or_else(invalid)?;
                        if name == &field.name || field.aliases.as_ref().is_some_and(|aliases| aliases.contains(name)) { break value; }
                        saved.insert(name.as_str(), value);
                    },
                };
                output.push((field.name.clone(), decode(&field.schema, value, names, depth + 1)?));
            }
            if !saved.is_empty() { return Err(error("avro-json", "Unknown buffered record fields")); }
            Avro::Record(output)
        }
        Schema::Array(array) => Avro::Array(value.as_array().ok_or_else(invalid)?.iter().map(|value| decode(&array.items, value, names, depth + 1)).collect::<Result<_>>()?),
        Schema::Map(map) => {
            let entries = value.as_object().ok_or_else(invalid)?;
            Avro::Map(AvroMap::new(usize::from(!entries.is_empty()), entries.iter().map(|(name, value)| Ok((MapKey::Utf8(name.as_bytes().to_vec()), decode(&map.types, value, names, depth + 1)?))).collect::<Result<_>>()?, map.attributes.get("avro.java.string").and_then(serde_json::Value::as_str) == Some("String")))
        }
        Schema::Null if value.is_null() => Avro::Null,
        Schema::Boolean => Avro::Boolean(value.as_bool().ok_or_else(invalid)?),
        Schema::Int | Schema::Date | Schema::TimeMillis => Avro::Int(integer(value, true)? as i32),
        Schema::Long | Schema::TimestampMillis => Avro::Long(integer(value, false)?),
        Schema::Float => Avro::Float(value.as_f64().ok_or_else(invalid)? as f32),
        Schema::Double => Avro::Double(value.as_f64().ok_or_else(invalid)?),
        Schema::String => Avro::String(value.as_str().ok_or_else(invalid)?.to_owned()),
        Schema::Bytes => Avro::Bytes(latin1(value.as_str().ok_or_else(invalid)?)),
        Schema::Fixed(fixed) => {
            let bytes = latin1(value.as_str().ok_or_else(invalid)?);
            if bytes.len() != fixed.size { return Err(error("avro-json", "Fixed value has the wrong length")); }
            Avro::Fixed(fixed.size, bytes)
        }
        Schema::Enum(enumeration) => {
            let symbol = value.as_str().ok_or_else(invalid)?;
            let index = enumeration.symbols.iter().position(|candidate| candidate == symbol).ok_or_else(invalid)?;
            Avro::Enum(index as u32, symbol.to_owned())
        }
        _ => return Err(invalid()),
    })
}

fn integer(value: &Json, narrow: bool) -> Result<i64> {
    if let Some(value) = value.as_i64() { if narrow && i32::try_from(value).is_err() { return Err(invalid()); } return Ok(value); }
    if value.as_u64().is_some() { return Err(invalid()); }
    let value = value.as_f64().ok_or_else(invalid)?;
    let (minimum, maximum) = if narrow { (f64::from(i32::MIN), f64::from(i32::MAX)) } else { (i64::MIN as f64, i64::MAX as f64) };
    if !value.is_finite() || value < minimum || value > maximum { return Err(invalid()); }
    Ok(value as i64)
}
fn floating(value: f64, single: Option<f32>) -> Result<Json> {
    if !value.is_finite() { return Ok(Json::String(if value.is_nan() { "NaN" } else if value.is_sign_negative() { "-Infinity" } else { "Infinity" }.into())); }
    let text = match single { Some(value) => rtdb_types::java_f32_text(value), None => rtdb_types::java_f64_text(value) };
    serde_json::from_str(&text).map_err(|cause| error("avro-json", cause))
}
fn json_bound(value: &Json, depth: usize) -> Result<usize> {
    nesting(depth)?;
    let add = |a: usize, b: usize| a.checked_add(b).ok_or_else(overflow);
    match value {
        Json::String(text) => add(4096, text.len().checked_mul(4).ok_or_else(overflow)?),
        Json::Array(values) => values.iter().try_fold(4096usize, |size, value| add(size, json_bound(value, depth + 1)?)),
        Json::Object(values) => values.iter().try_fold(4096usize, |size, (key, value)| add(add(size, key.len())?, json_bound(value, depth + 1)?)),
        _ => Ok(4096),
    }
}
pub(super) fn label(schema: &Schema) -> String { match physical(schema) {
    Schema::Record(record) => record.name.fullname(None), Schema::Enum(enumeration) => enumeration.name.fullname(None), Schema::Fixed(fixed) => fixed.name.fullname(None),
    schema => super::member_name(schema),
} }
fn physical(schema: &Schema) -> &Schema { match schema { Schema::Decimal(decimal) => physical(&decimal.inner), schema => schema } }
fn byte_text(bytes: Vec<u8>) -> String { bytes.into_iter().map(char::from).collect() }
fn latin1(text: &str) -> Vec<u8> { text.chars().map(|character| u8::try_from(u32::from(character)).unwrap_or(b'?')).collect() }
fn nesting(depth: usize) -> Result<()> { if depth > 128 { Err(error("avro-json", "Avro JSON datum nesting limit exceeded")) } else { Ok(()) } }
fn invalid() -> rtdb_types::Anomaly { error("avro-json", "JSON datum does not match its Avro schema") }
fn overflow() -> rtdb_types::Anomaly { super::resolution::overflow() }
