use std::{collections::BTreeMap, sync::Arc};
use arrow_array::{Array, ArrayRef, NullArray, UnionArray, builder::{Float64Builder, Int64Builder},
    types::{ArrowPrimitiveType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type}};
use arrow_schema::{DataType, Field, UnionFields};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use rtdb_types::{DecimalValue, ListValue, Result, StructFields, TimeUnit, Value};
use super::json_value::JsonValue as Json;
use super::{ConnectData, ConnectField, ConnectSchema, ConversionContext, Converter, PluginSpec, SchemaKind, error, numeric};
mod charset;

pub(super) fn open(spec: &PluginSpec, is_key: bool, parsers: &super::registry::ParserFactories) -> Result<Box<dyn Converter>> {
    match spec.class_name.as_str() {
        "io.confluent.connect.avro.AvroConverter" => Ok(Box::new(super::registry::RegistryConverter::with_parsers(spec, super::registry::RegistryFormat::Avro, is_key, parsers.clone())?)),
        "io.confluent.connect.json.JsonSchemaConverter" => Ok(Box::new(super::registry::RegistryConverter::with_parsers(spec, super::registry::RegistryFormat::Json, is_key, parsers.clone())?)),
        "org.apache.kafka.connect.json.JsonConverter" => {
            if let Some(format) = spec.options.get("decimal.format") { if !matches!(format.to_ascii_uppercase().as_str(), "BASE64" | "NUMERIC") { return Err(error("invalid-plugin-config", "decimal.format must be BASE64 or NUMERIC")); } }
            if let Some(size) = spec.options.get("schemas.cache.size") { if size.parse::<i32>().map_err(|cause| error("invalid-plugin-config", cause))? < 0 { return Err(error("invalid-plugin-config", "schemas.cache.size is negative")); } }
            let enabled = option_bool(&spec.options, "schemas.enable", true)?;
            let replace = option_bool(&spec.options, "replace.null.with.default", true)?;
            let schema = if enabled { spec.options.get("schema.content").map(|text| Json::from_str(text).map_err(|cause| error("invalid-plugin-config", cause)).and_then(|json| schema(&json, replace, 0, &mut DecodeBudget::new(text.len())?))).transpose()?.flatten() } else { None };
            Ok(Box::new(JsonConverter { enabled, replace, schema }))
        }
        class if matches!(class, "org.apache.kafka.connect.storage.StringConverter" | "org.apache.kafka.connect.converters.ByteArrayConverter" | "org.apache.kafka.connect.converters.BooleanConverter" | "org.apache.kafka.connect.converters.ShortConverter" | "org.apache.kafka.connect.converters.IntegerConverter" | "org.apache.kafka.connect.converters.LongConverter" | "org.apache.kafka.connect.converters.FloatConverter" | "org.apache.kafka.connect.converters.DoubleConverter") => {
            let kind = match class.rsplit('.').next().unwrap_or_default() {
                "StringConverter" => Scalar::String(charset::Charset::configured(&spec.options, is_key)?), "ByteArrayConverter" => Scalar::Bytes, "BooleanConverter" => Scalar::Bool,
                "ShortConverter" => Scalar::I16, "IntegerConverter" => Scalar::I32, "LongConverter" => Scalar::I64,
                "FloatConverter" => Scalar::F32, "DoubleConverter" => Scalar::F64,
                _ => return Err(error("plugin-class-not-found", class)),
            };
            Ok(Box::new(ScalarConverter { kind }))
        }
        class => Err(error("plugin-class-not-found", format!("No native converter registered for '{class}'"))),
    }
}
pub(super) fn option_bool(options: &BTreeMap<String, String>, key: &str, default: bool) -> Result<bool> {
    options.get(key).map(|value| super::config::boolean(value).ok_or_else(|| error("invalid-plugin-config", format!("Invalid boolean '{key}'")))).unwrap_or(Ok(default))
}
#[derive(Clone, Copy)] enum Scalar { String(charset::Charset), Bytes, Bool, I16, I32, I64, F32, F64 }
struct ScalarConverter { kind: Scalar }
#[async_trait]
impl Converter for ScalarConverter {
    async fn decode(&mut self, _: &str, bytes: Option<&[u8]>, context: &mut ConversionContext) -> Result<ConnectData> {
        context.reserve(bytes.map_or(0, <[u8]>::len).checked_mul(4).and_then(|size| size.checked_add(16_384)).ok_or_else(|| error("allocation-overflow", "Scalar conversion allocation overflow"))?)?;
        let kind = match self.kind { Scalar::String(_) => SchemaKind::String, Scalar::Bytes => SchemaKind::Bytes, Scalar::Bool => SchemaKind::Bool, Scalar::I16 => SchemaKind::I16, Scalar::I32 => SchemaKind::I32, Scalar::I64 => SchemaKind::I64, Scalar::F32 => SchemaKind::F32, Scalar::F64 => SchemaKind::F64 };
        let mut schema = ConnectSchema::new(kind); schema.optional = true;
        let Some(bytes) = bytes else { return Ok(ConnectData { schema: Some(Arc::new(schema)), value: Value::Null }); };
        let wrong_size = || error("conversion-error", "Kafka scalar payload has the wrong byte count");
        let value = match self.kind {
            Scalar::String(charset) => Value::Utf8(charset.decode(bytes)), Scalar::Bytes => Value::Binary(bytes.to_vec()),
            Scalar::Bool => { let [byte] = bytes else { return Err(wrong_size()); }; if *byte > 1 { return Err(error("conversion-error", "Invalid boolean byte")); } Value::Bool(*byte == 1) },
            Scalar::I16 => Value::I16(i16::from_be_bytes(bytes.try_into().map_err(|_| wrong_size())?)),
            Scalar::I32 => Value::I32(i32::from_be_bytes(bytes.try_into().map_err(|_| wrong_size())?)),
            Scalar::I64 => Value::I64(i64::from_be_bytes(bytes.try_into().map_err(|_| wrong_size())?)),
            Scalar::F32 => Value::F32(f32::from_be_bytes(bytes.try_into().map_err(|_| wrong_size())?)),
            Scalar::F64 => Value::F64(f64::from_be_bytes(bytes.try_into().map_err(|_| wrong_size())?)),
        };
        Ok(ConnectData { schema: Some(Arc::new(schema)), value })
    }
}
struct JsonConverter { enabled: bool, replace: bool, schema: Option<Arc<ConnectSchema>> }
#[async_trait]
impl Converter for JsonConverter {
    async fn decode(&mut self, _: &str, bytes: Option<&[u8]>, context: &mut ConversionContext) -> Result<ConnectData> {
        let Some(bytes) = bytes else { return Ok(ConnectData::null()); };
        let mut budget = DecodeBudget::new(bytes.len())?;
        context.reserve(budget.remaining)?;
        budget.claim(bytes.len().saturating_mul(64))?;
        let json = Json::from_slice(bytes).map_err(|cause| error("conversion-error", cause))?;
        let (schema, value) = if let Some(schema) = &self.schema { (Some(Arc::clone(schema)), &json) }
        else if self.enabled {
            json.as_object().filter(|object| object.len() == 2 && json.get("schema").is_some() && json.get("payload").is_some()).ok_or_else(|| error("conversion-error", "JsonConverter requires exactly schema and payload fields"))?;
            (schema(json.get("schema").expect("validated envelope"), self.replace, 0, &mut budget)?, json.get("payload").expect("validated envelope"))
        } else { (None, &json) };
        let value = convert_budget(&schema, value, self.replace, 0, &mut budget)?;
        Ok(ConnectData { schema, value })
    }
}
fn schema(json: &Json, replace: bool, depth: usize, budget: &mut DecodeBudget) -> Result<Option<Arc<ConnectSchema>>> {
    if json.is_null() { return Ok(None); }
    budget.claim(4096)?;
    if depth > 128 { return Err(error("conversion-error", "Connect schema nesting limit exceeded")); }
    let mut child = |key: &str| -> Result<Arc<ConnectSchema>> { schema(json.get(key).ok_or_else(|| error("conversion-error", format!("Schema lacks '{key}'")))?, replace, depth + 1, budget)?.ok_or_else(|| error("conversion-error", "Nested schema is null")) };
    let kind = match json.get("type").and_then(Json::as_str).ok_or_else(|| error("conversion-error", "Schema type is absent"))? {
        "boolean" => SchemaKind::Bool, "int8" => SchemaKind::I8, "int16" => SchemaKind::I16, "int32" => SchemaKind::I32, "int64" => SchemaKind::I64,
        "float" => SchemaKind::F32, "double" => SchemaKind::F64, "string" => SchemaKind::String, "bytes" => SchemaKind::Bytes,
        "array" => SchemaKind::Array(child("items")?), "map" => SchemaKind::Map(child("keys")?, child("values")?),
        "struct" => {
            let fields = json.get("fields").and_then(Json::as_array).ok_or_else(|| error("conversion-error", "Struct schema fields must be an array"))?;
            let mut seen = std::collections::BTreeSet::new();
            SchemaKind::Struct(fields.iter().map(|field| {
                let name = field.get("field").and_then(Json::as_str).ok_or_else(|| error("conversion-error", "Struct field name is absent"))?.to_owned();
                if !seen.insert(name.clone()) { return Err(error("conversion-error", "Duplicate Struct field")); }
                Ok(ConnectField { name, schema: schema(field, replace, depth + 1, budget)?.ok_or_else(|| error("conversion-error", "Struct field schema is null"))? })
            }).collect::<Result<_>>()?)
        }
        kind => return Err(error("conversion-error", format!("Unknown Connect schema type '{kind}'"))),
    };
    let mut result = ConnectSchema::new(kind);
    result.optional = json.get("optional").and_then(Json::as_bool).unwrap_or(false);
    result.name = json.get("name").and_then(Json::as_str).map(str::to_owned);
    result.doc = json.get("doc").and_then(Json::as_str).map(str::to_owned);
    result.version = json.get("version").filter(|value| value.is_integral()).and_then(Json::int_value);
    if let Some(parameters) = json.get("parameters").and_then(Json::as_object) {
        result.parameters = parameters.iter().map(|(key, value)| Ok((key.clone(), value.as_str().ok_or_else(|| error("conversion-error", "Schema parameter must be a string"))?.to_owned()))).collect::<Result<_>>()?;
    }
    if let Some(default) = json.get("default") { result.default = Some(convert_budget(&Some(Arc::new(result.clone())), default, replace, depth + 1, budget)?).filter(|value| *value != Value::Null); }
    Ok(Some(Arc::new(result)))
}
fn convert_budget(schema: &Option<Arc<ConnectSchema>>, json: &Json, replace: bool, depth: usize, budget: &mut DecodeBudget) -> Result<Value> {
    if let Some(schema) = schema { if matches!(schema.kind, SchemaKind::Reference(_)) {
        budget.claim(schema.view_allocation_bound())?;
        return convert_budget(&Some(Arc::new(schema.resolved().into_owned())), json, replace, depth, budget);
    } }
    budget.claim(512)?;
    if depth > 128 { return Err(error("conversion-error", "Connect value nesting limit exceeded")); }
    if json.is_null() {
        if let Some(schema) = schema {
            if replace { if let Some(default) = schema.default_value() { budget.claim(default.retained_bytes()?)?; return Ok(default.clone()); } }
            if !schema.optional { return Err(error("conversion-error", "Null for required Connect field")); }
        }
        return Ok(Value::Null);
    }
    if let Some(name) = schema.as_ref().and_then(|schema| schema.name.as_deref()) {
        let integer = || if json.is_integral() { Ok(json.long_value().expect("integral number")) } else { Err(error("conversion-error", "Logical temporal value must be an integer")) };
        let int = || if json.is_int() { Ok(json.int_value().expect("IntNode number")) } else { Err(error("conversion-error", "Logical Date and Time require an IntNode")) };
        match name {
            "org.apache.kafka.connect.data.Timestamp" => return Ok(Value::Timestamp { value: integer()?, unit: TimeUnit::Millisecond, timezone: Some("UTC".into()) }),
            "org.apache.kafka.connect.data.Date" => return Ok(Value::Timestamp { value: i64::from(int()?) * 86_400_000, unit: TimeUnit::Millisecond, timezone: Some("UTC".into()) }),
            "org.apache.kafka.connect.data.Time" => { let value = i64::from(int()?); if !(0..=86_400_000).contains(&value) { return Err(error("conversion-error", "Time is outside one day")); } return Ok(Value::Timestamp { value, unit: TimeUnit::Millisecond, timezone: Some("UTC".into()) }); }
            "org.apache.kafka.connect.data.Decimal" => return decimal(json, schema.as_ref().expect("logical schema")),
            _ => (),
        }
    }
    if let Some(text) = json.as_str() { budget.claim(text.len().saturating_mul(2))?; }
    let kind = schema.as_ref().map(|schema| &schema.kind);
    let integral = || json.long_value().unwrap_or(0);
    let int32 = || json.int_value().unwrap_or(0);
    Ok(match kind {
        Some(SchemaKind::Bool) => Value::Bool(json.as_bool().unwrap_or(false)),
        Some(SchemaKind::I8) => Value::I8(int32() as i8), Some(SchemaKind::I16) => Value::I16(int32() as i16),
        Some(SchemaKind::I32) => Value::I32(int32()), Some(SchemaKind::I64) => Value::I64(integral()),
        Some(SchemaKind::F32) => Value::F32(json.float_value().unwrap_or(0.0)), Some(SchemaKind::F64) => Value::F64(json.double_value().unwrap_or(0.0)),
        Some(SchemaKind::String) => json.as_str().map(|value| Value::Utf8(value.into())).unwrap_or(Value::Null),
        Some(SchemaKind::Bytes) => Value::Binary(STANDARD.decode(json.as_str().ok_or_else(|| error("conversion-error", "Bytes must be base64 text"))?).map_err(|cause| error("conversion-error", cause))?),
        Some(SchemaKind::Struct(fields)) => {
            budget.claim(fields.len().saturating_mul(4096))?;
            if !json.is_object() { return Err(error("conversion-error", "Struct payload must be an object")); }
            Value::Struct(fields.iter().map(|field| Ok((field.name.clone(), convert_budget(&Some(Arc::clone(&field.schema)), json.get(&field.name).unwrap_or(&Json::Null), replace, depth + 1, budget)?))).collect::<Result<_>>()?)
        }
        Some(SchemaKind::Array(element)) => Value::List(json_array(
            json.as_array().ok_or_else(|| error("conversion-error", "Array payload must be an array"))?, Some(element), replace, false, depth + 1, budget)?),
        Some(SchemaKind::ArrayOfNull) => Value::List(json_array(
            json.as_array().ok_or_else(|| error("conversion-error", "Array payload must be an array"))?, None, replace, false, depth + 1, budget)?),
        Some(SchemaKind::MapOfNull | SchemaKind::MapWithNullValues(_)) => {
            let entries = json.as_array().ok_or_else(|| error("conversion-error", "Map must be a tuple array"))?.iter().map(|entry| {
                let pair = entry.as_array().filter(|pair| pair.len() == 2).ok_or_else(|| error("conversion-error", "Map entry must be a pair"))?;
                let key = match kind { Some(SchemaKind::MapWithNullValues(key)) => Some(Arc::clone(key)), _ => None };
                Ok((convert_budget(&key, &pair[0], replace, depth + 1, budget)?, convert_budget(&None, &pair[1], replace, depth + 1, budget)?))
            }).collect::<Result<_>>()?;
            Value::Map { entries, sorted: false }
        }
        Some(SchemaKind::Map(key, value)) => {
            let entries = if matches!(key.resolved().kind, SchemaKind::String) {
                json.as_object().ok_or_else(|| error("conversion-error", "String-key map must be an object"))?.iter().map(|(name, json)| Ok((Value::Utf8(name.clone()), convert_budget(&Some(Arc::clone(value)), json, replace, depth + 1, budget)?))).collect::<Result<_>>()?
            } else {
                json.as_array().ok_or_else(|| error("conversion-error", "Map must be a tuple array"))?.iter().map(|entry| {
                    let pair = entry.as_array().filter(|pair| pair.len() == 2).ok_or_else(|| error("conversion-error", "Map entry must be a pair"))?;
                    Ok((convert_budget(&Some(Arc::clone(key)), &pair[0], replace, depth + 1, budget)?, convert_budget(&Some(Arc::clone(value)), &pair[1], replace, depth + 1, budget)?))
                }).collect::<Result<_>>()?
            };
            Value::Map { entries, sorted: false }
        }
        Some(SchemaKind::Reference(_)) => unreachable!("schema references resolve before conversion"),
        None => match json {
            Json::Null => Value::Null, Json::Boolean(value) => Value::Bool(*value), Json::String(value) => Value::Utf8(value.clone()),
            Json::Number(_) | Json::Decimal(_) => if json.is_integral() { Value::I64(integral()) } else { Value::F64(json.double_value().ok_or_else(|| error("conversion-error", "Invalid number"))?) },
            Json::Array(values) => Value::List(json_array(values, None, replace, false, depth + 1, budget)?),
            Json::Object(values) => Value::Map { entries: values.iter().map(|(key, value)| Ok((Value::Utf8(key.clone()), convert_budget(&None, value, replace, depth + 1, budget)?))).collect::<Result<_>>()?, sorted: false },
        },
    })
}
fn json_array(
    values: &[Json], element: Option<&Arc<ConnectSchema>>, replace: bool, registry: bool,
    depth: usize, budget: &mut DecodeBudget,
) -> Result<ListValue> {
    if let Some(schema) = element.filter(|schema| !matches!(schema.name.as_deref(), Some(
        "org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time"
        | "org.apache.kafka.connect.data.Timestamp" | "org.apache.kafka.connect.data.Decimal")))
    {
        macro_rules! primitive {
            ($ty:ty, $variant:ident, $read:expr) => {{
                let default = match schema.default_value().filter(|_| replace) {
                    Some(Value::$variant(value)) => Some(*value),
                    None => None,
                    Some(_) => return slow_json_array(values, element, replace, registry, depth, budget),
                };
                return primitive_json_array::<$ty>(values, schema, default, registry, depth, budget, $read);
            }};
        }
        match schema.kind {
            SchemaKind::I8 => primitive!(Int8Type, I8, |json: &Json| json.int_value().unwrap_or(0) as i8),
            SchemaKind::I16 => primitive!(Int16Type, I16, |json: &Json| json.int_value().unwrap_or(0) as i16),
            SchemaKind::I32 => primitive!(Int32Type, I32, |json: &Json| json.int_value().unwrap_or(0)),
            SchemaKind::I64 => primitive!(Int64Type, I64, |json: &Json| json.long_value().unwrap_or(0)),
            SchemaKind::F32 => primitive!(Float32Type, F32, |json: &Json| json.float_value().unwrap_or(0.0)),
            SchemaKind::F64 => primitive!(Float64Type, F64, |json: &Json| json.double_value().unwrap_or(0.0)),
            _ => (),
        }
    }
    if element.is_none() && values.iter().all(|value| value.is_null() || value.is_number()) {
        return schemaless_numbers(values, depth, budget);
    }
    slow_json_array(values, element, replace, registry, depth, budget)
}

fn slow_json_array(values: &[Json], element: Option<&Arc<ConnectSchema>>, replace: bool, registry: bool,
    depth: usize, budget: &mut DecodeBudget,
) -> Result<ListValue> {
    let schema = element.cloned();
    ListValue::try_from_cells(values.iter().map(|value| {
        if registry { registry_value(element.expect("registry array element"), Some(value), depth, budget) }
        else { convert_budget(&schema, value, replace, depth, budget) }
    }))
}

fn primitive_json_array<T: ArrowPrimitiveType>(
    values: &[Json], schema: &ConnectSchema, default: Option<T::Native>, registry: bool,
    depth: usize, budget: &mut DecodeBudget, read: impl Fn(&Json) -> T::Native,
) -> Result<ListValue> {
    numeric::collect::<T>(values.len(), values.iter().map(|json| {
        if registry {
            if depth > 128 { return Err(error("conversion-error", "Registry value nesting limit exceeded")); }
            budget.claim(512)?;
        }
        budget.claim(512)?;
        if depth > 128 { return Err(error("conversion-error", "Connect value nesting limit exceeded")); }
        if json.is_null() {
            if let Some(default) = default {
                budget.claim(schema.default_value().expect("matched default").retained_bytes()?)?;
                return Ok(Some(default));
            }
            if !schema.optional { return Err(error("conversion-error", "Null for required Connect field")); }
            return Ok(None);
        }
        if let Some(text) = json.as_str() { budget.claim(text.len().saturating_mul(2))?; }
        Ok(Some(read(json)))
    }))
}

fn schemaless_numbers(values: &[Json], depth: usize, budget: &mut DecodeBudget) -> Result<ListValue> {
    let int = |json: &Json| json.long_value().unwrap_or(0);
    let double = |json: &Json| json.double_value().ok_or_else(|| error("conversion-error", "Invalid number"));
    let mut charge = || {
        budget.claim(512)?;
        if depth > 128 { return Err(error("conversion-error", "Connect value nesting limit exceeded")); }
        Ok(())
    };
    if values.iter().all(|value| value.is_null() || value.is_integral()) {
        return numeric::collect::<Int64Type>(values.len(), values.iter().map(|json| {
            charge()?;
            Ok((!json.is_null()).then(|| int(json)))
        }));
    }
    if !values.iter().any(Json::is_integral) {
        return numeric::collect::<Float64Type>(values.len(), values.iter().map(|json| {
            charge()?;
            (!json.is_null()).then(|| double(json)).transpose()
        }));
    }
    let mut ints = Int64Builder::new();
    let mut doubles = Float64Builder::new();
    let mut nulls = 0;
    let mut fields = Vec::with_capacity(3);
    let mut ids = Vec::with_capacity(values.len());
    let mut offsets = Vec::with_capacity(values.len());
    for json in values {
        charge()?;
        let (ty, name, offset) = if json.is_null() {
            let offset = nulls;
            nulls += 1;
            (DataType::Null, "null", offset)
        } else if json.is_integral() {
            let offset = ints.values_slice().len();
            ints.append_value(int(json));
            (DataType::Int64, "i64", offset)
        } else {
            let offset = doubles.values_slice().len();
            doubles.append_value(double(json)?);
            (DataType::Float64, "f64", offset)
        };
        let id = fields.iter().position(|field: &Field| field.data_type() == &ty).unwrap_or(fields.len());
        if id == fields.len() { fields.push(Field::new(name, ty.clone(), ty == DataType::Null)); }
        ids.push(id as i8);
        offsets.push(i32::try_from(offset).map_err(|cause| error("allocation-overflow", cause))?);
    }
    let ints = Arc::new(ints.finish());
    let doubles = Arc::new(doubles.finish());
    let children = fields.iter().map(|field| -> ArrayRef {
        match field.data_type() {
            DataType::Int64 => ints.clone(),
            DataType::Float64 => doubles.clone(),
            _ => Arc::new(NullArray::new(nulls)),
        }
    }).collect();
    let fields = UnionFields::try_new(0..fields.len() as i8, fields).map_err(|cause| error("conversion-error", cause))?;
    let array = UnionArray::try_new(fields, ids.into(), Some(offsets.into()), children).map_err(|cause| error("conversion-error", cause))?;
    Ok(numeric::pack(Field::new("$data$", array.data_type().clone(), false), Arc::new(array)))
}

fn decimal(json: &Json, schema: &ConnectSchema) -> Result<Value> {
    use std::str::FromStr;
    let (coefficient, scale) = if json.is_number() {
        bigdecimal::BigDecimal::from_str(&json.number_text().expect("numeric value")).map_err(|cause| error("conversion-error", cause))?.as_bigint_and_exponent()
    } else {
        let scale = schema.parameters.get("scale").ok_or_else(|| error("conversion-error", "Decimal schema has no scale"))?.parse::<i64>().map_err(|cause| error("conversion-error", cause))?;
        let bytes = STANDARD.decode(json.as_str().ok_or_else(|| error("conversion-error", "Decimal must be numeric or base64"))?).map_err(|cause| error("conversion-error", cause))?;
        if bytes.is_empty() { return Err(error("conversion-error", "Decimal coefficient bytes are empty")); }
        (num_bigint::BigInt::from_signed_bytes_be(&bytes), scale)
    };
    let digits = coefficient.to_str_radix(10).trim_start_matches('-').len().max(1);
    let bit_width = if digits <= 32 { 128 } else { 256 };
    if digits > 64 { return Err(error("conversion-error", "Decimal precision exceeds the reference writer's limit")); }
    let bytes = coefficient.to_signed_bytes_be(); let width = usize::from(bit_width / 8u16);
    if bytes.len() > width { return Err(error("conversion-error", "Decimal coefficient exceeds its width")); }
    let mut unscaled = vec![if coefficient.sign() == num_bigint::Sign::Minus { 255 } else { 0 }; width];
    unscaled[width - bytes.len()..].copy_from_slice(&bytes);
    Ok(Value::Decimal(DecimalValue::new(unscaled, if bit_width == 128 { 32 } else { 64 }, i32::try_from(scale).map_err(|cause| error("conversion-error", cause))?, bit_width)?))
}

struct DecodeBudget { remaining: usize }
impl DecodeBudget {
    fn new(encoded_bytes: usize) -> Result<Self> { Ok(Self { remaining: encoded_bytes.checked_mul(512).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| error("allocation-overflow", "Connect decode budget overflow"))? }) }
    fn claim(&mut self, bytes: usize) -> Result<()> { self.remaining = self.remaining.checked_sub(bytes).ok_or_else(|| error("batch-too-large", "Connect decoded allocation exceeds its reserved bound"))?; Ok(()) }
}
pub(super) fn convert_registry(schema: &Arc<ConnectSchema>, json: &Json, limit: usize) -> Result<Value> {
    registry_value(schema, Some(json), 0, &mut DecodeBudget { remaining: limit })
}
fn registry_value(schema: &Arc<ConnectSchema>, json: Option<&Json>, depth: usize, budget: &mut DecodeBudget) -> Result<Value> {
    if matches!(schema.kind, SchemaKind::Reference(_)) {
        budget.claim(schema.view_allocation_bound())?;
        return registry_value(&Arc::new(schema.resolved().into_owned()), json, depth, budget);
    }
    if depth > 128 { return Err(error("conversion-error", "Registry value nesting limit exceeded")); }
    budget.claim(512)?;
    let Some(json) = json else {
        if let Some(default) = schema.default_value() { budget.claim(default.retained_bytes()?)?; return Ok(default.clone()); }
        return Ok(Value::Null);
    };
    if json.is_null() { return convert_budget(&Some(Arc::clone(schema)), json, true, depth, budget); }
    match &schema.kind {
        SchemaKind::Struct(fields) if (schema.name.as_deref() == Some("io.confluent.connect.json.OneOf") || schema.parameters.contains_key("org.apache.kafka.connect.data.Union")) => {
            let mut selected = None; let mut best_fields = 0usize;
            for field in fields {
                if matches!(field.schema.kind, SchemaKind::Reference(_)) { budget.claim(field.schema.view_allocation_bound())?; }
                let member = field.schema.resolved();
                let primitive = match &member.kind {
                    SchemaKind::Bool => json.is_boolean(), SchemaKind::I8 | SchemaKind::I16 | SchemaKind::I32 | SchemaKind::I64 => json.is_integral(),
                    SchemaKind::F32 | SchemaKind::F64 => json.is_number(), SchemaKind::String => json.is_string(), SchemaKind::Bytes => false,
                    SchemaKind::Array(_) | SchemaKind::ArrayOfNull => json.is_array(), SchemaKind::Map(_, _) | SchemaKind::MapWithNullValues(_) | SchemaKind::MapOfNull => json.is_object() || json.is_array(), SchemaKind::Struct(_) | SchemaKind::Reference(_) => false,
                };
                if primitive { selected = Some(field); break; }
                let count = matching_struct_fields(&member, json, depth, budget)?;
                if count > best_fields { best_fields = count; selected = Some(field); }
            }
            let selected = selected.ok_or_else(|| error("conversion-error", "No Connect union member matches the JSON value"))?;
            budget.claim(fields.len().saturating_mul(4096))?;
            let mut values = fields.iter().map(|field| (field.name.clone(), Value::Null)).collect::<StructFields<_>>();
            values.insert(selected.name.clone(), registry_value(&selected.schema, Some(json), depth + 1, budget)?); Ok(Value::Struct(values))
        }
        SchemaKind::Struct(fields) => {
            if !json.is_object() { return Err(error("conversion-error", "Connect Struct payload must be an object")); }
            budget.claim(fields.len().saturating_mul(4096))?;
            Ok(Value::Struct(fields.iter().map(|field| Ok((field.name.clone(), registry_value(&field.schema, json.get(&field.name), depth + 1, budget)?))).collect::<Result<_>>()?))
        }
        SchemaKind::Array(element) => Ok(Value::List(json_array(
            json.as_array().ok_or_else(|| error("conversion-error", "Connect array payload must be an array"))?, Some(element), true, true, depth + 1, budget)?)),
        SchemaKind::Map(key, value) => {
            let entries = if matches!(key.resolved().kind, SchemaKind::String) {
                json.as_object().ok_or_else(|| error("conversion-error", "Connect map payload must be an object"))?.iter().map(|(name, json)| { budget.claim(name.len().saturating_add(512))?; Ok((Value::Utf8(name.clone()), registry_value(value, Some(json), depth + 1, budget)?)) }).collect::<Result<_>>()?
            } else {
                json.as_array().ok_or_else(|| error("conversion-error", "Connect map payload must be an array of entry objects"))?.iter().map(|entry| {
                    entry.as_object().filter(|entry| entry.len() == 2).ok_or_else(|| error("conversion-error", "Map entry must be an object with key and value"))?;
                    Ok((registry_value(key, entry.get("key"), depth + 1, budget)?, registry_value(value, entry.get("value"), depth + 1, budget)?))
                }).collect::<Result<_>>()?
            }; Ok(Value::Map { entries, sorted: false })
        }
        _ => convert_budget(&Some(Arc::clone(schema)), json, true, depth, budget),
    }
}
fn matching_struct_fields(schema: &ConnectSchema, json: &Json, depth: usize, budget: &mut DecodeBudget) -> Result<usize> {
    if depth > 128 { return Err(error("conversion-error", "Registry value nesting limit exceeded")); }
    if matches!(schema.kind, SchemaKind::Reference(_)) { budget.claim(schema.view_allocation_bound())?; }
    let schema = schema.resolved();
    let (SchemaKind::Struct(fields), Some(_)) = (&schema.kind, json.as_object()) else { return Ok(0); };
    fields.iter().try_fold(0usize, |matched, field| match json.get(&field.name) {
        Some(value) => {
            let nested = matching_struct_fields(&field.schema, value, depth + 1, budget)?;
            matched.checked_add(1).and_then(|matched| matched.checked_add(nested)).ok_or_else(|| error("allocation-overflow", "Union match count exceeds range"))
        }
        None => Ok(matched),
    })
}

#[cfg(test)]
mod tests {
    use super::{DecodeBudget, Json, convert_budget, convert_registry, error, json_array};
    use crate::connect::{ConnectSchema, SchemaKind};
    use arrow_array::{Array, Float32Array, Int64Array, UnionArray};
    use rtdb_types::{ListValue, Value};
    use std::sync::Arc;

    #[test]
    fn json_numeric_arrays_match_scalar_layout_and_consume_the_same_budget() {
        for (kind, payload) in [(SchemaKind::F32, r#"[-0.0,1152921573326323713,null,"other"]"#),
            (SchemaKind::I16, "[32768,-32769,null,1.5]"), (SchemaKind::F64, "[]")] {
            let mut element = ConnectSchema::new(kind);
            element.optional = true;
            let element = Arc::new(element);
            let values = Json::from_str(payload).unwrap();
            let values = values.as_array().unwrap();
            for registry in [false, true] {
                let mut old = DecodeBudget::new(128).unwrap();
                let mut new = DecodeBudget::new(128).unwrap();
                let expected = ListValue::try_from_cells(values.iter().map(|value| {
                    if registry { super::registry_value(&element, Some(value), 1, &mut old) }
                    else { convert_budget(&Some(element.clone()), value, true, 1, &mut old) }
                })).unwrap();
                let actual = json_array(values, Some(&element), true, registry, 1, &mut new).unwrap();
                let (field, array) = actual.packed_column().unwrap();
                let (expected_field, expected_array) = expected.packed_column().unwrap();
                assert_eq!(field, expected_field);
                assert_eq!(array.to_data(), expected_array.to_data());
                assert_eq!(new.remaining, old.remaining);
                if let Some(array) = array.as_any().downcast_ref::<Float32Array>() {
                    assert_eq!(array.value(0).to_bits(), (-0.0_f32).to_bits());
                }
            }
        }
        let element = Arc::new(ConnectSchema::new(SchemaKind::I64));
        let root = Arc::new(ConnectSchema::new(SchemaKind::Array(element)));
        let json = Json::from_str("[null]").unwrap();
        let actual = convert_registry(&root, &json, 8192).unwrap_err();
        let expected = error("conversion-error", "Null for required Connect field");
        assert_eq!((actual.code, actual.message), (expected.code, expected.message));
    }

    #[test]
    fn schemaless_numbers_keep_the_original_dense_union_order_and_float_bits() {
        let json = Json::from_str("[null,-0.0,9223372036854775807,1.25]").unwrap();
        let Value::List(values) = convert_budget(&None, &json, true, 0, &mut DecodeBudget::new(128).unwrap()).unwrap() else { panic!("expected list"); };
        let union = values.array().unwrap().as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!(union.type_ids().as_ref(), &[0, 1, 2, 1]);
        let float = union.child(1).as_any().downcast_ref::<arrow_array::Float64Array>().unwrap();
        assert_eq!(float.value(0).to_bits(), (-0.0_f64).to_bits());
        let integer = union.child(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(integer.value(0), i64::MAX);
        assert!(union.child(0).data_type() == &arrow_schema::DataType::Null);
    }
}
