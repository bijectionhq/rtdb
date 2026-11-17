use std::{collections::BTreeMap, mem::size_of};
use super::*;
use super::super::schema_graph::{Scalar, SchemaView, ViewKind};

struct Walk<'a, const BUILD: bool> { context: &'a mut ConversionContext, demand: usize }

/// The context's reservation owner must outlive the returned value.
pub(super) fn direct(actual: &Schema, value: &Avro, plan: &AvroDatumPlan, destination: SchemaView<'_>, scrub: bool, context: &mut ConversionContext) -> Result<Value> {
    let mut measure = Walk::<false> { context, demand: 0 };
    measure.value(actual, value, plan, destination, scrub, 0)?;
    let demand = measure.demand;
    context.reserve(demand)?;
    Walk::<true> { context, demand: 0 }.value(actual, value, plan, destination, scrub, 0)?.ok_or_else(invalid)
}
fn invalid() -> rtdb_types::Anomaly { error("avro-decode", "Resolved value does not match the selected Connect output type") }
fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(resolution::overflow) }
fn mul(left: usize, right: usize) -> Result<usize> { left.checked_mul(right).ok_or_else(resolution::overflow) }
fn struct_slot(name: &str) -> Result<usize> { add(add(mul(size_of::<(String, Value)>(), 16)?, 256)?, name.len()) }

impl<const BUILD: bool> Walk<'_, BUILD> {
    fn claim(&mut self, bytes: usize) -> Result<()> {
        self.demand = add(self.demand, bytes)?;
        if !BUILD && self.demand > self.context.allocation_remaining() { return Err(error("batch-too-large", "Avro converted allocation exceeds its remaining limit")); }
        Ok(())
    }
    fn scratch(&mut self, bytes: usize) -> Result<()> {
        if !BUILD { self.context.reserve(bytes)?; }
        self.claim(bytes)
    }
    fn value(&mut self, actual: &Schema, value: &Avro, plan: &AvroDatumPlan, destination: SchemaView<'_>, scrub: bool, depth: usize) -> Result<Option<Value>> {
        if depth > 128 { return Err(error("avro-decode", "Avro datum nesting limit exceeded")); }
        let (actual, value) = datum::selected(actual, value, &plan.names)?;
        self.claim(size_of::<Value>())?;
        if matches!(value, Avro::Null) {
            if !destination.optional() { return Err(error("avro-decode", "Resolved null targets a required Connect field")); }
            return Ok(BUILD.then_some(Value::Null));
        }
        if matches!(destination.kind(), ViewKind::Struct(_)) && (destination.name() == Some("io.confluent.connect.avro.Union") || destination.parameter("org.apache.kafka.connect.data.Union").is_some()) {
            return Err(error("avro-decode", "Direct conversion requires a selected Connect union member"));
        }
        if let ViewKind::Scalar(kind) = destination.kind() {
            let valid = match (kind, value) {
                (Scalar::Bool, Avro::Boolean(_)) | (Scalar::I8 | Scalar::I16 | Scalar::I32, Avro::Int(_)) | (Scalar::I64, Avro::Long(_)) | (Scalar::F32, Avro::Float(_)) | (Scalar::F64, Avro::Double(_)) | (Scalar::String, Avro::String(_) | Avro::Utf8(_) | Avro::Enum(_, _)) | (Scalar::Bytes, Avro::Bytes(_) | Avro::Fixed(_, _)) => true,
                _ => false,
            };
            if !valid { return Err(invalid()); }
            if let Some(name @ ("org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time" | "org.apache.kafka.connect.data.Timestamp")) = destination.name() {
                let millis = match (name, destination.kind(), value) {
                    ("org.apache.kafka.connect.data.Date", ViewKind::Scalar(Scalar::I32), Avro::Int(value)) => i64::from(*value) * 86_400_000,
                    ("org.apache.kafka.connect.data.Time", ViewKind::Scalar(Scalar::I32), Avro::Int(value)) if (0..=86_400_000).contains(value) => i64::from(*value),
                    ("org.apache.kafka.connect.data.Timestamp", ViewKind::Scalar(Scalar::I64), Avro::Long(value)) => *value,
                    ("org.apache.kafka.connect.data.Time", ViewKind::Scalar(Scalar::I32), Avro::Int(_)) => return Err(error("avro-decode", "Time is outside one day")),
                    _ => return Err(error("avro-decode", "Logical date value has an incompatible primitive representation")),
                };
                self.claim(3)?;
                return Ok(if BUILD { Some(super::super::data::date_value(millis)) } else { None });
            }
            if destination.name() == Some("org.apache.kafka.connect.data.Decimal") {
                let (ViewKind::Scalar(Scalar::Bytes), Avro::Bytes(bytes) | Avro::Fixed(_, bytes)) = (destination.kind(), value) else { return Err(error("avro-decode", "Logical Decimal value has an incompatible primitive representation")); };
                if bytes.is_empty() { return Err(error("avro-decode", "Decimal coefficient bytes are empty")); }
                let scale = decimal_scale(destination.parameter("scale").ok_or_else(|| error("avro-decode", "Decimal schema has no scale"))?)?;
                let width = decimal_width(bytes)?;
                self.claim(width)?;
                return Ok(if BUILD { Some(decimal(bytes, scale, width)?) } else { None });
            }
        }
        let converted = match (destination.kind(), value) {
            (ViewKind::Scalar(Scalar::Bool), Avro::Boolean(value)) => BUILD.then_some(Value::Bool(*value)),
            (ViewKind::Scalar(Scalar::I8), Avro::Int(value)) => BUILD.then_some(Value::I8(*value as i8)),
            (ViewKind::Scalar(Scalar::I16), Avro::Int(value)) => BUILD.then_some(Value::I16(*value as i16)),
            (ViewKind::Scalar(Scalar::I32), Avro::Int(value)) => BUILD.then_some(Value::I32(*value)),
            (ViewKind::Scalar(Scalar::I64), Avro::Long(value)) => BUILD.then_some(Value::I64(*value)),
            (ViewKind::Scalar(Scalar::F32), Avro::Float(value)) => BUILD.then_some(Value::F32(*value)),
            (ViewKind::Scalar(Scalar::F64), Avro::Double(value)) => BUILD.then_some(Value::F64(*value)),
            (ViewKind::Scalar(Scalar::String), Avro::String(value) | Avro::Enum(_, value)) => { self.claim(value.len())?; if BUILD { Some(Value::Utf8(value.clone())) } else { None } }
            (ViewKind::Scalar(Scalar::String), Avro::Utf8(value)) => { self.claim(utf8_len(value)?)?; if BUILD { Some(Value::Utf8(utf8(value)?)) } else { None } }
            (ViewKind::Scalar(Scalar::Bytes), Avro::Bytes(value) | Avro::Fixed(_, value)) => { self.claim(value.len())?; if BUILD { Some(Value::Binary(value.clone())) } else { None } }
            (ViewKind::Array(element), Avro::Array(values)) => {
                let Schema::Array(array) = actual else { return Err(invalid()); };
                self.claim(mul(values.len(), size_of::<Value>())?)?;
                let mut output = if BUILD { Some(Vec::with_capacity(values.len())) } else { None };
                for value in values {
                    let value = self.value(&array.items, value, plan, element, scrub, depth + 1)?;
                    if let Some(output) = &mut output { output.push(value.ok_or_else(invalid)?); }
                }
                output.map(Value::List)
            }
            (ViewKind::Map(key, element), Avro::Map(values)) => {
                if key.optional() || !matches!(key.kind(), ViewKind::Scalar(Scalar::String)) { return Err(invalid()); }
                let Schema::Map(map) = actual else { return Err(invalid()); };
                self.map(values, &map.types, plan, element, scrub, depth + 1)?
            }
            (ViewKind::Map(key, element), Avro::Array(values)) => {
                if !key.optional() || !matches!(key.kind(), ViewKind::Scalar(Scalar::String)) { return Err(error("avro-decode", "Direct MapEntry conversion supports optional String keys only")); }
                let Schema::Array(array) = actual else { return Err(invalid()); };
                self.entries(values, &array.items, plan, key, element, scrub, depth + 1)?
            }
            (ViewKind::Struct(fields), Avro::Record(values)) => {
                let Schema::Record(record) = actual else { return Err(invalid()); };
                let mut output = BUILD.then(BTreeMap::new);
                for field in fields {
                    let selected = names::record_value(record, values, field.name, scrub)?;
                    self.claim(struct_slot(field.name)?)?;
                    let value = self.value(selected.schema, selected.value, plan, field.schema, scrub, depth + 1)?;
                    if let Some(output) = &mut output { output.insert(field.name.into(), value.ok_or_else(invalid)?); }
                }
                output.map(Value::Struct)
            }
            _ => return Err(invalid()),
        };
        if matches!(destination.name(), Some("org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time" | "org.apache.kafka.connect.data.Timestamp")) { return Err(error("avro-decode", "Logical date value has an incompatible primitive representation")); }
        if destination.name() == Some("org.apache.kafka.connect.data.Decimal") { return Err(error("avro-decode", "Logical Decimal value has an incompatible primitive representation")); }
        Ok(converted)
    }
    fn entries(&mut self, values: &[Avro], schema: &Schema, plan: &AvroDatumPlan, key_schema: SchemaView<'_>, element: SchemaView<'_>, scrub: bool, depth: usize) -> Result<Option<Value>> {
        let mut key_bytes = 0usize;
        for value in values {
            let mut value = value;
            while let Avro::Union(_, child) = value { value = child; }
            if let Avro::Record(fields) = value {
                if let Some((_, value)) = fields.iter().find(|(name, _)| name == "key") {
                    let mut value = value;
                    while let Avro::Union(_, child) = value { value = child; }
                    key_bytes = add(key_bytes, match value { Avro::String(value) | Avro::Enum(_, value) => value.len(), Avro::Utf8(value) => value.len(), _ => 0 })?;
                }
            }
        }
        self.scratch(map_scratch_counts(values.len(), key_bytes)?)?;
        self.claim(mul(values.len(), size_of::<Option<(Value, Value)>>())?)?;
        let mut keys = Vec::with_capacity(values.len());
        let mut converted = if BUILD { Some(Vec::with_capacity(values.len())) } else { None };
        for value in values {
            let (actual, value) = datum::selected(schema, value, &plan.names)?;
            let (Schema::Record(record), Avro::Record(values)) = (actual, value) else { return Err(invalid()); };
            let key = names::record_value(record, values, "key", false)?;
            let (actual_key, key_value) = datum::selected(key.schema, key.value, &plan.names)?;
            let key_text = match key_value {
                Avro::String(value) | Avro::Enum(_, value) => MapKey::String(value.clone()),
                Avro::Utf8(value) => MapKey::String(utf8(value)?),
                _ => return Err(error("avro-decode", "Direct MapEntry conversion requires non-null String keys")),
            };
            let key = self.value(actual_key, key_value, plan, key_schema, scrub, depth + 1)?;
            keys.push((key_text, ()));
            let value = names::record_value(record, values, "value", false)?;
            let value = self.value(value.schema, value.value, plan, element, scrub, depth + 1)?;
            if let Some(converted) = &mut converted { converted.push(Some((key.ok_or_else(invalid)?, value.ok_or_else(invalid)?))); }
        }
        let order = datum::string_map_order(&keys)?;
        self.claim(mul(order.len(), size_of::<(Value, Value)>())?)?;
        if let Some(mut converted) = converted {
            let mut output = Vec::with_capacity(order.len());
            for index in order { output.push(converted[index].take().ok_or_else(invalid)?); }
            Ok(Some(Value::Map { entries: output, sorted: false }))
        } else { Ok(None) }
    }
    fn map(&mut self, values: &AvroMap, schema: &Schema, plan: &AvroDatumPlan, element: SchemaView<'_>, scrub: bool, depth: usize) -> Result<Option<Value>> {
        self.scratch(map_scratch(values)?)?;
        let order = values.visible_indices()?;
        let mut keys = Vec::with_capacity(order.len());
        for index in &order {
            let (key, _) = values.iter().nth(*index).ok_or_else(invalid)?;
            keys.push((MapKey::String(key_text(key)?), ()));
        }
        let final_order = datum::string_map_order(&keys)?;
        self.claim(mul(order.len(), size_of::<Option<(Value, Value)>>())?)?;
        self.claim(mul(final_order.len(), size_of::<(Value, Value)>())?)?;
        let mut converted = if BUILD { Some(Vec::with_capacity(order.len())) } else { None };
        for (position, index) in order.iter().enumerate() {
            let (_, value) = values.iter().nth(*index).ok_or_else(invalid)?;
            let MapKey::String(key) = &keys[position].0 else { unreachable!() };
            self.claim(add(size_of::<Value>(), key.len())?)?;
            let value = self.value(schema, value, plan, element, scrub, depth + 1)?;
            if let Some(converted) = &mut converted { converted.push(Some((Value::Utf8(key.clone()), value.ok_or_else(invalid)?))); }
        }
        if let Some(mut converted) = converted {
            let mut output = Vec::with_capacity(final_order.len());
            for index in final_order { output.push(converted[index].take().ok_or_else(invalid)?); }
            Ok(Some(Value::Map { entries: output, sorted: false }))
        } else { Ok(None) }
    }
}
fn map_scratch(values: &AvroMap) -> Result<usize> {
    let mut count = 0usize;
    let mut keys = 0usize;
    for (key, _) in values.iter() { count = add(count, 1)?; keys = add(keys, key.len())?; }
    map_scratch_counts(count, keys)
}
fn map_scratch_counts(count: usize, keys: usize) -> Result<usize> { add(add(mul(count, 8192)?, mul(keys, 12)?)?, 4096) }
fn key_text(key: &MapKey) -> Result<String> { match key { MapKey::String(value) => Ok(value.clone()), MapKey::Utf8(value) => utf8(value) } }
fn utf8_parts(mut bytes: &[u8], mut emit: impl FnMut(&str) -> Result<()>) -> Result<()> {
    while !bytes.is_empty() {
        match std::str::from_utf8(bytes) {
            Ok(text) => { emit(text)?; break; }
            Err(error) => {
                let valid = error.valid_up_to();
                emit(std::str::from_utf8(&bytes[..valid]).expect("validated UTF8 prefix"))?;
                bytes = &bytes[valid..];
                let surrogate = bytes.len() >= 2 && bytes[0] == 0xed && (0xa0..=0xbf).contains(&bytes[1]);
                let skip = if surrogate { if bytes.get(2).is_some_and(|byte| (0x80..=0xbf).contains(byte)) { 3 } else { 2 } } else { error.error_len().unwrap_or(bytes.len()) };
                emit("\u{fffd}")?;
                bytes = &bytes[skip..];
            }
        }
    }
    Ok(())
}
fn utf8_len(bytes: &[u8]) -> Result<usize> { let mut size = 0; utf8_parts(bytes, |part| { size = add(size, part.len())?; Ok(()) })?; Ok(size) }
fn utf8(bytes: &[u8]) -> Result<String> { let mut output = String::with_capacity(utf8_len(bytes)?); utf8_parts(bytes, |part| { output.push_str(part); Ok(()) })?; Ok(output) }
fn decimal_scale(text: &str) -> Result<i32> {
    let (negative, digits) = if let Some(digits) = text.strip_prefix('-') { (true, digits) } else { (false, text.strip_prefix('+').unwrap_or(text)) };
    let invalid = || error("avro-decode", "Decimal scale is not a signed 32-bit integer");
    if digits.is_empty() { return Err(invalid()); }
    let magnitude = digits.chars().try_fold(0i64, |value, digit| {
        let digit = rtdb_types::decimal_digit(digit).filter(|_| digit.len_utf16() == 1).ok_or_else(invalid)?;
        value.checked_mul(10).and_then(|value| value.checked_add(i64::from(digit))).filter(|value| *value <= i64::from(i32::MAX) + i64::from(negative)).ok_or_else(invalid)
    })?;
    Ok(if negative { -magnitude } else { magnitude } as i32)
}
fn significant(mut bytes: &[u8]) -> &[u8] {
    while bytes.len() > 1 && ((bytes[0] == 0 && bytes[1] & 128 == 0) || (bytes[0] == 255 && bytes[1] & 128 != 0)) { bytes = &bytes[1..]; }
    bytes
}
fn decimal_width(bytes: &[u8]) -> Result<usize> {
    if bytes.is_empty() { return Err(error("avro-decode", "Decimal coefficient bytes are empty")); }
    let bytes = significant(bytes);
    if bytes.len() > 32 { return Err(error("avro-decode", "Decimal precision exceeds the reference writer limit")); }
    let negative = bytes[0] & 128 != 0;
    let mut magnitude = [0u8; 32];
    let start = 32 - bytes.len();
    if negative {
        let mut carry = true;
        for (index, byte) in bytes.iter().enumerate().rev() { let (value, overflow) = (!byte).overflowing_add(u8::from(carry)); magnitude[start + index] = value; carry = overflow; }
    } else { magnitude[start..].copy_from_slice(bytes); }
    const LIMIT_32: [u8; 32] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 238, 45, 109, 65, 91, 133, 172, 239, 129, 0, 0, 0, 0];
    const LIMIT_64: [u8; 32] = [0, 0, 0, 0, 0, 24, 79, 3, 233, 63, 249, 244, 218, 167, 151, 237, 110, 56, 237, 100, 191, 106, 31, 1, 0, 0, 0, 0, 0, 0, 0, 0];
    if magnitude < LIMIT_32 { Ok(16) } else if magnitude < LIMIT_64 { Ok(32) } else { Err(error("avro-decode", "Decimal precision exceeds the reference writer limit")) }
}
fn decimal(bytes: &[u8], scale: i32, width: usize) -> Result<Value> {
    let bytes = significant(bytes);
    let mut unscaled = vec![if bytes[0] & 128 != 0 { 255 } else { 0 }; width];
    let start = width.checked_sub(bytes.len()).ok_or_else(invalid)?;
    unscaled[start..].copy_from_slice(bytes);
    Ok(Value::Decimal(DecimalValue::new(unscaled, (width * 2) as i32, scale, (width * 8) as u16)?))
}

#[cfg(test)] mod tests;
