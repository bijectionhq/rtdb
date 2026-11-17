use std::collections::BTreeMap;
use serde_json::value::RawValue;
use rtdb_types::Result;
use super::error;
use super::json_decimal::JsonDecimal;

#[derive(Clone, Copy)]
enum NumberMode { Double, Decimal }
#[cfg(test)] #[path = "json_value_tests.rs"] mod tests;

#[derive(Clone, Debug)]
pub(super) enum JsonValue { Null, Boolean(bool), Number(String), Decimal(JsonDecimal), String(String), Array(Vec<JsonValue>), Object(JsonObject) }
#[derive(Clone, Debug)]
pub(super) struct JsonObject { fields: Vec<(String, JsonValue)>, positions: BTreeMap<String, usize> }
impl JsonObject {
    pub fn get(&self, key: &str) -> Option<&JsonValue> { self.positions.get(key).map(|index| &self.fields[*index].1) }
    pub fn len(&self) -> usize { self.fields.len() }
    pub fn iter(&self) -> std::slice::Iter<'_, (String, JsonValue)> { self.fields.iter() }
}
impl FromIterator<(String, JsonValue)> for JsonObject {
    fn from_iter<T: IntoIterator<Item = (String, JsonValue)>>(values: T) -> Self {
        let mut object = Self { fields: Vec::new(), positions: BTreeMap::new() };
        for (key, value) in values {
            if let Some(index) = object.positions.get(&key) { object.fields[*index].1 = value; }
            else { object.positions.insert(key.clone(), object.fields.len()); object.fields.push((key, value)); }
        }
        object
    }
}
impl<'a> IntoIterator for &'a JsonObject {
    type Item = &'a (String, JsonValue);
    type IntoIter = std::slice::Iter<'a, (String, JsonValue)>;
    fn into_iter(self) -> Self::IntoIter { self.fields.iter() }
}
impl PartialEq for JsonValue {
    fn eq(&self, other: &Self) -> bool { match (self, other) {
        (Self::Null, Self::Null) => true, (Self::Boolean(left), Self::Boolean(right)) => left == right,
        (Self::Number(left), Self::Number(right)) => number_equal(left, right),
        (Self::Decimal(left), Self::Decimal(right)) => left == right,
        (Self::String(left), Self::String(right)) => left == right,
        (Self::Array(left), Self::Array(right)) => left == right,
        (Self::Object(left), Self::Object(right)) => left.len() == right.len() && left.iter().all(|(key, value)| right.get(key) == Some(value)),
        _ => false,
    } }
}
impl JsonValue {
    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        Self::from_slice_mode(bytes, NumberMode::Double)
    }
    pub fn from_registry_slice(bytes: &[u8]) -> Result<Self> { Self::from_slice_mode(bytes, NumberMode::Decimal) }
    pub fn from_registry_str(text: &str) -> Result<Self> { Self::from_registry_slice(text.as_bytes()) }
    fn from_slice_mode(bytes: &[u8], mode: NumberMode) -> Result<Self> {
        let raw = serde_json::from_slice::<&RawValue>(bytes).map_err(|cause| error("conversion-error", cause))?;
        Self::parse_mode(raw, 0, mode).map_err(|cause| rtdb_types::Anomaly::new_nullable(rtdb_types::AnomalyCategory::Incorrect, "xtdb.kafka-connect-source/conversion-error", cause.message))
    }
    pub fn from_str(text: &str) -> Result<Self> { Self::from_slice(text.as_bytes()) }
    pub fn from_serde(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(value) => Self::Boolean(value),
            serde_json::Value::Number(value) => Self::Number(value.to_string()),
            serde_json::Value::String(value) => Self::String(value),
            serde_json::Value::Array(values) => Self::Array(values.into_iter().map(Self::from_serde).collect()),
            serde_json::Value::Object(values) => Self::Object(values.into_iter().map(|(key, value)| (key, Self::from_serde(value))).collect()),
        }
    }
    pub fn parse(raw: &RawValue, depth: usize) -> Result<Self> { Self::parse_mode(raw, depth, NumberMode::Double) }
    fn parse_mode(raw: &RawValue, depth: usize, mode: NumberMode) -> Result<Self> {
        if depth > 128 { return Err(error("avro-schema", "Default nesting limit exceeded")); }
        Ok(match raw.get().as_bytes().first() {
            Some(b'n') => Self::Null,
            Some(b't' | b'f') => Self::Boolean(json(raw.get())?),
            Some(b'"') => Self::String(json(raw.get())?),
            Some(b'[') => Self::Array(json::<Vec<&RawValue>>(raw.get())?.into_iter().map(|raw| Self::parse_mode(raw, depth + 1, mode)).collect::<Result<_>>()?),
            Some(b'{') => Self::Object(json::<RawObject<'_>>(raw.get())?.0.into_iter().map(|(key, raw)| Ok((key, Self::parse_mode(raw, depth + 1, mode)?))).collect::<Result<_>>()?),
            Some(b'-' | b'0'..=b'9') => if matches!(mode, NumberMode::Decimal) && raw.get().contains(['.', 'e', 'E']) { Self::Decimal(JsonDecimal::parse(raw.get())?) } else { Self::Number(raw.get().into()) },
            _ => return Err(error("avro-schema", "Invalid default JSON")),
        })
    }
    pub fn get(&self, key: &str) -> Option<&Self> { match self { Self::Object(values) => values.get(key), _ => None } }
    pub fn as_array(&self) -> Option<&Vec<Self>> { match self { Self::Array(values) => Some(values), _ => None } }
    pub fn as_object(&self) -> Option<&JsonObject> { match self { Self::Object(values) => Some(values), _ => None } }
    pub fn as_str(&self) -> Option<&str> { match self { Self::String(value) => Some(value), _ => None } }
    pub fn as_bool(&self) -> Option<bool> { match self { Self::Boolean(value) => Some(*value), _ => None } }
    pub fn is_null(&self) -> bool { matches!(self, Self::Null) }
    pub fn long_value(&self) -> Option<i64> {
        if let Self::Decimal(value) = self { return Some(value.long_value()); }
        let Self::Number(value) = self else { return None; };
        if value.contains(['.', 'e', 'E']) { return value.parse::<f64>().ok().map(|value| value as i64); }
        let negative = value.starts_with('-');
        let number = value.bytes().filter(u8::is_ascii_digit).fold(0u64, |number, digit| number.wrapping_mul(10).wrapping_add(u64::from(digit - b'0')));
        Some(if negative { number.wrapping_neg() } else { number } as i64)
    }
    pub fn int_value(&self) -> Option<i32> {
        if let Self::Decimal(value) = self { return Some(value.long_value() as i32); }
        let Self::Number(value) = self else { return None; };
        if value.contains(['.', 'e', 'E']) { value.parse::<f64>().ok().map(|value| value as i32) } else { self.long_value().map(|value| value as i32) }
    }
    pub fn double_value(&self) -> Option<f64> { match self { Self::Decimal(value) => Some(value.double_value()), Self::Number(value) if value == "-0" => Some(0.0), Self::Number(value) => value.parse().ok(), _ => None } }
    pub fn float_value(&self) -> Option<f32> {
        if let Self::Decimal(value) = self { return Some(value.float_value()); }
        if self.is_integral() { match self { Self::Number(value) if value == "-0" => Some(0.0), Self::Number(value) => value.parse().ok(), _ => None } }
        else { self.double_value().map(|value| value as f32) }
    }
    pub fn is_integral(&self) -> bool { matches!(self, Self::Number(value) if !value.contains(['.', 'e', 'E'])) }
    pub fn is_int(&self) -> bool { self.is_integral() && self.as_i64().is_some_and(|value| i32::try_from(value).is_ok()) }
    pub fn as_i64(&self) -> Option<i64> { if self.is_integral() { match self { Self::Number(value) => value.parse().ok(), _ => None } } else { None } }
    pub fn is_number(&self) -> bool { matches!(self, Self::Number(_) | Self::Decimal(_)) }
    pub fn is_boolean(&self) -> bool { matches!(self, Self::Boolean(_)) }
    pub fn is_string(&self) -> bool { matches!(self, Self::String(_)) }
    pub fn is_array(&self) -> bool { matches!(self, Self::Array(_)) }
    pub fn is_object(&self) -> bool { matches!(self, Self::Object(_)) }
    pub fn pointer(&self, pointer: &str) -> Option<&Self> {
        if pointer.is_empty() { return Some(self); }
        let mut value = self;
        for token in pointer.strip_prefix('/')?.split('/') {
            let token = token.replace("~1", "/").replace("~0", "~");
            value = match value {
                Self::Object(_) => value.get(&token)?,
                Self::Array(values) if !token.starts_with('+') && (token == "0" || !token.starts_with('0')) => values.get(token.parse::<usize>().ok()?)?,
                _ => return None,
            };
        }
        Some(value)
    }
    pub fn number_text(&self) -> Option<String> {
        if let Self::Decimal(value) = self { return Some(value.text()); }
        let Self::Number(value) = self else { return None; };
        Some(if self.is_integral() { if value == "-0" { "0".into() } else { value.clone() } }
            else { rtdb_types::java_f64_text(self.double_value()?) })
    }
    pub fn encoded_bound(&self) -> Result<usize> {
        let overflow = || error("allocation-overflow", "JSON output allocation exceeds range");
        match self {
            Self::Null | Self::Boolean(_) => Ok(5),
            Self::Number(value) => Ok(value.len().max(32)),
            Self::Decimal(value) => Ok(value.encoded_bound()),
            Self::String(value) => value.len().checked_mul(6).and_then(|bytes| bytes.checked_add(2)).ok_or_else(overflow),
            Self::Array(values) => values.iter().try_fold(2usize, |bytes, value| bytes.checked_add(value.encoded_bound()?).and_then(|bytes| bytes.checked_add(1)).ok_or_else(overflow)),
            Self::Object(values) => values.iter().try_fold(2usize, |bytes, (key, value)| bytes.checked_add(key.len().checked_mul(6).ok_or_else(overflow)?).and_then(|bytes| bytes.checked_add(4)).and_then(|bytes| bytes.checked_add(value.encoded_bound().ok()?)).ok_or_else(overflow)),
        }
    }
    pub fn write(&self, output: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Null => output.extend_from_slice(b"null"),
            Self::Boolean(value) => output.extend_from_slice(if *value { b"true" } else { b"false" }),
            Self::Number(_) | Self::Decimal(_) => {
                let text = self.number_text().ok_or_else(|| error("conversion-error", "Invalid JSON number"))?;
                if matches!(text.as_str(), "Infinity" | "-Infinity" | "NaN") { serde_json::to_writer(output, &text).map_err(|cause| error("conversion-error", cause))?; }
                else { output.extend_from_slice(text.as_bytes()); }
            }
            Self::String(value) => serde_json::to_writer(output, value).map_err(|cause| error("conversion-error", cause))?,
            Self::Array(values) => {
                output.push(b'[');
                for (index, value) in values.iter().enumerate() { if index != 0 { output.push(b','); } value.write(output)?; }
                output.push(b']');
            }
            Self::Object(values) => {
                output.push(b'{');
                for (index, (key, value)) in values.iter().enumerate() {
                    if index != 0 { output.push(b','); }
                    serde_json::to_writer(&mut *output, key).map_err(|cause| error("conversion-error", cause))?;
                    output.push(b':'); value.write(output)?;
                }
                output.push(b'}');
            }
        }
        Ok(())
    }
    pub fn to_vec(&self) -> Result<Vec<u8>> { let mut bytes = Vec::with_capacity(self.encoded_bound()?); self.write(&mut bytes)?; Ok(bytes) }

}

fn json<'a, T: serde::Deserialize<'a>>(text: &'a str) -> Result<T> { serde_json::from_str(text).map_err(|cause| error("conversion-error", cause)) }

pub(super) struct RawObject<'a>(pub Vec<(String, &'a RawValue)>);
impl<'de> serde::Deserialize<'de> for RawObject<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct ObjectVisitor;
        impl<'de> serde::de::Visitor<'de> for ObjectVisitor {
            type Value = RawObject<'de>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str("a JSON object") }
            fn visit_map<M: serde::de::MapAccess<'de>>(self, mut map: M) -> std::result::Result<Self::Value, M::Error> {
                let mut fields: Vec<(String, &'de RawValue)> = Vec::new();
                let mut positions = BTreeMap::<String, usize>::new();
                while let Some((key, value)) = map.next_entry::<String, &'de RawValue>()? {
                    if let Some(index) = positions.get(&key) { fields[*index].1 = value; }
                    else { positions.insert(key.clone(), fields.len()); fields.push((key, value)); }
                }
                Ok(RawObject(fields))
            }
        }
        deserializer.deserialize_map(ObjectVisitor)
    }
}

fn number_equal(left: &str, right: &str) -> bool {
    match (left.contains(['.', 'e', 'E']), right.contains(['.', 'e', 'E'])) {
        (true, true) => left.parse::<f64>().ok().map(f64::to_bits) == right.parse::<f64>().ok().map(f64::to_bits),
        (false, false) => left == right || matches!((left, right), ("0", "-0") | ("-0", "0")),
        _ => false,
    }
}
