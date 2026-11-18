use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use crate::{Anomaly, Result};

/// Maximum nesting depth shared by the Transit wire and the typed JSON representation.
pub const MAX_DEPTH: usize = 128;

#[derive(Clone, Debug, PartialEq)]
pub enum TransitValue {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    /// Requests single-precision output; wire decoders return `Float` for either width.
    Float32(f32),
    String(String),
    Keyword(String),
    Symbol(String),
    Bytes(Vec<u8>),
    Array(Vec<TransitValue>),
    Map(Vec<(TransitValue, TransitValue)>),
    Tagged { tag: String, representation: Box<TransitValue> },
}

impl TransitValue {
    pub fn instant(value: crate::Instant) -> Result<Self> {
        Ok(Self::Tagged { tag: "time/instant".into(), representation: Box::new(Self::String(value.to_iso_string()?)) })
    }

    pub fn table_ref(table: &crate::TableRef) -> Self {
        Self::Tagged {
            tag: "xt/table".into(),
            representation: Box::new(Self::Map(vec![
                (Self::Keyword("schema-name".into()), Self::String(table.schema.clone())),
                (Self::Keyword("table-name".into()), Self::String(table.table.clone())),
            ])),
        }
    }
}

/// Preserves Transit value types in JSON using `$transit/type` wrappers, including map key types.
/// This representation is internal JSON data, not the Transit JSON wire format.
pub fn to_json_value(value: &TransitValue) -> Result<serde_json::Value> {
    to_json(value, 0)
}

/// Bounds storage allocated by `to_json_value`, including wrapper objects and temporary Base64 text.
/// Reserve before conversion and retain the charge while the returned JSON value lives.
/// Size overflow and nesting beyond the converter's depth limit return errors.
pub fn json_value_allocation_bound(value: &TransitValue) -> Result<usize> {
    fn bound(value: &TransitValue,depth: usize) -> Result<usize> {
        if depth > MAX_DEPTH { return Err(invalid("Transit nesting limit exceeded")); }
        let add = |left: usize,right: usize|left.checked_add(right).ok_or_else(||invalid("Transit JSON allocation exceeds addressable memory"));
        let text = |length: usize|length.checked_mul(8).ok_or_else(||invalid("Transit JSON allocation exceeds addressable memory"));
        let payload = match value {
            TransitValue::String(value) | TransitValue::Keyword(value) | TransitValue::Symbol(value) => text(value.len())?,
            TransitValue::Bytes(value) => text(value.len())?,
            TransitValue::Array(values) => values.iter().try_fold(0,|bytes,value|add(bytes,bound(value,depth + 1)?))?,
            TransitValue::Map(entries) => entries.iter().try_fold(0,|bytes,(key,value)|add(add(add(bytes,512)?,bound(key,depth + 1)?)?,bound(value,depth + 1)?))?,
            TransitValue::Tagged { tag,representation } => add(text(tag.len())?,bound(representation,depth + 1)?)?,
            _ => 0,
        };add(2048,payload)
    }
    bound(value,0)
}

fn to_json(value: &TransitValue, depth: usize) -> Result<serde_json::Value> {
    if depth > MAX_DEPTH { return Err(invalid("Transit nesting limit exceeded")); }
    Ok(match value {
        TransitValue::Null => serde_json::Value::Null,
        TransitValue::Bool(value) => (*value).into(),
        TransitValue::Integer(value) => (*value).into(),
        TransitValue::Float(value) => serde_json::Number::from_f64(*value).map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::json!({"$transit/type":"float", "value":if value.is_nan() { "NaN" } else if value.is_sign_positive() { "INF" } else { "-INF" }})),
        TransitValue::Float32(value) => serde_json::json!({"$transit/type":"float32", "value":crate::java_f32_text(*value)}),
        TransitValue::String(value) => value.clone().into(),
        TransitValue::Keyword(value) => serde_json::json!({"$transit/type":"keyword", "value":value}),
        TransitValue::Symbol(value) => serde_json::json!({"$transit/type":"symbol", "value":value}),
        TransitValue::Bytes(value) => serde_json::json!({"$transit/type":"bytes", "value":STANDARD.encode(value)}),
        TransitValue::Array(values) => serde_json::Value::Array(values.iter().map(|value| to_json(value, depth + 1)).collect::<Result<_>>()?),
        TransitValue::Map(entries) => {
            let entries = entries.iter().map(|(key, value)| Ok(serde_json::Value::Array(vec![to_json(key, depth + 1)?, to_json(value, depth + 1)?]))).collect::<Result<Vec<_>>>()?;
            serde_json::json!({"$transit/type":"map", "value":entries})
        }
        TransitValue::Tagged { tag, representation } => serde_json::json!({"$transit/type":"tagged", "tag":tag, "value":to_json(representation, depth + 1)?}),
    })
}

/// Reverses `to_json_value`; ordinary JSON objects become Transit maps with string keys.
/// Objects with `$transit/type` use the wrapper schema and must have a valid representation.
pub fn from_json_value(value: &serde_json::Value) -> Result<TransitValue> {
    from_json(value, 0)
}

fn from_json(value: &serde_json::Value, depth: usize) -> Result<TransitValue> {
    if depth > MAX_DEPTH { return Err(invalid("Transit nesting limit exceeded")); }
    Ok(match value {
        serde_json::Value::Null => TransitValue::Null,
        serde_json::Value::Bool(value) => TransitValue::Bool(*value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() { TransitValue::Integer(value) }
            else if let Some(value) = value.as_u64() { TransitValue::Tagged { tag: "n".into(), representation: Box::new(TransitValue::String(value.to_string())) } }
            else { TransitValue::Float(value.as_f64().ok_or_else(|| invalid("JSON number is not representable"))?) }
        }
        serde_json::Value::String(value) => TransitValue::String(value.clone()),
        serde_json::Value::Array(values) => TransitValue::Array(values.iter().map(|value| from_json(value, depth + 1)).collect::<Result<_>>()?),
        serde_json::Value::Object(values) => {
            if let Some(kind) = values.get("$transit/type") {
                let kind = kind.as_str().ok_or_else(|| invalid("Transit JSON type is not text"))?;
                let representation = values.get("value").ok_or_else(|| invalid("Transit JSON wrapper has no value"))?;
                if values.len() != if kind == "tagged" { 3 } else { 2 } { return Err(invalid("Unexpected Transit JSON wrapper fields")); }
                let text = || representation.as_str().ok_or_else(|| invalid("Transit JSON representation is not text"));
                match kind {
                    "keyword" => TransitValue::Keyword(text()?.into()), "symbol" => TransitValue::Symbol(text()?.into()),
                    "bytes" => TransitValue::Bytes(STANDARD.decode(text()?).map_err(invalid)?),
                    "float" => TransitValue::Float(match text()? { "NaN" => f64::NAN, "INF" => f64::INFINITY, "-INF" => f64::NEG_INFINITY, _ => return Err(invalid("Invalid special float")) }),
                    "float32" => TransitValue::Float32(match text()? { "NaN" => f32::NAN,"Infinity" => f32::INFINITY,"-Infinity" => f32::NEG_INFINITY,value => value.parse().map_err(invalid)? }),
                    "tagged" => TransitValue::Tagged { tag: values.get("tag").and_then(serde_json::Value::as_str).ok_or_else(|| invalid("Transit JSON wrapper has no tag"))?.into(), representation: Box::new(from_json(representation, depth + 1)?) },
                    "map" => {
                        let entries = representation.as_array().ok_or_else(|| invalid("Transit JSON map representation is not an array"))?;
                        TransitValue::Map(entries.iter().map(|entry| {
                            let pair = entry.as_array().filter(|entry| entry.len() == 2).ok_or_else(|| invalid("Transit JSON map entry is not a pair"))?;
                            Ok((from_json(&pair[0], depth + 1)?, from_json(&pair[1], depth + 1)?))
                        }).collect::<Result<_>>()?)
                    }
                    _ => return Err(invalid("Unknown Transit JSON wrapper type")),
                }
            } else {
                TransitValue::Map(values.iter().map(|(key, value)| Ok((TransitValue::String(key.clone()), from_json(value, depth + 1)?))).collect::<Result<_>>()?)
            }
        }
    })
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("rtdb.format/invalid-transit", error.to_string())
}
