use std::{collections::BTreeMap, fmt};

use rtdb_types::{Anomaly, AnomalyCategory, Result, TypeForm};
use serde::{Serialize, Serializer, ser::{SerializeMap, SerializeSeq}};
use serde_json::Value;

use crate::transit::{TransitValue, to_json_value};

/// Retains the type form of a column-type diagnostic value.
pub fn column_type_data(form: &TypeForm<'_>) -> Result<Value> { to_json_value(&type_value(form)) }

/// Retains both the form and the vector type tag through Transit.
pub fn vector_type_data(form: &TypeForm<'_>) -> Result<Value> {
    to_json_value(&transit_tag("xtdb/type", type_value(form)))
}

fn type_value(form: &TypeForm<'_>) -> TransitValue {
    match form {
        TypeForm::Keyword(value) => TransitValue::Keyword((*value).into()),
        TypeForm::Text(value) => TransitValue::String((*value).into()),
        TypeForm::Integer(value) => TransitValue::Integer(*value),
        TypeForm::Boolean(value) => TransitValue::Bool(*value),
        TypeForm::Vector(values) => TransitValue::Array(values.iter().map(type_value).collect()),
        TypeForm::Set(values) => transit_tag("set", TransitValue::Array(values.iter().map(type_value).collect())),
        TypeForm::Map(entries) => TransitValue::Map(entries.iter().map(|(key, value)| (type_value(key), type_value(value))).collect()),
    }
}

fn transit_tag(tag: &str, representation: TransitValue) -> TransitValue {
    TransitValue::Tagged { tag: tag.into(), representation: Box::new(representation) }
}

#[derive(Clone, Copy)]
enum Encoding { Plain, Linked }

#[derive(Serialize)]
struct LinkedValue<T> {
    #[serde(rename = "@type")]
    tag: &'static str,
    #[serde(rename = "@value")]
    value: T,
}

impl Encoding {
    fn serialize<S: Serializer, T: Serialize>(self, tag: &'static str, value: T, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::Plain => value.serialize(serializer),
            Self::Linked => LinkedValue { tag, value }.serialize(serializer),
        }
    }
}

/// The plain JSON projection shared by PgWire and Flight SQL anomalies.
#[derive(Serialize)]
pub struct JsonAnomaly<'a> {
    category: AnomalyCategory,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<JsonContext<'a>>,
}

impl<'a> From<&'a Anomaly> for JsonAnomaly<'a> {
    fn from(error: &'a Anomaly) -> Self {
        Self { category: error.category, code: error_code(error), message: error.message.as_deref(),
            data: (!error.context.is_empty()).then_some(JsonContext { entries: &error.context, encoding: Encoding::Plain, code: None }) }
    }
}

pub fn json_ld(error: &Anomaly) -> std::result::Result<Value, serde_json::Error> {
    let category = serde_json::to_value(error.category)?;
    let context = JsonContext { entries: &error.context, encoding: Encoding::Linked, code: error_code(error) };
    Ok(serde_json::json!({"@type":format!("xt:{}", category.as_str().expect("anomaly category name")),
        "@value":{"xtdb.error/message":error.message, "xtdb.error/data":serde_json::to_value(context)?}}))
}

fn error_code(error: &Anomaly) -> Option<&str> { (!error.code.is_empty()).then_some(&error.code) }

struct JsonContext<'a> { entries: &'a BTreeMap<String, Value>, encoding: Encoding, code: Option<&'a str> }

impl Serialize for JsonContext<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        for (key, value) in self.entries {
            if self.code.is_some() && key == "xtdb.error/code" { continue; }
            map.serialize_entry(key, &JsonValue { value, encoding: self.encoding, depth: 0 })?;
        }
        if let Some(code) = self.code { map.serialize_entry("xtdb.error/code", &LinkedValue { tag: "xt:keyword", value: code })?; }
        map.end()
    }
}

struct JsonValue<'a> { value: &'a Value, encoding: Encoding, depth: usize }

impl Serialize for JsonValue<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        if self.depth > 128 { return Err(serde::ser::Error::custom("Diagnostic context nesting exceeds its limit")); }
        let node = |value| JsonValue { value, encoding: self.encoding, depth: self.depth + 1 };
        match wrapped(self.value) {
            Some(Wrapped::Keyword(value)) => return self.encoding.serialize("xt:keyword", value, serializer),
            Some(Wrapped::Symbol(value)) => return self.encoding.serialize("xt:symbol", value, serializer),
            Some(Wrapped::Set(value)) if value.is_array() => return self.encoding.serialize("xt:set", node(value), serializer),
            Some(Wrapped::VectorType(value)) if write_type(value, &mut Discard, 0).is_ok() => {
                return match self.encoding {
                    Encoding::Plain => serializer.collect_str(&VectorTypeDisplay(value)),
                    Encoding::Linked => LinkedValue { tag: "xt:type", value: node(value) }.serialize(serializer),
                };
            }
            Some(Wrapped::Map(entries)) if entries.iter().all(|entry| map_entry(entry).and_then(|(key, _)| map_key(key)).is_some()) => {
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                for entry in entries {
                    let (key, value) = map_entry(entry).expect("validated Transit map pair");
                    map.serialize_entry(map_key(key).expect("validated JSON key"), &node(value))?;
                }
                return map.end();
            }
            _ => (),
        }
        match self.value {
            Value::Object(fields) if !fields.contains_key("$transit/type") => {
                let mut map = serializer.serialize_map(Some(fields.len()))?;
                for (key, value) in fields { map.serialize_entry(key, &node(value))?; }
                map.end()
            }
            Value::Array(values) => {
                let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                for value in values { sequence.serialize_element(&node(value))?; }
                sequence.end()
            }
            value => value.serialize(serializer),
        }
    }
}

enum Wrapped<'a> {
    Keyword(&'a str),
    Symbol(&'a str),
    Set(&'a Value),
    VectorType(&'a Value),
    Map(&'a [Value]),
}

fn wrapped(value: &Value) -> Option<Wrapped<'_>> {
    let fields = value.as_object()?;
    let representation = fields.get("value")?;
    match (fields.get("$transit/type")?.as_str()?, fields.len()) {
        ("keyword", 2) => Some(Wrapped::Keyword(representation.as_str()?)),
        ("symbol", 2) => Some(Wrapped::Symbol(representation.as_str()?)),
        ("map", 2) => Some(Wrapped::Map(representation.as_array()?)),
        ("tagged", 3) => match fields.get("tag")?.as_str()? {
            "set" => Some(Wrapped::Set(representation)),
            "xtdb/type" => Some(Wrapped::VectorType(representation)),
            _ => None,
        },
        _ => None,
    }
}

fn map_entry(value: &Value) -> Option<(&Value, &Value)> {
    let [key, value] = value.as_array()?.as_slice() else { return None; };
    Some((key, value))
}

fn map_key(value: &Value) -> Option<&str> {
    match wrapped(value) {
        Some(Wrapped::Keyword(key) | Wrapped::Symbol(key)) => Some(key),
        _ => value.as_str(),
    }
}

struct Discard;
impl fmt::Write for Discard { fn write_str(&mut self, _: &str) -> fmt::Result { Ok(()) } }

struct VectorTypeDisplay<'a>(&'a Value);
impl fmt::Display for VectorTypeDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("#xt/type ")?;
        write_type(self.0, f, 0)
    }
}

fn write_type(value: &Value, output: &mut impl fmt::Write, depth: usize) -> fmt::Result {
    if depth > 128 { return Err(fmt::Error); }
    match wrapped(value) {
        Some(Wrapped::Keyword(keyword)) => write!(output, "{}", TypeForm::Keyword(keyword)),
        Some(Wrapped::Set(set)) => write_type_sequence(set.as_array().ok_or(fmt::Error)?, "#{", "}", output, depth),
        Some(Wrapped::Map(entries)) => {
            output.write_str("{")?;
            for (index, entry) in entries.iter().enumerate() {
                if index != 0 { output.write_str(", ")?; }
                let (key, value) = map_entry(entry).ok_or(fmt::Error)?;
                write_type(key, output, depth + 1)?;
                output.write_str(" ")?;
                write_type(value, output, depth + 1)?;
            }
            output.write_str("}")
        }
        _ => match value {
            Value::String(text) => write!(output, "{}", TypeForm::Text(text)),
            Value::Number(number) if number.is_i64() => write!(output, "{number}"),
            Value::Bool(boolean) => write!(output, "{boolean}"),
            Value::Array(values) => write_type_sequence(values, "[", "]", output, depth),
            _ => Err(fmt::Error),
        },
    }
}

fn write_type_sequence(values: &[Value], open: &str, close: &str, output: &mut impl fmt::Write, depth: usize) -> fmt::Result {
    output.write_str(open)?;
    for (index, value) in values.iter().enumerate() {
        if index != 0 { output.write_str(" ")?; }
        write_type(value, output, depth + 1)?;
    }
    output.write_str(close)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_type_context_projects_without_losing_its_transit_identity() {
        let form = TypeForm::Set(vec![TypeForm::Keyword("utf8"), TypeForm::Keyword("i64")]);
        let code = "xtdb.group-by/incomparable-min-max-types";
        let error = Anomaly::incorrect(code, format!("Incomparable types in min/max aggregate: #xt/type {form}"))
            .with_context("from-type", vector_type_data(&form).unwrap());

        let bytes = crate::transit::encode_anomaly(&error).unwrap();
        assert_eq!(crate::transit::decode_anomaly(&bytes).unwrap(), error);
        let plain = serde_json::to_value(JsonAnomaly::from(&error)).unwrap();
        assert_eq!(plain["data"]["from-type"], "#xt/type #{:utf8 :i64}");
        let linked = json_ld(&error).unwrap();
        assert_eq!(linked["@type"], "xt:incorrect");
        let data = &linked["@value"]["xtdb.error/data"];
        assert_eq!(data["xtdb.error/code"], serde_json::json!({"@type":"xt:keyword","@value":code}));
        assert_eq!(data["from-type"], serde_json::json!({"@type":"xt:type","@value":{"@type":"xt:set","@value":[
            {"@type":"xt:keyword","@value":"utf8"}, {"@type":"xt:keyword","@value":"i64"}
        ]}}));
    }

    #[test]
    fn invalid_and_unknown_legacy_wrappers_keep_their_diagnostic_data() {
        let legacy = serde_json::json!([
            {"$transit/type":"keyword","value":42},
            {"$transit/type":"tagged","tag":"xtdb/type","value":null},
            {"$transit/type":"tagged","tag":"further/type","value":true}
        ]);
        let error = Anomaly::incorrect("test/legacy", "legacy context").with_context("legacy", legacy.clone());
        assert_eq!(serde_json::to_value(JsonAnomaly::from(&error)).unwrap()["data"]["legacy"], legacy);
        assert_eq!(json_ld(&error).unwrap()["@value"]["xtdb.error/data"]["legacy"], legacy);
    }

    #[test]
    fn a_bounded_json_writer_can_stop_during_a_typed_vector_form() {
        struct Limited { remaining: usize, reached: usize }
        impl std::io::Write for Limited {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if bytes.len() > self.remaining { return Err(std::io::Error::other("diagnostic byte limit")); }
                self.remaining -= bytes.len();
                self.reached += bytes.len();
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
        }
        let names = (0..2048).map(|index| format!("f{index}")).collect::<Vec<_>>();
        let fields = names.iter().map(|name| (TypeForm::Text(name), TypeForm::Keyword("i64"))).collect();
        let form = TypeForm::Vector(vec![TypeForm::Keyword("struct"), TypeForm::Map(fields)]);
        let error = Anomaly::incorrect("test/limits", "typed context").with_context("from-type", vector_type_data(&form).unwrap());
        let mut writer = Limited { remaining: 4096, reached: 0 };
        let error = serde_json::to_writer(&mut writer, &JsonAnomaly::from(&error)).unwrap_err();
        assert!(error.to_string().contains("diagnostic byte limit"));
        assert!(writer.reached > 4000);
    }
}
