use std::{cell::Cell, collections::BTreeMap, fmt};

use arrow_array::builder::Float64Builder;
use rtdb_types::{ListValue, Result, Value};
use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::value::RawValue;

use super::{float, invalid, json_number, json_tag, read_json_legacy};

pub(super) fn read_json(text: &str) -> Result<Value> {
    let text = text.trim();
    let input = Input {
        text,
        offset: Cell::new(0),
    };
    let mut deserializer = serde_json::Deserializer::from_str(text);
    match input.read(&mut deserializer).and_then(|node| {
        deserializer.end()?;
        Ok(node)
    }) {
        Ok(node) => node.into_value(),
        // The raw decoder retains its syntax diagnostics and admits deeper nesting than serde's visitors.
        Err(_) => read_json_legacy(text),
    }
}

struct Input<'a> {
    text: &'a str,
    offset: Cell<usize>,
}

impl Input<'_> {
    // Serde validates the delimiters. This cursor lets each seed choose a container
    // visitor or borrow a scalar token with its original spelling.
    fn next_token(&self) -> usize {
        self.offset.get()
            + self.text.as_bytes()[self.offset.get()..]
                .iter()
                .take_while(|byte| matches!(byte, b' ' | b'\t' | b'\r' | b'\n' | b',' | b':'))
                .count()
    }
}

impl<'de> Input<'de> {
    fn read<D: Deserializer<'de>>(
        &self,
        deserializer: D,
    ) -> std::result::Result<Node<'de>, D::Error> {
        let start = self.next_token();
        let kind = match self.text.as_bytes().get(start) {
            Some(open @ (b'[' | b'{')) => {
                self.offset.set(start + 1);
                let kind = if *open == b'[' {
                    deserializer.deserialize_seq(self)?
                } else {
                    deserializer.deserialize_map(self)?
                };
                self.offset.set(self.next_token() + 1);
                kind
            }
            _ => {
                let raw = <&RawValue as serde::Deserialize>::deserialize(deserializer)?.get();
                self.offset.set(start + raw.len());
                Kind::Scalar
            }
        };
        Ok(Node {
            raw: &self.text[start..self.offset.get()],
            kind,
        })
    }
}

impl<'de> DeserializeSeed<'de> for &Input<'de> {
    type Value = Node<'de>;

    fn deserialize<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        self.read(deserializer)
    }
}

impl<'de> Visitor<'de> for &Input<'de> {
    type Value = Kind<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON array or object")
    }

    fn visit_seq<A: SeqAccess<'de>>(
        self,
        mut sequence: A,
    ) -> std::result::Result<Self::Value, A::Error> {
        let mut values = Vec::new();
        while let Some(child) = sequence.next_element_seed(self)? {
            values.push(child);
        }
        Ok(Kind::Array(values))
    }

    fn visit_map<A: MapAccess<'de>>(
        self,
        mut map: A,
    ) -> std::result::Result<Self::Value, A::Error> {
        let mut fields = Vec::new();
        while let Some(key) = map.next_key::<&RawValue>()? {
            let key = key.get();
            self.offset.set(self.next_token() + key.len());
            fields.push((key, map.next_value_seed(self)?));
        }
        Ok(Kind::Object(fields))
    }
}

struct Node<'a> {
    raw: &'a str,
    kind: Kind<'a>,
}

enum Kind<'a> {
    Scalar,
    Array(Vec<Node<'a>>),
    Object(Vec<(&'a str, Node<'a>)>),
}

impl Node<'_> {
    fn into_value(self) -> Result<Value> {
        Ok(match self.kind {
            Kind::Scalar => match self.raw {
                "null" => Value::Null,
                "true" => Value::Bool(true),
                "false" => Value::Bool(false),
                text if text.starts_with('"') => {
                    Value::Utf8(serde_json::from_str(text).map_err(invalid)?)
                }
                text => json_number(text)?,
            },
            Kind::Array(values) => {
                if let Some(list) = float_list(&values)? {
                    Value::List(list)
                } else {
                    Value::list(
                        values
                            .into_iter()
                            .map(Node::into_value)
                            .collect::<Result<_>>()?,
                    )
                }
            }
            Kind::Object(entries) => {
                let mut fields = BTreeMap::new();
                for (key, value) in entries {
                    let Ok(key) = serde_json::from_str::<String>(key) else {
                        return read_json_legacy(self.raw);
                    };
                    fields.insert(key, value);
                }
                let kind = fields
                    .get("@type")
                    .and_then(|node| serde_json::from_str::<String>(node.raw).ok());
                if let Some(kind) = kind {
                    let value = fields
                        .remove("@value")
                        .ok_or_else(|| invalid("JSON-LD value requires @value"))?;
                    json_tag(&kind, value.raw, || value.into_value())?
                } else {
                    Value::Struct(
                        fields
                            .into_iter()
                            .map(|(key, node)| Ok((key, node.into_value()?)))
                            .collect::<Result<_>>()?,
                    )
                }
            }
        })
    }
}

fn float_list(values: &[Node<'_>]) -> Result<Option<ListValue>> {
    let is_float = |text: &str| {
        matches!(text.as_bytes().first(), Some(b'-' | b'0'..=b'9'))
            && (text.contains(['.', 'e', 'E']) || text.trim_start_matches('-').len() > 64)
    };
    if !values
        .iter()
        .all(|node| node.raw == "null" || is_float(node.raw))
        || !values.iter().any(|node| node.raw != "null")
    {
        return Ok(None);
    }
    let mut builder = Float64Builder::with_capacity(values.len());
    for node in values {
        builder.append_option((node.raw != "null").then(|| float(node.raw)).transpose()?);
    }
    Ok(Some(ListValue::from_f64(builder.finish())))
}

#[cfg(test)]
mod tests {
    use super::{Cell, Input, Kind, Value, read_json, read_json_legacy};

    #[test]
    fn json_input_matches_legacy_names_tags_numbers_and_error_order() {
        for input in [
            r#"{"b":[1.0,-0,-0.0,1e999,18446744073709551616,1234567890123456789012345678901234567890],"a":{"@type":"xt:double","@value":-0}}"#,
            r#"{"inner":[{"ke\"y":true,"x\\":null,"\u00e9":{"$serde_json::private::Number":12}}]}"#,
            r#"{"z":{"@type":"xt:missing"},"z":{"@type":"xt:long","@value":"20"},"@type":false}"#,
            r#"{"@type":"xt:set","@value":[1,1,null,null,{"z":true,"a":false}]}"#,
            r#"{"@type":"xt:long","@value":"20","ignored":{"@type":"xt:missing"}}"#,
            r#"{"@type":"xt:error","@value":{"xtdb.error/message":"no","xtdb.error/data":{"xtdb.error/code":"demo/fault","number":7,"nested":{"z":0,"a":1}}}}"#,
            r#"{"z":{"@type":"xt:long"},"a":{"@type":"xt:missing","@value":12}}"#,
            r#"[{"@type":"xt:missing","@value":12}, ["\ud800"]]"#,
            r#"{"@type":"xt:long","@value":7,"ignored":{"\ud800":0}}"#,
            r#"{"@type":"xt:timestamp","@value":[2]}"#,
            r#"{"z":{"@type":"xt:missing","@value":12},"a":[1,]}"#,
        ] {
            assert_eq!(read_json(input), read_json_legacy(input), "{input}");
        }
        let Value::F64(number) = read_json(r#"{"@type":"xt:double","@value":-0}"#).unwrap() else {
            panic!()
        };
        assert_eq!(number.to_bits(), (-0.0f64).to_bits());
    }

    #[test]
    fn json_input_visitor_borrows_complete_subtrees_and_scalar_tokens() {
        let text = r#"{ "a": [{"b": 1e+090}, "x\"[}]"], "c": {"@value": [], "@type":"xt:set"} }"#;
        let input = Input {
            text,
            offset: Cell::new(0),
        };
        let node = input
            .read(&mut serde_json::Deserializer::from_str(text))
            .unwrap();
        assert_eq!(node.raw, text);
        let Kind::Object(fields) = node.kind else {
            panic!()
        };
        let items = &fields[0].1;
        assert_eq!(items.raw, r#"[{"b": 1e+090}, "x\"[}]"]"#);
        let Kind::Array(items) = &items.kind else {
            panic!()
        };
        let Kind::Object(nested) = &items[0].kind else {
            panic!()
        };
        assert_eq!(nested[0].1.raw, "1e+090");
        assert_eq!(fields[1].1.raw, r#"{"@value": [], "@type":"xt:set"}"#);
    }

    #[test]
    fn json_input_keeps_deep_documents_and_packed_float_bits() {
        let depth = 150;
        let deep = format!("{}null{}", "[".repeat(depth), "]".repeat(depth));
        assert_eq!(read_json(&deep), read_json_legacy(&deep));
        for oid in [114, 3802] {
            let value = super::super::read_text(oid, "[-0.0,null,1e-300,1e999]").unwrap();
            let Some(rtdb_types::ListView::F64(array)) = value.list_view() else {
                panic!("Expected packed doubles")
            };
            assert_eq!(array.value(0).to_bits(), (-0.0f64).to_bits());
            assert_eq!(
                array.iter().collect::<Vec<_>>(),
                [Some(-0.0), None, Some(1e-300), Some(f64::INFINITY)]
            );
        }
    }
}
