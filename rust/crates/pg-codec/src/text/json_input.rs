use std::{cell::Cell, collections::BTreeMap, fmt};

use arrow_array::builder::Float64Builder;
use rtdb_types::{ListValue, Result, Value};
use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::value::RawValue;

use super::{float, invalid, json_number, json_tag};

pub(super) fn read_json(text: &str) -> Result<Value> {
    let text = text.trim();
    let input = Input {
        text,
        offset: Cell::new(0),
    };
    let mut deserializer = serde_json::Deserializer::from_str(text);
    // Depth is bounded by the caller's input and stack; do not reject valid nested documents at serde's 128-level default.
    deserializer.disable_recursion_limit();
    let node = input.read(&mut deserializer).map_err(invalid)?;
    deserializer.end().map_err(invalid)?;
    node.into_value()
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
                    let key = serde_json::from_str::<String>(key).map_err(invalid)?;
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
    use super::{BTreeMap, Cell, Input, Kind, Value, read_json};

    #[test]
    fn json_input_preserves_names_tags_numbers_and_error_order() {
        let Value::Struct(top) = read_json(
            r#"{"b":[1.0,-0,-0.0,1e999,18446744073709551616,1234567890123456789012345678901234567890],"a":{"@type":"xt:double","@value":-0}}"#,
        )
        .unwrap() else {
            panic!("expected struct");
        };
        let Value::F64(number) = top["a"] else { panic!() };
        assert_eq!(number.to_bits(), (-0.0f64).to_bits());

        let Value::Struct(inner) = read_json(
            r#"{"inner":[{"ke\"y":true,"x\\":null,"\u00e9":{"$serde_json::private::Number":12}}]}"#,
        )
        .unwrap() else {
            panic!()
        };
        assert!(inner.contains_key("inner"));

        // Duplicate keys keep the last value; @type=false is not a JSON-LD tag.
        let Value::Struct(dups) = read_json(
            r#"{"z":{"@type":"xt:missing"},"z":{"@type":"xt:long","@value":"20"},"@type":false}"#,
        )
        .unwrap() else {
            panic!()
        };
        assert_eq!(dups["z"], Value::I64(20));
        assert_eq!(dups["@type"], Value::Bool(false));

        // JSON-LD set deduplicates values, and tagged values ignore sibling fields.
        let Value::Set(items) = read_json(
            r#"{"@type":"xt:set","@value":[1,1,null,null,{"z":true,"a":false}]}"#,
        )
        .unwrap() else {
            panic!()
        };
        assert_eq!(items.len(), 3);
        assert_eq!(
            read_json(r#"{"@type":"xt:long","@value":"20","ignored":{"@type":"xt:missing"}}"#).unwrap(),
            Value::I64(20)
        );
        assert_eq!(
            read_json(r#"{"@type":"xt:long","@value":7,"ignored":{"\ud800":0}}"#).unwrap(),
            Value::I64(7)
        );

        let Value::Transit(_) = read_json(
            r#"{"@type":"xt:error","@value":{"xtdb.error/message":"no","xtdb.error/data":{"xtdb.error/code":"demo/fault","number":7,"nested":{"z":0,"a":1}}}}"#,
        )
        .unwrap() else {
            panic!()
        };

        // Keys convert in sorted order so "a"'s unsupported tag fails before "z"'s missing @value.
        assert_eq!(
            read_json(r#"{"z":{"@type":"xt:long"},"a":{"@type":"xt:missing","@value":12}}"#)
                .unwrap_err()
                .code,
            "xtdb.postgres/unsupported-json-ld-value"
        );
        // Array elements convert in order, before later invalid string escapes.
        assert_eq!(
            read_json(r#"[{"@type":"xt:missing","@value":12}, ["\ud800"]]"#)
                .unwrap_err()
                .code,
            "xtdb.postgres/unsupported-json-ld-value"
        );
        // Syntax errors fail before semantic tag conversion, and invalid object keys fail cleanly.
        assert_eq!(
            read_json(r#"{"z":{"@type":"xt:missing","@value":12},"a":[1,]}"#)
                .unwrap_err()
                .code,
            "xtdb.postgres/invalid-column-value"
        );
        assert_eq!(
            read_json(r#"{"@type":"xt:timestamp","@value":[2]}"#)
                .unwrap_err()
                .code,
            "xtdb.postgres/invalid-column-value"
        );
        assert_eq!(
            read_json(r#"{"\ud800":0}"#).unwrap_err().code,
            "xtdb.postgres/invalid-column-value"
        );
        let _ = BTreeMap::<String, Value>::new();
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
        let mut expected = Value::Null;
        for _ in 0..depth {
            expected = Value::list(vec![expected]);
        }
        assert_eq!(read_json(&deep).unwrap(), expected);
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
