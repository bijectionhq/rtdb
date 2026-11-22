use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{Anomaly, LogicalType, Result, ScalarType, StructFields, TimeUnit, Value};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedSchema(OwnedSchemaWire);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedSchemaWire {
    fields: Vec<Field>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Field {
    name: String,
    #[serde(rename = "type")]
    kind: Kind,
    optional: bool,
    nullable: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Kind {
    Bool {},
    I8 {},
    I16 {},
    I32 {},
    I64 {},
    U8 {},
    U16 {},
    U32 {},
    U64 {},
    F16 {},
    F32 {},
    F64 {},
    Utf8 {},
    Binary {},
    DateDay {},
    Instant {},
    Uuid {},
    Decimal {
        precision: i32,
        scale: i32,
        #[serde(rename = "bitWidth")]
        bit_width: u16,
    },
}

impl Kind {
    fn scalar(self) -> ScalarType {
        match self {
            Self::Bool {} => ScalarType::Bool,
            Self::I8 {} => ScalarType::I8,
            Self::I16 {} => ScalarType::I16,
            Self::I32 {} => ScalarType::I32,
            Self::I64 {} => ScalarType::I64,
            Self::U8 {} => ScalarType::U8,
            Self::U16 {} => ScalarType::U16,
            Self::U32 {} => ScalarType::U32,
            Self::U64 {} => ScalarType::U64,
            Self::F16 {} => ScalarType::F16,
            Self::F32 {} => ScalarType::F32,
            Self::F64 {} => ScalarType::F64,
            Self::Utf8 {} => ScalarType::Utf8,
            Self::Binary {} => ScalarType::Binary,
            Self::DateDay {} => ScalarType::DateDay,
            Self::Uuid {} => ScalarType::Uuid,
            Self::Instant {} => ScalarType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some("UTC".into()),
            },
            Self::Decimal {
                precision,
                scale,
                bit_width,
            } => ScalarType::Decimal {
                precision,
                scale,
                bit_width,
            },
        }
    }

    fn accepts(self, value: &Value) -> bool {
        if matches!(self, Self::Instant {}) {
            return matches!(
                value,
                Value::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: Some(_),
                    ..
                }
            );
        }
        // A compound value is already incompatible, and its inferred type
        // could allocate in proportion to the rejected input.
        match value {
            Value::List(_)
            | Value::Set(_)
            | Value::Map { .. }
            | Value::FixedSizeList(_)
            | Value::TimestampTzRange { .. }
            | Value::Struct(_) => false,
            value => value.logical_type() == LogicalType::Scalar(self.scalar()),
        }
    }

    fn validate(self) -> Result<()> {
        if let Self::Decimal {
            precision,
            scale,
            bit_width,
        } = self
        {
            let maximum = match bit_width {
                128 => 38,
                256 => 76,
                _ => 0,
            };
            if !(1..=maximum).contains(&precision) || !(-128..=precision).contains(&scale) {
                return Err(invalid("Invalid owned decimal precision, scale or width"));
            }
        }
        Ok(())
    }
}

impl OwnedSchema {
    pub const MAX_JSON_BYTES: usize = 64 * 1024;

    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > Self::MAX_JSON_BYTES {
            return Err(invalid("Owned declaration exceeds its byte limit"));
        }
        let schema: OwnedSchemaWire = serde_json::from_slice(bytes)
            .map_err(|_| invalid("Invalid owned declaration fields or type tags"))?;
        if schema.fields.len() > 128 {
            return Err(invalid("An owned declaration accepts at most 128 fields"));
        }
        let mut names = BTreeSet::new();
        for field in &schema.fields {
            if field.name.is_empty()
                || field.name.len() > 128
                || field.name.starts_with('_')
                || field.name.chars().any(char::is_control)
                || !names.insert(&field.name)
            {
                return Err(invalid(
                    "Owned declaration field names must be distinct, nonempty and unreserved",
                ));
            }
            field.kind.validate()?;
        }
        Ok(Self(schema))
    }

    pub fn to_json(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(&self.0).map_err(|_| {
            Anomaly::fault(
                "rtdb.owned/schema-encode",
                "Cannot encode owned declaration",
            )
        })
    }

    pub fn retained_bytes(&self) -> usize {
        self.0.fields.capacity() * (std::mem::size_of::<Field>() + 64)
            + self
                .0
                .fields
                .iter()
                .map(|field| field.name.capacity())
                .sum::<usize>()
    }

    pub fn column_type(&self, name: &str) -> Option<ScalarType> {
        self.0.fields.iter().find(|field| field.name == name).map(|field| field.kind.scalar())
    }

    pub fn validate_row(&self, row: &StructFields<Value>) -> Result<()> {
        for field in &self.0.fields {
            let accepts = match row.get(field.name.as_str()) {
                None | Some(Value::Undefined) => field.optional,
                Some(Value::Null) => field.nullable,
                Some(value) => field.kind.accepts(value),
            };
            if !accepts {
                return Err(nonconforming(&field.name));
            }
        }
        for (name, value) in row {
            if !matches!(name.as_str(), "_id" | "_valid_from" | "_valid_to")
                && !matches!(value, Value::Undefined)
                && !self.0.fields.iter().any(|field| &field.name == name)
            {
                return Err(nonconforming(name));
            }
        }
        Ok(())
    }
}

fn invalid(message: &'static str) -> Anomaly {
    Anomaly::incorrect("rtdb.owned/invalid-declaration", message)
}

fn nonconforming(field: &str) -> Anomaly {
    Anomaly::incorrect(
        "rtdb.owned/nonconforming",
        "Owned row differs from its declaration",
    )
    .with_context("field", field.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declarations_are_closed_over_names_and_types() {
        for payload in [
            r#"{"fields":[{"name":"value","type":{"kind":"i64","extra":1},"optional":false,"nullable":false}]}"#,
            r#"{"fields":[{"name":"value","type":{"kind":"integer"},"optional":false,"nullable":false}]}"#,
            r#"{"fields":[{"name":"_id","type":{"kind":"i64"},"optional":false,"nullable":false}]}"#,
            r#"{"fields":[{"name":"value","type":{"kind":"decimal","precision":39,"scale":2,"bitWidth":128},"optional":false,"nullable":false}]}"#,
        ] {
            assert!(OwnedSchema::from_json(payload.as_bytes()).is_err());
        }
    }

    #[test]
    fn optional_and_nullable_are_independent() {
        let schema = OwnedSchema::from_json(
            br#"{"fields":[
            {"name":"optional","type":{"kind":"i64"},"optional":true,"nullable":false},
            {"name":"nullable","type":{"kind":"u64"},"optional":false,"nullable":true}
        ]}"#,
        )
        .unwrap();
        let mut row = StructFields::new();
        assert!(schema.validate_row(&row).is_err());
        row.insert("nullable".into(), Value::Null);
        assert!(schema.validate_row(&row).is_ok());
        row.insert("optional".into(), Value::Null);
        assert!(schema.validate_row(&row).is_err());
        row.insert("optional".into(), Value::Undefined);
        row.insert("nullable".into(), Value::I64(1));
        assert!(schema.validate_row(&row).is_err());
        row.insert("nullable".into(), Value::U64(u64::MAX));
        assert!(schema.validate_row(&row).is_ok());
    }
}
