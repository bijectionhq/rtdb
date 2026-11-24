use crate::{Anomaly, EntityId, Result, StructFields, Timestamp, Value};

pub const FIXED_RANGE_COMPONENT_BYTES: usize = 4096;
pub const FIXED_RANGE_MAX_GROUPS: i64 = 64;
pub const FIXED_RANGE_MAX_IDS: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FixedRangeRule {
    pub path: String,
    pub column: String,
    pub valid_at: Timestamp,
}

impl FixedRangeRule {
    pub fn to_value(&self) -> Value {
        Value::Struct(StructFields::from([
            ("path".into(), Value::Utf8(self.path.clone())),
            ("column".into(), Value::Utf8(self.column.clone())),
            ("valid_at".into(), Value::I64(self.valid_at.0)),
        ]))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let Value::Struct(fields) = value else {
            return Err(invalid());
        };
        let (Some(Value::Utf8(path)), Some(Value::Utf8(column)), Some(Value::I64(valid_at))) = (
            fields.get("path"),
            fields.get("column"),
            fields.get("valid_at"),
        ) else {
            return Err(invalid());
        };
        if fields.len() != 3 || *valid_at == Timestamp::MAX.0 {
            return Err(invalid());
        }
        crate::OwnedRelation::new(path.as_str())?;
        if column.is_empty() || column.len() > 96 || column.starts_with('_') {
            return Err(invalid());
        }
        Ok(Self {
            path: path.clone(),
            column: column.clone(),
            valid_at: Timestamp(*valid_at),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct FixedRangeKey {
    binary: Vec<u8>,
    id: String,
}

impl FixedRangeKey {
    pub fn from_cells(binary: &Value, id: &EntityId) -> Result<Self> {
        let EntityId::String(id) = id else {
            return Err(invalid());
        };
        Self::new(binary, id)
    }

    fn new(binary: &Value, id: &str) -> Result<Self> {
        let Value::Binary(binary) = binary else {
            return Err(invalid());
        };
        Self::from_parts(binary, id)
    }

    pub fn from_parts(binary: &[u8], id: &str) -> Result<Self> {
        if binary.len() > FIXED_RANGE_COMPONENT_BYTES || id.len() > FIXED_RANGE_COMPONENT_BYTES {
            return Err(invalid());
        }
        Ok(Self {
            binary: binary.to_vec(),
            id: id.to_owned(),
        })
    }

    pub fn from_bound(bound: &StructFields<Value>, column: &str) -> Result<Self> {
        if bound.len() != 2 {
            return Err(invalid());
        }
        let (Some(binary), Some(Value::Utf8(id))) = (bound.get(column), bound.get("_id")) else {
            return Err(invalid());
        };
        Self::new(binary, id)
    }

    pub fn to_value(&self) -> Value {
        Value::list(vec![
            Value::Binary(self.binary.clone()),
            Value::Utf8(self.id.clone()),
        ])
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let Value::List(values) = value else {
            return Err(invalid());
        };
        let values = values.view();
        if values.len() != 2 { return Err(invalid()); }
        let binary = values.binary(0).ok_or_else(invalid)?;
        let id = values.utf8(1).ok_or_else(invalid)?;
        Self::from_parts(binary, id)
    }

    pub fn belongs_to(&self, id: &EntityId) -> bool {
        matches!(id, EntityId::String(id) if id == &self.id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FixedRangeEvidence {
    pub rule: FixedRangeRule,
    pub revision: i64,
    pub lower: Option<FixedRangeKey>,
    pub upper: Option<FixedRangeKey>,
}

impl FixedRangeEvidence {
    pub fn retained_bytes(&self) -> usize {
        let bytes = |key: &FixedRangeKey| key.binary.capacity() + key.id.capacity();
        1024 + self.rule.path.capacity()
            + self.rule.column.capacity()
            + self.lower.as_ref().map_or(0, bytes)
            + self.upper.as_ref().map_or(0, bytes)
    }

    pub fn contains(&self, key: &FixedRangeKey) -> bool {
        self.lower.as_ref().is_none_or(|lower| key >= lower)
            && self.upper.as_ref().is_none_or(|upper| key < upper)
    }

    pub fn to_value(&self) -> Value {
        Value::Struct(StructFields::from([
            ("rule".into(), self.rule.to_value()),
            ("revision".into(), Value::I64(self.revision)),
            (
                "lower".into(),
                self.lower
                    .as_ref()
                    .map_or(Value::Null, FixedRangeKey::to_value),
            ),
            (
                "upper".into(),
                self.upper
                    .as_ref()
                    .map_or(Value::Null, FixedRangeKey::to_value),
            ),
        ]))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let Value::Struct(fields) = value else {
            return Err(invalid());
        };
        let (Some(rule), Some(Value::I64(revision)), Some(lower), Some(upper)) = (
            fields.get("rule"),
            fields.get("revision"),
            fields.get("lower"),
            fields.get("upper"),
        ) else {
            return Err(invalid());
        };
        if fields.len() != 4 || *revision < 0 {
            return Err(invalid());
        }
        let key = |value: &Value| {
            if *value == Value::Null {
                Ok(None)
            } else {
                FixedRangeKey::from_value(value).map(Some)
            }
        };
        Ok(Self {
            rule: FixedRangeRule::from_value(rule)?,
            revision: *revision,
            lower: key(lower)?,
            upper: key(upper)?,
        })
    }
}

fn invalid() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.owned/fixed-range-evidence",
        "Invalid fixed owned range evidence",
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn fixed_range_key_accepts_packed_binary_and_text_legs() {
        use std::sync::Arc;
        use arrow_array::{Array, BinaryArray, StringArray, UnionArray};
        use arrow_schema::{DataType, Field, UnionFields};

        let fields = UnionFields::try_new([3, 7], [
            Field::new("binary", DataType::Binary, false),
            Field::new("utf8", DataType::Utf8, false),
        ]).unwrap();
        let bytes: &[u8] = &[5, 2];
        let array = UnionArray::try_new(fields, vec![3, 7].into(), Some(vec![0, 0].into()), vec![
            Arc::new(BinaryArray::from(vec![bytes])),
            Arc::new(StringArray::from(vec!["a"])),
        ]).unwrap();
        let field = Arc::new(Field::new("value", array.data_type().clone(), false));
        let list = crate::ListValue::from_scalar_arrow(field, Arc::new(array)).unwrap();
        let key = super::FixedRangeKey::from_value(&crate::Value::List(list)).unwrap();
        assert_eq!(key.binary, bytes);
        assert_eq!(key.id, "a");
    }
}
