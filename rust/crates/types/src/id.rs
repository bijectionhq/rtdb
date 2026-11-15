use crate::{Anomaly, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fmt, str::FromStr};

#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Iid(pub [u8; 16]);

impl Iid {
    pub const MIN: Self = Self([0; 16]);
    pub const MAX: Self = Self([u8::MAX; 16]);

    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        let bytes: [u8; 16] = bytes.try_into().map_err(|_| {
            Anomaly::incorrect("xtdb/invalid-iid", "An IID contains exactly 16 bytes")
                .with_context("length", bytes.len().into())
        })?;
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8; 16] { &self.0 }

    /// Returns the unsigned two-bit bucket at a trie depth in `0..64`.
    pub fn bucket(&self, depth: usize) -> Result<u8> {
        if depth >= 64 {
            return Err(Anomaly::incorrect("xtdb/invalid-trie-depth", "IID trie depth exceeds 63"));
        }
        Ok((self.0[depth / 4] >> (6 - (depth % 4) * 2)) & 3)
    }
}

impl fmt::Display for Iid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 { write!(f, "{byte:02x}")?; }
        Ok(())
    }
}

impl fmt::Debug for Iid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { fmt::Display::fmt(self, f) }
}

impl FromStr for Iid {
    type Err = Anomaly;
    fn from_str(value: &str) -> Result<Self> {
        if value.len() != 32 || !value.is_ascii() {
            return Err(Anomaly::incorrect("xtdb/invalid-iid", "An IID is 32 hexadecimal digits"));
        }
        let mut bytes = [0; 16];
        for (idx, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&value[idx * 2..idx * 2 + 2], 16)
                .map_err(|_| Anomaly::incorrect("xtdb/invalid-iid", "Invalid hexadecimal IID"))?;
        }
        Ok(Self(bytes))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(from = "String", into = "String")]
pub struct Keyword(String);

impl Keyword {
    /// Accepts a Clojure keyword spelling, with or without its leading colon.
    pub fn parse(value: impl AsRef<str>) -> Result<Self> {
        let value = value.as_ref();
        let name = value.strip_prefix(':').unwrap_or(value);
        if name.is_empty() || name.starts_with(':') || name.chars().any(char::is_whitespace) {
            return Err(Anomaly::incorrect("xtdb/invalid-keyword", "Invalid keyword spelling"));
        }
        Ok(Self(name.to_owned()))
    }
    pub fn as_str(&self) -> &str { &self.0 }
    pub fn retained_bytes(&self) -> usize { self.0.capacity() }
    pub fn from_name(name: impl Into<String>) -> Self { Self(name.into()) }
}

impl From<String> for Keyword { fn from(value: String) -> Self { Self::from_name(value) } }
impl From<Keyword> for String {
    fn from(value: Keyword) -> Self { value.0 }
}
impl fmt::Display for Keyword {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, ":{}", self.0) }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum EntityId {
    String(String),
    Keyword(Keyword),
    Integer(i64),
    Uuid([u8; 16]),
}

impl EntityId {
    pub fn iid(&self) -> Iid {
        let input = match self {
            Self::Uuid(bytes) => return Iid(*bytes),
            Self::String(value) => format!("s{value}"),
            Self::Keyword(value) => format!("k{value}"),
            Self::Integer(value) => format!("i{value}"),
        };
        let hash = Sha256::digest(input.as_bytes());
        let mut iid = [0; 16];
        iid.copy_from_slice(&hash[..16]);
        Iid(iid)
    }

    pub fn uuid(value: &str) -> Result<Self> {
        uuid::Uuid::parse_str(value).map(|value| Self::Uuid(*value.as_bytes()))
            .map_err(|_| Anomaly::incorrect("xtdb/invalid-id", "Invalid UUID ID"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TableRef {
    pub schema: String,
    pub table: String,
}

impl TableRef {
    pub fn new(schema: impl Into<String>, table: impl Into<String>) -> Self {
        Self { schema: schema.into(), table: table.into() }
    }

    pub fn parse(value: &str) -> Result<Self> {
        let parts: Vec<_> = value.split('.').collect();
        let (schema, table) = match parts.as_slice() {
            [table] => ("public", *table),
            [schema, table] => (*schema, *table),
            _ => return Err(Self::invalid(value)),
        };
        if schema.is_empty() || table.is_empty() || schema.contains('/') || table.contains('/') {
            return Err(Self::invalid(value));
        }
        Ok(Self::new(schema, table))
    }

    fn invalid(value: &str) -> Anomaly {
        Anomaly::incorrect("xtdb.table/invalid-table-ref", format!("Invalid table reference: '{value}'"))
            .with_context("table-ref", value.into())
    }

    pub fn schema_and_table(&self) -> String { format!("{}/{}", self.schema, self.table) }
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}.{}", self.schema, self.table) }
}
