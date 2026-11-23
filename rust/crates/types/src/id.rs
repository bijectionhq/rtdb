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
    pub fn as_value(&self) -> crate::Value {
        match self {
            Self::String(value) => crate::Value::Utf8(value.clone()),
            Self::Keyword(value) => crate::Value::Keyword(value.clone()),
            Self::Integer(value) => crate::Value::I64(*value),
            Self::Uuid(value) => crate::Value::Uuid(*value),
        }
    }

    pub fn iid(&self) -> Iid {
        match self {
            Self::Uuid(bytes) => Iid(*bytes),
            Self::String(value) => Self::string_iid(value),
            Self::Keyword(value) => Self::keyword_iid(value.as_str()),
            Self::Integer(value) => Self::prefixed_iid(b"i", &value.to_string()),
        }
    }

    pub fn string_iid(value: &str) -> Iid { Self::prefixed_iid(b"s", value) }
    pub fn keyword_iid(name: &str) -> Iid { Self::prefixed_iid(b"k:", name) }

    /// A version of the streaming keyword hash omitted the colon when writing IIDs.
    /// Equality scans can include this digest for data written by that version.
    pub fn legacy_keyword_iid(name: &str) -> Iid { Self::prefixed_iid(b"k", name) }

    fn prefixed_iid(prefix: &[u8], value: &str) -> Iid {
        let mut hash = Sha256::new();
        hash.update(prefix);
        hash.update(value.as_bytes());
        let mut iid = [0; 16];
        iid.copy_from_slice(&hash.finalize()[..16]);
        Iid(iid)
    }

    /// Complete typed handle bytes. The enclosing value frames its length.
    /// No logical identifier equality depends on the IID digest.
    pub fn handle_bytes(&self) -> Vec<u8> {
        let (tag, payload): (u8, &[u8]) = match self {
            Self::String(value) => (0, value.as_bytes()),
            Self::Keyword(value) => (1, value.as_str().as_bytes()),
            Self::Integer(value) => {
                let mut handle = vec![2];
                handle.extend_from_slice(&((*value as u64) ^ (1 << 63)).to_be_bytes());
                return handle;
            }
            Self::Uuid(value) => (3, value),
        };
        let mut handle = Vec::with_capacity(payload.len() + 1);
        handle.push(tag);
        handle.extend_from_slice(payload);
        handle
    }

    pub fn from_handle_bytes(handle: &[u8]) -> Result<Self> {
        let invalid = || Anomaly::incorrect("rtdb.owned/invalid-handle", "Invalid complete typed identity");
        let (tag, payload) = handle.split_first().ok_or_else(invalid)?;
        match tag {
            0 | 1 => {
                let value = std::str::from_utf8(payload).map_err(|_| invalid())?.to_owned();
                Ok(if *tag == 0 { Self::String(value) } else { Self::Keyword(Keyword::from_name(value)) })
            }
            2 => Ok(Self::Integer((u64::from_be_bytes(payload.try_into().map_err(|_| invalid())?) ^ (1 << 63)) as i64)),
            3 => Ok(Self::Uuid(payload.try_into().map_err(|_| invalid())?)),
            _ => Err(invalid()),
        }
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

const OWNED_SCHEMA_PREFIX: &str = "rtdb_owned_";
const OWNED_SCHEMA_V1: &str = "rtdb_owned_v1";
const OWNED_PATH_CATALOG: &str = ".ordered-paths";
const OWNED_SEARCH_CATALOG: &str = ".search-paths";
const OWNED_RELATION_CATALOG: &str = ".relation-schemas";
pub const OWNED_ATTEMPT_RELATION: &str = "fact_attempts";
pub const OWNED_ROOT_PUBLICATION_RELATION: &str = "fact_publications";
pub const HOSTED_ROOT_RECEIPTS_TABLE: &str = "bj_fact_root_receipts";
pub const HOSTED_RECEIPT_TX_ID: &str = "_rtdb_receipt_tx_id";
const OWNED_WORLD_CLOCKS: &str = ".world-clocks";
const OWNED_WORLD_CATALOG: &str = ".worlds";
const OWNED_WORLD_PREFIX: &str = ".world-";
const OWNED_UNIQUE_PREFIX: &str = ".unique-";
const OWNED_CAPACITY_PREFIX: &str = ".capacity-";
const OWNED_FIXED_RANGE_PREFIX: &str = ".fixed-range-";
const OWNED_PURGE_PREFIX: &str = ".purges-";
const OWNED_LIFETIME_PREFIX: &str = ".lifetime-";
const OWNED_PURGE_FLOOR_PREFIX: &str = ".purge-floors-";
const OWNED_CHANGE_PREFIX: &str = ".changes-";

/// A native relation provisioned in the authenticated principal's namespace.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct OwnedRelation(String);

impl OwnedRelation {
    pub fn new(name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        if name.is_empty() || name.len() > 96 || !name.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')) {
            return Err(Anomaly::incorrect("rtdb.owned/invalid-relation", "Owned relation must be a nonempty lower-case name of at most 96 bytes"));
        }
        Ok(Self(name))
    }

    pub fn name(&self) -> &str { &self.0 }

    pub fn is_root_data(&self) -> bool {
        self.world().is_none() && !self.is_world_catalog()
            && !matches!(self.name(), OWNED_ATTEMPT_RELATION | OWNED_ROOT_PUBLICATION_RELATION)
    }

    pub fn table(&self, owner: &str) -> TableRef { owned_table(owner, &self.0) }

    /// Names the obsolete owner-wide marker only to reject pre-cutover history.
    pub fn obsolete_purge_table(owner: &str) -> TableRef { owned_table(owner, ".purges") }

    pub fn lifetime_table(&self, owner: &str) -> TableRef {
        owned_table(owner, &format!("{OWNED_LIFETIME_PREFIX}{}", self.0))
    }

    pub fn purge_table(&self, owner: &str) -> TableRef { owned_table(owner, &format!("{OWNED_PURGE_PREFIX}{}", self.0)) }

    pub fn purge_floor_table(&self, owner: &str) -> TableRef {
        owned_table(owner, &format!("{OWNED_PURGE_FLOOR_PREFIX}{}", self.0))
    }


    pub fn fixed_range_table(&self, owner: &str) -> TableRef { owned_table(owner, &format!("{OWNED_FIXED_RANGE_PREFIX}{}", self.0)) }

    pub fn capacity_table(&self, owner: &str) -> TableRef { owned_table(owner, &format!("{OWNED_CAPACITY_PREFIX}{}", self.0)) }

    pub fn unique_table(&self, owner: &str) -> TableRef { owned_table(owner, &format!("{OWNED_UNIQUE_PREFIX}{}", self.0)) }

    pub fn path_catalog(owner: &str) -> TableRef { owned_table(owner, OWNED_PATH_CATALOG) }

    pub fn search_catalog(owner: &str) -> TableRef { owned_table(owner, OWNED_SEARCH_CATALOG) }
    pub fn relation_catalog(owner: &str) -> TableRef { owned_table(owner, OWNED_RELATION_CATALOG) }

    pub fn world_clock_table(owner: &str) -> TableRef { owned_table(owner, OWNED_WORLD_CLOCKS) }

    pub fn world_catalog() -> Self { Self(OWNED_WORLD_CATALOG.into()) }

    /// Physical journal table; the reserved name cannot become an owned relation request.
    pub fn change_journal(&self, owner: &str) -> Result<TableRef> {
        if self.is_world_catalog() { return Err(crate::OwnedWorld::unavailable()); }
        Self::from_source(self.name().to_owned())?;
        Ok(Self(format!("{OWNED_CHANGE_PREFIX}{}", self.name())).table(owner))
    }

    pub fn in_world(&self, world: Iid) -> Result<Self> {
        Self::new(self.name())?;
        Ok(Self(format!("{OWNED_WORLD_PREFIX}{world}.{}", self.name())))
    }

    pub fn is_world_catalog(&self) -> bool { self.0 == OWNED_WORLD_CATALOG }

    pub fn world(&self) -> Option<Iid> {
        let (world, _) = self.0.strip_prefix(OWNED_WORLD_PREFIX)?.split_once('.')?;
        world.parse().ok()
    }

    pub fn root_relation(&self) -> Option<Self> {
        let (_, relation) = self.0.strip_prefix(OWNED_WORLD_PREFIX)?.split_once('.')?;
        Self::new(relation).ok()
    }

    /// The ordered source log carries internal relation addresses.
    pub fn from_source(name: String) -> Result<Self> {
        if name == OWNED_WORLD_CATALOG { return Ok(Self(name)); }
        if let Some((world, relation)) = name.strip_prefix(OWNED_WORLD_PREFIX).and_then(|rest| rest.split_once('.')) {
            let id: Iid = world.parse()?;
            let scoped = Self::new(relation)?.in_world(id)?;
            if scoped.0 == name { return Ok(scoped); }
        }
        Self::new(name)
    }

}

fn owned_table(owner: &str, name: &str) -> TableRef {
    let owner = Sha256::digest(owner.as_bytes());
    let mut physical = String::with_capacity(65 + name.len());
    for byte in owner {
        use fmt::Write;
        write!(&mut physical, "{byte:02x}").expect("writing to a string cannot fail");
    }
    physical.push('-');
    physical.push_str(name);
    TableRef::new(OWNED_SCHEMA_V1, physical)
}

impl TableRef {
    pub fn owned_schema(schema: &str) -> bool { schema.starts_with(OWNED_SCHEMA_PREFIX) }

    pub fn owned_world_scope(&self) -> (bool, Option<Iid>) {
        let Some((_, name)) = self.table.split_once('-') else { return (false, None); };
        if self.schema != OWNED_SCHEMA_V1 { return (false, None); }
        let addressed = name.strip_prefix(OWNED_CHANGE_PREFIX).unwrap_or(name);
        let world = addressed.strip_prefix(OWNED_WORLD_PREFIX)
            .and_then(|rest| rest.split_once('.')).and_then(|(world, _)| world.parse().ok());
        (name == OWNED_WORLD_CATALOG, world)
    }

    /// The protected storage name after its already validated owner namespace.
    pub fn owned_storage_name(&self) -> Option<&str> {
        self.supported_owned_name().then(|| self.table.split_once('-').map(|(_, name)| name)).flatten()
    }

    pub fn is_owned_lifetime_replica_table(name: &str) -> bool {
        name.split_once('/').is_some_and(|(schema, table)|
            Self::owned_schema(schema) && Self::new(schema, table).owned_lifetime_relation().is_some())
    }

    pub fn owned_lifetime_relation(&self) -> Option<&str> {
        self.owned_storage_name()?.strip_prefix(OWNED_LIFETIME_PREFIX)
    }

    pub fn owned_lifetime_schema_catalog(&self) -> Option<Self> {
        self.owned_lifetime_relation()?;
        let (namespace, _) = self.table.split_once('-')?;
        Some(Self::new(OWNED_SCHEMA_V1, format!("{namespace}-{OWNED_RELATION_CATALOG}")))
    }

    /// Maps logical data and its journal to the exact original relation.
    pub fn owned_effect_relation_name(&self) -> Option<&str> {
        let name = self.owned_storage_name()?;
        let relation = name.strip_prefix(OWNED_CHANGE_PREFIX).unwrap_or(name);
        OwnedRelation::from_source(relation.to_owned()).ok()
            .filter(|relation| !relation.is_world_catalog()).map(|_| relation)
    }

    /// The Root relation in the same owner namespace as this actual world table.
    pub fn owned_world_root_table(&self) -> Option<Self> {
        let name = self.owned_storage_name()?;
        let relation = OwnedRelation::from_source(name.to_owned()).ok()?.root_relation()?;
        let (owner, _) = self.table.split_once('-')?;
        Some(Self::new(OWNED_SCHEMA_V1, format!("{owner}-{}", relation.name())))
    }

    pub fn is_owned_change_journal(&self) -> bool {
        self.owned_storage_name().is_some_and(|name| name.starts_with(OWNED_CHANGE_PREFIX))
    }

    pub fn is_owned_world_clock_table(&self) -> bool {
        self.owned_storage_name() == Some(OWNED_WORLD_CLOCKS)
    }

    pub fn is_owned_path_catalog(&self) -> bool {
        self.table.split_once('-').is_some_and(|(_, name)| name == OWNED_PATH_CATALOG)
            && self.supported_owned_name()
    }

    pub fn is_owned_search_catalog(&self) -> bool {
        self.table.split_once('-').is_some_and(|(_, name)| name == OWNED_SEARCH_CATALOG)
            && self.supported_owned_name()
    }

    pub fn is_owned_relation_catalog(&self) -> bool {
        self.owned_storage_name() == Some(OWNED_RELATION_CATALOG)
    }

    /// Identifies derived Root writes with the same owner and source relation.
    pub fn owned_root_constraint_relation(&self) -> Option<&str> {
        let name = self.owned_storage_name()?;
        name.strip_prefix(OWNED_UNIQUE_PREFIX).or_else(|| name.strip_prefix(OWNED_CAPACITY_PREFIX))
    }

    pub fn is_owned_unique_table(&self) -> bool {
        self.table.split_once('-').is_some_and(|(_, name)| name.starts_with(OWNED_UNIQUE_PREFIX)) && self.supported_owned_name()
    }

    /// True only for ordinary owned relations within this protected catalog's namespace.
    pub fn owns_path_target(&self, target: &Self) -> bool {
        (self.is_owned_path_catalog() || self.is_owned_search_catalog())
            && target.supported_owned_name()
            && target.table.split_once('-').is_some_and(|(_, logical)| OwnedRelation::new(logical).is_ok())
            && target.table[..64] == self.table[..64]
    }

    /// A principal's logical data relation or its world overlay. Protected
    /// metadata tables use ordinary IID events and are outside this lane.
    pub fn is_owned_event_data(&self) -> bool {
        self.schema == OWNED_SCHEMA_V1
            && self.table.split_once('-').is_some_and(|(owner, name)| {
                owner.len() == 64 && owner.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    && OwnedRelation::from_source(name.to_owned()).is_ok_and(|relation| !relation.is_world_catalog())
            })
    }

    pub fn supported_owned_name(&self) -> bool {
        self.schema == OWNED_SCHEMA_V1
            && self.table.split_once('-').is_some_and(|(owner, name)| {
                owner.len() == 64 && owner.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    && (matches!(name, OWNED_PATH_CATALOG | OWNED_SEARCH_CATALOG | OWNED_RELATION_CATALOG | OWNED_WORLD_CLOCKS)
                        || name.strip_prefix(OWNED_LIFETIME_PREFIX).is_some_and(|relation| OwnedRelation::new(relation).is_ok())
                        || name.strip_prefix(OWNED_PURGE_FLOOR_PREFIX).is_some_and(|relation| OwnedRelation::new(relation).is_ok())
                        || name.strip_prefix(OWNED_PURGE_PREFIX).is_some_and(|relation| OwnedRelation::new(relation).is_ok())
                        || name.strip_prefix(OWNED_UNIQUE_PREFIX).is_some_and(|relation| OwnedRelation::new(relation).is_ok())
                        || name.strip_prefix(OWNED_CAPACITY_PREFIX).is_some_and(|logical| OwnedRelation::new(logical).is_ok())
                        || name.strip_prefix(OWNED_FIXED_RANGE_PREFIX).is_some_and(|logical| OwnedRelation::new(logical).is_ok())
                        || name.strip_prefix(OWNED_CHANGE_PREFIX).is_some_and(|logical|
                            OwnedRelation::from_source(logical.into()).is_ok_and(|relation| !relation.is_world_catalog()))
                        || OwnedRelation::from_source(name.into()).is_ok())
            })
    }
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}.{}", self.schema, self.table) }
}
