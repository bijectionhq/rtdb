use crate::{Anomaly, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{borrow::Cow, fmt, str::FromStr};

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
    Binary(Vec<u8>),
}

impl EntityId {
    pub fn as_value(&self) -> crate::Value {
        match self {
            Self::String(value) => crate::Value::Utf8(value.clone()),
            Self::Keyword(value) => crate::Value::Keyword(value.clone()),
            Self::Integer(value) => crate::Value::I64(*value),
            Self::Uuid(value) => crate::Value::Uuid(*value),
            Self::Binary(value) => crate::Value::Binary(value.clone()),
        }
    }

    pub fn iid(&self) -> Iid {
        match self {
            Self::Uuid(bytes) => Iid(*bytes),
            Self::String(value) => Self::string_iid(value),
            Self::Keyword(value) => Self::keyword_iid(value.as_str()),
            Self::Integer(value) => Self::prefixed_iid(b"i", &value.to_string()),
            Self::Binary(value) => Self::binary_iid(value),
        }
    }

    pub fn string_iid(value: &str) -> Iid { Self::prefixed_iid(b"s", value) }
    pub fn keyword_iid(name: &str) -> Iid { Self::prefixed_iid(b"k", name) }

    pub fn keyword_lookup_iids(name: &str) -> [Iid; 2] {
        [Self::keyword_iid(name), Self::prefixed_iid(b"k:", name)]
    }

    pub fn binary_iid(value: &[u8]) -> Iid { Self::hash_iid(b"b", value) }

    fn prefixed_iid(prefix: &[u8], value: &str) -> Iid { Self::hash_iid(prefix, value.as_bytes()) }

    fn hash_iid(prefix: &[u8], value: &[u8]) -> Iid {
        let mut hash = Sha256::new();
        hash.update(prefix);
        hash.update(value);
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
            Self::Binary(value) => (4, value),
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
            4 => Ok(Self::Binary(payload.to_vec())),
            _ => Err(invalid()),
        }
    }

    pub fn retained_bytes(&self) -> usize {
        match self {
            Self::String(value) => value.capacity(),
            Self::Keyword(value) => value.retained_bytes(),
            Self::Binary(value) => value.capacity(),
            Self::Integer(_) | Self::Uuid(_) => 0,
        }
    }

    pub fn uuid(value: &str) -> Result<Self> {
        uuid::Uuid::parse_str(value).map(|value| Self::Uuid(*value.as_bytes()))
            .map_err(|_| Anomaly::incorrect("xtdb/invalid-id", "Invalid UUID ID"))
    }
}

/// Homogeneous key order of a private identity page, including an empty first bound.
#[derive(Clone, Debug)]
pub enum OwnedIdentityScan<'a> {
    Text(Option<Cow<'a, str>>),
    Binary(Option<Cow<'a, [u8]>>),
    Unbounded,
}

impl<'a> OwnedIdentityScan<'a> {
    pub fn text(after: Option<&'a str>) -> Self { Self::Text(after.map(Cow::Borrowed)) }

    pub fn binary(after: Option<&'a [u8]>) -> Self { Self::Binary(after.map(Cow::Borrowed)) }

    pub fn bound(&self) -> Option<crate::Value> {
        match self {
            Self::Text(after) => after.as_deref().map(|key| crate::Value::Utf8(key.to_owned())),
            Self::Binary(after) => after.as_deref().map(|key| crate::Value::Binary(key.to_vec())),
            Self::Unbounded => None,
        }
    }

    pub fn accepts(&self, ty: &crate::LogicalType) -> bool {
        use crate::{LogicalType, ScalarType};
        match ty {
            LogicalType::Null | LogicalType::Nothing => true,
            LogicalType::Maybe(inner) | LogicalType::RunEndEncoded { value: inner, .. } => self.accepts(inner),
            LogicalType::Scalar(ScalarType::Utf8) => !matches!(self, Self::Binary(_)),
            LogicalType::Scalar(ScalarType::Binary) => !matches!(self, Self::Text(_)),
            _ => false,
        }
    }

    pub fn seek(id: &'a EntityId) -> Result<Self> {
        match id {
            EntityId::String(text) => Ok(Self::text(Some(text))),
            EntityId::Binary(bytes) => Ok(Self::binary(Some(bytes))),
            _ => Err(Anomaly::incorrect("rtdb.owned/identity-page", "An identity page requires Utf8 or Binary keys")),
        }
    }

    pub fn into_owned(self) -> OwnedIdentityScan<'static> {
        match self {
            Self::Text(after) => OwnedIdentityScan::Text(after.map(|key| Cow::Owned(key.into_owned()))),
            Self::Binary(after) => OwnedIdentityScan::Binary(after.map(|key| Cow::Owned(key.into_owned()))),
            Self::Unbounded => OwnedIdentityScan::Unbounded,
        }
    }
}

pub const SHARED_COLUMN_CARRIERS: &str = "fact_unit_shared_column_carriers";
pub const ROOT_ROW_CARRIERS: &str = "fact_protected_row_carriers";
pub const SEALED_SEARCH_UNIT_ROWS: &str = "fact_sealed_search_unit_rows_binary";

/// Decode an inventory key in the exact held unit's Binary prefix.
pub fn sealed_search_inventory_row<'a>(alias: &[u8; 32], key: &'a [u8]) -> Option<(&'a str, &'a [u8])> {
    let (&length, key) = key.strip_prefix(alias)?.split_first()?;
    let (relation, address) = key.split_at_checked(usize::from(length))?;
    let relation = std::str::from_utf8(relation).ok()?;
    if relation.len() > 96 || !relation.starts_with("fact_") || !relation.ends_with("_search")
        || !relation.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-'))
    {
        return None;
    }
    if !is_rank_row_address(address) { return None; }
    Some((relation, address))
}

/// A rank identity may refer to a clear row with a protected field.
pub fn is_rank_row_address(image: &[u8]) -> bool {
    let Some(mut payload) = image.get(17..).and_then(|payload| payload.strip_suffix(&[0, 1])) else { return false; };
    if image[0] != RootRowCarrier::REFERENCE || payload.is_empty() {
        return false;
    }
    if payload[0] == 0 { return RootRowCarrier::parse(image).is_some(); }
    while let Some((&byte, rest)) = payload.split_first() {
        if byte == 0 {
            let Some(escaped) = rest.strip_prefix(&[255]) else { return false; };
            payload = escaped;
        } else {
            payload = rest;
        }
    }
    true
}

#[cfg(test)]
#[test]
fn sealed_inventory_binds_alias_relation_and_raw_address() {
    let alias = [7; 32];
    let relation = b"fact_label_search";
    let raw_address = [&[11][..], &[8; 16], &[9, b'x', 0, 255, 0, 1]].concat();
    let key = [&alias[..], &[relation.len() as u8], relation, &raw_address].concat();
    assert_eq!(sealed_search_inventory_row(&alias, &key), Some(("fact_label_search", raw_address.as_slice())));
    assert!(sealed_search_inventory_row(&[8; 32], &key).is_none());
    assert!(sealed_search_inventory_row(&alias, &key[..key.len() - 1]).is_none());
    let mut malformed = key;
    malformed[32] += 1;
    assert!(sealed_search_inventory_row(&alias, &malformed).is_none());
}

/// The opaque ordered address used by the hosted protected Root row carrier.
#[derive(Clone, Copy)]
pub struct RootRowCarrier<'a>(&'a [u8]);

impl<'a> RootRowCarrier<'a> {
    const REFERENCE: u8 = 11;
    const MARKER: [u8; 2] = [0, 255];
    const END: [u8; 2] = [0, 1];
    const MAX_IMAGE: usize = 17 + 2 * (3 + 4096 + 32 + 4096) + 2;

    pub fn parse(image: &'a [u8]) -> Option<Self> {
        let mut payload = image.get(17..)?.strip_suffix(&Self::END)?.strip_prefix(&Self::MARKER)?;
        if image.len() > Self::MAX_IMAGE || image[0] != Self::REFERENCE { return None; }
        let mut at = 1;
        let mut partition = 0;
        let mut incarnation = false;
        while let Some((&byte, rest)) = payload.split_first() {
            payload = if byte == 0 { rest.strip_prefix(&[255])? } else { rest };
            if at <= 2 { partition = partition * 256 + usize::from(byte); }
            else if (3 + partition..3 + partition + 32).contains(&at) { incarnation |= byte != 0; }
            at += 1;
        }
        let width = at.checked_sub(3 + partition + 32)?;
        (partition <= 4096 && incarnation && matches!(width, 32 | 64 | 128 | 256 | 512 | 1024 | 2048 | 4096))
            .then_some(Self(image))
    }

    pub fn table_prefix(table: &[u8; 16]) -> [u8; 19] {
        let mut prefix = [0; 19];
        prefix[0] = Self::REFERENCE;
        prefix[1..17].copy_from_slice(table);
        prefix[17..].copy_from_slice(&Self::MARKER);
        prefix
    }

    pub fn assertion_key(self) -> Vec<u8> { self.journal_key(1) }
    pub fn retirement_key(self) -> Vec<u8> { self.journal_key(0) }

    pub fn is_assertion_key(id: &'a [u8]) -> bool {
        id.strip_prefix(&[1]).and_then(Self::parse).is_some()
    }

    fn journal_key(self, tag: u8) -> Vec<u8> {
        let mut id = Vec::with_capacity(1 + self.0.len());
        id.push(tag);
        id.extend_from_slice(self.0);
        id
    }

    /// The existing textual name of the durable custody control for this row.
    pub fn fence_name(self) -> String {
        use fmt::Write;
        let mut id = String::with_capacity(4 + 2 * self.0.len());
        id.push_str("row/");
        for byte in self.0 { write!(&mut id, "{byte:02x}").expect("writing to a string"); }
        id
    }

    /// Recover the ordered address bound by a named custody record.
    pub fn decode_named_address(text: &str) -> Option<Vec<u8>> {
        fn nibble(digit: u8) -> Option<u8> {
            match digit {
                b'0'..=b'9' => Some(digit - b'0'),
                b'a'..=b'f' => Some(digit - b'a' + 10),
                _ => None,
            }
        }
        if text.len() > 2 * Self::MAX_IMAGE || text.len() % 2 != 0 { return None; }
        let address: Vec<_> = text.as_bytes().chunks_exact(2)
            .map(|digits| Some(16 * nibble(digits[0])? + nibble(digits[1])?))
            .collect::<Option<_>>()?;
        RootRowCarrier::parse(&address)?;
        Some(address)
    }
}

const SHARED_COLUMN_CARRIER_PREFIX_WIDTH: usize = 5 + 16 + 32 + 16 + 1 + 16 + 16 + 4;

#[derive(Clone, Copy, Debug)]
pub struct SharedColumnCarrierPrefix<'a>(&'a [u8; SHARED_COLUMN_CARRIER_PREFIX_WIDTH]);

impl<'a> SharedColumnCarrierPrefix<'a> {
    pub const WIDTH: usize = SHARED_COLUMN_CARRIER_PREFIX_WIDTH;
    pub const ID_WIDTH: usize = Self::WIDTH + 4096;

    pub fn parse(bytes: &'a [u8]) -> Option<Self> {
        let bytes = <&[u8; SHARED_COLUMN_CARRIER_PREFIX_WIDTH]>::try_from(bytes).ok()?;
        bytes.starts_with(b"BFCP2").then_some(Self(bytes))
    }

    pub fn as_bytes(self) -> &'a [u8] { self.0 }

    pub fn contains(self, id: &[u8]) -> bool {
        id.len() == Self::ID_WIDTH && id.starts_with(self.0)
    }

    pub fn of_identity(id: &'a [u8]) -> Option<Self> {
        let (prefix, payload) = id.split_at_checked(Self::WIDTH)?;
        if payload.len() != Self::ID_WIDTH - Self::WIDTH { return None; }
        Self::parse(prefix)
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

    pub fn stores_expiring_transaction_metadata(&self) -> bool {
        self.schema == "xt" && matches!(self.table.as_str(),
            "idempotency_receipts" | "bounded_retry_receipts" | "rtdb_replay_keys" | crate::PLACED_ATTEMPT_OUTCOME_TABLE)
    }

    pub fn schema_and_table(&self) -> String { format!("{}/{}", self.schema, self.table) }
}

const OWNED_SCHEMA_PREFIX: &str = "rtdb_owned_";
const OWNED_SCHEMA_V1: &str = "rtdb_owned_v1";
const OWNED_PATH_CATALOG: &str = ".ordered-paths";
const OWNED_SEARCH_CATALOG: &str = ".search-paths";
const OWNED_SPATIAL_CATALOG: &str = ".spatial-paths";
const OWNED_VECTOR_CATALOG: &str = ".vector-paths";
const OWNED_RELATION_CATALOG: &str = ".relation-schemas";
const OWNED_ADOPTED_CATALOG: &str = ".adopted-relations";
const OWNED_RETIRED_CATALOG: &str = ".retired-relations";
pub const OWNED_ATTEMPT_RELATION: &str = "fact_attempts";
pub const OWNED_ROOT_PUBLICATION_RELATION: &str = "fact_publications";
pub const OWNED_PUBLIC_ROOT_VERSIONS_RELATION: &str = "fact_public_root_versions";
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
            && !matches!(self.name(), OWNED_ATTEMPT_RELATION | OWNED_ROOT_PUBLICATION_RELATION | OWNED_PUBLIC_ROOT_VERSIONS_RELATION)
    }

    pub fn is_world_custody_control(&self) -> bool {
        let name = self.0.strip_prefix(OWNED_WORLD_PREFIX)
            .and_then(|scoped| scoped.split_once('.')).map_or(self.name(), |(_, relation)| relation);
        matches!(name,
            "fact_unit_fences" | "fact_unit_column_payloads" | "fact_row_probe_floors"
            | "fact_row_legal_holds" | "fact_row_publications" | "fact_sealed_search_erasures")
    }

    pub fn admits_world_ciphertext_points(&self, schema: &crate::OwnedSchema) -> bool {
        match self.name() {
            "fact_unit_column_payloads" => schema.has_only_opaque_column("sealed_payload"),
            ROOT_ROW_CARRIERS => schema.is_opaque_root_carrier(),
            relation if relation.ends_with("_search") => schema.has_only_opaque_column("sealed_world_projection"),
            _ => false,
        }
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
    pub fn spatial_catalog(owner: &str) -> TableRef { owned_table(owner, OWNED_SPATIAL_CATALOG) }
    /// The principal's approximate vector paths (vector.allium: VectorIndexDefinition).
    pub fn vector_catalog(owner: &str) -> TableRef { owned_table(owner, OWNED_VECTOR_CATALOG) }

    pub fn relation_catalog(owner: &str) -> TableRef { owned_table(owner, OWNED_RELATION_CATALOG) }

    /// The principal's retired relations, each fenced against later writes and declarations.
    pub fn retired_catalog(owner: &str) -> TableRef { owned_table(owner, OWNED_RETIRED_CATALOG) }

    #[doc(hidden)]
    pub fn adopted_catalog(owner: &str) -> TableRef { owned_table(owner, OWNED_ADOPTED_CATALOG) }

    /// Every physical table this Root relation owns in the namespace of `namespace`, an owned table of the same principal.
    /// World overlays and their journals stay outside: a world's own retirement reclaims them.
    pub fn physical_tables(&self, namespace: &TableRef) -> Option<Vec<TableRef>> {
        if !self.is_root_data() || !namespace.supported_owned_name() { return None; }
        let (owner, _) = namespace.table.split_once('-')?;
        let name = self.name();
        Some([
            name.to_owned(),
            format!("{OWNED_CHANGE_PREFIX}{name}"),
            format!("{OWNED_UNIQUE_PREFIX}{name}"),
            format!("{OWNED_CAPACITY_PREFIX}{name}"),
            format!("{OWNED_FIXED_RANGE_PREFIX}{name}"),
            format!("{OWNED_PURGE_PREFIX}{name}"),
            format!("{OWNED_PURGE_FLOOR_PREFIX}{name}"),
            format!("{OWNED_LIFETIME_PREFIX}{name}"),
        ].into_iter().map(|table| TableRef::new(OWNED_SCHEMA_V1, format!("{owner}-{table}"))).collect())
    }

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

    /// Identifies purge metadata with the same owner and Root relation as the erased row.
    pub fn owned_purge_relation(&self) -> Option<(&str, bool)> {
        let name = self.owned_storage_name()?;
        name.strip_prefix(OWNED_PURGE_FLOOR_PREFIX).map(|relation| (relation, true))
            .or_else(|| name.strip_prefix(OWNED_PURGE_PREFIX).map(|relation| (relation, false)))
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

    pub fn is_owned_retired_catalog(&self) -> bool {
        self.owned_storage_name() == Some(OWNED_RETIRED_CATALOG)
    }

    /// The approximate vector path catalog in the same owner namespace as this owned table.
    pub fn owned_vector_catalog(&self) -> Option<Self> {
        self.owned_storage_name()?;
        let (namespace, _) = self.table.split_once('-')?;
        Some(Self::new(OWNED_SCHEMA_V1, format!("{namespace}-{OWNED_VECTOR_CATALOG}")))
    }

    /// The world catalog in the same owner namespace as this owned table.
    pub fn owned_world_catalog(&self) -> Option<Self> {
        self.owned_storage_name()?;
        let (namespace, _) = self.table.split_once('-')?;
        Some(Self::new(OWNED_SCHEMA_V1, format!("{namespace}-{OWNED_WORLD_CATALOG}")))
    }

    /// Identifies derived Root writes with the same owner and source relation.
    pub fn owned_root_constraint_relation(&self) -> Option<&str> {
        let name = self.owned_storage_name()?;
        name.strip_prefix(OWNED_UNIQUE_PREFIX).or_else(|| name.strip_prefix(OWNED_CAPACITY_PREFIX))
    }

    pub fn is_owned_spatial_catalog(&self) -> bool {
        self.table.split_once('-').is_some_and(|(_, name)| name == OWNED_SPATIAL_CATALOG)
            && self.supported_owned_name()
    }

    pub fn is_owned_vector_catalog(&self) -> bool {
        self.table.split_once('-').is_some_and(|(_, name)| name == OWNED_VECTOR_CATALOG)
            && self.supported_owned_name()
    }

    pub fn is_owned_unique_table(&self) -> bool {
        self.table.split_once('-').is_some_and(|(_, name)| name.starts_with(OWNED_UNIQUE_PREFIX)) && self.supported_owned_name()
    }

    /// True only for ordinary owned relations within this protected catalog's namespace.
    pub fn owns_path_target(&self, target: &Self) -> bool {
        (self.is_owned_path_catalog() || self.is_owned_search_catalog() || self.is_owned_spatial_catalog() || self.is_owned_vector_catalog())
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

    pub fn is_owned_world_topology(&self) -> bool {
        self.supported_owned_name() && self.table.split_once('-').is_some_and(|(_, name)|
            matches!(name, OWNED_WORLD_CATALOG | OWNED_WORLD_CLOCKS))
    }

    pub fn supported_owned_name(&self) -> bool {
        self.schema == OWNED_SCHEMA_V1
            && self.table.split_once('-').is_some_and(|(owner, name)| {
                owner.len() == 64 && owner.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    && (matches!(name, OWNED_PATH_CATALOG | OWNED_SEARCH_CATALOG | OWNED_SPATIAL_CATALOG | OWNED_VECTOR_CATALOG | OWNED_RELATION_CATALOG | OWNED_RETIRED_CATALOG | OWNED_ADOPTED_CATALOG | OWNED_WORLD_CLOCKS)
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
