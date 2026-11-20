use std::fmt;
use std::str::FromStr;

use rtdb_types::{Anomaly, BlockIndex, CivilDate, Result, StorageEpoch, StorageVersion, TableRef};
use serde::Serialize;

pub const CURRENT_STORAGE_VERSION: StorageVersion = StorageVersion(6);

pub fn lex_hex(value: u64) -> String {
    let body = format!("{value:x}");
    format!("{:x}{body}", body.len() - 1)
}

pub fn parse_lex_hex(value: &str) -> Result<u64> {
    let bytes = value.as_bytes();
    if bytes.len() < 2 || !bytes.iter().all(u8::is_ascii_hexdigit) {
        return Err(invalid_key(value));
    }
    let expected = (bytes[0] as char).to_digit(16).unwrap_or_default() as usize + 1;
    if bytes.len() != expected + 1 {
        return Err(invalid_key(value));
    }
    let number = u64::from_str_radix(&value[1..], 16).map_err(|_| invalid_key(value))?;
    if lex_hex(number) != value.to_ascii_lowercase() {
        return Err(invalid_key(value));
    }
    Ok(number)
}

fn nonnegative(value: i64, name: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| Anomaly::incorrect("rtdb.format/negative-position", format!("{name} must be non-negative")))
}

fn signed_position(value: &str) -> Result<i64> {
    i64::try_from(parse_lex_hex(value)?).map_err(|_| invalid_key(value))
}

fn invalid_key(value: &str) -> Anomaly {
    Anomaly::incorrect("rtdb.format/invalid-key", format!("Invalid storage key: {value}"))
}

pub fn storage_root(version: StorageVersion, epoch: StorageEpoch, partition: u32, partitions: u32) -> Result<String> {
    if partitions == 0 || partition >= partitions {
        return Err(Anomaly::incorrect("xtdb/invalid-partition", format!("Partition {partition} is outside 0..{partitions}")));
    }
    let version = nonnegative(i64::from(version.0), "storage version")?;
    let epoch = nonnegative(i64::from(epoch.0), "storage epoch")?;
    let mut root = format!("v{}", lex_hex(version));
    if epoch > 0 {
        root.push_str(&format!("_e{}", lex_hex(epoch)));
    }
    Ok(if partitions == 1 { root } else { format!("parts/{partition}/{root}") })
}

pub fn require_readable_version(version: StorageVersion) -> Result<()> {
    if version == CURRENT_STORAGE_VERSION {
        Ok(())
    } else {
        Err(Anomaly::unsupported("rtdb.format/storage-version", format!("Unsupported storage version {}", version.0)))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
pub struct TableSlug(String);

impl TableSlug {
    pub fn recorded(slug: impl Into<String>) -> Result<Self> {
        let slug = slug.into();
        if slug.is_empty() || matches!(slug.as_str(), "." | "..") || slug.contains(['/', '\\', '\0']) {
            return Err(invalid_key(&slug));
        }
        Ok(Self(slug))
    }

    pub fn for_unrecorded_table(table: &TableRef) -> Result<Self> {
        Self::recorded(format!("{}${}", table.schema, table.table).replace(['.', '/'], "$"))
    }

    pub fn as_str(&self) -> &str { &self.0 }

    pub fn table_prefix(&self) -> String { format!("tables/{}", self.0) }

    pub fn data_key(&self, trie: &TrieKey) -> String { format!("{}/data/{trie}.arrow", self.table_prefix()) }

    pub fn metadata_key(&self, trie: &TrieKey) -> String { format!("{}/meta/{trie}.arrow", self.table_prefix()) }

    /// Every artefact derived from one trie lives under this prefix, so collecting the trie can list and delete them all.
    pub fn artefact_prefix(&self, trie: &TrieKey) -> String { format!("{}/artefacts/{trie}", self.table_prefix()) }

    /// Every artefact of the table, under each of its tries' prefixes.
    pub fn artefacts_prefix(&self) -> String { format!("{}/artefacts", self.table_prefix()) }

    /// The trie, kind, definition id and version an artefact key was made from, or `None` for a key `artefact_key` does not make.
    pub fn parse_artefact_key(&self, key: &str) -> Option<(TrieKey, String, String, i64)> {
        let rest = key.strip_prefix(&self.artefacts_prefix())?.strip_prefix('/')?;
        let mut parts = rest.split('/');
        let (trie, kind, file, None) = (parts.next()?, parts.next()?, parts.next()?, parts.next()) else { return None };
        let (id, version) = file.strip_suffix(".bin")?.rsplit_once("-v")?;
        let (trie, version) = (trie.parse::<TrieKey>().ok()?, version.parse::<i64>().ok()?);
        (self.artefact_key(&trie, kind, id, version).ok()? == key).then(|| (trie, kind.to_string(), id.to_string(), version))
    }

    pub fn artefact_key(&self, trie: &TrieKey, kind: &str, definition_id: &str, definition_version: i64) -> Result<String> {
        for component in [kind, definition_id] {
            if component.is_empty() || matches!(component, "." | "..") || component.contains(['/', '\\', '\0']) { return Err(invalid_key(component)); }
        }
        Ok(format!("{}/{kind}/{definition_id}-v{definition_version}.bin", self.artefact_prefix(trie)))
    }

    pub fn block_key(&self, block: BlockIndex) -> Result<String> {
        Ok(format!("{}/{}", self.table_prefix(), block_key(block)?))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub struct TrieKey {
    level: i64,
    recency: Option<CivilDate>,
    part: Vec<u8>,
    block_index: BlockIndex,
}

impl TrieKey {
    pub fn new(level: i64, recency: Option<CivilDate>, part: Vec<u8>, block_index: BlockIndex) -> Result<Self> {
        nonnegative(level, "trie level")?;
        nonnegative(block_index.0, "block index")?;
        if part.iter().any(|bucket| *bucket > 3) || recency.is_some_and(|date| date.year() < 0) {
            return Err(Anomaly::incorrect("rtdb.format/invalid-trie-key", "Trie partitions use base-four buckets and recency years must be non-negative"));
        }
        Ok(Self { level, recency, part, block_index })
    }

    pub fn l0(block_index: BlockIndex) -> Result<Self> { Self::new(0, None, Vec::new(), block_index) }
    pub fn level(&self) -> i64 { self.level }
    pub fn recency(&self) -> Option<CivilDate> { self.recency }
    pub fn part(&self) -> &[u8] { &self.part }
    pub fn block_index(&self) -> BlockIndex { self.block_index }
}

impl fmt::Display for TrieKey {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(output, "l{}-r", lex_hex(self.level as u64))?;
        match self.recency {
            None => output.write_str("c")?,
            Some(date) => write!(output, "{:04}{:02}{:02}", date.year(), date.month(), date.day())?,
        }
        if !self.part.is_empty() {
            output.write_str("-p")?;
            for bucket in &self.part { write!(output, "{bucket}")?; }
        }
        write!(output, "-b{}", lex_hex(self.block_index.0 as u64))
    }
}

impl FromStr for TrieKey {
    type Err = Anomaly;

    fn from_str(key: &str) -> Result<Self> {
        let parts: Vec<_> = key.split('-').collect();
        if !(parts.len() == 3 || parts.len() == 4) { return Err(invalid_key(key)); }
        let level = signed_position(parts[0].strip_prefix('l').ok_or_else(|| invalid_key(key))?)?;
        let recency = parts[1].strip_prefix('r').ok_or_else(|| invalid_key(key))?;
        let recency = if recency == "c" {
            None
        } else {
            let recency = recency.strip_prefix('+').unwrap_or(recency);
            if !(5..=13).contains(&recency.len()) || !recency.bytes().all(|c| c.is_ascii_digit()) { return Err(invalid_key(key)); }
            let split = recency.len() - 4;
            let year = recency[..split].parse::<i32>().map_err(|_| invalid_key(key))?;
            let month = recency[split..split + 2].parse::<u32>().map_err(|_| invalid_key(key))?;
            let day = recency[split + 2..].parse::<u32>().map_err(|_| invalid_key(key))?;
            if !(1..=12).contains(&month) || !(1..=31).contains(&day) { return Err(invalid_key(key)); }
            Some(CivilDate::from_ymd(year, month, day.min(CivilDate::month_length(year, month))).map_err(|_| invalid_key(key))?)
        };
        let part = if parts.len() == 4 {
            let digits = parts[2].strip_prefix('p').ok_or_else(|| invalid_key(key))?;
            if digits.is_empty() || !digits.bytes().all(|c| (b'0'..=b'3').contains(&c)) { return Err(invalid_key(key)); }
            digits.bytes().map(|c| c - b'0').collect()
        } else { Vec::new() };
        let block = signed_position(parts.last().unwrap().strip_prefix('b').ok_or_else(|| invalid_key(key))?)?;
        Self::new(level, recency, part, BlockIndex(block))
    }
}

pub fn block_key(block: BlockIndex) -> Result<String> {
    Ok(format!("blocks/b{}.binpb", lex_hex(nonnegative(block.0, "block index")?)))
}

pub fn block_index_from_key(key: &str) -> Result<BlockIndex> {
    let name = key.rsplit('/').next().unwrap_or(key);
    let index = name.strip_prefix('b').and_then(|value| value.strip_suffix(".binpb")).ok_or_else(|| invalid_key(key))?;
    Ok(BlockIndex(signed_position(index)?))
}

pub fn oversized_key(block: BlockIndex, payload_id: &str) -> Result<String> {
    if payload_id.is_empty() || payload_id.contains(['/', '\\', '\0']) || matches!(payload_id, "." | "..") {
        return Err(invalid_key(payload_id));
    }
    Ok(format!("oversized/b{}-{payload_id}.binpb", lex_hex(nonnegative(block.0, "block index")?)))
}

pub fn oversized_block_index(key: &str) -> Option<BlockIndex> {
    let name = key.rsplit('/').next()?;
    let name = name.strip_prefix('b')?.strip_suffix(".binpb")?;
    let (index, payload) = name.split_once('-')?;
    if payload.is_empty() { return None; }
    signed_position(index).ok().map(BlockIndex)
}
