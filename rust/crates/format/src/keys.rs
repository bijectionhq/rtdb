use std::fmt;
use std::str::FromStr;

use rtdb_types::{Anomaly, BlockIndex, CivilDate, Result, StorageEpoch, StorageVersion, TableRef};
use serde::Serialize;

pub const CURRENT_STORAGE_VERSION: StorageVersion = StorageVersion(6);

/// Retirement intents and installed receipts belong to the residence that writes them.
pub const TRIE_RETIREMENT_DIRECTORY: &str = "~trie-retirements";

pub fn is_trie_retirement_object(key: &str) -> bool {
    key == TRIE_RETIREMENT_DIRECTORY || key.strip_prefix(TRIE_RETIREMENT_DIRECTORY)
        .is_some_and(|suffix| suffix.starts_with('/'))
}

/// Independently derived ordered indexes belong to one source history.
pub const ORDERED_INDEX_ROOT: &str = "ordered-index/v2";

pub fn is_ordered_index_object(name: &str) -> bool {
    name == ORDERED_INDEX_ROOT || name.strip_prefix(ORDERED_INDEX_ROOT)
        .is_some_and(|suffix| suffix.starts_with('/'))
}


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
    Ok(StorageRoot::partitioned(version, epoch, partition, partitions)?.key())
}

/// Where one partition's objects live in a store: the storage version and epoch, and the partition when there are several.
/// Every object key a database writes is relative to its root, and a provider's configured prefix goes above it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRoot { version: u64, epoch: u64, partition: u32, partitions: u32 }

impl StorageRoot {
    pub fn new(version: StorageVersion, epoch: StorageEpoch) -> Result<Self> { Self::partitioned(version, epoch, 0, 1) }

    pub fn partitioned(version: StorageVersion, epoch: StorageEpoch, partition: u32, partitions: u32) -> Result<Self> {
        if partitions == 0 || partition >= partitions {
            return Err(Anomaly::incorrect("xtdb/invalid-partition", format!("Partition {partition} is outside 0..{partitions}")));
        }
        Ok(Self { version: nonnegative(i64::from(version.0), "storage version")?, epoch: nonnegative(i64::from(epoch.0), "storage epoch")?, partition, partitions })
    }

    /// The root as a key prefix: `v06`, `v06_e110`, `parts/2/v06`.
    pub fn key(&self) -> String {
        let mut root = format!("v{}", lex_hex(self.version));
        if self.epoch > 0 { root.push_str(&format!("_e{}", lex_hex(self.epoch))); }
        if self.partitions == 1 { root } else { format!("parts/{}/{root}", self.partition) }
    }

    /// `root` (a root's key) under a provider's configured prefix, if it has a non-empty one.
    pub fn under_prefix(prefix: Option<&str>, root: &str) -> String {
        match prefix { Some(prefix) if !prefix.is_empty() => format!("{prefix}/{root}"), _ => root.to_owned() }
    }
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

pub fn skipped_tx_key(position: rtdb_types::SourcePosition) -> String {
    let digits = position.0.to_string();
    format!("skipped-txs/{}{digits}", digits.len() - 1)
}

pub fn skipped_tx_position(path: &str) -> Option<rtdb_types::SourcePosition> {
    let name = path.strip_prefix("skipped-txs/")?;
    (1..=2).find_map(|width| {
        let (_, digits) = name.split_at_checked(width)?;
        let position = digits.parse::<i64>().ok().filter(|position| *position >= 0)?;
        let source = rtdb_types::SourcePosition(position);
        (skipped_tx_key(source) == path).then_some(source)
    })
}

/// The dedicated L0 data and metadata objects of an engine receipt table.
pub fn is_receipt_l0_file(key: &str) -> bool {
    let Some((slug, file)) = key.strip_prefix("tables/").and_then(|tail| tail.split_once('/')) else {
        return false;
    };
    let Some(table) = slug.strip_prefix("xt$") else { return false; };
    TableRef::new("xt", table).stores_expiring_transaction_metadata()
        && file.strip_prefix("data/").or_else(|| file.strip_prefix("meta/"))
            .and_then(|file| file.strip_suffix(".arrow"))
            .and_then(|name| name.parse::<TrieKey>().ok())
            .is_some_and(|trie| trie.level() == 0)
}

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
    pub fn artefact_prefix(&self, trie: &TrieKey) -> String { self.owner_prefix(&ArtefactOwner::Trie(trie.clone())) }

    /// Every artefact derived from a partition group's data files lives under this prefix, so collecting its last member can list and delete them all.
    pub fn group_artefact_prefix(&self, group: &PartGroup) -> String { self.owner_prefix(&ArtefactOwner::Group(group.clone())) }

    fn owner_prefix(&self, owner: &ArtefactOwner) -> String {
        match owner {
            ArtefactOwner::Pack(_) => format!("{}/{owner}", self.packs_prefix()),
            _ => format!("{}/artefacts/{owner}", self.table_prefix()),
        }
    }

    pub fn packs_prefix(&self) -> String { format!("{}/artefacts/packs", self.table_prefix()) }

    /// Every artefact of the table, under each of its tries' and partition groups' prefixes.
    pub fn artefacts_prefix(&self) -> String { format!("{}/artefacts", self.table_prefix()) }

    /// The owner, kind, definition id and version an artefact key was made from, or `None` for a key `owner_artefact_key` does not make.
    pub fn parse_artefact_key(&self, key: &str) -> Option<(ArtefactOwner, String, String, i64)> {
        let rest = key.strip_prefix(&self.artefacts_prefix())?.strip_prefix('/')?;
        let rest = rest.strip_prefix("packs/").unwrap_or(rest);
        let mut parts = rest.split('/');
        let (owner, kind, file, None) = (parts.next()?, parts.next()?, parts.next()?, parts.next()) else { return None };
        let (id, version) = file.strip_suffix(".bin")?.rsplit_once("-v")?;
        let (owner, version) = (owner.parse::<ArtefactOwner>().ok()?, version.parse::<i64>().ok()?);
        (self.owner_artefact_key(&owner, kind, id, version).ok()? == key).then(|| (owner, kind.to_string(), id.to_string(), version))
    }

    pub fn artefact_key(&self, trie: &TrieKey, kind: &str, definition_id: &str, definition_version: i64) -> Result<String> {
        self.owner_artefact_key(&ArtefactOwner::Trie(trie.clone()), kind, definition_id, definition_version)
    }

    pub fn owner_artefact_key(&self, owner: &ArtefactOwner, kind: &str, definition_id: &str, definition_version: i64) -> Result<String> {
        for component in [kind, definition_id] {
            if component.is_empty() || matches!(component, "." | "..") || component.contains(['/', '\\', '\0']) { return Err(invalid_key(component)); }
        }
        Ok(format!("{}/{kind}/{definition_id}-v{definition_version}.bin", self.owner_prefix(owner)))
    }

    /// The object a record on `trie`'s entry names: under the trie's own prefix, or under its partition group's for a group-scoped record.
    pub fn recorded_artefact_key(&self, trie: &TrieKey, record: &crate::log::proto::ArtefactRecord) -> Result<String> {
        self.owner_artefact_key(&ArtefactOwner::of(trie, record)?, record.kind(), record.definition_id(), record.definition_version())
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
    /// The naming epoch the trie was planned in; a compactor reset starts a new one, so no name is ever published twice.
    epoch: u64,
}

impl TrieKey {
    pub fn new(level: i64, recency: Option<CivilDate>, part: Vec<u8>, block_index: BlockIndex) -> Result<Self> {
        nonnegative(level, "trie level")?;
        nonnegative(block_index.0, "block index")?;
        if part.iter().any(|bucket| *bucket > 3) || recency.is_some_and(|date| date.year() < 0) {
            return Err(Anomaly::incorrect("rtdb.format/invalid-trie-key", "Trie partitions use base-four buckets and recency years must be non-negative"));
        }
        Ok(Self { level, recency, part, block_index, epoch: 0 })
    }

    /// The same position planned in `epoch`.
    pub fn with_epoch(self, epoch: u64) -> Self { Self { epoch, ..self } }
    pub fn epoch(&self) -> u64 { self.epoch }

    pub fn l0(block_index: BlockIndex) -> Result<Self> { Self::new(0, None, Vec::new(), block_index) }
    pub fn level(&self) -> i64 { self.level }
    pub fn recency(&self) -> Option<CivilDate> { self.recency }
    pub fn part(&self) -> &[u8] { &self.part }
    pub fn block_index(&self) -> BlockIndex { self.block_index }

    /// The precedence of this persisted source when event IID and system time are equal.
    pub fn scan_precedence(&self) -> (i64, BlockIndex, CivilDate, &[u8]) {
        (self.level, self.block_index, self.recency.unwrap_or(CivilDate::MAX), &self.part)
    }

    /// The partition group of a tiered trie: a current trie at level 2 and up, or a historical one at level 3 and up (trie-cat.allium: InsertTrie_General).
    /// Level 0, level 1 and the levelled level-2 historical tries belong to none.
    pub fn part_group(&self) -> Option<PartGroup> {
        let tiered = self.level >= 3 || (self.level == 2 && self.recency.is_none());
        let (_, parent) = self.part.split_last().filter(|_| tiered)?;
        Some(PartGroup { parent: Self { part: parent.to_vec(), ..self.clone() } })
    }
}

/// How many sibling tries complete a tiered partition group, one per IID bucket under their parent part.
pub const BRANCH_FACTOR: u8 = 4;

/// The sibling outputs of tiered compaction at one block: the same level, recency and parent part, one per bucket.
/// They become live together; each is superseded on its own.
/// Its key is its members' with `g` for `l` and the members' own bucket dropped, e.g. `g02-rc-b05` for `l02-rc-p0-b05` to `l02-rc-p3-b05`.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub struct PartGroup { parent: TrieKey }

impl PartGroup {
    pub fn level(&self) -> i64 { self.parent.level }
    pub fn recency(&self) -> Option<CivilDate> { self.parent.recency }
    pub fn block_index(&self) -> BlockIndex { self.parent.block_index }
    /// In bucket order.
    pub fn members(&self) -> Vec<TrieKey> {
        (0..BRANCH_FACTOR).map(|bucket| {
            let mut part = self.parent.part.clone();
            part.push(bucket);
            TrieKey { part, ..self.parent.clone() }
        }).collect()
    }
}

impl fmt::Display for PartGroup {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parent = self.parent.to_string();
        write!(output, "g{}", &parent[1..])
    }
}

impl FromStr for PartGroup {
    type Err = Anomaly;

    fn from_str(key: &str) -> Result<Self> {
        let parent: TrieKey = format!("l{}", key.strip_prefix('g').ok_or_else(|| invalid_key(key))?).parse()?;
        let mut member = parent.clone();
        member.part.push(0);
        member.part_group().filter(|group| group.parent == parent).ok_or_else(|| invalid_key(key))
    }
}

/// What an artefact is derived from, and so the prefix its objects live under.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub enum ArtefactOwner { Trie(TrieKey), Group(PartGroup), Pack(ArtefactPack) }

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub struct ArtefactPack(Vec<TrieKey>);

impl ArtefactPack {
    pub const MAX_MEMBERS: usize = 8;

    pub fn new(mut members: Vec<TrieKey>) -> Result<Self> {
        members.sort_by(|a, b| a.part().cmp(b.part()));
        let length = members.iter().map(|key| key.to_string().len() + 1).sum::<usize>();
        if !(2..=Self::MAX_MEMBERS).contains(&members.len()) || length > 512
            || members.iter().any(|key| key.level() == 0 || key.recency() != members[0].recency())
            || members.windows(2).any(|pair| pair[1].part().starts_with(pair[0].part())) {
            return Err(Anomaly::incorrect("rtdb.format/invalid-artefact-pack", "An artefact pack needs bounded, disjoint primary members with a shared recency"));
        }
        Ok(Self(members))
    }

    pub fn members(&self) -> &[TrieKey] { &self.0 }
    pub fn recency(&self) -> Option<CivilDate> { self.0[0].recency() }
}

impl ArtefactOwner {
    /// The owner of a record on `trie`'s entry: the trie, or its partition group for a group-scoped record.
    pub fn of(trie: &TrieKey, record: &crate::log::proto::ArtefactRecord) -> Result<Self> {
        match record.scope() {
            crate::log::proto::ArtefactScope::Trie => Ok(Self::Trie(trie.clone())),
            crate::log::proto::ArtefactScope::PartGroup => trie.part_group().map(Self::Group)
                .ok_or_else(|| Anomaly::incorrect("rtdb.format/invalid-artefact-scope", format!("Trie {trie} belongs to no partition group"))),
            crate::log::proto::ArtefactScope::Pack => {
                if record.pack_members.len() > ArtefactPack::MAX_MEMBERS { return Err(invalid_key("artefact pack members")); }
                let members = record.pack_members.iter().map(|key| key.parse()).collect::<Result<Vec<_>>>()?;
                let pack = ArtefactPack::new(members)?;
                if !pack.members().contains(trie) || !pack.members().iter().map(ToString::to_string).eq(record.pack_members.iter().cloned()) {
                    return Err(invalid_key("artefact pack owner"));
                }
                Ok(Self::Pack(pack))
            }
        }
    }
    /// The tries whose data files the artefact is derived from.
    pub fn tries(&self) -> Vec<TrieKey> {
        match self { Self::Trie(trie) => vec![trie.clone()], Self::Group(group) => group.members(), Self::Pack(pack) => pack.members().to_vec() }
    }
}

impl fmt::Display for ArtefactOwner {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Trie(trie) => trie.fmt(output), Self::Group(group) => group.fmt(output),
            Self::Pack(pack) => {
                output.write_str("pack")?;
                for key in pack.members() { write!(output, "~{key}")?; }
                Ok(())
            }
        }
    }
}

impl FromStr for ArtefactOwner {
    type Err = Anomaly;

    fn from_str(key: &str) -> Result<Self> {
        if let Some(members) = key.strip_prefix("pack~") {
            if members.len() > 512 || members.split('~').count() > ArtefactPack::MAX_MEMBERS { return Err(invalid_key(key)); }
            let pack = ArtefactPack::new(members.split('~').map(str::parse).collect::<Result<Vec<_>>>()?)?;
            let owner = Self::Pack(pack);
            if owner.to_string() != key { return Err(invalid_key(key)); }
            Ok(owner)
        } else if key.starts_with('g') { key.parse().map(Self::Group) } else { key.parse().map(Self::Trie) }
    }
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
        write!(output, "-b{}", lex_hex(self.block_index.0 as u64))?;
        if self.epoch != 0 { write!(output, "-e{}", lex_hex(self.epoch))?; }
        Ok(())
    }
}

impl FromStr for TrieKey {
    type Err = Anomaly;

    fn from_str(key: &str) -> Result<Self> {
        let mut parts: Vec<_> = key.split('-').collect();
        let epoch = match parts.last().and_then(|last| last.strip_prefix('e')) {
            Some(epoch) => {
                let epoch = signed_position(epoch)?;
                if epoch <= 0 { return Err(invalid_key(key)); }
                parts.pop();
                epoch as u64
            }
            None => 0,
        };
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
        Ok(Self::new(level, recency, part, BlockIndex(block))?.with_epoch(epoch))
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
