#![allow(non_snake_case)]

use vstd::prelude::*;

verus! {

pub type Result<T> = core::result::Result<T, &'static str>;
pub fn invalid(message: &'static str) -> (out: &'static str)
    ensures out == message,
{ message }

pub struct BTreeSet { pub keys: Ghost<ISet<u64>> }
impl View for BTreeSet {
    type V = ISet<u64>;
    open spec fn view(&self) -> ISet<u64> { self.keys@ }
}

impl BTreeSet {
    #[verifier::external_body]
    pub fn new() -> (out: Self)
        ensures out@ == ISet::<u64>::empty(),
    { unimplemented!() }

    #[verifier::external_body]
    pub fn insert(&mut self, value: u64)
        ensures final(self)@ == old(self)@.insert(value),
    { unimplemented!() }

    #[verifier::external_body]
    pub fn append(&mut self, from: &mut Self)
        ensures final(self)@ == old(self)@.union(old(from)@), final(from)@ == ISet::<u64>::empty(),
    { unimplemented!() }

    #[verifier::external_body]
    pub fn into_keys(self) -> (out: Vec<u64>)
        ensures self@ == ISet::new(|key: u64| out@.contains(key)),
    { unimplemented!() }
}

pub struct ObjectKey;
impl ObjectKey {
    #[verifier::external_body]
    pub fn new(path: u64) -> (out: Result<u64>)
        ensures out matches Ok(key) ==> key == path,
    { unimplemented!() }
}

#[derive(Clone, Copy)]
pub struct TableRef {
    pub id: u64,
    pub expires: bool,
}

impl TableRef {
    pub fn stores_expiring_transaction_metadata(&self) -> (out: bool)
        ensures out == self.expires,
    { self.expires }
}

#[derive(Clone, Copy)]
pub struct TableSlug { pub id: u64 }

#[derive(Clone, Copy)]
pub struct TrieKey {
    pub id: u64,
    pub level: u32,
}

impl TrieKey {
    pub fn level(&self) -> (out: u32)
        ensures out == self.level,
    { self.level }
}

#[derive(Clone, Copy)]
pub struct TableEntry { pub table: TableRef, pub slug: TableSlug }

#[derive(Clone, Copy)]
pub enum CheckpointContents { Primary, Replay }

#[derive(Clone, Copy)]
pub enum TrieState { Nascent = 0, Live = 1, Garbage = 2 }

pub open spec fn stateTag(raw: Option<i32>) -> i32 {
    match raw { Some(tag) => tag, None => 0 }
}

pub struct TrieDetails {
    pub trie_state: Option<i32>,
    pub trie_key: Option<TrieKey>,
    pub table_name: Option<TableRef>,
    pub artefacts: Vec<ArtefactRecord>,
}

#[derive(Clone, Copy)]
pub struct ArtefactRecord { pub id: u64 }

#[derive(Clone, Copy)]
pub struct SearchRun { pub id: u64 }

pub struct TableWire {
    pub partitions: Vec<Vec<TrieDetails>>,
    pub tries: Vec<TrieDetails>,
    pub search_runs: Vec<SearchRun>,
}

pub struct TableBlock { pub wire: TableWire }

pub open spec fn flattenTries(partitions: Seq<Vec<TrieDetails>>) -> Seq<TrieDetails>
    decreases partitions.len(),
{
    if partitions.len() == 0 { Seq::empty() }
    else { partitions.first()@ + flattenTries(partitions.drop_first()) }
}

pub open spec fn tableTries(table: &TableBlock) -> Seq<TrieDetails> {
    if table.wire.partitions@.len() == 0 { table.wire.tries@ }
    else { flattenTries(table.wire.partitions@) }
}

impl TableBlock {
    #[verifier::external_body]
    pub fn trie_details(&self) -> (out: Vec<TrieDetails>)
        ensures out@ == tableTries(self),
    { unimplemented!() }
}

pub uninterp spec fn rootKey(block: i64) -> u64;
pub uninterp spec fn tableBlockKey(slug: TableSlug, block: i64) -> u64;
pub uninterp spec fn trieDataKey(slug: TableSlug, trie: TrieKey) -> u64;
pub uninterp spec fn trieMetadataKey(slug: TableSlug, trie: TrieKey) -> u64;
pub uninterp spec fn recordedKey(slug: TableSlug, trie: TrieKey, record: ArtefactRecord) -> Option<u64>;
pub uninterp spec fn searchRunKey(slug: TableSlug, run: SearchRun) -> u64;

#[verifier::external_body]
pub fn replay_artefact_file(slug: &TableSlug, trie: &TrieKey, record: &ArtefactRecord) -> (out: Result<Option<u64>>)
    ensures out matches Ok(key) ==> key == recordedKey(*slug, *trie, *record),
{ unimplemented!() }

#[verifier::external_body]
pub fn block_key(block: i64) -> (out: Result<u64>)
    ensures out matches Ok(key) ==> key == rootKey(block),
{ unimplemented!() }

impl TableSlug {
    #[verifier::external_body]
    pub fn block_key(&self, block: i64) -> (out: Result<u64>)
        ensures out matches Ok(key) ==> key == tableBlockKey(*self, block),
    { unimplemented!() }
    #[verifier::external_body]
    pub fn data_key(&self, trie: &TrieKey) -> (out: u64)
        ensures out == trieDataKey(*self, *trie),
    { unimplemented!() }
    #[verifier::external_body]
    pub fn metadata_key(&self, trie: &TrieKey) -> (out: u64)
        ensures out == trieMetadataKey(*self, *trie),
    { unimplemented!() }
}

impl SearchRun {
    #[verifier::external_body]
    pub fn object_key(&self, slug: &TableSlug) -> (out: Result<u64>)
        ensures out matches Ok(key) ==> key == searchRunKey(*slug, *self),
    { unimplemented!() }
}

pub fn checkpoint_trie_state(detail: &TrieDetails) -> (out: Result<TrieState>)
    ensures match out {
        Ok(TrieState::Nascent) => stateTag(detail.trie_state) == 0,
        Ok(TrieState::Live) => stateTag(detail.trie_state) == 1,
        Ok(TrieState::Garbage) => stateTag(detail.trie_state) == 2,
        Err(_) => stateTag(detail.trie_state) != 0 && stateTag(detail.trie_state) != 1 && stateTag(detail.trie_state) != 2,
    },
{
    match detail.trie_state.unwrap_or(0) {
        0 => Ok(TrieState::Nascent),
        1 => Ok(TrieState::Live),
        2 => Ok(TrieState::Garbage),
        _ => Err("Unknown trie state in checkpoint"),
    }
}

pub fn checkpoint_trie_key(detail: &TrieDetails) -> (out: Result<TrieKey>)
    ensures match out {
        Ok(key) => detail.trie_key == Some(key),
        Err(_) => detail.trie_key.is_none(),
    },
{
    match detail.trie_key { Some(parsed) => Ok(parsed), None => Err("Invalid trie key") }
}

pub fn legacy_table_ref_from_wire(name: &Option<TableRef>) -> (out: Result<TableRef>)
    ensures match out { Ok(table) => *name == Some(table), Err(_) => name.is_none() },
{
    match name { Some(table) => Ok(*table), None => Err("Invalid table reference") }
}

pub struct BlockCatalog {
    pub block: i64,
    pub entries: Vec<TableEntry>,
}

impl BlockCatalog {
    pub fn block_index(&self) -> (out: i64)
        ensures out == self.block,
    { self.block }

    #[verifier::external_body]
    pub fn table_entries(&self) -> (out: Result<Vec<TableEntry>>)
        ensures out matches Ok(entries) ==> entries@ == self.entries@,
    { unimplemented!() }
}

pub struct Tables { pub map: Ghost<Map<TableRef, TableBlock>> }

impl Tables {
    #[verifier::external_body]
    pub fn get(&self, table: &TableRef) -> (out: Option<&TableBlock>)
        ensures out.is_some() == self.map@.dom().contains(*table),
            out matches Some(block) ==> *block == self.map@[*table],
    { unimplemented!() }
}

pub struct Slugs { pub entries: Ghost<ISet<TableEntry>> }

pub uninterp spec fn slugsAt(entries: ISet<TableEntry>, table: TableRef) -> Seq<TableSlug>;

impl View for Slugs {
    type V = ISet<TableEntry>;
    open spec fn view(&self) -> ISet<TableEntry> { self.entries@ }
}

impl Slugs {
    #[verifier::external_body]
    pub fn new() -> (out: Self)
        ensures out.entries@ == ISet::<TableEntry>::empty(),
    { unimplemented!() }
    #[verifier::external_body]
    pub fn insert(&mut self, entry: TableEntry)
        ensures final(self)@ == old(self)@.insert(entry),
    { unimplemented!() }
}

#[verifier::external_body]
pub fn table_slugs<'a>(slugs: &'a Slugs, table: &TableRef) -> (out: Result<&'a Vec<TableSlug>>)
    ensures out matches Ok(resolved) ==> resolved@ == slugsAt(slugs.entries@, *table)
        && (forall|slug: TableSlug| #[trigger] resolved@.contains(slug)
            <==> slugs.entries@.contains(TableEntry { table: *table, slug })),
{ unimplemented!() }

pub struct AdoptedCatalogs { pub seen: Ghost<ISet<i64>> }
impl AdoptedCatalogs {
    #[verifier::external_body]
    pub fn new() -> (out: Self)
        ensures out.seen@ == ISet::<i64>::empty(),
    { unimplemented!() }

    #[verifier::external_body]
    pub fn get(&self, block: &i64) -> (out: Option<&BlockCatalog>)
        ensures out.is_some() == self.seen@.contains(*block),
    { unimplemented!() }

    #[verifier::external_body]
    pub fn insert(&mut self, block: i64, catalog: BlockCatalog)
        ensures final(self).seen@ == old(self).seen@.insert(block),
    { unimplemented!() }
}

pub struct CatalogSource { pub catalogs: Ghost<Map<i64, (BlockCatalog, Tables)>> }
impl CatalogSource {
    #[verifier::external_body]
    pub fn load(&self, index: i64) -> (out: Result<(BlockCatalog, Tables)>)
        ensures out matches Ok(catalog) ==> self.catalogs@.dom().contains(index)
            && catalog == self.catalogs@[index],
    { unimplemented!() }
}

pub fn validate_uploaded_root(_catalog: &BlockCatalog, _upload: &BlockUploaded, _legacy: bool) -> Result<()> {
    Ok(())
}

#[derive(Clone, Copy)]
pub struct ReplayBase {
    pub storage_version: i32,
    pub storage_epoch: i32,
    pub installed: Option<i64>,
}

pub struct BlockUploaded {
    pub storage_version: Option<i32>,
    pub storage_epoch: Option<i32>,
    pub block_index: Option<i64>,
    pub tries: Vec<TrieDetails>,
}

pub struct TriesAdded {
    pub storage_version: Option<i32>,
    pub storage_epoch: Option<i32>,
    pub tries: Vec<TrieDetails>,
}

pub struct ArtefactsAdded {
    pub storage_version: Option<i32>,
    pub storage_epoch: Option<i32>,
    pub table_name: Option<TableRef>,
    pub entries: Vec<ArtefactEntry>,
}

pub struct ArtefactEntry {
    pub trie_key: Option<TrieKey>,
    pub record: Option<ArtefactRecord>,
}

pub open spec fn parsedArtefactEntries(entries: Seq<ArtefactEntry>) -> Seq<(TrieKey, ArtefactRecord)>
    decreases entries.len(),
{
    if entries.len() == 0 { Seq::empty() }
    else {
        let tail = parsedArtefactEntries(entries.drop_first());
        match (entries.first().trie_key, entries.first().record) {
            (Some(trie), Some(record)) => seq![(trie, record)] + tail,
            _ => tail,
        }
    }
}

#[verifier::external_body]
pub fn artefact_entries(entries: &Vec<ArtefactEntry>) -> (out: Vec<(TrieKey, ArtefactRecord)>)
    ensures out@ == parsedArtefactEntries(entries@),
{ unimplemented!() }

pub enum ReplicaBody {
    ResolvedTx(()), NoOp(()), BlockBoundary(()), BlockUploaded(BlockUploaded),
    TriesAdded(TriesAdded), TriesDeleted(()), TablesRetired(()),
    ArtefactsAdded(ArtefactsAdded), ArtefactsRemoved(()), OversizedMessage(u64),
}

}
