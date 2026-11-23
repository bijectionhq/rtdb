#![allow(non_snake_case)]

use vstd::prelude::*;

use crate::inputs::*;
use crate::prost_replay::{PayloadSource, ReplicaEnvelope, payloadKey, replica_from_bytes};

verus! {

pub open spec fn keysThrough<T, K>(items: Seq<T>, contribution: spec_fn(T) -> ISet<K>, end: int) -> ISet<K> {
    ISet::new(|key: K| exists|i: int| 0 <= i < end && #[trigger] contribution(items[i]).contains(key))
}

pub open spec fn allKeys<T, K>(items: Seq<T>, contribution: spec_fn(T) -> ISet<K>) -> ISet<K> {
    keysThrough(items, contribution, items.len() as int)
}

proof fn extend<T, K>(items: Seq<T>, contribution: spec_fn(T) -> ISet<K>, index: int)
    requires 0 <= index < items.len(),
    ensures keysThrough(items, contribution, index + 1)
        == keysThrough(items, contribution, index).union(contribution(items[index])),
{
    assert forall|key: K| #[trigger] keysThrough(items, contribution, index + 1).contains(key)
        <==> keysThrough(items, contribution, index).union(contribution(items[index])).contains(key) by {
        if keysThrough(items, contribution, index + 1).contains(key) {
            let i = choose|i: int| 0 <= i < index + 1 && #[trigger] contribution(items[i]).contains(key);
            if i < index {
                assert(contribution(items[i]).contains(key));
            } else {
                assert(i == index);
            }
        } else if keysThrough(items, contribution, index).contains(key) {
            let i = choose|i: int| 0 <= i < index && #[trigger] contribution(items[i]).contains(key);
            assert(0 <= i < index + 1 && contribution(items[i]).contains(key));
        } else if contribution(items[index]).contains(key) {
            assert(0 <= index < index + 1 && contribution(items[index]).contains(key));
        }
    }
    assert(keysThrough(items, contribution, index + 1)
        =~= keysThrough(items, contribution, index).union(contribution(items[index])));
}

proof fn union_associative<K>(a: ISet<K>, b: ISet<K>, c: ISet<K>)
    ensures a.union(b).union(c) == a.union(b.union(c)),
{
    assert(a.union(b).union(c) =~= a.union(b.union(c)));
}

proof fn union_empty<K>(a: ISet<K>)
    ensures a.union(ISet::empty()) == a,
{
    assert(a.union(ISet::empty()) =~= a);
}

proof fn member_at<T>(values: Seq<T>, contribution: spec_fn(T) -> ISet<u64>, i: int, key: u64)
    requires 0 <= i < values.len(), contribution(values[i]).contains(key),
    ensures allKeys(values, contribution).contains(key),
{
    assert(exists|j: int| 0 <= j < values.len() && #[trigger] contribution(values[j]).contains(key));
}

proof fn key_set(values: Seq<u64>)
    ensures allKeys(values, |file: u64| ISet::empty().insert(file))
        == ISet::new(|key: u64| values.contains(key)),
{
    assert forall|key: u64| #[trigger] values.contains(key) <==>
        allKeys(values, |file: u64| ISet::empty().insert(file)).contains(key) by {
        if values.contains(key) {
            let i = choose|i: int| 0 <= i < values.len() && values[i] == key;
            assert(ISet::empty().insert(values[i]).contains(key));
            member_at(values, |file: u64| ISet::empty().insert(file), i, key);
        } else if allKeys(values, |file: u64| ISet::empty().insert(file)).contains(key) {
            let i = choose|i: int| 0 <= i < values.len() && ISet::empty().insert(values[i]).contains(key);
            assert(values[i] == key);
            assert(exists|j: int| 0 <= j < values.len() && values[j] == key);
        }
    }
    assert(allKeys(values, |file: u64| ISet::empty().insert(file))
        =~= ISet::new(|key: u64| values.contains(key)));
}

pub open spec fn trieKeys(slug: TableSlug, detail: TrieDetails, contents: CheckpointContents) -> ISet<u64> {
    let trie = detail.trie_key.unwrap();
    let garbage = stateTag(detail.trie_state) == 2;
    if garbage && (contents is Primary || trie.level != 0) { ISet::empty() }
    else {
        let pair = ISet::empty().insert(trieDataKey(slug, trie)).insert(trieMetadataKey(slug, trie));
        if contents is Replay && !garbage {
            pair.union(allKeys(detail.artefacts@, |record: ArtefactRecord| artefactEntryKeys(slug, (trie, record))))
        } else { pair }
    }
}

pub open spec fn retainedTrieKeys(entry: &TableEntry, detail: TrieDetails, contents: CheckpointContents) -> ISet<u64> {
    if entry.table.expires && detail.trie_state == Some(2) { ISet::empty() }
    else { trieKeys(entry.slug, detail, contents) }
}

pub open spec fn checkpointTrieFiles(table: &TableBlock, entry: &TableEntry, contents: CheckpointContents) -> ISet<u64> {
    let files = allKeys(tableTries(table), |detail: TrieDetails| retainedTrieKeys(entry, detail, contents));
    if contents is Replay {
        files.union(allKeys(table.wire.search_runs@, |run: SearchRun| ISet::empty().insert(searchRunKey(entry.slug, run))))
    } else { files }
}

pub open spec fn tableKeys(block: i64, entry: TableEntry, tables: &Tables) -> ISet<u64> {
    checkpointTrieFiles(&tables.map@[entry.table], &entry, CheckpointContents::Replay)
        .insert(tableBlockKey(entry.slug, block))
}

pub open spec fn baseKeys(catalog: &BlockCatalog, tables: &Tables) -> ISet<u64> {
    ISet::empty().insert(rootKey(catalog.block))
        .union(allKeys(catalog.entries@, |entry: TableEntry| tableKeys(catalog.block, entry, tables)))
}

pub open spec fn addedTrieKeys(trie: TrieDetails, slugs: ISet<TableEntry>) -> ISet<u64> {
    allKeys(slugsAt(slugs, trie.table_name.unwrap()), |slug: TableSlug| trieKeys(slug, trie, CheckpointContents::Replay))
}

pub open spec fn addedKeys(tries: Seq<TrieDetails>, slugs: ISet<TableEntry>) -> ISet<u64> {
    allKeys(tries, |trie: TrieDetails| addedTrieKeys(trie, slugs))
}

pub open spec fn artefactEntryKeys(slug: TableSlug, entry: (TrieKey, ArtefactRecord)) -> ISet<u64> {
    match recordedKey(slug, entry.0, entry.1) {
        Some(key) => ISet::empty().insert(key),
        None => ISet::empty(),
    }
}

pub open spec fn artefactKeysAt(slug: TableSlug, message: &ArtefactsAdded) -> ISet<u64> {
    allKeys(parsedArtefactEntries(message.entries@), |entry: (TrieKey, ArtefactRecord)| artefactEntryKeys(slug, entry))
}

pub open spec fn artefactKeys(message: &ArtefactsAdded, slugs: ISet<TableEntry>) -> ISet<u64> {
    if message.table_name.is_none() { ISet::empty() }
    else { allKeys(slugsAt(slugs, message.table_name.unwrap()), |slug: TableSlug| artefactKeysAt(slug, message)) }
}

pub open spec fn matchesStorage(base: ReplayBase, version: Option<i32>, epoch: Option<i32>) -> bool {
    version.unwrap_or(0) == base.storage_version && epoch.unwrap_or(0) == base.storage_epoch
}

pub open spec fn adoptedUpload(base: ReplayBase, upload: &BlockUploaded) -> bool {
    matchesStorage(base, upload.storage_version, upload.storage_epoch)
        && base.installed.unwrap_or(-1i64) < upload.block_index.unwrap_or(0)
}

pub open spec fn bodyFileKeys(base: ReplayBase, body: &ReplicaBody, slugs: ISet<TableEntry>) -> ISet<u64> {
    match body {
        ReplicaBody::BlockUploaded(upload) => if adoptedUpload(base, upload) { addedKeys(upload.tries@, slugs) } else { ISet::empty() },
        ReplicaBody::TriesAdded(message) => if matchesStorage(base, message.storage_version, message.storage_epoch) {
            addedKeys(message.tries@, slugs)
        } else { ISet::empty() },
        ReplicaBody::ArtefactsAdded(message) => if matchesStorage(base, message.storage_version, message.storage_epoch) {
            artefactKeys(message, slugs)
        } else { ISet::empty() },
        _ => ISet::empty(),
    }
}

pub open spec fn payloadKeys(envelope: &ReplicaEnvelope) -> ISet<u64> {
    if let Some(key) = payloadKey(envelope) { ISet::empty().insert(key) } else { ISet::empty() }
}

pub open spec fn decodedBody(envelope: &ReplicaEnvelope, source: &PayloadSource) -> ReplicaBody {
    match payloadKey(envelope) {
        Some(key) => replica_from_bytes(source.objects@[key].skip(1)).unwrap().2,
        None => envelope.message,
    }
}

} // verus!

macro_rules! each {
    ($keys:ident, $items:expr, $item:ident => $body:block, [$($contribution:tt)*]) => {
        vstd::prelude::verus_exec_expr!({
            let items = $items;
            let ghost values = items@;
            let ghost initial = $keys@;
            let mut index = 0;
            assert(keysThrough(values, $($contribution)*, 0) =~= ISet::empty());
            proof { union_empty(initial); }
            while index < items.len()
                invariant
                    0 <= index <= values.len(),
                    items@ == values,
                    $keys@ == initial.union(keysThrough(values, $($contribution)*, index as int)),
                decreases values.len() - index,
            {
                let $item = &items[index];
                assert(*$item == values[index as int]);
                let ghost previous = $keys@;
                $body;
                assert($keys@ =~= previous.union(($($contribution)*)(values[index as int])));
                proof {
                    extend(values, $($contribution)*, index as int);
                    union_associative(initial, keysThrough(values, $($contribution)*, index as int), ($($contribution)*)(values[index as int]));
                }
                index += 1;
            }
        })
    };
}

macro_rules! each_object_key {
    ($keys:ident, $items:expr, $item:ident => $body:block, [$($contribution:tt)*]) => {
        vstd::prelude::verus_exec_expr!({
            let paths = $items.into_keys();
            proof { key_set(paths@); }
            each!($keys, paths, path => { let $item = *path; $body }, [$($contribution)*]);
        })
    };
}

verus! {

impl TrieDetails {
    pub fn checkpoint_files(&self, slug: &TableSlug, contents: CheckpointContents) -> (out: Result<BTreeSet>)
        ensures out matches Ok(files) ==> files@ == trieKeys(*slug, *self, contents),
    {
        let detail = self;
        trie_checkpoint_files!(detail, slug, contents; each)
    }
}

impl TableBlock {
    pub fn checkpoint_trie_files(&self, entry: &TableEntry, contents: CheckpointContents) -> (out: Result<BTreeSet>)
        ensures out matches Ok(files) ==> files@ == checkpointTrieFiles(self, entry, contents),
    {
        let table = self;
        table_checkpoint_files!(table, entry, contents; each)
    }
}

fn insert_object_keys(keys: &mut BTreeSet, files: BTreeSet) -> (out: Result<()>)
    ensures out is Ok ==> final(keys)@ == old(keys)@.union(files@),
{
    checked_object_keys!(keys, files; each_object_key)
}

pub open spec fn slugEntries(catalog: &BlockCatalog) -> ISet<TableEntry> {
    allKeys(catalog.entries@, |entry: TableEntry| ISet::empty().insert(entry))
}

fn record_slugs(slugs: &mut Slugs, catalog: &BlockCatalog) -> (out: Result<()>)
    ensures out is Ok ==> final(slugs)@ == old(slugs)@.union(slugEntries(catalog)),
{
    macro_rules! insert {
        ($slugs:ident, $entry:ident) => { $slugs.insert(*$entry) };
    }
    record_catalog_slugs!(slugs, catalog; each, insert)
}

pub fn checkpoint_dependencies(catalog: &BlockCatalog, tables: &Tables) -> (out: Result<BTreeSet>)
    ensures out matches Ok(files) ==> files@ == baseKeys(catalog, tables),
{
    let contents = CheckpointContents::Replay;
    checkpoint_dependency_keys!(catalog, tables, contents; each)
}

fn added_trie_keys(slugs: &Slugs, tries: &Vec<TrieDetails>, keys: &mut BTreeSet) -> (out: Result<()>)
    ensures out is Ok ==> final(keys)@ == old(keys)@.union(addedKeys(tries@, slugs.entries@)),
{
    added_trie_dependency_keys!(slugs, tries, keys; each)
}

fn same_storage(base: &ReplayBase, version: Option<i32>, epoch: Option<i32>) -> (out: bool)
    ensures out == matchesStorage(*base, version, epoch),
{
    matching_replay_storage!(version, epoch, base.storage_version, base.storage_epoch)
}

fn adopted_upload<'a>(base: &ReplayBase, body: &'a ReplicaBody) -> (out: Option<&'a BlockUploaded>)
    ensures match body {
        ReplicaBody::BlockUploaded(upload) => if adoptedUpload(*base, upload) { out == Some(upload) } else { out.is_none() },
        _ => out.is_none(),
    },
{
    let installed = base.installed.unwrap_or(-1i64);
    new_upload!(base, body, installed)
}

pub fn record_dependency_keys(base: ReplayBase, body: &ReplicaBody, slugs: &Slugs, keys: &mut BTreeSet) -> (out: Result<()>)
    ensures out is Ok ==> final(keys)@ == old(keys)@.union(bodyFileKeys(base, body, slugs.entries@)),
{
    record_dependency_keys!(base, body, slugs, keys; each);
    Ok(())
}

pub fn adopt_catalog(catalog: &BlockCatalog, tables: &Tables, keys: &mut BTreeSet, block: i64) -> (out: Result<()>)
    ensures out is Ok ==> catalog.block == block && final(keys)@ == old(keys)@.union(baseKeys(catalog, tables)),
{
    adopt_dependency!(catalog, tables, keys, block);
    Ok(())
}

pub enum Cow<'a> {
    Borrowed(&'a ReplicaBody),
    Owned(ReplicaBody),
}

pub open spec fn value(body: Cow) -> ReplicaBody {
    match body { Cow::Borrowed(body) => *body, Cow::Owned(body) => body }
}

pub fn resolve_record<'a>(
    envelope: &'a ReplicaEnvelope, base: ReplayBase, keys: &mut BTreeSet, bodies: &mut Vec<Cow<'a>>, source: &PayloadSource,
) -> (out: Result<()>)
    ensures out is Ok ==> final(keys)@ == old(keys)@.union(payloadKeys(envelope))
        && final(bodies)@.len() == old(bodies)@.len() + 1
        && final(bodies)@.drop_last() == old(bodies)@
        && final(bodies)@.last() == resolvedCow(envelope, source),
{
    macro_rules! read_payload { ($key:expr) => { source.read($key) }; }
    oversized_dependency!(envelope, base, keys, bodies, read_payload);
    Ok(())
}

} // verus!

verus! {

impl<'a> Cow<'a> {
    pub fn as_ref(&self) -> (out: &ReplicaBody)
        ensures *out == value(*self),
    {
        match self { Cow::Borrowed(body) => *body, Cow::Owned(body) => body }
    }
}

pub open spec fn resolvedCow<'a>(envelope: &'a ReplicaEnvelope, source: &PayloadSource) -> Cow<'a> {
    match payloadKey(envelope) {
        Some(_) => Cow::Owned(decodedBody(envelope, source)),
        None => Cow::Borrowed(&envelope.message),
    }
}

pub open spec fn resolvedBodies<'a>(suffix: Seq<&'a ReplicaEnvelope>, source: &PayloadSource) -> Seq<Cow<'a>> {
    suffix.map_values(|envelope| resolvedCow(envelope, source))
}

pub open spec fn adoptedIndex(base: ReplayBase, body: &ReplicaBody) -> Option<i64> {
    match body {
        ReplicaBody::BlockUploaded(upload) => if adoptedUpload(base, upload) {
            Some(upload.block_index.unwrap_or(0))
        } else { None },
        _ => None,
    }
}

fn upload_block(upload: &BlockUploaded) -> (out: i64)
    ensures out == upload.block_index.unwrap_or(0),
{ upload.block_index.unwrap_or(0) }

pub open spec fn catalogKeys(source: &CatalogSource, block: i64) -> ISet<u64> {
    let (catalog, tables) = source.catalogs@[block];
    baseKeys(&catalog, &tables)
}

pub open spec fn adoptedBodyKeys(base: ReplayBase, body: &ReplicaBody, source: &CatalogSource) -> ISet<u64> {
    match adoptedIndex(base, body) {
        Some(block) => catalogKeys(source, block),
        None => ISet::empty(),
    }
}

pub open spec fn bodyKeys(
    base: ReplayBase, body: &ReplicaBody, source: &CatalogSource, slugs: ISet<TableEntry>,
) -> ISet<u64> {
    adoptedBodyKeys(base, body, source).union(bodyFileKeys(base, body, slugs))
}

pub open spec fn recordKeys(
    base: ReplayBase, envelope: &ReplicaEnvelope, payloads: &PayloadSource, catalogs: &CatalogSource, slugs: ISet<TableEntry>,
) -> ISet<u64> {
    payloadKeys(envelope).union(bodyKeys(base, &decodedBody(envelope, payloads), catalogs, slugs))
}

pub open spec fn adoptedSlugEntries(base: ReplayBase, body: &ReplicaBody, source: &CatalogSource) -> ISet<TableEntry> {
    match adoptedIndex(base, body) {
        Some(block) => slugEntries(&source.catalogs@[block].0),
        None => ISet::empty(),
    }
}

pub open spec fn initialSlugs(checkpoint: Option<&BlockCatalog>) -> ISet<TableEntry> {
    match checkpoint { Some(catalog) => slugEntries(catalog), None => ISet::empty() }
}

pub open spec fn replaySlugs(
    base: ReplayBase, checkpoint: Option<&BlockCatalog>, bodies: Seq<Cow>, catalogs: &CatalogSource,
) -> ISet<TableEntry> {
    initialSlugs(checkpoint).union(allKeys(bodies, |body: Cow| adoptedSlugEntries(base, &value(body), catalogs)))
}

pub open spec fn coveredCache(adopted: ISet<i64>, keys: ISet<u64>, slugs: ISet<TableEntry>, source: &CatalogSource) -> bool {
    forall|block: i64| #[trigger] adopted.contains(block) ==>
        catalogKeys(source, block).subset_of(keys) && slugEntries(&source.catalogs@[block].0).subset_of(slugs)
}

pub open spec fn tailKeys(
    base: ReplayBase, checkpoint: Option<&BlockCatalog>, suffix: Seq<&ReplicaEnvelope>, payloads: &PayloadSource, catalogs: &CatalogSource,
) -> ISet<u64> {
    let bodies = resolvedBodies(suffix, payloads);
    let slugs = replaySlugs(base, checkpoint, bodies, catalogs);
    allKeys(suffix, |envelope: &ReplicaEnvelope| recordKeys(base, envelope, payloads, catalogs, slugs))
}

proof fn partition_tail_keys(
    base: ReplayBase, suffix: Seq<&ReplicaEnvelope>, payloads: &PayloadSource, catalogs: &CatalogSource, slugs: ISet<TableEntry>,
    count: nat,
)
    requires count <= suffix.len(),
    ensures
        keysThrough(suffix, |envelope: &ReplicaEnvelope| recordKeys(base, envelope, payloads, catalogs, slugs), count as int)
        == keysThrough(suffix, |envelope: &ReplicaEnvelope| payloadKeys(envelope), count as int)
            .union(keysThrough(resolvedBodies(suffix, payloads),
                |body: Cow| adoptedBodyKeys(base, &value(body), catalogs), count as int))
            .union(keysThrough(resolvedBodies(suffix, payloads),
                |body: Cow| bodyFileKeys(base, &value(body), slugs), count as int)),
    decreases count,
{
    if count == 0 {
        assert(keysThrough(suffix, |envelope: &ReplicaEnvelope| recordKeys(base, envelope, payloads, catalogs, slugs), 0)
            =~= ISet::empty());
        assert(keysThrough(suffix, |envelope: &ReplicaEnvelope| payloadKeys(envelope), 0)
            =~= ISet::empty());
        assert(keysThrough(resolvedBodies(suffix, payloads), |body: Cow| adoptedBodyKeys(base, &value(body), catalogs), 0)
            =~= ISet::empty());
        assert(keysThrough(resolvedBodies(suffix, payloads), |body: Cow| bodyFileKeys(base, &value(body), slugs), 0)
            =~= ISet::empty());
    } else {
        let i = count as int - 1;
        let bodies = resolvedBodies(suffix, payloads);
        partition_tail_keys(base, suffix, payloads, catalogs, slugs, (count - 1) as nat);
        extend(suffix, |envelope: &ReplicaEnvelope| recordKeys(base, envelope, payloads, catalogs, slugs), i);
        extend(suffix, |envelope: &ReplicaEnvelope| payloadKeys(envelope), i);
        extend(bodies, |body: Cow| adoptedBodyKeys(base, &value(body), catalogs), i);
        extend(bodies, |body: Cow| bodyFileKeys(base, &value(body), slugs), i);
        assert(bodies[i] == resolvedCow(suffix[i], payloads));
        assert(value(bodies[i]) == decodedBody(suffix[i], payloads));
    }
    assert(
        keysThrough(suffix, |envelope: &ReplicaEnvelope| recordKeys(base, envelope, payloads, catalogs, slugs), count as int)
        =~= keysThrough(suffix, |envelope: &ReplicaEnvelope| payloadKeys(envelope), count as int)
            .union(keysThrough(resolvedBodies(suffix, payloads),
                |body: Cow| adoptedBodyKeys(base, &value(body), catalogs), count as int))
            .union(keysThrough(resolvedBodies(suffix, payloads),
                |body: Cow| bodyFileKeys(base, &value(body), slugs), count as int))
    );
}

} // verus!

macro_rules! each_resolved {
    ($keys:ident, $bodies:ident, $items:expr, $item:ident => $body:block; $payloads:ident) => {
        vstd::prelude::verus_exec_expr!({
            let inputs = $items;
            let ghost expected = resolvedBodies(inputs@, $payloads);
            let mut cursor = 0;
            assert(keysThrough(inputs@, |envelope: &ReplicaEnvelope| payloadKeys(envelope), 0) =~= ISet::empty());
            assert(expected.take(0) =~= Seq::<Cow>::empty());
            while cursor < inputs.len()
                invariant cursor <= inputs@.len(), expected == resolvedBodies(inputs@, $payloads),
                    $keys@ == keysThrough(inputs@, |envelope: &ReplicaEnvelope| payloadKeys(envelope), cursor as int),
                    $bodies@ == expected.take(cursor as int),
                decreases inputs@.len() - cursor,
            {
                let $item = &inputs[cursor];
                $body;
                assert(expected[cursor as int] == resolvedCow(*$item, $payloads));
                assert($bodies@ =~= expected.take(cursor as int + 1));
                proof { extend(inputs@, |envelope: &ReplicaEnvelope| payloadKeys(envelope), cursor as int); }
                cursor += 1;
            }
            assert($bodies@ =~= expected);
        })
    };
}

macro_rules! each_adopted {
    ($keys:ident, $slugs:ident, $adopted:ident, $items:expr, $base:ident, $item:ident => $body:block;
        $catalogs:ident
    ) => {
        vstd::prelude::verus_exec_expr!({
            let inputs = $items;
            let ghost start_keys = $keys@;
            let ghost start_slugs = $slugs.entries@;
            let mut cursor = 0;
            assert(keysThrough(inputs@, |body: Cow| adoptedBodyKeys($base, &value(body), $catalogs), 0) =~= ISet::empty());
            assert(keysThrough(inputs@, |body: Cow| adoptedSlugEntries($base, &value(body), $catalogs), 0) =~= ISet::empty());
            proof { union_empty(start_keys); union_empty(start_slugs); }
            while cursor < inputs.len()
                invariant cursor <= inputs@.len(),
                    $keys@ == start_keys.union(keysThrough(inputs@,
                        |body: Cow| adoptedBodyKeys($base, &value(body), $catalogs), cursor as int)),
                    $slugs.entries@ == start_slugs.union(keysThrough(inputs@,
                        |body: Cow| adoptedSlugEntries($base, &value(body), $catalogs), cursor as int)),
                    coveredCache($adopted.seen@, $keys@, $slugs.entries@, $catalogs),
                decreases inputs@.len() - cursor,
            {
                let $item = &inputs[cursor];
                let ghost seen = $adopted.seen@;
                let ghost keys_before = $keys@;
                let ghost slugs_before = $slugs.entries@;
                let ghost body = value(*$item);
                $body;
                assert($keys@ =~= keys_before.union(adoptedBodyKeys($base, &body, $catalogs)));
                assert($slugs.entries@ =~= slugs_before.union(adoptedSlugEntries($base, &body, $catalogs)));
                assert(coveredCache($adopted.seen@, $keys@, $slugs.entries@, $catalogs)) by {
                    assert forall|block: i64| #[trigger] $adopted.seen@.contains(block) implies
                        catalogKeys($catalogs, block).subset_of($keys@)
                            && slugEntries(&$catalogs.catalogs@[block].0).subset_of($slugs.entries@) by {
                        if seen.contains(block) {
                            assert(catalogKeys($catalogs, block).subset_of(keys_before));
                            assert(slugEntries(&$catalogs.catalogs@[block].0).subset_of(slugs_before));
                        } else {
                            assert(adoptedIndex($base, &body) == Some(block));
                        }
                    }
                }
                proof {
                    let key_fn = |body: Cow| adoptedBodyKeys($base, &value(body), $catalogs);
                    let slug_fn = |body: Cow| adoptedSlugEntries($base, &value(body), $catalogs);
                    extend(inputs@, key_fn, cursor as int);
                    extend(inputs@, slug_fn, cursor as int);
                    union_associative(start_keys, keysThrough(inputs@, key_fn, cursor as int), key_fn(*$item));
                    union_associative(start_slugs, keysThrough(inputs@, slug_fn, cursor as int), slug_fn(*$item));
                }
                cursor += 1;
            }
        })
    };
}

verus! {

pub fn tail_dependencies(
    base: ReplayBase, checkpoint: Option<&BlockCatalog>, suffix: Vec<&ReplicaEnvelope>, payloads: &PayloadSource,
    catalogs: &CatalogSource,
) -> (out: Result<BTreeSet>)
    requires base.installed == match checkpoint { Some(catalog) => Some(catalog.block), None => None },
    ensures out matches Ok(keys) ==> keys@ == tailKeys(base, checkpoint, suffix@, payloads, catalogs),
{
    macro_rules! borrow_envelope { ($item:ident) => { *$item }; }
    macro_rules! resolve_one {
        ($envelope:ident, $base:ident, $keys:ident, $bodies:ident) => {
            resolve_record($envelope, $base, $keys, $bodies, payloads)?
        };
    }
    macro_rules! read_catalog { ($block:expr) => { catalogs.load($block) }; }
    macro_rules! resolve {
        ($($body:tt)*) => { each_resolved!($($body)*; payloads) };
    }
    macro_rules! adopt {
        ($($body:tt)*) => { each_adopted!($($body)*; catalogs) };
    }
    macro_rules! collect_bodies {
        ($keys:ident, $slugs:ident, $items:expr, $item:ident => $body:block, [$($contribution:tt)*]) => {
            vstd::prelude::verus_exec_expr!({
                let inputs = $items;
                let ghost bodies = resolvedBodies(suffix@, payloads);
                let ghost slugs = replaySlugs(base, checkpoint, bodies, catalogs);
                let ghost prior = allKeys(suffix@, |envelope: &ReplicaEnvelope| payloadKeys(envelope))
                    .union(allKeys(bodies, |body: Cow| adoptedBodyKeys(base, &value(body), catalogs)));
                assert(inputs@ == bodies);
                assert($slugs.entries@ =~= slugs);
                assert($keys@ =~= prior);
                each!($keys, inputs, $item => $body, [$($contribution)*]);
                assert($keys@ =~= prior.union(allKeys(bodies, |body: Cow| bodyFileKeys(base, &value(body), slugs))));
                assert($keys@ == tailKeys(base, checkpoint, suffix@, payloads, catalogs));
            })
        };
    }
    let ghost bodies = resolvedBodies(suffix@, payloads);
    proof { partition_tail_keys(base, suffix@, payloads, catalogs, replaySlugs(base, checkpoint, bodies, catalogs), suffix@.len()); }
    let keys = tail_dependency_keys!(base, suffix, checkpoint; resolve, adopt, collect_bodies, borrow_envelope, resolve_one, read_catalog)?;
    assert(keys@ =~= tailKeys(base, checkpoint, suffix@, payloads, catalogs));
    Ok(keys)
}

} // verus!
