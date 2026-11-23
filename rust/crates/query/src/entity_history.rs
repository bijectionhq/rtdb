use std::{collections::{BTreeMap, BTreeSet}, ops::Range, sync::Arc};

use futures::{FutureExt, TryStreamExt};
use rtdb_format::entity_history::{self, HistoryRoot};
use rtdb_index::TrieState;
use rtdb_storage::{ObjectKey, audience_work, cache::{BufferPool, CacheLease}};
use rtdb_types::{Anomaly, AnomalyCategory, Iid, MemoryBudget, Result};

use crate::{LocatedHistoryEvent, ResolvedSearchRow, ScanRequest, SearchRowLocation, resolve_located_point};

pub struct HistoryObject<'a> {
    storage: &'a BufferPool,
    key: ObjectKey,
    memory: &'a Arc<MemoryBudget>,
    full: Option<CacheLease>,
}

impl<'a> HistoryObject<'a> {
    pub async fn open(request: &ScanRequest<'a>, key: ObjectKey, size: i64) -> Result<Self> {
        let storage = request.storage.as_ref();
        let full = if storage.object_store().supports_scan_ranges(&key) {
            None
        } else {
            let size = usize::try_from(size).map_err(|error| Anomaly::fault("rtdb.query/history-length", error.to_string()))?;
            let body = storage.get_or(&key, request.memory).await?;
            if body.as_slice().len() != size {
                return Err(Anomaly::fault("rtdb.query/history-length", "Entity history length differs from its catalog receipt"));
            }
            Some(body)
        };
        Ok(Self { storage, key, memory: request.memory, full })
    }

    pub async fn read(&self, range: Range<u64>) -> Result<CacheLease> {
        if let Some(bytes) = &self.full {
            let range = usize::try_from(range.start).ok().zip(usize::try_from(range.end).ok()).map(|(start, end)| start..end)
                .ok_or_else(|| Anomaly::fault("rtdb.query/history-length", "Entity history range exceeds addressable memory"))?;
            return Ok(bytes.slice(range)?);
        }
        Ok(self.storage.scan_range(&self.key, range, self.memory).await?)
    }
}

pub async fn table_point_rows(request: &ScanRequest<'_>, keys: &BTreeSet<Iid>) -> Result<Option<Vec<ResolvedSearchRow>>> {
    let table = &request.scan.table.name.table;
    if table.is_owned_event_data() || rtdb_types::TableRef::owned_schema(&table.schema)
        || !request.bounds.system_time.is_point() || !request.bounds.valid_time.is_point() {
        return Ok(None);
    }
    match read_history(request, keys).await {
        Err(error) if error.category != AnomalyCategory::Interrupted => Ok(None),
        result => result,
    }
}

async fn read_history(request: &ScanRequest<'_>, keys: &BTreeSet<Iid>) -> Result<Option<Vec<ResolvedSearchRow>>> {
    let table = &request.scan.table.name.table;
    let mut charge = request.memory.reserve(4096usize.saturating_add(keys.len().saturating_mul(480)))?;
    let mut histories = BTreeMap::<Iid, Vec<LocatedHistoryEvent<()>>>::new();
    let mut sources = request.snapshot.trie_catalog().iter_tries(table).filter(|trie| trie.state == TrieState::Live
        && keys.iter().any(|key| contains(trie.key.part(), *key)));
    let entry = request.snapshot.table_entry(table);
    let mut reads = futures::stream::FuturesOrdered::new();
    let mut count = 0usize;
    let mut identities = BTreeMap::<Iid, Option<Vec<u8>>>::new();
    loop {
        while reads.len() < 8 {
            let Some(trie) = sources.next() else { break; };
            let Some(table) = entry else { return Ok(None); };
            let record = trie.artefact(entity_history::KIND, entity_history::ID, entity_history::VERSION);
            reads.push_back(async move {
                if request.cancellation.is_cancelled() { return Err(cancelled()); }
                // A trie written before table history, or whose history build failed, is read from its primary pages for these keys alone.
                let Some(record) = record else {
                    let events = crate::scan::primary_point_events(request, &table.slug, &trie.key, Some(keys), usize::MAX).await?
                        .expect("an unlimited primary read completes");
                    let reservation = request.memory.reserve(4096usize.saturating_add(events.len().saturating_mul(384)))?;
                    return Ok::<_, Anomaly>((events.into_iter().map(|event| (event, None)).collect::<Vec<_>>(), reservation));
                };
                let root = HistoryRoot::decode(record.stats(), record.file_size())?;
                let key = ObjectKey::new(table.slug.recorded_artefact_key(&trie.key, record)?)?;
                let file = HistoryObject::open(request, key, record.file_size()).await?;
                let directory = file.read(root.range()).await?;
                let _directory = request.memory.reserve(directory.as_slice().len().saturating_mul(2).saturating_add(4096))?;
                let ranges = entity_history::ranges(directory.as_slice(), root, entity_history::owner(&table.slug, &trie.key), Some(keys))?;
                let mut rows = Vec::new();
                let mut reservation = request.memory.reserve(1024)?;
                for range in ranges {
                    if request.cancellation.is_cancelled() { return Err(cancelled()); }
                    let bytes = file.read(range.range()).await?;
                    let _decoded = request.memory.reserve(bytes.as_slice().len().saturating_mul(4).saturating_add(4096))?;
                    let page = range.read(bytes.as_slice())?;
                    for row in page.rows(keys) {
                        let lease = Arc::get_mut(&mut reservation).expect("unpublished history rows own their reservation");
                        let needed = 4096usize.saturating_add((rows.len() + 1).saturating_mul(384));
                        if needed > lease.bytes() { lease.grow_to(needed.max(lease.bytes().saturating_mul(2)))?; }
                        let (header, origin) = page.event(row)?;
                        rows.push((LocatedHistoryEvent { header, origin, witness: None,
                            location: SearchRowLocation::Persisted { trie: trie.key.clone(), page: range.page, row: range.first_row + row as u32 } },
                            page.entity_key(row).map(<[u8]>::to_vec)));
                    }
                }
                Ok((rows, reservation))
            }.boxed());
        }
        let Some((rows, _held)) = reads.try_next().await? else { break; };
        count = count.saturating_add(rows.len());
        charge_rows(&mut charge, count)?;
        for (row, key) in rows {
            if key.is_some() && !same_identity(&mut identities, row.header.iid, key) { return Ok(None); }
            histories.entry(row.header.iid).or_default().push(row);
        }
    }
    for segment in request.snapshot.iter_segments(table) {
        for &iid in keys {
            if request.cancellation.is_cancelled() { return Err(cancelled()); }
            for located in segment.iid_sources(iid, request.memory)? {
                let (header, source) = located?;
                audience_work::scan_items(1)?;
                count = count.saturating_add(1);
                charge_rows(&mut charge, count)?;
                let origin = source.batch.view().origin(source.row)?;
                histories.entry(iid).or_default().push(LocatedHistoryEvent { header, origin, witness: None, location: SearchRowLocation::Live(source) });
            }
        }
    }
    resolve_located_point(histories, request.bounds, request.scan.clamp_valid_time, request.memory)
        .map(|rows| rows.map(|rows| rows.into_iter().map(|(row, _)| row).collect()))
}

/// Two recorded keys behind one IID are distinct entities, which an IID-keyed history cannot separate.
fn same_identity(identities: &mut BTreeMap<Iid, Option<Vec<u8>>>, iid: Iid, key: Option<Vec<u8>>) -> bool {
    match identities.entry(iid) {
        std::collections::btree_map::Entry::Vacant(slot) => { slot.insert(key); true }
        std::collections::btree_map::Entry::Occupied(slot) => *slot.get() == key,
    }
}

fn charge_rows(charge: &mut Arc<rtdb_types::MemoryReservation>, count: usize) -> Result<()> {
    let lease = Arc::get_mut(charge).expect("unpublished histories own their reservation");
    let needed = 4096usize.saturating_add(count.saturating_mul(384));
    if needed > lease.bytes() { lease.grow_to(needed.max(lease.bytes().saturating_mul(2)))?; }
    Ok(())
}

fn contains(part: &[u8], iid: Iid) -> bool {
    part.iter().enumerate().all(|(depth, digit)| (iid.0[depth / 4] >> (6 - 2 * (depth % 4))) & 3 == *digit)
}

fn cancelled() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "rtdb.query/cancelled", "Entity history read cancelled") }

pub async fn uncovered_history_iids(request: &ScanRequest<'_>, covered: &BTreeSet<rtdb_types::BlockIndex>,
    scope: Option<&BTreeSet<Iid>>, limit: usize) -> Result<Option<BTreeSet<Iid>>> {
    match read_uncovered(request, covered, scope, limit).await {
        Err(error) if error.category != AnomalyCategory::Interrupted => Ok(None),
        result => result,
    }
}

async fn read_uncovered(request: &ScanRequest<'_>, covered: &BTreeSet<rtdb_types::BlockIndex>,
    scope: Option<&BTreeSet<Iid>>, limit: usize) -> Result<Option<BTreeSet<Iid>>> {
    let table = &request.scan.table.name.table;
    let mut iids = BTreeSet::new();
    let mut charge = request.memory.reserve(4096)?;
    let mut inspected = 0usize;
    let Some(entry) = request.snapshot.table_entry(table) else { return Ok(Some(iids)); };
    for trie in request.snapshot.trie_catalog().iter_tries(table).filter(|trie| trie.state == TrieState::Live) {
        if trie.key.level() == 0 && covered.contains(&trie.key.block_index()) { continue; }
        if scope.is_some_and(|keys| !keys.iter().any(|&key| contains(trie.key.part(), key))) { continue; }
        let budget = limit.saturating_mul(32).saturating_sub(inspected);
        let events = match trie.artefact(entity_history::KIND, entity_history::ID, entity_history::VERSION) {
            Some(record) => {
                let key = ObjectKey::new(entry.slug.recorded_artefact_key(&trie.key, record)?)?;
                let root = HistoryRoot::decode(record.stats(), record.file_size())?;
                let file = HistoryObject::open(request, key, record.file_size()).await?;
                let directory = file.read(root.range()).await?;
                let _directory = request.memory.reserve(directory.as_slice().len().saturating_mul(2).saturating_add(4096))?;
                let ranges = entity_history::ranges(directory.as_slice(), root, entity_history::owner(&entry.slug, &trie.key), scope)?;
                let mut events = Vec::new();
                for range in ranges {
                    if request.cancellation.is_cancelled() { return Err(cancelled()); }
                    inspected = inspected.saturating_add(range.rows as usize);
                    if inspected > limit.saturating_mul(32) { return Ok(None); }
                    let bytes = file.read(range.range()).await?;
                    let _decoded = request.memory.reserve(bytes.as_slice().len().saturating_mul(4).saturating_add(4096))?;
                    let page = range.read(bytes.as_slice())?;
                    for row in 0..range.rows as usize { events.push(page.event(row)?); }
                }
                events
            }
            // A trie without table history nominates from its primary pages, within the same inspection budget.
            None => {
                let Some(events) = crate::scan::primary_point_events(request, &entry.slug, &trie.key, scope, budget).await? else { return Ok(None); };
                inspected = inspected.saturating_add(events.len());
                events.into_iter().map(|event| (event.header, event.origin)).collect()
            }
        };
        for (header, origin) in events {
            if header.op != rtdb_types::EventOp::Put || header.system_from >= request.bounds.system_time.upper()
                || header.valid_time.lower() >= request.bounds.valid_time.upper()
                || header.valid_time.upper() <= request.bounds.valid_time.lower()
                || scope.is_some_and(|keys| !keys.contains(&header.iid))
                || (trie.key.level() != 0 && origin.is_some_and(|origin| covered.contains(&origin.source_block()))) {
                continue;
            }
            if iids.insert(header.iid) {
                if iids.len() > limit { return Ok(None); }
                charge_rows(&mut charge, iids.len())?;
            }
        }
    }
    Ok(Some(iids))
}
