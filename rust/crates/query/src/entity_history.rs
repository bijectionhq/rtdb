use std::{collections::{BTreeMap, BTreeSet}, ops::Range, sync::Arc};

use futures::{FutureExt, TryStreamExt};
use rtdb_format::entity_history::{self, HistoryRoot};
use rtdb_index::{CatalogTrie, TrieState};
use rtdb_storage::{ObjectKey, cache::{BufferPool, CacheLease}};
use rtdb_types::{Anomaly, AnomalyCategory, AnomalyKind, BlockIndex, EventHeader, EventOp, Iid, MemoryBudget, Result};

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
    if let Some(rows) = crate::entity_directory::resolve_points(request, keys).await? { return Ok(Some(rows)); }
    let read_ahead = request.memory.snapshot_budget().is_some();
    match read_history(request, keys, read_ahead).await {
        Err(error) if read_ahead && error.is(AnomalyKind::ResourceExhausted) => read_history(request, keys, false).await,
        result => result,
    }
}

async fn read_history(request: &ScanRequest<'_>, keys: &BTreeSet<Iid>, read_ahead: bool) -> Result<Option<Vec<ResolvedSearchRow>>> {
    let table = &request.scan.table.name.table;
    let mut charge = request.memory.reserve(4096usize.saturating_add(keys.len().saturating_mul(480)))?;
    let mut histories = BTreeMap::<Iid, Vec<LocatedHistoryEvent<()>>>::new();
    let mut sources = request.snapshot.trie_catalog().iter_tries(table).filter(|trie| trie.state == TrieState::Live
        && keys.iter().any(|key| contains(trie.key.part(), *key)));
    let entry = request.snapshot.table_entry(table);
    let mut reads = futures::stream::FuturesOrdered::new();
    let mut count = 0usize;
    let mut retained_keys = 0usize;
    let mut identities = BTreeMap::<Iid, Option<Vec<u8>>>::new();
    loop {
        while reads.len() < 8 {
            let Some(trie) = sources.next() else { break; };
            let Some(table) = entry else { return Ok(None); };
            let record = trie.artefact(entity_history::KIND, entity_history::ID, entity_history::VERSION);
            reads.push_back(async move {
                if request.cancellation.is_cancelled() { return Err(cancelled()); }
                let mut rows = Vec::new();
                let mut reservation = request.memory.reserve(4096)?;
                let mut key_bytes = 0usize;
                let mut visit = |event, key: Option<&[u8]>| {
                    key_bytes = key_bytes.saturating_add(key.map_or(0, <[u8]>::len));
                    charge_history(&mut reservation, rows.len() + 1, key_bytes)?;
                    rows.push((event, key.map(<[u8]>::to_vec)));
                    Ok(true)
                };
                let Some(record) = record else {
                    crate::scan::primary_point_events(request, &table.slug, &trie.key, Some(crate::IidScope::Set(keys)), usize::MAX, read_ahead, visit).await?;
                    return Ok::<_, Anomaly>((rows, reservation, key_bytes));
                };
                let root = HistoryRoot::decode(record.stats(), record.file_size())?;
                let key = ObjectKey::new(table.slug.recorded_artefact_key(&trie.key, record)?)?;
                let file = HistoryObject::open(request, key, record.file_size()).await?;
                let directory = file.read(root.range()).await?;
                let _directory = request.memory.reserve(directory.as_slice().len().saturating_mul(2).saturating_add(4096))?;
                let ranges = entity_history::ranges(directory.as_slice(), root, entity_history::owner(&table.slug, &trie.key), Some(keys))?;
                for range in ranges {
                    if request.cancellation.is_cancelled() { return Err(cancelled()); }
                    let bytes = file.read(range.range()).await?;
                    let _decoded = request.memory.reserve(bytes.as_slice().len().saturating_mul(4).saturating_add(4096))?;
                    let page = range.read(arrow_buffer::Buffer::from(bytes::Bytes::from_owner(bytes)))?;
                    for row in page.rows(keys) {
                        let (header, origin) = page.event(row)?;
                        visit(LocatedHistoryEvent { header, origin, witness: None,
                            location: SearchRowLocation::Persisted { trie: trie.key.clone(), page: range.page, row: range.first_row + row as u32 } },
                            page.entity_key(row))?;
                    }
                }
                Ok((rows, reservation, key_bytes))
            }.boxed());
        }
        let next = reads.try_next().await.and_then(|batch| {
            if let Some((rows, _, key_bytes)) = &batch {
                count = count.saturating_add(rows.len());
                retained_keys = retained_keys.saturating_add(*key_bytes);
                charge_history(&mut charge, count, retained_keys)?;
            }
            Ok(batch)
        });
        let Some((rows, _held, _)) = (match next {
            Err(error) if read_ahead && error.is(AnomalyKind::ResourceExhausted) => {
                while reads.try_next().await.transpose().is_some() {}
                return Err(error);
            }
            result => result?,
        }) else { break; };
        for (row, key) in rows {
            if !same_identity(&mut identities, row.header.iid, key) { return Ok(None); }
            histories.entry(row.header.iid).or_default().push(row);
        }
    }
    for segment in request.snapshot.iter_segments(table) {
        for &iid in keys {
            if request.cancellation.is_cancelled() { return Err(cancelled()); }
            for located in segment.iid_sources(iid, request.memory)? {
                let (header, source) = located?;
                count = count.saturating_add(1);
                let batch = source.batch.view();
                let key = batch.entity_key(source.row)?;
                retained_keys = retained_keys.saturating_add(key.map_or(0, <[u8]>::len));
                charge_history(&mut charge, count, retained_keys)?;
                if !same_identity(&mut identities, header.iid, key.map(<[u8]>::to_vec)) { return Ok(None); }
                let origin = batch.origin(source.row)?;
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

pub(crate) fn charge_history(charge: &mut Arc<rtdb_types::MemoryReservation>, count: usize, key_bytes: usize) -> Result<()> {
    let lease = Arc::get_mut(charge).expect("unpublished histories own their reservation");
    let needed = 4096usize.saturating_add(count.saturating_mul(384)).saturating_add(key_bytes);
    if needed > lease.bytes() { lease.grow_to(needed.max(lease.bytes().saturating_mul(2)))?; }
    Ok(())
}

pub(crate) fn contains(part: &[u8], iid: Iid) -> bool {
    part.iter().enumerate().all(|(depth, digit)| (iid.0[depth / 4] >> (6 - 2 * (depth % 4))) & 3 == *digit)
}

pub(crate) fn cancelled() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "rtdb.query/cancelled", "Entity history read cancelled") }

/// `None` when more than `limit` IIDs, or more events than their budget, are uncovered.
pub async fn uncovered_history_iids(request: &ScanRequest<'_>, covered: &BTreeSet<BlockIndex>,
    scope: Option<crate::IidScope<'_>>, limit: usize) -> Result<Option<BTreeSet<Iid>>> {
    read_uncovered(request, covered, scope, limit).await
}

pub(crate) async fn visit_trie_events(request: &ScanRequest<'_>, slug: &rtdb_format::keys::TableSlug,
    trie: &CatalogTrie, scope: Option<crate::IidScope<'_>>, max_rows: usize,
    mut visit: impl FnMut(LocatedHistoryEvent<()>, Option<&[u8]>) -> Result<bool>) -> Result<Option<usize>> {
    let Some(record) = trie.artefact(entity_history::KIND, entity_history::ID, entity_history::VERSION) else {
        return crate::scan::primary_point_events(request, slug, &trie.key, scope, max_rows, false, visit).await;
    };
    let key = ObjectKey::new(slug.recorded_artefact_key(&trie.key, record)?)?;
    let root = HistoryRoot::decode(record.stats(), record.file_size())?;
    let file = HistoryObject::open(request, key, record.file_size()).await?;
    let directory = file.read(root.range()).await?;
    let _directory = request.memory.reserve(directory.as_slice().len().saturating_mul(2).saturating_add(4096))?;
    let ranges = entity_history::ranges_matching(directory.as_slice(), root, entity_history::owner(slug, &trie.key),
        |first, last| scope.is_none_or(|scope| scope.intersects(first, last)))?;
    let mut inspected = 0usize;
    for range in ranges {
        if request.cancellation.is_cancelled() { return Err(cancelled()); }
        inspected = inspected.saturating_add(range.rows as usize);
        if inspected > max_rows { return Ok(None); }
        let bytes = file.read(range.range()).await?;
        let _decoded = request.memory.reserve(bytes.as_slice().len().saturating_mul(4).saturating_add(4096))?;
        let page = range.read(arrow_buffer::Buffer::from(bytes::Bytes::from_owner(bytes)))?;
        for row in 0..range.rows as usize {
            let (header, origin) = page.event(row)?;
            if scope.is_some_and(|keys| !keys.contains(&header.iid)) { continue; }
            let event = LocatedHistoryEvent { header, origin, witness: None, location: SearchRowLocation::Persisted {
                trie: trie.key.clone(), page: range.page, row: range.first_row + row as u32,
            }};
            if !visit(event, page.entity_key(row))? { return Ok(None); }
        }
    }
    Ok(Some(inspected))
}

async fn read_uncovered(request: &ScanRequest<'_>, covered: &BTreeSet<BlockIndex>,
    scope: Option<crate::IidScope<'_>>, limit: usize) -> Result<Option<BTreeSet<Iid>>> {
    if request.cancellation.is_cancelled() { return Err(cancelled()); }
    let table = &request.scan.table.name.table;
    let mut iids = BTreeSet::new();
    let mut charge = request.memory.reserve(4096)?;
    let budget = limit.saturating_mul(32);
    let Some(entry) = request.snapshot.table_entry(table) else { return Ok(Some(iids)); };
    let mut nominate = |header: EventHeader| -> Result<bool> {
        if header.op != EventOp::Put || header.system_from >= request.bounds.system_time.upper()
            || header.valid_time.lower() >= request.bounds.valid_time.upper()
            || header.valid_time.upper() <= request.bounds.valid_time.lower()
            || scope.is_some_and(|keys| !keys.contains(&header.iid)) {
            return Ok(true);
        }
        if !iids.contains(&header.iid) {
            if iids.len() == limit { return Ok(false); }
            charge_history(&mut charge, iids.len() + 1, 0)?;
            iids.insert(header.iid);
        }
        Ok(true)
    };
    use crate::entity_directory::Nomination;
    let (through, mut inspected) = match crate::entity_directory::nominate(request, covered, scope, budget, &mut nominate).await? {
        Nomination::Undirected => (None, 0),
        Nomination::Nominated { through, inspected } => (Some(through), inspected),
        Nomination::OverBudget => return Ok(None),
    };
    for trie in request.snapshot.trie_catalog().iter_tries(table).filter(|trie| trie.state == TrieState::Live) {
        let is_l0 = trie.key.level() == 0;
        let block = trie.key.block_index();
        if (is_l0 && covered.contains(&block)) || (!is_l0 && through.is_some_and(|through| block <= through)) { continue; }
        if scope.is_some_and(|keys| !keys.iter().any(|key| contains(trie.key.part(), key))) { continue; }
        let Some(rows) = visit_trie_events(request, &entry.slug, trie, scope, budget.saturating_sub(inspected), |event, _| {
            if event.origin.is_some_and(|origin| {
                let block = origin.source_block();
                covered.contains(&block) || (!is_l0 && through.is_some_and(|through| block <= through))
            }) { return Ok(true); }
            nominate(event.header)
        }).await? else { return Ok(None); };
        inspected = inspected.saturating_add(rows);
    }
    Ok(Some(iids))
}
