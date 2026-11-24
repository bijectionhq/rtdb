use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use rtdb_format::{
    cow_tree::{self, RootRef},
    entity_directory::{self, all_rows, iid_rows},
    keys::TableSlug,
    log::proto::EntityDirectoryRoot,
};
use rtdb_index::{Snapshot, TrieState};
use rtdb_storage::{audience_work, cache::BufferPool};
use rtdb_types::{
    Anomaly, AnomalyCategory, BlockIndex, EventHeader, EventOp, EventOrigin, Iid, MemoryBudget,
    Result, TableRef,
};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{
    LocatedHistoryEvent, ObjectPageStore, ResolvedSearchRow, ScanRequest, SearchRowLocation,
    entity_history::{self, cancelled, charge_history, contains},
    resolve_located_point,
};

pub use rtdb_format::entity_directory::LogicalEvent;

pub struct DirectoryRead<'a> {
    pub snapshot: &'a Snapshot,
    pub storage: &'a Arc<BufferPool>,
    pub memory: &'a Arc<MemoryBudget>,
    pub cancellation: &'a CancellationToken,
}

impl<'a> From<&ScanRequest<'a>> for DirectoryRead<'a> {
    fn from(request: &ScanRequest<'a>) -> Self {
        Self {
            snapshot: request.snapshot,
            storage: request.storage,
            memory: request.memory,
            cancellation: request.cancellation,
        }
    }
}

impl DirectoryRead<'_> {
    fn pages(&self, slug: &TableSlug) -> ObjectPageStore {
        ObjectPageStore {
            store: self.storage.object_store().clone(),
            storage: self.storage.clone(),
            prefix: entity_directory::prefix(slug),
        }
    }
}

pub async fn table_events(
    read: &DirectoryRead<'_>,
    table: &TableRef,
    visit: impl FnMut(LogicalEvent) -> Result<()> + Send,
) -> Result<()> {
    let root = read
        .snapshot
        .trie_catalog()
        .entity_directory(table)
        .ok_or_else(|| invalid("Table has no captured directory"))?;
    let entry = read
        .snapshot
        .table_entry(table)
        .ok_or_else(|| invalid("Directory table has no slug"))?;
    table_events_at(
        root,
        &entry.slug,
        read.storage,
        read.memory,
        read.cancellation,
        visit,
    )
    .await
}

pub async fn table_events_at(
    receipt: &EntityDirectoryRoot,
    slug: &TableSlug,
    storage: &Arc<BufferPool>,
    memory: &Arc<MemoryBudget>,
    cancellation: &CancellationToken,
    mut visit: impl FnMut(LogicalEvent) -> Result<()> + Send,
) -> Result<()> {
    let pages = ObjectPageStore {
        store: storage.object_store().clone(),
        storage: storage.clone(),
        prefix: entity_directory::prefix(slug),
    };
    let root = receipt.reference()?;
    let mut frontier = Sha256::new();
    let (mut count, mut has_erases) = (0i64, false);
    cow_tree::visit(&pages, &root, all_rows(), &|_| true, memory, |row| {
        if cancellation.is_cancelled() {
            return Err(cancelled());
        }
        let event = LogicalEvent::decode(&row)?;
        if event.origin.source_block().0 > receipt.through_block() {
            return Err(invalid("Event lies after the directory boundary"));
        }
        count = count
            .checked_add(1)
            .ok_or_else(|| invalid("Directory row count overflow"))?;
        if event.header.op == EventOp::Erase {
            has_erases = true;
            entity_directory::add_frontier(&mut frontier, &event)?;
        }
        visit(event)?;
        Ok(true)
    })
    .await?;
    if count != receipt.source_rows()
        || has_erases != receipt.has_erases()
        || &frontier.finalize()[..] != receipt.erase_frontier_digest()
    {
        return Err(invalid("Directory contents differ from their receipt"));
    }
    Ok(())
}

async fn visit_rows(
    read: &DirectoryRead<'_>,
    slug: &TableSlug,
    root: &RootRef,
    keys: Option<crate::IidScope<'_>>,
    mut visit: impl FnMut(LogicalEvent) -> Result<bool> + Send,
) -> Result<()> {
    let pages = read.pages(slug);
    let ranges = keys
        .iter()
        .flat_map(|keys| keys.iter().map(iid_rows))
        .chain(keys.is_none().then(all_rows));
    let mut done = false;
    for range in ranges {
        cow_tree::visit(&pages, root, range, &|_| true, read.memory, |row| {
            if read.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            audience_work::scan_items(1)?;
            done = !visit(LogicalEvent::decode(&row)?)?;
            Ok(!done)
        })
        .await?;
        if done {
            break;
        }
    }
    Ok(())
}

pub async fn resolve_points(
    request: &ScanRequest<'_>,
    keys: &BTreeSet<Iid>,
) -> Result<Option<Vec<ResolvedSearchRow>>> {
    let table = &request.scan.table.name.table;
    let catalog = request.snapshot.trie_catalog();
    let Some(receipt) = catalog.entity_directory(table) else {
        return Ok(None);
    };
    let entry = request
        .snapshot
        .table_entry(table)
        .ok_or_else(|| invalid("Directory table has no slug"))?;
    let root = receipt.reference()?;
    let through = BlockIndex(receipt.through_block());
    let mut histories = BTreeMap::<(Iid, Option<Vec<u8>>), Vec<LocatedHistoryEvent<()>>>::new();
    let mut charge = request.memory.reserve(4096)?;
    let (mut count, mut key_bytes) = (0usize, 0usize);
    let mut collect = |event: LocatedHistoryEvent<()>, key: Option<&[u8]>| {
        count = count.saturating_add(1);
        key_bytes = key_bytes.saturating_add(key.map_or(0, <[u8]>::len).saturating_mul(2));
        charge_history(&mut charge, count, key_bytes)?;
        histories
            .entry((event.header.iid, key.map(<[u8]>::to_vec)))
            .or_default()
            .push(event);
        Ok(true)
    };
    let read = DirectoryRead::from(request);
    visit_rows(&read, &entry.slug, &root, Some(crate::IidScope::Set(keys)), |event| {
        if event.origin.source_block() > through {
            return Err(invalid("Event lies after the directory boundary"));
        }
        let (trie, page, row) = event
            .payload_hint
            .ok_or_else(|| invalid("Directory event has no location hint"))?;
        collect(
            LocatedHistoryEvent {
                header: event.header,
                origin: Some(event.origin),
                witness: None,
                location: SearchRowLocation::Persisted { trie, page, row },
            },
            event.entity_key.as_deref(),
        )
    })
    .await?;
    for trie in catalog.iter_tries(table).filter(|trie| {
        trie.state == TrieState::Live
            && trie.key.block_index() > through
            && keys.iter().any(|&iid| contains(trie.key.part(), iid))
    }) {
        entity_history::visit_trie_events(
            request,
            &entry.slug,
            trie,
            Some(crate::IidScope::Set(keys)),
            usize::MAX,
            |event, key| match event.origin {
                Some(origin) if origin.source_block() <= through => Ok(true),
                Some(_) => collect(event, key),
                None => Err(invalid("A persisted tail row has no stable origin")),
            },
        )
        .await?;
    }
    for segment in request.snapshot.iter_segments(table) {
        for &iid in keys {
            if request.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            for row in segment.iid_sources(iid, request.memory)? {
                let (header, source) = row?;
                audience_work::scan_items(1)?;
                let batch = source.batch.view();
                let origin = batch.origin(source.row)?;
                let key = batch.entity_key(source.row)?.map(<[u8]>::to_vec);
                collect(
                    LocatedHistoryEvent {
                        header,
                        origin,
                        witness: None,
                        location: SearchRowLocation::Live(source),
                    },
                    key.as_deref(),
                )?;
            }
        }
    }
    let mut previous = None;
    let mut resolved = Vec::new();
    for ((iid, key), events) in histories {
        if previous == Some(iid) {
            return Ok(None);
        }
        if key.is_none() {
            previous = Some(iid);
        } else {
            previous = None;
        }
        let Some(rows) = resolve_located_point(
            BTreeMap::from([(iid, events)]),
            request.bounds,
            request.scan.clamp_valid_time,
            request.memory,
        )?
        else {
            return Ok(None);
        };
        for (mut row, _) in rows {
            if let SearchRowLocation::Persisted { trie, .. } = &row.location
                && !catalog
                    .trie(table, trie)
                    .is_some_and(|trie| trie.state == TrieState::Live)
            {
                let origin = row
                    .origin
                    .ok_or_else(|| invalid("A relocated directory event has no stable origin"))?;
                let keys = BTreeSet::from([row.version.iid]);
                let mut location = None;
                for trie in catalog.iter_tries(table).filter(|trie| {
                    trie.state == TrieState::Live
                        && contains(trie.key.part(), row.version.iid)
                        && trie.key.block_index() >= origin.source_block()
                }) {
                    let mut matching = |candidate: LocatedHistoryEvent<()>, _: Option<&[u8]>| {
                        if candidate.origin == Some(origin)
                            && candidate.header.version() == row.version
                            && candidate.header.op == EventOp::Put
                            && candidate
                                .header
                                .valid_time
                                .contains(request.bounds.valid_time.lower())
                        {
                            location = Some(candidate.location);
                            return Ok(false);
                        }
                        Ok(true)
                    };
                    match entity_history::visit_trie_events(
                        request,
                        &entry.slug,
                        trie,
                        Some(crate::IidScope::Set(&keys)),
                        usize::MAX,
                        &mut matching,
                    )
                    .await
                    {
                        Err(error) if error.category != AnomalyCategory::Interrupted => {
                            crate::scan::primary_point_events(
                                request,
                                &entry.slug,
                                &trie.key,
                                Some(crate::IidScope::Set(&keys)),
                                usize::MAX,
                                matching,
                            )
                            .await?;
                        }
                        result => {
                            result?;
                        }
                    }
                    if location.is_some() {
                        break;
                    }
                }
                row.location = location
                    .ok_or_else(|| invalid("Visible directory origin has no live payload"))?;
            }
            resolved.push(row);
        }
    }
    Ok(Some(resolved))
}

pub(crate) async fn nominate(
    request: &ScanRequest<'_>,
    covered: &BTreeSet<BlockIndex>,
    scope: Option<crate::IidScope<'_>>,
    limit: usize,
    visit: &mut (impl FnMut(EventHeader, EventOrigin) -> Result<bool> + Send),
) -> Result<Option<(BlockIndex, usize)>> {
    let table = &request.scan.table.name.table;
    let Some(receipt) = request.snapshot.trie_catalog().entity_directory(table) else {
        return Ok(None);
    };
    let entry = request
        .snapshot
        .table_entry(table)
        .ok_or_else(|| invalid("Directory table has no slug"))?;
    let read = DirectoryRead::from(request);
    let mut inspected = 0usize;
    let mut complete = true;
    visit_rows(&read, &entry.slug, &receipt.reference()?, scope, |event| {
        if covered.contains(&event.origin.source_block()) {
            return Ok(true);
        }
        inspected = inspected.saturating_add(1);
        complete = inspected <= limit && visit(event.header, event.origin)?;
        Ok(complete)
    })
    .await?;
    if !complete {
        return Err(invalid("Directory gap nomination exceeded its budget"));
    }
    Ok(Some((BlockIndex(receipt.through_block()), inspected)))
}

fn invalid(message: impl std::fmt::Display) -> Anomaly {
    Anomaly::fault("rtdb.query/entity-directory", message.to_string())
}
