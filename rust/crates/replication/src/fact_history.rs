//! Evidence for replacing the live physical history of an owned relation.

use std::{cmp::Reverse, collections::{BTreeMap, BTreeSet, btree_map::Entry}, io::Cursor, sync::Arc};

use bytes::Bytes;
use rtdb_columnar::MemoryBudget;
use rtdb_format::{
    catalog::TableEntry,
    data::EventBatch,
    ipc::{ArrowFile, inspect_file_reserved},
    log::proto::{PublishedObjectVersion, TriesAdded},
    metadata::{MetadataFile, MetadataNode},
};
use rtdb_index::{CatalogTrie, Snapshot};
use rtdb_storage::{HeldView, IoRegion, ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, AnomalyCategory, EventOp, EventPosition, Result, TableRef, Timestamp};
use tokio_util::sync::CancellationToken;

pub(crate) fn needs_census(message: &TriesAdded) -> bool {
    message.tries.iter().any(|trie| {
        trie.table_name.as_deref().and_then(|name| name.split_once('/')).is_some_and(|(schema, _)| TableRef::owned_schema(schema))
            && trie.trie_key.as_deref().and_then(|key| key.parse::<rtdb_format::keys::TrieKey>().ok()).is_some_and(|key| key.level() > 0)
    })
}

/// The receiver supplies the installed predecessor. A successful comparison
/// covers every original event of the replaced files, including all possible
/// winners and predicate references. Routes outside the replacement keep their
/// original publications.
pub(crate) async fn admit(
    store: &dyn ObjectStore, snapshot: Snapshot, additions: &BTreeMap<TableRef, Vec<CatalogTrie>>,
    as_of: Timestamp, memory: &Arc<MemoryBudget>, cancellation: &CancellationToken,
) -> Result<()> {
    let snapshot = Arc::new(snapshot);
    let hold = HeldView::catalog_at(store, snapshot.clone(), snapshot.catalog_cut(),
        snapshot.catalog_checkpoint()?, snapshot.catalog_file_paths()).await?;
    IoRegion::reading(hold, async {
        let current = snapshot.trie_catalog();
        for (table, tries) in additions.iter().filter(|(table, _)| TableRef::owned_schema(&table.schema)) {
            if tries.iter().any(|trie| trie.key.level() == 0) {
                return Err(uncovered("A history replacement cannot publish a new decision"));
            }
            let selected = tries.iter().filter(|trie| trie.key.epoch() == current.naming_epoch()).cloned();
            let successor = current.with_tries(table.clone(), selected, as_of)?;
            let before = current.current_tries(table);
            let after = successor.current_tries(table);
            let before_keys = before.iter().map(|trie| &trie.key).collect::<BTreeSet<_>>();
            let after_keys = after.iter().map(|trie| &trie.key).collect::<BTreeSet<_>>();
            let removed = before.iter().filter(|trie| !after_keys.contains(&trie.key)).copied().collect::<Vec<_>>();
            let added = after.iter().filter(|trie| !before_keys.contains(&trie.key)).copied().collect::<Vec<_>>();
            if removed.is_empty() && added.is_empty() { continue; }
            let entry = snapshot.table_entry(table).ok_or_else(|| uncovered("The receiver has no matching relation declaration"))?;
            compare(store, entry, &removed, &added, memory, cancellation).await?;
        }
        Ok(())
    }).await
}

struct EventEvidence {
    fingerprint: [u8; 32],
    original_route: Timestamp,
    successor_route: Option<Timestamp>,
}

#[derive(Default)]
struct Census(BTreeMap<EventPosition, EventEvidence>);

impl Census {
    fn original(&mut self, position: EventPosition, fingerprint: [u8; 32], route: Timestamp) -> Result<()> {
        match self.0.entry(position) {
            Entry::Vacant(slot) => { slot.insert(EventEvidence { fingerprint, original_route: route, successor_route: None }); }
            Entry::Occupied(mut slot) if slot.get().fingerprint == fingerprint => {
                let evidence = slot.get_mut();
                evidence.original_route = evidence.original_route.max(route);
            }
            Entry::Occupied(_) => return Err(uncovered("A source position has different original effects")),
        }
        Ok(())
    }

    fn successor(&mut self, position: EventPosition, fingerprint: [u8; 32], route: Timestamp) -> Result<()> {
        let evidence = self.0.get_mut(&position).filter(|original| original.fingerprint == fingerprint)
            .ok_or_else(|| uncovered("A successor position has no matching original effect"))?;
        evidence.successor_route = Some(evidence.successor_route.map_or(route, |current| current.max(route)));
        Ok(())
    }

    fn exhaust(self) -> Result<()> {
        if self.0.values().any(|row| row.successor_route.is_none_or(|route| route < row.original_route)) {
            return Err(uncovered("The successor lost an original effect or narrowed its historical route"));
        }
        Ok(())
    }
}

async fn compare(
    store: &dyn ObjectStore, entry: &TableEntry, removed: &[&CatalogTrie], added: &[&CatalogTrie],
    memory: &Arc<MemoryBudget>, cancellation: &CancellationToken,
) -> Result<()> {
    let rows = removed.iter().try_fold(0usize, |total, trie| {
        let rows = row_count(trie)?;
        total.checked_add(rows).ok_or_else(|| uncovered("The source event inventory exceeds its bound"))
    })?;
    let _inventory = memory.reserve(rows.checked_mul(128).ok_or_else(|| uncovered("The source event inventory exceeds its bound"))?)?;
    let mut census = Census::default();
    for trie in removed {
        visit(store, entry, trie, memory, cancellation, |position, fingerprint, route| census.original(position, fingerprint, route)).await?;
    }
    for trie in added {
        visit(store, entry, trie, memory, cancellation, |position, fingerprint, route| census.successor(position, fingerprint, route)).await?;
    }
    census.exhaust()
}

fn row_count(trie: &CatalogTrie) -> Result<usize> {
    trie.metadata.as_ref().and_then(|metadata| metadata.row_count).and_then(|rows| usize::try_from(rows).ok())
        .ok_or_else(|| uncovered("A selected trie has no exact row count"))
}

async fn visit(
    store: &dyn ObjectStore, entry: &TableEntry, trie: &CatalogTrie,
    memory: &Arc<MemoryBudget>, cancellation: &CancellationToken,
    mut event: impl FnMut(EventPosition, [u8; 32], Timestamp) -> Result<()>,
) -> Result<()> {
    let data = ObjectKey::new(entry.slug.data_key(&trie.key))?;
    let metadata = ObjectKey::new(entry.slug.metadata_key(&trie.key))?;
    let publications = trie.published_files.as_ref();
    let extent = u64::try_from(trie.data_file_size).map_err(|_| uncovered("A selected trie has an invalid size"))?;
    let (data, _data) = open(store, &data, publications.and_then(|files| files.data.as_ref()), Some(extent), memory).await?;
    let (metadata, _metadata) = open(store, &metadata, publications.and_then(|files| files.metadata.as_ref()), None, memory).await?;
    let metadata_layout = inspect_file_reserved(&mut Cursor::new(metadata.clone()), |bytes| memory.reserve(bytes))?;
    let _metadata_decode = memory.reserve(metadata_layout.full_allocation_bound()?)?;
    let metadata = MetadataFile::read_reader(Cursor::new(metadata))?;
    let mut data = Cursor::new(data);
    let data_layout = inspect_file_reserved(&mut data, |bytes| memory.reserve(bytes))?;
    let _data_open = memory.reserve(data_layout.opening_allocation_bound()?)?;
    let mut reader = ArrowFile::from_inspected_reader(data, None, data_layout)?;

    let pages = reader.page_count();
    let charge = pages.checked_mul(160).and_then(|bytes| metadata.nodes().len().checked_mul(160).and_then(|nodes| bytes.checked_add(nodes)))
        .ok_or_else(|| uncovered("The metadata route inventory exceeds its bound"))?;
    let _routes = memory.reserve(charge)?;
    let maximum = trie.key.recency().map_or(Ok(Timestamp::MAX), |date| {
        date.epoch_day().checked_mul(86_400_000_000).map(Timestamp).ok_or_else(|| uncovered("Invalid trie recency"))
    })?;
    let routes = page_routes(&metadata, pages, trie.key.part(), maximum)?;
    let mut remaining = row_count(trie)?;
    for (page, (path, route)) in routes.into_iter().enumerate() {
        if cancellation.is_cancelled() { return Err(uncovered("The receiver's history census was interrupted")); }
        let _page = memory.reserve(reader.page_allocation_bound(page)?)?;
        let batch = EventBatch::new(reader.read_page(page)?)?;
        remaining = remaining.checked_sub(batch.len()).ok_or_else(|| uncovered("The trie exceeded its recorded row count"))?;
        let mut previous = None;
        for row in 0..batch.len() {
            let header = batch.header(row)?;
            let rank = (header.iid, Reverse(header.precedence()));
            if previous.is_some_and(|prior| rank < prior) {
                return Err(uncovered("A selected page changed original winner order"));
            }
            previous = Some(rank);
            let borrowed = batch.borrow_event(row)?;
            let size = borrowed.document_allocation_bound()?.checked_add(borrowed.physical_entity_key().map_or(0, |key| key.len()))
                .and_then(|bytes| bytes.checked_add(1024)).ok_or_else(|| uncovered("A positioned event exceeds its decode bound"))?;
            let _decoded = memory.reserve(size)?;
            let value = batch.read_event(row)?;
            if header.op == EventOp::Erase {
                return Err(uncovered("Owned erasure requires its ordered custody successor"));
            }
            for (depth, bucket) in path.iter().enumerate() {
                if value.iid.bucket(depth)? != *bucket { return Err(uncovered("An event escaped its admitted IID route")); }
            }
            let position = value.position.ok_or_else(|| uncovered("A selected event has no original position"))?;
            event(position, value.history_fingerprint()?, route)?;
        }
        tokio::task::yield_now().await;
    }
    if remaining != 0 { return Err(uncovered("The selected trie inventory did not exhaust its row count")); }
    Ok(())
}

fn page_routes(metadata: &MetadataFile, pages: usize, prefix: &[u8], maximum: Timestamp)
    -> Result<Vec<(Vec<u8>, Timestamp)>> {
    let mut routes = vec![None; pages];
    let mut seen = vec![false; metadata.nodes().len()];
    let mut pending = metadata.root_index().into_iter().map(|node| (node, prefix.to_vec(), maximum)).collect::<Vec<_>>();
    while let Some((node, path, ceiling)) = pending.pop() {
        let visited = seen.get_mut(node).ok_or_else(|| uncovered("A metadata branch has no valid node"))?;
        if std::mem::replace(visited, true) || path.len() > 64 {
            return Err(uncovered("The metadata route has a repeated node or invalid depth"));
        }
        match &metadata.nodes()[node] {
            MetadataNode::Nil => {}
            MetadataNode::IidBranch(children) => {
                pending.extend(children.iter().enumerate().filter_map(|(bucket, child)| {
                    let mut next = path.clone();
                    next.push(bucket as u8);
                    Some(((*child)?, next, ceiling))
                }));
            }
            MetadataNode::RecencyBranch(children) => {
                pending.extend(children.iter().map(|(upper, child)| (*child, path.clone(), ceiling.min(*upper))));
            }
            MetadataNode::Leaf { data_page_index, .. } => {
                let route = routes.get_mut(*data_page_index).ok_or_else(|| uncovered("A metadata leaf has no data page"))?;
                if route.replace((path, ceiling)).is_some() { return Err(uncovered("A data page has more than one route")); }
            }
        }
    }
    routes.into_iter().map(|route| route.ok_or_else(|| uncovered("A data page is absent from the metadata routes"))).collect()
}

async fn open(
    store: &dyn ObjectStore, key: &ObjectKey, expected: Option<&PublishedObjectVersion>, extent: Option<u64>, memory: &Arc<MemoryBudget>,
) -> Result<(Bytes, Arc<rtdb_columnar::MemoryReservation>)> {
    async fn require_identity(store: &dyn ObjectStore, key: &ObjectKey, expected: Option<&PublishedObjectVersion>) -> Result<()> {
        if let Some(expected) = expected
            && store.retirement_publication(key).await?.map(PublishedObjectVersion::from).as_ref() != Some(expected)
        {
            return Err(uncovered("A selected trie publication changed"));
        }
        Ok(())
    }

    require_identity(store, key, expected).await?;
    let size = store.head_fresh(key).await?.size;
    if extent.is_some_and(|extent| extent != size) { return Err(uncovered("A selected trie has another extent")); }
    let length = usize::try_from(size).map_err(|_| uncovered("A selected trie exceeds addressable memory"))?;
    let allocation = store.exact_object_allocation(key, length)?;
    let charge = memory.reserve(allocation.retained_bytes.checked_add(allocation.scratch_bytes)
        .ok_or_else(|| uncovered("A selected trie exceeds its allocation bound"))?)?;
    let bytes = IoRegion::retaining(charge.clone(), store.get_fresh_exact(key, length)).await?;
    require_identity(store, key, expected).await?;
    Ok((bytes, charge))
}

pub(crate) fn uncovered(reason: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.replication/fact-history-uncovered", reason)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_format::data::{StoredEvent, StoredOperation};
    use rtdb_types::{Iid, Interval, StructFields, TxId, Value};

    #[test]
    fn a_successor_preserves_every_original_position_and_mask() -> Result<()> {
        let position = |ordinal| EventPosition { transaction: TxId(4), ordinal };
        let effect = |ordinal, operation| StoredEvent {
            position: Some(position(ordinal)), origin: None, commit_position: ordinal as u32,
            entity_key: Some(Arc::from(&b"original key"[..])), iid: Iid::MIN,
            system_from: Timestamp(100), valid_time: Interval::ALL, operation, recency: None,
        };
        let original = effect(1, StoredOperation::Put(StructFields::from([("value".into(), Value::I64(1))]))).history_fingerprint()?;
        let mask = effect(2, StoredOperation::Delete).history_fingerprint()?;
        let changed = effect(2, StoredOperation::Put(StructFields::new())).history_fingerprint()?;
        let prepare = || -> Result<Census> {
            let mut census = Census::default();
            census.original(position(1), original, Timestamp(200))?;
            census.original(position(1), original, Timestamp(250))?;
            census.original(position(2), mask, Timestamp::MAX)?;
            Ok(census)
        };
        let mut missing_mask = prepare()?;
        missing_mask.successor(position(1), original, Timestamp(250))?;
        assert!(missing_mask.successor(position(2), changed, Timestamp::MAX).is_err());
        assert!(missing_mask.exhaust().is_err());
        let mut narrowed = prepare()?;
        narrowed.successor(position(1), original, Timestamp(200))?;
        narrowed.successor(position(2), mask, Timestamp::MAX)?;
        assert!(narrowed.exhaust().is_err());
        let mut covered = prepare()?;
        covered.successor(position(1), original, Timestamp(250))?;
        covered.successor(position(2), mask, Timestamp::MAX)?;
        covered.exhaust()
    }
}
