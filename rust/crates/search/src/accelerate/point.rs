use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use futures::{FutureExt, TryStreamExt};
use rtdb_format::{entity_history::{self, HistoryRoot}, keys::TrieKey};
use rtdb_storage::ObjectKey;
use rtdb_index::TrieState;
use rtdb_query::{LocatedHistoryEvent, ResolvedSearchRow, ScanRequest, SearchRowLocation, resolve_located_point};
use rtdb_types::{Anomaly, AnomalyCategory, EventOrigin, Iid, Result, RowVersion, TableRef};

use super::{covers, search_cancelled, tree::RecordedTree};
use crate::{SearchDefinition, SearchRuntime, file::PACKED_FORMAT_VERSION};

pub(super) struct PackedVisibility {
    missing: Vec<TrieKey>,
    primary_missing: Vec<TrieKey>,
    column: usize,
    enabled: bool,
    logical: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum SearchWitness { Packed(usize, u32), Live(RowVersion, Option<EventOrigin>), Logical(crate::file::TextRevision) }

pub(super) struct ResolvedVersion {
    pub row: ResolvedSearchRow,
    pub witness: Option<SearchWitness>,
}

impl PackedVisibility {
    pub fn new(request: &ScanRequest<'_>, table: &TableRef, sources: &[RecordedTree], column: usize, logical: bool) -> Self {
        let indexed: BTreeSet<_> = sources.iter().filter(|source| source.pack.locator.format() >= PACKED_FORMAT_VERSION)
            .flat_map(|source| source.members.values()).collect();
        let earliest = request.bounds.valid_time.lower().0.min(request.bounds.system_time.lower().0);
        let missing = request.snapshot.trie_catalog().iter_tries(table)
            .filter(|entry| entry.state == TrieState::Live && !indexed.contains(&entry.key))
            .filter(|entry| entry.key.recency().is_none_or(|day| day.epoch_day().saturating_mul(86_400_000_000) > earliest))
            .map(|entry| entry.key.clone()).collect();
        let primary_missing = request.snapshot.trie_catalog().iter_tries(table)
            .filter(|trie| trie.state == TrieState::Live && trie.artefact(entity_history::KIND, entity_history::ID, entity_history::VERSION).is_none())
            .map(|trie| trie.key.clone()).collect();
        let enabled = !table.is_owned_event_data() && (!indexed.is_empty()
            || request.snapshot.serving_search::<crate::runtime::ServingPostings>().is_some());
        Self { missing, primary_missing, column, enabled, logical }
    }

    pub fn covers(&self, iid: Iid) -> bool {
        self.enabled && !self.missing.iter().any(|trie| covers(trie.part(), &iid.0))
    }

    pub fn can_resolve(&self, iid: Iid) -> bool {
        self.covers(iid) || (self.logical && !self.primary_missing.iter().any(|trie| covers(trie.part(), &iid.0)))
    }

    pub async fn resolve(&self, runtime: &SearchRuntime, request: &ScanRequest<'_>, definition: &SearchDefinition,
        sources: &[RecordedTree], iids: &BTreeSet<Iid>) -> Result<Option<Vec<ResolvedVersion>>> {
        if let Some(rows) = rtdb_query::table_point_rows(request, iids).await? {
            let mut resolved = Vec::with_capacity(rows.len());
            let mut persisted = BTreeMap::<TrieKey, Vec<ResolvedSearchRow>>::new();
            for row in rows {
                match &row.location {
                    SearchRowLocation::Live(_) => {
                        let witness = Some(SearchWitness::Live(row.version, row.origin));
                        resolved.push(ResolvedVersion { row, witness });
                    }
                    SearchRowLocation::Persisted { trie, .. } if self.logical && row.origin.is_some() => {
                        persisted.entry(trie.clone()).or_default().push(row);
                    }
                    SearchRowLocation::Persisted { .. } => resolved.push(ResolvedVersion { row, witness: None }),
                }
            }
            for (trie, rows) in persisted {
                let witnesses = match self.forward_witnesses(request, definition, &trie, &rows).await {
                    Ok(found) => found,
                    Err(error) if error.category == AnomalyCategory::Interrupted => return Err(error),
                    Err(_) => BTreeMap::new(),
                };
                resolved.extend(rows.into_iter().map(|row| {
                    let witness = witnesses.get(&row.version.iid).copied().map(SearchWitness::Logical);
                    ResolvedVersion { row, witness }
                }));
            }
            return Ok(Some(resolved));
        }
        if !iids.iter().all(|&iid| self.covers(iid)) { return Ok(None); }
        let mut reservation = request.memory.reserve(4096usize.saturating_add(iids.len().saturating_mul(1024)))?;
        let mut reads = futures::stream::FuturesOrdered::new();
        let mut sources = sources.iter().enumerate().filter(|(_, source)| source.pack.locator.format() >= PACKED_FORMAT_VERSION);
        let mut histories = BTreeMap::<Iid, Vec<LocatedHistoryEvent<SearchWitness>>>::new();
        let mut event_count = 0usize;
        loop {
            while reads.len() < 8 {
                let Some((index, source)) = sources.next() else { break; };
                reads.push_back(async move {
                    let scope: BTreeSet<_> = iids.iter().filter(|iid| source.members.values().any(|trie| covers(trie.part(), &iid.0))).copied().collect();
                    let result = async {
                        if request.cancellation.is_cancelled() { return Err(search_cancelled()); }
                        if scope.is_empty() { return Ok(None); }
                        let file = source.open(runtime, request, definition).await?;
                        file.versions(&scope, self.column, request.memory).await.map(Some)
                    }.await;
                    if let Err(error) = &result { source.report(runtime, request, definition, error); }
                    result.map(|events| (index, source, events))
                }.boxed());
            }
            let Some((index, source, versions)) = reads.try_next().await? else { break; };
            let Some((iids, _decoded)) = versions else { continue; };
            for (iid, events) in iids {
                for event in events {
                    let Some(trie) = source.members.get(&event.member) else { continue; };
                    if !covers(trie.part(), &iid.0) {
                        return Err(Anomaly::fault("rtdb.search/visibility-member", "Search event lies outside its primary member"));
                    }
                    event_count += 1;
                    reserve_events(&mut reservation, event_count)?;
                    histories.entry(iid).or_default().push(LocatedHistoryEvent { header: event.header, origin: None,
                        location: SearchRowLocation::Persisted { trie: trie.clone(), page: event.page, row: event.row },
                        witness: if self.logical { event.revision.map(SearchWitness::Logical) }
                            else { event.document.map(|document| SearchWitness::Packed(index, document)) } });
                }
            }
        }
        for segment in request.snapshot.iter_segments(&definition.table) {
            for &iid in iids {
                if request.cancellation.is_cancelled() { return Err(search_cancelled()); }
                for row in segment.iid_sources(iid, request.memory)? {
                    let (header, source) = row?;
                    event_count += 1;
                    reserve_events(&mut reservation, event_count)?;
                    let origin = source.batch.view().origin(source.row)?;
                    histories.entry(iid).or_default().push(LocatedHistoryEvent { header, origin, location: SearchRowLocation::Live(source),
                        witness: Some(SearchWitness::Live(header.version(), origin)) });
                }
            }
        }
        resolve_located_point(histories, request.bounds, request.scan.clamp_valid_time, request.memory)
            .map(|resolved| resolved.map(|rows| rows.into_iter().map(|(row, witness)| ResolvedVersion { row, witness }).collect()))
    }
    async fn forward_witnesses(&self, request: &ScanRequest<'_>, definition: &SearchDefinition, trie: &TrieKey,
        rows: &[ResolvedSearchRow]) -> Result<BTreeMap<Iid, crate::file::TextRevision>> {
        let id = crate::lane::forward_id(definition);
        let catalog = request.snapshot.trie_catalog();
        let Some(record) = catalog.trie(&definition.table, trie)
            .and_then(|entry| entry.artefact(crate::lane::KIND, &id, definition.version)) else { return Ok(BTreeMap::new()); };
        let Some(table) = request.snapshot.table_entry(&definition.table) else { return Ok(BTreeMap::new()); };
        let root = HistoryRoot::decode(record.stats(), record.file_size())?;
        let key = ObjectKey::new(table.slug.recorded_artefact_key(trie, record)?)?;
        let file = rtdb_query::HistoryObject::open(request, key, record.file_size()).await?;
        let bytes = file.read(root.range()).await?;
        let _directory = request.memory.reserve(bytes.as_slice().len().saturating_mul(2).saturating_add(4096))?;
        let iids = rows.iter().map(|row| row.version.iid).collect();
        let ranges = entity_history::ranges(bytes.as_slice(), root, entity_history::forward_owner(&table.slug, trie, &id, definition.version), Some(&iids))?;
        let mut result = BTreeMap::new();
        for range in ranges {
            if request.cancellation.is_cancelled() { return Err(search_cancelled()); }
            let selected: Vec<_> = rows.iter().filter_map(|row| match row.location {
                SearchRowLocation::Persisted { page, row: index, .. } if page == range.page
                    && index >= range.first_row && index - range.first_row < range.rows => Some((row, index - range.first_row)),
                _ => None,
            }).collect();
            if selected.is_empty() { continue; }
            let bytes = file.read(range.range()).await?;
            let _decoded = request.memory.reserve(bytes.as_slice().len().saturating_mul(4).saturating_add(4096))?;
            let page = range.read_forward(bytes.as_slice(), definition.columns.len())?;
            for (row, index) in selected {
                let iid = row.version.iid;
                if let Some(revision) = page.revision(index as usize, iid, row.origin.expect("stable forward event"), self.column)? {
                    result.insert(iid, revision);
                }
            }
        }
        Ok(result)
    }

}

fn reserve_events(reservation: &mut Arc<rtdb_types::MemoryReservation>, events: usize) -> Result<()> {
    if events % 64 == 1 {
        let bytes = 4096usize.saturating_add((events + 63).saturating_mul(384));
        let reservation = Arc::get_mut(reservation).expect("visibility planning owns its scratch");
        if reservation.bytes() < bytes { reservation.grow_to(bytes)?; }
    }
    Ok(())
}
