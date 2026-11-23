use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use futures::{FutureExt, TryStreamExt};
use rtdb_format::keys::TrieKey;
use rtdb_index::TrieState;
use rtdb_query::{LocatedHistoryEvent, ResolvedSearchRow, ScanRequest, SearchRowLocation, resolve_located_point};
use rtdb_types::{Anomaly, Iid, Result, RowVersion, TableRef};

use super::{covers, search_cancelled, tree::RecordedTree};
use crate::{SearchDefinition, SearchRuntime, file::PACKED_FORMAT_VERSION};

pub(super) struct PackedVisibility {
    missing: Vec<TrieKey>,
    column: usize,
    enabled: bool,
    logical: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum SearchWitness { Packed(usize, u32), Live(RowVersion), Logical(crate::file::TextRevision) }

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
        let enabled = !table.is_owned_event_data() && (!indexed.is_empty()
            || request.snapshot.serving_search::<crate::runtime::ServingPostings>().is_some());
        Self { missing, column, enabled, logical }
    }

    pub fn covers(&self, iid: Iid) -> bool {
        self.enabled && !self.missing.iter().any(|trie| covers(trie.part(), &iid.0))
    }

    pub async fn resolve(&self, runtime: &SearchRuntime, request: &ScanRequest<'_>, definition: &SearchDefinition,
        sources: &[RecordedTree], iids: &BTreeSet<Iid>) -> Result<Option<Vec<ResolvedVersion>>> {
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
                    histories.entry(iid).or_default().push(LocatedHistoryEvent { header: event.header,
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
                    histories.entry(iid).or_default().push(LocatedHistoryEvent { header, location: SearchRowLocation::Live(source),
                        witness: Some(SearchWitness::Live(header.version())) });
                }
            }
        }
        resolve_located_point(histories, request.bounds, request.scan.clamp_valid_time, request.memory)
            .map(|resolved| resolved.map(|rows| rows.into_iter().map(|(row, witness)| ResolvedVersion { row, witness }).collect()))
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
