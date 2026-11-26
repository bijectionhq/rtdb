use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use std::io::Cursor;
use std::sync::Arc;

use arrow_buffer::Buffer;
use bytes::Bytes;
use futures::StreamExt;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{data::{EncodedEvent, EventBatch}, ipc::{ArrowFile, inspect_file_reserved}};
use rtdb_index::{CatalogTrie, Snapshot, TrieState, branches::RankedIndexDeclaration};
use rtdb_query::{CandidateIids, IidScope, OrderedIidBuffer, QueryContext, visit_projected_data_events};
use rtdb_storage::{ObjectKey, cache::{BufferPool, CacheLease}};
use rtdb_types::{Anomaly, AnomalyCategory, Iid, Result, TableRef, TemporalBounds};
use rtdb_types::AnomalyKind;
use tokio_util::sync::CancellationToken;

use crate::{
    definition::{self, VectorDefinition},
    file::{Block, Cluster, Directory, Locator},
    probe,
    rows::{self, Partition, UnitProbe},
};

/// Where and at which cut a shortlist reads (vector.allium: ApproximateRead).
#[derive(Clone, Copy)]
pub struct Cut<'a> {
    pub snapshot: &'a Snapshot,
    pub storage: &'a Arc<BufferPool>,
    pub memory: &'a Arc<MemoryBudget>,
    pub cancellation: &'a CancellationToken,
    /// A valid-time point and a system-time point.
    pub bounds: TemporalBounds,
}

/// The budget past which the approximate route hands the read to the exact route.
#[derive(Clone, Copy, Debug)]
pub struct Budget { pub max_uncovered_bytes: i64, pub max_candidates: usize }

impl Default for Budget {
    fn default() -> Self { Self { max_uncovered_bytes: 64 << 20, max_candidates: 65_536 } }
}

/// One round of the shortlist: distinct candidates in IID order, and whether every eligible row has been considered.
pub struct Round { pub candidates: CandidateIids, pub complete: bool }

/// An opened generation and its clusters eligible under the partition filter, nearest centroid first.
struct Opened {
    read: GenerationRead,
    clusters: Vec<Cluster>,
    probed: usize,
    _allocation: Arc<MemoryReservation>,
}

enum GenerationRead { Ranges(ObjectKey), Verified(CacheLease) }

impl Opened {
    fn rank(read: GenerationRead, directory: &Directory, probe: &[f32], partition: Option<Partition>, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        let eligible = directory.partition_rows(partition);
        let count = eligible.len();
        if count == 0 { return Ok(None); }
        let key_bytes = match &read { GenerationRead::Ranges(key) => key.as_str().len(), GenerationRead::Verified(_) => 0 };
        let held = (2 * size_of::<Self>()).saturating_add(key_bytes).saturating_add(count.saturating_mul(size_of::<Cluster>()));
        let bytes = held.saturating_add(count.saturating_mul(size_of::<(f64, usize)>()));
        let mut allocation = memory.reserve(bytes)?;
        let mut order = Vec::with_capacity(count);
        order.extend(eligible.map(|row| (rows::unit_distance(probe, directory.centroid(row)), row)));
        order.sort_unstable_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
        let mut clusters = Vec::with_capacity(count);
        clusters.extend(order.iter().map(|&(_, row)| directory.cluster(row)));
        drop(order);
        Arc::get_mut(&mut allocation).expect("directory ranking owns its reservation").shrink_to(held)?;
        Ok(Some(Self { read, clusters, probed: 0, _allocation: allocation }))
    }
}

/// The nearest candidates of a probe across a definition's published generations and the frontier above them.
/// Each round probes further clusters; the caller resolves the candidates at its cut and widens the shortlist
/// until enough of them pass its filter, every row has been considered, or the budget is spent.
pub struct Shortlist {
    probe: UnitProbe,
    plan: probe::Plan,
    budget: Budget,
    partition: Option<Partition>,
    scores: Scores,
    opened: Vec<Opened>,
    _opened_allocation: Arc<MemoryReservation>,
}

impl Shortlist {
    /// Opens the generations at the cut and ranks the frontier, or `None` where the frontier exceeds the budget,
    /// the cut is not a pair of points, or the generations disagree with the definition.
    pub async fn open(cut: Cut<'_>, definition: &VectorDefinition, probe: UnitProbe, limit: usize, partition: Option<Partition>,
        scope: Option<IidScope<'_>>, budget: Budget) -> Result<Option<Self>> {
        let bounds = cut.bounds;
        if !(bounds.valid_time.is_point() && bounds.system_time.is_point()) || cut.snapshot.has_overlays() { return Ok(None); }
        if probe.values().len() != definition.dimensions { return Ok(None); }
        let Some(sources) = sources(cut, definition, budget)? else { return Ok(None) };
        let mut scores = Scores::new(cut.memory, budget.max_candidates)?;
        FrontierRanker {
            definition, probe: probe.values(), partition, scope, bounds, scores: &mut scores,
        }.rank(cut, &sources.entries).await?;
        if scores.overflowed { return Ok(None); }
        let mut opened_allocation = cut.memory.reserve(0)?;
        let mut opened = Vec::new();
        for source in sources.entries {
            let SourceKind::Generation { key, locator } = source.kind else { continue };
            if cut.cancellation.is_cancelled() { return Err(crate::kmeans::cancelled()); }
            let lease = cut.storage.get_range_or(&key, locator.directory_range(), cut.memory).await?;
            let directory = Directory::read(lease, &locator, cut.memory)?;
            if directory.dimensions != definition.dimensions { return Ok(None); }
            if let Some(generation) = Opened::rank(GenerationRead::Ranges(key), &directory, probe.values(), partition, cut.memory)? {
                admit_growth(&mut opened, 0, Arc::get_mut(&mut opened_allocation).expect("shortlist owns its allocation"))?;
                opened.push(generation);
            }
        }
        Ok(Some(Self { plan: probe::Plan::new(definition.approximate.recall, limit), probe, budget, partition, scores, opened, _opened_allocation: opened_allocation }))
    }

    /// Opens only fully proved native generations. The caller holds the catalog's read region.
    pub async fn open_native(
        context: &Arc<QueryContext>, bounds: TemporalBounds, table: &TableRef, name: &str,
        declaration: &RankedIndexDeclaration, probe: UnitProbe, limit: usize, partition: Option<Partition>,
    ) -> Result<Option<Self>> {
        if !(bounds.valid_time.is_point() && bounds.system_time.is_point()) { return Ok(None); }
        let Some(definition) = crate::readiness::native_definition(context, table, name, declaration).await? else {
            return Ok(None);
        };
        Self::open_mounted(context, bounds, &definition, probe, limit, partition).await
    }

    pub async fn open_mounted(context: &Arc<QueryContext>, bounds: TemporalBounds, definition: &VectorDefinition,
        probe: UnitProbe, limit: usize, partition: Option<Partition>) -> Result<Option<Self>> {
        if !(bounds.valid_time.is_point() && bounds.system_time.is_point()) || probe.values().len() != definition.dimensions {
            return Ok(None);
        }
        let budget = Budget { max_uncovered_bytes: 0, ..Budget::default() };
        let mut opened_allocation = context.memory.reserve(0)?;
        let mut opened = Vec::new();
        let covered = crate::readiness::visit_native_generations(context, definition, Some(bounds), |bytes, directory| {
            if let Some(generation) = Opened::rank(GenerationRead::Verified(bytes), &directory, probe.values(), partition, &context.memory)? {
                admit_growth(&mut opened, 0, Arc::get_mut(&mut opened_allocation).expect("shortlist owns its allocation"))?;
                opened.push(generation);
            }
            Ok(())
        }).await?;
        if !covered { return Ok(None); }
        let scores = Scores::new(&context.memory, budget.max_candidates)?;
        Ok(Some(Self { plan: probe::Plan::new(definition.approximate.recall, limit), probe, budget, partition, scores, opened, _opened_allocation: opened_allocation }))
    }

    /// Probes the clusters the current plan reaches and returns the nearest distinct IIDs, or `None` past the candidate budget.
    pub async fn round(&mut self, cut: Cut<'_>, scope: Option<IidScope<'_>>) -> Result<Option<Round>> {
        for generation in &mut self.opened {
            let probes = self.plan.probes(generation.clusters.len());
            let mut start = generation.probed.min(probes);
            while start < probes {
                let clusters = &generation.clusters[start..probes];
                let read = &generation.read;
                let width = read_width(cut, read, clusters);
                let clusters = &clusters[..width];
                let dimensions = self.probe.values().len();
                // Collected first so the stream holds plain futures rather than a lifetime-generic closure,
                // which the `Send` check of the async-trait accelerator cannot prove general enough.
                let reads = clusters.iter().enumerate().map(|(index, cluster)| {
                    async move { (index, read_block(cut, read, cluster, dimensions).await) }
                }).collect::<Vec<_>>();
                let blocks = futures::stream::iter(reads).buffer_unordered(width);
                futures::pin_mut!(blocks);
                let mut retry = [false; MAX_CLUSTER_READS];
                while let Some((index, block)) = blocks.next().await {
                    match block {
                        Ok(block) => rank_block(&block, cut, self.probe.values(), scope, &mut self.scores)?,
                        Err(error) if width > 1 && refused_read_memory(&error) => retry[index] = true,
                        Err(error) => return Err(error),
                    }
                }
                for (_, cluster) in clusters.iter().enumerate().filter(|(index, _)| retry[*index]) {
                    let block = read_block(cut, read, cluster, dimensions).await?;
                    rank_block(&block, cut, self.probe.values(), scope, &mut self.scores)?;
                }
                start += width;
                generation.probed = start;
            }
        }
        let Some((candidates, truncated)) = self.scores.nearest(self.plan.target(), cut.memory)? else { return Ok(None) };
        let complete = !truncated && self.opened.iter().all(|generation| generation.probed >= generation.clusters.len());
        Ok(Some(Round { candidates, complete }))
    }

    /// Doubles the probe fraction and the shortlist; false once the shortlist would exceed the candidate budget.
    pub fn widen(&mut self) -> bool { self.plan.widen(self.budget.max_candidates) }

    pub fn partition(&self) -> Option<Partition> { self.partition }
}

const MAX_CLUSTER_READS: usize = 8;

fn read_width(cut: Cut<'_>, read: &GenerationRead, clusters: &[Cluster]) -> usize {
    let mut available = cut.memory.available_bytes();
    let mut width = 0;
    for cluster in clusters.iter().take(MAX_CLUSTER_READS) {
        let needed = match read {
            GenerationRead::Ranges(key) => {
                let Ok(read) = cut.storage.object_store().scan_range_allocation(key, cluster.read_len()) else { break };
                read.retained_bytes.saturating_add(read.scratch_bytes.max(cluster.decode_bound()))
            }
            GenerationRead::Verified(_) => cluster.decode_bound(),
        };
        let Some(remaining) = available.checked_sub(needed) else { break };
        available = remaining;
        width += 1;
    }
    width.max(1)
}

async fn read_block(cut: Cut<'_>, read: &GenerationRead, cluster: &Cluster, dimensions: usize) -> Result<Block> {
    if cut.cancellation.is_cancelled() { return Err(crate::kmeans::cancelled()); }
    let lease = match read {
        GenerationRead::Ranges(key) => cut.cancellation.run_until_cancelled(
            cut.storage.get_range_or(key, cluster.range.clone(), cut.memory),
        ).await.ok_or_else(crate::kmeans::cancelled)??,
        GenerationRead::Verified(bytes) => bytes.slice(cluster.range.start as usize..cluster.range.end as usize)?,
    };
    Block::read(lease, cluster, dimensions, cut.memory)
}

fn rank_block(block: &Block, cut: Cut<'_>, probe: &[f32], scope: Option<IidScope<'_>>, scores: &mut Scores) -> Result<()> {
    for row in 0..block.len() {
        if row % 4096 == 0 && cut.cancellation.is_cancelled() { return Err(crate::kmeans::cancelled()); }
        let iid = block.iid(row)?;
        let (valid_from, valid_to, system_from) = block.extent(row);
        if scope.is_some_and(|scope| !scope.contains(&iid)) || !may_be_visible(valid_from, valid_to, system_from, cut.bounds) {
            continue;
        }
        scores.add(iid, rows::unit_distance(probe, block.unit(row)))?;
    }
    Ok(())
}

fn refused_read_memory(error: &Anomaly) -> bool {
    error.category != AnomalyCategory::Interrupted
        && error.is(AnomalyKind::ResourceExhausted)
}

enum SourceKind { Generation { key: ObjectKey, locator: Locator }, Frontier { data: ObjectKey } }

struct Source<'a> {
    trie: &'a CatalogTrie,
    ordinal: usize,
    kind: SourceKind,
}

struct Sources<'a> {
    entries: Vec<Source<'a>>,
    _allocation: Arc<MemoryReservation>,
}

/// The cut's sources, or `None` where the frontier is too large to rank exactly within budget.
fn sources<'a>(cut: Cut<'a>, definition: &VectorDefinition, budget: Budget) -> Result<Option<Sources<'a>>> {
    let table = &definition.table;
    let Some(entry) = cut.snapshot.table_entry(table) else { return Ok(None) };
    let live_visible = |trie: &&CatalogTrie| trie.state == TrieState::Live && visible(trie, cut.bounds);
    let mut allocation = cut.memory.reserve(0)?;
    let mut entries = Vec::new();
    let mut uncovered = 0i64;
    let _id = cut.memory.reserve(definition.name.len().saturating_add(64))?;
    let artefact_id = definition.artefact_id();
    for (ordinal, trie) in cut.snapshot.trie_catalog().iter_tries(table).filter(live_visible).enumerate() {
        if cut.cancellation.is_cancelled() { return Err(crate::kmeans::cancelled()); }
        let recorded = trie.artefact(definition::KIND, &artefact_id, definition.generation)
            .and_then(|record| Some((record, Locator::decode(record.stats.as_deref()?)?)))
            .filter(|(_, locator)| locator.dimensions as usize == definition.dimensions);
        let key_bytes = entry.slug.as_str().len().saturating_add(trie.key.part().len());
        let key_bytes = recorded.as_ref().map_or(key_bytes, |(record, _)| {
            record.pack_members.iter().fold(key_bytes.saturating_add(artefact_id.len()), |size, member| {
                size.saturating_add(member.len()).saturating_add(128)
            })
        });
        let mut key_scratch = recorded.map(|_| cut.memory.reserve(key_scratch_bytes(key_bytes))).transpose()?;
        let generation = recorded.and_then(|(record, locator)| {
            let key = ObjectKey::new(entry.slug.recorded_artefact_key(&trie.key, record).ok()?).ok()?;
            Some((key, locator))
        });
        let (key, locator) = match generation {
            Some((key, locator)) => (key, Some(locator)),
            None => {
                if trie.data_file_size < 0 { return Ok(None); }
                uncovered = uncovered.saturating_add(trie.data_file_size);
                if uncovered > budget.max_uncovered_bytes { return Ok(None); }
                if key_scratch.is_none() {
                    key_scratch = Some(cut.memory.reserve(key_scratch_bytes(key_bytes))?);
                }
                let Ok(key) = ObjectKey::new(entry.slug.data_key(&trie.key)) else { return Ok(None) };
                (key, None)
            }
        };
        let bytes = key.as_str().len().saturating_mul(2);
        let kind = match locator {
            Some(locator) => SourceKind::Generation { key, locator },
            None => SourceKind::Frontier { data: key },
        };
        admit_growth(&mut entries, bytes, Arc::get_mut(&mut allocation).expect("source plan owns its allocation"))?;
        entries.push(Source { trie, ordinal, kind });
        drop(key_scratch);
    }
    if uncovered > budget.max_uncovered_bytes { return Ok(None); }
    entries.sort_unstable_by(|left, right| left.trie.key.scan_precedence().cmp(&right.trie.key.scan_precedence())
        .then_with(|| left.ordinal.cmp(&right.ordinal)));
    Ok(Some(Sources { entries, _allocation: allocation }))
}

fn key_scratch_bytes(key_bytes: usize) -> usize {
    512usize.saturating_add(key_bytes.saturating_mul(4))
}

pub(crate) fn admit_growth<T>(values: &mut Vec<T>, additional: usize, allocation: &mut MemoryReservation) -> Result<()> {
    let held = allocation.bytes().saturating_add(additional);
    if values.len() < values.capacity() { return allocation.grow_to(held); }
    let current = values.capacity();
    let capacity = current.saturating_mul(2).max(1);
    allocation.grow_to(held.saturating_add(capacity.saturating_mul(size_of::<T>())))?;
    values.try_reserve_exact(capacity - values.len())
        .map_err(|error| Anomaly::fault("rtdb.vector/staging", error.to_string()))?;
    allocation.shrink_to(held.saturating_add((values.capacity() - current).saturating_mul(size_of::<T>())))
}

pub(crate) fn visible(trie: &CatalogTrie, bounds: TemporalBounds) -> bool {
    let earliest = bounds.valid_time.lower().0.min(bounds.system_time.lower().0);
    trie.key.recency().is_none_or(|date| date.epoch_day().saturating_mul(86_400_000_000) > earliest)
}

struct FrontierRanker<'a> {
    definition: &'a VectorDefinition,
    probe: &'a [f32],
    partition: Option<Partition>,
    scope: Option<IidScope<'a>>,
    bounds: TemporalBounds,
    scores: &'a mut Scores,
}

impl FrontierRanker<'_> {
    async fn rank(mut self, cut: Cut<'_>, sources: &[Source<'_>]) -> Result<()> {
        let definition = self.definition;
        for source in sources {
            let SourceKind::Frontier { data } = &source.kind else { continue };
            let columns = definition.put_columns();
            self.rank_file(cut, data, &columns).await?;
        }
        for segment in cut.snapshot.iter_segments(&definition.table) {
            for batch in segment.batches() {
                self.rank_batch(batch.view(), cut.memory)?;
            }
        }
        Ok(())
    }

    async fn rank_file(&mut self, cut: Cut<'_>, data: &ObjectKey, columns: &[String]) -> Result<()> {
        let check_cancelled = || {
            if cut.cancellation.is_cancelled() { return Err(crate::kmeans::cancelled()); }
            Ok(())
        };
        if cut.storage.object_store().supports_scan_ranges(data) {
            let visit = visit_projected_data_events(cut.storage, data, cut.memory, columns, check_cancelled,
                |events| self.rank_batch(events, cut.memory));
            return cut.cancellation.run_until_cancelled(visit).await.ok_or_else(crate::kmeans::cancelled)?;
        }

        let lease = cut.storage.get_or(data, cut.memory).await?;
        let (mut reader, _metadata) = open_frontier_file(Bytes::from_owner(lease), cut.memory)?;
        for page in 0..reader.page_count() {
            check_cancelled()?;
            let (batch, _page) = reader.read_event_page_reserved(page, columns, cut.memory)?;
            let events = EventBatch::new(batch)?;
            self.rank_batch(&events, cut.memory)?;
        }
        Ok(())
    }

    fn rank_batch(&mut self, events: &EventBatch, memory: &Arc<MemoryBudget>) -> Result<()> {
        for row in 0..events.len() {
            let event = events.borrow_event(row)?;
            let header = event.header();
            let eligible = |partition| self.partition.is_none_or(|expected| partition == expected)
                && self.scope.is_none_or(|scope| scope.contains(&header.iid))
                && may_be_visible(header.valid_time.lower().0, header.valid_time.upper().0, header.system_from.0, self.bounds);
            if let Some(distance) = rows::frontier_distance(&event, self.definition, self.probe, memory, eligible)? {
                self.scores.add(header.iid, distance)?;
            }
        }
        Ok(())
    }

}

fn open_frontier_file(bytes: Bytes, memory: &Arc<MemoryBudget>) -> Result<(ArrowFile, Arc<MemoryReservation>)> {
    let layout = inspect_file_reserved(&mut Cursor::new(bytes.as_ref()), |bytes| memory.reserve(bytes))?;
    let opening = memory.reserve(layout.opening_allocation_bound()?)?;
    let reader = if layout.uncompressed && layout.dictionary_encoded_bytes == 0 {
        ArrowFile::from_inspected_buffer(Buffer::from(bytes), None, layout)?
    } else {
        ArrowFile::from_inspected_reader(Cursor::new(bytes), None, layout)?
    };
    Ok((reader, opening))
}

struct Scores {
    by_iid: BTreeMap<Iid, f64>,
    admitted: usize,
    limit: usize,
    overflowed: bool,
    allocation: Arc<MemoryReservation>,
}

struct ScoredIid {
    iid: Iid,
    distance: f64,
}

impl Ord for ScoredIid {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance.total_cmp(&other.distance).then_with(|| self.iid.cmp(&other.iid))
    }
}
impl PartialOrd for ScoredIid { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) } }
impl PartialEq for ScoredIid { fn eq(&self, other: &Self) -> bool { self.cmp(other).is_eq() } }
impl Eq for ScoredIid {}

impl Scores {
    fn new(memory: &Arc<MemoryBudget>, limit: usize) -> Result<Self> {
        Ok(Self { by_iid: BTreeMap::new(), admitted: 0, limit, overflowed: false, allocation: memory.reserve(0)? })
    }

    fn add(&mut self, iid: Iid, distance: f64) -> Result<()> {
        if let Some(best) = self.by_iid.get_mut(&iid) {
            if distance.total_cmp(best).is_lt() { *best = distance; }
            return Ok(());
        }
        if self.by_iid.len() == self.limit {
            self.overflowed = true;
            return Ok(());
        }
        if self.by_iid.len() == self.admitted {
            let admitted = self.admitted.saturating_add(256).min(self.limit);
            Arc::get_mut(&mut self.allocation).expect("Scores own their allocation")
                .grow_to(admitted.saturating_mul(128))?;
            self.admitted = admitted;
        }
        self.by_iid.insert(iid, distance);
        Ok(())
    }

    fn nearest(&self, target: usize, memory: &Arc<MemoryBudget>) -> Result<Option<(CandidateIids, bool)>> {
        if self.overflowed { return Ok(None); }
        let count = self.by_iid.len().min(target);
        let mut selection = OrderedIidBuffer::with_capacity(count, memory)?;
        let truncated = count < self.by_iid.len();
        if !truncated {
            for &iid in self.by_iid.keys() {
                selection.push(iid);
            }
        } else {
            let _heap = memory.reserve(count.saturating_mul(size_of::<ScoredIid>()))?;
            let mut best = BinaryHeap::with_capacity(count);
            for (&iid, &distance) in &self.by_iid {
                let candidate = ScoredIid { iid, distance };
                if best.len() < count {
                    best.push(candidate);
                } else if let Some(mut worst) = best.peek_mut() && candidate < *worst {
                    *worst = candidate;
                }
            }
            for score in best {
                selection.push(score.iid);
            }
        }
        Ok(Some((selection.into_sorted_membership()?, truncated)))
    }

}

/// A version can be visible only if its valid interval meets the read's and it was written by the read's system instant.
/// Resolution only narrows this, so it rules candidates out and never in.
fn may_be_visible(valid_from: i64, valid_to: i64, system_from: i64, bounds: TemporalBounds) -> bool {
    valid_from < bounds.valid_time.upper().0 && valid_to > bounds.valid_time.lower().0 && system_from < bounds.system_time.upper().0
}

/// The approximate route is optional: a refused budget or an unreadable generation hands the read to the exact route.
pub fn declined(error: &Anomaly) -> bool {
    error.category != AnomalyCategory::Interrupted && (error.is(AnomalyKind::ResourceExhausted)
        || matches!(error.code.as_str(), "rtdb.storage/missing-object" | "rtdb.vector/corrupt-generation"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rtdb_format::data::{StoredEvent, StoredOperation, build_event_batch};
    use rtdb_types::{Interval, LogicalType, ScalarType, StructFields, TableRef, Timestamp, Value};

    use super::*;
    use crate::definition::{Approximate, Method};

    #[test]
    fn cluster_fanout_leaves_room_for_range_and_decode_bytes() -> Result<()> {
        let snapshot = rtdb_index::PartitionValue::loaded(Default::default(), Default::default(), Default::default())?.snapshot();
        let storage = Arc::new(BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 0));
        let memory = MemoryBudget::new(65_536);
        let cancellation = CancellationToken::new();
        let point = Interval::at(Timestamp(10))?;
        let cut = Cut { snapshot: &snapshot, storage: &storage, memory: &memory, cancellation: &cancellation,
            bounds: TemporalBounds { valid_time: point, system_time: point } };
        let read = GenerationRead::Ranges(ObjectKey::new("generation").unwrap());
        let clusters = [
            Cluster { range: 0..4096, rows: 1, digest: [0; 32] },
            Cluster { range: 4096..8192, rows: 1, digest: [0; 32] },
        ];
        let needed = clusters[0].decode_bound() + clusters[0].read_len();
        assert_eq!(read_width(cut, &read, &clusters), 2);
        let held = memory.reserve(65_536 - needed)?;
        assert_eq!(read_width(cut, &read, &clusters), 1);
        drop(held);
        assert_eq!(read_width(cut, &read, &clusters), 2);
        Ok(())
    }

    #[test]
    fn directory_ranking_funds_only_the_selected_partitions_and_keeps_the_grants() {
        use crate::{file::write_generation, rows::IndexedRows};
        use rtdb_types::{EventHeader, EventOp};

        let mut rows = IndexedRows::new(2);
        let selected = Partition([1; 32]);
        for id in 0..20u8 {
            let partition = if id < 5 { selected } else { Partition([id; 32]) };
            let header = EventHeader {
                position: None, iid: Iid([id; 16]), system_from: Timestamp(0), commit_position: 0,
                valid_time: Interval::ALL, op: EventOp::Put,
            };
            rows.push_unit(header, partition, &[1.0, 0.0]);
        }
        let mut bytes = Vec::new();
        let decode_memory = MemoryBudget::new(1 << 20);
        let locator = write_generation(rows, decode_memory.reserve(0).unwrap(), &mut bytes, &decode_memory,
            &CancellationToken::new()).unwrap().locator;
        let range = locator.directory_range();
        let directory_bytes = Bytes::from(bytes).slice(range.start as usize..range.end as usize);
        let directory = Directory::read(directory_bytes, &locator, &decode_memory).unwrap();
        let expected = vec![directory.cluster(0).range];
        assert!(!expected.is_empty() && expected.len() < directory.len());
        let first = ObjectKey::new("ivf/generation-1").unwrap();
        let second = ObjectKey::new("ivf/generation-2").unwrap();
        let budget = MemoryBudget::new(1 << 20);
        let a = Opened::rank(GenerationRead::Ranges(first.clone()), &directory, &[1.0, 0.0], Some(selected), &budget).unwrap().unwrap();
        let b = Opened::rank(GenerationRead::Ranges(second), &directory, &[1.0, 0.0], Some(selected), &budget).unwrap().unwrap();
        assert_eq!(a.clusters.iter().map(|cluster| cluster.range.clone()).collect::<Vec<_>>(), expected);
        assert_eq!(budget.used_bytes(), a._allocation.bytes() + b._allocation.bytes());
        drop(a);
        assert_eq!(budget.used_bytes(), b._allocation.bytes());
        drop(b);
        assert_eq!(budget.used_bytes(), 0);

        let limited = MemoryBudget::new(1);
        let refused = Opened::rank(GenerationRead::Ranges(first), &directory, &[1.0, 0.0], Some(selected), &limited).err().unwrap();
        assert!(declined(&refused));
        assert_eq!(limited.used_bytes(), 0);
    }

    fn nearest_iids(scores: &Scores, target: usize) -> Option<(Vec<Iid>, bool)> {
        scores.nearest(target, scores.allocation.budget()).unwrap()
            .map(|(iids, truncated)| (iids.iter().collect(), truncated))
    }

    fn definition() -> VectorDefinition {
        VectorDefinition {
            name: "neighbors".into(), table: TableRef::new("public", "documents"), column: "embedding".into(),
            partition: vec!["tenant".into()], dimensions: 2, approximate: Approximate { method: Method::Ivf, recall: 0.9 }, generation: 0,
        }
    }

    fn put(id: u8, components: [f32; 2], tenant: Value) -> StoredEvent {
        StoredEvent {
            position: None, origin: None, entity_key: None, commit_position: 0, recency: None,
            iid: Iid([id; 16]), system_from: Timestamp(10), valid_time: Interval::ALL,
            operation: StoredOperation::Put(StructFields::from([
                ("embedding".into(), Value::list(components.into_iter().map(Value::F32).collect())),
                ("tenant".into(), tenant),
            ])),
        }
    }

    fn batch(events: &[StoredEvent], tenant: ScalarType) -> Result<EventBatch> {
        let fields = StructFields::from([
            ("embedding".into(), LogicalType::list(LogicalType::Scalar(ScalarType::F32))),
            ("tenant".into(), LogicalType::Scalar(tenant)),
        ]);
        EventBatch::new(build_event_batch(events, Some(fields), None)?)
    }

    fn frontier<'a>(definition: &'a VectorDefinition, scope: &'a BTreeSet<Iid>, partition: Option<Partition>, scores: &'a mut Scores) -> FrontierRanker<'a> {
        let point = Interval::at(Timestamp(10)).unwrap();
        FrontierRanker {
            definition, probe: &[1.0, 0.0], partition, scope: Some(IidScope::Set(scope)),
            bounds: TemporalBounds { valid_time: point, system_time: point }, scores,
        }
    }

    #[test]
    fn frontier_scores_the_admitted_rows_across_batches() -> Result<()> {
        let definition = definition();
        let scope = BTreeSet::from([Iid([1; 16]), Iid([2; 16]), Iid([3; 16]), Iid([4; 16]), Iid([5; 16])]);
        let tenant = |name: &str| Value::Utf8(name.into());
        let partition = Partition::of([Some(std::borrow::Cow::Owned(tenant("a")))])?;
        let memory = MemoryBudget::new(8192);
        let mut scores = Scores::new(&memory, 8)?;
        let mut ranker = frontier(&definition, &scope, Some(partition), &mut scores);
        let mut expired = put(3, [1.0, 0.0], tenant("a"));
        expired.valid_time = Interval::from_micros(0, 10)?;
        let mut future = put(4, [1.0, 0.0], tenant("a"));
        future.system_from = Timestamp(11);
        ranker.rank_batch(&batch(&[put(1, [3.0, 4.0], tenant("a")), expired, future], ScalarType::Utf8)?, &memory)?;
        ranker.rank_batch(&batch(&[
            put(2, [1.0, 0.0], tenant("a")), put(5, [1.0, 0.0], tenant("b")), put(6, [1.0, 0.0], tenant("a")),
        ], ScalarType::Utf8)?, &memory)?;
        assert_eq!(ranker.scores.by_iid, BTreeMap::from([(Iid([1; 16]), 1.0 - f64::from(0.6_f32)), (Iid([2; 16]), 0.0)]));
        Ok(())
    }

    #[test]
    fn frontier_reports_partition_errors_only_for_indexable_vectors_even_outside_the_scope() -> Result<()> {
        let definition = definition();
        let scope = BTreeSet::new();
        let memory = MemoryBudget::new(8192);
        let mut scores = Scores::new(&memory, 8)?;
        let mut ranker = frontier(&definition, &scope, None, &mut scores);
        ranker.rank_batch(&batch(&[put(1, [0.0, -0.0], Value::F64(1.0))], ScalarType::F64)?, &memory)?;
        let batch = batch(&[put(1, [1.0, 0.0], Value::F64(1.0))], ScalarType::F64)?;
        assert_eq!(ranker.rank_batch(&batch, &memory).unwrap_err().code, "rtdb.vector/partition-type");
        Ok(())
    }

    #[test]
    fn frontier_file_reads_plain_arrow_from_its_original_owner() -> Result<()> {
        use arrow_array::{ArrayRef, RecordBatch, StringArray};

        let input = RecordBatch::try_from_iter([(
            "text", Arc::new(StringArray::from(vec!["retained payload"])) as ArrayRef,
        )]).unwrap();
        let bytes = Bytes::from(rtdb_format::ipc::write_file(input.schema_ref(), std::slice::from_ref(&input))?);
        let start = bytes.as_ptr() as usize;
        let source = start..start + bytes.len();
        let memory = MemoryBudget::new(1 << 20);
        let (mut reader, _opening) = open_frontier_file(bytes, &memory)?;
        let _page = memory.reserve(reader.page_allocation_bound(0)?)?;
        let page = reader.read_page(0)?;
        let text = page.column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0);
        assert_eq!(text, "retained payload");
        assert!(source.contains(&(text.as_ptr() as usize)));
        Ok(())
    }

    #[tokio::test]
    async fn the_shortlist_widens_over_previously_scored_candidates() -> Result<()> {
        let iid = |byte| Iid([byte; 16]);
        let memory = MemoryBudget::new(1 << 20);
        let budget = Budget { max_candidates: 128, ..Budget::default() };
        let mut scores = Scores::new(&memory, budget.max_candidates)?;
        for byte in 1..=70 { scores.add(iid(byte), f64::from(byte))?; }
        scores.add(iid(64), 100.0)?;
        assert_eq!(scores.by_iid.len(), 70);
        assert_eq!(scores.by_iid[&iid(64)], 64.0);

        let snapshot = rtdb_index::PartitionValue::loaded(Default::default(), Default::default(), Default::default())?.snapshot();
        let storage = Arc::new(BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 0));
        let cancellation = CancellationToken::new();
        let point = Interval::at(Timestamp(10))?;
        let cut = Cut { snapshot: &snapshot, storage: &storage, memory: &memory, cancellation: &cancellation,
            bounds: TemporalBounds { valid_time: point, system_time: point } };
        let mut shortlist = Shortlist {
            probe: UnitProbe::from_slice(&[1.0, 0.0], &memory)?.unwrap(), plan: probe::Plan::new(0.9, 1), budget, partition: None, scores, opened: Vec::new(), _opened_allocation: memory.reserve(0)?,
        };
        let first = shortlist.round(cut, None).await?.unwrap();
        assert_eq!(first.candidates.iter().collect::<Vec<_>>(), (1..=64).map(iid).collect::<Vec<_>>());
        assert!(!first.complete);
        drop(first);

        assert!(shortlist.widen());
        let final_round = shortlist.round(cut, None).await?.unwrap();
        assert_eq!(final_round.candidates.iter().collect::<Vec<_>>(), (1..=70).map(iid).collect::<Vec<_>>());
        assert!(final_round.complete);
        assert!(!shortlist.widen());
        drop(shortlist);
        assert!(memory.used_bytes() > 0);
        assert_eq!(final_round.candidates.len(), 70);
        drop(final_round);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    #[test]
    fn the_truncated_shortlist_breaks_distance_ties_by_iid() -> Result<()> {
        let iid = |byte| Iid([byte; 16]);
        let memory = MemoryBudget::new(8192);
        let mut scores = Scores::new(&memory, 8)?;
        for (id, distance) in [(1, 2.0), (2, 1.0), (3, 1.0), (4, 1.0), (5, 0.0), (6, -0.0)] {
            scores.add(iid(id), distance)?;
        }
        assert_eq!(nearest_iids(&scores, 3), Some((vec![iid(2), iid(5), iid(6)], true)));
        assert_eq!(nearest_iids(&scores, 1), Some((vec![iid(6)], true)));
        Ok(())
    }

    #[test]
    fn score_budget_counts_distinct_iids_and_declines_an_incomplete_shortlist() -> Result<()> {
        let mut scores = Scores::new(&MemoryBudget::new(8192), 1)?;
        let first = Iid([1; 16]);
        scores.add(first, 2.0)?;
        scores.add(first, 1.0)?;
        assert_eq!(scores.by_iid[&first], 1.0);
        assert_eq!(nearest_iids(&scores, 1), Some((vec![first], false)));
        scores.add(Iid([2; 16]), 0.0)?;
        assert_eq!(scores.by_iid.len(), 1);
        assert!(nearest_iids(&scores, 1).is_none());
        Ok(())
    }
}
