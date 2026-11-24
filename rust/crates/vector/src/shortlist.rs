use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use std::io::Cursor;
use std::sync::Arc;

use arrow_buffer::Buffer;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{data::{EncodedEvent, EventBatch}, ipc::{ArrowFile, inspect_file_reserved}};
use rtdb_index::{CatalogTrie, Snapshot, TrieState};
use rtdb_query::{IidScope, visit_projected_data_events};
use rtdb_storage::{ObjectKey, cache::BufferPool};
use rtdb_types::{Anomaly, AnomalyCategory, Iid, Result, TemporalBounds};
use tokio_util::sync::CancellationToken;

use crate::{
    definition::{self, VectorDefinition},
    file::{Block, Cluster, Directory, Locator},
    probe,
    rows::{self, Partition},
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
pub struct Round { pub candidates: Vec<Iid>, pub complete: bool }

/// An opened generation and its clusters eligible under the partition filter, nearest centroid first.
struct Opened {
    key: ObjectKey,
    clusters: Vec<Cluster>,
    probed: usize,
    _allocation: Arc<MemoryReservation>,
}

impl Opened {
    fn rank(key: &ObjectKey, directory: &Directory, probe: &[f32], partition: Option<Partition>, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        let eligible = || (0..directory.len()).filter(|&row| partition.is_none_or(|partition| directory.partition(row) == partition));
        let count = eligible().count();
        if count == 0 { return Ok(None); }
        let held = (2 * size_of::<Self>()).saturating_add(key.as_str().len()).saturating_add(count.saturating_mul(size_of::<Cluster>()));
        let bytes = held.saturating_add(count.saturating_mul(size_of::<(f64, usize)>()));
        let mut allocation = memory.reserve(bytes)?;
        let mut order = Vec::with_capacity(count);
        order.extend(eligible().map(|row| (rows::unit_distance(probe, directory.centroid(row)), row)));
        order.sort_unstable_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
        let mut clusters = Vec::with_capacity(count);
        clusters.extend(order.iter().map(|&(_, row)| directory.cluster(row)));
        drop(order);
        Arc::get_mut(&mut allocation).expect("directory ranking owns its reservation").shrink_to(held)?;
        Ok(Some(Self { key: key.clone(), clusters, probed: 0, _allocation: allocation }))
    }
}

/// The nearest candidates of a probe across a definition's published generations and the frontier above them.
/// Each round probes further clusters; the caller resolves the candidates at its cut and widens the shortlist
/// until enough of them pass its filter, every row has been considered, or the budget is spent.
pub struct Shortlist {
    probe: Vec<f32>,
    plan: probe::Plan,
    budget: Budget,
    partition: Option<Partition>,
    scores: Scores,
    opened: Vec<Opened>,
}

impl Shortlist {
    /// Opens the generations at the cut and ranks the frontier, or `None` where the frontier exceeds the budget,
    /// the cut is not a pair of points, or the generations disagree with the definition.
    pub async fn open(cut: Cut<'_>, definition: &VectorDefinition, probe: Vec<f32>, limit: usize, partition: Option<Partition>,
        scope: Option<IidScope<'_>>, budget: Budget) -> Result<Option<Self>> {
        let bounds = cut.bounds;
        if !(bounds.valid_time.is_point() && bounds.system_time.is_point()) || cut.snapshot.has_overlays() { return Ok(None); }
        if probe.len() != definition.dimensions { return Ok(None); }
        let Some(sources) = sources(cut, definition, budget) else { return Ok(None) };
        let mut scores = Scores::new(cut.memory, budget.max_candidates)?;
        FrontierRanker {
            definition, probe: &probe, partition, scope, bounds, unit: Vec::new(), scores: &mut scores,
        }.rank(cut, &sources).await?;
        if scores.overflowed { return Ok(None); }
        let mut opened = Vec::new();
        for source in &sources {
            let Source::Generation { key, locator } = source else { continue };
            let lease = cut.storage.get_range_or(key, locator.directory_range(), cut.memory).await?;
            let directory = Directory::read(lease, locator, cut.memory)?;
            if directory.dimensions != definition.dimensions { return Ok(None); }
            if let Some(generation) = Opened::rank(key, &directory, &probe, partition, cut.memory)? {
                opened.push(generation);
            }
        }
        Ok(Some(Self { plan: probe::Plan::new(definition.approximate.recall, limit), probe, budget, partition, scores, opened }))
    }

    /// Probes the clusters the current plan reaches and returns the nearest distinct IIDs, or `None` past the candidate budget.
    pub async fn round(&mut self, cut: Cut<'_>, scope: Option<IidScope<'_>>) -> Result<Option<Round>> {
        let bounds = cut.bounds;
        let dimensions = self.probe.len();
        for generation in &mut self.opened {
            let probes = self.plan.probes(generation.clusters.len());
            let start = generation.probed.min(probes);
            generation.probed = generation.probed.max(probes);
            let key = &generation.key;
            let blocks = futures::stream::iter(generation.clusters[start..probes].iter().map(|cluster| {
                async move {
                    let lease = cut.storage.get_range_or(key, cluster.range.clone(), cut.memory).await?;
                    Block::read(lease, cluster, dimensions, cut.memory)
                }
            })).buffer_unordered(8);
            futures::pin_mut!(blocks);
            while let Some(block) = blocks.try_next().await? {
                for row in 0..block.len() {
                    if row % 4096 == 0 && cut.cancellation.is_cancelled() { return Err(crate::kmeans::cancelled()); }
                    let iid = block.iid(row)?;
                    let (valid_from, valid_to, system_from) = block.extent(row);
                    if scope.is_some_and(|scope| !scope.contains(&iid)) || !may_be_visible(valid_from, valid_to, system_from, bounds) { continue; }
                    self.scores.add(iid, rows::unit_distance(&self.probe, block.unit(row)))?;
                }
            }
        }
        let Some((candidates, truncated)) = self.scores.nearest(self.plan.target()) else { return Ok(None) };
        let complete = !truncated && self.opened.iter().all(|generation| generation.probed >= generation.clusters.len());
        Ok(Some(Round { candidates, complete }))
    }

    /// Doubles the probe fraction and the shortlist; false once the shortlist would exceed the candidate budget.
    pub fn widen(&mut self) -> bool { self.plan.widen(self.budget.max_candidates) }

    pub fn partition(&self) -> Option<Partition> { self.partition }
}

/// A source of candidates at the cut.
enum Source { Generation { key: ObjectKey, locator: Locator }, Frontier { data: ObjectKey } }

/// The cut's sources, or `None` where the frontier is too large to rank exactly within budget.
fn sources(cut: Cut<'_>, definition: &VectorDefinition, budget: Budget) -> Option<Vec<Source>> {
    let table = &definition.table;
    let entry = cut.snapshot.table_entry(table)?;
    let earliest = cut.bounds.valid_time.lower().0.min(cut.bounds.system_time.lower().0);
    let visible = |trie: &&CatalogTrie| trie.key.recency().is_none_or(|date| date.epoch_day().saturating_mul(86_400_000_000) > earliest);
    let mut uncovered = 0i64;
    let mut sources = Vec::new();
    for trie in cut.snapshot.trie_catalog().current_tries(table).into_iter().filter(|trie| trie.state == TrieState::Live).filter(visible) {
        let recorded = trie.artefact(definition::KIND, &definition.artefact_id(), definition.generation)
            .and_then(|record| Some((record, Locator::decode(record.stats.as_deref()?)?)))
            .filter(|(_, locator)| locator.dimensions as usize == definition.dimensions)
            .and_then(|(record, locator)| Some((ObjectKey::new(entry.slug.recorded_artefact_key(&trie.key, record).ok()?).ok()?, locator)));
        match recorded {
            Some((key, locator)) => sources.push(Source::Generation { key, locator }),
            None => {
                uncovered = uncovered.saturating_add(trie.data_file_size);
                sources.push(Source::Frontier { data: ObjectKey::new(entry.slug.data_key(&trie.key)).ok()? });
            }
        }
    }
    (uncovered <= budget.max_uncovered_bytes).then_some(sources)
}

struct FrontierRanker<'a> {
    definition: &'a VectorDefinition,
    probe: &'a [f32],
    partition: Option<Partition>,
    scope: Option<IidScope<'a>>,
    bounds: TemporalBounds,
    unit: Vec<f32>,
    scores: &'a mut Scores,
}

impl FrontierRanker<'_> {
    async fn rank(mut self, cut: Cut<'_>, sources: &[Source]) -> Result<()> {
        let definition = self.definition;
        for source in sources {
            let Source::Frontier { data } = source else { continue };
            let columns = definition.put_columns();
            self.rank_file(cut, data, &columns).await?;
        }
        for segment in cut.snapshot.iter_segments(&definition.table) {
            for batch in segment.batches() {
                self.rank_batch(batch.view())?;
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
                |events| self.rank_batch(events));
            return cut.cancellation.run_until_cancelled(visit).await.ok_or_else(crate::kmeans::cancelled)?;
        }

        let lease = cut.storage.get_or(data, cut.memory).await?;
        let (mut reader, _metadata) = open_frontier_file(Bytes::from_owner(lease), cut.memory)?;
        for page in 0..reader.page_count() {
            check_cancelled()?;
            let _page = cut.memory.reserve(reader.page_allocation_bound(page)?)?;
            let events = EventBatch::new(reader.read_event_page(page, columns)?)?;
            self.rank_batch(&events)?;
        }
        Ok(())
    }

    fn rank_batch(&mut self, events: &EventBatch) -> Result<()> {
        let dimensions = self.definition.dimensions;
        for row in 0..events.len() {
            let event = events.borrow_event(row)?;
            self.unit.clear();
            // A put outside the requested cut can still fail vector or partition decoding.
            let Some(partition) = rows::append_indexable_event_unit(&event, self.definition, dimensions, &mut self.unit)? else { continue };
            let header = event.header();
            if self.partition.is_none_or(|expected| partition == expected)
                && self.scope.is_none_or(|scope| scope.contains(&header.iid))
                && may_be_visible(header.valid_time.lower().0, header.valid_time.upper().0, header.system_from.0, self.bounds)
            {
                self.scores.add(header.iid, rows::unit_distance(self.probe, &self.unit))?;
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
            // The grant covers the score tree, the selection heap and one IID round, including the held projection's set.
            Arc::get_mut(&mut self.allocation).expect("Scores own their allocation")
                .grow_to(admitted.saturating_mul(192))?;
            self.admitted = admitted;
        }
        self.by_iid.insert(iid, distance);
        Ok(())
    }

    fn nearest(&self, target: usize) -> Option<(Vec<Iid>, bool)> {
        if self.overflowed { return None; }
        if self.by_iid.len() <= target {
            return Some((self.by_iid.keys().copied().collect(), false));
        }
        let mut scores = self.by_iid.iter().map(|(&iid, &distance)| ScoredIid { iid, distance });
        let mut nearest = BinaryHeap::from(scores.by_ref().take(target).collect::<Vec<_>>());
        for candidate in scores {
            if let Some(mut worst) = nearest.peek_mut() && candidate < *worst {
                *worst = candidate;
            }
        }
        let mut candidates: Vec<_> = nearest.into_iter().map(|score| score.iid).collect();
        candidates.sort_unstable();
        Some((candidates, true))
    }
}

/// A version can be visible only if its valid interval meets the read's and it was written by the read's system instant.
/// Resolution only narrows this, so it rules candidates out and never in.
fn may_be_visible(valid_from: i64, valid_to: i64, system_from: i64, bounds: TemporalBounds) -> bool {
    valid_from < bounds.valid_time.upper().0 && valid_to > bounds.valid_time.lower().0 && system_from < bounds.system_time.upper().0
}

/// The approximate route is optional: a refused budget or an unreadable generation hands the read to the exact route.
pub fn declined(error: &Anomaly) -> bool {
    error.category != AnomalyCategory::Interrupted && matches!(error.code.as_str(),
        "rtdb/memory-budget-exceeded" | "rtdb.storage/memory-limit" | "rtdb.storage/missing-object" | "rtdb.vector/corrupt-generation")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rtdb_format::data::{StoredEvent, StoredOperation, build_event_batch};
    use rtdb_types::{Interval, LogicalType, ScalarType, StructFields, TableRef, Timestamp, Value};

    use super::*;
    use crate::definition::{Approximate, Method};

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
        let locator = write_generation(rows, &mut bytes, &CancellationToken::new()).unwrap().locator;
        let range = locator.directory_range();
        let decode_memory = MemoryBudget::new(1 << 20);
        let directory_bytes = Bytes::from(bytes).slice(range.start as usize..range.end as usize);
        let directory = Directory::read(directory_bytes, &locator, &decode_memory).unwrap();
        let expected: Vec<_> = (0..directory.len()).filter(|&row| directory.partition(row) == selected)
            .map(|row| directory.cluster(row).range).collect();
        assert!(!expected.is_empty() && expected.len() < directory.len());
        let first = ObjectKey::new("ivf/generation-1").unwrap();
        let second = ObjectKey::new("ivf/generation-2").unwrap();
        let budget = MemoryBudget::new(1 << 20);
        let a = Opened::rank(&first, &directory, &[1.0, 0.0], Some(selected), &budget).unwrap().unwrap();
        let b = Opened::rank(&second, &directory, &[1.0, 0.0], Some(selected), &budget).unwrap().unwrap();
        assert_eq!(a.clusters.iter().map(|cluster| cluster.range.clone()).collect::<Vec<_>>(), expected);
        assert_eq!(budget.used_bytes(), a._allocation.bytes() + b._allocation.bytes());
        drop(a);
        assert_eq!(budget.used_bytes(), b._allocation.bytes());
        drop(b);
        assert_eq!(budget.used_bytes(), 0);

        let limited = MemoryBudget::new(1);
        let refused = Opened::rank(&first, &directory, &[1.0, 0.0], Some(selected), &limited).err().unwrap();
        assert!(declined(&refused));
        assert_eq!(limited.used_bytes(), 0);
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
            bounds: TemporalBounds { valid_time: point, system_time: point }, unit: Vec::new(), scores,
        }
    }

    #[test]
    fn frontier_scores_the_admitted_rows_across_batches() -> Result<()> {
        let definition = definition();
        let scope = BTreeSet::from([Iid([1; 16]), Iid([2; 16]), Iid([3; 16]), Iid([4; 16]), Iid([5; 16])]);
        let tenant = |name: &str| Value::Utf8(name.into());
        let partition = Partition::of([Some(std::borrow::Cow::Owned(tenant("a")))])?;
        let mut scores = Scores::new(&MemoryBudget::new(8192), 8)?;
        let mut ranker = frontier(&definition, &scope, Some(partition), &mut scores);
        let mut expired = put(3, [1.0, 0.0], tenant("a"));
        expired.valid_time = Interval::from_micros(0, 10)?;
        let mut future = put(4, [1.0, 0.0], tenant("a"));
        future.system_from = Timestamp(11);
        ranker.rank_batch(&batch(&[put(1, [3.0, 4.0], tenant("a")), expired, future], ScalarType::Utf8)?)?;
        ranker.rank_batch(&batch(&[
            put(2, [1.0, 0.0], tenant("a")), put(5, [1.0, 0.0], tenant("b")), put(6, [1.0, 0.0], tenant("a")),
        ], ScalarType::Utf8)?)?;
        assert_eq!(ranker.scores.by_iid, BTreeMap::from([(Iid([1; 16]), 1.0 - f64::from(0.6_f32)), (Iid([2; 16]), 0.0)]));
        Ok(())
    }

    #[test]
    fn frontier_reports_partition_errors_only_for_indexable_vectors_even_outside_the_scope() -> Result<()> {
        let definition = definition();
        let scope = BTreeSet::new();
        let mut scores = Scores::new(&MemoryBudget::new(8192), 8)?;
        let mut ranker = frontier(&definition, &scope, None, &mut scores);
        ranker.rank_batch(&batch(&[put(1, [0.0, -0.0], Value::F64(1.0))], ScalarType::F64)?)?;
        let batch = batch(&[put(1, [1.0, 0.0], Value::F64(1.0))], ScalarType::F64)?;
        assert_eq!(ranker.rank_batch(&batch).unwrap_err().code, "rtdb.vector/partition-type");
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
            probe: vec![1.0, 0.0], plan: probe::Plan::new(0.9, 1), budget, partition: None, scores, opened: Vec::new(),
        };
        let first = shortlist.round(cut, None).await?.unwrap();
        assert_eq!(first.candidates, (1..=64).map(iid).collect::<Vec<_>>());
        assert!(!first.complete);
        drop(first);

        assert!(shortlist.widen());
        let final_round = shortlist.round(cut, None).await?.unwrap();
        assert_eq!(final_round.candidates, (1..=70).map(iid).collect::<Vec<_>>());
        assert!(final_round.complete);
        assert!(!shortlist.widen());
        drop(final_round);
        assert!(memory.used_bytes() > 0);
        drop(shortlist);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    #[test]
    fn the_truncated_shortlist_breaks_distance_ties_by_iid() -> Result<()> {
        let iid = |byte| Iid([byte; 16]);
        let mut scores = Scores::new(&MemoryBudget::new(8192), 8)?;
        for (id, distance) in [(1, 2.0), (2, 1.0), (3, 1.0), (4, 1.0), (5, 0.0), (6, -0.0)] {
            scores.add(iid(id), distance)?;
        }
        assert_eq!(scores.nearest(3), Some((vec![iid(2), iid(5), iid(6)], true)));
        assert_eq!(scores.nearest(1), Some((vec![iid(6)], true)));
        Ok(())
    }

    #[test]
    fn score_budget_counts_distinct_iids_and_declines_an_incomplete_shortlist() -> Result<()> {
        let mut scores = Scores::new(&MemoryBudget::new(8192), 1)?;
        let first = Iid([1; 16]);
        scores.add(first, 2.0)?;
        scores.add(first, 1.0)?;
        assert_eq!(scores.by_iid[&first], 1.0);
        assert_eq!(scores.nearest(1), Some((vec![first], false)));
        scores.add(Iid([2; 16]), 0.0)?;
        assert_eq!(scores.by_iid.len(), 1);
        assert!(scores.nearest(1).is_none());
        Ok(())
    }
}
