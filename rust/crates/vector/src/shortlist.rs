use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::sync::Arc;

use arrow_buffer::Buffer;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{data::{EncodedEvent, EventBatch}, ipc::{ArrowFile, inspect_file_reserved}};
use rtdb_index::{CatalogTrie, Snapshot, TrieState};
use rtdb_query::IidScope;
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

/// One round of the shortlist: the candidates to resolve, and whether every eligible row has been considered.
pub struct Round { pub candidates: BTreeSet<Iid>, pub complete: bool }

/// An opened generation and its clusters eligible under the partition filter, nearest centroid first.
struct Opened { key: ObjectKey, clusters: Vec<Cluster>, order: Vec<usize>, probed: usize }

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
            let bytes = cut.storage.get_range_or(key, locator.directory_range(), cut.memory).await?;
            let directory = Directory::read(Buffer::from(Bytes::from_owner(bytes)), locator)?;
            if directory.dimensions != definition.dimensions { return Ok(None); }
            let mut order: Vec<(f64, usize)> = directory.clusters.iter().enumerate()
                .filter(|(_, cluster)| partition.is_none_or(|partition| cluster.partition == partition))
                .map(|(index, cluster)| (rows::unit_distance(&probe, directory.centroid(cluster)), index)).collect();
            order.sort_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
            opened.push(Opened { key: key.clone(), clusters: directory.clusters, order: order.into_iter().map(|(_, index)| index).collect(), probed: 0 });
        }
        Ok(Some(Self { plan: probe::Plan::new(definition.approximate.recall, limit), probe, budget, partition, scores, opened }))
    }

    /// Probes the clusters the current plan reaches and returns the nearest distinct IIDs, or `None` past the candidate budget.
    pub async fn round(&mut self, cut: Cut<'_>, scope: Option<IidScope<'_>>) -> Result<Option<Round>> {
        let bounds = cut.bounds;
        let dimensions = self.probe.len();
        for generation in &mut self.opened {
            let probes = self.plan.probes(generation.order.len());
            let start = generation.probed.min(probes);
            generation.probed = generation.probed.max(probes);
            let key = &generation.key;
            let clusters = &generation.clusters;
            let blocks = futures::stream::iter(generation.order[start..probes].iter().copied().map(|index| {
                let cluster = &clusters[index];
                async move {
                    let bytes = cut.storage.get_range_or(key, cluster.range.clone(), cut.memory).await?;
                    Block::read(Buffer::from(Bytes::from_owner(bytes)), cluster, dimensions)
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
        let complete = !truncated && self.opened.iter().all(|generation| generation.probed >= generation.order.len());
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
            let lease = cut.storage.get_or(data, cut.memory).await?;
            let (mut reader, _metadata) = open_frontier_file(Bytes::from_owner(lease), cut.memory)?;
            let columns = definition.put_columns();
            for page in 0..reader.page_count() {
                if cut.cancellation.is_cancelled() { return Err(crate::kmeans::cancelled()); }
                let _page = cut.memory.reserve(reader.page_allocation_bound(page)?)?;
                let events = EventBatch::new(reader.read_event_page(page, &columns)?)?;
                self.rank_batch(&events)?;
            }
        }
        for segment in cut.snapshot.iter_segments(&definition.table) {
            for batch in segment.batches() {
                self.rank_batch(batch.view())?;
            }
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
            // The grant covers the score tree, ordering scratch and one returned IID set.
            Arc::get_mut(&mut self.allocation).expect("Scores own their allocation")
                .grow_to(admitted.saturating_mul(192))?;
            self.admitted = admitted;
        }
        self.by_iid.insert(iid, distance);
        Ok(())
    }

    fn nearest(&self, target: usize) -> Option<(BTreeSet<Iid>, bool)> {
        if self.overflowed { return None; }
        if self.by_iid.len() <= target {
            return Some((self.by_iid.keys().copied().collect(), false));
        }
        let mut ranked = self.by_iid.iter().collect::<Vec<_>>();
        ranked.sort_unstable_by(|(left, d1), (right, d2)| d1.total_cmp(d2).then(left.cmp(right)));
        let candidates = ranked.into_iter().take(target).map(|(iid, _)| *iid).collect();
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
    use rtdb_format::data::{StoredEvent, StoredOperation, build_event_batch};
    use rtdb_types::{Interval, LogicalType, ScalarType, StructFields, TableRef, Timestamp, Value};

    use super::*;
    use crate::definition::{Approximate, Method};

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

    #[test]
    fn the_shortlist_widens_over_previously_scored_candidates() -> Result<()> {
        let iid = |byte| Iid([byte; 16]);
        let mut plan = probe::Plan::new(0.9, 1);
        let memory = MemoryBudget::new(1 << 20);
        let mut scores = Scores::new(&memory, 128)?;
        for byte in 1..=70 { scores.add(iid(byte), f64::from(byte))?; }
        scores.add(iid(64), 100.0)?;
        assert_eq!(scores.by_iid.len(), 70);
        assert_eq!(scores.by_iid[&iid(64)], 64.0);
        let (kept, truncated) = scores.nearest(plan.target()).unwrap();
        assert_eq!(kept.len(), 64);
        assert!(!kept.contains(&iid(65)));
        assert!(truncated);
        assert!(plan.widen(128));
        let (kept, truncated) = scores.nearest(plan.target()).unwrap();
        assert_eq!(kept.len(), 70);
        assert!(kept.contains(&iid(65)));
        assert!(!truncated);
        assert!(memory.used_bytes() > 0);
        drop(scores);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    #[test]
    fn score_budget_counts_distinct_iids_and_declines_an_incomplete_shortlist() -> Result<()> {
        let mut scores = Scores::new(&MemoryBudget::new(8192), 1)?;
        let first = Iid([1; 16]);
        scores.add(first, 2.0)?;
        scores.add(first, 1.0)?;
        assert_eq!(scores.by_iid[&first], 1.0);
        assert_eq!(scores.nearest(1), Some((BTreeSet::from([first]), false)));
        scores.add(Iid([2; 16]), 0.0)?;
        assert_eq!(scores.by_iid.len(), 1);
        assert!(scores.nearest(1).is_none());
        Ok(())
    }
}
