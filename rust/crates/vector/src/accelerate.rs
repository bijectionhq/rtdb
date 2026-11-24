use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use rtdb_index::{CatalogTrie, TrieState};
use rtdb_plan::{BinaryOp, Expr};
use rtdb_query::{CandidateIids, IidScope, ScanAccelerator, ScanRequest, ScanSelection};
use rtdb_storage::ObjectKey;
use rtdb_types::{AnomalyCategory, Iid, Result, TemporalBounds, Value};

use crate::{
    definition::{self, Approximate, VectorDefinition},
    file::{Block, Directory, Locator},
    lane, probe,
    rows::{self, IndexedRow, Partition},
};

/// The opt-in predicate a scan carries to take the approximate route: `APPROXIMATE_NEAREST(column, probe, limit)`.
/// Evaluated on a row it holds where the column is not null, so a scan no accelerator narrows ranks exactly.
pub const PREDICATE: &str = "approximate_nearest";

/// Narrows a nearest-neighbour scan to the candidates of an approximate index's published generations (vector.allium: ApproximateRead).
/// Generations cover the current tries they were built on; the scan's other tries and its live segments are its frontier and are ranked exactly.
/// Every candidate is resolved and filtered at the scan's cut and reranked on its full-precision vector by the query, so erased,
/// deleted and superseded versions never reach a result.
/// The scan takes the exact route whenever this declines: no definition, a temporal range, an IID scope small enough to rank exactly,
/// a frontier above `max_uncovered_bytes`, or more than `max_candidates` before enough rows pass the filter.
pub struct VectorAccelerator {
    pub max_uncovered_bytes: i64,
    pub max_candidates: usize,
    pub exact_scope: usize,
}

impl Default for VectorAccelerator {
    fn default() -> Self { Self { max_uncovered_bytes: 64 << 20, max_candidates: 65_536, exact_scope: 4_096 } }
}

/// What the scan asked of the approximate route.
struct Asked { definition: VectorDefinition, probe: Vec<f32>, limit: usize }

/// A source of candidates at the scan's cut.
enum Source { Generation { key: ObjectKey, locator: Locator }, Frontier { data: ObjectKey } }

/// An opened generation and the clusters eligible under the partition filter, nearest centroid first.
struct Opened { key: ObjectKey, directory: Directory, order: Vec<usize>, probed: usize }

#[async_trait]
impl ScanAccelerator for VectorAccelerator {
    fn accepts_arrow_iids(&self) -> bool { true }

    async fn candidate_iids(&self, request: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>> {
        match self.narrow(request).await {
            Err(error) if declined(&error) => Ok(None),
            result => result,
        }
    }

    async fn select_candidates(&self, request: &ScanRequest<'_>) -> Result<Option<ScanSelection>> {
        let Some(iids) = self.candidate_iids(request).await? else { return Ok(None) };
        match CandidateIids::from_set(iids, request.memory) {
            Ok(iids) => Ok(Some(ScanSelection { iids, scores: None })),
            Err(error) if error.code == "rtdb/memory-budget-exceeded" => Ok(None),
            Err(error) => Err(error),
        }
    }
}

impl VectorAccelerator {
    async fn narrow(&self, request: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>> {
        let bounds = request.bounds;
        if !(bounds.valid_time.is_point() && bounds.system_time.is_point()) || request.snapshot.has_overlays() { return Ok(None); }
        let Some(asked) = self.asked(request).await? else { return Ok(None) };
        let Approximate { recall, .. } = asked.definition.approximate;
        let scope = request.iid_scope();
        if scope.is_some_and(|scope| scope.len() <= self.exact_scope.max(asked.limit)) { return Ok(None); }
        let partition = partition_filter(request, &asked.definition).await?;
        let Some(sources) = self.sources(request, &asked.definition) else { return Ok(None) };

        let mut frontier = Vec::new();
        for row in self.frontier_rows(request, &asked.definition, &sources).await? {
            if admits(&row, partition, scope, bounds) { frontier.push((rows::unit_distance(&asked.probe, &row.unit), row.iid)); }
        }
        let mut opened = Vec::new();
        for source in &sources {
            let Source::Generation { key, locator } = source else { continue };
            let bytes = request.storage.get_range_or(key, locator.directory_range(), request.memory).await?;
            let directory = Directory::read(bytes.as_slice(), locator)?;
            if directory.dimensions != asked.definition.dimensions { return Ok(None); }
            let mut order: Vec<(f64, usize)> = directory.clusters.iter().enumerate()
                .filter(|(_, cluster)| partition.is_none_or(|partition| cluster.partition == partition))
                .map(|(index, cluster)| (rows::unit_distance(&asked.probe, directory.centroid(cluster)), index)).collect();
            order.sort_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
            opened.push(Opened { key: key.clone(), directory, order: order.into_iter().map(|(_, index)| index).collect(), probed: 0 });
        }

        let mut plan = probe::Plan::new(recall, asked.limit);
        let mut gathered = frontier;
        loop {
            for generation in &mut opened {
                let probes = plan.probes(generation.order.len());
                let clusters: Vec<usize> = generation.order[generation.probed.min(probes)..probes].to_vec();
                generation.probed = generation.probed.max(probes);
                let blocks: Vec<Block> = futures::stream::iter(clusters.into_iter().map(|index| {
                    let cluster = &generation.directory.clusters[index];
                    let (key, dimensions) = (&generation.key, generation.directory.dimensions);
                    async move {
                        let bytes = request.storage.get_range_or(key, cluster.range.clone(), request.memory).await?;
                        Block::read(bytes.as_slice(), cluster, dimensions)
                    }
                })).buffer_unordered(8).try_collect().await?;
                for block in blocks {
                    for row in 0..block.len() {
                        let iid = block.iid(row)?;
                        let (valid_from, valid_to, system_from) = block.extent(row);
                        if scope.is_some_and(|scope| !scope.contains(&iid)) || !may_be_visible(valid_from, valid_to, system_from, bounds) { continue; }
                        gathered.push((rows::unit_distance(&asked.probe, block.unit(row)), iid));
                    }
                }
            }
            let (candidates, truncated) = nearest(&mut gathered, plan.target());
            if candidates.len() > self.max_candidates { return Ok(None); }
            let exhausted = !truncated && opened.iter().all(|generation| generation.probed >= generation.order.len());
            let Some(visible) = request.verifier.retain_visible(candidates.clone()).await? else { return Ok(Some(candidates)) };
            if visible.len() >= asked.limit || exhausted { return Ok(Some(visible)); }
            // Too few candidates passed the filter: widen the probe and the shortlist until the filter is satisfied or the budget is spent.
            if !plan.widen(self.max_candidates) { return Ok(None); }
        }
    }

    async fn asked(&self, request: &ScanRequest<'_>) -> Result<Option<Asked>> {
        let scan = request.scan;
        let mut found = None;
        for predicate in &scan.predicates { conjuncts(predicate, &mut |expression| {
            if found.is_none() && let Expr::Call { name, args, .. } = expression
                && name.eq_ignore_ascii_case(PREDICATE) && let [Expr::Column { id, .. }, probe, limit] = args.as_slice()
                && let Some(field) = scan.columns.iter().find(|field| field.id == *id) {
                found = Some((field.name.clone(), probe.clone(), limit.clone()));
            }
        }); }
        let Some((column, probe, limit)) = found else { return Ok(None) };
        let (Some(probe), Some(limit)) = (request.constants.constant(&probe).await?, request.constants.constant(&limit).await?) else { return Ok(None) };
        let Some(limit) = row_count(&limit) else { return Ok(None) };
        let table = &scan.table.name.table;
        let definitions = definition::definitions_in(request.database, request.partitions.to_vec(), Arc::clone(request.storage),
            Arc::clone(request.memory), request.current_time, request.cancellation).await?;
        let Some(definition) = definitions.into_iter().find(|definition| &definition.table == table && definition.column == column) else { return Ok(None) };
        let Some(probe) = rows::value_unit(&probe, definition.dimensions) else { return Ok(None) };
        Ok(Some(Asked { definition, probe, limit }))
    }

    /// The scan's sources at its cut, or `None` where the frontier is too large to rank exactly within budget.
    fn sources(&self, request: &ScanRequest<'_>, definition: &VectorDefinition) -> Option<Vec<Source>> {
        let table = &definition.table;
        let entry = request.snapshot.table_entry(table)?;
        let earliest = request.bounds.valid_time.lower().0.min(request.bounds.system_time.lower().0);
        let visible = |trie: &&CatalogTrie| trie.key.recency().is_none_or(|date| date.epoch_day().saturating_mul(86_400_000_000) > earliest);
        let mut uncovered = 0i64;
        let mut sources = Vec::new();
        for trie in request.snapshot.trie_catalog().current_tries(table).into_iter().filter(|trie| trie.state == TrieState::Live).filter(visible) {
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
        (uncovered <= self.max_uncovered_bytes).then_some(sources)
    }

    /// The indexable puts above the published frontier: tries without a generation, then the live segments.
    async fn frontier_rows(&self, request: &ScanRequest<'_>, definition: &VectorDefinition, sources: &[Source]) -> Result<Vec<IndexedRow>> {
        let mut output = Vec::new();
        for source in sources {
            let Source::Frontier { data } = source else { continue };
            let lease = request.storage.get_or(data, request.memory).await?;
            output.extend(lane::indexed_rows(std::io::Cursor::new(lease), definition, request.memory, request.cancellation)?);
        }
        for segment in request.snapshot.iter_segments(&definition.table) {
            for batch in segment.batches() {
                let events = batch.view();
                for row in 0..events.len() {
                    if let Some(indexed) = rows::indexed_row(&events.borrow_event(row)?, definition)? { output.push(indexed); }
                }
            }
        }
        Ok(output)
    }
}

/// The `target` nearest distinct IIDs of the gathered rows, which this leaves sorted and truncated to them,
/// and whether it left any distinct IID out.
fn nearest(gathered: &mut Vec<(f64, Iid)>, target: usize) -> (BTreeSet<Iid>, bool) {
    gathered.sort_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
    let mut seen = BTreeSet::new();
    let mut truncated = false;
    gathered.retain(|(_, iid)| {
        if seen.contains(iid) { return false; }
        if seen.len() == target { truncated = true; return false; }
        seen.insert(*iid)
    });
    (seen, truncated)
}

fn admits(row: &IndexedRow, partition: Option<Partition>, scope: Option<IidScope<'_>>, bounds: TemporalBounds) -> bool {
    partition.is_none_or(|partition| row.partition == partition)
        && scope.is_none_or(|scope| scope.contains(&row.iid))
        && may_be_visible(row.valid_from, row.valid_to, row.system_from, bounds)
}

/// A version can be visible only if its valid interval meets the read's and it was written by the read's system instant.
/// Resolution only narrows this, so it rules candidates out and never in.
fn may_be_visible(valid_from: i64, valid_to: i64, system_from: i64, bounds: TemporalBounds) -> bool {
    valid_from < bounds.valid_time.upper().0 && valid_to > bounds.valid_time.lower().0 && system_from < bounds.system_time.upper().0
}

/// The partition every eligible row shares, where the scan fixes each partition column by equality to a constant.
async fn partition_filter(request: &ScanRequest<'_>, definition: &VectorDefinition) -> Result<Option<Partition>> {
    if definition.partition.is_empty() { return Ok(Some(Partition::whole())); }
    let scan = request.scan;
    let mut equalities = Vec::new();
    for predicate in &scan.predicates { conjuncts(predicate, &mut |expression| {
        if let Expr::Binary { op: BinaryOp::Eq, left, right } = expression {
            let column = |side: &Expr| match side {
                Expr::Column { id, .. } => scan.columns.iter().find(|field| field.id == *id).map(|field| field.name.clone()),
                _ => None,
            };
            if let Some(name) = column(left) { equalities.push((name, (**right).clone())); }
            else if let Some(name) = column(right) { equalities.push((name, (**left).clone())); }
        }
    }); }
    let mut fixed = BTreeMap::new();
    for (name, expression) in equalities {
        if !definition.partition.contains(&name) || fixed.contains_key(&name) { continue; }
        if let Some(value) = request.constants.constant(&expression).await? { fixed.insert(name, value); }
    }
    let mut parts = Vec::with_capacity(definition.partition.len());
    for column in &definition.partition {
        let Some(value) = fixed.remove(column) else { return Ok(None) };
        if matches!(value, Value::Null | Value::Undefined) { return Ok(None); }
        parts.push(Some(std::borrow::Cow::Owned(value)));
    }
    Partition::of(parts).map(Some).or_else(|error| if error.code == "rtdb.vector/partition-type" { Ok(None) } else { Err(error) })
}

fn conjuncts<'a>(expression: &'a Expr, visit: &mut impl FnMut(&'a Expr)) {
    match expression {
        Expr::Binary { op: BinaryOp::And, left, right } => { conjuncts(left, visit); conjuncts(right, visit); }
        expression => visit(expression),
    }
}

fn row_count(value: &Value) -> Option<usize> {
    let count = match value {
        Value::I8(value) => i64::from(*value), Value::I16(value) => i64::from(*value), Value::I32(value) => i64::from(*value),
        Value::I64(value) => *value, Value::U8(value) => i64::from(*value), Value::U16(value) => i64::from(*value),
        Value::U32(value) => i64::from(*value), Value::U64(value) => i64::try_from(*value).ok()?,
        _ => return None,
    };
    usize::try_from(count).ok().filter(|count| *count > 0)
}

/// The approximate route is optional: a refused budget or an unreadable generation leaves the scan exact.
fn declined(error: &rtdb_types::Anomaly) -> bool {
    error.category != AnomalyCategory::Interrupted && (matches!(error.code.as_str(),
        "rtdb/memory-budget-exceeded" | "rtdb.storage/memory-limit" | "rtdb.storage/missing-object" | "rtdb.vector/corrupt-generation"))
}
