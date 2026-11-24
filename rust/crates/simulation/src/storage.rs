//! A deterministic simulation of a cell's storage lifecycle: deployments writing through their real store stacks, releasing, forking (now and at a past block), being reaped, deleted and moved between cells.
//!
//! Driven for real, against `rtdb_storage::MemoryStore` buckets: `store::open_deployment_store` (prefixed, referencing when forked, packing, garbage), `reference::fork`, `reference::written_at_ms`, `store::reaper_pass`, `reference::simulate_erase_deployment` and `reference::materialize`.
//! Modelled: what each deployment should read, and the admin sequences around the real calls (a fork flushes its parent first; a fork at a past block is refused once the block is as old as the reaper's grace; a move fences the holder, losing what it held back).
//! Time is a simulated clock, which the stores stamp releases and packs from through the real `store::stamps`, so a run is a pure function of its seed.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Range;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use rtdb_directory::{Checkpoint, DeploymentRecord, Lifecycle};
use rtdb_format::keys::{TrieKey, block_key, lex_hex};
use rtdb_multi::packing::PackDiscovery;
use rtdb_multi::reference;
use rtdb_multi::store::{self, Clock, GarbageStore};
use rtdb_storage::{Error as StoreError, MemoryStore, ObjectKey, ObjectStore, object_store::StoredObject};
use rtdb_types::BlockIndex;

use crate::Violation;
use crate::rng::Rng;

const CELLS: usize = 2;
const VERSION: &str = "v06";
/// The reaper's grace, in simulated milliseconds; a step advances the clock by up to two.
const GRACE_MS: i64 = 40;
const MAXIMUM_LIVE: usize = 6;
const TABLES: usize = 3;
const TRACE_LENGTH: usize = 200;
const FULL_CHECK_EVERY: usize = 20;

/// A deliberately broken variant, to show the checker finds what it is meant to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bug {
    /// The reaper erases a released object without first copying it into the forks that still reference it.
    ReaperSkipsCopy,
    /// A deletion erases a deployment's objects without first copying them into its forks.
    DeletionSkipsCopy,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub seed: u64,
    pub steps: usize,
    pub bug: Option<Bug>,
}

impl Config {
    pub fn new(seed: u64, steps: usize) -> Self { Self { seed, steps, bug: None } }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Summary {
    pub seed: u64,
    pub steps: usize,
    pub writes: usize,
    pub flushes: usize,
    pub compactions: usize,
    pub releases: usize,
    pub forks: usize,
    pub past_forks: usize,
    pub reaper_passes: usize,
    pub failed_pack_reads: usize,
    pub cleaned_moves: usize,
    /// Objects and garbage records the reaper erased.
    pub erased: usize,
    pub deletions: usize,
    pub moves: usize,
    pub restarts: usize,
    /// Objects a cell move left at the deployment's prefix in the cell it moved from.
    pub left_behind: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State { Live, Deleted }

/// A durable write in a deployment's view, inherited or its own, `at` the number of its blocks when it was written: it is in block `b`'s state when `at <= b`.
#[derive(Clone)]
struct Write { name: String, at: usize }

struct Deployment {
    record: DeploymentRecord,
    cell: usize,
    /// Every cell it has been in: a move never returns to one, whose directory keeps its tombstone.
    cells: BTreeMap<usize, Option<i64>>,
    state: State,
    /// Each block's digest, inherited and its own; what a block writes is a function of the digests up to it.
    chain: Vec<u64>,
    /// What it read right after each block was written.
    snapshots: Vec<BTreeMap<String, Bytes>>,
    history: Vec<Write>,
    /// What its holder holds back for the next block.
    pending: BTreeMap<String, Bytes>,
    /// Every durable object it wrote or inherited and has not released.
    readable: BTreeMap<String, Bytes>,
    /// What a fork at a past block may see beyond that block's state: its parent's later compaction outputs of blocks up to it, which are the same bytes the fork would write.
    optional: BTreeMap<String, Bytes>,
    /// Every name it has had, which it never writes again.
    known: BTreeSet<String>,
    /// Objects an ancestor wrote after this deployment's fork point, which it must never read.
    forbidden: BTreeSet<String>,
    children: Vec<usize>,
    holder: Option<Arc<GarbageStore>>,
}

impl Deployment {
    fn store(&self) -> String { format!("{}/{VERSION}", self.record.storage_prefix()) }
    fn live(&self) -> bool { self.state == State::Live }
}

type Step = Result<(), String>;

fn mix(left: u64, right: u64) -> u64 {
    let mut z = left ^ right.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn hash(text: &str) -> u64 { text.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01B3)) }

fn content(name: &str, digest: u64) -> Bytes { Bytes::from(format!("{name}#{digest:016x}")) }

fn object(name: &str) -> Result<ObjectKey, String> { ObjectKey::new(name).map_err(|error| error.to_string()) }

/// The block a trie file's name ends in.
fn trie_block(name: &str) -> Option<i64> {
    let stem = name.strip_prefix("tables/")?.rsplit('/').next()?.strip_suffix(".arrow")?;
    TrieKey::from_str(stem).ok().map(|key| key.block_index().0)
}

struct PackReadFailure<'a> {
    bucket: &'a dyn ObjectStore,
    fired: AtomicBool,
}

const PACK_READ_FAILURE: &str = "injected pack range-read failure";

#[async_trait::async_trait]
impl ObjectStore for PackReadFailure<'_> {
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.bucket.head(key).await }
    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.bucket.get(key).await }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
        if key.as_str().contains("/~packs/") && !self.fired.swap(true, Ordering::Relaxed) {
            return Err(StoreError::Task(PACK_READ_FAILURE.into()));
        }
        self.bucket.get_range(key, range).await
    }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> { self.bucket.put(key, bytes).await }
    async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> { self.bucket.put_file(key, source).await }
    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> { self.bucket.list(directory).await }
    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> { self.bucket.delete(key).await }
}

struct World {
    config: Config,
    rng: Rng,
    step: usize,
    now: Arc<AtomicI64>,
    clock: Clock,
    cells: Vec<Arc<MemoryStore>>,
    deployments: Vec<Deployment>,
    trace: VecDeque<String>,
    summary: Summary,
    /// The deployments the current step may have changed.
    dirty: BTreeSet<usize>,
}

impl World {
    fn new(config: Config) -> Self {
        let now = Arc::new(AtomicI64::new(1_000));
        let ticks = Arc::clone(&now);
        let clock: Clock = store::stamps(move || ticks.load(Ordering::SeqCst));
        let cells = (0..CELLS).map(|_| Arc::new(MemoryStore::default())).collect();
        let summary = Summary { seed: config.seed, ..Summary::default() };
        Self { rng: Rng::new(config.seed), config, step: 0, now, clock, cells, deployments: Vec::new(), trace: VecDeque::new(), summary, dirty: BTreeSet::new() }
    }

    fn now(&self) -> i64 { self.now.load(Ordering::SeqCst) }

    fn note(&mut self, event: String) {
        if self.trace.len() == TRACE_LENGTH { self.trace.pop_front(); }
        self.trace.push_back(format!("#{} t={} {event}", self.step, self.now()));
    }

    async fn run(mut self) -> Result<Summary, Violation> {
        for step in 0..self.config.steps {
            self.step = step;
            let outcome = match self.step_once().await { Ok(()) => self.check().await, failed => failed };
            if let Err(message) = outcome {
                return Err(Violation { seed: self.config.seed, step, message, trace: self.trace.into_iter().collect() });
            }
        }
        self.summary.steps = self.config.steps;
        Ok(self.summary)
    }

    fn live(&self) -> Vec<usize> { (0..self.deployments.len()).filter(|&index| self.deployments[index].live()).collect() }

    fn pick_live(&mut self) -> Option<usize> {
        let live = self.live();
        (!live.is_empty()).then(|| live[self.rng.index(live.len())])
    }

    async fn step_once(&mut self) -> Step {
        let tick = self.rng.below(3) as i64;
        self.now.fetch_add(tick, Ordering::SeqCst);
        let live = self.live().len();
        if live == 0 || live < MAXIMUM_LIVE && self.rng.chance(2) {
            self.dirty.insert(self.deployments.len());
            return self.create();
        }
        let Some(index) = self.pick_live() else { return Ok(()) };
        let cell = self.deployments[index].cell;
        let before = self.deployments.len();
        // What a step can change: the deployment it acts on and those forked from it, or, for a pass over a cell's bucket, every deployment there.
        let (outcome, cells) = match self.rng.below(100) {
            0..25 => (self.write_held_back(index).await, None),
            25..37 => (self.flush(index).await, None),
            37..47 => (self.compact(index).await, None),
            47..62 => (self.release(index).await, None),
            62..66 => { self.restart(index); (Ok(()), None) }
            66..72 if live < MAXIMUM_LIVE => (self.fork(index, false).await, None),
            72..78 if live < MAXIMUM_LIVE => (self.fork(index, true).await, None),
            78..92 => { let cell = self.rng.index(CELLS); (self.reap(cell).await, Some(vec![cell])) }
            92..95 => (self.delete(index).await, Some(vec![cell])),
            95..98 => (self.move_cell(index).await, Some((0..CELLS).collect())),
            _ => (Ok(()), None),
        };
        match cells {
            None => {
                let mut lineage = vec![index];
                while let Some(next) = lineage.pop() { self.dirty.insert(next); lineage.extend(self.deployments[next].children.iter().copied()); }
                self.dirty.extend(before..self.deployments.len());
            }
            Some(cells) => self.dirty.extend((0..self.deployments.len()).filter(|&other| cells.iter().any(|cell| self.deployments[other].cells.contains_key(cell)))),
        }
        outcome
    }

    fn create(&mut self) -> Step {
        let index = self.deployments.len();
        let cell = self.rng.index(CELLS);
        let mut record = DeploymentRecord::new(format!("d{index}"), "acme", Vec::new(), 0);
        record.id = format!("d{index}");
        record.lineage = record.id.clone();
        self.deployments.push(Deployment {
            record, cell, cells: BTreeMap::from([(cell, None)]), state: State::Live, chain: Vec::new(), snapshots: Vec::new(), history: Vec::new(),
            pending: BTreeMap::new(), readable: BTreeMap::new(), optional: BTreeMap::new(), known: BTreeSet::new(), forbidden: BTreeSet::new(), children: Vec::new(), holder: None,
        });
        self.note(format!("create d{index} in cell {cell}"));
        Ok(())
    }

    /// Opens `index`'s store as a node does: from its published checkpoint's pack if it has one, by listing otherwise.
    async fn open(&self, index: usize) -> Result<Arc<GarbageStore>, String> {
        let deployment = &self.deployments[index];
        let discovery = match deployment.record.checkpoint { Some(checkpoint) => PackDiscovery::From(checkpoint.block), None => PackDiscovery::List };
        store::open_deployment_store(self.cells[deployment.cell].clone(), &deployment.store(), discovery, Arc::clone(&self.clock)).await
            .map_err(|error| format!("d{index}'s store does not open: {error}"))
    }

    async fn holder(&mut self, index: usize) -> Result<Arc<GarbageStore>, String> {
        if let Some(holder) = &self.deployments[index].holder { return Ok(Arc::clone(holder)); }
        let holder = self.open(index).await?;
        self.deployments[index].holder = Some(Arc::clone(&holder));
        Ok(holder)
    }

    /// The digest of the block `index` is writing next.
    fn next_digest(&self, index: usize) -> u64 {
        let deployment = &self.deployments[index];
        mix(mix(deployment.chain.last().copied().unwrap_or(0), hash(&deployment.record.id)), deployment.chain.len() as u64)
    }

    async fn put(&mut self, index: usize, store: &dyn ObjectStore, name: &str, bytes: Bytes) -> Step {
        store.put(&object(name)?, bytes).await.map_err(|error| format!("d{index} could not write {name}: {error}"))
    }

    async fn write_held_back(&mut self, index: usize) -> Step {
        let block = lex_hex(self.deployments[index].chain.len() as u64);
        let table = self.rng.index(TABLES);
        let name = match self.rng.below(3) {
            0 => format!("tables/t{table}/data/l00-rc-b{block}.arrow"),
            1 => format!("tables/t{table}/meta/l00-rc-b{block}.arrow"),
            _ => format!("tables/t{table}/blocks/b{block}.binpb"),
        };
        if self.deployments[index].pending.contains_key(&name) { return Ok(()); }
        let bytes = content(&name, self.next_digest(index));
        let holder = self.holder(index).await?;
        self.put(index, holder.as_ref(), &name, bytes.clone()).await?;
        self.deployments[index].pending.insert(name.clone(), bytes);
        self.summary.writes += 1;
        self.note(format!("d{index} writes {name}, held back"));
        Ok(())
    }

    /// Records `names`, newly durable in `index`, as objects its descendants forked earlier must never read.
    fn forbid(&mut self, index: usize, names: &[String]) {
        let mut descendants = self.deployments[index].children.clone();
        while let Some(child) = descendants.pop() {
            self.deployments[child].forbidden.extend(names.iter().cloned());
            descendants.extend(self.deployments[child].children.iter().copied());
        }
    }

    async fn flush(&mut self, index: usize) -> Step {
        let block = self.deployments[index].chain.len();
        let digest = self.next_digest(index);
        let catalog = block_key(BlockIndex(block as i64)).map_err(|error| error.to_string())?;
        let bytes = content(&catalog, digest);
        let holder = self.holder(index).await?;
        self.put(index, holder.as_ref(), &catalog, bytes.clone()).await?;
        let deployment = &mut self.deployments[index];
        let mut written: Vec<(String, Bytes)> = std::mem::take(&mut deployment.pending).into_iter().collect();
        written.push((catalog.clone(), bytes));
        for (name, bytes) in &written {
            deployment.readable.insert(name.clone(), bytes.clone());
            deployment.known.insert(name.clone());
            deployment.history.push(Write { name: name.clone(), at: block });
        }
        deployment.chain.push(digest);
        deployment.snapshots.push(deployment.readable.clone());
        let names: Vec<String> = written.into_iter().map(|(name, _)| name).collect();
        self.forbid(index, &names);
        self.summary.flushes += 1;
        // The holder publishes the block as its checkpoint, unless the directory is out of its reach for now.
        let published = self.rng.chance(70);
        if published { self.deployments[index].record.checkpoint = Some(Checkpoint { block: block as i64, boundary: 0, source: 0, system_time: 0, root_sha256: None }); }
        self.note(format!("d{index} flushes block {block}, packing {} objects{}", names.len(), if published { ", and publishes it" } else { "" }));
        Ok(())
    }

    async fn compact(&mut self, index: usize) -> Step {
        let blocks = self.deployments[index].chain.len();
        if blocks == 0 { return Ok(()); }
        let block = self.rng.index(blocks);
        let level = 1 + self.rng.below(2);
        let table = self.rng.index(TABLES);
        let name = format!("tables/t{table}/data/l{}-rc-b{}.arrow", lex_hex(level), lex_hex(block as u64));
        if self.deployments[index].known.contains(&name) { return Ok(()); }
        let bytes = content(&name, self.deployments[index].chain[block]);
        // A compaction worker writes through a store of its own; the holder sees its output through its own.
        let store = if self.rng.chance(50) { self.holder(index).await? } else { self.open(index).await? };
        self.put(index, store.as_ref(), &name, bytes.clone()).await?;
        let deployment = &mut self.deployments[index];
        deployment.readable.insert(name.clone(), bytes);
        deployment.known.insert(name.clone());
        deployment.history.push(Write { name: name.clone(), at: blocks });
        self.forbid(index, std::slice::from_ref(&name));
        self.summary.compactions += 1;
        self.note(format!("d{index} writes compaction output {name}"));
        Ok(())
    }

    async fn release(&mut self, index: usize) -> Step {
        let readable = &self.deployments[index].readable;
        if readable.is_empty() { return Ok(()); }
        let name = readable.keys().nth(self.rng.index(readable.len())).expect("in range").clone();
        let holder = self.holder(index).await?;
        holder.delete(&object(&name)?).await.map_err(|error| format!("d{index} could not release {name}: {error}"))?;
        self.deployments[index].readable.remove(&name);
        self.summary.releases += 1;
        self.note(format!("d{index} releases {name}"));
        Ok(())
    }

    fn restart(&mut self, index: usize) {
        let deployment = &mut self.deployments[index];
        let lost = deployment.pending.len();
        deployment.holder = None;
        deployment.pending.clear();
        self.summary.restarts += 1;
        self.note(format!("d{index}'s holder restarts, losing {lost} held-back objects"));
    }

    async fn fork(&mut self, parent: usize, past: bool) -> Step {
        let blocks = self.deployments[parent].chain.len();
        if past && blocks == 0 { return Ok(()); }
        let at = past.then(|| self.rng.index(blocks));
        if let Some(block) = at {
            let bucket = Arc::clone(&self.cells[self.deployments[parent].cell]);
            let written = match reference::written_at_ms(bucket.as_ref(), &self.deployments[parent].record.storage_prefix(), block as i64).await {
                Ok(written) => written,
                // Every object of the block was released and its pack erased, which only the grace's passing allows.
                Err(error) if error.code == "rtdb.multi/no-such-block" => {
                    self.note(format!("a fork of d{parent} at block {block} is refused, the block's pack being erased"));
                    return Ok(());
                }
                Err(error) => return Err(format!("d{parent}'s block {block} has no write time: {error}")),
            };
            if self.now() - written >= GRACE_MS {
                self.note(format!("a fork of d{parent} at block {block}, written at {written}, is refused as older than the grace"));
                return Ok(());
            }
        } else if !self.deployments[parent].pending.is_empty() {
            self.flush(parent).await?;
        }
        let child = self.deployments.len();
        let source = &self.deployments[parent];
        let mut record = DeploymentRecord::new(format!("d{child}"), "acme", Vec::new(), 0);
        record.id = format!("d{child}");
        record.lineage = source.record.lineage.clone();
        record.parent = Some(source.record.id.clone());
        record.checkpoint = None;
        let bucket = Arc::clone(&self.cells[source.cell]);
        reference::simulate_fork(bucket.as_ref(), &source.record.storage_prefix(), &record.storage_prefix(), at.map(|block| block as i64)).await
            .map_err(|error| format!("forking d{parent} into d{child}: {error}"))?;
        let forked = match at {
            None => Deployment {
                record, cell: source.cell, cells: BTreeMap::from([(source.cell, None)]), state: State::Live,
                chain: source.chain.clone(), snapshots: source.snapshots.clone(), history: source.history.clone(),
                pending: BTreeMap::new(), readable: source.readable.clone(), optional: source.optional.clone(), known: source.known.clone(), forbidden: source.forbidden.clone(),
                children: Vec::new(), holder: None,
            },
            Some(block) => {
                let readable = source.snapshots[block].clone();
                let later: Vec<&Write> = source.history.iter().filter(|write| write.at > block).collect();
                let at_or_before = |name: &str| trie_block(name).is_some_and(|written| written <= block as i64) && !name.contains("/l00-");
                let mut optional: BTreeMap<String, Bytes> = source.optional.iter().filter(|(name, _)| at_or_before(name)).map(|(name, bytes)| (name.clone(), bytes.clone())).collect();
                for write in &later {
                    if at_or_before(&write.name) { optional.insert(write.name.clone(), content(&write.name, source.chain[trie_block(&write.name).expect("a trie file") as usize])); }
                }
                let history: Vec<Write> = source.history.iter().filter(|write| write.at <= block).cloned().collect();
                let mut forbidden = source.forbidden.clone();
                forbidden.extend(later.iter().map(|write| write.name.clone()).filter(|name| !optional.contains_key(name)));
                let known = history.iter().map(|write| write.name.clone()).chain(readable.keys().cloned()).chain(optional.keys().cloned()).collect();
                Deployment {
                    record, cell: source.cell, cells: BTreeMap::from([(source.cell, None)]), state: State::Live,
                    chain: source.chain[..=block].to_vec(), snapshots: source.snapshots[..=block].to_vec(), history,
                    pending: BTreeMap::new(), readable, optional, known, forbidden, children: Vec::new(), holder: None,
                }
            }
        };
        self.deployments.push(forked);
        self.deployments[parent].children.push(child);
        match at {
            Some(block) => { self.summary.past_forks += 1; self.note(format!("fork d{parent} at block {block} into d{child}")); }
            None => { self.summary.forks += 1; self.note(format!("fork d{parent} into d{child}")); }
        }
        Ok(())
    }

    /// The cell's deployment records as its directory holds them: a deleted or moved-away deployment is a tombstone.
    fn records(&self, cell: usize, except: Option<usize>) -> Vec<DeploymentRecord> {
        let mut records = Vec::new();
        for (index, deployment) in self.deployments.iter().enumerate() {
            if Some(index) == except { continue; }
            let Some(&moved_at_ms) = deployment.cells.get(&cell) else { continue };
            let mut record = deployment.record.clone();
            record.moved_at_ms = moved_at_ms;
            if !deployment.live() || moved_at_ms.is_some() { record.lifecycle = Lifecycle::Deleted; }
            records.push(record);
        }
        records
    }

    async fn reap(&mut self, cell: usize) -> Step {
        let records = self.records(cell, None);
        let bucket = Arc::clone(&self.cells[cell]);
        let grace = Duration::from_millis(GRACE_MS as u64);
        let outcomes = match self.config.bug {
            Some(Bug::ReaperSkipsCopy) => {
                let mut outcomes = Vec::new();
                for record in records.iter().filter(|record| record.lifecycle != Lifecycle::Deleted) {
                    outcomes.push(store::simulate_reap_without_references(bucket.as_ref(), record, self.now(), grace).await);
                }
                outcomes
            }
            _ => {
                if self.rng.chance(20) {
                    let faulty = PackReadFailure { bucket: bucket.as_ref(), fired: AtomicBool::new(false) };
                    let first = store::reaper_pass(&faulty, &records, self.now(), grace).await.map_err(|error| error.to_string())?;
                    let mut saw_failure = false;
                    for outcome in first {
                        match outcome {
                            Ok(result) => self.summary.erased += result.erased,
                            Err(error) if error.message.as_deref().is_some_and(|message| message.ends_with(PACK_READ_FAILURE)) => saw_failure = true,
                            Err(error) => return Err(error.to_string()),
                        }
                    }
                    if faulty.fired.load(Ordering::Relaxed) != saw_failure {
                        return Err("the reaper did not report a failed pack read".into());
                    }
                    if saw_failure {
                        self.summary.failed_pack_reads += 1;
                        self.note(format!("the reaper encounters a pack read failure in cell {cell}"));
                        self.dirty.extend(self.live());
                        self.check().await?;
                    }
                }
                store::simulate_reap_cell(bucket.as_ref(), &records, self.now(), grace).await.map_err(|error| format!("the reaper's scan of cell {cell} failed: {error}"))?
            }
        };
        let mut erased = 0;
        for outcome in outcomes { erased += outcome.map_err(|error| format!("the reaper failed in cell {cell}: {error}"))?.erased; }
        self.summary.reaper_passes += 1;
        self.summary.erased += erased;
        let others: Vec<_> = records.iter().filter(|record| record.moved_at_ms.is_none()).cloned().collect();
        let now = self.now();
        for record in records.iter().filter(|record| record.moved_at_ms.is_some_and(|moved| moved + GRACE_MS <= now)) {
            let prefix = object(&record.storage_prefix())?;
            if !bucket.list(Some(&prefix)).await.map_err(|error| error.to_string())?.is_empty() {
                if !reference::simulate_erase_deployment(bucket.as_ref(), prefix.as_str(), &others).await.map_err(|error| error.to_string())? {
                    return Err(format!("cell {cell} still holds {} after its move grace", record.name));
                }
                self.summary.cleaned_moves += 1;
            }
        }
        self.note(format!("the reaper passes over cell {cell}, erasing {erased}"));
        Ok(())
    }

    async fn delete(&mut self, index: usize) -> Step {
        let cell = self.deployments[index].cell;
        let others = match self.config.bug { Some(Bug::DeletionSkipsCopy) => Vec::new(), _ => self.records(cell, Some(index)) };
        let deployment = &mut self.deployments[index];
        deployment.holder = None;
        deployment.pending.clear();
        deployment.state = State::Deleted;
        let prefix = deployment.record.storage_prefix();
        let empty = reference::simulate_erase_deployment(self.cells[cell].as_ref(), &prefix, &others).await.map_err(|error| format!("deleting d{index}: {error}"))?;
        if !empty { return Err(format!("deleting d{index} did not prove its prefix empty")); }
        self.summary.deletions += 1;
        self.note(format!("delete d{index}"));
        Ok(())
    }

    async fn move_cell(&mut self, index: usize) -> Step {
        let Some(target) = (0..CELLS).find(|cell| !self.deployments[index].cells.contains_key(cell)) else { return Ok(()) };
        let moved_at = self.now();
        let deployment = &mut self.deployments[index];
        let source = deployment.cell;
        deployment.holder = None;
        deployment.pending.clear();
        let prefix = deployment.record.storage_prefix();
        reference::simulate_materialize(self.cells[source].as_ref(), &prefix, self.cells[target].as_ref(), &prefix).await.map_err(|error| format!("moving d{index}: {error}"))?;
        let deployment = &mut self.deployments[index];
        deployment.cell = target;
        deployment.cells.insert(source, Some(moved_at));
        deployment.cells.insert(target, None);
        let left = self.cells[source].list(Some(&object(&prefix)?)).await.map_err(|error| error.to_string())?.len();
        self.summary.moves += 1;
        self.summary.left_behind += left;
        self.note(format!("move d{index} from cell {source} to cell {target}, leaving {left} objects behind"));
        Ok(())
    }

    async fn listed(index: usize, store: &dyn ObjectStore) -> Result<BTreeSet<String>, String> {
        let mut listed = BTreeSet::new();
        for directory in ["tables", "blocks"] {
            for found in store.list(Some(&object(directory)?)).await.map_err(|error| format!("d{index} cannot list {directory}: {error}"))? { listed.insert(found.key.as_str().to_owned()); }
        }
        Ok(listed)
    }

    async fn reads(index: usize, store: &dyn ObjectStore, name: &str, bytes: &Bytes, through: &str) -> Step {
        match store.get(&object(name)?).await {
            Ok(read) if read == *bytes => Ok(()),
            Ok(read) => Err(format!("d{index} reads {name} through its {through} as {:?}, not {:?}", String::from_utf8_lossy(&read), String::from_utf8_lossy(bytes))),
            Err(error) => Err(format!("d{index} cannot read {name} through its {through}: {error}")),
        }
    }

    fn unexpected(&self, index: usize, name: &str, through: &str) -> String {
        let deployment = &self.deployments[index];
        if deployment.forbidden.contains(name) { format!("d{index} lists {name} through its {through}, which an ancestor wrote after d{index}'s fork point") }
        else if deployment.known.contains(name) { format!("d{index} lists {name} through its {through}, which it released") }
        else { format!("d{index} lists {name} through its {through}, which it never had") }
    }

    /// Checks every deployment the step may have changed, and every few steps all of them.
    async fn check(&mut self) -> Step {
        let dirty = std::mem::take(&mut self.dirty);
        let all = self.step % FULL_CHECK_EVERY == 0 || self.step + 1 == self.config.steps;
        for index in 0..self.deployments.len() {
            if !all && !dirty.contains(&index) { continue; }
            if !self.deployments[index].live() {
                let prefix = self.deployments[index].record.storage_prefix();
                let remaining = self.cells[self.deployments[index].cell].list(Some(&object(&prefix)?)).await.map_err(|error| error.to_string())?;
                if let Some(first) = remaining.first() { return Err(format!("{} objects of deleted d{index} remain, {} first", remaining.len(), first.key)); }
                continue;
            }
            let fresh = self.open(index).await?;
            let deployment = &self.deployments[index];
            let listed = Self::listed(index, fresh.as_ref()).await?;
            for (name, bytes) in &deployment.readable {
                if !listed.contains(name) { return Err(format!("d{index} does not list {name}, which it wrote or inherited and has not released")); }
                Self::reads(index, fresh.as_ref(), name, bytes, "store").await?;
            }
            for name in &listed {
                if deployment.readable.contains_key(name) { continue; }
                let Some(bytes) = deployment.optional.get(name) else { return Err(self.unexpected(index, name, "store")) };
                Self::reads(index, fresh.as_ref(), name, bytes, "store").await?;
            }
            for name in &deployment.forbidden {
                if deployment.known.contains(name) { continue; }
                if let Ok(read) = fresh.get(&object(name)?).await { return Err(format!("d{index} reads {name} as {:?}, which an ancestor wrote after its fork point", String::from_utf8_lossy(&read))); }
            }
            let Some(holder) = deployment.holder.clone() else { continue };
            let listed = Self::listed(index, holder.as_ref()).await?;
            for (name, bytes) in deployment.readable.iter().chain(&deployment.pending) {
                if !listed.contains(name) { return Err(format!("d{index}'s holder does not list {name}")); }
                Self::reads(index, holder.as_ref(), name, bytes, "holder's store").await?;
            }
            for name in &listed {
                if !deployment.readable.contains_key(name) && !deployment.pending.contains_key(name) && !deployment.optional.contains_key(name) { return Err(self.unexpected(index, name, "holder's store")); }
            }
        }
        Ok(())
    }
}

pub fn run_with(config: Config) -> Result<Summary, Violation> {
    let runtime = tokio::runtime::Builder::new_current_thread().build().expect("a current-thread runtime builds");
    runtime.block_on(World::new(config).run())
}
