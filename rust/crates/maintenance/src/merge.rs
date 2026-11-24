mod layout;
mod planner;
mod row_heap;

use rtdb_types::StructFields;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use async_trait::async_trait;
use arrow_schema::{DataType, SchemaRef};
use rtdb_types::CivilDate;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch, from_field};
use rtdb_format::{data::{EncodedEvent, EventBatch, PageEvent, compacted_event_schema, copy_compaction_batch}, ipc::{ArrowFile, ArrowFileWriter, inspect_file_reserved}, keys::TrieKey,
    log::proto::{TriesAdded, TrieDetails, TrieFileVersions}, metadata::MetadataFile};
use rtdb_index::{PartitionValue, TrieState};
use rtdb_storage::{HeldView, IoRegion, ObjectKey, ObjectStore, ReadReservation};
use rtdb_temporal::AdmittedPolygonCalculator;
use rtdb_types::{EventOp, Iid, Interval, LogicalType, Result, RunEndWidth, StorageEpoch, StorageVersion, TableRef, Timestamp};
use tokio_util::sync::CancellationToken;
use crate::{CompactionExecutor, CompactionJob, RecencyMode, SnapshotSource};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecencyPartition { Week, Month, Quarter, Year }

#[derive(Clone, Debug)]
pub struct MergeConfig {
    pub page_size: usize,
    pub run_end_encoded_iids: bool,
    pub within_file_recency_branching: bool,
    pub recency_partition: RecencyPartition,
    pub temporary_directory: PathBuf,
    pub temporary_byte_limit: u64,
    pub scan_tie_order_from_naming_epoch: Option<NonZeroU64>,
}

impl Default for MergeConfig {
    fn default() -> Self { Self { page_size: 1024, run_end_encoded_iids: false, within_file_recency_branching: false,
        recency_partition: RecencyPartition::Week, temporary_directory: std::env::temp_dir(), temporary_byte_limit: 16 * 1024 * 1024 * 1024,
        scan_tie_order_from_naming_epoch: None } }
}

impl MergeConfig {
    fn tie_order(&self, naming_epoch: u64) -> planner::TieOrder {
        match self.scan_tie_order_from_naming_epoch {
            Some(first) if naming_epoch >= first.get() => planner::TieOrder::Scan,
            _ => planner::TieOrder::Legacy,
        }
    }
}

pub struct SegmentCompactor {
    store: Arc<dyn ObjectStore>,
    snapshots: Arc<dyn SnapshotSource>,
    memory: Arc<MemoryBudget>,
    version: StorageVersion,
    epoch: StorageEpoch,
    config: MergeConfig,
    artefacts: Vec<Arc<dyn crate::ArtefactBuilder>>,
}

impl SegmentCompactor {
    pub fn new(store: Arc<dyn ObjectStore>, snapshots: Arc<dyn SnapshotSource>, memory: Arc<MemoryBudget>,
        version: StorageVersion, epoch: StorageEpoch, config: MergeConfig) -> Result<Self> {
        if config.page_size == 0 || config.temporary_byte_limit == 0 { return Err(crate::invalid("Compaction page size and temporary byte limit must be positive")); }
        Ok(Self { store, snapshots, memory, version, epoch, config, artefacts: Vec::new() })
    }

    /// Builds the artefacts these builders' definitions want on each output trie from its data file before the trie is published, and records them with it.
    pub fn with_artefacts(mut self, artefacts: Vec<Arc<dyn crate::ArtefactBuilder>>) -> Self { self.artefacts = artefacts; self }

    /// The records of every artefact built on an output; one that fails is left to the artefact lane, which builds whatever a live trie lacks.
    async fn artefacts_of(&self, snapshot: &Arc<rtdb_index::PartitionValue>, table: &rtdb_types::TableRef, trie: &TrieKey, data: &Path, size: u64, cancellation: &CancellationToken) -> Vec<rtdb_format::log::proto::ArtefactRecord> {
        let mut records = Vec::new();
        let size_signed = i64::try_from(size).unwrap_or(i64::MAX);
        for builder in &self.artefacts {
            let Ok(definitions) = builder.definitions(snapshot, cancellation).await else { continue };
            for definition in definitions.into_iter().filter(|definition| &definition.table == table && crate::wants(definition, trie, size_signed, snapshot.trie_catalog().file_size_target())) {
                // Tiered compaction publishes one bucket at a time; the lane sees the whole group once it is live.
                if trie.part_group().is_some() && definition.group_bytes.is_some_and(|limit| size_signed <= limit) { continue; }
                let job = crate::ArtefactJob { definition, owner: rtdb_format::keys::ArtefactOwner::Trie(trie.clone()) };
                if let Ok(record) = builder.build(&job, crate::ArtefactSource::Local { data, size }, snapshot, cancellation).await {
                    records.push(rtdb_format::log::proto::ArtefactRecord { scope: Some(job.scope() as i32), ..record });
                }
            }
        }
        records
    }
}

struct InputSegment { key: TrieKey, metadata: MetadataFile, reader: ArrowFile, _reservations: Vec<Arc<MemoryReservation>> }

struct MergeFile {
    path: PathBuf,
    part: Vec<u8>,
    recency: Option<CivilDate>,
    carries_recency: bool,
    keyed: bool,
    topology: bool,
    include_origins: bool,
    include_positions: bool,
    redacted_leg: bool,
    writer: Option<ArrowFileWriter>,
    leaves: Vec<layout::Leaf>,
}

struct OutputSchema { put: Option<StructFields<LogicalType>>, iid_encoding: Option<RunEndWidth>, recency: bool, keyed: bool, topology: bool, include_origins: bool, include_positions: bool, redacted_leg: bool }

impl OutputSchema {
    fn batch(&self, events: &[PageEvent], memory: &Arc<MemoryBudget>) -> Result<OwnedBatch> {
        let batch = copy_compaction_batch(events, self.put.clone(), self.iid_encoding, self.recency, self.redacted_leg, memory)?;
        let batch = if self.topology {
            rtdb_format::data::with_topology_commit_positions(batch, memory)?
        } else {
            batch
        };
        if self.include_origins && batch.batch().column_by_name(rtdb_format::data::EVENT_ORIGIN_FIELD).is_none() {
            return rtdb_format::data::append_origins(&batch, std::iter::repeat_n(None, events.len()), memory);
        }
        Ok(batch)
    }
    fn schema(&self) -> Result<SchemaRef> {
        let schema = compacted_event_schema(self.put.clone(), self.iid_encoding, self.recency, self.redacted_leg)?;
        let mut fields = schema.fields().to_vec();
        if self.keyed {
            fields.push(Arc::new(rtdb_format::data::entity_key_field()));
        }
        if self.keyed || self.topology {
            fields.push(Arc::new(rtdb_format::data::commit_position_field()));
        }
        if self.include_origins { fields.push(Arc::new(rtdb_format::data::event_origin_field())); }
        if self.include_positions { fields.push(Arc::new(rtdb_format::data::event_position_field())); }
        Ok(Arc::new(arrow_schema::Schema::new(fields)))
    }
}

fn create_file(path: &Path) -> Result<File> {
    OpenOptions::new().create_new(true).write(true).read(true).open(path).map_err(crate::invalid)
}

/// What encoding an event may hold at once: its document's decoded cells, and the event itself.
fn row_bytes(event: &PageEvent) -> Result<usize> {
    event.document_bytes()?.checked_add(event.entity_key().map_or(0, |key| key.len()))
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<PageEvent>() + 128)).ok_or_else(|| crate::invalid("Compaction row size overflow"))
}

fn event_reservation(memory: &Arc<MemoryBudget>, events: &[PageEvent], multiplier: usize) -> Result<Arc<MemoryReservation>> {
    let size = events.iter().try_fold(65_536usize, |size, event| size.checked_add(row_bytes(event)?).ok_or_else(|| crate::invalid("Compaction row size overflow")))?;
    memory.reserve(size.checked_mul(multiplier).ok_or_else(|| crate::invalid("Compaction allocation bound overflow"))?)
}

fn disk_usage(directory: &Path) -> Result<u64> {
    std::fs::read_dir(directory).map_err(crate::invalid)?.try_fold(0u64, |total, entry| {
        let size = entry.map_err(crate::invalid)?.metadata().map_err(crate::invalid)?.len();
        total.checked_add(size).ok_or_else(|| crate::invalid("Temporary file size overflow"))
    })
}

fn check_disk(directory: &Path, limit: u64, additional: u64) -> Result<()> {
    if disk_usage(directory)?.checked_add(additional).is_none_or(|size| size > limit) { return Err(crate::invalid("Compaction temporary disk budget exceeded")); }
    Ok(())
}

fn disk_remaining(directory: &Path, limit: u64) -> Result<u64> {
    limit.checked_sub(disk_usage(directory)?).ok_or_else(|| crate::invalid("Compaction temporary disk budget exceeded"))
}

fn limit_writer(writer: &mut ArrowFileWriter, directory: &Path, limit: u64) -> Result<()> {
    let allowance = writer.bytes_written().checked_add(disk_remaining(directory, limit)?).ok_or_else(|| crate::invalid("Compaction temporary disk budget overflow"))?;
    writer.set_byte_limit(allowance)
}

pub async fn download(store: &dyn ObjectStore, memory: &Arc<MemoryBudget>, key: &ObjectKey, path: &Path, cancellation: &CancellationToken) -> Result<File> {
    let object = store.head(key).await?;
    let mut file = create_file(path)?;
    let ranges = store.supports_scan_ranges(key);
    let mut position = 0;
    while position < object.size {
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        let remaining = object.size - position;
        let count = usize::try_from(if ranges { remaining.min(1024 * 1024) } else { remaining }).map_err(crate::invalid)?;
        let allocation = if ranges { store.scan_range_allocation(key, count)? } else { store.whole_object_allocation(key, count)? };
        let retained = memory.reserve(allocation.retained_bytes)?;
        let scratch = (allocation.scratch_bytes > 0).then(|| memory.reserve(allocation.scratch_bytes)).transpose()?;
        let reservation = ReadReservation::funded(count, retained, scratch);
        let bytes = if ranges {
            store.get_reserved_scan_range(key, position..position + count as u64, reservation).await?
        } else {
            store.get_reserved_object(key, reservation).await?
        };
        file.write_all(bytes.as_slice()).map_err(crate::invalid)?;
        position += count as u64;
    }
    file.seek(SeekFrom::Start(0)).map_err(crate::invalid)?;
    Ok(file)
}

pub fn open_input(
    mut file: impl Read + Seek + Send + 'static,
    memory: &Arc<MemoryBudget>,
) -> Result<(ArrowFile, Arc<MemoryReservation>)> {
    let layout = inspect_file_reserved(&mut file, |bytes| memory.reserve(bytes))?;
    let reservation = memory.reserve(layout.opening_allocation_bound()?)?;
    let reader = ArrowFile::from_inspected_reader(file, None, layout)?;
    Ok((reader, reservation))
}

pub fn read_page(reader: &mut ArrowFile, index: usize, memory: &Arc<MemoryBudget>) -> Result<OwnedBatch> {
    let bound = reader.page_allocation_bound(index)?;
    let reservation = memory.reserve(bound)?;
    OwnedBatch::with_reservation(reader.read_page(index)?, reservation)
}

fn decoded_bound(batch: &arrow_array::RecordBatch) -> Result<usize> {
    let mut bytes = batch.get_array_memory_size().checked_mul(2).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(|| crate::invalid("Decoded event allocation overflow"))?;
    let mut arrays: Vec<_> = batch.columns().iter().map(|array| array.to_data()).collect();
    while let Some(array) = arrays.pop() {
        bytes = bytes.checked_add(array.len().checked_mul(256).ok_or_else(|| crate::invalid("Decoded event allocation overflow"))?).ok_or_else(|| crate::invalid("Decoded event allocation overflow"))?;
        arrays.extend(array.child_data().iter().cloned());
    }
    Ok(bytes)
}

fn merged_put_schema(inputs: &[InputSegment]) -> Result<Option<StructFields<LogicalType>>> {
    let mut puts = Vec::new();
    for input in inputs {
        let schema = input.reader.schema();
        let op = schema.field_with_name("op").map_err(crate::invalid)?;
        let DataType::Union(fields, _) = op.data_type() else { return Err(crate::invalid("Input event schema lacks operation union")); };
        if let Some((_, field)) = fields.iter().find(|(_, field)| field.name() == "put" && !matches!(field.data_type(), DataType::Struct(fields) if fields.is_empty())) {
            puts.push(from_field(field)?);
        }
    }
    if puts.is_empty() { return Ok(None); }
    let LogicalType::Struct(fields) = LogicalType::join_all(&puts) else { return Err(crate::invalid("Merged put schema is not a struct")); };
    Ok(Some(fields))
}

/// Page and trie order use IID, then descending system time. Each complete
/// key within the current physical bucket owns its temporal state.
struct EntityStates<'a, State> {
    bucket: Option<Iid>,
    plain: Option<State>,
    keyed: BTreeMap<&'a [u8], (State, Arc<MemoryReservation>)>,
}

impl<'a, State> EntityStates<'a, State> {
    fn new() -> Self {
        Self { bucket: None, plain: None, keyed: BTreeMap::new() }
    }

    fn get(
        &mut self, event: &'a PageEvent, memory: &Arc<MemoryBudget>,
        init: impl FnOnce() -> State,
    ) -> Result<&mut State> {
        if self.bucket != Some(event.iid) {
            self.plain = None;
            self.keyed.clear();
            self.bucket = Some(event.iid);
        }
        let Some(key) = event.entity_key() else { return Ok(self.plain.get_or_insert_with(init)); };
        match self.keyed.entry(key) {
            std::collections::btree_map::Entry::Occupied(entry) => Ok(&mut entry.into_mut().0),
            std::collections::btree_map::Entry::Vacant(entry) => {
                let charge = memory.reserve(std::mem::size_of::<State>() + 512)?;
                Ok(&mut entry.insert((init(), charge)).0)
            }
        }
    }
}

struct SameTimeState {
    calculator: AdmittedPolygonCalculator,
    system_from: Option<Timestamp>,
    erased: bool,
}

fn resolve_same_time(events: &[PageEvent], memory: &Arc<MemoryBudget>) -> Result<(Vec<PageEvent>, Vec<Arc<MemoryReservation>>)> {
    let mut result = Vec::new();
    let mut charges = Vec::new();
    let mut states = EntityStates::new();
    for event in events {
        let state = states.get(event, memory, || SameTimeState {
            calculator: AdmittedPolygonCalculator::new(None, Arc::clone(memory)),
            system_from: None, erased: false,
        })?;
        if state.erased { continue; }
        if state.system_from != Some(event.system_from) {
            state.calculator.reset();
            state.system_from = Some(event.system_from);
        }
        let header = event.header();
        let resolved = state.calculator.visit(&header, |polygon| {
            if header.position.is_some() || header.commit_position != 0 || header.op == EventOp::Redacted {
                charges.push(memory.reserve(row_bytes(event)?.checked_mul(2).ok_or_else(|| crate::invalid("Resolved row allocation overflow"))?)?);
                result.push(event.clone());
                return Ok(());
            }
            for (valid_from, valid_to, system_to) in polygon.ranges() {
                if system_to > event.system_from {
                    charges.push(memory.reserve(row_bytes(event)?.checked_mul(2).ok_or_else(|| crate::invalid("Resolved row allocation overflow"))?)?);
                    let mut event = event.clone();
                    event.valid_time = Interval::new(valid_from, valid_to)?;
                    result.push(event);
                }
            }
            Ok(())
        })?;
        if header.op == EventOp::Erase { state.erased = true; }
        if resolved.is_none() {
            charges.push(memory.reserve(row_bytes(event)?.checked_mul(2).ok_or_else(|| crate::invalid("Resolved row allocation overflow"))?)?);
            let mut event = event.clone();
            event.valid_time = Interval::ALL;
            result.push(event);
        }
    }
    Ok((result, charges))
}

pub fn recency_partition(recency: Timestamp, partition: RecencyPartition) -> Result<CivilDate> {
    let epoch_day = recency.0.div_euclid(86_400_000_000) - i64::from(recency.0.rem_euclid(86_400_000_000) == 0);
    let date = CivilDate::from_epoch_day(epoch_day)?;
    match partition {
        RecencyPartition::Week => date.plus_days(i64::from(8 - date.iso_day_of_week())),
        RecencyPartition::Month => CivilDate::from_ymd(date.year(), date.month(), 1)?.plus_months(1),
        RecencyPartition::Quarter => CivilDate::from_ymd(date.year(), (date.month() - 1) / 3 * 3 + 1, 1)?.plus_months(3),
        RecencyPartition::Year => CivilDate::from_ymd(date.year() + 1, 1, 1),
    }
}

#[async_trait]
impl CompactionExecutor for SegmentCompactor {
    async fn execute(&self, job: &CompactionJob, cancellation: &CancellationToken) -> Result<TriesAdded> {
        let snapshot = self.snapshots.snapshot();
        if self.store.defers_trie_releases() && job.inputs.iter().any(|key| {
            !snapshot.trie_catalog().trie(&job.table, key).is_some_and(|trie| trie.state == TrieState::Live)
        }) {
            return Err(rtdb_types::Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.maintenance/compaction-input-retired",
                "The compaction selection no longer names live inputs"));
        }
        let captured = snapshot.snapshot();
        let view = HeldView::catalog_at(self.store.as_ref(), snapshot.clone(), captured.catalog_cut(), captured.catalog_checkpoint()?, captured.catalog_file_paths()).await?;
        IoRegion::reading(view, self.execute_job(job, snapshot, cancellation)).await
    }
}

impl SegmentCompactor {
    async fn execute_job(&self, job: &CompactionJob, snapshot: Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<TriesAdded> {
        let positioned_history = TableRef::owned_schema(&job.table.schema);
        let directory = tempfile::Builder::new().prefix("rtdb-compaction-").tempdir_in(&self.config.temporary_directory).map_err(crate::invalid)?;
        let entry = snapshot.table_catalog().entry(&job.table).cloned().ok_or_else(|| crate::invalid("Compaction table has no recorded slug"))?;
        let mut inputs = Vec::new();
        for (index, key) in job.inputs.iter().enumerate() {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            let data_key = ObjectKey::new(entry.slug.data_key(key)).map_err(crate::invalid)?;
            let meta_key = ObjectKey::new(entry.slug.metadata_key(key)).map_err(crate::invalid)?;
            let size = self.store.head(&data_key).await.map_err(crate::invalid)?.size.checked_add(self.store.head(&meta_key).await.map_err(crate::invalid)?.size).ok_or_else(|| crate::invalid("Input file size overflow"))?;
            check_disk(directory.path(), self.config.temporary_byte_limit, size)?;
            let data = download(self.store.as_ref(), &self.memory, &data_key, &directory.path().join(format!("input-{index}.arrow")), cancellation).await?;
            let mut metadata = download(self.store.as_ref(), &self.memory, &meta_key, &directory.path().join(format!("meta-{index}.arrow")), cancellation).await?;
            let metadata_layout = inspect_file_reserved(&mut metadata, |bytes| self.memory.reserve(bytes))?;
            let metadata_charge = self.memory.reserve(metadata_layout.full_allocation_bound()?)?;
            let metadata = MetadataFile::read_reader(metadata)?;
            let (reader, reservation) = open_input(data, &self.memory)?;
            inputs.push(InputSegment { key: key.clone(), metadata, reader, _reservations: vec![reservation, metadata_charge] });
        }
        let keyed = job.table.is_owned_event_data();
        let topology = job.table.is_owned_world_topology();
        let include_origins = inputs.iter().any(|input| input.reader.schema().index_of(rtdb_format::data::EVENT_ORIGIN_FIELD).is_ok());
        if inputs.iter().any(|input| input.reader.schema().index_of(rtdb_format::data::ENTITY_KEY_FIELD).is_ok() != keyed) {
            return Err(crate::invalid("Compaction input does not have the required entity-key layout"));
        }
        let include_positions = inputs.iter().any(|input| input.reader.schema().index_of(rtdb_format::data::EVENT_POSITION_FIELD).is_ok());
        if include_positions && inputs.iter().any(|input| input.reader.schema().index_of(rtdb_format::data::EVENT_POSITION_FIELD).is_err()) {
            return Err(crate::invalid("Positioned history cannot mix with unpositioned primary pages"));
        }
        let redacted_leg = inputs.iter().any(|input| input.reader.schema().field_with_name("op").is_ok_and(|op| {
            matches!(op.data_type(), DataType::Union(fields, _) if fields.iter().any(|(_, field)| field.name() == "redacted"))
        }));
        let put = merged_put_schema(&inputs)?;
        let carry = self.config.within_file_recency_branching && job.output.recency().is_some() && inputs.iter().all(|input| input.reader.schema().index_of("_recency").is_ok());
        let tasks = planner::plan(&inputs, &job.part)?;
        let tie_order = self.config.tie_order(job.output.epoch());
        let mut sources: Vec<_> = (0..inputs.len()).collect();
        sources.sort_by(|&left, &right| inputs[left].key.scan_precedence().cmp(&inputs[right].key.scan_precedence()));
        let mut source_precedence = vec![0; inputs.len()];
        for (priority, source) in sources.into_iter().enumerate() {
            source_precedence[source] = priority;
        }
        let initial_recency = if job.recency_mode == RecencyMode::Partition { None } else { job.output.recency() };
        let initial_carry = job.recency_mode == RecencyMode::Preserve && carry;
        let mut outputs = vec![MergeFile::new(directory.path().join("merged-0.arrow"), initial_recency, initial_carry, &job.part, &put, keyed, topology, include_origins, include_positions, redacted_leg, &self.config, &self.memory)?];
        for task in tasks {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            let mut rows = Vec::new();
            let mut charges = Vec::new();
            for page in &task.pages {
                let input = &mut inputs[page.segment];
                let batch = read_page(&mut input.reader, page.page, &self.memory)?;
                let _decoded = self.memory.reserve(decoded_bound(batch.batch())?)?;
                let mut events = EventBatch::new(batch.batch().clone())?.page_events()?;
                if input.key.level() == 0 && !positioned_history {
                    let (resolved, reservations) = resolve_same_time(&events, &self.memory)?;
                    events = resolved;
                    charges.extend(reservations);
                }
                let path = if job.part.len() > task.path.len() { &job.part } else { &task.path };
                events.retain(|event| planner::matches_path(event.iid, path));
                charges.push(event_reservation(&self.memory, &events, 2)?);
                rows.push(planner::PageRows { source_precedence: source_precedence[page.segment], events });
            }
            let mut heap = planner::RowHeap::new(&rows, tie_order)?;
            let mut states = EntityStates::new();
            let mut buckets: BTreeMap<usize, Vec<PageEvent>> = BTreeMap::new();
            let mut bucket_charges = Vec::new();
            let mut previous = None;
            while let Some((stream, row)) = heap.next() {
                let event = &rows[stream].events[row];
                let mut recency = if positioned_history {
                    if event.op == EventOp::Erase {
                        return Err(crate::invalid("Owned erasure requires its ordered custody successor"));
                    }
                    let coordinate = event.position.map(|position| (event.iid, position));
                    // The receiver checks the contents of every occurrence at
                    // this coordinate before it can select the replacement.
                    if coordinate.is_some() && previous == coordinate { continue; }
                    previous = coordinate;
                    Timestamp::MAX
                } else {
                    let calculator = states.get(event, &self.memory, || AdmittedPolygonCalculator::new(None, Arc::clone(&self.memory)))?;
                    let Some(recency) = calculator.visit(&event.header(), |polygon| Ok(polygon.recency()))? else { continue; };
                    recency
                };
                // This merge covers every older primary input for an expiring table.
                if event.op == EventOp::Erase && job.table.stores_expiring_transaction_metadata() {
                    continue;
                }
                if event.position.is_some() || event.commit_position != 0 || event.op == EventOp::Redacted {
                    recency = recency.max(event.system_from);
                }
                let target = if job.recency_mode == RecencyMode::Partition && recency != Timestamp::MAX {
                    let date = recency_partition(recency, self.config.recency_partition)?;
                    if let Some(index) = outputs.iter().position(|output| output.recency == Some(date)) { index }
                    else {
                        let index = outputs.len();
                        outputs.push(MergeFile::new(directory.path().join(format!("merged-{index}.arrow")), Some(date), self.config.within_file_recency_branching, &job.part, &put, keyed, topology, include_origins, include_positions, redacted_leg, &self.config, &self.memory)?);
                        index
                    }
                } else { 0 };
                bucket_charges.push(self.memory.reserve(row_bytes(event)?.checked_mul(2).ok_or_else(|| crate::invalid("Output row allocation overflow"))?)?);
                let mut event = event.clone();
                event.recency = if outputs[target].carries_recency { Some(if job.recency_mode == RecencyMode::Partition { recency } else { event.recency.ok_or_else(|| crate::invalid("Historical input lost recency"))? }) } else { None };
                buckets.entry(target).or_default().push(event);
            }
            drop(states);
            for (index, events) in buckets {
                outputs[index].write(&events, task.path.clone(), &put, &self.config, &self.memory)?;
            }
            tokio::task::yield_now().await;
        }
        drop(inputs);
        for output in &mut outputs { output.finish(&self.config)?; }
        let order: Vec<_> = if job.recency_mode == RecencyMode::Partition { (1..outputs.len()).chain(std::iter::once(0)).collect() } else { vec![0] };
        let mut tries = Vec::new();
        for index in order {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            let output = &outputs[index];
            let key = TrieKey::new(job.output.level(), output.recency, job.output.part().to_vec(), job.output.block_index())?.with_epoch(job.output.epoch());
            let data_path = directory.path().join(format!("output-{index}.arrow"));
            let meta_path = directory.path().join(format!("output-{index}.meta.arrow"));
            let (size, metadata) = layout::normalize(output, &put, &self.config, &self.memory, &data_path, &meta_path, cancellation)?;
            check_disk(directory.path(), self.config.temporary_byte_limit, 0)?;
            let data_key = ObjectKey::new(entry.slug.data_key(&key)).map_err(crate::invalid)?;
            let data = self.store.put_file_with_publication(&data_key, &data_path).await.map_err(|error| rtdb_types::Anomaly::from(error)
                .with_context("key", data_key.to_string().into()).with_context("operation", "put_file".into()))?;
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            let metadata_key = ObjectKey::new(entry.slug.metadata_key(&key)).map_err(crate::invalid)?;
            let meta = self.store.put_file_with_publication(&metadata_key, &meta_path).await.map_err(|error| rtdb_types::Anomaly::from(error)
                .with_context("key", metadata_key.to_string().into()).with_context("operation", "put_file".into()))?;
            let artefacts = self.artefacts_of(&snapshot, &job.table, &key, &data_path, size, cancellation).await;
            let published_files = if data.is_some() || meta.is_some() {
                Some(TrieFileVersions { data: data.map(Into::into), metadata: meta.map(Into::into) })
            } else { None };
            tries.push(TrieDetails { table_name: Some(entry.table.schema_and_table()), trie_key: Some(key.to_string()), data_file_size: Some(i64::try_from(size).map_err(crate::invalid)?), trie_metadata: Some(metadata), artefacts, published_files, ..Default::default() });
        }
        Ok(TriesAdded { storage_version: Some(self.version.0), storage_epoch: Some(self.epoch.0), tries, ..Default::default() })
    }
}

impl MergeFile {
    fn new(path: PathBuf, recency: Option<CivilDate>, carries_recency: bool, part: &[u8], put: &Option<StructFields<LogicalType>>, keyed: bool,
        topology: bool, include_origins: bool, include_positions: bool, redacted_leg: bool, config: &MergeConfig, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let schema = OutputSchema { put: put.clone(), iid_encoding: (config.run_end_encoded_iids && recency.is_some()).then_some(RunEndWidth::I32), recency: carries_recency, keyed, topology, include_origins, include_positions, redacted_leg };
        let directory = path.parent().ok_or_else(|| crate::invalid("Merge path has no parent"))?;
        let writer = ArrowFileWriter::new_bounded_reserved(create_file(&path)?, schema.schema()?.as_ref(), disk_remaining(directory, config.temporary_byte_limit)?, memory)?;
        Ok(Self { path, part: part.to_vec(), recency, carries_recency, keyed, topology, include_origins, include_positions, redacted_leg, writer: Some(writer), leaves: Vec::new() })
    }
    fn write(&mut self, events: &[PageEvent], path: Vec<u8>, put: &Option<StructFields<LogicalType>>, config: &MergeConfig,
        memory: &Arc<MemoryBudget>) -> Result<()> {
        if events.is_empty() { return Ok(()); }
        let schema = OutputSchema { put: put.clone(), iid_encoding: (config.run_end_encoded_iids && self.recency.is_some()).then_some(RunEndWidth::I32), recency: self.carries_recency, keyed: self.keyed, topology: self.topology, include_origins: self.include_origins, include_positions: self.include_positions, redacted_leg: self.redacted_leg };
        let writer = self.writer.as_mut().ok_or_else(|| crate::invalid("Merge file already ended"))?;
        limit_writer(writer, self.path.parent().ok_or_else(|| crate::invalid("Merge path has no parent"))?, config.temporary_byte_limit)?;
        let batch = schema.batch(events, memory)?;
        writer.write_event_page(batch.batch(), memory)?;
        self.leaves.push(layout::Leaf { page: self.leaves.len(), path, rows: events.len(), min_recency: events.iter().filter_map(|event| event.recency).min(), max_recency: events.iter().filter_map(|event| event.recency).max() });
        Ok(())
    }
    fn finish(&mut self, config: &MergeConfig) -> Result<()> {
        let mut writer = self.writer.take().ok_or_else(|| crate::invalid("Merge file already ended"))?;
        limit_writer(&mut writer, self.path.parent().ok_or_else(|| crate::invalid("Merge path has no parent"))?, config.temporary_byte_limit)?;
        writer.finish()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_format::data::StoredOperation;
    use rtdb_format::data::{StoredEvent, build_event_batch};
    use rtdb_types::{Iid, Value};

    struct WholeInput(bytes::Bytes);

    #[async_trait]
    impl ObjectStore for WholeInput {
        fn supports_scan_ranges(&self, _: &ObjectKey) -> bool { false }

        async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<rtdb_storage::StoredObject> {
            Ok(rtdb_storage::StoredObject { key: key.clone(), size: self.0.len() as u64 })
        }

        fn whole_object_allocation(&self, _: &ObjectKey, length: usize) -> rtdb_storage::Result<rtdb_storage::ObjectReadAllocation> {
            Ok(rtdb_storage::ObjectReadAllocation { retained_bytes: length, scratch_bytes: length })
        }

        async fn get_reserved_object(&self, key: &ObjectKey, reservation: ReadReservation) -> rtdb_storage::Result<rtdb_storage::RetainedBytes> {
            assert!(reservation.covers(&self.whole_object_allocation(key, self.0.len())?));
            reservation.finish(self.0.clone())
        }

        async fn get(&self, _: &ObjectKey) -> rtdb_storage::Result<bytes::Bytes> { unreachable!() }
        async fn get_range(&self, _: &ObjectKey, _: std::ops::Range<u64>) -> rtdb_storage::Result<bytes::Bytes> { unreachable!() }
        async fn put(&self, _: &ObjectKey, _: bytes::Bytes) -> rtdb_storage::Result<()> { unreachable!() }
        async fn put_file(&self, _: &ObjectKey, _: &Path) -> rtdb_storage::Result<()> { unreachable!() }
        async fn list(&self, _: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<rtdb_storage::StoredObject>> { unreachable!() }
        async fn delete(&self, _: &ObjectKey) -> rtdb_storage::Result<()> { unreachable!() }
    }

    #[tokio::test]
    async fn compaction_downloads_whole_only_inputs_under_a_complete_reservation() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("input.arrow");
        let input = WholeInput(bytes::Bytes::from(vec![7; 2 << 20]));
        let memory = MemoryBudget::new(4 << 20);
        let mut file = download(&input, &memory, &ObjectKey::new("input.arrow")?, &path, &CancellationToken::new()).await?;
        let mut body = Vec::new();
        file.read_to_end(&mut body).map_err(crate::invalid)?;
        assert_eq!(body.as_slice(), input.0.as_ref());
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    fn page(events: &[StoredEvent]) -> Vec<PageEvent> { EventBatch::new(build_event_batch(events, None, None).unwrap()).unwrap().page_events().unwrap() }

    #[test]
    fn topology_pages_keep_original_positions_and_positionless_readmission() {
        let memory = MemoryBudget::new(1 << 24);
        let id = rtdb_types::EntityId::Uuid([1; 16]);
        let record = StructFields::from([("_id".into(), id.as_value())]);
        let put = Some(StructFields::from([("_id".into(), id.as_value().logical_type())]));
        let schema = OutputSchema {
            put, iid_encoding: None, recency: false, keyed: false, topology: true,
            include_origins: false, include_positions: false, redacted_leg: false,
        };
        for position in [0, 7] {
            let stored = StoredEvent {
                position: None, origin: None, commit_position: position, entity_key: None,
                iid: id.iid(), system_from: Timestamp(10), valid_time: Interval::ALL,
                operation: StoredOperation::Put(record.clone()), recency: None,
            };
            let input = EventBatch::new(build_event_batch(&[stored], schema.put.clone(), None).unwrap()).unwrap();
            let batch = schema.batch(&input.page_events().unwrap(), &memory).unwrap();
            assert_eq!(batch.batch().schema(), schema.schema().unwrap());
            let view = EventBatch::new(batch.batch().clone()).unwrap();
            assert_eq!(view.header(0).unwrap().commit_position, position);
        }
    }

    #[test]
    fn normalization_retains_governed_event_keys_and_positions_in_the_output_file() {
        let directory = tempfile::tempdir().unwrap();
        let spill = directory.path().join("merge.arrow");
        let data = directory.path().join("data.arrow");
        let metadata = directory.path().join("meta.arrow");
        let memory = MemoryBudget::new(1 << 22);
        let config = MergeConfig::default();
        let events = [(b"world-a".as_slice(), 2), (b"world-b".as_slice(), 1)].map(|(key, commit_position)| StoredEvent { position: None,
            origin: None,
            commit_position,
            entity_key: Some(Arc::from(key)), iid: Iid([1; 16]), system_from: Timestamp(100),
            valid_time: Interval::ALL, operation: StoredOperation::Delete, recency: None,
        });
        let mut spill = MergeFile::new(spill, None, false, &[], &None, true, false, false, false, false, &config, &memory).unwrap();
        spill.write(&page(&events), vec![], &None, &config, &memory).unwrap();
        spill.finish(&config).unwrap();
        layout::normalize(&spill, &None, &config, &memory, &data, &metadata, &CancellationToken::new()).unwrap();
        let (mut reader, _reservation) = open_input(File::open(data).unwrap(), &memory).unwrap();
        let batch = EventBatch::new(read_page(&mut reader, 0, &memory).unwrap().batch().clone()).unwrap();
        assert_eq!(batch.entity_key(0).unwrap(), Some(b"world-a".as_slice()));
        assert_eq!(batch.entity_key(1).unwrap(), Some(b"world-b".as_slice()));
        assert_eq!(batch.header(0).unwrap().commit_position, 2);
        assert_eq!(batch.header(1).unwrap().commit_position, 1);
    }

    #[test]
    fn same_time_compaction_keeps_fully_superseded_landing_assertions_until_erasure() {
        let memory = MemoryBudget::new(1 << 20);
        let page = |events: &[StoredEvent]| {
            let fields = StructFields::from([("payload".into(), LogicalType::Scalar(rtdb_types::ScalarType::Utf8))]);
            EventBatch::new(build_event_batch(events, Some(fields), None).unwrap()).unwrap().page_events().unwrap()
        };
        let assertions = [2, 1].map(|commit_position| StoredEvent { position: None,
            origin: None,
            commit_position,
            entity_key: Some(Arc::from(b"entity".as_slice())),
            iid: Iid([1; 16]),
            system_from: Timestamp(100),
            valid_time: Interval::from_micros(10, 20).unwrap(),
            operation: if commit_position == 2 {
                StoredOperation::Put(StructFields::from([("payload".into(), rtdb_types::Value::Utf8("erase me".into()))]))
            } else {
                StoredOperation::Delete
            },
            recency: None,
        });
        let (surviving, _charges) = resolve_same_time(&page(&assertions), &memory).unwrap();
        assert_eq!(
            surviving.iter().map(EncodedEvent::header).collect::<Vec<_>>(),
            assertions.iter().map(StoredEvent::header).collect::<Vec<_>>(),
        );
        let erased = StoredEvent { position: None,
            origin: None,
            system_from: Timestamp(101),
            commit_position: 0,
            valid_time: Interval::ALL,
            operation: StoredOperation::Erase,
            ..assertions[0].clone()
        };
        let erased_header = erased.header();
        let [later, earlier] = assertions;
        let colliding = StoredEvent { position: None,
            origin: None,
            entity_key: Some(Arc::from(b"colliding-entity".as_slice())),
            system_from: Timestamp(99),
            ..earlier.clone()
        };
        let colliding_header = colliding.header();
        let (surviving, _charges) = resolve_same_time(&page(&[erased, later, earlier, colliding]), &memory).unwrap();
        assert_eq!(surviving.iter().map(EncodedEvent::header).collect::<Vec<_>>(), [erased_header, colliding_header]);
    }

    #[test]
    fn compaction_keeps_redacted_extent_and_origin_and_preserves_older_history() {
        use rtdb_types::{BlockIndex, EventOrigin};

        let directory = tempfile::tempdir().unwrap();
        let memory = MemoryBudget::new(1 << 22);
        let config = MergeConfig::default();
        let id = rtdb_types::EntityId::Integer(1);
        let redacted = StoredEvent { position: None,
            origin: Some(EventOrigin::at([1; 16], 2, BlockIndex(1))),
            entity_key: Some(id.handle_bytes().into()),
            iid: id.iid(),
            commit_position: 2,
            system_from: Timestamp(100),
            valid_time: Interval::from_micros(10, 20).unwrap(),
            operation: StoredOperation::Redacted,
            recency: None,
        };
        let earlier = StoredEvent { position: None,
            origin: Some(EventOrigin::at([1; 16], 1, BlockIndex(1))),
            commit_position: 1,
            valid_time: Interval::ALL,
            operation: StoredOperation::Delete,
            ..redacted.clone()
        };
        let (normalized, _charges) = resolve_same_time(&page(&[redacted.clone(), earlier.clone()]), &memory).unwrap();
        assert_eq!(normalized.iter().map(EncodedEvent::header).collect::<Vec<_>>(), [redacted.header(), earlier.header()]);
        let mut spill = MergeFile::new(
            directory.path().join("merge.arrow"), None, false, &[], &None, true, false, true, false, true, &config, &memory,
        ).unwrap();
        spill.write(&normalized[..1], vec![], &None, &config, &memory).unwrap();
        spill.write(&normalized[1..], vec![], &None, &config, &memory).unwrap();
        spill.finish(&config).unwrap();
        let (mut reader, _reservation) = open_input(File::open(spill.path).unwrap(), &memory).unwrap();
        for (index, expected) in [redacted, earlier].into_iter().enumerate() {
            let batch = read_page(&mut reader, index, &memory).unwrap();
            let events = EventBatch::new(batch.batch().clone()).unwrap();
            assert_eq!(events.read_event(0).unwrap(), expected);
        }
    }

    #[test]
    fn compaction_retains_the_ordered_prefix_at_a_tied_sql_time() {
        use rtdb_types::{EventPosition, TxId};
        let memory = MemoryBudget::new(1 << 20);
        let events = [12, 11].map(|transaction| StoredEvent {
            position: Some(EventPosition { transaction: TxId(transaction), ordinal: 0 }),
            origin: None,
            commit_position: 0,
            entity_key: None,
            iid: Iid([1; 16]),
            system_from: Timestamp(100),
            valid_time: Interval::ALL,
            operation: StoredOperation::Put(StructFields::from([("value".into(), Value::I64(transaction))])),
            recency: None,
        });
        let fields = StructFields::from([("value".into(), LogicalType::Scalar(rtdb_types::ScalarType::I64))]);
        let input = EventBatch::new(build_event_batch(&events, Some(fields), None).unwrap()).unwrap();
        let (surviving, _charges) = resolve_same_time(&input.page_events().unwrap(), &memory).unwrap();
        assert_eq!(surviving.iter().map(EncodedEvent::header).collect::<Vec<_>>(), events.map(|event| event.header()));
    }

    #[test]
    fn same_time_compaction_keeps_temporal_state_separate_for_colliding_keys() {
        let memory = MemoryBudget::new(1 << 20);
        let events = [b"world-a".as_slice(), b"world-b".as_slice()].map(|key| StoredEvent { position: None,
            origin: None,
            commit_position: 0,
            entity_key: Some(Arc::from(key)), iid: Iid([1; 16]), system_from: Timestamp(100),
            valid_time: Interval::ALL, operation: StoredOperation::Delete, recency: None,
        });
        let input = page(&events);
        let (rows, charges) = resolve_same_time(&input, &memory).unwrap();
        assert_eq!(rows.iter().map(PageEvent::entity_key).collect::<Vec<_>>(), [Some(b"world-a".as_slice()), Some(b"world-b".as_slice())]);
        drop((rows, charges));
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn compaction_reserves_encoding_scratch_before_writing_a_page() {
        let fields = StructFields::from([("payload".into(), LogicalType::Scalar(rtdb_types::ScalarType::Binary))]);
        let input = build_event_batch(&[StoredEvent { position: None,
            origin: None,
            commit_position: 0,
            entity_key: None,
            iid: Iid([1; 16]), system_from: Timestamp(100), valid_time: Interval::ALL, recency: None,
            operation: StoredOperation::Put(StructFields::from([("payload".into(), rtdb_types::Value::Binary(vec![7; 1 << 20]))])),
        }], Some(fields), None).unwrap();
        let file = tempfile::tempfile().unwrap();
        let memory = MemoryBudget::new(1 << 20);
        let mut writer = ArrowFileWriter::new_reserved(file, input.schema_ref(), &memory).unwrap();
        let position = writer.bytes_written();
        let before = memory.used_bytes();

        let error = writer.write_event_page(&input, &memory).unwrap_err();

        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(writer.bytes_written(), position);
        assert_eq!(memory.used_bytes(), before);
    }

    #[test]
    fn row_heap_matches_flatten_and_sort_without_losing_occurrences() {
        use std::cmp::Reverse;

        let iids = [
            Iid([0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255]),
            Iid([128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
        ];
        let events: Vec<_> = iids.into_iter().flat_map(|iid| {
            [i64::MAX - 1, i64::MIN + 1].map(|system_from| StoredEvent { position: None, origin: None, commit_position: 0, entity_key: None,
                iid,
                system_from: Timestamp(system_from),
                valid_time: Interval::from_micros(10, 20).unwrap(),
                operation: StoredOperation::Delete,
                recency: None,
            })
        }).collect();

        let mut pages = vec![Vec::new()];
        for (index, event) in events.iter().enumerate() {
            pages.push(page(std::slice::from_ref(event)));
            for later in &events[index..] {
                pages.push(page(&[event.clone(), later.clone()]));
            }
        }

        let key = |event: &PageEvent| (event.iid, Reverse(event.system_from));
        for count in 0..=4 {
            for mut choice in 0..pages.len().pow(count) {
                let streams: Vec<_> = (0..count).map(|source_precedence| {
                    let events = pages[choice % pages.len()].clone();
                    choice /= pages.len();
                    planner::PageRows { source_precedence: source_precedence as usize, events }
                }).collect();
                let lengths: Vec<_> = streams.iter().map(|page| page.events.len()).collect();
                let mut expected: Vec<_> = streams.iter().flat_map(|page| page.events.iter()).map(key).collect();
                expected.sort();

                let mut heap = planner::RowHeap::new(&streams, planner::TieOrder::Scan).unwrap();
                let mut positions = vec![0; streams.len()];
                let mut observed = Vec::with_capacity(expected.len());
                while let Some((stream, row)) = heap.next() {
                    assert_eq!(row, positions[stream], "merge skipped or repeated a row in a stream");
                    positions[stream] += 1;
                    observed.push(key(&streams[stream].events[row]));
                }
                assert_eq!(positions, lengths, "merge did not consume every input occurrence");
                assert_eq!(observed, expected, "merge order differs from sorting the input");
            }
        }

        for [later, earlier] in [[&events[1], &events[0]], [&events[2], &events[0]]] {
            let unsorted = [planner::PageRows { source_precedence: 0, events: page(&[later.clone(), earlier.clone()]) }];
            let error = planner::RowHeap::new(&unsorted, planner::TieOrder::Scan).err().unwrap();
            assert_eq!(error.message.as_deref(), Some("Compaction page is not IID ascending and system-position descending"));
        }
    }

    #[test]
    fn same_time_compaction_preserves_the_winning_put_or_delete_at_each_valid_time() {
        use rtdb_types::{ScalarType, Value};

        let intervals = [(0, 1), (0, 2), (0, 3), (1, 1), (1, 2), (1, 3), (2, 3)];
        let choices = intervals.len() * 2;
        let ordinal_type = LogicalType::Scalar(ScalarType::I64);
        let fields = StructFields::from([("ordinal".into(), ordinal_type.clone())]);
        let contains = |span: Interval, time: i64| span.lower().0 <= time && time < span.upper().0;
        let memory = MemoryBudget::new(1 << 20);

        for count in 0..=3 {
            for mut choice in 0..choices.pow(count) {
                let commands: Vec<_> = (0..count).map(|ordinal| {
                    let selected = choice % choices;
                    choice /= choices;
                    let (lower, upper) = intervals[selected / 2];
                    let operation = if selected % 2 == 0 {
                        StoredOperation::Put(StructFields::from([("ordinal".into(), Value::I64(i64::from(ordinal)))]))
                    } else { StoredOperation::Delete };
                    StoredEvent { position: None,
                        origin: None,
                        commit_position: 0,
                        entity_key: None,
                        iid: Iid([1; 16]),
                        system_from: Timestamp(100),
                        valid_time: Interval::from_micros(lower, upper).unwrap(),
                        operation,
                        recency: None,
                    }
                }).collect();
                let input = build_event_batch(&commands, Some(fields.clone()), None).unwrap();
                let input = EventBatch::new(input).unwrap().page_events().unwrap();
                let (resolved, charges) = resolve_same_time(&input, &memory).unwrap();

                for instant in -1..=4 {
                    let expected = commands.iter().find(|row| contains(row.valid_time, instant)).map(|row| match &row.operation {
                        StoredOperation::Put(document) => Some(document.get("ordinal").unwrap().clone()),
                        StoredOperation::Delete => None,
                        StoredOperation::Erase | StoredOperation::Redacted => unreachable!(),
                    });
                    let mut covering = resolved.iter().filter(|row| contains(row.valid_time, instant));
                    let observed = covering.next().map(|row| match row.op {
                        EventOp::Put => Some(row.put_cell("ordinal", &ordinal_type).unwrap().into_owned()),
                        EventOp::Delete => None,
                        EventOp::Erase | EventOp::Redacted => unreachable!(),
                    });
                    assert!(covering.next().is_none(), "resolved same-time fragments overlap at {instant}");
                    assert_eq!(observed, expected, "same-time resolution changed the winning command at {instant}");
                }

                drop((resolved, charges));
                assert_eq!(memory.used_bytes(), 0);
            }
        }
    }

    #[test]
    fn same_time_compaction_resolves_a_lone_event_with_only_its_output_row() {
        let input = page(&[StoredEvent { position: None,
            origin: None,
            commit_position: 0,
            entity_key: None,
            iid: Iid([1; 16]),
            system_from: Timestamp(100),
            valid_time: Interval::from_micros(10, 20).unwrap(),
            operation: StoredOperation::Delete,
            recency: None,
        }]);
        let output = row_bytes(&input[0]).unwrap() * 2;
        let memory = MemoryBudget::new(output - 1);
        let error = resolve_same_time(&input, &memory).err().unwrap();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(memory.used_bytes(), 0);

        let memory = MemoryBudget::new(output);
        let (rows, charges) = resolve_same_time(&input, &memory).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].header(), input[0].header());
        assert_eq!(memory.used_bytes(), output);
        drop((rows, charges));
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn same_time_compaction_admits_the_ceiling_before_cloning_a_second_events_row() {
        let event = |lower, upper| StoredEvent { position: None,
            origin: None,
            commit_position: 0,
            entity_key: None,
            iid: Iid([1; 16]),
            system_from: Timestamp(100),
            valid_time: Interval::from_micros(lower, upper).unwrap(),
            operation: StoredOperation::Delete,
            recency: None,
        };
        let input = page(&[event(10, 20), event(30, 40)]);
        let ceiling = 7 * std::mem::size_of::<Timestamp>() + 256;
        let output = row_bytes(&input[0]).unwrap() * 2;
        let limit = output + ceiling - 1;
        let memory = MemoryBudget::new(limit);
        let error = resolve_same_time(&input, &memory).err().unwrap();
        let expected = format!("Cannot reserve {ceiling} bytes from budget {limit} with {output} retained");
        assert_eq!(error.message.as_deref(), Some(expected.as_str()));
        assert_eq!(memory.used_bytes(), 0);
    }
}
