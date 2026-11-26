mod layout;
mod planner;
mod row_heap;

use rtdb_types::StructFields;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use async_trait::async_trait;
use arrow_schema::{DataType, SchemaRef};
use bytes::Bytes;
use rtdb_types::CivilDate;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch, from_field};
use rtdb_format::{data::{EncodedEvent, EventBatch, PageEvent, compacted_event_schema, copy_compaction_batch}, ipc::{ArrowFile, ArrowFileWriter, inspect_file_reserved}, keys::TrieKey,
    log::proto::{TriesAdded, TrieDetails, TrieFileVersions}, metadata::MetadataFile};
use rtdb_index::{PartitionValue, TrieState};
use rtdb_storage::{HeldView, IoRegion, LocalFile, ObjectKey, ObjectStore, ReadReservation, RetainedBytes};
use rtdb_temporal::AdmittedPolygonCalculator;
use rtdb_types::{EventOp, Iid, Interval, LogicalType, Result, RunEndWidth, ScalarType, StorageEpoch, StorageVersion, TableRef, Timestamp};
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

struct InputSegment { key: TrieKey, data: PrimaryReader }

struct InputMetadata {
    file: MetadataFile,
    _reservation: Arc<MemoryReservation>,
}

struct InputBytes(RetainedBytes);

impl AsRef<[u8]> for InputBytes {
    fn as_ref(&self) -> &[u8] { self.0.as_slice() }
}

const RETAINED_INPUT_METADATA_BYTES: usize = 1024 * 1024;

struct MergeOutput {
    path: PathBuf,
    part: Vec<u8>,
    recency: Option<CivilDate>,
    schema: OutputSchema,
    pages: MergePages,
    leaves: Vec<layout::Leaf>,
}

enum MergePages {
    Retained(RetainedPages),
    Writing(ArrowFileWriter),
    Spilled,
}

#[derive(Default)]
struct RetainedPages {
    batches: Vec<OwnedBatch>,
    bytes: usize,
}

struct MergePlan {
    tasks: Vec<planner::Task>,
    tie_order: planner::TieOrder,
    source_precedence: Vec<usize>,
}

const RETAINED_MERGE_BYTES: usize = 2 * 1024 * 1024;
const RETAINED_MERGE_TASKS: usize = 16;

#[derive(Clone)]
struct OutputSchema { put: Option<Arc<StructFields<LogicalType>>>, iid_encoding: Option<RunEndWidth>, recency: bool, keyed: bool, topology: bool, include_origins: bool, include_positions: bool, redacted_leg: bool }

impl OutputSchema {
    fn supports_retention(&self) -> bool {
        // The spill codec can rewrite nested union slots before the final writer reads them.
        self.put.as_ref().is_none_or(|fields| {
            fields.iter().all(|(_, ty)| {
                let ty = match ty { LogicalType::Maybe(inner) => inner.as_ref(), _ => ty };
                matches!(ty, LogicalType::Scalar(ScalarType::Bool | ScalarType::I8 | ScalarType::I16
                    | ScalarType::I32 | ScalarType::I64 | ScalarType::U8 | ScalarType::U16
                    | ScalarType::U32 | ScalarType::U64 | ScalarType::F32 | ScalarType::F64
                    | ScalarType::Utf8 | ScalarType::Binary | ScalarType::FixedSizeBinary(1..)
                    | ScalarType::DateDay | ScalarType::DateMilli | ScalarType::Time { .. }
                    | ScalarType::Timestamp { .. } | ScalarType::Duration(_) | ScalarType::Uuid))
            })
        })
    }

    fn batch<E: EncodedEvent>(&self, events: &[E], memory: &Arc<MemoryBudget>) -> Result<OwnedBatch> {
        let batch = copy_compaction_batch(events, self.put.as_deref().cloned(), self.iid_encoding, self.recency, self.redacted_leg, memory)?;
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
        let schema = compacted_event_schema(self.put.as_deref().cloned(), self.iid_encoding, self.recency, self.redacted_leg)?;
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

fn remove_temporary_file(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(crate::invalid(error)),
    }
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
    download_sized(store, memory, key, path, store.head(key).await?.size, cancellation).await
}

fn reserve_input_read(
    store: &dyn ObjectStore, key: &ObjectKey, count: usize, ranges: bool,
    retained_memory: &Arc<MemoryBudget>, memory: &Arc<MemoryBudget>,
) -> Result<ReadReservation> {
    let allocation = if ranges { store.scan_range_allocation(key, count)? } else { store.whole_object_allocation(key, count)? };
    let retained = retained_memory.reserve(allocation.retained_bytes.max(count))?;
    let scratch = (allocation.scratch_bytes > 0).then(|| memory.reserve(allocation.scratch_bytes)).transpose()?;
    Ok(ReadReservation::funded(count, retained, scratch))
}

async fn read_input_bytes(
    store: &dyn ObjectStore, key: &ObjectKey, position: u64, ranges: bool,
    reservation: ReadReservation, cancellation: &CancellationToken,
) -> Result<RetainedBytes> {
    if cancellation.is_cancelled() { return Err(crate::closed()); }
    let bytes = if ranges {
        store.get_reserved_scan_range(key, position..position + reservation.length() as u64, reservation).await?
    } else {
        store.get_reserved_object(key, reservation).await?
    };
    if cancellation.is_cancelled() { return Err(crate::closed()); }
    Ok(bytes)
}

async fn download_sized(
    store: &dyn ObjectStore, memory: &Arc<MemoryBudget>, key: &ObjectKey, path: &Path,
    size: u64, cancellation: &CancellationToken,
) -> Result<File> {
    if cancellation.is_cancelled() { return Err(crate::closed()); }
    let mut file = create_file(path)?;
    let ranges = store.supports_scan_ranges(key);
    let mut position = 0;
    while position < size {
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        let remaining = size - position;
        let count = usize::try_from(if ranges { remaining.min(1024 * 1024) } else { remaining }).map_err(crate::invalid)?;
        let reservation = reserve_input_read(store, key, count, ranges, memory, memory)?;
        let bytes = read_input_bytes(store, key, position, ranges, reservation, cancellation).await?;
        file.write_all(bytes.as_slice()).map_err(crate::invalid)?;
        position += count as u64;
    }
    file.seek(SeekFrom::Start(0)).map_err(crate::invalid)?;
    Ok(file)
}

fn if_memory_available<T>(result: Result<T>) -> Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.code == "rtdb/memory-budget-exceeded" => Ok(None),
        Err(error) => Err(error),
    }
}

async fn open_metadata(
    store: &dyn ObjectStore, key: &ObjectKey, path: &Path, size: u64,
    memory: &Arc<MemoryBudget>, retained_memory: &Arc<MemoryBudget>, cancellation: &CancellationToken,
) -> Result<InputMetadata> {
    if cancellation.is_cancelled() { return Err(crate::closed()); }
    let ranges = store.supports_scan_ranges(key);
    let (mut file, inspected) = if let Ok(count) = usize::try_from(size)
        && count > 0 && count <= retained_memory.limit_bytes()
        && let Some(reservation) = if_memory_available(reserve_input_read(store, key, count, ranges, retained_memory, memory))?
    {
        let bytes = read_input_bytes(store, key, 0, ranges, reservation, cancellation).await?;
        let mut cursor = Cursor::new(bytes.as_slice());
        let inspected = match if_memory_available(inspect_file_reserved(&mut cursor, |bytes| memory.reserve(bytes)))? {
            Some(layout) if layout.uncompressed => {
                if let Some(reservation) = if_memory_available(memory.reserve(layout.full_allocation_bound()?))? {
                    if cancellation.is_cancelled() { return Err(crate::closed()); }
                    let buffer = Bytes::from_owner(InputBytes(bytes)).into();
                    let file = ArrowFile::from_inspected_buffer(buffer, None, layout)?;
                    return Ok(InputMetadata { file: MetadataFile::read_file(file)?, _reservation: reservation });
                }
                Some(layout)
            }
            layout => layout,
        };
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        let mut file = create_file(path)?;
        file.write_all(bytes.as_slice()).map_err(crate::invalid)?;
        (file, inspected)
    } else {
        (download_sized(store, memory, key, path, size, cancellation).await?, None)
    };
    if cancellation.is_cancelled() { return Err(crate::closed()); }
    let layout = match inspected {
        Some(layout) => layout,
        None => inspect_file_reserved(&mut file, |bytes| memory.reserve(bytes))?,
    };
    let reservation = memory.reserve(layout.full_allocation_bound()?)?;
    let file = ArrowFile::from_inspected_reader(file, None, layout)?;
    Ok(InputMetadata { file: MetadataFile::read_file(file)?, _reservation: reservation })
}

/// An object already on this node's disk, read in place; the open file keeps its contents through a concurrent delete.
pub(crate) struct LocalInput(LocalFile);

impl Read for LocalInput {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> { let mut file = self.0.file(); file.read(buf) }
}

impl Seek for LocalInput {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> { let mut file = self.0.file(); file.seek(position) }
}

fn open_local_metadata(local: LocalFile, memory: &Arc<MemoryBudget>) -> Result<InputMetadata> {
    let mut file = LocalInput(local);
    let layout = inspect_file_reserved(&mut file, |bytes| memory.reserve(bytes))?;
    let reservation = memory.reserve(layout.full_allocation_bound()?)?;
    let file = ArrowFile::from_inspected_reader(file, None, layout)?;
    Ok(InputMetadata { file: MetadataFile::read_file(file)?, _reservation: reservation })
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

pub(crate) struct PrimaryReader {
    pub(crate) reader: ArrowFile,
    _opening: Arc<MemoryReservation>,
}

pub(crate) enum PrimaryData {
    File(File),
    Local(LocalFile),
    Retained { bytes: Bytes, spill: PathBuf },
}

pub(crate) fn primary_data_budget(memory: &Arc<MemoryBudget>) -> Arc<MemoryBudget> {
    const RETAINED_PRIMARY_BYTES: usize = 1024 * 1024;
    memory.scope_with_limit(RETAINED_PRIMARY_BYTES.min(memory.limit_bytes() / 8))
}

impl PrimaryData {
    pub(crate) async fn stored(
        store: &dyn ObjectStore, key: &ObjectKey, spill: PathBuf, size: u64,
        memory: &Arc<MemoryBudget>, retained: &Arc<MemoryBudget>, cancellation: &CancellationToken,
    ) -> Result<Self> {
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        if let Ok(count) = usize::try_from(size)
            && count > 0 && count <= retained.limit_bytes()
        {
            let ranges = store.supports_scan_ranges(key);
            if let Some(reservation) = if_memory_available(reserve_input_read(store, key, count, ranges, retained, memory))? {
                let bytes = read_input_bytes(store, key, 0, ranges, reservation, cancellation).await?;
                return Ok(Self::Retained { bytes: Bytes::from_owner(InputBytes(bytes)), spill });
            }
        }
        Ok(Self::File(download_sized(store, memory, key, &spill, size, cancellation).await?))
    }

    /// A retained read may replay its consumer, so that consumer owns all partial work.
    pub(crate) fn read<T>(
        self, memory: &Arc<MemoryBudget>, cancellation: &CancellationToken,
        mut consume: impl FnMut(PrimaryReader) -> Result<T>,
    ) -> Result<T> {
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        let file = match self {
            Self::File(file) => file,
            Self::Local(local) => {
                let (reader, opening) = open_input(LocalInput(local), memory)?;
                return consume(PrimaryReader { reader, _opening: opening });
            }
            Self::Retained { bytes, spill } => {
                let inspected = inspect_file_reserved(&mut Cursor::new(bytes.as_ref()), |bytes| memory.reserve(bytes));
                if let Some(layout) = if_memory_available(inspected)?
                    && layout.uncompressed && layout.dictionary_encoded_bytes == 0
                {
                    let result = (|| {
                        let opening = memory.reserve(layout.opening_allocation_bound()?)?;
                        let reader = ArrowFile::from_inspected_buffer(bytes.clone().into(), None, layout)?;
                        consume(PrimaryReader { reader, _opening: opening })
                    })();
                    if let Some(value) = if_memory_available(result)? { return Ok(value); }
                }
                if cancellation.is_cancelled() { return Err(crate::closed()); }
                let mut file = create_file(&spill)?;
                file.write_all(&bytes).map_err(crate::invalid)?;
                file
            }
        };
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        let (reader, opening) = open_input(file, memory)?;
        consume(PrimaryReader { reader, _opening: opening })
    }
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
        let schema = input.data.reader.schema();
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

fn same_time_key(event: &PageEvent) -> (Iid, Option<Vec<u8>>, Timestamp) {
    (event.iid, event.entity_key().map(<[u8]>::to_vec), event.system_from)
}

struct SameTimeState {
    calculator: AdmittedPolygonCalculator,
    system_from: Option<Timestamp>,
    erased: bool,
}

fn needs_same_time_resolution(events: &[PageEvent]) -> bool {
    events.windows(2).any(|pair| pair[0].iid >= pair[1].iid)
        || events.iter().any(|event| {
            event.op == EventOp::Erase || event.valid_time.is_empty() || event.system_from == Timestamp::MAX
        })
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
        let header = event.header();
        if state.system_from != Some(event.system_from) {
            state.calculator.reset();
            state.system_from = Some(event.system_from);
        }
        if header.op == EventOp::Erase {
            state.calculator.visit(&header, |_| Ok(()))?;
            state.erased = true;
            charges.push(memory.reserve(row_bytes(event)?.checked_mul(2).ok_or_else(|| crate::invalid("Resolved row allocation overflow"))?)?);
            let mut marker = event.clone();
            marker.valid_time = Interval::ALL;
            result.push(marker);
            continue;
        }
        state.calculator.visit(&header, |polygon| {
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
        let view = HeldView::catalog_at(self.store.as_ref(), snapshot.clone(), captured.catalog_cut(), captured.catalog_checkpoint()?).await?;
        IoRegion::reading(view, self.execute_job(job, snapshot, cancellation)).await
    }
}

impl SegmentCompactor {
    async fn execute_job(&self, job: &CompactionJob, snapshot: Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<TriesAdded> {
        let entry = snapshot.table_catalog().entry(&job.table).cloned().ok_or_else(|| crate::invalid("Compaction table has no recorded slug"))?;
        let mut retained_data = primary_data_budget(&self.memory);
        let mut retained_metadata = self.memory.scope_with_limit(RETAINED_INPUT_METADATA_BYTES.min(self.memory.limit_bytes() / 8));
        let (directory, mut outputs) = loop {
            let directory = tempfile::Builder::new().prefix("rtdb-compaction-").tempdir_in(&self.config.temporary_directory).map_err(crate::invalid)?;
            match self.merge_inputs(job, &entry.slug, directory.path(), &retained_data, &retained_metadata, cancellation).await {
                Err(error) if error.code == "rtdb/memory-budget-exceeded"
                    && (retained_data.peak_bytes() > 0 || retained_metadata.peak_bytes() > 0) => {
                    if cancellation.is_cancelled() { return Err(crate::closed()); }
                    directory.close().map_err(crate::invalid)?;
                    retained_data = self.memory.scope_with_limit(0);
                    retained_metadata = self.memory.scope_with_limit(0);
                }
                result => break (directory, result?),
            }
        };
        for output in &mut outputs { output.finish(&self.config, &self.memory, cancellation)?; }
        let mut outputs = outputs.into_iter().enumerate();
        let initial = outputs.next();
        let mut tries = Vec::new();
        for (index, mut output) in outputs.chain(initial) {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            let key = TrieKey::new(job.output.level(), output.recency, job.output.part().to_vec(), job.output.block_index())?.with_epoch(job.output.epoch());
            let data_path = directory.path().join(format!("output-{index}.arrow"));
            let meta_path = directory.path().join(format!("output-{index}.meta.arrow"));
            let (size, metadata) = output.normalize(&self.config, &self.memory, &data_path, &meta_path, cancellation)?;
            drop(output);
            check_disk(directory.path(), self.config.temporary_byte_limit, 0)?;
            if cancellation.is_cancelled() { return Err(crate::closed()); }
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

    async fn merge_inputs(&self, job: &CompactionJob, slug: &rtdb_format::keys::TableSlug, directory: &Path,
        retained_data: &Arc<MemoryBudget>, retained_metadata: &Arc<MemoryBudget>, cancellation: &CancellationToken) -> Result<Vec<MergeOutput>> {
        let mut inputs = Vec::new();
        let mut metadata = Vec::new();
        let mut input_bytes = 0u64;
        for (index, key) in job.inputs.iter().enumerate() {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            let data_key = ObjectKey::new(slug.data_key(key)).map_err(crate::invalid)?;
            let meta_key = ObjectKey::new(slug.metadata_key(key)).map_err(crate::invalid)?;
            let data_local = self.store.cached_local_file(&data_key).await.map_err(crate::invalid)?;
            let metadata_local = self.store.cached_local_file(&meta_key).await.map_err(crate::invalid)?;
            let data_size = if data_local.is_some() { 0 } else { self.store.head(&data_key).await.map_err(crate::invalid)?.size };
            let metadata_size = if metadata_local.is_some() { 0 } else { self.store.head(&meta_key).await.map_err(crate::invalid)?.size };
            input_bytes = input_bytes.checked_add(data_size).and_then(|bytes| bytes.checked_add(metadata_size))
                .ok_or_else(|| crate::invalid("Input file size overflow"))?;
            if input_bytes > self.config.temporary_byte_limit {
                return Err(crate::invalid("Compaction temporary disk budget exceeded"));
            }
            let data = match data_local {
                Some(local) => PrimaryData::Local(local),
                None => PrimaryData::stored(self.store.as_ref(), &data_key, directory.join(format!("input-{index}.arrow")),
                    data_size, &self.memory, retained_data, cancellation).await?,
            };
            metadata.push(match metadata_local {
                Some(local) => open_local_metadata(local, &self.memory)?,
                None => open_metadata(self.store.as_ref(), &meta_key, &directory.join(format!("meta-{index}.arrow")),
                    metadata_size, &self.memory, retained_metadata, cancellation).await?,
            });
            let data = data.read(&self.memory, cancellation, Ok)?;
            inputs.push(InputSegment { key: key.clone(), data });
        }
        let keyed = job.table.is_owned_event_data();
        let topology = job.table.is_owned_world_topology();
        let include_origins = inputs.iter().any(|input| input.data.reader.schema().index_of(rtdb_format::data::EVENT_ORIGIN_FIELD).is_ok());
        if inputs.iter().any(|input| input.data.reader.schema().index_of(rtdb_format::data::ENTITY_KEY_FIELD).is_ok() != keyed) {
            return Err(crate::invalid("Compaction input does not have the required entity-key layout"));
        }
        let include_positions = inputs.iter().any(|input| input.data.reader.schema().index_of(rtdb_format::data::EVENT_POSITION_FIELD).is_ok());
        if include_positions && inputs.iter().any(|input| input.data.reader.schema().index_of(rtdb_format::data::EVENT_POSITION_FIELD).is_err()) {
            return Err(crate::invalid("Positioned history cannot mix with unpositioned primary pages"));
        }
        let redacted_leg = inputs.iter().any(|input| input.data.reader.schema().field_with_name("op").is_ok_and(|op| {
            matches!(op.data_type(), DataType::Union(fields, _) if fields.iter().any(|(_, field)| field.name() == "redacted"))
        }));
        let put = merged_put_schema(&inputs)?.map(Arc::new);
        let carry = self.config.within_file_recency_branching && job.output.recency().is_some() && inputs.iter().all(|input| input.data.reader.schema().index_of("_recency").is_ok());
        let tasks = planner::plan(&metadata, &job.part)?;
        drop(metadata);
        let tie_order = self.config.tie_order(job.output.epoch());
        let mut sources: Vec<_> = (0..inputs.len()).collect();
        sources.sort_by(|&left, &right| inputs[left].key.scan_precedence().cmp(&inputs[right].key.scan_precedence()));
        let mut source_precedence = vec![0; inputs.len()];
        for (priority, source) in sources.into_iter().enumerate() {
            source_precedence[source] = priority;
        }
        let initial_recency = if job.recency_mode == RecencyMode::Partition { None } else { job.output.recency() };
        let initial_schema = OutputSchema {
            put, iid_encoding: (self.config.run_end_encoded_iids && initial_recency.is_some()).then_some(RunEndWidth::I32),
            recency: job.recency_mode == RecencyMode::Preserve && carry, keyed, topology, include_origins, include_positions, redacted_leg,
        };
        let retain_multiple = job.recency_mode == RecencyMode::Preserve
            && (2..=RETAINED_MERGE_TASKS).contains(&tasks.len());
        let mut retain = initial_schema.supports_retention() && (tasks.len() == 1 || retain_multiple);
        let plan = MergePlan { tasks, tie_order, source_precedence };
        let initial_path = directory.join("merged-0.arrow");
        loop {
            let mut initial = MergeOutput::pending(initial_path.clone(), initial_recency, &job.part, initial_schema.clone());
            if !retain {
                initial.start_spill(&self.config, &self.memory, cancellation)?;
            }
            match self.merge_tasks(job, &mut inputs, &plan, initial, cancellation).await {
                // The next task can run out of scratch before it can spill the retained pages.
                Err(error) if retain && retain_multiple && error.code == "rtdb/memory-budget-exceeded" => {
                    if cancellation.is_cancelled() { return Err(crate::closed()); }
                    remove_temporary_file(&initial_path)?;
                    retain = false;
                }
                result => return result,
            }
        }
    }

    async fn merge_tasks(&self, job: &CompactionJob, inputs: &mut [InputSegment], plan: &MergePlan, initial: MergeOutput,
        cancellation: &CancellationToken) -> Result<Vec<MergeOutput>> {
        let positioned_history = TableRef::owned_schema(&job.table.schema);
        let mut outputs = vec![initial];
        for task in &plan.tasks {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            let mut rows = Vec::new();
            let mut charges = Vec::new();
            let mut erase_times = BTreeMap::new();
            for page in &task.pages {
                let input = &mut inputs[page.segment];
                let batch = read_page(&mut input.data.reader, page.page, &self.memory)?;
                let _decoded = self.memory.reserve(decoded_bound(batch.batch())?)?;
                let mut events = EventBatch::new(batch.batch().clone())?.page_events()?;
                if input.key.level() == 0 && !positioned_history && needs_same_time_resolution(&events) {
                    let (resolved, reservations) = resolve_same_time(&events, &self.memory)?;
                    events = resolved;
                    charges.extend(reservations);
                }
                let path = if job.part.len() > task.path.len() { &job.part } else { &task.path };
                events.retain(|event| planner::matches_path(event.iid, path));
                if job.recency_mode == RecencyMode::Partition {
                    for event in events.iter().filter(|event| event.header().op == EventOp::Erase) {
                        if let std::collections::btree_map::Entry::Vacant(entry) = erase_times.entry(same_time_key(event)) {
                            entry.insert((self.memory.reserve(512)?, 0usize));
                        }
                    }
                }
                charges.push(event_reservation(&self.memory, &events, 2)?);
                rows.push(planner::PageRows { source_precedence: plan.source_precedence[page.segment], events });
            }
            for event in rows.iter().flat_map(|page| &page.events) {
                if let Some((_, events)) = erase_times.get_mut(&same_time_key(event)) { *events += 1; }
            }
            erase_times.retain(|_, (_, events)| *events > 1);
            let mut heap = planner::RowHeap::new(&rows, plan.tie_order)?;
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
                // Splitting an erase from same-time events lets trie precedence override operation order.
                if erase_times.contains_key(&same_time_key(event)) {
                    recency = Timestamp::MAX;
                } else if event.position.is_some() || event.commit_position != 0 || event.op == EventOp::Redacted {
                    recency = recency.max(event.system_from);
                }
                let target = if job.recency_mode == RecencyMode::Partition && recency != Timestamp::MAX {
                    let date = recency_partition(recency, self.config.recency_partition)?;
                    if let Some(index) = outputs.iter().position(|output| output.recency == Some(date)) { index }
                    else {
                        if matches!(outputs[0].pages, MergePages::Retained(_)) {
                            outputs[0].start_spill(&self.config, &self.memory, cancellation)?;
                        }
                        let index = outputs.len();
                        let schema = OutputSchema {
                            iid_encoding: self.config.run_end_encoded_iids.then_some(RunEndWidth::I32),
                            recency: self.config.within_file_recency_branching,
                            ..outputs[0].schema.clone()
                        };
                        let directory = outputs[0].path.parent().ok_or_else(|| crate::invalid("Merge path has no parent"))?;
                        let mut output = MergeOutput::pending(directory.join(format!("merged-{index}.arrow")), Some(date), &job.part, schema);
                        output.start_spill(&self.config, &self.memory, cancellation)?;
                        outputs.push(output);
                        index
                    }
                } else { 0 };
                bucket_charges.push(self.memory.reserve(row_bytes(event)?.checked_mul(2).ok_or_else(|| crate::invalid("Output row allocation overflow"))?)?);
                let mut event = event.clone();
                event.recency = if outputs[target].schema.recency { Some(if job.recency_mode == RecencyMode::Partition { recency } else { event.recency.ok_or_else(|| crate::invalid("Historical input lost recency"))? }) } else { None };
                buckets.entry(target).or_default().push(event);
            }
            drop(states);
            for (index, events) in buckets {
                outputs[index].write(&events, task.path.clone(), &self.config, &self.memory, cancellation)?;
            }
            tokio::task::yield_now().await;
        }
        Ok(outputs)
    }
}

impl MergeOutput {
    fn pending(path: PathBuf, recency: Option<CivilDate>, part: &[u8], schema: OutputSchema) -> Self {
        Self { path, part: part.to_vec(), recency, schema, pages: MergePages::Retained(RetainedPages::default()), leaves: Vec::new() }
    }

    fn start_spill(&mut self, config: &MergeConfig, memory: &Arc<MemoryBudget>, cancellation: &CancellationToken) -> Result<()> {
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        let MergePages::Retained(retained) = &mut self.pages else { return Err(crate::invalid("Merge spill already started")); };
        let directory = self.path.parent().ok_or_else(|| crate::invalid("Merge path has no parent"))?;
        let writer = ArrowFileWriter::new_bounded_reserved(create_file(&self.path)?, self.schema.schema()?.as_ref(), disk_remaining(directory, config.temporary_byte_limit)?, memory)?;
        let batches = std::mem::take(&mut retained.batches);
        self.pages = MergePages::Writing(writer);
        for batch in batches {
            self.spill_page(&batch, config, memory, cancellation)?;
        }
        Ok(())
    }

    fn write(&mut self, events: &[PageEvent], path: Vec<u8>, config: &MergeConfig, memory: &Arc<MemoryBudget>,
        cancellation: &CancellationToken) -> Result<()> {
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        if events.is_empty() { return Ok(()); }
        let candidate = if let MergePages::Retained(retained) = &mut self.pages && events.len() <= config.page_size {
            // Appended origin and topology columns can hold separate buffer reservations.
            let page_memory = memory.scope();
            let batch = self.schema.batch(events, &page_memory)?;
            let bytes = page_memory.used_bytes();
            if bytes <= RETAINED_MERGE_BYTES - retained.bytes
                && batch.batch().schema() == self.schema.schema()?
            {
                retained.bytes += bytes;
                retained.batches.push(batch);
                self.record_leaf(path, events);
                return Ok(());
            }
            retained.batches.is_empty().then_some(batch)
        } else { None };
        if matches!(self.pages, MergePages::Retained(_)) { self.start_spill(config, memory, cancellation)?; }
        let batch = match candidate { Some(batch) => batch, None => self.schema.batch(events, memory)? };
        self.spill_page(&batch, config, memory, cancellation)?;
        self.record_leaf(path, events);
        Ok(())
    }

    fn spill_page(&mut self, batch: &OwnedBatch, config: &MergeConfig, memory: &Arc<MemoryBudget>,
        cancellation: &CancellationToken) -> Result<()> {
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        let MergePages::Writing(writer) = &mut self.pages else { return Err(crate::invalid("Merge file already ended")); };
        limit_writer(writer, self.path.parent().ok_or_else(|| crate::invalid("Merge path has no parent"))?, config.temporary_byte_limit)?;
        writer.write_event_page(batch.batch(), memory)
    }

    fn record_leaf(&mut self, path: Vec<u8>, events: &[PageEvent]) {
        self.leaves.push(layout::Leaf { page: self.leaves.len(), path, rows: events.len(), min_recency: events.iter().filter_map(|event| event.recency).min(), max_recency: events.iter().filter_map(|event| event.recency).max() });
    }

    fn finish(&mut self, config: &MergeConfig, memory: &Arc<MemoryBudget>, cancellation: &CancellationToken) -> Result<()> {
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        if let MergePages::Retained(retained) = &self.pages {
            if !retained.batches.is_empty() { return Ok(()); }
            self.start_spill(config, memory, cancellation)?;
        }
        let MergePages::Writing(mut writer) = std::mem::replace(&mut self.pages, MergePages::Spilled) else {
            return Err(crate::invalid("Merge file already ended"));
        };
        limit_writer(&mut writer, self.path.parent().ok_or_else(|| crate::invalid("Merge path has no parent"))?, config.temporary_byte_limit)?;
        writer.finish()?;
        Ok(())
    }

    fn normalize(&mut self, config: &MergeConfig, memory: &Arc<MemoryBudget>, data: &Path, meta: &Path,
        cancellation: &CancellationToken) -> Result<(u64, rtdb_format::log::proto::TrieMetadata)> {
        let result = layout::normalize(self, config, memory, data, meta, cancellation);
        if !matches!(self.pages, MergePages::Retained(_)) || !result.as_ref().is_err_and(|error| error.code == "rtdb/memory-budget-exceeded") {
            return result;
        }
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        for path in [data, meta] {
            remove_temporary_file(path)?;
        }
        self.start_spill(config, memory, cancellation)?;
        self.finish(config, memory, cancellation)?;
        layout::normalize(self, config, memory, data, meta, cancellation)
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

    #[tokio::test]
    async fn compaction_primary_inputs_spill_at_the_shared_cap_and_release_bytes_before_output() -> Result<()> {
        let fields = StructFields::from([("value".into(), LogicalType::Scalar(ScalarType::I64))]);
        let event = StoredEvent {
            position: None, origin: None, commit_position: 0, iid: Iid([1; 16]), entity_key: None,
            system_from: Timestamp(10), valid_time: Interval::ALL, recency: None,
            operation: StoredOperation::Put(StructFields::from([("value".into(), Value::I64(17))])),
        };
        let page = build_event_batch(&[event], Some(fields.clone()), None)?;
        let store = WholeInput(rtdb_format::ipc::write_file(page.schema_ref(), std::slice::from_ref(&page))?.into());
        let size = store.0.len();
        let key = ObjectKey::new("data.arrow")?;
        let directory = tempfile::tempdir().unwrap();
        let memory = MemoryBudget::new(8 << 20);
        let retained = memory.scope_with_limit(size);
        let cancellation = CancellationToken::new();
        let first_path = directory.path().join("first.arrow");
        let second_path = directory.path().join("second.arrow");
        let first = PrimaryData::stored(&store, &key, first_path.clone(), size as u64, &memory, &retained, &cancellation).await?;
        let second = PrimaryData::stored(&store, &key, second_path.clone(), size as u64, &memory, &retained, &cancellation).await?;
        let mut first = first.read(&memory, &cancellation, Ok)?;
        let mut second = second.read(&memory, &cancellation, Ok)?;
        assert!(!first_path.exists());
        assert!(second_path.exists());
        assert_eq!(retained.used_bytes(), size);

        let batch = read_page(&mut first.reader, 0, &memory)?;
        let spilled = read_page(&mut second.reader, 0, &memory)?;
        assert_eq!(batch.batch(), spilled.batch());
        let events = EventBatch::new(batch.batch().clone())?.page_events()?;
        let output = copy_compaction_batch(&events, Some(fields), None, false, false, &memory)?;
        drop((first, second, batch, spilled));
        assert_eq!(retained.used_bytes(), size);
        drop(events);
        assert_eq!(retained.used_bytes(), 0);
        assert_eq!(EventBatch::new(output.batch().clone())?.header(0)?.system_from, Timestamp(10));
        drop(output);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn primary_input_replays_its_consumer_from_spill_after_memory_pressure() -> Result<()> {
        let page = build_event_batch(&[StoredEvent {
            position: None, origin: None, commit_position: 0, iid: Iid([1; 16]), entity_key: None,
            system_from: Timestamp(10), valid_time: Interval::ALL, recency: None, operation: StoredOperation::Delete,
        }], None, None)?;
        let store = WholeInput(rtdb_format::ipc::write_file(page.schema_ref(), std::slice::from_ref(&page))?.into());
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("spill.arrow");
        let memory = MemoryBudget::new(8 << 20);
        let retained = primary_data_budget(&memory);
        let cancellation = CancellationToken::new();
        let data = PrimaryData::stored(&store, &ObjectKey::new("data.arrow")?, path.clone(), store.0.len() as u64,
            &memory, &retained, &cancellation).await?;
        let mut attempts = 0;
        data.read(&memory, &cancellation, |mut input| {
            attempts += 1;
            let batch = read_page(&mut input.reader, 0, &memory)?;
            if attempts == 1 {
                assert!(!path.exists());
                assert_eq!(retained.used_bytes(), store.0.len());
                memory.reserve(memory.limit_bytes() - memory.used_bytes() + 1)?;
            }
            assert_eq!(retained.used_bytes(), 0);
            let header = EventBatch::new(batch.batch().clone())?.header(0)?;
            assert_eq!(header.op, EventOp::Delete);
            assert_eq!(header.system_from, Timestamp(10));
            Ok(())
        })?;
        assert_eq!(attempts, 2);
        assert_eq!(std::fs::read(path).map_err(crate::invalid)?.as_slice(), store.0.as_ref());
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn compaction_metadata_retains_original_bytes_up_to_the_job_limit() -> Result<()> {
        use rtdb_format::metadata::{LeafColumns, MetadataNode};

        let metadata = MetadataFile::from_nodes(vec![MetadataNode::Leaf {
            data_page_index: 3,
            columns: LeafColumns::new(Vec::new()),
        }], false)?;
        let store = WholeInput(Bytes::from(metadata.write()?));
        let size = store.0.len();
        let key = ObjectKey::new("metadata.arrow")?;
        let directory = tempfile::tempdir().unwrap();
        let memory = MemoryBudget::new(8 << 20);
        let retained = memory.scope_with_limit(size);
        let cancellation = CancellationToken::new();
        let first_path = directory.path().join("first.arrow");
        let second_path = directory.path().join("second.arrow");
        let first = open_metadata(&store, &key, &first_path, size as u64, &memory, &retained, &cancellation).await?;
        assert!(!first_path.exists());
        assert_eq!(retained.used_bytes(), size);

        let second = open_metadata(&store, &key, &second_path, size as u64, &memory, &retained, &cancellation).await?;
        assert_eq!(std::fs::read(second_path).map_err(crate::invalid)?.as_slice(), store.0.as_ref());
        assert_eq!(retained.used_bytes(), size);

        let inputs = [first, second];
        let tasks = planner::plan(&inputs, &[])?;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].pages.iter().map(|page| (page.segment, page.page)).collect::<Vec<_>>(), [(0, 3), (1, 3)]);
        drop(inputs);
        assert_eq!(retained.used_bytes(), 0);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    fn page(events: &[StoredEvent]) -> Vec<PageEvent> { EventBatch::new(build_event_batch(events, None, None).unwrap()).unwrap().page_events().unwrap() }

    #[test]
    fn unique_ordered_l0_events_keep_their_original_intervals() {
        let event = |iid, lower, upper| StoredEvent {
            position: None, origin: None, commit_position: 0, entity_key: None,
            iid: Iid([iid; 16]),
            system_from: Timestamp(100),
            valid_time: Interval::from_micros(lower, upper).unwrap(),
            operation: StoredOperation::Delete,
            recency: None,
        };
        let mut input = page(&[event(1, 10, 20), event(2, 20, 30)]);
        assert!(!needs_same_time_resolution(&input));
        let (resolved, _charges) = resolve_same_time(&input, &MemoryBudget::new(1 << 20)).unwrap();
        assert_eq!(
            input.iter().map(EncodedEvent::header).collect::<Vec<_>>(),
            resolved.iter().map(EncodedEvent::header).collect::<Vec<_>>(),
        );
        input[1].iid = input[0].iid;
        assert!(needs_same_time_resolution(&input));
        input[1].iid = Iid([2; 16]);
        input[1].op = EventOp::Erase;
        assert!(needs_same_time_resolution(&input));
        input[1].op = EventOp::Delete;
        input[1].valid_time = Interval::from_micros(10, 10).unwrap();
        assert!(needs_same_time_resolution(&input));
        input[1].valid_time = Interval::ALL;
        input[1].system_from = Timestamp::MAX;
        assert!(needs_same_time_resolution(&input));
    }

    fn spill(path: PathBuf, schema: OutputSchema, config: &MergeConfig, memory: &Arc<MemoryBudget>) -> MergeOutput {
        let mut spill = MergeOutput::pending(path, None, &[], schema);
        spill.start_spill(config, memory, &CancellationToken::new()).unwrap();
        spill
    }

    #[test]
    fn topology_pages_keep_original_positions_and_positionless_readmission() {
        let memory = MemoryBudget::new(1 << 24);
        let id = rtdb_types::EntityId::Uuid([1; 16]);
        let record = StructFields::from([("_id".into(), id.as_value())]);
        let put = Some(Arc::new(StructFields::from([("_id".into(), id.as_value().logical_type())])));
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
            let input = EventBatch::new(build_event_batch(&[stored], schema.put.as_deref().cloned(), None).unwrap()).unwrap();
            let batch = schema.batch(&input.page_events().unwrap(), &memory).unwrap();
            assert_eq!(batch.batch().schema(), schema.schema().unwrap());
            let view = EventBatch::new(batch.batch().clone()).unwrap();
            assert_eq!(view.header(0).unwrap().commit_position, position);
        }
    }

    #[test]
    fn retained_compaction_pages_match_spill_geometry_bytes_and_event_witnesses() {
        use rtdb_types::{BlockIndex, EventOrigin, EventPosition, TimeUnit, TxId};

        let directory = tempfile::tempdir().unwrap();
        let memory = MemoryBudget::new(32 << 20);
        let config = MergeConfig { page_size: 3, ..Default::default() };
        let cancellation = CancellationToken::new();
        let fixed_width = [
            ("day", Value::DateDay(-2)),
            ("millis", Value::DateMilli(-1)),
            ("time", Value::Time { value: 100, unit: TimeUnit::Nanosecond, bit_width: 64 }),
            ("timestamp", Value::Timestamp { value: -1067, unit: TimeUnit::Nanosecond, timezone: Some("America/New_York".into()) }),
            ("duration", Value::Duration { value: -3, unit: TimeUnit::Microsecond }),
            ("uuid", Value::Uuid([0xab; 16])),
            ("fixed_binary", Value::FixedSizeBinary(vec![4, 5, 6])),
        ];
        let mut fields = StructFields::from([
            ("text".into(), LogicalType::Scalar(ScalarType::Utf8).maybe()),
            ("number".into(), LogicalType::Scalar(ScalarType::F64)),
        ]);
        for (name, value) in &fixed_width {
            fields.insert((*name).into(), value.logical_type());
        }
        let schema = OutputSchema {
            put: Some(Arc::new(fields)),
            iid_encoding: None, recency: false, keyed: true, topology: false,
            include_origins: true, include_positions: true, redacted_leg: true,
        };
        let rows = [
            StoredOperation::Put(StructFields::from([("text".into(), Value::Utf8("a".into())), ("number".into(), Value::F64(-0.0))])),
            StoredOperation::Put(StructFields::from([("text".into(), Value::Null), ("number".into(), Value::F64(1.0))])),
            StoredOperation::Redacted,
            StoredOperation::Delete,
            StoredOperation::Put(StructFields::from([("text".into(), Value::Utf8("b".into())), ("number".into(), Value::F64(2.0))])),
        ].into_iter().enumerate().map(|(row, mut operation)| {
            if let StoredOperation::Put(document) = &mut operation {
                for (name, value) in &fixed_width {
                    document.insert((*name).into(), value.clone());
                }
            }
            StoredEvent {
                iid: Iid([[0, 1, 16, 64, 65][row]; 16]), system_from: Timestamp(10 + row as i64),
                valid_time: Interval::from_micros(row as i64, 100).unwrap(),
                entity_key: Some(Arc::from([row as u8].as_slice())), commit_position: row as u32 + 1,
                origin: (row >= 2).then(|| EventOrigin::at([1; 16], row as u64, BlockIndex(1))),
                position: Some(EventPosition { transaction: TxId(1), ordinal: row as u64 }), recency: None, operation,
            }
        }).collect::<Vec<_>>();
        let input = EventBatch::new(build_event_batch(&rows, schema.put.as_deref().cloned(), None).unwrap()).unwrap();
        let events = input.page_events().unwrap();
        assert!(schema.supports_retention());
        for (name, tasks, lengths) in [
            ("single", vec![(0..3, vec![0])], vec![3]),
            ("multiple", vec![(0..2, vec![0, 0]), (2..3, vec![0, 1]), (3..5, vec![1])], vec![3, 2]),
        ] {
            let directory = directory.path().join(name);
            std::fs::create_dir(&directory).unwrap();
            let mut retained = MergeOutput::pending(directory.join("retained-spill.arrow"), None, &[], schema.clone());
            let mut spilled = spill(directory.join("spill.arrow"), schema.clone(), &config, &memory);
            for output in [&mut retained, &mut spilled] {
                for (rows, path) in &tasks {
                    output.write(&events[rows.clone()], path.clone(), &config, &memory, &cancellation).unwrap();
                }
                output.finish(&config, &memory, &cancellation).unwrap();
            }
            assert!(matches!(&retained.pages, MergePages::Retained(pages) if pages.batches.len() == tasks.len()));
            assert!(!retained.path.exists());
            assert!(matches!(spilled.pages, MergePages::Spilled));

            let normalize = |output: &mut MergeOutput, name| {
                let data = directory.join(format!("{name}.arrow"));
                let meta = directory.join(format!("{name}.meta.arrow"));
                let (size, details) = output.normalize(&config, &memory, &data, &meta, &cancellation).unwrap();
                (std::fs::read(data).unwrap(), std::fs::read(meta).unwrap(), size, details)
            };
            let result = normalize(&mut retained, "retained");
            assert_eq!(result, normalize(&mut spilled, "spilled"));
            assert!(!retained.path.exists());
            let mut reader = ArrowFile::open(result.0.into(), None).unwrap();
            assert_eq!(reader.page_count(), lengths.len());
            let mut offset = 0;
            for (index, length) in lengths.into_iter().enumerate() {
                let page = EventBatch::new(reader.read_page(index).unwrap()).unwrap();
                assert_eq!(page.len(), length);
                let observed = (0..length).map(|row| page.read_event(row).unwrap()).collect::<Vec<_>>();
                assert_eq!(observed.as_slice(), &rows[offset..offset + length]);
                offset += length;
            }
            drop(retained);
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn retained_historical_subtries_match_spill_with_iid_runs_and_recency_cells() {
        use arrow_array::{RunArray, types::Int32Type};
        use rtdb_format::metadata::MetadataNode;

        const SIX_HOURS: i64 = 6 * 3_600_000_000;
        let directory = tempfile::tempdir().unwrap();
        let memory = MemoryBudget::new(32 << 20);
        let cancellation = CancellationToken::new();
        let date = CivilDate::from_ymd(1970, 1, 5).unwrap();
        for recency in [false, true] {
            let directory = directory.path().join(recency.to_string());
            std::fs::create_dir(&directory).unwrap();
            let config = MergeConfig { page_size: if recency { 4 } else { 8 }, ..Default::default() };
            let schema = OutputSchema {
                put: None, iid_encoding: Some(RunEndWidth::I32), recency, keyed: false, topology: false,
                include_origins: false, include_positions: false, redacted_leg: false,
            };
            assert!(schema.supports_retention());
            let mut retained = MergeOutput::pending(directory.join("retained-spill.arrow"), Some(date), &[1], schema.clone());
            let mut spilled = MergeOutput::pending(directory.join("spill.arrow"), Some(date), &[1], schema);
            spilled.start_spill(&config, &memory, &cancellation).unwrap();
            for task in 0..16u8 {
                let path = vec![1, task / 4, task % 4];
                let iid = Iid([64 + (task << 2); 16]);
                let rows = [2, 1].map(|time| StoredEvent {
                    iid, system_from: Timestamp(time), valid_time: Interval::ALL,
                    entity_key: None, commit_position: 0, position: None, origin: None,
                    operation: StoredOperation::Delete,
                    recency: recency.then_some(Timestamp(if task % 2 == 0 { 0 } else { SIX_HOURS })),
                });
                let events = page(&rows);
                for output in [&mut retained, &mut spilled] {
                    output.write(&events, path.clone(), &config, &memory, &cancellation).unwrap();
                }
            }
            let normalize = |output: &mut MergeOutput, name| {
                output.finish(&config, &memory, &cancellation).unwrap();
                let data = directory.join(format!("{name}.arrow"));
                let meta = directory.join(format!("{name}.meta.arrow"));
                let details = output.normalize(&config, &memory, &data, &meta, &cancellation).unwrap();
                (std::fs::read(data).unwrap(), std::fs::read(meta).unwrap(), details)
            };
            let result = normalize(&mut retained, "retained");
            assert_eq!(result, normalize(&mut spilled, "spilled"));
            assert!(!retained.path.exists());
            let (data, meta, _) = result;
            let meta = MetadataFile::read(meta.into()).unwrap();
            let Some(MetadataNode::IidBranch(root)) = meta.nodes().last() else { panic!("missing subtrie root") };
            assert_eq!(root.as_slice(), &[None, root[1], None, None]);
            let child = &meta.nodes()[root[1].unwrap()];
            if recency {
                let MetadataNode::RecencyBranch(cells) = child else { panic!("missing recency cells") };
                assert!(cells.iter().map(|(boundary, _)| *boundary).eq([Timestamp(0), Timestamp(SIX_HOURS)]));
            } else {
                assert!(matches!(child, MetadataNode::IidBranch(_)));
            }
            let mut data = ArrowFile::open(data.into(), None).unwrap();
            assert_eq!(data.page_count(), if recency { 8 } else { 4 });
            for index in 0..data.page_count() {
                let batch = data.read_page(index).unwrap();
                let iids = batch.column_by_name("_iid").unwrap().as_any().downcast_ref::<RunArray<Int32Type>>().unwrap();
                let ends = if recency { &[2, 4][..] } else { &[2, 4, 6, 8][..] };
                assert_eq!(iids.run_ends().values(), ends);
            }
            drop(retained);
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn flat_compaction_pages_over_the_aggregate_retention_cap_spill_in_task_order() {
        let directory = tempfile::tempdir().unwrap();
        let memory = MemoryBudget::new(64 << 20);
        let config = MergeConfig::default();
        let cancellation = CancellationToken::new();
        let schema = OutputSchema {
            put: Some(Arc::new(StructFields::from([("blob".into(), LogicalType::Scalar(ScalarType::Binary))]))),
            iid_encoding: None, recency: false, keyed: false, topology: false,
            include_origins: false, include_positions: false, redacted_leg: false,
        };
        assert!(schema.supports_retention());
        for (name, count, bytes) in [("single", 1, RETAINED_MERGE_BYTES + 1), ("multiple", 2, RETAINED_MERGE_BYTES / 2 + 1)] {
            let mut output = MergeOutput::pending(directory.path().join(format!("{name}.arrow")), None, &[], schema.clone());
            let events = (0..count).map(|index| StoredEvent {
                iid: Iid([64 * index as u8; 16]), system_from: Timestamp(1), valid_time: Interval::ALL,
                entity_key: None, commit_position: 0, origin: None, position: None, recency: None,
                operation: StoredOperation::Put(StructFields::from([("blob".into(), Value::Binary(vec![index as u8; bytes]))])),
            }).collect::<Vec<_>>();
            for (index, event) in events.iter().enumerate() {
                let input = EventBatch::new(build_event_batch(std::slice::from_ref(event), schema.put.as_deref().cloned(), None).unwrap()).unwrap();
                output.write(&input.page_events().unwrap(), vec![index as u8], &config, &memory, &cancellation).unwrap();
                assert_eq!(output.path.exists(), index + 1 == count);
            }
            output.finish(&config, &memory, &cancellation).unwrap();
            assert!(matches!(output.pages, MergePages::Spilled));
            assert_eq!(memory.used_bytes(), 0);
            let (mut reader, _reservation) = open_input(File::open(&output.path).unwrap(), &memory).unwrap();
            assert_eq!(reader.page_count(), count);
            for (index, expected) in events.iter().enumerate() {
                let page = EventBatch::new(reader.read_page(index).unwrap()).unwrap();
                assert_eq!(page.len(), 1);
                assert_eq!(&page.read_event(0).unwrap(), expected);
            }
        }
    }

    #[test]
    fn retained_compaction_pages_obey_cancellation_and_temporary_byte_limit() {
        let directory = tempfile::tempdir().unwrap();
        let memory = MemoryBudget::new(8 << 20);
        let config = MergeConfig { page_size: 1, ..Default::default() };
        let cancellation = CancellationToken::new();
        let schema = OutputSchema {
            put: None, iid_encoding: None, recency: false, keyed: false, topology: false,
            include_origins: false, include_positions: false, redacted_leg: false,
        };
        let events = [0, 64].map(|bucket| StoredEvent {
            iid: Iid([bucket; 16]), system_from: Timestamp(1), valid_time: Interval::ALL,
            entity_key: None, commit_position: 0, origin: None, position: None, recency: None,
            operation: StoredOperation::Delete,
        });
        let mut output = MergeOutput::pending(directory.path().join("spill.arrow"), None, &[], schema);
        for (index, event) in events.iter().enumerate() {
            output.write(&page(std::slice::from_ref(event)), vec![index as u8], &config, &memory, &cancellation).unwrap();
        }
        output.finish(&config, &memory, &cancellation).unwrap();
        assert!(matches!(&output.pages, MergePages::Retained(pages) if pages.batches.len() == 2));

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let meta = directory.path().join("meta.arrow");
        let data = directory.path().join("cancelled.arrow");
        assert_eq!(output.normalize(&config, &memory, &data, &meta, &cancelled).unwrap_err().code, crate::closed().code);
        assert!(!data.exists());
        assert!(!meta.exists());

        let limited = MergeConfig { temporary_byte_limit: disk_usage(directory.path()).unwrap() + 1, ..config };
        let data = directory.path().join("limited.arrow");
        let error = output.normalize(&limited, &memory, &data, &meta, &CancellationToken::new()).unwrap_err();
        assert!(error.message.unwrap().contains("Arrow output byte limit exceeded"));
        assert!(disk_usage(directory.path()).unwrap() <= limited.temporary_byte_limit);
        assert!(!meta.exists());
        assert!(!output.path.exists());
        drop(output);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn normalization_reuses_a_spill_page_with_nullable_union_components() {
        use rtdb_types::{ListKind, ScalarType, Value};

        let directory = tempfile::tempdir().unwrap();
        let data = directory.path().join("data.arrow");
        let meta = directory.path().join("meta.arrow");
        let memory = MemoryBudget::new(32 << 20);
        let config = MergeConfig::default();
        let cancellation = CancellationToken::new();
        let values = [Value::FixedSizeList(vec![Value::F32(-0.0), Value::F64(f64::from_bits(0x7ff8_0000_0000_0042)), Value::Null].into()), Value::Null];
        let item = LogicalType::from_legs([LogicalType::Scalar(ScalarType::F32), LogicalType::Scalar(ScalarType::F64), LogicalType::Null]);
        let fields = Some(StructFields::from([("v".into(), LogicalType::List {
            kind: ListKind::FixedSizeList(3), element: Box::new(item),
        }.maybe())]));
        let events = values.into_iter().map(|value| StoredEvent { position: None,
            iid: Iid([1; 16]), system_from: Timestamp(10), valid_time: Interval::ALL,
            origin: None, commit_position: 0, entity_key: None, recency: None,
            operation: StoredOperation::Put(StructFields::from([("v".into(), value)])),
        }).collect::<Vec<_>>();
        let input = EventBatch::new(build_event_batch(&events, fields.clone(), None).unwrap()).unwrap();
        let schema = OutputSchema { put: fields.clone().map(Arc::new), iid_encoding: None, recency: false, keyed: false, topology: false, include_origins: false, include_positions: false, redacted_leg: false };
        assert!(!schema.supports_retention());
        let mut spill = spill(directory.path().join("spill.arrow"), schema, &config, &memory);
        spill.write(&input.page_events().unwrap(), vec![], &config, &memory, &cancellation).unwrap();
        spill.finish(&config, &memory, &cancellation).unwrap();

        let expected = rtdb_format::data::copy_event_batch_with_recency(&events, fields.clone(), None, false).unwrap();
        let (mut reader, _reservation) = open_input(File::open(&spill.path).unwrap(), &memory).unwrap();
        let decoded = read_page(&mut reader, 0, &memory).unwrap();
        assert_ne!(decoded.batch().schema(), expected.schema());
        let restored = rtdb_format::ipc::restore_spill_page_schema(decoded.batch(), expected.schema_ref()).unwrap().unwrap();
        let components = |batch: &arrow_array::RecordBatch| {
            use arrow_array::cast::AsArray;
            let op = batch.column_by_name("op").unwrap().as_union();
            let put = op.child(op.type_id(0)).as_struct();
            let values = put.column(0).as_fixed_size_list().values().as_union();
            values.child(values.type_id(0)).to_data().buffers()[0].as_ptr()
        };
        assert_eq!(components(decoded.batch()), components(&restored));
        let reference = directory.path().join("rebuilt.arrow");
        let mut writer = ArrowFileWriter::new(create_file(&reference).unwrap(), expected.schema_ref()).unwrap();
        writer.write_event_page(&expected, &memory).unwrap();
        writer.finish().unwrap();
        let mut metadata = rtdb_format::metadata::MetadataBuilder::new(false, true);
        metadata.write_leaf(&expected).unwrap();
        let (metadata, _) = metadata.finish().unwrap();

        layout::normalize(&spill, &config, &memory, &data, &meta, &CancellationToken::new()).unwrap();
        assert_eq!(std::fs::read(data).unwrap(), std::fs::read(reference).unwrap());
        assert_eq!(std::fs::read(meta).unwrap(), metadata.write().unwrap());
    }

    #[test]
    fn normalization_splits_and_coalesces_spill_ranges_with_identical_file_bytes() {
        use rtdb_types::{ScalarType, Value};

        let directory = tempfile::tempdir().unwrap();
        let memory = MemoryBudget::new(32 << 20);
        let cancellation = CancellationToken::new();
        let fields = Some(StructFields::from([("v".into(), LogicalType::list(LogicalType::Scalar(ScalarType::F64).maybe()).maybe())]));
        let events = [0, 1, 128, 129].into_iter().map(|first| {
            let mut iid = [0; 16];
            iid[0] = first;
            StoredEvent {
                position: None,
                iid: Iid(iid), system_from: Timestamp(10), valid_time: Interval::ALL,
                origin: None, commit_position: 0, entity_key: None, recency: None,
                operation: if first == 1 { StoredOperation::Delete } else {
                    StoredOperation::Put(StructFields::from([("v".into(), Value::list(vec![
                        Value::F64(-0.0), Value::Null, Value::F64(f64::from_bits(0x7ff8_0000_0000_0042)),
                    ]))]))
                },
            }
        }).collect::<Vec<_>>();
        let output_schema = OutputSchema { put: fields.clone().map(Arc::new), iid_encoding: None, recency: false, keyed: false, topology: false, include_origins: false, include_positions: false, redacted_leg: false };
        let schema = output_schema.schema().unwrap();
        for (name, page_size) in [("split", 2), ("coalesce", 4)] {
            let directory = directory.path().join(name);
            std::fs::create_dir(&directory).unwrap();
            let config = MergeConfig { page_size, ..Default::default() };
            let data = directory.join("data.arrow");
            let meta = directory.join("meta.arrow");
            let mut spill = spill(directory.join("spill.arrow"), output_schema.clone(), &config, &memory);
            let events_in = |events: &[StoredEvent]| {
                EventBatch::new(build_event_batch(events, fields.clone(), None).unwrap()).unwrap().page_events().unwrap()
            };
            if name == "split" {
                spill.write(&events_in(&events), vec![], &config, &memory, &cancellation).unwrap();
            } else {
                spill.write(&events_in(&events[..2]), vec![0], &config, &memory, &cancellation).unwrap();
                spill.write(&events_in(&events[2..]), vec![2], &config, &memory, &cancellation).unwrap();
            }
            spill.finish(&config, &memory, &cancellation).unwrap();

            let (mut reader, _reservation) = open_input(File::open(&spill.path).unwrap(), &memory).unwrap();
            let pages = (0..spill.leaves.len()).map(|page| {
                let decoded = read_page(&mut reader, page, &memory).unwrap();
                EventBatch::new(decoded.batch().clone()).unwrap()
            }).collect::<Vec<_>>();
            let ranges = if name == "split" { vec![(&pages[0], 2..4)] } else { vec![(&pages[0], 0..2), (&pages[1], 0..2)] };
            assert!(rtdb_format::data::copy_spill_event_ranges(&ranges, &memory).unwrap().is_some());

            let reference = directory.join("rebuilt.arrow");
            let mut writer = ArrowFileWriter::new(create_file(&reference).unwrap(), &schema).unwrap();
            let mut metadata = rtdb_format::metadata::MetadataBuilder::new(false, true);
            let mut leaves = Vec::new();
            for events in events.chunks(page_size) {
                let batch = rtdb_format::data::copy_event_batch_with_recency(events, fields.clone(), None, false).unwrap();
                leaves.push(metadata.write_leaf(&batch).unwrap());
                writer.write_event_page(&batch, &memory).unwrap();
            }
            if name == "split" { metadata.write_iid_branch([Some(leaves[0]), None, Some(leaves[1]), None]).unwrap(); }
            let (metadata, _) = metadata.finish().unwrap();
            writer.finish().unwrap();

            layout::normalize(&spill, &config, &memory, &data, &meta, &CancellationToken::new()).unwrap();
            assert_eq!(std::fs::read(data).unwrap(), std::fs::read(reference).unwrap());
            assert_eq!(std::fs::read(meta).unwrap(), metadata.write().unwrap());
        }
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn normalization_retains_governed_event_keys_and_positions_in_the_output_file() {
        let directory = tempfile::tempdir().unwrap();
        let spill_path = directory.path().join("merge.arrow");
        let data = directory.path().join("data.arrow");
        let metadata = directory.path().join("meta.arrow");
        let memory = MemoryBudget::new(1 << 22);
        let config = MergeConfig::default();
        let cancellation = CancellationToken::new();
        let events = [(b"world-a".as_slice(), 2), (b"world-b".as_slice(), 1)].map(|(key, commit_position)| StoredEvent { position: None,
            origin: None,
            commit_position,
            entity_key: Some(Arc::from(key)), iid: Iid([1; 16]), system_from: Timestamp(100),
            valid_time: Interval::ALL, operation: StoredOperation::Delete, recency: None,
        });
        let schema = OutputSchema { put: None, iid_encoding: None, recency: false, keyed: true, topology: false, include_origins: false, include_positions: false, redacted_leg: false };
        let mut spill = spill(spill_path, schema, &config, &memory);
        spill.write(&page(&events), vec![], &config, &memory, &cancellation).unwrap();
        spill.finish(&config, &memory, &cancellation).unwrap();
        layout::normalize(&spill, &config, &memory, &data, &metadata, &CancellationToken::new()).unwrap();
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
        let cancellation = CancellationToken::new();
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
        let schema = OutputSchema { put: None, iid_encoding: None, recency: false, keyed: true, topology: false, include_origins: true, include_positions: false, redacted_leg: true };
        let mut spill = spill(directory.path().join("merge.arrow"), schema, &config, &memory);
        spill.write(&normalized[..1], vec![], &config, &memory, &cancellation).unwrap();
        spill.write(&normalized[1..], vec![], &config, &memory, &cancellation).unwrap();
        spill.finish(&config, &memory, &cancellation).unwrap();
        let (mut reader, _reservation) = open_input(File::open(spill.path).unwrap(), &memory).unwrap();
        for (index, expected) in [redacted, earlier].into_iter().enumerate() {
            let batch = read_page(&mut reader, index, &memory).unwrap();
            let events = EventBatch::new(batch.batch().clone()).unwrap();
            assert_eq!(events.read_event(0).unwrap(), expected);
        }
    }

    #[test]
    fn compaction_retains_the_ordered_prefix_at_a_tied_sql_time() {
        use rtdb_types::{EventPosition, LogicalType, ScalarType, TxId, Value};
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
