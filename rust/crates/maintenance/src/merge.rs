mod layout;
mod planner;

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use async_trait::async_trait;
use arrow_schema::{DataType, SchemaRef};
use chrono::{Datelike, Days, NaiveDate};
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch, from_field};
use rtdb_format::{data::{EventBatch, StoredEvent, copy_event_batch_with_recency}, ipc::{ArrowFile, ArrowFileWriter, inspect_file_reserved}, keys::TrieKey,
    log::proto::{TriesAdded, TrieDetails}, metadata::MetadataFile};
use rtdb_storage::{ObjectKey, ObjectStore, ReadReservation};
use rtdb_temporal::PolygonCalculator;
use rtdb_types::{EventOp, Interval, LogicalType, Result, RunEndWidth, StorageEpoch, StorageVersion, Timestamp};
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
}

impl Default for MergeConfig {
    fn default() -> Self { Self { page_size: 1024, run_end_encoded_iids: false, within_file_recency_branching: false,
        recency_partition: RecencyPartition::Week, temporary_directory: std::env::temp_dir(), temporary_byte_limit: 16 * 1024 * 1024 * 1024 } }
}

pub struct SegmentCompactor {
    store: Arc<dyn ObjectStore>,
    snapshots: Arc<dyn SnapshotSource>,
    memory: Arc<MemoryBudget>,
    version: StorageVersion,
    epoch: StorageEpoch,
    config: MergeConfig,
}

impl SegmentCompactor {
    pub fn new(store: Arc<dyn ObjectStore>, snapshots: Arc<dyn SnapshotSource>, memory: Arc<MemoryBudget>,
        version: StorageVersion, epoch: StorageEpoch, config: MergeConfig) -> Result<Self> {
        if config.page_size == 0 || config.temporary_byte_limit == 0 { return Err(crate::invalid("Compaction page size and temporary byte limit must be positive")); }
        Ok(Self { store, snapshots, memory, version, epoch, config })
    }
}

struct InputSegment { key: TrieKey, metadata: MetadataFile, reader: ArrowFile, _reservations: Vec<Arc<MemoryReservation>> }

struct MergeFile {
    path: PathBuf,
    part: Vec<u8>,
    recency: Option<NaiveDate>,
    carries_recency: bool,
    writer: Option<ArrowFileWriter>,
    leaves: Vec<layout::Leaf>,
}

struct OutputSchema { put: Option<BTreeMap<String, LogicalType>>, iid_encoding: Option<RunEndWidth>, recency: bool }

impl OutputSchema {
    fn batch(&self, events: &[StoredEvent]) -> Result<arrow_array::RecordBatch> {
        copy_event_batch_with_recency(events, self.put.clone(), self.iid_encoding, self.recency)
    }
    fn schema(&self) -> Result<SchemaRef> { Ok(self.batch(&[])?.schema()) }
}

fn create_file(path: &Path) -> Result<File> {
    OpenOptions::new().create_new(true).write(true).read(true).open(path).map_err(crate::invalid)
}

fn row_bytes(event: &StoredEvent) -> Result<usize> {
    let bytes = match &event.operation {
        rtdb_format::data::StoredOperation::Put(document) => document.iter().try_fold(0usize, |total, (key, value)| {
            let value_bytes = value.retained_bytes()?;
            total.checked_add(key.len()).and_then(|bytes| bytes.checked_add(96)).and_then(|bytes| bytes.checked_add(value_bytes)).ok_or_else(|| crate::invalid("Compaction row size overflow"))
        })?,
        _ => 0,
    };
    bytes.checked_add(std::mem::size_of::<StoredEvent>() + 128).ok_or_else(|| crate::invalid("Compaction row size overflow"))
}

fn event_reservation(memory: &Arc<MemoryBudget>, events: &[StoredEvent], multiplier: usize) -> Result<Arc<MemoryReservation>> {
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

async fn download(store: &dyn ObjectStore, memory: &Arc<MemoryBudget>, key: &ObjectKey, path: &Path, cancellation: &CancellationToken) -> Result<File> {
    let object = store.head(key).await.map_err(crate::invalid)?;
    let mut file = create_file(path)?;
    let mut position = 0;
    while position < object.size {
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        let count = usize::try_from((object.size - position).min(1024 * 1024)).map_err(crate::invalid)?;
        let reservation = ReadReservation::new(count, memory.reserve(count)?);
        let bytes = store.get_reserved_range(key, position..position + count as u64, reservation).await.map_err(crate::invalid)?;
        file.write_all(bytes.as_slice()).map_err(crate::invalid)?;
        position += count as u64;
    }
    file.seek(SeekFrom::Start(0)).map_err(crate::invalid)?;
    Ok(file)
}

fn open_input(mut file: File, memory: &Arc<MemoryBudget>) -> Result<(ArrowFile, Arc<MemoryReservation>)> {
    let layout = inspect_file_reserved(&mut file, |bytes| memory.reserve(bytes))?;
    if !layout.uncompressed { return Err(rtdb_types::Anomaly::unsupported("rtdb.maintenance/compressed-input", "Compaction requires uncompressed Arrow input until decoded buffer allocation bounds are available")); }
    let bytes = layout.dictionary_encoded_bytes.checked_mul(8).and_then(|bytes| layout.footer_encoded_bytes.checked_mul(64).and_then(|footer| bytes.checked_add(footer))).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(|| crate::invalid("Arrow opening allocation overflow"))?;
    let reservation = memory.reserve(bytes)?;
    let reader = ArrowFile::from_inspected_reader(file, None, layout)?;
    Ok((reader, reservation))
}

fn read_page(reader: &mut ArrowFile, index: usize, memory: &Arc<MemoryBudget>) -> Result<OwnedBatch> {
    let bound = reader.encoded_page_size(index)?.checked_mul(8).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(|| crate::invalid("Arrow page allocation overflow"))?;
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

fn merged_put_schema(inputs: &[InputSegment]) -> Result<Option<BTreeMap<String, LogicalType>>> {
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

fn resolve_same_time(events: &[StoredEvent], memory: &Arc<MemoryBudget>) -> Result<(Vec<StoredEvent>, Vec<Arc<MemoryReservation>>)> {
    let mut result = Vec::new();
    let mut charges = Vec::new();
    let mut calculator = PolygonCalculator::new(None);
    let mut previous = None;
    let mut seen_erase = false;
    let mut erased_iid = None;
    for event in events {
        let group = (event.iid, event.system_from);
        if previous != Some(group) { calculator.reset(); previous = Some(group); seen_erase = false; }
        let polygon = if erased_iid == Some(event.iid) { None } else { calculator.calculate(&event.header())? };
        if event.header().op == EventOp::Erase { erased_iid = Some(event.iid); }
        match polygon {
            None => {
                if !seen_erase {
                    charges.push(memory.reserve(row_bytes(event)?.checked_mul(2).ok_or_else(|| crate::invalid("Resolved row allocation overflow"))?)?);
                    let mut event = event.clone();
                    event.valid_time = Interval::new(Timestamp::MIN, Timestamp::MAX)?;
                    result.push(event);
                }
                seen_erase = true;
            }
            Some(polygon) => for index in 0..polygon.range_count() {
                if polygon.system_time_ceilings()[index] > event.system_from {
                    charges.push(memory.reserve(row_bytes(event)?.checked_mul(2).ok_or_else(|| crate::invalid("Resolved row allocation overflow"))?)?);
                    let mut event = event.clone();
                    event.valid_time = Interval::new(polygon.valid_times()[index], polygon.valid_times()[index + 1])?;
                    result.push(event);
                }
            },
        }
    }
    Ok((result, charges))
}

pub fn recency_partition(recency: Timestamp, partition: RecencyPartition) -> Result<NaiveDate> {
    let instant = chrono::DateTime::from_timestamp_micros(recency.0).ok_or_else(|| crate::invalid("Recency timestamp exceeds calendar range"))?;
    let instant = instant.checked_sub_signed(chrono::Duration::nanoseconds(1)).ok_or_else(|| crate::invalid("Recency timestamp underflow"))?;
    let date = instant.date_naive();
    let result = match partition {
        RecencyPartition::Week => date.checked_add_days(Days::new(u64::from(7 - date.weekday().num_days_from_monday()))),
        RecencyPartition::Month => if date.month() == 12 { NaiveDate::from_ymd_opt(date.year() + 1, 1, 1) } else { NaiveDate::from_ymd_opt(date.year(), date.month() + 1, 1) },
        RecencyPartition::Quarter => { let month = (date.month0() / 3 + 1) * 3 + 1; NaiveDate::from_ymd_opt(date.year() + i32::from(month > 12), if month > 12 { 1 } else { month }, 1) }
        RecencyPartition::Year => NaiveDate::from_ymd_opt(date.year() + 1, 1, 1),
    };
    result.ok_or_else(|| crate::invalid("Recency partition exceeds calendar range"))
}

#[async_trait]
impl CompactionExecutor for SegmentCompactor {
    async fn execute(&self, job: &CompactionJob, cancellation: &CancellationToken) -> Result<TriesAdded> {
        let directory = tempfile::Builder::new().prefix("rtdb-compaction-").tempdir_in(&self.config.temporary_directory).map_err(crate::invalid)?;
        let snapshot = self.snapshots.snapshot();
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
            if !inspect_file_reserved(&mut metadata, |bytes| self.memory.reserve(bytes))?.uncompressed {
                return Err(rtdb_types::Anomaly::unsupported("rtdb.maintenance/compressed-input", "Compaction requires uncompressed Arrow metadata"));
            }
            let metadata_charge = self.memory.reserve(usize::try_from(metadata.metadata().map_err(crate::invalid)?.len()).map_err(crate::invalid)?.checked_mul(64).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(|| crate::invalid("Metadata allocation overflow"))?)?;
            let metadata = MetadataFile::read_reader(metadata)?;
            let (reader, reservation) = open_input(data, &self.memory)?;
            inputs.push(InputSegment { key: key.clone(), metadata, reader, _reservations: vec![reservation, metadata_charge] });
        }
        let put = merged_put_schema(&inputs)?;
        let carry = self.config.within_file_recency_branching && job.output.recency().is_some() && inputs.iter().all(|input| input.reader.schema().index_of("_recency").is_ok());
        let tasks = planner::plan(&inputs, &job.part)?;
        let initial_recency = if job.recency_mode == RecencyMode::Partition { None } else { job.output.recency() };
        let initial_carry = job.recency_mode == RecencyMode::Preserve && carry;
        let mut outputs = vec![MergeFile::new(directory.path().join("merged-0.arrow"), initial_recency, initial_carry, &job.part, &put, &self.config, &self.memory)?];
        for task in tasks {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            let mut rows = Vec::new();
            let mut charges = Vec::new();
            for page in &task.pages {
                let input = &mut inputs[page.segment];
                let batch = read_page(&mut input.reader, page.page, &self.memory)?;
                let _decoded = self.memory.reserve(decoded_bound(batch.batch())?)?;
                let events = EventBatch::new(batch.batch().clone())?;
                let mut events = (0..events.len()).map(|row| events.read_event(row)).collect::<Result<Vec<_>>>()?;
                if input.key.level() == 0 {
                    let (resolved, reservations) = resolve_same_time(&events, &self.memory)?;
                    events = resolved;
                    charges.extend(reservations);
                }
                let path = if job.part.len() > task.path.len() { &job.part } else { &task.path };
                events.retain(|event| planner::matches_path(event.iid, path));
                charges.push(event_reservation(&self.memory, &events, 2)?);
                rows.push(events);
            }
            let mut heap = planner::RowHeap::new(&rows)?;
            let mut calculator = PolygonCalculator::new(None);
            let mut buckets: BTreeMap<usize, Vec<StoredEvent>> = BTreeMap::new();
            let mut bucket_charges = Vec::new();
            while let Some((stream, row)) = heap.next(&rows) {
                let event = &rows[stream][row];
                let Some(polygon) = calculator.calculate(&event.header())? else { continue; };
                let recency = polygon.recency();
                let target = if job.recency_mode == RecencyMode::Partition && recency != Timestamp::MAX {
                    let date = recency_partition(recency, self.config.recency_partition)?;
                    if let Some(index) = outputs.iter().position(|output| output.recency == Some(date)) { index }
                    else {
                        let index = outputs.len();
                        outputs.push(MergeFile::new(directory.path().join(format!("merged-{index}.arrow")), Some(date), self.config.within_file_recency_branching, &job.part, &put, &self.config, &self.memory)?);
                        index
                    }
                } else { 0 };
                bucket_charges.push(self.memory.reserve(row_bytes(event)?.checked_mul(2).ok_or_else(|| crate::invalid("Output row allocation overflow"))?)?);
                let mut event = event.clone();
                event.recency = if outputs[target].carries_recency { Some(if job.recency_mode == RecencyMode::Partition { recency } else { event.recency.ok_or_else(|| crate::invalid("Historical input lost recency"))? }) } else { None };
                buckets.entry(target).or_default().push(event);
            }
            for (index, events) in buckets {
                let _charge = event_reservation(&self.memory, &events, 8)?;
                outputs[index].write(&events, task.path.clone(), &put, &self.config)?;
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
            let key = TrieKey::new(job.output.level(), output.recency, job.output.part().to_vec(), job.output.block_index())?;
            let data_path = directory.path().join(format!("output-{index}.arrow"));
            let meta_path = directory.path().join(format!("output-{index}.meta.arrow"));
            let (size, metadata) = layout::normalize(output, &put, &self.config, &self.memory, &data_path, &meta_path, cancellation)?;
            check_disk(directory.path(), self.config.temporary_byte_limit, 0)?;
            self.store.put_file(&ObjectKey::new(entry.slug.data_key(&key)).map_err(crate::invalid)?, &data_path).await.map_err(crate::invalid)?;
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            self.store.put_file(&ObjectKey::new(entry.slug.metadata_key(&key)).map_err(crate::invalid)?, &meta_path).await.map_err(crate::invalid)?;
            tries.push(TrieDetails { table_name: Some(entry.table.schema_and_table()), trie_key: Some(key.to_string()), data_file_size: Some(i64::try_from(size).map_err(crate::invalid)?), trie_metadata: Some(metadata), ..Default::default() });
        }
        Ok(TriesAdded { storage_version: Some(self.version.0), storage_epoch: Some(self.epoch.0), tries, ..Default::default() })
    }
}

impl MergeFile {
    fn new(path: PathBuf, recency: Option<NaiveDate>, carries_recency: bool, part: &[u8], put: &Option<BTreeMap<String, LogicalType>>, config: &MergeConfig, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let schema = OutputSchema { put: put.clone(), iid_encoding: (config.run_end_encoded_iids && recency.is_some()).then_some(RunEndWidth::I32), recency: carries_recency };
        let directory = path.parent().ok_or_else(|| crate::invalid("Merge path has no parent"))?;
        let writer = ArrowFileWriter::new_bounded_reserved(create_file(&path)?, &schema.schema()?, disk_remaining(directory, config.temporary_byte_limit)?, memory)?;
        Ok(Self { path, part: part.to_vec(), recency, carries_recency, writer: Some(writer), leaves: Vec::new() })
    }
    fn write(&mut self, events: &[StoredEvent], path: Vec<u8>, put: &Option<BTreeMap<String, LogicalType>>, config: &MergeConfig) -> Result<()> {
        if events.is_empty() { return Ok(()); }
        let schema = OutputSchema { put: put.clone(), iid_encoding: (config.run_end_encoded_iids && self.recency.is_some()).then_some(RunEndWidth::I32), recency: self.carries_recency };
        let writer = self.writer.as_mut().ok_or_else(|| crate::invalid("Merge file already ended"))?;
        limit_writer(writer, self.path.parent().ok_or_else(|| crate::invalid("Merge path has no parent"))?, config.temporary_byte_limit)?;
        writer.write(&schema.batch(events)?)?;
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
