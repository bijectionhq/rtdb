use std::fs::File;
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{
    Array, FixedSizeBinaryArray, RecordBatch, UInt32Array,
    builder::{ArrayBuilder, FixedSizeBinaryBuilder, Int64Builder},
    cast::AsArray,
    types::Int64Type,
};
use arrow_buffer::Buffer;
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use geometry::{GeometryWork, KeyRange};
use rtdb_columnar::{MemoryBudget, MemoryReservation, binary_cell};
use rtdb_format::{
    data::{EncodedEvent, EventRef, PutColumn},
    ipc::{ArrowFileWriter, PageDecoder, inspect_file_reserved},
};
use rtdb_types::{Anomaly, AnomalyCategory, EventOp, Iid, Result};
use sha2::{Digest, Sha256};

use crate::{definition::Definition, kernel::Location};

const ROW_BYTES: usize = 32 + 8 + 16;
const BATCH_ROWS: usize = 16_384;
const MAX_COUNT: usize = 1_000_000;
pub(crate) const MAX_FILE_BYTES: u64 = (ROW_BYTES * MAX_COUNT + (256 << 10)) as u64;

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("partition", DataType::FixedSizeBinary(32), false),
        Field::new("cell", DataType::Int64, false),
        Field::new("iid", DataType::FixedSizeBinary(16), false),
    ])
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Partition([u8; 32]);

impl Partition {
    pub(crate) fn of<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> Self {
        let mut hasher = Sha256::new();
        for part in parts {
            hasher.update((part.len() as u64).to_be_bytes());
            hasher.update(part);
        }
        Self(hasher.finalize().into())
    }

    fn from_event(event: &EventRef<'_>, definition: &Definition) -> Result<Self> {
        let mut hasher = Sha256::new();
        for column in &definition.partition {
            let part = event_binary(event, column)?.unwrap_or_default();
            hasher.update((part.len() as u64).to_be_bytes());
            hasher.update(part);
        }
        Ok(Self(hasher.finalize().into()))
    }
}

fn event_binary<'a>(event: &'a EventRef<'_>, column: &str) -> Result<Option<&'a [u8]>> {
    match event.put_column(column) {
        Some(PutColumn::Stored(field, array, row)) => binary_cell(field, array, row),
        Some(PutColumn::Absent) | None => Ok(None),
    }
}

pub(crate) fn event_keys(
    event: &EventRef<'_>,
    definition: &Definition,
    work: &mut GeometryWork,
) -> Result<Option<(Partition, Iid, Vec<i64>)>> {
    let header = event.header();
    if header.op != EventOp::Put {
        return Ok(None);
    }
    let partition = Partition::from_event(event, definition)?;
    let keys = match event_binary(event, &definition.geometry)? {
        Some(binary) => work.scoped(|work| {
            match Location::decode_stored(binary, definition.encoding, work)? {
                Some(location) => location.keys(work),
                None => Ok(vec![i64::MIN]),
            }
        })?,
        None => vec![i64::MIN],
    };
    Ok(Some((partition, header.iid, keys)))
}

pub(crate) struct Postings {
    partitions: FixedSizeBinaryBuilder,
    cells: Int64Builder,
    iids: FixedSizeBinaryBuilder,
    reservation: Arc<MemoryReservation>,
}

impl Postings {
    pub(crate) fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        Ok(Self {
            partitions: FixedSizeBinaryBuilder::with_capacity(0, 32),
            cells: Int64Builder::with_capacity(0),
            iids: FixedSizeBinaryBuilder::with_capacity(0, 16),
            reservation: memory.reserve(0)?,
        })
    }

    pub(crate) fn append(&mut self, partition: Partition, iid: Iid, keys: Vec<i64>) -> Result<()> {
        let length = self
            .cells
            .len()
            .checked_add(keys.len())
            .filter(|length| *length <= MAX_COUNT)
            .ok_or_else(work_limit)?;
        if length * ROW_BYTES > self.reservation.bytes() {
            let capacity = length.next_power_of_two().max(8);
            Arc::get_mut(&mut self.reservation)
                .expect("builder owns its reservation")
                .grow_to(capacity * ROW_BYTES)?;
        }
        for cell in keys {
            self.partitions.append_value(partition.0).map_err(corrupt)?;
            self.cells.append_value(cell);
            self.iids.append_value(iid.0).map_err(corrupt)?;
        }
        Ok(())
    }

    pub(crate) fn write(
        mut self,
        file: File,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<(u64, Vec<u8>)> {
        let schema = Arc::new(schema());
        let memory = self.reservation.budget();
        let partitions = self.partitions.finish();
        let cells = self.cells.finish();
        let iids = self.iids.finish();
        let _order = memory.reserve(cells.len() * std::mem::size_of::<u32>())?;
        let mut order = (0..cells.len() as u32).collect::<Vec<_>>();
        let key = |row: u32| {
            let row = row as usize;
            (partitions.value(row), cells.value(row), iids.value(row))
        };
        order.sort_unstable_by_key(|&row| key(row));
        order.dedup_by_key(|row| key(*row));
        let order = UInt32Array::from(order);
        let mut writer =
            ArrowFileWriter::new_hashed_bounded_reserved(file, &schema, MAX_FILE_BYTES, memory)?;
        for start in (0..order.len()).step_by(BATCH_ROWS) {
            if cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let rows = order.slice(start, (order.len() - start).min(BATCH_ROWS));
            let _scratch = memory.reserve(rows.len() * ROW_BYTES * 4 + (64 << 10))?;
            let first = rows.value(0) as usize;
            let contiguous = rows.values().iter().enumerate()
                .all(|(index, &row)| row as usize == first + index);
            let columns = [&partitions as &dyn Array, &cells, &iids]
                .into_iter()
                .map(|array| {
                    if contiguous {
                        Ok(array.slice(first, rows.len()))
                    } else {
                        arrow_select::take::take(array, &rows, None).map_err(corrupt)
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let batch = RecordBatch::try_new(Arc::clone(&schema), columns).map_err(corrupt)?;
            writer.write(&batch)?;
        }
        let (length, digest) = writer.finish_hashed()?;
        if cancellation.is_cancelled() {
            return Err(cancelled());
        }
        Ok((length, digest.to_vec()))
    }
}

pub(crate) fn sha256(bytes: &[u8], check: &impl Fn() -> Result<()>) -> Result<[u8; 32]> {
    let mut hash = Sha256::new();
    for chunk in bytes.chunks(64 << 10) {
        check()?;
        hash.update(chunk);
    }
    Ok(hash.finalize().into())
}

pub(crate) fn verify_digest(actual: [u8; 32], expected: &[u8]) -> Result<()> {
    if actual.as_slice() != expected {
        return Err(corrupt("Spatial posting file identity does not match the catalog"));
    }
    Ok(())
}

pub(crate) fn candidates(
    bytes: impl AsRef<[u8]> + Send + 'static,
    partition: Option<Partition>,
    ranges: &[KeyRange],
    memory: &Arc<MemoryBudget>,
    check: impl Fn() -> Result<()>,
    mut insert: impl FnMut(Iid) -> Result<()>,
) -> Result<()> {
    if bytes.as_ref().len() as u64 > MAX_FILE_BYTES {
        return Err(corrupt("Spatial posting file exceeds its size limit"));
    }
    let file_error = |error: Anomaly| {
        if error.code == "rtdb/memory-budget-exceeded" {
            error
        } else {
            corrupt(error)
        }
    };
    let bytes = Buffer::from(Bytes::from_owner(bytes));
    let layout = inspect_file_reserved(&mut Cursor::new(bytes.as_slice()), |size| {
        memory.reserve(size)
    })
    .map_err(file_error)?;
    let footer_start = bytes.len() - layout.footer_encoded_bytes;
    let _footer = memory.reserve(layout.opening_allocation_bound().map_err(file_error)?)?;
    let file = PageDecoder::from_footer(
        &bytes[footer_start..bytes.len() - 10],
        footer_start as u64,
        None,
    )
    .map_err(file_error)?;
    if file.schema().as_ref() != &schema()
        || file.dictionary_count() != 0
        || file.page_count() > MAX_COUNT.div_ceil(BATCH_ROWS)
    {
        return Err(corrupt("Invalid spatial posting schema or page count"));
    }
    let mut total = 0;
    for page in 0..file.page_count() {
        check()?;
        let range = file.page_range(page).map_err(file_error)?;
        let range = range.start as usize..range.end as usize;
        let _page = memory.reserve(
            file.page_bound(page, &bytes[range.clone()])
                .map_err(file_error)?,
        )?;
        let batch = file
            .decode_page(page, bytes.slice_with_length(range.start, range.len()))
            .map_err(file_error)?;
        let rows = batch.num_rows();
        total += rows;
        if rows == 0
            || rows > BATCH_ROWS
            || total > MAX_COUNT
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(corrupt("Invalid spatial posting rows"));
        }
        let fixed = |index: usize| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("posting schema checked")
        };
        let partitions = fixed(0);
        let cells = batch.column(1).as_primitive::<Int64Type>();
        let iids = fixed(2);
        let iid_at = |row| {
            Iid(iids
                .value(row)
                .try_into()
                .expect("posting IID has a fixed width"))
        };
        if let Some(partition) = partition {
            let find = |cell| {
                let target = (partition.0.as_slice(), cell);
                let mut low = 0;
                let mut high = rows;
                while low < high {
                    let row = low + (high - low) / 2;
                    if (partitions.value(row), cells.value(row)) < target {
                        low = row + 1;
                    } else {
                        high = row;
                    }
                }
                low
            };
            for range in ranges {
                check()?;
                let first = find(range.start);
                for row in first..rows {
                    if (row - first) % 1024 == 0 {
                        check()?;
                    }
                    if partitions.value(row) != partition.0 || cells.value(row) > range.end {
                        break;
                    }
                    insert(iid_at(row))?;
                }
            }
        } else {
            for (row, &cell) in cells.values().iter().enumerate() {
                if row % 1024 == 0 {
                    check()?;
                }
                let first = ranges.partition_point(|range| range.end < cell);
                if ranges.get(first).is_some_and(|range| range.start <= cell) {
                    insert(iid_at(row))?;
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn work_limit() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.spatial/work-limit",
        "Spatial read or build exceeds its bounded work allowance",
    )
}

pub(crate) fn cancelled() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Interrupted,
        "rtdb.spatial/cancelled",
        "Spatial work was cancelled",
    )
}

fn corrupt(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::fault("rtdb.spatial/postings", error.to_string())
}
