use std::fs::File;
use std::future::Future;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::sync::Arc;

use arrow_array::{
    Array, FixedSizeBinaryArray, Int64Array, RecordBatch, UInt32Array,
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
    ipc::{ArrowFileWriter, PageDecoder},
    log::proto::ArtefactRecord,
};
use rtdb_types::{Anomaly, AnomalyCategory, EventOp, Iid, Result};
use sha2::{Digest, Sha256};

use crate::{definition::Definition, kernel::Location};

const ROW_BYTES: usize = 32 + 8 + 16;
const BATCH_ROWS: usize = 16_384;
const MAX_COUNT: usize = 1_000_000;
const MAX_PAGES: usize = MAX_COUNT.div_ceil(BATCH_ROWS);
const MAX_FILE_BYTES: u64 = (ROW_BYTES * MAX_COUNT + (256 << 10)) as u64;
const MAX_TAIL_BYTES: usize = 64 << 10;
const TRAILER_BYTES: usize = 10;
const DIGEST_BYTES: usize = 32;
const FENCE_BYTES: usize = 100;
const PAGE_ENTRY_BYTES: usize = FENCE_BYTES + DIGEST_BYTES;
const HASH_CHUNK_BYTES: usize = 64 << 10;
pub(crate) const CORRUPT: &str = "rtdb.spatial/postings";

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
        let page_count = order.len().div_ceil(BATCH_ROWS);
        let directory_bytes = page_count * PAGE_ENTRY_BYTES;
        let _fences = memory.reserve(page_count * std::mem::size_of::<PageFence>())?;
        let mut fences = Vec::with_capacity(page_count);
        let mut writer = ArrowFileWriter::new_bounded_reserved(
            file, &schema, MAX_FILE_BYTES - directory_bytes as u64, memory,
        )?;
        let check = || if cancellation.is_cancelled() { Err(cancelled()) } else { Ok(()) };
        for start in (0..order.len()).step_by(BATCH_ROWS) {
            check()?;
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
            fences.push(PostingColumns::new(&batch)?.fence(&check)?);
            writer.write(&batch)?;
        }
        write_directory(writer.finish()?, &fences, memory, &check)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Root {
    start: u64,
    length: u32,
    digest: [u8; DIGEST_BYTES],
}

impl Root {
    pub(crate) fn from_record(record: &ArtefactRecord) -> Result<Self> {
        Self::decode(record.stats(), u64::try_from(record.file_size()).map_err(corrupt)?)
    }

    pub(crate) fn decode(bytes: &[u8], size: u64) -> Result<Self> {
        if bytes.len() != 12 + DIGEST_BYTES || size > MAX_FILE_BYTES {
            return Err(corrupt("Invalid spatial posting catalog receipt"));
        }
        let start = u64::from_le_bytes(bytes[..8].try_into().expect("root offset width"));
        let length = u32::from_le_bytes(bytes[8..12].try_into().expect("root length width"));
        if start < 12
            || !(TRAILER_BYTES..=MAX_TAIL_BYTES).contains(&(length as usize))
            || start.checked_add(u64::from(length)) != Some(size)
        {
            return Err(corrupt("Spatial posting directory lies outside its object"));
        }
        Ok(Self { start, length, digest: bytes[12..].try_into().expect("root digest width") })
    }

    fn encode(self) -> Vec<u8> {
        [self.start.to_le_bytes().as_slice(), &self.length.to_le_bytes(), &self.digest].concat()
    }

    pub(crate) fn range(self) -> Range<u64> {
        self.start..self.start + u64::from(self.length)
    }
}

type PostingKey = (Partition, i64);

#[derive(PartialEq, Eq)]
struct PageFence {
    rows: usize,
    first: PostingKey,
    last: PostingKey,
    min_cell: i64,
    max_cell: i64,
}

impl PageFence {
    fn encode(&self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(&(self.rows as u32).to_le_bytes());
        for (partition, cell) in [self.first, self.last] {
            bytes.extend_from_slice(&partition.0);
            bytes.extend_from_slice(&cell.to_le_bytes());
        }
        bytes.extend_from_slice(&self.min_cell.to_le_bytes());
        bytes.extend_from_slice(&self.max_cell.to_le_bytes());
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let cell = |start: usize| i64::from_le_bytes(bytes[start..start + 8].try_into().expect("cell width"));
        let key = |start: usize| (Partition(bytes[start..start + 32].try_into().expect("partition width")), cell(start + 32));
        let fence = Self {
            rows: u32::from_le_bytes(bytes[..4].try_into().expect("row count width")) as usize,
            first: key(4),
            last: key(44),
            min_cell: cell(84),
            max_cell: cell(92),
        };
        if !(1..=BATCH_ROWS).contains(&fence.rows)
            || fence.first > fence.last
            || !(fence.min_cell..=fence.max_cell).contains(&fence.first.1)
            || !(fence.min_cell..=fence.max_cell).contains(&fence.last.1)
        {
            return Err(corrupt("Invalid spatial posting page fences or row count"));
        }
        Ok(fence)
    }

    fn overlaps(&self, partition: Option<Partition>, ranges: &[KeyRange]) -> bool {
        let (mut start, mut end) = (self.min_cell, self.max_cell);
        if let Some(partition) = partition {
            if partition < self.first.0 || partition > self.last.0 {
                return false;
            }
            if partition == self.first.0 {
                start = start.max(self.first.1);
            }
            if partition == self.last.0 {
                end = end.min(self.last.1);
            }
        }
        let first = ranges.partition_point(|range| range.end < start);
        ranges.get(first).is_some_and(|range| range.start <= end)
    }
}

struct PostingColumns<'a> {
    partitions: &'a FixedSizeBinaryArray,
    cells: &'a Int64Array,
    iids: &'a FixedSizeBinaryArray,
}

impl<'a> PostingColumns<'a> {
    fn new(batch: &'a RecordBatch) -> Result<Self> {
        if !(1..=BATCH_ROWS).contains(&batch.num_rows())
            || batch.columns().iter().any(|column| column.null_count() != 0)
        {
            return Err(corrupt("Invalid spatial posting rows"));
        }
        let fixed = |index: usize| {
            batch.column(index).as_any().downcast_ref::<FixedSizeBinaryArray>()
                .expect("posting schema checked")
        };
        Ok(Self {
            partitions: fixed(0),
            cells: batch.column(1).as_primitive::<Int64Type>(),
            iids: fixed(2),
        })
    }

    fn fence(&self, check: &impl Fn() -> Result<()>) -> Result<PageFence> {
        let rows = self.cells.len();
        let key = |row| {
            (Partition(self.partitions.value(row).try_into().expect("partition width")), self.cells.value(row))
        };
        let tuple = |row| (self.partitions.value(row), self.cells.value(row), self.iids.value(row));
        let mut previous = tuple(0);
        let (mut min_cell, mut max_cell) = (previous.1, previous.1);
        for row in 1..rows {
            if row % 1024 == 0 {
                check()?;
            }
            let current = tuple(row);
            if previous >= current {
                return Err(corrupt("Spatial posting page is out of order"));
            }
            min_cell = min_cell.min(current.1);
            max_cell = max_cell.max(current.1);
            previous = current;
        }
        Ok(PageFence { rows, first: key(0), last: key(rows - 1), min_cell, max_cell })
    }

    fn visit(
        &self,
        partition: Option<Partition>,
        ranges: &[KeyRange],
        check: &impl Fn() -> Result<()>,
        insert: &mut impl FnMut(Iid) -> Result<()>,
    ) -> Result<()> {
        let rows = self.cells.len();
        let iid_at = |row| Iid(self.iids.value(row).try_into().expect("posting IID width"));
        if let Some(partition) = partition {
            let find = |cell| {
                let target = (partition.0.as_slice(), cell);
                let mut low = 0;
                let mut high = rows;
                while low < high {
                    let row = low + (high - low) / 2;
                    if (self.partitions.value(row), self.cells.value(row)) < target {
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
                    if self.partitions.value(row) != partition.0 || self.cells.value(row) > range.end {
                        break;
                    }
                    insert(iid_at(row))?;
                }
            }
        } else {
            for (row, &cell) in self.cells.values().iter().enumerate() {
                if row % 1024 == 0 {
                    check()?;
                }
                let first = ranges.partition_point(|range| range.end < cell);
                if ranges.get(first).is_some_and(|range| range.start <= cell) {
                    insert(iid_at(row))?;
                }
            }
        }
        Ok(())
    }
}

fn footer_length(trailer: &[u8]) -> Result<usize> {
    if trailer.len() != TRAILER_BYTES || &trailer[4..] != b"ARROW1" {
        return Err(corrupt("Invalid spatial posting Arrow trailer"));
    }
    let length = u32::from_le_bytes(trailer[..4].try_into().expect("footer length width")) as usize;
    if length == 0 || length > MAX_TAIL_BYTES - TRAILER_BYTES {
        return Err(corrupt("Invalid spatial posting Arrow footer length"));
    }
    Ok(length)
}

fn posting_decoder(footer: &[u8], data_end: u64) -> Result<PageDecoder> {
    let decoder = PageDecoder::from_footer(footer, data_end, None).map_err(corrupt)?;
    if decoder.schema().as_ref() != &schema()
        || decoder.dictionary_count() != 0
        || decoder.page_count() > MAX_PAGES
    {
        return Err(corrupt("Invalid spatial posting schema or page count"));
    }
    Ok(decoder)
}

fn write_directory(
    mut file: File,
    fences: &[PageFence],
    memory: &Arc<MemoryBudget>,
    check: &impl Fn() -> Result<()>,
) -> Result<(u64, Vec<u8>)> {
    check()?;
    let end = file.stream_position().map_err(corrupt)?;
    file.seek(SeekFrom::End(-(TRAILER_BYTES as i64))).map_err(corrupt)?;
    let mut trailer = [0; TRAILER_BYTES];
    file.read_exact(&mut trailer).map_err(corrupt)?;
    let footer_bytes = footer_length(&trailer)?;
    let directory_bytes = fences.len() * PAGE_ENTRY_BYTES;
    let tail_bytes = directory_bytes + footer_bytes + TRAILER_BYTES;
    if tail_bytes > MAX_TAIL_BYTES {
        return Err(work_limit());
    }
    let start = end.checked_sub((footer_bytes + TRAILER_BYTES) as u64)
        .ok_or_else(|| corrupt("Spatial posting footer offset exceeds its file"))?;
    let _scratch = memory.reserve(tail_bytes + footer_bytes * 16 + (64 << 10) + HASH_CHUNK_BYTES)?;
    let mut footer = vec![0; footer_bytes];
    file.seek(SeekFrom::Start(start)).map_err(corrupt)?;
    file.read_exact(&mut footer).map_err(corrupt)?;
    let decoder = posting_decoder(&footer, start)?;
    if decoder.page_count() != fences.len() {
        return Err(corrupt("Spatial posting writer page count differs from its fences"));
    }

    let mut directory = Vec::with_capacity(directory_bytes);
    let mut scratch = vec![0; HASH_CHUNK_BYTES];
    for (page, fence) in fences.iter().enumerate() {
        let range = decoder.page_range(page).map_err(corrupt)?;
        file.seek(SeekFrom::Start(range.start)).map_err(corrupt)?;
        let mut hasher = Sha256::new();
        let mut remaining = (range.end - range.start) as usize;
        while remaining != 0 {
            check()?;
            let chunk = remaining.min(scratch.len());
            file.read_exact(&mut scratch[..chunk]).map_err(corrupt)?;
            hasher.update(&scratch[..chunk]);
            remaining -= chunk;
        }
        fence.encode(&mut directory);
        directory.extend_from_slice(&hasher.finalize());
    }
    let digest = Sha256::new().chain_update(&directory).chain_update(&footer).chain_update(trailer).finalize().into();

    // Insert the directory before the Arrow footer so the footer and its trailer stay at the end.
    file.seek(SeekFrom::Start(start)).map_err(corrupt)?;
    file.write_all(&directory).map_err(corrupt)?;
    file.write_all(&footer).map_err(corrupt)?;
    file.write_all(&trailer).map_err(corrupt)?;
    file.flush().map_err(corrupt)?;
    check()?;
    let root = Root { start, length: tail_bytes as u32, digest };
    Ok((root.range().end, root.encode()))
}

fn sha256(bytes: &[u8], check: &impl Fn() -> Result<()>) -> Result<[u8; DIGEST_BYTES]> {
    let mut hash = Sha256::new();
    for chunk in bytes.chunks(HASH_CHUNK_BYTES) {
        check()?;
        hash.update(chunk);
    }
    Ok(hash.finalize().into())
}

struct Page {
    range: Range<u64>,
    fence: PageFence,
    digest: [u8; DIGEST_BYTES],
}

pub(crate) async fn candidates<F: Future<Output = Result<Bytes>> + Send>(
    root: Root,
    partition: Option<Partition>,
    ranges: &[KeyRange],
    memory: &Arc<MemoryBudget>,
    check: impl Fn() -> Result<()>,
    mut fetch: impl FnMut(Range<u64>) -> F,
    mut insert: impl FnMut(Iid) -> Result<()>,
) -> Result<()> {
    check()?;
    let tail = fetch(root.range()).await?;
    check()?;
    if tail.len() != root.length as usize || sha256(&tail, &check)? != root.digest {
        return Err(corrupt("Spatial posting directory does not match the held catalog"));
    }
    let trailer_start = tail.len() - TRAILER_BYTES;
    let footer_bytes = footer_length(&tail[trailer_start..])?;
    let footer_start = trailer_start.checked_sub(footer_bytes)
        .ok_or_else(|| corrupt("Spatial posting Arrow footer lies outside its directory"))?;
    let _layout = memory.reserve(footer_bytes * 16 + (64 << 10) + MAX_PAGES * std::mem::size_of::<Page>())?;
    let decoder = posting_decoder(&tail[footer_start..trailer_start], root.start)?;
    if footer_start != decoder.page_count() * PAGE_ENTRY_BYTES {
        return Err(corrupt("Spatial posting directory and Arrow footer disagree on page count"));
    }

    let mut total = 0;
    let mut previous: Option<(u64, PostingKey)> = None;
    let mut pages = Vec::with_capacity(decoder.page_count());
    for (page, entry) in tail[..footer_start].chunks_exact(PAGE_ENTRY_BYTES).enumerate() {
        let fence = PageFence::decode(&entry[..FENCE_BYTES])?;
        let range = decoder.page_range(page).map_err(corrupt)?;
        let length = range.end - range.start;
        let body = (fence.rows * ROW_BYTES) as u64;
        total += fence.rows;
        if total > MAX_COUNT
            || (page + 1 < decoder.page_count() && fence.rows != BATCH_ROWS)
            || !(body..=body + (64 << 10)).contains(&length)
            || previous.is_some_and(|(end, last)| end != range.start || last > fence.first)
        {
            return Err(corrupt("Invalid spatial posting page inventory"));
        }
        previous = Some((range.end, fence.last));
        pages.push(Page { range, fence, digest: entry[FENCE_BYTES..].try_into().expect("page digest width") });
    }
    if previous.is_some_and(|(end, _)| end + 4 != root.start) {
        return Err(corrupt("Spatial posting pages do not reach the Arrow end marker"));
    }
    drop(tail);

    for (page, Page { range, fence, digest }) in pages.into_iter().enumerate() {
        if !fence.overlaps(partition, ranges) {
            continue;
        }
        check()?;
        let length = (range.end - range.start) as usize;
        let bytes = fetch(range).await?;
        check()?;
        if bytes.len() != length || sha256(&bytes, &check)? != digest {
            return Err(corrupt("Spatial posting page does not match its directory"));
        }
        let metadata = decoder.page_metadata_range(page).map_err(corrupt)?;
        let metadata_length = (metadata.end - metadata.start) as usize;
        if decoder.page_rows(page, &bytes[..metadata_length]).map_err(corrupt)? != fence.rows {
            return Err(corrupt("Spatial posting page row count differs from its directory"));
        }
        let _page = memory.reserve(decoder.page_bound(page, &bytes).map_err(corrupt)?)?;
        let batch = decoder.decode_page(page, Buffer::from(bytes)).map_err(corrupt)?;
        let columns = PostingColumns::new(&batch)?;
        if columns.fence(&check)? != fence {
            return Err(corrupt("Spatial posting page rows or fences differ from its directory"));
        }
        columns.visit(partition, ranges, &check, &mut insert)?;
    }
    check()
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

pub(crate) fn corrupt(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::fault(CORRUPT, error.to_string())
}

#[cfg(test)]
mod tests {
    use std::{ops::Range, sync::Arc};

    use bytes::Bytes;
    use geometry::KeyRange;
    use rtdb_columnar::MemoryBudget;
    use rtdb_types::Iid;
    use tokio_util::sync::CancellationToken;

    use super::{BATCH_ROWS, CORRUPT, PAGE_ENTRY_BYTES, Partition, Postings, Root, TRAILER_BYTES,
        PageFence, cancelled, candidates, footer_length, posting_decoder, sha256};

    const SELECTED_CELL: i64 = BATCH_ROWS as i64;
    const SELECTED: Partition = Partition([1; 32]);
    const OTHER: Partition = Partition([2; 32]);
    const WANTED: Iid = Iid([3; 16]);

    #[test]
    fn mixed_partition_fences_keep_cell_extremes_for_unbound_partitions() {
        let fence = PageFence {
            rows: 4,
            first: (SELECTED, 10),
            last: (OTHER, 0),
            min_cell: -10,
            max_cell: 20,
        };
        let lower = [KeyRange { start: -10, end: -10 }];
        let upper = [KeyRange { start: 20, end: 20 }];
        assert!(fence.overlaps(None, &lower));
        assert!(fence.overlaps(None, &upper));
        assert!(fence.overlaps(Some(OTHER), &lower));
        assert!(fence.overlaps(Some(SELECTED), &upper));
        assert!(!fence.overlaps(Some(SELECTED), &lower));
        assert!(!fence.overlaps(Some(OTHER), &upper));
    }

    fn write_fixture(memory: &Arc<MemoryBudget>) -> (Vec<u8>, Root, Vec<Range<u64>>) {
        let mut postings = Postings::new(memory).unwrap();
        postings.append(SELECTED, Iid([1; 16]), (0..SELECTED_CELL).collect()).unwrap();
        postings.append(SELECTED, WANTED, (SELECTED_CELL..2 * SELECTED_CELL).collect()).unwrap();
        postings.append(OTHER, Iid([2; 16]), vec![SELECTED_CELL]).unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        let (length, stats) = postings.write(file.reopen().unwrap(), &CancellationToken::new()).unwrap();
        let bytes = std::fs::read(file.path()).unwrap();
        assert_eq!(length, bytes.len() as u64);
        let root = Root::decode(&stats, length).unwrap();
        let trailer = bytes.len() - TRAILER_BYTES;
        let start = trailer - footer_length(&bytes[trailer..]).unwrap();
        let decoder = posting_decoder(&bytes[start..trailer], root.start).unwrap();
        assert_eq!(decoder.page_count(), 3);
        let pages = (0..decoder.page_count()).map(|page| decoder.page_range(page).unwrap()).collect();
        (bytes, root, pages)
    }

    #[tokio::test]
    async fn narrow_scan_reads_only_the_directory_and_the_page_overlapping_both_fences() {
        let memory = MemoryBudget::new(32 << 20);
        let (mut bytes, root, pages) = write_fixture(&memory);
        bytes[pages[0].start as usize] ^= 1;
        bytes[pages[2].start as usize] ^= 1;
        let bytes = Bytes::from(bytes);
        let ranges = [KeyRange { start: SELECTED_CELL, end: SELECTED_CELL }];
        let mut requests = Vec::new();
        let mut found = Vec::new();
        candidates(root, Some(SELECTED), &ranges, &memory, || Ok(()), |range| {
            requests.push(range.clone());
            std::future::ready(Ok(bytes.slice(range.start as usize..range.end as usize)))
        }, |iid| { found.push(iid); Ok(()) }).await.unwrap();
        assert_eq!(requests, [root.range(), pages[1].clone()]);
        assert_eq!(found, [WANTED]);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[tokio::test]
    async fn corrupt_directory_is_rejected_before_any_page_is_requested() {
        let memory = MemoryBudget::new(32 << 20);
        let (mut bytes, root, _) = write_fixture(&memory);
        bytes[root.start as usize] ^= 1;
        let bytes = Bytes::from(bytes);
        let mut requests = Vec::new();
        let error = candidates(root, Some(SELECTED), &[KeyRange { start: SELECTED_CELL, end: SELECTED_CELL }],
            &memory, || Ok(()), |range| {
                requests.push(range.clone());
                std::future::ready(Ok(bytes.slice(range.start as usize..range.end as usize)))
            }, |_| panic!("corrupt directory supplied a candidate")).await.unwrap_err();
        assert_eq!(error.code, CORRUPT);
        assert_eq!(requests, [root.range()]);
    }

    #[tokio::test]
    async fn cancellation_during_a_page_read_stops_candidate_publication() {
        let memory = MemoryBudget::new(32 << 20);
        let (bytes, root, pages) = write_fixture(&memory);
        let bytes = Bytes::from(bytes);
        let cancellation = CancellationToken::new();
        let mut requests = Vec::new();
        let error = candidates(root, Some(SELECTED), &[KeyRange { start: 0, end: SELECTED_CELL }],
            &memory, || {
                if cancellation.is_cancelled() { Err(cancelled()) } else { Ok(()) }
            }, |range| {
                if range == pages[0] {
                    cancellation.cancel();
                }
                requests.push(range.clone());
                std::future::ready(Ok(bytes.slice(range.start as usize..range.end as usize)))
            }, |_| panic!("cancelled read supplied a candidate")).await.unwrap_err();
        assert_eq!(error.code, cancelled().code);
        assert_eq!(requests, [root.range(), pages[0].clone()]);
    }

    #[tokio::test]
    async fn selected_page_must_match_its_authenticated_digest_rows_and_fences() {
        let memory = MemoryBudget::new(32 << 20);
        let (original, root, pages) = write_fixture(&memory);
        let ranges = [KeyRange { start: SELECTED_CELL, end: SELECTED_CELL }];
        for mismatch in ["digest", "rows", "fence"] {
            let mut bytes = original.clone();
            let directory = root.start as usize;
            let mut receipt = root;
            match mismatch {
                "digest" => bytes[pages[2].end as usize - 1] ^= 1,
                "rows" => bytes[directory + 2 * PAGE_ENTRY_BYTES] = 2,
                "fence" => bytes[directory + 2 * PAGE_ENTRY_BYTES + 92..][..8]
                    .copy_from_slice(&(SELECTED_CELL + 1).to_le_bytes()),
                _ => unreachable!(),
            }
            receipt.digest = sha256(&bytes[directory..], &|| Ok(())).unwrap();
            let bytes = Bytes::from(bytes);
            let mut requests = Vec::new();
            let error = candidates(receipt, Some(OTHER), &ranges, &memory, || Ok(()), |range| {
                requests.push(range.clone());
                std::future::ready(Ok(bytes.slice(range.start as usize..range.end as usize)))
            }, |_| panic!("mismatched page supplied a candidate")).await.unwrap_err();
            assert_eq!(error.code, CORRUPT, "{mismatch}");
            assert_eq!(requests, [root.range(), pages[2].clone()], "{mismatch}");
            let message = error.message.unwrap();
            match mismatch {
                "digest" => assert_eq!(message, "Spatial posting page does not match its directory"),
                "rows" => assert_eq!(message, "Spatial posting page row count differs from its directory"),
                _ => assert_eq!(message, "Spatial posting page rows or fences differ from its directory"),
            }
        }
    }
}
