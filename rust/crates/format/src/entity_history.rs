use std::{collections::BTreeSet, io::{self, Write}, ops::Range, sync::{Arc, LazyLock}};

use arrow_array::{Array, FixedSizeBinaryArray, Int64Array, RecordBatch, UInt8Array, UInt32Array,
    builder::{BinaryBuilder, FixedSizeBinaryBuilder}, cast::AsArray, types::{Int64Type, UInt8Type, UInt32Type}};
use arrow_buffer::{Buffer, NullBufferBuilder, bit_chunk_iterator::UnalignedBitChunk};
use arrow_ipc::reader::{FileDecoder, read_footer_length};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{Anomaly, EventHeader, EventOp, EventOrigin, EventPosition, Iid, Interval, Result, Timestamp};
use sha2::{Digest, Sha256};

use crate::{data::{EncodedEvent, EventBatch, origin_array}, keys::{TableSlug, TrieKey}};

pub const KIND: &str = "entity-history";
pub const ID: &str = "headers-2";
pub const VERSION: i64 = 2;
const ROOT_MAGIC: &[u8; 8] = b"RTDBHST2";
const ENTRY_BYTES: usize = 88;
const ROWS_PER_RANGE: usize = 2048;
const MAX_ROOT_BYTES: usize = 4 << 20;
const MAX_RANGE_BYTES: usize = 16 << 20;
const REVISION_WIDTH: i32 = 32;

fn schema() -> &'static SchemaRef {
    static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(Schema::new(vec![
        Field::new("iid", DataType::FixedSizeBinary(16), false),
        Field::new("system_from", DataType::Int64, false),
        Field::new("valid_from", DataType::Int64, false),
        Field::new("valid_to", DataType::Int64, false),
        Field::new("op", DataType::UInt8, false),
        Field::new("commit_position", DataType::UInt32, false),
        Field::new("origin", DataType::FixedSizeBinary(EventOrigin::WIDTH), true),
        Field::new("entity_key", DataType::Binary, true),
        Field::new("position", DataType::FixedSizeBinary(EventPosition::WIDTH), true),
    ])));
    &SCHEMA
}

pub fn is_ranged_artefact(key: &str) -> bool {
    key.strip_prefix("tables/").and_then(|tail| tail.split_once("/artefacts/"))
        .and_then(|(slug, _)| TableSlug::recorded(slug).ok())
        .and_then(|slug| slug.parse_artefact_key(key))
        .is_some_and(|(owner, kind, id, generation)| matches!(owner, crate::keys::ArtefactOwner::Trie(_))
            && ((kind == KIND && id == ID && generation == VERSION) || (kind == "search" && id.ends_with("f16w"))))
}

pub fn owner(slug: &TableSlug, trie: &TrieKey) -> [u8; 32] {
    Sha256::digest(format!("rtdb/entity-history/{}/{}", slug.as_str(), trie).as_bytes()).into()
}

pub fn forward_owner(slug: &TableSlug, trie: &TrieKey, definition: &str, generation: i64) -> [u8; 32] {
    Sha256::digest(format!("rtdb/search-forward/{}/{trie}/{definition}/{generation}", slug.as_str()).as_bytes()).into()
}

#[derive(Clone, Copy)]
pub struct HistoryRoot { pub offset: u64, pub length: u32, digest: [u8; 32] }

impl HistoryRoot {
    pub fn encode(self) -> Vec<u8> {
        [self.offset.to_le_bytes().as_slice(), &self.length.to_le_bytes(), &self.digest].concat()
    }

    pub fn decode(bytes: &[u8], object_size: i64) -> Result<Self> {
        if bytes.len() != 44 { return Err(invalid("Entity history root has the wrong length")); }
        let offset = u64_at(bytes, 0);
        let length = u32_at(bytes, 8);
        if length < 44 || length as usize > MAX_ROOT_BYTES || offset.checked_add(u64::from(length)) != u64::try_from(object_size).ok() {
            return Err(invalid("Entity history root lies outside its object"));
        }
        Ok(Self { offset, length, digest: bytes[12..44].try_into().expect("root digest width") })
    }

    pub fn range(self) -> Range<u64> { self.offset..self.offset + u64::from(self.length) }
}

#[derive(Clone, Copy)]
pub struct HistoryRange {
    pub first: Iid,
    pub last: Iid,
    pub page: u32,
    pub first_row: u32,
    pub rows: u32,
    offset: u64,
    length: u32,
    digest: [u8; 32],
}

impl HistoryRange {
    pub fn range(self) -> Range<u64> { self.offset..self.offset + u64::from(self.length) }

    pub fn read(self, bytes: Buffer) -> Result<HistoryPage> {
        let batch = self.read_batch(bytes, schema())?;
        if batch.columns()[..6].iter().any(|array| array.null_count() != 0) {
            return Err(invalid("Entity history contains a null header"));
        }
        Ok(HistoryPage(batch))
    }

    pub fn read_forward(self, bytes: Buffer, columns: usize, column: usize, memory: &Arc<MemoryBudget>) -> Result<ForwardPage> {
        let (footer, footer_length) = self.verified_footer(&bytes)?;
        let column_count = columns.checked_add(2).ok_or_else(wrong_layout)?;
        if column >= columns { return Err(unidentified_event()); }
        let projection = [0, 1, 2 + column];
        let stored = footer_schema(footer)?;
        if stored.fields().map_or(0, |fields| fields.len()) != column_count {
            return Err(wrong_layout());
        }
        let block = batch_block(footer)?;
        let block_bytes = block_slice(&bytes, block)?;
        let selected_bytes = if footer.dictionaries().is_none_or(|blocks| blocks.is_empty()) {
            forward_projection_bytes(block, &block_bytes, column_count, self.rows as usize, &projection)?
        } else {
            None
        };
        let _layout = selected_bytes.map(|_| memory.reserve(footer_length.saturating_mul(16))).transpose()?;
        let decode_bytes = selected_bytes.unwrap_or_else(|| bytes.len().saturating_mul(4));
        let charge = memory.reserve(decode_bytes.saturating_add(4096))?;
        let stored = Arc::new(arrow_ipc::convert::fb_to_schema(stored));
        if !stored.metadata().is_empty()
            || !stored.fields().iter().zip(forward_fields(columns)).all(|(field, expected)| field.as_ref() == &expected) {
            return Err(wrong_layout());
        }
        let decoder = load_decoder(footer, stored, &bytes)?;
        let batch = if selected_bytes.is_some() {
            decode_batch(&decoder.with_projection(projection.to_vec()), block, &block_bytes)?
        } else {
            decode_batch(&decoder, block, &block_bytes)?.project(&projection).map_err(invalid)?
        };
        self.check_rows(&batch)?;
        Ok(ForwardPage { batch, _charge: charge })
    }

    fn read_batch(self, bytes: Buffer, schema: &SchemaRef) -> Result<RecordBatch> {
        let (footer, _) = self.verified_footer(&bytes)?;
        let stored = Arc::new(arrow_ipc::convert::fb_to_schema(footer_schema(footer)?));
        let decoder = load_decoder(footer, Arc::clone(&stored), &bytes)?;
        if stored != *schema { return Err(wrong_layout()); }
        let block = batch_block(footer)?;
        let batch = decode_batch(&decoder, block, &block_slice(&bytes, block)?)?;
        self.check_rows(&batch)?;
        Ok(batch)
    }

    fn verified_footer(self, bytes: &[u8]) -> Result<(arrow_ipc::Footer<'_>, usize)> {
        verify(bytes, self.length, self.digest)?;
        let footer_end = bytes.len().checked_sub(10).ok_or_else(|| invalid("Arrow file is shorter than its footer"))?;
        let footer_length = read_footer_length(bytes[footer_end..].try_into().expect("Arrow footer tail width")).map_err(invalid)?;
        let footer_start = footer_end.checked_sub(footer_length).ok_or_else(|| invalid("Arrow footer exceeds its file"))?;
        let footer = arrow_ipc::root_as_footer(&bytes[footer_start..footer_end])
            .map_err(|error| invalid(format!("Unable to get root as footer: {error:?}")))?;
        Ok((footer, footer_length))
    }

    fn check_rows(self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() != self.rows as usize || batch.column(0).null_count() != 0 {
            return Err(invalid("History range does not account for every row"));
        }
        let iids = binary(batch, 0);
        if iids.value(0) != self.first.0 || iids.value(self.rows as usize - 1) != self.last.0
            || (1..self.rows as usize).any(|row| iids.value(row - 1) > iids.value(row)) {
            return Err(invalid("History range has inconsistent IID fences"));
        }
        Ok(())
    }
}

fn footer_schema<'a>(footer: arrow_ipc::Footer<'a>) -> Result<arrow_ipc::Schema<'a>> {
    let stored = footer.schema().ok_or_else(|| invalid("Arrow footer has no schema"))?;
    if !stored.endianness().equals_to_target_endianness() {
        return Err(invalid("the endianness of the source system does not match the endianness of the target system."));
    }
    Ok(stored)
}

fn batch_block<'a>(footer: arrow_ipc::Footer<'a>) -> Result<&'a arrow_ipc::Block> {
    let blocks = footer.recordBatches().ok_or_else(|| invalid("Unable to get record batches from IPC Footer"))?;
    if blocks.len() != 1 { return Err(wrong_layout()); }
    Ok(blocks.get(0))
}

fn load_decoder(footer: arrow_ipc::Footer<'_>, schema: SchemaRef, bytes: &Buffer) -> Result<FileDecoder> {
    let mut decoder = FileDecoder::new(schema, footer.version());
    if let Some(dictionaries) = footer.dictionaries() {
        for block in dictionaries {
            decoder.read_dictionary(block, &block_slice(bytes, block)?).map_err(invalid)?;
        }
    }
    Ok(decoder)
}

fn decode_batch(decoder: &FileDecoder, block: &arrow_ipc::Block, bytes: &Buffer) -> Result<RecordBatch> {
    decoder.read_record_batch(block, bytes).map_err(invalid)?.ok_or_else(|| invalid("History range has no batch"))
}

fn block_slice(bytes: &Buffer, block: &arrow_ipc::Block) -> Result<Buffer> {
    let start = usize::try_from(block.offset()).map_err(invalid)?;
    let metadata = usize::try_from(block.metaDataLength()).map_err(invalid)?;
    let body = usize::try_from(block.bodyLength()).map_err(invalid)?;
    let length = metadata.checked_add(body).filter(|length| {
        start.checked_add(*length).is_some_and(|end| end <= bytes.len())
    }).ok_or_else(|| invalid("Arrow block exceeds its file"))?;
    if length < 4 || (length < 8 && bytes[start..start + 4] == [0xff; 4]) {
        return Err(invalid("Arrow block has a truncated prefix"));
    }
    Ok(bytes.slice_with_length(start, length))
}

fn forward_projection_bytes(block: &arrow_ipc::Block, bytes: &[u8], columns: usize, rows: usize,
    projection: &[usize]) -> Result<Option<usize>> {
    let metadata = usize::try_from(block.metaDataLength()).map_err(invalid)?;
    let prefix = if bytes[..4] == [0xff; 4] { 8 } else { 4 };
    let header = bytes.get(prefix..metadata).ok_or_else(wrong_layout)?;
    let message = arrow_ipc::root_as_message(header).map_err(invalid)?;
    let Some(batch) = message.header_as_record_batch().filter(|batch| batch.compression().is_none()) else {
        return Ok(None);
    };
    if batch.length() != rows as i64 { return Err(invalid("History range does not account for every row")); }
    let body = &bytes[metadata..];
    let buffer_count = columns.checked_mul(2).ok_or_else(wrong_layout)?;
    let nodes = batch.nodes().filter(|nodes| nodes.len() == columns).ok_or_else(wrong_layout)?;
    let buffers = batch.buffers().filter(|buffers| buffers.len() == buffer_count).ok_or_else(wrong_layout)?;
    if message.bodyLength() != body.len() as i64 || batch.variadicBufferCounts().is_some_and(|counts| !counts.is_empty()) {
        return Err(wrong_layout());
    }

    let mut selected = 0;
    for (column, node) in nodes.iter().enumerate() {
        let validity = forward_buffer(body, buffers.get(2 * column))?;
        let values = forward_buffer(body, buffers.get(2 * column + 1))?;
        let width = match column { 0 => 16, 1 => EventOrigin::WIDTH, _ => REVISION_WIDTH } as usize;
        let nulls = usize::try_from(node.null_count()).map_err(invalid)?;
        if node.length() != rows as i64 || values.len() < rows * width || nulls > rows
            || (nulls != 0 && (validity.len() < rows.div_ceil(8)
                || UnalignedBitChunk::new(validity, 0, rows).count_ones() != rows - nulls)) {
            return Err(wrong_layout());
        }
        if projection.contains(&column) { selected += validity.len() + values.len(); }
    }
    Ok(Some(selected))
}

fn forward_buffer<'a>(body: &'a [u8], buffer: &arrow_ipc::Buffer) -> Result<&'a [u8]> {
    let offset = usize::try_from(buffer.offset()).map_err(invalid)?;
    let length = usize::try_from(buffer.length()).map_err(invalid)?;
    let end = offset.checked_add(length).ok_or_else(wrong_layout)?;
    body.get(offset..end).ok_or_else(wrong_layout)
}

fn forward_schema(columns: usize) -> SchemaRef {
    Arc::new(Schema::new(forward_fields(columns).collect::<Vec<_>>()))
}

fn forward_fields(columns: usize) -> impl Iterator<Item = Field> {
    [
        Field::new("iid", DataType::FixedSizeBinary(16), false),
        Field::new("origin", DataType::FixedSizeBinary(EventOrigin::WIDTH), true),
    ].into_iter().chain((0..columns).map(|index| Field::new(format!("revision_{index}"), DataType::FixedSizeBinary(REVISION_WIDTH), true)))
}

fn binary(batch: &RecordBatch, column: usize) -> &FixedSizeBinaryArray {
    batch.column(column).as_any().downcast_ref::<FixedSizeBinaryArray>().expect("validated binary layout")
}

pub struct ForwardPage {
    batch: RecordBatch,
    _charge: Arc<MemoryReservation>,
}

impl ForwardPage {
    pub fn revision(&self, row: usize, iid: Iid, origin: EventOrigin) -> Result<Option<[u8; 32]>> {
        if row >= self.batch.num_rows() || binary(&self.batch, 0).value(row) != iid.0
            || binary(&self.batch, 1).is_null(row)
            || binary(&self.batch, 1).value(row) != origin.0 {
            return Err(unidentified_event());
        }
        let values = binary(&self.batch, 2);
        Ok(values.is_valid(row).then(|| values.value(row).try_into().expect("revision width")))
    }
}

fn wrong_layout() -> Anomaly { invalid("History range has the wrong Arrow layout") }

fn unidentified_event() -> Anomaly { invalid("Forward witness does not identify the resolved event") }

pub struct HistoryPage(RecordBatch);

impl HistoryPage {
    fn iids(&self) -> &FixedSizeBinaryArray {
        self.0.column(0).as_any().downcast_ref::<FixedSizeBinaryArray>().expect("history IID layout")
    }

    fn iid(&self, row: usize) -> Iid { Iid(self.iids().value(row).try_into().expect("history IID width")) }

    pub fn rows<'a>(&'a self, keys: &'a BTreeSet<Iid>) -> impl Iterator<Item = usize> + 'a {
        let keys = keys.range(self.iid(0)..=self.iid(self.0.num_rows() - 1));
        keys.flat_map(move |key| {
            let mut first = 0;
            let mut end = self.0.num_rows();
            while first < end {
                let middle = first + (end - first) / 2;
                if self.iid(middle) < *key { first = middle + 1; } else { end = middle; }
            }
            (first..self.0.num_rows()).take_while(move |&row| self.iid(row) == *key)
        })
    }

    /// The event's complete entity key, recorded where its table keeps one.
    pub fn entity_key(&self, row: usize) -> Option<&[u8]> {
        let keys = self.0.column(7).as_any().downcast_ref::<arrow_array::BinaryArray>().expect("history key layout");
        keys.is_valid(row).then(|| keys.value(row))
    }

    pub fn event(&self, row: usize) -> Result<(EventHeader, Option<EventOrigin>)> {
        let time = |column: usize| Timestamp(self.0.column(column).as_primitive::<Int64Type>().value(row));
        let origins = self.0.column(6).as_any().downcast_ref::<FixedSizeBinaryArray>().expect("history origin layout");
        let origin = origins.is_valid(row).then(|| EventOrigin(origins.value(row).try_into().expect("history origin width")));
        let op = match self.0.column(4).as_primitive::<UInt8Type>().value(row) {
            0 => EventOp::Put, 1 => EventOp::Delete, 2 => EventOp::Erase, 3 => EventOp::Redacted,
            _ => return Err(invalid("Entity history contains an unknown operation")),
        };
        let positions = self.0.column(8).as_any().downcast_ref::<FixedSizeBinaryArray>().expect("history position layout");
        let position = positions.is_valid(row).then(|| EventPosition::from_bytes(positions.value(row).try_into().expect("history position width")));
        Ok((EventHeader { position, iid: self.iid(row), system_from: time(1), valid_time: Interval::new(time(2), time(3))?,
            commit_position: self.0.column(5).as_primitive::<UInt32Type>().value(row), op }, origin))
    }
}

pub fn ranges(bytes: &[u8], root: HistoryRoot, owner: [u8; 32], keys: Option<&BTreeSet<Iid>>) -> Result<Vec<HistoryRange>> {
    ranges_matching(bytes, root, owner, |first, last| keys.is_none_or(|keys| keys.range(first..=last).next().is_some()))
}

pub fn ranges_matching(bytes: &[u8], root: HistoryRoot, owner: [u8; 32], keep: impl Fn(Iid, Iid) -> bool) -> Result<Vec<HistoryRange>> {
    verify(bytes, root.length, root.digest)?;
    if &bytes[..8] != ROOT_MAGIC || bytes[8..40] != owner {
        return Err(invalid("Entity history directory belongs to another primary trie"));
    }
    let count = u32_at(bytes, 40) as usize;
    if bytes.len().checked_sub(44) != count.checked_mul(ENTRY_BYTES) {
        return Err(invalid("Entity history directory has an invalid entry count"));
    }
    let mut previous_end = 0u64;
    let mut previous_page = None::<(u32, u32)>;
    let mut result = Vec::new();
    for bytes in bytes[44..].chunks_exact(ENTRY_BYTES) {
        let entry = HistoryRange {
            first: Iid(bytes[..16].try_into().expect("history IID width")),
            last: Iid(bytes[16..32].try_into().expect("history IID width")),
            page: u32_at(bytes, 32), first_row: u32_at(bytes, 36), rows: u32_at(bytes, 40),
            offset: u64_at(bytes, 44), length: u32_at(bytes, 52),
            digest: bytes[56..].try_into().expect("history digest width"),
        };
        let next_page = previous_page.map_or(entry.page == 0 && entry.first_row == 0, |(page, after)| {
            (entry.page == page && entry.first_row == after)
                || (page.checked_add(1) == Some(entry.page) && entry.first_row == 0)
        });
        if entry.first > entry.last || entry.rows == 0 || entry.rows as usize > ROWS_PER_RANGE || !next_page
            || entry.offset != previous_end || entry.length == 0 || entry.length as usize > MAX_RANGE_BYTES {
            return Err(invalid("Entity history directory has an invalid page range"));
        }
        previous_page = Some((entry.page, entry.first_row.checked_add(entry.rows).ok_or_else(|| invalid("Entity history row offset overflow"))?));
        previous_end = entry.offset.checked_add(u64::from(entry.length)).filter(|end| *end <= root.offset)
            .ok_or_else(|| invalid("Entity history range extends beyond its directory"))?;
        if keep(entry.first, entry.last) { result.push(entry); }
    }
    if previous_end != root.offset { return Err(invalid("Entity history directory omits object ranges")); }
    Ok(result)
}

pub struct HistoryWriter<W> {
    sink: W,
    offset: u64,
    directory: Vec<u8>,
    charge: Arc<MemoryReservation>,
    failed_range: bool,
}

struct RangeSink<'a, W> {
    sink: &'a mut W,
    length: usize,
    digest: Sha256,
    exceeded: bool,
}

impl<W: Write> Write for RangeSink<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_RANGE_BYTES - self.length {
            self.exceeded = true;
            return Err(io::Error::other("History exceeds its page or directory allowance"));
        }
        let written = self.sink.write(bytes)?;
        self.digest.update(&bytes[..written]);
        self.length += written;
        Ok(written)
    }

    // The outer history writer flushes the sink after it writes the directory.
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

impl<W: Write> HistoryWriter<W> {
    pub fn new(sink: W, owner: [u8; 32], memory: &Arc<MemoryBudget>) -> Result<Self> {
        let charge = memory.reserve(4096)?;
        let mut directory = Vec::with_capacity(charge.bytes());
        directory.extend(ROOT_MAGIC);
        directory.extend(owner);
        directory.extend([0; 4]);
        Ok(Self { sink, offset: 0, directory, charge, failed_range: false })
    }

    pub fn append(&mut self, page: u32, events: &EventBatch) -> Result<()> {
        for first in (0..events.len()).step_by(ROWS_PER_RANGE) {
            let end = (first + ROWS_PER_RANGE).min(events.len());
            let bytes = (first..end).try_fold(16_384usize, |bytes, row| {
                bytes.checked_add(512 + events.entity_key(row)?.map_or(0, <[u8]>::len).saturating_mul(4))
                    .ok_or_else(|| invalid("Entity history page size overflow"))
            })?;
            let _scratch = self.charge.budget().reserve(bytes)?;
            let headers = (first..end).map(|row| events.header(row)).collect::<Result<Vec<_>>>()?;
            if headers.windows(2).any(|pair| pair[0].iid > pair[1].iid) { return Err(invalid("Primary page must be sorted by IID")); }
            let mut iids = FixedSizeBinaryBuilder::with_capacity(end - first, 16);
            let mut keys = BinaryBuilder::new();
            let mut positions = FixedSizeBinaryBuilder::with_capacity(end - first, EventPosition::WIDTH);
            for (row, header) in (first..end).zip(&headers) {
                iids.append_value(header.iid.0).map_err(invalid)?;
                keys.append_option(events.entity_key(row)?);
                match header.position {
                    Some(position) => positions.append_value(position.to_bytes()).map_err(invalid)?,
                    None => positions.append_null(),
                }
            }
            let origins = (first..end).map(|row| events.origin(row)).collect::<Result<Vec<_>>>()?;
            let batch = RecordBatch::try_new(Arc::clone(schema()), vec![
                Arc::new(iids.finish()),
                Arc::new(Int64Array::from_iter_values(headers.iter().map(|header| header.system_from.0))),
                Arc::new(Int64Array::from_iter_values(headers.iter().map(|header| header.valid_time.lower().0))),
                Arc::new(Int64Array::from_iter_values(headers.iter().map(|header| header.valid_time.upper().0))),
                Arc::new(UInt8Array::from_iter_values(headers.iter().map(|header| match header.op {
                    EventOp::Put => 0, EventOp::Delete => 1, EventOp::Erase => 2, EventOp::Redacted => 3,
                }))),
                Arc::new(UInt32Array::from_iter_values(headers.iter().map(|header| header.commit_position))),
                Arc::new(origin_array(origins)?),
                Arc::new(keys.finish()),
                Arc::new(positions.finish()),
            ]).map_err(invalid)?;
            self.write_range(page, first, headers[0].iid, headers[headers.len() - 1].iid, &batch)?;
        }
        Ok(())
    }

    pub fn append_forward(&mut self, page: u32, events: &EventBatch, columns: &[String],
        mut revision: impl FnMut(crate::data::PutText<'_>) -> Result<Option<[u8; 32]>>) -> Result<()> {
        self.append_forward_revisions(page, events, columns.len(), |_, event, column| revision(event.put_text(&columns[column])?))
    }

    pub fn append_forward_revisions(&mut self, page: u32, events: &EventBatch, columns: usize,
        mut revision: impl FnMut(usize, &crate::data::EventRef<'_>, usize) -> Result<Option<[u8; 32]>>) -> Result<()> {
        let schema = forward_schema(columns);
        for first in (0..events.len()).step_by(ROWS_PER_RANGE) {
            let end = (first + ROWS_PER_RANGE).min(events.len());
            let len = end - first;
            let plain_iids = match events.iids() {
                Ok(rtdb_columnar::IidView::Plain(values)) => Some(values),
                _ => None,
            };
            let origins = events.origins().ok().flatten().filter(|origins| (first..end).all(|row| {
                origins.is_valid(row) || origins.value(row).iter().all(|&byte| byte == 0)
            }));
            let controls = usize::from(plain_iids.is_none()) * 16
                + if origins.is_none() { EventOrigin::WIDTH as usize } else { 0 };
            let _scratch = self.charge.budget().reserve(16_384usize.saturating_add(
                len.saturating_mul(224usize.saturating_add(controls).saturating_add(columns.saturating_mul(128)))))?;
            let mut iids = plain_iids.is_none().then(|| FixedSizeBinaryBuilder::with_capacity(len, 16));
            let mut origin = origins.is_none().then(|| FixedSizeBinaryBuilder::with_capacity(len, EventOrigin::WIDTH));
            let mut revisions: Vec<_> = (0..columns).map(|_| FixedSizeBinaryBuilder::with_capacity(end - first, 32)).collect();
            let mut low = None;
            let mut high = None;
            for row in first..end {
                let event = events.borrow_event(row)?;
                let iid = event.header().iid;
                if high.is_some_and(|previous| previous > iid) { return Err(invalid("Primary page must be sorted by IID")); }
                low.get_or_insert(iid);
                high = Some(iid);
                if let Some(iids) = &mut iids { iids.append_value(iid.0).map_err(invalid)?; }
                if let Some(origin) = &mut origin {
                    if let Some(value) = event.origin() { origin.append_value(value.0).map_err(invalid)?; }
                    else { origin.append_null(); }
                }
                for (column, values) in revisions.iter_mut().enumerate() {
                    if let Some(value) = revision(row, &event, column)? {
                        values.append_value(value).map_err(invalid)?;
                    } else { values.append_null(); }
                }
            }
            let iids = match iids {
                Some(mut iids) => iids.finish(),
                None => plain_iids.expect("plain IIDs are available").slice(first, len),
            };
            let origins = match origin {
                Some(mut origin) => origin.finish(),
                None => {
                    let slice = origins.expect("source origins are available").slice(first, len);
                    if slice.null_count() == 0 { slice } else {
                        let mut valid = NullBufferBuilder::new(len);
                        for row in 0..len { valid.append(slice.is_valid(row)); }
                        FixedSizeBinaryArray::try_new(EventOrigin::WIDTH, slice.values().clone(), valid.finish()).map_err(invalid)?
                    }
                }
            };
            let arrays = [Arc::new(iids) as arrow_array::ArrayRef, Arc::new(origins)]
                .into_iter().chain(revisions.iter_mut().map(|values| Arc::new(values.finish()) as arrow_array::ArrayRef)).collect();
            let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).map_err(invalid)?;
            self.write_range(page, first, low.expect("nonempty range"), high.expect("nonempty range"), &batch)?;
        }
        Ok(())
    }

    fn write_range(&mut self, page: u32, first: usize, low: Iid, high: Iid, batch: &RecordBatch) -> Result<()> {
        if self.failed_range { return Err(invalid("History contains an incomplete range")); }
        self.failed_range = true;
        let directory_end = self.directory.len() + ENTRY_BYTES;
        if directory_end > MAX_ROOT_BYTES {
            return Err(invalid("History exceeds its page or directory allowance"));
        }
        let first = u32::try_from(first).map_err(invalid)?;
        self.offset.checked_add(MAX_RANGE_BYTES as u64).ok_or_else(|| invalid("History range offset overflow"))?;
        let lease = Arc::get_mut(&mut self.charge).expect("unpublished history writer owns its directory");
        if directory_end > lease.bytes() {
            lease.grow_to((lease.bytes() * 2).min(MAX_ROOT_BYTES))?;
        }
        if directory_end > self.directory.capacity() {
            self.directory.try_reserve_exact(lease.bytes() - self.directory.len()).map_err(invalid)?;
        }
        let mut sink = RangeSink { sink: &mut self.sink, length: 0, digest: Sha256::new(), exceeded: false };
        let written = (|| {
            let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut sink, batch.schema_ref())?;
            writer.write(batch)?;
            writer.finish()
        })();
        if sink.exceeded { return Err(invalid("History exceeds its page or directory allowance")); }
        written.map_err(invalid)?;
        let length = sink.length as u32;
        let digest = sink.digest.finalize();
        self.directory.extend(low.0);
        self.directory.extend(high.0);
        self.directory.extend(page.to_le_bytes());
        self.directory.extend(first.to_le_bytes());
        self.directory.extend((batch.num_rows() as u32).to_le_bytes());
        self.directory.extend(self.offset.to_le_bytes());
        self.directory.extend(length.to_le_bytes());
        self.directory.extend(digest);
        self.offset += u64::from(length);
        self.failed_range = false;
        Ok(())
    }

    pub fn finish(mut self) -> Result<(W, HistoryRoot)> {
        if self.failed_range { return Err(invalid("History contains an incomplete range")); }
        let count = ((self.directory.len() - 44) / ENTRY_BYTES) as u32;
        self.directory[40..44].copy_from_slice(&count.to_le_bytes());
        let root = HistoryRoot { offset: self.offset, length: self.directory.len() as u32, digest: Sha256::digest(&self.directory).into() };
        self.sink.write_all(&self.directory).map_err(invalid)?;
        self.sink.flush().map_err(invalid)?;
        Ok((self.sink, root))
    }
}

fn verify(bytes: &[u8], length: u32, digest: [u8; 32]) -> Result<()> {
    if bytes.len() != length as usize || <[u8; 32]>::from(Sha256::digest(bytes)) != digest {
        return Err(invalid("Entity history range failed authentication"));
    }
    Ok(())
}

fn u32_at(bytes: &[u8], first: usize) -> u32 { u32::from_le_bytes(bytes[first..first + 4].try_into().expect("validated history layout")) }
fn u64_at(bytes: &[u8], first: usize) -> u64 { u64::from_le_bytes(bytes[first..first + 8].try_into().expect("validated history layout")) }
fn invalid(error: impl std::fmt::Display) -> Anomaly { Anomaly::fault("rtdb.format/entity-history", error.to_string()) }

#[test]
fn history_range_reuses_authenticated_arrow_buffers() {
    let iid = Iid([3; 16]);
    let event = crate::data::StoredEvent {
        position: None, origin: None, commit_position: 0, entity_key: None, recency: None,
        iid, system_from: Timestamp(4), valid_time: Interval::ALL,
        operation: crate::data::StoredOperation::Delete,
    };
    let source = EventBatch::new(crate::data::build_event_batch(&[event], None, None).unwrap()).unwrap();
    let owner = [42; 32];
    let mut writer = HistoryWriter::new(Vec::new(), owner, &MemoryBudget::new(1 << 20)).unwrap();
    writer.append(0, &source).unwrap();
    let (bytes, root) = writer.finish().unwrap();
    let encoded = Buffer::from_vec(bytes);
    let range = ranges(&encoded[root.offset as usize..], root, owner, None).unwrap()[0];
    let file = encoded.slice_with_length(range.offset as usize, range.length as usize);
    let reference_bytes = file.to_vec();
    let reference = arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(&reference_bytes), None)
        .unwrap().next().unwrap().unwrap();
    let inside = file.as_ptr() as usize..file.as_ptr() as usize + file.len();

    let decoded = range.read(file).unwrap();
    drop(encoded);
    assert!(inside.contains(&(decoded.iids().value(0).as_ptr() as usize)));
    assert_eq!(decoded.0, reference);
    let mut writer = arrow_ipc::writer::FileWriter::try_new(Vec::new(), decoded.0.schema_ref()).unwrap();
    writer.write(&decoded.0).unwrap();
    assert_eq!(writer.into_inner().unwrap(), reference_bytes);
}


#[test]
fn forward_ranges_preserve_bytes_for_plain_and_run_end_iids() {
    let iid = Iid([3; 16]);
    let mut events = vec![crate::data::StoredEvent {
        position: None, origin: Some(EventOrigin([7; EventOrigin::WIDTH as usize])),
        commit_position: 0, entity_key: None, recency: None, iid, system_from: Timestamp(4),
        valid_time: Interval::ALL, operation: crate::data::StoredOperation::Delete,
    }; 2];
    events[1].origin = None;
    let memory = MemoryBudget::new(1 << 20);
    let encode = |encoding| {
        let events = EventBatch::new(crate::data::build_event_batch(&events, None, encoding).unwrap()).unwrap();
        let mut writer = HistoryWriter::new(Vec::new(), [42; 32], &memory).unwrap();
        writer.append_forward_revisions(0, &events, 1, |row, _, _| Ok((row == 0).then_some([5; 32]))).unwrap();
        writer.finish().unwrap().0
    };
    assert_eq!(encode(None), encode(Some(rtdb_types::RunEndWidth::I16)));
}

#[test]
fn a_partially_written_history_range_cannot_be_finalized() {
    struct BrokenSink(usize);
    impl Write for BrokenSink {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.0 == 0 { return Err(io::Error::other("sink failed")); }
            let written = bytes.len().min(self.0);
            self.0 -= written;
            Ok(written)
        }

        fn flush(&mut self) -> io::Result<()> { Ok(()) }
    }

    let batch = RecordBatch::new_empty(Arc::clone(schema()));
    let mut writer = HistoryWriter::new(BrokenSink(12), [42; 32], &MemoryBudget::new(1 << 20)).unwrap();
    let iid = Iid([3; 16]);
    assert!(writer.write_range(0, 0, iid, iid, &batch).is_err());
    assert!(writer.finish().is_err());
}
