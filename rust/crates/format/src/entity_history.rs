use std::{collections::BTreeSet, io::{Cursor, Write}, ops::Range, sync::{Arc, LazyLock}};

use arrow_array::{Array, FixedSizeBinaryArray, Int64Array, RecordBatch, UInt8Array, UInt32Array,
    builder::{BinaryBuilder, FixedSizeBinaryBuilder}, cast::AsArray, types::{Int64Type, UInt8Type, UInt32Type}};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{Anomaly, EventHeader, EventOp, EventOrigin, Iid, Interval, Result, Timestamp};
use sha2::{Digest, Sha256};

use crate::{data::{EncodedEvent, EventBatch, origin_array}, keys::{TableSlug, TrieKey}};

pub const KIND: &str = "entity-history";
pub const ID: &str = "headers-1";
pub const VERSION: i64 = 1;
const ROOT_MAGIC: &[u8; 8] = b"RTDBHST1";
const ENTRY_BYTES: usize = 88;
const ROWS_PER_RANGE: usize = 2048;
const MAX_ROOT_BYTES: usize = 4 << 20;
const MAX_RANGE_BYTES: usize = 16 << 20;

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

    pub fn read(self, bytes: &[u8]) -> Result<HistoryPage> {
        let batch = self.read_batch(bytes, schema())?;
        if batch.columns()[..6].iter().any(|array| array.null_count() != 0) {
            return Err(invalid("Entity history contains a null header"));
        }
        Ok(HistoryPage(batch))
    }

    pub fn read_forward(self, bytes: &[u8], columns: usize) -> Result<ForwardPage> {
        self.read_batch(bytes, &forward_schema(columns)).map(ForwardPage)
    }

    fn read_batch(self, bytes: &[u8], schema: &SchemaRef) -> Result<RecordBatch> {
        verify(bytes, self.length, self.digest)?;
        let mut reader = arrow_ipc::reader::FileReader::try_new(Cursor::new(bytes), None).map_err(invalid)?;
        if reader.schema() != *schema || reader.num_batches() != 1 {
            return Err(invalid("History range has the wrong Arrow layout"));
        }
        let batch = reader.next().ok_or_else(|| invalid("History range has no batch"))?.map_err(invalid)?;
        if batch.num_rows() != self.rows as usize || batch.column(0).null_count() != 0 {
            return Err(invalid("History range does not account for every row"));
        }
        let iids = binary(&batch, 0);
        if iids.value(0) != self.first.0 || iids.value(self.rows as usize - 1) != self.last.0
            || (1..self.rows as usize).any(|row| iids.value(row - 1) > iids.value(row)) {
            return Err(invalid("History range has inconsistent IID fences"));
        }
        Ok(batch)
    }
}

fn forward_schema(columns: usize) -> SchemaRef {
    Arc::new(Schema::new([
        Field::new("iid", DataType::FixedSizeBinary(16), false),
        Field::new("origin", DataType::FixedSizeBinary(EventOrigin::WIDTH), true),
    ].into_iter().chain((0..columns).map(|index| Field::new(format!("revision_{index}"), DataType::FixedSizeBinary(32), true))).collect::<Vec<_>>()))
}

fn binary(batch: &RecordBatch, column: usize) -> &FixedSizeBinaryArray {
    batch.column(column).as_any().downcast_ref::<FixedSizeBinaryArray>().expect("validated binary layout")
}

pub struct ForwardPage(RecordBatch);

impl ForwardPage {
    pub fn revision(&self, row: usize, iid: Iid, origin: EventOrigin, column: usize) -> Result<Option<[u8; 32]>> {
        if row >= self.0.num_rows() || column >= self.0.num_columns() - 2
            || binary(&self.0, 0).value(row) != iid.0
            || binary(&self.0, 1).is_null(row)
            || binary(&self.0, 1).value(row) != origin.0 {
            return Err(invalid("Forward witness does not identify the resolved event"));
        }
        let values = binary(&self.0, 2 + column);
        Ok(values.is_valid(row).then(|| values.value(row).try_into().expect("revision width")))
    }
}

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

    pub fn event(&self, row: usize) -> Result<(EventHeader, Option<EventOrigin>)> {
        let time = |column: usize| Timestamp(self.0.column(column).as_primitive::<Int64Type>().value(row));
        let origins = self.0.column(6).as_any().downcast_ref::<FixedSizeBinaryArray>().expect("history origin layout");
        let origin = origins.is_valid(row).then(|| EventOrigin(origins.value(row).try_into().expect("history origin width")));
        let op = match self.0.column(4).as_primitive::<UInt8Type>().value(row) {
            0 => EventOp::Put, 1 => EventOp::Delete, 2 => EventOp::Erase, 3 => EventOp::Redacted,
            _ => return Err(invalid("Entity history contains an unknown operation")),
        };
        Ok((EventHeader { iid: self.iid(row), system_from: time(1), valid_time: Interval::new(time(2), time(3))?,
            commit_position: self.0.column(5).as_primitive::<UInt32Type>().value(row), op }, origin))
    }
}

pub fn ranges(bytes: &[u8], root: HistoryRoot, owner: [u8; 32], keys: Option<&BTreeSet<Iid>>) -> Result<Vec<HistoryRange>> {
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
        if keys.is_none_or(|keys| keys.range(entry.first..=entry.last).next().is_some()) { result.push(entry); }
    }
    if previous_end != root.offset { return Err(invalid("Entity history directory omits object ranges")); }
    Ok(result)
}

pub struct HistoryWriter<W> {
    sink: W,
    offset: u64,
    directory: Vec<u8>,
    charge: Arc<MemoryReservation>,
}

impl<W: Write> HistoryWriter<W> {
    pub fn new(sink: W, owner: [u8; 32], memory: &Arc<MemoryBudget>) -> Result<Self> {
        let charge = memory.reserve(4096)?;
        let mut directory = [ROOT_MAGIC.as_slice(), &owner].concat();
        directory.extend([0; 4]);
        Ok(Self { sink, offset: 0, directory, charge })
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
            for (row, header) in (first..end).zip(&headers) {
                iids.append_value(header.iid.0).map_err(invalid)?;
                keys.append_option(events.entity_key(row)?);
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
            ]).map_err(invalid)?;
            self.write_range(page, first, headers[0].iid, headers[headers.len() - 1].iid, &batch)?;
        }
        Ok(())
    }

    pub fn append_forward(&mut self, page: u32, events: &EventBatch, columns: &[String],
        mut revision: impl FnMut(crate::data::PutText<'_>) -> Result<Option<[u8; 32]>>) -> Result<()> {
        let schema = forward_schema(columns.len());
        for first in (0..events.len()).step_by(ROWS_PER_RANGE) {
            let end = (first + ROWS_PER_RANGE).min(events.len());
            let _scratch = self.charge.budget().reserve(16_384usize.saturating_add(
                (end - first).saturating_mul(320usize.saturating_add(columns.len().saturating_mul(128)))))?;
            let mut iids = FixedSizeBinaryBuilder::with_capacity(end - first, 16);
            let mut origin = FixedSizeBinaryBuilder::with_capacity(end - first, EventOrigin::WIDTH);
            let mut revisions: Vec<_> = columns.iter().map(|_| FixedSizeBinaryBuilder::with_capacity(end - first, 32)).collect();
            let mut low = None;
            let mut high = None;
            for row in first..end {
                let event = events.borrow_event(row)?;
                let iid = event.header().iid;
                if high.is_some_and(|previous| previous > iid) { return Err(invalid("Primary page must be sorted by IID")); }
                low.get_or_insert(iid);
                high = Some(iid);
                iids.append_value(iid.0).map_err(invalid)?;
                if let Some(value) = events.origin(row)? { origin.append_value(value.0).map_err(invalid)?; } else { origin.append_null(); }
                for (column, values) in columns.iter().zip(&mut revisions) {
                    if let Some(value) = revision(event.put_text(column)?)? {
                        values.append_value(value).map_err(invalid)?;
                    } else { values.append_null(); }
                }
            }
            let arrays = [Arc::new(iids.finish()) as arrow_array::ArrayRef, Arc::new(origin.finish())]
                .into_iter().chain(revisions.iter_mut().map(|values| Arc::new(values.finish()) as arrow_array::ArrayRef)).collect();
            let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).map_err(invalid)?;
            self.write_range(page, first, low.expect("nonempty range"), high.expect("nonempty range"), &batch)?;
        }
        Ok(())
    }

    fn write_range(&mut self, page: u32, first: usize, low: Iid, high: Iid, batch: &RecordBatch) -> Result<()> {
        let mut writer = arrow_ipc::writer::FileWriter::try_new(Vec::new(), batch.schema().as_ref()).map_err(invalid)?;
        writer.write(batch).map_err(invalid)?;
        let bytes = writer.into_inner().map_err(invalid)?;
        if bytes.len() > MAX_RANGE_BYTES || self.directory.len() + ENTRY_BYTES > MAX_ROOT_BYTES {
            return Err(invalid("History exceeds its page or directory allowance"));
        }
        let lease = Arc::get_mut(&mut self.charge).expect("unpublished history writer owns its directory");
        if self.directory.len() + ENTRY_BYTES > lease.bytes() {
            lease.grow_to((lease.bytes() * 2).min(MAX_ROOT_BYTES))?;
        }
        let length = bytes.len() as u32;
        self.directory.extend(low.0);
        self.directory.extend(high.0);
        self.directory.extend(page.to_le_bytes());
        self.directory.extend(u32::try_from(first).map_err(invalid)?.to_le_bytes());
        self.directory.extend((batch.num_rows() as u32).to_le_bytes());
        self.directory.extend(self.offset.to_le_bytes());
        self.directory.extend(length.to_le_bytes());
        self.directory.extend(Sha256::digest(&bytes));
        self.sink.write_all(&bytes).map_err(invalid)?;
        self.offset += u64::from(length);
        Ok(())
    }

    pub fn finish(mut self) -> Result<(W, HistoryRoot)> {
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
