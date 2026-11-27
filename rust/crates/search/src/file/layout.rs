use std::collections::BTreeMap;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::{Range, RangeInclusive};
use std::sync::{Arc, LazyLock, OnceLock, Weak};

use arrow_array::{Array, ArrayRef, LargeListArray, RecordBatch, StructArray};
use arrow_buffer::{Buffer, OffsetBuffer};
use arrow_ipc::{Block, MetadataVersion, convert::fb_to_schema, reader::{FileDecoder, read_footer_length}, writer::FileWriter};
use arrow_schema::{DataType as T, Field, FieldRef, Fields, Schema, SchemaRef};
use futures::{StreamExt, TryStreamExt};
use rtdb_storage::{ObjectKey, cache::BufferPool};
use rtdb_types::Result;
use sha2::{Digest, Sha256};

use super::{FORMAT_VERSION, LEGACY_FORMAT_VERSION, corrupt, fault};

const CHECKSUMS: &str = "rtdb.search.record_batch.sha256";

#[derive(Clone, Copy)]
pub(super) enum Table {
    Header,
    Columns,
    Members,
    Pages,
    Events,
    Runs,
    RowRuns,
    Sections,
    Terms,
    Postings,
    Windows,
    Blocks,
    Frequencies,
    Positions,
    Directory,
    Root,
    ImpactIds,
    Synopsis,
    RankNodes,
    Versions,
    Routes,
    TextRevisions,
    TokenSequences,
    Erases,
}

impl Table {
    const ALL: [Self; 24] = [
        Self::Header, Self::Columns, Self::Members, Self::Pages, Self::Events,
        Self::Runs, Self::RowRuns, Self::Sections, Self::Terms, Self::Postings,
        Self::Windows, Self::Blocks, Self::Frequencies, Self::Positions, Self::Directory, Self::Root, Self::ImpactIds, Self::Synopsis, Self::RankNodes, Self::Versions, Self::Routes,
        Self::TextRevisions, Self::TokenSequences, Self::Erases,
    ];

    fn field(self) -> Field {
        let (name, fields) = match self {
            Self::Header => ("header", fields([
                ("format", T::Int32, false), ("analyzer", T::Int32, false),
                ("definition", T::Int64, true), ("identity", T::FixedSizeBinary(32), false),
            ])),
            Self::Columns => ("columns", fields([
                ("name", T::Utf8, false), ("runs", T::UInt32, false), ("tokens", T::UInt64, false),
            ])),
            Self::Members => ("members", fields([("first_page", T::UInt32, false), ("first_run", T::UInt32, false)])),
            Self::Pages => ("pages", fields([("first_event", T::UInt32, false)])),
            Self::Events => ("events", fields([
                ("iid", T::FixedSizeBinary(16), false), ("system_from", T::Int64, false),
                ("valid_from", T::Int64, false), ("valid_to", T::Int64, false), ("op", T::UInt8, false),
                ("entity_key", T::Binary, true),
                ("event_position", T::FixedSizeBinary(rtdb_types::EventPosition::WIDTH), true),
            ])),
            Self::Runs => ("runs", fields([
                ("iid", T::FixedSizeBinary(16), false), ("token_length", T::UInt32, false),
                ("entity_key", T::Binary, true),
                ("lifespan", T::UInt8, false),
                ("rectangles", list_type(T::Struct(fields([
                    ("valid_from", T::Int64, false), ("valid_to", T::Int64, false),
                    ("system_from", T::Int64, false), ("system_to", T::Int64, false),
                ]))), false),
            ])),
            Self::RowRuns => ("row_runs", fields([("runs", list_type(T::UInt32), false)])),
            Self::Sections => ("sections", section_fields()),
            Self::Terms => ("terms", fields([
                ("column", T::UInt32, false), ("term", T::Utf8, false),
                ("page", T::UInt32, false), ("index", T::UInt32, false),
                ("frequency", T::UInt32, false),
            ])),
            Self::Postings => ("postings", fields([
                ("documents", list_type(T::UInt32), false),
                ("positions", list_type(list_type(T::UInt32)), false),
                ("windows", list_type(T::Struct(fields([
                    ("window", T::UInt32, false), ("frequency", T::UInt32, false), ("length", T::UInt32, false),
                ]))), false),
            ])),
            Self::Windows => ("sql_order", fields([("first_id", T::Int64, true)])),
            Self::Blocks => ("blocks", fields([("blocks", list_type(T::Struct(fields([
                ("page", T::UInt32, false), ("slot", T::UInt32, false), ("documents", T::UInt32, false),
                ("impacts", list_type(T::Struct(fields([
                    ("window", T::UInt32, false), ("frequency", T::UInt32, false),
                    ("length", T::UInt32, false), ("same_frequency", T::Boolean, false), ("same_length", T::Boolean, false),
                    ("max_unit_score", T::Float64, false),
                ]))), false),
            ]))), false)])),
            Self::Frequencies => ("frequencies", fields([
                ("documents", list_type(T::UInt32), false), ("counts", list_type(T::UInt32), false),
            ])),
            Self::Positions => ("positions", fields([("positions", list_type(list_type(T::UInt32)), false)])),
            Self::Directory => ("directory", fields([
                ("lower", T::Binary, false), ("upper", T::Binary, false),
                ("offset", T::UInt64, false), ("metadata", T::UInt32, false), ("body", T::UInt64, false),
                ("digest", T::FixedSizeBinary(32), false),
            ])),
            Self::Root => ("root", fields([
                ("kind", T::UInt8, false), ("height", T::UInt8, false),
                ("offset", T::UInt64, false), ("metadata", T::UInt32, false), ("body", T::UInt64, false),
                ("digest", T::FixedSizeBinary(32), false),
            ])),
            Self::ImpactIds => ("impact_ids", fields([
                ("ids", list_type(T::Struct(fields([
                    ("window", T::UInt32, false), ("minimum", T::Int64, true), ("peak_minimum", T::Int64, true),
                ]))), false),
            ])),
            Self::Synopsis => ("term_synopsis", fields([("bits", T::FixedSizeBinary(512), false)])),
            Self::RankNodes => ("rank_nodes", fields([
                ("first_window", T::UInt32, false), ("last_window", T::UInt32, false),
                ("height", T::UInt8, false),
                ("offset", T::UInt64, false), ("metadata", T::UInt32, false), ("body", T::UInt64, false),
                ("digest", T::FixedSizeBinary(32), false),
                ("frequency", T::UInt32, false), ("stored_length", T::UInt32, false),
                ("unit_score", T::Float64, false), ("minimum", T::Int64, true), ("peak_minimum", T::Int64, true),
            ])),
            Self::Versions => ("versions", fields([
                ("commit_position", T::UInt32, false), ("member", T::UInt32, false),
                ("page", T::UInt32, false), ("row", T::UInt32, false),
            ])),
            Self::Routes => ("routes", fields([
                ("kind", T::UInt8, false), ("lower", T::Binary, false), ("upper", T::Binary, false),
                ("offset", T::UInt64, false), ("metadata", T::UInt32, false), ("body", T::UInt64, false),
                ("digest", T::FixedSizeBinary(32), false),
            ])),
            Self::TextRevisions => ("text_revisions", fields([("digests", list_type(T::FixedSizeBinary(33)), false)])),
            Self::TokenSequences => ("token_sequences", fields([
                ("column", T::UInt32, false), ("revision", T::FixedSizeBinary(32), false),
                ("tokens", list_type(T::Utf8), false),
                ("system_from", T::Int64, false), ("commit_position", T::UInt32, false),
                ("event_position", T::FixedSizeBinary(rtdb_types::EventPosition::WIDTH), true),
            ])),
            Self::Erases => ("erases", fields([
                ("iid", T::FixedSizeBinary(16), false),
                ("system_from", T::Int64, false), ("commit_position", T::UInt32, false),
                ("event_position", T::FixedSizeBinary(rtdb_types::EventPosition::WIDTH), true),
            ])),
        };
        Field::new(name, list_type(T::Struct(fields)), false)
    }

    pub(super) fn fields(self) -> Fields {
        let T::LargeList(item) = schema(super::LOGICAL_FORMAT_VERSION).field(self as usize).data_type().clone() else { unreachable!() };
        let T::Struct(fields) = item.data_type() else { unreachable!() };
        fields.clone()
    }

    pub(super) fn array(self, columns: Vec<ArrayRef>) -> Result<StructArray> {
        StructArray::try_new(self.fields(), columns, None).map_err(fault)
    }
}

pub(super) fn section_fields() -> Fields {
    fields([
        ("class", T::UInt8, false), ("valid_from", T::Int64, false), ("valid_to", T::Int64, false),
        ("system_from", T::Int64, false), ("system_to", T::Int64, false),
        ("birth", T::Int64, false), ("death", T::Int64, false),
    ])
}

fn fields<const N: usize>(names: [(&str, T, bool); N]) -> Fields {
    names.into_iter().map(|(name, ty, nullable)| Field::new(name, ty, nullable)).collect::<Vec<_>>().into()
}

fn list_type(ty: T) -> T { T::LargeList(Arc::new(Field::new("item", ty, false))) }

fn tables(format: i32) -> &'static [Table] {
    match format {
        LEGACY_FORMAT_VERSION => &Table::ALL[..10],
        FORMAT_VERSION => &Table::ALL[..14],
        super::TREE_FORMAT_VERSION => &Table::ALL[..19],
        super::PACKED_FORMAT_VERSION => &Table::ALL[..21],
        _ => &Table::ALL,
    }
}

pub(super) fn schema(format: i32) -> &'static SchemaRef {
    static LEGACY: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(Schema::new(tables(LEGACY_FORMAT_VERSION).iter().copied().map(Table::field).collect::<Vec<_>>())));
    static PAGED: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(Schema::new(tables(FORMAT_VERSION).iter().copied().map(Table::field).collect::<Vec<_>>())));
    static TREE: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(Schema::new(tables(super::TREE_FORMAT_VERSION).iter().copied().map(Table::field).collect::<Vec<_>>())));
    static PACKED: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(Schema::new(tables(super::PACKED_FORMAT_VERSION).iter().copied().map(Table::field).collect::<Vec<_>>())));
    static LOGICAL: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(Schema::new(Table::ALL.map(Table::field).to_vec())));
    match format {
        LEGACY_FORMAT_VERSION => &LEGACY,
        FORMAT_VERSION => &PAGED,
        super::PACKED_FORMAT_VERSION => &PACKED,
        super::LOGICAL_FORMAT_VERSION => &LOGICAL,
        _ => &TREE,
    }
}

pub(super) fn item_of(field: &Field) -> FieldRef {
    let T::LargeList(item) = field.data_type() else { unreachable!() };
    Arc::clone(item)
}

pub(super) fn list(values: ArrayRef, lengths: impl IntoIterator<Item = usize>, item: FieldRef) -> Result<ArrayRef> {
    let offsets = OffsetBuffer::from_lengths(lengths);
    Ok(Arc::new(LargeListArray::try_new(item, offsets, values, None).map_err(fault)?))
}

pub(super) fn table(batch: &RecordBatch, table: Table) -> StructArray {
    let array = batch.column(table as usize).as_any().downcast_ref::<LargeListArray>().expect("search file layout");
    let value = array.value(0);
    value.as_any().downcast_ref::<StructArray>().expect("search file layout").clone()
}

pub(super) fn record(format: i32, populated: impl IntoIterator<Item = (Table, StructArray)>) -> Result<RecordBatch> {
    let mut contents: [Option<StructArray>; Table::ALL.len()] = std::array::from_fn(|_| None);
    for (table, array) in populated { contents[table as usize] = Some(array); }
    let schema = schema(format);
    let columns = tables(format).iter().copied().zip(contents).map(|(table, content)| {
        let content = content.unwrap_or_else(|| StructArray::new_empty_fields(0, None));
        let item = item_of(schema.field(table as usize));
        let values: ArrayRef = if content.num_columns() == 0 {
            arrow_array::new_empty_array(item.data_type())
        } else {
            Arc::new(content)
        };
        let len = values.len();
        list(values, [len], item)
    }).collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(fault)
}

pub(crate) struct FileDigestWriter<W> {
    output: W,
    digest: Sha256,
}

impl<W> FileDigestWriter<W> {
    pub(crate) fn new(output: W) -> Self {
        Self { output, digest: Sha256::new() }
    }

    pub(crate) fn into_digest(self) -> [u8; 32] {
        self.digest.finalize().into()
    }
}

impl<W: Write> Write for FileDigestWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.output.write(buf)?;
        self.digest.update(&buf[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

struct DigestWriter<W> {
    writer: W,
    digest: Sha256,
    position: usize,
    prefix: [u8; 8],
    captured: usize,
    root: Option<Vec<u8>>,
}

impl<W: Write> Write for DigestWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.writer.write(buf)?;
        self.digest.update(&buf[..written]);
        let copied = written.min(self.prefix.len() - self.captured);
        self.prefix[self.captured..self.captured + copied].copy_from_slice(&buf[..copied]);
        self.captured += copied;
        if let Some(root) = &mut self.root {
            if written <= super::MAX_ROOT_BYTES as usize - root.len() {
                root.extend_from_slice(&buf[..written]);
            } else {
                self.root = None;
            }
        }
        self.position += written;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> { self.writer.flush() }
}

pub(super) struct Writer<W> {
    writer: FileWriter<DigestWriter<W>>,
    checksums: String,
    format: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct PageRef {
    pub offset: u64,
    pub metadata: u32,
    pub body: u64,
    pub digest: [u8; 32],
}

pub(super) struct AuthenticatedPage {
    page: PageRef,
    bytes: Buffer,
}

impl PageRef {
    pub fn range(self) -> Result<Range<u64>> {
        let end = self.offset.checked_add(u64::from(self.metadata))
            .and_then(|end| end.checked_add(self.body))
            .filter(|_| self.offset >= 8 && self.metadata >= 8)
            .ok_or_else(|| corrupt("Search page locator exceeds its object"))?;
        Ok(self.offset..end)
    }

    pub fn authenticate(self, bytes: Buffer) -> Result<AuthenticatedPage> {
        let range = self.range()?;
        if range.end - range.start != bytes.len() as u64 || <[u8; 32]>::from(Sha256::digest(bytes.as_slice())) != self.digest {
            return Err(corrupt("Search page does not match its authenticated locator"));
        }
        Ok(AuthenticatedPage { page: self, bytes })
    }

    pub fn decode(self, bytes: Buffer, decoder: &FileDecoder) -> Result<RecordBatch> {
        self.authenticate(bytes)?.decode(decoder)
    }
}

pub(crate) fn inspect_uncompressed_record(bytes: &[u8], metadata: usize, body: usize,
    rows: RangeInclusive<usize>,
) -> Result<usize> {
    let bad = || corrupt("Search page has an invalid Arrow record layout");
    if metadata.checked_add(body) != Some(bytes.len()) || !bytes.starts_with(&[0xff; 4]) {
        return Err(bad());
    }
    let encoded = bytes.get(4..8).ok_or_else(bad)?;
    let header = i32::from_le_bytes(encoded.try_into().expect("four metadata length bytes"));
    if usize::try_from(header).ok() != metadata.checked_sub(8) || metadata < 8 {
        return Err(bad());
    }
    let message = arrow_ipc::root_as_message(&bytes[8..metadata]).map_err(fault)?;
    let record = message.header_as_record_batch().ok_or_else(bad)?;
    if message.version() != MetadataVersion::V5 || usize::try_from(message.bodyLength()).ok() != Some(body)
        || record.compression().is_some() || record.variadicBufferCounts().is_some_and(|counts| !counts.is_empty()) {
        return Err(bad());
    }
    if !usize::try_from(record.length()).is_ok_and(|count| rows.contains(&count)) {
        return Err(corrupt("Search page has an invalid row count"));
    }
    let nodes = record.nodes().ok_or_else(bad)?;
    let buffers = record.buffers().ok_or_else(bad)?;
    for node in nodes.iter() {
        if node.length() < 0 || node.null_count() < 0 || node.null_count() > node.length() {
            return Err(bad());
        }
    }
    let descriptors = 8192usize.saturating_add(nodes.len().saturating_mul(512)).saturating_add(buffers.len().saturating_mul(256));
    buffers.into_iter().try_fold(descriptors, |bound, buffer| {
        let start = usize::try_from(buffer.offset()).map_err(fault)?;
        let len = usize::try_from(buffer.length()).map_err(fault)?;
        if start.checked_add(len).is_none_or(|end| end > body) { return Err(bad()); }
        bound.checked_add(len).ok_or_else(bad)
    })
}

impl AuthenticatedPage {
    pub fn decode_bound(&self) -> Result<usize> {
        inspect_uncompressed_record(&self.bytes, self.page.metadata as usize, usize::try_from(self.page.body).map_err(fault)?, 1..=1)
    }

    pub fn decode_admitted(mut self, decoder: &FileDecoder,
        reserve: impl Fn(usize) -> Result<Arc<rtdb_types::MemoryReservation>>, detach: bool,
    ) -> Result<RecordBatch> {
        let bound = self.decode_bound()?;
        let wire = self.bytes.len();
        let mut charge = reserve(bound)?;
        if detach {
            let copy = reserve(wire.next_multiple_of(64))?;
            let bytes = Buffer::from_slice_ref(self.bytes.as_slice());
            self.bytes = rtdb_columnar::retain_buffer(&bytes, &copy);
        }
        let backing = self.bytes.clone();
        let batch = self.decode(decoder)?;
        let resident = rtdb_columnar::shared_allocation_size_excluding(&batch, &backing)?;
        let retained = Arc::get_mut(&mut charge).expect("decoded page reservation remains private").split_off(resident)?;
        let columns = batch.columns().iter().map(|array| rtdb_columnar::retain_array(Arc::clone(array), &retained))
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(batch.schema(), columns).map_err(fault)
    }

    pub fn decode(self, decoder: &FileDecoder) -> Result<RecordBatch> {
        let page = self.page;
        let block = Block::new(i64::try_from(page.offset).map_err(fault)?,
            i32::try_from(page.metadata).map_err(fault)?, i64::try_from(page.body).map_err(fault)?);
        let batch = decoder.read_record_batch(&block, &self.bytes).map_err(fault)?
            .filter(|batch| batch.num_rows() == 1)
            .ok_or_else(|| corrupt("Search page has an invalid row count"))?;
        if batch.columns().iter().any(|column| column.is_null(0)) {
            return Err(corrupt("Search page omits a table"));
        }
        Ok(batch)
    }
}

impl<W: Write> Writer<W> {
    pub(super) fn new(output: W, format: i32) -> Result<Self> {
        let mut writer = FileWriter::try_new(DigestWriter {
            writer: output, digest: Sha256::new(), position: 0, prefix: [0; 8], captured: 0, root: None,
        }, schema(format)).map_err(fault)?;
        writer.get_mut().digest.reset();
        Ok(Self { writer, checksums: String::new(), format })
    }

    pub(super) fn page(&mut self, populated: impl IntoIterator<Item = (Table, StructArray)>) -> Result<PageRef> {
        let batch = record(self.format, populated)?;
        self.write_batch(&batch)
    }

    pub(super) fn root_page(&mut self, populated: impl IntoIterator<Item = (Table, StructArray)>)
        -> Result<(PageRef, Option<Vec<u8>>)> {
        // Packed locators may inline the exact bytes of the written root page.
        self.writer.get_mut().root = (self.format >= super::PACKED_FORMAT_VERSION).then(Vec::new);
        let page = self.page(populated);
        let root = self.writer.get_mut().root.take();
        Ok((page?, root))
    }

    pub(super) fn write_batch(&mut self, batch: &RecordBatch) -> Result<PageRef> {
        let output = self.writer.get_mut();
        let start = output.position;
        output.captured = 0;
        self.writer.write(batch).map_err(fault)?;
        let output = self.writer.get_mut();
        if output.captured != 8 || output.prefix[..4] != [0xff; 4] {
            return Err(fault("Search Arrow batch has no continuation marker"));
        }
        let metadata = u32::from_le_bytes(output.prefix[4..].try_into().expect("fixed-width Arrow prefix"))
            .checked_add(8).ok_or_else(|| fault("Search batch metadata is too large"))?;
        let length = output.position - start;
        let body = length.checked_sub(metadata as usize).ok_or_else(|| fault("Search batch length is invalid"))?;
        let digest = output.digest.finalize_reset();
        self.checksums.push_str(&format!("{digest:x}"));
        Ok(PageRef { offset: start as u64, metadata, body: body as u64, digest: digest.into() })
    }

    pub(super) fn finish(mut self) -> Result<u64> {
        self.writer.write_metadata(CHECKSUMS, self.checksums);
        self.writer.finish().map_err(fault)?;
        let output = self.writer.get_mut();
        output.flush().map_err(fault)?;
        Ok(output.position as u64)
    }
}

/// Immutable bytes that keep their backing allocation alive for decoded Arrow buffers.
pub enum Source {
    Bytes(bytes::Bytes),
    Lease(rtdb_storage::cache::CacheLease),
    Mapped(memmap2::Mmap, rtdb_storage::LocalFile),
}

impl From<bytes::Bytes> for Source { fn from(bytes: bytes::Bytes) -> Self { Self::Bytes(bytes) } }
impl From<Vec<u8>> for Source { fn from(bytes: Vec<u8>) -> Self { Self::Bytes(bytes.into()) } }
impl From<rtdb_storage::cache::CacheLease> for Source { fn from(lease: rtdb_storage::cache::CacheLease) -> Self { Self::Lease(lease) } }

impl Source {
    /// Keeps the mapped file and any cache lease alive until the last Arrow buffer closes.
    pub fn mapped(local: rtdb_storage::LocalFile) -> Result<Self> {
        // SAFETY: published objects are immutable and the local lease keeps this file alive.
        let map = unsafe { memmap2::Mmap::map(local.file()) }.map_err(fault)?;
        Ok(Self::Mapped(map, local))
    }
}

impl AsRef<[u8]> for Source {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Bytes(bytes) => bytes,
            Self::Lease(lease) => lease.as_slice(),
            Self::Mapped(map, _) => map,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FooterLocator {
    pub length: usize,
    pub start: usize,
    checksum: String,
}

impl FooterLocator {
    pub fn read(input: &mut std::fs::File) -> Result<Self> {
        let length = usize::try_from(input.metadata().map_err(fault)?.len()).map_err(fault)?;
        let tail = length.checked_sub(10).ok_or_else(|| corrupt("Search file has no Arrow footer"))?;
        input.seek(SeekFrom::Start(tail as u64)).map_err(fault)?;
        let mut trailer = [0; 10];
        input.read_exact(&mut trailer).map_err(fault)?;
        let footer_size = read_footer_length(trailer).map_err(fault)?;
        let start = tail.checked_sub(footer_size).filter(|&start| start >= 8)
            .ok_or_else(|| corrupt("Search file footer exceeds its bounds"))?;
        input.seek(SeekFrom::Start(start as u64)).map_err(fault)?;
        let mut digest = Sha256::new();
        let mut remaining = length - start;
        let mut chunk = [0; 4096];
        while remaining > 0 {
            let count = remaining.min(chunk.len());
            input.read_exact(&mut chunk[..count]).map_err(fault)?;
            digest.update(&chunk[..count]);
            remaining -= count;
        }
        Ok(Self { length, start, checksum: format!("{:x}", digest.finalize()) })
    }

    pub fn encode(&self) -> String { format!("{}:{}:{}", self.length, self.start, self.checksum) }

    pub fn decode(value: &str) -> Option<Self> {
        let (length, tail) = value.split_once(':')?;
        let (start, checksum) = tail.split_once(':')?;
        let length: usize = length.parse().ok()?;
        let start: usize = start.parse().ok()?;
        (start >= 8 && length.checked_sub(start)? > 10 && checksum.len() == 64
            && checksum.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .then(|| Self { length, start, checksum: checksum.to_owned() })
    }

    pub fn verify(&self, footer: &[u8]) -> Result<()> {
        if footer.len() != self.length - self.start || format!("{:x}", Sha256::digest(footer)) != self.checksum {
            return Err(corrupt("Search file footer fails its catalog checksum"));
        }
        Ok(())
    }
}

enum ReadPages {
    Local(Box<dyn Fn(Range<usize>) -> Result<Buffer> + Send + Sync>),
    Remote { storage: Weak<BufferPool>, key: ObjectKey },
    Checked(BTreeMap<usize, RecordBatch>),
    Legacy { catalog: Box<super::legacy11::Catalog>, scratch: Arc<rtdb_types::MemoryBudget> },
}

pub(super) struct Reader {
    read: ReadPages,
    decoder: FileDecoder,
    blocks: Vec<Block>,
    hashes: String,
    pages: Vec<OnceLock<RecordBatch>>,
    pub(super) format: i32,
    length: usize,
}

impl Reader {
    pub(super) fn from_checked_batches(batches: Vec<RecordBatch>, format: i32) -> Self {
        Self { read: ReadPages::Local(Box::new(|_| Err(corrupt("Search page was not materialized")))),
            decoder: FileDecoder::new(Arc::clone(schema(format)), arrow_ipc::MetadataVersion::V5),
            blocks: Vec::new(), hashes: String::new(), pages: batches.into_iter().map(OnceLock::from).collect(), format, length: 0 }
    }

    pub(super) fn from_sparse_checked_batches(batches: BTreeMap<usize, RecordBatch>, format: i32) -> Self {
        Self { read: ReadPages::Checked(batches),
            decoder: FileDecoder::new(Arc::clone(schema(format)), arrow_ipc::MetadataVersion::V5),
            blocks: Vec::new(), hashes: String::new(), pages: Vec::new(), format, length: 0 }
    }

    pub(super) fn from_legacy(catalog: super::legacy11::Catalog, scratch: Arc<rtdb_types::MemoryBudget>) -> Self {
        Self { read: ReadPages::Legacy { catalog: Box::new(catalog), scratch },
            decoder: FileDecoder::new(Arc::clone(schema(LEGACY_FORMAT_VERSION)), MetadataVersion::V5),
            blocks: Vec::new(), hashes: String::new(), pages: Vec::new(), format: LEGACY_FORMAT_VERSION, length: 0 }
    }

    pub(super) fn from_source(source: Source) -> Result<Self> {
        let buffer = Buffer::from(bytes::Bytes::from_owner(source));
        Self::open(buffer.len(), move |range| Ok(buffer.slice_with_length(range.start, range.len())))
    }

    pub(super) fn open(length: usize, read: impl Fn(Range<usize>) -> Result<Buffer> + Send + Sync + 'static) -> Result<Self> {
        let tail = length.checked_sub(10).ok_or_else(|| corrupt("Search file has no Arrow footer"))?;
        let footer_length = read_footer_length(read(tail..length)?.as_slice().try_into().map_err(fault)?).map_err(fault)?;
        let start = tail.checked_sub(footer_length).filter(|start| *start >= 8)
            .ok_or_else(|| corrupt("Search file footer exceeds its bounds"))?;
        let head = read(0..6)?;
        if head.as_slice() != b"ARROW1" { return Err(corrupt("Search file has no Arrow header")); }
        let bytes = read(start..tail)?;
        Self::with_footer(length, start, bytes, ReadPages::Local(Box::new(read)))
    }

    pub(super) async fn remote(storage: &Arc<BufferPool>, key: &ObjectKey, locator: &FooterLocator) -> Result<Self> {
        let footer = storage.get_scan_range(key, locator.start as u64..locator.length as u64).await?;
        let bytes = footer.as_slice();
        locator.verify(bytes)?;
        let tail = bytes.len().checked_sub(10).ok_or_else(|| corrupt("Search file has no Arrow trailer"))?;
        if read_footer_length(bytes[tail..].try_into().map_err(fault)?).map_err(fault)? != tail {
            return Err(corrupt("Search file trailer disagrees with its catalog locator"));
        }
        let bytes = Buffer::from(bytes::Bytes::from_owner(footer)).slice_with_length(0, tail);
        let reader = Self::with_footer(locator.length, locator.start, bytes,
            ReadPages::Remote { storage: Arc::downgrade(storage), key: key.clone() })?;
        let last_metadata = if reader.format == FORMAT_VERSION { 1 } else { 0 };
        reader.prefetch(0..=last_metadata).await?;
        Ok(reader)
    }

    fn with_footer(length: usize, start: usize, bytes: Buffer, read: ReadPages) -> Result<Self> {
        let footer = arrow_ipc::root_as_footer(bytes.as_slice()).map_err(fault)?;
        let file_schema = footer.schema().ok_or_else(|| corrupt("Search file has no schema"))?;
        let decoded_schema = fb_to_schema(file_schema);
        let format = if decoded_schema == **schema(FORMAT_VERSION) { FORMAT_VERSION }
            else if decoded_schema == **schema(LEGACY_FORMAT_VERSION) { LEGACY_FORMAT_VERSION }
            else { return Err(corrupt("Search file has an incompatible Arrow schema")); };
        if !file_schema.endianness().equals_to_target_endianness()
            || footer.dictionaries().is_some_and(|blocks| !blocks.is_empty()) {
            return Err(corrupt("Search file has an incompatible Arrow schema"));
        }
        let blocks: Vec<_> = footer.recordBatches().ok_or_else(|| corrupt("Search file has no record batches"))?.iter().copied().collect();
        let mut previous = 8;
        for block in &blocks {
            let range = Self::range(block)?;
            if range.start < previous || range.end > start {
                return Err(corrupt("Search record batch exceeds the data region"));
            }
            previous = range.end;
        }
        let hashes = footer.custom_metadata().into_iter().flatten().find(|metadata| metadata.key() == Some(CHECKSUMS))
            .and_then(|metadata| metadata.value()).filter(|hashes| hashes.len() == blocks.len().saturating_mul(64) && hashes.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .ok_or_else(|| corrupt("Search file has missing or invalid batch checksums"))?.to_owned();
        let pages = (0..blocks.len()).map(|_| OnceLock::new()).collect();
        Ok(Self { read, decoder: FileDecoder::new(Arc::clone(schema(format)), footer.version()), blocks, hashes, pages, format, length })
    }

    pub(super) fn len(&self) -> usize {
        match &self.read {
            ReadPages::Checked(pages) => pages.len(),
            ReadPages::Legacy { catalog, .. } => catalog.len(),
            _ => self.pages.len(),
        }
    }

    pub(super) fn metadata_bytes(&self) -> usize {
        self.blocks.iter().take(if self.format == FORMAT_VERSION { 2 } else { 1 })
            .filter_map(|block| Self::range(block).ok()).map(|range| range.len())
            .sum::<usize>().saturating_add(self.hashes.len())
    }

    fn range(block: &Block) -> Result<Range<usize>> {
        let start = usize::try_from(block.offset()).map_err(fault)?;
        let metadata = usize::try_from(block.metaDataLength()).map_err(fault)?;
        let body = usize::try_from(block.bodyLength()).map_err(fault)?;
        let end = start.checked_add(metadata).and_then(|value| value.checked_add(body))
            .ok_or_else(|| corrupt("Search record batch range overflows"))?;
        Ok(start..end)
    }

    pub(super) fn page(&self, page: usize) -> Result<RecordBatch> {
        match &self.read {
            ReadPages::Checked(pages) => return pages.get(&page).cloned().ok_or_else(|| corrupt("Search page was not prepared")),
            ReadPages::Legacy { catalog, scratch } => return catalog.page(page, Some(scratch)),
            _ => {}
        }
        let slot = self.pages.get(page).ok_or_else(|| corrupt("Search file has no such record batch"))?;
        if let Some(batch) = slot.get() { return Ok(batch.clone()); }
        let block = self.blocks.get(page).ok_or_else(|| corrupt("Search file has no such record batch"))?;
        let ReadPages::Local(read) = &self.read else {
            return Err(fault("Search range was not loaded before decoding"));
        };
        self.decode(page, read(Self::range(block)?)?)
    }

    fn decode(&self, page: usize, bytes: Buffer) -> Result<RecordBatch> {
        let block = &self.blocks[page];
        let length = Self::range(block)?.len();
        if bytes.len() != length || format!("{:x}", Sha256::digest(bytes.as_slice())) != self.hashes[page * 64..(page + 1) * 64] {
            return Err(corrupt("Search record batch fails its checksum"));
        }
        let batch = self.decoder.read_record_batch(block, &bytes).map_err(fault)?
            .filter(|batch| batch.num_rows() == 1).ok_or_else(|| corrupt("Search record batch has an invalid row count"))?;
        if batch.columns().iter().any(|column| column.is_null(0)) {
            return Err(corrupt("Search record batch omits a table"));
        }
        Ok(self.pages[page].get_or_init(|| batch).clone())
    }

    pub(super) async fn prefetch(&self, pages: impl IntoIterator<Item = usize>) -> Result<()> {
        let ReadPages::Remote { storage, key } = &self.read else { return Ok(()); };
        let storage = storage.upgrade().ok_or_else(|| fault("Search object storage has closed"))?;
        let pages = pages.into_iter().collect::<std::collections::BTreeSet<_>>();
        let ranges = pages.into_iter().filter(|&page| self.pages.get(page).is_none_or(|slot| slot.get().is_none()))
            .map(|page| {
                let block = self.blocks.get(page).ok_or_else(|| corrupt("Search file has no such record batch"))?;
                Ok((page, Self::range(block)?))
            }).collect::<Result<Vec<_>>>()?;
        let groups = read_groups(ranges, self.length);
        futures::stream::iter(groups).map(|(range, pages)| {
            let storage = &storage;
            async move {
                let bytes = if range.start == 0 && range.end == self.length {
                    storage.get(key).await?
                } else {
                    storage.get_scan_range(key, range.start as u64..range.end as u64).await?
                };
                let bytes = Buffer::from(bytes::Bytes::from_owner(bytes));
                for (page, span) in pages {
                    if span.end - range.start > bytes.len() { return Err(corrupt("Search file range exceeds the object")); }
                    self.decode(page, bytes.slice_with_length(span.start - range.start, span.len()))?;
                }
                Ok(())
            }
        }).buffer_unordered(8).try_for_each(|()| async { Ok(()) }).await
    }

    #[cfg(test)]
    pub(super) fn page_range(&self, page: usize) -> Range<usize> { Self::range(&self.blocks[page]).unwrap() }
}

type PageRanges = Vec<(usize, Range<usize>)>;

fn read_groups(ranges: PageRanges, length: usize) -> Vec<(Range<usize>, PageRanges)> {
    let used: usize = ranges.iter().map(|(_, range)| range.len()).sum();
    if !ranges.is_empty() && used >= length - length / 4 {
        return vec![(0..length, ranges)];
    }
    let mut groups: Vec<(Range<usize>, PageRanges)> = Vec::new();
    for (page, range) in ranges {
        if let Some((last, pages)) = groups.last_mut()
            && range.start - last.end <= 4096 && range.end - last.start <= 512 * 1024 {
            last.end = range.end;
            pages.push((page, range));
        } else {
            groups.push((range.clone(), vec![(page, range)]));
        }
    }
    groups
}
