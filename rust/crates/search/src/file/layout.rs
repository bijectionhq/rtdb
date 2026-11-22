use std::io::{self, Write};
use std::ops::Range;
use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::{Array, ArrayRef, LargeListArray, RecordBatch, StructArray};
use arrow_buffer::{Buffer, OffsetBuffer};
use arrow_ipc::{Block, convert::fb_to_schema, reader::{FileDecoder, read_footer_length}, writer::FileWriter};
use arrow_schema::{DataType as T, Field, FieldRef, Fields, Schema, SchemaRef};
use rtdb_types::Result;
use sha2::{Digest, Sha256};

use super::{corrupt, fault};

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
}

impl Table {
    const ALL: [Self; 10] = [
        Self::Header, Self::Columns, Self::Members, Self::Pages, Self::Events,
        Self::Runs, Self::RowRuns, Self::Sections, Self::Terms, Self::Postings,
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
            ])),
            Self::Runs => ("runs", fields([
                ("iid", T::FixedSizeBinary(16), false), ("token_length", T::UInt32, false),
                ("identity_type", T::UInt8, false), ("identity_text", T::Utf8, true),
                ("identity_integer", T::Int64, true), ("identity_uuid", T::FixedSizeBinary(16), true),
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
        };
        Field::new(name, list_type(T::Struct(fields)), false)
    }

    pub(super) fn fields(self) -> Fields {
        let T::LargeList(item) = schema().field(self as usize).data_type().clone() else { unreachable!() };
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

fn schema() -> &'static SchemaRef {
    static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(Schema::new(Table::ALL.map(Table::field).to_vec())));
    &SCHEMA
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

struct DigestWriter<W> {
    writer: W,
    digest: Sha256,
}

impl<W: Write> Write for DigestWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.writer.write(buf)?;
        self.digest.update(&buf[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> { self.writer.flush() }
}

pub(super) struct Writer<W> {
    writer: FileWriter<DigestWriter<W>>,
    checksums: String,
}

impl<W: Write> Writer<W> {
    pub(super) fn new(output: W) -> Result<Self> {
        let mut writer = FileWriter::try_new(DigestWriter { writer: output, digest: Sha256::new() }, schema()).map_err(fault)?;
        writer.get_mut().digest.reset();
        Ok(Self { writer, checksums: String::new() })
    }

    pub(super) fn page(&mut self, tables: impl IntoIterator<Item = (Table, StructArray)>) -> Result<()> {
        let mut contents: [Option<StructArray>; Table::ALL.len()] = std::array::from_fn(|_| None);
        for (table, array) in tables { contents[table as usize] = Some(array); }
        let columns = Table::ALL.into_iter().zip(contents).map(|(table, content)| {
            let content = content.unwrap_or_else(|| StructArray::new_empty_fields(0, None));
            let item = item_of(schema().field(table as usize));
            let values: ArrayRef = if content.num_columns() == 0 {
                arrow_array::new_empty_array(item.data_type())
            } else {
                Arc::new(content)
            };
            let len = values.len();
            list(values, [len], item)
        }).collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new(Arc::clone(schema()), columns).map_err(fault)?;
        self.writer.write(&batch).map_err(fault)?;
        self.checksums.push_str(&format!("{:x}", self.writer.get_mut().digest.finalize_reset()));
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<()> {
        self.writer.write_metadata(CHECKSUMS, self.checksums);
        self.writer.finish().map_err(fault)
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
    /// Maps a local immutable search file, retaining its cache lease until the last Arrow buffer closes.
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

pub(super) struct Reader {
    read: Box<dyn Fn(Range<usize>) -> Result<Buffer> + Send + Sync>,
    decoder: FileDecoder,
    blocks: Vec<Block>,
    hashes: String,
    pages: Vec<OnceLock<RecordBatch>>,
}

impl Reader {
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
        let footer = arrow_ipc::root_as_footer(bytes.as_slice()).map_err(fault)?;
        let file_schema = footer.schema().ok_or_else(|| corrupt("Search file has no schema"))?;
        if !file_schema.endianness().equals_to_target_endianness() || fb_to_schema(file_schema) != **schema()
            || footer.dictionaries().is_some_and(|blocks| !blocks.is_empty())
        {
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
        Ok(Self { read: Box::new(read), decoder: FileDecoder::new(Arc::clone(schema()), footer.version()), blocks, hashes, pages })
    }

    pub(super) fn len(&self) -> usize { self.blocks.len() }

    fn range(block: &Block) -> Result<Range<usize>> {
        let start = usize::try_from(block.offset()).map_err(fault)?;
        let metadata = usize::try_from(block.metaDataLength()).map_err(fault)?;
        let body = usize::try_from(block.bodyLength()).map_err(fault)?;
        let end = start.checked_add(metadata).and_then(|value| value.checked_add(body))
            .ok_or_else(|| corrupt("Search record batch range overflows"))?;
        Ok(start..end)
    }

    pub(super) fn page(&self, page: usize) -> Result<RecordBatch> {
        let block = self.blocks.get(page).ok_or_else(|| corrupt("Search file has no such record batch"))?;
        let slot = &self.pages[page];
        if let Some(batch) = slot.get() { return Ok(batch.clone()); }
        let range = Self::range(block)?;
        let length = range.len();
        let bytes = (self.read)(range)?;
        if bytes.len() != length || format!("{:x}", Sha256::digest(bytes.as_slice())) != self.hashes[page * 64..(page + 1) * 64] {
            return Err(corrupt("Search record batch fails its checksum"));
        }
        let batch = self.decoder.read_record_batch(block, &bytes).map_err(fault)?
            .filter(|batch| batch.num_rows() == 1).ok_or_else(|| corrupt("Search record batch has an invalid row count"))?;
        if batch.columns().iter().any(|column| column.is_null(0)) {
            return Err(corrupt("Search record batch omits a table"));
        }
        Ok(slot.get_or_init(|| batch).clone())
    }

    #[cfg(test)]
    pub(super) fn page_range(&self, page: usize) -> Range<usize> { Self::range(&self.blocks[page]).unwrap() }
}
