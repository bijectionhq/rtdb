use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::convert::try_schema_from_ipc_buffer;
use arrow_ipc::reader::{FileReader, StreamReader};
use arrow_ipc::writer::{DictionaryTracker, FileWriter, IpcDataGenerator, IpcWriteOptions, StreamWriter, write_message};
use arrow_schema::{Schema, SchemaRef};
use rtdb_types::{Anomaly, Result};

pub struct ArrowFile {
    reader: FileReader<Box<dyn RetainedReader>>,
    encoded_page_sizes: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileLayout {
    pub footer_encoded_bytes: usize,
    pub dictionary_encoded_bytes: usize,
    pub page_encoded_bytes: Vec<usize>,
    pub uncompressed: bool,
}

trait RetainedReader: Read + Seek + Send {}
impl<T: Read + Seek + Send> RetainedReader for T {}

impl ArrowFile {
    pub fn open(bytes: Arc<[u8]>, projection: Option<Vec<usize>>) -> Result<Self> {
        Self::from_reader(Cursor::new(bytes), projection)
    }

    pub fn from_reader(reader: impl Read + Seek + Send + 'static, projection: Option<Vec<usize>>) -> Result<Self> {
        let mut reader: Box<dyn RetainedReader> = Box::new(reader);
        let layout = inspect_file(reader.as_mut())?;
        Self::from_inspected_reader(reader, projection, layout)
    }

    /// The layout must describe this immutable reader, as returned by `inspect_file`.
    pub fn from_inspected_reader(reader: impl Read + Seek + Send + 'static, projection: Option<Vec<usize>>, layout: FileLayout) -> Result<Self> {
        let mut reader: Box<dyn RetainedReader> = Box::new(reader);
        reader.seek(SeekFrom::Start(0)).map_err(invalid)?;
        Ok(Self { reader: FileReader::try_new(reader, projection).map_err(invalid)?, encoded_page_sizes: layout.page_encoded_bytes })
    }

    pub fn schema(&self) -> SchemaRef { self.reader.schema() }
    pub fn page_count(&self) -> usize { self.reader.num_batches() }

    /// Encoded metadata and body bytes; compressed pages can require a larger decoded allocation.
    pub fn encoded_page_size(&self, page: usize) -> Result<usize> {
        self.encoded_page_sizes.get(page).copied().ok_or_else(|| invalid("Arrow page exceeds its file"))
    }

    /// Returned batches retain their buffers independently of this reader.
    pub fn read_page(&mut self, page: usize) -> Result<RecordBatch> {
        if page >= self.page_count() {
            return Err(invalid(format!("Page {page} is outside a {}-page file", self.page_count())));
        }
        self.reader.set_index(page).map_err(invalid)?;
        self.reader.next().ok_or_else(|| invalid("Arrow file ended before its declared page"))?.map_err(invalid)
    }
}

pub fn inspect_file<R: Read + Seek + ?Sized>(reader: &mut R) -> Result<FileLayout> {
    inspect_file_reserved(reader, |_| Ok(()))
}

/// Acquires a caller-owned guard before each footer or message allocation and retains it while those bytes are inspected.
pub fn inspect_file_reserved<R: Read + Seek + ?Sized, G>(reader: &mut R, mut reserve: impl FnMut(usize) -> Result<G>) -> Result<FileLayout> {
    let end = reader.seek(SeekFrom::End(0)).map_err(invalid)?;
    if end < 10 { return Err(invalid("Arrow file is shorter than its footer")); }
    reader.seek(SeekFrom::End(-10)).map_err(invalid)?;
    let mut tail = [0u8; 10];
    reader.read_exact(&mut tail).map_err(invalid)?;
    let footer_length = arrow_ipc::reader::read_footer_length(tail).map_err(invalid)?;
    let footer_start = end.checked_sub(10 + footer_length as u64).ok_or_else(|| invalid("Arrow footer exceeds its file"))?;
    let _footer = reserve(footer_length.checked_mul(2).and_then(|size| size.checked_add(65_536)).ok_or_else(|| invalid("Arrow inspection allocation overflow"))?)?;
    reader.seek(SeekFrom::Start(footer_start)).map_err(invalid)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(footer_length).map_err(invalid)?;
    bytes.resize(footer_length, 0);
    reader.read_exact(&mut bytes).map_err(invalid)?;
    let footer = arrow_ipc::root_as_footer(&bytes).map_err(invalid)?;
    let size = |block: &arrow_ipc::Block| -> Result<usize> {
        let metadata = usize::try_from(block.metaDataLength()).map_err(invalid)?;
        let body = usize::try_from(block.bodyLength()).map_err(invalid)?;
        metadata.checked_add(body).ok_or_else(|| invalid("Arrow page length overflow"))
    };
    let mut layout = FileLayout { footer_encoded_bytes: footer_length + 10, dictionary_encoded_bytes: 0, page_encoded_bytes: Vec::new(), uncompressed: true };
    for block in footer.dictionaries().into_iter().flat_map(|blocks| blocks.iter()).chain(footer.recordBatches().into_iter().flat_map(|blocks| blocks.iter())) {
        let offset = u64::try_from(block.offset()).map_err(invalid)?;
        let block_size = size(block)?;
        if offset.checked_add(block_size as u64).is_none_or(|end| end > footer_start) { return Err(invalid("Arrow block exceeds data region")); }
        let metadata_length = usize::try_from(block.metaDataLength()).map_err(invalid)?;
        let _message = reserve(metadata_length)?;
        let mut metadata = vec![0; metadata_length];
        reader.seek(SeekFrom::Start(offset)).map_err(invalid)?;
        reader.read_exact(&mut metadata).map_err(invalid)?;
        let prefix = if metadata.starts_with(&[0xff; 4]) { 8 } else { 4 };
        let message = arrow_ipc::root_as_message(metadata.get(prefix..).ok_or_else(|| invalid("Arrow message has no body"))?).map_err(invalid)?;
        if let Some(batch) = message.header_as_record_batch() { layout.uncompressed &= batch.compression().is_none(); }
        else if let Some(dictionary) = message.header_as_dictionary_batch() {
            layout.uncompressed &= dictionary.data().ok_or_else(|| invalid("Arrow dictionary has no batch"))?.compression().is_none();
        } else { return Err(invalid("Arrow data block has no record batch")); }
    }
    if let Some(dictionaries) = footer.dictionaries() {
        for block in dictionaries { layout.dictionary_encoded_bytes = layout.dictionary_encoded_bytes.checked_add(size(block)?).ok_or_else(|| invalid("Arrow dictionary size overflow"))?; }
    }
    if let Some(blocks) = footer.recordBatches() { layout.page_encoded_bytes = blocks.iter().map(size).collect::<Result<_>>()?; }
    Ok(layout)
}

pub struct ArrowStream {
    reader: StreamReader<Cursor<Arc<[u8]>>>,
}

impl ArrowStream {
    pub fn open(bytes: Arc<[u8]>, projection: Option<Vec<usize>>) -> Result<Self> {
        Ok(Self { reader: StreamReader::try_new(Cursor::new(bytes), projection).map_err(invalid)? })
    }

    pub fn schema(&self) -> SchemaRef { self.reader.schema() }
}

impl Iterator for ArrowStream {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> { self.reader.next().map(|batch| batch.map_err(invalid)) }
}

pub fn read_stream(bytes: Arc<[u8]>) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let reader = ArrowStream::open(bytes, None)?;
    let schema = reader.schema();
    Ok((schema, reader.collect::<Result<Vec<_>>>()?))
}

pub fn write_file(schema: &Schema, pages: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut writer = FileWriter::try_new_with_options(Vec::new(), schema, write_options()?).map_err(invalid)?;
    for page in pages { writer.write(page).map_err(invalid)?; }
    writer.finish().map_err(invalid)?;
    let mut bytes = writer.into_inner().map_err(invalid)?;
    let footer_length_at = bytes.len().checked_sub(10).ok_or_else(|| invalid("Arrow writer omitted its footer"))?;
    let footer_length = i32::from_le_bytes(bytes[footer_length_at..footer_length_at + 4].try_into().map_err(invalid)?);
    let footer_length = usize::try_from(footer_length).map_err(invalid)?;
    let footer_start = footer_length_at.checked_sub(footer_length).ok_or_else(|| invalid("Arrow footer exceeds its file"))?;
    jvm_end_marker(&mut bytes, footer_start)?;
    Ok(bytes)
}

pub struct ArrowFileWriter { writer: FileWriter<LimitedFile> }

struct LimitedFile { file: File, position: u64, limit: u64 }

impl Write for LimitedFile {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.position.checked_add(bytes.len() as u64).is_none_or(|position| position > self.limit) {
            return Err(std::io::Error::other("Arrow output byte limit exceeded"));
        }
        let count = self.file.write(bytes)?;
        self.position += count as u64;
        Ok(count)
    }
    fn flush(&mut self) -> std::io::Result<()> { self.file.flush() }
}

impl ArrowFileWriter {
    /// The file must be empty and positioned at offset zero.
    pub fn new(file: File, schema: &Schema) -> Result<Self> {
        Self::new_bounded(file, schema, u64::MAX)
    }

    /// Rejects a write before the file would exceed `byte_limit`, including its footer.
    pub fn new_bounded(mut file: File, schema: &Schema, byte_limit: u64) -> Result<Self> {
        if file.metadata().map_err(invalid)?.len() != 0 || file.stream_position().map_err(invalid)? != 0 { return Err(invalid("Arrow output file must be empty and positioned at zero")); }
        Ok(Self { writer: FileWriter::try_new_with_options(LimitedFile { file, position: 0, limit: byte_limit }, schema, write_options()?).map_err(invalid)? })
    }

    pub fn bytes_written(&mut self) -> u64 { self.writer.get_mut().position }

    /// The owner may lower the remaining allowance between writes; already-written bytes must still fit.
    pub fn set_byte_limit(&mut self, byte_limit: u64) -> Result<()> {
        let sink = self.writer.get_mut();
        if sink.position > byte_limit { return Err(invalid("Arrow output already exceeds the requested byte limit")); }
        sink.limit = byte_limit;
        Ok(())
    }

    pub fn write(&mut self, page: &RecordBatch) -> Result<()> { self.writer.write(page).map_err(invalid) }

    /// Finishes the JVM-compatible end marker and returns the caller-owned file at its end.
    pub fn finish(mut self) -> Result<File> {
        self.writer.finish().map_err(invalid)?;
        let mut file = self.writer.into_inner().map_err(invalid)?.file;
        let end = file.seek(SeekFrom::End(0)).map_err(invalid)?;
        file.seek(SeekFrom::End(-10)).map_err(invalid)?;
        let mut tail = [0; 10];
        file.read_exact(&mut tail).map_err(invalid)?;
        let length = arrow_ipc::reader::read_footer_length(tail).map_err(invalid)? as u64;
        let footer_start = end.checked_sub(length + 10).ok_or_else(|| invalid("Arrow footer exceeds its file"))?;
        let marker = footer_start.checked_sub(8).ok_or_else(|| invalid("Arrow file has no end marker"))?;
        file.seek(SeekFrom::Start(marker)).map_err(invalid)?;
        let mut expected = [0; 8];
        file.read_exact(&mut expected).map_err(invalid)?;
        if expected != [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0] { return Err(invalid("Arrow writer produced an unexpected end marker")); }
        file.seek(SeekFrom::Start(marker)).map_err(invalid)?;
        file.write_all(&[0; 4]).map_err(invalid)?;
        let mut buffer = [0; 8192];
        let mut position = footer_start;
        while position < end {
            let count = usize::try_from((end - position).min(buffer.len() as u64)).map_err(invalid)?;
            file.seek(SeekFrom::Start(position)).map_err(invalid)?;
            file.read_exact(&mut buffer[..count]).map_err(invalid)?;
            file.seek(SeekFrom::Start(position - 4)).map_err(invalid)?;
            file.write_all(&buffer[..count]).map_err(invalid)?;
            position += count as u64;
        }
        file.set_len(end - 4).map_err(invalid)?;
        file.seek(SeekFrom::End(0)).map_err(invalid)?;
        file.flush().map_err(invalid)?;
        Ok(file)
    }
}

pub fn write_stream(schema: &Schema, pages: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut writer = StreamWriter::try_new_with_options(Vec::new(), schema, write_options()?).map_err(invalid)?;
    for page in pages { writer.write(page).map_err(invalid)?; }
    writer.finish().map_err(invalid)?;
    let mut bytes = writer.into_inner().map_err(invalid)?;
    let end = bytes.len();
    jvm_end_marker(&mut bytes, end)?;
    Ok(bytes)
}

/// Writes pages without buffering the complete stream; the sink owns and enforces its allocation bound.
pub fn write_stream_to<'a, W: Write>(schema: &Schema, pages: impl IntoIterator<Item = &'a RecordBatch>, sink: W) -> Result<W> {
    let mut writer = StreamWriter::try_new_with_options(TailWriter { inner: sink, pending: Vec::with_capacity(8) }, schema, write_options()?).map_err(invalid)?;
    for page in pages { writer.write(page).map_err(invalid)?; }
    writer.finish().map_err(invalid)?;
    let mut sink = writer.into_inner().map_err(invalid)?;
    if sink.pending.as_slice() != [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0] { return Err(invalid("Arrow writer produced an unexpected end marker")); }
    sink.inner.write_all(&[0; 4]).map_err(invalid)?;
    sink.inner.flush().map_err(invalid)?;
    Ok(sink.inner)
}

struct TailWriter<W> { inner: W, pending: Vec<u8> }

impl<W: Write> Write for TailWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let count = self.pending.len().saturating_add(bytes.len()).saturating_sub(8);
        let old = count.min(self.pending.len());
        self.inner.write_all(&self.pending[..old])?;
        self.pending.drain(..old);
        self.inner.write_all(&bytes[..count - old])?;
        self.pending.extend_from_slice(&bytes[count - old..]);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { self.inner.flush() }
}

pub fn read_schema_message(bytes: &[u8]) -> Result<Schema> {
    try_schema_from_ipc_buffer(bytes).map_err(invalid)
}

pub fn write_schema_message(schema: &Schema) -> Result<Vec<u8>> {
    let options = write_options()?;
    let encoded = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(schema, &mut DictionaryTracker::new(true), &options);
    let mut bytes = Vec::new();
    write_message(&mut bytes, encoded, &options).map_err(invalid)?;
    Ok(bytes)
}

fn write_options() -> Result<IpcWriteOptions> {
    IpcWriteOptions::try_new(8, false, arrow_ipc::MetadataVersion::V5).map_err(invalid)
}

fn jvm_end_marker(bytes: &mut Vec<u8>, end: usize) -> Result<()> {
    let marker = end.checked_sub(8).ok_or_else(|| invalid("Arrow writer omitted its end marker"))?;
    if bytes.get(marker..end) != Some(&[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]) {
        return Err(invalid("Arrow writer produced an unexpected end marker"));
    }
    // ArrowUnloader.Mode.STREAM ends with one zero i32, also immediately before a file footer.
    bytes.drain(marker..marker + 4);
    Ok(())
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("rtdb.format/invalid-arrow", error.to_string())
}
