//! The generation file: immutable, byte-deterministic IVF clusters over one trie's current puts (vector.allium: VectorGeneration).
//!
//! Layout: each cluster's rows as an Arrow IPC stream, each aligned to 64 bytes, then a directory stream
//! naming every cluster's partition, centroid, byte range, row count and digest, then a 32-byte footer.
//! The artefact record's stats carry the footer and the directory digest, so a reader opens the directory
//! with one range read and checks every byte it uses.

use std::io::Write;
use std::ops::Range;
use std::sync::Arc;

use arrow_array::{
    Array, FixedSizeBinaryArray, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, UInt32Array, UInt64Array,
};
use arrow_buffer::{Buffer, ToByteSlice};
use arrow_ipc::{Message, Precision, reader::read_record_batch, root_as_message};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::ipc::{UncompressedBuffer, write_uncompressed_stream_from_buffers};
use rtdb_storage::cache::CacheLease;
use rtdb_types::{Anomaly, Iid, Result};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{definition::FILE_VERSION, kmeans, rows::{IndexedRow, IndexedRows, Partition}};

const MAGIC: &[u8; 8] = b"RTDBVEC1";
const FOOTER_BYTES: usize = 32;
const ALIGNMENT: u64 = 64;
const CONTINUATION: [u8; 4] = [255; 4];
const END: [u8; 8] = [255, 255, 255, 255, 0, 0, 0, 0];
const DECODE_SCRATCH_BYTES: usize = 8192;
const BUFFER_OWNER_BYTES: usize = 128;

fn unit_field(dimensions: usize) -> Result<DataType> {
    let width = i32::try_from(dimensions).map_err(|_| corrupt("Vector dimensions exceed the Arrow list width"))?;
    Ok(DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), width))
}

fn block_schema(dimensions: usize) -> Result<SchemaRef> {
    Ok(Arc::new(Schema::new(vec![
        Field::new("_iid", DataType::FixedSizeBinary(16), false),
        Field::new("valid_from", DataType::Int64, false),
        Field::new("valid_to", DataType::Int64, false),
        Field::new("system_from", DataType::Int64, false),
        Field::new("unit", unit_field(dimensions)?, false),
    ])))
}

fn directory_schema(dimensions: usize) -> Result<SchemaRef> {
    Ok(Arc::new(Schema::new(vec![
        Field::new("partition", DataType::FixedSizeBinary(32), false),
        Field::new("centroid", unit_field(dimensions)?, false),
        Field::new("offset", DataType::UInt64, false),
        Field::new("length", DataType::UInt64, false),
        Field::new("rows", DataType::UInt32, false),
        Field::new("digest", DataType::FixedSizeBinary(32), false),
    ])))
}

/// Where a generation's directory is and what it must hash to, as its artefact record states.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Locator { pub(crate) directory: (u64, u64), pub(crate) dimensions: u32, pub(crate) digest: [u8; 32] }

impl Locator {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut stats = footer(self.directory.0, self.directory.1, self.dimensions).to_vec();
        stats.extend_from_slice(&self.digest);
        stats
    }

    pub(crate) fn decode(stats: &[u8]) -> Option<Self> {
        let (footer, digest) = (stats.get(..FOOTER_BYTES)?, stats.get(FOOTER_BYTES..)?);
        if stats.len() != FOOTER_BYTES + 32 || &footer[24..32] != MAGIC
            || u32::from_le_bytes(footer[20..24].try_into().ok()?) != FILE_VERSION { return None; }
        Some(Self {
            directory: (u64::from_le_bytes(footer[0..8].try_into().ok()?), u64::from_le_bytes(footer[8..16].try_into().ok()?)),
            dimensions: u32::from_le_bytes(footer[16..20].try_into().ok()?), digest: digest.try_into().ok()?,
        })
    }

    pub(crate) fn directory_range(&self) -> Range<u64> { self.directory.0..self.directory.0.saturating_add(self.directory.1) }
}

pub(crate) fn verified_directory(
    bytes: &CacheLease, locator: &Locator, memory: &Arc<MemoryBudget>, cancellation: &CancellationToken,
) -> Result<Directory> {
    let index = |offset| usize::try_from(offset).map_err(|_| corrupt("Vector generation range exceeds addressable memory"));
    let start = index(locator.directory.0)?;
    let length = index(locator.directory.1)?;
    let end = start.checked_add(length).ok_or_else(|| corrupt("Vector directory range overflow"))?;
    if bytes.as_slice().get(end..) != Some(footer(locator.directory.0, locator.directory.1, locator.dimensions).as_slice()) {
        return Err(corrupt("The persisted vector footer does not match its catalogue locator"));
    }
    let directory = Directory::read(bytes.slice(start..end)?, locator, memory)?;
    let mut previous_end = 0;
    for row in 0..directory.len() {
        if cancellation.is_cancelled() { return Err(kmeans::cancelled()); }
        let cluster = directory.cluster(row);
        if cluster.range.start < previous_end { return Err(corrupt("Vector cluster ranges overlap or are out of order")); }
        previous_end = cluster.range.end;
        let from = index(cluster.range.start)?;
        let to = index(cluster.range.end)?;
        Block::read(bytes.slice(from..to)?, &cluster, directory.dimensions, memory)?;
    }
    Ok(directory)
}

fn footer(offset: u64, length: u64, dimensions: u32) -> [u8; FOOTER_BYTES] {
    let mut footer = [0u8; FOOTER_BYTES];
    footer[0..8].copy_from_slice(&offset.to_le_bytes());
    footer[8..16].copy_from_slice(&length.to_le_bytes());
    footer[16..20].copy_from_slice(&dimensions.to_le_bytes());
    footer[20..24].copy_from_slice(&FILE_VERSION.to_le_bytes());
    footer[24..32].copy_from_slice(MAGIC);
    footer
}

/// What a written generation reports for its artefact record.
pub(crate) struct Written { pub(crate) size: u64, pub(crate) locator: Locator }

/// Writes the generation of `rows`, which this sorts so that the bytes depend only on the set of rows.
/// Each partition trains its own clusters; a row belongs to exactly one cluster of its partition.
pub(crate) fn write_generation(mut rows: IndexedRows, output: &mut impl Write, cancellation: &CancellationToken) -> Result<Written> {
    let dimensions = rows.dimensions();
    let dimensions_u32 = u32::try_from(dimensions).map_err(|_| corrupt("Vector dimensions exceed the file format"))?;
    rows.sort();
    let ordered_rows = rows.rows();
    let block = block_schema(dimensions)?;
    let mut written = 0u64;
    let mut partitions = Vec::new();
    let mut centroids = Vec::new();
    let mut offsets = Vec::new();
    let mut lengths = Vec::new();
    let mut counts = Vec::new();
    let mut digests = Vec::new();
    let mut start = 0;
    while start < ordered_rows.len() {
        let partition = ordered_rows[start].partition;
        let end = start + ordered_rows[start..].iter().take_while(|row| row.partition == partition).count();
        let members = &ordered_rows[start..end];
        let (trained, assignment) = kmeans::train(members.len(), dimensions, |index| rows.unit(&members[index]), cancellation)?;
        let centroids_by_cluster = trained.chunks_exact(dimensions);
        let mut selections = vec![Vec::new(); centroids_by_cluster.len()];
        for (row, cluster) in members.iter().zip(assignment) { selections[cluster as usize].push(row); }
        for (centroid, selected) in centroids_by_cluster.zip(selections) {
            if selected.is_empty() { continue; }
            if cancellation.is_cancelled() { return Err(kmeans::cancelled()); }
            let count = u32::try_from(selected.len()).map_err(|_| corrupt("A cluster exceeds the row count the format records"))?;
            let padding = ((ALIGNMENT - written % ALIGNMENT) % ALIGNMENT) as usize;
            output.write_all(&[0u8; ALIGNMENT as usize][..padding]).map_err(io)?;
            written += padding as u64;
            let (length, digest) = write_block(output, &block, &rows, &selected)?;
            partitions.extend_from_slice(&partition.0);
            centroids.extend_from_slice(centroid);
            offsets.push(written);
            lengths.push(length);
            counts.push(count);
            digests.extend_from_slice(&digest);
            written += length;
        }
        start = end;
    }
    let directory = directory_schema(dimensions)?;
    let count = counts.len();
    let buffers: [&[u8]; 13] = [
        &[], &partitions,
        &[], &[], centroids.to_byte_slice(),
        &[], offsets.to_byte_slice(),
        &[], lengths.to_byte_slice(),
        &[], counts.to_byte_slice(),
        &[], &digests,
    ];
    let directory_offset = written;
    let (directory_len, digest) = write_counted_stream(
        output, &directory, count, &[count, count, centroids.len(), count, count, count, count],
        &mut buffers.map(UncompressedBuffer::Slice),
    )?;
    written += directory_len;
    let locator = Locator { directory: (directory_offset, directory_len), dimensions: dimensions_u32, digest };
    output.write_all(&footer(directory_offset, directory_len, dimensions_u32)).map_err(io)?;
    written += FOOTER_BYTES as u64;
    Ok(Written { size: written, locator })
}

fn write_block(
    output: &mut impl Write, schema: &Schema, rows: &IndexedRows, selected: &[&IndexedRow],
) -> Result<(u64, [u8; 32])> {
    use UncompressedBuffer::{Slice, Spans};

    let count = selected.len();
    let mut iids = Vec::with_capacity(count * 16);
    let mut valid_from = Vec::with_capacity(count);
    let mut valid_to = Vec::with_capacity(count);
    let mut system_from = Vec::with_capacity(count);
    for row in selected {
        iids.extend_from_slice(&row.iid.0);
        valid_from.push(row.valid_from);
        valid_to.push(row.valid_to);
        system_from.push(row.system_from);
    }
    let unit_count = count * rows.dimensions();
    let mut unit_spans = rows.unit_spans(selected).map(|span| span.to_byte_slice());
    let mut buffers = [
        Slice(&[]), Slice(&iids),
        Slice(&[]), Slice(valid_from.to_byte_slice()),
        Slice(&[]), Slice(valid_to.to_byte_slice()),
        Slice(&[]), Slice(system_from.to_byte_slice()),
        Slice(&[]), Slice(&[]), Spans { length: unit_count * size_of::<f32>(), spans: &mut unit_spans },
    ];
    write_counted_stream(output, schema, count, &[count, count, count, count, count, unit_count], &mut buffers)
}

fn write_counted_stream(
    output: &mut impl Write, schema: &Schema, rows: usize, node_lengths: &[usize], buffers: &mut [UncompressedBuffer<'_, '_>],
) -> Result<(u64, [u8; 32])> {
    let mut output = HashingWriter { output, length: 0, digest: Sha256::new() };
    write_uncompressed_stream_from_buffers(&mut output, schema, rows, node_lengths, buffers).map_err(|error| match error {
        ArrowError::IoError(_, error) => io(error),
        error => arrow(error),
    })?;
    Ok((output.length, output.digest.finalize().into()))
}

struct HashingWriter<'a, W> {
    output: &'a mut W,
    length: u64,
    digest: Sha256,
}

impl<W: Write> Write for HashingWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let count = self.output.write(bytes)?;
        self.length += count as u64;
        self.digest.update(&bytes[..count]);
        Ok(count)
    }

    fn flush(&mut self) -> std::io::Result<()> { self.output.flush() }
}

fn message(bytes: &[u8]) -> Result<(Message<'_>, Range<usize>)> {
    let prefix = if bytes.starts_with(&CONTINUATION) { 4 } else { 0 };
    let invalid = || corrupt("Invalid Arrow IPC message in a vector generation");
    let length = bytes.get(prefix..prefix + 4).ok_or_else(invalid)?;
    let length = usize::try_from(i32::from_le_bytes(length.try_into().map_err(|_| invalid())?)).map_err(|_| invalid())?;
    let start = prefix + 4;
    let end = start.checked_add(length).ok_or_else(invalid)?;
    let header = root_as_message(bytes.get(start..end).ok_or_else(invalid)?).map_err(|_| invalid())?;
    let body_length = usize::try_from(header.bodyLength()).map_err(|_| invalid())?;
    let body_end = end.checked_add(body_length).filter(|end| *end <= bytes.len()).ok_or_else(invalid)?;
    Ok((header, end..body_end))
}

// Check the wire fields without materializing the stored schema's metadata.
fn matches_field(stored: arrow_ipc::Field<'_>, field: &Field) -> bool {
    if stored.name().unwrap_or_default() != field.name() || stored.nullable() != field.is_nullable()
        || stored.dictionary().is_some()
        || stored.custom_metadata().is_some_and(|entries| entries.iter().any(|entry| entry.key().is_some() && entry.value().is_some())) {
        return false;
    }
    match field.data_type() {
        DataType::FixedSizeBinary(width) => stored.type_as_fixed_size_binary().is_some_and(|ty| ty.byteWidth() == *width),
        DataType::FixedSizeList(child, width) => {
            stored.type_as_fixed_size_list().is_some_and(|ty| ty.listSize() == *width)
                && stored.children().is_some_and(|children| children.len() == 1 && matches_field(children.get(0), child))
        }
        DataType::Float32 => stored.type_as_floating_point().is_some_and(|ty| ty.precision() == Precision::SINGLE),
        DataType::Int64 => stored.type_as_int().is_some_and(|ty| ty.is_signed() && ty.bitWidth() == 64),
        DataType::UInt64 => stored.type_as_int().is_some_and(|ty| !ty.is_signed() && ty.bitWidth() == 64),
        DataType::UInt32 => stored.type_as_int().is_some_and(|ty| !ty.is_signed() && ty.bitWidth() == 32),
        _ => false,
    }
}

struct DecodedBatch {
    batch: RecordBatch,
    _scratch: Arc<MemoryReservation>,
    backing: Arc<MemoryReservation>,
}

fn decode(
    bytes: impl AsRef<[u8]> + Send + 'static, schema: impl FnOnce() -> Result<SchemaRef>,
    expected_rows: Option<usize>, memory: &Arc<MemoryBudget>,
) -> Result<DecodedBatch> {
    let scratch = memory.reserve(DECODE_SCRATCH_BYTES)?;
    let schema = schema()?;
    let mut backing = memory.reserve(size_of_val(&bytes) + 2 * BUFFER_OWNER_BYTES)?;
    let bytes = Buffer::from(Bytes::from_owner(bytes));
    let (header, body) = message(&bytes)?;
    let stored = header.header_as_schema().filter(|_| body.is_empty()).ok_or_else(|| corrupt("A vector stream must begin with its schema"))?;
    if !stored.endianness().equals_to_target_endianness()
        || stored.fields().is_none_or(|fields| fields.len() != schema.fields().len()
            || !fields.iter().zip(schema.fields().iter()).all(|(stored, field)| matches_field(stored, field))) {
        return Err(corrupt("A vector generation stream has the wrong layout"));
    }
    let first_end = body.end;
    let (header, body) = message(&bytes[first_end..])?;
    let batch = header.header_as_record_batch().ok_or_else(|| corrupt("A vector generation stream holds no batch"))?;
    let tail = &bytes[first_end + body.end..];
    if !tail.is_empty() && tail != END && tail != [0; 4] {
        return Err(corrupt("A vector generation stream does not hold exactly one batch of its layout"));
    }
    let rows = usize::try_from(batch.length()).map_err(|_| corrupt("Invalid vector generation row count"))?;
    if rows > body.len() || expected_rows.is_some_and(|expected| rows != expected) {
        return Err(corrupt("A vector generation stream does not hold its recorded rows"));
    }
    if batch.compression().is_some() || batch.variadicBufferCounts().is_some_and(|counts| !counts.is_empty())
        || batch.nodes().is_none_or(|nodes| nodes.iter().any(|node| {
            node.null_count() != 0 || usize::try_from(node.length()).map_or(true, |length| length > body.len())
        }))
        || batch.buffers().is_none_or(|buffers| buffers.iter().any(|buffer| {
            buffer.offset() < 0 || buffer.length() < 0 || buffer.offset().checked_add(buffer.length())
                .and_then(|end| usize::try_from(end).ok()).is_none_or(|end| end > body.len())
        })) {
        return Err(corrupt("Invalid Arrow IPC buffers in a vector generation"));
    }
    let data = bytes.slice_with_length(first_end + body.start, body.len());
    let copy_bytes = alignment_copy_bytes(data.as_slice(), batch, &schema)?;
    if copy_bytes > 0 {
        let retained = backing.bytes().checked_add(copy_bytes)
            .ok_or_else(|| corrupt("Vector decode allocation exceeds addressable memory"))?;
        Arc::get_mut(&mut backing).expect("vector decode owns its reservation").grow_to(retained)?;
    }
    let batch = read_record_batch(&data, batch, schema, &Default::default(), None, &header.version())
        .map_err(|error| corrupt(&error.to_string()))?;
    Ok(DecodedBatch { batch, _scratch: scratch, backing })
}

fn alignment_copy_bytes(body: &[u8], batch: arrow_ipc::RecordBatch<'_>, schema: &Schema) -> Result<usize> {
    let invalid = || corrupt("Invalid Arrow IPC buffers in a vector generation");
    let overflow = || corrupt("Vector decode allocation exceeds addressable memory");
    let mut buffers = batch.buffers().ok_or_else(invalid)?.iter();
    schema.fields().iter().try_fold(0usize, |bytes, field| {
        let ty = if let DataType::FixedSizeList(child, _) = field.data_type() {
            buffers.next().ok_or_else(invalid)?;
            child.data_type()
        } else {
            field.data_type()
        };
        let values = buffers.nth(1).ok_or_else(invalid)?;
        let alignment = match ty {
            DataType::FixedSizeBinary(_) => 1,
            DataType::Float32 => align_of::<f32>(),
            DataType::Int64 => align_of::<i64>(),
            DataType::UInt64 => align_of::<u64>(),
            DataType::UInt32 => align_of::<u32>(),
            _ => return Err(invalid()),
        };
        let offset = values.offset() as usize;
        if body[offset..].as_ptr().align_offset(alignment) == 0 { return Ok(bytes); }
        // Arrow's alignment copy rounds each buffer's capacity to 64 bytes.
        let capacity = (values.length() as usize).checked_next_multiple_of(64).ok_or_else(overflow)?;
        bytes.checked_add(capacity).and_then(|bytes| bytes.checked_add(BUFFER_OWNER_BYTES)).ok_or_else(overflow)
    })
}

/// One cluster as the directory names it.
pub(crate) struct Cluster {
    pub(crate) range: Range<u64>,
    pub(crate) rows: u32,
    pub(crate) digest: [u8; 32],
}

/// A generation's directory: its clusters and their unit centroids.
pub(crate) struct Directory {
    pub(crate) dimensions: usize,
    partitions: FixedSizeBinaryArray,
    centroids: Float32Array,
    offsets: UInt64Array,
    lengths: UInt64Array,
    counts: UInt32Array,
    digests: FixedSizeBinaryArray,
    _backing: Arc<MemoryReservation>,
}

impl Directory {
    /// Decodes directory bytes, checking them against the locator's digest.
    pub(crate) fn read(bytes: impl AsRef<[u8]> + Send + 'static, locator: &Locator, memory: &Arc<MemoryBudget>) -> Result<Self> {
        if bytes.as_ref().len() as u64 != locator.directory.1 || <[u8; 32]>::from(Sha256::digest(bytes.as_ref())) != locator.digest {
            return Err(corrupt("A vector generation directory does not match its recorded length or digest"));
        }
        let dimensions = locator.dimensions as usize;
        let decoded = decode(bytes, || directory_schema(dimensions), None, memory)?;
        let batch = &decoded.batch;
        let column = |index: usize| batch.column(index).as_ref();
        let partitions = column(0).as_any().downcast_ref::<FixedSizeBinaryArray>().ok_or_else(|| corrupt("partition"))?;
        let centroids = column(1).as_any().downcast_ref::<FixedSizeListArray>().ok_or_else(|| corrupt("centroid"))?;
        let offsets = column(2).as_any().downcast_ref::<UInt64Array>().ok_or_else(|| corrupt("offset"))?;
        let lengths = column(3).as_any().downcast_ref::<UInt64Array>().ok_or_else(|| corrupt("length"))?;
        let counts = column(4).as_any().downcast_ref::<UInt32Array>().ok_or_else(|| corrupt("rows"))?;
        let digests = column(5).as_any().downcast_ref::<FixedSizeBinaryArray>().ok_or_else(|| corrupt("digest"))?;
        let values = centroids.values().as_any().downcast_ref::<Float32Array>().ok_or_else(|| corrupt("centroid"))?.clone();
        if batch.num_rows().checked_mul(dimensions) != Some(values.len()) { return Err(corrupt("centroid width")); }
        if partitions.value_length() != 32 || digests.value_length() != 32 {
            return Err(corrupt("A cluster has an invalid partition or digest width"));
        }
        for (&offset, &length) in offsets.values().iter().zip(lengths.values().iter()) {
            offset.checked_add(length)
                .filter(|end| offset.is_multiple_of(ALIGNMENT) && offset < *end && *end <= locator.directory.0)
                .ok_or_else(|| corrupt("A cluster has an invalid range"))?;
        }
        Ok(Self {
            dimensions, partitions: partitions.clone(), centroids: values,
            offsets: offsets.clone(), lengths: lengths.clone(), counts: counts.clone(),
            digests: digests.clone(), _backing: decoded.backing,
        })
    }

    pub(crate) fn len(&self) -> usize { self.offsets.len() }

    pub(crate) fn partition(&self, row: usize) -> Partition {
        Partition(self.partitions.value(row).try_into().expect("validated partition width"))
    }

    pub(crate) fn centroid(&self, row: usize) -> &[f32] {
        &self.centroids.values()[row * self.dimensions..(row + 1) * self.dimensions]
    }

    pub(crate) fn cluster(&self, row: usize) -> Cluster {
        let start = self.offsets.value(row);
        Cluster {
            range: start..start + self.lengths.value(row),
            rows: self.counts.value(row),
            digest: self.digests.value(row).try_into().expect("validated digest width"),
        }
    }
}

/// One cluster's rows.
pub(crate) struct Block {
    iids: FixedSizeBinaryArray,
    valid_from: Int64Array,
    valid_to: Int64Array,
    system_from: Int64Array,
    units: Float32Array,
    dimensions: usize,
    _backing: Arc<MemoryReservation>,
}

impl Block {
    pub(crate) fn read(bytes: impl AsRef<[u8]> + Send + 'static, cluster: &Cluster, dimensions: usize, memory: &Arc<MemoryBudget>) -> Result<Self> {
        if cluster.range.end.checked_sub(cluster.range.start) != Some(bytes.as_ref().len() as u64)
            || <[u8; 32]>::from(Sha256::digest(bytes.as_ref())) != cluster.digest {
            return Err(corrupt("A vector cluster does not match its recorded length or digest"));
        }
        let decoded = decode(bytes, || block_schema(dimensions), Some(cluster.rows as usize), memory)?;
        let batch = &decoded.batch;
        let column = |index: usize| batch.column(index).as_ref();
        let integers = |index: usize| column(index).as_any().downcast_ref::<Int64Array>().cloned().ok_or_else(|| corrupt("temporal column"));
        let units = column(4).as_any().downcast_ref::<FixedSizeListArray>().ok_or_else(|| corrupt("unit"))?
            .values().as_any().downcast_ref::<Float32Array>().ok_or_else(|| corrupt("unit"))?.clone();
        if batch.num_rows().checked_mul(dimensions) != Some(units.len()) { return Err(corrupt("unit width")); }
        Ok(Self {
            iids: column(0).as_any().downcast_ref::<FixedSizeBinaryArray>().cloned().ok_or_else(|| corrupt("_iid"))?,
            valid_from: integers(1)?, valid_to: integers(2)?, system_from: integers(3)?, units, dimensions,
            _backing: decoded.backing,
        })
    }

    pub(crate) fn len(&self) -> usize { self.iids.len() }
    pub(crate) fn iid(&self, row: usize) -> Result<Iid> { Iid::from_slice(self.iids.value(row)) }
    /// The row's valid interval and system-from, for ruling out versions the read cannot see.
    pub(crate) fn extent(&self, row: usize) -> (i64, i64, i64) { (self.valid_from.value(row), self.valid_to.value(row), self.system_from.value(row)) }
    pub(crate) fn unit(&self, row: usize) -> &[f32] { &self.units.values()[row * self.dimensions..(row + 1) * self.dimensions] }
}

fn io(error: std::io::Error) -> Anomaly { Anomaly::fault("rtdb.vector/io", error.to_string()) }
fn arrow(error: arrow_schema::ArrowError) -> Anomaly { Anomaly::fault("rtdb.vector/arrow", error.to_string()) }
pub(crate) fn corrupt(detail: &str) -> Anomaly { Anomaly::fault("rtdb.vector/corrupt-generation", format!("Corrupt vector generation: {detail}")) }

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{StreamExt, channel::oneshot};
    use rtdb_storage::{MemoryStore, ObjectKey, ObjectStore, cache::{BufferPool, CacheLease}};
    use rtdb_types::{EventHeader, EventOp, Interval, Timestamp};

    struct StoredBytes(Buffer);

    impl AsRef<[u8]> for StoredBytes {
        fn as_ref(&self) -> &[u8] { self.0.as_slice() }
    }

    fn encode(schema: &SchemaRef, batch: RecordBatch) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, schema).map_err(arrow)?;
        writer.write(&batch).map_err(arrow)?;
        writer.finish().map_err(arrow)?;
        drop(writer);
        Ok(bytes)
    }

    fn rows(seeds: impl IntoIterator<Item = u64>) -> IndexedRows {
        let mut rows = IndexedRows::new(2);
        for seed in seeds {
            let mut iid = [0u8; 16];
            iid[..8].copy_from_slice(&seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes());
            let angle = seed as f64 * 0.37;
            let header = EventHeader { position: None, iid: Iid(iid), system_from: Timestamp(seed as i64), commit_position: 0,
                valid_time: Interval::from_micros(0, i64::MAX).unwrap(), op: EventOp::Put };
            let partition = Partition::of([Some(std::borrow::Cow::Owned(rtdb_types::Value::I64((seed % 2) as i64)))]).unwrap();
            rows.push_unit(header, partition, &[angle.cos() as f32, angle.sin() as f32]);
        }
        rows
    }

    #[tokio::test]
    async fn native_readiness_requires_the_persisted_footer_and_every_cluster() {
        let cancellation = CancellationToken::new();
        let memory = MemoryBudget::new(1 << 20);
        let store = Arc::new(MemoryStore::default());
        let pool = BufferPool::new(Arc::clone(&store) as Arc<dyn ObjectStore>, 1 << 20);
        let mut generation = Vec::new();
        let written = write_generation(rows(0..16), &mut generation, &cancellation).unwrap();
        let length = generation.len();
        let key = ObjectKey::new("ready-vector").unwrap();
        store.put(&key, Bytes::from(generation.clone())).await.unwrap();
        let bytes = pool.get_published_exact_or(&key, length, None, &memory).await.unwrap().unwrap();
        verified_directory(&bytes, &written.locator, &memory, &cancellation).unwrap();
        let claimed = rtdb_format::log::proto::PublishedObjectVersion { version: Some("another-version".into()), ..Default::default() };
        assert!(pool.get_published_exact_or(&key, length, Some(&claimed), &memory).await.unwrap().is_none());
        for (name, offset) in [("bad-cluster", 0), ("bad-footer", length - 1)] {
            let key = ObjectKey::new(name).unwrap();
            let mut altered = generation.clone();
            altered[offset] ^= 1;
            store.put(&key, Bytes::from(altered)).await.unwrap();
            let bytes = pool.get_fresh_exact_or(&key, length, &memory).await.unwrap();
            assert_eq!(verified_directory(&bytes, &written.locator, &memory, &cancellation).err().unwrap().code,
                "rtdb.vector/corrupt-generation");
        }
        let bytes = pool.get_exact_cached_or(&key, length, &memory).await.unwrap();
        store.delete(&key).await.unwrap();
        assert!(pool.get_fresh_exact_or(&key, length, &memory).await.is_err());
        assert_eq!(bytes.as_slice().len(), length);
    }

    #[test]
    fn a_generation_is_a_function_of_its_rows() {
        let token = CancellationToken::new();
        let (mut first, mut second) = (Vec::new(), Vec::new());
        let written = write_generation(rows(0..1_000), &mut first, &token).unwrap();
        write_generation(rows((0..1_000).rev()), &mut second, &token).unwrap();
        assert_eq!(first, second);
        assert_eq!(written.size, first.len() as u64);
        let locator = Locator::decode(&written.locator.encode()).unwrap();
        let bytes = Buffer::from_slice_ref(&first);
        let directory_bytes = bytes.slice_with_length(locator.directory.0 as usize, locator.directory.1 as usize);
        let directory_layout = directory_schema(2).unwrap();
        let memory = MemoryBudget::new(1 << 20);
        let directory_batch = decode(StoredBytes(directory_bytes.clone()), || Ok(Arc::clone(&directory_layout)), None, &memory).unwrap();
        assert_eq!(directory_bytes.as_slice(), encode(&directory_layout, directory_batch.batch).unwrap().as_slice());
        let directory = Directory::read(StoredBytes(directory_bytes), &locator, &memory).unwrap();
        let total: u32 = (0..directory.len()).map(|row| directory.cluster(row).rows).sum();
        assert_eq!(total, 1_000);
        let schema = block_schema(2).unwrap();
        let mut previous_end = 0u64;
        for cluster in (0..directory.len()).map(|row| directory.cluster(row)) {
            let start = cluster.range.start;
            assert_eq!(start, previous_end.next_multiple_of(ALIGNMENT));
            assert!(first[previous_end as usize..start as usize].iter().all(|&byte| byte == 0));
            previous_end = cluster.range.end;
            let range = bytes.slice_with_length(start as usize, (cluster.range.end - start) as usize);
            let batch = decode(StoredBytes(range.clone()), || Ok(Arc::clone(&schema)), Some(cluster.rows as usize), &memory).unwrap();
            assert_eq!(range.as_slice(), encode(&schema, batch.batch).unwrap().as_slice());
            let block = Block::read(StoredBytes(range), &cluster, 2, &memory).unwrap();
            assert_eq!(block.len(), cluster.rows as usize);
            for row in 0..block.len() {
                let (_, _, seed) = block.extent(row);
                let angle = seed as f64 * 0.37;
                assert_eq!(block.unit(row), &[angle.cos() as f32, angle.sin() as f32]);
            }
        }
        assert_eq!(locator.directory.0, previous_end);
        assert_eq!(&first[written.size as usize - FOOTER_BYTES..], &locator.encode()[..FOOTER_BYTES]);
    }

    #[test]
    fn cluster_stream_uses_the_selected_unit_order() {
        let rows = rows([0, 2, 4, 6]);
        let source = rows.rows();
        let contiguous = [&source[1], &source[2], &source[3]];
        let reordered = [&source[3], &source[1], &source[2]];
        let gapped = [&source[0], &source[2], &source[3]];

        let schema = block_schema(2).unwrap();
        let memory = MemoryBudget::new(1 << 20);
        for (selected, run_lengths) in [(contiguous, &[3][..]), (reordered, &[1, 2][..]), (gapped, &[1, 2][..])] {
            let mut spans = rows.unit_spans(&selected);
            let mut start = 0;
            for &run_length in run_lengths {
                let span = spans.next().unwrap();
                assert_eq!(span.as_ptr(), rows.unit(selected[start]).as_ptr());
                assert_eq!(span.len(), run_length * rows.dimensions());
                start += run_length;
            }
            assert!(spans.next().is_none());

            let mut bytes = Vec::new();
            let (length, digest) = write_block(&mut bytes, &schema, &rows, &selected).unwrap();
            let cluster = Cluster { range: 0..length, rows: selected.len() as u32, digest };
            let buffer = Buffer::from_vec(bytes);
            let batch = decode(StoredBytes(buffer.clone()), || Ok(Arc::clone(&schema)), Some(selected.len()), &memory).unwrap();
            assert_eq!(buffer.as_slice(), encode(&schema, batch.batch).unwrap().as_slice());
            let block = Block::read(StoredBytes(buffer), &cluster, 2, &memory).unwrap();
            for (index, row) in selected.into_iter().enumerate() {
                assert_eq!(block.iid(index).unwrap(), row.iid);
                assert_eq!(block.extent(index), (row.valid_from, row.valid_to, row.system_from));
                assert_eq!(block.unit(index), rows.unit(row));
            }
        }
    }

    #[test]
    fn short_cluster_writes_keep_the_digest_and_io_failure_stops_before_the_directory() {
        struct ShortOutput { bytes: Vec<u8>, limit: usize }
        impl Write for ShortOutput {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                let count = bytes.len().min(11).min(self.limit - self.bytes.len());
                if count == 0 && !bytes.is_empty() { return Err(std::io::Error::other("sink failed")); }
                self.bytes.extend_from_slice(&bytes[..count]);
                Ok(count)
            }
            fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
        }

        let token = CancellationToken::new();
        let mut expected = Vec::new();
        let written = write_generation(rows(0..4), &mut expected, &token).unwrap();
        let mut output = ShortOutput { bytes: Vec::new(), limit: expected.len() };
        let actual = write_generation(rows(0..4), &mut output, &token).unwrap();
        assert_eq!(actual.locator, written.locator);
        assert_eq!(actual.size, written.size);
        assert_eq!(output.bytes, expected);

        let limit = written.locator.directory.0 as usize - END.len() - 1;
        let mut output = ShortOutput { bytes: Vec::new(), limit };
        let error = write_generation(rows(0..4), &mut output, &token).err().unwrap();
        assert_eq!(error.code, "rtdb.vector/io");
        assert_eq!(output.bytes.as_slice(), &expected[..limit]);
    }

    #[tokio::test]
    async fn directory_and_cluster_columns_keep_their_range_and_decode_charges() {
        let mut bytes = Vec::new();
        let locator = write_generation(rows(0..4), &mut bytes, &CancellationToken::new()).unwrap().locator;
        let key = ObjectKey::new("vector").unwrap();
        let range_owner = size_of::<CacheLease>() + 2 * BUFFER_OWNER_BYTES;
        let directory_len = locator.directory.1 as usize;
        for shift in [0, 1] {
            let store = Arc::new(MemoryStore::default());
            let mut stored = vec![0; shift];
            stored.extend_from_slice(&bytes);
            let stored = Buffer::from_slice_ref(&stored).slice(shift);
            store.put(&key, Bytes::from_owner(StoredBytes(stored))).await.unwrap();
            let pool = BufferPool::new(store, 0);

            let memory = MemoryBudget::new(1 << 20);
            let lease = pool.get_range_or(&key, locator.directory_range(), &memory).await.unwrap();
            let directory_bytes = lease.as_slice().as_ptr_range();
            let directory = Directory::read(lease, &locator, &memory).unwrap();
            let (cluster, later_cluster) = (directory.cluster(0), directory.cluster(1));
            assert_eq!(directory_bytes.contains(&directory.centroid(0).as_ptr().cast::<u8>()), shift == 0);
            let copies = shift * 4 * (64 + BUFFER_OWNER_BYTES);
            let directory_charge = directory_len + range_owner + copies;
            assert_eq!(memory.used_bytes(), directory_charge);

            let (release, wait) = oneshot::channel();
            let reads = futures::stream::iter([(&cluster, Some(wait)), (&later_cluster, None)].into_iter().map(|(cluster, wait)| {
                let (pool, key, memory) = (&pool, &key, &memory);
                async move {
                    let lease = pool.get_range_or(key, cluster.range.clone(), memory).await.unwrap();
                    if let Some(wait) = wait { wait.await.unwrap(); }
                    let addresses = lease.as_slice().as_ptr_range();
                    (cluster.range.start, addresses, Block::read(lease, cluster, 2, memory).unwrap())
                }
            })).buffer_unordered(2);
            futures::pin_mut!(reads);
            let (start, later_bytes, later_block) = reads.next().await.unwrap();
            assert_eq!(start, later_cluster.range.start);
            assert_eq!(later_bytes.contains(&later_block.unit(0).as_ptr().cast::<u8>()), shift == 0);
            release.send(()).unwrap();
            let (start, cluster_bytes, block) = reads.next().await.unwrap();
            assert_eq!(start, cluster.range.start);
            assert_eq!(cluster_bytes.contains(&block.unit(0).as_ptr().cast::<u8>()), shift == 0);
            let cluster_len = (cluster.range.end - cluster.range.start) as usize;
            let block_charge = cluster_len + range_owner + copies;
            let later_charge = (later_cluster.range.end - later_cluster.range.start) as usize + range_owner + copies;
            assert_eq!(memory.used_bytes(), directory_charge + block_charge + later_charge);
            drop(directory);
            assert_eq!(memory.used_bytes(), block_charge + later_charge);
            drop(block);
            assert_eq!(memory.used_bytes(), later_charge);
            drop(later_block);
            assert_eq!(memory.used_bytes(), 0);

            if shift == 1 {
                let limited = MemoryBudget::new(directory_len + range_owner + DECODE_SCRATCH_BYTES);
                let lease = pool.get_range_or(&key, locator.directory_range(), &limited).await.unwrap();
                let error = Directory::read(lease, &locator, &limited).err().unwrap();
                assert_eq!(error.code, "rtdb/memory-budget-exceeded");
                assert!(crate::shortlist::declined(&error));
                assert_eq!(limited.used_bytes(), 0);

                let limited = MemoryBudget::new(cluster_len + range_owner + DECODE_SCRATCH_BYTES);
                let lease = pool.get_range_or(&key, cluster.range.clone(), &limited).await.unwrap();
                let error = Block::read(lease, &cluster, 2, &limited).err().unwrap();
                assert_eq!(error.code, "rtdb/memory-budget-exceeded");
                assert!(crate::shortlist::declined(&error));
                assert_eq!(limited.used_bytes(), 0);
            }
        }
    }

    #[test]
    fn misaligned_cluster_values_still_decode_and_bad_records_decline() {
        let mut bytes = Vec::new();
        let locator = write_generation(rows(0..4), &mut bytes, &CancellationToken::new()).unwrap().locator;
        let aligned = Buffer::from_slice_ref(&bytes);
        let memory = MemoryBudget::new(1 << 20);
        let directory_bytes = aligned.slice_with_length(locator.directory.0 as usize, locator.directory.1 as usize);
        let directory = Directory::read(StoredBytes(directory_bytes), &locator, &memory).unwrap();
        let cluster = directory.cluster(0);
        let mut shifted = vec![0];
        shifted.extend_from_slice(&bytes[cluster.range.start as usize..cluster.range.end as usize]);
        let range = Buffer::from_slice_ref(&shifted).slice(1);
        let block = Block::read(StoredBytes(range.clone()), &cluster, 2, &memory).unwrap();
        assert_eq!(block.len(), cluster.rows as usize);
        assert!(!range.as_ptr_range().contains(&block.unit(0).as_ptr().cast::<u8>()));

        let bad_rows = Cluster { rows: cluster.rows + 1, range: cluster.range.clone(), ..cluster };
        let bad_digest = Cluster { digest: [0; 32], range: cluster.range.clone(), ..cluster };
        let bad_length = Cluster { range: cluster.range.start..cluster.range.end + 1, ..cluster };
        for record in [&bad_rows, &bad_digest, &bad_length] {
            let error = Block::read(StoredBytes(range.clone()), record, 2, &memory).err().unwrap();
            assert!(crate::shortlist::declined(&error));
        }
        let error = Block::read(StoredBytes(range), &cluster, 3, &memory).err().unwrap();
        assert!(crate::shortlist::declined(&error));
    }
}
