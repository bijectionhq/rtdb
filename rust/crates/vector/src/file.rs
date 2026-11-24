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
    builder::FixedSizeBinaryBuilder,
};
use arrow_buffer::Buffer;
use arrow_ipc::{Message, convert::fb_to_schema, reader::read_record_batch, root_as_message, writer::StreamWriter};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rtdb_types::{Anomaly, Iid, Result};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{definition::FILE_VERSION, kmeans, rows::{IndexedRow, IndexedRows, Partition}};

const MAGIC: &[u8; 8] = b"RTDBVEC1";
const FOOTER_BYTES: usize = 32;
const ALIGNMENT: u64 = 64;
const CONTINUATION: [u8; 4] = [255; 4];
const END: [u8; 8] = [255, 255, 255, 255, 0, 0, 0, 0];

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
            let padding = ((ALIGNMENT - written % ALIGNMENT) % ALIGNMENT) as usize;
            output.write_all(&[0u8; ALIGNMENT as usize][..padding]).map_err(io)?;
            written += padding as u64;
            let bytes = encode(&block, block_batch(&block, &rows, &selected)?)?;
            output.write_all(&bytes).map_err(io)?;
            partitions.push(partition);
            centroids.extend_from_slice(centroid);
            offsets.push(written);
            lengths.push(bytes.len() as u64);
            counts.push(u32::try_from(selected.len()).map_err(|_| corrupt("A cluster exceeds the row count the format records"))?);
            digests.push(<[u8; 32]>::from(Sha256::digest(&bytes)));
            written += bytes.len() as u64;
        }
        start = end;
    }
    let directory = directory_schema(dimensions)?;
    let mut partition_column = FixedSizeBinaryBuilder::with_capacity(partitions.len(), 32);
    for partition in &partitions { partition_column.append_value(partition.0).map_err(arrow)?; }
    let mut digest_column = FixedSizeBinaryBuilder::with_capacity(digests.len(), 32);
    for digest in &digests { digest_column.append_value(digest).map_err(arrow)?; }
    let batch = RecordBatch::try_new(Arc::clone(&directory), vec![
        Arc::new(partition_column.finish()),
        Arc::new(unit_list(centroids, dimensions)?),
        Arc::new(UInt64Array::from(offsets)),
        Arc::new(UInt64Array::from(lengths)),
        Arc::new(UInt32Array::from(counts)),
        Arc::new(digest_column.finish()),
    ]).map_err(arrow)?;
    let bytes = encode(&directory, batch)?;
    let directory_offset = written;
    output.write_all(&bytes).map_err(io)?;
    written += bytes.len() as u64;
    let locator = Locator { directory: (directory_offset, bytes.len() as u64), dimensions: dimensions_u32, digest: Sha256::digest(&bytes).into() };
    output.write_all(&footer(directory_offset, bytes.len() as u64, dimensions_u32)).map_err(io)?;
    written += FOOTER_BYTES as u64;
    Ok(Written { size: written, locator })
}

fn block_batch(schema: &SchemaRef, rows: &IndexedRows, selected: &[&IndexedRow]) -> Result<RecordBatch> {
    let mut iids = FixedSizeBinaryBuilder::with_capacity(selected.len(), 16);
    let mut units = Vec::with_capacity(selected.len() * rows.dimensions());
    for row in selected {
        iids.append_value(row.iid.0).map_err(arrow)?;
        units.extend_from_slice(rows.unit(row));
    }
    RecordBatch::try_new(Arc::clone(schema), vec![
        Arc::new(iids.finish()),
        Arc::new(Int64Array::from_iter_values(selected.iter().map(|row| row.valid_from))),
        Arc::new(Int64Array::from_iter_values(selected.iter().map(|row| row.valid_to))),
        Arc::new(Int64Array::from_iter_values(selected.iter().map(|row| row.system_from))),
        Arc::new(unit_list(units, rows.dimensions())?),
    ]).map_err(arrow)
}

fn unit_list(values: Vec<f32>, dimensions: usize) -> Result<FixedSizeListArray> {
    let width = i32::try_from(dimensions).map_err(|_| corrupt("Vector dimensions exceed the Arrow list width"))?;
    FixedSizeListArray::try_new(Arc::new(Field::new("item", DataType::Float32, false)), width, Arc::new(Float32Array::from(values)), None)
        .map_err(arrow)
}

fn encode(schema: &SchemaRef, batch: RecordBatch) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, schema).map_err(arrow)?;
    writer.write(&batch).map_err(arrow)?;
    writer.finish().map_err(arrow)?;
    drop(writer);
    Ok(bytes)
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

fn decode(bytes: Buffer, schema: &SchemaRef, expected_rows: Option<usize>) -> Result<RecordBatch> {
    let (header, body) = message(&bytes)?;
    let stored = header.header_as_schema().filter(|_| body.is_empty()).ok_or_else(|| corrupt("A vector stream must begin with its schema"))?;
    if !stored.endianness().equals_to_target_endianness()
        || stored.fields().is_none_or(|fields| fields.len() != schema.fields().len())
        || fb_to_schema(stored).fields() != schema.fields() {
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
    // Arrow retains aligned slices of the input and copies misaligned primitive buffers.
    read_record_batch(&data, batch, Arc::clone(schema), &Default::default(), None, &header.version())
        .map_err(|error| corrupt(&error.to_string()))
}

/// One cluster as the directory names it.
pub(crate) struct Cluster {
    pub(crate) partition: Partition,
    pub(crate) centroid: Range<usize>,
    pub(crate) range: Range<u64>,
    pub(crate) rows: u32,
    pub(crate) digest: [u8; 32],
}

/// A generation's directory: its clusters and their unit centroids.
pub(crate) struct Directory { pub(crate) dimensions: usize, pub(crate) clusters: Vec<Cluster>, centroids: Float32Array }

impl Directory {
    /// Decodes directory bytes, checking them against the locator's digest.
    pub(crate) fn read(bytes: Buffer, locator: &Locator) -> Result<Self> {
        if bytes.len() as u64 != locator.directory.1 || <[u8; 32]>::from(Sha256::digest(bytes.as_slice())) != locator.digest {
            return Err(corrupt("A vector generation directory does not match its recorded length or digest"));
        }
        let dimensions = locator.dimensions as usize;
        let batch = decode(bytes, &directory_schema(dimensions)?, None)?;
        let column = |index: usize| batch.column(index).as_ref();
        let partitions = column(0).as_any().downcast_ref::<FixedSizeBinaryArray>().ok_or_else(|| corrupt("partition"))?;
        let centroids = column(1).as_any().downcast_ref::<FixedSizeListArray>().ok_or_else(|| corrupt("centroid"))?;
        let offsets = column(2).as_any().downcast_ref::<UInt64Array>().ok_or_else(|| corrupt("offset"))?;
        let lengths = column(3).as_any().downcast_ref::<UInt64Array>().ok_or_else(|| corrupt("length"))?;
        let counts = column(4).as_any().downcast_ref::<UInt32Array>().ok_or_else(|| corrupt("rows"))?;
        let digests = column(5).as_any().downcast_ref::<FixedSizeBinaryArray>().ok_or_else(|| corrupt("digest"))?;
        let values = centroids.values().as_any().downcast_ref::<Float32Array>().ok_or_else(|| corrupt("centroid"))?.clone();
        if batch.num_rows().checked_mul(dimensions) != Some(values.len()) { return Err(corrupt("centroid width")); }
        let clusters = (0..batch.num_rows()).map(|row| {
            let offset = offsets.value(row);
            let end = offset.checked_add(lengths.value(row))
                .filter(|end| offset.is_multiple_of(ALIGNMENT) && offset < *end && *end <= locator.directory.0)
                .ok_or_else(|| corrupt("A cluster has an invalid range"))?;
            Ok(Cluster {
                partition: Partition(partitions.value(row).try_into().map_err(|_| corrupt("partition"))?),
                centroid: row * dimensions..(row + 1) * dimensions,
                range: offset..end, rows: counts.value(row),
                digest: digests.value(row).try_into().map_err(|_| corrupt("digest"))?,
            })
        }).collect::<Result<Vec<_>>>()?;
        Ok(Self { dimensions, clusters, centroids: values })
    }

    pub(crate) fn centroid(&self, cluster: &Cluster) -> &[f32] { &self.centroids.values()[cluster.centroid.clone()] }
}

/// One cluster's rows.
pub(crate) struct Block { iids: FixedSizeBinaryArray, valid_from: Int64Array, valid_to: Int64Array, system_from: Int64Array, units: Float32Array, dimensions: usize }

impl Block {
    pub(crate) fn read(bytes: Buffer, cluster: &Cluster, dimensions: usize) -> Result<Self> {
        if cluster.range.end.checked_sub(cluster.range.start) != Some(bytes.len() as u64)
            || <[u8; 32]>::from(Sha256::digest(bytes.as_slice())) != cluster.digest {
            return Err(corrupt("A vector cluster does not match its recorded length or digest"));
        }
        let batch = decode(bytes, &block_schema(dimensions)?, Some(cluster.rows as usize))?;
        let column = |index: usize| batch.column(index).as_ref();
        let integers = |index: usize| column(index).as_any().downcast_ref::<Int64Array>().cloned().ok_or_else(|| corrupt("temporal column"));
        let units = column(4).as_any().downcast_ref::<FixedSizeListArray>().ok_or_else(|| corrupt("unit"))?
            .values().as_any().downcast_ref::<Float32Array>().ok_or_else(|| corrupt("unit"))?.clone();
        if batch.num_rows().checked_mul(dimensions) != Some(units.len()) { return Err(corrupt("unit width")); }
        Ok(Self {
            iids: column(0).as_any().downcast_ref::<FixedSizeBinaryArray>().cloned().ok_or_else(|| corrupt("_iid"))?,
            valid_from: integers(1)?, valid_to: integers(2)?, system_from: integers(3)?, units, dimensions,
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
    use bytes::Bytes;
    use rtdb_columnar::MemoryBudget;
    use rtdb_storage::{MemoryStore, ObjectKey, ObjectStore, cache::BufferPool};
    use rtdb_types::{EventHeader, EventOp, Interval, Timestamp};

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
        let directory = Directory::read(bytes.slice_with_length(locator.directory.0 as usize, locator.directory.1 as usize), &locator).unwrap();
        let total: u32 = directory.clusters.iter().map(|cluster| cluster.rows).sum();
        assert_eq!(total, 1_000);
        for cluster in &directory.clusters {
            let block = Block::read(bytes.slice_with_length(cluster.range.start as usize, (cluster.range.end - cluster.range.start) as usize), cluster, 2).unwrap();
            assert_eq!(block.len(), cluster.rows as usize);
            for row in 0..block.len() {
                let (_, _, seed) = block.extent(row);
                let angle = seed as f64 * 0.37;
                assert_eq!(block.unit(row), &[angle.cos() as f32, angle.sin() as f32]);
            }
        }
    }

    #[tokio::test]
    async fn aligned_directory_and_cluster_columns_keep_their_range_charges() {
        struct Aligned(Buffer);
        impl AsRef<[u8]> for Aligned {
            fn as_ref(&self) -> &[u8] { self.0.as_slice() }
        }

        let mut bytes = Vec::new();
        let locator = write_generation(rows(0..4), &mut bytes, &CancellationToken::new()).unwrap().locator;
        let memory = MemoryBudget::new(bytes.len());
        let store = Arc::new(MemoryStore::default());
        let key = ObjectKey::new("vector").unwrap();
        store.put(&key, Bytes::from_owner(Aligned(Buffer::from_slice_ref(&bytes)))).await.unwrap();
        let pool = BufferPool::new(store, 0);

        let lease = pool.get_range_or(&key, locator.directory_range(), &memory).await.unwrap();
        let directory_bytes = lease.as_slice().as_ptr_range();
        let directory = Directory::read(Buffer::from(Bytes::from_owner(lease)), &locator).unwrap();
        let cluster = &directory.clusters[0];
        assert!(directory_bytes.contains(&directory.centroid(cluster).as_ptr().cast::<u8>()));
        assert_eq!(memory.used_bytes(), locator.directory.1 as usize);

        let lease = pool.get_range_or(&key, cluster.range.clone(), &memory).await.unwrap();
        let cluster_bytes = lease.as_slice().as_ptr_range();
        let cluster_len = lease.as_slice().len();
        let block = Block::read(Buffer::from(Bytes::from_owner(lease)), cluster, 2).unwrap();
        assert!(cluster_bytes.contains(&block.unit(0).as_ptr().cast::<u8>()));
        assert_eq!(memory.used_bytes(), locator.directory.1 as usize + cluster_len);
        drop(directory);
        assert_eq!(memory.used_bytes(), cluster_len);
        drop(block);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn misaligned_cluster_values_still_decode_and_bad_records_decline() {
        let mut bytes = Vec::new();
        let locator = write_generation(rows(0..4), &mut bytes, &CancellationToken::new()).unwrap().locator;
        let aligned = Buffer::from_slice_ref(&bytes);
        let directory = Directory::read(aligned.slice_with_length(locator.directory.0 as usize, locator.directory.1 as usize), &locator).unwrap();
        let cluster = &directory.clusters[0];
        let mut shifted = vec![0];
        shifted.extend_from_slice(&bytes[cluster.range.start as usize..cluster.range.end as usize]);
        let range = Buffer::from_slice_ref(&shifted).slice(1);
        let block = Block::read(range.clone(), cluster, 2).unwrap();
        assert_eq!(block.len(), cluster.rows as usize);
        assert!(!range.as_ptr_range().contains(&block.unit(0).as_ptr().cast::<u8>()));

        let bad_rows = Cluster { rows: cluster.rows + 1, range: cluster.range.clone(), centroid: cluster.centroid.clone(), ..*cluster };
        let bad_digest = Cluster { digest: [0; 32], range: cluster.range.clone(), centroid: cluster.centroid.clone(), ..*cluster };
        let bad_length = Cluster { range: cluster.range.start..cluster.range.end + 1, centroid: cluster.centroid.clone(), ..*cluster };
        for record in [&bad_rows, &bad_digest, &bad_length] {
            let error = Block::read(range.clone(), record, 2).err().unwrap();
            assert!(crate::shortlist::declined(&error));
        }
        let error = Block::read(range, cluster, 3).err().unwrap();
        assert!(crate::shortlist::declined(&error));
    }
}
