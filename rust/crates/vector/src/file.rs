//! The generation file: immutable, byte-deterministic IVF clusters over one trie's current puts (vector.allium: VectorGeneration).
//!
//! Layout: each cluster's rows as an Arrow IPC stream, each aligned to 64 bytes, then a directory stream
//! naming every cluster's partition, centroid, byte range, row count and digest, then a 32-byte footer.
//! The artefact record's stats carry the footer and the directory digest, so a reader opens the directory
//! with one range read and checks every byte it uses.

use std::io::{Cursor, Write};
use std::ops::Range;
use std::sync::Arc;

use arrow_array::{
    Array, FixedSizeBinaryArray, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, UInt32Array, UInt64Array,
    builder::FixedSizeBinaryBuilder,
};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rtdb_types::{Anomaly, Iid, Result};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{definition::FILE_VERSION, kmeans, rows::{IndexedRow, Partition}};

const MAGIC: &[u8; 8] = b"RTDBVEC1";
const FOOTER_BYTES: usize = 32;
const ALIGNMENT: u64 = 64;

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
pub(crate) fn write_generation(mut rows: Vec<IndexedRow>, dimensions: usize, output: &mut impl Write, cancellation: &CancellationToken) -> Result<Written> {
    let dimensions_u32 = u32::try_from(dimensions).map_err(|_| corrupt("Vector dimensions exceed the file format"))?;
    rows.sort_by(|left, right| (left.partition, left.iid, left.system_from, left.valid_from, left.valid_to)
        .cmp(&(right.partition, right.iid, right.system_from, right.valid_from, right.valid_to)));
    let block = block_schema(dimensions)?;
    let mut written = 0u64;
    let mut partitions = Vec::new();
    let mut centroids = Vec::new();
    let mut offsets = Vec::new();
    let mut lengths = Vec::new();
    let mut counts = Vec::new();
    let mut digests = Vec::new();
    let mut start = 0;
    while start < rows.len() {
        let partition = rows[start].partition;
        let end = start + rows[start..].iter().take_while(|row| row.partition == partition).count();
        let members = &rows[start..end];
        let units: Vec<&[f32]> = members.iter().map(|row| row.unit.as_slice()).collect();
        let (trained, assignment) = kmeans::train(&units, dimensions, cancellation)?;
        for (cluster, centroid) in trained.chunks_exact(dimensions).enumerate() {
            let selected: Vec<&IndexedRow> = members.iter().zip(&assignment).filter(|(_, assigned)| **assigned as usize == cluster)
                .map(|(row, _)| row).collect();
            if selected.is_empty() { continue; }
            if cancellation.is_cancelled() { return Err(kmeans::cancelled()); }
            let padding = ((ALIGNMENT - written % ALIGNMENT) % ALIGNMENT) as usize;
            output.write_all(&[0u8; ALIGNMENT as usize][..padding]).map_err(io)?;
            written += padding as u64;
            let bytes = encode(&block, block_batch(&block, &selected, dimensions)?)?;
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

fn block_batch(schema: &SchemaRef, rows: &[&IndexedRow], dimensions: usize) -> Result<RecordBatch> {
    let mut iids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    for row in rows { iids.append_value(row.iid.0).map_err(arrow)?; }
    RecordBatch::try_new(Arc::clone(schema), vec![
        Arc::new(iids.finish()),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.valid_from))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.valid_to))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.system_from))),
        Arc::new(unit_list(rows.iter().flat_map(|row| row.unit.iter().copied()).collect(), dimensions)?),
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

fn decode(bytes: &[u8], schema: &SchemaRef) -> Result<RecordBatch> {
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(arrow)?;
    let batch = reader.next().ok_or_else(|| corrupt("A vector generation stream holds no batch"))?.map_err(arrow)?;
    if reader.next().is_some() || batch.schema_ref().fields() != schema.fields() {
        return Err(corrupt("A vector generation stream does not hold exactly one batch of its layout"));
    }
    Ok(batch)
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
    pub(crate) fn read(bytes: &[u8], locator: &Locator) -> Result<Self> {
        if <[u8; 32]>::from(Sha256::digest(bytes)) != locator.digest { return Err(corrupt("A vector generation directory does not match its digest")); }
        let dimensions = locator.dimensions as usize;
        let batch = decode(bytes, &directory_schema(dimensions)?)?;
        let column = |index: usize| batch.column(index).as_ref();
        let partitions = column(0).as_any().downcast_ref::<FixedSizeBinaryArray>().ok_or_else(|| corrupt("partition"))?;
        let centroids = column(1).as_any().downcast_ref::<FixedSizeListArray>().ok_or_else(|| corrupt("centroid"))?;
        let offsets = column(2).as_any().downcast_ref::<UInt64Array>().ok_or_else(|| corrupt("offset"))?;
        let lengths = column(3).as_any().downcast_ref::<UInt64Array>().ok_or_else(|| corrupt("length"))?;
        let counts = column(4).as_any().downcast_ref::<UInt32Array>().ok_or_else(|| corrupt("rows"))?;
        let digests = column(5).as_any().downcast_ref::<FixedSizeBinaryArray>().ok_or_else(|| corrupt("digest"))?;
        let values = centroids.values().as_any().downcast_ref::<Float32Array>().ok_or_else(|| corrupt("centroid"))?.clone();
        if values.len() != batch.num_rows() * dimensions { return Err(corrupt("centroid width")); }
        let clusters = (0..batch.num_rows()).map(|row| {
            let end = offsets.value(row).checked_add(lengths.value(row)).filter(|end| *end <= locator.directory.0)
                .ok_or_else(|| corrupt("A cluster extends past the directory"))?;
            Ok(Cluster {
                partition: Partition(partitions.value(row).try_into().map_err(|_| corrupt("partition"))?),
                centroid: row * dimensions..(row + 1) * dimensions,
                range: offsets.value(row)..end, rows: counts.value(row),
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
    pub(crate) fn read(bytes: &[u8], cluster: &Cluster, dimensions: usize) -> Result<Self> {
        if <[u8; 32]>::from(Sha256::digest(bytes)) != cluster.digest { return Err(corrupt("A vector cluster does not match its digest")); }
        let batch = decode(bytes, &block_schema(dimensions)?)?;
        if batch.num_rows() != cluster.rows as usize { return Err(corrupt("A vector cluster does not hold its recorded rows")); }
        let column = |index: usize| batch.column(index).as_ref();
        let integers = |index: usize| column(index).as_any().downcast_ref::<Int64Array>().cloned().ok_or_else(|| corrupt("temporal column"));
        let units = column(4).as_any().downcast_ref::<FixedSizeListArray>().ok_or_else(|| corrupt("unit"))?
            .values().as_any().downcast_ref::<Float32Array>().ok_or_else(|| corrupt("unit"))?.clone();
        if units.len() != batch.num_rows() * dimensions { return Err(corrupt("unit width")); }
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

    fn rows(count: u64) -> Vec<IndexedRow> {
        (0..count).map(|seed| {
            let mut iid = [0u8; 16];
            iid[..8].copy_from_slice(&seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes());
            let angle = seed as f64 * 0.37;
            IndexedRow {
                iid: Iid(iid), valid_from: 0, valid_to: i64::MAX, system_from: seed as i64,
                partition: Partition::of([Some(std::borrow::Cow::Owned(rtdb_types::Value::I64((seed % 2) as i64)))]).unwrap(),
                unit: vec![angle.cos() as f32, angle.sin() as f32],
            }
        }).collect()
    }

    #[test]
    fn a_generation_is_a_function_of_its_rows() {
        let token = CancellationToken::new();
        let (mut first, mut second) = (Vec::new(), Vec::new());
        let written = write_generation(rows(1_000), 2, &mut first, &token).unwrap();
        let mut reversed = rows(1_000);
        reversed.reverse();
        write_generation(reversed, 2, &mut second, &token).unwrap();
        assert_eq!(first, second);
        assert_eq!(written.size, first.len() as u64);
        let locator = Locator::decode(&written.locator.encode()).unwrap();
        let directory = Directory::read(&first[locator.directory_range().start as usize..locator.directory_range().end as usize], &locator).unwrap();
        let total: u32 = directory.clusters.iter().map(|cluster| cluster.rows).sum();
        assert_eq!(total, 1_000);
        for cluster in &directory.clusters {
            let block = Block::read(&first[cluster.range.start as usize..cluster.range.end as usize], cluster, 2).unwrap();
            assert_eq!(block.len(), cluster.rows as usize);
        }
    }
}
