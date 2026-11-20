use std::io::Cursor;

use arrow_array::Array;
use arrow_schema::Field;
use roaring::RoaringBitmap;
use rtdb_types::{Anomaly, Result, Value};

pub const BLOOM_BITS: u32 = 22;
pub const BLOOM_K: u32 = 5;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct BloomFilter {
    bitmap: RoaringBitmap,
}

impl BloomFilter {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut input = Cursor::new(bytes);
        let bitmap = RoaringBitmap::deserialize_from(&mut input).map_err(invalid)?;
        if input.position() != bytes.len() as u64 { return Err(invalid("Trailing bytes after Bloom bitmap")); }
        Ok(Self { bitmap })
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut output = Vec::with_capacity(self.bitmap.serialized_size());
        self.bitmap.serialize_into(&mut output).map_err(invalid)?;
        Ok(output)
    }

    /// Adds bit positions in bulk; sorted and de-duplicated first, they enter the bitmap as one union.
    pub fn insert_positions(&mut self, positions: &mut Vec<u32>) {
        positions.sort_unstable();
        positions.dedup();
        if let Ok(other) = RoaringBitmap::from_sorted_iter(positions.drain(..)) { self.bitmap |= other; }
    }

    pub fn insert_hashes(&mut self, hash1: i32, hash2: i32) {
        for hash in hashes(hash1, hash2) { self.bitmap.insert(hash); }
    }

    /// Adds the five JVM vector hashes, including the null vector's zero hashes.
    /// Struct values must retain the source vector's field order.
    /// The bit positions `insert_value` sets for `value`.
    pub fn value_positions(value: &Value) -> Result<[u32; BLOOM_K as usize]> {
        Ok(hashes(rtdb_columnar::hash_value(value, 0)?, rtdb_columnar::hash_value_murmur3(value, 0)?))
    }

    pub fn insert_value(&mut self, value: &Value) -> Result<()> {
        self.insert_hashes(rtdb_columnar::hash_value(value, 0)?, rtdb_columnar::hash_value_murmur3(value, 0)?);
        Ok(())
    }

    pub fn insert_cell(&mut self, field: &Field, array: &dyn Array, row: usize) -> Result<()> {
        let (first, second) = byte_hashes(&rtdb_columnar::read_value(field, array, row)?)?;
        self.insert_hashes(first, second);
        Ok(())
    }

    pub fn contains_hashes(&self, hash1: i32, hash2: i32) -> bool {
        hashes(hash1, hash2).into_iter().all(|hash| self.bitmap.contains(hash))
    }

    pub fn contains_cell(&self, field: &Field, array: &dyn Array, row: usize) -> Result<bool> {
        let (first, second) = byte_hashes(&rtdb_columnar::read_value(field, array, row)?)?;
        Ok(self.contains_hashes(first, second))
    }

    /// Whether a byte-backed value's persisted-column hashes are all present.
    pub fn contains_value(&self, value: &Value) -> Result<bool> {
        let (first, second) = byte_hashes(value)?;
        Ok(self.contains_hashes(first, second))
    }

    pub fn intersects(&self, other: &Self) -> bool { !self.bitmap.is_disjoint(&other.bitmap) }
    pub fn cardinality(&self) -> u64 { self.bitmap.len() }
}

fn byte_hashes(value: &Value) -> Result<(i32, i32)> {
    let bytes: &[u8] = match value {
        Value::Null | Value::Undefined => return Ok((0, 0)),
        Value::Utf8(value) | Value::Uri(value) => value.as_bytes(),
        Value::Keyword(value) => value.as_str().as_bytes(),
        Value::Binary(value) | Value::FixedSizeBinary(value) | Value::Transit(value) => value,
        Value::Uuid(value) => value,
        _ => return Err(Anomaly::unsupported("rtdb.format/non-byte-bloom", "Persisted column Bloom metadata supports byte-backed values")),
    };
    Ok((rtdb_columnar::xxhash32(bytes, 0) as i32, rtdb_columnar::murmur3_32(bytes, 0) as i32))
}

pub fn hashes(hash1: i32, hash2: i32) -> [u32; BLOOM_K as usize] {
    std::array::from_fn(|index| hash1.wrapping_add(hash2.wrapping_mul(index as i32)) as u32 & ((1 << BLOOM_BITS) - 1))
}

pub fn from_column(field: &Field, array: &dyn Array) -> Result<BloomFilter> {
    let mut bloom = BloomFilter::default();
    let reader = rtdb_columnar::ColumnReader::new(field)?;
    for row in 0..array.len() {
        let value = reader.read(array, row)?;
        if !matches!(value, Value::Null | Value::Undefined) {
            let (first, second) = byte_hashes(&value)?;
            bloom.insert_hashes(first, second);
        }
    }
    Ok(bloom)
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("rtdb.format/invalid-bloom", error.to_string())
}

#[cfg(test)]
mod bulk_tests {
    use super::*;

    #[test]
    fn positions_added_in_bulk_set_the_same_bits_as_values_added_one_by_one() {
        let values: Vec<Value> = (0..5000).map(|index| if index % 7 == 0 { Value::Utf8(format!("k{}", index % 300)) } else { Value::I64(index % 1000) }).collect();
        let mut single = BloomFilter::default();
        for value in &values { single.insert_value(value).unwrap(); }
        let mut bulk = BloomFilter::default();
        let mut pending = Vec::new();
        for (index, value) in values.iter().enumerate() {
            pending.extend(BloomFilter::value_positions(value).unwrap());
            if index % 777 == 0 { bulk.insert_positions(&mut pending); }
        }
        bulk.insert_positions(&mut pending);
        assert_eq!(bulk, single);
    }
}
