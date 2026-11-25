use std::io::{Cursor, Write};

use arrow_array::{Array, cast::AsArray};
use arrow_schema::Field;
use roaring::RoaringBitmap;
use rtdb_columnar::ArrowCell;
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, Value};

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
        let mut output = Vec::with_capacity(self.serialized_size());
        self.write_bytes(&mut output)?;
        Ok(output)
    }

    pub(crate) fn serialized_size(&self) -> usize {
        self.bitmap.serialized_size()
    }

    pub(crate) fn write_bytes(&self, output: impl Write) -> Result<()> {
        self.bitmap.serialize_into(output).map_err(invalid)
    }

    /// Adds bit positions in bulk; sorted and de-duplicated first, they enter the bitmap as one union.
    pub fn insert_positions(&mut self, positions: &mut Vec<u32>) {
        positions.sort_unstable();
        positions.dedup();
        if let Ok(other) = RoaringBitmap::from_sorted_iter(positions.drain(..)) { self.bitmap |= other; }
    }

    /// Adds bit positions given in strictly increasing order.
    pub fn insert_sorted_positions(&mut self, positions: impl IntoIterator<Item = u32>) {
        if let Ok(other) = RoaringBitmap::from_sorted_iter(positions) { self.bitmap |= other; }
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

    /// Adds the `rows` of a string, binary or fixed-size binary array as `insert_cell` adds each, hashing every cell's
    /// bytes in place and entering their positions in one union; false, adding nothing, for any other layout.
    pub fn insert_byte_cells<'a>(&mut self, array: &'a dyn Array, rows: impl Iterator<Item = usize>) -> bool {
        let cell: Box<dyn Fn(usize) -> &'a [u8] + 'a> = match array.data_type() {
            arrow_schema::DataType::Utf8 => { let array = array.as_string::<i32>(); Box::new(move |row| array.value(row).as_bytes()) }
            arrow_schema::DataType::Binary => { let array = array.as_binary::<i32>(); Box::new(move |row| array.value(row)) }
            arrow_schema::DataType::FixedSizeBinary(_) => { let array = array.as_fixed_size_binary(); Box::new(move |row| array.value(row)) }
            _ => return false,
        };
        let mut positions = Vec::new();
        for row in rows {
            let (first, second) = if array.is_null(row) { (0, 0) } else {
                let bytes = cell(row);
                (rtdb_columnar::xxhash32(bytes, 0) as i32, rtdb_columnar::murmur3_32(bytes, 0) as i32)
            };
            positions.extend(hashes(first, second));
        }
        self.insert_positions(&mut positions);
        true
    }

    pub fn insert_cell(&mut self, field: &Field, array: &dyn Array, row: usize) -> Result<()> {
        let (first, second) = cell_hashes(field, array, row)?.unwrap_or((0, 0));
        self.insert_hashes(first, second);
        Ok(())
    }

    pub fn contains_hashes(&self, hash1: i32, hash2: i32) -> bool {
        self.contains_positions(&hashes(hash1, hash2))
    }

    pub fn contains_positions(&self, positions: &[u32; BLOOM_K as usize]) -> bool {
        positions.iter().all(|&position| self.bitmap.contains(position))
    }

    pub fn contains_cell(&self, field: &Field, array: &dyn Array, row: usize) -> Result<bool> {
        let (first, second) = cell_hashes(field, array, row)?.unwrap_or((0, 0));
        Ok(self.contains_hashes(first, second))
    }

    /// Whether a byte-backed value's persisted-column hashes are all present.
    pub fn contains_value(&self, value: &Value) -> Result<bool> {
        Ok(self.contains_positions(&Self::byte_value_positions(value)?))
    }

    /// The positions to probe in a persisted column Bloom filter for a byte-backed value.
    pub fn byte_value_positions(value: &Value) -> Result<[u32; BLOOM_K as usize]> {
        let (first, second) = byte_hashes(value)?;
        Ok(hashes(first, second))
    }

    pub fn intersects(&self, other: &Self) -> bool { !self.bitmap.is_disjoint(&other.bitmap) }
    pub fn cardinality(&self) -> u64 { self.bitmap.len() }
}

fn cell_hashes(field: &Field, array: &dyn Array, row: usize) -> Result<Option<(i32, i32)>> {
    let cell = ArrowCell { field, array, row };
    if let Ok(physical) = cell.physical()
        && let Ok(logical) = physical.logical_type()
        && (field.data_type() == physical.field.data_type()
            || rtdb_columnar::from_field(field).and_then(|logical| rtdb_columnar::to_field(field.name(), &logical))
                .is_ok_and(|canonical| canonical.data_type() == field.data_type())) {
        let ArrowCell { array, row, .. } = physical;
        let bytes = match logical {
            LogicalType::Null => return Ok(None),
            LogicalType::Scalar(ScalarType::Utf8 | ScalarType::Keyword) => Some(array.as_string::<i32>().value(row).as_bytes()),
            LogicalType::Scalar(ScalarType::Uri) => {
                let uri = array.as_string::<i32>().value(row);
                rtdb_types::UriParts::parse(uri)?;
                Some(uri.as_bytes())
            }
            LogicalType::Scalar(ScalarType::Binary | ScalarType::Transit) => Some(array.as_binary::<i32>().value(row)),
            LogicalType::Scalar(ScalarType::Uuid | ScalarType::FixedSizeBinary(_)) => Some(array.as_fixed_size_binary().value(row)),
            _ => None,
        };
        if let Some(bytes) = bytes { return Ok(Some(hash_bytes(bytes))); }
    }
    match rtdb_columnar::read_value(field, array, row)? {
        Value::Null | Value::Undefined => Ok(None),
        value => byte_hashes(&value).map(Some),
    }
}

fn hash_bytes(bytes: &[u8]) -> (i32, i32) {
    (rtdb_columnar::xxhash32(bytes, 0) as i32, rtdb_columnar::murmur3_32(bytes, 0) as i32)
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
    Ok(hash_bytes(bytes))
}

pub fn hashes(hash1: i32, hash2: i32) -> [u32; BLOOM_K as usize] {
    std::array::from_fn(|index| hash1.wrapping_add(hash2.wrapping_mul(index as i32)) as u32 & ((1 << BLOOM_BITS) - 1))
}

pub fn from_column(field: &Field, array: &dyn Array) -> Result<BloomFilter> {
    let mut bloom = BloomFilter::default();
    rtdb_columnar::from_field(field)?;
    for row in 0..array.len() {
        if let Some((first, second)) = cell_hashes(field, array, row)? {
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
    fn metadata_bloom_cells_hash_active_byte_legs_and_nulls() {
        use rtdb_types::{Keyword, ScalarType};

        let logical = LogicalType::from_legs([LogicalType::Null, LogicalType::Scalar(ScalarType::Keyword),
            LogicalType::Scalar(ScalarType::Binary), LogicalType::Scalar(ScalarType::Uuid)]);
        let values = [Value::Null, Value::Keyword(Keyword::from_name("ns/x")), Value::Binary(vec![0, 0xff]), Value::Uuid([3; 16])];
        let field = rtdb_columnar::to_field("id", &logical).unwrap();
        let array = rtdb_columnar::build_array(&logical, &values).unwrap();
        let mut expected = BloomFilter::default();
        let mut actual = BloomFilter::default();
        for (row, value) in values.iter().enumerate() {
            let (first, second) = byte_hashes(value).unwrap();
            expected.insert_hashes(first, second);
            actual.insert_cell(&field, array.as_ref(), row).unwrap();
            assert!(actual.contains_cell(&field, array.as_ref(), row).unwrap());
        }
        assert_eq!(actual.to_bytes().unwrap(), expected.to_bytes().unwrap());
    }

    #[test]
    fn metadata_bloom_rejects_aliased_physical_legs() {
        use std::sync::Arc;
        use arrow_array::{BinaryArray, StringArray, UnionArray};
        use arrow_schema::{DataType, UnionFields};

        let array = UnionArray::try_new(UnionFields::try_new([0, 1], [
            Field::new("binary", DataType::Utf8, false),
            Field::new("utf8", DataType::Binary, false),
        ]).unwrap(), vec![0, 1].into(), Some(vec![0, 0].into()), vec![
            Arc::new(StringArray::from(vec!["text"])),
            Arc::new(BinaryArray::from_iter_values([b"bytes"])),
        ]).unwrap();
        let field = Field::new("id", array.data_type().clone(), false);
        let expected = rtdb_columnar::read_value(&field, &array, 0).unwrap_err();
        let actual = BloomFilter::default().insert_cell(&field, &array, 0).unwrap_err();
        assert_eq!(actual, expected);
    }

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
