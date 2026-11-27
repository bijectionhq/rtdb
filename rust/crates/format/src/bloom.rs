use std::io::{Cursor, Write};

use arrow_array::{Array, cast::AsArray};
use arrow_schema::Field;
use roaring::RoaringBitmap;
use rtdb_columnar::ArrowCell;
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, Value};

pub const BLOOM_BITS: u32 = 22;
pub const BLOOM_K: u32 = 5;
const RADIX_DIGIT_BITS: u32 = BLOOM_BITS.div_ceil(2);

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
        if positions.len() >= 1024 {
            let mut scratch = vec![0; positions.len()];
            radix_pass(&positions, &mut scratch, 0);
            radix_pass(&scratch, &mut positions, RADIX_DIGIT_BITS);
        } else {
            positions.sort_unstable();
        }
        positions.dedup();
        self.insert_sorted_positions(positions);
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

    pub fn contains_serialized_positions(bytes: &[u8], positions: &[u32; BLOOM_K as usize]) -> Result<bool> {
        match probe_serialized(bytes, positions) {
            Some(matches) => Ok(matches),
            // The checked decoder reads bodies in sequence and derives run cardinality from the intervals.
            None => Ok(Self::from_bytes(bytes)?.contains_positions(positions)),
        }
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

fn probe_serialized(bytes: &[u8], positions: &[u32; BLOOM_K as usize]) -> Option<bool> {
    const NO_RUN_COOKIE: u32 = 12346;
    const RUN_COOKIE: u16 = 12347;
    const NO_OFFSET_THRESHOLD: usize = 4;

    let (cookie, mut body) = bytes.split_first_chunk::<4>()?;
    let cookie = u32::from_le_bytes(*cookie);
    let (count, run_flags) = match cookie {
        NO_RUN_COOKIE => (
            u32::from_le_bytes(take(&mut body, 4)?.try_into().ok()?) as usize,
            &[][..],
        ),
        cookie if cookie as u16 == RUN_COOKIE => {
            let count = (cookie >> 16) as usize + 1;
            (count, take(&mut body, count.div_ceil(8))?)
        }
        _ => return None,
    };
    if count > usize::from(u16::MAX) + 1 {
        return None;
    }
    let descriptions = take(&mut body, 4 * count)?.as_chunks::<4>().0;
    let offsets = if run_flags.is_empty() || count >= NO_OFFSET_THRESHOLD {
        take(&mut body, 4 * count)?.as_chunks::<4>().0
    } else {
        &[]
    };

    let mut matched = 0;
    let mut previous_key = None;
    for (index, &[key_lo, key_hi, card_lo, card_hi]) in descriptions.iter().enumerate() {
        let key = u16::from_le_bytes([key_lo, key_hi]);
        if previous_key.replace(key).is_some_and(|previous| previous >= key) {
            return None;
        }
        if let Some(offset) = offsets.get(index)
            && u32::from_le_bytes(*offset) as usize != bytes.len() - body.len()
        {
            return None;
        }
        let cardinality = usize::from(u16::from_le_bytes([card_lo, card_hi])) + 1;
        let is_run = run_flags.get(index / 8).is_some_and(|&flags| flags & (1 << (index % 8)) != 0);
        let container = SerializedContainer::read(&mut body, cardinality, is_run)?;
        matched += positions.iter()
            .filter(|&&position| (position >> 16) as u16 == key && container.contains(position as u16))
            .count();
    }
    body.is_empty().then_some(matched == positions.len())
}

fn take<'a>(bytes: &mut &'a [u8], len: usize) -> Option<&'a [u8]> {
    let (prefix, suffix) = bytes.split_at_checked(len)?;
    *bytes = suffix;
    Some(prefix)
}

enum SerializedContainer<'a> {
    Array(&'a [[u8; 2]]),
    Bitmap(&'a [u8]),
    Run(&'a [[u8; 4]]),
}

impl<'a> SerializedContainer<'a> {
    fn read(body: &mut &'a [u8], cardinality: usize, is_run: bool) -> Option<Self> {
        if is_run {
            let count = usize::from(u16::from_le_bytes(take(body, 2)?.try_into().ok()?));
            if count == 0 {
                return None;
            }
            let runs = take(body, 4 * count)?.as_chunks::<4>().0;
            let mut previous_end: Option<u16> = None;
            let mut bits = 0;
            for &[start_lo, start_hi, len_lo, len_hi] in runs {
                let start = u16::from_le_bytes([start_lo, start_hi]);
                let length_minus_one = u16::from_le_bytes([len_lo, len_hi]);
                let end = start.checked_add(length_minus_one)?;
                if previous_end.replace(end).is_some_and(|previous| start <= previous.saturating_add(1)) {
                    return None;
                }
                bits += usize::from(length_minus_one) + 1;
            }
            (bits == cardinality).then_some(Self::Run(runs))
        } else if cardinality <= 4096 {
            let values = take(body, 2 * cardinality)?.as_chunks::<2>().0;
            values.windows(2)
                .all(|pair| u16::from_le_bytes(pair[0]) < u16::from_le_bytes(pair[1]))
                .then_some(Self::Array(values))
        } else {
            let bitmap = take(body, 8192)?;
            let bits: usize = bitmap.as_chunks::<8>().0.iter()
                .map(|word| u64::from_le_bytes(*word).count_ones() as usize)
                .sum();
            (bits == cardinality).then_some(Self::Bitmap(bitmap))
        }
    }

    fn contains(&self, low: u16) -> bool {
        match self {
            Self::Array(values) => values.binary_search_by_key(&low, |value| u16::from_le_bytes(*value)).is_ok(),
            Self::Bitmap(bytes) => bytes[usize::from(low) / 8] & (1 << (low % 8)) != 0,
            Self::Run(runs) => {
                let index = runs.partition_point(|&[lo, hi, _, _]| u16::from_le_bytes([lo, hi]) <= low);
                if index == 0 {
                    return false;
                }
                let [start_lo, start_hi, len_lo, len_hi] = runs[index - 1];
                low - u16::from_le_bytes([start_lo, start_hi]) <= u16::from_le_bytes([len_lo, len_hi])
            }
        }
    }
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

fn radix_pass(source: &[u32], target: &mut [u32], shift: u32) {
    const BUCKETS: usize = 1 << RADIX_DIGIT_BITS;
    let bucket = |position: u32| (position >> shift) as usize & (BUCKETS - 1);
    let mut offsets = [0usize; BUCKETS];
    for &position in source {
        offsets[bucket(position)] += 1;
    }
    let mut start = 0;
    for count in &mut offsets {
        let next = start + *count;
        *count = start;
        start = next;
    }
    for &position in source {
        let offset = &mut offsets[bucket(position)];
        target[*offset] = position;
        *offset += 1;
    }
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
    use arrow_schema::Field;
    use roaring::RoaringBitmap;
    use rtdb_types::{LogicalType, Value};
    use super::{BloomFilter, byte_hashes, probe_serialized};

    #[test]
    fn borrowed_probes_match_owned_probes_across_portable_layouts() {
        let mut bitmap: RoaringBitmap = [7, 65_535].into_iter()
            .chain((65_536..=73_728).step_by(2))
            .chain(131_072..196_608)
            .chain(196_623..196_628)
            .chain(196_631..196_635)
            .collect();
        let mut plain = vec![0];
        bitmap.serialize_into(&mut plain).unwrap();
        assert_eq!(&plain[1..5], &[58, 48, 0, 0]);
        assert!(bitmap.optimize());
        let mut mixed = vec![0];
        bitmap.serialize_into(&mut mixed).unwrap();
        assert_eq!(&mixed[1..6], &[59, 48, 3, 0, 0b1100]);

        for bytes in [&plain[1..], &mixed[1..]] {
            let owned = BloomFilter::from_bytes(bytes).unwrap();
            for (positions, expected) in [
                ([65_535, 73_728, 196_607, 196_623, 196_634], true),
                ([65_536; 5], true),
                ([65_537; 5], false),
                ([7, 73_728, 196_607, 196_622, 196_634], false),
                ([7, 73_728, 196_607, 196_629, 196_634], false),
                ([7, 73_728, 196_607, 196_623, 196_635], false),
                ([u32::MAX; 5], false),
            ] {
                assert_eq!(probe_serialized(bytes, &positions), Some(expected), "{positions:?}");
                assert_eq!(owned.contains_positions(&positions), expected);
            }
        }

        let empty = [58, 48, 0, 0, 0, 0, 0, 0];
        let run_without_offsets = [59, 48, 0, 0, 1, 3, 0, 4, 0, 1, 0, 2, 0, 4, 0];
        assert_eq!(probe_serialized(&empty, &[0; 5]), Some(false));
        assert_eq!(
            probe_serialized(&run_without_offsets, &[196_610, 196_611, 196_612, 196_613, 196_614]),
            Some(true),
        );
        assert_eq!(probe_serialized(&run_without_offsets, &[196_615; 5]), Some(false));
    }

    #[test]
    fn borrowed_probes_preserve_checked_errors_even_for_a_missing_key() {
        let bitmap: RoaringBitmap = (0..=8192).step_by(2).collect();
        let mut wrong_bit_count = Vec::new();
        bitmap.serialize_into(&mut wrong_bit_count).unwrap();
        wrong_bit_count[16] ^= 1;
        let duplicate_lows = [58, 48, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 16, 0, 0, 0, 5, 0, 5, 0];
        let duplicate_keys = [
            58, 48, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 24, 0, 0, 0, 26, 0, 0, 0, 1, 0, 2, 0,
        ];
        let overlapping_runs = [59, 48, 0, 0, 1, 0, 0, 4, 0, 2, 0, 2, 0, 2, 0, 4, 0, 1, 0];
        let overflowing_run = [59, 48, 0, 0, 1, 0, 0, 1, 0, 1, 0, 255, 255, 1, 0];
        let truncated_offset_table = [58, 48, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 16, 0, 0];
        let probe = [u32::MAX; 5];
        for bytes in [
            wrong_bit_count.as_slice(), &duplicate_lows, &duplicate_keys, &overlapping_runs, &overflowing_run,
            &truncated_offset_table,
        ] {
            assert_eq!(probe_serialized(bytes, &probe), None);
            assert_eq!(
                BloomFilter::contains_serialized_positions(bytes, &probe).unwrap_err(),
                BloomFilter::from_bytes(bytes).unwrap_err(),
            );
        }
    }

    #[test]
    fn noncanonical_headers_use_the_existing_sequential_decoder() {
        let mut bitmap = BloomFilter::default();
        bitmap.insert_sorted_positions([1, 65_538]);
        let mut wrong_offset = bitmap.to_bytes().unwrap();
        wrong_offset[16..20].copy_from_slice(&26u32.to_le_bytes());
        let wrong_run_cardinality = [59, 48, 0, 0, 1, 0, 0, 0, 0, 1, 0, 1, 0, 4, 0];
        for bytes in [wrong_offset.as_slice(), &wrong_run_cardinality] {
            let positions = [1; 5];
            assert_eq!(probe_serialized(bytes, &positions), None);
            assert!(BloomFilter::contains_serialized_positions(bytes, &positions).unwrap());
        }
    }

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
        use arrow_array::{Array, BinaryArray, StringArray, UnionArray};
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
    fn selected_byte_cells_produce_the_same_bloom_bytes_as_individual_inserts() {
        let field = Field::new("sku", arrow_schema::DataType::Utf8, true);
        for len in [128, 1024] {
            let array = arrow_array::StringArray::from_iter(
                (0..len).map(|row| (row % 7 != 0).then(|| format!("sku-{}", row % 500))),
            );
            let rows = (0..len).filter(|row| row % 4 != 0);
            let mut expected = BloomFilter::default();
            for row in rows.clone() {
                expected.insert_cell(&field, &array, row).unwrap();
            }
            let mut bulk = BloomFilter::default();
            assert!(bulk.insert_byte_cells(&array, rows));
            assert_eq!(bulk.to_bytes().unwrap(), expected.to_bytes().unwrap());
        }
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
