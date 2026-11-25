use std::collections::BTreeMap;
use std::ops::Range;

use arrow_array::{Array, RecordBatch, StructArray, UnionArray};
use arrow_schema::{DataType, Field};
use rtdb_columnar::ColumnPresence;
use rtdb_types::{Anomaly, MemoryReservation, Result};

pub const DEFAULT_REGISTER_COUNT: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hll { bytes: Vec<u8> }

impl Hll {
    pub fn new(register_count: usize) -> Result<Self> {
        if !register_count.is_power_of_two() || !(2..=(1 << 20)).contains(&register_count) {
            return Err(invalid("HLL register count must be a power of two between 2 and 1048576"));
        }
        Ok(Self { bytes: vec![0; register_count * 4] })
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        let count = bytes.len() / 4;
        if bytes.len() % 4 != 0 || !count.is_power_of_two() || !(2..=(1 << 20)).contains(&count) {
            return Err(invalid("HLL must contain a power-of-two array of big-endian i32 registers"));
        }
        for register in bytes.chunks_exact(4) {
            let value = i32::from_be_bytes(register.try_into().map_err(invalid)?);
            if !(0..=33).contains(&value) { return Err(invalid("HLL register is outside its hash-width range")); }
        }
        Ok(Self { bytes })
    }

    pub fn as_bytes(&self) -> &[u8] { &self.bytes }
    pub fn into_bytes(self) -> Vec<u8> { self.bytes }

    pub fn insert_hash(&mut self, hash: i32) {
        let count = self.bytes.len() / 4;
        let bits = count.trailing_zeros();
        let hash = hash as u32;
        let index = ((hash >> (32 - bits)) as usize) & (count - 1);
        let remainder = hash & ((1 << (32 - bits)) - 1);
        let rank = (remainder.leading_zeros() + 1 - bits) as i32;
        let offset = index * 4;
        let current = i32::from_be_bytes(self.bytes[offset..offset + 4].try_into().expect("one HLL register"));
        self.bytes[offset..offset + 4].copy_from_slice(&current.max(rank).to_be_bytes());
    }

    pub fn estimate(&self) -> f64 {
        let count = (self.bytes.len() / 4) as f64;
        let mut accumulator = 0.0;
        let mut zeros = 0usize;
        for bytes in self.bytes.chunks_exact(4) {
            let register = i32::from_be_bytes(bytes.try_into().expect("one HLL register"));
            accumulator += 2.0f64.powi(-register);
            zeros += usize::from(register == 0);
        }
        let alpha = 0.7213 / (1.0 + 1.079 / count);
        let estimate = alpha * count.powi(2) / accumulator;
        if estimate <= 2.5 * count {
            return if zeros == 0 { estimate } else { count * (count / zeros as f64).ln() };
        }
        // HyperLogLog.kt narrows 0xffffffff to a signed Int before this correction.
        let largest_unsigned_int = -1.0;
        if estimate > largest_unsigned_int / 30.0 {
            return 4_294_967_296.0 * (1.0 - estimate / largest_unsigned_int).ln();
        }
        estimate
    }

    pub fn combined(&self, other: &Self) -> Result<Self> { Self::from_bytes(combine(&self.bytes, &other.bytes)?) }
}

impl Default for Hll {
    fn default() -> Self { Self { bytes: vec![0; DEFAULT_REGISTER_COUNT * 4] } }
}

pub fn combine(left: &[u8], right: &[u8]) -> Result<Vec<u8>> {
    if left.len() != right.len() || left.len() % 4 != 0 { return Err(invalid("HLL buffers must have the same register count")); }
    let mut combined = Vec::with_capacity(left.len());
    for (left, right) in left.chunks_exact(4).zip(right.chunks_exact(4)) {
        let left = i32::from_be_bytes(left.try_into().map_err(invalid)?);
        let right = i32::from_be_bytes(right.try_into().map_err(invalid)?);
        combined.extend_from_slice(&left.max(right).to_be_bytes());
    }
    Ok(combined)
}

pub fn from_column(field: &Field, array: &dyn Array) -> Result<Vec<u8>> {
    let mut hll = Hll::default();
    let reader = ColumnPresence::new(field)?;
    if !reader.is_bottom() {
        for row in 0..array.len() {
            if let Some(hash) = cell_hash(&reader, field, array, row)? { hll.insert_hash(hash); }
        }
    }
    Ok(hll.into_bytes())
}

pub fn from_event_batch(batch: &RecordBatch, rows: Range<usize>) -> Result<BTreeMap<String, Vec<u8>>> {
    event_sketches([(batch, rows)], |_| Ok(()))
}

pub fn from_event_batches<'a>(
    batches: impl IntoIterator<Item = (&'a RecordBatch, Range<usize>)>,
    reservation: &mut MemoryReservation,
) -> Result<BTreeMap<String, Vec<u8>>> {
    event_sketches(batches, |name| {
        let bytes = reservation.bytes().checked_add(DEFAULT_REGISTER_COUNT * 4 + 256)
            .and_then(|bytes| bytes.checked_add(name.len()))
            .ok_or_else(|| invalid("HLL allocation exceeds addressable memory"))?;
        reservation.grow_to(bytes)
    })
}

fn event_sketches<'a>(
    batches: impl IntoIterator<Item = (&'a RecordBatch, Range<usize>)>,
    mut admit_column: impl FnMut(&str) -> Result<()>,
) -> Result<BTreeMap<String, Vec<u8>>> {
    let mut sketches = BTreeMap::<String, (usize, Hll)>::new();
    for (batch, rows) in batches {
        fold_event_batch(batch, rows, &mut sketches, &mut admit_column)?;
    }
    Ok(sketches.into_iter().map(|(name, (_, sketch))| (name, sketch.into_bytes())).collect())
}

fn fold_event_batch(
    batch: &RecordBatch,
    rows: Range<usize>,
    sketches: &mut BTreeMap<String, (usize, Hll)>,
    admit_column: &mut impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    if rows.start > rows.end || rows.end > batch.num_rows() { return Err(invalid("HLL event range exceeds batch length")); }
    let schema = batch.schema();
    let op_index = schema.index_of("op").map_err(invalid)?;
    let op = batch.column(op_index).as_any().downcast_ref::<UnionArray>().ok_or_else(|| invalid("Event operations must be a union"))?;
    let DataType::Union(fields, _) = op.data_type() else { return Err(invalid("Event operation union has no fields")); };
    let Some((put_id, put_field)) = fields.iter().find(|(_, field)| field.name() == "put") else { return Ok(()); };
    let put = op.child(put_id).as_any().downcast_ref::<StructArray>().ok_or_else(|| invalid("Put operations must contain a struct"))?;
    let DataType::Struct(fields) = put_field.data_type() else { return Err(invalid("Put schema must be a struct")); };

    // Every duplicate child still runs its read and hash checks in physical order.
    // Only the last child contributes registers.
    for (index, field) in fields.iter().enumerate() {
        if let Some((last, _)) = sketches.get_mut(field.name()) {
            *last = index;
        } else {
            admit_column(field.name())?;
            sketches.insert(field.name().clone(), (index, Hll::default()));
        }
    }
    for (index, field) in fields.iter().enumerate() {
        let reader = ColumnPresence::new(field)?;
        if reader.is_bottom() { continue; }
        let (last, hll) = sketches.get_mut(field.name()).expect("PUT field has a sketch");
        for row in rows.clone().filter(|&row| op.type_id(row) == put_id) {
            if let Some(hash) = cell_hash(&reader, field, put.column(index).as_ref(), op.value_offset(row))? {
                if *last == index { hll.insert_hash(hash); }
            }
        }
    }
    Ok(())
}

fn cell_hash(reader: &ColumnPresence<'_>, field: &Field, array: &dyn Array, row: usize) -> Result<Option<i32>> {
    if reader.is_present(array, row)? {
        return rtdb_columnar::hash_cell(field, array, row, 0).map(Some);
    }
    Ok(None)
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("rtdb.format/invalid-hll", error.to_string())
}

#[test]
fn union_and_run_end_nulls_do_not_add_hll_registers() {
    use rtdb_types::{LogicalType, RunEndWidth, Value};

    let values = [Value::Null, Value::Utf8("hello".into()), Value::Utf8("hello".into()), Value::I64(7), Value::Null];
    let ty = LogicalType::join_all(values.iter().map(Value::logical_type).collect::<Vec<_>>().iter());
    let mut expected = Hll::default();
    for value in values.iter().filter(|value| **value != Value::Null) {
        expected.insert_hash(rtdb_columnar::hash_value(value, 0).unwrap());
    }
    let encoded = ty.clone().run_end_encoded(RunEndWidth::I32).unwrap();
    for ty in [ty, encoded] {
        let field = rtdb_columnar::to_field("v", &ty).unwrap();
        let array = rtdb_columnar::build_array(&ty, &values).unwrap();
        assert_eq!(from_column(&field, array.as_ref()).unwrap(), expected.bytes);
    }
}

#[test]
fn event_chunks_keep_last_named_child_and_fold_only_selected_puts() {
    use std::sync::Arc;
    use arrow_array::{ArrayRef, Int64Array, NullArray};
    use arrow_schema::{Schema, UnionFields};
    use rtdb_types::{MemoryBudget, Value};

    let batch = |fields: Vec<Field>, columns: Vec<ArrayRef>| {
        let document = rtdb_columnar::build_struct_array(fields.into(), &columns, 2, None).unwrap();
        let legs = UnionFields::try_new([0, 1], [
            Field::new("delete", DataType::Null, true),
            Field::new("put", document.data_type().clone(), false),
        ]).unwrap();
        let op = UnionArray::try_new(legs, vec![1, 0, 1].into(), Some(vec![1, 0, 0].into()),
            vec![Arc::new(NullArray::new(1)), document]).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("op", op.data_type().clone(), false)]));
        RecordBatch::try_new(schema, vec![Arc::new(op)]).unwrap()
    };
    let ints = |values| Arc::new(Int64Array::from(values)) as ArrayRef;
    let v = Field::new("v", DataType::Int64, true);
    let bottom = Field::new("bottom", DataType::Null, false);
    let first = batch(vec![v.clone(), v.clone(), bottom], vec![
        ints(vec![Some(10), Some(11)]), ints(vec![Some(23), Some(29)]), Arc::new(NullArray::new(2)),
    ]);
    let empty_range = batch(vec![Field::new("empty", DataType::Int64, true)], vec![ints(vec![Some(5), None])]);
    let last = batch(vec![v], vec![ints(vec![None, Some(31)])]);

    let memory = MemoryBudget::new(16 * 1024);
    let mut reservation = memory.reserve(0).unwrap();
    let sketches = from_event_batches([(&first, 1..3), (&empty_range, 0..0), (&last, 0..3)],
        Arc::get_mut(&mut reservation).unwrap()).unwrap();
    let mut expected = Hll::default();
    for value in [23, 31] {
        expected.insert_hash(rtdb_columnar::hash_value(&Value::I64(value), 0).unwrap());
    }
    assert_eq!(sketches, BTreeMap::from([
        ("bottom".to_owned(), Hll::default().into_bytes()),
        ("empty".to_owned(), Hll::default().into_bytes()),
        ("v".to_owned(), expected.into_bytes()),
    ]));
    assert_eq!(from_event_batch(&first, 0..4).unwrap_err().code, "rtdb.format/invalid-hll");
    assert!(reservation.bytes() >= sketches.values().map(Vec::len).sum::<usize>());
    drop(sketches);
    drop(reservation);
    assert_eq!(memory.used_bytes(), 0);
}
