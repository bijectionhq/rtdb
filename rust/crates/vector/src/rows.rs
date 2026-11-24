use std::borrow::Cow;

use arrow_array::{BooleanArray, cast::AsArray};
use arrow_schema::DataType;
use rtdb_columnar::{ArrowCell, read_execution_value};
use rtdb_format::data::{EncodedEvent, EventRef, PutColumn};
use rtdb_types::{Anomaly, EventHeader, EventOp, Iid, ListView, Result, Value};
use sha2::{Digest, Sha256};

use crate::definition::VectorDefinition;

/// The digest of a row's partition values, in the definition's partition column order.
/// Rows of different partitions never share a cluster, so a partition filter applies before ranking.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Partition(pub [u8; 32]);

impl Partition {
    /// The digest of a definition without partition columns.
    pub fn whole() -> Self { Self::of(std::iter::empty::<Option<Cow<'_, Value>>>()).expect("an empty partition encodes") }

    /// Absent and null values share one encoding, as an equality filter can match neither.
    pub fn of<'a>(parts: impl IntoIterator<Item = Option<Cow<'a, Value>>>) -> Result<Self> {
        let mut hasher = Sha256::new();
        for part in parts {
            match part.as_deref() {
                None | Some(Value::Null | Value::Undefined) => hash_part(&mut hasher, 0, &[]),
                Some(Value::Bool(value)) => hash_part(&mut hasher, 1, &[u8::from(*value)]),
                Some(Value::I8(value)) => hash_integer(&mut hasher, i64::from(*value)),
                Some(Value::I16(value)) => hash_integer(&mut hasher, i64::from(*value)),
                Some(Value::I32(value)) => hash_integer(&mut hasher, i64::from(*value)),
                Some(Value::I64(value)) => hash_integer(&mut hasher, *value),
                Some(Value::U8(value)) => hash_integer(&mut hasher, i64::from(*value)),
                Some(Value::U16(value)) => hash_integer(&mut hasher, i64::from(*value)),
                Some(Value::U32(value)) => hash_integer(&mut hasher, i64::from(*value)),
                Some(Value::U64(value)) => match i64::try_from(*value) {
                    Ok(value) => hash_integer(&mut hasher, value),
                    Err(_) => hash_part(&mut hasher, 3, &value.to_be_bytes()),
                },
                Some(Value::Utf8(text)) => hash_part(&mut hasher, 4, text.as_bytes()),
                Some(Value::Binary(bytes) | Value::FixedSizeBinary(bytes)) => hash_part(&mut hasher, 5, bytes),
                Some(Value::Uuid(bytes)) => hash_part(&mut hasher, 6, bytes),
                Some(_) => return Err(Anomaly::incorrect("rtdb.vector/partition-type",
                    "A vector index partition column holds booleans, integers, text, binary or UUIDs")),
            }
        }
        Ok(Self(hasher.finalize().into()))
    }
}

fn hash_part(hasher: &mut Sha256, tag: u8, bytes: &[u8]) {
    hasher.update([tag]);
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn hash_integer(hasher: &mut Sha256, value: i64) {
    hash_part(hasher, 2, &value.to_be_bytes());
}

fn arrow_partition(event: &EventRef<'_>, columns: &[String]) -> Option<Partition> {
    let mut hasher = Sha256::new();
    for column in columns {
        match event.put_column(column) {
            Some(PutColumn::Stored(field, array, row)) => {
                hash_arrow_part(&mut hasher, ArrowCell { field, array, row })?;
            }
            Some(PutColumn::Absent) | None => hash_part(&mut hasher, 0, &[]),
        }
    }
    Some(Partition(hasher.finalize().into()))
}

fn hash_arrow_part(hasher: &mut Sha256, cell: ArrowCell<'_>) -> Option<()> {
    if !cell.field.metadata().is_empty() || cell.field.data_type() != cell.array.data_type()
        || cell.row >= cell.array.len()
    {
        return None;
    }
    let data_type = cell.field.data_type();
    match data_type {
        DataType::Null | DataType::Boolean | DataType::Int8 | DataType::Int16 | DataType::Int32
        | DataType::Int64 | DataType::UInt8 | DataType::UInt16 | DataType::UInt32
        | DataType::UInt64 | DataType::Utf8 | DataType::Binary => {}
        _ => return None,
    }
    if cell.array.is_null(cell.row) {
        hash_part(hasher, 0, &[]);
    } else {
        match data_type {
            DataType::Null => hash_part(hasher, 0, &[]),
            DataType::Boolean => {
                let array = cell.array.as_any().downcast_ref::<BooleanArray>()?;
                hash_part(hasher, 1, &[u8::from(array.value(cell.row))]);
            }
            DataType::Utf8 => hash_part(hasher, 4, cell.array.as_string::<i32>().value(cell.row).as_bytes()),
            DataType::Binary => hash_part(hasher, 5, cell.array.as_binary::<i32>().value(cell.row)),
            _ => hash_integer(hasher, cell.signed_integer()?),
        }
    }
    Some(())
}

/// One indexable put and the position of its unit vector in the staged components.
pub(crate) struct IndexedRow {
    pub(crate) iid: Iid,
    pub(crate) valid_from: i64,
    pub(crate) valid_to: i64,
    pub(crate) system_from: i64,
    pub(crate) partition: Partition,
    unit_offset: usize,
}

pub(crate) struct IndexedRows {
    dimensions: usize,
    rows: Vec<IndexedRow>,
    units: Vec<f32>,
}

impl IndexedRows {
    pub(crate) fn new(dimensions: usize) -> Self { Self { dimensions, rows: Vec::new(), units: Vec::new() } }

    pub(crate) fn dimensions(&self) -> usize { self.dimensions }
    pub(crate) fn rows(&self) -> &[IndexedRow] { &self.rows }
    pub(crate) fn unit(&self, row: &IndexedRow) -> &[f32] { &self.units[row.unit_offset..row.unit_offset + self.dimensions] }

    pub(crate) fn sort(&mut self) {
        self.rows.sort_by_key(|row| (row.partition, row.iid, row.system_from, row.valid_from, row.valid_to));
    }

    pub(crate) fn push_event(&mut self, event: &EventRef<'_>, definition: &VectorDefinition) -> Result<()> {
        let header = event.header();
        if header.op != EventOp::Put { return Ok(()); }
        let unit_offset = self.units.len();
        if !append_event_unit(event, &definition.column, self.dimensions, &mut self.units)? { return Ok(()); }
        let partition = match arrow_partition(event, &definition.partition) {
            Some(partition) => partition,
            None => definition.partition.iter().map(|column| event_value(event, column)).collect::<Result<Vec<_>>>()
                .and_then(Partition::of).inspect_err(|_| self.units.truncate(unit_offset))?,
        };
        self.push_row(header, partition, unit_offset);
        Ok(())
    }

    fn push_row(&mut self, header: EventHeader, partition: Partition, unit_offset: usize) {
        self.rows.push(IndexedRow {
            iid: header.iid, valid_from: header.valid_time.lower().0, valid_to: header.valid_time.upper().0,
            system_from: header.system_from.0, partition, unit_offset,
        });
    }

    #[cfg(test)]
    pub(crate) fn push_unit(&mut self, header: EventHeader, partition: Partition, unit: &[f32]) {
        assert_eq!(unit.len(), self.dimensions);
        let unit_offset = self.units.len();
        self.units.extend_from_slice(unit);
        self.push_row(header, partition, unit_offset);
    }
}

fn event_value<'a>(event: &'a EventRef<'_>, column: &str) -> Result<Option<Cow<'a, Value>>> {
    match event.put_column(column) {
        Some(PutColumn::Stored(field, array, row)) => Ok(Some(Cow::Owned(read_execution_value(field, array, row)?))),
        Some(PutColumn::Absent) | None => Ok(None),
    }
}

fn append_event_unit(event: &EventRef<'_>, column: &str, dimensions: usize, output: &mut Vec<f32>) -> Result<bool> {
    let Some(PutColumn::Stored(field, array, row)) = event.put_column(column) else { return Ok(false) };
    let cell = ArrowCell { field, array, row };
    if cell.is_null()? { return Ok(false); }
    if let Some((values, range)) = cell.float_list_span() {
        return Ok(range.len() == dimensions && append_unit(output, range.map(|index| values.float_component(index))));
    }
    let value = read_execution_value(field, array, row)?;
    Ok(append_value_unit(output, &value, dimensions))
}

/// The unit vector of a SQL list value, or none where its cosine distance is undefined.
pub(crate) fn value_unit(value: &Value, dimensions: usize) -> Option<Vec<f32>> {
    let mut output = Vec::new();
    append_value_unit(&mut output, value, dimensions).then_some(output)
}

fn append_value_unit(output: &mut Vec<f32>, value: &Value, dimensions: usize) -> bool {
    let Some(view) = value.list_view() else { return false };
    view.len() == dimensions && append_unit(output, (0..view.len()).map(|index| list_component(view, index)))
}

fn list_component(view: ListView<'_>, index: usize) -> Option<f64> {
    if let Some(component) = view.float_component(index) { return Some(component); }
    Some(match view.get(index)?.as_ref() {
        Value::I8(value) => f64::from(*value), Value::I16(value) => f64::from(*value), Value::I32(value) => f64::from(*value),
        Value::I64(value) => *value as f64, Value::U8(value) => f64::from(*value), Value::U16(value) => f64::from(*value),
        Value::U32(value) => f64::from(*value), Value::U64(value) => *value as f64,
        Value::F32(value) => f64::from(*value), Value::F64(value) => *value,
        _ => return None,
    })
}

pub(crate) fn unit_of(components: &[f32]) -> Option<Vec<f32>> {
    let mut output = Vec::new();
    append_unit(&mut output, components.iter().map(|component| Some(f64::from(*component)))).then_some(output)
}

/// Normalizes in f64 in component order, so the stored unit vector is a deterministic function of the input.
fn append_unit(output: &mut Vec<f32>, components: impl Iterator<Item = Option<f64>> + Clone) -> bool {
    let norm = components.clone().try_fold(0.0, |sum, component| {
        let component = component.filter(|value| value.is_finite())?;
        Some(sum + component * component)
    }).map(f64::sqrt);
    let Some(norm) = norm.filter(|norm| norm.is_finite() && *norm > 0.0) else { return false };
    output.extend(components.map(|component| (component.expect("component checked during normalization") / norm) as f32));
    true
}

/// Cosine distance between unit vectors, accumulated in f64 in component order.
pub(crate) fn unit_distance(left: &[f32], right: &[f32]) -> f64 {
    1.0 - left.iter().zip(right).map(|(left, right)| f64::from(*left) * f64::from(*right)).sum::<f64>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_partition_uses_the_execution_value_digest() -> Result<()> {
        use arrow_array::{Array, BinaryArray, StringArray, UInt8Array};

        let text = StringArray::from(vec![None, Some("key")]);
        let binary = BinaryArray::from(vec![Some(&b"\0a"[..]), None]);
        let integer = UInt8Array::from(vec![255, 128]);
        let flag = BooleanArray::from(vec![true, false]);
        for row in 0..2 {
            let mut hasher = Sha256::new();
            let arrays: [&dyn Array; 4] = [&text, &binary, &integer, &flag];
            let mut parts = Vec::new();
            for array in arrays {
                let field = arrow_schema::Field::new("key", array.data_type().clone(), true);
                hash_arrow_part(&mut hasher, ArrowCell { field: &field, array, row }).unwrap();
                parts.push(Some(Cow::Owned(read_execution_value(&field, array, row)?)));
            }
            assert_eq!(Partition(hasher.finalize().into()), Partition::of(parts)?);
        }
        Ok(())
    }

    #[test]
    fn normalization_keeps_f64_precision_until_the_final_components() {
        let components = [1e100, -2e100, -0.0, 9_007_199_254_740_993i64 as f64];
        let value = Value::list(vec![Value::F64(components[0]), Value::F64(components[1]), Value::F32(-0.0), Value::I64(9_007_199_254_740_993)]);
        let norm = components.iter().map(|value| value * value).sum::<f64>().sqrt();
        let expected = components.map(|component| ((component / norm) as f32).to_bits());
        let unit = value_unit(&value, 4).unwrap();
        assert_eq!(unit.into_iter().map(f32::to_bits).collect::<Vec<_>>(), expected);
    }

    #[test]
    fn undefined_vectors_leave_staged_components_untouched() {
        let mut output = vec![1.0];
        for components in [[Some(1.0), None], [Some(f64::NAN), Some(1.0)], [Some(0.0), Some(-0.0)], [Some(f64::MAX), Some(1.0)]] {
            assert!(!append_unit(&mut output, components.into_iter()));
            assert_eq!(output, [1.0]);
        }
        assert!(append_unit(&mut output, [Some(0.0), Some(1.0)].into_iter()));
        assert_eq!(output, [1.0, 0.0, 1.0]);
    }
}
