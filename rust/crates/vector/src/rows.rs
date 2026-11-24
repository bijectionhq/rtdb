use std::borrow::Cow;

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
            let (tag, bytes): (u8, Cow<'_, [u8]>) = match part.as_deref() {
                None | Some(Value::Null | Value::Undefined) => (0, Cow::Borrowed(&[])),
                Some(Value::Bool(value)) => (1, Cow::Owned(vec![u8::from(*value)])),
                Some(Value::I8(value)) => (2, Cow::Owned(i64::from(*value).to_be_bytes().to_vec())),
                Some(Value::I16(value)) => (2, Cow::Owned(i64::from(*value).to_be_bytes().to_vec())),
                Some(Value::I32(value)) => (2, Cow::Owned(i64::from(*value).to_be_bytes().to_vec())),
                Some(Value::I64(value)) => (2, Cow::Owned(value.to_be_bytes().to_vec())),
                Some(Value::U8(value)) => (2, Cow::Owned(i64::from(*value).to_be_bytes().to_vec())),
                Some(Value::U16(value)) => (2, Cow::Owned(i64::from(*value).to_be_bytes().to_vec())),
                Some(Value::U32(value)) => (2, Cow::Owned(i64::from(*value).to_be_bytes().to_vec())),
                Some(Value::U64(value)) => match i64::try_from(*value) {
                    Ok(value) => (2, Cow::Owned(value.to_be_bytes().to_vec())),
                    Err(_) => (3, Cow::Owned(value.to_be_bytes().to_vec())),
                },
                Some(Value::Utf8(text)) => (4, Cow::Borrowed(text.as_bytes())),
                Some(Value::Binary(bytes) | Value::FixedSizeBinary(bytes)) => (5, Cow::Borrowed(bytes.as_slice())),
                Some(Value::Uuid(bytes)) => (6, Cow::Borrowed(bytes.as_slice())),
                Some(_) => return Err(Anomaly::incorrect("rtdb.vector/partition-type",
                    "A vector index partition column holds booleans, integers, text, binary or UUIDs")),
            };
            hasher.update([tag]);
            hasher.update((bytes.len() as u64).to_be_bytes());
            hasher.update(&bytes);
        }
        Ok(Self(hasher.finalize().into()))
    }
}

/// One indexable put: its identity, temporal extent, partition and unit vector.
pub(crate) struct IndexedRow {
    pub(crate) iid: Iid,
    pub(crate) valid_from: i64,
    pub(crate) valid_to: i64,
    pub(crate) system_from: i64,
    pub(crate) partition: Partition,
    pub(crate) unit: Vec<f32>,
}

/// The row a put contributes to a generation, or none.
/// Deletes and erases contribute nothing: resolution at the read cut excludes the versions they end.
/// A vector without a defined cosine distance (wrong dimension, a non-finite component, a zero norm) is left out.
pub(crate) fn indexed_row(event: &EventRef<'_>, definition: &VectorDefinition) -> Result<Option<IndexedRow>> {
    let header = event.header();
    if header.op != EventOp::Put { return Ok(None); }
    let Some(unit) = event_unit(event, &definition.column, definition.dimensions)? else { return Ok(None) };
    let partition = Partition::of(definition.partition.iter().map(|column| event_value(event, column)).collect::<Result<Vec<_>>>()?)?;
    Ok(Some(row(header, partition, unit)))
}

fn row(header: EventHeader, partition: Partition, unit: Vec<f32>) -> IndexedRow {
    IndexedRow {
        iid: header.iid, valid_from: header.valid_time.lower().0, valid_to: header.valid_time.upper().0,
        system_from: header.system_from.0, partition, unit,
    }
}

fn event_value<'a>(event: &'a EventRef<'_>, column: &str) -> Result<Option<Cow<'a, Value>>> {
    match event.put_column(column) {
        Some(PutColumn::Stored(field, array, row)) => Ok(Some(Cow::Owned(read_execution_value(field, array, row)?))),
        Some(PutColumn::Absent) | None => Ok(None),
    }
}

fn event_unit(event: &EventRef<'_>, column: &str, dimensions: usize) -> Result<Option<Vec<f32>>> {
    let Some(PutColumn::Stored(field, array, row)) = event.put_column(column) else { return Ok(None) };
    let cell = ArrowCell { field, array, row };
    if cell.is_null()? { return Ok(None); }
    if let Some((values, range)) = cell.float_list_span() {
        if range.len() != dimensions { return Ok(None); }
        return Ok(unit((0..range.len()).map(|index| values.float_component(range.start + index))));
    }
    let value = read_execution_value(field, array, row)?;
    Ok(value_unit(&value, dimensions))
}

/// The unit vector of a SQL list value, or none where its cosine distance is undefined.
pub(crate) fn value_unit(value: &Value, dimensions: usize) -> Option<Vec<f32>> {
    let view = value.list_view()?;
    if view.len() != dimensions { return None; }
    unit((0..view.len()).map(|index| list_component(view, index)))
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
    unit(components.iter().map(|component| Some(f64::from(*component))))
}

/// Normalizes in f64 in component order, so the stored unit vector is a deterministic function of the input.
fn unit(components: impl Iterator<Item = Option<f64>>) -> Option<Vec<f32>> {
    let components = components.collect::<Option<Vec<f64>>>()?;
    if components.is_empty() || components.iter().any(|component| !component.is_finite()) { return None; }
    let norm = components.iter().map(|component| component * component).sum::<f64>().sqrt();
    if !(norm.is_finite() && norm > 0.0) { return None; }
    Some(components.into_iter().map(|component| (component / norm) as f32).collect())
}

/// Cosine distance between unit vectors, accumulated in f64 in component order.
pub(crate) fn unit_distance(left: &[f32], right: &[f32]) -> f64 {
    1.0 - left.iter().zip(right).map(|(left, right)| f64::from(*left) * f64::from(*right)).sum::<f64>()
}
