use std::{borrow::Cow, sync::Arc};

use arrow_array::{BooleanArray, FixedSizeBinaryArray, cast::AsArray};
use arrow_schema::{DataType, UnionMode};
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation, estimate_read_value_bytes, read_execution_value};
use rtdb_format::data::{EncodedEvent, EventRef, PutColumn};
use rtdb_types::{Anomaly, EventHeader, EventOp, Iid, ListValue, ListView, NumericSpan, Result, Value};
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
            hash_value_part(&mut hasher, part.as_deref())?;
        }
        Ok(Self(hasher.finalize().into()))
    }
}

fn hash_value_part(hasher: &mut Sha256, part: Option<&Value>) -> Result<()> {
    match part {
        None | Some(Value::Null | Value::Undefined) => hash_part(hasher, 0, &[]),
        Some(Value::Bool(value)) => hash_part(hasher, 1, &[u8::from(*value)]),
        Some(Value::I8(value)) => hash_integer(hasher, i64::from(*value)),
        Some(Value::I16(value)) => hash_integer(hasher, i64::from(*value)),
        Some(Value::I32(value)) => hash_integer(hasher, i64::from(*value)),
        Some(Value::I64(value)) => hash_integer(hasher, *value),
        Some(Value::U8(value)) => hash_integer(hasher, i64::from(*value)),
        Some(Value::U16(value)) => hash_integer(hasher, i64::from(*value)),
        Some(Value::U32(value)) => hash_integer(hasher, i64::from(*value)),
        Some(Value::U64(value)) => match i64::try_from(*value) {
            Ok(value) => hash_integer(hasher, value),
            Err(_) => hash_part(hasher, 3, &value.to_be_bytes()),
        },
        Some(Value::Utf8(text)) => hash_part(hasher, 4, text.as_bytes()),
        Some(Value::Binary(bytes) | Value::FixedSizeBinary(bytes)) => hash_part(hasher, 5, bytes),
        Some(Value::Uuid(bytes)) => hash_part(hasher, 6, bytes),
        Some(_) => return Err(Anomaly::incorrect("rtdb.vector/partition-type",
            "A vector index partition column holds booleans, integers, text, binary or UUIDs")),
    }
    Ok(())
}

fn hash_part(hasher: &mut Sha256, tag: u8, bytes: &[u8]) {
    hasher.update([tag]);
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn hash_integer(hasher: &mut Sha256, value: i64) {
    hash_part(hasher, 2, &value.to_be_bytes());
}

fn arrow_partition(event: &EventRef<'_>, columns: &[String], memory: &Arc<MemoryBudget>) -> Result<Partition> {
    let mut hasher = Sha256::new();
    let mut type_error = None;
    for column in columns {
        let Some(PutColumn::Stored(field, array, row)) = event.put_column(column) else {
            hash_part(&mut hasher, 0, &[]);
            continue;
        };
        if hash_arrow_part(&mut hasher, ArrowCell { field, array, row }).is_none() {
            let _scratch = reserve_decode(field, array, row, memory)?;
            let value = read_execution_value(field, array, row)?;
            if let Err(error) = hash_value_part(&mut hasher, Some(&value)) {
                // The former full-row decode reported later decode errors before a digest type error.
                type_error.get_or_insert(error);
            }
        }
    }
    match type_error {
        Some(error) => Err(error),
        None => Ok(Partition(hasher.finalize().into())),
    }
}

fn reserve_decode(field: &arrow_schema::Field, array: &dyn arrow_array::Array, row: usize, memory: &Arc<MemoryBudget>)
    -> Result<Arc<MemoryReservation>>
{
    memory.reserve(estimate_read_value_bytes(field, array, row)?)
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
        | DataType::UInt64 | DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_) => {}
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
            DataType::FixedSizeBinary(_) => {
                let array = cell.array.as_any().downcast_ref::<FixedSizeBinaryArray>()?;
                hash_part(hasher, 5, array.value(cell.row));
            }
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

#[derive(Clone, Copy)]
pub(crate) enum ClusterRows<'a> {
    Ordered(&'a [IndexedRow]),
    Selected(&'a [IndexedRow], &'a [usize]),
}

impl<'a> ClusterRows<'a> {
    pub(crate) fn len(self) -> usize {
        match self {
            Self::Ordered(rows) => rows.len(),
            Self::Selected(_, order) => order.len(),
        }
    }

    pub(crate) fn iter(self) -> impl Iterator<Item = &'a IndexedRow> {
        (0..self.len()).map(move |index| match self {
            Self::Ordered(rows) => &rows[index],
            Self::Selected(rows, order) => &rows[order[index]],
        })
    }
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

    pub(crate) fn unit_spans<'a>(&'a self, selected: impl Iterator<Item = &'a IndexedRow> + 'a)
        -> impl Iterator<Item = &'a [f32]> + 'a
    {
        let mut selected = selected.peekable();
        std::iter::from_fn(move || {
            let start = selected.next()?.unit_offset;
            let mut end = start + self.dimensions;
            while selected.next_if(|row| row.unit_offset == end).is_some() {
                end += self.dimensions;
            }
            Some(&self.units[start..end])
        })
    }

    pub(crate) fn sort(&mut self) {
        self.rows.sort_by_key(|row| (row.partition, row.iid, row.system_from, row.valid_from, row.valid_to));
    }

    pub(crate) fn push_event(&mut self, event: &EventRef<'_>, definition: &VectorDefinition, staging: &mut MemoryReservation) -> Result<()> {
        let unit_offset = self.units.len();
        let dimensions = self.dimensions;
        let memory = Arc::clone(staging.budget());
        let Some(cell) = event_vector(event, definition) else { return Ok(()) };
        if with_cell_unit(cell, dimensions, &memory, |component| {
            admit_staging_row(&mut self.rows, &mut self.units, dimensions, staging)?;
            self.units.extend((0..dimensions).map(component));
            Ok(())
        })?.is_some() {
            let partition = arrow_partition(event, &definition.partition, &memory)
                .inspect_err(|_| self.units.truncate(unit_offset))?;
            self.push_row(event.header(), partition, unit_offset);
        }
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

fn admit_staging_row(rows: &mut Vec<IndexedRow>, units: &mut Vec<f32>, dimensions: usize, staging: &mut MemoryReservation) -> Result<()> {
    let grow = |capacity: usize, needed: usize| {
        if capacity >= needed { capacity } else { capacity.saturating_mul(2).max(needed) }
    };
    let row_capacity = grow(rows.capacity(), rows.len().saturating_add(1));
    let unit_capacity = grow(units.capacity(), units.len().saturating_add(dimensions));
    if row_capacity == rows.capacity() && unit_capacity == units.capacity() { return Ok(()); }
    staging.grow_to(row_capacity.saturating_mul(size_of::<IndexedRow>())
        .saturating_add(unit_capacity.saturating_mul(size_of::<f32>())))?;
    let allocation_error = |error: std::collections::TryReserveError| {
        Anomaly::fault("rtdb.vector/staging", error.to_string())
    };
    rows.try_reserve_exact(row_capacity - rows.len()).map_err(allocation_error)?;
    units.try_reserve_exact(unit_capacity - units.len()).map_err(allocation_error)?;
    Ok(())
}

fn event_vector<'a>(event: &'a EventRef<'_>, definition: &VectorDefinition) -> Option<ArrowCell<'a>> {
    if event.header().op != EventOp::Put { return None; }
    let Some(PutColumn::Stored(field, array, row)) = event.put_column(&definition.column) else { return None };
    Some(ArrowCell { field, array, row })
}

pub(crate) fn frontier_distance(
    event: &EventRef<'_>, definition: &VectorDefinition, probe: &[f32], memory: &Arc<MemoryBudget>,
    eligible: impl FnOnce(Partition) -> bool,
) -> Result<Option<f64>> {
    let Some(cell) = event_vector(event, definition) else { return Ok(None) };
    with_cell_unit(cell, definition.dimensions, memory, |component| {
        let partition = arrow_partition(event, &definition.partition, memory)?;
        Ok(eligible(partition).then(|| {
            1.0 - probe.iter().enumerate()
                .map(|(index, &left)| f64::from(left) * f64::from(component(index)))
                .sum::<f64>()
        }))
    }).map(Option::flatten)
}

fn with_cell_unit<T>(
    cell: ArrowCell<'_>, dimensions: usize, memory: &Arc<MemoryBudget>,
    consume: impl FnOnce(&dyn Fn(usize) -> f32) -> Result<T>,
) -> Result<Option<T>> {
    if cell.is_null()? { return Ok(None); }
    if let Some((values, range)) = cell.float_list_span() {
        if range.len() != dimensions { return Ok(None); }
        return with_normalized(dimensions, |index| values.float_component(range.start + index), consume);
    }
    if let Some(span) = numeric_training_span(cell) {
        if span.len() != dimensions { return Ok(None); }
        return with_normalized(dimensions, |index| span.get(index)?.signed().as_f64(), consume);
    }
    let _scratch = reserve_decode(cell.field, cell.array, cell.row, memory)?;
    let value = read_execution_value(cell.field, cell.array, cell.row)?;
    let Some(view) = value.list_view().filter(|view| view.len() == dimensions) else { return Ok(None) };
    with_normalized(dimensions, |index| list_component(view, index), consume)
}

fn with_normalized<T>(
    dimensions: usize, component: impl Fn(usize) -> Option<f64>,
    consume: impl FnOnce(&dyn Fn(usize) -> f32) -> Result<T>,
) -> Result<Option<T>> {
    let Some(norm) = component_norm((0..dimensions).map(&component)) else { return Ok(None) };
    consume(&|index| normalized(component(index), norm)).map(Some)
}

fn numeric_training_span(cell: ArrowCell<'_>) -> Option<NumericSpan<'_>> {
    if !cell.field.metadata().is_empty() { return None; }
    let element = match cell.field.data_type() {
        DataType::List(element) => element,
        DataType::FixedSizeList(element, width) => {
            i32::try_from(cell.row.checked_mul(usize::try_from(*width).ok()?)?).ok()?;
            element
        }
        _ => return None,
    };
    let component = |ty: &DataType| matches!(ty,
        DataType::Null | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        | DataType::Float32 | DataType::Float64);
    let supported = match element.data_type() {
        DataType::Union(fields, UnionMode::Dense) =>
            fields.iter().all(|(_, field)| component(field.data_type())),
        ty => component(ty),
    };
    if supported && ListValue::supports_numeric_arrow(element) {
        cell.numeric_list_span()
    } else {
        None
    }
}

/// A scoring probe and its retained normalized components.
pub struct UnitProbe {
    values: Vec<f32>,
    _allocation: Arc<MemoryReservation>,
}

impl UnitProbe {
    pub(crate) fn values(&self) -> &[f32] { &self.values }

    pub fn from_slice(components: &[f32], memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        Self::prepare(components.len(), |index| Some(f64::from(components[index])), memory)
    }

    pub(crate) fn from_value(value: &Value, dimensions: usize, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        let Some(view) = value.list_view().filter(|view| view.len() == dimensions) else { return Ok(None) };
        Self::prepare(dimensions, |index| list_component(view, index), memory)
    }

    fn prepare(dimensions: usize, component: impl Fn(usize) -> Option<f64>, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        with_normalized(dimensions, component, |component| {
            let allocation = memory.reserve(dimensions.saturating_mul(size_of::<f32>()))?;
            let values = (0..dimensions).map(component).collect();
            Ok(Self { values, _allocation: allocation })
        })
    }
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

fn component_norm(mut components: impl Iterator<Item = Option<f64>>) -> Option<f64> {
    components.try_fold(0.0, |sum, component| {
        let component = component.filter(|value| value.is_finite())?;
        Some(sum + component * component)
    }).map(f64::sqrt).filter(|norm| norm.is_finite() && *norm > 0.0)
}

fn normalized(component: Option<f64>, norm: f64) -> f32 {
    (component.expect("component checked during normalization") / norm) as f32
}

/// Cosine distance between unit vectors, accumulated in f64 in component order.
pub(crate) fn unit_distance(left: &[f32], right: &[f32]) -> f64 {
    1.0 - left.iter().zip(right).map(|(left, right)| f64::from(*left) * f64::from(*right)).sum::<f64>()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn append_unit_with(output: &mut Vec<f32>, components: impl Iterator<Item = Option<f64>> + Clone,
        admit: impl FnOnce(&mut Vec<f32>) -> Result<()>) -> Result<bool> {
        let Some(norm) = component_norm(components.clone()) else { return Ok(false) };
        admit(output)?;
        output.extend(components.map(|component| normalized(component, norm)));
        Ok(true)
    }

    #[test]
    fn generation_admits_components_and_row_capacity_before_growth() -> Result<()> {
        use rtdb_types::{Interval, MemoryBudget, Timestamp};

        for dimensions in [2, 1024] {
            let page_bytes = 4096;
            let vector_bytes = size_of::<IndexedRow>() + dimensions * size_of::<f32>();
            let budget = MemoryBudget::new(page_bytes + vector_bytes);
            let _page = budget.reserve(page_bytes)?;
            let mut staging = budget.reserve(0)?;
            let mut arena = IndexedRows::new(dimensions);
            let unit = std::iter::repeat_n(Some(1.0), dimensions);
            let admit = |rows: &mut Vec<IndexedRow>, units: &mut Vec<f32>, staging: &mut std::sync::Arc<MemoryReservation>| {
                append_unit_with(units, unit.clone(), |units| {
                    admit_staging_row(rows, units, dimensions, std::sync::Arc::get_mut(staging).unwrap())
                })
            };
            assert!(admit(&mut arena.rows, &mut arena.units, &mut staging)?);
            let header = EventHeader {
                position: None, iid: Iid([1; 16]), system_from: Timestamp(0), commit_position: 0,
                valid_time: Interval::ALL, op: EventOp::Put,
            };
            arena.push_row(header, Partition::whole(), 0);
            assert_eq!(staging.bytes(), vector_bytes);
            let before = (arena.rows.capacity(), arena.units.capacity());
            let error = admit(&mut arena.rows, &mut arena.units, &mut staging).unwrap_err();
            assert!(crate::shortlist::declined(&error));
            assert_eq!((arena.rows.capacity(), arena.units.capacity()), before);
            assert_eq!(arena.rows.len(), 1);
            assert_eq!(arena.units.len(), dimensions);
        }
        Ok(())
    }

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
    fn borrowed_training_keeps_the_signed_execution_values() -> Result<()> {
        use arrow_array::{Array, ListArray, types::UInt64Type};

        let list = ListArray::from_iter_primitive::<UInt64Type, _, _>([
            Some(vec![Some(u64::MAX), Some(3)]),
            Some(vec![None, Some(5)]),
        ]);
        let field = arrow_schema::Field::new("embedding", list.data_type().clone(), true);
        for row in 0..list.len() {
            let cell = ArrowCell { field: &field, array: &list, row };
            assert!(numeric_training_span(cell).is_some());
            let memory = MemoryBudget::new(1 << 20);
            let expected = UnitProbe::from_value(&read_execution_value(&field, &list, row)?, 2, &memory)?;
            let output = with_cell_unit(cell, 2, &memory, |unit| Ok([unit(0), unit(1)]))?;
            assert_eq!(output.as_ref().map(|unit| unit.as_slice()), expected.as_ref().map(UnitProbe::values));
        }
        Ok(())
    }

    #[test]
    fn normalization_keeps_f64_precision_until_the_final_components() {
        let components = [1e100, -2e100, -0.0, 9_007_199_254_740_993i64 as f64];
        let value = Value::list(vec![Value::F64(components[0]), Value::F64(components[1]), Value::F32(-0.0), Value::I64(9_007_199_254_740_993)]);
        let norm = components.iter().map(|value| value * value).sum::<f64>().sqrt();
        let expected = components.map(|component| ((component / norm) as f32).to_bits());
        let memory = MemoryBudget::new(4 * size_of::<f32>());
        let unit = UnitProbe::from_value(&value, 4, &memory).unwrap().unwrap();
        assert_eq!(unit.values().iter().copied().map(f32::to_bits).collect::<Vec<_>>(), expected);
        assert_eq!(memory.used_bytes(), 4 * size_of::<f32>());
        drop(unit);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn undefined_vectors_leave_staged_components_untouched() {
        let mut output = vec![1.0];
        for components in [[Some(1.0), None], [Some(f64::NAN), Some(1.0)], [Some(0.0), Some(-0.0)], [Some(f64::MAX), Some(1.0)]] {
            assert!(!append_unit_with(&mut output, components.into_iter(), |_| Ok(())).unwrap());
            assert_eq!(output, [1.0]);
        }
        assert!(append_unit_with(&mut output, [Some(0.0), Some(1.0)].into_iter(), |_| Ok(())).unwrap());
        assert_eq!(output, [1.0, 0.0, 1.0]);
    }
}
