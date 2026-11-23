use std::sync::{Arc, OnceLock};

use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_types::{LogicalType, Result, Value};

use crate::{cursor::OwnedRow, invalid};

pub struct BoundParameters {
    input: Input,
    types: Vec<LogicalType>,
    _reservation: Arc<MemoryReservation>,
}

enum Input {
    Row(OwnedRow),
    Arrow(ArrowRow),
    Overlay { outer: Arc<BoundParameters>, values: Vec<(usize, OverlayValue)> },
}

pub(crate) enum OverlayValue {
    Scalar(Value),
    Arrow(ArrowBinding),
}

pub(crate) struct ArrowBinding {
    batch: Arc<crate::columnar::ColumnBatch>,
    column: usize,
    row: usize,
    memory: Arc<MemoryBudget>,
    scalar: LazyValue,
}

impl ArrowBinding {
    pub(crate) fn new(batch: Arc<crate::columnar::ColumnBatch>, column: usize, row: usize, memory: Arc<MemoryBudget>) -> Option<Self> {
        batch.columns.get(column)?.expression_cell(row)?;
        Some(Self { batch, column, row, memory, scalar: LazyValue::default() })
    }

    fn cell(&self) -> Option<rtdb_expr::ArrowParameterCell<'_>> {
        let rtdb_columnar::ArrowCell { field, array, row } = self.batch.columns[self.column].expression_cell(self.row)?;
        Some(rtdb_expr::ArrowParameterCell { field, array, row })
    }

    fn value(&self) -> Result<&Value> {
        let column = &self.batch.columns[self.column];
        self.scalar.read(&self.memory, || column.bound(self.row), || {
            let mut value = column.value(self.row)?;
            value.admit_expression_dates()?;
            Ok(value)
        })
    }
}

impl OverlayValue {
    fn known(&self) -> Option<&Value> {
        match self {
            Self::Scalar(value) => Some(value),
            Self::Arrow(binding) => binding.scalar.value.get().map(|value| &value.value),
        }
    }

    fn get(&self) -> Result<&Value> {
        match self {
            Self::Scalar(value) => Ok(value),
            Self::Arrow(binding) => binding.value(),
        }
    }

    fn retained_bytes(&self) -> Result<usize> {
        match self {
            Self::Scalar(value) => value.retained_bytes(),
            Self::Arrow(_) => Ok(0),
        }
    }
}

struct ArrowRow {
    batch: OwnedBatch,
    row: usize,
    values: Box<[LazyValue]>,
    memory: Arc<MemoryBudget>,
}

#[derive(Default)]
struct LazyValue {
    lock: parking_lot::Mutex<()>,
    value: OnceLock<RetainedValue>,
}

struct RetainedValue { value: Value, _reservation: Arc<MemoryReservation> }

impl LazyValue {
    fn read(&self, memory: &Arc<MemoryBudget>, bound: impl FnOnce() -> Result<usize>, read: impl FnOnce() -> Result<Value>) -> Result<&Value> {
        if let Some(value) = self.value.get() { return Ok(&value.value); }
        let _lock = self.lock.lock();
        if let Some(value) = self.value.get() { return Ok(&value.value); }
        let mut reservation = memory.reserve(bound()?)?;
        let value = read()?;
        Arc::get_mut(&mut reservation).expect("Decoded parameter owns its reservation").shrink_to(value.retained_bytes()?)?;
        Ok(&self.value.get_or_init(|| RetainedValue { value, _reservation: reservation }).value)
    }
}

impl ArrowRow {
    fn value(&self, index: usize) -> Result<Option<&Value>> {
        let Some(cell) = self.values.get(index) else { return Ok(None); };
        let field = self.batch.batch().schema_ref().field(index);
        let array = self.batch.batch().column(index).as_ref();
        cell.read(&self.memory,
            || rtdb_columnar::estimate_read_value_bytes(field, array, self.row),
            || rtdb_columnar::read_execution_value(field, array, self.row),
        ).map(Some)
    }
}

impl rtdb_expr::ParameterLookup for BoundParameters {
    fn width(&self) -> usize { self.types.len() }
    fn value(&self, index: usize) -> Result<Option<&Value>> { self.get(index) }
    fn known_value(&self, index: usize) -> Option<&Value> {
        if index >= self.types.len() { return None; }
        match &self.input {
            Input::Row(row) => row.values.get(index),
            Input::Arrow(row) => row.values[index].value.get().map(|value| &value.value),
            Input::Overlay { outer, values } => match values.iter().rev().find(|(slot, _)| *slot == index) {
                Some((_, value)) => value.known(),
                None if index < outer.types.len() => outer.known_value(index),
                None => Some(&Value::Null),
            },
        }
    }
    fn arrow_cell(&self, index: usize) -> Option<rtdb_expr::ArrowParameterCell<'_>> {
        if let Input::Overlay { outer, values } = &self.input {
            return match values.iter().rev().find(|(slot, _)| *slot == index) {
                Some((_, OverlayValue::Arrow(binding))) => binding.cell(),
                Some((_, OverlayValue::Scalar(_))) => None,
                None => outer.arrow_cell(index),
            };
        }
        let (batch, row) = self.arrow_input(index)?;
        Some(rtdb_expr::ArrowParameterCell {
            field: batch.batch().schema_ref().fields()[index].as_ref(),
            array: batch.batch().column(index).as_ref(),
            row,
        })
    }
}

impl BoundParameters {
    pub(crate) fn arrow_input(&self, index: usize) -> Option<(&OwnedBatch, usize)> {
        if index >= self.types.len() { return None; }
        match &self.input {
            Input::Row(_) => None,
            Input::Arrow(row) => Some((&row.batch, row.row)),
            Input::Overlay { outer, values } => {
                if values.iter().any(|(slot, _)| *slot == index) { return None; }
                outer.arrow_input(index)
            }
        }
    }

    #[cfg(test)]
    pub fn infer(values: Vec<Value>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let values = OwnedRow::new(values, memory)?;
        let bound = values.values.iter().try_fold(4096usize, |bytes, value| {
            bytes.checked_add(value.retained_bytes()?.checked_mul(16).ok_or_else(size_error)?).ok_or_else(size_error)
        })?;
        let temporary = memory.reserve(bound)?;
        let types = values.values.iter().map(Value::logical_type).collect::<Vec<_>>();
        let reservation = memory.reserve(type_bytes(&types)?)?;
        drop(temporary);
        Ok(Self { input: Input::Row(values), types, _reservation: reservation })
    }

    pub fn new(values: Vec<Value>, types: Vec<LogicalType>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        if values.len() != types.len() { return Err(invalid("parameter-width", "Parameter values and declared types have different widths")); }
        let values = OwnedRow::new(values, memory)?;
        let reservation = memory.reserve(type_bytes(&types)?)?;
        Ok(Self { input: Input::Row(values), types, _reservation: reservation })
    }

    pub fn from_arrow(batch: OwnedBatch, row: usize, types: Vec<LogicalType>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        if batch.batch().num_columns() != types.len() || row >= batch.batch().num_rows() {
            return Err(invalid("parameter-width", "The Arrow parameter position and declared types must match the batch"));
        }
        let slots = types.len().checked_mul(std::mem::size_of::<LazyValue>()).ok_or_else(size_error)?;
        let reservation = memory.reserve(type_bytes(&types)?.checked_add(slots).ok_or_else(size_error)?)?;
        let values = (0..types.len()).map(|_| LazyValue::default()).collect();
        let input = Input::Arrow(ArrowRow { batch, row, values, memory: Arc::clone(memory) });
        Ok(Self { input, types, _reservation: reservation })
    }

    pub fn overlay(self: &Arc<Self>, values: Vec<(usize, Value)>, types: Vec<LogicalType>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        self.overlay_sources(values.into_iter().map(|(slot, value)| (slot, OverlayValue::Scalar(value))).collect(), types, memory)
    }

    pub(crate) fn overlay_sources(self: &Arc<Self>, values: Vec<(usize, OverlayValue)>, types: Vec<LogicalType>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let slots = values.capacity().checked_mul(std::mem::size_of::<(usize, OverlayValue)>()).ok_or_else(size_error)?;
        let bytes = values.iter().try_fold(type_bytes(&types)?.checked_add(slots).ok_or_else(size_error)?, |bytes, (_, value)| {
            bytes.checked_add(value.retained_bytes()?).ok_or_else(size_error)
        })?;
        let reservation = memory.reserve(bytes)?;
        Ok(Self { input: Input::Overlay { outer: Arc::clone(self), values }, types, _reservation: reservation })
    }

    pub fn get(&self, index: usize) -> Result<Option<&Value>> {
        if index >= self.types.len() { return Ok(None); }
        match &self.input {
            Input::Row(row) => Ok(row.values.get(index)),
            Input::Arrow(row) => row.value(index),
            Input::Overlay { outer, values } => match values.iter().rev().find(|(slot, _)| *slot == index) {
                Some((_, value)) => value.get().map(Some),
                None => Ok(Some(outer.get(index)?.unwrap_or(&Value::Null))),
            },
        }
    }

    pub fn evaluator(&self) -> rtdb_expr::Parameters<'_> {
        match &self.input {
            Input::Row(row) => rtdb_expr::Parameters::Values(&row.values),
            _ => rtdb_expr::Parameters::Lookup(self),
        }
    }
    pub fn use_execution_budget(&mut self, memory: &Arc<MemoryBudget>) {
        if let Input::Arrow(row) = &mut self.input { row.memory = Arc::clone(memory); }
    }
    pub fn types(&self) -> &[LogicalType] { &self.types }
}

fn type_bytes(types: &Vec<LogicalType>) -> Result<usize> {
    let spare = (types.capacity() - types.len()).checked_mul(std::mem::size_of::<LogicalType>()).ok_or_else(size_error)?;
    types.iter().try_fold(spare.checked_add(256).ok_or_else(size_error)?, |bytes, ty| bytes.checked_add(ty.retained_bytes()?).ok_or_else(size_error))
}

fn size_error() -> rtdb_types::Anomaly { invalid("parameter-size", "Parameter metadata exceeds addressable memory") }

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::ScalarType;

    #[test]
    fn correlated_arrow_parameter_keeps_its_vector_until_a_row_api_requests_it() {
        use arrow_array::{FixedSizeListArray, Float32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use rtdb_expr::ParameterLookup;

        let memory = MemoryBudget::new(1 << 20);
        let item = Arc::new(Field::new("$data$", DataType::Float32, false));
        let shape = DataType::FixedSizeList(item.clone(), 2);
        let list = FixedSizeListArray::try_new(item, 2, Arc::new(Float32Array::from(vec![0.25, -0.5, 1.0, 2.0])), None).unwrap();
        let field = Field::new("emb", shape, false);
        let ty = rtdb_columnar::from_field(&field).unwrap();
        let record = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(list)]).unwrap();
        let source = Arc::new(crate::columnar::ColumnBatch::from_owned(OwnedBatch::new(record, &memory).unwrap()).unwrap());
        let base = Arc::new(BoundParameters::new(vec![], vec![], &memory).unwrap());
        let binding = ArrowBinding::new(source.clone(), 0, 1, memory.clone()).unwrap();
        let bound = base.overlay_sources(vec![(0, OverlayValue::Arrow(binding))], vec![ty], &memory).unwrap();

        let cell = bound.arrow_cell(0).unwrap();
        assert_eq!(cell.row, 1);
        assert!(bound.known_value(0).is_none());
        assert_eq!(bound.get(0).unwrap(), Some(&Value::FixedSizeList(rtdb_types::ListValue::from_f32(Float32Array::from(vec![1.0, 2.0])))));
    }

    #[test]
    fn parameter_schema_and_cells_share_one_retained_owner() {
        let memory = MemoryBudget::new(1 << 20);
        let parameters = Arc::new(BoundParameters::new(vec![Value::I64(1)], vec![LogicalType::Scalar(ScalarType::I64).maybe()], &memory).unwrap());
        let retained = Arc::clone(&parameters);
        drop(parameters);
        assert_eq!(retained.get(0).unwrap(), Some(&Value::I64(1)));
        assert_eq!(retained.types(), [LogicalType::Scalar(ScalarType::I64).maybe()]);
        let Input::Row(row) = &retained.input else { panic!("Expected row parameters"); };
        assert_eq!(memory.used_bytes(), row.bytes() + retained._reservation.bytes());
        drop(retained);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn failed_metadata_admission_releases_the_adopted_values() {
        let memory = MemoryBudget::new(1024);
        assert!(BoundParameters::infer(vec![Value::Utf8("value".into())], &memory).is_err());
        assert_eq!(memory.used_bytes(), 0);
        assert!(BoundParameters::new(vec![Value::Null], vec![], &memory).is_err());
        assert_eq!(memory.used_bytes(), 0);
    }
}
