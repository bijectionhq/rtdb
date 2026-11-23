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
    Overlay { outer: Arc<BoundParameters>, values: Vec<(usize, Value)> },
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

impl ArrowRow {
    fn value(&self, index: usize) -> Result<Option<&Value>> {
        let Some(cell) = self.values.get(index) else { return Ok(None); };
        if let Some(value) = cell.value.get() { return Ok(Some(&value.value)); }
        let _lock = cell.lock.lock();
        if let Some(value) = cell.value.get() { return Ok(Some(&value.value)); }
        let field = self.batch.batch().schema_ref().field(index);
        let column = self.batch.batch().column(index);
        let bytes = rtdb_columnar::estimate_read_value_bytes(field, column.as_ref(), self.row)?;
        let mut reservation = self.memory.reserve(bytes)?;
        let value = rtdb_columnar::read_execution_value(field, column.as_ref(), self.row)?;
        Arc::get_mut(&mut reservation).expect("Decoded parameter owns its reservation").shrink_to(value.retained_bytes()?)?;
        Ok(Some(&cell.value.get_or_init(|| RetainedValue { value, _reservation: reservation }).value))
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
            Input::Overlay { outer, values } => values.iter().rev().find(|(slot, _)| *slot == index).map(|(_, value)| value)
                .or_else(|| if index < outer.types.len() { outer.known_value(index) } else { Some(&Value::Null) }),
        }
    }
    fn arrow_cell(&self, index: usize) -> Option<rtdb_expr::ArrowParameterCell<'_>> {
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
        let slots = values.capacity().checked_mul(std::mem::size_of::<(usize, Value)>()).ok_or_else(size_error)?;
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
                Some((_, value)) => Ok(Some(value)),
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
