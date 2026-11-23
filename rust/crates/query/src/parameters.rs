use std::sync::Arc;

use arrow_array::{Array, FixedSizeListArray};
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_types::{LogicalType, Result, Value};

use crate::{cursor::OwnedRow, invalid};

pub(crate) struct BoundParameters {
    values: OwnedRow,
    types: Vec<LogicalType>,
    arrow: Option<PackedVectors>,
    _reservation: Arc<MemoryReservation>,
}

impl BoundParameters {
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
        Ok(Self { values, types, arrow: None, _reservation: reservation })
    }

    pub fn new(values: Vec<Value>, types: Vec<LogicalType>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        if values.len() != types.len() { return Err(invalid("parameter-width", "Parameter values and declared types have different widths")); }
        let values = OwnedRow::new(values, memory)?;
        let reservation = memory.reserve(type_bytes(&types)?)?;
        Ok(Self { values, types, arrow: None, _reservation: reservation })
    }

    pub fn with_arrow(mut self, arrow: Option<PackedVectors>) -> Self {
        self.arrow = arrow;
        self
    }

    pub fn vector(&self, index: usize) -> Option<&FixedSizeListArray> {
        let parameters = self.arrow.as_ref()?;
        if !parameters.mask.get(index).copied().unwrap_or(false) { return None; }
        parameters.batch.batch().column(index).as_any().downcast_ref()
    }

    pub fn materialize(&self, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        let Some(arrow) = &self.arrow else { return Ok(None); };
        let skip = arrow.mask.iter().map(|packed| !packed).collect::<Vec<_>>();
        let (mut values, _scratch) = decode_row(&arrow.batch, &skip, memory)?;
        let _retained = memory.reserve(self.values.bytes())?;
        for (index, value) in values.iter_mut().enumerate() {
            if arrow.mask[index] { rtdb_columnar::admit_execution_value(value); }
            else { *value = self.values.values[index].clone(); }
        }
        Ok(Some(Self::new(values, self.types.clone(), memory)?))
    }

    pub fn values(&self) -> &[Value] { &self.values.values }
    pub fn types(&self) -> &[LogicalType] { &self.types }
    pub fn bytes(&self) -> Result<usize> { self.values.bytes().checked_add(self._reservation.bytes()).ok_or_else(size_error) }
}

pub(crate) struct PackedVectors { pub batch: OwnedBatch, pub mask: Vec<bool> }

pub(crate) fn decode_row(batch: &OwnedBatch, skip: &[bool], memory: &Arc<MemoryBudget>) -> Result<(Vec<Value>, Arc<MemoryReservation>)> {
    let batch = batch.batch();
    if batch.num_rows() != 1 { return Err(invalid("parameter-rows", "A query requires one Arrow argument row")); }
    let schema = batch.schema_ref();
    let bound = schema.fields().iter().zip(batch.columns()).enumerate().try_fold(4096usize, |bytes, (index, (field, array))| {
        let cell = if skip.get(index) == Some(&true) { 0 } else { rtdb_columnar::estimate_read_value_bytes(field, array.as_ref(), 0)? };
        bytes.checked_add(cell).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)
    })?;
    let scratch = memory.reserve(bound)?;
    let values = schema.fields().iter().zip(batch.columns()).enumerate().map(|(index, (field, array))| {
        if skip.get(index) == Some(&true) { Ok(Value::Null) } else { rtdb_columnar::read_value(field, array.as_ref(), 0) }
    }).collect::<Result<_>>()?;
    Ok((values, scratch))
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
        assert_eq!(retained.values(), [Value::I64(1)]);
        assert_eq!(retained.types(), [LogicalType::Scalar(ScalarType::I64).maybe()]);
        assert_eq!(memory.used_bytes(), retained.bytes().unwrap());
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
