use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{LogicalType, Result, Value};

use crate::{cursor::OwnedRow, invalid};

pub(crate) struct BoundParameters {
    values: OwnedRow,
    types: Vec<LogicalType>,
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
        Ok(Self { values, types, _reservation: reservation })
    }

    pub fn new(values: Vec<Value>, types: Vec<LogicalType>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        if values.len() != types.len() { return Err(invalid("parameter-width", "Parameter values and declared types have different widths")); }
        let values = OwnedRow::new(values, memory)?;
        let reservation = memory.reserve(type_bytes(&types)?)?;
        Ok(Self { values, types, _reservation: reservation })
    }

    pub fn values(&self) -> &[Value] { &self.values.values }
    pub fn types(&self) -> &[LogicalType] { &self.types }
    pub fn bytes(&self) -> Result<usize> { self.values.bytes().checked_add(self._reservation.bytes()).ok_or_else(size_error) }
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
