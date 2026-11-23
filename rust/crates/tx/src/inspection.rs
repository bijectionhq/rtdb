use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Array, FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray, UnionArray};
use arrow_schema::{DataType, Field};
use rtdb_columnar::{MemoryBudget, MemoryReservation, from_field, leg_name, read_value};
use rtdb_types::{Anomaly, ListKind, ListValue, LogicalType, Result, Value};

pub enum OperationValue {
    Scalar(Value),
    Tagged { tag: String, value: Box<OperationValue> },
    Struct(BTreeMap<String, OperationValue>),
    List { values: Vec<OperationValue>, set: bool },
    Map(Vec<(OperationValue, OperationValue)>),
}
pub struct OperationRows { rows: Vec<OperationValue>, reservation: Arc<MemoryReservation> }
impl OperationRows {
    pub fn rows(&self) -> &[OperationValue] { &self.rows }
    pub fn allocation_bytes(&self) -> usize { self.reservation.bytes() }
}
impl OperationValue { fn is_null(&self) -> bool { matches!(self, Self::Scalar(Value::Null | Value::Undefined)) } }

pub fn decode_operation_rows(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<OperationRows> {
    let reservation = memory.reserve(crate::ipc::allocation_bound(bytes)?)?;
    let pages = crate::ipc::decode(bytes, memory)?;
    let mut rows = Vec::new();
    for page in pages {
        let schema = page.batch().schema();
        for row in 0..page.batch().num_rows() {
            let mut fields = BTreeMap::new();
            for (column, field) in schema.fields().iter().enumerate() {
                let value = cell(field, page.batch().column(column).as_ref(), row)?;
                if !value.is_null() { fields.insert(field.name().clone(), value); }
            }
            rows.push(OperationValue::Struct(fields));
        }
    }
    Ok(OperationRows { rows, reservation })
}

fn cell(field: &Field, array: &dyn Array, row: usize) -> Result<OperationValue> {
    if array.is_null(row) { return Ok(OperationValue::Scalar(Value::Null)); }
    Ok(match field.data_type() {
        DataType::Union(fields, _) => {
            let union = array.as_any().downcast_ref::<UnionArray>().ok_or_else(invalid)?;
            let type_id = union.type_id(row);
            let field = fields.iter().find(|(id, _)| *id == type_id).map(|(_, field)| field).ok_or_else(invalid)?;
            let value = cell(field, union.child(type_id).as_ref(), union.value_offset(row))?;
            let logical = from_field(field)?.legs().into_iter().find(|ty| *ty != LogicalType::Null).unwrap_or(LogicalType::Null);
            if field.name() == &leg_name(&logical)? { value }
            else { OperationValue::Tagged { tag: field.name().clone(), value: Box::new(value) } }
        }
        DataType::Struct(fields) => {
            let array = array.as_any().downcast_ref::<StructArray>().ok_or_else(invalid)?;
            let mut values = BTreeMap::new();
            for (column, field) in fields.iter().enumerate() {
                let value = cell(field, array.column(column).as_ref(), row)?;
                if !value.is_null() { values.insert(field.name().clone(), value); }
            }
            OperationValue::Struct(values)
        }
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) => {
            let logical = from_field(field)?;
            let packed = logical.legs().iter().any(|ty| matches!(ty, LogicalType::List { kind: ListKind::List | ListKind::FixedSizeList(_), .. }))
                && ListValue::supports_numeric_arrow(child)
                && match child.data_type() {
                    DataType::Union(fields, _) => fields.iter().all(|(_, field)| from_field(field).and_then(|ty| leg_name(&ty))
                        .is_ok_and(|name| name == field.name().as_str())),
                    _ => true,
                };
            if packed || logical.legs().iter().any(|ty| matches!(ty, LogicalType::List { kind: ListKind::TimestampTzRange, .. })) {
                OperationValue::Scalar(read_value(field, array, row)?)
            }
            else {
                let values = if let Some(array) = array.as_any().downcast_ref::<ListArray>() { array.value(row) }
                    else if let Some(array) = array.as_any().downcast_ref::<LargeListArray>() { array.value(row) }
                    else if let Some(array) = array.as_any().downcast_ref::<FixedSizeListArray>() { array.value(row) }
                    else { return Err(invalid()); };
                OperationValue::List { set: logical.legs().iter().any(|ty| matches!(ty, LogicalType::List { kind: ListKind::Set, .. })), values: (0..values.len()).map(|row| cell(child, values.as_ref(), row)).collect::<Result<_>>()? }
            }
        }
        DataType::Map(entries, _) => {
            let array = array.as_any().downcast_ref::<MapArray>().ok_or_else(invalid)?;
            let values = array.value(row);
            let DataType::Struct(fields) = entries.data_type() else { return Err(invalid()); };
            OperationValue::Map((0..values.len()).map(|row| Ok((cell(&fields[0], values.column(0).as_ref(), row)?, cell(&fields[1], values.column(1).as_ref(), row)?))).collect::<Result<_>>()?)
        }
        _ => OperationValue::Scalar(read_value(field, array, row)?),
    })
}
fn invalid() -> Anomaly { Anomaly::incorrect("rtdb.tx/inspection-array", "Transaction operations array does not match its Arrow field") }
