use std::collections::BTreeMap;
use std::mem::size_of;
use std::sync::Arc;

use arrow_array::{Array, FixedSizeListArray, LargeListArray, ListArray, MapArray, RecordBatch, StructArray, UnionArray};
use arrow_schema::{DataType, Field};
use rtdb_columnar::{MemoryBudget, MemoryReservation, estimate_read_cell_bytes, from_field, leg_name, read_value};
use rtdb_format::ipc::{OwnedStream, schema_translation_bound};
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
    let mut scratch = RowScratch { reservation: memory.reserve(crate::ipc::allocation_bound(bytes)?)?, used: 0 };
    let stream = OwnedStream::read(bytes, memory)?;
    let _schema_scratch = memory.reserve(schema_translation_bound(stream.schema())?)?;
    let mut rows = Vec::new();
    for page in stream.into_pages() {
        for row in 0..page.batch().num_rows() {
            scratch.fund(2 * size_of::<OperationValue>())?;
            rows.push(read_row(page.batch(), row, &mut scratch)?);
        }
    }
    Ok(OperationRows { rows, reservation: scratch.reservation })
}

/// Preflights and decodes the complete IPC stream before visiting rows in wire order.
/// Each callback borrows a logical row backed by reusable scratch.
pub fn visit_operation_rows(bytes: &[u8], memory: &Arc<MemoryBudget>, mut visit: impl FnMut(&OperationValue)) -> Result<()> {
    let stream = OwnedStream::read(bytes, memory)?;
    let _schema_scratch = memory.reserve(schema_translation_bound(stream.schema())?)?;
    let mut scratch = RowScratch { reservation: memory.reserve(0)?, used: 0 };
    for page in stream.into_pages() {
        for row in 0..page.batch().num_rows() {
            scratch.used = 0;
            visit(&read_row(page.batch(), row, &mut scratch)?);
        }
    }
    Ok(())
}

struct RowScratch {
    reservation: Arc<MemoryReservation>,
    used: usize,
}

impl RowScratch {
    fn fund(&mut self, bytes: usize) -> Result<()> {
        self.used = self.used.checked_add(bytes).ok_or_else(size_error)?;
        let reservation = Arc::get_mut(&mut self.reservation).expect("Operation row reader owns its scratch");
        if self.used > reservation.bytes() {
            let preferred = self.used.saturating_add((reservation.bytes() / 2).max(512));
            if reservation.grow_to(preferred).is_err() {
                reservation.grow_to(self.used)?;
            }
        }
        Ok(())
    }

    fn insert(&mut self, fields: &mut BTreeMap<String, OperationValue>, name: &str, value: OperationValue) -> Result<()> {
        if !value.is_null() {
            let entry = 16 * (size_of::<String>() + size_of::<OperationValue>());
            self.fund(entry.checked_add(name.len()).ok_or_else(size_error)?)?;
            fields.insert(name.to_owned(), value);
        }
        Ok(())
    }

    fn scalar(&mut self, field: &Field, array: &dyn Array, row: usize) -> Result<OperationValue> {
        self.fund(estimate_read_cell_bytes(field, array, row)?)?;
        read_value(field, array, row).map(OperationValue::Scalar)
    }
}

fn read_row(batch: &RecordBatch, row: usize, scratch: &mut RowScratch) -> Result<OperationValue> {
    let mut fields = BTreeMap::new();
    for (field, array) in batch.schema_ref().fields().iter().zip(batch.columns()) {
        let value = cell(field, array.as_ref(), row, scratch)?;
        scratch.insert(&mut fields, field.name(), value)?;
    }
    Ok(OperationValue::Struct(fields))
}

fn cell(field: &Field, array: &dyn Array, row: usize, scratch: &mut RowScratch) -> Result<OperationValue> {
    if array.is_null(row) { return Ok(OperationValue::Scalar(Value::Null)); }
    Ok(match field.data_type() {
        DataType::Union(fields, _) => {
            let union = array.as_any().downcast_ref::<UnionArray>().ok_or_else(invalid)?;
            let type_id = union.type_id(row);
            let field = fields.iter().find(|(id, _)| *id == type_id).map(|(_, field)| field).ok_or_else(invalid)?;
            let value = cell(field, union.child(type_id).as_ref(), union.value_offset(row), scratch)?;
            let logical = from_field(field)?.legs().into_iter().find(|ty| *ty != LogicalType::Null).unwrap_or(LogicalType::Null);
            if field.name() == &leg_name(&logical)? { value }
            else {
                scratch.fund(size_of::<OperationValue>().checked_add(field.name().len()).ok_or_else(size_error)?)?;
                OperationValue::Tagged { tag: field.name().clone(), value: Box::new(value) }
            }
        }
        DataType::Struct(fields) => {
            let array = array.as_any().downcast_ref::<StructArray>().ok_or_else(invalid)?;
            let mut values = BTreeMap::new();
            for (column, field) in fields.iter().enumerate() {
                let value = cell(field, array.column(column).as_ref(), row, scratch)?;
                scratch.insert(&mut values, field.name(), value)?;
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
                scratch.scalar(field, array, row)?
            }
            else {
                scratch.fund(512)?;
                let values = if let Some(array) = array.as_any().downcast_ref::<ListArray>() { array.value(row) }
                    else if let Some(array) = array.as_any().downcast_ref::<LargeListArray>() { array.value(row) }
                    else if let Some(array) = array.as_any().downcast_ref::<FixedSizeListArray>() { array.value(row) }
                    else { return Err(invalid()); };
                scratch.fund(values.len().checked_mul(size_of::<OperationValue>()).ok_or_else(size_error)?)?;
                let mut elements = Vec::with_capacity(values.len());
                for row in 0..values.len() { elements.push(cell(child, values.as_ref(), row, scratch)?); }
                OperationValue::List { set: logical.legs().iter().any(|ty| matches!(ty, LogicalType::List { kind: ListKind::Set, .. })), values: elements }
            }
        }
        DataType::Map(entries, _) => {
            let array = array.as_any().downcast_ref::<MapArray>().ok_or_else(invalid)?;
            scratch.fund(512)?;
            let values = array.value(row);
            let DataType::Struct(fields) = entries.data_type() else { return Err(invalid()); };
            scratch.fund(values.len().checked_mul(size_of::<(OperationValue, OperationValue)>()).ok_or_else(size_error)?)?;
            let mut elements = Vec::with_capacity(values.len());
            for row in 0..values.len() {
                let key = cell(&fields[0], values.column(0).as_ref(), row, scratch)?;
                let value = cell(&fields[1], values.column(1).as_ref(), row, scratch)?;
                elements.push((key, value));
            }
            OperationValue::Map(elements)
        }
        _ => scratch.scalar(field, array, row)?,
    })
}
fn invalid() -> Anomaly { Anomaly::incorrect("rtdb.tx/inspection-array", "Transaction operations array does not match its Arrow field") }
fn size_error() -> Anomaly { Anomaly::incorrect("rtdb.tx/inspection-allocation", "Transaction operation row exceeds addressable memory") }
