use std::sync::Arc;

use arrow_array::{Array, ArrayRef};
use arrow_schema::Field;
use rtdb_columnar::MemoryBudget;
use rtdb_types::{Result, Value};

use super::overflow;

pub(super) enum ControlCell<'a> {
    Value(Value),
    Funded(&'a ArrayRef),
}

pub(super) fn control_record(
    memory: &Arc<MemoryBudget>, fields: &[(&str, ControlCell<'_>)],
) -> Result<ArrayRef> {
    let bound = fields.iter().try_fold(8192usize, |bytes, (_, cell)| {
        let extra = match cell {
            ControlCell::Value(value) => value.retained_bytes()?,
            ControlCell::Funded(array) => array.get_array_memory_size()
                .checked_sub(array.get_buffer_memory_size()).ok_or_else(overflow)?,
        };
        extra.checked_mul(4).and_then(|extra| bytes.checked_add(extra)).ok_or_else(overflow)
    })?;
    let mut charge = memory.reserve(bound)?;
    let mut arrays = Vec::with_capacity(fields.len());
    let mut schema = Vec::with_capacity(fields.len());
    let mut shared_bytes = 0usize;
    for (name, cell) in fields {
        let (field, array) = match cell {
            ControlCell::Value(value) => {
                let ty = value.logical_type();
                (rtdb_columnar::to_field(*name, &ty)?, rtdb_columnar::build_array(&ty, std::slice::from_ref(value))?)
            }
            ControlCell::Funded(array) => {
                shared_bytes = shared_bytes.checked_add(array.get_buffer_memory_size()).ok_or_else(overflow)?;
                (Field::new(*name, array.data_type().clone(), false), Arc::clone(array))
            }
        };
        schema.push(field);
        arrays.push(array);
    }
    let record = rtdb_columnar::build_struct_array(schema.into(), &arrays, 1, None)?;
    let retained = record.get_array_memory_size().checked_sub(shared_bytes).ok_or_else(overflow)?;
    Arc::get_mut(&mut charge).expect("Control record construction owns its reservation").shrink_to(retained)?;
    rtdb_columnar::retain_array(record, &charge)
}

#[cfg(test)]
pub(super) fn assert_control_bytes(
    memory: &Arc<MemoryBudget>, table: &rtdb_types::TableRef, expected: rtdb_types::StructFields<Value>,
    positioned: ArrayRef, sibling: Option<rtdb_types::StructFields<Value>>,
) -> Result<()> {
    use rtdb_index::{PartitionState, TransactionBuffer};
    use rtdb_types::{EntityId, EventHeader, EventOp, Interval, StructFields, Timestamp, TransactionKey, TxId};

    let key = TransactionKey { tx_id: TxId(1), system_time: Timestamp(10) };
    let valid_time = Interval::ALL;
    let base = PartitionState::empty(Default::default())?;
    let mut legacy = TransactionBuffer::new(base.snapshot(), key, Arc::clone(memory))?;
    let mut typed = TransactionBuffer::new(base.snapshot(), key, Arc::clone(memory))?;
    for tx in [&mut legacy, &mut typed] {
        if let Some(sibling) = &sibling {
            tx.put(table.clone(), sibling.clone(), valid_time)?;
        } else {
            tx.delete(table.clone(), &EntityId::String("released".into()), valid_time)?;
        }
    }
    let id = expected.get("_id").unwrap().entity_id()?;
    legacy.put(table.clone(), expected, valid_time)?;
    let header = EventHeader {
        position: None, iid: id.iid(), system_from: key.system_time, commit_position: 0, valid_time, op: EventOp::Put,
    };
    let batch = rtdb_format::data::funded_put_batch(positioned, &[header], memory)?;
    typed.append_assembled_put_batch(table, batch, &StructFields::new())?;

    let legacy = legacy.stage()?;
    let typed = typed.stage()?;
    let encode = |batch: &arrow_array::RecordBatch| crate::ipc::encode(batch.schema_ref(), [batch], memory);
    let expected = legacy.tables()[table].batch().unwrap().arrow();
    let actual = typed.tables()[table].batch().unwrap().arrow();
    assert_eq!(encode(expected)?.as_slice(), encode(actual)?.as_slice());
    Ok(())
}
