//! The reference's dense unions mark an undefined cell - a struct field its row lacks - with type id -1 and no child slot.
//! Arrow-rs admits only a union's declared type ids, so pages are translated between that form and the native one:
//! leg 0 holding a null in a slot its non-nullable field never otherwise holds.

use std::sync::Arc;
use arrow_array::{ArrayRef, BooleanArray, RecordBatch, make_array, new_null_array};
use arrow_buffer::{Buffer, ScalarBuffer};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, UnionMode};
use rtdb_columnar::{MemoryBudget, MemoryReservation, copy_event_cells, estimate_event_copy_cell_bytes};
use rtdb_types::Result;
use super::{allocation_add, allocation_mul, invalid};

/// Rewrites a page decoded without union validation into the native form, then validates it in full.
pub(super) fn reinstate(batch: RecordBatch) -> Result<RecordBatch> {
    let columns = batch.columns().iter().map(|column| {
        let data = native(&column.to_data())?;
        data.validate_full().map_err(invalid)?;
        Ok(make_array(data))
    }).collect::<Result<Vec<ArrayRef>>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(invalid)
}

/// Replaces `data`'s children and, for a rewritten union, its buffers.
/// SAFETY: callers keep each child's type and every array's length; `reinstate` validates the native result, and the
/// reference-form result is only encoded.
fn rebuilt(data: &ArrayData, buffers: Option<Vec<Buffer>>, children: Vec<ArrayData>) -> ArrayData {
    let mut builder = data.clone().into_builder().child_data(children);
    if let Some(buffers) = buffers { builder = builder.buffers(buffers); }
    unsafe { builder.build_unchecked() }
}

fn dense_union_ids(data: &ArrayData) -> Result<Option<(ScalarBuffer<i8>, ScalarBuffer<i32>)>> {
    if !matches!(data.data_type(), DataType::Union(_, UnionMode::Dense)) { return Ok(None); }
    if data.offset() != 0 || data.buffers().len() != 2 { return Err(invalid("A dense union is sliced or lacks its id and offset buffers")); }
    if data.buffers()[0].len() < data.len() || data.buffers()[1].len() < data.len() * 4 { return Err(invalid("A dense union's id or offset buffer is shorter than the union")); }
    Ok(Some((ScalarBuffer::new(data.buffers()[0].clone(), 0, data.len()), ScalarBuffer::new(data.buffers()[1].clone(), 0, data.len()))))
}

fn native(data: &ArrayData) -> Result<ArrayData> {
    let mut children = data.child_data().iter().map(native).collect::<Result<Vec<_>>>()?;
    let (DataType::Union(fields, _), Some((ids, offsets))) = (data.data_type(), dense_union_ids(data)?) else { return Ok(rebuilt(data, None, children)); };
    if !ids.iter().any(|id| *id < 0) { return Ok(rebuilt(data, None, children)); }
    let (first, _) = fields.iter().next().ok_or_else(|| invalid("A dense union with undefined cells has no legs"))?;
    let first_child = children.first().ok_or_else(|| invalid("A dense union with undefined cells has no children"))?;
    first_child.validate_full().map_err(invalid)?;
    let slot = i32::try_from(first_child.len()).map_err(invalid)?;
    let appended = arrow_select::concat::concat(&[make_array(first_child.clone()).as_ref(), new_null_array(first_child.data_type(), 1).as_ref()]).map_err(invalid)?;
    children[0] = appended.to_data();
    let (ids, offsets): (Vec<i8>, Vec<i32>) = ids.iter().zip(offsets.iter()).map(|(id, offset)| if *id < 0 { (first, slot) } else { (*id, *offset) }).unzip();
    Ok(rebuilt(data, Some(vec![Buffer::from_vec(ids), Buffer::from_vec(offsets)]), children))
}

/// Rewrites a native page into the reference's form for persistence: a leg-0 null in a non-nullable leg becomes type id -1
/// with offset 0, and its slot leaves the child. The result is for encoding only; arrow-rs would reject it on validation.
pub(super) fn undefine(batch: &RecordBatch) -> Result<RecordBatch> {
    undefine_page(batch, &mut None)
}

pub(super) struct EventCopies<'a> {
    pub(super) memory: &'a Arc<MemoryBudget>,
    pub(super) retained: Vec<Arc<MemoryReservation>>,
}

pub(super) fn undefine_page(batch: &RecordBatch, copies: &mut Option<EventCopies<'_>>) -> Result<RecordBatch> {
    let mut changed = false;
    let columns = batch.columns().iter().map(|column| Ok(make_array(reference(&column.to_data(), &mut changed, copies)?))).collect::<Result<Vec<ArrayRef>>>()?;
    if !changed { return Ok(batch.clone()); }
    // SAFETY: the columns keep the schema's types and the batch's row count; the batch is only encoded.
    Ok(unsafe { RecordBatch::new_unchecked(batch.schema(), columns, batch.num_rows()) })
}

/// Filters the native leg-0 child before converting any descendant, since `filter` only accepts valid arrays.
fn reference(data: &ArrayData, changed: &mut bool, copies: &mut Option<EventCopies<'_>>) -> Result<ArrayData> {
    let convert = |children: &[ArrayData], changed: &mut bool, copies: &mut Option<EventCopies<'_>>| {
        children.iter().map(|child| reference(child, changed, copies)).collect::<Result<Vec<_>>>()
    };
    let unchanged = |data: &ArrayData, changed: &mut bool, copies: &mut Option<EventCopies<'_>>| {
        Ok(rebuilt(data, None, convert(data.child_data(), changed, copies)?))
    };
    let (DataType::Union(fields, _), Some((ids, offsets))) = (data.data_type(), dense_union_ids(data)?) else { return unchanged(data, changed, copies); };
    let Some((first, first_field)) = fields.iter().next() else { return unchanged(data, changed, copies); };
    let source = &data.child_data()[0];
    if first_field.is_nullable() || source.nulls().is_none() { return unchanged(data, changed, copies); }
    let undefined = |id: i8, offset: i32| id == first && source.is_null(offset as usize);
    if !ids.iter().zip(offsets.iter()).any(|(id, offset)| undefined(*id, *offset)) { return unchanged(data, changed, copies); }
    *changed = true;
    let mut remapped = vec![0i32; source.len()];
    let mut next = 0;
    for (slot, position) in remapped.iter_mut().enumerate() {
        if source.is_valid(slot) { *position = next; next += 1; }
    }
    let mut children = data.child_data().to_vec();
    children[0] = defined_cells(source, first_field, copies)?;
    let children = convert(&children, changed, copies)?;
    let (ids, offsets): (Vec<i8>, Vec<i32>) = ids.iter().zip(offsets.iter()).map(|(id, offset)| {
        if undefined(*id, *offset) { (-1, 0) } else if *id == first { (*id, remapped[*offset as usize]) } else { (*id, *offset) }
    }).unzip();
    Ok(rebuilt(data, Some(vec![Buffer::from_vec(ids), Buffer::from_vec(offsets)]), children))
}

fn defined_cells(source: &ArrayData, field: &Field, copies: &mut Option<EventCopies<'_>>) -> Result<ArrayData> {
    let array = make_array(source.clone());
    if let Some(copies) = copies
        && !source.child_data().is_empty()
    {
        let base = allocation_add(allocation_mul(field.size(), 8)?, allocation_mul(source.len(), 32)?)?;
        let bound = (0..source.len()).filter(|&row| source.is_valid(row)).try_fold(base, |bytes, row| {
            let cell = estimate_event_copy_cell_bytes(field, array.as_ref(), row)?;
            allocation_add(bytes, allocation_mul(cell, 4)?)
        })?;
        copies.retained.push(copies.memory.reserve(bound)?);
        let picks = (0..source.len()).filter(|&row| source.is_valid(row)).map(|row| Some((0, row))).collect::<Vec<_>>();
        return Ok(copy_event_cells(&[array.as_ref()], &picks)?.to_data());
    }
    let keep = BooleanArray::from((0..source.len()).map(|slot| !source.is_null(slot)).collect::<Vec<_>>());
    Ok(arrow_select::filter::filter(array.as_ref(), &keep).map_err(invalid)?.to_data())
}
