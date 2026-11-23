use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, ArrayRef, StructArray};
use arrow_schema::{DataType, Field as ArrowField, Fields, UnionMode};
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation, from_field, to_field};
use rtdb_plan::{Expr, Schema};
use rtdb_types::{LogicalType, Result};

use super::{ArrowVector, Column, ColumnBatch, SelectedColumn, arrow_cell_is_null, size_error, stable_read_layout};

#[derive(Clone, Copy)]
struct Record<'a> {
    array: &'a StructArray,
    position: usize,
}

fn record(column: &Column, row: usize) -> Option<Option<Record<'_>>> {
    let Some((array, field, logical, position)) = column.arrow_cell(row)? else { return Some(None); };
    if array.is_null(position) || field.data_type() == &DataType::Null && field.is_nullable() {
        return Some(None);
    }
    let logical = logical?;
    let cell = (ArrowCell { field, array, row: position }).physical().ok()?;
    if matches!(field.data_type(), DataType::Union(_, UnionMode::Dense)) {
        let active_type = from_field(cell.field).ok()?;
        // A named leg can supply the reader's logical type even when the physical field differs.
        if let LogicalType::Union(legs) = logical
            && let Some(named) = legs.iter().find(|leg| rtdb_columnar::leg_name(leg).is_ok_and(|name| name == *cell.field.name()))
            && *named != active_type {
            return None;
        }
        if !stable_read_layout(&active_type, cell.field) { return None; }
    } else if !stable_read_layout(logical, field) {
        return None;
    }
    if cell.is_null().ok()? {
        return (!cell.is_undefined().ok()?).then_some(None);
    }
    Some(Some(Record { array: cell.array.as_any().downcast_ref()?, position: cell.row }))
}

pub(super) fn merge(
    args: &[Expr], schema: &Schema, batch: &ColumnBatch, memory: &Arc<MemoryBudget>,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let [Expr::Column { id: old, .. }, Expr::Column { id: patch, .. }] = args else { return Ok(None); };
    let (Some(old), Some(patch)) = (schema.position(*old), schema.position(*patch)) else { return Ok(None); };
    let bytes = batch.len.checked_mul(256).and_then(|bytes| bytes.checked_add(8192)).ok_or_else(size_error)?;
    let reservation = memory.reserve(bytes)?;
    let mut rows = Vec::with_capacity(batch.len);
    let mut layouts = HashMap::new();
    let mut groups = Vec::new();
    let mut positions = Vec::with_capacity(batch.len);
    for row in 0..batch.len {
        let (Some(old), Some(Some(patch))) = (record(&batch.columns[old], row), record(&batch.columns[patch], row)) else { return Ok(None); };
        let fields = (old.map(|old| old.array.fields()), patch.array.fields());
        let group = *layouts.entry(fields).or_insert_with(|| {
            let group = groups.len();
            groups.push((fields, Vec::new()));
            group
        });
        let indices = &mut groups[group].1;
        positions.push((group, indices.len()));
        indices.push(row);
        rows.push((old, patch));
    }
    if groups.is_empty() { return Ok(None); }
    let heterogeneous = groups.len() > 2
        || groups.iter().filter(|((old, _), _)| old.is_some()).count() > 1
        || groups.iter().any(|((_, fields), _)| *fields != groups[0].0.1);
    let union_records = [old, patch].iter().any(|&column| {
        (0..batch.len).any(|row| batch.columns[column].arrow_cell(row).flatten()
            .is_some_and(|(_, field, _, _)| matches!(field.data_type(), DataType::Union(..))))
    });
    let mut sources = Vec::with_capacity(groups.len());
    for ((old, patch), indices) in groups {
        let Some(((array, field, logical), charge)) = merged_group(&rows, &indices, old, patch, memory)? else {
            return Ok(None);
        };
        if heterogeneous || union_records {
            rtdb_columnar::validate_projected_decimals(&field, array.as_ref())?;
        }
        if indices.len() == batch.len {
            return Ok(Some((Column::Arrow { array, field, logical }, charge)));
        }
        sources.push(Some((array, field, logical, Some(charge))));
    }
    Ok(Some((Column::Selected(SelectedColumn::new(sources, Arc::new(positions))), reservation)))
}

fn merged_group<'a>(
    rows: &[(Option<Record<'a>>, Record<'a>)], indices: &[usize], old: Option<&Fields>, patch: &Fields, memory: &Arc<MemoryBudget>,
) -> Result<Option<(ArrowVector, Arc<MemoryReservation>)>> {
    let previous = old.into_iter().flatten().enumerate().map(|(index, field)| {
        (field, Some(index), patch.iter().position(|candidate| candidate.name() == field.name()))
    });
    let added = patch.iter().enumerate().filter(|(_, field)| {
        old.is_none_or(|fields| !fields.iter().any(|candidate| candidate.name() == field.name()))
    }).map(|(index, field)| (field, None, Some(index)));
    let metadata = old.into_iter().flatten().chain(patch).try_fold(4096usize, |bytes, field| {
        bytes.checked_add(field.size().checked_mul(8).ok_or_else(size_error)?).ok_or_else(size_error)
    })?;
    let mut charge = memory.reserve(metadata)?;
    let mut fields = Vec::new();
    let mut arrays = Vec::new();
    for (field, old_index, patch_index) in previous.chain(added) {
        let ty = match (old_index, patch_index) {
            (Some(old_index), Some(patch_index)) => {
                let old = from_field(&old.expect("The record's old fields were bound")[old_index])?;
                let patch = from_field(&patch[patch_index])?;
                rtdb_plan::function_type("_patch", &[
                    LogicalType::Struct(rtdb_types::StructFields::from([(field.name().clone(), old)])),
                    LogicalType::Struct(rtdb_types::StructFields::from([(field.name().clone(), patch)])),
                ]).map(|ty| rtdb_plan::field_type(&ty, field.name()))?
            }
            _ => from_field(field)?,
        };
        let target = to_field(field.name(), &ty)?;
        let preferred = |row: usize| {
            let (old, patch) = rows[row];
            if let Some(index) = patch_index {
                let field = &patch.array.fields()[index];
                let array = patch.array.column(index);
                if old_index.is_none() || !arrow_cell_is_null(field, array.as_ref(), patch.position)? {
                    return Some((field, array, patch.position));
                }
            }
            let (old, index) = (old?, old_index?);
            Some((&old.array.fields()[index], old.array.column(index), old.position))
        };
        let Some((array, _scratch)) = gather_child(indices, preferred, &target, memory)? else { return Ok(None); };
        let owner = Arc::get_mut(&mut charge).expect("The patch record owns its reservation");
        owner.grow_to(owner.bytes().checked_add(array.get_array_memory_size()).ok_or_else(size_error)?)?;
        fields.push(target);
        arrays.push(array);
    }
    let Ok(array) = StructArray::try_new_with_length(fields.into(), arrays, None, indices.len()) else { return Ok(None); };
    let field = Arc::new(ArrowField::new("value", array.data_type().clone(), false));
    let logical = from_field(&field)?;
    Ok(Some(((Arc::new(array), field, Some(logical)), charge)))
}

fn gather_child<'a>(
    rows: &[usize], cell: impl Fn(usize) -> Option<(&'a Arc<ArrowField>, &'a ArrayRef, usize)>,
    target: &ArrowField, memory: &Arc<MemoryBudget>,
) -> Result<Option<(ArrayRef, Arc<MemoryReservation>)>> {
    let bound = rows.len().checked_mul(128).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
    let mut scratch = memory.reserve(bound)?;
    let lease = Arc::get_mut(&mut scratch).expect("The patch child owns its scratch");
    let mut slots = HashMap::new();
    let mut arrays = Vec::new();
    let mut selected = Vec::with_capacity(rows.len());
    for &row in rows {
        let Some((field, source, offset)) = cell(row) else { return Ok(None); };
        let key = Arc::as_ptr(source).cast::<()>();
        let index = if let Some(&index) = slots.get(&key) {
            index
        } else {
            let array = if field.data_type() == target.data_type() && field.metadata() == target.metadata() {
                Arc::clone(source)
            } else {
                let bytes = rtdb_columnar::arrow_relayout_bound(source.as_ref(), field, target)?;
                lease.grow_to(lease.bytes().checked_add(bytes).ok_or_else(size_error)?)?;
                let Ok((physical, array)) = rtdb_columnar::relayout_arrow_column(Arc::clone(source), field, target) else { return Ok(None); };
                if physical.data_type() != target.data_type() { return Ok(None); }
                array
            };
            let index = arrays.len();
            arrays.push(array);
            slots.insert(key, index);
            index
        };
        let bytes = rtdb_columnar::estimate_copy_cell_bytes(target, arrays[index].as_ref(), offset)?;
        lease.grow_to(lease.bytes().checked_add(bytes.checked_mul(4).ok_or_else(size_error)?).ok_or_else(size_error)?)?;
        selected.push((index, offset));
    }
    let arrays = arrays.iter().map(|array| array.as_ref()).collect::<Vec<_>>();
    let Ok(array) = rtdb_columnar::copy_selected_cells(&arrays, &selected) else { return Ok(None); };
    Ok(Some((array, scratch)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::RecordBatch;
    use rtdb_plan::{ColumnId, Field};
    use rtdb_types::{StructFields, Value};

    #[test]
    fn null_patch_cells_keep_existing_fields_and_missing_left_records_keep_the_patch_shape() {
        let memory = MemoryBudget::new(1 << 20);
        let explicit = |entries: &[(&str, Value)]| Value::Struct(entries.iter().map(|(name, value)| ((*name).into(), value.clone())).collect());
        let old = explicit(&[("retained", Value::I64(7)), ("shared", Value::Utf8("old".into()))]);
        let patch = explicit(&[("shared", Value::Null), ("added", Value::I64(9))]);
        let schema = Schema::new([old.logical_type().maybe(), patch.logical_type()].into_iter().enumerate().map(|(index, data_type)| {
            Field { id: ColumnId(index as u32), name: format!("c{index}"), qualifier: None,
                qualified_only: false, unnamed: false, data_type }
        }).collect()).unwrap();
        let arrow_fields = schema.fields().iter().map(|field| to_field(&field.name, &field.data_type).unwrap()).collect::<Vec<_>>();
        let arrays = [
            rtdb_columnar::copy_array(&schema.fields()[0].data_type, &[old, Value::Null]).unwrap(),
            rtdb_columnar::copy_array(&schema.fields()[1].data_type, &[patch.clone(), patch.clone()]).unwrap(),
        ];
        let input = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(arrow_fields)), arrays.to_vec()).unwrap();
        let input = ColumnBatch::from_owned(rtdb_columnar::OwnedBatch::new(input, &memory).unwrap()).unwrap();
        let args = schema.fields().iter().map(Field::expression).collect::<Vec<_>>();
        let (column, _charge) = merge(&args, &schema, &input, &memory).unwrap().unwrap();
        assert!(matches!(column, Column::Selected(_)));
        let expected = Value::Struct(StructFields::from([
            ("retained".into(), Value::I64(7)), ("shared".into(), Value::Utf8("old".into())), ("added".into(), Value::I64(9)),
        ]));
        assert_eq!(column.value(0).unwrap(), expected);
        assert_eq!(column.value(1).unwrap(), patch);
    }
    #[test]
    fn patch_selects_vectors_from_distinct_struct_legs_without_joining_their_field_sets() {
        use arrow_buffer::ScalarBuffer;
        use arrow_schema::UnionFields;

        let memory = MemoryBudget::new(1 << 20);
        let old_a = Value::Struct(StructFields::from([
            ("a".into(), Value::I64(1)),
            ("vector".into(), Value::FixedSizeList(vec![Value::F32(-0.0), Value::Null].into())),
        ]));
        let old_b = Value::Struct(StructFields::from([
            ("b".into(), Value::I64(2)),
            ("vector".into(), Value::list(vec![Value::F64(2.0)])),
        ]));
        let patch = Value::Struct(StructFields::from([("vector".into(), Value::Null), ("c".into(), Value::I64(3))]));
        let fields = UnionFields::try_new([2, 5], [
            to_field("first", &old_a.logical_type()).unwrap(), to_field("second", &old_b.logical_type()).unwrap(),
        ]).unwrap();
        let children = [old_a, old_b].into_iter().map(|cell| rtdb_columnar::copy_array(&cell.logical_type(), &[cell]).unwrap()).collect();
        let union = arrow_array::UnionArray::try_new(fields.clone(), ScalarBuffer::from(vec![5, 2, 5]), Some(ScalarBuffer::from(vec![0, 0, 0])), children).unwrap();
        let old = ArrowField::new("old", DataType::Union(fields, UnionMode::Dense), false);
        let patch_field = to_field("patch", &patch.logical_type()).unwrap();
        let patches = rtdb_columnar::copy_array(&patch.logical_type(), &[patch.clone(), patch.clone(), patch]).unwrap();
        let schema = Schema::new([&old, &patch_field].into_iter().enumerate().map(|(id, field)| {
            Field { id: ColumnId(id as u32), name: field.name().clone(), qualifier: None, qualified_only: false, unnamed: false,
                data_type: from_field(field).unwrap() }
        }).collect()).unwrap();
        let input = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new([old, patch_field].to_vec())), vec![Arc::new(union), patches]).unwrap();
        let input = ColumnBatch::from_owned(rtdb_columnar::OwnedBatch::new(input, &memory).unwrap()).unwrap();
        let args = schema.fields().iter().map(Field::expression).collect::<Vec<_>>();

        let (column, _charge) = merge(&args, &schema, &input, &memory).unwrap().unwrap();
        assert!(matches!(column, Column::Selected(_)));
        for (row, key) in [(0, "b"), (1, "a"), (2, "b")] {
            let (array, field, _, row) = column.arrow_cell(row).unwrap().unwrap();
            let cell = ArrowCell { field, array, row };
            let fields = cell.structure().unwrap().map(|field| field.field.name().as_str()).collect::<Vec<_>>();
            assert_eq!(fields, [key, "vector", "c"]);
            let vector = cell.structure().unwrap().nth(1).unwrap();
            let (components, range) = vector.float_list_span().unwrap();
            if key == "a" {
                assert_eq!(components.float_component(range.start).unwrap().to_bits(), (-0.0_f64).to_bits());
                assert_eq!(components.float_component(range.start + 1), None);
            } else {
                assert_eq!(components.float_component(range.start), Some(2.0));
            }
        }
    }

}
