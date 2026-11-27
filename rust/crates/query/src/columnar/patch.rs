use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, ArrayRef, StructArray};
use arrow_schema::{DataType, Field as ArrowField, Fields, UnionMode};
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation, from_field, to_field};
use rtdb_plan::{Expr, Schema};
use rtdb_types::{LogicalType, Result, Value};

use super::{ArrowVector, Column, ColumnBatch, ProjectionInput, SelectedColumn, SelectedRows, arrow_cell_is_null, pack_evaluated_column, size_error, stable_read_layout};

#[derive(Clone, Copy)]
struct Record<'a> {
    array: &'a StructArray,
    position: usize,
    owner: &'a Arc<MemoryReservation>,
}

impl<'a> Record<'a> {
    fn child(self, index: usize) -> ChildCell<'a> {
        ChildCell {
            field: &self.array.fields()[index],
            array: self.array.column(index),
            position: self.position,
            owner: self.owner,
        }
    }
}

#[derive(Clone, Copy)]
struct ChildCell<'a> {
    field: &'a ArrowField,
    array: &'a ArrayRef,
    position: usize,
    owner: &'a Arc<MemoryReservation>,
}

impl ChildCell<'_> {
    fn matches_layout(self, target: &ArrowField) -> bool {
        self.field.data_type() == target.data_type() && self.field.metadata() == target.metadata()
    }
}

fn record<'a>(column: &'a Column, owner: &'a Arc<MemoryReservation>, row: usize) -> Option<Option<Record<'a>>> {
    let (array, field, logical, position, owner) = match column {
        Column::Arrow { array, field, logical } => (array, field, logical, row, owner),
        Column::Selected(selected) => {
            let Some(((array, field, logical, custody), position)) = selected.source(row).ok()? else { return Some(None); };
            (array, field, logical, position, custody.as_ref().unwrap_or(owner))
        }
        Column::Values(_) => return None,
    };
    if array.is_null(position) || field.data_type() == &DataType::Null && field.is_nullable() {
        return Some(None);
    }
    let logical = logical.as_ref()?;
    let cell = (ArrowCell { field, array: array.as_ref(), row: position }).physical().ok()?;
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
    Some(Some(Record { array: cell.array.as_any().downcast_ref()?, position: cell.row, owner }))
}

fn uniform_record<'a>(column: &'a Column, owner: &'a Arc<MemoryReservation>) -> Option<Option<Record<'a>>> {
    let Column::Arrow { array, field, logical } = column else { return None; };
    match field.data_type() {
        DataType::Null if field.is_nullable() && logical.as_ref() == Some(&LogicalType::Null) => Some(None),
        DataType::Struct(_) if array.null_count() == 0 => record(column, owner, 0),
        _ => None,
    }
}

pub(super) fn merge_expression(
    args: &[Expr], schema: &Schema, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>, context: &crate::QueryContext,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let [left, right] = args else { return Ok(None); };
    let memory = &context.memory;
    if matches!(left, Expr::Column { .. }) && matches!(right, Expr::Column { .. }) {
        return merge(args, schema, batch, memory);
    }
    if batch.len == 0 || rtdb_plan::function_type_with_args("_patch", args).is_err() { return Ok(None); }
    let merged = (|| {
        let build = |expr: &Expr| {
            if matches!(expr, Expr::Field { .. })
                && let Some(result) = super::projection::shared_column(expr, schema, batch, memory)?
            {
                return Ok(Some(result));
            }
            super::projection::computed_record(expr, schema, batch, parameters, context)
        };
        let Some(old) = bind_operand(left, schema, batch, build)? else { return Ok(None); };
        let Some(patch) = bind_operand(right, schema, batch, build)? else { return Ok(None); };
        let (old, patch) = (old.parts(), patch.parts());
        if !computed_operand_layout(old.0) || !computed_operand_layout(patch.0) { return Ok(None); }
        merge_operands([old, patch], batch.len, memory)
    })();
    match merged {
        Ok(merged) => Ok(merged),
        Err(_) => { context.check_cancelled()?; Ok(None) }
    }
}

fn bind_operand<'a>(
    expr: &Expr, schema: &Schema, batch: &'a ColumnBatch,
    compute: impl FnOnce(&Expr) -> Result<Option<(Column, Arc<MemoryReservation>)>>,
) -> Result<Option<ProjectionInput<'a>>> {
    if let Expr::Column { id, .. } = expr {
        return Ok(schema.position(*id).map(|position| ProjectionInput::Stored(&batch.columns[position], batch.column_reservation(position))));
    }
    Ok(compute(expr)?.map(|(column, owner)| ProjectionInput::Computed(column, owner)))
}

fn computed_operand_layout(column: &Column) -> bool {
    let supported = |field: &ArrowField| super::projection::record_field(field, false);
    match column {
        Column::Arrow { field, .. } => supported(field),
        Column::Selected(selected) => selected.source_layouts().all(|(_, field, _)| supported(field)),
        Column::Values(_) => false,
    }
}

fn merge(
    args: &[Expr], schema: &Schema, batch: &ColumnBatch, memory: &Arc<MemoryBudget>,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let [Expr::Column { id: old, .. }, Expr::Column { id: patch, .. }] = args else { return Ok(None); };
    let (Some(old), Some(patch)) = (schema.position(*old), schema.position(*patch)) else { return Ok(None); };
    merge_operands([
        (&batch.columns[old], batch.column_reservation(old)),
        (&batch.columns[patch], batch.column_reservation(patch)),
    ], batch.len, memory)
}

fn merge_operands(
    [old, patch]: [(&Column, &Arc<MemoryReservation>); 2], len: usize, memory: &Arc<MemoryBudget>,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    if len != 0 {
        if let Some(old) = uniform_record(old.0, old.1)
            && let Some(Some(patch)) = uniform_record(patch.0, patch.1)
            && let Some(group) = merged_group(
                |row| (old.map(|record| Record { position: row, ..record }), Record { position: row, ..patch }),
                0..len, old.map(|record| record.array.fields()), patch.array.fields(), memory,
            )?
        {
            let MergedGroup { vector: (array, field, logical), charge, shared_children } = group;
            if !shared_children { return Ok(Some((Column::Arrow { array, field, logical }, charge))); }
            let reservation = memory.reserve(512)?;
            let sources = Arc::new(vec![Some((array, field, logical, Some(charge)))]);
            let column = Column::Selected(SelectedColumn { sources, cells: SelectedRows::Span(0..len) });
            return Ok(Some((column, reservation)));
        }
    }
    let bytes = len.checked_mul(256).and_then(|bytes| bytes.checked_add(8192)).ok_or_else(size_error)?;
    let mut reservation = memory.reserve(bytes)?;
    fn pack_operand(column: &Column, memory: &Arc<MemoryBudget>) -> Option<(Column, Arc<MemoryReservation>)> {
        let Column::Values(values) = column else { return None; };
        values.iter().all(|value| matches!(value, Value::Struct(_) | Value::Null | Value::Undefined))
            .then(|| pack_evaluated_column(values, memory).ok()).flatten()
    }
    let operands = [old, patch];
    let packed = operands.map(|(column, _)| pack_operand(column, memory));
    let [(old, old_owner), (patch, patch_owner)] = std::array::from_fn(|index| {
        packed[index].as_ref().map_or(operands[index], |(column, owner)| (column, owner))
    });
    let mut rows = Vec::with_capacity(len);
    let mut layouts = HashMap::new();
    let mut groups = Vec::new();
    let mut positions = Vec::with_capacity(len);
    for row in 0..len {
        let (Some(old), Some(Some(patch))) = (record(old, old_owner, row), record(patch, patch_owner, row)) else { return Ok(None); };
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
    let union_records = [old, patch].iter().any(|column| {
        (0..len).any(|row| column.arrow_cell(row).flatten()
            .is_some_and(|(_, field, _, _)| matches!(field.data_type(), DataType::Union(..))))
    });
    let mut sources = Vec::with_capacity(groups.len());
    for ((old, patch), indices) in groups {
        let Some(MergedGroup { vector: (array, field, logical), charge, shared_children }) =
            merged_group(|row| rows[row], indices.iter().copied(), old, patch, memory)? else {
            return Ok(None);
        };
        if heterogeneous || union_records || packed.iter().any(Option::is_some) {
            rtdb_columnar::validate_projected_decimals(&field, array.as_ref())?;
        }
        if indices.len() == len && !shared_children {
            return Ok(Some((Column::Arrow { array, field, logical }, charge)));
        }
        sources.push(Some((array, field, logical, Some(charge))));
    }
    let cells = if sources.len() == 1 {
        drop(positions);
        SelectedRows::Span(0..len)
    } else {
        SelectedRows::Picks(Arc::new(positions))
    };
    let column = Column::Selected(SelectedColumn { sources: Arc::new(sources), cells });
    drop((rows, layouts));
    drop(packed);
    let retained = column.retained_bytes()?;
    let lease = Arc::get_mut(&mut reservation).expect("The patch merge owns its selection reservation");
    if retained <= lease.bytes() { lease.shrink_to(retained)?; } else { lease.grow_to(retained)?; }
    Ok(Some((column, reservation)))
}

struct MergedGroup {
    vector: ArrowVector,
    charge: Arc<MemoryReservation>,
    shared_children: bool,
}

fn merged_group<'a>(
    record_at: impl Fn(usize) -> (Option<Record<'a>>, Record<'a>),
    indices: impl ExactSizeIterator<Item = usize> + Clone, old: Option<&Fields>, patch: &Fields, memory: &Arc<MemoryBudget>,
) -> Result<Option<MergedGroup>> {
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
    let lease = Arc::get_mut(&mut charge).expect("The patch record owns its reservation");
    let mut fields = Vec::new();
    let mut arrays = Vec::new();
    let mut shared_children = false;
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
            let (old, patch) = record_at(row);
            if let Some(index) = patch_index {
                let child = patch.child(index);
                if old_index.is_none() || !arrow_cell_is_null(child.field, child.array.as_ref(), child.position)? {
                    return Some(child);
                }
            }
            Some(old?.child(old_index?))
        };
        let array = if let Some(array) = retain_child_range(indices.clone(), preferred, &target, lease)? {
            shared_children = true;
            array
        } else {
            let Some((array, mut scratch)) = gather_child(indices.clone(), preferred, &target, memory)? else { return Ok(None); };
            let retained = array.get_array_memory_size();
            let scratch = Arc::get_mut(&mut scratch).expect("The patch child owns its construction reservation");
            if retained <= scratch.bytes() { scratch.shrink_to(retained)?; } else { scratch.grow_to(retained)?; }
            lease.absorb(scratch)?;
            array
        };
        fields.push(target);
        arrays.push(array);
    }
    let Ok(array) = StructArray::try_new_with_length(fields.into(), arrays, None, indices.len()) else { return Ok(None); };
    let field = Arc::new(ArrowField::new("value", array.data_type().clone(), false));
    let logical = from_field(&field)?;
    Ok(Some(MergedGroup { vector: (Arc::new(array), field, Some(logical)), charge, shared_children }))
}

fn retain_child_range<'a>(
    mut rows: impl ExactSizeIterator<Item = usize>, cell: impl Fn(usize) -> Option<ChildCell<'a>>, target: &ArrowField,
    lease: &mut MemoryReservation,
) -> Result<Option<ArrayRef>> {
    let len = rows.len();
    let Some(first) = rows.next().and_then(&cell) else { return Ok(None); };
    let Some(end) = first.position.checked_add(len) else { return Ok(None); };
    if end > first.array.len() || !first.matches_layout(target) || first.array.data_type() != target.data_type()
        || !rows.enumerate().all(|(offset, row)| {
            cell(row).is_some_and(|next| Arc::ptr_eq(first.array, next.array)
                && next.position == first.position + offset + 1
                && (std::ptr::eq(first.field, next.field) || next.matches_layout(target)))
        }) {
        return Ok(None);
    }

    let descriptors = first.array.get_array_memory_size().checked_sub(first.array.get_buffer_memory_size()).ok_or_else(size_error)?;
    let retained = lease.bytes();
    let bound = descriptors.checked_mul(4).and_then(|bytes| bytes.checked_add(512))
        .and_then(|bytes| retained.checked_add(bytes)).ok_or_else(size_error)?;
    lease.grow_to(bound)?;
    let view = if len == first.array.len() { Arc::clone(first.array) } else { first.array.slice(first.position, len) };
    // A query page can own the source reservation outside its Arrow buffers.
    if let Ok(view) = rtdb_columnar::retain_array(view, first.owner) {
        return Ok(Some(view));
    }
    lease.shrink_to(retained)?;
    Ok(None)
}

fn gather_child<'a>(
    rows: impl ExactSizeIterator<Item = usize>, cell: impl Fn(usize) -> Option<ChildCell<'a>>,
    target: &ArrowField, memory: &Arc<MemoryBudget>,
) -> Result<Option<(ArrayRef, Arc<MemoryReservation>)>> {
    let bound = rows.len().checked_mul(128).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
    let mut scratch = memory.reserve(bound)?;
    let lease = Arc::get_mut(&mut scratch).expect("The patch child owns its scratch");
    let mut slots = HashMap::new();
    let mut arrays = Vec::new();
    let mut selected = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(child) = cell(row) else { return Ok(None); };
        let ChildCell { field, array: source, position: offset, .. } = child;
        let key = Arc::as_ptr(source).cast::<()>();
        let index = if let Some(&index) = slots.get(&key) {
            index
        } else {
            let array = if child.matches_layout(target) {
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
    use rtdb_types::StructFields;

    #[test]
    fn record_projection_uniform_patch_reuses_children_under_a_small_merge_budget() {
        let origin = MemoryBudget::new(1 << 20);
        let memory = MemoryBudget::new(1 << 16);
        let len = 4096;
        let old = Value::Struct(StructFields::from([("kept".into(), Value::I64(7))]));
        let patch = Value::Struct(StructFields::from([("kept".into(), Value::Null), ("new".into(), Value::I64(8))]));
        for (old, expected_kept) in [(old, Value::I64(7)), (Value::Null, Value::Null)] {
            let schema = Schema::new([old.logical_type(), patch.logical_type()].into_iter().enumerate().map(|(id, data_type)| {
                Field { id: ColumnId(id as u32), name: format!("c{id}"), qualifier: None,
                    qualified_only: false, unnamed: false, data_type }
            }).collect()).unwrap();
            let fields = schema.fields().iter().map(|field| to_field(&field.name, &field.data_type).unwrap()).collect::<Vec<_>>();
            let arrays = [old, patch.clone()].into_iter().map(|value| rtdb_columnar::copy_array(&value.logical_type(), &vec![value; len]).unwrap()).collect();
            let input = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), arrays).unwrap();
            let input = ColumnBatch::from_owned(rtdb_columnar::OwnedBatch::new(input, &origin).unwrap()).unwrap();
            let source = Arc::downgrade(input.column_reservation(0));
            let args = schema.fields().iter().map(Field::expression).collect::<Vec<_>>();
            let (column, charge) = merge(&args, &schema, &input, &memory).unwrap().unwrap();
            drop(input);
            assert!(source.upgrade().is_some());
            let Value::Struct(record) = column.value(len - 1).unwrap() else { panic!("Expected a patched record"); };
            assert_eq!(record.keys().map(String::as_str).collect::<Vec<_>>(), ["kept", "new"]);
            assert_eq!(record["kept"], expected_kept);
            assert_eq!(record["new"], Value::I64(8));
            drop((column, charge));
            assert!(source.upgrade().is_none());
            assert_eq!(memory.used_bytes(), 0);
        }
    }

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

    #[test]
    fn patch_keeps_arrow_vectors_when_the_other_operand_is_a_row_record() {
        use arrow_array::StringArray;

        let memory = MemoryBudget::new(1 << 20);
        let vector = Value::FixedSizeList(vec![Value::F32(-0.0), Value::Null].into());
        let record = |name: &str| Value::Struct(StructFields::from([
            ("vector".into(), vector.clone()), ("name".into(), Value::Utf8(name.into())),
        ]));
        let patch = Value::Struct(StructFields::from([("vector".into(), Value::Null), ("label".into(), Value::I64(5))]));
        let logical = record("first").logical_type();
        let field = Arc::new(to_field("lhs", &logical).unwrap());
        let array = rtdb_columnar::copy_array(&logical, &[record("first"), record("second"), record("third")]).unwrap();
        let text = array.as_any().downcast_ref::<StructArray>().unwrap().column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let source_bytes = text.value(1).as_ptr();
        let schema = Schema::new([logical.clone(), patch.logical_type()].into_iter().enumerate().map(|(index, data_type)| {
            Field { id: ColumnId(index as u32), name: format!("c{index}"), qualifier: None, qualified_only: false, unnamed: false, data_type }
        }).collect()).unwrap();
        let old = ColumnBatch::settled(vec![Column::Arrow { array, field, logical: Some(logical) }],
            3, memory.reserve(4096).unwrap()).unwrap();
        let owner = Arc::downgrade(old.column_reservation(0));
        let (old_rows, selection) = old.lend_window_span(0, 1..3, &memory).unwrap().unwrap();
        let batch = ColumnBatch::settled(vec![old_rows, Column::Values(vec![patch.clone(), patch])],
            2, memory.reserve(4096).unwrap()).unwrap();
        drop((old, selection));
        let args = schema.fields().iter().map(Field::expression).collect::<Vec<_>>();
        let (column, charge) = merge(&args, &schema, &batch, &memory).unwrap().unwrap();
        drop(batch);
        assert!(owner.upgrade().is_some());
        let Some(Some((array, field, _, row))) = column.arrow_cell(0) else { panic!("Expected an Arrow record"); };
        let text = array.as_any().downcast_ref::<StructArray>().unwrap().column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(text.value(row).as_ptr(), source_bytes);
        let cell = (ArrowCell { array, field, row }).structure().unwrap().next().unwrap();
        let (components, range) = cell.float_list_span().unwrap();
        assert_eq!(components.float_component(range.start).unwrap().to_bits(), (-0.0_f64).to_bits());
        assert_eq!(components.float_component(range.start + 1), None);
        for (row, name) in ["second", "third"].into_iter().enumerate() {
            assert_eq!(column.value(row).unwrap(), Value::Struct(StructFields::from([
                ("vector".into(), vector.clone()), ("name".into(), Value::Utf8(name.into())), ("label".into(), Value::I64(5)),
            ])));
        }
        drop((column, charge));
        assert!(owner.upgrade().is_none());
        assert_eq!(memory.used_bytes(), 0);
    }
}
