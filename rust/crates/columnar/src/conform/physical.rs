use std::sync::Arc;
use arrow_array::{Array, ArrayRef, NullArray, PrimitiveArray, RunArray, UnionArray, make_array, types::{Int16Type, Int32Type, Int64Type, RunEndIndexType}};
use arrow_buffer::{ArrowNativeType, ScalarBuffer};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, UnionFields, UnionMode};
use rtdb_types::{LogicalType as L, Result};
use crate::interleave::{Padding, undefined_cells};
use crate::read_allocation::null_copy_bytes;
use crate::{MemoryBudget, MemoryReservation, copy_nullable_cells, copy_selected_cells, estimate_copy_cell_bytes, estimate_null_copy_cell_bytes, from_field, leg_name, to_field};

pub(super) fn additional_bytes(data: &ArrayData, source: &Field, target: &Field, event: bool) -> Result<usize> {
    let null_bytes = |target: &Field, cell_bytes: usize| {
        let cells = super::multiply(super::multiply(cell_bytes, 4)?, data.len())?;
        super::add(super::multiply(target.size(), 8)?, cells)
    };
    if undefined_union(source, target, event) {
        return null_bytes(target, null_copy_bytes(target, Padding::Undefined)?);
    }
    if let DataType::RunEndEncoded(ends, values) = target.data_type() {
        if let DataType::RunEndEncoded(source_ends, source_values) = source.data_type()
            && source_ends.data_type() == ends.data_type() {
            return additional_bytes(&data.child_data()[1], source_values, values, event);
        }
        let end_bytes = super::multiply(data.len(), 64)?;
        return super::add(end_bytes, additional_bytes(data, source, values, event)?);
    }
    if matches!(source.data_type(), DataType::RunEndEncoded(..)) {
        let array = make_array(data.clone());
        let initial = super::multiply(target.size(), 8)?;
        return (0..data.len()).try_fold(initial, |bytes, row| {
            let source = estimate_copy_cell_bytes(source, array.as_ref(), row)?;
            super::add(bytes, super::add(super::multiply(source, 4)?, super::multiply(target.size(), 2)?)?)
        });
    }
    match (source.data_type(), target.data_type()) {
        (source_type, DataType::Union(targets, UnionMode::Dense)) => {
            let tags = super::multiply(data.len(), 64)?;
            let children = match source_type {
                DataType::Union(sources, UnionMode::Dense) => {
                    targets.iter().try_fold(0, |bytes, (_, target)| {
                        let name = leg_name(&from_field(target)?)?;
                        let Some((index, _, source)) = union_leg(sources, &name) else { return Ok(bytes) };
                        super::add(bytes, additional_bytes(&data.child_data()[index], source, target, event)?)
                    })?
                }
                _ => {
                    if let Some(name) = monomorphic_leg(source)?
                        && let Some((_, _, target)) = union_leg(targets, &name) {
                        let copy = if data.null_count() > 0 {
                            let array = make_array(data.clone());
                            (0..array.len()).filter(|&row| array.is_valid(row)).try_fold(0, |bytes, row| {
                                super::add(bytes, super::multiply(estimate_copy_cell_bytes(source, array.as_ref(), row)?, 4)?)
                            })?
                        } else { 0 };
                        super::add(copy, additional_bytes(data, source, target, event)?)?
                    } else { 0 }
                }
            };
            super::add(tags, children)
        }
        (DataType::Null, _) => null_bytes(target, estimate_null_copy_cell_bytes(target)?),
        (DataType::Struct(source), DataType::Struct(target)) => target.iter().try_fold(0, |bytes, field| {
            let extra = match source.iter().position(|original| original.name() == field.name()) {
                Some(index) => additional_bytes(&data.child_data()[index], &source[index], field, event)?,
                None => null_bytes(field, estimate_null_copy_cell_bytes(field)?)?,
            };
            super::add(bytes, extra)
        }),
        (DataType::List(source), DataType::List(target))
        | (DataType::FixedSizeList(source, _), DataType::FixedSizeList(target, _))
        | (DataType::Map(source, _), DataType::Map(target, _)) => additional_bytes(&data.child_data()[0], source, target, event),
        _ => Ok(0),
    }
}

pub(super) fn relayout_result(array: &ArrayRef, source: &Field, target: &Field, memory: &Arc<MemoryBudget>) -> Result<(Field, ArrayRef, Vec<Arc<MemoryReservation>>)> {
    let bound = super::arrow_relayout_bound(array.as_ref(), source, target)?;
    let scratch = std::cell::RefCell::new(vec![memory.reserve(bound)?]);
    let reserve = |bytes| {
        scratch.borrow_mut().push(memory.reserve(bytes)?);
        Ok(())
    };
    let (field, array) = relayout_in(Arc::clone(array), source, target, false, Some(&reserve))?;
    Ok((field, array, scratch.into_inner()))
}

/// A result union of one data leg and nullable Null legs reads as that leg's type with nulls,
/// so a declared plain field receives the leg's cells by position.
/// Returns `None` for any other source, or a target that is itself a union.
fn collapse_union(array: &ArrayRef, source: &Field, target: &Field) -> Result<Option<(ArrayRef, Field)>> {
    let Some((leg, field)) = collapsible_union(source, target) else { return Ok(None) };
    let union = array.as_any().downcast_ref::<UnionArray>().ok_or_else(incompatible)?;
    let values = union.child(leg);
    let child = if let Some(start) = contiguous_union(union, leg) {
        values.slice(start, union.len())
    } else {
        let picks = (0..union.len()).map(|row| (union.type_id(row) == leg).then(|| union.value_offset(row) as u32))
            .collect::<arrow_array::UInt32Array>();
        arrow_select::take::take(values.as_ref(), &picks, None).map_err(crate::arrow_error)?
    };
    Ok(Some((child, field.as_ref().clone().with_nullable(true))))
}

fn contiguous_union(union: &UnionArray, leg: i8) -> Option<usize> {
    if union.is_empty() { return Some(0); }
    let first = union.value_offset(0);
    (0..union.len()).all(|row| union.type_id(row) == leg && union.value_offset(row) == first + row).then_some(first)
}

/// Bounds the gather `collapse_union` performs, including the per-component indices a fixed-size list takes.
fn collapse_bytes(array: &dyn Array, source: &Field, target: &Field) -> Result<Option<usize>> {
    let Some((leg, field)) = collapsible_union(source, target) else { return Ok(None) };
    let DataType::Union(fields, _) = source.data_type() else { return Ok(None) };
    let Some(index) = fields.iter().position(|(id, _)| id == leg) else { return Ok(None) };
    if array.as_any().downcast_ref::<UnionArray>().and_then(|union| contiguous_union(union, leg)).is_some() {
        return Ok(Some(0));
    }
    let rows = array.len();
    let picks = super::multiply(rows, 16)?;
    let output = match field.data_type() {
        DataType::FixedSizeList(item, width) => {
            if let Some((width, component)) = usize::try_from(*width).ok().zip(item.data_type().primitive_width()) {
                super::multiply(super::multiply(rows, width)?, super::add(component, 8)?)?
            } else {
                let array = make_array(array.to_data());
                (0..rows).try_fold(0, |bytes, row| super::add(bytes, super::multiply(estimate_copy_cell_bytes(source, array.as_ref(), row)?, 4)?))?
            }
        }
        _ => super::multiply(make_array(array.to_data().child_data()[index].clone()).get_array_memory_size(), 3)?,
    };
    Ok(Some(super::add(picks, output)?))
}

fn collapsible_union(source: &Field, target: &Field) -> Option<(i8, Arc<Field>)> {
    let DataType::Union(fields, UnionMode::Dense) = source.data_type() else { return None };
    if matches!(target.data_type(), DataType::Union(..)) { return None; }
    let mut data = None;
    for (id, field) in fields.iter() {
        match field.data_type() {
            DataType::Null if field.is_nullable() => {}
            DataType::Null | DataType::Union(..) => return None,
            _ if data.is_some() => return None,
            _ => data = Some((id, Arc::clone(field))),
        }
    }
    data
}

fn undefined_union(source: &Field, target: &Field, event: bool) -> bool {
    event && source.data_type() == &DataType::Null && !source.is_nullable()
        && matches!(target.data_type(), DataType::Union(_, UnionMode::Dense))
}

pub(super) fn relayout(array: ArrayRef, source: &Field, target: &Field, event: bool) -> Result<(Field, ArrayRef)> {
    relayout_in(array, source, target, event, None)
}

fn relayout_in(array: ArrayRef, source: &Field, target: &Field, event: bool, result: Option<&dyn Fn(usize) -> Result<()>>) -> Result<(Field, ArrayRef)> {
    if let Some(reserve) = result
        && let Some(bound) = collapse_bytes(array.as_ref(), source, target)?
    {
        reserve(bound)?;
        if let Some((leg, field)) = collapse_union(&array, source, target)? {
            reserve(super::arrow_relayout_bound(leg.as_ref(), &field, target)?)?;
            return relayout_in(leg, &field, target, false, result);
        }
    }
    if let Some(reserve) = result
        && matches!(source.data_type(), DataType::Union(_, UnionMode::Dense))
        && !matches!(target.data_type(), DataType::Union(..) | DataType::RunEndEncoded(..))
    {
        return gather_result_union(&array, source, target, None, reserve);
    }
    if undefined_union(source, target, event) {
        return Ok((target.clone(), undefined_cells(target.data_type(), array.len())?));
    }
    if let DataType::RunEndEncoded(ends, values) = target.data_type()
        && !matches!(source.data_type(), DataType::RunEndEncoded(source_ends, _) if source_ends.data_type() == ends.data_type()) {
        let (field, array) = relayout_in(array, source, values, event, result)?;
        let runs = match ends.data_type() {
            DataType::Int16 => pack_runs::<Int16Type>(&array)?,
            DataType::Int32 => pack_runs::<Int32Type>(&array)?,
            DataType::Int64 => pack_runs::<Int64Type>(&array)?,
            _ => return Err(incompatible()),
        };
        let layout = DataType::RunEndEncoded(Arc::clone(ends), Arc::new(field));
        let data = runs.to_data().into_builder().data_type(layout.clone()).build().map_err(crate::arrow_error)?;
        return Ok((target.clone().with_data_type(layout), make_array(data)));
    }
    if let DataType::RunEndEncoded(ends, values) = source.data_type()
        && !matches!(target.data_type(), DataType::RunEndEncoded(..)) {
        let expanded = match ends.data_type() {
            DataType::Int16 => unpack_runs::<Int16Type>(array.as_ref(), event)?,
            DataType::Int32 => unpack_runs::<Int32Type>(array.as_ref(), event)?,
            DataType::Int64 => unpack_runs::<Int64Type>(array.as_ref(), event)?,
            _ => return Err(incompatible()),
        };
        return relayout_in(expanded, values, target, event, result);
    }
    if let DataType::Union(fields, UnionMode::Dense) = target.data_type() {
        if let Some(reserve) = result
            && matches!(source.data_type(), DataType::Union(_, UnionMode::Dense))
        {
            match union(Arc::clone(&array), source, target, fields, event, result) {
                Err(error) if error.code == "xtdb/page-outside-declared-type" => {
                    return gather_result_union(&array, source, target, Some(fields), reserve);
                }
                outcome => return outcome,
            }
        }
        return union(array, source, target, fields, event, result);
    }
    if source.data_type() == &DataType::Null && target.data_type() != &DataType::Null {
        let field = target.clone().with_nullable(true);
        let empty = arrow_array::new_empty_array(target.data_type());
        let nulls = copy_nullable_cells(&[empty.as_ref()], &vec![None; array.len()])?;
        return Ok((field, nulls));
    }
    let data = array.to_data();
    let mut children = Vec::with_capacity(data.child_data().len());
    let data_type = match (source.data_type(), target.data_type()) {
        (DataType::Struct(source_fields), DataType::Struct(target_fields)) => {
            let fields = target_fields.iter().map(|target| {
                let (field, child) = match source_fields.iter().position(|source| source.name() == target.name()) {
                    Some(index) => relayout_in(make_array(data.child_data()[index].clone()), &source_fields[index], target, event, result)?,
                    None => {
                        let null = Field::new(target.name(), DataType::Null, true);
                        relayout_in(Arc::new(NullArray::new(data.len())), &null, target, event, result)?
                    }
                };
                children.push(child.to_data());
                Ok(field)
            }).collect::<Result<Vec<_>>>()?;
            DataType::Struct(fields.into())
        }
        (DataType::List(source), DataType::List(target)) => {
            let (field, child) = relayout_in(make_array(data.child_data()[0].clone()), source, target, event, result)?;
            children.push(child.to_data());
            DataType::List(Arc::new(field))
        }
        (DataType::FixedSizeList(source, count), DataType::FixedSizeList(target, target_count)) if count == target_count => {
            let (field, child) = relayout_in(make_array(data.child_data()[0].clone()), source, target, event, result)?;
            children.push(child.to_data());
            DataType::FixedSizeList(Arc::new(field), *count)
        }
        (DataType::Map(source, sorted), DataType::Map(target, target_sorted)) if sorted == target_sorted => {
            let (field, child) = relayout_in(make_array(data.child_data()[0].clone()), source, target, event, result)?;
            children.push(child.to_data());
            DataType::Map(Arc::new(field), *sorted)
        }
        (DataType::RunEndEncoded(source_ends, source_values), DataType::RunEndEncoded(target_ends, target_values)) => {
            let (ends, ends_array) = relayout_in(make_array(data.child_data()[0].clone()), source_ends, target_ends, event, result)?;
            let (values, values_array) = relayout_in(make_array(data.child_data()[1].clone()), source_values, target_values, event, result)?;
            children.extend([ends_array.to_data(), values_array.to_data()]);
            DataType::RunEndEncoded(Arc::new(ends), Arc::new(values))
        }
        (source, target) if source == target => target.clone(),
        (DataType::Int8, DataType::UInt8) | (DataType::Int16, DataType::UInt16)
        | (DataType::Int32, DataType::UInt32) | (DataType::Int64, DataType::UInt64) => target.data_type().clone(),
        _ => return Err(incompatible()),
    };
    let field = target.clone().with_data_type(data_type.clone()).with_nullable(target.is_nullable() || source.is_nullable());
    let data = data.into_builder().data_type(data_type).child_data(children).build().map_err(crate::arrow_error)?;
    Ok((field, make_array(data)))
}

fn gather_result_union(
    array: &ArrayRef, source: &Field, target: &Field, targets: Option<&UnionFields>,
    reserve: &dyn Fn(usize) -> Result<()>,
) -> Result<(Field, ArrayRef)> {
    let rows = array.len();
    let bound = (0..rows).try_fold(super::add(super::multiply(rows, 128)?, 4096)?, |bytes, row| {
        super::add(bytes, super::multiply(estimate_copy_cell_bytes(source, array.as_ref(), row)?, 4)?)
    })?;
    reserve(bound)?;

    let mut legs = match targets {
        Some(fields) => fields.iter().map(|(id, field)| {
            Ok((id, super::leg_name(&from_field(field)?)?, ResultLeg::new(Arc::clone(field))))
        }).collect::<Result<Vec<_>>>()?,
        None => {
            let field = target.clone().with_nullable(target.is_nullable() || from_field(source)?.nullable());
            vec![(0, String::new(), ResultLeg::new(Arc::new(field)))]
        }
    };
    let mut ids = Vec::with_capacity(rows);
    let mut offsets = Vec::with_capacity(rows);
    for row in 0..rows {
        let cell = crate::ArrowCell { field: source, array: array.as_ref(), row }.physical()?;
        if cell.is_undefined()? {
            return Err(rtdb_types::Anomaly::fault("xtdb/zero-leg-read", "The bottom type has no values to read"));
        }
        let absent = cell.is_null()?;
        let leg = if targets.is_none() { 0 } else {
            let name = if absent { String::from("null") } else { super::leg_name(&from_field(&cell.field.clone().with_nullable(false))?)? };
            legs.iter().position(|(_, target, _)| *target == name).ok_or_else(incompatible)?
        };
        let (id, _, output) = &mut legs[leg];
        ids.push(*id);
        offsets.push(i32::try_from(output.picks.len()).map_err(|_| super::fault("page-allocation-size", "Union child exceeds i32 offsets"))?);
        output.push(cell, absent);
    }
    if targets.is_none() {
        return legs.pop().expect("plain output has one leg").2.finish(reserve);
    }
    let mut fields = Vec::with_capacity(legs.len());
    let mut children = Vec::with_capacity(legs.len());
    for (id, _, leg) in legs {
        let (field, child) = leg.finish(reserve)?;
        fields.push((id, field));
        children.push(child);
    }
    let fields = UnionFields::try_new(fields.iter().map(|(id, _)| *id), fields.iter().map(|(_, field)| field.clone()))
        .map_err(crate::arrow_error)?;
    let union = UnionArray::try_new(fields.clone(), ids.into(), Some(offsets.into()), children).map_err(crate::arrow_error)?;
    Ok((target.clone().with_data_type(DataType::Union(fields, UnionMode::Dense)), Arc::new(union)))
}

struct ResultLeg<'a> {
    target: Arc<Field>,
    sources: Vec<(&'a Field, &'a dyn Array)>,
    picks: Vec<Option<(usize, usize)>>,
}

impl<'a> ResultLeg<'a> {
    fn new(target: Arc<Field>) -> Self { Self { target, sources: Vec::new(), picks: Vec::new() } }

    fn push(&mut self, cell: crate::ArrowCell<'a>, absent: bool) {
        let pick = if absent {
            None
        } else {
            let source = self.sources.iter().position(|(field, array)| {
                std::ptr::eq(*field, cell.field) && std::ptr::addr_eq(*array, cell.array)
            }).unwrap_or_else(|| {
                self.sources.push((cell.field, cell.array));
                self.sources.len() - 1
            });
            Some((source, cell.row))
        };
        self.picks.push(pick);
    }

    fn finish(self, reserve: &dyn Fn(usize) -> Result<()>) -> Result<(Field, ArrayRef)> {
        let mut target = self.target.as_ref().clone();
        let mut children = Vec::with_capacity(self.sources.len());
        for (source, array) in self.sources {
            reserve(super::arrow_relayout_bound(array, source, &target)?)?;
            let (field, child) = relayout_in(make_array(array.to_data()), source, &target, false, Some(reserve))?;
            target = field.clone();
            children.push((field, child));
        }
        let mut inputs = Vec::with_capacity(children.len().max(1));
        for (source, child) in children {
            if source == target {
                inputs.push(child);
            } else {
                reserve(super::arrow_relayout_bound(child.as_ref(), &source, &target)?)?;
                let (_, child) = relayout_in(child, &source, &target, false, Some(reserve))?;
                inputs.push(child);
            }
        }
        if inputs.is_empty() { inputs.push(arrow_array::new_empty_array(target.data_type())); }
        let inputs = inputs.iter().map(|array| array.as_ref()).collect::<Vec<_>>();
        let array = copy_nullable_cells(&inputs, &self.picks)?;
        let target = target.with_nullable(self.target.is_nullable() || array.null_count() != 0);
        Ok((target, array))
    }
}

fn pack_runs<R: RunEndIndexType>(array: &ArrayRef) -> Result<ArrayRef> {
    let ends = (1..=array.len()).map(|end| R::Native::from_usize(end).ok_or_else(incompatible)).collect::<Result<Vec<_>>>()?;
    let ends = PrimitiveArray::<R>::new(ends.into(), None);
    Ok(Arc::new(RunArray::<R>::try_new(&ends, array.as_ref()).map_err(crate::arrow_error)?))
}

fn unpack_runs<R: RunEndIndexType>(array: &dyn Array, event: bool) -> Result<ArrayRef> {
    let runs = array.as_any().downcast_ref::<RunArray<R>>().ok_or_else(incompatible)?;
    let selected = (0..runs.len()).map(|row| Some((0, runs.get_physical_index(row)))).collect::<Vec<_>>();
    let values = &[runs.values().as_ref()];
    if event { crate::copy_event_cells(values, &selected) } else { copy_nullable_cells(values, &selected) }
}

fn union(array: ArrayRef, source: &Field, target: &Field, targets: &UnionFields, event: bool, result: Option<&dyn Fn(usize) -> Result<()>>) -> Result<(Field, ArrayRef)> {
    let canonical = array.as_any().downcast_ref::<UnionArray>();
    let source_fields = match source.data_type() { DataType::Union(fields, UnionMode::Dense) => Some(fields), _ => None };
    let source_leg = if source_fields.is_none() { monomorphic_leg(source)? } else { None };
    let valid = array.nulls().filter(|nulls| source_leg.is_some() && nulls.null_count() > 0);
    let mut fields = Vec::with_capacity(targets.len());
    let mut children = Vec::with_capacity(targets.len());
    let mut mappings = Vec::with_capacity(targets.len());
    let mut nonnull_id = None;
    let mut null_id = None;
    for (target_id, target_field) in targets.iter() {
        let logical = from_field(target_field)?;
        let name = leg_name(&logical)?;
        let (source_field, child) = if let Some(sources) = source_fields {
            if let Some((_, source_id, field)) = union_leg(sources, &name) {
                mappings.push((source_id, target_id));
                (field.as_ref().clone(), Arc::clone(canonical.ok_or_else(incompatible)?.child(source_id)))
            } else { (target_field.as_ref().clone(), arrow_array::new_empty_array(target_field.data_type())) }
        } else if logical == L::Null {
            null_id = Some(target_id);
            (to_field(target_field.name(), &L::Null)?, Arc::new(NullArray::new(array.len())) as ArrayRef)
        } else if source_leg.as_deref() == Some(name.as_str()) {
            nonnull_id = Some(target_id);
            let child = match valid {
                Some(valid) => {
                    let picks = valid.valid_indices().map(|row| (0, row)).collect::<Vec<_>>();
                    copy_selected_cells(&[array.as_ref()], &picks)?
                }
                None => Arc::clone(&array),
            };
            (source.clone().with_nullable(false), child)
        } else { (target_field.as_ref().clone(), arrow_array::new_empty_array(target_field.data_type())) };
        let (field, child) = relayout_in(child, &source_field, target_field, event, result)?;
        fields.push((target_id, field));
        children.push(child);
    }
    let mut ids = Vec::with_capacity(array.len());
    let mut offsets = Vec::with_capacity(array.len());
    let mut value_offset = 0;
    for row in 0..array.len() {
        let (id, offset) = if let Some(source) = canonical {
            let id = mappings.iter().find(|(id, _)| *id == source.type_id(row)).map(|(_, target)| *target).ok_or_else(incompatible)?;
            (id, source.value_offset(row))
        } else if array.data_type() == &DataType::Null || array.is_null(row) {
            (null_id.ok_or_else(incompatible)?, row)
        } else {
            let offset = if valid.is_some() { value_offset } else { row };
            value_offset += 1;
            (nonnull_id.ok_or_else(incompatible)?, offset)
        };
        ids.push(id);
        offsets.push(i32::try_from(offset).map_err(|_| super::fault("page-allocation-size", "Union child exceeds i32 offsets"))?);
    }
    let fields = UnionFields::try_new(fields.iter().map(|(id, _)| *id), fields.iter().map(|(_, field)| field.clone())).map_err(crate::arrow_error)?;
    let array = Arc::new(UnionArray::try_new(fields.clone(), ScalarBuffer::from(ids), Some(ScalarBuffer::from(offsets)), children).map_err(crate::arrow_error)?);
    Ok((target.clone().with_data_type(DataType::Union(fields, UnionMode::Dense)), array))
}

fn monomorphic_leg(field: &Field) -> Result<Option<String>> {
    from_field(field)?.legs().into_iter().find(|leg| !matches!(leg, L::Null | L::Nothing)).as_ref().map(leg_name).transpose()
}

fn union_leg<'a>(fields: &'a UnionFields, name: &str) -> Option<(usize, i8, &'a Arc<Field>)> {
    fields.iter().enumerate().find_map(|(index, (id, field))| {
        from_field(field).and_then(|logical| leg_name(&logical)).is_ok_and(|leg| leg == name).then_some((index, id, field))
    })
}

fn incompatible() -> rtdb_types::Anomaly { super::fault("page-outside-declared-type", "Page physical layout differs from its declared type") }

#[test]
fn union_relayout_copies_only_the_selected_children_of_a_nullable_list() {
    use arrow_array::cast::AsArray;
    use arrow_buffer::NullBuffer;
    use rtdb_types::{ListKind, ScalarType, Value};

    let width = 1 << 20;
    let element = L::from_legs([L::Scalar(ScalarType::I64), L::Scalar(ScalarType::FixedSizeBinary(width))]);
    let ty = L::List { kind: ListKind::FixedSizeList(1), element: Box::new(element) }.maybe();
    let field = to_field("column", &ty).unwrap();
    let cells = [
        Value::FixedSizeList(vec![Value::FixedSizeBinary(vec![9; width as usize])].into()),
        Value::FixedSizeList(vec![Value::I64(42)].into()),
    ];
    let array = crate::build_array(&ty, &cells).unwrap();
    let data = array.to_data().into_builder().nulls(Some(NullBuffer::from(vec![false, true]))).build().unwrap();
    let array = make_array(data);
    let joined = ty.join(&L::Scalar(ScalarType::Utf8));
    let target = to_field("column", &joined).unwrap();
    assert!(crate::arrow_relayout_bound(array.as_ref(), &field, &target).unwrap() < width as usize);

    let (target, output) = crate::relayout_arrow_column(array, &field, &target).unwrap();

    assert!(output.get_buffer_memory_size() < width as usize);
    assert_eq!(crate::read_value(&target, output.as_ref(), 0).unwrap(), Value::Null);
    assert_eq!(crate::read_value(&target, output.as_ref(), 1).unwrap(), cells[1]);
    let union = output.as_union();
    assert_eq!(union.child(union.type_id(1)).len(), 1);
}
