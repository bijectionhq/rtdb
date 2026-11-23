use std::sync::Arc;
use arrow_array::{Array, ArrayRef, NullArray, UnionArray, builder::StringBuilder, cast::AsArray, make_array, types::{Int16Type, Int32Type, Int64Type, RunEndIndexType}};
use arrow_buffer::{NullBufferBuilder, ScalarBuffer};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, UnionFields, UnionMode};
use rtdb_types::{LogicalType as L, Result};
use crate::interleave::{Padding, undefined_cells};
use crate::read_allocation::null_copy_bytes;
use crate::{copy_nullable_cells, copy_selected_cells, estimate_copy_cell_bytes, estimate_null_copy_cell_bytes, from_field, leg_name, to_field};

pub(super) fn additional_bytes(data: &ArrayData, source: &Field, target: &Field, event: bool) -> Result<usize> {
    let null_bytes = |target: &Field, cell_bytes: usize| {
        let cells = super::multiply(super::multiply(cell_bytes, 4)?, data.len())?;
        super::add(super::multiply(target.size(), 8)?, cells)
    };
    if undefined_union(source, target, event) {
        return null_bytes(target, null_copy_bytes(target, Padding::Undefined)?);
    }
    if source.data_type() == target.data_type() { return Ok(0); }
    let repack = matches!((source.data_type(), target.data_type()),
        (DataType::Union(fields, mode), DataType::Union(_, UnionMode::Dense)) if repack_union(fields, *mode));
    if repack || converts_positions(source.data_type(), target.data_type()) {
        let array = make_array(data.clone());
        return (0..array.len()).try_fold(super::multiply(target.size(), 8)?, |bytes, row| {
            super::add(bytes, super::multiply(estimate_copy_cell_bytes(source, array.as_ref(), row)?, 8)?)
        });
    }
    match (source.data_type(), target.data_type()) {
        (source_type, DataType::Union(targets, UnionMode::Dense)) => {
            let tags = super::multiply(data.len(), 64)?;
            let children = match source_type {
                DataType::Union(sources, _) => {
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

fn variable_list(data_type: &DataType) -> bool {
    matches!(data_type, DataType::List(_) | DataType::LargeList(_) | DataType::ListView(_) | DataType::LargeListView(_))
}

fn converts_positions(source: &DataType, target: &DataType) -> bool {
    (matches!(source, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Dictionary(_, _))
        && matches!(target, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View))
        || (variable_list(source) && variable_list(target) && !matches!((source, target), (DataType::List(_), DataType::List(_))))
        || (matches!(source, DataType::Union(_, _)) && !matches!(target, DataType::Union(_, _)))
        || (matches!(source, DataType::RunEndEncoded(_, _)) && !matches!(target, DataType::RunEndEncoded(_, _)))
        || (matches!(target, DataType::RunEndEncoded(_, _))
            && !matches!((source, target), (DataType::RunEndEncoded(ends, _), DataType::RunEndEncoded(next, _)) if ends.data_type() == next.data_type()))
}

fn undefined_union(source: &Field, target: &Field, event: bool) -> bool {
    event && source.data_type() == &DataType::Null && !source.is_nullable()
        && matches!(target.data_type(), DataType::Union(_, UnionMode::Dense))
}

pub(super) fn relayout(array: ArrayRef, source: &Field, target: &Field, event: bool) -> Result<(Field, ArrayRef)> {
    if undefined_union(source, target, event) {
        return Ok((target.clone(), undefined_cells(target.data_type(), array.len())?));
    }
    if source.data_type() == target.data_type() {
        return Ok((target.clone().with_nullable(target.is_nullable() || source.is_nullable()), array));
    }
    if let DataType::RunEndEncoded(ends, values) = target.data_type()
        && !matches!(source.data_type(), DataType::RunEndEncoded(previous, _) if previous.data_type() == ends.data_type()) {
        let (field, array) = relayout(array, source, values, event)?;
        let data_type = DataType::RunEndEncoded(Arc::clone(ends), Arc::new(field));
        let array = match ends.data_type() {
            DataType::Int16 => encode_runs::<Int16Type>(array, &data_type)?,
            DataType::Int32 => encode_runs::<Int32Type>(array, &data_type)?,
            DataType::Int64 => encode_runs::<Int64Type>(array, &data_type)?,
            _ => return Err(incompatible()),
        };
        return Ok((target.clone().with_data_type(data_type), array));
    }
    if let DataType::RunEndEncoded(ends, values) = source.data_type()
        && !matches!(target.data_type(), DataType::RunEndEncoded(_, _)) {
        macro_rules! expand {
            ($index:ty) => {{
                let runs = array.as_run::<$index>();
                let (field, values) = relayout(Arc::clone(runs.values()), values, target, event)?;
                let picks = (0..runs.len()).map(|row| (0, runs.get_physical_index(row))).collect::<Vec<_>>();
                return Ok((field, copy_selected_cells(&[values.as_ref()], &picks)?));
            }};
        }
        match ends.data_type() {
            DataType::Int16 => expand!(Int16Type),
            DataType::Int32 => expand!(Int32Type),
            DataType::Int64 => expand!(Int64Type),
            _ => return Err(incompatible()),
        }
    }
    if let DataType::Union(fields, UnionMode::Dense) = target.data_type() {
        if let DataType::Union(sources, mode) = source.data_type() && repack_union(sources, *mode) {
            return gather_union(array.as_union(), sources, target, fields, event);
        }
        if matches!(source.data_type(), DataType::Dictionary(_, _)) {
            let utf8 = to_field(source.name(), &from_field(source)?)?;
            let (source, array) = relayout(array, source, &utf8, event)?;
            return union(array, &source, target, fields, event);
        }
        return union(array, source, target, fields, event);
    }
    if source.data_type() == &DataType::Null && target.data_type() != &DataType::Null {
        let field = target.clone().with_nullable(true);
        let empty = arrow_array::new_empty_array(target.data_type());
        let nulls = copy_nullable_cells(&[empty.as_ref()], &vec![None; array.len()])?;
        return Ok((field, nulls));
    }
    if let DataType::Union(fields, _) = source.data_type() {
        let union = array.as_union();
        let children = fields.iter().map(|(id, source)| {
            if source.data_type() == &DataType::Null { Ok((id, None)) }
            else { relayout(Arc::clone(union.child(id)), source, target, event).map(|(_, array)| (id, Some(array))) }
        }).collect::<Result<Vec<_>>>()?;
        let empty = arrow_array::new_empty_array(target.data_type());
        let sources = children.iter().map(|(_, child)| child.as_deref().unwrap_or(empty.as_ref())).collect::<Vec<_>>();
        let picks = (0..union.len()).map(|row| {
            let position = children.iter().position(|(id, _)| *id == union.type_id(row)).ok_or_else(incompatible)?;
            Ok(children[position].1.as_ref().map(|_| (position, union.value_offset(row))))
        }).collect::<Result<Vec<_>>>()?;
        let array = copy_nullable_cells(&sources, &picks)?;
        let field = target.clone().with_nullable(target.is_nullable() || array.logical_null_count() != 0);
        return Ok((field, array));
    }
    if variable_list(source.data_type()) && variable_list(target.data_type())
        && !matches!((source.data_type(), target.data_type()), (DataType::List(_), DataType::List(_))) {
        return variable_list_column(array.as_ref(), source, target, event);
    }
    if matches!(source.data_type(), DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Dictionary(_, _))
        && matches!(target.data_type(), DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) {
        macro_rules! strings {
            ($builder:expr) => {{
                let mut builder = $builder;
                for row in 0..array.len() {
                    builder.append_option(crate::text::physical_text(array.as_ref(), row)?);
                }
                Arc::new(builder.finish()) as ArrayRef
            }};
        }
        if target.data_type() == &DataType::Utf8 {
            let bound = (0..array.len()).try_fold(0usize, |bytes, row| {
                let text = crate::text::physical_text(array.as_ref(), row)?;
                super::add(bytes, text.map_or(0, str::len))
            })?;
            i32::try_from(bound).map_err(|_| super::fault("page-allocation-size", "Utf8 column exceeds i32 offsets"))?;
        }
        let array = match target.data_type() {
            DataType::Utf8 => strings!(StringBuilder::new()),
            DataType::LargeUtf8 => strings!(arrow_array::builder::LargeStringBuilder::new()),
            DataType::Utf8View => strings!(arrow_array::builder::StringViewBuilder::new()),
            _ => unreachable!(),
        };
        let field = target.clone().with_nullable(target.is_nullable() || array.null_count() != 0);
        return Ok((field, array));
    }
    let data = array.to_data();
    let mut children = Vec::with_capacity(data.child_data().len());
    let data_type = match (source.data_type(), target.data_type()) {
        (DataType::Struct(source_fields), DataType::Struct(target_fields)) => {
            let fields = target_fields.iter().map(|target| {
                let (field, child) = match source_fields.iter().position(|source| source.name() == target.name()) {
                    Some(index) => relayout(make_array(data.child_data()[index].clone()), &source_fields[index], target, event)?,
                    None => {
                        let null = Field::new(target.name(), DataType::Null, true);
                        relayout(Arc::new(NullArray::new(data.len())), &null, target, event)?
                    }
                };
                children.push(child.to_data());
                Ok(field)
            }).collect::<Result<Vec<_>>>()?;
            DataType::Struct(fields.into())
        }
        (DataType::List(source), DataType::List(target)) => {
            let (field, child) = relayout(make_array(data.child_data()[0].clone()), source, target, event)?;
            children.push(child.to_data());
            DataType::List(Arc::new(field))
        }
        (DataType::FixedSizeList(source, count), DataType::FixedSizeList(target, target_count)) if count == target_count => {
            let (field, child) = relayout(make_array(data.child_data()[0].clone()), source, target, event)?;
            children.push(child.to_data());
            DataType::FixedSizeList(Arc::new(field), *count)
        }
        (DataType::Map(source, sorted), DataType::Map(target, target_sorted)) if sorted == target_sorted => {
            let (field, child) = relayout(make_array(data.child_data()[0].clone()), source, target, event)?;
            children.push(child.to_data());
            DataType::Map(Arc::new(field), *sorted)
        }
        (DataType::RunEndEncoded(source_ends, source_values), DataType::RunEndEncoded(target_ends, target_values)) => {
            let (ends, ends_array) = relayout(make_array(data.child_data()[0].clone()), source_ends, target_ends, event)?;
            let (values, values_array) = relayout(make_array(data.child_data()[1].clone()), source_values, target_values, event)?;
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

fn encode_runs<R: RunEndIndexType>(values: ArrayRef, data_type: &DataType) -> Result<ArrayRef> {
    use arrow_buffer::ArrowNativeType;
    let overflow = || super::fault("page-allocation-size", "Array exceeds its declared run-end width");
    R::Native::from_usize(values.len()).ok_or_else(overflow)?;
    let ends = (1..=values.len()).map(|end| R::Native::from_usize(end).ok_or_else(overflow)).collect::<Result<Vec<_>>>()?;
    let ends = arrow_array::PrimitiveArray::<R>::new(ScalarBuffer::from(ends), None);
    let data = ArrayData::builder(data_type.clone()).len(values.len())
        .child_data(vec![ends.to_data(), values.to_data()]).build().map_err(crate::arrow_error)?;
    Ok(make_array(data))
}

fn variable_list_column(array: &dyn Array, source: &Field, target: &Field, event: bool) -> Result<(Field, ArrayRef)> {
    let source_child = match source.data_type() {
        DataType::List(child) | DataType::LargeList(child) | DataType::ListView(child) | DataType::LargeListView(child) => child,
        _ => return Err(incompatible()),
    };
    let target_child = match target.data_type() {
        DataType::List(child) | DataType::LargeList(child) | DataType::ListView(child) | DataType::LargeListView(child) => child,
        _ => return Err(incompatible()),
    };
    let mut offsets = Vec::with_capacity(array.len() + 1);
    let mut picks = Vec::new();
    let mut nulls = NullBufferBuilder::new(array.len());
    for row in 0..array.len() {
        offsets.push(picks.len());
        nulls.append(array.is_valid(row));
        if array.is_valid(row) {
            let (_, _, span) = crate::list_span(source, array, row).ok_or_else(incompatible)?;
            picks.extend(span.map(|index| (0, index)));
        }
    }
    offsets.push(picks.len());
    let values = make_array(array.to_data().child_data()[0].clone());
    let values = copy_selected_cells(&[values.as_ref()], &picks)?;
    let (child, values) = relayout(values, source_child, target_child, event)?;
    let child = Arc::new(child);
    let nulls = nulls.finish();
    let info = |message| super::fault("page-allocation-size", message);
    let array = match target.data_type() {
        DataType::List(_) | DataType::ListView(_) => {
            let offsets = offsets.iter().map(|&offset| i32::try_from(offset).map_err(|_| info("List exceeds i32 offsets"))).collect::<Result<Vec<_>>>()?;
            if matches!(target.data_type(), DataType::ListView(_)) {
                let sizes = offsets.windows(2).map(|pair| pair[1] - pair[0]).collect::<Vec<_>>();
                let array = arrow_array::ListViewArray::try_new(child, offsets[..array.len()].to_vec().into(), sizes.into(), values, nulls);
                Arc::new(array.map_err(crate::arrow_error)?) as ArrayRef
            } else {
                let offsets = arrow_buffer::OffsetBuffer::new(offsets.into());
                Arc::new(arrow_array::ListArray::try_new(child, offsets, values, nulls).map_err(crate::arrow_error)?) as ArrayRef
            }
        }
        DataType::LargeList(_) | DataType::LargeListView(_) => {
            let offsets = offsets.iter().map(|&offset| i64::try_from(offset).map_err(|_| info("List exceeds i64 offsets"))).collect::<Result<Vec<_>>>()?;
            if matches!(target.data_type(), DataType::LargeListView(_)) {
                let sizes = offsets.windows(2).map(|pair| pair[1] - pair[0]).collect::<Vec<_>>();
                let array = arrow_array::LargeListViewArray::try_new(child, offsets[..array.len()].to_vec().into(), sizes.into(), values, nulls);
                Arc::new(array.map_err(crate::arrow_error)?) as ArrayRef
            } else {
                let offsets = arrow_buffer::OffsetBuffer::new(offsets.into());
                Arc::new(arrow_array::LargeListArray::try_new(child, offsets, values, nulls).map_err(crate::arrow_error)?) as ArrayRef
            }
        }
        _ => unreachable!(),
    };
    Ok((target.clone().with_data_type(array.data_type().clone()).with_nullable(target.is_nullable() || source.is_nullable()), array))
}

fn repack_union(fields: &UnionFields, mode: UnionMode) -> bool {
    if mode == UnionMode::Sparse { return true; }
    let mut names = std::collections::BTreeSet::new();
    fields.iter().any(|(_, field)| {
        field.data_type() != &DataType::Null && (from_field(field).is_ok_and(|ty| ty.nullable())
            || monomorphic_leg(field).is_ok_and(|name| !names.insert(name)))
    })
}

fn gather_union(array: &UnionArray, sources: &UnionFields, target: &Field, targets: &UnionFields, event: bool) -> Result<(Field, ArrayRef)> {
    let mut aligned: Vec<Vec<ArrayRef>> = vec![Vec::new(); targets.len()];
    let mut mapping: [Option<(usize, usize)>; 128] = [None; 128];
    let null = targets.iter().position(|(_, field)| field.data_type() == &DataType::Null);
    for (tag, field) in sources.iter() {
        if field.data_type() == &DataType::Null { continue; }
        let name = monomorphic_leg(field)?.ok_or_else(incompatible)?;
        let (position, _, target) = union_leg(targets, &name).ok_or_else(incompatible)?;
        let (_, values) = relayout(Arc::clone(array.child(tag)), field, target, event)?;
        mapping[tag as usize] = Some((position, aligned[position].len()));
        aligned[position].push(values);
    }
    if let Some(null) = null {
        aligned[null].push(Arc::new(NullArray::new(1)));
    }
    let mut selected = vec![Vec::new(); targets.len()];
    let mut tags = Vec::with_capacity(array.len());
    let mut offsets = Vec::with_capacity(array.len());
    let target_ids = targets.iter().map(|(id, _)| id).collect::<Vec<_>>();
    for row in 0..array.len() {
        let tag = array.type_id(row);
        let offset = array.value_offset(row);
        let (position, source, offset) = match mapping[tag as usize] {
            Some((position, source)) if aligned[position][source].is_valid(offset) => (position, source, offset),
            _ => (null.ok_or_else(incompatible)?, 0, 0),
        };
        tags.push(target_ids[position]);
        offsets.push(i32::try_from(selected[position].len())
            .map_err(|_| super::fault("page-allocation-size", "Union child exceeds i32 offsets"))?);
        selected[position].push((source, offset));
    }
    let children = targets.iter().zip(aligned).zip(selected).map(|(((_, field), arrays), selected)| {
        if selected.is_empty() { return Ok(arrow_array::new_empty_array(field.data_type())); }
        let arrays = arrays.iter().map(|array| array.as_ref()).collect::<Vec<_>>();
        copy_selected_cells(&arrays, &selected)
    }).collect::<Result<Vec<_>>>()?;
    let array = UnionArray::try_new(targets.clone(), tags.into(), Some(offsets.into()), children).map_err(crate::arrow_error)?;
    Ok((target.clone(), Arc::new(array)))
}

fn union(array: ArrayRef, source: &Field, target: &Field, targets: &UnionFields, event: bool) -> Result<(Field, ArrayRef)> {
    let canonical = array.as_any().downcast_ref::<UnionArray>();
    let source_fields = match source.data_type() { DataType::Union(fields, _) => Some(fields), _ => None };
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
        let (field, child) = relayout(child, &source_field, target_field, event)?;
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
        monomorphic_leg(field).is_ok_and(|leg| leg.as_deref() == Some(name) || (field.data_type() == &DataType::Null && name == "null")).then_some((index, id, field))
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
        Value::FixedSizeList(vec![Value::FixedSizeBinary(vec![9; width as usize])]),
        Value::FixedSizeList(vec![Value::I64(42)]),
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
