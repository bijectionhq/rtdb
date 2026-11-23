use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{Array, ArrayRef, UnionArray, cast::AsArray};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Field, UnionFields, UnionMode};
use rtdb_columnar::{MemoryBudget, arrow_relayout_bound, copy_selected_cells, estimate_copy_cell_bytes, from_field, leg_name, relayout_arrow_column, retain_array, retain_buffer, to_field};
use rtdb_types::{Anomaly, LogicalType, Result};

use super::{normalized_document_array, normalized_field, overflow};

pub(super) fn normalize(input: &ArrayRef, memory: &Arc<MemoryBudget>) -> Result<Option<ArrayRef>> {
    let union = input.as_union();
    let DataType::Union(fields, UnionMode::Dense) = union.data_type() else { return Ok(None); };
    let mut names = BTreeMap::new();
    let mut repackage = false;
    let mut changed = false;
    let mut distinct_layouts = false;
    let mut sources: Vec<Child> = Vec::with_capacity(fields.len());
    for (tag, field) in fields.iter() {
        let original = union.child(tag);
        let Some(array) = normalized_document_array(original, memory, true)? else { return Ok(None); };
        changed |= !Arc::ptr_eq(&array, original);
        let field = normalized_field(field, &array);
        let field = if !matches!(field.data_type(), DataType::Null | DataType::Union(..)) && array.null_count() != 0 {
            field.with_nullable(true)
        } else {
            field
        };
        let logical = from_field(&field)?;
        let plain = match &logical { LogicalType::Maybe(value) => value.as_ref(), plain => plain };
        if matches!(plain, LogicalType::Union(_) | LogicalType::Nothing) {
            let field = Field::new("", union.data_type().clone(), false);
            return Ok(super::ArrowDocuments::compatible(&field, false).then(|| Arc::clone(input)));
        }
        let name = leg_name(plain)?;
        if let Some(previous) = names.insert(name.clone(), plain.clone()) {
            distinct_layouts |= previous != *plain;
            repackage = true;
        }
        repackage |= plain != &LogicalType::Null && logical.nullable();
        sources.push((tag, field, array, logical, name));
    }
    let logical = LogicalType::join_all(sources.iter().map(|(_, _, _, ty, _)| ty));
    if distinct_layouts {
        let field = Field::new("", union.data_type().clone(), false);
        return Ok(super::ArrowDocuments::compatible(&field, false).then(|| Arc::clone(input)));
    }
    if matches!(logical, LogicalType::Union(_)) && !repackage {
        if !changed { return Ok(Some(Arc::clone(input))); }
        let fields = UnionFields::try_new(sources.iter().map(|(tag, ..)| *tag), sources.iter().map(|(_, field, ..)| field.clone())).map_err(invalid)?;
        let children = sources.into_iter().map(|(_, _, array, ..)| array).collect();
        let array = UnionArray::try_new(fields, union.type_ids().clone(), union.offsets().cloned(), children).map_err(invalid)?;
        return Ok(Some(Arc::new(array)));
    }

    let target = to_field("", &logical)?;
    if target.data_type() == &DataType::Null {
        return Ok(Some(arrow_array::new_null_array(&DataType::Null, union.len())));
    }
    let targets: Vec<_> = match target.data_type() {
        DataType::Union(fields, UnionMode::Dense) => fields.iter().map(|(_, field)| field.as_ref()).collect(),
        _ => vec![&target],
    };
    let bytes = union.len().checked_mul(64).and_then(|bytes| bytes.checked_add(target.size().checked_mul(8)?))
        .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
    let _scratch = memory.reserve(bytes)?;
    let null_target = if targets.len() == 1 && target.is_nullable() { Some(0) }
        else { targets.iter().position(|field| field.data_type() == &DataType::Null) };
    let mut mappings = [None; 128];
    for (source, (tag, _, _, _, name)) in sources.iter().enumerate() {
        let target = if targets.len() == 1 { Some(0) } else { targets.iter().position(|field| field.name() == name) };
        mappings[*tag as usize] = Some((source, target.ok_or_else(|| invalid("Dense union child is missing from its canonical layout"))?));
    }

    let mut picks = vec![Vec::new(); targets.len()];
    let mut tags = Vec::new();
    let mut offsets = Vec::new();
    if targets.len() > 1 {
        tags.reserve_exact(union.len());
        offsets.reserve_exact(union.len());
    }
    for row in 0..union.len() {
        let (source, target) = mappings[union.type_id(row) as usize].ok_or_else(|| invalid("Dense union row has no child"))?;
        let array = &sources[source].2;
        let offset = union.value_offset(row);
        let (source, target, offset) = if array.data_type() == &DataType::Null || array.is_null(offset) {
            (sources.len(), null_target.ok_or_else(|| invalid("Dense union has no canonical null slot"))?, 0)
        } else {
            (source, target, offset)
        };
        if targets.len() > 1 {
            tags.push(target as i8);
            offsets.push(i32::try_from(picks[target].len()).map_err(|_| overflow())?);
        }
        picks[target].push((source, offset));
    }
    let children = targets.into_iter().zip(picks.iter_mut()).map(|(field, picks)| {
        copy_children(field, picks, &sources, memory)
    }).collect::<Result<Vec<_>>>()?;
    let DataType::Union(fields, UnionMode::Dense) = target.data_type() else {
        return Ok(children.into_iter().next());
    };

    let bytes = union.len().checked_mul(5).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?;
    let owner = memory.reserve(bytes)?;
    let tags = ScalarBuffer::from(tags).into_inner();
    let offsets = ScalarBuffer::from(offsets).into_inner();
    let tags = ScalarBuffer::from(retain_buffer(&tags, &owner));
    let offsets = ScalarBuffer::from(retain_buffer(&offsets, &owner));
    let array = UnionArray::try_new(fields.clone(), tags, Some(offsets), children).map_err(invalid)?;
    Ok(Some(Arc::new(array)))
}

type Child = (i8, Field, ArrayRef, LogicalType, String);

fn copy_children(target: &Field, picks: &mut [(usize, usize)], sources: &[Child], memory: &Arc<MemoryBudget>) -> Result<ArrayRef> {
    if picks.is_empty() { return Ok(arrow_array::new_empty_array(target.data_type())); }
    let mut arrays = Vec::new();
    let mut reservations = Vec::new();
    let mut positions = vec![usize::MAX; sources.len() + 1];
    for (source, _) in picks.iter_mut() {
        if positions[*source] == usize::MAX {
            let array = match sources.get(*source) {
                Some((_, field, array, ..)) if field.data_type() == target.data_type() => Arc::clone(array),
                Some((_, field, array, ..)) => {
                    let bound = arrow_relayout_bound(array.as_ref(), field, target)?;
                    reservations.push(memory.reserve(bound)?);
                    let (physical, child) = relayout_arrow_column(Arc::clone(array), field, target)?;
                    if physical.data_type() != target.data_type() { return Err(invalid("Dense union child differs from its canonical layout")); }
                    child
                }
                None => {
                    let null = Field::new(target.name(), DataType::Null, true);
                    let array = Arc::new(arrow_array::NullArray::new(1));
                    reservations.push(memory.reserve(arrow_relayout_bound(array.as_ref(), &null, target)?)?);
                    relayout_arrow_column(array, &null, target)?.1
                }
            };
            positions[*source] = arrays.len();
            arrays.push(array);
        }
        *source = positions[*source];
    }
    if arrays.len() == 1 && reservations.is_empty() && picks.len() == arrays[0].len()
        && picks.iter().enumerate().all(|(index, &(_, row))| index == row) {
        return Ok(arrays.pop().expect("one active union child"));
    }
    let base = target.size().checked_mul(8).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
    let bound = picks.iter().try_fold(base, |bytes, &(source, row)| {
        let cell = estimate_copy_cell_bytes(target, arrays[source].as_ref(), row)?;
        cell.checked_mul(4).and_then(|cell| bytes.checked_add(cell)).ok_or_else(overflow)
    })?;
    let mut reservation = memory.reserve(bound)?;
    let arrays = arrays.iter().map(|array| array.as_ref()).collect::<Vec<_>>();
    let output = copy_selected_cells(&arrays, picks)?;
    let retained = output.get_array_memory_size();
    let owner = Arc::get_mut(&mut reservation).expect("A canonical union child owns its reservation");
    if retained > owner.bytes() { owner.grow_to(retained)?; } else { owner.shrink_to(retained)?; }
    retain_array(output, &reservation)
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("rtdb.tx/document-schema", error.to_string())
}
