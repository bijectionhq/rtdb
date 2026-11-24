use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, cast::AsArray, types::{Int16Type, Int32Type, Int64Type}};
use arrow_schema::{DataType, Field, UnionMode};
use rtdb_columnar::{BatchBuilder, EXTENSION_NAME_KEY, copy_event_cells, copy_selected_cells, estimate_copy_cell_bytes,
    estimate_event_copy_cell_bytes, estimate_event_null_cell_bytes, event_relayout_bound, from_field, relayout_event_column};
use rtdb_types::{LogicalType, Result, UriParts};

use super::{EncodedEvent, PutColumn, event_size};

struct Source<'a> {
    field: &'a Field,
    array: &'a dyn Array,
    relayout: bool,
    picks: Vec<(usize, usize)>,
}

pub(super) fn gather_column<E: EncodedEvent>(
    documents: &[&E], target: &Field, logical: &LogicalType, mut builder: Option<&mut BatchBuilder>,
) -> Result<Option<ArrayRef>> {
    if documents.first().is_none_or(|doc| doc.put_column(target.name()).is_none()) {
        return Ok(None);
    }
    let target_type = logical.unencoded();
    // Persisted runs coalesce equal values across page boundaries; gathering run positions does not do that.
    if target_type != *logical { return Ok(None); }
    let schema_bytes = schema_bound(target)?;
    let base = documents.len().checked_mul(256).and_then(|bytes| bytes.checked_add(schema_bytes)).ok_or_else(event_size)?;
    let _plan = builder.as_deref().map(|builder| builder.reserve_scratch(base)).transpose()?;
    let mut slots = HashMap::new();
    let mut sources: Vec<Source<'_>> = Vec::new();
    let mut cells = Vec::with_capacity(documents.len());
    for document in documents {
        match document.put_column(target.name()) {
            Some(PutColumn::Absent) if logical.nullable() || *logical == LogicalType::Nothing => cells.push(None),
            Some(PutColumn::Stored(field, array, row)) => {
                let key = (std::ptr::from_ref(field), std::ptr::from_ref(array));
                let slot = if let Some(&slot) = slots.get(&key) {
                    slot
                } else {
                    let _schema = builder.as_deref().map(|builder| builder.reserve_scratch(schema_bound(field)?)).transpose()?;
                    let stored = from_field(field)?;
                    if field.data_type() != array.data_type()
                        || (*logical != LogicalType::Nothing && stored == LogicalType::Nothing)
                        || stored.unencoded().join(&target_type) != target_type
                    {
                        return Ok(None);
                    }
                    let relayout = field.data_type() != target.data_type() || field.metadata() != target.metadata();
                    let slot = sources.len();
                    sources.push(Source { field, array, relayout, picks: Vec::new() });
                    slots.insert(key, slot);
                    slot
                };
                if row >= array.len() || (!logical.nullable() && array.is_null(row))
                    || !valid_cell(field, array, row, true)
                {
                    return Ok(None);
                }
                let source = &mut sources[slot];
                let row = if source.relayout {
                    let position = source.picks.len();
                    source.picks.push((0, row));
                    position
                } else { row };
                cells.push(Some((slot, row)));
            }
            _ => return Ok(None),
        }
    }

    let mut scratch = Vec::new();
    let mut aligned = Vec::with_capacity(sources.len());
    for source in &sources {
        if !source.relayout {
            aligned.push(None);
            continue;
        }
        if let Some(builder) = builder.as_deref() {
            let bound = source.picks.iter().try_fold(schema_bound(source.field)?, |bytes, &(_, row)| {
                add_copy_bound(bytes, estimate_copy_cell_bytes(source.field, source.array, row)?)
            })?;
            scratch.push(builder.reserve_scratch(bound)?);
        }
        let Ok(selection) = copy_selected_cells(&[source.array], &source.picks) else {
            return Ok(None);
        };
        if let Some(builder) = builder.as_deref() {
            let bound = event_relayout_bound(selection.as_ref(), source.field, target)?;
            scratch.push(builder.reserve_scratch(bound)?);
        }
        let Ok((field, array)) = relayout_event_column(selection, source.field, target) else {
            return Ok(None);
        };
        if field.data_type() != target.data_type() {
            return Ok(None);
        }
        aligned.push(Some(array));
    }
    let mut arrays: Vec<&dyn Array> = sources.iter().zip(&aligned)
        .map(|(source, aligned)| aligned.as_deref().unwrap_or(source.array)).collect();
    let empty;
    if arrays.is_empty() {
        empty = arrow_array::new_empty_array(target.data_type());
        arrays.push(empty.as_ref());
    }
    let build = || {
        if let [Some(array)] = aligned.as_slice()
            && sources[0].picks.len() == cells.len()
            && compact_event_layout(array.as_ref(), false)
        {
            return Ok(Arc::clone(array));
        }
        copy_event_cells(&arrays, &cells)
    };
    let array = if let Some(builder) = builder.as_mut() {
        let bound = cells.iter().try_fold(schema_bound(target)?, |bytes, cell| {
            let cell_size = match *cell {
                Some((source, row)) => estimate_event_copy_cell_bytes(target, arrays[source], row)?,
                None => estimate_event_null_cell_bytes(target)?,
            };
            add_copy_bound(bytes, cell_size)
        })?;
        builder.column(bound, build)?
    } else {
        build()?
    };
    if !logical.nullable() && *logical != LogicalType::Nothing && array.logical_null_count() != 0 {
        return Ok(None);
    }
    Ok(Some(array))
}

fn compact_event_layout(array: &dyn Array, masked: bool) -> bool {
    match array.data_type() {
        DataType::Union(fields, UnionMode::Dense) => {
            // A final event gather can change the union tags masked by a null parent.
            if masked { return false; }
            let union = array.as_union();
            let mut lengths = [0; 128];
            for row in 0..union.len() {
                let length = &mut lengths[union.type_id(row) as usize];
                if union.value_offset(row) != *length { return false; }
                *length += 1;
            }
            fields.iter().all(|(id, _)| {
                let child = union.child(id);
                child.len() == lengths[id as usize] && compact_event_layout(child.as_ref(), false)
            })
        }
        DataType::Struct(_) => array.as_struct().columns().iter()
            .all(|child| compact_event_layout(child.as_ref(), masked || array.null_count() != 0)),
        DataType::FixedSizeList(..) => compact_event_layout(
            array.as_fixed_size_list().values().as_ref(), masked || array.null_count() != 0),
        DataType::List(_) => compact_event_layout(array.as_list::<i32>().values().as_ref(), false),
        DataType::Map(..) => {
            let entries = array.as_map().entries();
            entries.column(0).logical_null_count() == 0 && compact_event_layout(entries, false)
        }
        DataType::RunEndEncoded(..) => false,
        _ => true,
    }
}

fn schema_bound(field: &Field) -> Result<usize> {
    field.size().checked_mul(8).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(event_size)
}

fn add_copy_bound(bytes: usize, cell: usize) -> Result<usize> {
    cell.checked_mul(4).and_then(|cell| bytes.checked_add(cell)).ok_or_else(event_size)
}

pub(super) fn valid_cell(field: &Field, array: &dyn Array, row: usize, bottom: bool) -> bool {
    let extension = field.metadata().get(EXTENSION_NAME_KEY).map(String::as_str);
    match field.data_type() {
        DataType::Null => field.is_nullable() || bottom,
        _ if array.is_null(row) => true,
        DataType::Utf8 if extension == Some("uri") => UriParts::parse(array.as_string::<i32>().value(row)).is_ok(),
        DataType::Union(fields, UnionMode::Dense) => {
            let union = array.as_union();
            let tag = union.type_id(row);
            let Some((_, field)) = fields.iter().find(|(id, _)| *id == tag) else { return false };
            let child = union.child(tag).as_ref();
            let offset = union.value_offset(row);
            if field.data_type() == &DataType::Null { return field.is_nullable(); }
            child.is_valid(offset) && valid_cell(field, child, offset, false)
        }
        DataType::Struct(fields) => fields.iter().zip(array.as_struct().columns())
            .all(|(field, child)| valid_cell(field, child.as_ref(), row, true)),
        DataType::List(field) => {
            let list = array.as_list::<i32>();
            let offsets = list.value_offsets();
            (offsets[row]..offsets[row + 1]).all(|index| valid_cell(field, list.values().as_ref(), index as usize, false))
        }
        DataType::FixedSizeList(item, size) => {
            let list = array.as_fixed_size_list();
            let start = row * *size as usize;
            let values = list.values();
            if extension == Some("xt/tstz-range") {
                return matches!(item.data_type(), DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some(zone)) if zone.as_ref() == "UTC")
                    && values.is_valid(start) && values.is_valid(start + 1);
            }
            (start..start + *size as usize).all(|index| valid_cell(item, values.as_ref(), index, false))
        }
        DataType::Map(field, _) => {
            let map = array.as_map();
            let offsets = map.value_offsets();
            (offsets[row]..offsets[row + 1]).all(|index| valid_cell(field, map.entries(), index as usize, false))
        }
        DataType::RunEndEncoded(ends, field) => {
            macro_rules! run {
                ($ty:ty) => {{
                    let runs = array.as_run::<$ty>();
                    valid_cell(field, runs.values().as_ref(), runs.get_physical_index(row), false)
                }};
            }
            match ends.data_type() {
                DataType::Int16 => run!(Int16Type),
                DataType::Int32 => run!(Int32Type),
                DataType::Int64 => run!(Int64Type),
                _ => false,
            }
        }
        _ => true,
    }
}
