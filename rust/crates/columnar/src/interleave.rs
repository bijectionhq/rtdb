use std::{fmt::Display, sync::Arc};
use arrow_array::{Array, ArrayRef, FixedSizeListArray, ListArray, MapArray, UnionArray};
use arrow_buffer::{NullBuffer, NullBufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, UnionFields};
use rtdb_types::{Anomaly, Result};

/// Interleaves cells of arrays sharing one layout; a dense union gathers each leg's children in bulk, where Arrow's
/// generic path would extend the union one cell at a time.
pub fn interleave(arrays: &[&dyn Array], cells: &[(usize, usize)]) -> Result<ArrayRef> {
    if let Some(&(source, start)) = cells.first()
        && let Some(array) = arrays.get(source)
        && start.checked_add(cells.len()).is_some_and(|end| end <= array.len())
        && cells.iter().enumerate().all(|(offset, &(index, row))| index == source && row == start + offset) {
        return Ok(array.slice(start, cells.len()));
    }
    match arrays.first().map(|array| array.data_type()) {
        Some(DataType::Union(fields, arrow_schema::UnionMode::Dense)) => interleave_union(fields, arrays, cells, interleave),
        _ => arrow_select::interleave::interleave(arrays, cells).map_err(|error| invalid(error)),
    }
}

/// Copies selected cells into Arrow arrays, allocating dense-union children according to their selected positions.
/// Sources have the same layout, and every selection names a row within its source.
pub fn copy_selected_cells(arrays: &[&dyn Array], cells: &[(usize, usize)]) -> Result<ArrayRef> {
    use arrow_array::cast::AsArray;
    let layout = arrays.first().map(|array| array.data_type()).ok_or_else(|| invalid("Nothing to interleave"))?;
    if arrays.iter().any(|array| array.data_type() != layout) {
        return Err(invalid("Selected Arrow layouts differ"));
    }
    let error = |error| invalid(error);
    match layout {
        DataType::Union(fields, arrow_schema::UnionMode::Dense) => interleave_union(fields, arrays, cells, copy_selected_cells),
        DataType::Struct(fields) => {
            let structs: Vec<_> = arrays.iter().map(|array| array.as_struct()).collect();
            let columns = (0..fields.len()).map(|position| {
                let inputs = structs.iter().map(|array| array.column(position).as_ref()).collect::<Vec<_>>();
                copy_selected_cells(&inputs, cells)
            }).collect::<Result<Vec<_>>>()?;
            crate::build_struct_array(fields.clone(), &columns, cells.len(), interleave_validity(arrays, cells))
        }
        DataType::List(field) => {
            let lists = arrays.iter().map(|array| array.as_list::<i32>()).map(|array| (array.value_offsets(), array.values().as_ref()));
            let (offsets, values) = interleave_list_values(lists, cells)?;
            Ok(Arc::new(ListArray::try_new(Arc::clone(field), offsets, values, interleave_validity(arrays, cells)).map_err(error)?))
        }
        DataType::FixedSizeList(field, width) => {
            let width = usize::try_from(*width).map_err(|_| invalid("Negative fixed-size list width"))?;
            let lists = arrays.iter().map(|array| array.as_fixed_size_list().values().as_ref()).collect::<Vec<_>>();
            let count = cells.len().checked_mul(width).ok_or_else(size_error)?;
            let mut picks = Vec::with_capacity(count);
            for &(source, row) in cells {
                let start = row.checked_mul(width).ok_or_else(size_error)?;
                let end = start.checked_add(width).ok_or_else(size_error)?;
                picks.extend((start..end).map(|index| (source, index)));
            }
            let values = copy_selected_cells(&lists, &picks)?;
            Ok(Arc::new(FixedSizeListArray::try_new_with_length(Arc::clone(field), width as i32, values, interleave_validity(arrays, cells), cells.len()).map_err(error)?))
        }
        DataType::Map(field, sorted) => {
            let maps = arrays.iter().map(|array| array.as_map()).map(|array| (array.value_offsets(), array.entries() as &dyn Array));
            let (offsets, values) = interleave_list_values(maps, cells)?;
            let entries = values.as_struct();
            if entries.column(0).logical_null_count() != 0 { return Err(invalid("Arrow map keys cannot be null")); }
            Ok(Arc::new(MapArray::try_new(Arc::clone(field), offsets, entries.clone(), interleave_validity(arrays, cells), *sorted).map_err(error)?))
        }
        _ => arrow_select::interleave::interleave(arrays, cells).map_err(error),
    }
}

fn interleave_union(fields: &UnionFields, arrays: &[&dyn Array], cells: &[(usize, usize)],
    select: fn(&[&dyn Array], &[(usize, usize)]) -> Result<ArrayRef>) -> Result<ArrayRef> {
    let unions = arrays.iter().map(|array| array.as_any().downcast_ref::<UnionArray>().ok_or_else(|| invalid("A union source is not a union"))).collect::<Result<Vec<_>>>()?;
    let mut picks: Vec<Vec<(usize, usize)>> = vec![Vec::new(); fields.iter().map(|(id, _)| id as usize + 1).max().unwrap_or(0)];
    let mut ids = Vec::with_capacity(cells.len());
    let mut offsets = Vec::with_capacity(cells.len());
    for &(source, row) in cells {
        let union = unions[source];
        let id = union.type_id(row);
        let leg = picks.get_mut(usize::try_from(id).map_err(|_| invalid("A union cell names no leg"))?).ok_or_else(|| invalid("A union cell names no leg"))?;
        offsets.push(i32::try_from(leg.len()).map_err(|_| invalid("A union leg exceeds i32 offsets"))?);
        leg.push((source, union.value_offset(row)));
        ids.push(id);
    }
    let children = fields.iter().map(|(id, _)| {
        let children: Vec<&dyn Array> = unions.iter().map(|union| union.child(id).as_ref()).collect();
        select(&children, &picks[id as usize])
    }).collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(UnionArray::try_new(fields.clone(), ScalarBuffer::from(ids), Some(ScalarBuffer::from(offsets)), children)
        .map_err(|error| invalid(error))?))
}

fn interleave_validity(arrays: &[&dyn Array], cells: &[(usize, usize)]) -> Option<NullBuffer> {
    let mut validity = NullBufferBuilder::new(cells.len());
    for &(source, row) in cells { validity.append(arrays[source].is_valid(row)); }
    validity.finish()
}

fn interleave_list_values<'a>(lists: impl Iterator<Item = (&'a [i32], &'a dyn Array)>, cells: &[(usize, usize)])
    -> Result<(OffsetBuffer<i32>, ArrayRef)> {
    let (offsets, values): (Vec<_>, Vec<_>) = lists.unzip();
    let mut picks = Vec::new();
    let mut selected = Vec::with_capacity(cells.len().checked_add(1).ok_or_else(size_error)?);
    selected.push(0);
    for &(source, row) in cells {
        let start = offsets[source][row] as usize;
        let end = offsets[source][row + 1] as usize;
        let count = picks.len().checked_add(end.checked_sub(start).ok_or_else(size_error)?).ok_or_else(size_error)?;
        selected.push(i32::try_from(count).map_err(|_| size_error())?);
        picks.extend((start..end).map(|index| (source, index)));
    }
    let values = copy_selected_cells(&values, &picks)?;
    Ok((OffsetBuffer::new(ScalarBuffer::from(selected)), values))
}

fn invalid(error: impl Display) -> Anomaly { Anomaly::incorrect("rtdb/column-selection", error.to_string()) }
fn size_error() -> Anomaly { invalid("Arrow selection exceeds addressable memory") }
