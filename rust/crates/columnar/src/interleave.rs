use std::{fmt::Display, sync::Arc};
use arrow_array::{Array, ArrayRef, MapArray, PrimitiveArray, RunArray, UnionArray, make_array, new_null_array, types::{Int16Type, Int32Type, Int64Type, RunEndIndexType}};
use arrow_buffer::{ArrowNativeType, NullBuffer, NullBufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, UnionFields};
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
        Some(DataType::Union(fields, arrow_schema::UnionMode::Dense)) => interleave_union(fields, arrays, cells, Padding::Null, interleave),
        _ => arrow_select::interleave::interleave(arrays, cells).map_err(|error| invalid(error)),
    }
}

/// Copies selected Arrow cells, allocating dense-union and run-end children according to their selected positions.
/// Sources have the same layout, and every selection names a row within its source.
pub fn copy_selected_cells(arrays: &[&dyn Array], cells: &[(usize, usize)]) -> Result<ArrayRef> {
    let cells = cells.iter().copied().map(Some).collect::<Vec<_>>();
    copy_nullable_cells(arrays, &cells)
}

/// Copies selected Arrow cells with typed null slots for missing cells.
pub fn copy_nullable_cells(arrays: &[&dyn Array], cells: &[Option<(usize, usize)>]) -> Result<ArrayRef> {
    copy_cells(arrays, cells, Padding::Null)
}

/// Copies cells into persisted event columns, keeping the legacy undefined slots beneath null parents.
pub fn copy_event_cells(arrays: &[&dyn Array], cells: &[Option<(usize, usize)>]) -> Result<ArrayRef> {
    copy_cells(arrays, cells, Padding::Event)
}

pub(crate) fn undefined_cells(data_type: &DataType, rows: usize) -> Result<ArrayRef> {
    let empty = arrow_array::new_empty_array(data_type);
    copy_nested_cells(&[empty.as_ref()], &vec![None; rows], Padding::Undefined)
}

fn copy_cells(arrays: &[&dyn Array], cells: &[Cell], padding: Padding) -> Result<ArrayRef> {
    let layout = arrays.first().map(|array| array.data_type()).ok_or_else(|| invalid("Nothing to interleave"))?;
    if arrays.iter().any(|array| array.data_type() != layout) {
        return Err(invalid("Selected Arrow layouts differ"));
    }
    copy_nested_cells(arrays, cells, padding)
}

type Cell = Option<(usize, usize)>;

#[derive(Clone, Copy)]
pub(crate) enum Padding {
    Null,
    Event,
    Undefined,
}

impl Padding {
    pub(crate) fn under_null_parent(self) -> Self {
        match self {
            Self::Null => Self::Null,
            Self::Event | Self::Undefined => Self::Undefined,
        }
    }

    pub(crate) fn union_leg(self, fields: &UnionFields) -> Option<(i8, &Arc<Field>)> {
        match self {
            Self::Undefined => fields.iter().next(),
            Self::Null | Self::Event => fields.iter().find(|(_, field)| field.data_type() == &DataType::Null).or_else(|| fields.iter().next()),
        }
    }
}

fn copy_nested_cells(arrays: &[&dyn Array], cells: &[Cell], padding: Padding) -> Result<ArrayRef> {
    use arrow_array::cast::AsArray;
    let layout = arrays[0].data_type();
    match layout {
        DataType::Union(fields, arrow_schema::UnionMode::Dense) =>
            interleave_union(fields, arrays, cells, padding, |children, picks| copy_nested_cells(children, picks, padding)),
        DataType::Struct(fields) => {
            let structs: Vec<_> = arrays.iter().map(|array| array.as_struct()).collect();
            let visible = visible_cells(arrays, cells);
            let columns = (0..fields.len()).map(|position| {
                let inputs = structs.iter().map(|array| array.column(position).as_ref()).collect::<Vec<_>>();
                copy_nested_cells(&inputs, &visible, padding.under_null_parent())
            }).collect::<Result<Vec<_>>>()?;
            crate::build_struct_array(fields.clone(), &columns, cells.len(), interleave_validity(&visible))
        }
        DataType::List(_) => {
            let lists = arrays.iter().map(|array| array.as_list::<i32>()).map(|array| (array.value_offsets(), array.values().as_ref()));
            let visible = visible_cells(arrays, cells);
            let (offsets, values) = interleave_list_values(lists, &visible, padding)?;
            // A union's explicit null leg does not require a physical null bitmap on the list child.
            let data = ArrayData::builder(layout.clone()).len(cells.len()).nulls(interleave_validity(&visible))
                .add_buffer(offsets.into_inner().into_inner()).add_child_data(values.to_data()).build().map_err(invalid)?;
            Ok(make_array(data))
        }
        DataType::FixedSizeList(_, width) => {
            let width = usize::try_from(*width).map_err(|_| invalid("Negative fixed-size list width"))?;
            let lists = arrays.iter().map(|array| array.as_fixed_size_list().values().as_ref()).collect::<Vec<_>>();
            let count = cells.len().checked_mul(width).ok_or_else(size_error)?;
            let mut picks = Vec::with_capacity(count);
            let visible = visible_cells(arrays, cells);
            for cell in &visible {
                if let Some((source, row)) = cell {
                    let start = row.checked_mul(width).ok_or_else(size_error)?;
                    let end = start.checked_add(width).ok_or_else(size_error)?;
                    picks.extend((start..end).map(|index| Some((*source, index))));
                } else {
                    picks.extend(std::iter::repeat_n(None, width));
                }
            }
            let values = copy_nested_cells(&lists, &picks, padding.under_null_parent())?;
            let data = ArrayData::builder(layout.clone()).len(cells.len()).nulls(interleave_validity(&visible))
                .add_child_data(values.to_data()).build().map_err(invalid)?;
            Ok(make_array(data))
        }
        DataType::Map(field, sorted) => {
            let maps = arrays.iter().map(|array| array.as_map()).map(|array| (array.value_offsets(), array.entries() as &dyn Array));
            let visible = visible_cells(arrays, cells);
            let (offsets, values) = interleave_list_values(maps, &visible, padding)?;
            let entries = values.as_struct();
            if entries.column(0).logical_null_count() != 0 { return Err(invalid("Arrow map keys cannot be null")); }
            Ok(Arc::new(MapArray::try_new(Arc::clone(field), offsets, entries.clone(), interleave_validity(&visible), *sorted).map_err(invalid)?))
        }
        DataType::RunEndEncoded(ends, _) => match ends.data_type() {
            DataType::Int16 => copy_runs::<Int16Type>(arrays, cells, padding),
            DataType::Int32 => copy_runs::<Int32Type>(arrays, cells, padding),
            DataType::Int64 => copy_runs::<Int64Type>(arrays, cells, padding),
            _ => Err(invalid("A run-end array requires signed 16-, 32- or 64-bit run ends")),
        },
        _ => {
            let mut null_cell = None;
            let picks = cells.iter().map(|cell| {
                if let Some((source, row)) = cell
                    && arrays[*source].is_valid(*row) {
                    (*source, *row)
                } else {
                    null_cell.get_or_insert_with(|| new_null_array(layout, 1));
                    (arrays.len(), 0)
                }
            }).collect::<Vec<_>>();
            let mut inputs = arrays.to_vec();
            if let Some(ref null) = null_cell { inputs.push(null.as_ref()); }
            arrow_select::interleave::interleave(&inputs, &picks).map_err(invalid)
        }
    }
}

fn copy_runs<R: RunEndIndexType>(arrays: &[&dyn Array], cells: &[Cell], padding: Padding) -> Result<ArrayRef> {
    let end = |value| R::Native::from_usize(value).ok_or_else(|| {
        Anomaly::incorrect("rtdb/run-end-selection-width", "Selected rows exceed the run-end width")
    });
    end(cells.len())?;
    let runs = arrays.iter().map(|array| {
        array.as_any().downcast_ref::<RunArray<R>>().ok_or_else(|| invalid("A run-end source is not a run array"))
    }).collect::<Result<Vec<_>>>()?;
    let inputs = runs.iter().map(|array| array.values().as_ref()).collect::<Vec<_>>();
    let mut picks = Vec::new();
    let mut ends = Vec::new();
    for (index, cell) in cells.iter().enumerate() {
        let physical = cell.map(|(source, row)| (source, runs[source].get_physical_index(row)));
        if picks.last() != Some(&physical) {
            if !picks.is_empty() { ends.push(end(index)?); }
            picks.push(physical);
        }
    }
    if !picks.is_empty() { ends.push(end(cells.len())?); }
    let values = copy_nested_cells(&inputs, &picks, padding)?;
    let ends = PrimitiveArray::<R>::new(ScalarBuffer::from(ends), None);
    let data = ArrayData::builder(arrays[0].data_type().clone()).len(cells.len())
        .add_child_data(ends.to_data()).add_child_data(values.to_data()).build().map_err(invalid)?;
    Ok(make_array(data))
}

fn interleave_union<C>(fields: &UnionFields, arrays: &[&dyn Array], cells: &[C], padding: Padding,
    select: impl Fn(&[&dyn Array], &[C]) -> Result<ArrayRef>) -> Result<ArrayRef>
where C: Copy + From<(usize, usize)> + Into<Cell> {
    let unions = arrays.iter().map(|array| array.as_any().downcast_ref::<UnionArray>().ok_or_else(|| invalid("A union source is not a union"))).collect::<Result<Vec<_>>>()?;
    let mut picks: Vec<Vec<C>> = vec![Vec::new(); fields.iter().map(|(id, _)| id as usize + 1).max().unwrap_or(0)];
    let mut ids = Vec::with_capacity(cells.len());
    let mut offsets = Vec::with_capacity(cells.len());
    let masked_leg = padding.union_leg(fields).map(|(id, _)| id);
    for &cell in cells {
        let (id, pick) = if let Some((source, row)) = cell.into() {
            let union = unions[source];
            (union.type_id(row), C::from((source, union.value_offset(row))))
        } else {
            (masked_leg.ok_or_else(|| invalid("A masked union cell needs a leg"))?, cell)
        };
        let leg = picks.get_mut(usize::try_from(id).map_err(|_| invalid("A union cell names no leg"))?).ok_or_else(|| invalid("A union cell names no leg"))?;
        offsets.push(i32::try_from(leg.len()).map_err(|_| invalid("A union leg exceeds i32 offsets"))?);
        leg.push(pick);
        ids.push(id);
    }
    let children = fields.iter().map(|(id, _)| {
        let children: Vec<&dyn Array> = unions.iter().map(|union| union.child(id).as_ref()).collect();
        select(&children, &picks[id as usize])
    }).collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(UnionArray::try_new(fields.clone(), ScalarBuffer::from(ids), Some(ScalarBuffer::from(offsets)), children)
        .map_err(|error| invalid(error))?))
}

fn visible_cells(arrays: &[&dyn Array], cells: &[Cell]) -> Vec<Cell> {
    cells.iter().map(|cell| cell.filter(|&(source, row)| arrays[source].is_valid(row))).collect()
}

fn interleave_validity(cells: &[Cell]) -> Option<NullBuffer> {
    let mut validity = NullBufferBuilder::new(cells.len());
    for cell in cells { validity.append(cell.is_some()); }
    validity.finish()
}

fn interleave_list_values<'a>(lists: impl Iterator<Item = (&'a [i32], &'a dyn Array)>, cells: &[Cell], padding: Padding)
    -> Result<(OffsetBuffer<i32>, ArrayRef)> {
    let (offsets, values): (Vec<_>, Vec<_>) = lists.unzip();
    let mut picks = Vec::new();
    let mut selected = Vec::with_capacity(cells.len().checked_add(1).ok_or_else(size_error)?);
    selected.push(0);
    for &cell in cells {
        let count = if let Some((source, row)) = cell {
            let start = offsets[source][row] as usize;
            let end = offsets[source][row + 1] as usize;
            let count = picks.len().checked_add(end.checked_sub(start).ok_or_else(size_error)?).ok_or_else(size_error)?;
            let count = i32::try_from(count).map_err(|_| size_error())?;
            picks.extend((start..end).map(|index| Some((source, index))));
            count
        } else {
            *selected.last().expect("List offsets start at zero")
        };
        selected.push(count);
    }
    let values = copy_nested_cells(&values, &picks, padding)?;
    Ok((OffsetBuffer::new(ScalarBuffer::from(selected)), values))
}

fn invalid(error: impl Display) -> Anomaly { Anomaly::incorrect("rtdb/column-selection", error.to_string()) }
fn size_error() -> Anomaly { invalid("Arrow selection exceeds addressable memory") }

#[test]
fn copying_fixed_width_cells_clears_invalid_payloads() {
    use arrow_array::{FixedSizeBinaryArray, Int64Array, cast::AsArray, types::Int64Type};

    let mask = NullBuffer::from(vec![false, true]);
    let binary = FixedSizeBinaryArray::try_new(2, vec![17u8, 18, 19, 20].into(), Some(mask.clone())).unwrap();
    let numbers = Int64Array::try_new(vec![123, 456].into(), Some(mask)).unwrap();

    let binary = copy_selected_cells(&[&binary], &[(0, 0), (0, 1)]).unwrap();
    let numbers = copy_selected_cells(&[&numbers], &[(0, 0), (0, 1)]).unwrap();

    assert!(binary.is_null(0));
    assert_eq!(binary.as_fixed_size_binary().value_data(), [0, 0, 19, 20]);
    assert!(numbers.is_null(0));
    assert_eq!(numbers.as_primitive::<Int64Type>().values().as_ref(), &[0, 456]);
}

#[test]
fn compact_selection_keeps_sliced_run_end_values_encoded() {
    use arrow_array::{Int16Array, Int64Array};

    let values = RunArray::<Int16Type>::try_new(
        &Int16Array::from(vec![3, 6, 10]), &Int64Array::from(vec![Some(11), None, Some(42)]),
    ).unwrap().slice(2, 7);
    let other = RunArray::<Int16Type>::try_new(&Int16Array::from(vec![2]), &Int64Array::from(vec![7])).unwrap();

    let selected = copy_selected_cells(&[&values, &other], &[(0, 0), (0, 3), (0, 2), (1, 1), (0, 6), (0, 4)]).unwrap();
    let selected = selected.as_any().downcast_ref::<RunArray<Int16Type>>().unwrap();

    assert_eq!(selected.run_ends().values(), &[1, 3, 4, 6]);
    let actual = selected.values().as_any().downcast_ref::<Int64Array>().unwrap().iter().collect::<Vec<_>>();
    assert_eq!(actual, [Some(11), None, Some(7), Some(42)]);
}
