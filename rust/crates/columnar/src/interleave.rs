use std::{fmt::Display, sync::Arc};
use arrow_array::{Array, ArrayRef, BooleanArray, MapArray, PrimitiveArray, RunArray, UnionArray, make_array, new_null_array, types::{Int16Type, Int32Type, Int64Type, RunEndIndexType}};
use arrow_buffer::{ArrowNativeType, BooleanBufferBuilder, NullBuffer, NullBufferBuilder, OffsetBuffer, ScalarBuffer, bit_iterator::BitSliceIterator};
use arrow_data::{ArrayData, transform::{Capacities, MutableArrayData}};
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
        Some(DataType::Union(fields, arrow_schema::UnionMode::Dense)) =>
            interleave_union(fields, arrays, cells.iter().copied(), cells.len(), Padding::Null, interleave),
        _ => arrow_select::interleave::interleave(arrays, cells).map_err(|error| invalid(error)),
    }
}

/// Copies selected Arrow cells, allocating dense-union and run-end children according to their selected positions.
/// Sources have the same layout, and every selection names a row within its source.
pub fn copy_selected_cells(arrays: &[&dyn Array], cells: &[(usize, usize)]) -> Result<ArrayRef> {
    copy_cells(arrays, Selection::Picks(cells), Padding::Null)
}

/// Copies selected Arrow cells with typed null slots for missing cells.
pub fn copy_nullable_cells(arrays: &[&dyn Array], cells: &[Option<(usize, usize)>]) -> Result<ArrayRef> {
    copy_cells(arrays, Selection::Nullable(cells), Padding::Null)
}

/// Copies cells into persisted event columns, keeping the legacy undefined slots beneath null parents.
pub fn copy_event_cells(arrays: &[&dyn Array], cells: &[Option<(usize, usize)>]) -> Result<ArrayRef> {
    copy_cells(arrays, Selection::Nullable(cells), Padding::Event)
}

pub(crate) fn undefined_cells(data_type: &DataType, rows: usize) -> Result<ArrayRef> {
    let empty = arrow_array::new_empty_array(data_type);
    let mut ranges = CellRanges::default();
    ranges.push(CellRange::Nulls(rows))?;
    copy_nested_cells(&[empty.as_ref()], Selection::Ranges(&ranges), Padding::Undefined)
}

fn copy_cells(arrays: &[&dyn Array], cells: Selection<'_>, padding: Padding) -> Result<ArrayRef> {
    let layout = arrays.first().map(|array| array.data_type()).ok_or_else(|| invalid("Nothing to interleave"))?;
    if arrays.iter().any(|array| array.data_type() != layout) {
        return Err(invalid("Selected Arrow layouts differ"));
    }
    copy_nested_cells(arrays, cells, padding)
}

type Pick = (usize, usize);
type Cell = Option<Pick>;

#[derive(Clone, Copy)]
enum Selection<'a> {
    Picks(&'a [Pick]),
    Nullable(&'a [Cell]),
    Ranges(&'a CellRanges),
}

impl<'a> Selection<'a> {
    fn len(self) -> usize {
        match self {
            Self::Picks(cells) => cells.len(),
            Self::Nullable(cells) => cells.len(),
            Self::Ranges(cells) => cells.len,
        }
    }

    fn iter(self) -> impl Iterator<Item = Cell> + 'a {
        let (picks, nullable, ranges): (&[Pick], &[Cell], &[CellRange]) = match self {
            Self::Picks(picks) => (picks, &[], &[]),
            Self::Nullable(picks) => (&[], picks, &[]),
            Self::Ranges(ranges) => (&[], &[], &ranges.spans),
        };
        picks.iter().copied().map(Some)
            .chain(nullable.iter().copied())
            .chain(ranges.iter().copied().flat_map(CellRange::cells))
    }

    fn validity(self) -> Option<NullBuffer> {
        let mut validity = NullBufferBuilder::new(self.len());
        match self {
            Self::Picks(_) => return None,
            Self::Nullable(cells) => {
                for cell in cells { validity.append(cell.is_some()); }
            }
            Self::Ranges(ranges) => {
                for &range in &ranges.spans {
                    match range {
                        CellRange::Source { start, end, .. } => validity.append_n_non_nulls(end - start),
                        CellRange::Nulls(count) => validity.append_n_nulls(count),
                    }
                }
            }
        }
        validity.finish()
    }
}

#[derive(Clone, Copy)]
enum CellRange {
    Source { source: usize, start: usize, end: usize },
    Nulls(usize),
}

impl CellRange {
    fn cells(self) -> impl Iterator<Item = Cell> {
        let (source, rows) = match self {
            Self::Source { source, start, end } => (Some(source), start..end),
            Self::Nulls(count) => (None, 0..count),
        };
        rows.map(move |row| source.map(|source| (source, row)))
    }
}

#[derive(Default)]
struct CellRanges {
    spans: Vec<CellRange>,
    len: usize,
}

impl CellRanges {
    fn push(&mut self, span: CellRange) -> Result<()> {
        let count = match span {
            CellRange::Source { start, end, .. } => end.checked_sub(start).ok_or_else(size_error)?,
            CellRange::Nulls(count) => count,
        };
        if count == 0 { return Ok(()); }
        self.len = self.len.checked_add(count).ok_or_else(size_error)?;
        match (self.spans.last_mut(), span) {
            (Some(CellRange::Source { source: previous, end: previous_end, .. }), CellRange::Source { source, start, end })
                if *previous == source && *previous_end == start => *previous_end = end,
            (Some(CellRange::Nulls(previous)), CellRange::Nulls(count)) => *previous += count,
            _ => self.spans.push(span),
        }
        Ok(())
    }

    fn visit_visible(&self, arrays: &[&dyn Array], mut visit: impl FnMut(CellRange) -> Result<()>) -> Result<()> {
        for &span in &self.spans {
            if let CellRange::Source { source, start, end } = span
                && let Some(nulls) = arrays[source].nulls().filter(|nulls| nulls.null_count() != 0) {
                let mut next = start;
                for (first, last) in BitSliceIterator::new(nulls.validity(), nulls.offset() + start, end - start) {
                    let first = start + first;
                    if first > next { visit(CellRange::Nulls(first - next))?; }
                    next = start + last;
                    visit(CellRange::Source { source, start: first, end: next })?;
                }
                if next < end { visit(CellRange::Nulls(end - next))?; }
            } else {
                visit(span)?;
            }
        }
        Ok(())
    }
}

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

fn copy_nested_cells(arrays: &[&dyn Array], cells: Selection<'_>, padding: Padding) -> Result<ArrayRef> {
    use arrow_array::cast::AsArray;
    let layout = arrays[0].data_type();
    match layout {
        DataType::Union(fields, arrow_schema::UnionMode::Dense) => match cells {
            Selection::Picks(picks) => interleave_union(fields, arrays, picks.iter().copied(), cells.len(), padding,
                |children, picks| copy_nested_cells(children, Selection::Picks(picks), padding)),
            _ => interleave_union(fields, arrays, cells.iter(), cells.len(), padding,
                |children, picks| copy_nested_cells(children, Selection::Nullable(picks), padding)),
        },
        DataType::Struct(fields) => {
            let structs: Vec<_> = arrays.iter().map(|array| array.as_struct()).collect();
            let copy = |visible: Selection<'_>| {
                let columns = (0..fields.len()).map(|position| {
                    let inputs = structs.iter().map(|array| array.column(position).as_ref()).collect::<Vec<_>>();
                    copy_nested_cells(&inputs, visible, padding.under_null_parent())
                }).collect::<Result<Vec<_>>>()?;
                crate::build_struct_array(fields.clone(), &columns, cells.len(), visible.validity())
            };
            if arrays.iter().all(|array| array.null_count() == 0) {
                return copy(cells);
            }
            match cells {
                Selection::Ranges(ranges) => {
                    let mut visible = CellRanges::default();
                    ranges.visit_visible(arrays, |span| visible.push(span))?;
                    copy(Selection::Ranges(&visible))
                }
                _ if cells.iter().flatten().any(|(source, row)| arrays[source].is_null(row)) => {
                    let visible = visible_cells(arrays, cells).collect::<Vec<_>>();
                    copy(Selection::Nullable(&visible))
                }
                _ => copy(cells),
            }
        }
        DataType::List(_) => {
            let lists = arrays.iter().map(|array| array.as_list::<i32>()).map(|array| (array.value_offsets(), array.values().as_ref()));
            let (offsets, values, validity) = interleave_list_values(lists, arrays, cells, padding)?;
            // A union's explicit null leg does not require a physical null bitmap on the list child.
            let data = ArrayData::builder(layout.clone()).len(cells.len()).nulls(validity)
                .add_buffer(offsets.into_inner().into_inner()).add_child_data(values.to_data()).build().map_err(invalid)?;
            Ok(make_array(data))
        }
        DataType::FixedSizeList(_, width) => {
            let width = usize::try_from(*width).map_err(|_| invalid("Negative fixed-size list width"))?;
            let lists = arrays.iter().map(|array| array.as_fixed_size_list().values().as_ref()).collect::<Vec<_>>();
            cells.len().checked_mul(width).ok_or_else(size_error)?;
            let mut ranges = CellRanges::default();
            let mut validity = NullBufferBuilder::new(cells.len());
            for cell in visible_cells(arrays, cells) {
                validity.append(cell.is_some());
                if let Some((source, row)) = cell {
                    let start = row.checked_mul(width).ok_or_else(size_error)?;
                    let end = start.checked_add(width).ok_or_else(size_error)?;
                    ranges.push(CellRange::Source { source, start, end })?;
                } else {
                    ranges.push(CellRange::Nulls(width))?;
                }
            }
            let values = copy_nested_cells(&lists, Selection::Ranges(&ranges), padding.under_null_parent())?;
            let data = ArrayData::builder(layout.clone()).len(cells.len()).nulls(validity.finish())
                .add_child_data(values.to_data()).build().map_err(invalid)?;
            Ok(make_array(data))
        }
        DataType::Map(field, sorted) => {
            let maps = arrays.iter().map(|array| array.as_map()).map(|array| (array.value_offsets(), array.entries() as &dyn Array));
            let (offsets, values, validity) = interleave_list_values(maps, arrays, cells, padding)?;
            let entries = values.as_struct();
            if entries.column(0).logical_null_count() != 0 { return Err(invalid("Arrow map keys cannot be null")); }
            Ok(Arc::new(MapArray::try_new(Arc::clone(field), offsets, entries.clone(), validity, *sorted).map_err(invalid)?))
        }
        DataType::RunEndEncoded(ends, _) => match ends.data_type() {
            DataType::Int16 => copy_runs::<Int16Type>(arrays, cells, padding),
            DataType::Int32 => copy_runs::<Int32Type>(arrays, cells, padding),
            DataType::Int64 => copy_runs::<Int64Type>(arrays, cells, padding),
            _ => Err(invalid("A run-end array requires signed 16-, 32- or 64-bit run ends")),
        },
        _ => copy_flat_cells(arrays, cells),
    }
}

fn copy_runs<R: RunEndIndexType>(arrays: &[&dyn Array], cells: Selection<'_>, padding: Padding) -> Result<ArrayRef> {
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
    let values = copy_nested_cells(&inputs, Selection::Nullable(&picks), padding)?;
    let ends = PrimitiveArray::<R>::new(ScalarBuffer::from(ends), None);
    let data = ArrayData::builder(arrays[0].data_type().clone()).len(cells.len())
        .add_child_data(ends.to_data()).add_child_data(values.to_data()).build().map_err(invalid)?;
    Ok(make_array(data))
}

fn interleave_union<C>(fields: &UnionFields, arrays: &[&dyn Array], cells: impl Iterator<Item = C>, len: usize, padding: Padding,
    select: impl Fn(&[&dyn Array], &[C]) -> Result<ArrayRef>) -> Result<ArrayRef>
where C: Copy + From<(usize, usize)> + Into<Cell> {
    let unions = arrays.iter().map(|array| array.as_any().downcast_ref::<UnionArray>().ok_or_else(|| invalid("A union source is not a union"))).collect::<Result<Vec<_>>>()?;
    let mut picks: Vec<Vec<C>> = vec![Vec::new(); fields.iter().map(|(id, _)| id as usize + 1).max().unwrap_or(0)];
    let mut ids = Vec::with_capacity(len);
    let mut offsets = Vec::with_capacity(len);
    let masked_leg = padding.union_leg(fields).map(|(id, _)| id);
    for cell in cells {
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

fn copy_flat_cells(arrays: &[&dyn Array], cells: Selection<'_>) -> Result<ArrayRef> {
    if let Selection::Picks(picks) = cells
        && (arrays.iter().all(|array| array.null_count() == 0)
            || picks.iter().all(|&(source, row)| arrays[source].is_valid(row))) {
        return arrow_select::interleave::interleave(arrays, picks).map_err(invalid);
    }
    if let Selection::Ranges(ranges) = cells
        && let Some(copied) = copy_flat_ranges(arrays, ranges)? {
        return Ok(copied);
    }

    let mut needs_null = false;
    let mut picks = Vec::with_capacity(cells.len());
    picks.extend(visible_cells(arrays, cells).map(|cell| cell.unwrap_or_else(|| {
        needs_null = true;
        (arrays.len(), 0)
    })));
    if needs_null {
        let null = new_null_array(arrays[0].data_type(), 1);
        let mut inputs = arrays.to_vec();
        inputs.push(null.as_ref());
        arrow_select::interleave::interleave(&inputs, &picks).map_err(invalid)
    } else {
        arrow_select::interleave::interleave(arrays, &picks).map_err(invalid)
    }
}

fn copy_flat_ranges(arrays: &[&dyn Array], ranges: &CellRanges) -> Result<Option<ArrayRef>> {
    let layout = arrays[0].data_type();
    let len = ranges.len;
    if len == 0 { return Ok(Some(arrow_array::new_empty_array(layout))); }
    if layout == &DataType::Null { return Ok(Some(new_null_array(layout, len))); }
    if layout == &DataType::Boolean { return copy_boolean_ranges(arrays, ranges).map(Some); }
    let width = match layout {
        DataType::Utf8 | DataType::Binary | DataType::LargeUtf8 | DataType::LargeBinary => None,
        DataType::FixedSizeBinary(width) => Some(usize::try_from(*width).map_err(|_| size_error())?),
        other => match other.primitive_width() {
            Some(width) => Some(width),
            None => return Ok(None),
        },
    };
    if let Some(width) = width {
        check_buffer_size(len.checked_mul(width).ok_or_else(size_error)?)?;
    }

    let data = arrays.iter().map(|array| array.to_data()).collect::<Vec<_>>();
    let capacity = match layout {
        DataType::Utf8 | DataType::Binary => byte_range_capacity::<i32>(arrays, &data, ranges)?,
        DataType::LargeUtf8 | DataType::LargeBinary => byte_range_capacity::<i64>(arrays, &data, ranges)?,
        _ => Capacities::Array(len),
    };
    let padded = ranges.spans.iter().any(|span| matches!(span, CellRange::Nulls(_)));
    let mut output = MutableArrayData::with_capacities(data.iter().collect(), padded, capacity);
    // Extending through a source null would copy its hidden payload.
    ranges.visit_visible(arrays, |span| {
        match span {
            CellRange::Source { source, start, end } => output.extend(source, start, end),
            CellRange::Nulls(count) => output.extend_nulls(count),
        }
        Ok(())
    })?;
    Ok(Some(make_array(output.freeze())))
}

fn copy_boolean_ranges(arrays: &[&dyn Array], ranges: &CellRanges) -> Result<ArrayRef> {
    use arrow_array::cast::AsArray;

    let mut values = BooleanBufferBuilder::new(ranges.len);
    let mut validity = NullBufferBuilder::new(ranges.len);
    ranges.visit_visible(arrays, |span| {
        match span {
            CellRange::Source { source, start, end } => {
                let bits = arrays[source].as_boolean().values();
                let offset = bits.offset();
                values.append_packed_range(offset + start..offset + end, bits.values());
                validity.append_n_non_nulls(end - start);
            }
            CellRange::Nulls(count) => {
                values.append_n(count, false);
                validity.append_n_nulls(count);
            }
        }
        Ok(())
    })?;
    Ok(Arc::new(BooleanArray::new(values.finish(), validity.finish())))
}

fn byte_range_capacity<O: ArrowNativeType>(arrays: &[&dyn Array], data: &[ArrayData], ranges: &CellRanges) -> Result<Capacities> {
    // Arrow's extend reserves one extra offset for the source range's leading boundary.
    let capacity = ranges.len.checked_add(1).ok_or_else(size_error)?;
    let offsets = capacity.checked_add(1).and_then(|count| count.checked_mul(std::mem::size_of::<O>())).ok_or_else(size_error)?;
    check_buffer_size(offsets)?;
    let mut bytes = 0usize;
    ranges.visit_visible(arrays, |span| {
        if let CellRange::Source { source, start, end } = span {
            let offsets = data[source].buffer::<O>(0);
            let len = offsets[end].as_usize().checked_sub(offsets[start].as_usize()).ok_or_else(size_error)?;
            bytes = bytes.checked_add(len).ok_or_else(size_error)?;
        }
        Ok(())
    })?;
    O::from_usize(bytes).ok_or_else(|| invalid(arrow_schema::ArrowError::OffsetOverflowError(bytes)))?;
    check_buffer_size(bytes)?;
    Ok(Capacities::Binary(capacity, Some(bytes)))
}

fn check_buffer_size(bytes: usize) -> Result<()> {
    let alignment = arrow_buffer::alloc::ALIGNMENT.max(64);
    std::alloc::Layout::from_size_align(bytes, alignment).map_err(|_| size_error())?;
    Ok(())
}

fn visible_cells<'a>(arrays: &'a [&'a dyn Array], cells: Selection<'a>) -> impl Iterator<Item = Cell> + 'a {
    cells.iter().map(move |cell| cell.filter(|&(source, row)| arrays[source].is_valid(row)))
}

fn interleave_list_values<'a>(lists: impl Iterator<Item = (&'a [i32], &'a dyn Array)>, arrays: &[&dyn Array], cells: Selection<'_>,
    padding: Padding) -> Result<(OffsetBuffer<i32>, ArrayRef, Option<NullBuffer>)> {
    let (offsets, values): (Vec<_>, Vec<_>) = lists.unzip();
    let mut ranges = CellRanges::default();
    let mut validity = NullBufferBuilder::new(cells.len());
    let mut selected = Vec::with_capacity(cells.len().checked_add(1).ok_or_else(size_error)?);
    selected.push(0);
    for cell in visible_cells(arrays, cells) {
        validity.append(cell.is_some());
        if let Some((source, row)) = cell {
            let start = usize::try_from(offsets[source][row]).map_err(|_| size_error())?;
            let end = usize::try_from(offsets[source][row + 1]).map_err(|_| size_error())?;
            ranges.push(CellRange::Source { source, start, end })?;
        }
        selected.push(i32::try_from(ranges.len).map_err(|_| size_error())?);
    }
    let values = copy_nested_cells(&values, Selection::Ranges(&ranges), padding)?;
    Ok((OffsetBuffer::new(ScalarBuffer::from(selected)), values, validity.finish()))
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
fn copying_contiguous_flat_cells_owns_the_values() {
    use arrow_array::{Int64Array, cast::AsArray};

    let source = Int64Array::from(vec![None, Some(10), Some(20)]);
    let picks = [(0, 1), (0, 2)];

    let retained = interleave(&[&source], &picks).unwrap();
    let copied = copy_selected_cells(&[&source], &picks).unwrap();

    let original = source.values()[1..].as_ptr();
    assert_eq!(retained.as_primitive::<Int64Type>().values().as_ptr(), original);
    let copied = copied.as_primitive::<Int64Type>();
    assert_eq!(copied.values().as_ref(), &[10, 20]);
    assert_ne!(copied.values().as_ptr(), original);
}

#[test]
fn copying_sliced_lists_rebases_offsets_and_omits_hidden_text() {
    use arrow_array::{ListArray, StringArray, cast::AsArray};

    let children = StringArray::new(OffsetBuffer::new(vec![0, 1, 2, 8, 9, 10, 11, 12].into()),
        b"zaSECRETbhcd".to_vec().into(), Some(NullBuffer::from(vec![true, true, false, true, true, true, true]))).slice(1, 6);
    let list = ListArray::new(Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(vec![0, 1, 3, 4, 6, 6].into()), Arc::new(children),
        Some(NullBuffer::from(vec![true, true, false, true, true])));
    let slice = list.slice(1, 4);

    let copied = copy_selected_cells(&[&slice, &list], &[(0, 2), (0, 1), (0, 0), (0, 3), (1, 0), (0, 2)]).unwrap();
    let copied = copied.as_list::<i32>();
    assert_eq!(copied.value_offsets(), &[0, 2, 2, 4, 4, 5, 7]);
    assert!(copied.is_null(1));
    assert!(copied.is_valid(3));
    let values = copied.values().as_string::<i32>();
    assert_eq!(values.iter().collect::<Vec<_>>(), [Some("c"), Some("d"), None, Some("b"), Some("a"), Some("c"), Some("d")]);
    assert_eq!(values.value_data(), b"cdbacd");
}

#[test]
fn copying_sliced_fixed_lists_clears_masked_boolean_bits() {
    use arrow_array::{FixedSizeListArray, cast::AsArray};
    use arrow_buffer::BooleanBuffer;

    let children = BooleanArray::new(BooleanBuffer::from(vec![false, true, true, true, true, true, true]),
        Some(NullBuffer::from(vec![true, true, true, true, false, true, true]))).slice(1, 6);
    let list = FixedSizeListArray::new(Arc::new(Field::new("item", DataType::Boolean, true)), 2, Arc::new(children),
        Some(NullBuffer::from(vec![true, true, false]))).slice(1, 2);

    let copied = copy_selected_cells(&[&list], &[(0, 1), (0, 0), (0, 1)]).unwrap();
    let copied = copied.as_fixed_size_list();
    assert_eq!(copied.nulls().unwrap().iter().collect::<Vec<_>>(), [false, true, false]);
    let values = copied.values().as_boolean();
    assert_eq!(values.iter().collect::<Vec<_>>(), [None, None, Some(true), None, None, None]);
    assert_eq!(values.values().values(), &[0b00000100]);
}

#[test]
fn copying_fixed_list_padding_preserves_the_event_union_leg() {
    use arrow_array::{FixedSizeListArray, Int64Array, NullArray, cast::AsArray};

    let fields = UnionFields::try_new([4, 7], [Field::new("number", DataType::Int64, false), Field::new("null", DataType::Null, true)]).unwrap();
    let children = UnionArray::try_new(fields, vec![4, 4, 4, 7, 4, 4].into(), Some(vec![0, 1, 2, 0, 3, 4].into()),
        vec![Arc::new(Int64Array::from(vec![90, 91, 17, 98, 99])), Arc::new(NullArray::new(1))]).unwrap();
    let list = FixedSizeListArray::new(Arc::new(Field::new("item", children.data_type().clone(), true)), 2, Arc::new(children),
        Some(NullBuffer::from(vec![true, true, false]))).slice(1, 2);
    let picks = [Some((0, 1)), Some((0, 0)), None];

    let events = copy_event_cells(&[&list], &picks).unwrap();
    let defined = copy_nullable_cells(&[&list], &picks).unwrap();

    let events = events.as_fixed_size_list().values().as_union();
    assert_eq!(events.type_ids().as_ref(), &[4, 4, 4, 7, 4, 4]);
    assert_eq!(events.offsets().unwrap().as_ref(), &[0, 1, 2, 0, 3, 4]);
    let numbers = events.child(4).as_primitive::<Int64Type>();
    assert_eq!(numbers.iter().collect::<Vec<_>>(), [None, None, Some(17), None, None]);
    assert_eq!(numbers.values().as_ref(), &[0, 0, 17, 0, 0]);
    let defined = defined.as_fixed_size_list().values().as_union();
    assert_eq!(defined.type_ids().as_ref(), &[7, 7, 4, 7, 7, 7]);
    assert_eq!(defined.offsets().unwrap().as_ref(), &[0, 1, 0, 2, 3, 4]);
}

#[test]
fn copying_sliced_maps_detaches_the_visible_entries_and_rejects_null_keys() {
    use arrow_array::{Int64Array, StringArray, StructArray, cast::AsArray};

    let keys = Int64Array::from(vec![Some(10), None, Some(30), Some(40)]);
    let entries = StructArray::new(vec![Field::new("key", DataType::Int64, true), Field::new("value", DataType::Utf8, true)].into(),
        vec![Arc::new(keys.clone()), Arc::new(StringArray::from(vec![Some("a"), Some("hidden"), None, Some("d")]))], None);
    let field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    let offsets = OffsetBuffer::new(vec![0, 1, 2, 4].into());
    let map = MapArray::new(Arc::clone(&field), offsets.clone(), entries.clone(), Some(NullBuffer::from(vec![true, false, true])), true);
    let slice = map.slice(1, 2);

    let copied = copy_selected_cells(&[&slice], &[(0, 0), (0, 1)]).unwrap();
    let copied = copied.as_map();
    assert_eq!(copied.data_type(), map.data_type());
    assert_eq!(copied.value_offsets(), &[0, 0, 2]);
    assert!(copied.is_null(0));
    let copied_keys = copied.keys().as_primitive::<Int64Type>();
    assert_eq!(copied_keys.values().as_ref(), &[30, 40]);
    assert_ne!(copied_keys.values().as_ptr(), keys.values()[2..].as_ptr());
    assert_eq!(copied.values().as_string::<i32>().iter().collect::<Vec<_>>(), [None, Some("d")]);

    let visible_null = MapArray::new(field, offsets, entries, None, true);
    let error = copy_selected_cells(&[&visible_null], &[(0, 1)]).unwrap_err();
    assert_eq!(error.code, "rtdb/column-selection");
    assert_eq!(error.message.as_deref(), Some("Arrow map keys cannot be null"));
}

#[test]
fn copying_lists_rejects_child_counts_beyond_signed_offsets() {
    use arrow_array::{ListArray, NullArray};

    let values = Arc::new(NullArray::new(i32::MAX as usize));
    let list = ListArray::new(Arc::new(Field::new("item", DataType::Null, true)),
        OffsetBuffer::new(vec![0, i32::MAX].into()), values, None);

    let error = copy_selected_cells(&[&list], &[(0, 0), (0, 0)]).unwrap_err();
    assert_eq!(error.code, "rtdb/column-selection");
    assert_eq!(error.message.as_deref(), Some("Arrow selection exceeds addressable memory"));
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
