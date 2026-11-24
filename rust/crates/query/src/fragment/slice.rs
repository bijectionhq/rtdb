use std::{mem::{size_of, size_of_val}, ops::Range, sync::Arc};

use arrow_array::{Array, ArrayRef, make_array};
use arrow_buffer::{ArrowNativeType, BooleanBuffer, Buffer, NullBuffer};
use arrow_data::{ArrayData, ArrayDataBuilder, BufferSpec, layout};
use arrow_schema::{DataType, Field, UnionFields, UnionMode};
use rtdb_columnar::{MemoryBudget, MemoryReservation};

use super::{IPC_ALIGNMENT, nested_layout};

pub(super) fn prepare(array: &dyn Array, field: &Field, rows: Range<usize>, memory: &Arc<MemoryBudget>)
    -> Option<(ArrayRef, Arc<MemoryReservation>, usize)> {
    if array.data_type() != field.data_type() { return None; }
    let mut reservation = memory.reserve(field.size().checked_mul(4)?.checked_add(1024)?).ok()?;
    let work = Arc::get_mut(&mut reservation).expect("The slice owns its reservation");
    let data = slice(&array.to_data(), rows, work, 0)?;
    let body = ipc_body_bytes(&data)?;
    Some((make_array(data), reservation, body))
}

fn slice(data: &ArrayData, rows: Range<usize>, work: &mut MemoryReservation, depth: usize) -> Option<ArrayData> {
    if rows.start > rows.end || rows.end > data.len() || depth > 64 { return None; }
    if !nested_layout(data.data_type()) { return scalar(data, rows, work); }
    let nulls = data.nulls().map(|nulls| nulls.slice(rows.start, rows.len())).filter(|nulls| nulls.null_count() > 0);
    let nulls = match nulls {
        Some(nulls) => Some(trim_null_padding(&nulls, work)?.unwrap_or(nulls)),
        None => None,
    };
    let builder = ArrayDataBuilder::new(data.data_type().clone()).len(rows.len()).nulls(nulls.clone());
    let child_depth = depth + 1;
    let builder = match data.data_type() {
        DataType::List(_) | DataType::Map(_, _) => {
            let offsets = data.buffer::<i32>(0).get(rows.start..=rows.end)?;
            if nulls.as_ref().is_some_and(|nulls| nulls.iter().zip(offsets.windows(2)).any(|(valid, pair)| !valid && pair[0] != pair[1])) {
                return None;
            }
            let start = usize::try_from(offsets[0]).ok()?;
            let end = usize::try_from(*offsets.last()?).ok()?;
            let child = slice(data.child_data().first()?, start..end, work, child_depth)?;
            if matches!(data.data_type(), DataType::Map(_, _)) {
                let keys = child.child_data().first()?;
                if keys.null_count() != 0 || matches!(keys.data_type(), DataType::Null | DataType::Union(_, _) | DataType::RunEndEncoded(_, _)) {
                    return None;
                }
            }
            let offsets = if start == 0 {
                data.buffers()[0].slice_with_length((data.offset() + rows.start) * size_of::<i32>(), size_of_val(offsets))
            } else {
                reserve(work, size_of_val(offsets))?;
                let base = offsets[0];
                Buffer::from_vec(offsets.iter().map(|offset| offset - base).collect())
            };
            builder.add_buffer(offsets).add_child_data(child)
        }
        DataType::Struct(_) => {
            let mut children = Vec::with_capacity(data.child_data().len());
            for child in data.child_data() {
                if nulls.as_ref().is_some_and(|nulls| nulls.iter().zip(rows.clone()).any(|(valid, row)| !valid && !is_padding(child, row))) {
                    return None;
                }
                children.push(slice(child, rows.clone(), work, child_depth)?);
            }
            builder.child_data(children)
        }
        DataType::FixedSizeList(field, width) => {
            let width = usize::try_from(*width).ok()?;
            let start = data.offset().checked_add(rows.start)?.checked_mul(width)?;
            let end = start.checked_add(rows.len().checked_mul(width)?)?;
            let child = data.child_data().first()?;
            if width != 0 && nulls.as_ref().is_some_and(|nulls| {
                nulls.iter().zip((start..end).step_by(width)).any(|(valid, row)| {
                    !valid && (row..row + width).any(|index| !is_padding(child, index))
                })
            }) {
                return None;
            }
            let child = slice(child, start..end, work, child_depth)?;
            let retained = work.bytes();
            if nulls.is_some() && !field.is_nullable() {
                reserve(work, aligned(child.len().div_ceil(8))?)?;
            }
            let result = builder.add_child_data(child).build();
            work.shrink_to(retained).ok()?;
            return result.ok();
        }
        DataType::Union(fields, UnionMode::Dense) => return dense_union(data, fields, rows, builder, work, child_depth),
        DataType::RunEndEncoded(ends, _) => return match ends.data_type() {
            DataType::Int16 => run_ends::<i16>(data, rows, builder, work, child_depth),
            DataType::Int32 => run_ends::<i32>(data, rows, builder, work, child_depth),
            DataType::Int64 => run_ends::<i64>(data, rows, builder, work, child_depth),
            _ => None,
        },
        _ => return None,
    };
    builder.build().ok()
}

fn dense_union(data: &ArrayData, fields: &UnionFields, rows: Range<usize>, builder: ArrayDataBuilder,
    work: &mut MemoryReservation, depth: usize) -> Option<ArrayData> {
    let tags = data.buffer::<i8>(0).get(rows.clone())?;
    let offsets = data.buffer::<i32>(1).get(rows.clone())?;
    // Arrow IPC 58.4.1 writes dense union buffers as supplied, including when a list encloses the union (#242).
    let mut starts = [0; 128];
    let mut ends = [0; 128];
    for (&tag, &offset) in tags.iter().zip(offsets) {
        let tag = usize::try_from(tag).ok()?;
        let offset = usize::try_from(offset).ok()?;
        if ends[tag] == 0 {
            starts[tag] = offset;
        } else if offset != ends[tag] {
            return None;
        }
        ends[tag] = offset + 1;
    }

    let children = fields.iter().zip(data.child_data()).map(|((tag, _), child)| {
        let tag = usize::try_from(tag).ok()?;
        slice(child, starts[tag]..ends[tag], work, depth)
    }).collect::<Option<Vec<_>>>()?;
    let start = data.offset() + rows.start;
    let tags_buffer = data.buffers()[0].slice_with_length(start, rows.len());
    let offsets_buffer = if starts.iter().all(|&start| start == 0) {
        data.buffers()[1].slice_with_length(start * size_of::<i32>(), size_of_val(offsets))
    } else {
        reserve(work, size_of_val(offsets))?;
        Buffer::from_vec(tags.iter().zip(offsets).map(|(&tag, &offset)| offset - starts[tag as usize] as i32).collect())
    };
    let id_slots = fields.iter().map(|(tag, _)| tag as usize + 1).max().unwrap_or(1);
    reserve(work, id_slots * size_of::<Option<ArrayRef>>())?;
    let retained = work.bytes();
    reserve(work, 128 * size_of::<Option<usize>>())?;
    let result = builder.add_buffer(tags_buffer).add_buffer(offsets_buffer).child_data(children).build();
    work.shrink_to(retained).ok()?;
    result.ok()
}

fn run_ends<T: ArrowNativeType + Ord>(data: &ArrayData, rows: Range<usize>, builder: ArrayDataBuilder,
    work: &mut MemoryReservation, depth: usize) -> Option<ArrayData> {
    let [ends, values] = data.child_data() else { return None; };
    let run_ends = ends.buffer::<T>(0).get(..ends.len())?;
    let start = data.offset().checked_add(rows.start)?;
    let end = data.offset().checked_add(rows.end)?;
    let physical = if rows.is_empty() {
        0..0
    } else {
        let first = run_ends.partition_point(|run_end| run_end.as_usize() <= start);
        let last = run_ends.partition_point(|run_end| run_end.as_usize() < end);
        first..last.checked_add(1)?
    };
    let selected = run_ends.get(physical.clone())?;
    let child = slice(values, physical.clone(), work, depth)?;
    let ends = if selected.is_empty() || (start == 0 && selected.last()?.as_usize() == end) {
        ends.slice(physical.start, physical.len())
    } else {
        reserve(work, size_of_val(selected))?;
        let mut rebased = Vec::with_capacity(selected.len());
        for run_end in selected {
            rebased.push(T::from_usize(run_end.as_usize().min(end) - start)?);
        }
        ArrayDataBuilder::new(ends.data_type().clone()).len(rebased.len()).add_buffer(Buffer::from_vec(rebased)).build().ok()?
    };
    builder.child_data(vec![ends, child]).build().ok()
}

fn is_padding(data: &ArrayData, row: usize) -> bool {
    match data.data_type() {
        DataType::Null => true,
        DataType::Union(fields, UnionMode::Dense) => fields.iter().find(|(_, field)| field.data_type() == &DataType::Null)
            .is_some_and(|(tag, _)| data.buffer::<i8>(0).get(row) == Some(&tag)),
        _ => data.is_null(row),
    }
}

fn scalar(data: &ArrayData, rows: Range<usize>, work: &mut MemoryReservation) -> Option<ArrayData> {
    let sliced = data.slice(rows.start, rows.len());
    let clean = |is_empty: &dyn Fn(usize) -> bool| {
        sliced.nulls().is_none_or(|nulls| nulls.null_count() == 0 || nulls.iter().enumerate().all(|(row, valid)| valid || is_empty(row)))
    };
    let compatible = match data.data_type() {
        DataType::Null => true,
        DataType::Boolean => clean(&|row| {
            let bit = sliced.offset() + row;
            sliced.buffers()[0][bit / 8] & (1 << (bit % 8)) == 0
        }),
        DataType::Utf8 | DataType::Binary => {
            let offsets = sliced.buffer::<i32>(0);
            clean(&|row| offsets[row] == offsets[row + 1])
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            let offsets = sliced.buffer::<i64>(0);
            clean(&|row| offsets[row] == offsets[row + 1])
        }
        DataType::FixedSizeBinary(width) => clean(&|row| empty_slot(&sliced, row, *width as usize)),
        ty => ty.primitive_width().is_some_and(|width| clean(&|row| empty_slot(&sliced, row, width))),
    };
    if !compatible { return None; }
    let cleaned_nulls = match sliced.nulls().filter(|nulls| nulls.null_count() != 0) {
        Some(nulls) => trim_null_padding(nulls, work)?,
        None => None,
    };
    if data.data_type() == &DataType::Boolean
        && let Some(buffer) = trim_bitmap_padding(&sliced.buffers()[0], sliced.offset(), sliced.len(), work)? {
        return ArrayDataBuilder::new(DataType::Boolean).len(sliced.len())
            .nulls(cleaned_nulls.or_else(|| sliced.nulls().cloned())).add_buffer(buffer).build().ok();
    }
    match cleaned_nulls {
        Some(nulls) => sliced.into_builder().nulls(Some(nulls)).build().ok(),
        None => Some(sliced),
    }
}

fn empty_slot(data: &ArrayData, row: usize, width: usize) -> bool {
    let start = (data.offset() + row) * width;
    data.buffers()[0][start..start + width].iter().all(|&byte| byte == 0)
}

fn reserve(work: &mut MemoryReservation, bytes: usize) -> Option<()> {
    work.grow_to(work.bytes().checked_add(bytes)?).ok()
}

fn trim_null_padding(nulls: &NullBuffer, work: &mut MemoryReservation) -> Option<Option<NullBuffer>> {
    Some(trim_bitmap_padding(nulls.buffer(), nulls.offset(), nulls.len(), work)?
        .map(|buffer| NullBuffer::new(BooleanBuffer::new(buffer, 0, nulls.len()))))
}

fn trim_bitmap_padding(buffer: &Buffer, offset: usize, len: usize, work: &mut MemoryReservation) -> Option<Option<Buffer>> {
    // Arrow IPC 58.4.1 writes the whole last byte when a bitmap starts on a byte boundary.
    let trailing = len % 8;
    if offset % 8 != 0 || trailing == 0 { return Some(None); }
    let start = offset / 8;
    let bytes = buffer.as_slice().get(start..start.checked_add(len.div_ceil(8))?)?;
    let mask = (1u8 << trailing) - 1;
    if *bytes.last()? & !mask == 0 { return Some(None); }
    reserve(work, bytes.len())?;
    let mut cleaned = bytes.to_vec();
    *cleaned.last_mut()? &= mask;
    Some(Some(Buffer::from_vec(cleaned)))
}

fn aligned(bytes: usize) -> Option<usize> {
    let mask = IPC_ALIGNMENT - 1;
    Some(bytes.checked_add(mask)? & !mask)
}

fn ipc_body_bytes(data: &ArrayData) -> Option<usize> {
    fn variable_bytes<T: ArrowNativeType>(data: &ArrayData) -> Option<usize> {
        if data.is_empty() { return Some(0); }
        let offsets = data.buffer::<T>(0);
        offsets[data.len()].as_usize().checked_sub(offsets[0].as_usize())
    }

    let bitmap = data.len().div_ceil(8);
    let mut bytes = if data.null_count() > 0 { aligned(bitmap)? } else { 0 };
    let ty = data.data_type();
    let has_offsets = matches!(ty, DataType::List(_) | DataType::Map(_, _) | DataType::Binary | DataType::Utf8 | DataType::LargeBinary | DataType::LargeUtf8);
    for spec in layout(ty).buffers {
        let length = match spec {
            BufferSpec::FixedWidth { byte_width, .. } => data.len().checked_add(usize::from(has_offsets))?.checked_mul(byte_width)?,
            BufferSpec::VariableWidth if matches!(ty, DataType::LargeBinary | DataType::LargeUtf8) => variable_bytes::<i64>(data)?,
            BufferSpec::VariableWidth => variable_bytes::<i32>(data)?,
            BufferSpec::BitMap => bitmap,
            BufferSpec::AlwaysNull => 0,
        };
        bytes = bytes.checked_add(aligned(length)?)?;
    }
    for child in data.child_data() {
        bytes = bytes.checked_add(ipc_body_bytes(child)?)?;
    }
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use std::{ops::Range, sync::Arc};
    use arrow_array::{Array, ArrayRef, FixedSizeListArray, Int16Array, Int64Array, ListArray, MapArray, NullArray, RecordBatch, RunArray, StringArray, StructArray, UnionArray, types::Int16Type};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
    use arrow_schema::{DataType, Field, Schema, UnionFields};
    use rtdb_columnar::{MemoryBudget, MemoryReservation, copy_selected_cells};

    use super::prepare;

    fn compare_with_gather(source: &dyn Array, rows: Range<usize>) -> (ArrayRef, Arc<MemoryReservation>) {
        let memory = MemoryBudget::new(1 << 20);
        let field = Field::new("nested", source.data_type().clone(), source.is_nullable());
        let (borrowed, scratch, body) = prepare(source, &field, rows.clone(), &memory).unwrap();
        let picks = rows.map(|row| (0, row)).collect::<Vec<_>>();
        let gathered = copy_selected_cells(&[source], &picks).unwrap();
        assert_eq!(borrowed.as_ref(), gathered.as_ref());

        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::clone(&borrowed)]).unwrap();
        let (_, encoded) = IpcDataGenerator::default().encode(&batch, &mut DictionaryTracker::new(false),
            &IpcWriteOptions::default(), &mut Default::default()).unwrap();
        assert_eq!(body, encoded.arrow_data.len());
        (borrowed, scratch)
    }

    #[test]
    fn list_slices_rebase_dense_legs_and_keep_the_selected_tag_and_value_buffers() {
        let numbers = Int64Array::from(vec![90, 10, 42]);
        let fields = UnionFields::try_new([4, 7, 9], [Field::new("number", DataType::Int64, false),
            Field::new("null", DataType::Null, true), Field::new("text", DataType::Utf8, false)]).unwrap();
        let union = UnionArray::try_new(fields, vec![4, 9, 4, 7, 4].into(), Some(vec![0, 0, 1, 0, 2].into()),
            vec![Arc::new(numbers.clone()), Arc::new(NullArray::new(1)), Arc::new(StringArray::from(vec!["unselected"]))]).unwrap();
        let field = Arc::new(Field::new("item", union.data_type().clone(), true));
        let list = ListArray::new(field, OffsetBuffer::new(vec![0, 2, 4, 4, 4, 5].into()), Arc::new(union.clone()),
            Some(NullBuffer::from(vec![true, true, false, true, true])));

        let (borrowed, _scratch) = compare_with_gather(&list, 1..4);
        let borrowed = borrowed.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(borrowed.value_offsets(), &[0, 2, 2, 2]);
        assert!(borrowed.is_null(1));
        assert!(borrowed.is_valid(2));
        let values = borrowed.values().as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!(values.type_ids().as_ptr(), union.type_ids()[2..].as_ptr());
        assert_eq!(values.offsets().unwrap().as_ref(), &[0, 0]);
        assert!(values.child(9).is_empty());
        let numbers_out = values.child(4).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(numbers_out.values().as_ptr(), numbers.values()[1..].as_ptr());
    }

    #[test]
    fn sliced_struct_keeps_map_entries_and_fixed_list_run_values() {
        let keys = Int64Array::from(vec![10, 20, 30, 40]);
        let entries = StructArray::from(vec![
            (Arc::new(Field::new("key", DataType::Int64, false)), Arc::new(keys.clone()) as ArrayRef),
            (Arc::new(Field::new("value", DataType::Utf8, true)), Arc::new(StringArray::from(vec![Some("a"), None, Some("c"), Some("d")])) as ArrayRef),
        ]);
        let map = MapArray::new(Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0, 1, 2, 2, 4].into()), entries,
            Some(NullBuffer::from(vec![true, true, false, true])), true);
        let values = Int64Array::from(vec![Some(11), None, Some(42)]);
        let runs = RunArray::<Int16Type>::try_new(&Int16Array::from(vec![3, 5, 8]), &values).unwrap();
        let fixed = FixedSizeListArray::new(Arc::new(Field::new("item", runs.data_type().clone(), true)), 2, Arc::new(runs), None);
        let source = StructArray::from(vec![
            (Arc::new(Field::new("map", map.data_type().clone(), true)), Arc::new(map) as ArrayRef),
            (Arc::new(Field::new("fixed", fixed.data_type().clone(), false)), Arc::new(fixed) as ArrayRef),
        ]);

        let (borrowed, _scratch) = compare_with_gather(&source.slice(1, 3), 0..2);
        let borrowed = borrowed.as_any().downcast_ref::<StructArray>().unwrap();
        let map = borrowed.column(0).as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.value_offsets(), &[0, 1, 1]);
        assert!(map.is_null(1));
        let keys_out = map.keys().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(keys_out.values().as_ptr(), keys.values()[1..].as_ptr());
        let fixed = borrowed.column(1).as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        let runs = fixed.values().as_any().downcast_ref::<RunArray<Int16Type>>().unwrap();
        assert_eq!(runs.run_ends().values(), &[1, 3, 4]);
        let values_out = runs.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values_out.values().as_ptr(), values.values().as_ptr());
    }
}
