use std::{collections::HashMap, ops::Range, sync::Arc};

use arrow_array::{Array, ArrayRef, Int32Array, MapArray, NullArray, TimestampMicrosecondArray, UnionArray,
    builder::PrimitiveBuilder, cast::AsArray, make_array, types::{ArrowPrimitiveType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type}};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_data::{ArrayData, transform::MutableArrayData};
use arrow_schema::{DataType, Field, UnionMode};
use rtdb_columnar::{BatchBuilder, ColumnPresence, FundedColumn, EXTENSION_NAME_KEY, copy_event_cells, estimate_event_copy_cell_bytes,
    estimate_event_null_cell_bytes, from_field, pack_runs};
use rtdb_types::{Anomaly, ListKind, LogicalType, Result, RunEndWidth, ScalarType, TimeUnit, Value};

use super::{EncodedEvent, PutColumn, event_size};

#[derive(Clone, Copy)]
enum Cell<'a> {
    Stored(&'a Field, &'a dyn Array, usize),
    Null,
    Undefined,
}

fn read_size_ceiling(field: &Field) -> Option<usize> {
    // SLOT dominates the fixed charges in estimate_read_cell_bytes, including each struct entry and sequence element.
    const SLOT: usize = 1024 + 32 * (std::mem::size_of::<Value>() + std::mem::size_of::<String>());
    const OFFSET_LIMIT: usize = i32::MAX as usize;
    let sequence = |child, count: usize| {
        read_size_ceiling(child)?.checked_add(SLOT)?.checked_mul(count)?.checked_add(SLOT)
    };
    let payload = match field.data_type() {
        DataType::Null if !field.is_nullable() => return None,
        DataType::Null | DataType::Boolean => 0,
        DataType::Utf8 | DataType::Binary => OFFSET_LIMIT.checked_mul(2)?,
        DataType::FixedSizeBinary(width) => usize::try_from(*width).ok()?.checked_mul(2)?,
        DataType::Timestamp(_, zone) => zone.as_ref().map_or(0, |zone| zone.len()),
        DataType::List(child) | DataType::Map(child, _) => sequence(child, OFFSET_LIMIT)?,
        DataType::FixedSizeList(child, width) => sequence(child, usize::try_from(*width).ok()?)?,
        DataType::RunEndEncoded(_, child) => read_size_ceiling(child)?,
        DataType::Union(fields, UnionMode::Dense) => fields.iter().try_fold(0usize, |max, (_, child)| {
            Some(max.max(read_size_ceiling(child)?))
        })?,
        DataType::Struct(fields) => fields.iter().try_fold(0usize, |total, child| {
            total.checked_add(SLOT)?.checked_add(child.name().len().checked_mul(2)?)?
                .checked_add(read_size_ceiling(child)?)
        })?,
        ty if ty.primitive_width().is_some() => 64,
        _ => return None,
    };
    SLOT.checked_add(payload)
}

pub(super) fn column<E: EncodedEvent>(
    events: &[&E], target: &Field, logical: &LogicalType, mut builder: Option<&mut BatchBuilder>,
    preflight_scalar_read: impl FnOnce() -> Result<()>,
) -> Result<Option<ArrayRef>> {
    if events.is_empty() { return Ok(None); }
    let reserved = builder.is_some();
    let mut defer_read_size = reserved;
    let mut previous = None;
    let limit = usize::MAX / events.len();
    for event in events {
        let Some(column) = event.put_column(target.name()) else { return Ok(None); };
        if defer_read_size && let PutColumn::Stored(field, array, row) = column {
            let key = std::ptr::from_ref(field);
            defer_read_size = row < array.len() && field.data_type() == array.data_type()
                && (previous == Some(key) || read_size_ceiling(field).is_some_and(|bytes| bytes <= limit));
            previous = Some(key);
        }
    }
    if reserved && !defer_read_size { preflight_scalar_read()?; }
    if let Some(builder) = builder.as_deref_mut()
        && let Some(array) = plain_primitive_column(events, target, builder)?
    {
        return Ok(Some(array));
    }
    let mismatch = |row| {
        let message = if reserved {
            "Allocation estimate requires values belonging to the declared logical type".into()
        } else {
            format!("Value at row {row} does not belong to {logical:?}")
        };
        Anomaly::incorrect("rtdb/column-type-mismatch", message)
    };
    // Without room for the gather plan the caller takes its per-cell path, so a refusal yields `None`.
    enum Plan { Funded(Option<Arc<rtdb_columnar::MemoryReservation>>), Unfunded }
    let reserve_plan = |bytes| match builder.as_deref() {
        None => Plan::Funded(None),
        Some(builder) => builder.try_reserve_scratch(bytes).map_or(Plan::Unfunded, |charge| Plan::Funded(Some(charge))),
    };
    let plan_size = events.len().checked_mul(128).ok_or_else(event_size)?;
    let Plan::Funded(_plan) = reserve_plan(plan_size) else { return Ok(None); };
    let mut cells = Vec::with_capacity(events.len());
    let mut aliases = Vec::new();
    let mut source_types = HashMap::new();
    let mut field_charges = Vec::new();
    let mut can_gather = true;
    for event in events {
        let cell = match event.put_column(target.name()).expect("Checked all events for stored columns") {
            PutColumn::Absent if logical == &LogicalType::Nothing => Cell::Undefined,
            PutColumn::Absent => Cell::Null,
            PutColumn::Stored(field, array, index) => {
                let reader = match source_types.entry(std::ptr::from_ref(field)) {
                    std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let bytes = field.size().checked_mul(8).ok_or_else(event_size)?;
                        let Plan::Funded(charge) = reserve_plan(bytes) else { return Ok(None); };
                        field_charges.extend(charge);
                        entry.insert(ColumnPresence::new(field)?)
                    }
                };
                if !reader.validate_stored(array, index)? {
                    match reader.stored_alias(array, index)? {
                        Some(field) => {
                            let bytes = field.size().checked_mul(2).ok_or_else(event_size)?;
                            let Plan::Funded(charge) = reserve_plan(bytes) else { return Ok(None); };
                            field_charges.extend(charge);
                            aliases.push((cells.len(), field));
                        }
                        None => can_gather = false,
                    }
                }
                if reader.is_bottom() { Cell::Undefined } else { select(field, array, index) }
            }
        };
        cells.push(cell);
    }
    for (index, field) in &aliases {
        if let Cell::Stored(_, array, row) = cells[*index] {
            cells[*index] = Cell::Stored(field, array, row);
        }
    }
    if let Some(row) = cells.iter().position(|cell| match cell {
        Cell::Null => !logical.nullable(),
        Cell::Undefined => logical != &LogicalType::Nothing,
        Cell::Stored(..) => false,
    }) && (reserved || row == 0) && can_gather {
        return Err(mismatch(row));
    }
    if !can_gather || !supported(target, true)
        || cells.iter().any(|&cell| matches!(cell, Cell::Stored(field, ..) if !supported(field, false))) {
        return Ok(None);
    }
    for (row, &cell) in cells.iter().enumerate() {
        let fits = match cell {
            Cell::Undefined => logical == &LogicalType::Nothing,
            _ => admitted(target, cell),
        };
        if !fits { return Err(mismatch(row)); }
    }
    let runs = matches!(target.data_type(), DataType::RunEndEncoded(..)).then(|| run_positions(&cells));
    let bound = builder.as_ref().map(|_| {
        let selected = runs.as_ref().map_or(cells.as_slice(), |(_, selected)| selected);
        let padding = estimate_event_null_cell_bytes(target)?;
        selected.iter().try_fold(target.size().checked_mul(8)
            .and_then(|size| size.checked_add(4096)).ok_or_else(event_size)?, |bytes, &cell| {
            let selected = match cell {
                Cell::Stored(field, array, row) => estimate_event_copy_cell_bytes(field, array, row)?,
                _ => 0,
            };
            selected.checked_add(padding).and_then(|cell| cell.checked_mul(4))
                .and_then(|cell| bytes.checked_add(cell)).ok_or_else(event_size)
        })
    }).transpose()?;
    let build = || match runs {
        Some((ends, values)) => gather_runs(target, ends, &values),
        None => gather(target, &cells),
    };
    if let Some((builder, bound)) = builder.zip(bound) {
        match builder.try_funded_column(bound, build)? {
            FundedColumn::Built(column) => Ok(column),
            FundedColumn::Unfunded => Ok(None),
        }
    } else {
        build()
    }
}

fn plain_primitive_column<E: EncodedEvent>(
    events: &[&E], target: &Field, builder: &mut BatchBuilder,
) -> Result<Option<ArrayRef>> {
    let ty = target.data_type();
    if !target.metadata().is_empty() || !(ty.is_integer() || ty.is_floating() || ty.is_temporal()) {
        return Ok(None);
    }
    if !events.iter().all(|event| match event.put_column(target.name()) {
        Some(PutColumn::Stored(field, array, row)) => field.metadata().is_empty()
            && field.data_type() == ty && array.data_type() == ty
            && row < array.len() && (target.is_nullable() || array.is_valid(row)),
        Some(PutColumn::Absent) => target.is_nullable(),
        None => false,
    }) {
        return Ok(None);
    }
    let width = ty.primitive_width().expect("Plain primitive width");
    let bound = events.len().checked_mul(width + 1)
        .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(event_size)?;
    macro_rules! primitive {
        ($array_type:ty) => { build_primitive::<$array_type, E>(events, target) };
    }
    match builder.try_funded_column(bound, || {
        let array = arrow_array::downcast_primitive!(ty => (primitive), _ => unreachable!("Plain primitive"));
        Ok(Some(array))
    })? {
        FundedColumn::Built(array) => Ok(array),
        FundedColumn::Unfunded => Ok(None),
    }
}

fn build_primitive<T: ArrowPrimitiveType, E: EncodedEvent>(events: &[&E], target: &Field) -> ArrayRef {
    let mut output = PrimitiveBuilder::<T>::with_capacity(events.len()).with_data_type(target.data_type().clone());
    for event in events {
        match event.put_column(target.name()) {
            Some(PutColumn::Stored(_, array, row)) if array.is_valid(row) => {
                output.append_value(array.as_primitive::<T>().value(row));
            }
            _ => output.append_null(),
        }
    }
    Arc::new(output.finish())
}

fn has_packed_null_payload(item: &Field, values: &dyn Array, start: usize, end: usize) -> bool {
    if !rtdb_types::ListValue::supports_numeric_arrow(item) { return false; }
    match item.data_type() {
        DataType::Union(..) => {
            let union = values.as_union();
            (start..end).any(|position| {
                let child = union.child(union.type_id(position));
                child.data_type() != &DataType::Null && child.is_null(union.value_offset(position))
            })
        }
        DataType::Float32 | DataType::Float64 | DataType::Null => false,
        _ => values.null_count() != 0 && (start..end).any(|position| values.is_null(position)),
    }
}

fn exact_packed_children(item: &Field, cells: &[Cell<'_>], width: Option<usize>, length: usize) -> Option<ArrayRef> {
    let mut sources = Vec::new();
    let mut slots = HashMap::new();
    let mut spans: Vec<(Option<usize>, Range<usize>)> = Vec::new();
    let mut masked = false;
    for &cell in cells {
        if let Cell::Stored(source, array, row) = cell {
            let (source, values, start, end) = list_span(source, array, row);
            if !rtdb_types::ListValue::supports_numeric_arrow(source) { return None; }
            if start == end { continue; }
            if source.data_type() != item.data_type() || source.metadata() != item.metadata() { return None; }
            let slot = Some(*slots.entry(std::ptr::from_ref(values)).or_insert_with(|| {
                let slot = sources.len();
                sources.push(values);
                slot
            }));
            if let Some((previous, range)) = spans.last_mut() && *previous == slot && range.end == start {
                range.end = end;
            } else {
                spans.push((slot, start..end));
            }
        } else if let Some(width) = width.filter(|width| *width != 0) {
            masked = true;
            spans.push((None, 0..width));
        }
    }
    if let [(Some(slot), range)] = spans.as_slice() {
        let slice = sources[*slot].slice(range.start, range.len());
        if reusable_packed_child(slice.as_ref()) { return Some(slice); }
    }
    if sources.is_empty() { return Some(arrow_array::new_null_array(item.data_type(), length)); }
    let sources = sources.iter().map(|source| source.to_data()).collect::<Vec<_>>();
    // MutableArrayData reserves the given capacity for every union leg.
    let capacity = if matches!(item.data_type(), DataType::Union(..)) { 0 } else { length };
    let mut child = MutableArrayData::new(sources.iter().collect(), masked, capacity);
    for (source, range) in spans {
        match source {
            Some(slot) => child.extend(slot, range.start, range.end),
            None => child.extend_nulls(range.len()),
        }
    }
    Some(make_array(child.freeze()))
}

fn reusable_packed_child(array: &dyn Array) -> bool {
    let fits = |buffer: &arrow_buffer::Buffer, bytes: usize| {
        buffer.len() >= bytes && (bytes..=bytes.saturating_add(63)).contains(&buffer.capacity())
    };
    let data = array.to_data();
    if let DataType::Union(fields, UnionMode::Dense) = data.data_type() {
        if !fits(&data.buffers()[0], array.len()) || !fits(&data.buffers()[1], array.len() * 4) { return false; }
        let union = array.as_union();
        let mut lengths = [0; 128];
        for (&tag, &offset) in union.type_ids().iter().zip(union.offsets().expect("Dense union offsets").iter()) {
            let length = &mut lengths[tag as usize];
            if offset != *length { return false; }
            *length += 1;
        }
        return fields.iter().all(|(tag, _)| {
            let child = union.child(tag);
            child.len() == lengths[tag as usize] as usize && reusable_packed_child(child.as_ref())
        });
    }
    if let Some(width) = data.data_type().primitive_width()
        && !fits(&data.buffers()[0], array.len() * width) { return false; }
    array.nulls().is_none_or(|nulls| {
        let bytes = array.len().div_ceil(8);
        let tail = array.len() % 8;
        // A retained validity bitmap must serialize the same zero padding as a fresh gather.
        nulls.offset() == 0 && fits(nulls.buffer(), bytes)
            && (tail == 0 || nulls.buffer().as_slice()[bytes - 1] >> tail == 0)
    })
}

fn relayout_packed_children(item: &Field, cells: &[Cell<'_>], width: Option<usize>) -> Result<Option<ArrayRef>> {
    let mut chunks = Vec::with_capacity(cells.len());
    for &cell in cells {
        match cell {
            Cell::Stored(source, array, row) => {
                let (source, values, start, end) = list_span(source, array, row);
                if !rtdb_types::ListValue::supports_numeric_arrow(source) { return Ok(None); }
                let values = values.slice(start, end - start);
                let length = i32::try_from(values.len()).map_err(|_| event_size())?;
                let selected = if matches!(source.data_type(), DataType::Float32 | DataType::Float64) && values.null_count() != 0 {
                    let picks = (0..values.len()).map(|row| Some((0, row))).collect::<Vec<_>>();
                    copy_event_cells(&[values.as_ref()], &picks)?
                } else {
                    let indices = Int32Array::from_iter_values(0..length);
                    arrow_select::take::take(values.as_ref(), &indices, None).map_err(rtdb_columnar::arrow_error)?
                };
                if selected.is_empty() {
                    chunks.push(arrow_array::new_empty_array(item.data_type()));
                    continue;
                }
                let Ok((_, aligned)) = rtdb_columnar::relayout_arrow_column(selected, source, item) else { return Ok(None); };
                if aligned.data_type() != item.data_type() { return Ok(None); }
                chunks.push(aligned);
            }
            _ if width.is_some() => chunks.push(arrow_array::new_null_array(item.data_type(), width.unwrap())),
            _ => {}
        }
    }
    match chunks.as_slice() {
        [] => Ok(Some(arrow_array::new_empty_array(item.data_type()))),
        [child] => Ok(Some(Arc::clone(child))),
        _ => {
            let arrays = chunks.iter().map(|array| array.as_ref()).collect::<Vec<_>>();
            arrow_select::concat::concat(&arrays).map(Some).map_err(rtdb_columnar::arrow_error)
        }
    }
}

fn select<'a>(field: &'a Field, array: &'a dyn Array, row: usize) -> Cell<'a> {
    match field.data_type() {
        DataType::Null if !field.is_nullable() => Cell::Undefined,
        DataType::Null => Cell::Null,
        DataType::Union(fields, UnionMode::Dense) => {
            let union = array.as_union();
            let id = union.type_id(row);
            let child = fields.iter().find(|(tag, _)| *tag == id).expect("Arrow union tag is declared").1;
            select(child, union.child(id).as_ref(), union.value_offset(row))
        }
        DataType::RunEndEncoded(ends, child) => {
            macro_rules! physical {
                ($ty:ty) => {{
                    let run = array.as_run::<$ty>();
                    select(child, run.values().as_ref(), run.get_physical_index(row))
                }};
            }
            match ends.data_type() {
                DataType::Int16 => physical!(Int16Type),
                DataType::Int32 => physical!(Int32Type),
                DataType::Int64 => physical!(Int64Type),
                _ => unreachable!("Validated run end width"),
            }
        }
        _ if array.is_null(row) => Cell::Null,
        _ => Cell::Stored(field, array, row),
    }
}

fn supported(field: &Field, declared: bool) -> bool {
    match field.data_type() {
        DataType::Struct(fields) => fields.iter().all(|field| supported(field, declared)),
        DataType::Union(legs, UnionMode::Dense) => legs.iter().enumerate().all(|(position, (_, leg))| {
            supported(leg, declared) && (!declared || !legs.iter().take(position).any(|(_, earlier)| earlier.name() == leg.name()))
        }),
        DataType::List(child) | DataType::FixedSizeList(child, _) => {
            field.metadata().get(EXTENSION_NAME_KEY).is_none_or(|extension| extension == "set" || extension == "xt/tstz-range")
                && supported(child, declared)
        }
        DataType::Map(entries, _) | DataType::RunEndEncoded(_, entries) => supported(entries, declared),
        _ => true,
    }
}

fn compatible_kind(source: &Field, target: &Field) -> bool {
    if source.metadata().get(EXTENSION_NAME_KEY) != target.metadata().get(EXTENSION_NAME_KEY) { return false; }
    match (source.data_type(), target.data_type()) {
        (DataType::List(_), DataType::List(_)) | (DataType::Struct(_), DataType::Struct(_)) => true,
        (DataType::Map(_, a), DataType::Map(_, b)) => a == b,
        (DataType::FixedSizeList(_, a), DataType::FixedSizeList(_, b)) => a == b,
        (a, b) => a == b,
    }
}

fn same_scalar(source: &Field, target: &Field) -> bool {
    source.metadata() == target.metadata() || from_field(source).is_ok_and(|source| {
        from_field(target).is_ok_and(|target| {
            let target = target.legs();
            source.legs().into_iter().all(|leg| leg == LogicalType::Null || target.contains(&leg))
        })
    })
}

fn is_range(field: &Field) -> bool {
    field.metadata().get(EXTENSION_NAME_KEY).is_some_and(|extension| extension == "xt/tstz-range")
}

fn range_endpoint_type() -> LogicalType {
    LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".to_owned()) })
}

fn range_endpoints(field: &Field, array: &dyn Array, row: usize) -> [i64; 2] {
    use arrow_array::types::{TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType};
    let (field, values, start, _) = list_span(field, array, row);
    [start, start + 1].map(|position| {
        let Cell::Stored(field, values, index) = select(field, values, position) else { unreachable!("Validated range endpoint") };
        match field.data_type() {
            DataType::Timestamp(arrow_schema::TimeUnit::Second, _) => values.as_primitive::<TimestampSecondType>().value(index),
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => values.as_primitive::<TimestampMillisecondType>().value(index),
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => values.as_primitive::<TimestampMicrosecondType>().value(index),
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => values.as_primitive::<TimestampNanosecondType>().value(index),
            _ => unreachable!("Validated range endpoint"),
        }
    })
}

fn admitted(target: &Field, cell: Cell<'_>) -> bool {
    match target.data_type() {
        DataType::RunEndEncoded(_, values) => return admitted(values, cell),
        DataType::Union(legs, UnionMode::Dense) => return legs.iter().any(|(_, leg)| admitted(leg, cell)),
        _ => {}
    }
    let (source, array, row) = match cell {
        Cell::Stored(field, array, row) => (field, array, row),
        Cell::Null => return target.is_nullable(),
        Cell::Undefined => return true,
    };
    if !compatible_kind(source, target) { return false; }
    if is_range(source) {
        let range = LogicalType::List { kind: ListKind::TimestampTzRange, element: Box::new(range_endpoint_type()) };
        return from_field(target).is_ok_and(|target| target.join(&range) == target);
    }
    match target.data_type() {
        DataType::List(item) | DataType::FixedSizeList(item, _) | DataType::Map(item, _) => {
            let (source, array, start, end) = list_span(source, array, row);
            (start..end).all(|row| admitted(item, select(source, array, row)))
        }
        DataType::Struct(targets) => {
            let array = array.as_struct();
            array.fields().iter().enumerate().all(|(position, field)| {
                targets.iter().find(|target| target.name() == field.name()).is_some_and(|target| {
                    admitted(target, select(field, array.column(position).as_ref(), row))
                })
            }) && targets.iter().all(|target| {
                admitted(target, Cell::Null) || array.fields().iter().any(|field| field.name() == target.name())
            })
        }
        _ => same_scalar(source, target),
    }
}

fn run_positions<'a>(cells: &[Cell<'a>]) -> (Vec<usize>, Vec<Cell<'a>>) {
    // Compare before widening the layout, which could make distinct structs identical.
    let mut ends = Vec::new();
    let mut values = Vec::new();
    for (index, &cell) in cells.iter().enumerate() {
        if cells.get(index + 1).is_none_or(|&next| !same_cell(cell, next)) {
            ends.push(index + 1);
            values.push(cell);
        }
    }
    (ends, values)
}

fn gather_runs(target: &Field, run_ends: Vec<usize>, cells: &[Cell<'_>]) -> Result<Option<ArrayRef>> {
    let DataType::RunEndEncoded(ends, value) = target.data_type() else { unreachable!("Expected a run-end layout") };
    let Some(values) = gather(value, cells)? else { return Ok(None); };
    let width = match ends.data_type() {
        DataType::Int16 => RunEndWidth::I16,
        DataType::Int32 => RunEndWidth::I32,
        DataType::Int64 => RunEndWidth::I64,
        _ => unreachable!("Validated run end width"),
    };
    pack_runs(target.data_type(), width, run_ends, values.as_ref()).map(Some)
}

fn gather(target: &Field, cells: &[Cell<'_>]) -> Result<Option<ArrayRef>> {
    match target.data_type() {
        DataType::RunEndEncoded(..) => {
            let (ends, values) = run_positions(cells);
            gather_runs(target, ends, &values)
        }
        DataType::Union(legs, UnionMode::Dense) => {
            let mut picks = vec![Vec::new(); legs.len()];
            let mut ids = Vec::with_capacity(cells.len());
            let mut offsets = Vec::with_capacity(cells.len());
            for &cell in cells {
                let mut matching = legs.iter().enumerate().filter(|(_, (_, leg))| admitted(leg, cell));
                let (position, id) = if matches!(cell, Cell::Undefined) {
                    (0, legs.iter().next().expect("A declared union has legs").0)
                } else {
                    let (position, (id, _)) = matching.next().expect("The selected cell has a compatible union leg");
                    if matching.next().is_some() { return Ok(None); }
                    (position, id)
                };
                ids.push(id);
                offsets.push(i32::try_from(picks[position].len())
                    .map_err(|_| Anomaly::incorrect("rtdb/union-offset-overflow", "Union child exceeds i32 offsets").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded))?);
                picks[position].push(cell);
            }
            let mut values = Vec::with_capacity(legs.len());
            for ((_, leg), picks) in legs.iter().zip(&picks) {
                let Some(array) = gather(leg, picks)? else { return Ok(None); };
                values.push(array);
            }
            let array = UnionArray::try_new(legs.clone(), ScalarBuffer::from(ids), Some(ScalarBuffer::from(offsets)), values)
                .map_err(rtdb_columnar::arrow_error)?;
            Ok(Some(Arc::new(array)))
        }
        DataType::Struct(fields) => {
            let mut valid = NullBufferBuilder::new(cells.len());
            for &cell in cells {
                valid.append(matches!(cell, Cell::Stored(..)));
            }
            let mut arrays = Vec::with_capacity(fields.len());
            let mut picks = Vec::with_capacity(cells.len());
            for field in fields {
                picks.clear();
                picks.extend(cells.iter().map(|&cell| {
                    match cell {
                        Cell::Stored(_, array, index) => {
                            let array = array.as_struct();
                            match array.fields().iter().position(|source| source.name() == field.name()) {
                                Some(position) => select(&array.fields()[position], array.column(position).as_ref(), index),
                                None => Cell::Null,
                            }
                        }
                        Cell::Null | Cell::Undefined => Cell::Undefined,
                    }
                }));
                let Some(array) = gather(field, &picks)? else { return Ok(None); };
                arrays.push(array);
            }
            rtdb_columnar::build_struct_array(fields.clone(), &arrays, cells.len(), valid.finish()).map(Some)
        }
        DataType::FixedSizeList(item, 2) if is_range(target) => {
            if cells.len() > i32::MAX as usize / 2 {
                return Err(Anomaly::incorrect("rtdb/list-offset-overflow", "List children exceed i32 offsets").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded));
            }
            // Range values store the endpoint integers verbatim and write them in the canonical UTC layout.
            let field = rtdb_columnar::to_field(item.name(), &range_endpoint_type())?;
            let mut valid = NullBufferBuilder::new(cells.len());
            let mut endpoints = Vec::new();
            let mut picks = Vec::new();
            for &cell in cells {
                if let Cell::Stored(source, array, row) = cell {
                    valid.append_non_null();
                    picks.push(Some(endpoints.len()));
                    endpoints.extend(range_endpoints(source, array, row));
                } else {
                    valid.append_null();
                    picks.push(None);
                }
            }
            let endpoints = TimestampMicrosecondArray::from(endpoints).with_timezone("UTC");
            let picks = picks.into_iter().flat_map(|index| match index {
                Some(index) => [Cell::Stored(&field, &endpoints, index), Cell::Stored(&field, &endpoints, index + 1)],
                None => [Cell::Undefined; 2],
            }).collect::<Vec<_>>();
            let Some(child) = gather(item, &picks)? else { return Ok(None); };
            let data = ArrayData::builder(target.data_type().clone()).len(cells.len()).nulls(valid.finish())
                .add_child_data(child.to_data()).build().map_err(rtdb_columnar::arrow_error)?;
            Ok(Some(make_array(data)))
        }
        DataType::List(item) | DataType::FixedSizeList(item, _) | DataType::Map(item, _) => {
            let width = match target.data_type() { DataType::FixedSizeList(_, width) => Some(*width as usize), _ => None };
            let packed = !target.metadata().contains_key(EXTENSION_NAME_KEY) && rtdb_types::ListValue::supports_numeric_arrow(item)
                && cells.iter().any(|&cell| match cell {
                    Cell::Stored(field, array, row) => {
                        let (item, values, start, end) = list_span(field, array, row);
                        has_packed_null_payload(item, values, start, end)
                    }
                    _ => false,
                });
            let mut children = Vec::new();
            let mut offsets = vec![0i32];
            let mut length = 0i32;
            let mut valid = NullBufferBuilder::new(cells.len());
            for &cell in cells {
                let added = match cell {
                    Cell::Stored(field, array, index) => {
                        let (field, array, start, end) = list_span(field, array, index);
                        if !packed { children.extend((start..end).map(|index| select(field, array, index))); }
                        valid.append_non_null();
                        end - start
                    }
                    Cell::Null | Cell::Undefined => {
                        if !packed { children.extend(std::iter::repeat_n(Cell::Undefined, width.unwrap_or(0))); }
                        valid.append_null();
                        width.unwrap_or(0)
                    }
                };
                let overflow = || Anomaly::incorrect("rtdb/list-offset-overflow", "List children exceed i32 offsets").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded);
                let offset = i32::try_from(added).map_err(|_| overflow())?;
                length = length.checked_add(offset).ok_or_else(overflow)?;
                if width.is_none() { offsets.push(length); }
            }
            let child = if packed {
                match exact_packed_children(item, cells, width, length as usize) {
                    Some(child) => child,
                    None => {
                        let Some(child) = relayout_packed_children(item, cells, width)? else { return Ok(None); };
                        child
                    }
                }
            } else {
                let Some(child) = gather(item, &children)? else { return Ok(None); };
                child
            };
            if let DataType::Map(entries, sorted) = target.data_type() {
                let child = child.as_struct();
                if child.column(0).logical_null_count() != 0 {
                    return Err(Anomaly::incorrect("rtdb/null-map-key", "Arrow map keys cannot be null"));
                }
                let array = MapArray::try_new(Arc::clone(entries), OffsetBuffer::new(ScalarBuffer::from(offsets)),
                    child.clone(), valid.finish(), *sorted).map_err(rtdb_columnar::arrow_error)?;
                return Ok(Some(Arc::new(array)));
            }
            let mut data = ArrayData::builder(target.data_type().clone()).len(cells.len()).nulls(valid.finish())
                .add_child_data(child.to_data());
            if width.is_none() { data = data.add_buffer(ScalarBuffer::from(offsets).into_inner()); }
            Ok(Some(make_array(data.build().map_err(rtdb_columnar::arrow_error)?)))
        }
        DataType::Null => Ok(Some(Arc::new(NullArray::new(cells.len())))),
        _ => {
            let mut slots = HashMap::new();
            let mut sources = Vec::new();
            let mut picks = Vec::with_capacity(cells.len());
            for &cell in cells {
                let pick = match cell {
                    Cell::Stored(_, array, index) => {
                        let source = *slots.entry(std::ptr::from_ref(array)).or_insert_with(|| {
                            let source = sources.len();
                            sources.push(array);
                            source
                        });
                        Some((source, index))
                    }
                    _ => None,
                };
                picks.push(pick);
            }
            let empty;
            if sources.is_empty() {
                empty = arrow_array::new_empty_array(target.data_type());
                sources.push(empty.as_ref());
            }
            copy_event_cells(&sources, &picks).map(Some)
        }
    }
}

fn list_span<'a>(field: &'a Field, array: &'a dyn Array, row: usize) -> (&'a Field, &'a dyn Array, usize, usize) {
    match field.data_type() {
        DataType::List(child) => {
            let list = array.as_list::<i32>();
            let offsets = list.value_offsets();
            (child, list.values().as_ref(), offsets[row] as usize, offsets[row + 1] as usize)
        }
        DataType::FixedSizeList(child, width) => {
            let list = array.as_fixed_size_list();
            let width = *width as usize;
            (child, list.values().as_ref(), row * width, (row + 1) * width)
        }
        DataType::Map(child, _) => {
            let map = array.as_map();
            let offsets = map.value_offsets();
            (child, map.entries(), offsets[row] as usize, offsets[row + 1] as usize)
        }
        _ => unreachable!("Expected a list source"),
    }
}

fn same_cell(left: Cell<'_>, right: Cell<'_>) -> bool {
    match (left, right) {
        (Cell::Null, Cell::Null) | (Cell::Undefined, Cell::Undefined) => true,
        (Cell::Stored(a, left, i), Cell::Stored(b, right, j)) => {
            if std::ptr::eq(a, b) && std::ptr::eq(left, right) && i == j { return true; }
            if !compatible_kind(a, b) { return false; }
            if is_range(a) { return range_endpoints(a, left, i) == range_endpoints(b, right, j); }
            match a.data_type() {
                DataType::Float32 => left.as_primitive::<Float32Type>().value(i).to_bits() == right.as_primitive::<Float32Type>().value(j).to_bits(),
                DataType::Float64 => left.as_primitive::<Float64Type>().value(i).to_bits() == right.as_primitive::<Float64Type>().value(j).to_bits(),
                DataType::List(_) | DataType::FixedSizeList(..) | DataType::Map(..) => {
                    let (a, left, start, end) = list_span(a, left, i);
                    let (b, right, other, limit) = list_span(b, right, j);
                    end - start == limit - other
                        && (start..end).zip(other..limit).all(|(i, j)| same_cell(select(a, left, i), select(b, right, j)))
                }
                DataType::Struct(_) => {
                    let left = left.as_struct();
                    let right = right.as_struct();
                    left.fields().len() == right.fields().len()
                        && left.fields().iter().zip(right.fields()).enumerate().all(|(position, (a, b))| {
                            a.name() == b.name() && same_cell(
                                select(a, left.column(position).as_ref(), i),
                                select(b, right.column(position).as_ref(), j),
                            )
                        })
                }
                _ => same_scalar(a, b) && left.slice(i, 1).to_data() == right.slice(j, 1).to_data(),
            }
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_columnar::{MemoryBudget, copy_array, to_field};
    use rtdb_types::{EventHeader, EventOp, Iid, Interval, ListKind, ScalarType, StructFields, Timestamp, Value};
    use crate::{data::{StoredEvent, StoredOperation, copy_encoded_batch_with_recency,
        copy_compaction_batch, copy_event_batch_with_recency}, ipc::write_stream};

    struct ArrowOnly<'a>(PutColumn<'a>);

    impl EncodedEvent for ArrowOnly<'_> {
        fn header(&self) -> EventHeader {
            EventHeader { position: None, iid: Iid([1; 16]), system_from: Timestamp(1), commit_position: 0, valid_time: Interval::ALL, op: EventOp::Put }
        }
        fn recency(&self) -> Option<Timestamp> { None }
        fn put_within(&self, _: &StructFields<LogicalType>) -> bool { true }
        fn put_cell(&self, _: &str, _: &LogicalType) -> Result<std::borrow::Cow<'_, Value>> {
            panic!("Arrow event payload must stay in its arrays")
        }
        fn put_column(&self, _: &str) -> Option<PutColumn<'_>> {
            Some(match self.0 {
                PutColumn::Absent => PutColumn::Absent,
                PutColumn::Stored(field, array, offset) => PutColumn::Stored(field, array, offset),
            })
        }
    }

    fn stored(values: Vec<Value>) -> Vec<StoredEvent> {
        values.into_iter().map(|value| {
            let header = ArrowOnly(PutColumn::Absent).header();
            StoredEvent { position: header.position, iid: header.iid, system_from: header.system_from, commit_position: 0, valid_time: header.valid_time,
                origin: None, entity_key: None, recency: None, operation: StoredOperation::Put(StructFields::from([("v".into(), value)])) }
        }).collect()
    }

    fn assert_same_ipc(events: &[ArrowOnly<'_>], values: Vec<Value>, logical: LogicalType) {
        let fields = StructFields::from([("v".into(), logical)]);
        let expected = copy_event_batch_with_recency(&stored(values), Some(fields.clone()), None, false).unwrap();
        let memory = MemoryBudget::new(8 << 20);
        let actual = copy_compaction_batch(events, Some(fields), None, false, false, &memory).unwrap();
        assert_eq!(write_stream(&expected.schema(), &[expected.clone()]).unwrap(),
            write_stream(&actual.batch().schema(), &[actual.batch().clone()]).unwrap());
        drop(actual);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn run_end_vectors_keep_the_scalar_writers_run_boundaries_and_bytes() {
        for kind in [ListKind::List, ListKind::FixedSizeList(3)] {
            let item = LogicalType::from_legs([LogicalType::Scalar(ScalarType::F32), LogicalType::Scalar(ScalarType::F64), LogicalType::Null]);
            let plain = LogicalType::List { kind: kind.clone(), element: Box::new(item) }.maybe();
            let target = LogicalType::RunEndEncoded { width: RunEndWidth::I16, value: Box::new(plain.clone()) };
            let source = LogicalType::RunEndEncoded { width: RunEndWidth::I32, value: Box::new(plain.clone()) };
            let vector = |zero, payload| {
                let values = vec![Value::F32(zero), Value::F64(f64::from_bits(payload)), Value::Null];
                if matches!(kind, ListKind::List) { Value::list(values) } else { Value::FixedSizeList(values.into()) }
            };
            let nan1 = 0x7ff8_0000_0000_0042;
            let nan2 = 0x7ff8_0000_0000_0099;
            let negative = vector(-0.0, nan1);
            let positive = vector(0.0, nan1);
            let changed = vector(0.0, nan2);
            let left = copy_array(&source, &[Value::Null, negative.clone(), negative.clone()]).unwrap();
            let left = left.slice(1, 2);
            let right = copy_array(&plain, &[negative.clone(), positive.clone(), changed.clone(), Value::Null, Value::Null]).unwrap();
            let a = to_field("v", &source).unwrap();
            let b = to_field("v", &plain).unwrap();
            let events = [( &a, &left), (&b, &right)].into_iter().flat_map(|(field, array)| {
                (0..array.len()).map(move |row| ArrowOnly(PutColumn::Stored(field, array.as_ref(), row)))
            }).collect::<Vec<_>>();
            assert_same_ipc(&events, vec![negative.clone(), negative.clone(), negative, positive, changed, Value::Null, Value::Null], target);
        }
    }

    #[test]
    fn dense_union_uses_selected_rows_and_components_to_admit_a_plain_vector() {
        let component = LogicalType::Scalar(ScalarType::F32).maybe();
        let vector_type = LogicalType::List { kind: ListKind::List, element: Box::new(component) }.maybe();
        let expanded_vector = LogicalType::List { kind: ListKind::List, element: Box::new(LogicalType::from_legs([
            LogicalType::Scalar(ScalarType::F32), LogicalType::Scalar(ScalarType::F64), LogicalType::Null])) };
        let source = LogicalType::from_legs([expanded_vector, LogicalType::Scalar(ScalarType::Utf8), LogicalType::Null]);
        let vector = Value::list(vec![Value::F32(-0.0), Value::Null, Value::F32(f32::from_bits(0x7fc0_0042))]);
        let extra = Value::list(vec![Value::F64(4321.0)]);
        let field = to_field("v", &source).unwrap();
        let array = copy_array(&source, &[extra, vector.clone(), Value::Null, Value::Utf8("unused".into())]).unwrap();
        let events = [
            ArrowOnly(PutColumn::Stored(&field, array.as_ref(), 2)),
            ArrowOnly(PutColumn::Stored(&field, array.as_ref(), 1)),
            ArrowOnly(PutColumn::Absent),
            ArrowOnly(PutColumn::Stored(&field, array.as_ref(), 1)),
        ];
        assert_same_ipc(&events, vec![Value::Null, vector.clone(), Value::Null, vector], vector_type);
    }

    #[test]
    fn a_named_union_leg_can_widen_vector_nullability_without_reinterpreting_values() {
        use arrow_schema::UnionFields;

        let float = LogicalType::Scalar(ScalarType::F32);
        let optional_components = LogicalType::List { kind: ListKind::List, element: Box::new(float.clone().maybe()) };
        let required_components = LogicalType::List { kind: ListKind::List, element: Box::new(float) };
        let with_nulls = to_field("other", &optional_components).unwrap();
        let without_nulls = to_field("list", &required_components).unwrap();
        let children = UnionFields::try_new([3, 7], [with_nulls, without_nulls]).unwrap();
        let first = Value::list(vec![Value::F32(-0.0), Value::Null]);
        let second = Value::list(vec![Value::F32(f32::from_bits(0x7fc0_0042))]);
        let a = copy_array(&optional_components, std::slice::from_ref(&first)).unwrap();
        let b = copy_array(&required_components, std::slice::from_ref(&second)).unwrap();
        let array = UnionArray::try_new(children, ScalarBuffer::from(vec![7, 3]),
            Some(ScalarBuffer::from(vec![0, 0])), vec![a, b]).unwrap();
        let field = Field::new("v", array.data_type().clone(), false);
        let events = [0, 1].map(|row| ArrowOnly(PutColumn::Stored(&field, &array, row)));
        assert_same_ipc(&events, vec![second, first], optional_components);
    }

    #[test]
    fn copying_a_packed_numeric_union_keeps_legacy_tags_on_null_components() {
        use arrow_array::{Float32Array, Float64Array};
        use arrow_buffer::NullBuffer;

        let item = LogicalType::from_legs([LogicalType::Scalar(ScalarType::F32), LogicalType::Scalar(ScalarType::F64), LogicalType::Null]);
        let ty = LogicalType::List { kind: ListKind::List, element: Box::new(item) };
        let field = to_field("v", &ty).unwrap();
        let DataType::List(item) = field.data_type() else { unreachable!() };
        let DataType::Union(legs, _) = item.data_type() else { unreachable!() };
        let numbers = UnionArray::try_new(legs.clone(), ScalarBuffer::from(vec![0, 1]), Some(ScalarBuffer::from(vec![0, 0])), vec![
            Arc::new(Float32Array::from(vec![-0.0])),
            Arc::new(Float64Array::new(ScalarBuffer::from(vec![f64::from_bits(0x7ff8_0000_0000_0042)]), Some(NullBuffer::new_null(1)))),
            Arc::new(NullArray::new(0)),
        ]).unwrap();
        let values = make_array(ArrayData::builder(field.data_type().clone()).len(1)
            .add_buffer(ScalarBuffer::from(vec![0_i32, 2]).into_inner()).add_child_data(numbers.to_data()).build().unwrap());
        let docs = rtdb_columnar::build_struct_array(vec![field].into(), &[values], 1, None).unwrap();
        let memory = MemoryBudget::new(8 << 20);
        let input = crate::data::owned_put_batch(docs, &[ArrowOnly(PutColumn::Absent).header()], None, &memory).unwrap();
        let input = crate::data::EventBatch::new(input.batch().clone()).unwrap();
        let fields = Some(StructFields::from([("v".into(), ty)]));
        let row = input.borrow_event(0).unwrap();
        let events = [ArrowOnly(row.put_column("v").unwrap()), ArrowOnly(row.put_column("v").unwrap())];
        let expected = copy_event_batch_with_recency(&[input.read_event(0).unwrap(), input.read_event(0).unwrap()], fields.clone(), None, false).unwrap();
        let actual = copy_compaction_batch(&events, fields, None, false, false, &memory).unwrap();
        let operation = actual.batch().column_by_name("op").unwrap().as_union();
        let list = operation.child(operation.type_id(0)).as_struct().column(0).as_list::<i32>();
        assert_eq!(list.values().as_union().type_ids().as_ref(), &[0, 1, 0, 1]);
        assert_eq!(write_stream(expected.schema_ref(), std::slice::from_ref(&expected)).unwrap(),
            write_stream(actual.batch().schema_ref(), std::slice::from_ref(actual.batch())).unwrap());
    }

    #[test]
    fn packed_fixed_lists_compact_sliced_unions_and_pad_masked_children() {
        use arrow_array::{Float32Array, Float64Array};
        use arrow_buffer::NullBuffer;

        let item = LogicalType::from_legs([LogicalType::Scalar(ScalarType::F32), LogicalType::Scalar(ScalarType::F64), LogicalType::Null]);
        let ty = LogicalType::List { kind: ListKind::FixedSizeList(2), element: Box::new(item) }.maybe();
        let field = to_field("v", &ty).unwrap();
        let DataType::FixedSizeList(item, 2) = field.data_type() else { unreachable!() };
        let DataType::Union(legs, _) = item.data_type() else { unreachable!() };
        let payload = 0x7ff8_0000_0000_0042;
        let numbers = UnionArray::try_new(legs.clone(), vec![0, 0, 1, 0, 2, 1, 0, 1].into(),
            Some(vec![2, 0, 1, 1, 0, 0, 0, 1].into()), vec![
                Arc::new(Float32Array::from(vec![-0.0, 7.0, 9.0])),
                Arc::new(Float64Array::new(vec![3.0, f64::from_bits(payload)].into(), Some(NullBuffer::from(vec![true, false])))),
                Arc::new(NullArray::new(1)),
            ]).unwrap();
        let values = make_array(ArrayData::builder(field.data_type().clone()).len(4)
            .add_child_data(numbers.to_data()).nulls(Some(NullBuffer::from(vec![true, true, false, true])))
            .build().unwrap()).slice(1, 3);
        let events = [
            ArrowOnly(PutColumn::Stored(&field, values.as_ref(), 2)),
            ArrowOnly(PutColumn::Stored(&field, values.as_ref(), 1)),
            ArrowOnly(PutColumn::Stored(&field, values.as_ref(), 0)),
            ArrowOnly(PutColumn::Absent),
        ];
        let expected = [2, 1, 0].into_iter().map(|row| rtdb_columnar::read_value(&field, values.as_ref(), row).unwrap())
            .chain([Value::Null]).collect();
        assert_same_ipc(&events, expected, ty);
    }

    #[test]
    fn adjacent_packed_lists_reuse_compact_numeric_buffers() {
        use arrow_array::{Int64Array, ListArray};
        use arrow_buffer::NullBuffer;

        let ty = LogicalType::list(LogicalType::Scalar(ScalarType::I64).maybe());
        let field = to_field("v", &ty).unwrap();
        let DataType::List(item) = field.data_type() else { unreachable!() };
        let numbers = Int64Array::new(vec![4, i64::MIN, 7].into(), Some(NullBuffer::from(vec![true, false, true])));
        let lists = ListArray::try_new(Arc::clone(item), OffsetBuffer::new(vec![0i32, 2, 3].into()), Arc::new(numbers.clone()), None).unwrap();
        let cells = [Cell::Stored(&field, &lists, 0), Cell::Stored(&field, &lists, 1)];
        let gathered = gather(&field, &cells).unwrap().unwrap();
        let child = gathered.as_list::<i32>().values().as_primitive::<Int64Type>();
        assert_eq!(child.values().inner().data_ptr(), numbers.values().inner().data_ptr());
        assert_eq!(child.values().as_ref(), &[4, i64::MIN, 7]);
        assert!(child.is_null(1));
    }

    #[test]
    fn packed_children_relayout_legacy_union_tags_to_the_declared_order() {
        use arrow_array::{Float32Array, Float64Array};
        use arrow_buffer::NullBuffer;
        use arrow_schema::UnionFields;

        let item_type = LogicalType::from_legs([LogicalType::Scalar(ScalarType::F32), LogicalType::Scalar(ScalarType::F64), LogicalType::Null]);
        let ty = LogicalType::list(item_type);
        let target = to_field("v", &ty).unwrap();
        let DataType::List(item) = target.data_type() else { unreachable!() };
        let DataType::Union(legs, _) = item.data_type() else { unreachable!() };
        let legacy = UnionFields::try_new([8, 2, 7], [2, 1, 0].map(|index| Arc::clone(&legs.get(index).unwrap().1))).unwrap();
        let payload = 0x7ff8_0000_0000_0042;
        let values = UnionArray::try_new(legacy, vec![2, 7].into(), Some(vec![0, 0].into()), vec![
            Arc::new(NullArray::new(0)),
            Arc::new(Float64Array::new(vec![f64::from_bits(payload)].into(), Some(NullBuffer::new_null(1)))),
            Arc::new(Float32Array::from(vec![-0.0])),
        ]).unwrap();
        let legacy_type = DataType::List(Arc::new(item.as_ref().clone().with_data_type(values.data_type().clone())));
        let source = Field::new("v", legacy_type.clone(), false);
        let values = make_array(ArrayData::builder(legacy_type).len(1)
            .add_buffer(ScalarBuffer::from(vec![0i32, 2]).into_inner()).add_child_data(values.to_data()).build().unwrap());
        let result = gather(&target, &[Cell::Stored(&source, values.as_ref(), 0)]).unwrap().unwrap();
        let child = result.as_list::<i32>().values().as_union();
        assert_eq!(child.type_ids().as_ref(), &[1, 0]);
        assert_eq!(child.offsets().unwrap().as_ref(), &[0, 0]);
        assert_eq!(child.child(1).as_primitive::<Float64Type>().value(0).to_bits(), payload);
        assert!(child.child(1).is_null(0));
        assert_eq!(child.child(0).as_primitive::<Float32Type>().value(0).to_bits(), (-0.0f32).to_bits());
    }

    #[test]
    fn timestamp_ranges_read_raw_endpoint_ticks_from_selected_legs() {
        use arrow_array::{FixedSizeListArray, TimestampNanosecondArray};
        use arrow_schema::{TimeUnit as ArrowUnit, UnionFields};

        let nano = Field::new("nanos", DataType::Timestamp(ArrowUnit::Nanosecond, None), false);
        let micro = Field::new("micros", DataType::Timestamp(ArrowUnit::Microsecond, Some("UTC".into())), false);
        let legs = UnionFields::try_new([3, 6], [nano, micro]).unwrap();
        let endpoints = Arc::new(UnionArray::try_new(legs, vec![3, 6, 6, 3].into(), Some(vec![0, 0, 1, 1].into()),
            vec![Arc::new(TimestampNanosecondArray::from(vec![-1001, 7999])),
                Arc::new(TimestampMicrosecondArray::from(vec![2000, -2333]).with_timezone("UTC"))]).unwrap());
        let item = Arc::new(Field::new("item", endpoints.data_type().clone(), false));
        let ranges = FixedSizeListArray::try_new(item, 2, endpoints, None).unwrap();
        let mut field = Field::new("v", ranges.data_type().clone(), false);
        field.set_metadata(HashMap::from([(EXTENSION_NAME_KEY.into(), "xt/tstz-range".into())]));
        let target = LogicalType::List { kind: ListKind::TimestampTzRange, element: Box::new(range_endpoint_type()) };
        let target = LogicalType::RunEndEncoded { width: RunEndWidth::I32, value: Box::new(target) };
        let events = [1, 0, 1, 1].map(|row| ArrowOnly(PutColumn::Stored(&field, &ranges, row)));
        let values = [1, 0, 1, 1].map(|row| rtdb_columnar::read_value(&field, &ranges, row).unwrap()).to_vec();
        assert_same_ipc(&events, values, target);
    }

    #[test]
    fn a_named_union_alias_writes_its_logical_type_from_shared_utf8_storage() {
        use arrow_array::StringArray;
        use arrow_schema::UnionFields;

        let uri = LogicalType::Scalar(ScalarType::Uri);
        let legs = UnionFields::try_new([1, 4], [
            Field::new("uri", DataType::Utf8, false), to_field("other", &uri).unwrap(),
        ]).unwrap();
        let values = Arc::new(StringArray::from(vec!["https://example.com/a", "https://example.com/b"]));
        let array = UnionArray::try_new(legs, vec![1, 4, 1].into(), Some(vec![0, 1, 0].into()),
            vec![values.clone(), values]).unwrap();
        let field = Field::new("v", array.data_type().clone(), false);
        let target = LogicalType::RunEndEncoded { width: RunEndWidth::I32, value: Box::new(uri) };
        let events = [0, 1, 2, 0].map(|row| ArrowOnly(PutColumn::Stored(&field, &array, row)));
        let values = [0, 1, 2, 0].map(|row| rtdb_columnar::read_value(&field, &array, row).unwrap()).to_vec();
        assert_same_ipc(&events, values, target);
    }

    #[test]
    fn a_typed_run_copy_does_not_hold_the_scalar_read_reservation() {
        let list = LogicalType::list(LogicalType::Scalar(ScalarType::F32));
        let value = Value::list(vec![Value::F32(-0.0); 256]);
        let field = to_field("v", &list).unwrap();
        let array = copy_array(&list, std::slice::from_ref(&value)).unwrap();
        let events = (0..32).map(|_| ArrowOnly(PutColumn::Stored(&field, array.as_ref(), 0))).collect::<Vec<_>>();
        let target = LogicalType::RunEndEncoded { width: RunEndWidth::I32, value: Box::new(list) };
        let target_field = to_field("v", &target).unwrap();
        let scratch = rtdb_columnar::estimate_read_cell_bytes(&field, array.as_ref(), 0).unwrap() * events.len();
        let copy = estimate_event_copy_cell_bytes(&field, array.as_ref(), 0).unwrap();
        let padding = estimate_event_null_cell_bytes(&target_field).unwrap();
        let budget = (copy + padding) * 4 + scratch / 4;
        let memory = MemoryBudget::new(budget);
        let fields = Some(StructFields::from([("v".into(), target)]));
        let expected = copy_event_batch_with_recency(&stored(vec![value; events.len()]), fields.clone(), None, false).unwrap();
        let actual = copy_compaction_batch(&events, fields, None, false, false, &memory).unwrap();
        assert_eq!(write_stream(expected.schema_ref(), std::slice::from_ref(&expected)).unwrap(),
            write_stream(actual.batch().schema_ref(), std::slice::from_ref(actual.batch())).unwrap());
        drop(actual);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn compaction_aligns_selected_nested_union_cells_when_whole_layout_scratch_is_unfunded() {
        let int = LogicalType::Scalar(ScalarType::I64);
        let text = LogicalType::Scalar(ScalarType::Utf8);
        let target = LogicalType::list(int.join(&text));
        let fields = Some(StructFields::from([("v".into(), target.clone())]));
        let field = to_field("v", &target).unwrap();
        let values = (0..64).map(|i| Value::list(vec![
            if i % 2 == 0 { Value::I64(i) } else { Value::Utf8(i.to_string()) }
        ])).collect::<Vec<_>>();
        let sources = values.iter().map(|value| {
            let logical = value.logical_type();
            (to_field("v", &logical).unwrap(), copy_array(&logical, std::slice::from_ref(value)).unwrap())
        }).collect::<Vec<_>>();
        let events = sources.iter().map(|(field, array)| ArrowOnly(PutColumn::Stored(field, array.as_ref(), 0))).collect::<Vec<_>>();
        let memory = MemoryBudget::new(768 << 10);
        let (mut builder, _) = super::super::event_builder(events.len(), fields.as_ref(), &memory).unwrap();
        let refs = events.iter().collect::<Vec<_>>();
        assert!(super::super::columns::gather_column(&refs, &field, &target, Some(&mut builder)).unwrap().is_none());
        drop(builder);
        let actual = copy_compaction_batch(&events, fields.clone(), None, false, false, &memory).unwrap();
        let expected = copy_event_batch_with_recency(&stored(values), fields, None, false).unwrap();
        assert_eq!(write_stream(expected.schema_ref(), std::slice::from_ref(&expected)).unwrap(),
            write_stream(actual.batch().schema_ref(), std::slice::from_ref(actual.batch())).unwrap());
        drop(actual);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn plain_timestamp_output_keeps_scalar_ipc_when_neither_selection_plan_fits() {
        use arrow_array::TimestampNanosecondArray;
        use arrow_buffer::NullBuffer;

        let zone = "Europe/Paris";
        let ty = LogicalType::Scalar(ScalarType::Timestamp {
            unit: TimeUnit::Nanosecond, timezone: Some(zone.to_owned()),
        }).maybe();
        let fields = StructFields::from([("v".into(), ty.clone())]);
        let target = to_field("v", &ty).unwrap();
        let values = TimestampNanosecondArray::new(
            vec![-1001, i64::MAX, 17].into(),
            Some(NullBuffer::from(vec![true, false, true])),
        ).with_timezone(zone);
        let positions = (0..512).map(|row| [Some(2), None, Some(0), Some(1)][row % 4]).collect::<Vec<_>>();
        let events = positions.iter().map(|&row| ArrowOnly(match row {
            Some(row) => PutColumn::Stored(&target, &values, row),
            None => PutColumn::Absent,
        })).collect::<Vec<_>>();
        let expected = stored(positions.into_iter().map(|row| match row {
            Some(row) => rtdb_columnar::read_value(&target, &values, row).unwrap(),
            None => Value::Null,
        }).collect());
        let reference_memory = MemoryBudget::new(8 << 20);
        let (base, _) = super::super::event_builder(events.len(), Some(&fields), &reference_memory).unwrap();
        let memory = MemoryBudget::new(reference_memory.used_bytes() + (32 << 10));
        drop(base);
        let (mut builder, _) = super::super::event_builder(events.len(), Some(&fields), &memory).unwrap();
        let refs = events.iter().collect::<Vec<_>>();
        assert!(super::super::columns::gather_column(&refs, &target, &ty, Some(&mut builder)).unwrap().is_none());
        assert!(builder.try_reserve_scratch(events.len() * 128).is_none());
        drop(builder);

        for compact in [false, true] {
            let (expected, actual) = if compact {
                (
                    copy_event_batch_with_recency(&expected, Some(fields.clone()), None, false).unwrap(),
                    copy_compaction_batch(&events, Some(fields.clone()), None, false, false, &memory).unwrap(),
                )
            } else {
                (
                    super::super::owned_event_batch(&expected, Some(fields.clone()), true, &reference_memory).unwrap().batch().clone(),
                    super::super::owned_event_batch(&events, Some(fields.clone()), true, &memory).unwrap(),
                )
            };
            assert_eq!(write_stream(expected.schema_ref(), std::slice::from_ref(&expected)).unwrap(),
                write_stream(actual.batch().schema_ref(), std::slice::from_ref(actual.batch())).unwrap());
            let held = actual.batch().clone();
            drop(actual);
            assert!(memory.used_bytes() > 0);
            drop(held);
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn native_selection_defers_scalar_sizing_for_a_bounded_layout() {
        let ty = LogicalType::list(LogicalType::Scalar(ScalarType::I64));
        let field = to_field("v", &ty).unwrap();
        let values = copy_array(&ty, &[Value::list(vec![Value::I64(1)])]).unwrap();
        let event = ArrowOnly(PutColumn::Stored(&field, values.as_ref(), 0));
        let memory = MemoryBudget::new(1 << 20);
        let mut builder = BatchBuilder::new(0, &memory).unwrap();
        let output = column(&[&event], &field, &ty, Some(&mut builder), || panic!("Scalar sizing is for fallback"));
        assert!(output.unwrap().is_some());
    }

    #[test]
    fn widening_structs_inside_runs_preserves_distinct_document_shapes() {
        let payload = Value::Map {
            entries: vec![(Value::I64(1), Value::list(vec![Value::F64(-0.0)]))],
            sorted: false,
        };
        let left = Value::Struct(StructFields::from([("vector".into(), payload.clone())]));
        let right = Value::Struct(StructFields::from([("vector".into(), payload), ("marker".into(), Value::Null)]));
        let wide = right.logical_type();
        let narrow = left.logical_type();
        let a = to_field("v", &narrow).unwrap();
        let b = to_field("v", &wide).unwrap();
        let first = copy_array(&narrow, std::slice::from_ref(&left)).unwrap();
        let second = copy_array(&wide, std::slice::from_ref(&right)).unwrap();
        let events = [
            ArrowOnly(PutColumn::Stored(&a, first.as_ref(), 0)),
            ArrowOnly(PutColumn::Stored(&b, second.as_ref(), 0)),
        ];
        let target = LogicalType::RunEndEncoded { width: RunEndWidth::I32, value: Box::new(wide) };
        assert_same_ipc(&events, vec![left, right], target);
    }

    #[test]
    fn absent_cells_and_invalid_reads_report_the_scalar_writers_errors() {
        let target = LogicalType::Scalar(ScalarType::I64);
        let fields = StructFields::from([("v".into(), target)]);
        let memory = MemoryBudget::new(1 << 20);
        let missing = [ArrowOnly(PutColumn::Absent)];
        let error = copy_encoded_batch_with_recency(&missing, Some(fields.clone()), None, false).unwrap_err();
        assert_eq!(error.code, "rtdb/column-type-mismatch");
        assert_eq!(error.message.as_deref(), Some("Value at row 0 does not belong to Scalar(I64)"));
        let error = copy_compaction_batch(&missing, Some(fields.clone()), None, false, false, &memory).unwrap_err();
        assert_eq!(error.code, "rtdb/column-type-mismatch");
        assert_eq!(error.message.as_deref(), Some("Allocation estimate requires values belonging to the declared logical type"));

        let field = to_field("v", &LogicalType::Scalar(ScalarType::Uri)).unwrap();
        let array = arrow_array::StringArray::from(vec!["bad uri"]);
        let events = [ArrowOnly(PutColumn::Absent), ArrowOnly(PutColumn::Stored(&field, &array, 0))];
        let scalar = rtdb_columnar::read_value(&field, &array, 0).unwrap_err();
        let typed = copy_compaction_batch(&events, Some(fields), None, false, false, &memory).unwrap_err();
        assert_eq!((typed.code, typed.message), (scalar.code, scalar.message));
    }

    #[test]
    fn scalar_sizing_errors_precede_native_read_errors() {
        let integer = LogicalType::Scalar(ScalarType::I64);
        let uri = to_field("v", &LogicalType::Scalar(ScalarType::Uri)).unwrap();
        let invalid_uri = arrow_array::StringArray::from(vec!["bad uri"]);
        let bottom = to_field("v", &LogicalType::Nothing).unwrap();
        let nulls = NullArray::new(1);
        let events = [
            ArrowOnly(PutColumn::Stored(&uri, &invalid_uri, 0)),
            ArrowOnly(PutColumn::Stored(&bottom, &nulls, 0)),
        ];
        let memory = MemoryBudget::new(1 << 20);
        for target in [integer.clone(), LogicalType::RunEndEncoded { width: RunEndWidth::I32, value: Box::new(integer) }] {
            let fields = StructFields::from([("v".into(), target)]);
            let error = copy_compaction_batch(&events, Some(fields), None, false, false, &memory).unwrap_err();
            assert_eq!(error.code, "xtdb/zero-leg-read");
            assert_eq!(error.message.as_deref(), Some("The bottom type has no values to read"));
        }
    }
}
