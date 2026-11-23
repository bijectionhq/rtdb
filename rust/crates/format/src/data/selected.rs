use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, ArrayRef, MapArray, NullArray, UnionArray, cast::AsArray, make_array,
    types::{Float32Type, Float64Type, Int16Type, Int32Type, Int64Type}};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, UnionMode};
use rtdb_columnar::{BatchBuilder, ColumnPresence, EXTENSION_NAME_KEY, copy_event_cells, estimate_event_copy_cell_bytes,
    estimate_event_null_cell_bytes, from_field, pack_runs};
use rtdb_types::{Anomaly, LogicalType, Result, RunEndWidth};

use super::{EncodedEvent, PutColumn, event_size};

#[derive(Clone, Copy)]
enum Cell<'a> {
    Stored(&'a Field, &'a dyn Array, usize),
    Null,
    Undefined,
}

pub(super) fn column<E: EncodedEvent>(
    events: &[&E], target: &Field, logical: &LogicalType, builder: Option<&mut BatchBuilder>,
) -> Result<Option<ArrayRef>> {
    if events.is_empty() || events.iter().any(|event| event.put_column(target.name()).is_none()) {
        return Ok(None);
    }
    let reserved = builder.is_some();
    let mismatch = |row| {
        let message = if reserved {
            "Allocation estimate requires values belonging to the declared logical type".into()
        } else {
            format!("Value at row {row} does not belong to {logical:?}")
        };
        Anomaly::incorrect("rtdb/column-type-mismatch", message)
    };
    let mut cells = Vec::with_capacity(events.len());
    let mut source_types = HashMap::new();
    let mut can_gather = true;
    for event in events {
        let cell = match event.put_column(target.name()).expect("Checked all events for stored columns") {
            PutColumn::Absent if logical == &LogicalType::Nothing => Cell::Undefined,
            PutColumn::Absent => Cell::Null,
            PutColumn::Stored(field, array, index) => {
                let reader = match source_types.entry(std::ptr::from_ref(field)) {
                    std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::hash_map::Entry::Vacant(entry) => entry.insert(ColumnPresence::new(field)?),
                };
                can_gather &= reader.validate_stored(array, index)?;
                if reader.is_bottom() { Cell::Undefined } else { select(field, array, index) }
            }
        };
        cells.push(cell);
    }
    if let Some(row) = cells.iter().position(|cell| match cell {
        Cell::Null => !logical.nullable(),
        Cell::Undefined => logical != &LogicalType::Nothing,
        Cell::Stored(..) => false,
    }) && (reserved || row == 0) && can_gather {
        return Err(mismatch(row));
    }
    if !can_gather || !supported(target, true)
        || cells.iter().any(|&cell| matches!(cell, Cell::Stored(field, ..) if !supported(field, false)) || has_packed_null_payload(cell)) {
        return Ok(None);
    }
    for (row, &cell) in cells.iter().enumerate() {
        let fits = match cell {
            Cell::Undefined => logical == &LogicalType::Nothing,
            _ => admitted(target, cell),
        };
        if !fits { return Err(mismatch(row)); }
    }
    let build = || gather(target, &cells);
    if let Some(builder) = builder {
        let padding = estimate_event_null_cell_bytes(target)?;
        let bound = cells.iter().try_fold(target.size().checked_mul(8)
            .and_then(|size| size.checked_add(4096)).ok_or_else(event_size)?, |bytes, &cell| {
            let selected = match cell {
                Cell::Stored(field, array, row) => estimate_event_copy_cell_bytes(field, array, row)?,
                _ => 0,
            };
            selected.checked_add(padding).and_then(|cell| cell.checked_mul(4))
                .and_then(|cell| bytes.checked_add(cell)).ok_or_else(event_size)
        })?;
        match builder.try_column(bound, build) {
            Err(error) if error.code == "rtdb/memory-budget-exceeded" => Ok(None),
            result => result,
        }
    } else {
        build()
    }
}

fn has_packed_null_payload(cell: Cell<'_>) -> bool {
    let Cell::Stored(field, array, row) = cell else { return false; };
    match field.data_type() {
        DataType::Struct(fields) => fields.iter().zip(array.as_struct().columns()).any(|(field, column)| {
            has_packed_null_payload(select(field, column.as_ref(), row))
        }),
        DataType::List(_) | DataType::FixedSizeList(..) | DataType::Map(..) => {
            let (item, values, start, end) = list_span(field, array, row);
            if !field.metadata().contains_key(EXTENSION_NAME_KEY) && rtdb_types::ListValue::supports_numeric_arrow(item) {
                // The packed-list writer can retain the original tag and the underlying bytes of a null component.
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
            } else {
                (start..end).any(|position| has_packed_null_payload(select(item, values, position)))
            }
        }
        _ => false,
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
            field.metadata().get(EXTENSION_NAME_KEY).is_none_or(|extension| extension == "set") && supported(child, declared)
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

fn gather(target: &Field, cells: &[Cell<'_>]) -> Result<Option<ArrayRef>> {
    match target.data_type() {
        DataType::RunEndEncoded(ends, value) => {
            // Compare selected values before widening their layout, which could make distinct structs identical.
            let mut picks = Vec::new();
            let mut run_ends = Vec::new();
            for (index, &pick) in cells.iter().enumerate() {
                if cells.get(index + 1).is_none_or(|&next| !same_cell(pick, next)) {
                    run_ends.push(index + 1);
                    picks.push(pick);
                }
            }
            let Some(values) = gather(value, &picks)? else { return Ok(None); };
            let width = match ends.data_type() {
                DataType::Int16 => RunEndWidth::I16,
                DataType::Int32 => RunEndWidth::I32,
                DataType::Int64 => RunEndWidth::I64,
                _ => unreachable!("Validated run end width"),
            };
            pack_runs(target.data_type(), width, run_ends, values.as_ref()).map(Some)
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
                    .map_err(|_| Anomaly::incorrect("rtdb/union-offset-overflow", "Union child exceeds i32 offsets"))?);
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
        DataType::List(item) | DataType::FixedSizeList(item, _) | DataType::Map(item, _) => {
            let width = match target.data_type() { DataType::FixedSizeList(_, width) => Some(*width as usize), _ => None };
            let mut children = Vec::new();
            let mut offsets = vec![0];
            let mut valid = NullBufferBuilder::new(cells.len());
            for &cell in cells {
                match cell {
                    Cell::Stored(field, array, index) => {
                        let (field, array, start, end) = list_span(field, array, index);
                        children.extend((start..end).map(|index| select(field, array, index)));
                        valid.append_non_null();
                    }
                    Cell::Null | Cell::Undefined => {
                        children.extend(std::iter::repeat_n(Cell::Undefined, width.unwrap_or(0)));
                        valid.append_null();
                    }
                }
                offsets.push(i32::try_from(children.len())
                    .map_err(|_| Anomaly::incorrect("rtdb/list-offset-overflow", "List children exceed i32 offsets"))?);
            }
            let Some(child) = gather(item, &children)? else { return Ok(None); };
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
        let expected = copy_event_batch_with_recency(&[input.read_event(0).unwrap()], fields.clone(), None, false).unwrap();
        let actual = owned_encoded_batch_with_recency(&input.page_events().unwrap(), fields, None, false, &memory).unwrap();
        let operation = actual.batch().column_by_name("op").unwrap().as_union();
        let list = operation.child(operation.type_id(0)).as_struct().column(0).as_list::<i32>();
        assert_eq!(list.values().as_union().type_ids().as_ref(), &[0, 1]);
        assert_eq!(write_stream(expected.schema_ref(), std::slice::from_ref(&expected)).unwrap(),
            write_stream(actual.batch().schema_ref(), std::slice::from_ref(actual.batch())).unwrap());
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
}
