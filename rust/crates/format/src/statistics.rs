use rtdb_types::StructFields;
use std::{collections::BTreeMap, ops::Range, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeListArray, Float64Array, Int32Array, Int64Array,
    ListArray, RecordBatch, RunArray, StringArray, StructArray, UnionArray,
    types::{Int16Type, Int32Type, Int64Type, RunEndIndexType},
};
use arrow_buffer::{
    ArrowNativeType, BooleanBuffer, BooleanBufferBuilder, Buffer, NullBuffer, NullBufferBuilder,
    OffsetBuffer, ScalarBuffer,
};
use arrow_schema::{DataType, Field, UnionMode};
use rtdb_types::{Anomaly, LogicalType, Result, Value};

use crate::bloom::BloomFilter;

enum Mask {
    All,
    Span(Range<usize>),
    Selected { offset: usize, rows: BooleanBuffer },
}

impl Mask {
    fn span(&self, len: usize) -> Range<usize> {
        match self {
            Self::All => 0..len,
            Self::Span(span) => span.clone(),
            Self::Selected { offset, rows } => *offset..*offset + rows.len(),
        }
    }

    fn rows(&self, len: usize) -> impl Iterator<Item = usize> + '_ {
        let (span, selected) = match self {
            Self::Selected { offset, rows } => {
                (0..0, Some(rows.set_indices().map(move |row| *offset + row)))
            }
            _ => (self.span(len), None),
        };
        span.chain(selected.into_iter().flatten())
    }

    fn count_in(&self, span: Range<usize>) -> usize {
        match self {
            Self::Selected { offset, rows } => {
                let start = span.start - *offset;
                rows.inner().count_set_bits_offset(rows.offset() + start, span.len())
            }
            _ => span.len(),
        }
    }

    fn with_parent_validity(&self, array: &dyn Array) -> Option<Self> {
        let Some(validity) = array.nulls().filter(|validity| validity.null_count() != 0) else {
            // An explicit selection keeps unreferenced dense union child slots out of the fold.
            return matches!(self, Self::All).then(|| Self::Span(0..array.len()));
        };
        let span = self.span(array.len());
        let valid = validity.inner().slice(span.start, span.len());
        let rows = match self {
            Self::Selected { rows, .. } => rows & &valid,
            _ => valid,
        };
        Some(Self::Selected { offset: span.start, rows })
    }
}

#[derive(Default)]
struct Statistics {
    flavours: StructFields<Flavour>,
    numeric: BTreeMap<&'static str, (f64, f64)>,
    bytes_seen: bool,
    bloom: Option<BloomFilter>,
}

enum Flavour {
    Presence,
    Child(i32),
    Children(Vec<i32>),
    Bounds(Option<f64>, Option<f64>),
    Bytes(Option<BloomFilter>),
}

struct ColumnSummary {
    name: String,
    root: bool,
    count: i64,
    flavours: StructFields<Flavour>,
}

pub(crate) fn page_statistics(batch: &RecordBatch, calculate_blooms: bool, base: i32) -> Result<StructArray> {
    let columns = event_statistics(batch, calculate_blooms, base)?;
    build_statistics(&columns)
}

pub fn columns_for_event_page(batch: &RecordBatch, calculate_blooms: bool) -> Result<Vec<Value>> {
    event_statistics(batch, calculate_blooms, 0)?.into_iter().map(column_value).collect()
}

fn event_statistics(batch: &RecordBatch, calculate_blooms: bool, base: i32) -> Result<Vec<ColumnSummary>> {
    let schema = batch.schema();
    let op_index = schema.index_of("op").map_err(invalid)?;
    let op = downcast::<UnionArray>(batch.column(op_index).as_ref())?;
    let DataType::Union(fields, _) = op.data_type() else { return Err(invalid("Missing event operation fields")); };
    let has_redacted = fields.iter().find(|(_, field)| field.name() == "redacted")
        .is_some_and(|(id, _)| op.type_ids().contains(&id));
    let mut output = Vec::new();
    for name in ["_iid", "_valid_from", "_valid_to", "_system_from", "_recency"] {
        match schema.index_of(name) {
            Ok(index) => {
                let column = write_column(schema.field(index), batch.column(index).as_ref(), true, calculate_blooms, &Mask::All, &mut output, base, 0)?;
                if name == "_system_from" && has_redacted {
                    output[(column - base) as usize].flavours.insert("redacted?".into(), Flavour::Presence);
                }
            }
            Err(_) if name == "_recency" => {}
            Err(error) => return Err(invalid(error)),
        }
    }
    if let Some((id, field)) = fields.iter().find(|(_, field)| field.name() == "put") {
        let put = downcast::<StructArray>(op.child(id).as_ref())?;
        let DataType::Struct(fields) = field.data_type() else { return Err(invalid("Put operation must be a struct")); };
        for (index, field) in fields.iter().enumerate() { write_column(field, put.column(index).as_ref(), true, calculate_blooms, &Mask::All, &mut output, base, 0)?; }
    }
    Ok(output)
}

pub fn column_metadata(field: &Field, array: &dyn Array, root: bool, calculate_blooms: bool, output: &mut Vec<Value>) -> Result<usize> {
    let base = i32::try_from(output.len()).map_err(invalid)?;
    let mut columns = Vec::new();
    let index = write_column(field, array, root, calculate_blooms, &Mask::All, &mut columns, base, 0)?;
    output.extend(columns.into_iter().map(column_value).collect::<Result<Vec<_>>>()?);
    Ok(index as usize)
}

#[allow(clippy::too_many_arguments)]
fn write_column(field: &Field, array: &dyn Array, root: bool, calculate_blooms: bool, mask: &Mask,
    output: &mut Vec<ColumnSummary>, base: i32, depth: usize) -> Result<i32> {
    if depth > 128 { return Err(invalid("Column metadata nesting exceeds 128 levels")); }
    let mut statistics = Statistics::default();
    let with_bloom = calculate_blooms || (root && field.name() == "_iid");
    fold(field, array, mask, with_bloom, calculate_blooms, output, &mut statistics, base, depth)?;
    let count = match present_cells(field, array, mask) {
        Some(count) => i64::try_from(count).map_err(invalid)?,
        None => {
            let reader = rtdb_columnar::ColumnPresence::new(field)?;
            if field.data_type() == array.data_type() && reader.uses_parent_validity() {
                i64::try_from(validity_count(array, mask)).map_err(invalid)?
            } else if let DataType::RunEndEncoded(ends, _) = field.data_type()
                && field.data_type() == array.data_type()
            {
                match ends.data_type() {
                    DataType::Int16 => run_presence_count(downcast::<RunArray<Int16Type>>(array)?, mask, &reader)?,
                    DataType::Int32 => run_presence_count(downcast::<RunArray<Int32Type>>(array)?, mask, &reader)?,
                    DataType::Int64 => run_presence_count(downcast::<RunArray<Int64Type>>(array)?, mask, &reader)?,
                    _ => return Err(invalid("Invalid run-end index width")),
                }
            } else {
                let mut count = 0i64;
                for row in mask.rows(array.len()) {
                    if reader.is_present(array, row)? { count += 1; }
                }
                count
            }
        }
    };
    for (name, (min, max)) in statistics.numeric {
        statistics.flavours.insert(name.into(), Flavour::Bounds(
            (min != f64::INFINITY).then_some(min), (max != f64::NEG_INFINITY).then_some(max),
        ));
    }
    if statistics.bytes_seen {
        statistics.flavours.insert("bytes".into(), Flavour::Bytes(statistics.bloom));
    }
    let index = i32::try_from(output.len()).map_err(invalid)?;
    let index = index.checked_add(base).ok_or_else(|| Anomaly::incorrect(
        "rtdb.format/invalid-trie-metadata", "Metadata child index overflow",
    ))?;
    output.push(ColumnSummary { name: field.name().clone(), root, count, flavours: statistics.flavours });
    Ok(index)
}

fn column_value(column: ColumnSummary) -> Result<Value> {
    let mut fields = StructFields::from([
        ("col-name".into(), Value::Utf8(column.name)),
        ("root-col?".into(), Value::Bool(column.root)),
        ("count".into(), Value::I64(column.count)),
    ]);
    for (name, flavour) in column.flavours {
        let value = match flavour {
            Flavour::Presence => Value::Bool(true),
            Flavour::Child(index) => Value::I32(index),
            Flavour::Children(indices) => Value::list(indices.into_iter().map(Value::I32).collect()),
            Flavour::Bounds(min, max) => Value::Struct(StructFields::from([
                ("min".into(), min.map_or(Value::Null, Value::F64)),
                ("max".into(), max.map_or(Value::Null, Value::F64)),
            ])),
            Flavour::Bytes(bloom) => {
                let bloom = bloom.map(|bloom| bloom.to_bytes()).transpose()?;
                Value::Struct(StructFields::from([
                    ("bloom".into(), bloom.map_or(Value::Null, Value::Binary)),
                ]))
            }
        };
        fields.insert(name, value);
    }
    Ok(Value::Struct(fields))
}

fn build_statistics(columns: &[ColumnSummary]) -> Result<StructArray> {
    let mut fields = vec![
        Field::new("col-name", DataType::Utf8, false),
        Field::new("root-col?", DataType::Boolean, false),
        Field::new("count", DataType::Int64, false),
    ];
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(columns.iter().map(|column| column.name.as_str()))),
        Arc::new(BooleanArray::from_iter(columns.iter().map(|column| Some(column.root)))),
        Arc::new(Int64Array::from_iter_values(columns.iter().map(|column| column.count))),
    ];
    let flavours: rtdb_types::OrderedSet<&String> = columns.iter().flat_map(|column| column.flavours.keys()).collect();
    for name in flavours {
        let cells = || columns.iter().map(|column| column.flavours.get(name));
        let example = cells().flatten().next().expect("flavour came from a page column");
        let nulls = || (cells().any(|cell| cell.is_none())).then(|| NullBuffer::from(cells().map(|cell| cell.is_some()).collect::<Vec<_>>()));
        let (nullable, array): (bool, ArrayRef) = match example {
            Flavour::Presence => (cells().any(|cell| cell.is_none()), Arc::new(BooleanArray::from_iter(
                cells().map(|cell| cell.map(|_| true)),
            ))),
            Flavour::Child(_) => (true, Arc::new(Int32Array::from_iter(
                cells().map(|cell| match cell { Some(Flavour::Child(index)) => Some(*index), _ => None }),
            ))),
            Flavour::Children(_) => {
                let item = Arc::new(Field::new("$data$", DataType::Int32, false));
                let mut offsets = vec![0i32];
                let mut children = Vec::new();
                for cell in cells() {
                    if let Some(Flavour::Children(indices)) = cell { children.extend_from_slice(indices); }
                    offsets.push(i32::try_from(children.len()).map_err(invalid)?);
                }
                (true, Arc::new(ListArray::try_new(item, OffsetBuffer::new(ScalarBuffer::from(offsets)),
                    Arc::new(Int32Array::from(children)), nulls()).map_err(invalid)?))
            }
            Flavour::Bounds(..) => {
                let mut fields = Vec::with_capacity(2);
                let mut children: Vec<ArrayRef> = Vec::with_capacity(2);
                for (lower, name) in [(true, "min"), (false, "max")] {
                    let bound = |cell: &Flavour| match cell { Flavour::Bounds(min, max) => if lower { *min } else { *max }, _ => None };
                    let nullable = cells().flatten().any(|cell| bound(cell).is_none());
                    fields.push(Field::new(name, DataType::Float64, nullable));
                    children.push(Arc::new(Float64Array::from_iter(cells().map(|cell| cell.and_then(bound)))));
                }
                (true, Arc::new(StructArray::try_new(fields.into(), children, nulls()).map_err(invalid)?))
            }
            Flavour::Bytes(_) => {
                let blooms = cells().map(|cell| match cell {
                    Some(Flavour::Bytes(bloom)) => bloom.as_ref(),
                    _ => None,
                });
                let fields = vec![Field::new("bloom", DataType::Binary, true)].into();
                let bloom = Arc::new(build_blooms(blooms)?);
                let bytes = StructArray::try_new(fields, vec![bloom], nulls()).map_err(invalid)?;
                (true, Arc::new(bytes))
            }
        };
        fields.push(Field::new(name, array.data_type().clone(), nullable));
        arrays.push(array);
    }
    StructArray::try_new(fields.into(), arrays, None).map_err(invalid)
}

fn build_blooms<'a>(
    blooms: impl ExactSizeIterator<Item = Option<&'a BloomFilter>> + Clone,
) -> Result<BinaryArray> {
    let mut offsets = Vec::with_capacity(blooms.len() + 1);
    let mut validity = NullBufferBuilder::new(blooms.len());
    let mut end = 0i32;
    offsets.push(end);
    for bloom in blooms.clone() {
        validity.append(bloom.is_some());
        if let Some(bloom) = bloom {
            let size = i32::try_from(bloom.serialized_size()).map_err(|_| bloom_allocation_overflow())?;
            end = end.checked_add(size).ok_or_else(bloom_allocation_overflow)?;
        }
        offsets.push(end);
    }

    let mut values = vec![0u8; end as usize];
    for (bloom, span) in blooms.zip(offsets.windows(2)) {
        if let Some(bloom) = bloom {
            let mut target = &mut values[span[0] as usize..span[1] as usize];
            bloom.write_bytes(&mut target)?;
            if !target.is_empty() {
                return Err(invalid("Bloom metadata serialized size disagrees with its offset"));
            }
        }
    }
    BinaryArray::try_new(OffsetBuffer::new(offsets.into()), Buffer::from_vec(values), validity.finish())
        .map_err(invalid)
}

fn bloom_allocation_overflow() -> Anomaly {
    invalid("Bloom metadata allocation overflow")
}

fn visible_run_spans<R: RunEndIndexType>(
    runs: &RunArray<R>,
    rows: Range<usize>,
) -> (Range<usize>, impl Iterator<Item = Range<usize>> + '_) {
    let physical = if rows.is_empty() {
        0..0
    } else {
        runs.get_physical_index(rows.start)..runs.get_physical_index(rows.end - 1) + 1
    };
    let offset = runs.offset();
    let mut start = rows.start;
    let spans = runs.run_ends().values()[physical.clone()].iter().map(move |end| {
        let end = (end.as_usize() - offset).min(rows.end);
        let span = start..end;
        start = end;
        span
    });
    (physical, spans)
}

fn run_presence_count<R: RunEndIndexType>(
    runs: &RunArray<R>,
    mask: &Mask,
    reader: &rtdb_columnar::ColumnPresence<'_>,
) -> Result<i64> {
    let (_, spans) = visible_run_spans(runs, mask.span(runs.len()));
    let mut count = 0;
    for span in spans {
        let row = span.start;
        let selected = mask.count_in(span);
        if selected != 0 && reader.is_present(runs, row)? {
            count += selected;
        }
    }
    i64::try_from(count).map_err(invalid)
}

#[allow(clippy::too_many_arguments)]
fn fold(field: &Field, array: &dyn Array, mask: &Mask, with_bloom: bool, calculate_blooms: bool, output: &mut Vec<ColumnSummary>, statistics: &mut Statistics, base: i32, depth: usize) -> Result<()> {
    if depth > 128 { return Err(invalid("Column metadata nesting exceeds 128 levels")); }
    match field.data_type() {
        DataType::Union(fields, UnionMode::Dense) => {
            let union = downcast::<UnionArray>(array)?;
            for (id, field) in fields.iter() {
                let child = union.child(id);
                let child_mask = match mask {
                    Mask::All => Mask::All,
                    _ => {
                        let mut selected = BooleanBufferBuilder::new(child.len());
                        selected.append_n(child.len(), false);
                        for row in mask.rows(array.len()) {
                            if union.type_id(row) == id {
                                selected.set_bit(union.value_offset(row), true);
                            }
                        }
                        Mask::Selected { offset: 0, rows: selected.finish() }
                    }
                };
                fold(field, child.as_ref(), &child_mask, with_bloom, calculate_blooms, output, statistics, base, depth + 1)?;
            }
            return Ok(());
        }
        DataType::RunEndEncoded(ends, value) => {
            macro_rules! runs {
                ($ty:ty) => {{
                    let runs = downcast::<RunArray<$ty>>(array)?;
                    let (physical, spans) = visible_run_spans(runs, mask.span(runs.len()));
                    let child_mask = match mask {
                        Mask::Selected { .. } => Mask::Selected {
                            offset: physical.start,
                            rows: spans.map(|span| mask.count_in(span) != 0).collect(),
                        },
                        _ => Mask::Span(physical),
                    };
                    fold(value, runs.values().as_ref(), &child_mask, with_bloom, calculate_blooms, output, statistics, base, depth + 1)
                }};
            }
            return match ends.data_type() { DataType::Int16 => runs!(Int16Type), DataType::Int32 => runs!(Int32Type), DataType::Int64 => runs!(Int64Type), _ => Err(invalid("Invalid run-end index width")) };
        }
        DataType::Struct(fields) => {
            let values = downcast::<StructArray>(array)?;
            let selection = field.is_nullable().then(|| mask.with_parent_validity(array)).flatten();
            let child_mask = selection.as_ref().unwrap_or(mask);
            let children = fields.iter().enumerate().map(|(index, field)| {
                write_column(field, values.column(index).as_ref(), false, calculate_blooms, child_mask, output, base, depth + 1)
            }).collect::<Result<Vec<_>>>()?;
            statistics.flavours.insert("struct".into(), Flavour::Children(children));
            return Ok(());
        }
        DataType::List(element) | DataType::FixedSizeList(element, _) => {
            let values = match field.data_type() {
                DataType::List(_) => downcast::<ListArray>(array)?.values(),
                _ => downcast::<FixedSizeListArray>(array)?.values(),
            };
            let child = write_column(element, values.as_ref(), false, calculate_blooms, &Mask::All, output, base, depth + 1)?;
            let name = if field.metadata().get(rtdb_columnar::EXTENSION_NAME_KEY).is_some_and(|name| name == "set") { "set" } else { "list" };
            statistics.flavours.insert(name.into(), Flavour::Child(child));
            return Ok(());
        }
        DataType::Null => return Ok(()),
        _ => {}
    }
    let ty = rtdb_columnar::from_field(field)?;
    if ty == LogicalType::Nothing { return Ok(()); }
    let extension = field.metadata().get(rtdb_columnar::EXTENSION_NAME_KEY).map(String::as_str);
    let presence = matches!(field.data_type(), DataType::Boolean | DataType::Interval(_)) || matches!(extension, Some("xt/oid" | "xt/regclass" | "xt/regproc" | "xt/interval-month-day-micro" | "xt/transit+msgpack"));
    if presence {
        let mono = match &ty { LogicalType::Maybe(value) => value.as_ref(), value => value };
        statistics.flavours.insert(rtdb_columnar::leg_name(mono)?, Flavour::Presence);
        return Ok(());
    }
    let byte_backed = matches!(field.data_type(), DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_));
    if byte_backed {
        statistics.bytes_seen = true;
        if with_bloom {
            let bloom = statistics.bloom.get_or_insert_with(BloomFilter::default);
            if !bloom.insert_byte_cells(array, mask.rows(array.len())) {
                for row in mask.rows(array.len()) { bloom.insert_cell(field, array, row)?; }
            }
        }
        return Ok(());
    }
    let group = match field.data_type() {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => "date-times",
        DataType::Time32(_) | DataType::Time64(_) => "times-of-day",
        DataType::Duration(_) => "durations",
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 | DataType::Float16 | DataType::Float32 | DataType::Float64 | DataType::Decimal32(_, _) | DataType::Decimal64(_, _) | DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "numbers",
        _ => return Err(Anomaly::unsupported("rtdb.format/metadata-flavour", format!("Unsupported column metadata type: {:?}", field.data_type()))),
    };
    let bounds = statistics.numeric.entry(group).or_insert((f64::INFINITY, f64::NEG_INFINITY));
    let cells = numeric_cells(array);
    for row in mask.rows(array.len()) {
        if array.is_null(row) { continue; }
        let value = match &cells {
            Some(cells) => cells(row),
            None => rtdb_columnar::decimal_cell_as_f64(field, array, row)?,
        };
        if value.is_nan() || bounds.0.is_nan() { bounds.0 = f64::NAN; } else { bounds.0 = bounds.0.min(value); }
        if value.is_nan() || bounds.1.is_nan() { bounds.1 = f64::NAN; } else { bounds.1 = bounds.1.max(value); }
    }
    Ok(())
}

/// Counts selected cells whose logical presence depends only on the array's validity bitmap.
fn present_cells(field: &Field, array: &dyn Array, mask: &Mask) -> Option<usize> {
    let data_type = array.data_type();
    let flat = data_type.is_primitive() || matches!(data_type, DataType::Boolean | DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_));
    let list = matches!(data_type, DataType::List(_) | DataType::FixedSizeList(_, _));
    if !(flat || list) || field.data_type() != data_type || !field.metadata().is_empty() { return None; }
    Some(validity_count(array, mask))
}

fn validity_count(array: &dyn Array, mask: &Mask) -> usize {
    match mask {
        Mask::All => array.len() - array.null_count(),
        _ if array.null_count() == 0 => mask.count_in(mask.span(array.len())),
        _ => mask.rows(array.len()).filter(|row| array.is_valid(*row)).count(),
    }
}

/// Reads numeric statistics in place, using signed execution widths and seconds for time values.
fn numeric_cells(array: &dyn Array) -> Option<Box<dyn Fn(usize) -> f64 + '_>> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::*;
    use arrow_schema::TimeUnit as Unit;
    let per_second = |unit: &Unit| match unit { Unit::Second => 1.0, Unit::Millisecond => 1e3, Unit::Microsecond => 1e6, Unit::Nanosecond => 1e9 };
    Some(match array.data_type() {
        DataType::Int8 => { let array = array.as_primitive::<Int8Type>(); Box::new(move |row| f64::from(array.value(row))) }
        DataType::Int16 => { let array = array.as_primitive::<Int16Type>(); Box::new(move |row| f64::from(array.value(row))) }
        DataType::Int32 => { let array = array.as_primitive::<Int32Type>(); Box::new(move |row| f64::from(array.value(row))) }
        DataType::Int64 => { let array = array.as_primitive::<Int64Type>(); Box::new(move |row| array.value(row) as f64) }
        DataType::UInt8 => { let array = array.as_primitive::<UInt8Type>(); Box::new(move |row| f64::from(array.value(row) as i8)) }
        DataType::UInt16 => { let array = array.as_primitive::<UInt16Type>(); Box::new(move |row| f64::from(array.value(row) as i16)) }
        DataType::UInt32 => { let array = array.as_primitive::<UInt32Type>(); Box::new(move |row| f64::from(array.value(row) as i32)) }
        DataType::UInt64 => { let array = array.as_primitive::<UInt64Type>(); Box::new(move |row| (array.value(row) as i64) as f64) }
        DataType::Float16 => { let array = array.as_primitive::<Float16Type>(); Box::new(move |row| array.value(row).to_f64()) }
        DataType::Float32 => { let array = array.as_primitive::<Float32Type>(); Box::new(move |row| f64::from(array.value(row))) }
        DataType::Float64 => { let array = array.as_primitive::<Float64Type>(); Box::new(move |row| array.value(row)) }
        DataType::Date32 => { let array = array.as_primitive::<Date32Type>(); Box::new(move |row| i64::from(array.value(row)).wrapping_mul(86_400) as f64) }
        DataType::Date64 => { let array = array.as_primitive::<Date64Type>(); Box::new(move |row| (array.value(row).div_euclid(rtdb_types::java_date::MILLIS_PER_DAY) * 86_400) as f64) }
        DataType::Time32(Unit::Second) => { let array = array.as_primitive::<Time32SecondType>(); Box::new(move |row| f64::from(array.value(row))) }
        DataType::Time32(Unit::Millisecond) => { let array = array.as_primitive::<Time32MillisecondType>(); Box::new(move |row| f64::from(array.value(row)) / 1e3) }
        DataType::Time64(Unit::Microsecond) => { let array = array.as_primitive::<Time64MicrosecondType>(); Box::new(move |row| array.value(row) as f64 / 1e6) }
        DataType::Time64(Unit::Nanosecond) => { let array = array.as_primitive::<Time64NanosecondType>(); Box::new(move |row| array.value(row) as f64 / 1e9) }
        DataType::Timestamp(unit, _) => {
            let scale = per_second(unit);
            let values: Box<dyn Fn(usize) -> i64 + '_> = match unit {
                Unit::Second => { let array = array.as_primitive::<TimestampSecondType>(); Box::new(move |row| array.value(row)) }
                Unit::Millisecond => { let array = array.as_primitive::<TimestampMillisecondType>(); Box::new(move |row| array.value(row)) }
                Unit::Microsecond => { let array = array.as_primitive::<TimestampMicrosecondType>(); Box::new(move |row| array.value(row)) }
                Unit::Nanosecond => { let array = array.as_primitive::<TimestampNanosecondType>(); Box::new(move |row| array.value(row)) }
            };
            Box::new(move |row| values(row) as f64 / scale)
        }
        DataType::Duration(unit) => {
            let scale = per_second(unit);
            let values: Box<dyn Fn(usize) -> i64 + '_> = match unit {
                Unit::Second => { let array = array.as_primitive::<DurationSecondType>(); Box::new(move |row| array.value(row)) }
                Unit::Millisecond => { let array = array.as_primitive::<DurationMillisecondType>(); Box::new(move |row| array.value(row)) }
                Unit::Microsecond => { let array = array.as_primitive::<DurationMicrosecondType>(); Box::new(move |row| array.value(row)) }
                Unit::Nanosecond => { let array = array.as_primitive::<DurationNanosecondType>(); Box::new(move |row| array.value(row)) }
            };
            Box::new(move |row| values(row) as f64 / scale)
        }
        _ => return None,
    })
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T> { array.as_any().downcast_ref().ok_or_else(|| invalid("Metadata array and field disagree")) }
fn invalid(error: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("rtdb.format/invalid-column-metadata", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int16Array, cast::AsArray};
    use arrow_schema::UnionFields;

    #[test]
    fn sliced_runs_keep_logical_counts_and_exact_float_bounds_at_every_width() {
        let values = Float64Array::from(vec![
            Some(-100.0), Some(-0.0), None, Some(f64::NAN), Some(0.0), Some(100.0),
        ]);
        macro_rules! check_width {
            ($width:ty, $ends:ty) => {{
                let ends = <$ends>::from(vec![4, 9, 14, 18, 22, 26]);
                let runs = RunArray::<$width>::try_new(&ends, &values).unwrap();
                let field = Field::new("encoded", runs.data_type().clone(), false);
                for (start, len, count, bound) in [
                    (7, 5, 2, Some(-0.0f64)),
                    (13, 8, 7, Some(f64::NAN)),
                    (26, 0, 0, None),
                ] {
                    let mut columns = Vec::new();
                    column_metadata(&field, &runs.slice(start, len), true, false, &mut columns).unwrap();
                    assert_eq!(columns[0].field("count"), Some(&Value::I64(count)));
                    let numbers = columns[0].field("numbers").unwrap();
                    for name in ["min", "max"] {
                        let actual = match numbers.field(name).unwrap() {
                            Value::F64(number) => Some(number.to_bits()),
                            Value::Null => None,
                            value => panic!("Unexpected numeric bound {value:?}"),
                        };
                        assert_eq!(actual, bound.map(f64::to_bits));
                    }
                }
            }};
        }
        check_width!(Int16Type, Int16Array);
        check_width!(Int32Type, Int32Array);
        check_width!(Int64Type, Int64Array);
    }

    #[test]
    fn nested_masks_select_run_values_and_active_union_slots() {
        let fields = UnionFields::try_new([3, 7], [
            Field::new("utf8", DataType::Utf8, false),
            Field::new("i64", DataType::Int64, false),
        ]).unwrap();
        let union = UnionArray::try_new(
            fields, vec![3, 3, 7, 3, 3].into(), Some(vec![0, 1, 0, 2, 3].into()),
            vec![
                Arc::new(StringArray::from(vec!["left", "first", "second", "right", "unreferenced"])),
                Arc::new(Int64Array::from(vec![999])),
            ],
        ).unwrap();
        let runs = RunArray::<Int32Type>::try_new(&Int32Array::from(vec![3, 6, 9, 12, 15]), &union)
            .unwrap().slice(2, 11);
        let wrap = |name: &str, child: ArrayRef, nullable, nulls| {
            let fields = vec![Field::new(name, child.data_type().clone(), nullable)].into();
            StructArray::try_new(fields, vec![child], nulls).unwrap()
        };
        let inner = wrap("encoded", Arc::new(runs), false, Some(
            vec![true, true, false, true, true, true, true, true, false, true, true].into(),
        ));
        let required = wrap("inner", Arc::new(inner), true, None);
        let outer = wrap("required", Arc::new(required), false, Some(
            vec![false, true, true, true, false, false, false, true, true, false, false].into(),
        ));
        let field = Field::new("outer", outer.data_type().clone(), true);
        let mut columns = Vec::new();
        column_metadata(&field, &outer, true, true, &mut columns).unwrap();

        let encoded = &columns[0];
        assert_eq!(encoded.field("col-name"), Some(&Value::Utf8("encoded".into())));
        assert_eq!(encoded.field("count"), Some(&Value::I64(3)));
        assert_eq!(encoded.field("numbers").unwrap().field("max"), Some(&Value::Null));
        let mut bloom = BloomFilter::default();
        for value in ["first", "second"] {
            bloom.insert_value(&Value::Utf8(value.into())).unwrap();
        }
        assert_eq!(encoded.field("bytes").unwrap().field("bloom"), Some(&Value::Binary(bloom.to_bytes().unwrap())));
    }

    #[test]
    fn null_free_nullable_structs_still_exclude_unreferenced_union_values() {
        let fields = UnionFields::try_new([3], [Field::new("i64", DataType::Int64, false)]).unwrap();
        let union = UnionArray::try_new(
            fields, vec![3].into(), Some(vec![1].into()), vec![Arc::new(Int64Array::from(vec![999, 7]))],
        ).unwrap();
        let child = Field::new("selected", union.data_type().clone(), false);
        let array = StructArray::try_new(vec![child].into(), vec![Arc::new(union)], None).unwrap();
        let mut columns = Vec::new();
        for (nullable, max) in [(true, 7.0), (false, 999.0)] {
            let field = Field::new("root", array.data_type().clone(), nullable);
            columns.clear();
            column_metadata(&field, &array, true, false, &mut columns).unwrap();
            assert_eq!(columns[0].field("numbers").unwrap().field("max"), Some(&Value::F64(max)));
        }
    }

    #[test]
    fn typed_bloom_values_preserve_portable_bytes_and_distinct_absences() {
        let mut bloom = BloomFilter::default();
        bloom.insert_hashes(1, 1);
        let columns: Vec<_> = [
            None, Some(None), Some(Some(BloomFilter::default())), Some(Some(bloom)), None,
        ].into_iter().enumerate().map(|(index, bloom)| ColumnSummary {
            name: index.to_string(),
            root: true,
            count: 0,
            flavours: bloom.into_iter().map(|bloom| ("bytes".into(), Flavour::Bytes(bloom))).collect(),
        }).collect();
        let page = build_statistics(&columns).unwrap();
        let bytes = page.column_by_name("bytes").unwrap().as_struct();
        let binary = bytes.column(0).as_binary::<i32>();
        let empty = [58, 48, 0, 0, 0, 0, 0, 0];
        let populated = [58, 48, 0, 0, 1, 0, 0, 0, 0, 0, 4, 0, 16, 0, 0, 0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0];

        assert!(bytes.is_null(0));
        assert!(bytes.is_valid(1));
        assert!(binary.is_null(0));
        assert!(binary.is_null(1));
        assert!(binary.is_valid(2));
        assert!(binary.is_valid(3));
        assert!(bytes.is_null(4));
        assert!(binary.is_null(4));
        assert_eq!(binary.value_offsets(), &[0, 0, 0, 8, 34, 34]);
        assert_eq!(binary.value(2), &empty);
        assert_eq!(binary.value(3), &populated);

        let values = columns.into_iter().map(column_value).collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(values[0].field("bytes"), None);
        assert_eq!(values[1].field("bytes").unwrap().field("bloom"), Some(&Value::Null));
        assert_eq!(values[2].field("bytes").unwrap().field("bloom"), Some(&Value::Binary(empty.to_vec())));
        assert_eq!(values[3].field("bytes").unwrap().field("bloom"), Some(&Value::Binary(populated.to_vec())));
    }

    #[test]
    fn decimal_statistics_handle_full_scale_without_exponent_sized_allocation() {
        for (scale, expected) in [(i32::MIN, f64::NEG_INFINITY), (i32::MAX, -0.0)] {
            let value = Value::Decimal(rtdb_types::DecimalValue::new((-123i128).to_be_bytes().to_vec(), 3, scale, 128).unwrap());
            let field = rtdb_columnar::to_field("d", &value.logical_type()).unwrap();
            let array = rtdb_columnar::copy_array(&value.logical_type(), &[value]).unwrap();
            let mut columns = Vec::new();
            column_metadata(&field, array.as_ref(), true, false, &mut columns).unwrap();
            let Value::Struct(column) = &columns[0] else { panic!("Expected column metadata"); };
            let Value::Struct(numbers) = &column["numbers"] else { panic!("Expected numeric statistics"); };
            let Value::F64(min) = numbers["min"] else { panic!("Expected the minimum"); };
            assert_eq!(min.to_bits(), expected.to_bits());
        }
    }
    #[test]
    fn date_millisecond_statistics_floor_to_the_containing_day() {
        let millis = arrow_array::Date64Array::from(vec![-86_400_001, -86_400_000, -1, 0, 86_400_001]);
        let days = arrow_array::Date32Array::from(vec![-2, -1, -1, 0, 1]);
        let millis = numeric_cells(&millis).unwrap();
        let days = numeric_cells(&days).unwrap();
        for row in 0..5 { assert_eq!(millis(row), days(row)); }
    }
}

#[cfg(test)]
mod float_list_tests {
    use super::*;
    use arrow_array::{Float32Array, builder::{FixedSizeListBuilder, Float32Builder}};
    use std::sync::Arc;

    #[test]
    fn ambiguous_set_union_preserves_presence_and_bloom_metadata_bytes() {
        use arrow_buffer::ScalarBuffer;
        use arrow_schema::UnionFields;
        use rtdb_types::{ListKind, ScalarType};
        use crate::metadata::{LeafColumns, MetadataFile, MetadataNode};

        let float = LogicalType::Scalar(ScalarType::F32);
        let set = |element| LogicalType::List { kind: ListKind::Set, element: Box::new(element) };
        let with_nulls = set(float.clone().maybe()).maybe();
        let plain = set(float);
        let first = rtdb_columnar::to_field("other", &with_nulls).unwrap();
        let second = rtdb_columnar::to_field("set", &plain).unwrap();
        let text = Field::new("text", DataType::Utf8, true);
        assert!(rtdb_columnar::ColumnPresence::new(&first).unwrap().uses_parent_validity());
        let union_fields = UnionFields::try_new([3, 5, 7], [first, second, text]).unwrap();
        let sets = rtdb_columnar::copy_array(&with_nulls, &[Value::Set(vec![Value::Null, Value::F32(-0.0)]), Value::Null]).unwrap();
        let more_sets = rtdb_columnar::copy_array(&plain, &[Value::Set(vec![Value::F32(3.0)])]).unwrap();
        let texts = Arc::new(arrow_array::StringArray::from(vec![Some("word"), None]));
        let union = UnionArray::try_new(union_fields, ScalarBuffer::from(vec![3, 7, 5, 3, 7]),
            Some(ScalarBuffer::from(vec![0, 0, 0, 1, 1])), vec![sets, more_sets, texts]).unwrap();
        let field = Field::new("payload", union.data_type().clone(), false);
        let mut actual = Vec::new();
        column_metadata(&field, &union, true, true, &mut actual).unwrap();
        let scalar_count = (0..union.len()).filter(|&row| {
            !matches!(rtdb_columnar::read_value(&field, &union, row).unwrap(), Value::Null | Value::Undefined)
        }).count();
        assert_eq!(scalar_count, 3);
        let mut bloom = BloomFilter::default();
        bloom.insert_value(&Value::Utf8("word".into())).unwrap();
        bloom.insert_value(&Value::Null).unwrap();
        let mut expected = actual.clone();
        let Value::Struct(root) = expected.last_mut().unwrap() else { unreachable!() };
        root.insert("count".into(), Value::I64(scalar_count as i64));
        root.insert("bytes".into(), Value::Struct(StructFields::from([("bloom".into(), Value::Binary(bloom.to_bytes().unwrap()))])));
        let ipc = |columns| MetadataFile::from_nodes(vec![
            MetadataNode::Leaf { data_page_index: 0, columns: LeafColumns::new(columns) },
        ], false).unwrap().write().unwrap();
        assert_eq!(ipc(actual), ipc(expected));
    }

    #[test]
    fn float_list_presence_uses_parent_validity_and_selection() {
        let item = Arc::new(Field::new("item", DataType::Float32, true));
        let values = Arc::new(Float32Array::from(vec![Some(1.0), None, Some(2.0), Some(3.0)]));
        let list = FixedSizeListArray::try_new(item, 2, values, Some(vec![true, false].into())).unwrap();
        let field = Field::new("embedding", list.data_type().clone(), true);
        assert_eq!(present_cells(&field, &list, &Mask::All), Some(1));
        let selected = Mask::Selected { offset: 0, rows: vec![false, true].into() };
        assert_eq!(present_cells(&field, &list, &selected), Some(0));

        let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), 2);
        builder.values().append_null();
        builder.values().append_null();
        builder.append(false);
        let nulls = builder.finish();
        let field = Field::new("embedding", nulls.data_type().clone(), true);
        assert_eq!(present_cells(&field, &nulls, &Mask::All), Some(0));
    }
}
