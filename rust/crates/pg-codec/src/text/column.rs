use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, FixedSizeListArray, ListArray, NullArray, PrimitiveArray, UnionArray,
    builder::{ArrayBuilder, BinaryBuilder, TimestampMicrosecondBuilder, BooleanBuilder, FixedSizeBinaryBuilder, PrimitiveBuilder, StringBuilder},
    types::{ArrowPrimitiveType, Date32Type, Decimal128Type, Decimal256Type, DurationMicrosecondType,
        Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, IntervalMonthDayNanoType,
        IntervalYearMonthType, Time64NanosecondType, TimestampMicrosecondType}};
use arrow_buffer::{IntervalMonthDayNano, NullBuffer, NullBufferBuilder, OffsetBuffer, i256};
use arrow_schema::{DataType, Field, UnionFields, UnionMode};
use rtdb_columnar::BatchBuilder;
use rtdb_format::transit::{TransitValue, decode_json, encode_msgpack};
use rtdb_types::{Anomaly, LogicalType, PgType, Result, ScalarType, TimeUnit};

use super::{bytea, invalid, interval, interval_type, local_timestamp_micros, parse_boolean, parse_decimal, parse_uuid, range, float, float32, visit_array, zoned_timestamp_micros};

/// Decodes a PostgreSQL column using a fixed Arrow layout when supported.
/// The caller admits input, output and scratch allocations.
/// An absent array selects the general text decoder.
pub fn read_text_column<'a>(oid: u32, count: usize, mut cell: impl FnMut(usize) -> Result<Option<&'a str>>) -> Result<Option<ArrayRef>> {
    let result: ArrayRef = match oid {
        16 => {
            let mut builder = BooleanBuilder::with_capacity(count);
            for row in 0..count { builder.append_option(cell(row)?.map(parse_boolean).transpose()?); }
            Arc::new(builder.finish())
        }
        21 => primitive::<Int16Type>(count, cell, |text| text.parse().map_err(invalid))?,
        23 => primitive::<Int32Type>(count, cell, |text| text.parse().map_err(invalid))?,
        20 => primitive::<Int64Type>(count, cell, |text| text.parse().map_err(invalid))?,
        700 => primitive::<Float32Type>(count, cell, float32)?,
        701 => primitive::<Float64Type>(count, cell, float)?,
        1083 => primitive::<Time64NanosecondType>(count, cell, |text| Ok(rtdb_types::parse_iso_local_time(text)? as i64))?,
        11112 => primitive::<DurationMicrosecondType>(count, cell, crate::interval::duration_micros)?,
        1007 => list(count, cell, false, PrimitiveBuilder::<Int32Type>::new(), append_integer_row::<Int32Type>)?,
        1016 => list(count, cell, false, PrimitiveBuilder::<Int64Type>::new(), append_integer_row::<Int64Type>)?,
        1009 => list(count, cell, false, StringBuilder::new(), |builder, text| {
            visit_array(text, |item| { builder.append_option(item.as_deref()); Ok(()) })
        })?,
        1021 => float_array::<Float32Type>(count, cell, float32)?,
        1022 => float_array::<Float64Type>(count, cell, float)?,
        oid if PgType::from_oid(oid as i32).is_none() || matches!(oid, 25 | 1042 | 1043) => {
            let mut builder = StringBuilder::with_capacity(count, 0);
            for row in 0..count { builder.append_option(cell(row)?); }
            Arc::new(builder.finish())
        }
        _ => return Ok(None),
    };
    Ok(Some(result))
}

fn float_array<'a, T: ArrowPrimitiveType>(
    count: usize, mut cell: impl FnMut(usize) -> Result<Option<&'a str>>, parse: impl Fn(&str) -> Result<T::Native>,
) -> Result<ArrayRef> {
    let build = |count, cell: &mut dyn FnMut(usize) -> Result<Option<&'a str>>| {
        list(count, cell, true, PrimitiveBuilder::<T>::with_capacity(0), |builder, text| {
            visit_array(text, |item| {
                builder.append_option(item.as_deref().map(&parse).transpose()?);
                Ok(())
            })
        })
    };
    let mut has_text = false;
    for row in 0..count {
        if cell(row)?.is_some_and(float_array_text_layout) { has_text = true; break; }
    }
    if !has_text { return build(count, &mut cell); }

    let mut lists = Vec::new();
    let mut strings = StringBuilder::with_capacity(count, 0);
    let mut nulls = 0;
    let mut legs = Vec::with_capacity(3);
    let mut ids = Vec::with_capacity(count);
    let mut offsets = Vec::with_capacity(count);
    for row in 0..count {
        let (name, offset) = match cell(row)? {
            Some(text) if float_array_text_layout(text) => {
                let offset = strings.len();
                strings.append_value(text);
                ("utf8", offset)
            }
            Some(text) => {
                let offset = lists.len();
                lists.push(text);
                ("list", offset)
            }
            None => {
                let offset = nulls;
                nulls += 1;
                ("null", offset)
            }
        };
        let leg = legs.iter().position(|&candidate| candidate == name).unwrap_or(legs.len());
        if leg == legs.len() { legs.push(name); }
        ids.push(leg as i8);
        offsets.push(i32::try_from(offset).map_err(invalid)?);
    }
    if lists.is_empty() {
        let mut strings = StringBuilder::with_capacity(count, 0);
        for row in 0..count { strings.append_option(cell(row)?); }
        return Ok(Arc::new(strings.finish()));
    }
    let lists = build(lists.len(), &mut |row| Ok(Some(lists[row])))?;
    let strings = Arc::new(strings.finish());
    let children: Vec<ArrayRef> = legs.iter().map(|name| match *name {
        "list" => lists.clone(),
        "utf8" => strings.clone(),
        _ => Arc::new(NullArray::new(nulls)),
    }).collect();
    let fields = legs.iter().zip(&children).map(|(&name, child)| Field::new(name, child.data_type().clone(), name == "null"));
    let fields = UnionFields::try_new(0..legs.len() as i8, fields).map_err(invalid)?;
    Ok(Arc::new(UnionArray::try_new(fields, ids.into(), Some(offsets.into()), children).map_err(invalid)?))
}

pub(super) fn float_array_text_layout(text: &str) -> bool {
    let text = text.trim_start();
    text.starts_with('[') || text.strip_prefix('{').is_some_and(|inner| inner.trim_start().starts_with('{'))
}

fn primitive<'a, T: ArrowPrimitiveType>(
    count: usize, mut cell: impl FnMut(usize) -> Result<Option<&'a str>>, mut parse: impl FnMut(&str) -> Result<T::Native>,
) -> Result<ArrayRef> {
    let mut builder = PrimitiveBuilder::<T>::with_capacity(count);
    for row in 0..count { builder.append_option(cell(row)?.map(&mut parse).transpose()?); }
    Ok(Arc::new(builder.finish()))
}

fn append_integer_row<T: ArrowPrimitiveType>(builder: &mut PrimitiveBuilder<T>, text: &str) -> Result<()>
where
    T::Native: std::str::FromStr,
    <T::Native as std::str::FromStr>::Err: std::fmt::Display,
{
    let start = builder.len();
    let mut first = None;
    let mut has_null = false;
    visit_array(text, |item| {
        if let Some(item) = item {
            let value = item.parse().map_err(invalid)?;
            first.get_or_insert(value);
            builder.append_value(value);
        } else {
            has_null = true;
            builder.append_null();
        }
        Ok(())
    })?;
    fill_integer_nulls(builder, start, first.filter(|_| has_null));
    Ok(())
}

pub(super) fn fill_integer_nulls<T: ArrowPrimitiveType>(
    builder: &mut PrimitiveBuilder<T>, start: usize, first: Option<T::Native>,
) {
    // Packed lists gather null slots from offset zero in each row's compact value buffer.
    if let Some(first) = first {
        for index in start..builder.len() {
            if builder.validity_slice().is_some_and(|bits| !arrow_buffer::bit_util::get_bit(bits, index)) {
                builder.values_slice_mut()[index] = first;
            }
        }
    }
}

fn list<'a, B: ArrayBuilder>(
    count: usize, mut cell: impl FnMut(usize) -> Result<Option<&'a str>>,
    keep_child_type: bool,
    mut builder: B, mut append_row: impl FnMut(&mut B, &'a str) -> Result<()>,
) -> Result<ArrayRef> {
    let mut offsets = Vec::with_capacity(count + 1);
    let mut present = Vec::with_capacity(count);
    offsets.push(0);
    for row in 0..count {
        let text = cell(row)?;
        present.push(text.is_some());
        if let Some(text) = text { append_row(&mut builder, text)?; }
        offsets.push(i32::try_from(builder.len()).map_err(invalid)?);
    }
    let child = builder.finish();
    let (field, child) = if !keep_child_type && child.null_count() == child.len() {
        let field = Field::new("$data$", DataType::Null, !child.is_empty());
        (field, Arc::new(NullArray::new(child.len())) as ArrayRef)
    } else {
        (Field::new("$data$", child.data_type().clone(), child.null_count() != 0), child)
    };
    let validity = present.iter().any(|&valid| !valid).then(|| NullBuffer::from(present));
    let list = ListArray::try_new(Arc::new(field), OffsetBuffer::new(offsets.into()), child, validity).map_err(invalid)?;
    Ok(Arc::new(list))
}

/// Appends a source column with its native field metadata and writer reservation.
pub fn read_source_text_column<'a>(
    name: &str, oid: u32, count: usize, cell: impl FnMut(usize) -> Result<Option<&'a str>>,
    output: &mut BatchBuilder,
) -> Result<Option<(Field, ArrayRef)>> {
    let result = match oid {
        3910 => {
            let (field, array) = ranges(name, count, cell)?;
            account_field(field, array, output)?
        }
        16384 => {
            let Some((field, array)) = transit_column(name, count, cell)? else { return Ok(None); };
            account_field(field, array, output)?
        }
        17 => scalar_column(name, count, cell, output, bytea, |_| ScalarType::Binary, |bytes| bytes.len(),
            |_, rows| Ok(Arc::new(rows.into_iter().collect::<BinaryArray>())))?,
        2950 => scalar_column(name, count, cell, output, parse_uuid, |_| ScalarType::Uuid, |_| 0,
            |_, rows| {
                let mut builder = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
                for uuid in rows {
                    if let Some(uuid) = uuid { builder.append_value(uuid).map_err(invalid)?; }
                    else { builder.append_null(); }
                }
                Ok(Arc::new(builder.finish()))
            })?,
        1082 => scalar_column(name, count, cell, output, rtdb_types::CivilDate::parse_iso, |_| ScalarType::DateDay, |_| 0,
            |_, rows| native_array::<Date32Type, _>(rows, |date| i32::try_from(date.epoch_day()).map_err(|_| {
                Anomaly::incorrect("xtdb/date-out-of-range", format!("Date {date} is outside the range of its Arrow date type"))
            })))?,
        1114 => scalar_column(name, count, cell, output, local_timestamp_micros,
            |_| ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: None }, |_| 0,
            |_, rows| native_array::<TimestampMicrosecondType, _>(rows, Ok))?,
        1184 => scalar_column(name, count, cell, output, zoned_timestamp_micros,
            |(_, zone)| ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some(zone.clone()) }, |(_, zone)| zone.len(),
            |field, rows| {
                let values = rows.into_iter().map(|row| row.map(|(micros, _)| micros)).collect::<PrimitiveArray<TimestampMicrosecondType>>();
                Ok(Arc::new(values.with_data_type(field.data_type().clone())))
            })?,
        1186 => scalar_column(name, count, cell, output, interval,
            |&(_, days, nanos)| interval_type(days, nanos), |_| 0,
            |field, rows| match field.data_type() {
                DataType::Interval(arrow_schema::IntervalUnit::YearMonth) =>
                    native_array::<IntervalYearMonthType, _>(rows, |(months, _, _)| Ok(months)),
                _ => native_array::<IntervalMonthDayNanoType, _>(rows, |(months, days, nanos)| Ok(IntervalMonthDayNano::new(months, days, nanos))),
            })?,
        1700 => scalar_column(name, count, cell, output, parse_decimal,
            |value| ScalarType::Decimal { precision: value.precision, scale: value.scale, bit_width: value.bit_width },
            |value| value.unscaled.len(), |field, rows| {
                let decimal_width = |_| Anomaly::incorrect("rtdb/decimal-width", "Decimal coefficient must fill its declared signed width");
                if matches!(field.data_type(), DataType::Decimal128(_, _)) {
                    let values = rows.into_iter().map(|value| value.map(|value| {
                        value.unscaled.as_slice().try_into().map(i128::from_be_bytes).map_err(decimal_width)
                    }).transpose()).collect::<Result<Vec<_>>>()?;
                    Ok(Arc::new(PrimitiveArray::<Decimal128Type>::from(values).with_data_type(field.data_type().clone())))
                } else {
                    let values = rows.into_iter().map(|value| value.map(|value| {
                        value.unscaled.as_slice().try_into().map(i256::from_be_bytes).map_err(decimal_width)
                    }).transpose()).collect::<Result<Vec<_>>>()?;
                    Ok(Arc::new(PrimitiveArray::<Decimal256Type>::from(values).with_data_type(field.data_type().clone())))
                }
            })?,
        _ => {
            let Some(array) = read_text_column(oid, count, cell)? else { return Ok(None); };
            if matches!(oid, 1021 | 1022) { account_column(name, array, output)? }
            else {
                let array = if array.null_count() == count { Arc::new(NullArray::new(count)) } else { array };
                account_column(name, array, output)?
            }
        }
    };
    Ok(Some(result))
}

fn account_column(name: &str, array: ArrayRef, output: &mut BatchBuilder) -> Result<(Field, ArrayRef)> {
    let field = Field::new(name, array.data_type().clone(), !array.is_empty()
        && (array.null_count() != 0 || array.data_type() == &DataType::Null));
    account_field(field, array, output)
}

fn account_field(field: Field, array: ArrayRef, output: &mut BatchBuilder) -> Result<(Field, ArrayRef)> {
    let bound = array.get_array_memory_size().checked_add(512)
        .ok_or_else(|| Anomaly::fault("xtdb.postgres/protocol-error", "PostgreSQL Arrow batch allocation overflow"))?;
    Ok((field, output.column(bound, || Ok(array))?))
}

/// Decodes the text output shared by pgvector vector and halfvec.
pub fn read_vector_column<'a>(
    name: &str, typmod: i32, count: usize, mut cell: impl FnMut(usize) -> Result<Option<&'a str>>,
    output: &mut BatchBuilder,
) -> Result<(Field, ArrayRef)> {
    if typmod == -1 {
        let array = list(count, cell, true, PrimitiveBuilder::<Float32Type>::with_capacity(0), |builder, text| {
            for component in vector_components(text, None)? { builder.append_value(component?); }
            Ok(())
        })?;
        return account_column(name, array, output);
    }

    let width = vector_width(typmod)?;
    let total = count.checked_mul(width).ok_or_else(|| invalid("pgvector column length overflow"))?;
    let scratch = total.checked_mul(8).and_then(|size| size.checked_add(4096))
        .ok_or_else(|| invalid("pgvector column allocation overflow"))?;
    let array = output.column(scratch, || {
        let mut builder = PrimitiveBuilder::<Float32Type>::with_capacity(total);
        let mut present = Vec::with_capacity(count);
        for row in 0..count {
            let text = cell(row)?;
            present.push(text.is_some());
            if let Some(text) = text {
                for component in vector_components(text, Some(width))? { builder.append_value(component?); }
            } else {
                builder.append_nulls(width);
            }
        }
        let child = Arc::new(Field::new("$data$", DataType::Float32, false));
        let validity = present.contains(&false).then(|| NullBuffer::from(present));
        Ok(Arc::new(FixedSizeListArray::try_new(child, typmod, Arc::new(builder.finish()), validity).map_err(invalid)?))
    })?;
    let field = Field::new(name, array.data_type().clone(), array.null_count() != 0);
    Ok((field, array))
}

pub(super) fn vector_width(typmod: i32) -> Result<usize> {
    usize::try_from(typmod).ok().filter(|width| *width > 0)
        .ok_or_else(|| invalid("Invalid pgvector dimension"))
}

pub(super) fn vector_components(text: &str, width: Option<usize>) -> Result<impl Iterator<Item = Result<f32>> + '_> {
    let elements = text.strip_prefix('[').and_then(|text| text.strip_suffix(']'))
        .filter(|text| !text.is_empty()).ok_or_else(|| invalid("Invalid pgvector text value"))?;
    let components = elements.split(',');
    if width.is_some_and(|width| components.clone().count() != width) {
        return Err(invalid("pgvector value does not match its declared dimension"));
    }
    Ok(components.map(vector_component))
}

fn vector_component(text: &str) -> Result<f32> {
    let value = float32(text.trim())?;
    if !value.is_finite() { return Err(invalid("pgvector components must be finite")); }
    Ok(value)
}

fn native_array<T: ArrowPrimitiveType, P>(rows: Vec<Option<P>>, mut decode: impl FnMut(P) -> Result<T::Native>) -> Result<ArrayRef> {
    let mut builder = PrimitiveBuilder::<T>::with_capacity(rows.len());
    for row in rows { builder.append_option(row.map(&mut decode).transpose()?); }
    Ok(Arc::new(builder.finish()))
}

fn scalar_column<'a, P>(
    name: &str, count: usize, mut cell: impl FnMut(usize) -> Result<Option<&'a str>>,
    output: &mut BatchBuilder, mut parse: impl FnMut(&'a str) -> Result<P>,
    scalar: impl Fn(&P) -> ScalarType, heap: impl Fn(&P) -> usize,
    build: impl Fn(&Field, Vec<Option<P>>) -> Result<ArrayRef>,
) -> Result<(Field, ArrayRef)> {
    let mut types = rtdb_types::OrderedSet::new();
    let mut rows = Vec::with_capacity(count);
    for row in 0..count {
        let value = cell(row)?.map(&mut parse).transpose()?;
        let ty = value.as_ref().map_or(LogicalType::Null, |value| LogicalType::Scalar(scalar(value)));
        let leg = types.iter().position(|candidate| candidate == &ty).unwrap_or(types.len());
        types.insert(ty);
        rows.push((leg, value));
    }
    let logical = if rows.is_empty() { LogicalType::Nothing } else { LogicalType::from_legs(types.iter().cloned()) };
    let field = rtdb_columnar::to_field(name, &logical)?;
    let types = types.into_iter().collect::<Vec<_>>();
    let bound = rtdb_columnar::estimate_scalar_builder_bytes(&logical, rows.iter().map(|(leg, value)| {
        match (&types[*leg], value) {
            (LogicalType::Scalar(ty), Some(value)) => Some((ty, heap(value))),
            _ => None,
        }
    }))?;
    let array = output.column(bound, || {
        match field.data_type() {
            DataType::Null => Ok(Arc::new(NullArray::new(count)) as ArrayRef),
            DataType::Union(fields, UnionMode::Dense) => {
                let mut children = types.iter().map(|_| Vec::new()).collect::<Vec<_>>();
                let mut ids = Vec::with_capacity(count);
                let mut offsets = Vec::with_capacity(count);
                for (leg, value) in rows {
                    ids.push(leg as i8);
                    offsets.push(i32::try_from(children[leg].len()).map_err(|_| {
                        Anomaly::incorrect("rtdb/union-offset-overflow", "Union child exceeds i32 offsets")
                    })?);
                    children[leg].push(value);
                }
                let arrays = children.into_iter().zip(fields.iter()).map(|(rows, (_, field))| {
                    if field.data_type() == &DataType::Null { Ok(Arc::new(NullArray::new(rows.len())) as ArrayRef) }
                    else { build(field, rows) }
                }).collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(UnionArray::try_new(fields.clone(), ids.into(), Some(offsets.into()), arrays).map_err(invalid)?))
            }
            _ => build(&field, rows.into_iter().map(|(_, value)| value).collect()),
        }
    })?;
    Ok((field, array))
}

fn ranges<'a>(
    name: &str, count: usize, mut cell: impl FnMut(usize) -> Result<Option<&'a str>>,
) -> Result<(Field, ArrayRef)> {
    let mut values = TimestampMicrosecondBuilder::new().with_timezone("UTC");
    let mut valid = NullBufferBuilder::new(count);
    let mut present = 0;
    for row in 0..count {
        let bounds = cell(row)?.map(range).transpose()?.flatten();
        valid.append(bounds.is_some());
        if let Some((lower, upper)) = bounds {
            values.append_value(lower);
            values.append_value(upper);
            present += 1;
        } else {
            values.append_nulls(2);
        }
    }
    if present == 0 { return null_column(name, count); }
    let logical = PgType::TsTzRange.logical_type().expect("range type");
    let field = rtdb_columnar::to_field(name, &nullable(logical, present != count))?;
    let DataType::FixedSizeList(item, width) = field.data_type() else { unreachable!() };
    let array = FixedSizeListArray::try_new(Arc::clone(item), *width, Arc::new(values.finish()), valid.finish())
        .map_err(invalid)?;
    Ok((field, Arc::new(array)))
}

fn transit_column<'a>(
    name: &str, count: usize, mut cell: impl FnMut(usize) -> Result<Option<&'a str>>,
) -> Result<Option<(Field, ArrayRef)>> {
    let mut opaque = BinaryBuilder::new();
    for row in 0..count {
        let wire = cell(row)?.map(|text| decode_json(text.as_bytes())).transpose()?.unwrap_or(TransitValue::Null);
        match &wire {
            TransitValue::Null => opaque.append_null(),
            TransitValue::Tagged { tag, .. } if super::is_opaque_transit(tag) => {
                opaque.append_value(encode_msgpack(&wire)?);
            }
            _ => return Ok(None),
        }
    }
    let array = opaque.finish();
    if array.null_count() == count { return null_column(name, count).map(Some); }
    let logical = nullable(LogicalType::Scalar(ScalarType::Transit), array.null_count() != 0);
    Ok(Some((rtdb_columnar::to_field(name, &logical)?, Arc::new(array))))
}

fn nullable(logical: LogicalType, has_null: bool) -> LogicalType {
    if has_null { logical.maybe() } else { logical }
}

fn null_column(name: &str, count: usize) -> Result<(Field, ArrayRef)> {
    Ok((rtdb_columnar::to_field(name, &LogicalType::Null)?, Arc::new(NullArray::new(count))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read_text;
    use rtdb_types::Value;

    #[test]
    fn source_column_matches_legacy_ipc_for_fixed_layouts() {
        use arrow_array::RecordBatch;
        use arrow_schema::Schema;
        let memory = rtdb_columnar::MemoryBudget::new(16 << 20);
        for (oid, rows) in [
            (3910, vec![None, Some("empty"), Some("[2000-01-01 00:00:00+00,)"), Some("[,2000-01-01 00:00:00+00)")]),
            (16384, vec![None, Some(r#"["~#xtdb/clj-form","a"]"#), Some(r#"["~#xtdb/clj-form",["^ ","foo",2]]"#)]),
            (1007, vec![None, Some("{NULL,1,2}"), Some("{3,NULL}"), Some("{}"), Some("{NULL}")]),
            (1016, vec![Some("{-9223372036854775808,NULL}"), Some("{NULL,7}"), None]),
            (1009, vec![Some(r#"{"NULL",NULL,"a,b","q\"r"}"#), Some("{}"), None]),
            (1021, vec![Some("{{1,2},{3,4}}"), None, Some("[0:1]={1,2}")]),
            (1007, vec![Some("{}"), None, Some("{}")]),
            (1007, vec![Some("{NULL}"), Some("{}"), None, Some("{NULL,NULL}")]),
        ] {
            let values = rows.iter().map(|text| text.map_or(Ok(Value::Null), |text| read_text(oid, text))).collect::<Result<Vec<_>>>().unwrap();
            let logical = LogicalType::join_all(&values.iter().map(Value::logical_type).collect::<Vec<_>>());
            let legacy_field = rtdb_columnar::to_field("value", &logical).unwrap();
            let legacy = rtdb_columnar::copy_array(&logical, &values).unwrap();
            let mut output = BatchBuilder::new(4096, &memory).unwrap();
            let (field, actual) = read_source_text_column("value", oid, rows.len(), |row| Ok(rows[row]), &mut output).unwrap().unwrap();
            let ipc = |field: Field, array| {
                let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap();
                rtdb_format::ipc::write_stream(batch.schema_ref(), std::slice::from_ref(&batch)).unwrap()
            };
            assert_eq!(field, legacy_field, "OID {oid}");
            assert_eq!(actual.to_data(), legacy.to_data(), "OID {oid}");
            assert!(ipc(field, actual) == ipc(legacy_field, legacy), "IPC differs for OID {oid} and rows {rows:?}");
        }
    }

    #[test]
    fn source_column_preserves_array_failures_and_dynamic_transit() {
        let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        let mut output = BatchBuilder::new(4096, &memory).unwrap();
        let first = read_text(1007, "{bad}").unwrap_err();
        let error = read_source_text_column("value", 1007, 2, |row| {
            Ok(Some(["{bad}", "{{1},{2}}"][row]))
        }, &mut output).unwrap_err();
        assert_eq!(error, first);
        for text in ["{{1},{2}}", "[0:1]={1,2}"] {
            assert_eq!(
                read_source_text_column("value", 1007, 1, |_| Ok(Some(text)), &mut output).unwrap_err(),
                read_text(1007, text).unwrap_err(),
            );
        }
        assert!(read_source_text_column("value", 16384, 1, |_| Ok(Some(r#"["^ ","a",1]"#)), &mut output).unwrap().is_none());
    }
}
