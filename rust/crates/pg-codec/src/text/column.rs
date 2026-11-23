use std::borrow::Cow;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, ListArray, NullArray, builder::{BooleanBuilder, PrimitiveBuilder, StringBuilder}, types::*};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use rtdb_types::{PgType, Result};

use super::{invalid, parse_boolean, float, float32, visit_array};

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
        1007 => list(count, cell, PrimitiveBuilder::<Int32Type>::new(), append_primitive::<Int32Type>)?,
        1016 => list(count, cell, PrimitiveBuilder::<Int64Type>::new(), append_primitive::<Int64Type>)?,
        1009 => list(count, cell, StringBuilder::new(), |builder, text| {
            builder.append_option(text.as_deref());
            Ok(())
        })?,
        oid if PgType::from_oid(oid as i32).is_none() || matches!(oid, 25 | 1042 | 1043 | 1021 | 1022) => {
            let mut builder = StringBuilder::with_capacity(count, 0);
            for row in 0..count { builder.append_option(cell(row)?); }
            Arc::new(builder.finish())
        }
        _ => return Ok(None),
    };
    Ok(Some(result))
}

fn primitive<'a, T: ArrowPrimitiveType>(
    count: usize, mut cell: impl FnMut(usize) -> Result<Option<&'a str>>, mut parse: impl FnMut(&str) -> Result<T::Native>,
) -> Result<ArrayRef> {
    let mut builder = PrimitiveBuilder::<T>::with_capacity(count);
    for row in 0..count { builder.append_option(cell(row)?.map(&mut parse).transpose()?); }
    Ok(Arc::new(builder.finish()))
}

fn append_primitive<T: ArrowPrimitiveType>(builder: &mut PrimitiveBuilder<T>, text: Option<Cow<'_, str>>) -> Result<()>
where
    T::Native: std::str::FromStr,
    <T::Native as std::str::FromStr>::Err: std::fmt::Display,
{
    builder.append_option(text.as_deref().map(|text| text.parse().map_err(invalid)).transpose()?);
    Ok(())
}

fn list<'a, B: arrow_array::builder::ArrayBuilder>(
    count: usize, mut cell: impl FnMut(usize) -> Result<Option<&'a str>>,
    mut builder: B, mut append: impl FnMut(&mut B, Option<Cow<'_, str>>) -> Result<()>,
) -> Result<ArrayRef> {
    let mut offsets = Vec::with_capacity(count + 1);
    let mut present = Vec::with_capacity(count);
    offsets.push(0);
    for row in 0..count {
        let text = cell(row)?;
        present.push(text.is_some());
        if let Some(text) = text { visit_array(text, |item| append(&mut builder, item))?; }
        offsets.push(i32::try_from(builder.len()).map_err(invalid)?);
    }
    let child = builder.finish();
    let (field, child) = if child.null_count() == child.len() {
        let field = Field::new("$data$", DataType::Null, !child.is_empty());
        (field, Arc::new(NullArray::new(child.len())) as ArrayRef)
    } else {
        (Field::new("$data$", child.data_type().clone(), child.null_count() != 0), child)
    };
    let validity = present.iter().any(|&valid| !valid).then(|| NullBuffer::from(present));
    let list = ListArray::try_new(Arc::new(field), OffsetBuffer::new(offsets.into()), child, validity).map_err(invalid)?;
    Ok(Arc::new(list))
}
