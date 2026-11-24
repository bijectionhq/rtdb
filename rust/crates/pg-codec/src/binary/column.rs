use std::sync::Arc;

use arrow_array::{Array, ArrayRef, NullArray, builder::{
    BooleanBuilder, FixedSizeBinaryBuilder, PrimitiveBuilder, StringBuilder,
}, types::{ArrowPrimitiveType, Int16Type, Int32Type, Int64Type}};
use arrow_schema::Field;
use rtdb_types::{LogicalType, Result, ScalarType};

use super::{Input, boolean, invalid};

/// Builds fixed-layout snapshot columns from the PostgreSQL binary payload.
pub fn read_source_binary_column<'a>(
    name: &str, oid: u32, count: usize, mut cell: impl FnMut(usize) -> Result<Option<&'a [u8]>>,
) -> Result<Option<(Field, ArrayRef)>> {
    let (scalar, array): (_, ArrayRef) = match oid {
        16 => {
            let mut builder = BooleanBuilder::with_capacity(count);
            for row in 0..count {
                builder.append_option(cell(row)?.map(|bytes| parse(bytes, boolean)).transpose()?);
            }
            (ScalarType::Bool, Arc::new(builder.finish()))
        }
        21 => (ScalarType::I16, primitive::<Int16Type>(count, cell, |input| input.i16())?),
        23 => (ScalarType::I32, primitive::<Int32Type>(count, cell, |input| input.i32())?),
        20 => (ScalarType::I64, primitive::<Int64Type>(count, cell, |input| input.i64())?),
        25 | 1043 => {
            let mut builder = StringBuilder::with_capacity(count, 0);
            for row in 0..count {
                builder.append_option(cell(row)?.map(std::str::from_utf8).transpose().map_err(invalid)?);
            }
            (ScalarType::Utf8, Arc::new(builder.finish()))
        }
        2950 => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(count, 16);
            for row in 0..count {
                if let Some(bytes) = cell(row)? {
                    let uuid = parse(bytes, |input| input.take(16))?;
                    builder.append_value(uuid).map_err(invalid)?;
                } else { builder.append_null(); }
            }
            (ScalarType::Uuid, Arc::new(builder.finish()))
        }
        _ => return Ok(None),
    };
    let (logical, array) = if array.null_count() == count {
        let logical = if count == 0 { LogicalType::Nothing } else { LogicalType::Null };
        (logical, Arc::new(NullArray::new(count)) as ArrayRef)
    } else {
        let logical = LogicalType::Scalar(scalar);
        (if array.null_count() != 0 { logical.maybe() } else { logical }, array)
    };
    Ok(Some((rtdb_columnar::to_field(name, &logical)?, array)))
}

fn primitive<'a, T: ArrowPrimitiveType>(
    count: usize, mut cell: impl FnMut(usize) -> Result<Option<&'a [u8]>>,
    mut read: impl FnMut(&mut Input<'a>) -> Result<T::Native>,
) -> Result<ArrayRef> {
    let mut builder = PrimitiveBuilder::<T>::with_capacity(count);
    for row in 0..count {
        builder.append_option(cell(row)?.map(|bytes| parse(bytes, &mut read)).transpose()?);
    }
    Ok(Arc::new(builder.finish()))
}

fn parse<'a, T>(bytes: &'a [u8], read: impl FnOnce(&mut Input<'a>) -> Result<T>) -> Result<T> {
    let mut input = Input(bytes);
    let value = read(&mut input)?;
    if !input.0.is_empty() { return Err(invalid("Trailing bytes in PostgreSQL binary value")); }
    Ok(value)
}
