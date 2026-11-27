use std::sync::Arc;

use arrow_array::{Array, ArrayRef, NullArray, builder::{
    BooleanBuilder, FixedSizeBinaryBuilder, PrimitiveBuilder, StringBuilder,
}, types::{ArrowPrimitiveType, Int16Type, Int32Type, Int64Type}};
use arrow_schema::Field;
use rtdb_columnar::BatchBuilder;
use rtdb_types::{LogicalType, Result, ScalarType};

use super::{Input, boolean, invalid, read_fixed};

/// Builds a direct binary column under the snapshot batch's reservation.
pub fn read_source_binary_column<'a>(
    name: &str, oid: u32, count: usize, mut cell: impl FnMut(usize) -> Result<Option<&'a [u8]>>,
    output: &mut BatchBuilder,
) -> Result<Option<(Field, ArrayRef)>> {
    let text_bytes = if matches!(oid, 25 | 1043) {
        let payload = (0..count).try_fold(0usize, |size, row| {
            size.checked_add(cell(row)?.map_or(0, <[u8]>::len)).ok_or_else(overflow)
        })?;
        i32::try_from(payload).map_err(invalid)?;
        payload
    } else {
        0
    };
    let (scalar, values) = match oid {
        16 => (ScalarType::Bool, Some(count.div_ceil(8))),
        21 => (ScalarType::I16, count.checked_mul(2)),
        23 => (ScalarType::I32, count.checked_mul(4)),
        20 => (ScalarType::I64, count.checked_mul(8)),
        25 | 1043 => {
            let offsets = count.checked_add(1).and_then(|size| size.checked_mul(4));
            (ScalarType::Utf8, offsets.and_then(|size| size.checked_add(text_bytes)))
        }
        2950 => (ScalarType::Uuid, count.checked_mul(16)),
        _ => return Ok(None),
    };
    // Allow for Arrow descriptors and bitmap capacity rounding in addition to the buffers.
    let bound = values.and_then(|size| size.checked_add(count.div_ceil(8)))
        .and_then(|size| size.checked_add(512)).ok_or_else(overflow)?;
    let array = output.column(bound, || {
        let array: ArrayRef = match &scalar {
            ScalarType::Bool => {
                let mut builder = BooleanBuilder::with_capacity(count);
                for row in 0..count {
                    builder.append_option(cell(row)?.map(|bytes| read_fixed(bytes, boolean)).transpose()?);
                }
                Arc::new(builder.finish())
            }
            ScalarType::I16 => primitive::<Int16Type>(count, cell, |input| input.i16())?,
            ScalarType::I32 => primitive::<Int32Type>(count, cell, |input| input.i32())?,
            ScalarType::I64 => primitive::<Int64Type>(count, cell, |input| input.i64())?,
            ScalarType::Utf8 => {
                let mut builder = StringBuilder::with_capacity(count, text_bytes);
                for row in 0..count {
                    builder.append_option(cell(row)?.map(std::str::from_utf8).transpose().map_err(invalid)?);
                }
                Arc::new(builder.finish())
            }
            ScalarType::Uuid => {
                let mut builder = FixedSizeBinaryBuilder::with_capacity(count, 16);
                for row in 0..count {
                    if let Some(bytes) = cell(row)? {
                        let uuid = read_fixed(bytes, |input| input.take(16))?;
                        builder.append_value(uuid).map_err(invalid)?;
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            _ => unreachable!("direct binary column type"),
        };
        Ok(if array.null_count() == count { Arc::new(NullArray::new(count)) } else { array })
    })?;
    let logical = match (count, array.logical_null_count()) {
        (0, _) => LogicalType::Nothing,
        (count, nulls) if count == nulls => LogicalType::Null,
        (_, 0) => LogicalType::Scalar(scalar),
        _ => LogicalType::Scalar(scalar).maybe(),
    };
    Ok(Some((rtdb_columnar::to_field(name, &logical)?, array)))
}

fn overflow() -> rtdb_types::Anomaly { invalid("PostgreSQL binary column allocation overflow") }

fn primitive<'a, T: ArrowPrimitiveType>(
    count: usize, mut cell: impl FnMut(usize) -> Result<Option<&'a [u8]>>,
    mut read: impl FnMut(&mut Input<'a>) -> Result<T::Native>,
) -> Result<ArrayRef> {
    let mut builder = PrimitiveBuilder::<T>::with_capacity(count);
    for row in 0..count {
        builder.append_option(cell(row)?.map(|bytes| read_fixed(bytes, &mut read)).transpose()?);
    }
    Ok(Arc::new(builder.finish()))
}
