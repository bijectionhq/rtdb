use crate::{
    QueryContext,
    columnar::{
        BatchCursor, BoxBatchCursor, Column, ColumnBatch, gather_list_pages, retain_list_spans,
        single_batch,
    },
    context::Execution,
    invalid,
};
use arrow_array::{
    Array, ArrayRef, NullArray,
    builder::{
        ArrayBuilder, BooleanBuilder, FixedSizeBinaryBuilder, GenericByteBuilder, PrimitiveBuilder,
    },
    cast::AsArray,
    downcast_primitive,
    types::{BinaryType, ByteArrayType, LargeBinaryType, LargeUtf8Type, Utf8Type},
};
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType;
use rtdb_columnar::MemoryReservation;
use rtdb_plan::Expr;
use rtdb_types::{LogicalType, Result};
use std::sync::Arc;

pub(crate) fn open_arrow(execution: &Execution, position: usize) -> Result<Option<BoxBatchCursor>> {
    let Some((input, row)) = execution.parameters.arrow_input(position) else { return Ok(None); };
    let output = ColumnBatch::from_list_parameter(
        input, position, row, &execution.parameters.types()[position], &execution.context.memory,
    )?;
    Ok(output.map(|output| single_batch(Some(output), Arc::clone(&execution.context))))
}

pub(crate) async fn open_columns(execution: &Execution, expression: Expr) -> Result<BoxBatchCursor> {
    if let Expr::Parameter { index, .. } = expression
        && let Some(input) = open_arrow(execution, index)?
    {
        return Ok(input);
    }
    let mut input = execution.open_list(expression, None).await?;
    let context = &execution.context;
    let memory = &context.memory;
    let mut pages = Vec::new();
    let mut rows = 0usize;
    let mut inventory = memory.reserve(0)?;
    while let Some(batch) = input.next_batch().await? {
        rows = rows.checked_add(batch.len).ok_or_else(size_error)?;
        let lease = Arc::get_mut(&mut inventory).expect("column table owns its page inventory");
        lease.grow_to(lease.bytes().checked_add(256).ok_or_else(size_error)?)?;
        pages.push(batch);
        tokio::task::yield_now().await;
    }
    drop(input);
    context.check_cancelled()?;
    let result = if rows == 0 {
        ColumnBatch::empty_with_width(1, memory)?
    } else if pages.len() == 1 {
        pages.pop().expect("One complete list page")
    } else {
        let (column, charge) = if let Some(output) = assemble_flat_pages(&pages, rows, context).await? {
            output
        } else if let Some(output) = retain_list_spans(&pages, context).await? {
            output
        } else {
            gather_list_pages(&pages, rows, context)?
        };
        drop(pages);
        drop(inventory);
        let mut result = ColumnBatch::settled(Vec::new(), rows, memory.reserve(256)?)?;
        result.append_column(column, charge, memory)?;
        result
    };
    context.check_cancelled()?;
    Ok(single_batch(Some(result), Arc::clone(context)))
}

fn size_error() -> rtdb_types::Anomaly {
    invalid("column-table-size", "Column table exceeds addressable memory")
}

async fn assemble_flat_pages(
    pages: &[ColumnBatch], rows: usize, context: &QueryContext,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let [Column::Arrow { field, logical, .. }] = pages[0].columns.as_slice() else { return Ok(None); };
    let ty = field.data_type();
    let width = match ty {
        DataType::Utf8 | DataType::Binary => 4,
        DataType::LargeUtf8 | DataType::LargeBinary => 8,
        DataType::FixedSizeBinary(width) => usize::try_from(*width).map_err(|_| size_error())?,
        DataType::Boolean | DataType::Null => 0,
        ty => match ty.primitive_width() {
            Some(width) => width,
            None => return Ok(None),
        },
    };
    let mut payload = 0usize;
    for (index, page) in pages.iter().enumerate() {
        if index % 1024 == 0 {
            tokio::task::yield_now().await;
            context.check_cancelled()?;
        }
        let [Column::Arrow { array, field: other, logical: other_type }] = page.columns.as_slice() else { return Ok(None); };
        if other != field || other_type != logical || array.data_type() != ty || array.len() != page.len {
            return Ok(None);
        }
        let bytes = match ty {
            DataType::Utf8 => byte_span::<Utf8Type>(array.as_ref()),
            DataType::LargeUtf8 => byte_span::<LargeUtf8Type>(array.as_ref()),
            DataType::Binary => byte_span::<BinaryType>(array.as_ref()),
            DataType::LargeBinary => byte_span::<LargeBinaryType>(array.as_ref()),
            _ => 0,
        };
        payload = payload.checked_add(bytes).ok_or_else(size_error)?;
    }
    let exceeds_offsets = match ty {
        DataType::Utf8 | DataType::Binary => payload > i32::MAX as usize,
        DataType::FixedSizeBinary(_) => rows.checked_mul(width).ok_or_else(size_error)? > i32::MAX as usize,
        _ => false,
    };
    if exceeds_offsets {
        return Ok(None);
    }
    let logical_bytes = logical.as_ref().map_or(Ok(0), LogicalType::retained_bytes)?;
    let bound = rows.checked_add(1).and_then(|slots| slots.checked_mul(width))
        .and_then(|bytes| bytes.checked_add(payload))
        .and_then(|bytes| bytes.checked_add(rows.div_ceil(8).checked_mul(2)?))
        .and_then(|bytes| bytes.checked_add(field.size()))
        .and_then(|bytes| bytes.checked_add(logical_bytes))
        .and_then(|bytes| bytes.checked_add(4096))
        .ok_or_else(size_error)?;
    let reservation = match context.memory.reserve(bound) {
        Ok(reservation) => reservation,
        Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => return Ok(None),
        Err(error) => return Err(error),
    };
    let arrow_error = |error| invalid("column-gather", error);
    macro_rules! bytes {
        ($ty:ty) => {
            build_spans(pages, context, GenericByteBuilder::<$ty>::with_capacity(rows, payload), |out, span|
                out.append_array(span.as_bytes::<$ty>()).map_err(arrow_error)).await?
        };
    }
    macro_rules! primitive {
        ($ty:ty) => {
            build_spans(pages, context, PrimitiveBuilder::<$ty>::with_capacity(rows).with_data_type(ty.clone()), |out, span| {
                out.append_array(span.as_primitive::<$ty>());
                Ok(())
            }).await?
        };
    }
    let array = match ty {
        DataType::Null => Arc::new(NullArray::new(rows)) as ArrayRef,
        DataType::Boolean => build_spans(pages, context, BooleanBuilder::with_capacity(rows), |out, span| {
            out.append_array(span.as_boolean());
            Ok(())
        }).await?,
        DataType::FixedSizeBinary(width) => build_spans(pages, context, FixedSizeBinaryBuilder::with_capacity(rows, *width), |out, span|
            out.append_array(span.as_fixed_size_binary()).map_err(arrow_error)).await?,
        DataType::Utf8 => bytes!(Utf8Type),
        DataType::LargeUtf8 => bytes!(LargeUtf8Type),
        DataType::Binary => bytes!(BinaryType),
        DataType::LargeBinary => bytes!(LargeBinaryType),
        ty => downcast_primitive!(ty => (primitive), _ => return Ok(None)),
    };
    Ok(Some((Column::Arrow { array, field: Arc::clone(field), logical: logical.clone() }, reservation)))
}

fn byte_span<T: ByteArrayType>(array: &dyn Array) -> usize {
    let offsets = array.as_bytes::<T>().value_offsets();
    offsets[array.len()].as_usize() - offsets[0].as_usize()
}

async fn build_spans<B: ArrayBuilder>(
    pages: &[ColumnBatch], context: &QueryContext, mut output: B,
    mut append: impl FnMut(&mut B, &dyn Array) -> Result<()>,
) -> Result<ArrayRef> {
    let mut remaining = 1024;
    for page in pages {
        let Column::Arrow { array, .. } = &page.columns[0] else { unreachable!("compatible list page"); };
        let mut start = 0;
        while start < page.len {
            let count = (page.len - start).min(remaining);
            if count == page.len {
                append(&mut output, array.as_ref())?;
            } else {
                append(&mut output, array.slice(start, count).as_ref())?;
            }
            start += count;
            remaining -= count;
            if remaining == 0 {
                tokio::task::yield_now().await;
                context.check_cancelled()?;
                remaining = 1024;
            }
        }
    }
    Ok(output.finish())
}
