use std::sync::Arc;

use arrow_array::{Array, FixedSizeListArray, Float64Array, ListArray, StructArray, TimestampMicrosecondArray};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_schema::{DataType, TimeUnit as ArrowUnit};
use rtdb_columnar::MemoryReservation;
use rtdb_expr::kernels::RangeBins;
use rtdb_plan::{Expr, Schema};
use rtdb_types::{ListKind, LogicalType, Result, ScalarType, TimeUnit, Value};

use super::{Column, ColumnBatch, float_vector::nonnull, owned_projection_bytes, size_error};
use crate::QueryContext;

pub(crate) fn is_zoned_micros(ty: &LogicalType) -> bool {
    matches!(nonnull(ty), LogicalType::Scalar(ScalarType::Timestamp {
        unit: TimeUnit::Microsecond, timezone: Some(_),
    }))
}

pub(crate) fn is_period(ty: &LogicalType) -> bool {
    matches!(nonnull(ty), LogicalType::List { kind: ListKind::TimestampTzRange, element } if is_zoned_micros(element))
}

pub(crate) fn bounds(column: &Column, row: usize) -> Option<Option<(i64, i64)>> {
    if let Column::Values(values) = column {
        return match values.get(row)? {
            Value::TimestampTzRange { lower, upper } => Some(Some((*lower, *upper))),
            Value::Null | Value::Undefined => Some(None),
            _ => None,
        };
    }
    let Some((array, field, logical, row)) = column.arrow_cell(row)? else { return Some(None); };
    if array.data_type() != field.data_type() { return None; }
    if field.data_type() == &DataType::Null && field.is_nullable() && logical.is_none_or(|ty| *ty == LogicalType::Null) {
        return Some(None);
    }
    let DataType::FixedSizeList(child, 2) = field.data_type() else { return None; };
    if field.extension_type_name() != Some("xt/tstz-range") || logical.is_some_and(|ty| !is_period(ty))
        || !child.metadata().is_empty() || !matches!(child.data_type(), DataType::Timestamp(ArrowUnit::Microsecond, Some(_)))
    {
        return None;
    }
    let ranges = array.as_any().downcast_ref::<FixedSizeListArray>()?;
    if ranges.is_null(row) { return Some(None); }
    let endpoints = ranges.values().as_any().downcast_ref::<TimestampMicrosecondArray>()?;
    let offset = ranges.value_offset(row) as usize;
    if endpoints.is_null(offset) || endpoints.is_null(offset + 1) { return None; }
    Some(Some((endpoints.value(offset), endpoints.value(offset + 1))))
}

pub(super) fn range_bins(expr: &Expr, schema: &Schema, batch: &ColumnBatch, context: &QueryContext)
    -> Result<Option<(Column, Arc<MemoryReservation>)>>
{
    let Expr::Call { name, args, .. } = expr else { return Ok(None); };
    let [Expr::Literal(stride), Expr::Call { name: lower, args: from, .. }, Expr::Call { name: upper, args: to, .. }] = args.as_slice()
        else { return Ok(None); };
    if name != "range_bins" || lower != "lower" || upper != "upper" || from != to { return Ok(None); }
    let [Expr::Column { id, .. }] = from.as_slice() else { return Ok(None); };
    let Some(position) = schema.position(*id) else { return Ok(None); };
    if !is_period(&schema.fields()[position].data_type)
        || !matches!(stride.logical_type(), LogicalType::Scalar(ScalarType::Interval(_) | ScalarType::IntervalMonthDayMicro))
    {
        return Ok(None);
    }
    let Ok(stride) = RangeBins::stride_micros(stride, &context.timezone) else { return Ok(None); };
    let input = &batch.columns[position];

    let row_bytes = size_of::<Option<RangeBins>>() + size_of::<i32>() + 1;
    let overhead = batch.len.checked_mul(row_bytes).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
    let mut owner = context.memory.reserve(overhead)?;
    let mut bins = Vec::with_capacity(batch.len);
    let mut offsets = Vec::with_capacity(batch.len + 1);
    offsets.push(0i32);
    let mut valid = NullBufferBuilder::new(batch.len);
    let mut total = 0usize;
    for row in 0..batch.len {
        context.check_cancelled_at(row)?;
        let Some(period) = bounds(input, row) else { return Ok(None); };
        let row_bins = period.filter(|(_, upper)| *upper != i64::MAX)
            .map(|(from, to)| RangeBins::from_micros(stride, from, to, 0));
        total = total.checked_add(row_bins.map_or(Ok(0), RangeBins::len)?).ok_or_else(size_error)?;
        let Ok(offset) = i32::try_from(total) else { return Ok(None); };
        offsets.push(offset);
        valid.append(row_bins.is_some());
        bins.push(row_bins);
    }
    let bytes = total.checked_mul(24).and_then(|bytes| bytes.checked_add(overhead)).ok_or_else(size_error)?;
    Arc::get_mut(&mut owner).expect("The bin projection owns its reservation").grow_to(bytes)?;
    let mut from = Vec::with_capacity(total);
    let mut to = Vec::with_capacity(total);
    let mut weight = Vec::with_capacity(total);
    for (bins, offsets) in bins.into_iter().zip(offsets.windows(2)) {
        let Some(bins) = bins else { continue; };
        for index in 0..(offsets[1] - offsets[0]) as usize {
            context.check_cancelled_at(from.len())?;
            let (start, end, fraction) = bins.cell(index);
            from.push(start);
            to.push(end);
            weight.push(fraction);
        }
    }
    let logical = LogicalType::list(rtdb_plan::range_bin_element_type());
    let nulls = valid.finish();
    let logical = if nulls.is_some() { logical.maybe() } else { logical };
    let field = Arc::new(rtdb_columnar::to_field("value", &logical)?);
    let DataType::List(child) = field.data_type() else { unreachable!("Range bins are a list"); };
    let DataType::Struct(fields) = child.data_type() else { unreachable!("Range bin elements are structs"); };
    let arrow_error = |error| crate::invalid("range-bins", error);
    let values = StructArray::try_new(fields.clone(), vec![
        Arc::new(TimestampMicrosecondArray::from(from).with_timezone("UTC")),
        Arc::new(TimestampMicrosecondArray::from(to).with_timezone("UTC")),
        Arc::new(Float64Array::from(weight)),
    ], None).map_err(arrow_error)?;
    let array = ListArray::try_new(Arc::clone(child), OffsetBuffer::new(offsets.into()), Arc::new(values), nulls)
        .map_err(arrow_error)?;
    let column = Column::Arrow { array: Arc::new(array), field, logical: Some(logical) };
    let lease = Arc::get_mut(&mut owner).expect("The bin projection owns its reservation");
    let retained = owned_projection_bytes(&column)?;
    if retained <= lease.bytes() { lease.shrink_to(retained)?; } else { lease.grow_to(retained)?; }
    Ok(Some((column, owner)))
}
