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

pub(crate) fn timestamp_ticks(column: &Column, row: usize) -> Option<Option<i64>> {
    if let Column::Values(values) = column {
        return match values.get(row)? {
            Value::Timestamp { unit: TimeUnit::Microsecond, timezone: Some(_), value } => Some(Some(*value)),
            Value::Null | Value::Undefined => Some(None),
            _ => None,
        };
    }
    let Some((array, field, logical, row)) = column.arrow_cell(row)? else { return Some(None); };
    if array.data_type() != field.data_type() || !field.metadata().is_empty() { return None; }
    if field.data_type() == &DataType::Null && field.is_nullable() && logical.is_none_or(|ty| *ty == LogicalType::Null) {
        return Some(None);
    }
    if !matches!(field.data_type(), DataType::Timestamp(ArrowUnit::Microsecond, Some(_)))
        || logical.is_some_and(|ty| !is_zoned_micros(ty))
    {
        return None;
    }
    let values = array.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
    Some(values.is_valid(row).then(|| values.value(row)))
}

pub(super) fn construct(expr: &Expr, schema: &Schema, batch: &ColumnBatch, context: &QueryContext)
    -> Result<Option<(Column, Arc<MemoryReservation>)>>
{
    let Expr::Call { name, args, return_type } = expr else { return Ok(None); };
    if name != "period" || !is_period(return_type) { return Ok(None); }
    let [lower, upper] = args.as_slice() else { return Ok(None); };
    let bind_endpoint = |expr: &Expr| match expr {
        Expr::Column { id, .. } => {
            let position = schema.position(*id)?;
            let ty = &schema.fields()[position].data_type;
            let column = batch.columns.get(position)?;
            (*ty == LogicalType::Null || is_zoned_micros(ty)).then_some(Some(column))
        }
        Expr::Literal(Value::Null | Value::Undefined) => Some(None),
        _ => None,
    };
    let (Some(lower), Some(upper)) = (bind_endpoint(lower), bind_endpoint(upper)) else { return Ok(None); };
    let length = batch.len.checked_mul(2).ok_or_else(size_error)?;
    let bytes = length.checked_mul(size_of::<i64>()).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
    let mut owner = context.memory.reserve(bytes)?;
    let mut endpoints = Vec::with_capacity(length);
    for row in 0..batch.len {
        context.check_cancelled_at(row)?;
        let read = |column: Option<&Column>| column.map_or(Some(None), |column| timestamp_ticks(column, row));
        let (Some(lower), Some(upper)) = (read(lower), read(upper)) else { return Ok(None); };
        let lower = lower.unwrap_or(i64::MIN);
        let upper = upper.unwrap_or(i64::MAX);
        if lower >= upper { return Ok(None); }
        endpoints.extend([lower, upper]);
    }
    let logical = Value::TimestampTzRange { lower: i64::MIN, upper: i64::MAX }.logical_type();
    let field = Arc::new(rtdb_columnar::to_field("value", &logical)?);
    let DataType::FixedSizeList(child, 2) = field.data_type() else { unreachable!("Periods have two endpoints"); };
    let endpoints = Arc::new(TimestampMicrosecondArray::from(endpoints).with_timezone("UTC"));
    let array = FixedSizeListArray::try_new(Arc::clone(child), 2, endpoints, None)
        .map_err(|error| crate::invalid("period-column", error))?;
    let column = Column::Arrow { array: Arc::new(array), field, logical: Some(logical) };
    settle(column, &mut owner).map(|column| Some((column, owner)))
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
    settle(column, &mut owner).map(|column| Some((column, owner)))
}

fn settle(column: Column, owner: &mut Arc<MemoryReservation>) -> Result<Column> {
    let lease = Arc::get_mut(owner).expect("The period projection owns its reservation");
    let retained = owned_projection_bytes(&column)?;
    if retained <= lease.bytes() { lease.shrink_to(retained)?; } else { lease.grow_to(retained)?; }
    Ok(column)
}

#[test]
fn period_projection_writes_arrow_with_a_literal_unbounded_endpoint() {
    use rtdb_columnar::MemoryBudget;
    use rtdb_plan::{ColumnId, Field};
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    use rtdb_types::Timestamp;
    use tokio_util::sync::CancellationToken;

    let memory = MemoryBudget::new(1 << 20);
    let catalog = crate::CatalogSnapshot::new("xtdb", [("xtdb".into(), crate::DatabaseSnapshot {
        partitions: vec![],
        storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)),
    })].into(), false).unwrap();
    let context = QueryContext::new(catalog, Arc::clone(&memory), Timestamp(0), "UTC", Default::default(),
        &CancellationToken::new()).unwrap();
    let ty = LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }).maybe();
    let input = Field { id: ColumnId(0), name: "endpoint".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: ty.clone() };
    let schema = Schema::new(vec![input.clone()]).unwrap();
    let array = TimestampMicrosecondArray::from(vec![Some(7), None]).with_timezone("UTC");
    let field = Arc::new(rtdb_columnar::to_field("endpoint", &ty).unwrap());
    let batch = ColumnBatch::settled(vec![Column::Arrow { array: Arc::new(array), field, logical: Some(ty) }],
        2, memory.reserve(4096).unwrap()).unwrap();

    for (args, expected) in [
        (vec![input.expression(), Expr::Literal(Value::Null)], (7, i64::MAX)),
        (vec![Expr::Literal(Value::Null), input.expression()], (i64::MIN, 7)),
    ] {
        let period = Expr::Call { name: "period".into(), args,
            return_type: Value::TimestampTzRange { lower: i64::MIN, upper: i64::MAX }.logical_type() };
        let (column, _owner) = construct(&period, &schema, &batch, &context).unwrap().unwrap();
        assert!(matches!(column, Column::Arrow { .. }));
        assert_eq!(bounds(&column, 0), Some(Some(expected)));
        assert_eq!(bounds(&column, 1), Some(Some((i64::MIN, i64::MAX))));
    }
}
