use std::sync::Arc;

use arrow_array::{Array, FixedSizeListArray, Float64Array, ListArray, StructArray, TimestampMicrosecondArray};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_schema::{DataType, Field as ArrowField, TimeUnit as ArrowUnit};
use rtdb_columnar::{ArrowCell, MemoryReservation};
use rtdb_expr::kernels::{RangeBins, validate_period_bounds};
use rtdb_plan::{Expr, Schema};
use rtdb_types::{ListKind, LogicalType, Result, ScalarType, TimeUnit, Value};

use super::{Column, ColumnBatch, calendar::{BinStride, CalendarInput, CalendarType}, float_vector::nonnull, owned_projection_bytes, size_error};
use crate::{QueryContext, parameters::BoundParameters};

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

enum PeriodEndpoint<'a> {
    Open,
    Calendar(CalendarInput<'a>),
    MicrosColumn(&'a Column),
}

impl<'a> PeriodEndpoint<'a> {
    fn bind(expr: &'a Expr, schema: &'a Schema, batch: &'a ColumnBatch, params: &'a BoundParameters) -> Option<Self> {
        if let Expr::Column { id, .. } = expr {
            let position = schema.position(*id)?;
            let ty = &schema.fields()[position].data_type;
            if *ty == LogicalType::Null || is_zoned_micros(ty) {
                return Some(Self::MicrosColumn(batch.columns.get(position)?));
            }
        }
        match expr {
            Expr::Literal(Value::Null | Value::Undefined) => Some(Self::Open),
            Expr::Parameter { index, data_type: LogicalType::Null } if params.types().get(*index)? == &LogicalType::Null => {
                let parameters = params.evaluator();
                let null = match parameters.arrow_cell(*index) {
                    Some(cell) => cell.field.is_nullable() && cell.field.metadata().is_empty()
                        && cell.field.data_type() == &DataType::Null && cell.array.data_type() == &DataType::Null && cell.row < cell.array.len(),
                    None => matches!(parameters.known(*index), Some(Value::Null | Value::Undefined)),
                };
                null.then_some(Self::Open)
            }
            _ => CalendarInput::bind(expr, schema, batch, params).map(Self::Calendar),
        }
    }

    fn ticks(&self, row: usize) -> Option<Option<i64>> {
        match self {
            Self::Open => Some(None),
            Self::Calendar(input) => input.ticks(row),
            Self::MicrosColumn(column) => timestamp_ticks(column, row),
        }
    }

    fn micros(&self, ticks: Option<i64>, infinity: i64, timezone: &str) -> Result<i64> {
        match (self, ticks) {
            (_, None) => Ok(infinity),
            (Self::Calendar(input), Some(ticks)) => input.ty.cast_timestamp(ticks, TimeUnit::Microsecond, true, timezone),
            (_, Some(ticks)) => Ok(ticks),
        }
    }
}

pub(super) fn construct(expr: &Expr, schema: &Schema, batch: &ColumnBatch, params: &BoundParameters, context: &QueryContext)
    -> Result<Option<(Column, Arc<MemoryReservation>)>>
{
    let Expr::Call { name, args, return_type } = expr else { return Ok(None); };
    if name != "period" || !is_period(return_type) { return Ok(None); }
    let [lower, upper] = args.as_slice() else { return Ok(None); };
    let (Some(lower), Some(upper)) = (PeriodEndpoint::bind(lower, schema, batch, params), PeriodEndpoint::bind(upper, schema, batch, params))
        else { return Ok(None); };
    let length = batch.len.checked_mul(2).ok_or_else(size_error)?;
    let bytes = length.checked_mul(size_of::<i64>()).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
    let mut owner = context.memory.reserve(bytes)?;
    let mut endpoints = Vec::with_capacity(length);
    for row in 0..batch.len {
        context.check_cancelled_at(row)?;
        let (Some(from), Some(to)) = (lower.ticks(row), upper.ticks(row)) else { return Ok(None); };
        let from = lower.micros(from, i64::MIN, &context.timezone)?;
        let to = upper.micros(to, i64::MAX, &context.timezone)?;
        validate_period_bounds(from, to)?;
        endpoints.extend([from, to]);
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
    if let Column::Values(values) = column { return period_value(values.get(row)?); }
    let Some((array, field, logical, row)) = column.arrow_cell(row)? else { return Some(None); };
    bounds_arrow(array, field, logical, row)
}

fn period_value(value: &Value) -> Option<Option<(i64, i64)>> {
    match value {
        Value::TimestampTzRange { lower, upper } => Some(Some((*lower, *upper))),
        Value::Null | Value::Undefined => Some(None),
        _ => None,
    }
}

fn bounds_arrow(array: &dyn Array, field: &ArrowField, logical: Option<&LogicalType>, row: usize) -> Option<Option<(i64, i64)>> {
    if array.data_type() != field.data_type() || row >= array.len() { return None; }
    if field.data_type() == &DataType::Null && field.is_nullable() && logical.is_none_or(|ty| *ty == LogicalType::Null) {
        return Some(None);
    }
    let DataType::FixedSizeList(child, 2) = field.data_type() else { return None; };
    if field.extension_type_name() != Some("xt/tstz-range") || logical.is_some_and(|ty| !is_period(ty))
        || !child.metadata().is_empty() || !matches!(child.data_type(), DataType::Timestamp(ArrowUnit::Microsecond, Some(_)))
    {
        return None;
    }
    if array.is_null(row) { return Some(None); }
    let (_, children, cells) = ArrowCell { field, array, row }.sequence_range()?;
    let endpoints = children.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
    let (lower, upper) = (cells.start, cells.end - 1);
    if endpoints.is_null(lower) || endpoints.is_null(upper) { return None; }
    Some(Some((endpoints.value(lower), endpoints.value(upper))))
}

enum PeriodInput<'a> {
    Column(&'a Column),
    Constant(Option<(i64, i64)>),
}

impl<'a> PeriodInput<'a> {
    fn bind(expr: &'a Expr, schema: &'a Schema, batch: &'a ColumnBatch, params: &'a BoundParameters) -> Option<Self> {
        match expr {
            Expr::Column { id, .. } => {
                let position = schema.position(*id)?;
                is_period(&schema.fields()[position].data_type).then_some(Self::Column(batch.columns.get(position)?))
            }
            Expr::Parameter { index, data_type } if is_period(data_type) && is_period(params.types().get(*index)?) => {
                let parameters = params.evaluator();
                let period = match parameters.arrow_cell(*index) {
                    Some(cell) => bounds_arrow(cell.array, cell.field, None, cell.row)?,
                    None => period_value(parameters.known(*index)?)?,
                };
                Some(Self::Constant(period))
            }
            Expr::Literal(value @ Value::TimestampTzRange { .. }) => Some(Self::Constant(period_value(value)?)),
            _ => None,
        }
    }

    fn bounds(&self, row: usize) -> Option<Option<(i64, i64)>> {
        match self {
            Self::Column(column) => bounds(column, row),
            Self::Constant(bounds) => Some(*bounds),
        }
    }
}

fn lowered_period<'a>(from: &'a Expr, to: &Expr) -> Option<&'a Expr> {
    let (Expr::Call { name: lower, args: left, .. }, Expr::Call { name: upper, args: right, .. }) = (from, to) else { return None; };
    let [period] = left.as_slice() else { return None; };
    (lower == "lower" && upper == "upper" && left == right).then_some(period)
}

enum BinEndpoints<'a> {
    Period(PeriodInput<'a>),
    Direct(CalendarInput<'a>, CalendarInput<'a>),
}

impl<'a> BinEndpoints<'a> {
    fn bind(from: &'a Expr, to: &'a Expr, schema: &'a Schema, batch: &'a ColumnBatch, params: &'a BoundParameters) -> Option<Self> {
        if let Some(period) = lowered_period(from, to) {
            return PeriodInput::bind(period, schema, batch, params).map(Self::Period);
        }
        Some(Self::Direct(CalendarInput::bind(from, schema, batch, params)?, CalendarInput::bind(to, schema, batch, params)?))
    }

    fn ticks(&self, row: usize) -> Option<Option<(i64, i64)>> {
        match self {
            Self::Period(period) => period.bounds(row).map(|bounds| bounds.filter(|(_, upper)| *upper != i64::MAX)),
            Self::Direct(from, to) => {
                let Some(from) = from.ticks(row)? else { return Some(None); };
                Some(to.ticks(row)?.map(|to| (from, to)))
            }
        }
    }

    fn types(&self) -> [CalendarType<'_>; 2] {
        match self {
            Self::Period(_) => [CalendarType::Timestamp(TimeUnit::Microsecond, Some("UTC")); 2],
            Self::Direct(from, to) => [from.ty, to.ty],
        }
    }
}

pub(super) fn range_bins(expr: &Expr, schema: &Schema, batch: &ColumnBatch, params: &BoundParameters, context: &QueryContext)
    -> Result<Option<(Column, Arc<MemoryReservation>)>>
{
    let Expr::Call { name, args, return_type } = expr else { return Ok(None); };
    let ([stride, from, to] | [stride, from, to, _]) = args.as_slice() else { return Ok(None); };
    let logical = LogicalType::list(rtdb_plan::range_bin_element_type());
    if !matches!(name.as_str(), "range_bins" | "range-bins") || nonnull(return_type) != &logical { return Ok(None); }
    let (Some(stride), Some(bounds)) = (BinStride::bind(stride, schema, batch, params), BinEndpoints::bind(from, to, schema, batch, params))
        else { return Ok(None); };
    let origin = match args.get(3) {
        Some(origin) => {
            let Some(origin) = CalendarInput::bind(origin, schema, batch, params) else { return Ok(None); };
            Some(origin)
        }
        None => None,
    };
    let [from, to] = bounds.types();
    let zoned = |ty: CalendarType<'_>| matches!(ty, CalendarType::Timestamp(_, Some(_)));
    let zoned_non_microsecond = |ty: CalendarType<'_>| matches!(ty, CalendarType::Timestamp(unit, Some(_)) if unit != TimeUnit::Microsecond);
    let mut types = [Some(from), Some(to), origin.as_ref().map(|origin| origin.ty)].into_iter().flatten();
    if types.clone().all(zoned) && types.any(zoned_non_microsecond)
    {
        return Ok(None);
    }

    let row_bytes = size_of::<Option<RangeBins>>() + size_of::<i32>() + 1;
    let overhead = batch.len.checked_mul(row_bytes).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
    let mut owner = context.memory.reserve(overhead)?;
    let mut bins = Vec::with_capacity(batch.len);
    let mut offsets = Vec::with_capacity(batch.len + 1);
    offsets.push(0i32);
    let mut valid = NullBufferBuilder::new(batch.len);
    let mut total = 0usize;
    let read = |row| -> Result<Option<Option<RangeBins>>> {
        let Some(stride) = stride.parts(row) else { return Ok(None); };
        let Some(stride) = stride else { return Ok(Some(None)); };
        let Some(ticks) = bounds.ticks(row) else { return Ok(None); };
        let Some((from_ticks, to_ticks)) = ticks else { return Ok(Some(None)); };
        let origin = if let Some(origin) = &origin {
            let Some(ticks) = origin.ticks(row) else { return Ok(None); };
            let Some(ticks) = ticks else { return Ok(Some(None)); };
            Some((origin.ty, ticks))
        } else { None };
        let stride = RangeBins::stride_parts_micros(stride.months, stride.days, stride.nanoseconds)?;
        let micros = |ty: CalendarType<'_>, ticks| ty.cast_timestamp(ticks, TimeUnit::Microsecond, true, &context.timezone);
        let from = micros(from, from_ticks)?;
        let to = micros(to, to_ticks)?;
        let origin = origin.map(|(ty, ticks)| micros(ty, ticks)).transpose()?.unwrap_or(0);
        Ok(Some(Some(RangeBins::from_micros(stride, from, to, origin))))
    };
    for row in 0..batch.len {
        context.check_cancelled_at(row)?;
        let Some(row_bins) = read(row)? else { return Ok(None); };
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
    let params = BoundParameters::infer(vec![], &memory).unwrap();

    for (args, expected) in [
        (vec![input.expression(), Expr::Literal(Value::Null)], (7, i64::MAX)),
        (vec![Expr::Literal(Value::Null), input.expression()], (i64::MIN, 7)),
    ] {
        let period = Expr::Call { name: "period".into(), args,
            return_type: Value::TimestampTzRange { lower: i64::MIN, upper: i64::MAX }.logical_type() };
        let (column, _owner) = construct(&period, &schema, &batch, &params, &context).unwrap().unwrap();
        assert!(matches!(column, Column::Arrow { .. }));
        assert_eq!(bounds(&column, 0), Some(Some(expected)));
        assert_eq!(bounds(&column, 1), Some(Some((i64::MIN, i64::MAX))));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rtdb_columnar::MemoryBudget;
    use rtdb_plan::{ColumnId, Expr, Field, Schema, function_type_with_args};
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    use rtdb_types::{CivilDate, LogicalType, Result, TimeUnit, Timestamp, Value};
    use tokio_util::sync::CancellationToken;

    use super::{Column, ColumnBatch, bounds, construct, range_bins};
    use crate::{QueryContext, parameters::BoundParameters};

    fn context(timezone: &str) -> Arc<QueryContext> {
        let memory = MemoryBudget::new(4 << 20);
        let catalog = crate::CatalogSnapshot::new("xtdb", [("xtdb".into(), crate::DatabaseSnapshot {
            partitions: vec![],
            storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)),
        })].into(), false).unwrap();
        QueryContext::new(catalog, memory, Timestamp(0), timezone, Default::default(), &CancellationToken::new()).unwrap()
    }

    fn field(id: u32, data_type: LogicalType) -> Field {
        Field { id: ColumnId(id), name: format!("c{id}"), qualifier: None, qualified_only: false, unnamed: false, data_type }
    }

    fn timestamp(value: i64, unit: TimeUnit, zone: Option<&str>) -> Value {
        Value::Timestamp { value, unit, timezone: zone.map(str::to_owned) }
    }

    fn interval(months: i32, days: i32, nanoseconds: i64) -> Value {
        Value::IntervalMonthDayMicro { months, days, nanoseconds }
    }

    fn parameter(index: usize, value: &Value) -> Expr {
        Expr::Parameter { index, data_type: value.logical_type() }
    }

    fn call(name: &str, args: Vec<Expr>) -> Expr {
        let return_type = function_type_with_args(name, &args).unwrap();
        Expr::Call { name: name.into(), args, return_type }
    }

    fn batch(schema: &Schema, input: &[Vec<Value>], context: &QueryContext) -> ColumnBatch {
        let columns = schema.fields().iter().zip(input).map(|(field, cells)| Column::Arrow {
            array: rtdb_columnar::build_array(&field.data_type, cells).unwrap(),
            field: Arc::new(rtdb_columnar::to_field(&field.name, &field.data_type).unwrap()),
            logical: Some(field.data_type.clone()),
        }).collect();
        ColumnBatch::settled(columns, input[0].len(), context.memory.reserve(4096).unwrap()).unwrap()
    }

    fn scalar(expr: &Expr, schema: &Schema, column_cells: &[Vec<Value>], row: usize, params: &BoundParameters, context: &QueryContext) -> Result<Value> {
        let values = column_cells.iter().map(|cells| cells[row].clone()).collect::<Vec<_>>();
        let context = rtdb_expr::EvalContext { timezone: &context.timezone, parameters: params.evaluator(),
            parameter_types: Some(params.types()), ..Default::default() };
        rtdb_expr::evaluate_projection(expr, schema, &values, &context)
    }

    #[test]
    fn period_casts_date64_and_a_bound_nanosecond_endpoint() {
        let ctx = context("America/New_York");
        let date = field(0, Value::DateMilli(0).logical_type().maybe());
        let schema = Schema::new(vec![date.clone()]).unwrap();
        let day = CivilDate::from_ymd(2021, 3, 14).unwrap().epoch_day();
        let input = [vec![Value::DateMilli(day * 86_400_000 + 12_345), Value::Null]];
        let upper = timestamp(1_615_780_800_000_000_123, TimeUnit::Nanosecond, Some("Europe/Paris"));
        let expr = call("period", vec![date.expression(), parameter(0, &upper)]);
        let open = call("period", vec![date.expression(), Expr::Parameter { index: 1, data_type: LogicalType::Null }]);
        let params = BoundParameters::infer(vec![upper, Value::Null], &ctx.memory).unwrap();
        let input_page = batch(&schema, &input, &ctx);
        for expr in [expr, open] {
            let (output, _) = construct(&expr, &schema, &input_page, &params, &ctx).unwrap().unwrap();
            assert!(matches!(output, Column::Arrow { .. }));
            for row in 0..2 {
                assert_eq!(output.value(row).unwrap(), scalar(&expr, &schema, &input, row, &params, &ctx).unwrap());
            }
            assert_eq!(bounds(&output, 0).unwrap().unwrap().0, 1_615_698_000_000_000);
            assert_eq!(bounds(&output, 1).unwrap().unwrap().0, i64::MIN);
        }
    }

    #[test]
    fn period_reports_the_first_endpoint_conversion_or_ordering_error() {
        let ctx = context("UTC");
        let from = field(0, timestamp(0, TimeUnit::Second, Some("UTC")).logical_type());
        let schema = Schema::new(vec![from.clone()]).unwrap();
        let period = call("period", vec![from.expression(), Expr::Literal(timestamp(0, TimeUnit::Microsecond, Some("UTC")))]);
        let params = BoundParameters::infer(vec![], &ctx.memory).unwrap();
        for (rows, code) in [([i64::MAX, 1], "xtdb.expression/overflow-error"), ([1, i64::MAX], "xtdb/invalid-period")] {
            let input = [rows.map(|seconds| timestamp(seconds, TimeUnit::Second, Some("UTC"))).to_vec()];
            let page = batch(&schema, &input, &ctx);
            let error = construct(&period, &schema, &page, &params, &ctx).err().unwrap();
            let expected = scalar(&period, &schema, &input, 0, &params, &ctx).unwrap_err();
            assert_eq!(error.code, code);
            assert_eq!(error.code, expected.code);
            assert_eq!(error.context, expected.context);
        }
    }

    #[test]
    fn range_bins_reads_dynamic_strides_and_a_local_bound_origin() {
        let ctx = context("America/New_York");
        let stride = field(0, interval(0, 0, 0).logical_type().maybe());
        let finite = Value::TimestampTzRange { lower: 18_000_000_000, upper: 18_000_005_000 };
        let period = field(1, finite.logical_type());
        let schema = Schema::new(vec![stride.clone(), period.clone()]).unwrap();
        let origin = timestamp(1, TimeUnit::Millisecond, None);
        let args = vec![stride.expression(), call("lower", vec![period.expression()]),
            call("upper", vec![period.expression()]), parameter(0, &origin)];
        let bins = call("range_bins", args);
        let params = BoundParameters::infer(vec![origin], &ctx.memory).unwrap();
        let input = [vec![interval(0, 0, 2_000_000), interval(0, 0, 3_000_000), interval(1, 0, 0), Value::Null],
            vec![finite.clone(), finite.clone(), Value::TimestampTzRange { lower: 18_000_000_000, upper: i64::MAX }, finite]];
        let page = batch(&schema, &input, &ctx);
        let (output, _) = range_bins(&bins, &schema, &page, &params, &ctx).unwrap().unwrap();
        for row in 0..4 {
            assert_eq!(output.value(row).unwrap(), scalar(&bins, &schema, &input, row, &params, &ctx).unwrap());
        }
        assert_eq!(output.value(2).unwrap(), Value::Null);
        assert_eq!(output.value(3).unwrap(), Value::Null);
    }

    #[test]
    fn range_bins_reads_a_bound_period_with_a_literal_stride() {
        let ctx = context("UTC");
        let input = [vec![Value::Null]];
        let schema = Schema::new(vec![field(0, LogicalType::Null)]).unwrap();
        let range = Value::TimestampTzRange { lower: -7, upper: 8 };
        let period = parameter(0, &range);
        let expr = call("range-bins", vec![Expr::Literal(interval(0, 0, 3_000)),
            call("lower", vec![period.clone()]), call("upper", vec![period])]);
        let params = BoundParameters::infer(vec![range], &ctx.memory).unwrap();
        let page = batch(&schema, &input, &ctx);
        let (output, _) = range_bins(&expr, &schema, &page, &params, &ctx).unwrap().unwrap();
        assert_eq!(output.value(0).unwrap(), scalar(&expr, &schema, &input, 0, &params, &ctx).unwrap());
    }

    #[test]
    fn a_sliced_period_reads_its_own_endpoint_pair() {
        let first = Value::TimestampTzRange { lower: 1, upper: 3 };
        let second = Value::TimestampTzRange { lower: 7, upper: 13 };
        let logical = first.logical_type();
        let array = rtdb_columnar::build_array(&logical, &[first, second]).unwrap();
        let field = Arc::new(rtdb_columnar::to_field("value", &logical).unwrap());
        let column = Column::Arrow { array: array.slice(1, 1), field, logical: Some(logical) };
        assert_eq!(bounds(&column, 0), Some(Some((7, 13))));
    }

    #[test]
    fn range_bins_converts_mixed_scale_bounds_and_reports_the_first_bad_stride() {
        let ctx = context("UTC");
        let lo = timestamp(0, TimeUnit::Millisecond, None);
        let hi = timestamp(5_000_123, TimeUnit::Nanosecond, Some("Europe/Paris"));
        let stride = field(0, interval(0, 0, 0).logical_type());
        let from = field(1, lo.logical_type());
        let to = field(2, hi.logical_type());
        let schema = Schema::new(vec![stride.clone(), from.clone(), to.clone()]).unwrap();
        let expr = call("range_bins", vec![stride.expression(), from.expression(), to.expression()]);
        let params = BoundParameters::infer(vec![], &ctx.memory).unwrap();
        let input = [vec![interval(0, 0, 2_000_000)], vec![lo.clone()], vec![hi.clone()]];
        let page = batch(&schema, &input, &ctx);
        let (output, _) = range_bins(&expr, &schema, &page, &params, &ctx).unwrap().unwrap();
        assert_eq!(output.value(0).unwrap(), scalar(&expr, &schema, &input, 0, &params, &ctx).unwrap());

        let zero_micros = interval(0, 0, 999);
        let with_month = interval(1, 0, 2_000_000);
        for (strides, code) in [(vec![zero_micros.clone(), with_month.clone()], "xtdb.expression/division-by-zero"),
            (vec![with_month, zero_micros], "xtdb.expression/cannot-cast-mdn-interval-with-months")]
        {
            let input = [strides, vec![lo.clone(); 2], vec![hi.clone(); 2]];
            let page = batch(&schema, &input, &ctx);
            let error = range_bins(&expr, &schema, &page, &params, &ctx).err().unwrap();
            assert_eq!(error.code, code);
            assert_eq!(error.code, scalar(&expr, &schema, &input, 0, &params, &ctx).unwrap_err().code);
        }

        for (stride, code) in [(interval(1, 0, 2_000_000), "xtdb.expression/cannot-cast-mdn-interval-with-months"),
            (interval(0, 0, 2_000_000), "xtdb.expression/overflow-error")]
        {
            let input = [vec![stride], vec![timestamp(i64::MAX, TimeUnit::Millisecond, None)], vec![hi.clone()]];
            let page = batch(&schema, &input, &ctx);
            let error = range_bins(&expr, &schema, &page, &params, &ctx).err().unwrap();
            assert_eq!(error.code, code);
            assert_eq!(error.code, scalar(&expr, &schema, &input, 0, &params, &ctx).unwrap_err().code);
        }
    }
}
