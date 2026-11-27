use std::sync::Arc;

use arrow_array::{Array, Date32Array, Date64Array, IntervalMonthDayNanoArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, builder::PrimitiveBuilder, types::{ArrowPrimitiveType, Date32Type, Date64Type, IntervalMonthDayNano,
        IntervalMonthDayNanoType, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType}};
use arrow_schema::{DataType, Field as ArrowField, IntervalUnit as ArrowIntervalUnit};
use rtdb_columnar::MemoryReservation;
use rtdb_expr::kernels::{AgeOperand, FixedDateBin, calendar_age, cast_date_timestamp_ticks, cast_timestamp_ticks,
    timestamp_truncation_zone, truncate_date32, truncate_date64, truncate_timestamp};
use rtdb_plan::{CastTarget, CastTimezone, Expr, Schema, TemporalField, TemporalKind, truncation_zone};
use rtdb_types::{AnomalyKind, IntervalUnit, LogicalType, Result, ScalarType, TimeUnit, Value, ZoneId, java_date::MILLIS_PER_DAY};

use super::{Column, ColumnBatch, owned_projection_bytes, plain_scalar, size_error};
use crate::{QueryContext, parameters::BoundParameters};

enum CalendarSource<'a> {
    Column(&'a Column),
    Constant(Option<i64>),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CalendarType<'a> {
    DateDay,
    DateMilli,
    Timestamp(TimeUnit, Option<&'a str>),
}

impl<'a> CalendarType<'a> {
    fn of(scalar: &'a ScalarType) -> Option<Self> {
        Some(match scalar {
            ScalarType::DateDay => Self::DateDay,
            ScalarType::DateMilli => Self::DateMilli,
            ScalarType::Timestamp { unit, timezone } => Self::Timestamp(*unit, timezone.as_deref()),
            _ => return None,
        })
    }

    fn age_operand<'zone>(self, ticks: i64, zone: Option<&'zone ZoneId>) -> AgeOperand<'zone> {
        match self {
            Self::DateDay => AgeOperand::Date(ticks),
            Self::DateMilli => AgeOperand::Date(ticks.div_euclid(MILLIS_PER_DAY)),
            Self::Timestamp(unit, label) => AgeOperand::Timestamp(ticks, unit, label.and(zone)),
        }
    }

    pub(super) fn cast_timestamp(self, ticks: i64, unit: TimeUnit, zoned: bool, timezone: &str) -> Result<i64> {
        match self {
            Self::DateDay => cast_date_timestamp_ticks(ticks, unit, zoned, timezone),
            Self::DateMilli => cast_date_timestamp_ticks(ticks.div_euclid(MILLIS_PER_DAY), unit, zoned, timezone),
            Self::Timestamp(source, label) => cast_timestamp_ticks(ticks, source, label.is_some(), unit, zoned, timezone),
        }
    }
}

pub(super) struct CalendarInput<'a> {
    source: CalendarSource<'a>,
    pub(super) ty: CalendarType<'a>,
}

impl<'a> CalendarInput<'a> {
    pub(super) fn bind(expr: &'a Expr, schema: &'a Schema, batch: &'a ColumnBatch, params: &'a BoundParameters) -> Option<Self> {
        let (source, ty) = match expr {
            Expr::Literal(value) => {
                let (value, ty) = Self::value(value)?;
                (CalendarSource::Constant(Some(value)), ty)
            }
            Expr::Column { id, .. } => {
                let position = schema.position(*id)?;
                let ty = CalendarType::of(plain_scalar(&schema.fields()[position].data_type)?)?;
                let column = batch.columns.get(position)?;
                if matches!(column, Column::Values(_)) { return None; }
                (CalendarSource::Column(column), ty)
            }
            Expr::Parameter { index, data_type } => {
                let declared = plain_scalar(params.types().get(*index)?)?;
                if plain_scalar(data_type) != Some(declared) { return None; }
                let ty = CalendarType::of(declared)?;
                let parameters = params.evaluator();
                let ticks = match parameters.arrow_cell(*index) {
                    Some(cell) => Self::read_arrow(ty, cell.array, cell.field, None, cell.row)?,
                    None => match parameters.known(*index)? {
                        Value::Null | Value::Undefined => None,
                        value => {
                            let (ticks, actual) = Self::value(value)?;
                            if actual != ty { return None; }
                            Some(ticks)
                        }
                    },
                };
                (CalendarSource::Constant(ticks), ty)
            }
            _ => return None,
        };
        Some(Self { source, ty })
    }

    fn value(value: &'a Value) -> Option<(i64, CalendarType<'a>)> {
        match value {
            Value::Timestamp { value, unit, timezone } => Some((*value, CalendarType::Timestamp(*unit, timezone.as_deref()))),
            Value::DateDay(days) => Some((i64::from(*days), CalendarType::DateDay)),
            Value::DateMilli(ms) => Some((*ms, CalendarType::DateMilli)),
            _ => None,
        }
    }

    pub(super) fn ticks(&self, row: usize) -> Option<Option<i64>> {
        let column = match self.source {
            CalendarSource::Constant(value) => return Some(value),
            CalendarSource::Column(column) => column,
        };
        let Some((array, field, logical, row)) = column.arrow_cell(row)? else { return Some(None); };
        Self::read_arrow(self.ty, array, field, logical, row)
    }

    fn read_arrow(ty: CalendarType<'_>, array: &dyn Array, field: &ArrowField, logical: Option<&LogicalType>, row: usize) -> Option<Option<i64>> {
        if array.data_type() != field.data_type() || row >= array.len() || !field.metadata().is_empty() { return None; }
        if array.data_type() == &DataType::Null && field.is_nullable() && logical.is_none_or(|ty| *ty == LogicalType::Null) {
            return Some(None);
        }
        match (logical, ty, array.data_type()) {
            (Some(logical), _, _) if plain_scalar(logical).and_then(CalendarType::of) == Some(ty) => {}
            (None, CalendarType::DateDay, DataType::Date32) | (None, CalendarType::DateMilli, DataType::Date64) => {}
            (None, CalendarType::Timestamp(_, label), DataType::Timestamp(_, physical_zone)) if physical_zone.as_deref() == label => {}
            _ => return None,
        }
        let array = array.as_any();
        macro_rules! read {
            ($ty:ty) => {{
                let array = array.downcast_ref::<$ty>()?;
                Some(array.is_valid(row).then(|| i64::from(array.value(row))))
            }};
        }
        match ty {
            CalendarType::DateDay => read!(Date32Array),
            CalendarType::DateMilli => read!(Date64Array),
            CalendarType::Timestamp(TimeUnit::Second, _) => read!(TimestampSecondArray),
            CalendarType::Timestamp(TimeUnit::Millisecond, _) => read!(TimestampMillisecondArray),
            CalendarType::Timestamp(TimeUnit::Microsecond, _) => read!(TimestampMicrosecondArray),
            CalendarType::Timestamp(TimeUnit::Nanosecond, _) => read!(TimestampNanosecondArray),
        }
    }

    fn map_ticks<T>(&self, row: usize, f: impl FnOnce(i64) -> Result<T>) -> Result<Option<Option<T>>> {
        self.ticks(row).map(|ticks| ticks.map(f).transpose()).transpose()
    }
}

pub(super) struct BinStride<'a> {
    column: Option<&'a Column>,
    constant: Option<IntervalMonthDayNano>,
    pub(super) unit: TimeUnit,
}

impl<'a> BinStride<'a> {
    pub(super) fn bind(expr: &'a Expr, schema: &'a Schema, batch: &'a ColumnBatch, params: &'a BoundParameters) -> Option<Self> {
        let unit = |ty: &LogicalType| match plain_scalar(ty)? {
            ScalarType::IntervalMonthDayMicro => Some(TimeUnit::Microsecond),
            ScalarType::Interval(IntervalUnit::MonthDayNano) => Some(TimeUnit::Nanosecond),
            _ => None,
        };
        match expr {
            Expr::Literal(value) => {
                let (constant, unit) = match value {
                    Value::IntervalMonthDayMicro { .. } => (Self::value(value, TimeUnit::Microsecond)?, TimeUnit::Microsecond),
                    Value::IntervalMonthDayNano { .. } => (Self::value(value, TimeUnit::Nanosecond)?, TimeUnit::Nanosecond),
                    _ => return None,
                };
                Some(Self { column: None, constant, unit })
            }
            Expr::Column { id, .. } => {
                let position = schema.position(*id)?;
                let unit = unit(&schema.fields()[position].data_type)?;
                let column = batch.columns.get(position)?;
                if matches!(column, Column::Values(_)) { return None; }
                Some(Self { column: Some(column), constant: None, unit })
            }
            Expr::Parameter { index, data_type } => {
                let stride_unit = unit(params.types().get(*index)?)?;
                if unit(data_type) != Some(stride_unit) { return None; }
                let parameters = params.evaluator();
                let constant = match parameters.arrow_cell(*index) {
                    Some(cell) => Self::arrow(stride_unit, cell.array, cell.field, None, cell.row)?,
                    None => Self::value(parameters.known(*index)?, stride_unit)?,
                };
                Some(Self { column: None, constant, unit: stride_unit })
            }
            _ => None,
        }
    }

    fn value(value: &Value, unit: TimeUnit) -> Option<Option<IntervalMonthDayNano>> {
        match (value, unit) {
            (Value::Null | Value::Undefined, _) => Some(None),
            (Value::IntervalMonthDayMicro { months, days, nanoseconds }, TimeUnit::Microsecond)
            | (Value::IntervalMonthDayNano { months, days, nanoseconds }, TimeUnit::Nanosecond) => {
                Some(Some(IntervalMonthDayNano::new(*months, *days, *nanoseconds)))
            }
            _ => None,
        }
    }

    fn arrow(unit: TimeUnit, array: &dyn Array, field: &ArrowField, logical: Option<&LogicalType>, row: usize)
        -> Option<Option<IntervalMonthDayNano>>
    {
        if array.data_type() != field.data_type() || row >= array.len() { return None; }
        if field.data_type() == &DataType::Null && field.metadata().is_empty() && field.is_nullable()
            && logical.is_none_or(|ty| *ty == LogicalType::Null)
        {
            return Some(None);
        }
        let layout = match unit {
            TimeUnit::Microsecond => field.extension_type_name() == Some("xt/intervalDMMType"),
            TimeUnit::Nanosecond => field.metadata().is_empty(),
            _ => false,
        };
        let ty = if unit == TimeUnit::Microsecond { ScalarType::IntervalMonthDayMicro } else { ScalarType::Interval(IntervalUnit::MonthDayNano) };
        if !layout || field.data_type() != &DataType::Interval(ArrowIntervalUnit::MonthDayNano)
            || logical.is_some_and(|logical| plain_scalar(logical) != Some(&ty))
        {
            return None;
        }
        let array = array.as_any().downcast_ref::<IntervalMonthDayNanoArray>()?;
        Some(array.is_valid(row).then(|| array.value(row)))
    }

    pub(super) fn parts(&self, row: usize) -> Option<Option<IntervalMonthDayNano>> {
        let Some(column) = self.column else { return Some(self.constant); };
        let Some((array, field, logical, row)) = column.arrow_cell(row)? else { return Some(None); };
        Self::arrow(self.unit, array, field, logical, row)
    }
}

struct AgeInput<'a> {
    input: CalendarInput<'a>,
    timestamp_cast: Option<(TimeUnit, Option<&'a str>)>,
}

impl<'a> AgeInput<'a> {
    fn bind(expr: &'a Expr, schema: &'a Schema, batch: &'a ColumnBatch, params: &'a BoundParameters, timezone: &'a str) -> Option<Self> {
        let (expr, timestamp_cast) = match expr {
            Expr::Cast { expr, target: CastTarget::Temporal(target) } if target.precision.is_none() => {
                match expr.as_ref() {
                    Expr::Column { id, .. } if !schema.get(*id)?.data_type.nullable() => {}
                    Expr::Parameter { index, .. } if !params.types().get(*index)?.nullable() => {}
                    Expr::Literal(_) => {}
                    _ => return None,
                }
                let label = match &target.kind {
                    TemporalKind::TimestampLocal => None,
                    TemporalKind::TimestampZone(CastTimezone::Explicit(label)) => Some(label.as_str()),
                    TemporalKind::TimestampZone(CastTimezone::Session) => Some(timezone),
                    _ => return None,
                };
                (expr.as_ref(), Some((target.unit, label)))
            }
            _ => (expr, None),
        };
        Some(Self { input: CalendarInput::bind(expr, schema, batch, params)?, timestamp_cast })
    }

    fn ty(&self) -> CalendarType<'a> {
        self.timestamp_cast.map_or(self.input.ty, |(unit, label)| CalendarType::Timestamp(unit, label))
    }

    fn cast(&self, ticks: i64, timezone: &str) -> Result<i64> {
        match self.timestamp_cast {
            Some((unit, label)) => self.input.ty.cast_timestamp(ticks, unit, label.is_some(), timezone),
            None => Ok(ticks),
        }
    }
}

pub(super) fn project(expr: &Expr, schema: &Schema, batch: &ColumnBatch, params: &BoundParameters, context: &QueryContext)
    -> Result<Option<(Column, Arc<MemoryReservation>)>>
{
    let Expr::Call { name, args, return_type } = expr else { return Ok(None); };
    if batch.len == 0 { return Ok(None); }
    let Some(result) = plain_scalar(return_type) else { return Ok(None); };
    match (name.as_str(), args.as_slice()) {
        ("date_trunc" | "date-trunc", [Expr::Literal(Value::Utf8(field)), source] | [Expr::Literal(Value::Utf8(field)), source, _]) => {
            let Some(source) = CalendarInput::bind(source, schema, batch, params) else { return Ok(None); };
            if CalendarType::of(result) != Some(source.ty) { return Ok(None); }
            let Ok(field) = TemporalField::parse(field) else { return Ok(None); };
            if !field.supports("date_trunc", result) { return Ok(None); }
            let override_zone = match args.get(2) {
                None => None,
                Some(Expr::Literal(Value::Utf8(zone))) if matches!(source.ty, CalendarType::Timestamp(_, Some(_))) => {
                    let Ok(zone) = truncation_zone(zone) else { return Ok(None); };
                    Some(zone)
                }
                _ => return Ok(None),
            };
            match source.ty {
                CalendarType::DateDay => build::<Date32Type>(batch, context, result, |row| {
                    source.map_ticks(row, |ticks| truncate_date32(field, ticks))
                }),
                CalendarType::DateMilli => build::<Date64Type>(batch, context, result, |row| {
                    source.map_ticks(row, |ticks| truncate_date64(field, ticks))
                }),
                CalendarType::Timestamp(unit, label) => {
                    let Ok(zone) = timestamp_truncation_zone(field, label, override_zone) else { return Ok(None); };
                    let cell = |row| source.map_ticks(row, |ticks| truncate_timestamp(field, ticks, unit, zone.as_ref()));
                    match unit {
                        TimeUnit::Second => build::<TimestampSecondType>(batch, context, result, cell),
                        TimeUnit::Millisecond => build::<TimestampMillisecondType>(batch, context, result, cell),
                        TimeUnit::Microsecond => build::<TimestampMicrosecondType>(batch, context, result, cell),
                        TimeUnit::Nanosecond => build::<TimestampNanosecondType>(batch, context, result, cell),
                    }
                }
            }
        }
        ("date_bin" | "date-bin", [stride, source] | [stride, source, _]) => {
            let Some(stride) = BinStride::bind(stride, schema, batch, params) else { return Ok(None); };
            let Some(source) = CalendarInput::bind(source, schema, batch, params) else { return Ok(None); };
            let CalendarType::Timestamp(source_unit, source_zone) = source.ty else { return Ok(None); };
            let origin = match args.get(2) {
                None => CalendarInput { source: CalendarSource::Constant(Some(0)), ty: CalendarType::Timestamp(TimeUnit::Microsecond, Some("UTC")) },
                Some(origin) => {
                    let Some(origin) = CalendarInput::bind(origin, schema, batch, params) else { return Ok(None); };
                    origin
                }
            };
            let CalendarType::Timestamp(origin_unit, origin_zone) = origin.ty else { return Ok(None); };
            if source_zone.is_some() != origin_zone.is_some() { return Ok(None); }
            let ScalarType::Timestamp { unit, timezone } = result else { return Ok(None); };
            if *unit != FixedDateBin::output_unit(stride.unit, origin_unit) || timezone.as_deref() != origin_zone { return Ok(None); }
            let cell = |row| {
                let (Some(stride_parts), Some(origin)) = (stride.parts(row), origin.ticks(row)) else { return Ok(None); };
                let (Some(parts), Some(origin)) = (stride_parts, origin) else { return Ok(Some(None)); };
                let Some(source) = source.ticks(row) else { return Ok(None); };
                let Some(source) = source else { return Ok(Some(None)); };
                if parts.months != 0 || parts.days != 0 { return Ok(None); }
                let bin = FixedDateBin::new(parts.nanoseconds, stride.unit, source_unit, origin, origin_unit);
                bin.cell(source).map(|value| Some(Some(value)))
            };
            match unit {
                TimeUnit::Microsecond => build::<TimestampMicrosecondType>(batch, context, result, cell),
                TimeUnit::Nanosecond => build::<TimestampNanosecondType>(batch, context, result, cell),
                _ => Ok(None),
            }
        }
        ("age", [end, start]) => {
            let timezone = &context.timezone;
            let (Some(end), Some(start)) = (AgeInput::bind(end, schema, batch, params, timezone), AgeInput::bind(start, schema, batch, params, timezone))
                else { return Ok(None); };
            let inputs = [end.ty(), start.ty()];
            let expected = if inputs.iter().any(|ty| matches!(ty, CalendarType::Timestamp(TimeUnit::Nanosecond, _))) {
                ScalarType::Interval(IntervalUnit::MonthDayNano)
            } else { ScalarType::IntervalMonthDayMicro };
            if *result != expected { return Ok(None); }
            let needs_zone = inputs.iter().any(|ty| matches!(ty, CalendarType::Timestamp(_, Some(_))));
            let Ok(zone) = needs_zone.then(|| ZoneId::parse(timezone)).transpose() else { return Ok(None); };
            build::<IntervalMonthDayNanoType>(batch, context, result, |row| {
                let Some(end_ticks) = end.input.ticks(row) else { return Ok(None); };
                let Some(end_ticks) = end_ticks else { return Ok(Some(None)); };
                let Some(start_ticks) = start.input.ticks(row) else { return Ok(None); };
                let Some(start_ticks) = start_ticks else { return Ok(Some(None)); };
                let end_ticks = end.cast(end_ticks, timezone)?;
                let start_ticks = start.cast(start_ticks, timezone)?;
                let (months, days, nanos) = calendar_age(end.ty().age_operand(end_ticks, zone.as_ref()), start.ty().age_operand(start_ticks, zone.as_ref()))?;
                Ok(Some(Some(IntervalMonthDayNano::new(months, days, nanos))))
            })
        }
        _ => Ok(None),
    }
}

fn build<T: ArrowPrimitiveType>(batch: &ColumnBatch, context: &QueryContext, scalar: &ScalarType,
    mut cell: impl FnMut(usize) -> Result<Option<Option<T::Native>>>) -> Result<Option<(Column, Arc<MemoryReservation>)>>
{
    let zone_bytes = match scalar {
        ScalarType::Timestamp { timezone: Some(zone), .. } => zone.len().checked_mul(4).ok_or_else(size_error)?,
        _ => 0,
    };
    let bytes = batch.len.checked_mul(size_of::<T::Native>() + 1)
        .and_then(|bytes| bytes.checked_add(zone_bytes)?.checked_add(4096)).ok_or_else(size_error)?;
    let mut owner = match context.memory.reserve(bytes) {
        Ok(owner) => owner,
        Err(error) if error.is(AnomalyKind::ResourceExhausted) => return Ok(None),
        Err(error) => return Err(error),
    };
    let logical = LogicalType::Scalar(scalar.clone());
    let base_field = rtdb_columnar::to_field("value", &logical)?;
    let mut out = PrimitiveBuilder::<T>::with_capacity(batch.len).with_data_type(base_field.data_type().clone());
    for row in 0..batch.len {
        context.check_cancelled_at(row)?;
        let Some(value) = cell(row)? else { return Ok(None); };
        out.append_option(value);
    }
    let array = out.finish();
    let (logical, field) = if array.null_count() != 0 { (logical.maybe(), base_field.with_nullable(true)) } else { (logical, base_field) };
    let column = Column::Arrow { array: Arc::new(array), field: Arc::new(field), logical: Some(logical) };
    let retained = owned_projection_bytes(&column)?;
    let lease = Arc::get_mut(&mut owner).expect("Calendar projection owns its reservation");
    if retained <= lease.bytes() { lease.shrink_to(retained)?; } else { lease.grow_to(retained)?; }
    Ok(Some((column, owner)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, Date32Array, Date64Array, IntervalMonthDayNanoArray, RecordBatch, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, types::IntervalMonthDayNano};
    use rtdb_columnar::{MemoryBudget, OwnedBatch};
    use rtdb_plan::{CastTarget, CastTimezone, ColumnId, Expr, Field, Schema, TemporalCast, TemporalKind, function_type_with_args};
    use rtdb_types::{IntervalUnit, LogicalType, ScalarType, TimeUnit, Timestamp, Value};

    use super::super::{BatchCursor, BatchExpressions, Column, ColumnBatch, SelectedColumn, owned_projection_bytes, plain_scalar, single_batch};
    use crate::{CatalogSnapshot, DatabaseSnapshot, QueryContext, context::{Execution, OuterRows}, parameters::BoundParameters};

    fn execution(timezone: &str) -> Execution {
        let memory = MemoryBudget::new(1 << 20);
        let database = DatabaseSnapshot {
            partitions: vec![],
            storage: Arc::new(rtdb_storage::cache::BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
        };
        let catalog = CatalogSnapshot::new("xtdb", [("xtdb".into(), database)].into(), false).unwrap();
        let context = QueryContext::new(catalog, Arc::clone(&memory), Timestamp(0), timezone,
            Default::default(), &tokio_util::sync::CancellationToken::new()).unwrap();
        Execution { context, parameters: Arc::new(BoundParameters::infer(vec![], &memory).unwrap()),
            outer: OuterRows::empty(&memory).unwrap(), bindings: None, analysis: None }
    }

    fn timestamp(value: i64, unit: TimeUnit, zone: Option<&str>) -> Value {
        Value::Timestamp { value, unit, timezone: zone.map(str::to_owned) }
    }

    fn field(id: u32, unit: TimeUnit, zone: Option<&str>) -> Field {
        Field { id: ColumnId(id), name: format!("ts{id}"), qualifier: None, qualified_only: false,
            unnamed: false, data_type: timestamp(0, unit, zone).logical_type().maybe() }
    }

    fn call(name: &str, args: Vec<Expr>) -> Expr {
        let return_type = function_type_with_args(name, &args).unwrap();
        Expr::Call { name: name.into(), args, return_type }
    }

    fn arrow(array: impl Array + 'static, field: &Field) -> Column {
        Column::Arrow { array: Arc::new(array), field: Arc::new(rtdb_columnar::to_field(&field.name, &field.data_type).unwrap()),
            logical: Some(field.data_type.clone()) }
    }

    fn dataset(columns: Vec<Column>, len: usize) -> ColumnBatch {
        ColumnBatch::settled(columns, len, MemoryBudget::new(1 << 20).reserve(0).unwrap()).unwrap()
    }

    fn cursor(execution: Execution, schema: Schema, batch: ColumnBatch, expressions: Vec<Expr>) -> BatchExpressions {
        let input = single_batch(Some(batch), Arc::clone(&execution.context));
        let mut cursor = BatchExpressions::new(input, schema, expressions, execution);
        cursor.scalar_fallbacks = Some(0);
        cursor
    }

    async fn assert_parity(execution: Execution, schema: Schema, batch: ColumnBatch, calls: Vec<Expr>, types: Vec<ScalarType>) {
        let timezone = &execution.context.timezone;
        let context = rtdb_expr::EvalContext { timezone, parameters: execution.parameters.evaluator(),
            parameter_types: Some(execution.parameters.types()), ..Default::default() };
        let expected: Vec<Vec<Value>> = (0..batch.len).map(|row| {
            let values = batch.columns.iter().map(|column| column.value(row).unwrap()).collect::<Vec<_>>();
            calls.iter().map(|expr| rtdb_expr::evaluate_projection(expr, &schema, &values, &context).unwrap()).collect()
        }).collect();
        let mut cursor = cursor(execution.clone(), schema, batch, calls);
        let baseline = execution.context.memory.used_bytes();
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(cursor.scalar_fallbacks, Some(0));
        for (index, (column, ty)) in page.columns.iter().zip(types).enumerate() {
            let Column::Arrow { array, field, logical: Some(logical) } = column else { panic!("Expected typed calendar output"); };
            assert_eq!(plain_scalar(logical), Some(&ty));
            assert_eq!(field.as_ref(), &rtdb_columnar::to_field("value", logical).unwrap());
            assert_eq!(array.data_type(), field.data_type());
            assert!(page.column_reservation(index).bytes() >= owned_projection_bytes(column).unwrap());
            for (row, expected) in expected.iter().enumerate() { assert_eq!(column.value(row).unwrap(), expected[index]); }
        }
        drop(page);
        assert_eq!(execution.context.memory.used_bytes(), baseline);
    }

    #[tokio::test]
    async fn date_trunc_preserves_date_width_and_timestamp_precision() {
        use TimeUnit::{Millisecond, Nanosecond};
        let day = Field { data_type: LogicalType::Scalar(ScalarType::DateDay).maybe(), ..field(0, Millisecond, None) };
        let millis = Field { data_type: LogicalType::Scalar(ScalarType::DateMilli).maybe(), ..field(1, Millisecond, None) };
        let local = field(2, Millisecond, None);
        let zoned = field(3, Nanosecond, Some("+05:30"));
        let schema = Schema::new(vec![day.clone(), millis.clone(), local.clone(), zoned.clone()]).unwrap();
        let batch = dataset(vec![
            arrow(Date32Array::from(vec![Some(-1), None, Some(7)]), &day),
            arrow(Date64Array::from(vec![Some(-1), Some(86_400_001), None]), &millis),
            arrow(TimestampMillisecondArray::from(vec![Some(-1), None, Some(1001)]), &local),
            arrow(TimestampNanosecondArray::from(vec![Some(-1), Some(1001), None]).with_timezone("+05:30"), &zoned),
        ], 3);
        let calls = [("WEEK", &day), ("DAY", &millis), ("SECOND", &local), ("MICROSECOND", &zoned)].map(|(unit, source)| {
            call("date_trunc", vec![Expr::Literal(Value::Utf8(unit.into())), source.expression()])
        });
        let types = schema.fields().iter().map(|field| plain_scalar(&field.data_type).unwrap().clone()).collect();
        assert_parity(execution("Asia/Tokyo"), schema, batch, calls.into(), types).await;
    }

    #[tokio::test]
    async fn date_trunc_uses_the_label_or_override_across_dst() {
        let ts = field(0, TimeUnit::Microsecond, Some("America/New_York"));
        let schema = Schema::new(vec![ts.clone()]).unwrap();
        let array: Arc<dyn Array> = Arc::new(TimestampMicrosecondArray::from(vec![
            1_636_263_000_000_123, 1_636_266_600_000_123, 1_633_189_500_000_123,
        ]).with_timezone("America/New_York"));
        let field = Arc::new(rtdb_columnar::to_field(&ts.name, &ts.data_type).unwrap());
        let picks = Arc::new(vec![(0, 1), (0, 0), (1, 0), (0, 2)]);
        let batch = dataset(vec![Column::Selected(SelectedColumn::new(
            vec![Some((array, field, Some(ts.data_type.clone()), None)), None], picks,
        ))], 4);
        let unit = Expr::Literal(Value::Utf8("HOUR".into()));
        let calls = vec![
            call("date_trunc", vec![unit.clone(), ts.expression()]),
            call("date-trunc", vec![unit, ts.expression(), Expr::Literal(Value::Utf8("Australia/Lord_Howe".into()))]),
        ];
        let mut cursor = cursor(execution("Asia/Tokyo"), schema, batch, calls);
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(cursor.scalar_fallbacks, Some(0));
        let zoned = |ticks| timestamp(ticks, TimeUnit::Microsecond, Some("America/New_York"));
        assert_eq!(page.columns[0].value(0).unwrap(), zoned(1_636_264_800_000_000));
        assert_eq!(page.columns[0].value(1).unwrap(), zoned(1_636_261_200_000_000));
        assert_eq!(page.columns[0].value(2).unwrap(), Value::Null);
        assert_eq!(page.columns[1].value(3).unwrap(), zoned(1_633_188_600_000_000));
    }

    #[tokio::test]
    async fn date_trunc_reports_the_first_reached_error_after_nulls() {
        let ts = field(0, TimeUnit::Second, Some("+05:30"));
        let schema = Schema::new(vec![ts.clone()]).unwrap();
        let trunc = call("date_trunc", vec![Expr::Literal(Value::Utf8("DAY".into())), ts.expression()]);
        for (values, code) in [([i64::MAX, i64::MIN], "xtdb.expression/overflow-error"), ([i64::MIN, i64::MAX], "xtdb/invalid-date")] {
            let array = TimestampSecondArray::from(vec![None, Some(values[0]), Some(values[1])]).with_timezone("+05:30");
            let batch = dataset(vec![arrow(array, &ts)], 3);
            let mut cursor = cursor(execution("UTC"), schema.clone(), batch, vec![trunc.clone()]);
            let error = cursor.next_batch().await.err().expect("A reached DATE_TRUNC row fails");
            assert_eq!(error.code, code);
            assert_eq!(cursor.scalar_fallbacks, Some(0));
        }
    }

    #[tokio::test]
    async fn date_trunc_date64_rejects_a_midnight_outside_arrow() {
        let date = Field { data_type: LogicalType::Scalar(ScalarType::DateMilli).maybe(), ..field(0, TimeUnit::Millisecond, None) };
        let schema = Schema::new(vec![date.clone()]).unwrap();
        let batch = dataset(vec![arrow(Date64Array::from(vec![None, Some(i64::MIN)]), &date)], 2);
        let trunc = call("date_trunc", vec![Expr::Literal(Value::Utf8("DAY".into())), date.expression()]);
        let scalar = rtdb_expr::evaluate_projection(&trunc, &schema, &[Value::DateMilli(i64::MIN)], &Default::default()).unwrap_err();
        let mut cursor = cursor(execution("UTC"), schema, batch, vec![trunc]);
        let error = cursor.next_batch().await.err().expect("Midnight underflows Date64");
        assert_eq!(error.code, scalar.code);
        assert_eq!(cursor.scalar_fallbacks, Some(0));
    }

    #[tokio::test]
    async fn date_trunc_date32_rejects_a_week_start_outside_arrow() {
        let date = Field { data_type: LogicalType::Scalar(ScalarType::DateDay).maybe(), ..field(0, TimeUnit::Millisecond, None) };
        let schema = Schema::new(vec![date.clone()]).unwrap();
        let batch = dataset(vec![arrow(Date32Array::from(vec![None, Some(i32::MIN)]), &date)], 2);
        let trunc = call("date_trunc", vec![Expr::Literal(Value::Utf8("WEEK".into())), date.expression()]);
        let mut cursor = cursor(execution("UTC"), schema, batch, vec![trunc]);
        let error = cursor.next_batch().await.err().expect("Week start underflows Date32");
        assert_eq!(error.code, "xtdb.expression/overflow-error");
        assert_eq!(cursor.scalar_fallbacks, Some(0));
    }

    #[tokio::test]
    async fn date_trunc_resolves_a_stored_zone_only_for_reached_whole_second_fields() {
        let label = "Not/A_Zone";
        let ts = field(0, TimeUnit::Microsecond, Some(label));
        let schema = Schema::new(vec![ts.clone()]).unwrap();
        let batch = |ticks| dataset(vec![arrow(TimestampMicrosecondArray::from(vec![ticks]).with_timezone(label), &ts)], 1);
        let trunc = |unit: &str| call("date_trunc", vec![Expr::Literal(Value::Utf8(unit.into())), ts.expression()]);
        let ty = plain_scalar(&ts.data_type).unwrap().clone();
        for ticks in [Some(1234), None] {
            assert_parity(execution("UTC"), schema.clone(), batch(ticks), vec![trunc("MILLISECOND")], vec![ty.clone()]).await;
        }

        let mut nulls = cursor(execution("UTC"), schema.clone(), batch(None), vec![trunc("SECOND")]);
        assert_eq!(nulls.next_batch().await.unwrap().unwrap().columns[0].value(0).unwrap(), Value::Null);
        assert_eq!(nulls.scalar_fallbacks, Some(1));

        let mut reached = cursor(execution("UTC"), schema, batch(Some(1234)), vec![trunc("SECOND")]);
        let error = reached.next_batch().await.err().expect("A reached SECOND truncation resolves the zone");
        assert_eq!(error.code, "xtdb/invalid-timezone");
        assert_eq!(reached.scalar_fallbacks, Some(1));
    }

    #[tokio::test]
    async fn calendar_projection_date_bin_preserves_origin_labels_units_and_negative_quotients() {
        use TimeUnit::{Microsecond, Millisecond, Nanosecond};
        let ts = field(0, Millisecond, Some("Europe/London"));
        let schema = Schema::new(vec![ts.clone()]).unwrap();
        let batch = dataset(vec![arrow(TimestampMillisecondArray::from(vec![Some(-1_001), Some(-999), None, Some(1_001)])
            .with_timezone("Europe/London"), &ts)], 4);
        let stride = Value::IntervalMonthDayMicro { months: 0, days: 0, nanoseconds: 1_000_000_000 };
        let nano_stride = Value::IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 500_700_000 };
        let zone = "America/New_York";
        let calls = vec![
            call("date_bin", vec![Expr::Literal(stride), ts.expression()]),
            call("date_bin", vec![Expr::Literal(nano_stride), ts.expression(), Expr::Literal(timestamp(7, Nanosecond, Some(zone)))]),
        ];
        let types = vec![ScalarType::Timestamp { unit: Microsecond, timezone: Some("UTC".into()) },
            ScalarType::Timestamp { unit: Nanosecond, timezone: Some(zone.into()) }];
        assert_parity(execution("Asia/Tokyo"), schema, batch, calls, types).await;
    }

    #[tokio::test]
    async fn date_bin_reads_per_row_stride_and_origin() {
        use TimeUnit::{Microsecond, Millisecond, Nanosecond};
        let source = field(0, Millisecond, Some("Europe/London"));
        let origin = field(1, Nanosecond, Some("America/New_York"));
        let stride = Field { data_type: LogicalType::Scalar(ScalarType::IntervalMonthDayMicro).maybe(), ..field(2, Microsecond, None) };
        let schema = Schema::new(vec![source.clone(), origin.clone(), stride.clone()]).unwrap();
        let fixed = |nanos| Some(IntervalMonthDayNano::new(0, 0, nanos));
        let source_field = rtdb_columnar::to_field(&source.name, &source.data_type).unwrap();
        let source_label = "Europe/London";
        let mixed = SelectedColumn::new(vec![
            Some((Arc::new(TimestampMillisecondArray::from(vec![-1001, 2001]).with_timezone(source_label)),
                Arc::new(source_field.clone()), Some(source.data_type.clone()), None)),
            Some((Arc::new(TimestampMillisecondArray::from(vec![i64::MAX]).with_timezone(source_label)),
                Arc::new(source_field.with_metadata([("source".into(), "external".into())].into())), Some(source.data_type.clone()), None)),
        ], Arc::new(vec![(0, 0), (0, 1), (1, 0), (1, 0)]));
        let batch = dataset(vec![
            Column::Selected(mixed),
            arrow(TimestampNanosecondArray::from(vec![Some(7), Some(-19), None, Some(99)])
                .with_timezone("America/New_York"), &origin),
            arrow(IntervalMonthDayNanoArray::from(vec![fixed(1_000_000_000), fixed(500_000_000), fixed(0), None]), &stride),
        ], 4);
        let bin = call("date_bin", vec![stride.expression(), source.expression(), origin.expression()]);
        assert_parity(execution("Asia/Tokyo"), schema, batch, vec![bin],
            vec![ScalarType::Timestamp { unit: Nanosecond, timezone: Some("America/New_York".into()) }]).await;
    }

    #[tokio::test]
    async fn date_bin_accepts_a_bound_stride_and_origin() {
        let source = field(0, TimeUnit::Millisecond, None);
        let schema = Schema::new(vec![source.clone()]).unwrap();
        let batch = dataset(vec![arrow(TimestampMillisecondArray::from(vec![Some(-1001), Some(2001), None]), &source)], 3);
        let mut execution = execution("Asia/Tokyo");
        let stride = Value::IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 500_000_000 };
        let origin = timestamp(7, TimeUnit::Nanosecond, None);
        let parameter = |index, value: &Value| Expr::Parameter { index, data_type: value.logical_type() };
        let bin = call("date_bin", vec![parameter(0, &stride), source.expression(), parameter(1, &origin)]);
        execution.parameters = Arc::new(BoundParameters::infer(vec![stride, origin], &execution.context.memory).unwrap());
        assert_parity(execution, schema, batch, vec![bin], vec![ScalarType::Timestamp { unit: TimeUnit::Nanosecond, timezone: None }]).await;
    }

    #[tokio::test]
    async fn date_bin_uses_scalar_arithmetic_for_reached_calendar_strides() {
        let ts = field(0, TimeUnit::Microsecond, Some("America/New_York"));
        let stride = Field { data_type: LogicalType::Scalar(ScalarType::Interval(IntervalUnit::MonthDayNano)), ..field(1, TimeUnit::Microsecond, None) };
        let schema = Schema::new(vec![ts.clone(), stride.clone()]).unwrap();
        let batch = dataset(vec![
            arrow(TimestampMicrosecondArray::from(vec![86_400_000_000i64]).with_timezone("America/New_York"), &ts),
            arrow(IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNano::new(0, 1, 0)]), &stride),
        ], 1);
        let bin = call("date_bin", vec![stride.expression(), ts.expression()]);
        let values = batch.columns.iter().map(|column| column.value(0).unwrap()).collect::<Vec<_>>();
        let expected = rtdb_expr::evaluate_projection(&bin, &schema, &values, &Default::default()).unwrap();
        let mut cursor = cursor(execution("UTC"), schema, batch, vec![bin]);
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(page.columns[0].value(0).unwrap(), expected);
        assert_eq!(cursor.scalar_fallbacks, Some(1));
    }

    #[tokio::test]
    async fn calendar_projection_age_preserves_precision_and_session_zone_across_dst() {
        use TimeUnit::{Microsecond, Nanosecond, Second};
        let end = field(0, Microsecond, Some("UTC"));
        let start = field(1, Second, Some("Europe/Paris"));
        let schema = Schema::new(vec![end.clone(), start.clone()]).unwrap();
        let array: Arc<dyn Array> = Arc::new(TimestampMicrosecondArray::from(vec![
            1_636_264_200_000_000, 1_636_267_800_000_000, 1_615_703_400_000_000, 1_615_707_000_000_000, i64::MIN,
        ]).with_timezone("UTC"));
        let selected = SelectedColumn::new(vec![Some((array, Arc::new(rtdb_columnar::to_field("ts0", &end.data_type).unwrap()),
            Some(end.data_type.clone()), None)), None], Arc::new(vec![(0, 1), (0, 0), (0, 3), (0, 2), (0, 4), (1, 0)]));
        let starts = TimestampSecondArray::from(vec![Some(1_636_264_200), Some(1_636_267_800), Some(1_615_703_400),
            Some(1_615_707_000), None, Some(0)]).with_timezone("Europe/Paris");
        let batch = dataset(vec![Column::Selected(selected), arrow(starts, &start)], 6);
        let nano = Expr::Literal(timestamp(1_636_264_200_000_000_123, Nanosecond, None));
        let calls = vec![call("age", vec![end.expression(), start.expression()]), call("age", vec![nano, start.expression()])];
        assert_parity(execution("America/New_York"), schema, batch, calls,
            vec![ScalarType::IntervalMonthDayMicro, ScalarType::Interval(IntervalUnit::MonthDayNano)]).await;
    }

    #[tokio::test]
    async fn age_reads_an_arrow_parameter_and_casts_in_the_session_zone() {
        let end = Field { data_type: timestamp(0, TimeUnit::Microsecond, None).logical_type(), ..field(0, TimeUnit::Microsecond, None) };
        let start = field(1, TimeUnit::Microsecond, Some("Asia/Tokyo"));
        let schema = Schema::new(vec![end.clone()]).unwrap();
        let batch = dataset(vec![arrow(TimestampMicrosecondArray::from(vec![1_636_253_400_000_017i64]), &end)], 1);
        let mut execution = execution("America/New_York");
        let physical = arrow_schema::Schema::new(vec![rtdb_columnar::to_field(&start.name, &start.data_type).unwrap()]);
        let argument = Arc::new(TimestampMicrosecondArray::from(vec![1_636_263_000_000_000i64]).with_timezone("Asia/Tokyo"));
        let record = RecordBatch::try_new(Arc::new(physical), vec![argument]).unwrap();
        let memory = &execution.context.memory;
        execution.parameters = Arc::new(BoundParameters::from_arrow(OwnedBatch::new(record, memory).unwrap(),
            0, vec![start.data_type.clone()], memory).unwrap());
        let cast = Expr::Cast { expr: Box::new(end.expression()), target: CastTarget::Temporal(TemporalCast {
            kind: TemporalKind::TimestampZone(CastTimezone::Explicit("Europe/Paris".into())), unit: TimeUnit::Microsecond, precision: None,
        }) };
        let input = Expr::Parameter { index: 0, data_type: start.data_type.clone() };
        let age = call("age", vec![cast, input.clone()]);
        let trunc = call("date_trunc", vec![Expr::Literal(Value::Utf8("MICROSECOND".into())), input]);
        let mut cursor = cursor(execution.clone(), schema.clone(), batch, vec![age.clone(), trunc]);
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(cursor.scalar_fallbacks, Some(0));
        assert!(execution.parameters.evaluator().known(0).is_none());
        let context = rtdb_expr::EvalContext { timezone: &execution.context.timezone, parameters: execution.parameters.evaluator(),
            parameter_types: Some(execution.parameters.types()), ..Default::default() };
        let expected = rtdb_expr::evaluate_projection(&age, &schema, &[timestamp(1_636_253_400_000_017, TimeUnit::Microsecond, None)], &context).unwrap();
        assert_eq!(page.columns[0].value(0).unwrap(), expected);
        assert_eq!(page.columns[1].value(0).unwrap(), timestamp(1_636_263_000_000_000, TimeUnit::Microsecond, Some("Asia/Tokyo")));
    }

    #[tokio::test]
    async fn age_does_not_demand_a_timestamp_cast_when_the_other_argument_is_null() {
        let end = Field { data_type: timestamp(0, TimeUnit::Second, None).logical_type(), ..field(0, TimeUnit::Second, None) };
        let start = field(1, TimeUnit::Second, None);
        let schema = Schema::new(vec![end.clone(), start.clone()]).unwrap();
        let age = |end: &Field| {
            let cast = Expr::Cast { expr: Box::new(end.expression()), target: CastTarget::Temporal(TemporalCast {
                kind: TemporalKind::TimestampLocal, unit: TimeUnit::Nanosecond, precision: None,
            }) };
            call("age", vec![cast, start.expression()])
        };
        let batch = |end: &Field, start_ticks| dataset(vec![
            arrow(TimestampSecondArray::from(vec![i64::MAX]), end),
            arrow(TimestampSecondArray::from(vec![start_ticks]), &start),
        ], 1);
        let direct = age(&end);
        let oracle = |start| rtdb_expr::evaluate_projection(&direct, &schema,
            &[timestamp(i64::MAX, TimeUnit::Second, None), start], &Default::default());
        let mut skipped = cursor(execution("UTC"), schema.clone(), batch(&end, None), vec![direct.clone()]);
        assert_eq!(skipped.next_batch().await.unwrap().unwrap().columns[0].value(0).unwrap(), Value::Null);
        assert_eq!(oracle(Value::Null).unwrap(), Value::Null);
        assert_eq!(skipped.scalar_fallbacks, Some(0));
        let mut reached = cursor(execution("UTC"), schema.clone(), batch(&end, Some(0)), vec![direct.clone()]);
        let expected = oracle(timestamp(0, TimeUnit::Second, None)).unwrap_err();
        assert_eq!(reached.next_batch().await.err().unwrap().code, expected.code);
        assert_eq!(reached.scalar_fallbacks, Some(0));

        let nullable = Field { data_type: end.data_type.clone().maybe(), ..end.clone() };
        let schema = Schema::new(vec![nullable.clone(), start.clone()]).unwrap();
        let age = age(&nullable);
        let values = [timestamp(i64::MAX, TimeUnit::Second, None), Value::Null];
        let expected = rtdb_expr::evaluate_projection(&age, &schema, &values, &Default::default()).unwrap_err();
        let mut fallback = cursor(execution("UTC"), schema, batch(&nullable, None), vec![age]);
        assert_eq!(fallback.next_batch().await.err().unwrap().code, expected.code);
        assert_eq!(fallback.scalar_fallbacks, Some(1));
    }

    #[tokio::test]
    async fn calendar_projection_age_reads_date_millis_as_epoch_days() {
        let day = Field { data_type: LogicalType::Scalar(ScalarType::DateMilli).maybe(), ..field(0, TimeUnit::Microsecond, None) };
        let schema = Schema::new(vec![day.clone()]).unwrap();
        let batch = dataset(vec![arrow(Date64Array::from(vec![Some(-1), Some(0), Some(86_400_000), None]), &day)], 4);
        let call = call("age", vec![day.expression(), Expr::Literal(Value::DateDay(0))]);
        assert_parity(execution("America/New_York"), schema, batch, vec![call], vec![ScalarType::IntervalMonthDayMicro]).await;
    }

    #[tokio::test]
    async fn calendar_projection_date_bin_reports_the_first_reached_error() {
        let ts = field(0, TimeUnit::Microsecond, Some("UTC"));
        let schema = Schema::new(vec![ts.clone()]).unwrap();
        let stride = Expr::Literal(Value::IntervalMonthDayMicro { months: 0, days: 0, nanoseconds: 0 });
        let origin = Expr::Literal(timestamp(-1, TimeUnit::Microsecond, Some("UTC")));
        let bin = call("date_bin", vec![stride, ts.expression(), origin]);
        for (values, code) in [([i64::MAX, 0], "xtdb.expression/overflow-error"),
            ([0, i64::MAX], "xtdb.expression/division-by-zero")]
        {
            let batch = dataset(vec![arrow(TimestampMicrosecondArray::from(vec![None, Some(values[0]), Some(values[1])])
                .with_timezone("UTC"), &ts)], 3);
            let mut cursor = cursor(execution("UTC"), schema.clone(), batch, vec![bin.clone()]);
            let error = cursor.next_batch().await.err().expect("A reached DATE_BIN row fails");
            assert_eq!(error.code, code);
            assert_eq!(cursor.scalar_fallbacks, Some(0));
        }
    }
}
