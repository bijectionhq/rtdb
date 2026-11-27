use std::sync::Arc;

use arrow_array::{Array, Date32Array, Date64Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, builder::PrimitiveBuilder, types::{ArrowPrimitiveType, IntervalMonthDayNano,
        IntervalMonthDayNanoType, TimestampMicrosecondType, TimestampNanosecondType}};
use arrow_schema::DataType;
use rtdb_columnar::MemoryReservation;
use rtdb_expr::kernels::{AgeOperand, FixedDateBin, calendar_age};
use rtdb_plan::{Expr, Schema};
use rtdb_types::{AnomalyKind, IntervalUnit, LogicalType, Result, ScalarType, TimeUnit, Value, ZoneId, java_date::MILLIS_PER_DAY};

use super::{Column, ColumnBatch, owned_projection_bytes, plain_scalar, size_error};
use crate::QueryContext;

enum CalendarSource<'a> {
    Column(&'a Column),
    Literal(i64),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CalendarType<'a> {
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
}

struct CalendarInput<'a> {
    source: CalendarSource<'a>,
    ty: CalendarType<'a>,
}

impl<'a> CalendarInput<'a> {
    fn bind(expr: &'a Expr, schema: &'a Schema, batch: &'a ColumnBatch) -> Option<Self> {
        let (source, ty) = match expr {
            Expr::Literal(Value::Timestamp { value, unit, timezone }) =>
                (CalendarSource::Literal(*value), CalendarType::Timestamp(*unit, timezone.as_deref())),
            Expr::Literal(Value::DateDay(days)) => (CalendarSource::Literal(i64::from(*days)), CalendarType::DateDay),
            Expr::Literal(Value::DateMilli(ms)) => (CalendarSource::Literal(*ms), CalendarType::DateMilli),
            Expr::Column { id, .. } => {
                let position = schema.position(*id)?;
                let ty = CalendarType::of(plain_scalar(&schema.fields()[position].data_type)?)?;
                let column = &batch.columns[position];
                if matches!(column, Column::Values(_)) { return None; }
                (CalendarSource::Column(column), ty)
            }
            _ => return None,
        };
        Some(Self { source, ty })
    }

    fn ticks(&self, row: usize) -> Option<Option<i64>> {
        let column = match self.source {
            CalendarSource::Literal(value) => return Some(Some(value)),
            CalendarSource::Column(column) => column,
        };
        let Some((array, field, logical, row)) = column.arrow_cell(row)? else { return Some(None); };
        if array.data_type() != field.data_type() || row >= array.len() || !field.metadata().is_empty() { return None; }
        if array.data_type() == &DataType::Null && field.is_nullable() && logical.is_none_or(|ty| *ty == LogicalType::Null) {
            return Some(None);
        }
        match (logical, self.ty, array.data_type()) {
            (Some(logical), ty, _) if plain_scalar(logical).and_then(CalendarType::of) == Some(ty) => {}
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
        match self.ty {
            CalendarType::DateDay => read!(Date32Array),
            CalendarType::DateMilli => read!(Date64Array),
            CalendarType::Timestamp(TimeUnit::Second, _) => read!(TimestampSecondArray),
            CalendarType::Timestamp(TimeUnit::Millisecond, _) => read!(TimestampMillisecondArray),
            CalendarType::Timestamp(TimeUnit::Microsecond, _) => read!(TimestampMicrosecondArray),
            CalendarType::Timestamp(TimeUnit::Nanosecond, _) => read!(TimestampNanosecondArray),
        }
    }
}

pub(super) fn project(expr: &Expr, schema: &Schema, batch: &ColumnBatch, context: &QueryContext)
    -> Result<Option<(Column, Arc<MemoryReservation>)>>
{
    let Expr::Call { name, args, return_type } = expr else { return Ok(None); };
    if batch.len == 0 { return Ok(None); }
    let Some(result) = plain_scalar(return_type) else { return Ok(None); };
    match (name.as_str(), args.as_slice()) {
        ("date_bin" | "date-bin", [Expr::Literal(stride), source] | [Expr::Literal(stride), source, _]) => {
            let Some(source) = CalendarInput::bind(source, schema, batch) else { return Ok(None); };
            let CalendarType::Timestamp(source_unit, source_zone) = source.ty else { return Ok(None); };
            let (origin, origin_unit, origin_zone) = match args.get(2) {
                None => (0, TimeUnit::Microsecond, Some("UTC")),
                Some(Expr::Literal(Value::Timestamp { value, unit, timezone })) => (*value, *unit, timezone.as_deref()),
                _ => return Ok(None),
            };
            if source_zone.is_some() != origin_zone.is_some() { return Ok(None); }
            let Some(bin) = FixedDateBin::new(stride, source_unit, origin, origin_unit) else { return Ok(None); };
            let ScalarType::Timestamp { unit, timezone } = result else { return Ok(None); };
            if *unit != bin.output_unit() || timezone.as_deref() != origin_zone { return Ok(None); }
            let cell = |row| source.ticks(row).map(|ticks| ticks.map(|ticks| bin.cell(ticks)).transpose()).transpose();
            match unit {
                TimeUnit::Microsecond => build::<TimestampMicrosecondType>(batch, context, result, cell),
                TimeUnit::Nanosecond => build::<TimestampNanosecondType>(batch, context, result, cell),
                _ => Ok(None),
            }
        }
        ("age", [end, start]) => {
            let (Some(end), Some(start)) = (CalendarInput::bind(end, schema, batch), CalendarInput::bind(start, schema, batch))
                else { return Ok(None); };
            let inputs = [end.ty, start.ty];
            let expected = if inputs.iter().any(|ty| matches!(ty, CalendarType::Timestamp(TimeUnit::Nanosecond, _))) {
                ScalarType::Interval(IntervalUnit::MonthDayNano)
            } else { ScalarType::IntervalMonthDayMicro };
            if *result != expected { return Ok(None); }
            let needs_zone = inputs.iter().any(|ty| matches!(ty, CalendarType::Timestamp(_, Some(_))));
            let Ok(zone) = needs_zone.then(|| ZoneId::parse(&context.timezone)).transpose() else { return Ok(None); };
            build::<IntervalMonthDayNanoType>(batch, context, result, |row| {
                let (Some(end_ticks), Some(start_ticks)) = (end.ticks(row), start.ticks(row)) else { return Ok(None); };
                let value = match (end_ticks, start_ticks) {
                    (Some(a), Some(b)) => {
                        let (months, days, nanos) = calendar_age(end.ty.age_operand(a, zone.as_ref()), start.ty.age_operand(b, zone.as_ref()))?;
                        Some(IntervalMonthDayNano::new(months, days, nanos))
                    }
                    _ => None,
                };
                Ok(Some(value))
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

    use arrow_array::{Array, Date64Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray};
    use rtdb_columnar::MemoryBudget;
    use rtdb_plan::{ColumnId, Expr, Field, Schema, function_type_with_args};
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
        let context = rtdb_expr::EvalContext { timezone, ..Default::default() };
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
