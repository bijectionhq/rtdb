use std::{cmp::Ordering, collections::{HashMap, HashSet}, mem::size_of, sync::Arc};

use arrow_array::{Array, cast::AsArray, types::*};
use arrow_schema::{DataType, Field as ArrowField, TimeUnit as ArrowTimeUnit};
use async_trait::async_trait;
use rtdb_columnar::{ArrowCell, CellReadCheck, MemoryReservation};
use rtdb_expr::{cast, compare_in_timezone, is_true, number, sql_eq_in_timezone};
use rtdb_plan::{AggregateExpr, AggregateFunction, Binder, NamedExpr, PercentileFraction, PercentileKind, Schema};
use rtdb_types::{LogicalType, Result, ScalarType, TimeUnit, Value};

use crate::context::Execution;
use crate::columnar::{BatchCursor, Column, ColumnBatch, PredicateSelection};
use crate::cursor::OwnedRow;
use crate::runtime_schema::{EmissionMode, EmittedRelation};
use crate::set_keys::{BatchKeyIndex, KeyColumns};
use crate::sort::NativeKeyKind;
use crate::invalid;

#[derive(Clone, Copy)]
struct Rows<'a> { batch: &'a ColumnBatch, projected: bool, memory: &'a Arc<rtdb_columnar::MemoryBudget> }

enum AggregateCell<'a> {
    Borrowed(&'a Value),
    Read { value: Value, _reservation: Arc<rtdb_columnar::MemoryReservation> },
}

impl AsRef<Value> for AggregateCell<'_> {
    fn as_ref(&self) -> &Value {
        match self { Self::Borrowed(value) => value, Self::Read { value, .. } => value }
    }
}

impl<'a> Rows<'a> {
    fn columns(batch: &'a ColumnBatch, positions: &[usize], projected: bool, context: &'a crate::QueryContext) -> Result<Self> {
        if batch.len != 0 {
            for &position in positions {
                Self::validate_column(&batch.columns[position], batch.len, context)?;
            }
        }
        Ok(Self { batch, projected, memory: &context.memory })
    }

    fn validate_column(column: &Column, len: usize, context: &crate::QueryContext) -> Result<()> {
        type Layout = (*const arrow_schema::Field, Option<*const LogicalType>);
        let mut first: Option<(Layout, CellReadCheck<'_>)> = None;
        let mut reservation: Option<Arc<MemoryReservation>> = None;
        let mut additional: HashMap<Layout, CellReadCheck<'_>> = HashMap::new();
        for row in 0..len {
            context.check_cancelled_at(row)?;
            let Some(Some((array, field, logical, offset))) = column.arrow_cell(row) else { continue; };
            let layout = (std::ptr::from_ref(field), logical.map(std::ptr::from_ref));
            if let Some((known, check)) = &first && *known == layout {
                check.validate(array, offset)?;
                continue;
            }
            if let Some(check) = additional.get(&layout) {
                check.validate(array, offset)?;
                continue;
            }
            let check = {
                let derived;
                let _schema;
                let logical = match logical {
                    Some(logical) => logical,
                    None => {
                        _schema = context.memory.reserve(field.size().checked_mul(8)
                            .ok_or_else(|| invalid("column-size", "Column page exceeds addressable memory"))?)?;
                        derived = rtdb_columnar::from_field(field)?;
                        &derived
                    }
                };
                if logical == &LogicalType::Nothing {
                    return Err(rtdb_types::Anomaly::fault("xtdb/zero-leg-read", "The bottom type has no values to read"));
                }
                CellReadCheck::admit(field, array, logical, offset, &context.memory)?
            };
            if first.is_none() {
                first = Some((layout, check));
            } else {
                let bytes = additional.len().checked_add(1)
                    .and_then(|len| len.checked_mul(4 * (size_of::<(Layout, CellReadCheck<'_>)>() + 1)))
                    .ok_or_else(|| invalid("aggregate-size", "Read check inventory exceeds addressable memory"))?;
                match &mut reservation {
                    Some(reservation) => Arc::get_mut(reservation).expect("Read checks own their reservation").grow_to(bytes)?,
                    None => reservation = Some(context.memory.reserve(bytes)?),
                }
                additional.insert(layout, check);
            }
        }
        Ok(())
    }

    fn len(&self) -> usize { self.batch.len }

    fn value(&self, row: usize, position: Option<usize>) -> Result<AggregateCell<'a>> {
        let Some(position) = position else { return Ok(AggregateCell::Borrowed(&Value::Null)); };
        let column = &self.batch.columns[position];
        if let Column::Values(values) = column && (!self.projected || !values[row].has_calendar_object()) {
            return Ok(AggregateCell::Borrowed(&values[row]));
        }
        let reservation = self.memory.reserve(column.bound(row)?)?;
        let mut value = column.value(row)?;
        if self.projected { value.admit_expression_dates()?; }
        Ok(AggregateCell::Read { value, _reservation: reservation })
    }

    fn is_null(&self, row: usize, position: Option<usize>) -> Result<bool> {
        if let Some(position) = position
            && let Some(is_null) = self.batch.columns[position].null_at(row) {
            return Ok(is_null);
        }
        Ok(matches!(self.value(row, position)?.as_ref(), Value::Null | Value::Undefined))
    }

    fn arrow_column(&self, position: Option<usize>) -> Option<&'a Column> {
        self.batch.columns.get(position?)
    }
}

struct AggregateInput { batches: crate::columnar::BoxBatchCursor, projected: bool }

#[derive(Clone, Copy)]
enum GroupMapping<'a> {
    Global(usize),
    Grouped(&'a [i32]),
}

impl<'a> GroupMapping<'a> {
    fn iter(self) -> impl Iterator<Item = usize> + Clone + 'a {
        let (len, ordinals) = match self {
            Self::Global(len) => (len, None),
            Self::Grouped(ordinals) => (ordinals.len(), Some(ordinals)),
        };
        (0..len).map(move |row| ordinals.map_or(0, |groups| groups[row] as usize))
    }
}

pub(crate) struct AggregateTypes { input: LogicalType, declared_output: LogicalType, output: LogicalType }

impl AggregateTypes {
    pub fn derive(expression: &AggregateExpr, input: LogicalType, global: bool, mode: EmissionMode) -> Result<Self> {
        use AggregateFunction as A;
        use LogicalType as T;
        use ScalarType as S;
        let declared_input = match mode {
            EmissionMode::ArrowFields => input.clone(),
            EmissionMode::Execution => expression.argument.as_ref().map_or(T::Nothing, rtdb_plan::Expr::data_type),
        };
        let promoted = || T::least_upper_bound(std::slice::from_ref(&declared_input))
            .ok_or_else(|| rtdb_types::Anomaly::incorrect("xtdb.group-by/incomparable-aggregate-types", format!("Cannot compute {:?} over {declared_input:?}", expression.function)));
        let output = match expression.function.canonical() {
            A::Count | A::CountStar => T::Scalar(S::I64),
            A::VariancePopulation | A::VarianceSample | A::StddevPopulation | A::StddevSample => T::Scalar(S::F64).maybe(),
            A::Every | A::Any => T::Scalar(S::Bool).maybe(),
            A::Percentile { kind: PercentileKind::Continuous, .. } => T::Scalar(S::F64).maybe(),
            A::Percentile { kind: PercentileKind::Discrete, .. } => declared_input.clone().maybe(),
            A::ArrayAgg | A::VecAgg => {
                let list = T::list(declared_input.join(&input));
                if (global && expression.function.canonical() == A::ArrayAgg) || (matches!(mode, EmissionMode::Execution) && expression.filter.is_some()) {
                    list.maybe()
                } else { list }
            }
            A::Sum => promoted()?.maybe(),
            A::Average => match promoted()? {
                ty @ T::Scalar(S::Duration(_)) => ty.maybe(),
                T::Nothing | T::Null | T::Scalar(S::I8 | S::I16 | S::I32 | S::I64 | S::U8 | S::U16 | S::U32 | S::U64 | S::F16 | S::F32 | S::F64 | S::Decimal { .. }) => T::Scalar(S::F64).maybe(),
                _ => return Err(invalid("invalid-aggregate-type", "AVG requires numeric or duration input")),
            },
            A::Alias(_) | A::Named(_) => unreachable!(),
            A::Min | A::Max => match T::least_upper_bound(std::slice::from_ref(&declared_input)) {
                None => {
                    let form = declared_input.type_form()?;
                    let data = rtdb_format::diagnostics::vector_type_data(&form)?;
                    return Err(rtdb_types::Anomaly::incorrect("xtdb.group-by/incomparable-min-max-types", format!("Incomparable types in min/max aggregate: #xt/type {form}"))
                        .with_context("from-type", data));
                }
                Some(ty @ (T::Nothing | T::Null | T::Scalar(S::I8 | S::I16 | S::I32 | S::I64 | S::U8 | S::U16 | S::U32 | S::U64 | S::F16 | S::F32 | S::F64 | S::Decimal { .. }
                    | S::Duration(_) | S::DateDay | S::DateMilli | S::Timestamp { .. } | S::Time { .. } | S::Utf8 | S::Binary | S::Keyword | S::Uri))) => ty.maybe(),
                Some(ty) => {
                    let code = "xtdb.group-by/unsupported-min-max-type";
                    let Ok(form) = ty.type_form() else {
                        return Err(rtdb_types::Anomaly::unsupported(code, format!("Unsupported type in min/max aggregate: {ty:?}")));
                    };
                    let data = rtdb_format::diagnostics::column_type_data(&form)?;
                    return Err(rtdb_types::Anomaly::unsupported(code, format!("Unsupported type in min/max aggregate: {form}"))
                        .with_context("unsupported-type", data));
                }
            },
        };
        Ok(Self { input, output: mode.field_type(&output), declared_output: output })
    }
    pub fn output_type(&self) -> &LogicalType { &self.output }
}

pub(crate) struct AggregateCursor {
    input: Option<AggregateInput>, schema: Schema, groups: Vec<NamedExpr>, aggregates: Vec<AggregateExpr>,
    runtime: Arc<EmittedRelation>, execution: Execution,
}

struct ReducedGroups {
    keys: ColumnBatch,
    columns: Vec<AggregateColumn>,
    _reservation: Arc<rtdb_columnar::MemoryReservation>,
}
impl AggregateCursor {
    /// `projected` when the batches passed through a column projection, which admits each cell's dates as it copies it.
    pub fn from_batches(input: crate::columnar::BoxBatchCursor, projected: bool, schema: Schema, groups: Vec<NamedExpr>, aggregates: Vec<AggregateExpr>, runtime: Arc<EmittedRelation>, execution: Execution) -> Result<Self> {
        Self::with_input(AggregateInput { batches: input, projected }, schema, groups, aggregates, runtime, execution)
    }
    fn with_input(input: AggregateInput, schema: Schema, groups: Vec<NamedExpr>, mut aggregates: Vec<AggregateExpr>, runtime: Arc<EmittedRelation>, execution: Execution) -> Result<Self> {
        if runtime.aggregates()?.len() != aggregates.len() { return Err(invalid("runtime-plan", "Aggregate factories disagree with their expressions")); }
        for aggregate in &mut aggregates {
            aggregate.function = aggregate.function.canonical();
            if let AggregateFunction::Percentile { fraction, .. } = &mut aggregate.function {
                if let PercentileFraction::Integer(value) = *fraction { *fraction = PercentileFraction::Literal(value as f64); }
                if let PercentileFraction::Parameter { index } = *fraction {
                    let _scratch = execution.context.memory.reserve(4096)?;
                    let value = execution.parameters.get(index)?.unwrap_or(&Value::Null);
                    let resolved = number(value).ok_or_else(|| rtdb_types::Anomaly::incorrect("xtdb/expected-number", "Percentile fraction parameter must be a number").with_context("param", serde_json::json!(format!("?_{}", index))))?;
                    *fraction = PercentileFraction::Literal(resolved);
                }
                if let PercentileFraction::Literal(value) = *fraction && !(0.0..=1.0).contains(&value) {
                    return Err(rtdb_types::Anomaly::incorrect("xtdb.operator.group-by/percentile-fraction-out-of-range",
                        format!("percentile fraction must be between 0 and 1, got: {}", rtdb_types::java_f64_text(value))).with_context("fraction", serde_json::json!(value)));
                }
            }
        }
        Ok(Self { input: Some(input), schema, groups, aggregates, runtime, execution })
    }
    fn positions(schema: &Schema, groups: &[NamedExpr], aggregates: &[AggregateExpr]) -> Option<Vec<usize>> {
        let mut positions = Vec::new();
        for group in groups {
            let rtdb_plan::Expr::Column { id, .. } = &group.expr else { return None };
            positions.push(schema.position(*id)?);
        }
        let mut arguments = HashSet::new();
        let mut add_argument = |argument: &rtdb_plan::Expr| {
            let position = argument_position(argument, schema).ok()?;
            if arguments.insert(position) { positions.push(position); }
            Some(())
        };
        for aggregate in aggregates {
            if aggregate.function.canonical() == AggregateFunction::ArrayAgg {
                for order in &aggregate.order_by {
                    add_argument(&order.expr)?;
                }
            }
            if let Some(argument) = &aggregate.argument { add_argument(argument)?; }
        }
        Some(positions)
    }

    async fn reduce_global(&mut self, input: AggregateInput) -> Result<(Vec<AggregateColumn>, Arc<rtdb_columnar::MemoryReservation>)> {
        let AggregateInput { mut batches, projected } = input;
        let (mut columns, reservation) = self.accumulators()?;
        let positions = Self::positions(&self.schema, &self.groups, &self.aggregates);
        while let Some(batch) = batches.next_batch().await? {
            let mapping = GroupMapping::Global(batch.len);
            if positions.is_some() && self.accepts_typed_aggregates(&batch)? {
                self.reduce_typed(&batch, mapping, 1, &mut columns)?;
                continue;
            }
            let positions = positions.as_ref().ok_or_else(|| invalid("aggregate-input", "A batch input reached an aggregate that reads whole rows"))?;
            let rows = Rows::columns(&batch, positions, projected, &self.execution.context)?;
            self.execution.context.check_cancelled()?;
            for ((column, aggregate), types) in columns.iter_mut().zip(&self.aggregates).zip(self.runtime.aggregates()?) {
                column.aggregate(rows, mapping, 1, aggregate, types, &self.schema, &self.execution).await?;
            }
        }
        Ok((columns, reservation))
    }

    fn accumulators(&self) -> Result<(Vec<AggregateColumn>, Arc<rtdb_columnar::MemoryReservation>)> {
        let bytes = self.aggregates.len().checked_mul(std::mem::size_of::<AggregateColumn>())
            .ok_or_else(|| invalid("aggregate-size", "Aggregate column container overflow"))?;
        let reservation = self.execution.context.memory.reserve(bytes)?;
        let columns = self.aggregates.iter().map(|aggregate| AggregateColumn::new(aggregate, &self.schema, &self.execution)).collect::<Result<_>>()?;
        Ok((columns, reservation))
    }

    async fn reduce_grouped(&mut self, input: AggregateInput) -> Result<Option<ReducedGroups>> {
        let AggregateInput { mut batches, projected } = input;
        let context = &self.execution.context;
        let memory = &context.memory;
        let group_width = self.groups.len();
        let key_schema = Schema::new(self.runtime.schema().fields()[..group_width].to_vec())?;
        let positions = Self::positions(&self.schema, &self.groups, &self.aggregates).ok_or_else(|| invalid("aggregate-input", "Arrow group key column is absent"))?;
        let (group_positions, arguments) = positions.split_at(group_width);
        let mut index = BatchKeyIndex::vector_hashing(context)?;
        let (mut columns, reservation) = self.accumulators()?;
        while let Some(batch) = crate::columnar::poll_nested_batch(batches.as_mut()).await? {
            context.check_cancelled()?;
            crate::operators::validate_distinct_types(&key_schema)?;
            if batch.len == 0 { continue; }
            let _mapping = memory.reserve(batch.len.checked_mul(size_of::<i32>())
                .ok_or_else(|| invalid("aggregate-size", "Group mapping exceeds addressable memory"))?)?;
            let keys = KeyColumns::projection(&batch, group_positions);
            let mut mapping = Vec::with_capacity(batch.len);
            index.identify(keys, projected, context, &key_schema, |_, ordinal, _| {
                if ordinal >= i32::MAX as usize {
                    return Err(invalid("group-count", "Aggregate group count exceeds the reference vector limit"));
                }
                mapping.push(ordinal as i32);
                Ok(())
            })?;
            let mapping = GroupMapping::Grouped(&mapping);
            if self.accepts_typed_aggregates(&batch)? {
                self.reduce_typed(&batch, mapping, index.len(), &mut columns)?;
                continue;
            }
            let rows = Rows::columns(&batch, arguments, projected, context)?;
            for ((column, aggregate), types) in columns.iter_mut().zip(&self.aggregates).zip(self.runtime.aggregates()?) {
                column.aggregate(rows, mapping, index.len(), aggregate, types, &self.schema, &self.execution).await?;
            }
        }
        drop(batches);
        Ok(index.into_group_keys(group_width, context)?.map(|keys| ReducedGroups { keys, columns, _reservation: reservation }))
    }
}

#[async_trait]
impl BatchCursor for AggregateCursor {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        self.execution.context.check_cancelled()?;
        let Some(input) = self.input.take() else { return Ok(None); };
        self.reduce(input).await
    }
}

impl AggregateCursor {
    /// Every group, held in memory until the input ends, as XTDB's group-by holds them; it fails when the query budget
    /// cannot hold them rather than spilling.
    async fn reduce(&mut self, input: AggregateInput) -> Result<Option<ColumnBatch>> {
        let (mut batch, columns, _reservation) = if self.groups.is_empty() {
            let (columns, _reservation) = self.reduce_global(input).await?;
            let batch = ColumnBatch::settled(vec![], 1, self.execution.context.memory.reserve(256)?)?;
            (batch, columns, _reservation)
        } else {
            let Some(ReducedGroups { keys, columns, _reservation }) = self.reduce_grouped(input).await? else {
                for (aggregate, types) in self.aggregates.iter().zip(self.runtime.aggregates()?) {
                    validate_collection_order(aggregate, types, &self.schema, &self.execution)?;
                }
                return Ok(None);
            };
            (keys, columns, _reservation)
        };
        let context = &self.execution.context;
        let schema = self.runtime.schema();
        let fields = &schema.fields()[self.groups.len()..];
        for (((column, aggregate), field), types) in columns.into_iter().zip(&self.aggregates).zip(fields).zip(self.runtime.aggregates()?) {
            context.check_cancelled()?;
            let mut states = match column.accumulation {
                Accumulation::Counts(mut counts) => {
                    counts.ensure(batch.len)?;
                    for _ in (0..batch.len).step_by(1024) {
                        context.check_cancelled()?;
                    }
                    let logical = field.data_type.clone();
                    let field = Arc::new(rtdb_columnar::to_field(&field.name, &logical)?);
                    let array = Arc::new(arrow_array::Int64Array::from(counts.values));
                    batch.append_column(Column::Arrow { array, field, logical: Some(logical) }, counts.reservation, &context.memory)?;
                    continue;
                }
                Accumulation::Scalar(states) => states,
                Accumulation::Vectors(vectors) => {
                    validate_collection_order(aggregate, types, &self.schema, &self.execution)?;
                    let (column, charge) = vectors.finish(batch.len, aggregate, field, &self.execution).await?;
                    if charge.backing().is_empty() {
                        batch.append_column(column, charge, &context.memory)?;
                    } else {
                        batch.append_funded_column(column, charge, &context.memory)?;
                    }
                    continue;
                }
            };
            states.ensure(batch.len)?;
            if let Some((column, charge)) = fixed_scalar_results(&states, &aggregate.function, field, context)? {
                batch.append_column(column, charge, &context.memory)?;
                continue;
            }
            if let Some((column, charge)) = byte_scalar_results(&states, &aggregate.function, field, context)? {
                batch.append_column(column, charge, &context.memory)?;
                continue;
            }
            let bytes = batch.len.checked_mul(std::mem::size_of::<Value>().max(40)).and_then(|bytes| bytes.checked_add(field.name.len()))
                .and_then(|bytes| bytes.checked_add(512)).ok_or_else(|| invalid("aggregate-size", "Aggregate output exceeds addressable memory"))?;
            let reservation = context.memory.reserve(bytes)?;
            let scope = self.execution.scope()?;
            let mut reservation = reservation;
            let lease = Arc::get_mut(&mut reservation).expect("Aggregate results own their reservation");
            let mut values = Vec::with_capacity(batch.len);
            for (row, state) in states.values.iter().enumerate() {
                context.check_cancelled_at(row)?;
                let value = state.finish(aggregate, types.output_type(), &scope)?;
                lease.grow_to(lease.bytes().checked_add(value.retained_bytes()?)
                    .ok_or_else(|| invalid("aggregate-size", "Aggregate output exceeds addressable memory"))?)?;
                values.push(value);
                scope.reset()?;
            }
            let (column, scratch) = crate::columnar::pack_evaluated_column(&values, &context.memory)?;
            drop((values, reservation));
            batch.append_column(column, scratch, &context.memory)?;
        }
        Ok(Some(batch))
    }
}

fn fixed_scalar_results(
    states: &ScalarGroups, function: &AggregateFunction, field: &rtdb_plan::Field, context: &crate::QueryContext,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    use AggregateFunction as A;
    use ScalarType as S;
    use rtdb_types::TimeUnit;
    use arrow_array::builder::BooleanBuilder;

    let LogicalType::Scalar(scalar) = without_nullability(&field.data_type) else { return Ok(None); };
    let state_error = || invalid("aggregate-state", "Aggregate result state does not match its output vector");
    macro_rules! primitive {
        ($ty:ty, $read:expr) => {
            primitive_results::<$ty>(states, field, context, $read).map(Some)
        };
    }
    macro_rules! retained {
        ($state:expr, $pat:pat => $value:expr) => {
            match &$state.accumulator {
                ScalarState::Empty => Ok(None),
                state => match state.retained() {
                    Some($pat) => Ok(Some($value)),
                    _ => Err(state_error()),
                },
            }
        };
    }
    macro_rules! numeric {
        ($ty:ty, $variant:ident) => {
            primitive!($ty, |state| match &state.accumulator {
                ScalarState::$variant(value) => Ok(Some(*value)),
                _ => retained!(state, Value::$variant(value) => *value),
            })
        };
    }
    // A winning timestamp has already been cast to the output type by the reducer.
    let timestamp = |state: &ScalarAccumulator| match &state.accumulator {
        ScalarState::TemporalExtremum(value) => Ok(Some(*value)),
        _ => retained!(state, Value::Timestamp { value, .. } => *value),
    };
    let time = |state: &ScalarAccumulator| match &state.accumulator {
        ScalarState::Empty => Ok(None),
        ScalarState::TemporalExtremum(value) => Ok(Some(*value)),
        _ => Err(state_error()),
    };
    let time32 = |state: &ScalarAccumulator| time(state)?.map(|value| {
        i32::try_from(value).map_err(|_| rtdb_types::Anomaly::incorrect("rtdb/time-overflow", "Time exceeds i32 representation")
            .with_kind(rtdb_types::AnomalyKind::NumericValueOutOfRange))
    }).transpose();
    let duration = |state: &ScalarAccumulator| match (&state.accumulator, function) {
        (_, A::Average) if state.count == 0 => Ok(None),
        (ScalarState::Duration(value, _), A::Average) => Ok(Some(value.wrapping_div(state.count))),
        (ScalarState::Duration(value, _), _) => Ok(Some(*value)),
        (ScalarState::TemporalExtremum(value), A::Min | A::Max) => Ok(Some(*value)),
        _ => retained!(state, Value::Duration { value, .. } => *value),
    };
    match (function, scalar) {
        (A::Sum | A::Min | A::Max, S::I32) => numeric!(Int32Type, I32),
        (A::Sum | A::Min | A::Max, S::I64) => numeric!(Int64Type, I64),
        (A::Sum | A::Min | A::Max, S::F32) => numeric!(Float32Type, F32),
        (A::Min | A::Max, S::F64) => numeric!(Float64Type, F64),
        (A::Sum, S::F64) => primitive!(Float64Type, |state| match state.accumulator {
            ScalarState::F64(sum) => Ok(Some(sum)),
            ScalarState::Empty => Ok(None),
            _ => Err(invalid("aggregate-input", "Expected a floating-point aggregate state")),
        }),
        (A::Average, S::F64) => primitive!(Float64Type, |state| Ok(state.average_double())),
        (A::VariancePopulation | A::VarianceSample | A::StddevPopulation | A::StddevSample, S::F64) =>
            primitive!(Float64Type, |state| Ok(state.variance_double(function))),
        (A::Min | A::Max, S::I8) => numeric!(Int8Type, I8),
        (A::Min | A::Max, S::I16) => numeric!(Int16Type, I16),
        (A::Min | A::Max, S::Time { unit: TimeUnit::Second, bit_width: 32 }) => primitive!(Time32SecondType, time32),
        (A::Min | A::Max, S::Time { unit: TimeUnit::Millisecond, bit_width: 32 }) => primitive!(Time32MillisecondType, time32),
        (A::Min | A::Max, S::Time { unit: TimeUnit::Microsecond, bit_width: 64 }) => primitive!(Time64MicrosecondType, time),
        (A::Min | A::Max, S::Time { unit: TimeUnit::Nanosecond, bit_width: 64 }) => primitive!(Time64NanosecondType, time),
        (A::Min | A::Max, S::DateDay) => primitive!(Date32Type, |state| {
            date_result(state, rtdb_types::DateUnit::Day)?.map(i32::try_from).transpose().map_err(|_| state_error())
        }),
        (A::Min | A::Max, S::DateMilli) => primitive!(Date64Type, |state| date_result(state, rtdb_types::DateUnit::Millisecond)),
        (A::Min | A::Max, S::Timestamp { unit, .. }) => match unit {
            TimeUnit::Second => primitive!(TimestampSecondType, timestamp),
            TimeUnit::Millisecond => primitive!(TimestampMillisecondType, timestamp),
            TimeUnit::Microsecond => primitive!(TimestampMicrosecondType, timestamp),
            TimeUnit::Nanosecond => primitive!(TimestampNanosecondType, timestamp),
        },
        (A::Sum | A::Average | A::Min | A::Max, S::Duration(unit)) => match unit {
            TimeUnit::Second => primitive!(DurationSecondType, duration),
            TimeUnit::Millisecond => primitive!(DurationMillisecondType, duration),
            TimeUnit::Microsecond => primitive!(DurationMicrosecondType, duration),
            TimeUnit::Nanosecond => primitive!(DurationNanosecondType, duration),
        },
        (A::Min | A::Max, S::Decimal { bit_width: 128, .. })
            if states.values.iter().any(|state| matches!(state.accumulator, ScalarState::Retained(_))) => Ok(None),
        (A::Sum | A::Min | A::Max, S::Decimal { bit_width: 128, .. }) => primitive!(Decimal128Type, |state| {
            Ok(match &state.accumulator {
                ScalarState::Empty => None,
                ScalarState::Decimal128(bytes) => Some(i128::from_ne_bytes(*bytes)),
                state => {
                    let Some(Value::Decimal(value)) = state.retained() else { return Err(sum_state()); };
                    Some(i128::from_be_bytes(value.unscaled.as_slice().try_into().map_err(|_| sum_state())?))
                }
            })
        }),
        (A::Every | A::Any, S::Bool) => {
            let len = states.values.len();
            let (column, charge) = build_fixed_results(&field.name, &field.data_type, context, len, len.div_ceil(8), |_| {
                let mut builder = BooleanBuilder::with_capacity(len);
                for (row, state) in states.values.iter().enumerate() {
                    context.check_cancelled_at(row)?;
                    let value = match state.accumulator {
                        ScalarState::Boolean(value) => Some(value),
                        _ => retained!(state, Value::Bool(value) => *value)?,
                    };
                    builder.append_option(value);
                }
                Ok(Arc::new(builder.finish()))
            })?;
            Ok(Some((column, charge)))
        }
        _ => Ok(None),
    }
}

fn date_result(state: &ScalarAccumulator, unit: rtdb_types::DateUnit) -> Result<Option<i64>> {
    match &state.accumulator {
        ScalarState::Empty => Ok(None),
        ScalarState::TemporalExtremum(value) => Ok(Some(*value)),
        state => {
            if let Some(Value::Date(value)) = state.retained() {
                match rtdb_types::CalendarDate::new(value.date(), unit).object_value()? {
                    Value::DateDay(days) => return Ok(Some(i64::from(days))),
                    Value::DateMilli(millis) => return Ok(Some(millis)),
                    _ => {}
                }
            }
            Err(invalid("aggregate-state", "Expected a date extremum"))
        }
    }
}

fn byte_scalar_results(
    states: &ScalarGroups, function: &AggregateFunction, field: &rtdb_plan::Field, context: &crate::QueryContext,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    use arrow_array::builder::{BinaryBuilder, StringBuilder};
    use ScalarType as S;
    if !matches!(function, AggregateFunction::Min | AggregateFunction::Max) {
        return Ok(None);
    }
    let required = without_nullability(&field.data_type);
    let LogicalType::Scalar(scalar @ (S::Utf8 | S::Binary | S::Keyword | S::Uri)) = required else { return Ok(None); };
    let len = states.values.len();
    let mut payload = 0usize;
    let mut has_winner = false;
    let mut has_null = false;
    for (row, state) in states.values.iter().enumerate() {
        context.check_cancelled_at(row)?;
        let size = match (scalar, state.accumulator.retained()) {
            (S::Utf8, Some(Value::Utf8(value))) | (S::Uri, Some(Value::Uri(value))) => value.len(),
            (S::Binary, Some(Value::Binary(value))) => value.len(),
            (S::Keyword, Some(Value::Keyword(value))) => value.as_str().len(),
            (_, None) if matches!(state.accumulator, ScalarState::Empty) => {
                has_null = true;
                continue;
            }
            _ => return Ok(None),
        };
        has_winner = true;
        let Some(bytes) = payload.checked_add(size).filter(|bytes| i32::try_from(*bytes).is_ok()) else {
            return Ok(None);
        };
        payload = bytes;
    }
    if !has_winner {
        let charge = context.memory.reserve(512)?;
        let field = Arc::new(rtdb_columnar::to_field("value", &LogicalType::Null)?);
        let array = Arc::new(arrow_array::NullArray::new(len));
        return Ok(Some((Column::Arrow { array, field, logical: Some(LogicalType::Null) }, charge)));
    }
    let bytes = len.checked_add(1)
        .and_then(|offsets| offsets.checked_mul(size_of::<i32>()))
        .and_then(|bytes| bytes.checked_add(payload))
        .ok_or_else(|| invalid("aggregate-size", "Aggregate output exceeds addressable memory"))?;
    let observed;
    let (name, logical) = if matches!(scalar, S::Keyword | S::Uri) {
        observed = if has_null { required.clone().maybe() } else { required.clone() };
        ("value", &observed)
    } else {
        (field.name.as_str(), &field.data_type)
    };
    build_fixed_results(name, logical, context, len, bytes, |_| {
        macro_rules! build {
            ($builder:ty, $variant:ident, $value:ident => $text:expr) => {{
                let mut builder = <$builder>::with_capacity(len, payload);
                for (row, state) in states.values.iter().enumerate() {
                    context.check_cancelled_at(row)?;
                    match state.accumulator.retained() {
                        Some(Value::$variant($value)) => builder.append_value($text),
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }};
        }
        match scalar {
            S::Utf8 => build!(StringBuilder, Utf8, value => value),
            S::Binary => build!(BinaryBuilder, Binary, value => value),
            S::Keyword => build!(StringBuilder, Keyword, value => value.as_str()),
            S::Uri => build!(StringBuilder, Uri, value => {
                rtdb_types::UriParts::parse(value)?;
                value
            }),
            _ => unreachable!(),
        }
    }).map(Some)
}

fn primitive_results<T: ArrowPrimitiveType>(
    states: &ScalarGroups, field: &rtdb_plan::Field, context: &crate::QueryContext,
    read: impl Fn(&ScalarAccumulator) -> Result<Option<T::Native>>,
) -> Result<(Column, Arc<MemoryReservation>)> {
    let len = states.values.len();
    let values = len.checked_mul(size_of::<T::Native>())
        .ok_or_else(|| invalid("aggregate-size", "Aggregate output exceeds addressable memory"))?;
    build_fixed_results(&field.name, &field.data_type, context, len, values, |data_type| {
        let mut builder = arrow_array::builder::PrimitiveBuilder::<T>::with_capacity(len).with_data_type(data_type.clone());
        for (row, state) in states.values.iter().enumerate() {
            context.check_cancelled_at(row)?;
            builder.append_option(read(state)?);
        }
        Ok(Arc::new(builder.finish()))
    })
}

fn build_fixed_results(
    name: &str, logical: &LogicalType, context: &crate::QueryContext, len: usize, values: usize,
    build: impl FnOnce(&DataType) -> Result<arrow_array::ArrayRef>,
) -> Result<(Column, Arc<MemoryReservation>)> {
    let logical_bytes = logical.retained_bytes()?;
    let bytes = values.checked_add(len.div_ceil(8))
        .and_then(|bytes| bytes.checked_add(logical_bytes.checked_mul(2)?))
        .and_then(|bytes| bytes.checked_add(name.len()))
        .and_then(|bytes| bytes.checked_add(512))
        .ok_or_else(|| invalid("aggregate-size", "Aggregate output exceeds addressable memory"))?;
    let charge = context.memory.reserve(bytes)?;
    let field = Arc::new(rtdb_columnar::to_field(name, logical)?);
    let array = build(field.data_type())?;
    Ok((Column::Arrow { array, field, logical: Some(logical.clone()) }, charge))
}

fn double_results(values: Vec<Option<f64>>, reservation: Arc<MemoryReservation>) -> Result<(Column, Arc<MemoryReservation>)> {
    let (array, logical): (arrow_array::ArrayRef, _) = if values.iter().all(Option::is_none) {
        (Arc::new(arrow_array::NullArray::new(values.len())), LogicalType::Null)
    } else {
        let logical = LogicalType::Scalar(ScalarType::F64);
        let logical = if values.iter().any(Option::is_none) { logical.maybe() } else { logical };
        (Arc::new(arrow_array::Float64Array::from(values)), logical)
    };
    let field = Arc::new(rtdb_columnar::to_field("value", &logical)?);
    Ok((Column::Arrow { array, field, logical: Some(logical) }, reservation))
}

fn direct_rank_type(ty: &LogicalType) -> bool {
    use ScalarType as S;
    matches!(without_nullability(ty), LogicalType::Null | LogicalType::Scalar(
        S::Bool | S::I8 | S::I16 | S::I32 | S::I64 | S::F32 | S::F64 | S::Utf8 | S::Binary))
}

enum PercentileKey {
    Native(NativeKeyKind),
    Decimal,
    Number,
}

impl PercentileKey {
    fn bind(ty: &LogicalType, batches: &[ColumnBatch], reservation: &mut MemoryReservation)
        -> Result<Option<(Self, ArrowField)>> {
        use ScalarType as S;
        if batches.is_empty() { return Ok(None); }
        let ty = without_nullability(ty);
        let key = match ty {
            LogicalType::Scalar(S::Decimal { .. }) => Self::Decimal,
            LogicalType::Scalar(S::Timestamp { .. } | S::Time { .. } | S::Duration(_) | S::DateDay | S::DateMilli
                | S::Uuid | S::Interval(rtdb_types::IntervalUnit::YearMonth)) =>
                Self::Native(NativeKeyKind::of(ty).expect("Fixed-width percentile key has a native sort reader")),
            _ => return Ok(None),
        };
        let previous_bytes = reservation.bytes();
        let bytes = ty.retained_bytes()?.checked_mul(8).and_then(|bytes| bytes.checked_add(512))
            .and_then(|bytes| bytes.checked_add(previous_bytes))
            .ok_or_else(|| invalid("aggregate-size", "Percentile field exceeds addressable memory"))?;
        reservation.grow_to(bytes)?;
        let Ok(field) = rtdb_columnar::to_field("value", ty) else {
            reservation.shrink_to(previous_bytes)?;
            return Ok(None);
        };
        let accepts = |array: &dyn Array, source: &ArrowField, logical: Option<&LogicalType>| {
            source.data_type() == array.data_type() && (if source.data_type() == &DataType::Null {
                source.is_nullable() && logical.is_none_or(|ty| *ty == LogicalType::Null)
            } else {
                source.data_type() == field.data_type() && source.metadata() == field.metadata()
                    && logical.is_none_or(|logical| without_nullability(logical) == ty)
            })
        };
        let matches = batches.iter().all(|batch| match &batch.columns[0] {
            Column::Arrow { array, field, logical } => accepts(array.as_ref(), field, logical.as_ref()),
            Column::Selected(selected) => selected.source_layouts().all(|(array, field, logical)| accepts(array, field, logical)),
            Column::Values(_) => false,
        });
        if !matches {
            drop(field);
            reservation.shrink_to(previous_bytes)?;
            return Ok(None);
        }
        if matches!(key, Self::Decimal) {
            reservation.grow_to(bytes.checked_add(4096)
                .ok_or_else(|| invalid("aggregate-size", "Percentile decimal conversion exceeds addressable memory"))?)?;
        }
        Ok(Some((key, field)))
    }

    fn decimal(position: (&Column, usize)) -> Result<f64> {
        let ArrowCell { array, field, row } = ranked_arrow_cell(position);
        rtdb_columnar::decimal_cell_as_f64(field, array, row)
    }

    fn compatible(ty: &LogicalType, batches: &[ColumnBatch]) -> Option<Self> {
        if let Some(kind) = NativeKeyKind::of(ty)
            && batches.iter().all(|batch| {
                let column = &batch.columns[0];
                admits_reduction_column(column, |field, logical| {
                    !matches!(field.data_type(), DataType::Union(..))
                        && logical.is_none_or(|ty| crate::columnar::stable_read_layout(ty, field))
                }) && kind.accepts(column)
            })
        {
            return Some(Self::Native(kind));
        }
        if ty.legs().iter().all(|ty| matches!(ty, LogicalType::Null | LogicalType::Scalar(
            ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64
            | ScalarType::U8 | ScalarType::U16 | ScalarType::U32 | ScalarType::U64
            | ScalarType::F32 | ScalarType::F64 | ScalarType::Decimal { .. }
        ))) && batches.iter().all(|batch| admits_reduction_column(&batch.columns[0], |field, logical| {
            NumericColumn::admits_leaf(field, logical, None) || decimal_leaf(field, logical)
        })) {
            return Some(Self::Number);
        }
        None
    }

    fn number((column, row): (&Column, usize)) -> Result<NumericCell> {
        let Some(cell) = present_column_position(column, row)? else {
            return Err(invalid("percentile-input", "A ranked numeric cell must be present"));
        };
        NumericCell::read_number_arrow(cell)
    }

    fn compare(&self, left: (&Column, usize), right: (&Column, usize)) -> Result<Ordering> {
        match self {
            Self::Native(kind) => kind.compare(left, right, false, false),
            Self::Decimal => Ok(Self::decimal(left)?.total_cmp(&Self::decimal(right)?)),
            Self::Number => Self::number(left)?.key().ordered_compare(Self::number(right)?.key())
                .ok_or_else(|| invalid("percentile-input", "Expected two numeric rank keys")),
        }
    }
}

fn admits_discrete_winners(ty: &LogicalType, batches: &[ColumnBatch]) -> bool {
    let legs = ty.legs();
    let accepts = |array: &dyn Array, field: &ArrowField, logical: Option<&LogicalType>| {
        if array.data_type() != field.data_type() { return false; }
        let Ok(actual) = rtdb_columnar::from_field(field) else { return false; };
        if logical.is_some_and(|ty| *ty != actual) { return false; }
        let scalar = without_nullability(&actual);
        if *scalar == LogicalType::Null { return field.is_nullable(); }
        if !legs.contains(scalar) || !matches!(scalar, LogicalType::Scalar(
            ScalarType::Bool | ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64
            | ScalarType::F32 | ScalarType::F64 | ScalarType::Utf8 | ScalarType::Binary
            | ScalarType::Keyword | ScalarType::FixedSizeBinary(_) | ScalarType::Uuid
            | ScalarType::DateDay | ScalarType::DateMilli | ScalarType::Timestamp { .. }
            | ScalarType::Time { .. } | ScalarType::Duration(_) | ScalarType::Decimal { .. }
            | ScalarType::Interval(rtdb_types::IntervalUnit::YearMonth | rtdb_types::IntervalUnit::MonthDayNano)
            | ScalarType::IntervalMonthDayMicro
        )) {
            return false;
        }
        rtdb_columnar::to_field(field.name(), &actual)
            .is_ok_and(|expected| expected.data_type() == field.data_type() && expected.metadata() == field.metadata())
    };
    !batches.is_empty() && batches.iter().all(|batch| match &batch.columns[0] {
        Column::Arrow { array, field, logical } => accepts(array.as_ref(), field, logical.as_ref()),
        Column::Selected(selected) => selected.source_layouts().all(|(array, field, logical)| accepts(array, field, logical)),
        Column::Values(_) => false,
    })
}

fn ranked_arrow_cell((column, row): (&Column, usize)) -> ArrowCell<'_> {
    let (array, field, _, row) = column.arrow_cell(row).flatten().expect("A ranked Arrow cell is present");
    ArrowCell { array, field, row }
}

fn copy_ranked_cells(
    batches: &[ColumnBatch], positions: &[Option<(usize, usize)>], field: ArrowField, memory: &Arc<rtdb_columnar::MemoryBudget>,
) -> Result<(Column, Arc<MemoryReservation>)> {
    // Fixed-size binary interleave also allocates ArrayData and extension closures for its sources.
    let bytes_per_position = if matches!(field.data_type(), DataType::FixedSizeBinary(_)) { 512 } else { 256 };
    let bytes = positions.len().checked_mul(bytes_per_position)
        .and_then(|bytes| field.size().checked_mul(8).and_then(|field| bytes.checked_add(field)))
        .and_then(|bytes| bytes.checked_add(4096))
        .ok_or_else(|| invalid("aggregate-size", "Percentile output exceeds addressable memory"))?;
    let reservation = memory.reserve(bytes)?;
    let mut sources = HashMap::new();
    let mut arrays = Vec::new();
    let picks = positions.iter().map(|position| position.map(|(page, row)| {
        let ArrowCell { array, row, .. } = ranked_arrow_cell((&batches[page].columns[0], row));
        let source = *sources.entry(std::ptr::from_ref(array).cast::<()>()).or_insert_with(|| {
            let source = arrays.len();
            arrays.push(array);
            source
        });
        (source, row)
    })).collect::<Vec<_>>();
    let (array, field) = if arrays.is_empty() {
        (Arc::new(arrow_array::NullArray::new(picks.len())) as arrow_array::ArrayRef, ArrowField::new("value", DataType::Null, true))
    } else {
        let array = rtdb_columnar::copy_nullable_cells(&arrays, &picks)?;
        let field = field.with_nullable(array.null_count() != 0);
        (array, field)
    };
    let logical = rtdb_columnar::from_field(&field)?;
    Ok((Column::Arrow { array, field: Arc::new(field), logical: Some(logical) }, reservation))
}

fn count_layout(data_type: &DataType) -> bool {
    use DataType as T;
    matches!(data_type, T::Boolean | T::Int8 | T::Int16 | T::Int32 | T::Int64
        | T::UInt8 | T::UInt16 | T::UInt32 | T::UInt64 | T::Float32 | T::Float64
        | T::Utf8 | T::Binary | T::FixedSizeBinary(_))
}

fn decimal_storage(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Decimal32(..) | DataType::Decimal64(..) | DataType::Decimal128(..) | DataType::Decimal256(..))
}

fn decimal_leaf(field: &ArrowField, logical: Option<&LogicalType>) -> bool {
    if field.data_type() == &DataType::Null {
        return field.metadata().is_empty() && field.is_nullable() && logical.is_none_or(|ty| *ty == LogicalType::Null);
    }
    decimal_storage(field.data_type())
        && rtdb_columnar::decimal_field_parameters(field).is_ok_and(|parameters| parameters.is_some())
        && logical.is_none_or(|ty| crate::columnar::stable_read_layout(ty, field))
}

fn masked_count_layout(data_type: &DataType) -> bool {
    count_layout(data_type) || matches!(data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(..) | DataType::Duration(_)
        | DataType::Time32(ArrowTimeUnit::Second | ArrowTimeUnit::Millisecond)
        | DataType::Time64(ArrowTimeUnit::Microsecond | ArrowTimeUnit::Nanosecond))
}

fn accepts_masked_count(column: &Column) -> bool {
    admits_reduction_column(column, |field, logical| {
        if decimal_storage(field.data_type()) { return decimal_leaf(field, logical); }
        admits_plain_leaf(field, logical, masked_count_layout) || admits_dense_numeric(field, logical)
    })
}

fn admits_dense_numeric(field: &ArrowField, logical: Option<&LogicalType>) -> bool {
    use DataType as T;
    let T::Union(children, arrow_schema::UnionMode::Dense) = field.data_type() else { return false; };
    if !field.metadata().is_empty() || logical == Some(&LogicalType::Nothing) { return false; }
    children.iter().all(|(_, child)| {
        if !child.metadata().is_empty() { return false; }
        // A named leg can override the physical type during scalar reads.
        match (child.name().as_str(), child.data_type()) {
            ("null", T::Null) => child.is_nullable(),
            ("i8", T::Int8) | ("i16", T::Int16) | ("i32", T::Int32) | ("i64", T::Int64)
            | ("u8", T::UInt8) | ("u16", T::UInt16) | ("u32", T::UInt32) | ("u64", T::UInt64)
            | ("f32", T::Float32) | ("f64", T::Float64) => true,
            _ => false,
        }
    })
}

fn count_is_present(page: Rows<'_>, typed: Option<&Column>, row: usize, argument: Option<usize>) -> Result<bool> {
    match typed {
        Some(column) => Ok(present_column_cell(column, row)?.is_some()),
        None => Ok(!page.is_null(row, argument)?),
    }
}

fn accepts_typed_column(column: &Column, accepts_type: impl Fn(&DataType) -> bool) -> bool {
    admits_reduction_column(column, |field, logical| admits_plain_leaf(field, logical, &accepts_type))
}

fn admits_plain_leaf(field: &ArrowField, logical: Option<&LogicalType>, accepts_type: impl Fn(&DataType) -> bool) -> bool {
    if !field.metadata().is_empty() { return false; }
    if field.data_type() == &DataType::Null {
        return field.is_nullable() && logical.is_none_or(|ty| *ty == LogicalType::Null);
    }
    accepts_type(field.data_type())
        && logical.is_none_or(|ty| crate::columnar::stable_read_layout(ty, field))
}

fn admits_byte_extremum(column: &Column, output: &LogicalType) -> bool {
    use rtdb_columnar::{EXTENSION_METADATA_KEY, EXTENSION_NAME_KEY};
    use ScalarType as S;
    match output {
        LogicalType::Scalar(S::Utf8) => accepts_typed_column(column, |ty| *ty == DataType::Utf8),
        LogicalType::Scalar(S::Binary) => accepts_typed_column(column, |ty| *ty == DataType::Binary),
        LogicalType::Scalar(scalar @ (S::Keyword | S::Uri)) => admits_reduction_column(column, |field, logical| {
            if field.data_type() == &DataType::Null {
                return admits_plain_leaf(field, logical, |_| false);
            }
            let extension = if *scalar == S::Uri { "uri" } else { "xt/clj-keyword" };
            let metadata = field.metadata();
            field.data_type() == &DataType::Utf8 && metadata.len() == 2
                && metadata.get(EXTENSION_NAME_KEY).is_some_and(|name| name == extension)
                && metadata.get(EXTENSION_METADATA_KEY).is_some_and(String::is_empty)
                && logical.is_none_or(|ty| without_nullability(ty) == output
                    && matches!(ty, LogicalType::Maybe(_)) == field.is_nullable())
        }),
        _ => false,
    }
}

fn admits_reduction_column(column: &Column, leaf: impl Fn(&ArrowField, Option<&LogicalType>) -> bool) -> bool {
    let accepts = |array: &dyn Array, field: &ArrowField, logical: Option<&LogicalType>| {
        field.data_type() == array.data_type() && admits_reduction_field(field, logical, &leaf)
    };
    match column {
        Column::Arrow { array, field, logical } => accepts(array.as_ref(), field, logical.as_ref()),
        Column::Selected(selected) => selected.source_layouts().all(|(array, field, logical)| accepts(array, field, logical)),
        Column::Values(_) => false,
    }
}

fn admits_reduction_field(field: &ArrowField, logical: Option<&LogicalType>, leaf: &impl Fn(&ArrowField, Option<&LogicalType>) -> bool) -> bool {
    if let DataType::RunEndEncoded(ends, values) = field.data_type() {
        if !field.metadata().is_empty() || !matches!(ends.data_type(), DataType::Int16 | DataType::Int32 | DataType::Int64) {
            return false;
        }
        let child_type = match logical {
            Some(LogicalType::RunEndEncoded { value, .. }) => Some(value.as_ref()),
            None => None,
            _ => return false,
        };
        return admits_reduction_field(values, child_type, leaf);
    }
    leaf(field, logical)
}

fn present_column_cell(column: &Column, row: usize) -> Result<Option<(&dyn Array, usize)>> {
    Ok(present_column_position(column, row)?.map(|cell| (cell.array, cell.row)))
}

fn present_column_position(column: &Column, row: usize) -> Result<Option<ArrowCell<'_>>> {
    present_arrow_position(column.arrow_cell(row).ok_or_else(|| invalid("column-row", "Column row is outside its page"))?)
}

fn present_column_cells(column: &Column) -> impl Iterator<Item = Result<Option<(&dyn Array, usize)>>> + '_ {
    let mut selected = match column {
        Column::Selected(column) => Some(column.arrow_cells()),
        _ => None,
    };
    (0..).map(move |row| match &mut selected {
        Some(cells) => {
            let cell = cells.next().ok_or_else(|| invalid("column-row", "Column row is outside its page"))?;
            Ok(present_arrow_position(cell)?.map(|cell| (cell.array, cell.row)))
        }
        None => present_column_cell(column, row),
    })
}

fn present_arrow_position<'a>(cell: Option<(&'a dyn Array, &'a ArrowField, Option<&LogicalType>, usize)>) -> Result<Option<ArrowCell<'a>>> {
    let Some((array, field, _, row)) = cell else { return Ok(None); };
    let cell = ArrowCell { array, field, row }.physical()?;
    Ok((cell.array.data_type() != &DataType::Null && !cell.array.is_null(cell.row)).then_some(cell))
}

fn admits_temporal_extremum(ty: &LogicalType, physical: &DataType) -> bool {
    matches!((without_nullability(ty), physical),
        (LogicalType::Scalar(ScalarType::Timestamp { .. }), DataType::Timestamp(..))
        | (LogicalType::Scalar(ScalarType::Time { .. }), DataType::Time32(ArrowTimeUnit::Second | ArrowTimeUnit::Millisecond)
            | DataType::Time64(ArrowTimeUnit::Microsecond | ArrowTimeUnit::Nanosecond))
        | (LogicalType::Scalar(ScalarType::Duration(_)), DataType::Duration(_))
        | (LogicalType::Scalar(ScalarType::DateDay), DataType::Date32)
        | (LogicalType::Scalar(ScalarType::DateMilli), DataType::Date64))
}

fn time_unit(unit: &ArrowTimeUnit) -> TimeUnit {
    match unit {
        ArrowTimeUnit::Second => TimeUnit::Second,
        ArrowTimeUnit::Millisecond => TimeUnit::Millisecond,
        ArrowTimeUnit::Microsecond => TimeUnit::Microsecond,
        ArrowTimeUnit::Nanosecond => TimeUnit::Nanosecond,
    }
}

fn cast_extremum_ticks(cell: ArrowCell<'_>, output: &LogicalType, timezone: &str) -> Result<i64> {
    match without_nullability(output) {
        LogicalType::Scalar(ScalarType::Timestamp { unit, timezone: zone }) => {
            rtdb_expr::cast_bound_timestamp(cell, *unit, zone.is_some(), timezone)?
                .ok_or_else(|| invalid("aggregate-state", "Expected a timestamp extremum input"))
        }
        LogicalType::Scalar(ScalarType::Time { unit, .. } | ScalarType::Duration(unit)) => {
            let source = match cell.field.data_type() {
                DataType::Time32(source) | DataType::Time64(source) | DataType::Duration(source) => time_unit(source),
                _ => return Err(invalid("aggregate-state", "Expected a time or duration input")),
            };
            let ticks = temporal_ticks(cell.array, cell.row)?;
            if source == *unit { Ok(ticks) } else { rtdb_expr::rescale_units(ticks, source, *unit) }
        }
        _ => Err(invalid("aggregate-state", "Expected a temporal extremum output")),
    }
}

fn temporal_ticks(array: &dyn Array, row: usize) -> Result<i64> {
    use ArrowTimeUnit as T;
    macro_rules! ticks {
        ($ty:ty) => { array.as_primitive_opt::<$ty>().map(|array| i64::from(array.value(row))) };
    }
    let value = match array.data_type() {
        DataType::Date32 => ticks!(Date32Type),
        DataType::Date64 => ticks!(Date64Type),
        DataType::Time32(T::Second) => ticks!(Time32SecondType),
        DataType::Time32(T::Millisecond) => ticks!(Time32MillisecondType),
        DataType::Time64(T::Microsecond) => ticks!(Time64MicrosecondType),
        DataType::Time64(T::Nanosecond) => ticks!(Time64NanosecondType),
        DataType::Duration(T::Second) => ticks!(DurationSecondType),
        DataType::Duration(T::Millisecond) => ticks!(DurationMillisecondType),
        DataType::Duration(T::Microsecond) => ticks!(DurationMicrosecondType),
        DataType::Duration(T::Nanosecond) => ticks!(DurationNanosecondType),
        _ => None,
    };
    value.ok_or_else(|| rtdb_types::Anomaly::incorrect("rtdb/array-layout", "Arrow array and logical field layout disagree"))
}

impl AggregateCursor {
    fn accepts_typed_aggregates(&self, batch: &ColumnBatch) -> Result<bool> {
        use AggregateFunction as A;
        let types = self.runtime.aggregates()?;
        let float = LogicalType::Scalar(ScalarType::F64);
        for (aggregate,types) in self.aggregates.iter().zip(types) {
            if aggregate.filter.is_some() || aggregate.distinct { return Ok(false); }
            let argument = aggregate.argument.as_ref().map(|argument|argument_position(argument,&self.schema)).transpose()?;
            let fits = match (&aggregate.function,argument) {
                (A::CountStar,_) => true,
                (A::Count,Some(position)) => accepts_typed_column(&batch.columns[position], count_layout),
                (A::Sum | A::Average,Some(position)) =>
                    without_nullability(&types.input) == &float && without_nullability(types.output_type()) == &float
                        && accepts_typed_column(&batch.columns[position], |ty| *ty == DataType::Float64),
                _ => false,
            };
            if !fits { return Ok(false); }
        }
        Ok(true)
    }

    fn reduce_typed(&self, batch: &ColumnBatch, mapping: GroupMapping<'_>, group_count: usize, columns: &mut [AggregateColumn]) -> Result<()> {
        use AggregateFunction as A;
        let types = self.runtime.aggregates()?;
        let float = LogicalType::Scalar(ScalarType::F64);
        for ((column,aggregate),types) in columns.iter_mut().zip(&self.aggregates).zip(types) {
            validate_aggregate_input(aggregate,types)?;
            let argument = aggregate.argument.as_ref().map(|argument|argument_position(argument,&self.schema)).transpose()?;
            match (&aggregate.function, argument, &mut column.accumulation) {
                (A::CountStar, _, Accumulation::Counts(counts)) => {
                    counts.ensure(group_count)?;
                    for (row, group) in mapping.iter().enumerate() {
                        self.execution.context.check_cancelled_at(row)?;
                        counts.values[group] = counts.values[group].wrapping_add(1);
                    }
                }
                (A::Count, Some(position), Accumulation::Counts(counts)) => {
                    counts.ensure(group_count)?;
                    let column = &batch.columns[position];
                    let mut cells = present_column_cells(column);
                    let direct = match column {
                        Column::Arrow { array, .. } if array.len() >= batch.len && count_layout(array.data_type()) => Some(array.as_ref()),
                        _ => None,
                    };
                    for (row, group) in mapping.iter().enumerate() {
                        self.execution.context.check_cancelled_at(row)?;
                        let present = match direct {
                            Some(array) => !array.is_null(row),
                            None => cells.next().expect("The reader reports missing cells")?.is_some(),
                        };
                        if present { counts.values[group] = counts.values[group].wrapping_add(1); }
                    }
                }
                (A::Sum | A::Average, Some(position), Accumulation::Scalar(states)) => {
                    states.ensure(group_count)?;
                    let column = &batch.columns[position];
                    let mut cells = present_column_cells(column);
                    let direct = match column {
                        Column::Arrow { array, .. } if array.len() >= batch.len =>
                            array.as_any().downcast_ref::<arrow_array::Float64Array>(),
                        _ => None,
                    };
                    let average = aggregate.function == A::Average;
                    for (row, group) in mapping.iter().enumerate() {
                        self.execution.context.check_cancelled_at(row)?;
                        let value = match direct {
                            Some(array) => (!array.is_null(row)).then(|| array.value(row)),
                            None => cells.next().expect("The reader reports missing cells")?
                                .map(|(array, offset)| array.as_primitive::<Float64Type>().value(offset)),
                        };
                        let Some(value) = value else { continue; };
                        let state = &mut states.values[group];
                        state.add_numeric(NumericCell::Float64(value), &float)?;
                        if average { state.count = state.count.wrapping_add(1); }
                    }
                }
                _ => return Err(invalid("aggregate-input","A typed aggregate has no typed accumulator")),
            }
        }
        Ok(())
    }
}

struct AggregateColumn { accumulation: Accumulation, distinct: Option<DistinctSeen> }
/// The distinct arguments each group has taken so far, by the group and the hash of the argument.
enum DistinctSeen {
    FirstBatch,
    Rows { index: HashMap<(usize, u32), Vec<usize>>, values: Vec<OwnedRow> },
    Columns(Box<BatchKeyIndex>),
}

impl DistinctSeen {
    fn select(
        &mut self, input: Rows<'_>, mapping: GroupMapping<'_>, mask: &mut [bool], argument: Option<usize>,
        types: &AggregateTypes, execution: &Execution,
    ) -> Result<()> {
        if matches!(self, Self::FirstBatch) {
            *self = match (input, argument) {
                (_, Some(_)) => Self::Columns(Box::new(BatchKeyIndex::vector_hashing(&execution.context)?)),
                _ => Self::Rows { index: HashMap::new(), values: Vec::new() },
            };
        }
        let mut retained_source = None;
        let mut admitted = false;
        let positions = [argument.unwrap_or_default()];
        let mut typed = None;
        for (row, (group, selected)) in mapping.iter().zip(mask).enumerate() {
            execution.context.check_cancelled()?;
            if !*selected { continue; }
            if !admitted {
                rtdb_plan::validate_equality_types(&types.input, &types.input)?;
                if argument.is_some() {
                    match KeyColumns::projection(input.batch, &positions).bind_typed(&execution.context) {
                        Ok(readers) => typed = readers,
                        Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => {},
                        Err(error) => return Err(error),
                    }
                }
                admitted = true;
            }
            match self {
                Self::Columns(index) => {
                    let (Rows { batch, projected, .. }, Some(argument)) = (input, argument) else {
                        return Err(invalid("aggregate-input", "A distinct batch argument is absent"));
                    };
                    *selected = index.insert_distinct_argument(batch, argument, row, group, projected, typed.as_ref(), &mut retained_source, &execution.context)?;
                }
                Self::Rows { index, values } => {
                    let cell = input.value(row, argument)?;
                    let value = cell.as_ref();
                    let hash = crate::join_storage::hash_values(std::iter::once(value))?;
                    let candidates = index.entry((group, hash)).or_default();
                    for &candidate in candidates.iter() {
                        let allocation = execution.scope()?;
                        if sql_eq_in_timezone(value, &values[candidate].values[0], &execution.context.timezone, &allocation)? == Some(true) {
                            *selected = false;
                            break;
                        }
                    }
                    if *selected {
                        candidates.push(values.len());
                        values.push(OwnedRow::copy_parts(&[std::slice::from_ref(value)], 0, 0, &execution.context.memory)?);
                    }
                }
                Self::FirstBatch => unreachable!(),
            }
        }
        if let Self::Columns(index) = self {
            index.finish_distinct_argument(retained_source, &execution.context)?;
        }
        Ok(())
    }
}

enum AggregateMask {
    All,
    Bitmap(PredicateSelection),
    Flags(Vec<bool>),
}

impl AggregateMask {
    fn contains(&self, row: usize) -> bool {
        match self {
            Self::All => true,
            Self::Bitmap(selection) => selection.contains(row),
            Self::Flags(flags) => flags[row],
        }
    }

    fn flags(&mut self, len: usize, scope: &rtdb_expr::EvaluationScope) -> Result<&mut [bool]> {
        if !matches!(self, Self::Flags(_)) {
            scope.reserve(len)?;
            *self = Self::Flags((0..len).map(|row| self.contains(row)).collect());
        }
        let Self::Flags(flags) = self else { unreachable!("The aggregate mask has mutable flags"); };
        Ok(flags)
    }
}

enum Accumulation { Counts(GroupStates<i64>), Scalar(ScalarGroups), Vectors(CollectionVectors) }
impl AggregateColumn {
    fn new(aggregate: &AggregateExpr,schema: &Schema,execution: &Execution) -> Result<Self> {
        let accumulation = match aggregate.function {
            AggregateFunction::Count | AggregateFunction::CountStar =>
                Accumulation::Counts(GroupStates::new(execution)?),
            AggregateFunction::ArrayAgg | AggregateFunction::VecAgg | AggregateFunction::Percentile { .. } => {
                let order = CollectionOrder::new(aggregate, schema)?;
                Accumulation::Vectors(CollectionVectors::new(&execution.context.memory, order)?)
            }
            _ => Accumulation::Scalar(ScalarGroups::new(execution)?),
        };
        let distinct = if aggregate.distinct && !matches!(aggregate.function,AggregateFunction::Min | AggregateFunction::Max) { Some(DistinctSeen::FirstBatch) } else { None };
        Ok(Self { accumulation,distinct })
    }
    async fn aggregate(&mut self,input: Rows<'_>,mapping: GroupMapping<'_>,group_count: usize,aggregate: &AggregateExpr,types: &AggregateTypes,schema: &Schema,execution: &Execution) -> Result<()> {
        let scope = execution.scope()?;
        let mut mask = AggregateMask::All;
        if let Some(filter) = &aggregate.filter {
            execution.context.check_cancelled()?;
            if let Some(predicate) = crate::columnar::functions::Predicate::bind(filter, schema)
                && let Some(selection) = predicate.selection(input.batch, execution.parameters.evaluator(), &execution.context.memory, || {
                    execution.prepare_expression(filter, schema, &scope).map(|_| ())
                })?
            {
                mask = AggregateMask::Bitmap(selection);
            } else {
                let invocation = execution.prepare_expression(filter, schema, &scope)?;
                for (row, selected) in mask.flags(input.len(), &scope)?.iter_mut().enumerate() {
                    let row_scope = execution.scope()?;
                    let input = crate::columnar::BatchRow { batch: input.batch, row, admit_dates: false };
                    let values = rtdb_expr::ExpressionRow::Borrowed(&input);
                    *selected = is_true(&execution.evaluate_invocation_row(&row_scope, &invocation, values).await?)?;
                }
            }
        }
        let argument = aggregate.argument.as_ref().map(|argument|argument_position(argument,schema)).transpose()?;
        if let Some(seen) = &mut self.distinct {
            seen.select(input, mapping, mask.flags(input.len(), &scope)?, argument, types, execution)?;
        }
        validate_aggregate_input(aggregate,types)?;
        match &mut self.accumulation {
            Accumulation::Counts(counts) => {
                counts.ensure(group_count)?;
                let all = aggregate.function == AggregateFunction::CountStar;
                let typed = input.arrow_column(argument).filter(|column| accepts_masked_count(column));
                for (row, group) in mapping.iter().enumerate() {
                    execution.context.check_cancelled_at(row)?;
                    if mask.contains(row) && (all || count_is_present(input, typed, row, argument)?) {
                        counts.values[group] = counts.values[group].wrapping_add(1);
                    }
                }
            }
            Accumulation::Scalar(states) => {
                states.aggregate(input,mapping,|row| mask.contains(row),group_count,argument,aggregate,types,&scope,execution)?;
            }
            Accumulation::Vectors(vectors) => {
                let Rows { batch, projected, .. } = input;
                let argument = argument.ok_or_else(|| invalid("aggregate-state", "Collected aggregate argument is absent"))?;
                if matches!(aggregate.function, AggregateFunction::Percentile { .. }) {
                    let typed = input.arrow_column(Some(argument)).filter(|column| accepts_masked_count(column));
                    for (row, selected) in mask.flags(input.len(), &scope)?.iter_mut().enumerate() {
                        execution.context.check_cancelled_at(row)?;
                        if *selected { *selected = count_is_present(input, typed, row, Some(argument))?; }
                    }
                }
                vectors.append(batch, argument, mapping, |row| mask.contains(row), group_count, projected, &execution.context)?;
            }
        }
        Ok(())
    }
}

fn validate_collection_order(aggregate: &AggregateExpr, types: &AggregateTypes, schema: &Schema, execution: &Execution) -> Result<()> {
    if matches!(aggregate.function, AggregateFunction::Percentile { .. }) {
        let declared = aggregate.argument.as_ref().map_or(LogicalType::Null, rtdb_plan::Expr::data_type);
        rtdb_expr::validate_order_type(&declared.join(&types.input))?;
    }
    if !aggregate.order_by.is_empty() {
        let scope = execution.scope()?;
        for order in &aggregate.order_by {
            rtdb_expr::validate_order_type(&execution.expression_type(&scope, &order.expr, schema)?)?;
        }
    }
    Ok(())
}

struct CollectionVectors {
    batches: Vec<ColumnBatch>,
    groups: Vec<Vec<(usize, usize)>>,
    selected: usize,
    present: bool,
    reservation: Arc<rtdb_columnar::MemoryReservation>,
    order: Option<CollectionOrder>,
    projected: bool,
}

impl CollectionVectors {
    fn new(memory: &Arc<rtdb_columnar::MemoryBudget>, order: Option<CollectionOrder>) -> Result<Self> {
        Ok(Self { batches: Vec::new(), groups: Vec::new(), selected: 0, present: false, reservation: memory.reserve(4096)?, order, projected: false })
    }

    fn append(&mut self, batch: &ColumnBatch, position: usize, mapping: GroupMapping<'_>, selected: impl Fn(usize) -> bool + Copy, groups: usize, projected: bool, context: &crate::QueryContext) -> Result<()> {
        self.present |= batch.len != 0;
        self.projected |= projected;
        if let Some(order) = &self.order {
            if batch.len == 0 { return Ok(()); }
            let batch = order.page(batch, mapping, selected, context)?;
            self.account_pages(self.selected, groups)?;
            self.batches.push(batch);
            return Ok(());
        }
        self.collect(batch, position, mapping.iter().enumerate()
            .filter_map(move |(row, group)| selected(row).then_some((row, group))), groups, context)
    }

    fn collect(&mut self, batch: &ColumnBatch, position: usize, picks: impl Iterator<Item = (usize, usize)> + Clone, groups: usize, context: &crate::QueryContext) -> Result<()> {
        let count = picks.clone().count();
        if count == 0 { return Ok(()); }
        self.admit_positions(count, groups)?;
        let page = self.batches.len();
        self.batches.push(batch.key_projection(&[position], &context.memory)?);
        for (row, group) in picks {
            self.push_position(group, page, row)?;
        }
        Ok(())
    }

    fn admit_positions(&mut self, count: usize, groups: usize) -> Result<()> {
        self.selected = self.selected.checked_add(count).filter(|count| *count <= i32::MAX as usize)
            .ok_or_else(|| invalid("aggregate-size", "List aggregate result exceeds signed Arrow offsets"))?;
        self.account_pages(self.selected, groups)?;
        self.groups.resize_with(groups, Vec::new);
        Ok(())
    }

    fn push_position(&mut self, group: usize, page: usize, row: usize) -> Result<()> {
        self.groups.get_mut(group).ok_or_else(|| invalid("aggregate-group", "List aggregate group is absent"))?.push((page, row));
        Ok(())
    }

    fn account_pages(&mut self, positions: usize, groups: usize) -> Result<()> {
        let bytes = positions.checked_mul(4 * std::mem::size_of::<(usize, usize)>())
            .and_then(|bytes| bytes.checked_add(groups.checked_mul(96)?))
            .and_then(|bytes| bytes.checked_add((self.batches.len() + 1).checked_mul(4 * std::mem::size_of::<ColumnBatch>())?))
            .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("aggregate-size", "List aggregate allocation overflow"))?;
        Arc::get_mut(&mut self.reservation).expect("The list aggregate owns its result positions").grow_to(bytes)
    }

    async fn finish(mut self, groups: usize, aggregate: &AggregateExpr, output: &rtdb_plan::Field,
        execution: &Execution) -> Result<(Column, Arc<rtdb_columnar::MemoryReservation>)> {
        if let AggregateFunction::Percentile { kind, fraction: PercentileFraction::Literal(fraction), descending } = aggregate.function {
            let input = aggregate.argument.as_ref().map_or(LogicalType::Nothing, rtdb_plan::Expr::data_type);
            return self.percentiles(groups, kind, fraction, descending, input, output, execution).await;
        }
        if let Some(order) = self.order.take() {
            self = order.sort(self, groups, &aggregate.order_by, execution).await?;
        }
        let context = &execution.context;
        let memory = &context.memory;
        let base = groups.checked_mul(64).and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(|| invalid("aggregate-size", "List aggregate allocation overflow"))?;
        let _scratch = memory.reserve(base)?;
        let mut offsets = Vec::with_capacity(groups + 1);
        let mut valid = Vec::with_capacity(groups);
        let mut end = 0i32;
        offsets.push(0);
        for group in 0..groups {
            context.check_cancelled_at(group)?;
            let rows = self.groups.get(group).map_or(&[][..], Vec::as_slice);
            end += rows.len() as i32;
            offsets.push(end);
            valid.push(!rows.is_empty() || (aggregate.function == AggregateFunction::VecAgg && !self.present));
        }
        let picks = self.groups.iter().flatten().copied();
        ColumnBatch::collect_lists_from_rows(&self.batches, picks, self.selected, offsets, valid, output, memory)
    }

    async fn percentiles(self, groups: usize, kind: PercentileKind, fraction: f64, descending: bool, input_type: LogicalType,
        output: &rtdb_plan::Field, execution: &Execution) -> Result<(Column, Arc<rtdb_columnar::MemoryReservation>)> {
        use crate::scalar_key::ScalarKey;
        let context = &execution.context;
        let memory = &context.memory;
        let bytes = groups.checked_mul(2 * std::mem::size_of::<Value>()).and_then(|bytes| bytes.checked_add(512))
            .ok_or_else(|| invalid("aggregate-size", "Percentile results exceed addressable memory"))?;
        let mut reservation = memory.reserve(bytes)?;
        let typed = PercentileKey::bind(&input_type, &self.batches,
            Arc::get_mut(&mut reservation).expect("The percentile aggregate owns its result reservation"))?;
        let mut compatible = typed.is_none().then(|| PercentileKey::compatible(&input_type, &self.batches)).flatten();
        if matches!(compatible, Some(PercentileKey::Number)) {
            let lease = Arc::get_mut(&mut reservation).expect("The percentile aggregate owns its result reservation");
            let bytes = lease.bytes().checked_add(4096)
                .ok_or_else(|| invalid("aggregate-size", "Percentile decimal conversion exceeds addressable memory"))?;
            match lease.grow_to(bytes) {
                Ok(()) => {},
                Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => compatible = None,
                Err(error) => return Err(error),
            }
        }
        let comparison_key = typed.as_ref().map(|(key, _)| key).or(compatible.as_ref());
        let copy_typed = kind == PercentileKind::Discrete && typed.is_some()
            && without_nullability(&input_type) == without_nullability(&output.data_type);
        let direct_gather = kind == PercentileKind::Discrete && direct_rank_type(&output.data_type);
        let borrow_winners = kind == PercentileKind::Discrete && !copy_typed && !direct_gather
            && admits_discrete_winners(&output.data_type, &self.batches);
        let gather = copy_typed || borrow_winners || direct_gather;
        let mut values = Vec::with_capacity(if kind == PercentileKind::Discrete && !gather { groups } else { 0 });
        let mut picks = Vec::with_capacity(if gather { groups } else { 0 });
        let mut interpolated = Vec::with_capacity(if kind == PercentileKind::Continuous { groups } else { 0 });
        let batches = Arc::new(self.batches);
        let mut positions = self.groups.into_iter();
        let read = |(batch, row): (usize, usize)| Rows { batch: &batches[batch], projected: self.projected, memory }.value(row, Some(0));
        let source = |(batch, row): (usize, usize)| (&batches[batch].columns[0], row);
        let read_number = |position: (usize, usize)| -> Result<f64> {
            if let Some((PercentileKey::Decimal, _)) = &typed {
                return PercentileKey::decimal(source(position));
            }
            if matches!(&compatible, Some(PercentileKey::Number | PercentileKey::Native(NativeKeyKind::Number))) {
                return Ok(PercentileKey::number(source(position))?.as_f64());
            }
            let (column, row) = source(position);
            match ScalarKey::read(column, row) {
                Some(ScalarKey::Signed(value)) => return Ok(value as f64),
                Some(ScalarKey::Float(value)) => return Ok(value),
                _ => {}
            }
            let cell = read(position)?;
            number(cell.as_ref()).ok_or_else(|| invalid("percentile-input", "Continuous percentile requires numeric values"))
        };
        for group in 0..groups {
            context.check_cancelled_at(group)?;
            let rows = positions.next().unwrap_or_default();
            if rows.is_empty() {
                if gather {
                    picks.push(None);
                } else if kind == PercentileKind::Continuous {
                    interpolated.push(None);
                } else {
                    values.push(Value::Null);
                }
                continue;
            }
            let (low, high, weight) = percentile_ranks(kind, fraction, rows.len() as i32)?;
            let (lower, upper) = if rows.len().saturating_mul(std::mem::size_of::<usize>()) > context.options.sort_memory_bytes {
                let input = PercentileInput {
                    batches: Arc::clone(&batches),
                    rows: Arc::new(rows),
                    context: Arc::clone(context),
                    start: 0,
                    _reservation: Arc::clone(&self.reservation),
                };
                Self::spilled_ranks(input, low as u64, high as u64, descending, &input_type, self.projected, execution).await?
            } else {
                let key = |(batch, row): (usize, usize)| ScalarKey::read(&batches[batch].columns[0], row);
                let indices = crate::sort::sort_ordinals(rows.len(), context, |left, right| {
                    let (left, right) = if descending { (rows[right], rows[left]) } else { (rows[left], rows[right]) };
                    if let Some(key) = comparison_key {
                        return key.compare(source(left), source(right));
                    }
                    if let (Some(left), Some(right)) = (key(left), key(right))
                        && let Some(order) = left.ordered_compare(right) { return Ok(order); }
                    rtdb_expr::ordered_compare_in_timezone(read(left)?.as_ref(), read(right)?.as_ref(), false, &context.timezone)
                })?;
                (rows[indices.values[low as usize]], rows[indices.values[high as usize]])
            };
            if gather {
                if copy_typed || borrow_winners {
                    context.check_cancelled()?;
                    if borrow_winners || matches!(&typed, Some((PercentileKey::Decimal, _))) {
                        ranked_arrow_cell(source(lower)).validate_decimal_precision()?;
                    }
                }
                picks.push(Some(lower));
                continue;
            }
            if kind == PercentileKind::Continuous {
                let lower = read_number(lower)?;
                let value = if low == high {
                    lower
                } else {
                    let upper = read_number(upper)?;
                    lower + weight * (upper - lower)
                };
                interpolated.push(Some(value));
                continue;
            }
            let scope = execution.scope()?;
            let lower = read(lower)?;
            let value = lower.as_ref();
            scope.reserve(value.retained_bytes()?.checked_mul(2).ok_or_else(|| invalid("aggregate-size", "Percentile output overflow"))?)?;
            let value = execution.evaluate(&scope, &rtdb_plan::Expr::Literal(value.clone()), &Schema::default(), &[]).await?;
            let lease = Arc::get_mut(&mut reservation).expect("The percentile aggregate owns its result reservation");
            lease.grow_to(lease.bytes().checked_add(value.retained_bytes()?).ok_or_else(|| invalid("aggregate-size", "Percentile output overflow"))?)?;
            values.push(value);
        }
        if copy_typed {
            let (_, field) = typed.expect("A typed percentile result has a field");
            return copy_ranked_cells(&batches, &picks, field, memory);
        }
        if gather {
            return crate::columnar::gather_nullable_column(|batch| &batches[batch], &picks, 0, memory);
        }
        if kind == PercentileKind::Continuous {
            return double_results(interpolated, reservation);
        }
        crate::columnar::pack_evaluated_column(&values, memory)
    }

    async fn spilled_ranks(input: PercentileInput, low: u64, high: u64,
        descending: bool, ty: &LogicalType, projected: bool, execution: &Execution) -> Result<((usize, usize), (usize, usize))> {
        use crate::sort::batches::BatchSort;
        use rtdb_plan::SortExpr;

        let mut binder = Binder::default();
        let key = binder.field("value", None, ty.clone())?;
        let ordinal = binder.field("position", None, LogicalType::Scalar(ScalarType::I64))?;
        let schema = Schema::new(vec![key.clone(), ordinal])?;
        let rows = Arc::clone(&input.rows);
        let order = vec![SortExpr { expr: key.expression(), descending, nulls_first: false }];
        let mut sorted = BatchSort::new(Box::new(input), schema, order, execution.clone(), projected, low, high + 1)?;
        let mut first = None;
        let mut last = None;
        while let Some(batch) = sorted.next_batch().await? {
            for row in 0..batch.len {
                let Value::I64(ordinal) = batch.columns[1].value(row)? else {
                    return Err(invalid("percentile-state", "Sorted percentile position is absent"));
                };
                let value = rows.get(ordinal as usize).copied()
                    .ok_or_else(|| invalid("percentile-state", "Sorted percentile position is outside its group"))?;
                first.get_or_insert(value);
                last = Some(value);
            }
        }
        first.zip(last).ok_or_else(|| invalid("percentile-state", "Sorted percentile has no ranks"))
    }
}

struct PercentileInput {
    batches: Arc<Vec<ColumnBatch>>,
    rows: Arc<Vec<(usize, usize)>>,
    context: Arc<crate::QueryContext>,
    start: usize,
    _reservation: Arc<MemoryReservation>,
}

#[async_trait]
impl BatchCursor for PercentileInput {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        self.context.check_cancelled()?;
        if self.start == self.rows.len() {
            return Ok(None);
        }
        let bytes_per_row = size_of::<(usize, usize)>() + size_of::<i64>();
        let run_rows = (self.context.options.sort_memory_bytes / bytes_per_row).clamp(1, 4096);
        let end = (self.start + run_rows).min(self.rows.len());
        let memory = &self.context.memory;
        let rows = self.rows[self.start..end].iter().copied().map(Some);
        let (column, charge) = crate::columnar::gather_nullable_column_iter(|page| &self.batches[page], rows, 0, memory)?;
        let mut batch = ColumnBatch::settled(vec![column], end - self.start, charge)?;
        let (column, charge) = crate::columnar::ordinal_column(batch.len, self.start as i64, "position", &self.context)?;
        batch.append_column(column, charge, memory)?;
        self.start = end;
        Ok(Some(batch))
    }
}

struct CollectionOrder {
    positions: Vec<usize>,
    schema: Schema,
    group: rtdb_plan::Field,
}

impl CollectionOrder {
    fn new(aggregate: &AggregateExpr, input: &Schema) -> Result<Option<Self>> {
        if aggregate.order_by.is_empty() || aggregate.function != AggregateFunction::ArrayAgg { return Ok(None); }
        let argument = aggregate.argument.as_ref().ok_or_else(|| invalid("aggregate-state", "Ordered aggregate argument is absent"))?;
        let argument = argument_position(argument, input)?;
        let mut positions = vec![argument];
        for order in &aggregate.order_by {
            let position = argument_position(&order.expr, input)?;
            if !positions.contains(&position) { positions.push(position); }
        }
        let group = Binder::after(input)?.field("_aggregate_group", None, LogicalType::Scalar(ScalarType::I64).maybe())?;
        let schema = Schema::new(positions.iter().map(|&position| input.fields()[position].clone())
            .chain(std::iter::once(group.clone())).collect())?;
        Ok(Some(Self { positions, schema, group }))
    }

    fn page(&self, batch: &ColumnBatch, mapping: GroupMapping<'_>, selected: impl Fn(usize) -> bool, context: &crate::QueryContext) -> Result<ColumnBatch> {
        let memory = &context.memory;
        let mut output = batch.key_projection(&self.positions, memory)?;
        let bytes = batch.len.checked_mul(16).and_then(|bytes| bytes.checked_add(512))
            .ok_or_else(|| invalid("aggregate-size", "Ordered list aggregate group vector overflow"))?;
        let reservation = memory.reserve(bytes)?;
        let mut groups = arrow_array::builder::Int64Builder::with_capacity(batch.len);
        for (row, group) in mapping.iter().enumerate() {
            context.check_cancelled_at(row)?;
            if selected(row) { groups.append_value(group as i64); } else { groups.append_null(); }
        }
        let logical = self.group.data_type.clone();
        let field = Arc::new(rtdb_columnar::to_field(&self.group.name, &logical)?);
        output.append_column(Column::Arrow { array: Arc::new(groups.finish()), field, logical: Some(logical) }, reservation, memory)?;
        Ok(output)
    }

    async fn sort(self, input: CollectionVectors, groups: usize, order: &[rtdb_plan::SortExpr], execution: &Execution) -> Result<CollectionVectors> {
        use crate::{scalar_key::ScalarKey, sort::batches::BatchSort};
        let context = &execution.context;
        let memory = &context.memory;
        let group = self.positions.len();
        let mut output = CollectionVectors::new(memory, None)?;
        output.present = input.present;
        let source = Box::new(CollectionInput { batches: input.batches.into_iter(), _reservation: input.reservation });
        let mut sorted = BatchSort::new(source, self.schema, order.to_vec(), execution.clone(), input.projected, 0, i64::MAX as u64)?.in_memory();
        while let Some(batch) = sorted.next_batch().await? {
            let group_at = |row| -> Result<Option<usize>> {
                Ok(match ScalarKey::read(&batch.columns[group], row) {
                    Some(ScalarKey::Signed(value)) => Some(value as usize),
                    Some(ScalarKey::Null) => None,
                    _ => match batch.columns[group].value(row)? {
                        Value::I64(value) => Some(value as usize),
                        Value::Null | Value::Undefined => None,
                        _ => return Err(invalid("aggregate-group", "Expected a group index for an ordered aggregate")),
                    },
                })
            };
            let (span, count) = if let Column::Arrow { array, .. } = &batch.columns[group]
                && array.data_type() == &DataType::Int64 && array.null_count() == 0
            {
                context.check_cancelled()?;
                (0..batch.len, batch.len)
            } else {
                let mut span = 0..0;
                let mut count = 0;
                for row in 0..batch.len {
                    context.check_cancelled_at(row)?;
                    if group_at(row)?.is_some() {
                        if count == 0 { span.start = row; }
                        span.end = row + 1;
                        count += 1;
                    }
                }
                (span, count)
            };
            if count == 0 { continue; }
            output.admit_positions(count, groups)?;
            let page = output.batches.len();
            let retained = if span.len() == count { batch.retain_ordered_list_span(span.clone(), memory)? } else { None };
            let _positions = if retained.is_none() {
                Some(memory.reserve(count.checked_mul(size_of::<(usize, usize)>())
                    .ok_or_else(|| invalid("aggregate-size", "Ordered list positions overflow"))?)?)
            } else {
                None
            };
            let mut rows = if retained.is_none() { Vec::with_capacity(count) } else { Vec::new() };
            let mut offset = 0;
            for row in span {
                context.check_cancelled_at(row)?;
                if let Some(group) = group_at(row)? {
                    output.push_position(group, page, offset)?;
                    if retained.is_none() { rows.push((0, row)); }
                    offset += 1;
                }
            }
            let arguments = match retained {
                Some(arguments) => arguments,
                None => batch.compact_group_key_columns(&rows, Some(&[0]), memory)?,
            };
            output.batches.push(arguments);
        }
        Ok(output)
    }
}

struct CollectionInput {
    batches: std::vec::IntoIter<ColumnBatch>,
    _reservation: Arc<rtdb_columnar::MemoryReservation>,
}

#[async_trait]
impl BatchCursor for CollectionInput {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> { Ok(self.batches.next()) }
}

pub(crate) fn project_group_keys(
    input: crate::columnar::BoxBatchCursor, schema: Schema, groups: &[NamedExpr], execution: &Execution,
) -> Result<(crate::columnar::BoxBatchCursor, Schema, Vec<NamedExpr>)> {
    use rtdb_plan::Expr;

    let mut fields = schema.fields().to_vec();
    let mut expressions = fields.iter().map(rtdb_plan::Field::expression).collect::<Vec<_>>();
    let mut binder = Binder::after(&schema)?;
    let scope = execution.scope()?;
    let mut project = |expression: &mut Expr| -> Result<()> {
        if matches!(expression, Expr::Column { .. }) { return Ok(()); }
        let ty = execution.expression_type(&scope, expression, &schema)?;
        let field = binder.field("_aggregate_input", None, ty)?;
        expressions.push(std::mem::replace(expression, field.expression()));
        fields.push(field);
        Ok(())
    };
    let mut groups = groups.to_vec();
    for group in &mut groups { project(&mut group.expr)?; }
    if fields.len() == schema.len() { return Ok((input, schema, groups)); }
    let output = Schema::new(fields)?;
    let input = crate::columnar::BatchExpressions::new(input, schema, expressions, execution.clone()).validating_input(true);
    Ok((Box::new(input), output, groups))
}

fn argument_position(argument: &rtdb_plan::Expr,schema: &Schema) -> Result<usize> {
    match argument { rtdb_plan::Expr::Column { id,.. } => schema.position(*id).ok_or_else(||invalid("aggregate-column","Aggregate argument column is absent")),_ => Err(invalid("aggregate-column","Aggregate argument must be projected before aggregation")) }
}

#[derive(Clone, Copy)]
enum NumericCell {
    Integer(i64),
    Float32(f32),
    Float64(f64),
}

impl NumericCell {
    fn read_decimal_f32(cell: ArrowCell<'_>) -> Result<Self> {
        use std::io::Write;

        let (parameters, mut coefficient) = cell.decimal_coefficient()?.ok_or_else(sum_state)?;
        let start = coefficient.len() - usize::from(parameters.bit_width / 8);
        let sign = if coefficient[start] & 0x80 == 0 { 0 } else { 0xff };
        coefficient[..start].fill(sign);
        // The spelling fits 77 coefficient digits, punctuation, and a signed scale exponent.
        const TEXT_CAPACITY: usize = 96;
        let mut text = [0; TEXT_CAPACITY];
        let mut tail = text.as_mut_slice();
        write!(tail, "{}", rtdb_types::jvm_number::decimal_coefficient_display(coefficient, parameters.scale))
            .map_err(|_| sum_state())?;
        let len = TEXT_CAPACITY - tail.len();
        let decimal = std::str::from_utf8(&text[..len]).map_err(|_| sum_state())?;
        Ok(Self::Float32(decimal.parse().map_err(|_| sum_state())?))
    }

    fn read_number_arrow(cell: ArrowCell<'_>) -> Result<Self> {
        if decimal_storage(cell.field.data_type()) {
            return Ok(Self::Float64(rtdb_columnar::decimal_cell_as_f64(cell.field, cell.array, cell.row)?));
        }
        Self::read_arrow(cell)
    }

    fn read_arrow(cell: ArrowCell<'_>) -> Result<Self> {
        Ok(match cell.field.data_type() {
            DataType::Float32 => Self::Float32(cell.array.as_primitive::<Float32Type>().value(cell.row)),
            DataType::Float64 => Self::Float64(cell.array.as_primitive::<Float64Type>().value(cell.row)),
            _ => Self::Integer(cell.signed_integer().ok_or_else(sum_state)?),
        })
    }

    fn as_f32(self) -> f32 {
        match self {
            Self::Integer(value) => value as f32,
            Self::Float32(value) => value,
            Self::Float64(value) => value as f32,
        }
    }

    fn as_f64(self) -> f64 {
        match self {
            Self::Integer(value) => value as f64,
            Self::Float32(value) => f64::from(value),
            Self::Float64(value) => value,
        }
    }

    fn key(self) -> crate::scalar_key::ScalarKey<'static> {
        match self {
            Self::Integer(value) => crate::scalar_key::ScalarKey::Signed(value),
            float => crate::scalar_key::ScalarKey::Float(float.as_f64()),
        }
    }
}

struct NumericColumn<'a>(&'a Column);

impl<'a> NumericColumn<'a> {
    fn admits_leaf(field: &ArrowField, logical: Option<&LogicalType>, expected: Option<&LogicalType>) -> bool {
        use LogicalType::Scalar;
        use ScalarType::{I16, I32, I64};

        if !field.metadata().is_empty() { return false; }
        let physical = match field.data_type() {
            DataType::Null => return field.is_nullable() && logical.is_none_or(|ty| *ty == LogicalType::Null),
            DataType::Int8 => ScalarType::I8,
            DataType::Int16 => ScalarType::I16,
            DataType::Int32 => ScalarType::I32,
            DataType::Int64 => ScalarType::I64,
            DataType::UInt8 => ScalarType::U8,
            DataType::UInt16 => ScalarType::U16,
            DataType::UInt32 => ScalarType::U32,
            DataType::UInt64 => ScalarType::U64,
            DataType::Float32 => ScalarType::F32,
            DataType::Float64 => ScalarType::F64,
            _ => return false,
        };
        let physical = Scalar(physical);
        logical.is_none_or(|ty| without_nullability(ty) == &physical)
            && expected.is_none_or(|ty| {
                let ty = without_nullability(ty);
                ty == &physical || matches!((field.data_type(), ty),
                    (DataType::Int8, Scalar(I16))
                    | (DataType::Int8 | DataType::Int16, Scalar(I32))
                    | (DataType::Int8 | DataType::Int16 | DataType::Int32, Scalar(I64)))
            })
    }

    fn bind(column: &'a Column, expected: Option<&LogicalType>) -> Option<Self> {
        admits_reduction_column(column, |field, logical| {
            Self::admits_leaf(field, logical, expected) || (expected.is_none() && admits_dense_numeric(field, logical))
        }).then_some(Self(column))
    }

    fn get(&self, row: usize) -> Result<Option<NumericCell>> {
        present_column_position(self.0, row)?.map(NumericCell::read_arrow).transpose()
    }
}

struct GroupStates<T> {
    values: Vec<T>,
    state: Arc<rtdb_columnar::MemoryBudget>,
    reservation: Arc<MemoryReservation>,
}

impl<T: Default> GroupStates<T> {
    fn new(execution: &Execution) -> Result<Self> {
        let state = Arc::clone(&execution.context.memory);
        let reservation = state.reserve(0)?;
        Ok(Self { values: Vec::new(), state, reservation })
    }
    fn ensure(&mut self,count: usize) -> Result<()> {
        if self.values.len() >= count { return Ok(()); }
        let bytes = count.checked_mul(size_of::<T>())
            .ok_or_else(|| invalid("aggregate-size", "Aggregate group accumulator capacity overflow"))?;
        let reservation = self.state.reserve(bytes)?;
        self.values.reserve_exact(count - self.values.len());
        self.values.resize_with(count, T::default);
        self.reservation = reservation;
        Ok(())
    }
}

type ScalarGroups = GroupStates<ScalarAccumulator>;

impl ScalarGroups {
    fn aggregate(&mut self,page: Rows<'_>,mapping: GroupMapping<'_>,selected: impl Fn(usize) -> bool + Copy,group_count: usize,argument: Option<usize>,aggregate: &AggregateExpr,types: &AggregateTypes,scope: &rtdb_expr::EvaluationScope,execution: &Execution) -> Result<()> {
        use AggregateFunction as A;
        match aggregate.function {
            A::Sum | A::Average => {
                let summer = SumFactory::new(&types.input,types.output_type())?;
                self.ensure(group_count)?;
                self.sum_page(page,mapping,selected,argument,&summer,execution)?;
                if aggregate.function == A::Average { self.count_page(page,mapping,selected,argument,execution)?; }
            }
            A::VariancePopulation | A::VarianceSample | A::StddevPopulation | A::StddevSample => {
                let squares = square_column(page,argument,&types.input,scope,execution)?;
                let sum_type = LogicalType::Scalar(ScalarType::F64);
                let summer = SumFactory::new(&types.input,&sum_type)?;
                self.ensure(group_count)?;
                self.sum_page(page,mapping,selected,argument,&summer,execution)?;
                for (row, (value, group)) in squares.into_iter().zip(mapping.iter()).enumerate() {
                    execution.context.check_cancelled()?;
                    if selected(row) { if let Some(value) = value {
                        let state = &mut self.values[group];
                        state.squares = Some(state.squares.map_or(value,|previous|previous + value));
                    } }
                }
                self.count_page(page,mapping,selected,argument,execution)?;
            }
            A::Min | A::Max => {
                self.ensure(group_count)?;
                let minimum = aggregate.function == A::Min;
                let output = types.output_type();
                let date_millis = without_nullability(output) == &LogicalType::Scalar(ScalarType::DateMilli);
                let column = page.arrow_column(argument);
                let numeric = column.and_then(|column| NumericColumn::bind(column, Some(output)));
                let temporal = column.filter(|column| accepts_typed_column(column, |ty| admits_temporal_extremum(output, ty)));
                let decimal = column.filter(|column| {
                    let Some((precision, scale, 128)) = decimal_type(output) else { return false; };
                    let expected = rtdb_columnar::DecimalParameters { precision, scale, bit_width: 128 };
                    admits_reduction_column(column, |field, logical| decimal_leaf(field, logical) && (
                        field.data_type() == &DataType::Null
                            || rtdb_columnar::decimal_field_parameters(field).is_ok_and(|params| params == Some(expected))
                    ))
                });
                let required = without_nullability(output);
                let bytes = column.filter(|column| admits_byte_extremum(column, required));
                for (row, group) in mapping.iter().enumerate() {
                    execution.context.check_cancelled_at(row)?;
                    if selected(row) {
                        let state = &mut self.values[group];
                        if let Some(column) = &numeric {
                            if let Some(value) = column.get(row)? {
                                state.numeric_extremum(value, output, minimum)?;
                            }
                        } else if let Some(column) = temporal {
                            if let Some(cell) = present_column_position(column, row)? {
                                if matches!(cell.field.data_type(), DataType::Date32 | DataType::Date64) {
                                    state.date_extremum(temporal_ticks(cell.array, cell.row)?, date_millis, minimum);
                                } else {
                                    let ticks = cast_extremum_ticks(cell, output, &execution.context.timezone)?;
                                    state.temporal_extremum(ticks, minimum)?;
                                }
                            }
                        } else if let Some(column) = decimal && !matches!(state.accumulator, ScalarState::Retained(_)) {
                            if let Some((array, offset)) = present_column_cell(column, row)? {
                                state.decimal_extremum(array.as_primitive::<Decimal128Type>().value(offset), minimum)?;
                            }
                        } else if let Some(column) = bytes {
                            if let Some((array, offset)) = present_column_cell(column, row)? {
                                match required {
                                    LogicalType::Scalar(ScalarType::Utf8) => {
                                        let text = array.as_string::<i32>().value(offset);
                                        state.byte_extremum(text.as_bytes(), minimum, &self.state, || Value::Utf8(text.to_owned()))?;
                                    }
                                    LogicalType::Scalar(ScalarType::Binary) => {
                                        let bytes = array.as_binary::<i32>().value(offset);
                                        state.byte_extremum(bytes, minimum, &self.state, || Value::Binary(bytes.to_vec()))?;
                                    }
                                    LogicalType::Scalar(ScalarType::Keyword) => {
                                        let text = array.as_string_opt::<i32>()
                                            .ok_or_else(|| rtdb_types::Anomaly::incorrect("rtdb/array-layout", "Arrow array and logical field layout disagree"))?
                                            .value(offset);
                                        state.byte_extremum(text.as_bytes(), minimum, &self.state, || Value::Keyword(rtdb_types::Keyword::from_name(text)))?;
                                    }
                                    LogicalType::Scalar(ScalarType::Uri) => {
                                        // Rows::columns validates URI syntax in page order before the aggregate filter runs.
                                        let text = array.as_string_opt::<i32>()
                                            .ok_or_else(|| rtdb_types::Anomaly::incorrect("rtdb/array-layout", "Arrow array and logical field layout disagree"))?
                                            .value(offset);
                                        state.byte_extremum(text.as_bytes(), minimum, &self.state, || Value::Uri(text.to_owned()))?;
                                    }
                                    _ => unreachable!(),
                                }
                            }
                        } else {
                            let value = page.value(row,argument)?;
                            state.scalar_extremum(value.as_ref(), output, minimum, execution, &self.state)?;
                        }
                    }
                }
            }
            A::Every | A::Any => {
                self.ensure(group_count)?;
                let every = aggregate.function == A::Every;
                let boolean = page.arrow_column(argument).filter(|column| accepts_typed_column(column, |ty| *ty == DataType::Boolean));
                for (row, group) in mapping.iter().enumerate() {
                    execution.context.check_cancelled_at(row)?;
                    if !selected(row) { continue; }
                    let state = &mut self.values[group];
                    if let Some(column) = boolean {
                        if let Some((array, offset)) = present_column_cell(column, row)? {
                            state.reduce_boolean(Some(array.as_boolean().value(offset)), every)?;
                        }
                    } else {
                        let value = page.value(row, argument)?;
                        match value.as_ref() {
                            Value::Null | Value::Undefined => (),
                            Value::Bool(value) => state.reduce_boolean(Some(*value), every)?,
                            _ => state.reduce_boolean(None, every)?,
                        }
                    }
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }
    fn sum_page(&mut self,page: Rows<'_>,mapping: GroupMapping<'_>,selected: impl Fn(usize) -> bool + Copy,argument: Option<usize>,summer: &SumFactory<'_>,execution: &Execution) -> Result<()> {
        let _scratch = execution.context.memory.reserve(4096)?;
        let source = page.arrow_column(argument);
        let input = without_nullability(summer.input);
        let numeric = matches!(summer.output, LogicalType::Scalar(ScalarType::I32 | ScalarType::I64 | ScalarType::F32 | ScalarType::F64))
            .then(|| NumericColumn::bind(source?, None)).flatten();
        let float32 = summer.output == &LogicalType::Scalar(ScalarType::F32);
        let floating = source.filter(|column| {
            numeric.is_none() && (float32 || summer.output == &LogicalType::Scalar(ScalarType::F64))
                && admits_reduction_column(column, |field, logical| {
                    NumericColumn::admits_leaf(field, logical, None) || decimal_leaf(field, logical)
                })
        });
        let decimal = match summer.output {
            LogicalType::Scalar(ScalarType::Decimal { scale, bit_width: 128, .. }) if *scale >= 0 =>
                source.filter(|column| admits_reduction_column(column, decimal_leaf)).map(|column| (column, *scale)),
            _ => None,
        };
        let duration = match summer.output {
            LogicalType::Scalar(ScalarType::Duration(output)) => source
                .filter(|column| accepts_typed_column(column, |ty| matches!(ty, DataType::Duration(_))))
                .map(|column| (column, *output)),
            _ => None,
        };
        for (index, group) in mapping.iter().enumerate() {
            execution.context.check_cancelled_at(index)?;
            if !selected(index) { continue; }
            let state = &mut self.values[group];
            if let Some(column) = &numeric {
                if let Some(value) = column.get(index)? {
                    state.add_numeric(value, summer.output)?;
                }
                continue;
            }
            if let Some(column) = floating {
                if let Some(cell) = present_column_position(column, index)? {
                    let value = if float32 && decimal_storage(cell.field.data_type()) {
                        NumericCell::read_decimal_f32(cell)?
                    } else {
                        NumericCell::read_number_arrow(cell)?
                    };
                    state.add_numeric(value, summer.output)?;
                }
                continue;
            }
            if let Some((column, output)) = duration {
                let Some((array, offset)) = present_column_cell(column, index)? else {
                    if matches!(input, LogicalType::Scalar(ScalarType::Duration(_))) { return Err(duration_null(index)); }
                    continue;
                };
                let DataType::Duration(unit) = array.data_type() else { return Err(sum_state()); };
                state.add_duration(temporal_ticks(array, offset)?, time_unit(unit), output)?;
                continue;
            }
            if let Some((column, scale)) = decimal && !matches!(state.accumulator, ScalarState::Retained(_)) {
                if let Some(cell) = present_column_position(column, index)? {
                    if let Some((parameters, bytes)) = cell.decimal_coefficient()?
                        && parameters.bit_width == 128 && parameters.scale == scale
                    {
                        state.add_decimal128(i128::from_be_bytes(bytes[16..].try_into().expect("Decimal128 coefficient width")))?;
                        continue;
                    }
                } else {
                    if matches!(input, LogicalType::Scalar(ScalarType::Decimal { .. })) { state.add_decimal128(0)?; }
                    continue;
                }
            }
            let value = page.value(index,argument)?;
            summer.add(state, value.as_ref(), index, &self.state)?;
        }
        Ok(())
    }
    fn count_page(&mut self,page: Rows<'_>,mapping: GroupMapping<'_>,selected: impl Fn(usize) -> bool + Copy,argument: Option<usize>,execution: &Execution) -> Result<()> {
        let typed = page.arrow_column(argument).filter(|column| accepts_masked_count(column));
        for (row, group) in mapping.iter().enumerate() {
            execution.context.check_cancelled_at(row)?;
            if !selected(row) { continue; }
            if count_is_present(page, typed, row, argument)? {
                let state = &mut self.values[group];
                state.count = state.count.wrapping_add(1);
            }
        }
        Ok(())
    }
}
#[derive(Default)]
enum ScalarState {
    #[default]
    Empty,
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Duration(i64, TimeUnit),
    Decimal128([u8; 16]),
    TemporalExtremum(i64),
    Boolean(bool),
    Retained(OwnedRow),
}

impl ScalarState {
    fn retained(&self) -> Option<&Value> {
        match self {
            Self::Retained(row) => Some(&row.values[0]),
            _ => None,
        }
    }
}

#[derive(Default)]
struct ScalarAccumulator { count: i64, accumulator: ScalarState, squares: Option<f64> }
impl ScalarAccumulator {
    fn add_decimal128(&mut self, coefficient: i128) -> Result<()> {
        let sum = match self.accumulator {
            ScalarState::Empty => coefficient,
            ScalarState::Decimal128(sum) => i128::from_ne_bytes(sum).checked_add(coefficient).ok_or_else(|| {
                rtdb_types::Anomaly::incorrect("xtdb.expression/overflow-error", "data exception - overflow error")
            })?,
            _ => return Err(sum_state()),
        };
        self.accumulator = ScalarState::Decimal128(sum.to_ne_bytes());
        Ok(())
    }

    fn decimal_extremum(&mut self, coefficient: i128, minimum: bool) -> Result<()> {
        match self.accumulator {
            ScalarState::Empty => (),
            ScalarState::Decimal128(previous) => {
                if coefficient.cmp(&i128::from_ne_bytes(previous)) != if minimum { Ordering::Less } else { Ordering::Greater } {
                    return Ok(());
                }
            }
            _ => return Err(invalid("aggregate-state", "Expected a Decimal128 extremum")),
        }
        self.accumulator = ScalarState::Decimal128(coefficient.to_ne_bytes());
        Ok(())
    }

    fn byte_extremum(&mut self, bytes: &[u8], minimum: bool, memory: &Arc<rtdb_columnar::MemoryBudget>, winner: impl FnOnce() -> Value) -> Result<()> {
        if let Some(previous) = self.accumulator.retained() {
            let previous = match previous {
                Value::Utf8(text) | Value::Uri(text) => text.as_bytes(),
                Value::Binary(bytes) => bytes,
                Value::Keyword(keyword) => keyword.as_str().as_bytes(),
                _ => return Err(invalid("aggregate-state", "Expected a text, binary, keyword or URI extremum")),
            };
            if bytes.cmp(previous) != if minimum { Ordering::Less } else { Ordering::Greater } {
                return Ok(());
            }
        }
        let charge = bytes.len().checked_add(size_of::<Value>() + 256)
            .ok_or_else(|| invalid("aggregate-size", "Byte aggregate state exceeds addressable memory"))?;
        let reservation = memory.reserve(charge)?;
        self.accumulator = ScalarState::Retained(OwnedRow::with_reservation(vec![winner()], reservation)?);
        Ok(())
    }

    fn temporal_extremum(&mut self, value: i64, minimum: bool) -> Result<()> {
        match self.accumulator {
            ScalarState::Empty => self.accumulator = ScalarState::TemporalExtremum(value),
            ScalarState::TemporalExtremum(ref mut previous) => {
                if value.cmp(previous) == if minimum { Ordering::Less } else { Ordering::Greater } {
                    *previous = value;
                }
            }
            _ => return Err(invalid("aggregate-state", "Expected a temporal extremum")),
        }
        Ok(())
    }

    fn date_extremum(&mut self, ticks: i64, millis: bool, minimum: bool) {
        if self.date_wins(date_days(ticks, millis), millis, minimum) {
            self.accumulator = ScalarState::TemporalExtremum(ticks);
        }
    }

    fn date_wins(&self, days: i64, millis: bool, minimum: bool) -> bool {
        let previous = match &self.accumulator {
            ScalarState::TemporalExtremum(ticks) => Some(date_days(*ticks, millis)),
            state => state.retained().and_then(Value::date_days),
        };
        previous.is_none_or(|previous| days.cmp(&previous) == if minimum { Ordering::Less } else { Ordering::Greater })
    }

    fn add_duration(&mut self, value: i64, input: TimeUnit, output: TimeUnit) -> Result<()> {
        if input > output { return Err(sum_state()); }
        let value = value.wrapping_mul(output.units_per_second() / input.units_per_second());
        let sum = match self.accumulator {
            ScalarState::Empty => value,
            ScalarState::Duration(previous, unit) if unit == output => previous.wrapping_add(value),
            _ => return Err(sum_state()),
        };
        self.accumulator = ScalarState::Duration(sum, output);
        Ok(())
    }

    fn reduce_boolean(&mut self, value: Option<bool>, every: bool) -> Result<()> {
        match &mut self.accumulator {
            ScalarState::Empty => {
                let value = value.ok_or_else(|| rtdb_types::Anomaly::unsupported("rtdb.query/boolean-setter", "Reference boolean accumulator does not implement this value setter"))?;
                self.accumulator = ScalarState::Boolean(value);
            }
            ScalarState::Boolean(previous) => {
                let value = value.unwrap_or(true);
                *previous = if every { *previous && value } else { *previous || value };
            }
            _ => return Err(invalid("aggregate-state", "Expected a Boolean reduction")),
        }
        Ok(())
    }

    fn add_numeric(&mut self, value: NumericCell, output: &LogicalType) -> Result<()> {
        use ScalarState as S;
        self.accumulator = match (output, &mut self.accumulator, value) {
            (LogicalType::Scalar(ScalarType::I32), S::I32(sum), NumericCell::Integer(value)) => {
                *sum = sum.wrapping_add(value as i32);
                return Ok(());
            }
            (LogicalType::Scalar(ScalarType::I64), S::I64(sum), NumericCell::Integer(value)) => {
                *sum = sum.wrapping_add(value);
                return Ok(());
            }
            (LogicalType::Scalar(ScalarType::F32), S::F32(sum), value) => {
                *sum += value.as_f32();
                return Ok(());
            }
            (LogicalType::Scalar(ScalarType::F64), S::F64(sum), value) => {
                *sum += value.as_f64();
                return Ok(());
            }
            (LogicalType::Scalar(ScalarType::I32), S::Empty, NumericCell::Integer(value)) => S::I32(value as i32),
            (LogicalType::Scalar(ScalarType::I64), S::Empty, NumericCell::Integer(value)) => S::I64(value),
            (LogicalType::Scalar(ScalarType::F32), S::Empty, value) => S::F32(value.as_f32()),
            (LogicalType::Scalar(ScalarType::F64), S::Empty, value) => S::F64(value.as_f64()),
            _ => return Err(sum_state()),
        };
        Ok(())
    }

    fn numeric_extremum(&mut self, value: NumericCell, output: &LogicalType, minimum: bool) -> Result<()> {
        use ScalarState as S;
        let previous = match self.accumulator {
            S::Empty => None,
            S::I8(value) => Some(NumericCell::Integer(i64::from(value))),
            S::I16(value) => Some(NumericCell::Integer(i64::from(value))),
            S::I32(value) => Some(NumericCell::Integer(i64::from(value))),
            S::I64(value) => Some(NumericCell::Integer(value)),
            S::F32(value) => Some(NumericCell::Float32(value)),
            S::F64(value) => Some(NumericCell::Float64(value)),
            _ => return Err(invalid("aggregate-state", "Expected a numeric extremum")),
        };
        if let Some(previous) = previous {
            if value.key().ordered_compare(previous.key()) != Some(if minimum { Ordering::Less } else { Ordering::Greater }) {
                return Ok(());
            }
        }
        self.accumulator = match (without_nullability(output), value) {
            (LogicalType::Scalar(ScalarType::I8), NumericCell::Integer(value)) => S::I8(value as i8),
            (LogicalType::Scalar(ScalarType::I16), NumericCell::Integer(value)) => S::I16(value as i16),
            (LogicalType::Scalar(ScalarType::I32), NumericCell::Integer(value)) => S::I32(value as i32),
            (LogicalType::Scalar(ScalarType::I64), NumericCell::Integer(value)) => S::I64(value),
            (LogicalType::Scalar(ScalarType::F32), NumericCell::Float32(value)) => S::F32(value),
            (LogicalType::Scalar(ScalarType::F64), NumericCell::Float64(value)) => S::F64(value),
            _ => return Err(invalid("aggregate-state", "Numeric extremum does not match its output vector")),
        };
        Ok(())
    }

    fn scalar_extremum(&mut self, value: &Value, output: &LogicalType, minimum: bool, execution: &Execution, state: &Arc<rtdb_columnar::MemoryBudget>) -> Result<()> {
        if matches!(value, Value::Null | Value::Undefined) { return Ok(()); }
        let _value = execution.context.memory.reserve(value.retained_bytes()?)?;
        let value = cast(value, output, &execution.context.timezone)?;
        let numeric = match value {
            Value::I8(value) => Some(NumericCell::Integer(i64::from(value))),
            Value::I16(value) => Some(NumericCell::Integer(i64::from(value))),
            Value::I32(value) => Some(NumericCell::Integer(i64::from(value))),
            Value::I64(value) => Some(NumericCell::Integer(value)),
            Value::F32(value) => Some(NumericCell::Float32(value)),
            Value::F64(value) => Some(NumericCell::Float64(value)),
            _ => None,
        };
        if let Some(value) = numeric {
            return self.numeric_extremum(value, output, minimum);
        }
        match &value {
            Value::DateDay(ticks) => return Ok(self.date_extremum(i64::from(*ticks), false, minimum)),
            Value::DateMilli(ticks) => return Ok(self.date_extremum(*ticks, true, minimum)),
            Value::Date(date) => {
                let millis = date.unit() == rtdb_types::DateUnit::Millisecond;
                if self.date_wins(date.date().epoch_day(), millis, minimum) {
                    self.accumulator = ScalarState::Retained(OwnedRow::new(vec![value], state)?);
                }
                return Ok(());
            }
            _ => {}
        }
        if let Value::Timestamp { value, .. } | Value::Time { value, .. } | Value::Duration { value, .. } = value {
            return self.temporal_extremum(value, minimum);
        }
        let compact;
        let _compact_charge;
        let previous = if let Value::Decimal(value) = &value
            && let Some((precision, scale, 128)) = decimal_type(output)
        {
            if !matches!(self.accumulator, ScalarState::Retained(_))
                && let Ok(bytes) = value.unscaled.as_slice().try_into()
            {
                return self.decimal_extremum(i128::from_be_bytes(bytes), minimum);
            }
            if let ScalarState::Decimal128(bytes) = self.accumulator {
                _compact_charge = execution.context.memory.reserve(16)?;
                compact = Value::Decimal(decimal128_value(i128::from_ne_bytes(bytes), precision, scale)?);
                Some(&compact)
            } else {
                self.accumulator.retained()
            }
        } else {
            self.accumulator.retained()
        };
        if let Some(previous) = previous {
            let order = match (&value, previous) {
                (Value::Uri(left), Value::Uri(right)) => left.as_bytes().cmp(right.as_bytes()),
                _ => compare_in_timezone(&value, previous, false, &execution.context.timezone)?,
            };
            if order != if minimum { Ordering::Less } else { Ordering::Greater } { return Ok(()); }
        }
        self.accumulator = ScalarState::Retained(OwnedRow::new(vec![value], state)?);
        Ok(())
    }
    fn average_double(&self) -> Option<f64> {
        if self.count == 0 { return None; }
        match self.accumulator {
            ScalarState::F64(sum) => Some(sum / self.count as f64),
            _ => None,
        }
    }

    fn variance_double(&self, function: &AggregateFunction) -> Option<f64> {
        use AggregateFunction as A;
        let sample = matches!(function, A::VarianceSample | A::StddevSample);
        let ScalarState::F64(sum) = self.accumulator else { return None; };
        if self.count < if sample { 2 } else { 1 } { return None; }
        let variance = (self.squares? - sum * sum / self.count as f64) / (self.count - i64::from(sample)) as f64;
        Some(if matches!(function, A::StddevPopulation | A::StddevSample) { variance.sqrt() } else { variance })
    }

    fn finish(&self,aggregate: &AggregateExpr,ty: &LogicalType,output: &rtdb_expr::EvaluationScope) -> Result<Value> {
        use AggregateFunction as A;
        if let Some(value) = self.accumulator.retained() { output.reserve(value.retained_bytes()?)?; }
        let value = match &aggregate.function {
            A::Average if self.count == 0 => Value::Null,
            A::Average => match self.accumulator {
                ScalarState::Duration(value, unit) => Value::Duration { value: value.wrapping_div(self.count), unit },
                _ => self.average_double().map_or(Value::Null, Value::F64),
            },
            A::VariancePopulation | A::VarianceSample | A::StddevPopulation | A::StddevSample =>
                self.variance_double(&aggregate.function).map_or(Value::Null, Value::F64),
            _ => match &self.accumulator {
                ScalarState::Empty => Value::Null,
                ScalarState::I8(value) => Value::I8(*value),
                ScalarState::I16(value) => Value::I16(*value),
                ScalarState::I32(sum) => Value::I32(*sum),
                ScalarState::I64(sum) => Value::I64(*sum),
                ScalarState::F32(sum) => Value::F32(*sum),
                ScalarState::F64(sum) => Value::F64(*sum),
                ScalarState::Duration(value, unit) => Value::Duration { value: *value, unit: *unit },
                ScalarState::TemporalExtremum(value) => match without_nullability(ty) {
                    LogicalType::Scalar(ScalarType::Timestamp { unit, timezone }) => {
                        if let Some(zone) = timezone { output.reserve(zone.len())?; }
                        Value::Timestamp { value: *value, unit: *unit, timezone: timezone.clone() }
                    }
                    LogicalType::Scalar(ScalarType::Time { unit, bit_width }) => Value::Time { value: *value, unit: *unit, bit_width: *bit_width },
                    LogicalType::Scalar(ScalarType::Duration(unit)) => Value::Duration { value: *value, unit: *unit },
                    LogicalType::Scalar(ScalarType::DateDay) => Value::DateDay(*value as i32),
                    LogicalType::Scalar(ScalarType::DateMilli) => Value::DateMilli(*value),
                    _ => return Err(invalid("aggregate-state", "Expected a temporal extremum output type")),
                },
                ScalarState::Boolean(value) => Value::Bool(*value),
                ScalarState::Decimal128(coefficient) => {
                    let Some((precision, scale, 128)) = decimal_type(ty) else { return Err(sum_state()); };
                    output.reserve(16)?;
                    Value::Decimal(decimal128_value(i128::from_ne_bytes(*coefficient), precision, scale)?)
                }
                ScalarState::Retained(row) => row.values[0].clone(),
            },
        };
        Ok(value)
    }
}

fn date_days(ticks: i64, millis: bool) -> i64 {
    if millis { ticks.div_euclid(rtdb_types::java_date::MILLIS_PER_DAY) } else { ticks }
}

fn percentile_ranks(kind: PercentileKind, fraction: f64, count: i32) -> Result<(i32, i32, f64)> {
    if count == 1 { return Ok((0, 0, 0.0)); }
    if kind == PercentileKind::Discrete {
        let rank = if fraction <= 0.0 { 0 } else if fraction >= 1.0 { count - 1 } else { ((fraction * f64::from(count)).ceil() as i32 - 1).max(0) };
        return Ok((rank, rank, 0.0));
    }
    let rank = fraction * f64::from(count - 1);
    let low = rank.floor();
    let high = rank.ceil();
    if low < 0.0 || high >= f64::from(count) {
        return Err(rtdb_types::Anomaly::fault("rtdb.query/percentile-rank", "Continuous percentile rank lies outside the sorted input"));
    }
    Ok((low as i32, high as i32, rank - low))
}

fn decimal_type(logical: &LogicalType) -> Option<(i32, i32, u16)> {
    match logical {
        LogicalType::Scalar(ScalarType::Decimal { precision, scale, bit_width }) => Some((*precision, *scale, *bit_width)),
        LogicalType::Maybe(inner) | LogicalType::RunEndEncoded { value: inner, .. } => decimal_type(inner),
        _ => None,
    }
}
fn decimal_value(value: &Value) -> Result<&rtdb_types::DecimalValue> {
    if let Value::Decimal(value) = value { Ok(value) } else { Err(invalid("decimal-sum", "Reference decimal SUM accepts decimal input vectors")) }
}
fn decimal128_value(coefficient: i128, precision: i32, scale: i32) -> Result<rtdb_types::DecimalValue> {
    rtdb_types::DecimalValue::new(coefficient.to_be_bytes().to_vec(), precision, scale, 128)
}
fn validate_aggregate_input(aggregate: &AggregateExpr, types: &AggregateTypes) -> Result<()> {
    if matches!(aggregate.function, AggregateFunction::Min | AggregateFunction::Max)
        && types.declared_output.legs().iter().any(|leg| matches!(leg, LogicalType::Scalar(ScalarType::U8 | ScalarType::U16 | ScalarType::U32 | ScalarType::U64 | ScalarType::F16))) {
        return Err(rtdb_types::Anomaly::unsupported("rtdb.query/min-max-setter", "Reference MIN/MAX does not implement the output vector setter"));
    }
    if matches!(aggregate.function,AggregateFunction::Every | AggregateFunction::Any) { validate_boolean_factory(&types.input)?; }
    Ok(())
}

fn validate_boolean_factory(input: &LogicalType) -> Result<()> {
    use LogicalType as T;
    use ScalarType as S;
    match input {
        T::Nothing | T::Null | T::Scalar(S::Bool | S::I32 | S::I64 | S::U32 | S::U64 | S::F32 | S::F64 | S::Duration(_) | S::Timestamp { .. } | S::DateDay | S::DateMilli) => Ok(()),
        T::Maybe(inner) | T::RunEndEncoded { value: inner,.. } => validate_boolean_factory(inner),
        T::Union(legs) => { for leg in legs { validate_boolean_factory(leg)?; }Ok(()) }
        _ => Err(rtdb_types::Anomaly::unsupported("rtdb.query/boolean-setter","Reference reducing aggregate has no setter for this emitted type")),
    }
}

struct SumFactory<'a> { input: &'a LogicalType, output: &'a LogicalType }
impl<'a> SumFactory<'a> {
    fn new(input: &'a LogicalType,output: &'a LogicalType) -> Result<Self> {
        validate_sum_factory(input,without_nullability(output))?;
        Ok(Self { input,output: without_nullability(output) })
    }
    fn add(&self, state: &mut ScalarAccumulator, value: &Value, index: usize, memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<()> {
        use ScalarType as S;
        let null = matches!(value,Value::Null | Value::Undefined);
        if null && matches!(without_nullability(self.input),LogicalType::Scalar(S::Duration(_))) {
            return Err(duration_null(index));
        }
        if null && !matches!((without_nullability(self.input),self.output),
            (LogicalType::Scalar(S::Decimal { .. }),LogicalType::Scalar(S::Decimal { .. }))) { return Ok(()); }
        let next = match self.output {
            LogicalType::Scalar(S::I32 | S::I64) =>
                return state.add_numeric(NumericCell::Integer(signed_integer(value)?), self.output),
            LogicalType::Scalar(S::F32) => {
                return state.add_numeric(NumericCell::Float32(vector_float(value)?), self.output);
            }
            LogicalType::Scalar(S::F64) => {
                return state.add_numeric(NumericCell::Float64(vector_double(value)?), self.output);
            }
            LogicalType::Scalar(S::Decimal { precision, scale, bit_width }) => {
                let input = if null { None } else { Some(decimal_value(value)?) };
                if *bit_width == 128 && *scale >= 0 && !matches!(state.accumulator, ScalarState::Retained(_))
                    && let Some(coefficient) = match input {
                        None => Some(0),
                        Some(value) if value.bit_width == 128 && value.scale == *scale =>
                            value.unscaled.as_slice().try_into().ok().map(i128::from_be_bytes),
                        _ => None,
                    }
                {
                    return state.add_decimal128(coefficient);
                }
                let compact;
                let previous = match state.accumulator {
                    ScalarState::Decimal128(coefficient) => {
                        compact = decimal128_value(i128::from_ne_bytes(coefficient), *precision, *scale)?;
                        Some(&compact)
                    }
                    _ => state.accumulator.retained().map(decimal_value).transpose()?,
                };
                Value::Decimal(rtdb_expr::decimal_sum(previous, input, *precision, *scale, *bit_width)?)
            }
            LogicalType::Scalar(S::Duration(output)) => match value {
                Value::Duration { value, unit } => return state.add_duration(*value, *unit, *output),
                _ => return Err(sum_state()),
            },
            _ => return Err(sum_state()),
        };
        match &mut state.accumulator {
            ScalarState::Retained(row) if row.values[0].retained_bytes()? == next.retained_bytes()? => row.values[0] = next,
            accumulator => *accumulator = ScalarState::Retained(OwnedRow::new(vec![next], memory)?),
        }
        Ok(())
    }
}

fn without_nullability(logical: &LogicalType) -> &LogicalType {
    if let LogicalType::Maybe(inner) = logical { without_nullability(inner) } else { logical }
}

fn validate_sum_factory(input: &LogicalType,output: &LogicalType) -> Result<()> {
    use LogicalType as T;
    use ScalarType as S;
    match input {
        T::Maybe(inner) => return validate_sum_factory(inner,output),
        T::Union(legs) => { for leg in legs { validate_sum_factory(leg,output)?; }return Ok(()); }
        T::Nothing | T::Null if !matches!(output,T::Union(_) | T::RunEndEncoded { .. }) => return Ok(()),
        T::Scalar(S::I8 | S::I16 | S::I32 | S::I64 | S::U8 | S::U16 | S::U32 | S::U64) => match output {
            T::Scalar(S::I8 | S::I16) => return Err(rtdb_types::Anomaly::unsupported("rtdb.query/small-integer-sum","Reference SUM does not implement byte or short accumulators")),
            T::Scalar(S::I32 | S::I64 | S::F32 | S::F64) => return Ok(()),
            _ => (),
        },
        T::Scalar(S::F32 | S::F64) if matches!(output,T::Scalar(S::F32 | S::F64)) => return Ok(()),
        T::Scalar(S::Decimal { .. }) if matches!(output,T::Scalar(S::F32 | S::F64 | S::Decimal { .. })) => return Ok(()),
        T::Scalar(S::Duration(input)) if matches!(output,T::Scalar(S::Duration(output)) if input <= output) => return Ok(()),
        _ => (),
    }
    let code = if decimal_type(output).is_some() { "rtdb.query/decimal-sum-input" } else { "rtdb.query/sum-input" };
    Err(rtdb_types::Anomaly::unsupported(code,"Reference vector does not implement SUM into this output vector"))
}

fn signed_integer(value: &Value) -> Result<i64> {
    Ok(match value {
        Value::I8(value) => i64::from(*value),Value::I16(value) => i64::from(*value),Value::I32(value) => i64::from(*value),Value::I64(value) => *value,
        Value::U8(value) => i64::from(*value as i8),Value::U16(value) => i64::from(*value as i16),Value::U32(value) => i64::from(*value as i32),Value::U64(value) => *value as i64,
        _ => return Err(sum_state()),
    })
}
fn vector_float(value: &Value) -> Result<f32> {
    match value {
        Value::F32(value) => Ok(*value),Value::F64(value) => Ok(*value as f32),
        Value::Decimal(value) => rtdb_expr::decimal_text(value).parse().map_err(|_|sum_state()),
        _ => Ok(signed_integer(value)? as f32),
    }
}
fn vector_double(value: &Value) -> Result<f64> {
    match value {
        Value::F32(value) => Ok(f64::from(*value)),Value::F64(value) => Ok(*value),Value::Decimal(value) => Ok(rtdb_expr::decimal_to_f64(value)),
        _ => Ok(signed_integer(value)? as f64),
    }
}
fn sum_state() -> rtdb_types::Anomaly { invalid("aggregate-state","SUM cell does not match its admitted vector factory") }

fn duration_null(index: usize) -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::fault("xtdb.error/unknown", format!("null at index {index}"))
}

fn square_column(page: Rows<'_>,argument: Option<usize>,input: &LogicalType,scope: &rtdb_expr::EvaluationScope,execution: &Execution) -> Result<Vec<Option<f64>>> {
    if !matches!(without_nullability(input),LogicalType::Scalar(ScalarType::I32 | ScalarType::I64 | ScalarType::U32 | ScalarType::U64 | ScalarType::F64)) {
        return Err(rtdb_types::Anomaly::unsupported("rtdb.query/variance-input","Reference variance requires an IntVector, LongVector or DoubleVector"));
    }
    scope.reserve(page.len().checked_mul(std::mem::size_of::<Option<f64>>()).ok_or_else(||invalid("aggregate-size","Variance square column capacity overflow"))?)?;
    let mut squares = Vec::with_capacity(page.len());
    if let Some(column) = page.arrow_column(argument).and_then(|column| NumericColumn::bind(column, None)) {
        for row in 0..page.len() {
            execution.context.check_cancelled()?;
            squares.push(column.get(row)?.map(|cell| {
                let value = cell.as_f64();
                value * value
            }));
        }
        return Ok(squares);
    }
    for row in 0..page.len() {
        execution.context.check_cancelled()?;
        let cell = page.value(row,argument)?;
        let value = cell.as_ref();
        squares.push(if matches!(value,Value::Null | Value::Undefined) { None } else { let value = vector_double(value)?;Some(value * value) });
    }
    Ok(squares)
}

#[cfg(test)]
mod page_tests;
