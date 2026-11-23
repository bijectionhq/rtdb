use std::cmp::Ordering;
use std::sync::Arc;

use arrow_array::Array;
use async_trait::async_trait;
use rtdb_expr::{cast, compare_in_timezone, is_true, number, sql_eq_in_timezone};
use rtdb_plan::{AggregateExpr, AggregateFunction, Binder, NamedExpr, PercentileFraction, PercentileKind, Schema};
use rtdb_types::{LogicalType, Result, ScalarType, Value};

use crate::context::Execution;
use crate::columnar::{BatchCursor, Column, ColumnBatch};
use crate::cursor::OwnedRow;
use crate::runtime_schema::{EmissionMode, EmittedRelation};
use crate::set_keys::BatchKeyIndex;
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
        for &position in positions {
            for row in 0..batch.len {
                context.check_cancelled_at(row)?;
                batch.columns[position].validate_readable_cell(row, &context.memory)?;
            }
        }
        Ok(Self { batch, projected, memory: &context.memory })
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
}

struct AggregateInput { batches: crate::columnar::BoxBatchCursor, projected: bool }

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
        for aggregate in aggregates {
            if aggregate.function.canonical() == AggregateFunction::ArrayAgg {
                for order in &aggregate.order_by {
                    positions.push(argument_position(&order.expr, schema).ok()?);
                }
            }
            if let Some(argument) = &aggregate.argument { positions.push(argument_position(argument, schema).ok()?); }
        }
        Some(positions)
    }

    async fn reduce_global(&mut self, input: AggregateInput) -> Result<(Vec<AggregateColumn>, Arc<rtdb_columnar::MemoryReservation>)> {
        let AggregateInput { mut batches, projected } = input;
        let (mut columns, reservation) = self.accumulators()?;
        let positions = Self::positions(&self.schema, &self.groups, &self.aggregates);
        while let Some(batch) = batches.next_batch().await? {
            if positions.is_some() && self.accepts_typed_aggregates(&batch)? {
                let memory = &self.execution.context.memory;
                let _mapping = memory.reserve(batch.len.checked_mul(std::mem::size_of::<usize>())
                    .ok_or_else(|| invalid("aggregate-size", "Group mapping overflow"))?)?;
                self.reduce_typed(&batch, &vec![0; batch.len], 1, &mut columns)?;
                continue;
            }
            let positions = positions.as_ref().ok_or_else(|| invalid("aggregate-input", "A batch input reached an aggregate that reads whole rows"))?;
            let rows = Rows::columns(&batch, positions, projected, &self.execution.context)?;
            self.execution.context.check_cancelled()?;
            let scope = self.execution.scope()?;
            scope.reserve(rows.len().checked_mul(std::mem::size_of::<usize>()).ok_or_else(|| invalid("aggregate-size", "Group mapping overflow"))?)?;
            let mapping = vec![0; rows.len()];
            for ((column, aggregate), types) in columns.iter_mut().zip(&self.aggregates).zip(self.runtime.aggregates()?) {
                column.aggregate(rows, &mapping, 1, aggregate, types, &self.schema, &self.execution).await?;
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
            let _mapping = memory.reserve(batch.len.checked_mul(std::mem::size_of::<usize>())
                .ok_or_else(|| invalid("aggregate-size", "Group mapping exceeds addressable memory"))?)?;
            let keys = {
                let _rows = memory.reserve(batch.len.checked_mul(std::mem::size_of::<(usize, usize)>())
                    .ok_or_else(|| invalid("aggregate-size", "Group key positions exceed addressable memory"))?)?;
                let rows = (0..batch.len).map(|row| (0, row)).collect::<Vec<_>>();
                ColumnBatch::take_rows_with_custody(&[&batch], &rows, group_positions, true, memory)?
            };
            let mut mapping = Vec::with_capacity(batch.len);
            index.identify(&keys, projected, context, &key_schema, |_, ordinal, _| {
                if ordinal >= i32::MAX as usize {
                    return Err(invalid("group-count", "Aggregate group count exceeds the reference vector limit"));
                }
                mapping.push(ordinal);
                Ok(())
            })?;
            drop(keys);
            if self.accepts_typed_aggregates(&batch)? {
                self.reduce_typed(&batch, &mapping, index.len(), &mut columns)?;
                continue;
            }
            let rows = Rows::columns(&batch, arguments, projected, context)?;
            for ((column, aggregate), types) in columns.iter_mut().zip(&self.aggregates).zip(self.runtime.aggregates()?) {
                column.aggregate(rows, &mapping, index.len(), aggregate, types, &self.schema, &self.execution).await?;
            }
        }
        Ok(index.group_keys(group_width, context)?.map(|keys| ReducedGroups { keys, columns, _reservation: reservation }))
    }
}

#[async_trait]
impl BatchCursor for AggregateCursor {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        self.execution.context.check_cancelled()?;
        let Some(input) = self.input.take() else { return Ok(None); };
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
                Accumulation::Scalar(states) => states,
                Accumulation::Vectors(vectors) => {
                    validate_collection_order(aggregate, types, &self.schema, &self.execution)?;
                    let (column, charge) = vectors.finish(batch.len, aggregate, field, &self.execution).await?;
                    batch.append_column(column, charge, &context.memory)?;
                    continue;
                }
            };
            states.ensure(batch.len, &self.execution)?;
            let bytes = batch.len.checked_mul(std::mem::size_of::<Value>().max(40)).and_then(|bytes| bytes.checked_add(field.name.len()))
                .and_then(|bytes| bytes.checked_add(512)).ok_or_else(|| invalid("aggregate-size", "Aggregate output exceeds addressable memory"))?;
            let reservation = context.memory.reserve(bytes)?;
            let array: arrow_array::ArrayRef = match aggregate.function {
                AggregateFunction::Count | AggregateFunction::CountStar => {
                    let mut counts = Vec::with_capacity(batch.len);
                    for (index, state) in states.values.iter().enumerate() {
                        context.check_cancelled_at(index)?;
                        counts.push(state.count);
                    }
                    Arc::new(arrow_array::Int64Array::from(counts))
                }
                AggregateFunction::Sum | AggregateFunction::Average
                    if without_nullability(&field.data_type) == &LogicalType::Scalar(ScalarType::F64) => {
                    let scope = self.execution.scope()?;
                    let mut sums = Vec::with_capacity(batch.len);
                    for (index, state) in states.values.iter().enumerate() {
                        context.check_cancelled_at(index)?;
                        sums.push(match state.finish(aggregate, &scope)? {
                            Value::F64(value) => Some(value),
                            Value::Null => None,
                            _ => return Err(invalid("aggregate-input", "Expected a floating-point aggregate state")),
                        });
                        scope.reset()?;
                    }
                    Arc::new(arrow_array::Float64Array::from(sums))
                }
                _ => {
                    let scope = self.execution.scope()?;
                    let mut reservation = reservation;
                    let lease = Arc::get_mut(&mut reservation).expect("Aggregate results own their reservation");
                    let mut values = Vec::with_capacity(batch.len);
                    for (row, state) in states.values.iter().enumerate() {
                        context.check_cancelled_at(row)?;
                        let value = state.finish(aggregate, &scope)?;
                        lease.grow_to(lease.bytes().checked_add(value.retained_bytes()?)
                            .ok_or_else(|| invalid("aggregate-size", "Aggregate output exceeds addressable memory"))?)?;
                        values.push(value);
                        scope.reset()?;
                    }
                    let (column, scratch) = crate::columnar::pack_evaluated_column(&values, &context.memory)?;
                    drop((values, reservation));
                    batch.append_column(column, scratch, &context.memory)?;
                    continue;
                }
            };
            let logical = field.data_type.clone();
            let field = Arc::new(rtdb_columnar::to_field(&field.name, &logical)?);
            batch.append_column(Column::Arrow { array, field, logical: Some(logical) }, reservation, &context.memory)?;
        }
        Ok(Some(batch))
    }
}

fn direct_rank_type(ty: &LogicalType) -> bool {
    use ScalarType as S;
    matches!(without_nullability(ty), LogicalType::Null | LogicalType::Scalar(
        S::Bool | S::I8 | S::I16 | S::I32 | S::I64 | S::F32 | S::F64 | S::Utf8 | S::Binary))
}

fn float_column(column: &crate::columnar::Column) -> Option<&arrow_array::Float64Array> {
    let crate::columnar::Column::Arrow { array, field, .. } = column else { return None };
    if !field.metadata().is_empty() { return None; }
    array.as_any().downcast_ref::<arrow_array::Float64Array>()
}
fn marked_nulls(column: &crate::columnar::Column) -> Option<&dyn arrow_array::Array> {
    use arrow_schema::DataType as T;
    let crate::columnar::Column::Arrow { array, field, .. } = column else { return None };
    (field.metadata().is_empty() && matches!(array.data_type(),
        T::Boolean | T::Int8 | T::Int16 | T::Int32 | T::Int64 | T::UInt8 | T::UInt16 | T::UInt32 | T::UInt64 |
        T::Float32 | T::Float64 | T::Utf8 | T::Binary | T::FixedSizeBinary(_))).then_some(array.as_ref())
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
                (A::Count,Some(position)) => marked_nulls(&batch.columns[position]).is_some(),
                (A::Sum | A::Average,Some(position)) => float_column(&batch.columns[position]).is_some()
                    && without_nullability(&types.input) == &float && without_nullability(types.output_type()) == &float,
                _ => false,
            };
            if !fits { return Ok(false); }
        }
        Ok(true)
    }

    fn reduce_typed(&self, batch: &ColumnBatch, mapping: &[usize], group_count: usize, columns: &mut [AggregateColumn]) -> Result<()> {
        use AggregateFunction as A;
        let types = self.runtime.aggregates()?;
        let memory = &self.execution.context.memory;
        for ((column,aggregate),types) in columns.iter_mut().zip(&self.aggregates).zip(types) {
            validate_aggregate_input(aggregate,types)?;
            let Accumulation::Scalar(states) = &mut column.accumulation else { return Err(invalid("aggregate-input","A typed aggregate collects its rows")) };
            states.ensure(group_count,&self.execution)?;
            let argument = aggregate.argument.as_ref().map(|argument|argument_position(argument,&self.schema)).transpose()?;
            match (&aggregate.function,argument) {
                (A::CountStar,_) => for (row, group) in mapping.iter().enumerate() {
                    self.execution.context.check_cancelled_at(row)?;
                    states.values[*group].count = states.values[*group].count.wrapping_add(1);
                },
                (A::Count,Some(position)) => {
                    let array = marked_nulls(&batch.columns[position]).ok_or_else(||invalid("aggregate-input","A typed count lost its column"))?;
                    for (row, group) in mapping.iter().enumerate() {
                        self.execution.context.check_cancelled_at(row)?;
                        if !array.is_null(row) { states.values[*group].count = states.values[*group].count.wrapping_add(1); }
                    }
                }
                (A::Sum | A::Average,Some(position)) => {
                    let array = float_column(&batch.columns[position]).ok_or_else(||invalid("aggregate-input","A typed sum lost its column"))?;
                    for (row, group) in mapping.iter().enumerate() {
                        self.execution.context.check_cancelled_at(row)?;
                        if array.is_null(row) { continue; }
                        let value = array.value(row);
                        let state = &mut states.values[*group];
                        if let Some(accumulator) = &mut state.accumulator {
                            let Value::F64(sum) = &mut accumulator.values[0] else { return Err(sum_state()); };
                            *sum += value;
                        } else {
                            state.accumulator = Some(OwnedRow::new(vec![Value::F64(value)], memory)?);
                        }
                        if aggregate.function == A::Average { state.count = state.count.wrapping_add(1); }
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
struct DistinctSeen {
    index: std::collections::HashMap<(usize, u32), Vec<usize>>,
    values: Vec<DistinctCell>,
}

enum DistinctCell {
    Value(OwnedRow),
    Arrow { key: Arc<ColumnBatch>, row: usize, projected: bool },
}
enum Accumulation { Scalar(ScalarGroups), Vectors(CollectionVectors) }
impl AggregateColumn {
    fn new(aggregate: &AggregateExpr,schema: &Schema,execution: &Execution) -> Result<Self> {
        let accumulation = match aggregate.function {
            AggregateFunction::ArrayAgg | AggregateFunction::VecAgg | AggregateFunction::Percentile { .. } => {
                let order = CollectionOrder::new(aggregate, schema)?;
                Accumulation::Vectors(CollectionVectors::new(&execution.context.memory, order)?)
            }
            _ => Accumulation::Scalar(ScalarGroups::new(execution)?),
        };
        let distinct = if aggregate.distinct && !matches!(aggregate.function,AggregateFunction::Min | AggregateFunction::Max) { Some(DistinctSeen { index: Default::default(), values: Vec::new() }) } else { None };
        Ok(Self { accumulation,distinct })
    }
    async fn aggregate(&mut self,input: Rows<'_>,mapping: &[usize],group_count: usize,aggregate: &AggregateExpr,types: &AggregateTypes,schema: &Schema,execution: &Execution) -> Result<()> {
        let scope = execution.scope()?;
        scope.reserve(input.len())?;
        let mut mask = vec![true;input.len()];
        if let Some(filter) = &aggregate.filter {
            let invocation = execution.prepare_expression(filter,schema,&scope)?;
            for (row, selected) in mask.iter_mut().enumerate() {
                let row_scope = execution.scope()?;
                let input = crate::columnar::BatchRow { batch: input.batch, row, admit_dates: false };
                let values = rtdb_expr::ExpressionRow::Borrowed(&input);
                *selected = is_true(&execution.evaluate_invocation_row(&row_scope, &invocation, values).await?)?;
            }
        }
        let argument = aggregate.argument.as_ref().map(|argument|argument_position(argument,schema)).transpose()?;
        if let Some(seen) = &mut self.distinct {
            let mut vector = None;
            let mut admitted = false;
            for (row, (group, selected)) in mapping.iter().zip(&mut mask).enumerate() {
                execution.context.check_cancelled()?;
                if !*selected { continue; }
                if !admitted { rtdb_plan::validate_equality_types(&types.input,&types.input)?; admitted = true; }
                let cell = input.value(row, argument)?;
                let value = cell.as_ref();
                let hash = crate::join_storage::hash_values(std::iter::once(value))?;
                if let Some(candidates) = seen.index.get(&(*group, hash)) {
                    for &candidate in candidates {
                        let allocation = rtdb_expr::EvaluationScope::new(&execution.context.memory)?;
                        let decoded;
                        let other = match &seen.values[candidate] {
                            DistinctCell::Value(row) => &row.values[0],
                            DistinctCell::Arrow { key, row, projected } => {
                                decoded = key.columns[0].expression_value(*row, *projected, Some(&allocation))?;
                                &decoded
                            }
                        };
                        if sql_eq_in_timezone(value, other, &execution.context.timezone, &allocation)? == Some(true) {
                            *selected = false;
                            break;
                        }
                    }
                }
                if *selected {
                    seen.index.entry((*group, hash)).or_default().push(seen.values.len());
                    let cell = match argument {
                        Some(position) => {
                            let Rows { batch, projected, memory } = input;
                            if vector.is_none() { vector = Some(Arc::new(batch.key_projection(&[position], memory)?)); }
                            DistinctCell::Arrow { key: Arc::clone(vector.as_ref().expect("The input key vector is retained")), row, projected }
                        }
                        _ => DistinctCell::Value(OwnedRow::copy_parts(&[std::slice::from_ref(value)], 0, 0, &execution.context.memory)?),
                    };
                    seen.values.push(cell);
                }
            }
        }
        validate_aggregate_input(aggregate,types)?;
        match &mut self.accumulation {
            Accumulation::Scalar(states) => {
                states.aggregate(input,mapping,&mask,group_count,argument,aggregate,types,&scope,execution)?;
            }
            Accumulation::Vectors(vectors) => {
                let Rows { batch, projected, .. } = input;
                let argument = argument.ok_or_else(|| invalid("aggregate-state", "Collected aggregate argument is absent"))?;
                if matches!(aggregate.function, AggregateFunction::Percentile { .. }) {
                    for (row, selected) in mask.iter_mut().enumerate() {
                        execution.context.check_cancelled_at(row)?;
                        if *selected { *selected = !input.is_null(row, Some(argument))?; }
                    }
                }
                vectors.append(batch, argument, mapping, &mask, group_count, projected, &execution.context)?;
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

    fn append(&mut self, batch: &ColumnBatch, position: usize, mapping: &[usize], mask: &[bool], groups: usize, projected: bool, context: &crate::QueryContext) -> Result<()> {
        self.present |= batch.len != 0;
        self.projected |= projected;
        if let Some(order) = &self.order {
            if batch.len == 0 { return Ok(()); }
            let batch = order.page(batch, mapping, mask, context)?;
            self.account_pages(self.selected, groups)?;
            self.batches.push(batch);
            return Ok(());
        }
        self.collect(batch, position, mapping.iter().copied().zip(mask).enumerate()
            .filter_map(|(row, (group, &selected))| selected.then_some((row, group))), groups, context)
    }

    fn collect(&mut self, batch: &ColumnBatch, position: usize, picks: impl Iterator<Item = (usize, usize)> + Clone, groups: usize, context: &crate::QueryContext) -> Result<()> {
        let count = picks.clone().count();
        if count == 0 { return Ok(()); }
        self.selected = self.selected.checked_add(count).filter(|count| *count <= i32::MAX as usize)
            .ok_or_else(|| invalid("aggregate-size", "List aggregate result exceeds signed Arrow offsets"))?;
        self.account_pages(self.selected, groups)?;
        self.groups.resize_with(groups, Vec::new);
        let page = self.batches.len();
        self.batches.push(batch.key_projection(&[position], &context.memory)?);
        for (row, group) in picks {
            self.groups.get_mut(group).ok_or_else(|| invalid("aggregate-group", "List aggregate group is absent"))?.push((page, row));
        }
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
        let base = groups.checked_mul(64).and_then(|bytes| self.selected.checked_mul(64).and_then(|rows| bytes.checked_add(rows)))
            .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("aggregate-size", "List aggregate allocation overflow"))?;
        let _scratch = memory.reserve(base)?;
        let mut picks = Vec::with_capacity(self.selected);
        let mut offsets = Vec::with_capacity(groups + 1);
        let mut valid = Vec::with_capacity(groups);
        offsets.push(0);
        for group in 0..groups {
            context.check_cancelled_at(group)?;
            let rows = self.groups.get(group).map_or(&[][..], Vec::as_slice);
            picks.extend_from_slice(rows);
            offsets.push(picks.len() as i32);
            valid.push(!rows.is_empty() || (aggregate.function == AggregateFunction::VecAgg && !self.present));
        }
        let selected = if picks.is_empty() {
            ColumnBatch::settled(vec![Column::Values(Vec::new())], 0, memory.reserve(256)?)?
        } else {
            ColumnBatch::take_rows(&self.batches, &picks, &[0], memory)?
        };
        selected.collect_lists(offsets, valid, output, memory)
    }

    async fn percentiles(self, groups: usize, kind: PercentileKind, fraction: f64, descending: bool, input_type: LogicalType,
        output: &rtdb_plan::Field, execution: &Execution) -> Result<(Column, Arc<rtdb_columnar::MemoryReservation>)> {
        use crate::scalar_key::ScalarKey;
        let context = &execution.context;
        let memory = &context.memory;
        let bytes = groups.checked_mul(2 * std::mem::size_of::<Value>()).and_then(|bytes| bytes.checked_add(512))
            .ok_or_else(|| invalid("aggregate-size", "Percentile results exceed addressable memory"))?;
        let mut reservation = memory.reserve(bytes)?;
        let mut values = Vec::with_capacity(groups);
        let mut picks = Vec::with_capacity(groups);
        let gather = kind == PercentileKind::Discrete && direct_rank_type(&output.data_type);
        let read = |(batch, row): (usize, usize)| Rows { batch: &self.batches[batch], projected: self.projected, memory }.value(row, Some(0));
        for group in 0..groups {
            context.check_cancelled_at(group)?;
            let rows = self.groups.get(group).map_or(&[][..], Vec::as_slice);
            if rows.is_empty() {
                if gather { picks.push(None); } else { values.push(Value::Null); }
                continue;
            }
            let (low, high, weight) = percentile_ranks(kind, fraction, rows.len() as i32)?;
            let (lower, upper) = if rows.len().saturating_mul(std::mem::size_of::<usize>()) > context.options.sort_memory_bytes {
                self.spilled_ranks(rows, low as u64, high as u64, descending, &input_type, execution).await?
            } else {
                let key = |(batch, row): (usize, usize)| ScalarKey::read(&self.batches[batch].columns[0], row);
                let indices = crate::sort::sort_ordinals(rows.len(), context, |left, right| {
                    let (left, right) = if descending { (rows[right], rows[left]) } else { (rows[left], rows[right]) };
                    if let (Some(left), Some(right)) = (key(left), key(right))
                        && let Some(order) = left.ordered_compare(right) { return Ok(order); }
                    rtdb_expr::ordered_compare_in_timezone(read(left)?.as_ref(), read(right)?.as_ref(), false, &context.timezone)
                })?;
                (rows[indices.values[low as usize]], rows[indices.values[high as usize]])
            };
            if gather {
                picks.push(Some(lower));
                continue;
            }
            let scope = execution.scope()?;
            let lower = read(lower)?;
            let value = if kind == PercentileKind::Discrete {
                let value = lower.as_ref();
                scope.reserve(value.retained_bytes()?.checked_mul(2).ok_or_else(|| invalid("aggregate-size", "Percentile output overflow"))?)?;
                execution.evaluate(&scope, &rtdb_plan::Expr::Literal(value.clone()), &Schema::default(), &[]).await?
            } else {
                let lower = number(lower.as_ref()).ok_or_else(|| invalid("percentile-input", "Continuous percentile requires numeric values"))?;
                if low == high {
                    Value::F64(lower)
                } else {
                    let upper = read(upper)?;
                    let upper = number(upper.as_ref()).ok_or_else(|| invalid("percentile-input", "Continuous percentile requires numeric values"))?;
                    Value::F64(lower + weight * (upper - lower))
                }
            };
            let lease = Arc::get_mut(&mut reservation).expect("The percentile aggregate owns its result reservation");
            lease.grow_to(lease.bytes().checked_add(value.retained_bytes()?).ok_or_else(|| invalid("aggregate-size", "Percentile output overflow"))?)?;
            values.push(value);
        }
        if gather {
            return crate::columnar::gather_nullable_column(|batch| &self.batches[batch], &picks, 0, memory);
        }
        crate::columnar::pack_evaluated_column(&values, memory)
    }

    async fn spilled_ranks(&self, rows: &[(usize, usize)], low: u64, high: u64, descending: bool, ty: &LogicalType,
        execution: &Execution) -> Result<((usize, usize), (usize, usize))> {
        use crate::sort::batches::BatchSort;
        use rtdb_plan::SortExpr;

        let context = &execution.context;
        let mut binder = Binder::default();
        let key = binder.field("value", None, ty.clone())?;
        let ordinal = binder.field("position", None, LogicalType::Scalar(ScalarType::I64))?;
        let schema = Schema::new(vec![key.clone(), ordinal])?;
        let mut batch = ColumnBatch::take_rows_with_custody(&self.batches, rows, &[0], true, &context.memory)?;
        let (column, charge) = crate::columnar::ordinal_column(batch.len, 0, "position", context)?;
        batch.append_column(column, charge, &context.memory)?;
        let input = crate::columnar::single_batch(Some(batch), Arc::clone(context));
        let order = vec![SortExpr { expr: key.expression(), descending, nulls_first: false }];
        let mut sorted = BatchSort::new(input, schema, order, execution.clone(), self.projected, low, high + 1)?;
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

    fn page(&self, batch: &ColumnBatch, mapping: &[usize], mask: &[bool], context: &crate::QueryContext) -> Result<ColumnBatch> {
        let memory = &context.memory;
        let mut output = batch.key_projection(&self.positions, memory)?;
        let bytes = batch.len.checked_mul(16).and_then(|bytes| bytes.checked_add(512))
            .ok_or_else(|| invalid("aggregate-size", "Ordered list aggregate group vector overflow"))?;
        let reservation = memory.reserve(bytes)?;
        let mut groups = arrow_array::builder::Int64Builder::with_capacity(batch.len);
        for (&group, &selected) in mapping.iter().zip(mask) {
            if selected { groups.append_value(group as i64); } else { groups.append_null(); }
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
        let mut sorted = BatchSort::new(source, self.schema, order.to_vec(), execution.clone(), input.projected, 0, i64::MAX as u64)?;
        while let Some(batch) = sorted.next_batch().await? {
            let _positions = memory.reserve(batch.len.checked_mul(2 * std::mem::size_of::<(usize, usize)>())
                .ok_or_else(|| invalid("aggregate-size", "Ordered list positions overflow"))?)?;
            let mut picks = Vec::with_capacity(batch.len);
            for row in 0..batch.len {
                context.check_cancelled_at(row)?;
                let value = match ScalarKey::read(&batch.columns[group], row) {
                    Some(ScalarKey::Signed(value)) => Some(value),
                    Some(ScalarKey::Null) => None,
                    _ => match batch.columns[group].value(row)? {
                        Value::I64(value) => Some(value),
                        Value::Null | Value::Undefined => None,
                        _ => return Err(invalid("aggregate-group", "Expected a group index for an ordered aggregate")),
                    },
                };
                if let Some(value) = value { picks.push((row, value as usize)); }
            }
            if !picks.is_empty() {
                let rows = picks.iter().map(|&(row, _)| (0, row)).collect::<Vec<_>>();
                let arguments = batch.key_projection(&[0], memory)?.compact_group_keys(&rows, memory)?;
                output.collect(&arguments, 0, picks.iter().enumerate().map(|(row, &(_, group))| (row, group)), groups, context)?;
            }
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

struct ScalarGroups { values: Vec<ScalarAccumulator>, reservation: Arc<rtdb_columnar::MemoryReservation> }
impl ScalarGroups {
    fn new(execution: &Execution) -> Result<Self> { Ok(Self { values: Vec::new(),reservation: execution.context.memory.reserve(0)? }) }
    fn ensure(&mut self,count: usize,execution: &Execution) -> Result<()> {
        if self.values.len() >= count { return Ok(()); }
        let bytes = count.checked_mul(std::mem::size_of::<ScalarAccumulator>()).ok_or_else(||invalid("aggregate-size","Aggregate group accumulator capacity overflow"))?;
        let reservation = execution.context.memory.reserve(bytes)?;
        self.values.reserve_exact(count - self.values.len());
        self.values.resize_with(count,ScalarAccumulator::default);
        self.reservation = reservation;
        Ok(())
    }
    fn aggregate(&mut self,page: Rows<'_>,mapping: &[usize],mask: &[bool],group_count: usize,argument: Option<usize>,aggregate: &AggregateExpr,types: &AggregateTypes,scope: &rtdb_expr::EvaluationScope,execution: &Execution) -> Result<()> {
        use AggregateFunction as A;
        match aggregate.function {
            A::Sum | A::Average => {
                let summer = SumFactory::new(&types.input,types.output_type())?;
                self.ensure(group_count,execution)?;
                self.sum_page(page,mapping,mask,argument,&summer,execution)?;
                if aggregate.function == A::Average { self.count_page(page,mapping,mask,argument,false,execution)?; }
            }
            A::VariancePopulation | A::VarianceSample | A::StddevPopulation | A::StddevSample => {
                let squares = square_column(page,argument,&types.input,scope,execution)?;
                let sum_type = LogicalType::Scalar(ScalarType::F64);
                let summer = SumFactory::new(&types.input,&sum_type)?;
                self.ensure(group_count,execution)?;
                self.sum_page(page,mapping,mask,argument,&summer,execution)?;
                for ((value,group),selected) in squares.into_iter().zip(mapping).zip(mask) {
                    execution.context.check_cancelled()?;
                    if *selected { if let Some(value) = value {
                        let state = &mut self.values[*group];
                        state.squares = Some(state.squares.map_or(value,|previous|previous + value));
                    } }
                }
                self.count_page(page,mapping,mask,argument,false,execution)?;
            }
            A::Count | A::CountStar => {
                self.ensure(group_count,execution)?;
                self.count_page(page,mapping,mask,argument,aggregate.function == A::CountStar,execution)?;
            }
            _ => {
                self.ensure(group_count,execution)?;
                for (row,(group,selected)) in mapping.iter().zip(mask).enumerate() {
                    execution.context.check_cancelled_at(row)?;
                    if *selected {
                        let value = page.value(row,argument)?;
                        self.values[*group].step(value.as_ref(),aggregate,types,execution)?;
                    }
                }
            }
        }
        Ok(())
    }
    fn sum_page(&mut self,page: Rows<'_>,mapping: &[usize],mask: &[bool],argument: Option<usize>,summer: &SumFactory<'_>,execution: &Execution) -> Result<()> {
        let _scratch = execution.context.memory.reserve(4096)?;
        for (index,(group,selected)) in mapping.iter().zip(mask).enumerate() {
            execution.context.check_cancelled_at(index)?;
            if !*selected { continue; }
            let value = page.value(index,argument)?;
            let state = &mut self.values[*group];
            if let Some(next) = summer.add(state.accumulator.as_ref().map(|row|&row.values[0]),value.as_ref(),index)? {
                match &mut state.accumulator {
                    // A sum that retains what the previous one did reuses its cell and charge.
                    Some(accumulator) if accumulator.values[0].retained_bytes()? == next.retained_bytes()? => accumulator.values[0] = next,
                    accumulator => *accumulator = Some(OwnedRow::new(vec![next],&execution.context.memory)?),
                }
            }
        }
        Ok(())
    }
    fn count_page(&mut self,page: Rows<'_>,mapping: &[usize],mask: &[bool],argument: Option<usize>,all: bool,execution: &Execution) -> Result<()> {
        for (row,(group,selected)) in mapping.iter().zip(mask).enumerate() {
            execution.context.check_cancelled_at(row)?;
            if *selected && (all || !page.is_null(row,argument)?) {
                let state = &mut self.values[*group];
                state.count = state.count.wrapping_add(1);
            }
        }
        Ok(())
    }
}
#[derive(Default)]
struct ScalarAccumulator { count: i64, accumulator: Option<OwnedRow>, squares: Option<f64> }
impl ScalarAccumulator {
    fn step(&mut self,value: &Value,aggregate: &AggregateExpr,types: &AggregateTypes,execution: &Execution) -> Result<()> {
        let _value = execution.context.memory.reserve(value.retained_bytes()?)?;
        let value = value.clone();
        if matches!(value, Value::Null | Value::Undefined) { return Ok(()); }
        use AggregateFunction as A;
        let previous = self.accumulator.as_ref().map(|row| &row.values[0]);
        let next = match &aggregate.function {
            A::Min | A::Max => {
                let value = cast(&value, types.output_type(), &execution.context.timezone)?;
                let replace = match previous { None => true, Some(previous) => {
                    let order = match (&value,previous) { (Value::Uri(left),Value::Uri(right)) => left.as_bytes().cmp(right.as_bytes()), _ => compare_in_timezone(&value, previous, false, &execution.context.timezone)? };
                    order == if aggregate.function == A::Min { Ordering::Less } else { Ordering::Greater }
                } };
                if replace { Some(value) } else { None }
            }
            A::Every | A::Any => {
                let previous = previous.and_then(|value| if let Value::Bool(value) = value { Some(*value) } else { None });
                let value = match (previous,&value) {
                    (None,Value::Bool(value)) => *value,
                    (None,_) => return Err(rtdb_types::Anomaly::unsupported("rtdb.query/boolean-setter","Reference boolean accumulator does not implement this value setter")),
                    (Some(previous),value) => if aggregate.function == A::Every { previous && !matches!(value,Value::Bool(false)) } else { previous || !matches!(value,Value::Bool(false)) },
                };
                Some(Value::Bool(value))
            }
            _ => unreachable!(),
        };
        if let Some(next) = next { self.accumulator = Some(OwnedRow::new(vec![next], &execution.context.memory)?); }
        Ok(())
    }
    fn finish(&self,aggregate: &AggregateExpr,output: &rtdb_expr::EvaluationScope) -> Result<Value> {
        use AggregateFunction as A;
        if let Some(accumulator) = &self.accumulator { output.reserve(accumulator.values[0].retained_bytes()?)?; }
        let value = match &aggregate.function {
            A::Count | A::CountStar => Value::I64(self.count),
            A::Average if self.count == 0 => Value::Null,
            A::Average => match self.accumulator.as_ref().map(|row| &row.values[0]) {
                Some(Value::Duration { value, unit }) => Value::Duration { value: value.wrapping_div(self.count), unit: *unit },
                Some(Value::F64(value)) => Value::F64(*value / self.count as f64),
                _ => Value::Null,
            },
            A::VariancePopulation | A::VarianceSample | A::StddevPopulation | A::StddevSample => {
                let sample = matches!(aggregate.function, A::VarianceSample | A::StddevSample);
                if let (Some(Value::F64(sum)),Some(squares)) = (self.accumulator.as_ref().map(|row|&row.values[0]),self.squares) {
                    if self.count < if sample { 2 } else { 1 } { return Ok(Value::Null); }
                    let variance = (squares - sum * sum / self.count as f64) / (self.count - i64::from(sample)) as f64;
                    Value::F64(if matches!(aggregate.function, A::StddevPopulation | A::StddevSample) { variance.sqrt() } else { variance })
                } else { Value::Null }
            }
            _ => self.accumulator.as_ref().map_or(Value::Null, |row| row.values[0].clone()),
        };
        Ok(value)
    }
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
    fn add(&self,previous: Option<&Value>,value: &Value,index: usize) -> Result<Option<Value>> {
        use ScalarType as S;
        let null = matches!(value,Value::Null | Value::Undefined);
        if null && matches!(without_nullability(self.input),LogicalType::Scalar(S::Duration(_))) {
            return Err(rtdb_types::Anomaly::fault("xtdb.error/unknown",format!("null at index {index}")));
        }
        if null && !matches!((without_nullability(self.input),self.output),
            (LogicalType::Scalar(S::Decimal { .. }),LogicalType::Scalar(S::Decimal { .. }))) { return Ok(None); }
        let output = match self.output {
            LogicalType::Scalar(S::I32) => {
                let value = signed_integer(value)? as i32;
                Value::I32(match previous { Some(Value::I32(previous)) => previous.wrapping_add(value),None => value,_ => return Err(sum_state()) })
            }
            LogicalType::Scalar(S::I64) => {
                let value = signed_integer(value)?;
                Value::I64(match previous { Some(Value::I64(previous)) => previous.wrapping_add(value),None => value,_ => return Err(sum_state()) })
            }
            LogicalType::Scalar(S::F32) => {
                let value = vector_float(value)?;
                Value::F32(match previous { Some(Value::F32(previous)) => previous + value,None => value,_ => return Err(sum_state()) })
            }
            LogicalType::Scalar(S::F64) => {
                let value = vector_double(value)?;
                Value::F64(match previous { Some(Value::F64(previous)) => previous + value,None => value,_ => return Err(sum_state()) })
            }
            LogicalType::Scalar(S::Decimal { precision,scale,bit_width }) => Value::Decimal(rtdb_expr::decimal_sum(previous.map(decimal_value).transpose()?,if null { None } else { Some(decimal_value(value)?) },*precision,*scale,*bit_width)?),
            LogicalType::Scalar(S::Duration(unit)) => {
                let value = match value {
                    Value::Duration { value,unit: input } if input <= unit => value.wrapping_mul(unit.units_per_second() / input.units_per_second()),
                    _ => return Err(sum_state()),
                };
                Value::Duration { value: match previous { Some(Value::Duration { value: previous,unit: previous_unit }) if previous_unit == unit => previous.wrapping_add(value),None => value,_ => return Err(sum_state()) },unit: *unit }
            }
            _ => return Err(sum_state()),
        };
        Ok(Some(output))
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

fn square_column(page: Rows<'_>,argument: Option<usize>,input: &LogicalType,scope: &rtdb_expr::EvaluationScope,execution: &Execution) -> Result<Vec<Option<f64>>> {
    if !matches!(without_nullability(input),LogicalType::Scalar(ScalarType::I32 | ScalarType::I64 | ScalarType::U32 | ScalarType::U64 | ScalarType::F64)) {
        return Err(rtdb_types::Anomaly::unsupported("rtdb.query/variance-input","Reference variance requires an IntVector, LongVector or DoubleVector"));
    }
    scope.reserve(page.len().checked_mul(std::mem::size_of::<Option<f64>>()).ok_or_else(||invalid("aggregate-size","Variance square column capacity overflow"))?)?;
    let mut squares = Vec::with_capacity(page.len());
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
