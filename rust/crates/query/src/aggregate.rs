use std::cmp::Ordering;
use std::sync::Arc;

use arrow_array::Array;
use async_trait::async_trait;
use rtdb_expr::{cast, compare_in_timezone, is_true, number, sql_eq_in_timezone};
use rtdb_plan::{AggregateExpr, AggregateFunction, Binder, NamedExpr, PercentileFraction, PercentileKind, Schema};
use rtdb_types::{LogicalType, Result, ScalarType, Value};

use crate::context::Execution;
use crate::cursor::{BoxCursor, OwnedRow, RowCursor};
use crate::operators::rows_equal;
use crate::pages::{BoxPageCursor, OwnedPage, PageBuilder, PageCursor};
use crate::runtime_schema::{EmissionMode, EmittedRelation};
use crate::{invalid, sort::SortCursor};

/// The rows an aggregate reads: a page, or the cells of the positions it reads, taken column by column from a batch.
#[derive(Clone, Copy)]
enum Rows<'a> { Page(&'a OwnedPage), Columns(&'a ColumnValues) }
/// One materialised column per input position an aggregate reads; positions it doesn't read are absent.
struct ColumnValues { columns: Vec<Option<Vec<Value>>>, len: usize, _reservation: Arc<rtdb_columnar::MemoryReservation> }
static NULL: Value = Value::Null;
impl<'a> Rows<'a> {
    fn len(&self) -> usize { match self { Self::Page(page) => page.len(), Self::Columns(columns) => columns.len } }
    fn value(&self, row: usize, position: Option<usize>) -> &'a Value {
        let Some(position) = position else { return &NULL };
        match self {
            Self::Page(page) => &page.rows()[row].values[position],
            Self::Columns(columns) => columns.columns.get(position).and_then(Option::as_ref).map_or(&NULL, |column| &column[row]),
        }
    }
    fn page(&self) -> Result<&'a OwnedPage> { match self { Self::Page(page) => Ok(page), Self::Columns(_) => Err(invalid("aggregate-input", "This aggregate reads whole rows")) } }
}
impl ColumnValues {
    fn new(batch: &crate::columnar::ColumnBatch, positions: &[usize], admit_dates: bool, memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<Self> {
        let mut bytes = 256usize;
        for &position in positions { for row in 0..batch.len { bytes = bytes.checked_add(batch.columns[position].bound(row)?).ok_or_else(||invalid("aggregate-size","Aggregate input exceeds addressable memory"))?; } }
        let reservation = memory.reserve(bytes)?;
        let mut columns: Vec<Option<Vec<Value>>> = (0..batch.columns.len()).map(|_| None).collect();
        for &position in positions {
            if columns[position].is_some() { continue; }
            columns[position] = Some((0..batch.len).map(|row| {
                let mut value = batch.columns[position].value(row)?;
                if admit_dates { value.admit_expression_dates()?; }
                Ok(value)
            }).collect::<Result<_>>()?);
        }
        Ok(Self { columns, len: batch.len, _reservation: reservation })
    }
}

/// Where an aggregate's input comes from; batches skip materialising rows when every aggregate reads only its argument.
enum AggregateInput { Pages(BoxPageCursor), Batches { batches: crate::columnar::BoxBatchCursor, projected: bool } }

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
impl AggregateCursor {
    pub fn new(input: BoxPageCursor, schema: Schema, groups: Vec<NamedExpr>, aggregates: Vec<AggregateExpr>, runtime: Arc<EmittedRelation>, execution: Execution) -> Result<Self> {
        Self::with_input(AggregateInput::Pages(input), schema, groups, aggregates, runtime, execution)
    }
    /// `projected` when the batches passed through a column projection, which admits each cell's dates as it copies it.
    pub fn from_batches(input: crate::columnar::BoxBatchCursor, projected: bool, schema: Schema, groups: Vec<NamedExpr>, aggregates: Vec<AggregateExpr>, runtime: Arc<EmittedRelation>, execution: Execution) -> Result<Self> {
        Self::with_input(AggregateInput::Batches { batches: input, projected }, schema, groups, aggregates, runtime, execution)
    }
    fn with_input(input: AggregateInput, schema: Schema, groups: Vec<NamedExpr>, mut aggregates: Vec<AggregateExpr>, runtime: Arc<EmittedRelation>, execution: Execution) -> Result<Self> {
        if runtime.aggregates()?.len() != aggregates.len() { return Err(invalid("runtime-plan", "Aggregate factories disagree with their expressions")); }
        for aggregate in &mut aggregates {
            aggregate.function = aggregate.function.canonical();
            if let AggregateFunction::Percentile { fraction, .. } = &mut aggregate.function {
                if let PercentileFraction::Integer(value) = *fraction { *fraction = PercentileFraction::Literal(value as f64); }
                if let PercentileFraction::Parameter { index } = *fraction {
                    let _scratch = execution.context.memory.reserve(4096)?;
                    let value = execution.parameters.values().get(index).unwrap_or(&Value::Null);
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
    fn column_positions(&self) -> Option<Vec<usize>> { Self::reads_columns(&self.schema, &self.groups, &self.aggregates) }
    /// The input positions every aggregate and group reads, when each reads only its argument or key column.
    pub fn reads_columns(schema: &Schema, groups: &[NamedExpr], aggregates: &[AggregateExpr]) -> Option<Vec<usize>> {
        let mut positions = Vec::new();
        for group in groups {
            let rtdb_plan::Expr::Column { id, .. } = &group.expr else { return None };
            positions.push(schema.position(*id)?);
        }
        for aggregate in aggregates {
            if aggregate.filter.is_some() || aggregate.distinct || !aggregate.order_by.is_empty() { return None; }
            if matches!(aggregate.function.canonical(), AggregateFunction::ArrayAgg | AggregateFunction::VecAgg | AggregateFunction::Percentile { .. }) { return None; }
            if let Some(argument) = &aggregate.argument { positions.push(argument_position(argument, schema).ok()?); }
        }
        Some(positions)
    }

    async fn aggregate(&mut self, mut input: AggregateInput) -> Result<Option<OwnedPage>> {
        let memory = &self.execution.context.memory;
        let mut keys = GroupKeys::new(self.groups.is_empty())?;
        let _columns = memory.reserve(self.aggregates.len().checked_mul(std::mem::size_of::<AggregateColumn>()).ok_or_else(||invalid("aggregate-size","Aggregate column container overflow"))?)?;
        let mut columns = self.aggregates.iter().map(|aggregate|AggregateColumn::new(aggregate,&self.execution)).collect::<Result<Vec<_>>>()?;
        let positions = self.column_positions();
        let mut typed_groups = TypedGroups::default();
        loop {
            let (page, values);
            let rows = match &mut input {
                AggregateInput::Pages(pages) => { let Some(next) = pages.next_page().await? else { break }; page = next; Rows::Page(&page) }
                AggregateInput::Batches { batches, projected } => {
                    let Some(batch) = batches.next_batch().await? else { break };
                    if let Some(positions) = &positions && self.typed_batch(&batch,positions,&mut keys,&mut columns,&mut typed_groups)? { continue; }
                    match &positions {
                        Some(positions) => { values = ColumnValues::new(&batch, positions, *projected, memory)?; Rows::Columns(&values) }
                        None => return Err(invalid("aggregate-input", "A batch input reached an aggregate that reads whole rows")),
                    }
                }
            };
            self.execution.context.check_cancelled()?;
            let scope = self.execution.scope()?;
            scope.reserve(rows.len().checked_mul(std::mem::size_of::<usize>()).ok_or_else(||invalid("aggregate-size","Group mapping overflow"))?)?;
            let mapping = if self.groups.is_empty() { vec![0; rows.len()] } else {
                for field in &self.runtime.schema().fields()[..self.groups.len()] { rtdb_plan::validate_equality_types(&field.data_type,&field.data_type)?; }
                let mut mapping = Vec::with_capacity(rows.len());
                match rows {
                    Rows::Page(page) => {
                        let projected = crate::pages::project(page,&self.schema,self.groups.iter().map(|group|&group.expr),&self.execution).await?;
                        for key in projected.rows() { mapping.push(keys.insert(&key.values,&self.execution)?); }
                    }
                    Rows::Columns(_) => {
                        let key_positions = &positions.as_ref().expect("column rows have positions")[..self.groups.len()];
                        let mut key = Vec::with_capacity(key_positions.len());
                        for row in 0..rows.len() {
                            // A single key cell that admitting would leave as it is groups in place, without a copy.
                            if let [position] = key_positions {
                                let value = rows.value(row, Some(*position));
                                if !matches!(value, Value::Date(_) | Value::List(_) | Value::Set(_) | Value::FixedSizeList(_) | Value::Struct(_) | Value::Map { .. }) {
                                    mapping.push(keys.insert(std::slice::from_ref(value),&self.execution)?);
                                    continue;
                                }
                            }
                            key.clear();
                            for position in key_positions {
                                let mut value = rows.value(row, Some(*position)).clone();
                                value.admit_expression_dates()?;
                                key.push(value);
                            }
                            mapping.push(keys.insert(&key,&self.execution)?);
                        }
                    }
                }
                mapping
            };
            for ((column,aggregate),types) in columns.iter_mut().zip(&self.aggregates).zip(self.runtime.aggregates()?) {
                column.aggregate(rows,&mapping,keys.count,aggregate,types,&self.schema,&self.execution).await?;
            }
        }
        let mut output = PageBuilder::new(keys.count,memory)?;
        if keys.global { output.push(OwnedRow::copy_parts(&[],0,self.aggregates.len(),memory)?)?; }
        for (index,key) in keys.keys.iter().enumerate() {
            self.execution.context.check_cancelled_at(index)?;
            output.push(OwnedRow::copy_parts(&[&key.values[2..]],0,self.aggregates.len(),memory)?)?;
        }
        for (index,((column,aggregate),types)) in columns.into_iter().zip(&self.aggregates).zip(self.runtime.aggregates()?).enumerate() {
            column.finish(&mut output,self.groups.len() + index,aggregate,types,&self.schema,&self.execution).await?;
        }
        Ok((output.len() > 0).then(||output.finish()))
    }
}
#[async_trait]
impl PageCursor for AggregateCursor {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.execution.context.check_cancelled()?;
        let Some(input) = self.input.take() else { return Ok(None); };
        self.aggregate(input).await
    }
}

/// Group ids already found for a single integer or string key, by the key read straight from its array.
/// A key absent here is found through `GroupKeys` and remembered, so the groups and their first-seen keys are its.
#[derive(Default)]
struct TypedGroups { ints: std::collections::HashMap<i64, usize>, strings: std::collections::HashMap<String, usize>, null: Option<usize> }

/// A batch column an aggregate reads straight from its array.
enum TypedColumn<'a> { Int(Vec<Option<i64>>), Utf8(&'a arrow_array::StringArray) }
impl<'a> TypedColumn<'a> {
    fn of(column: &'a crate::columnar::Column) -> Option<Self> {
        let crate::columnar::Column::Arrow { array, field, .. } = column else { return None };
        if !field.metadata().is_empty() { return None; }
        if let Some(strings) = array.as_any().downcast_ref::<arrow_array::StringArray>() { return Some(Self::Utf8(strings)); }
        crate::batch_join::int_cells(array.as_ref()).map(Self::Int)
    }
}

/// A plain float column, or a plain column whose nulls the array marks, as typed aggregates read them.
fn float_column(column: &crate::columnar::Column) -> Option<&arrow_array::Float64Array> {
    let crate::columnar::Column::Arrow { array, field, .. } = column else { return None };
    if !field.metadata().is_empty() { return None; }
    array.as_any().downcast_ref::<arrow_array::Float64Array>()
}
fn marked_nulls(column: &crate::columnar::Column) -> Option<&dyn arrow_array::Array> {
    let crate::columnar::Column::Arrow { array, field, .. } = column else { return None };
    (field.metadata().is_empty() && !matches!(array.data_type(), arrow_schema::DataType::Union(..) | arrow_schema::DataType::Null)).then_some(array.as_ref())
}

impl AggregateCursor {
    /// Aggregates a batch straight from its arrays where its single group key is an integer or string column and every
    /// aggregate is a count or a float sum or average; otherwise leaves the batch for the value path and returns false.
    fn typed_batch(&self,batch: &crate::columnar::ColumnBatch,positions: &[usize],keys: &mut GroupKeys,columns: &mut [AggregateColumn],typed: &mut TypedGroups) -> Result<bool> {
        use AggregateFunction as A;
        let key = match self.groups.len() {
            0 => None,
            1 => match TypedColumn::of(&batch.columns[positions[0]]) { Some(column) => Some(column), None => return Ok(false) },
            _ => return Ok(false),
        };
        let types = self.runtime.aggregates()?;
        let float = LogicalType::Scalar(ScalarType::F64);
        for (aggregate,types) in self.aggregates.iter().zip(types) {
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
        let memory = &self.execution.context.memory;
        let mapping = match &key {
            None => vec![0; batch.len],
            Some(key) => {
                for field in &self.runtime.schema().fields()[..1] { rtdb_plan::validate_equality_types(&field.data_type,&field.data_type)?; }
                let _mapping = memory.reserve(batch.len.checked_mul(std::mem::size_of::<usize>()).ok_or_else(||invalid("aggregate-size","Group mapping overflow"))?)?;
                let mut mapping = Vec::with_capacity(batch.len);
                for row in 0..batch.len {
                    self.execution.context.check_cancelled_at(row)?;
                    let known = match key {
                        TypedColumn::Int(cells) => match cells[row] { Some(cell) => typed.ints.get(&cell).copied(), None => typed.null },
                        TypedColumn::Utf8(strings) => if strings.is_null(row) { typed.null } else { typed.strings.get(strings.value(row)).copied() },
                    };
                    let group = match known {
                        Some(group) => group,
                        None => {
                            let value = batch.columns[positions[0]].value(row)?;
                            let group = keys.insert(std::slice::from_ref(&value),&self.execution)?;
                            match key {
                                TypedColumn::Int(cells) => match cells[row] { Some(cell) => { typed.ints.insert(cell,group); } None => typed.null = Some(group) },
                                TypedColumn::Utf8(strings) => if strings.is_null(row) { typed.null = Some(group) } else { typed.strings.insert(strings.value(row).to_owned(),group); },
                            }
                            group
                        }
                    };
                    mapping.push(group);
                }
                mapping
            }
        };
        let group_count = keys.count;
        for ((column,aggregate),types) in columns.iter_mut().zip(&self.aggregates).zip(types) {
            validate_aggregate_input(aggregate,types)?;
            let Accumulation::Scalar(states) = &mut column.accumulation else { return Err(invalid("aggregate-input","A typed aggregate collects its rows")) };
            states.ensure(group_count,&self.execution)?;
            let argument = aggregate.argument.as_ref().map(|argument|argument_position(argument,&self.schema)).transpose()?;
            match (&aggregate.function,argument) {
                (A::CountStar,_) => for group in &mapping { states.values[*group].count = states.values[*group].count.wrapping_add(1); },
                (A::Count,Some(position)) => {
                    let array = marked_nulls(&batch.columns[position]).ok_or_else(||invalid("aggregate-input","A typed count lost its column"))?;
                    for (row,group) in mapping.iter().enumerate() { if !array.is_null(row) { states.values[*group].count = states.values[*group].count.wrapping_add(1); } }
                }
                (A::Sum | A::Average,Some(position)) => {
                    let array = float_column(&batch.columns[position]).ok_or_else(||invalid("aggregate-input","A typed sum lost its column"))?;
                    // Each group's sum folds this batch's cells onto its running sum in row order, as `SumFactory::add` does.
                    let mut sums: Vec<Option<f64>> = states.values.iter().map(|state| match state.accumulator.as_ref().map(|row|&row.values[0]) { Some(Value::F64(sum)) => Some(*sum), _ => None }).collect();
                    let mut touched = vec![false; group_count];
                    for (row,group) in mapping.iter().enumerate() {
                        if array.is_null(row) { continue; }
                        let value = array.value(row);
                        sums[*group] = Some(sums[*group].map_or(value,|sum| sum + value));
                        touched[*group] = true;
                        if aggregate.function == A::Average { states.values[*group].count = states.values[*group].count.wrapping_add(1); }
                    }
                    for (group,sum) in sums.into_iter().enumerate() {
                        let (true, Some(sum)) = (touched[group], sum) else { continue };
                        match &mut states.values[group].accumulator {
                            Some(accumulator) => accumulator.values[0] = Value::F64(sum),
                            accumulator => *accumulator = Some(OwnedRow::new(vec![Value::F64(sum)],memory)?),
                        }
                    }
                }
                _ => return Err(invalid("aggregate-input","A typed aggregate has no typed accumulator")),
            }
        }
        Ok(true)
    }
}

/// Group keys in first-seen order, with an index from key hash to the groups that share it.
struct GroupKeys { count: usize, global: bool, index: std::collections::HashMap<u32, Vec<usize>>, keys: Vec<OwnedRow>, _index_reservation: Option<Arc<rtdb_columnar::MemoryReservation>>, inserts: usize }
impl GroupKeys {
    fn new(global: bool) -> Result<Self> {
        Ok(Self { count: usize::from(global),global,index: Default::default(),keys: Vec::new(),_index_reservation: None,inserts: 0 })
    }
    fn insert(&mut self,values: &[Value],execution: &Execution) -> Result<usize> {
        self.inserts = self.inserts.wrapping_add(1);
        execution.context.check_cancelled_at(self.inserts - 1)?;
        if self.global { return Ok(0); }
        let hash = crate::join_storage::hash_values(values)?;
        if let Some(candidates) = self.index.get(&hash) {
            for &id in candidates {
                if rows_equal(values,&self.keys[id].values[2..],&execution.context.timezone,&execution.context.memory)? { return Ok(id); }
            }
        }
        let id = self.count;
        self.count = self.count.checked_add(1).filter(|count|*count <= i32::MAX as usize).ok_or_else(||invalid("group-count","Aggregate group count exceeds the reference vector limit"))?;
        let key = OwnedRow::copy_parts(&[&[Value::U64(id as u64),Value::U32(hash)],values],0,0,&execution.context.memory)?;
        if self.keys.len() == self.keys.capacity() {
            let capacity = self.keys.capacity().saturating_mul(2).max(16);
            let bytes = capacity.checked_mul(std::mem::size_of::<OwnedRow>() + std::mem::size_of::<(u32, Vec<usize>)>() + 2 * std::mem::size_of::<usize>())
                .ok_or_else(||invalid("group-count","Aggregate group index exceeds addressable memory"))?;
            self._index_reservation = Some(execution.context.memory.reserve(bytes)?);
            self.keys.reserve_exact(capacity - self.keys.len());
        }
        self.keys.push(key);
        self.index.entry(hash).or_default().push(id);
        Ok(id)
    }
}

struct AggregateColumn { accumulation: Accumulation, distinct: Option<DistinctSeen> }
/// The distinct arguments each group has taken so far, by the group and the hash of the argument.
struct DistinctSeen { index: std::collections::HashMap<(usize, u32), Vec<usize>>, values: Vec<OwnedRow> }
enum Accumulation { Scalar(ScalarGroups), Collected(PageBuilder) }
impl AggregateColumn {
    fn new(aggregate: &AggregateExpr,execution: &Execution) -> Result<Self> {
        let accumulation = match aggregate.function {
            AggregateFunction::ArrayAgg | AggregateFunction::VecAgg | AggregateFunction::Percentile { .. } => Accumulation::Collected(PageBuilder::new(0,&execution.context.memory)?),
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
            for (row,selected) in input.page()?.rows().iter().zip(&mut mask) {
                let row_scope = execution.scope()?;
                *selected = is_true(&execution.evaluate_invocation(&row_scope,&invocation,&row.values).await?)?;
            }
        }
        let argument = aggregate.argument.as_ref().map(|argument|argument_position(argument,schema)).transpose()?;
        if let Some(seen) = &mut self.distinct {
            let mut admitted = false;
            for ((row,group),selected) in input.page()?.rows().iter().zip(mapping).zip(&mut mask) {
                execution.context.check_cancelled()?;
                if !*selected { continue; }
                if !admitted { rtdb_plan::validate_equality_types(&types.input,&types.input)?; admitted = true; }
                let value = argument.map(|position|&row.values[position]).unwrap_or(&Value::Null);
                let hash = crate::join_storage::hash_values(std::iter::once(value))?;
                if let Some(candidates) = seen.index.get(&(*group, hash)) {
                    for &candidate in candidates {
                        let allocation = rtdb_expr::EvaluationScope::new(&execution.context.memory)?;
                        if sql_eq_in_timezone(value,&seen.values[candidate].values[0],&execution.context.timezone,&allocation)? == Some(true) { *selected = false; break; }
                    }
                }
                if *selected {
                    seen.index.entry((*group, hash)).or_default().push(seen.values.len());
                    seen.values.push(OwnedRow::copy_parts(&[std::slice::from_ref(value)],0,0,&execution.context.memory)?);
                }
            }
        }
        validate_aggregate_input(aggregate,types)?;
        match &mut self.accumulation {
            Accumulation::Scalar(states) => {
                states.aggregate(input,mapping,&mask,group_count,argument,aggregate,types,&scope,execution)?;
            }
            Accumulation::Collected(rows) => {
                for ((row,group),selected) in input.page()?.rows().iter().zip(mapping).zip(mask) {
                    execution.context.check_cancelled()?;
                    let row = OwnedRow::copy_parts(&[&row.values,&[Value::U64(*group as u64),Value::Bool(selected)]],0,0,&execution.context.memory)?;
                    rows.push_growing(row,&execution.context.memory)?;
                }
            }
        }
        Ok(())
    }
    async fn finish(self,output: &mut PageBuilder,column: usize,aggregate: &AggregateExpr,types: &AggregateTypes,schema: &Schema,execution: &Execution) -> Result<()> {
        match self.accumulation {
            Accumulation::Scalar(mut states) => {
                states.ensure(output.len(),execution)?;
                for (state,row) in states.values.iter().zip(output.rows_mut()) {
                    execution.context.check_cancelled()?;
                    let scope = execution.scope()?;
                    let value = state.finish(aggregate,&scope)?;
                    row.replace_value(column,value,&execution.context.memory)?;
                }
            }
            Accumulation::Collected(rows) => {
                if matches!(aggregate.function,AggregateFunction::Percentile { .. }) {
                    let declared = aggregate.argument.as_ref().map_or(LogicalType::Null,rtdb_plan::Expr::data_type);
                    rtdb_expr::validate_order_type(&declared.join(&types.input))?;
                }
                if aggregate.function == AggregateFunction::ArrayAgg {
                    let scope = execution.scope()?;
                    for order in &aggregate.order_by {
                        let ty = execution.expression_type(&scope,&order.expr,schema)?;
                        rtdb_expr::validate_order_type(&ty)?;
                    }
                }
                let present = rows.len() > 0;
                let mut binder = Binder::after(schema)?;
                let group_field = binder.field("_aggregate_group",None,LogicalType::Scalar(ScalarType::U64))?;
                let mask_field = binder.field("_aggregate_selected",None,LogicalType::Scalar(ScalarType::Bool))?;
                let stored_schema = schema.concat(&Schema::new(vec![group_field,mask_field])?)?;
                let input: BoxPageCursor = Box::new(CollectedPage(Some(rows.finish())));
                let mut input: BoxCursor = if aggregate.function == AggregateFunction::ArrayAgg && !aggregate.order_by.is_empty() {
                    Box::new(SortCursor::from_pages(input,stored_schema,aggregate.order_by.clone(),execution.clone()))
                } else { crate::pages::rows(input,Arc::clone(&execution.context)) };
                // One pass splits the ordered rows into each group's selected arguments, in order.
                let argument = argument_position(aggregate.argument.as_ref().ok_or_else(||invalid("aggregate-state","Collected aggregate argument is absent"))?,schema)?;
                let width = schema.len();
                let mut buckets: Vec<std::collections::VecDeque<OwnedRow>> = (0..output.len()).map(|_| std::collections::VecDeque::new()).collect();
                while let Some(row) = input.next().await? {
                    execution.context.check_cancelled()?;
                    if row.values.get(width + 1) != Some(&Value::Bool(true)) { continue; }
                    let Some(bucket) = buckets.get_mut(group_id(&row.values[width..])?) else { continue };
                    bucket.push_back(OwnedRow::copy_parts(&[std::slice::from_ref(&row.values[argument])],0,0,&execution.context.memory)?);
                }
                drop(input);
                for (row,bucket) in output.rows_mut().iter_mut().zip(buckets) {
                    execution.context.check_cancelled()?;
                    let scope = execution.scope()?;
                    let selected = CollectedRows(bucket);
                    let value = if let AggregateFunction::Percentile { kind,fraction: PercentileFraction::Literal(fraction),descending } = &aggregate.function {
                        percentile(Box::new(selected),*kind,*fraction,*descending,&scope,execution).await?
                    } else {
                        let mut selected = selected;
                        let mut values = Vec::new();
                        while let Some(value) = selected.next().await? {
                            let value = &value.values[0];
                            if values.len() == values.capacity() {
                                let capacity = values.len().checked_mul(2).map(|capacity|capacity.max(1)).ok_or_else(||invalid("aggregate-size","Array aggregate capacity overflow"))?;
                                scope.reserve(capacity.checked_mul(std::mem::size_of::<Value>()).ok_or_else(||invalid("aggregate-size","Array aggregate capacity overflow"))?)?;
                                values.reserve_exact(capacity - values.len());
                            }
                            scope.reserve(value.retained_bytes()?)?;
                            values.push(if matches!(value,Value::Undefined) { Value::Null } else { value.clone() });
                        }
                        if values.is_empty() && (aggregate.function == AggregateFunction::ArrayAgg || present) { Value::Null } else { Value::list(values) }
                    };
                    row.replace_value(column,value,&execution.context.memory)?;
                }
            }
        }
        Ok(())
    }
}

struct CollectedPage(Option<OwnedPage>);
#[async_trait]
impl PageCursor for CollectedPage {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> { Ok(self.0.take()) }
}
/// One group's selected arguments, in order.
struct CollectedRows(std::collections::VecDeque<OwnedRow>);
#[async_trait]
impl RowCursor for CollectedRows {
    async fn next(&mut self) -> Result<Option<OwnedRow>> { Ok(self.0.pop_front()) }
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
                        let value = page.value(row,argument);
                        self.values[*group].step(value,aggregate,types,execution)?;
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
            let value = page.value(index,argument);
            let state = &mut self.values[*group];
            if let Some(next) = summer.add(state.accumulator.as_ref().map(|row|&row.values[0]),value,index)? {
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
            let value = page.value(row,argument);
            if *selected && (all || !matches!(value,Value::Null | Value::Undefined)) {
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

async fn percentile(selected: BoxCursor, kind: PercentileKind, fraction: f64, descending: bool, output: &rtdb_expr::EvaluationScope, execution: &Execution) -> Result<Value> {
    let mut input = PercentileRows { input: selected, execution: execution.clone() };
    let mut values = crate::sort::RecordBuffer::new(Arc::clone(&execution.context))?;
    while let Some(row) = input.next().await? {
        execution.context.check_cancelled()?;
        values.push(row)?;
    }
    drop(input);
    let values = values.finish()?;
    let count = i32::try_from(values.len()).map_err(|_|invalid("percentile-size","Percentile input exceeds the reference vector row limit"))?;
    if count == 0 { return Ok(Value::Null); }
    let indices = crate::sort::sort_records(&values,&execution.context,1,|left,right| {
        let (left,right) = if descending { (right,left) } else { (left,right) };
        rtdb_expr::ordered_compare_in_timezone(&left[0],&right[0],false,&execution.context.timezone)
    })?;
    let (low, high, weight) = percentile_ranks(kind, fraction, count)?;
    let mut access = values.access()?;
    let low_row = access.get(indices.values[low as usize])?;
    if kind == PercentileKind::Discrete {
        let value = &low_row.values()[0];
        output.reserve(value.retained_bytes()?.checked_mul(2).ok_or_else(|| invalid("aggregate-size", "Aggregate result exceeds addressable memory"))?)?;
        return execution.evaluate(output, &rtdb_plan::Expr::Literal(value.clone()), &Schema::default(), &[]).await;
    }
    let lower = number(&low_row.values()[0]).ok_or_else(||invalid("percentile-input","Continuous percentile requires numeric values"))?;
    if low == high { return Ok(Value::F64(lower)); }
    let high_row = access.get(indices.values[high as usize])?;
    let upper = number(&high_row.values()[0]).ok_or_else(||invalid("percentile-input","Continuous percentile requires numeric values"))?;
    Ok(Value::F64(lower + weight * (upper - lower)))
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

struct PercentileRows { input: BoxCursor, execution: Execution }
#[async_trait]
impl RowCursor for PercentileRows {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        while let Some(row) = self.input.next().await? {
            self.execution.context.check_cancelled()?;
            if matches!(row.values[0],Value::Null | Value::Undefined) { continue; }
            return Ok(Some(row));
        }
        Ok(None)
    }
}
fn group_id(row: &[Value]) -> Result<usize> {
    match row.first() { Some(Value::U64(id)) => usize::try_from(*id).map_err(|_|invalid("invalid-spill","Aggregate group ID exceeds addressable memory")),_ => Err(invalid("invalid-spill","Aggregate record lacks a group ID")) }
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
        && types.declared_output.legs().iter().any(|leg| matches!(leg, LogicalType::Scalar(ScalarType::Decimal { .. } | ScalarType::I8 | ScalarType::I16 | ScalarType::U8 | ScalarType::U16 | ScalarType::U32 | ScalarType::U64 | ScalarType::F16 | ScalarType::Time { .. }))) {
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
        let value = page.value(row,argument);
        squares.push(if matches!(value,Value::Null | Value::Undefined) { None } else { let value = vector_double(value)?;Some(value * value) });
    }
    Ok(squares)
}

#[cfg(test)]
mod page_tests;
