use rtdb_types::StructFields;
use std::{future::Future, pin::Pin, sync::Arc, task::{Context, Poll, Waker}};
use rtdb_columnar::{ArrowCell, MemoryReservation};
use rtdb_plan::{BinaryOp, CastTarget, Expr, Schema, UnaryOp};
use rtdb_types::{Anomaly, ListKind, ListValue, LogicalType, Result, Value};
use super::invocation::{PreparedList, Replacements};
use super::{EvalContext, ExpressionRow, boolean, integer, nullish, overflow};

mod compare;
mod computed;
mod calls;
mod packed;
mod scalar;

type Pending<'a,T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

#[derive(Clone,Copy,PartialEq,Eq)]
enum Kind { List, Set, Fixed }

#[derive(Clone)]
enum EvalValue<'a> {
    Scalar(Value),
    Struct(Arc<StructFields<EvalValue<'a>>>),
    List(Kind,Arc<ListReader<'a>>),
}

enum ListReader<'a> {
    Expressions(&'a [Expr]),
    Values(Vec<Value>),
    Stored(rtdb_types::ListValue),
    Prepared(&'a PreparedList),
    Arrow { cell: ArrowCell<'a>, backing: Option<&'a Arc<MemoryReservation>> },
    Cast { input: EvalValue<'a>, element: Target<'a>, union_admitted: bool },
    Trim { input: EvalValue<'a>, size: i64 },
    IntegerSeries(crate::series::IntegerSeries),
    RangeBins(crate::temporal_functions::RangeBins),
}

impl ListReader<'_> {
    fn stored(&self) -> Option<&rtdb_types::ListValue> {
        match self {
            Self::Stored(items) => Some(items),
            Self::Prepared(list) => Some(&list.values),
            _ => None,
        }
    }
}

struct PackedList {
    values: ListValue,
    backing: Option<Arc<MemoryReservation>>,
}

impl From<ListValue> for PackedList {
    fn from(values: ListValue) -> Self { Self { values, backing: None } }
}

struct EvaluationFrame<'a> { schema: &'a Schema, row: ExpressionRow<'a>, context: &'a EvalContext<'a>, asynchronous: bool, replacements: Option<&'a super::invocation::Replacements<'a>> }

impl<'a> EvaluationFrame<'a> {
    fn arrow_input(&self, expr: &Expr) -> Option<ArrowCell<'a>> {
        match expr {
            Expr::Column { id, .. } => self.row.arrow_cell(self.schema.position(*id)?),
            Expr::Field { expr, name, .. } => self.arrow_input(expr)?.structure()?
                .find(|child| child.field.name() == name),
            Expr::Parameter { index, .. } => self.context.parameters.arrow_cell(*index)
                .filter(|cell| matches!(cell.sequence_kind(), Some(ListKind::List | ListKind::FixedSizeList(_)))),
            _ => None,
        }
    }

    fn arrow_backing(&self, mut expr: &Expr) -> Option<&'a Arc<MemoryReservation>> {
        while let Expr::Field { expr: parent, .. } = expr { expr = parent; }
        let Expr::Parameter { index, .. } = expr else { return None; };
        self.context.parameters.arrow_backing(*index)
    }

    fn prepared_list(&self, expr: &Expr) -> Option<&'a PreparedList> {
        self.replacements.and_then(|invocation| invocation.list(expr))
    }
}

fn packed_numeric_cell(cell: ArrowCell<'_>) -> bool {
    use arrow_schema::DataType;
    let Ok(cell) = cell.physical() else { return false; };
    matches!(cell.sequence_kind(), Some(ListKind::List | ListKind::FixedSizeList(_)))
        && matches!(cell.field.data_type(), DataType::List(child) | DataType::FixedSizeList(child, _)
            if rtdb_types::ListValue::supports_numeric_arrow(child))
}

#[derive(Clone,Copy)]
enum Target<'a> { Cast(&'a CastTarget), Fixed(&'a LogicalType) }

fn target_bound(target: &CastTarget) -> Result<usize> {
    let bytes = match target {
        CastTarget::Fixed(ty) => ty.retained_bytes()?,
        CastTarget::List(element) => target_bound(element)?,
        CastTarget::Checked { requested,target } => requested.retained_bytes()?.checked_add(target_bound(target)?).ok_or_else(overflow)?,
        CastTarget::Deferred(target) => target.retained_bytes()?,
        CastTarget::Temporal(rtdb_plan::TemporalCast { kind: rtdb_plan::TemporalKind::TimestampZone(rtdb_plan::CastTimezone::Explicit(zone)), .. }) => zone.len(),
        _ => 0,
    };
    bytes.checked_add(4096).ok_or_else(overflow)
}

fn value_type_bound(value: &Value) -> Result<usize> {
    let mut bytes = 512usize;
    let mut add = |value: usize| -> Result<()> { bytes = bytes.checked_add(value).ok_or_else(overflow)?; Ok(()) };
    match value {
        Value::Set(values) => for value in values { add(value_type_bound(value)?)?; },
        Value::List(values) | Value::FixedSizeList(values) => if let rtdb_types::ListView::Values(values) = values.view() {
            for value in values { add(value_type_bound(value)?)?; }
        },
        Value::Struct(fields) => for (name, value) in fields { add(name.len())?; add(value_type_bound(value)?)?; },
        Value::Map { entries, .. } => for (key, value) in entries { add(value_type_bound(key)?)?; add(value_type_bound(value)?)?; },
        Value::Timestamp { timezone: Some(zone), .. } => add(zone.len())?,
        _ => {}
    }
    Ok(bytes)
}

pub(super) fn expression_type_bound(expr: &Expr, prepared: Option<&Replacements<'_>>) -> Result<usize> {
    if let Some(list) = prepared.and_then(|invocation| invocation.list(expr)) { return Ok(list.type_bound); }
    stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || expression_type_bound_at(expr, prepared))
}
fn expression_type_bound_at(expr: &Expr, prepared: Option<&Replacements<'_>>) -> Result<usize> {
    let bound = |expr| expression_type_bound(expr, prepared);
    let mut bytes = 512usize;
    let mut add = |value: usize| -> Result<()> { bytes = bytes.checked_add(value).ok_or_else(overflow)?;Ok(()) };
    match expr {
        Expr::Literal(value) => add(value_type_bound(value)?)?,
        Expr::DurationLiteral(_) => {},
        Expr::Column { data_type,.. } | Expr::Parameter { data_type,.. } | Expr::Correlated { data_type,.. } => add(data_type.retained_bytes()?)?,
        Expr::Field { data_type,expr,.. } => { add(data_type.retained_bytes()?)?;add(bound(expr)?)?; }
        Expr::Subscript { data_type,expr,index } => { add(data_type.retained_bytes()?)?;add(bound(expr)?)?;add(bound(index)?)?; }
        Expr::Unary { expr,.. } => add(bound(expr)?)?,
        Expr::Cast { expr,target } => { add(bound(expr)?)?;add(target_bound(target)?)?; }
        Expr::Binary { left,right,.. } => { add(bound(left)?)?;add(bound(right)?)?; }
        Expr::Call { name,args,return_type } => { add(name.len())?;add(return_type.retained_bytes()?)?;for expr in args { add(bound(expr)?)?; } }
        Expr::RegexReplace { source,return_type,.. } => { add(return_type.retained_bytes()?)?;add(bound(source)?)?; }
        Expr::List(items) | Expr::Coalesce(items) => for expr in items { add(bound(expr)?)?; },
        Expr::Struct(fields) => for (name,expr) in fields { add(name.len())?;add(bound(expr)?)?; },
        Expr::RowCopy(fields) => for field in fields { add(field.name.len())?;add(field.data_type.retained_bytes()?)?; },
        Expr::Between { operand,low,high,.. } => { add(bound(operand)?)?;add(bound(low)?)?;add(bound(high)?)?; }
        Expr::SimpleCase { operand,branches,otherwise } => { add(bound(operand)?)?;for (candidate,value) in branches { add(bound(candidate)?)?;add(bound(value)?)?; } add(bound(otherwise)?)?; }
        Expr::Case { branches,otherwise } => { for (condition,value) in branches { add(bound(condition)?)?;add(bound(value)?)?; } add(bound(otherwise)?)?; }
        Expr::In { expr,values,.. } => { add(bound(expr)?)?;for expr in values { add(bound(expr)?)?; } }
        Expr::Like { expr,pattern,.. } => { add(bound(expr)?)?;add(bound(pattern)?)?; }
    }
    Ok(bytes)
}

fn literal_payload_bound(expr: &Expr) -> Result<usize> {
    let bytes = match expr {
        Expr::Literal(value) => value.retained_bytes()?,
        Expr::RegexReplace { recipe, .. } => recipe.retained_bytes()?,
        Expr::Field { name, .. } | Expr::Call { name, .. } => name.len(),
        Expr::RowCopy(fields) => fields.iter().try_fold(0usize, |bytes, field| {
            bytes.checked_add(field.qualifier.as_ref().map_or(Ok(0), |qualifier| qualifier.retained_bytes())?).ok_or_else(overflow)
        })?,
        _ => 0,
    };
    expr.children().into_iter().try_fold(bytes, |bytes, child| bytes.checked_add(literal_payload_bound(child)?).ok_or_else(overflow))
}

fn value_type(value: &EvalValue<'_>,frame: &EvaluationFrame<'_>) -> Result<LogicalType> {
    frame.context.reserve(4096)?;
    Ok(match value {
        EvalValue::Scalar(value) => { frame.context.reserve(value.retained_bytes()?.checked_mul(8).ok_or_else(overflow)?)?;value.logical_type() }
        EvalValue::Struct(fields) => {
            let mut types = StructFields::new();
            for (name,value) in fields.iter() { frame.context.reserve(name.len().checked_add(4096).ok_or_else(overflow)?)?;types.insert(name.clone(),value_type(value,frame)?); }
            LogicalType::Struct(types)
        }
        EvalValue::List(kind,reader) => {
            let element = match reader.as_ref() {
                ListReader::Prepared(list) => {
                    frame.context.reserve(list.element.retained_bytes()?)?;
                    list.element.clone()
                }
                ListReader::Expressions(items) => {
                    frame.context.reserve(items.iter().try_fold(0usize,|bytes,expr|bytes.checked_add(expression_type_bound(expr, frame.replacements)?).ok_or_else(overflow))?.checked_mul(8).ok_or_else(overflow)?)?;
                    LogicalType::join_all(&items.iter().map(|expr|scalar::emitted_type(expr,frame)).collect::<Result<Vec<_>>>()?)
                }
                ListReader::Values(items) => {
                    frame.context.reserve(items.iter().try_fold(0usize,|bytes,item|bytes.checked_add(item.retained_bytes()?).and_then(|bytes|bytes.checked_add(4096)).ok_or_else(overflow))?.checked_mul(8).ok_or_else(overflow)?)?;
                    LogicalType::join_all(&items.iter().map(Value::logical_type).collect::<Vec<_>>())
                }
                ListReader::Stored(items) => {
                    if let rtdb_types::ListView::Values(values) = items.view() {
                        let bound = values.iter().try_fold(0usize, |bytes, item| {
                            bytes.checked_add(item.retained_bytes()?).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)
                        })?;
                        frame.context.reserve(bound.checked_mul(8).ok_or_else(overflow)?)?;
                    }
                    items.element_type()
                }
                ListReader::Arrow { cell, .. } => {
                    frame.context.reserve(cell.observed_type_bytes()?)?;
                    if let Some(element) = cell.sequence_range().and_then(|(field, array, range)| rtdb_types::ListValue::observed_numeric_type(field, array, range)) {
                        rtdb_columnar::execution_type(&element)
                    } else {
                        let LogicalType::List { element, .. } = cell.observed_type()? else { return Err(invalid("list type")); };
                        rtdb_columnar::execution_type(&element)
                    }
                }
                ListReader::Cast { input,element,.. } => {
                    if let Some(target) = union_target(*element) {
                        frame.context.reserve(target.retained_bytes()?.checked_mul(8).ok_or_else(overflow)?)?;
                        return Ok(LogicalType::list(target.unencoded()));
                    }
                    let source = value_type(input,frame)?;
                    let LogicalType::List { element: source,.. } = source else { return Err(invalid("list cast type")); };
                    match element {
                        Target::Cast(target) => { frame.context.reserve(target_bound(target)?.checked_add(source.retained_bytes()?).and_then(|n|n.checked_mul(8)).ok_or_else(overflow)?)?;target.result_type(&source) }
                        Target::Fixed(ty) => { frame.context.reserve(ty.retained_bytes()?)?;(*ty).clone() }
                    }
                }
                ListReader::Trim { input,.. } => {
                    let LogicalType::List { element,.. } = value_type(input,frame)? else { return Err(invalid("trim type")); };
                    *element
                }
                ListReader::IntegerSeries(_) => LogicalType::Scalar(rtdb_types::ScalarType::I64),
                ListReader::RangeBins(_) => rtdb_plan::range_bin_element_type(),
            };
            LogicalType::List { kind: match kind { Kind::List => ListKind::List,Kind::Set => ListKind::Set,Kind::Fixed => ListKind::FixedSizeList(i32::try_from(len(value)?).map_err(|_|overflow())?) },element: Box::new(element) }
        }
    })
}

fn boxed<'a,T>(future: impl Future<Output=Result<T>> + Send + 'a, frame: &EvaluationFrame<'_>) -> Result<Pending<'a,T>> {
    frame.context.reserve(std::mem::size_of_val(&future).checked_add(256).ok_or_else(overflow)?)?;
    Ok(Box::pin(GrowStack(future)))
}

/// Polls its future with stack to spare: each expression level nests one poll inside its parent's.
struct GrowStack<F>(F);
impl<F: Future> Future for GrowStack<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<F::Output> {
        // SAFETY: the inner future is structurally pinned; it is never moved out of or replaced.
        let inner = unsafe { self.map_unchecked_mut(|grow| &mut grow.0) };
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || inner.poll(cx))
    }
}

fn invalid(name: &str) -> Anomaly { Anomaly::incorrect("xtdb/invalid-argument-types",format!("{name} is not applicable to these collection values")) }

fn plain_type(ty: &LogicalType) -> &LogicalType {
    match ty { LogicalType::Maybe(value) | LogicalType::RunEndEncoded { value, .. } => plain_type(value), _ => ty }
}

pub(super) fn expression_type(expr: &Expr,schema: &Schema,context: &EvalContext<'_>) -> Result<LogicalType> {
    if context.parameter_types.is_some_and(|types| types.len() != context.parameters.width()) {
        return Err(Anomaly::fault("rtdb.expr/parameter-schema", "Parameter values and declared types have different widths"));
    }
    context.reserve(expression_type_bound(expr, None)?.checked_mul(8).ok_or_else(overflow)?)?;
    scalar::emitted_type(expr,&EvaluationFrame { schema,row: ExpressionRow::Values(&[]),context,asynchronous: false,replacements: None })
}

pub(super) fn required(expr: &Expr) -> bool {
    match expr {
        Expr::List(_) | Expr::Struct(_) | Expr::RegexReplace {..} => true,
        Expr::Literal(Value::List(_) | Value::Set(_) | Value::FixedSizeList(_) | Value::Struct(_)) => true,
        Expr::Unary { .. } | Expr::Binary { .. } | Expr::Cast { .. } | Expr::Field { .. } => true,
        Expr::Call { name, return_type: LogicalType::Null | LogicalType::Nothing, .. } if !["set","trim_array","trim-array","regexp_replace","regexp-replace"].iter().any(|special| name.eq_ignore_ascii_case(special)) => true,
        Expr::Call { name, .. } if calls::is_cosine(name) || scalar::ordinary_call(name) => true,
        Expr::Call { name, args, .. } if !args.is_empty() && crate::temporal_functions::strict_call(name) || crate::interval_constructor::is_name(name) || crate::bitwise::is_name(name) || ["is_true","is-true","is_false","is-false","distinct_from","distinct-from","_meta_double","_meta-double","get_field","get-field","compare","compare_nulls_first","compare-nulls-first","compare_nulls_last","compare-nulls-last","like","ilike"].iter().any(|function| name.eq_ignore_ascii_case(function)) => true,
        Expr::Call { name,args, .. } => ["_id_eq_any","nth","set","trim_array","trim-array","array_upper","array-upper","array_lower","array-lower","array_length","array-length","col_description","col-description","pg_indexam_has_property","pg-indexam-has-property","mod","generate_series","generate-series","range_bins","range-bins","date_bin","date-bin"].iter().any(|function| name.eq_ignore_ascii_case(function)) || args.iter().any(required),
        Expr::Subscript { .. } => true,
        Expr::Case { .. } | Expr::SimpleCase { .. } | Expr::Between { .. } | Expr::Coalesce(_) => true,
        Expr::In { expr,values, .. } => required(expr) || values.iter().any(required),
        Expr::Like { .. } => true,
        _ => false,
    }
}

pub(super) fn execute_sync(expr: &Expr,schema: &Schema,row: ExpressionRow<'_>,context: &EvalContext<'_>) -> Result<Value> {
    poll_sync(execute(expr,schema,row,context,false,None))
}
pub(super) fn execute_sync_prepared(expr: &Expr,schema: &Schema,row: ExpressionRow<'_>,context: &EvalContext<'_>,replacements: &super::invocation::Replacements<'_>) -> Result<Value> {
    poll_sync(execute(expr,schema,row,context,false,Some(replacements)))
}
fn poll_sync(future: impl Future<Output=Result<Value>>) -> Result<Value> {
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(result) => result,
        Poll::Pending => Err(Anomaly::fault("rtdb.expr/unexpected-suspension","Synchronous collection evaluation suspended")),
    }
}
pub(super) async fn execute_async(expr: &Expr,schema: &Schema,row: ExpressionRow<'_>,context: &EvalContext<'_>) -> Result<Value> { execute(expr,schema,row,context,true,None).await }
pub(super) async fn execute_async_prepared(expr: &Expr,schema: &Schema,row: ExpressionRow<'_>,context: &EvalContext<'_>,replacements: &super::invocation::Replacements<'_>) -> Result<Value> { execute(expr,schema,row,context,true,Some(replacements)).await }
async fn execute(expr: &Expr,schema: &Schema,row: ExpressionRow<'_>,context: &EvalContext<'_>,asynchronous: bool,replacements: Option<&super::invocation::Replacements<'_>>) -> Result<Value> {
    let frame = EvaluationFrame { schema,row,context,asynchronous,replacements };
    let value = evaluate(expr,&frame)?.await?;
    materialize(value,&frame)?.await
}

pub(super) async fn execute_with_arrow_list<T>(
    expr: &Expr, schema: &Schema, row: ExpressionRow<'_>, context: &EvalContext<'_>, replacements: &super::invocation::Replacements<'_>,
    retain: impl FnOnce(ArrowCell<'_>) -> Result<Option<T>>,
) -> Result<std::ops::ControlFlow<T, Value>> {
    use std::ops::ControlFlow;

    let frame = EvaluationFrame { schema, row, context, asynchronous: true, replacements: Some(replacements) };
    let value = evaluate(expr, &frame)?.await?;
    if let EvalValue::List(Kind::List | Kind::Fixed, reader) = &value
        && let ListReader::Arrow { cell, .. } = reader.as_ref()
        && let Some(output) = retain(*cell)?
    {
        return Ok(ControlFlow::Break(output));
    }
    materialize(value, &frame)?.await.map(ControlFlow::Continue)
}

pub(super) fn list_events<'a>(expr: &'a Expr, schema: &'a Schema, row: &'a [Value], context: &'a EvalContext<'a>, replacements: &'a super::invocation::Replacements<'a>, batch_rows: usize)
    -> futures::stream::BoxStream<'a, Result<super::ListEvent>> {
    Box::pin(async_stream::try_stream! {
        if batch_rows == 0 { Err(Anomaly::incorrect("xtdb/invalid-batch-size", "List page size must be positive"))?; }
        let allocation = context.allocation.ok_or_else(|| Anomaly::fault("rtdb.expr/allocation-scope", "List cursor requires an allocation scope"))?;
        let frame = EvaluationFrame { schema, row: ExpressionRow::Values(row), context, asynchronous: true, replacements: Some(replacements) };
        let continuation = scalar::prepare(expr, &frame)?.await?;
        let value = if continuation.is_list() { Some(continuation.demand().await?) } else { None };
        if let Some(EvalValue::List(_, reader)) = &value
            && let ListReader::IntegerSeries(series) = reader.as_ref()
        {
            yield super::ListEvent::Opened(Some(super::IndexedListBatches {
                series: *series, position: 0, rows_per_batch: batch_rows, memory: allocation.memory_budget().clone(),
            }));
            return;
        }
        yield super::ListEvent::Opened(None);
        if let Some(value) = value {
            let size = len(&value)?;
            let mut position = 0usize;
            while position < size {
                let count = (size - position).min(batch_rows);
                let scope = crate::EvaluationScope::new(allocation.memory_budget())?;
                scope.reserve(count.checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?)?;
                let page_context = context.with_allocation(&scope);
                if let Some(PackedList { values, backing }) = packed_slice(&value, position, count, true, &page_context)? {
                    if let Some((field, array)) = values.packed_column() {
                        rtdb_columnar::validate_projected_decimals(&field, array.as_ref())?;
                    }
                    position += count;
                    yield super::ListEvent::Batch(super::ListBatch { values, scope, backing });
                    continue;
                }
                let mut values = rtdb_types::ListBuilder::with_capacity(count);
                {
                    let page_context = context.with_allocation(&scope);
                    let page_frame = EvaluationFrame { schema, row: ExpressionRow::Values(row), context: &page_context, asynchronous: true, replacements: Some(replacements) };
                    for index in position..position + count {
                        append_list_element(&value, index, &page_frame, &mut values, true).await?;
                    }
                }
                position += count;
                yield super::ListEvent::Batch(super::ListBatch { values: values.finish(), scope, backing: None });
            }
        }
    })
}

pub(super) async fn execute_list_async(expr: &Expr, schema: &Schema, row: ExpressionRow<'_>, context: &EvalContext<'_>,replacements: Option<&super::invocation::Replacements<'_>>) -> Result<super::EvaluatedList> {
    let frame = EvaluationFrame { schema, row, context, asynchronous: true,replacements };
    let continuation = scalar::prepare(expr, &frame)?.await?;
    if !continuation.is_list() { return Ok(super::EvaluatedList { storage: super::ListStorage::Values(Vec::new()) }); }
    let value = continuation.demand().await?;
    if let EvalValue::List(_, reader) = &value {
        if let ListReader::IntegerSeries(series) = reader.as_ref() {
            return Ok(super::EvaluatedList { storage: super::ListStorage::Integer(*series) });
        }
        if let ListReader::RangeBins(bins) = reader.as_ref() {
            return Ok(super::EvaluatedList { storage: super::ListStorage::Bins { reader: *bins,length: bins.len()? } });
        }
    }
    let mut value = materialize(value, &frame)?.await?;
    value.admit_expression_dates()?;
    if !matches!(expr, Expr::Column { .. } | Expr::RowCopy(_)) { super::validate_projection_value(&value)?; }
    let storage = match value {
        Value::Set(values) => super::ListStorage::Values(values),
        Value::List(values) | Value::FixedSizeList(values) => super::ListStorage::List(values),
        _ => super::ListStorage::Values(Vec::new()),
    };
    Ok(super::EvaluatedList { storage })
}

fn clone_value<'a>(value: &EvalValue<'a>,frame: &EvaluationFrame<'_>) -> Result<EvalValue<'a>> {
    if let EvalValue::Scalar(value) = value { frame.context.reserve(value.retained_bytes()?)?; }
    Ok(value.clone())
}
fn scalar(value: Value) -> EvalValue<'static> { EvalValue::Scalar(value) }
fn is_null(value: &EvalValue<'_>) -> bool { matches!(value,EvalValue::Scalar(value) if nullish(value)) }
fn scalar_ref<'a>(value: &'a EvalValue<'_>) -> Result<&'a Value> { match value { EvalValue::Scalar(value) => Ok(value), _ => Err(invalid("scalar operation")) } }
fn box_cell<'a>(mut value: EvalValue<'a>) -> Result<EvalValue<'a>> {
    if let EvalValue::Scalar(value) = &mut value { value.admit_box_date()?; }
    Ok(value)
}
fn make_list<'a>(kind: Kind,reader: ListReader<'a>,frame: &EvaluationFrame<'_>) -> Result<EvalValue<'a>> {
    frame.context.reserve(std::mem::size_of::<ListReader<'a>>() + 256)?;
    Ok(EvalValue::List(kind,Arc::new(reader)))
}
fn from_value<'a>(value: Value,frame: &EvaluationFrame<'_>) -> Result<EvalValue<'a>> {
    match value {
        Value::List(values) => make_list(Kind::List,ListReader::Stored(values),frame),
        Value::Set(values) => make_list(Kind::Set,ListReader::Values(values),frame),
        Value::FixedSizeList(values) => make_list(Kind::Fixed,ListReader::Stored(values),frame),
        Value::Struct(fields) => {
            let mut cells = StructFields::new();
            for (name,value) in fields { frame.context.reserve(4096)?;cells.insert(name,box_cell(from_value(value,frame)?)?); }
            frame.context.reserve(256)?;
            Ok(EvalValue::Struct(Arc::new(cells)))
        }
        value => Ok(EvalValue::Scalar(value)),
    }
}
fn from_arrow<'a>(
    cell: ArrowCell<'a>, backing: Option<&'a Arc<MemoryReservation>>, frame: &EvaluationFrame<'_>,
) -> Result<EvalValue<'a>> {
    let cell = cell.physical()?;
    if cell.is_undefined()? { return Ok(scalar(Value::Undefined)); }
    if cell.is_null()? { return Ok(scalar(Value::Null)); }
    if let LogicalType::List { kind, .. } = cell.logical_type()? {
        let kind = match kind {
            ListKind::List => Some(Kind::List),
            ListKind::Set => Some(Kind::Set),
            ListKind::FixedSizeList(_) => Some(Kind::Fixed),
            _ => None,
        };
        if let Some(kind) = kind {
            return make_list(kind, ListReader::Arrow { cell, backing }, frame);
        }
    }
    frame.context.reserve(rtdb_columnar::estimate_read_value_bytes(cell.field, cell.array, cell.row)?)?;
    from_value(rtdb_columnar::read_execution_value(cell.field, cell.array, cell.row)?, frame)
}

fn size(value: &EvalValue<'_>) -> Result<i32> {
    if let EvalValue::List(_,reader) = value {
        match reader.as_ref() {
            ListReader::RangeBins(bins) => return bins.size(),
            ListReader::Cast { input,.. } => return size(input),
            _ => (),
        }
    }
    i32::try_from(len(value)?).map_err(|_|overflow())
}
fn len(value: &EvalValue<'_>) -> Result<usize> {
    let EvalValue::List(_,reader) = value else { return Err(invalid("list length")); };
    match reader.as_ref() {
        ListReader::Expressions(items) => Ok(items.len()), ListReader::Values(items) => Ok(items.len()),
        ListReader::Stored(items) => Ok(items.len()),
        ListReader::Prepared(list) => Ok(list.values.len()),
        ListReader::Arrow { cell, .. } => Ok(cell.sequence().ok_or_else(|| invalid("list length"))?.len()),
        ListReader::Cast { input, .. } => len(input),
        ListReader::Trim { size,.. } => crate::collection::view_size(*size),
        ListReader::IntegerSeries(series) => Ok(series.len()),
        ListReader::RangeBins(bins) => bins.len(),
    }
}
fn nth<'a>(value: EvalValue<'a>,index: usize,frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,EvalValue<'a>>> {
    boxed(async move {
        let EvalValue::List(_,reader) = value else { return Err(invalid("nth")); };
        let value = match reader.as_ref() {
            ListReader::Expressions(items) => evaluate((*items).get(index).ok_or_else(|| invalid("nth"))?,frame)?.await?,
            ListReader::Values(items) => from_value(super::retained_clone(items.get(index).ok_or_else(|| invalid("nth"))?,frame.context)?,frame)?,
            list @ (ListReader::Stored(_) | ListReader::Prepared(_)) => {
                let value = list.stored().expect("stored list").get(index).ok_or_else(|| invalid("nth"))?;
                from_value(super::retained_clone(value.as_ref(), frame.context)?, frame)?
            },
            ListReader::Arrow { cell, backing } => {
                let (field, array, range) = cell.sequence_range().ok_or_else(|| invalid("nth"))?;
                if index >= range.len() { return Err(invalid("nth")); }
                let item = ArrowCell { field, array, row: range.start + index };
                from_arrow(item, *backing, frame)?
            }
            ListReader::Cast { input,element,union_admitted } => {
                let value = nth(clone_value(input,frame)?,index,frame)?.await?;
                if let Some(target) = union_target(*element) {
                    frame.context.reserve(target.retained_bytes()?.checked_mul(8).ok_or_else(overflow)?)?;
                    if !union_admitted && !target.legs().contains(&value_type(&value,frame)?) { return Err(invalid("list union")); }
                    value
                } else { cast_value(value,*element,frame)?.await? }
            }
            ListReader::Trim { input,.. } => nth(clone_value(input,frame)?,index,frame)?.await?,
            ListReader::IntegerSeries(series) => scalar(series.nth(index)),
            ListReader::RangeBins(bins) => { frame.context.reserve(4096)?;from_value(bins.nth(index),frame)? }
        };
        box_cell(value)
    },frame)
}

fn borrowed_vector<'a>(value: &'a EvalValue<'a>) -> Option<crate::vector::VectorInput<'a>> {
    use crate::vector::VectorInput;

    let EvalValue::List(Kind::List | Kind::Fixed, reader) = value else { return None; };
    match reader.as_ref() {
        ListReader::Stored(_) | ListReader::Prepared(_) => Some(VectorInput::Stored(reader.stored()?.view())),
        ListReader::Arrow { cell, .. } => VectorInput::from_arrow(*cell),
        ListReader::Trim { input, size } => {
            let size = crate::collection::view_size(*size).ok()?;
            let vector = borrowed_vector(input)?;
            if matches!(vector, VectorInput::Stored(rtdb_types::ListView::Numeric(_))) { return None; }
            vector.prefix(size)
        }
        _ => None,
    }
}

fn packed_slice(value: &EvalValue<'_>, offset: usize, count: usize, projection: bool, context: &EvalContext<'_>) -> Result<Option<PackedList>> {
    let EvalValue::List(Kind::List | Kind::Fixed, reader) = value else { return Ok(None); };
    let end = offset.checked_add(count).ok_or_else(overflow)?;
    match reader.as_ref() {
        ListReader::Stored(items) if end <= items.len() => {
            if let rtdb_types::ListView::Scalars(values) = items.view()
                && matches!(values.array().data_type(), arrow_schema::DataType::Union(..))
            {
                let field = values.field();
                let array = values.array().as_ref();
                let mut rows = offset..end;
                return rtdb_columnar::repack_scalar_list(field, array, rows.clone(), false, || {
                    let bound = rows.try_fold(4096usize, |bytes, row| {
                        rtdb_columnar::estimate_copy_cell_bytes(field, array, row)?.checked_mul(4)
                            .and_then(|size| bytes.checked_add(size)).ok_or_else(overflow)
                    })?;
                    context.reserve(bound)
                }).map(|list| list.map(PackedList::from));
            }
            let Some(slice) = items.slice_packed(offset, count) else { return Ok(None); };
            // A yielded page can outlive the source reader, even when its Arrow slice retains the whole parent buffer.
            context.reserve(slice.retained_payload_bytes()?)?;
            Ok(Some(slice.into()))
        }
        ListReader::Arrow { cell, .. } => {
            let Some((field, array, range)) = cell.sequence_range() else { return Ok(None); };
            if end > range.len() { return Ok(None); }
            let rows = (range.start + offset)..(range.start + end);
            let reserve = || {
                let bound = rows.clone().try_fold(4096usize, |bytes, row| {
                    rtdb_columnar::estimate_copy_cell_bytes(field, array, row)?.checked_mul(4)
                        .and_then(|size| bytes.checked_add(size))
                        .and_then(|size| size.checked_add(std::mem::size_of::<(usize, usize)>()))
                        .ok_or_else(overflow)
                })?;
                context.reserve(bound)
            };
            if !rtdb_types::ListValue::supports_numeric_arrow(field) {
                if let Some(span) = cell.numeric_list_span_range(offset..end) {
                    reserve()?;
                    return Ok(rtdb_columnar::repack_numeric_list(&span, true).map(PackedList::from));
                }
                return rtdb_columnar::repack_scalar_list(field, array, rows.clone(), true, reserve).map(|list| list.map(PackedList::from));
            }
            reserve()?;
            let selection = rows.map(|row| (0, row)).collect::<Vec<_>>();
            let array = rtdb_columnar::copy_selected_cells(&[array], &selection)?;
            let values = rtdb_types::ListValue::from_numeric_arrow(Arc::new(field.clone()), array);
            Ok(values.map(PackedList::from))
        }
        ListReader::Cast { input, element, .. } => packed::cast(input, *element, offset..end, projection, context),
        ListReader::Trim { input, size } if end <= crate::collection::view_size(*size)? => {
            packed_slice(input, offset, count, projection, context)
        }
        _ => Ok(None),
    }
}

async fn append_list_element<'a>(
    value: &EvalValue<'a>, index: usize, frame: &'a EvaluationFrame<'a>,
    builder: &mut rtdb_types::ListBuilder, projection: bool,
) -> Result<()> {
    let computed = if let EvalValue::List(_, reader) = value
        && let ListReader::Expressions(items) = reader.as_ref()
    {
        computed::element(&items[index], frame).await?
    } else { None };
    let mut cell = match computed {
        Some(computed::Output::Number(number)) => {
            if projection { crate::list_cast::validate_output(number)?; }
            builder.push_numeric(number);
            return Ok(());
        }
        Some(computed::Output::Value(cell)) => materialize(box_cell(cell)?, frame)?.await?,
        None => materialize(nth(clone_value(value, frame)?, index, frame)?.await?, frame)?.await?,
    };
    if projection {
        cell.admit_expression_dates()?;
        super::validate_projection_value(&cell)?;
    }
    builder.push(cell);
    Ok(())
}

fn materialize<'a>(value: EvalValue<'a>,frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,Value>> {
    boxed(async move {
        match value {
            EvalValue::Scalar(value) => Ok(value),
            EvalValue::Struct(fields) => {
                let mut result = StructFields::new();
                for (name,value) in fields.iter() {
                    frame.context.reserve(name.len().checked_add(4096).ok_or_else(overflow)?)?;
                    result.insert(name.clone(),materialize(clone_value(value,frame)?,frame)?.await?);
                }
                Ok(Value::Struct(result))
            }
            value @ EvalValue::List(kind,_) => {
                if let EvalValue::List(_, reader) = &value
                    && let Some(items) = reader.stored()
                    && (items.array().is_some() || matches!(reader.as_ref(), ListReader::Prepared(_)))
                {
                    if matches!(reader.as_ref(), ListReader::Prepared(_)) {
                        frame.context.reserve(items.retained_payload_bytes()?)?;
                    }
                    return Ok(match kind {
                        Kind::List => Value::List(items.clone()),
                        Kind::Fixed => Value::FixedSizeList(items.clone()),
                        Kind::Set => unreachable!("set storage is not a list"),
                    });
                }
                if let EvalValue::List(_, reader) = &value
                    && let ListReader::Arrow { cell, .. } = reader.as_ref()
                    && !matches!(kind, Kind::Set)
                {
                    if cell.sequence_range().is_some_and(|(field, _, _)| rtdb_types::ListValue::supports_numeric_arrow(field)) {
                        frame.context.reserve(rtdb_columnar::estimate_read_value_bytes(cell.field, cell.array, cell.row)?)?;
                        return rtdb_columnar::read_execution_value(cell.field, cell.array, cell.row);
                    }
                    if let Some(span) = cell.numeric_list_span() {
                        frame.context.reserve(rtdb_columnar::estimate_read_value_bytes(cell.field, cell.array, cell.row)?)?;
                        if let Some(list) = rtdb_columnar::repack_numeric_list(&span, true) {
                            return Ok(if kind == Kind::Fixed { Value::FixedSizeList(list) } else { Value::List(list) });
                        }
                    }
                    if let Some((field, array, range)) = cell.sequence_range()
                        && let Some(list) = rtdb_columnar::repack_scalar_list(field, array, range, true, || {
                            frame.context.reserve(rtdb_columnar::estimate_read_value_bytes(cell.field, cell.array, cell.row)?)
                        })?
                    {
                        return Ok(if kind == Kind::Fixed { Value::FixedSizeList(list) } else { Value::List(list) });
                    }
                }
                if let EvalValue::List(Kind::List, reader) = &value
                    && let ListReader::Cast { input, element, .. } = reader.as_ref()
                    && let Some(PackedList { values, .. }) = packed::cast(input, *element, 0..len(input)?, false, frame.context)?
                {
                    return Ok(Value::List(values));
                }
                let size = len(&value)?;
                if let EvalValue::List(Kind::List, reader) = &value
                    && matches!(reader.as_ref(), ListReader::Trim { .. })
                    && let Some(PackedList { values, .. }) = packed_slice(&value, 0, size, false, frame.context)?
                {
                    return Ok(Value::List(values));
                }
                frame.context.reserve(size.checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?)?;
                if matches!(kind, Kind::Set) {
                    let mut values = Vec::with_capacity(size);
                    for index in 0..size {
                        values.push(materialize(nth(clone_value(&value,frame)?,index,frame)?.await?,frame)?.await?);
                        if frame.asynchronous && index % 1024 == 1023 { tokio::task::yield_now().await; }
                    }
                    return Ok(Value::Set(values));
                }
                let mut values = rtdb_types::ListBuilder::with_capacity(size);
                for index in 0..size {
                    append_list_element(&value, index, frame, &mut values, false).await?;
                    if frame.asynchronous && index % 1024 == 1023 { tokio::task::yield_now().await; }
                }
                let values = values.finish();
                Ok(if matches!(kind, Kind::Fixed) { Value::FixedSizeList(values) } else { Value::List(values) })
            }
        }
    },frame)
}

fn element_target<'a>(target: Target<'a>) -> Option<Target<'a>> {
    match target {
        Target::Cast(CastTarget::Checked { target, .. }) => element_target(Target::Cast(target)),
        Target::Cast(CastTarget::List(element)) => Some(Target::Cast(element)),
        Target::Cast(CastTarget::Fixed(ty)) | Target::Fixed(ty) => match plain_type(ty) {
            LogicalType::List { kind: ListKind::List,element } => Some(Target::Fixed(element)),
            _ => None,
        }
        _ => None,
    }
}
fn union_target<'a>(target: Target<'a>) -> Option<&'a LogicalType> {
    match target {
        Target::Cast(target) => target.union_target(),
        Target::Fixed(ty) => match ty {
            LogicalType::Maybe(_) | LogicalType::Union(_) => Some(ty),
            LogicalType::RunEndEncoded { value,.. } => union_target(Target::Fixed(value)),
            _ => None,
        },
    }
}
fn cast_value<'a>(value: EvalValue<'a>,target: Target<'a>,frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,EvalValue<'a>>> {
    boxed(async move {
        if is_null(&value) { return Ok(value); }
        let source = value_type(&value,frame)?;
        let identity = match target {
            Target::Cast(CastTarget::Checked { requested,.. }) => *requested == source,
            Target::Cast(CastTarget::Fixed(ty)) | Target::Fixed(ty) => *ty == source,
            _ => false,
        };
        if identity && matches!(value,EvalValue::Struct(_) | EvalValue::List(Kind::Set | Kind::Fixed,_)) { return Ok(value); }
        if let Target::Cast(target) = target { target.validate_source(&source)?; }
        if let Some(element) = element_target(target) {
            let value = match value {
                value @ EvalValue::List(Kind::List | Kind::Fixed,_) => value,
                EvalValue::Scalar(Value::Utf8(text)) => {
                    frame.context.reserve(text.len().checked_mul(128).and_then(|n|n.checked_add(4096)).ok_or_else(overflow)?)?;
                    from_value(Value::list(crate::cast::pg_array_text(&text)?),frame)?
                }
                _ => return Err(invalid("list cast")),
            };
            let packed = match &value {
                EvalValue::List(_, reader) => match reader.as_ref() {
                    ListReader::Stored(list) => list.array().is_some(),
                    ListReader::Arrow { cell, .. } => cell.sequence_range().is_some_and(|(field, _, _)| rtdb_types::ListValue::supports_numeric_arrow(field)),
                    _ => false,
                },
                _ => false,
            };
            let union_admitted = packed && if let (Some(target), LogicalType::List { element, .. }) = (union_target(element), &source) {
                frame.context.reserve(target.retained_bytes()?.checked_mul(8).ok_or_else(overflow)?)?;
                let accepted = target.legs();
                element.legs().iter().all(|ty| accepted.contains(ty))
            } else { false };
            return make_list(Kind::List,ListReader::Cast { input: value,element,union_admitted },frame);
        }
        let value = materialize(value,frame)?.await?;
        frame.context.reserve(crate::string::render::allocation_bound(&value)?)?;
        let output = match target {
            Target::Cast(target) => crate::pg_catalog::cast(&value,target,frame.context)?,
            Target::Fixed(ty) => {
                frame.context.reserve(ty.retained_bytes()?.checked_mul(4).ok_or_else(overflow)?)?;
                crate::pg_catalog::cast(&value,&CastTarget::fixed(ty.clone()),frame.context)?
            }
        };
        from_value(output,frame)
    },frame)
}

fn evaluate<'a>(expr: &'a Expr,frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,EvalValue<'a>>> {
    boxed(async move {
        if let Some(cell) = frame.arrow_input(expr) {
            let value = from_arrow(cell, frame.arrow_backing(expr), frame)?;
            return if matches!(expr, Expr::Field { .. }) { box_cell(value) } else { Ok(value) };
        }
        if !required(expr) { return evaluate_scalar(expr, frame).await; }
        match expr {
            Expr::List(items) => {
                let list = frame.prepared_list(expr).map_or(ListReader::Expressions(items), ListReader::Prepared);
                make_list(Kind::List, list, frame)
            }
            Expr::Struct(fields) => {
                let mut values = StructFields::new();
                for (name,expr) in fields {
                    frame.context.reserve(name.len().checked_add(4096).ok_or_else(overflow)?)?;
                    values.insert(name.clone(),box_cell(evaluate(expr,frame)?.await?)?);
                }
                frame.context.reserve(256)?;
                Ok(EvalValue::Struct(Arc::new(values)))
            }
            Expr::Literal(value) => from_value(super::retained_clone(value,frame.context)?,frame),
            Expr::Field { .. } => scalar::prepare(expr,frame)?.await?.demand().await,
            Expr::Subscript { .. } | Expr::RegexReplace {..} => scalar::prepare(expr,frame)?.await?.demand().await,
            Expr::Coalesce(_) | Expr::Case { .. } | Expr::SimpleCase { .. } | Expr::Between { .. } | Expr::Cast { .. } => scalar::prepare(expr,frame)?.await?.demand().await,
            Expr::Call { name, return_type: LogicalType::Null | LogicalType::Nothing, .. } if !["set","trim_array","trim-array","regexp_replace","regexp-replace"].iter().any(|special| name.eq_ignore_ascii_case(special)) => scalar::prepare(expr,frame)?.await?.demand().await,
            Expr::Call { name, .. } if scalar::ordinary_call(name) => scalar::prepare(expr,frame)?.await?.demand().await,
            Expr::Call { name,.. } if crate::temporal_functions::strict_call(name) || crate::interval_constructor::is_name(name) || ["nth","col_description","col-description","pg_indexam_has_property","pg-indexam-has-property","mod","array_upper","array-upper","array_lower","array-lower","array_length","array-length","generate_series","generate-series","range_bins","range-bins","date_bin","date-bin"].iter().any(|function| name.eq_ignore_ascii_case(function)) => scalar::prepare(expr,frame)?.await?.demand().await,
            Expr::Call { name, .. } if crate::bitwise::is_name(name) || ["is_true","is-true","is_false","is-false","distinct_from","distinct-from","_meta_double","_meta-double","get_field","get-field","compare","compare_nulls_first","compare-nulls-first","compare_nulls_last","compare-nulls-last","like","ilike"].iter().any(|function| name.eq_ignore_ascii_case(function)) => scalar::prepare(expr,frame)?.await?.demand().await,
            Expr::Call { name,args, .. } => calls::call(expr,name,args,frame)?.await,
            Expr::Unary { .. } | Expr::Binary { .. } => scalar::prepare(expr,frame)?.await?.demand().await,
            Expr::In { expr,values,negated } => {
                let value = evaluate(expr,frame)?.await?;
                let mut unknown = false;
                for candidate in values {
                    match compare::equal(clone_value(&value,frame)?,evaluate(candidate,frame)?.await?,false,frame)?.await? {
                        Some(true) => return Ok(scalar(Value::Bool(!*negated))),None => unknown = true,_ => (),
                    }
                }
                Ok(scalar(if unknown { Value::Null } else { Value::Bool(*negated) }))
            }
            Expr::Like { .. } => scalar::prepare(expr,frame)?.await?.demand().await,
            _ => Err(Anomaly::fault("rtdb.expr/collection-dispatch","Expression has no collection dispatch")),
        }
    },frame)
}

async fn evaluate_scalar<'a>(expr: &Expr, frame: &'a EvaluationFrame<'a>) -> Result<EvalValue<'a>> {
    from_value(scalar_value(expr, frame).await?, frame)
}

async fn scalar_value(expr: &Expr, frame: &EvaluationFrame<'_>) -> Result<Value> {
    if frame.asynchronous { super::evaluate_async_inner(expr,frame.schema,frame.row,frame.context).await }
    else { super::evaluate_inner(expr,frame.schema,frame.row,frame.context) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn immediate<T>(mut future: Pending<'_,T>) -> T {
        match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(result) => result.unwrap(),Poll::Pending => panic!("Unexpected suspension"),
        }
    }

    #[test]
    fn repeated_reads_of_one_list_reader_rerun_random() {
        let expression = Expr::List(vec![Expr::Call { name: "random".into(),args: vec![],return_type: LogicalType::Scalar(rtdb_types::ScalarType::F64) }]);
        let schema = Schema::default();let context = EvalContext::default();
        let frame = EvaluationFrame { schema: &schema,row: ExpressionRow::Values(&[]),context: &context,asynchronous: false,replacements: None };
        let reader = immediate(evaluate(&expression,&frame).unwrap());
        let first = immediate(materialize(immediate(nth(clone_value(&reader,&frame).unwrap(),0,&frame).unwrap()),&frame).unwrap());
        let mut changed = false;
        for _ in 0..32 {
            let value = immediate(materialize(immediate(nth(clone_value(&reader,&frame).unwrap(),0,&frame).unwrap()),&frame).unwrap());
            let Value::F64(number) = value else { panic!() };
            assert!((0.0..1.0).contains(&number));
            changed |= value != first;
        }
        assert!(changed);
    }
}
