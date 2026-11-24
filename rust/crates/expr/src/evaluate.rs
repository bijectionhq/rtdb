use rtdb_types::StructFields;
use crate::{arithmetic_in_timezone, compare::predicate_order, functions::scalar_call};
use crate::compare::{nullish,sql_eq_scoped,sql_not_eq_scoped};
use crate::numeric::{inapplicable, integer, integer_value, number, overflow};
use arrow_array::RecordBatch;
use rtdb_columnar::{MemoryBudget, OwnedBatch, copy_array, estimate_builder_bytes, to_field};
use rtdb_plan::{BinaryOp, Expr, Schema, UnaryOp};
use rtdb_types::{Anomaly, ListValue, LogicalType, Result, Instant, Value};
use std::cmp::Ordering;
use std::sync::Arc;

mod reader;
mod invocation;
pub use invocation::ExpressionInvocation;

/// Derives all emitted type alternatives from the complete input and outer schemas and bound parameters.
/// Performs expression admission without reading row payloads; temporary metadata uses a call-local scope and the returned type is charged to the caller's scope.
pub fn expression_type(expr: &Expr,schema: &Schema,context: &EvalContext<'_>) -> Result<LogicalType> {
    let Some(allocation) = context.allocation else { return reader::expression_type(expr,schema,context); };
    let scratch = crate::EvaluationScope::new(allocation.memory_budget())?;
    let result = reader::expression_type(expr,schema,&context.with_allocation(&scratch))?;
    context.reserve(result.retained_bytes()?)?;
    Ok(result)
}

pub struct OuterRow<'a> { pub schema: &'a Schema, pub values: &'a [Value] }

pub trait ParameterLookup: Sync {
    fn width(&self) -> usize;
    fn value(&self, index: usize) -> Result<Option<&Value>>;
    fn known_value(&self, index: usize) -> Option<&Value>;
    fn arrow_cell(&self, index: usize) -> Option<ArrowParameterCell<'_>>;
    fn correlated_value(&self, _depth: usize, _position: usize, _scope: Option<&crate::EvaluationScope>) -> Result<Option<Value>> {
        Ok(None)
    }
}

pub type ArrowParameterCell<'a> = rtdb_columnar::ArrowCell<'a>;

#[derive(Clone, Copy)]
pub enum Parameters<'a> {
    Values(&'a [Value]),
    Lookup(&'a dyn ParameterLookup),
}

impl<'a> Parameters<'a> {
    pub fn width(self) -> usize {
        match self { Self::Values(values) => values.len(), Self::Lookup(lookup) => lookup.width() }
    }
    pub fn get(self, index: usize) -> Result<Option<&'a Value>> {
        match self { Self::Values(values) => Ok(values.get(index)), Self::Lookup(lookup) => lookup.value(index) }
    }
    pub fn known(self, index: usize) -> Option<&'a Value> {
        match self { Self::Values(values) => values.get(index), Self::Lookup(lookup) => lookup.known_value(index) }
    }
    pub fn arrow_cell(self, index: usize) -> Option<ArrowParameterCell<'a>> {
        match self { Self::Values(_) => None, Self::Lookup(lookup) => lookup.arrow_cell(index) }
    }
}

/// Borrowed scalar inputs and allocation scope.
/// With no allocation scope, the caller must reserve primitive expression intermediates and returned values independently.
/// Reached stored Transit object decoding requires an allocation scope, including equality and field access.
/// Catalog-dependent evaluation borrows the captured catalog and requires an allocation scope for enumeration.
pub struct EvalContext<'a> {
    pub parameters: Parameters<'a>,
    /// Complete declared vector types for the bound parameters, including unselected union legs.
    /// When supplied, the slice must have the same width as `parameters` and overrides expression annotations.
    /// Each type must describe its parameter cell using execution vector types.
    pub parameter_types: Option<&'a [LogicalType]>,
    pub current_time: Instant,
    pub timezone: &'a str,
    pub outer_rows: &'a [OuterRow<'a>],
    pub snapshot_token: Option<&'a str>,
    pub allocation: Option<&'a crate::EvaluationScope>,
    pub catalog: Option<&'a dyn crate::PgCatalogLookup>,
}

impl Default for EvalContext<'_> {
    fn default() -> Self { Self { parameters: Parameters::Values(&[]), parameter_types: None, current_time: Instant::default(), timezone: "UTC", outer_rows: &[], snapshot_token: None, allocation: None, catalog: None } }
}

impl EvalContext<'_> {
    fn with_allocation<'a>(&'a self, allocation: &'a crate::EvaluationScope) -> EvalContext<'a> {
        EvalContext { parameters: self.parameters, parameter_types: self.parameter_types, current_time: self.current_time, timezone: self.timezone,
            outer_rows: self.outer_rows, snapshot_token: self.snapshot_token, allocation: Some(allocation), catalog: self.catalog }
    }
    pub(crate) fn reserve(&self, bytes: usize) -> Result<()> { crate::admission::Admission::for_context(self.allocation).reserve(bytes) }
}

#[derive(Clone)]
pub struct CompiledExpr { expr: Expr, schema: Schema }

impl CompiledExpr {
    pub fn compile(expr: &Expr, schema: &Schema) -> Result<Self> {
        expr.validate_columns(&schema.ids())?;
        let mut error = None;
        expr.visit(&mut |expr| {
            if let Expr::Call { name, args, .. } = expr {
                if let Err(failure) = rtdb_plan::function_type_with_args(name, args) { error = Some(failure); }
            }
        });
        if let Some(error) = error { return Err(error); }
        Ok(Self { expr: expr.clone(), schema: schema.clone() })
    }
    pub fn data_type(&self) -> LogicalType { self.expr.data_type() }
    pub fn evaluate(&self, row: &[Value], context: &EvalContext<'_>) -> Result<Value> { evaluate(&self.expr, &self.schema, row, context) }
    /// Evaluates with the caller-owned suspension, cancellation and allocation contract of `evaluate_async`.
    pub async fn evaluate_async(&self, row: &[Value], context: &EvalContext<'_>) -> Result<Value> { evaluate_async(&self.expr, &self.schema, row, context).await }
}

enum ListEvent {
    Opened(Option<IndexedListBatches>),
    Batch(ListBatch),
}

/// A completed list slice retaining all cell evaluation charges until it is dropped.
pub struct ListBatch { values: rtdb_types::ListValue, _scope: crate::EvaluationScope }
impl ListBatch {
    pub fn values(&self) -> &rtdb_types::ListValue { &self.values }
}

/// Caller-polled list evaluation, with its descriptor selected once at opening.
/// Each pull completes one semantic page.
/// Dropping the reader releases its retained reader state; returned batches and caller-owned preparation charges keep their own lifetimes.
pub struct ListExpression<'a> { batches: ListBatches<'a> }

enum ListBatches<'a> {
    Indexed(IndexedListBatches),
    Stream(futures::stream::BoxStream<'a, Result<ListEvent>>),
}

impl ListExpression<'_> {
    /// Transfers the independent indexed reader, when list opening selected one.
    pub fn into_indexed(self) -> std::result::Result<IndexedListBatches, Self> {
        match self.batches {
            ListBatches::Indexed(batches) => Ok(batches),
            ListBatches::Stream(_) => Err(self),
        }
    }

    pub async fn next_batch(&mut self) -> Result<Option<ListBatch>> {
        use futures::StreamExt;
        match &mut self.batches {
            ListBatches::Indexed(batches) => batches.next_batch(),
            ListBatches::Stream(events) => match events.next().await.transpose()? {
                Some(ListEvent::Batch(batch)) => Ok(Some(batch)),
                Some(ListEvent::Opened(_)) => Err(Anomaly::fault("rtdb.expr/list-state", "List descriptor opened twice")),
                None => Ok(None),
            },
        }
    }
}

/// Pages over an opened integer series, with a position that only changes at page boundaries.
pub struct IndexedListBatches {
    series: crate::series::IntegerSeries,
    position: usize,
    rows_per_batch: usize,
    memory: std::sync::Arc<rtdb_types::MemoryBudget>,
}

impl IndexedListBatches {
    /// Advances whole pages within the given row budget and returns the number of skipped cells.
    pub fn skip_complete_batches(&mut self, rows: u64) -> u64 {
        let remaining = self.series.len() - self.position;
        let requested = usize::try_from(rows).unwrap_or(usize::MAX);
        let skipped = if requested >= remaining { remaining } else { requested / self.rows_per_batch * self.rows_per_batch };
        self.position += skipped;
        skipped as u64
    }

    pub fn next_batch(&mut self) -> Result<Option<ListBatch>> {
        let count = (self.series.len() - self.position).min(self.rows_per_batch);
        if count == 0 { return Ok(None); }
        let scope = crate::EvaluationScope::new(&self.memory)?;
        scope.reserve(count.checked_mul(std::mem::size_of::<i64>()).and_then(|bytes| bytes.checked_add(512)).ok_or_else(overflow)?)?;
        let end = self.position + count;
        let array = arrow_array::Int64Array::from_iter_values((self.position..end).map(|index| self.series.at(index)));
        let field = Arc::new(arrow_schema::Field::new("value", arrow_schema::DataType::Int64, false));
        let values = rtdb_types::ListValue::from_numeric_arrow(field, Arc::new(array)).expect("i64 series is numeric");
        self.position = end;
        Ok(Some(ListBatch { values, _scope: scope }))
    }
}

enum ListStorage { Integer(crate::series::IntegerSeries), Bins { reader: crate::temporal_functions::RangeBins,length: usize }, List(ListValue), Values(Vec<Value>) }

/// Owned list cells or an immutable generated-list descriptor.
/// Retain the evaluation scope until the result is dropped or its `retained_bytes` charge is transferred.
pub struct EvaluatedList { storage: ListStorage }
impl EvaluatedList {
    pub fn len(&self) -> usize { match &self.storage { ListStorage::Integer(series) => series.len(), ListStorage::Bins { length,.. } => *length, ListStorage::List(list) => list.len(), ListStorage::Values(values) => values.len() } }
    pub fn is_empty(&self) -> bool { self.len() == 0 }
    /// Borrows scalar cells if present, and returns an empty slice for an empty packed list.
    pub fn scalar_values(&self) -> Option<&[Value]> {
        match &self.storage {
            ListStorage::Values(values) => Some(values),
            ListStorage::List(list) => list.scalar_values(),
            _ => None,
        }
    }

    /// Exposes packed list cells or writes a generated list directly into Arrow.
    /// Keep the evaluation scope until the returned column is dropped or admitted separately.
    pub fn arrow_column(&self, scope: &crate::EvaluationScope) -> Result<Option<(arrow_schema::FieldRef, arrow_array::ArrayRef)>> {
        use arrow_array::{ArrayRef, Int64Array};
        use arrow_schema::{DataType, Field};
        Ok(match &self.storage {
            ListStorage::List(list) => list.packed_column(),
            ListStorage::Integer(series) => {
                scope.reserve(series.len().checked_mul(8).and_then(|bytes| bytes.checked_add(512)).ok_or_else(overflow)?)?;
                let array = Arc::new(Int64Array::from_iter_values((0..series.len()).map(|index| series.at(index)))) as ArrayRef;
                Some((Arc::new(Field::new("value", DataType::Int64, false)), array))
            }
            ListStorage::Bins { reader, length } => Some(reader.arrow_column(*length, scope)?),
            ListStorage::Values(_) => None,
        })
    }

    /// Bounds the temporary allocation made by `get`; reserve it before calling and retain it while the returned cell lives.
    pub fn cell_allocation_bound(&self) -> usize { if matches!(self.storage,ListStorage::Bins {..}) { 4096 } else { 0 } }
    /// Returns no cell outside the list; generated cells are owned and stored cells remain borrowed.
    /// The caller owns the temporary charge described by `cell_allocation_bound`.
    pub fn get(&self, index: usize) -> Option<std::borrow::Cow<'_, Value>> {
        match &self.storage {
            ListStorage::Integer(series) => (index < series.len()).then(|| std::borrow::Cow::Owned(series.nth(index))),
            ListStorage::Bins { reader,length } => (index < *length).then(|| std::borrow::Cow::Owned(reader.nth(index))),
            ListStorage::List(list) => list.get(index),
            ListStorage::Values(values) => values.get(index).map(std::borrow::Cow::Borrowed),
        }
    }
    /// Includes the descriptor, vector capacity and all owned cell storage.
    pub fn retained_bytes(&self) -> Result<usize> {
        let base = std::mem::size_of::<Self>();
        match &self.storage {
            ListStorage::Integer(_) | ListStorage::Bins {..} => Ok(base),
            ListStorage::List(list) => base.checked_add(list.retained_payload_bytes()?).ok_or_else(overflow),
            ListStorage::Values(values) => {
                let spare = (values.capacity() - values.len()).checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?;
                values.iter().try_fold(base.checked_add(spare).ok_or_else(overflow)?, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(overflow))
            }
        }
    }
}

/// Evaluates a list-table input with the projection validation and cancellation contract of `evaluate_projection_async`.
/// Integer series, range bins, and packed lists retain their existing storage. Non-list inputs produce no cells.
/// Returned cells borrow no inputs; the caller retains the allocation scope until it transfers the result charge.
pub async fn evaluate_list_async(expr: &Expr, schema: &Schema, row: &[Value], context: &EvalContext<'_>) -> Result<EvaluatedList> {
    ExpressionInvocation::prepare(expr,schema,context)?.evaluate_list_async(row,context).await
}

pub fn is_true(value: &Value) -> Result<bool> {
    match value { Value::Bool(value) => Ok(*value), Value::Null | Value::Undefined => Ok(false), _ => Err(inapplicable("boolean predicate", &[value])) }
}

fn truth(value: &Value) -> Result<Option<bool>> {
    match value { Value::Bool(value) => Ok(Some(*value)), Value::Null | Value::Undefined => Ok(None), _ => Err(inapplicable("boolean", &[value])) }
}
fn boolean(value: Option<bool>) -> Value { value.map(Value::Bool).unwrap_or(Value::Null) }
fn retained_clone(value: &Value, context: &EvalContext<'_>) -> Result<Value> { context.reserve(value.retained_bytes()?)?; Ok(value.clone()) }

fn coerce_comparison_literals(left_is_literal: bool, right_is_literal: bool, left: &mut Value, right: &mut Value, timezone: &str) -> Result<()> {
    let date_time = |value: &Value| value.date_days().is_some() || matches!(value, Value::Timestamp { .. });
    if left_is_literal && date_time(right) { *left = crate::cast(left, &right.logical_type(), timezone)?; }
    else if right_is_literal && date_time(left) { *right = crate::cast(right, &left.logical_type(), timezone)?; }
    Ok(())
}

fn unary_value(op: UnaryOp, value: Value) -> Result<Value> {
    match op {
        UnaryOp::IsNull | UnaryOp::IsUnknown => Ok(Value::Bool(nullish(&value))),
        UnaryOp::IsNotNull => Ok(Value::Bool(!nullish(&value))),
        UnaryOp::IsTrue => Ok(Value::Bool(truth(&value)? == Some(true))),
        UnaryOp::IsFalse => Ok(Value::Bool(truth(&value)? == Some(false))),
        UnaryOp::Not => Ok(boolean(truth(&value)?.map(|value| !value))),
        _ if nullish(&value) => Ok(Value::Null),
        UnaryOp::Negate => {
            if let Some(result) = crate::temporal::negate(&value) { return result; }
            if let Some(integer) = integer(&value) {
                let LogicalType::Scalar(ty) = value.logical_type() else { return Err(inapplicable("negate", &[&value])); };
                integer_value(integer.checked_neg().ok_or_else(overflow)?, &ty)
            } else if let Some(number) = number(&value) {
                let LogicalType::Scalar(ty) = value.logical_type() else { return Err(inapplicable("negate", &[&value])); };
                if let Value::Decimal(decimal) = &value { crate::numeric::decimal_value(-crate::numeric::decimal(decimal), decimal.precision, decimal.scale, decimal.bit_width) }
                else { Ok(crate::numeric::floating_value(-number, &ty)) }
            } else { Err(inapplicable("negate", &[&value])) }
        }
        UnaryOp::BitNot => {
            crate::bitwise::call("bit_not", &[value]).unwrap()
        }
    }
}

fn binary_value(op: BinaryOp, left: Value, right: Value, context: &EvalContext<'_>) -> Result<Value> {
    use BinaryOp as B;
    match op {
        B::And => Ok(boolean(match (truth(&left)?, truth(&right)?) { (Some(false), _) | (_, Some(false)) => Some(false), (Some(true), Some(true)) => Some(true), _ => None })),
        B::Or => Ok(boolean(match (truth(&left)?, truth(&right)?) { (Some(true), _) | (_, Some(true)) => Some(true), (Some(false), Some(false)) => Some(false), _ => None })),
        B::IsDistinctFrom | B::IsNotDistinctFrom => {
            let distinct = if nullish(&left) || nullish(&right) { Some(nullish(&left) != nullish(&right)) }
                else { sql_not_eq_scoped(&left, &right, context.timezone, context.allocation)? };
            Ok(boolean(distinct.map(|distinct| if op == B::IsDistinctFrom { distinct } else { !distinct })))
        }
        B::Eq => Ok(boolean(sql_eq_scoped(&left, &right, context.timezone, context.allocation)?)),
        B::NotEq => Ok(boolean(sql_not_eq_scoped(&left, &right, context.timezone, context.allocation)?)),
        _ if nullish(&left) || nullish(&right) => Ok(Value::Null),
        B::Lt | B::LtEq | B::Gt | B::GtEq => {
            if number(&left).is_some_and(f64::is_nan) || number(&right).is_some_and(f64::is_nan) { return Ok(Value::Bool(false)); }
            let order = predicate_order(&left, &right, context.timezone)?;
            Ok(Value::Bool(match op { B::Lt => order == Ordering::Less, B::LtEq => order != Ordering::Greater, B::Gt => order == Ordering::Greater, _ => order != Ordering::Less }))
        }
        B::Concat => {
            context.reserve(left.retained_bytes()?.checked_add(right.retained_bytes()?).and_then(|bytes| bytes.checked_mul(2)).ok_or_else(overflow)?)?;
            match (left,right) {
                (Value::Utf8(mut a),Value::Utf8(b)) => { a.push_str(&b);Ok(Value::Utf8(a)) },
                (Value::Binary(mut a),Value::Binary(b)) => { a.extend(b);Ok(Value::Binary(a)) },
                (a @ Value::List(_),b @ Value::List(_)) => Err(rtdb_types::Anomaly::incorrect("xtdb.error/illegal-arg", format!("No matching clause: {}", vector_type_text(&a.logical_type().join(&b.logical_type()))))),
                (a,b) => Err(inapplicable("concat",&[&a,&b])),
            }
        }
        _ => arithmetic_in_timezone(op,&left,&right,context.timezone),
    }
}

/// An input provider for the current expression row. Values are decoded only when demanded.
pub trait RowInput: Sync {
    fn value(&self, index: usize, allocation: Option<&crate::EvaluationScope>) -> Result<Option<Value>>;
    fn arrow_cell(&self, _index: usize) -> Option<ArrowParameterCell<'_>> { None }
}

#[derive(Clone, Copy)]
pub enum ExpressionRow<'a> {
    Values(&'a [Value]),
    Borrowed(&'a dyn RowInput),
}

impl<'a> ExpressionRow<'a> {
    fn arrow_cell(self, index: usize) -> Option<ArrowParameterCell<'a>> {
        match self {
            Self::Values(_) => None,
            Self::Borrowed(row) => row.arrow_cell(index),
        }
    }

    fn value(self, schema: &Schema, id: rtdb_plan::ColumnId, context: &EvalContext<'_>) -> Result<Value> {
        let missing = || Anomaly::fault("rtdb.expr/missing-column", format!("Input does not contain column {}", id.0));
        let index = schema.position(id).ok_or_else(missing)?;
        match self {
            Self::Values(row) => retained_clone(row.get(index).ok_or_else(missing)?, context),
            Self::Borrowed(row) => row.value(index, context.allocation)?.ok_or_else(missing),
        }
    }
}

macro_rules! expression_body {
    ($expression:expr, $input_schema:expr, $input_row:expr, $inputs:expr, $eval:ident, $call:ident, $reader:ident) => {{
    let (expr, schema, row, context) = ($expression, $input_schema, $input_row, $inputs);
    match expr {
        Expr::Literal(value) => retained_clone(value, context),
        Expr::DurationLiteral(value) => {
            let nanos = i64::try_from(value.nanoseconds()).map_err(|_| Anomaly::incorrect(
                "xtdb.expression/overflow-error", "data exception - overflow error",
            ))?;
            Ok(Value::Duration { value: nanos / 1000, unit: rtdb_types::TimeUnit::Microsecond })
        }
        Expr::Column { id, .. } => row.value(schema, *id, context),
        Expr::Parameter { index, .. } => retained_clone(context.parameters.get(*index)?
            .ok_or_else(|| Anomaly::incorrect("xtdb/missing-parameter", format!("Missing parameter ${}", index + 1)))?, context),
        Expr::Correlated { depth, id, .. } => {
            let missing = || Anomaly::fault("rtdb.expr/missing-correlated-column", "Correlated row is unavailable");
            let outer = context.outer_rows.get(*depth).ok_or_else(missing)?;
            let index = outer.schema.position(*id).ok_or_else(missing)?;
            if let Parameters::Lookup(inputs) = context.parameters
                && let Some(value) = inputs.correlated_value(*depth, index, context.allocation)? {
                return Ok(value);
            }
            retained_clone(outer.values.get(index).ok_or_else(missing)?, context)
        }
        Expr::Unary { op, expr } => {
            let value = $eval!(expr)?;
            unary_value(*op,value)
        }
        Expr::Binary { op, left, right } => {
            use BinaryOp as B;
            let (left_expr, right_expr) = (left.as_ref(), right.as_ref());
            let mut left = $eval!(left_expr)?;
            if *op == B::And && truth(&left)? == Some(false) { return Ok(Value::Bool(false)); }
            if *op == B::Or && truth(&left)? == Some(true) { return Ok(Value::Bool(true)); }
            if *op == B::Concat && nullish(&left) { return Ok(Value::Null); }
            let mut right = $eval!(right_expr)?;
            if matches!(op, B::Eq | B::NotEq | B::Lt | B::LtEq | B::Gt | B::GtEq) {
                coerce_comparison_literals(matches!(left_expr, Expr::Literal(Value::Utf8(_))), matches!(right_expr, Expr::Literal(Value::Utf8(_))), &mut left, &mut right, context.timezone)?;
            }
            binary_value(*op,left,right,context)
        }
        Expr::Call { name, args, .. } if name == "coalesce" => {
            for expr in args { let value = $eval!(expr)?; if !nullish(&value) { return Ok(value); } }
            Ok(Value::Null)
        }
        Expr::Call { name, args, .. } if name.eq_ignore_ascii_case("date_bin") || name.eq_ignore_ascii_case("date-bin") => {
            let stride = $eval!(&args[0])?;
            let origin = if let Some(origin) = args.get(2) { $eval!(origin)? } else { Value::Timestamp { value: 0,unit: rtdb_types::TimeUnit::Microsecond,timezone: Some("UTC".into()) } };
            if nullish(&stride) || nullish(&origin) { return Ok(Value::Null); }
            let source = $eval!(&args[1])?;
            $call!(name, &[stride,source,origin])
        }
        Expr::Call { name, args, .. } if name.eq_ignore_ascii_case("concat") => {
            context.reserve(args.len().checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?)?;
            let mut values = Vec::with_capacity(args.len());
            for expr in args {
                let value = $eval!(expr)?;
                if nullish(&value) { return Ok(Value::Null); }
                values.push(value);
            }
            $call!(name, &values)
        }
        Expr::Call { name, args, .. } if name == "nullif" => {
            let mut left = $eval!(&args[0])?;
            let mut right = $eval!(&args[1])?;
            coerce_comparison_literals(false, matches!(&args[1], Expr::Literal(Value::Utf8(_))), &mut left, &mut right, context.timezone)?;
            Ok(if sql_eq_scoped(&left, &right, context.timezone, context.allocation)? == Some(true) { Value::Null } else { left })
        }
        Expr::Call { name, args, .. } if name == "greatest" || name == "least" => {
            let Some(first) = args.first() else { return Ok(Value::Null); };
            let mut best = $eval!(first)?;
            for expr in &args[1..] {
                if nullish(&best) { return Ok(Value::Null); }
                best = $call!(name, &[best, $eval!(expr)?])?;
            }
            Ok(best)
        }
        Expr::Call { name, args, .. } => {
            context.reserve(args.len().checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?)?;
            let mut values = Vec::with_capacity(args.len());
            for argument in args { values.push($eval!(argument)?); }
            $call!(name, &values)
        }
        Expr::Cast { expr, target } => {
            let value = $eval!(expr)?;
            context.reserve(crate::string::render::allocation_bound(&value)?)?;
            crate::pg_catalog::cast(&value, target, context)
        }
        Expr::SimpleCase { .. } | Expr::Between { .. } | Expr::RegexReplace {..} => $reader!(expr),
        Expr::Case { branches, otherwise } => {
            for (condition, value) in branches { if is_true(&$eval!(condition)?)? { return $eval!(value); } }
            $eval!(otherwise)
        }
        Expr::Coalesce(args) => {
            for expr in args { let value = $eval!(expr)?; if !nullish(&value) { return Ok(value); } }
            Ok(Value::Null)
        }
        Expr::Field { expr, name, .. } => crate::field::get(&$eval!(expr)?,Some(name),context),
        Expr::Subscript { expr, index, .. } => $call!("nth", &[$eval!(expr)?, $eval!(index)?]),
        Expr::List(items) => {
            context.reserve(items.len().checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?)?;
            let mut values = rtdb_types::ListBuilder::with_capacity(items.len());
            for item in items { values.push($eval!(item)?); }
            Ok(Value::List(values.finish()))
        }
        Expr::Struct(fields) => {
            let mut values = StructFields::new();
            for (name, expr) in fields {
                context.reserve(name.len().checked_add(4096).ok_or_else(overflow)?)?;
                let mut value = $eval!(expr)?;
                value.admit_box_date()?;
                values.insert(name.clone(),value);
            }
            Ok(Value::Struct(values))
        },
        Expr::RowCopy(fields) => Ok(Value::Struct(fields.iter().map(|field| {
            context.reserve(field.name.len().checked_add(4096).ok_or_else(overflow)?)?;
            Ok((field.name.clone(), row.value(schema, field.id, context)?))
        }).collect::<Result<_>>()?)),
        Expr::In { expr, values, negated } => {
            let value = $eval!(expr)?;
            let mut unknown = false;
            for candidate in values {
                let candidate = $eval!(candidate)?;
                match sql_eq_scoped(&value, &candidate, context.timezone, context.allocation)? { Some(true) => return Ok(Value::Bool(!*negated)), None => unknown = true, _ => () }
            }
            Ok(if unknown { Value::Null } else { Value::Bool(*negated) })
        }
        Expr::Like { expr, pattern, escape, case_insensitive, negated } => {
            let (value, pattern) = ($eval!(expr)?, $eval!(pattern)?);
            crate::string::pattern::like(&value,&pattern,*escape,*case_insensitive,*negated,context)
        }
    }
    }};
}

/// Evaluates immediately on the caller's thread, preserving short-circuit branches.
/// A reached nonzero SLEEP returns `rtdb.expr/async-required`; production callers accepting SLEEP use `evaluate_async`.
/// Returned values borrow no inputs; retain the allocation scope until their memory charge is transferred or dropped.
/// Scalar calendar intermediates retain their range; struct fields and demanded list cells undergo typed-box admission.
/// List size and type checks do not demand elements; computed elements run when selected.
/// Returned collections are fully materialized and completed dates undergo expression-vector narrowing.
pub fn evaluate(expr: &Expr, schema: &Schema, row: &[Value], context: &EvalContext<'_>) -> Result<Value> {
    ExpressionInvocation::prepare(expr,schema,context)?.evaluate(row,context)
}

fn evaluate_inner(expr: &Expr, schema: &Schema, row: ExpressionRow<'_>, context: &EvalContext<'_>) -> Result<Value> {
    macro_rules! eval { ($child:expr) => { evaluate_inner($child, schema, row, context) }; }
    macro_rules! call { ($name:expr, $args:expr) => { scalar_call($name, $args, context) }; }
    macro_rules! reader { ($expression:expr) => { reader::execute_sync($expression,schema,row,context) }; }
    expression_body!(expr, schema, row, context, eval, call, reader)
}

/// Evaluates on the caller's future, suspending for reached SLEEP calls and yielding during temporal series construction.
/// Dropping the future drops its timer and intermediates; the caller owns cancellation and retains the allocation scope.
/// SLEEP requires a Tokio time driver; non-suspending scalar expressions keep the synchronous evaluation path.
pub async fn evaluate_async(expr: &Expr, schema: &Schema, row: &[Value], context: &EvalContext<'_>) -> Result<Value> {
    ExpressionInvocation::prepare(expr,schema,context)?.evaluate_async(row,context).await
}

async fn evaluate_async_inner(expr: &Expr, schema: &Schema, row: ExpressionRow<'_>, context: &EvalContext<'_>) -> Result<Value> {
    if !expr.requires_suspension() { return evaluate_inner(expr, schema, row, context); }
    evaluate_suspending(expr, schema, row, context)?.await
}

fn evaluate_suspending<'a>(expr: &'a Expr, schema: &'a Schema, row: ExpressionRow<'a>, context: &'a EvalContext<'a>) -> Result<std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value>> + Send + 'a>>> {
    let future = async move {
        macro_rules! eval { ($child:expr) => { evaluate_async_inner($child, schema, row, context).await }; }
        macro_rules! call { ($name:expr, $args:expr) => { crate::functions::scalar_call_async($name, $args, context).await }; }
        macro_rules! reader { ($expression:expr) => { reader::execute_async($expression,schema,row,context).await }; }
        expression_body!(expr, schema, row, context, eval, call, reader)
    };
    context.reserve(std::mem::size_of_val(&future).checked_add(256).ok_or_else(overflow)?)?;
    Ok(Box::pin(future))
}

/// Evaluates one vector projection, copying columns or RowCopy records and validating computed decimal outputs.
/// Validation includes nested cells, but excludes intermediate scalar results and unselected branches.
/// The caller reserves 4096 bytes of validation scratch and charges the returned value before retaining it.
/// Scalar evaluation can allocate before its result is measured and charged.
pub fn evaluate_projection(expr: &Expr, schema: &Schema, row: &[Value], context: &EvalContext<'_>) -> Result<Value> {
    let value = evaluate(expr, schema, row, context)?;
    if !matches!(expr, Expr::Column { .. } | Expr::RowCopy(_)) { validate_projection_value(&value)?; }
    Ok(value)
}

/// Uses the same output validation and ownership contract as `evaluate_projection`, with caller-owned suspension.
/// Dropping the future releases pending SLEEP timers; cancellation is controlled by the caller.
pub async fn evaluate_projection_async(expr: &Expr, schema: &Schema, row: &[Value], context: &EvalContext<'_>) -> Result<Value> {
    let value = evaluate_async(expr, schema, row, context).await?;
    if !matches!(expr, Expr::Column { .. } | Expr::RowCopy(_)) { validate_projection_value(&value)?; }
    Ok(value)
}

pub fn validate_projection_value(value: &Value) -> Result<()> {
    match value {
        Value::Decimal(value) => crate::decimal::validate_write(value),
        Value::Set(values) => values.iter().try_for_each(validate_projection_value),
        Value::List(values) | Value::FixedSizeList(values) => match values.view() {
            rtdb_types::ListView::Values(values) => values.iter().try_for_each(validate_projection_value),
            rtdb_types::ListView::Numeric(values) => rtdb_columnar::validate_projected_decimals(values.field(), values.array().as_ref()),
            rtdb_types::ListView::Scalars(values) => rtdb_columnar::validate_projected_decimals(values.field(), values.array().as_ref()),
            _ => Ok(()),
        },
        Value::Struct(fields) => fields.values().try_for_each(validate_projection_value),
        Value::Map { entries, .. } => entries.iter().try_for_each(|(key, value)| { validate_projection_value(key)?; validate_projection_value(value) }),
        _ => Ok(()),
    }
}

/// The input batch remains borrowed; returned Arrow buffers retain their budget reservation.
/// Compile expressions against `execution_field_type` fields; input cells and domain parameters use signed vector reads.
/// Bare columns retain raw coefficients; computed outputs follow ordinary decimal vector-write validation.
pub fn evaluate_batch(expr: &CompiledExpr, batch: &RecordBatch, context: &EvalContext<'_>, budget: &Arc<MemoryBudget>) -> Result<OwnedBatch> {
    if batch.num_columns() != expr.schema.len() { return Err(Anomaly::fault("rtdb.expr/input-schema", "Batch width differs from bound expression input")); }
    let schema_bytes = batch.schema().fields().iter().try_fold(0usize, |bytes, field| bytes.checked_add(field.size().checked_mul(4).ok_or_else(overflow)?).ok_or_else(overflow))?;
    let _schema = budget.reserve(schema_bytes)?;
    for (actual, expected) in batch.schema().fields().iter().zip(expr.schema.fields()) {
        if rtdb_columnar::execution_field_type(actual)?.unencoded() != expected.data_type.unencoded() {
            return Err(Anomaly::fault("rtdb.expr/input-schema", format!("Input type differs from bound column {}", expected.name)));
        }
    }
    let container_bytes = batch.num_rows().checked_mul(std::mem::size_of::<Value>() + std::mem::size_of::<Arc<rtdb_columnar::MemoryReservation>>()).and_then(|bytes| bytes.checked_add(512)).ok_or_else(overflow)?;
    let _containers = budget.reserve(container_bytes)?;
    let normalized = if let Parameters::Values(values) = context.parameters {
        let bytes = values.iter().try_fold(256usize, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(overflow))?;
        let reservation = budget.reserve(bytes)?;
        let mut values = values.to_vec();
        values.iter_mut().try_for_each(rtdb_columnar::admit_execution_value)?;
        Some((values, reservation))
    } else { None };
    let parameters = normalized.as_ref().map_or(context.parameters, |(values, _)| Parameters::Values(values));
    let preparation = crate::EvaluationScope::new(budget)?;
    let preparation_context = EvalContext { parameters,parameter_types: context.parameter_types,current_time: context.current_time,timezone: context.timezone,outer_rows: context.outer_rows,snapshot_token: context.snapshot_token,allocation: Some(&preparation),catalog: context.catalog };
    let invocation = ExpressionInvocation::prepare(&expr.expr,&expr.schema,&preparation_context)?;
    let mut values = Vec::with_capacity(batch.num_rows());
    let mut scratch = Vec::with_capacity(batch.num_rows());
    for index in 0..batch.num_rows() {
        let scope = crate::EvaluationScope::new(budget)?;
        let context = EvalContext { parameters, parameter_types: context.parameter_types, current_time: context.current_time, timezone: context.timezone, outer_rows: context.outer_rows, snapshot_token: context.snapshot_token, allocation: Some(&scope), catalog: context.catalog };
        let _decimal_validation = budget.reserve(4096)?;
        scope.reserve(batch.columns().iter().zip(batch.schema().fields()).try_fold(0usize, |bytes, (array, field)| bytes.checked_add(rtdb_columnar::estimate_read_value_bytes(field, array.as_ref(), index)?).ok_or_else(overflow))?)?;
        let row = batch.columns().iter().zip(batch.schema().fields()).map(|(array, field)| rtdb_columnar::read_execution_value(field, array.as_ref(), index)).collect::<Result<Vec<_>>>()?;
        let value = invocation.evaluate_projection(&row,&context)?;
        scratch.push(budget.reserve(value.retained_bytes()?)?);
        values.push(value);
    }
    let logical = invocation.data_type().clone();
    let reservation = budget.reserve(estimate_builder_bytes(&logical, &values)?)?;
    let array = copy_array(&logical, &values)?;
    let schema = Arc::new(arrow_schema::Schema::new(vec![to_field("result", &logical)?]));
    let batch = RecordBatch::try_new(schema, vec![array]).map_err(|error| Anomaly::fault("rtdb.expr/output-schema", error.to_string()))?;
    OwnedBatch::with_reservation(batch, reservation)
}

/// The reference's `VectorType` spelling, which its concat reports for an operand type it doesn't concatenate.
pub(crate) fn vector_type_text(data_type: &rtdb_types::LogicalType) -> String {
    use rtdb_types::{LogicalType as L, ScalarType as S};
    match data_type {
        L::Null => "<Null>".into(),
        L::Maybe(inner) => format!("[:? {}]", vector_type_text(inner)),
        L::List { element, .. } => format!("[<List> {}]", vector_type_text(element)),
        L::Struct(fields) => format!("{{{}}}", fields.iter().map(|(name, field)| format!("{name}: {}", vector_type_text(field))).collect::<Vec<_>>().join(", ")),
        L::Scalar(scalar) => format!("<{}>", match scalar {
            S::Bool => "Bool".into(), S::Utf8 => "Utf8".into(), S::Binary => "Binary".into(),
            S::I8 => "Int(8, true)".into(), S::I16 => "Int(16, true)".into(), S::I32 => "Int(32, true)".into(), S::I64 => "Int(64, true)".into(),
            S::F32 => "FloatingPoint(SINGLE)".into(), S::F64 => "FloatingPoint(DOUBLE)".into(),
            other => format!("{other:?}"),
        }),
        other => format!("{other:?}"),
    }
}
