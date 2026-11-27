//! Column-at-a-time evaluation of scalar expressions over plain scalar cells: calls, arithmetic, negation, numeric
//! casts, searched CASE and predicates (comparisons, `===`, LIKE, BETWEEN, IS NULL, NOT, AND and OR), the scalar
//! functions through `rtdb_expr::kernels`, which share their per-value code.
//! A failing cell raises the row evaluator's error for that cell. Each CASE branch and each condition after the first
//! is computed only over the rows that reach it, and the right side of AND and OR only over the rows the left side
//! leaves open, so a cell fails only where the row evaluator would evaluate it.
//! `Ok(None)` leaves a batch to the row evaluator where a layout, a cell type or a result is outside these vectors;
//! every error is raised.
//! Every vector built along the way is charged to the query's memory budget while it lives.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, NullArray, StringArray, TimestampMicrosecondArray, new_null_array};
use arrow_schema::{DataType, Field as ArrowField, TimeUnit as ArrowUnit};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_expr::kernels::{self, Function, LikeMatcher, Selection};
use rtdb_plan::{BinaryOp, CastTarget, Expr, Schema, UnaryOp};
use rtdb_types::{LogicalType, Result, ScalarType, TimeUnit, Value};

use super::{Column, ColumnBatch, SelectedColumn, plain_scalar};

mod predicate;
use predicate::{Cell, Cells, Operand};

/// A batch-length column of one plain scalar type, or of nulls only.
#[derive(Clone)]
enum Vector {
    Null,
    I32(Int32Array),
    I64(Int64Array),
    F64(Float64Array),
    Text(StringArray),
    Bool(BooleanArray),
    /// Microsecond timestamps read under their logical type's zone label.
    Micros(TimestampMicrosecondArray, Option<String>),
}

impl Vector {
    fn scalar(&self) -> Option<ScalarType> {
        Some(match self {
            Self::Null => return None,
            Self::I32(_) => ScalarType::I32,
            Self::I64(_) => ScalarType::I64,
            Self::F64(_) => ScalarType::F64,
            Self::Text(_) => ScalarType::Utf8,
            Self::Bool(_) => ScalarType::Bool,
            Self::Micros(_, zone) => ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: zone.clone() },
        })
    }

    fn array(&self) -> Option<ArrayRef> {
        Some(match self {
            Self::Null => return None,
            Self::I32(array) => Arc::new(array.clone()),
            Self::I64(array) => Arc::new(array.clone()),
            Self::F64(array) => Arc::new(array.clone()),
            Self::Text(array) => Arc::new(array.clone()),
            Self::Bool(array) => Arc::new(array.clone()),
            Self::Micros(array, zone) => Arc::new(array.clone().with_timezone_opt(zone.clone())),
        })
    }

    /// The vector of `array`'s cells read as `scalar`.
    fn of(array: &dyn Array, scalar: &ScalarType) -> Option<Self> {
        let any = array.as_any();
        Some(match scalar {
            ScalarType::I32 => Self::I32(any.downcast_ref::<Int32Array>()?.clone()),
            ScalarType::I64 => Self::I64(any.downcast_ref::<Int64Array>()?.clone()),
            ScalarType::F64 => Self::F64(any.downcast_ref::<Float64Array>()?.clone()),
            ScalarType::Utf8 => Self::Text(any.downcast_ref::<StringArray>()?.clone()),
            ScalarType::Bool => Self::Bool(any.downcast_ref::<BooleanArray>()?.clone()),
            ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone } => Self::Micros(any.downcast_ref::<TimestampMicrosecondArray>()?.clone(), timezone.clone()),
            _ => return None,
        })
    }

    /// The bytes of the buffers this vector holds.
    fn bytes(&self) -> usize {
        match self {
            Self::Null => 0,
            Self::I32(array) => array.get_array_memory_size(),
            Self::I64(array) => array.get_array_memory_size(),
            Self::F64(array) => array.get_array_memory_size(),
            Self::Text(array) => array.get_array_memory_size(),
            Self::Bool(array) => array.get_array_memory_size(),
            Self::Micros(array, _) => array.get_array_memory_size(),
        }
    }
}

/// A vector with the charge for the buffers it built; one read straight from the batch shares the batch's buffers and
/// holds none.
struct Charged { vector: Vector, _charge: Option<Arc<MemoryReservation>> }

impl Charged {
    fn shared(vector: Vector) -> Self { Self { vector, _charge: None } }
    fn built(vector: Vector, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let charge = memory.reserve(vector.bytes())?;
        Ok(Self { vector, _charge: Some(charge) })
    }
}

/// The scalar a stored layout's cells read as, `None` within `Some` for a layout of nulls only.
fn layout(array: &dyn Array, field: &ArrowField, logical: Option<&LogicalType>) -> Option<Option<ScalarType>> {
    let plain = |data_type: &DataType| -> Option<ScalarType> {
        let scalar = match data_type {
            DataType::Int32 => ScalarType::I32, DataType::Int64 => ScalarType::I64, DataType::Float64 => ScalarType::F64,
            DataType::Utf8 => ScalarType::Utf8, DataType::Boolean => ScalarType::Bool,
            _ => return None,
        };
        match logical {
            Some(logical) => (plain_scalar(logical) == Some(&scalar)).then_some(scalar),
            None => field.metadata().is_empty().then_some(scalar),
        }
    };
    Some(match array.data_type() {
        DataType::Null if field.is_nullable() && logical.is_none_or(|ty| *ty == LogicalType::Null) => None,
        DataType::Timestamp(ArrowUnit::Microsecond, zone) => Some(match (logical, logical.and_then(plain_scalar)) {
            (Some(_), Some(scalar @ ScalarType::Timestamp { unit: TimeUnit::Microsecond, .. })) => scalar.clone(),
            (None, _) if field.metadata().is_empty() => ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: zone.as_ref().map(|zone| zone.to_string()) },
            _ => return None,
        }),
        data_type => Some(plain(data_type)?),
    })
}

fn read(column: &Column, declared: &LogicalType, len: usize, memory: &Arc<MemoryBudget>) -> Result<Option<Charged>> {
    if *declared == LogicalType::Null { return Ok(Some(Charged::shared(Vector::Null))); }
    let Some(expected) = plain_scalar(declared) else { return Ok(None) };
    Ok(match column {
        Column::Arrow { array, field, logical } => match layout(array.as_ref(), field, logical.as_ref()) {
            Some(None) => Some(Charged::shared(Vector::Null)),
            Some(Some(scalar)) if scalar == *expected => Vector::of(array.as_ref(), expected).map(Charged::shared),
            _ => None,
        },
        Column::Selected(selected) => read_selected(selected, expected, memory)?,
        Column::Values(values) => read_values(values, expected, len).map(|vector| Charged::built(vector, memory)).transpose()?,
    })
}

/// Gathers a selection whose sources all read as `expected` or as nulls into one vector.
fn read_selected(selected: &SelectedColumn, expected: &ScalarType, memory: &Arc<MemoryBudget>) -> Result<Option<Charged>> {
    let mut data_type = None;
    let mut readable = Vec::with_capacity(selected.sources.len());
    for source in selected.sources.iter() {
        let typed = match source {
            None => false,
            Some((array, field, logical, _)) => match layout(array.as_ref(), field, logical.as_ref()) {
                None => return Ok(None),
                Some(None) => false,
                Some(Some(scalar)) if scalar == *expected => {
                    if *data_type.get_or_insert_with(|| array.data_type().clone()) != *array.data_type() { return Ok(None); }
                    true
                }
                Some(Some(_)) => return Ok(None),
            },
        };
        readable.push(typed);
    }
    let Some(data_type) = data_type else { return Ok(Some(Charged::shared(Vector::Null))) };
    let nulls = new_null_array(&data_type, 1);
    let null = selected.sources.len();
    let arrays: Vec<&dyn Array> = selected.sources.iter().zip(&readable)
        .map(|(source, typed)| match source { Some((array, ..)) if *typed => array.as_ref(), _ => nulls.as_ref() })
        .chain(std::iter::once(nulls.as_ref())).collect();
    let _cells = memory.reserve(cell_bytes(selected.cells.len())?)?;
    let cells: Vec<(usize, usize)> = selected.cells.iter().map(|(source, row)| if readable[source] { (source, row) } else { (null, 0) }).collect();
    let gathered = super::interleave(&arrays, &cells)?;
    Vector::of(gathered.as_ref(), expected).map(|vector| Charged::built(vector, memory)).transpose()
}

/// The bytes of the row-to-source map an interleave reads.
fn cell_bytes(len: usize) -> Result<usize> {
    len.checked_mul(size_of::<(usize, usize)>()).ok_or_else(super::size_error)
}

fn read_values(values: &[Value], expected: &ScalarType, len: usize) -> Option<Vector> {
    if values.len() != len { return None; }
    fn cells<'a, T>(values: &'a [Value], read: impl Fn(&'a Value) -> Option<T>) -> Option<Vec<Option<T>>> {
        values.iter().map(|value| match value { Value::Null => Some(None), value => read(value).map(Some) }).collect()
    }
    Some(match expected {
        ScalarType::I32 => Vector::I32(cells(values, |value| match value { Value::I32(value) => Some(*value), _ => None })?.into()),
        ScalarType::I64 => Vector::I64(cells(values, |value| match value { Value::I64(value) => Some(*value), _ => None })?.into()),
        ScalarType::F64 => Vector::F64(cells(values, |value| match value { Value::F64(value) => Some(*value), _ => None })?.into()),
        ScalarType::Bool => Vector::Bool(cells(values, |value| match value { Value::Bool(value) => Some(*value), _ => None })?.into()),
        ScalarType::Utf8 => Vector::Text(cells(values, |value| match value { Value::Utf8(value) => Some(value.as_str()), _ => None })?.into()),
        ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone } => Vector::Micros(cells(values, |value| match value {
            Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: zone } if zone == timezone => Some(*value),
            _ => None,
        })?.into(), timezone.clone()),
        _ => return None,
    })
}

/// Every row's copy of a constant.
fn constant(value: &Value, len: usize) -> Option<Vector> {
    Some(match value {
        Value::Null => Vector::Null,
        Value::I32(value) => Vector::I32(Int32Array::from_value(*value, len)),
        Value::I64(value) => Vector::I64(Int64Array::from_value(*value, len)),
        Value::F64(value) => Vector::F64(Float64Array::from_value(*value, len)),
        Value::Bool(value) => Vector::Bool(BooleanArray::from(vec![*value; len])),
        Value::Utf8(value) => Vector::Text(StringArray::from_iter_values(std::iter::repeat_n(value.as_str(), len))),
        Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone } => Vector::Micros(TimestampMicrosecondArray::from_value(*value, len), timezone.clone()),
        _ => return None,
    })
}

/// A projected expression, its calls resolved once to the kernels that compute them.
pub(crate) struct Projection(Node);

struct Node { kind: Kind, declared: LogicalType }

enum Kind {
    /// The batch column at this position.
    Column(usize),
    Literal(Value),
    Parameter(usize),
    Extract { field: String, source: Box<Node> },
    /// `scale` is `None` for the one-argument form, which leaves an integer as it is.
    Round { value: Box<Node>, scale: Option<i64> },
    Abs(Box<Node>),
    TextCase { text: Box<Node>, upper: bool },
    Length(Box<Node>),
    Substring { text: Box<Node>, position: Box<Node>, length: Option<Box<Node>> },
    /// A function of one double, reading an integer argument as the double nearest it.
    Double(fn(f64) -> f64, Box<Node>),
    /// `+`, `-`, `*`, `/` or MOD.
    Arithmetic { op: BinaryOp, left: Box<Node>, right: Box<Node> },
    Negate(Box<Node>),
    /// A CAST to the node's declared scalar.
    Cast(Box<Node>),
    Not(Box<Node>),
    /// `=`, `<>`, `<`, `<=`, `>` or `>=`.
    Compare { op: BinaryOp, left: Box<Node>, right: Box<Node> },
    /// `===`, which reads both sides at every row and holds of two nulls.
    Identical { left: Box<Node>, right: Box<Node> },
    /// IS NULL or IS UNKNOWN, or IS NOT NULL where `negated`.
    IsNull { operand: Box<Node>, negated: bool },
    /// LIKE over a literal or bound pattern.
    Like { input: Box<Node>, pattern: Box<Node>, escape: Option<char>, case_insensitive: bool, negated: bool },
    /// BETWEEN, not symmetric.
    Between { operand: Box<Node>, low: Box<Node>, high: Box<Node> },
    /// AND, or OR where not `and`.
    Logical { and: bool, left: Box<Node>, right: Box<Node> },
    /// The conditions of the branches; the values of the branches, then the otherwise value.
    Case { conditions: Vec<Node>, values: Vec<Node> },
}

impl Kind {
    /// The arguments of a strict node, which is null wherever one of them is, in the order the row evaluator prepares
    /// them; `None` for a leaf, a CASE or a predicate answered as truths.
    fn arguments(&self) -> Option<Vec<&Node>> {
        Some(match self {
            Self::Column(_) | Self::Literal(_) | Self::Parameter(_) | Self::Case { .. } | Self::Compare { .. } | Self::Identical { .. }
                | Self::IsNull { .. } | Self::Like { .. } | Self::Between { .. } | Self::Logical { .. } => return None,
            Self::Extract { source: argument, .. } | Self::Round { value: argument, .. } | Self::Abs(argument) | Self::TextCase { text: argument, .. }
                | Self::Length(argument) | Self::Double(_, argument) | Self::Negate(argument) | Self::Cast(argument) | Self::Not(argument) => vec![argument.as_ref()],
            Self::Substring { text, position, length } => [Some(text), Some(position), length.as_ref()].into_iter().flatten().map(|argument| argument.as_ref()).collect(),
            Self::Arithmetic { left, right, .. } => vec![left.as_ref(), right.as_ref()],
        })
    }

    /// Whether this is a predicate answered as truths rather than through its strict arguments.
    fn answers_truths(&self) -> bool {
        matches!(self, Self::Compare { .. } | Self::Identical { .. } | Self::IsNull { .. } | Self::Like { .. } | Self::Between { .. } | Self::Logical { .. })
    }
}

impl Projection {
    /// `None` for an expression no batch can compute here; a bare column, literal or parameter is copied elsewhere.
    pub(crate) fn bind(expr: &Expr, schema: &Schema) -> Option<Self> {
        if matches!(expr, Expr::Column { .. } | Expr::Literal(_) | Expr::Parameter { .. }) { return None; }
        Node::bind(expr, schema).map(Self)
    }

    /// The column over `batch`, typed as the evaluator's packed results are, or `Ok(None)` where the evaluator must
    /// compute it.
    pub(crate) fn column(&self, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>, memory: &Arc<MemoryBudget>) -> Result<Option<Column>> {
        self.evaluated(&Evaluation { columns: &batch.columns, len: batch.len, parameters: Parameters::Values(parameters), memory })
    }

    fn evaluated(&self, evaluation: &Evaluation<'_>) -> Result<Option<Column>> {
        let Some(charged) = evaluation.vector(&self.0, None)? else { return Ok(None) };
        let vector = charged.vector;
        let len = evaluation.len;
        let (array, logical): (ArrayRef, LogicalType) = match (vector.array(), vector.scalar()) {
            (Some(array), Some(scalar)) if array.null_count() < len => {
                let logical = LogicalType::Scalar(scalar);
                let logical = if array.null_count() == 0 { logical } else { logical.maybe() };
                (array, logical)
            }
            _ => (Arc::new(NullArray::new(len)), LogicalType::Null),
        };
        let field = rtdb_columnar::to_field("value", &logical)?;
        Ok((field.data_type() == array.data_type()).then(|| Column::Arrow { array, field: Arc::new(field), logical: Some(logical) }))
    }
}

/// The column of `expr` over `batch`, `Ok(None)` where the row evaluator computes it.
pub(crate) fn column(expr: &Expr, schema: &Schema, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>, memory: &Arc<MemoryBudget>) -> Result<Option<Column>> {
    match Projection::bind(expr, schema) {
        Some(projection) => projection.column(batch, parameters, memory),
        None => Ok(None),
    }
}

/// Each row's three-valued truth under `predicate` over `batch`, keeping unknown (null) apart from false, or
/// `Ok(None)` where the row evaluator computes it. The vectors built along the way are charged to the batch's budget.
pub(crate) fn truth(predicate: &Expr, schema: &Schema, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>) -> Result<Option<Vec<Option<bool>>>> {
    let Some(node) = Node::bind(predicate, schema).filter(Node::truth) else { return Ok(None) };
    let evaluation = Evaluation { columns: &batch.columns, len: batch.len, parameters: Parameters::Values(parameters), memory: batch.reservation.budget() };
    let Some(charged) = evaluation.vector(&node, None)? else { return Ok(None) };
    Ok(match &charged.vector {
        Vector::Null => Some(vec![None; batch.len]),
        Vector::Bool(values) => Some(values.iter().collect()),
        _ => None,
    })
}

/// The one-row column of `expr` over `columns`, reading its parameters from the one-row `arguments`, charged to
/// `memory` for as long as it lives.
pub(crate) fn bound_column(expr: &Expr, schema: &Schema, columns: &[Column], arguments: &[Column], memory: &Arc<MemoryBudget>) -> Result<Option<Column>> {
    if !bound_lowerable(expr) { return Ok(None); }
    let Some(projection) = Projection::bind(expr, schema) else { return Ok(None) };
    let evaluation = Evaluation { columns, len: 1, parameters: Parameters::Columns(arguments), memory };
    let Some(Column::Arrow { array, field, logical }) = projection.evaluated(&evaluation)? else { return Ok(None) };
    // A null result keeps the expression's declared type, as the query's result schema gives it.
    let (array, field, logical) = match (&logical, plain_scalar(&expr.data_type())) {
        (Some(LogicalType::Null), Some(_)) if expr.data_type().nullable() => {
            let declared = expr.data_type();
            let field = rtdb_columnar::to_field("value", &declared)?;
            (new_null_array(field.data_type(), array.len()), Arc::new(field), Some(declared))
        }
        _ => (array, field, logical),
    };
    let charge = memory.reserve(array.get_array_memory_size())?;
    let array = rtdb_columnar::retain_array(array, &charge)?;
    Ok(Some(Column::Arrow { array, field, logical }))
}

/// Whether a leaf of this declared type can be read as a vector.
fn plain_leaf(logical: &LogicalType) -> bool {
    match logical {
        LogicalType::Null => true,
        LogicalType::Maybe(inner) => plain_leaf(inner),
        LogicalType::Scalar(scalar) => matches!(scalar, ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64
            | ScalarType::F32 | ScalarType::F64 | ScalarType::Utf8 | ScalarType::Bool),
        _ => false,
    }
}

/// Whether the shape of `expr` could be computed here; its inputs' layouts decide per batch.
pub(crate) fn lowerable(expr: &Expr) -> bool { lowerable_at(expr, false) }

/// As `lowerable`, for an expression over parameters bound one row at a time.
pub(crate) fn bound_lowerable(expr: &Expr) -> bool { lowerable_at(expr, true) }

fn lowerable_at(expr: &Expr, bound: bool) -> bool {
    stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || {
        let lower = |expr: &Expr| lowerable_at(expr, bound);
        match expr {
            Expr::Parameter { .. } if bound => true,
            Expr::Column { .. } | Expr::Parameter { .. } | Expr::Literal(_) => plain_leaf(&expr.data_type()),
            Expr::Binary { op: BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide | BinaryOp::Modulo | BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::Lt | BinaryOp::LtEq
                | BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::And | BinaryOp::Or, left, right } => lower(left) && lower(right),
            Expr::Unary { op: UnaryOp::Not | UnaryOp::IsNull | UnaryOp::IsNotNull | UnaryOp::Negate, expr } => lower(expr),
            Expr::Cast { expr, target: CastTarget::Fixed(_) } => lower(expr),
            Expr::Case { branches, otherwise } => branches.iter().all(|(condition, value)| lower(condition) && lower(value)) && lower(otherwise),
            Expr::Call { name, args, .. } => match (Function::resolve(name), args.as_slice()) {
                (Some(Function::Upper | Function::Lower), [arg]) if bound => {
                    matches!(arg, Expr::Column { .. } | Expr::Parameter { .. } | Expr::Literal(_)) && lower(arg)
                }
                (Some(Function::Upper | Function::Lower | Function::Length | Function::Abs), [arg]) => lower(arg),
                (Some(Function::Mod), [left, right]) => lower(left) && lower(right),
                _ => false,
            },
            _ => false,
        }
    })
}

impl Node {
    fn bind(expr: &Expr, schema: &Schema) -> Option<Self> {
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || Self::bind_at(expr, schema))
    }

    fn bind_at(expr: &Expr, schema: &Schema) -> Option<Self> {
        let child = |expr: &Expr| Self::bind(expr, schema).map(Box::new);
        let kind = match expr {
            Expr::Column { id, .. } => Kind::Column(schema.position(*id)?),
            Expr::Literal(value) => Kind::Literal(value.clone()),
            Expr::Parameter { index, .. } => Kind::Parameter(*index),
            Expr::Call { name, args, .. } if name == "===" => match args.as_slice() {
                [left, right] => Kind::Identical { left: child(left)?, right: child(right)? },
                _ => return None,
            },
            Expr::Call { name, args, .. } => match (Function::resolve(name)?, args.as_slice()) {
                (Function::Extract, [Expr::Literal(Value::Utf8(field)), source]) => Kind::Extract { field: field.clone(), source: child(source)? },
                (Function::Round, [value]) => Kind::Round { value: child(value)?, scale: None },
                (Function::Round, [value, Expr::Literal(Value::I64(scale))]) => Kind::Round { value: child(value)?, scale: Some(*scale) },
                (Function::Round, [value, Expr::Literal(Value::I32(scale))]) => Kind::Round { value: child(value)?, scale: Some(i64::from(*scale)) },
                (Function::Abs, [value]) => Kind::Abs(child(value)?),
                (Function::Upper, [text]) => Kind::TextCase { text: child(text)?, upper: true },
                (Function::Lower, [text]) => Kind::TextCase { text: child(text)?, upper: false },
                (Function::Length, [text]) => Kind::Length(child(text)?),
                (Function::Substring, [text, position]) => Kind::Substring { text: child(text)?, position: child(position)?, length: None },
                (Function::Substring, [text, position, length]) => Kind::Substring { text: child(text)?, position: child(position)?, length: Some(child(length)?) },
                (Function::Mod, [left, right]) => Kind::Arithmetic { op: BinaryOp::Modulo, left: child(left)?, right: child(right)? },
                (function, [value]) => Kind::Double(function.double()?, child(value)?),
                _ => return None,
            },
            Expr::Binary { op: op @ (BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide | BinaryOp::Modulo), left, right } =>
                Kind::Arithmetic { op: *op, left: child(left)?, right: child(right)? },
            Expr::Binary { op: op @ (BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq), left, right } =>
                Kind::Compare { op: *op, left: child(left)?, right: child(right)? },
            Expr::Binary { op: op @ (BinaryOp::And | BinaryOp::Or), left, right } =>
                Kind::Logical { and: *op == BinaryOp::And, left: child(left)?.filter_truth()?, right: child(right)?.filter_truth()? },
            Expr::Unary { op: UnaryOp::Negate, expr: operand } => Kind::Negate(child(operand)?),
            Expr::Unary { op: UnaryOp::Not, expr: operand } => Kind::Not(child(operand)?),
            Expr::Unary { op: op @ (UnaryOp::IsNull | UnaryOp::IsUnknown | UnaryOp::IsNotNull), expr: operand } =>
                Kind::IsNull { operand: child(operand)?, negated: *op == UnaryOp::IsNotNull },
            Expr::Like { expr: input, pattern, escape, case_insensitive, negated }
                if matches!(pattern.as_ref(), Expr::Literal(Value::Utf8(_) | Value::Null) | Expr::Parameter { .. }) =>
                Kind::Like { input: child(input)?, pattern: child(pattern)?, escape: *escape, case_insensitive: *case_insensitive, negated: *negated },
            Expr::Between { operand, low, high, symmetric: false } => Kind::Between { operand: child(operand)?, low: child(low)?, high: child(high)? },
            Expr::Cast { expr: operand, target: CastTarget::Fixed(_) } => Kind::Cast(child(operand)?),
            Expr::Case { branches, otherwise } => Kind::Case {
                conditions: branches.iter().map(|(condition, _)| Self::bind(condition, schema).filter(Self::truth)).collect::<Option<_>>()?,
                values: branches.iter().map(|(_, value)| value).chain(std::iter::once(otherwise.as_ref()))
                    .map(|value| Self::bind(value, schema)).collect::<Option<_>>()?,
            },
            _ => return None,
        };
        Some(Self { kind, declared: expr.data_type() })
    }

    /// Whether this node yields truths: it is typed boolean or null.
    fn truth(&self) -> bool { self.declared == LogicalType::Null || plain_scalar(&self.declared) == Some(&ScalarType::Bool) }

    fn filter_truth(self: Box<Self>) -> Option<Box<Self>> { self.truth().then_some(self) }

    fn leaf(&self) -> bool { matches!(self.kind, Kind::Column(_) | Kind::Literal(_) | Kind::Parameter(_)) }

    /// Whether preparing this node, of a single type other than null, as the row evaluator does before demanding its
    /// payload raises nothing: whatever preparing it demands (the conditions of a CASE, the operand of BETWEEN, a
    /// predicate that may be null, an argument of several legs) can't fail.
    fn settles(&self) -> bool {
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || match &self.kind {
            Kind::Column(_) | Kind::Literal(_) | Kind::Parameter(_) => true,
            Kind::Compare { left, right, .. } | Kind::Identical { left, right } => left.prepares() && right.prepares(),
            Kind::Like { input, pattern, .. } => input.prepares() && pattern.prepares(),
            Kind::IsNull { operand, .. } => operand.prepares(),
            // AND and OR answer as they are prepared where they may be null, and are deferred where they can't be.
            Kind::Logical { .. } => !self.declared.nullable() || self.infallible(),
            Kind::Between { operand, .. } => operand.infallible() && (!self.declared.nullable() || self.infallible()),
            Kind::Case { conditions, values } => conditions.iter().all(Self::infallible) && values.iter().all(Self::settles),
            kind => kind.arguments().is_some_and(|arguments| arguments.into_iter().all(Self::prepares)),
        })
    }

    /// Whether preparing this node as an argument raises nothing: one of several legs, or typed null, is demanded as it
    /// is prepared; one of a single other type is only prepared.
    fn prepares(&self) -> bool {
        if self.declared != LogicalType::Null && self.declared.legs().len() < 2 { self.settles() } else { self.infallible() }
    }

    /// Whether computing this node over any rows raises nothing.
    fn infallible(&self) -> bool {
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || match &self.kind {
            Kind::Column(_) | Kind::Literal(_) | Kind::Parameter(_) => true,
            Kind::Not(operand) | Kind::IsNull { operand, .. } => operand.infallible(),
            Kind::Compare { left, right, .. } | Kind::Identical { left, right } | Kind::Logical { left, right, .. } => left.infallible() && right.infallible(),
            Kind::Between { operand, low, high } => operand.infallible() && low.infallible() && high.infallible(),
            Kind::Like { input, pattern, escape, case_insensitive, negated } => input.infallible() && match &pattern.kind {
                Kind::Literal(Value::Utf8(pattern)) => LikeMatcher::compile(pattern, *escape, *case_insensitive, *negated).is_ok(),
                Kind::Literal(Value::Null) => true,
                _ => false,
            },
            Kind::Case { conditions, values } => conditions.iter().chain(values).all(Self::infallible),
            _ => false,
        })
    }
}

/// Where parameters are read from: values known for the whole batch, or one-row columns bound with the query.
#[derive(Clone, Copy)]
enum Parameters<'a> { Values(rtdb_expr::Parameters<'a>), Columns(&'a [Column]) }

struct Evaluation<'a> {
    columns: &'a [Column],
    len: usize,
    parameters: Parameters<'a>,
    memory: &'a Arc<MemoryBudget>,
}

/// A strict node's arguments once prepared: null at every row, or computed with the rows none is null at, `None` for
/// every row.
enum Prepared<T> { Null, Live { rows: Option<Vec<bool>>, arguments: Vec<T> } }

/// Where a prepared argument is null.
enum Validity { Valid, Null, Rows(Vec<bool>) }

/// A computed argument whose nulls its preparation reads.
trait Argument {
    /// Where the argument is valid among `rows`, `None` where a cell there is outside these vectors.
    fn validity(&self, rows: Selection<'_>, len: usize) -> Option<Validity>;
}

impl Argument for Charged {
    fn validity(&self, _: Selection<'_>, len: usize) -> Option<Validity> {
        Some(match self.vector.array() {
            None => Validity::Null,
            Some(array) if array.null_count() == 0 => Validity::Valid,
            Some(array) => Validity::Rows((0..len).map(|row| array.is_valid(row)).collect()),
        })
    }
}

impl Argument for Operand<'_> {
    fn validity(&self, rows: Selection<'_>, len: usize) -> Option<Validity> {
        match self {
            Self::Computed(charged) => charged.validity(rows, len),
            Self::Constant(None) | Self::Cells(Cells::Null) => Some(Validity::Null),
            Self::Constant(Some(_)) => Some(Validity::Valid),
            Self::Cells(cells) if cells.complete() => Some(Validity::Valid),
            Self::Cells(_) | Self::Values { .. } => {
                let mut valid = Vec::with_capacity(len);
                for row in 0..len { valid.push(reads(rows, row) && self.get(row).ok()?.is_some()); }
                Some(Validity::Rows(valid))
            }
        }
    }
}

/// A predicate's answers: null at every row, as where a strict argument is, or a truth for each row.
enum Truths { Null, Rows(Vec<Option<bool>>) }

fn reads(rows: Selection<'_>, row: usize) -> bool { rows.is_none_or(|rows| rows[row]) }

/// Whether a vector holds truths.
fn holds_truths(vector: &Vector) -> bool { matches!(vector, Vector::Null | Vector::Bool(_)) }

/// The truth a vector of truths holds at `row`.
fn truth_at(vector: &Vector, row: usize) -> Option<bool> {
    match vector { Vector::Bool(values) => values.is_valid(row).then(|| values.value(row)), _ => None }
}

/// Integers of the type `result`, held as 64-bit integers, as a vector of that type.
fn narrowed(values: Int64Array, result: &ScalarType) -> Vector {
    match result {
        ScalarType::I32 => Vector::I32(values.unary(|value| value as i32)),
        _ => Vector::I64(values),
    }
}

impl Evaluation<'_> {
    /// `node` over the rows `selection` picks; the others are null.
    fn vector(&self, node: &Node, selection: Selection<'_>) -> Result<Option<Charged>> {
        // A branch no row takes computes nothing, so nothing in it can fail.
        if selection.is_some_and(|rows| !rows.contains(&true)) { return Ok(Some(Charged::shared(Vector::Null))); }
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || self.vector_at(node, selection))
    }

    fn built(&self, vector: Option<Vector>) -> Result<Option<Charged>> {
        vector.map(|vector| Charged::built(vector, self.memory)).transpose()
    }

    /// The charge for one truth per row while a predicate's answers are gathered.
    fn truth_scratch(&self) -> Result<Arc<MemoryReservation>> {
        self.memory.reserve(self.len.checked_mul(size_of::<Option<bool>>()).ok_or_else(super::size_error)?)
    }

    fn vector_at(&self, node: &Node, selection: Selection<'_>) -> Result<Option<Charged>> {
        if node.kind.answers_truths() { return self.predicate(node, selection); }
        let len = self.len;
        let null = || -> Result<Option<Charged>> { Ok(Some(Charged::shared(Vector::Null))) };
        // A strict node's arguments are prepared, in order, before any payload is read.
        let live;
        let (selection, arguments) = match node.kind.arguments() {
            None => (selection, Vec::new()),
            Some(arguments) => match self.prepared(&arguments, selection, |argument, rows| self.vector(argument, rows))? {
                None => return Ok(None),
                Some(Prepared::Null) => return null(),
                Some(Prepared::Live { rows, arguments }) => {
                    live = rows;
                    (live.as_deref(), arguments)
                }
            },
        };
        let mut arguments = arguments.into_iter();
        let mut argument = move || arguments.next().expect("a strict node's arguments are prepared");
        let charged = match &node.kind {
            Kind::Column(position) => read(&self.columns[*position], &node.declared, len, self.memory)?,
            Kind::Literal(value) => self.built(constant(value, len))?,
            Kind::Parameter(index) => match self.parameters {
                // An Arrow-bound parameter is decoded on its first read, which `known` alone would leave undone.
                Parameters::Values(parameters) => match parameters.get(*index)? {
                    Some(value) => self.built(constant(value, len))?,
                    None => None,
                },
                Parameters::Columns(arguments) => match arguments.get(*index) {
                    Some(argument) => read(argument, &node.declared, len, self.memory)?,
                    None => None,
                },
            },
            Kind::Extract { field, .. } => {
                let source = argument();
                match &source.vector {
                    Vector::Null => return null(),
                    Vector::Micros(values, zone) => self.built(kernels::extract_timestamps(field, values, zone.as_deref(), selection)?.map(Vector::I32))?,
                    _ => None,
                }
            }
            Kind::Round { scale, .. } => {
                let value = argument();
                // Rounding an integer to no places is the integer itself.
                if scale.is_none() && matches!(value.vector, Vector::I32(_) | Vector::I64(_)) {
                    Some(value)
                } else {
                    match &value.vector {
                        Vector::Null => return null(),
                        Vector::F64(values) => self.built(Some(Vector::F64(kernels::round_doubles(values, scale.unwrap_or(0), selection)?)))?,
                        _ => None,
                    }
                }
            }
            Kind::Abs(_) => {
                let argument = argument();
                match &argument.vector {
                    Vector::Null => return null(),
                    Vector::F64(values) => self.built(Some(Vector::F64(kernels::doubles(f64::abs, values))))?,
                    Vector::I64(values) => self.built(Some(Vector::I64(kernels::abs_integers(values, selection)?)))?,
                    _ => None,
                }
            }
            Kind::TextCase { upper, .. } => {
                let text = argument();
                match &text.vector {
                    Vector::Null => return null(),
                    Vector::Text(values) => self.built(kernels::text_case(values, *upper, selection)?.map(Vector::Text))?,
                    _ => None,
                }
            }
            Kind::Length(_) => {
                let text = argument();
                match &text.vector {
                    Vector::Null => return null(),
                    Vector::Text(values) => self.built(Some(Vector::I32(kernels::text_lengths(values))))?,
                    _ => None,
                }
            }
            Kind::Substring { length, .. } => {
                let (text, position) = (argument(), argument());
                let length = length.as_ref().map(|_| argument());
                let Vector::Text(values) = &text.vector else { return Ok(None) };
                let Some(positions) = self.integers(&position.vector)? else { return Ok(None) };
                let lengths = match &length {
                    Some(length) => match self.integers(&length.vector)? { Some(lengths) => Some(lengths), None => return Ok(None) },
                    None => None,
                };
                self.built(kernels::text_substrings(values, &positions.0, lengths.as_ref().map(|lengths| &lengths.0), selection)?.map(Vector::Text))?
            }
            Kind::Double(function, _) => {
                let argument = argument();
                let Some(values) = self.doubles(&argument.vector)? else { return Ok(None) };
                self.built(Some(Vector::F64(kernels::doubles(*function, &values.0))))?
            }
            Kind::Arithmetic { op, .. } => {
                let (left, right) = (argument(), argument());
                let floating = matches!(left.vector, Vector::F64(_)) || matches!(right.vector, Vector::F64(_));
                let wide = matches!(left.vector, Vector::I64(_)) || matches!(right.vector, Vector::I64(_));
                match plain_scalar(&node.declared) {
                    Some(ScalarType::F64) if floating => {
                        let (Some(left), Some(right)) = (self.doubles(&left.vector)?, self.doubles(&right.vector)?) else { return Ok(None) };
                        self.built(Some(Vector::F64(kernels::float_arithmetic(*op, &left.0, &right.0, selection)?)))?
                    }
                    // Two integers give the wider of their types.
                    Some(result @ (ScalarType::I32 | ScalarType::I64)) if !floating && wide == (*result == ScalarType::I64) => {
                        let (Some(left), Some(right)) = (self.integers(&left.vector)?, self.integers(&right.vector)?) else { return Ok(None) };
                        let values = kernels::integer_arithmetic(*op, &left.0, &right.0, result, selection)?;
                        self.built(Some(narrowed(values, result)))?
                    }
                    _ => None,
                }
            }
            Kind::Negate(_) => {
                let operand = argument();
                match &operand.vector {
                    Vector::Null => return null(),
                    Vector::F64(values) => self.built(Some(Vector::F64(kernels::doubles(|value: f64| -value, values))))?,
                    vector @ (Vector::I32(_) | Vector::I64(_)) => {
                        let result = vector.scalar().expect("an integer vector has a type");
                        let Some(values) = self.integers(vector)? else { return Ok(None) };
                        self.built(Some(narrowed(kernels::negate_integers(&values.0, &result, selection)?, &result)))?
                    }
                    _ => None,
                }
            }
            Kind::Cast(_) => {
                let operand = argument();
                let Some(target) = plain_scalar(&node.declared) else { return Ok(None) };
                let identity = !matches!(operand.vector, Vector::Micros(..)) && operand.vector.scalar().as_ref() == Some(target);
                if identity {
                    Some(operand)
                } else {
                    match (&operand.vector, target) {
                        (Vector::Null, _) => return null(),
                        (Vector::I32(values), ScalarType::I64) => self.built(Some(Vector::I64(values.unary(i64::from))))?,
                        (Vector::I64(values), ScalarType::I32) => self.built(Some(narrowed(kernels::cast_integers(values, &ScalarType::I32, selection)?, &ScalarType::I32)))?,
                        (Vector::I32(values), ScalarType::F64) => self.built(Some(Vector::F64(values.unary(f64::from))))?,
                        (Vector::I64(values), ScalarType::F64) => self.built(Some(Vector::F64(values.unary(|value| value as f64))))?,
                        _ => None,
                    }
                }
            }
            Kind::Not(_) => {
                let operand = argument();
                match &operand.vector {
                    Vector::Null => return null(),
                    Vector::Bool(values) => self.built(Some(Vector::Bool(values.iter().map(|value| value.map(|value| !value)).collect())))?,
                    _ => None,
                }
            }
            Kind::Case { conditions, values } => self.case(conditions, values, selection)?,
            Kind::Compare { .. } | Kind::Identical { .. } | Kind::IsNull { .. } | Kind::Like { .. } | Kind::Between { .. } | Kind::Logical { .. } =>
                unreachable!("predicates are answered as truths"),
        };
        let Some(charged) = charged else { return Ok(None) };
        // Each node yields its declared type, as the evaluator's cells do.
        let typed = match charged.vector.scalar() {
            None => node.declared.nullable(),
            Some(scalar) => plain_scalar(&node.declared) == Some(&scalar),
        };
        Ok(typed.then_some(charged))
    }

    /// A strict node's `arguments` prepared over `selection` as the row evaluator's continuations prepare them, one
    /// after another, each over the rows no earlier one is null at. An argument typed null is null at every row, after
    /// its own arguments are prepared; one of a type with several legs, a nullable one among them, is demanded as it
    /// is prepared, and is null where its value is; one of a single other type is typed so at every row, and its
    /// payload is computed once all are prepared, over only the rows none is null at, where it would be demanded.
    /// Where an argument is null at every row, the single-typed ones before it are left unread, which is only taken
    /// here where preparing them raises nothing.
    fn prepared<'n, T: Argument>(&self, arguments: &[&'n Node], selection: Selection<'_>,
        compute: impl Fn(&'n Node, Selection<'_>) -> Result<Option<T>>) -> Result<Option<Prepared<T>>> {
        let len = self.len;
        let mut rows: Option<Vec<bool>> = selection.map(<[bool]>::to_vec);
        let mut computed: Vec<Option<T>> = Vec::with_capacity(arguments.len());
        let null = |computed: &[Option<T>]| -> Option<Prepared<T>> {
            let deferred = arguments.iter().zip(computed).filter(|(_, value)| value.is_none());
            deferred.into_iter().all(|(argument, _)| argument.settles()).then_some(Prepared::Null)
        };
        for argument in arguments {
            if argument.declared == LogicalType::Null {
                if null(&computed).is_none() || compute(*argument, rows.as_deref())?.is_none() { return Ok(None); }
                return Ok(Some(Prepared::Null));
            }
            if argument.declared.legs().len() < 2 { computed.push(None); continue; }
            let Some(value) = compute(*argument, rows.as_deref())? else { return Ok(None) };
            match value.validity(rows.as_deref(), len) {
                None => return Ok(None),
                Some(Validity::Null) => return Ok(null(&computed)),
                Some(Validity::Valid) => {}
                Some(Validity::Rows(valid)) => {
                    let live: Vec<bool> = (0..len).map(|row| valid[row] && reads(rows.as_deref(), row)).collect();
                    if !live.contains(&true) { return Ok(null(&computed)); }
                    rows = Some(live);
                }
            }
            computed.push(Some(value));
        }
        let mut prepared = Vec::with_capacity(arguments.len());
        for (argument, value) in arguments.iter().zip(computed) {
            let value = match value {
                Some(value) => value,
                None => match compute(*argument, rows.as_deref())? { Some(value) => value, None => return Ok(None) },
            };
            prepared.push(value);
        }
        Ok(Some(Prepared::Live { rows, arguments: prepared }))
    }

    /// The cells of a predicate's operand over `rows`: read in place from a column, a bound parameter or a literal, or
    /// computed.
    fn operand<'s>(&'s self, node: &'s Node, rows: Selection<'_>) -> Result<Option<Operand<'s>>> {
        Ok(match &node.kind {
            Kind::Column(position) => self.cells(&self.columns[*position], rows)?,
            Kind::Literal(value) => predicate::scalar(value).ok().map(Operand::Constant),
            Kind::Parameter(index) => match self.parameters {
                // An Arrow-bound parameter is decoded on its first read, which `known` alone would leave undone.
                Parameters::Values(parameters) => match parameters.get(*index)? {
                    Some(value) => predicate::scalar(value).ok().map(Operand::Constant),
                    None => None,
                },
                Parameters::Columns(arguments) => match arguments.get(*index) {
                    Some(argument) => self.cells(argument, rows)?,
                    None => None,
                },
            },
            _ => self.vector(node, rows)?.map(Operand::Computed),
        })
    }

    /// A column's cells, read in place where a reader takes its layout, else read once into values over `rows`.
    fn cells<'s>(&'s self, column: &'s Column, rows: Selection<'_>) -> Result<Option<Operand<'s>>> {
        if let Some(cells) = Cells::of(column) { return Ok(Some(Operand::Cells(cells))); }
        match column {
            Column::Selected(_) => return Ok(None),
            Column::Arrow { array, .. } if !predicate::readable_values(array.data_type()) => return Ok(None),
            _ => {}
        }
        let mut charge = self.memory.reserve(self.len.checked_mul(size_of::<Value>()).ok_or_else(super::size_error)?)?;
        let lease = Arc::get_mut(&mut charge).expect("a new reservation is unshared");
        let mut values = Vec::with_capacity(self.len);
        for row in 0..self.len {
            let value = if reads(rows, row) { column.value(row)? } else { Value::Null };
            lease.grow_to(lease.bytes().checked_add(value.retained_bytes()?).ok_or_else(super::size_error)?)?;
            values.push(value);
        }
        Ok(Some(Operand::Values { values, _charge: charge }))
    }

    /// A predicate's truths over the rows `selection` picks, as a vector of truths of its declared type.
    fn predicate(&self, node: &Node, selection: Selection<'_>) -> Result<Option<Charged>> {
        let truths = match &node.kind {
            Kind::Compare { op, left, right } => self.compare(*op, left, right, selection)?,
            Kind::Identical { left, right } => self.identical(left, right, selection)?,
            Kind::IsNull { operand, negated } => self.is_null(operand, *negated, selection)?,
            Kind::Like { input, pattern, escape, case_insensitive, negated } => self.like(input, pattern, (*escape, *case_insensitive, *negated), selection)?,
            Kind::Between { operand, low, high } => self.between(operand, low, high, selection)?,
            Kind::Logical { and, left, right } => self.logical(*and, left, right, selection)?,
            _ => unreachable!("only predicates are answered as truths"),
        };
        match truths {
            None => Ok(None),
            Some(Truths::Null) => Ok(Some(Charged::shared(Vector::Null))),
            Some(Truths::Rows(truths)) if plain_scalar(&node.declared) == Some(&ScalarType::Bool) =>
                Charged::built(Vector::Bool(BooleanArray::from(truths)), self.memory).map(Some),
            // A predicate typed null answers null at every row.
            Some(Truths::Rows(truths)) => Ok((node.declared.nullable() && truths.iter().all(Option::is_none)).then(|| Charged::shared(Vector::Null))),
        }
    }

    /// A comparison, strict in both operands.
    fn compare(&self, op: BinaryOp, left: &Node, right: &Node, selection: Selection<'_>) -> Result<Option<Truths>> {
        use std::cmp::Ordering::{self, Equal, Greater, Less};
        let (equality_only, accept): (bool, fn(Ordering) -> bool) = match op {
            BinaryOp::Eq => (true, |ordering| ordering == Equal), BinaryOp::NotEq => (true, |ordering| ordering != Equal),
            BinaryOp::Lt => (false, |ordering| ordering == Less), BinaryOp::LtEq => (false, |ordering| ordering != Greater),
            BinaryOp::Gt => (false, |ordering| ordering == Greater), BinaryOp::GtEq => (false, |ordering| ordering != Less),
            _ => return Ok(None),
        };
        let Some(prepared) = self.prepared(&[left, right], selection, |operand, rows| self.operand(operand, rows))? else { return Ok(None) };
        let Prepared::Live { rows, arguments } = prepared else { return Ok(Some(Truths::Null)) };
        let [left, right] = arguments.as_slice() else { unreachable!("a comparison has two operands") };
        let live = |row: usize| reads(rows.as_deref(), row);
        let len = self.len;
        let _scratch = self.truth_scratch()?;
        // An integer column against an integer constant compares the values in place.
        match (left, right) {
            (Operand::Cells(Cells::Int(array)), Operand::Constant(Some(Cell::Int(constant)))) =>
                return Ok(Some(Truths::Rows((0..len).map(|row| (live(row) && array.is_valid(row)).then(|| accept(array.value(row).cmp(constant)))).collect()))),
            (Operand::Constant(Some(Cell::Int(constant))), Operand::Cells(Cells::Int(array))) =>
                return Ok(Some(Truths::Rows((0..len).map(|row| (live(row) && array.is_valid(row)).then(|| accept(constant.cmp(&array.value(row))))).collect()))),
            _ => {}
        }
        let mut truths = Vec::with_capacity(len);
        for row in 0..len {
            if !live(row) { truths.push(None); continue; }
            let (Ok(a), Ok(b)) = (left.get(row), right.get(row)) else { return Ok(None) };
            truths.push(match (a, b) {
                // An ordering with NaN on either side is false, as the scalar comparison answers.
                (Some(a), Some(b)) if !equality_only && (a.is_nan() || b.is_nan()) => Some(false),
                (Some(a), Some(b)) => match predicate::order(a, b, equality_only) { Ok(ordering) => Some(accept(ordering)), Err(()) => return Ok(None) },
                _ => None,
            });
        }
        Ok(Some(Truths::Rows(truths)))
    }

    /// `left === right`, both read at every row.
    fn identical(&self, left: &Node, right: &Node, selection: Selection<'_>) -> Result<Option<Truths>> {
        let Some(left) = self.operand(left, selection)? else { return Ok(None) };
        let Some(right) = self.operand(right, selection)? else { return Ok(None) };
        let _scratch = self.truth_scratch()?;
        let mut truths = Vec::with_capacity(self.len);
        for row in 0..self.len {
            if !reads(selection, row) { truths.push(None); continue; }
            let (Ok(a), Ok(b)) = (left.get(row), right.get(row)) else { return Ok(None) };
            let Ok(same) = predicate::identical(a, b) else { return Ok(None) };
            truths.push(Some(same));
        }
        Ok(Some(Truths::Rows(truths)))
    }

    /// IS NULL, or IS NOT NULL where `negated`.
    fn is_null(&self, operand: &Node, negated: bool, selection: Selection<'_>) -> Result<Option<Truths>> {
        let len = self.len;
        let answer = |null: bool| Some(null != negated);
        // A computed operand of a single type other than null is never null, and the row evaluator only prepares it.
        let computed = !operand.leaf() && !matches!(operand.kind, Kind::Case { .. });
        if computed && operand.declared != LogicalType::Null && operand.declared.legs().len() < 2 {
            return Ok(operand.settles().then(|| Truths::Rows((0..len).map(|row| if reads(selection, row) { answer(false) } else { None }).collect())));
        }
        if let Kind::Column(position) = &operand.kind && let Column::Selected(column) = &self.columns[*position] {
            return Ok(column.null_test(!negated).map(Truths::Rows));
        }
        let Some(value) = self.operand(operand, selection)? else { return Ok(None) };
        let _scratch = self.truth_scratch()?;
        let mut truths = Vec::with_capacity(len);
        for row in 0..len {
            if !reads(selection, row) { truths.push(None); continue; }
            let Ok(cell) = value.get(row) else { return Ok(None) };
            truths.push(answer(cell.is_none()));
        }
        Ok(Some(Truths::Rows(truths)))
    }

    /// LIKE, strict in its input and pattern; an invalid pattern raises the scalar LIKE's error where both are present.
    fn like(&self, input: &Node, pattern: &Node, (escape, case_insensitive, negated): (Option<char>, bool, bool), selection: Selection<'_>) -> Result<Option<Truths>> {
        let Some(prepared) = self.prepared(&[input, pattern], selection, |operand, rows| self.operand(operand, rows))? else { return Ok(None) };
        let Prepared::Live { rows, arguments } = prepared else { return Ok(Some(Truths::Null)) };
        let [input, pattern] = arguments.as_slice() else { unreachable!("LIKE has an input and a pattern") };
        let pattern = match pattern {
            Operand::Constant(Some(Cell::Text(pattern))) => *pattern,
            // A pattern bound one row at a time is that row's.
            pattern if self.len == 1 => match pattern.get(0) { Ok(Some(Cell::Text(pattern))) => pattern, _ => return Ok(None) },
            _ => return Ok(None),
        };
        let like = LikeMatcher::compile(pattern, escape, case_insensitive, negated)?;
        let mut matches = like.matcher();
        let _scratch = self.truth_scratch()?;
        let mut truths = Vec::with_capacity(self.len);
        for row in 0..self.len {
            if !reads(rows.as_deref(), row) { truths.push(None); continue; }
            truths.push(match input.get(row) {
                Ok(None) => None,
                Ok(Some(Cell::Text(text))) => Some(matches(text)),
                _ => return Ok(None),
            });
        }
        Ok(Some(Truths::Rows(truths)))
    }

    /// `operand BETWEEN low AND high`: the operand is demanded first, the low bound only where it is present, and the
    /// high bound only where the low one doesn't already answer false.
    fn between(&self, operand: &Node, low: &Node, high: &Node, selection: Selection<'_>) -> Result<Option<Truths>> {
        use std::cmp::Ordering::{Greater, Less};
        let len = self.len;
        let Some(value) = self.operand(operand, selection)? else { return Ok(None) };
        let mut present = vec![false; len];
        for (row, present) in present.iter_mut().enumerate() {
            if !reads(selection, row) { continue; }
            let Ok(cell) = value.get(row) else { return Ok(None) };
            *present = cell.is_some();
        }
        let _scratch = self.truth_scratch()?;
        let Some(lower) = self.bound(&value, low, &present, |ordering| ordering != Less)? else { return Ok(None) };
        let open: Vec<bool> = (0..len).map(|row| present[row] && lower[row] != Some(false)).collect();
        let Some(upper) = self.bound(&value, high, &open, |ordering| ordering != Greater)? else { return Ok(None) };
        Ok(Some(Truths::Rows((0..len).map(|row| {
            if !present[row] { return None; }
            match (lower[row], upper[row]) { (Some(false), _) | (_, Some(false)) => Some(false), (Some(true), Some(true)) => Some(true), _ => None }
        }).collect())))
    }

    /// One bound of BETWEEN against `value` over `rows`: whether each row's value lies on its side, null where the
    /// bound is.
    fn bound(&self, value: &Operand<'_>, bound: &Node, rows: &[bool], accept: fn(std::cmp::Ordering) -> bool) -> Result<Option<Vec<Option<bool>>>> {
        let mut truths = vec![None; self.len];
        if !rows.contains(&true) { return Ok(Some(truths)); }
        let Some(bound) = self.operand(bound, Some(rows))? else { return Ok(None) };
        for (row, truth) in truths.iter_mut().enumerate() {
            if !rows[row] { continue; }
            let (Ok(Some(a)), Ok(b)) = (value.get(row), bound.get(row)) else { return Ok(None) };
            *truth = match b {
                None => None,
                Some(b) if a.is_nan() || b.is_nan() => Some(false),
                Some(b) => match predicate::order(a, b, false) { Ok(ordering) => Some(accept(ordering)), Err(()) => return Ok(None) },
            };
        }
        Ok(Some(truths))
    }

    /// AND, or OR where not `and`: the right side is computed only over the rows the left side leaves open.
    fn logical(&self, and: bool, left: &Node, right: &Node, selection: Selection<'_>) -> Result<Option<Truths>> {
        let len = self.len;
        // The answer either side alone decides: false for AND, true for OR.
        let decided = !and;
        let Some(left) = self.vector(left, selection)? else { return Ok(None) };
        if !holds_truths(&left.vector) { return Ok(None); }
        let open: Vec<bool> = (0..len).map(|row| reads(selection, row) && truth_at(&left.vector, row) != Some(decided)).collect();
        let right = match self.vector(right, Some(open.as_slice()))? {
            Some(right) if holds_truths(&right.vector) => right,
            _ => return Ok(None),
        };
        let _scratch = self.truth_scratch()?;
        Ok(Some(Truths::Rows((0..len).map(|row| {
            if !reads(selection, row) { return None; }
            let left = truth_at(&left.vector, row);
            if left == Some(decided) { return Some(decided); }
            let right = truth_at(&right.vector, row);
            if right == Some(decided) { Some(decided) } else if left.is_none() || right.is_none() { None } else { Some(!decided) }
        }).collect())))
    }

    /// Integers widened as the scalar functions widen their integer arguments, with the charge for any copy.
    fn integers(&self, vector: &Vector) -> Result<Option<(Int64Array, Option<Arc<MemoryReservation>>)>> {
        Ok(match vector {
            Vector::I64(array) => Some((array.clone(), None)),
            Vector::I32(array) => {
                let array = array.unary(i64::from);
                let charge = self.memory.reserve(array.get_array_memory_size())?;
                Some((array, Some(charge)))
            }
            _ => None,
        })
    }

    /// Numbers as the scalar functions read a numeric argument as a double, with the charge for any copy.
    fn doubles(&self, vector: &Vector) -> Result<Option<(Float64Array, Option<Arc<MemoryReservation>>)>> {
        let array = match vector {
            Vector::F64(array) => return Ok(Some((array.clone(), None))),
            Vector::I64(array) => array.unary(|value| value as f64),
            Vector::I32(array) => array.unary(f64::from),
            _ => return Ok(None),
        };
        let charge = self.memory.reserve(array.get_array_memory_size())?;
        Ok(Some((array, Some(charge))))
    }

    /// Each row takes the value of the first branch whose condition holds, as the evaluator's searched CASE does: each
    /// condition is computed over only the rows no earlier one holds at, and each branch over only the rows that take it.
    fn case(&self, conditions: &[Node], values: &[Node], selection: Selection<'_>) -> Result<Option<Charged>> {
        let len = self.len;
        let _cells = self.memory.reserve(cell_bytes(len)?.checked_add(len).ok_or_else(super::size_error)?)?;
        let mut open: Vec<bool> = (0..len).map(|row| reads(selection, row)).collect();
        let mut chosen = vec![conditions.len(); len];
        for (branch, condition) in conditions.iter().enumerate() {
            if !open.contains(&true) { break; }
            let Some(truths) = self.vector(condition, Some(open.as_slice()))? else { return Ok(None) };
            if !holds_truths(&truths.vector) { return Ok(None); }
            for (row, open) in open.iter_mut().enumerate() {
                if *open && truth_at(&truths.vector, row) == Some(true) {
                    chosen[row] = branch;
                    *open = false;
                }
            }
        }
        let cells: Vec<(usize, usize)> = chosen.iter().copied().zip(0..len).collect();
        let mut computed = Vec::with_capacity(values.len());
        for (branch, value) in values.iter().enumerate() {
            let taken: Vec<bool> = (0..len).map(|row| chosen[row] == branch && reads(selection, row)).collect();
            let Some(value) = self.vector(value, Some(taken.as_slice()))? else { return Ok(None) };
            computed.push(value);
        }
        let mut typed = computed.iter().map(|value| &value.vector).filter(|value| !matches!(value, Vector::Null));
        let Some(first) = typed.next() else { return Ok(Some(Charged::shared(Vector::Null))) };
        let Some(scalar) = first.scalar() else { return Ok(None) };
        if typed.any(|value| value.scalar().as_ref() != Some(&scalar)) { return Ok(None); }
        let Some(template) = first.array() else { return Ok(None) };
        let nulls = new_null_array(template.data_type(), len);
        let _nulls = self.memory.reserve(nulls.get_array_memory_size())?;
        let arrays = computed.iter().map(|value| value.vector.array()).collect::<Vec<_>>();
        let sources: Vec<&dyn Array> = arrays.iter().map(|array| array.as_deref().unwrap_or(nulls.as_ref())).collect();
        let chosen = super::interleave(&sources, &cells)?;
        self.built(Vector::of(chosen.as_ref(), &scalar))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_columnar::MemoryBudget;
    use rtdb_plan::{ColumnId, Field};
    use rtdb_types::Timestamp;

    struct Random(u64);
    impl Random {
        fn next(&mut self) -> u64 { self.0 ^= self.0 << 13; self.0 ^= self.0 >> 7; self.0 ^= self.0 << 17; self.0 }
        fn below(&mut self, bound: u64) -> u64 { self.next() % bound }
        fn pick<T: Clone>(&mut self, items: &[T]) -> T { items[self.below(items.len() as u64) as usize].clone() }
    }

    const UTC: Option<&str> = Some("UTC");
    fn types() -> [ScalarType; 6] {
        [ScalarType::I64, ScalarType::I32, ScalarType::F64, ScalarType::Utf8, ScalarType::Bool,
            ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("Europe/London".into()) }]
    }
    fn field(id: u32) -> Field {
        Field { id: ColumnId(id), name: format!("c{id}"), qualifier: None, qualified_only: false, unnamed: false,
            data_type: LogicalType::Scalar(types()[id as usize].clone()).maybe() }
    }
    fn column(id: u32) -> Expr { field(id).expression() }
    fn call(name: &str, args: Vec<Expr>) -> Expr {
        let return_type = rtdb_plan::function_type_with_args(name, &args).unwrap();
        Expr::Call { name: name.into(), args, return_type }
    }

    /// The kernel column of `expr` over `batch`, `Ok(None)` where the evaluator computes it.
    fn project(expr: &Expr, schema: &Schema, batch: &ColumnBatch, memory: &Arc<MemoryBudget>) -> Result<Option<Column>> {
        super::column(expr, schema, batch, rtdb_expr::Parameters::Values(&[]), memory)
    }

    /// The cells of one column, laid out as the scan's three column shapes lay them out.
    fn shaped(random: &mut Random, array: ArrayRef, scalar: &ScalarType) -> Column {
        let logical = LogicalType::Scalar(scalar.clone()).maybe();
        let field = Arc::new(rtdb_columnar::to_field("c", &logical).unwrap());
        let arrow = Column::Arrow { array: Arc::clone(&array), field: Arc::clone(&field), logical: Some(logical.clone()) };
        match random.below(3) {
            0 => arrow,
            1 => Column::Values((0..array.len()).map(|row| arrow.value(row).unwrap()).collect()),
            _ => {
                // Two pages and an absent one, each row picked from where it lies.
                let len = array.len();
                let split = len / 2;
                let sources = vec![Some((array.slice(0, split), Arc::clone(&field), Some(logical.clone()), None)),
                    None, Some((array.slice(split, len - split), field, Some(logical), None))];
                let cells = (0..len).map(|row| if row % 11 == 5 { (1, 0) } else if row < split { (0, row) } else { (2, row - split) }).collect();
                Column::Selected(SelectedColumn::new(sources, Arc::new(cells)))
            }
        }
    }

    fn batch(random: &mut Random, rows: usize, memory: &Arc<MemoryBudget>) -> ColumnBatch {
        let null = |random: &mut Random| random.below(7) == 0;
        let mut i64s = Vec::new(); let mut i32s = Vec::new(); let mut f64s = Vec::new(); let mut texts = Vec::new(); let mut bools = Vec::new(); let mut times = Vec::new();
        let doubles = [0.0, -0.0, 2.5, -2.5, 1e300, -1e-300, f64::NAN, f64::INFINITY, 1234.5678, 9999.995, -16.0];
        let strings = ["", "a", "closed", "open", "ß", "straße", "name-0000007-éß", "İx", "😀x😀", "line\nbreak", "日本語"];
        for _ in 0..rows {
            i64s.push((!null(random)).then(|| match random.below(8) { 0 => i64::MAX, 1 => i64::MIN, _ => random.below(41) as i64 - 20 }));
            i32s.push((!null(random)).then(|| random.below(21) as i32 - 5));
            f64s.push((!null(random)).then(|| if random.below(2) == 0 { random.pick(&doubles) } else { (random.below(2_000_000) as f64) / 100.0 }));
            texts.push((!null(random)).then(|| random.pick(&strings).to_owned()));
            bools.push((!null(random)).then(|| random.below(2) == 0));
            // Around the 2021 London transitions, and anywhere within a few centuries.
            times.push((!null(random)).then(|| match random.below(3) {
                0 => 1_616_893_200_000_000 + random.below(7_200_000_000) as i64 - 3_600_000_000,
                1 => 1_635_642_000_000_000 + random.below(7_200_000_000) as i64 - 3_600_000_000,
                _ => (random.next() % 20_000_000_000_000_000) as i64 - 10_000_000_000_000_000,
            }));
        }
        let arrays: [ArrayRef; 6] = [Arc::new(Int64Array::from(i64s)), Arc::new(Int32Array::from(i32s)), Arc::new(Float64Array::from(f64s)),
            Arc::new(StringArray::from(texts)), Arc::new(BooleanArray::from(bools)),
            Arc::new(TimestampMicrosecondArray::from(times).with_timezone("Europe/London"))];
        let columns = arrays.into_iter().zip(types()).map(|(array, scalar)| shaped(random, array, &scalar)).collect();
        ColumnBatch::settled(columns, rows, memory.reserve(0).unwrap()).unwrap()
    }

    fn number(random: &mut Random, depth: usize) -> Expr {
        if depth == 0 || random.below(3) == 0 {
            return match random.below(5) {
                0 => column(0), 1 => column(1), 2 => column(2),
                3 => call("extract", vec![Expr::Literal(Value::Utf8(random.pick(&["YEAR", "MONTH", "DAY", "HOUR", "DOW", "WEEK", "TIMEZONE_HOUR", "EPOCH"]).into())), column(5)]),
                _ => call("length", vec![text(random, 0)]),
            };
        }
        let argument = number(random, depth - 1);
        match random.below(7) {
            0 => call(random.pick(&["sqrt", "ln", "exp", "sin", "atan", "floor", "ceil", "log10"]), vec![argument]),
            1 => if random.below(2) == 0 { call("round", vec![argument]) } else { call("round", vec![argument, Expr::Literal(Value::I64(random.below(9) as i64 - 3))]) },
            2 => call("abs", vec![argument]),
            3 => {
                let op = random.pick(&[BinaryOp::Add, BinaryOp::Subtract, BinaryOp::Multiply, BinaryOp::Divide, BinaryOp::Modulo]);
                Expr::binary(op, argument, number(random, depth - 1))
            }
            4 => Expr::Unary { op: rtdb_plan::UnaryOp::Negate, expr: Box::new(argument) },
            5 => match plain_scalar(&argument.data_type()) {
                Some(ScalarType::I64) => Expr::Cast { expr: Box::new(argument), target: CastTarget::Fixed(LogicalType::Scalar(ScalarType::I32)) },
                Some(ScalarType::I32 | ScalarType::F64) => Expr::Cast { expr: Box::new(argument), target: CastTarget::Fixed(LogicalType::Scalar(ScalarType::F64)) },
                _ => argument,
            },
            _ => {
                let ty = argument.data_type();
                let other = if ty == LogicalType::Scalar(ScalarType::F64).maybe() { column(2) } else { argument.clone() };
                Expr::Case { branches: vec![(predicate(random), argument)], otherwise: Box::new(other) }
            }
        }
    }

    fn text(random: &mut Random, depth: usize) -> Expr {
        if depth == 0 || random.below(3) == 0 { return column(3); }
        let argument = text(random, depth - 1);
        match random.below(4) {
            0 => call("upper", vec![argument]),
            1 => call("lower", vec![argument]),
            2 => {
                let mut args = vec![argument, if random.below(4) == 0 { column(1) } else { Expr::Literal(Value::I64(random.below(12) as i64 - 3)) }];
                if random.below(3) != 0 { args.push(Expr::Literal(Value::I64(random.below(6) as i64 - 1))); }
                call("substring", args)
            }
            _ => Expr::Case { branches: vec![(predicate(random), argument)], otherwise: Box::new(Expr::Literal(Value::Utf8("other".into()))) },
        }
    }

    fn predicate(random: &mut Random) -> Expr {
        match random.below(8) {
            0 => column(4),
            1 => Expr::binary(BinaryOp::Eq, column(3), Expr::Literal(Value::Utf8(random.pick(&["closed", "ß", ""]).into()))),
            2 => Expr::binary(BinaryOp::Gt, column(random.below(3) as u32), Expr::Literal(Value::I64(random.below(9) as i64 - 4))),
            3 => Expr::binary(random.pick(&[BinaryOp::And, BinaryOp::Or]), predicate(random), predicate(random)),
            4 => Expr::Unary { op: random.pick(&[UnaryOp::Not, UnaryOp::IsNull, UnaryOp::IsNotNull]), expr: Box::new(predicate(random)) },
            5 => Expr::Between { operand: Box::new(column(random.below(3) as u32)), low: Box::new(Expr::Literal(Value::I64(random.below(9) as i64 - 6))),
                high: Box::new(Expr::Literal(Value::I64(random.below(9) as i64 - 2))), symmetric: false },
            // Fails where c0 is zero or at the edge of its range, so only the rows AND, OR and CASE reach may compute it.
            6 => Expr::binary(BinaryOp::Gt, Expr::binary(BinaryOp::Divide, Expr::Literal(Value::I64(8)), column(0)), Expr::Literal(Value::I64(1))),
            _ => Expr::binary(BinaryOp::Lt, Expr::binary(BinaryOp::Add, column(0), Expr::Literal(Value::I64(1))), Expr::Literal(Value::I64(3))),
        }
    }

    /// The integer CASE that sums matching rows.
    fn tally(random: &mut Random) -> Expr {
        Expr::Case { branches: vec![(predicate(random), Expr::Literal(Value::I64(1)))], otherwise: Box::new(Expr::Literal(Value::I64(random.below(2) as i64))) }
    }

    #[test]
    fn projected_calls_and_cases_agree_with_the_scalar_evaluator_on_every_batch_they_compute() {
        let memory = MemoryBudget::new(1 << 28);
        let schema = Schema::new((0..6).map(field).collect()).unwrap();
        let mut random = Random(0x2545_f491_4f6c_dd1d);
        let (mut computed, mut raised, mut declined) = (0, 0, 0);
        for round in 0..1500 {
            let batch = batch(&mut random, 40, &memory);
            let expr = match round % 3 { 0 => number(&mut random, 3), 1 => text(&mut random, 3), _ => tally(&mut random) };
            let scope = rtdb_expr::EvaluationScope::new(&memory).unwrap();
            let context = rtdb_expr::EvalContext { parameters: rtdb_expr::Parameters::Values(&[]), parameter_types: None, current_time: rtdb_types::Instant::from(Timestamp(0)), timezone: "UTC",
                outer_rows: &[], snapshot_token: None, allocation: Some(&scope), catalog: None };
            let expected: Vec<rtdb_types::Result<Value>> = (0..batch.len).map(|row| {
                let values: Vec<Value> = batch.columns.iter().map(|column| column.value(row).unwrap()).collect();
                rtdb_expr::evaluate(&expr, &schema, &values, &context)
            }).collect();
            let column = match project(&expr, &schema, &batch, &memory) {
                Ok(Some(column)) => column,
                Ok(None) => { declined += 1; continue }
                Err(error) => {
                    // Cells fail in column order rather than row order, but only cells the evaluator also fails on.
                    let failures: Vec<_> = expected.iter().filter_map(|result| result.as_ref().err()).collect();
                    assert!(failures.iter().any(|failure| failure.code == error.code), "{expr:?}: the kernel raised {error:?} where the evaluator raises {failures:?}");
                    raised += 1;
                    continue;
                }
            };
            computed += 1;
            for (row, expected) in expected.into_iter().enumerate() {
                let expected = expected.unwrap_or_else(|error| panic!("the kernel computed {expr:?} where the evaluator raises {error:?}"));
                let actual = column.value(row).unwrap();
                let same = match (&expected, &actual) { (Value::F64(a), Value::F64(b)) => a.to_bits() == b.to_bits(), (a, b) => a == b };
                assert!(same, "{expr:?} at row {row}: evaluator {expected:?}, kernel {actual:?}");
            }
        }
        assert!(computed > 300 && raised > 0, "the kernels should compute many generated expressions: computed {computed}, raised {raised}, declined {declined}");
    }

    #[test]
    fn like_truths_agree_with_the_scalar_evaluator_over_every_column_shape() {
        let memory = MemoryBudget::new(1 << 28);
        let schema = Schema::new((0..6).map(field).collect()).unwrap();
        let mut random = Random(0x0bad_cafe_dead_beef);
        let context = rtdb_expr::EvalContext::default();
        let mut computed = 0;
        for _ in 0..300 {
            let batch = batch(&mut random, 40, &memory);
            let pattern = random.pick(&["%7-é%", "%é%", "na%", "_", "%", "", "%ß", "line%", "%a_e%", "%\\%"]);
            let escape = if random.below(2) == 0 { Some('\\') } else { None };
            let expr = Expr::Like { expr: Box::new(column(3)), pattern: Box::new(Expr::Literal(Value::Utf8(pattern.into()))), escape,
                case_insensitive: random.below(3) == 0, negated: random.below(2) == 0 };
            let expected: Option<Vec<Option<bool>>> = (0..batch.len).map(|row| {
                let values: Vec<Value> = batch.columns.iter().map(|column| column.value(row).unwrap()).collect();
                match rtdb_expr::evaluate(&expr, &schema, &values, &context).ok()? { Value::Null => Some(None), Value::Bool(value) => Some(Some(value)), _ => None }
            }).collect();
            match super::truth(&expr, &schema, &batch, rtdb_expr::Parameters::Values(&[])) {
                Ok(Some(actual)) => {
                    computed += 1;
                    assert_eq!(expected, Some(actual), "{expr:?}");
                }
                Ok(None) => {}
                Err(error) => assert!(expected.is_none(), "{expr:?}: the evaluator answers where the vectors raise {error:?}"),
            }
        }
        assert!(computed > 200, "computed {computed}");
    }

    #[test]
    fn a_kernel_column_is_typed_as_the_evaluators_packed_results() {
        let memory = MemoryBudget::new(1 << 20);
        let field = Field { id: ColumnId(0), name: "ts".into(), qualifier: None, qualified_only: false, unnamed: false,
            data_type: LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: UTC.map(str::to_owned) }) };
        let schema = Schema::new(vec![field.clone()]).unwrap();
        let array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![Some(1_577_836_800_000_000), Some(1_580_515_200_000_000)]).with_timezone("UTC"));
        let column = Column::Arrow { field: Arc::new(ArrowField::new("ts", array.data_type().clone(), false)), array, logical: Some(field.data_type.clone()) };
        let batch = ColumnBatch::settled(vec![column], 2, memory.reserve(0).unwrap()).unwrap();
        let month = call("extract", vec![Expr::Literal(Value::Utf8("MONTH".into())), field.expression()]);
        let Ok(Some(Column::Arrow { array, logical, .. })) = project(&month, &schema, &batch, &memory) else { panic!("EXTRACT over timestamps is a kernel") };
        assert_eq!(logical, Some(LogicalType::Scalar(ScalarType::I32)));
        assert_eq!(array.as_any().downcast_ref::<Int32Array>().unwrap().values().to_vec(), vec![1, 2]);

        let null = call("extract", vec![Expr::Literal(Value::Utf8("MONTH".into())), Expr::Literal(Value::Null)]);
        let Ok(Some(Column::Arrow { logical, .. })) = project(&null, &schema, &batch, &memory) else { panic!("a null argument is a null column") };
        assert_eq!(logical, Some(LogicalType::Null));
    }

    #[test]
    fn kernel_intermediates_are_charged_while_they_live() {
        let rows = 4096;
        let text = "x".repeat(100);
        let schema = Schema::new((0..6).map(field).collect()).unwrap();
        // The literal is copied into every row before the CASE picks between it and the column.
        let expr = call("upper", vec![Expr::Case { branches: vec![(column(4), Expr::Literal(Value::Utf8(text.clone())))], otherwise: Box::new(column(3)) }]);
        let projection = Projection::bind(&expr, &schema).expect("UPPER over a text CASE is a kernel");

        let roomy = MemoryBudget::new(1 << 28);
        let ample = batch(&mut Random(7), rows, &roomy);
        let computed = projection.column(&ample, rtdb_expr::Parameters::Values(&[]), &roomy).unwrap();
        assert!(computed.is_some(), "the kernel computes {expr:?}");
        assert!(roomy.peak_bytes() >= rows * text.len(), "peak {} bytes", roomy.peak_bytes());

        // The batch's own columns are charged to another budget, so only the kernel's vectors meet the limit.
        let tight = MemoryBudget::new(rows * text.len() / 2);
        let limited = batch(&mut Random(7), rows, &roomy);
        let error = projection.column(&limited, rtdb_expr::Parameters::Values(&[]), &tight).err().expect("the literal's copies exceed the budget");
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    }

    #[test]
    fn calls_resolve_to_kernels_once_in_any_spelling() {
        let schema = Schema::new((0..6).map(field).collect()).unwrap();
        let spelled = |name: &str| Expr::Call { name: name.into(), args: vec![column(3)], return_type: LogicalType::Scalar(ScalarType::I32).maybe() };
        assert!(Projection::bind(&spelled("CHARACTER-LENGTH"), &schema).is_some());
        assert!(Projection::bind(&spelled("char_length"), &schema).is_some());
        assert!(Projection::bind(&spelled("reverse"), &schema).is_none());
        assert!(Projection::bind(&column(3), &schema).is_none(), "a bare column is no call");
    }

    #[test]
    fn a_failing_cell_raises_the_evaluators_error_and_an_untaken_branch_cannot_fail() {
        let memory = MemoryBudget::new(1 << 20);
        let schema = Schema::new((0..6).map(field).collect()).unwrap();
        let rows = 3;
        let integers: ArrayRef = Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(4), Some(0)]));
        let flags: ArrayRef = Arc::new(BooleanArray::from(vec![Some(false), Some(true), Some(true)]));
        let mut columns: Vec<Column> = (0..6).map(|id| {
            let logical = LogicalType::Scalar(types()[id].clone()).maybe();
            let field = Arc::new(rtdb_columnar::to_field("c", &logical).unwrap());
            Column::Arrow { array: new_null_array(field.data_type(), rows), field, logical: Some(logical) }
        }).collect();
        for (id, array) in [(0, integers), (4, flags)] {
            let logical = LogicalType::Scalar(types()[id].clone()).maybe();
            columns[id] = Column::Arrow { field: Arc::new(rtdb_columnar::to_field("c", &logical).unwrap()), array, logical: Some(logical) };
        }
        let batch = ColumnBatch::settled(columns, rows, memory.reserve(0).unwrap()).unwrap();
        let context = rtdb_expr::EvalContext::default();
        let evaluated = |expr: &Expr| -> Vec<rtdb_types::Result<Value>> { (0..rows).map(|row| {
            let values: Vec<Value> = batch.columns.iter().map(|column| column.value(row).unwrap()).collect();
            rtdb_expr::evaluate(expr, &schema, &values, &context)
        }).collect() };

        // ABS(i64::MIN) overflows only in the row that doesn't take the branch.
        let guarded = Expr::Case { branches: vec![(column(4), call("abs", vec![column(0)]))], otherwise: Box::new(Expr::Literal(Value::I64(-1))) };
        let Some(Column::Arrow { array, .. }) = project(&guarded, &schema, &batch, &memory).unwrap() else { panic!("the guarded ABS is a kernel") };
        let expected: Vec<Value> = evaluated(&guarded).into_iter().map(|result| result.unwrap()).collect();
        assert_eq!(expected, [Value::I64(-1), Value::I64(4), Value::I64(0)]);
        assert_eq!(array.as_any().downcast_ref::<Int64Array>().unwrap().iter().collect::<Vec<_>>(), [Some(-1), Some(4), Some(0)]);

        // Taken by every row, the branch fails as the evaluator does, rather than leaving the batch to it.
        for failing in [call("abs", vec![column(0)]), Expr::binary(BinaryOp::Divide, Expr::Literal(Value::I64(8)), column(0)), Expr::binary(BinaryOp::Subtract, column(0), Expr::Literal(Value::I64(1)))] {
            let expected = evaluated(&failing).into_iter().find_map(|result| result.err()).expect("some row fails");
            let error = project(&failing, &schema, &batch, &memory).err().unwrap_or_else(|| panic!("{failing:?} raises"));
            assert_eq!(error.code, expected.code, "{failing:?}");
        }
        let divided = Expr::binary(BinaryOp::Divide, Expr::Literal(Value::I64(8)), column(0));
        let guarded = Expr::Case { branches: vec![(Expr::binary(BinaryOp::NotEq, column(0), Expr::Literal(Value::I64(0))), divided)], otherwise: Box::new(Expr::Literal(Value::I64(7))) };
        let Some(Column::Arrow { array, .. }) = project(&guarded, &schema, &batch, &memory).unwrap() else { panic!("the guarded division is a kernel") };
        assert_eq!(array.as_any().downcast_ref::<Int64Array>().unwrap().iter().collect::<Vec<_>>(), [Some(0), Some(2), Some(7)]);
    }

    /// The predicate's truths over `batch`, as the row evaluator gives them.
    fn evaluated_truths(expr: &Expr, schema: &Schema, batch: &ColumnBatch) -> rtdb_types::Result<Vec<Option<bool>>> {
        let context = rtdb_expr::EvalContext::default();
        (0..batch.len).map(|row| {
            let values: Vec<Value> = batch.columns.iter().map(|column| column.value(row).unwrap()).collect();
            Ok(match rtdb_expr::evaluate(expr, schema, &values, &context)? { Value::Bool(value) => Some(value), _ => None })
        }).collect()
    }

    fn integers(values: Vec<Option<i64>>, memory: &Arc<MemoryBudget>) -> (Schema, ColumnBatch) {
        let logical = LogicalType::Scalar(ScalarType::I64).maybe();
        let field = Field { id: ColumnId(0), name: "c0".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: logical.clone() };
        let len = values.len();
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let column = Column::Arrow { field: Arc::new(rtdb_columnar::to_field("c0", &logical).unwrap()), array, logical: Some(logical) };
        (Schema::new(vec![field]).unwrap(), ColumnBatch::settled(vec![column], len, memory.reserve(0).unwrap()).unwrap())
    }

    #[test]
    fn and_or_and_case_conditions_compute_their_later_operands_only_where_the_earlier_leave_the_answer_open() {
        let memory = MemoryBudget::new(1 << 20);
        let (schema, batch) = integers(vec![Some(0), Some(4), None, Some(16)], &memory);
        let x = column(0);
        let nonzero = Expr::binary(BinaryOp::NotEq, x.clone(), Expr::Literal(Value::I64(0)));
        let zero = Expr::binary(BinaryOp::Eq, x.clone(), Expr::Literal(Value::I64(0)));
        let divided = Expr::binary(BinaryOp::Gt, Expr::binary(BinaryOp::Divide, Expr::Literal(Value::I64(8)), x.clone()), Expr::Literal(Value::I64(1)));
        let guarded = [
            Expr::binary(BinaryOp::And, nonzero.clone(), divided.clone()),
            Expr::binary(BinaryOp::Or, zero.clone(), divided.clone()),
            Expr::Unary { op: UnaryOp::Not, expr: Box::new(Expr::binary(BinaryOp::Or, zero.clone(), divided.clone())) },
            Expr::Case { branches: vec![(zero.clone(), Expr::Literal(Value::Bool(false))), (divided.clone(), Expr::Literal(Value::Bool(true)))],
                otherwise: Box::new(Expr::Literal(Value::Bool(false))) },
        ];
        for predicate in guarded {
            let expected = evaluated_truths(&predicate, &schema, &batch).expect("the guard keeps the division from zero");
            let actual = truth(&predicate, &schema, &batch, rtdb_expr::Parameters::Values(&[])).unwrap();
            assert_eq!(actual, Some(expected), "{predicate:?}");
        }

        // Unguarded, the division fails as the row evaluator's does, rather than leaving the batch to it.
        for predicate in [divided.clone(), Expr::binary(BinaryOp::And, divided.clone(), nonzero.clone())] {
            let expected = evaluated_truths(&predicate, &schema, &batch).expect_err("the division by zero is reached");
            let error = truth(&predicate, &schema, &batch, rtdb_expr::Parameters::Values(&[])).expect_err("the vectors raise it too");
            assert_eq!(error.code, expected.code, "{predicate:?}");
        }
    }

    #[test]
    fn a_refused_reservation_raises_rather_than_leaving_the_predicate_to_the_row_evaluator() {
        let memory = MemoryBudget::new(1 << 20);
        let (schema, batch) = integers(vec![Some(2), None], &memory);
        let predicate = Expr::binary(BinaryOp::Gt, Expr::binary(BinaryOp::Add, column(0), Expr::Literal(Value::I64(3))), Expr::Literal(Value::I64(4)));
        let baseline = memory.used_bytes();
        assert_eq!(truth(&predicate, &schema, &batch, rtdb_expr::Parameters::Values(&[])).unwrap(), Some(vec![Some(true), None]));
        assert_eq!(memory.used_bytes(), baseline, "the computed operand's charge ends with it");
        let full = memory.reserve(memory.limit_bytes() - baseline).unwrap();
        let error = truth(&predicate, &schema, &batch, rtdb_expr::Parameters::Values(&[])).expect_err("the budget has no room for the sum");
        assert!(error.is(rtdb_types::AnomalyKind::ResourceExhausted), "{error:?}");
        drop(full);
        assert_eq!(memory.used_bytes(), baseline);
    }

    /// Commit 56d3e368fa: reading a parameter with `known` declined every Arrow-bound point read to the row evaluator.
    #[test]
    fn an_arrow_bound_parameter_is_decoded_rather_than_declined() {
        struct Undecoded(Value);
        impl rtdb_expr::ParameterLookup for Undecoded {
            fn width(&self) -> usize { 1 }
            fn value(&self, index: usize) -> rtdb_types::Result<Option<&Value>> { Ok((index == 0).then_some(&self.0)) }
            fn known_value(&self, _: usize) -> Option<&Value> { None }
            fn arrow_cell(&self, _: usize) -> Option<rtdb_expr::ArrowParameterCell<'_>> { None }
        }
        let memory = MemoryBudget::new(1 << 20);
        let lookup = Undecoded(Value::I64(2));
        let parameters = rtdb_expr::Parameters::Lookup(&lookup);
        let (schema, batch) = integers(vec![Some(1), Some(2)], &memory);
        let parameter = Expr::Parameter { index: 0, data_type: LogicalType::Scalar(ScalarType::I64).maybe() };
        let predicate = Expr::binary(BinaryOp::Eq, column(0), parameter.clone());
        assert_eq!(truth(&predicate, &schema, &batch, parameters).unwrap(), Some(vec![Some(false), Some(true)]));
        let sum = Expr::binary(BinaryOp::Add, column(0), parameter);
        let Some(Column::Arrow { array, .. }) = super::column(&sum, &schema, &batch, parameters, &memory).unwrap() else { panic!("the sum over a bound parameter is a kernel") };
        assert_eq!(array.as_any().downcast_ref::<Int64Array>().unwrap().iter().collect::<Vec<_>>(), [Some(3), Some(4)]);
    }

    #[test]
    fn an_invalid_like_pattern_raises_where_an_input_is_present() {
        let memory = MemoryBudget::new(1 << 20);
        let schema = Schema::new((0..6).map(field).collect()).unwrap();
        let rows = 2;
        let columns: Vec<Column> = (0..6).map(|id| {
            let logical = LogicalType::Scalar(types()[id].clone()).maybe();
            let field = Arc::new(rtdb_columnar::to_field("c", &logical).unwrap());
            let array = if id == 3 { Arc::new(StringArray::from(vec![Some("x"), None])) as ArrayRef } else { new_null_array(field.data_type(), rows) };
            Column::Arrow { array, field, logical: Some(logical) }
        }).collect();
        let batch = ColumnBatch::settled(columns, rows, memory.reserve(0).unwrap()).unwrap();
        let like = Expr::Like { expr: Box::new(column(3)), pattern: Box::new(Expr::Literal(Value::Utf8("x\\".into()))), escape: Some('\\'),
            case_insensitive: false, negated: false };
        let expected = evaluated_truths(&like, &schema, &batch).expect_err("a pattern ending in its escape is invalid");
        let error = truth(&like, &schema, &batch, rtdb_expr::Parameters::Values(&[])).expect_err("the vectors raise it too");
        assert_eq!(error.code, expected.code);
    }
}
