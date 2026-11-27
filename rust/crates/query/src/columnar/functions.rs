//! Column-at-a-time evaluation of scalar expressions over plain scalar cells: calls, arithmetic, negation, numeric
//! casts and searched CASE through `rtdb_expr::kernels`, which share the scalar functions' per-value code, and
//! predicates through the predicate kernel.
//! A failing cell raises the row evaluator's error for that cell. Each CASE branch is computed only over the rows that
//! take it, so a cell fails only where the row evaluator would evaluate it.
//! `Ok(None)` leaves a batch to the row evaluator where a layout, a cell type or a result is outside these vectors.
//! Every vector built along the way is charged to the query's memory budget while it lives.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, NullArray, StringArray, TimestampMicrosecondArray, new_null_array};
use arrow_schema::{DataType, Field as ArrowField, TimeUnit as ArrowUnit};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_expr::kernels::{self, Function, Selection};
use rtdb_plan::{BinaryOp, CastTarget, Expr, Schema, UnaryOp};
use rtdb_types::{LogicalType, Result, ScalarType, TimeUnit, Value};

use super::{Column, ColumnBatch, SelectedColumn, plain_scalar};

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

/// The scalar a stored layout's cells read as, `None` within `Some` for a layout of nulls only, as the predicate
/// kernel's cell reader takes it.
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
    let Ok(gathered) = super::interleave(&arrays, &cells) else { return Ok(None) };
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
    /// A predicate, whose truths the predicate kernel computes.
    Truth(Expr),
    /// The values of the branches, then the otherwise value.
    Case { conditions: Vec<Expr>, values: Vec<Node> },
}

impl Kind {
    /// The arguments of a strict node, which is null wherever one of them is, in the order the row evaluator prepares
    /// them; `None` for a leaf, a predicate or a CASE.
    fn arguments(&self) -> Option<Vec<&Node>> {
        Some(match self {
            Self::Column(_) | Self::Literal(_) | Self::Parameter(_) | Self::Truth(_) | Self::Case { .. } => return None,
            Self::Extract { source: argument, .. } | Self::Round { value: argument, .. } | Self::Abs(argument) | Self::TextCase { text: argument, .. }
                | Self::Length(argument) | Self::Double(_, argument) | Self::Negate(argument) | Self::Cast(argument) => vec![argument.as_ref()],
            Self::Substring { text, position, length } => [Some(text), Some(position), length.as_ref()].into_iter().flatten().map(|argument| argument.as_ref()).collect(),
            Self::Arithmetic { left, right, .. } => vec![left.as_ref(), right.as_ref()],
        })
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
    pub(crate) fn column(&self, schema: &Schema, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>, memory: &Arc<MemoryBudget>) -> Result<Option<Column>> {
        self.evaluated(&Evaluation { schema, columns: &batch.columns, len: batch.len, batch: Some(batch), parameters: Parameters::Values(parameters), memory })
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
        Some(projection) => projection.column(schema, batch, parameters, memory),
        None => Ok(None),
    }
}

/// The one-row column of `expr` over `columns`, reading its parameters from the one-row `arguments`, charged to
/// `memory` for as long as it lives.
pub(crate) fn bound_column(expr: &Expr, schema: &Schema, columns: &[Column], arguments: &[Column], memory: &Arc<MemoryBudget>) -> Result<Option<Column>> {
    if !bound_lowerable(expr) { return Ok(None); }
    let Some(projection) = Projection::bind(expr, schema) else { return Ok(None) };
    let evaluation = Evaluation { schema, columns, len: 1, batch: None, parameters: Parameters::Columns(arguments), memory };
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
        LogicalType::Maybe(inner) => plain_leaf(inner),
        LogicalType::Scalar(scalar) => matches!(scalar, ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64
            | ScalarType::F32 | ScalarType::F64 | ScalarType::Utf8 | ScalarType::Bool),
        _ => false,
    }
}

/// Whether the shape of `expr` could be computed here; its inputs' layouts decide per batch.
pub(crate) fn lowerable(expr: &Expr) -> bool { lowerable_at(expr, None) }

/// As `lowerable`, for an expression over parameters bound one row at a time, to a bounded depth.
pub(crate) fn bound_lowerable(expr: &Expr) -> bool { lowerable_at(expr, Some(32)) }

fn lowerable_at(expr: &Expr, remaining: Option<usize>) -> bool {
    if remaining == Some(0) { return false; }
    let lower = |expr: &Expr| lowerable_at(expr, remaining.map(|depth| depth - 1));
    match expr {
        Expr::Parameter { .. } if remaining.is_some() => true,
        Expr::Column { .. } | Expr::Parameter { .. } | Expr::Literal(_) => plain_leaf(&expr.data_type()),
        Expr::Binary { op: BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide | BinaryOp::Modulo | BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::Lt | BinaryOp::LtEq
            | BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::And | BinaryOp::Or, left, right } => lower(left) && lower(right),
        Expr::Unary { op: UnaryOp::Not | UnaryOp::IsNull | UnaryOp::IsNotNull | UnaryOp::Negate, expr } => lower(expr),
        Expr::Cast { expr, target: CastTarget::Fixed(_) } => lower(expr),
        Expr::Case { branches, otherwise } => branches.iter().all(|(condition, value)| lower(condition) && lower(value)) && lower(otherwise),
        Expr::Call { name, args, .. } => match (Function::resolve(name), args.as_slice()) {
            (Some(Function::Upper | Function::Lower), [arg]) if remaining.is_some() => {
                matches!(arg, Expr::Column { .. } | Expr::Parameter { .. } | Expr::Literal(_)) && lower(arg)
            }
            (Some(Function::Upper | Function::Lower | Function::Length | Function::Abs), [arg]) => lower(arg),
            (Some(Function::Mod), [left, right]) => lower(left) && lower(right),
            _ => false,
        },
        _ => false,
    }
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
            Expr::Call { name, args, .. } if name == "===" && args.len() == 2 => Kind::Truth(expr.clone()),
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
            Expr::Unary { op: UnaryOp::Negate, expr: operand } => Kind::Negate(child(operand)?),
            Expr::Cast { expr: operand, target: CastTarget::Fixed(_) } => Kind::Cast(child(operand)?),
            Expr::Case { branches, otherwise } => Kind::Case {
                conditions: branches.iter().map(|(condition, _)| condition.clone()).collect(),
                values: branches.iter().map(|(_, value)| value).chain(std::iter::once(otherwise.as_ref()))
                    .map(|value| Self::bind(value, schema)).collect::<Option<_>>()?,
            },
            expr if plain_scalar(&expr.data_type()) == Some(&ScalarType::Bool) => Kind::Truth(expr.clone()),
            _ => return None,
        };
        Some(Self { kind, declared: expr.data_type() })
    }
}

/// Where parameters are read from: values known for the whole batch, or one-row columns bound with the query.
#[derive(Clone, Copy)]
enum Parameters<'a> { Values(rtdb_expr::Parameters<'a>), Columns(&'a [Column]) }

struct Evaluation<'a> {
    schema: &'a Schema,
    columns: &'a [Column],
    len: usize,
    /// The batch the predicate kernel reads, `None` for parameters bound one row at a time.
    batch: Option<&'a ColumnBatch>,
    parameters: Parameters<'a>,
    memory: &'a Arc<MemoryBudget>,
}

/// A strict node's arguments once prepared: null at every row, or computed with the rows none is null at, `None` for
/// every row.
enum Prepared { Null, Live { rows: Option<Vec<bool>>, arguments: Vec<Charged> } }

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

    fn vector_at(&self, node: &Node, selection: Selection<'_>) -> Result<Option<Charged>> {
        let len = self.len;
        let null = || -> Result<Option<Charged>> { Ok(Some(Charged::shared(Vector::Null))) };
        // A strict node's arguments are prepared, in order, before any payload is read.
        let live;
        let (selection, arguments) = match node.kind.arguments() {
            None => (selection, Vec::new()),
            Some(arguments) => match self.prepared(&arguments, selection)? {
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
                Parameters::Values(parameters) => match parameters.known(*index) {
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
            Kind::Truth(predicate) => {
                // The predicate kernel raises nothing, so its truths can be computed for every row.
                let truth = match (self.batch, self.parameters) {
                    (Some(batch), Parameters::Values(parameters)) => super::kernel::truth(predicate, self.schema, batch, parameters),
                    (None, Parameters::Columns(arguments)) => self.bound_truth(predicate, arguments)?,
                    _ => None,
                };
                let Some(truth) = truth else { return Ok(None) };
                self.built(Some(Vector::Bool(BooleanArray::from(truth))))?
            }
            Kind::Case { conditions, values } => self.case(conditions, values, selection)?,
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
    /// Where an argument is null at every row, the single-typed ones before it are left unread but for what preparing
    /// them demands.
    fn prepared(&self, arguments: &[&Node], selection: Selection<'_>) -> Result<Option<Prepared>> {
        let len = self.len;
        let mut rows: Option<Vec<bool>> = selection.map(<[bool]>::to_vec);
        let mut computed: Vec<Option<Charged>> = Vec::with_capacity(arguments.len());
        let null = |computed: &[Option<Charged>]| -> Option<Prepared> {
            let deferred = arguments.iter().zip(computed).filter(|(_, value)| value.is_none());
            deferred.into_iter().all(|(argument, _)| self.settles(argument)).then_some(Prepared::Null)
        };
        for argument in arguments {
            if argument.declared == LogicalType::Null {
                if null(&computed).is_none() || self.vector(argument, rows.as_deref())?.is_none() { return Ok(None); }
                return Ok(Some(Prepared::Null));
            }
            if argument.declared.legs().len() < 2 { computed.push(None); continue; }
            let Some(value) = self.vector(argument, rows.as_deref())? else { return Ok(None) };
            let Some(array) = value.vector.array() else { return Ok(null(&computed)) };
            if array.null_count() != 0 {
                let live: Vec<bool> = (0..len).map(|row| array.is_valid(row) && rows.as_ref().is_none_or(|rows| rows[row])).collect();
                if !live.contains(&true) { return Ok(null(&computed)); }
                rows = Some(live);
            }
            computed.push(Some(value));
        }
        let mut prepared = Vec::with_capacity(arguments.len());
        for (argument, value) in arguments.iter().zip(computed) {
            let value = match value {
                Some(value) => value,
                None => match self.vector(argument, rows.as_deref())? { Some(value) => value, None => return Ok(None) },
            };
            prepared.push(value);
        }
        Ok(Some(Prepared::Live { rows, arguments: prepared }))
    }

    /// Whether preparing `node`, of a single type other than null, demands only what the predicate kernel computes
    /// without raising: the conditions of each CASE and the operands of each predicate in it.
    fn settles(&self, node: &Node) -> bool {
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || match &node.kind {
            Kind::Column(_) | Kind::Literal(_) | Kind::Parameter(_) => true,
            Kind::Truth(predicate) => match (self.batch, self.parameters) {
                (Some(batch), Parameters::Values(parameters)) => super::kernel::truth(predicate, self.schema, batch, parameters).is_some(),
                _ => false,
            },
            Kind::Case { conditions, values } => matches!(self.branches(conditions), Ok(Some(_))) && values.iter().all(|value| self.settles(value)),
            kind => kind.arguments().is_some_and(|arguments| arguments.into_iter().all(|argument| self.settles(argument))),
        })
    }

    /// For each row, the branch it takes and the row: the first branch whose condition holds, else the otherwise
    /// value at `conditions.len()`; `Ok(None)` where the predicate kernel leaves a condition to the row evaluator.
    fn branches(&self, conditions: &[Expr]) -> Result<Option<Vec<(usize, usize)>>> {
        let (Some(batch), Parameters::Values(parameters)) = (self.batch, self.parameters) else { return Ok(None) };
        let len = self.len;
        let truth_bytes = conditions.len().checked_mul(len).and_then(|cells| cells.checked_mul(size_of::<Option<bool>>())).ok_or_else(super::size_error)?;
        let _truths = self.memory.reserve(truth_bytes)?;
        let mut truths = Vec::with_capacity(conditions.len());
        for condition in conditions {
            let Some(truth) = super::kernel::truth(condition, self.schema, batch, parameters) else { return Ok(None) };
            truths.push(truth);
        }
        Ok(Some((0..len).map(|row| (truths.iter().position(|truth| truth[row] == Some(true)).unwrap_or(conditions.len()), row)).collect()))
    }

    /// The predicate kernel's truths of `predicate` over one row bound with the query: the parameters it reads are
    /// that row's values, charged while they are read.
    fn bound_truth(&self, predicate: &Expr, arguments: &[Column]) -> Result<Option<Vec<Option<bool>>>> {
        let mut read = vec![false; arguments.len()];
        let mut missing = false;
        predicate.visit(&mut |expr| if let Expr::Parameter { index, .. } = expr {
            match read.get_mut(*index) { Some(read) => *read = true, None => missing = true }
        });
        if missing { return Ok(None); }
        let mut values = Vec::with_capacity(arguments.len());
        let mut bytes = arguments.len().checked_mul(size_of::<Value>()).ok_or_else(super::size_error)?;
        for (argument, read) in arguments.iter().zip(read) {
            let value = if read { argument.value(0)? } else { Value::Null };
            bytes = bytes.checked_add(value.retained_bytes()?).ok_or_else(super::size_error)?;
            values.push(value);
        }
        let _values = self.memory.reserve(bytes)?;
        let batch = ColumnBatch::settled(self.columns.iter().map(Column::duplicate).collect(), self.len, self.memory.reserve(0)?)?;
        Ok(super::kernel::truth(predicate, self.schema, &batch, rtdb_expr::Parameters::Values(&values)))
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

    /// Each row takes the value of the first branch whose condition holds, as the evaluator's searched CASE does,
    /// and each branch is computed over only the rows that take it.
    fn case(&self, conditions: &[Expr], values: &[Node], selection: Selection<'_>) -> Result<Option<Charged>> {
        let len = self.len;
        let _cells = self.memory.reserve(cell_bytes(len)?.checked_add(len).ok_or_else(super::size_error)?)?;
        let Some(cells) = self.branches(conditions)? else { return Ok(None) };
        let mut computed = Vec::with_capacity(values.len());
        for (branch, value) in values.iter().enumerate() {
            let taken: Vec<bool> = cells.iter().map(|(chosen, row)| *chosen == branch && selection.is_none_or(|rows| rows[*row])).collect();
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
        let Ok(chosen) = super::interleave(&sources, &cells) else { return Ok(None) };
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
        match random.below(3) {
            0 => column(4),
            1 => Expr::binary(BinaryOp::Eq, column(3), Expr::Literal(Value::Utf8(random.pick(&["closed", "ß", ""]).into()))),
            _ => Expr::binary(BinaryOp::Gt, column(random.below(3) as u32), Expr::Literal(Value::I64(random.below(9) as i64 - 4))),
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
            let actual = super::super::kernel::truth(&expr, &schema, &batch, rtdb_expr::Parameters::Values(&[]));
            if let Some(actual) = actual {
                computed += 1;
                assert_eq!(expected, Some(actual), "{expr:?}");
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
        let computed = projection.column(&schema, &ample, rtdb_expr::Parameters::Values(&[]), &roomy).unwrap();
        assert!(computed.is_some(), "the kernel computes {expr:?}");
        assert!(roomy.peak_bytes() >= rows * text.len(), "peak {} bytes", roomy.peak_bytes());

        // The batch's own columns are charged to another budget, so only the kernel's vectors meet the limit.
        let tight = MemoryBudget::new(rows * text.len() / 2);
        let limited = batch(&mut Random(7), rows, &roomy);
        let error = projection.column(&schema, &limited, rtdb_expr::Parameters::Values(&[]), &tight).err().expect("the literal's copies exceed the budget");
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
}
