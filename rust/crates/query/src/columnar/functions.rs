//! Column-at-a-time evaluation of scalar expressions over plain scalar cells: calls, arithmetic, negation, numeric
//! casts, searched CASE and predicates (comparisons, `===`, LIKE, BETWEEN, IS NULL, NOT, AND and OR), the scalar
//! functions through `rtdb_expr::kernels`, which share their per-value code.
//! Shape, schema type, comparison compatibility and strict-argument settling are decided at bind/plan time in
//! `Projection::bind` and `Predicate::bind`, so unsupported expressions go to the row evaluator by plan rather than
//! per-batch trial.
//! A failing cell raises the row evaluator's error for that cell. Each CASE branch and each condition after the first
//! is computed only over the rows that reach it, and the right side of AND and OR only over the rows the left side
//! leaves open, so a cell fails only where the row evaluator would evaluate it.
//! Every vector built along the way is charged to the query's memory budget while it lives.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, NullArray, PrimitiveArray, StringArray, StructArray,
    TimestampMicrosecondArray, make_array, builder::{BooleanBuilder, PrimitiveBuilder}, new_null_array, types::{ArrowPrimitiveType, Float64Type, Int32Type}};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field as ArrowField, TimeUnit as ArrowUnit};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_expr::kernels::{self, Function, LikeMatcher, Selection};
use rtdb_plan::{BinaryOp, CastTarget, Expr, Schema, UnaryOp};
use rtdb_types::{LogicalType, Result, ScalarType, TimeUnit, Value};

use super::{Column, ColumnBatch, PredicateSelection, SelectedColumn, StoredVector, float_vector::nonnull, plain_scalar};

mod predicate;
mod range;
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
        Column::Selected(selected) => read_selected(selected, expected, &[], memory)?,
        Column::Values(values) => read_values(values, expected, len).map(|vector| Charged::built(vector, memory)).transpose()?,
    })
}

/// Gathers a selection whose sources all read as `expected` or as nulls into one vector.
fn read_selected(
    selected: &SelectedColumn, expected: &ScalarType, parent_nulls: &[Option<&NullBuffer>], memory: &Arc<MemoryBudget>,
) -> Result<Option<Charged>> {
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
    let cells: Vec<(usize, usize)> = selected.cells.iter().map(|(source, row)| {
        if readable[source] && parent_nulls.get(source).and_then(|nulls| *nulls).is_none_or(|nulls| nulls.is_valid(row)) {
            (source, row)
        } else { (null, 0) }
    }).collect();
    let gathered = super::interleave(&arrays, &cells)?;
    Vector::of(gathered.as_ref(), expected).map(|vector| Charged::built(vector, memory)).transpose()
}

fn struct_child<'a>(array: &'a dyn Array, field: &ArrowField, logical: Option<&LogicalType>, name: &str)
    -> Option<(&'a StructArray, &'a ArrayRef, &'a Arc<ArrowField>)>
{
    if !field.metadata().is_empty() || field.data_type() != array.data_type() { return None; }
    let LogicalType::Struct(types) = nonnull(logical?) else { return None; };
    let parent = array.as_any().downcast_ref::<StructArray>()?;
    let position = parent.fields().iter().position(|child| child.name() == name)?;
    let child = &parent.fields()[position];
    let physical = rtdb_columnar::from_field(child).ok()?;
    (&physical == types.get(name)?).then(|| (parent, parent.column(position), child))
}

fn read_struct_field(column: &Column, name: &str, declared: &LogicalType, memory: &Arc<MemoryBudget>) -> Result<Option<Charged>> {
    let Some(expected) = plain_scalar(declared) else { return Ok(None); };
    match column {
        Column::Arrow { array, field, logical } => {
            if array.data_type() == &DataType::Null && field.is_nullable() && logical.as_ref() == Some(&LogicalType::Null) {
                return Ok(Some(Charged::shared(Vector::Null)));
            }
            let Some((parent, child, field)) = struct_child(array.as_ref(), field, logical.as_ref(), name) else { return Ok(None); };
            match layout(child.as_ref(), field, None) {
                Some(None) => Ok(Some(Charged::shared(Vector::Null))),
                Some(Some(ty)) if ty == *expected => {
                    if parent.null_count() == 0 {
                        return Ok(Vector::of(child.as_ref(), expected).map(Charged::shared));
                    }
                    let bytes = child.len().div_ceil(8).checked_add(512).ok_or_else(super::size_error)?;
                    let owner = memory.reserve(bytes)?;
                    let nulls = NullBuffer::union(parent.nulls(), child.nulls());
                    let Ok(data) = child.to_data().into_builder().nulls(nulls).build() else { return Ok(None); };
                    Ok(Vector::of(make_array(data).as_ref(), expected).map(|vector| Charged { vector, _charge: Some(owner) }))
                }
                _ => Ok(None),
            }
        }
        Column::Selected(parent) => {
            let bytes = parent.sources.len().checked_mul(size_of::<Option<StoredVector>>() + size_of::<Option<&NullBuffer>>())
                .and_then(|bytes| bytes.checked_add(512)).ok_or_else(super::size_error)?;
            let _sources = memory.reserve(bytes)?;
            let mut sources = Vec::with_capacity(parent.sources.len());
            let mut parent_nulls = Vec::with_capacity(parent.sources.len());
            for source in parent.sources.iter() {
                match source {
                    Some((array, field, logical, _)) if !(array.data_type() == &DataType::Null && field.is_nullable()
                        && logical.as_ref() == Some(&LogicalType::Null)) => {
                        let Some((parent, child, field)) = struct_child(array.as_ref(), field, logical.as_ref(), name) else { return Ok(None); };
                        sources.push(Some((Arc::clone(child), Arc::clone(field), None, None)));
                        parent_nulls.push(parent.nulls());
                    }
                    _ => {
                        sources.push(None);
                        parent_nulls.push(None);
                    }
                }
            }
            let selected = SelectedColumn { sources: Arc::new(sources), cells: parent.cells.clone() };
            read_selected(&selected, expected, &parent_nulls, memory)
        }
        Column::Values(_) => Ok(None),
    }
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

/// A projected expression, its calls and schema layout checks resolved once at plan/preparation time.
#[derive(Clone, Debug)]
pub(crate) struct Projection(Node);

/// A predicate expression, its calls and schema layout checks resolved once at plan/preparation time.
#[derive(Clone, Debug)]
pub(crate) struct Predicate(PredicatePlan);

#[derive(Clone, Debug)]
enum PredicatePlan {
    Range(range::RangePredicate),
    Vector(Node),
}

/// The static vector type of a bound node, decided from the schema and expression types at bind time.
#[derive(Clone, Debug, PartialEq, Eq)]
enum VectorType {
    Null,
    I32,
    I64,
    F64,
    Text,
    Bool,
    Micros(Option<String>),
}

impl VectorType {
    fn of(logical: &LogicalType) -> Option<Self> {
        if *logical == LogicalType::Null { return Some(Self::Null); }
        Some(match plain_scalar(logical)? {
            ScalarType::I32 => Self::I32,
            ScalarType::I64 => Self::I64,
            ScalarType::F64 => Self::F64,
            ScalarType::Utf8 => Self::Text,
            ScalarType::Bool => Self::Bool,
            ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone } => Self::Micros(timezone.clone()),
            _ => return None,
        })
    }

    fn of_value(value: &Value) -> Option<Self> {
        Some(match value {
            Value::Null => Self::Null,
            Value::I32(_) => Self::I32,
            Value::I64(_) => Self::I64,
            Value::F64(_) => Self::F64,
            Value::Utf8(_) => Self::Text,
            Value::Bool(_) => Self::Bool,
            Value::Timestamp { unit: TimeUnit::Microsecond, timezone, .. } => Self::Micros(timezone.clone()),
            _ => return None,
        })
    }

    fn cell_kinds(self) -> Vec<predicate::CellKind> {
        match self {
            Self::Null => Vec::new(),
            Self::I32 | Self::I64 => vec![predicate::CellKind::Int],
            Self::F64 => vec![predicate::CellKind::Float],
            Self::Text => vec![predicate::CellKind::Text],
            Self::Bool => vec![predicate::CellKind::Bool],
            Self::Micros(zone) => vec![predicate::CellKind::Micros(zone)],
        }
    }
}

#[derive(Clone, Debug)]
struct Node { kind: Kind, declared: LogicalType }

#[derive(Clone, Debug)]
enum Kind {
    /// The batch column at this position.
    Column(usize),
    StructField { position: usize, name: String },
    Literal(Value),
    Parameter(usize),
    Extract { field: String, source: Box<Node> },
    /// `scale` is `None` for the one-argument form, which leaves an integer as it is.
    Round { value: Box<Node>, scale: Option<i64> },
    RoundFloatColumn { position: usize, scale: i64 },
    Abs(Box<Node>),
    TextCase { text: Box<Node>, upper: bool },
    Length { operand: Box<Node>, octets: bool },
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
    Compare { op: BinaryOp, left: Box<Node>, right: Box<Node>, allow_half: bool },
    /// `===`, which reads both sides at every row and holds of two nulls.
    Identical { left: Box<Node>, right: Box<Node>, allow_half: bool },
    /// IS NULL or IS UNKNOWN, or IS NOT NULL where `negated`.
    IsNull { operand: Box<Node>, negated: bool },
    /// LIKE over a literal or bound pattern.
    Like {
        input: Box<Node>, pattern: Box<Node>, escape: Option<char>, case_insensitive: bool, negated: bool,
        compiled: Option<Arc<LikeMatcher>>,
    },
    /// BETWEEN, not symmetric.
    Between { operand: Box<Node>, low: Box<Node>, high: Box<Node> },
    /// AND, or OR where not `and`.
    Logical { and: bool, left: Box<Node>, right: Box<Node> },
    /// The conditions of the branches; the values of the branches, then the otherwise value.
    Case { conditions: Vec<Node>, values: Vec<Node> },
}

impl Kind {
    /// Strict arguments prepared as vectors, in the order the row evaluator prepares them.
    fn arguments(&self) -> Option<Vec<&Node>> {
        Some(match self {
            Self::Column(_) | Self::StructField { .. } | Self::Literal(_) | Self::Parameter(_) | Self::Case { .. } | Self::Length { .. } | Self::Double(..)
                | Self::RoundFloatColumn { .. }
                | Self::Compare { .. } | Self::Identical { .. }
                | Self::IsNull { .. } | Self::Like { .. } | Self::Between { .. } | Self::Logical { .. } => return None,
            Self::Extract { source: argument, .. } | Self::Round { value: argument, .. } | Self::Abs(argument) | Self::TextCase { text: argument, .. }
                | Self::Negate(argument) | Self::Cast(argument) | Self::Not(argument) => vec![argument.as_ref()],
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
    /// `None` for an expression whose shape or schema types cannot be computed here; a bare column, literal or
    /// parameter is copied elsewhere.
    pub(crate) fn bind(expr: &Expr, schema: &Schema) -> Option<Self> {
        if matches!(expr, Expr::Column { .. } | Expr::Literal(_) | Expr::Parameter { .. }) { return None; }
        Node::bind_root(expr, schema, None).map(|(node, _)| Self(node))
            .or_else(|| Self::bind_double_column_cast(expr, schema))
    }

    fn bind_double_column_cast(expr: &Expr, schema: &Schema) -> Option<Self> {
        let Expr::Cast { expr: input, target: CastTarget::Fixed(LogicalType::Scalar(ScalarType::F64)) } = expr else { return None; };
        if !matches!(input.as_ref(), Expr::Column { .. }) { return None; }
        let declared = expr.data_type();
        if VectorType::of(&declared)? != VectorType::F64 { return None; }
        let (operand, kinds) = Node::bind_operand(input, schema, None)?;
        if kinds.is_empty() || !kinds.iter().all(|kind| matches!(kind, predicate::CellKind::Int | predicate::CellKind::Float)) {
            return None;
        }
        let kind = Kind::Double(|value| value, Box::new(operand));
        Some(Self(Node { kind, declared }))
    }

    fn bind_with_parameters(expr: &Expr, schema: &Schema, parameters: &[Option<LogicalType>]) -> Option<Self> {
        if matches!(expr, Expr::Column { .. } | Expr::Literal(_) | Expr::Parameter { .. }) { return None; }
        Node::bind_root(expr, schema, Some(parameters)).map(|(node, _)| Self(node))
    }

    pub(crate) fn retained_bytes(&self) -> Result<usize> { self.0.retained_bytes() }

    /// The column over `batch`, typed as the evaluator's packed results are, or `Ok(None)` where the evaluator must
    /// compute it.
    pub(crate) fn column(&self, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>, memory: &Arc<MemoryBudget>) -> Result<Option<Column>> {
        self.evaluated(&Evaluation { columns: &batch.columns, len: batch.len, parameters: Parameters::Values(parameters), memory })
    }

    fn evaluated(&self, evaluation: &Evaluation<'_>) -> Result<Option<Column>> {
        let Some(charged) = evaluation.vector(&self.0, None)? else { return Ok(None) };
        Self::observed(&charged.vector, evaluation.len)
    }

    fn observed(vector: &Vector, len: usize) -> Result<Option<Column>> {
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

impl Predicate {
    /// `None` where the predicate's shape or schema types cannot be evaluated here.
    pub(crate) fn bind(expr: &Expr, schema: &Schema) -> Option<Self> {
        if let Some(range) = range::RangePredicate::bind(expr, schema) {
            return Some(Self(PredicatePlan::Range(range)));
        }
        let (node, ty) = Node::bind_root(expr, schema, None)?;
        matches!(ty, VectorType::Bool | VectorType::Null).then_some(Self(PredicatePlan::Vector(node)))
    }

    pub(crate) fn retained_bytes(&self) -> Result<usize> {
        match &self.0 {
            PredicatePlan::Range(_) => Ok(size_of::<range::RangePredicate>()),
            PredicatePlan::Vector(node) => node.retained_bytes(),
        }
    }

    pub(crate) fn selection(&self, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>, memory: &Arc<MemoryBudget>,
        prepare: impl FnOnce() -> Result<()>) -> Result<Option<PredicateSelection>> {
        let node = match &self.0 {
            PredicatePlan::Range(range) => {
                let selection = range.selection(batch, parameters, memory)?;
                if selection.is_some() { prepare()?; }
                return Ok(selection);
            }
            PredicatePlan::Vector(node) => node,
        };
        let evaluation = Evaluation { columns: &batch.columns, len: batch.len, parameters: Parameters::Values(parameters), memory: batch.reservation.budget() };
        let Some(Charged { vector, _charge: charge }) = evaluation.vector(node, None)? else { return Ok(None); };
        if !holds_truths(&vector) { return Ok(None); }
        prepare()?;
        Ok(Some(match (vector, charge) {
            (Vector::Null, _) => PredicateSelection::from_bits(std::iter::repeat_n(false, batch.len), memory)?,
            (Vector::Bool(values), Some(charge)) => PredicateSelection::from_arrow(values, charge)?,
            (Vector::Bool(values), None) => PredicateSelection::from_bits((0..batch.len).map(|row| {
                values.is_valid(row) && values.value(row)
            }), memory)?,
            _ => return Ok(None),
        }))
    }

    /// Each row's three-valued truth over `batch`, keeping unknown (null) apart from false.
    pub(crate) fn truth(&self, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>) -> Result<Option<Vec<Option<bool>>>> {
        let node = match &self.0 {
            PredicatePlan::Range(range) => return range.truth(batch, parameters),
            PredicatePlan::Vector(node) => node,
        };
        let evaluation = Evaluation { columns: &batch.columns, len: batch.len, parameters: Parameters::Values(parameters), memory: batch.reservation.budget() };
        let Some(charged) = evaluation.vector(node, None)? else { return Ok(None) };
        Ok(match &charged.vector {
            Vector::Null => Some(vec![None; batch.len]),
            Vector::Bool(values) => Some(values.iter().collect()),
            _ => None,
        })
    }
}

/// The column of `expr` over `batch`, `Ok(None)` where the row evaluator computes it.
pub(crate) fn column(expr: &Expr, schema: &Schema, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>, memory: &Arc<MemoryBudget>) -> Result<Option<Column>> {
    match Projection::bind(expr, schema) {
        Some(projection) => projection.column(batch, parameters, memory),
        None => Ok(None),
    }
}

pub(super) fn funded_record_case(
    expr: &Expr, schema: &Schema, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>, memory: &Arc<MemoryBudget>,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let Some(projection) = Projection::bind(expr, schema) else { return Ok(None); };
    let Kind::TextCase { text, .. } = &projection.0.kind else { return Ok(None); };
    let Kind::Column(position) = &text.kind else { return Ok(None); };
    let plain = |array: &dyn Array, field: &ArrowField, logical: Option<&LogicalType>| {
        field.metadata().is_empty() && array.data_type() == field.data_type()
            && (field.data_type() == &DataType::Utf8 || (field.data_type() == &DataType::Null && field.is_nullable()))
            && logical.is_none_or(|logical| super::stable_read_layout(logical, field))
    };
    let admitted = match &batch.columns[*position] {
        Column::Arrow { array, field, logical } => {
            let nulls = array.logical_null_count();
            plain(array.as_ref(), field, logical.as_ref()) && (nulls == 0 || nulls == batch.len)
        }
        Column::Selected(selected) => {
            if !selected.source_layouts().all(|(array, field, logical)| plain(array, field, logical)) { return Ok(None); }
            let mut nulls = selected.arrow_cells().map(|cell| {
                cell.is_none_or(|(array, field, _, row)| field.data_type() == &DataType::Null || array.is_null(row))
            });
            nulls.next().is_some_and(|first| nulls.all(|null| null == first))
        }
        Column::Values(_) => false,
    };
    if !admitted { return Ok(None); }
    let evaluation = Evaluation { columns: &batch.columns, len: batch.len, parameters: Parameters::Values(parameters), memory };
    let Some(Charged { vector, _charge: charge }) = evaluation.vector(&projection.0, None)? else { return Ok(None); };
    let Some(column) = Projection::observed(&vector, batch.len)? else { return Ok(None); };
    drop(vector);
    let bytes = super::owned_projection_bytes(&column)?;
    let owner = match charge {
        Some(mut owner) => {
            let lease = Arc::get_mut(&mut owner).expect("The case kernel owns its output reservation");
            if bytes <= lease.bytes() { lease.shrink_to(bytes)?; } else { lease.grow_to(bytes)?; }
            owner
        }
        None => memory.reserve(bytes)?,
    };
    Ok(Some((column, owner)))
}

/// Each row's three-valued truth under `predicate` over `batch`, keeping unknown (null) apart from false, or
/// `Ok(None)` where the row evaluator computes it. The vectors built along the way are charged to the batch's budget.
pub(crate) fn truth(predicate: &Expr, schema: &Schema, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>) -> Result<Option<Vec<Option<bool>>>> {
    match Predicate::bind(predicate, schema) {
        Some(predicate) => predicate.truth(batch, parameters),
        None => Ok(None),
    }
}

fn column_logical(column: &Column) -> Option<LogicalType> {
    match column {
        Column::Arrow { field, logical, .. } => match logical {
            Some(ty) => Some(ty.clone()),
            None => rtdb_columnar::from_field(field).ok(),
        },
        _ => None,
    }
}

/// The one-row column of `expr` over `columns`, reading its parameters from the one-row `arguments`, charged to
/// `memory` for as long as it lives.
pub(crate) fn bound_column(expr: &Expr, schema: &Schema, columns: &[Column], arguments: &[Column], memory: &Arc<MemoryBudget>) -> Result<Option<Column>> {
    if !bound_lowerable(expr) { return Ok(None); }
    let parameter_types: Vec<Option<LogicalType>> = arguments.iter().map(column_logical).collect();
    let Some(projection) = Projection::bind_with_parameters(expr, schema, &parameter_types) else { return Ok(None) };
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

/// Whether a strict node's deferred single-leg arguments all settle before any null or nullable argument, so masking
/// out null rows cannot skip an error the row evaluator raises while preparing an earlier argument.
fn strict_args_settle(arguments: &[&Node]) -> bool {
    let mut non_settling_deferred = false;
    for argument in arguments {
        if argument.declared == LogicalType::Null || argument.declared.legs().len() >= 2 {
            if non_settling_deferred { return false; }
        } else if !argument.settles() {
            non_settling_deferred = true;
        }
    }
    true
}

/// Whether the row evaluator coerces a UTF-8 string literal against a temporal operand using the session timezone.
fn literal_temporal_coercion(expr: &Expr, other_kinds: &[predicate::CellKind]) -> bool {
    matches!(expr, Expr::Literal(Value::Utf8(_)))
        && other_kinds.iter().any(|kind| matches!(kind, predicate::CellKind::Micros(_)))
}

impl Node {
    fn bind_root(expr: &Expr, schema: &Schema, params: Option<&[Option<LogicalType>]>) -> Option<(Self, VectorType)> {
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || Self::bind_vector_at(expr, schema, params, true))
    }

    fn bind_vector(expr: &Expr, schema: &Schema, params: Option<&[Option<LogicalType>]>) -> Option<(Self, VectorType)> {
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || Self::bind_vector_at(expr, schema, params, false))
    }

    fn bind_operand(expr: &Expr, schema: &Schema, params: Option<&[Option<LogicalType>]>) -> Option<(Self, Vec<predicate::CellKind>)> {
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || Self::bind_operand_at(expr, schema, params, false))
    }

    fn bind_operand_at(
        expr: &Expr, schema: &Schema, params: Option<&[Option<LogicalType>]>, half: bool,
    ) -> Option<(Self, Vec<predicate::CellKind>)> {
        match expr {
            Expr::Column { id, .. } => {
                let position = schema.position(*id)?;
                let kinds = predicate::cell_kinds(&schema.fields()[position].data_type, half)?;
                Some((Self { kind: Kind::Column(position), declared: expr.data_type() }, kinds))
            }
            Expr::Literal(value) => {
                let kinds = predicate::value_cell_kind(value, half)?.into_iter().collect();
                Some((Self { kind: Kind::Literal(value.clone()), declared: expr.data_type() }, kinds))
            }
            Expr::Parameter { index, data_type } => {
                let resolved = if *data_type == LogicalType::Nothing {
                    params.and_then(|types| types.get(*index)).and_then(Option::as_ref)?
                } else {
                    data_type
                };
                let kinds = predicate::cell_kinds(resolved, half)?;
                Some((Self { kind: Kind::Parameter(*index), declared: resolved.clone() }, kinds))
            }
            _ => {
                let (node, ty) = Self::bind_vector(expr, schema, params)?;
                Some((node, ty.cell_kinds()))
            }
        }
    }

    fn bind_vector_at(expr: &Expr, schema: &Schema, params: Option<&[Option<LogicalType>]>, root: bool) -> Option<(Self, VectorType)> {
        let declared = expr.data_type();
        match expr {
            Expr::Column { id, .. } => {
                let position = schema.position(*id)?;
                let source_ty = VectorType::of(&schema.fields()[position].data_type)?;
                let result_ty = VectorType::of(&declared)?;
                if source_ty != result_ty && !(source_ty == VectorType::Null && declared.nullable()) {
                    return None;
                }
                return Some((Self { kind: Kind::Column(position), declared }, source_ty));
            }
            Expr::Literal(value) => {
                let ty = VectorType::of_value(value)?;
                return Some((Self { kind: Kind::Literal(value.clone()), declared }, ty));
            }
            Expr::Parameter { index, data_type } => {
                let resolved = if *data_type == LogicalType::Nothing {
                    params.and_then(|types| types.get(*index)).and_then(Option::as_ref)?
                } else {
                    data_type
                };
                let ty = VectorType::of(resolved)?;
                return Some((Self { kind: Kind::Parameter(*index), declared: resolved.clone() }, ty));
            }
            _ => {}
        }
        let result_ty = VectorType::of(&declared)?;
        let half_comparison = |left: &Expr, right: &Expr| {
            let leaf = |expr: &Expr| matches!(expr, Expr::Column { .. } | Expr::Literal(_));
            root && leaf(left) && leaf(right)
        };
        let (kind, ty) = match expr {
            Expr::Field { expr, name, .. } => {
                let Expr::Column { id, .. } = expr.as_ref() else { return None; };
                let position = schema.position(*id)?;
                let LogicalType::Struct(types) = nonnull(&schema.fields()[position].data_type) else { return None; };
                let ty = VectorType::of(types.get(name)?)?;
                if ty == VectorType::Null || ty != result_ty { return None; }
                (Kind::StructField { position, name: name.clone() }, ty)
            }
            Expr::Call { name, args, .. } if name == "===" => match args.as_slice() {
                [left, right] => {
                    if result_ty != VectorType::Bool { return None; }
                    let allow_half = half_comparison(left, right);
                    let (left, left_kinds) = Self::bind_operand_at(left, schema, params, allow_half)?;
                    let (right, right_kinds) = Self::bind_operand_at(right, schema, params, allow_half)?;
                    if !predicate::all_identical(&left_kinds, &right_kinds) { return None; }
                    (Kind::Identical { left: Box::new(left), right: Box::new(right), allow_half }, VectorType::Bool)
                }
                _ => return None,
            },
            Expr::Call { name, args, .. } => match (Function::resolve(name)?, args.as_slice()) {
                (Function::Extract, [Expr::Literal(Value::Utf8(field)), source]) => {
                    let (source, source_ty) = Self::bind_vector(source, schema, params)?;
                    if !strict_args_settle(&[&source]) { return None; }
                    let ty = match &source_ty {
                        VectorType::Null => { if !declared.nullable() { return None; } VectorType::Null }
                        VectorType::Micros(zone) => {
                            if result_ty != VectorType::I32 { return None; }
                            // Validate the extract field and timezone once at bind time through the same kernel check.
                            let empty = TimestampMicrosecondArray::from(Vec::<Option<i64>>::new());
                            if kernels::extract_timestamps(field, &empty, zone.as_deref(), None).ok().flatten().is_none() {
                                return None;
                            }
                            VectorType::I32
                        }
                        _ => return None,
                    };
                    (Kind::Extract { field: field.clone(), source: Box::new(source) }, ty)
                }
                (Function::Round, [value]) => Self::bind_round(value, None, &declared, &result_ty, schema, params)?,
                (Function::Round, [value, Expr::Literal(Value::I64(scale))]) =>
                    Self::bind_round(value, Some(*scale), &declared, &result_ty, schema, params)?,
                (Function::Round, [value, Expr::Literal(Value::I32(scale))]) =>
                    Self::bind_round(value, Some(i64::from(*scale)), &declared, &result_ty, schema, params)?,
                (Function::Abs, [value]) => {
                    let (value, v_ty) = Self::bind_vector(value, schema, params)?;
                    if !strict_args_settle(&[&value]) { return None; }
                    let ty = match v_ty {
                        VectorType::Null => { if !declared.nullable() { return None; } VectorType::Null }
                        ty @ (VectorType::F64 | VectorType::I64) if result_ty == ty => ty,
                        _ => return None,
                    };
                    (Kind::Abs(Box::new(value)), ty)
                }
                (Function::Upper, [text]) => {
                    let (text, t_ty) = Self::bind_vector(text, schema, params)?;
                    if !strict_args_settle(&[&text]) { return None; }
                    let ty = match t_ty {
                        VectorType::Null => { if !declared.nullable() { return None; } VectorType::Null }
                        VectorType::Text if result_ty == VectorType::Text => VectorType::Text,
                        _ => return None,
                    };
                    (Kind::TextCase { text: Box::new(text), upper: true }, ty)
                }
                (Function::Lower, [text]) => {
                    let (text, t_ty) = Self::bind_vector(text, schema, params)?;
                    if !strict_args_settle(&[&text]) { return None; }
                    let ty = match t_ty {
                        VectorType::Null => { if !declared.nullable() { return None; } VectorType::Null }
                        VectorType::Text if result_ty == VectorType::Text => VectorType::Text,
                        _ => return None,
                    };
                    (Kind::TextCase { text: Box::new(text), upper: false }, ty)
                }
                (function @ (Function::Length | Function::OctetLength), [text]) => {
                    let (operand, kinds) = Self::bind_operand(text, schema, params)?;
                    let ty = match kinds.as_slice() {
                        [] if declared.nullable() => VectorType::Null,
                        [predicate::CellKind::Text] if result_ty == VectorType::I32 => VectorType::I32,
                        _ => return None,
                    };
                    (Kind::Length { operand: Box::new(operand), octets: function == Function::OctetLength }, ty)
                }
                (Function::Substring, [text, position]) => {
                    let (text, t_ty) = Self::bind_vector(text, schema, params)?;
                    let (position, p_ty) = Self::bind_vector(position, schema, params)?;
                    if !strict_args_settle(&[&text, &position]) { return None; }
                    if !matches!(t_ty, VectorType::Null | VectorType::Text) || !matches!(p_ty, VectorType::Null | VectorType::I32 | VectorType::I64) {
                        return None;
                    }
                    let ty = if t_ty == VectorType::Null || p_ty == VectorType::Null {
                        if !declared.nullable() { return None; }
                        VectorType::Null
                    } else {
                        if result_ty != VectorType::Text { return None; }
                        VectorType::Text
                    };
                    (Kind::Substring { text: Box::new(text), position: Box::new(position), length: None }, ty)
                }
                (Function::Substring, [text, position, length]) => {
                    let (text, t_ty) = Self::bind_vector(text, schema, params)?;
                    let (position, p_ty) = Self::bind_vector(position, schema, params)?;
                    let (length, l_ty) = Self::bind_vector(length, schema, params)?;
                    if !strict_args_settle(&[&text, &position, &length]) { return None; }
                    if !matches!(t_ty, VectorType::Null | VectorType::Text)
                        || !matches!(p_ty, VectorType::Null | VectorType::I32 | VectorType::I64)
                        || !matches!(l_ty, VectorType::Null | VectorType::I32 | VectorType::I64)
                    {
                        return None;
                    }
                    let ty = if t_ty == VectorType::Null || p_ty == VectorType::Null || l_ty == VectorType::Null {
                        if !declared.nullable() { return None; }
                        VectorType::Null
                    } else {
                        if result_ty != VectorType::Text { return None; }
                        VectorType::Text
                    };
                    (Kind::Substring { text: Box::new(text), position: Box::new(position), length: Some(Box::new(length)) }, ty)
                }
                (Function::Mod, [left, right]) => Self::bind_arithmetic(BinaryOp::Modulo, left, right, &declared, &result_ty, schema, params)?,
                (function, [value]) => {
                    let function = function.double()?;
                    let (value, kinds) = Self::bind_operand(value, schema, params)?;
                    let ty = if kinds.is_empty() && declared.nullable() {
                        VectorType::Null
                    } else if result_ty == VectorType::F64 && kinds.iter().all(|kind| matches!(kind, predicate::CellKind::Int | predicate::CellKind::Float)) {
                        VectorType::F64
                    } else {
                        return None;
                    };
                    (Kind::Double(function, Box::new(value)), ty)
                }
                _ => return None,
            },
            Expr::Binary { op: op @ (BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide | BinaryOp::Modulo), left, right } =>
                Self::bind_arithmetic(*op, left, right, &declared, &result_ty, schema, params)?,
            Expr::Binary { op: op @ (BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq), left, right } => {
                let allow_half = half_comparison(left, right);
                let (left_node, left_kinds) = Self::bind_operand_at(left, schema, params, allow_half)?;
                let (right_node, right_kinds) = Self::bind_operand_at(right, schema, params, allow_half)?;
                if !strict_args_settle(&[&left_node, &right_node]) { return None; }
                if literal_temporal_coercion(left, &right_kinds) || literal_temporal_coercion(right, &left_kinds) { return None; }
                if !predicate::all_orderable(&left_kinds, &right_kinds) { return None; }
                let ty = if left_kinds.is_empty() || right_kinds.is_empty() {
                    if !declared.nullable() { return None; }
                    VectorType::Null
                } else {
                    if result_ty != VectorType::Bool { return None; }
                    VectorType::Bool
                };
                (Kind::Compare { op: *op, left: Box::new(left_node), right: Box::new(right_node), allow_half }, ty)
            }
            Expr::Binary { op: op @ (BinaryOp::And | BinaryOp::Or), left, right } => {
                let (left_node, left_ty) = Self::bind_vector(left, schema, params)?;
                let (right_node, right_ty) = Self::bind_vector(right, schema, params)?;
                if !matches!(left_ty, VectorType::Bool | VectorType::Null) || !matches!(right_ty, VectorType::Bool | VectorType::Null) {
                    return None;
                }
                let ty = if left_ty == VectorType::Null && right_ty == VectorType::Null {
                    if !declared.nullable() { return None; }
                    VectorType::Null
                } else {
                    if result_ty != VectorType::Bool { return None; }
                    VectorType::Bool
                };
                (Kind::Logical { and: *op == BinaryOp::And, left: Box::new(left_node), right: Box::new(right_node) }, ty)
            }
            Expr::Unary { op: UnaryOp::Negate, expr: operand } => {
                let (operand, o_ty) = Self::bind_vector(operand, schema, params)?;
                if !strict_args_settle(&[&operand]) { return None; }
                let ty = match o_ty {
                    VectorType::Null => { if !declared.nullable() { return None; } VectorType::Null }
                    ty @ (VectorType::F64 | VectorType::I64 | VectorType::I32) if result_ty == ty => ty,
                    _ => return None,
                };
                (Kind::Negate(Box::new(operand)), ty)
            }
            Expr::Unary { op: UnaryOp::Not, expr: operand } => {
                let (operand, o_ty) = Self::bind_vector(operand, schema, params)?;
                if !strict_args_settle(&[&operand]) { return None; }
                let ty = match o_ty {
                    VectorType::Null => { if !declared.nullable() { return None; } VectorType::Null }
                    VectorType::Bool if result_ty == VectorType::Bool => VectorType::Bool,
                    _ => return None,
                };
                (Kind::Not(Box::new(operand)), ty)
            }
            Expr::Unary { op: op @ (UnaryOp::IsNull | UnaryOp::IsUnknown | UnaryOp::IsNotNull), expr: operand } => {
                if result_ty != VectorType::Bool { return None; }
                let operand_declared = operand.data_type();
                let computed_single = !matches!(operand.as_ref(), Expr::Column { .. } | Expr::Literal(_) | Expr::Parameter { .. } | Expr::Case { .. })
                    && operand_declared != LogicalType::Null && operand_declared.legs().len() < 2;
                let operand_node = if computed_single {
                    let (node, _) = Self::bind_vector(operand, schema, params)?;
                    if !node.settles() { return None; }
                    node
                } else {
                    Self::bind_operand(operand, schema, params)?.0
                };
                (Kind::IsNull { operand: Box::new(operand_node), negated: *op == UnaryOp::IsNotNull }, VectorType::Bool)
            }
            Expr::Like { expr: input, pattern, escape, case_insensitive, negated }
                if matches!(pattern.as_ref(), Expr::Literal(Value::Utf8(_) | Value::Null) | Expr::Parameter { .. }) =>
            {
                let (input_node, input_kinds) = Self::bind_operand(input, schema, params)?;
                let (pattern_node, pattern_kinds) = Self::bind_operand(pattern, schema, params)?;
                if !strict_args_settle(&[&input_node, &pattern_node]) { return None; }
                if !input_kinds.iter().all(|kind| *kind == predicate::CellKind::Text)
                    || !pattern_kinds.iter().all(|kind| *kind == predicate::CellKind::Text)
                {
                    return None;
                }
                let ty = if input_kinds.is_empty() || pattern_kinds.is_empty() {
                    if !declared.nullable() { return None; }
                    VectorType::Null
                } else {
                    if result_ty != VectorType::Bool { return None; }
                    VectorType::Bool
                };
                let compiled = match &pattern_node.kind {
                    Kind::Literal(Value::Utf8(pattern)) =>
                        LikeMatcher::compile(pattern, *escape, *case_insensitive, *negated).ok().map(Arc::new),
                    _ => None,
                };
                // Scalar LIKE compiles a reached pattern before demanding a deferred input payload.
                if compiled.is_none() && !matches!(&pattern_node.kind, Kind::Literal(Value::Null)) && !input_node.infallible() {
                    return None;
                }
                (Kind::Like {
                    input: Box::new(input_node),
                    pattern: Box::new(pattern_node),
                    escape: *escape,
                    case_insensitive: *case_insensitive,
                    negated: *negated,
                    compiled,
                }, ty)
            }
            Expr::Between { operand, low, high, symmetric: false } => {
                let (operand_node, operand_kinds) = Self::bind_operand(operand, schema, params)?;
                let (low_node, low_kinds) = Self::bind_operand(low, schema, params)?;
                let (high_node, high_kinds) = Self::bind_operand(high, schema, params)?;
                if literal_temporal_coercion(low, &operand_kinds) || literal_temporal_coercion(high, &operand_kinds) { return None; }
                if !predicate::all_orderable(&operand_kinds, &low_kinds) || !predicate::all_orderable(&operand_kinds, &high_kinds) { return None; }
                let ty = if operand_kinds.is_empty() || (low_kinds.is_empty() && high_kinds.is_empty()) {
                    if !declared.nullable() { return None; }
                    VectorType::Null
                } else {
                    if result_ty != VectorType::Bool { return None; }
                    VectorType::Bool
                };
                (Kind::Between { operand: Box::new(operand_node), low: Box::new(low_node), high: Box::new(high_node) }, ty)
            }
            Expr::Cast { expr: operand, target: CastTarget::Fixed(_) } => {
                if result_ty == VectorType::Null { return None; }
                let (operand, o_ty) = Self::bind_vector(operand, schema, params)?;
                if !strict_args_settle(&[&operand]) { return None; }
                let ty = match (&o_ty, &result_ty) {
                    (VectorType::Null, _) => { if !declared.nullable() { return None; } VectorType::Null }
                    (a, b) if a == b && !matches!(a, VectorType::Micros(_)) => result_ty.clone(),
                    (VectorType::I32, VectorType::I64 | VectorType::F64)
                    | (VectorType::I64, VectorType::I32 | VectorType::F64) => result_ty.clone(),
                    _ => return None,
                };
                (Kind::Cast(Box::new(operand)), ty)
            }
            Expr::Case { branches, otherwise } => {
                let conditions = branches.iter().map(|(condition, _)| {
                    let (node, ty) = Self::bind_vector(condition, schema, params)?;
                    matches!(ty, VectorType::Bool | VectorType::Null).then_some(node)
                }).collect::<Option<Vec<_>>>()?;
                let mut values = Vec::with_capacity(branches.len() + 1);
                let mut non_null = None;
                for value in branches.iter().map(|(_, value)| value).chain(std::iter::once(otherwise.as_ref())) {
                    let (node, ty) = Self::bind_vector(value, schema, params)?;
                    if ty != VectorType::Null {
                        if let Some(expected) = &non_null {
                            if *expected != ty { return None; }
                        } else {
                            non_null = Some(ty);
                        }
                    }
                    values.push(node);
                }
                let ty = match non_null {
                    None => { if !declared.nullable() { return None; } VectorType::Null }
                    Some(ty) if ty == result_ty => ty,
                    Some(_) => return None,
                };
                (Kind::Case { conditions, values }, ty)
            }
            _ => return None,
        };
        Some((Self { kind, declared }, ty))
    }

    fn bind_round(value: &Expr, scale: Option<i64>, declared: &LogicalType, result_ty: &VectorType,
        schema: &Schema, params: Option<&[Option<LogicalType>]>,
    ) -> Option<(Kind, VectorType)> {
        let floating = |logical: &LogicalType| predicate::cell_kinds(logical, false).is_some_and(|kinds| kinds == [predicate::CellKind::Float]);
        if *result_ty == VectorType::F64 && let Expr::Column { id, data_type } = value && floating(data_type) {
            let position = schema.position(*id)?;
            if floating(&schema.fields()[position].data_type) {
                let kind = Kind::RoundFloatColumn { position, scale: scale.unwrap_or(0) };
                return Some((kind, VectorType::F64));
            }
        }
        let (value, ty) = Self::bind_vector(value, schema, params)?;
        if !strict_args_settle(&[&value]) { return None; }
        match ty {
            VectorType::Null if declared.nullable() => {}
            VectorType::F64 if ty == *result_ty => {}
            VectorType::I32 | VectorType::I64 if scale.is_none() && ty == *result_ty => {}
            _ => return None,
        }
        Some((Kind::Round { value: Box::new(value), scale }, ty))
    }

    fn bind_arithmetic(
        op: BinaryOp, left: &Expr, right: &Expr, declared: &LogicalType, result_ty: &VectorType,
        schema: &Schema, params: Option<&[Option<LogicalType>]>,
    ) -> Option<(Kind, VectorType)> {
        let (left, l_ty) = Self::bind_vector(left, schema, params)?;
        let (right, r_ty) = Self::bind_vector(right, schema, params)?;
        if !strict_args_settle(&[&left, &right]) { return None; }
        if !matches!(l_ty, VectorType::Null | VectorType::I32 | VectorType::I64 | VectorType::F64)
            || !matches!(r_ty, VectorType::Null | VectorType::I32 | VectorType::I64 | VectorType::F64)
        {
            return None;
        }
        let ty = if l_ty == VectorType::Null || r_ty == VectorType::Null {
            if !declared.nullable() { return None; }
            VectorType::Null
        } else if l_ty == VectorType::F64 || r_ty == VectorType::F64 {
            if *result_ty != VectorType::F64 { return None; }
            VectorType::F64
        } else if l_ty == VectorType::I64 || r_ty == VectorType::I64 {
            if *result_ty != VectorType::I64 { return None; }
            VectorType::I64
        } else {
            if *result_ty != VectorType::I32 { return None; }
            VectorType::I32
        };
        Some((Kind::Arithmetic { op, left: Box::new(left), right: Box::new(right) }, ty))
    }

    fn retained_bytes(&self) -> Result<usize> {
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || {
            let add = |a: usize, b: usize| a.checked_add(b).ok_or_else(super::size_error);
            let mut bytes = add(size_of::<Self>(), self.declared.retained_bytes()?)?;
            match &self.kind {
                Kind::Column(_) | Kind::Parameter(_) | Kind::RoundFloatColumn { .. } => {}
                Kind::StructField { name, .. } => bytes = add(bytes, name.capacity())?,
                Kind::Literal(value) => bytes = add(bytes, value.retained_bytes()?)?,
                Kind::Extract { field, source } => {
                    bytes = add(bytes, field.capacity())?;
                    bytes = add(bytes, source.retained_bytes()?)?;
                }
                Kind::Round { value: child, .. } | Kind::Abs(child) | Kind::TextCase { text: child, .. }
                | Kind::Length { operand: child, .. } | Kind::Double(_, child) | Kind::Negate(child) | Kind::Cast(child)
                | Kind::Not(child) | Kind::IsNull { operand: child, .. } => {
                    bytes = add(bytes, child.retained_bytes()?)?;
                }
                Kind::Substring { text, position, length } => {
                    bytes = add(bytes, text.retained_bytes()?)?;
                    bytes = add(bytes, position.retained_bytes()?)?;
                    if let Some(length) = length { bytes = add(bytes, length.retained_bytes()?)?; }
                }
                Kind::Arithmetic { left, right, .. } | Kind::Compare { left, right, .. }
                | Kind::Identical { left, right, .. } | Kind::Logical { left, right, .. } => {
                    bytes = add(bytes, left.retained_bytes()?)?;
                    bytes = add(bytes, right.retained_bytes()?)?;
                }
                Kind::Like { input, pattern, compiled, .. } => {
                    bytes = add(bytes, input.retained_bytes()?)?;
                    bytes = add(bytes, pattern.retained_bytes()?)?;
                    if let Some(compiled) = compiled {
                        bytes = add(bytes, size_of::<LikeMatcher>() + 2 * size_of::<usize>())?;
                        bytes = add(bytes, compiled.retained_bytes())?;
                    }
                }
                Kind::Between { operand, low, high } => {
                    bytes = add(bytes, operand.retained_bytes()?)?;
                    bytes = add(bytes, low.retained_bytes()?)?;
                    bytes = add(bytes, high.retained_bytes()?)?;
                }
                Kind::Case { conditions, values } => {
                    for child in conditions.iter().chain(values) { bytes = add(bytes, child.retained_bytes()?)?; }
                }
            }
            Ok(bytes)
        })
    }

    fn leaf(&self) -> bool { matches!(self.kind, Kind::Column(_) | Kind::Literal(_) | Kind::Parameter(_)) }

    /// Whether preparing this node, of a single type other than null, as the row evaluator does before demanding its
    /// payload raises nothing: whatever preparing it demands (the conditions of a CASE, the operand of BETWEEN, a
    /// predicate that may be null, an argument of several legs) can't fail.
    fn settles(&self) -> bool {
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || match &self.kind {
            Kind::Column(_) | Kind::StructField { .. } | Kind::Literal(_) | Kind::Parameter(_) | Kind::RoundFloatColumn { .. } => true,
            Kind::Compare { left, right, .. } | Kind::Identical { left, right, .. } => left.prepares() && right.prepares(),
            Kind::Like { input, pattern, .. } => input.prepares() && pattern.prepares(),
            Kind::Length { operand, .. } | Kind::Double(_, operand) => operand.prepares(),
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
            Kind::Column(_) | Kind::StructField { .. } | Kind::Literal(_) | Kind::Parameter(_) => true,
            Kind::Not(operand) | Kind::IsNull { operand, .. } => operand.infallible(),
            Kind::Compare { left, right, .. } | Kind::Identical { left, right, .. } | Kind::Logical { left, right, .. } => left.infallible() && right.infallible(),
            Kind::Between { operand, low, high } => operand.infallible() && low.infallible() && high.infallible(),
            Kind::Like { input, pattern, compiled, .. } =>
                input.infallible() && (compiled.is_some() || matches!(&pattern.kind, Kind::Literal(Value::Null))),
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
            Self::Cells(_) => {
                let mut valid = Vec::with_capacity(len);
                for row in 0..len { valid.push(reads(rows, row) && self.get(row).ok()?.is_some()); }
                Some(Validity::Rows(valid))
            }
        }
    }
}

/// A predicate's answers: null at every row, as where a strict argument is, or a truth for each row.
enum Truths { Null, Bits(BooleanArray, Arc<MemoryReservation>), Rows(Vec<Option<bool>>) }

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
            Kind::StructField { position, name } => read_struct_field(&self.columns[*position], name, &node.declared, self.memory)?,
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
            Kind::RoundFloatColumn { position, scale } => {
                if len == 0 { return null(); }
                let Some(cells) = Cells::of(&self.columns[*position], false) else { return Ok(None) };
                self.map_operand::<Float64Type>(&Operand::Cells(cells), selection, Vector::F64, |cell| match cell {
                    Cell::Float(value) => Some(kernels::round_double(value, *scale)),
                    _ => None,
                })?
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
            Kind::Length { operand, octets } => {
                if len == 0 { return null(); }
                let Some(operand) = self.operand(operand, selection)? else { return Ok(None) };
                self.map_operand::<Int32Type>(&operand, selection, Vector::I32, |cell| match cell {
                    Cell::Text(text) => i32::try_from(if *octets { text.len() } else { text.chars().count() }).ok().map(Ok),
                    _ => None,
                })?
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
            Kind::Double(function, operand) => {
                if len == 0 { return null(); }
                let Some(operand) = self.operand(operand, selection)? else { return Ok(None) };
                let flat_doubles = match &operand {
                    Operand::Cells(Cells::Float(values)) => Some(*values),
                    Operand::Computed(Charged { vector: Vector::F64(values), .. }) => Some(values),
                    _ => None,
                };
                if selection.is_none() && let Some(values) = flat_doubles {
                    if values.null_count() == len { return null(); }
                    self.built(Some(Vector::F64(kernels::doubles(*function, values))))?
                } else {
                    self.map_operand::<Float64Type>(&operand, selection, Vector::F64, |cell| match cell {
                        Cell::Float(value) => Some(Ok(function(value))),
                        Cell::Int(value) => Some(Ok(function(value as f64))),
                        _ => None,
                    })?
                }
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
        self.operand_with_half(node, rows, false)
    }

    fn operand_with_half<'s>(&'s self, node: &'s Node, rows: Selection<'_>, allow_half: bool) -> Result<Option<Operand<'s>>> {
        Ok(match &node.kind {
            Kind::Column(position) => Cells::of(&self.columns[*position], allow_half).map(Operand::Cells),
            Kind::Literal(value) => predicate::scalar(value, allow_half).ok().map(Operand::Constant),
            Kind::Parameter(index) => match self.parameters {
                // An Arrow-bound parameter is decoded on its first read, which `known` alone would leave undone.
                Parameters::Values(parameters) => match parameters.get(*index)? {
                    Some(value) => predicate::scalar(value, allow_half).ok().map(Operand::Constant),
                    None => None,
                },
                Parameters::Columns(arguments) => arguments.get(*index).and_then(|column| Cells::of(column, allow_half)).map(Operand::Cells),
            },
            _ => self.vector(node, rows)?.map(Operand::Computed),
        })
    }

    fn map_operand<T: ArrowPrimitiveType>(&self, operand: &Operand<'_>, selection: Selection<'_>,
        vector: impl FnOnce(PrimitiveArray<T>) -> Vector, value: impl Fn(Cell<'_>) -> Option<Result<T::Native>>,
    ) -> Result<Option<Charged>> {
        if matches!(operand, Operand::Constant(None) | Operand::Cells(Cells::Null) | Operand::Computed(Charged { vector: Vector::Null, .. })) {
            return Ok(Some(Charged::shared(Vector::Null)));
        }
        let mut cells = (0..self.len).map(|row| {
            if !reads(selection, row) { return Ok(None); }
            operand.get(row)?.map(|cell| value(cell).ok_or(())).transpose()
        }).enumerate();
        let Some((first, cell)) = cells.find(|(_, cell)| !matches!(cell, Ok(None))) else {
            return Ok(Some(Charged::shared(Vector::Null)));
        };
        let Ok(Some(cell)) = cell else { return Ok(None) };
        let cell = cell?;
        let bytes = self.len.checked_mul(size_of::<T::Native>())
            .and_then(|bytes| bytes.checked_add(PredicateSelection::bitmap_bound(self.len)))
            .ok_or_else(super::size_error)?;
        let charge = self.memory.reserve(bytes)?;
        let mut output = PrimitiveBuilder::<T>::with_capacity(self.len);
        output.append_nulls(first);
        output.append_value(cell);
        for (_, cell) in cells {
            let Ok(cell) = cell else { return Ok(None) };
            output.append_option(cell.transpose()?);
        }
        Ok(Some(Charged { vector: vector(output.finish()), _charge: Some(charge) }))
    }

    fn truth_bits(&self, mut read: impl FnMut(usize) -> std::result::Result<Option<bool>, ()>) -> Result<Option<Truths>> {
        let charge = self.memory.reserve(PredicateSelection::bitmap_bound(self.len) * 2)?;
        let mut output = BooleanBuilder::with_capacity(self.len);
        for row in 0..self.len {
            let Ok(value) = read(row) else { return Ok(None) };
            output.append_option(value);
        }
        Ok(Some(Truths::Bits(output.finish(), charge)))
    }

    /// A predicate's truths over the rows `selection` picks, as a vector of truths of its declared type.
    fn predicate(&self, node: &Node, selection: Selection<'_>) -> Result<Option<Charged>> {
        let truths = match &node.kind {
            Kind::Compare { op, left, right, allow_half } => self.compare(*op, left, right, selection, *allow_half)?,
            Kind::Identical { left, right, allow_half } => self.identical(left, right, selection, *allow_half)?,
            Kind::IsNull { operand, negated } => self.is_null(operand, *negated, selection)?,
            Kind::Like { input, pattern, escape, case_insensitive, negated, compiled } =>
                self.like(input, pattern, (*escape, *case_insensitive, *negated), compiled.as_deref(), selection)?,
            Kind::Between { operand, low, high } => self.between(operand, low, high, selection)?,
            Kind::Logical { and, left, right } => self.logical(*and, left, right, selection)?,
            _ => unreachable!("only predicates are answered as truths"),
        };
        match truths {
            None => Ok(None),
            Some(Truths::Null) => Ok(Some(Charged::shared(Vector::Null))),
            Some(Truths::Bits(values, charge)) if plain_scalar(&node.declared) == Some(&ScalarType::Bool) =>
                Ok(Some(Charged { vector: Vector::Bool(values), _charge: Some(charge) })),
            Some(Truths::Bits(values, _)) =>
                Ok((node.declared.nullable() && values.null_count() == self.len).then(|| Charged::shared(Vector::Null))),
            Some(Truths::Rows(truths)) if plain_scalar(&node.declared) == Some(&ScalarType::Bool) =>
                Charged::built(Vector::Bool(BooleanArray::from(truths)), self.memory).map(Some),
            // A predicate typed null answers null at every row.
            Some(Truths::Rows(truths)) => Ok((node.declared.nullable() && truths.iter().all(Option::is_none)).then(|| Charged::shared(Vector::Null))),
        }
    }

    /// A comparison, strict in both operands.
    fn compare(&self, op: BinaryOp, left: &Node, right: &Node, selection: Selection<'_>, allow_half: bool) -> Result<Option<Truths>> {
        use std::cmp::Ordering::{self, Equal, Greater, Less};
        let (equality_only, accept): (bool, fn(Option<Ordering>) -> bool) = match op {
            BinaryOp::Eq => (true, |ordering| ordering == Some(Equal)),
            BinaryOp::NotEq => (true, |ordering| ordering != Some(Equal)),
            BinaryOp::Lt => (false, |ordering| ordering == Some(Less)),
            BinaryOp::LtEq => (false, |ordering| matches!(ordering, Some(Less | Equal))),
            BinaryOp::Gt => (false, |ordering| ordering == Some(Greater)),
            BinaryOp::GtEq => (false, |ordering| matches!(ordering, Some(Greater | Equal))),
            _ => return Ok(None),
        };
        let Some(prepared) = self.prepared(&[left, right], selection, |operand, rows| self.operand_with_half(operand, rows, allow_half))?
            else { return Ok(None) };
        let Prepared::Live { rows, arguments } = prepared else { return Ok(Some(Truths::Null)) };
        let [left, right] = arguments.as_slice() else { unreachable!("a comparison has two operands") };
        let live = |row: usize| reads(rows.as_deref(), row);
        match (left, right) {
            (Operand::Cells(Cells::Int(array)), Operand::Constant(Some(Cell::Int(constant)))) =>
                return self.truth_bits(|row| Ok((live(row) && array.is_valid(row)).then(|| accept(Some(array.value(row).cmp(constant)))))),
            (Operand::Constant(Some(Cell::Int(constant))), Operand::Cells(Cells::Int(array))) =>
                return self.truth_bits(|row| Ok((live(row) && array.is_valid(row)).then(|| accept(Some(constant.cmp(&array.value(row))))))),
            _ => {}
        }
        self.truth_bits(|row| {
            if !live(row) { return Ok(None); }
            match (left.get(row)?, right.get(row)?) {
                (Some(a), Some(b)) => predicate::order(a, b, equality_only).map(|order| Some(accept(order))),
                _ => Ok(None),
            }
        })
    }

    /// `left === right`, both read at every row.
    fn identical(&self, left: &Node, right: &Node, selection: Selection<'_>, allow_half: bool) -> Result<Option<Truths>> {
        let Some(left) = self.operand_with_half(left, selection, allow_half)? else { return Ok(None) };
        let Some(right) = self.operand_with_half(right, selection, allow_half)? else { return Ok(None) };
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
        if selection.is_none()
            && let Kind::Column(position) = &operand.kind
            && let Column::Selected(column) = &self.columns[*position]
            && let Some(truths) = column.null_test(!negated)
        {
            let _scratch = self.truth_scratch()?;
            return Ok(Some(Truths::Rows(truths)));
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
    fn like(&self, input: &Node, pattern: &Node, (escape, case_insensitive, negated): (Option<char>, bool, bool),
        compiled: Option<&LikeMatcher>, selection: Selection<'_>,
    ) -> Result<Option<Truths>> {
        if self.len == 0 { return Ok(Some(Truths::Null)); }
        let Some(prepared) = self.prepared(&[input, pattern], selection, |operand, rows| self.operand(operand, rows))? else { return Ok(None) };
        let Prepared::Live { rows, arguments } = prepared else { return Ok(Some(Truths::Null)) };
        let [input, pattern] = arguments.as_slice() else { unreachable!("LIKE has an input and a pattern") };
        let read_text = |row| {
            if !reads(rows.as_deref(), row) { return Ok(None); }
            match input.get(row)? { Some(Cell::Text(text)) => Ok(Some(text)), None => Ok(None), _ => Err(()) }
        };
        let _runtime_charge;
        let runtime;
        let like = match compiled {
            Some(like) => like,
            None => {
                let first = (0..self.len).map(&read_text).find(|text| !matches!(text, Ok(None)));
                match first {
                    Some(Ok(Some(_))) => {}
                    None => return Ok(Some(Truths::Null)),
                    _ => return Ok(None),
                }
                let pattern = match pattern {
                    Operand::Constant(Some(Cell::Text(pattern))) => *pattern,
                    pattern if self.len == 1 => match pattern.get(0) {
                        Ok(Some(Cell::Text(pattern))) => pattern,
                        _ => return Ok(None),
                    },
                    _ => return Ok(None),
                };
                _runtime_charge = self.memory.reserve(LikeMatcher::compilation_bytes(pattern))?;
                runtime = LikeMatcher::compile(pattern, escape, case_insensitive, negated)?;
                &runtime
            }
        };
        let _scratch = self.memory.reserve(like.scratch_bytes())?;
        let mut matches = like.matcher();
        self.truth_bits(|row| read_text(row).map(|text| text.map(&mut matches)))
    }

    /// `operand BETWEEN low AND high`: the operand is demanded first, the low bound only where it is present, and the
    /// high bound only where the low one doesn't already answer false.
    fn between(&self, operand: &Node, low: &Node, high: &Node, selection: Selection<'_>) -> Result<Option<Truths>> {
        use std::cmp::Ordering::{Equal, Greater, Less};
        let len = self.len;
        let Some(value) = self.operand(operand, selection)? else { return Ok(None) };
        let mut present = vec![false; len];
        for (row, present) in present.iter_mut().enumerate() {
            if !reads(selection, row) { continue; }
            let Ok(cell) = value.get(row) else { return Ok(None) };
            *present = cell.is_some();
        }
        let _scratch = self.truth_scratch()?;
        let Some(lower) = self.bound(&value, low, &present, |ordering| matches!(ordering, Some(Greater | Equal)))? else { return Ok(None) };
        let open: Vec<bool> = (0..len).map(|row| present[row] && lower[row] != Some(false)).collect();
        let Some(upper) = self.bound(&value, high, &open, |ordering| matches!(ordering, Some(Less | Equal)))? else { return Ok(None) };
        Ok(Some(Truths::Rows((0..len).map(|row| {
            if !present[row] { return None; }
            match (lower[row], upper[row]) { (Some(false), _) | (_, Some(false)) => Some(false), (Some(true), Some(true)) => Some(true), _ => None }
        }).collect())))
    }

    /// One bound of BETWEEN against `value` over `rows`: whether each row's value lies on its side, null where the
    /// bound is.
    fn bound(&self, value: &Operand<'_>, bound: &Node, rows: &[bool], accept: fn(Option<std::cmp::Ordering>) -> bool) -> Result<Option<Vec<Option<bool>>>> {
        let mut truths = vec![None; self.len];
        if !rows.contains(&true) { return Ok(Some(truths)); }
        let Some(bound) = self.operand(bound, Some(rows))? else { return Ok(None) };
        for (row, truth) in truths.iter_mut().enumerate() {
            if !rows[row] { continue; }
            let (Ok(Some(a)), Ok(b)) = (value.get(row), bound.get(row)) else { return Ok(None) };
            *truth = match b {
                None => None,
                Some(b) => match predicate::order(a, b, false) {
                    Ok(ordering) => Some(accept(ordering)),
                    Err(()) => return Ok(None),
                },
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
    fn character_and_octet_lengths_read_selected_run_end_text() {
        use arrow_array::{RunArray, types::Int32Type};
        let memory = MemoryBudget::new(1 << 20);
        let logical = LogicalType::RunEndEncoded {
            width: rtdb_types::RunEndWidth::I32,
            value: Box::new(LogicalType::Scalar(ScalarType::Utf8).maybe()),
        };
        let input = Field { data_type: logical.clone(), ..field(0) };
        let schema = Schema::new(vec![input.clone()]).unwrap();
        let encoded = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![2, 3, 5]),
            &StringArray::from(vec![Some("é🙂"), None, Some("e\u{301}")]),
        ).unwrap();
        let sources = vec![
            Some((Arc::new(encoded) as ArrayRef, Arc::new(rtdb_columnar::to_field("c0", &logical).unwrap()), Some(logical), None)),
            None,
        ];
        let positions = Arc::new(vec![(0, 4), (0, 1), (1, 0), (0, 2), (0, 0)]);
        let batch = ColumnBatch::settled(
            vec![Column::Selected(SelectedColumn::new(sources, positions))],
            5,
            memory.reserve(0).unwrap(),
        ).unwrap();
        for (name, expected) in [
            ("length", [Some(2), Some(2), None, None, Some(2)]),
            ("character_length", [Some(2), Some(2), None, None, Some(2)]),
            ("octet_length", [Some(3), Some(6), None, None, Some(6)]),
        ] {
            let expr = call(name, vec![input.expression()]);
            let Some(Column::Arrow { array, .. }) = project(&expr, &schema, &batch, &memory).unwrap() else {
                panic!("{name} can read the selected text")
            };
            let actual = array.as_any().downcast_ref::<Int32Array>().unwrap().iter().collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }

        let binary = call("octet_length", vec![Expr::Literal(Value::Binary(vec![0, 255]))]);
        assert!(Projection::bind(&binary, &schema).is_none());
        assert_eq!(rtdb_expr::evaluate(&binary, &schema, &[], &rtdb_expr::EvalContext::default()).unwrap(), Value::I32(2));
    }

    #[test]
    fn selected_float32_math_keeps_null_and_binary64_results() {
        use arrow_array::Float32Array;
        let memory = MemoryBudget::new(1 << 20);
        let logical = LogicalType::Scalar(ScalarType::F32).maybe();
        let input = Field { data_type: logical.clone(), ..field(0) };
        let schema = Schema::new(vec![input.clone()]).unwrap();
        let source = Arc::new(Float32Array::from(vec![Some(-0.0), Some(0.2), None, Some(f32::NAN)])) as ArrayRef;
        let arrow_field = Arc::new(rtdb_columnar::to_field("c0", &logical).unwrap());
        let selected = SelectedColumn::new(
            vec![Some((source, arrow_field, Some(logical), None)), None],
            Arc::new(vec![(0, 1), (1, 0), (0, 0), (0, 2), (0, 3)]),
        );
        let batch = ColumnBatch::settled(vec![Column::Selected(selected)], 5, memory.reserve(0).unwrap()).unwrap();
        let expr = call("sqrt", vec![input.expression()]);
        let Some(Column::Arrow { array, .. }) = project(&expr, &schema, &batch, &memory).unwrap() else {
            panic!("SQRT can read selected Float32 cells")
        };
        let values = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(values.value(0).to_bits(), f64::from(0.2_f32).sqrt().to_bits());
        assert!(values.is_null(1));
        assert_eq!(values.value(2).to_bits(), (-0.0_f64).to_bits());
        assert!(values.is_null(3));
        assert!(values.value(4).is_nan());
    }

    #[test]
    fn cast_to_double_borrows_selected_numeric_layouts() {
        use arrow_array::{Float32Array, RunArray};

        let long = LogicalType::Scalar(ScalarType::I64);
        let real = LogicalType::Scalar(ScalarType::F32);
        let double = LogicalType::Scalar(ScalarType::F64);
        let encoded_type = LogicalType::RunEndEncoded {
            width: rtdb_types::RunEndWidth::I32,
            value: Box::new(real.clone().maybe()),
        };
        let input = Field { data_type: LogicalType::from_legs([long.clone(), real, LogicalType::Null]), ..field(0) };
        let schema = Schema::new(vec![input.clone()]).unwrap();
        let source = |array: ArrayRef, logical: LogicalType| {
            let field = Arc::new(rtdb_columnar::to_field("c0", &logical).unwrap());
            Some((array, field, Some(logical), None))
        };
        let reals = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![2, 3, 5]),
            &Float32Array::from(vec![Some(-0.0), None, Some(f32::from_bits(0x7fc0_0042))]),
        ).unwrap().slice(1, 4);
        let longs = Int64Array::from(vec![(1_i64 << 53) + 1, -7]);
        let sources = vec![source(Arc::new(reals), encoded_type), source(Arc::new(longs), long), None];
        let rows = Arc::new(vec![(1, 0), (0, 0), (2, 0), (0, 1), (0, 3), (1, 1)]);
        let len = rows.len();
        let input_budget = MemoryBudget::new(1 << 20);
        let batch = ColumnBatch::settled(vec![Column::Selected(SelectedColumn::new(sources, rows))], len, input_budget.reserve(0).unwrap()).unwrap();
        let cast = Expr::Cast { expr: Box::new(input.expression()), target: CastTarget::Fixed(double.clone()) };
        let context = rtdb_expr::EvalContext::default();
        let output_bound = len * size_of::<f64>() + PredicateSelection::bitmap_bound(len);
        let budget = MemoryBudget::new(output_bound);
        let Some(Column::Arrow { array, logical, .. }) = project(&cast, &schema, &batch, &budget).unwrap() else {
            panic!("Expected Arrow doubles");
        };
        assert_eq!(logical, Some(double.maybe()));
        let result = array.as_any().downcast_ref::<Float64Array>().unwrap();
        for (row, actual) in result.iter().enumerate() {
            let cell = batch.columns[0].value(row).unwrap();
            let expected = rtdb_expr::evaluate(&cast, &schema, &[cell], &context).unwrap();
            match expected {
                Value::Null => assert_eq!(actual, None),
                Value::F64(expected) => assert_eq!(actual.map(f64::to_bits), Some(expected.to_bits())),
                value => panic!("Unexpected scalar result: {value:?}"),
            }
        }
        let refused = MemoryBudget::new(output_bound - 1);
        let error = project(&cast, &schema, &batch, &refused).err().expect("kernel output requires admission");
        assert!(error.is(rtdb_types::AnomalyKind::ResourceExhausted));
        assert_eq!(refused.used_bytes(), 0);

        let division = Expr::binary(BinaryOp::Divide, cast, Expr::Literal(Value::F64(0.0)));
        assert!(Projection::bind(&division, &schema).is_none());
    }

    #[test]
    fn round_borrows_selected_mixed_floats_with_run_ends_within_the_output_budget() {
        use arrow_array::{Float32Array, RunArray};

        let float32 = LogicalType::Scalar(ScalarType::F32);
        let float64 = LogicalType::Scalar(ScalarType::F64);
        let encoded_type = LogicalType::RunEndEncoded {
            width: rtdb_types::RunEndWidth::I32,
            value: Box::new(float32.clone().maybe()),
        };
        let input = Field { data_type: LogicalType::from_legs([float32, float64.clone(), LogicalType::Null]), ..field(0) };
        let schema = Schema::new(vec![input.clone()]).unwrap();
        let source = |array: ArrayRef, logical: LogicalType| {
            let field = Arc::new(rtdb_columnar::to_field("c0", &logical).unwrap());
            Some((array, field, Some(logical), None))
        };
        let encoded = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![2, 3, 5]),
            &Float32Array::from(vec![Some(0.2), None, Some(-2.5)]),
        ).unwrap().slice(1, 4);
        let doubles = Float64Array::from(vec![-0.0, f64::NAN, f64::INFINITY, 1.25]);
        let sources = vec![source(Arc::new(encoded), encoded_type), source(Arc::new(doubles), float64.clone()), None];
        let pattern = [(0, 2), (1, 0), (2, 0), (0, 1), (0, 0), (1, 2), (1, 1), (1, 3), (0, 2)];
        let len = pattern.len() * 128;
        let positions = Arc::new(pattern.into_iter().cycle().take(len).collect());
        let input_memory = MemoryBudget::new(1 << 20);
        let batch = ColumnBatch::settled(
            vec![Column::Selected(SelectedColumn::new(sources, positions))],
            len,
            input_memory.reserve(0).unwrap(),
        ).unwrap();
        let output_bytes = len * size_of::<f64>() + PredicateSelection::bitmap_bound(len);
        let memory = MemoryBudget::new(output_bytes);
        let context = rtdb_expr::EvalContext::default();
        for scale in [None, Some(Value::I32(1)), Some(Value::I64(-1))] {
            let args = std::iter::once(input.expression()).chain(scale.map(Expr::Literal)).collect();
            let expr = call("round", args);
            let Some(Column::Arrow { array, logical, .. }) = project(&expr, &schema, &batch, &memory).unwrap() else {
                panic!("ROUND can read selected mixed float cells")
            };
            assert_eq!(logical, Some(float64.clone().maybe()));
            let output = array.as_any().downcast_ref::<Float64Array>().unwrap();
            for (row, actual) in output.iter().enumerate() {
                let input = batch.columns[0].value(row).unwrap();
                let expected = rtdb_expr::evaluate(&expr, &schema, &[input], &context).unwrap();
                match expected {
                    Value::Null => assert_eq!(actual, None),
                    Value::F64(expected) if expected.is_nan() => assert!(actual.unwrap().is_nan()),
                    Value::F64(expected) => assert_eq!(actual.map(f64::to_bits), Some(expected.to_bits())),
                    other => panic!("unexpected ROUND result {other:?}"),
                }
            }
        }
        let expr = call("round", vec![input.expression()]);
        let refused = MemoryBudget::new(output_bytes - 1);
        let error = project(&expr, &schema, &batch, &refused).err().expect("the result needs admission");
        assert!(error.is(rtdb_types::AnomalyKind::ResourceExhausted));
        assert_eq!(refused.used_bytes(), 0);
    }

    #[test]
    fn borrowed_round_skips_null_and_untaken_cells_but_reports_reached_overflow() {
        use arrow_array::Float32Array;

        let input = Field { data_type: LogicalType::Scalar(ScalarType::F32).maybe(), ..field(0) };
        let schema = Schema::new(vec![input.clone()]).unwrap();
        let array = Arc::new(Float32Array::from(vec![None, Some(f32::INFINITY), Some(1.0)]));
        let arrow_field = Arc::new(rtdb_columnar::to_field("c0", &input.data_type).unwrap());
        let batch = ColumnBatch::settled(
            vec![Column::Arrow { array, field: arrow_field, logical: Some(input.data_type.clone()) }],
            3,
            MemoryBudget::new(1 << 20).reserve(0).unwrap(),
        ).unwrap();
        let round = call("round", vec![input.expression(), Expr::Literal(Value::I64(400))]);
        let memory = MemoryBudget::new(1 << 20);
        let context = rtdb_expr::EvalContext::default();
        let expected = rtdb_expr::evaluate(&round, &schema, &[Value::F32(1.0)], &context).unwrap_err();
        let reached = project(&round, &schema, &batch, &memory).err().expect("a finite cell overflows");
        assert_eq!(reached.code, expected.code);
        assert_eq!(memory.used_bytes(), 0);

        let null_scale = call("round", vec![input.expression(), Expr::Literal(Value::Null)]);
        assert!(Projection::bind(&null_scale, &schema).is_none());
        assert_eq!(rtdb_expr::evaluate(&null_scale, &schema, &[Value::F32(1.0)], &context).unwrap(), Value::Null);

        let guarded = Expr::Case {
            branches: vec![(Expr::binary(BinaryOp::Eq, input.expression(), Expr::Literal(Value::F32(1.0))), Expr::Literal(Value::F64(-1.0)))],
            otherwise: Box::new(round),
        };
        let Some(Column::Arrow { array, .. }) = project(&guarded, &schema, &batch, &memory).unwrap() else {
            panic!("the guarded ROUND uses the Arrow kernel")
        };
        let output = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(output.iter().collect::<Vec<_>>(), [None, Some(f64::INFINITY), Some(-1.0)]);
    }

    #[test]
    fn all_null_unary_input_needs_no_fixed_width_output_buffer() {
        let fields = [field(3), field(2)];
        let schema = Schema::new(fields.to_vec()).unwrap();
        let len = 1024;
        let columns = fields.iter().map(|input| {
            let field = Arc::new(rtdb_columnar::to_field(&input.name, &input.data_type).unwrap());
            let array = new_null_array(field.data_type(), len);
            Column::Arrow { array, field, logical: Some(input.data_type.clone()) }
        }).collect();
        let input_memory = MemoryBudget::new(1 << 20);
        let batch = ColumnBatch::settled(columns, len, input_memory.reserve(0).unwrap()).unwrap();
        let memory = MemoryBudget::new(0);
        for (name, input) in [("length", &fields[0]), ("sqrt", &fields[1]), ("round", &fields[1])] {
            let expr = call(name, vec![input.expression()]);
            let Some(Column::Arrow { array, .. }) = project(&expr, &schema, &batch, &memory).unwrap() else {
                panic!("the unary kernel accepts all-null input")
            };
            assert_eq!((array.len(), array.data_type()), (len, &DataType::Null));
        }
    }

    #[test]
    fn like_preserves_strict_nulls_and_reached_error_order() {
        let memory = MemoryBudget::new(1 << 20);
        let schema = Schema::default();
        let batch = ColumnBatch::settled(vec![], 1, memory.reserve(0).unwrap()).unwrap();
        let empty = ColumnBatch::settled(vec![], 0, memory.reserve(0).unwrap()).unwrap();
        let text = || Expr::Literal(Value::Utf8("é".into()));
        let invalid = || Expr::Literal(Value::Utf8("!".into()));
        let substring = || call("substring", vec![text(), Expr::Literal(Value::I64(1)), Expr::Literal(Value::I64(-1))]);
        let like = |input, pattern| Expr::Like {
            expr: Box::new(input), pattern: Box::new(pattern), escape: Some('!'), case_insensitive: false, negated: false,
        };
        let no_arguments = rtdb_expr::Parameters::Values(&[]);
        for (expr, expected) in [
            (Expr::binary(BinaryOp::And, Expr::Literal(Value::Bool(false)), like(text(), invalid())), Some(false)),
            (like(Expr::Literal(Value::Null), invalid()), None),
            (like(substring(), Expr::Literal(Value::Null)), None),
        ] {
            let bound = Predicate::bind(&expr, &schema).expect("LIKE binds without demanding its inputs");
            assert_eq!(bound.truth(&batch, no_arguments).unwrap(), Some(vec![expected]));
        }
        let expr = like(text(), invalid());
        let bound = Predicate::bind(&expr, &schema).unwrap();
        assert_eq!(bound.truth(&empty, no_arguments).unwrap(), Some(vec![]));
        assert_eq!(bound.truth(&batch, no_arguments).unwrap_err().code, "xtdb/invalid-like-pattern");

        let expr = like(substring(), invalid());
        assert!(Predicate::bind(&expr, &schema).is_none());
        let expected = rtdb_expr::evaluate(&expr, &schema, &[], &rtdb_expr::EvalContext::default()).unwrap_err();
        assert_eq!(expected.code, "xtdb/invalid-like-pattern");
    }

    #[test]
    fn bound_like_reads_each_executions_pattern() {
        let memory = MemoryBudget::new(1 << 20);
        let schema = Schema::default();
        let batch = ColumnBatch::settled(vec![], 1, memory.reserve(0).unwrap()).unwrap();
        let expr = Expr::Like {
            expr: Box::new(Expr::Literal(Value::Utf8("éclair".into()))),
            pattern: Box::new(Expr::Parameter { index: 0, data_type: LogicalType::Scalar(ScalarType::Utf8).maybe() }),
            escape: Some('!'),
            case_insensitive: false,
            negated: false,
        };
        let bound = Predicate::bind(&expr, &schema).unwrap();
        for (pattern, expected) in [
            (Value::Null, None),
            (Value::Utf8("é%".into()), Some(true)),
            (Value::Utf8("z%".into()), Some(false)),
        ] {
            let arguments = [pattern];
            assert_eq!(bound.truth(&batch, rtdb_expr::Parameters::Values(&arguments)).unwrap(), Some(vec![expected]));
        }
        let invalid = [Value::Utf8("!".into())];
        assert_eq!(bound.truth(&batch, rtdb_expr::Parameters::Values(&invalid)).unwrap_err().code, "xtdb/invalid-like-pattern");
    }

    #[test]
    fn comparison_projection_writes_nullable_bits_within_its_bitmap_budget() {
        let len = 8192;
        let memory = MemoryBudget::new(4096);
        let input_memory = MemoryBudget::new(1 << 20);
        let input = field(3);
        let schema = Schema::new(vec![input.clone()]).unwrap();
        let text = StringArray::from_iter((0..len).map(|row| match row % 3 {
            0 => Some("é"),
            1 => None,
            _ => Some("z"),
        }));
        let field = Arc::new(rtdb_columnar::to_field("c3", &input.data_type).unwrap());
        let source = Column::Arrow { array: Arc::new(text), field, logical: Some(input.data_type.clone()) };
        let batch = ColumnBatch::settled(vec![source], len, input_memory.reserve(0).unwrap()).unwrap();
        let expr = Expr::binary(BinaryOp::Eq, input.expression(), Expr::Literal(Value::Utf8("é".into())));
        let Some(Column::Arrow { array, .. }) = project(&expr, &schema, &batch, &memory).unwrap() else {
            panic!("a text equality comparison writes Boolean bits")
        };
        let values = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(values.true_count(), len.div_ceil(3));
        assert_eq!(values.null_count(), (len + 1) / 3);
        assert!(!values.value(2));
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

    #[test]
    fn non_ascii_text_ordering_uses_unsigned_utf8_byte_order_without_declining() {
        let memory = MemoryBudget::new(1 << 20);
        let logical = LogicalType::Scalar(ScalarType::Utf8).maybe();
        let make_field = |id: u32, name: &str| Field {
            id: ColumnId(id), name: name.into(), qualifier: None, qualified_only: false, unnamed: false, data_type: logical.clone(),
        };
        let (a_field, b_field) = (make_field(0, "a"), make_field(1, "b"));
        let schema = Schema::new(vec![a_field.clone(), b_field.clone()]).unwrap();
        let samples = [
            None, Some(""), Some("a"), Some("z"), Some("B"), Some("ß"), Some("straße"),
            Some("é"), Some("e\u{0301}"), Some("İx"), Some("日本語"), Some("😀"), Some("😁"),
        ];
        let mut left_cells = Vec::new();
        let mut right_cells = Vec::new();
        for &l in &samples {
            for &r in &samples {
                left_cells.push(l);
                right_cells.push(r);
            }
        }
        let rows = left_cells.len();
        let arrow_field = Arc::new(rtdb_columnar::to_field("t", &logical).unwrap());
        let left_col = Column::Arrow { array: Arc::new(StringArray::from(left_cells)), field: Arc::clone(&arrow_field), logical: Some(logical.clone()) };
        let right_col = Column::Arrow { array: Arc::new(StringArray::from(right_cells)), field: arrow_field, logical: Some(logical) };
        let batch = ColumnBatch::settled(vec![left_col, right_col], rows, memory.reserve(0).unwrap()).unwrap();
        let (a, b) = (a_field.expression(), b_field.expression());
        let ops = [BinaryOp::Eq, BinaryOp::NotEq, BinaryOp::Lt, BinaryOp::LtEq, BinaryOp::Gt, BinaryOp::GtEq];
        for op in ops {
            let expr = Expr::binary(op, a.clone(), b.clone());
            let compiled = Predicate::bind(&expr, &schema).unwrap_or_else(|| panic!("non-ASCII text comparison {op:?} binds at plan time"));
            let actual = compiled.truth(&batch, rtdb_expr::Parameters::Values(&[])).unwrap()
                .unwrap_or_else(|| panic!("non-ASCII text comparison {op:?} must not decline"));
            let expected = evaluated_truths(&expr, &schema, &batch).unwrap();
            assert_eq!(actual, expected, "{op:?}");
        }
        let between = Expr::Between {
            operand: Box::new(a.clone()),
            low: Box::new(Expr::Literal(Value::Utf8("ß".into()))),
            high: Box::new(Expr::Literal(Value::Utf8("😀".into()))),
            symmetric: false,
        };
        let compiled = Predicate::bind(&between, &schema).expect("non-ASCII BETWEEN binds at plan time");
        let actual = compiled.truth(&batch, rtdb_expr::Parameters::Values(&[])).unwrap().expect("non-ASCII BETWEEN must not decline");
        assert_eq!(actual, evaluated_truths(&between, &schema, &batch).unwrap());
    }

    #[test]
    fn nan_equality_inequality_ordering_and_between_match_xtdb_without_declining() {
        let memory = MemoryBudget::new(1 << 20);
        let f64_ty = LogicalType::Scalar(ScalarType::F64).maybe();
        let i64_ty = LogicalType::Scalar(ScalarType::I64).maybe();
        let bool_ty = LogicalType::Scalar(ScalarType::Bool).maybe();
        let mk = |id: u32, name: &str, data_type: LogicalType| Field {
            id: ColumnId(id), name: name.into(), qualifier: None, qualified_only: false, unnamed: false, data_type,
        };
        let (fa, fb, ic, bd) = (mk(0, "fa", f64_ty.clone()), mk(1, "fb", f64_ty.clone()), mk(2, "ic", i64_ty.clone()), mk(3, "bd", bool_ty.clone()));
        let schema = Schema::new(vec![fa.clone(), fb.clone(), ic.clone(), bd.clone()]).unwrap();
        let payload_nan = f64::from_bits(0x7ff8_0000_0000_0042);
        let floats_a = vec![
            Some(f64::NAN), Some(f64::NAN), Some(-f64::NAN), Some(payload_nan),
            Some(-0.0), Some(0.0), Some(1.0), Some(f64::INFINITY), Some(f64::NEG_INFINITY),
            Some((1i64 << 53) as f64), None,
        ];
        let floats_b = vec![
            Some(f64::NAN), Some(1.0), Some(payload_nan), Some(0.0),
            Some(0.0), Some(-0.0), Some(f64::NAN), Some(f64::NAN), Some(f64::INFINITY),
            Some((1i64 << 53) as f64), Some(f64::NAN),
        ];
        let ints = vec![
            Some(1), Some(0), Some(i64::MAX), Some(i64::MIN),
            Some(0), Some(0), Some((1i64 << 53) + 1), Some(i64::MAX), Some(-1),
            Some((1i64 << 53) + 1), None,
        ];
        let bools = vec![
            Some(false), Some(true), Some(false), Some(true),
            Some(false), Some(true), None, Some(false), Some(true),
            Some(false), None,
        ];
        let rows = floats_a.len();
        let col = |array: ArrayRef, logical: &LogicalType| Column::Arrow {
            field: Arc::new(rtdb_columnar::to_field("c", logical).unwrap()),
            array,
            logical: Some(logical.clone()),
        };
        let batch = ColumnBatch::settled(vec![
            col(Arc::new(Float64Array::from(floats_a)), &f64_ty),
            col(Arc::new(Float64Array::from(floats_b)), &f64_ty),
            col(Arc::new(Int64Array::from(ints)), &i64_ty),
            col(Arc::new(BooleanArray::from(bools)), &bool_ty),
        ], rows, memory.reserve(0).unwrap()).unwrap();

        let ops = [BinaryOp::Eq, BinaryOp::NotEq, BinaryOp::Lt, BinaryOp::LtEq, BinaryOp::Gt, BinaryOp::GtEq];
        for op in ops {
            for (left, right) in [
                (fa.expression(), fb.expression()),
                (fa.expression(), ic.expression()),
                (ic.expression(), fa.expression()),
                (fa.expression(), Expr::Literal(Value::F64(f64::NAN))),
                (bd.expression(), Expr::Literal(Value::Bool(true))),
            ] {
                let expr = Expr::binary(op, left, right);
                let compiled = Predicate::bind(&expr, &schema).unwrap_or_else(|| panic!("comparison binds at plan time: {expr:?}"));
                let actual = compiled.truth(&batch, rtdb_expr::Parameters::Values(&[])).unwrap()
                    .unwrap_or_else(|| panic!("comparison must not decline: {expr:?}"));
                let expected = evaluated_truths(&expr, &schema, &batch).unwrap();
                assert_eq!(actual, expected, "{expr:?}");
            }
        }
        // NaN = NaN is false; NaN <> NaN is true; NaN === NaN is false.
        let eq_nan = Predicate::bind(&Expr::binary(BinaryOp::Eq, fa.expression(), fb.expression()), &schema).unwrap();
        assert_eq!(eq_nan.truth(&batch, rtdb_expr::Parameters::Values(&[])).unwrap().unwrap()[0], Some(false));
        let ne_nan = Predicate::bind(&Expr::binary(BinaryOp::NotEq, fa.expression(), fb.expression()), &schema).unwrap();
        assert_eq!(ne_nan.truth(&batch, rtdb_expr::Parameters::Values(&[])).unwrap().unwrap()[0], Some(true));
        let strict_nan = Expr::Call { name: "===".into(), args: vec![fa.expression(), fb.expression()], return_type: LogicalType::Scalar(ScalarType::Bool) };
        let compiled_strict = Predicate::bind(&strict_nan, &schema).unwrap();
        assert_eq!(compiled_strict.truth(&batch, rtdb_expr::Parameters::Values(&[])).unwrap().unwrap(), evaluated_truths(&strict_nan, &schema, &batch).unwrap());

        for (low, high) in [
            (Expr::Literal(Value::F64(0.0)), Expr::Literal(Value::F64(1.0))),
            (Expr::Literal(Value::F64(f64::NAN)), Expr::Literal(Value::F64(1.0))),
            (Expr::Literal(Value::Null), Expr::Literal(Value::F64(1.0))),
            (Expr::Literal(Value::Null), Expr::Literal(Value::Null)),
        ] {
            let between = Expr::Between { operand: Box::new(fa.expression()), low: Box::new(low), high: Box::new(high), symmetric: false };
            let compiled = Predicate::bind(&between, &schema).unwrap();
            let actual = compiled.truth(&batch, rtdb_expr::Parameters::Values(&[])).unwrap().expect("NaN BETWEEN must not decline");
            assert_eq!(actual, evaluated_truths(&between, &schema, &batch).unwrap(), "{between:?}");
        }
    }

    #[test]
    fn half_float_comparisons_write_scalar_equivalent_truths_and_selection() {
        let memory = MemoryBudget::new(1 << 20);
        let logical = LogicalType::Scalar(ScalarType::F16).maybe();
        let field = Field {
            id: ColumnId(0), name: "half".into(), qualifier: None, qualified_only: false, unnamed: false,
            data_type: logical.clone(),
        };
        let values = [
            Value::F16(0x8000), Value::F16(0), Value::F16(0x7e42), Value::Null,
            Value::F16(0x3c00), Value::F16(0x7c00),
        ];
        let batch = ColumnBatch::settled(vec![Column::Arrow {
            array: rtdb_columnar::build_array(&logical, &values).unwrap(),
            field: Arc::new(rtdb_columnar::to_field("half", &logical).unwrap()),
            logical: Some(logical),
        }], values.len(), memory.reserve(0).unwrap()).unwrap();
        let schema = Schema::new(vec![field.clone()]).unwrap();
        let zero = Expr::Literal(Value::F16(0));
        let expressions = [
            Expr::binary(BinaryOp::Eq, field.expression(), zero.clone()),
            Expr::binary(BinaryOp::NotEq, field.expression(), zero.clone()),
            Expr::binary(BinaryOp::Lt, field.expression(), Expr::Literal(Value::F64(0.0))),
            Expr::Between { operand: Box::new(field.expression()), low: Box::new(zero),
                high: Box::new(Expr::Literal(Value::F16(0x3c00))), symmetric: false },
            Expr::Call { name: "===".into(), args: vec![field.expression(), Expr::Literal(Value::I32(1))],
                return_type: LogicalType::Scalar(ScalarType::Bool) },
        ];
        for predicate in expressions {
            let compiled = Predicate::bind(&predicate, &schema).unwrap();
            let parameters = rtdb_expr::Parameters::Values(&[]);
            let expected = evaluated_truths(&predicate, &schema, &batch).unwrap();
            assert_eq!(compiled.truth(&batch, parameters).unwrap().unwrap(), expected);
            let selection = compiled.selection(&batch, parameters, &memory, || Ok(())).unwrap().unwrap();
            assert_eq!((0..batch.len).map(|row| selection.contains(row)).collect::<Vec<_>>(),
                expected.into_iter().map(|truth| truth == Some(true)).collect::<Vec<_>>());
        }
    }

    #[test]
    fn half_float_comparison_keeps_fallible_compound_predicates_in_row_order() {
        let field = |id, name: &str, scalar| Field {
            id: ColumnId(id), name: name.into(), qualifier: None, qualified_only: false, unnamed: false,
            data_type: LogicalType::Scalar(scalar),
        };
        let half = field(0, "half", ScalarType::F16);
        let number = field(1, "number", ScalarType::I64);
        let schema = Schema::new(vec![half.clone(), number.clone()]).unwrap();
        let integer = |value| Expr::Literal(Value::I64(value));
        let comparison = Expr::binary(BinaryOp::Eq, half.expression(), Expr::Literal(Value::F16(0x3c00)));
        let add = Expr::binary(BinaryOp::Eq,
            Expr::binary(BinaryOp::Add, number.expression(), integer(1)), integer(0));
        let divide = Expr::binary(BinaryOp::Eq,
            Expr::binary(BinaryOp::Divide, integer(1), number.expression()), integer(0));
        let predicate = Expr::binary(BinaryOp::Or,
            Expr::binary(BinaryOp::And, comparison.clone(), add.clone()), divide.clone());
        let context = rtdb_expr::EvalContext::default();
        let first = [Value::F16(0x3c00), Value::I64(0)];
        let second = [Value::F16(0x3c00), Value::I64(i64::MAX)];
        let divide_by_zero = rtdb_expr::evaluate(&divide, &schema, &first, &context).unwrap_err();
        let overflow = rtdb_expr::evaluate(&add, &schema, &second, &context).unwrap_err();
        assert_ne!(divide_by_zero.code, overflow.code);
        assert_eq!(rtdb_expr::evaluate(&predicate, &schema, &first, &context).unwrap_err().code, divide_by_zero.code);
        assert_eq!(rtdb_expr::evaluate(&predicate, &schema, &second, &context).unwrap_err().code, overflow.code);
        assert!(Predicate::bind(&comparison, &schema).is_some());
        assert!(Projection::bind(&comparison, &schema).is_some());
        assert!(Predicate::bind(&predicate, &schema).is_none());
        assert!(Projection::bind(&predicate, &schema).is_none());
    }

    #[test]
    fn plan_time_binding_decides_column_vs_row_path_from_schema() {
        let memory = MemoryBudget::new(1 << 20);
        let mk = |id: u32, name: &str, data_type: LogicalType| Field {
            id: ColumnId(id), name: name.into(), qualifier: None, qualified_only: false, unnamed: false, data_type,
        };
        let i64_f = mk(0, "i64", LogicalType::Scalar(ScalarType::I64).maybe());
        let i32_f = mk(1, "i32", LogicalType::Scalar(ScalarType::I32).maybe());
        let f32_f = mk(2, "f32", LogicalType::Scalar(ScalarType::F32).maybe());
        let text_f = mk(3, "text", LogicalType::Scalar(ScalarType::Utf8).maybe());
        let date_f = mk(4, "date", LogicalType::Scalar(ScalarType::DateDay).maybe());
        let utc_f = mk(5, "utc", LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }).maybe());
        let local_f = mk(6, "local", LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: None }).maybe());
        let bad_tz_f = mk(7, "bad_tz", LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("Not/A_Zone".into()) }).maybe());
        let uuid_f = mk(8, "uuid", LogicalType::Scalar(ScalarType::Uuid).maybe());
        let ree_ty = LogicalType::RunEndEncoded { width: rtdb_types::RunEndWidth::I32, value: Box::new(LogicalType::Scalar(ScalarType::I64).maybe()) };
        let ree_f = mk(9, "ree", ree_ty.clone());
        let schema = Schema::new(vec![
            i64_f.clone(), i32_f.clone(), f32_f.clone(), text_f.clone(), date_f.clone(),
            utc_f.clone(), local_f.clone(), bad_tz_f.clone(), uuid_f.clone(), ree_f.clone(),
        ]).unwrap();

        // Unsupported layouts, types, casts, extract fields, literal coercions and non-settling deferred arguments
        // are rejected at bind/plan time rather than declined per batch.
        let rejected_predicates = [
            Expr::binary(BinaryOp::Eq, date_f.expression(), Expr::Literal(Value::DateDay(0))),
            Expr::binary(BinaryOp::Lt, i64_f.expression(), text_f.expression()),
            Expr::binary(BinaryOp::Lt, utc_f.expression(), local_f.expression()),
            Expr::binary(BinaryOp::Eq, utc_f.expression(), Expr::Literal(Value::Utf8("2020-01-01T00:00:00Z".into()))),
            Expr::binary(BinaryOp::Eq, i64_f.expression(), Expr::Parameter { index: 0, data_type: LogicalType::Nothing }),
            Expr::Call { name: "===".into(), args: vec![text_f.expression(), uuid_f.expression()], return_type: LogicalType::Scalar(ScalarType::Bool) },
            Expr::Call { name: "===".into(), args: vec![i64_f.expression(), utc_f.expression()], return_type: LogicalType::Scalar(ScalarType::Bool) },
            // Non-settling single-leg CASE before a nullable column must go to the row evaluator so row-0 prepare errors are preserved.
            Expr::binary(
                BinaryOp::Eq,
                Expr::Case {
                    branches: vec![(
                        Expr::binary(BinaryOp::Eq, Expr::binary(BinaryOp::Divide, Expr::Literal(Value::I64(1)), Expr::Literal(Value::I64(0))), Expr::Literal(Value::I64(1))),
                        Expr::Literal(Value::I64(1)),
                    )],
                    otherwise: Box::new(Expr::Literal(Value::I64(2))),
                },
                i64_f.expression(),
            ),
            Expr::Unary {
                op: UnaryOp::IsNull,
                expr: Box::new(Expr::binary(
                    BinaryOp::Add,
                    Expr::Case {
                        branches: vec![(
                            Expr::binary(BinaryOp::Eq, Expr::binary(BinaryOp::Divide, Expr::Literal(Value::I64(1)), Expr::Literal(Value::I64(0))), Expr::Literal(Value::I64(1))),
                            Expr::Literal(Value::I64(1)),
                        )],
                        otherwise: Box::new(Expr::Literal(Value::I64(2))),
                    },
                    Expr::Literal(Value::I64(1)),
                )),
            },
        ];
        for expr in rejected_predicates {
            assert!(Predicate::bind(&expr, &schema).is_none(), "expected bind-time rejection for predicate {expr:?}");
        }

        let rejected_projections = [
            Expr::binary(BinaryOp::Add, f32_f.expression(), Expr::Literal(Value::F64(1.0))),
            Expr::binary(BinaryOp::Add, ree_f.expression(), Expr::Literal(Value::I64(1))),
            Expr::Cast { expr: Box::new(text_f.expression()), target: CastTarget::Fixed(LogicalType::Scalar(ScalarType::I64)) },
            Expr::Cast { expr: Box::new(utc_f.expression()), target: CastTarget::Fixed(LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) })) },
            call("abs", vec![i32_f.expression()]),
            call("round", vec![i32_f.expression(), Expr::Literal(Value::I64(2))]),
            Expr::Call { name: "extract".into(), args: vec![Expr::Literal(Value::Utf8("BOGUS".into())), utc_f.expression()], return_type: LogicalType::Scalar(ScalarType::I32).maybe() },
            Expr::Call { name: "extract".into(), args: vec![Expr::Literal(Value::Utf8("YEAR".into())), bad_tz_f.expression()], return_type: LogicalType::Scalar(ScalarType::I32).maybe() },
            Expr::Case {
                branches: vec![(Expr::Literal(Value::Bool(true)), i64_f.expression())],
                otherwise: Box::new(i32_f.expression()),
            },
        ];
        for expr in rejected_projections {
            assert!(Projection::bind(&expr, &schema).is_none(), "expected bind-time rejection for projection {expr:?}");
        }

        // F32 and RunEndEncoded columns are rejected for arithmetic vectors, but bind and evaluate directly in predicates.
        let f32_pred = Expr::binary(BinaryOp::Lt, f32_f.expression(), Expr::Literal(Value::F32(1.5)));
        assert!(Predicate::bind(&f32_pred, &schema).is_some());

        let ree_schema = Schema::new(vec![mk(0, "ree", ree_ty.clone())]).unwrap();
        let ree_array: ArrayRef = Arc::new(
            arrow_array::RunArray::<arrow_array::types::Int32Type>::try_new(
                &Int32Array::from(vec![2, 3, 5]),
                &Int64Array::from(vec![Some(10), None, Some(25)]),
            ).unwrap(),
        );
        let ree_field = Arc::new(rtdb_columnar::to_field("ree", &ree_ty).unwrap());
        let ree_batch = ColumnBatch::settled(
            vec![Column::Arrow { array: ree_array, field: ree_field, logical: Some(ree_ty) }],
            5,
            memory.reserve(0).unwrap(),
        ).unwrap();
        let ree_pred = Expr::binary(BinaryOp::Gt, ree_schema.fields()[0].expression(), Expr::Literal(Value::I64(15)));
        let compiled = Predicate::bind(&ree_pred, &ree_schema).expect("RunEndEncoded predicate binds at plan time");
        let actual = compiled.truth(&ree_batch, rtdb_expr::Parameters::Values(&[])).unwrap().expect("RunEndEncoded predicate evaluates in place");
        assert_eq!(actual, evaluated_truths(&ree_pred, &ree_schema, &ree_batch).unwrap());
    }
}
