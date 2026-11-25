//! Vectorised expression evaluation through DataFusion's physical expressions.
//! Only shapes whose results match the scalar evaluator exactly are lowered: the declared result type of each node is
//! kept by casting its operands, and checked arithmetic leaves failures to the scalar evaluator.
use std::{fmt, sync::{Arc, LazyLock}};

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_array::{cast::AsArray, types::{ArrowPrimitiveType, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type}};
use arrow_schema::{ArrowError, DataType, Field as ArrowField, FieldRef, Schema as ArrowSchema};
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_common::arrow::compute::kernels::arity::try_binary;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, Operator, ScalarUDF, Volatility, create_udf};
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr, expressions as df};
use rtdb_plan::{BinaryOp, CastTarget, Expr, Schema, UnaryOp};
use rtdb_types::{LogicalType, Result, ScalarType, Value};

use crate::columnar::{Column, ColumnBatch};

/// The scalar types a lowered node may produce.
fn arrow_type(logical: &LogicalType) -> Option<(ScalarType, DataType)> {
    let scalar = match logical { LogicalType::Scalar(scalar) => scalar, LogicalType::Maybe(inner) => return arrow_type(inner), _ => return None };
    let data_type = match scalar {
        ScalarType::I8 => DataType::Int8, ScalarType::I16 => DataType::Int16, ScalarType::I32 => DataType::Int32, ScalarType::I64 => DataType::Int64,
        ScalarType::F32 => DataType::Float32, ScalarType::F64 => DataType::Float64,
        ScalarType::Utf8 => DataType::Utf8, ScalarType::Bool => DataType::Boolean,
        _ => return None,
    };
    Some((scalar.clone(), data_type))
}

fn integer(data_type: &DataType) -> bool { matches!(data_type, DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64) }

fn modulo_function<T: ArrowPrimitiveType>(compute: fn(T::Native, T::Native) -> Result<T::Native>) -> Arc<ScalarUDF> {
    let data_type = T::DATA_TYPE;
    Arc::new(create_udf("xtdb_mod", vec![data_type.clone(), data_type.clone()], data_type, Volatility::Immutable, Arc::new(move |args| {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        let result = try_binary::<_, _, _, T>(arrays[0].as_primitive::<T>(), arrays[1].as_primitive::<T>(), |left, right| {
            compute(left, right).map_err(|error| ArrowError::ComputeError(error.to_string()))
        })?;
        if args.iter().all(|argument| matches!(argument, ColumnarValue::Scalar(_))) {
            return Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&result, 0)?));
        }
        Ok(ColumnarValue::Array(Arc::new(result)))
    })))
}

#[derive(Debug, Eq, Hash)]
struct CheckedDivisor(Arc<dyn PhysicalExpr>);

impl PartialEq for CheckedDivisor {
    fn eq(&self, other: &Self) -> bool { self.0.eq(&other.0) }
}

impl fmt::Display for CheckedDivisor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.0) }
}

impl PhysicalExpr for CheckedDivisor {
    fn return_field(&self, schema: &ArrowSchema) -> datafusion_common::Result<FieldRef> {
        self.0.return_field(schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        let value = self.0.evaluate(batch)?;
        // A CASE branch can invoke its expression on zero selected rows, even with a scalar divisor.
        if batch.num_rows() != 0 && zero_present(&value) {
            return Err(ArrowError::DivideByZero.into());
        }
        Ok(value)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> { vec![&self.0] }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn PhysicalExpr>>)
        -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        let [child]: [Arc<dyn PhysicalExpr>; 1] = children.try_into()
            .map_err(|_| DataFusionError::Internal("A checked divisor has one child".into()))?;
        Ok(Arc::new(Self(child)))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { self.0.fmt_sql(f) }
}

fn zero_present(value: &ColumnarValue) -> bool {
    match value {
        ColumnarValue::Scalar(value) => match value {
            ScalarValue::Int8(v) => *v == Some(0),
            ScalarValue::Int16(v) => *v == Some(0),
            ScalarValue::Int32(v) => *v == Some(0),
            ScalarValue::Int64(v) => *v == Some(0),
            ScalarValue::Float64(v) => *v == Some(0.0),
            _ => true,
        },
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Int8 => array.as_primitive::<Int8Type>().iter().any(|v| v == Some(0)),
            DataType::Int16 => array.as_primitive::<Int16Type>().iter().any(|v| v == Some(0)),
            DataType::Int32 => array.as_primitive::<Int32Type>().iter().any(|v| v == Some(0)),
            DataType::Int64 => array.as_primitive::<Int64Type>().iter().any(|v| v == Some(0)),
            DataType::Float64 => array.as_primitive::<Float64Type>().iter().any(|v| v == Some(0.0)),
            _ => true,
        },
    }
}

static FUNCTION_CONFIG: LazyLock<Arc<ConfigOptions>> = LazyLock::new(|| Arc::new(ConfigOptions::default()));

struct FunctionLayout {
    function: Arc<ScalarUDF>,
    input: DataType,
    output: FieldRef,
}

impl FunctionLayout {
    fn new(function: Arc<ScalarUDF>, input: DataType, arity: usize) -> Option<Self> {
        // These UDFs derive their return fields from argument types alone. Validate each signature once.
        let fields: Vec<_> = (0..arity).map(|i| ArrowField::new(format!("c{i}"), input.clone(), true)).collect();
        let arguments = fields.iter().enumerate()
            .map(|(i, field)| Arc::new(df::Column::new(field.name(), i)) as Arc<dyn PhysicalExpr>).collect();
        let schema = ArrowSchema::new(fields);
        let call = ScalarFunctionExpr::try_new(Arc::clone(&function), arguments, &schema, Arc::clone(&FUNCTION_CONFIG)).ok()?;
        Some(Self { function, input, output: call.return_field(&schema).ok()? })
    }

    fn call(&self, arguments: Vec<Arc<dyn PhysicalExpr>>, expected: &DataType) -> Option<Arc<dyn PhysicalExpr>> {
        (self.output.data_type() == expected).then(|| {
            Arc::new(ScalarFunctionExpr::new(self.function.name(), Arc::clone(&self.function), arguments,
                Arc::clone(&self.output), Arc::clone(&FUNCTION_CONFIG))) as Arc<dyn PhysicalExpr>
        })
    }
}

static SIGNED_MODULO: LazyLock<Option<FunctionLayout>> = LazyLock::new(||
    FunctionLayout::new(modulo_function::<Int64Type>(rtdb_expr::signed_modulo), DataType::Int64, 2));
static FLOAT_MODULO: LazyLock<Option<FunctionLayout>> = LazyLock::new(||
    FunctionLayout::new(modulo_function::<Float64Type>(rtdb_expr::float_modulo), DataType::Float64, 2));
static UPPER: LazyLock<Option<FunctionLayout>> = LazyLock::new(||
    FunctionLayout::new(datafusion_functions::string::upper(), DataType::Utf8, 1));
static LOWER: LazyLock<Option<FunctionLayout>> = LazyLock::new(||
    FunctionLayout::new(datafusion_functions::string::lower(), DataType::Utf8, 1));
static LENGTH: LazyLock<Option<FunctionLayout>> = LazyLock::new(||
    FunctionLayout::new(datafusion_functions::unicode::character_length(), DataType::Utf8, 1));
static ABS: LazyLock<[Option<FunctionLayout>; 5]> = LazyLock::new(|| {
    [DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64, DataType::Float64]
        .map(|input| FunctionLayout::new(datafusion_functions::math::abs(), input, 1))
});

/// The input columns an expression reads, in the order the lowered expression addresses them.
struct Inputs<'a> {
    schema: &'a Schema,
    columns: &'a [Column],
    arguments: Option<&'a [Column]>,
    positions: Vec<InputPosition>,
    fields: Vec<ArrowField>,
    arrays: Vec<ArrayRef>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum InputPosition { Column(usize), Argument(usize) }

impl Inputs<'_> {
    fn column(&mut self, id: rtdb_plan::ColumnId, expected: &DataType) -> Option<Arc<dyn PhysicalExpr>> {
        self.bind(InputPosition::Column(self.schema.position(id)?), expected)
    }

    fn bind(&mut self, position: InputPosition, expected: &DataType) -> Option<Arc<dyn PhysicalExpr>> {
        let column = match position {
            InputPosition::Column(index) => self.columns.get(index)?,
            InputPosition::Argument(index) => self.arguments?.get(index)?,
        };
        let Column::Arrow { array, field, logical } = column else { return None };
        let plain = match logical { Some(logical) => arrow_type(logical).is_some_and(|(_, data_type)| &data_type == expected), None => field.metadata().is_empty() };
        if !plain || array.data_type() != expected { return None; }
        let index = match self.positions.iter().position(|known| *known == position) {
            Some(index) => index,
            None => {
                let index = self.positions.len();
                self.positions.push(position);
                self.fields.push(ArrowField::new(format!("c{index}"), expected.clone(), true));
                self.arrays.push(Arc::clone(array));
                index
            }
        };
        Some(Arc::new(df::Column::new(self.fields[index].name(), index)))
    }
}

struct Lowering<'a> { inputs: Inputs<'a>, parameters: rtdb_expr::Parameters<'a> }

fn literal(value: &Value, expected: &DataType) -> Option<ScalarValue> {
    Some(match (value, expected) {
        (Value::I8(v), DataType::Int8) => ScalarValue::Int8(Some(*v)), (Value::I16(v), DataType::Int16) => ScalarValue::Int16(Some(*v)),
        (Value::I32(v), DataType::Int32) => ScalarValue::Int32(Some(*v)), (Value::I64(v), DataType::Int64) => ScalarValue::Int64(Some(*v)),
        (Value::F32(v), DataType::Float32) => ScalarValue::Float32(Some(*v)), (Value::F64(v), DataType::Float64) => ScalarValue::Float64(Some(*v)),
        (Value::Utf8(v), DataType::Utf8) => ScalarValue::Utf8(Some(v.clone())),
        (Value::Bool(v), DataType::Boolean) => ScalarValue::Boolean(Some(*v)),
        _ => return None,
    })
}

impl Lowering<'_> {
    /// Lowers `expr` to produce exactly its declared type, or `None` where the shape stays with the evaluator.
    fn lower(&mut self, expr: &Expr) -> Option<(Arc<dyn PhysicalExpr>, DataType)> {
        let (_, data_type) = arrow_type(&expr.data_type())?;
        let lowered: Arc<dyn PhysicalExpr> = match expr {
            Expr::Column { id, .. } => self.inputs.column(*id, &data_type)?,
            Expr::Literal(value) => Arc::new(df::Literal::new(literal(value, &data_type)?)),
            Expr::Parameter { index, .. } => match self.inputs.arguments {
                Some(_) => self.inputs.bind(InputPosition::Argument(*index), &data_type)?,
                None => {
                    let value = if let Some(value) = self.parameters.known(*index) {
                        literal(value, &data_type)?
                    } else {
                        let cell = self.parameters.arrow_cell(*index)?;
                        if !(integer(&data_type) || matches!(data_type, DataType::Boolean | DataType::Float32 | DataType::Float64))
                            || cell.array.data_type() != &data_type || !cell.field.metadata().is_empty() {
                            return None;
                        }
                        ScalarValue::try_from_array(cell.array, cell.row).ok()?
                    };
                    Arc::new(df::Literal::new(value))
                }
            },
            Expr::Binary { op: op @ (BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide), left, right } => {
                if !(integer(&data_type) || data_type == DataType::Float64) { return None; }
                let left = self.cast_to(left, &data_type)?;
                let right = self.cast_to(right, &data_type)?;
                let (op, right) = match op {
                    BinaryOp::Add => (Operator::Plus, right),
                    BinaryOp::Subtract => (Operator::Minus, right),
                    BinaryOp::Multiply => (Operator::Multiply, right),
                    _ => (Operator::Divide, Arc::new(CheckedDivisor(right)) as Arc<dyn PhysicalExpr>),
                };
                Arc::new(df::BinaryExpr::new(left, op, right).with_fail_on_overflow(true))
            }
            Expr::Binary { op: BinaryOp::Modulo, left, right } => self.modulo(left, right, &data_type)?,
            Expr::Binary { op: op @ (BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq), left, right } => {
                // Floats compare by IEEE in the evaluator but by total order in Arrow, so only exact domains are lowered.
                let (_, left_type) = arrow_type(&left.data_type())?;
                let (_, right_type) = arrow_type(&right.data_type())?;
                let common = match (&left_type, &right_type) {
                    (a, b) if integer(a) && integer(b) => DataType::Int64,
                    (DataType::Utf8, DataType::Utf8) => DataType::Utf8,
                    (DataType::Boolean, DataType::Boolean) => DataType::Boolean,
                    _ => return None,
                };
                let left = self.cast_to(left, &common)?;
                let right = self.cast_to(right, &common)?;
                let op = match op { BinaryOp::Eq => Operator::Eq, BinaryOp::NotEq => Operator::NotEq, BinaryOp::Lt => Operator::Lt, BinaryOp::LtEq => Operator::LtEq, BinaryOp::Gt => Operator::Gt, _ => Operator::GtEq };
                Arc::new(df::BinaryExpr::new(left, op, right))
            }
            Expr::Binary { op: op @ (BinaryOp::And | BinaryOp::Or), left, right } => {
                let (left, left_type) = self.lower(left)?;
                let (right, right_type) = self.lower(right)?;
                if left_type != DataType::Boolean || right_type != DataType::Boolean { return None; }
                Arc::new(df::BinaryExpr::new(left, if *op == BinaryOp::And { Operator::And } else { Operator::Or }, right))
            }
            Expr::Unary { op: UnaryOp::Not, expr } => { let (inner, inner_type) = self.lower(expr)?; if inner_type != DataType::Boolean { return None; } Arc::new(df::NotExpr::new(inner)) }
            Expr::Unary { op: UnaryOp::IsNull, expr } => Arc::new(df::IsNullExpr::new(self.lower(expr)?.0)),
            Expr::Unary { op: UnaryOp::IsNotNull, expr } => Arc::new(df::IsNotNullExpr::new(self.lower(expr)?.0)),
            Expr::Unary { op: UnaryOp::Negate, expr } => {
                let inner = self.cast_to(expr, &data_type)?;
                if integer(&data_type) {
                    // Arrow's negation wraps; the evaluator rejects the minimum, as checked subtraction from zero does.
                    let zero = ScalarValue::new_zero(&data_type).ok()?;
                    Arc::new(df::BinaryExpr::new(Arc::new(df::Literal::new(zero)), Operator::Minus, inner).with_fail_on_overflow(true))
                } else if data_type == DataType::Float64 { Arc::new(df::NegativeExpr::new(inner)) } else { return None }
            }
            Expr::Cast { expr: inner, target: CastTarget::Fixed(_) } => {
                let (_, source) = arrow_type(&inner.data_type())?;
                // Widening and narrowing integer casts error on overflow, as the evaluator's do; integers become doubles exactly as they do there.
                if !(integer(&source) && (integer(&data_type) || data_type == DataType::Float64) || source == data_type) { return None; }
                let (inner, _) = self.lower(inner)?;
                Arc::new(df::CastExpr::new(inner, data_type.clone(), Some(datafusion_common::arrow::compute::CastOptions { safe: false, ..Default::default() })))
            }
            Expr::Case { branches, otherwise } => {
                let mut whens = Vec::with_capacity(branches.len());
                for (condition, value) in branches {
                    let (condition, condition_type) = self.lower(condition)?;
                    if condition_type != DataType::Boolean { return None; }
                    whens.push((condition, self.cast_to(value, &data_type)?));
                }
                let otherwise = self.cast_to(otherwise, &data_type)?;
                Arc::new(df::CaseExpr::try_new(None, whens, Some(otherwise)).ok()?)
            }
            Expr::Call { name, args, .. } if name.eq_ignore_ascii_case("mod") => {
                let [left, right] = args.as_slice() else { return None; };
                self.modulo(left, right, &data_type)?
            }
            Expr::Call { name, args, .. } if args.len() == 1 => {
                let layout = match (name.to_ascii_lowercase().as_str(), &data_type) {
                    ("upper", DataType::Utf8) => UPPER.as_ref()?,
                    ("lower", DataType::Utf8) => LOWER.as_ref()?,
                    ("length" | "character_length" | "char_length", DataType::Int32) => LENGTH.as_ref()?,
                    ("abs", data_type) => ABS.iter().flatten().find(|layout| &layout.input == data_type)?,
                    _ => return None,
                };
                let (argument, argument_type) = self.lower(&args[0])?;
                if argument_type != layout.input { return None; }
                layout.call(vec![argument], &data_type)?
            }
            _ => return None,
        };
        Some((lowered, data_type))
    }

    /// Lowers `expr` and casts it to `target`, where the evaluator converts the operand the same way.
    fn cast_to(&mut self, expr: &Expr, target: &DataType) -> Option<Arc<dyn PhysicalExpr>> {
        let (lowered, data_type) = self.lower(expr)?;
        if &data_type == target { return Some(lowered); }
        let exact = integer(&data_type) && (integer(target) && widens(&data_type, target) || *target == DataType::Float64)
            || data_type == DataType::Float32 && *target == DataType::Float64;
        exact.then(|| Arc::new(df::CastExpr::new(lowered, target.clone(), Some(datafusion_common::arrow::compute::CastOptions { safe: false, ..Default::default() }))) as Arc<dyn PhysicalExpr>)
    }

    fn modulo(&mut self, left: &Expr, right: &Expr, result_type: &DataType) -> Option<Arc<dyn PhysicalExpr>> {
        let layout = if integer(result_type) { SIGNED_MODULO.as_ref()? }
            else if matches!(result_type, DataType::Float32 | DataType::Float64) { FLOAT_MODULO.as_ref()? }
            else { return None; };
        let arguments = vec![self.cast_to(left, &layout.input)?, self.cast_to(right, &layout.input)?];
        let call = layout.call(arguments, &layout.input)?;
        if *result_type == layout.input { return Some(call); }
        Some(Arc::new(df::CastExpr::new(call, result_type.clone(), Some(datafusion_common::arrow::compute::CastOptions { safe: false, ..Default::default() }))))
    }
}

fn widens(from: &DataType, to: &DataType) -> bool {
    let width = |data_type: &DataType| match data_type { DataType::Int8 => 1, DataType::Int16 => 2, DataType::Int32 => 4, DataType::Int64 => 8, _ => 0 };
    width(from) <= width(to)
}

/// Whether the shape of `expr` could be lowered; its inputs' layouts decide per batch.
pub(crate) fn lowerable(expr: &Expr) -> bool {
    lowerable_at(expr, None)
}

pub(crate) fn bound_lowerable(expr: &Expr) -> bool {
    lowerable_at(expr, Some(32))
}

fn lowerable_at(expr: &Expr, remaining: Option<usize>) -> bool {
    if remaining == Some(0) { return false; }
    let lower = |expr: &Expr| lowerable_at(expr, remaining.map(|depth| depth - 1));
    match expr {
        Expr::Parameter { .. } if remaining.is_some() => true,
        Expr::Column { .. } | Expr::Parameter { .. } | Expr::Literal(_) => arrow_type(&expr.data_type()).is_some(),
        Expr::Binary { op: BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide | BinaryOp::Modulo | BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::Lt | BinaryOp::LtEq
            | BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::And | BinaryOp::Or, left, right } => lower(left) && lower(right),
        Expr::Unary { op: UnaryOp::Not | UnaryOp::IsNull | UnaryOp::IsNotNull | UnaryOp::Negate, expr } => lower(expr),
        Expr::Cast { expr, target: CastTarget::Fixed(_) } => lower(expr),
        Expr::Case { branches, otherwise } => branches.iter().all(|(condition, value)| lower(condition) && lower(value)) && lower(otherwise),
        Expr::Call { name, args, .. } => match (name.to_ascii_lowercase().as_str(), args.as_slice()) {
            ("upper" | "lower", [arg]) if remaining.is_some() => {
                matches!(arg, Expr::Column { .. } | Expr::Parameter { .. } | Expr::Literal(_)) && lower(arg)
            }
            ("upper" | "lower" | "length" | "character_length" | "char_length" | "abs", [arg]) => lower(arg),
            ("mod", [left, right]) => lower(left) && lower(right),
            _ => false,
        },
        _ => false,
    }
}

/// Evaluates `expr` over a batch's columns, or `None` where the evaluator must compute this batch.
pub(crate) fn evaluate(expr: &Expr, schema: &Schema, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>) -> Option<ArrayRef> {
    if !lowerable(expr) { return None; }
    evaluate_at(expr, schema, &batch.columns, batch.len, parameters, None)
}

fn evaluate_at(expr: &Expr, schema: &Schema, columns: &[Column], len: usize, parameters: rtdb_expr::Parameters<'_>, arguments: Option<&[Column]>) -> Option<ArrayRef> {
    let inputs = Inputs { schema, columns, arguments, positions: Vec::new(), fields: Vec::new(), arrays: Vec::new() };
    let mut lowering = Lowering { inputs, parameters };
    let (lowered, data_type) = lowering.lower(expr)?;
    let input = RecordBatch::try_new_with_options(Arc::new(ArrowSchema::new(lowering.inputs.fields)), lowering.inputs.arrays,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(len))).ok()?;
    let result = match lowered.evaluate(&input).ok()? { ColumnarValue::Array(array) => array, scalar => scalar.into_array(len).ok()? };
    (result.data_type() == &data_type && result.len() == len).then_some(result)
}

/// The column `evaluate` computed, typed as the expression's declared scalar.
pub(crate) fn column(expr: &Expr, schema: &Schema, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>) -> Option<Column> {
    computed_column(expr, evaluate(expr, schema, batch, parameters)?)
}

pub(crate) fn comparison_operand_bound(expr: &Expr, schema: &Schema, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>) -> Option<usize> {
    if !lowerable(expr) { return None; }
    let mut bound = EvaluationBound {
        schema, batch, parameters,
        bytes: crate::runtime_schema::expression_bound(expr).ok()?.checked_mul(4)?,
        per_array: batch.len.checked_mul(64)?.checked_add(512)?,
        inputs: 0,
        selections: 0,
    };
    bound.expression(expr)?;
    // DataFusion's CASE and short-circuit AND can copy their input batch for a selection.
    bound.bytes.checked_add(bound.inputs.checked_mul(bound.selections)?.checked_mul(4)?)
}

struct EvaluationBound<'a> {
    schema: &'a Schema,
    batch: &'a ColumnBatch,
    parameters: rtdb_expr::Parameters<'a>,
    bytes: usize,
    per_array: usize,
    inputs: usize,
    selections: usize,
}

impl EvaluationBound<'_> {
    fn expression(&mut self, expr: &Expr) -> Option<usize> {
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || self.expression_at(expr))
    }

    fn expression_at(&mut self, expr: &Expr) -> Option<usize> {
        self.bytes = self.bytes.checked_add(self.per_array)?;
        let text = match expr {
            Expr::Column { id, .. } => {
                let Column::Arrow { array, .. } = self.batch.columns.get(self.schema.position(*id)?)? else { return None; };
                let text = array.as_any().downcast_ref::<StringArray>().map_or(0, |strings| {
                    let offsets = strings.value_offsets();
                    (offsets[strings.len()] - offsets[0]) as usize
                });
                self.inputs = self.inputs.checked_add(self.per_array)?.checked_add(text)?;
                text
            }
            Expr::Literal(Value::Utf8(text)) => text.len().checked_mul(self.batch.len.max(1))?,
            Expr::Literal(_) => 0,
            Expr::Parameter { index, .. } => match self.parameters.known(*index) {
                Some(Value::Utf8(text)) => text.len().checked_mul(self.batch.len.max(1))?,
                _ => 0,
            },
            Expr::Cast { expr, target: CastTarget::Fixed(_) } => self.expression(expr)?,
            Expr::Unary { expr, .. } => { self.expression(expr)?; 0 }
            Expr::Binary { op, left, right } => {
                self.expression(left)?;
                self.expression(right)?;
                if *op == BinaryOp::And { self.selections = self.selections.checked_add(1)?; }
                0
            }
            Expr::Case { branches, otherwise } => {
                self.selections = self.selections.checked_add(branches.len())?;
                let mut text = 0usize;
                for (condition, value) in branches {
                    self.expression(condition)?;
                    text = text.checked_add(self.expression(value)?)?;
                }
                text.checked_add(self.expression(otherwise)?)?
            }
            Expr::Call { name, args, .. } => {
                let text = args.iter().try_fold(0usize, |text, arg| text.checked_add(self.expression(arg)?))?;
                if name.eq_ignore_ascii_case("upper") || name.eq_ignore_ascii_case("lower") {
                    // A Unicode case mapping emits up to three code points of up to four bytes each.
                    text.checked_mul(12)?
                } else { 0 }
            }
            _ => return None,
        };
        self.bytes = self.bytes.checked_add(text.checked_mul(4)?)?;
        Some(text)
    }
}

pub(crate) fn bound_column(expr: &Expr, schema: &Schema, columns: &[Column], arguments: &[Column],
    memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<Option<Column>> {
    if !bound_lowerable(expr) { return Ok(None); }
    let mut bytes = crate::runtime_schema::expression_bound(expr)?.checked_mul(4).ok_or_else(bound_size)?;
    let mut referenced: Result<()> = Ok(());
    expr.visit(&mut |node| {
        if referenced.is_err() { return; }
        let column = match node {
            Expr::Column { id, .. } => schema.position(*id).and_then(|index| columns.get(index)),
            Expr::Parameter { index, .. } => arguments.get(*index),
            _ => None,
        };
        if let Some(column) = column {
            referenced = (|| {
                // DataFusion's case conversion can allocate from UTF-8 offsets even for a masked cell.
                let span = match column {
                    Column::Arrow { array, .. } => array.as_any().downcast_ref::<StringArray>()
                        .map_or(0, |strings| (strings.value_offsets()[1] - strings.value_offsets()[0]) as usize),
                    _ => 0,
                };
                let extra = column.bound(0)?.max(span).checked_mul(64).ok_or_else(bound_size)?;
                bytes = bytes.checked_add(extra).ok_or_else(bound_size)?;
                Ok(())
            })();
        }
    });
    referenced?;
    let mut reservation = memory.reserve(bytes)?;
    let Some(array) = evaluate_at(expr, schema, columns, 1, rtdb_expr::Parameters::Values(&[]), Some(arguments)) else { return Ok(None); };
    let retained = array.get_array_memory_size();
    if retained > bytes { return Ok(None); }
    Arc::get_mut(&mut reservation).expect("bound expression owns its reservation").shrink_to(retained)?;
    let array = rtdb_columnar::retain_array(array, &reservation)?;
    Ok(computed_column(expr, array))
}

fn bound_size() -> rtdb_types::Anomaly { crate::invalid("parameter-size", "Arrow expression inputs exceed addressable memory") }

fn computed_column(expr: &Expr, array: ArrayRef) -> Option<Column> {
    let (scalar, _) = arrow_type(&expr.data_type())?;
    let field = Arc::new(ArrowField::new("value", array.data_type().clone(), true));
    Some(Column::Arrow { array, field, logical: Some(LogicalType::Scalar(scalar)) })
}

/// The truth of a predicate per row, as the row filter's: `None` for a null.
pub(crate) fn truth(predicate: &Expr, schema: &Schema, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>) -> Option<Vec<Option<bool>>> {
    let array = evaluate(predicate, schema, batch, parameters)?;
    let booleans = array.as_any().downcast_ref::<arrow_array::BooleanArray>()?;
    Some(booleans.iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
    use rtdb_columnar::MemoryBudget;
    use rtdb_plan::{ColumnId, Field};
    use rtdb_types::Timestamp;

    fn arrow(array: ArrayRef, scalar: ScalarType) -> Column {
        let field = Arc::new(ArrowField::new("value", array.data_type().clone(), true));
        Column::Arrow { array, field, logical: Some(LogicalType::Scalar(scalar)) }
    }

    #[test]
    fn division_checks_the_divisor_in_the_rows_that_reach_it() {
        let memory = MemoryBudget::new(1 << 20);
        let numerator = field(0, ScalarType::F64);
        let divisor = field(1, ScalarType::F64);
        let condition = field(2, ScalarType::Bool);
        let schema = Schema::new(vec![numerator.clone(), divisor.clone(), condition.clone()]).unwrap();
        let divide = |right| Expr::binary(BinaryOp::Divide, numerator.expression(), right);
        let guarded = Expr::Case {
            branches: vec![(condition.expression(), divide(divisor.expression()))],
            otherwise: Box::new(Expr::Literal(Value::F64(7.0))),
        };
        let batch = ColumnBatch::settled(vec![
            arrow(Arc::new(Float64Array::from(vec![Some(8.0), Some(8.0), None, None, Some(8.0)])), ScalarType::F64),
            arrow(Arc::new(Float64Array::from(vec![Some(2.0), None, Some(2.0), Some(-0.0), Some(0.0)])), ScalarType::F64),
            arrow(Arc::new(BooleanArray::from(vec![true, true, true, false, false])), ScalarType::Bool),
        ], 5, memory.reserve(0).unwrap()).unwrap();
        let parameters = rtdb_expr::Parameters::Values(&[]);
        let values = evaluate(&guarded, &schema, &batch, parameters).unwrap();
        assert_eq!(values.as_primitive::<Float64Type>().iter().collect::<Vec<_>>(), [Some(4.0), None, None, Some(7.0), Some(7.0)]);
        assert!(evaluate(&divide(divisor.expression()), &schema, &batch, parameters).is_none());
        for zero in [0.0, -0.0] {
            assert!(evaluate(&divide(Expr::Literal(Value::F64(zero))), &schema, &batch, parameters).is_none());
            let empty = ColumnBatch::settled(vec![], 0, memory.reserve(0).unwrap()).unwrap();
            let constant = Expr::binary(BinaryOp::Divide, Expr::Literal(Value::F64(8.0)), Expr::Literal(Value::F64(zero)));
            let empty_result = evaluate(&constant, &Schema::default(), &empty, parameters).unwrap();
            assert!(empty_result.is_empty());
        }

        let null = Expr::Parameter { index: 0, data_type: numerator.data_type.clone() };
        let null_divided_by_zero = Expr::binary(BinaryOp::Divide, null, Expr::Literal(Value::F64(-0.0)));
        let arguments = [arrow(Arc::new(Float64Array::from(vec![None::<f64>])), ScalarType::F64)];
        assert!(bound_column(&null_divided_by_zero, &Schema::default(), &[], &arguments, &memory).unwrap().is_none());
    }

    #[test]
    fn division_fallback_reports_the_first_reached_scalar_error() {
        let memory = MemoryBudget::new(1 << 20);
        let schema = Schema::default();
        let batch = ColumnBatch::settled(vec![], 1, memory.reserve(0).unwrap()).unwrap();
        let division = |left, right| Expr::binary(BinaryOp::Divide, Expr::Literal(Value::I64(left)), Expr::Literal(Value::I64(right)));
        let overflow = division(i64::MIN, -1);
        let zero = division(1, 0);
        let scope = rtdb_expr::EvaluationScope::new(&memory).unwrap();
        let context = rtdb_expr::EvalContext { parameters: rtdb_expr::Parameters::Values(&[]), parameter_types: None,
            current_time: rtdb_types::Instant::from(Timestamp(0)), timezone: "UTC", outer_rows: &[], snapshot_token: None,
            allocation: Some(&scope), catalog: None };
        for (left, right, code) in [
            (overflow.clone(), zero.clone(), "xtdb/numeric-value-out-of-range"),
            (zero.clone(), overflow, "xtdb.expression/division-by-zero"),
        ] {
            let expr = Expr::binary(BinaryOp::Add, left, right);
            assert!(evaluate(&expr, &schema, &batch, context.parameters).is_none());
            assert_eq!(rtdb_expr::evaluate(&expr, &schema, &[], &context).unwrap_err().code, code);
        }
        let unreachable = Expr::binary(BinaryOp::And, Expr::Literal(Value::Bool(false)),
            Expr::binary(BinaryOp::Eq, zero, Expr::Literal(Value::I64(1))));
        assert_eq!(truth(&unreachable, &schema, &batch, context.parameters), Some(vec![Some(false)]));
    }

    #[test]
    fn function_layouts_reuse_types_and_bind_live_arrays_and_parameters() {
        let memory = MemoryBudget::new(1 << 20);
        let input = field(0, ScalarType::I32);
        let schema = Schema::new(vec![input.clone()]).unwrap();
        let abs = Expr::Call {
            name: "abs".into(),
            args: vec![Expr::binary(BinaryOp::Add, input.expression(), Expr::Parameter { index: 0, data_type: input.data_type.clone() })],
            return_type: input.data_type.clone(),
        };
        for (value, argument, expected) in [(-7, 3, 4), (11, -2, 9)] {
            let batch = ColumnBatch::settled(vec![arrow(Arc::new(Int32Array::from(vec![value])), ScalarType::I32)],
                1, memory.reserve(0).unwrap()).unwrap();
            let argument = [Value::I32(argument)];
            let result = evaluate(&abs, &schema, &batch, rtdb_expr::Parameters::Values(&argument)).unwrap();
            assert_eq!(result.as_primitive::<Int32Type>().value(0), expected);
        }

        let upper = Expr::Call {
            name: "upper".into(),
            args: vec![Expr::Parameter { index: 0, data_type: LogicalType::Scalar(ScalarType::Utf8) }],
            return_type: LogicalType::Scalar(ScalarType::Utf8),
        };
        for (value, expected) in [("abc", "ABC"), ("Straße", "STRASSE")] {
            let arguments = [arrow(Arc::new(StringArray::from(vec![value])), ScalarType::Utf8)];
            let result = bound_column(&upper, &Schema::default(), &[], &arguments, &memory).unwrap().unwrap();
            assert_eq!(result.value(0).unwrap(), Value::Utf8(expected.into()));
        }
    }

    #[test]
    fn modulo_projects_primitive_arrow_cells_and_feeds_an_integer_predicate() {
        let memory = MemoryBudget::new(1 << 20);
        for (ty, rows, divisor, expected) in [
            (ScalarType::I8, vec![Value::I8(i8::MIN), Value::I8(-7), Value::Null], Value::I8(3), vec![Value::I8(1), Value::I8(2), Value::Null]),
            (ScalarType::I16, vec![Value::I16(-7), Value::I16(7)], Value::I16(-3), vec![Value::I16(-1), Value::I16(-2)]),
            (ScalarType::I32, vec![Value::I32(-7)], Value::I32(3), vec![Value::I32(2)]),
            (ScalarType::I64, vec![Value::I64(i64::MIN)], Value::I64(-1), vec![Value::I64(0)]),
            (ScalarType::F32, vec![Value::F32(-7.5), Value::Null], Value::I8(3), vec![Value::F32(1.5), Value::Null]),
            (ScalarType::F64, vec![Value::F64(7.5), Value::F64(1e30)], Value::F64(3.0), vec![Value::F64(1.5), Value::F64(0.0)]),
        ] {
            let field = field(0, ty);
            let schema = Schema::new(vec![field.clone()]).unwrap();
            let arrow_field = Arc::new(rtdb_columnar::to_field("v", &field.data_type).unwrap());
            let array = rtdb_columnar::build_array(&field.data_type, &rows).unwrap();
            let column = Column::Arrow { array, field: arrow_field, logical: Some(field.data_type.clone()) };
            let batch = ColumnBatch::settled(vec![column], rows.len(), memory.reserve(0).unwrap()).unwrap();
            let args = vec![field.expression(), Expr::Literal(divisor)];
            let result_type = rtdb_expr::function_type("mod", &args.iter().map(Expr::data_type).collect::<Vec<_>>()).unwrap();
            let function = Expr::Call { name: "mod".into(), args, return_type: result_type };
            let parameters = rtdb_expr::Parameters::Values(&[]);
            let output = super::column(&function, &schema, &batch, parameters).expect("Expected primitive Arrow MOD");
            for (row, value) in expected.iter().enumerate() { assert_eq!(&output.value(row).unwrap(), value); }
            if matches!(expected[0], Value::I8(_)) {
                let predicate = Expr::binary(BinaryOp::Gt, function, Expr::Literal(Value::I8(1)));
                assert_eq!(truth(&predicate, &schema, &batch, parameters).unwrap(), [Some(false), Some(true), None]);
            }
        }
    }

    struct Random(u64);
    impl Random {
        fn next(&mut self) -> u64 { self.0 ^= self.0 << 13; self.0 ^= self.0 >> 7; self.0 ^= self.0 << 17; self.0 }
        fn below(&mut self, bound: u64) -> u64 { self.next() % bound }
    }
    fn field(id: u32, scalar: ScalarType) -> Field {
        Field { id: ColumnId(id), name: format!("c{id}"), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(scalar).maybe() }
    }
    const TYPES: [ScalarType; 5] = [ScalarType::I64, ScalarType::I32, ScalarType::F64, ScalarType::Utf8, ScalarType::Bool];
    fn column(id: u32) -> Expr { Expr::Column { id: ColumnId(id), data_type: LogicalType::Scalar(TYPES[id as usize].clone()).maybe() } }

    fn batch(random: &mut Random, rows: usize, memory: &std::sync::Arc<MemoryBudget>) -> ColumnBatch {
        let null = |random: &mut Random| random.below(8) == 0;
        let ints = |random: &mut Random| -> i64 { match random.below(10) { 0 => i64::MAX - random.below(2) as i64, 1 => i64::MIN + random.below(2) as i64, 2 => 0, _ => random.below(41) as i64 - 20 } };
        let mut i64s = Vec::new(); let mut i32s = Vec::new(); let mut f64s = Vec::new(); let mut texts = Vec::new(); let mut bools = Vec::new();
        for _ in 0..rows {
            i64s.push((!null(random)).then(|| ints(random)));
            i32s.push((!null(random)).then(|| match random.below(8) { 0 => i32::MAX, 1 => i32::MIN, 2 => 0, _ => random.below(41) as i32 - 20 }));
            f64s.push((!null(random)).then(|| { let value = (random.below(41) as f64 - 20.0) / 4.0; if value == 0.0 && random.below(2) == 0 { -0.0 } else { value } }));
            texts.push((!null(random)).then(|| ["a", "B", "ß", "straße", "Ǆx", ""][random.below(6) as usize].to_owned()));
            bools.push((!null(random)).then(|| random.below(2) == 0));
        }
        let arrow = |array: ArrayRef, scalar: ScalarType| Column::Arrow { field: Arc::new(ArrowField::new("c", array.data_type().clone(), true)), array, logical: Some(LogicalType::Scalar(scalar)) };
        ColumnBatch::settled(vec![arrow(Arc::new(Int64Array::from(i64s)), ScalarType::I64), arrow(Arc::new(Int32Array::from(i32s)), ScalarType::I32),
            arrow(Arc::new(Float64Array::from(f64s)), ScalarType::F64), arrow(Arc::new(StringArray::from(texts)), ScalarType::Utf8),
            arrow(Arc::new(BooleanArray::from(bools)), ScalarType::Bool)], rows, memory.reserve(0).unwrap()).unwrap()
    }

    fn number(random: &mut Random, depth: usize) -> Expr {
        if depth == 0 || random.below(3) == 0 {
            return match random.below(6) {
                0 => column(0), 1 => column(1), 2 => column(2),
                3 => Expr::Literal(Value::I64(random.below(7) as i64 - 3)),
                4 => Expr::Literal(Value::F64((random.below(9) as f64 - 4.0) / 2.0)),
                _ => Expr::Call { name: "length".into(), args: vec![text(random)], return_type: LogicalType::Scalar(ScalarType::I32).maybe() },
            };
        }
        match random.below(7) {
            0..=2 => {
                let op = [BinaryOp::Add, BinaryOp::Subtract, BinaryOp::Multiply, BinaryOp::Divide][random.below(4) as usize];
                Expr::binary(op, number(random, depth - 1), number(random, depth - 1))
            }
            3 => Expr::Unary { op: UnaryOp::Negate, expr: Box::new(number(random, depth - 1)) },
            4 => Expr::Cast { expr: Box::new(number(random, depth - 1)), target: CastTarget::Fixed(LogicalType::Scalar([ScalarType::F64, ScalarType::I64, ScalarType::I32][random.below(3) as usize].clone())) },
            5 => { let argument = number(random, depth - 1); let ty = rtdb_plan::function_type("abs", &[argument.data_type()]).unwrap(); Expr::Call { name: "abs".into(), args: vec![argument], return_type: ty } }
            _ => Expr::Case { branches: vec![(predicate(random, 0), integer_number(random, depth - 1)), (predicate(random, 0), integer_number(random, depth - 1))], otherwise: Box::new(integer_number(random, depth - 1)) },
        }
    }
    /// A big-integer expression, so CASE branches share their result type.
    fn integer_number(random: &mut Random, depth: usize) -> Expr {
        if depth == 0 || random.below(2) == 0 {
            return if random.below(2) == 0 { column(0) } else { Expr::Literal(Value::I64(random.below(7) as i64 - 3)) };
        }
        let op = [BinaryOp::Add, BinaryOp::Subtract, BinaryOp::Multiply, BinaryOp::Divide][random.below(4) as usize];
        Expr::binary(op, integer_number(random, depth - 1), integer_number(random, depth - 1))
    }
    fn text(random: &mut Random) -> Expr {
        match random.below(3) {
            0 => column(3),
            name => Expr::Call { name: if name == 1 { "upper".into() } else { "lower".into() }, args: vec![column(3)], return_type: LogicalType::Scalar(ScalarType::Utf8).maybe() },
        }
    }
    fn predicate(random: &mut Random, depth: usize) -> Expr {
        let comparison = |random: &mut Random, left: Expr, right: Expr| Expr::binary([BinaryOp::Eq, BinaryOp::NotEq, BinaryOp::Lt, BinaryOp::LtEq, BinaryOp::Gt, BinaryOp::GtEq][random.below(6) as usize], left, right);
        if depth == 0 || random.below(3) == 0 {
            return match random.below(4) {
                0 => column(4),
                1 => { let (left, right) = (column(random.below(2) as u32), Expr::Literal(Value::I64(random.below(9) as i64 - 4))); comparison(random, left, right) }
                2 => { let right = Expr::Literal(Value::Utf8(["a", "B", "ß"][random.below(3) as usize].into())); let left = text(random); comparison(random, left, right) }
                _ => Expr::Unary { op: if random.below(2) == 0 { UnaryOp::IsNull } else { UnaryOp::IsNotNull }, expr: Box::new(column(random.below(5) as u32)) },
            };
        }
        match random.below(4) {
            0 => Expr::binary(BinaryOp::And, predicate(random, depth - 1), predicate(random, depth - 1)),
            1 => Expr::binary(BinaryOp::Or, predicate(random, depth - 1), predicate(random, depth - 1)),
            2 => Expr::Unary { op: UnaryOp::Not, expr: Box::new(predicate(random, depth - 1)) },
            _ => { let (left, right) = (number(random, depth - 1), number(random, depth - 1)); comparison(random, left, right) }
        }
    }

    #[test]
    fn vectorised_columns_agree_with_the_scalar_evaluator_on_every_batch_they_compute() {
        let memory = MemoryBudget::new(1 << 28);
        let schema = Schema::new((0..5).map(|id| field(id, TYPES[id as usize].clone())).collect()).unwrap();
        let mut random = Random(0x2545_f491_4f6c_dd1d);
        let (mut computed, mut declined) = (0, 0);
        for round in 0..1500 {
            let batch = batch(&mut random, 48, &memory);
            let expr = if round % 2 == 0 { number(&mut random, 3) } else { predicate(&mut random, 3) };
            let scope = rtdb_expr::EvaluationScope::new(&memory).unwrap();
            let context = rtdb_expr::EvalContext { parameters: rtdb_expr::Parameters::Values(&[]), parameter_types: None, current_time: rtdb_types::Instant::from(Timestamp(0)), timezone: "UTC",
                outer_rows: &[], snapshot_token: None, allocation: Some(&scope), catalog: None };
            let expected: Vec<rtdb_types::Result<Value>> = (0..batch.len).map(|row| {
                let values: Vec<Value> = batch.columns.iter().map(|column| column.value(row).unwrap()).collect();
                rtdb_expr::evaluate(&expr, &schema, &values, &context)
            }).collect();
            let Some(column) = super::column(&expr, &schema, &batch, rtdb_expr::Parameters::Values(&[])) else { declined += 1; continue };
            computed += 1;
            for (row, expected) in expected.into_iter().enumerate() {
                let expected = expected.unwrap_or_else(|error| panic!("the vectorised path computed {expr:?} where the evaluator raises {error:?}"));
                let actual = column.value(row).unwrap();
                let same = match (&expected, &actual) { (Value::F64(a), Value::F64(b)) => a.to_bits() == b.to_bits(), (a, b) => a == b };
                assert!(same, "{expr:?} at row {row}: evaluator {expected:?}, vectorised {actual:?}");
            }
        }
        assert!(computed > 300, "the vectorised path should compute many generated expressions: computed {computed}, declined {declined}");
    }
}
