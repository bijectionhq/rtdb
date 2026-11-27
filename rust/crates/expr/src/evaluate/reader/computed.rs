use std::cmp::Ordering;

use rtdb_types::{NumericCell, ScalarType, decimal_arrow::DecimalParameters};

use crate::admission::Admission;
use crate::decimal::ScalarDecimal;
use super::*;

pub(super) enum Output<'a> {
    Number(NumericCell),
    Decimal(ScalarDecimal),
    Text(&'a str, Admission<'a>),
    Value(EvalValue<'a>),
}

impl<'a> Output<'a> {
    pub(super) fn into_value(self) -> Result<EvalValue<'a>> {
        Ok(match self {
            Self::Number(number) => EvalValue::Scalar(number.into_value()),
            Self::Decimal(decimal) => EvalValue::Scalar(decimal.into_cell()?.into_value()),
            Self::Text(text, admission) => {
                admission.reserve(text.len())?;
                scalar(Value::Utf8(text.to_owned()))
            }
            Self::Value(value) => value,
        })
    }

    pub(super) fn text(&self) -> Option<&str> {
        match self {
            Self::Text(text, _) => Some(text),
            Self::Value(EvalValue::Scalar(Value::Utf8(text))) => Some(text),
            _ => None,
        }
    }

    pub(super) fn into_text_or_value(self) -> Result<Self> {
        match self {
            Self::Text(..) => Ok(self),
            output => output.into_value().map(Self::Value),
        }
    }

    pub(super) fn number(&self) -> Option<NumericCell> {
        match self {
            Self::Number(number) => Some(*number),
            Self::Value(EvalValue::Scalar(value)) if nullish(value) => Some(NumericCell::Null),
            Self::Value(EvalValue::Scalar(value)) => NumericCell::from_value(value),
            _ => None,
        }
    }

    pub(super) fn is_null(&self) -> bool {
        match self {
            Self::Value(value) => is_null(value),
            Self::Number(number) => *number == NumericCell::Null,
            Self::Decimal(_) | Self::Text(..) => false,
        }
    }

    pub(super) fn is_decimal(&self) -> bool {
        matches!(self, Self::Decimal(_)) || matches!(self.number(), Some(NumericCell::Decimal(..)))
    }

    pub(super) fn has_wide_decimal(&self) -> bool {
        matches!(self, Self::Decimal(_))
    }

    pub(super) fn scalar_type(&self) -> Option<ScalarType> {
        match self {
            Self::Decimal(decimal) => Some(decimal.scalar_type()),
            _ => self.number().and_then(NumericCell::scalar_type),
        }
    }

    pub(super) fn as_f64(&self, frame: &EvaluationFrame<'_>) -> Result<Option<f64>> {
        match self {
            Self::Decimal(decimal) => decimal.as_f64(frame.context).map(Some),
            _ => Ok(self.number().and_then(NumericCell::as_f64)),
        }
    }

    pub(super) fn into_decimal(self, frame: &EvaluationFrame<'_>) -> Result<ScalarDecimal> {
        match self {
            Self::Decimal(decimal) => Ok(decimal),
            _ => exact(self.number().expect("decimal or integer"), frame),
        }
    }

    pub(super) fn clone_narrow(&self, frame: &EvaluationFrame<'_>) -> Result<EvalValue<'a>> {
        match self {
            Self::Number(number) => {
                if let NumericCell::Decimal(_, params) = number {
                    frame.context.reserve(usize::from(params.bit_width / 8))?;
                }
                Ok(scalar(number.into_value()))
            }
            Self::Value(value) => clone_value(value, frame),
            Self::Text(text, admission) => Self::Text(text, *admission).into_value(),
            Self::Decimal(_) => Err(overflow()),
        }
    }
}

fn exact(number: NumericCell, frame: &EvaluationFrame<'_>) -> Result<ScalarDecimal> {
    frame.context.reserve(256)?;
    match number {
        NumericCell::Decimal(bytes, params) => Ok(ScalarDecimal::from_cell(bytes, params)),
        number => number.integer().map(ScalarDecimal::from_integer).ok_or_else(overflow),
    }
}

fn decimal_order(
    left: &Output<'_>, right: &Output<'_>, frame: &EvaluationFrame<'_>, total: bool,
) -> Result<Option<Ordering>> {
    let (decimal, other, reversed) = match (left, right) {
        (Output::Decimal(left), Output::Decimal(right)) => return left.compare(right, frame.context).map(Some),
        (Output::Decimal(decimal), other) => (decimal, other, false),
        (other, Output::Decimal(decimal)) => (decimal, other, true),
        _ => return Ok(None),
    };
    let number = match (other, total) {
        (Output::Value(EvalValue::Scalar(Value::Oid(value) | Value::RegClass(value) | Value::RegProc(value))), true) => Some(NumericCell::I32(*value)),
        _ => other.number(),
    };
    let Some(number) = number.filter(|number| *number != NumericCell::Null) else { return Ok(None); };
    let order = if matches!(number, NumericCell::Decimal(..)) || number.integer().is_some() {
        decimal.compare(&exact(number, frame)?, frame.context).map(Some)?
    } else {
        let (left, right) = (decimal.as_f64(frame.context)?, number.as_f64().expect("floating-point number"));
        if total { Some(crate::compare::float_order(left, right)) } else { left.partial_cmp(&right) }
    };
    Ok(order.map(|order| if reversed { order.reverse() } else { order }))
}

fn decimal_equal(left: &Output<'_>, right: &Output<'_>, strict: bool, frame: &EvaluationFrame<'_>) -> Result<bool> {
    match (left, right) {
        (Output::Decimal(decimal), Output::Value(EvalValue::Scalar(Value::Transit(bytes))))
        | (Output::Value(EvalValue::Scalar(Value::Transit(bytes))), Output::Decimal(decimal)) => {
            let scope = crate::transit_object::scope(frame.context.allocation)?;
            let stored = crate::transit_object::Decoded::read(bytes, scope)?;
            Ok(stored.equivalent_number(&decimal.number(frame.context)?))
        }
        _ if strict && !(left.is_decimal() && right.is_decimal()) => Ok(false),
        _ => Ok(decimal_order(left, right, frame, false)? == Some(Ordering::Equal)),
    }
}

pub(super) async fn equal<'a>(
    left: &Output<'a>, right: Output<'a>, strict: bool, right_text_literal: bool, frame: &'a EvaluationFrame<'a>,
) -> Result<Option<bool>> {
    if left.has_wide_decimal() || right.has_wide_decimal() {
        if left.is_null() || right.is_null() { return Ok(if strict { Some(false) } else { None }); }
        return decimal_equal(left, &right, strict, frame).map(Some);
    }
    let (mut left, mut right) = (left.clone_narrow(frame)?, right.into_value()?);
    if let (EvalValue::Scalar(left), EvalValue::Scalar(right)) = (&mut left, &mut right) {
        super::super::coerce_comparison_literals(false, right_text_literal, left, right, frame.context.timezone)?;
    }
    compare::equal(left, right, strict, frame)?.await
}

pub(super) async fn order<'a>(left: &Output<'a>, right: &Output<'a>, frame: &'a EvaluationFrame<'a>) -> Result<Ordering> {
    if left.has_wide_decimal() || right.has_wide_decimal() {
        return wide_order(left, right, frame);
    }
    compare::order(left.clone_narrow(frame)?, right.clone_narrow(frame)?, frame)?.await
}

fn wide_order(left: &Output<'_>, right: &Output<'_>, frame: &EvaluationFrame<'_>) -> Result<Ordering> {
    if let Some(order) = decimal_order(left, right, frame, true)? { return Ok(order); }
    let ty = |output: &Output<'_>| match output {
        Output::Value(value) => value_type(value, frame),
        Output::Text(..) => Ok(LogicalType::Scalar(ScalarType::Utf8)),
        _ => Ok(output.scalar_type().map_or(LogicalType::Null, LogicalType::Scalar)),
    };
    Err(rtdb_types::function_type_mismatch("compare", &[ty(left)?, ty(right)?]))
}

pub(super) async fn predicate<'a>(
    op: BinaryOp, left: Output<'a>, right: Output<'a>, left_expr: Option<&Expr>, right_expr: &Expr,
    frame: &'a EvaluationFrame<'a>,
) -> Result<EvalValue<'a>> {
    if let Some(result) = decimal_predicate(op, &left, &right, frame)? {
        return Ok(scalar(boolean(result)));
    }
    let (mut left, mut right) = (left.into_value()?, right.into_value()?);
    if !matches!(op, BinaryOp::IsDistinctFrom | BinaryOp::IsNotDistinctFrom) {
        calls::literal_coercion(left_expr, right_expr, &mut left, &mut right, frame)?;
    }
    calls::binary_values(op, left, right, frame)?.await
}

pub(super) fn decimal_predicate(
    op: BinaryOp, left: &Output<'_>, right: &Output<'_>, frame: &EvaluationFrame<'_>,
) -> Result<Option<Option<bool>>> {
    use BinaryOp as B;
    if left.has_wide_decimal() || right.has_wide_decimal() {
        let result = if left.is_null() || right.is_null() {
            match op {
                B::IsDistinctFrom => Some(left.is_null() != right.is_null()),
                B::IsNotDistinctFrom => Some(left.is_null() == right.is_null()),
                _ => None,
            }
        } else {
            let equal = || decimal_equal(left, right, false, frame);
            Some(match op {
                B::Eq | B::IsNotDistinctFrom => equal()?,
                B::NotEq | B::IsDistinctFrom => !equal()?,
                B::Lt | B::LtEq | B::Gt | B::GtEq => {
                    let unordered = [left.number(), right.number()].into_iter().flatten().any(|cell| matches!(
                        cell, NumericCell::F16(_) | NumericCell::F32(_) | NumericCell::F64(_)
                    ) && cell.as_f64().is_some_and(f64::is_nan));
                    if unordered { return Ok(Some(Some(false))); }
                    let order = wide_order(left, right, frame)?;
                    match op {
                        B::Lt => order == Ordering::Less,
                        B::LtEq => order != Ordering::Greater,
                        B::Gt => order == Ordering::Greater,
                        _ => order != Ordering::Less,
                    }
                }
                _ => return Err(invalid("compare")),
            })
        };
        return Ok(Some(result));
    }
    Ok(None)
}

pub(super) async fn element<'a>(expr: &'a Expr, frame: &'a EvaluationFrame<'a>) -> Result<Option<Output<'a>>> {
    if let Expr::Literal(value) = expr
        && let Some(number) = NumericCell::from_value(value)
    {
        frame.context.reserve(value.retained_bytes()?)?;
        return Ok(Some(Output::Number(number)));
    }
    if let Some(cell) = frame.arrow_input(expr) {
        let cell = cell.physical()?;
        if let Some(span) = rtdb_types::NumericSpan::new(cell.field, cell.array, cell.row..cell.row + 1) {
            if !cell.is_null()? {
                frame
                    .context
                    .reserve(rtdb_columnar::estimate_read_value_bytes(cell.field, cell.array, cell.row)?)?;
            }
            return Ok(Some(Output::Number(span.get(0).unwrap().signed())));
        }
    }
    if let Expr::Call { name, args, return_type } = expr {
        let function = frame.resolved_call(expr, name);
        if function == crate::functions::ResolvedFunction::Known(crate::functions::ScalarFunction::Random)
            && *return_type == LogicalType::Scalar(ScalarType::F64)
            && args.is_empty()
        {
            frame.context.reserve(4096)?;
            return crate::volatile::random().map(|value| Some(Output::Number(NumericCell::F64(value))));
        }
        if function.is_ordinary_call() {
            return scalar::prepare(expr, frame)?.await?.demand_output().await.map(Some);
        }
    }
    if matches!(expr, Expr::Unary { .. } | Expr::Binary { .. } | Expr::Cast { .. } | Expr::Case { .. }
        | Expr::SimpleCase { .. } | Expr::Coalesce(_))
    {
        return scalar::prepare(expr, frame)?.await?.demand_output().await.map(Some);
    }
    Ok(None)
}

pub(super) fn floating(value: f64, target: &ScalarType) -> NumericCell {
    match target {
        ScalarType::F16 => NumericCell::F16(half::f16::from_f64(value).to_bits()),
        ScalarType::F32 => NumericCell::F32(value as f32),
        _ => NumericCell::F64(value),
    }
}

pub(super) async fn arithmetic<'a>(
    op: BinaryOp, lhs: Output<'a>, rhs: Output<'a>, frame: &'a EvaluationFrame<'a>,
) -> Result<Output<'a>> {
    if lhs.is_null() || rhs.is_null() {
        return Ok(Output::Number(NumericCell::Null));
    }
    if (lhs.is_decimal() || rhs.is_decimal())
        && let (Some(left), Some(right)) = (lhs.scalar_type(), rhs.scalar_type())
    {
        let types = [LogicalType::Scalar(left), LogicalType::Scalar(right)];
        let exact = |output: &Output<'_>| output.is_decimal() || output.number().and_then(NumericCell::integer).is_some();
        if exact(&lhs) && exact(&rhs) {
            let a = lhs.into_decimal(frame)?;
            let b = rhs.into_decimal(frame)?;
            let params = if op == BinaryOp::Modulo {
                let Some(LogicalType::Scalar(ScalarType::Decimal { precision, scale, bit_width })) =
                    LogicalType::least_upper_bound(&types) else { return Err(overflow()); };
                DecimalParameters { precision, scale, bit_width }
            } else {
                let (precision, bit_width) = if a.params.precision <= 32 && b.params.precision <= 32 {
                    (32, 128)
                } else {
                    (64, 256)
                };
                let scale = rtdb_plan::decimal_arithmetic_scale(op, a.params.scale, b.params.scale)?;
                DecimalParameters { precision, scale, bit_width }
            };
            return a.arithmetic(op, b, params, frame.context).map(Output::Decimal);
        }
        if let Some(LogicalType::Scalar(ty @ (ScalarType::F16 | ScalarType::F32 | ScalarType::F64))) =
            LogicalType::least_upper_bound(&types)
        {
            let a = lhs.as_f64(frame)?.expect("numeric left operand");
            let b = rhs.as_f64(frame)?.expect("numeric right operand");
            let value = crate::numeric::float_arithmetic(op, a, b).expect("arithmetic operation")?;
            return Ok(Output::Number(floating(value, &ty)));
        }
    }
    if let (Some(a), Some(b)) = (lhs.number(), rhs.number()) {
        if !matches!(a, NumericCell::Decimal(..))
            && !matches!(b, NumericCell::Decimal(..))
            && let Some(LogicalType::Scalar(ty)) = LogicalType::least_upper_bound(&[
                LogicalType::Scalar(a.scalar_type().unwrap()),
                LogicalType::Scalar(b.scalar_type().unwrap()),
            ])
        {
            use crate::numeric::{float_arithmetic, integer_arithmetic, integer_cell};
            if let (Some(a), Some(b)) = (a.integer(), b.integer()) {
                return integer_cell(integer_arithmetic(op, a, b)?, &ty).map(Output::Number);
            }
            let value = float_arithmetic(op, a.as_f64().unwrap(), b.as_f64().unwrap()).expect("arithmetic operation")?;
            return Ok(Output::Number(floating(value, &ty)));
        }
    }
    calls::binary_values(op, lhs.into_value()?, rhs.into_value()?, frame)?.await.map(Output::Value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn decimal_elements_use_the_scalar_coefficient_and_metadata() {
        let context = EvalContext::default();
        let schema = Schema::default();
        let frame = EvaluationFrame { schema: &schema, row: ExpressionRow::Values(&[]), context: &context,
            asynchronous: true, replacements: None };
        let decimal = Value::Decimal(rtdb_types::DecimalValue::new(325_i128.to_be_bytes().to_vec(), 32, 2, 128).unwrap());
        let divisor = Value::I32(2);
        for op in [BinaryOp::Add, BinaryOp::Subtract, BinaryOp::Multiply, BinaryOp::Divide, BinaryOp::Modulo] {
            let expected = crate::arithmetic(op, &decimal, &divisor).unwrap();
            let expression = Expr::Binary { op, left: Box::new(Expr::Literal(decimal.clone())), right: Box::new(Expr::Literal(divisor.clone())) };
            let Some(Output::Decimal(actual)) = element(&expression, &frame).await.unwrap() else {
                panic!("expected a typed decimal result");
            };
            assert_eq!(actual.into_cell().unwrap().into_value(), expected);
        }
    }

    #[tokio::test]
    async fn nested_numeric_operators_and_selected_branches_keep_a_typed_payload() {
        let context = EvalContext::default();
        let schema = Schema::default();
        let frame = EvaluationFrame { schema: &schema, row: ExpressionRow::Values(&[]), context: &context,
            asynchronous: true, replacements: None };
        let binary = |op, left, right| Expr::Binary { op, left: Box::new(left), right: Box::new(right) };
        let divide = binary(BinaryOp::Divide, Expr::Literal(Value::I64(1)), Expr::Literal(Value::I64(0)));
        let add = binary(BinaryOp::Add, Expr::Literal(Value::I32(6)), Expr::Literal(Value::I32(5)));
        let cast = Expr::Cast { expr: Box::new(add), target: CastTarget::Fixed(LogicalType::Scalar(ScalarType::F64)) };
        let negate = Expr::Unary { op: UnaryOp::Negate, expr: Box::new(cast) };
        let selected = Expr::Case { branches: vec![(Expr::Literal(Value::Bool(false)), divide.clone())], otherwise: Box::new(negate) };
        assert!(matches!(element(&selected, &frame).await.unwrap(), Some(Output::Number(NumericCell::F64(-11.0)))));
        let log = Expr::Call {
            name: "log".into(),
            args: vec![Expr::Literal(Value::F64(2.0)), Expr::Literal(Value::F64(8.0))],
            return_type: LogicalType::Scalar(ScalarType::F64),
        };
        assert!(matches!(element(&log, &frame).await.unwrap(), Some(Output::Number(NumericCell::F64(3.0)))));
        let null = binary(BinaryOp::Add, divide, Expr::Literal(Value::Null));
        assert!(matches!(element(&null, &frame).await.unwrap().unwrap().number(), Some(NumericCell::Null)));
    }

    #[tokio::test]
    async fn arithmetic_and_volatile_calls_produce_numeric_components() {
        let context = EvalContext::default();
        let schema = Schema::default();
        let frame = EvaluationFrame {
            schema: &schema,
            row: ExpressionRow::Values(&[]),
            context: &context,
            asynchronous: true,
            replacements: None,
        };
        let add = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Literal(Value::I64(i64::MAX - 1))),
            right: Box::new(Expr::Literal(Value::I64(1))),
        };
        assert!(matches!(element(&add, &frame).await.unwrap(), Some(Output::Number(NumericCell::I64(i64::MAX)))));
        let random =
            Expr::Call { name: "random".into(), args: vec![], return_type: LogicalType::Scalar(ScalarType::F64) };
        let expr = Expr::List(vec![random.clone(), random]);
        let value =
            super::super::execute_async(&expr, &schema, ExpressionRow::Values(&[]), &context).await.unwrap();
        let Some(rtdb_types::ListView::F64(array)) = value.list_view() else {
            panic!("expected float components");
        };
        assert!(array.values().iter().all(|number| (0.0..1.0).contains(number)));
        assert!(!array.values().windows(2).all(|pair| pair[0] == pair[1]));
    }
}
