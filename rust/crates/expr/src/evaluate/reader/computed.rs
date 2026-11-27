use rtdb_types::{NumericCell, ScalarType};

use super::*;

pub(super) enum Output<'a> {
    Number(NumericCell),
    Value(EvalValue<'a>),
}

impl<'a> Output<'a> {
    pub(super) fn into_value(self) -> EvalValue<'a> {
        match self {
            Self::Number(number) => EvalValue::Scalar(number.into_value()),
            Self::Value(value) => value,
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
    if let (Some(a), Some(b)) = (lhs.number(), rhs.number()) {
        if a == NumericCell::Null || b == NumericCell::Null {
            return Ok(Output::Number(NumericCell::Null));
        }
        if let Some(decimal) = decimal_arithmetic(op, a, b) {
            return decimal.map(Output::Number);
        }
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
    calls::binary_values(op, lhs.into_value(), rhs.into_value(), frame)?.await.map(Output::Value)
}

fn decimal_arithmetic(op: BinaryOp, left: NumericCell, right: NumericCell) -> Option<Result<NumericCell>> {
    use rtdb_types::decimal_arrow::DecimalParameters;
    if !matches!(left, NumericCell::Decimal(..)) && !matches!(right, NumericCell::Decimal(..)) {
        return None;
    }
    let parts = |value: NumericCell| match value {
        NumericCell::Decimal(bytes, params) => Some((bytes, params.precision, params.scale)),
        value => {
            let integer = value.integer()?;
            let mut bytes = [if integer < 0 { u8::MAX } else { 0 }; 32];
            bytes[16..].copy_from_slice(&integer.to_be_bytes());
            Some((bytes, 32, 0))
        }
    };
    let (a, ap, ls) = parts(left)?;
    let (b, bp, rs) = parts(right)?;
    Some((|| {
        let params = if op == BinaryOp::Modulo {
            let types = [left, right].map(|cell| LogicalType::Scalar(cell.scalar_type().unwrap()));
            let Some(LogicalType::Scalar(ScalarType::Decimal { precision, scale, bit_width })) =
                LogicalType::least_upper_bound(&types) else { return Err(overflow()); };
            DecimalParameters { precision, scale, bit_width }
        } else {
            let (precision, bit_width) = if ap <= 32 && bp <= 32 { (32, 128) } else { (64, 256) };
            let scale = rtdb_plan::decimal_arithmetic_scale(op, ls, rs)?;
            DecimalParameters { precision, scale, bit_width }
        };
        crate::decimal::arithmetic_cells(op, (a, ls), (b, rs), params)
    })())
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
            let Some(Output::Number(actual)) = element(&expression, &frame).await.unwrap() else {
                panic!("expected a typed decimal result");
            };
            assert_eq!(actual.into_value(), expected);
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
