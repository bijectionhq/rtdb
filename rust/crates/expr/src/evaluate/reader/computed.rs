use rtdb_types::{NumericCell, ScalarType};

use super::*;

pub(super) enum Element {
    Number(NumericCell),
    Scalar(Value),
}

pub(super) async fn element(expr: &Expr, frame: &EvaluationFrame<'_>) -> Result<Option<Element>> {
    if let Expr::Literal(value) = expr
        && let Some(number) = NumericCell::from_value(value)
    {
        frame.context.reserve(value.retained_bytes()?)?;
        return Ok(Some(Element::Number(number)));
    }
    if let Some(cell) = frame.arrow_input(expr) {
        let cell = cell.physical()?;
        if let Some(span) = rtdb_types::NumericSpan::new(cell.field, cell.array, cell.row..cell.row + 1) {
            if !cell.is_null()? {
                frame
                    .context
                    .reserve(rtdb_columnar::estimate_read_value_bytes(cell.field, cell.array, cell.row)?)?;
            }
            return Ok(Some(Element::Number(span.get(0).unwrap().signed())));
        }
    }
    if let Expr::Call { name, args, return_type: LogicalType::Scalar(ScalarType::F64) } = expr
        && name.eq_ignore_ascii_case("random")
        && args.is_empty()
    {
        frame.context.reserve(4096)?;
        return crate::volatile::random().map(|value| Some(Element::Number(NumericCell::F64(value))));
    }
    let Expr::Binary {
        op: op @ (BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide | BinaryOp::Modulo),
        left,
        right,
    } = expr
    else {
        return Ok(None);
    };
    frame.context.reserve(expression_type_bound(expr)?.checked_mul(8).ok_or_else(overflow)?)?;
    scalar::emitted_type(expr, frame)?;
    let lhs = scalar::prepare(left, frame)?.await?;
    if lhs.is_null() {
        return Ok(Some(Element::Number(NumericCell::Null)));
    }
    let rhs = scalar::prepare(right, frame)?.await?;
    if rhs.is_null() {
        return Ok(Some(Element::Number(NumericCell::Null)));
    }

    let lhs = lhs.demand().await?;
    let rhs = rhs.demand().await?;
    if let (EvalValue::Scalar(a), EvalValue::Scalar(b)) = (&lhs, &rhs) {
        if nullish(a) || nullish(b) {
            return Ok(Some(Element::Number(NumericCell::Null)));
        }
        if let (Some(a), Some(b)) = (NumericCell::from_value(a), NumericCell::from_value(b))
            && !matches!(a, NumericCell::Decimal(..))
            && !matches!(b, NumericCell::Decimal(..))
            && let Some(LogicalType::Scalar(ty)) = LogicalType::least_upper_bound(&[
                LogicalType::Scalar(a.scalar_type().unwrap()),
                LogicalType::Scalar(b.scalar_type().unwrap()),
            ])
        {
            use crate::numeric::{float_arithmetic, integer_arithmetic, integer_cell};
            if let (Some(a), Some(b)) = (a.integer(), b.integer()) {
                return integer_cell(integer_arithmetic(*op, a, b)?, &ty)
                    .map(|value| Some(Element::Number(value)));
            }
            let result =
                float_arithmetic(*op, a.as_f64().unwrap(), b.as_f64().unwrap()).expect("arithmetic operation")?;
            return Ok(Some(Element::Number(match ty {
                ScalarType::F16 => NumericCell::F16(half::f16::from_f64(result).to_bits()),
                ScalarType::F32 => NumericCell::F32(result as f32),
                _ => NumericCell::F64(result),
            })));
        }
    }

    let value = materialize(box_cell(calls::binary_values(*op, lhs, rhs, frame)?.await?)?, frame)?.await?;
    Ok(Some(Element::Scalar(value)))
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(matches!(element(&add, &frame).await.unwrap(), Some(Element::Number(NumericCell::I64(i64::MAX)))));
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
