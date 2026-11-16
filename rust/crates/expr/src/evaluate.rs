use crate::{arithmetic_in_timezone, compare, functions::scalar_call, null_eq, sql_eq};
use crate::compare::nullish;
use crate::numeric::{inapplicable, integer, integer_value, number, overflow};
use arrow_array::RecordBatch;
use rtdb_columnar::{MemoryBudget, OwnedBatch, build_array, estimate_builder_bytes, read_value, to_field};
use rtdb_plan::{BinaryOp, Expr, Schema, UnaryOp};
use rtdb_types::{Anomaly, LogicalType, Result, Timestamp, Value};
use std::cmp::Ordering;
use std::sync::Arc;

pub struct OuterRow<'a> { pub schema: &'a Schema, pub values: &'a [Value] }

pub struct EvalContext<'a> {
    pub parameters: &'a [Value],
    pub current_time: Timestamp,
    pub timezone: &'a str,
    pub outer_rows: &'a [OuterRow<'a>],
}

impl Default for EvalContext<'_> {
    fn default() -> Self { Self { parameters: &[], current_time: Timestamp(0), timezone: "UTC", outer_rows: &[] } }
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
}

pub fn is_true(value: &Value) -> Result<bool> {
    match value { Value::Bool(value) => Ok(*value), Value::Null | Value::Undefined => Ok(false), _ => Err(inapplicable("boolean predicate", &[value])) }
}

fn truth(value: &Value) -> Result<Option<bool>> {
    match value { Value::Bool(value) => Ok(Some(*value)), Value::Null | Value::Undefined => Ok(None), _ => Err(inapplicable("boolean", &[value])) }
}
fn boolean(value: Option<bool>) -> Value { value.map(Value::Bool).unwrap_or(Value::Null) }

pub fn evaluate(expr: &Expr, schema: &Schema, row: &[Value], context: &EvalContext<'_>) -> Result<Value> {
    let eval = |expr: &Expr| evaluate(expr, schema, row, context);
    match expr {
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Column { id, .. } => schema.position(*id).and_then(|index| row.get(index)).cloned()
            .ok_or_else(|| Anomaly::fault("rtdb.expr/missing-column", format!("Input does not contain column {}", id.0))),
        Expr::Parameter { index, .. } => context.parameters.get(*index).cloned()
            .ok_or_else(|| Anomaly::incorrect("xtdb/missing-parameter", format!("Missing parameter ${}", index + 1))),
        Expr::Correlated { depth, id, .. } => context.outer_rows.get(*depth)
            .and_then(|outer| outer.schema.position(*id).and_then(|index| outer.values.get(index))).cloned()
            .ok_or_else(|| Anomaly::fault("rtdb.expr/missing-correlated-column", "Correlated row is unavailable")),
        Expr::Unary { op, expr } => {
            let value = eval(expr)?;
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
                    let integer = integer(&value).ok_or_else(|| inapplicable("bit_not", &[&value]))?;
                    let LogicalType::Scalar(ty) = value.logical_type() else { return Err(inapplicable("bit_not", &[&value])); };
                    integer_value(!integer, &ty)
                }
            }
        }
        Expr::Binary { op, left, right } => {
            use BinaryOp as B;
            let left = eval(left)?;
            if *op == B::And && truth(&left)? == Some(false) { return Ok(Value::Bool(false)); }
            if *op == B::Or && truth(&left)? == Some(true) { return Ok(Value::Bool(true)); }
            let right = eval(right)?;
            match op {
                B::And => Ok(boolean(match (truth(&left)?, truth(&right)?) { (Some(false), _) | (_, Some(false)) => Some(false), (Some(true), Some(true)) => Some(true), _ => None })),
                B::Or => Ok(boolean(match (truth(&left)?, truth(&right)?) { (Some(true), _) | (_, Some(true)) => Some(true), (Some(false), Some(false)) => Some(false), _ => None })),
                B::IsDistinctFrom | B::IsNotDistinctFrom => {
                    let equal = if nullish(&left) || nullish(&right) { Some(nullish(&left) && nullish(&right)) }
                        else { sql_eq(&left, &right)? };
                    Ok(boolean(equal.map(|equal| if *op == B::IsDistinctFrom { !equal } else { equal })))
                }
                B::Eq => Ok(boolean(sql_eq(&left, &right)?)),
                B::NotEq => Ok(boolean(sql_eq(&left, &right)?.map(|value| !value))),
                _ if nullish(&left) || nullish(&right) => Ok(Value::Null),
                B::Lt | B::LtEq | B::Gt | B::GtEq => {
                    if number(&left).is_some_and(f64::is_nan) || number(&right).is_some_and(f64::is_nan) { return Ok(Value::Bool(false)); }
                    let order = compare(&left, &right, false)?;
                    Ok(Value::Bool(match op { B::Lt => order == Ordering::Less, B::LtEq => order != Ordering::Greater, B::Gt => order == Ordering::Greater, _ => order != Ordering::Less }))
                }
                B::Concat => match (left, right) {
                    (Value::Utf8(mut a), Value::Utf8(b)) => { a.push_str(&b); Ok(Value::Utf8(a)) }
                    (Value::List(mut a), Value::List(b)) => { a.extend(b); Ok(Value::List(a)) }
                    (a, b) => Err(inapplicable("concat", &[&a, &b])),
                },
                _ => arithmetic_in_timezone(*op, &left, &right, context.timezone),
            }
        }
        Expr::Call { name, args, .. } if name == "coalesce" => {
            for expr in args { let value = eval(expr)?; if !nullish(&value) { return Ok(value); } }
            Ok(Value::Null)
        }
        Expr::Call { name, args, .. } => scalar_call(name, &args.iter().map(eval).collect::<Result<Vec<_>>>()?, context),
        Expr::Cast { expr, target } => crate::cast::cast_target(&eval(expr)?, target, context.timezone),
        Expr::Case { branches, otherwise } => {
            for (condition, value) in branches { if is_true(&eval(condition)?)? { return eval(value); } }
            eval(otherwise)
        }
        Expr::Coalesce(args) => {
            for expr in args { let value = eval(expr)?; if !nullish(&value) { return Ok(value); } }
            Ok(Value::Null)
        }
        Expr::Field { expr, name, .. } => match eval(expr)? {
            Value::Struct(mut fields) => Ok(fields.remove(name).unwrap_or(Value::Null)),
            Value::Null | Value::Undefined => Ok(Value::Null), value => Err(inapplicable("get_field", &[&value])),
        },
        Expr::Subscript { expr, index, .. } => scalar_call("nth", &[eval(expr)?, eval(index)?], context),
        Expr::List(items) => Ok(Value::List(items.iter().map(eval).collect::<Result<Vec<_>>>()?)),
        Expr::Struct(fields) => Ok(Value::Struct(fields.iter().map(|(name, expr)| Ok((name.clone(), eval(expr)?))).collect::<Result<_>>()?)),
        Expr::In { expr, values, negated } => {
            let value = eval(expr)?;
            let mut unknown = false;
            for candidate in values {
                match sql_eq(&value, &eval(candidate)?)? { Some(true) => return Ok(Value::Bool(!*negated)), None => unknown = true, _ => () }
            }
            Ok(if unknown { Value::Null } else { Value::Bool(*negated) })
        }
        Expr::Like { expr, pattern, escape, case_insensitive, negated } => {
            let (value, pattern) = (eval(expr)?, eval(pattern)?);
            if nullish(&value) || nullish(&pattern) { return Ok(Value::Null); }
            let (Value::Utf8(value), Value::Utf8(pattern)) = (&value, &pattern) else { return Err(inapplicable("like", &[&value, &pattern])); };
            let mut regex = String::from(if *case_insensitive { "(?i)^" } else { "^" });
            let mut chars = pattern.chars();
            while let Some(ch) = chars.next() {
                if Some(ch) == *escape {
                    let escaped = chars.next().ok_or_else(|| Anomaly::incorrect("xtdb/invalid-like-pattern", "LIKE pattern ends with its escape character"))?;
                    regex.push_str(&regex::escape(&escaped.to_string()));
                } else { match ch { '%' => regex.push_str(".*"), '_' => regex.push('.'), _ => regex.push_str(&regex::escape(&ch.to_string())) } }
            }
            regex.push_str("\\z");
            let matches = regex::Regex::new(&regex).map_err(|error| Anomaly::incorrect("xtdb/invalid-regex", error.to_string()))?.is_match(value);
            Ok(Value::Bool(matches != *negated))
        }
    }
}

/// The input batch remains borrowed; returned Arrow buffers retain their budget reservation.
pub fn evaluate_batch(expr: &CompiledExpr, batch: &RecordBatch, context: &EvalContext<'_>, budget: &Arc<MemoryBudget>) -> Result<OwnedBatch> {
    if batch.num_columns() != expr.schema.len() { return Err(Anomaly::fault("rtdb.expr/input-schema", "Batch width differs from bound expression input")); }
    for (actual, expected) in batch.schema().fields().iter().zip(expr.schema.fields()) {
        if rtdb_columnar::from_field(actual)?.unencoded() != expected.data_type.unencoded() {
            return Err(Anomaly::fault("rtdb.expr/input-schema", format!("Input type differs from bound column {}", expected.name)));
        }
    }
    let mut values = Vec::with_capacity(batch.num_rows());
    let mut scratch = Vec::new();
    for index in 0..batch.num_rows() {
        let row = batch.columns().iter().zip(batch.schema().fields()).map(|(array, field)| read_value(field, array.as_ref(), index)).collect::<Result<Vec<_>>>()?;
        let value = expr.evaluate(&row, context)?;
        scratch.push(budget.reserve(value.retained_bytes()?)?);
        values.push(value);
    }
    let logical = expr.data_type();
    let reservation = budget.reserve(estimate_builder_bytes(&logical, &values)?)?;
    let array = build_array(&logical, &values)?;
    let schema = Arc::new(arrow_schema::Schema::new(vec![to_field("result", &logical)?]));
    let batch = RecordBatch::try_new(schema, vec![array]).map_err(|error| Anomaly::fault("rtdb.expr/output-schema", error.to_string()))?;
    OwnedBatch::with_reservation(batch, reservation)
}
