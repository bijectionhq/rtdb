use rtdb_plan::{CastTarget, Expr};
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, Value};
use sqlparser::ast;

#[derive(Clone, Copy)]
pub(crate) enum JsonAccess { Field, FieldText, Path, PathText }
enum PathStep { Field(String), Index(i64) }
impl JsonAccess {
    pub(crate) fn operator(operator: &ast::BinaryOperator) -> Option<Self> {
        Some(match operator {
            ast::BinaryOperator::Arrow => Self::Field,ast::BinaryOperator::LongArrow => Self::FieldText,
            ast::BinaryOperator::HashArrow => Self::Path,ast::BinaryOperator::HashLongArrow => Self::PathText,_ => return None,
        })
    }
    pub(crate) fn validate(self,argument: &ast::Expr) -> Result<()> { self.steps(argument).map(|_| ()) }
    pub(crate) fn lower(self,mut object: Expr,argument: &ast::Expr) -> Result<Expr> {
        for step in self.steps(argument)? {
            object = match step {
                PathStep::Field(name) => {
                    let data_type = rtdb_plan::field_type(&object.data_type(),&name);
                    Expr::Field { expr: Box::new(object),name,data_type }
                }
                PathStep::Index(index) => {
                    let index = Expr::Literal(Value::I64(index));
                    let data_type = rtdb_plan::function_type("nth",&[object.data_type(),index.data_type()])?;
                    Expr::Subscript { expr: Box::new(object),index: Box::new(index),data_type }
                }
            };
        }
        Ok(if matches!(self,Self::FieldText | Self::PathText) { Expr::Cast { expr: Box::new(object),target: CastTarget::fixed(LogicalType::Scalar(ScalarType::Utf8)) } } else { object })
    }
    fn steps(self,argument: &ast::Expr) -> Result<Vec<PathStep>> {
        if matches!(self,Self::Field | Self::FieldText) { return Ok(vec![step(argument)?]); }
        let ast::Expr::Array(array) = unwrap(argument) else {
            return Err(Anomaly::unsupported("xtdb.sql/non-literal-json-path",format!("PostgreSQL {} operator currently only supports literal array paths",if matches!(self,Self::Path) { "#>" } else { "#>>" })));
        };
        array.elem.iter().map(step).collect()
    }
}
fn unwrap(mut expression: &ast::Expr) -> &ast::Expr {
    while let ast::Expr::Nested(inner) | ast::Expr::UnaryOp { op: ast::UnaryOperator::Plus,expr: inner } = expression { expression = inner; }
    expression
}
fn step(expression: &ast::Expr) -> Result<PathStep> {
    let expression = unwrap(expression);
    if let ast::Expr::Value(value) = expression {
        if matches!(value.value,ast::Value::SingleQuotedString(_) | ast::Value::EscapedStringLiteral(_) | ast::Value::DollarQuotedString(_)) {
            let Value::Utf8(value) = crate::types::literal(&value.value)? else { unreachable!() };
            return Ok(PathStep::Field(value));
        }
    }
    integer(expression)?.map(PathStep::Index).ok_or_else(nonliteral)
}
fn integer(expression: &ast::Expr) -> Result<Option<i64>> {
    let expression = unwrap(expression);
    if let ast::Expr::UnaryOp { op: ast::UnaryOperator::Minus,expr } = expression {
        if matches!(expr.as_ref(),ast::Expr::Value(value) if matches!(&value.value,ast::Value::Number(value,_) if value == "9223372036854775808")) { return Ok(Some(i64::MIN)); }
        return integer(expr)?.map(|value| value.checked_neg().ok_or_else(||crate::bind::error("Integer literal exceeds signed 64 bits"))).transpose();
    }
    if let ast::Expr::Value(value) = expression {
        if matches!(value.value,ast::Value::Number(..)) {
            return Ok(match crate::types::literal(&value.value)? { Value::I64(value) => Some(value),_ => None });
        }
    }
    Ok(None)
}
fn nonliteral() -> Anomaly { Anomaly::unsupported("xtdb.sql/non-literal-field-access","PostgreSQL JSON operators currently only support string/integer literal field names") }
