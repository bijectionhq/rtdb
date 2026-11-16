use crate::Expr;
use rtdb_types::{Anomaly, Result, Value};

/// Borrowed PatchGaps endpoint syntax; parameter values are resolved by the execution owner.
#[derive(Clone, Copy, Debug)]
pub enum PatchEndpoint<'a> {
    Default,
    CurrentTime,
    DateTime(&'a Value),
    Parameter(usize),
}

impl<'a> PatchEndpoint<'a> {
    /// Accepts an omitted/null option, `:now`, a zero-argument current-timestamp call, a datetime literal or a direct parameter.
    /// Other expressions return `rtdb.plan/patch-endpoint` before any payload is evaluated.
    /// The descriptor borrows literals from `expression`; no allocation or parameter type conversion occurs.
    pub fn from_expr(expression: &'a Expr) -> Result<Self> {
        match expression {
            Expr::Literal(Value::Null) => Ok(Self::Default),
            Expr::Literal(Value::Keyword(value)) if value.as_str() == "now" => Ok(Self::CurrentTime),
            Expr::Literal(value @ (Value::Date(_) | Value::DateDay(_) | Value::DateMilli(_) | Value::Timestamp { .. })) => Ok(Self::DateTime(value)),
            Expr::Parameter { index,.. } => Ok(Self::Parameter(*index)),
            Expr::Call { name,args,.. } if matches!(name.as_str(),"current_timestamp" | "current-timestamp") && args.is_empty() => Ok(Self::CurrentTime),
            _ => Err(Anomaly::incorrect("rtdb.plan/patch-endpoint", "Patch gap endpoints require a datetime literal, parameter or current timestamp")),
        }
    }
}
