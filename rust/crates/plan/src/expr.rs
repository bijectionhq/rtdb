use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, Value};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ColumnId(pub u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSpan { pub start: usize, pub end: usize }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOp {
    Add, Subtract, Multiply, Divide, Modulo,
    Eq, NotEq, Lt, LtEq, Gt, GtEq, IsDistinctFrom, IsNotDistinctFrom,
    And, Or, Concat, BitAnd, BitOr, BitXor, ShiftLeft, ShiftRight,
}

pub fn function_type(name: &str, arguments: &[LogicalType]) -> Result<LogicalType> {
    use ScalarType as S;
    let name = name.to_lowercase().replace('-', "_");
    let numeric = || LogicalType::least_upper_bound(arguments).unwrap_or_else(|| LogicalType::join_all(arguments));
    let (ty, arity, nullable) = match name.as_str() {
        "lower" | "upper" => {
            let mut outputs = Vec::new();
            for leg in arguments.first().map(LogicalType::legs).unwrap_or_default() {
                outputs.push(match leg {
                    LogicalType::List { kind: rtdb_types::ListKind::TimestampTzRange, .. } => {
                        let instant = LogicalType::Scalar(S::Timestamp { unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) });
                        if name == "upper" { instant.maybe() } else { instant }
                    }
                    LogicalType::Null => LogicalType::Null,
                    _ => LogicalType::Scalar(S::Utf8),
                });
            }
            (LogicalType::join_all(&outputs), 1..=1, true)
        }
        "str" => (LogicalType::Scalar(S::Utf8), 1..=1, true),
        "trim" | "trim_leading" | "trim_trailing" => (LogicalType::Scalar(S::Utf8), 1..=2, true),
        "substring" | "substr" => (LogicalType::Scalar(S::Utf8), 2..=3, true),
        "replace" => (LogicalType::Scalar(S::Utf8), 3..=3, true),
        "concat" => (LogicalType::Scalar(S::Utf8), 0..=usize::MAX, true),
        "character_length" | "char_length" | "length" | "octet_length" | "bit_length" | "cardinality" => (LogicalType::Scalar(S::I32), 1..=1, true),
        "position" => (LogicalType::Scalar(S::I32), 2..=2, true),
        "abs" | "floor" | "ceil" | "ceiling" => (numeric(), 1..=1, true),
        "round" | "trunc" => (arguments.first().cloned().unwrap_or(LogicalType::Null), 1..=2, true),
        "greatest" | "least" => (LogicalType::join_all(arguments), 1..=usize::MAX, true),
        "power" | "atan2" => (LogicalType::Scalar(S::F64), 2..=2, true),
        "sqrt" | "exp" | "ln" | "sin" | "cos" | "tan" | "asin" | "acos" | "atan" | "degrees" | "radians" => (LogicalType::Scalar(S::F64), 1..=1, true),
        "log" => (LogicalType::Scalar(S::F64), 1..=2, true),
        "current_timestamp" | "now" => (LogicalType::Scalar(S::Timestamp { unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) }), 0..=0, false),
        "localtimestamp" => (LogicalType::Scalar(S::Timestamp { unit: rtdb_types::TimeUnit::Microsecond, timezone: None }), 0..=0, false),
        "current_date" => (LogicalType::Scalar(S::DateDay), 0..=0, false),
        "current_time" | "localtime" => (LogicalType::Scalar(S::Time { unit: rtdb_types::TimeUnit::Microsecond, bit_width: 64 }), 0..=0, false),
        "_iid" => (LogicalType::Scalar(S::FixedSizeBinary(16)), 1..=1, true),
        "keyword" => (LogicalType::Scalar(S::Keyword), 1..=1, true),
        "uuid" => (LogicalType::Scalar(S::Uuid), 1..=1, true),
        "uri" => (LogicalType::Scalar(S::Uri), 1..=1, true),
        "namespace" => (LogicalType::Scalar(S::Utf8).maybe(), 1..=1, true),
        "local_name" => (LogicalType::Scalar(S::Utf8), 1..=1, true),
        "boolean" | "true?" | "false?" | "nil?" | "is_null" | "boolean?" | "number?" | "integer?" | "float?" | "string?" | "keyword?" | "uuid?" | "list?" | "set?" | "map?" => (LogicalType::Scalar(S::Bool), 1..=1, false),
        "null_eq" => (LogicalType::Scalar(S::Bool), 2..=2, false),
        "like_regex" => (LogicalType::Scalar(S::Bool), 2..=3, true),
        "nullif" => (arguments.first().cloned().unwrap_or(LogicalType::Null).maybe(), 2..=2, true),
        "coalesce" => (LogicalType::join_all(arguments), 0..=usize::MAX, false),
        "set" => (LogicalType::List { kind: rtdb_types::ListKind::Set, element: Box::new(LogicalType::join_all(arguments)) }, 0..=usize::MAX, false),
        "period" => (LogicalType::List { kind: rtdb_types::ListKind::TimestampTzRange, element: Box::new(LogicalType::Scalar(S::Timestamp { unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) })) }, 2..=2, false),
        "lower_inf" | "upper_inf" => (LogicalType::Scalar(S::Bool), 1..=1, true),
        "nth" => (arguments.first().map(|ty| {
            let elements: Vec<_> = ty.legs().into_iter().filter_map(|ty| match ty { LogicalType::List { element, .. } => Some(*element), _ => None }).collect();
            LogicalType::join_all(&elements).maybe()
        }).unwrap_or(LogicalType::Null), 2..=2, true),
        _ => return Err(Anomaly::unsupported("xtdb/unknown-function", format!("Unknown scalar function: {name}"))),
    };
    if !arity.contains(&arguments.len()) {
        return Err(Anomaly::incorrect("xtdb/invalid-function-arity", format!("Wrong argument count for {name}: {}", arguments.len())));
    }
    if name == "period" {
        let date_time = |ty: &LogicalType| matches!(ty, LogicalType::Scalar(S::DateDay | S::DateMilli | S::Timestamp { .. }));
        let type_name = |ty: &LogicalType| match ty {
            LogicalType::Null => "null".into(),
            LogicalType::Scalar(S::DateDay | S::DateMilli) => "date".into(),
            LogicalType::Scalar(S::Timestamp { timezone: Some(_), .. }) => "timestamp-tz".into(),
            LogicalType::Scalar(S::Timestamp { timezone: None, .. }) => "timestamp-local".into(),
            LogicalType::Scalar(scalar) => format!("{scalar:?}").to_lowercase(),
            other => format!("{other:?}").to_lowercase(),
        };
        for from in arguments[0].legs() {
            for to in arguments[1].legs() {
                let applicable = if from == LogicalType::Null {
                    matches!(to, LogicalType::Null | LogicalType::Scalar(S::Timestamp { timezone: Some(_), .. }))
                } else {
                    date_time(&from) && (to == LogicalType::Null || date_time(&to))
                };
                if !applicable {
                    return Err(Anomaly::incorrect("xtdb.expression/function-type-mismatch", format!("period not applicable to types {} and {}", type_name(&from), type_name(&to))));
                }
            }
        }
    }
    Ok(if nullable && arguments.iter().any(LogicalType::nullable) { ty.maybe() } else { ty })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOp { Negate, Not, BitNot, IsNull, IsNotNull, IsTrue, IsFalse, IsUnknown }

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    Literal(Value),
    Column { id: ColumnId, data_type: LogicalType },
    Parameter { index: usize, data_type: LogicalType },
    Correlated { depth: usize, id: ColumnId, data_type: LogicalType },
    Unary { op: UnaryOp, expr: Box<Expr> },
    Binary { op: BinaryOp, left: Box<Expr>, right: Box<Expr> },
    Call { name: String, args: Vec<Expr>, return_type: LogicalType },
    Cast { expr: Box<Expr>, data_type: LogicalType },
    Case { branches: Vec<(Expr, Expr)>, otherwise: Box<Expr> },
    Coalesce(Vec<Expr>),
    Field { expr: Box<Expr>, name: String, data_type: LogicalType },
    Subscript { expr: Box<Expr>, index: Box<Expr>, data_type: LogicalType },
    List(Vec<Expr>),
    Struct(Vec<(String, Expr)>),
    In { expr: Box<Expr>, values: Vec<Expr>, negated: bool },
    Like { expr: Box<Expr>, pattern: Box<Expr>, escape: Option<char>, case_insensitive: bool, negated: bool },
}

impl Expr {
    pub fn column(id: ColumnId, data_type: LogicalType) -> Self { Self::Column { id, data_type } }
    pub fn binary(op: BinaryOp, left: Self, right: Self) -> Self { Self::Binary { op, left: Box::new(left), right: Box::new(right) } }
    pub fn unary(op: UnaryOp, expr: Self) -> Self { Self::Unary { op, expr: Box::new(expr) } }

    pub fn data_type(&self) -> LogicalType {
        use BinaryOp as B;
        use UnaryOp as U;
        match self {
            Self::Literal(value) => value.logical_type(),
            Self::Column { data_type, .. } | Self::Parameter { data_type, .. }
            | Self::Correlated { data_type, .. }
            | Self::Field { data_type, .. } | Self::Subscript { data_type, .. } => data_type.clone(),
            Self::Cast { expr, data_type } => {
                let source = expr.data_type();
                if matches!(source, LogicalType::Null | LogicalType::Nothing) { source }
                else if source.nullable() { data_type.clone().maybe() } else { data_type.clone() }
            }
            Self::Call { return_type, .. } => return_type.clone(),
            Self::Unary { op: U::IsNull | U::IsNotNull | U::IsTrue | U::IsFalse | U::IsUnknown, .. } => LogicalType::Scalar(ScalarType::Bool),
            Self::Unary { op: U::Not, expr } => boolean_type(expr.data_type().nullable()),
            Self::Unary { expr, .. } => expr.data_type(),
            Self::Binary { op: B::IsDistinctFrom | B::IsNotDistinctFrom, left, right } => {
                let mut nullable = false;
                for left in left.data_type().legs().into_iter().filter(|ty| *ty != LogicalType::Null) {
                    for right in right.data_type().legs().into_iter().filter(|ty| *ty != LogicalType::Null) {
                        nullable |= equality_nullable(&left, &right);
                    }
                }
                boolean_type(nullable)
            }
            Self::Binary { op: B::Eq | B::NotEq, left, right } => boolean_type(equality_nullable(&left.data_type(), &right.data_type())),
            Self::Binary { op: B::Lt | B::LtEq | B::Gt | B::GtEq | B::And | B::Or, left, right } => boolean_type(left.data_type().nullable() || right.data_type().nullable()),
            Self::In { expr, values, .. } => boolean_type(values.iter().any(|value| equality_nullable(&expr.data_type(), &value.data_type()))),
            Self::Like { expr, pattern, .. } => boolean_type(expr.data_type().nullable() || pattern.data_type().nullable()),
            Self::Binary { op: B::Concat, left, right } => {
                let mut results = Vec::new();
                for left in left.data_type().legs() {
                    for right in right.data_type().legs() {
                        results.push(match (&left, &right) {
                            (LogicalType::Null, _) | (_, LogicalType::Null) => LogicalType::Null,
                            (LogicalType::List { kind: rtdb_types::ListKind::List, element: left }, LogicalType::List { kind: rtdb_types::ListKind::List, element: right }) => LogicalType::list(left.join(right)),
                            _ => LogicalType::Scalar(ScalarType::Utf8),
                        });
                    }
                }
                LogicalType::join_all(&results)
            }
            Self::Binary { op, left, right } => {
                let types = [left.data_type(), right.data_type()];
                let result = arithmetic_type(*op, &types[0], &types[1]);
                if types.iter().any(LogicalType::nullable) { result.maybe() } else { result }
            }
            Self::Case { branches, otherwise } => {
                let types: Vec<_> = branches.iter().map(|(_, value)| value.data_type()).chain([otherwise.data_type()]).collect();
                LogicalType::join_all(&types)
            }
            Self::Coalesce(args) => {
                let types: Vec<_> = args.iter().map(Self::data_type).collect();
                LogicalType::join_all(&types)
            }
            Self::List(args) => {
                let types: Vec<_> = args.iter().map(Self::data_type).collect();
                LogicalType::list(LogicalType::join_all(&types))
            }
            Self::Struct(fields) => LogicalType::Struct(fields.iter().map(|(name, expr)| (name.clone(), expr.data_type())).collect()),
        }
    }

    pub fn children(&self) -> Vec<&Self> {
        match self {
            Self::Unary { expr, .. } | Self::Cast { expr, .. } | Self::Field { expr, .. } => vec![expr],
            Self::Binary { left, right, .. } => vec![left, right],
            Self::Call { args, .. } | Self::Coalesce(args) | Self::List(args) => args.iter().collect(),
            Self::Case { branches, otherwise } => branches.iter().flat_map(|(condition, value)| [condition, value]).chain([otherwise.as_ref()]).collect(),
            Self::Subscript { expr, index, .. } => vec![expr, index],
            Self::Struct(fields) => fields.iter().map(|(_, value)| value).collect(),
            Self::In { expr, values, .. } => std::iter::once(expr.as_ref()).chain(values).collect(),
            Self::Like { expr, pattern, .. } => vec![expr, pattern],
            Self::Literal(_) | Self::Column { .. } | Self::Parameter { .. } | Self::Correlated { .. } => vec![],
        }
    }

    pub fn referenced_columns(&self) -> BTreeSet<ColumnId> {
        let mut columns = BTreeSet::new();
        self.visit(&mut |expr| { if let Self::Column { id, .. } = expr { columns.insert(*id); } });
        columns
    }

    pub fn visit(&self, f: &mut impl FnMut(&Self)) {
        f(self);
        for child in self.children() { child.visit(f); }
    }

    pub fn parameter_count(&self) -> usize {
        let mut count = 0;
        self.visit(&mut |expr| { if let Self::Parameter { index, .. } = expr { count = count.max(index.saturating_add(1)); } });
        count
    }

    pub fn validate_columns(&self, columns: &BTreeSet<ColumnId>) -> Result<()> {
        for missing in self.referenced_columns().difference(columns) {
            return Err(Anomaly::fault("rtdb.plan/unbound-column", format!("Column {} is outside the input schema", missing.0)));
        }
        Ok(())
    }
}

fn boolean_type(nullable: bool) -> LogicalType {
    let ty = LogicalType::Scalar(ScalarType::Bool);
    if nullable { ty.maybe() } else { ty }
}

fn equality_nullable(left: &LogicalType, right: &LogicalType) -> bool {
    if left.nullable() || right.nullable() { return true; }
    left.legs().iter().any(|left| right.legs().iter().any(|right| match (left, right) {
        (LogicalType::Struct(left), LogicalType::Struct(right)) => left.keys().eq(right.keys()),
        (LogicalType::List { .. }, LogicalType::List { .. }) => true,
        _ => false,
    }))
}

pub fn arithmetic_type(op: BinaryOp, left: &LogicalType, right: &LogicalType) -> LogicalType {
    if matches!(op, BinaryOp::ShiftLeft | BinaryOp::ShiftRight) { return left.unencoded(); }
    let mut results = Vec::new();
    for left in left.legs() {
        for right in right.legs() {
            let decimal_parts = |ty: &LogicalType| match ty {
                LogicalType::Scalar(ScalarType::Decimal { precision, scale, .. }) => Some((*precision, *scale)),
                LogicalType::Scalar(ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64) => Some((32, 0)),
                _ => None,
            };
            let has_decimal = [&left, &right].iter().any(|ty| matches!(ty, LogicalType::Scalar(ScalarType::Decimal { .. })));
            let result = if has_decimal {
                if let (Some((lp, ls)), Some((rp, rs))) = (decimal_parts(&left), decimal_parts(&right)) {
                    let precision = if lp <= 32 && rp <= 32 { 32 } else { 64 };
                    let scale = match op {
                        BinaryOp::Multiply => i16::from(ls) + i16::from(rs),
                        BinaryOp::Divide => 6.max(i16::from(ls) + i16::from(rs) + 1),
                        _ => i16::from(ls.max(rs)),
                    };
                    i8::try_from(scale).ok().map(|scale| LogicalType::Scalar(ScalarType::Decimal { precision, scale, bit_width: if precision <= 38 { 128 } else { 256 } }))
                } else { LogicalType::least_upper_bound(&[left.clone(), right.clone()]) }
            } else { LogicalType::least_upper_bound(&[left.clone(), right.clone()]) };
            results.push(result.unwrap_or_else(|| left.join(&right)));
        }
    }
    LogicalType::join_all(&results)
}
