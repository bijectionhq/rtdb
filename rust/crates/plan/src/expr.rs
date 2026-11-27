use rtdb_types::{OrderedSet, StructFields};
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
    if crate::equality::call(&name) && arguments.len() == 2 { crate::validate_equality_types(&arguments[0], &arguments[1])?; }
    if name == "_patch" { return patch_type(arguments); }
    if name == "get_field" {
        if arguments.len() != 1 { return Err(Anomaly::incorrect("xtdb/invalid-function-arity", "get_field requires one argument")); }
        return Ok(field_type(&arguments[0], ""));
    }
    if let Some(result) = crate::comparison::signature(&name, arguments) { return result; }
    if let Some(result) = crate::period::predicate_type(&name, arguments) { return result; }
    if let Some(result) = crate::search::signature(&name, arguments) { return result; }
    if matches!(name.as_str(), "cosine_distance" | "cosine_distance_f32") {
        if arguments.len() != 2 {
            return Err(Anomaly::incorrect("xtdb/invalid-function-arity", format!("{name} requires two arguments")));
        }
        return Ok(LogicalType::Scalar(S::F64).maybe());
    }
    if name == "entity_sort_key" {
        if arguments.len() != 1 {
            return Err(Anomaly::incorrect("xtdb/invalid-function-arity", "entity_sort_key requires one argument"));
        }
        return Ok(LogicalType::Scalar(S::Binary).maybe());
    }
    if name == "rank_admitted_count" {
        if arguments.len() != 2 {
            return Err(Anomaly::incorrect("xtdb/invalid-function-arity", "rank_admitted_count requires two arguments"));
        }
        return Ok(LogicalType::Scalar(S::I64));
    }
    if matches!(name.as_str(), "spatial_match" | "spatial_distance") {
        let (arity, ty) = if name == "spatial_match" { (4..=5, S::Bool) } else { (4..=6, S::F64) };
        if !arity.contains(&arguments.len()) {
            return Err(Anomaly::incorrect("xtdb/invalid-function-arity", format!("Wrong argument count for {name}")));
        }
        return Ok(LogicalType::Scalar(ty).maybe());
    }
    if name == "approximate_nearest" {
        if arguments.len() != 3 {
            return Err(Anomaly::incorrect("xtdb/invalid-function-arity", "approximate_nearest requires a column, a probe vector and a limit"));
        }
        return Ok(LogicalType::Scalar(S::Bool));
    }
    if let Some(result) = crate::string::signature(&name, arguments) { return result; }
    if let Some(result) = crate::regex::signature(&name, arguments) { return result; }
    if let Some(result) = crate::session_info::signature(&name, arguments) { return result; }
    if let Some(result) = crate::collection::signature(&name, arguments) { return result; }
    if let Some(result) = crate::bitwise::signature(&name, arguments) { return result; }
    if let Some(result) = crate::series::signature(&name, arguments) { return result; }
    if let Some(result) = crate::temporal_functions::signature(&name, arguments) { return result; }
    if let Some(result) = crate::interval_constructor::signature(&name, arguments) { return result; }
    if name == "epoch_nanos_be" {
        let [timestamp] = arguments else {
            return Err(Anomaly::incorrect("xtdb/invalid-function-arity", "EPOCH_NANOS_BE takes one timestamp"));
        };
        let mut output = LogicalType::Nothing;
        for leg in timestamp.legs() {
            output = output.join(&match leg {
                LogicalType::Null => LogicalType::Null,
                LogicalType::Scalar(S::Timestamp { timezone: Some(_), .. }) => LogicalType::Scalar(S::Binary),
                other => return Err(rtdb_types::function_type_mismatch(&name, [&other])),
            });
        }
        return Ok(output);
    }
    if name == "_meta_double" {
        if arguments.len() != 1 { return Err(Anomaly::incorrect("xtdb/invalid-function-arity", "_meta_double requires one argument")); }
        if arguments[0].legs().contains(&LogicalType::Scalar(S::F16)) { return Err(Anomaly::unsupported("rtdb/jvm-half-float", "The pinned JVM vector factory rejects half-precision floats")); }
        return Ok(LogicalType::join_all(&arguments[0].legs().into_iter().map(|ty| match ty {
            LogicalType::Scalar(S::I8 | S::I16 | S::I32 | S::I64 | S::U8 | S::U16 | S::U32 | S::U64 | S::F32 | S::F64 | S::Decimal { .. }
                | S::DateDay | S::DateMilli | S::Timestamp { .. } | S::Time { .. } | S::Duration(_)) => LogicalType::Scalar(S::F64),
            _ => LogicalType::Null,
        }).collect::<Vec<_>>()));
    }
    if name == "distinct_from" {
        if arguments.len() != 2 { return Err(Anomaly::incorrect("xtdb/invalid-function-arity", "distinct_from requires two arguments")); }
        return Ok(Expr::binary(BinaryOp::IsDistinctFrom, Expr::Parameter { index: 0, data_type: arguments[0].clone() }, Expr::Parameter { index: 1, data_type: arguments[1].clone() }).data_type());
    }
    if matches!(name.as_str(), "uri_scheme" | "uri_user_info" | "uri_host" | "uri_port" | "uri_path" | "uri_query" | "uri_fragment") {
        if arguments.len() != 1 { return Err(Anomaly::incorrect("xtdb/invalid-function-arity", format!("Wrong argument count for {name}: {}", arguments.len()))); }
        let mut result = LogicalType::Nothing;
        for leg in arguments[0].legs() {
            result = result.join(&match leg {
                LogicalType::Null => LogicalType::Null,
                LogicalType::Scalar(S::Uri) => LogicalType::Scalar(if name == "uri_port" { S::I32 } else { S::Utf8 }).maybe(),
                _ => return Err(Anomaly::incorrect("xtdb.expression/function-type-mismatch", format!("{name} requires a URI"))),
            });
        }
        return Ok(result);
    }
    if matches!(name.as_str(), "log10" | "sinh" | "cosh" | "tanh" | "double" | "mod") {
        let arity = if name == "mod" { 2 } else { 1 };
        if arguments.len() != arity { return Err(Anomaly::incorrect("xtdb/invalid-function-arity", format!("Wrong argument count for {name}: {}", arguments.len()))); }
        let numeric = |ty: &LogicalType| matches!(ty, LogicalType::Scalar(S::I8 | S::I16 | S::I32 | S::I64 | S::U8 | S::U16 | S::U32 | S::U64 | S::F32 | S::F64 | S::Decimal { .. }));
        let mut output = LogicalType::Nothing;
        for first in arguments[0].legs() {
            for second in if arity == 2 { arguments[1].legs() } else { OrderedSet::from([LogicalType::Scalar(S::I64)]) } {
                let ty = if first == LogicalType::Null || second == LogicalType::Null { LogicalType::Null }
                    else if numeric(&first) && numeric(&second) {
                        if name == "mod" { LogicalType::least_upper_bound(&[first.clone(), second]).ok_or_else(|| Anomaly::incorrect("xtdb.expression/function-type-mismatch", "Incomparable MOD types"))? }
                        else { LogicalType::Scalar(S::F64) }
                    } else { return Err(rtdb_types::function_type_mismatch(&name, if arity == 2 { vec![&first, &second] } else { vec![&first] })); };
                output = output.join(&ty);
            }
        }
        return Ok(output);
    }
    if matches!(name.as_str(), "col_description" | "pg_indexam_has_property") {
        if arguments.len() != 2 { return Err(Anomaly::incorrect("xtdb/invalid-function-arity", format!("Wrong argument count for {name}: {}", arguments.len()))); }
        let signed = |ty: &LogicalType| matches!(ty, LogicalType::Scalar(S::I8 | S::I16 | S::I32 | S::I64));
        for first in arguments[0].legs() {
            for second in arguments[1].legs() {
                if first == LogicalType::Null || second == LogicalType::Null { continue; }
                let valid = if name == "col_description" { (signed(&first) || first == LogicalType::Scalar(S::Oid)) && signed(&second) }
                    else { signed(&first) && second == LogicalType::Scalar(S::Utf8) };
                if !valid { return Err(rtdb_types::function_type_mismatch(&name, [&first, &second])); }
            }
        }
        return Ok(LogicalType::Null);
    }
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
                    LogicalType::Scalar(S::Utf8) => LogicalType::Scalar(S::Utf8),
                    other => return Err(rtdb_types::function_type_mismatch(&name, [&other])),
                });
            }
            (LogicalType::join_all(&outputs), 1..=1, true)
        }
        "format_type" => (LogicalType::Scalar(S::Utf8), 2..=2, false),
        "length" | "cardinality" => (LogicalType::Scalar(S::I32), 1..=1, true),
        "abs" => (LogicalType::join_all(arguments), 1..=1, true),
        "floor" | "ceil" | "ceiling" => (LogicalType::Scalar(S::F64), 1..=1, true),
        "round" => {
            let ty = if arguments.len() == 2 && arguments.iter().all(|ty| !ty.legs().is_empty()) && arguments.iter().any(is_null_only) {
                LogicalType::Null
            } else {
                round_type(arguments.first().unwrap_or(&LogicalType::Nothing), None, arguments.len() == 2)?
            };
            (ty, 1..=2, true)
        }
        "greatest" | "least" => (if arguments.is_empty() { LogicalType::Null } else { LogicalType::join_all(arguments) }, 0..=usize::MAX, true),
        "power" => (LogicalType::Scalar(S::F64), 2..=2, true),
        "sqrt" | "exp" | "ln" | "sin" | "cos" | "tan" | "asin" | "acos" | "atan" => (LogicalType::Scalar(S::F64), 1..=1, true),
        "log" => (LogicalType::Scalar(S::F64), 2..=2, true),
        "random" => (LogicalType::Scalar(S::F64), 0..=0, false),
        "sleep" => (LogicalType::Null, 1..=1, true),
        "snapshot_token" => (LogicalType::Scalar(S::Utf8).maybe(), 0..=0, false),
        "_iid" => (LogicalType::Scalar(S::FixedSizeBinary(16)), 1..=1, true),
        "keyword" => (LogicalType::Scalar(S::Keyword), 1..=1, true),
        "namespace" => (LogicalType::Scalar(S::Utf8).maybe(), 1..=1, true),
        "local_name" => (LogicalType::Scalar(S::Utf8), 1..=1, true),
        "boolean" | "true?" | "false?" | "nil?" | "is_true" | "is_false" | "is_null" => (LogicalType::Scalar(S::Bool), 1..=1, false),
        "null_eq" | "===" => (LogicalType::Scalar(S::Bool), 2..=2, false),
        "nullif" => (arguments.first().cloned().unwrap_or(LogicalType::Null).maybe(), 2..=2, false),
        "coalesce" => (LogicalType::join_all(arguments), 0..=usize::MAX, false),
        "set" => (LogicalType::List { kind: rtdb_types::ListKind::Set, element: Box::new(LogicalType::join_all(arguments)) }, 0..=usize::MAX, false),
        "period" => (LogicalType::List { kind: rtdb_types::ListKind::TimestampTzRange, element: Box::new(LogicalType::Scalar(S::Timestamp { unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) })) }, 2..=2, false),
        "lower_inf" | "upper_inf" => (LogicalType::Scalar(S::Bool), 1..=1, true),
        _ => return if arguments.iter().any(|ty| ty.legs().is_empty()) { Ok(LogicalType::Nothing) }
            else if arguments.iter().any(|ty| ty.legs() == OrderedSet::from([LogicalType::Null])) { Ok(LogicalType::Null) }
            else { Err(Anomaly::incorrect("xtdb.expression/function-type-mismatch", format!("{name} not applicable to argument types"))) },
    };
    if !arity.contains(&arguments.len()) {
        return Err(Anomaly::incorrect("xtdb/invalid-function-arity", format!("Wrong argument count for {name}: {}", arguments.len())));
    }
    if name == "sleep" && arguments[0].legs().iter().any(|ty| !matches!(ty, LogicalType::Nothing | LogicalType::Null | LogicalType::Scalar(S::Duration(_)))) {
        return Err(Anomaly::incorrect("xtdb.expression/function-type-mismatch", "sleep requires a duration"));
    }
    if name == "format_type" {
        for (index, ty) in arguments.iter().enumerate() {
            if ty.legs().iter().any(|leg| !matches!(leg, LogicalType::Null | LogicalType::Scalar(S::I8 | S::I16 | S::I32 | S::I64)) && !(index == 0 && *leg == LogicalType::Scalar(S::Oid))) {
                return Err(Anomaly::incorrect("xtdb.expression/function-type-mismatch", "format_type requires an integer or OID and an integer modifier"));
            }
        }
        return Ok(if arguments[0] == LogicalType::Null { LogicalType::Null } else if arguments[0].nullable() { ty.maybe() } else { ty });
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
    if nullable && !arguments.iter().any(|ty| ty.legs().is_empty()) && arguments.iter().any(is_null_only) {
        return Ok(LogicalType::Null);
    }
    Ok(if nullable && arguments.iter().any(LogicalType::nullable) { ty.maybe() } else { ty })
}

fn patch_type(arguments: &[LogicalType]) -> Result<LogicalType> {
    if arguments.len() != 2 {
        return Err(Anomaly::incorrect("xtdb/invalid-function-arity", format!("Wrong argument count for _patch: {}", arguments.len())));
    }
    let mut results = Vec::new();
    for left in arguments[0].legs() {
        for right in arguments[1].legs() {
            let LogicalType::Struct(right) = right else {
                return Err(Anomaly::incorrect("xtdb.expression/function-type-mismatch", "_patch requires a struct on the right"));
            };
            let mut fields = match &left {
                LogicalType::Null => StructFields::new(),
                LogicalType::Struct(fields) => fields.clone(),
                _ => return Err(Anomaly::incorrect("xtdb.expression/function-type-mismatch", "_patch requires a struct or null on the left")),
            };
            for (name, right_type) in right {
                let field_type = if right_type.nullable() {
                    let mut legs = right_type.legs();
                    legs.remove(&LogicalType::Null);
                    legs.extend(fields.get(&name).unwrap_or(&LogicalType::Null).legs());
                    LogicalType::from_legs(legs)
                } else { right_type };
                fields.insert(name, field_type);
            }
            results.push(LogicalType::Struct(fields));
        }
    }
    Ok(LogicalType::join_all(&results))
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
    /// Keeps raw replacement metadata outside the source expression's argument evaluation.
    RegexReplace { source: Box<Expr>,recipe: crate::RegexReplacement,return_type: LogicalType },
    Cast { expr: Box<Expr>, target: crate::CastTarget },
    Case { branches: Vec<(Expr, Expr)>, otherwise: Box<Expr> },
    /// Evaluates the operand once before testing ordered equality alternatives, including a null operand.
    SimpleCase { operand: Box<Expr>, branches: Vec<(Expr, Expr)>, otherwise: Box<Expr> },
    /// Binds a complex operand once before short-circuiting the ordered bound checks.
    Between { operand: Box<Expr>,low: Box<Expr>,high: Box<Expr>,symmetric: bool },
    Coalesce(Vec<Expr>),
    Field { expr: Box<Expr>, name: String, data_type: LogicalType },
    Subscript { expr: Box<Expr>, index: Box<Expr>, data_type: LogicalType },
    List(Vec<Expr>),
    Struct(Vec<(String, Expr)>),
    /// Packs selected input columns without evaluating or rewriting their cells.
    /// Column IDs must exist in the input, and field names must be unique within the record.
    RowCopy(Vec<crate::Field>),
    In { expr: Box<Expr>, values: Vec<Expr>, negated: bool },
    Like { expr: Box<Expr>, pattern: Box<Expr>, escape: Option<char>, case_insensitive: bool, negated: bool },
    /// Keeps the source duration's signed-second range until a consumer demands an Arrow value.
    DurationLiteral(rtdb_types::DurationLiteral),
}

impl Expr {
    /// Retains raw replacement metadata with its declared source type; metadata is admitted when the expression is emitted.
    pub fn regex_replace(source: Self,recipe: crate::RegexReplacement) -> Self {
        let return_type = recipe.declared_type(&source.data_type());
        Self::RegexReplace { source: Box::new(source),recipe,return_type }
    }
    /// Reports whether an evaluated branch can suspend; unused branches still determine this capability.
    pub fn requires_suspension(&self) -> bool {
        let mut required = false;
        self.visit(&mut |expr| { if let Self::Call { name, .. } = expr { required |= name.eq_ignore_ascii_case("sleep"); } });
        required
    }
    /// Reports calls whose repeated evaluation can change their result or produce a delay.
    /// Submission-time expansion and independently evaluated pruning keys must not pre-evaluate these calls.
    pub fn is_volatile(&self) -> bool {
        let mut volatile = false;
        self.visit(&mut |expr| { if let Self::Call { name, .. } = expr { volatile |= name.eq_ignore_ascii_case("random") || name.eq_ignore_ascii_case("sleep"); } });
        volatile
    }
    pub fn requires_current_time(&self) -> bool {
        let mut required = false;
        self.visit(&mut |expr| match expr {
            Self::Cast { expr, target } => required |= target.requires_current_time(&expr.data_type()),
            Self::Call { name, .. } => required |= crate::temporal_functions::clock(name),
            _ => (),
        });
        required
    }
    pub fn column(id: ColumnId, data_type: LogicalType) -> Self { Self::Column { id, data_type } }
    pub fn binary(op: BinaryOp, left: Self, right: Self) -> Self { Self::Binary { op, left: Box::new(left), right: Box::new(right) } }
    pub fn unary(op: UnaryOp, expr: Self) -> Self { Self::Unary { op, expr: Box::new(expr) } }

    /// Lowers BETWEEN, retaining inline comparisons for literal and input references used by pruning.
    /// Other operands are bound exactly once, including null-producing operands.
    pub fn between(operand: Self,low: Self,high: Self,symmetric: bool) -> Self {
        if !matches!(operand,Self::Literal(_) | Self::Column {..} | Self::Parameter {..} | Self::Correlated {..}) {
            return Self::Between { operand: Box::new(operand),low: Box::new(low),high: Box::new(high),symmetric };
        }
        let forward = Self::binary(BinaryOp::And,Self::binary(BinaryOp::GtEq,operand.clone(),low.clone()),Self::binary(BinaryOp::LtEq,operand.clone(),high.clone()));
        if symmetric { Self::binary(BinaryOp::Or,forward,Self::binary(BinaryOp::And,Self::binary(BinaryOp::GtEq,operand.clone(),high),Self::binary(BinaryOp::LtEq,operand,low))) }
        else { forward }
    }

    pub fn data_type(&self) -> LogicalType {
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || self.data_type_at())
    }
    fn data_type_at(&self) -> LogicalType {
        use BinaryOp as B;
        use UnaryOp as U;
        match self {
            Self::Literal(value) => value.logical_type(),
            Self::DurationLiteral(_) => LogicalType::Scalar(ScalarType::Duration(rtdb_types::TimeUnit::Microsecond)),
            Self::Column { data_type, .. } | Self::Parameter { data_type, .. }
            | Self::Correlated { data_type, .. }
            | Self::Field { data_type, .. } => data_type.clone(),
            Self::Subscript { expr, index, .. } => function_type("nth", &[expr.data_type(), index.data_type()]).unwrap_or(LogicalType::Nothing),
            Self::Cast { expr, target } => target.result_type(&expr.data_type()),
            Self::Call { return_type, .. } | Self::RegexReplace { return_type,.. } => return_type.clone(),
            Self::Unary { op: U::IsNull | U::IsNotNull | U::IsTrue | U::IsFalse | U::IsUnknown, .. } => LogicalType::Scalar(ScalarType::Bool),
            Self::Unary { op: U::Not, expr } => {
                let ty = expr.data_type();
                if is_null_only(&ty) { LogicalType::Null } else { boolean_type(ty.nullable()) }
            }
            Self::Unary { op: U::BitNot, expr } => crate::bitwise::signature("bit_not", &[expr.data_type()]).unwrap().unwrap_or(LogicalType::Nothing),
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
            Self::Binary { op: B::Eq | B::NotEq, left, right } => {
                let (left, right) = (left.data_type(), right.data_type());
                if is_null_only(&left) || is_null_only(&right) { LogicalType::Null }
                else { boolean_type(equality_nullable(&left, &right)) }
            }
            Self::Binary { op: B::Lt | B::LtEq | B::Gt | B::GtEq, left, right } => {
                let (left, right) = (left.data_type(), right.data_type());
                if is_null_only(&left) || is_null_only(&right) { LogicalType::Null }
                else { boolean_type(left.nullable() || right.nullable()) }
            }
            Self::Binary { op: B::And | B::Or, left, right } => boolean_type(left.data_type().nullable() || right.data_type().nullable()),
            Self::In { expr, values, .. } => boolean_type(values.iter().any(|value| equality_nullable(&expr.data_type(), &value.data_type()))),
            Self::Like { expr, pattern, case_insensitive,.. } => function_type(if *case_insensitive { "ilike" } else { "like" }, &[expr.data_type(),pattern.data_type()]).unwrap_or(LogicalType::Nothing),
            Self::Binary { op: B::Concat, left, right } => {
                let mut results = Vec::new();
                for left in left.data_type().legs() {
                    for right in right.data_type().legs() {
                        results.push(match (&left, &right) {
                            (LogicalType::Null, _) | (_, LogicalType::Null) => LogicalType::Null,
                            (LogicalType::Scalar(ScalarType::Binary), LogicalType::Scalar(ScalarType::Binary)) => LogicalType::Scalar(ScalarType::Binary),
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
            Self::Case { branches, otherwise } | Self::SimpleCase { branches, otherwise, .. } => {
                let types: Vec<_> = branches.iter().map(|(_, value)| value.data_type()).chain([otherwise.data_type()]).collect();
                LogicalType::join_all(&types)
            }
            Self::Between { operand,low,high,.. } => boolean_type(operand.data_type().nullable() || low.data_type().nullable() || high.data_type().nullable()),
            Self::Coalesce(args) => {
                let types: Vec<_> = args.iter().map(Self::data_type).collect();
                LogicalType::join_all(&types)
            }
            Self::List(args) => {
                let types: Vec<_> = args.iter().map(Self::data_type).collect();
                LogicalType::list(LogicalType::join_all(&types))
            }
            Self::Struct(fields) => LogicalType::Struct(fields.iter().map(|(name, expr)| (name.clone(), expr.data_type())).collect()),
            Self::RowCopy(fields) => LogicalType::Struct(fields.iter().map(|field| (field.name.clone(), field.data_type.clone())).collect()),
        }
    }

    pub fn children(&self) -> Vec<&Self> {
        match self {
            Self::Unary { expr, .. } | Self::Cast { expr, .. } | Self::Field { expr, .. } => vec![expr],
            Self::Binary { left, right, .. } => vec![left, right],
            Self::Call { args, .. } | Self::Coalesce(args) | Self::List(args) => args.iter().collect(),
            Self::RegexReplace { source,.. } => vec![source],
            Self::Case { branches, otherwise } => branches.iter().flat_map(|(condition, value)| [condition, value]).chain([otherwise.as_ref()]).collect(),
            Self::Between { operand,low,high,.. } => vec![operand,low,high],
            Self::SimpleCase { operand, branches, otherwise } => std::iter::once(operand.as_ref()).chain(branches.iter().flat_map(|(candidate, value)| [candidate, value])).chain([otherwise.as_ref()]).collect(),
            Self::Subscript { expr, index, .. } => vec![expr, index],
            Self::Struct(fields) => fields.iter().map(|(_, value)| value).collect(),
            Self::In { expr, values, .. } => std::iter::once(expr.as_ref()).chain(values).collect(),
            Self::Like { expr, pattern, .. } => vec![expr, pattern],
            Self::Literal(_) | Self::DurationLiteral(_) | Self::Column { .. } | Self::Parameter { .. } | Self::Correlated { .. } | Self::RowCopy(_) => vec![],
        }
    }

    pub fn referenced_columns(&self) -> BTreeSet<ColumnId> {
        let mut columns = BTreeSet::new();
        self.visit(&mut |expr| { match expr {
            Self::Column { id, .. } => { columns.insert(*id); }
            Self::RowCopy(fields) => columns.extend(fields.iter().map(|field| field.id)),
            _ => {}
        } });
        columns
    }

    /// Visits this expression and its subexpressions in pre-order, without recursing on the stack.
    pub fn visit(&self, f: &mut impl FnMut(&Self)) {
        let mut pending = vec![self];
        while let Some(expression) = pending.pop() {
            f(expression);
            pending.extend(expression.children().into_iter().rev());
        }
    }

    /// Visits original subexpressions before applying each rewrite.
    /// A replacement expression is not traversed during the same pass.
    pub fn rewrite(&mut self, rewrite: &mut impl FnMut(&mut Self)) {
        match self {
            Self::Unary { expr, .. } | Self::Cast { expr, .. } | Self::Field { expr, .. }
            | Self::RegexReplace { source: expr, .. } => expr.rewrite(rewrite),
            Self::Binary { left, right, .. } => {
                left.rewrite(rewrite);
                right.rewrite(rewrite);
            }
            Self::Call { args, .. } | Self::Coalesce(args) | Self::List(args) => {
                for arg in args { arg.rewrite(rewrite); }
            }
            Self::Case { branches, otherwise } => {
                for (condition, value) in branches {
                    condition.rewrite(rewrite);
                    value.rewrite(rewrite);
                }
                otherwise.rewrite(rewrite);
            }
            Self::SimpleCase { operand, branches, otherwise } => {
                operand.rewrite(rewrite);
                for (candidate, value) in branches {
                    candidate.rewrite(rewrite);
                    value.rewrite(rewrite);
                }
                otherwise.rewrite(rewrite);
            }
            Self::Between { operand, low, high, .. } => {
                operand.rewrite(rewrite);
                low.rewrite(rewrite);
                high.rewrite(rewrite);
            }
            Self::Subscript { expr, index, .. } => {
                expr.rewrite(rewrite);
                index.rewrite(rewrite);
            }
            Self::Struct(fields) => {
                for (_, value) in fields { value.rewrite(rewrite); }
            }
            Self::In { expr, values, .. } => {
                expr.rewrite(rewrite);
                for value in values { value.rewrite(rewrite); }
            }
            Self::Like { expr, pattern, .. } => {
                expr.rewrite(rewrite);
                pattern.rewrite(rewrite);
            }
            Self::Literal(_) | Self::DurationLiteral(_) | Self::Column { .. } | Self::Parameter { .. } | Self::Correlated { .. } | Self::RowCopy(_) => {}
        }
        rewrite(self);
    }

    pub fn parameter_count(&self) -> usize {
        let mut count = 0;
        self.visit(&mut |expr| { if let Self::Parameter { index, .. } = expr { count = count.max(index.saturating_add(1)); } });
        count
    }

    pub fn validate_columns(&self, columns: &BTreeSet<ColumnId>) -> Result<()> {
        crate::validate_equality_expression(self)?;
        for missing in self.referenced_columns().difference(columns) {
            return Err(Anomaly::fault("rtdb.plan/unbound-column", format!("Column {} is outside the input schema", missing.0)));
        }
        let mut failure = None;
        self.visit(&mut |expr| {
            if let Self::RowCopy(fields) = expr {
                let mut names = BTreeSet::new();
                if fields.iter().any(|field| !names.insert(&field.name)) {
                    failure = Some(Anomaly::fault("rtdb.plan/duplicate-record-field", "Raw record fields must have unique names"));
                }
            }
            if let Self::Binary { op, left, right } = expr {
                if matches!(op,BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq) { if let Err(error) = crate::validate_ordered_predicate_types(&left.data_type(),&right.data_type()) { failure = Some(error); } }
                if let Some(name) = crate::bitwise::binary_name(*op) {
                    if let Err(error) = function_type(name, &[left.data_type(), right.data_type()]) { failure = Some(error); }
                }
                for left in left.data_type().legs() { for right in right.data_type().legs() {
                    if let (LogicalType::Scalar(ScalarType::Decimal { scale: left, .. }), LogicalType::Scalar(ScalarType::Decimal { scale: right, .. })) = (&left, &right) {
                        if let Err(error) = decimal_arithmetic_scale(*op, *left, *right) { failure = Some(error); }
                    }
                } }
            }
            if let Self::Between { operand,low,high,.. } = expr {
                for bound in [low,high] { if let Err(error) = crate::validate_ordered_predicate_types(&operand.data_type(),&bound.data_type()) { failure = Some(error); } }
            }
            if let Self::Unary { op: UnaryOp::BitNot, expr } = expr {
                if let Err(error) = function_type("bit_not", &[expr.data_type()]) { failure = Some(error); }
            }
            if let Self::Like { expr,pattern,case_insensitive,.. } = expr {
                if let Err(error) = function_type(if *case_insensitive { "ilike" } else { "like" }, &[expr.data_type(),pattern.data_type()]) { failure = Some(error); }
            }
            if let Self::Subscript { expr, index, .. } = expr {
                if let Err(error) = function_type("nth", &[expr.data_type(), index.data_type()]) { failure = Some(error); }
            }
            if let Self::Call { name, args, .. } = expr { if let Err(error) = function_type_with_args(name, args) { failure = Some(error); } }
        });
        if let Some(error) = failure { return Err(error); }
        Ok(())
    }
}

/// Returns a struct field's type, nullable Transit for Transit objects, and Null for absent or unsupported fields.
/// Field spelling is used directly; callers perform their frontend's name normalization first.
pub fn field_type(source: &LogicalType, name: &str) -> LogicalType {
    LogicalType::join_all(&source.legs().into_iter().map(|ty| match ty {
        LogicalType::Struct(fields) => fields.get(name).cloned().unwrap_or(LogicalType::Null),
        LogicalType::Scalar(ScalarType::Transit) => LogicalType::Scalar(ScalarType::Transit).maybe(),
        _ => LogicalType::Null,
    }).collect::<Vec<_>>())
}

fn is_null_only(ty: &LogicalType) -> bool {
    *ty == LogicalType::Null || ty.legs() == OrderedSet::from([LogicalType::Null])
}

fn boolean_type(nullable: bool) -> LogicalType {
    let ty = LogicalType::Scalar(ScalarType::Bool);
    if nullable { ty.maybe() } else { ty }
}

/// Computes literal-dependent function metadata and validates constant fields, precisions, and scales.
pub fn function_type_with_args(name: &str, arguments: &[Expr]) -> Result<LogicalType> {
    let types = arguments.iter().map(Expr::data_type).collect::<Vec<_>>();
    let result = crate::temporal_functions::with_args(name, arguments, function_type(name, &types)?)?;
    let result = crate::interval_constructor::with_args(name, arguments, result)?;
    if !name.eq_ignore_ascii_case("round") || is_null_only(&result) { return Ok(result); }
    let source = types.first().unwrap_or(&LogicalType::Nothing);
    let constant_scale_input = source.legs().iter().find_map(|leg| match leg {
        LogicalType::Scalar(ScalarType::Decimal { .. }) => Some("DECIMAL"),
        LogicalType::Scalar(ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64) => Some("INTEGER"),
        _ => None,
    });
    let scale = if let (Some(input), [_, scale]) = (constant_scale_input, arguments) {
        let non_literal = || non_literal_round_scale(input);
        let value = match scale {
            Expr::Literal(Value::I8(value)) => i64::from(*value), Expr::Literal(Value::I16(value)) => i64::from(*value),
            Expr::Literal(Value::I32(value)) => i64::from(*value), Expr::Literal(Value::I64(value)) => *value,
            Expr::Unary { op: UnaryOp::Negate, expr } => match expr.as_ref() { Expr::Literal(Value::I64(value)) => value.checked_neg().ok_or_else(decimal_scale_error)?, _ => return Err(non_literal()) },
            _ => return Err(non_literal()),
        };
        Some(i32::try_from(value).map_err(|_| decimal_scale_error())?)
    } else { None };
    let result = round_type(source, scale, arguments.len() == 2)?;
    Ok(if types.iter().any(LogicalType::nullable) { result.maybe() } else { result })
}

fn non_literal_round_scale(input: &str) -> Anomaly {
    Anomaly::unsupported("xtdb.expression/non-literal-scale", format!("ROUND({input}, INTEGER) with non-constant scale is not supported. Use ROUND(val::DOUBLE, scale) for dynamic scales."))
}
fn decimal_scale_error() -> Anomaly { Anomaly::incorrect("xtdb.expression/overflow-error", "Decimal scale exceeds signed 32 bits") }
fn round_type(source: &LogicalType, scale: Option<i32>, explicit: bool) -> Result<LogicalType> {
    if explicit && source.legs().iter().any(|leg| matches!(leg, LogicalType::Scalar(ScalarType::I8 | ScalarType::I16))) {
        return Err(Anomaly::unsupported("xtdb.expression/unsupported-round-type", "ROUND with a scale requires i32 or i64 integer input"));
    }
    let result = source.legs().into_iter().map(|leg| match leg {
        LogicalType::Scalar(ScalarType::Decimal { precision, bit_width, .. }) => LogicalType::Scalar(ScalarType::Decimal { precision, scale: scale.unwrap_or(0), bit_width }),
        LogicalType::Scalar(ty @ (ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64)) => {
            if explicit { LogicalType::Scalar(ScalarType::Decimal { precision: if ty == ScalarType::I64 { 19 } else { 10 }, scale: scale.unwrap_or(0), bit_width: 128 }) } else { LogicalType::Scalar(ty) }
        }
        LogicalType::Null | LogicalType::Nothing => leg,
        _ => LogicalType::Scalar(ScalarType::F64),
    }).collect::<Vec<_>>();
    Ok(LogicalType::join_all(&result))
}

/// Returns the decimal arithmetic scale, rejecting results outside signed 32 bits before coefficient evaluation.
/// Multiplication uses `left + right`, division uses `max(6, left + right + 1)`, and other operators use `max(left, right)`.
pub fn decimal_arithmetic_scale(op: BinaryOp, left: i32, right: i32) -> Result<i32> {
    let scale = match op {
        BinaryOp::Multiply => i64::from(left) + i64::from(right),
        BinaryOp::Divide => 6.max(i64::from(left) + i64::from(right) + 1),
        _ => i64::from(left.max(right)),
    };
    i32::try_from(scale).map_err(|_| decimal_scale_error())
}

fn equality_nullable(left: &LogicalType, right: &LogicalType) -> bool {
    if left.nullable() || right.nullable() { return true; }
    left.legs().iter().any(|left| right.legs().iter().any(|right| match (left, right) {
        (LogicalType::Struct(left), LogicalType::Struct(right)) => left.keys().eq(right.keys()),
        (LogicalType::List { kind: rtdb_types::ListKind::List | rtdb_types::ListKind::FixedSizeList(_), .. },
         LogicalType::List { kind: rtdb_types::ListKind::List | rtdb_types::ListKind::FixedSizeList(_), .. }) => true,
        _ => false,
    }))
}

pub fn arithmetic_type(op: BinaryOp, left: &LogicalType, right: &LogicalType) -> LogicalType {
    if let Some(name) = crate::bitwise::binary_name(op) { return function_type(name, &[left.clone(), right.clone()]).unwrap_or(LogicalType::Nothing); }
    let mut results = Vec::new();
    for left in left.legs() {
        for right in right.legs() {
            if left == LogicalType::Null || right == LogicalType::Null {
                results.push(LogicalType::Null);
                continue;
            }
            if op == BinaryOp::Multiply && crate::period::is_range(&left) && crate::period::is_range(&right) {
                results.push(left.clone().maybe());
                continue;
            }
            if let (LogicalType::Scalar(left), LogicalType::Scalar(right)) = (&left, &right) {
                if let Some(result) = crate::temporal_arithmetic_type(op, left, right) {
                    results.push(LogicalType::Scalar(result));
                    continue;
                }
            }
            let decimal_parts = |ty: &LogicalType| match ty {
                LogicalType::Scalar(ScalarType::Decimal { precision, scale, .. }) => Some((*precision, *scale)),
                LogicalType::Scalar(ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64) => Some((32, 0)),
                _ => None,
            };
            let has_decimal = [&left, &right].iter().any(|ty| matches!(ty, LogicalType::Scalar(ScalarType::Decimal { .. })));
            let result = if op == BinaryOp::Modulo { LogicalType::least_upper_bound(&[left.clone(), right.clone()]) } else if has_decimal {
                if let (Some((lp, ls)), Some((rp, rs))) = (decimal_parts(&left), decimal_parts(&right)) {
                    let precision = if lp <= 32 && rp <= 32 { 32 } else { 64 };
                    decimal_arithmetic_scale(op, ls, rs).ok().map(|scale| LogicalType::Scalar(ScalarType::Decimal { precision, scale, bit_width: if precision <= 32 { 128 } else { 256 } }))
                } else { LogicalType::least_upper_bound(&[left.clone(), right.clone()]) }
            } else { LogicalType::least_upper_bound(&[left.clone(), right.clone()]) };
            results.push(result.unwrap_or_else(|| left.join(&right)));
        }
    }
    LogicalType::join_all(&results)
}
