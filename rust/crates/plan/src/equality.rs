use rtdb_types::{Anomaly, ListKind, LogicalType as T, Result};
use crate::{BinaryOp, Expr};

/// Validates every declared nonnull equality pair before values or collection sizes are read.
/// Ordinary lists recurse through element legs; structs recurse only when their field names match.
/// Set/Set pairs return `xtdb.expression/set-equality`; strict `===` uses a separate contract.
pub fn validate_equality_types(left: &T, right: &T) -> Result<()> {
    match (left, right) {
        (T::Nothing | T::Null, _) | (_, T::Nothing | T::Null) => Ok(()),
        (T::Maybe(inner) | T::RunEndEncoded { value: inner, .. }, _) => validate_equality_types(inner, right),
        (_, T::Maybe(inner) | T::RunEndEncoded { value: inner, .. }) => validate_equality_types(left, inner),
        (T::Union(legs), _) => legs.iter().try_for_each(|leg| validate_equality_types(leg, right)),
        (_, T::Union(legs)) => legs.iter().try_for_each(|leg| validate_equality_types(left, leg)),
        (T::List { kind: ListKind::Set, .. }, T::List { kind: ListKind::Set, .. }) => Err(Anomaly::unsupported("xtdb.expression/set-equality", "Equality and inequality are not supported for sets")),
        (T::List { kind: ListKind::List, element: left }, T::List { kind: ListKind::List, element: right }) => validate_equality_types(left, right),
        (T::Struct(left), T::Struct(right)) if left.keys().eq(right.keys()) => left.values().zip(right.values()).try_for_each(|(left, right)| validate_equality_types(left, right)),
        _ => Ok(()),
    }
}

pub(crate) fn binary(op: BinaryOp) -> bool { matches!(op, BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::IsDistinctFrom | BinaryOp::IsNotDistinctFrom) }
pub(crate) fn call(name: &str) -> bool { ["null_eq", "null-eq", "distinct_from", "distinct-from", "nullif"].iter().any(|candidate| name.eq_ignore_ascii_case(candidate)) }

/// Validates equality calls in every expression branch, including branches that produce no values.
/// The caller owns temporary type metadata allocation; this does not evaluate arguments.
pub fn validate_equality_expression(root: &Expr) -> Result<()> {
    let mut pending = vec![root];
    while let Some(expression) = pending.pop() {
    match expression {
        Expr::Binary { op, left, right } if binary(*op) => validate_equality_types(&left.data_type(), &right.data_type())?,
        Expr::Call { name, args, .. } if call(name) && args.len() == 2 => validate_equality_types(&args[0].data_type(), &args[1].data_type())?,
        Expr::SimpleCase { operand, branches, .. } => for (candidate, _) in branches { validate_equality_types(&operand.data_type(), &candidate.data_type())?; },
        Expr::In { expr, values, .. } => for value in values { validate_equality_types(&expr.data_type(), &value.data_type())?; },
        _ => (),
    }
    pending.extend(expression.children().into_iter().rev());
    }
    Ok(())
}
