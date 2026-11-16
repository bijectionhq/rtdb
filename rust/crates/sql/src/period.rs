use rtdb_plan::{BinaryOp as B, Expr};
use rtdb_types::{Anomaly, Result};

pub(crate) fn call(name: &str, args: Vec<Expr>) -> Result<Expr> {
    let return_type = rtdb_plan::function_type_with_args(name, &args)?;
    Ok(Expr::Call { name: name.into(), args, return_type })
}

fn lower(period: &Expr) -> Result<Expr> { call("lower", vec![period.clone()]) }
fn upper(period: &Expr) -> Result<Expr> {
    Ok(Expr::Coalesce(vec![call("upper", vec![period.clone()])?, crate::mutation::timestamp_literal(i64::MAX)]))
}

pub(crate) fn predicate(name: &str, left: Expr, right: Expr) -> Result<Expr> {
    if name == "contains" { return call("contains?", vec![left, right]); }
    let (lf, lt, rf, rt) = (lower(&left)?, upper(&left)?, lower(&right)?, upper(&right)?);
    let comparison = |op, left, right| Expr::binary(op, left, right);
    let and = |left, right| Expr::binary(B::And, left, right);
    Ok(match name {
        "overlaps" => and(comparison(B::Lt, lf, rt), comparison(B::Gt, lt, rf)),
        "equals" => and(comparison(B::Eq, lf, rf), comparison(B::IsNotDistinctFrom, call("upper", vec![left])?, call("upper", vec![right])?)),
        "strictly_contains" => and(comparison(B::Lt, lf, rf), comparison(B::Gt, lt, rt)),
        "strictly_overlaps" => and(comparison(B::Gt, lf, rf), comparison(B::Lt, lt, rt)),
        "precedes" => comparison(B::LtEq, lt, rf),
        "strictly_precedes" => comparison(B::Lt, lt, rf),
        "immediately_precedes" => comparison(B::Eq, lt, rf),
        "succeeds" => comparison(B::GtEq, lf, rt),
        "strictly_succeeds" => comparison(B::Gt, lf, rt),
        "immediately_succeeds" => comparison(B::Eq, lf, rt),
        "lags" => and(comparison(B::GtEq, lf, rf), comparison(B::Gt, lt, rt)),
        "strictly_lags" => and(comparison(B::Gt, lf, rf), comparison(B::Gt, lt, rt)),
        "immediately_lags" => and(comparison(B::Eq, lf, rf), comparison(B::Gt, lt, rt)),
        "leads" => and(comparison(B::Lt, lf, rf), comparison(B::LtEq, lt, rt)),
        "strictly_leads" => and(comparison(B::Lt, lf, rf), comparison(B::Lt, lt, rt)),
        "immediately_leads" => and(comparison(B::Lt, lf, rf), comparison(B::Eq, lt, rt)),
        _ => return Err(Anomaly::fault("rtdb.sql/period-predicate", format!("Unknown period predicate: {name}"))),
    })
}

pub(crate) fn overlaps(periods: Vec<Expr>) -> Result<Expr> {
    if periods.len() < 2 { return Err(Anomaly::incorrect("xtdb/invalid-function-arity", "OVERLAPS requires at least two periods")); }
    let lower = periods.iter().map(lower).collect::<Result<Vec<_>>>()?;
    let upper = periods.iter().map(upper).collect::<Result<Vec<_>>>()?;
    Ok(Expr::binary(B::Lt, call("greatest", lower)?, call("least", upper)?))
}
