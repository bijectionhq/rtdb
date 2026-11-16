use crate::{Expr, IntervalField, IntervalQualifier};
use rtdb_types::{Anomaly, IntervalUnit, LogicalType as L, Result, ScalarType as S, Value};

/// Validated literal metadata for a single- or multi-field interval constructor call.
/// The recipe borrows no query state; execution supplies one integer or text value.
#[derive(Clone, Debug)]
pub struct IntervalConstructor { qualifier: IntervalQualifier }
impl IntervalConstructor {
    /// Validates leading precision in 1..=8 and the case-sensitive field name.
    /// SECOND also requires fractional precision in 0..=9; other fields ignore that option.
    /// Invalid fields or precisions return interval-specific anomalies.
    pub fn single(unit: &str, leading: i64, fractional: Option<i64>) -> Result<Self> {
        leading_precision(leading)?;
        let start = IntervalField::parse(unit)?;
        if start == IntervalField::Second { crate::fractional_precision(fractional.ok_or_else(nonliteral)?,true)?; }
        Ok(Self { qualifier: IntervalQualifier { start,end: None,leading_precision: Some(leading),fractional_precision: fractional } })
    }
    /// Validates a case-sensitive field pair, leading precision and the SECOND fractional precision.
    /// The caller supplies fractional metadata even when the end field is not SECOND.
    /// Invalid fields or precisions return interval-specific anomalies.
    pub fn multi(start: &str, end: &str, leading: i64, fractional: i64) -> Result<Self> {
        let qualifier = IntervalQualifier { start: IntervalField::parse(start)?,end: Some(IntervalField::parse(end)?),leading_precision: Some(leading),fractional_precision: Some(fractional) };
        qualifier.validate_units()?;
        leading_precision(leading)?;
        if qualifier.end == Some(IntervalField::Second) { crate::fractional_precision(fractional,true)?; }
        Ok(Self { qualifier })
    }
    pub fn qualifier(&self) -> &IntervalQualifier { &self.qualifier }
    pub fn result_type(&self) -> L { self.qualifier.result_type(&L::Scalar(S::Utf8)) }
}

fn leading_precision(value: i64) -> Result<()> {
    if !(1..=8).contains(&value) { return Err(Anomaly::incorrect("xtdb.expression/invalid-interval-precision",if value < 1 { "The minimum leading field precision is 1." } else { "The maximum leading field precision is 8." }).with_context("precision",value.into())); }
    Ok(())
}
fn nonliteral() -> Anomaly { Anomaly::unsupported("xtdb.expression/non-literal-interval-metadata","Interval constructor metadata must be literal") }
fn mismatch(name: &str) -> Anomaly { Anomaly::incorrect("xtdb.expression/function-type-mismatch",format!("{name} not applicable to argument types")) }
fn signed(ty: &L) -> bool { matches!(ty,L::Scalar(S::I8 | S::I16 | S::I32 | S::I64)) }
pub(crate) fn is_name(name: &str) -> bool { matches!(name,"single_field_interval" | "multi_field_interval") }

pub(crate) fn signature(name: &str,args: &[L]) -> Option<Result<L>> {
    if !is_name(name) { return None; }
    Some((|| {
        let single = name == "single_field_interval";
        if args.len() != if single { 4 } else { 5 } { return Err(Anomaly::incorrect("xtdb/invalid-function-arity",format!("Wrong argument count for {name}"))); }
        let mut output = L::Nothing;
        combinations(args,&mut Vec::with_capacity(args.len()),&mut |row| {
            if row.contains(&L::Null) { output = output.join(&L::Null);return Ok(()); }
            let metadata = row[1] == L::Scalar(S::Utf8) && signed(&row[2])
                && if single { signed(&row[3]) } else { row[3] == L::Scalar(S::Utf8) && signed(&row[4]) };
            if !(row[0] == L::Scalar(S::Utf8) || single && signed(&row[0])) || !metadata { return Err(mismatch(name)); }
            let mut ty = L::Scalar(S::Interval(IntervalUnit::YearMonth)).join(&L::Scalar(S::IntervalMonthDayMicro));
            if !single { ty = ty.join(&L::Scalar(S::Interval(IntervalUnit::MonthDayNano))); }
            output = output.join(&ty);Ok(())
        })?;
        Ok(output)
    })())
}
fn combinations(args: &[L],row: &mut Vec<L>,visit: &mut impl FnMut(&[L])->Result<()>) -> Result<()> {
    let Some((head,tail)) = args.split_first() else { return visit(row); };
    for leg in head.legs() { row.push(leg);combinations(tail,row,visit)?;row.pop(); } Ok(())
}
fn text(expr: &Expr) -> Result<&str> { match expr { Expr::Literal(Value::Utf8(value)) => Ok(value),_ => Err(nonliteral()) } }
fn number(expr: &Expr) -> Result<i64> {
    match expr {
        Expr::Literal(Value::I8(value)) => Ok(i64::from(*value)),Expr::Literal(Value::I16(value)) => Ok(i64::from(*value)),
        Expr::Literal(Value::I32(value)) => Ok(i64::from(*value)),Expr::Literal(Value::I64(value)) => Ok(*value),_ => Err(nonliteral()),
    }
}

/// Validates an interval constructor's types and consumed literal metadata.
/// Returns None for another function or a call with only null/unknown dispatches.
/// A nonliteral ignored fractional argument is permitted outside single-field SECOND.
pub fn interval_constructor(name: &str,args: &[Expr]) -> Result<Option<IntervalConstructor>> {
    if !["single_field_interval","single-field-interval","multi_field_interval","multi-field-interval"].iter().any(|candidate|name.eq_ignore_ascii_case(candidate)) { return Ok(None); }
    let name = name.to_lowercase().replace('-',"_");
    let Some(base) = signature(&name,&args.iter().map(Expr::data_type).collect::<Vec<_>>()) else { return Ok(None); };
    let base = base?;
    if matches!(base,L::Null | L::Nothing) { return Ok(None); }
    Ok(Some(if name == "single_field_interval" {
        let leading = number(&args[2])?;
        leading_precision(leading)?;
        let unit = text(&args[1])?;
        IntervalConstructor::single(unit,leading,if unit == "SECOND" { Some(number(&args[3])?) } else { None })?
    } else {
        let fractional = number(&args[4])?;
        let start = text(&args[1])?;let end = text(&args[3])?;
        let provisional = IntervalQualifier { start: IntervalField::parse(start)?,end: Some(IntervalField::parse(end)?),leading_precision: None,fractional_precision: None };
        provisional.validate_units()?;
        IntervalConstructor::multi(start,end,number(&args[2])?,fractional)?
    }))
}
pub(crate) fn with_args(name: &str,args: &[Expr],base: L) -> Result<L> {
    Ok(match interval_constructor(name,args)? { Some(recipe) => if base.nullable() { recipe.result_type().maybe() } else { recipe.result_type() },None => base })
}
