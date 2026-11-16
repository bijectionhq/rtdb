use rtdb_plan::IntervalConstructor;
use rtdb_types::{Anomaly, Result, Value};

pub(crate) fn is_name(name: &str) -> bool {
    ["single_field_interval","single-field-interval","multi_field_interval","multi-field-interval"].iter().any(|candidate| name.eq_ignore_ascii_case(candidate))
}
pub(crate) fn construct(value: &Value,recipe: &IntervalConstructor) -> Result<Value> {
    crate::interval_cast::cast(value,recipe.qualifier()).map_err(|error| if error.code == "xtdb/numeric-value-out-of-range" {
        Anomaly::incorrect("xtdb.expression/overflow-error","data exception - overflow error")
    } else { error })
}
pub(crate) fn call(name: &str,args: &[Value]) -> Option<Result<Value>> {
    if !is_name(name) { return None; }
    Some((|| {
        if args.iter().any(crate::compare::nullish) { return Ok(Value::Null); }
        let text = |index| match &args[index] { Value::Utf8(value) => Ok(value.as_str()),_ => Err(crate::numeric::inapplicable(name,&[&args[index]])) };
        let number = |index| crate::numeric::integer(&args[index]).and_then(|value|i64::try_from(value).ok()).ok_or_else(|| crate::numeric::inapplicable(name,&[&args[index]]));
        let recipe = if name == "single_field_interval" {
            let unit = text(1)?;
            IntervalConstructor::single(unit,number(2)?,if unit == "SECOND" { Some(number(3)?) } else { None })?
        } else { IntervalConstructor::multi(text(1)?,text(3)?,number(2)?,number(4)?)? };
        construct(&args[0],&recipe)
    })())
}
