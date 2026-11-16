use serde_json::{Map, Value};

use crate::{compiler, paths::Location, types::JsonType, ValidationError};

#[inline]
pub(crate) fn map_get_u64<'a>(
    m: &'a Map<String, Value>,
    ctx: &compiler::Context,
    type_name: &str,
) -> Option<Result<u64, ValidationError<'a>>> {
    let value = m.get(type_name)?;
    if let Value::Number(number) = value {
        if let Some(value) = crate::ext::numeric::unsigned_limit(number, ctx.supports_integer_valued_numbers()) { return Some(Ok(value)); }
    }
    Some(Err(fail_on_non_positive_integer(value, ctx.location().clone())))
}

pub(crate) fn fail_on_non_positive_integer(
    value: &Value,
    instance_path: Location,
) -> ValidationError<'_> {
    if matches!(value, Value::Number(number) if number.as_str().starts_with('-') && crate::ext::numeric::is_integer(number)) {
        ValidationError::minimum(Location::new(), instance_path, value, 0.into())
    } else {
        ValidationError::single_type_error(Location::new(), instance_path, value, JsonType::Integer)
    }
}
