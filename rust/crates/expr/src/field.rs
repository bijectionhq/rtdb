use rtdb_types::{Result,Value};
use crate::{EvalContext,transit_object::{Decoded,scope}};

pub(crate) fn get(value: &Value,name: Option<&str>,context: &EvalContext<'_>) -> Result<Value> {
    match value {
        Value::Struct(fields) => fields.get(name.unwrap_or("")).map(|value| {
            context.reserve(value.retained_bytes()?)?;
            Ok(value.clone())
        }).unwrap_or(Ok(Value::Null)),
        Value::Transit(bytes) => {
            let object = Decoded::read(bytes,scope(context.allocation)?)?;
            object.field(name)?.map(|value|value.encode().map(Value::Transit)).unwrap_or(Ok(Value::Null))
        }
        _ => Ok(Value::Null),
    }
}
