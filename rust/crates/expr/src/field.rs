use rtdb_format::transit::encode_msgpack;
use rtdb_types::{Result,Value};
use crate::{EvalContext,transit_object::Object};

pub(crate) fn get(value: &Value,name: Option<&str>,context: &EvalContext<'_>) -> Result<Value> {
    match value {
        Value::Struct(fields) => fields.get(name.unwrap_or("")).map(|value| {
            context.reserve(value.retained_bytes()?)?;
            Ok(value.clone())
        }).unwrap_or(Ok(Value::Null)),
        Value::Transit(bytes) => {
            context.reserve(crate::text_allocation_bound(value)?)?;
            let object = Object::decode(bytes)?;
            object.field(name)?.map(|value|encode_msgpack(&value.wire()?).map(Value::Transit)).unwrap_or(Ok(Value::Null))
        }
        _ => Ok(Value::Null),
    }
}
