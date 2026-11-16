use std::str::FromStr;
use serde_json::{Map, Value};
use crate::{compiler, error::ValidationError, ext::jvm::Semantics, keywords::CompilationResult, paths::{LazyLocation, Location}, types::{JsonType, JsonTypeSet}, validator::Validate};

struct TypeValidator { types: JsonTypeSet, semantics: Semantics, location: Location }
impl Validate for TypeValidator {
    fn is_valid(&self, instance: &Value) -> bool {
        match instance {
            Value::Number(number) => self.types.contains(JsonType::Number) || self.types.contains(JsonType::Integer) && self.semantics.integer(number),
            _ => self.types.contains_value_type(instance),
        }
    }
    fn validate<'i>(&self, instance: &'i Value, location: &LazyLocation) -> Result<(), ValidationError<'i>> {
        if self.is_valid(instance) { Ok(()) }
        else { Err(ValidationError::multiple_type_error(self.location.clone(), location.into(), instance, self.types)) }
    }
}
pub(crate) fn compile<'a>(ctx: &compiler::Context, _: &'a Map<String, Value>, schema: &'a Value) -> Option<CompilationResult<'a>> {
    let mut types = JsonTypeSet::empty();
    let values: &[Value] = match schema { Value::Array(values) => values, _ => std::slice::from_ref(schema) };
    for value in values {
        let ty = value.as_str().and_then(|value| JsonType::from_str(value).ok());
        let Some(ty) = ty else {
            return Some(Err(ValidationError::enumeration(Location::new(), ctx.location().join("type"), value, &serde_json::json!(["array", "boolean", "integer", "null", "number", "object", "string"]))));
        };
        types = types.insert(ty);
    }
    Some(Ok(Box::new(TypeValidator { types, semantics: ctx.numeric_semantics(), location: ctx.location().join("type") })))
}
