use crate::{
    compiler, error::ValidationError, ext::numeric::Numeric, keywords::CompilationResult,
    paths::{LazyLocation, Location}, types::JsonType, validator::Validate,
};
use serde_json::{Map, Number, Value};

pub(crate) struct MultipleOfValidator {
    multiple_of: Numeric,
    original: Number,
    location: Location,
}
impl Validate for MultipleOfValidator {
    fn is_valid(&self, instance: &Value) -> bool {
        match instance { Value::Number(item) => Numeric::new(item).multiple_of(&self.multiple_of), _ => true }
    }
    fn validate<'i>(&self, instance: &'i Value, location: &LazyLocation) -> Result<(), ValidationError<'i>> {
        if self.is_valid(instance) { Ok(()) }
        else { Err(ValidationError::multiple_of(self.location.clone(), location.into(), instance, self.original.clone())) }
    }
}
pub(crate) fn compile<'a>(ctx: &compiler::Context, _: &'a Map<String, Value>, schema: &'a Value) -> Option<CompilationResult<'a>> {
    Some(match schema {
        Value::Number(number) => {
            if ctx.numeric_semantics() == crate::ext::jvm::Semantics::Everit && number.as_f64().is_none_or(|value| value == 0.0 || !value.is_finite()) {
                return Some(Err(ValidationError::custom(ctx.location().join("multipleOf"), Location::new(), schema, "Everit multipleOf requires a finite nonzero double projection")));
            }
            Ok(Box::new(MultipleOfValidator { multiple_of: Numeric::new(number), original: number.clone(), location: ctx.location().join("multipleOf") }))
        },
        _ => Err(ValidationError::single_type_error(Location::new(), ctx.location().clone(), schema, JsonType::Number)),
    })
}

#[cfg(test)]
mod tests {
    use crate::tests_util;
    use serde_json::{json, Value};
    use test_case::test_case;

    #[test_case(&json!({"multipleOf": 2}), &json!(4))]
    #[test_case(&json!({"multipleOf": 1.0}), &json!(4.0))]
    #[test_case(&json!({"multipleOf": 1.5}), &json!(3.0))]
    #[test_case(&json!({"multipleOf": 1.5}), &json!(4.5))]
    #[test_case(&json!({"multipleOf": 0.1}), &json!(12.2))]
    #[test_case(&json!({"multipleOf": 0.0001}), &json!(3.1254))]
    #[test_case(&json!({"multipleOf": 0.0001}), &json!(47.498))]
    #[test_case(&json!({"multipleOf": 0.1}), &json!(1.1))]
    #[test_case(&json!({"multipleOf": 0.1}), &json!(1.2))]
    #[test_case(&json!({"multipleOf": 0.1}), &json!(1.3))]
    #[test_case(&json!({"multipleOf": 0.02}), &json!(1.02))]
    #[test_case(&json!({"multipleOf": 1e-16}), &json!(1e-15))]
    fn multiple_of_is_valid(schema: &Value, instance: &Value) {
        tests_util::is_valid(schema, instance);
    }

    #[test_case(&json!({"multipleOf": 1.0}), &json!(4.5))]
    #[test_case(&json!({"multipleOf": 0.1}), &json!(4.55))]
    #[test_case(&json!({"multipleOf": 0.2}), &json!(4.5))]
    #[test_case(&json!({"multipleOf": 0.02}), &json!(1.01))]
    #[test_case(&json!({"multipleOf": 1.3}), &json!(1.3e-16))]
    fn multiple_of_is_not_valid(schema: &Value, instance: &Value) {
        tests_util::is_not_valid(schema, instance);
    }

    #[test_case(&json!({"multipleOf": 2}), &json!(3), "/multipleOf")]
    #[test_case(&json!({"multipleOf": 1.5}), &json!(5), "/multipleOf")]
    fn location(schema: &Value, instance: &Value, expected: &str) {
        tests_util::assert_schema_location(schema, instance, expected);
    }
}
