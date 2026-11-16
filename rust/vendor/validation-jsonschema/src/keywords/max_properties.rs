use crate::{
    compiler,
    error::ValidationError,
    keywords::{helpers::fail_on_non_positive_integer, CompilationResult},
    paths::{LazyLocation, Location},
    validator::Validate,
};
use serde_json::{Map, Value};

pub(crate) struct MaxPropertiesValidator {
    limit: i128,
    location: Location,
}

impl MaxPropertiesValidator {
    #[inline]
    pub(crate) fn compile<'a>(
        ctx: &compiler::Context,
        schema: &'a Value,
        location: Location,
    ) -> CompilationResult<'a> {
        if let Value::Number(number) = schema {
            if let Some(limit) = crate::ext::jvm::count(number, ctx.numeric_semantics(), ctx.supports_integer_valued_numbers()) {
                return Ok(Box::new(MaxPropertiesValidator { limit, location }));
            }
        }
        Err(fail_on_non_positive_integer(schema, location))
    }
}

impl Validate for MaxPropertiesValidator {
    fn is_valid(&self, instance: &Value) -> bool {
        if let Value::Object(item) = instance {
            if (item.len() as i128) > self.limit {
                return false;
            }
        }
        true
    }

    fn validate<'i>(
        &self,
        instance: &'i Value,
        location: &LazyLocation,
    ) -> Result<(), ValidationError<'i>> {
        if let Value::Object(item) = instance {
            if (item.len() as i128) > self.limit {
                return Err(ValidationError::max_properties(
                    self.location.clone(),
                    location.into(),
                    instance,
                    self.limit,
                ));
            }
        }
        Ok(())
    }
}

#[inline]
pub(crate) fn compile<'a>(
    ctx: &compiler::Context,
    _: &'a Map<String, Value>,
    schema: &'a Value,
) -> Option<CompilationResult<'a>> {
    let location = ctx.location().join("maxProperties");
    Some(MaxPropertiesValidator::compile(ctx, schema, location))
}

#[cfg(test)]
mod tests {
    use crate::tests_util;
    use serde_json::json;

    #[test]
    fn location() {
        tests_util::assert_schema_location(
            &json!({"maxProperties": 1}),
            &json!({"a": 1, "b": 2}),
            "/maxProperties",
        );
    }
}
