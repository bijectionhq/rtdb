use crate::{
    compiler,
    error::ValidationError,
    keywords::{helpers::fail_on_non_positive_integer, CompilationResult},
    paths::{LazyLocation, Location},
    validator::Validate,
};
use serde_json::{Map, Value};

pub(crate) struct MaxItemsValidator {
    limit: i128,
    location: Location,
}

impl MaxItemsValidator {
    #[inline]
    pub(crate) fn compile<'a>(
        ctx: &compiler::Context,
        schema: &'a Value,
        location: Location,
    ) -> CompilationResult<'a> {
        if let Value::Number(number) = schema {
            if let Some(limit) = crate::ext::jvm::count(number, ctx.numeric_semantics(), ctx.supports_integer_valued_numbers()) {
                return Ok(Box::new(MaxItemsValidator { limit, location }));
            }
        }
        Err(fail_on_non_positive_integer(schema, location))
    }
}

impl Validate for MaxItemsValidator {
    fn is_valid(&self, instance: &Value) -> bool {
        if let Value::Array(items) = instance {
            if (items.len() as i128) > self.limit {
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
        if let Value::Array(items) = instance {
            if (items.len() as i128) > self.limit {
                return Err(ValidationError::max_items(
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
    _: &Map<String, Value>,
    schema: &'a Value,
) -> Option<CompilationResult<'a>> {
    let location = ctx.location().join("maxItems");
    Some(MaxItemsValidator::compile(ctx, schema, location))
}

#[cfg(test)]
mod tests {
    use crate::tests_util;
    use serde_json::json;

    #[test]
    fn location() {
        tests_util::assert_schema_location(&json!({"maxItems": 1}), &json!([1, 2]), "/maxItems");
    }
}
