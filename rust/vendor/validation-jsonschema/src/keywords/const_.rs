use crate::{
    compiler, error::ValidationError, ext::jvm::Semantics, keywords::CompilationResult, paths::Location,
    validator::Validate,
};
use serde_json::{Map, Number, Value};

use crate::paths::LazyLocation;

struct ConstArrayValidator {
    value: Vec<Value>,
    location: Location,
    semantics: Semantics,
}
impl ConstArrayValidator {
    #[inline]
    pub(crate) fn compile(value: &[Value], location: Location, semantics: Semantics) -> CompilationResult<'_> {
        Ok(Box::new(ConstArrayValidator {
            value: value.to_vec(),
            location,
            semantics,
        }))
    }
}
impl Validate for ConstArrayValidator {
    fn validate<'i>(
        &self,
        instance: &'i Value,
        location: &LazyLocation,
    ) -> Result<(), ValidationError<'i>> {
        if self.is_valid(instance) {
            Ok(())
        } else {
            Err(ValidationError::constant_array(
                self.location.clone(),
                location.into(),
                instance,
                &self.value,
            ))
        }
    }

    #[inline]
    fn is_valid(&self, instance: &Value) -> bool {
        if let Value::Array(instance_value) = instance {
            self.value.len() == instance_value.len() && self.value.iter().zip(instance_value).all(|(left, right)| self.semantics.equal(left, right))
        } else {
            false
        }
    }
}

struct ConstBooleanValidator {
    value: bool,
    location: Location,
}
impl ConstBooleanValidator {
    #[inline]
    pub(crate) fn compile<'a>(value: bool, location: Location) -> CompilationResult<'a> {
        Ok(Box::new(ConstBooleanValidator { value, location }))
    }
}
impl Validate for ConstBooleanValidator {
    fn validate<'i>(
        &self,
        instance: &'i Value,
        location: &LazyLocation,
    ) -> Result<(), ValidationError<'i>> {
        if self.is_valid(instance) {
            Ok(())
        } else {
            Err(ValidationError::constant_boolean(
                self.location.clone(),
                location.into(),
                instance,
                self.value,
            ))
        }
    }

    #[inline]
    fn is_valid(&self, instance: &Value) -> bool {
        if let Value::Bool(instance_value) = instance {
            &self.value == instance_value
        } else {
            false
        }
    }
}

struct ConstNullValidator {
    location: Location,
}
impl ConstNullValidator {
    #[inline]
    pub(crate) fn compile<'a>(location: Location) -> CompilationResult<'a> {
        Ok(Box::new(ConstNullValidator { location }))
    }
}
impl Validate for ConstNullValidator {
    fn validate<'i>(
        &self,
        instance: &'i Value,
        location: &LazyLocation,
    ) -> Result<(), ValidationError<'i>> {
        if self.is_valid(instance) {
            Ok(())
        } else {
            Err(ValidationError::constant_null(
                self.location.clone(),
                location.into(),
                instance,
            ))
        }
    }
    #[inline]
    fn is_valid(&self, instance: &Value) -> bool {
        instance.is_null()
    }
}

struct ConstNumberValidator {
    original_value: Number,
    location: Location,
    semantics: Semantics,
}

impl ConstNumberValidator {
    #[inline]
    pub(crate) fn compile(original_value: &Number, location: Location, semantics: Semantics) -> CompilationResult<'_> {
        Ok(Box::new(ConstNumberValidator {
            original_value: original_value.clone(),
            location,
            semantics,
        }))
    }
}

impl Validate for ConstNumberValidator {
    fn validate<'i>(
        &self,
        instance: &'i Value,
        location: &LazyLocation,
    ) -> Result<(), ValidationError<'i>> {
        if self.is_valid(instance) {
            Ok(())
        } else {
            Err(ValidationError::constant_number(
                self.location.clone(),
                location.into(),
                instance,
                &self.original_value,
            ))
        }
    }

    fn is_valid(&self, instance: &Value) -> bool {
        if let Value::Number(item) = instance {
            self.semantics.number_equal(&self.original_value, item)
        } else {
            false
        }
    }
}

pub(crate) struct ConstObjectValidator {
    value: Map<String, Value>,
    location: Location,
    semantics: Semantics,
}

impl ConstObjectValidator {
    #[inline]
    pub(crate) fn compile(value: &Map<String, Value>, location: Location, semantics: Semantics) -> CompilationResult<'_> {
        Ok(Box::new(ConstObjectValidator {
            value: value.clone(),
            location,
            semantics,
        }))
    }
}

impl Validate for ConstObjectValidator {
    fn validate<'i>(
        &self,
        instance: &'i Value,
        location: &LazyLocation,
    ) -> Result<(), ValidationError<'i>> {
        if self.is_valid(instance) {
            Ok(())
        } else {
            Err(ValidationError::constant_object(
                self.location.clone(),
                location.into(),
                instance,
                &self.value,
            ))
        }
    }
    fn is_valid(&self, instance: &Value) -> bool {
        if let Value::Object(item) = instance {
            self.value.len() == item.len() && self.value.iter().zip(item).all(|((lk, lv), (rk, rv))| lk == rk && self.semantics.equal(lv, rv))
        } else {
            false
        }
    }
}

pub(crate) struct ConstStringValidator {
    value: String,
    location: Location,
}

impl ConstStringValidator {
    #[inline]
    pub(crate) fn compile(value: &str, location: Location) -> CompilationResult<'_> {
        Ok(Box::new(ConstStringValidator {
            value: value.to_string(),
            location,
        }))
    }
}

impl Validate for ConstStringValidator {
    fn validate<'i>(
        &self,
        instance: &'i Value,
        location: &LazyLocation,
    ) -> Result<(), ValidationError<'i>> {
        if self.is_valid(instance) {
            Ok(())
        } else {
            Err(ValidationError::constant_string(
                self.location.clone(),
                location.into(),
                instance,
                &self.value,
            ))
        }
    }
    fn is_valid(&self, instance: &Value) -> bool {
        if let Value::String(item) = instance {
            &self.value == item
        } else {
            false
        }
    }
}

#[inline]
pub(crate) fn compile<'a>(
    ctx: &compiler::Context,
    _: &'a Map<String, Value>,
    schema: &'a Value,
) -> Option<CompilationResult<'a>> {
    let location = ctx.location().join("const");
    match schema {
        Value::Array(items) => Some(ConstArrayValidator::compile(items, location, ctx.numeric_semantics())),
        Value::Bool(item) => Some(ConstBooleanValidator::compile(*item, location)),
        Value::Null => Some(ConstNullValidator::compile(location)),
        Value::Number(item) => Some(ConstNumberValidator::compile(item, location, ctx.numeric_semantics())),
        Value::Object(map) => Some(ConstObjectValidator::compile(map, location, ctx.numeric_semantics())),
        Value::String(string) => Some(ConstStringValidator::compile(string, location)),
    }
}

#[cfg(test)]
mod tests {
    use crate::tests_util;
    use serde_json::{json, Value};
    use test_case::test_case;

    #[test_case(&json!({"const": 1}), &json!(2), "/const")]
    #[test_case(&json!({"const": null}), &json!(3), "/const")]
    #[test_case(&json!({"const": false}), &json!(4), "/const")]
    #[test_case(&json!({"const": []}), &json!(5), "/const")]
    #[test_case(&json!({"const": {}}), &json!(6), "/const")]
    #[test_case(&json!({"const": ""}), &json!(7), "/const")]
    fn location(schema: &Value, instance: &Value, expected: &str) {
        tests_util::assert_schema_location(schema, instance, expected);
    }
}
