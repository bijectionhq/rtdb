use serde_json::{Map, Value};

/// Tests for two JSON values to be equal using the JSON Schema semantic.
#[must_use]
pub fn equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::String(left), Value::String(right)) => left == right,
        (Value::Bool(left), Value::Bool(right)) => left == right,
        (Value::Null, Value::Null) => true,
        (Value::Number(left), Value::Number(right)) => super::numeric::equal(left, right),
        (Value::Array(left), Value::Array(right)) => equal_arrays(left, right),
        (Value::Object(left), Value::Object(right)) => equal_objects(left, right),
        (_, _) => false,
    }
}

#[inline]
pub(crate) fn equal_arrays(left: &[Value], right: &[Value]) -> bool {
    left.len() == right.len() && {
        let mut idx = 0_usize;
        while idx < left.len() {
            if !equal(&left[idx], &right[idx]) {
                return false;
            }
            idx += 1;
        }
        true
    }
}

#[inline]
pub(crate) fn equal_objects(left: &Map<String, Value>, right: &Map<String, Value>) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|((ka, va), (kb, vb))| ka == kb && equal(va, vb))
}

#[cfg(test)]
mod tests {
    use super::equal;
    use serde_json::{json, Value};
    use test_case::test_case;

    #[test_case(&json!(1), &json!(1.0))]
    #[test_case(&json!([2]), &json!([2.0]))]
    #[test_case(&json!([-3]), &json!([-3.0]))]
    #[test_case(&json!({"a": 1}), &json!({"a": 1.0}))]
    fn are_equal(left: &Value, right: &Value) {
        assert!(equal(left, right));
    }

    #[test_case(&json!(1), &json!(2.0))]
    #[test_case(&json!([]), &json!(["foo"]))]
    #[test_case(&json!([-3]), &json!([-4.0]))]
    #[test_case(&json!({"a": 1}), &json!({"a": 1.0, "b": 2}))]
    fn are_not_equal(left: &Value, right: &Value) {
        assert!(!equal(left, right));
    }
}
