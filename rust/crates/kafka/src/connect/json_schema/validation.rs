use rtdb_types::Result;
use validation_json::{Number, Value};
use super::super::{error, json_value::JsonValue};

pub(super) fn view(value: &JsonValue) -> Result<Value> {
    Ok(match value {
        JsonValue::Null => Value::Null,
        JsonValue::Boolean(value) => Value::Bool(*value),
        JsonValue::String(value) => Value::String(value.clone()),
        JsonValue::Number(_) => Value::Number(value.number_text().ok_or_else(|| error("json-schema", "Invalid numeric validation value"))?.parse::<Number>().map_err(|cause| error("json-schema", cause))?),
        JsonValue::Decimal(value) => {
            let mut text = value.text();
            if !text.contains(['.', 'e', 'E']) { text.push_str("E+0"); }
            Value::Number(text.parse::<Number>().map_err(|cause| error("json-schema", cause))?)
        }
        JsonValue::Array(values) => Value::Array(values.iter().map(view).collect::<Result<_>>()?),
        JsonValue::Object(values) => Value::Object(values.iter().map(|(key, value)| Ok((key.clone(), view(value)?))).collect::<Result<_>>()?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn isolated_numeric_values_and_private_marker_keys_do_not_change_workspace_serde() {
        let text = "18446744073709551617";
        let ordinary: serde_json::Value = serde_json::from_str(text).unwrap();
        assert!(!ordinary.is_i64());
        assert!(!ordinary.is_u64());
        assert_ne!(ordinary.to_string(), text);
        assert_eq!(view(&JsonValue::from_registry_str(text).unwrap()).unwrap().as_number().unwrap().as_str(), text);
        let object = JsonValue::from_registry_str(r#"{"$serde_json::private::Number":"18446744073709551617","value":1.00}"#).unwrap();
        let adapted = view(&object).unwrap();
        assert_eq!(adapted["$serde_json::private::Number"].as_str(), Some(text));
        assert_eq!(adapted["value"].as_number().unwrap().as_str(), "1.00");
        let decimal = view(&JsonValue::from_registry_str("1e0").unwrap()).unwrap();
        assert!(decimal.as_number().unwrap().as_str().contains(['e', 'E']));
    }
}
