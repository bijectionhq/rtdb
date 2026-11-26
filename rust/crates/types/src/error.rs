use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, fmt};

pub type Result<T> = std::result::Result<T, Anomaly>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AnomalyCategory {
    Incorrect,
    Unsupported,
    NotFound,
    Forbidden,
    Conflict,
    Busy,
    Unavailable,
    Interrupted,
    Fault,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Anomaly {
    pub category: AnomalyCategory,
    pub code: String,
    pub message: Option<String>,
    #[serde(with = "context")]
    pub context: BTreeMap<String, Value>,
}

/// A JSON value decodes only from a self-describing format, so a binary
/// encoding carries each context value as its JSON text.
mod context {
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};
    use serde_json::Value;
    use std::collections::BTreeMap;

    pub fn serialize<S: Serializer>(context: &BTreeMap<String, Value>, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() { return context.serialize(serializer); }
        serializer.collect_map(context.iter().map(|(key, value)| (key, value.to_string())))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<BTreeMap<String, Value>, D::Error> {
        if deserializer.is_human_readable() { return BTreeMap::deserialize(deserializer); }
        BTreeMap::<String, String>::deserialize(deserializer)?.into_iter()
            .map(|(key, text)| serde_json::from_str(&text).map(|value| (key, value)).map_err(D::Error::custom))
            .collect()
    }
}

impl Anomaly {
    pub fn new(category: AnomalyCategory, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new_nullable(category, code, Some(message.into()))
    }

    pub fn new_nullable(category: AnomalyCategory, code: impl Into<String>, message: Option<String>) -> Self {
        Self { category, code: code.into(), message, context: BTreeMap::new() }
    }

    pub fn incorrect(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(AnomalyCategory::Incorrect, code, message)
    }

    pub fn unsupported(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(AnomalyCategory::Unsupported, code, message)
    }

    pub fn fault(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(AnomalyCategory::Fault, code, message)
    }

    pub fn with_context(mut self, key: impl Into<String>, value: Value) -> Self {
        self.context.insert(key.into(), value);
        self
    }

    pub fn with_transit_context(self, key: impl Into<String>, value: &crate::transit_value::TransitValue) -> Result<Self> {
        Ok(self.with_context(key, crate::transit_value::to_json_value(value)?))
    }

    pub fn is_caller_error(&self) -> bool {
        matches!(self.category, AnomalyCategory::Incorrect | AnomalyCategory::Unsupported
            | AnomalyCategory::NotFound | AnomalyCategory::Forbidden | AnomalyCategory::Conflict)
    }
}

impl fmt::Display for Anomaly {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.code)?;
        if let Some(message) = &self.message { write!(f, ": {message}")?; }
        Ok(())
    }
}

impl std::error::Error for Anomaly {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_preserve_present_empty_and_absent_messages() {
        for error in [Anomaly::new(AnomalyCategory::Fault, "test/error", ""), Anomaly::fault("test/error", ""), Anomaly::incorrect("test/error", ""), Anomaly::unsupported("test/error", "")] {
            assert_eq!(error.message.as_deref(), Some(""));
        }
        for message in [None, Some(String::new()), Some("detail".into())] {
            let error = Anomaly::new_nullable(AnomalyCategory::Fault, "test/error", message.clone());
            assert_eq!(error.message, message);
            assert_eq!(error.clone(), error);
            assert_eq!(serde_json::from_str::<Anomaly>(&serde_json::to_string(&error).unwrap()).unwrap(), error);
        }
        assert_ne!(Anomaly::new_nullable(AnomalyCategory::Fault, "test/error", None), Anomaly::fault("test/error", ""));
    }

    #[test]
    fn display_omits_the_separator_only_for_absent_messages() {
        assert_eq!(Anomaly::new_nullable(AnomalyCategory::Fault, "test/error", None).to_string(), "test/error");
        assert_eq!(Anomaly::fault("test/error", "").to_string(), "test/error: ");
        assert_eq!(Anomaly::fault("test/error", "detail").to_string(), "test/error: detail");
    }
}
