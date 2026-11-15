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
    pub message: String,
    pub context: BTreeMap<String, Value>,
}

impl Anomaly {
    pub fn new(category: AnomalyCategory, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self { category, code: code.into(), message: message.into(), context: BTreeMap::new() }
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

    pub fn is_caller_error(&self) -> bool {
        matches!(self.category, AnomalyCategory::Incorrect | AnomalyCategory::Unsupported
            | AnomalyCategory::NotFound | AnomalyCategory::Forbidden | AnomalyCategory::Conflict)
    }
}

impl fmt::Display for Anomaly {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for Anomaly {}
