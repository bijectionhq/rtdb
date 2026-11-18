use std::fmt::Write;

use rtdb_types::Anomaly;
use serde_json::{Map, Value};

pub(crate) struct PlanningDiagnostic {
    message: String,
    details: Map<String, Value>,
}

impl PlanningDiagnostic {
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into(), details: Map::new() }
    }

    pub fn with_detail(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.details.insert(key.into(), value.into());
        self
    }

    pub fn into_anomaly(self, other: impl IntoIterator<Item = Self>) -> Anomaly {
        let mut message = String::from("Errors planning SQL statement:");
        let mut errors = Vec::new();
        for error in std::iter::once(self).chain(other) {
            write!(message, "\n  - {}", error.message).expect("writing to a String cannot fail");
            errors.push(Value::Object(error.details));
        }
        Anomaly::incorrect("xtdb/sql-error", message).with_context("errors", errors.into())
    }
}
