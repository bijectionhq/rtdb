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

    pub fn column_count(expected: usize, given: usize) -> Self {
        Self::new(format!("Column count mismatch: expected {expected}, given {given}"))
            .with_detail("expected", serde_json::json!(expected))
            .with_detail("given", serde_json::json!(given))
    }

    pub fn table_projection(alias: String, projections: Vec<String>) -> Self {
        Self::new(format!("Table projection mismatch for {alias} ({})", projections.join(",")))
            .with_detail("table-alias", alias)
            .with_detail("projections", serde_json::json!(projections))
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
