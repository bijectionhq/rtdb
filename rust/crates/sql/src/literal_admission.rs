use rtdb_types::{Anomaly, Result, Value};
use sqlparser::tokenizer::Span;
use crate::interval_literal::IntervalLiteralError;

struct AdmittedInterval { occurrence: Span, input: String, parsed: std::result::Result<Value, IntervalLiteralError> }

#[derive(Default)]
pub(crate) struct LiteralAdmission { intervals: Vec<AdmittedInterval> }
impl LiteralAdmission {
    pub fn interval(&mut self, occurrence: Span, input: String) -> Value {
        if let Some(admitted) = self.intervals.iter().find(|entry| entry.occurrence == occurrence) {
            return admitted.parsed.clone().unwrap_or(Value::Null);
        }
        let parsed = crate::interval_literal::parse(&input);
        let value = parsed.clone().unwrap_or(Value::Null);
        self.intervals.push(AdmittedInterval { occurrence, input, parsed });
        value
    }

    pub fn check(&self) -> Result<()> {
        if self.intervals.iter().all(|entry| entry.parsed.is_ok()) { return Ok(()); }
        let mut message = String::from("Errors planning SQL statement:");
        let mut errors = Vec::new();
        for entry in &self.intervals {
            if let Err(cause) = &entry.parsed {
                let input = &entry.input;
                message.push_str(&format!("\n  - Cannot parse interval: {input} - failed with message {}", cause.message().unwrap_or("null")));
                errors.push(serde_json::json!({"i-str": input, "msg": cause.message()}));
            }
        }
        Err(Anomaly::incorrect("xtdb/sql-error", message).with_context("errors", errors.into()))
    }
}
