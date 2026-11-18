use rtdb_types::{Result, Value};
use sqlparser::tokenizer::Span;
use crate::interval_literal::IntervalLiteralError;
use crate::planning_diagnostic::PlanningDiagnostic;

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
        let mut errors = self.intervals.iter().filter_map(|entry| {
            let cause = entry.parsed.as_ref().err()?;
            let input = &entry.input;
            Some(PlanningDiagnostic::new(format!("Cannot parse interval: {input} - failed with message {}", cause.message().unwrap_or("null")))
                .with_detail("i-str", input.clone()).with_detail("msg", serde_json::json!(cause.message())))
        });
        match errors.next() {
            Some(first) => Err(first.into_anomaly(errors)),
            None => Ok(()),
        }
    }
}
