use rtdb_types::{Result, Value};
use sqlparser::tokenizer::Span;
use crate::planning_diagnostic::PlanningDiagnostic;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum TemporalLiteralKind { Date, Time, Timestamp, Interval }

impl TemporalLiteralKind {
    fn parse(self, input: &str) -> std::result::Result<Value, Option<String>> {
        match self {
            Self::Interval => crate::interval_literal::parse(input).map_err(|error| error.message().map(str::to_owned)),
            Self::Date => rtdb_types::parse_sql_date_literal(input).map_err(|error| error.message),
            Self::Timestamp => rtdb_types::parse_sql_timestamp_literal(input).map_err(|error| error.message),
            Self::Time => rtdb_types::validate_sql_time_literal(input).map(|_| Value::Utf8(input.to_owned())).map_err(|error| error.message),
        }
    }

    fn names(self) -> (&'static str, &'static str) {
        match self {
            Self::Date => ("date", "d-str"),
            Self::Time => ("time", "t-str"),
            Self::Timestamp => ("timestamp", "ts-str"),
            Self::Interval => ("interval", "i-str"),
        }
    }
}

struct AdmittedLiteral {
    occurrence: Span,
    kind: TemporalLiteralKind,
    input: String,
    parsed: std::result::Result<Value, Option<String>>,
}

#[derive(Default)]
pub(crate) struct LiteralAdmission { temporal: Vec<AdmittedLiteral> }
impl LiteralAdmission {
    pub fn temporal(&mut self, occurrence: Span, kind: TemporalLiteralKind, input: &str) -> Value {
        if let Some(admitted) = self.temporal.iter().find(|entry| entry.occurrence == occurrence && entry.kind == kind) {
            return admitted.parsed.clone().unwrap_or(Value::Null);
        }
        let parsed = kind.parse(input);
        let value = parsed.clone().unwrap_or(Value::Null);
        self.temporal.push(AdmittedLiteral { occurrence, kind, input: input.to_owned(), parsed });
        value
    }

    pub fn has_errors(&self) -> bool { self.temporal.iter().any(|entry| entry.parsed.is_err()) }

    pub fn check(&self) -> Result<()> {
        let mut errors = self.temporal.iter().filter_map(|entry| {
            let cause = entry.parsed.as_ref().err()?;
            let (kind, key) = entry.kind.names();
            let input = &entry.input;
            Some(PlanningDiagnostic::new(format!("Cannot parse {kind}: {input} - failed with message {}", cause.as_deref().unwrap_or("null")))
                .with_detail(key, input.clone()).with_detail("msg", serde_json::json!(cause)))
        });
        match errors.next() {
            Some(first) => Err(first.into_anomaly(errors)),
            None => Ok(()),
        }
    }
}
