use crate::{EvalContext, cast};
use crate::{compare::nullish, numeric::inapplicable};
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, TimeUnit, Value};

/// Checks the ordering of canonical microsecond period endpoints, including the infinity sentinels.
pub fn validate_period_bounds(lower: i64, upper: i64) -> Result<()> {
    if lower < upper { return Ok(()); }
    Err(Anomaly::incorrect("xtdb/invalid-period", "'from' must be earlier than 'to' when constructing a period")
        .with_context("from", lower.into()).with_context("to", upper.into()))
}

pub(crate) fn intersection(left: &Value, right: &Value) -> Option<Value> {
    let (Value::TimestampTzRange { lower: lf, upper: lt }, Value::TimestampTzRange { lower: rf, upper: rt }) = (left, right) else { return None; };
    let (lower, upper) = ((*lf).max(*rf), (*lt).min(*rt));
    Some(if lower < upper { Value::TimestampTzRange { lower, upper } } else { Value::Null })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PeriodPredicate {
    Contains,
    StrictlyContains,
    Overlaps,
    StrictlyOverlaps,
    Equals,
    Precedes,
    StrictlyPrecedes,
    ImmediatelyPrecedes,
    Succeeds,
    StrictlySucceeds,
    ImmediatelySucceeds,
    Leads,
    StrictlyLeads,
    ImmediatelyLeads,
    Lags,
    StrictlyLags,
    ImmediatelyLags,
}

impl PeriodPredicate {
    pub(crate) fn from_canonical(name: &str) -> Option<(Self, bool)> {
        let (base, question) = match name.strip_suffix('?') {
            Some(base) => (base, true),
            None => (name, false),
        };
        let predicate = match base {
            "contains" => Self::Contains,
            "strictly_contains" => Self::StrictlyContains,
            "overlaps" => Self::Overlaps,
            "strictly_overlaps" => Self::StrictlyOverlaps,
            "equals" => Self::Equals,
            "precedes" => Self::Precedes,
            "strictly_precedes" => Self::StrictlyPrecedes,
            "immediately_precedes" => Self::ImmediatelyPrecedes,
            "succeeds" => Self::Succeeds,
            "strictly_succeeds" => Self::StrictlySucceeds,
            "immediately_succeeds" => Self::ImmediatelySucceeds,
            "leads" => Self::Leads,
            "strictly_leads" => Self::StrictlyLeads,
            "immediately_leads" => Self::ImmediatelyLeads,
            "lags" => Self::Lags,
            "strictly_lags" => Self::StrictlyLags,
            "immediately_lags" => Self::ImmediatelyLags,
            _ => return None,
        };
        Some((predicate, question))
    }

    pub(crate) fn canonical_name(self, question: bool) -> &'static str {
        match (self, question) {
            (Self::Contains, false) => "contains",
            (Self::Contains, true) => "contains?",
            (Self::StrictlyContains, false) => "strictly_contains",
            (Self::StrictlyContains, true) => "strictly_contains?",
            (Self::Overlaps, false) => "overlaps",
            (Self::Overlaps, true) => "overlaps?",
            (Self::StrictlyOverlaps, false) => "strictly_overlaps",
            (Self::StrictlyOverlaps, true) => "strictly_overlaps?",
            (Self::Equals, false) => "equals",
            (Self::Equals, true) => "equals?",
            (Self::Precedes, false) => "precedes",
            (Self::Precedes, true) => "precedes?",
            (Self::StrictlyPrecedes, false) => "strictly_precedes",
            (Self::StrictlyPrecedes, true) => "strictly_precedes?",
            (Self::ImmediatelyPrecedes, false) => "immediately_precedes",
            (Self::ImmediatelyPrecedes, true) => "immediately_precedes?",
            (Self::Succeeds, false) => "succeeds",
            (Self::Succeeds, true) => "succeeds?",
            (Self::StrictlySucceeds, false) => "strictly_succeeds",
            (Self::StrictlySucceeds, true) => "strictly_succeeds?",
            (Self::ImmediatelySucceeds, false) => "immediately_succeeds",
            (Self::ImmediatelySucceeds, true) => "immediately_succeeds?",
            (Self::Leads, false) => "leads",
            (Self::Leads, true) => "leads?",
            (Self::StrictlyLeads, false) => "strictly_leads",
            (Self::StrictlyLeads, true) => "strictly_leads?",
            (Self::ImmediatelyLeads, false) => "immediately_leads",
            (Self::ImmediatelyLeads, true) => "immediately_leads?",
            (Self::Lags, false) => "lags",
            (Self::Lags, true) => "lags?",
            (Self::StrictlyLags, false) => "strictly_lags",
            (Self::StrictlyLags, true) => "strictly_lags?",
            (Self::ImmediatelyLags, false) => "immediately_lags",
            (Self::ImmediatelyLags, true) => "immediately_lags?",
        }
    }

    pub(crate) fn eval(self, question: bool, args: &[Value], context: &EvalContext<'_>) -> Result<Value> {
        let name = self.canonical_name(question);
        if args.iter().any(nullish) { return Ok(Value::Null); }
        let Value::TimestampTzRange { lower: lf, upper: lt } = &args[0] else { return Err(inapplicable(name, &[&args[0], &args[1]])); };
        let (rf, rt) = match &args[1] {
            Value::TimestampTzRange { lower, upper } => (*lower, *upper),
            point if self == Self::Contains && question => {
                let point = match point {
                    Value::Timestamp { value, timezone: Some(_), .. } => *value,
                    Value::Timestamp { unit, .. } => {
                        let Value::Timestamp { value, .. } = cast(point, &LogicalType::Scalar(ScalarType::Timestamp { unit: *unit, timezone: Some(context.timezone.into()) }), context.timezone)? else { return Err(inapplicable(name, &[point])); };
                        value
                    }
                    Value::Date(_) | Value::DateDay(_) | Value::DateMilli(_) => {
                        let Value::Timestamp { value, .. } = cast(point, &LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some(context.timezone.into()) }), context.timezone)? else { return Err(inapplicable(name, &[point])); };
                        value
                    }
                    _ => return Err(inapplicable(name, &[point])),
                };
                return Ok(Value::Bool(*lf <= point && point < *lt));
            }
            point => return Err(inapplicable(name, &[&args[0], point])),
        };
        let (lf, lt) = (*lf, *lt);
        Ok(Value::Bool(match self {
            Self::Contains => lf <= rf && lt >= rt,
            Self::StrictlyContains => lf < rf && lt > rt,
            Self::Overlaps => lf < rt && lt > rf,
            Self::StrictlyOverlaps => lf > rf && lt < rt,
            Self::Equals => lf == rf && lt == rt,
            Self::Precedes => lt <= rf,
            Self::StrictlyPrecedes => lt < rf,
            Self::ImmediatelyPrecedes => lt == rf,
            Self::Succeeds => lf >= rt,
            Self::StrictlySucceeds => lf > rt,
            Self::ImmediatelySucceeds => lf == rt,
            Self::Leads => lf < rf && rf < lt && lt <= rt,
            Self::StrictlyLeads => lf < rf && rf < lt && lt < rt,
            Self::ImmediatelyLeads => lf < rf && lt == rt,
            Self::Lags => lf >= rf && rf < lt && lt > rt,
            Self::StrictlyLags => lf > rf && rf < lt && lt > rt,
            Self::ImmediatelyLags => lf == rf && lt > rt,
        }))
    }
}
