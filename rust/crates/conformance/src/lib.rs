use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub mod controls;
pub mod faults;
pub mod history;
pub mod sql_logic;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CaseIdentity {
    pub reference_revision: String,
    pub source: String,
    pub qualified_name: String,
    pub parameter_fixture: String,
    pub configuration: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Comparison { Exact, TypedRowMultiset }

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExpectedOutcome {
    Returned,
    Failure { stage: String, category: String, code: String, signature: Value },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequiredObservation {
    pub id: String,
    pub comparison: Comparison,
    pub expected: ExpectedOutcome,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CaseMapping {
    pub identity: CaseIdentity,
    pub source_assertion_lines: Vec<usize>,
    pub counterparts: Vec<String>,
    pub observations: Vec<RequiredObservation>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ledger {
    pub schema_version: u32,
    pub inventory_revision: String,
    pub inventory_complete: bool,
    pub runtime_reconciled: bool,
    pub cases: Vec<CaseMapping>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunContext {
    pub binary_sha256: String,
    pub fixture_sha256: String,
    pub configuration_sha256: String,
    pub artifact_revision: String,
    pub applied_basis: Value,
    pub valid_time: Value,
    pub system_time: Value,
    pub clock: String,
    pub timezone: String,
    pub storage_version: i32,
    pub features: BTreeSet<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Outcome {
    Returned { typed_value: Value },
    ExpectedFailure { stage: String, category: String, code: String, signature: Value },
    UnexpectedFailure { stage: String, code: String, detail: String },
    Unavailable { reason: String },
    Skipped { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObservationReceipt {
    pub id: String,
    pub executed: bool,
    pub outcome: Outcome,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CaseReceipt {
    pub identity: CaseIdentity,
    pub context: RunContext,
    pub runner: String,
    pub trace_sha256: String,
    pub observations: Vec<ObservationReceipt>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Difference {
    pub observation: String,
    pub path: String,
    pub reason: String,
    pub reference: Option<Value>,
    pub native: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Disposition { PassingBoth, MatchingExpectedFailure, Mismatch, MissingEvidence }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CaseReport {
    pub identity: CaseIdentity,
    pub disposition: Disposition,
    pub comparisons: usize,
    pub differences: Vec<Difference>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Report {
    pub comparator_version: String,
    pub required_groups: usize,
    pub passing_both: usize,
    pub matching_expected_failure: usize,
    pub missing_evidence: usize,
    pub mismatches: usize,
    pub strict_agreement: Option<f64>,
    pub complete_parity: bool,
    pub cases: Vec<CaseReport>,
    pub manifest_errors: Vec<String>,
}

fn canonical(value: &Value) -> String {
    match value {
        Value::Object(object) => {
            let sorted: BTreeMap<_, _> = object.iter().map(|(key, value)| (key, canonical(value))).collect();
            let values: Vec<_> = sorted.into_iter().map(|(key, value)| format!("{}:{value}", serde_json::to_string(key).expect("JSON string"))).collect();
            format!("{{{}}}", values.join(","))
        }
        Value::Array(array) => format!("[{}]", array.iter().map(canonical).collect::<Vec<_>>().join(",")),
        _ => value.to_string(),
    }
}

fn escape_pointer(key: &str) -> String { key.replace('~', "~0").replace('/', "~1") }

fn first_difference(left: &Value, right: &Value, path: &str) -> Option<(String, Option<Value>, Option<Value>)> {
    if left == right { return None; }
    match (left, right) {
        (Value::Array(left), Value::Array(right)) => {
            for index in 0..left.len().max(right.len()) {
                let path = format!("{path}/{index}");
                match (left.get(index), right.get(index)) {
                    (Some(left), Some(right)) => if let Some(difference) = first_difference(left, right, &path) { return Some(difference); },
                    (left, right) => return Some((path, left.cloned(), right.cloned())),
                }
            }
            None
        }
        (Value::Object(left), Value::Object(right)) => {
            let keys: BTreeSet<_> = left.keys().chain(right.keys()).collect();
            for key in keys {
                let path = format!("{path}/{}", escape_pointer(key));
                match (left.get(key), right.get(key)) {
                    (Some(left), Some(right)) => if let Some(difference) = first_difference(left, right, &path) { return Some(difference); },
                    (left, right) => return Some((path, left.cloned(), right.cloned())),
                }
            }
            None
        }
        _ => Some((path.to_owned(), Some(left.clone()), Some(right.clone()))),
    }
}

fn normalized(value: &Value, comparison: Comparison) -> Result<Value, String> {
    match comparison {
        Comparison::Exact => Ok(value.clone()),
        Comparison::TypedRowMultiset => {
            let mut result = value.clone();
            let schema = result.get("schema").and_then(Value::as_array)
                .ok_or_else(|| "typed multiset requires an explicit schema array".to_owned())?;
            if schema.iter().any(|field| !field.is_string() && !field.is_object()) {
                return Err("typed multiset schema fields must identify their types".into());
            }
            let columns = schema.len();
            let rows = result.get_mut("rows").and_then(Value::as_array_mut)
                .ok_or_else(|| "typed multiset requires an object with a rows array and an explicit schema".to_owned())?;
            if rows.iter().any(|row| row.as_array().is_none_or(|row| row.len() != columns)) {
                return Err("typed multiset rows must match the schema width".into());
            }
            rows.sort_by_cached_key(canonical);
            Ok(result)
        }
    }
}

fn evidence_error(observation: &str, reason: impl Into<String>) -> Difference {
    Difference { observation: observation.into(), path: String::new(), reason: reason.into(), reference: None, native: None }
}

fn valid_digest(value: &str) -> bool { value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) }

fn nonempty(value: &str) -> bool { !value.trim().is_empty() }

fn valid_identity(identity: &CaseIdentity) -> bool {
    [&identity.reference_revision, &identity.source, &identity.qualified_name, &identity.parameter_fixture, &identity.configuration]
        .into_iter().all(|value| nonempty(value))
}

fn valid_expected(expected: &ExpectedOutcome) -> bool {
    match expected {
        ExpectedOutcome::Returned => true,
        ExpectedOutcome::Failure { stage, category, code, .. } => [stage, category, code].into_iter().all(|value| nonempty(value)),
    }
}

fn expected_outcome(expected: &ExpectedOutcome, outcome: &Outcome) -> bool {
    match (expected, outcome) {
        (ExpectedOutcome::Returned, Outcome::Returned { .. }) => true,
        (ExpectedOutcome::Failure { stage, category, code, signature }, Outcome::ExpectedFailure {
            stage: actual_stage, category: actual_category, code: actual_code, signature: actual_signature,
        }) => stage == actual_stage && category == actual_category && code == actual_code && signature == actual_signature,
        _ => false,
    }
}

fn compare_case(mapping: &CaseMapping, reference: Option<&CaseReceipt>, native: Option<&CaseReceipt>) -> CaseReport {
    let mut report = CaseReport { identity: mapping.identity.clone(), disposition: Disposition::PassingBoth, comparisons: 0, differences: Vec::new() };
    let (Some(reference), Some(native)) = (reference, native) else {
        report.disposition = Disposition::MissingEvidence;
        report.differences.push(evidence_error("receipt", "paired case receipt missing"));
        return report;
    };
    if reference.context.artifact_revision != mapping.identity.reference_revision {
        report.disposition = Disposition::MissingEvidence;
        report.differences.push(evidence_error("reference", "reference artifact revision differs from the pinned case revision"));
    }
    for (side, receipt) in [("reference", reference), ("native", native)] {
        if !nonempty(&receipt.runner) || !valid_digest(&receipt.context.binary_sha256)
            || !valid_digest(&receipt.context.fixture_sha256) || !valid_digest(&receipt.context.configuration_sha256)
            || !valid_digest(&receipt.trace_sha256) || !nonempty(&receipt.context.artifact_revision)
            || !nonempty(&receipt.context.clock) || !nonempty(&receipt.context.timezone)
            || receipt.context.storage_version < 0 || receipt.context.features.iter().any(|feature| !nonempty(feature))
        {
            report.disposition = Disposition::MissingEvidence;
            report.differences.push(evidence_error("provenance", format!("{side} receipt has incomplete artifact or trace provenance")));
        }
    }
    let mut reference_context = reference.context.clone();
    let mut native_context = native.context.clone();
    reference_context.binary_sha256.clear();
    native_context.binary_sha256.clear();
    reference_context.artifact_revision.clear();
    native_context.artifact_revision.clear();
    if reference_context != native_context {
        report.disposition = Disposition::MissingEvidence;
        report.differences.push(evidence_error("basis", "paired observations used different fixtures, configurations, clocks or visibility bases"));
    }
    if !valid_identity(&mapping.identity) || mapping.observations.is_empty() || mapping.counterparts.is_empty()
        || mapping.counterparts.iter().any(|counterpart| !nonempty(counterpart))
        || mapping.source_assertion_lines.is_empty() || mapping.source_assertion_lines.contains(&0)
    {
        report.disposition = Disposition::MissingEvidence;
        report.differences.push(evidence_error("mapping", "case lacks assertion locations, counterparts, or required observations"));
    }
    let mut reference_observations = BTreeMap::new();
    let mut native_observations = BTreeMap::new();
    for (receipt, observations) in [(reference, &mut reference_observations), (native, &mut native_observations)] {
        for observation in &receipt.observations {
            if observations.insert(observation.id.as_str(), observation).is_some() {
                report.disposition = Disposition::MissingEvidence;
                report.differences.push(evidence_error(&observation.id, "duplicate observation receipt"));
            }
        }
    }
    let mut expected_failure = false;
    let mut required_ids = BTreeSet::new();
    for required in &mapping.observations {
        if !nonempty(&required.id) || !valid_expected(&required.expected) {
            if report.disposition != Disposition::Mismatch { report.disposition = Disposition::MissingEvidence; }
            report.differences.push(evidence_error(&required.id, "required observation has no identity or complete expected outcome"));
            continue;
        }
        if !required_ids.insert(&required.id) {
            if report.disposition != Disposition::Mismatch { report.disposition = Disposition::MissingEvidence; }
            report.differences.push(evidence_error(&required.id, "duplicate required observation"));
            continue;
        }
        let (Some(reference), Some(native)) = (reference_observations.get(required.id.as_str()), native_observations.get(required.id.as_str())) else {
            if report.disposition != Disposition::Mismatch { report.disposition = Disposition::MissingEvidence; }
            report.differences.push(evidence_error(&required.id, "required observation was not recorded on both sides"));
            continue;
        };
        if !reference.executed || !native.executed {
            if report.disposition != Disposition::Mismatch { report.disposition = Disposition::MissingEvidence; }
            report.differences.push(evidence_error(&required.id, "required observation was not executed"));
            continue;
        }
        if !matches!(&reference.outcome, Outcome::Unavailable { .. } | Outcome::Skipped { .. })
            && !matches!(&native.outcome, Outcome::Unavailable { .. } | Outcome::Skipped { .. })
            && (!expected_outcome(&required.expected, &reference.outcome) || !expected_outcome(&required.expected, &native.outcome))
        {
            report.disposition = Disposition::Mismatch;
            report.differences.push(evidence_error(&required.id, "execution outcome does not match the mapped expectation"));
            continue;
        }
        let values = match (&reference.outcome, &native.outcome) {
            (Outcome::Returned { typed_value: reference }, Outcome::Returned { typed_value: native }) => {
                normalized(reference, required.comparison).and_then(|reference| normalized(native, required.comparison).map(|native| (reference, native)))
            }
            (Outcome::ExpectedFailure { .. }, Outcome::ExpectedFailure { .. }) => {
                expected_failure = true;
                Ok((serde_json::to_value(&reference.outcome).expect("outcome serialization"), serde_json::to_value(&native.outcome).expect("outcome serialization")))
            }
            (Outcome::Unavailable { .. } | Outcome::Skipped { .. }, _) | (_, Outcome::Unavailable { .. } | Outcome::Skipped { .. }) => Err("observation unavailable or skipped".into()),
            _ => {
                report.disposition = Disposition::Mismatch;
                report.differences.push(evidence_error(&required.id, "outcomes differ or include an unexpected execution failure"));
                continue;
            }
        };
        match values {
            Err(reason) => {
                if report.disposition != Disposition::Mismatch { report.disposition = Disposition::MissingEvidence; }
                report.differences.push(evidence_error(&required.id, reason));
            }
            Ok((left, right)) => {
                report.comparisons += 1;
                if let Some((path, reference, native)) = first_difference(&left, &right, "") {
                    report.disposition = Disposition::Mismatch;
                    report.differences.push(Difference { observation: required.id.clone(), path, reason: "observation differs".into(), reference, native });
                }
            }
        }
    }
    for id in reference_observations.keys().chain(native_observations.keys()) {
        if !required_ids.iter().any(|required| required.as_str() == *id) {
            if report.disposition != Disposition::Mismatch { report.disposition = Disposition::MissingEvidence; }
            report.differences.push(evidence_error(id, "receipt observation has no assertion mapping"));
        }
    }
    if expected_failure && report.disposition == Disposition::PassingBoth { report.disposition = Disposition::MatchingExpectedFailure; }
    report
}

/// Counts assertion groups only after every required observation has paired execution evidence.
pub fn compare(ledger: &Ledger, reference: &[CaseReceipt], native: &[CaseReceipt]) -> Report {
    let mut report = Report { comparator_version: env!("CARGO_PKG_VERSION").into(), required_groups: ledger.cases.len(),
        passing_both: 0, matching_expected_failure: 0, missing_evidence: 0, mismatches: 0,
        strict_agreement: None, complete_parity: false, cases: Vec::new(), manifest_errors: Vec::new() };
    if ledger.schema_version != 1 { report.manifest_errors.push("unsupported ledger schema version".into()); }
    let mut reference_by_case = BTreeMap::new();
    let mut native_by_case = BTreeMap::new();
    let mut duplicate_receipts = BTreeSet::new();
    for (side, receipts, indexed) in [("reference", reference, &mut reference_by_case), ("native", native, &mut native_by_case)] {
        for receipt in receipts {
            if indexed.insert(&receipt.identity, receipt).is_some() {
                duplicate_receipts.insert(&receipt.identity);
                report.manifest_errors.push(format!("duplicate {side} case receipt: {:?}", receipt.identity));
            }
        }
    }
    let mut identities = BTreeSet::new();
    let mut mapping_counts = BTreeMap::new();
    for mapping in &ledger.cases { *mapping_counts.entry(&mapping.identity).or_insert(0usize) += 1; }
    for mapping in &ledger.cases {
        if !identities.insert(&mapping.identity) { report.manifest_errors.push(format!("duplicate ledger identity: {:?}", mapping.identity)); }
        if mapping.identity.reference_revision != ledger.inventory_revision { report.manifest_errors.push("ledger case belongs to another reference revision".into()); }
        let mut case = compare_case(mapping, reference_by_case.get(&mapping.identity).copied(), native_by_case.get(&mapping.identity).copied());
        if duplicate_receipts.contains(&mapping.identity) || mapping_counts[&mapping.identity] > 1 {
            if case.disposition != Disposition::Mismatch { case.disposition = Disposition::MissingEvidence; }
            case.differences.push(evidence_error("receipt", "duplicate case mappings or receipts cannot establish an assertion outcome"));
        }
        match case.disposition {
            Disposition::PassingBoth => report.passing_both += 1,
            Disposition::MatchingExpectedFailure => report.matching_expected_failure += 1,
            Disposition::Mismatch => report.mismatches += 1,
            Disposition::MissingEvidence => report.missing_evidence += 1,
        }
        report.cases.push(case);
    }
    for receipt in reference.iter().chain(native) {
        if !identities.contains(&receipt.identity) { report.manifest_errors.push(format!("receipt has no mapping: {:?}", receipt.identity)); }
    }
    if !ledger.inventory_complete { report.manifest_errors.push("source inventory is incomplete".into()); }
    if !ledger.runtime_reconciled { report.manifest_errors.push("runtime discovery has not been reconciled".into()); }
    if report.required_groups == 0 { report.manifest_errors.push("no required assertion groups".into()); }
    if report.manifest_errors.is_empty() {
        report.strict_agreement = Some((report.passing_both + report.matching_expected_failure) as f64 / report.required_groups as f64);
        report.complete_parity = report.missing_evidence == 0 && report.mismatches == 0;
    }
    report
}
