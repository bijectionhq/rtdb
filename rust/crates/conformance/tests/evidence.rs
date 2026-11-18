use rtdb_conformance::*;
use serde_json::{Value, json};
use std::collections::BTreeSet;

fn paired(observation: &str, value: Value, comparison: Comparison) -> (Ledger, CaseReceipt, CaseReceipt) {
    let identity = CaseIdentity {
        reference_revision: "c84f9334eb060ceb92d28ecdd4a06e6b625932fd".into(),
        source: "fixture".into(), qualified_name: "control".into(), parameter_fixture: "fixed".into(), configuration: "local".into(),
    };
    let ledger = Ledger { schema_version: 1, inventory_revision: identity.reference_revision.clone(), inventory_complete: true,
        runtime_reconciled: true, cases: vec![CaseMapping { identity: identity.clone(), source_assertion_lines: vec![1],
            counterparts: vec!["control".into()], observations: vec![RequiredObservation { id: observation.into(), comparison, expected: ExpectedOutcome::Returned }] }] };
    let receipt = CaseReceipt {
        identity, runner: "fixture-control".into(), trace_sha256: "a".repeat(64),
        context: RunContext {
            binary_sha256: "b".repeat(64), fixture_sha256: "c".repeat(64), configuration_sha256: "d".repeat(64),
            artifact_revision: "c84f9334eb060ceb92d28ecdd4a06e6b625932fd".into(), applied_basis: json!({"transaction": 2}),
            valid_time: json!([0, 100]), system_time: json!([0, 3]), clock: "2026-07-01T00:00:00Z".into(),
            timezone: "UTC".into(), storage_version: 6, features: BTreeSet::new(),
        },
        observations: vec![ObservationReceipt { id: observation.into(), executed: true, outcome: Outcome::Returned { typed_value: value } }],
    };
    (ledger, receipt.clone(), receipt)
}

fn returned(receipt: &mut CaseReceipt) -> &mut Value {
    match &mut receipt.observations[0].outcome { Outcome::Returned { typed_value } => typed_value, _ => panic!("returned observation required") }
}

#[test]
fn wrong_ceiling_is_reported_at_the_precise_observation_and_element() {
    let (ledger, reference, mut native) = paired("iid-a/earlier-ceiling", json!({"valid_times": [0,10,20,100], "system_time_ceilings": [i64::MAX,2,i64::MAX]}), Comparison::Exact);
    returned(&mut native)["system_time_ceilings"][1] = json!(3);
    let report = compare(&ledger, &[reference], &[native]);
    assert!(!report.complete_parity);
    assert_eq!(report.cases[0].differences[0].observation, "iid-a/earlier-ceiling");
    assert_eq!(report.cases[0].differences[0].path, "/system_time_ceilings/1");
    assert_eq!(report.cases[0].differences[0].reference, Some(json!(2)));
}

#[test]
fn absent_term_sequence_is_not_an_explicit_zero() {
    let (ledger, reference, mut native) = paired("legacy/term-sequence", json!({"term_sequence": null}), Comparison::Exact);
    returned(&mut native)["term_sequence"] = json!(0);
    let report = compare(&ledger, &[reference], &[native]);
    assert_eq!(report.mismatches, 1);
    assert_eq!(report.cases[0].differences[0].path, "/term_sequence");
}

#[test]
fn erased_entity_control_cannot_be_removed_from_the_result() {
    let (ledger, reference, mut native) = paired("erase/surviving-control", json!({"schema": ["_id"], "rows": [[{"utf8":"B"}]]}), Comparison::Exact);
    returned(&mut native)["rows"] = json!([]);
    let report = compare(&ledger, &[reference], &[native]);
    assert_eq!(report.cases[0].differences[0].path, "/rows/0");
}

#[test]
fn unordered_comparison_preserves_duplicates_and_schema() {
    let (ledger, reference, mut native) = paired("rows", json!({"schema": ["i64"], "rows": [[1],[2],[1]]}), Comparison::TypedRowMultiset);
    returned(&mut native)["rows"] = json!([[2],[1],[1]]);
    assert!(compare(&ledger, &[reference.clone()], &[native.clone()]).complete_parity);
    returned(&mut native)["rows"] = json!([[2],[1]]);
    assert_eq!(compare(&ledger, &[reference.clone()], &[native.clone()]).mismatches, 1);
    returned(&mut native)["rows"] = json!([[2],[1],[1]]);
    returned(&mut native)["schema"] = json!(["i32"]);
    assert_eq!(compare(&ledger, &[reference], &[native]).mismatches, 1);
}

#[test]
fn unexecuted_or_absent_observations_never_count_as_agreement() {
    let (ledger, reference, mut native) = paired("required", json!(true), Comparison::Exact);
    native.observations[0].executed = false;
    assert_eq!(compare(&ledger, &[reference.clone()], &[native.clone()]).missing_evidence, 1);
    native.observations.clear();
    assert_eq!(compare(&ledger, &[reference], &[native]).missing_evidence, 1);
}

#[test]
fn matching_skips_are_unverified() {
    let (ledger, mut reference, mut native) = paired("cloud", json!(true), Comparison::Exact);
    reference.observations[0].outcome = Outcome::Skipped { reason: "credentials unavailable".into() };
    native.observations[0].outcome = Outcome::Skipped { reason: "credentials unavailable".into() };
    let report = compare(&ledger, &[reference], &[native]);
    assert_eq!(report.missing_evidence, 1);
    assert!(!report.complete_parity);
}

#[test]
fn different_applied_histories_cannot_be_paired_by_sql_time_alone() {
    let (ledger, reference, mut native) = paired("history", json!(true), Comparison::Exact);
    native.context.applied_basis = json!({"transaction": 3});
    assert_eq!(compare(&ledger, &[reference], &[native]).missing_evidence, 1);
}

#[test]
fn identical_unexpected_crashes_are_not_matching_expected_failures() {
    let (ledger, mut reference, mut native) = paired("query", json!(true), Comparison::Exact);
    reference.observations[0].outcome = Outcome::UnexpectedFailure { stage: "prepare".into(), code: "crash".into(), detail: "process exited".into() };
    native.observations[0].outcome = reference.observations[0].outcome.clone();
    assert_eq!(compare(&ledger, &[reference], &[native]).mismatches, 1);
}

#[test]
fn an_unreconciled_inventory_cannot_claim_complete_parity() {
    let (mut ledger, reference, native) = paired("observation", json!(true), Comparison::Exact);
    ledger.runtime_reconciled = false;
    let report = compare(&ledger, &[reference], &[native]);
    assert!(!report.complete_parity);
    assert!(report.strict_agreement.is_none());
}

#[test]
fn duplicate_receipts_and_unmapped_observations_block_a_parity_claim() {
    let (ledger, reference, mut native) = paired("observation", json!(true), Comparison::Exact);
    let duplicate = compare(&ledger, &[reference.clone(), reference.clone()], &[native.clone()]);
    assert!(!duplicate.complete_parity);
    assert_eq!(duplicate.passing_both, 0);
    assert_eq!(duplicate.missing_evidence, 1);
    native.observations.push(ObservationReceipt { id: "unmapped".into(), executed: true, outcome: Outcome::Returned { typed_value: json!(42) } });
    assert_eq!(compare(&ledger, &[reference], &[native]).missing_evidence, 1);
}

#[test]
fn receipts_cannot_declare_their_own_expected_failure() {
    let (ledger, mut reference, mut native) = paired("query", json!(true), Comparison::Exact);
    reference.observations[0].outcome = Outcome::ExpectedFailure {
        stage: "execute".into(), category: "fault".into(), code: "broken".into(), signature: json!({"detail": "failed"}),
    };
    native.observations[0].outcome = reference.observations[0].outcome.clone();
    let report = compare(&ledger, &[reference], &[native]);
    assert_eq!(report.matching_expected_failure, 0);
    assert_eq!(report.mismatches, 1);
    assert!(!report.complete_parity);
}

#[test]
fn expected_failures_match_the_ledger_signature_and_stage() {
    let (mut ledger, mut reference, mut native) = paired("query", json!(true), Comparison::Exact);
    ledger.cases[0].observations[0].expected = ExpectedOutcome::Failure {
        stage: "prepare".into(), category: "incorrect".into(), code: "syntax".into(), signature: json!({"position": 4}),
    };
    reference.observations[0].outcome = Outcome::ExpectedFailure {
        stage: "prepare".into(), category: "incorrect".into(), code: "syntax".into(), signature: json!({"position": 4}),
    };
    native.observations[0].outcome = reference.observations[0].outcome.clone();
    let report = compare(&ledger, &[reference.clone()], &[native.clone()]);
    assert_eq!(report.matching_expected_failure, 1);
    assert!(report.complete_parity);
    native.observations[0].outcome = Outcome::ExpectedFailure {
        stage: "execute".into(), category: "incorrect".into(), code: "syntax".into(), signature: json!({"position": 4}),
    };
    reference.observations[0].outcome = native.observations[0].outcome.clone();
    assert_eq!(compare(&ledger, &[reference], &[native]).mismatches, 1);
}

#[test]
fn malformed_rows_and_null_schemas_cannot_count_as_typed_evidence() {
    for value in [json!({"schema": null, "rows": []}), json!({"schema": ["i64"], "rows": [1]}),
        json!({"schema": ["i64"], "rows": [[1, 2]]}), json!({"schema": [null], "rows": [[1]]})]
    {
        let (ledger, reference, native) = paired("rows", value, Comparison::TypedRowMultiset);
        let report = compare(&ledger, &[reference], &[native]);
        assert_eq!(report.missing_evidence, 1);
        assert!(!report.complete_parity);
    }
}

#[test]
fn missing_later_observations_do_not_erase_an_earlier_mismatch() {
    let (mut ledger, reference, mut native) = paired("first", json!(1), Comparison::Exact);
    *returned(&mut native) = json!(2);
    ledger.cases[0].observations.push(RequiredObservation {
        id: "missing".into(), comparison: Comparison::Exact, expected: ExpectedOutcome::Returned,
    });
    let report = compare(&ledger, &[reference], &[native]);
    assert_eq!(report.mismatches, 1);
    assert_eq!(report.cases[0].differences.len(), 2);
}

#[test]
fn zero_assertion_lines_and_blank_observation_ids_are_unverified() {
    let (mut ledger, reference, native) = paired(" ", json!(1), Comparison::Exact);
    ledger.cases[0].source_assertion_lines = vec![0];
    let report = compare(&ledger, &[reference], &[native]);
    assert_eq!(report.missing_evidence, 1);
    assert!(!report.complete_parity);
}

#[test]
fn matching_blank_clocks_are_missing_provenance() {
    let (ledger, mut reference, mut native) = paired("basis", json!(true), Comparison::Exact);
    reference.context.clock.clear();
    native.context.clock.clear();
    assert_eq!(compare(&ledger, &[reference], &[native]).missing_evidence, 1);
}

#[test]
fn duplicate_ledger_groups_are_never_counted_as_passing_groups() {
    let (mut ledger, reference, native) = paired("observation", json!(true), Comparison::Exact);
    ledger.cases.push(ledger.cases[0].clone());
    let report = compare(&ledger, &[reference], &[native]);
    assert_eq!(report.passing_both, 0);
    assert_eq!(report.missing_evidence, 2);
    assert!(report.strict_agreement.is_none());
}

fn embedded_migration() -> (Ledger, CaseReceipt, CaseReceipt) {
    let (mut ledger, mut reference, mut native) = paired("parser-cause", Value::Null, Comparison::Exact);
    ledger.cases[0].observations[0].expected = serde_json::from_value(json!({
        "kind": "declared_migration",
        "migration": "embedded_sql_parser_cause",
        "reference": {
            "stage": "parse_statement_ast", "category": "incorrect", "code": "xtdb/sql-error",
            "signature": {"cause": {
                "class": "org.antlr.v4.runtime.InputMismatchException",
                "recognizer_class": "xtdb.antlr.Sql",
                "offending_state": 574,
                "shared_decision_array": true,
            }},
        },
        "native": {
            "stage": "statement_prepare", "category": "incorrect", "code": "xtdb/sql-error",
            "signature": {"cause": null},
        },
    })).unwrap();
    reference.observations[0].outcome = Outcome::ExpectedFailure {
        stage: "parse_statement_ast".into(), category: "incorrect".into(), code: "xtdb/sql-error".into(),
        signature: json!({"cause": {
            "class": "org.antlr.v4.runtime.InputMismatchException",
            "recognizer_class": "xtdb.antlr.Sql",
            "offending_state": 574,
            "shared_decision_array": true,
        }}),
    };
    native.observations[0].outcome = Outcome::ExpectedFailure {
        stage: "statement_prepare".into(), category: "incorrect".into(), code: "xtdb/sql-error".into(),
        signature: json!({"cause": null}),
    };
    native.context.artifact_revision = "implementation-revision".into();
    native.context.binary_sha256 = "e".repeat(64);
    native.trace_sha256 = "f".repeat(64);
    (ledger, reference, native)
}

#[test]
fn matched_declared_migration_stays_in_the_denominator_and_out_of_strict_parity() {
    let (mut ledger, reference, native) = embedded_migration();
    let (mut equal_ledger, mut equal_reference, mut equal_native) = paired("value", json!(42), Comparison::Exact);
    equal_ledger.cases[0].identity.qualified_name = "ordinary-parity".into();
    equal_reference.identity = equal_ledger.cases[0].identity.clone();
    equal_native.identity = equal_ledger.cases[0].identity.clone();
    ledger.cases.append(&mut equal_ledger.cases);
    let report = compare(&ledger, &[reference, equal_reference], &[native, equal_native]);
    assert_eq!(report.required_groups, 2);
    assert_eq!(report.passing_both, 1);
    assert_eq!(report.matching_expected_failure, 0);
    assert_eq!(report.matched_declared_migration, 1);
    assert_eq!((report.mismatches, report.missing_evidence), (0, 0));
    assert_eq!(report.strict_agreement, Some(0.5));
    assert!(!report.complete_parity);
    assert_eq!(report.cases[0].comparisons, 1);
    assert_eq!(report.cases[0].disposition, Disposition::MatchedDeclaredMigration);
    assert!(report.cases[0].differences.is_empty());
    assert_eq!(serde_json::to_value(&report).unwrap()["matched_declared_migration"], json!(1));
}

#[test]
fn swapped_or_changed_migration_failure_outcomes_are_mismatches() {
    let (ledger, reference, native) = embedded_migration();
    let mut swapped_reference = reference.clone();
    let mut swapped_native = native.clone();
    std::mem::swap(&mut swapped_reference.observations[0].outcome, &mut swapped_native.observations[0].outcome);
    let mut changed_signature = native.clone();
    let Outcome::ExpectedFailure { signature, .. } = &mut changed_signature.observations[0].outcome else {
        panic!("native failure required");
    };
    *signature = json!({});
    let mut changed_stage = native.clone();
    let Outcome::ExpectedFailure { stage, .. } = &mut changed_stage.observations[0].outcome else {
        panic!("native failure required");
    };
    *stage = "execute".into();
    let mut identical_failure = native.clone();
    identical_failure.observations[0].outcome = reference.observations[0].outcome.clone();
    for (reference, native, sides) in [
        (swapped_reference, swapped_native, &["reference", "native"][..]),
        (reference.clone(), changed_signature, &["native"][..]),
        (reference.clone(), changed_stage, &["native"][..]),
        (reference, identical_failure, &["native"][..]),
    ] {
        let report = compare(&ledger, &[reference], &[native]);
        assert_eq!(report.mismatches, 1);
        assert_eq!(report.matched_declared_migration, 0);
        assert_eq!(report.cases[0].comparisons, 0);
        assert_eq!(report.cases[0].differences.len(), sides.len());
        for (difference, side) in report.cases[0].differences.iter().zip(sides.iter().copied()) {
            assert_eq!(difference.observation, "parser-cause");
            assert!(difference.reason.starts_with(side));
        }
    }
}

#[test]
fn incomplete_or_unauditable_migration_evidence_cannot_confirm_the_declaration() {
    let (ledger, reference, native) = embedded_migration();
    let mut unexecuted = native.clone();
    unexecuted.observations[0].executed = false;
    let mut unavailable = native.clone();
    unavailable.observations[0].outcome = Outcome::Unavailable { reason: "runner unavailable".into() };
    let mut skipped = native.clone();
    skipped.observations[0].outcome = Outcome::Skipped { reason: "unselected".into() };
    let mut untraced = native.clone();
    untraced.trace_sha256.clear();
    let mut different_fixture = native.clone();
    different_fixture.context.fixture_sha256 = "a".repeat(64);
    for native_receipts in [
        vec![], vec![unexecuted], vec![unavailable], vec![skipped], vec![untraced], vec![different_fixture],
        vec![native.clone(), native],
    ] {
        let report = compare(&ledger, std::slice::from_ref(&reference), &native_receipts);
        assert_eq!(report.missing_evidence, 1);
        assert_eq!(report.matched_declared_migration, 0);
        assert!(!report.complete_parity);
    }
}

#[test]
fn matched_migration_observation_does_not_hide_a_missing_or_mismatched_companion() {
    let (mut ledger, mut reference, mut native) = embedded_migration();
    ledger.cases[0].observations.push(RequiredObservation {
        id: "healthy-follow-up".into(), comparison: Comparison::Exact, expected: ExpectedOutcome::Returned,
    });
    let missing = compare(&ledger, std::slice::from_ref(&reference), std::slice::from_ref(&native));
    assert_eq!(missing.missing_evidence, 1);
    assert_eq!(missing.matched_declared_migration, 0);
    reference.observations.push(ObservationReceipt {
        id: "healthy-follow-up".into(), executed: true, outcome: Outcome::Returned { typed_value: json!(42) },
    });
    native.observations.push(ObservationReceipt {
        id: "healthy-follow-up".into(), executed: true, outcome: Outcome::Returned { typed_value: json!(43) },
    });
    let mismatch = compare(&ledger, &[reference], &[native]);
    assert_eq!(mismatch.mismatches, 1);
    assert_eq!(mismatch.matched_declared_migration, 0);
    assert_eq!(mismatch.cases[0].differences[0].observation, "healthy-follow-up");
}

#[test]
fn migration_declarations_cannot_claim_unapproved_stages_codes_or_cause_shapes() {
    let (ledger, reference, native) = embedded_migration();
    for (pointer, value) in [
        ("/reference/stage", json!("execute")),
        ("/native/code", json!("xtdb/other-error")),
        ("/reference/signature/cause/shared_decision_array", json!(false)),
        ("/native/signature", json!({})),
    ] {
        let mut changed = ledger.clone();
        let mut expectation = serde_json::to_value(&changed.cases[0].observations[0].expected).unwrap();
        *expectation.pointer_mut(pointer).unwrap() = value;
        changed.cases[0].observations[0].expected = serde_json::from_value(expectation.clone()).unwrap();
        let mut reference = reference.clone();
        let mut native = native.clone();
        for (side, receipt) in [("reference", &mut reference), ("native", &mut native)] {
            let mut actual = expectation[side].clone();
            actual["kind"] = json!("expected_failure");
            receipt.observations[0].outcome = serde_json::from_value(actual).unwrap();
        }
        let report = compare(&changed, &[reference], &[native]);
        assert_eq!(report.missing_evidence, 1);
        assert_eq!(report.matched_declared_migration, 0);
    }
    let mut changed = ledger;
    changed.cases[0].observations[0].comparison = Comparison::TypedRowMultiset;
    let report = compare(&changed, &[reference], &[native]);
    assert_eq!(report.missing_evidence, 1);
    assert_eq!(report.matched_declared_migration, 0);
}

#[test]
fn equality_only_reports_keep_their_original_serialized_shape() {
    let (ledger, reference, native) = paired("ordinary", json!(42), Comparison::Exact);
    let report = compare(&ledger, &[reference], &[native]);
    let legacy = serde_json::to_value(&report).unwrap();
    assert!(legacy.get("matched_declared_migration").is_none());
    let decoded: Report = serde_json::from_value(legacy).unwrap();
    assert_eq!(decoded.matched_declared_migration, 0);
    assert_eq!(decoded.strict_agreement, Some(1.0));
    assert!(decoded.complete_parity);
}
