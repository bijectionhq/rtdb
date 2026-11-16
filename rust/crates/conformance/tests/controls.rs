use rtdb_conformance::{Outcome, controls::observe};
use serde_json::{Value, json};

const TEMPORAL: &[u8] = include_bytes!("../../../conformance/fixtures/temporal-ceilings.json");
const TERM: &[u8] = include_bytes!("../../../conformance/fixtures/term-sequence.json");

fn values(bytes: &[u8]) -> serde_json::Map<String, Value> {
    observe(bytes).unwrap().into_iter().map(|observation| {
        assert!(observation.executed);
        let Outcome::Returned { typed_value } = observation.outcome else { panic!("control did not return") };
        (observation.id, typed_value)
    }).collect()
}

#[test]
fn temporal_runner_reads_actual_event_inputs_and_never_expected_observations() {
    let mut fixture: Value = serde_json::from_slice(TEMPORAL).unwrap();
    let expected = fixture["expected_observations"].clone();
    assert_eq!(Value::Object(values(TEMPORAL)), expected);
    fixture["expected_observations"] = json!({"fabricated": "never executed"});
    assert_eq!(Value::Object(values(&serde_json::to_vec(&fixture).unwrap())), expected);
    fixture["events"][0]["system_from"] = json!(3);
    let changed = values(&serde_json::to_vec(&fixture).unwrap());
    assert_eq!(changed["iid-a/earlier/system-time-ceilings"], json!([i64::MAX, 3, i64::MAX]));
    assert_eq!(changed["iid-b/system-time-ceilings"], json!([i64::MAX]));
}

#[test]
fn term_runner_round_trips_wire_presence_and_ignores_expected_fields() {
    let mut fixture: Value = serde_json::from_slice(TERM).unwrap();
    let actual = values(TERM);
    assert_eq!(actual["legacy/decoded"], json!({"term": 3, "term_sequence": null}));
    assert_eq!(actual["claim/decoded"], json!({"term": 3, "term_sequence": 0}));
    fixture["envelopes"][0]["expected_term_sequence"] = json!(0);
    assert_eq!(values(&serde_json::to_vec(&fixture).unwrap()), actual);
    fixture["envelopes"][0]["bytes_hex"] = fixture["envelopes"][1]["bytes_hex"].clone();
    assert_eq!(values(&serde_json::to_vec(&fixture).unwrap())["legacy/decoded"]["term_sequence"], json!(0));
}

#[test]
fn invalid_iids_and_missing_observation_events_fail_the_runner() {
    let mut fixture: Value = serde_json::from_slice(TEMPORAL).unwrap();
    fixture["events"][0]["iid"] = json!("01");
    assert!(observe(&serde_json::to_vec(&fixture).unwrap()).is_err());
    let mut fixture: Value = serde_json::from_slice(TEMPORAL).unwrap();
    fixture["observations"][0]["event_index"] = json!(9);
    assert!(observe(&serde_json::to_vec(&fixture).unwrap()).is_err());
}
