use rtdb_conformance::{Outcome, controls::observe};
use serde_json::{Value, json};

const TEMPORAL: &[u8] = include_bytes!("../../../conformance/fixtures/temporal-ceilings.json");
const TERM: &[u8] = include_bytes!("../../../conformance/fixtures/term-sequence.json");
const TABLE: &[u8] = include_bytes!("../../../conformance/fixtures/table-column.json");

async fn values(bytes: &[u8]) -> serde_json::Map<String, Value> {
    observe(bytes).await.unwrap().into_iter().map(|observation| {
        assert!(observation.executed);
        let Outcome::Returned { typed_value } = observation.outcome else { panic!("control did not return") };
        (observation.id, typed_value)
    }).collect()
}

#[tokio::test]
async fn temporal_runner_reads_actual_event_inputs_and_never_expected_observations() {
    let mut fixture: Value = serde_json::from_slice(TEMPORAL).unwrap();
    let expected = fixture["expected_observations"].clone();
    assert_eq!(Value::Object(values(TEMPORAL).await), expected);
    fixture["expected_observations"] = json!({"fabricated": "never executed"});
    assert_eq!(Value::Object(values(&serde_json::to_vec(&fixture).unwrap()).await), expected);
    fixture["events"][0]["system_from"] = json!(3);
    let changed = values(&serde_json::to_vec(&fixture).unwrap()).await;
    assert_eq!(changed["iid-a/earlier/system-time-ceilings"], json!([i64::MAX, 3, i64::MAX]));
    assert_eq!(changed["iid-b/system-time-ceilings"], json!([i64::MAX]));
}

#[tokio::test]
async fn term_runner_round_trips_wire_presence_and_ignores_expected_fields() {
    let mut fixture: Value = serde_json::from_slice(TERM).unwrap();
    let actual = values(TERM).await;
    assert_eq!(actual["legacy/decoded"], json!({"term": 3, "term_sequence": null}));
    assert_eq!(actual["claim/decoded"], json!({"term": 3, "term_sequence": 0}));
    fixture["envelopes"][0]["expected_term_sequence"] = json!(0);
    assert_eq!(values(&serde_json::to_vec(&fixture).unwrap()).await, actual);
    fixture["envelopes"][0]["bytes_hex"] = fixture["envelopes"][1]["bytes_hex"].clone();
    assert_eq!(values(&serde_json::to_vec(&fixture).unwrap()).await["legacy/decoded"]["term_sequence"], json!(0));
}

#[tokio::test]
async fn invalid_iids_and_missing_observation_events_fail_the_runner() {
    let mut fixture: Value = serde_json::from_slice(TEMPORAL).unwrap();
    fixture["events"][0]["iid"] = json!("01");
    assert!(observe(&serde_json::to_vec(&fixture).unwrap()).await.is_err());
    let mut fixture: Value = serde_json::from_slice(TEMPORAL).unwrap();
    fixture["observations"][0]["event_index"] = json!(9);
    assert!(observe(&serde_json::to_vec(&fixture).unwrap()).await.is_err());
}

#[tokio::test]
async fn column_table_runner_observes_production_rows_types_and_names_without_reading_expectations() {
    let mut fixture: Value = serde_json::from_slice(TABLE).unwrap();
    let expected = fixture["cases"].as_array().unwrap().iter().map(|case| {
        (case["id"].as_str().unwrap().to_owned(), json!({(case["column"].as_str().unwrap()): case["expected"]}))
    }).collect::<serde_json::Map<String, Value>>();
    assert_eq!(values(TABLE).await, expected);
    for case in fixture["cases"].as_array_mut().unwrap() { case["expected"] = json!("fabricated"); }
    assert_eq!(values(&serde_json::to_vec(&fixture).unwrap()).await, expected);
    fixture["cases"][0]["input"]["List"][0] = json!({"I64": 9});
    let changed = values(&serde_json::to_vec(&fixture).unwrap()).await;
    assert_eq!(changed["integers"]["x5"]["rows"][0][0], json!({"I64": 9}));
    assert_eq!(expected["integers"]["x5"]["rows"][0][0], json!({"I64": 1}));
    assert_eq!(changed["mixed-numbers"], expected["mixed-numbers"]);
}

#[tokio::test]
async fn column_table_fixture_profile_and_observation_id_validation_reject_unobserved_cases() {
    let original: Value = serde_json::from_slice(TABLE).unwrap();
    for input in [json!({"I32": 1}), json!({"List": [{"List": []}]}), json!({"List": [{"Utf8": "not in profile"}]})] {
        let mut fixture = original.clone();
        fixture["cases"][0]["input"] = input;
        assert!(observe(&serde_json::to_vec(&fixture).unwrap()).await.is_err());
    }
    for id in [json!(""), original["cases"][1]["id"].clone()] {
        let mut fixture = original.clone();
        fixture["cases"][0]["id"] = id;
        assert!(observe(&serde_json::to_vec(&fixture).unwrap()).await.is_err());
    }
}
