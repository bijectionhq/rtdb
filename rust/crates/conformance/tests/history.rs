use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn historical_prices_keep_their_user_prefix_across_layouts_and_pruning_modes() {
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../conformance/fixtures/historical-prices.json");
    let expected: Value = serde_json::from_slice(&std::fs::read(&fixture).unwrap()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let execution = rtdb_conformance::history::execute(&fixture, &directory.path().join("history")).await.unwrap();
    assert_eq!(execution["execution_complete"], true, "{execution:#}");
    assert_eq!(execution["complete_parity"], false);
    let mut identities = BTreeSet::new();
    let mut by_query = BTreeMap::new();
    for layout in execution["layouts"].as_array().unwrap() {
        assert!(!layout["artifacts"].as_array().unwrap().is_empty());
        for observation in layout["observations"].as_array().unwrap() {
            assert!(identities.insert(observation["id"].as_str().unwrap().to_owned()));
            assert_eq!(observation["executed"], true);
            let query = expected["queries"].as_array().unwrap().iter().find(|query|query["id"]==observation["query"]).unwrap();
            let product = observation["product"].as_str().unwrap();
            let price = &query["expected_prices"][product];
            let rows = observation["rows"].as_array().unwrap();
            if price.is_null() { assert!(rows.is_empty(), "{}",observation["id"]); }
            else {
                assert_eq!(rows.len(), 1, "{}",observation["id"]);
                assert_eq!(rows[0][0], json!({"kind":"i64","value":price}), "{}",observation["id"]);
                assert_eq!(rows[0].as_array().unwrap().len(), 5);
                for (index, bound) in rows[0].as_array().unwrap().iter().enumerate().skip(1) {
                    if matches!(index, 2 | 4) && *bound == json!({"kind":"null"}) { continue; }
                    assert_eq!(bound["kind"], "timestamp"); assert_eq!(bound["unit"], "microsecond");
                }
            }
            let key = (observation["query"].as_str().unwrap().to_owned(),product.to_owned());
            let value = json!({"schema":observation["schema"],"rows":observation["rows"]});
            if let Some(previous) = by_query.insert(key, value.clone()) { assert_eq!(value, previous, "{}",observation["id"]); }
        }
    }
    assert_eq!(identities.len(),112);
    assert_eq!(by_query.len(),14);
}
