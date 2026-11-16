use rtdb_conformance::{controls::digest_file, history};
use rtdb_storage::log::{Log, ReadOnlyLocalLog};
use serde_json::{Value, json};
use std::{fs, path::Path};

fn copy(source: &Path, target: &Path) {
    fs::create_dir(target).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap(); let destination = target.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() { copy(&entry.path(), &destination); }
        else { fs::copy(entry.path(), destination).unwrap(); }
    }
}
fn write(path: &Path, value: &Value) { fs::write(path, serde_json::to_vec(value).unwrap()).unwrap(); }

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn isolated_closed_history_recovery_checks_exact_manifest_before_opening() {
    let original = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../conformance/fixtures/historical-prices.json");
    let mut fixture: Value = serde_json::from_slice(&fs::read(original).unwrap()).unwrap();
    fixture["prefixes"].as_array_mut().unwrap().retain(|prefix| prefix["id"] == "july");
    fixture["queries"].as_array_mut().unwrap().retain(|query| query["prefix"] == "july");
    let temporary = tempfile::tempdir().unwrap(); let root = temporary.path();
    let fixture_path = root.join("fixture.json"); write(&fixture_path, &fixture);
    let execution = history::execute(&fixture_path, &root.join("history")).await.unwrap();
    assert_eq!(execution["execution_complete"], true, "{execution:#}");
    let baseline = execution["layouts"].as_array().unwrap().iter().find(|layout| layout["layout"] == "flushed").unwrap();
    let source = root.join("history/july-flushed"); let isolated = root.join("isolated"); copy(&source, &isolated);
    let source_digest = digest_file(&source.join("log/REPLICA_LOG")).unwrap();
    let source_head = ReadOnlyLocalLog::open(isolated.join("log/LOG"), Default::default()).unwrap().latest().unwrap().0;
    let replica_head = ReadOnlyLocalLog::open(isolated.join("log/REPLICA_LOG"), Default::default()).unwrap().latest().unwrap().0;
    let mut manifest = json!({"schema_version":1,"closed":true,"fixture_sha256":digest_file(&fixture_path).unwrap(),"prefix":"july",
        "storage_version":6,"storage_epoch":0,"partitions":1,"external_source_token":null,"files":baseline["artifacts"],
        "checkpoint":baseline["checkpoint"],"log_heads":{"source":source_head,"replica":replica_head}});
    let manifest_path = root.join("manifest.json"); write(&manifest_path, &manifest);
    let request_path = root.join("request.json");
    let mut request = json!({"schema_version":1,"fixture":fixture_path,"fixture_sha256":digest_file(&fixture_path).unwrap(),
        "directory":isolated,"manifest":manifest_path,"manifest_sha256":digest_file(&manifest_path).unwrap(),"prefix":"july","action":"capture",
        "canary":{"id":"C","price":991,"system_time":"2026-10-01T00:00:00Z","valid_time":"2026-10-01T00:00:00Z"},"retry_compaction":false});
    write(&request_path, &request);
    let captured = history::recovery::execute(&request_path, &root.join("capture")).await.unwrap();
    assert_eq!(captured["execution_complete"], true, "{captured:#}");
    assert_eq!(captured["closure"], "graceful");
    manifest["checkpoint"] = captured["result"]["checkpoint"].clone(); write(&manifest_path, &manifest);
    request["manifest_sha256"] = json!(digest_file(&manifest_path).unwrap()); request["action"] = json!("recover"); write(&request_path, &request);
    let recovered = history::recovery::execute(&request_path, &root.join("recovered")).await.unwrap();
    assert_eq!(recovered["execution_complete"], true, "{recovered:#}");
    assert_eq!(recovered["result"]["checkpoint"]["replica_position"], replica_head);
    for (actual, expected) in recovered["result"]["observations"].as_array().unwrap().iter().zip(captured["result"]["observations"].as_array().unwrap()) {
        assert_eq!(actual["id"], expected["id"]); assert_eq!(actual["schema"], expected["schema"]); assert_eq!(actual["rows"], expected["rows"]);
    }
    request["manifest_sha256"] = json!("0".repeat(64)); write(&request_path, &request);
    let rejected = history::recovery::execute(&request_path, &root.join("rejected")).await.unwrap();
    assert_eq!(rejected["execution_complete"], false);
    assert!(rejected["error"].as_str().unwrap().contains("manifest"));
    assert_eq!(digest_file(&source.join("log/REPLICA_LOG")).unwrap(), source_digest);
}
