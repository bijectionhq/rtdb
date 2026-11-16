use std::fs;
use serde_json::json;
use sha2::{Digest, Sha256};

#[tokio::test]
async fn backend_capture_reads_real_local_bytes_without_creating_logs_and_closes_its_resources() {
    let temporary = tempfile::tempdir().unwrap();
    let namespace = "rtdb-recovery-0123456789abcdef";
    let root = temporary.path().join(namespace);
    fs::create_dir(&root).unwrap();
    let storage = root.join("storage");
    let version = rtdb_format::keys::storage_root(rtdb_format::keys::CURRENT_STORAGE_VERSION, rtdb_types::StorageEpoch(0),0,1).unwrap();
    fs::create_dir_all(storage.join(&version)).unwrap();
    fs::write(storage.join(&version).join("protected.bin"),b"immutable payload").unwrap();
    let configuration = root.join("configuration.yaml");
    let text = format!("log: !Local\n  path: {}\nstorage: !Local\n  path: {}\nserver: null\nflightSql: null\n",root.join("log").display(),storage.display());
    fs::write(&configuration,&text).unwrap();
    let request = root.join("request.json");
    fs::write(&request,serde_json::to_vec(&json!({"schema_version":1,"namespace":namespace,"configuration":configuration,
        "configuration_sha256":format!("{:x}",Sha256::digest(text.as_bytes())),"capability":"local","token_kind":"none",
        "action":{"kind":"capture"},"maximum_bytes":1024*1024,"timeout_seconds":30})).unwrap()).unwrap();
    let output = root.join("capture");
    let receipt = rtdb_conformance::backend::execute(&request,&output).await.unwrap();
    assert_eq!(receipt["execution_complete"],true);
    assert_eq!(receipt["resources_closed"],true);
    assert_eq!(receipt["result"]["source"]["tip"],-1);
    assert_eq!(receipt["result"]["replica"]["tip"],-1);
    assert!(!root.join("log/LOG").exists());
    assert_eq!(fs::read(output.join(receipt["result"]["objects"][0]["file"].as_str().unwrap())).unwrap(),b"immutable payload");
    assert_eq!(fs::read(storage.join(version).join("protected.bin")).unwrap(),b"immutable payload");
}
