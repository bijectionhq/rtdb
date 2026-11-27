use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{fs, path::Path};

fn plan(root: &Path, records: Vec<Value>, direct: bool, query_limit: Option<u64>) -> std::path::PathBuf {
    plan_at(root, "c84f9334eb060ceb92d28ecdd4a06e6b625932fd", records, direct, query_limit)
}
fn plan_at(root: &Path, reference: &str, records: Vec<Value>, direct: bool, query_limit: Option<u64>) -> std::path::PathBuf {
    let options = json!({"direct_sql":direct,"decorrelate":true,"query_limit":query_limit,"max_rows":100,"max_result_bytes":1048576});
    let mut source = String::new();
    let mut assertion = 0;
    let count = records.len();
    for (index, record) in records.into_iter().enumerate() {
        let is_assertion = matches!(record["type"].as_str(), Some("statement" | "query"));
        if is_assertion { assertion += 1; }
        let assertion_ordinal = if is_assertion { json!(assertion) } else { Value::Null };
        let identity = json!({"reference":reference,"source":"fixture.test","source_sha256":"a".repeat(64),
            "selection":"fixture","options":options,"ordinal":index + 1,"assertion_ordinal":assertion_ordinal,"line":index * 4 + 1});
        source.push_str(&serde_json::to_string(&json!({"identity":identity,"ordinal":index + 1,"assertion_ordinal":assertion_ordinal,
            "line":index * 4 + 1,"record":record,"parse_error":null})).unwrap());
        source.push('\n');
    }
    fs::write(root.join("records.jsonl"), &source).unwrap();
    let plan = json!({"schema_version":1,"reference":reference,"selection":"fixture","scripts":[{
        "source":"fixture.test","source_sha256":"a".repeat(64),"records":count,"assertions":assertion,"parse_errors":0,
        "records_file":"records.jsonl","records_sha256":format!("{:x}",Sha256::digest(source.as_bytes())),"selection":"fixture","options":options}]});
    let path = root.join("plan.json"); fs::write(&path, serde_json::to_vec(&plan).unwrap()).unwrap(); path
}
fn receipts(root: &Path) -> Vec<Value> {
    fs::read_to_string(root.join("script-00000.jsonl")).unwrap().lines().map(|line| serde_json::from_str(line).unwrap()).collect()
}
fn statement(sql: &str) -> Value { json!({"type":"statement","mode":"ok","sql":sql}) }
fn query(sql: &str, expected: &str) -> Value { json!({"type":"query","sql":sql,"type_string":"I","sort":"nosort","expected":{"count":1,"text":expected}}) }

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn synthetic_schema_inserts_real_documents_and_hides_id_from_query_binding() {
    let directory = tempfile::tempdir().unwrap();
    let plan = plan(directory.path(), vec![statement("CREATE TABLE t1(a INTEGER, b INTEGER)"), statement("INSERT INTO t1(b,a) VALUES(101,104)"), query("SELECT a FROM t1", "104\n")], false, None);
    let output = directory.path().join("output");
    let execution = rtdb_conformance::sql_logic::execute(&plan, &output).await.unwrap();
    assert_eq!(execution["execution_complete"], true, "{execution:#}");
    assert_eq!(execution["source_dirty"], env!("RTDB_CONFORMANCE_DIRTY") != "false");
    assert!(execution.get("artifact_dirty").is_none());
    let rows = receipts(&output);
    assert_eq!(rows[0]["effect"], "synthetic_create_table");
    assert_eq!(rows[1]["effect"], "synthetic_insert_documents");
    assert_eq!(rows[2]["disposition"], "query_ok", "{}", rows[2]);
    assert_eq!(rows[2]["result"]["text"], "104\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skipped_queries_consume_limit_and_keep_following_statement_receipt() {
    let directory = tempfile::tempdir().unwrap();
    let mut skipped = query("SELECT 1", "1\n"); skipped["skipif"] = json!(["xtdb"]);
    let plan = plan(directory.path(), vec![skipped, statement("INSERT INTO t(_id) VALUES(1)")], true, Some(1));
    let output = directory.path().join("output");
    rtdb_conformance::sql_logic::execute(&plan, &output).await.unwrap();
    let rows = receipts(&output);
    assert_eq!(rows[0]["disposition"], "engine_condition");
    assert_eq!(rows[0]["query_count_after"], 1);
    assert_eq!(rows[1]["disposition"], "query_limit");
    assert_eq!(rows[1]["engine_executed"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pinned_halt_error_does_not_remove_subsequent_query() {
    let directory = tempfile::tempdir().unwrap();
    let plan = plan(directory.path(), vec![json!({"type":"halt"}), query("SELECT 1", "1\n")], true, None);
    let output = directory.path().join("output");
    rtdb_conformance::sql_logic::execute(&plan, &output).await.unwrap();
    let rows = receipts(&output);
    assert_eq!(rows[0]["disposition"], "halt_error");
    assert_eq!(rows[1]["disposition"], "query_ok", "{}", rows[1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_record_identity_tampering_prevents_completed_script_evidence() {
    let directory = tempfile::tempdir().unwrap();
    let plan_path = plan(directory.path(), vec![query("SELECT 1", "1\n")], true, None);
    let mut plan: Value = serde_json::from_slice(&fs::read(&plan_path).unwrap()).unwrap();
    let path = directory.path().join("records.jsonl");
    let mut record: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    record["identity"]["line"] = 9.into();
    let bytes = serde_json::to_string(&record).unwrap() + "\n";
    fs::write(&path, &bytes).unwrap();
    plan["scripts"][0]["records_sha256"] = format!("{:x}",Sha256::digest(bytes.as_bytes())).into();
    fs::write(&plan_path, serde_json::to_vec(&plan).unwrap()).unwrap();
    let output = directory.path().join("output");
    let execution = rtdb_conformance::sql_logic::execute(&plan_path, &output).await.unwrap();
    assert_eq!(execution["execution_complete"], false);
    assert_eq!(execution["scripts"][0]["execution_complete"], false);
}

#[tokio::test]
async fn unavailable_script_still_verifies_original_assertion_denominator() {
    let directory = tempfile::tempdir().unwrap();
    let plan_path = plan(directory.path(), vec![query("SELECT 1", "1\n")], true, None);
    let mut plan: Value = serde_json::from_slice(&fs::read(&plan_path).unwrap()).unwrap();
    plan["scripts"][0]["parse_errors"] = 1.into();
    plan["scripts"][0]["assertions"] = 2.into();
    fs::write(&plan_path, serde_json::to_vec(&plan).unwrap()).unwrap();
    let execution = rtdb_conformance::sql_logic::execute(&plan_path, &directory.path().join("output")).await.unwrap();
    assert_eq!(execution["execution_complete"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upstream_pinned_commit_is_accepted_and_unpinned_reference_is_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let accepted_dir = directory.path().join("accepted-plan");
    let rejected_dir = directory.path().join("rejected-plan");
    fs::create_dir(&accepted_dir).unwrap();
    fs::create_dir(&rejected_dir).unwrap();
    let accepted = plan_at(&accepted_dir, "0b9fa440f23d2165250d215209f7fa9bd7c26a6f", vec![query("SELECT 1", "1\n")], true, None);
    let execution = rtdb_conformance::sql_logic::execute(&accepted, &directory.path().join("accepted-out")).await.unwrap();
    assert_eq!(execution["execution_complete"], true, "{execution:#}");
    let rejected = plan_at(&rejected_dir, "0000000000000000000000000000000000000000", vec![query("SELECT 1", "1\n")], true, None);
    assert!(rtdb_conformance::sql_logic::execute(&rejected, &directory.path().join("rejected-out")).await.is_err());
}
