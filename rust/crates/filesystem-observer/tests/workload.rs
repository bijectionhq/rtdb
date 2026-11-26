use rtdb_filesystem_observer::validate_workload;
use serde_json::json;

fn proof(kind:&str,sql:&str)->Result<serde_json::Value,Box<dyn std::error::Error+Send+Sync>> {
    validate_workload(&serde_json::to_vec(&json!({"schema_version":1,"statements":[{"location":"source/0","kind":kind,"sql":sql,"psycopg_parameters":false}]})).unwrap())
}

#[test]
fn production_parser_separates_literal_management_words_from_actual_management() {
    for sql in ["SELECT 'ATTACH DATABASE escaped'", "SELECT 1 AS \"ATTACH\"", "/* ATTACH DATABASE escaped */ SELECT 1"] {
        assert!(proof("read",sql).is_ok(),"{sql}");
    }
    for sql in ["ATTACH DATABASE escaped", "DETACH DATABASE xtdb", "PREPARE s AS SELECT 1", "EXECUTE s", "GRANT root TO bob", "SELECT 1; ATTACH DATABASE escaped", "EXPLAIN GRANT root TO bob", "ALTER DATABASE xtdb SET storage = 'outside'"] {
        assert!(proof("transaction_statement",sql).is_err(),"{sql}");
    }
}

#[test]
fn begin_and_fixed_generated_controls_have_explicit_policy_roles() {
    assert!(proof("begin","BEGIN READ WRITE WITH (METADATA = 1)").is_ok());
    assert!(proof("begin","BEGIN; ATTACH DATABASE escaped").is_err());
    assert!(proof("read","SET AWAIT_TOKEN = 'value'").is_err());
    assert!(proof("generated","SET AWAIT_TOKEN = 'value''; ATTACH DATABASE escaped'").is_ok());
    assert!(proof("generated","SHOW LATEST_COMPLETED_TXS").is_ok());
    assert!(proof("mutation","INSERT INTO prices (_id, price) VALUES (1, 112)").is_ok());
    assert!(proof("read","INSERT INTO prices (_id) VALUES (1)").is_err());
}

#[test]
fn duplicate_source_identity_and_oversized_sql_fail_before_qualification() {
    let row=json!({"location":"same","kind":"read","sql":"SELECT 1","psycopg_parameters":false});
    assert!(validate_workload(&serde_json::to_vec(&json!({"schema_version":1,"statements":[row.clone(),row]})).unwrap()).is_err());
    assert!(proof("read",&format!("SELECT '{}'","x".repeat(65536))).is_err());
}

#[test]
fn positional_psycopg_bulk_sources_are_classified_as_their_actual_wire_sql() {
    let request=json!({"schema_version":1,"statements":[{"location":"bulk","kind":"mutation","sql":"INSERT INTO t (_id) VALUES (%s)","psycopg_parameters":true}]});
    let proof=validate_workload(&serde_json::to_vec(&request).unwrap()).unwrap();
    assert_ne!(proof["statements"][0]["sql_sha256"],proof["statements"][0]["wire_sql_sha256"]);
}
