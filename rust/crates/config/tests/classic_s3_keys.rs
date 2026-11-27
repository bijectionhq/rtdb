#![cfg(feature = "s3")]

use std::{fs, process::Command};

use rtdb_columnar::MemoryBudget;
use rtdb_config::{InputFormat, digest, qualify_with_classic_s3_inventory};
use serde_json::{Value, json};

const CONFIG: &str = "
diskCache: {path: /unopened/cache}
databases:
  inventory:
    log: !Local {path: /unopened/logs}
    storage: !Remote
      epoch: 2
      objectStore: !S3 {bucket: fixture-bucket, region: us-east-1, prefix: snapshots}
";
const FIXTURE: &str = include_str!("fixtures/classic-s3-alias-collision.json");
const SHARED_ROOT: &str = "snapshots/v06_e02/";
const JVM_KEY: &str = "snapshots/v06_e02/tables/public$t?/data/l00-rc-b00.arrow";
const LEGACY_KEY: &str = "snapshots/v06_e02/tables/public$t%3F/data/l00-rc-b00.arrow";
const DOUBLE_ENCODED_KEY: &str = "snapshots/v06_e02/tables/public$t%253F/data/l00-rc-b00.arrow";

fn fixture() -> Value {
    serde_json::from_str(FIXTURE).unwrap()
}

fn provenance() -> String {
    json!({
        "reference_revision": "0b9fa440f23d2165250d215209f7fa9bd7c26a6f",
        "implementation_revision": "a".repeat(40),
        "server_artifact_sha256": "b".repeat(64),
        "migrator_artifact_sha256": "c".repeat(64),
    })
    .to_string()
}

fn report(config: &str, environment: &[u8], evidence: &Value) -> Value {
    let report = qualify_with_classic_s3_inventory(
        config,
        InputFormat::IngestYaml,
        environment,
        provenance().as_bytes(),
        evidence.to_string().as_bytes(),
        &MemoryBudget::new(32 * 1024 * 1024),
    )
    .unwrap();
    serde_json::to_value(report).unwrap()
}

fn preflight(evidence: &Value) -> Value {
    report(CONFIG, b"{}", evidence)["classic_s3_key_preflight"].clone()
}

fn assert_refused(preflight: &Value, code: &str) {
    assert_eq!(preflight["status"], "refused");
    assert!(preflight.get("layout").is_none());
    assert!(
        preflight["findings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|finding| finding["code"] == code),
        "{preflight}"
    );
}

fn version(key: &str, version_id: &str, kind: &str, is_latest: bool) -> Value {
    json!({"key": key, "version_id": version_id, "kind": kind, "is_latest": is_latest})
}

#[test]
fn literal_percent_path_and_encoded_question_mark_collide_at_the_same_exact_key() {
    let input = fixture();
    let preflight = preflight(&input);
    assert_refused(&preflight, "logical-key-alias-collision");
    assert_eq!(preflight["evidence"]["objects"][0]["jvm_key"], JVM_KEY);
    assert_eq!(
        preflight["evidence"]["objects"][0]["legacy_rtdb_key"],
        LEGACY_KEY
    );
    assert_eq!(preflight["evidence"]["objects"][1]["jvm_key"], LEGACY_KEY);
    assert_eq!(
        preflight["evidence"]["objects"][1]["legacy_rtdb_key"],
        DOUBLE_ENCODED_KEY
    );
    assert_eq!(preflight["evidence"]["namespace"], input["namespace"]);
    assert_eq!(preflight["evidence"]["versions"], input["versions"]);
}

#[test]
fn classic_literal_s3_configuration_retains_the_preflight_scope() {
    use rtdb_format::config::{S3_JVM_LITERAL_STORE, S3_STORE, decode_object_store, parse_yaml};

    let yaml = "!S3 {bucket: fixture-bucket, region: us-east-1, prefix: snapshots}";
    let configured = yaml.replace("prefix: snapshots", "prefix: snapshots, keyLayout: jvm_literal");
    let legacy = decode_object_store(parse_yaml(yaml).unwrap()).unwrap();
    let literal = decode_object_store(parse_yaml(&configured).unwrap()).unwrap();
    assert!(legacy.type_url.ends_with(S3_STORE));
    assert!(literal.type_url.ends_with(S3_JVM_LITERAL_STORE));
    assert!(decode_object_store(parse_yaml("!S3 {bucket: fixture-bucket, keyLayout: jvm_literal}").unwrap()).is_err());
    assert!(decode_object_store(parse_yaml("!S3 {bucket: fixture-bucket, keyLayout: auto}").unwrap()).is_err());

    let mut input = fixture();
    input["logical_objects"] = json!([input["logical_objects"][1].clone()]);
    let configured = CONFIG.replace(yaml, &configured);
    let checked = report(&configured, b"{}", &input);
    assert_eq!(checked["classic_s3_key_preflight"]["layout"], "jvm_literal");
    assert_eq!(checked["databases"][0]["storage"]["provider"]["key_layout"], "jvm_literal");
}

#[test]
fn the_same_exact_version_is_attributed_by_recorded_logical_key() {
    for (logical_index, expected) in [(0, "legacy_rtdb"), (1, "jvm_literal")] {
        let mut input = fixture();
        input["logical_objects"] = json!([input["logical_objects"][logical_index].clone()]);
        let report = report(CONFIG, b"{}", &input);
        let preflight = &report["classic_s3_key_preflight"];
        assert_eq!(preflight["status"], "eligible");
        assert_eq!(preflight["layout"], expected);
        assert_eq!(preflight["evidence"]["versions"], input["versions"]);
        assert_eq!(report["cutover_eligible"], false);
    }
}

#[test]
fn ordinary_paths_share_one_version_namespace() {
    let key = format!("{SHARED_ROOT}tables/public$docs/data/l00-rc-b00.arrow");
    let mut input = fixture();
    input["logical_objects"] = json!([{"key": "tables/public$docs/data/l00-rc-b00.arrow", "requires_current_version": true}]);
    input["versions"] = json!([version(&key, "same", "object", true)]);
    let preflight = preflight(&input);
    assert_eq!(preflight["status"], "eligible");
    assert_eq!(preflight["layout"], "shared");
    assert_eq!(preflight["evidence"]["objects"][0]["jvm_key"], key);
    assert_eq!(preflight["evidence"]["objects"][0]["legacy_rtdb_key"], key);
    assert_eq!(
        preflight["evidence"]["listed_prefixes"],
        json!([SHARED_ROOT])
    );
}

#[test]
fn delete_markers_and_exact_version_ids_remain_scoped_to_each_provider_key() {
    let mut input = fixture();
    input["logical_objects"].as_array_mut().unwrap().truncate(1);
    input["versions"].as_array_mut().unwrap().push(version(
        JVM_KEY,
        "current+/=",
        "delete_marker",
        true,
    ));
    let ambiguous = preflight(&input);
    assert_refused(&ambiguous, "both-key-aliases-occupied");
    assert!(
        ambiguous["findings"]
            .as_array()
            .unwrap()
            .iter()
            .all(|finding| finding["code"] != "inconsistent-version-history")
    );
    assert!(
        ambiguous["evidence"]["versions"]
            .as_array()
            .unwrap()
            .contains(&version(JVM_KEY, "current+/=", "delete_marker", true))
    );

    input["versions"] = json!([
        version(JVM_KEY, "delete", "delete_marker", true),
        version(JVM_KEY, "historical", "object", false)
    ]);
    assert_refused(&preflight(&input), "required-key-deleted");
    input["logical_objects"][0]["requires_current_version"] = json!(false);
    let retired = preflight(&input);
    assert_eq!(retired["layout"], "jvm_literal");
    assert_eq!(retired["evidence"]["versions"], input["versions"]);
}

#[test]
fn a_mixed_namespace_cannot_select_one_layout() {
    let mut input = fixture();
    input["logical_objects"][1]["key"] = json!("tables/public$u~/data/l00-rc-b00.arrow");
    input["versions"].as_array_mut().unwrap().push(version(
        "snapshots/v06_e02/tables/public$u~/data/l00-rc-b00.arrow",
        "literal",
        "object",
        true,
    ));
    assert_refused(&preflight(&input), "mixed-key-layout");
}

#[test]
fn missing_inconsistent_or_unaccounted_evidence_refuses_layout_selection() {
    let mut input = fixture();
    input["logical_objects"].as_array_mut().unwrap().truncate(1);
    for (versions, code) in [
        (json!([]), "missing-provider-key"),
        (
            json!([version(LEGACY_KEY, "null", "object", true)]),
            "missing-exact-version",
        ),
        (
            json!([
                version(LEGACY_KEY, "v", "object", true),
                version(LEGACY_KEY, "v", "delete_marker", false)
            ]),
            "inconsistent-version-history",
        ),
        (
            json!([version(LEGACY_KEY, "v", "object", false)]),
            "inconsistent-version-history",
        ),
        (
            json!([
                version(LEGACY_KEY, "v", "object", true),
                version(LEGACY_KEY, "w", "delete_marker", true)
            ]),
            "inconsistent-version-history",
        ),
        (
            json!([version(
                &format!("{SHARED_ROOT}unknown"),
                "v",
                "object",
                true
            )]),
            "unaccounted-provider-key",
        ),
        (
            json!([version("snapshots/v06_e020/neighbor", "v", "object", true)]),
            "provider-key-out-of-scope",
        ),
    ] {
        input["versions"] = versions;
        assert_refused(&preflight(&input), code);
    }
    assert_eq!(preflight(&input)["evidence"]["versions"], json!([]));
}

#[test]
fn the_capture_must_cover_both_raw_prefixes_and_match_the_declared_provider() {
    let config = CONFIG.replace(
        "prefix: snapshots",
        "prefix: 'snapshots/é~', endpoint: 'https://objects.test'",
    );
    let literal_root = "snapshots/é~/v06_e02/";
    let legacy_root = "snapshots/%C3%A9%7E/v06_e02/";
    let key = format!("{legacy_root}tables/public$t%3F/data/l00-rc-b00.arrow");
    let assert_report = |input: &Value| report(&config, b"{}", input)["classic_s3_key_preflight"].clone();
    let mut input = fixture();
    input["logical_objects"].as_array_mut().unwrap().truncate(1);
    input["versions"] = json!([version(&key, "old+/=", "object", true)]);
    input["namespace"]["endpoint"] = json!("https://fixture-bucket.objects.test/");
    input["listed_prefixes"] = json!([legacy_root]);
    assert_refused(&assert_report(&input), "s3-listing-scope-mismatch");
    input["listed_prefixes"] = json!([literal_root, legacy_root]);
    let result = assert_report(&input);
    assert_eq!(result["layout"], "legacy_rtdb");
    assert_eq!(
        result["evidence"]["listed_prefixes"],
        json!([legacy_root, literal_root])
    );
    assert_eq!(result["evidence"]["versions"], input["versions"]);

    input["complete"] = json!(false);
    assert_refused(&assert_report(&input), "incomplete-inventory");
    input["complete"] = json!(true);
    input["namespace"]["endpoint"] = json!("https://objects.test/");
    let mismatch = assert_report(&input);
    assert_refused(&mismatch, "s3-namespace-mismatch");
    assert!(mismatch.get("evidence").is_none());
}

#[test]
fn provider_scope_uses_an_explicit_classic_configuration() {
    let input = fixture();
    let implicit = CONFIG.replace("region: us-east-1, ", "");
    assert_refused(
        &report(&implicit, b"{}", &input)["classic_s3_key_preflight"],
        "unresolved-s3-scope",
    );
    let overridden = report(
        CONFIG,
        br#"{"AWS_ENDPOINT_URL_S3":"https://SECRET.test"}"#,
        &input,
    );
    assert_refused(
        &overridden["classic_s3_key_preflight"],
        "unqualified-s3-configuration",
    );
    assert!(!overridden.to_string().contains("SECRET"));
}

#[test]
fn a_second_declared_database_cannot_claim_one_of_the_same_prefixes() {
    let config = format!(
        "{CONFIG}  other:\n    storage: !Remote\n      epoch: 2\n      objectStore: !S3 {{bucket: fixture-bucket, region: us-east-1, prefix: snapshots}}\n"
    );
    let result = report(&config, b"{}", &fixture());
    assert_refused(
        &result["classic_s3_key_preflight"],
        "overlapping-database-prefixes",
    );
}

#[test]
fn optional_cli_flag_binds_the_inventory_without_changing_cutover_result() {
    let files = tempfile::tempdir().unwrap();
    for (name, text) in [
        ("config.yaml", CONFIG),
        ("environment.json", "{}"),
        ("provenance.json", &provenance()),
        ("inventory.json", FIXTURE),
    ] {
        fs::write(files.path().join(name), text).unwrap();
    }

    let run = |option: &[&str]| {
        let output = Command::new(env!("CARGO_BIN_EXE_rtdb-config"))
            .current_dir(files.path())
            .args([
                "ingest-yaml",
                "config.yaml",
                "environment.json",
                "provenance.json",
            ])
            .args(option)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        serde_json::from_slice::<Value>(&output.stdout).unwrap()
    };
    let before = run(&[]);
    let mut during = run(&["--classic-s3-key-inventory", "inventory.json"]);
    let preflight = during
        .as_object_mut()
        .unwrap()
        .remove("classic_s3_key_preflight")
        .unwrap();
    assert_refused(&preflight, "logical-key-alias-collision");
    assert_eq!(preflight["inventory_sha256"], digest(FIXTURE.as_bytes()));
    during
        .as_object_mut()
        .unwrap()
        .remove("environment_lookups");
    let mut before = before;
    before
        .as_object_mut()
        .unwrap()
        .remove("environment_lookups");
    assert_eq!(before, during);
}
