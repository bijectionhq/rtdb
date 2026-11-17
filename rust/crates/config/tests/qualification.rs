use rtdb_columnar::MemoryBudget;
use rtdb_config::{InputFormat, QualificationReport, digest, qualify};
use serde_json::json;

fn provenance() -> Vec<u8> {
    serde_json::to_vec(&json!({"reference_revision":"a".repeat(40),"implementation_revision":"b".repeat(40),
        "server_artifact_sha256":"c".repeat(64),"migrator_artifact_sha256":"d".repeat(64)})).unwrap()
}
fn assess(config: &str, format: InputFormat, environment: &[u8]) -> QualificationReport {
    qualify(config, format, environment, &provenance(), &MemoryBudget::new(128 * 1024 * 1024)).unwrap()
}
fn local() -> &'static str {
    "databases:\n  inventory:\n    log: !Local {path: /unopened/logs, epoch: 2}\n    storage: !Local {path: /unopened/objects, epoch: 3}\n"
}

#[test]
fn fixed_inventory_resolves_native_log_and_storage_identities_without_cutover_approval() {
    let report = assess(local(), InputFormat::IngestYaml, b"{}");
    assert!(report.configuration_compatible);
    assert!(!report.cutover_eligible);
    assert!(report.configuration_gaps.is_empty());
    let database = &report.databases[0];
    assert_eq!(database.name, "inventory");
    assert_eq!(database.log["source_path"], "/unopened/logs/LOG");
    assert_eq!(database.log["replica_path"], "/unopened/logs/REPLICA_LOG");
    let root = rtdb_format::keys::storage_root(rtdb_format::keys::CURRENT_STORAGE_VERSION, rtdb_types::StorageEpoch(3), 0, 1).unwrap();
    assert_eq!(database.storage["root"], format!("/unopened/objects/{root}"));
    assert!(report.cutover_requirements.iter().any(|value| value.contains("acknowledged Rust writes")));
}

#[test]
fn ordinary_node_configuration_requires_an_explicit_attached_database_inventory() {
    let report = assess("log: !Local {path: /logs}\nstorage: !Local {path: /objects}\n", InputFormat::Yaml, b"{}");
    assert!(report.configuration_gaps.iter().any(|finding| finding.code == "attached-databases-uninventoried"));
    assert!(!report.configuration_compatible);
}

#[test]
fn reports_all_unknown_fields_and_object_hooks_without_serializing_values() {
    let report = assess("server: {customHook: SECRET_A}\nmodules: [!Jvm {factory: SECRET_B}]\nunknownRoot: SECRET_C\n", InputFormat::Yaml, b"{}");
    for path in ["$.server.customHook", "$.modules[0]", "$.unknownRoot"] {
        assert!(report.configuration_gaps.iter().any(|finding| finding.path == path), "{path}");
    }
    let encoded = serde_json::to_string(&report).unwrap();
    for secret in ["SECRET_A", "SECRET_B", "SECRET_C"] { assert!(!encoded.contains(secret)); }
    assert!(!report.configuration_compatible);
}

#[test]
fn credentials_and_url_userinfo_queries_are_redacted_in_success_and_error_reports() {
    let config = "authn: !OpenIdConnect {issuerUrl: 'https://user:URL_SECRET@issuer.test/openid?token=QUERY_SECRET', clientId: client, clientSecret: CLIENT_SECRET}\nremotes:\n  upstream: !Postgres {hostname: pg.test, database: db, username: reader, password: PASSWORD_SECRET}\n  cloud: !S3 {accessKey: ACCESS_SECRET, secretKey: KEY_SECRET}\nmodules: [!AzureMonitor {connectionString: CONNECTION_SECRET}]\n";
    let report = assess(config, InputFormat::Yaml, b"{}");
    let encoded = serde_json::to_string(&report).unwrap();
    for secret in ["URL_SECRET", "QUERY_SECRET", "CLIENT_SECRET", "PASSWORD_SECRET", "ACCESS_SECRET", "KEY_SECRET", "CONNECTION_SECRET"] { assert!(!encoded.contains(secret), "{secret}"); }
    assert_eq!(report.authentication.as_ref().unwrap()["issuer"], "https://issuer.test/openid");
    let report = assess("authn: !SingleRootUser {password: [MALFORMED_SECRET]}\n", InputFormat::Yaml, b"{}");
    assert!(!serde_json::to_string(&report).unwrap().contains("MALFORMED_SECRET"));
}

#[test]
fn explicit_environment_and_raw_provenance_inputs_have_independent_digests() {
    let config = "log: !Local {path: !Env LOG_ROOT}\nauthn: !SingleRootUser {password: !Env PASSWORD}\n";
    let env = br#"{"LOG_ROOT":"/declared/log","PASSWORD":"ENV_SECRET"}"#;
    let report = assess(config, InputFormat::Yaml, env);
    assert_eq!(report.configuration_sha256, digest(config.as_bytes()));
    assert_eq!(report.environment_input_sha256, digest(env));
    assert_eq!(report.provenance_input_sha256, digest(&provenance()));
    assert!(report.environment_lookups.contains("LOG_ROOT"));
    assert!(!serde_json::to_string(&report).unwrap().contains("ENV_SECRET"));
    let missing = assess(config, InputFormat::Yaml, b"{}");
    assert!(missing.configuration_gaps.iter().any(|finding| finding.code == "production-decoder-rejected"));
}

#[test]
fn duplicate_environment_keys_and_invalid_provenance_fail_closed() {
    let duplicate = assess(local(), InputFormat::IngestYaml, br#"{"A":"one","A":"two"}"#);
    assert!(!duplicate.configuration_compatible);
    assert!(duplicate.configuration_gaps.iter().any(|finding| finding.code == "invalid-environment"));
    let report = qualify(local(), InputFormat::IngestYaml, b"{}", b"{}", &MemoryBudget::new(64 * 1024 * 1024)).unwrap();
    assert!(!report.configuration_compatible);
    assert!(report.configuration_gaps.iter().any(|finding| finding.code == "invalid-provenance"));
}

#[test]
fn edn_projection_never_silently_qualifies_nested_jvm_options() {
    let report = assess("{:server {:port 0 :custom-jvm-hook true}}", InputFormat::Edn, b"{}");
    assert!(report.configuration_gaps.iter().any(|finding| finding.code == "edn-projection-unqualified"));
    assert!(!report.configuration_compatible);
}

#[test]
fn database_partition_epoch_and_remote_reference_gaps_remain_visible() {
    let report = assess("databases:\n  broken:\n    partitions: 2\n    log: !Kafka {cluster: missing, topic: same, replicaTopic: same, epoch: 65536}\n    storage: !Remote {epoch: -1, objectStore: !S3 {bucket: b, prefix: ../escape, remote: missing}}\n", InputFormat::IngestYaml, b"{}");
    for code in ["unsupported-partition-count", "missing-or-wrong-remote", "aliased-log-identities", "invalid-log-epoch", "invalid-storage-epoch", "missing-disk-cache"] {
        assert!(report.configuration_gaps.iter().any(|finding| finding.code == code), "{code}");
    }
    if !cfg!(feature = "s3") { assert!(report.configuration_gaps.iter().any(|finding| finding.code == "feature-not-compiled")); }
}

#[test]
fn relative_paths_and_properties_files_are_not_resolved_by_touching_the_host() {
    let report = assess("log: !Local {path: relative}\nremotes: {k: !Kafka {bootstrapServers: broker:9092, propertiesFile: /missing/secrets.properties}}\n", InputFormat::Yaml, b"{}");
    for code in ["unresolved-local-path", "uninspected-properties-file"] { assert!(report.configuration_gaps.iter().any(|finding| finding.code == code)); }
}

#[test]
fn report_owns_its_charge_and_budget_exhaustion_returns_no_partial_success() {
    let memory = MemoryBudget::new(64 * 1024 * 1024);
    let report = qualify(local(), InputFormat::IngestYaml, b"{}", &provenance(), &memory).unwrap();
    assert!(memory.used_bytes() > local().len());
    drop(report);
    assert_eq!(memory.used_bytes(), 0);
    let memory = MemoryBudget::new(1024);
    assert!(qualify(local(), InputFormat::IngestYaml, b"{}", &provenance(), &memory).is_err());
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn qualification_does_not_create_any_configured_local_roots() {
    let directory = tempfile::tempdir().unwrap();
    let logs = directory.path().join("absent-logs");
    let objects = directory.path().join("absent-objects");
    let config = format!("databases:\n  db:\n    log: !Local {{path: '{}'}}\n    storage: !Local {{path: '{}'}}\n", logs.display(), objects.display());
    let report = assess(&config, InputFormat::IngestYaml, b"{}");
    assert!(report.configuration_compatible);
    assert!(!logs.exists()); assert!(!objects.exists());
}
