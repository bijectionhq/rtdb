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

const PROTOBUF_SOURCE: &str = r#"
remotes:
  events: !Kafka
    bootstrapServers: broker.invalid:9092
    schemaRegistryUrl: https://remote-only.invalid
databases:
  inventory:
    log: !Kafka {cluster: events, topic: transactions}
    storage: !Local {path: /unopened/objects}
    externalSource: !KafkaConnect
      remote: events
      topic: events
      indexer: !Docs {table: public.events}
      connectConfig:
        key.converter: io.confluent.connect.protobuf.ProtobufConverter
        key.converter.schema.registry.url: https://user:REGISTRY_URL_SECRET@registry.invalid
        key.converter.basic.auth.credentials.source: URL
        key.converter.key.subject.name.strategy: io.confluent.kafka.serializers.subject.RecordNameStrategy
        key.converter.int.for.enums: 'true'
        value.converter: io.confluent.connect.protobuf.ProtobufConverter
        value.converter.schema.registry.url: 'https://registry.invalid, https://backup-registry.invalid'
        value.converter.schema.registry.basic.auth.credentials.source: USER_INFO
        value.converter.schema.registry.basic.auth.user.info: 'reader:REGISTRY_BASIC_SECRET'
        value.converter.schema.registry.ssl.truststore.location: /unopened/REGISTRY_TRUSTSTORE_PATH
        value.converter.schema.registry.ssl.truststore.password: REGISTRY_TLS_SECRET
        value.converter.flatten.unions: 'true'
        value.converter.use.latest.version: 'true'
"#;

#[test]
fn protobuf_registry_configuration_qualifies_with_compiled_connect_support() {
    let report = assess(PROTOBUF_SOURCE, InputFormat::IngestYaml, b"{}");
    let missing_features: Vec<_> = report
        .configuration_gaps
        .iter()
        .map(|finding| {
            assert_eq!(finding.code, "feature-not-compiled");
            finding.path.as_str()
        })
        .collect();
    let mut expected = Vec::new();
    if !cfg!(feature = "kafka") {
        expected.extend(["remotes.events", "databases.inventory.log"]);
    }
    if !cfg!(feature = "kafka-connect") {
        expected.push("databases.inventory.externalSource");
    }
    assert_eq!(missing_features, expected);
    assert_eq!(
        report.configuration_compatible,
        cfg!(feature = "kafka-connect")
    );
    assert_eq!(
        report.databases[0].external_source.as_ref().unwrap()["kind"],
        "kafka-connect"
    );
    assert!(!report.cutover_eligible);
    let encoded = serde_json::to_string(&report).unwrap();
    for secret in [
        "REGISTRY_URL_SECRET",
        "REGISTRY_BASIC_SECRET",
        "REGISTRY_TLS_SECRET",
        "REGISTRY_TRUSTSTORE_PATH",
    ] {
        assert!(!encoded.contains(secret));
    }
}

#[test]
fn connect_source_requires_a_kafka_remote_and_valid_docs_table() {
    let config = PROTOBUF_SOURCE
        .replace("remote: events", "remote: absent")
        .replace("table: public.events", "table: 'public.'");
    let report = assess(&config, InputFormat::IngestYaml, b"{}");
    for code in ["missing-or-wrong-remote", "invalid-connect-indexer-table"] {
        assert!(report.configuration_gaps.iter().any(|finding| {
            finding.code == code && finding.path == "databases.inventory.externalSource"
        }));
    }
}

#[cfg(feature = "kafka-connect")]
#[test]
fn registry_bearer_provider_class_must_have_a_native_implementation() {
    let config = PROTOBUF_SOURCE
        .replace("value.converter.schema.registry.basic.auth.credentials.source: USER_INFO",
            "value.converter.bearer.auth.credentials.source: CUSTOM")
        .replace("value.converter.schema.registry.basic.auth.user.info: 'reader:REGISTRY_BASIC_SECRET'",
            "value.converter.bearer.auth.token: REGISTRY_TOKEN_SECRET\n        value.converter.bearer.auth.custom.provider.class: io.confluent.kafka.schemaregistry.client.security.bearerauth.StaticTokenCredentialProvider");
    let report = assess(&config, InputFormat::IngestYaml, b"{}");
    assert!(report.configuration_compatible);
    assert!(
        !serde_json::to_string(&report)
            .unwrap()
            .contains("REGISTRY_TOKEN_SECRET")
    );

    let config = config.replace(
        "io.confluent.kafka.schemaregistry.client.security.bearerauth.StaticTokenCredentialProvider",
        "custom.BEARER_PROVIDER_SECRET",
    );
    let report = assess(&config, InputFormat::IngestYaml, b"{}");
    assert!(
        report
            .configuration_gaps
            .iter()
            .any(|finding| finding.code == "invalid-connect-converter-options")
    );
    let encoded = serde_json::to_string(&report).unwrap();
    assert!(!encoded.contains("REGISTRY_TOKEN_SECRET"));
    assert!(!encoded.contains("BEARER_PROVIDER_SECRET"));
}

#[cfg(feature = "kafka-connect")]
#[test]
fn connect_rejects_unsupported_or_invalid_converter_configuration_without_values() {
    let prefix = "databases.inventory.externalSource.connectConfig";
    for (from, to, code, option) in [
        (
            "value.converter: io.confluent.connect.protobuf.ProtobufConverter",
            "value.converter: custom.CONVERTER_CLASS_SECRET",
            "unsupported-connect-converter",
            "value.converter",
        ),
        (
            "        value.converter.schema.registry.url: 'https://registry.invalid, https://backup-registry.invalid'\n",
            "",
            "invalid-connect-converter-options",
            "value.converter",
        ),
        (
            "value.converter.flatten.unions: 'true'",
            "value.converter.specific.protobuf.value.type: custom.GENERATED_CLASS_SECRET",
            "invalid-connect-converter-options",
            "value.converter",
        ),
        (
            "value.converter.schema.registry.basic.auth.credentials.source: USER_INFO",
            "value.converter.schema.registry.basic.auth.credentials.source: JVM_PROVIDER_SECRET",
            "invalid-connect-converter-options",
            "value.converter",
        ),
        (
            "value.converter.flatten.unions: 'true'",
            "value.converter.value.subject.name.strategy: custom.STRATEGY_SECRET",
            "connect-plugin-options-unqualified",
            "value.converter.value.subject.name.strategy",
        ),
        (
            "value.converter.flatten.unions: 'true'",
            "value.converter.unknown.option: OPTION_VALUE_SECRET",
            "connect-plugin-options-unqualified",
            "value.converter.unknown.option",
        ),
    ] {
        let report = assess(
            &PROTOBUF_SOURCE.replace(from, to),
            InputFormat::IngestYaml,
            b"{}",
        );
        assert!(!report.configuration_compatible);
        assert!(report.configuration_gaps.iter().any(|finding| finding.code == code && finding.path == format!("{prefix}.{option}")), "{option}");
        assert!(!serde_json::to_string(&report).unwrap().contains("_SECRET"));
    }
}

#[cfg(feature = "kafka-connect")]
#[test]
fn connect_parser_diagnostics_do_not_expose_values_and_transforms_remain_unqualified() {
    let transform = r#"
        transforms: route
        transforms.route.type: org.apache.kafka.connect.transforms.RegexRouter
        transforms.route.predicate: select
        transforms.route.negate: 'false'
        predicates: select
        predicates.select.type: org.apache.kafka.connect.transforms.predicates.TopicNameMatches
"#;
    for (transform, code) in [
        (transform.to_owned(), "connect-plugin-options-unqualified"),
        (
            transform.replace("'false'", "NEGATE_VALUE_SECRET"),
            "invalid-connect-description",
        ),
    ] {
        let report = assess(
            &format!("{PROTOBUF_SOURCE}{transform}"),
            InputFormat::IngestYaml,
            b"{}",
        );
        assert!(!report.configuration_compatible);
        assert!(
            report
                .configuration_gaps
                .iter()
                .any(|finding| finding.code == code)
        );
        assert!(
            !serde_json::to_string(&report)
                .unwrap()
                .contains("NEGATE_VALUE_SECRET")
        );
    }
}

#[cfg(feature = "kafka")]
const BROKER_OIDC: &str = r#"
logClusters:
  events: !Kafka
    bootstrapServers: broker.invalid:9092
    propertiesMap:
      security.protocol: SASL_SSL
      sasl.mechanism: OAUTHBEARER
      sasl.login.callback.handler.class: org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
      sasl.login.class: org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerRefreshingLogin
      sasl.oauthbearer.jwt.retriever.class: org.apache.kafka.common.security.oauthbearer.ClientCredentialsJwtRetriever
      sasl.oauthbearer.jwt.validator.class: org.apache.kafka.common.security.oauthbearer.ClientJwtValidator
      sasl.oauthbearer.token.endpoint.url: ' https://issuer.invalid/token?tenant=ENDPOINT_SECRET '
      sasl.jaas.config: 'org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required extension_logicalCluster="EXTENSION_SECRET";'
      sasl.oauthbearer.client.credentials.client.id: fixture
      sasl.oauthbearer.client.credentials.client.secret: !Env BROKER_PASSWORD
      sasl.oauthbearer.scope: 'fixtures write'
      sasl.oauthbearer.header.urlencode: 'true'
      sasl.oauthbearer.sub.claim.name: principal
      sasl.oauthbearer.scope.claim.name: permissions
      sasl.login.connect.timeout.ms: '2000'
      sasl.login.read.timeout.ms: '3000'
      sasl.login.retry.backoff.ms: '100'
      sasl.login.retry.backoff.max.ms: '500'
      sasl.login.refresh.window.factor: '0.75'
      sasl.login.refresh.window.jitter: '0.05'
      sasl.login.refresh.min.period.seconds: '60'
      sasl.login.refresh.buffer.seconds: '300'
databases:
  inventory:
    log: !Kafka {cluster: events, topic: transactions}
    storage: !Local {path: /unopened/objects}
"#;

#[cfg(feature = "kafka")]
const ALLOWED_BROKER_ENV: &[u8] = br#"{
    "BROKER_PASSWORD":"BROKER_SECRET",
    "RTDB_KAFKA_OAUTHBEARER_ALLOWED_URLS":"https://other.invalid/token, https://issuer.invalid/token?tenant=ENDPOINT_SECRET "
}"#;

#[cfg(feature = "kafka")]
#[test]
fn broker_oidc_checks_the_supplied_exact_endpoint_and_redacts_credentials() {
    let report = assess(BROKER_OIDC, InputFormat::IngestYaml, ALLOWED_BROKER_ENV);
    assert!(report.configuration_compatible);
    assert!(
        report
            .environment_lookups
            .contains("RTDB_KAFKA_OAUTHBEARER_ALLOWED_URLS")
    );
    let encoded = serde_json::to_string(&report).unwrap();
    for secret in ["BROKER_SECRET", "ENDPOINT_SECRET", "EXTENSION_SECRET"] {
        assert!(!encoded.contains(secret));
    }
    for environment in [
        br#"{"BROKER_PASSWORD":"BROKER_SECRET"}"#.as_slice(),
        br#"{"BROKER_PASSWORD":"BROKER_SECRET","RTDB_KAFKA_OAUTHBEARER_ALLOWED_URLS":"https://issuer.invalid/token"}"#.as_slice(),
    ] {
        let report = assess(BROKER_OIDC, InputFormat::IngestYaml, environment);
        assert!(!report.configuration_compatible);
        assert_eq!(report.configuration_gaps.len(), 1);
        assert_eq!(report.configuration_gaps[0].code, "kafka-oauth-endpoint-not-allowed");
        let encoded = serde_json::to_string(&report).unwrap();
        assert!(!encoded.contains("ENDPOINT_SECRET"));
        assert!(!encoded.contains("BROKER_SECRET"));
    }
}

#[cfg(feature = "kafka")]
#[test]
fn broker_oauth_custom_classes_and_assertions_still_block_qualification() {
    for (from, to, code) in [
        (
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler",
            "custom.CALLBACK_SECRET",
            "unsupported-kafka-auth-properties",
        ),
        (
            "org.apache.kafka.common.security.oauthbearer.ClientCredentialsJwtRetriever",
            "org.apache.kafka.common.security.oauthbearer.FileJwtRetriever",
            "unsupported-kafka-auth-properties",
        ),
        (
            "sasl.oauthbearer.scope: 'fixtures write'",
            "sasl.oauthbearer.assertion.file: ASSERTION_PATH_SECRET",
            "unsupported-kafka-auth-properties",
        ),
        (
            "sasl.oauthbearer.scope: 'fixtures write'",
            "sasl.oauthbearer.jwks.endpoint.url: 'https://issuer.invalid/JWKS_SECRET'",
            "unqualified-kafka-property",
        ),
    ] {
        let report = assess(
            &BROKER_OIDC.replace(from, to),
            InputFormat::IngestYaml,
            ALLOWED_BROKER_ENV,
        );
        assert!(!report.configuration_compatible);
        assert!(
            report
                .configuration_gaps
                .iter()
                .any(|finding| finding.code == code)
        );
        let encoded = serde_json::to_string(&report).unwrap();
        for secret in [
            "CALLBACK_SECRET",
            "ASSERTION_PATH_SECRET",
            "JWKS_SECRET",
            "BROKER_SECRET",
            "ENDPOINT_SECRET",
        ] {
            assert!(!encoded.contains(secret));
        }
    }
}
