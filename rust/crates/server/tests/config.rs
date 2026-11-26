use std::time::Duration;
use rtdb_auth::{AuthConfig, Method};
use rtdb_server::config::{KeyVaultSettings, NodeConfiguration, RemoteConfig, host_timezone_from, parse_duration};

fn decode(yaml: &str) -> rtdb_types::Result<NodeConfiguration> { NodeConfiguration::from_yaml_with_environment(yaml, &|_| None, false) }

const MANAGED_WORKLOAD_OIDC: &str = r#"
authn: !ManagedWorkloadOidc
  create:
    issuerUrl: https://create.example.invalid/
    issuerCaCertificate: create fixture CA
    audience: create-cell
    clientIdPrefix: create-
  assignment:
    issuerUrl: https://assignment.example.invalid/
    issuerCaCertificate: assignment fixture CA
    audience: assignment-cell
    clientIdPrefix: assignment-
    authorityPoint:
      relation: public.bj_installation
      rowId: authority
      validAtMicros: 0
"#;

const SCOPED_AUDIENCE_ISSUER: &str = r#"  scopedAudience:
    issuerUrl: https://audience.example.invalid/
    issuerCaCertificate: audience fixture CA
    clientIdPrefix: audience-
"#;

#[test]
fn managed_workload_oidc_enrolls_an_independent_scoped_audience_issuer() {
    let yaml = format!("{MANAGED_WORKLOAD_OIDC}{SCOPED_AUDIENCE_ISSUER}");
    let AuthConfig::ManagedWorkloadOidc(auth) = decode(&yaml).unwrap().auth else { panic!("expected managed workload OIDC"); };
    let audience = auth.scoped_audience().unwrap();
    assert_eq!(audience.issuer_url().as_str(), "https://audience.example.invalid/");
    assert_eq!(audience.client_id_prefix(), "audience-");

    for invalid in [
        yaml.replace("clientIdPrefix: audience-", "clientIdPrefix: create-scope-"),
        yaml.replace("clientIdPrefix: audience-", "clientIdPrefix: assignment"),
        yaml.replace("clientIdPrefix: audience-", "clientIdPrefix: bad:scope"),
        yaml.replace("https://audience.example.invalid/", "https://CREATE.example.invalid:443/scope"),
        yaml.replace("https://audience.example.invalid/", "http://audience.example.invalid/"),
        yaml.replace("    issuerCaCertificate: audience fixture CA\n", ""),
    ] {
        assert!(decode(&invalid).is_err());
    }
}

#[test]
fn managed_workload_oidc_has_disjoint_issuer_enrollments_and_one_authority_point() {
    let config = decode(MANAGED_WORKLOAD_OIDC).unwrap();
    let AuthConfig::ManagedWorkloadOidc(auth) = config.auth else { panic!("expected managed workload OIDC"); };
    assert_eq!(auth.create().issuer_url().as_str(), "https://create.example.invalid/");
    assert_eq!(auth.create().grants().audience(), "create-cell");
    assert!(auth.create().grants().assignment().is_none());
    assert_eq!(auth.assignment().issuer_url().as_str(), "https://assignment.example.invalid/");
    assert_eq!(auth.assignment().grants().client_id_prefix(), "assignment-");
    let point = rtdb_tx::transact::workload::Point::new(
        "public.bj_installation", "authority".into(), rtdb_types::Timestamp(0),
    ).unwrap();
    assert_eq!(auth.assignment().grants().assignment(), Some(&point));

    for prefix in ["create-".into(), "create-assignment-".into(), "cre".into(), "''".into(), "bad:prefix".into(), "a".repeat(129)] {
        let yaml = MANAGED_WORKLOAD_OIDC.replace("clientIdPrefix: assignment-", &format!("clientIdPrefix: {prefix}"));
        assert!(decode(&yaml).is_err(), "accepted prefix {prefix}");
    }
    for (original, replacement) in [
        ("https://assignment.example.invalid/", "https://CREATE.example.invalid:443/other-path"),
        ("https://assignment.example.invalid/", "http://assignment.example.invalid/"),
        ("issuerCaCertificate: assignment fixture CA", "issuerCaCertificate: ''"),
        ("    issuerCaCertificate: create fixture CA\n", ""),
        ("    authorityPoint:", "    unexpectedAuthorityPoint:"),
    ] {
        assert!(decode(&MANAGED_WORKLOAD_OIDC.replace(original, replacement)).is_err());
    }
}

#[tokio::test]
async fn default_runtime_refuses_the_managed_workload_oidc_profile() {
    let config = decode(MANAGED_WORKLOAD_OIDC).unwrap();
    let result = rtdb_server::ServerDriver::open(config, 1 << 30).await;
    assert_eq!(result.err().expect("Default requires another profile").code, "rtdb.auth/managed-workload-oidc-config");
}

#[test]
fn cluster_default_image_config_funds_the_required_remote_disk_cache() {
    let environment = |name: &str| match name {
        "RTDB_KAFKA_BOOTSTRAP" => Some("kafka.internal:9092".into()),
        "RTDB_KAFKA_TOPIC" => Some("rtdb".into()),
        "RTDB_BUCKET" => Some("rtdb-test-objects".into()),
        "AWS_REGION" => Some("eu-north-1".into()),
        "RTDB_ROOT_PASSWORD" => Some("fixture".into()),
        _ => None,
    };
    let config = NodeConfiguration::from_yaml_with_environment(
        include_str!("../../../release/cluster-default.yaml"), &environment, false,
    ).unwrap();
    assert!(matches!(config.primary.storage, Some(rtdb_format::database::proto::database_config::Storage::RemoteStorage(_))));
    assert!(config.node_options(1 << 30).unwrap().default_v1_serving);
    let cache = config.disk_cache.unwrap();
    assert_eq!(cache.path, std::path::Path::new("/var/lib/rtdb/cache"));
    assert_eq!(cache.maximum_bytes, Some(8 << 30));
}

#[test]
fn default_image_config_starts_without_a_password_and_takes_xtdb_password_like_upstream() {
    let image = include_str!("../../../release/default.yaml");
    let config = NodeConfiguration::from_yaml_with_environment(image, &|_| None, false).unwrap();
    assert!(matches!(config.auth, AuthConfig::SingleRootUser { password: None }));
    let config = NodeConfiguration::from_yaml_with_environment(image, &|name| (name == "XTDB_PASSWORD").then(|| "from-env".into()), false).unwrap();
    let AuthConfig::SingleRootUser { password: Some(password) } = config.auth else { panic!("XTDB_PASSWORD sets the root password") };
    assert_eq!(password.expose(), "from-env");
    assert_eq!(config.health.unwrap().port, 8080);
}

#[test]
fn embedded_node_defaults_are_the_server_defaults() {
    let config = decode("{}").unwrap();
    let embedded = rtdb_node::NodeOptions::default();
    assert_eq!(embedded.replication.rows_per_block, config.indexer.rows_per_block);
    assert_eq!(embedded.replication.flush_timeout, config.indexer.flush_duration);
    assert_eq!(config.compactor.threads, rtdb_node::NodeOptions::default_compaction_threads(|_| None));
    assert_eq!(config.node_options(1 << 30).unwrap().replication.node_id, config.node_id);
}

#[test]
fn startup_defaults_match_the_pinned_factories() {
    let config = decode("{}").unwrap();
    assert_eq!(config.server.unwrap().port, 0);
    assert_eq!(config.flight_sql.unwrap().transaction_idle_timeout, Duration::from_secs(1800));
    assert_eq!(config.indexer.log_limit, 64);
    assert_eq!(config.indexer.page_limit, 1024);
    assert_eq!(config.indexer.rows_per_block, 102400);
    assert_eq!(config.indexer.flush_duration, Duration::from_secs(900));
    assert_eq!(config.garbage.blocks_to_keep, 10);
    assert_eq!(config.garbage.garbage_lifetime, Duration::from_secs(86400));
    assert_eq!(config.memory_cache.maximum_ratio, 0.5);
    assert!(config.disk_cache.is_none());
    assert!(matches!(config.auth, AuthConfig::SingleRootUser { password: None }));
}

#[test]
fn ordered_index_reference_publication_uses_the_configured_format() -> rtdb_types::Result<()> {
    let automatic = decode("{}")?.node_options(1 << 20)?;
    assert!(!automatic.ordered_index_arrow_references);
    assert!(automatic.ordered_index_arrow_references_on_private_storage);
    let enabled = decode("orderedIndexArrowReferences: true")?.node_options(1 << 20)?;
    assert!(enabled.ordered_index_arrow_references);
    let disabled = decode("orderedIndexArrowReferences: false")?.node_options(1 << 20)?;
    assert!(!disabled.ordered_index_arrow_references);
    assert!(!disabled.ordered_index_arrow_references_on_private_storage);
    Ok(())
}

#[test]
fn yaml_and_edn_preserve_the_difference_between_omitted_and_configured_timezones() {
    let omitted = decode("{}").unwrap();
    assert_eq!(omitted.default_timezone, None);
    assert_eq!(omitted.node_options(1 << 20).unwrap().default_timezone, "Z");
    let utc = decode("defaultTz: UTC").unwrap();
    assert_eq!(utc.default_timezone.as_deref(), Some("UTC"));
    assert_eq!(utc.node_options(1 << 20).unwrap().default_timezone, "UTC");
    let (paris, _) = NodeConfiguration::from_edn_with_environment(
        r#"{:default-tz "Europe/Paris"}"#, &|_| None,
    ).unwrap();
    assert_eq!(paris.node_options(1 << 20).unwrap().default_timezone, "Europe/Paris");
}

#[test]
fn a_configured_default_timezone_does_not_reach_the_pgwire_session_zone() {
    let environment = |name: &str| (name == "TZ").then(|| "Asia/Tokyo".to_owned());
    let config = NodeConfiguration::from_yaml_with_environment("defaultTz: Europe/Paris", &environment, false).unwrap();
    assert_eq!(config.node_timezone(), "Europe/Paris");
    assert_eq!(config.pgwire_timezone(), "Asia/Tokyo");
}

#[cfg(unix)]
#[test]
fn the_host_timezone_is_found_where_the_jvm_looks_for_it() {
    use std::path::Path;
    let directory = tempfile::tempdir().unwrap();
    let zoneinfo = directory.path().join("zoneinfo");
    std::fs::create_dir_all(zoneinfo.join("Europe")).unwrap();
    std::fs::create_dir_all(zoneinfo.join("Asia")).unwrap();
    std::fs::write(zoneinfo.join("Europe/Paris"), b"paris").unwrap();
    std::fs::write(zoneinfo.join("Asia/Tokyo"), b"tokyo").unwrap();
    std::fs::write(zoneinfo.join("UTC"), b"utc").unwrap();
    let linked = directory.path().join("linked");
    std::os::unix::fs::symlink(zoneinfo.join("Europe/Paris"), &linked).unwrap();
    let copied = directory.path().join("copied");
    std::fs::write(&copied, b"tokyo").unwrap();
    let copied_utc = directory.path().join("copied-utc");
    std::fs::write(&copied_utc, b"utc").unwrap();
    let unknown = directory.path().join("unknown");
    std::fs::write(&unknown, b"elsewhere").unwrap();
    let missing = directory.path().join("missing");
    let zone = |tz: Option<&str>, localtime: &Path| host_timezone_from(tz, localtime, &zoneinfo, 0).unwrap();
    assert_eq!(zone(Some("Asia/Tokyo"), &linked), "Asia/Tokyo");
    assert_eq!(zone(Some(":Asia/Tokyo"), &missing), "Asia/Tokyo");
    assert_eq!(zone(Some(""), &linked), "Europe/Paris");
    assert_eq!(zone(None, &linked), "Europe/Paris");
    assert_eq!(zone(None, &copied), "Asia/Tokyo", "a copied localtime is matched by content, as the JDK does");
    assert_eq!(zone(None, &copied_utc), "UTC");
    assert_eq!(zone(None, &unknown), "GMT");
    assert_eq!(zone(None, &missing), "GMT");
    assert_eq!(zone(Some("PST"), &missing), "America/Los_Angeles");
    assert_eq!(zone(Some("EST"), &missing), "-05:00");
    assert_eq!(zone(Some("GMT+9"), &missing), "GMT+09:00");
}

#[cfg(unix)]
#[test]
fn a_tz_the_jvm_does_not_know_starts_in_a_gmt_offset_rather_than_failing_startup() {
    let paris = std::path::Path::new("/var/db/timezone/zoneinfo/Europe/Paris");
    for tz in ["Mars/Olympus", ":Mars/Olympus", "/etc/elsewhere"] {
        let zone = host_timezone_from(Some(tz), paris, std::path::Path::new("/usr/share/zoneinfo"), 0).unwrap();
        assert!(zone.starts_with("GMT"), "{tz}: {zone}");
        let environment = |name: &str| (name == "TZ").then(|| tz.to_owned());
        assert!(NodeConfiguration::from_yaml_with_environment("", &environment, false).unwrap().pgwire_timezone().starts_with("GMT"), "{tz}");
    }
}

#[test]
fn the_startup_environment_s_tz_is_the_pgwire_session_zone() {
    let environment = |name: &str| (name == "TZ").then(|| "Asia/Tokyo".to_owned());
    assert_eq!(NodeConfiguration::from_yaml_with_environment("", &environment, false).unwrap().pgwire_timezone(), "Asia/Tokyo");
}

#[test]
fn nonpositive_compactor_threads_disable_the_compactor() {
    for threads in [0, -1] { assert_eq!(decode(&format!("compactor: {{threads: {threads}}}")).unwrap().compactor.threads, 0); }
}

#[test]
fn startup_environment_resolves_inside_provider_properties_and_numeric_fields() {
    let environment = |name: &str| match name { "BROKERS" => Some("kafka:9092".into()), "PORT" => Some("8081".into()), "XTDB_PASSWORD" => Some("secret".into()), "XTDB_SKIP_TXS" => Some("9, 2".into()), _ => None };
    let config = NodeConfiguration::from_yaml_with_environment("remotes:\n  kafka: !Kafka\n    bootstrapServers: !Env BROKERS\n    propertiesMap:\n      sasl.jaas.config: 'secret-config'\nhealthz:\n  port: !Env PORT\nserver: null\nflightSql: null", &environment, false).unwrap();
    assert!(config.server.is_none());
    assert!(config.flight_sql.is_none());
    assert_eq!(config.health.as_ref().unwrap().port, 8081);
    assert_eq!(config.indexer.skip_transactions.iter().map(|position| position.0).collect::<Vec<_>>(), vec![2, 9]);
    assert!(matches!(config.remotes.get("kafka"), Some(RemoteConfig::Kafka { bootstrap_servers, .. }) if bootstrap_servers == "kafka:9092"));
    assert!(!format!("{config:?}").contains("secret"));
    assert!(!format!("{:?}", config.remotes).contains("secret-config"));
    let AuthConfig::SingleRootUser { password: Some(password) } = config.auth else { panic!() };
    assert_eq!(password.expose(), "secret");
}

#[test]
fn a_user_list_grants_databases_per_user() {
    let config = decode("authn: !UserList\n  users:\n    alice: !Argon2id encoded\n  grants:\n    alice:\n      '*': READ_WRITE\n      control: DENY\n      blog: READ").unwrap();
    let AuthConfig::UserList { grants, .. } = config.auth else { panic!() };
    assert_eq!(grants["alice"]["*"], rtdb_auth::GrantedAccess::Write);
    assert_eq!(grants["alice"]["control"], rtdb_auth::GrantedAccess::Deny);
    assert_eq!(grants["alice"]["blog"], rtdb_auth::GrantedAccess::Read);
    assert!(decode("authn: !UserList\n  users: {}\n  grants:\n    alice:\n      shop: ADMIN").is_err());
}

#[cfg(feature = "multi")]
#[test]
fn deployment_administration_grants_require_explicit_principals_and_flight() {
    assert!(decode("deploymentAdminPrincipals: [automation]").is_ok());
    assert!(decode("deploymentAdminPrincipals: [automation]\nflightSql: null").is_err());
    assert!(decode("deploymentAdminPrincipals: [123]").is_err());
    assert!(decode("deploymentAdminPrincipals: [' ']").is_err());
    assert!(decode("deploymentCreatePrincipals: [cell-creator]").is_ok());
    assert!(decode("deploymentCreatePrincipals: [cell-creator]\nflightSql: null").is_err());
    assert!(decode("deploymentCreatePrincipals: [123]").is_err());
    assert!(decode("defaultCheckpointRestoreDirectory: /var/lib/rtdb/restored").is_ok());
    assert!(decode("defaultCheckpointRestoreDirectory: relative/path").is_err());
}

#[test]
fn startup_auth_rules_remain_ordered_and_default_oidc_to_reference_trust() {
    let config = decode("authn: !UserList\n  users:\n    alice: !Argon2id encoded\n  rules:\n    - user: alice\n      method: PASSWORD\n    - remoteAddress: 127.0.0.1\n      method: TRUST").unwrap();
    let AuthConfig::UserList { users, rules, .. } = config.auth else { panic!() };
    assert_eq!(users["alice"].encoded(), "encoded");
    assert_eq!(rules[0].method, Method::Password);
    assert_eq!(rules[1].remote_address.as_ref().map(|block| block.as_str()), Some("127.0.0.1"));
    let config = decode("authn: !OpenIdConnect\n  issuerUrl: http://localhost:8080/realm\n  clientId: db\n  clientSecret: secret").unwrap();
    let AuthConfig::OpenIdConnect(config) = config.auth else { panic!() };
    assert_eq!(config.rules[0].method, Method::Trust);
    assert!(config.workload_grants.is_none());
}

#[test]
fn startup_bounds_private_oidc_issuer_trust_to_https() {
    let https = "authn: !OpenIdConnect\n  issuerUrl: https://issuer.example/realm\n  clientId: db\n  clientSecret: secret";
    let config = decode(&format!("{https}\n  issuerCaCertificate: private-ca")).unwrap();
    let AuthConfig::OpenIdConnect(config) = config.auth else { panic!() };
    assert_eq!(config.issuer_ca_pem.as_deref(), Some("private-ca"));
    assert!(decode(&format!("{https}\n  issuerCaCertificate: ''")).is_err());
    assert!(decode(&format!("{}\n  issuerCaCertificate: private-ca", https.replace("https:", "http:"))).is_err());
}

#[test]
fn startup_configures_an_explicit_workload_audience_and_client_prefix() {
    let oidc = "authn: !OpenIdConnect\n  issuerUrl: https://issuer.example/realm\n  clientId: db\n  clientSecret: secret";
    let config = decode(&format!("{oidc}\n  workloadGrants: {{audience: cell, clientIdPrefix: workload-}}")).unwrap();
    let AuthConfig::OpenIdConnect(config) = config.auth else { panic!() };
    let grants = config.workload_grants.unwrap();
    assert_eq!(grants.audience(), "cell");
    assert_eq!(grants.client_id_prefix(), "workload-");
    assert!(decode(&format!("{oidc}\n  workloadGrants: {{audience: cell}}")).is_err());
    assert!(decode(&format!("{oidc}\n  workloadGrants: {{audience: cell, clientIdPrefix: '', extra: true}}")).is_err());
}

#[test]
fn startup_rejects_remote_alias_collisions_and_unknown_fields() {
    let config = "logClusters:\n  shared: !Kafka {bootstrapServers: 'localhost:9092'}\nremotes:\n  shared: !S3 {accessKey: access, secretKey: secret}";
    assert_eq!(decode(config).unwrap_err().code, "xtdb/duplicate-remote-alias");
    assert!(decode("indexer: {logLmit: 5}").is_err());
    assert!(decode("server: {port: 65536}").is_err());
    assert!(decode("memoryCache: {maxSizeRatio: 1.1}").is_err());
    assert!(decode("remotes: {azure: !AzureBlob {connectionString: secret, typo: true}}").is_err());
}

#[test]
fn ingest_configuration_owns_a_fixed_database_set() {
    let config = NodeConfiguration::from_yaml_with_environment("databases:\n  ingest: {log: !InMemory {}, storage: !InMemory {}}\nhealthz: {port: 0}", &|_| None, true).unwrap();
    assert!(config.fixed_databases.unwrap().contains_key("ingest"));
    assert!(config.server.is_none());
    assert!(NodeConfiguration::from_yaml_with_environment("server: {port: 0}", &|_| None, true).is_err());
}

#[test]
fn durations_keep_fractional_and_component_sign_semantics() {
    assert_eq!(parse_duration("PT1.000000001S").unwrap(), Duration::new(1, 1));
    assert_eq!(parse_duration("P1DT2H3M4S").unwrap(), Duration::from_secs(93784));
    assert_eq!(parse_duration("PT1M-2S").unwrap(), Duration::from_secs(58));
    assert_eq!(parse_duration("-PT-1S").unwrap(), Duration::from_secs(1));
    assert!(parse_duration("PT-0.1S").is_err());
    assert!(parse_duration("P1MT").is_err());
    assert!(parse_duration("PT").is_err());
}

#[test]
fn scan_tie_order_uses_an_explicit_positive_naming_epoch() {
    assert!(decode("").unwrap().compactor.scan_tie_order_from_naming_epoch.is_none());
    for epoch in [0, -1] {
        assert!(decode(&format!("compactor: {{scanTieOrderFromNamingEpoch: {epoch}}}")).is_err());
    }
    let config = decode("compactor: {scanTieOrderFromNamingEpoch: 7}").unwrap();
    let options = config.node_options(1 << 20).unwrap();
    assert_eq!(options.compaction.scan_tie_order_from_naming_epoch.unwrap().get(), 7);
}

#[test]
fn a_malformed_auth_rule_remote_address_stops_the_configuration_loading() {
    let error = decode("authn: !UserList\n  users: {}\n  rules:\n    - method: TRUST\n      remoteAddress: 10.0.0.0/33\n").unwrap_err();
    assert_eq!(error.code, "xtdb.authn/invalid-remote-address");
}

#[test]
fn compactor_opt_in_221_turns_on_both_formats_from_yaml_or_environment() {
    let off = decode("{}").unwrap().compactor;
    assert!(!off.run_end_encoded_iids && !off.within_file_recency_branching);
    let on = decode("compactor: {optIn221: true}").unwrap().compactor;
    assert!(on.run_end_encoded_iids && on.within_file_recency_branching);
    let environment = |name: &str| (name == "XTDB_COMPACTOR_221_OPT_IN").then(|| "true".to_owned());
    let on = NodeConfiguration::from_yaml_with_environment("{}", &environment, false).unwrap().compactor;
    assert!(on.run_end_encoded_iids && on.within_file_recency_branching);
    for removed in ["reeIids", "withinFileRecencyBranching"] {
        assert!(decode(&format!("compactor: {{{removed}: true}}")).is_err(), "{removed}");
    }
}

#[test]
fn node_options_apply_indexer_compactor_gc_and_shared_memory_settings() {
    let config = decode("memoryCache: {maxSizeBytes: 1024}\nindexer: {enabled: false, rowsPerBlock: 42, pageLimit: 16, logLimit: 8, skipTxs: [3, 9], flushDuration: PT5S}\ncompactor: {threads: 0, optIn221: true}\ngarbageCollector: {enabled: false, blocksToKeep: 4, garbageLifetime: PT12H}\nreadOnlyDatabases: true").unwrap();
    let options = config.node_options(8192).unwrap();
    assert_eq!(options.cache_bytes, 1024);
    assert_eq!(options.execution_memory_bytes, 7168);
    assert!(!options.replication.enabled);
    assert_eq!(options.replication.rows_per_block, 42);
    assert_eq!(options.replication.flush_timeout, Duration::from_secs(5));
    assert_eq!(options.replication.skipped_transactions.len(), 2);
    assert_eq!(options.trie.page_limit(), 16);
    assert_eq!(options.trie.log_limit(), 8);
    assert_eq!(options.compaction_threads, 0);
    assert!(options.compaction.run_end_encoded_iids);
    assert!(options.compaction.within_file_recency_branching);
    assert!(!options.automatic_gc);
    assert_eq!(options.garbage.blocks_to_keep, 4);
    assert_eq!(options.garbage.garbage_lifetime, Duration::from_secs(43200));
    assert!(options.read_only);
    assert!(config.node_options(1024).is_err());
}

#[test]
fn garbage_collector_settings_accept_the_forms_xtdb_yaml_accepts() {
    for (text, blocks) in [("4", 4), ("+4", 4), ("'4'", 4), ("0x1A", 26), ("0o17", 15), ("0x+2", 2), ("-0x-1", 0), ("0x", 0), ("4.0", 0), ("0", 0), ("-0x1", 0)] {
        let decoded = decode(&format!("garbageCollector:\n  blocksToKeep: {text}\n"));
        match blocks {
            0 => assert!(decoded.is_err(), "{text}"),
            blocks => assert_eq!(decoded.unwrap().garbage.blocks_to_keep, blocks, "{text}"),
        }
    }
    for (text, seconds) in [("PT12H", 43_200.0), ("P1D", 86_400.0), ("p1dt1h", 90_000.0), ("PT1H-30M", 1_800.0), ("-PT-1H", 3_600.0), ("PT0.5S", 0.5), ("PT1,25S", 1.25), ("PT", -1.0), ("P", -1.0), ("1h", -1.0)] {
        let decoded = decode(&format!("garbageCollector:\n  garbageLifetime: {text}\n"));
        if seconds < 0.0 {
            assert!(decoded.is_err(), "{text}");
        } else {
            assert_eq!(decoded.unwrap().garbage.garbage_lifetime, Duration::from_secs_f64(seconds), "{text}");
        }
    }
    for key in ["readerLease", "readerLeaseMargin"] {
        assert!(decode(&format!("garbageCollector: {{{key}: PT1H}}")).is_err(), "{key} is not an XTDB key");
    }
}

#[test]
fn search_is_on_by_default_and_switches_off_without_touching_other_settings() {
    let defaults = decode("{}").unwrap().node_options(8192).unwrap();
    assert_eq!(defaults.artefact_threads, 1);
    let configured = decode("search: {threads: 3}").unwrap().node_options(8192).unwrap();
    assert_eq!(configured.artefact_threads, 3);
    let off = decode("search: {enabled: false}").unwrap().node_options(8192).unwrap();
    assert_eq!(off.artefact_threads, 0);
    assert!(off.scan_acceleration && defaults.scan_acceleration);
    assert_eq!(off.compaction_threads, defaults.compaction_threads);
    assert!(decode("search: {threads: 0}").is_err());
    assert!(decode("search: {unknown: 1}").is_err());
}

#[test]
fn search_write_format_defaults_to_paged_arrow_and_accepts_supported_versions() {
    use rtdb_node::SearchFileFormat;
    let configured = |yaml| decode(yaml).unwrap().search.write_format;
    assert_eq!(configured("{}"), SearchFileFormat::Paged);
    assert_eq!(configured("search: {threads: 2}"), SearchFileFormat::Paged);
    assert_eq!(configured("search: {writeFormat: 11}"), SearchFileFormat::Tantivy);
    assert_eq!(configured("search: {writeFormat: 12}"), SearchFileFormat::Arrow);
    assert_eq!(configured("search: {writeFormat: 13}"), SearchFileFormat::Paged);
    assert_eq!(configured("search: {writeFormat: 14}"), SearchFileFormat::Tree);
    assert_eq!(configured("search: {writeFormat: 15}"), SearchFileFormat::Packed);
    assert_eq!(configured("search: {writeFormat: 16}"), SearchFileFormat::Logical);
    assert!(decode("search: {writeFormat: 17}").is_err());
}


#[test]
fn an_out_of_range_listener_port_is_the_reference_illegal_argument() {
    for (yaml, port) in [("server: {port: 99999}", 99999), ("flightSql: {port: -2}", -2), ("healthz: {port: 65536}", 65536)] {
        let error = decode(yaml).unwrap_err();
        assert_eq!((error.code.as_str(), error.message), ("xtdb.error/illegal-arg", Some(format!("Port value out of range: {port}"))), "{yaml}");
    }
    assert!(decode("server: {port: -1}").is_ok());
}

#[test]
fn assignment_coordinates_come_from_operator_configuration() {
    let oidc = "authn: !OpenIdConnect\n  issuerUrl: https://issuer.example/realm\n  clientId: db\n  clientSecret: secret\n  rules: [{method: CLIENT_CREDENTIALS}]\n  workloadGrants:\n    audience: cell\n    clientIdPrefix: workload-";
    let config = decode(&format!("{oidc}\n    assignment: {{relation: public.bj_installation, rowId: authority, validAtMicros: 0}}")).unwrap();
    let AuthConfig::OpenIdConnect(config) = config.auth else { panic!() };
    let grants = config.workload_grants.unwrap();
    let point = grants.assignment().unwrap();
    assert_eq!(point.table.to_string(), "public.bj_installation");
    assert_eq!(point.valid_at, rtdb_types::Timestamp(0));
    assert!(decode(&format!("{}\n    assignment: {{relation: public.bj_installation, rowId: authority, validAtMicros: 0}}",
        oidc.replace("CLIENT_CREDENTIALS", "TRUST"))).is_err());
    assert!(decode(&format!("{oidc}\n    assignment: {{relation: bj_installation, rowId: authority, validAtMicros: 0}}")).is_err());
    assert!(decode(&format!("{oidc}\n    assignment: {{relation: public.bj_installation, rowId: authority}}")).is_err());
}

#[test]
fn default_erasure_reads_yaml_then_its_historical_environment_variables() {
    let off = decode("{}").unwrap().default_erasure;
    assert_eq!((off.local, off.kafka), (false, false));
    let on = decode("defaultLocalErasure: true\ndefaultKafkaErasure: true").unwrap().default_erasure;
    assert_eq!((on.local, on.kafka), (true, true));
    let environment = |name: &str| match name {
        "RTDB_DEFAULT_LOCAL_ERASURE" => Some("1".into()),
        "RTDB_DEFAULT_KAFKA_ERASURE" => Some("0".into()),
        _ => None,
    };
    let from_environment = NodeConfiguration::from_yaml_with_environment("{}", &environment, false).unwrap().default_erasure;
    assert_eq!((from_environment.local, from_environment.kafka), (true, false));
    let overridden = NodeConfiguration::from_yaml_with_environment("defaultLocalErasure: false", &environment, false).unwrap().default_erasure;
    assert!(!overridden.local);
    for value in ["true", "yes", " 1", "2"] {
        let environment = |name: &str| (name == "RTDB_DEFAULT_LOCAL_ERASURE").then(|| value.to_owned());
        assert_eq!(NodeConfiguration::from_yaml_with_environment("{}", &environment, false).unwrap_err().code, "xtdb/invalid-node-config", "{value}");
    }
    assert!(decode("defaultKafkaErasure: 1").is_err());
}

#[test]
fn configured_kafka_erasure_reaches_backend_resource_validation() {
    let config = decode("defaultV1Serving: true\ndefaultKafkaErasure: true\nserver: null\nflightSql: null").unwrap();
    let memory = rtdb_columnar::MemoryBudget::new(64 << 20);
    let metrics = rtdb_metrics::Registry::new("fixture", std::sync::Arc::clone(&memory)).unwrap();
    match rtdb_server::open_backend_resources(&config, &memory, &metrics) {
        Err(error) => assert_eq!(error.code, "rtdb.server/kafka-erasure"),
        Ok(_) => panic!("Kafka erasure without its key vaults opened"),
    }
}

#[test]
fn key_vaults_are_captured_from_the_startup_environment() {
    let environment = |name: &str| match name {
        "RTDB_CLASSIC_KEY_VAULT" => Some("/vaults/classic".into()),
        "RTDB_V1_TOKEN_KEY_VAULT" => Some("/vaults/token".into()),
        "RTDB_V1_CHECKPOINT_KEY_VAULT" => Some("/vaults/checkpoint".into()),
        _ => None,
    };
    let expected = KeyVaultSettings { classic: Some("/vaults/classic".into()), v1_token: Some("/vaults/token".into()), v1_checkpoint: Some("/vaults/checkpoint".into()) };
    for ingest in [false, true] {
        assert_eq!(NodeConfiguration::from_yaml_with_environment("", &environment, ingest).unwrap().key_vaults, expected);
    }
    assert_eq!(decode("").unwrap().key_vaults, KeyVaultSettings::default());
}

#[test]
fn backend_resources_open_the_configured_classic_key_vault() {
    let missing = tempfile::tempdir().unwrap().path().join("classic-vault");
    let environment = |name: &str| (name == "RTDB_CLASSIC_KEY_VAULT").then(|| missing.to_str().unwrap().to_owned());
    let config = NodeConfiguration::from_yaml_with_environment("server: null\nflightSql: null", &environment, false).unwrap();
    let memory = rtdb_columnar::MemoryBudget::new(64 << 20);
    let metrics = rtdb_metrics::Registry::new("fixture", std::sync::Arc::clone(&memory)).unwrap();
    assert!(rtdb_server::open_backend_resources(&config, &memory, &metrics).is_err(), "a missing configured vault opened");
}

#[test]
fn apple_sync_is_validated_and_installed_from_the_startup_environment() {
    let typo = |name: &str| (name == rtdb_server::config::APPLE_SYNC_SETTING).then(|| "fsnyc".to_owned());
    assert_eq!(rtdb_server::config::install_apple_sync(&typo).unwrap_err().code, "rtdb.storage/apple-sync");
    assert_eq!(rtdb_server::config::install_apple_sync(&|_| None).unwrap(), rtdb_storage::AppleSync::Full);
    assert_eq!(rtdb_storage::AppleSync::installed(), rtdb_storage::AppleSync::Full);
    let weaker = |name: &str| (name == rtdb_server::config::APPLE_SYNC_SETTING).then(|| "fsync".to_owned());
    assert!(rtdb_server::config::install_apple_sync(&weaker).is_err());
}
