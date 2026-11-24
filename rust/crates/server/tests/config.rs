use std::time::Duration;
use rtdb_auth::{AuthConfig, Method};
use rtdb_server::config::{NodeConfiguration, RemoteConfig, parse_duration};

fn decode(yaml: &str) -> rtdb_types::Result<NodeConfiguration> { NodeConfiguration::from_yaml_with_environment(yaml, &|_| None, false) }

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
    assert_eq!(config.garbage.reader_lease, Duration::from_secs(3600));
    assert_eq!(config.garbage.reader_lease_margin, Duration::from_secs(60));
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
    assert_eq!(rules[1].remote_address.as_deref(), Some("127.0.0.1"));
    let config = decode("authn: !OpenIdConnect\n  issuerUrl: http://localhost:8080/realm\n  clientId: db\n  clientSecret: secret").unwrap();
    let AuthConfig::OpenIdConnect(config) = config.auth else { panic!() };
    assert_eq!(config.rules[0].method, Method::Trust);
    assert!(config.workload_grants.is_none());
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
fn node_options_apply_indexer_compactor_gc_and_shared_memory_settings() {
    let config = decode("memoryCache: {maxSizeBytes: 1024}\nindexer: {enabled: false, rowsPerBlock: 42, pageLimit: 16, logLimit: 8, skipTxs: [3, 9], flushDuration: PT5S}\ncompactor: {threads: 0, reeIids: true, withinFileRecencyBranching: true}\ngarbageCollector: {enabled: false, blocksToKeep: 4, garbageLifetime: PT12H, readerLease: PT30M, readerLeaseMargin: PT20S}\nreadOnlyDatabases: true").unwrap();
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
    assert_eq!(options.garbage.reader_lease, Duration::from_secs(1800));
    assert_eq!(options.garbage.reader_lease_margin, Duration::from_secs(20));
    assert!(options.read_only);
    assert!(config.node_options(1024).is_err());
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
