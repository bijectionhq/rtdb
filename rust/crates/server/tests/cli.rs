use std::io::{Cursor, Write};
use rtdb_server::cli::{self, Command, NodeMode};

fn parse(arguments: &[&str]) -> Command { cli::parse(arguments.iter().map(|value| (*value).into()), &|_| None).unwrap() }

#[test]
fn dev_uses_loopback_listeners_and_recovers_a_committed_write() {
    std::thread::Builder::new()
        .stack_size(rtdb_node::WORKER_STACK_BYTES)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .thread_stack_size(rtdb_node::WORKER_STACK_BYTES)
                .enable_all()
                .build()
                .unwrap()
                .block_on(dev_restart());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn dev_restart() {
    let directory = tempfile::tempdir().unwrap();
    let data = directory.path().join("dev data");
    let Command::Dev(options) = parse(&["dev", "--data", data.to_str().unwrap(), "--port", "0", "--flight-sql-port", "0"]) else {
        panic!("expected dev startup");
    };
    for reopening in [false, true] {
        let limit = options.allocation_limit_bytes;
        let (config, memory, _configuration) = options.load_reserved().unwrap().into_execution(limit).unwrap();
        assert!(config.node_options(limit).unwrap().default_v1_serving);
        let (handle, driver) = rtdb_server::ServerDriver::open_with_memory(config, limit, memory).await.unwrap();
        assert!(handle.addresses().pgwire.unwrap().ip().is_loopback());
        assert!(handle.addresses().flight_sql.unwrap().ip().is_loopback());
        let driver = tokio::spawn(driver.run());
        let user = handle.authenticator().authenticate_trusted("xtdb").unwrap();
        let mut session = handle.node().session("xtdb", user).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            if reopening {
                handle.node().sync_all(std::time::Duration::from_secs(10), session.state().cancellation()).await.unwrap();
                let mut query = session.query_sql("SELECT value FROM docs WHERE _id = 1", vec![]).await.unwrap();
                let batch = query.next_batch().await.unwrap().unwrap();
                assert_eq!(batch.batch().num_rows(), 1);
                assert_eq!(
                    rtdb_columnar::read_value(batch.batch().schema().field(0), batch.batch().column(0).as_ref(), 0).unwrap(),
                    rtdb_types::Value::I64(42),
                );
            } else {
                let insert = rtdb_tx::TxOp::Sql { sql: "INSERT INTO docs (_id, value) VALUES (1, 42)".into(), args: None };
                let result = session.state_mut().execute_tx(vec![insert], Default::default()).await.unwrap();
                assert!(result.committed(), "{:?}", result.error());
            }
        }).await.unwrap();
        drop(session);
        handle.shutdown();
        driver.await.unwrap().unwrap();
    }
    assert!(data.join("log").is_dir());
    assert!(data.join("storage").is_dir());
}

#[test]
fn multi_is_explicit_in_development_and_cluster_startup() {
    let Command::Dev(development) = parse(&["dev", "--multi", "--data", "data"]) else { panic!("expected development startup") };
    assert!(development.multi);
    assert_eq!(development.data, std::path::Path::new("data"));
    assert!(matches!(
        parse(&["node", "--multi", "-f", "cluster.yaml"]),
        Command::StartMulti { config_env: None, .. }
    ));
    assert!(cli::parse(["dev", "--multi", "--port", "5432"].map(str::to_owned), &|_| None).is_err());
}

#[test]
fn multi_development_tls_requires_an_exact_incarnation_and_explicit_local_data() {
    let id = "01234567-89ab-4def-8000-000000000001";
    let Command::Dev(options) = parse(&[
        "dev", "--multi", "--data", "fresh", "--multi-tls-dir", "tls",
        "--incarnation", id, "--flight-sql-port", "0",
    ]) else { panic!("expected Multi development startup") };
    assert_eq!(options.multi_tls.unwrap().incarnation.to_string(), id);

    let journal = "logkeeper+tls://127.0.0.1:5448,127.0.0.1:5449,127.0.0.1:5450";
    let Command::Dev(options) = parse(&[
        "dev", "--multi", "--data", "fresh", "--multi-tls-dir", "tls",
        "--incarnation", id, "--multi-journal-url", journal, "--multi-receiver-clock", "receiver.yaml",
    ]) else { panic!("expected Multi development startup") };
    assert_eq!(options.multi_tls.unwrap().incarnation.to_string(), id);

    let invalid: &[&[&str]] = &[
        &["dev", "--multi", "--data", "fresh", "--multi-journal-url", journal],
        &["dev", "--multi", "--data", "fresh", "--multi-receiver-clock", "receiver.yaml"],
        &["dev", "--multi", "--data", "fresh", "--multi-tls-dir", "tls", "--incarnation", id, "--multi-receiver-clock"],
        &["dev", "--data", "fresh", "--multi-tls-dir", "tls", "--incarnation", id],
        &["dev", "--multi", "--multi-tls-dir", "tls", "--incarnation", id],
        &["dev", "--multi", "--data", "fresh", "--multi-tls-dir", "tls"],
        &["dev", "--multi", "--data", "fresh", "--incarnation", id],
        &["dev", "--multi", "--data", "fresh", "--multi-tls-dir", "tls", "--incarnation", "00000000-0000-0000-0000-000000000000"],
        &["dev", "--multi", "--data", "fresh", "--multi-tls-dir", "tls", "--incarnation", id, "--flight-sql-port", "-1"],
    ];
    for arguments in invalid {
        assert!(cli::parse(arguments.iter().copied().map(str::to_owned), &|_| None).is_err(), "{arguments:?}");
    }
}

#[test]
fn multi_tls_development_mounts_custody_only_for_the_exact_original() {
    let directory = tempfile::tempdir().unwrap();
    let mut password = tempfile::NamedTempFile::new_in(directory.path()).unwrap();
    password.write_all(&[b'a'; 64]).unwrap();
    password.persist(directory.path().join("password")).unwrap();
    let provider = tempfile::NamedTempFile::new_in(directory.path()).unwrap();
    let directory = directory.path().to_str().unwrap();
    let provider = provider.path().to_str().unwrap();
    let id = "01234567-89ab-4def-8000-000000000001";
    let arguments = [
        "dev", "--multi", "--data", "fresh", "--incarnation", id,
        "--multi-tls-dir", directory, "--custody-provider-config", provider,
    ];
    let Command::Dev(options) = parse(&arguments) else { panic!("expected development TLS"); };
    let config = format!(r#"custodyProviders:
  - database: xtdb
    endpoint: https://127.0.0.1:7777/observe
    bearerToken: fixture-custody-token
    incarnation: "{}"
    installation: "{}"
    sourceHistory: "{}"
    registry: "{}"
    originalWriter: "{}"
    providerPublicKey: "{}"
    writerTable: public.writer
    writerId: authority
    writerValidAtMicros: "0"
"#, id.replace('-', ""), "01".repeat(16), "02".repeat(32), "03".repeat(32), "04".repeat(32), "05".repeat(32));
    std::fs::write(provider, &config).unwrap();
    let loaded = options.load_reserved().unwrap();
    let providers = &loaded.configuration().flight_sql.as_ref().unwrap().custody_providers;
    assert_eq!(providers.len(), 1);
    assert_eq!(providers[0].database, "xtdb");

    for wrong in [
        config.replace("database: xtdb", "database: other"),
        config.replace(&id.replace('-', ""), &"06".repeat(16)),
        config.replace("database: xtdb", "database: xtdb\n    owner: other"),
        "custodyProviders: []".into(),
    ] {
        std::fs::write(provider, wrong).unwrap();
        assert!(options.load_reserved().is_err());
    }
    let no_tls = ["dev", "--multi", "--data", "fresh", "--custody-provider-config", provider];
    assert!(cli::parse(no_tls.map(str::to_owned), &|_| None).is_err());
    let mixed_auth = arguments.into_iter().chain(["--multi-authn", provider]).map(str::to_owned);
    assert!(cli::parse(mixed_auth, &|_| None).is_err());
}

#[test]
fn multi_tls_development_authn_preserves_separate_oidc_and_user_list_profiles() {
    use rtdb_auth::{AuthConfig, GrantedAccess};
    let mut input = tempfile::NamedTempFile::new().unwrap();
    let path = input.path().to_str().unwrap().to_owned();
    let path = path.as_str();
    let id = "01234567-89ab-4def-8000-000000000001";
    let valid = [
        "dev", "--multi", "--data", "fresh", "--incarnation", id,
        "--multi-tls-dir", "tls", "--multi-authn", path,
    ];
    let Command::Dev(options) = parse(&valid) else { panic!("expected development TLS"); };

    let oidc = r#"authn: !OpenIdConnect
  issuerUrl: https://127.0.0.1:7777
  issuerCaCertificate: fixture-ca
  clientId: rtdb
  clientSecret: fixture
  workloadGrants:
    audience: local-create
    clientIdPrefix: managed-
  rules:
    - method: CLIENT_CREDENTIALS
"#;
    input.write_all(oidc.as_bytes()).unwrap();
    let loaded = options.load_reserved().unwrap();
    let AuthConfig::OpenIdConnect(auth) = &loaded.configuration().auth else { panic!("expected OIDC"); };
    assert_eq!(auth.issuer_ca_pem.as_deref(), Some("fixture-ca"));
    assert_eq!(auth.workload_grants.as_ref().unwrap().audience(), "local-create");
    assert_eq!(loaded.configuration().flight_sql.as_ref().unwrap().host, "127.0.0.1");

    let user_list = r#"authn: !UserList
  users:
    placement: !Argon2id fixture-hash
  grants:
    placement:
      xtdb: READ_WRITE
"#;
    std::fs::write(path, user_list).unwrap();
    let loaded = options.load_reserved().unwrap();
    let AuthConfig::UserList { grants, .. } = &loaded.configuration().auth else { panic!("expected UserList"); };
    assert_eq!(grants["placement"]["xtdb"], GrantedAccess::Write);

    for yaml in [
        "authn: !SingleRootUser {}",
        "flightSql: {host: '0.0.0.0'}",
        "",
    ] {
        std::fs::write(path, yaml).unwrap();
        assert!(options.load_reserved().is_err());
    }
    for invalid in [
        format!("{user_list}server: null\n"),
        user_list.replace("xtdb:", "'*':"),
        user_list.split("  grants:").next().unwrap().into(),
    ] {
        std::fs::write(path, invalid).unwrap();
        assert!(options.load_reserved().is_err());
    }
    #[cfg(feature = "multi")]
    {
        std::fs::write(path, format!("{user_list}deploymentCreatePrincipals: [placement]\n")).unwrap();
        assert!(options.load_reserved().is_ok());
    }
    assert!(cli::parse(["dev", "--multi", "--multi-authn", path].map(str::to_owned), &|_| None).is_err());
}

#[test]
fn multi_tls_development_admits_a_separate_loopback_managed_workload_oidc_profile() {
    use rtdb_auth::AuthConfig;

    let mut input = tempfile::NamedTempFile::new().unwrap();
    let path = input.path().to_str().unwrap().to_owned();
    let arguments = [
        "dev", "--multi", "--data", "fresh", "--incarnation", "01234567-89ab-4def-8000-000000000001",
        "--multi-tls-dir", "tls", "--multi-authn", &path,
    ];
    let Command::Dev(options) = parse(&arguments) else { panic!("expected development TLS"); };
    let profile = r#"authn: !ManagedWorkloadOidc
  create:
    issuerUrl: https://127.0.0.3/
    issuerCaCertificate: create fixture CA
    audience: local-create
    clientIdPrefix: local-create-
  assignment:
    issuerUrl: https://127.0.0.4/
    issuerCaCertificate: assignment fixture CA
    audience: local-assignment
    clientIdPrefix: local-assignment-
    authorityPoint:
      relation: public.app_installation
      rowId: authority
      validAtMicros: 0
"#;
    input.write_all(profile.as_bytes()).unwrap();
    let loaded = options.load_reserved().unwrap();
    let AuthConfig::ManagedWorkloadOidc(auth) = &loaded.configuration().auth else { panic!("expected combined OIDC"); };
    assert_eq!(auth.create().issuer_url().as_str(), "https://127.0.0.3/");
    assert_eq!(auth.assignment().issuer_url().as_str(), "https://127.0.0.4/");
    assert_eq!(auth.assignment().grants().client_id_prefix(), "local-assignment-");
    assert!(auth.create().grants().assignment().is_none());

    for invalid in [
        profile.replace("127.0.0.3", "create.example.invalid"),
        profile.replace("127.0.0.4", "assignment.example.invalid"),
        profile.replace("127.0.0.4", "127.0.0.3"),
        profile.replace("clientIdPrefix: local-assignment-", "clientIdPrefix: local-create-shadow-"),
        format!("{profile}deploymentCreatePrincipals: [seed]\n"),
    ] {
        std::fs::write(&path, invalid).unwrap();
        assert!(options.load_reserved().is_err());
    }
}

#[test]
fn configuration_environment_is_an_explicit_multi_only_source() {
    let Command::StartMulti { options, config_env, .. } =
        parse(&["node", "--config-env", "MULTI_NODE_YAML", "--multi"])
    else {
        panic!("expected Multi startup");
    };
    assert!(options.file.is_none());
    assert_eq!(config_env.as_deref(), Some("MULTI_NODE_YAML"));

    let invalid: &[&[&str]] = &[
        &["node", "--config-env", "MULTI_NODE_YAML"],
        &["node", "--multi", "--config-env", "MULTI_NODE_YAML", "-f", "cluster.yaml"],
        &["node", "--multi", "--config-env", "--file"],
        &["node", "--multi", "--config-env", "ONE", "--config-env", "TWO"],
    ];
    for arguments in invalid {
        assert!(cli::parse(arguments.iter().copied().map(str::to_owned), &|_| None).is_err());
    }
}

#[test]
fn native_follower_is_an_explicit_installed_multi_startup() {
    let Command::StartMulti { native_follower, .. } =
        parse(&["node", "--native-follower", "follower.json", "--multi"])
    else {
        panic!("expected Multi startup");
    };
    assert_eq!(native_follower.as_deref(), Some(std::path::Path::new("follower.json")));

    let invalid: &[&[&str]] = &[
        &["node", "--native-follower", "follower.json"],
        &["dev", "--multi", "--native-follower", "follower.json"],
        &["node", "--multi", "--native-follower", "--memory-limit", "123"],
        &["node", "--multi", "--native-follower", "one", "--native-follower", "two"],
    ];
    for arguments in invalid {
        assert!(cli::parse(arguments.iter().copied().map(str::to_owned), &|_| None).is_err());
    }
}

#[test]
fn dev_rejects_missing_data_and_node_config_overrides() {
    for arguments in [["dev", "--data", "--port"], ["dev", "-f", "node.yaml"]] {
        assert!(cli::parse(arguments.map(str::to_owned), &|_| None).is_err());
    }
    assert!(matches!(parse(&["dev", "--help"]), Command::Help));
}

#[test]
fn object_sort_property_is_resolved_once_and_reaches_node_query_options() {
    use rtdb_types::ObjectSort;
    let Command::Start(options) = parse(&["-Djava.util.Arrays.useLegacyMergeSort=TrUe","playground"]) else { panic!("expected start") };
    assert_eq!(options.object_sort,ObjectSort::LegacyMergeSort);
    assert_eq!(options.load().unwrap().node_options(1 << 26).unwrap().query.object_sort,ObjectSort::LegacyMergeSort);
    let parsed = options.load_reserved().unwrap();
    assert_eq!(parsed.configuration().node_options(1 << 26).unwrap().query.object_sort,ObjectSort::LegacyMergeSort);
    let Command::Start(default) = parse(&["playground"]) else { panic!("expected start") };
    assert_eq!(default.object_sort,ObjectSort::TimSort);
    assert_eq!(parsed.configuration().object_sort,ObjectSort::LegacyMergeSort);
    for argument in ["-Djava.util.Arrays.useLegacyMergeSort","-Djava.util.Arrays.useLegacyMergeSort=false","-Djava.util.Arrays.useLegacyMergeSort= true","-Djava.util.Arrays.useLegacyMergeSort=yes"] {
        let Command::Start(options) = parse(&["-Djava.util.Arrays.useLegacyMergeSort=true","node",argument]) else { panic!("expected start") };
        assert_eq!(options.object_sort,ObjectSort::TimSort);
    }
    let Command::CompactorReset { options,.. } = parse(&["reset-compactor","xtdb","-Djava.util.Arrays.useLegacyMergeSort=true"]) else { panic!("expected reset") };
    assert_eq!(options.object_sort,ObjectSort::LegacyMergeSort);
    for argument in ["-Dxtdb.vector.null-checks=false","-Dunknown=true"] {
        assert!(cli::parse([argument.to_owned(),"node".into()],&|_|None).is_err());
        assert!(cli::parse(["node".into(),argument.to_owned()],&|_|None).is_err());
    }
    assert!(cli::parse([format!("-Djava.util.Arrays.useLegacyMergeSort={}","x".repeat(4096))],&|_|None).is_err());
}

#[test]
fn captured_object_sort_survives_yaml_edn_and_ingest_configuration_loading() {
    let directory = tempfile::tempdir().unwrap();
    for (mode,file,text) in [("node","node.yaml","server: null\n"),("node","node.edn","{}"),("ingest","ingest.yaml","databases: {}\n")] {
        let path = directory.path().join(file);std::fs::write(&path,text).unwrap();
        let Command::Start(options) = parse(&[mode,"-f",path.to_str().unwrap(),"-Djava.util.Arrays.useLegacyMergeSort=true"]) else { panic!("expected start") };
        assert_eq!(options.load().unwrap().node_options(1 << 26).unwrap().query.object_sort,rtdb_types::ObjectSort::LegacyMergeSort);
        let parsed = options.load_reserved().unwrap();
        let (configuration,_,lease) = parsed.into_execution(1 << 26).unwrap();
        assert_eq!(configuration.node_options(1 << 26).unwrap().query.object_sort,rtdb_types::ObjectSort::LegacyMergeSort);
        drop(configuration);drop(lease);
    }
}

#[test]
fn compiled_build_info_ignores_runtime_version_overrides() {
    assert!(matches!(parse(&["--build-info"]), Command::BuildInfo));
    assert!(cli::parse(["--build-info", "extra"].map(str::to_owned), &|_| None).is_err());
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_rtdb")).arg("--build-info").env("XTDB_VERSION", "runtime-only-version").env("GIT_SHA", "runtime-only-revision").output().unwrap();
    assert!(output.status.success());
    let actual: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let mut expected = serde_json::to_value(rtdb_types::compiled_build_info()).unwrap();
    expected["standalone_protocol"] = 3.into();
    assert_eq!(actual, expected);
    assert_eq!(actual["tzdb_sha256"].as_str().unwrap().len(), 64);
    assert_ne!(actual["version"], "runtime-only-version");
}

#[test]
fn installed_standalone_requires_the_canonical_uuid_and_rejects_legacy_action_flags() {
    let id = "01234567-89ab-4def-8000-000000000001";
    let init = ["initialize-standalone", "--state", "stage", "--name", "team.dev", "--id", id];
    let Command::InitializeStandalone(target) = parse(&init) else { panic!("expected standalone initialization") };
    assert_eq!(target.name, "team.dev");
    assert_eq!(target.id.to_string(), id);
    for invalid_id in ["00000000-0000-0000-0000-000000000000", "01234567-89AB-4DEF-8000-000000000001"] {
        let mut arguments = init;
        arguments[6] = invalid_id;
        assert!(cli::parse(arguments.map(str::to_owned), &|_| None).is_err());
    }
    let launch = [
        "standalone", "--state", "installed", "--name", "team.dev", "--id", id,
        "--certificate", "certificate.pem", "--private-key", "key.pem",
        "--password-file", "password", "--ready-file", "ready.json", "--parent-stdin",
        "--custody-provider-config", "provider.yaml",
    ];
    let Command::Standalone(options) = parse(&launch) else { panic!("expected standalone launch") };
    assert_eq!(options.custody_provider_config.as_deref(), Some(std::path::Path::new("provider.yaml")));
    assert!(cli::parse(launch.into_iter().filter(|flag| *flag != "--parent-stdin").map(str::to_owned), &|_| None).is_err());
    assert!(cli::parse(launch.into_iter().chain(["--owned-flight"]).map(str::to_owned), &|_| None).is_err());
    assert!(cli::parse(init.into_iter().chain(["--mode", "default"]).map(str::to_owned), &|_| None).is_err());
}

#[test]
fn node_and_ingest_options_keep_configuration_and_allocation_limits() {
    let Command::Start(options) = parse(&["node", "-f", "node.yaml", "--memory-limit", "1048576"]) else { panic!("expected start") };
    assert_eq!(options.mode, NodeMode::Node);
    assert_eq!(options.file.unwrap().to_str(), Some("node.yaml"));
    assert_eq!(options.allocation_limit_bytes, 1048576);
    let Command::Start(options) = cli::parse(["ingest".into()], &|name| (name == "RTDB_MEMORY_LIMIT_BYTES").then(|| "2097152".into())).unwrap() else { panic!("expected start") };
    assert_eq!(options.mode, NodeMode::Ingest);
    assert_eq!(options.allocation_limit_bytes, 2097152);
    let Command::Start(options) = parse(&["compactor", "-f", "node.yaml"]) else { panic!("expected compactor") };
    assert_eq!(options.mode, NodeMode::Compactor);
    assert_eq!(options.allocation_limit_bytes, cli::default_memory_limit_bytes());
    let Command::Start(options) = cli::parse(["node".into(), "--memory-limit".into(), "1048576".into()], &|name| (name == "RTDB_MEMORY_LIMIT_BYTES").then(|| "2097152".into())).unwrap() else { panic!("expected start") };
    assert_eq!(options.allocation_limit_bytes, 1048576);
    assert!(matches!(parse(&["node", "--help"]), Command::Help));
    assert!(cli::parse(["--memory-limit".into(), "0".into()], &|_| None).is_err());
}

#[test]
fn playground_sets_ephemeral_ports_and_trusted_in_memory_configuration() {
    let Command::Start(options) = parse(&["playground", "--port", "0", "--flight-sql-port", "-1"]) else { panic!("expected start") };
    let config = options.load().unwrap();
    assert!(config.playground);
    assert_eq!(config.server.as_ref().unwrap().host, "0.0.0.0");
    assert_eq!(config.flight_sql.as_ref().unwrap().host, "0.0.0.0");
    assert_eq!(config.server.unwrap().port, 0);
    assert_eq!(config.flight_sql.unwrap().port, -1);
    assert!(matches!(config.auth, rtdb_auth::AuthConfig::SingleRootUser { password: None }));
    let config = options.load_reserved().unwrap();
    assert_eq!(config.configuration().server.as_ref().unwrap().host, "0.0.0.0");
    assert_eq!(config.configuration().flight_sql.as_ref().unwrap().host, "0.0.0.0");
    assert_eq!(rtdb_server::config::PgwireSettings::default().host, "127.0.0.1");
    assert_eq!(rtdb_server::config::FlightSettings::default().host, "127.0.0.1");
}

#[test]
fn password_algorithms_conflict_and_missing_input_is_reported_without_echoing_it() {
    assert!(cli::parse(["hash-password", "--bcrypt", "--argon2id"].map(str::to_owned), &|_| None).is_err());
    let command = parse(&["hash-password", "--bcrypt", "--", "secret"]);
    assert!(!format!("{command:?}").contains("secret"));
    assert!(cli::hash_password(false, None, &mut Cursor::new(Vec::<u8>::new())).is_err());
}

#[test]
fn snapshot_export_requires_database_and_preserves_dry_run_configuration() {
    let Command::SnapshotExport { options, database, dry_run } = parse(&["export-snapshot", "analytics", "--dry-run", "-f", "node.edn", "--memory-limit", "1048576"]) else { panic!("expected snapshot export") };
    assert_eq!(database, "analytics");
    assert!(dry_run);
    assert_eq!(options.file.unwrap().to_str(), Some("node.edn"));
    assert_eq!(options.allocation_limit_bytes, 1048576);
    assert!(cli::parse(["export-snapshot".into()], &|_| None).is_err());
    assert!(cli::parse(["export-snapshot", "one", "two"].map(str::to_owned), &|_| None).is_err());
}

#[test]
fn compactor_reset_requires_database_and_preserves_dry_run_configuration() {
    let Command::CompactorReset { options, database, dry_run, retained_base: None } = parse(&["reset-compactor", "analytics", "--dry-run", "-f", "node.yaml"]) else { panic!("expected compactor reset") };
    assert_eq!(database, "analytics");
    assert!(dry_run);
    assert_eq!(options.file.unwrap().to_str(), Some("node.yaml"));
    assert!(cli::parse(["reset-compactor".into()], &|_| None).is_err());
    assert!(cli::parse(["reset-compactor", "one", "two"].map(str::to_owned), &|_| None).is_err());
}

#[test]
fn reset_compactor_accepts_a_retained_export_only_for_reset() {
    let Command::CompactorReset { retained_base: Some(export), dry_run: true, .. } =
        parse(&["reset-compactor", "xtdb", "--retained-base", "export", "--dry-run"])
    else { panic!("expected retained reset") };
    assert_eq!(export.to_str(), Some("export"));
    for arguments in [
        &["reset-compactor", "xtdb", "--retained-base", "--dry-run"][..],
        &["export-snapshot", "xtdb", "--retained-base", "export"][..],
        &["reset-compactor", "xtdb", "--retained-base", "first", "--retained-base", "second"][..],
    ] {
        assert!(cli::parse(arguments.iter().map(|arg| (*arg).to_owned()), &|_| None).is_err());
    }
}

#[test]
fn retain_repair_base_requires_a_detached_store_and_checkpoint_receipt() {
    let base = format!("9@{}", "a1".repeat(32));
    let Command::RetainRepairBase { request, memory_bytes, reclaim_receipts } = parse(&[
        "retain-repair-base", "source", "logs/REPLICA_LOG", "export", &base, "--reclaim-receipts", "--memory-limit", "104857600",
    ]) else { panic!("expected repair base retention") };
    assert_eq!(request.source_storage.to_str(), Some("source"));
    assert_eq!(request.base_storage.to_str(), Some("export"));
    assert_eq!(request.replica_log.to_str(), Some("logs/REPLICA_LOG"));
    assert_eq!(request.base.block.0, 9);
    assert_eq!(request.base.sha256, [0xa1; 32]);
    assert_eq!(memory_bytes, 104857600);
    assert!(reclaim_receipts);
    assert_eq!(request.classic_key_vault, None);
    let environment = |name: &str| (name == "RTDB_CLASSIC_KEY_VAULT").then(|| "/vaults/classic".to_owned());
    let Command::RetainRepairBase { request, .. } = cli::parse(["retain-repair-base", "source", "logs/REPLICA_LOG", "export", &base].map(str::to_owned), &environment).unwrap()
    else { panic!("expected repair base retention") };
    assert_eq!(request.classic_key_vault.as_deref(), Some(std::path::Path::new("/vaults/classic")));
    assert!(cli::parse(["retain-repair-base", "source", "logs/REPLICA_LOG", &base].map(str::to_owned), &|_| None).is_err());
}

#[test]
fn a_retained_base_requires_the_detached_export_and_one_target_receipt() {
    let through = format!("23@{}", "C4".repeat(32));
    let Command::RepairRetainedCheckpoint { request, checkpoint_key_vault, .. } = parse(&[
        "repair-checkpoint", "source", "logs/REPLICA_LOG", "repairs", &through,
        "--retained-base", "--base-storage", "export", "--checkpoint-key-vault", "original-vault",
    ]) else { panic!("expected repair from retained base") };
    assert_eq!(request.base_storage.to_str(), Some("export"));
    assert_eq!(checkpoint_key_vault.as_deref(), Some(std::path::Path::new("original-vault")));
    assert_eq!(request.through.block.0, 23);
    let detached_required = ["repair-checkpoint", "source", "logs/REPLICA_LOG", "repairs", &through, "--retained-base"];
    assert!(cli::parse(detached_required.map(str::to_owned), &|_| None).is_err());
    let only_one_receipt = ["repair-checkpoint", "source", "logs/REPLICA_LOG", "repairs", &through, &through,
        "--retained-base", "--base-storage", "export"];
    assert!(cli::parse(only_one_receipt.map(str::to_owned), &|_| None).is_err());
}

#[test]
fn checkpoint_repair_requires_exact_publication_receipts() {
    let base = format!("9@{}", "a1".repeat(32));
    let through = format!("23@{}", "C4".repeat(32));
    let Command::RepairCheckpoint { request, memory_bytes, checkpoint_key_vault } = parse(&[
        "repair-checkpoint", "source", "logs/REPLICA_LOG", "repairs", &base, &through,
        "--base-storage", "known-good", "--memory-limit", "104857600",
    ]) else { panic!("expected checkpoint repair") };
    assert_eq!(request.source_storage.to_str(), Some("source"));
    assert_eq!(request.base_storage.unwrap().to_str(), Some("known-good"));
    assert_eq!(request.replica_log.to_str(), Some("logs/REPLICA_LOG"));
    assert_eq!(request.output_parent.to_str(), Some("repairs"));
    assert_eq!(request.base.block.0, 9);
    assert_eq!(request.base.sha256, [0xa1; 32]);
    assert_eq!(request.through.block.0, 23);
    assert_eq!(request.through.sha256, [0xc4; 32]);
    assert_eq!(memory_bytes, 104857600);
    assert!(checkpoint_key_vault.is_none());
    for invalid in ["9", "9@abcd", &format!("-9@{}", "ab".repeat(32)), &format!("9@{}", "zz".repeat(32))] {
        let arguments = ["repair-checkpoint", "source", "logs/REPLICA_LOG", "repairs", invalid, &through];
        assert!(cli::parse(arguments.map(str::to_owned), &|_| None).is_err());
    }
}

#[test]
fn inspection_commands_require_a_path_and_accept_memory_limits() {
    for command in ["read-arrow-file", "read-arrow-stream-file", "read-hash-trie-file", "read-table-block-file", "read-block-file"] {
        let Command::Inspect { path, allocation_limit_bytes, .. } = parse(&[command, "input.bin", "--memory-limit", "1000000"]) else { panic!("expected inspection") };
        assert_eq!(path.to_str(), Some("input.bin"));
        assert_eq!(allocation_limit_bytes, 1000000);
        assert!(cli::parse([command.to_owned()], &|_| None).is_err());
    }
}
