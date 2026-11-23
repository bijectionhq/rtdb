use std::io::Cursor;
use rtdb_server::cli::{self, Command, NodeMode};

fn parse(arguments: &[&str]) -> Command { cli::parse(arguments.iter().map(|value| (*value).into()), &|_| None).unwrap() }

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
    assert_eq!(actual, serde_json::to_value(rtdb_types::compiled_build_info()).unwrap());
    assert_eq!(actual["tzdb_sha256"].as_str().unwrap().len(), 64);
    assert_ne!(actual["version"], "runtime-only-version");
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
    let Command::RetainRepairBase { request, memory_bytes } = parse(&[
        "retain-repair-base", "source", "logs/REPLICA_LOG", "export", &base, "--memory-limit", "104857600",
    ]) else { panic!("expected repair base retention") };
    assert_eq!(request.source_storage.to_str(), Some("source"));
    assert_eq!(request.base_storage.to_str(), Some("export"));
    assert_eq!(request.replica_log.to_str(), Some("logs/REPLICA_LOG"));
    assert_eq!(request.base.block.0, 9);
    assert_eq!(request.base.sha256, [0xa1; 32]);
    assert_eq!(memory_bytes, 104857600);
    assert!(cli::parse(["retain-repair-base", "source", "logs/REPLICA_LOG", &base].map(str::to_owned), &|_| None).is_err());
}

#[test]
fn a_retained_base_requires_the_detached_export_and_one_target_receipt() {
    let through = format!("23@{}", "C4".repeat(32));
    let Command::RepairRetainedCheckpoint { request, .. } = parse(&[
        "repair-checkpoint", "source", "logs/REPLICA_LOG", "repairs", &through,
        "--retained-base", "--base-storage", "export",
    ]) else { panic!("expected repair from retained base") };
    assert_eq!(request.base_storage.to_str(), Some("export"));
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
    let Command::RepairCheckpoint { request, memory_bytes } = parse(&[
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
