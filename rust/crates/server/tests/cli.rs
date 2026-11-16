use std::io::Cursor;
use rtdb_server::cli::{self, Command, NodeMode};

fn parse(arguments: &[&str]) -> Command { cli::parse(arguments.iter().map(|value| (*value).into()), &|_| None).unwrap() }

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
    let Command::CompactorReset { options, database, dry_run } = parse(&["reset-compactor", "analytics", "--dry-run", "-f", "node.yaml"]) else { panic!("expected compactor reset") };
    assert_eq!(database, "analytics");
    assert!(dry_run);
    assert_eq!(options.file.unwrap().to_str(), Some("node.yaml"));
    assert!(cli::parse(["reset-compactor".into()], &|_| None).is_err());
    assert!(cli::parse(["reset-compactor", "one", "two"].map(str::to_owned), &|_| None).is_err());
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
