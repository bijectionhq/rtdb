use rtdb_plan::{ControlAccessOptions, ControlCommitMode, ControlStatement, ControlValue, CopyFormat, Expr};
use rtdb_sql::{PrepareOptions, parse_control};
use rtdb_types::{LogicalType, ScalarType, Value};

fn parse(source: &str) -> ControlStatement { parse_control(source, &PrepareOptions::default()).unwrap().unwrap() }

#[test]
fn sql_parser_begin_with_preserves_defaults_parameters_and_access_specific_options() {
    let ControlStatement::Begin(options) = parse("BEGIN READ ONLY WITH (CLOCK_TIME = ?, SNAPSHOT_TOKEN DEFAULT, TIME ZONE 'Europe/Paris', AWAIT_TOKEN $3)") else { panic!() };
    assert!(matches!(options.timezone, Some(ControlValue::Expression(Expr::Literal(Value::Utf8(ref value)))) if value == "Europe/Paris"));
    let Some(ControlAccessOptions::ReadOnly(options)) = options.access else { panic!() };
    assert!(matches!(options.snapshot_token, Some(ControlValue::Default)));
    assert!(matches!(options.clock_time, Some(ControlValue::Expression(Expr::Parameter { index: 0, .. }))));
    assert!(matches!(options.await_token, Some(ControlValue::Expression(Expr::Parameter { index: 2, .. }))));
    assert!(options.snapshot_time.is_none());
    assert!(parse_control("BEGIN READ ONLY, SYSTEM_TIME '2024-01-01'", &Default::default()).is_err());
    assert!(parse_control("BEGIN SYSTEM_TIME '2024-01-01'", &Default::default()).is_err());
}

#[test]
fn bijection_at_is_a_session_control() {
    let ControlStatement::SetParameter { name, .. } = parse("SET bijection.at = TIMESTAMP '2027-01-15T00:00:00Z'") else { panic!() };
    assert_eq!(name, "bijection.at");
    assert_eq!(parse("SET bijection.at = NULL"), ControlStatement::SetParameter { name, value: Expr::Literal(Value::Null) });
}

#[test]
fn sql_parser_start_transaction_and_set_transaction_share_characteristics() {
    let ControlStatement::Begin(begin) = parse("START TRANSACTION SYSTEM_TIME ?, READ WRITE, ASYNC TRUE, TIMEZONE DEFAULT") else { panic!() };
    let ControlStatement::SetTransaction(set) = parse("SET TRANSACTION SYSTEM_TIME ?, READ WRITE, ASYNC TRUE, TIMEZONE DEFAULT") else { panic!() };
    assert_eq!(begin, set);
    assert_eq!(parse("COMMIT ASYNC"), ControlStatement::Commit(Some(ControlCommitMode::Async)));
    assert_eq!(parse("COMMIT SYNC"), ControlStatement::Commit(Some(ControlCommitMode::Sync)));
    assert!(parse_control("SET TRANSACTION READ WRITE, ASYNC ?", &Default::default()).is_err());
}

#[test]
fn sql_parser_prepare_retains_original_inner_sql_and_execute_parameter_positions() {
    assert_eq!(parse("PREPARE \"X\" AS SELECT 'a;b', ?"), ControlStatement::Prepare { name: "X".into(), source: "SELECT 'a;b', ?".into() });
    let ControlStatement::Execute { name, arguments } = parse("EXECUTE \"X\" (1 + 2, $4)") else { panic!() };
    assert_eq!(name, "X"); assert_eq!(arguments.len(), 2);
    assert!(matches!(arguments[1], Expr::Parameter { index: 3, .. }));
    assert!(parse_control("EXECUTE x()", &Default::default()).is_err());
    assert!(parse_control("SHOW timezone; ROLLBACK", &Default::default()).is_err());
}

#[test]
fn sql_parser_copy_attach_and_show_are_shared_typed_controls() {
    let ControlStatement::CopyIn { table, format } = parse("COPY foo.bar FROM STDIN WITH (FORMAT = 'arrow-stream')") else { panic!() };
    assert_eq!(table.iter().map(|name| name.normalized()).collect::<Vec<_>>(), ["foo", "bar"]);
    assert_eq!(format, Some(CopyFormat::ArrowStream));
    assert_eq!(parse("ATTACH DATABASE \"MyDb\" WITH 'log: !InMemory'"), ControlStatement::AttachDatabase { name: "MyDb".into(), configuration_yaml: Some("log: !InMemory".into()), source: "ATTACH DATABASE \"MyDb\" WITH 'log: !InMemory'".into() });
    assert!(parse("SHOW TIME ZONE").requires_read_transaction());
    assert!(!parse("SHOW latest_submitted_tx").requires_read_transaction());
    assert_eq!(parse_control("SELECT 1", &Default::default()).unwrap(), None);
}

#[test]
fn copy_and_attach_use_control_character_string_bodies() {
    for literal in ["'arrow-stream'", "$$arrow-stream$$", "$fmt$arrow-stream$fmt$", "E'arrow-stream'", "e'arrow-stream'"] {
        let ControlStatement::CopyIn { format, .. } = parse(&format!("COPY docs FROM STDIN WITH (FORMAT {literal})")) else { panic!() };
        assert_eq!(format, Some(CopyFormat::ArrowStream));
    }
    for (literal, expected) in [
        ("'it''s yaml'", "it's yaml"),
        ("$$key: 'value'; -- body\nnext: x$$", "key: 'value'; -- body\nnext: x"),
        ("$cfg$é: 値\n$other$$cfg$", "é: 値\n$other$"),
        (r"E'key: \n\t\x41\u0042'", r"key: \n\t\x41\u0042"),
        (r"e'it''s \'quoted\''", r"it''s \'quoted\'"),
        ("E'é\nline'", "é\nline"),
    ] {
        let source = format!("/* é\n */ ATTACH DATABASE db WITH {literal}");
        assert_eq!(parse(&source), ControlStatement::AttachDatabase { name: "db".into(), configuration_yaml: Some(expected.into()), source: source.clone() });
    }
    let ControlStatement::CopyIn { format, .. } = parse(r"COPY docs FROM STDIN WITH (FORMAT E'arrow\x2dstream')") else { panic!() };
    assert_eq!(format, None);
}

#[test]
fn copy_classification_uses_the_first_case_sensitive_format_and_retains_missing_formats() {
    for options in [" WITH ()", " WITH (,,,)", " WITH (FORMAT 'unknown')", " WITH (FORMAT 'ARROW-STREAM')", " WITH (FORMAT 'unknown', FORMAT 'arrow-stream')"] {
        let ControlStatement::CopyIn { format, .. } = parse(&format!("COPY docs FROM STDIN{options}")) else { panic!() };
        assert_eq!(format, None, "{options}");
    }
    for options in ["FORMAT 'arrow-file', FORMAT 'arrow-stream'", ", FORMAT 'arrow-file',, FORMAT 'unknown',", "FORMAT $fmt$arrow-file$fmt$, FORMAT E'ARROW-STREAM'"] {
        let ControlStatement::CopyIn { format, .. } = parse(&format!("COPY docs FROM STDIN WITH ({options})")) else { panic!() };
        assert_eq!(format, Some(CopyFormat::ArrowFile), "{options}");
    }
    for options in ["FORMAT 'arrow-stream', FORMAT 1", "FORMAT 'arrow-stream', UNKNOWN 'x'", "FORMAT 'arrow-stream' FORMAT 'arrow-file'"] {
        assert!(parse_control(&format!("COPY docs FROM STDIN WITH ({options})"), &PrepareOptions::default()).is_err(), "{options}");
    }
    assert!(parse_control("COPY docs FROM STDIN", &PrepareOptions::default()).is_err());
}

#[test]
fn later_copy_format_bodies_do_not_replace_or_redecode_the_first_option() {
    for first in ["arrow-file", "unknown"] {
        let source = format!("COPY docs FROM STDIN WITH (FORMAT '{first}'{})", r", FORMAT E'\x41'".repeat(16_384));
        let ControlStatement::CopyIn { format, .. } = parse(&source) else { panic!() };
        assert_eq!(format, if first == "arrow-file" { Some(CopyFormat::ArrowFile) } else { None });
    }
}

#[test]
fn sql_parser_session_literals_and_typed_control_parameters() {
    assert_eq!(parse("SET standard_conforming_strings TO ON"), ControlStatement::SetParameter { name: "standard_conforming_strings".into(), value: Expr::Literal(Value::Bool(true)) });
    assert!(parse_control("SET application_name = ?", &Default::default()).is_err());
    let options = PrepareOptions { parameter_types: [(0, LogicalType::Scalar(ScalarType::Utf8))].into(), ..Default::default() };
    let Some(ControlStatement::SetTimeZone(Expr::Parameter { index: 0, data_type })) = parse_control("SET TIME ZONE ?", &options).unwrap() else { panic!() };
    assert_eq!(data_type, LogicalType::Scalar(ScalarType::Utf8));
}

#[test]
fn control_names_follow_the_reference_identifier_rule() {
    for source in ["SHOW ALL", "SHOW where", "SET ALL = 1", "SET SESSION AWAIT_TOKEN = 'x'", "SET ROLE ALL", "DETACH DATABASE from", "COPY order FROM STDIN WITH ()", "SET x = X'00'"] {
        let error = parse_control(source, &PrepareOptions::default()).unwrap_err();
        assert_eq!(error.code, "xtdb/sql-error", "{source}");
    }
    for (source, name) in [("SHOW \"all\"", "all"), ("SHOW TRANSACTION ISOLATION LEVEL", "transaction_isolation"), ("SHOW AWAIT_TOKEN", "await_token"),
        ("SHOW snapshot_token", "snapshot_token"), ("SHOW CLOCK_TIME", "clock_time"), ("SHOW TIME ZONE", "timezone"), ("SHOW database", "database")] {
        assert_eq!(parse(source), ControlStatement::Show { name: name.into() }, "{source}");
    }
    assert_eq!(parse("SET ROLE NONE"), ControlStatement::SetRole);
    assert!(matches!(parse("SET AWAIT_TOKEN = 'x'"), ControlStatement::SetAwaitToken(_)));
}
