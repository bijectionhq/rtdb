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
    assert_eq!(parse("ATTACH DATABASE \"MyDb\" WITH 'log: !InMemory'"), ControlStatement::AttachDatabase { name: "MyDb".into(), configuration_yaml: Some("log: !InMemory".into()) });
    assert!(parse("SHOW TIME ZONE").requires_read_transaction());
    assert!(!parse("SHOW latest_submitted_tx").requires_read_transaction());
    assert_eq!(parse_control("SELECT 1", &Default::default()).unwrap(), None);
}

#[test]
fn sql_parser_session_literals_and_typed_control_parameters() {
    assert_eq!(parse("SET standard_conforming_strings TO ON"), ControlStatement::SetParameter { name: "standard_conforming_strings".into(), value: Expr::Literal(Value::Bool(true)) });
    assert!(parse_control("SET application_name = ?", &Default::default()).is_err());
    let options = PrepareOptions { parameter_types: [(0, LogicalType::Scalar(ScalarType::Utf8))].into(), ..Default::default() };
    let Some(ControlStatement::SetTimeZone(Expr::Parameter { index: 0, data_type })) = parse_control("SET TIME ZONE ?", &options).unwrap() else { panic!() };
    assert_eq!(data_type, LogicalType::Scalar(ScalarType::Utf8));
}
