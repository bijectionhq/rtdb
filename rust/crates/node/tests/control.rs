use rtdb_columnar::MemoryBudget;
use rtdb_node::{AccessSettings, OptionUpdate, control_schema, transaction_settings};
use rtdb_plan::{ControlStatement, Schema};
use rtdb_types::{LogicalType, ScalarType, Timestamp, Value};

fn parse(source: &str) -> ControlStatement { rtdb_sql::parse_control(source, &Default::default()).unwrap().unwrap() }

#[test]
fn xtdb_transaction_options_resolve_timezone_before_system_time_even_when_listed_last() {
    let ControlStatement::Begin(options) = parse("BEGIN READ WRITE WITH (SYSTEM_TIME TIMESTAMP '2024-01-01 00:00:00', TIMEZONE 'America/New_York')") else { panic!() };
    let settings = transaction_settings(&options, &[], "UTC", "UTC", Timestamp(0), &MemoryBudget::new(1_000_000)).unwrap();
    let Some(AccessSettings::ReadWrite(settings)) = settings.access else { panic!() };
    assert!(matches!(settings.system_time, OptionUpdate::Set(Timestamp(1_704_085_200_000_000))));
}

#[test]
fn xtdb_transaction_default_timezone_uses_session_value_and_null_clears_options() {
    let ControlStatement::SetTransaction(options) = parse("SET TRANSACTION READ ONLY, CLOCK_TIME TIMESTAMP '2024-01-01 00:00:00', TIMEZONE DEFAULT, SNAPSHOT_TOKEN ?, AWAIT_TOKEN DEFAULT") else { panic!() };
    let settings = transaction_settings(&options, &[Value::Null], "America/New_York", "Europe/Paris", Timestamp(0), &MemoryBudget::new(1_000_000)).unwrap();
    assert!(matches!(settings.timezone, OptionUpdate::Clear));
    let Some(AccessSettings::ReadOnly(settings)) = settings.access else { panic!() };
    assert!(matches!(settings.clock_time, OptionUpdate::Set(Timestamp(1_704_063_600_000_000))));
    assert!(matches!(settings.snapshot_token, OptionUpdate::Clear));
    assert!(matches!(settings.await_token, OptionUpdate::Clear));
    assert!(matches!(settings.snapshot_time, OptionUpdate::Unchanged));
}

#[test]
fn xtdb_transaction_metadata_retains_its_budget_and_async_requires_boolean() {
    let ControlStatement::Begin(options) = parse("BEGIN READ WRITE WITH (METADATA ?)") else { panic!() };
    let memory = MemoryBudget::new(100_000);
    let settings = transaction_settings(&options, &[Value::Struct([("trace".into(), Value::Utf8("request-1".into()))].into())], "UTC", "UTC", Timestamp(0), &memory).unwrap();
    assert!(memory.used_bytes() > 0);
    drop(settings); assert_eq!(memory.used_bytes(), 0);
    let ControlStatement::Begin(options) = parse("BEGIN READ WRITE, ASYNC 1") else { panic!() };
    assert!(transaction_settings(&options, &[], "UTC", "UTC", Timestamp(0), &memory).is_err());
}

#[test]
fn xtdb_show_prepared_schema_does_not_require_a_live_connection() {
    let schema: Schema = control_schema(&parse("SHOW latest_submitted_tx")).unwrap().unwrap();
    assert_eq!(schema.fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), ["tx_id", "system_time", "committed", "error", "await_token"]);
    assert_eq!(schema.fields()[3].data_type, LogicalType::Scalar(ScalarType::Transit).maybe());
    assert!(schema.fields().iter().all(|field| field.data_type.nullable()));
    assert!(control_schema(&parse("COMMIT")).unwrap().is_none());
}
