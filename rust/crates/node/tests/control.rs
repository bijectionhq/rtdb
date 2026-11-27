use rtdb_columnar::MemoryBudget;
use rtdb_node::{AccessSettings, OptionUpdate, control_schema, transaction_settings};
use rtdb_plan::{ControlStatement, Schema};
use rtdb_types::{LogicalType, ScalarType, Timestamp, Value};

fn parse(source: &str) -> ControlStatement { rtdb_sql::parse_control(source, &Default::default()).unwrap().unwrap() }

#[tokio::test]
async fn retained_control_cast_uses_each_execution_timezone() {
    let ControlStatement::Begin(options) = parse("BEGIN READ WRITE WITH (METADATA CAST('1970-01-01Z' AS TIMESTAMPTZ))") else { panic!() };
    let Some(rtdb_plan::ControlAccessOptions::ReadWrite(options)) = &options.access else { panic!() };
    let Some(rtdb_plan::ControlValue::Expression(expression)) = &options.metadata else { panic!() };
    for zone in ["UTC","Europe/Paris"] {
        let value = rtdb_expr::evaluate_async(expression,&Schema::default(),&[],&rtdb_expr::EvalContext { timezone: zone,..Default::default() }).await.unwrap();
        assert_eq!(value,Value::Timestamp { value: 0,unit: rtdb_types::TimeUnit::Microsecond,timezone: Some(zone.into()) });
    }
}

#[tokio::test]
async fn xtdb_transaction_options_resolve_timezone_before_system_time_even_when_listed_last() {
    let ControlStatement::Begin(options) = parse("BEGIN READ WRITE WITH (SYSTEM_TIME TIMESTAMP '2024-01-01 00:00:00', TIMEZONE 'America/New_York')") else { panic!() };
    let memory = MemoryBudget::new(1_000_000);
    let allocation = rtdb_expr::EvaluationScope::new(&memory).unwrap();
    let settings = transaction_settings(&options, &[], "UTC", "UTC", Timestamp(0), &memory, &allocation, None).await.unwrap();
    let Some(AccessSettings::ReadWrite(settings)) = settings.access else { panic!() };
    assert!(matches!(settings.system_time, OptionUpdate::Set(Timestamp(1_704_085_200_000_000))));
}

#[tokio::test]
async fn write_metadata_resolves_timezone_even_when_listed_last() {
    let ControlStatement::Begin(options) = parse("BEGIN READ WRITE WITH (METADATA {sample: CAST(TIMESTAMP '2024-01-01 00:00:00' AS TIMESTAMPTZ)}, TIMEZONE 'America/New_York')") else { panic!() };
    let memory = MemoryBudget::new(1_000_000);
    let allocation = rtdb_expr::EvaluationScope::new(&memory).unwrap();
    let settings = transaction_settings(&options, &[], "UTC", "UTC", Timestamp(0), &memory, &allocation, None).await.unwrap();
    let Some(AccessSettings::ReadWrite(settings)) = settings.access else { panic!() };
    let OptionUpdate::Set(metadata) = settings.user_metadata else { panic!() };
    let Value::Struct(fields) = metadata.value() else { panic!() };
    assert_eq!(
        rtdb_expr::timestamp_micros(&fields["sample"], "America/New_York").unwrap(),
        Timestamp(1_704_085_200_000_000),
    );
}

#[tokio::test]
async fn xtdb_transaction_default_timezone_uses_session_value_and_null_clears_options() {
    let ControlStatement::SetTransaction(options) = parse("SET TRANSACTION READ ONLY, CLOCK_TIME TIMESTAMP '2024-01-01 00:00:00', TIMEZONE DEFAULT, SNAPSHOT_TOKEN ?, AWAIT_TOKEN DEFAULT") else { panic!() };
    let memory = MemoryBudget::new(1_000_000);
    let allocation = rtdb_expr::EvaluationScope::new(&memory).unwrap();
    let settings = transaction_settings(&options, &[Value::Null], "America/New_York", "Europe/Paris", Timestamp(0), &memory, &allocation, None).await.unwrap();
    assert!(matches!(settings.timezone, OptionUpdate::Clear));
    let Some(AccessSettings::ReadOnly(settings)) = settings.access else { panic!() };
    assert!(matches!(settings.clock_time, OptionUpdate::Set(value) if value == Timestamp(1_704_063_600_000_000).into()));
    assert!(matches!(settings.snapshot_token, OptionUpdate::Clear));
    assert!(matches!(settings.await_token, OptionUpdate::Clear));
    assert!(matches!(settings.snapshot_time, OptionUpdate::Unchanged));
}

#[tokio::test]
async fn xtdb_transaction_metadata_retains_its_budget_and_async_requires_boolean() {
    let ControlStatement::Begin(options) = parse("BEGIN READ WRITE WITH (METADATA ?)") else { panic!() };
    let memory = MemoryBudget::new(100_000);
    let allocation = rtdb_expr::EvaluationScope::new(&memory).unwrap();
    let settings = transaction_settings(&options, &[Value::Struct([("trace".into(), Value::Utf8("request-1".into()))].into())], "UTC", "UTC", Timestamp(0), &memory, &allocation, None).await.unwrap();
    assert!(memory.used_bytes() > 0);
    drop(settings); drop(allocation); assert_eq!(memory.used_bytes(), 0);
    let ControlStatement::Begin(options) = parse("BEGIN READ WRITE, ASYNC 1") else { panic!() };
    let allocation = rtdb_expr::EvaluationScope::new(&memory).unwrap();
    assert!(transaction_settings(&options, &[], "UTC", "UTC", Timestamp(0), &memory, &allocation, None).await.is_err());
}

#[test]
fn xtdb_show_prepared_schema_does_not_require_a_live_connection() {
    let schema: Schema = control_schema(&parse("SHOW latest_submitted_tx")).unwrap().unwrap();
    assert_eq!(schema.fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), ["tx_id", "system_time", "committed", "error", "await_token"]);
    assert_eq!(schema.fields()[3].data_type, LogicalType::Scalar(ScalarType::Transit).maybe());
    assert!(schema.fields().iter().all(|field| field.data_type.nullable()));
    assert!(control_schema(&parse("COMMIT")).unwrap().is_none());

    let isolation_level = control_schema(&parse("SHOW TRANSACTION ISOLATION LEVEL")).unwrap().unwrap();
    assert_eq!(isolation_level.fields().len(), 1);
    assert_eq!(isolation_level.fields()[0].name, "transaction_isolation");
    assert_eq!(isolation_level.fields()[0].data_type, LogicalType::Scalar(ScalarType::Utf8));

    for variable in ["transaction_isolation", "application_name"] {
        let variable_schema = control_schema(&parse(&format!("SHOW {variable}"))).unwrap().unwrap();
        assert_eq!(variable_schema.fields().len(), 1);
        assert_eq!(variable_schema.fields()[0].name, variable);
        assert_eq!(variable_schema.fields()[0].data_type, LogicalType::Scalar(ScalarType::Utf8).maybe());
    }
}

#[tokio::test]
async fn clock_literal_is_resolved_after_timezone_without_arrow_microsecond_admission() {
    let memory = MemoryBudget::new(1_000_000);
    let allocation = rtdb_expr::EvaluationScope::new(&memory).unwrap();
    let ControlStatement::Begin(options) = parse("BEGIN READ ONLY WITH (CLOCK_TIME +(TIMESTAMP '1970-01-01T01:00:00.000000999'), TIMEZONE '+01:00')") else { panic!() };
    let settings = transaction_settings(&options,&[],"UTC","UTC",Timestamp(0),&memory,&allocation,None).await.unwrap();
    let Some(AccessSettings::ReadOnly(settings)) = settings.access else { panic!() };
    assert!(matches!(settings.clock_time,OptionUpdate::Set(value) if value == rtdb_types::Instant::new(0,999).unwrap()));
    let ControlStatement::Begin(options) = parse("BEGIN READ ONLY WITH (CLOCK_TIME ?)") else { panic!() };
    let parameter = Value::Timestamp { value: -1,unit: rtdb_types::TimeUnit::Nanosecond,timezone: Some("UTC".into()) };
    let settings = transaction_settings(&options,&[parameter],"UTC","UTC",Timestamp(0),&memory,&allocation,None).await.unwrap();
    let Some(AccessSettings::ReadOnly(settings)) = settings.access else { panic!() };
    assert!(matches!(settings.clock_time,OptionUpdate::Set(value) if value == rtdb_types::Instant::new(-1,999_999_999).unwrap()));
}

#[tokio::test]
async fn clock_string_date_fallback_differs_from_a_typed_timestamp_literal() {
    let memory = MemoryBudget::new(1_000_000);
    let allocation = rtdb_expr::EvaluationScope::new(&memory).unwrap();
    for (value,expected) in [("'1970-01-01T12:00:00.000000999'",rtdb_types::Instant::default()),("TIMESTAMP '1970-01-01T12:00:00.000000999'",rtdb_types::Instant::new(43_200,999).unwrap())] {
        let ControlStatement::Begin(options) = parse(&format!("BEGIN READ ONLY WITH (CLOCK_TIME {value})")) else { panic!() };
        let settings = transaction_settings(&options,&[],"UTC","UTC",Timestamp(0),&memory,&allocation,None).await.unwrap();
        let Some(AccessSettings::ReadOnly(settings)) = settings.access else { panic!() };
        assert!(matches!(settings.clock_time,OptionUpdate::Set(value) if value == expected));
    }
}
