use std::{collections::BTreeMap,sync::Arc};
use rtdb_columnar::{MemoryBudget,read_value};
use rtdb_index::PartitionState;
use rtdb_plan::{Expr,Statement};
use rtdb_query::{CatalogSnapshot,DatabaseSnapshot,PreparedQuery,QueryContext,QueryOptions};
use rtdb_sql::{PrepareOptions,prepare};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_trie::TrieConfig;
use rtdb_types::{LogicalType as L,Result,ScalarType as S,TimeUnit as U,Timestamp,Value as V};
use tokio_util::sync::CancellationToken;

fn context(zone: &str) -> Arc<QueryContext> {
    let partition = PartitionState::empty(TrieConfig::new(32,32).unwrap()).unwrap();
    let database = DatabaseSnapshot { partitions: vec![partition.value().snapshot()],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    QueryContext::new(catalog,MemoryBudget::new(1 << 27),Timestamp(0),zone,QueryOptions::default(),&CancellationToken::new()).unwrap()
}
async fn query(source: &str,zone: &str) -> Result<Vec<Vec<V>>> {
    let context = context(zone);
    let options = PrepareOptions { default_timezone: zone.into(),..Default::default() };
    let Statement::Query(relation) = prepare(source,&context.catalog,&options)?.statement else { panic!() };
    let mut cursor = PreparedQuery::new(relation)?.execute(context,vec![]).await?;let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() { rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field,array)|read_value(field,array.as_ref(),row)).collect::<Result<_>>()?); }
    }
    Ok(rows)
}

#[tokio::test]
async fn sql_extract_and_trunc_normalize_fields_and_execute_real_projection() {
    assert_eq!(query("SELECT EXTRACT(WEEK FROM DATE '2021-01-01'),EXTRACT(DOW FROM DATE '2021-01-01') AS test_output_2,DATE_TRUNC(month,DATE '2024-03-19'),DATE_TRUNC('year',DATE '2024-03-19') AS test_output_4","UTC").await.unwrap(),[vec![V::I32(53),V::I32(5),V::DateDay(19783),V::DateDay(19723)]]);
    assert_eq!(query("SELECT AGE(DATE '2024-01-01',DATE '2023-01-01')","UTC").await.unwrap(),[vec![V::IntervalMonthDayMicro { months: 12,days: 0,nanoseconds: 0 }]]);
    assert_eq!(query("SELECT DATE_BIN(INTERVAL '15' MINUTE,TIMESTAMP '1969-12-31T23:50:00Z')","UTC").await.unwrap(),[vec![V::Timestamp { value: 0,unit: U::Microsecond,timezone: Some("UTC".into()) }]]);
}

#[tokio::test]
async fn sql_range_bins_lower_period_bounds_and_support_lazy_cardinality() {
    assert_eq!(query("SELECT CARDINALITY(RANGE_BINS(INTERVAL '15' MINUTE,PERIOD(TIMESTAMP '2020-01-01T00:13Z',TIMESTAMP '2020-01-01T00:38Z')))","UTC").await.unwrap(),[vec![V::I32(3)]]);
    assert_eq!(query("SELECT CARDINALITY(RANGE_BINS(INTERVAL '1' SECOND,PERIOD(TIMESTAMP '2020-01-01T00:00Z',NULL)))","UTC").await.unwrap(),[vec![V::Null]]);
}

#[tokio::test]
async fn sql_clock_aliases_capture_zone_metadata_at_command_preparation() {
    let rows = query("SELECT CURRENT_DATE,LOCAL_DATE,CURRENT_TIME(0),LOCAL_TIME(0),CURRENT_TIMESTAMP(0),LOCAL_TIMESTAMP(0),CURRENT_TIMEZONE","+05:45").await.unwrap();
    assert_eq!(rows,[vec![V::DateDay(0),V::DateDay(0),V::Time { value: 0,unit: U::Second,bit_width: 32 },V::Time { value: 20700,unit: U::Second,bit_width: 32 },V::Timestamp { value: 0,unit: U::Second,timezone: Some("+05:45".into()) },V::Timestamp { value: 20700,unit: U::Second,timezone: None },V::Utf8("+05:45".into())]]);
    let context = context("UTC");
    for zone in ["UTC","Europe/Paris"] {
        let Statement::Query(relation) = prepare("SELECT NOW() AS n",&context.catalog,&PrepareOptions { default_timezone: zone.into(),..Default::default() }).unwrap().statement else { panic!() };
        assert_eq!(relation.schema().fields()[0].data_type,L::Scalar(S::Timestamp { unit: U::Microsecond,timezone: Some(zone.into()) }));
    }
}

#[test]
fn sql_temporal_admission_rejects_dynamic_fields_zones_and_nonliteral_strides() {
    let context = context("UTC");
    for expression in ["DATE_TRUNC('DAY',TIMESTAMP '2020-01-01', 'UTC')","DATE_TRUNC(1,DATE '2020-01-01')","EXTRACT(EPOCH FROM DATE '2020-01-01')","DATE_BIN(15,TIMESTAMP '2020-01-01T00:00Z')","CURRENT_DATE(3)","CURRENT_TIMESTAMP(1+1)","RANGE_BINS(INTERVAL '1' DAY)"] {
        assert!(prepare(&format!("SELECT {expression}"),&context.catalog,&PrepareOptions::default()).is_err(),"{expression}");
    }
    let Statement::Query(relation) = prepare("SELECT DATE_TRUNC('MoNtH',DATE '2020-03-10')",&context.catalog,&PrepareOptions::default()).unwrap().statement else { panic!() };
    let mut found = false;
    for expression in relation.expressions() { expression.visit(&mut |expression| if let Expr::Call { name,args,.. } = expression { if name == "date_trunc" { assert_eq!(args[0],Expr::Literal(V::Utf8("MONTH".into())));found = true; } }); }
    assert!(found);
}

#[test]
fn query_setting_syntax_retains_temporal_functions_as_unevaluated_values() {
    let context = context("UTC");
    for expression in ["CURRENT_TIMESTAMP(9)","LOCAL_TIME(3)","CURRENT_TIMEZONE()","DATE_TRUNC(month,DATE '2024-03-19')","DATE_BIN(INTERVAL '1' DAY,TIMESTAMP '2024-01-01T00:00Z')","RANGE_BINS(INTERVAL '1' DAY,PERIOD(TIMESTAMP '2024-01-01T00:00Z',TIMESTAMP '2024-01-02T00:00Z'))","EXTRACT(YEAR FROM $1)"] {
        let prepared = prepare(&format!("SETTING CLOCK_TIME={expression} SELECT 1"),&context.catalog,&PrepareOptions::default()).unwrap();
        assert!(matches!(prepared.settings.clock_time,Some(rtdb_plan::SettingValue::Unevaluated { .. })),"{expression}");
    }
}

#[test]
fn temporal_sql_grammar_is_identical_in_query_and_setting_expressions() {
    let context = context("UTC");
    for expression in ["CURRENT_TIMESTAMP(-1)","CURRENT_TIMESTAMP(+1)","CURRENT_TIMESTAMP(NULL)","CURRENT_TIMESTAMP(1+1)","CURRENT_TIMESTAMP(1.0)","CURRENT_TIMESTAMP($1)","CURRENT_TIME()","LOCAL_TIME()","LOCAL_TIMESTAMP()","DATE_TRUNC(\"month\",NULL)","DATE_TRUNC(DOW,NULL)","DATE_TRUNC(day,NULL,NULL)","DATE_TRUNC(day,NULL,$1)","EXTRACT(MILLISECOND FROM NULL)","EXTRACT(CENTURY FROM NULL)","EXTRACT(YEAR,NULL)","EXTRACT('YEAR' FROM NULL)","EXTRACT(\"YEAR\" FROM NULL)"] {
        for source in [format!("SELECT {expression}"),format!("SETTING SNAPSHOT_TIME={expression} SELECT 1")] {
            assert!(prepare(&source,&context.catalog,&PrepareOptions::default()).is_err(),"{source}");
        }
    }
    for expression in ["NOW()","CURRENT_TIMESTAMP()","CURRENT_TIME","LOCAL_TIME(9)","LOCAL_TIMESTAMP(0)","DATE_TRUNC('DOW',NULL)","DATE_TRUNC(NANOSECOND,NULL)","DATE_TRUNC(day,NULL,'UTC')","EXTRACT(EPOCH FROM NULL)"] {
        for source in [format!("SELECT {expression}"),format!("SETTING SNAPSHOT_TIME={expression} SELECT 1")] {
            prepare(&source,&context.catalog,&PrepareOptions::default()).unwrap();
        }
    }
}

#[tokio::test(start_paused = true)]
async fn sql_extract_does_not_demand_a_monomorphic_null_sleep_payload() {
    let start = tokio::time::Instant::now();
    assert_eq!(query("SELECT EXTRACT(YEAR FROM PG_SLEEP(1))","UTC").await.unwrap(),[vec![V::Null]]);
    assert_eq!(tokio::time::Instant::now(),start);
}
