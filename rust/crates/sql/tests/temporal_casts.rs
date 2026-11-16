use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType, Result, ScalarType, TimeUnit as U, Timestamp, Value as V};
use tokio_util::sync::CancellationToken;

async fn query(source: &str, timezone: &str) -> Result<(LogicalType,V)> {
    let database = DatabaseSnapshot { partitions: Vec::new(), storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(),database)]), false)?;
    let context = QueryContext::new(catalog, MemoryBudget::new(1 << 26), Timestamp(0), timezone, QueryOptions::default(), &CancellationToken::new())?;
    let options = PrepareOptions { default_timezone: timezone.into(), ..Default::default() };
    let prepared = PreparedQuery::from_prepared(prepare(source,&context.catalog,&options)?,&context.memory)?;
    let declared = prepared.schema().fields()[0].data_type.clone();
    let mut cursor = prepared.execute(context,Vec::new()).await?;
    let batch = cursor.next_batch().await?.unwrap();
    let value = read_value(batch.batch().schema().field(0),batch.batch().column(0).as_ref(),0)?;
    assert_eq!(cursor.runtime_schema().fields()[0].data_type,declared);
    assert!(cursor.next_batch().await?.is_none());
    Ok((declared,value))
}

#[tokio::test]
async fn temporal_precision_survives_sql_binding_and_arrow_execution() {
    for (source, expected) in [
        ("SELECT CAST('05:34:56.123456789' AS TIME(7))",V::Time { value: 20_096_123_456_700, unit: U::Nanosecond, bit_width: 64 }),
        ("SELECT CAST('PT13M56.123456789S' AS DURATION(7))",V::Duration { value: 836_123_456_700, unit: U::Nanosecond }),
        ("SELECT CAST('1970-01-01T00:00:00.123456789+01:30' AS TIMESTAMP(8))",V::Timestamp { value: 123_456_780, unit: U::Nanosecond, timezone: None }),
        ("SELECT CAST('1970-01-01T00:00:00.123456789+01:30' AS TIMESTAMP(8) WITH TIME ZONE)",V::Timestamp { value: -5_399_876_543_220, unit: U::Nanosecond, timezone: Some("America/New_York".into()) }),
        ("SELECT CAST('1970-01-01T00:00:00Z' AS TIME WITH TIME ZONE)",V::Timestamp { value: 0, unit: U::Microsecond, timezone: Some("America/New_York".into()) }),
        ("XTQL $$(-> (rel [{}] []) (return {:v (cast-tstz \"1970-01-01Z\")}))$$",V::Timestamp { value: 0,unit: U::Microsecond,timezone: Some("America/New_York".into()) }),
    ] { assert_eq!(query(source,"America/New_York").await.unwrap(),(expected.logical_type(),expected)); }
    assert_eq!(query("SELECT CAST('01:02:03' AS TIME(10))","UTC").await.unwrap_err().code,"xtdb.expression/invalid-fractional-precision");
    assert_eq!(query("SELECT CAST(NULL AS TIME(10))","UTC").await.unwrap(),(LogicalType::Null,V::Null));
}

#[tokio::test]
async fn interval_sql_qualifiers_keep_source_dependent_output_types() {
    for (source, expected) in [
        ("SELECT CAST('1-10' AS INTERVAL YEAR TO MONTH)",V::IntervalYearMonth(22)),
        ("SELECT CAST('1 10:10:10.111111111' AS INTERVAL DAY TO SECOND(7))",V::IntervalMonthDayNano { months: 0, days: 1, nanoseconds: 36_610_111_111_100 }),
        ("SELECT CAST('1.123456789' AS INTERVAL SECOND(0))",V::IntervalMonthDayMicro { months: 0, days: 0, nanoseconds: 1_123_456_789 }),
        ("SELECT CAST(3 AS INTERVAL DAY)",V::IntervalMonthDayMicro { months: 0, days: 3, nanoseconds: 0 }),
        ("SELECT CAST(CAST('PT36H10M10.111111S' AS DURATION(6)) AS INTERVAL DAY TO SECOND(4))",V::IntervalMonthDayMicro { months: 0, days: 1, nanoseconds: 43_810_111_100_000 }),
    ] { assert_eq!(query(source,"UTC").await.unwrap(),(expected.logical_type(),expected)); }
    assert_eq!(query("SELECT CAST(3 AS INTERVAL DAY TO SECOND)","UTC").await.unwrap_err().code,"xtdb.expression/attempting-to-cast-int-to-multi-field-interval");
    assert_eq!(query("SELECT CAST('1970-01-01T00:00:00' AS TIMESTAMPTZ)","UTC").await.unwrap_err().code,"xtdb.expression/invalid-temporal-string");
    assert_eq!(query("SELECT CAST('1970-01-01Z' AS TIMESTAMPTZ)","Europe/Paris").await.unwrap().0,LogicalType::Scalar(ScalarType::Timestamp { unit: U::Microsecond, timezone: Some("Europe/Paris".into()) }));
}
