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
async fn a_date_has_no_time_cast() {
    let error = query("SELECT CAST(DATE '2020-01-05' AS TIME) AS ts", "UTC").await.unwrap_err();
    assert_eq!(error.code, "xtdb.expression/cast-error");
    assert!(error.message.as_deref().unwrap().starts_with("Unsupported cast: "), "{error:?}");
}

#[tokio::test]
async fn explicit_date_casts_and_implicit_timestamp_and_date_comparisons_report_invalid_text() {
    let mut errors = Vec::new();
    for sql in [
        "SELECT CAST('not-a-date' AS DATE)",
        "SELECT DATE '2020-01-01' >= 'not-a-date'",
    ] {
        let error = query(sql, "UTC").await.unwrap_err();
        assert_eq!(error.code, "xtdb.expression/invalid-temporal-string", "{sql}");
        assert_eq!(error.context["s"], "not-a-date", "{sql}");
        assert_eq!(error.context["date-type"], "date", "{sql}");
        errors.push(error);
    }
    assert_eq!(errors[0], errors[1]);
    let timestamp = query("SELECT TIMESTAMP '2020-01-01T00:00Z' >= 'not-a-timestamp'", "UTC").await.unwrap_err();
    assert_eq!(timestamp.code, "xtdb/invalid-date-time");
    assert_eq!(timestamp.context["timestamp"], "not-a-timestamp");
}

#[tokio::test]
async fn malformed_timestamp_casts_report_the_accepted_prefix() {
    for (input, display, index) in [
        ("2021-10-21 12", "2021-10-21T12", 10),
        ("2021-10-21T12:34:", "2021-10-21T12:34:", 16),
        ("2021-10-21T12:34:56.1234567890", "2021-10-21T12:34:56.1234567890", 29),
        ("2021-10-21T12:34:56+01:0", "2021-10-21T12:34:56+01:0", 22),
        ("2021-10-21T12:34:56[Invalid/Zone]", "2021-10-21T12:34:56[Invalid/Zone]", 19),
        ("2021-10-21T12:34Z 😀", "2021-10-21T12:34ZT😀", 17),
    ] {
        for target in ["TIMESTAMP", "TIMESTAMPTZ"] {
            let error = query(&format!("SELECT CAST('{input}' AS {target})"), "UTC").await.unwrap_err();
            assert_eq!(error.code, "xtdb/invalid-date-time");
            assert_eq!(error.message.as_deref(), Some(format!(
                "invalid timestamp: Text '{display}' could not be parsed, unparsed text found at index {index}",
            ).as_str()));
            assert_eq!(error.context["timestamp"], input);
        }
    }
    let error = query("SELECT CAST('2021-1-21' AS TIMESTAMP)", "UTC").await.unwrap_err();
    assert_eq!(error.message.as_deref(), Some("invalid timestamp: Text '2021-1-21' could not be parsed at index 5"));
    let error = query("SELECT CAST('2021-10-21T25:00' AS TIMESTAMP)", "UTC").await.unwrap_err();
    assert_eq!(error.message.as_deref(), Some(
        "invalid timestamp: Text '2021-10-21T25:00' could not be parsed: Invalid value for HourOfDay (valid values 0 - 23): 25",
    ));
}

#[tokio::test]
async fn duration_literals_execute_at_microsecond_precision() {
    for (source, micros) in [
        ("SELECT DURATION 'P1D' as duration", 86_400_000_000),
        ("SELECT DURATION 'PT1H' as duration", 3_600_000_000),
        ("SELECT DURATION 'P1DT2H' as duration", 93_600_000_000),
        ("SELECT DURATION 'P-1DT2H' as duration", -79_200_000_000),
        ("SELECT DURATION 'PT-0.123456789S'", -123_456),
    ] {
        let value = V::Duration { value: micros, unit: U::Microsecond };
        assert_eq!(query(source, "UTC").await.unwrap(), (value.logical_type(), value), "{source}");
    }
}

#[tokio::test]
async fn keyword_literals_accept_one_optional_leading_colon() {
    let (ty, value) = query("SELECT KEYWORD ':example' = KEYWORD 'example'", "UTC").await.unwrap();
    assert_eq!((ty, value), (LogicalType::Scalar(ScalarType::Bool), V::Bool(true)));
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

#[tokio::test]
async fn interval_overflow_uses_the_same_diagnostic_for_arithmetic_and_casts() {
    for sql in [
        "SELECT INTERVAL '1' HOUR * 2147483647",
        "SELECT INTERVAL '1' MONTH * 4294967297",
        "SELECT INTERVAL '1' DAY * 4294967297",
        "SELECT CAST(INTERVAL '200000' DAY AS INTERVAL HOUR)",
        "SELECT CAST('PT3000000H' AS INTERVAL)",
    ] {
        let error = query(sql, "UTC").await.unwrap_err();
        assert_eq!(error.code, "xtdb.expression/overflow-error", "{sql}");
        assert_eq!(error.message.as_deref(), Some("data exception - overflow error"), "{sql}");
    }
}

#[tokio::test]
async fn interval_casts_validate_the_bound_qualifier() {
    for (start, end, message) in [
        ("MINUTE", "HOUR", "Interval end field must have less significance than the start field."),
        ("YEAR", "DAY", "If YEAR specified as the interval start field, MONTH must be the end field."),
        ("MONTH", "SECOND", "MONTH is not permitted as the interval start field."),
    ] {
        let sql = format!("SELECT CAST('11:10' AS INTERVAL {start} TO {end})");
        let error = query(&sql, "UTC").await.unwrap_err();
        assert_eq!(error.code, "xtdb.expression/invalid-interval-units", "{sql}");
        assert_eq!(error.message.as_deref(), Some(message), "{sql}");
        assert_eq!(error.context["start-unit"], start);
        assert_eq!(error.context["end-unit"], end);
    }
}

#[tokio::test]
async fn unqualified_interval_literals_keep_their_prepared_and_vector_types() {
    for (text, expected) in [
        ("P",V::IntervalYearMonth(0)),
        ("PT",V::IntervalYearMonth(0)),
        ("P1Y",V::IntervalYearMonth(12)),
        ("P1Y-2M3D",V::IntervalMonthDayMicro { months: 10,days: 3,nanoseconds: 0 }),
        ("PT5H6M12.912S",V::IntervalMonthDayMicro { months: 0,days: 0,nanoseconds: 18_372_912_000_000 }),
        ("PT5H-6M-12.912S",V::IntervalMonthDayMicro { months: 0,days: 0,nanoseconds: 17_627_088_000_000 }),
        ("P1Y3DT12H52S",V::IntervalMonthDayMicro { months: 12,days: 3,nanoseconds: 43_252_000_000_000 }),
        ("P1Y10M3DT12H52S",V::IntervalMonthDayMicro { months: 22,days: 3,nanoseconds: 43_252_000_000_000 }),
        ("P1W",V::IntervalMonthDayMicro { months: 0,days: 7,nanoseconds: 0 }),
        ("-P1Y2DT3S",V::IntervalMonthDayMicro { months: -12,days: -2,nanoseconds: -3_000_000_000 }),
        ("PT0.000000001S",V::IntervalMonthDayNano { months: 0,days: 0,nanoseconds: 1 }),
        ("PT-0.000000001S",V::IntervalMonthDayNano { months: 0,days: 0,nanoseconds: -1 }),
        ("P0D",V::IntervalYearMonth(0)),
        ("P1DT0.000001S",V::IntervalMonthDayMicro { months: 0,days: 1,nanoseconds: 1000 }),
        ("1 year 2 months",V::IntervalYearMonth(14)),
        ("-30 day",V::IntervalMonthDayMicro { months: 0,days: -30,nanoseconds: 0 }),
        ("30 day",V::IntervalMonthDayMicro { months: 0,days: 30,nanoseconds: 0 }),
        ("prefix 2 days suffix",V::IntervalMonthDayMicro { months: 0,days: 2,nanoseconds: 0 }),
        ("2days",V::IntervalMonthDayMicro { months: 0,days: 2,nanoseconds: 0 }),
        ("PT0.S",V::IntervalYearMonth(0)),
        ("-P-1Y-2DT-3S",V::IntervalMonthDayMicro { months: 12,days: 2,nanoseconds: 3_000_000_000 }),
        ("P-2147483648D",V::IntervalMonthDayMicro { months: 0,days: i32::MIN,nanoseconds: 0 }),
        ("P306783378W",V::IntervalMonthDayMicro { months: 0,days: 2_147_483_646,nanoseconds: 0 }),
        ("1 day -1 day",V::IntervalYearMonth(0)),
        ("PT-0.S",V::IntervalYearMonth(0)),
        ("P1Y-12M",V::IntervalYearMonth(0)),
        ("P1DT",V::IntervalMonthDayMicro { months: 0,days: 1,nanoseconds: 0 }),
        ("1 SECOND",V::IntervalMonthDayMicro { months: 0,days: 0,nanoseconds: 1_000_000_000 }),
        ("0.5 second",V::IntervalMonthDayMicro { months: 0,days: 0,nanoseconds: 5_000_000_000 }),
        ("year1day",V::IntervalMonthDayMicro { months: 0,days: 1,nanoseconds: 0 }),
        ("1\u{b}day",V::IntervalMonthDayMicro { months: 0,days: 1,nanoseconds: 0 }),
    ] {
        assert_eq!(query(&format!("SELECT INTERVAL '{text}'"),"UTC").await.unwrap(),(expected.logical_type(),expected),"{text}");
    }
    assert_eq!(query("SELECT INTERVAL - 'P1D'","UTC").await.unwrap(),(LogicalType::Scalar(ScalarType::IntervalMonthDayMicro),V::IntervalMonthDayMicro { months: 0,days: -1,nanoseconds: 0 }));
}

#[tokio::test]
async fn range_accepts_the_original_unqualified_interval_stride() {
    let timestamps = [1_577_836_800_000_000,1_577_923_200_000_000,1_578_009_600_000_000].into_iter()
        .map(|value| V::Timestamp { value,unit: U::Microsecond,timezone: None }).collect();
    let expected = V::list(timestamps);
    assert_eq!(query("SELECT RANGE(DATE '2020-01-01',DATE '2020-01-04',INTERVAL 'P1D')","UTC").await.unwrap(),(expected.logical_type(),expected));
}
