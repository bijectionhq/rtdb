use rtdb_types::StructFields;
use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_index::{PartitionState, TransactionBuffer};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_trie::TrieConfig;
use rtdb_types::{Interval, Result, TableRef, TimeBasis, TimeUnit, Timestamp, TransactionKey, TxId, Value as V};
use tokio_util::sync::CancellationToken;

fn timestamp(value: i64) -> V { V::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) } }
fn basis(value: i64) -> TimeBasis { TimeBasis { databases: BTreeMap::from([("xtdb".into(), vec![Some(Timestamp(value).into())])]) } }

fn context() -> Arc<QueryContext> {
    let memory = MemoryBudget::new(1 << 27);
    let index_memory = MemoryBudget::new(1 << 27);
    let state = PartitionState::empty(TrieConfig::new(32, 32).unwrap()).unwrap();
    for (id, at, value) in [(0, 100, "old"), (1, 200, "new")] {
        let mut tx = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(id), system_time: Timestamp(at) }, Arc::clone(&index_memory)).unwrap();
        tx.put(TableRef::new("public", "foo"), StructFields::from([("_id".into(), V::I64(1)), ("v".into(), V::Utf8(value.into()))]), Interval::new(Timestamp(10), Timestamp(30)).unwrap()).unwrap();
        state.apply(&tx.stage().unwrap()).unwrap();
    }
    let database = DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, memory, Timestamp(25), "UTC", QueryOptions { batch_rows: 1, ..Default::default() }, &CancellationToken::new()).unwrap()
}

async fn query(context: Arc<QueryContext>, sql: &str, parameters: Vec<V>) -> Result<Vec<Vec<V>>> {
    let options = PrepareOptions { parameter_types: parameters.iter().enumerate().map(|(index, value)| (index, value.logical_type())).collect(), ..Default::default() };
    let prepared = prepare(sql, &context.catalog, &options)?;
    let baseline = context.memory.used_bytes();
    let mut cursor = PreparedQuery::from_prepared(prepared, &context.memory)?.execute(Arc::clone(&context), parameters).await?;
    let mut rows = Vec::new();
    while let Some(page) = cursor.next_batch().await? {
        for row in 0..page.batch().num_rows() {
            rows.push(page.batch().schema().fields().iter().zip(page.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?);
        }
    }
    drop(cursor);
    assert_eq!(context.memory.used_bytes(), baseline);
    Ok(rows)
}

#[tokio::test]
async fn defaults_reach_nested_scans_and_explicit_extents_override_each_dimension() {
    let sql = "SETTING DEFAULT VALID_TIME TO ALL, DEFAULT SYSTEM_TIME AS OF TIMESTAMP '1970-01-01T00:00:00.000150Z' SELECT a.v,b.v AS test_output_2 FROM foo a CROSS JOIN (SELECT v FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '1970-01-01T00:00:00.000200Z') b";
    assert_eq!(query(context(), sql, vec![]).await.unwrap(), [vec![V::Utf8("old".into()), V::Utf8("new".into())]]);
    assert!(query(context(), "SETTING DEFAULT VALID_TIME AS OF TIMESTAMP '1970-01-01T00:00:00.000050Z' SELECT * FROM foo", vec![]).await.unwrap().is_empty());
    assert_eq!(query(context(), "SETTING DEFAULT VALID_TIME AS OF TIMESTAMP '1970-01-01T00:00:00.000050Z' SELECT v FROM foo FOR ALL VALID_TIME", vec![]).await.unwrap(), [vec![V::Utf8("new".into())]]);
    for extent in ["FROM TIMESTAMP '1970-01-01T00:00:00.000015Z' TO TIMESTAMP '1970-01-01T00:00:00.000020Z'", "BETWEEN TIMESTAMP '1970-01-01T00:00:00.000015Z' AND TIMESTAMP '1970-01-01T00:00:00.000020Z'"] {
        assert_eq!(query(context(), &format!("SETTING DEFAULT VALID_TIME {extent} SELECT v FROM foo"), vec![]).await.unwrap(), [vec![V::Utf8("new".into())]]);
    }
}

#[tokio::test]
async fn snapshot_settings_keep_lexical_parameter_indices_and_the_time_ceiling() {
    let sql = "SETTING SNAPSHOT_TIME = ?, CLOCK_TIME = ? SELECT v,CURRENT_TIMESTAMP,? AS tail FROM foo FOR ALL VALID_TIME FOR SYSTEM_TIME AS OF TIMESTAMP '1970-01-01T00:00:00.000300Z'";
    assert_eq!(rtdb_sql::parse(sql).unwrap()[0].parameter_count(), 3);
    assert_eq!(query(context(), sql, vec![timestamp(150), timestamp(999), V::I64(7)]).await.unwrap(),
        [vec![V::Utf8("old".into()), timestamp(999), V::I64(7)]]);
    assert!(prepare("SETTING SNAPSHOT_TIME=TIMESTAMP 'not-a-date' SELECT 1", &context().catalog, &PrepareOptions::default()).is_err());
}

#[tokio::test]
async fn clock_literals_parameters_nulls_and_unevaluated_calls_follow_query_resolution() {
    assert_eq!(query(context(), "SETTING CLOCK_TIME='2020-01-01Z' SELECT CURRENT_TIMESTAMP", vec![]).await.unwrap(), [vec![timestamp(1_577_836_800_000_000)]]);
    let context = context().with_read_options(&basis(200), None, Timestamp(300)).unwrap();
    assert_eq!(query(Arc::clone(&context), "SETTING CLOCK_TIME=NULL SELECT CURRENT_TIMESTAMP", vec![]).await.unwrap(), [vec![timestamp(25)]]);
    assert_eq!(query(Arc::clone(&context), "SETTING CLOCK_TIME=? SELECT CURRENT_TIMESTAMP", vec![V::Null]).await.unwrap(), [vec![timestamp(300)]]);
    assert_eq!(query(Arc::clone(&context), "SETTING CLOCK_TIME=CURRENT_TIMESTAMP SELECT 1 WHERE FALSE", vec![]).await.unwrap_err().code, "rtdb.query/invalid-setting-value");
    assert_eq!(query(context, "SETTING CLOCK_TIME='2020-01-01Z',CLOCK_TIME='2021-01-01Z' SELECT CURRENT_TIMESTAMP", vec![]).await.unwrap(), [vec![timestamp(1_609_459_200_000_000)]]);
}

#[test]
fn settings_are_top_level_query_prefixes_and_all_duplicate_occurrences_are_planned() {
    for sql in ["SETTING CLOCK_TIME='2020-01-01Z' INSERT INTO foo (_id) VALUES (1)", "SELECT * FROM (SETTING CLOCK_TIME='2020-01-01Z' SELECT 1) x", "SETTING CLOCK_TIME '2020-01-01Z' SELECT 1"] { assert!(rtdb_sql::parse(sql).is_err()); }
    assert!(prepare("SETTING CLOCK_TIME=TIMESTAMP 'invalid',CLOCK_TIME='2020-01-01Z' SELECT 1", &context().catalog, &PrepareOptions::default()).is_err());
}

#[tokio::test]
async fn clock_settings_preserve_exact_literals_strings_parameters_and_fallback() {
    let nanos = |value| V::Timestamp { value,unit: TimeUnit::Nanosecond,timezone: Some("UTC".into()) };
    for literal in ["TIMESTAMP '1970-01-01T00:00:00.000000999Z'","+(TIMESTAMP '1970-01-01T00:00:00.000000999Z')","'1970-01-01T00:00:00.000000999Z'"] {
        assert_eq!(query(context(),&format!("SETTING CLOCK_TIME={literal} SELECT CURRENT_TIMESTAMP(9),CURRENT_TIMESTAMP(6) AS test_output_2"),vec![]).await.unwrap(),[vec![nanos(999),timestamp(0)]]);
    }
    assert_eq!(query(context(),"SETTING CLOCK_TIME=? SELECT CURRENT_TIMESTAMP(9)",vec![nanos(-1)]).await.unwrap(),[vec![nanos(-1)]]);
    let context = context().with_read_options(&basis(200),None,rtdb_types::Instant::new(0,123).unwrap()).unwrap();
    assert_eq!(query(Arc::clone(&context),"SETTING CLOCK_TIME=? SELECT CURRENT_TIMESTAMP(9)",vec![V::Null]).await.unwrap(),[vec![nanos(123)]]);
    assert_eq!(query(context,"SETTING CLOCK_TIME=NULL SELECT CURRENT_TIMESTAMP(9)",vec![]).await.unwrap(),[vec![nanos(25_000)]]);
}

#[tokio::test]
async fn exact_clock_is_narrowed_only_for_physical_default_valid_time() {
    let rows = query(context(),"SETTING CLOCK_TIME=TIMESTAMP '1970-01-01T00:00:00.000025999Z' SELECT v,CURRENT_TIMESTAMP(9) FROM foo",vec![]).await.unwrap();
    assert_eq!(rows,[vec![V::Utf8("new".into()),V::Timestamp { value: 25_999,unit: TimeUnit::Nanosecond,timezone: Some("UTC".into()) }]]);
}

#[tokio::test]
async fn clock_literal_can_supply_a_calendar_date_beyond_the_microsecond_timestamp_domain() {
    let date = rtdb_types::CivilDate::from_ymd(300_000,1,1).unwrap();
    assert_eq!(query(context(),"SETTING CLOCK_TIME=TIMESTAMP '300000-01-01T00:00:00.000000999Z' SELECT CURRENT_DATE",vec![]).await.unwrap(),[vec![V::DateDay(date.epoch_day() as i32)]]);
    assert_eq!(query(context(),"SETTING CLOCK_TIME=TIMESTAMP '300000-01-01T00:00:00.000000999Z' SELECT v FROM foo FOR ALL VALID_TIME FOR ALL SYSTEM_TIME",vec![]).await.unwrap().len(),2);
}
