use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_format::basis::{decode_time_basis, encode_time_basis};
use rtdb_index::{PartitionState, TransactionBuffer};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_trie::TrieConfig;
use rtdb_types::{Instant, Interval, Result, TableRef, TimeBasis, TimeUnit, Timestamp, TransactionKey, TxId, Value as V};
use tokio_util::sync::CancellationToken;

fn timestamp(value: i64) -> V { V::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) } }
fn basis(value: i64) -> TimeBasis { TimeBasis { databases: BTreeMap::from([("xtdb".into(), vec![Some(Timestamp(value).into())])]) } }
fn token(value: i64) -> String { encode_time_basis(&basis(value)) }

fn context() -> Arc<QueryContext> {
    let memory = MemoryBudget::new(1 << 27);
    let state = PartitionState::empty(TrieConfig::new(32, 32).unwrap()).unwrap();
    for (id, at, value) in [(0, 100, "old"), (1, 200, "new")] {
        let mut tx = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(id), system_time: Timestamp(at) }, Arc::clone(&memory)).unwrap();
        tx.put(TableRef::new("public", "foo"), BTreeMap::from([("_id".into(), V::I64(1)), ("v".into(), V::Utf8(value.into()))]), Interval::new(Timestamp(10), Timestamp(30)).unwrap()).unwrap();
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
async fn scalar_settings_keep_lexical_parameter_indices_and_snapshot_caps_explicit_time() {
    let sql = "SETTING SNAPSHOT_TOKEN = ?, CLOCK_TIME = ? SELECT v,CURRENT_TIMESTAMP,SNAPSHOT_TOKEN,? AS tail FROM foo FOR ALL VALID_TIME FOR SYSTEM_TIME AS OF TIMESTAMP '1970-01-01T00:00:00.000300Z'";
    assert_eq!(rtdb_sql::parse(sql).unwrap()[0].parameter_count(), 3);
    let old = [vec![V::Utf8("old".into()), timestamp(999), V::Utf8(token(150)), V::I64(7)]];
    assert_eq!(query(context(), sql, vec![V::Utf8(token(150)), timestamp(999), V::I64(7)]).await.unwrap(), old, "the transaction after the snapshot does not supersede");
    let rows = query(context(), &sql.replace("000300Z", "000100Z"), vec![V::Utf8(token(150)), timestamp(999), V::I64(7)]).await.unwrap();
    assert_eq!(rows, old);
    assert_eq!(query(context(), &format!("SETTING SNAPSHOT_TOKEN = '{}' SELECT 1 WHERE FALSE", token(201)), vec![]).await.unwrap_err().code, "xtdb/unindexed-tx");
    assert_eq!(query(context(), "SETTING SNAPSHOT_TOKEN = 'bad token' SELECT 1", vec![]).await.unwrap_err().code, "xtdb.basis/invalid-basis");
}

#[tokio::test]
async fn sql_token_can_replace_an_older_option_token_but_retains_the_option_time_cap() {
    let context = context().with_read_options(&basis(100), Some(Timestamp(175)), Timestamp(300)).unwrap();
    assert_eq!(query(Arc::clone(&context), "SELECT v,SNAPSHOT_TOKEN FROM foo", vec![]).await.unwrap(), [vec![V::Utf8("old".into()), V::Utf8(token(100))]]);
    assert_eq!(query(Arc::clone(&context), &format!("SETTING SNAPSHOT_TOKEN='{}' SELECT v,SNAPSHOT_TOKEN FROM foo", token(200)), vec![]).await.unwrap(), [vec![V::Utf8("old".into()), V::Utf8(token(175))]]);
    assert_eq!(context.catalog.database("xtdb").unwrap().partitions[0].tx_basis().unwrap().system_time, Timestamp(200));
    assert_eq!(query(context, "SETTING SNAPSHOT_TOKEN=NULL SELECT SNAPSHOT_TOKEN", vec![]).await.unwrap(), [vec![V::Utf8(token(175))]]);
}

#[tokio::test]
async fn planned_snapshot_time_is_ignored_but_its_literal_is_validated_at_preparation() {
    assert_eq!(query(context(), "SETTING SNAPSHOT_TIME=TIMESTAMP '1970-01-01T00:00:00.000150Z' SELECT v,SNAPSHOT_TOKEN FROM foo", vec![]).await.unwrap(), [vec![V::Utf8("new".into()), V::Utf8(token(200))]]);
    assert_eq!(query(context(), "SETTING SNAPSHOT_TIME=CURRENT_TIMESTAMP SELECT v FROM foo", vec![]).await.unwrap(), [vec![V::Utf8("new".into())]]);
    assert!(prepare("SETTING SNAPSHOT_TIME=TIMESTAMP 'not-a-date' SELECT 1", &context().catalog, &PrepareOptions::default()).is_err());
    assert!(prepare("SETTING SNAPSHOT_TIME=TIME '25:00:00' SELECT 1", &context().catalog, &PrepareOptions::default()).is_err());
    assert_eq!(query(context(), "SETTING SNAPSHOT_TIME=TIME '1:2:3+01:00' SELECT 1", vec![]).await.unwrap(), [vec![V::I64(1)]]);
}

#[tokio::test]
async fn unused_setting_forms_validate_syntax_and_literals_without_scalar_typing() {
    for sql in [
        "SETTING SNAPSHOT_TIME=LOWER(1) SELECT 1",
        "SETTING SNAPSHOT_TIME=REGEXP_REPLACE('x','[','y',1,1,'i') SELECT 1",
        "SETTING SNAPSHOT_TIME=FORMAT(1),CLOCK_TIME=LOWER(1),CLOCK_TIME='2020-01-01Z' SELECT 1",
        "SETTING DEFAULT VALID_TIME AS OF LOWER(1),DEFAULT VALID_TIME ALL SELECT 1",
        "SETTING DEFAULT SYSTEM_TIME AS OF LOWER(1) SELECT 1",
        "SETTING DEFAULT VALID_TIME AS OF LOWER(1) SELECT 1 FROM foo FOR ALL VALID_TIME",
    ] { assert_eq!(query(context(), sql, vec![]).await.unwrap(), [vec![V::I64(1)]], "{sql}"); }
    assert_eq!(query(context(), "SETTING SNAPSHOT_TIME=LOWER(?) SELECT ?", vec![V::I64(1), V::I64(9)]).await.unwrap(), [vec![V::I64(9)]]);
    assert_eq!(query(context(), "SETTING CLOCK_TIME=LOWER(1) SELECT 1", vec![]).await.unwrap_err().code, "rtdb.query/invalid-setting-value");
    let overwritten_parameter = "SETTING CLOCK_TIME=$2,CLOCK_TIME='2020-01-01Z' SELECT 1";
    assert_eq!(query(context(), overwritten_parameter, vec![]).await.unwrap_err().code, "rtdb.query/missing-parameter");
    assert_eq!(query(context(), overwritten_parameter, vec![V::Null, V::Null]).await.unwrap(), [vec![V::I64(1)]]);
    for sql in [
        "SETTING SNAPSHOT_TIME=LOWER() SELECT 1",
        "SETTING SNAPSHOT_TIME=REGEXP_REPLACE('x',LOWER('X'),'y') SELECT 1",
        "SETTING SNAPSHOT_TIME=missing_function(1) SELECT 1",
        "SETTING SNAPSHOT_TIME=LOWER(DATE '2020-02-30') SELECT 1",
        "SETTING SNAPSHOT_TIME=FORMAT(1,TIMESTAMP 'invalid') SELECT 1",
        "SETTING DEFAULT VALID_TIME AS OF TIMESTAMP 'invalid',DEFAULT VALID_TIME ALL SELECT 1",
    ] { assert!(prepare(sql, &context().catalog, &PrepareOptions::default()).is_err(), "{sql}"); }
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

#[tokio::test]
async fn token_metadata_preserves_nanoseconds_unknown_databases_and_empty_basis() {
    let mut expected = basis(150);
    expected.databases.get_mut("xtdb").unwrap()[0] = Some(Instant::new(0, 150_123).unwrap());
    expected.databases.insert("elsewhere".into(), vec![Some(Instant::new(900, 5).unwrap()), None]);
    let rows = query(context(), "SETTING SNAPSHOT_TOKEN=? SELECT v,SNAPSHOT_TOKEN FROM foo", vec![V::Utf8(encode_time_basis(&expected))]).await.unwrap();
    assert_eq!(rows[0][0], V::Utf8("old".into()));
    let V::Utf8(token) = &rows[0][1] else { panic!("Expected token") };
    assert_eq!(decode_time_basis(token).unwrap(), expected);
    assert_eq!(query(context(), "SETTING SNAPSHOT_TOKEN='' SELECT SNAPSHOT_TOKEN", vec![]).await.unwrap(), [vec![V::Utf8(String::new())]]);
    assert_eq!(query(context(), "SETTING SNAPSHOT_TOKEN='' SELECT v FROM foo ORDER BY v", vec![]).await.unwrap(), [vec![V::Utf8("new".into())], vec![V::Utf8("old".into())]]);
}

#[tokio::test]
async fn missing_and_null_partition_token_entries_leave_system_time_unconstrained() {
    for basis in [
        TimeBasis { databases: BTreeMap::from([("elsewhere".into(), vec![Some(Timestamp(150).into())])]) },
        TimeBasis { databases: BTreeMap::from([("xtdb".into(), vec![None])]) },
        TimeBasis { databases: BTreeMap::from([("xtdb".into(), vec![])]) },
    ] {
        let token = encode_time_basis(&basis);
        let rows = query(context(), "SETTING SNAPSHOT_TOKEN=? SELECT v,SNAPSHOT_TOKEN FROM foo ORDER BY v", vec![V::Utf8(token.clone())]).await.unwrap();
        assert_eq!(rows, [vec![V::Utf8("new".into()), V::Utf8(token.clone())], vec![V::Utf8("old".into()), V::Utf8(token.clone())]]);
        assert_eq!(query(context(), "SETTING SNAPSHOT_TOKEN=? SELECT v FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '1970-01-01T00:00:00.000150Z'", vec![V::Utf8(token)]).await.unwrap(), [vec![V::Utf8("old".into())]]);
    }
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
