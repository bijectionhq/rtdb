use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_format::transit::{TransitValue, decode_msgpack};
use rtdb_index::{PartitionState, TransactionBuffer};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{Interval, Keyword, Result, TableRef, Timestamp, TransactionKey, TxId, Value};
use tokio_util::sync::CancellationToken;

fn id(number: u8) -> Value { let mut bytes = [0; 16]; bytes[15] = number; Value::Uuid(bytes) }
fn context() -> Arc<QueryContext> {
    let memory = MemoryBudget::new(1 << 27);
    let state = PartitionState::empty(Default::default()).unwrap();
    let mut tx = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(0), system_time: Timestamp(1) }, memory).unwrap();
    for (table, column, values) in [("foo", "v", vec!["x", "y"]), ("bar", "w", vec!["x", "z", "y"])] {
        for (index, value) in values.iter().enumerate() {
            tx.put(TableRef::new("public", table), BTreeMap::from([("_id".into(), id(index as u8 + 1)), (column.into(), Value::Utf8((*value).into()))]), Interval::ALL).unwrap();
        }
    }
    state.apply(&tx.stage().unwrap()).unwrap();
    let database = DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 27), Timestamp(2), "UTC", QueryOptions { batch_rows: 1, ..Default::default() }, &CancellationToken::new()).unwrap()
}
async fn rows(context: &Arc<QueryContext>, sql: &str) -> Result<Vec<BTreeMap<String, Value>>> {
    let prepared = PreparedQuery::from_prepared(prepare(sql, &context.catalog, &PrepareOptions::default())?, &context.memory)?;
    let mut cursor = prepared.execute(Arc::clone(context), vec![]).await?;
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() {
            rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field, array)| Ok((field.name().clone(), read_value(field, array.as_ref(), row)?))).collect::<Result<_>>()?);
        }
    }
    Ok(rows)
}
fn scan<'a>(rows: &'a [BTreeMap<String, Value>], name: &str) -> &'a BTreeMap<String, Value> {
    rows.iter().find(|row| row["op"] == Value::Keyword(Keyword::from_name("scan")) && row["attributes"].field("scan_source") == Some(&Value::Utf8(format!("public.{name}")))).unwrap()
}
fn entry<'a>(value: &'a TransitValue, key: &str) -> Option<&'a TransitValue> {
    let TransitValue::Map(entries) = value else { panic!("Expected Transit map") };
    entries.iter().find_map(|(name, value)| (name == &TransitValue::Keyword(key.into())).then_some(value))
}
fn filters(row: &BTreeMap<String, Value>) -> Vec<TransitValue> {
    let Value::Transit(bytes) = &row["pushdowns"] else { panic!("Expected actual scan pushdowns") };
    let TransitValue::Array(values) = decode_msgpack(bytes).unwrap() else { panic!("Expected pushdown vector") }; values
}

#[tokio::test]
async fn api_test_join_iid_and_bloom_descriptors_follow_the_actual_probe_scan() {
    let context = context();
    let measured = rows(&context, "EXPLAIN ANALYZE SELECT f.v, b.w FROM foo f JOIN bar b ON f._id = b._id").await.unwrap();
    assert_eq!(scan(&measured, "foo")["pushdowns"], Value::Null);
    let probe = scan(&measured, "bar");
    assert_eq!(probe["row_count"], Value::I64(2));
    let filters = filters(probe); assert_eq!(filters.len(), 1);
    assert_eq!(entry(&filters[0], "column"), Some(&TransitValue::String("_id".into())));
    assert_eq!(entry(entry(&filters[0], "iids").unwrap(), "count"), Some(&TransitValue::Integer(2)));
    assert!(matches!(entry(entry(&filters[0], "bloom-filter").unwrap(), "cardinality"), Some(TransitValue::Integer(value)) if *value > 0));
    let result = rows(&context, "SELECT f.v, b.w FROM foo f JOIN bar b ON f._id = b._id ORDER BY f.v").await.unwrap();
    assert_eq!(result, [BTreeMap::from([("v".into(), Value::Utf8("x".into())), ("w".into(), Value::Utf8("x".into()))]), BTreeMap::from([("v".into(), Value::Utf8("y".into())), ("w".into(), Value::Utf8("z".into()))])]);
    assert_eq!(context.memory.used_bytes(), 0); assert_eq!(context.spill_bytes(), 0);
}

#[tokio::test]
async fn renamed_non_id_keys_have_blooms_without_iids_and_unadvanced_join_has_neither() {
    let context = context();
    let measured = rows(&context, "EXPLAIN ANALYZE SELECT f.v, b.x FROM foo f JOIN (SELECT w AS x FROM bar) b ON f.v = b.x").await.unwrap();
    let filters = filters(scan(&measured, "bar"));
    assert_eq!(entry(&filters[0], "column"), Some(&TransitValue::String("w".into())));
    assert_eq!(entry(&filters[0], "iids"), None);
    for sql in ["EXPLAIN ANALYZE SELECT * FROM foo", "EXPLAIN ANALYZE SELECT f.v FROM foo f JOIN bar b ON f._id = b._id LIMIT 0"] {
        assert!(rows(&context, sql).await.unwrap().iter().all(|row| row["pushdowns"] == Value::Null));
    }
    assert_eq!(context.memory.used_bytes(), 0); assert_eq!(context.spill_bytes(), 0);
}

#[tokio::test]
async fn empty_build_retains_empty_iid_descriptor_and_outer_joins_keep_unmatched_rows() {
    let context = context();
    let measured = rows(&context, "EXPLAIN ANALYZE SELECT f.v FROM (SELECT * FROM foo WHERE v = 'absent') f JOIN bar b ON f._id = b._id").await.unwrap();
    let empty_filters = filters(scan(&measured, "bar"));
    assert_eq!(entry(entry(&empty_filters[0], "iids").unwrap(), "count"), Some(&TransitValue::Integer(0)));
    for (join, count) in [("LEFT JOIN", 2), ("RIGHT JOIN", 3), ("FULL JOIN", 5)] {
        let sql = format!("SELECT f.v, b.w FROM foo f {join} bar b ON f.v = 'absent' AND f._id = b._id");
        assert_eq!(rows(&context, &sql).await.unwrap().len(), count);
        let measured = rows(&context, &format!("EXPLAIN ANALYZE {sql}")).await.unwrap();
        let probe = scan(&measured, if join == "LEFT JOIN" { "foo" } else { "bar" });
        let filters = filters(probe); assert_eq!(filters.len(), 1);
        assert_eq!(entry(&filters[0], "bloom-filter"), None);
        assert_eq!(entry(entry(&filters[0], "iids").unwrap(), "count"), Some(&TransitValue::Integer(0)));
    }
    assert_eq!(context.memory.used_bytes(), 0); assert_eq!(context.spill_bytes(), 0);
}

#[tokio::test]
async fn cancellation_drops_probe_filters_and_build_spool() {
    let context = context();
    let prepared = PreparedQuery::from_prepared(prepare("SELECT f.v, b.w FROM foo f JOIN bar b ON f._id = b._id", &context.catalog, &PrepareOptions::default()).unwrap(), &context.memory).unwrap();
    let mut cursor = prepared.execute(Arc::clone(&context), vec![]).await.unwrap();
    drop(cursor.next_batch().await.unwrap().unwrap());
    assert!(context.memory.used_bytes() > 0); assert!(context.spill_bytes() > 0);
    context.cancellation.cancel();
    assert_eq!(cursor.next_batch().await.err().unwrap().code, "xtdb/query-cancelled");
    drop(cursor); drop(prepared);
    assert_eq!(context.memory.used_bytes(), 0); assert_eq!(context.spill_bytes(), 0);
}

#[tokio::test]
async fn builds_exceeding_the_threshold_disable_filters_without_changing_join_rows() {
    let base = context();
    let context = QueryContext::new(base.catalog.clone(), MemoryBudget::new(1 << 27), Timestamp(2), "UTC", QueryOptions { join_build_rows: 1, batch_rows: 1, ..Default::default() }, &CancellationToken::new()).unwrap();
    let sql = "SELECT f.v, b.w FROM foo f JOIN bar b ON f._id = b._id";
    assert_eq!(rows(&context, sql).await.unwrap().len(), 2);
    let measured = rows(&context, &format!("EXPLAIN ANALYZE {sql}")).await.unwrap();
    assert!(measured.iter().all(|row| row["pushdowns"] == Value::Null));
    assert_eq!(scan(&measured, "bar")["row_count"], Value::I64(3));
    assert_eq!(context.memory.used_bytes(), 0); assert_eq!(context.spill_bytes(), 0);
}

#[tokio::test]
async fn inherited_join_filters_stop_before_row_bounds_and_window_evaluation() {
    let context = context();
    for bounds in ["LIMIT 1", "OFFSET 2 LIMIT 1"] {
        let sql = format!("SELECT b._id FROM (SELECT * FROM foo WHERE v = 'y') f JOIN (SELECT _id FROM bar ORDER BY _id {bounds}) b ON f._id = b._id");
        assert!(rows(&context, &sql).await.unwrap().is_empty(), "{sql}");
        let measured = rows(&context, &format!("EXPLAIN ANALYZE {sql}")).await.unwrap();
        assert_eq!(scan(&measured, "bar")["pushdowns"], Value::Null);
    }
    let sql = "SELECT b.rn FROM (SELECT * FROM foo WHERE v = 'y') f JOIN (SELECT _id, row_number() OVER (ORDER BY _id) rn FROM bar) b ON f._id = b._id";
    assert_eq!(rows(&context, sql).await.unwrap(), [BTreeMap::from([("rn".into(), Value::I64(2))])]);
    assert_eq!(scan(&rows(&context, &format!("EXPLAIN ANALYZE {sql}")).await.unwrap(), "bar")["pushdowns"], Value::Null);
}
