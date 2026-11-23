use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::Statement;
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{Result, Timestamp, Value as V};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: Vec::new(), storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 27), Timestamp(0), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap()
}

async fn query(source: &str) -> Result<Vec<Vec<V>>> {
    let context = context();
    let Statement::Query(relation) = prepare(source, &context.catalog, &PrepareOptions::default())?.statement else { panic!("query") };
    let mut cursor = PreparedQuery::new(relation)?.execute(context, vec![]).await?;
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() { rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?); }
    }
    Ok(rows)
}

const DATA: &str = "WITH customers AS (SELECT * FROM (VALUES (0, 'bob'), (1, 'alice')) t(_id, name)),
    orders AS (SELECT * FROM (VALUES (0, 0, 26.20), (1, 0, 8.99), (2, 1, 12.34)) t(_id, customer_id, value))";

fn doc(fields: &[(&str, V)]) -> V {
    V::Struct(fields.iter().map(|(name, value)| (name.to_string(), value.clone())).collect())
}

#[tokio::test]
async fn nest_one_returns_the_correlated_row_as_a_struct() {
    let mut rows = query(&format!("{DATA} SELECT o._id AS order_id, o.value,
        NEST_ONE(SELECT c.name FROM customers c WHERE c._id = o.customer_id) AS customer
        FROM orders o")).await.unwrap();
    rows.sort_by_key(|row| format!("{:?}", row[0]));
    let bob = doc(&[("name", V::Utf8("bob".into()))]);
    assert_eq!(rows, [
        vec![V::I64(0), V::F64(26.20), bob.clone()],
        vec![V::I64(1), V::F64(8.99), bob],
        vec![V::I64(2), V::F64(12.34), doc(&[("name", V::Utf8("alice".into()))])],
    ]);
}

#[tokio::test]
async fn nest_one_is_null_without_a_row_and_rejects_several() {
    assert_eq!(query("SELECT NEST_ONE(SELECT 1 AS x WHERE FALSE) AS n").await.unwrap(), [vec![V::Null]]);
    assert_eq!(query("SELECT NEST_ONE(SELECT x FROM (VALUES (1), (2)) t(x)) AS n").await.unwrap_err().code,
        "xtdb.single-join/cardinality-violation");
}

#[tokio::test]
async fn nest_many_returns_the_correlated_rows_as_a_list_of_structs() {
    let mut rows = query(&format!("{DATA} SELECT c._id AS customer_id, c.name,
        NEST_MANY(SELECT o._id AS order_id, o.value FROM orders o WHERE o.customer_id = c._id) AS orders
        FROM customers c")).await.unwrap();
    rows.sort_by_key(|row| format!("{:?}", row[0]));
    for row in &mut rows {
        if let V::List(documents) = &mut row[2] { documents.values_mut().unwrap().sort_by_key(|document| format!("{document:?}")); }
    }
    let order = |id, value| doc(&[("order_id", V::I64(id)), ("value", V::F64(value))]);
    assert_eq!(rows, [
        vec![V::I64(0), V::Utf8("bob".into()), V::list(vec![order(0, 26.20), order(1, 8.99)])],
        vec![V::I64(1), V::Utf8("alice".into()), V::list(vec![order(2, 12.34)])],
    ]);
    assert_eq!(query("SELECT NEST_MANY(SELECT 1 AS x WHERE FALSE) AS n").await.unwrap(), [vec![V::Null]]);
}
