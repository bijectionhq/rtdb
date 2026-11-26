use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{ColumnId, Expr, Field, Relation, RelationNode, Schema};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType, ScalarType, Timestamp, Value};
use tokio_util::sync::CancellationToken;

fn field(id: u32, name: &str, ty: LogicalType) -> Field { Field { id: ColumnId(id), name: name.into(), qualifier: None, qualified_only: false, unnamed: false, data_type: ty } }

fn context(hash_memory_bytes: usize) -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    let options = QueryOptions { batch_rows: 64, hash_memory_bytes, ..Default::default() };
    QueryContext::new(catalog, MemoryBudget::new(1 << 26), Timestamp(0), "UTC", options, &CancellationToken::new()).unwrap()
}

/// Nullable pairs with many duplicates, the copies of each pair spread over the whole input.
fn distinct_pairs() -> Arc<Relation> {
    let schema = Schema::new(vec![
        field(0, "a", LogicalType::Scalar(ScalarType::I64).maybe()),
        field(1, "b", LogicalType::Scalar(ScalarType::Utf8).maybe()),
    ]).unwrap();
    let rows = (0..3000_i64).map(|n| vec![
        Expr::Literal(if n % 23 == 0 { Value::Null } else { Value::I64((n * 7919) % 401) }),
        Expr::Literal(if n % 3 == 0 { Value::Null } else { Value::Utf8(format!("s{}", n % 2)) }),
    ]).collect();
    let input = Arc::new(Relation::new(schema.clone(), RelationNode::Values { rows }).unwrap());
    Arc::new(Relation::new(schema, RelationNode::Distinct { input }).unwrap())
}

async fn rows(context: &Arc<QueryContext>) -> Vec<String> {
    let mut cursor = PreparedQuery::new(distinct_pairs()).unwrap().execute(Arc::clone(context), vec![]).await.unwrap();
    let mut rows = Vec::new();
    while let Some(page) = cursor.next_batch().await.unwrap() {
        let batch = page.batch();
        for row in 0..batch.num_rows() {
            let values = (0..batch.num_columns())
                .map(|column| read_value(batch.schema().field(column), batch.column(column).as_ref(), row).unwrap())
                .collect::<Vec<_>>();
            rows.push(format!("{values:?}"));
        }
    }
    rows
}

#[tokio::test]
async fn distinct_past_its_hash_memory_spills_partitions_and_matches_the_in_memory_result() {
    let reference = context(usize::MAX);
    let mut expected = rows(&reference).await;
    assert_eq!(reference.spill_peak_bytes(), 0);
    expected.sort();
    let unique = expected.len();
    expected.dedup();
    assert_eq!(expected.len(), unique);

    let spilling = context(1);
    let mut actual = rows(&spilling).await;
    actual.sort();
    assert_eq!(actual, expected);
    assert!(spilling.spill_peak_bytes() > 0);
    assert_eq!(spilling.spill_bytes(), 0);
    assert_eq!(spilling.memory.used_bytes(), 0);
}

#[tokio::test]
async fn cancelling_a_spilled_distinct_deletes_its_pending_partitions() {
    let context = context(1);
    let mut cursor = PreparedQuery::new(distinct_pairs()).unwrap().execute(Arc::clone(&context), vec![]).await.unwrap();
    let mut pages = Vec::new();
    while context.spill_bytes() == 0 {
        pages.push(cursor.next_batch().await.unwrap().expect("the distinct spills before it ends"));
    }
    cursor.cancel();
    assert!(cursor.next_batch().await.is_err());
    drop((pages, cursor));
    assert_eq!(context.spill_bytes(), 0);
    assert_eq!(context.memory.used_bytes(), 0);
}

#[tokio::test]
async fn distinct_spills_on_the_keys_it_holds_not_on_what_else_the_budget_holds() {
    let reference = context(usize::MAX);
    let mut expected = rows(&reference).await;
    expected.sort();

    let context = context(1 << 20);
    let mut cursor = PreparedQuery::new(distinct_pairs()).unwrap().execute(Arc::clone(&context), vec![]).await.unwrap();
    let first = cursor.next_batch().await.unwrap().expect("the distinct emits its first keys");
    let mut actual = Vec::new();
    let mut collect = |page: &rtdb_columnar::OwnedBatch| {
        let batch = page.batch();
        for row in 0..batch.num_rows() {
            let values = (0..batch.num_columns())
                .map(|column| read_value(batch.schema().field(column), batch.column(column).as_ref(), row).unwrap())
                .collect::<Vec<_>>();
            actual.push(format!("{values:?}"));
        }
    };
    collect(&first);
    // Another holder of the query's budget grows well past the distinct's share while the distinct runs.
    let elsewhere = context.memory.reserve(16 << 20).unwrap();
    while let Some(page) = cursor.next_batch().await.unwrap() { collect(&page); }
    drop(elsewhere);
    actual.sort();
    assert_eq!(actual, expected);
    assert_eq!(context.spill_peak_bytes(), 0, "the distinct's own keys stay far below its share");
}
