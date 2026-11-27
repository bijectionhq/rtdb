use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{ColumnId, Expr, Field, Relation, RelationNode, Schema};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType, ScalarType, Timestamp, Value};
use tokio_util::sync::CancellationToken;

fn field(id: u32, name: &str, ty: LogicalType) -> Field { Field { id: ColumnId(id), name: name.into(), qualifier: None, qualified_only: false, unnamed: false, data_type: ty } }

fn context(memory_bytes: usize) -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    let options = QueryOptions { batch_rows: 64, ..Default::default() };
    QueryContext::new(catalog, MemoryBudget::new(memory_bytes), Timestamp(0), "UTC", options, &CancellationToken::new()).unwrap()
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
async fn distinct_holds_its_keys_in_memory_and_never_spills() {
    let context = context(1 << 26);
    let mut actual = rows(&context).await;
    assert_eq!(context.spill_peak_bytes(), 0);
    actual.sort();
    let unique = actual.len();
    actual.dedup();
    assert_eq!(actual.len(), unique);
    assert_eq!(context.memory.used_bytes(), 0);
}

/// XTDB's DISTINCT keeps its keys in a `DistinctRelationMap` and fails the query when its allocator refuses them; so
/// does this, with no spill.
#[tokio::test]
async fn distinct_past_the_query_budget_fails_rather_than_spilling() {
    let reference = context(1 << 26);
    rows(&reference).await;
    let peak = reference.memory.peak_bytes();

    let context = context(peak - 1);
    let error = async {
        let mut cursor = PreparedQuery::new(distinct_pairs())?.execute(Arc::clone(&context), vec![]).await?;
        while cursor.next_batch().await?.is_some() {}
        Ok::<_, rtdb_types::Anomaly>(())
    }.await.expect_err("the distinct fits a budget below its peak");
    assert!(error.is(rtdb_types::AnomalyKind::ResourceExhausted), "{error:?}");
    assert_eq!(context.spill_peak_bytes(), 0);
}
