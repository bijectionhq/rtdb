use std::{collections::BTreeMap, future::{Future, poll_fn}, pin::Pin, sync::Arc, task::Poll};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{ColumnId, Expr, Field, NamedExpr, Relation, RelationNode, Schema};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType, TimeUnit, Timestamp, Value};
use tokio_util::sync::CancellationToken;

fn context(query: &CancellationToken, owner: &CancellationToken) -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::with_owner(catalog, MemoryBudget::new(1 << 24), Timestamp(0), "UTC", QueryOptions::default(), query, owner).unwrap()
}
fn field(data_type: LogicalType) -> Field {
    Field { id: ColumnId(0), name: "value".into(), qualifier: None, qualified_only: false, unnamed: false, data_type }
}

fn projected(expression: Expr) -> PreparedQuery {
    let projection = NamedExpr { field: field(expression.data_type()), expr: expression };
    let relation = Arc::new(Relation::unit()).project(vec![projection]).unwrap();
    PreparedQuery::new(Arc::new(relation)).unwrap()
}

fn sleeping_values() -> PreparedQuery {
    let schema = Schema::new(vec![field(LogicalType::Null)]).unwrap();
    let rows = vec![vec![Expr::Literal(Value::Null)], vec![sleep()]];
    PreparedQuery::new(Arc::new(Relation::new(schema, RelationNode::Values { rows }).unwrap())).unwrap()
}

fn sleep() -> Expr { Expr::Call { name: "sleep".into(), args: vec![Expr::Literal(Value::Duration { value: 60_000, unit: TimeUnit::Millisecond })], return_type: LogicalType::Null } }

async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
    poll_fn(|context| {
        assert!(future.as_mut().poll(context).is_pending());
        Poll::Ready(())
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn pending_sleep_yields_to_query_and_database_cancellation_and_releases_row_scope() {
    for cancel_owner in [false, true] {
        let query = CancellationToken::new();
        let owner = CancellationToken::new();
        let context = context(&query, &owner);
        let mut cursor = projected(sleep()).execute(Arc::clone(&context), vec![]).await.unwrap();
        let cancel = if cancel_owner { &owner } else { &query };
        let (result, ()) = tokio::join!(cursor.next_batch(), async { tokio::task::yield_now().await; cancel.cancel(); });
        assert_eq!(result.err().unwrap().code, "xtdb/query-cancelled");
        drop(cursor);
        assert_eq!(context.memory.used_bytes(), 0);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_a_pending_sleep_pull_and_cursor_keeps_sibling_query_usable() {
    let context = context(&CancellationToken::new(), &CancellationToken::new());
    let mut sleeping = projected(sleep()).execute(Arc::clone(&context), vec![]).await.unwrap();
    {
        let mut pending = Box::pin(sleeping.next_batch());
        assert_pending(pending.as_mut()).await;
    }
    sleeping.cancel();
    drop(sleeping);
    assert_eq!(context.memory.used_bytes(), 0);
    let mut sibling = projected(Expr::Literal(Value::I64(7))).execute(Arc::clone(&context), vec![]).await.unwrap();
    let batch = sibling.next_batch().await.unwrap().unwrap();
    assert_eq!(read_value(batch.batch().schema().field(0), batch.batch().column(0).as_ref(), 0).unwrap(), Value::I64(7));
    drop(batch); drop(sibling);
    assert_eq!(context.memory.used_bytes(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_values_opening_releases_unpublished_rows() {
    for cancel_owner in [false, true] {
        let query = CancellationToken::new();
        let owner = CancellationToken::new();
        let context = context(&query, &owner);
        let statement = sleeping_values();
        let mut opening = Box::pin(statement.execute(Arc::clone(&context), vec![]));
        assert_pending(opening.as_mut()).await;
        let cancel = if cancel_owner { &owner } else { &query };
        cancel.cancel();
        let result = opening.await;
        assert_eq!(result.err().unwrap().code, "xtdb/query-cancelled");
        assert_eq!(context.memory.used_bytes(), 0);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_values_opening_releases_unpublished_rows_without_cancelling_its_sibling() {
    let query = CancellationToken::new();
    let owner = CancellationToken::new();
    let context = context(&query, &owner);
    let statement = sleeping_values();
    let mut opening = Box::pin(statement.execute(Arc::clone(&context), vec![]));
    assert_pending(opening.as_mut()).await;
    assert!(context.memory.used_bytes() > 0);
    drop(opening);
    assert_eq!(context.memory.used_bytes(), 0);
    assert!(!query.is_cancelled());
    assert!(!owner.is_cancelled());
    let mut sibling = projected(Expr::Literal(Value::I64(7))).execute(Arc::clone(&context), vec![]).await.unwrap();
    let batch = sibling.next_batch().await.unwrap().unwrap();
    assert_eq!(read_value(batch.batch().schema().field(0), batch.batch().column(0).as_ref(), 0).unwrap(), Value::I64(7));
    drop(batch); drop(sibling);
    assert_eq!(context.memory.used_bytes(), 0);
}
