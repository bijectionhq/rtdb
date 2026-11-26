use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::MemoryBudget;
use rtdb_plan::{BinaryOp, CastTarget, ColumnId, Expr, Field, Relation, RelationNode, Schema};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType, ScalarType, Timestamp, Value};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 24), Timestamp(0), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap()
}

/// `SELECT a FROM (VALUES (?)) t(a) WHERE CAST(a AS BIGINT) = 1`, the parameter typed as text by the plan.
fn cast_filter() -> Arc<Relation> {
    let text = LogicalType::Scalar(ScalarType::Utf8);
    let a = Field { id: ColumnId(0), name: "a".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: text.clone() };
    let values = Arc::new(Relation::new(Schema::new(vec![a.clone()]).unwrap(),
        RelationNode::Values { rows: vec![vec![Expr::Parameter { index: 0, data_type: text }]] }).unwrap());
    let cast = Expr::Cast { expr: Box::new(a.expression()), target: CastTarget::Fixed(LogicalType::Scalar(ScalarType::I64)) };
    Arc::new(values.filter(Expr::binary(BinaryOp::Eq, cast, Expr::Literal(Value::I64(1)))).unwrap())
}

async fn rows(query: &PreparedQuery, parameter: &str) -> rtdb_types::Result<usize> {
    let mut cursor = query.execute(context(), vec![Value::Utf8(parameter.into())]).await?;
    let mut rows = 0;
    while let Some(page) = cursor.next_batch().await? { rows += page.batch().num_rows(); }
    Ok(rows)
}

/// The kept plan admits its expressions once for their types; a later execution's parameter values are still read and
/// still fail where they do not convert.
#[tokio::test]
async fn a_kept_plan_reads_each_executions_parameter_values() {
    let keeper = MemoryBudget::new(1 << 24);
    let query = PreparedQuery::new_in(cast_filter(), &keeper).unwrap();
    assert_eq!(rows(&query, "1").await.unwrap(), 1);
    assert_eq!(rows(&query, "2").await.unwrap(), 0);
    assert!(rows(&query, "x").await.is_err(), "a value that does not convert fails in a later execution too");
    assert_eq!(rows(&query, "1").await.unwrap(), 1);
}
