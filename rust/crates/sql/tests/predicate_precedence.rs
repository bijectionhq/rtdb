use std::{collections::BTreeMap,sync::Arc};
use rtdb_columnar::{MemoryBudget,read_value};
use rtdb_plan::Statement;
use rtdb_query::{CatalogSnapshot,DatabaseSnapshot,PreparedQuery,QueryContext,QueryOptions};
use rtdb_sql::{PrepareOptions,prepare};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{Result,Timestamp,Value as V};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    QueryContext::new(catalog,MemoryBudget::new(1 << 27),Timestamp(0),"UTC",QueryOptions::default(),&CancellationToken::new()).unwrap()
}
async fn query(source: &str,arguments: Vec<V>) -> Result<Vec<Vec<V>>> {
    let context = context();
    let result = async {
        let Statement::Query(relation) = prepare(source,&context.catalog,&PrepareOptions::default())?.statement else { panic!() };
        let mut cursor = PreparedQuery::new(relation)?.execute(Arc::clone(&context),arguments).await?;
        let mut rows = vec![];
        while let Some(batch) = cursor.next_batch().await? {
            for row in 0..batch.batch().num_rows() { rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field,array)|read_value(field,array.as_ref(),row)).collect::<Result<_>>()?); }
        }
        Ok(rows)
    }.await;
    assert_eq!(context.memory.used_bytes(),0);
    result
}

#[tokio::test]
async fn ordinary_predicates_bind_above_comparison_and_below_common_values() {
    for (expression,expected) in [
        ("FALSE = 'a' LIKE 'a'",false),("TRUE = 'a' ILIKE 'A'",true),
        ("TRUE = 1 IN (1)",true),("FALSE = 1 NOT IN (1)",true),
        ("TRUE = 1 BETWEEN 0 AND 2",true),("FALSE = 1 NOT BETWEEN 0 AND 2",true),
        ("'a' LIKE 'a' = TRUE",true),("'a' || 'b' ILIKE 'A' || 'B'",true),
        ("NOT FALSE = 'a' LIKE 'a' AND TRUE",true),
        ("1 BETWEEN 0 AND 2 IN (TRUE)",true),("'a' LIKE 'a' IN (TRUE)",true),
        ("TRUE = 1 IN (SELECT 1)",true),("TRUE = 1 IN (0,1,2,3,4,5,6,7,8)",true),
        ("1 + 2 & 3 BETWEEN 2 | 1 AND 4 - 1",true),
    ] { assert_eq!(query(&format!("SELECT {expression}"),vec![]).await.unwrap(),[vec![V::Bool(expected)]],"{expression}"); }
    assert_eq!(query("SELECT NULL LIKE 'a' OR FALSE,NOT 1 BETWEEN 0 AND 2",vec![]).await.unwrap(),[vec![V::Null,V::Bool(false)]]);
}

#[tokio::test(start_paused = true)]
async fn escape_values_are_syntax_only_and_never_bind_or_execute() {
    let start = tokio::time::Instant::now();
    for escape in ["1 / 0","PG_SLEEP(0.005)","unknown_column","UNREGISTERED_FUNCTION()","(SELECT missing FROM missing_table)","(NOT TRUE)","CASE WHEN 1 / 0 = 0 THEN 1 ELSE 2 END"] {
        assert_eq!(query(&format!("SELECT 'a' LIKE 'a' ESCAPE {escape}"),vec![]).await.unwrap(),[vec![V::Bool(true)]],"{escape}");
    }
    assert_eq!(tokio::time::Instant::now(),start);
    let source = "SELECT 'a' LIKE 'a' ESCAPE ?,?";
    assert_eq!(rtdb_sql::parse(source).unwrap()[0].parameter_count(),2);
    let prepared = prepare(source,&context().catalog,&PrepareOptions::default()).unwrap();
    assert_eq!(prepared.parameter_types.keys().copied().collect::<Vec<_>>(),[1]);
}

#[test]
fn common_value_operands_require_parentheses_around_predicates_and_boolean_operators() {
    for source in [
        "SELECT 'a' LIKE NOT TRUE","SELECT 'a' ILIKE NOT TRUE","SELECT 'a' LIKE 'a' ESCAPE NOT TRUE",
        "SELECT 'a' LIKE_REGEX NOT TRUE","SELECT 'a' LIKE_REGEX 'a' FLAG NOT TRUE","SELECT 'a' ~ NOT TRUE",
        "SELECT 1 BETWEEN NOT TRUE AND 2","SELECT 1 BETWEEN 0 AND NOT TRUE",
        "SELECT 1 BETWEEN 0 AND 2 BETWEEN FALSE AND TRUE","SELECT 'a' LIKE 'a' BETWEEN FALSE AND TRUE",
        "SELECT 'a' LIKE - NOT TRUE","SELECT 'a' LIKE 'a' ESCAPE 1 + NOT TRUE",
    ] { assert!(rtdb_sql::parse(source).is_err(),"{source}"); }
    for source in [
        "SELECT 'a' LIKE (NOT TRUE)","SELECT 'a' LIKE (TRUE = TRUE)","SELECT 'a' LIKE_REGEX 'a' FLAG (NOT TRUE)",
        "SELECT 1 BETWEEN (NOT TRUE) AND 2","SELECT (1 BETWEEN 0 AND 2) BETWEEN FALSE AND TRUE",
        "SELECT 'a' LIKE 'a' ESCAPE CAST(NOT TRUE AS TEXT)","SELECT 'a' LIKE 'a' ESCAPE POSITION((TRUE = TRUE) IN 'x')",
    ] { assert!(rtdb_sql::parse(source).is_ok(),"{source}"); }
}
