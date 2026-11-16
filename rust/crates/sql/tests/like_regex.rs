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
async fn regex_predicates_lower_flags_negation_and_common_value_operands() {
    for (expression,expected) in [
        ("'A' LIKE_REGEX 'a' FLAG 'i'",true),("'A' NOT LIKE_REGEX 'a'",true),
        ("'A' ~ 'a'",false),("'A' ~* 'a'",true),("'A' !~ 'a'",true),("'A' !~* 'a'",false),
        ("'A' || 'b' LIKE_REGEX 'a' || 'b' FLAG 'i'",true),
        ("NOT 'A' ~* 'a' OR 'A' ~* 'a' AND TRUE",true),
        ("'a' LIKE_REGEX 'a' = TRUE",true),
        ("'A' LIKE_REGEX 'a' FLAG CASE WHEN TRUE THEN 'i' ELSE '' END",false),
        ("('a' LIKE_REGEX 'a' FLAG '') IN (TRUE,FALSE)",true),
    ] { assert_eq!(query(&format!("SELECT {expression}"),vec![]).await.unwrap(),[vec![V::Bool(expected)]],"{expression}"); }
    assert_eq!(query("SELECT $1 LIKE_REGEX $2 FLAG $3",vec![V::Utf8("A".into()),V::Utf8("a".into()),V::Utf8("i".into())]).await.unwrap(),[vec![V::Bool(false)]]);
    assert_eq!(query("SELECT $1 ~* $2",vec![V::Utf8("A".into()),V::Utf8("a".into())]).await.unwrap(),[vec![V::Bool(true)]]);
    assert_eq!(query("SELECT SUM(v) = 1 AND 'a' LIKE_REGEX MAX(p) FROM (VALUES (1,'a')) AS t(v,p)",vec![]).await.unwrap(),[vec![V::Bool(true)]]);
}

#[tokio::test]
async fn regex_sql_demand_preserves_control_selection_and_literal_flags() {
    assert_eq!(query("SELECT CAST(1 / 0 AS TEXT) LIKE_REGEX '['",vec![]).await.unwrap_err().code,"xtdb.expression/illegal-argument-exception");
    for expression in ["NULL LIKE_REGEX '['","CAST(1 / 0 AS TEXT) LIKE_REGEX '[' FLAG NULL"] {
        assert_eq!(query(&format!("SELECT {expression}"),vec![]).await.unwrap(),[vec![V::Null]]);
    }
    assert_eq!(query("SELECT 'A' LIKE_REGEX 'a' FLAG CAST(1 / 0 AS TEXT)",vec![]).await.unwrap(),[vec![V::Bool(false)]]);
    assert_eq!(query("SELECT 'A' LIKE_REGEX '[' FLAG CASE WHEN 1 / 0 = 0 THEN 'i' ELSE '' END",vec![]).await.unwrap_err().code,"xtdb.expression/division-by-zero");
    assert_eq!(query("SELECT 'A' LIKE_REGEX 'a' FLAG CASE WHEN TRUE THEN CAST(1 / 0 AS TEXT) ELSE NULL END",vec![]).await.unwrap_err().code,"xtdb.expression/division-by-zero");
    for source in ["SELECT LIKE_REGEX('a','a','')","SELECT 'a' LIKE_REGEX 'a' FLAG 1","SELECT 1 ~ 'a'"] { assert!(query(source,vec![]).await.is_err(),"{source}"); }
}

#[test]
fn regex_settings_preserve_flags_parameters_without_materializing_the_private_pair() {
    let source = "SETTING CLOCK_TIME = ? LIKE_REGEX ? FLAG ? SELECT ?";
    assert_eq!(rtdb_sql::parse(source).unwrap()[0].parameter_count(),4);
    let prepared = prepare(source,&context().catalog,&PrepareOptions::default()).unwrap();
    assert_eq!(prepared.parameter_types.len(),4);
    assert_eq!(prepared.settings.clock_time.unwrap().parameter_count(),3);
}
