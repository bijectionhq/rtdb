use std::{collections::BTreeMap,sync::Arc};
use rtdb_columnar::{MemoryBudget,read_value};
use rtdb_plan::{Expr,Relation,RelationNode,Statement};
use rtdb_query::{CatalogSnapshot,DatabaseSnapshot,PreparedQuery,QueryContext,QueryOptions};
use rtdb_sql::{PrepareOptions,prepare};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{Result,Timestamp,Value as V};
use tokio_util::sync::CancellationToken;

fn context(batch_rows: usize) -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    QueryContext::new(catalog,MemoryBudget::new(1 << 27),Timestamp(0),"UTC",QueryOptions { batch_rows,..Default::default() },&CancellationToken::new()).unwrap()
}
async fn query(source: &str,parameters: Vec<V>,context: Arc<QueryContext>) -> Result<Vec<Vec<V>>> {
    let prepared = prepare(source,&context.catalog,&PrepareOptions::default())?;
    let prepared = PreparedQuery::from_prepared(prepared,&context.memory)?;
    let mut cursor = prepared.execute(context,parameters).await?;
    let mut rows = Vec::new();
    while let Some(page) = cursor.next_batch().await? {
        for row in 0..page.batch().num_rows() {
            rows.push(page.batch().schema().fields().iter().zip(page.batch().columns()).map(|(field,array)|read_value(field,array.as_ref(),row)).collect::<Result<_>>()?);
        }
    }
    Ok(rows)
}
fn window(relation: &Relation) -> Option<&Relation> {
    if matches!(relation.node(),RelationNode::Window {..}) { return Some(relation); }
    relation.children().into_iter().find_map(|child|window(child))
}

#[test]
fn window_order_inputs_are_private_upstream_projections_in_source_order() {
    let context = context(1);
    let Statement::Query(relation) = prepare("SELECT *,ROW_NUMBER() OVER(PARTITION BY p ORDER BY v,v+1) AS rn FROM (VALUES (1,2)) t(p,v)",&context.catalog,&PrepareOptions::default()).unwrap().statement else { panic!("query") };
    assert_eq!(relation.schema().fields().iter().map(|field|field.name.as_str()).collect::<Vec<_>>(),["p","v","rn"]);
    let RelationNode::Window { input,order_by,.. } = window(&relation).unwrap().node() else { panic!("window") };
    let RelationNode::Extend { input: source,expressions } = input.node() else { panic!("append projection") };
    assert_eq!(source.schema().len(),2);
    assert_eq!(expressions.len(),2);
    assert!(matches!(expressions[0].expr,Expr::Column {..}));
    assert!(matches!(expressions[1].expr,Expr::Binary {..}));
    for (order,projection) in order_by.iter().zip(expressions) {
        assert_eq!(order.expr,projection.field.expression());
        assert!(source.schema().get(projection.field.id).is_none());
    }
}

#[tokio::test]
async fn window_order_payload_failure_precedes_partition_type_admission() {
    for batch_rows in [1,1024] {
        let context = context(batch_rows);
        let result = query("SELECT ROW_NUMBER() OVER(PARTITION BY x ORDER BY 1/0) FROM (VALUES ($1)) t(x)",vec![V::Set(vec![V::I64(1)])],Arc::clone(&context)).await;
        assert_eq!(result.unwrap_err().code,"xtdb.expression/division-by-zero");
        assert_eq!(context.spill_bytes(),0);assert_eq!(context.memory.used_bytes(),0);
    }
}

#[tokio::test(start_paused = true)]
async fn window_order_effects_complete_before_partition_type_admission() {
    let context = context(1);let start = tokio::time::Instant::now();
    let result = query("SELECT ROW_NUMBER() OVER(PARTITION BY x ORDER BY PG_SLEEP(60.0)) FROM (VALUES ($1)) t(x)",vec![V::Set(vec![V::I64(1)])],Arc::clone(&context)).await;
    assert_eq!(result.unwrap_err().code,"xtdb.expression/set-equality");
    assert!(tokio::time::Instant::now() > start);
    assert_eq!(context.spill_bytes(),0);assert_eq!(context.memory.used_bytes(),0);
}

#[tokio::test]
async fn private_window_order_columns_do_not_change_wildcards_or_results() {
    let context = context(1);
    assert_eq!(query("SELECT *,ROW_NUMBER() OVER(PARTITION BY p ORDER BY -v) AS rn FROM (VALUES (1,2),(1,3)) t(p,v)",vec![],Arc::clone(&context)).await.unwrap(),[
        vec![V::I64(1),V::I64(3),V::I64(1)],vec![V::I64(1),V::I64(2),V::I64(2)],
    ]);
    assert_eq!(context.spill_bytes(),0);assert_eq!(context.memory.used_bytes(),0);
}
