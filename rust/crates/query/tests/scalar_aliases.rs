use std::{collections::BTreeMap,sync::Arc};
use rtdb_columnar::{MemoryBudget,read_value};
use rtdb_plan::{ColumnId,Expr,Field,NamedExpr,Relation,RelationNode,Schema,function_type};
use rtdb_query::{CatalogSnapshot,DatabaseSnapshot,PreparedQuery,QueryContext,QueryOptions};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{LogicalType as L,ScalarType as S,Timestamp,Value as V};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn scalar_aliases_bind_null_parameters_without_losing_nonnull_result_types() {
    let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    let context = QueryContext::new(catalog,MemoryBudget::new(1 << 25),Timestamp(0),"UTC",QueryOptions::default(),&CancellationToken::new()).unwrap();
    let input = Arc::new(Relation::new(Schema::default(),RelationNode::Values { rows: vec![vec![]] }).unwrap());
    let mut expressions = Vec::new();
    for (index,name) in ["is_true","is_false","distinct_from","_meta_double","compare_nulls_first","compare_nulls_last","compare"].into_iter().enumerate() {
        let mut args = vec![Expr::Parameter { index: 0,data_type: L::Scalar(S::I64) }];
        if name == "distinct_from" || name.starts_with("compare") { args.push(Expr::Literal(V::I64(1))); }
        let data_type = function_type(name,&args.iter().map(Expr::data_type).collect::<Vec<_>>()).unwrap();
        expressions.push(NamedExpr {
            field: Field { id: ColumnId(index as u32),name: name.into(),qualifier: None,qualified_only: false,unnamed: false,data_type: data_type.clone() },
            expr: Expr::Call { name: name.into(),args,return_type: data_type },
        });
    }
    let schema = Schema::new(expressions.iter().map(|expression| expression.field.clone()).collect()).unwrap();
    let relation = Arc::new(Relation::new(schema,RelationNode::Project { input,expressions }).unwrap());
    let prepared = PreparedQuery::new(relation).unwrap();
    let mut cursor = prepared.execute(Arc::clone(&context),vec![V::Null]).await.unwrap();
    assert_eq!(cursor.runtime_schema().fields().iter().map(|field| field.data_type.clone()).collect::<Vec<_>>(),[L::Scalar(S::Bool),L::Scalar(S::Bool),L::Scalar(S::Bool),L::Null,L::Scalar(S::I32),L::Scalar(S::I32),L::Null]);
    let batch = cursor.next_batch().await.unwrap().unwrap();
    let values = batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field,array)|read_value(field,array.as_ref(),0).unwrap()).collect::<Vec<_>>();
    assert_eq!(values,[V::Bool(false),V::Bool(false),V::Bool(true),V::Null,V::I32(-1),V::I32(1),V::Null]);
    drop(batch);
    assert!(cursor.next_batch().await.unwrap().is_none());
    drop(cursor);
    assert_eq!(context.memory.used_bytes(),0);
}
