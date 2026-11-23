use std::{collections::BTreeMap,sync::Arc};
use rtdb_columnar::{MemoryBudget,read_value};
use rtdb_plan::Statement;
use rtdb_query::{CatalogSnapshot,DatabaseSnapshot,PreparedQuery,QueryContext,QueryOptions};
use rtdb_sql::{PrepareOptions,prepare};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{ObjectSort,Result,Timestamp,Value};
use tokio_util::sync::CancellationToken;

async fn query(sql: &str,object_sort: ObjectSort) -> Vec<Vec<Value>> {
    let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    let context = QueryContext::new(catalog,MemoryBudget::new(1 << 27),Timestamp(0),"UTC",QueryOptions { object_sort,sort_memory_bytes: 1,batch_rows: 1,..Default::default() },&CancellationToken::new()).unwrap();
    let Statement::Query(relation) = prepare(sql,&context.catalog,&PrepareOptions::default()).unwrap().statement else { panic!("expected query") };
    let mut cursor = PreparedQuery::new(relation).unwrap().execute(Arc::clone(&context),vec![]).await.unwrap();
    let mut rows = Vec::new();
    while let Some(page) = cursor.next_batch().await.unwrap() {
        for row in 0..page.batch().num_rows() {
            rows.push(page.batch().schema().fields().iter().zip(page.batch().columns()).map(|(field,array)|read_value(field,array.as_ref(),row)).collect::<Result<Vec<_>>>().unwrap());
        }
    }
    drop(cursor);assert_eq!(context.memory.used_bytes(),0);assert_eq!(context.spill_bytes(),0);
    rows
}

#[tokio::test]
async fn configured_object_sort_reaches_order_window_array_and_percentile_consumers() {
    for object_sort in [ObjectSort::TimSort,ObjectSort::LegacyMergeSort] {
        let source = "FROM (VALUES ('c',3),('b',1),('a',1),('d',2)) t(label,n)";
        assert_eq!(query(&format!("SELECT label {source} ORDER BY n LIMIT 3"),object_sort).await,
            [vec![Value::Utf8("b".into())],vec![Value::Utf8("a".into())],vec![Value::Utf8("d".into())]]);
        assert_eq!(query(&format!("SELECT label,ROW_NUMBER() OVER (ORDER BY n) AS rn {source} ORDER BY rn"),object_sort).await,
            [vec![Value::Utf8("b".into()),Value::I64(1)],vec![Value::Utf8("a".into()),Value::I64(2)],vec![Value::Utf8("d".into()),Value::I64(3)],vec![Value::Utf8("c".into()),Value::I64(4)]]);
        assert_eq!(query(&format!("SELECT ARRAY_AGG(label ORDER BY n),PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY n),PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY n DESC) {source}"),object_sort).await,
            [vec![Value::list(["b","a","d","c"].into_iter().map(|value|Value::Utf8(value.into())).collect()),Value::F64(1.5),Value::I64(2)]]);
    }
}
