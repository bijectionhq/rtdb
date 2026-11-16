use std::{collections::BTreeMap,sync::Arc};
use rtdb_columnar::{MemoryBudget,read_value};
use rtdb_plan::Statement;
use rtdb_query::{CatalogSnapshot,DatabaseSnapshot,PreparedQuery,QueryContext,QueryOptions};
use rtdb_sql::{PrepareOptions,prepare};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{Result,Timestamp,Value as V};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: Vec::new(),storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    QueryContext::new(catalog,MemoryBudget::new(1 << 27),Timestamp(0),"UTC",QueryOptions::default(),&CancellationToken::new()).unwrap()
}
async fn query(sql: &str,arguments: Vec<V>) -> Result<Vec<Vec<V>>> {
    let context = context();
    let result = async {
        let Statement::Query(relation) = prepare(sql,&context.catalog,&PrepareOptions::default())?.statement else { panic!("query") };
        let mut cursor = PreparedQuery::new(relation)?.execute(Arc::clone(&context),arguments).await?;
        let mut rows = Vec::new();
        while let Some(page) = cursor.next_batch().await? {
            let batch = page.batch();
            for row in 0..batch.num_rows() { rows.push(batch.schema().fields().iter().zip(batch.columns()).map(|(field,array)|read_value(field,array.as_ref(),row)).collect::<Result<_>>()?); }
        }
        Ok(rows)
    }.await;
    assert_eq!(context.memory.used_bytes(),0);
    result
}

#[tokio::test]
async fn sql_bitwise_operators_keep_signed_widths_and_parameter_admission() {
    assert_eq!(query("SELECT ~CAST(0 AS SMALLINT),6 & 3,6 | 3,6 # 3,CAST(127 AS SMALLINT) << 1,-4 >> 1,(1/0) & NULL",vec![]).await.unwrap(),
        [vec![V::I16(-1),V::I64(2),V::I64(7),V::I64(5),V::I16(254),V::I64(-2),V::Null]]);
    assert_eq!(query("SELECT CAST(1 AS INTEGER) << 2147483648",vec![]).await.unwrap_err().code,"xtdb.expression/overflow-error");
    assert_eq!(query("SELECT $1 << $2",vec![V::I8(127),V::I64(1)]).await.unwrap(),[vec![V::I8(-2)]]);
    assert_eq!(query("SELECT $1 & 1",vec![V::U8(1)]).await.unwrap(),[vec![V::I64(1)]]);
    let context = context();
    for expression in ["1.0 & 1","~TRUE","bit_and(1,2)","bit_not(1)","6 ^ 3"] {
        assert!(prepare(&format!("SELECT {expression}"),&context.catalog,&PrepareOptions::default()).is_err(),"{expression}");
    }
}

#[tokio::test]
async fn sql_bitwise_precedence_keeps_one_left_associative_level_below_arithmetic() {
    assert_eq!(query("SELECT 1 | 2 & 4,7 # 3 & 2,1 | 2 << 1,8 >> 1 & 2,~1+2,~1+2 & 7,1+2 << 2+1",vec![]).await.unwrap(),
        [vec![V::I64(0),V::I64(0),V::I64(6),V::I64(0),V::I64(-4),V::I64(4),V::I64(24)]]);
    assert_eq!(query("SELECT 'a' || 'b' || 'c'",vec![]).await.unwrap(),[vec![V::Utf8("abc".into())]]);
}

#[tokio::test]
async fn runtime_bitwise_type_validation_uses_argument_types_without_input_columns() {
    assert_eq!(query("SELECT $1 & $2,$1 | $2,$1 # $2,$1 << $2,$1 >> $2",vec![V::I16(6),V::I16(1)]).await.unwrap(),
        [vec![V::I16(0),V::I16(7),V::I16(7),V::I16(12),V::I16(3)]]);
    assert!(query("SELECT $1 & $2",vec![V::Bool(true),V::Bool(false)]).await.is_err());
}
