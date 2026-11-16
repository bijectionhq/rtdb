use std::{collections::BTreeMap,sync::Arc};
use rtdb_columnar::{MemoryBudget,read_value};
use rtdb_format::transit::{TransitValue as T,decode_msgpack,encode_msgpack};
use rtdb_plan::Statement;
use rtdb_query::{CatalogSnapshot,DatabaseSnapshot,PreparedQuery,QueryContext,QueryOptions};
use rtdb_sql::{PrepareOptions,prepare};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{LogicalType as L,Result,ScalarType as S,Timestamp,Value as V};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    QueryContext::new(catalog,MemoryBudget::new(1 << 26),Timestamp(0),"UTC",QueryOptions::default(),&CancellationToken::new()).unwrap()
}

async fn query(source: &str,argument: V) -> Result<(L,V)> {
    let context = context();
    let options = PrepareOptions { parameter_types: BTreeMap::from([(0,L::Scalar(S::Transit))]),..Default::default() };
    let Statement::Query(relation) = prepare(source,&context.catalog,&options)?.statement else { panic!("query") };
    let mut cursor = PreparedQuery::new(relation)?.execute(Arc::clone(&context),vec![argument]).await?;
    let ty = cursor.runtime_schema().fields()[0].data_type.clone();
    let batch = cursor.next_batch().await?.unwrap();
    let value = read_value(batch.batch().schema().field(0),batch.batch().column(0).as_ref(),0)?;
    drop(batch);
    assert!(cursor.next_batch().await?.is_none());
    drop(cursor);
    assert_eq!(context.memory.used_bytes(),0);
    Ok((ty,value))
}

#[tokio::test]
async fn sql_field_access_refines_transit_parameters_and_preserves_scalar_null_results() {
    let object = T::Map(vec![(T::Keyword("a".into()),T::Bool(false)),(T::String("a".into()),T::Integer(7))]);
    let (ty,value) = query("SELECT ($1).a",V::Transit(encode_msgpack(&object).unwrap())).await.unwrap();
    assert_eq!(ty,L::Scalar(S::Transit).maybe());
    let V::Transit(bytes) = value else { panic!("Transit") };
    assert_eq!(decode_msgpack(&bytes).unwrap(),T::Integer(7));
    assert_eq!(query("SELECT ($1).a",V::Null).await.unwrap(),(L::Null,V::Null));
    assert_eq!(query("SELECT (1 / 0).a",V::Null).await.unwrap(),(L::Null,V::Null));
    assert_eq!(query("SELECT ({'a': FALSE}).a",V::Null).await.unwrap(),(L::Scalar(S::Bool),V::Bool(false)));
    assert_eq!(query("SELECT ({'a': 1 / 0}).absent",V::Null).await.unwrap_err().code,"xtdb.expression/division-by-zero");
}

#[tokio::test]
async fn sql_transit_fields_construct_registered_children_before_selection() {
    let tag = |name: &str,value|T::Tagged { tag: name.into(),representation: Box::new(value) };
    let object = T::Map(vec![(T::Keyword("a".into()),tag("i8",T::Integer(7)))]);
    let (_,value) = query("SELECT ($1).a",V::Transit(encode_msgpack(&object).unwrap())).await.unwrap();
    let V::Transit(bytes) = value else { panic!("Transit") };
    assert_eq!(decode_msgpack(&bytes).unwrap(),T::Integer(7));
    let object = T::Map(vec![(T::Keyword("a".into()),T::Integer(7)),(T::Keyword("ignored".into()),tag("u",T::String("invalid".into())))]);
    assert!(query("SELECT ($1).a",V::Transit(encode_msgpack(&object).unwrap())).await.is_err());
}

#[tokio::test]
async fn sql_fields_expose_transaction_records_and_stored_error_data() {
    for (tag,representation,field) in [
        ("xtdb/tx-key",T::Map(vec![(T::Keyword("tx-id".into()),T::Integer(9))]),"tx-id"),
        ("xtdb/illegal-arg",T::Array(vec![T::Null,T::Null,T::Map(vec![(T::Keyword("detail".into()),T::Integer(9))])]),"detail"),
    ] {
        let object = T::Tagged { tag: tag.into(),representation: Box::new(representation) };
        let (_,value) = query(&format!("SELECT ($1).\"{field}\""),V::Transit(encode_msgpack(&object).unwrap())).await.unwrap();
        let V::Transit(bytes) = value else { panic!("Transit") };
        assert_eq!(decode_msgpack(&bytes).unwrap(),T::Integer(9));
    }
}
