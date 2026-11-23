use rtdb_types::StructFields;
use std::{collections::BTreeMap,sync::Arc};
use rtdb_columnar::{MemoryBudget,read_value};
use rtdb_plan::{BinaryOp,Expr,RelationNode,Statement,UnaryOp};
use rtdb_query::{CatalogSnapshot,DatabaseSnapshot,PreparedQuery,QueryContext,QueryOptions};
use rtdb_sql::{PrepareOptions,prepare};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{LogicalType as L,Result,ScalarType as S,Timestamp,Value as V};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    QueryContext::new(catalog,MemoryBudget::new(1 << 27),Timestamp(0),"UTC",QueryOptions::default(),&CancellationToken::new()).unwrap()
}
async fn query(source: &str,arguments: Vec<V>) -> Result<Vec<Vec<V>>> {
    let context = context();
    let options = PrepareOptions { parameter_types: arguments.iter().enumerate().map(|(index,value)|(index,value.logical_type())).collect(),..Default::default() };
    let prepared = prepare(source,&context.catalog,&options)?;
    let mut cursor = PreparedQuery::from_prepared(prepared,&context.memory)?.execute(Arc::clone(&context),arguments).await?;
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() { rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field,array)|read_value(field,array.as_ref(),row)).collect::<Result<_>>()?); }
    }
    drop(cursor);assert_eq!(context.memory.used_bytes(),0);Ok(rows)
}
fn object() -> V {
    V::Struct(StructFields::from([
        ("age".into(),V::I64(25)),("name".into(),V::Utf8("Alice".into())),("Camel".into(),V::I64(7)),
        ("nested".into(),V::Struct(StructFields::from([("inner".into(),V::I64(42))]))),
        ("items".into(),V::list(vec![V::I64(10),V::Struct(StructFields::from([("answer".into(),V::I64(42))]))])),
    ]))
}

#[tokio::test]
async fn postgres_json_field_and_path_access_preserve_types_spelling_and_nulls() {
    let result = query("SELECT $1->'age', $1->>'age', $1->'nested'->>'inner', $1 #> ARRAY['nested','inner'], $1 #>> ARRAY['name'], $1->'missing', $1->'Camel', $1->'camel'",vec![object()]).await.unwrap();
    assert_eq!(result,[vec![V::I64(25),V::Utf8("25".into()),V::Utf8("42".into()),V::I64(42),V::Utf8("Alice".into()),V::Null,V::I64(7),V::Null]]);
    assert_eq!(query("SELECT $1 #> ARRAY['items',1,'answer'], $1 #>> ARRAY['items',1,'answer'], $1->'items'->0",vec![object()]).await.unwrap(),[vec![V::I64(42),V::Utf8("42".into()),V::I64(10)]]);
    assert_eq!(query("SELECT $1 #> ARRAY[], 123 #>> ARRAY[]",vec![V::I64(7)]).await.unwrap(),[vec![V::I64(7),V::Utf8("123".into())]]);
}

#[tokio::test]
async fn json_integer_access_is_zero_based_and_keeps_existing_nth_bounds() {
    assert_eq!(query("SELECT ARRAY[10,20]->0, ARRAY[10,20]->>1, ARRAY[10,20]->(-1), ARRAY[10,20]->2, ARRAY[10,20]->(-9223372036854775808)",vec![]).await.unwrap(),[vec![V::I64(10),V::Utf8("20".into()),V::Null,V::Null,V::Null]]);
    assert_eq!(query("SELECT $1 -> (+('Camel')), ARRAY[10,20] -> -(-1), $1 #> (+(ARRAY['nested','inner']))",vec![object()]).await.unwrap(),[vec![V::I64(7),V::I64(20),V::I64(42)]]);
    assert_eq!(query("SELECT 1->'x', NULL->'x', 1->0, NULL #>> ARRAY['x']",vec![]).await.unwrap(),[vec![V::Null,V::Null,V::Null,V::Null]]);
}

#[tokio::test]
async fn json_operators_bind_above_unary_and_arithmetic_below_cast_and_subscript() {
    assert_eq!(query("SELECT -ARRAY[3]->0, +ARRAY[3]->0, ARRAY[3]->0*2, 10+ARRAY[3]->0, ARRAY[ARRAY[5]][1]->0",vec![]).await.unwrap(),[vec![V::I64(-3),V::I64(3),V::I64(6),V::I64(13),V::I64(5)]]);
    assert_eq!(query("SELECT ($1->>'age')::bigint + 1, ($1->'items')[1]",vec![object()]).await.unwrap(),[vec![V::I64(26),V::I64(10)]]);
    let context = context();
    let options = PrepareOptions { parameter_types: BTreeMap::from([(0,object().logical_type())]),..Default::default() };
    assert!(prepare("SELECT $1->>'age'::text",&context.catalog,&options).is_err());
    let Statement::Query(relation) = prepare("SELECT -ARRAY[3]->0*2",&context.catalog,&PrepareOptions::default()).unwrap().statement else { panic!() };
    let RelationNode::Project { expressions,.. } = relation.node() else { panic!() };
    let Expr::Binary { op: BinaryOp::Multiply,left,.. } = &expressions[0].expr else { panic!() };
    assert!(matches!(left.as_ref(),Expr::Unary { op: UnaryOp::Negate,expr } if matches!(expr.as_ref(),Expr::Subscript { .. })));
}

#[test]
fn json_path_admission_is_shared_by_normal_and_ignored_setting_expressions() {
    let context = context();
    let options = PrepareOptions { parameter_types: BTreeMap::from([(0,L::Scalar(S::I64))]),..Default::default() };
    for (expression,code) in [
        ("1->$1","xtdb.sql/non-literal-field-access"),("1->(1+1)","xtdb.sql/non-literal-field-access"),
        ("1->1.0","xtdb.sql/non-literal-field-access"),("1->NULL","xtdb.sql/non-literal-field-access"),
        ("1->TRUE","xtdb.sql/non-literal-field-access"),("1->LOWER('x')","xtdb.sql/non-literal-field-access"),
        ("1 #> $1","xtdb.sql/non-literal-json-path"),("1 #> '{x}'","xtdb.sql/non-literal-json-path"),
        ("1 #> ARRAY[$1]","xtdb.sql/non-literal-field-access"),("1 #> ARRAY[NULL]","xtdb.sql/non-literal-field-access"),
        ("1 #>> ARRAY['x',1+1]","xtdb.sql/non-literal-field-access"),("1 #> ARRAY[ARRAY[0]]","xtdb.sql/non-literal-field-access"),
    ] {
        for source in [format!("SELECT {expression}"),format!("SETTING SNAPSHOT_TIME={expression} SELECT 1")] {
            assert_eq!(prepare(&source,&context.catalog,&options).unwrap_err().code,code,"{source}");
        }
    }
}

#[tokio::test]
async fn ignored_setting_json_access_validates_literals_without_evaluating_the_object() {
    assert_eq!(query("SETTING SNAPSHOT_TIME=(1/0)->'x' SELECT 1",vec![]).await.unwrap(),[vec![V::I64(1)]]);
    assert_eq!(query("SETTING SNAPSHOT_TIME=LOWER(1) #> ARRAY['x',0] SELECT 1",vec![]).await.unwrap(),[vec![V::I64(1)]]);
    assert!(query("SETTING SNAPSHOT_TIME=DATE '2020-02-30'->'x' SELECT 1",vec![]).await.is_err());
}

#[tokio::test]
async fn json_access_reuses_transit_field_extraction_and_text_cast() {
    use rtdb_format::transit::{TransitValue as T,encode_msgpack};
    let value = V::Transit(encode_msgpack(&T::Map(vec![(T::Keyword("tx-key".into()),T::Map(vec![(T::Keyword("tx-id".into()),T::Integer(7))]))])).unwrap());
    assert_eq!(query("SELECT $1 #>> ARRAY['tx-key','tx-id'], ($1->'tx-key')->>'tx-id'",vec![value]).await.unwrap(),[vec![V::Utf8("7".into()),V::Utf8("7".into())]]);
}

#[tokio::test]
async fn json_index_and_field_constant_null_outcomes_retain_continuation_demand() {
    assert_eq!(query("SELECT (1/0)->0, (1/0)->'x', ARRAY[1/0]->(-1)",vec![]).await.unwrap(),[vec![V::Null,V::Null,V::Null]]);
    assert_eq!(query("SELECT (CASE WHEN 1/0=1 THEN 1 ELSE 2 END)->0",vec![]).await.unwrap_err().code,"xtdb.expression/division-by-zero");
    assert_eq!(query("SELECT (CASE WHEN TRUE THEN 1/0 ELSE NULL END)->0",vec![]).await.unwrap_err().code,"xtdb.expression/division-by-zero");
}
