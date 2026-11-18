use std::{collections::BTreeMap,sync::Arc};
use rtdb_columnar::{MemoryBudget,read_value};
use rtdb_plan::Statement;
use rtdb_query::{CatalogSnapshot,DatabaseSnapshot,PreparedQuery,QueryContext,QueryOptions};
use rtdb_sql::{PrepareOptions,prepare};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{ListKind,LogicalType as L,Result,ScalarType as S,Timestamp,Value as V};
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
async fn emitted_byte_buffer_equality_applies_to_operators_and_nested_values() {
    let keyword = V::Keyword(rtdb_types::Keyword::from_name("ns/medusa"));
    for value in [V::Utf8("ns/medusa".into()), V::Binary(b"ns/medusa".to_vec()), V::FixedSizeBinary(b"ns/medusa".to_vec())] {
        assert_eq!(query("SELECT $1 = $2, $2 = $1, $1 <> $2, $1 IS DISTINCT FROM $2, ARRAY[$1] = ARRAY[$2]", vec![keyword.clone(), value]).await.unwrap(),
            [vec![V::Bool(true), V::Bool(true), V::Bool(false), V::Bool(false), V::Bool(true)]]);
    }
    assert_eq!(query("SELECT KEYWORD 'ns/medusa' = 'medusa', KEYWORD 'medusa' = ':medusa', KEYWORD 'medusa' = NULL", vec![]).await.unwrap(),
        [vec![V::Bool(false), V::Bool(false), V::Null]]);
    assert_eq!(query("SELECT UUID '61626364-6566-6768-696a-6b6c6d6e6f70' = 'abcdefghijklmnop', UUID '61626364-6566-6768-696a-6b6c6d6e6f70' = KEYWORD 'abcdefghijklmnop'", vec![]).await.unwrap(),
        [vec![V::Bool(true), V::Bool(true)]]);
}

#[tokio::test]
async fn emitted_byte_buffers_keep_their_types_through_hash_join_and_membership() {
    let keyword = V::Keyword(rtdb_types::Keyword::from_name("abc"));
    assert_eq!(query("SELECT a.id AS keyword_id, b.id AS string_id FROM (VALUES (KEYWORD 'abc')) a(id) JOIN (VALUES ('abc')) b(id) ON a.id = b.id", vec![]).await.unwrap(),
        [vec![keyword.clone(), V::Utf8("abc".into())]]);
    for source in [
        "SELECT a.id FROM (VALUES (KEYWORD 'abc')) a(id) WHERE a.id IN ('abc')",
        "SELECT a.id FROM (VALUES (KEYWORD 'abc')) a(id) WHERE a.id IN (VALUES ('abc'))",
    ] {
        assert_eq!(query(source, vec![]).await.unwrap(), [vec![keyword.clone()]], "{source}");
    }
}

#[tokio::test]
async fn byte_buffer_predicates_and_vector_ordering_follow_their_respective_type_rules() {
    assert_eq!(query("SELECT KEYWORD 'é' < 'z', 'é' < KEYWORD 'z', KEYWORD 'é' < KEYWORD 'z', 'é' < 'z'", vec![]).await.unwrap(),
        [vec![V::Bool(true), V::Bool(true), V::Bool(false), V::Bool(false)]]);
    assert_eq!(query("SELECT COUNT(DISTINCT id) FROM (VALUES (KEYWORD 'abc'), ('abc')) a(id)", vec![]).await.unwrap(), [vec![V::I64(1)]]);
    assert_eq!(query("SELECT id FROM (VALUES (KEYWORD 'a'), ('b')) a(id) ORDER BY id", vec![]).await.unwrap_err().code,
        "xtdb.expression/function-type-mismatch");
}

#[tokio::test]
async fn text_to_varbinary_uses_hexadecimal_digits_and_reports_utf16_offsets() {
    assert_eq!(query("SELECT CAST('616263' AS VARBINARY) = KEYWORD 'abc', CAST('0AfF' AS BYTEA), CAST('' AS VARBINARY), CAST('０１' AS VARBINARY), CAST('١٠' AS VARBINARY), CAST('Ａｆ' AS VARBINARY)", vec![]).await.unwrap(),
        [vec![V::Bool(true), V::Binary(vec![0x0a, 0xff]), V::Binary(vec![]), V::Binary(vec![1]), V::Binary(vec![16]), V::Binary(vec![0xaf])]]);
    assert_eq!(query("SELECT CAST(CAST('616263' AS VARBINARY) AS TEXT)", vec![]).await.unwrap(),
        [vec![V::Utf8("\\x616263".into())]]);
    for (input, expected) in [
        ("0", "Odd number of characters 1."),
        ("0g", "Illegal hexadecimal character 0x67 at index 1."),
        ("é0", "Illegal hexadecimal character 0xE9 at index 0."),
        ("😀", "Illegal hexadecimal character 0x3D at index 0."),
    ] {
        let error = query("SELECT CAST($1 AS VARBINARY)", vec![V::Utf8(input.into())]).await.unwrap_err();
        assert_eq!(error.code, "xtdb.error/unknown");
        assert_eq!(error.message.as_deref(), Some(expected));
    }
}

#[tokio::test]
async fn sql_equality_rejects_runtime_set_parameters_and_empty_declared_set_lists() {
    for source in ["SELECT $1 = $2","SELECT $1 <> $2","SELECT $1 IS DISTINCT FROM $2","SELECT 1 WHERE $1 = $2","SELECT CASE WHEN FALSE THEN $1 = $2 ELSE TRUE END"] {
        assert_eq!(query(source,vec![V::Set(vec![]),V::Set(vec![])]).await.unwrap_err().code,"xtdb.expression/set-equality","{source}");
    }
    let context = context();
    let element = L::List { kind: ListKind::Set,element: Box::new(L::Scalar(S::I64)) };
    let options = PrepareOptions { parameter_types: BTreeMap::from([(0,L::list(element))]),..Default::default() };
    assert!(prepare("SELECT $1 = $1",&context.catalog,&options).is_err());
    assert_eq!(query("SELECT $1 = NULL",vec![V::Set(vec![])]).await.unwrap(),[vec![V::Null]]);
}
#[tokio::test]
async fn implicit_set_equality_rejects_the_first_row_but_preserves_empty_inputs_and_masks() {
    for source in [
        "SELECT DISTINCT $1 AS v",
        "SELECT COUNT(*) FROM (SELECT $1 AS v) q GROUP BY v",
        "SELECT COUNT(DISTINCT v) FROM (SELECT $1 AS v) q",
        "SELECT ROW_NUMBER() OVER (PARTITION BY v) FROM (SELECT $1 AS v) q",
        "SELECT $1 AS v INTERSECT ALL SELECT $1 AS v",
        "SELECT $1 AS v EXCEPT ALL SELECT $1 AS v",
    ] { assert_eq!(query(source,vec![V::Set(vec![])]).await.unwrap_err().code,"xtdb.expression/set-equality","{source}"); }
    assert_eq!(query("SELECT DISTINCT v FROM (SELECT $1 AS v) q WHERE FALSE",vec![V::Set(vec![])]).await.unwrap(),Vec::<Vec<V>>::new());
    assert_eq!(query("SELECT COUNT(DISTINCT v) FILTER (WHERE FALSE) FROM (SELECT $1 AS v) q",vec![V::Set(vec![])]).await.unwrap(),[vec![V::I64(0)]]);
}

#[tokio::test]
async fn refined_set_equality_is_admitted_in_unselected_expression_children() {
    for source in [
        "SELECT CASE WHEN TRUE THEN 1 WHEN $1 = $2 THEN 2 ELSE 3 END",
        "SELECT NULL[CASE WHEN TRUE THEN 1 WHEN $1 = $2 THEN 2 ELSE 3 END]",
        "SELECT NULL LIKE CASE WHEN TRUE THEN '%' WHEN $1 = $2 THEN '_' ELSE 'x' END",
        "SELECT * FROM information_schema.tables FOR VALID_TIME AS OF CASE WHEN TRUE THEN TIMESTAMP '1970-01-01T00:00:00Z' WHEN $1 = $2 THEN TIMESTAMP '1970-01-02T00:00:00Z' ELSE NULL END",
        "SELECT * FROM information_schema.tables FOR SYSTEM_TIME AS OF CASE WHEN TRUE THEN TIMESTAMP '1970-01-01T00:00:00Z' WHEN $1 = $2 THEN TIMESTAMP '1970-01-02T00:00:00Z' ELSE NULL END",
    ] { assert_eq!(query(source,vec![V::Set(vec![]),V::Set(vec![])]).await.unwrap_err().code,"xtdb.expression/set-equality","{source}"); }
}
