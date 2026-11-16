use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{Schema, Statement};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType as T, Result, ScalarType as S, Timestamp, Value as V};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: Vec::new(), storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 27), Timestamp(0), "UTC", QueryOptions { batch_rows: 1, ..Default::default() }, &CancellationToken::new()).unwrap()
}

async fn query(sql: &str) -> Result<(Schema, Vec<Vec<V>>)> {
    let context = context();
    let Statement::Query(relation) = prepare(sql, &context.catalog, &PrepareOptions::default())?.statement else { panic!("Expected query") };
    let schema = relation.schema().clone();
    let mut cursor = PreparedQuery::new(relation)?.execute(context, vec![])?;
    let mut rows = Vec::new();
    while let Some(page) = cursor.next_batch().await? {
        let batch = page.batch();
        for row in 0..batch.num_rows() {
            rows.push(batch.schema().fields().iter().zip(batch.columns()).map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?);
        }
    }
    Ok((schema, rows))
}

fn names(schema: &Schema) -> Vec<&str> { schema.fields().iter().map(|field| field.name.as_str()).collect() }

#[tokio::test]
async fn named_columns_join_retains_both_qualified_types_and_original_left_common_binding() {
    let (schema, rows) = query("SELECT x AS common_key, l.x AS left_key, r.x AS right_key FROM (VALUES (1)) AS l(x) JOIN (VALUES (1.0)) AS r(x) USING (x)").await.unwrap();
    assert_eq!(names(&schema), ["common_key", "left_key", "right_key"]);
    assert_eq!(schema.fields().iter().map(|field| field.data_type.clone()).collect::<Vec<_>>(), [T::Scalar(S::I64), T::Scalar(S::I64), T::Scalar(S::F64)]);
    assert_eq!(rows, [vec![V::I64(1), V::I64(1), V::F64(1.0)]]);
}

#[tokio::test]
async fn named_outer_joins_preserve_pinned_left_only_common_names_without_coalescing() {
    for (join, expected, left_nullable, right_nullable) in [
        ("JOIN", vec![], false, false),
        ("LEFT JOIN", vec![vec![V::I64(1), V::I64(1), V::Null]], false, true),
        ("RIGHT JOIN", vec![vec![V::Null, V::Null, V::I64(2)]], true, false),
        ("FULL JOIN", vec![vec![V::I64(1), V::I64(1), V::Null], vec![V::Null, V::Null, V::I64(2)]], true, true),
    ] {
        let sql = format!("SELECT x AS common_key, l.x AS left_key, r.x AS right_key FROM (VALUES (1)) AS l(x) {join} (VALUES (2)) AS r(x) USING (x) ORDER BY COALESCE(l.x,r.x)");
        let (schema, rows) = query(&sql).await.unwrap();
        assert_eq!(rows, expected, "{join}");
        assert_eq!(schema.fields()[0].data_type.nullable(), left_nullable, "{join}");
        assert_eq!(schema.fields()[1].data_type.nullable(), left_nullable, "{join}");
        assert_eq!(schema.fields()[2].data_type.nullable(), right_nullable, "{join}");
    }
}

#[tokio::test]
async fn natural_join_uses_all_shared_names_and_nulls_do_not_match() {
    let (_, rows) = query("SELECT l.x, l.y FROM (VALUES (1,2),(1,3),(NULL,2)) AS l(x,y) NATURAL JOIN (VALUES (1,2),(2,3),(NULL,2)) AS r(x,y)").await.unwrap();
    assert_eq!(rows, [vec![V::I64(1), V::I64(2)]]);
    let (_, rows) = query("SELECT x AS common_key, r.x AS right_key FROM (VALUES (1)) AS l(x) NATURAL RIGHT JOIN (VALUES (2)) AS r(x)").await.unwrap();
    assert_eq!(rows, [vec![V::Null, V::I64(2)]]);
    let (schema, rows) = query("SELECT * FROM (VALUES (1,2)) AS l(z,a) NATURAL JOIN (VALUES (3),(4)) AS r(b) ORDER BY b").await.unwrap();
    assert_eq!(names(&schema), ["z", "a", "b"]);
    assert_eq!(rows, [vec![V::I64(1), V::I64(2), V::I64(3)], vec![V::I64(1), V::I64(2), V::I64(4)]]);
}

#[tokio::test]
async fn named_join_wildcards_retain_each_side_and_duplicate_outputs_fail() {
    let (schema, rows) = query("SELECT r.* FROM (VALUES (1)) AS l(x) LEFT JOIN (VALUES (2,3)) AS r(x,y) USING(x)").await.unwrap();
    assert_eq!(names(&schema), ["x", "y"]);
    assert!(schema.fields().iter().all(|field| field.data_type.nullable()));
    assert_eq!(rows, [vec![V::Null, V::Null]]);
    let (schema, rows) = query("SELECT *,l.x AS x FROM (VALUES (1,2)) AS l(x,a) JOIN (VALUES (1,3)) AS r(x,b) USING(x)").await.unwrap();
    assert_eq!(names(&schema), ["a", "b", "x"]);
    assert_eq!(rows, [vec![V::I64(2), V::I64(3), V::I64(1)]]);
    let (schema, rows) = query("SELECT l.x AS x,* FROM (VALUES (1,2)) AS l(x,a) JOIN (VALUES (1,3)) AS r(x,b) USING(x)").await.unwrap();
    assert_eq!(names(&schema), ["a", "b", "x"]);
    assert_eq!(rows, [vec![V::I64(2), V::I64(3), V::I64(1)]]);
    for sql in [
        "SELECT * FROM (VALUES (1)) AS l(x) JOIN (VALUES (1)) AS r(x) USING(x)",
        "SELECT l.*,r.* FROM (VALUES (1)) AS l(x) NATURAL JOIN (VALUES (1)) AS r(x)",
        "SELECT 1 AS x,2 AS x",
    ] { assert!(query(sql).await.unwrap_err().message.contains("Duplicate column projection"), "{sql}"); }
}

#[tokio::test]
async fn named_joins_keep_nested_and_quoted_resolution_and_reject_missing_or_ambiguous_keys() {
    assert_eq!(query("SELECT x AS common_key, b.x AS second_key, c.x AS third_key FROM (VALUES (1)) AS a(x) JOIN (VALUES (1)) AS b(x) USING(x,x) JOIN (VALUES (1)) AS c(x) USING(x)").await.unwrap().1,
        [vec![V::I64(1), V::I64(1), V::I64(1)]]);
    assert_eq!(query("SELECT \"K\" AS key FROM (VALUES (1)) AS a(\"K\") JOIN (VALUES (1)) AS b(\"K\") USING(\"K\")").await.unwrap().1, [vec![V::I64(1)]]);
    for sql in [
        "SELECT a.x FROM (VALUES (1)) AS a(x) JOIN (VALUES (1)) AS b(y) USING(x)",
        "SELECT a.x FROM ((VALUES (1)) AS a(x) CROSS JOIN (VALUES (2)) AS b(x)) JOIN (VALUES (1)) AS c(x) USING(x)",
        "SELECT y FROM (VALUES (1,2)) AS a(x,y) JOIN (VALUES (1,3)) AS b(x,y) USING(x)",
    ] { assert!(query(sql).await.is_err(), "{sql}"); }
}

#[tokio::test]
async fn order_by_nonprojected_inputs_preserves_only_requested_output() {
    let (schema, rows) = query("SELECT label FROM (VALUES ('b',2),('a',1),('c',3)) AS t(label,rank) ORDER BY rank DESC").await.unwrap();
    assert_eq!(names(&schema), ["label"]);
    assert_eq!(rows, [vec![V::Utf8("c".into())], vec![V::Utf8("b".into())], vec![V::Utf8("a".into())]]);
    assert_eq!(query("SELECT x+1 AS y FROM (VALUES (3),(1),(2)) AS t(x) ORDER BY x").await.unwrap().1, [vec![V::I64(2)], vec![V::I64(3)], vec![V::I64(4)]]);
    assert_eq!(query("SELECT x+1 AS y FROM (VALUES (3),(1),(2)) AS t(x) ORDER BY y*2").await.unwrap().1, [vec![V::I64(2)], vec![V::I64(3)], vec![V::I64(4)]]);
}

#[tokio::test]
async fn order_by_aliases_shadow_inputs_inside_expressions_but_qualified_inputs_remain_available() {
    let input = "FROM (VALUES (1),(2),(3)) AS t(x)";
    assert_eq!(query(&format!("SELECT -x AS x {input} ORDER BY x+1")).await.unwrap().1, [vec![V::I64(-3)], vec![V::I64(-2)], vec![V::I64(-1)]]);
    assert_eq!(query(&format!("SELECT -x AS x {input} ORDER BY t.x")).await.unwrap().1, [vec![V::I64(-1)], vec![V::I64(-2)], vec![V::I64(-3)]]);
    assert_eq!(query(&format!("SELECT -x AS x {input} ORDER BY 1")).await.unwrap().1, [vec![V::I64(-3)], vec![V::I64(-2)], vec![V::I64(-1)]]);
}

#[tokio::test]
async fn order_by_subqueries_capture_nonprojected_and_qualified_inputs() {
    assert_eq!(query("SELECT label FROM (VALUES ('b',2),('a',1)) AS t(label,rank) ORDER BY (SELECT t.rank)").await.unwrap().1,
        [vec![V::Utf8("a".into())], vec![V::Utf8("b".into())]]);
    assert_eq!(query("SELECT (SELECT -t.x) AS value FROM (VALUES (2),(1)) AS t(x) ORDER BY t.x").await.unwrap().1, [vec![V::I64(-1)], vec![V::I64(-2)]]);
}

#[tokio::test]
async fn distinct_follows_ordering_and_does_not_include_nonprojected_sort_values() {
    assert_eq!(query("SELECT DISTINCT label FROM (VALUES ('a',3),('a',2),('b',1)) AS t(label,rank) ORDER BY rank").await.unwrap().1,
        [vec![V::Utf8("b".into())], vec![V::Utf8("a".into())]]);
    assert_eq!(query("SELECT DISTINCT label FROM (VALUES ('a',3),('a',2),('b',1)) AS t(label,rank) ORDER BY rank DESC").await.unwrap().1,
        [vec![V::Utf8("a".into())], vec![V::Utf8("b".into())]]);
}

#[tokio::test]
async fn grouped_order_by_uses_group_expressions_and_projected_aliases() {
    assert_eq!(query("SELECT COUNT(*) AS n FROM (VALUES (2),(1),(1)) AS t(x) GROUP BY x ORDER BY x").await.unwrap().1, [vec![V::I64(2)], vec![V::I64(1)]]);
    assert_eq!(query("SELECT UPPER(category) AS cat,COUNT(*) AS n FROM (VALUES ('b'),('a'),('a')) AS t(category) GROUP BY UPPER(category) ORDER BY UPPER(category)").await.unwrap().1,
        [vec![V::Utf8("A".into()), V::I64(2)], vec![V::Utf8("B".into()), V::I64(1)]]);
    assert_eq!(query("SELECT -x AS x,COUNT(*) AS n FROM (VALUES (2),(1)) AS t(x) GROUP BY x ORDER BY x").await.unwrap().1,
        [vec![V::I64(-2), V::I64(1)], vec![V::I64(-1), V::I64(1)]]);
    for sql in ["SELECT COUNT(*) AS n FROM (VALUES (1)) AS t(x) ORDER BY COUNT(*)", "SELECT 1 AS x ORDER BY 0", "SELECT 1 AS x ORDER BY 2", "SELECT x FROM (VALUES (1,2)) AS t(x,y) UNION ALL SELECT x FROM (VALUES (2,1)) AS t(x,y) ORDER BY y"] {
        assert!(query(sql).await.is_err(), "{sql}");
    }
}
