use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::Schema;
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, parse, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{Result, Timestamp, Value};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: Vec::new(), storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 27), Timestamp(0), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap()
}

async fn query(sql: &str) -> Result<(Schema, Vec<Vec<Value>>)> {
    let context = context();
    let prepared = PreparedQuery::from_prepared(prepare(sql, &context.catalog, &PrepareOptions::default())?, &context.memory)?;
    let schema = prepared.schema().clone();
    let mut cursor = prepared.execute(Arc::clone(&context), Vec::new()).await?;
    let mut rows = Vec::new();
    while let Some(page) = cursor.next_batch().await? {
        let batch = page.batch();
        for row in 0..batch.num_rows() {
            rows.push(batch.schema().fields().iter().zip(batch.columns()).map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?);
        }
    }
    drop(cursor); drop(prepared);
    assert_eq!(context.memory.used_bytes(), 0); assert_eq!(context.spill_bytes(), 0);
    Ok((schema, rows))
}

fn names(schema: &Schema) -> Vec<&str> { schema.fields().iter().map(|field| field.name.as_str()).collect() }

#[tokio::test]
async fn wildcard_exclusion_and_renaming_keep_original_column_order_and_identity() {
    let from = "FROM (VALUES (1,3,'a')) docs(_id,x,y)";
    for (projection, expected_names, expected) in [
        ("* EXCLUDE x", vec!["_id", "y"], vec![Value::I64(1), Value::Utf8("a".into())]),
        ("* EXCLUDE (x,y)", vec!["_id"], vec![Value::I64(1)]),
        ("docs.* EXCLUDE x", vec!["_id", "y"], vec![Value::I64(1), Value::Utf8("a".into())]),
        ("docs.* RENAME y AS new_y", vec!["_id", "x", "new_y"], vec![Value::I64(1), Value::I64(3), Value::Utf8("a".into())]),
        ("* RENAME ignored.public.docs.x AS renamed_x", vec!["_id", "renamed_x", "y"], vec![Value::I64(1), Value::I64(3), Value::Utf8("a".into())]),
        ("* RENAME (x AS y,y AS z)", vec!["_id", "y", "z"], vec![Value::I64(1), Value::I64(3), Value::Utf8("a".into())]),
        ("* EXCLUDE y RENAME (x AS y,y AS z)", vec!["_id", "y"], vec![Value::I64(1), Value::I64(3)]),
        ("* RENAME (x AS first_x,x AS last_x)", vec!["_id", "last_x", "y"], vec![Value::I64(1), Value::I64(3), Value::Utf8("a".into())]),
        ("docs.* RENAME missing AS ignored", vec!["_id", "x", "y"], vec![Value::I64(1), Value::I64(3), Value::Utf8("a".into())]),
        ("* RENAME missing AS ignored", vec!["_id", "x", "y"], vec![Value::I64(1), Value::I64(3), Value::Utf8("a".into())]),
    ] {
        let (schema, rows) = query(&format!("SELECT {projection} {from}")).await.unwrap();
        assert_eq!(names(&schema), expected_names, "{projection}"); assert_eq!(rows, [expected], "{projection}");
    }
}

#[tokio::test]
async fn explicit_names_suppress_original_wildcard_fields_before_rename_and_follow_them() {
    for projection in ["* RENAME x AS z,99 AS x", "99 AS x,* RENAME x AS z"] {
        let (schema, rows) = query(&format!("SELECT {projection} FROM (VALUES (1,2)) t(x,y)")).await.unwrap();
        assert_eq!(names(&schema), ["y", "x"]); assert_eq!(rows, [vec![Value::I64(2), Value::I64(99)]]);
    }
    assert!(query("SELECT * RENAME x AS z,99 AS z FROM (VALUES (1,2)) t(x,y)").await.unwrap_err().message.contains("Duplicate column projection"));
    let (schema, rows) = query("SELECT * EXCLUDE (x,y) FROM (VALUES (1,2)) t(x,y)").await.unwrap();
    assert!(schema.is_empty()); assert_eq!(rows, [Vec::<Value>::new()]);
}

#[tokio::test]
async fn qualified_source_renames_keep_both_join_sides_and_outer_nulls() {
    let from = "FROM ((VALUES (1,3)) foo(_id,x) LEFT JOIN (VALUES (2,'one')) bar(_id,y) ON FALSE)";
    for projection in ["* RENAME (foo._id AS foo_id,bar._id AS bar_id)", "foo.* RENAME _id foo_id,bar.* RENAME _id bar_id"] {
        let (schema, rows) = query(&format!("SELECT {projection} {from}")).await.unwrap();
        assert_eq!(names(&schema), ["foo_id", "x", "bar_id", "y"]);
        assert_eq!(rows, [vec![Value::I64(1), Value::I64(3), Value::Null, Value::Null]]);
        assert!(schema.fields()[2].data_type.nullable()); assert!(schema.fields()[3].data_type.nullable());
    }
    let (schema, rows) = query("SELECT * RENAME (t.x AS first,t.y AS second) FROM (VALUES (1,2)) t(x,y)").await.unwrap();
    assert_eq!(names(&schema), ["first", "second"]); assert_eq!(rows, [vec![Value::I64(1), Value::I64(2)]]);
}

#[tokio::test]
async fn wildcard_renames_preserve_quoted_case_and_literal_dots() {
    let (schema, rows) = query("SELECT * EXCLUDE \"x.y\" RENAME (\"Case\" AS \"Mixed\",x AS \"y.z\") FROM (VALUES (1,2,3)) t(\"Case\",\"x.y\",x)").await.unwrap();
    assert_eq!(names(&schema), ["Mixed", "y.z"]); assert_eq!(rows, [vec![Value::I64(1), Value::I64(3)]]);
    let (schema, rows) = query("SELECT * RENAME \"RENAME\" AS \"FROM\" FROM (VALUES (3)) t(\"RENAME\")").await.unwrap();
    assert_eq!(names(&schema), ["FROM"]); assert_eq!(rows, [vec![Value::I64(3)]]);
    assert_eq!(query("SELECT 2 * \"RENAME\" AS product FROM (VALUES (3)) t(\"RENAME\")").await.unwrap().1, [vec![Value::I64(6)]]);
}

#[tokio::test]
async fn nested_queries_and_correlated_wildcards_use_original_identifier_spans() {
    let (schema, rows) = query("WITH v AS (SELECT * RENAME a.x y FROM (VALUES (7)) a(x)) SELECT v.* RENAME y z FROM v").await.unwrap();
    assert_eq!(names(&schema), ["z"]); assert_eq!(rows, [vec![Value::I64(7)]]);
    assert_eq!(query("SELECT (SELECT * RENAME o.x AS y) AS result FROM (VALUES (7)) o(x)").await.unwrap().1, [vec![Value::I64(7)]]);
    assert_eq!(query("SELECT (SELECT t.* EXCLUDE y RENAME x AS z FROM (VALUES (2)) t(y)) AS result FROM (VALUES (7)) t(x)").await.unwrap().1, [vec![Value::I64(7)]]);
    let parsed = parse("SELECT * RENAME (t.x AS z),$1 AS p FROM (VALUES (7)) t(x)").unwrap();
    assert_eq!(parsed[0].parameter_count(), 1);
}

#[tokio::test]
async fn rename_resolution_precedes_unqualified_exclusion_but_qualified_selection_must_survive() {
    assert_eq!(query("SELECT * EXCLUDE x RENAME x AS z FROM (VALUES (1)) l(x) CROSS JOIN (VALUES (2)) r(x)").await.unwrap_err().code, "xtdb/ambiguous-column");
    assert!(query("SELECT t.* EXCLUDE x RENAME missing AS z FROM (VALUES (1)) t(x)").await.unwrap_err().message.contains("Table not found"));
    assert!(query("SELECT t.* FROM (VALUES (1)) other(x)").await.unwrap_err().message.contains("Table not found"));
}

#[test]
fn wildcard_modifier_grammar_rejects_qualified_exclusions_and_qualified_star_rename_sources() {
    let context = context();
    for projection in ["* EXCLUDE t.x", "t.* RENAME t.x AS z", "* RENAME x AS z EXCLUDE y", "* RENAME x AS z RENAME y AS q", "* EXCLUDE ()", "* RENAME ()", "* RENAME x", "* EXCEPT (x)", "* REPLACE (2 AS x)", "public.t.*"] {
        let sql = format!("SELECT {projection} FROM (VALUES (1,2)) t(x,y)");
        assert!(prepare(&sql, &context.catalog, &PrepareOptions::default()).is_err(), "{sql}");
    }
    let sql = "SELECT t.* RENAME t.x AS z FROM (VALUES (1)) t(x)";
    let error = prepare(sql, &context.catalog, &PrepareOptions::default()).unwrap_err();
    assert_eq!(error.context["span"]["start"], serde_json::json!(sql.find("t.x").unwrap()));
}
