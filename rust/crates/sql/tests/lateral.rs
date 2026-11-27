use rtdb_types::StructFields;
use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_index::{PartitionState, TransactionBuffer};
use rtdb_plan::{ApplyKind, Relation, RelationNode, Statement};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{AnomalyCategory, Interval, Result, TableRef, Timestamp, TransactionKey, TxId, Value};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let memory = MemoryBudget::new(128 << 20);
    let state = PartitionState::empty(Default::default()).unwrap();
    let mut transaction = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(0), system_time: Timestamp(100) }, memory.clone()).unwrap();
    for (table, rows) in [
        ("x", vec![vec![("_id", Value::I64(1)), ("x", Value::I64(1)), ("y", Value::I64(1))]]),
        ("z", vec![vec![("_id", Value::I64(1)), ("x", Value::I64(1)), ("z", Value::I64(1))]]),
        ("y", vec![vec![("_id", Value::I64(1)), ("b", Value::Utf8("one".into()))]]),
        ("nested_table", vec![vec![("_id", Value::I64(1)), ("nest", Value::list(vec![Value::I64(1), Value::I64(2), Value::I64(3)]))]]),
        ("other_table", (1..=3).map(|id| vec![("_id", Value::I64(id))]).collect()),
        ("actors", [
            ("bob", "Bob", vec!["The Great Escape", "Bob the Builder", "Bob the Builder Electric Boogaloo"]),
            ("steve", "Steve", vec!["Hot Fuzz", "Shaun of the Dead"]),
        ].into_iter().map(|(id, name, films)| vec![
            ("_id", Value::Utf8(id.into())), ("name", Value::Utf8(name.into())),
            ("films", Value::list(films.into_iter().map(|film| Value::Utf8(film.into())).collect())),
        ]).collect()),
        ("docs", vec![vec![("_id", Value::I64(1)), ("xs", Value::list(vec![Value::I64(2)])), ("ys", Value::list(vec![Value::I64(3)]))]]),
        ("foo", vec![vec![("_id", Value::I64(1)), ("vs", Value::list((1..=2).map(|i| Value::Struct(StructFields::from([
            ("t".into(), Value::I64(i)), ("v".into(), Value::I64(i)),
        ]))).collect()))]]),
    ] {
        for row in rows { transaction.put(TableRef::new("public", table), row.into_iter().map(|(name, value)| (name.into(), value)).collect(), Interval::ALL).unwrap(); }
    }
    state.apply(&transaction.stage().unwrap()).unwrap();
    let database = DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, memory, Timestamp(100), "UTC", QueryOptions { batch_rows: 1, ..Default::default() }, &CancellationToken::new()).unwrap()
}

async fn query(sql: &str, parameters: Vec<Value>) -> Result<Vec<Vec<Value>>> {
    let context = context();
    let options = PrepareOptions { parameter_types: parameters.iter().enumerate().map(|(index, value)| (index, value.logical_type())).collect(), ..Default::default() };
    let prepared = prepare(sql, &context.catalog, &options)?;
    let Statement::Query(relation) = prepared.statement else { panic!("Expected query") };
    let mut cursor = PreparedQuery::new(relation)?.execute(context, parameters).await?;
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() {
            rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field, column)| read_value(field, column.as_ref(), row)).collect::<Result<_>>()?);
        }
    }
    Ok(rows)
}

#[tokio::test]
async fn lateral_derived_table_and_join_lateral_preserve_original_query_forms() {
    for sql in [
        "SELECT x.y, y.z FROM x, LATERAL (SELECT z.z FROM z WHERE z.z = x.y) AS y",
        "SELECT x.y, y.z FROM x CROSS JOIN LATERAL (SELECT z.z FROM z WHERE z.z = x.y) AS y",
        "SELECT x.y, y.z FROM x LEFT JOIN LATERAL (SELECT z.z FROM z WHERE z.z = x.y) AS y ON TRUE",
    ] { assert_eq!(query(sql, vec![]).await.unwrap(), vec![vec![Value::I64(1), Value::I64(1)]], "{sql}"); }
    assert_eq!(query("SELECT y.z FROM LATERAL (SELECT z.z FROM z WHERE z.z = 1) AS y", vec![]).await.unwrap(), vec![vec![Value::I64(1)]]);
    assert_eq!(query("SELECT x, x.y, y.z FROM x NATURAL JOIN LATERAL (SELECT x, z.z FROM z WHERE z.z = x.y) AS y", vec![]).await.unwrap(), vec![vec![Value::I64(1), Value::I64(1), Value::I64(1)]]);
    assert_eq!(query("SELECT nt._id nid, o._id oid FROM nested_table nt CROSS JOIN LATERAL (SELECT * FROM other_table o WHERE o._id = nt._id) o", vec![]).await.unwrap(), vec![vec![Value::I64(1), Value::I64(1)]]);
}

#[tokio::test]
async fn select_star_lateral_join_and_lateral_with_unnest_4009_keep_aliases_and_arrays() {
    assert_eq!(query("FROM UNNEST(ARRAY[1,2]) AS u(v)", vec![]).await.unwrap(), vec![vec![Value::I64(1)], vec![Value::I64(2)]]);
    assert_eq!(query("SELECT FROM (VALUES (1),(2)) AS t(v)", vec![]).await.unwrap(), vec![vec![],vec![]]);
    assert_eq!(query("SELECT * FROM (SELECT y.b FROM y) AS x (x_b), LATERAL (SELECT y.b AS z_b, x_b FROM y) AS z (z_b, outer_b)", vec![]).await.unwrap(), vec![vec![Value::Utf8("one".into()), Value::Utf8("one".into()), Value::Utf8("one".into())]]);
    let array = Value::list(vec![Value::I64(1), Value::I64(2), Value::I64(3)]);
    assert_eq!(query("SELECT * FROM nested_table nt, LATERAL (SELECT nt.nest) t(a)", vec![]).await.unwrap(), vec![vec![Value::I64(1), array.clone(), array.clone()]]);
    assert_eq!(query("SELECT * FROM nested_table nt, LATERAL (FROM UNNEST(nt.nest) AS foo(id)) t(a)", vec![]).await.unwrap(), (1..=3).map(|id| vec![Value::I64(1), array.clone(), Value::I64(id)]).collect::<Vec<_>>());
    assert_eq!(query("SELECT nt._id, value, ordinal FROM nested_table nt, UNNEST(nt.nest) WITH ORDINALITY AS items(value, ordinal)", vec![]).await.unwrap(), (1..=3).map(|id| vec![Value::I64(1), Value::I64(id), Value::I32(id as i32)]).collect::<Vec<_>>());
}

#[tokio::test]
async fn left_lateral_filters_before_null_extension_and_preserves_many_rows() {
    for suffix in ["WHERE y = l.x) AS r ON TRUE", ") AS r ON r.y = l.x", ") AS r ON EXISTS (SELECT 1 WHERE r.y = l.x)"] {
        let sql = format!("SELECT l.x, r.y FROM (VALUES (1), (2)) AS l(x) LEFT JOIN LATERAL (SELECT y FROM (VALUES (2), (2), (3)) AS t(y) WHERE l.x > 0 {}", suffix.replace("WHERE y", "AND y"));
        let mut actual = query(&sql, vec![]).await.unwrap();
        let mut expected = vec![vec![Value::I64(1), Value::Null], vec![Value::I64(2), Value::I64(2)], vec![Value::I64(2), Value::I64(2)]];
        actual.sort_by_key(|row| format!("{row:?}"));
        expected.sort_by_key(|row| format!("{row:?}"));
        assert_eq!(actual, expected, "{sql}");
    }
    assert_eq!(query("SELECT l.x, r.y FROM (VALUES (1), (2)) AS l(x) LEFT JOIN LATERAL (SELECT l.x AS y WHERE FALSE) AS r ON TRUE", vec![]).await.unwrap(), vec![vec![Value::I64(1), Value::Null], vec![Value::I64(2), Value::Null]]);
    assert_eq!(query("SELECT l.x, r.y FROM (VALUES (1), (2)) AS l(x) CROSS JOIN LATERAL (SELECT l.x AS y WHERE FALSE) AS r", vec![]).await.unwrap(), Vec::<Vec<Value>>::new());
}

#[tokio::test]
async fn named_lateral_joins_keep_common_columns_and_qualified_copies() {
    for join in ["JOIN", "LEFT JOIN", "NATURAL JOIN", "NATURAL LEFT JOIN"] {
        let condition = if join.starts_with("NATURAL") { "" } else { " USING (x)" };
        let sql = format!("SELECT x AS common_x, l.x AS left_x, r.x AS right_x FROM (VALUES (1), (2)) AS l(x) {join} LATERAL (SELECT l.x AS x WHERE l.x = 2) AS r{condition}");
        let mut expected = vec![vec![Value::I64(2), Value::I64(2), Value::I64(2)]];
        if join.contains("LEFT") { expected.insert(0, vec![Value::I64(1), Value::I64(1), Value::Null]); }
        assert_eq!(query(&sql, vec![]).await.unwrap(), expected, "{sql}");
    }
}

#[tokio::test]
async fn lateral_scopes_follow_from_order_and_remain_lexical_across_nested_queries() {
    for sql in [
        "SELECT c.z FROM (VALUES (2)) AS a(x), LATERAL (SELECT a.x + 1 AS y) AS b, LATERAL (SELECT a.x + b.y AS z) AS c",
        "SELECT c.z FROM (VALUES (2)) AS a(x), (VALUES (3)) AS b(y) JOIN LATERAL (SELECT a.x + b.y AS z) AS c ON TRUE",
        "SELECT c.z FROM (VALUES (2)) AS a(x), LATERAL (WITH v AS (SELECT a.x AS x) SELECT (SELECT v.x + 3) AS z FROM v) AS c",
        "SELECT (SELECT r.z FROM LATERAL (SELECT a.x + 3 AS z) AS r) FROM (VALUES (2)) AS a(x)",
    ] { assert_eq!(query(sql, vec![]).await.unwrap(), vec![vec![Value::I64(5)]], "{sql}"); }
    assert_eq!(query("SELECT r.z FROM (VALUES (2)) AS a(x), LATERAL (SELECT t.x AS z FROM (VALUES (9)) AS t(x)) AS r", vec![]).await.unwrap(), vec![vec![Value::I64(9)]]);
    for sql in [
        "SELECT * FROM (VALUES (1)) AS a(x), (SELECT a.x) AS r",
        "SELECT * FROM (VALUES (1)) AS a(x), (SELECT * FROM LATERAL (SELECT a.x) AS inner_r) AS r",
        "SELECT * FROM LATERAL (SELECT later.x) AS r, (VALUES (1)) AS later(x)",
        "SELECT * FROM (VALUES (1)) AS a(x), (VALUES (2)) AS b(y) CROSS JOIN LATERAL (SELECT a.x) AS r",
        "SELECT (SELECT r.z FROM (VALUES (3)) AS b(y) CROSS JOIN LATERAL (SELECT a.x + b.y AS z) AS r) FROM (VALUES (2)) AS a(x)",
    ] { assert!(query(sql, vec![]).await.is_err(), "{sql}"); }
}

#[tokio::test]
async fn lateral_captures_keep_external_parameter_arity_and_empty_input_short_circuit() {
    let sql = "SELECT a.x, b.y FROM (VALUES (?)) AS a(x) CROSS JOIN LATERAL (SELECT ? + a.x AS y) AS b";
    assert_eq!(query(sql, vec![Value::I64(2), Value::I64(10)]).await.unwrap(), vec![vec![Value::I64(2), Value::I64(12)]]);
    let context = context();
    let options = PrepareOptions { parameter_types: BTreeMap::from([(0, Value::I64(0).logical_type()), (1, Value::I64(0).logical_type())]), ..Default::default() };
    let prepared = prepare(sql, &context.catalog, &options).unwrap();
    assert_eq!(prepared.parameter_types.len(), 2);
    let Statement::Query(relation) = prepared.statement else { panic!("Expected query") };
    assert_eq!(relation.parameter_count(), 2);
    fn contains_apply(relation: &Relation) -> bool { matches!(relation.node(), RelationNode::Apply { kind: ApplyKind::Inner, .. }) || relation.children().iter().any(|child| contains_apply(child)) }
    assert!(contains_apply(&relation));
    assert_eq!(query("SELECT r.y FROM (SELECT 1 AS x WHERE FALSE) AS l CROSS JOIN LATERAL (SELECT l.x / 0 AS y) AS r", vec![]).await.unwrap(), Vec::<Vec<Value>>::new());
}

#[tokio::test]
async fn lateral_cursor_cancellation_releases_dependent_state_but_retains_returned_batches() {
    let context = context();
    let baseline = context.memory.used_bytes();
    let Statement::Query(relation) = prepare("SELECT r.y FROM (VALUES (1), (2)) AS l(x) CROSS JOIN LATERAL (SELECT l.x + y AS y FROM (VALUES (10), (20)) AS t(y)) AS r", &context.catalog, &PrepareOptions::default()).unwrap().statement else { panic!("Expected query") };
    let prepared = PreparedQuery::new(relation).unwrap();
    let mut cursor = prepared.execute(context.clone(), vec![]).await.unwrap();
    let batch = cursor.next_batch().await.unwrap().unwrap();
    let retained = batch.batch().clone();
    drop(batch);
    cursor.cancel();
    assert_eq!(cursor.next_batch().await.unwrap_err().category, AnomalyCategory::Interrupted);
    drop(cursor);
    assert!(context.memory.used_bytes() > baseline);
    assert_eq!(read_value(retained.schema().field(0), retained.column(0).as_ref(), 0).unwrap(), Value::I64(11));
    drop(retained);
    assert_eq!(context.memory.used_bytes(), baseline);
    let mut next = prepared.execute(context.clone(), vec![]).await.unwrap();
    assert!(next.next_batch().await.unwrap().is_some());
}

#[tokio::test]
async fn uncorrelated_lateral_right_and_full_joins_keep_ordinary_outer_join_results() {
    assert_eq!(query("SELECT l.x, r.y FROM (VALUES (1)) AS l(x) RIGHT JOIN LATERAL (SELECT 2 AS y) AS r ON FALSE", vec![]).await.unwrap(), vec![vec![Value::Null, Value::I64(2)]]);
    let rows = query("SELECT l.x, r.y FROM (VALUES (1)) AS l(x) FULL JOIN LATERAL (SELECT 2 AS y) AS r ON FALSE", vec![]).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.contains(&vec![Value::I64(1), Value::Null]));
    assert!(rows.contains(&vec![Value::Null, Value::I64(2)]));
    for join in ["RIGHT JOIN", "FULL JOIN"] {
        assert!(query(&format!("SELECT * FROM (VALUES (1)) AS l(x) {join} LATERAL (SELECT l.x AS y) AS r ON TRUE"), vec![]).await.is_err());
    }
    assert!(query("SELECT * FROM LATERAL (SELECT 1)", vec![]).await.is_err());
}

#[test]
fn recursive_ctes_retain_the_reference_unsupported_anomaly() {
    let context = context();
    for sql in ["WITH RECURSIVE c AS (SELECT 1 AS x) SELECT * FROM c", "WITH RECURSIVE c AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM c WHERE x < 3) SELECT * FROM c"] {
        let error = prepare(sql, &context.catalog, &PrepareOptions::default()).unwrap_err();
        assert_eq!(error.category, AnomalyCategory::Unsupported);
        assert_eq!(error.code, "xtdb.sql/recursive-ctes");
        assert_eq!(error.message.as_deref(), Some("Recursive CTEs are not supported yet"));
    }
}

#[tokio::test]
async fn unnest_ordinal_width_follows_the_bound_table_and_cross_apply_shapes() {
    let cases = [
        (
            "SELECT v,n FROM UNNEST(ARRAY[1,2]) WITH ORDINALITY AS u(v,n)",
            vec![vec![Value::I64(1),Value::I64(1)],vec![Value::I64(2),Value::I64(2)]],
            rtdb_types::ScalarType::I64,
        ),
        (
            "SELECT k,v,n FROM (VALUES (1),(2)) AS t(k) CROSS JOIN UNNEST(ARRAY[1,2]) WITH ORDINALITY AS u(v,n)",
            vec![vec![Value::I64(1),Value::I64(1),Value::I64(1)],vec![Value::I64(1),Value::I64(2),Value::I64(2)],
                 vec![Value::I64(2),Value::I64(1),Value::I64(1)],vec![Value::I64(2),Value::I64(2),Value::I64(2)]],
            rtdb_types::ScalarType::I64,
        ),
        (
            "SELECT k,v,n FROM (VALUES (1),(2)) AS t(k) CROSS JOIN UNNEST(ARRAY[k,k+1]) WITH ORDINALITY AS u(v,n)",
            vec![vec![Value::I64(1),Value::I64(1),Value::I32(1)],vec![Value::I64(1),Value::I64(2),Value::I32(2)],
                 vec![Value::I64(2),Value::I64(2),Value::I32(1)],vec![Value::I64(2),Value::I64(3),Value::I32(2)]],
            rtdb_types::ScalarType::I32,
        ),
        (
            "SELECT k,v,n FROM (VALUES (1),(2)) AS t(k), LATERAL (SELECT v,n FROM UNNEST(ARRAY[t.k]) WITH ORDINALITY AS u(v,n)) AS u",
            vec![vec![Value::I64(1),Value::I64(1),Value::I64(1)],vec![Value::I64(2),Value::I64(2),Value::I64(1)]],
            rtdb_types::ScalarType::I64,
        ),
    ];
    let context = context();
    for (sql, expected, ordinal) in cases {
        let prepared = prepare(sql, &context.catalog, &PrepareOptions::default()).unwrap();
        let query_plan = PreparedQuery::from_prepared(prepared, &context.memory).unwrap();
        let expected_type = rtdb_types::LogicalType::Scalar(ordinal);
        assert_eq!(query_plan.schema().fields().last().unwrap().data_type, expected_type, "{sql}");
        let mut cursor = query_plan.execute(Arc::clone(&context), vec![]).await.unwrap();
        assert_eq!(cursor.runtime_schema().fields().last().unwrap().data_type, expected_type, "{sql}");
        let mut actual = Vec::new();
        while let Some(batch) = cursor.next_batch().await.unwrap() {
            for row in 0..batch.batch().num_rows() {
                actual.push(batch.batch().schema().fields().iter().zip(batch.batch().columns())
                    .map(|(field, column)| read_value(field, column.as_ref(), row).unwrap()).collect::<Vec<_>>());
            }
        }
        assert_eq!(actual, expected, "{sql}");
    }
}

#[tokio::test]
async fn captured_unnest_reads_a_staged_column_and_preserves_external_parameter_ids() {
    let sql = "SELECT v, n FROM (VALUES (?)) AS t(k), UNNEST(ARRAY[k, k+?]) WITH ORDINALITY AS u(v,n)";
    let context = context();
    let value_type = Value::I64(0).logical_type();
    let options = PrepareOptions { parameter_types: BTreeMap::from([(0, value_type.clone()), (1, value_type)]), ..Default::default() };
    let prepared = prepare(sql, &context.catalog, &options).unwrap();
    assert_eq!(prepared.parameter_types.len(), 2);
    let Statement::Query(relation) = prepared.statement else { panic!("Expected query") };
    assert_eq!(relation.parameter_count(), 2);
    fn unnest(relation: &Relation) -> Option<&RelationNode> {
        if matches!(relation.node(), RelationNode::Unnest { .. }) { return Some(relation.node()); }
        relation.children().into_iter().find_map(|child| unnest(child))
    }
    let Some(RelationNode::Unnest { input, expr, .. }) = unnest(&relation) else { panic!("Expected staged UNNEST") };
    let RelationNode::Extend { expressions, .. } = input.node() else { panic!("Expected column extension") };
    assert_eq!(expressions.len(), 1);
    assert_eq!(expr, &expressions[0].field.expression());
    assert_eq!(expressions[0].expr.parameter_count(), 2);
    assert_eq!(expressions[0].expr.referenced_columns().len(), 1);
    assert_eq!(query(sql, vec![Value::I64(2), Value::I64(10)]).await.unwrap(), [vec![Value::I64(2),Value::I32(1)],vec![Value::I64(12),Value::I32(2)]]);
}

#[tokio::test]
async fn unnest_completes_its_reference_materialization_boundary_before_limit() {
    let context = context();
    let baseline = context.memory.used_bytes();
    let standalone = "SELECT v FROM UNNEST(ARRAY[1,1/0]) AS u(v) LIMIT 1";
    let captured = "SELECT v FROM (VALUES (1),(0)) AS t(k),UNNEST(ARRAY[1/k]) AS u(v) LIMIT 1";
    let prepare_query = |sql| PreparedQuery::from_prepared(prepare(sql, &context.catalog, &PrepareOptions::default()).unwrap(), &context.memory).unwrap();
    let query_plan = prepare_query(standalone);
    assert!(query_plan.execute(Arc::clone(&context), vec![]).await.is_err());
    drop(query_plan);
    assert_eq!(context.memory.used_bytes(), baseline);
    let query_plan = prepare_query(captured);
    let mut cursor = query_plan.execute(Arc::clone(&context), vec![]).await.unwrap();
    assert_eq!(cursor.next_batch().await.unwrap_err().code, "xtdb.expression/division-by-zero");
    drop(cursor);
    drop(query_plan);
    assert_eq!(context.memory.used_bytes(), baseline);
    assert!(query("SELECT v FROM (SELECT 1 AS k WHERE FALSE) AS t, UNNEST(ARRAY[k/0]) AS u(v)", vec![]).await.unwrap().is_empty());
}

#[tokio::test]
async fn actor_unnest_preserves_both_original_runtime_assertions() {
    let expected = [
        ("Bob", "The Great Escape", 1),
        ("Bob", "Bob the Builder", 2),
        ("Bob", "Bob the Builder Electric Boogaloo", 3),
        ("Steve", "Hot Fuzz", 1),
        ("Steve", "Shaun of the Dead", 2),
    ];
    assert_eq!(query("SELECT name, film FROM actors, UNNEST(films) AS films(film)", vec![]).await.unwrap(),
        expected.map(|(name, film, _)| vec![Value::Utf8(name.into()), Value::Utf8(film.into())]));
    assert_eq!(query("SELECT name, film, ord FROM actors, UNNEST(films) WITH ORDINALITY AS films(film, ord)", vec![]).await.unwrap(),
        expected.map(|(name, film, ord)| vec![Value::Utf8(name.into()), Value::Utf8(film.into()), Value::I32(ord)]));
}

#[test]
fn multiple_unnests_unnest_distinct_columns() {
    let context = context();
    let sql = "SELECT a.x, b.y FROM docs d, UNNEST(d.xs) AS a(x), UNNEST(d.ys) AS b(y)";
    let Statement::Query(relation) = prepare(sql, &context.catalog, &PrepareOptions::default()).unwrap().statement else { panic!("Expected query") };
    fn staged_ids(relation: &Relation, ids: &mut std::collections::BTreeSet<rtdb_plan::ColumnId>) {
        if let RelationNode::Unnest { expr: rtdb_plan::Expr::Column { id, .. }, .. } = relation.node() {
            ids.insert(*id);
        }
        for child in relation.children() { staged_ids(child, ids); }
    }
    let mut ids = std::collections::BTreeSet::new();
    staged_ids(&relation, &mut ids);
    assert_eq!(ids.len(), 2);
}

#[tokio::test]
async fn lateral_unnest_staging_is_hidden_from_sql_lookup() {
    let from = "FROM (VALUES (2)) AS t(k), UNNEST(ARRAY[k, k+1]) WITH ORDINALITY AS u(v, n)";
    assert_eq!(query(&format!("SELECT * {from}"), vec![]).await.unwrap(), [
        vec![Value::I64(2), Value::I64(2), Value::I32(1)],
        vec![Value::I64(2), Value::I64(3), Value::I32(2)],
    ]);
    let context = context();
    let err = prepare(&format!("SELECT _unnest_0 {from}"), &context.catalog, &PrepareOptions::default()).unwrap_err();
    assert_eq!(err.code, "xtdb/column-not-found");
}

#[tokio::test]
async fn unnest_filters_base_columns_before_staging_and_generated_columns_after() {
    let context = context();
    let baseline = context.memory.used_bytes();
    for (predicate, excludes_invalid_input) in [("k=1", true), ("v=1", false), ("n=1", false)] {
        let sql = format!("SELECT v FROM (VALUES (1),(0)) AS t(k), UNNEST(ARRAY[1/k]) WITH ORDINALITY AS u(v,n) WHERE {predicate}");
        let prepared = prepare(&sql, &context.catalog, &PrepareOptions::default()).unwrap();
        let query_plan = PreparedQuery::from_prepared(prepared, &context.memory).unwrap();
        let mut cursor = query_plan.execute(Arc::clone(&context), vec![]).await.unwrap();
        if excludes_invalid_input {
            let page = cursor.next_batch().await.unwrap().unwrap();
            assert_eq!(page.batch().num_rows(), 1);
            assert_eq!(read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap(), Value::I64(1));
            assert!(cursor.next_batch().await.unwrap().is_none());
        } else {
            assert_eq!(cursor.next_batch().await.unwrap_err().code, "xtdb.expression/division-by-zero", "{predicate}");
        }
        drop(cursor);
        drop(query_plan);
        assert_eq!(context.memory.used_bytes(), baseline);
    }
}

#[tokio::test]
async fn unnest_selection_preserves_the_three_original_row_and_predicate_placements() {
    let context = context();
    let queries = [
        ("FROM foo AS f, UNNEST(vs) AS uvs(v) WHERE f._id < 2 SELECT f._id, (v).t AS t, (v).v AS v", true),
        ("FROM foo AS f, UNNEST(vs) AS uvs(v) WHERE (uvs.v).v < 2 SELECT f._id, (v).t AS t, (v).v AS v", false),
        ("FROM foo AS f, UNNEST(vs) WITH ORDINALITY AS uvs (v, ord) WHERE uvs.ord < 2 SELECT f._id, (v).t AS t, (v).v AS v", false),
    ];
    fn find(relation: &Relation, accepts: fn(&RelationNode) -> bool) -> Option<&RelationNode> {
        if accepts(relation.node()) { return Some(relation.node()); }
        relation.children().into_iter().find_map(|child| find(child, accepts))
    }
    for (source, base_filter) in queries {
        let prepared = prepare(source, &context.catalog, &PrepareOptions::default()).unwrap();
        let optimized = PreparedQuery::from_prepared(prepared, &context.memory).unwrap();
        assert_eq!(
            optimized.schema().fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(),
            ["_id", "t", "v"],
            "{source}",
        );
        let relation = optimized.relation();
        let Some(RelationNode::Scan(scan)) = find(relation, |node| matches!(node, RelationNode::Scan(_))) else { panic!("Missing original table scan") };
        if base_filter {
            assert_eq!(scan.predicates.len(), 1, "{source}");
            assert!(matches!(&scan.predicates[0], rtdb_plan::Expr::Binary { op: rtdb_plan::BinaryOp::Lt, .. }));
            assert!(find(relation, |node| matches!(node, RelationNode::Filter { .. })).is_none(), "{source}");
        } else {
            assert!(scan.predicates.is_empty(), "{source}");
            let Some(RelationNode::Filter { input, .. }) = find(relation, |node| matches!(node, RelationNode::Filter { .. })) else { panic!("Missing post-UNNEST filter") };
            assert!(matches!(input.node(), RelationNode::Unnest { .. }), "{source}");
        }
        let mut cursor = optimized.execute(Arc::clone(&context), vec![]).await.unwrap();
        let mut rows = Vec::new();
        while let Some(batch) = cursor.next_batch().await.unwrap() {
            for row in 0..batch.batch().num_rows() {
                rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns())
                    .map(|(field, column)| read_value(field, column.as_ref(), row).unwrap()).collect::<Vec<_>>());
            }
        }
        let count = if base_filter { 2 } else { 1 };
        assert_eq!(rows, (1..=count).map(|i| vec![Value::I64(1), Value::I64(i), Value::I64(i)]).collect::<Vec<_>>(), "{source}");
    }
}

#[tokio::test]
async fn from_first_filters_and_projections_bind_in_source_order() {
    let sql = "FROM (VALUES (?),(?)) AS t(k) WHERE k > ? SELECT k + ? AS k WHERE k < ? SELECT k * ? AS result";
    let args = [0, 3, 1, 4, 8, 2].into_iter().map(Value::I64).collect();
    assert_eq!(rtdb_sql::parse(sql).unwrap()[0].parameter_count(), 6);
    assert_eq!(query(sql, args).await.unwrap(), vec![vec![Value::I64(14)]]);
    assert_eq!(
        query("FROM (VALUES (1),(2)) AS t(k) WHERE k = 1", vec![]).await.unwrap(),
        vec![vec![Value::I64(1)]],
    );
    assert_eq!(
        query("FROM (VALUES (1),(2)) AS t(k) SELECT WHERE TRUE", vec![]).await.unwrap(),
        vec![vec![], vec![]],
    );
    assert!(query("FROM (VALUES (1)) AS t(k) SELECT k AS x WHERE t.k = 1", vec![]).await.is_err());
}

#[tokio::test]
async fn from_first_specifications_are_query_operands_in_nested_contexts() {
    let sql = "WITH MATERIALIZED c AS (FROM (VALUES (1),(2)) AS t(k) WHERE k = 2 SELECT k) \
               FROM (FROM c SELECT k) AS d WHERE k = (FROM c SELECT k) SELECT k \
               UNION ALL FROM c WHERE k > 1 SELECT k ORDER BY k";
    assert_eq!(query(sql, vec![]).await.unwrap(), vec![vec![Value::I64(2)], vec![Value::I64(2)]]);
    assert_eq!(
        query("FROM (VALUES (1),(2),(2)) AS t(k) GROUP BY k HAVING COUNT(*) > 1 SELECT k, COUNT(*) AS n", vec![]).await.unwrap(),
        vec![vec![Value::I64(2), Value::I64(2)]],
    );
}

#[tokio::test]
async fn from_first_projection_exposes_only_its_outputs_to_later_tails() {
    let prefix = "FROM (VALUES (2)) AS o(k), LATERAL (FROM (VALUES (1)) AS t(v)";
    assert_eq!(
        query(&format!("{prefix} SELECT o.k AS v) AS d SELECT d.v"), vec![]).await.unwrap(),
        vec![vec![Value::I64(2)]],
    );
    for later in ["WHERE o.k = 2", "SELECT o.k AS v", "WHERE TRUE ORDER BY o.k"] {
        let error = query(&format!("{prefix} SELECT v {later}) AS d SELECT d.v"), vec![]).await.unwrap_err();
        assert_eq!(error.code, "xtdb/column-not-found", "{later}");
    }
    assert_eq!(
        query(&format!("{prefix} SELECT v WHERE (SELECT v) = 1) AS d SELECT d.v"), vec![]).await.unwrap(),
        vec![vec![Value::I64(1)]],
    );
}

#[tokio::test]
async fn from_first_select_keyword_can_also_name_a_projected_column() {
    for (sql, value) in [
        ("FROM (VALUES (2)) AS t(select) SELECT select", 2),
        ("FROM (VALUES (2,3)) AS t(select, v) SELECT SELECT v", 2),
        ("FROM (VALUES (2)) AS t(v) SELECT SELECT 1 AS n", 1),
    ] {
        assert_eq!(query(sql, vec![]).await.unwrap(), vec![vec![Value::I64(value)]], "{sql}");
    }
}

#[tokio::test]
async fn unnest_predicate_movement_respects_local_and_captured_column_demand() {
    let context = context();
    for (sql, value) in [
        (
            "FROM (VALUES (1)) AS o(k) LEFT JOIN LATERAL (FROM (VALUES (1),(0)) AS t(x), UNNEST(ARRAY[1/t.x]) AS u(v) WHERE t.x = o.k SELECT v) AS d ON TRUE SELECT d.v",
            Value::I64(1),
        ),
        (
            "FROM (VALUES (0)) AS o(k) LEFT JOIN LATERAL (FROM (VALUES (1)) AS t(x), UNNEST(CASE WHEN x=1 THEN ARRAY[] ELSE ARRAY[x] END) AS u(v) WHERE 1/o.k = 1 SELECT v) AS d ON TRUE SELECT d.v",
            Value::Null,
        ),
    ] {
        let prepared = prepare(sql, &context.catalog, &PrepareOptions::default()).unwrap();
        let plan = PreparedQuery::from_prepared(prepared, &context.memory).unwrap();
        let mut cursor = plan.execute(Arc::clone(&context), vec![]).await.unwrap();
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(page.batch().num_rows(), 1);
        assert_eq!(read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap(), value);
        assert!(cursor.next_batch().await.unwrap().is_none());
    }
}
