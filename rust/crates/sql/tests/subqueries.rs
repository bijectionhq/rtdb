use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{JoinKind, Relation, RelationNode, Statement};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType, Result, ScalarType, Timestamp, Value};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: Vec::new(), storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 24)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 27), Timestamp(100), "UTC", QueryOptions { batch_rows: 1, ..Default::default() }, &CancellationToken::new()).unwrap()
}

async fn query(sql: &str, parameters: Vec<Value>) -> Result<Vec<Vec<Value>>> {
    let context = context();
    let options = PrepareOptions { parameter_types: parameters.iter().enumerate().map(|(index, value)| (index, value.logical_type())).collect(), ..Default::default() };
    let prepared = prepare(sql, &context.catalog, &options)?;
    let query = PreparedQuery::from_prepared(prepared, &context.memory)?;
    let mut cursor = query.execute(context, parameters).await?;
    let mut result = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        let schema = batch.batch().schema();
        for row in 0..batch.batch().num_rows() {
            result.push(schema.fields().iter().zip(batch.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?);
        }
    }
    Ok(result)
}

#[tokio::test]
async fn values_rows_accept_scalar_expressions_and_keep_row_constructor_widths() {
    assert_eq!(query("VALUES 40 + 1, (41) + 1, (43), ROW(44), $1", vec![Value::I64(45)]).await.unwrap(),
        (41..=45).map(|value| vec![Value::I64(value)]).collect::<Vec<_>>());
    assert_eq!(query("VALUES (1, 2), ROW(3, 4)", vec![]).await.unwrap(),
        [vec![Value::I64(1), Value::I64(2)], vec![Value::I64(3), Value::I64(4)]]);
    assert_eq!(query("VALUES ROW(), ()", vec![]).await.unwrap(), [vec![], vec![]]);
    assert_eq!(query("SELECT 42 IN (VALUES 40 + 2, 43)", vec![]).await.unwrap(), [vec![Value::Bool(true)]]);
    assert_eq!(query("SELECT q._column_1 FROM (VALUES 41 + 1) q", vec![]).await.unwrap(), [vec![Value::I64(42)]]);
    let context = context();
    let Statement::Query(values) = prepare("VALUES (1, 2)", &context.catalog, &PrepareOptions::default()).unwrap().statement else { panic!("VALUES relation") };
    assert_eq!(values.schema().fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), ["_column_1", "_column_2"]);
    assert_eq!(query("VALUES 1, (2, 3)", vec![]).await.unwrap_err().message.as_deref(),
        Some("Column count mismatch: expected 1, given 2"));
}

#[tokio::test]
async fn select_all_preserves_duplicate_rows() {
    for sql in ["SELECT ALL x FROM (VALUES (1), (1)) t(x)", "FROM (VALUES (1), (1)) t(x) SELECT ALL x"] {
        assert_eq!(query(sql, vec![]).await.unwrap(), [vec![Value::I64(1)], vec![Value::I64(1)]]);
    }
    assert_eq!(query("SELECT DISTINCT x FROM (VALUES (1), (1)) t(x)", vec![]).await.unwrap(), [vec![Value::I64(1)]]);
}

#[tokio::test]
async fn scalar_subquery_in_select_and_where_preserves_empty_and_single_row_results() {
    assert_eq!(query("SELECT (SELECT 42) AS answer", vec![]).await.unwrap(), vec![vec![Value::I64(42)]]);
    assert_eq!(query("SELECT (SELECT x FROM (VALUES (1)) AS t(x) WHERE FALSE) AS answer", vec![]).await.unwrap(), vec![vec![Value::Null]]);
    assert_eq!(query("SELECT x FROM (VALUES (1), (2), (3)) AS t(x) WHERE x = (SELECT MAX(y) FROM (VALUES (1), (2)) AS u(y))", vec![]).await.unwrap(), vec![vec![Value::I64(2)]]);
    assert!(query("SELECT (SELECT x FROM (VALUES (1), (2)) AS t(x))", vec![]).await.is_err());
    assert!(query("SELECT (SELECT 1, 2)", vec![]).await.is_err());
}

#[tokio::test]
async fn array_query_collects_one_column_with_order_duplicates_and_nulls() {
    let source = "SELECT ARRAY(SELECT n FROM (VALUES (3),(1),(NULL),(3)) t(n) ORDER BY n NULLS LAST)";
    let expected = Value::list(vec![Value::I64(1), Value::I64(3), Value::I64(3), Value::Null]);
    assert_eq!(query(source, vec![]).await.unwrap(), [vec![expected]]);
    assert_eq!(query("SELECT ARRAY(SELECT 1 WHERE FALSE), ARRAY(SELECT NULL)", vec![]).await.unwrap(),
        [vec![Value::list(vec![]), Value::list(vec![Value::Null])]]);
    assert_eq!(query("SELECT ARRAY(SELECT ARRAY(SELECT 2)), ARRAY(VALUES (1),(2))", vec![]).await.unwrap(),
        [vec![Value::list(vec![Value::list(vec![Value::I64(2)])]), Value::list(vec![Value::I64(1), Value::I64(2)])]]);
    assert_eq!(query("SELECT ARRAY(FROM (VALUES (3),(1)) t(n) SELECT n ORDER BY n)", vec![]).await.unwrap(),
        [vec![Value::list(vec![Value::I64(1), Value::I64(3)])]]);
    assert_eq!(query("SELECT ARRAY(WITH a AS (SELECT 3 AS n) SELECT n FROM a)", vec![]).await.unwrap(),
        [vec![Value::list(vec![Value::I64(3)])]]);
    assert_eq!(query("SELECT ARRAY(SELECT n FROM (VALUES (0)) t(n) ORDER BY 1 / n LIMIT 0)", vec![]).await.unwrap(),
        [vec![Value::list(vec![])]]);
}

#[tokio::test]
async fn optional_select_items_preserve_rows_columns_and_wildcard_positions() {
    assert_eq!(query("SELECT", vec![]).await.unwrap(), [vec![]]);
    assert!(query("SELECT ,, WHERE FALSE", vec![]).await.unwrap().is_empty());
    assert_eq!(query("SELECT ,, FROM (VALUES (1)) t(a)", vec![]).await.unwrap(), [vec![]]);
    for sql in [
        "SELECT ,,a,,b, FROM (VALUES (1,2)) t(a,b)",
        "SELECT *,, FROM (VALUES (1,2)) t(a,b)",
        "SELECT ,* FROM (VALUES (1,2)) t(a,b)",
        "SELECT ,t.*, FROM (VALUES (1,2)) t(a,b)",
        "FROM (VALUES (1,2)) t(a,b) SELECT ,,a,,b,",
    ] {
        assert_eq!(query(sql, vec![]).await.unwrap(), [vec![Value::I64(1), Value::I64(2)]], "{sql}");
    }
    for sql in [
        "SELECT a,*,b FROM (VALUES (1,2)) t(a,b)",
        "SELECT ,*, FROM (VALUES (1)) t(a)",
        "SELECT *,* FROM (VALUES (1)) t(a)",
        "FROM (VALUES (1)) t(a) SELECT *,*",
        "SELECT 1 WHERE 1 IN (,1)",
    ] {
        assert_eq!(query(sql, vec![]).await.unwrap_err().code, "xtdb/sql-error", "{sql}");
    }
}

#[tokio::test]
async fn array_query_uses_lexical_correlations_and_join_condition_scope() {
    let source = "SELECT v.x, ARRAY(SELECT n FROM (VALUES (2),(1)) t(n) WHERE t.n = v.x) FROM (VALUES (1),(2),(3)) v(x)";
    assert_eq!(query(source, vec![]).await.unwrap(), [
        vec![Value::I64(1), Value::list(vec![Value::I64(1)])],
        vec![Value::I64(2), Value::list(vec![Value::I64(2)])],
        vec![Value::I64(3), Value::list(vec![])],
    ]);
    let source = "SELECT a.n FROM (VALUES (ARRAY[1,2])) a(n) INNER JOIN (VALUES (2)) b(x) ON ARRAY(SELECT n FROM (VALUES (1),(2)) t(n)) = a.n";
    assert_eq!(query(source, vec![]).await.unwrap(), [vec![Value::list(vec![Value::I64(1), Value::I64(2)])]]);
    let source = "SELECT $1, ARRAY(SELECT n FROM (VALUES (1),(2)) t(n) WHERE n <= $2)";
    assert_eq!(query(source, vec![Value::I64(9), Value::I64(1)]).await.unwrap(),
        [vec![Value::I64(9), Value::list(vec![Value::I64(1)])]]);
}

#[tokio::test]
async fn array_query_requires_one_column_and_a_subquery_scope() {
    for (source, message) in [
        ("SELECT ARRAY(SELECT 1, 2)", "Subquery arity error: expected a single column, got 2"),
        ("SELECT 1 WHERE 1 IN (ARRAY(SELECT 1))", "Subqueries are not allowed in this context"),
        ("SELECT 1 WHERE 1 IN ((SELECT 1), 1)", "Subqueries are not allowed in this context"),
    ] {
        let error = query(source, vec![]).await.unwrap_err();
        assert_eq!(error.message.as_deref(), Some(message), "{source}");
    }
}

#[tokio::test]
async fn independent_scalar_subqueries_build_before_probing_an_empty_input() {
    for source in [
        "SELECT (SELECT 1 / 0) WHERE FALSE",
        "SELECT (SELECT 1 / 0) FROM (VALUES (1)) t(n) WHERE n = 2",
        "SELECT ARRAY(SELECT 1 / 0) WHERE FALSE",
        "SELECT (VALUES (1 / 0)) LIMIT 0",
        "SELECT ARRAY(VALUES (1 / 0)) LIMIT 0",
        "SELECT (SELECT n FROM (VALUES (1),(2),(1 / 0)) t(n))",
    ] {
        let error = query(source, vec![]).await.unwrap_err();
        assert_eq!(error.code, "xtdb.expression/division-by-zero", "{source}");
    }
    for source in [
        "SELECT (SELECT 1 / t.n) FROM (VALUES (0)) t(n) WHERE FALSE",
        "SELECT ARRAY(SELECT 1 / t.n) FROM (VALUES (0)) t(n) WHERE FALSE",
        "SELECT (SELECT 1 / 0) LIMIT 0",
        "SELECT ARRAY(SELECT 1 / 0) LIMIT 0",
        "SELECT (SELECT n FROM (VALUES (1),(2)) t(n)) WHERE FALSE",
    ] {
        assert_eq!(query(source, vec![]).await.unwrap(), Vec::<Vec<Value>>::new(), "{source}");
    }
}

#[tokio::test]
async fn scalar_cardinality_is_checked_per_probe_for_both_join_modes() {
    for source in [
        "SELECT (SELECT n FROM (VALUES (1),(2)) t(n))",
        "SELECT (SELECT n + a FROM (VALUES (1),(2)) t(n)) FROM (VALUES (0)) q(a)",
    ] {
        let error = query(source, vec![]).await.unwrap_err();
        assert_eq!(error.code, "xtdb.single-join/cardinality-violation", "{source}");
        assert_eq!(error.message.as_deref(), Some("cardinality violation"), "{source}");
    }
}

#[tokio::test]
async fn correlated_scalar_subquery_in_select_and_nested_cte_keep_lexical_captures() {
    let expected = vec![vec![Value::I64(1), Value::I64(11)], vec![Value::I64(2), Value::I64(12)]];
    assert_eq!(query("SELECT t.x, (SELECT t.x + 10) FROM (VALUES (1), (2)) AS t(x)", vec![]).await.unwrap(), expected);
    assert_eq!(query("SELECT t.x, (SELECT (SELECT t.x + 10)) FROM (VALUES (1), (2)) AS t(x)", vec![]).await.unwrap(), expected);
    assert_eq!(query("SELECT t.x, (WITH c AS (SELECT t.x + 10 AS v) SELECT (SELECT v FROM c)) FROM (VALUES (1), (2)) AS t(x)", vec![]).await.unwrap(), expected);
    assert_eq!(query("SELECT (SELECT t.x FROM (VALUES (9)) AS t(x)) FROM (VALUES (1)) AS t(x)", vec![]).await.unwrap(), vec![vec![Value::I64(9)]]);
}

#[tokio::test]
async fn exists_and_not_exists_work_in_predicates_and_select_without_evaluating_unused_projections() {
    assert_eq!(query("SELECT t.x, EXISTS (SELECT u.y FROM (VALUES (2)) AS u(y) WHERE u.y = t.x) FROM (VALUES (1), (2)) AS t(x)", vec![]).await.unwrap(), vec![vec![Value::I64(1), Value::Bool(false)], vec![Value::I64(2), Value::Bool(true)]]);
    assert_eq!(query("SELECT t.x FROM (VALUES (1), (2)) AS t(x) WHERE NOT EXISTS (SELECT u.y FROM (VALUES (2)) AS u(y) WHERE u.y = t.x)", vec![]).await.unwrap(), vec![vec![Value::I64(1)]]);
    assert_eq!(query("SELECT EXISTS (SELECT 1 / 0), EXISTS (SELECT (SELECT x FROM (VALUES (1), (2)) AS u(x)))", vec![]).await.unwrap(), vec![vec![Value::Bool(true), Value::Bool(true)]]);
    assert_eq!(query("SELECT EXISTS (SELECT n FROM (SELECT 1 / 0 AS n) AS derived)", vec![]).await.unwrap(), vec![vec![Value::Bool(true)]]);
    assert_eq!(query("SELECT EXISTS (SELECT DISTINCT x FROM (VALUES (1), (1)) AS t(x) OFFSET 1)", vec![]).await.unwrap(), vec![vec![Value::Bool(false)]]);
}

#[tokio::test]
async fn quantified_subquery_truth_tables_keep_nulls_and_empty_sets() {
    for (haystack, any_one, all_one, any_null, all_null) in [
        ("SELECT 1 WHERE FALSE", Value::Bool(false), Value::Bool(true), Value::Bool(false), Value::Bool(true)),
        ("VALUES (1), (NULL)", Value::Bool(true), Value::Null, Value::Null, Value::Null),
        ("VALUES (2), (NULL)", Value::Null, Value::Bool(false), Value::Null, Value::Null),
        ("VALUES (1), (1)", Value::Bool(true), Value::Bool(true), Value::Null, Value::Null),
    ] {
        let sql = format!("SELECT 1 = ANY ({haystack}), 1 = ALL ({haystack}), NULL = ANY ({haystack}), NULL = ALL ({haystack})");
        assert_eq!(query(&sql, vec![]).await.unwrap(), vec![vec![any_one, all_one, any_null, all_null]], "{sql}");
    }
    assert_eq!(query("SELECT 1 IN (VALUES (1), (NULL)), 1 NOT IN (VALUES (2), (NULL)), 3 > ALL (VALUES (1), (2)), 1 <> SOME (VALUES (1), (2))", vec![]).await.unwrap(), vec![vec![Value::Bool(true), Value::Null, Value::Bool(true), Value::Bool(true)]]);
}

#[tokio::test]
async fn quantified_arrays_keep_null_and_empty_distinct_and_capture_each_outer_row() {
    assert_eq!(query("SELECT 1 = ANY (ARRAY[]), 1 = ALL (ARRAY[]), 1 = ANY (NULL), 1 = ALL (NULL)", vec![]).await.unwrap(), vec![vec![Value::Bool(false), Value::Bool(true), Value::Null, Value::Null]]);
    assert_eq!(query("SELECT 2 = ANY (t.x) FROM (VALUES (ARRAY[1, 2]), (ARRAY[3, NULL]), (NULL)) AS t(x)", vec![]).await.unwrap(), vec![vec![Value::Bool(true)], vec![Value::Null], vec![Value::Null]]);
}

#[tokio::test]
async fn id_array_parameters_preserve_the_quantified_truth_table() {
    let sql = "SELECT _id = ANY($1) FROM (VALUES (1), (2)) AS t(_id)";
    for (parameter, expected) in [
        (Value::list(vec![Value::I64(1), Value::Null]), vec![Value::Bool(true), Value::Null]),
        (Value::list(vec![Value::F64(2.0)]), vec![Value::Bool(false), Value::Bool(true)]),
        (Value::list(vec![]), vec![Value::Bool(false), Value::Bool(false)]),
        (Value::Null, vec![Value::Null, Value::Null]),
    ] {
        assert_eq!(query(sql, vec![parameter]).await.unwrap(), expected.into_iter().map(|value| vec![value]).collect::<Vec<_>>());
    }
}

#[tokio::test]
async fn subqueries_in_values_aggregates_having_and_order_by_use_the_correct_input() {
    assert_eq!(query("VALUES ((SELECT 1)), ((SELECT 2))", vec![]).await.unwrap(), vec![vec![Value::I64(1)], vec![Value::I64(2)]]);
    assert_eq!(query("SELECT SUM((SELECT t.x)) FROM (VALUES (1), (2)) AS t(x)", vec![]).await.unwrap(), vec![vec![Value::I64(3)]]);
    assert_eq!(query("SELECT t.x, (SELECT t.x) FROM (VALUES (1), (1), (2)) AS t(x) GROUP BY t.x HAVING t.x = (SELECT 2)", vec![]).await.unwrap(), vec![vec![Value::I64(2), Value::I64(2)]]);
    assert_eq!(query("SELECT x FROM (VALUES (1), (2)) AS t(x) ORDER BY (SELECT -x)", vec![]).await.unwrap(), vec![vec![Value::I64(2)], vec![Value::I64(1)]]);
    assert!(query("SELECT 1 ORDER BY (SELECT 2), 2", vec![]).await.is_err());
}

#[tokio::test]
async fn subquery_join_conditions_filter_before_outer_null_extension_and_keep_column_order() {
    let on = "EXISTS (SELECT 1 WHERE l.x = r.y)";
    assert_eq!(query(&format!("SELECT * FROM (VALUES (1), (2)) AS l(x) LEFT JOIN (VALUES (2)) AS r(y) ON {on}"), vec![]).await.unwrap(), vec![vec![Value::I64(1), Value::Null], vec![Value::I64(2), Value::I64(2)]]);
    assert_eq!(query(&format!("SELECT * FROM (VALUES (2)) AS l(x) RIGHT JOIN (VALUES (1), (2)) AS r(y) ON {on}"), vec![]).await.unwrap(), vec![vec![Value::Null, Value::I64(1)], vec![Value::I64(2), Value::I64(2)]]);
    assert!(query("SELECT * FROM (VALUES (1)) AS l(x) JOIN (VALUES (1)) AS r(x) ON x = 1 AND EXISTS (SELECT 1)", vec![]).await.is_err());
}

#[test]
fn subquery_captures_do_not_increase_external_parameter_arity_or_leak_wildcard_columns() {
    let context = context();
    let prepared = prepare("SELECT t.*, (SELECT ? + t.x) AS y FROM (VALUES (?)) AS t(x) WHERE t.x IN (SELECT ?)", &context.catalog, &PrepareOptions { parameter_types: BTreeMap::from([(0, LogicalType::Scalar(ScalarType::I64)), (1, LogicalType::Scalar(ScalarType::I64)), (2, LogicalType::Scalar(ScalarType::I64))]), ..Default::default() }).unwrap();
    assert_eq!(prepared.parameter_types.len(), 3);
    let Statement::Query(relation) = prepared.statement else { panic!("expected query") };
    assert_eq!(relation.parameter_count(), 3);
    assert_eq!(relation.schema().fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), ["x", "y"]);
    fn scopes(input: &Relation) -> (usize, usize) {
        let applies = usize::from(matches!(input.node(), RelationNode::Apply { .. }));
        let independent_marks = usize::from(matches!(input.node(), RelationNode::Mark { arguments, .. } if arguments.is_empty()));
        input.children().into_iter().map(|child| scopes(child))
            .fold((applies, independent_marks), |(applies, marks), (nested_applies, nested_marks)| (applies + nested_applies, marks + nested_marks))
    }
    assert_eq!(scopes(&relation), (1, 1));
}

#[tokio::test]
async fn lexical_parameters_remain_distinct_from_subquery_captures() {
    assert_eq!(query("SELECT t.x, (SELECT ? + t.x) FROM (VALUES (?)) AS t(x) WHERE t.x IN (SELECT ?)", vec![Value::I64(10), Value::I64(2), Value::I64(2)]).await.unwrap(), vec![vec![Value::I64(2), Value::I64(12)]]);
    assert_eq!(query("SELECT (SELECT $3 + t.x) FROM (VALUES ($1)) AS t(x)", vec![Value::I64(2), Value::Null, Value::I64(10)]).await.unwrap(), vec![vec![Value::I64(12)]]);
}

#[tokio::test]
async fn subquery_helper_ids_are_hidden_from_current_and_outer_name_resolution() {
    fn helper(input: &Relation) -> Option<String> {
        if let RelationNode::Join { right, kind: JoinKind::Single, .. } = input.node() {
            return right.schema().fields().first().map(|field| field.name.clone());
        }
        input.children().into_iter().find_map(|child| helper(child))
    }
    let context = context();
    let prepared = prepare("SELECT (SELECT 1)", &context.catalog, &PrepareOptions::default()).unwrap();
    let Statement::Query(relation) = prepared.statement else { panic!("expected query") };
    let name = helper(&relation).unwrap();
    for sql in [format!("SELECT (SELECT 1), {name}"), format!("SELECT (SELECT 1), (SELECT {name})")] {
        let error = prepare(&sql, &context.catalog, &PrepareOptions::default()).unwrap_err();
        assert_eq!(error.code, "xtdb/column-not-found", "{sql}");
    }
    assert_eq!(query(&format!("SELECT (SELECT 1), {name}, (SELECT t.{name}) FROM (VALUES (42)) AS t({name})"), vec![]).await.unwrap(), vec![vec![Value::I64(1), Value::I64(42), Value::I64(42)]]);
}
