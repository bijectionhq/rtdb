use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{ExplainMode, Statement};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{Result, Timestamp, Value};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 27), Timestamp(0), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap()
}
async fn collect(context: &Arc<QueryContext>, prepared: rtdb_plan::PreparedStatement) -> Result<Vec<Vec<Value>>> {
    let query = PreparedQuery::from_prepared(prepared, &context.memory)?;
    let mut cursor = query.execute(Arc::clone(context), vec![]).await?;
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() {
            rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?);
        }
    }
    Ok(rows)
}
async fn sql(context: &Arc<QueryContext>, query: &str) -> Result<Vec<Vec<Value>>> {
    collect(context, prepare(query, &context.catalog, &PrepareOptions::default()).unwrap()).await
}
fn aggregate_forms(rows: &[Vec<Value>]) -> Vec<String> {
    rows.iter().filter_map(|row| row[2].field("aggregates")).flat_map(|value| {
        let Value::List(aggregates) = value else { panic!("Expected aggregate forms") };
        aggregates.iter().map(|value| {
            let Value::List(pair) = value else { panic!("Expected output/form pair") };
            let Value::Utf8(form) = &pair[1] else { panic!("Expected printed form") }; form.clone()
        })
    }).collect()
}

#[tokio::test]
async fn supported_sql_aggregate_aliases_and_integer_percentile_literals_retain_their_forms() {
    let context = context();
    let forms = aggregate_forms(&sql(&context, "EXPLAIN SELECT BOOL_AND(v), EVERY(v), BOOL_OR(v) FROM (VALUES (TRUE),(FALSE)) t(v)").await.unwrap());
    for prefix in ["(bool_and ", "(every ", "(bool_or "] { assert!(forms.iter().any(|form| form.starts_with(prefix)), "{forms:?}"); }
    assert_eq!(sql(&context, "SELECT BOOL_AND(v), EVERY(v), BOOL_OR(v) FROM (VALUES (TRUE),(FALSE)) t(v)").await.unwrap(), [vec![Value::Bool(false), Value::Bool(false), Value::Bool(true)]]);
    for (fraction, spelling, expected) in [("1", "1", 20), ("1.0", "1.0", 20), ("-1", "-1", 10)] {
        let query = format!("SELECT PERCENTILE_DISC({fraction}) WITHIN GROUP (ORDER BY v) FROM (VALUES (10),(20)) t(v)");
        let forms = aggregate_forms(&sql(&context, &format!("EXPLAIN {query}")).await.unwrap());
        assert!(forms[0].starts_with(&format!("(percentile_disc {spelling} [")), "{forms:?}");
        assert_eq!(sql(&context, &query).await.unwrap(), [vec![Value::I64(expected)]]);
    }
    assert_eq!(context.memory.used_bytes(), 0);
}

#[tokio::test]
async fn computed_aggregate_inputs_and_array_order_keys_run_before_filter_masks() {
    let context = context();
    for expression in ["SUM(1 / n)", "ARRAY_AGG(n ORDER BY 1 / n)"] {
        let query = format!("SELECT {expression} FILTER (WHERE FALSE) FROM (VALUES (0)) t(n)");
        assert!(sql(&context, &query).await.is_err(), "{query}");
        let empty = format!("SELECT {expression} FILTER (WHERE FALSE) FROM (SELECT n FROM (VALUES (0)) t(n) WHERE FALSE) t");
        assert_eq!(sql(&context, &empty).await.unwrap(), [vec![Value::Null]], "{empty}");
    }
    let explained = sql(&context, "EXPLAIN SELECT SUM(1 / n) FILTER (WHERE FALSE) FROM (VALUES (0)) t(n)").await.unwrap();
    let forms = aggregate_forms(&explained);
    assert!(forms[0].starts_with("(sum _aggregate_in_"));
    assert!(!forms[0].contains("(/ "));
    assert!(explained.iter().any(|row| matches!(row[2].field("project"), Some(Value::Utf8(form)) if form.contains("(/ 1 "))));
    assert_eq!(context.memory.used_bytes(), 0); assert_eq!(context.spill_bytes(), 0);
}

#[test]
fn sql_does_not_invent_aggregate_factories_or_modifiers_absent_from_the_pinned_grammar() {
    let context = context();
    for expression in ["ANY(v)", "SOME(v)", "VARIANCE(v)", "STDDEV(v)", "BOOL_AND(DISTINCT v)", "ARRAY_AGG(DISTINCT v)", "SUM(v ORDER BY v)"] {
        assert!(prepare(&format!("SELECT {expression} FROM (VALUES (1)) t(v)"), &context.catalog, &PrepareOptions::default()).is_err(), "{expression}");
    }
}

fn aggregate_nodes(relation: &rtdb_plan::Relation) -> Vec<&rtdb_plan::AggregateExpr> {
    let mut output = match relation.node() { rtdb_plan::RelationNode::Aggregate { aggregates, .. } => aggregates.iter().collect(), _ => Vec::new() };
    for child in relation.children() { output.extend(aggregate_nodes(child)); }
    output
}

#[test]
fn identical_aggregate_occurrences_have_independent_inputs_and_output_fields() {
    let context = context();
    let prepared = prepare("SELECT SUM(ABS(n)) AS a, SUM(ABS(n)) AS b FROM (VALUES (1)) t(n) HAVING SUM(ABS(n)) >= 0", &context.catalog, &PrepareOptions::default()).unwrap();
    let Statement::Query(relation) = prepared.statement else { panic!("Expected query") };
    let aggregates = aggregate_nodes(&relation);
    assert_eq!(aggregates.len(), 3);
    let outputs = aggregates.iter().map(|aggregate| aggregate.field.id).collect::<std::collections::BTreeSet<_>>();
    let inputs = aggregates.iter().map(|aggregate| match aggregate.argument.as_ref().unwrap() {
        rtdb_plan::Expr::Column { id, .. } => *id, _ => panic!("Expected projected aggregate input"),
    }).collect::<std::collections::BTreeSet<_>>();
    assert_eq!(outputs.len(), 3); assert_eq!(inputs.len(), 3);
    let rtdb_plan::RelationNode::Project { expressions, .. } = relation.node() else { panic!("Expected projection") };
    assert_ne!(expressions[0].expr, expressions[1].expr);
}

#[tokio::test]
async fn repeated_aggregate_occurrences_in_nested_expressions_having_and_explain_remain_distinct() {
    let context = context();
    let query = "SELECT SUM(v) AS a, SUM(v) + SUM(v) AS b, COUNT(*) AS c, COUNT(*) AS d FROM (VALUES (1),(2)) t(v) HAVING SUM(v) > 0";
    assert_eq!(sql(&context, query).await.unwrap(), [vec![Value::I64(3), Value::I64(6), Value::I64(2), Value::I64(2)]]);
    assert_eq!(aggregate_forms(&sql(&context, &format!("EXPLAIN {query}")).await.unwrap()).len(), 6);
    let query = "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v) AS a, PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v) AS b FROM (VALUES (1),(2)) t(v)";
    assert_eq!(aggregate_forms(&sql(&context, &format!("EXPLAIN {query}")).await.unwrap()).len(), 2);
    assert_eq!(sql(&context, query).await.unwrap(), [vec![Value::I64(1), Value::I64(1)]]);
    assert_eq!(context.memory.used_bytes(), 0); assert_eq!(context.spill_bytes(), 0);
}

#[tokio::test]
async fn xtql_factory_spelling_and_explicit_all_quantifiers_survive_explanation() {
    let context = context();
    for name in ["sum_all", "sum-all", "sum-distinct", "array_agg", "array-agg", "array-agg-distinct", "array_agg-distinct", "row-count", "row_count", "SUM_ALL", "sUm-DiStInCt", "ArRaY_aGg-DiStInCt", "RoW-CoUnT"] {
        let normalized = name.to_ascii_lowercase().replace('-', "_");
        let args = if normalized.starts_with("row") { "" } else { " v" };
        let source = format!("(-> (rel [{{:v 1}} {{:v 1}}] [v]) (aggregate {{:result ({name}{args})}}))");
        let mut prepared = rtdb_xtql::compile(&source, &context.catalog, &rtdb_xtql::CompileOptions::default()).unwrap();
        let expected = match normalized.as_str() {
            "sum_all" | "row_count" => Value::I64(2), "sum_distinct" => Value::I64(1),
            "array_agg" => Value::List(vec![Value::I64(1), Value::I64(1)]), "array_agg_distinct" => Value::List(vec![Value::I64(1)]),
            _ => unreachable!(),
        };
        assert_eq!(collect(&context, prepared.clone()).await.unwrap(), [vec![expected]], "{name}");
        let Statement::Query(relation) = prepared.statement else { panic!("Expected query") };
        prepared.statement = Statement::Explain { relation, mode: ExplainMode::Plan };
        let forms = aggregate_forms(&collect(&context, prepared).await.unwrap());
        assert!(forms[0].starts_with(&format!("({name}")), "{forms:?}");
    }
    for name in ["average", "any", "vec_agg_distinct", "bool_and_all", "var_pop_distinct"] {
        let source = format!("(-> (rel [{{:v 1}}] [v]) (aggregate {{:result ({name} v)}}))");
        assert!(rtdb_xtql::compile(&source, &context.catalog, &rtdb_xtql::CompileOptions::default()).is_err(), "{name}");
    }
}
