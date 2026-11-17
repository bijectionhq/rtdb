use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{Schema, Statement};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType, Result, ScalarType, Timestamp, Value};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 25), Timestamp(0), "UTC", QueryOptions { batch_rows: 1, sort_memory_bytes: 128, ..Default::default() }, &CancellationToken::new()).unwrap()
}

async fn query(sql: &str, parameters: Vec<Value>) -> Result<(Schema, Vec<Vec<Value>>)> {
    let context = context();
    let Statement::Query(relation) = prepare(sql, &context.catalog, &PrepareOptions::default())?.statement else { panic!("expected query") };
    let mut cursor = PreparedQuery::new(relation)?.execute(Arc::clone(&context), parameters).await?;
    let schema = cursor.runtime_schema().clone();
    let mut rows = Vec::new();
    while let Some(page) = cursor.next_batch().await? {
        for row in 0..page.batch().num_rows() {
            rows.push(page.batch().schema().fields().iter().zip(page.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?);
        }
    }
    drop(cursor);
    assert_eq!(context.memory.used_bytes(), 0);
    assert_eq!(context.spill_bytes(), 0);
    Ok((schema, rows))
}

#[tokio::test]
async fn continuous_interpolates_and_discrete_selects_a_sorted_value_in_both_directions() {
    let (schema, rows) = query("SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY n), PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY n DESC), PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY n), PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY n DESC) FROM (VALUES (40), (10), (30), (20), (NULL)) t(n)", vec![]).await.unwrap();
    assert_eq!(rows, [vec![Value::F64(17.5), Value::F64(32.5), Value::I64(10), Value::I64(40)]]);
    assert_eq!(schema.fields()[0].data_type, LogicalType::Scalar(ScalarType::F64).maybe());
    assert_eq!(schema.fields()[2].data_type, LogicalType::Scalar(ScalarType::I64).maybe());
}

#[tokio::test]
async fn percentile_filter_preserves_trailing_groups_and_excludes_null_input() {
    let (_, rows) = query("SELECT dept, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) FILTER (WHERE active), PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY salary) FILTER (WHERE active), COUNT(*) FROM (VALUES ('a',10,TRUE),('a',20,FALSE),('a',30,TRUE),('a',NULL,TRUE),('b',40,FALSE),('b',50,FALSE),('c',NULL,TRUE)) t(dept,salary,active) GROUP BY dept ORDER BY dept", vec![]).await.unwrap();
    assert_eq!(rows, [vec![Value::Utf8("a".into()), Value::F64(20.0), Value::I64(10), Value::I64(4)], vec![Value::Utf8("b".into()), Value::Null, Value::Null, Value::I64(2)], vec![Value::Utf8("c".into()), Value::Null, Value::Null, Value::I64(1)]]);
}

#[tokio::test]
async fn discrete_preserves_text_and_decimal_values_while_continuous_emits_double() {
    assert_eq!(query("SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY s), PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY s DESC) FROM (VALUES ('z'),('a'),('m'),(NULL)) t(s)", vec![]).await.unwrap().1, [vec![Value::Utf8("m".into()), Value::Utf8("m".into())]]);
    let (schema, rows) = query("SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY d), PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY d) FROM (VALUES (CAST(1 AS DECIMAL(8,2))), (CAST(2 AS DECIMAL(8,2)))) t(d)", vec![]).await.unwrap();
    let decimal = rtdb_types::DecimalValue::new(100i128.to_be_bytes().to_vec(), 8, 2, 128).unwrap();
    assert_eq!(rows, [vec![Value::Decimal(decimal.clone()), Value::F64(1.5)]]);
    assert_eq!(schema.fields()[0].data_type, Value::Decimal(decimal).logical_type().maybe());
}

#[tokio::test]
async fn discrete_output_uses_decimal_object_write_validation_after_raw_sum() {
    let input = "(SELECT SUM(n) AS n FROM (VALUES (CAST(9 AS DECIMAL(1,0))), (CAST(9 AS DECIMAL(1,0)))) t(n)) s";
    assert_eq!(query(&format!("SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY n) FROM {input}"), vec![]).await.unwrap().1, [vec![Value::F64(18.0)]]);
    assert_eq!(query(&format!("SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY n) FROM {input}"), vec![]).await.err().unwrap().code, "rtdb/decimal-precision");
}

#[tokio::test]
async fn fraction_parameters_keep_lexical_positions_and_rebind_for_each_execution() {
    let context = context();
    let statement = prepare("SELECT PERCENTILE_CONT(?) WITHIN GROUP (ORDER BY n) FILTER (WHERE n > ?), PERCENTILE_DISC(?) WITHIN GROUP (ORDER BY n DESC) FROM (VALUES (10),(20),(30),(40)) t(n)", &context.catalog, &PrepareOptions::default()).unwrap();
    assert_eq!(statement.parameter_types.keys().copied().collect::<Vec<_>>(), [0, 1, 2]);
    let Statement::Query(relation) = statement.statement else { panic!("expected query") };
    assert_eq!(relation.parameter_count(), 3);
    let prepared = PreparedQuery::new(relation).unwrap();
    for (fraction, expected) in [(0.0, [Value::F64(20.0), Value::I64(40)]), (1.0, [Value::F64(40.0), Value::I64(10)])] {
        let mut cursor = prepared.execute(Arc::clone(&context), vec![Value::F64(fraction), Value::I64(10), Value::F64(fraction)]).await.unwrap();
        let page = cursor.next_batch().await.unwrap().unwrap();
        let values = page.batch().schema().fields().iter().zip(page.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), 0).unwrap()).collect::<Vec<_>>();
        assert_eq!(values, expected);
        assert!(cursor.next_batch().await.unwrap().is_none());
    }
    assert_eq!(context.memory.used_bytes(), 0);
    assert_eq!(context.spill_bytes(), 0);
}

#[tokio::test]
async fn global_empty_and_all_null_inputs_emit_null_with_declared_factory_types() {
    for source in ["(VALUES (CAST(NULL AS BIGINT))) t(n)", "(SELECT 1 AS n WHERE FALSE) t"] {
        let (schema, rows) = query(&format!("SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY n), PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY n) FROM {source}"), vec![]).await.unwrap();
        assert_eq!(rows, [vec![Value::Null, Value::Null]]);
        assert_eq!(schema.fields()[0].data_type, LogicalType::Scalar(ScalarType::F64).maybe());
    }
    let (schema, rows) = query("SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY _valid_from) FROM pg_catalog.pg_namespace", vec![]).await.unwrap();
    assert_eq!(rows, [vec![Value::Null]]);
    assert!(matches!(&schema.fields()[0].data_type, LogicalType::Maybe(inner) if matches!(inner.as_ref(), LogicalType::Scalar(ScalarType::Timestamp { .. }))));
}

#[tokio::test]
async fn percentile_comparator_rejects_list_and_struct_inputs_even_without_comparisons() {
    for function in ["PERCENTILE_DISC", "PERCENTILE_CONT"] {
        for input in ["ARRAY[n]", "{n:n}"] {
            for source in ["(SELECT 1 AS n WHERE FALSE) t", "(VALUES (1)) t(n)", "(VALUES (1),(2)) t(n)"] {
                for filter in ["", "FILTER (WHERE FALSE)"] {
                    let sql = format!("SELECT {function}(0.5) WITHIN GROUP (ORDER BY {input}) {filter} FROM {source}");
                    assert_eq!(query(&sql, vec![]).await.err().unwrap().code, "xtdb.expression/function-type-mismatch", "{sql}");
                }
            }
        }
    }
    assert_eq!(query("SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY ARRAY[_valid_from]) FROM pg_catalog.pg_namespace", vec![]).await.err().unwrap().code, "xtdb.expression/function-type-mismatch");
}

#[tokio::test]
async fn ordered_decimal_ties_keep_input_order_across_spills_and_minimum_stays_exact() {
    let source = "(VALUES (CAST(9007199254740993 AS DECIMAL(20,0))), (CAST(9007199254740992 AS DECIMAL(20,0)))) t(n)";
    let expected = Value::Decimal(rtdb_types::DecimalValue::new(9_007_199_254_740_993i128.to_be_bytes().to_vec(), 20, 0, 128).unwrap());
    for direction in ["ASC", "DESC"] {
        assert_eq!(query(&format!("SELECT PERCENTILE_DISC(0) WITHIN GROUP (ORDER BY n {direction}) FROM {source}"), vec![]).await.unwrap().1, [vec![expected.clone()]]);
        assert_eq!(query(&format!("SELECT n FROM {source} ORDER BY n {direction} LIMIT 1"), vec![]).await.unwrap().1, [vec![expected.clone()]]);
        assert_eq!(query(&format!("SELECT n,ROW_NUMBER() OVER (ORDER BY n {direction}) AS r FROM {source} ORDER BY r LIMIT 1"), vec![]).await.unwrap().1, [vec![expected.clone(), Value::I64(1)]]);
    }
    assert_eq!(query("SELECT MIN(n) FROM (VALUES (9007199254740993), (9007199254740992)) t(n)", vec![]).await.unwrap().1, [vec![Value::I64(9_007_199_254_740_992)]]);
}

#[tokio::test]
async fn ordinary_ordering_skips_comparator_for_empty_input_but_rejects_singleton_nested_values() {
    for input in ["ARRAY[n]", "{n:n}"] {
        assert!(query(&format!("SELECT {input} AS v FROM (SELECT 1 AS n WHERE FALSE) t ORDER BY v"), vec![]).await.unwrap().1.is_empty());
        for source in ["(VALUES (1)) t(n)", "(VALUES (1),(2)) t(n)"] {
            assert_eq!(query(&format!("SELECT {input} AS v FROM {source} ORDER BY v"), vec![]).await.err().unwrap().code, "xtdb.expression/function-type-mismatch");
        }
    }
    assert_eq!(query("SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY n) FROM (SELECT n FROM (VALUES (1),('x')) t(n) WHERE FALSE) s", vec![]).await.err().unwrap().code, "xtdb.expression/function-type-mismatch");
}

#[tokio::test]
async fn nonnumeric_fraction_parameters_fail_before_empty_input_and_filtered_values() {
    for value in [Value::Null, Value::Bool(true), Value::Utf8("0.5".into())] {
        let error = query("SELECT PERCENTILE_CONT($1) WITHIN GROUP (ORDER BY n) FROM (SELECT 1 AS n WHERE FALSE) t", vec![value]).await.err().unwrap();
        assert_eq!(error.code, "xtdb/expected-number");
    }
    assert!(query("SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST(s AS BIGINT)) FILTER (WHERE FALSE) FROM (VALUES ('invalid')) t(s)", vec![]).await.is_err());
}

#[tokio::test]
async fn discrete_clamps_fraction_but_continuous_preserves_reference_rank_failures() {
    assert_eq!(query("SELECT PERCENTILE_DISC(-1) WITHIN GROUP (ORDER BY n), PERCENTILE_DISC(2) WITHIN GROUP (ORDER BY n) FROM (VALUES (10),(20)) t(n)", vec![]).await.unwrap().1, [vec![Value::I64(10), Value::I64(20)]]);
    for fraction in [-0.1, 1.1, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(query("SELECT PERCENTILE_CONT($1) WITHIN GROUP (ORDER BY n) FROM (VALUES (10),(20)) t(n)", vec![Value::F64(fraction)]).await.err().unwrap().code, "rtdb.query/percentile-rank");
        assert_eq!(query("SELECT PERCENTILE_CONT($1) WITHIN GROUP (ORDER BY n) FROM (VALUES (10)) t(n)", vec![Value::F64(fraction)]).await.unwrap().1, [vec![Value::F64(10.0)]]);
    }
    assert_eq!(query("SELECT PERCENTILE_CONT($1) WITHIN GROUP (ORDER BY n), PERCENTILE_DISC($1) WITHIN GROUP (ORDER BY n) FROM (VALUES (10),(20)) t(n)", vec![Value::F64(f64::NAN)]).await.unwrap().1, [vec![Value::F64(10.0), Value::I64(10)]]);
}

#[test]
fn null_order_warnings_retain_occurrences_and_source_order() {
    let context = context();
    let prepared = prepare("SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY n NULLS LAST), PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY n NULLS FIRST), PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY n NULLS LAST) FROM (VALUES (1),(2)) t(n)", &context.catalog, &PrepareOptions::default()).unwrap();
    assert_eq!(prepared.notices.iter().map(|notice| notice.message.as_str()).collect::<Vec<_>>(), [
        "NULL ordering is ignored for percentile_disc (nulls are excluded from ordered-set aggregates)",
        "NULL ordering is ignored for percentile_cont (nulls are excluded from ordered-set aggregates)",
        "NULL ordering is ignored for percentile_disc (nulls are excluded from ordered-set aggregates)",
    ]);
}

#[test]
fn ordered_set_syntax_rejects_dynamic_fraction_multiple_keys_and_aggregate_modifiers() {
    let context = context();
    for sql in [
        "SELECT PERCENTILE_CONT(n) WITHIN GROUP (ORDER BY n) FROM (VALUES (1)) t(n)",
        "SELECT PERCENTILE_CONT(0.1+0.2) WITHIN GROUP (ORDER BY n) FROM (VALUES (1)) t(n)",
        "SELECT PERCENTILE_CONT(NULL) WITHIN GROUP (ORDER BY n) FROM (VALUES (1)) t(n)",
        "SELECT PERCENTILE_CONT(DISTINCT 0.5) WITHIN GROUP (ORDER BY n) FROM (VALUES (1)) t(n)",
        "SELECT PERCENTILE_CONT(0.5)",
        "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY n,n) FROM (VALUES (1)) t(n)",
        "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY n) OVER () FROM (VALUES (1)) t(n)",
        "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(n)) FROM (VALUES (1)) t(n)",
    ] { assert!(prepare(sql, &context.catalog, &PrepareOptions::default()).is_err(), "{sql}"); }
}
