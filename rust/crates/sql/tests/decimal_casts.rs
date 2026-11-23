use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::Statement;
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{DecimalValue, LogicalType, Result, ScalarType, Timestamp, Value};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: Vec::new(), storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 24), Timestamp(0), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap()
}

async fn query(sql: &str) -> Result<(LogicalType, Value)> {
    let context = context();
    let Statement::Query(relation) = prepare(sql, &context.catalog, &PrepareOptions::default())?.statement else { panic!("expected query") };
    let data_type = relation.schema().fields()[0].data_type.clone();
    let mut cursor = PreparedQuery::new(relation)?.execute(context, Vec::new()).await?;
    let batch = cursor.next_batch().await?.unwrap();
    assert_eq!(batch.batch().num_rows(), 1);
    let value = read_value(&batch.batch().schema().fields()[0], batch.batch().column(0).as_ref(), 0)?;
    assert!(cursor.next_batch().await?.is_none());
    Ok((data_type, value))
}

fn decimal(coefficient: i128, precision: i32, scale: i32, bit_width: u16) -> Value {
    let width = usize::from(bit_width / 8);
    let mut bytes = vec![if coefficient < 0 { 255 } else { 0 }; width];
    bytes[width - 16..].copy_from_slice(&coefficient.to_be_bytes());
    Value::Decimal(DecimalValue::new(bytes, precision, scale, bit_width).unwrap())
}

#[tokio::test]
async fn decimal_casting_preserves_original_values_precision_scale_and_width_assertions() {
    for (sql, coefficient, precision, scale, bit_width) in [
        ("SELECT 301.02::DECIMAL(5, 2) AS v", 30102, 5, 2, 128),
        ("SELECT 301.02::DECIMAL(5, 1) AS v", 3010, 5, 1, 128),
        ("SELECT 301.02::DECIMAL(6, 3) AS v", 301020, 6, 3, 128),
        ("SELECT 301.02::DECIMAL(5) AS v", 301, 5, 0, 128),
        ("SELECT 301.02::DECIMAL AS v", 301020000000, 64, 9, 256),
        ("SELECT '301.02'::DECIMAL AS v", 301020000000, 64, 9, 256),
        ("SELECT '301.02'::NUMERIC(33, 2) AS v", 30102, 33, 2, 256),
        ("SELECT '301.02'::DEC(32, 2) AS v", 30102, 32, 2, 128),
        ("SELECT ('301.02'::DECIMAL(5, 2))::DECIMAL AS v", 30102, 5, 2, 128),
        ("SELECT '0.001'::DECIMAL(1, 3) AS v", 1, 1, 3, 128),
    ] {
        assert_eq!(query(sql).await.unwrap(), (LogicalType::Scalar(ScalarType::Decimal { precision, scale, bit_width }), decimal(coefficient, precision, scale, bit_width)), "{sql}");
    }
    assert!(query("SELECT '301.02'::DECIMAL(65) AS v").await.is_err());
}

#[tokio::test]
async fn explicit_decimal_cast_uses_half_even_rounding_and_preserves_nulls() {
    for (text, coefficient) in [("1.245", 124), ("1.255", 126), ("-1.245", -124), ("-1.255", -126)] {
        assert_eq!(query(&format!("SELECT '{text}'::DECIMAL(5, 2)")).await.unwrap().1, decimal(coefficient, 5, 2, 128));
    }
    assert_eq!(query("SELECT NULL::DECIMAL(5, 2)").await.unwrap(), (LogicalType::Null, Value::Null));
    assert_eq!(query("SELECT NULL::DECIMAL(0, 3)").await.unwrap(), (LogicalType::Null, Value::Null));
    assert!(query("SELECT 1000::DECIMAL(3)").await.is_err());
    assert!(query("SELECT 1::DECIMAL(0)").await.is_err());
    assert!(query("SELECT 1::DECIMAL(3, -1)").await.is_err());
}

#[tokio::test]
async fn array_decimal_casts_apply_options_and_preserve_element_nulls() {
    assert_eq!(query("SELECT ARRAY['1.245', NULL, '1.255']::DECIMAL(5,2)[]").await.unwrap(),
        (LogicalType::list(LogicalType::Scalar(ScalarType::Decimal { precision: 5, scale: 2, bit_width: 128 }).maybe()), Value::List(vec![decimal(124, 5, 2, 128), Value::Null, decimal(126, 5, 2, 128)])));
    assert_eq!(query("SELECT '{1.245,NULL,1.255}'::DECIMAL(5,2)[]").await.unwrap().1,
        Value::List(vec![decimal(124, 5, 2, 128), Value::Null, decimal(126, 5, 2, 128)]));
}

#[tokio::test]
async fn unqualified_decimal_cast_preserves_mixed_decimal_union_metadata() {
    let context = context();
    let sql = "SELECT v::DECIMAL FROM (VALUES ('1.25'::DECIMAL(4,2)), ('1.234'::DECIMAL(5,3))) AS t(v)";
    let Statement::Query(relation) = prepare(sql, &context.catalog, &PrepareOptions::default()).unwrap().statement else { panic!("Expected query") };
    let expected = [decimal(125, 4, 2, 128), decimal(1234, 5, 3, 128)];
    assert_eq!(relation.schema().fields()[0].data_type, LogicalType::join_all(&expected.iter().map(Value::logical_type).collect::<Vec<_>>()));
    let mut cursor = PreparedQuery::new(relation).unwrap().execute(context, Vec::new()).await.unwrap();
    let mut actual = Vec::new();
    while let Some(page) = cursor.next_batch().await.unwrap() {
        for row in 0..page.batch().num_rows() { actual.push(read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), row).unwrap()); }
    }
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn decimal_casts_retain_signed32_scale_and_round_result_metadata() {
    for (sql, coefficient, precision, scale) in [
        ("SELECT '1E-1000'::DECIMAL(32,1000)", 1, 32, 1000),
        ("SELECT '1E-2147483647'::DECIMAL(32,2147483647)", 1, 32, i32::MAX),
        ("SELECT '0'::DECIMAL(1,2147483647)", 0, 1, i32::MAX),
        ("SELECT ROUND('1.25E-1000'::DECIMAL(32,1002),1001)", 13, 32, 1001),
        ("SELECT ROUND('1.25'::DECIMAL(32,2))", 1, 32, 0),
    ] {
        assert_eq!(query(sql).await.unwrap(), (LogicalType::Scalar(ScalarType::Decimal { precision, scale, bit_width: 128 }), decimal(coefficient, precision, scale, 128)), "{sql}");
    }
    assert_eq!(query("SELECT FLOOR('1.25'::DECIMAL(32,2))").await.unwrap(), (LogicalType::Scalar(ScalarType::F64), Value::F64(1.0)));
    assert!(query("SELECT '1'::DECIMAL(32,2147483647)").await.is_err());
    assert!(query("SELECT '0'::DECIMAL(32,2147483648)").await.is_err());
    let context = context();
    assert!(prepare("SELECT ROUND('1.25'::DECIMAL(32,2), ?)", &context.catalog, &PrepareOptions::default()).is_err());
}

#[tokio::test]
async fn decimal_aggregates_use_bounded_full_scale_values() {
    assert_eq!(query("SELECT SUM(v) FROM (VALUES ('1E-1000'::DECIMAL(32,1000)), ('2E-1000'::DECIMAL(32,1000))) t(v)").await.unwrap().1, decimal(3, 32, 1000, 128));
    assert_eq!(query("SELECT AVG(v) FROM (VALUES ('1E-2147483647'::DECIMAL(32,2147483647)), ('2E-2147483647'::DECIMAL(32,2147483647))) t(v)").await.unwrap().1, Value::F64(0.0));
}

#[tokio::test]
async fn decimal_sum_raw_output_survives_identity_and_requires_valid_computed_projection() {
    let source = "(SELECT SUM(v) AS s FROM (VALUES ('9'::DECIMAL(1,0)), ('9'::DECIMAL(1,0))) t(v)) sums";
    assert_eq!(query(&format!("SELECT s AS renamed FROM {source}")).await.unwrap().1, decimal(18, 1, 0, 128));
    for projection in [
        "CAST(s AS DECIMAL(1,0))", "CAST(s AS DECIMAL)", "COALESCE(s, NULL)",
        "CASE WHEN true THEN s ELSE NULL END", "ARRAY[s]", "{'v': s}",
        "ARRAY[s]::DECIMAL(1,0)[]",
    ] {
        let result = query(&format!("SELECT {projection} FROM {source}")).await;
        assert_eq!(result.as_ref().err().map(|error| error.code.as_str()), Some("rtdb/decimal-precision"), "{projection}: {result:?}");
    }
    assert_eq!(query(&format!("SELECT s + '0'::DECIMAL(1,0) FROM {source}")).await.unwrap().1, decimal(18, 32, 0, 128));
    for projection in ["CAST(s AS DECIMAL(1,0)) > 0", "CAST(s AS DECIMAL) > 0", "COALESCE(s, NULL) > 0"] {
        assert_eq!(query(&format!("SELECT {projection} FROM {source}")).await.unwrap().1, Value::Bool(true), "{projection}");
    }
    assert_eq!(query(&format!("SELECT CAST(CAST(s AS DECIMAL(1,0)) AS TEXT) FROM {source}")).await.unwrap().1, Value::Utf8("18".into()));
    assert_eq!(query(&format!("SELECT CASE WHEN false THEN s ELSE NULL END FROM {source}")).await.unwrap().1, Value::Null);
    assert_eq!(query(&format!("SELECT s::DECIMAL(2,0) FROM {source}")).await.unwrap().1, decimal(18, 2, 0, 128));
    let negative = source.replace("'9'", "'-9'");
    assert_eq!(query(&format!("SELECT s FROM {negative}")).await.unwrap().1, decimal(-18, 1, 0, 128));
    assert_eq!(query(&format!("SELECT ARRAY[s] FROM {negative}")).await.unwrap_err().code, "rtdb/decimal-precision");
    assert_eq!(
        query("SELECT ARRAY[s] FROM (VALUES ('-9'::DECIMAL(1,0))) AS t(s)").await.unwrap().1,
        Value::List(vec![decimal(-9, 1, 0, 128)]),
    );
}
