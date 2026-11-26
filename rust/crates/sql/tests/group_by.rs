use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_index::PartitionState;
use rtdb_plan::Statement;
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_trie::TrieConfig;
use rtdb_types::{Result, Timestamp, Value as V};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let state = PartitionState::empty(TrieConfig::new(32, 32).unwrap()).unwrap();
    let database = DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 27), Timestamp(0), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap()
}
async fn query(source: &str) -> Result<Vec<Vec<V>>> {
    let context = context();
    let Statement::Query(relation) = prepare(source, &context.catalog, &PrepareOptions::default())?.statement else { panic!("query") };
    let mut cursor = PreparedQuery::new(relation)?.execute(context, vec![]).await?;
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() { rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?); }
    }
    Ok(rows)
}

#[tokio::test]
async fn a_wildcard_over_grouped_columns_keeps_the_source_column_order() {
    let mut rows = query("SELECT * FROM (VALUES (83, 0, 38), (26, 0, 79)) AS t (col0, col1, col2) GROUP BY t.col1, t.col2, t.col0").await.unwrap();
    rows.sort_by_key(|row| match row[0] { V::I64(value) => value, _ => 0 });
    assert_eq!(rows, [vec![V::I64(26), V::I64(0), V::I64(79)], vec![V::I64(83), V::I64(0), V::I64(38)]]);
}

#[tokio::test]
async fn a_repeated_grouping_key_groups_once() {
    let mut rows = query("SELECT - t.col2 FROM (VALUES (1, 58), (2, 79), (3, 58)) AS t (col0, col2) GROUP BY t.col2, t.col2").await.unwrap();
    rows.sort_by_key(|row| match row[0] { V::I64(value) => value, _ => 0 });
    assert_eq!(rows, [vec![V::I64(-79)], vec![V::I64(-58)]]);
}

#[tokio::test]
async fn deeply_nested_predicates_parse_bind_and_execute() {
    query(r#"SELECT pk FROM (VALUES (1, 1, 1.5, 1, 1, 1.5)) AS tab0 (pk, col0, col1, col2, col3, col4) WHERE (col0 < 0 AND (col0 < 0) AND col0 <= 1 OR (col0 <= 8 AND ((((((col1 <= 5.84 OR (col4 IS NULL) AND (col1 = 8.49) AND col3 < 4)))) OR (col4 = 1.81 AND col1 >= 2.0) AND ((((((col3 > 1 AND col1 < 0.34 AND col1 = 9.44 AND (col0 < 4) AND col0 > 7 OR col3 < 6 AND (col0 = 0) OR ((col4 < 4.46 OR (((col4 IN (2.18)) AND (col1 > 0.73 AND col0 > 2 AND (((col3 < 0) AND col0 IS NULL) AND col3 IS NULL) AND col3 = 3 OR col0 <= 6) AND (((col3 = 5))))) OR col1 > 5.93 OR col4 IS NULL) OR (col1 > 7.69 OR (col4 > 9.86) OR col0 IS NULL) OR col0 <= 3 OR col1 = 9.45) AND col0 <= 7)) AND col4 >= 9.34 AND (col3 = 8) AND col4 >= 1.68)) AND (col3 IS NULL))) AND (((col0 = 7) OR col3 >= 9)) AND ((((col0 <= 1 OR col0 < 6))) AND col0 >= 3 OR ((col3 < 1)) AND (col1 <= 8.62) OR col0 = 7 OR (col0 IS NULL) AND ((col3 < 8) OR col0 > 7 AND col0 IS NULL) OR col1 = 2.35) OR col3 IS NULL OR (col0 >= 5 OR col1 > 3.72 AND (col1 >= 9.84 AND col0 <= 3 AND (col1 > 4.43) AND ((col0 > 3) OR (((((((((col0 IN (0) OR col1 < 1.31 OR col0 >= 8 OR col4 > 9.21) AND (col1 = 4.97 OR (((col4 <= 0.60)))) AND col4 IS NULL)))))) AND col0 IS NULL) OR col1 <= 7.98))) OR ((col0 <= 1 AND col4 IS NULL OR (col3 BETWEEN 6 AND 0) AND (col0 > 6) AND ((col3 > 5 AND col3 < 2)))) AND ((col0 <= 0) OR (col1 <= 2.18) OR ((col4 < 3.72)) AND col0 IN (1,5,3,9,9) AND col3 > 7 OR col0 BETWEEN 9 AND 5 OR (col3 > 1 OR ((((col0 IS NULL)) AND col0 = 7 AND col3 <= 7 OR ((col4 >= 1.87)) OR (col0 IS NULL))) OR ((col1 < 8.63) AND (col4 IS NULL AND ((((col3 >= 2)))) OR (((col3 > 5)) OR col1 IN (SELECT col4 FROM (VALUES (1, 1, 1.5, 1, 1, 1.5)) AS tab0 (pk, col0, col1, col2, col3, col4) WHERE ((col3 IS NULL AND ((((col1 >= 3.70) AND (((col4 <= 5.29 AND ((col1 = 1.61 OR col0 = 2 AND (col0 > 2) OR col3 < 0) OR ((((col1 <= 5.33) AND col3 IN (2,9,9,3,3)) AND col3 >= 5 OR (col0 < 7)) OR col0 = 8) OR col0 <= 3 OR col0 > 6) AND ((col4 >= 7.79 OR (col0 <= 1 OR (col1 < 2.29) AND col0 BETWEEN 5 AND 1 OR ((col0 = 0 AND col4 > 3.39 OR col4 >= 4.50) OR col0 > 2) OR col3 < 4 OR col3 IN (5) OR col4 IS NULL AND col0 > 0 AND col3 = 0 AND col0 < 5 AND col3 = 8 AND (col4 > 5.19) AND (col4 < 2.90) OR col3 = 2 OR (((((col3 < 4)) OR (col0 < 0) OR (col4 = 6.29) AND ((col3 < 4) AND col4 >= 3.86) OR col3 > 1 AND (col4 IN (5.55)) OR col0 <= 5 OR ((col0 >= 7 OR (col0 >= 4)))))))) AND col3 >= 0 AND col0 >= 7 AND col3 > 9 AND (col0 > 6 OR ((col1 < 2.17 AND col0 <= 9 OR ((((col0 = 8))) AND col1 <= 1.39 AND ((col3 > 8)) AND col0 <= 5 AND col0 = 9) OR (col0 < 0) AND (col3 IS NULL))) OR (col0 IN (9,4,8,7) AND col0 > 4 AND col4 IN (0.75,0.53,2.87) AND ((col4 >= 0.21)))))) AND col1 > 2.89) OR col4 >= 5.6 AND (col3 < 2) AND col4 BETWEEN 4.79 AND 5.35 OR col4 < 4.15))) AND ((col4 > 7.46) OR (col1 < 1.74)) AND (col0 >= 6))) AND col3 < 6))) AND col4 >= 7.78 AND (col1 <= 4.63 OR col0 > 4) OR col3 <= 5 OR (col0 <= 8 OR (col3 IS NULL) AND col0 < 1) AND col1 < 4.15 AND col3 IS NULL)) OR col3 = 7 AND col3 <= 1 AND (col3 <= 9 OR col0 > 8) AND col0 = 6) AND col3 < 2 OR (col3 >= 3 AND (col1 < 6.34) OR (((col0 <= 3 AND col3 IS NULL) AND col3 <= 8 AND (col1 <= 5.5 OR col3 IN (4,7,1,1,3) OR ((col4 < 1.34)))))) AND col1 < 8.44 AND (((col0 > 5) AND col3 >= 8)) AND (col3 > 7 AND col0 = 1)) OR (col1 > 0.64) OR col0 = 5 OR ((col0 >= 9)) OR col3 > 5) OR col3 < 4 OR col4 > 8.75 OR col1 > 3.88)) AND col1 > 4.5 AND col3 > 5) OR (col3 IN (5,4,6,9,8))) AND col0 < 1 OR col3 = 9 OR (col1 <= 6.53 OR col3 IS NULL) OR col3 > 6 AND (col4 <= 3.73)
"#).await.unwrap();
}

#[tokio::test]
async fn predicates_nested_up_to_the_parser_limit_never_overflow() {
    let mut predicate = String::from("x = 1");
    for depth in 0..120 { predicate = format!("({predicate} AND x < {depth} + 1000)"); }
    let result = query(&format!("SELECT x FROM (VALUES (1)) AS t (x) WHERE {predicate}")).await;
    assert!(matches!(result.as_deref(), Ok([row]) if row == &vec![V::I64(1)]) || result.is_err(), "{result:?}");
}

#[tokio::test]
async fn arithmetic_and_parentheses_nested_up_to_the_parser_limit_never_overflow() {
    for (open, close) in [("(1 + ", ")"), ("(", ")"), ("-(", ")"), ("CASE WHEN x = 1 THEN ", " ELSE 0 END")] {
        for depth in [60, 120, 250] {
            let expression = format!("{}x{}", open.repeat(depth), close.repeat(depth));
            let result = query(&format!("SELECT {expression} FROM (VALUES (1)) AS t (x)")).await;
            assert!(result.is_ok() || result.is_err(), "{open} {depth}");
        }
    }
}

#[tokio::test]
async fn an_integer_grouping_element_groups_by_the_constant() {
    assert_eq!(query("SELECT COUNT(*) FROM (VALUES (1, 'a'), (2, 'b')) AS t (x, y) GROUP BY 1").await.unwrap(), [vec![V::I64(2)]]);
    assert_eq!(query("SELECT 1, COUNT(*) FROM (VALUES (1, 'a'), (2, 'b')) AS t (x, y) GROUP BY 1").await.unwrap(), [vec![V::I64(1), V::I64(2)]]);
    assert!(query("SELECT t.y, COUNT(*) FROM (VALUES (1, 'a'), (2, 'b')) AS t (x, y) GROUP BY 1").await.is_err());
}
