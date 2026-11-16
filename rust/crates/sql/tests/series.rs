use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_index::PartitionState;
use rtdb_plan::{Expr, Statement};
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

async fn query(source: &str, end_exclusive: bool) -> Result<Vec<Vec<V>>> {
    let context = context();
    let options = PrepareOptions { generate_series_end_exclusive: end_exclusive, ..Default::default() };
    let Statement::Query(relation) = prepare(source, &context.catalog, &options)?.statement else { panic!("query") };
    let mut cursor = PreparedQuery::new(relation)?.execute(context, vec![]).await?;
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() { rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?); }
    }
    Ok(rows)
}

fn integers(values: &[i64]) -> V { V::List(values.iter().copied().map(V::I64).collect()) }

#[tokio::test]
async fn sql_series_injects_the_step_and_distinguishes_inclusive_and_exclusive_endpoints() {
    assert_eq!(query("SELECT GENERATE_SERIES(1,4),RANGE(1,4),RANGE(1,8,3),GENERATE_SERIES(4,1,-1)", false).await.unwrap(),
        [vec![integers(&[1,2,3,4]),integers(&[1,2,3]),integers(&[1,4,7]),integers(&[4,3,2,1])]]);
    assert_eq!(query("SELECT RANGE(10,3),RANGE(1,1),RANGE(1,2,2),GENERATE_SERIES(1,1)", false).await.unwrap(),
        [vec![integers(&[]),integers(&[]),integers(&[1]),integers(&[1])]]);
    assert_eq!(query("SELECT GENERATE_SERIES(1,4),RANGE(1,4),GENERATE_SERIES(4,1,-1)", true).await.unwrap(),
        [vec![integers(&[1,2,3]),integers(&[1,2,3]),integers(&[4,3,2])]]);
    assert_eq!(query("SELECT custom.GENERATE_SERIES(1,2),custom.RANGE(1,2),GENERATE_SERIES(NULL,2)", false).await.unwrap(),
        [vec![integers(&[1,2]),integers(&[1]),V::Null]]);
}

#[test]
fn series_plans_capture_the_explicit_compatibility_flag() {
    let context = context();
    for (end_exclusive, name, inclusive) in [(false,"GENERATE_SERIES",true),(true,"GENERATE_SERIES",false),(false,"RANGE",false),(true,"RANGE",false)] {
        let options = PrepareOptions { generate_series_end_exclusive: end_exclusive, ..Default::default() };
        let Statement::Query(relation) = prepare(&format!("SELECT {name}(1,4)"), &context.catalog, &options).unwrap().statement else { panic!("query") };
        let mut calls = Vec::new();
        for expression in relation.expressions() {
            expression.visit(&mut |expression| if let Expr::Call { name,args,.. } = expression {
                if name == "generate_series" { calls.push(args.clone()); }
            });
        }
        assert_eq!(calls, [vec![Expr::Literal(V::I64(1)),Expr::Literal(V::I64(4)),Expr::Literal(V::I64(1)),Expr::Literal(V::Bool(inclusive))]]);
    }
}

#[test]
fn sql_series_admission_keeps_dedicated_spelling_arity_and_qualification() {
    let context = context();
    for expression in ["GENERATE_SERIES(1)","RANGE(1)","GENERATE_SERIES(1,2,1,TRUE)","RANGE(1,2,1,FALSE)",r#""generate_series"(1,2)"#,r#"custom."range"(1,2)"#,"catalog.schema.GENERATE_SERIES(1,2)"] {
        assert!(prepare(&format!("SELECT {expression}"), &context.catalog, &PrepareOptions::default()).is_err(), "{expression}");
        assert!(prepare(&format!("SETTING SNAPSHOT_TIME={expression} SELECT 1"), &context.catalog, &PrepareOptions::default()).is_err(), "{expression}");
    }
    assert!(prepare("SETTING SNAPSHOT_TIME=custom.GENERATE_SERIES(1,4) SELECT 1", &context.catalog, &PrepareOptions::default()).is_ok());
}

fn labels(source: &str) -> Vec<String> {
    let context = context();
    let Statement::Query(relation) = prepare(source, &context.catalog, &PrepareOptions::default()).unwrap().statement else { panic!("query") };
    relation.schema().fields().iter().map(|field| field.name.clone()).collect()
}

#[tokio::test]
async fn series_tables_bind_aliases_lateral_arguments_and_ordinality() {
    assert_eq!(query("SELECT * FROM GENERATE_SERIES(1,3) AS g", false).await.unwrap(), [vec![V::I64(1)],vec![V::I64(2)],vec![V::I64(3)]]);
    assert_eq!(query("SELECT * FROM GENERATE_SERIES(1,3) AS g", true).await.unwrap(), [vec![V::I64(1)],vec![V::I64(2)]]);
    assert_eq!(query("SELECT stop,v,n FROM (VALUES (2),(4)) AS limits(stop),custom.RANGE(1,limits.stop) WITH ORDINALITY AS g(v,n) ORDER BY stop,n", false).await.unwrap(),
        [vec![V::I64(2),V::I64(1),V::I64(1)],vec![V::I64(4),V::I64(1),V::I64(1)],vec![V::I64(4),V::I64(2),V::I64(2)],vec![V::I64(4),V::I64(3),V::I64(3)]]);
    assert!(query("SELECT * FROM GENERATE_SERIES(NULL,3) WITH ORDINALITY AS g(v,n)", false).await.unwrap().is_empty());
    assert!(query("SELECT * FROM RANGE(3,1) AS g(v)", false).await.unwrap().is_empty());
    assert_eq!(query("SELECT * FROM GENERATE_SERIES(DATE '2024-01-31',DATE '2024-04-01',INTERVAL '1' MONTH) AS g(day)", false).await.unwrap(),
        [vec![V::DateDay(19753)],vec![V::DateDay(19782)],vec![V::DateDay(19811)]]);
}

#[test]
fn series_and_unnest_wildcards_assign_labels_within_each_wildcard() {
    for source in ["SELECT * FROM GENERATE_SERIES(1,3) WITH ORDINALITY AS g", "SELECT * FROM UNNEST(ARRAY[1,2]) WITH ORDINALITY AS g"] {
        assert_eq!(labels(source), ["_column_1","_column_2"]);
    }
    assert_eq!(labels("SELECT 42 AS answer,g.* FROM GENERATE_SERIES(1,3) WITH ORDINALITY AS g"), ["answer","_column_1","_column_2"]);
    assert_eq!(labels("SELECT * FROM (VALUES (9)) AS t(k),RANGE(1,3) AS g"), ["k","_column_2"]);
    assert_eq!(labels("SELECT * FROM GENERATE_SERIES(1,3) WITH ORDINALITY AS g(value,position)"), ["value","position"]);
    assert_eq!(labels("SELECT q._column_1 FROM (SELECT * FROM RANGE(1,3) AS g) AS q"), ["_column_1"]);
    assert_eq!(labels("SELECT GENERATE_SERIES(1,3),RANGE(1,3)"), ["_column_1","_column_2"]);
    let context = context();
    for source in ["SELECT _column_1 FROM RANGE(1,3) AS g", "SELECT * FROM RANGE(1,3)", "SELECT * FROM RANGE(1,3) AS g(x,y)", "SELECT * FROM RANGE(1,3) WITH ORDINALITY AS g(x)", "SELECT * FROM unknown(1,3) AS g", r#"SELECT * FROM "range"(1,3) AS g"#, "SELECT * FROM a.b.RANGE(1,3) AS g", "SELECT * FROM RANGE(1,3,1,TRUE) AS g"] {
        assert!(prepare(source, &context.catalog, &PrepareOptions::default()).is_err(), "{source}");
    }
}

#[tokio::test]
async fn series_table_limit_keeps_a_billion_element_integer_reader_bounded() {
    assert_eq!(query("SELECT value FROM RANGE(0,2147483647) AS g(value) LIMIT 3", false).await.unwrap(), [vec![V::I64(0)],vec![V::I64(1)],vec![V::I64(2)]]);
}

#[tokio::test(start_paused = true)]
async fn list_tables_discard_nonlist_payloads_but_preserve_controls_and_polymorphic_boxes() {
    for source in ["SELECT * FROM UNNEST(1) AS t(x)", "SELECT * FROM UNNEST(1/0) AS t(x)", "SELECT * FROM UNNEST(PERIOD(DATE '2020-01-03',DATE '2020-01-01')) AS t(x)", "SELECT * FROM UNNEST(PG_SLEEP(60.0)) AS t(x)", "SELECT * FROM UNNEST(CASE WHEN FALSE THEN ARRAY[1] ELSE 42 END) AS t(x)"] {
        assert!(tokio::time::timeout(std::time::Duration::from_secs(1),query(source,false)).await.unwrap().unwrap().is_empty(),"{source}");
    }
    assert_eq!(query("SELECT * FROM UNNEST(CASE WHEN TRUE THEN ARRAY[1] ELSE 42 END) AS t(x)",false).await.unwrap(),[vec![V::I64(1)]]);
    for source in ["SELECT * FROM UNNEST(CASE WHEN 1/0=0 THEN ARRAY[1] ELSE 42 END) AS t(x)", "SELECT * FROM UNNEST(CASE WHEN FALSE THEN ARRAY[1] ELSE 1/0 END) AS t(x)"] {
        assert!(query(source,false).await.is_err(),"{source}");
    }
    assert!(tokio::time::timeout(std::time::Duration::from_secs(1),query("SELECT * FROM UNNEST(CASE WHEN FALSE THEN ARRAY[1] ELSE PG_SLEEP(60.0) END) AS t(x)",false)).await.is_err());
}

#[test]
fn non_list_extensions_advertise_null_table_elements() {
    let context = context();
    let Statement::Query(relation) = prepare("SELECT * FROM UNNEST(PERIOD(DATE '2020-01-01',DATE '2020-01-03')) AS t(x)",&context.catalog,&PrepareOptions::default()).unwrap().statement else { panic!("query") };
    assert_eq!(relation.schema().fields()[0].data_type,rtdb_types::LogicalType::Null);
}
