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
async fn postgres_null_functions_accept_catalog_rows_and_oid_casts() {
    assert_eq!(query("SELECT COL_DESCRIPTION(12345,1),COL_DESCRIPTION(12345::OID,1),PG_INDEXAM_HAS_PROPERTY(403,'can_order')").await.unwrap(), [vec![V::Null,V::Null,V::Null]]);
    assert_eq!(query("SELECT amname,pg_indexam_has_property(oid,'can_order') FROM pg_am ORDER BY amname").await.unwrap(),
        [vec![V::Utf8("btree".into()),V::Null],vec![V::Utf8("hash".into()),V::Null],vec![V::Utf8("heap".into()),V::Null]]);
}

#[tokio::test]
async fn generic_sql_function_schema_qualifiers_keep_the_reference_name_resolution() {
    assert_eq!(query("SELECT pg_catalog.format_type(20,NULL),pg_catalog.col_description(1,1),custom.pg_indexam_has_property(1,'anything'),custom.abs(-3)").await.unwrap(),
        [vec![V::Utf8("bigint".into()),V::Null,V::Null,V::I64(3)]]);
    assert!(query("SELECT custom.unknown_function(1)").await.is_err());
    assert!(query("SELECT catalog.schema.abs(1)").await.is_err());
}

#[tokio::test(start_paused = true)]
async fn sql_null_catalogue_calls_discard_payloads_but_keep_control_and_union_demands() {
    let start = tokio::time::Instant::now();
    for expression in [
        "COL_DESCRIPTION(1 / 0,1)", "PG_INDEXAM_HAS_PROPERTY(1 / 0,'anything')",
        "COL_DESCRIPTION(NULL,1 / 0)", "COL_DESCRIPTION(1 / 0,NULL)",
        "PG_INDEXAM_HAS_PROPERTY(NULL,1 / 0)", "PG_INDEXAM_HAS_PROPERTY(1 / 0,NULL)",
        "COL_DESCRIPTION(PG_SLEEP(60.0),1)", "PG_INDEXAM_HAS_PROPERTY(1,PG_SLEEP(60.0))",
    ] {
        assert_eq!(query(&format!("SELECT {expression}")).await.unwrap(), [vec![V::Null]], "{expression}");
    }
    assert_eq!(tokio::time::Instant::now(), start);
    for expression in [
        "COL_DESCRIPTION(CASE WHEN 1 / 0 = 0 THEN 1 ELSE 2 END,1)",
        "COL_DESCRIPTION(CASE WHEN TRUE THEN 1 / 0 ELSE NULL END,1)",
        "PG_INDEXAM_HAS_PROPERTY(CASE WHEN TRUE THEN 1 / 0 ELSE NULL END,'anything')",
    ] {
        assert_eq!(query(&format!("SELECT {expression}")).await.unwrap_err().code, "xtdb.expression/division-by-zero", "{expression}");
    }
}

#[tokio::test]
async fn sql_registered_names_preserve_quoted_spelling_and_ignore_only_one_schema() {
    assert_eq!(query(r#"SELECT "Any-Schema"."col_description"(1,1),"Other"."pg_indexam_has_property"(1,'anything'),"abs"(-3)"#).await.unwrap(),
        [vec![V::Null,V::Null,V::I64(3)]]);
    for expression in [
        r#""COL_DESCRIPTION"(1,1)"#, r#"custom."col-description"(1,1)"#,
        r#"custom."PG_INDEXAM_HAS_PROPERTY"(1,'anything')"#,
        r#"custom."pg-indexam-has-property"(1,'anything')"#,
        "custom.current_setting('search_path')", r#""current_schema"()"#,
        r#""coalesce"(1,2)"#, "catalog.schema.abs(1)",
    ] {
        assert!(query(&format!("SELECT {expression}")).await.is_err(), "{expression}");
        assert!(query(&format!("SETTING SNAPSHOT_TIME={expression} SELECT 1")).await.is_err(), "{expression}");
    }
}

#[test]
fn shared_scalar_aliases_remain_outside_the_sql_named_function_catalogue() {
    let context = context();
    for expression in ["is_true(TRUE)","is_false(FALSE)","distinct_from(1,2)","_meta_double(1)","compare(1,2)","compare_nulls_first(1,2)","compare_nulls_last(1,2)","get_field(1)"] {
        for statement in [format!("SELECT {expression}"),format!("SETTING SNAPSHOT_TIME={expression} SELECT 1")] {
            assert!(prepare(&statement,&context.catalog,&PrepareOptions::default()).is_err(),"{statement}");
        }
    }
}
