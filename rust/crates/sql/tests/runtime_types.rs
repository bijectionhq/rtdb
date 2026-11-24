use std::{collections::BTreeMap,sync::Arc};
use rtdb_columnar::{MemoryBudget,read_value};
use rtdb_query::{CatalogSnapshot,DatabaseSnapshot,PreparedQuery,QueryContext,QueryOptions};
use rtdb_sql::{PrepareOptions,prepare};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{Result,Timestamp,Value as V};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    QueryContext::new(catalog,MemoryBudget::new(1 << 27),Timestamp(0),"UTC",QueryOptions::default(),&CancellationToken::new()).unwrap()
}
async fn query(source: &str,arguments: Vec<V>) -> Result<Vec<Vec<V>>> {
    let context = context();
    let result = async {
        let prepared = prepare(source,&context.catalog,&PrepareOptions::default())?;
        let mut cursor = PreparedQuery::from_prepared(prepared,&context.memory)?.execute(Arc::clone(&context),arguments).await?;
        let mut rows = vec![];
        while let Some(batch) = cursor.next_batch().await? {
            for row in 0..batch.batch().num_rows() { rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field,array)|read_value(field,array.as_ref(),row)).collect::<Result<_>>()?); }
        }
        Ok(rows)
    }.await;
    assert_eq!(context.memory.used_bytes(),0);
    result
}

#[tokio::test]
async fn hot_expression_sql_array_literals_preserve_bits_nulls_and_demand_order() {
    let rows = query("SELECT ARRAY[1, -0.0, NULL], COSINE_DISTANCE(ARRAY[v, 0], ARRAY[1, 0]) FROM (VALUES (1), (-1)) AS t(v) ORDER BY v DESC", vec![]).await.unwrap();
    for (row, score) in rows.iter().zip([0.0, 2.0]) {
        let list = row[0].list_view().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list.get(0).unwrap().as_ref(), &V::I64(1));
        let V::F64(zero) = *list.get(1).unwrap() else { panic!("expected a double"); };
        assert_eq!(zero.to_bits(), (-0.0_f64).to_bits());
        assert_eq!(list.get(2).unwrap().as_ref(), &V::Null);
        assert_eq!(row[1], V::F64(score));
    }
    assert_eq!(rows.len(), 2);
    assert_eq!(query("SELECT CARDINALITY(ARRAY[1, 1 / 0])", vec![]).await.unwrap(), [vec![V::I32(2)]]);
    assert_eq!(query("SELECT COSINE_DISTANCE(ARRAY[1, 1 / 0], ARRAY[1])", vec![]).await.unwrap_err().code, "xtdb.expression/division-by-zero");
}

#[tokio::test]
async fn prepared_parameter_types_preserve_scalar_control_and_case_boxing() {
    for sql in [
        "SELECT (CASE WHEN TRUE THEN 1 / 0 ELSE $1 END) + NULL",
        "SELECT (CASE 1 WHEN 1 THEN 1 / 0 ELSE $1 END) + NULL",
        "SELECT (1 / 0 = 0 AND $1) IS NULL",
        "SELECT (1 / 0 = 0 OR $1) IS NULL",
    ] {
        assert_eq!(query(sql,vec![V::Null]).await.unwrap_err().code,"xtdb.expression/division-by-zero","{sql}");
    }
    assert_eq!(query("SELECT (CASE WHEN TRUE THEN 1 / 0 ELSE $1 END) + NULL",vec![V::I64(1)]).await.unwrap(),[vec![V::Null]]);
}

#[tokio::test]
async fn runtime_relation_schema_survives_projection_filter_sort_and_list_expansion() {
    for sql in [
        "SELECT (CASE WHEN TRUE THEN 1 / 0 ELSE v END) + NULL FROM (VALUES (1),($1)) AS t(v)",
        "SELECT v FROM (VALUES (1),($1)) AS t(v) WHERE ((CASE WHEN TRUE THEN 1 / 0 ELSE v END) + NULL) IS NULL",
        "SELECT v FROM (VALUES (1),($1)) AS t(v) ORDER BY (CASE WHEN TRUE THEN 1 / 0 ELSE v END) + NULL",
        "SELECT x FROM (VALUES (1),($1)) AS t(v), UNNEST(ARRAY[(CASE WHEN TRUE THEN 1 / 0 ELSE v END) + NULL]) AS u(x)",
    ] {
        assert_eq!(query(sql,vec![V::Null]).await.unwrap_err().code,"xtdb.expression/division-by-zero","{sql}");
    }
}

#[tokio::test]
async fn apply_and_mark_captures_keep_the_full_bound_parameter_schema() {
    for sql in [
        "SELECT (SELECT (CASE WHEN TRUE THEN 1 / 0 ELSE t.v END) + NULL) FROM (VALUES (1), ($1)) AS t(v) LIMIT 1",
        "SELECT t.v IN (SELECT (CASE WHEN TRUE THEN 1 / 0 ELSE t.v END) + NULL) FROM (VALUES (1), ($1)) AS t(v) LIMIT 1",
        "SELECT (SELECT (SELECT (CASE WHEN TRUE THEN 1 / 0 ELSE t.v END) + NULL)) FROM (VALUES (1), ($1)) AS t(v) LIMIT 1",
    ] {
        assert_eq!(query(sql,vec![V::Null]).await.unwrap_err().code,"xtdb.expression/division-by-zero","{sql}");
    }
    assert_eq!(query("SELECT (SELECT (CASE WHEN TRUE THEN 1 / 0 ELSE t.v END) + NULL) FROM (VALUES (1), ($1)) AS t(v) LIMIT 1",vec![V::I64(2)]).await.unwrap(),[vec![V::Null]]);
}
