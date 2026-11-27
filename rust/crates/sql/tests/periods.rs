use rtdb_types::StructFields;
use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_index::{PartitionState, TransactionBuffer};
use rtdb_plan::{Expr, Relation, RelationNode, Schema, Statement};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{Mutation, PrepareOptions, prepare, prepare_tx};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_trie::TrieConfig;
use rtdb_types::{Interval, LogicalType, Result, TableRef, TimeUnit, Timestamp, TransactionKey, TxId, Value as V};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let memory = MemoryBudget::new(1 << 27);
    let state = PartitionState::empty(TrieConfig::new(32, 32).unwrap()).unwrap();
    let mut first = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(0), system_time: Timestamp(100) }, Arc::clone(&memory)).unwrap();
    first.put(TableRef::new("public", "foo"), StructFields::from([("_id".into(), V::I64(1)), ("v".into(), V::Utf8("old".into()))]), Interval::new(Timestamp(10), Timestamp(30)).unwrap()).unwrap();
    first.put(TableRef::new("public", "bar"), StructFields::from([("_id".into(), V::I64(2)), ("v".into(), V::Utf8("bar".into()))]), Interval::new(Timestamp(20), Timestamp::MAX).unwrap()).unwrap();
    first.put(TableRef::new("xt", "txs"), StructFields::from([("_id".into(), V::I64(3))]), Interval::new(Timestamp(10), Timestamp(30)).unwrap()).unwrap();
    first.put(TableRef::new("xt", "events"), StructFields::from([("_id".into(), V::I64(4))]), Interval::new(Timestamp(10), Timestamp(30)).unwrap()).unwrap();
    state.apply(&first.stage().unwrap()).unwrap();
    let mut second = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(1), system_time: Timestamp(200) }, Arc::clone(&memory)).unwrap();
    second.put(TableRef::new("public", "foo"), StructFields::from([("_id".into(), V::I64(1)), ("v".into(), V::Utf8("new".into()))]), Interval::new(Timestamp(10), Timestamp(30)).unwrap()).unwrap();
    state.apply(&second.stage().unwrap()).unwrap();
    let database = DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, memory, Timestamp(25), "UTC", QueryOptions { batch_rows: 1, ..Default::default() }, &CancellationToken::new()).unwrap()
}

async fn collect(relation: Arc<Relation>, context: Arc<QueryContext>) -> Result<(Schema, Vec<Vec<V>>)> {
    let schema = relation.schema().clone();
    let mut cursor = PreparedQuery::new(relation)?.execute(context, vec![]).await?;
    let mut rows = Vec::new();
    while let Some(page) = cursor.next_batch().await? {
        let batch = page.batch();
        for row in 0..batch.num_rows() {
            rows.push(batch.schema().fields().iter().zip(batch.columns()).map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?);
        }
    }
    Ok((schema, rows))
}

async fn query(sql: &str) -> Result<(Schema, Vec<Vec<V>>)> {
    let context = context();
    let Statement::Query(relation) = prepare(sql, &context.catalog, &PrepareOptions::default())?.statement else { panic!("Expected query") };
    collect(relation, context).await
}

fn timestamp(value: i64) -> V { V::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) } }
fn period(lower: i64, upper: i64) -> V { V::TimestampTzRange { lower, upper } }

#[tokio::test]
async fn tsbs_period_constructor_preserves_zoned_ticks_infinite_bounds_and_errors() {
    let zoned = "SELECT PERIOD(lo, hi) FROM (VALUES
                    (TIMESTAMP '1970-01-01T01:00:00.000010+01:00', TIMESTAMP '1970-01-01T00:00:00.000030Z'),
                    (TIMESTAMP '1970-01-01T01:00:00.000040+01:00', TIMESTAMP '1970-01-01T00:00:00.000050Z')
                  ) t(lo, hi)";
    assert_eq!(query(zoned).await.unwrap().1, [vec![period(10, 30)], vec![period(40, 50)]]);
    let sql = "SELECT PERIOD(lo, hi) AS p FROM (VALUES
                 (1, TIMESTAMP '1970-01-01T01:00:00.000010+01:00', TIMESTAMP '1970-01-01T00:00:00.000030Z'),
                 (2, NULL::timestamptz, TIMESTAMP '1970-01-01T00:00:00.000030Z'),
                 (3, TIMESTAMP '1970-01-01T00:00:00.000010Z', NULL::timestamptz)
               ) t(n, lo, hi) ORDER BY n";
    assert_eq!(query(sql).await.unwrap().1,
               [vec![period(10, 30)], vec![period(i64::MIN, 30)], vec![period(10, i64::MAX)]]);
    let inverted = "SELECT PERIOD(lo, hi) FROM (VALUES
                       (TIMESTAMP '1970-01-01T00:00:00.000030Z', TIMESTAMP '1970-01-01T00:00:00.000010Z')
                     ) t(lo, hi)";
    assert_eq!(query(inverted).await.unwrap_err().code, "xtdb/invalid-period");
}

#[tokio::test]
async fn tsbs_period_selected_bin_fields_feed_arithmetic_with_nulls() {
    let sql = "WITH readings AS (
                 SELECT _valid_time AS p, 8::DOUBLE PRECISION AS velocity FROM foo FOR ALL VALID_TIME
                 UNION ALL SELECT _valid_time, NULL::DOUBLE PRECISION FROM foo FOR ALL VALID_TIME
               )
               SELECT (bin)._from AS lo, (bin)._weight AS weight, velocity * (bin)._weight AS weighted
               FROM readings, UNNEST(RANGE_BINS(INTERVAL 'PT0.000025S', p)) AS bins(bin)
               ORDER BY lo, weighted NULLS LAST";
    assert_eq!(query(sql).await.unwrap().1, [
        vec![timestamp(0), V::F64(0.75), V::F64(6.0)], vec![timestamp(0), V::F64(0.75), V::Null],
        vec![timestamp(25), V::F64(0.25), V::F64(2.0)], vec![timestamp(25), V::F64(0.25), V::Null],
    ]);
}

#[tokio::test]
async fn tsbs_period_bins_preserve_typed_bounds_weights_and_open_end() {
    let sql = "WITH readings AS (
                 SELECT _valid_time AS p FROM foo FOR ALL VALID_TIME
                 UNION ALL SELECT _valid_time FROM bar FOR ALL VALID_TIME
               )
               SELECT (bin)._from AS lo, (bin)._to AS hi, (bin)._weight AS weight
               FROM readings, UNNEST(RANGE_BINS(INTERVAL 'PT0.000020S', p)) AS bins(bin)
               ORDER BY lo";
    let (_, rows) = query(sql).await.unwrap();
    assert_eq!(rows, [vec![timestamp(0), timestamp(20), V::F64(0.5)],
                      vec![timestamp(20), timestamp(40), V::F64(0.5)]]);

    let sql = "SELECT RANGE_BINS(INTERVAL 'PT0.000020S', p) AS bins FROM (
                 SELECT PERIOD(TIMESTAMP '1969-12-31T23:59:59.999970Z',
                               TIMESTAMP '1969-12-31T23:59:59.999990Z') AS p
               ) bounds";
    let explicit_origin = sql.replace(", p)", ", p, TIMESTAMP '1970-01-01T00:00:00Z')");
    assert_eq!(query(sql).await.unwrap().1, query(&explicit_origin).await.unwrap().1);

    let sql = "SELECT RANGE_BINS(INTERVAL 'PT0.000020S', b._valid_time) AS bins
               FROM (VALUES (TRUE), (FALSE)) matches(keep)
               LEFT JOIN bar FOR ALL VALID_TIME AS b ON matches.keep";
    assert_eq!(query(sql).await.unwrap().1, [vec![V::Null], vec![V::Null]]);
}

#[tokio::test]
async fn tsbs_period_point_left_join_preserves_boundaries_duplicates_and_null_extension() {
    let sql = "WITH hours AS (
                 SELECT h::timestamptz AS hour FROM generate_series(
                   TIMESTAMP '1970-01-01T00:00:00Z', TIMESTAMP '1970-01-01T00:00:00.000040Z',
                   INTERVAL 'PT0.000010S') AS x(h)
                 UNION ALL SELECT TIMESTAMP '1970-01-01T00:00:00.000010Z'
                 UNION ALL SELECT NULL::timestamptz
               )
               SELECT hour, COUNT(d._id) AS n FROM hours
               LEFT JOIN foo FOR ALL VALID_TIME AS d ON d._valid_time CONTAINS hours.hour
               GROUP BY hour ORDER BY hour NULLS LAST";
    let (_, rows) = query(sql).await.unwrap();
    assert_eq!(rows, [vec![timestamp(0), V::I64(0)], vec![timestamp(10), V::I64(2)],
                      vec![timestamp(20), V::I64(1)], vec![timestamp(30), V::I64(0)],
                      vec![timestamp(40), V::I64(0)], vec![V::Null, V::I64(0)]]);
    let (_, rows) = query(&sql.replace("JOIN foo", "JOIN bar")).await.unwrap();
    assert_eq!(rows, [vec![timestamp(0), V::I64(0)], vec![timestamp(10), V::I64(0)],
                      vec![timestamp(20), V::I64(1)], vec![timestamp(30), V::I64(1)],
                      vec![timestamp(40), V::I64(1)], vec![V::Null, V::I64(0)]]);
}

#[tokio::test]
async fn temporal_test_period_projections_and_upper_infinity() {
    let (schema, rows) = query("SELECT _valid_time,_system_time FROM foo").await.unwrap();
    assert_eq!(schema.fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), ["_valid_time", "_system_time"]);
    assert_eq!(rows, [vec![period(10, 30), period(200, i64::MAX)]]);
    assert!(schema.fields().iter().all(|field| !field.data_type.nullable()));
    assert_eq!(query("SELECT LOWER(_valid_time) AS f,UPPER(_valid_time) AS t,LOWER_INF(_valid_time) AS lf,UPPER_INF(_valid_time) AS ut FROM bar").await.unwrap().1,
        [vec![timestamp(20), V::Null, V::Bool(false), V::Bool(true)]]);
    assert_eq!(query("SELECT LOWER(_system_time) AS f,UPPER(_system_time) AS t,_valid_time FROM foo FOR ALL SYSTEM_TIME ORDER BY _system_from").await.unwrap().1,
        [vec![timestamp(100), timestamp(200), period(10, 30)], vec![timestamp(200), V::Null, period(10, 30)]]);
}

#[tokio::test]
async fn virtual_endpoints_keep_advertised_types_and_runtime_null_vectors() {
    let context = context();
    let Statement::Query(relation) = prepare("SELECT _iid,_valid_from,_valid_to,_system_from,_system_to,_valid_time,_system_time FROM information_schema.tables WHERE table_schema='public' AND table_name='foo'", &context.catalog, &PrepareOptions::default()).unwrap().statement else { panic!("Expected query") };
    let mut cursor = PreparedQuery::new(relation).unwrap().execute(Arc::clone(&context), vec![]).await.unwrap();
    assert!(!cursor.schema().fields()[1].data_type.nullable());
    assert!(cursor.runtime_schema().fields()[..5].iter().all(|field| field.data_type == LogicalType::Null));
    let page = cursor.next_batch().await.unwrap().unwrap();
    assert!(page.batch().schema().fields()[..5].iter().all(|field| rtdb_columnar::from_field(field).unwrap() == LogicalType::Null));
    let row = page.batch().schema().fields().iter().zip(page.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), 0).unwrap()).collect::<Vec<_>>();
    assert_eq!(row, [V::Null, V::Null, V::Null, V::Null, V::Null, period(i64::MIN, i64::MAX), period(i64::MIN, i64::MAX)]);
    assert!(cursor.next_batch().await.unwrap().is_none());
    for table in ["pg_catalog.pg_namespace", "xt.live_tables"] {
        let (_, rows) = query(&format!("SELECT _valid_from,_valid_time FROM {table} LIMIT 1")).await.unwrap();
        assert_eq!(rows, [vec![V::Null, period(i64::MIN, i64::MAX)]]);
    }
    for table in ["xt.txs", "xt.events"] {
        assert_eq!(query(&format!("SELECT _valid_from,_valid_time FROM {table}")).await.unwrap().1, [vec![timestamp(10), period(10, 30)]]);
    }
}

#[tokio::test]
async fn virtual_runtime_types_propagate_through_correlated_nested_and_outer_projections() {
    assert_eq!(query("SELECT s._valid_from AS direct,(SELECT s._valid_from) AS correlated,ARRAY[s._valid_from] AS nested FROM pg_catalog.pg_namespace s LIMIT 1").await.unwrap().1,
        [vec![V::Null, V::Null, V::list(vec![V::Null])]]);
    assert_eq!(query("SELECT s._valid_time,s._valid_from FROM foo LEFT JOIN pg_catalog.pg_namespace s ON FALSE").await.unwrap().1, [vec![V::Null, V::Null]]);
    assert_eq!(query("SELECT MIN(_valid_from),COUNT(_valid_from) FROM pg_catalog.pg_namespace").await.unwrap().1, [vec![V::Null, V::I64(0)]]);
    assert_eq!(query("SELECT COUNT(*) FROM pg_catalog.pg_namespace GROUP BY _valid_from").await.unwrap().1.len(), 1);
}

#[tokio::test]
async fn virtual_runtime_type_narrowing_preserves_negative_round_scale() {
    let value = V::Decimal(rtdb_types::DecimalValue::new(12i128.to_be_bytes().to_vec(), 19, -1, 128).unwrap());
    assert_eq!(query("SELECT ROUND(COALESCE(CAST(_valid_from AS BIGINT),123),-1) FROM pg_catalog.pg_namespace LIMIT 1").await.unwrap().1, [vec![value]]);
}

#[tokio::test]
async fn virtual_null_vectors_preserve_aggregate_factories_and_promote_array_elements() {
    let context = context();
    let sql = "SELECT ARRAY_AGG(_valid_from),ARRAY_AGG(ARRAY[_valid_from]) AS test_output_2,SUM(CAST(_valid_from AS DECIMAL)),AVG(CAST(_valid_from AS DECIMAL)),MIN(_valid_from),MAX(_valid_from) FROM (SELECT _valid_from FROM pg_catalog.pg_namespace LIMIT 1) s";
    let Statement::Query(relation) = prepare(sql, &context.catalog, &PrepareOptions::default()).unwrap().statement else { panic!("Expected query") };
    let mut cursor = PreparedQuery::new(relation).unwrap().execute(Arc::clone(&context), vec![]).await.unwrap();
    let instant = timestamp(0).logical_type();
    assert_eq!(cursor.schema().fields()[0].data_type, LogicalType::list(instant.clone()).maybe());
    let runtime = cursor.runtime_schema();
    assert_eq!(runtime.fields()[0].data_type, LogicalType::list(instant.clone().maybe()).maybe());
    assert_eq!(runtime.fields()[1].data_type, LogicalType::list(LogicalType::list(instant.clone().maybe())).maybe());
    assert_eq!(runtime.fields()[2].data_type, cursor.schema().fields()[2].data_type);
    assert_eq!(runtime.fields()[3].data_type, cursor.schema().fields()[3].data_type);
    assert_eq!(runtime.fields()[4].data_type, instant.clone().maybe());
    assert_eq!(runtime.fields()[5].data_type, instant.maybe());
    let page = cursor.next_batch().await.unwrap().unwrap();
    let row = page.batch().schema().fields().iter().zip(page.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), 0).unwrap()).collect::<Vec<_>>();
    assert_eq!(row, [V::list(vec![V::Null]), V::list(vec![V::list(vec![V::Null])]), V::Null, V::Null, V::Null, V::Null]);
    assert!(cursor.next_batch().await.unwrap().is_none());
}

#[tokio::test]
async fn correlated_aggregate_uses_the_same_runtime_null_dispatch() {
    let sql = "SELECT (SELECT SUM(CAST(s._valid_from AS DECIMAL)) FROM (VALUES (1)) v(n)),(SELECT ARRAY_AGG(s._valid_from) FROM (VALUES (1)) v(n)) FROM pg_catalog.pg_namespace s LIMIT 1";
    assert_eq!(query(sql).await.unwrap().1, [vec![V::Null, V::list(vec![V::Null])]]);
}

#[tokio::test]
async fn decimal_min_max_keep_the_declared_type_across_multiple_pages() {
    let decimal = |n: i128| V::Decimal(rtdb_types::DecimalValue::new(n.to_be_bytes().to_vec(), 4, 2, 128).unwrap());
    for (function, expected) in [("MIN", decimal(-240)), ("MAX", decimal(199))] {
        let sql = format!("SELECT {function}(CAST(n AS DECIMAL(4,2))) FROM (VALUES (1.99),(-2.40),(NULL),(1.10)) v(n)");
        let (schema, rows) = query(&sql).await.unwrap();
        assert_eq!(schema.fields()[0].data_type, expected.logical_type().maybe());
        assert_eq!(rows, [vec![expected]]);
        let sql = format!("SELECT {function}(CAST(n AS DECIMAL(4,2))) FILTER (WHERE FALSE) FROM (VALUES (1)) v(n)");
        assert_eq!(query(&sql).await.unwrap().1, [vec![V::Null]]);
    }
}

#[tokio::test]
async fn smallint_sum_dispatch_precedes_filter_and_keeps_empty_or_null_vector_input_null() {
    assert_eq!(query("SELECT SUM(1::SMALLINT) FILTER (WHERE FALSE) FROM (VALUES (1)) v(n)").await.unwrap_err().code,
        "rtdb.query/small-integer-sum");
    assert_eq!(query("SELECT SUM(1::SMALLINT) FILTER (WHERE FALSE) FROM (VALUES (1)) v(n) WHERE FALSE").await.unwrap().1,
        [vec![V::Null]]);
    let (schema, rows) = query("SELECT SUM(CAST(_valid_from AS SMALLINT)) FROM pg_catalog.pg_namespace").await.unwrap();
    assert_eq!(schema.fields()[0].data_type, V::I16(0).logical_type().maybe());
    assert_eq!(rows, [vec![V::Null]]);
}

#[tokio::test]
async fn period_projections_are_hidden_until_exported_by_select_or_declared_in_base_scope() {
    for sql in ["SELECT * FROM foo", "SELECT f.* FROM foo f", "SELECT * FROM (SELECT * FROM foo) f"] {
        assert_eq!(query(sql).await.unwrap().0.fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), ["_id", "v"]);
    }
    for sql in ["SELECT * FROM (SELECT _valid_time FROM foo) f", "WITH f AS (SELECT _valid_time FROM foo) SELECT * FROM f", "SELECT f.* FROM foo AS f(_valid_time)"] {
        assert_eq!(query(sql).await.unwrap().1, [vec![period(10, 30)]], "{sql}");
    }
    assert!(query("WITH f AS (SELECT * FROM foo) SELECT _valid_time FROM f").await.is_err());
    assert_eq!(query("SELECT f.* FROM foo AS f(v,unknown,v)").await.unwrap().1, [vec![V::Utf8("new".into()), V::Null]]);
    assert_eq!(query("SELECT f._valid_time FROM foo AS f(_system_from,_system_to,_valid_from,_valid_to)").await.unwrap().1, [vec![period(10, 30)]]);
}

#[tokio::test]
async fn correlated_and_outer_join_periods_preserve_scope_and_null_extension() {
    assert_eq!(query("SELECT (SELECT f._valid_time) AS p FROM foo f").await.unwrap().1, [vec![period(10, 30)]]);
    assert_eq!(query("SELECT f._valid_time AS f,b._valid_time AS b FROM foo f LEFT JOIN bar b ON FALSE").await.unwrap().1, [vec![period(10, 30), V::Null]]);
    assert!(query("SELECT _valid_time FROM foo CROSS JOIN bar").await.unwrap_err().code.contains("ambiguous"));
    assert_eq!(query("SELECT foo._valid_time * bar._valid_time AS intersection FROM foo JOIN bar ON foo._valid_time OVERLAPS bar._valid_time").await.unwrap().1, [vec![period(20, 30)]]);
    assert_eq!(query("SELECT v FROM foo ORDER BY LOWER(_valid_time)").await.unwrap().1, [vec![V::Utf8("new".into())]]);
    assert_eq!(query("SELECT COUNT(*) AS n FROM foo GROUP BY _valid_time").await.unwrap().1, [vec![V::I64(1)]]);
    assert_eq!(query("SELECT _valid_time FROM foo WHERE _valid_time CONTAINS TIMESTAMP '1970-01-01T00:00:00.000020Z'").await.unwrap().1, [vec![period(10, 30)]]);
    assert_eq!(query("SELECT f.\"_valid_time\" FROM foo f").await.unwrap().1, [vec![period(10, 30)]]);
    assert!(query("SELECT f.\"_VALID_TIME\" FROM foo f").await.is_err());
}

#[tokio::test]
async fn scalar_periods_support_open_bounds_intersection_and_variadic_overlaps() {
    assert_eq!(query("SELECT LOWER_INF(PERIOD(NULL,NULL)) AS lf,UPPER_INF(TSTZRANGE(NULL,NULL)) AS uf,UPPER(PERIOD(NULL,NULL)) AS u").await.unwrap().1,
        [vec![V::Bool(true), V::Bool(true), V::Null]]);
    assert_eq!(query("SELECT PERIOD(DATE '2020-01-01',DATE '2020-01-02') * PERIOD(DATE '2020-01-02',DATE '2020-01-03') AS empty").await.unwrap().1, [vec![V::Null]]);
    assert_eq!(query("SELECT OVERLAPS(PERIOD(DATE '2020-01-01',DATE '2020-01-04'),PERIOD(DATE '2020-01-02',NULL),PERIOD(DATE '2020-01-03',DATE '2020-01-05')) AS overlap").await.unwrap().1, [vec![V::Bool(true)]]);
    assert_eq!(query("SELECT TRUE = PERIOD(DATE '2020-01-01',DATE '2020-01-03') OVERLAPS PERIOD(DATE '2020-01-02',NULL) AS precedence").await.unwrap().1, [vec![V::Bool(true)]]);
}

#[tokio::test]
async fn infix_period_predicates_keep_pinned_endpoint_comparisons() {
    let left = "PERIOD(DATE '2020-01-01',DATE '2020-01-02')";
    let right = "PERIOD(DATE '2020-01-03',DATE '2020-01-04')";
    for (predicate, expected) in [
        ("OVERLAPS", false), ("STRICTLY OVERLAPS", false), ("EQUALS", false), ("CONTAINS", false),
        ("STRICTLY CONTAINS", false), ("PRECEDES", true), ("STRICTLY PRECEDES", true), ("IMMEDIATELY PRECEDES", false),
        ("SUCCEEDS", false), ("STRICTLY SUCCEEDS", false), ("IMMEDIATELY SUCCEEDS", false),
        ("LEADS", true), ("STRICTLY LEADS", true), ("IMMEDIATELY LEADS", false),
        ("LAGS", false), ("STRICTLY LAGS", false), ("IMMEDIATELY LAGS", false),
    ] {
        assert_eq!(query(&format!("SELECT {left} {predicate} {right} AS result")).await.unwrap().1, [vec![V::Bool(expected)]], "{predicate}");
    }
    assert!(query(&format!("SELECT \"leads\"({left},{right}) AS scalar_leads")).await.is_err());
    assert_eq!(query("SELECT PERIOD(NULL,NULL) EQUALS PERIOD(NULL,NULL) AS same").await.unwrap().1, [vec![V::Bool(true)]]);
    assert_eq!(query("SELECT NULL OVERLAPS PERIOD(NULL,NULL) AS unknown_overlap").await.unwrap().1, [vec![V::Null]]);
    assert!(query(&format!("SELECT LEADS({left},{right}) AS bad")).await.is_err());
}

#[tokio::test]
async fn dml_period_names_keep_physical_null_scan_and_never_become_document_fields() {
    let context = context();
    let mutation = prepare_tx("UPDATE foo SET v=LOWER(_valid_time)", &context.catalog, &PrepareOptions::default()).unwrap();
    let Mutation::Put { input, .. } = mutation else { panic!("Expected put") };
    let rows = collect(input, Arc::clone(&context)).await.unwrap().1;
    let V::Struct(doc) = &rows[0][3] else { panic!("Expected document") };
    assert_eq!(doc, &BTreeMap::from([("_id".into(), V::I64(1)), ("v".into(), V::Null)]));
    assert!(prepare_tx("ERASE FROM foo WHERE _valid_time IS NULL", &context.catalog, &PrepareOptions::default()).is_err());
    let Mutation::Delete { input, .. } = prepare_tx("DELETE FROM foo WHERE _valid_time IS NULL", &context.catalog, &PrepareOptions::default()).unwrap() else { panic!("Expected delete") };
    assert_eq!(collect(input, context).await.unwrap().1.len(), 1);
}

#[test]
fn untouched_queries_do_not_construct_unused_periods_and_predicate_errors_keep_spans() {
    let context = context();
    let Statement::Query(relation) = prepare("SELECT * FROM foo", &context.catalog, &PrepareOptions::default()).unwrap().statement else { panic!("Expected query") };
    fn has_period(relation: &Relation) -> bool {
        let mut found = false;
        for expr in relation.expressions() { expr.visit(&mut |expr| { if matches!(expr, Expr::Call { name, .. } if name == "period") { found = true; } }); }
        found || relation.children().iter().any(|child| has_period(child))
    }
    assert!(!has_period(&relation));
    let error = prepare("SELECT _valid_time CONTAINS 'bad' FROM foo", &context.catalog, &PrepareOptions::default()).unwrap_err();
    assert!(error.context.contains_key("span"));
    let Statement::Query(relation) = prepare("SELECT f._valid_time FROM foo f", &context.catalog, &PrepareOptions::default()).unwrap().statement else { panic!("Expected query") };
    let RelationNode::Project { input, .. } = relation.node() else { panic!("Expected projection") };
    assert!(matches!(input.node(), RelationNode::Project { .. }));
}
