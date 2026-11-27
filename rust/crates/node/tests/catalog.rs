use rtdb_types::StructFields;
use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_index::{PartitionState, TransactionBuffer};
use rtdb_node::{PreparedSql, ResultSignaturePolicy};
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows, CatalogRuntime, CatalogUser, CatalogUsers};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{Interval, Result, TableRef, Timestamp, TransactionKey, TxId, Value};
use tokio_util::sync::CancellationToken;

fn context(with_runtime: bool, document: Option<StructFields<Value>>) -> Arc<QueryContext> {
    let memory = MemoryBudget::new(1 << 28);
    let state = PartitionState::empty(Default::default()).unwrap();
    if let Some(document) = document {
        let mut tx = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(0), system_time: Timestamp(1) }, Arc::clone(&memory)).unwrap();
        tx.put(TableRef::new("public", "docs"), document, Interval::from_micros(0, i64::MAX).unwrap()).unwrap();
        state.apply(&tx.stage().unwrap()).unwrap();
    }
    let database = DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let mut catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    if with_runtime { catalog = catalog.with_runtime(Arc::new(Runtime(Arc::clone(&memory)))); }
    QueryContext::new(catalog, memory, Timestamp(10), "UTC", QueryOptions { batch_rows: 2, ..Default::default() }, &CancellationToken::new()).unwrap()
}
struct Runtime(Arc<MemoryBudget>);
impl CatalogUsers for Runtime {
    fn users(&self) -> Result<CatalogRows<CatalogUser>> { Ok(CatalogRows::new(vec![CatalogUser { name: "alice".into(), superuser: false }], self.0.reserve(4096)?)) }
    fn is_superuser(&self, user: &str) -> bool { user == "xtdb" }
}
impl CatalogMetrics for Runtime {
    fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> {
        Ok(CatalogRows::new(vec![CatalogMetric::Gauge { name: "xtdb.query.open".into(), tags: BTreeMap::from([("db".into(), "xtdb".into())]), unit: None, value: 2.0 }], self.0.reserve(4096)?))
    }
}
#[async_trait]
impl CatalogRuntime for Runtime {
    async fn memberships(&self) -> Result<CatalogRows<(String, String)>> {
        Ok(CatalogRows::new(vec![
            ("alice".into(), "reader".into()),
            ("xtdb".into(), "alice".into()),
            ("member\u{ff21}".into(), "role\u{ff21}".into()),
            ("member\u{10400}".into(), "role\u{10400}".into()),
        ], self.0.reserve(4096)?))
    }
}
async fn query(context: &Arc<QueryContext>, sql: &str) -> Vec<Vec<Value>> {
    let prepared = PreparedSql::prepare(sql, context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut cursor = prepared.open_query(Arc::clone(context), vec![]).await.unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await.unwrap() {
        let schema = batch.batch().schema();
        for row in 0..batch.batch().num_rows() { rows.push(schema.fields().iter().enumerate().map(|(column, field)| read_value(field, batch.batch().column(column).as_ref(), row).unwrap()).collect()); }
    }
    rows
}

#[tokio::test]
async fn information_schema_test_pg_namespace_has_reference_oids() {
    let context = context(false, None);
    assert_eq!(query(&context, "SELECT oid, nspname, nspowner, nspacl FROM pg_namespace ORDER BY nspname").await, vec![
        vec![Value::I32(1980112537), Value::Utf8("information_schema".into()), Value::I32(1376455703), Value::Null],
        vec![Value::I32(2125819141), Value::Utf8("pg_catalog".into()), Value::I32(1376455703), Value::Null],
        vec![Value::I32(1106696632), Value::Utf8("public".into()), Value::I32(1376455703), Value::Null],
        vec![Value::I32(683075021), Value::Utf8("xt".into()), Value::I32(1376455703), Value::Null],
    ]);
}

#[tokio::test]
async fn information_schema_npgsql_get_schema_columns_query_joins_type_descriptors() {
    let context = context(false, Some(StructFields::from([("_id".into(), Value::I64(1)), ("s".into(), Value::Utf8("a".into())), ("ints".into(), Value::list(vec![Value::I64(1), Value::I64(2)]))])));
    assert_eq!(query(&context, "SELECT column_name, format_type(typ.oid, NULL) AS data_type FROM information_schema.columns JOIN pg_namespace AS ns ON ns.nspname = udt_schema JOIN pg_type AS typ ON typ.typnamespace = ns.oid AND typname = udt_name WHERE table_name = 'docs' AND column_name NOT IN ('_system_from', '_system_to', '_valid_from', '_valid_to') ORDER BY column_name").await,
        vec![vec![Value::Utf8("_id".into()), Value::Utf8("bigint".into())], vec![Value::Utf8("ints".into()), Value::Utf8("bigint[]".into())], vec![Value::Utf8("s".into()), Value::Utf8("text".into())]]);
    assert_eq!(query(&context, "SELECT character_set_catalog, character_set_schema, collation_catalog FROM information_schema.columns WHERE table_name = 'docs' AND column_name = 's'").await, vec![vec![Value::Null, Value::Null, Value::Null]]);
    assert_eq!(query(&context, "SELECT xt_type FROM information_schema.columns WHERE table_name = 'docs' AND column_name = '_valid_to'").await, vec![vec![Value::Utf8("[:? :instant]".into())]]);
}

#[tokio::test]
async fn catalog_roles_share_a_namespace_and_sort_login_names_before_granted_roles() {
    let context = context(true, None);
    assert_eq!(query(&context, "SELECT rolname, rolsuper, rolcanlogin FROM pg_roles").await, vec![
        vec![Value::Utf8("alice".into()), Value::Bool(false), Value::Bool(true)],
        vec![Value::Utf8("member\u{10400}".into()), Value::Bool(false), Value::Bool(true)],
        vec![Value::Utf8("member\u{ff21}".into()), Value::Bool(false), Value::Bool(true)],
        vec![Value::Utf8("xtdb".into()), Value::Bool(true), Value::Bool(true)],
        vec![Value::Utf8("reader".into()), Value::Bool(false), Value::Bool(false)],
        vec![Value::Utf8("role\u{10400}".into()), Value::Bool(false), Value::Bool(false)],
        vec![Value::Utf8("role\u{ff21}".into()), Value::Bool(false), Value::Bool(false)],
    ]);
    assert_eq!(query(&context, "SELECT passwd IS NULL AS redacted FROM pg_user").await, vec![vec![Value::Bool(true)]]);
}

#[tokio::test]
async fn metrics_tags_stream_as_arrow_and_declared_empty_catalogs_keep_their_schemas() {
    let missing = context(false, None);
    let context = context(true, None);
    assert_eq!(query(&context, "SELECT name, tags, value FROM xt.metrics_gauges").await, vec![vec![Value::Utf8("xtdb.query.open".into()), Value::Map { entries: vec![(Value::Utf8("db".into()), Value::Utf8("xtdb".into()))], sorted: false }, Value::F64(2.0)]]);
    let prepared = PreparedSql::prepare("SELECT classid, refobjid FROM pg_depend", &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
    assert_eq!(prepared.prepared_schema().unwrap().len(), 2);
    assert!(query(&context, "SELECT * FROM pg_depend").await.is_empty());
    let prepared = PreparedSql::prepare("SELECT usename FROM pg_user", &missing, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut cursor = prepared.open_query(missing, vec![]).await.unwrap();
    assert_eq!(cursor.next_batch().await.unwrap_err().code, "rtdb.query/catalog-runtime-unavailable");
}

#[tokio::test]
async fn hidden_temporal_fields_bind_explicitly_and_stay_out_of_aliased_wildcards() {
    let context = context(false, Some(StructFields::from([("_id".into(), Value::I64(1)), ("v".into(), Value::I64(2))])));
    assert_eq!(query(&context, "SELECT d.* FROM docs AS d(_id, v)").await, vec![vec![Value::I64(1), Value::I64(2)]]);
    assert_eq!(query(&context, "SELECT _system_from FROM docs FOR ALL SYSTEM_TIME").await, vec![vec![Value::Timestamp { value: 1, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) }]]);
    assert_eq!(query(&context, "SELECT _system_from, _valid_to FROM docs FOR ALL SYSTEM_TIME").await, vec![vec![Value::Timestamp { value: 1, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) }, Value::Null]]);
}

#[tokio::test]
async fn catalog_column_ordinals_put_internal_names_first_and_compare_utf16_within_each_group() {
    let document = StructFields::from([
        ("_id".into(), Value::I64(1)),
        ("alpha".into(), Value::I64(2)),
        ("zeta".into(), Value::I64(3)),
        ("\u{ff21}".into(), Value::I64(4)),
        ("\u{10400}".into(), Value::I64(5)),
    ]);
    let context = context(false, Some(document));
    let expected = ["_id", "_system_from", "_system_to", "_valid_from", "_valid_to", "alpha", "zeta", "\u{10400}", "\u{ff21}"]
        .into_iter().enumerate()
        .map(|(index, name)| vec![Value::Utf8(name.into()), Value::I32(index as i32 + 1)])
        .collect::<Vec<_>>();
    assert_eq!(query(&context,
        "SELECT column_name, ordinal_position FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = 'docs' ORDER BY ordinal_position"
    ).await, expected);
    assert_eq!(query(&context,
        "SELECT attname, attnum FROM pg_attribute JOIN pg_class ON attrelid = oid
         WHERE relname = 'docs' ORDER BY attnum"
    ).await, expected);
}

#[tokio::test]
async fn pg_type_discovers_native_float_array_element_types() {
    let context = context(false, None);
    assert_eq!(query(&context,
        "SELECT e.oid AS element_oid, a.oid AS array_oid, a.typname, a.typelem FROM pg_type e
         JOIN pg_type a ON e.typarray = a.oid
         WHERE e.oid IN (700, 701) ORDER BY element_oid"
    ).await, [
        vec![Value::I32(700), Value::I32(1021), Value::Utf8("_float4".into()), Value::I32(700)],
        vec![Value::I32(701), Value::I32(1022), Value::Utf8("_float8".into()), Value::I32(701)],
    ]);
}

#[tokio::test]
async fn information_schema_select_star_projects_columns_in_alphabetical_order() {
    let context = context(false, None);
    let projected = |sql| {
        PreparedSql::prepare(sql, &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap()
            .prepared_schema().unwrap().fields().iter().map(|field| field.name.clone()).collect::<Vec<_>>()
    };
    assert_eq!(
        projected("SELECT * FROM information_schema.key_column_usage LIMIT 5"),
        ["column_name", "constraint_catalog", "constraint_name", "constraint_schema", "ordinal_position", "position_in_unique_constraint", "table_catalog", "table_name", "table_schema"],
    );
    assert_eq!(
        projected("SELECT * FROM information_schema.table_constraints LIMIT 5"),
        ["constraint_catalog", "constraint_name", "constraint_schema", "constraint_type", "initially_deferred", "is_deferrable", "table_catalog", "table_name", "table_schema"],
    );
    assert_eq!(
        projected("SELECT * FROM information_schema.tables LIMIT 0"),
        ["table_catalog", "table_name", "table_schema", "table_type"],
    );
}
