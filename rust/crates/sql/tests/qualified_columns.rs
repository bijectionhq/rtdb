use std::{collections::BTreeMap, sync::Arc};

use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_index::{PartitionState, TransactionBuffer};
use rtdb_plan::{Catalog, ColumnQualifier, Dml, Relation, RelationNode, Statement};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{Interval, Result, TableRef, Timestamp, TransactionKey, TxId, Value};
use tokio_util::sync::CancellationToken;

fn context(transaction_scoped: bool) -> Arc<QueryContext> {
    let memory = MemoryBudget::new(128 << 20);
    let mut databases = BTreeMap::new();
    for (database, number) in [("xtdb", 10), ("archive", 20)] {
        if transaction_scoped && database != "xtdb" { continue; }
        let state = PartitionState::empty(Default::default()).unwrap();
        let mut tx = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(0), system_time: Timestamp(100) }, Arc::clone(&memory)).unwrap();
        for (schema, table, value) in [("public", "foo", number), ("other", "foo", number + 20)] {
            let payload = Value::Struct(BTreeMap::from([("leaf".into(), Value::I64(7)), ("child".into(), Value::Struct(BTreeMap::from([("leaf".into(), Value::I64(8))])))]));
            tx.put(TableRef::new(schema, table), BTreeMap::from([("_id".into(), Value::I64(1)), ("v".into(), Value::I64(value)), ("payload".into(), payload)]), Interval::ALL).unwrap();
        }
        tx.put(TableRef::new("CaseSchema", "CaseTable"), BTreeMap::from([("_id".into(), Value::I64(1)), ("CaseColumn".into(), Value::I64(40))]), Interval::ALL).unwrap();
        state.apply(&tx.stage().unwrap()).unwrap();
        databases.insert(database.into(), DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) });
    }
    let catalog = CatalogSnapshot::new("xtdb", databases, transaction_scoped).unwrap();
    QueryContext::new(catalog, memory, Timestamp(100), "UTC", QueryOptions { batch_rows: 1, ..Default::default() }, &CancellationToken::new()).unwrap()
}

async fn query(sql: &str) -> Result<Vec<Vec<Value>>> {
    let context = context(false);
    let Statement::Query(relation) = prepare(sql, &context.catalog, &PrepareOptions::default())?.statement else { panic!("Expected query") };
    let mut cursor = PreparedQuery::new(relation)?.execute(context, vec![]).await?;
    let mut rows = Vec::new();
    while let Some(page) = cursor.next_batch().await? {
        for row in 0..page.batch().num_rows() {
            rows.push(page.batch().schema().fields().iter().zip(page.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?);
        }
    }
    Ok(rows)
}

fn scans(relation: &Relation) -> Vec<&rtdb_plan::Scan> {
    match relation.node() {
        RelationNode::Scan(scan) => vec![scan],
        _ => relation.children().into_iter().flat_map(|child| scans(child)).collect(),
    }
}

#[test]
fn schema_qualified_names_keep_the_original_upstream_query_identities() {
    let context = context(false);
    for (identity, sql) in [
        ("test-schema-qualified-names-fully-qualified", "SELECT information_schema.columns.column_name FROM information_schema.columns"),
        ("test-schema-qualified-names-aliased-table", "SELECT f.column_name FROM information_schema.columns AS f"),
        ("test-schema-qualified-names-implict-pg_catalog", "SELECT pg_attribute.attname FROM pg_attribute"),
        ("test-schema-qualified-names-unqualified-col-ref", "SELECT pg_attribute.attname FROM pg_catalog.pg_attribute"),
        ("test-schema-qualified-names-qualified-col-ref", "SELECT pg_catalog.pg_attribute.attname FROM pg_attribute"),
    ] {
        let prepared = prepare(sql, &context.catalog, &PrepareOptions::default()).unwrap_or_else(|error| panic!("{identity}: {error}"));
        let Statement::Query(relation) = prepared.statement else { panic!("Expected query") };
        assert_eq!(relation.schema().len(), 1, "{identity}");
    }
}

#[tokio::test]
async fn base_columns_accept_schema_and_database_and_keep_alias_identity() {
    for column in ["v", "foo.v", "public.foo.v", "xtdb.public.foo.v", "ignored.xtdb.public.foo.v"] {
        assert_eq!(query(&format!("SELECT {column} FROM public.foo")).await.unwrap(), [vec![Value::I64(10)]], "{column}");
    }
    for column in ["f.v", "public.f.v", "xtdb.public.f.v"] {
        assert_eq!(query(&format!("SELECT {column} FROM public.foo AS f")).await.unwrap(), [vec![Value::I64(10)]], "{column}");
    }
    assert_eq!(query("SELECT f.* FROM public.foo AS f").await.unwrap()[0].len(), 3);
    assert_eq!(query("SELECT \"xtdb\".\"CaseSchema\".\"Alias\".\"CaseColumn\" FROM \"CaseSchema\".\"CaseTable\" AS \"Alias\"").await.unwrap(), [vec![Value::I64(40)]]);
    let context = context(false);
    for sql in [
        "SELECT other.foo.v FROM public.foo", "SELECT archive.public.foo.v FROM public.foo",
        "SELECT public.foo.v FROM public.foo AS f", "SELECT public.alias.v FROM public.foo AS f",
        "SELECT CaseSchema.CaseTable.CaseColumn FROM \"CaseSchema\".\"CaseTable\"",
    ] { assert_eq!(prepare(sql, &context.catalog, &PrepareOptions::default()).unwrap_err().code, "xtdb/column-not-found", "{sql}"); }
}

#[tokio::test]
async fn database_and_schema_qualification_disambiguate_same_named_relations() {
    assert_eq!(query("SELECT xtdb.public.foo.v AS current_value, archive.public.foo.v AS archived_value FROM xtdb.public.foo, archive.public.foo").await.unwrap(), [vec![Value::I64(10), Value::I64(20)]]);
    assert_eq!(query("SELECT public.foo.v AS public_value, other.foo.v AS other_value FROM public.foo, other.foo").await.unwrap(), [vec![Value::I64(10), Value::I64(30)]]);
    assert_eq!(query("SELECT archive.public.foo.v FROM archive.foo").await.unwrap(), [vec![Value::I64(20)]]);
    for column in ["v", "foo.v", "public.foo.v"] {
        let sql = format!("SELECT {column} FROM xtdb.public.foo, archive.public.foo");
        assert_eq!(prepare(&sql, &context(false).catalog, &PrepareOptions::default()).unwrap_err().code, "xtdb/ambiguous-column");
    }
}

#[tokio::test]
async fn derived_cte_and_unnest_aliases_ignore_leading_namespace_components() {
    for sql in [
        "SELECT unknown.namespace.d.v FROM (SELECT v FROM foo) AS d",
        "WITH d AS (SELECT v FROM foo) SELECT unknown.namespace.d.v FROM d",
        "WITH d AS (SELECT v FROM foo) SELECT unknown.namespace.e.v FROM d AS e",
        "SELECT unknown.namespace.items.v FROM UNNEST(ARRAY[10]) AS items(v)",
    ] { assert_eq!(query(sql).await.unwrap(), [vec![Value::I64(10)]], "{sql}"); }
    assert!(prepare("SELECT public.foo.v FROM (SELECT v FROM public.foo) AS d", &context(false).catalog, &PrepareOptions::default()).is_err());
}

#[tokio::test]
async fn complete_column_chains_and_explicit_field_access_remain_distinct() {
    for sql in [
        "SELECT (xtdb.public.foo.payload).leaf FROM public.foo",
        "SELECT (public.f.payload).leaf FROM public.foo AS f",
        "SELECT ((xtdb.public.foo.payload).child).leaf - 1 FROM public.foo",
        "SELECT (d.payload).leaf FROM (SELECT payload FROM foo) AS d",
    ] { assert_eq!(query(sql).await.unwrap(), [vec![Value::I64(7)]], "{sql}"); }
    assert_eq!(query("SELECT (public.foo.payload).absent FROM public.foo").await.unwrap(), [vec![Value::Null]]);
    for sql in [
        "SELECT public.foo.payload.leaf FROM public.foo",
        "SELECT xtdb.public.foo.payload.leaf FROM public.foo",
        "SELECT information_schema.columns.column_name.my_field FROM information_schema.columns",
    ] { assert_eq!(prepare(sql, &context(false).catalog, &PrepareOptions::default()).unwrap_err().code, "xtdb/column-not-found", "{sql}"); }
}

#[tokio::test]
async fn qualification_survives_grouping_join_period_and_correlated_lateral_scopes() {
    assert_eq!(query("SELECT public.foo.v, COUNT(*) FROM public.foo GROUP BY public.foo.v HAVING xtdb.public.foo.v=10 ORDER BY xtdb.public.foo.v").await.unwrap(), [vec![Value::I64(10), Value::I64(1)]]);
    assert_eq!(query("SELECT public.f.v AS first_v, other.g.v AS second_v FROM public.foo f JOIN other.foo g ON xtdb.public.f._id=xtdb.other.g._id").await.unwrap(), [vec![Value::I64(10), Value::I64(30)]]);
    assert_eq!(query("SELECT xtdb.public.foo.v, (SELECT archive.public.foo.v FROM archive.public.foo WHERE archive.public.foo._id=xtdb.public.foo._id) FROM public.foo").await.unwrap(), [vec![Value::I64(10), Value::I64(20)]]);
    assert_eq!(query("SELECT d.v FROM public.foo f, LATERAL (SELECT xtdb.public.f.v) AS d").await.unwrap(), [vec![Value::I64(10)]]);
    assert_eq!(query("SELECT public.foo.v, ROW_NUMBER() OVER (PARTITION BY public.foo.v ORDER BY xtdb.public.foo._id) AS n FROM public.foo").await.unwrap(), [vec![Value::I64(10), Value::I64(1)]]);
    assert_eq!(query("SELECT public.foo._valid_time, xtdb.public.foo._system_time FROM public.foo").await.unwrap(), [vec![Value::TimestampTzRange { lower: i64::MIN, upper: i64::MAX }, Value::TimestampTzRange { lower: 100, upper: i64::MAX }]]);
    for sql in [
        "SELECT d.v FROM public.foo f, (SELECT xtdb.public.f.v) AS d",
        "SELECT public.foo._valid_time FROM public.foo AS f",
        "SELECT public.foo.* FROM public.foo", "SELECT xtdb.public.foo.* FROM public.foo",
    ] { assert!(prepare(sql, &context(false).catalog, &PrepareOptions::default()).is_err(), "{sql}"); }
}

#[test]
fn mutation_alias_scopes_retain_their_reference_prefix_rules_and_transaction_database() {
    let context = context(true);
    for sql in [
        "UPDATE public.foo AS f SET v=ignored.schema.f.v WHERE unknown.f._id=1",
        "DELETE FROM public.foo AS f WHERE ignored.schema.f._id=1",
        "ERASE FROM public.foo AS f WHERE ignored.schema.f._id=1",
        "UPDATE public.foo AS f SET v=(SELECT ignored.schema.f.v)",
        "INSERT INTO target (_id,v) SELECT xtdb.public.foo._id,xtdb.public.foo.v FROM public.foo",
    ] {
        let prepared = prepare(sql, &context.catalog, &PrepareOptions::default()).unwrap_or_else(|error| panic!("{sql}: {error}"));
        let Statement::Dml(dml) = prepared.statement else { panic!("Expected DML") };
        let (target, input) = match dml { Dml::Insert { table, input, .. } | Dml::Update { table, input, .. } | Dml::Delete { table, input, .. } | Dml::Erase { table, input, .. } => (table, input), _ => panic!("Unexpected DML") };
        assert_eq!(target.database, "xtdb");
        assert_eq!(target.table.schema, "public");
        for scan in scans(&input) {
            assert_eq!(scan.table.name.database, "xtdb");
            if !sql.starts_with("INSERT") { assert!(scan.columns.iter().all(|field| matches!(&field.qualifier, Some(ColumnQualifier::Alias(_))))); }
        }
    }
    for sql in ["UPDATE archive.foo SET v=1", "INSERT INTO target (_id) SELECT _id FROM archive.foo", "INSERT INTO target (_id) SELECT _id FROM archive.public.foo"] {
        assert!(prepare(sql, &context.catalog, &PrepareOptions::default()).is_err(), "{sql}");
    }
    assert!(context.catalog.transaction_scoped());
}

#[tokio::test]
async fn qualified_array_subscript_keeps_column_resolution_and_nested_field_access() {
    let source = "SELECT q.items[1].v AS first, q.items[2].v AS second, (q.doc).items[1] AS nested FROM (VALUES
        (ARRAY[{v: 10}, {v: 20}], {items: ARRAY[30, 40]})) q(items, doc)";
    assert_eq!(query(source).await.unwrap(), [vec![Value::I64(10), Value::I64(20), Value::I64(30)]]);

    let source = "SELECT (q.items)[2][1] FROM (VALUES (ARRAY[ARRAY[10,20], ARRAY[30,40]])) q(items)";
    assert_eq!(query(source).await.unwrap(), [vec![Value::I64(30)]]);
    let source = "SELECT q.items[1] FROM (VALUES (ARRAY[10])) x(items)";
    assert_eq!(query(source).await.unwrap_err().code, "xtdb/column-not-found");
}
