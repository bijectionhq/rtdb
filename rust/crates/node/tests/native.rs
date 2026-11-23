use rtdb_types::StructFields;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, atomic::{AtomicBool, AtomicI64, Ordering}};
use std::time::Duration;

use async_trait::async_trait;
use futures::FutureExt;
use rtdb_columnar::read_value;
use rtdb_format::database::proto::{DatabaseConfig, LocalLog, LocalStorage, database_config};
use rtdb_maintenance::CollectionObserver;
use rtdb_node::*;
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows, CatalogUser, CatalogUsers};
use rtdb_query::QueryCursor;
use rtdb_tx::{Documents, SqlArguments, TransactionAuthorization, TxOp};
use rtdb_types::{Anomaly, Result, TableRef, Timestamp, TxBasis, TxId, Value};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[path = "support/io.rs"]
mod io;

struct Authority;
impl TransactionAuthorization for Authority { fn check_role_membership(&self, _: &str, _: Option<&str>) -> Result<()> { Ok(()) } }
#[async_trait]
impl SessionIdentity for Authority {
    fn user(&self) -> Option<&str> { Some("xtdb") }
    async fn revalidate(&self, _: Timestamp) -> Result<()> { Ok(()) }
    fn authorize_database(&self, _: &str, _: DatabaseAccess) -> Result<()> { Ok(()) }
}
struct Observer;
impl CollectionObserver for Observer { fn table_failed(&self, table: Option<&TableRef>, error: &Anomaly) { panic!("unexpected collection failure for {table:?}: {error}"); } }
struct CatalogFixtures;
impl CatalogUsers for CatalogFixtures {
    fn users(&self) -> Result<CatalogRows<CatalogUser>> { Ok(CatalogRows::new(Vec::new(), Arc::new(()))) }
    fn is_superuser(&self, _: &str) -> bool { false }
}
impl CatalogMetrics for CatalogFixtures { fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> { Ok(CatalogRows::new(Vec::new(), Arc::new(()))) } }
struct TickClock(AtomicI64);
impl Clock for TickClock { fn now(&self) -> Timestamp { Timestamp(self.0.fetch_add(1000, Ordering::Relaxed)) } }
fn services() -> NodeServices {
    let clock = Arc::new(TickClock(AtomicI64::new(1_000_000)));
    NodeServices { resources: Arc::new(NativeResources::default().with_local_log_clock(clock.clone(), false)), clock, ..NodeServices::local(Arc::new(Authority), Arc::new(Observer), Arc::new(CatalogFixtures), Arc::new(CatalogFixtures)) }
}
fn fresh_local_services(template: &NodeServices) -> NodeServices {
    NodeServices { resources: Arc::new(NativeResources::default().with_local_log_clock(Arc::clone(&template.clock), false)), ..template.clone() }
}
fn options() -> NodeOptions {
    let mut options = NodeOptions::default();
    options.replication.assertion_interval = Duration::from_millis(1);
    options.replication.election_seed = 1;
    options.replication.flush_timeout = Duration::from_secs(3600);
    options.replication.rows_per_block = 10_000;
    options.automatic_compaction = false;
    options.automatic_gc = false;
    options
}
async fn running(options: NodeOptions, services: NodeServices) -> (Node, tokio::task::JoinHandle<Result<()>>) {
    let (node, driver) = Node::open(options, services).await.unwrap();
    (node, tokio::spawn(driver.run()))
}

#[derive(Default)]
struct PausedReadFence {
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

#[async_trait]
impl ReadFence for PausedReadFence {
    async fn confirm(&self, _: &str) -> Result<()> {
        self.entered.notify_one();
        self.resume.notified().await;
        Ok(())
    }
}

#[tokio::test]
async fn a_placed_query_refuses_a_database_detached_or_replaced_while_checking_its_fence() {
    for replace in [false, true] {
        let mut config = options();
        config.membership = Membership::Placed;
        let fence = Arc::new(PausedReadFence::default());
        let mut services = services();
        services.read_fence = Some(fence.clone());
        let (node, driver) = running(config, services).await;
        node.database_or_attach("shop".into(), Default::default()).await.unwrap().unwrap();
        let basis = QueryBasis {
            snapshot: node.snapshot_basis().unwrap(), snapshot_time: None, current_time: node.query_instant(), timezone: "UTC".into(),
        };
        let request_node = node.clone();
        let query = tokio::spawn(async move {
            request_node.query_context("shop", &basis, &Authority, &CancellationToken::new()).await
        });
        tokio::time::timeout(Duration::from_secs(5), fence.entered.notified()).await.unwrap();
        node.detach_local("shop".into()).await.unwrap();
        if replace {
            tokio::time::timeout(Duration::from_secs(5), async {
                while node.catalog().is_draining("shop") { tokio::task::yield_now().await; }
            }).await.unwrap();
            node.database_or_attach("shop".into(), Default::default()).await.unwrap().unwrap();
        }
        fence.resume.notify_one();
        let Err(error) = query.await.unwrap() else { panic!("the former holding accepted a query"); };
        assert_eq!(error.code, NOT_HELD);
        stop(&node, driver).await;
    }
}

#[tokio::test]
async fn a_placed_session_cannot_reopen_a_replaced_database() {
    let mut config = options();
    config.membership = Membership::Placed;
    let (node, driver) = running(config, services()).await;
    node.database_or_attach("shop".into(), Default::default()).await.unwrap().unwrap();
    let mut original = node.session("shop", Arc::new(Authority)).unwrap();
    let result = original.query_sql("SELECT 1", vec![]).await.unwrap();
    assert_eq!(rows(result).await, [vec![Value::I64(1)]]);

    node.detach_local("shop".into()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while node.catalog().is_draining("shop") { tokio::task::yield_now().await; }
    }).await.unwrap();
    node.database_or_attach("shop".into(), Default::default()).await.unwrap().unwrap();
    let mut replacement = node.session("shop", Arc::new(Authority)).unwrap();
    assert_eq!(rows(replacement.query_sql("SELECT 2", vec![]).await.unwrap()).await, [vec![Value::I64(2)]]);
    let Err(error) = original.query_sql("SELECT 3", vec![]).await else {
        panic!("the stale session opened a query on the replacement");
    };
    assert_eq!(error.code, NOT_HELD);
    let error = original.state_mut().submit_tx(Vec::new(), Default::default()).await.unwrap_err();
    assert_eq!(error.code, NOT_HELD);
    stop(&node, driver).await;
}

#[tokio::test]
async fn a_placed_session_waits_for_its_own_await_basis() {
    let mut config = options();
    config.membership = Membership::Placed;
    let (node, driver) = running(config, services()).await;
    for name in ["shop", "other"] {
        node.database_or_attach(name.into(), Default::default()).await.unwrap().unwrap();
    }
    let mut session = node.session("shop", Arc::new(Authority)).unwrap();
    let other_basis = rtdb_format::basis::encode_tx_basis(&TxBasis {
        databases: BTreeMap::from([("other".into(), vec![TxId(1_000_000)])]),
    });
    session.state_mut().set_await_token(Some(other_basis)).unwrap();
    let query = tokio::time::timeout(Duration::from_secs(1), session.query_sql("SELECT 1", vec![])).await.unwrap().unwrap();
    assert_eq!(rows(query).await, [vec![Value::I64(1)]]);
    for sql in ["SHOW latest_completed_txs", "SHOW latest_submitted_msg_ids", "SHOW latest_processed_msg_ids"] {
        let statement = rtdb_sql::parse_control(sql, &Default::default()).unwrap().unwrap();
        let token = session.state().cancellation().child_token();
        let ControlResult::Rows(result) = session.execute_control(&statement, &[], &token).await.unwrap() else {
            panic!("expected transaction status for {sql}");
        };
        let reported = rows(result).await;
        assert_eq!(reported.len(), 1);
        assert_eq!(reported[0][0], Value::Utf8("shop".into()));
    }
    drop(session);
    stop(&node, driver).await;
}

#[tokio::test]
async fn database_metric_provider_uses_current_catalog_without_retaining_node_lifetime() {
    let (node, driver) = Node::open(options(), services()).await.unwrap();
    let provider = node.metrics_provider().unwrap();
    let snapshot = provider.metrics().unwrap();
    assert!(snapshot.rows().iter().any(|metric| matches!(metric, CatalogMetric::Gauge { name, tags, value, .. } if name == "node.tx.latestCompletedTxId" && *value == -1.0 && tags.get("db").is_some_and(|name| name == "xtdb"))));
    assert!(snapshot.rows().iter().any(|metric| matches!(metric, CatalogMetric::Gauge { name, value, .. } if name == "xtdb.block.last_upload_time" && *value == 0.0)));
    drop(node); drop(driver);
    assert!(provider.metrics().unwrap().rows().is_empty());
    assert!(!snapshot.rows().is_empty());
}
async fn stop(node: &Node, driver: tokio::task::JoinHandle<Result<()>>) {
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}
fn put(node: &Node, id: i64, value: i64) -> TxOp {
    TxOp::PutDocs { table: TableRef::new("public", "docs"), documents: Documents::new(vec![StructFields::from([("_id".into(), Value::I64(id)), ("value".into(), Value::I64(value))])], node.memory()).unwrap(), valid_from: None, valid_to: None }
}

#[tokio::test]
async fn source_barrier_waits_for_submitted_transactions_before_the_next_read() {
    let (node, driver) = Node::open(options(), services()).await.unwrap();
    let mut writer = node.session("xtdb", Arc::new(Authority)).unwrap();
    let mut reader = node.session("xtdb", Arc::new(Authority)).unwrap();
    writer.state_mut().submit_tx(vec![put(&node, 1, 10)], AutonomousTxOptions::default()).await.unwrap();
    assert!(node.catalog().database("xtdb").unwrap().progress().current().tx_key.is_none());

    let token = reader.state().cancellation().child_token();
    let statement = rtdb_sql::parse_control("SHOW rtdb_source_barrier", &Default::default()).unwrap().unwrap();
    let mut barrier = Box::pin(reader.execute_control(&statement, &[], &token));
    assert!(barrier.as_mut().now_or_never().is_none());
    let driver = tokio::spawn(driver.run());
    let ControlResult::Rows(show) = barrier.await.unwrap() else { panic!("expected a barrier acknowledgement"); };
    assert_eq!(rows(show).await, [vec![Value::Utf8("1".into())]]);
    assert_eq!(reader.state().status(), TransactionStatus::Idle);
    assert_eq!(rows(reader.query_sql("SELECT value FROM docs FOR ALL VALID_TIME", vec![]).await.unwrap()).await, [vec![Value::I64(10)]]);
    stop(&node, driver).await;
}

#[tokio::test]
async fn source_barrier_refuses_a_node_without_an_indexer() {
    let mut config = options();
    config.replication.enabled = false;
    let (node, driver) = Node::open(config, services()).await.unwrap();
    let mut reader = node.session("xtdb", Arc::new(Authority)).unwrap();
    let token = reader.state().cancellation().child_token();
    let statement = rtdb_sql::parse_control("SHOW rtdb_source_barrier", &Default::default()).unwrap().unwrap();
    let Err(error) = reader.execute_control(&statement, &[], &token).await else { panic!("barrier passed without an indexer"); };
    assert_eq!(error.code, "xtdb/log-processor-not-initialised");
    driver.close_without_run().await.unwrap();
}

#[tokio::test]
async fn hosted_source_barrier_requires_an_authoritative_multi_holder() {
    let (node, driver) = running(options(), services()).await;
    let mut reader = node.session("xtdb", Arc::new(Authority)).unwrap();
    let token = reader.state().cancellation().child_token();
    let statement = rtdb_sql::parse_control("SHOW rtdb_hosted_source_barrier", &Default::default()).unwrap().unwrap();
    let Err(error) = reader.execute_control(&statement, &[], &token).await else { panic!("standalone node certified a hosted history"); };
    assert_eq!(error.code, "rtdb.node/hosted-source-unavailable");
    stop(&node, driver).await;
}

#[tokio::test]
async fn read_transaction_pinned_before_ingestion_retains_a_null_partition_time() {
    let (node, driver) = running(options(), services()).await;
    let mut reader = node.session("xtdb", Arc::new(Authority)).unwrap();
    reader.state_mut().set_auto_commit(false).await.unwrap();
    assert_eq!(rows(reader.query_sql("SELECT 1", vec![]).await.unwrap()).await, [vec![Value::I64(1)]]);
    let basis = reader.state_mut().read_basis().await.unwrap();
    assert_eq!(basis.snapshot.databases["xtdb"], vec![None]);
    let mut writer = node.session("xtdb", Arc::new(Authority)).unwrap();
    assert!(writer.state_mut().execute_tx(vec![put(&node, 1, 10)], AutonomousTxOptions::default()).await.unwrap().committed());
    assert_eq!(rows(reader.query_sql("SELECT value FROM docs FOR ALL VALID_TIME", vec![]).await.unwrap()).await, [vec![Value::I64(10)]]);
    assert_eq!(reader.state_mut().read_basis().await.unwrap().snapshot, basis.snapshot);
    reader.state_mut().rollback_tx().unwrap();
    stop(&node, driver).await;
}

#[tokio::test]
async fn native_query_admits_syntax_after_the_reached_read_basis() {
    let mut config = options();
    config.replication.enabled = false;
    let (node, driver) = Node::open(config, services()).await.unwrap();
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let memory = node.memory();
    let baseline = memory.used_bytes();
    let context = session.query_context().await.unwrap();
    let context_bytes = memory.used_bytes() - baseline;
    assert!(context_bytes > 0);
    drop(context);
    assert_eq!(memory.used_bytes(), baseline);

    let source = format!("SELECT '{}'; SELECT @", "é".repeat(64));
    let syntax_bytes = rtdb_sql::statement_syntax_estimate(source.len()).unwrap();
    let expected = rtdb_sql::parse_in(&source, memory).unwrap_err();
    assert_eq!(expected.code, "xtdb/sql-error");

    let pressure = memory.reserve(memory.limit_bytes() - baseline - context_bytes - syntax_bytes + 1).unwrap();
    let Err(error) = session.query_sql(&source, Vec::new()).await else { panic!("syntax reservation must be refused"); };
    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    let retained_at_parse = baseline + pressure.bytes() + context_bytes;
    let message = format!("Cannot reserve {syntax_bytes} bytes from budget {} with {retained_at_parse} retained", memory.limit_bytes());
    assert_eq!(error.message.as_deref(), Some(message.as_str()));
    assert_eq!(memory.used_bytes(), baseline + pressure.bytes());
    drop(pressure);

    let pressure = memory.reserve(memory.limit_bytes() - baseline - context_bytes - syntax_bytes).unwrap();
    let Err(error) = session.query_sql(&source, Vec::new()).await else { panic!("malformed source must be rejected"); };
    assert_eq!(error, expected);
    assert_eq!(memory.used_bytes(), baseline + pressure.bytes());

    session.state().cancellation().cancel();
    let Err(error) = session.query_sql(&source, Vec::new()).await else { panic!("cancelled session must be rejected"); };
    assert_eq!(error.code, "xtdb/connection-cancelled");
    assert_eq!(memory.used_bytes(), baseline + pressure.bytes());
    drop(pressure);
    drop(session);
    driver.close_without_run().await.unwrap();
}

#[tokio::test]
async fn native_internal_reads_return_stored_transaction_result_and_role_membership() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (node, driver) = running(options(), services()).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        let grant = TxOp::Sql { sql: "GRANT reader TO alice".into(), args: None };
        let first = session.state_mut().execute_tx(vec![grant], AutonomousTxOptions::default()).await.unwrap();
        assert!(first.committed());
        let second = session.state_mut().execute_tx(vec![put(&node, 1, 10)], AutonomousTxOptions::default()).await.unwrap();
        assert!(second.committed());
        assert!(second.tx_key().tx_id > first.tx_key().tx_id);
        assert_eq!(node.latest_completed_transactions().unwrap()["xtdb"], vec![Some(second.tx_key())]);
        let stored = node.await_transaction("xtdb", first.tx_key().tx_id, &Authority, &CancellationToken::new()).await.unwrap();
        assert_eq!(stored, first);

        let memberships = session.query_sql("SELECT admin_option FROM pg_catalog.pg_auth_members", Vec::new()).await.unwrap();
        assert_eq!(rows(memberships).await, [vec![Value::Bool(false)]]);
        stop(&node, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn native_sql_and_xtql_admit_unsigned_external_parameters_before_binding() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    assert_eq!(rows(session.query_sql("SELECT STR(?) AS v", vec![Value::U64(u64::MAX)]).await.unwrap()).await, [vec![Value::Utf8("-1".into())]]);
    assert_eq!(rows(session.query_xtql("#(rel [{:v %}] [v])", vec![Value::U8(u8::MAX)]).await.unwrap()).await, [vec![Value::I8(-1)]]);
    stop(&node, driver).await;
}

#[tokio::test]
async fn native_logical_queries_execute_bound_relations_with_named_arguments() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let token = session.state().cancellation().child_token();
    let empty_columns = "[:select {:predicate (== ?ap_n 2)} [:table {:rows [{} {} {}]}]]";
    let arguments = BTreeMap::from([("ap-n".into(), Value::I64(2))]);
    let cursor = session.query_ra(empty_columns, arguments.into(), &token).await.unwrap();
    assert_eq!(rows(cursor).await, [vec![], vec![], vec![]]);

    let ordered = "[:sort {:order-specs [[a {:direction :desc :null-ordering :nulls-last}]] :skip 1 :limit 1} [:table {:rows [{:a 7} {} {:a 2}]}]]";
    let cursor = session.query_ra(ordered, StructFields::new(), &token).await.unwrap();
    assert_eq!(rows(cursor).await, [vec![Value::I64(2)]]);
    stop(&node, driver).await;
}

#[tokio::test]
async fn native_logical_query_checks_named_argument_admission_before_binding() {
    let mut config = options();
    config.replication.enabled = false;
    let (node, driver) = Node::open(config, services()).await.unwrap();
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let operation = session.state().cancellation().child_token();
    let memory = node.memory();
    let baseline = memory.used_bytes();
    let context = session.query_context_for("xtdb", &operation).await.unwrap();
    let context_bytes = memory.used_bytes() - baseline;
    drop(context);
    assert_eq!(memory.used_bytes(), baseline);

    let arguments = || BTreeMap::from([("param".into(), Value::Utf8("x".repeat(64 << 10)))]);
    let pressure = memory.reserve(memory.limit_bytes() - baseline - context_bytes - (256 << 10)).unwrap();
    let Err(error) = session.query_ra("@", arguments().into(), &operation).await else {
        panic!("expected a failed parameter reservation");
    };
    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), baseline + pressure.bytes());
    drop(pressure);

    let source = "[:table {:rows [{:value 1}]}]";
    let cursor = session.query_ra(source, arguments().into(), &operation).await.unwrap();
    assert_eq!(rows(cursor).await, [vec![Value::I64(1)]]);
    drop(session);
    driver.close_without_run().await.unwrap();
}

#[tokio::test]
async fn native_logical_queries_preserve_explicit_page_boundaries() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let token = session.state().cancellation().child_token();
    let plan = "[:literal-pages {:pages [[{:a 1}] [] [{:a 2} {:a 3}]]}]";
    let mut cursor = session.query_ra(plan, StructFields::new(), &token).await.unwrap();
    for expected in [1, 0, 2] {
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(page.batch().num_rows(), expected);
    }
    assert!(cursor.next_batch().await.unwrap().is_none());
    stop(&node, driver).await;
}

#[tokio::test]
async fn disabled_indexing_and_read_only_nodes_do_not_resolve_external_source_factories() {
    for read_only in [false, true] {
        let mut config = options();
        config.replication.enabled = read_only;
        config.read_only = read_only;
        let Membership::Durable { primary, .. } = &mut config.membership else { panic!("expected durable membership") };
        primary.external_source = Some(prost_types::Any { type_url: "proto.xtdb.com/unknown.Source".into(), value: vec![1, 2, 3] });
        let (node, driver) = running(config, services()).await;
        assert!(node.catalog().database("xtdb").unwrap().external_source());
        assert!(node.catalog().database("xtdb").unwrap().progress().fault().is_none());
        stop(&node, driver).await;
    }
}

#[tokio::test]
async fn log_tables_test_source_log_msgs_basic_and_tx_ops_decoded() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (node, driver) = running(options(), services()).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        session.state_mut().execute_operation(put(&node, 1, 10)).await.unwrap();
        session.state_mut().execute_operation(put(&node, 2, 20)).await.unwrap();
        let messages = rows(session.query_sql("SELECT msg_id, log_offset, msg, tx_ops FROM xt.source_log_msgs WHERE msg_id BETWEEN 0 AND 100 ORDER BY msg_id", vec![]).await.unwrap()).await;
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0][0], Value::I64(0)); assert_eq!(messages[0][1], Value::I64(0));
        assert!(matches!(&messages[0][2], Value::Utf8(value) if value.contains(":type :tx")));
        assert!(matches!(&messages[0][3], Value::Utf8(value) if value.contains("#xt/tagged [:put-docs") && value.contains(":xt/id 1")));
        assert!(rows(session.query_sql("SELECT * FROM xt.source_log_msgs WHERE msg_id BETWEEN 99999 AND 99999", vec![]).await.unwrap()).await.is_empty());
        let error = match session.query_sql("SELECT msg_id FROM xt.source_log_msgs", vec![]).await { Ok(_) => panic!("unbounded inspection succeeded"), Err(error) => error };
        assert_eq!(error.code, "xtdb/missing-log-bounds");
        stop(&node, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn flush_all_observes_successive_durable_blocks_before_returning() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    for (id, expected) in [(1, "00"), (2, "01")] {
        session.state_mut().execute_operation(put(&node, id, id * 10)).await.unwrap();
        node.flush_all(Duration::from_secs(10)).await.unwrap();
        let blocks = rows(session.query_sql(
            "SELECT block_idx FROM xt.table_block_files WHERE table_name = 'public/docs' AND block_idx = ?",
            vec![Value::Utf8(expected.into())],
        ).await.unwrap()).await;
        assert_eq!(blocks, vec![vec![Value::Utf8(expected.into())]]);
    }
    stop(&node, driver).await;
}

#[tokio::test]
async fn block_tables_test_block_files_with_params_and_table_block_tries() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (node, driver) = running(options(), services()).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        session.state_mut().execute_operation(put(&node, 1, 10)).await.unwrap();
        let database = node.catalog().database("xtdb").unwrap();
        database.flush().await.unwrap();
        loop { if database.status().adopted_block.is_some() { break; } tokio::task::yield_now().await; }
        let blocks = rows(session.query_sql("SELECT block_idx, file_size FROM xt.block_files WHERE block_idx BETWEEN ? AND ?", vec![Value::Utf8("00".into()), Value::Utf8("264".into())]).await.unwrap()).await;
        assert_eq!(blocks.len(), 1); assert_eq!(blocks[0][0], Value::Utf8("00".into()));
        assert!(matches!(blocks[0][1], Value::I64(size) if size > 0));
        let table = rows(session.query_sql("SELECT table_name, row_count, fields, partition_count FROM xt.table_block_files WHERE table_name = ? AND block_idx = ?", vec![Value::Utf8("public/docs".into()), Value::Utf8("00".into())]).await.unwrap()).await;
        assert_eq!(table.len(), 1); assert_eq!(table[0][0], Value::Utf8("public/docs".into()));
        assert!(matches!(&table[0][2], Value::Utf8(value) if value.contains("_id")));
        let tries = rows(session.query_sql("SELECT trie_key, data_file_size FROM xt.table_block_file_tries WHERE table_name = 'public/docs' AND block_idx = '00'", vec![]).await.unwrap()).await;
        assert!(!tries.is_empty());
        assert!(rows(session.query_sql("SELECT * FROM xt.table_block_files WHERE table_name = 'public/nonexistent' AND block_idx = '00'", vec![]).await.unwrap()).await.is_empty());
        let error = match session.query_sql("SELECT * FROM xt.table_block_files WHERE table_name = 'public/docs'", vec![]).await { Ok(_) => panic!("missing block equality accepted"), Err(error) => error };
        assert_eq!(error.code, "xtdb/missing-table-block-predicates");
        stop(&node, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn shared_show_controls_pin_snapshot_reads_and_keep_generic_show_unresolved() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let token = session.state().cancellation().child_token();
    let parse = |sql| rtdb_sql::parse_control(sql, &Default::default()).unwrap().unwrap();
    session.execute_control(&parse("BEGIN"), &[], &token).await.unwrap();
    session.prepare_context(&token).await.unwrap();
    assert_eq!(session.state().status(), TransactionStatus::Pending);
    let ControlResult::Rows(show) = session.execute_control(&parse("SHOW latest_submitted_tx"), &[], &token).await.unwrap() else { panic!() };
    assert!(rows(show).await.is_empty()); assert_eq!(session.state().status(), TransactionStatus::Pending);
    let ControlResult::Rows(show) = session.execute_control(&parse("SHOW snapshot_token"), &[], &token).await.unwrap() else { panic!() };
    assert_eq!(rows(show).await.len(), 1); assert_eq!(session.state().status(), TransactionStatus::Reading);
    session.execute_control(&parse("ROLLBACK"), &[], &token).await.unwrap();
    let error = match session.execute_control_with(&parse("BEGIN"), &[], &token, ControlExecutionOptions { frontend: ControlFrontend::Adbc, ..Default::default() }).await { Ok(_) => panic!(), Err(error) => error };
    assert_eq!(error.code, "xtdb.adbc/tx-control-as-sql");
    token.cancel(); assert!(session.prepare_context(&token).await.is_err());
    assert!(!session.state().cancellation().is_cancelled());
    stop(&node, driver).await;
}
#[tokio::test(flavor = "current_thread")]
async fn cancelled_sleep_in_begin_options_does_not_publish_a_partial_transaction() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let token = session.state().cancellation().child_token();
    let statement = rtdb_sql::parse_control("BEGIN READ WRITE WITH (METADATA PG_SLEEP(60.0))", &Default::default()).unwrap().unwrap();
    let (result, ()) = tokio::join!(session.execute_control(&statement, &[], &token), async { tokio::task::yield_now().await; token.cancel(); });
    assert!(result.is_err());
    assert_eq!(session.state().status(), TransactionStatus::Idle);
    assert!(!session.state().cancellation().is_cancelled());
    let token = session.state().cancellation().child_token();
    let statement = rtdb_sql::parse_control("BEGIN", &Default::default()).unwrap().unwrap();
    drop(session.execute_control(&statement, &[], &token).await.unwrap());
    assert_eq!(session.state().status(), TransactionStatus::Pending);
    session.state_mut().rollback_tx().unwrap();
    stop(&node, driver).await;
}

async fn rows(mut cursor: QueryCursor) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await.unwrap() {
        let schema = batch.batch().schema();
        for row in 0..batch.batch().num_rows() {
            rows.push(schema.fields().iter().enumerate().map(|(column, field)| read_value(field, batch.batch().column(column).as_ref(), row).unwrap()).collect());
        }
    }
    rows
}

#[tokio::test]
async fn native_sessions_execute_data_dependent_writes_and_keep_historical_reads() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (node, driver) = running(options(), services()).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        let first = session.state_mut().execute_tx(vec![put(&node, 1, 10)], AutonomousTxOptions::default()).await.unwrap();
        assert!(first.committed());
        assert_eq!(rows(session.query_sql("SELECT value FROM docs WHERE _id = 1", vec![]).await.unwrap()).await, vec![vec![Value::I64(10)]]);
        let update = TxOp::Sql { sql: "UPDATE docs SET value = value + ? WHERE _id = 1".into(), args: Some(SqlArguments::from_rows(vec![vec![Value::I64(5)]], node.memory()).unwrap()) };
        let updated = session.state_mut().execute_tx(vec![update], AutonomousTxOptions::default()).await.unwrap();
        assert!(updated.committed(), "{:?}", updated.error());
        assert_eq!(node.await_transaction("xtdb", first.tx_key().tx_id, &Authority, &CancellationToken::new()).await.unwrap(), first);
        assert_eq!(rows(session.query_sql("SELECT value FROM docs WHERE _id = 1", vec![]).await.unwrap()).await, vec![vec![Value::I64(15)]]);
        assert_eq!(rows(session.query_sql("SELECT value FROM docs FOR ALL SYSTEM_TIME WHERE _id = 1 ORDER BY _system_from", vec![]).await.unwrap()).await, vec![vec![Value::I64(10)], vec![Value::I64(15)]]);
        stop(&node, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn control_result_retains_allocation_during_pending_write_and_releases_on_drop() {
    use tokio::io::AsyncWriteExt;
    let mut config = options(); config.replication.enabled = false;
    let (node, driver) = running(config, services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let statement = rtdb_plan::ControlStatement::Prepare { name: "large".into(), source: "x".repeat(100_000) };
    let cancellation = session.state().cancellation().child_token();
    let result = session.execute_control(&statement, &[], &cancellation).await.unwrap();
    let memory = Arc::clone(node.memory());
    let (mut writer, _reader) = tokio::io::duplex(64);
    let mut write = Box::pin(async move {
        let ControlResult::Prepare { source, allocation, .. } = result else { panic!("prepare") };
        writer.write_all(source.as_bytes()).await.unwrap();
        drop(source); drop(allocation);
    });
    assert!(write.as_mut().now_or_never().is_none());
    drop(session); stop(&node, driver).await; drop(node);
    assert!(memory.used_bytes() >= 100_000);
    drop(write);
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn control_and_resolver_text_casts_share_captured_relation_names() {
    use rtdb_plan::{CastTarget, ControlStatement, Expr};
    use rtdb_types::{LogicalType, ScalarType};

    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let token = session.state().cancellation().child_token();
    let relation = Expr::Cast {
        expr: Box::new(Expr::Literal(Value::Utf8("pg_class".into()))),
        target: CastTarget::fixed(LogicalType::Scalar(ScalarType::RegClass)),
    };
    let statement = ControlStatement::SetParameter {
        name: "application_name".into(),
        value: Expr::Cast {
            expr: Box::new(relation),
            target: CastTarget::fixed(LogicalType::Scalar(ScalarType::Utf8)),
        },
    };
    drop(session.execute_control(&statement, &[], &token).await.unwrap());
    assert_eq!(session.state().parameter("application_name"), Some("pg_class"));
    let statement = TxOp::Sql { sql: "INSERT INTO docs (_id,value) VALUES (1,CAST(CAST('pg_class' AS REGCLASS) AS TEXT))".into(), args: None };
    let result = session.state_mut().execute_tx(vec![statement], AutonomousTxOptions::default()).await.unwrap();
    assert!(result.committed(), "{:?}", result.error());
    assert_eq!(rows(session.query_sql("SELECT value FROM docs", vec![]).await.unwrap()).await, [vec![Value::Utf8("pg_class".into())]]);
    drop(session); stop(&node, driver).await;
}

#[tokio::test]
async fn asynchronous_submission_is_recorded_before_application_and_read_awaits_the_token() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (node, driver) = running(options(), services()).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        let submitted = session.state_mut().submit_tx(vec![put(&node, 1, 42)], AutonomousTxOptions::default()).await.unwrap();
        assert_eq!(session.state().last_submitted().unwrap().tx_id(), submitted.tx_id);
        assert_eq!(rows(session.query_sql("SELECT value FROM docs", vec![]).await.unwrap()).await, vec![vec![Value::I64(42)]]);
        stop(&node, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn tables_with_colliding_escaped_names_keep_separate_rows_after_restart() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let directory = tempfile::tempdir().unwrap();
        let configuration = DatabaseConfig {
            log: Some(database_config::Log::LocalLog(LocalLog { path: Some(directory.path().join("log").to_string_lossy().into_owned()), epoch: Some(0) })),
            storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: Some(directory.path().join("store").to_string_lossy().into_owned()), epoch: Some(0) })),
            ..Default::default()
        };
        let mut settings = options();
        settings.membership = Membership::Durable { primary: configuration, skipped: BTreeSet::new() };
        let runtime = services();
        let (node, driver) = running(settings.clone(), fresh_local_services(&runtime)).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        for statement in [r#"INSERT INTO "a$b" (_id, value) VALUES (2, 22)"#, r#"INSERT INTO "a$b~1" (_id, value) VALUES (3, 33)"#] {
            let outcome = session.state_mut().execute_tx(vec![TxOp::Sql { sql: statement.into(), args: None }], AutonomousTxOptions::default()).await.unwrap();
            assert!(outcome.committed(), "{statement}: {:?}", outcome.error());
        }
        let database = node.catalog().database("xtdb").unwrap();
        let cancellation = CancellationToken::new();
        database.flush().await.unwrap();
        node.sync_all(Duration::from_secs(10), &cancellation).await.unwrap();

        let statement = r#"INSERT INTO "a.b" (_id, value) VALUES (1, 11)"#;
        let outcome = session.state_mut().execute_tx(vec![TxOp::Sql { sql: statement.into(), args: None }], AutonomousTxOptions::default()).await.unwrap();
        assert!(outcome.committed(), "{:?}", outcome.error());
        database.flush().await.unwrap();
        node.sync_all(Duration::from_secs(10), &cancellation).await.unwrap();
        drop((session, database));
        stop(&node, driver).await;
        drop(node);

        let (reopened, driver) = running(settings, fresh_local_services(&runtime)).await;
        let mut session = reopened.session("xtdb", Arc::new(Authority)).unwrap();
        for (statement, expected) in [
            (r#"SELECT value FROM "a.b""#, 11),
            (r#"SELECT value FROM "a$b""#, 22),
            (r#"SELECT value FROM "a$b~1""#, 33),
        ] {
            assert_eq!(rows(session.query_sql(statement, vec![]).await.unwrap()).await, vec![vec![Value::I64(expected)]]);
        }
        stop(&reopened, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn local_recovery_reads_flushed_and_unflushed_transactions_and_retained_batches_survive_shutdown() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let directory = tempfile::tempdir().unwrap();
        let configuration = DatabaseConfig {
            log: Some(database_config::Log::LocalLog(LocalLog { path: Some(directory.path().join("log").to_string_lossy().into_owned()), epoch: Some(0) })),
            storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: Some(directory.path().join("store").to_string_lossy().into_owned()), epoch: Some(0) })),
            ..Default::default()
        };
        let mut configuration_options = options();
        configuration_options.membership = Membership::Durable { primary: configuration, skipped: BTreeSet::new() };
        let runtime_services = services();
        let (node, driver) = running(configuration_options.clone(), fresh_local_services(&runtime_services)).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        assert!(session.state_mut().execute_tx(vec![put(&node, 1, 10)], AutonomousTxOptions::default()).await.unwrap().committed());
        node.catalog().database("xtdb").unwrap().flush().await.unwrap();
        node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
        node.catalog().database("xtdb").unwrap().compaction().unwrap().compact_all().await.unwrap();
        assert!(session.state_mut().execute_tx(vec![put(&node, 2, 20)], AutonomousTxOptions::default()).await.unwrap().committed());
        let mut cursor = session.query_sql("SELECT _id, value FROM docs ORDER BY _id", vec![]).await.unwrap();
        let retained = cursor.next_batch().await.unwrap().unwrap();
        drop(cursor);
        stop(&node, driver).await;
        assert_eq!(retained.batch().num_rows(), 2);
        assert_eq!(read_value(retained.batch().schema().field(1), retained.batch().column(1).as_ref(), 1).unwrap(), Value::I64(20));
        let (reopened, driver) = running(configuration_options, fresh_local_services(&runtime_services)).await;
        reopened.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
        let mut session = reopened.session("xtdb", Arc::new(Authority)).unwrap();
        assert_eq!(rows(session.query_sql("SELECT _id, value FROM docs ORDER BY _id", vec![]).await.unwrap()).await, vec![vec![Value::I64(1), Value::I64(10)], vec![Value::I64(2), Value::I64(20)]]);
        stop(&reopened, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn local_recovery_syncs_an_empty_newer_log_epoch_and_preserves_flushed_rows() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let directory = tempfile::tempdir().unwrap();
        let mut configuration = DatabaseConfig {
            log: Some(database_config::Log::LocalLog(LocalLog { path: Some(directory.path().join("log-0").to_string_lossy().into_owned()), epoch: Some(0) })),
            storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: Some(directory.path().join("store").to_string_lossy().into_owned()), epoch: Some(0) })),
            ..Default::default()
        };
        let mut settings = options();
        settings.membership = Membership::Durable { primary: configuration.clone(), skipped: BTreeSet::new() };
        let runtime_services = services();
        let (node, driver) = running(settings.clone(), fresh_local_services(&runtime_services)).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        let transaction = session.state_mut().execute_tx(vec![put(&node, 1, 10)], AutonomousTxOptions::default()).await.unwrap();
        assert!(transaction.committed());
        node.catalog().database("xtdb").unwrap().flush().await.unwrap();
        node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
        stop(&node, driver).await;

        configuration.log = Some(database_config::Log::LocalLog(LocalLog { path: Some(directory.path().join("log-1").to_string_lossy().into_owned()), epoch: Some(1) }));
        settings.membership = Membership::Durable { primary: configuration, skipped: BTreeSet::new() };
        let (reopened, driver) = running(settings, fresh_local_services(&runtime_services)).await;
        reopened.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
        let progress = reopened.catalog().database("xtdb").unwrap().progress().current();
        assert_eq!(progress.source_position, rtdb_types::SourcePosition::from_epoch_offset(rtdb_types::StorageEpoch(1), -1).unwrap());
        assert_eq!(progress.tx_key, Some(transaction.tx_key()));
        let mut session = reopened.session("xtdb", Arc::new(Authority)).unwrap();
        assert_eq!(rows(session.query_sql("SELECT _id, value FROM docs", vec![]).await.unwrap()).await, vec![vec![Value::I64(1), Value::I64(10)]]);
        stop(&reopened, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn local_recovery_rejects_out_of_sync_source_logs_before_open_returns_in_every_database_mode() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let directory = tempfile::tempdir().unwrap();
        let configuration = DatabaseConfig {
            log: Some(database_config::Log::LocalLog(LocalLog { path: Some(directory.path().join("original").to_string_lossy().into_owned()), epoch: Some(1) })),
            storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: Some(directory.path().join("store").to_string_lossy().into_owned()), epoch: Some(0) })),
            ..Default::default()
        };
        let mut settings = options();
        settings.membership = Membership::Durable { primary: configuration.clone(), skipped: BTreeSet::new() };
        let runtime_services = services();
        let (node, driver) = running(settings, fresh_local_services(&runtime_services)).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        assert!(session.state_mut().execute_tx(vec![put(&node, 1, 10)], AutonomousTxOptions::default()).await.unwrap().committed());
        node.catalog().database("xtdb").unwrap().flush().await.unwrap();
        node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
        stop(&node, driver).await;

        for epoch in [0, 1] {
            for (enabled, read_only, may_lead, external) in [(true, false, true, false), (false, false, true, false), (true, true, true, false), (true, false, false, false), (true, false, true, true)] {
                let mut configuration = configuration.clone();
                configuration.log = Some(database_config::Log::LocalLog(LocalLog { path: Some(directory.path().join("empty").to_string_lossy().into_owned()), epoch: Some(epoch) }));
                if external { configuration.external_source = Some(prost_types::Any { type_url: "proto.xtdb.com/test.StartupSource".into(), value: Vec::new() }); }
                let mut settings = options();
                settings.replication.enabled = enabled;
                settings.replication.may_lead = may_lead;
                settings.read_only = read_only;
                settings.membership = Membership::Durable { primary: configuration, skipped: BTreeSet::new() };
                let mut runtime = fresh_local_services(&runtime_services);
                runtime.external_sources.insert("test.StartupSource".into(), Arc::new(UnopenedStartupSourceFactory));
                let error = match Node::open(settings, runtime).await {
                    Ok((_, driver)) => { driver.close_without_run().await.unwrap(); panic!("out-of-sync source log opened"); }
                    Err(error) => error,
                };
                assert_eq!(error.code, "xtdb/out-of-sync-log");
                assert!(error.message.as_deref().unwrap().starts_with("Database 'xtdb' failed to start due to an invalid transaction log state (the log is empty) that does not correspond with the latest processed message (epoch=1 and offset="), "{:?}", error.message);
            }
        }
    }).await.unwrap();
}

struct UnopenedStartupSourceFactory;
#[async_trait]
impl ExternalSourceFactory for UnopenedStartupSourceFactory {
    fn validate(&self, _: &prost_types::Any) -> Result<()> { Ok(()) }
    async fn open(&self, _: &prost_types::Any, _: &str, _: Arc<rtdb_storage::IoRegion>) -> Result<Box<dyn ExternalSource>> { panic!("source opened before persisted log validation"); }
}

struct RecoveryCatalogStorage {
    bytes: Vec<u8>,
    opened: tokio::sync::mpsc::UnboundedSender<Arc<rtdb_storage::IoRegion>>,
}
#[async_trait]
impl RemoteStorageFactory for RecoveryCatalogStorage {
    async fn open(&self, _: &prost_types::Any, _: &rtdb_storage::ObjectKey, io: Arc<rtdb_storage::IoRegion>) -> Result<Arc<dyn rtdb_storage::ObjectStore>> {
        use rtdb_storage::ObjectStore;
        let store = Arc::new(rtdb_storage::MemoryStore::default());
        store.put(&rtdb_storage::ObjectKey::new(rtdb_format::keys::block_key(rtdb_types::BlockIndex(0))?)?, self.bytes.clone().into()).await?;
        self.opened.send(io).unwrap();
        Ok(store)
    }
}
struct RecoveryLogFactory(Arc<std::sync::atomic::AtomicUsize>);
#[async_trait]
impl LogFactory for RecoveryLogFactory {
    async fn open(&self, _: &prost_types::Any, _: LogAccess, _: CancellationToken) -> Result<OpenedLogs> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(Anomaly::fault("test/recovery-log-open", "Observed log opening"))
    }
}

#[tokio::test]
async fn persisted_catalog_recovery_precedes_log_opening_and_failed_open_drains_its_region() {
    use rtdb_format::{block::proto::Block, catalog::{BlockCatalog, TableEntry}};
    let missing_table = BlockCatalog::from_proto(Block {
        block_index: Some(0), tables: vec![TableEntry::new(16_384, TableRef::new("public", "docs")).unwrap().to_proto()], ..Default::default()
    }).unwrap().encode();
    let empty_catalog = BlockCatalog::from_proto(Block { block_index: Some(0), ..Default::default() }).unwrap().encode();
    for (bytes, expected, calls) in [(vec![0xff], "rtdb.format/invalid-catalog", 0), (missing_table, "rtdb.storage/missing-object", 0), (empty_catalog, "test/recovery-log-open", 1)] {
        let cache_directory = tempfile::tempdir().unwrap();
        let cache = rtdb_storage::disk_cache::DiskCache::open(cache_directory.path(), 1024 * 1024, rtdb_columnar::MemoryBudget::new(4 * 1024 * 1024)).unwrap();
        let (opened, mut regions) = tokio::sync::mpsc::unbounded_channel();
        let log_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let resources = NativeResources::default().with_disk_cache(cache)
            .register_storage("test.RecoveryCatalog", Arc::new(RecoveryCatalogStorage { bytes, opened })).unwrap()
            .register_log("test.RecoveryLog", Arc::new(RecoveryLogFactory(Arc::clone(&log_calls)))).unwrap();
        let configuration = DatabaseConfig {
            log: Some(database_config::Log::OtherLog(prost_types::Any { type_url: "proto.xtdb.com/test.RecoveryLog".into(), value: Vec::new() })),
            storage: Some(database_config::Storage::RemoteStorage(rtdb_format::database::proto::RemoteStorage {
                object_store: Some(prost_types::Any { type_url: "proto.xtdb.com/test.RecoveryCatalog".into(), value: Vec::new() }), epoch: None,
            })), ..Default::default()
        };
        let mut settings = options();
        settings.membership = Membership::Durable { primary: configuration, skipped: BTreeSet::new() };
        let runtime = NodeServices { resources: Arc::new(resources), ..services() };
        let error = match Node::open(settings, runtime).await {
            Err(error) => error,
            Ok((_, driver)) => { driver.close_without_run().await.unwrap(); panic!("invalid recovery fixture opened"); }
        };
        assert_eq!(error.code, expected);
        assert_eq!(log_calls.load(Ordering::SeqCst), calls);
        let region = regions.recv().await.unwrap();
        assert!(matches!(region.run_blocking(|| Ok(())).await, Err(rtdb_storage::Error::Closed(_))));
    }
}

#[tokio::test]
async fn offline_export_copies_one_loaded_block_and_reopens_as_a_single_partition_store() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let directory = tempfile::tempdir().unwrap();
        let store_path = directory.path().join("store");
        let configuration = DatabaseConfig {
            log: Some(database_config::Log::LocalLog(LocalLog { path: Some(directory.path().join("log").to_string_lossy().into_owned()), epoch: Some(0) })),
            storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: Some(store_path.to_string_lossy().into_owned()), epoch: Some(0) })),
            ..Default::default()
        };
        let mut settings = options();
        settings.membership = Membership::Durable { primary: configuration.clone(), skipped: BTreeSet::new() };
        let runtime_services = services();
        let (node, driver) = running(settings.clone(), fresh_local_services(&runtime_services)).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        assert!(session.state_mut().execute_tx(vec![put(&node, 1, 10)], AutonomousTxOptions::default()).await.unwrap().committed());
        node.catalog().database("xtdb").unwrap().flush().await.unwrap();
        node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
        node.catalog().database("xtdb").unwrap().compaction().unwrap().compact_all().await.unwrap();
        node.catalog().database("xtdb").unwrap().flush().await.unwrap();
        node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
        assert!(node.snapshot_export("xtdb", Timestamp(0)).is_err());
        drop(session);
        stop(&node, driver).await;
        drop(node);

        settings.replication.enabled = false;
        settings.compaction_threads = 0;
        let (node, driver) = running(settings.clone(), fresh_local_services(&runtime_services)).await;
        let plan = node.snapshot_export("xtdb", Timestamp(0)).unwrap().unwrap();
        assert!(plan.directory().as_str().ends_with("_19700101T000000Z"));
        assert_eq!(plan.files()[0].as_str(), rtdb_format::keys::block_key(plan.block_index()).unwrap());
        assert!(plan.files().iter().any(|path| path.as_str().contains("/data/l01-rc")));
        assert!(!plan.files().iter().any(|path| path.as_str().contains("/data/l00-")));
        let destination = store_path.join("v06").join(plan.directory().as_str());
        assert!(!destination.exists());
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(plan.copy(&cancelled).await.is_err());
        assert!(!destination.exists());
        plan.copy(&CancellationToken::new()).await.unwrap();
        plan.copy(&CancellationToken::new()).await.unwrap();
        for source in plan.files() {
            assert_eq!(std::fs::read(store_path.join("v06").join(source.as_str())).unwrap(), std::fs::read(store_path.join("v06").join(plan.destination(source).unwrap().as_str())).unwrap());
        }
        drop(plan);
        stop(&node, driver).await;
        drop(node);
        let mut exported = configuration;
        exported.storage = Some(database_config::Storage::LocalStorage(LocalStorage { path: Some(destination.to_string_lossy().into_owned()), epoch: Some(0) }));
        settings.membership = Membership::Durable { primary: exported, skipped: BTreeSet::new() };
        let (restored, driver) = running(settings, fresh_local_services(&runtime_services)).await;
        let mut session = restored.session("xtdb", Arc::new(Authority)).unwrap();
        assert_eq!(rows(session.query_sql("SELECT value FROM docs", vec![]).await.unwrap()).await, vec![vec![Value::I64(10)]]);
        drop(session);
        stop(&restored, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn offline_export_has_no_manifest_before_a_completed_block() {
    let mut settings = options();
    settings.replication.enabled = false;
    settings.compaction_threads = 0;
    let (node, driver) = running(settings, services()).await;
    assert!(node.snapshot_export("xtdb", Timestamp(0)).unwrap().is_none());
    stop(&node, driver).await;
}

#[tokio::test]
async fn compactor_reset_preserves_l0_history_and_deletes_compacted_files_only_after_publication() {
    tokio::time::timeout(Duration::from_secs(40), async {
        let directory = tempfile::tempdir().unwrap();
        let store = directory.path().join("store");
        let primary = DatabaseConfig {
            log: Some(database_config::Log::LocalLog(LocalLog { path: Some(directory.path().join("log").to_string_lossy().into_owned()), epoch: Some(0) })),
            storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: Some(store.to_string_lossy().into_owned()), epoch: Some(0) })),
            ..Default::default()
        };
        let mut settings = options();
        settings.membership = Membership::Durable { primary, skipped: BTreeSet::new() };
        let runtime_services = services();
        let (node, driver) = running(settings.clone(), fresh_local_services(&runtime_services)).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        session.state_mut().execute_operation(put(&node, 1, 10)).await.unwrap();
        session.state_mut().execute_operation(put(&node, 1, 20)).await.unwrap();
        node.catalog().database("xtdb").unwrap().flush().await.unwrap();
        node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
        node.catalog().database("xtdb").unwrap().compaction().unwrap().compact_all().await.unwrap();
        let sql = "SELECT value FROM docs FOR ALL SYSTEM_TIME ORDER BY value";
        let before = rows(session.query_sql(sql, vec![]).await.unwrap()).await;
        assert_eq!(before, vec![vec![Value::I64(10)], vec![Value::I64(20)]]);
        assert!(node.prepare_compactor_reset("xtdb", &CancellationToken::new()).await.is_err());
        drop(session);
        stop(&node, driver).await;
        drop(node);

        settings.compaction_threads = 0;
        let (node, driver) = running(settings.clone(), fresh_local_services(&runtime_services)).await;
        let cancellation = CancellationToken::new();
        let plan = node.prepare_compactor_reset("xtdb", &cancellation).await.unwrap();
        let compacted = plan.files_to_delete().to_vec();
        assert!(!compacted.is_empty());
        assert!(plan.replacement_tries().values().flatten().all(|trie| trie.key.level() == 0));
        assert!(plan.replacement_tries().values().any(|tries| !tries.is_empty()));
        for key in &compacted { assert!(store.join("v06").join(key.as_str()).is_file()); }
        cancellation.cancel();
        assert!(plan.apply(&cancellation).await.is_err());
        for key in &compacted { assert!(store.join("v06").join(key.as_str()).is_file()); }

        let plan = node.prepare_compactor_reset("xtdb", &CancellationToken::new()).await.unwrap();
        node.catalog().database("xtdb").unwrap().flush().await.unwrap();
        node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
        assert_eq!(plan.apply(&CancellationToken::new()).await.unwrap_err().code, "rtdb.compactor-reset/catalog-moved");
        for key in &compacted { assert!(store.join("v06").join(key.as_str()).is_file()); }

        let plan = node.prepare_compactor_reset("xtdb", &CancellationToken::new()).await.unwrap();
        let published = plan.apply(&CancellationToken::new()).await.unwrap();
        assert!(node.catalog().database("xtdb").unwrap().status().adopted_block.as_ref().is_some_and(|block| block.block_index() >= published));
        for key in &compacted { assert!(!store.join("v06").join(key.as_str()).exists()); }
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        assert_eq!(rows(session.query_sql(sql, vec![]).await.unwrap()).await, before);
        drop(session);
        stop(&node, driver).await;
        drop(node);

        let (node, driver) = running(settings, fresh_local_services(&runtime_services)).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        assert_eq!(rows(session.query_sql(sql, vec![]).await.unwrap()).await, before);
        drop(session);
        stop(&node, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn compactor_reset_requires_a_completed_transaction() {
    let mut settings = options();
    settings.compaction_threads = 0;
    let (node, driver) = running(settings, services()).await;
    let plan = node.prepare_compactor_reset("xtdb", &CancellationToken::new()).await.unwrap();
    assert!(plan.files_to_delete().is_empty());
    assert_eq!(plan.apply(&CancellationToken::new()).await.unwrap_err().code, "rtdb.compactor-reset/no-transactions");
    stop(&node, driver).await;
}

#[tokio::test]
async fn follower_compactor_publishes_results_through_the_writer_without_claiming_a_term() {
    struct SharedResources {
        source: Arc<rtdb_storage::log::MemoryLog>,
        replica: Arc<rtdb_storage::log::MemoryLog>,
        store: Arc<rtdb_storage::MemoryStore>,
    }
    #[async_trait]
    impl ResourceFactory for SharedResources {
        fn validate(&self, _: &str, _: &DatabaseConfig) -> Result<()> { Ok(()) }
        async fn open_storage(&self, _: &str, _: &DatabaseConfig) -> Result<DatabaseStorage> {
            Ok(DatabaseStorage {
                store: self.store.clone(), blocks: Default::default(), storage_epoch: rtdb_types::StorageEpoch(0), storage_version: rtdb_format::keys::CURRENT_STORAGE_VERSION,
                io: rtdb_storage::IoRegion::new(),
            })
        }
        async fn open_logs(&self, _: &str, _: &DatabaseConfig, _: LogAccess, _: CancellationToken, _: &DatabaseStorage) -> Result<OpenedLogs> {
            Ok(OpenedLogs { source: self.source.clone(), replica: self.replica.clone(), source_epoch: rtdb_types::StorageEpoch(0), replica_epoch: rtdb_types::StorageEpoch(0), drivers: Vec::new() })
        }
    }
    tokio::time::timeout(Duration::from_secs(30), async {
        let shared = Arc::new(SharedResources {
            source: Arc::new(rtdb_storage::log::MemoryLog::new(1 << 26)), replica: Arc::new(rtdb_storage::log::MemoryLog::new(1 << 26)), store: Arc::new(rtdb_storage::MemoryStore::default()),
        });
        let runtime_services = NodeServices { resources: shared, ..services() };
        let mut writer_options = options();
        writer_options.compaction_threads = 0;
        let (writer, writer_driver) = running(writer_options, runtime_services.clone()).await;
        let mut session = writer.session("xtdb", Arc::new(Authority)).unwrap();
        session.state_mut().execute_operation(put(&writer, 1, 10)).await.unwrap();
        writer.catalog().database("xtdb").unwrap().flush().await.unwrap();
        writer.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
        let mut follower_options = options();
        follower_options.replication.may_lead = false;
        let (follower, follower_driver) = running(follower_options, runtime_services).await;
        follower.catalog().database("xtdb").unwrap().compaction().unwrap().compact_all().await.unwrap();
        follower.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
        assert_eq!(follower.catalog().database("xtdb").unwrap().status().role, rtdb_replication::Role::Following);
        assert!(matches!(writer.catalog().database("xtdb").unwrap().status().role, rtdb_replication::Role::Leading(_)));
        assert_eq!(rows(session.query_sql("SELECT value FROM docs", vec![]).await.unwrap()).await, vec![vec![Value::I64(10)]]);
        let mut follower_session = follower.session("xtdb", Arc::new(Authority)).unwrap();
        assert_eq!(rows(follower_session.query_sql("SELECT value FROM docs", vec![]).await.unwrap()).await, vec![vec![Value::I64(10)]]);
        drop((session, follower_session));
        stop(&follower, follower_driver).await;
        stop(&writer, writer_driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn startup_selects_a_newer_checkpoint_when_its_initial_replica_prefix_was_trimmed() {
    use rtdb_storage::log::{LogOffset, MemoryLog};

    struct SharedLogs {
        source: Arc<MemoryLog>,
        replica: Arc<MemoryLog>,
        store: Arc<rtdb_storage::MemoryStore>,
    }

    struct LogOpening { reached: Semaphore, release: Semaphore }
    struct Resources { shared: Arc<SharedLogs>, opening: Option<Arc<LogOpening>> }

    #[async_trait]
    impl ResourceFactory for Resources {
        fn validate(&self, _: &str, _: &DatabaseConfig) -> Result<()> { Ok(()) }

        async fn open_storage(&self, _: &str, _: &DatabaseConfig) -> Result<DatabaseStorage> {
            Ok(DatabaseStorage {
                store: self.shared.store.clone(),
                blocks: Default::default(),
                storage_epoch: rtdb_types::StorageEpoch(0),
                storage_version: rtdb_format::keys::CURRENT_STORAGE_VERSION,
                io: rtdb_storage::IoRegion::new(),
            })
        }

        async fn open_logs(&self, _: &str, _: &DatabaseConfig, _: LogAccess, _: CancellationToken, _: &DatabaseStorage) -> Result<OpenedLogs> {
            if let Some(opening) = &self.opening {
                opening.reached.add_permits(1);
                opening.release.acquire().await.unwrap().forget();
            }
            Ok(OpenedLogs {
                source: self.shared.source.clone(), replica: self.shared.replica.clone(),
                source_epoch: rtdb_types::StorageEpoch(0), replica_epoch: rtdb_types::StorageEpoch(0), drivers: Vec::new(),
            })
        }
    }

    tokio::time::timeout(Duration::from_secs(30), async {
        let shared = Arc::new(SharedLogs {
            source: Arc::new(MemoryLog::new(1 << 26)), replica: Arc::new(MemoryLog::new(1 << 26)),
            store: Arc::new(rtdb_storage::MemoryStore::default()),
        });
        let runtime = services();
        let writer_resources = Arc::new(Resources { shared: shared.clone(), opening: None });
        let (writer, writer_driver) = running(options(), NodeServices { resources: writer_resources, ..runtime.clone() }).await;
        let mut session = writer.session("xtdb", Arc::new(Authority)).unwrap();
        let database = writer.catalog().database("xtdb").unwrap();
        let token = CancellationToken::new();
        session.state_mut().execute_operation(put(&writer, 1, 10)).await.unwrap();
        database.flush().await.unwrap();
        writer.sync_all(Duration::from_secs(10), &token).await.unwrap();
        let initial = database.status().adopted_block.unwrap();

        let opening = Arc::new(LogOpening { reached: Semaphore::new(0), release: Semaphore::new(0) });
        let reader_resources = Arc::new(Resources { shared: shared.clone(), opening: Some(opening.clone()) });
        let mut reader_options = options();
        reader_options.read_only = true;
        let starting = tokio::spawn(Node::open(reader_options, NodeServices { resources: reader_resources, ..runtime }));
        opening.reached.acquire().await.unwrap().forget();

        session.state_mut().execute_operation(put(&writer, 2, 20)).await.unwrap();
        database.flush().await.unwrap();
        writer.sync_all(Duration::from_secs(10), &token).await.unwrap();
        let replacement = database.status().adopted_block.unwrap();
        assert!(replacement.block_index() > initial.block_index());
        let last_boundary = replacement.boundary_replica_position().unwrap().offset();
        assert!(last_boundary > initial.boundary_replica_position().unwrap().offset() + 1);
        shared.replica.truncate_before(LogOffset(last_boundary)).unwrap();
        session.state_mut().execute_operation(put(&writer, 3, 30)).await.unwrap();
        opening.release.add_permits(1);

        let (reader, driver) = starting.await.unwrap().expect("startup must find a checkpoint with a retained replica suffix");
        let reader_driver = tokio::spawn(driver.run());
        reader.sync_all(Duration::from_secs(10), &token).await.expect("startup must replay the retained replica suffix");
        let resumed = reader.catalog().database("xtdb").unwrap().status().adopted_block.unwrap();
        assert_eq!(resumed.block_index(), replacement.block_index());
        let mut session = reader.session("xtdb", Arc::new(Authority)).unwrap();
        let cursor = session.query_sql("SELECT value FROM docs ORDER BY _id", vec![]).await.unwrap();
        assert_eq!(rows(cursor).await, [[Value::I64(10)], [Value::I64(20)], [Value::I64(30)]]);
        stop(&reader, reader_driver).await;
        stop(&writer, writer_driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn catalog_transactions_advance_system_time_and_replay_in_order() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let directory = tempfile::tempdir().unwrap();
        let mut settings = options();
        settings.membership = Membership::Durable {
            primary: local_database(directory.path()), skipped: BTreeSet::from(["secondary".into()]),
        };
        let runtime = services();
        let boot = || NodeServices {
            resources: Arc::new(NativeResources::default().with_local_log_clock(runtime.clock.clone(), true)),
            ..runtime.clone()
        };
        let (node, driver) = running(settings.clone(), boot()).await;
        let token = CancellationToken::new();
        let first_time = Timestamp(1_000_000_000);
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        let put = session.state_mut().execute_tx(vec![put(&node, 1, 10)], AutonomousTxOptions {
            system_time: Some(first_time), ..Default::default()
        }).await.unwrap();
        assert!(put.committed());
        assert_eq!(put.tx_key().system_time, first_time);

        let attached = node.attach_database("secondary".into(), Default::default(), &Authority, &token)
            .await.expect("catalog attach must advance past the previous transaction's time");
        assert!(attached.committed());
        assert_eq!(attached.tx_key().system_time, Timestamp(first_time.0 + 1));
        let refused = node.detach_database("xtdb".into(), &Authority, &token).await.unwrap();
        assert_eq!(refused.error().unwrap().code, "xtdb/cannot-detach-primary");
        assert_eq!(refused.tx_key().system_time, Timestamp(first_time.0 + 2));
        stop(&node, driver).await;

        let (reopened, driver) = running(settings, boot()).await;
        reopened.sync_all(Duration::from_secs(10), &token).await.unwrap();
        assert!(reopened.catalog().secondary_databases().contains_key("secondary"));
        assert_eq!(reopened.catalog().database("xtdb").unwrap().progress().current().tx_key, Some(refused.tx_key()));
        let mut session = reopened.session("xtdb", Arc::new(Authority)).unwrap();
        let cursor = session.query_sql("SELECT committed FROM xt.txs FOR ALL VALID_TIME ORDER BY _id", vec![]).await.unwrap();
        assert_eq!(rows(cursor).await, [[Value::Bool(true)], [Value::Bool(true)], [Value::Bool(false)]]);
        stop(&reopened, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn skipped_database_remains_a_member_until_detach_and_await_ignores_unknown_names() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut options = options();
        options.membership = Membership::Durable { primary: Default::default(), skipped: BTreeSet::from(["dormant".into()]) };
        let (node, driver) = running(options, services()).await;
        let identity = Authority;
        let token = CancellationToken::new();
        assert!(node.attach_database("dormant".into(), Default::default(), &identity, &token).await.unwrap().committed());
        assert!(node.catalog().database("dormant").is_err());
        assert!(node.catalog().secondary_databases().contains_key("dormant"));
        node.await_basis(Some(TxBasis { databases: BTreeMap::from([("unknown".into(), vec![TxId(i64::MAX)])]) }), Duration::from_millis(1), &token).await.unwrap();
        assert!(node.detach_database("dormant".into(), &identity, &token).await.unwrap().committed());
        assert!(!node.catalog().secondary_databases().contains_key("dormant"));
        let primary = node.detach_database("xtdb".into(), &identity, &token).await.unwrap();
        assert!(!primary.committed());
        assert_eq!(primary.error().unwrap().code, "xtdb/cannot-detach-primary");
        stop(&node, driver).await;
    }).await.unwrap();
}

struct HeldTeardown {
    native: NativeResources,
    armed: AtomicBool,
    started: Arc<Semaphore>,
    release: Arc<Semaphore>,
}
#[async_trait]
impl ResourceFactory for HeldTeardown {
    async fn close(&self) -> Result<()> { self.native.close().await }
    fn validate(&self, name: &str, configuration: &DatabaseConfig) -> Result<()> { self.native.validate(name, configuration) }
    async fn open_storage(&self, name: &str, configuration: &DatabaseConfig) -> Result<DatabaseStorage> { self.native.open_storage(name, configuration).await }
    async fn open_logs(&self, name: &str, configuration: &DatabaseConfig, access: LogAccess, cancellation: CancellationToken, storage: &DatabaseStorage) -> Result<OpenedLogs> {
        let mut logs = self.native.open_logs(name, configuration, access, cancellation.clone(), storage).await?;
        if name == "other" && self.armed.swap(false, Ordering::AcqRel) {
            let started = Arc::clone(&self.started);
            let release = Arc::clone(&self.release);
            logs.drivers.push(async move {
                cancellation.cancelled().await;
                started.add_permits(1);
                release.acquire().await.unwrap().forget();
                Ok(())
            }.boxed());
        }
        Ok(logs)
    }
}

#[tokio::test]
async fn detach_removes_lookup_and_membership_before_join_but_reuses_the_name_only_after_join() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let resources = Arc::new(HeldTeardown { native: NativeResources::default(), armed: AtomicBool::new(true), started: Arc::new(Semaphore::new(0)), release: Arc::new(Semaphore::new(0)) });
        let mut services = services();
        services.resources = resources.clone();
        let (node, driver) = running(options(), services).await;
        let token = CancellationToken::new();
        assert!(node.attach_database("other".into(), Default::default(), &Authority, &token).await.unwrap().committed());
        assert!(node.detach_database("other".into(), &Authority, &token).await.unwrap().committed());
        resources.started.acquire().await.unwrap().forget();
        assert!(node.catalog().database("other").is_err());
        assert!(!node.catalog().secondary_databases().contains_key("other"));
        let rejected = node.attach_database("other".into(), Default::default(), &Authority, &token).await.unwrap();
        assert!(!rejected.committed());
        assert_eq!(rejected.error().unwrap().code, "xtdb/db-being-detached");
        resources.release.add_permits(1);
        while node.catalog().check_attach("other", &Default::default()).is_err() { tokio::time::sleep(Duration::from_millis(1)).await; }
        assert!(node.attach_database("other".into(), Default::default(), &Authority, &token).await.unwrap().committed());
        stop(&node, driver).await;
    }).await.unwrap();
}

struct FailedResource { native: NativeResources, armed: AtomicBool }
#[async_trait]
impl ResourceFactory for FailedResource {
    async fn close(&self) -> Result<()> { self.native.close().await }
    fn validate(&self, name: &str, configuration: &DatabaseConfig) -> Result<()> { self.native.validate(name, configuration) }
    async fn open_storage(&self, name: &str, configuration: &DatabaseConfig) -> Result<DatabaseStorage> { self.native.open_storage(name, configuration).await }
    async fn open_logs(&self, name: &str, configuration: &DatabaseConfig, access: LogAccess, cancellation: CancellationToken, storage: &DatabaseStorage) -> Result<OpenedLogs> {
        let mut logs = self.native.open_logs(name, configuration, access, cancellation, storage).await?;
        if name == "other" && self.armed.swap(false, Ordering::AcqRel) {
            logs.drivers.push(async { Err(Anomaly::fault("test/resource-failed", "Injected resource failure")) }.boxed());
        }
        Ok(logs)
    }
}

struct ClosingResource { native: NativeResources, fail_open: bool, closing: Semaphore, release: Semaphore }
#[async_trait]
impl ResourceFactory for ClosingResource {
    fn validate(&self, name: &str, configuration: &DatabaseConfig) -> Result<()> { self.native.validate(name, configuration) }
    async fn open_storage(&self, name: &str, configuration: &DatabaseConfig) -> Result<DatabaseStorage> {
        if self.fail_open { return Err(Anomaly::fault("test/open-failed", "Injected resource opening failure")); }
        self.native.open_storage(name, configuration).await
    }
    async fn open_logs(&self, name: &str, configuration: &DatabaseConfig, access: LogAccess, cancellation: CancellationToken, storage: &DatabaseStorage) -> Result<OpenedLogs> { self.native.open_logs(name, configuration, access, cancellation, storage).await }
    async fn close(&self) -> Result<()> { self.closing.add_permits(1); self.release.acquire().await.unwrap().forget(); self.native.close().await }
}

#[tokio::test]
async fn node_shutdown_and_failed_open_await_resource_region_close() {
    tokio::time::timeout(Duration::from_secs(20), async {
        for mode in 0..3 {
            let fail_open = mode == 1;
            let resources = Arc::new(ClosingResource { native: NativeResources::default(), fail_open, closing: Semaphore::new(0), release: Semaphore::new(0) });
            let mut services = services(); services.resources = resources.clone();
            if fail_open {
                let opening = tokio::spawn(Node::open(options(), services));
                resources.closing.acquire().await.unwrap().forget(); assert!(!opening.is_finished()); resources.release.add_permits(1);
                let Err(error) = opening.await.unwrap() else { panic!("opening should fail"); }; assert_eq!(error.code, "test/open-failed");
            } else if mode == 0 {
                let (node, driver) = running(options(), services).await; node.shutdown();
                resources.closing.acquire().await.unwrap().forget(); assert!(!driver.is_finished()); resources.release.add_permits(1);
                driver.await.unwrap().unwrap();
            } else {
                let (node, driver) = Node::open(options(), services).await.unwrap();
                let closing = tokio::spawn(driver.close_without_run());
                resources.closing.acquire().await.unwrap().forget(); assert!(!closing.is_finished()); assert!(node.cancellation().is_cancelled()); resources.release.add_permits(1);
                closing.await.unwrap().unwrap();
            }
        }
    }).await.unwrap();
}

#[tokio::test]
async fn failed_database_remains_queryable_and_can_be_detached_after_its_driver_joined() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut services = services();
        services.resources = Arc::new(FailedResource { native: NativeResources::default(), armed: AtomicBool::new(true) });
        let (node, driver) = running(options(), services).await;
        let token = CancellationToken::new();
        assert!(node.attach_database("other".into(), Default::default(), &Authority, &token).await.unwrap().committed());
        let database = node.catalog().database("other").unwrap();
        while database.progress().fault().is_none() { tokio::task::yield_now().await; }
        let mut session = node.session("other", Arc::new(Authority)).unwrap();
        assert_eq!(rows(session.query_sql("SELECT 1", Vec::new()).await.unwrap()).await, vec![vec![Value::I64(1)]]);
        assert!(node.detach_database("other".into(), &Authority, &token).await.unwrap().committed());
        while node.catalog().check_attach("other", &Default::default()).is_err() { tokio::task::yield_now().await; }
        assert!(node.attach_database("other".into(), Default::default(), &Authority, &token).await.unwrap().committed());
        stop(&node, driver).await;
    }).await.unwrap();
}

struct ObservedIoResources {
    native: NativeResources,
    opened: tokio::sync::mpsc::UnboundedSender<Arc<dyn rtdb_storage::ObjectStore>>,
    fail: Option<Arc<Semaphore>>,
}
#[async_trait]
impl ResourceFactory for ObservedIoResources {
    fn validate(&self, name: &str, configuration: &DatabaseConfig) -> Result<()> { self.native.validate(name, configuration) }
    async fn close(&self) -> Result<()> { self.native.close().await }
    async fn open_storage(&self, name: &str, configuration: &DatabaseConfig) -> Result<DatabaseStorage> {
        let storage = self.native.open_storage(name, configuration).await?;
        if name == "other" { self.opened.send(Arc::clone(&storage.store)).unwrap(); }
        Ok(storage)
    }
    async fn open_logs(&self, name: &str, configuration: &DatabaseConfig, access: LogAccess, cancellation: CancellationToken, storage: &DatabaseStorage) -> Result<OpenedLogs> {
        let mut logs = self.native.open_logs(name, configuration, access, cancellation.clone(), storage).await?;
        if name == "other" {
            if let Some(fail) = &self.fail {
                let fail = Arc::clone(fail);
                logs.drivers.push(async move {
                    tokio::select! {
                        _ = cancellation.cancelled() => Ok(()),
                        permit = fail.acquire() => { permit.unwrap().forget(); Err(Anomaly::fault("test/io-driver-failed", "Injected worker failure")) },
                    }
                }.boxed());
            }
        }
        Ok(logs)
    }
}

fn local_database(directory: &std::path::Path) -> DatabaseConfig {
    DatabaseConfig {
        log: Some(database_config::Log::LocalLog(LocalLog { path: Some(directory.join("log").to_string_lossy().into_owned()), epoch: None })),
        storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: Some(directory.join("store").to_string_lossy().into_owned()), epoch: None })),
        ..Default::default()
    }
}

#[tokio::test]
async fn detach_holds_the_name_until_cancelled_io_drains_for_running_and_failed_workers() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for fail_worker in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let (opened, mut stores) = tokio::sync::mpsc::unbounded_channel();
            let failure = Arc::new(Semaphore::new(0));
            let mut services = services();
            services.resources = Arc::new(ObservedIoResources { native: NativeResources::default().with_local_log_clock(Arc::clone(&services.clock), false), opened, fail: fail_worker.then(|| Arc::clone(&failure)) });
            let (node, driver) = running(options(), services).await;
            let token = CancellationToken::new();
            assert!(node.attach_database("other".into(), local_database(directory.path()), &Authority, &token).await.unwrap().committed());
            let store = stores.recv().await.unwrap();
            let database = node.catalog().database("other").unwrap();
            let mut session = node.session("other", Arc::new(Authority)).unwrap();
            assert!(session.state_mut().execute_tx(vec![put(&node, 1, 10)], AutonomousTxOptions::default()).await.unwrap().committed());
            database.flush().await.unwrap();
            while database.status().adopted_block.is_none() { tokio::task::yield_now().await; }
            if fail_worker {
                failure.add_permits(1);
                while database.progress().fault().is_none() { tokio::task::yield_now().await; }
                assert_eq!(rows(session.query_sql("SELECT value FROM docs", vec![]).await.unwrap()).await, vec![vec![Value::I64(10)]]);
                assert_eq!(database.block_lag().await.unwrap(), 0);
            }
            let release = io::hold_cancelled_local_read(&store).await;
            assert!(node.detach_database("other".into(), &Authority, &token).await.unwrap().committed());
            assert!(node.catalog().database("other").is_err());
            assert!(!node.catalog().secondary_databases().contains_key("other"));
            let missing = rtdb_storage::ObjectKey::new("closed-probe").unwrap();
            while !matches!(store.head(&missing).await, Err(rtdb_storage::Error::Closed(_))) { tokio::task::yield_now().await; }
            assert_eq!(node.catalog().check_attach("other", &DatabaseConfig::default()).unwrap_err().code, "xtdb/db-being-detached");
            let mut primary = node.session("xtdb", Arc::new(Authority)).unwrap();
            assert_eq!(rows(primary.query_sql("SELECT 1", vec![]).await.unwrap()).await, vec![vec![Value::I64(1)]]);
            release.send(()).unwrap();
            while node.catalog().check_attach("other", &DatabaseConfig::default()).is_err() { tokio::task::yield_now().await; }
            assert!(matches!(store.head(&missing).await, Err(rtdb_storage::Error::Closed(_))));
            assert!(node.attach_database("other".into(), DatabaseConfig::default(), &Authority, &token).await.unwrap().committed());
            stop(&node, driver).await;
        }
    }).await.unwrap();
}

struct PartialIoResources {
    native: NativeResources,
    opened: tokio::sync::mpsc::UnboundedSender<(Arc<dyn rtdb_storage::ObjectStore>, std::sync::mpsc::Sender<()>)>,
    pending: bool,
}
#[async_trait]
impl ResourceFactory for PartialIoResources {
    fn validate(&self, name: &str, configuration: &DatabaseConfig) -> Result<()> { self.native.validate(name, configuration) }
    async fn close(&self) -> Result<()> { self.native.close().await }
    async fn open_storage(&self, name: &str, configuration: &DatabaseConfig) -> Result<DatabaseStorage> { self.native.open_storage(name, configuration).await }
    async fn open_logs(&self, name: &str, configuration: &DatabaseConfig, access: LogAccess, cancellation: CancellationToken, storage: &DatabaseStorage) -> Result<OpenedLogs> {
        let mut logs = self.native.open_logs(name, configuration, access, cancellation, storage).await?;
        if name == "other" {
            let release = io::hold_cancelled_local_read(&storage.store).await;
            self.opened.send((Arc::clone(&storage.store), release)).unwrap();
            if self.pending { std::future::pending::<()>().await; }
            logs.replica = Arc::clone(&logs.source);
        }
        Ok(logs)
    }
}

#[tokio::test]
async fn failed_or_cancelled_attach_drains_partial_io_after_releasing_unstarted_log_writers() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for pending in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let (opened, mut stores) = tokio::sync::mpsc::unbounded_channel();
            let mut services = services();
            services.resources = Arc::new(PartialIoResources { native: NativeResources::default(), opened, pending });
            let (node, driver) = running(options(), services).await;
            let configuration = local_database(directory.path());
            let opening_node = node.clone();
            let opening = tokio::spawn(async move { opening_node.database_or_attach("other".into(), configuration).await });
            let (store, release) = stores.recv().await.unwrap();
            if pending { node.shutdown(); }
            let key = rtdb_storage::ObjectKey::new("closed-probe").unwrap();
            while !matches!(store.head(&key).await, Err(rtdb_storage::Error::Closed(_))) { tokio::task::yield_now().await; }
            assert!(!driver.is_finished());
            if !pending { assert!(!opening.is_finished()); }
            let (log, writer) = rtdb_storage::log::LocalLog::open(directory.path().join("log/LOG"), rtdb_storage::log::LocalLogOptions { maximum_message_bytes: 1024, queue_capacity: 8 }).unwrap();
            drop(writer);
            log.close_and_wait().await;
            release.send(()).unwrap();
            let error = match opening.await.unwrap() { Err(error) => error, Ok(_) => panic!("partial opening succeeded") };
            if pending { driver.await.unwrap().unwrap(); }
            else {
                assert_eq!(error.code, "rtdb.replication/aliased-logs");
                assert!(node.catalog().check_attach("other", &DatabaseConfig::default()).is_ok());
                let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
                assert_eq!(rows(session.query_sql("SELECT 1", vec![]).await.unwrap()).await, vec![vec![Value::I64(1)]]);
                stop(&node, driver).await;
            }
        }
    }).await.unwrap();
}

#[tokio::test]
async fn health_counts_noncritical_failures_but_only_critical_failures_make_alive_unhealthy() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (node, driver) = running(options(), services()).await;
        let health = HealthChecks::new(node.clone()).unwrap();
        assert_eq!(health.started().status, 200);
        assert_eq!(health.ready().body, "Ready.");
        assert_eq!(health.alive().await.unwrap().body, "Alive.");
        assert_eq!(health.finish_block(Some("missing")).await.status, 500);
        assert_eq!(health.finish_block(Some("xtdb")).await.status, 200);
        assert!(node.attach_database("other".into(), Default::default(), &Authority, &CancellationToken::new()).await.unwrap().committed());
        node.catalog().database("other").unwrap().progress().ingestion_failed(Anomaly::fault("test/noncritical", "Noncritical ingestion failed"));
        let response = health.alive().await.unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.headers["X-XTDB-Databases-Unhealthy"], "1");
        assert_eq!(response.headers["X-XTDB-Databases-Checked"], "2");
        node.catalog().database("xtdb").unwrap().progress().ingestion_failed(Anomaly::fault("test/critical", "Primary ingestion failed"));
        let response = health.alive().await.unwrap();
        assert_eq!(response.status, 503);
        assert!(response.body.contains("Primary ingestion failed"));
        assert_eq!(response.headers["X-XTDB-Databases-Unhealthy"], "2");
        node.shutdown();
        let _ = driver.await.unwrap();
    }).await.unwrap();
}

struct PendingIdentity { entered: Semaphore, released: Arc<AtomicBool> }
struct ReleaseFlag(Arc<AtomicBool>);
impl Drop for ReleaseFlag { fn drop(&mut self) { self.0.store(true, Ordering::Release); } }
#[async_trait]
impl SessionIdentity for PendingIdentity {
    fn user(&self) -> Option<&str> { Some("subject") }
    async fn revalidate(&self, _: Timestamp) -> Result<()> {
        let _release = ReleaseFlag(Arc::clone(&self.released));
        self.entered.add_permits(1);
        futures::future::pending().await
    }
    fn authorize_database(&self, _: &str, _: DatabaseAccess) -> Result<()> { Ok(()) }
}

#[tokio::test]
async fn caller_or_node_cancellation_drops_inflight_identity_refresh_before_query_admission() {
    tokio::time::timeout(Duration::from_secs(20), async {
        for cancel_node in [false, true] {
            let (node, driver) = running(options(), services()).await;
            let identity = Arc::new(PendingIdentity { entered: Semaphore::new(0), released: Arc::new(AtomicBool::new(false)) });
            let cancellation = CancellationToken::new();
            let request = {
                let node = node.clone(); let identity = Arc::clone(&identity); let cancellation = cancellation.clone();
                tokio::spawn(async move {
                    let basis = QueryBasis { snapshot: node.snapshot_basis()?, snapshot_time: None, current_time: node.query_instant(), timezone: "UTC".into() };
                    node.query_context("xtdb", &basis, identity.as_ref(), &cancellation).await
                })
            };
            identity.entered.acquire().await.unwrap().forget();
            if cancel_node { node.shutdown(); } else { cancellation.cancel(); }
            assert_eq!(request.await.unwrap().err().unwrap().category, rtdb_types::AnomalyCategory::Interrupted);
            assert!(identity.released.load(Ordering::Acquire));
            stop(&node, driver).await;
        }
    }).await.unwrap();
}

#[tokio::test]
async fn node_shutdown_cancels_direct_query_contexts_with_independent_caller_tokens() {
    let (node, driver) = running(options(), services()).await;
    let cancellation = CancellationToken::new();
    let basis = QueryBasis { snapshot: node.snapshot_basis().unwrap(), snapshot_time: None, current_time: node.query_instant(), timezone: "UTC".into() };
    let context = node.query_context("xtdb", &basis, &Authority, &cancellation).await.unwrap();
    let prepared = PreparedSql::prepare("SELECT 1", &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut cursor = prepared.open_query(context, Vec::new()).await.unwrap();
    stop(&node, driver).await;
    assert!(!cancellation.is_cancelled());
    assert_eq!(cursor.next_batch().await.unwrap_err().category, rtdb_types::AnomalyCategory::Interrupted);
}

#[tokio::test]
async fn skipped_membership_does_not_require_a_node_local_backend_factory() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut options = options();
        options.membership = Membership::Durable { primary: Default::default(), skipped: BTreeSet::from(["dormant".into()]) };
        let (node, driver) = running(options, services()).await;
        let configuration = DatabaseConfig {
            log: Some(database_config::Log::OtherLog(prost_types::Any { type_url: "proto.example/RemoteLog".into(), value: vec![1, 2, 3] })),
            ..Default::default()
        };
        let outcome = node.attach_database("dormant".into(), configuration.clone(), &Authority, &CancellationToken::new()).await.unwrap();
        assert!(outcome.committed(), "{:?}", outcome.error());
        assert_eq!(node.catalog().secondary_databases().get("dormant"), Some(&configuration));
        assert!(node.catalog().database("dormant").is_err());
        stop(&node, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn zero_compactor_threads_disable_compaction_and_shared_memory_covers_sessions() {
    let mut options = options();
    options.compaction_threads = 0;
    let memory = rtdb_columnar::MemoryBudget::new(options.execution_memory_bytes);
    let (node, driver) = Node::open_with_memory(options, services(), Arc::clone(&memory)).await.unwrap();
    assert!(Arc::ptr_eq(node.memory(), &memory));
    assert!(node.catalog().database("xtdb").unwrap().compaction().is_none());
    let baseline = memory.used_bytes();
    let session = node.session("xtdb", Arc::new(Authority)).unwrap();
    assert!(memory.used_bytes() > baseline);
    drop(session);
    assert_eq!(memory.used_bytes(), baseline);
    let task = tokio::spawn(driver.run());
    stop(&node, task).await;
}

struct SourceCaptureGate { started: tokio::sync::oneshot::Sender<()>, release: std::sync::mpsc::Receiver<()> }
struct HeldSourceCapture {
    _charge: Arc<rtdb_columnar::MemoryReservation>,
    dropped: Arc<AtomicBool>,
}
impl Drop for HeldSourceCapture { fn drop(&mut self) { self.dropped.store(true, Ordering::SeqCst); } }
struct HeldSourceFactory {
    gate: std::sync::Mutex<Option<SourceCaptureGate>>,
    opened: tokio::sync::mpsc::UnboundedSender<Arc<rtdb_storage::IoRegion>>,
    memory: Arc<rtdb_columnar::MemoryBudget>,
    dropped: Arc<AtomicBool>,
}
struct HeldSource {
    gate: Option<SourceCaptureGate>,
    io: Arc<rtdb_storage::IoRegion>,
    memory: Arc<rtdb_columnar::MemoryBudget>,
    dropped: Arc<AtomicBool>,
}
#[async_trait]
impl ExternalSourceFactory for HeldSourceFactory {
    fn validate(&self, _: &prost_types::Any) -> Result<()> { Ok(()) }
    async fn open(&self, _: &prost_types::Any, _: &str, io: Arc<rtdb_storage::IoRegion>) -> Result<Box<dyn ExternalSource>> {
        self.opened.send(Arc::clone(&io)).unwrap();
        Ok(Box::new(HeldSource { gate: self.gate.lock().unwrap().take(), io, memory: Arc::clone(&self.memory), dropped: Arc::clone(&self.dropped) }))
    }
}
#[async_trait]
impl ExternalSource for HeldSource {
    async fn assign(&mut self) -> Result<Box<dyn ExternalSourceDriver>> {
        let Some(gate) = self.gate.take() else { return std::future::pending().await; };
        let charge = self.memory.reserve(4096)?;
        let dropped = Arc::clone(&self.dropped);
        let _capture = self.io.run_blocking(move || {
            gate.started.send(()).unwrap();
            gate.release.recv().unwrap();
            Ok(HeldSourceCapture { _charge: charge, dropped })
        }).await.map_err(Anomaly::from)?;
        std::future::pending().await
    }
}

#[tokio::test]
async fn cancelled_source_assignment_io_drains_before_database_name_reuse_or_node_shutdown() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for shutdown in [false, true] {
            let (entered, started) = tokio::sync::oneshot::channel();
            let (release, held) = std::sync::mpsc::channel();
            let (opened, mut regions) = tokio::sync::mpsc::unbounded_channel();
            let memory = rtdb_columnar::MemoryBudget::new(4096);
            let dropped = Arc::new(AtomicBool::new(false));
            let mut services = services();
            services.external_sources.insert("test.HeldSource".into(), Arc::new(HeldSourceFactory {
                gate: std::sync::Mutex::new(Some(SourceCaptureGate { started: entered, release: held })), opened,
                memory: Arc::clone(&memory), dropped: Arc::clone(&dropped),
            }));
            let (node, driver) = running(options(), services).await;
            let token = CancellationToken::new();
            let configuration = DatabaseConfig { external_source: Some(prost_types::Any { type_url: "proto.xtdb.com/test.HeldSource".into(), value: Vec::new() }), ..Default::default() };
            assert!(node.attach_database("other".into(), configuration, &Authority, &token).await.unwrap().committed());
            let io = regions.recv().await.unwrap();
            started.await.unwrap();
            assert_eq!(memory.used_bytes(), 4096);
            if shutdown { node.shutdown(); }
            else { assert!(node.detach_database("other".into(), &Authority, &token).await.unwrap().committed()); }
            while !matches!(io.run_blocking(|| Ok(())).await, Err(rtdb_storage::Error::Closed(_))) { tokio::task::yield_now().await; }
            assert!(!dropped.load(Ordering::SeqCst));
            assert_eq!(memory.used_bytes(), 4096);
            assert!(!driver.is_finished());
            if !shutdown {
                assert!(node.catalog().database("other").is_err());
                assert!(!node.catalog().secondary_databases().contains_key("other"));
                assert_eq!(node.catalog().check_attach("other", &DatabaseConfig::default()).unwrap_err().code, "xtdb/db-being-detached");
                let mut primary = node.session("xtdb", Arc::new(Authority)).unwrap();
                assert_eq!(rows(primary.query_sql("SELECT 1", vec![]).await.unwrap()).await, [vec![Value::I64(1)]]);
            }
            release.send(()).unwrap();
            if shutdown { driver.await.unwrap().unwrap(); }
            else {
                while node.catalog().check_attach("other", &DatabaseConfig::default()).is_err() { tokio::task::yield_now().await; }
                assert!(node.attach_database("other".into(), DatabaseConfig::default(), &Authority, &token).await.unwrap().committed());
                stop(&node, driver).await;
            }
            assert!(dropped.load(Ordering::SeqCst));
            assert_eq!(memory.used_bytes(), 0);
            assert!(matches!(io.run_blocking(|| Ok(())).await, Err(rtdb_storage::Error::Closed(_))));
        }
    }).await.unwrap();
}

#[tokio::test]
async fn source_log_uri_values_render_as_nfc_ascii_without_changing_stored_text() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (node, driver) = running(options(), services()).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        let original = "HTTP://host/e\u{301}";
        let operation = TxOp::PutDocs { table: TableRef::new("public","docs"), documents: Documents::new(vec![StructFields::from([
            ("_id".into(),Value::I64(1)),("uri".into(),Value::Uri(original.into())),
        ])],node.memory()).unwrap(),valid_from: None,valid_to: None };
        session.state_mut().execute_operation(operation).await.unwrap();
        let messages = rows(session.query_sql("SELECT tx_ops FROM xt.source_log_msgs WHERE msg_id BETWEEN 0 AND 100",vec![]).await.unwrap()).await;
        assert!(messages.iter().any(|row| matches!(&row[0],Value::Utf8(text) if text.contains("#xt/uri \"HTTP://host/%C3%A9\""))));
        assert_eq!(rows(session.query_sql("SELECT CAST(uri AS VARCHAR) FROM docs",vec![]).await.unwrap()).await,[vec![Value::Utf8(original.into())]]);
        stop(&node,driver).await;
    }).await.unwrap();
}

mod peer_failure {
    use super::*;
    use prost::bytes::Bytes;
    use rtdb_replication::Role;
    use rtdb_storage::log::{AppendReceipt, Log, LogOffset, LogRange, LogRecord, LogTail};

    struct Gate { entered: Semaphore, release: Semaphore }
    impl Gate {
        fn new() -> Self { Self { entered: Semaphore::new(0), release: Semaphore::new(0) } }
        async fn hold(&self) {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
    }

    struct GatedReplica { log: Arc<dyn Log>, tail: Arc<Gate> }
    #[async_trait]
    impl Log for GatedReplica {
        async fn enqueue(&self, payload: Bytes, timestamp: Timestamp) -> rtdb_storage::Result<AppendReceipt> {
            self.log.enqueue(payload, timestamp).await
        }
        fn latest(&self) -> rtdb_storage::Result<LogOffset> { self.log.latest() }
        async fn tail(&self, after: LogOffset) -> rtdb_storage::Result<Box<dyn LogTail>> {
            self.tail.hold().await;
            self.log.tail(after).await
        }
        async fn range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Box<dyn LogRange>> {
            self.log.range(from, to).await
        }
        async fn read_range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Vec<LogRecord>> {
            self.log.read_range(from, to).await
        }
    }

    fn fault() -> Anomaly { Anomaly::fault("test/peer-log-failed", "Injected database log peer failure") }

    struct Resources {
        native: NativeResources,
        failure: Arc<Semaphore>,
        teardown: Arc<Gate>,
        tail: Option<Arc<Gate>>,
    }

    #[async_trait]
    impl ResourceFactory for Resources {
        fn validate(&self, name: &str, config: &DatabaseConfig) -> Result<()> { self.native.validate(name, config) }
        async fn close(&self) -> Result<()> { self.native.close().await }
        async fn open_storage(&self, name: &str, config: &DatabaseConfig) -> Result<DatabaseStorage> {
            self.native.open_storage(name, config).await
        }
        async fn open_logs(&self, name: &str, config: &DatabaseConfig, access: LogAccess, cancellation: CancellationToken, storage: &DatabaseStorage) -> Result<OpenedLogs> {
            let mut logs = self.native.open_logs(name, config, access, cancellation.clone(), storage).await?;
            if name == "other" {
                if let Some(tail) = &self.tail {
                    logs.replica = Arc::new(GatedReplica { log: Arc::clone(&logs.replica), tail: Arc::clone(tail) });
                }
                let failure = Arc::clone(&self.failure);
                let failure_cancellation = cancellation.clone();
                logs.drivers.push(async move {
                    tokio::select! {
                        _ = failure_cancellation.cancelled() => Ok(()),
                        permit = failure.acquire() => { permit.unwrap().forget(); Err(fault()) },
                    }
                }.boxed());
                let teardown = Arc::clone(&self.teardown);
                logs.drivers.push(async move {
                    cancellation.cancelled().await;
                    teardown.hold().await;
                    Ok(())
                }.boxed());
            }
            Ok(logs)
        }
    }

    async fn start(tail: Option<Arc<Gate>>) -> (Node, tokio::task::JoinHandle<Result<()>>, Arc<Resources>) {
        let resources = Arc::new(Resources {
            native: NativeResources::default(),
            failure: Arc::new(Semaphore::new(0)),
            teardown: Arc::new(Gate::new()),
            tail,
        });
        let mut services = services();
        services.resources = resources.clone();
        let (node, driver) = running(options(), services).await;
        assert!(node.attach_database("other".into(), Default::default(), &Authority, &CancellationToken::new()).await.unwrap().committed());
        (node, driver, resources)
    }

    async fn release_and_stop(node: &Node, driver: tokio::task::JoinHandle<Result<()>>, resources: &Resources) {
        resources.teardown.release.add_permits(1);
        let cancellation = CancellationToken::new();
        assert!(node.detach_database("other".into(), &Authority, &cancellation).await.unwrap().committed());
        while node.catalog().check_attach("other", &DatabaseConfig::default()).is_err() { tokio::task::yield_now().await; }
        stop(node, driver).await;
    }

    #[tokio::test]
    async fn running_database_publishes_the_peer_fault_before_log_teardown_finishes() {
        tokio::time::timeout(Duration::from_secs(20), async {
            let (node, driver, resources) = start(None).await;
            let cancellation = CancellationToken::new();
            let database = node.catalog().database("other").unwrap();
            let mut session = node.session("other", Arc::new(Authority)).unwrap();
            let applied = session.state_mut().execute_tx(vec![put(&node, 1, 10)], AutonomousTxOptions::default()).await.unwrap();
            assert!(applied.committed());
            let key = applied.tx_key();
            let next = TxId(key.tx_id.0 + 1);
            let wait = node.await_transaction("other", next, &Authority, &cancellation);
            tokio::pin!(wait);
            assert!(futures::poll!(&mut wait).is_pending());

            resources.failure.add_permits(1);
            resources.teardown.entered.acquire().await.unwrap().forget();
            assert_eq!(database.status().role, Role::Stopped);
            assert_eq!(wait.await.unwrap_err(), fault());
            assert_eq!(node.await_transaction("other", next, &Authority, &cancellation).await.unwrap_err(), fault());
            assert_eq!(node.await_transaction("other", key.tx_id, &Authority, &cancellation).await.unwrap().tx_key(), key);
            release_and_stop(&node, driver, &resources).await;
        }).await.unwrap();
    }

    #[tokio::test]
    async fn successful_replica_tail_open_preserves_an_earlier_peer_fault() {
        tokio::time::timeout(Duration::from_secs(20), async {
            let tail = Arc::new(Gate::new());
            let (node, driver, resources) = start(Some(Arc::clone(&tail))).await;
            let cancellation = CancellationToken::new();
            let database = node.catalog().database("other").unwrap();
            tail.entered.acquire().await.unwrap().forget();
            let wait = node.await_transaction("other", TxId(0), &Authority, &cancellation);
            tokio::pin!(wait);
            assert!(futures::poll!(&mut wait).is_pending());

            resources.failure.add_permits(1);
            assert_eq!(wait.await.unwrap_err(), fault());
            assert_eq!(database.status().role, Role::Following);
            tail.release.add_permits(1);
            resources.teardown.entered.acquire().await.unwrap().forget();
            assert_eq!(database.status().role, Role::Stopped);
            assert_eq!(database.status().highest_term, rtdb_types::Term(0));
            assert_eq!(database.progress().fault(), Some(fault()));
            assert_eq!(node.await_transaction("other", TxId(0), &Authority, &cancellation).await.unwrap_err(), fault());
            release_and_stop(&node, driver, &resources).await;
        }).await.unwrap();
    }
}

#[tokio::test]
async fn a_read_basis_after_the_latest_completed_tx_names_both_instants() {
    let (node, driver) = running(options(), services()).await;
    let cancellation = CancellationToken::new();
    let basis_at = |micros: i64| QueryBasis {
        snapshot: rtdb_types::TimeBasis { databases: BTreeMap::from([("xtdb".into(), vec![Some(rtdb_types::Instant::from(Timestamp(micros)))])]) },
        snapshot_time: None, current_time: node.query_instant(), timezone: "UTC".into() };
    let error = node.query_context("xtdb", &basis_at(1_577_836_800_000_000), &Authority, &cancellation).await.err().unwrap();
    assert_eq!((error.code.as_str(), error.message.as_deref()), ("xtdb/unindexed-tx", Some("system-time (2020-01-01T00:00:00Z) is after the latest completed tx (nil)")));
    assert_eq!(serde_json::Value::Object(error.context.clone().into_iter().collect()), serde_json::json!({
        "latest-completed-tx": rtdb_types::transit_value::to_json_value(&rtdb_types::transit_value::TransitValue::Null).unwrap(),
        "system-time": rtdb_types::transit_value::to_json_value(&rtdb_types::transit_value::TransitValue::instant(rtdb_types::Instant::from(Timestamp(1_577_836_800_000_000))).unwrap()).unwrap(),
    }));
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    assert!(session.state_mut().execute_tx(vec![put(&node, 1, 10)], AutonomousTxOptions::default()).await.unwrap().committed());
    let error = node.query_context("xtdb", &basis_at(1_609_459_200_000_000), &Authority, &cancellation).await.err().unwrap();
    let message = error.message.unwrap();
    assert!(message.starts_with("system-time (2021-01-01T00:00:00Z) is after the latest completed tx (#xt/tx-key {:tx-id 0, :system-time #xt/instant \"1970-01-01T00:00:"), "{message}");
    stop(&node, driver).await;
}

#[tokio::test]
async fn clock_time_is_shown_in_the_session_zone_and_database_controls_report_their_target() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let token = session.state().cancellation().child_token();
    let parse = |sql| rtdb_sql::parse_control(sql, &Default::default()).unwrap().unwrap();
    session.state_mut().set_timezone("Europe/Paris").unwrap();
    let ControlResult::Rows(show) = session.execute_control(&parse("SHOW CLOCK_TIME"), &[], &token).await.unwrap() else { panic!() };
    let shown = rows(show).await;
    assert!(matches!(&shown[0][0], Value::Timestamp { timezone: Some(zone), .. } if zone == "Europe/Paris"), "{shown:?}");

    let error = match session.execute_control(&parse("ATTACH DATABASE new_db WITH $$ somethingElse: $$ "), &[], &token).await { Ok(_) => panic!(), Err(error) => error };
    assert_eq!(error.message.as_deref(), Some("Invalid database config in `ATTACH DATABASE`: Unknown property 'somethingElse'. Known properties are: critical, externalSource, log, mode, partitions, storage"));
    assert_eq!(error.context["sql"], "ATTACH DATABASE new_db WITH $$ somethingElse: $$");

    session.execute_control(&parse("BEGIN"), &[], &token).await.unwrap();
    for (sql, code, name) in [("ATTACH DATABASE new_db", "xtdb.pgwire/attach-db-in-tx", "new_db"), ("DETACH DATABASE old_db", "xtdb.pgwire/detach-db-in-tx", "old_db")] {
        let error = match session.execute_control(&parse(sql), &[], &token).await { Ok(_) => panic!(), Err(error) => error };
        assert_eq!((error.code.as_str(), &error.context["db-name"]), (code, &serde_json::json!(name)));
    }
    session.execute_control(&parse("ROLLBACK"), &[], &token).await.unwrap();
    stop(&node, driver).await;
}

#[tokio::test]
async fn txs_table_lists_every_submitted_transaction() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    for value in 0..40 { assert!(session.state_mut().execute_tx(vec![put(&node, 1, value)], AutonomousTxOptions::default()).await.unwrap().committed()); }
    let txs = tokio::time::timeout(Duration::from_secs(20), async { rows(session.query_sql("FROM xt.txs", vec![]).await.unwrap()).await }).await.expect("FROM xt.txs completes");
    assert_eq!(txs.len(), 40);
    let history = rows(session.query_sql("SELECT * FROM docs FOR VALID_TIME ALL", vec![]).await.unwrap()).await;
    assert_eq!(history.len(), 40);
    stop(&node, driver).await;
}

#[tokio::test]
async fn system_time_as_of_after_the_basis_reads_as_of_the_basis() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    for value in [1, 2] { assert!(session.state_mut().execute_tx(vec![put(&node, 1, value)], AutonomousTxOptions::default()).await.unwrap().committed()); }
    let query = "SELECT value FROM docs FOR ALL VALID_TIME FOR SYSTEM_TIME AS OF TIMESTAMP '3002-01-01 00:00:00+00:00' ORDER BY value";
    assert_eq!(rows(session.query_sql(query, vec![]).await.unwrap()).await, [vec![Value::I64(1)], vec![Value::I64(2)]]);
    let past = "SELECT value FROM docs FOR ALL VALID_TIME FOR SYSTEM_TIME AS OF TIMESTAMP '1960-01-01 00:00:00+00:00'";
    assert!(rows(session.query_sql(past, vec![]).await.unwrap()).await.is_empty());
    stop(&node, driver).await;
}

#[tokio::test]
async fn a_write_to_a_read_only_database_is_rejected_by_its_log() {
    let (node, driver) = running(options(), services()).await;
    let token = CancellationToken::new();
    let mut configuration = rtdb_format::config::default_database_config();
    configuration.mode = Some(1);
    assert!(node.attach_database("ro_db".into(), configuration, &Authority, &token).await.unwrap().committed());
    let mut session = node.session("ro_db", Arc::new(Authority)).unwrap();
    let error = match session.state_mut().execute_tx(vec![put(&node, 1, 1)], AutonomousTxOptions::default()).await { Ok(result) => result.error().cloned().expect("rejected"), Err(error) => error };
    assert_eq!((error.code.as_str(), error.message.as_deref()), ("xtdb/read-only-database", Some("Cannot append to read-only database log")));
    stop(&node, driver).await;
}

#[tokio::test]
async fn a_table_of_wide_heterogeneous_documents_stays_queryable_within_its_budget() {
    let mut options = options();
    options.execution_memory_bytes = 64 * 1024 * 1024;
    let (node, driver) = running(options, services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let leaf = |i: i64| match i % 6 {
        0 => Value::I64(i), 1 => Value::Utf8(format!("s{i}")), 2 => Value::F64(i as f64 / 3.0), 3 => Value::Bool(i % 4 == 0),
        4 => Value::Timestamp { value: i * 1_000_000, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some(format!("+{:02}:00", i % 12)) },
        _ => Value::Keyword(rtdb_types::Keyword::from_name(&format!("k{i}"))),
    };
    for i in 0..60 {
        let nested = Value::Struct(StructFields::from([(format!("f{}", i % 7), leaf(i)), ("l".into(), Value::list((0..(i % 5)).map(|j| leaf(i + j)).collect()))]));
        let document = BTreeMap::from([("_id".into(), Value::I64(1)), (format!("c{}", i % 9), Value::list(vec![nested, leaf(i + 1)]))]);
        let put = TxOp::PutDocs { table: TableRef::new("public", "docs"), documents: Documents::new(vec![document.into()], node.memory()).unwrap(), valid_from: None, valid_to: None };
        assert!(session.state_mut().execute_tx(vec![put], AutonomousTxOptions::default()).await.unwrap().committed());
    }
    assert_eq!(rows(session.query_sql("SELECT * FROM docs FOR VALID_TIME ALL", vec![]).await.unwrap()).await.len(), 60);
    stop(&node, driver).await;
}

#[tokio::test]
async fn now_in_an_xtql_temporal_filter_is_the_query_time() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    assert!(session.state_mut().execute_tx(vec![put(&node, 1, 7)], AutonomousTxOptions::default()).await.unwrap().committed());
    let current = "(from :docs {:bind [value] :for-valid-time (in :now nil)})";
    assert_eq!(rows(session.query_xtql(current, vec![]).await.unwrap()).await, [vec![Value::I64(7)]]);
    let at_now = "(from :docs {:bind [value] :for-valid-time (at :now)})";
    assert_eq!(rows(session.query_xtql(at_now, vec![]).await.unwrap()).await, [vec![Value::I64(7)]]);
    stop(&node, driver).await;
}

#[tokio::test]
async fn a_left_join_through_an_argument_subquery_takes_the_dependent_binding() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    for (id, b) in [(1, 1), (2, 3), (3, 0)] {
        let document = BTreeMap::from([("_id".into(), Value::I64(id)), ("a".into(), Value::I64(2)), ("b".into(), Value::I64(b))]);
        let put = TxOp::PutDocs { table: TableRef::new("public", "docs"), documents: Documents::new(vec![document.into()], node.memory()).unwrap(), valid_from: None, valid_to: None };
        assert!(session.state_mut().execute_tx(vec![put], AutonomousTxOptions::default()).await.unwrap().committed());
    }
    let query = "(-> (unify (from :docs [{:xt/id id} a b]) (left-join [(fn [a] (rel [{:b a}] [b])) (+ a 1)] [b])) (order-by id) (return id b))";
    assert_eq!(rows(session.query_xtql(query, vec![]).await.unwrap()).await, [vec![Value::I64(1), Value::Null], vec![Value::I64(2), Value::I64(3)], vec![Value::I64(3), Value::Null]]);
    stop(&node, driver).await;
}

#[tokio::test]
async fn xtql_projections_keep_their_written_column_order() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    for (query, expected) in [
        ("(-> (rel [{:x 1 :y 2}] [x y]) (return y x))", vec!["y", "x"]),
        ("(-> (rel [{:x 1 :y 2}] [x y]) (with {:b 3} {:a 4}))", vec!["x", "y", "b", "a"]),
    ] {
        let cursor = session.query_xtql(query, vec![]).await.unwrap();
        assert_eq!(cursor.schema().fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), expected, "{query}");
    }
    stop(&node, driver).await;
}

#[tokio::test]
async fn wide_heterogeneous_documents_flush_and_delete_across_blocks() {
    let mut options = options();
    options.execution_memory_bytes = 128 * 1024 * 1024;
    let (node, driver) = running(options, services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let leaf = |i: i64| match i % 5 {
        0 => Value::I64(i), 1 => Value::Utf8(format!("s{i}")), 2 => Value::F64(i as f64 / 3.0), 3 => Value::Bool(i % 4 == 0),
        _ => Value::Timestamp { value: i * 1_000_000, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some(format!("+{:02}:00", i % 12)) },
    };
    let nested = |i: i64| (0..3).fold(leaf(i), |inner, depth| Value::Struct(StructFields::from([(format!("f{i}_{depth}"), inner), (format!("g{i}_{depth}"), Value::list(vec![leaf(i + depth), leaf(i + depth + 1)]))])));
    let documents = (0..80).map(|i| StructFields::from([
        ("_id".into(), Value::I64(i)),
        (format!("c{i}"), Value::list(vec![nested(i), leaf(i + 1)])),
    ])).collect::<Vec<_>>();
    let flush = || async { node.catalog().database("xtdb").unwrap().flush().await.unwrap(); node.sync_all(Duration::from_secs(30), &CancellationToken::new()).await.unwrap(); };
    let put = TxOp::PutDocs { table: TableRef::new("public", "docs"), documents: Documents::new(documents, node.memory()).unwrap(), valid_from: None, valid_to: None };
    assert!(session.state_mut().execute_tx(vec![put], AutonomousTxOptions::default()).await.unwrap().committed());
    flush().await;
    let delete = TxOp::DeleteDocs { table: TableRef::new("public", "docs"), ids: rtdb_tx::EntityIds::new((0..80).map(|id| Value::I64(id).entity_id().unwrap()).collect(), node.memory()).unwrap(), valid_from: None, valid_to: None };
    assert!(session.state_mut().execute_tx(vec![delete], AutonomousTxOptions::default()).await.unwrap().committed());
    flush().await;
    assert!(rows(session.query_sql("SELECT * FROM docs", vec![]).await.unwrap()).await.is_empty());
    assert_eq!(rows(session.query_sql("SELECT * FROM docs FOR ALL VALID_TIME", vec![]).await.unwrap()).await.len(), 80);
    stop(&node, driver).await;
}

#[tokio::test]
async fn a_left_join_without_arguments_keeps_the_outer_binding() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    for (id, parent) in [(1, None), (2, Some(1))] {
        let mut document = BTreeMap::from([("_id".into(), Value::I64(id))]);
        if let Some(parent) = parent { document.insert("parent".into(), Value::I64(parent)); }
        let put = TxOp::PutDocs { table: TableRef::new("public", "docs"), documents: Documents::new(vec![document.into()], node.memory()).unwrap(), valid_from: None, valid_to: None };
        assert!(session.state_mut().execute_tx(vec![put], AutonomousTxOptions::default()).await.unwrap().committed());
    }
    let query = "(-> (unify (from :docs [{:xt/id e}]) (left-join (from :docs [{:xt/id c :parent e}]) [e c])) (order-by e c) (return e c))";
    assert_eq!(rows(session.query_xtql(query, vec![]).await.unwrap()).await, [vec![Value::I64(1), Value::I64(2)], vec![Value::I64(2), Value::Null]]);
    stop(&node, driver).await;
}

#[tokio::test]
async fn executing_a_transaction_without_an_indexer_fails_and_queries_still_run() {
    let mut config = options(); config.replication.enabled = false;
    let (node, driver) = running(config, services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let error = tokio::time::timeout(Duration::from_secs(10), session.state_mut().execute_tx(vec![put(&node, 1, 10)], AutonomousTxOptions::default())).await.unwrap().unwrap_err();
    assert_eq!((error.category, error.message.as_deref()), (rtdb_types::AnomalyCategory::Fault, Some("log processor not initialised")));
    assert_eq!(rows(session.query_sql("SELECT 1 AS v", vec![]).await.unwrap()).await, [vec![Value::I64(1)]]);
    drop(session); stop(&node, driver).await;
}

#[tokio::test]
async fn a_put_error_names_its_table_and_a_patch_error_does_not() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let TxOp::PutDocs { table, documents, .. } = put(&node, 1, 10) else { unreachable!() };
    let inverted = TxOp::PutDocs { table: table.clone(), documents, valid_from: Some(rtdb_types::Timestamp(2_000)), valid_to: Some(rtdb_types::Timestamp(1_000)) };
    let result = session.state_mut().execute_tx(vec![inverted], AutonomousTxOptions::default()).await.unwrap();
    let error = result.error().expect("the inverted valid time aborts the transaction");
    assert_eq!(error.code, "xtdb.indexer/invalid-valid-times");
    assert_eq!(rtdb_format::transit::from_json_value(&error.context["table"]).unwrap(), rtdb_format::transit::TransitValue::table_ref(&table));
    let TxOp::PutDocs { table, documents, .. } = put(&node, 1, 10) else { unreachable!() };
    let patch = TxOp::PatchDocs { table, documents, valid_from: Some(rtdb_types::Timestamp(2_000)), valid_to: Some(rtdb_types::Timestamp(1_000)) };
    let result = session.state_mut().execute_tx(vec![patch], AutonomousTxOptions::default()).await.unwrap();
    let error = result.error().expect("the inverted valid time aborts the patch");
    assert_eq!(error.code, "xtdb.indexer/invalid-valid-times");
    assert!(!error.context.contains_key("table"), "the reference's patch path adds no table: {:?}", error.context);
    drop(session); stop(&node, driver).await;
}

#[tokio::test]
async fn a_stored_struct_reads_back_with_its_fields_in_written_order() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let data = Value::Struct(StructFields::from([("foo".into(), Value::I64(1)), ("bar".into(), Value::I64(2))]));
    let documents = Documents::new(vec![StructFields::from([("_id".into(), Value::I64(1)), ("data".into(), data.clone())])], node.memory()).unwrap();
    let put = TxOp::PutDocs { table: TableRef::new("public", "docs"), documents, valid_from: None, valid_to: None };
    assert!(session.state_mut().execute_tx(vec![put], AutonomousTxOptions::default()).await.unwrap().committed());
    let insert = TxOp::Sql { sql: "INSERT INTO sql_docs (_id, data) VALUES (1, OBJECT(foo: 1, bar: 2))".into(), args: None };
    assert!(session.state_mut().execute_tx(vec![insert], AutonomousTxOptions::default()).await.unwrap().committed());
    for table in ["docs", "sql_docs"] {
        let cursor = session.query_sql(&format!("SELECT data FROM {table}"), vec![]).await.unwrap();
        let Value::Struct(fields) = &rows(cursor).await[0][0] else { panic!("Expected a struct") };
        assert_eq!(fields.keys().collect::<Vec<_>>(), ["foo", "bar"], "{table}");
    }
    let equal = rows(session.query_sql("SELECT OBJECT(a: 1, b: 2) = OBJECT(b: 2, a: 1), OBJECT(a: 1) = OBJECT(a: 1, b: 2)", vec![]).await.unwrap()).await;
    assert_eq!(equal, [vec![Value::Bool(true), Value::Bool(false)]], "structs compare as maps whatever their field order");
    drop(session); stop(&node, driver).await;
}

#[tokio::test]
async fn a_placed_catalog_starts_empty_and_holds_exactly_what_is_attached_and_detached_locally() {
    let mut config = options();
    config.membership = Membership::Placed;
    let (node, driver) = running(config, services()).await;
    assert!(node.catalog().database_names().is_empty(), "nothing starts open, not even a primary");
    assert!(node.catalog().secondary_databases().is_empty());
    assert_eq!(node.catalog().check_attach("shop", &DatabaseConfig::default()).unwrap_err().code, "xtdb/fixed-database-catalog", "log-borne instructions are refused");

    node.database_or_attach("shop".into(), DatabaseConfig::default()).await.unwrap().expect("a placed catalog attaches locally");
    let mut session = node.session("shop", Arc::new(Authority)).unwrap();
    assert!(session.state_mut().execute_tx(vec![put(&node, 1, 10)], AutonomousTxOptions::default()).await.unwrap().committed());
    assert_eq!(rows(session.query_sql("SELECT value FROM docs", vec![]).await.unwrap()).await, [vec![Value::I64(10)]]);
    drop(session);

    node.detach_local("shop".into()).await.unwrap();
    assert!(node.catalog().database("shop").is_err(), "a detached database no longer resolves");
    tokio::time::timeout(Duration::from_secs(10), async {
        while node.catalog().is_draining("shop") { tokio::task::yield_now().await; }
    }).await.expect("the detach drains");
    node.database_or_attach("shop".into(), DatabaseConfig::default()).await.unwrap().expect("the name is free again once drained");
    stop(&node, driver).await;
}

#[tokio::test]
async fn a_session_on_a_placed_node_sees_its_own_database_and_no_other() {
    let mut config = options();
    config.membership = Membership::Placed;
    let (node, driver) = running(config, services()).await;
    for name in ["shop", "blog"] { node.database_or_attach(name.into(), DatabaseConfig::default()).await.unwrap().unwrap(); }
    let mut blog = node.session("blog", Arc::new(Authority)).unwrap();
    assert!(blog.state_mut().execute_tx(vec![put(&node, 1, 10)], AutonomousTxOptions::default()).await.unwrap().committed());
    let mut shop = node.session("shop", Arc::new(Authority)).unwrap();
    assert!(shop.state_mut().execute_tx(vec![put(&node, 1, 20)], AutonomousTxOptions::default()).await.unwrap().committed());
    assert_eq!(rows(shop.query_sql("SELECT value FROM docs", vec![]).await.unwrap()).await, [vec![Value::I64(20)]]);
    let reached = match shop.query_sql("SELECT value FROM blog.public.docs", vec![]).await { Ok(cursor) => Some(rows(cursor).await), Err(_) => None };
    assert!(reached.as_ref().is_none_or(|rows| rows.is_empty()), "another tenant's table is not reachable by name: {reached:?}");
    assert_eq!(rows(blog.query_sql("SELECT value FROM blog.public.docs", vec![]).await.unwrap()).await, [vec![Value::I64(10)]], "a session still names its own database");
    shop.state_mut().set_auto_commit(false).await.unwrap();
    let basis = shop.state_mut().read_basis().await.unwrap();
    assert_eq!(basis.snapshot.databases.keys().cloned().collect::<Vec<_>>(), ["shop"], "a session's basis names no other tenant");
    shop.state_mut().rollback_tx().unwrap();
    drop((shop, blog));
    stop(&node, driver).await;
}

#[tokio::test]
async fn concurrent_transactions_with_one_idempotency_key_commit_once_and_both_report_it() {
    let (node, driver) = running(options(), services()).await;
    let metadata = || {
        let mut options = AutonomousTxOptions::default();
        options.user_metadata = Some(rtdb_tx::UserMetadata::new(Value::Struct(StructFields::from([("idempotency_key".into(), Value::Utf8("charge-7".into()))])), node.memory()).unwrap());
        options
    };
    let mut first = node.session("xtdb", Arc::new(Authority)).unwrap();
    let mut second = node.session("xtdb", Arc::new(Authority)).unwrap();
    let (a, b) = tokio::join!(
        first.state_mut().execute_tx(vec![put(&node, 1, 10)], metadata()),
        second.state_mut().execute_tx(vec![put(&node, 2, 20)], metadata()),
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert!(a.committed() && b.committed(), "both callers are told the keyed transaction committed");
    assert_eq!(a.tx_key(), b.tx_key(), "and both are told it is the same transaction");
    assert_eq!(rows(first.query_sql("SELECT COUNT(*) FROM docs", vec![]).await.unwrap()).await, [vec![Value::I64(1)]], "only one of them ran");
    let mut reserved = AutonomousTxOptions::default();
    reserved.user_metadata = Some(rtdb_tx::UserMetadata::new(Value::Struct(StructFields::from([("idempotency_key".into(), Value::Utf8("~table".into()))])), node.memory()).unwrap());
    assert!(first.state_mut().execute_tx(vec![put(&node, 3, 30)], reserved).await.is_err(), "the key the keys table is created with is not a caller's to use");
    drop((first, second));
    stop(&node, driver).await;
}

#[tokio::test]
async fn duplicate_transaction_outcomes_are_deployment_local_and_repeatable() {
    let mut config = options();
    config.membership = Membership::Placed;
    let clock = Arc::new(TickClock(AtomicI64::new(1_000_000)));
    let services = NodeServices {
        resources: Arc::new(NativeResources::default().with_local_log_clock(clock.clone(), false)),
        clock: clock.clone(),
        ..services()
    };
    let (node, driver) = running(config, services).await;
    for name in ["shop", "blog"] {
        node.database_or_attach(name.into(), DatabaseConfig::default()).await.unwrap().unwrap();
    }
    let metadata = |key: &str| AutonomousTxOptions {
        user_metadata: Some(rtdb_tx::UserMetadata::new(
            Value::Struct(StructFields::from([("idempotency_key".into(), Value::Utf8(key.into()))])),
            node.memory(),
        ).unwrap()),
        ..AutonomousTxOptions::default()
    };
    let mut shop = node.session("shop", Arc::new(Authority)).unwrap();
    let mut blog = node.session("blog", Arc::new(Authority)).unwrap();
    let shop_original = shop.state_mut().execute_tx(vec![put(&node, 1, 10)], metadata("order-1")).await.unwrap();
    let blog_original = blog.state_mut().execute_tx(vec![put(&node, 1, 20)], metadata("article-1")).await.unwrap();
    assert!(shop_original.committed() && blog_original.committed());

    let shop_retry = shop.state_mut().submit_tx(vec![put(&node, 2, 11)], metadata("order-1")).await.unwrap();
    let blog_retry = blog.state_mut().submit_tx(vec![put(&node, 2, 21)], metadata("article-1")).await.unwrap();
    assert_eq!(shop_retry.tx_id, blog_retry.tx_id);
    let cancellation = CancellationToken::new();
    for (name, retry, original) in [
        ("shop", shop_retry.tx_id, &shop_original),
        ("blog", blog_retry.tx_id, &blog_original),
        ("shop", shop_retry.tx_id, &shop_original),
    ] {
        let result = node.await_transaction(name, retry, &Authority, &cancellation).await.unwrap();
        assert!(result.committed(), "{name}: {result:?}");
        assert_eq!(result.tx_key(), original.tx_key());
    }
    for (session, value) in [(&mut shop, 10), (&mut blog, 20)] {
        assert_eq!(rows(session.query_sql("SELECT value FROM docs", vec![]).await.unwrap()).await, [vec![Value::I64(value)]]);
    }
    clock.0.fetch_add(Duration::from_secs(3601).as_micros() as i64, Ordering::Relaxed);
    let reused = shop.state_mut().execute_tx(vec![put(&node, 3, 30)], metadata("order-1")).await.unwrap();
    assert!(reused.committed());
    assert!(reused.tx_key().tx_id > shop_retry.tx_id);
    let delayed = node.await_transaction("shop", shop_retry.tx_id, &Authority, &cancellation).await.unwrap();
    assert!(delayed.committed());
    assert_eq!(delayed.tx_key(), shop_original.tx_key());
    drop((shop, blog));
    stop(&node, driver).await;
}

#[tokio::test]
async fn a_keyed_transaction_rejected_before_its_duplicate_guard_keeps_its_error() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let metadata = rtdb_tx::UserMetadata::new(Value::Struct(StructFields::from([
        ("idempotency_key".into(), Value::Utf8("order-1".into())),
    ])), node.memory()).unwrap();
    let original = session.state_mut().execute_tx(vec![put(&node, 1, 10)], AutonomousTxOptions {
        user_metadata: Some(metadata.clone()),
        ..AutonomousTxOptions::default()
    }).await.unwrap();
    assert!(original.committed());
    let retry = session.state_mut().submit_tx(vec![put(&node, 2, 20)], AutonomousTxOptions {
        system_time: Some(original.tx_key().system_time),
        user_metadata: Some(metadata),
        ..AutonomousTxOptions::default()
    }).await.unwrap();
    let cancellation = CancellationToken::new();
    let result = node.await_transaction("xtdb", retry.tx_id, &Authority, &cancellation).await.unwrap();
    assert_eq!(result.error().map(|error| error.code.as_str()), Some("invalid-system-time"));
    assert!(session.state_mut().execute_tx(vec![put(&node, 3, 30)], AutonomousTxOptions::default()).await.unwrap().committed());
    let delayed = node.await_transaction("xtdb", retry.tx_id, &Authority, &cancellation).await.unwrap();
    assert_eq!(delayed.error().map(|error| error.code.as_str()), Some("invalid-system-time"));
    assert_eq!(delayed.tx_key(), result.tx_key());
    drop(session);
    stop(&node, driver).await;
}

#[tokio::test]
async fn a_command_resent_under_its_session_key_on_a_new_session_writes_nothing_again() {
    let (node, driver) = running(options(), services()).await;
    let command = |key: &'static str, ids: [i64; 2]| {
        let node = node.clone();
        async move {
            let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
            session.state_mut().set_parameter(rtdb_node::IDEMPOTENCY_KEY_PARAMETER, Some(key.into())).unwrap();
            for id in ids { assert!(session.state_mut().execute_tx(vec![put(&node, id, id)], AutonomousTxOptions::default()).await.unwrap().committed()); }
        }
    };
    command("first", [1, 2]).await;
    command("first", [3, 4]).await;
    command("second", [5, 6]).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    assert_eq!(rows(session.query_sql("SELECT _id FROM docs ORDER BY _id", vec![]).await.unwrap()).await, [[Value::I64(1)], [Value::I64(2)], [Value::I64(5)], [Value::I64(6)]].map(Vec::from),
        "the resent command's transactions take the keys the first run's did, in order, and run no more");
    drop(session);
    stop(&node, driver).await;
}

#[tokio::test]
async fn each_argument_row_of_a_batched_update_reads_what_the_rows_before_it_wrote() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (node, driver) = running(options(), services()).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        let insert = TxOp::Sql { sql: "INSERT INTO docs (_id, value) VALUES (?, ?)".into(), args: Some(SqlArguments::from_rows(vec![vec![Value::I64(1), Value::I64(10)], vec![Value::I64(2), Value::I64(20)]], node.memory()).unwrap()) };
        assert!(session.state_mut().execute_tx(vec![insert], AutonomousTxOptions::default()).await.unwrap().committed());
        let arguments = vec![vec![Value::I64(5), Value::I64(1)], vec![Value::I64(7), Value::I64(2)], vec![Value::I64(100), Value::I64(1)]];
        let update = TxOp::Sql { sql: "UPDATE docs SET added = ?, value = value + 1 WHERE _id = ?".into(), args: Some(SqlArguments::from_rows(arguments, node.memory()).unwrap()) };
        let updated = session.state_mut().execute_tx(vec![update], AutonomousTxOptions::default()).await.unwrap();
        assert!(updated.committed(), "{:?}", updated.error());
        assert_eq!(rows(session.query_sql("SELECT _id, value, added FROM docs ORDER BY _id", vec![]).await.unwrap()).await,
            vec![vec![Value::I64(1), Value::I64(12), Value::I64(100)], vec![Value::I64(2), Value::I64(21), Value::I64(7)]]);
        stop(&node, driver).await;
    }).await.unwrap();
}

fn put_rows(node: &Node, table: &str, documents: Vec<StructFields<Value>>, valid_from: Option<Timestamp>, valid_to: Option<Timestamp>) -> TxOp {
    TxOp::PutDocs { table: TableRef::new("public", table), documents: Documents::new(documents, node.memory()).unwrap(), valid_from, valid_to }
}
fn sql(node: &Node, sql: &str, arguments: Vec<Value>) -> TxOp {
    TxOp::Sql { sql: sql.into(), args: (!arguments.is_empty()).then(|| SqlArguments::from_rows(vec![arguments], node.memory()).unwrap()) }
}

#[tokio::test]
async fn a_set_based_update_rewrites_exactly_the_rows_whose_assigned_values_differ() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (node, driver) = running(options(), services()).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        let instant = |micros: i64| Value::Timestamp { value: micros, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) };
        let row = |id: i64, f: Value, t: Value, n: Value, ts: Value, d: Value| StructFields::from([("_id".into(), Value::I64(id)), ("f".into(), f), ("t".into(), t), ("n".into(), n), ("ts".into(), ts), ("d".into(), d)]);
        let documents = vec![
            row(1, Value::F64(1.5), Value::Utf8("a".into()), Value::I64(1), instant(1_000), Value::DateDay(18_000)),
            row(2, Value::F64(f64::NAN), Value::Null, Value::Null, Value::Null, Value::Null),
            row(3, Value::F64(0.0), Value::Utf8(String::new()), Value::I64(2), instant(-1), Value::DateDay(-1)),
            row(4, Value::Null, Value::Utf8("b".into()), Value::I64(3), instant(i64::MAX - 1), Value::DateDay(0)),
        ];
        assert!(session.state_mut().execute_tx(vec![put_rows(&node, "items", documents, None, None)], AutonomousTxOptions::default()).await.unwrap().committed());
        for (statement, arguments) in [
            // Only NaN differs from itself; a null equals a null.
            ("UPDATE items SET f = f", vec![]),
            // -0.0 equals 0.0, so the stored 0.0 keeps its sign.
            ("UPDATE items SET f = ? WHERE _id = 3", vec![Value::F64(-0.0)]),
            ("UPDATE items SET t = t, ts = ts, d = d", vec![]),
            ("UPDATE items SET n = n + 1", vec![]),
            ("UPDATE items SET f = f * 1.01 WHERE n < 100", vec![]),
        ] {
            let result = session.state_mut().execute_tx(vec![sql(&node, statement, arguments)], AutonomousTxOptions::default()).await.unwrap();
            assert!(result.committed(), "{statement}: {:?}", result.error());
        }
        let versions = rows(session.query_sql("SELECT _id, COUNT(*) AS versions FROM items FOR ALL SYSTEM_TIME GROUP BY _id ORDER BY _id", vec![]).await.unwrap()).await;
        assert_eq!(versions, [1, 2, 3, 4].into_iter().zip([3, 2, 2, 2]).map(|(id, count)| vec![Value::I64(id), Value::I64(count)]).collect::<Vec<_>>());
        let current = rows(session.query_sql("SELECT _id, f, t, n, ts, d FROM items ORDER BY _id", vec![]).await.unwrap()).await;
        let f = |row: usize| match current[row][1] { Value::F64(value) => Some(value.to_bits()), Value::Null => None, ref other => panic!("{other:?}") };
        assert_eq!([f(0), f(2), f(3)], [Some((1.5f64 * 1.01).to_bits()), Some(0.0f64.to_bits()), None]);
        assert!(f64::from_bits(f(1).unwrap()).is_nan());
        assert_eq!(current.iter().map(|row| row[3].clone()).collect::<Vec<_>>(), [Value::I64(2), Value::Null, Value::I64(3), Value::I64(4)]);
        assert_eq!(current.iter().map(|row| (row[2].clone(), row[4].clone(), row[5].clone())).collect::<Vec<_>>(), [
            (Value::Utf8("a".into()), instant(1_000), Value::DateDay(18_000)), (Value::Null, Value::Null, Value::Null),
            (Value::Utf8(String::new()), instant(-1), Value::DateDay(-1)), (Value::Utf8("b".into()), instant(i64::MAX - 1), Value::DateDay(0)),
        ]);
        stop(&node, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn an_updated_row_is_valid_from_the_transaction_time_to_the_end_of_the_version_it_replaces() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (node, driver) = running(options(), services()).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        let instant = |time: Timestamp| Value::Timestamp { value: time.0, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) };
        let row = |id: i64, value: i64| StructFields::from([("_id".into(), Value::I64(id)), ("v".into(), Value::I64(value))]);
        let (start, end) = (Timestamp(500), Timestamp(10_000_000_000));
        let bounded = session.state_mut().execute_tx(vec![put_rows(&node, "spans", vec![row(1, 10)], Some(start), Some(end))], AutonomousTxOptions::default()).await.unwrap();
        let open = session.state_mut().execute_tx(vec![put_rows(&node, "spans", vec![row(2, 20)], None, None)], AutonomousTxOptions::default()).await.unwrap();
        assert!(bounded.committed() && open.committed());
        let updated = session.state_mut().execute_tx(vec![sql(&node, "UPDATE spans SET v = v + 1", vec![])], AutonomousTxOptions::default()).await.unwrap();
        assert!(updated.committed(), "{:?}", updated.error());
        let now = updated.tx_key().system_time;
        let history = rows(session.query_sql("SELECT _id, v, _valid_from, _valid_to FROM spans FOR ALL VALID_TIME ORDER BY _id, _valid_from", vec![]).await.unwrap()).await;
        assert_eq!(history, vec![
            vec![Value::I64(1), Value::I64(10), instant(start), instant(now)],
            vec![Value::I64(1), Value::I64(11), instant(now), instant(end)],
            vec![Value::I64(2), Value::I64(20), instant(open.tx_key().system_time), instant(now)],
            vec![Value::I64(2), Value::I64(21), instant(now), Value::Null],
        ]);
        stop(&node, driver).await;
    }).await.unwrap();
}

#[derive(Default)]
struct OrderedIndexReads(std::sync::Mutex<Vec<Option<usize>>>);

#[async_trait]
impl rtdb_query::ScanAccelerator for OrderedIndexReads {
    async fn candidate_iids(&self, request: &rtdb_query::ScanRequest<'_>) -> Result<Option<BTreeSet<rtdb_types::Iid>>> {
        if request.scan.table.name.table == TableRef::new("public", "orders") {
            self.0.lock().unwrap().push(request.scan.iids.as_ref().map(BTreeSet::len));
        }
        Ok(None)
    }
}

impl OrderedIndexReads {
    fn take(&self) -> Vec<Option<usize>> { std::mem::take(&mut *self.0.lock().unwrap()) }
}

async fn index_sql(session: &mut NativeSession, statement: &str) -> rtdb_types::Timestamp {
    let result = session.state_mut().execute_tx(
        vec![TxOp::Sql { sql: statement.into(), args: None }], AutonomousTxOptions::default(),
    ).await.unwrap();
    assert!(result.committed(), "{statement}: {result:?}");
    result.tx_key().system_time
}

async fn index_flush(node: &Node, database: &Database) {
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
}

#[tokio::test]
async fn ordered_index_declaration_if_not_exists_preserves_the_definition() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    index_sql(&mut session, "CREATE INDEX IF NOT EXISTS orders_key ON orders (priority, _id)").await;
    let query = "SELECT columns, version FROM xt.ordered_indexes WHERE _id = 'orders_key'";
    let original = rows(session.query_sql(query, vec![]).await.unwrap()).await;
    assert_eq!(original.len(), 1);
    index_sql(&mut session, "CREATE INDEX IF NOT EXISTS orders_key ON orders (_id)").await;
    assert_eq!(rows(session.query_sql(query, vec![]).await.unwrap()).await, original);
    stop(&node, driver).await;
}

#[tokio::test]
async fn ordered_index_publishes_its_own_roots_and_seeks_both_directions_at_a_basis() {
    let observer = Arc::new(OrderedIndexReads::default());
    let mut services = services();
    services.scan_accelerators.push(observer.clone());
    let mut config = options();
    config.automatic_artefacts = false;
    let (node, driver) = running(config, services).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    index_sql(&mut session, "CREATE INDEX orders_by_customer ON orders (customer, priority, _id)").await;

    index_sql(&mut session, "INSERT INTO orders RECORDS {_id: 1, customer: 'a', priority: 1}, {_id: 2, customer: 'a', priority: 2}, {_id: 3, customer: 'b', priority: 0}").await;
    index_flush(&node, &database).await;
    database.build_artefacts().await.unwrap();
    let past = index_sql(&mut session, "INSERT INTO orders RECORDS {_id: 4, customer: 'a', priority: 2}, {_id: 5, customer: 'a', priority: 3}, {_id: 6, customer: 'a', priority: 5}").await;
    index_flush(&node, &database).await;
    database.build_artefacts().await.unwrap();

    let root = rtdb_storage::ObjectKey::new("ordered-index/v2").unwrap();
    let paths = database.object_store().list(Some(&root)).await.unwrap();
    let manifests: Vec<_> = paths.iter().filter(|entry| entry.key.as_str().contains("/manifests/")).collect();
    assert_eq!(manifests.len(), 1);
    let manifest: serde_json::Value = serde_json::from_slice(&database.object_store().get(&manifests[0].key).await.unwrap()).unwrap();
    assert_eq!(manifest["layout"]["source_rows"], 6);
    assert!(manifest["layout"]["revisions"].is_object());
    assert!(database.snapshot().trie_entries(&TableRef::new("public", "orders")).iter().all(|trie| trie.key.level() == 0));

    let warm = "SELECT _id FROM orders WHERE customer = 'a' ORDER BY priority, _id LIMIT 1";
    assert_eq!(rows(session.query_sql(warm, vec![]).await.unwrap()).await, [vec![Value::I64(1)]]);
    assert!(observer.take().is_empty());

    index_sql(&mut session, "UPDATE orders SET priority = 9 WHERE _id = 1").await;
    index_sql(&mut session, "DELETE FROM orders WHERE _id = 2").await;
    index_sql(&mut session, "INSERT INTO orders RECORDS {_id: 7, customer: 'a', priority: 2}").await;
    let current = "SELECT _id FROM orders WHERE customer = 'a' AND priority >= 2 ORDER BY priority, _id LIMIT 2";
    observer.take();
    assert_eq!(rows(session.query_sql(current, vec![]).await.unwrap()).await, [vec![Value::I64(4)], vec![Value::I64(7)]]);
    assert!(observer.take().is_empty());

    let next = "SELECT _id FROM orders WHERE customer = 'a' AND (priority > 2 OR (priority = 2 AND _id > 4)) ORDER BY priority, _id LIMIT 2";
    assert_eq!(rows(session.query_sql(next, vec![]).await.unwrap()).await, [vec![Value::I64(7)], vec![Value::I64(5)]]);
    assert!(observer.take().is_empty());

    let offset = "SELECT _id FROM orders WHERE customer = 'a' ORDER BY priority, _id LIMIT 2 OFFSET 1";
    assert_eq!(rows(session.query_sql(offset, vec![]).await.unwrap()).await, [vec![Value::I64(7)], vec![Value::I64(5)]]);
    assert!(observer.take().is_empty());

    let reverse = "SELECT _id FROM orders WHERE customer = 'a' ORDER BY priority DESC, _id DESC LIMIT 2";
    assert_eq!(rows(session.query_sql(reverse, vec![]).await.unwrap()).await, [vec![Value::I64(1)], vec![Value::I64(6)]]);
    assert!(observer.take().is_empty());

    let old = "SELECT _id FROM orders FOR SYSTEM_TIME AS OF ? WHERE customer = 'a' ORDER BY priority, _id LIMIT 2";
    let at = Value::Timestamp { value: past.0, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) };
    assert_eq!(rows(session.query_sql(old, vec![at]).await.unwrap()).await, [vec![Value::I64(1)], vec![Value::I64(2)]]);
    assert!(observer.take().is_empty());

    stop(&node, driver).await;
}

#[tokio::test]
async fn ordered_index_limits_distinct_visible_keys_in_both_directions() {
    let observer = Arc::new(OrderedIndexReads::default());
    let mut services = services();
    services.scan_accelerators.push(observer.clone());
    let mut config = options();
    config.automatic_artefacts = false;
    let (node, driver) = running(config, services).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();

    let index = "CREATE INDEX IF NOT EXISTS orders_candidates ON orders (world, key, label)";
    index_sql(&mut session, index).await;
    index_sql(&mut session, index).await;
    index_sql(&mut session, "INSERT INTO orders RECORDS
        {_id: 1, world: 4, key: 'a', label: 'hidden'},
        {_id: 2, world: 4, key: 'b', label: 'public'},
        {_id: 3, world: 4, key: 'b', label: 'low'},
        {_id: 4, world: 4, key: 'c', label: 'low'},
        {_id: 5, world: 4, key: 'c', label: 'public'},
        {_id: 6, world: 8, key: 'd', label: 'public'}").await;
    index_flush(&node, &database).await;
    database.build_artefacts().await.unwrap();

    for (direction, expected) in [("ASC", ["b", "c"]), ("DESC", ["c", "b"])] {
        let query = format!("SELECT DISTINCT key FROM orders WHERE world = 4
            AND (label = 'low' OR label = 'public') ORDER BY key {direction} LIMIT 2");
        assert_eq!(rows(session.query_sql(&query, vec![]).await.unwrap()).await,
            expected.map(|key| vec![Value::Utf8(key.into())]));
        assert!(observer.take().is_empty(), "the ordered index answered without opening a candidate scan");
    }

    let query = "SELECT DISTINCT key FROM orders WHERE world = 4 AND key > 'b'
        AND (label = 'low' OR label = 'public') ORDER BY key LIMIT 1";
    assert_eq!(rows(session.query_sql(query, vec![]).await.unwrap()).await, [vec![Value::Utf8("c".into())]]);
    assert!(observer.take().is_empty(), "the ordered index answered the continuation");
    stop(&node, driver).await;
}

#[tokio::test]
async fn ordered_index_retires_erased_keys_and_drop_recreate_gets_a_fresh_generation() {
    let mut config = options();
    config.automatic_artefacts = false;
    let (node, driver) = running(config, services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    index_sql(&mut session, "CREATE INDEX orders_key ON orders (customer, _id)").await;
    let original = index_sql(&mut session, "INSERT INTO orders RECORDS {_id: 1, customer: 'erased-index-value'}, {_id: 2, customer: 'retained'}").await;
    index_flush(&node, &database).await;
    database.build_artefacts().await.unwrap();
    let prefix = rtdb_storage::ObjectKey::new("ordered-index/v2").unwrap();
    let store = database.object_store();
    let mut erased_pages = Vec::new();
    for entry in store.list(Some(&prefix)).await.unwrap() {
        if entry.key.as_str().contains("/pages/") && String::from_utf8_lossy(&store.get(&entry.key).await.unwrap()).contains("erased-index-value") {
            erased_pages.push(entry.key);
        }
    }
    assert!(!erased_pages.is_empty());
    assert!(erased_pages.iter().all(|page| page.as_str().contains("/generations/0000000000000000/")));

    let erasure = session.state_mut().execute_tx(
        ["INSERT INTO orders RECORDS {_id: 1, customer: 'erased-in-revision'}",
         "ERASE FROM orders WHERE _id = 1",
         "INSERT INTO orders RECORDS {_id: 1, customer: 'z-reinserted'}"]
            .into_iter().map(|sql| TxOp::Sql { sql: sql.into(), args: None }).collect(),
        AutonomousTxOptions::default(),
    ).await.unwrap();
    assert!(erasure.committed());
    index_flush(&node, &database).await;
    database.build_artefacts().await.unwrap();
    let objects = database.object_store().list(Some(&prefix)).await.unwrap();
    assert!(objects.iter().any(|entry| entry.key.as_str().contains("/pages/")));
    for entry in &objects {
        let bytes = database.object_store().get(&entry.key).await.unwrap();
        assert!(!String::from_utf8_lossy(&bytes).contains("erased-index-value"));
        assert!(!String::from_utf8_lossy(&bytes).contains("erased-in-revision"));
    }
    let manifest = objects.iter().find(|entry| entry.key.as_str().contains("/manifests/")).unwrap();
    let encoded = store.get(&manifest.key).await.unwrap();
    let obsolete = rtdb_storage::ObjectKey::new(manifest.key.as_str().replace("/manifests/scoped/", "/manifests/scrubbed/")).unwrap();
    store.put(&obsolete, Vec::from("obsolete replay layout").into()).await.unwrap();
    let checkpoint: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    let scope = checkpoint["layout"]["scope"].as_u64().unwrap();
    assert!(scope > 0);
    let generation_dir = format!("/generations/{scope:016x}/pages/");
    assert!(objects.iter().filter(|entry| entry.key.as_str().contains("/pages/")).all(|entry| entry.key.as_str().contains(&generation_dir)));
    let root = &checkpoint["layout"]["revisions"];
    let generation = root["generation"].as_i64().unwrap();
    let digest = root["digest"].as_str().unwrap();
    let root_suffix = format!("/pages/{generation:016x}/{digest}");
    let current_page = objects.iter().find(|entry| entry.key.as_str().ends_with(&root_suffix)).unwrap();
    store.delete(&current_page.key).await.unwrap();
    for page in &erased_pages {
        store.put(page, Vec::from("reserved erased page address").into()).await.unwrap();
    }
    database.build_artefacts().await.unwrap();
    assert_eq!(store.get(&manifest.key).await.unwrap(), encoded);
    assert!(matches!(store.head(&obsolete).await, Err(rtdb_storage::Error::MissingObject(_))));
    store.head(&current_page.key).await.unwrap();
    for page in &erased_pages {
        assert!(matches!(store.head(page).await, Err(rtdb_storage::Error::MissingObject(_))));
    }
    let query = "SELECT _id FROM orders WHERE customer >= 'a' ORDER BY customer, _id LIMIT 2";
    let retained = [vec![Value::I64(2)], vec![Value::I64(1)]];
    assert_eq!(rows(session.query_sql(query, vec![]).await.unwrap()).await, retained);
    let history = "SELECT _id FROM orders FOR SYSTEM_TIME AS OF ? WHERE customer >= 'a' ORDER BY customer, _id LIMIT 2";
    let at = Value::Timestamp { value: original.0, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) };
    assert_eq!(rows(session.query_sql(history, vec![at]).await.unwrap()).await, [vec![Value::I64(2)]]);

    index_sql(&mut session, "DROP INDEX orders_key").await;
    database.build_artefacts().await.unwrap();
    assert!(database.object_store().list(Some(&prefix)).await.unwrap().is_empty());
    index_sql(&mut session, "CREATE INDEX orders_key ON orders (customer, _id)").await;
    index_flush(&node, &database).await;
    database.build_artefacts().await.unwrap();
    assert_eq!(rows(session.query_sql(query, vec![]).await.unwrap()).await, retained);
    for entry in database.object_store().list(Some(&prefix)).await.unwrap() {
        let bytes = database.object_store().get(&entry.key).await.unwrap();
        assert!(!String::from_utf8_lossy(&bytes).contains("erased-index-value"));
    }
    stop(&node, driver).await;
}

#[tokio::test]
async fn ordered_index_reopens_its_manifest_and_covers_flushed_lag() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = options();
    config.automatic_artefacts = false;
    config.membership = Membership::Durable { primary: local_database(directory.path()), skipped: BTreeSet::new() };
    let mut template = services();
    let observer = Arc::new(OrderedIndexReads::default());
    template.scan_accelerators.push(observer.clone());

    let (node, driver) = running(config.clone(), fresh_local_services(&template)).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    index_sql(&mut session, "CREATE INDEX orders_key ON orders (priority, _id)").await;
    index_sql(&mut session, "INSERT INTO orders RECORDS {_id: 1, priority: 5}, {_id: 2, priority: 7}").await;
    index_flush(&node, &database).await;
    database.build_artefacts().await.unwrap();
    index_sql(&mut session, "INSERT INTO orders RECORDS {_id: 3, priority: 6}").await;
    index_sql(&mut session, "UPDATE orders SET priority = 10 WHERE _id = 2").await;
    index_sql(&mut session, "DELETE FROM orders WHERE _id = 3").await;
    index_sql(&mut session, "INSERT INTO orders RECORDS {_id: 4, priority: 8}").await;
    index_flush(&node, &database).await;
    drop(session);
    drop(database);
    stop(&node, driver).await;
    drop(node);

    let (node, driver) = running(config, fresh_local_services(&template)).await;
    node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    observer.take();
    let query = "SELECT _id FROM orders WHERE priority > 5 ORDER BY priority, _id LIMIT 1";
    assert_eq!(rows(session.query_sql(query, vec![]).await.unwrap()).await, [vec![Value::I64(4)]]);
    assert!(observer.take().is_empty());
    stop(&node, driver).await;
}

#[tokio::test]
async fn ordered_index_serves_later_pages_after_independent_data_compaction() {
    let observer = Arc::new(OrderedIndexReads::default());
    let mut template = services();
    template.scan_accelerators.push(observer.clone());
    let mut config = options();
    config.automatic_artefacts = false;
    let (node, driver) = running(config, template).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    index_sql(&mut session, "CREATE INDEX orders_key ON orders (priority, _id)").await;
    let records = (0..540).map(|id| format!("{{_id: {id}, priority: {id}}}")).collect::<Vec<_>>().join(", ");
    index_sql(&mut session, &format!("INSERT INTO orders RECORDS {records}")).await;
    index_flush(&node, &database).await;
    database.build_artefacts().await.unwrap();

    let root = rtdb_storage::ObjectKey::new("ordered-index/v2").unwrap();
    let paths = database.object_store().list(Some(&root)).await.unwrap();
    let manifest = paths.iter().find(|entry| entry.key.as_str().contains("/manifests/")).unwrap();
    let encoded = database.object_store().get(&manifest.key).await.unwrap();
    assert!(paths.iter().filter(|entry| entry.key.as_str().contains("/pages/")).count() > 3);
    database.compaction().unwrap().compact_all().await.unwrap();
    assert_eq!(database.object_store().get(&manifest.key).await.unwrap(), encoded);

    let query = "SELECT _id FROM orders WHERE priority >= 520 ORDER BY priority, _id LIMIT 2";
    assert_eq!(rows(session.query_sql(query, vec![]).await.unwrap()).await, [vec![Value::I64(520)], vec![Value::I64(521)]]);
    assert!(observer.take().is_empty());

    let reverse = "SELECT _id FROM orders WHERE priority < 520 ORDER BY priority DESC, _id DESC LIMIT 2";
    assert_eq!(rows(session.query_sql(reverse, vec![]).await.unwrap()).await, [vec![Value::I64(519)], vec![Value::I64(518)]]);
    assert!(observer.take().is_empty());

    let gap = "SELECT _id FROM orders WHERE priority > 519 AND priority < 520 ORDER BY priority, _id LIMIT 1";
    assert!(rows(session.query_sql(gap, vec![]).await.unwrap()).await.is_empty());
    assert!(observer.take().is_empty());
    stop(&node, driver).await;
}


#[tokio::test]
async fn rebuilding_a_missing_ordered_root_preserves_the_incremental_manifest() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = options();
    config.automatic_artefacts = false;
    config.membership = Membership::Durable { primary: local_database(directory.path()), skipped: BTreeSet::new() };
    let mut template = services();
    let observer = Arc::new(OrderedIndexReads::default());
    template.scan_accelerators.push(observer.clone());

    let (node, driver) = running(config.clone(), fresh_local_services(&template)).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    index_sql(&mut session, "CREATE INDEX orders_key ON orders (priority, _id)").await;
    index_sql(&mut session, "INSERT INTO orders RECORDS {_id: 1, priority: 2}").await;
    index_flush(&node, &database).await;
    database.build_artefacts().await.unwrap();
    index_sql(&mut session, "INSERT INTO orders RECORDS {_id: 2, priority: 1}").await;
    index_flush(&node, &database).await;
    database.build_artefacts().await.unwrap();
    let prefix = rtdb_storage::ObjectKey::new("ordered-index/v2").unwrap();
    let objects = database.object_store().list(Some(&prefix)).await.unwrap();
    let manifest = objects.iter().find(|entry| entry.key.as_str().contains("/manifests/")).unwrap();
    let manifest_key = manifest.key.clone();
    let manifest_bytes = database.object_store().get(&manifest_key).await.unwrap();
    let checkpoint: serde_json::Value = serde_json::from_slice(&manifest_bytes).unwrap();
    let root = &checkpoint["layout"]["revisions"];
    let generation = root["generation"].as_i64().unwrap();
    let digest = root["digest"].as_str().unwrap();
    let page_suffix = format!("/pages/{generation:016x}/{digest}");
    let page = objects.iter().find(|object| object.key.as_str().ends_with(&page_suffix)).unwrap();
    database.object_store().delete(&page.key).await.unwrap();
    drop(session);
    drop(database);
    stop(&node, driver).await;
    drop(node);

    let (node, driver) = running(config, fresh_local_services(&template)).await;
    node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    let query = "SELECT _id FROM orders WHERE priority >= 1 ORDER BY priority, _id LIMIT 1";
    observer.take();
    assert_eq!(rows(session.query_sql(query, vec![]).await.unwrap()).await, [vec![Value::I64(2)]]);
    assert_eq!(observer.take(), [None]);

    database.build_artefacts().await.unwrap();
    assert_eq!(database.object_store().get(&manifest_key).await.unwrap(), manifest_bytes);
    assert!(database.object_store().head(&page.key).await.is_ok());
    assert_eq!(rows(session.query_sql(query, vec![]).await.unwrap()).await, [vec![Value::I64(2)]]);
    assert!(observer.take().is_empty());
    stop(&node, driver).await;
}

#[tokio::test]
async fn ordered_coverage_uses_only_adopted_l0_files() {
    let mut config = options();
    config.automatic_artefacts = false;
    let observer = Arc::new(OrderedIndexReads::default());
    let mut services = services();
    services.scan_accelerators.push(observer.clone());
    let (node, driver) = running(config, services).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    index_sql(&mut session, "CREATE INDEX orders_key ON orders (priority, _id)").await;
    index_sql(&mut session, "INSERT INTO orders RECORDS {_id: 1, priority: 9}").await;
    index_flush(&node, &database).await;

    let snapshot = database.snapshot();
    let table = TableRef::new("public", "orders");
    let entry = snapshot.table_entry(&table).unwrap();
    let trie = rtdb_format::keys::TrieKey::l0(snapshot.trie_catalog().l0_max_block_index(&table)).unwrap();
    let canonical = entry.slug.data_key(&trie);
    let canonical = rtdb_storage::ObjectKey::new(canonical).unwrap();
    index_sql(&mut session, "INSERT INTO unrelated RECORDS {_id: 1}").await;
    index_flush(&node, &database).await;
    let gap = database.snapshot().trie_catalog().l0_max_block_index(&TableRef::new("public", "unrelated"));
    assert!(gap > trie.block_index());
    let unadopted = rtdb_format::keys::TrieKey::l0(gap).unwrap();
    let copy = rtdb_storage::ObjectKey::new(entry.slug.data_key(&unadopted)).unwrap();
    let store = database.object_store();
    store.put(&copy, store.get(&canonical).await.unwrap()).await.unwrap();
    index_sql(&mut session, "INSERT INTO orders RECORDS {_id: 2, priority: 2}").await;
    index_flush(&node, &database).await;

    database.build_artefacts().await.unwrap();
    let query = "SELECT _id FROM orders ORDER BY priority, _id LIMIT 1";
    observer.take();
    assert_eq!(rows(session.query_sql(query, vec![]).await.unwrap()).await, [vec![Value::I64(2)]]);
    assert!(observer.take().is_empty());
    stop(&node, driver).await;
}

#[tokio::test]
async fn cosine_distance_ranks_the_versions_that_pass_the_filter_at_each_valid_point() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let embedding = |x, y| Value::FixedSizeList(vec![Value::F32(x), Value::F32(y)].into());
    let document = |id, status: &str, vector| StructFields::from([
        ("_id".into(), Value::I64(id)),
        ("status".into(), Value::Utf8(status.into())),
        ("embedding".into(), vector),
    ]);
    let initial = put_rows(&node, "vectors", vec![
        document(1, "open", embedding(1.0, 0.0)),
        document(2, "open", embedding(0.0, 1.0)),
        document(3, "closed", embedding(1.0, 0.0)),
    ], Some(Timestamp(0)), None);
    assert!(session.state_mut().execute_tx(vec![initial], AutonomousTxOptions::default()).await.unwrap().committed());

    let correction = put_rows(&node, "vectors", vec![
        document(1, "open", embedding(-1.0, 0.0)),
    ], Some(Timestamp(2_000_000)), Some(Timestamp(3_000_000)));
    assert!(session.state_mut().execute_tx(vec![correction], AutonomousTxOptions::default()).await.unwrap().committed());

    for (second, expected) in [(1, 1), (2, 2), (3, 1)] {
        let query = format!("SELECT _id FROM vectors FOR VALID_TIME AS OF TIMESTAMP '1970-01-01T00:00:0{second}Z' WHERE status = 'open' ORDER BY COSINE_DISTANCE(embedding, $1), _id LIMIT 1");
        let result = session.query_sql(&query, vec![embedding(1.0, 0.0)]).await.unwrap();
        assert_eq!(rows(result).await, [vec![Value::I64(expected)]]);
    }
    stop(&node, driver).await;
}
