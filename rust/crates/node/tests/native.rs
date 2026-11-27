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
use rtdb_tx::{Documents, EntityIds, SqlArguments, TransactionAuthorization, TxOp};
use rtdb_types::{Anomaly, EntityId, OwnedIdentityScan, Result, TableRef, Timestamp, TxBasis, TxId, Value};
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

#[tokio::test]
async fn an_implicit_basis_checks_the_replica_index_before_capturing_applied_history() {
    use rtdb_format::{envelope::{ReplicaBody, ReplicaEnvelope}, log::proto};
    use rtdb_storage::log::{Log, LogOffset, MemoryLog};
    use rtdb_types::{StorageEpoch, Term, TermSequence};

    struct ReadIndex(Arc<Semaphore>);
    #[async_trait]
    impl ReplicaReadIndex for ReadIndex {
        async fn confirm(&self) -> Result<LogOffset> {
            self.0.acquire().await.unwrap().forget();
            Ok(LogOffset(1))
        }
    }

    struct Resources {
        native: NativeResources,
        replica: Arc<MemoryLog>,
        confirm: Arc<Semaphore>,
    }
    #[async_trait]
    impl ResourceFactory for Resources {
        fn validate(&self, name: &str, config: &DatabaseConfig) -> Result<()> { self.native.validate(name, config) }
        async fn open_storage(&self, name: &str, config: &DatabaseConfig) -> Result<DatabaseStorage> {
            self.native.open_storage(name, config).await
        }
        async fn open_logs(&self, _: &str, _: &DatabaseConfig, _: LogAccess, _: CancellationToken, _: &DatabaseStorage) -> Result<OpenedLogs> {
            Ok(OpenedLogs {
                source: Arc::new(MemoryLog::new(1024)), replica: self.replica.clone(), source_epoch: StorageEpoch(0), replica_epoch: StorageEpoch(0),
                replica_read_index: Some(Box::new(ReadIndex(self.confirm.clone()))), placed_transfer: None, await_authority: rtdb_node::AwaitAuthority::SourcePosition, drivers: Vec::new(),
            })
        }
    }

    let confirm = Arc::new(Semaphore::new(1));
    let replica = Arc::new(MemoryLog::new(1024));
    let providers = Arc::new(Resources { native: NativeResources::default(), replica: replica.clone(), confirm: confirm.clone() });
    let mut config = options();
    config.membership = Membership::Placed;
    let (node, driver) = running(config, NodeServices { resources: providers, ..services() }).await;
    let database = node.database_or_attach("shop".into(), DatabaseConfig { mode: Some(1), ..Default::default() }).await.unwrap().unwrap();
    let mut session = node.session("shop", Arc::new(Authority)).unwrap();
    assert_eq!(session.state_mut().read_basis().await.unwrap_err().code, REPLICA_BEHIND);

    session.state_mut().begin().unwrap();
    let mut pending = Box::pin(session.state_mut().read_basis());
    assert!(futures::poll!(&mut pending).is_pending());
    let system_time = Timestamp(100);
    for (sequence, message) in [
        ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() }),
        ReplicaBody::ResolvedTx(proto::ResolvedTx {
            tx_id: Some(0), system_time_micros: Some(system_time.0), committed: Some(true), src_msg_id: Some(0), ..Default::default()
        }),
    ].into_iter().enumerate() {
        let envelope = ReplicaEnvelope { term: Term(1), term_sequence: Some(TermSequence(sequence as i64)), message };
        replica.enqueue(envelope.encode().into(), system_time).await.unwrap().durable().await.unwrap();
    }
    let mut applied = database.watch_status();
    tokio::time::timeout(Duration::from_secs(5), applied.wait_for(|status| status.installed.map(|prefix| prefix.replica) == Some(rtdb_types::ReplicaPosition(1)))).await.unwrap().unwrap();
    confirm.add_permits(1);
    let basis = tokio::time::timeout(Duration::from_secs(5), pending).await.unwrap().unwrap();
    assert_eq!(basis.snapshot.databases["shop"], [Some(system_time.into())]);
    assert_eq!(session.state_mut().read_basis().await.unwrap(), basis);
    stop(&node, driver).await;
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
            snapshot: node.snapshot_basis().unwrap(), replica_capture: None, resume_token: None, has_await_floor: false, snapshot_time: None, current_time: node.query_instant(), timezone: "UTC".into(),
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
async fn waiting_for_a_local_detach_resolves_when_its_teardown_finishes() {
    let mut config = options();
    config.membership = Membership::Placed;
    let (node, driver) = running(config, services()).await;
    tokio::time::timeout(Duration::from_secs(5), node.catalog().detached("shop")).await.unwrap();
    node.database_or_attach("shop".into(), Default::default()).await.unwrap().unwrap();

    node.detach_local("shop".into()).await.unwrap();
    let catalog = Arc::clone(node.catalog());
    let waiter = tokio::spawn(async move { catalog.detached("shop").await });
    tokio::time::timeout(Duration::from_secs(5), waiter).await.unwrap().unwrap();
    assert!(!node.catalog().is_draining("shop"));
    node.database_or_attach("shop".into(), Default::default()).await.unwrap().unwrap();
    stop(&node, driver).await;
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
async fn owned_native_relations_are_scoped_to_the_durable_principal() {
    use rtdb_types::{EntityId, OwnedRelation};

    struct OtherOwner;
    #[async_trait]
    impl SessionIdentity for OtherOwner {
        fn user(&self) -> Option<&str> { Some("another-owner") }
        async fn revalidate(&self, _: Timestamp) -> Result<()> { Ok(()) }
        fn authorize_database(&self, _: &str, _: DatabaseAccess) -> Result<()> { Ok(()) }
    }

    let mut settings = options();
    settings.owned_relations = true;
    let (node, driver) = running(settings, services()).await;
    let mut owner = node.session("xtdb", Arc::new(Authority)).unwrap();
    let mut other = node.session("xtdb", Arc::new(OtherOwner)).unwrap();
    let relation = OwnedRelation::new("orders").unwrap();
    let insert = |value| TxOp::OwnedInsertDocs {
        relation: relation.clone(),
        documents: Documents::new(vec![StructFields::from([
            ("_id".into(), Value::I64(1)), ("value".into(), Value::I64(value)),
        ])], node.memory()).unwrap(),
        valid_from: None, valid_to: None,
    };
    let options = AutonomousTxOptions::default();
    let first = owner.state_mut().execute_tx(vec![insert(10)], options.clone()).await.unwrap();
    assert!(first.committed(), "{:?}", first.error());
    let id = EntityId::Integer(1);
    assert_eq!(rows(owner.open_owned_read().await.unwrap().open_point(&relation, &id).await.unwrap().unwrap()).await[0][..2], [Value::I64(1), Value::I64(10)]);
    assert!(other.open_owned_read().await.unwrap().open_point(&relation, &id).await.unwrap().is_none());

    let physical = relation.table("xtdb");
    let raw = TxOp::PutDocs {
        table: physical.clone(),
        documents: Documents::new(vec![StructFields::from([("_id".into(), Value::I64(2))])], node.memory()).unwrap(),
        valid_from: None, valid_to: None,
    };
    assert_eq!(owner.state_mut().execute_tx(vec![raw], options.clone()).await.unwrap_err().code, "xtdb/forbidden-table");
    let name = format!(r#""{}"."{}""#, physical.schema, physical.table);
    assert_eq!(owner.query_sql(&format!("SELECT * FROM {name}"), vec![]).await.err().unwrap().code, "xtdb/table-not-found");
    assert!(rows(owner.query_sql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'rtdb_owned_v1'", vec![]).await.unwrap()).await.is_empty());
    let ddl = TxOp::Sql { sql: format!("CREATE TABLE {name} (value BIGINT)"), args: None };
    let rejected = owner.state_mut().execute_tx(vec![ddl], options.clone()).await.unwrap();
    assert_eq!(rejected.error().unwrap().code, "xtdb/forbidden-table");

    let mixed = owner.state_mut().execute_tx(vec![put(&node, 1, 7), insert(11)], options.clone()).await.unwrap();
    assert_eq!(mixed.error().unwrap().code, "rtdb.owned/mixed-transaction");
    assert!(owner.query_sql("SELECT * FROM docs", vec![]).await.is_err());
    let second = other.state_mut().execute_tx(vec![insert(12)], options).await.unwrap();
    assert!(second.committed(), "{:?}", second.error());
    assert_eq!(rows(other.open_owned_read().await.unwrap().open_point(&relation, &id).await.unwrap().unwrap()).await[0][..2], [Value::I64(1), Value::I64(12)]);
    drop(other);
    drop(owner);
    stop(&node, driver).await;
}

#[tokio::test]
async fn held_owned_points_share_one_cut_and_close_refuses_new_reads() {
    use rtdb_types::{EntityId, OwnedRelation};

    let mut settings = options();
    settings.owned_relations = true;
    let (node, driver) = running(settings, services()).await;
    let mut writer = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("held-orders").unwrap();
    let documents = |value| Documents::new(vec![StructFields::from([
        ("_id".into(), Value::I64(1)), ("value".into(), Value::I64(value)),
    ])], node.memory()).unwrap();
    let options = AutonomousTxOptions::default();
    let initial = TxOp::OwnedInsertDocs {
        relation: relation.clone(), documents: documents(10), valid_from: Some(Timestamp(0)), valid_to: None,
    };
    assert!(writer.state_mut().execute_tx(vec![initial], options.clone()).await.unwrap().committed());

    let held = writer.open_owned_read().await.unwrap();
    let id = EntityId::Integer(1);
    let first = rows(held.open_point(&relation, &id).await.unwrap().unwrap()).await;
    let updated = TxOp::OwnedReplaceDocs {
        relation: relation.clone(), documents: documents(20), valid_from: Some(Timestamp(0)), valid_to: None,
    };
    let write = writer.state_mut().execute_tx(vec![updated], options).await.unwrap();
    assert!(write.committed());
    assert!(write.tx_key().system_time > held.present().system_time);
    assert_eq!(rows(held.open_point(&relation, &id).await.unwrap().unwrap()).await, first);
    held.close();
    assert!(held.open_point(&relation, &id).await.is_err());
    drop(held);
    drop(writer);
    stop(&node, driver).await;
}

#[tokio::test]
async fn held_owned_point_reads_an_explicit_valid_instant_at_the_original_system_cut() {
    use rtdb_types::{EntityId, OwnedRelation};

    let mut settings = options();
    settings.owned_relations = true;
    let (node, driver) = running(settings, services()).await;
    let mut writer = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("held-business-time").unwrap();
    let id = EntityId::Integer(1);
    let documents = |value| Documents::new(vec![StructFields::from([
        ("_id".into(), Value::I64(1)), ("value".into(), Value::I64(value)),
    ])], node.memory()).unwrap();
    let write = TxOp::OwnedInsertDocs {
        relation: relation.clone(), documents: documents(10), valid_from: Some(Timestamp(10)), valid_to: Some(Timestamp(20)),
    };
    assert!(writer.state_mut().execute_tx(vec![write], AutonomousTxOptions::default()).await.unwrap().committed());
    let held = writer.open_owned_read().await.unwrap();
    let update = TxOp::OwnedReplaceDocs {
        relation: relation.clone(), documents: documents(11), valid_from: Some(Timestamp(10)), valid_to: Some(Timestamp(20)),
    };
    assert!(writer.state_mut().execute_tx(vec![update], AutonomousTxOptions::default()).await.unwrap().committed());
    let cursor = held.open_point_at(&relation, &id, Some(Timestamp(10))).await.unwrap().unwrap();
    assert_eq!(rows(cursor).await[0][..2], [Value::I64(1), Value::I64(10)]);
    let cursor = held.open_point_at(&relation, &id, Some(Timestamp(20))).await.unwrap().unwrap();
    assert!(rows(cursor).await.is_empty());
    assert!(held.open_point_at(&relation, &id, Some(Timestamp::MAX)).await.is_err());
    assert!(rows(held.open_point(&relation, &id).await.unwrap().unwrap()).await.is_empty());
    drop(held);
    drop(writer);
    stop(&node, driver).await;
}

#[tokio::test]
async fn held_owned_versions_page_source_groups_and_include_an_empty_deleted_timeline() {
    use rtdb_types::{EntityId, Interval, OwnedRelation};
    let mut settings = options();
    settings.owned_relations = true;
    let (node, driver) = running(settings, services()).await;
    let mut writer = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("system-history").unwrap();
    let id = EntityId::Integer(1);
    let docs = |id, value| Documents::new(vec![StructFields::from([
        ("_id".into(), Value::I64(id)), ("value".into(), Value::I64(value)),
    ])], node.memory()).unwrap();
    let make = |id, value| TxOp::OwnedInsertDocs {
        relation: relation.clone(), documents: docs(id, value), valid_from: Some(Timestamp(0)), valid_to: None,
    };
    let first = writer.state_mut().execute_tx(vec![make(1, 10)], AutonomousTxOptions::default()).await.unwrap();
    assert!(first.committed());
    let unrelated = writer.state_mut().execute_tx(vec![make(2, 99)], AutonomousTxOptions::default()).await.unwrap();
    assert!(unrelated.committed());
    let correction = TxOp::OwnedReplaceDocs {
        relation: relation.clone(), documents: docs(1, 20), valid_from: Some(Timestamp(5)), valid_to: Some(Timestamp(10)),
    };
    let changed = writer.state_mut().execute_tx(vec![correction], AutonomousTxOptions::default()).await.unwrap();
    assert!(changed.committed());
    let delete = TxOp::OwnedDelete {
        relation: relation.clone(), id: id.clone(), valid_from: Some(Timestamp::MIN), valid_to: Some(Timestamp::MAX),
    };
    let deleted = writer.state_mut().execute_tx(vec![delete], AutonomousTxOptions::default()).await.unwrap();
    assert!(deleted.committed());

    let held = writer.open_owned_read().await.unwrap();
    let floor = first.tx_key().system_time;
    let through = held.present().system_time.next().unwrap();
    use rtdb_node::{OwnedVersionOrder::{Ascending, Descending}, OwnedVersionRequest};

    let window = Interval::new(Timestamp::MIN, through).unwrap();
    let request = |after, scan_limit| OwnedVersionRequest {
        system: window, after, scan_limit, result_limit: scan_limit, order: Ascending,
    };
    let mut page = held.versions(&relation, &id, request(0, 1)).await.unwrap();
    assert_eq!(page.floor, Timestamp::MIN);
    assert_eq!(page.next_revision, 1);
    assert!(!page.done);
    let first_version = page.versions.pop().unwrap();
    assert_eq!(first_version.system_time, floor);
    assert_eq!(rows(first_version.timeline).await.len(), 1);
    let skipped = held.versions(&relation, &id, request(page.next_revision, 1)).await.unwrap();
    assert_eq!(skipped.next_revision, 2);
    assert!(skipped.versions.is_empty());
    let mut last = held.versions(&relation, &id, request(skipped.next_revision, 2)).await.unwrap();
    assert!(last.done);
    assert_eq!(last.next_revision, 4);
    let tombstoned = last.versions.pop().unwrap();
    assert_eq!(tombstoned.system_time, deleted.tx_key().system_time);
    assert!(rows(tombstoned.timeline).await.is_empty());
    let corrected = last.versions.pop().unwrap();
    assert_eq!(corrected.system_time, changed.tx_key().system_time);
    assert_eq!(rows(corrected.timeline).await.len(), 3);

    let descending = |after| OwnedVersionRequest {
        system: Interval::new(unrelated.tx_key().system_time, deleted.tx_key().system_time).unwrap(),
        after,
        scan_limit: 2,
        result_limit: 1,
        order: Descending,
    };
    let recent = held.versions(&relation, &id, descending(0)).await.unwrap();
    assert_eq!(recent.scanned, 1);
    assert_eq!(recent.next_revision, 3);
    assert_eq!(recent.versions[0].system_time, changed.tx_key().system_time);
    assert!(!recent.done);
    let older = held.versions(&relation, &id, descending(recent.next_revision)).await.unwrap();
    assert_eq!(older.scanned, 1);
    assert_eq!(older.next_revision, 2);
    assert!(older.versions.is_empty());
    assert!(older.done);

    drop(held);
    drop(writer);
    stop(&node, driver).await;
}

#[tokio::test]
async fn held_owned_valid_during_clips_winners_and_omits_deleted_windows() {
    use rtdb_types::{EntityId, Interval, OwnedRelation, TimeUnit};

    let mut settings = options();
    settings.owned_relations = true;
    let (node, driver) = running(settings, services()).await;
    let mut writer = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("valid-history").unwrap();
    let id = EntityId::Integer(1);
    let documents = |value| Documents::new(vec![StructFields::from([
        ("_id".into(), id.as_value()), ("value".into(), Value::I64(value)),
    ])], node.memory()).unwrap();
    let base = TxOp::OwnedInsertDocs {
        relation: relation.clone(), documents: documents(10), valid_from: Some(Timestamp(0)), valid_to: Some(Timestamp(100)),
    };
    let first = writer.state_mut().execute_tx(vec![base], AutonomousTxOptions::default()).await.unwrap();
    assert!(first.committed());
    let correction = TxOp::OwnedReplaceDocs {
        relation: relation.clone(), documents: documents(20), valid_from: Some(Timestamp(20)), valid_to: Some(Timestamp(60)),
    };
    let second = writer.state_mut().execute_tx(vec![correction], AutonomousTxOptions::default()).await.unwrap();
    assert!(second.committed());
    let delete = |from, to| TxOp::OwnedDelete {
        relation: relation.clone(), id: id.clone(), valid_from: Some(Timestamp(from)), valid_to: Some(Timestamp(to)),
    };
    assert!(writer.state_mut().execute_tx(vec![delete(30, 40)], AutonomousTxOptions::default()).await.unwrap().committed());
    let held = writer.open_owned_read().await.unwrap();
    assert!(writer.state_mut().execute_tx(vec![delete(60, 90)], AutonomousTxOptions::default()).await.unwrap().committed());

    let valid = Interval::from_micros(10, 70).unwrap();
    let mut history = held.open_valid_during_observed(&relation, &id, valid, held.system_time()).await.unwrap();
    assert!(history.witness().is_err());
    let mut actual = Vec::new();
    while let Some(batch) = history.next_batch().await.unwrap() {
        let batch = batch.batch();
        let read = |name, row| {
            let (slot, field) = batch.schema_ref().column_with_name(name).unwrap();
            rtdb_columnar::read_value(field, batch.column(slot).as_ref(), row).unwrap()
        };
        let instant = |name, row| match read(name, row) {
            Value::Timestamp { value, unit: TimeUnit::Microsecond, .. } => value,
            other => panic!("unexpected timestamp {other:?}"),
        };
        for row in 0..batch.num_rows() {
            actual.push((instant("_valid_from", row), instant("_valid_to", row), read("value", row), instant("_system_from", row)));
        }
    }
    assert_eq!(actual, [
        (10, 20, Value::I64(10), first.tx_key().system_time.0),
        (20, 30, Value::I64(20), second.tx_key().system_time.0),
        (40, 60, Value::I64(20), second.tx_key().system_time.0),
        (60, 70, Value::I64(10), first.tx_key().system_time.0),
    ]);
    assert!(history.witness().unwrap().is_none(), "standalone holds do not issue ordered witnesses");
    drop(history);
    let missing = EntityId::Integer(2);
    let mut absent = held.open_valid_during_observed(&relation, &missing, valid, held.system_time()).await.unwrap();
    assert!(absent.witness().is_err());
    assert!(absent.next_batch().await.unwrap().is_none());
    assert!(absent.witness().unwrap().is_none());
    drop(absent);
    drop(held);
    drop(writer);
    stop(&node, driver).await;
}

#[tokio::test]
async fn normal_owned_points_and_commits_keep_colliding_full_ids_independent() {
    use rtdb_types::{EntityId, OwnedRelation};

    let mut settings = options();
    settings.owned_relations = true;
    let (node, driver) = running(settings, services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("colliding-ids").unwrap();
    let first = EntityId::String("same-canonical-iid".into());
    let second = EntityId::Uuid(first.iid().0);
    assert_eq!(first.iid(), second.iid());
    let documents = |id: &EntityId, value| Documents::new(vec![StructFields::from([
        ("_id".into(), id.as_value()), ("value".into(), Value::I64(value)),
    ])], node.memory()).unwrap();
    let create = |id, value| TxOp::OwnedInsertDocs {
        relation: relation.clone(), documents: documents(id, value), valid_from: None, valid_to: None,
    };
    let initial = session.state_mut().execute_tx(vec![create(&first, 1), create(&second, 2)], AutonomousTxOptions::default()).await.unwrap();
    assert!(initial.committed(), "{:?}", initial.error());
    let held = session.open_owned_read().await.unwrap();
    let mut versions = Vec::new();
    for (id, expected) in [(&first, 1), (&second, 2)] {
        let mut point = held.open_point(&relation, id).await.unwrap().unwrap();
        let batch = point.next_batch().await.unwrap().unwrap();
        versions.push(rtdb_types::OwnedPointVersion {
            history: initial.tx_key().system_time,
            position: rtdb_query::owned_row_event_position(batch.batch(), 0).unwrap(),
        });
        let found = rows(held.open_point(&relation, id).await.unwrap().unwrap()).await;
        assert_eq!(found[0][..2], [id.as_value(), Value::I64(expected)]);
    }
    assert_ne!(versions[0].position, versions[1].position);

    let check = |id: &EntityId, expected| TxOp::OwnedCheck {
        relation: relation.clone(), id: id.clone(), valid_at: held.present().system_time, expected,
        revealed: None,
    };
    let replace = TxOp::OwnedReplaceDocs {
        relation: relation.clone(), documents: documents(&second, 3), valid_from: None, valid_to: None,
    };
    let delete = TxOp::OwnedDelete { relation: relation.clone(), id: first.clone(), valid_from: None, valid_to: None };
    let changed = session.state_mut().execute_tx(
        vec![check(&first, Some(versions[0])), check(&second, Some(versions[1])), replace, delete],
        AutonomousTxOptions::default(),
    ).await.unwrap();
    assert!(changed.committed(), "{:?}", changed.error());
    let after = session.open_owned_read().await.unwrap();
    assert!(rows(after.open_point(&relation, &first).await.unwrap().unwrap()).await.is_empty());
    assert_eq!(rows(after.open_point(&relation, &second).await.unwrap().unwrap()).await[0][..2], [second.as_value(), Value::I64(3)]);
    assert_eq!(rows(held.open_point(&relation, &first).await.unwrap().unwrap()).await[0][..2], [first.as_value(), Value::I64(1)]);
    drop(after);
    drop(held);
    drop(session);
    stop(&node, driver).await;
}

#[tokio::test]
async fn owned_erase_revokes_old_holds_without_erasing_a_colliding_identity() {
    use rtdb_types::{EntityId, OwnedChangeKind, OwnedRelation};

    let mut settings = options();
    settings.owned_relations = true;
    let (node, driver) = running(settings, services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("ordered-purge").unwrap();
    let retained = EntityId::String("same-canonical-iid".into());
    let erased = EntityId::Uuid(retained.iid().0);
    let documents = Documents::new([&retained, &erased].into_iter().map(|id| StructFields::from([
        ("_id".into(), id.as_value()), ("payload".into(), Value::I64(1)),
    ])).collect(), node.memory()).unwrap();
    let initial = session.state_mut().execute_tx(vec![TxOp::OwnedInsertDocs {
        relation: relation.clone(), documents, valid_from: None, valid_to: None,
    }], AutonomousTxOptions::default()).await.unwrap();
    assert!(initial.committed(), "{:?}", initial.error());

    let unrelated = OwnedRelation::new("other-retained-hold").unwrap();
    let other_document = Documents::new(vec![StructFields::from([
        ("_id".into(), retained.as_value()), ("payload".into(), Value::I64(2)),
    ])], node.memory()).unwrap();
    let separate = session.state_mut().execute_tx(vec![TxOp::OwnedInsertDocs {
        relation: unrelated.clone(), documents: other_document, valid_from: None, valid_to: None,
    }], AutonomousTxOptions::default()).await.unwrap();
    assert!(separate.committed(), "{:?}", separate.error());

    let before = session.open_owned_read().await.unwrap();
    let mut stale_cursor = before.open_point(&relation, &erased).await.unwrap().unwrap();
    let unaffected_cursor = before.open_point(&unrelated, &retained).await.unwrap().unwrap();
    let old_valid = before.present().system_time;
    let mut observation = before.open_point(&relation, &erased).await.unwrap().unwrap();
    let batch = observation.next_batch().await.unwrap().unwrap();
    let expected = Some(rtdb_types::OwnedPointVersion {
        history: initial.tx_key().system_time,
        position: rtdb_query::owned_row_event_position(batch.batch(), 0).unwrap(),
    });
    drop(batch);
    drop(observation);
    let purge = session.state_mut().execute_tx(vec![
        TxOp::OwnedCheck { relation: relation.clone(), id: erased.clone(), valid_at: old_valid, expected, revealed: None },
        TxOp::OwnedErase { relation: relation.clone(), id: erased.clone() },
    ], AutonomousTxOptions::default()).await.unwrap();
    assert!(purge.committed(), "{:?}", purge.error());
    before.check().unwrap();
    before.check_relation(&unrelated).unwrap();
    assert!(before.open_point(&relation, &erased).await.is_err());
    assert!(stale_cursor.next_batch().await.is_err());
    assert_eq!(rows(unaffected_cursor).await[0][0], retained.as_value());

    let after = session.open_owned_read().await.unwrap();
    assert!(rows(after.open_point(&relation, &erased).await.unwrap().unwrap()).await.is_empty());
    assert_eq!(rows(after.open_point(&relation, &retained).await.unwrap().unwrap()).await[0][0], retained.as_value());
    assert!(after.open_historical_point(&relation, &erased, old_valid, old_valid).await.is_err());
    assert!(after.open_historical_point(&relation, &retained, old_valid, old_valid).await.unwrap().is_some());
    let changes = after.changes(&relation, old_valid, 1).await.unwrap();
    assert_eq!(changes.groups.len(), 1);
    assert_eq!(changes.groups[0].system_time, purge.tx_key().system_time);
    assert_eq!(changes.groups[0].touches.len(), 1);
    assert_eq!(changes.groups[0].touches[0].kind, OwnedChangeKind::Erase);
    assert_eq!(changes.groups[0].touches[0].id, erased);

    let also_erased = session.state_mut().execute_tx(vec![TxOp::OwnedErase {
        relation: relation.clone(), id: retained.clone(),
    }], AutonomousTxOptions::default()).await.unwrap();
    assert!(also_erased.committed(), "{:?}", also_erased.error());
    let current = session.open_owned_read().await.unwrap();
    let first_floor = purge.tx_key().system_time;
    assert!(current.check_point_purge_floor(&relation, &erased, first_floor).await.is_ok());
    assert!(current.check_point_purge_floor(&relation, &retained, first_floor).await.is_err());
    drop(current);
    drop(after);
    drop(before);
    drop(session);
    stop(&node, driver).await;
}

#[tokio::test]
async fn ordered_owned_unique_groups_swaps_and_refuses_conflicts() {
    use rtdb_types::{EntityId, OwnedRelation};

    let mut settings = options();
    settings.owned_relations = true;
    let (node, driver) = running(settings, services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("unique-orders").unwrap();
    let declaration = TxOp::OwnedDeclareUniquePath {
        relation: relation.clone(), name: OwnedRelation::new("order-key").unwrap(), column: "canonical".into(),
    };
    assert!(session.state_mut().execute_tx(vec![declaration], AutonomousTxOptions::default()).await.unwrap().committed());
    let documents = |id, key| Documents::new(vec![StructFields::from([
        ("_id".into(), Value::I64(id)), ("canonical".into(), Value::Binary(vec![key])),
    ])], node.memory()).unwrap();
    let insert = |id, key| TxOp::OwnedInsertDocs {
        relation: relation.clone(), documents: documents(id, key), valid_from: None, valid_to: None,
    };
    let replace = |id, key, valid_to| TxOp::OwnedReplaceDocs {
        relation: relation.clone(), documents: documents(id, key), valid_from: None, valid_to,
    };
    let written = session.state_mut().execute_tx(vec![insert(1, 1), insert(2, 2)], AutonomousTxOptions::default()).await.unwrap();
    assert!(written.committed(), "{:?}", written.error());
    let conflict = session.state_mut().execute_tx(vec![replace(1, 2, None)], AutonomousTxOptions::default()).await.unwrap();
    let error = conflict.error().unwrap();
    assert_eq!(error.code, "rtdb.owned/unique-taken");
    assert!(error.context.is_empty());
    let conflict = session.state_mut().execute_tx(vec![replace(1, 3, None), replace(2, 3, None)], AutonomousTxOptions::default()).await.unwrap();
    assert_eq!(conflict.error().unwrap().code, "rtdb.owned/unique-taken");

    let swap = session.state_mut().execute_tx(vec![replace(2, 1, None), replace(1, 2, None)], AutonomousTxOptions::default()).await.unwrap();
    assert!(swap.committed(), "{:?}", swap.error());
    let held = session.open_owned_read().await.unwrap();
    let key = rows(held.open_point(&relation, &EntityId::Integer(2)).await.unwrap().unwrap()).await;
    assert_eq!(key[0][1], Value::Binary(vec![1]));
    let path = OwnedRelation::new("order-key").unwrap();
    let valid = held.system_time();
    let hit = held.unique(&relation, &path, &[1], valid).await.unwrap().unwrap();
    assert_eq!(
        rtdb_query::owned_row_revision(hit.row.batch(), 0, &EntityId::Integer(2), node.memory()).unwrap(),
        hit.revision,
    );
    assert!(held.unique(&relation, &path, &[3], valid).await.unwrap().is_none());
    let future = held.unique(&relation, &path, &[1], valid.next().unwrap()).await.unwrap().unwrap();
    assert_eq!(
        rtdb_query::owned_row_revision(future.row.batch(), 0, &EntityId::Integer(2), node.memory()).unwrap(),
        hit.revision,
    );
    drop(held);

    let end = Timestamp(i64::MAX - 1);
    let bounded = session.state_mut().execute_tx(
        vec![replace(1, 4, Some(end))], AutonomousTxOptions::default(),
    ).await.unwrap();
    assert!(bounded.committed(), "{:?}", bounded.error());
    let held = session.open_owned_read().await.unwrap();
    assert!(held.unique(&relation, &path, &[4], bounded.tx_key().system_time).await.unwrap().is_some());
    assert!(held.unique(&relation, &path, &[4], end).await.unwrap().is_none());
    assert!(held.unique(&relation, &path, &[2], end).await.unwrap().is_some());
    drop(held);
    let release = TxOp::OwnedDelete {
        relation: relation.clone(), id: EntityId::Integer(2), valid_from: None, valid_to: None,
    };
    let transfer = session.state_mut().execute_tx(vec![insert(3, 1), release], AutonomousTxOptions::default()).await.unwrap();
    assert!(transfer.committed(), "{:?}", transfer.error());
    let private = relation.unique_table("xtdb");
    let query = format!("SELECT holder FROM {}.\"{}\"", private.schema, private.table);
    assert!(session.query_sql(&query, vec![]).await.is_err());

    drop(session);
    stop(&node, driver).await;
}

#[tokio::test]
async fn owned_prefix_capacity_checks_atomic_final_membership() {
    use rtdb_types::{EntityId, OwnedRelation};

    let mut settings = options();
    settings.owned_relations = true;
    let (node, driver) = running(settings, services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("capacity-orders").unwrap();
    let other = OwnedRelation::new("capacity-other").unwrap();
    let rule = |relation: &OwnedRelation| TxOp::OwnedDeclareCapacity {
        relation: relation.clone(), column: "canonical".into(), prefix_bytes: 1, cap: 2, when: Vec::new(),
    };
    let insert = |relation: &OwnedRelation, id, key| TxOp::OwnedInsertDocs {
        relation: relation.clone(),
        documents: Documents::new(vec![StructFields::from([
            ("_id".into(), Value::I64(id)), ("canonical".into(), Value::Binary(key)),
        ])], node.memory()).unwrap(),
        valid_from: None, valid_to: None,
    };
    let delete = |id, valid_to| TxOp::OwnedDelete {
        relation: relation.clone(), id: EntityId::Integer(id), valid_from: None, valid_to,
    };
    let outcome = session.state_mut().execute_tx(vec![
        rule(&relation), insert(&relation, 1, vec![7, 1]), insert(&relation, 2, vec![7, 2]),
    ], AutonomousTxOptions::default()).await.unwrap();
    assert!(outcome.committed(), "{:?}", outcome.error());
    let over = session.state_mut().execute_tx(vec![insert(&relation, 3, vec![7, 3])],
        AutonomousTxOptions::default()).await.unwrap();
    assert_eq!(over.error().unwrap().code, "rtdb.owned/capacity-exceeded");
    let erase = session.state_mut().execute_tx(vec![TxOp::OwnedErase {
        relation: relation.clone(), id: EntityId::Integer(2),
    }], AutonomousTxOptions::default()).await.unwrap();
    assert_eq!(erase.error().unwrap().code, "rtdb.owned/purge-path-uncovered");
    let still_full = session.state_mut().execute_tx(vec![insert(&relation, 3, vec![7, 3])],
        AutonomousTxOptions::default()).await.unwrap();
    assert_eq!(still_full.error().unwrap().code, "rtdb.owned/capacity-exceeded");
    let outcome = session.state_mut().execute_tx(vec![
        insert(&relation, 3, vec![7, 3]), delete(1, None), insert(&relation, 4, vec![8, 4]),
        insert(&other, 1, vec![7, 1]),
    ], AutonomousTxOptions::default()).await.unwrap();
    assert!(outcome.committed(), "{:?}", outcome.error());
    let deadline = || TxOp::owned_deadline(Timestamp::MAX);
    let count = |prefix, at_most| TxOp::OwnedCheckCapacity {
        relation: relation.clone(), column: "canonical".into(), prefix, at_most,
    };
    let rejected = session.state_mut().execute_tx(vec![
        deadline(), count(vec![7], 1), insert(&other, 2, vec![7, 2]),
    ], AutonomousTxOptions::default()).await.unwrap();
    assert_eq!(rejected.error().unwrap().code, "rtdb.owned/count-bound-exceeded");
    let accepted = session.state_mut().execute_tx(vec![
        deadline(), count(vec![7], 1), count(vec![8], 1), delete(2, None), insert(&other, 2, vec![7, 2]),
    ], AutonomousTxOptions::default()).await.unwrap();
    assert!(accepted.committed(), "{:?}", accepted.error());
    let added = session.state_mut().execute_tx(vec![
        deadline(), count(vec![7], 1), count(vec![8], 1), insert(&relation, 5, vec![8, 5]),
    ], AutonomousTxOptions::default()).await.unwrap();
    assert_eq!(added.error().unwrap().code, "rtdb.owned/count-bound-exceeded");
    let unchanged = session.state_mut().execute_tx(vec![
        deadline(), count(vec![7], 1), count(vec![8], 1),
    ], AutonomousTxOptions::default()).await.unwrap();
    assert!(unchanged.committed(), "{:?}", unchanged.error());
    let wrong_prefix = session.state_mut().execute_tx(vec![
        deadline(), count(vec![7, 1], 2),
    ], AutonomousTxOptions::default()).await.unwrap();
    assert_eq!(wrong_prefix.error().unwrap().code, "rtdb.owned/count-bound");
    let timed = session.state_mut().execute_tx(vec![delete(2, Some(Timestamp(i64::MAX - 1)))],
        AutonomousTxOptions::default()).await.unwrap();
    assert_eq!(timed.error().unwrap().code, "rtdb.owned/capacity-valid-time");
    let late = session.state_mut().execute_tx(vec![rule(&other)], AutonomousTxOptions::default()).await.unwrap();
    assert_eq!(late.error().unwrap().code, "rtdb.owned/capacity-bootstrap");

    drop(session);
    stop(&node, driver).await;
}

#[tokio::test]
async fn populated_capacity_is_installed_at_the_final_relation_frontier() {
    use rtdb_types::{EntityId, OwnedRelation, OwnedSchema};

    let mut settings = options();
    settings.owned_relations = true;
    let (node, driver) = running(settings, services()).await;
    let mut writer = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("populated-capacity").unwrap();
    let schema = OwnedSchema::from_json(
        br#"{"fields":[{"name":"canonical","type":{"kind":"binary"},"optional":false,"nullable":false}],"validity":"acceptance"}"#,
    ).unwrap();
    let insert = |id: i64| TxOp::OwnedInsertDocs {
        relation: relation.clone(),
        documents: Documents::new(vec![StructFields::from([
            ("_id".into(), Value::I64(id)), ("canonical".into(), Value::Binary(vec![7, id as u8])),
        ])], node.memory()).unwrap(),
        valid_from: None, valid_to: None,
    };
    let initial = writer.state_mut().execute_tx(vec![
        TxOp::OwnedDeclareRelation { relation: relation.clone(), schema },
        insert(1), insert(2), insert(3),
    ], AutonomousTxOptions::default()).await.unwrap();
    assert!(initial.committed(), "{:?}", initial.error());

    let rule = |cap| TxOp::OwnedDeclareCapacity {
        relation: relation.clone(), column: "canonical".into(), prefix_bytes: 1, cap, when: Vec::new(),
    };
    let delete = |id| TxOp::OwnedDelete {
        relation: relation.clone(), id: EntityId::Integer(id), valid_from: None, valid_to: None,
    };
    let refused = writer.state_mut().execute_tx(vec![rule(2)], AutonomousTxOptions::default()).await.unwrap();
    assert_eq!(refused.error().unwrap().code, "rtdb.owned/capacity-already-exceeded");
    let applied = writer.state_mut().execute_tx(vec![
        delete(1), rule(2),
    ], AutonomousTxOptions::default()).await.unwrap();
    assert!(applied.committed(), "{:?}", applied.error());
    let next = writer.state_mut().execute_tx(vec![insert(4)], AutonomousTxOptions::default()).await.unwrap();
    assert_eq!(next.error().unwrap().code, "rtdb.owned/capacity-exceeded");

    let refused = writer.state_mut().execute_tx(vec![rule(1)], AutonomousTxOptions::default()).await.unwrap();
    assert_eq!(refused.error().unwrap().code, "rtdb.owned/capacity-already-exceeded");
    let swap = writer.state_mut().execute_tx(vec![delete(2), insert(4)], AutonomousTxOptions::default()).await.unwrap();
    assert!(swap.committed(), "{:?}", swap.error());
    let raised = writer.state_mut().execute_tx(vec![rule(3), insert(5)], AutonomousTxOptions::default()).await.unwrap();
    assert!(raised.committed(), "{:?}", raised.error());
    let lowered = writer.state_mut().execute_tx(vec![
        rule(1), delete(3), delete(4),
    ], AutonomousTxOptions::default()).await.unwrap();
    assert!(lowered.committed(), "{:?}", lowered.error());
    let next = writer.state_mut().execute_tx(vec![insert(6)], AutonomousTxOptions::default()).await.unwrap();
    assert_eq!(next.error().unwrap().code, "rtdb.owned/capacity-exceeded");

    drop(writer);
    stop(&node, driver).await;
}

#[tokio::test]
async fn owned_acceptance_validity_rejects_scheduled_writes_at_ordering() {
    use rtdb_types::{OwnedRelation, OwnedSchema};

    let mut settings = options();
    settings.owned_relations = true;
    let (node, driver) = running(settings, services()).await;
    let mut writer = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("at-acceptance").unwrap();
    let schema = OwnedSchema::from_json(br#"{"fields":[],"validity":"acceptance"}"#).unwrap();
    let declared = writer.state_mut().execute_tx(
        vec![TxOp::OwnedDeclareRelation { relation: relation.clone(), schema }],
        AutonomousTxOptions::default(),
    ).await.unwrap();
    assert!(declared.committed(), "{:?}", declared.error());

    let id = EntityId::String("item".into());
    let replace = |override_from, valid_from| {
        let mut row = StructFields::from([("_id".into(), id.as_value())]);
        if override_from {
            row.insert("_valid_from".into(), Value::Timestamp {
                value: 15,
                unit: rtdb_types::TimeUnit::Microsecond,
                timezone: Some("UTC".into()),
            });
        }
        TxOp::OwnedReplaceDocs {
            relation: relation.clone(),
            documents: Documents::new(vec![row], node.memory()).unwrap(),
            valid_from,
            valid_to: None,
        }
    };
    let delete = |valid_to| TxOp::OwnedDelete {
        relation: relation.clone(), id: id.clone(), valid_from: None, valid_to,
    };
    for op in [replace(false, Some(Timestamp(15))), replace(true, None), delete(Some(Timestamp(15)))] {
        let rejected = writer.state_mut().execute_tx(vec![op], AutonomousTxOptions::default()).await.unwrap();
        assert_eq!(rejected.error().unwrap().code, "rtdb.owned/validity-policy");
    }
    let put = writer.state_mut().execute_tx(vec![replace(false, None)], AutonomousTxOptions::default()).await.unwrap();
    assert!(put.committed(), "{:?}", put.error());
    let removed = writer.state_mut().execute_tx(vec![delete(None)], AutonomousTxOptions::default()).await.unwrap();
    assert!(removed.committed(), "{:?}", removed.error());
    let held = writer.open_owned_read().await.unwrap();
    let changes = held.changes(&relation, Timestamp::MIN, 2).await.unwrap();
    assert_eq!(changes.groups.len(), 2);
    for (group, accepted) in changes.groups.iter().zip([put.tx_key(), removed.tx_key()]) {
        assert_eq!(group.system_time, accepted.system_time);
        assert_eq!(group.touches[0].valid.lower(), accepted.system_time);
        assert_eq!(group.touches[0].valid.upper(), Timestamp::MAX);
    }
    drop(held);
    drop(writer);
    stop(&node, driver).await;
}

#[tokio::test]
async fn a_retired_owned_relation_refuses_writes_and_declarations() {
    use rtdb_types::{OwnedRelation, OwnedSchema};

    let mut settings = options();
    settings.owned_relations = true;
    let (node, driver) = running(settings, services()).await;
    let mut writer = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("retiring").unwrap();
    let path = OwnedRelation::new("by-rank").unwrap();
    let schema = OwnedSchema::from_json(
        br#"{"fields":[{"name":"rank","type":{"kind":"i64"},"optional":false,"nullable":false}]}"#,
    ).unwrap();
    let declare = || TxOp::OwnedDeclareRelation { relation: relation.clone(), schema: schema.clone() };
    let declare_path = || TxOp::OwnedDeclarePath {
        relation: relation.clone(), name: path.clone(), columns: vec!["rank".into(), "_id".into()],
    };
    let insert = |id: i64| TxOp::OwnedInsertDocs {
        relation: relation.clone(),
        documents: Documents::new(vec![StructFields::from([
            ("_id".into(), Value::I64(id)), ("rank".into(), Value::I64(id)),
        ])], node.memory()).unwrap(),
        valid_from: None, valid_to: None,
    };
    let retire = |retain_micros| TxOp::OwnedRetireRelation { relation: relation.clone(), retain_micros };
    for operation in [declare(), insert(1), declare_path()] {
        let written = writer.state_mut().execute_tx(vec![operation], AutonomousTxOptions::default()).await.unwrap();
        assert!(written.committed(), "{:?}", written.error());
    }
    let together = writer.state_mut().execute_tx(vec![retire(0), insert(2)], AutonomousTxOptions::default()).await.unwrap();
    assert_eq!(together.error().unwrap().code, "rtdb.owned/relation-retired");

    let retired = writer.state_mut().execute_tx(vec![retire(0)], AutonomousTxOptions::default()).await.unwrap();
    assert!(retired.committed(), "{:?}", retired.error());
    for operation in [insert(3), declare(), declare_path()] {
        let refused = writer.state_mut().execute_tx(vec![operation], AutonomousTxOptions::default()).await.unwrap();
        assert_eq!(refused.error().unwrap().code, "rtdb.owned/relation-retired");
    }
    let again = writer.state_mut().execute_tx(vec![retire(0)], AutonomousTxOptions::default()).await.unwrap();
    assert!(again.committed(), "{:?}", again.error());
    let other = writer.state_mut().execute_tx(vec![retire(1)], AutonomousTxOptions::default()).await.unwrap();
    assert_eq!(other.error().unwrap().code, "rtdb.owned/relation-retired");

    drop(writer);
    stop(&node, driver).await;
}

#[tokio::test]
async fn held_ranges_read_arrow_ids_at_lifetime_and_historical_cuts() {
    use rtdb_types::{OwnedRelation, OwnedSchema};

    let mut settings = options();
    settings.owned_relations = true;
    settings.automatic_artefacts = false;
    let clock = Arc::new(TickClock(AtomicI64::new(1_000_000)));
    let runtime = NodeServices {
        resources: Arc::new(NativeResources::default().with_local_log_clock(clock.clone(), false)),
        clock: clock.clone(),
        ..services()
    };
    let (node, driver) = running(settings, runtime).await;
    let mut writer = node.session("xtdb", Arc::new(Authority)).unwrap();
    let ids = [Value::Utf8("order/first".into()), Value::Utf8("order/second".into())];
    let lifetime = 10_000_000;
    for (name, policy) in [
        ("historical-range", r#""validity":"explicit""#.into()),
        ("lifetime-range", format!(r#""validity":"acceptance","lifetime_micros":{lifetime}"#)),
    ] {
        let relation = OwnedRelation::new(name).unwrap();
        let path = OwnedRelation::new(format!("by-priority-{name}")).unwrap();
        let schema = OwnedSchema::from_json(format!(r#"{{{policy},"fields":[
            {{"name":"priority","type":{{"kind":"i64"}},"optional":false,"nullable":false}}
        ]}}"#).as_bytes()).unwrap();
        let governed = schema.lifetime_micros().is_some();
        for operation in [
            TxOp::OwnedDeclareRelation { relation: relation.clone(), schema },
            TxOp::OwnedDeclarePath { relation: relation.clone(), name: path.clone(), columns: vec!["priority".into(), "_id".into()] },
        ] {
            let result = writer.state_mut().execute_tx(vec![operation], AutonomousTxOptions::default()).await.unwrap();
            assert!(result.committed(), "{:?}", result.error());
        }
        let mut revisions = Vec::new();
        for (offset, id) in ids.iter().enumerate() {
            let documents = Documents::new(vec![StructFields::from([
                ("_id".into(), id.clone()), ("priority".into(), Value::I64(offset as i64)),
            ])], node.memory()).unwrap();
            let result = writer.state_mut().execute_tx(vec![TxOp::OwnedInsertDocs {
                relation: relation.clone(), documents, valid_from: (!governed).then_some(Timestamp(0)), valid_to: None,
            }], AutonomousTxOptions::default()).await.unwrap();
            assert!(result.committed(), "{:?}", result.error());
            revisions.push(result.tx_key().system_time);
        }

        let first = revisions[0];
        let held = writer.open_owned_read().await.unwrap();
        let request = |descending, limit| OwnedRangeRequest { lower: None, upper: None, after: None, descending, limit };
        let read_ids = |page: OwnedRangePage| {
            let batch = page.batch.unwrap();
            let (index, field) = page.schema.column_with_name("_id").unwrap();
            (0..batch.batch().num_rows()).map(|row| {
                read_value(field, batch.batch().column(index).as_ref(), row).unwrap()
            }).collect::<Vec<_>>()
        };
        let (page, expected) = if governed {
            let (page, _, expiry) = held.range_page_by_key_lifetime(&relation, &path, request(false, 2), None).await.unwrap();
            let cutoff = Timestamp(first.0 + lifetime);
            assert_eq!(expiry, Some(cutoff));
            let present = held.present().system_time;
            let (reverse, _, reverse_expiry) = held.range_page_by_key_lifetime(&relation, &path, request(true, 2), Some(present)).await.unwrap();
            assert_eq!(read_ids(reverse), [ids[1].clone(), ids[0].clone()]);
            assert_eq!(reverse_expiry, expiry);
            let (single, _, single_expiry) = held.range_page_by_key_lifetime(&relation, &path, request(true, 1), None).await.unwrap();
            assert_eq!(read_ids(single), [ids[1].clone()]);
            assert_eq!(single_expiry, Some(Timestamp(revisions[1].0 + lifetime)));

            let bound = StructFields::from([("priority".into(), Value::I64(2)), ("_id".into(), ids[1].clone())]);
            let empty = OwnedRangeRequest { lower: Some(OwnedKeyBound::Values(&bound)), ..request(false, 2) };
            let (empty, _, empty_expiry) = held.range_page_by_key_lifetime(&relation, &path, empty, None).await.unwrap();
            assert!(empty.batch.is_none());
            assert_eq!(empty_expiry, None);
            let Err(error) = held.range_page_by_key_lifetime(&relation, &path, request(false, 0), Some(first)).await else {
                panic!("lifetime ranges require the held present");
            };
            assert_eq!(error.code, "rtdb.owned/lifetime-uncovered");
            let Err(error) = held.range_page_by_key_lifetime(&relation, &path, request(false, 0), Some(present)).await else {
                panic!("lifetime ranges require an admitted page limit");
            };
            assert_eq!(error.code, "rtdb.owned/range-limit");
            clock.0.fetch_max(cutoff.0, Ordering::Relaxed);
            assert_eq!(held.check_lifetime_range_release(&relation, expiry).unwrap_err().code, "rtdb.owned/lifetime-expired");
            (page, ids.as_slice())
        } else {
            let page = held.range_page_by_key_at_system(&relation, &path, request(false, 2), first, Timestamp(0)).await.unwrap();
            (page, &ids[..1])
        };
        assert_eq!(read_ids(page).as_slice(), expected);
    }
    drop(writer);
    stop(&node, driver).await;
}

#[tokio::test]
async fn held_owned_range_covers_declared_empty_and_uncheckpointed_relations() {
    use rtdb_types::OwnedRelation;

    let mut settings = options();
    settings.owned_relations = true;
    settings.automatic_artefacts = false;
    let (node, driver) = running(settings, services()).await;
    let mut writer = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("held-range").unwrap();
    let path = OwnedRelation::new("by-priority").unwrap();
    let schema = rtdb_types::OwnedSchema::from_json(
        br#"{"fields":[{"name":"priority","type":{"kind":"i64"},"optional":false,"nullable":false}]}"#,
    ).unwrap();
    let write = writer.state_mut().execute_tx(
        vec![TxOp::OwnedDeclareRelation { relation: relation.clone(), schema }],
        AutonomousTxOptions::default(),
    ).await.unwrap();
    assert!(write.committed(), "{:?}", write.error());
    let declaration = TxOp::OwnedDeclarePath {
        relation: relation.clone(), name: path.clone(), columns: vec!["priority".into(), "_id".into()],
    };
    let write = writer.state_mut().execute_tx(vec![declaration], AutonomousTxOptions::default()).await.unwrap();
    assert!(write.committed(), "{:?}", write.error());
    let empty = writer.open_owned_read().await.unwrap();
    let key = |priority, id| StructFields::from([
        ("priority".into(), Value::I64(priority)),
        ("_id".into(), Value::I64(id)),
    ]);
    let lower = key(10, 1);
    let upper = key(20, 3);
    let empty_count = || rtdb_node::OwnedCountRequest {
        lower: Some(OwnedKeyBound::Values(&lower)), upper: Some(OwnedKeyBound::Values(&upper)), valid_at: Timestamp(10),
    };
    let request = rtdb_node::OwnedRangeRequest {
        lower: Some(OwnedKeyBound::Values(&lower)), upper: Some(OwnedKeyBound::Values(&upper)), after: None, descending: false, limit: 1,
    };
    let page = empty.range_page_by_key(&relation, &path, request).await.unwrap();
    assert!(page.batch.is_none());
    assert_eq!(page.schema.field(0).data_type(), &arrow_schema::DataType::Int64);
    assert_eq!(page.schema.field(1).data_type(), &arrow_schema::DataType::Int64);
    assert_eq!(empty.count_range(&relation, &path, empty_count()).await.unwrap().count, 0);
    let malformed = StructFields::from([
        ("priority".into(), Value::Utf8("10".into())),
        ("_id".into(), Value::I64(1)),
    ]);
    let request = rtdb_node::OwnedCountRequest {
        lower: Some(OwnedKeyBound::Values(&malformed)), upper: Some(OwnedKeyBound::Values(&upper)), valid_at: Timestamp(10),
    };
    assert_eq!(empty.count_range(&relation, &path, request).await.err().unwrap().code, "rtdb.owned/range-bound");
    let documents = Documents::new(
        [(3, 20), (2, 10), (1, 10)].into_iter().map(|(id, priority)| StructFields::from([
            ("_id".into(), Value::I64(id)),
            ("priority".into(), Value::I64(priority)),
        ])).collect(),
        node.memory(),
    ).unwrap();
    let insert = TxOp::OwnedInsertDocs {
        relation: relation.clone(), documents, valid_from: Some(Timestamp(0)), valid_to: None,
    };
    let write = writer.state_mut().execute_tx(vec![insert], AutonomousTxOptions::default()).await.unwrap();
    assert!(write.committed(), "{:?}", write.error());
    assert_eq!(empty.count_range(&relation, &path, empty_count()).await.unwrap().count, 0);
    drop(empty);
    let publications = OwnedRelation::new("publications").unwrap();
    let marker = EntityId::String("head".into());
    let publish = TxOp::OwnedInsertDocs {
        relation: publications.clone(),
        documents: Documents::new(
            vec![StructFields::from([("_id".into(), marker.as_value())])], node.memory(),
        ).unwrap(),
        valid_from: Some(Timestamp::MIN),
        valid_to: None,
    };
    let write = writer.state_mut().execute_tx(vec![publish], AutonomousTxOptions::default()).await.unwrap();
    assert!(write.committed(), "{:?}", write.error());
    let selected = write.tx_key().system_time;
    let correction = TxOp::OwnedReplaceDocs {
        relation: relation.clone(),
        documents: Documents::new(vec![StructFields::from([
            ("_id".into(), Value::I64(1)), ("priority".into(), Value::I64(99)),
        ])], node.memory()).unwrap(),
        valid_from: Some(Timestamp(10)), valid_to: Some(Timestamp(20)),
    };
    let write = writer.state_mut().execute_tx(vec![correction], AutonomousTxOptions::default()).await.unwrap();
    assert!(write.committed(), "{:?}", write.error());
    let open_count = || rtdb_node::OwnedCountRequest {
        lower: None, upper: None, valid_at: Timestamp(10),
    };
    let held = writer.open_owned_read().await.unwrap();
    let unknown = OwnedRelation::new("unknown-path").unwrap();
    let failure = held.range(&relation, &unknown, None, None, 2).await.err().unwrap();
    assert_eq!(failure.code, "rtdb.owned/path-uncovered");
    assert_eq!(held.count_range(&relation, &path, open_count()).await.unwrap().count, 3);
    let page = held.range(&relation, &path, None, None, 2).await.unwrap();
    assert_eq!(page.batch.unwrap().batch().num_rows(), 2);
    held.close();
    drop(held);

    let database = node.catalog().database("xtdb").unwrap();
    index_flush(&node, &database).await;
    let held = writer.open_owned_read().await.unwrap();
    assert_eq!(held.count_range(&relation, &path, open_count()).await.unwrap().count, 3);
    drop(held);
    database.build_artefacts().await.unwrap();
    let held = writer.open_owned_read().await.unwrap();
    let lower = Value::I64(10);
    let upper = Value::I64(20);
    let result = held.range(&relation, &path, Some(&lower), Some(&upper), 8).await.unwrap();
    let batch = result.batch.unwrap();
    let batch = batch.batch();
    assert_eq!(batch.num_rows(), 2);
    let ids = (0..batch.num_rows()).map(|row| {
        rtdb_columnar::read_value(result.schema.field(1), batch.column(1).as_ref(), row).unwrap()
    }).collect::<Vec<_>>();
    assert_eq!(ids, [Value::I64(1), Value::I64(2)]);

    let lower = key(10, 1);
    let upper = key(20, 3);
    for (valid_at, expected) in [(9, 2), (10, 1), (19, 1), (20, 2)] {
        let request = rtdb_node::OwnedCountRequest {
            lower: Some(OwnedKeyBound::Values(&lower)), upper: Some(OwnedKeyBound::Values(&upper)), valid_at: Timestamp(valid_at),
        };
        let (result, witness) = held.count_range_observed(&relation, &path, request).await.unwrap();
        assert!(witness.is_none(), "standalone counts do not issue ordered witnesses");
        assert_eq!(result.count, expected);
        assert_eq!(result.present, held.present().system_time);
        assert_eq!(result.valid_at, Timestamp(valid_at));
    }
    for (descending, after, next) in [(false, key(10, 1), 2), (true, key(10, 2), 1)] {
        let request = rtdb_node::OwnedRangeRequest {
            lower: Some(OwnedKeyBound::Values(&lower)), upper: Some(OwnedKeyBound::Values(&upper)),
            after: Some(OwnedKeyBound::Values(&after)), descending, limit: 1,
        };
        let page = held.range_page_by_key(&relation, &path, request).await.unwrap();
        let batch = page.batch.unwrap();
        assert_eq!(batch.batch().num_rows(), 1);
        assert_eq!(
            rtdb_columnar::read_value(page.schema.field(1), batch.batch().column(1).as_ref(), 0).unwrap(),
            Value::I64(next),
        );
    }
    let old = held.derive_as_of(
        selected, &publications, &marker, vec![publications.clone(), relation.clone()],
    ).await.unwrap().unwrap();
    for cut in [&held, &old] {
        let valid = Timestamp(10);
        let request = rtdb_node::OwnedRangeRequest {
            lower: Some(OwnedKeyBound::Values(&lower)), upper: Some(OwnedKeyBound::Values(&upper)),
            after: None, descending: false, limit: 1,
        };
        let page = cut.range_page_by_key_at_system(&relation, &path, request, selected, valid).await.unwrap();
        let batch = page.batch.unwrap();
        assert_eq!(batch.batch().num_rows(), 1);
        assert_eq!(
            rtdb_columnar::read_value(page.schema.field(1), batch.batch().column(1).as_ref(), 0).unwrap(),
            Value::I64(1),
        );
        let request = rtdb_node::OwnedCountRequest {
            lower: Some(OwnedKeyBound::Values(&lower)), upper: Some(OwnedKeyBound::Values(&upper)), valid_at: valid,
        };
        let result = cut.count_range_at_system(&relation, &path, request, selected).await.unwrap();
        assert_eq!((result.count, result.present, result.valid_at), (2, selected, valid));
    }
    let late = TxOp::OwnedInsertDocs {
        relation: relation.clone(),
        documents: Documents::new(vec![StructFields::from([
            ("_id".into(), Value::I64(4)), ("priority".into(), Value::I64(15)),
        ])], node.memory()).unwrap(),
        valid_from: Some(Timestamp(0)),
        valid_to: None,
    };
    let write = writer.state_mut().execute_tx(vec![late], AutonomousTxOptions::default()).await.unwrap();
    assert!(write.committed(), "{:?}", write.error());
    let fresh = writer.open_owned_read().await.unwrap();
    assert_eq!(fresh.count_range(&relation, &path, open_count()).await.unwrap().count, 4);
    assert_eq!(held.count_range(&relation, &path, open_count()).await.unwrap().count, 3);
    drop(fresh);
    drop(old);
    drop(held);
    drop(writer);
    stop(&node, driver).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_awaits_of_stored_outcomes_each_read_their_own_transaction() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (node, driver) = running(options(), services()).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        let mut results = Vec::new();
        for id in 0..9 {
            let result = session.state_mut().execute_tx(vec![put(&node, id, id)], AutonomousTxOptions::default()).await.unwrap();
            assert!(result.committed());
            results.push(result);
        }
        let stored = results[..8].to_vec();
        let awaits = stored.iter().map(|expected| {
            let node = node.clone();
            let tx_id = expected.tx_key().tx_id;
            tokio::spawn(async move { node.await_transaction("xtdb", tx_id, &Authority, &CancellationToken::new()).await })
        }).collect::<Vec<_>>();
        for (expected, awaited) in stored.iter().zip(futures::future::join_all(awaits).await) {
            assert_eq!(&awaited.unwrap().unwrap(), expected);
        }
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
async fn native_logical_table_scan_and_sort_preserve_their_root_pages() {
    let mut config = options();
    config.query.batch_rows = 1;
    let (node, driver) = running(config, services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let token = session.state().cancellation().child_token();
    let tx = session.state_mut().execute_tx(vec![put(&node, 1, 10), put(&node, 2, 20)],
        AutonomousTxOptions::default()).await.unwrap();
    assert!(tx.committed());
    let scan = "[:scan {:db-name \"xtdb\" :table #xt/table docs :columns [value]}]";
    let mut cursor = session.query_ra(scan, StructFields::new(), &token).await.unwrap();
    let batch = cursor.next_batch().await.unwrap().unwrap();
    let column = batch.batch().column(0).as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
    let mut values = column.values().to_vec();
    values.sort_unstable();
    assert_eq!(values, [10, 20]);
    assert!(cursor.next_batch().await.unwrap().is_none());

    let order = format!("[:sort {{:order-specs [[value {{:direction :desc}}]]}} {scan}]");
    let mut cursor = session.query_ra(&order, StructFields::new(), &token).await.unwrap();
    let batch = cursor.next_batch().await.unwrap().unwrap();
    let column = batch.batch().column(0).as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
    assert_eq!(column.values().as_ref(), &[20, 10]);
    assert!(cursor.next_batch().await.unwrap().is_none());
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
        let offsets = rows(session.query_sql("SELECT log_offset FROM xt.source_log_msgs WHERE msg_id BETWEEN 0 AND 100 AND msg LIKE '%:type :tx%' ORDER BY log_offset", vec![]).await.unwrap()).await;
        assert_eq!(offsets, [vec![Value::I64(0)], vec![Value::I64(1)]]);
        assert!(rows(session.query_sql("SELECT * FROM xt.source_log_msgs WHERE msg_id BETWEEN 99999 AND 99999", vec![]).await.unwrap()).await.is_empty());
        let error = match session.query_sql("SELECT msg_id FROM xt.source_log_msgs", vec![]).await { Ok(_) => panic!("unbounded inspection succeeded"), Err(error) => error };
        assert_eq!(error.code, "xtdb/missing-log-bounds");
        stop(&node, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn typed_table_declarations_survive_a_block_without_application_rows() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let declare = TxOp::Sql {
        sql: "CREATE TABLE invoices (_id TEXT, body TEXT, effective TIMESTAMPTZ, amount BIGINT, extra)".into(),
        args: None,
    };
    let committed = session.state_mut().execute_tx(vec![declare], AutonomousTxOptions::default()).await.unwrap();
    assert!(committed.committed());

    let table = TableRef::new("public", "invoices");
    let database = node.catalog().database("xtdb").unwrap();
    let original = database.snapshot().column_types(&table);
    use rtdb_types::{LogicalType, ScalarType, TimeUnit};
    let expected = [
        ("body", LogicalType::Scalar(ScalarType::Utf8)),
        ("effective", LogicalType::Scalar(ScalarType::Timestamp { timezone: Some("UTC".into()), unit: TimeUnit::Microsecond })),
        ("amount", LogicalType::Scalar(ScalarType::I64)),
    ];
    for (name, ty) in expected {
        assert!(original[name].legs().contains(&ty));
    }
    assert_eq!(original["extra"], LogicalType::Nothing);
    assert!(rows(session.query_sql("SELECT body, effective, amount FROM invoices", vec![]).await.unwrap()).await.is_empty());

    node.flush_all(Duration::from_secs(10)).await.unwrap();
    assert_eq!(database.snapshot().column_types(&table), original);
    stop(&node, driver).await;
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

#[tokio::test]
async fn sql_snapshot_tokens_are_xtdb_time_bases_read_at_the_redeeming_clock() {
    let services = NodeServices {
        resources: Arc::new(NativeResources::default().with_log_clock({ let clock = Arc::new(AtomicI64::new(1_000_000)); Arc::new(move || Ok(Timestamp(clock.fetch_add(1_000, Ordering::Relaxed)))) })),
        ..services()
    };
    let (node, driver) = running(options(), services).await;
    let mut writer = node.session("xtdb", Arc::new(Authority)).unwrap();
    let first = writer.state_mut().execute_operation(put(&node, 1, 10)).await.unwrap().unwrap().tx_key();
    let mut reader = node.session("xtdb", Arc::new(Authority)).unwrap();
    let original = rows(reader.query_sql("SELECT value, SNAPSHOT_TOKEN FROM docs FOR ALL VALID_TIME", vec![]).await.unwrap()).await;
    let Value::Utf8(token) = original[0][1].clone() else { panic!("expected a snapshot token"); };
    let expected = rtdb_format::basis::encode_time_basis(&rtdb_types::TimeBasis {
        databases: BTreeMap::from([("xtdb".to_owned(), vec![Some(first.system_time.into())])]),
    });
    assert_eq!(token, expected, "the token is XTDB's base64 protobuf TimeBasis");
    let next = writer.state_mut().execute_operation(put(&node, 1, 20)).await.unwrap().unwrap().tx_key();
    // A time basis names system times only, as XTDB's, so the reopened read needs the second transaction later.
    assert!(next.system_time > first.system_time);
    let live = rows(reader.query_sql("SELECT value FROM docs", vec![]).await.unwrap()).await;
    assert_eq!(live, [vec![Value::I64(20)]]);
    let reopened = rows(reader.query_sql(
        "SETTING SNAPSHOT_TOKEN=? SELECT value, SNAPSHOT_TOKEN FROM docs FOR ALL VALID_TIME",
        vec![Value::Utf8(token.clone())],
    ).await.unwrap()).await;
    assert_eq!(reopened, original);
    let invalid = match reader.query_sql("SETTING SNAPSHOT_TOKEN=? SELECT value FROM docs", vec![Value::Utf8("rtdb-snapshot-v3:e30".into())]).await {
        Ok(cursor) => rows_result(cursor).await.unwrap_err(),
        Err(error) => error,
    };
    assert_eq!((invalid.category, invalid.code.as_str()), (rtdb_types::AnomalyCategory::Incorrect, "xtdb.basis/invalid-basis"));
    reader.state_mut().begin_with(TransactionSettings {
        access: Some(AccessSettings::ReadOnly(ReadOnlySettings { snapshot_token: OptionUpdate::Set(token), ..Default::default() })),
        ..Default::default()
    }).unwrap();
    let reopened = rows(reader.query_sql("SELECT value, SNAPSHOT_TOKEN, CURRENT_TIMESTAMP FROM docs FOR ALL VALID_TIME", vec![]).await.unwrap()).await;
    assert_eq!(reopened.iter().map(|row| row[..2].to_vec()).collect::<Vec<_>>(), original);
    assert_ne!(reopened[0][2], Value::Null, "the clock comes from the redeeming session, not the token");
    stop(&node, driver).await;
}

#[tokio::test]
async fn transactions_in_one_microsecond_get_distinct_system_times_that_tokens_name_unambiguously() {
    let services = NodeServices {
        resources: Arc::new(NativeResources::default().with_log_clock(Arc::new(|| Ok(Timestamp(1_000_000))))),
        ..services()
    };
    let (node, driver) = running(options(), services).await;
    let mut writer = node.session("xtdb", Arc::new(Authority)).unwrap();
    let first = writer.state_mut().execute_operation(put(&node, 1, 10)).await.unwrap().unwrap().tx_key();
    let mut reader = node.session("xtdb", Arc::new(Authority)).unwrap();
    let original = rows(reader.query_sql("SELECT value, SNAPSHOT_TOKEN FROM docs", vec![]).await.unwrap()).await;
    let Value::Utf8(token) = original[0][1].clone() else { panic!("expected a snapshot token"); };
    // As XTDB's TxResolver: the frozen log's second transaction steps one microsecond past the first.
    let next = writer.state_mut().execute_operation(put(&node, 1, 20)).await.unwrap().unwrap().tx_key();
    assert_eq!(next.system_time, Timestamp(first.system_time.0 + 1));
    let reopened = rows(reader.query_sql(
        "SETTING SNAPSHOT_TOKEN=? SELECT value FROM docs", vec![Value::Utf8(token)],
    ).await.unwrap()).await;
    assert_eq!(reopened, [vec![Value::I64(10)]], "the first transaction's token never reads its successor");
    assert_eq!(rows(reader.query_sql("SELECT value FROM docs", vec![]).await.unwrap()).await, [vec![Value::I64(20)]]);
    stop(&node, driver).await;
}

async fn rows_result(mut cursor: QueryCursor) -> Result<()> {
    while cursor.next_batch().await?.is_some() {}
    Ok(())
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
async fn bitemporal_sql_matches_ordered_history_through_flush_compaction_and_restart() {
    #[derive(Clone, Copy)]
    enum Mutation { Put(i64), Delete, Erase }

    struct Event { id: i64, system: i64, from: i64, to: i64, mutation: Mutation }

    impl Event {
        fn operation(&self, node: &Node) -> TxOp {
            let table = TableRef::new("public", "docs");
            let valid_from = Some(Timestamp(self.from));
            let valid_to = Some(Timestamp(self.to));
            let ids = || EntityIds::new(vec![Value::I64(self.id).entity_id().unwrap()], node.memory()).unwrap();
            match self.mutation {
                Mutation::Put(value) => {
                    let document = StructFields::from([("_id".into(), Value::I64(self.id)), ("value".into(), Value::I64(value))]);
                    let documents = Documents::new(vec![document], node.memory()).unwrap();
                    TxOp::PutDocs { table, documents, valid_from, valid_to }
                }
                Mutation::Delete => TxOp::DeleteDocs { table, ids: ids(), valid_from, valid_to },
                Mutation::Erase => TxOp::EraseDocs { table, ids: ids() },
            }
        }
    }

    async fn check(session: &mut NativeSession, history: &[Event], phase: &str) {
        for system in [99_999, 100_000, 200_000, 300_000, 400_000, 500_000, 600_000] {
            for valid in [0, 100_000, 199_999, 200_000, 399_999, 400_000, 500_000, 600_000] {
                let mut visible = BTreeMap::new();
                for event in history {
                    let covers = event.system <= system && event.from <= valid && valid < event.to;
                    match event.mutation {
                        Mutation::Erase => { visible.remove(&event.id); }
                        Mutation::Put(value) if covers => { visible.insert(event.id, value); }
                        Mutation::Delete if covers => { visible.remove(&event.id); }
                        _ => {}
                    }
                }
                for minimum in [None, Some(10)] {
                    let predicate = minimum.map_or("", |_| " WHERE value >= 10");
                    let sql = format!(
                        "SELECT _id, value FROM docs FOR VALID_TIME AS OF TIMESTAMP '1970-01-01 00:00:00.{valid:06}+00:00' \
                         FOR SYSTEM_TIME AS OF TIMESTAMP '1970-01-01 00:00:00.{system:06}+00:00'{predicate} ORDER BY _id"
                    );
                    let expected: Vec<_> = visible.iter()
                        .filter(|(_, value)| minimum.is_none_or(|lower| **value >= lower))
                        .map(|(&id, &value)| vec![Value::I64(id), Value::I64(value)])
                        .collect();
                    let cursor = session.query_sql(&sql, vec![]).await.unwrap();
                    assert_eq!(rows(cursor).await, expected, "{phase}: {sql}");
                }
            }
        }
    }

    let events = [
        Event { id: 1, system: 100_000, from: 0, to: 600_000, mutation: Mutation::Put(12) },
        Event { id: 1, system: 200_000, from: 200_000, to: 400_000, mutation: Mutation::Put(9) },
        Event { id: 1, system: 300_000, from: 400_000, to: 500_000, mutation: Mutation::Delete },
        Event { id: 2, system: 400_000, from: 0, to: 600_000, mutation: Mutation::Put(20) },
        Event { id: 2, system: 500_000, from: 0, to: 600_000, mutation: Mutation::Erase },
        Event { id: 3, system: 600_000, from: 150_000, to: 650_000, mutation: Mutation::Put(30) },
    ];

    for bypass in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let store = directory.path().join("store");
        let configuration = DatabaseConfig {
            log: Some(database_config::Log::LocalLog(LocalLog { path: Some(directory.path().join("log").to_string_lossy().into_owned()), epoch: Some(0) })),
            storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: Some(store.to_string_lossy().into_owned()), epoch: Some(0) })),
            ..Default::default()
        };
        let mut settings = options();
        settings.membership = Membership::Durable { primary: configuration, skipped: BTreeSet::new() };
        settings.query.bypass_temporal_pruning = bypass;
        let clock = Arc::new(ReplayTestClock(AtomicI64::new(events[0].system)));
        let runtime = NodeServices { clock: clock.clone(), ..services() };
        let (node, driver) = running(settings.clone(), fresh_local_services(&runtime)).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        let database = node.catalog().database("xtdb").unwrap();
        for (index, event) in events.iter().enumerate() {
            clock.0.store(event.system, Ordering::SeqCst);
            let outcome = session.state_mut().execute_tx(vec![event.operation(&node)], AutonomousTxOptions::default()).await.unwrap();
            assert!(outcome.committed(), "{:?}", outcome.error());
            assert_eq!(outcome.tx_key().system_time, Timestamp(event.system));
            let prefix = &events[..=index];
            if index == 2 { check(&mut session, prefix, "live").await; }
            if index == 2 || index == 4 {
                database.flush().await.unwrap();
                node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
            }
            if index == 4 {
                check(&mut session, prefix, "flushed").await;
                database.compaction().unwrap().compact_all().await.unwrap();
                node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
                let data = std::fs::read_dir(store.join("v06/tables/public$docs/data")).unwrap();
                assert!(data.map(|entry| entry.unwrap()).any(|entry| entry.file_name().to_string_lossy().starts_with("l01")));
                check(&mut session, prefix, "compacted").await;
            }
        }
        check(&mut session, &events, "compacted with live tail").await;
        drop(session);
        drop(database);
        stop(&node, driver).await;
        drop(node);

        let (reopened, driver) = running(settings, fresh_local_services(&runtime)).await;
        reopened.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
        let mut session = reopened.session("xtdb", Arc::new(Authority)).unwrap();
        check(&mut session, &events, "restarted").await;
        stop(&reopened, driver).await;
    }
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
        drop(session);
        let cut = node.catalog().database("xtdb").unwrap().snapshot().catalog_cut().unwrap();
        let ledger = rtdb_storage::LocalStore::open(directory.path().join("store/v06")).unwrap().with_catalog_ledger(cut.replica_epoch);
        assert!(rtdb_storage::ObjectStore::advance_catalog_floor(&ledger, cut).await.unwrap());
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
        let empty = TxOp::Sql { sql: "CREATE TABLE empty_history (value BIGINT)".into(), args: None };
        assert!(session.state_mut().execute_tx(vec![empty], AutonomousTxOptions::default()).await.unwrap().committed());
        node.catalog().database("xtdb").unwrap().flush().await.unwrap();
        node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
        node.catalog().database("xtdb").unwrap().compaction().unwrap().compact_all().await.unwrap();
        node.catalog().database("xtdb").unwrap().flush().await.unwrap();
        node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
        assert!(node.snapshot_export("xtdb", Timestamp(0)).await.is_err());
        drop(session);
        stop(&node, driver).await;
        drop(node);

        settings.replication.enabled = false;
        settings.compaction_threads = 0;
        let (node, driver) = running(settings.clone(), fresh_local_services(&runtime_services)).await;
        let plan = node.snapshot_export("xtdb", Timestamp(0)).await.unwrap().unwrap();
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
        assert!(rows(session.query_sql("SELECT value FROM empty_history", vec![]).await.unwrap()).await.is_empty());
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
    assert!(node.snapshot_export("xtdb", Timestamp(0)).await.unwrap().is_none());
    stop(&node, driver).await;
}

#[tokio::test]
async fn compactor_reset_preserves_checkpoint_history_and_resumes_retirement_after_reader_drain() {
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
        let cut = node.catalog().database("xtdb").unwrap().snapshot().catalog_cut().unwrap();
        let ledger = rtdb_storage::LocalStore::open(store.join("v06")).unwrap().with_catalog_ledger(cut.replica_epoch);
        assert!(rtdb_storage::ObjectStore::advance_catalog_floor(&ledger, cut).await.unwrap());
        stop(&node, driver).await;
        drop(node);

        settings.compaction_threads = 0;
        let (node, driver) = running(settings.clone(), fresh_local_services(&runtime_services)).await;
        let cancellation = CancellationToken::new();
        let initial = node.prepare_compactor_reset("xtdb", &cancellation).await.unwrap();
        let table = TableRef::new("public", "docs");
        let snapshot = node.catalog().database("xtdb").unwrap().snapshot();
        let slug = snapshot.table_catalog().entry(&table).unwrap().slug.clone();
        drop(snapshot);
        let key = &initial.replacement_tries()[&table][0].key;
        let storage_root = store.join("v06");
        let required = storage_root.join(slug.data_key(key));
        let saved = required.with_extension("backup");
        std::fs::rename(&required, &saved).unwrap();
        let refused = node.prepare_compactor_reset("xtdb", &cancellation).await.err().unwrap();
        assert_eq!(refused.code, "rtdb.compactor-reset/incomplete-inventory");
        std::fs::rename(saved, required).unwrap();
        for compacted in initial.files_to_delete() { assert!(storage_root.join(compacted.as_str()).is_file()); }
        drop(initial);

        let orphan = rtdb_format::keys::TrieKey::l0(rtdb_types::BlockIndex(256)).unwrap();
        std::fs::write(storage_root.join(slug.data_key(&orphan)), b"unpublished").unwrap();
        std::fs::write(storage_root.join(slug.metadata_key(&orphan)), b"unpublished").unwrap();
        let plan = node.prepare_compactor_reset("xtdb", &cancellation).await.unwrap();
        assert!(!plan.replacement_tries()[&table].iter().any(|try_| try_.key == orphan));
        let compacted = plan.files_to_delete().to_vec();
        assert!(!compacted.is_empty());
        assert!(plan.replacement_tries().values().flatten().any(|trie| trie.key.level() > 0));
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
        let database = node.catalog().database("xtdb").unwrap();
        let cut = database.snapshot().catalog_cut().unwrap();
        // A reader of the pre-reset catalog registers nothing with storage and does not defer the reset's release.
        let old_reader = database.snapshot();
        let published = plan.apply(&CancellationToken::new()).await.unwrap();
        assert!(database.status().adopted_block.as_ref().is_some_and(|block| block.block_index() >= published));
        for key in &compacted { assert!(storage_root.join(key.as_str()).is_file()); }
        let pending = rtdb_format::trie_retirement::journal_directory(cut) + "/reset-pending";
        assert!(std::fs::read_dir(storage_root.join(&pending)).unwrap().next().is_some());
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        assert_eq!(rows(session.query_sql(sql, vec![]).await.unwrap()).await, before);
        drop((session, database));
        stop(&node, driver).await;
        drop(node);

        settings.compaction_threads = 1;
        settings.automatic_gc = true;
        settings.garbage.blocks_to_keep = usize::MAX;
        let (node, driver) = running(settings, fresh_local_services(&runtime_services)).await;
        let database = node.catalog().database("xtdb").unwrap();
        let mut status = database.watch_status();
        tokio::time::timeout(Duration::from_secs(10), status.wait_for(|state| {
            state.installed.is_some() && state.adopted_block.as_ref().is_some_and(|block| block.block_index() >= published)
        })).await.unwrap().unwrap();
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        assert_eq!(rows(session.query_sql(sql, vec![]).await.unwrap()).await, before);
        assert_eq!(database.snapshot().trie_catalog().naming_epoch(), 1, "the reset's block carries the new naming epoch through restart");
        tokio::time::timeout(Duration::from_secs(10), async {
            while compacted.iter().any(|key| storage_root.join(key.as_str()).exists()) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.unwrap();
        assert_eq!(database.status().adopted_block.as_ref().unwrap().block_index(), published);
        assert!(old_reader.catalog_cut().is_some_and(|captured| captured == cut));
        let data_directory = storage_root.join("tables").join(slug.as_str()).join("data");
        let recovered: BTreeSet<_> = std::fs::read_dir(&data_directory).unwrap()
            .map(|entry| entry.unwrap().file_name()).collect();
        session.state_mut().execute_operation(put(&node, 2, 30)).await.unwrap();
        database.flush().await.unwrap();
        node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
        database.compaction().unwrap().compact_all().await.unwrap();
        let mut expected = before;
        expected.push(vec![Value::I64(30)]);
        assert_eq!(rows(session.query_sql(sql, vec![]).await.unwrap()).await, expected);
        let mut recompacted = Vec::new();
        for entry in std::fs::read_dir(data_directory).unwrap() {
            let name = entry.unwrap().file_name();
            if recovered.contains(&name) { continue; }
            let name = name.to_str().unwrap();
            let Some(stem) = name.strip_suffix(".arrow") else { continue; };
            let key: rtdb_format::keys::TrieKey = stem.parse().unwrap();
            if key.level() != 0 { recompacted.push(key); }
        }
        assert!(!recompacted.is_empty(), "compaction ran again after the reset");
        for key in &recompacted {
            assert_eq!(key.epoch(), 1, "{key} is named in the reset's epoch");
            let name = slug.data_key(key);
            assert!(compacted.iter().all(|old| old.as_str() != name), "{name} reuses a name published before the reset");
        }
        drop(session);
        stop(&node, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn a_trusted_checkpoint_and_replica_journal_rebuild_after_l0_is_removed() {
    use rtdb_format::keys::TrieKey;

    let directory = tempfile::tempdir().unwrap();
    let source_storage = directory.path().join("source");
    let source_log = directory.path().join("log");
    let primary = DatabaseConfig {
        log: Some(database_config::Log::LocalLog(LocalLog { path: Some(source_log.display().to_string()), epoch: Some(0) })),
        storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: Some(source_storage.display().to_string()), epoch: Some(0) })),
        ..Default::default()
    };
    let mut settings = options();
    settings.membership = Membership::Durable { primary: primary.clone(), skipped: BTreeSet::new() };
    let runtime = services();
    let (node, driver) = running(settings.clone(), fresh_local_services(&runtime)).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    let cancellation = CancellationToken::new();
    let flush = async |database: &Database| {
        let mut status = database.watch_status();
        let position = database.flush().await.unwrap();
        let adopted = tokio::time::timeout(Duration::from_secs(10), status.wait_for(|status| {
            status.adopted_block.as_ref().and_then(|block| block.latest_source_position()).is_some_and(|source| source >= position)
        })).await.unwrap().unwrap();
        adopted.adopted_block.clone().unwrap()
    };
    session.state_mut().execute_operation(put(&node, 1, 10)).await.unwrap();
    let first = flush(&database).await;
    database.compaction().unwrap().compact_all().await.unwrap();
    let base = flush(&database).await;
    assert!(base.block_index() > first.block_index());
    let table = TableRef::new("public", "docs");
    let snapshot = database.snapshot();
    let slug = snapshot.table_catalog().entry(&table).unwrap().slug.clone();
    assert!(snapshot.trie_catalog().current_tries(&table).iter().all(|try_| try_.key.level() > 0));
    drop(snapshot);
    drop((session, database));
    stop(&node, driver).await;
    drop(node);

    let mut offline = settings.clone();
    offline.replication.enabled = false;
    offline.compaction_threads = 0;
    let (exporter, driver) = running(offline, fresh_local_services(&runtime)).await;
    let export = exporter.snapshot_export("xtdb", Timestamp(0)).await.unwrap().unwrap();
    assert_eq!(export.block_index(), base.block_index());
    export.copy(&cancellation).await.unwrap();
    let original_base_files = export.files().to_vec();
    let trusted_base = directory.path().join("known-good");
    std::fs::rename(source_storage.join("v06").join(export.directory().as_str()), &trusted_base).unwrap();
    drop(export);
    stop(&exporter, driver).await;
    drop(exporter);

    let (node, driver) = running(settings.clone(), fresh_local_services(&runtime)).await;
    let database = node.catalog().database("xtdb").unwrap();
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    session.state_mut().execute_operation(put(&node, 1, 20)).await.unwrap();
    session.state_mut().execute_operation(put(&node, 2, 30)).await.unwrap();
    let middle = flush(&database).await;
    session.state_mut().execute_operation(put(&node, 3, 40)).await.unwrap();
    let through = flush(&database).await;
    let sql = "SELECT _id, value FROM docs FOR ALL SYSTEM_TIME ORDER BY _id, value";
    let before = rows(session.query_sql(sql, vec![]).await.unwrap()).await;
    let mut repair = LocalRepair {
        source_storage: source_storage.clone(), base_storage: Some(trusted_base), replica_log: source_log.join("REPLICA_LOG"), output_parent: directory.path().to_owned(),
        base: RepairCheckpoint { block: base.block_index(), sha256: base.publication_sha256().unwrap() },
        through: RepairCheckpoint { block: through.block_index(), sha256: through.publication_sha256().unwrap() },
    };
    let memory = rtdb_columnar::MemoryBudget::new(512 << 20);
    let refusal = repair.rebuild(memory.clone(), &cancellation).await.unwrap_err();
    assert_eq!(refusal.code, "rtdb.repair/source-unavailable");
    drop((session, database));
    stop(&node, driver).await;
    drop(node);

    for file in original_base_files {
        std::fs::remove_file(source_storage.join("v06").join(file.as_str())).unwrap();
    }
    for block in [first.block_index(), middle.block_index(), through.block_index()] {
        let key = TrieKey::l0(block).unwrap();
        std::fs::remove_file(source_storage.join("v06").join(slug.data_key(&key))).unwrap();
        std::fs::remove_file(source_storage.join("v06").join(slug.metadata_key(&key))).unwrap();
    }
    let receipt = repair.base;
    repair.base.sha256[0] ^= 1;
    let refusal = repair.rebuild(memory.clone(), &cancellation).await.unwrap_err();
    assert_eq!(refusal.code, "rtdb.repair/invalid-checkpoint");
    repair.base = receipt;
    let receipt = repair.through;
    repair.through.sha256[0] ^= 1;
    let refusal = repair.rebuild(memory.clone(), &cancellation).await.unwrap_err();
    assert_eq!(refusal.code, "rtdb.repair/invalid-checkpoint");
    repair.through = receipt;

    let original_journal = repair.replica_log.clone();
    repair.replica_log = directory.path().join("truncated");
    let target_offset = through.boundary_replica_position().unwrap().after_offset(rtdb_types::StorageEpoch(0));
    let mut replica = std::fs::File::open(&original_journal).unwrap();
    let mut truncated = std::fs::File::create(&repair.replica_log).unwrap();
    let copied = std::io::copy(&mut std::io::Read::take(&mut replica, target_offset.try_into().unwrap()), &mut truncated).unwrap();
    assert_eq!(i64::try_from(copied).unwrap(), target_offset);
    drop(truncated);
    let refusal = repair.rebuild(memory.clone(), &cancellation).await.unwrap_err();
    assert_eq!(refusal.code, "rtdb.repair/source-unavailable");
    repair.replica_log = original_journal;

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let refusal = repair.rebuild(memory.clone(), &cancelled).await.unwrap_err();
    assert_eq!(refusal.code, "rtdb.repair/cancelled");
    assert!(!directory.path().read_dir().unwrap().any(|entry| entry.unwrap().file_name().to_string_lossy().starts_with("repair-")));

    let rebuilt = repair.rebuild(memory, &cancellation).await.unwrap();
    assert_eq!(rebuilt.latest_transaction, through.latest_completed_tx().unwrap());
    assert_eq!(rebuilt.checkpoint.block, through.block_index());
    let latest_root = rebuilt.path.join("v06").join(rtdb_format::keys::block_key(rebuilt.checkpoint.block).unwrap());
    assert_eq!(rtdb_format::catalog::publication_digest(&std::fs::read(latest_root).unwrap()), rebuilt.checkpoint.sha256);
    for block in [base.block_index(), middle.block_index()] {
        assert!(!rebuilt.path.join("v06").join(rtdb_format::keys::block_key(block).unwrap()).exists());
        assert!(!rebuilt.path.join("v06").join(slug.block_key(block).unwrap()).exists());
    }

    let reopened = DatabaseConfig {
        log: Some(database_config::Log::LocalLog(LocalLog {
            path: Some(directory.path().join("new-log").display().to_string()), epoch: Some(1),
        })),
        storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: Some(rebuilt.path.display().to_string()), epoch: Some(0) })),
        ..primary
    };
    settings.membership = Membership::Durable { primary: reopened, skipped: BTreeSet::new() };
    let (replacement, driver) = running(settings, fresh_local_services(&runtime)).await;
    replacement.sync_all(Duration::from_secs(10), &cancellation).await.unwrap();
    let mut session = replacement.session("xtdb", Arc::new(Authority)).unwrap();
    assert_eq!(rows(session.query_sql(sql, vec![]).await.unwrap()).await, before);
    let transaction = session.state_mut().execute_operation(put(&replacement, 3, 50)).await.unwrap().unwrap();
    assert!(transaction.tx_key().tx_id > rebuilt.latest_transaction.tx_id);
    assert_eq!(rows(session.query_sql("SELECT value FROM docs WHERE _id = 3", vec![]).await.unwrap()).await, vec![vec![Value::I64(50)]]);
    drop(session);
    stop(&replacement, driver).await;
}

#[tokio::test]
async fn an_installed_repair_base_keeps_older_targets_and_offloaded_replay_through_collection() {
    use rtdb_format::{catalog::BlockCatalog, keys::{block_key, oversized_block_index}};

    let directory = tempfile::tempdir().unwrap();
    let source_storage = directory.path().join("source");
    let objects = source_storage.join("v06");
    let source_log = directory.path().join("log");
    let replica_log = source_log.join("REPLICA_LOG");
    let primary = DatabaseConfig {
        log: Some(database_config::Log::LocalLog(LocalLog { path: Some(source_log.display().to_string()), epoch: Some(0) })),
        storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: Some(source_storage.display().to_string()), epoch: Some(0) })),
        ..Default::default()
    };
    let mut settings = options();
    settings.membership = Membership::Durable { primary: primary.clone(), skipped: BTreeSet::new() };
    settings.garbage.blocks_to_keep = 1;
    let runtime = services();
    let native = || {
        let mut resources = NativeResources::default().with_local_log_clock(Arc::clone(&runtime.clock), false);
        resources.maximum_log_message_bytes = 16 << 10;
        NodeServices { resources: Arc::new(resources), ..runtime.clone() }
    };
    let cancellation = CancellationToken::new();
    let memory = rtdb_columnar::MemoryBudget::new(512 << 20);
    let flush = async |database: &Database| {
        let mut status = database.watch_status();
        let position = database.flush().await.unwrap();
        let adopted = tokio::time::timeout(Duration::from_secs(10), status.wait_for(|status| {
            status.adopted_block.as_ref().and_then(|block| block.latest_source_position()).is_some_and(|source| source >= position)
        })).await.unwrap().unwrap();
        adopted.adopted_block.clone().unwrap()
    };
    let receipt = |catalog: &BlockCatalog| RepairCheckpoint {
        block: catalog.block_index(), sha256: catalog.publication_sha256().unwrap(),
    };
    let (node, driver) = running(settings.clone(), native()).await;
    let database = node.catalog().database("xtdb").unwrap();
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    session.state_mut().execute_operation(put(&node, -2, -2)).await.unwrap();
    let old = flush(&database).await;
    session.state_mut().execute_operation(put(&node, -1, -1)).await.unwrap();
    let base = flush(&database).await;
    drop((session, database));
    stop(&node, driver).await;
    drop(node);

    let mut offline = settings.clone();
    offline.replication.enabled = false;
    offline.compaction_threads = 0;
    let export = async |label: &str| {
        let (exporter, driver) = running(offline.clone(), native()).await;
        let plan = exporter.snapshot_export("xtdb", Timestamp(0)).await.unwrap().unwrap();
        plan.copy(&cancellation).await.unwrap();
        let detached = directory.path().join(label);
        std::fs::rename(objects.join(plan.directory().as_str()), &detached).unwrap();
        drop(plan);
        stop(&exporter, driver).await;
        detached
    };
    let trusted_base = export("base").await;
    let mut hold = RetainRepairBase {
        source_storage: source_storage.clone(), replica_log: replica_log.clone(),
        base_storage: trusted_base.clone(), base: receipt(&base), classic_key_vault: None,
    };
    hold.base.sha256[0] ^= 1;
    assert_eq!(hold.install(memory.clone(), &cancellation).await.unwrap_err().code, "rtdb.repair/invalid-checkpoint");
    hold.base = receipt(&base);
    let slug = base.table_entries().unwrap().into_iter().find(|entry| entry.table.table == "docs").unwrap().slug;
    let trie = rtdb_format::keys::TrieKey::l0(base.block_index()).unwrap();
    let relative = slug.data_key(&trie);
    let version = rtdb_format::keys::StorageRoot::new(
        rtdb_format::keys::CURRENT_STORAGE_VERSION, rtdb_types::StorageEpoch(0),
    ).unwrap().key();
    let detached_data = trusted_base.join(version).join(relative);
    let original_data = std::fs::read(&detached_data).unwrap();
    let mut changed_data = original_data.clone();
    changed_data[0] ^= 1;
    std::fs::write(&detached_data, changed_data).unwrap();
    assert_eq!(hold.install(memory.clone(), &cancellation).await.unwrap_err().code, "rtdb.repair/invalid-checkpoint");
    std::fs::write(&detached_data, original_data).unwrap();
    assert_eq!(hold.install(memory.clone(), &cancellation).await.unwrap(), base.boundary_replica_position().unwrap());

    settings.automatic_gc = true;
    let (node, driver) = running(settings.clone(), native()).await;
    let database = node.catalog().database("xtdb").unwrap();
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let result = session.state_mut().execute_operation(TxOp::Sql {
        sql: "INSERT INTO docs (_id, value) SELECT x AS _id, x AS value FROM GENERATE_SERIES(1, 3000) AS g(x)".into(),
        args: None,
    }).await.unwrap().unwrap();
    assert!(result.committed());
    let through = flush(&database).await;
    let payload = std::fs::read_dir(objects.join("oversized")).unwrap().map(|entry| entry.unwrap().path())
        .find(|path| oversized_block_index(path.to_str().unwrap()) == Some(through.block_index())).unwrap();
    let sql = "SELECT count(*) FROM docs";
    let expected = rows(session.query_sql(sql, vec![]).await.unwrap()).await;
    assert_eq!(expected, vec![vec![Value::I64(3002)]]);
    session.state_mut().execute_operation(put(&node, -3, -3)).await.unwrap();
    let replacement = flush(&database).await;
    let await_collection = async |path: &std::path::Path| {
        tokio::time::timeout(Duration::from_secs(10), async {
            while path.exists() { tokio::time::sleep(Duration::from_millis(5)).await; }
        }).await.unwrap();
    };
    await_collection(&objects.join(block_key(old.block_index()).unwrap())).await;
    let root = objects.join(block_key(through.block_index()).unwrap());
    assert!(root.exists());
    for entry in through.table_entries().unwrap() {
        assert!(objects.join(entry.slug.block_key(through.block_index()).unwrap()).exists());
    }
    assert!(payload.exists());
    drop((session, database));
    stop(&node, driver).await;
    drop(node);

    let repair = RetainedLocalRepair {
        source_storage: source_storage.clone(), base_storage: trusted_base,
        replica_log, output_parent: directory.path().to_owned(), through: receipt(&through),
    };
    let anchor_path = objects.join(".tmp/catalog-ledger/repair-anchor");
    let anchor = std::fs::read(&anchor_path).unwrap();
    let (block, boundary, digest, inventory): (i64, i64, [u8; 32], [u8; 32]) = serde_json::from_slice(&anchor).unwrap();
    std::fs::write(&anchor_path, serde_json::to_vec(&(block, boundary + 1, digest, inventory)).unwrap()).unwrap();
    let refusal = repair.rebuild(memory.clone(), &cancellation).await
        .expect_err("a retained base must bind the boundary of the detached export");
    assert_eq!(refusal.message.as_deref(), Some("The retained repair boundary differs from its exported checkpoint"));
    std::fs::write(&anchor_path, &anchor).unwrap();
    let rebuilt = repair.rebuild(memory.clone(), &cancellation).await.unwrap();
    assert_eq!(rebuilt.latest_transaction, through.latest_completed_tx().unwrap());
    let mut reopened = settings.clone();
    reopened.membership = Membership::Durable {
        primary: DatabaseConfig {
            log: Some(database_config::Log::LocalLog(LocalLog {
                path: Some(directory.path().join("replacement-log").display().to_string()), epoch: Some(1),
            })),
            storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: Some(rebuilt.path.display().to_string()), epoch: Some(0) })),
            ..primary
        },
        skipped: BTreeSet::new(),
    };
    let (reader, driver) = running(reopened, native()).await;
    let mut reader_session = reader.session("xtdb", Arc::new(Authority)).unwrap();
    assert_eq!(rows(reader_session.query_sql(sql, vec![]).await.unwrap()).await, expected);
    let repaired_database = reader.catalog().database("xtdb").unwrap();
    assert!(repaired_database.snapshot().catalog_cut().is_none());
    let adopted = flush(&repaired_database).await;
    let catalog_cut = repaired_database.snapshot().catalog_cut().unwrap();
    assert_eq!(catalog_cut.replica_epoch, rtdb_types::StorageEpoch(1));
    assert!(catalog_cut.through >= adopted.boundary_replica_position().unwrap());
    drop(repaired_database);
    drop(reader_session);
    stop(&reader, driver).await;

    let next_base = export("next-base").await;
    assert_eq!(hold.install(memory.clone(), &cancellation).await.unwrap_err().code, "rtdb.repair/invalid-checkpoint");
    hold.base_storage = next_base;
    hold.base = receipt(&replacement);
    hold.install(memory, &cancellation).await.unwrap();

    let (node, driver) = running(settings, native()).await;
    let database = node.catalog().database("xtdb").unwrap();
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    session.state_mut().execute_operation(put(&node, -4, -4)).await.unwrap();
    assert!(flush(&database).await.block_index() > replacement.block_index());
    await_collection(&root).await;
    assert!(payload.exists());
    drop((session, database));
    stop(&node, driver).await;
}

#[tokio::test]
async fn retained_reset_recovers_compacted_base_and_missing_l0_suffix_in_place() {
    use rtdb_format::{catalog::BlockCatalog, keys::{TrieKey, block_key}};

    let directory = tempfile::tempdir().unwrap();
    let storage = directory.path().join("objects");
    let objects = storage.join("v06");
    let journal = directory.path().join("journal");
    let primary = DatabaseConfig {
        log: Some(database_config::Log::LocalLog(LocalLog { path: Some(journal.display().to_string()), epoch: Some(0) })),
        storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: Some(storage.display().to_string()), epoch: Some(0) })),
        ..Default::default()
    };
    let mut settings = options();
    settings.membership = Membership::Durable { primary, skipped: BTreeSet::new() };
    settings.automatic_gc = false;
    let runtime = services();
    let cancel = CancellationToken::new();
    let flush = async |database: &Database| {
        let mut status = database.watch_status();
        let position = database.flush().await.unwrap();
        let adopted = tokio::time::timeout(Duration::from_secs(10), status.wait_for(|status| {
            status.adopted_block.as_ref().and_then(|block| block.latest_source_position()).is_some_and(|source| source >= position)
        })).await.unwrap().unwrap();
        adopted.adopted_block.clone().unwrap()
    };
    let table = TableRef::new("public", "docs");
    let (node, driver) = running(settings.clone(), fresh_local_services(&runtime)).await;
    let database = node.catalog().database("xtdb").unwrap();
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    session.state_mut().execute_operation(put(&node, 1, 10)).await.unwrap();
    flush(&database).await;
    database.compaction().unwrap().compact_all().await.unwrap();
    let base = flush(&database).await;
    assert!(database.snapshot().trie_catalog().current_tries(&table).iter().all(|trie| trie.key.level() > 0));
    drop((session, database));
    stop(&node, driver).await;
    drop(node);

    let mut offline = settings.clone();
    offline.replication.enabled = false;
    offline.compaction_threads = 0;
    let (node, driver) = running(offline, fresh_local_services(&runtime)).await;
    let exported = node.snapshot_export("xtdb", Timestamp(0)).await.unwrap().unwrap();
    exported.copy(&cancel).await.unwrap();
    let detached = directory.path().join("detached");
    std::fs::rename(objects.join(exported.directory().as_str()), &detached).unwrap();
    drop(exported);
    stop(&node, driver).await;
    drop(node);

    RetainRepairBase {
        source_storage: storage, replica_log: journal.join("REPLICA_LOG"),
        base_storage: detached.clone(),
        base: RepairCheckpoint { block: base.block_index(), sha256: base.publication_sha256().unwrap() },
        classic_key_vault: None,
    }.install(rtdb_columnar::MemoryBudget::new(512 << 20), &cancel).await.unwrap();

    let (node, driver) = running(settings.clone(), fresh_local_services(&runtime)).await;
    let database = node.catalog().database("xtdb").unwrap();
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    session.state_mut().execute_operation(put(&node, 1, 20)).await.unwrap();
    let suffix = flush(&database).await;
    database.compaction().unwrap().compact_all().await.unwrap();
    flush(&database).await;
    let slug = database.snapshot().table_entry(&table).unwrap().slug.clone();
    let sql = "SELECT value FROM docs FOR ALL SYSTEM_TIME ORDER BY value";
    let expected = vec![vec![Value::I64(10)], vec![Value::I64(20)]];
    assert_eq!(rows(session.query_sql(sql, vec![]).await.unwrap()).await, expected);
    drop((session, database));
    stop(&node, driver).await;
    drop(node);

    let missing = TrieKey::l0(suffix.block_index()).unwrap();
    let missing_data = objects.join(slug.data_key(&missing));
    std::fs::remove_file(&missing_data).unwrap();
    std::fs::remove_file(objects.join(slug.metadata_key(&missing))).unwrap();

    settings.compaction_threads = 0;
    let (node, driver) = running(settings.clone(), fresh_local_services(&runtime)).await;
    let installed = node.prepare_compactor_reset("xtdb", &cancel).await.unwrap();
    assert!(installed.replacement_tries()[&table].iter().all(|trie| trie.key != missing));
    drop(installed);
    let base_path = detached.join("v06").join(block_key(base.block_index()).unwrap());
    let exact_base = std::fs::read(&base_path).unwrap();
    let mut altered = BlockCatalog::decode(&exact_base).unwrap().as_proto().clone();
    altered.naming_epoch = Some(5);
    std::fs::write(&base_path, BlockCatalog::from_proto(altered).unwrap().encode()).unwrap();
    assert_eq!(node.prepare_compactor_reset_from_retained_base("xtdb", &detached, &cancel).await.err().unwrap().code, "rtdb.repair/invalid-checkpoint");
    std::fs::write(&base_path, exact_base).unwrap();

    let plan = node.prepare_compactor_reset_from_retained_base("xtdb", &detached, &cancel).await.unwrap();
    assert!(plan.replacement_tries()[&table].iter().any(|trie| trie.key.level() > 0));
    assert!(plan.replacement_tries()[&table].iter().any(|trie| trie.key == missing));
    assert!(!missing_data.exists());
    let retired = plan.files_to_delete().to_vec();
    let checkpoint = plan.apply(&cancel).await.unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    assert_eq!(database.snapshot().table_catalog().block_index(), Some(checkpoint));
    assert!(missing_data.is_file());
    let replacement = database.snapshot().trie_catalog().current_tries(&table).into_iter()
        .filter(|trie| trie.key.level() > 0).map(|trie| slug.data_key(&trie.key)).collect::<Vec<_>>();
    assert!(!replacement.is_empty());
    assert!(replacement.iter().all(|file| objects.join(file).is_file() && retired.iter().all(|old| old.as_str() != file)));
    assert!(retired.iter().all(|file| !objects.join(file.as_str()).exists()));
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    assert_eq!(rows(session.query_sql(sql, vec![]).await.unwrap()).await, expected);
    drop((session, database));
    stop(&node, driver).await;
    drop(node);

    let (node, driver) = running(settings, fresh_local_services(&runtime)).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    assert_eq!(rows(session.query_sql(sql, vec![]).await.unwrap()).await, expected);
    let tx = session.state_mut().execute_operation(put(&node, 2, 30)).await.unwrap().unwrap();
    assert!(tx.committed());
    assert_eq!(rows(session.query_sql("SELECT value FROM docs WHERE _id = 2", vec![]).await.unwrap()).await, vec![vec![Value::I64(30)]]);
    drop(session);
    stop(&node, driver).await;
}

#[tokio::test]
async fn compactor_reset_requires_a_completed_transaction() {
    let mut settings = options();
    settings.compaction_threads = 0;
    let (node, driver) = running(settings, services()).await;
    let error = node.prepare_compactor_reset("xtdb", &CancellationToken::new()).await.err().unwrap();
    assert_eq!(error.code, "rtdb.compactor-reset/no-transactions");
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
                audience_storage: None,
                checkpoint_metadata: None, fragment_storage: None,
                store: self.store.clone(), blocks: Default::default(), storage_epoch: rtdb_types::StorageEpoch(0), storage_version: rtdb_format::keys::CURRENT_STORAGE_VERSION,
                io: rtdb_storage::IoRegion::new(),
            })
        }
        async fn open_logs(&self, _: &str, _: &DatabaseConfig, _: LogAccess, _: CancellationToken, _: &DatabaseStorage) -> Result<OpenedLogs> {
            Ok(OpenedLogs { source: self.source.clone(), replica: self.replica.clone(), source_epoch: rtdb_types::StorageEpoch(0),
                replica_epoch: rtdb_types::StorageEpoch(0), replica_read_index: None, placed_transfer: None, await_authority: rtdb_node::AwaitAuthority::SourcePosition, drivers: Vec::new() })
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
        for node in [&writer, &follower] { assert_eq!(node.catalog().database("xtdb").unwrap().block_lag().await.unwrap(), 0, "a caught-up node has no block lag"); }
        assert_eq!(rows(session.query_sql("SELECT value FROM docs", vec![]).await.unwrap()).await, vec![vec![Value::I64(10)]]);
        let mut follower_session = follower.session("xtdb", Arc::new(Authority)).unwrap();
        assert_eq!(rows(follower_session.query_sql("SELECT value FROM docs", vec![]).await.unwrap()).await, vec![vec![Value::I64(10)]]);
        drop((session, follower_session));
        stop(&follower, follower_driver).await;
        stop(&writer, writer_driver).await;
    }).await.unwrap();
}

/// As XTDB, a superseded trie stays for the garbage window (`blocksToKeep` blocks, less `garbageLifetime`) and no longer:
/// a running query on the old leader reads it while it is inside the window, but holds nothing, so once the window
/// passes the successor retires it whether or not that query is still open.
#[tokio::test]
async fn a_successor_retires_an_old_leaders_trie_once_it_leaves_the_garbage_window_whatever_its_queries() {
    use rtdb_format::keys::{CURRENT_STORAGE_VERSION, TrieKey};
    use rtdb_replication::Role;
    use rtdb_storage::{Error, IoRegion, LocalStore, ObjectKey, log::{AppendTimestampSource, MemoryLog}};
    use rtdb_types::StorageEpoch;

    struct SharedJournal {
        source: Arc<MemoryLog>,
        replica: Arc<MemoryLog>,
        storage: std::path::PathBuf,
    }

    #[async_trait]
    impl ResourceFactory for SharedJournal {
        fn validate(&self, _: &str, _: &DatabaseConfig) -> Result<()> { Ok(()) }

        async fn open_storage(&self, _: &str, _: &DatabaseConfig) -> Result<DatabaseStorage> {
            let io = IoRegion::new();
            let store = LocalStore::open_in(&self.storage, Arc::clone(&io))?.with_catalog_ledger(StorageEpoch(0));
            Ok(DatabaseStorage {
                audience_storage: None,
                store: Arc::new(store), blocks: Default::default(), checkpoint_metadata: None, fragment_storage: None,
                storage_epoch: StorageEpoch(0), storage_version: CURRENT_STORAGE_VERSION, io,
            })
        }

        async fn open_logs(&self, _: &str, _: &DatabaseConfig, _: LogAccess, _: CancellationToken, _: &DatabaseStorage) -> Result<OpenedLogs> {
            Ok(OpenedLogs {
                source: self.source.clone(), replica: self.replica.clone(),
                source_epoch: StorageEpoch(0), replica_epoch: StorageEpoch(0),
                replica_read_index: None, placed_transfer: None, await_authority: AwaitAuthority::SourcePosition, drivers: Vec::new(),
            })
        }
    }

    struct JournalClock(Arc<TickClock>);
    impl AppendTimestampSource for JournalClock {
        fn timestamp(&self, _: &[u8], _: Timestamp) -> rtdb_storage::Result<Timestamp> { Ok(self.0.now()) }
    }

    struct CollectorClock { wall: Arc<TickClock>, monotonic: AtomicI64 }
    impl Clock for CollectorClock {
        fn now(&self) -> Timestamp { self.wall.now() }
        fn monotonic_micros(&self) -> i64 { self.monotonic.load(Ordering::Relaxed) }
    }

    async fn flush(node: &Node, database: &Database) {
        let mut status = database.watch_status();
        let next = status.borrow().open_block;
        database.flush().await.unwrap();
        status.wait_for(|state| state.adopted_block.as_ref().is_some_and(|block| block.block_index() >= next)).await.unwrap();
        node.sync_all(Duration::from_secs(5), node.cancellation()).await.unwrap();
    }

    tokio::time::timeout(Duration::from_secs(30), async {
        let files = tempfile::tempdir().unwrap();
        let wall = Arc::new(TickClock(AtomicI64::new(1_000_000)));
        let journal = || Arc::new(MemoryLog::with_timestamp_source(1 << 26, Arc::new(JournalClock(wall.clone()))));
        let shared = Arc::new(SharedJournal { source: journal(), replica: journal(), storage: files.path().join("objects") });
        let old_clock = Arc::new(CollectorClock { wall: wall.clone(), monotonic: AtomicI64::new(0) });
        let new_clock = Arc::new(CollectorClock { wall: wall.clone(), monotonic: AtomicI64::new(9_000_000) });
        let lifetime = Duration::from_secs(60 * 60);
        let mut config = options();
        config.compaction_threads = 1;
        config.garbage.blocks_to_keep = 1;
        config.garbage.garbage_lifetime = chrono::TimeDelta::from_std(lifetime).unwrap();

        let mut predecessor_config = config.clone();
        predecessor_config.replication.idle_heartbeat = false;
        predecessor_config.replication.claim_wait = Some(Duration::from_secs(60));
        let (predecessor, predecessor_driver) = running(predecessor_config, NodeServices {
            resources: shared.clone(), clock: old_clock, ..services()
        }).await;
        let previous = predecessor.catalog().database("xtdb").unwrap();
        let mut previous_status = previous.watch_status();
        let mut reader = predecessor.session("xtdb", Arc::new(Authority)).unwrap();
        reader.state_mut().execute_operation(put(&predecessor, 1, 10)).await.unwrap();
        flush(&predecessor, &previous).await;
        previous.compaction().unwrap().compact_all().await.unwrap();
        let table = TableRef::new("public", "docs");
        let old: TrieKey = "l01-rc-b00".parse().unwrap();
        let snapshot = previous.snapshot();
        assert_eq!(snapshot.trie_entries(&table).iter().map(|trie| &trie.key).collect::<Vec<_>>(), [&old]);
        let slug = &snapshot.table_entry(&table).unwrap().slug;
        let files = [slug.data_key(&old), slug.metadata_key(&old)].map(|name| ObjectKey::new(name).unwrap());
        drop(snapshot);
        let Role::Leading(old_term) = previous_status.borrow().role else { panic!("the first node must lead") };
        let cursor = reader.query_sql("SELECT value FROM docs", vec![]).await.unwrap();

        config.automatic_gc = true;
        config.replication.claim_wait = Some(Duration::from_millis(25));
        let (successor, successor_driver) = running(config, NodeServices {
            resources: shared, clock: new_clock.clone(), ..services()
        }).await;
        let current = successor.catalog().database("xtdb").unwrap();
        current.watch_status().wait_for(|state| matches!(state.role, Role::Leading(term) if term > old_term)).await.unwrap();
        previous_status.wait_for(|state| state.role == Role::Following && state.highest_term > old_term).await.unwrap();

        let mut writer = successor.session("xtdb", Arc::new(Authority)).unwrap();
        writer.state_mut().execute_operation(put(&successor, 2, 20)).await.unwrap();
        flush(&successor, &current).await;
        current.compaction().unwrap().compact_all().await.unwrap();
        assert!(current.trie_catalog().garbage_tries(&table, Timestamp::MAX).iter().any(|trie| trie.key == old),
            "the successor compaction must supersede the old trie");

        // Inside the window the superseded files stay, so the old leader's running query reads them.
        let store = current.object_store();
        for id in 3..6 {
            writer.state_mut().execute_operation(put(&successor, id, id * 10)).await.unwrap();
            flush(&successor, &current).await;
        }
        for file in &files { store.head_fresh(file).await.unwrap(); }
        assert_eq!(rows(cursor).await, vec![vec![Value::I64(10)]]);

        // An open query is no hold: once the window passes, the files go while it is still open.
        let held = reader.query_sql("SELECT value FROM docs", vec![]).await.unwrap();
        wall.0.fetch_add(i64::try_from(2 * lifetime.as_micros()).unwrap(), Ordering::Relaxed);
        let mut ids = 6..;
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let id = ids.next().unwrap();
                writer.state_mut().execute_operation(put(&successor, id, id * 10)).await.unwrap();
                flush(&successor, &current).await;
                let retired = futures::future::join_all(files.iter().map(|file| store.head_fresh(file))).await;
                if retired.into_iter().all(|result| matches!(result, Err(Error::MissingObject(_)))) { break; }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await.expect("the successor must retire the trie once it leaves the garbage window, though a query is open");
        drop(held);
        drop((reader, writer, previous, current));
        stop(&predecessor, predecessor_driver).await;
        stop(&successor, successor_driver).await;
    }).await.expect("the shared-journal handoff must finish");
}

#[tokio::test]
async fn startup_selects_a_newer_checkpoint_when_its_initial_replica_prefix_was_trimmed() {
    use rtdb_storage::{Result as StorageResult, log::{AppendReceipt, Log, LogOffset, LogRange, LogRecord, LogTail, MemoryLog}};
    use std::sync::atomic::AtomicUsize;

    struct SharedLogs {
        source: Arc<MemoryLog>,
        replica: Arc<MemoryLog>,
        store: Arc<rtdb_storage::MemoryStore>,
    }

    struct LogOpening {
        reached: Semaphore,
        release: Semaphore,
        attempted: AtomicUsize,
        reopened: AtomicUsize,
    }
    struct StartupLog { inner: Arc<MemoryLog>, opening: Arc<LogOpening> }

    #[async_trait]
    impl Log for StartupLog {
        async fn enqueue(&self, payload: prost::bytes::Bytes, timestamp: Timestamp) -> StorageResult<AppendReceipt> {
            self.inner.enqueue(payload, timestamp).await
        }
        fn latest(&self) -> StorageResult<LogOffset> { self.inner.latest() }
        async fn recovery_tail(&self, after: LogOffset) -> StorageResult<Option<Box<dyn LogTail>>> {
            if self.opening.attempted.fetch_add(1, Ordering::SeqCst) == 0 {
                self.opening.reached.add_permits(1);
                self.opening.release.acquire().await.unwrap().forget();
            }
            self.inner.recovery_tail(after).await
        }
        async fn tail(&self, after: LogOffset) -> StorageResult<Box<dyn LogTail>> {
            self.opening.reopened.fetch_add(1, Ordering::SeqCst);
            self.inner.tail(after).await
        }
        async fn range(&self, from: LogOffset, to: LogOffset) -> StorageResult<Box<dyn LogRange>> {
            self.inner.range(from, to).await
        }
        async fn read_range(&self, from: LogOffset, to: LogOffset) -> StorageResult<Vec<LogRecord>> {
            self.inner.read_range(from, to).await
        }
    }
    struct Resources { shared: Arc<SharedLogs>, opening: Option<Arc<LogOpening>> }

    #[async_trait]
    impl ResourceFactory for Resources {
        fn validate(&self, _: &str, _: &DatabaseConfig) -> Result<()> { Ok(()) }

        async fn open_storage(&self, _: &str, _: &DatabaseConfig) -> Result<DatabaseStorage> {
            Ok(DatabaseStorage {
                audience_storage: None,
                checkpoint_metadata: None, fragment_storage: None,
                store: self.shared.store.clone(),
                blocks: Default::default(),
                storage_epoch: rtdb_types::StorageEpoch(0),
                storage_version: rtdb_format::keys::CURRENT_STORAGE_VERSION,
                io: rtdb_storage::IoRegion::new(),
            })
        }

        async fn open_logs(&self, _: &str, _: &DatabaseConfig, _: LogAccess, _: CancellationToken, _: &DatabaseStorage) -> Result<OpenedLogs> {
            let replica: Arc<dyn Log> = match &self.opening {
                Some(opening) => Arc::new(StartupLog { inner: self.shared.replica.clone(), opening: opening.clone() }),
                None => self.shared.replica.clone(),
            };
            Ok(OpenedLogs {
                source: self.shared.source.clone(), replica,
                source_epoch: rtdb_types::StorageEpoch(0), replica_epoch: rtdb_types::StorageEpoch(0), replica_read_index: None,
                placed_transfer: None, await_authority: rtdb_node::AwaitAuthority::SourcePosition, drivers: Vec::new(),
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

        let opening = Arc::new(LogOpening {
            reached: Semaphore::new(0), release: Semaphore::new(0), attempted: AtomicUsize::new(0), reopened: AtomicUsize::new(0),
        });
        let reader_resources = Arc::new(Resources { shared: shared.clone(), opening: Some(opening.clone()) });
        let mut reader_options = options();
        reader_options.read_only = true;
        let mut starting = tokio::spawn(Node::open(reader_options, NodeServices { resources: reader_resources, ..runtime }));
        tokio::select! {
            _ = &mut starting => panic!("startup completed before admitting its replica cursor"),
            reached = opening.reached.acquire() => reached.unwrap().forget(),
        }

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
        let selected = reader.catalog().database("xtdb").unwrap().status().adopted_block.unwrap();
        assert_eq!(selected.block_index(), replacement.block_index());
        assert_eq!(opening.attempted.load(Ordering::SeqCst), 2);
        let reader_driver = tokio::spawn(driver.run());
        reader.sync_all(Duration::from_secs(10), &token).await.expect("startup must replay the retained replica suffix");
        assert_eq!(opening.reopened.load(Ordering::SeqCst), 0, "the driver must reuse startup's admitted cursor");
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
        struct SteppedClock(AtomicI64);
        impl Clock for SteppedClock { fn now(&self) -> Timestamp { Timestamp(self.0.load(Ordering::Relaxed)) } }
        let first_time = Timestamp(1_000_000_000);
        let clock = Arc::new(SteppedClock(AtomicI64::new(first_time.0)));
        let mut runtime = services();
        runtime.clock = clock.clone();
        let boot = || NodeServices {
            resources: Arc::new(NativeResources::default().with_local_log_clock(runtime.clock.clone(), true)),
            ..runtime.clone()
        };
        let (node, driver) = running(settings.clone(), boot()).await;
        let token = CancellationToken::new();
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        let put = session.state_mut().execute_tx(vec![put(&node, 1, 10)], AutonomousTxOptions::default()).await.unwrap();
        assert!(put.committed());
        assert_eq!(put.tx_key().system_time, first_time);
        clock.0.store(1_000_000, Ordering::Relaxed);

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

#[cfg(unix)]
#[tokio::test]
async fn default_sealed_local_erase_waits_for_the_original_generation_and_resumes_after_restart() {
    use rtdb_node::serving::QuerySource;
    use rtdb_tx::transact::{Erasure, Op, PHYSICAL_ERASURE, Transact};
    use rtdb_types::rtdb_v1::{At, Layer, MAIN_BRANCH};

    tokio::time::timeout(Duration::from_secs(30), async {
        let files = tempfile::tempdir().unwrap();
        let private = || {
            use std::os::unix::fs::PermissionsExt;
            tempfile::Builder::new().permissions(std::fs::Permissions::from_mode(0o700)).tempdir().unwrap()
        };
        let classic_vault = private();
        let checkpoint_vault = private();
        let clock = Arc::new(TickClock(AtomicI64::new(1_000_000)));
        let runtime = || {
            let clock_for_keys = Arc::clone(&clock);
            let kms = rtdb_crypt::FileKms::open(checkpoint_vault.path(), move || clock_for_keys.0.load(Ordering::SeqCst)).unwrap();
            let keys = rtdb_storage::sealing::ClassicKeyVault::open(classic_vault.path()).unwrap();
            NodeServices {
                clock: clock.clone(),
                resources: Arc::new(NativeResources::default().with_local_log_clock(clock.clone(), false)
                    .with_classic_key_vault(keys).with_local_checkpoint_kms(kms)),
                ..services()
            }
        };
        let mut settings = options();
        settings.default_v1_serving = true;
        settings.membership = Membership::Durable { primary: local_database(files.path()), skipped: BTreeSet::new() };
        let cancel = CancellationToken::new();
        let (node, owner) = running(settings.clone(), runtime()).await;
        let database = node.catalog().database("xtdb").unwrap();
        let serving = database.serving_driver().unwrap();
        assert!(serving.features().contains(&PHYSICAL_ERASURE));
        let mut sql = node.session("xtdb", Arc::new(Authority)).unwrap();
        index_sql(&mut sql, "INSERT INTO erase_rows (_id, value) VALUES (1, 10), (2, 20)").await;
        index_flush(&node, &database).await;
        let view = serving.query(QuerySource { branch: MAIN_BRANCH.into(), at: At::Linearizable, layer: Layer::Merged },
            false, node.now(), &cancel).await.unwrap();
        let generation = view.key_generation.unwrap();
        // A SQL reader of the pre-erasure catalog registers nothing; the erasure floor refuses it once installed.
        let old_sql_cut = database.snapshot().catalog_cut().unwrap();
        let incarnation = uuid::Uuid::from_bytes(serving.incarnation());
        let key = b"sealed-local-erase".to_vec();
        let request = Transact {
            key: key.clone(), require: vec![PHYSICAL_ERASURE.into()], window: Default::default(),
            guards: Vec::new(), requires: Vec::new(), otherwise: Vec::new(),
            ops: vec![Op::Erase { branch: None, table: TableRef::new("public", "erase_rows"), ids: vec![EntityId::Integer(1)] }],
            custody_request_digest: None, request_memory: None,
        };
        assert_eq!(serving.transact(request, &cancel).await.unwrap_err().code, "rtdb/erasure-pending");
        let retirement = checkpoint_vault.path().join(format!("{}-{generation:016x}.retirement", incarnation.simple()));
        while !retirement.is_file() { tokio::time::sleep(Duration::from_millis(5)).await; }
        let deadline = i64::from_be_bytes(std::fs::read(retirement).unwrap().try_into().unwrap());
        assert!(serving.check_read_delivery(Some(generation), &BTreeSet::new(), &cancel).await.is_err());
        clock.0.store(deadline + 1_000_000, Ordering::SeqCst);
        assert!(database.object_store().require_erasure_catalog(Some(old_sql_cut)).await.is_err());
        assert_eq!(serving.tx_status(&key, &cancel).await.unwrap_err().code, "rtdb/erasure-pending");
        drop(view);
        stop(&node, owner).await;
        drop(sql);
        drop(database);
        drop(node);

        let (node, owner) = running(settings, runtime()).await;
        let database = node.catalog().database("xtdb").unwrap();
        let serving = database.serving_driver().unwrap();
        let receipt = loop {
            match serving.tx_status(&key, &cancel).await {
                Ok(receipt) => break receipt,
                Err(error) if error.code == "rtdb/erasure-pending" => tokio::time::sleep(Duration::from_millis(5)).await,
                Err(error) => panic!("failed to recover the ordered erasure: {error}"),
            }
        };
        assert_eq!(receipt.erasure, Some(Erasure::Armed { unreadable_by: Timestamp(deadline) }));
        assert!(serving.check_read_delivery(Some(generation), &BTreeSet::new(), &cancel).await.is_err());
        let mut sql = node.session("xtdb", Arc::new(Authority)).unwrap();
        assert_eq!(rows(sql.query_sql("SELECT _id FROM erase_rows ORDER BY _id", vec![]).await.unwrap()).await,
            [vec![Value::I64(2)]]);
        stop(&node, owner).await;
    }).await.unwrap();
}

#[tokio::test]
async fn default_local_restore_activates_an_independent_branch_and_recovers_its_terminal() {
    use rtdb_node::serving::QuerySource;
    use rtdb_types::rtdb_v1::{At, Layer, MAIN_BRANCH};

    tokio::time::timeout(Duration::from_secs(30), async {
        let original = tempfile::tempdir().unwrap();
        let provisioned = tempfile::tempdir().unwrap();
        let mut settings = options();
        settings.default_v1_serving = true;
        settings.membership = Membership::Durable { primary: local_database(original.path()), skipped: BTreeSet::new() };
        let runtime = services();
        let (node, driver) = running(settings.clone(), fresh_local_services(&runtime)).await;
        let cancellation = CancellationToken::new();
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        index_sql(&mut session, "INSERT INTO restore_rows (_id, value) VALUES (1, 10)").await;
        let primary = node.catalog().database("xtdb").unwrap();
        index_flush(&node, &primary).await;
        let at = |at| QuerySource { branch: MAIN_BRANCH.into(), at, layer: Layer::Merged };
        let source = primary.serving_driver().unwrap().query(at(At::Linearizable), false, node.now(), &cancellation).await.unwrap().cut.basis;
        let ticket = node.export_default_checkpoint("xtdb", uuid::Uuid::new_v4(), source, None, None, &cancellation).await.unwrap();
        let restored = node.restore_default_checkpoint("xtdb", "restored", &ticket, provisioned.path(), &cancellation).await.unwrap();
        assert_ne!(restored.incarnation(), source.incarnation());
        assert_eq!(restored.position(), source.position());
        let destination = node.catalog().database("restored").unwrap();
        assert_eq!(destination.serving_driver().unwrap().query(at(At::Exactly(restored)), false, node.now(), &cancellation).await.unwrap().cut.basis, restored);
        let mut copy = node.session("restored", Arc::new(Authority)).unwrap();
        assert_eq!(rows(copy.query_sql("SELECT value FROM restore_rows WHERE _id = 1", vec![]).await.unwrap()).await, [vec![Value::I64(10)]]);
        index_sql(&mut copy, "INSERT INTO restore_rows (_id, value) VALUES (2, 20)").await;
        index_flush(&node, &destination).await;
        assert!(rows(session.query_sql("SELECT value FROM restore_rows WHERE _id = 2", vec![]).await.unwrap()).await.is_empty());
        node.release_default_checkpoint("xtdb", &ticket, &cancellation).await.unwrap();
        drop(copy);
        drop(session);
        drop(destination);
        drop(primary);
        stop(&node, driver).await;
        drop(node);

        let (node, driver) = running(settings, fresh_local_services(&runtime)).await;
        assert_eq!(node.restore_default_checkpoint("xtdb", "restored", &ticket, provisioned.path(), &cancellation).await.unwrap(), restored);
        let mut copy = node.session("restored", Arc::new(Authority)).unwrap();
        assert_eq!(rows(copy.query_sql("SELECT value FROM restore_rows WHERE _id = 2", vec![]).await.unwrap()).await, [vec![Value::I64(20)]]);
        drop(copy);
        assert!(node.detach_database("restored".into(), &Authority, &cancellation).await.unwrap().committed());
        assert_eq!(node.restore_default_checkpoint("xtdb", "restored", &ticket, provisioned.path(), &cancellation).await.unwrap_err().code,
            "rtdb/default-restore-retired");
        stop(&node, driver).await;
    }).await.unwrap();
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
                    let basis = QueryBasis { snapshot: node.snapshot_basis()?, replica_capture: None, resume_token: None, has_await_floor: false, snapshot_time: None, current_time: node.query_instant(), timezone: "UTC".into() };
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
async fn retained_prepared_statements_charge_only_their_own_bytes_after_their_queries_close() {
    let (node, driver) = running(options(), services()).await;
    let cancellation = CancellationToken::new();
    // The statements' own budget: the query budget each context reserves from. Its used bytes exclude the idle credit its
    // thread shards hold, which the node root counts as used and which depends on the shard count and thread placement.
    let keeper = node.query_memory();
    let before = keeper.used_bytes();
    let mut retained = Vec::new();
    for index in 0..200 {
        let basis = QueryBasis { snapshot: node.snapshot_basis().unwrap(), replica_capture: None, resume_token: None, has_await_floor: false, snapshot_time: None, current_time: node.query_instant(), timezone: "UTC".into() };
        let context = node.query_context("xtdb", &basis, &Authority, &cancellation).await.unwrap();
        assert!(Arc::ptr_eq(&context.memory, &keeper), "a direct query context reserves from the node's query budget");
        let prepared = PreparedSql::prepare(&format!("SELECT {index} AS n"), &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
        let cursor = prepared.open_query(context, Vec::new()).await.unwrap();
        assert_eq!(rows(cursor).await.len(), 1);
        retained.push(prepared);
    }
    let per_statement = keeper.used_bytes().saturating_sub(before) / retained.len();
    assert!(per_statement < 32 << 10, "a retained statement pins {per_statement} bytes, not a query's credit");
    drop(retained);
    stop(&node, driver).await;
}

#[tokio::test]
async fn node_shutdown_cancels_direct_query_contexts_with_independent_caller_tokens() {
    let (node, driver) = running(options(), services()).await;
    let cancellation = CancellationToken::new();
    let basis = QueryBasis { snapshot: node.snapshot_basis().unwrap(), replica_capture: None, resume_token: None, has_await_floor: false, snapshot_time: None, current_time: node.query_instant(), timezone: "UTC".into() };
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
        replica_capture: None,
        resume_token: None,
        has_await_floor: false,
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
async fn bijection_at_selects_read_validity_and_refuses_writes_until_cleared() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    index_sql(&mut session, "INSERT INTO docs (_id, value, _valid_from, _valid_to)
        VALUES (1, 10, TIMESTAMP '2027-01-01T00:00:00Z', TIMESTAMP '2027-02-01T00:00:00Z')").await;
    let token = session.state().cancellation().child_token();
    let parse = |sql| rtdb_sql::parse_control(sql, &Default::default()).unwrap().unwrap();
    session.execute_control(&parse("SET bijection.at = TIMESTAMP '2027-01-15T00:00:00Z'"), &[], &token).await.unwrap();
    let result = rows(session.query_sql("SELECT value, CURRENT_TIMESTAMP FROM docs", vec![]).await.unwrap()).await;
    assert_eq!(result[0][0], Value::I64(10));
    let fixed = rtdb_types::parse_sql_timestamp_instant("2027-01-15T00:00:00Z", "UTC").unwrap().as_micros().unwrap();
    assert_eq!(rtdb_expr::timestamp_micros(&result[0][1], "UTC").unwrap(), fixed);
    assert!(session.execute_control(&parse("BEGIN READ WRITE"), &[], &token).await.is_err());
    let refused = session.state_mut().execute_tx(vec![put(&node, 2, 20)], AutonomousTxOptions::default()).await.unwrap_err();
    assert_eq!(refused.code, "xtdb/read-only-tx");

    session.execute_control(&parse("SET bijection.at = NULL"), &[], &token).await.unwrap();
    assert!(rows(session.query_sql("SELECT value FROM docs", vec![]).await.unwrap()).await.is_empty());
    assert!(session.state_mut().execute_tx(vec![put(&node, 2, 20)], AutonomousTxOptions::default()).await.unwrap().committed());
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
async fn source_document_slices_fit_the_transaction_budget() {
    for (count, extra_fields) in [(100, 10), (2000, 0)] {
        let mut options = options();
        options.execution_memory_bytes = 256 * 1024 * 1024;
        let (node, driver) = running(options, services()).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        let table = TableRef::new("public", "docs");
        let writes = (0..count).map(|id| {
            let mut document = StructFields::from([("_id".into(), Value::I64(id))]);
            for field in 0..extra_fields {
                document.insert(format!("field_{id}_{field}"), Value::Utf8(format!("value_{id}_{field}")));
            }
            TxOp::PutDocs {
                table: table.clone(), documents: Documents::new(vec![document], node.memory()).unwrap(),
                valid_from: None, valid_to: None,
            }
        }).collect();
        let result = session.state_mut().execute_tx(writes, AutonomousTxOptions::default()).await.unwrap();
        assert!(result.committed(), "{count} operations: {:?}", result.error());
        let projected = rows(session.query_sql("SELECT _id FROM docs", vec![]).await.unwrap()).await;
        assert_eq!(projected.len(), count as usize);
        stop(&node, driver).await;
    }
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
async fn a_replayed_patch_preserves_a_null_override_and_fills_both_valid_time_gaps() {
    use rtdb_types::Timestamp;

    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let table = TableRef::new("public", "patch_segments");
    let documents = Documents::new(vec![StructFields::from([
        ("_id".into(), Value::I64(1)), ("retained".into(), Value::I64(11)),
    ])], node.memory()).unwrap();
    let put = TxOp::PutDocs {
        table: table.clone(), documents, valid_from: Some(Timestamp(10)), valid_to: Some(Timestamp(20)),
    };
    assert!(session.state_mut().execute_tx(vec![put], AutonomousTxOptions::default()).await.unwrap().committed());
    let documents = Documents::new(vec![StructFields::from([
        ("_id".into(), Value::I64(1)), ("retained".into(), Value::Null), ("revised".into(), Value::I64(42)),
    ])], node.memory()).unwrap();
    let patch = TxOp::PatchDocs {
        table, documents, valid_from: Some(Timestamp(5)), valid_to: Some(Timestamp(25)),
    };
    let result = session.state_mut().execute_tx(vec![patch], AutonomousTxOptions::default()).await.unwrap();
    assert!(result.committed(), "{:?}", result.error());
    let cursor = session.query_sql("SELECT retained, revised FROM patch_segments FOR ALL VALID_TIME ORDER BY _valid_from", vec![]).await.unwrap();
    assert_eq!(rows(cursor).await, [
        vec![Value::Null, Value::I64(42)],
        vec![Value::I64(11), Value::I64(42)],
        vec![Value::Null, Value::I64(42)],
    ]);
    drop(session);
    stop(&node, driver).await;
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
async fn every_reader_of_one_catalog_version_shares_its_open_databases_until_membership_changes() {
    let mut config = options();
    config.membership = Membership::Placed;
    let (node, driver) = running(config, services()).await;
    node.database_or_attach("shop".into(), DatabaseConfig::default()).await.unwrap().unwrap();
    let first = node.catalog().open_database_map();
    assert!(Arc::ptr_eq(&first, &node.catalog().open_database_map()), "readers of one version share its open databases");
    assert_eq!(first.keys().cloned().collect::<Vec<_>>(), ["shop"]);

    node.database_or_attach("blog".into(), DatabaseConfig::default()).await.unwrap().unwrap();
    let attached = node.catalog().open_database_map();
    assert_eq!(attached.keys().cloned().collect::<Vec<_>>(), ["blog", "shop"]);
    assert_eq!(first.keys().cloned().collect::<Vec<_>>(), ["shop"], "an earlier reader keeps the version it read");

    node.detach_local("shop".into()).await.unwrap();
    assert_eq!(node.catalog().open_database_map().keys().cloned().collect::<Vec<_>>(), ["blog"]);
    assert_eq!(node.catalog().open_databases().keys().cloned().collect::<Vec<_>>(), ["blog"]);
    drop((first, attached));
    stop(&node, driver).await;
}

#[tokio::test]
async fn queries_share_one_credited_budget_that_never_holds_back_the_node_limit() {
    let (node, driver) = running(options(), services()).await;
    assert!(Arc::ptr_eq(&node.query_memory(), &node.query_memory()));
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    assert!(session.state_mut().execute_tx(vec![put(&node, 1, 10)], AutonomousTxOptions::default()).await.unwrap().committed());
    for _ in 0..50 {
        assert_eq!(rows(session.query_sql("SELECT value FROM docs", vec![]).await.unwrap()).await, [vec![Value::I64(10)]]);
    }
    let available = node.memory().available_bytes();
    let whole = node.memory().reserve(available).expect("unspent query credit is reclaimed for a reservation that fits");
    assert!(node.memory().reserve(1).is_err());
    drop(whole);
    drop(session);
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
        second.state_mut().execute_tx(vec![put(&node, 1, 10)], metadata()),
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert!(a.committed() && b.committed(), "both callers are told the keyed transaction committed");
    assert_eq!(a.tx_key(), b.tx_key(), "and both are told it is the same transaction");
    assert_eq!(rows(first.query_sql("SELECT COUNT(*) FROM docs", vec![]).await.unwrap()).await, [vec![Value::I64(1)]], "only one of them ran");
    let different = first.state_mut().execute_tx(vec![put(&node, 2, 20)], metadata()).await.unwrap();
    assert_eq!(different.error().map(|error| error.code.as_str()), Some("rtdb/idempotency-conflict"), "the key cannot name a different transaction");
    assert_eq!(rows(first.query_sql("SELECT COUNT(*) FROM docs", vec![]).await.unwrap()).await, [vec![Value::I64(1)]]);
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

    let shop_retry = shop.state_mut().submit_tx(vec![put(&node, 1, 10)], metadata("order-1")).await.unwrap();
    let blog_retry = blog.state_mut().submit_tx(vec![put(&node, 1, 20)], metadata("article-1")).await.unwrap();
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
async fn legacy_source_and_typed_duplicate_outcomes_survive_recovery() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let directory = tempfile::tempdir().unwrap();
        let config = DatabaseConfig {
            log: Some(database_config::Log::LocalLog(LocalLog {
                path: Some(directory.path().join("log").to_string_lossy().into_owned()), epoch: Some(0),
            })),
            storage: Some(database_config::Storage::LocalStorage(LocalStorage {
                path: Some(directory.path().join("store").to_string_lossy().into_owned()), epoch: Some(0),
            })),
            ..Default::default()
        };
        let clock = Arc::new(ReplayTestClock(AtomicI64::new(1_000_000)));
        let runtime = NodeServices { clock: clock.clone(), ..services() };
        let resources = NativeResources::default().with_local_log_clock(Arc::clone(&runtime.clock), false);
        let cancellation = CancellationToken::new();
        let mut seeded = resources.open("xtdb", &config, LogAccess::ReadWrite, cancellation.clone()).await.unwrap();
        let drivers: Vec<_> = std::mem::take(&mut seeded.logs.drivers).into_iter().map(tokio::spawn).collect();
        let memory = rtdb_columnar::MemoryBudget::new(16 * 1024 * 1024);
        let key = rtdb_tx::UserMetadata::new(Value::Struct(StructFields::from([
            ("idempotency_key".into(), Value::Utf8("legacy-charge".into())),
        ])), &memory).unwrap();
        let put_doc = |table, id, valid_to| TxOp::PutDocs {
            table: TableRef::new("public", table),
            documents: Documents::new(vec![StructFields::from([("_id".into(), id)])], &memory).unwrap(),
            valid_from: None, valid_to,
        };
        let old_guard = || TxOp::Sql {
            sql: "ASSERT NOT EXISTS (SELECT 1 FROM rtdb_idempotency_keys WHERE _id = 'legacy-charge')".into(),
            args: None,
        };
        let marker = || put_doc("rtdb_idempotency_keys", Value::Utf8("legacy-charge".into()), Some(Timestamp(3_601_000_000)));
        let mut ids = Vec::new();
        for (index, operations) in [
            vec![put_doc("rtdb_idempotency_keys", Value::Utf8("~table".into()), None), old_guard(),
                 put_doc("docs", Value::I64(1), None), marker()],
            vec![old_guard(), put_doc("docs", Value::I64(2), None), marker()],
        ].into_iter().enumerate() {
            let request = rtdb_tx::SourceTransaction::new(operations, rtdb_tx::SubmitOptions {
                user_metadata: Some(key.clone()), ..Default::default()
            }, &memory).unwrap();
            let request = if index == 1 { request.with_idempotency_guard(Timestamp(1_000_000), false) } else { request };
            let encoded = request.encode().unwrap();
            let Some(rtdb_format::envelope::SourceMessage::Protobuf(rtdb_format::envelope::SourceBody::Tx(wire))) =
                rtdb_format::envelope::SourceMessage::decode(encoded.as_slice()).unwrap()
            else { panic!("expected a structured source transaction") };
            assert_eq!(wire.idempotency_guard.is_some(), index == 1);
            let appended = seeded.logs.source.enqueue(encoded.into_bytes(), runtime.clock.now()).await.unwrap().durable().await.unwrap();
            ids.push(TxId(appended.offset.0));
        }
        cancellation.cancel();
        for driver in drivers { driver.await.unwrap().unwrap(); }
        seeded.io.close_and_wait().await;
        drop(seeded);
        resources.close().await.unwrap();

        let mut settings = options();
        settings.membership = Membership::Durable { primary: config, skipped: BTreeSet::new() };
        let (node, driver) = running(settings.clone(), fresh_local_services(&runtime)).await;
        let cancellation = CancellationToken::new();
        let original = node.await_transaction("xtdb", ids[0], &Authority, &cancellation).await.unwrap();
        assert!(original.committed());
        let legacy_retry = node.await_transaction("xtdb", ids[1], &Authority, &cancellation).await.unwrap();
        assert_eq!(legacy_retry.tx_key(), original.tx_key());

        clock.0.store(1_000_000 + rtdb_tx::RECEIPT_WINDOW_MICROS, Ordering::Relaxed);
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        assert!(session.state_mut().execute_tx(vec![TxOp::Sql { sql: "ASSERT TRUE".into(), args: None }], Default::default()).await.unwrap().committed());
        node.catalog().database("xtdb").unwrap().flush().await.unwrap();
        node.sync_all(Duration::from_secs(10), &cancellation).await.unwrap();
        drop(session);
        stop(&node, driver).await;

        clock.0.store(999_999, Ordering::Relaxed);

        let (reopened, driver) = running(settings, fresh_local_services(&runtime)).await;
        let result = reopened.await_transaction("xtdb", ids[1], &Authority, &cancellation).await.unwrap();
        assert!(result.committed());
        assert_eq!(result.tx_key(), original.tx_key());
        let mut session = reopened.session("xtdb", Arc::new(Authority)).unwrap();
        assert_eq!(rows(session.query_sql("SELECT _id FROM docs FOR ALL VALID_TIME", vec![]).await.unwrap()).await, [vec![Value::I64(1)]]);
        let projection = format!("SELECT user_metadata FROM xt.txs FOR ALL VALID_TIME WHERE _id = {}", ids[1].0);
        assert_eq!(rows(session.query_sql(&projection, vec![]).await.unwrap()).await, [vec![Value::Null]]);
        drop(session);
        stop(&reopened, driver).await;
    }).await.unwrap();
}

struct ReplayTestClock(AtomicI64);

impl Clock for ReplayTestClock {
    fn now(&self) -> Timestamp { Timestamp(self.0.load(Ordering::Relaxed)) }
}

#[tokio::test]
async fn bounded_retries_preserve_terminal_aborts_and_do_not_reopen_after_clock_rollback() {
    let directory = tempfile::tempdir().unwrap();
    let primary = DatabaseConfig {
        log: Some(database_config::Log::LocalLog(LocalLog {
            path: Some(directory.path().join("log").to_string_lossy().into_owned()), epoch: Some(0),
        })),
        storage: Some(database_config::Storage::LocalStorage(LocalStorage {
            path: Some(directory.path().join("store").to_string_lossy().into_owned()), epoch: Some(0),
        })),
        ..Default::default()
    };
    let mut settings = options();
    settings.membership = Membership::Durable { primary, skipped: BTreeSet::new() };
    let clock = Arc::new(ReplayTestClock(AtomicI64::new(1_000_000)));
    let runtime = NodeServices { clock: clock.clone(), ..services() };
    let cutoff = 2_000_000;
    let retry_options = |node: &Node, nonce| AutonomousTxOptions {
        user_metadata: Some(rtdb_tx::UserMetadata::new(Value::Struct(StructFields::from([
            ("rtdb_retry_v1".into(), Value::Struct(StructFields::from([
                ("nonce".into(), Value::Utf8(format!("{nonce:032x}"))),
                ("not_after_micros".into(), Value::I64(cutoff)),
            ]))),
        ])), node.memory()).unwrap()),
        ..Default::default()
    };
    let conditional_write = |node| vec![
        TxOp::Sql { sql: "ASSERT EXISTS (SELECT 1 FROM docs WHERE _id = 1)".into(), args: None },
        put(node, 2, 20),
    ];

    let (node, driver) = running(settings.clone(), fresh_local_services(&runtime)).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let show = rtdb_sql::parse_control("SHOW rtdb_bounded_retry", &Default::default()).unwrap().unwrap();
    let ControlResult::Rows(reply) = session.execute_control(&show, &[], &CancellationToken::new()).await.unwrap() else {
        panic!("expected bounded retry capability");
    };
    assert_eq!(rows(reply).await, [vec![Value::Utf8("1".into())]]);
    assert!(session.state_mut().execute_tx(vec![put(&node, 0, 0)], Default::default()).await.unwrap().committed());
    let aborted = session.state_mut().execute_tx(conditional_write(&node), retry_options(&node, 1)).await.unwrap();
    assert_eq!(aborted.error().map(|error| error.code.as_str()), Some("xtdb/assert-failed"));
    let committed = session.state_mut().execute_tx(vec![put(&node, 1, 10)], retry_options(&node, 2)).await.unwrap();
    assert!(committed.committed());
    node.catalog().database("xtdb").unwrap().flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
    drop(session);
    stop(&node, driver).await;

    clock.0.store(999_999, Ordering::Relaxed);
    let (node, driver) = running(settings.clone(), fresh_local_services(&runtime)).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let aborted_retry = session.state_mut().execute_tx(conditional_write(&node), retry_options(&node, 1)).await.unwrap();
    assert_eq!(aborted_retry, aborted);
    let committed_retry = session.state_mut().execute_tx(vec![put(&node, 1, 10)], retry_options(&node, 2)).await.unwrap();
    assert_eq!(committed_retry, committed);
    let conflict = session.state_mut().execute_tx(vec![put(&node, 3, 30)], retry_options(&node, 2)).await.unwrap();
    assert_eq!(conflict.error().map(|error| error.code.as_str()), Some("rtdb/idempotency-conflict"));

    clock.0.store(cutoff, Ordering::Relaxed);
    assert!(session.state_mut().execute_tx(vec![TxOp::Sql { sql: "ASSERT TRUE".into(), args: None }], Default::default()).await.unwrap().committed());
    clock.0.store(cutoff - 1, Ordering::Relaxed);
    let expired = session.state_mut().execute_tx(vec![put(&node, 3, 30)], retry_options(&node, 2)).await.unwrap();
    assert_eq!(expired.error().map(|error| error.code.as_str()), Some("rtdb/idempotency-expired"));
    drop(session);
    stop(&node, driver).await;

    clock.0.store(999_999, Ordering::Relaxed);
    let (node, driver) = running(settings.clone(), fresh_local_services(&runtime)).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let late = session.state_mut().execute_tx(conditional_write(&node), retry_options(&node, 1)).await.unwrap();
    assert_eq!(late.error().map(|error| error.code.as_str()), Some("rtdb/idempotency-expired"));
    assert_eq!(rows(session.query_sql("SELECT _id FROM docs FOR ALL VALID_TIME ORDER BY _id", vec![]).await.unwrap()).await,
        [vec![Value::I64(0)], vec![Value::I64(1)]]);
    let retirement = 1_000_000 + rtdb_tx::RECEIPT_WINDOW_MICROS;
    clock.0.store(retirement, Ordering::Relaxed);
    assert!(session.state_mut().execute_tx(vec![TxOp::Sql { sql: "ASSERT TRUE".into(), args: None }], Default::default()).await.unwrap().committed());
    node.catalog().database("xtdb").unwrap().flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
    drop(session);
    stop(&node, driver).await;

    clock.0.store(999_999, Ordering::Relaxed);
    let (node, driver) = running(settings, fresh_local_services(&runtime)).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let history = format!("SELECT committed, user_metadata FROM xt.txs FOR ALL VALID_TIME WHERE _id = {}", committed.tx_key().tx_id.0);
    assert_eq!(rows(session.query_sql(&history, vec![]).await.unwrap()).await, [vec![Value::Bool(true), Value::Null]]);
    let resident = "SELECT _id FROM xt.bounded_retry_receipts FOR ALL VALID_TIME";
    assert!(rows(session.query_sql(resident, vec![]).await.unwrap()).await.is_empty());
    let original = node.await_transaction("xtdb", committed.tx_key().tx_id, &Authority, &CancellationToken::new()).await.unwrap();
    assert_eq!(original, committed);
    drop(session);
    stop(&node, driver).await;
}

#[tokio::test]
async fn ordered_present_is_applied_and_advances_when_the_append_clock_retreats() {
    let clock = Arc::new(ReplayTestClock(AtomicI64::new(1_000_000)));
    let log_clock = Arc::clone(&clock);
    let resources = NativeResources::default().with_log_clock(Arc::new(move || Ok(log_clock.now())));
    let services = NodeServices { clock: clock.clone(), resources: Arc::new(resources), ..services() };
    let (node, driver) = running(options(), services).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let initial = session.state_mut().execute_tx(
        vec![put(&node, 1, 10)],
        AutonomousTxOptions::default(),
    ).await.unwrap();
    assert!(initial.committed());

    let first = session.reserve_ordered_present().await.unwrap();
    clock.0.store(999_999, Ordering::Relaxed);
    let second = session.reserve_ordered_present().await.unwrap();
    assert!(initial.tx_key().system_time < first.system_time);
    assert!(first.system_time < second.system_time);
    let database = node.catalog().database("xtdb").unwrap();
    assert_eq!(database.snapshot().tx_basis(), Some(second));
    let (_, token) = session.state().connection_await_token();
    let basis = rtdb_format::basis::decode_tx_basis(token.unwrap()).unwrap();
    assert_eq!(basis.databases["xtdb"], [second.tx_id]);

    assert_eq!(
        rows(session.query_sql("SELECT _id FROM docs FOR ALL VALID_TIME", vec![]).await.unwrap()).await,
        [vec![Value::I64(1)]],
    );
    let next = session.state_mut().execute_tx(
        vec![put(&node, 2, 20)],
        AutonomousTxOptions::default(),
    ).await.unwrap();
    assert!(next.committed());
    assert!(second.system_time < next.tx_key().system_time);
    drop(session);
    stop(&node, driver).await;
}

#[tokio::test]
async fn ordered_present_refuses_read_only_grants_and_session_modes_before_append() {
    struct ReadOnlyIdentity;
    #[async_trait]
    impl SessionIdentity for ReadOnlyIdentity {
        fn user(&self) -> Option<&str> { Some("reader") }
        async fn revalidate(&self, _: Timestamp) -> Result<()> { Ok(()) }
        fn authorize_database(&self, _: &str, access: DatabaseAccess) -> Result<()> {
            match access {
                DatabaseAccess::Read => Ok(()),
                DatabaseAccess::Write => Err(Anomaly::new(
                    rtdb_types::AnomalyCategory::Forbidden, "test/read-only", "Write grant required",
                )),
            }
        }
    }

    let (node, driver) = running(options(), services()).await;
    let database = node.catalog().database("xtdb").unwrap();
    let before = database.latest_submitted_position().unwrap();
    let mut reader = node.session("xtdb", Arc::new(ReadOnlyIdentity)).unwrap();
    assert_eq!(reader.reserve_ordered_present().await.unwrap_err().code, "test/read-only");

    let mut writer = node.session("xtdb", Arc::new(Authority)).unwrap();
    writer.state_mut().set_session_characteristics(Some(rtdb_plan::AccessMode::ReadOnly)).unwrap();
    assert_eq!(writer.reserve_ordered_present().await.unwrap_err().code, "xtdb/read-only-tx");
    writer.state_mut().set_session_characteristics(None).unwrap();
    writer.state_mut().set_auto_commit(false).await.unwrap();
    assert_eq!(writer.reserve_ordered_present().await.unwrap_err().code, "xtdb/tx-already-in-progress");
    assert_eq!(database.latest_submitted_position().unwrap(), before);

    drop(reader);
    drop(writer);
    stop(&node, driver).await;
}

async fn commit_private(session: &mut NativeSession, operation: TxOp, key: &str, not_after: Timestamp) -> Result<CommitResult> {
    session.state_mut().begin()?;
    session.state_mut().execute_operation(operation).await?;
    session.state_mut().commit_tx_with_replay_permit(ReplaySubmissionPermit { key: key.to_owned(), not_after }).await
}

#[tokio::test]
async fn private_replay_is_rejected_at_the_exclusive_cutoff_without_appending() {
    let clock = Arc::new(ReplayTestClock(AtomicI64::new(1_000_000)));
    let services = NodeServices { clock, ..services() };
    let (node, driver) = running(options(), services).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let key = "00112233445566778899aabbccddeeff/0";
    let db = node.catalog().database("xtdb").unwrap();
    let before = db.progress().current().source_position;
    let error = commit_private(&mut session, put(&node, 1, 10), key, Timestamp(1_000_000)).await.unwrap_err();
    assert_eq!(error.code, "rtdb.node/replay-window-elapsed");
    assert_eq!(db.progress().current().source_position, before);
    assert_eq!(session.state().status(), TransactionStatus::Idle);

    let metadata = rtdb_tx::UserMetadata::new(Value::Struct(StructFields::from([
        ("idempotency_key".into(), Value::Utf8("own-application-key".into())),
    ])), node.memory()).unwrap();
    session.state_mut().begin_with(TransactionSettings {
        access: Some(AccessSettings::ReadWrite(ReadWriteSettings {
            user_metadata: OptionUpdate::Set(metadata),
            ..Default::default()
        })),
        ..Default::default()
    }).unwrap();
    session.state_mut().execute_operation(put(&node, 2, 20)).await.unwrap();
    let error = session.state_mut().commit_tx_with_replay_permit(ReplaySubmissionPermit {
        key: key.into(), not_after: Timestamp(1_000_001),
    }).await.unwrap_err();
    assert_eq!(error.code, "rtdb.node/conflicting-replay-key");
    assert_eq!(db.progress().current().source_position, before);

    let ordinary = session.state_mut().execute_tx(vec![put(&node, 3, 30)], AutonomousTxOptions::default()).await.unwrap();
    assert!(ordinary.committed());
    assert_eq!(rows(session.query_sql("SELECT _id FROM xt.txs FOR ALL VALID_TIME", vec![]).await.unwrap()).await,
        [vec![Value::I64(ordinary.tx_key().tx_id.0)]]);
    assert_eq!(rows(session.query_sql("SELECT _id FROM docs FOR ALL VALID_TIME", vec![]).await.unwrap()).await,
        [vec![Value::I64(3)]]);
    drop(session);
    stop(&node, driver).await;
}

#[tokio::test]
async fn private_replay_lookup_budget_error_aborts_without_stopping_resolution() {
    #[derive(Default)]
    struct LookupBudget(AtomicBool);

    #[async_trait]
    impl rtdb_query::ScanAccelerator for LookupBudget {
        async fn candidate_iids(&self, request: &rtdb_query::ScanRequest<'_>) -> Result<Option<BTreeSet<rtdb_types::Iid>>> {
            let table = &request.scan.table.name.table;
            if table.schema == "xt" && table.table == "rtdb_replay_keys" && self.0.load(Ordering::Relaxed) {
                return Err(Anomaly::incorrect("rtdb/memory-budget-exceeded", "Replay lookup test budget exhausted"));
            }
            Ok(None)
        }
    }

    let budget = Arc::new(LookupBudget::default());
    let mut services = services();
    services.scan_accelerators.push(budget.clone());
    let (node, driver) = running(options(), services).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let key = "00112233445566778899aabbccddeeff/0";
    let cutoff = Timestamp(7_200_000_000);
    let original = commit_private(&mut session, put(&node, 1, 10), key, cutoff).await.unwrap();
    let CommitResult::Executed(original) = original else { panic!("expected synchronous commit") };
    assert!(original.committed());

    budget.0.store(true, Ordering::Relaxed);
    let failed = commit_private(&mut session, put(&node, 2, 20), key, cutoff).await.unwrap();
    let CommitResult::Executed(failed) = failed else { panic!("expected synchronous abort") };
    assert_eq!(failed.error().map(|error| error.code.as_str()), Some("rtdb/memory-budget-exceeded"));
    assert!(failed.tx_key().tx_id > original.tx_key().tx_id);

    budget.0.store(false, Ordering::Relaxed);
    let retry = commit_private(&mut session, put(&node, 3, 30), key, cutoff).await.unwrap();
    let CommitResult::Executed(retry) = retry else { panic!("expected synchronous retry") };
    assert!(retry.committed());
    assert_eq!(retry.tx_key(), original.tx_key());
    assert_eq!(rows(session.query_sql("SELECT _id FROM docs FOR ALL VALID_TIME", vec![]).await.unwrap()).await,
        [vec![Value::I64(1)]]);
    drop(session);
    stop(&node, driver).await;
}

#[tokio::test]
async fn external_source_aborts_keep_their_error_when_it_names_a_private_replay_receipt() {
    #[derive(Clone)]
    struct Source(tokio::sync::mpsc::UnboundedSender<TxId>);
    struct Writer;

    #[async_trait]
    impl ExternalSourceFactory for Source {
        fn validate(&self, _: &prost_types::Any) -> Result<()> { Ok(()) }
        async fn open(&self, _: &prost_types::Any, _: &str, _: Arc<rtdb_storage::IoRegion>) -> Result<Box<dyn ExternalSource>> {
            Ok(Box::new(self.clone()))
        }
    }

    #[async_trait]
    impl ExternalSource for Source {
        async fn assign(&mut self) -> Result<Box<dyn ExternalSourceDriver>> { Ok(Box::new(self.clone())) }
    }

    #[async_trait]
    impl ExternalSourceDriver for Source {
        async fn run(self: Box<Self>, term: rtdb_replication::TermHandle, _: Option<Arc<[u8]>>,
                     _: tokio::sync::watch::Receiver<rtdb_replication::DriverStatus>,
                     cancellation: CancellationToken) -> Result<()> {
            for _ in 0..2 {
                let result = term.resolve_external(Box::new(Writer), None, None).await?;
                assert!(!result.committed());
                self.0.send(result.tx_key().tx_id).unwrap();
            }
            cancellation.cancelled().await;
            Ok(())
        }
    }

    #[async_trait]
    impl rtdb_tx::TransactionWriter for Writer {
        async fn write(&mut self, _: &mut rtdb_tx::OpenTransaction<'_>) -> Result<rtdb_tx::WriterResult> {
            let error = Anomaly::incorrect("rtdb.tx/private-replay-duplicate", "External source diagnostic")
                .with_context("original-tx-id", 777.into())
                .with_context("original-system-time-micros", 1000.into());
            Ok(rtdb_tx::WriterResult::Aborted { error, user_metadata: None })
        }
    }

    tokio::time::timeout(Duration::from_secs(20), async {
        let (receipts, mut applied) = tokio::sync::mpsc::unbounded_channel();
        let mut services = services();
        services.external_sources.insert("test.PrivateReplayError".into(), Arc::new(Source(receipts)));
        let mut settings = options();
        settings.membership = Membership::Durable {
            primary: DatabaseConfig {
                external_source: Some(prost_types::Any { type_url: "test.PrivateReplayError".into(), value: vec![] }),
                ..Default::default()
            },
            skipped: BTreeSet::new(),
        };
        let (node, driver) = running(settings, services).await;
        let first = applied.recv().await.unwrap();
        let second = applied.recv().await.unwrap();
        assert!(first < second);
        let cancellation = CancellationToken::new();
        for id in [second, first] {
            let result = node.await_transaction("xtdb", id, &Authority, &cancellation).await.unwrap();
            assert_eq!(result.tx_key().tx_id, id);
            assert_eq!(result.error().map(|error| error.message.as_deref().unwrap()), Some("External source diagnostic"));
        }
        stop(&node, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn concurrent_private_replays_commit_one_effect_and_return_the_same_key() {
    let (node, driver) = running(options(), services()).await;
    let mut first = node.session("xtdb", Arc::new(Authority)).unwrap();
    let mut second = node.session("xtdb", Arc::new(Authority)).unwrap();
    let key = "00112233445566778899aabbccddeeff/0";
    let cutoff = Timestamp(7_200_000_000);
    let (first_result, second_result) = tokio::join!(
        commit_private(&mut first, put(&node, 1, 10), key, cutoff),
        commit_private(&mut second, put(&node, 2, 20), key, cutoff),
    );
    let (CommitResult::Executed(first_result), CommitResult::Executed(second_result)) =
        (first_result.unwrap(), second_result.unwrap())
    else { panic!("expected synchronous results") };
    assert!(first_result.committed() && second_result.committed());
    assert_eq!(first_result.tx_key(), second_result.tx_key());
    assert_eq!(rows(first.query_sql("SELECT COUNT(*) FROM docs FOR ALL VALID_TIME", vec![]).await.unwrap()).await,
        [vec![Value::I64(1)]]);
    drop((first, second));
    stop(&node, driver).await;
}

#[tokio::test]
async fn a_private_replay_marker_covers_out_of_order_samples_and_survives_restart() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let directory = tempfile::tempdir().unwrap();
        let config = DatabaseConfig {
            log: Some(database_config::Log::LocalLog(LocalLog {
                path: Some(directory.path().join("log").to_string_lossy().into_owned()), epoch: Some(0),
            })),
            storage: Some(database_config::Storage::LocalStorage(LocalStorage {
                path: Some(directory.path().join("store").to_string_lossy().into_owned()), epoch: Some(0),
            })),
            ..Default::default()
        };
        let clock = Arc::new(ReplayTestClock(AtomicI64::new(1_000_000)));
        let runtime = NodeServices { clock: clock.clone(), ..services() };
        let mut settings = options();
        settings.membership = Membership::Durable { primary: config, skipped: BTreeSet::new() };
        let (node, driver) = running(settings.clone(), fresh_local_services(&runtime)).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        let key = "00112233445566778899aabbccddeeff/0";
        let cutoff = Timestamp(7_200_000_000);
        let failure = commit_private(&mut session, TxOp::Sql { sql: "ASSERT FALSE".into(), args: None }, key, cutoff)
            .await.unwrap();
        let CommitResult::Executed(failure) = failure else { panic!("expected synchronous failure") };
        assert_eq!(failure.error().map(|error| error.code.as_str()), Some("xtdb/assert-failed"));
        let original = commit_private(&mut session, put(&node, 1, 10), key, cutoff).await.unwrap();
        let CommitResult::Executed(original) = original else { panic!("expected synchronous commit") };
        assert!(original.committed());

        for sql in [
            "INSERT INTO xt.rtdb_replay_keys (_id, original_tx_id) VALUES ('00112233445566778899aabbccddeeff/0', -1)",
            "ERASE FROM xt.rtdb_replay_keys WHERE _id = '00112233445566778899aabbccddeeff/0'",
        ] {
            let outcome = session.state_mut().execute_tx(vec![TxOp::Sql { sql: sql.into(), args: None }], AutonomousTxOptions::default()).await;
            let error = match &outcome {
                Ok(result) => result.error().expect("system marker mutation must abort"),
                Err(error) => error,
            };
            assert_eq!(error.code, "xtdb/forbidden-table", "{sql}");
        }

        clock.0.store(0, Ordering::Relaxed);
        let earlier = commit_private(&mut session, put(&node, 2, 20), key, cutoff).await.unwrap();
        let CommitResult::Executed(earlier) = earlier else { panic!("expected synchronous retry") };
        assert_eq!(earlier.tx_key(), original.tx_key());
        node.catalog().database("xtdb").unwrap().flush().await.unwrap();
        let cancellation = CancellationToken::new();
        node.sync_all(Duration::from_secs(10), &cancellation).await.unwrap();
        drop(session);
        stop(&node, driver).await;

        clock.0.store(cutoff.0 - 1, Ordering::Relaxed);
        let (reopened, driver) = running(settings.clone(), fresh_local_services(&runtime)).await;
        let mut session = reopened.session("xtdb", Arc::new(Authority)).unwrap();
        let later = commit_private(&mut session, put(&reopened, 3, 30), key, cutoff).await.unwrap();
        let CommitResult::Executed(later) = later else { panic!("expected recovered retry") };
        assert_eq!(later.tx_key(), original.tx_key());
        let last = reopened.catalog().database("xtdb").unwrap().progress().current().tx_key.unwrap();
        assert_ne!(last.tx_id, original.tx_key().tx_id);
        drop(session);
        stop(&reopened, driver).await;

        let (reopened, driver) = running(settings, fresh_local_services(&runtime)).await;
        let recovered_outcome = reopened.await_transaction("xtdb", last.tx_id, &Authority, &cancellation).await.unwrap();
        assert!(recovered_outcome.committed());
        assert_eq!(recovered_outcome.tx_key(), original.tx_key());
        let mut session = reopened.session("xtdb", Arc::new(Authority)).unwrap();
        assert_eq!(rows(session.query_sql("SELECT _id FROM docs FOR ALL VALID_TIME", vec![]).await.unwrap()).await,
            [vec![Value::I64(1)]]);
        drop(session);
        stop(&reopened, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn a_supplied_system_time_commits_even_ahead_of_the_log_and_refuses_one_not_strictly_later() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let at = |time| AutonomousTxOptions { system_time: Some(Timestamp(time)), ..Default::default() };
    let first = session.state_mut().execute_tx(vec![put(&node, 1, 10)], at(100)).await.unwrap();
    assert!(first.committed(), "{:?}", first.error());
    assert_eq!(first.tx_key().system_time, Timestamp(100));

    let token = session.state().cancellation().child_token();
    let parse = |sql| rtdb_sql::parse_control(sql, &Default::default()).unwrap().unwrap();
    session.execute_control(&parse("BEGIN READ WRITE WITH (SYSTEM_TIME TIMESTAMP '1970-01-01T00:00:00.0002Z')"), &[], &token).await.unwrap();
    session.state_mut().execute_operation(TxOp::Sql { sql: "INSERT INTO docs (_id, value) VALUES (1, 20)".into(), args: None }).await.unwrap();
    let CommitResult::Executed(second) = session.state_mut().commit_tx(None).await.unwrap() else { panic!("expected a synchronous commit") };
    assert!(second.committed(), "{:?}", second.error());
    assert_eq!(second.tx_key().system_time, Timestamp(200));

    for requested in [200, 150] {
        let refused = session.state_mut().execute_tx(vec![put(&node, 1, requested)], at(requested)).await.unwrap();
        assert_eq!(refused.error().map(|error| error.code.as_str()), Some("invalid-system-time"), "{requested}");
        assert!(refused.tx_key().system_time.0 > 200);
    }
    assert_eq!(rows(session.query_sql("SELECT value FROM docs", vec![]).await.unwrap()).await, [vec![Value::I64(20)]]);
    let between = "SELECT value FROM docs FOR SYSTEM_TIME AS OF TIMESTAMP '1970-01-01T00:00:00.00015Z'";
    assert_eq!(rows(session.query_sql(between, vec![]).await.unwrap()).await, [vec![Value::I64(10)]]);

    // As in XTDB, a time ahead of the log commits, and the next default time follows it by one tick.
    let future = 4_102_444_800_000_000; // 2100-01-01T00:00:00Z
    let ahead = session.state_mut().execute_tx(vec![put(&node, 1, 30)], at(future)).await.unwrap();
    assert!(ahead.committed(), "{:?}", ahead.error());
    assert_eq!(ahead.tx_key().system_time, Timestamp(future));
    let tied = session.state_mut().execute_tx(vec![put(&node, 1, 35)], at(future)).await.unwrap();
    assert_eq!(tied.error().map(|error| error.code.as_str()), Some("invalid-system-time"));
    assert_eq!(tied.tx_key().system_time, Timestamp(future + 1));
    let next = session.state_mut().execute_tx(vec![put(&node, 1, 40)], AutonomousTxOptions::default()).await.unwrap();
    assert!(next.committed(), "{:?}", next.error());
    assert_eq!(next.tx_key().system_time, Timestamp(future + 2));
    let latest = "SELECT value FROM docs FOR VALID_TIME AS OF TIMESTAMP '2100-01-01T00:00:00.000002Z' \
        FOR SYSTEM_TIME AS OF TIMESTAMP '2100-01-01T00:00:00.000002Z'";
    assert_eq!(rows(session.query_sql(latest, vec![]).await.unwrap()).await, [vec![Value::I64(40)]]);
    drop(session);
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
    command("first", [1, 2]).await;
    command("second", [5, 6]).await;
    let mut altered = node.session("xtdb", Arc::new(Authority)).unwrap();
    altered.state_mut().set_parameter(rtdb_node::IDEMPOTENCY_KEY_PARAMETER, Some("first".into())).unwrap();
    let refused = altered.state_mut().execute_tx(vec![put(&node, 3, 3)], AutonomousTxOptions::default()).await.unwrap();
    assert_eq!(refused.error().map(|error| error.code.as_str()), Some("rtdb/idempotency-conflict"), "a resend that changes a transaction is refused");
    drop(altered);
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
async fn an_ordered_sql_write_can_encode_its_own_revision_exactly() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let ordered = index_sql(&mut session,
        "INSERT INTO revisions (_id, value) VALUES (1,
            OVERLAY(X('6162') PLACING EPOCH_NANOS_BE(CURRENT_TIMESTAMP) FROM 2 FOR 0))").await;
    let word = (u64::try_from(ordered.0).unwrap() * 1_000).to_be_bytes();
    let expected = [b"a".as_slice(), &word, b"b"].concat();
    assert_eq!(rows(session.query_sql("SELECT value FROM revisions", vec![]).await.unwrap()).await,
        [vec![Value::Binary(expected)]]);
    stop(&node, driver).await;
}

#[derive(Clone, Default)]
struct BatchSortTrace(Arc<std::sync::Mutex<BTreeMap<String, u64>>>);

impl rtdb_plan::Tracer for BatchSortTrace {
    fn query_enabled(&self) -> bool { true }
    fn transaction_enabled(&self) -> bool { false }
    fn start(&self, _: Option<rtdb_plan::SpanContext>, _: &str, _: &[(&str, rtdb_plan::TraceValue<'_>)]) -> Option<Box<dyn rtdb_plan::TraceSpan>> {
        Some(Box::new(self.clone()))
    }
}

impl rtdb_plan::TraceSpan for BatchSortTrace {
    fn context(&self) -> rtdb_plan::SpanContext {
        rtdb_plan::SpanContext { trace_id: [1; 16], span_id: [1; 8] }
    }
    fn attribute(&mut self, name: &str, value: rtdb_plan::TraceValue<'_>) {
        let value = match value {
            rtdb_plan::TraceValue::Unsigned(value) => value,
            rtdb_plan::TraceValue::Boolean(value) => u64::from(value),
            _ => return,
        };
        let mut trace = self.0.lock().unwrap();
        trace.entry(name.into()).and_modify(|old| *old = (*old).max(value)).or_insert(value);
    }
    fn end_at(&mut self, _: std::time::SystemTime) {}
}

#[tokio::test]
async fn arrow_probe_unions_sort_stably_in_memory_and_in_spill_runs() {
    for (sort_memory_bytes, spills) in [(1 << 20, 0), (1, 1)] {
        let trace = BatchSortTrace::default();
        let mut services = services();
        services.tracer = Some(Arc::new(trace.clone()));
        let mut config = options();
        config.query.sort_memory_bytes = sort_memory_bytes;
        config.query.batch_rows = 2;
        let (node, driver) = running(config, services).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        index_sql(&mut session, "INSERT INTO histories RECORDS
            {_id: 1, world: 1, key: 'b', idx: 0, value: X('42')},
            {_id: 2, world: 1, key: 'a', idx: 0, value: X('4c')},
            {_id: 3, world: 2, key: 'a', idx: 0, value: X('52')},
            {_id: 4, world: 2, key: 'b', idx: 0, value: X('4e')}").await;
        let payloads = "SELECT key, value FROM (
            SELECT 0::BIGINT AS probe, key, value, _system_from, idx FROM histories WHERE world = 1
            UNION ALL
            SELECT 0::BIGINT AS probe, key, value, _system_from, idx FROM histories WHERE world = 2
        ) AS worldstore_probes ORDER BY probe, key, _system_from DESC, idx DESC";
        let context = session.query_context().await.unwrap();
        let prepared = PreparedSql::prepare(payloads, &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
        let result = rows(prepared.open_query_columns_typed(context.clone(), vec![], &[]).await.unwrap()).await;
        assert_eq!(result, [("a", b'L'), ("a", b'R'), ("b", b'B'), ("b", b'N')]
            .map(|(key, payload)| vec![Value::Utf8(key.into()), Value::Binary(vec![payload])]));
        assert_eq!(context.spill_bytes(), 0);
        {
            let mut trace = trace.0.lock().unwrap();
            assert_eq!(trace.get("sort_columnar"), Some(&1));
            assert_eq!(trace.get("sort_arrow_keys"), Some(&1));
            assert_eq!(trace.get("union_all_columnar"), Some(&1));
            assert_eq!(trace.get("sort_columnar_spill"), Some(&spills));
            trace.clear();
        }

        let keys = "SELECT * FROM (
            SELECT 0::BIGINT AS probe, a.key FROM (
                SELECT DISTINCT key FROM histories WHERE world = 1 ORDER BY key ASC LIMIT 1) AS a
            UNION ALL
            SELECT 1::BIGINT AS probe, b.key FROM (
                SELECT DISTINCT key FROM histories WHERE world = 2 ORDER BY key DESC LIMIT 1) AS b
        ) AS worldstore_probes ORDER BY probe, key";
        let prepared = PreparedSql::prepare(keys, &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
        assert_eq!(rows(prepared.open_query_columns_typed(context, vec![], &[]).await.unwrap()).await, [
            vec![Value::I64(0), Value::Utf8("a".into())],
            vec![Value::I64(1), Value::Utf8("b".into())],
        ]);
        assert_eq!(trace.0.lock().unwrap().get("union_all_columnar"), Some(&1));
        stop(&node, driver).await;
    }
}

#[tokio::test]
async fn columnar_sort_spills_for_buffers_held_by_selected_payloads() {
    let trace = BatchSortTrace::default();
    let mut services = services();
    services.tracer = Some(Arc::new(trace.clone()));
    let mut config = options();
    config.automatic_compaction = false;
    config.query.sort_memory_bytes = 32 * 1024;
    config.query.batch_rows = 2;
    let (node, driver) = running(config, services).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    let payload = |id| Value::FixedSizeList(vec![Value::I64(id)].into());
    for id in [3, 1, 2] {
        let documents = Documents::new(vec![StructFields::from([
            ("_id".into(), Value::I64(id)), ("cost".into(), Value::I64(4 - id)), ("payload".into(), payload(id)),
            ("unprojected".into(), Value::Binary(vec![id as u8; 64 * 1024])),
        ])], node.memory()).unwrap();
        let operation = TxOp::PutDocs {
            table: TableRef::new("public", "loads"), documents, valid_from: None, valid_to: None,
        };
        let result = session.state_mut().execute_tx(vec![operation], AutonomousTxOptions::default()).await.unwrap();
        assert!(result.committed(), "{result:?}");
        index_flush(&node, &database).await;
    }
    let context = session.query_context().await.unwrap();
    let prepared = PreparedSql::prepare(
        "SELECT _id, payload FROM loads ORDER BY cost", &context, Default::default(), ResultSignaturePolicy::Dynamic,
    ).unwrap();

    let result = rows(prepared.open_query_columns_typed(context, vec![], &[]).await.unwrap()).await;

    assert_eq!(result, [3, 2, 1].map(|id| vec![Value::I64(id), payload(id)]));
    let reported_spill = trace.0.lock().unwrap().get("sort_columnar_spill").copied();
    assert_eq!(reported_spill, Some(1));
    stop(&node, driver).await;
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
    let bytes = database.object_store().get(&manifests[0].key).await.unwrap();
    assert!(bytes.starts_with(&[0xff; 4]));
    let mut stream = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
    let manifest = stream.next().unwrap().unwrap();
    let integer = |name| manifest.column_by_name(name).unwrap().as_any()
        .downcast_ref::<arrow_array::Int64Array>().unwrap().value(0);
    assert_eq!(integer("source_rows"), 6);
    assert_eq!(manifest.column_by_name("revisions").unwrap().null_count(), 0);

    let measured = rows(session.query_sql(
        "SELECT index_name, definition_version, through_block, file_bytes \
         FROM xt.live_ordered_index_storage WHERE schema_name = 'public' AND table_name = 'orders'",
        vec![],
    ).await.unwrap()).await;
    let [entry] = measured.as_slice() else { panic!("Expected exactly one published index: {measured:?}"); };
    let [Value::Utf8(name), Value::I64(version), Value::I64(through), Value::I64(bytes)] = entry.as_slice()
    else { panic!("Expected one published index's metadata: {entry:?}"); };
    assert_eq!(name, "orders_by_customer");
    assert!(*version >= 0);
    assert_eq!(*through, integer("through"));
    let published_bytes: u64 = paths.iter().filter(|entry| {
        entry.key.as_str().contains("/pages/") || entry.key.as_str().contains("/manifests/")
    }).map(|entry| entry.size).sum();
    let manifest_bytes = manifests[0].size;
    assert!((*bytes as u64) > manifest_bytes);
    assert!((*bytes as u64) <= published_bytes);
    assert!(database.snapshot().trie_entries(&TableRef::new("public", "orders")).iter().all(|trie| trie.key.level() == 0));

    let warm = "SELECT _id FROM orders WHERE customer = 'a' ORDER BY priority, _id LIMIT 1";
    assert_eq!(rows(session.query_sql(warm, vec![]).await.unwrap()).await, [vec![Value::I64(1)]]);
    assert!(observer.take().is_empty());

    let bounded = "SELECT _id FROM orders WHERE customer = 'a' ORDER BY priority, _id LIMIT ? OFFSET ?";
    assert_eq!(rows(session.query_sql(bounded, vec![Value::I64(2), Value::I64(1)]).await.unwrap()).await,
        [vec![Value::I64(2)], vec![Value::I64(4)]]);
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
async fn ordered_index_pages_zoned_instants_at_head_and_at_a_retained_system_time() {
    let observer = Arc::new(OrderedIndexReads::default());
    let mut services = services();
    services.scan_accelerators.push(observer.clone());
    let mut config = options();
    config.automatic_artefacts = false;
    let (node, driver) = running(config, services).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    index_sql(
        &mut session,
        "CREATE INDEX orders_effective ON orders (effective, _id)",
    )
    .await;

    let instant = |value, zone: &str| Value::Timestamp {
        value,
        unit: rtdb_types::TimeUnit::Microsecond,
        timezone: Some(zone.into()),
    };
    let row = |id, micros| {
        StructFields::from([
            ("_id".into(), Value::I64(id)),
            ("effective".into(), instant(micros, "UTC")),
        ])
    };
    let past = session
        .state_mut()
        .execute_tx(
            vec![put_rows(
                &node,
                "orders",
                vec![row(1, -1), row(2, 0), row(3, 0), row(4, 2)],
                None,
                None,
            )],
            AutonomousTxOptions::default(),
        )
        .await
        .unwrap();
    assert!(past.committed());
    index_flush(&node, &database).await;
    database.build_artefacts().await.unwrap();

    let revised = session
        .state_mut()
        .execute_tx(
            vec![put_rows(&node, "orders", vec![row(1, 3)], None, None)],
            AutonomousTxOptions::default(),
        )
        .await
        .unwrap();
    assert!(revised.committed());
    observer.take();

    let current = "SELECT _id, effective FROM orders ORDER BY effective DESC, _id DESC LIMIT 2";
    assert_eq!(
        rows(session.query_sql(current, vec![]).await.unwrap()).await,
        [
            vec![Value::I64(1), instant(3, "UTC")],
            vec![Value::I64(4), instant(2, "UTC")],
        ]
    );
    assert!(observer.take().is_empty());

    let old = "SELECT _id, effective FROM orders FOR SYSTEM_TIME AS OF ?
               WHERE effective >= ? ORDER BY effective, _id LIMIT 1";
    let basis = instant(past.tx_key().system_time.0, "UTC");
    assert_eq!(
        rows(
            session
                .query_sql(old, vec![basis.clone(), instant(-1, "+01:00")])
                .await
                .unwrap()
        )
        .await,
        [vec![Value::I64(1), instant(-1, "UTC")],]
    );
    assert!(observer.take().is_empty());

    let continuation = "SELECT _id FROM orders FOR SYSTEM_TIME AS OF ?
                        WHERE effective > ? OR (effective = ? AND _id > 2)
                        ORDER BY effective, _id LIMIT 2";
    let boundary = instant(0, "+01:00");
    assert_eq!(
        rows(
            session
                .query_sql(continuation, vec![basis, boundary.clone(), boundary])
                .await
                .unwrap()
        )
        .await,
        [vec![Value::I64(3)], vec![Value::I64(4)],]
    );
    assert!(observer.take().is_empty());
    stop(&node, driver).await;
}

#[tokio::test]
async fn ordered_float_keys_sort_signed_zeros_and_keep_nan_out_of_comparisons() {
    let observer = Arc::new(OrderedIndexReads::default());
    let mut services = services();
    services.scan_accelerators.push(observer.clone());
    let mut config = options();
    config.automatic_artefacts = false;
    let (node, driver) = running(config, services).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    index_sql(&mut session, "CREATE INDEX orders_amount ON orders (amount, _id)").await;
    let input = [
        (1, 0.0), (2, -0.0), (3, f64::NAN), (4, -f64::NAN),
        (5, 0.5), (6, f64::NEG_INFINITY), (7, f64::INFINITY),
    ].into_iter().map(|(id, amount)| StructFields::from([
        ("_id".into(), Value::I64(id)), ("amount".into(), Value::F64(amount)),
    ])).collect();
    let commit = session.state_mut().execute_tx(
        vec![put_rows(&node, "orders", input, None, None)],
        AutonomousTxOptions::default(),
    ).await.unwrap();
    assert!(commit.committed());
    index_flush(&node, &database).await;
    database.build_artefacts().await.unwrap();
    let ids = |ids: &[i64]| ids.iter().map(|&id| vec![Value::I64(id)]).collect::<Vec<_>>();
    let all = "SELECT _id FROM orders ORDER BY amount, _id LIMIT 7";
    assert_eq!(rows(session.query_sql(all, vec![]).await.unwrap()).await, ids(&[6, 2, 1, 5, 7, 3, 4]));
    assert!(observer.take().is_empty(), "the ordered seek should select the rows");

    let above = "SELECT _id FROM orders WHERE amount > ? ORDER BY amount DESC, _id DESC LIMIT 1";
    assert_eq!(rows(session.query_sql(above, vec![Value::F64(0.25)]).await.unwrap()).await, ids(&[7]));
    assert!(observer.take().is_empty(), "NaN cannot occupy an indexed limit slot");

    let zeros = "SELECT _id FROM orders WHERE amount = ? ORDER BY amount, _id LIMIT 2";
    assert_eq!(rows(session.query_sql(zeros, vec![Value::F64(0.0)]).await.unwrap()).await, ids(&[2, 1]));
    stop(&node, driver).await;
}

#[tokio::test]
async fn ordered_index_gathers_projected_table_columns_at_the_original_basis() {
    let observer = Arc::new(OrderedIndexReads::default());
    let mut services = services();
    services.scan_accelerators.push(observer.clone());
    let mut config = options();
    config.automatic_artefacts = false;
    let (node, driver) = running(config, services).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    index_sql(&mut session, "CREATE INDEX orders_key ON orders (customer, priority, _id)").await;
    index_sql(&mut session,
        "INSERT INTO orders RECORDS {_id: 1, customer: 'a', priority: 3, payload: 'third', detail: {a: 3}},
         {_id: 2, customer: 'a', priority: 1, payload: 'first', detail: {a: 1}}").await;
    index_flush(&node, &database).await;
    let past = index_sql(&mut session,
        "INSERT INTO orders RECORDS {_id: 3, customer: 'a', priority: 2, payload: 'second', detail: {b: 2}},
         {_id: 4, customer: 'a', priority: 4}").await;
    index_flush(&node, &database).await;
    database.build_artefacts().await.unwrap();
    index_sql(&mut session, "UPDATE orders SET payload = 'changed' WHERE _id = 3").await;

    observer.take();
    let query = "SELECT payload, _id FROM orders FOR SYSTEM_TIME AS OF ? WHERE customer = 'a'
                 ORDER BY priority, _id LIMIT 3 OFFSET 1";
    let at = Value::Timestamp { value: past.0, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) };
    assert_eq!(rows(session.query_sql(query, vec![at]).await.unwrap()).await, [
        vec![Value::Utf8("second".into()), Value::I64(3)],
        vec![Value::Utf8("third".into()), Value::I64(1)],
        vec![Value::Null, Value::I64(4)],
    ]);
    assert!(observer.take().is_empty(), "the index selected IIDs before reading table payload");
    let query = "SELECT payload, _id FROM orders FOR SYSTEM_TIME AS OF ? WHERE customer = 'a'
                 AND payload <> 'second' ORDER BY priority, _id LIMIT 1 OFFSET 1";
    let at = Value::Timestamp { value: past.0, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) };
    assert_eq!(rows(session.query_sql(query, vec![at]).await.unwrap()).await,
        [vec![Value::Utf8("third".into()), Value::I64(1)]]);
    assert!(observer.take().is_empty(), "the index feeds the columnar residual filter");
    let query = "SELECT detail, _id FROM orders FOR SYSTEM_TIME AS OF ? WHERE customer = 'a'
                 ORDER BY priority, _id LIMIT 3 OFFSET 1";
    let at = Value::Timestamp { value: past.0, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) };
    let record = |a, b| Value::Struct(StructFields::from([("a".into(), a), ("b".into(), b)]));
    assert_eq!(rows(session.query_sql(query, vec![at]).await.unwrap()).await, [
        vec![record(Value::Null, Value::I64(2)), Value::I64(3)],
        vec![record(Value::I64(3), Value::Null), Value::I64(1)],
        vec![Value::Null, Value::I64(4)],
    ]);
    assert!(observer.take().is_empty(), "the ordered seek supplied the nested payload rows");
    let input = (10..310).map(|id| StructFields::from([
        ("_id".into(), Value::I64(id)), ("customer".into(), Value::Utf8("batch".into())),
        ("priority".into(), Value::I64(id)),
        ("payload".into(), Value::Utf8(if id % 3 == 0 { "skip" } else { "keep" }.into())),
    ])).collect();
    let commit = session.state_mut().execute_tx(
        vec![put_rows(&node, "orders", input, None, None)],
        AutonomousTxOptions::default(),
    ).await.unwrap();
    assert!(commit.committed());
    index_flush(&node, &database).await;
    database.build_artefacts().await.unwrap();
    observer.take();
    let query = "SELECT payload, _id FROM orders WHERE customer = 'batch' AND payload <> 'skip'
                 ORDER BY priority, _id LIMIT 140 OFFSET 20";
    let expected = (10..310).filter(|id| id % 3 != 0).skip(20).take(140)
        .map(|id| vec![Value::Utf8("keep".into()), Value::I64(id)]).collect::<Vec<_>>();
    assert_eq!(rows(session.query_sql(query, vec![]).await.unwrap()).await, expected);
    assert!(observer.take().is_empty(), "the ordered payload cursor supplied multiple candidate batches");
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

    let table = TableRef::new("public", "orders");
    let snapshot = database.snapshot();
    let entry = snapshot.table_entry(&table).unwrap();
    let trie = rtdb_format::keys::TrieKey::l0(snapshot.trie_catalog().l0_max_block_index(&table)).unwrap();
    let primary = rtdb_storage::ObjectKey::new(entry.slug.data_key(&trie)).unwrap();
    database.object_store().delete(&primary).await.unwrap();
    database.storage().invalidate(&primary).await.unwrap();

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
    let offset = "SELECT DISTINCT key FROM orders WHERE world = 4 AND (label = 'low' OR label = 'public')
        ORDER BY key ASC LIMIT 1 OFFSET 1";
    assert_eq!(rows(session.query_sql(offset, vec![]).await.unwrap()).await, [vec![Value::Utf8("c".into())]]);
    assert!(observer.take().is_empty());
    stop(&node, driver).await;
}

#[tokio::test]
async fn ordered_distinct_compound_prefix_pages_without_table_payloads() {
    let observer = Arc::new(OrderedIndexReads::default());
    let mut services = services();
    services.scan_accelerators.push(observer.clone());
    let mut config = options();
    config.automatic_artefacts = false;
    let (node, driver) = running(config, services).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    index_sql(
        &mut session,
        "CREATE INDEX history_order ON orders (world, tick, document, audience)",
    ).await;
    index_sql(&mut session, "INSERT INTO orders RECORDS
        {_id: 1, world: 1, tick: 10, document: 'a', audience: 'hidden'},
        {_id: 2, world: 1, tick: 10, document: 'b', audience: 'public'},
        {_id: 3, world: 1, tick: 10, document: 'b', audience: 'public'},
        {_id: 4, world: 1, tick: 10, document: 'c', audience: 'public'},
        {_id: 5, world: 1, tick: 11, document: 'b', audience: 'public'},
        {_id: 6, world: 1, tick: 11, document: NULL, audience: 'public'},
        {_id: 7, world: 2, tick: 10, document: 'a', audience: 'public'}").await;
    index_flush(&node, &database).await;
    database.build_artefacts().await.unwrap();

    let table = TableRef::new("public", "orders");
    let snapshot = database.snapshot();
    let entry = snapshot.table_entry(&table).unwrap();
    let trie = rtdb_format::keys::TrieKey::l0(snapshot.trie_catalog().l0_max_block_index(&table)).unwrap();
    let primary = rtdb_storage::ObjectKey::new(entry.slug.data_key(&trie)).unwrap();
    database.object_store().delete(&primary).await.unwrap();
    database.storage().invalidate(&primary).await.unwrap();

    let query = "SELECT DISTINCT tick, document FROM orders WHERE world = 1
        AND document IS NOT NULL AND audience = 'public'
        AND (tick > 10 OR (tick = 10 AND document >= 'b'))
        ORDER BY tick, document LIMIT 2";
    let pair = |tick, id: &str| vec![Value::I64(tick), Value::Utf8(id.into())];
    assert_eq!(rows(session.query_sql(query, vec![]).await.unwrap()).await,
        [pair(10, "b"), pair(10, "c")]);
    let reverse = "SELECT DISTINCT tick, document FROM orders WHERE world = 1
        AND document IS NOT NULL AND audience = 'public'
        ORDER BY tick DESC, document DESC LIMIT 1 OFFSET 1";
    assert_eq!(rows(session.query_sql(reverse, vec![]).await.unwrap()).await,
        [pair(10, "c")]);
    assert!(observer.take().is_empty(), "compound pages were served from index postings");
    stop(&node, driver).await;
}

#[tokio::test]
async fn a_covered_ordered_scan_yields_a_batch_before_opening_later_leaves() {
    let observer = Arc::new(OrderedIndexReads::default());
    let mut services = services();
    services.scan_accelerators.push(observer.clone());
    let mut config = options();
    config.automatic_artefacts = false;
    let (node, driver) = running(config, services).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    index_sql(&mut session, "CREATE INDEX orders_key ON orders (world, key, label)").await;
    let records = (0..400).map(|id| format!("{{_id: {id}, world: 1, key: 'key-{id:04}', label: 'visible'}}"))
        .collect::<Vec<_>>().join(", ");
    index_sql(&mut session, &format!("INSERT INTO orders RECORDS {records}")).await;
    index_flush(&node, &database).await;
    database.build_artefacts().await.unwrap();
    let query = "SELECT DISTINCT key FROM orders WHERE world = 1 AND label = 'visible' ORDER BY key LIMIT 400";
    let mut cursor = session.query_sql(query, vec![]).await.unwrap();
    let first = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(first.batch().num_rows(), 128);
    assert_eq!(first.batch().num_columns(), 1);
    assert!(observer.take().is_empty(), "the first batch came from the ordered index");

    let objects = database.object_store();
    let root = rtdb_storage::ObjectKey::new(rtdb_format::keys::ORDERED_INDEX_ROOT).unwrap();
    let mut removed = 0;
    for page in objects.list(Some(&root)).await.unwrap() {
        if page.key.as_str().contains("/pages/") {
            objects.delete(&page.key).await.unwrap();
            removed += 1;
        }
    }
    assert!(removed > 0);
    let mut delivered = first.batch().num_rows();
    loop {
        match cursor.next_batch().await {
            Ok(Some(batch)) => delivered += batch.batch().num_rows(),
            Ok(None) => panic!("The index stream completed after {delivered} rows with later pages unavailable"),
            Err(error) => {
                assert_eq!(error.code, "rtdb.storage/missing-object");
                assert!(error.context["key"].as_str().is_some_and(|key| key.contains("/pages/")));
                break;
            }
        }
    }
    assert!(delivered < 400);
    drop(cursor);
    stop(&node, driver).await;
}

#[tokio::test]
async fn ordered_index_retires_erased_keys_and_drop_recreate_gets_a_fresh_generation() {
    let mut config = options();
    config.automatic_artefacts = false;
    config.ordered_index_arrow_references_on_private_storage = false;
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
async fn ordered_index_rebuilds_missing_l0_from_a_reopened_journal_without_resurrecting_erased_keys() {
    use rtdb_format::keys::TrieKey;
    use rtdb_storage::ObjectKey;

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
    index_sql(&mut session, "CREATE INDEX orders_key ON orders (customer, _id)").await;
    let original = index_sql(&mut session, "INSERT INTO orders RECORDS {_id: 1, customer: 'removed-secret'}, {_id: 2, customer: 'retained'}").await;
    index_flush(&node, &database).await;
    let table = TableRef::new("public", "orders");
    let old_l0 = {
        let snapshot = database.snapshot();
        let trie = TrieKey::l0(snapshot.trie_catalog().l0_max_block_index(&table)).unwrap();
        ObjectKey::new(snapshot.table_entry(&table).unwrap().slug.data_key(&trie)).unwrap()
    };
    let erase = ["INSERT INTO orders RECORDS {_id: 1, customer: 'removed-in-revision'}",
        "ERASE FROM orders WHERE _id = 1",
        "INSERT INTO orders RECORDS {_id: 1, customer: 'z-reinserted'}"];
    let result = session.state_mut().execute_tx(
        erase.into_iter().map(|sql| TxOp::Sql { sql: sql.into(), args: None }).collect(),
        AutonomousTxOptions::default(),
    ).await.unwrap();
    assert!(result.committed());
    index_flush(&node, &database).await;
    database.build_artefacts().await.unwrap();
    let prefix = ObjectKey::new("ordered-index/v2").unwrap();
    let baseline = {
        let store = database.object_store();
        let mut baseline = BTreeMap::new();
        for object in store.list(Some(&prefix)).await.unwrap() {
            baseline.insert(object.key.clone(), store.get(&object.key).await.unwrap());
            store.delete(&object.key).await.unwrap();
        }
        assert!(baseline.keys().any(|key| key.as_str().contains("/manifests/")));
        baseline
    };
    drop(session);
    drop(database);
    stop(&node, driver).await;
    drop(node);

    let (node, driver) = running(config, fresh_local_services(&template)).await;
    node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    let store = database.object_store();
    store.delete(&old_l0).await.unwrap();
    database.build_artefacts().await.unwrap();
    let mut rebuilt = BTreeMap::new();
    for object in store.list(Some(&prefix)).await.unwrap() {
        rebuilt.insert(object.key.clone(), store.get(&object.key).await.unwrap());
    }
    assert_eq!(rebuilt, baseline);
    assert!(matches!(store.head(&old_l0).await, Err(rtdb_storage::Error::MissingObject(_))));

    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    observer.take();
    let current = "SELECT _id FROM orders WHERE customer >= 'a' ORDER BY customer, _id LIMIT 2";
    assert_eq!(rows(session.query_sql(current, vec![]).await.unwrap()).await, [vec![Value::I64(2)], vec![Value::I64(1)]]);
    let historical = "SELECT _id FROM orders FOR SYSTEM_TIME AS OF ? WHERE customer >= 'a' ORDER BY customer, _id LIMIT 2";
    let at = Value::Timestamp { value: original.0, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) };
    assert_eq!(rows(session.query_sql(historical, vec![at]).await.unwrap()).await, [vec![Value::I64(2)]]);
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
async fn held_owned_changes_preserve_whole_groups_and_cover_the_ordered_cut() {
    use rtdb_types::{EntityId, OwnedChangeKind, OwnedRelation};

    let mut settings = options();
    settings.owned_relations = true;
    let (node, driver) = running(settings, services()).await;
    let mut writer = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("feed").unwrap();
    let other = OwnedRelation::new("other-feed").unwrap();
    let empty = writer.open_owned_read().await.unwrap();
    let page = empty.changes(&relation, Timestamp::MIN, 1).await.unwrap();
    assert!(page.groups.is_empty());
    assert_eq!(page.covered_through, empty.present().system_time);
    drop(empty);

    let make_insert = |relation: &OwnedRelation, ids: &[i64]| TxOp::OwnedInsertDocs {
        relation: relation.clone(),
        documents: Documents::new(ids.iter().map(|id| StructFields::from([("_id".into(), Value::I64(*id))])).collect(), node.memory()).unwrap(),
        valid_from: Some(Timestamp(10)), valid_to: Some(Timestamp(20)),
    };
    let outcome = writer.state_mut().execute_tx(vec![make_insert(&relation, &[1, 2]), make_insert(&other, &[1])],
        AutonomousTxOptions::default()).await.unwrap();
    assert!(outcome.committed());
    let first_time = outcome.tx_key().system_time;
    let held = writer.open_owned_read().await.unwrap();
    let first = held.changes(&relation, Timestamp::MIN, 1).await.unwrap();
    assert_eq!(first.groups.len(), 1);
    assert_eq!(first.groups[0].revision, 1);
    assert_eq!(first.groups[0].system_time, first_time);
    assert_eq!(first.covered_through, held.present().system_time);
    assert_eq!(first.groups[0].touches.iter().map(|touch| touch.id.clone()).collect::<Vec<_>>(),
        [EntityId::Integer(1), EntityId::Integer(2)]);
    assert!(first.groups[0].touches.iter().all(|touch| touch.kind == OwnedChangeKind::Put
        && touch.valid == rtdb_types::Interval::from_micros(10, 20).unwrap()));
    assert!(held.changes(&relation, first_time, 1).await.unwrap().groups.is_empty());
    assert_eq!(held.changes(&other, Timestamp::MIN, 1).await.unwrap().groups[0].system_time, first_time);

    let delete = TxOp::OwnedDelete {
        relation: relation.clone(), id: EntityId::Integer(1),
        valid_from: Some(Timestamp(15)), valid_to: Some(Timestamp(18)),
    };
    let refused = writer.state_mut().execute_tx(vec![delete.clone(), TxOp::OwnedCheck {
        relation: relation.clone(), id: EntityId::Integer(1), valid_at: Timestamp(15), expected: None,
        revealed: None,
    }], AutonomousTxOptions::default()).await.unwrap();
    assert!(!refused.committed());
    let after_refusal = writer.open_owned_read().await.unwrap();
    assert!(after_refusal.changes(&relation, first_time, 128).await.unwrap().groups.is_empty());
    drop(after_refusal);

    let outcome = writer.state_mut().execute_tx(vec![delete], AutonomousTxOptions::default()).await.unwrap();
    assert!(outcome.committed());
    let second_time = outcome.tx_key().system_time;
    assert!(held.changes(&relation, first_time, 1).await.unwrap().groups.is_empty());
    drop(held);

    let database = node.catalog().database("xtdb").unwrap();
    index_flush(&node, &database).await;
    let reopened = writer.open_owned_read().await.unwrap();
    let first = reopened.changes(&relation, Timestamp::MIN, 1).await.unwrap();
    assert_eq!(first.covered_through, first_time);
    let next = reopened.changes(&relation, first.covered_through, 1).await.unwrap();
    assert_eq!(next.groups.len(), 1);
    assert_eq!(next.groups[0].revision, 2);
    assert_eq!(next.groups[0].system_time, second_time);
    assert_eq!(next.covered_through, reopened.present().system_time);
    assert_eq!(next.groups[0].touches[0].kind, OwnedChangeKind::Delete);
    assert_eq!(next.groups[0].touches[0].valid, rtdb_types::Interval::from_micros(15, 18).unwrap());
    let lower = EntityId::Integer(2);
    let upper = EntityId::Integer(3);
    let valid = rtdb_types::Interval::from_micros(15, 16).unwrap();
    let regional = reopened.changes_in_region(&relation, Timestamp::MIN, 1, Some(&lower), Some(&upper), valid).await.unwrap();
    assert_eq!(regional.groups[0].touches.len(), 1);
    assert_eq!(regional.groups[0].touches[0].id, lower);
    assert_eq!(regional.covered_through, first_time);
    let unrelated = reopened.changes_in_region(&relation, regional.covered_through, 1, Some(&lower), Some(&upper), valid).await.unwrap();
    assert!(unrelated.groups.is_empty());
    assert_eq!(unrelated.covered_through, reopened.present().system_time);
    let adjacent = rtdb_types::Interval::from_micros(20, 21).unwrap();
    let unrelated = reopened.changes_in_region(&relation, Timestamp::MIN, 1, None, None, adjacent).await.unwrap();
    assert!(unrelated.groups.is_empty());
    assert_eq!(unrelated.covered_through, first_time);
    assert!(reopened.changes(&other, first_time, 1).await.unwrap().groups.is_empty());
    let id = EntityId::Integer(1);
    let mut old = reopened.open_historical_point(&relation, &id, first_time, Timestamp(15)).await.unwrap().unwrap();
    let batch = old.next_batch().await.unwrap().unwrap();
    assert_eq!(batch.batch().num_rows(), 1);
    assert!(reopened.open_historical_point(&relation, &id, reopened.present().system_time.next().unwrap(), Timestamp(15)).await.is_err());
    drop(old);
    drop(reopened);
    drop(writer);
    stop(&node, driver).await;
}

#[tokio::test]
async fn merged_held_changes_resume_only_after_whole_visible_commits() {
    use rtdb_types::{EntityId, OwnedRelation};

    let mut settings = options();
    settings.owned_relations = true;
    let (node, driver) = running(settings, services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let first = OwnedRelation::new("first-feed").unwrap();
    let second = OwnedRelation::new("second-feed").unwrap();
    let excluded = OwnedRelation::new("excluded-feed").unwrap();
    let empty = OwnedRelation::new("empty-feed").unwrap();
    let insert = |relation: &OwnedRelation, id| TxOp::OwnedInsertDocs {
        relation: relation.clone(),
        documents: Documents::new(vec![StructFields::from([("_id".into(), Value::I64(id))])], node.memory()).unwrap(),
        valid_from: None, valid_to: None,
    };
    let joined = session.state_mut().execute_tx(vec![
        insert(&second, 2), insert(&first, 1), insert(&excluded, 9),
    ], AutonomousTxOptions::default()).await.unwrap();
    let invisible = session.state_mut().execute_tx(vec![insert(&excluded, 10)], AutonomousTxOptions::default()).await.unwrap();
    let later = session.state_mut().execute_tx(vec![insert(&first, 3)], AutonomousTxOptions::default()).await.unwrap();
    assert!(joined.committed() && invisible.committed() && later.committed());

    let held = session.open_owned_read().await.unwrap();
    let selected = [second.clone(), empty, first.clone()];
    let page = held.merged_changes(&selected, Timestamp::MIN, 1).await.unwrap();
    assert_eq!(page.commits.len(), 1);
    let present = held.present().system_time;
    assert_eq!(page.covered_through().unwrap(), joined.tx_key().system_time);
    assert_eq!(page.commits[0].changes.len(), 2);
    assert_eq!(page.commits[0].changes[0].relation, first);
    assert_eq!(page.commits[0].changes[1].relation, second);
    assert_eq!(page.commits[0].changes[1].touches[0].id, EntityId::Integer(2));

    let next = held.merged_changes(&selected, page.commits[0].publication.system_time, 1).await.unwrap();
    assert_eq!(next.commits.len(), 1);
    assert_eq!(next.covered_through().unwrap(), present);
    assert_eq!(next.commits[0].changes.len(), 1);
    assert_eq!(next.commits[0].changes[0].touches[0].id, EntityId::Integer(3));
    assert!(held.merged_changes(&selected, next.commits[0].publication.system_time, 1).await.unwrap().commits.is_empty());
    assert!(held.merged_changes(&[first.clone(), first.clone()], Timestamp::MIN, 1).await.is_err());

    assert!(session.state_mut().execute_tx(vec![insert(&second, 4)], AutonomousTxOptions::default()).await.unwrap().committed());
    assert!(held.merged_changes(&selected, next.commits[0].publication.system_time, 1).await.unwrap().commits.is_empty());

    drop(held);
    drop(session);
    stop(&node, driver).await;
}

#[tokio::test]
async fn derived_owned_root_points_use_the_visible_marker_and_a_closed_scope() {
    use rtdb_node::{OwnedVersionOrder, OwnedVersionRequest};
    use rtdb_types::{EntityId, Interval, OwnedRelation};

    let mut settings = options();
    settings.owned_relations = true;
    let (node, driver) = running(settings, services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let publications = OwnedRelation::new("publications").unwrap();
    let facts = OwnedRelation::new("facts").unwrap();
    let other = OwnedRelation::new("other").unwrap();
    let marker = EntityId::String("head".into());
    let fact = EntityId::String("key".into());
    let second_key = EntityId::String("second-key".into());
    let unselected_key = EntityId::String("future-key".into());
    let put = |relation: &OwnedRelation, id: &EntityId, value| TxOp::OwnedReplaceDocs {
        relation: relation.clone(),
        documents: Documents::new(vec![StructFields::from([
            ("_id".into(), id.as_value()), ("payload".into(), Value::I64(value)),
        ])], node.memory()).unwrap(),
        valid_from: (relation == &publications).then_some(Timestamp::MIN), valid_to: None,
    };
    let first = session.state_mut().execute_tx(vec![put(&publications, &marker, 1), put(&facts, &fact, 1), put(&facts, &second_key, 5)],
        AutonomousTxOptions::default()).await.unwrap();
    let subsequent = session.state_mut().execute_tx(
        vec![put(&facts, &second_key, 9)], AutonomousTxOptions::default(),
    ).await.unwrap();
    assert!(subsequent.committed());
    assert!(subsequent.tx_key().tx_id > first.tx_key().tx_id);
    let intermediate = session.reserve_ordered_present().await.unwrap();
    let second = session.state_mut().execute_tx(vec![put(&publications, &marker, 2), put(&facts, &fact, 2), put(&facts, &unselected_key, 6)],
        AutonomousTxOptions::default()).await.unwrap();
    assert!(first.committed() && second.committed());

    let fresh = session.open_owned_read().await.unwrap();
    use rtdb_node::OwnedPublicationLookup;
    assert_eq!(fresh.lookup_publication(&facts, OwnedPublicationLookup::Latest).await.unwrap(),
        Some((3, second.tx_key())));
    assert_eq!(fresh.lookup_publication(&facts, OwnedPublicationLookup::Sequence(2)).await.unwrap(),
        Some((2, subsequent.tx_key())));
    let wrong_history = rtdb_types::TransactionKey { system_time: first.tx_key().system_time, ..subsequent.tx_key() };
    assert!(fresh.lookup_publication(&facts, OwnedPublicationLookup::Position(wrong_history)).await.unwrap().is_none());
    let exact = fresh.derive_at_publication(first.tx_key(), &publications, &marker,
        vec![publications.clone(), facts.clone()]).await.unwrap().unwrap();
    assert_eq!(exact.historical_publication(), Some(first.tx_key()));
    assert_eq!(exact.lookup_publication(&facts, OwnedPublicationLookup::Latest).await.unwrap(),
        Some((1, first.tx_key())));
    assert!(exact.lookup_publication(&facts, OwnedPublicationLookup::Sequence(2)).await.unwrap().is_none());
    drop(exact);
    let scope = vec![publications.clone(), facts.clone()];
    assert!(fresh.derive_as_of(Timestamp::MIN, &publications, &marker, scope.clone()).await.unwrap().is_none());
    assert!(fresh.derive_as_of(intermediate.system_time, &publications, &marker, vec![facts.clone()]).await.is_err());
    let old = fresh.derive_as_of(intermediate.system_time, &publications, &marker, scope)
        .await.unwrap().unwrap();
    assert_eq!(old.system_time(), first.tx_key().system_time);
    assert_eq!(old.historical_publication(), Some(first.tx_key()));
    let payload = |batch: &arrow_array::RecordBatch| {
        let column = batch.schema_ref().index_of("payload").unwrap();
        rtdb_columnar::read_value(batch.schema_ref().field(column), batch.column(column).as_ref(), 0).unwrap()
    };
    for (before, expected) in [(true, 5), (false, 9)] {
        let mut cursor = fresh.open_root_cut_point(
            &facts, &second_key, subsequent.tx_key(), subsequent.tx_key().system_time, before,
        ).await.unwrap().unwrap();
        assert_eq!(payload(cursor.next_batch().await.unwrap().unwrap().batch()), Value::I64(expected));
    }
    let mut cursor = old.open_point(&facts, &second_key).await.unwrap().unwrap();
    assert_eq!(payload(cursor.next_batch().await.unwrap().unwrap().batch()), Value::I64(5));
    drop(cursor);
    let mut timeline = fresh.open_valid_during_observed(
        &facts, &fact, Interval::ALL, old.system_time(),
    ).await.unwrap();
    let first_batch = timeline.next_batch().await.unwrap().unwrap();
    assert_eq!(first_batch.batch().num_rows(), 1);
    assert!(timeline.next_batch().await.unwrap().is_none());
    assert!(timeline.witness().unwrap().is_none());
    drop(timeline);
    drop(fresh);
    assert!(old.open_point(&other, &fact).await.is_err());
    assert!(old.changes(&facts, Timestamp::MIN, 1).await.is_err());

    let mut cursor = old.open_point(&facts, &fact).await.unwrap().unwrap();
    let batch = cursor.next_batch().await.unwrap().unwrap();
    let index = batch.batch().schema_ref().index_of("payload").unwrap();
    let field = batch.batch().schema_ref().field(index);
    assert_eq!(rtdb_columnar::read_value(field, batch.batch().column(index).as_ref(), 0).unwrap(), Value::I64(1));
    drop(cursor);

    assert!(old.open_valid_during_observed(&other, &fact, Interval::ALL, old.system_time()).await.is_err());
    assert!(old.open_valid_during_observed(&facts, &fact, Interval::ALL, second.tx_key().system_time).await.is_err());
    let mut timeline = old.open_valid_during_observed(&facts, &fact, Interval::ALL, old.system_time()).await.unwrap();
    let historical = timeline.next_batch().await.unwrap().unwrap();
    assert_eq!(historical.batch(), first_batch.batch());
    assert!(timeline.next_batch().await.unwrap().is_none());
    assert!(timeline.witness().unwrap().is_none());
    drop(timeline);
    let versions = |through| OwnedVersionRequest {
        system: Interval::new(Timestamp::MIN, through).unwrap(),
        after: 0,
        scan_limit: 4,
        result_limit: 4,
        order: OwnedVersionOrder::Descending,
    };
    let through = old.system_time().next().unwrap();
    assert!(old.versions(&other, &fact, versions(through)).await.is_err());
    assert!(old.versions(&facts, &fact, versions(second.tx_key().system_time.next().unwrap())).await.is_err());
    let mut page = old.versions(&facts, &fact, versions(through)).await.unwrap();
    assert!(page.done && page.witness.is_none());
    assert_eq!(page.versions.len(), 1);
    let version = page.versions.pop().unwrap();
    assert_eq!(version.system_time, old.system_time());
    assert_eq!(rows(version.timeline).await.len(), 1);

    let cut = old.system_time();
    let keys = old.identity_page(&facts, cut, cut, &OwnedIdentityScan::text(None), 1).await.unwrap();
    assert_eq!(keys.keys, vec![fact.clone()]);
    assert!(keys.more);
    let keys = old.identity_page(&facts, cut, cut, &OwnedIdentityScan::seek(keys.keys.last().unwrap()).unwrap(), 1).await.unwrap();
    assert_eq!(keys.keys, vec![second_key]);
    assert!(!keys.more);
    assert!(old.identity_page(&facts, cut, Timestamp::MIN, &OwnedIdentityScan::text(None), 2).await.unwrap().keys.is_empty());
    assert!(old.identity_page(&other, cut, cut, &OwnedIdentityScan::text(None), 1).await.is_err());

    drop(old);
    drop(session);
    stop(&node, driver).await;
}

#[tokio::test]
async fn binary_owned_history_keeps_original_positions_and_refuses_duplicate_creates() {
    use rtdb_types::{Interval, OwnedChangeKind, OwnedRelation};

    let mut settings = options();
    settings.owned_relations = true;
    let (node, driver) = running(settings, services()).await;
    let mut writer = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("binary-facts").unwrap();
    let short = EntityId::Binary(vec![0, 255]);
    let long = EntityId::Binary(vec![0, 255, 0]);
    let put = |id: &EntityId, from, to| TxOp::OwnedInsertDocs {
        relation: relation.clone(),
        documents: Documents::new(vec![StructFields::from([("_id".into(), id.as_value())])], node.memory()).unwrap(),
        valid_from: Some(Timestamp(from)), valid_to: Some(Timestamp(to)),
    };
    let first = writer.state_mut().execute_tx(
        vec![put(&long, 10, 30), put(&short, 10, 15), put(&short, 20, 30)], AutonomousTxOptions::default(),
    ).await.unwrap();
    assert!(first.committed());
    let rejected = writer.state_mut().execute_tx(
        vec![put(&short, 12, 14)], AutonomousTxOptions::default(),
    ).await.unwrap();
    assert_eq!(rejected.error().unwrap().code, "rtdb.tx/exists");
    let last = writer.state_mut().execute_tx(vec![TxOp::OwnedDelete {
        relation: relation.clone(), id: short.clone(), valid_from: Some(Timestamp(20)), valid_to: Some(Timestamp(30)),
    }], AutonomousTxOptions::default()).await.unwrap();
    assert!(last.committed());

    let held = writer.open_owned_read().await.unwrap();
    let cut = first.tx_key().system_time;
    let first_page = held.identity_page_all_valid(&relation, cut, &OwnedIdentityScan::Binary(None), 1).await.unwrap();
    assert_eq!(first_page.keys, [short.clone()]);
    assert!(first_page.more);
    let next_page = held.identity_page_all_valid(&relation, cut, &OwnedIdentityScan::seek(&short).unwrap(), 1).await.unwrap();
    assert_eq!(next_page.keys, [long.clone()]);
    assert!(!next_page.more);
    assert!(held.identity_page(&relation, cut, Timestamp(22), &OwnedIdentityScan::text(None), 2).await.is_err());

    let mut original = held.open_root_cut_point(&relation, &short, first.tx_key(), Timestamp(22), false)
        .await.unwrap().unwrap();
    let original_row = original.next_batch().await.unwrap().unwrap();
    let position = rtdb_query::owned_row_event_position(original_row.batch(), 0).unwrap();
    assert_eq!(position.transaction, first.tx_key().tx_id);
    let mut earlier = held.open_root_cut_point(&relation, &short, first.tx_key(), Timestamp(12), false)
        .await.unwrap().unwrap();
    let earlier_row = earlier.next_batch().await.unwrap().unwrap();
    let earlier_position = rtdb_query::owned_row_event_position(earlier_row.batch(), 0).unwrap();
    assert_eq!(earlier_position.transaction, position.transaction);
    assert!(earlier_position.ordinal < position.ordinal);
    let groups = held.changes(&relation, Timestamp::MIN, 16).await.unwrap().groups;
    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0].touches[2].id, short);
    assert_eq!(groups[1].touches[0].kind, OwnedChangeKind::Delete);
    assert_eq!(groups[1].touches[0].valid, Interval::from_micros(20, 30).unwrap());

    drop(earlier);
    drop(original);
    drop(held);
    drop(writer);
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

#[tokio::test]
async fn guarded_cosine_leaves_disabled_zero_norm_vectors_unread() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let embedding = |x, y| Value::FixedSizeList(vec![Value::F32(x), Value::F32(y)].into());
    let document = |id, enabled, vector| StructFields::from([
        ("_id".into(), Value::I64(id)),
        ("enabled".into(), enabled),
        ("embedding".into(), vector),
    ]);
    let put = put_rows(&node, "vectors", vec![
        document(1, Value::Bool(true), embedding(1.0, 0.0)),
        document(2, Value::Bool(false), embedding(0.0, 0.0)),
        document(3, Value::Null, embedding(0.0, 0.0)),
    ], Some(Timestamp(0)), None);
    assert!(session.state_mut().execute_tx(vec![put], AutonomousTxOptions::default()).await.unwrap().committed());

    let by_row = "SELECT CASE WHEN enabled THEN COSINE_DISTANCE(embedding, $1) ELSE NULL END AS distance FROM vectors ORDER BY _id";
    let result = session.query_sql(by_row, vec![embedding(1.0, 0.0)]).await.unwrap();
    assert_eq!(rows(result).await, [vec![Value::F64(0.0)], vec![Value::Null], vec![Value::Null]]);

    let by_argument = "SELECT CASE WHEN $2 THEN COSINE_DISTANCE(embedding, $1) ELSE NULL END AS distance FROM vectors ORDER BY _id";
    let result = session.query_sql(by_argument, vec![embedding(0.0, 0.0), Value::Bool(false)]).await.unwrap();
    assert_eq!(rows(result).await, [vec![Value::Null], vec![Value::Null], vec![Value::Null]]);
    let invalid = session.query_sql(by_argument, vec![embedding(1.0, 0.0), Value::Bool(true)]).await;
    let error = match invalid {
        Ok(mut result) => loop {
            match result.next_batch().await {
                Ok(Some(_)) => continue,
                Ok(None) => break None,
                Err(error) => break Some(error),
            }
        },
        Err(error) => Some(error),
    }.expect("the selected zero vector cannot be scored");
    assert_eq!(error.code, "rtdb.vector/zero-norm");
    stop(&node, driver).await;
}

#[tokio::test]
async fn show_transaction_isolation_reads_session_parameter_distinct_from_isolation_level() {
    let (node, driver) = running(options(), services()).await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let token = session.state().cancellation().child_token();
    let parse = |sql: &str| rtdb_sql::parse_control(sql, &Default::default()).unwrap().unwrap();

    session.state_mut().begin().unwrap();
    let ControlResult::Rows(cursor) = session.execute_control(&parse("SHOW transaction_isolation"), &[], &token).await.unwrap() else {
        panic!("expected rows from SHOW transaction_isolation");
    };
    assert_eq!(rows(cursor).await, [vec![Value::Null]]);
    assert!(!session.state().is_read_only());

    drop(session.execute_control(&parse("SET transaction_isolation = 'serializable'"), &[], &token).await.unwrap());
    let ControlResult::Rows(cursor) = session.execute_control(&parse("SHOW transaction_isolation"), &[], &token).await.unwrap() else {
        panic!("expected rows from SHOW transaction_isolation after SET");
    };
    assert_eq!(rows(cursor).await, [vec![Value::Utf8("serializable".into())]]);
    assert!(!session.state().is_read_only());

    let ControlResult::Rows(cursor) = session.execute_control(&parse("SHOW TRANSACTION ISOLATION LEVEL"), &[], &token).await.unwrap() else {
        panic!("expected rows from SHOW TRANSACTION ISOLATION LEVEL");
    };
    assert_eq!(rows(cursor).await, [vec![Value::Utf8("read committed".into())]]);
    assert!(session.state().is_read_only());

    drop(session);
    stop(&node, driver).await;
}
