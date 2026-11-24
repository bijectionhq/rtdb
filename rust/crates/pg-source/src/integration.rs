use std::{collections::BTreeSet, sync::Arc, time::Duration};
use rtdb_columnar::MemoryBudget;
use rtdb_types::Value;
use crate::{ConfirmationState, Remote, SourceConfig, SslMode, snapshot::SnapshotReader, stream::ChangeStream, wire::Connection};

#[tokio::test]
#[ignore = "requires an explicitly configured PostgreSQL 17+ server with wal_level=logical and replication credentials"]
async fn exported_snapshot_and_copyboth_preserve_concurrent_updates_and_full_toast() {
    let mut remote = Remote::new(std::env::var("RTDB_PG_HOST").unwrap_or_else(|_| "127.0.0.1".into()), std::env::var("RTDB_PG_DATABASE").expect("RTDB_PG_DATABASE"), std::env::var("RTDB_PG_USER").expect("RTDB_PG_USER"), std::env::var("RTDB_PG_PASSWORD").unwrap_or_default());
    remote.port = std::env::var("RTDB_PG_PORT").unwrap_or_else(|_| "5432".into()).parse().unwrap();
    remote.ssl = SslMode::Disable;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table = format!("rtdb_cdc_{suffix}"); let publication = format!("rtdb_pub_{suffix}"); let slot = format!("rtdb_slot_{suffix}");
    let memory = MemoryBudget::new(256 << 20);
    let mut control = Connection::open(&remote, false, Arc::clone(&memory)).await.unwrap();
    control.query(&format!("CREATE TABLE {table} (_id integer PRIMARY KEY, value text, toast text)")).await.unwrap();
    control.query(&format!("ALTER TABLE {table} REPLICA IDENTITY FULL")).await.unwrap();
    control.query(&format!("INSERT INTO {table} VALUES (1, 'before', repeat(md5(random()::text), 1000))")).await.unwrap();
    control.query(&format!("CREATE PUBLICATION {publication} FOR TABLE {table}")).await.unwrap();
    let config = SourceConfig { remote: "test".into(), slot_name: slot.clone(), publication_name: publication.clone(), indexer: None, ignore_tables: BTreeSet::new() };
    let mut snapshot = SnapshotReader::open(&remote, &config, Arc::clone(&memory)).await.unwrap();
    let start = snapshot.lsn;
    control.query(&format!("UPDATE {table} SET value = 'after' WHERE _id = 1")).await.unwrap();
    let batch = snapshot.next_batch().await.unwrap().unwrap();
    assert_eq!(batch.row_count(), 1); assert_eq!(change_cell(&batch, "value"), Value::Utf8("before".into()));
    let toast = change_cell(&batch, "toast");
    assert!(snapshot.next_batch().await.unwrap().is_none()); snapshot.finish().await.unwrap();
    wait_for_slot_release(&mut control, &slot).await;
    let mut stream = ChangeStream::open(&remote, &config, start, Arc::clone(&memory)).await.unwrap();
    let changed = tokio::time::timeout(Duration::from_secs(20), async { loop { if let Some(transaction) = stream.read().await.unwrap().transaction { break transaction; } } }).await.unwrap();
    assert!(changed.lsn > start); assert_eq!(change_cell(&changed, "value"), Value::Utf8("after".into())); assert_eq!(change_cell(&changed, "toast"), toast);
    let feedback = ConfirmationState::new(changed.lsn, Some(changed.lsn)).acknowledgement();
    stream.acknowledge(&feedback).await.unwrap();
    drop(stream);
    wait_for_slot_release(&mut control, &slot).await;
    let mut resumed = ChangeStream::open(&remote, &config, changed.lsn, Arc::clone(&memory)).await.unwrap();
    control.query(&format!("DELETE FROM {table} WHERE _id = 1")).await.unwrap();
    let deleted = tokio::time::timeout(Duration::from_secs(20), async { loop { if let Some(transaction) = resumed.read().await.unwrap().transaction { if transaction.lsn > changed.lsn { break transaction; } } } }).await.unwrap();
    assert_eq!(deleted.batches[0].kind, crate::RowKind::Delete); assert_eq!(change_cell(&deleted, "_id"), Value::I32(1));
    drop(resumed);
    wait_for_slot_release(&mut control, &slot).await;
    control.query(&format!("SELECT pg_drop_replication_slot('{slot}')")).await.unwrap();
    control.query(&format!("DROP PUBLICATION {publication}")).await.unwrap();
    control.query(&format!("DROP TABLE {table}")).await.unwrap();
}

fn change_cell(transaction: &crate::decoder::DecodedTransaction, field: &str) -> Value {
    let batch = transaction.batches[0].rows.batch();
    let column = batch.schema_ref().index_of(field).unwrap();
    rtdb_columnar::read_value(batch.schema_ref().field(column), batch.column(column).as_ref(), 0).unwrap()
}

struct Effects;
#[async_trait::async_trait]
impl rtdb_replication::DatabaseEffects for Effects {
    fn check_db_op(&self, _: &rtdb_format::log::proto::resolved_tx::DbOp) -> rtdb_types::Result<()> { Ok(()) }
    async fn apply_db_op(&self, _: &rtdb_format::log::proto::resolved_tx::DbOp) -> rtdb_types::Result<()> { Ok(()) }
    fn secondary_databases(&self) -> rtdb_types::Result<std::collections::BTreeMap<String, rtdb_format::database::proto::DatabaseConfig>> { Ok(Default::default()) }
}
struct Authority;
impl rtdb_tx::TransactionAuthorization for Authority { fn check_role_membership(&self, _: &str, _: Option<&str>) -> rtdb_types::Result<()> { Ok(()) } }
async fn wait_for_slot_release(control: &mut Connection, slot: &str) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let result = control.query(&format!("SELECT active::text FROM pg_replication_slots WHERE slot_name = '{slot}'")).await.unwrap();
            assert_eq!(result.rows.len(), 1);
            if result.rows[0].cell(0).unwrap() == Some("false") { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
}
async fn wait_for(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(20), async { while !predicate() { tokio::time::sleep(Duration::from_millis(10)).await; } }).await.unwrap();
}

#[tokio::test]
#[ignore = "requires an explicitly configured PostgreSQL 17+ server with wal_level=logical and replication credentials"]
async fn native_source_applies_snapshot_streams_and_confirms_only_the_adopted_extent() {
    use rtdb_storage::{MemoryStore, cache::BufferPool, log::MemoryLog};
    use rtdb_replication::{BlockStore, PartitionLogs, ProgressMonitor, ReplicationConfig, ReplicationDriver, Role};
    use rtdb_types::{SourcePosition, StorageEpoch, Timestamp};
    use tokio_util::sync::CancellationToken;
    let mut remote = Remote::new(std::env::var("RTDB_PG_HOST").unwrap_or_else(|_| "127.0.0.1".into()), std::env::var("RTDB_PG_DATABASE").expect("RTDB_PG_DATABASE"), std::env::var("RTDB_PG_USER").expect("RTDB_PG_USER"), std::env::var("RTDB_PG_PASSWORD").unwrap_or_default());
    remote.port = std::env::var("RTDB_PG_PORT").unwrap_or_else(|_| "5432".into()).parse().unwrap(); remote.ssl = SslMode::Disable; remote.status_interval = Duration::from_millis(50);
    let suffix = uuid::Uuid::new_v4().simple().to_string(); let table = format!("rtdb_flow_{suffix}"); let publication = format!("rtdb_pub_{suffix}"); let slot = format!("rtdb_slot_{suffix}");
    let memory = MemoryBudget::new(1 << 30); let store = Arc::new(MemoryStore::default()); let cache = Arc::new(BufferPool::new(store.clone(), 1 << 27));
    let logs = PartitionLogs::new(Arc::new(MemoryLog::new(1 << 27)), Arc::new(MemoryLog::new(1 << 27)), StorageEpoch(0), StorageEpoch(0), rtdb_format::keys::CURRENT_STORAGE_VERSION, StorageEpoch(0), store.clone(), Arc::clone(&cache), Arc::clone(&memory)).unwrap();
    let blocks = BlockStore::new(store, Arc::clone(&cache), Arc::clone(&memory), 100_000_000).unwrap();
    let recovered = blocks.recover(rtdb_trie::TrieConfig::default(), SourcePosition(-1), Timestamp(0), rtdb_replication::BlockDiscovery::List).await.unwrap();
    let partition = Arc::clone(&recovered.partition); let progress = ProgressMonitor::new(recovered.progress.clone());
    let resolve = Arc::new(rtdb_tx::ResolveContext { decision_clock: None, write_event_origins: false, placed_receipt_retirement: false, owned_witness_scope: None, world_constraints: None, database: "cdc".into(), storage: cache, memory: Arc::clone(&memory), query_options: Default::default(), scan_accelerators: Default::default(), cancellation: CancellationToken::new(), default_tz: "UTC".into(), authorization: Arc::new(Authority), execution_observer: None, tracer: None });
    let (handle, driver) = ReplicationDriver::new(ReplicationConfig { external_source: true, ..Default::default() }, logs, Arc::clone(&blocks), recovered, progress, resolve, Arc::new(Effects), CancellationToken::new()).unwrap();
    let replica_job = tokio::spawn(driver.run()); wait_for(|| matches!(handle.status().role, Role::Leading(_))).await;
    let mut control = Connection::open(&remote, false, Arc::clone(&memory)).await.unwrap();
    control.query(&format!("CREATE TABLE {table} (_id integer PRIMARY KEY, value text)")).await.unwrap(); control.query(&format!("INSERT INTO {table} VALUES (1, 'snapshot')")).await.unwrap(); control.query(&format!("CREATE PUBLICATION {publication} FOR TABLE {table}")).await.unwrap();
    let config = SourceConfig { remote: "test".into(), slot_name: slot.clone(), publication_name: publication.clone(), indexer: None, ignore_tables: BTreeSet::new() };
    let source = crate::SourceDriver::new(remote, config, Arc::clone(&memory), None).unwrap(); let cancellation = CancellationToken::new();
    let source_job = tokio::spawn(source.run(handle.term().unwrap(), None, handle.watch_status(), cancellation.clone()));
    let applied_token = || handle.progress().current().external_token.as_deref().and_then(|token| crate::ResumeToken::decode(token).ok());
    wait_for(|| applied_token().is_some_and(|token| token.snapshot_completed)).await;
    assert_eq!(partition.value().live().tables().get(&rtdb_types::TableRef::new("public", &table)).map(|table| table.row_count()), Some(1));
    handle.flush().await.unwrap(); wait_for(|| handle.status().adopted_block.is_some()).await;
    let durable = crate::ResumeToken::decode(handle.status().adopted_block.as_ref().unwrap().external_source_token().unwrap()).unwrap().lsn;
    control.query(&format!("INSERT INTO {table} VALUES (2, 'unflushed')")).await.unwrap(); wait_for(|| applied_token().is_some_and(|token| token.lsn > durable)).await;
    let applied = applied_token().unwrap().lsn;
    for _ in 0..20 {
        let result = control.query(&format!("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = '{slot}'")).await.unwrap();
        let confirmed: crate::Lsn = result.rows[0].cell(0).unwrap().unwrap().parse().unwrap(); assert!(confirmed < applied);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cancellation.cancel(); source_job.await.unwrap().unwrap(); handle.shutdown(); replica_job.await.unwrap().unwrap();
    let recovered = blocks.recover(rtdb_trie::TrieConfig::default(), SourcePosition(-1), Timestamp(0), rtdb_replication::BlockDiscovery::List).await.unwrap();
    assert_eq!(crate::ResumeToken::decode(recovered.progress.external_token.as_deref().unwrap()).unwrap().lsn, durable);
    wait_for_slot_release(&mut control, &slot).await;
    control.query(&format!("SELECT pg_drop_replication_slot('{slot}')")).await.unwrap(); control.query(&format!("DROP PUBLICATION {publication}")).await.unwrap(); control.query(&format!("DROP TABLE {table}")).await.unwrap();
}

#[tokio::test]
#[ignore = "requires an explicitly configured PostgreSQL 17+ server with wal_level=logical and replication credentials"]
async fn native_source_recovers_confirmed_local_journal_transactions_before_the_first_block() {
    use rtdb_replication::{BlockStore, PartitionLogs, ProgressMonitor, ReplicationConfig, ReplicationDriver, Role};
    use rtdb_storage::{LocalStore, cache::BufferPool, log::{LocalLog, LocalLogOptions}};
    use rtdb_types::{SourcePosition, StorageEpoch, Timestamp};
    use tokio_util::sync::CancellationToken;

    let directory = tempfile::tempdir().unwrap();
    let memory = MemoryBudget::new(1 << 28);
    let start_target = || async {
        let open_log = |name| LocalLog::open(&directory.path().join(name),
            LocalLogOptions { maximum_message_bytes: 4 << 20, queue_capacity: 32 }).unwrap();
        let (source, intake) = open_log("source");
        let (replica, journal) = open_log("replica");
        let stop_logs = CancellationToken::new();
        let logs_jobs = [tokio::spawn(intake.run(stop_logs.clone())), tokio::spawn(journal.run(stop_logs.clone()))];
        let store = Arc::new(LocalStore::open(directory.path().join("objects")).unwrap());
        let cache = Arc::new(BufferPool::new(store.clone(), 1 << 25));
        let logs = PartitionLogs::new(source, replica, StorageEpoch(0), StorageEpoch(0),
            rtdb_format::keys::CURRENT_STORAGE_VERSION, StorageEpoch(0), store.clone(), cache.clone(), memory.clone()).unwrap();
        let blocks = BlockStore::new(store, cache.clone(), memory.clone(), 100_000_000).unwrap();
        let recovered = blocks.recover(rtdb_trie::TrieConfig::default(), SourcePosition(-1), Timestamp(0), Default::default()).await.unwrap();
        let partition = recovered.partition.clone();
        let progress = ProgressMonitor::new(recovered.progress.clone());
        let resolve = Arc::new(rtdb_tx::ResolveContext { decision_clock: None, write_event_origins: false, placed_receipt_retirement: false, owned_witness_scope: None, world_constraints: None, database: "cdc".into(), storage: cache, memory: memory.clone(), query_options: Default::default(), scan_accelerators: Default::default(), cancellation: CancellationToken::new(), default_tz: "UTC".into(), authorization: Arc::new(Authority), execution_observer: None, tracer: None });
        let (handle, driver) = ReplicationDriver::new(ReplicationConfig { external_source: true, ..Default::default() },
            logs, blocks, recovered, progress, resolve, Arc::new(Effects), CancellationToken::new()).unwrap();
        let replication = tokio::spawn(driver.run());
        wait_for(|| matches!(handle.status().role, Role::Leading(_))).await;
        (handle, replication, partition, stop_logs, logs_jobs)
    };

    let mut remote = Remote::new(std::env::var("RTDB_PG_HOST").unwrap_or_else(|_| "127.0.0.1".into()), std::env::var("RTDB_PG_DATABASE").expect("RTDB_PG_DATABASE"), std::env::var("RTDB_PG_USER").expect("RTDB_PG_USER"), std::env::var("RTDB_PG_PASSWORD").unwrap_or_default());
    remote.port = std::env::var("RTDB_PG_PORT").unwrap_or_else(|_| "5432".into()).parse().unwrap();
    remote.ssl = SslMode::Disable;
    remote.status_interval = Duration::from_millis(50);
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table = format!("rtdb_early_{suffix}");
    let publication = format!("rtdb_pub_{suffix}");
    let slot = format!("rtdb_slot_{suffix}");
    let mut control = Connection::open(&remote, false, memory.clone()).await.unwrap();
    control.query(&format!("CREATE TABLE {table} (_id integer PRIMARY KEY, value text)")).await.unwrap();
    control.query(&format!("INSERT INTO {table} VALUES (1, 'snapshot')")).await.unwrap();
    control.query(&format!("CREATE PUBLICATION {publication} FOR TABLE {table}")).await.unwrap();
    let config = SourceConfig { remote: "test".into(), slot_name: slot.clone(), publication_name: publication.clone(), indexer: None, ignore_tables: BTreeSet::new() };

    let (handle, replication, _, stop_logs, logs_jobs) = start_target().await;
    let source = crate::SourceDriver::new(remote.clone(), config.clone(), memory.clone(), None).unwrap();
    let cancel_source = CancellationToken::new();
    let source_job = tokio::spawn(source.run(handle.term().unwrap(), None, handle.watch_status(), cancel_source.clone()));
    let applied = || handle.progress().current().external_token.as_deref().and_then(|token| crate::ResumeToken::decode(token).ok());
    wait_for(|| applied().is_some_and(|token| token.snapshot_completed)).await;
    let snapshot_lsn = applied().unwrap().lsn;
    control.query(&format!("INSERT INTO {table} VALUES (2, 'replica only')")).await.unwrap();
    wait_for(|| applied().is_some_and(|token| token.lsn > snapshot_lsn)).await;
    let confirmed = applied().unwrap().lsn;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let result = control.query(&format!("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = '{slot}'")).await.unwrap();
            let upstream: crate::Lsn = result.rows[0].cell(0).unwrap().unwrap().parse().unwrap();
            if upstream >= confirmed { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    assert!(handle.status().adopted_block.is_none());
    cancel_source.cancel();
    source_job.await.unwrap().unwrap();
    handle.shutdown();
    replication.await.unwrap().unwrap();
    drop(handle);
    stop_logs.cancel();
    for job in logs_jobs { job.await.unwrap().unwrap(); }

    let (handle, replication, partition, stop_logs, logs_jobs) = start_target().await;
    let after = handle.progress().current().external_token.unwrap();
    assert_eq!(crate::ResumeToken::decode(&after).unwrap().lsn, confirmed);
    assert!(handle.status().adopted_block.is_none());
    let table_ref = rtdb_types::TableRef::new("public", &table);
    let row_count = || partition.value().live().tables().get(&table_ref).map(|table| table.row_count());
    assert_eq!(row_count(), Some(2));
    wait_for_slot_release(&mut control, &slot).await;
    let source = crate::SourceDriver::new(remote, config, memory.clone(), None).unwrap();
    let cancel_resumed = CancellationToken::new();
    let source_job = tokio::spawn(source.run(handle.term().unwrap(), Some(after), handle.watch_status(), cancel_resumed.clone()));
    control.query(&format!("INSERT INTO {table} VALUES (3, 'resumed')")).await.unwrap();
    wait_for(|| row_count() == Some(3)).await;
    cancel_resumed.cancel();
    source_job.await.unwrap().unwrap();
    handle.shutdown();
    replication.await.unwrap().unwrap();
    stop_logs.cancel();
    for job in logs_jobs { job.await.unwrap().unwrap(); }
    wait_for_slot_release(&mut control, &slot).await;
    control.query(&format!("SELECT pg_drop_replication_slot('{slot}')")).await.unwrap();
    control.query(&format!("DROP PUBLICATION {publication}")).await.unwrap();
    control.query(&format!("DROP TABLE {table}")).await.unwrap();
}

#[tokio::test]
async fn pg_source_arrow_wal_keeps_replica_table_bytes_and_event_ordinals() {
    use rtdb_format::envelope::ReplicaBody;
    use rtdb_index::PartitionState;
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    use rtdb_types::{SourcePosition, Timestamp};
    use tokio_util::sync::CancellationToken;

    let memory = MemoryBudget::new(1 << 26);
    let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
    let context = rtdb_tx::ResolveContext {
        database: "cdc".into(), storage, memory: Arc::clone(&memory), cancellation: CancellationToken::new(),
        default_tz: "UTC".into(), authorization: Arc::new(Authority), query_options: Default::default(),
        scan_accelerators: Default::default(), decision_clock: None, world_constraints: None,
        execution_observer: None, tracer: None, write_event_origins: false, placed_receipt_retirement: false,
        owned_witness_scope: None,
    };
    let partition = PartitionState::empty(Default::default()).unwrap();
    let (actual, legacy) = crate::decoder::tests::transactions(&memory);
    let mut replicas = Vec::new();
    for transaction in [actual, legacy] {
        let mut resolver = rtdb_tx::TxResolver::new(None, SourcePosition(-1), None, &memory).unwrap();
        let mut writer = crate::indexer::ImportWriter { transaction, indexer: None, memory: Arc::clone(&memory) };
        let resolved = resolver.resolve_external(partition.snapshot(), Timestamp(1_600_000_000_000_000), None, Some(vec![1, 2, 3]), &mut writer, &context).await.unwrap();
        replicas.push(resolved.to_replica_body(&memory).unwrap());
    }
    let [actual, expected] = replicas.as_slice() else { unreachable!() };
    let (ReplicaBody::ResolvedTx(actual), ReplicaBody::ResolvedTx(expected)) = (actual.body(), expected.body()) else { unreachable!() };
    assert_eq!(actual.external_source_token, expected.external_source_token);
    assert_eq!(actual.table_data.keys().collect::<Vec<_>>(), expected.table_data.keys().collect::<Vec<_>>());
    for (table, page) in &actual.table_data {
        assert!(page == &expected.table_data[table], "replica Arrow bytes differ for {table}");
    }
}

#[tokio::test]
#[ignore = "requires an explicitly configured PostgreSQL server"]
async fn pg_source_arrow_snapshot_fetch_requests_binary_and_preserves_unknown_types_as_text() {
    use rtdb_pg_codec::Format;
    let mut remote = Remote::new(
        std::env::var("RTDB_PG_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
        std::env::var("RTDB_PG_DATABASE").expect("RTDB_PG_DATABASE"),
        std::env::var("RTDB_PG_USER").expect("RTDB_PG_USER"),
        std::env::var("RTDB_PG_PASSWORD").unwrap_or_default(),
    );
    remote.port = std::env::var("RTDB_PG_PORT").unwrap_or_else(|_| "5432".into()).parse().unwrap();
    remote.ssl = SslMode::Disable;
    let memory = MemoryBudget::new(128 << 20);
    let mut query = Connection::open(&remote, false, memory).await.unwrap();
    query.query("BEGIN").await.unwrap();
    query.query("CREATE TYPE pg_temp.rtdb_test_enum AS ENUM ('active')").await.unwrap();
    query.query("DECLARE rtdb_rows NO SCROLL CURSOR FOR SELECT n, 'active'::pg_temp.rtdb_test_enum FROM generate_series(1, 1001) n").await.unwrap();
    let statement = query.prepare_snapshot_fetch().await.unwrap();
    let first = query.snapshot_fetch(&statement).await.unwrap();
    assert_eq!(first.columns.iter().map(|column| column.format).collect::<Vec<_>>(), [Format::Binary, Format::Text]);
    assert_eq!(first.rows.len(), 1000);
    assert_eq!(first.rows[0].cell_bytes(0).unwrap(), Some(1i32.to_be_bytes().as_slice()));
    assert_eq!(first.rows[0].cell(1).unwrap(), Some("active"));
    let second = query.snapshot_fetch(&statement).await.unwrap();
    assert_eq!(second.rows.len(), 1);
    assert_eq!(second.rows[0].cell_bytes(0).unwrap(), Some(1001i32.to_be_bytes().as_slice()));
    assert!(query.snapshot_fetch(&statement).await.unwrap().rows.is_empty());
    query.query("ROLLBACK").await.unwrap();
}

#[tokio::test]
#[ignore = "requires a PostgreSQL 17+ server with pgvector, wal_level=logical and replication credentials"]
async fn vector_types_and_domains_have_the_same_arrow_layout_in_snapshot_and_wal() {
    use arrow_array::{Array, FixedSizeListArray, Float32Array, Float64Array, ListArray};
    use arrow_schema::DataType;

    let mut remote = Remote::new(
        std::env::var("RTDB_PG_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
        std::env::var("RTDB_PG_DATABASE").expect("RTDB_PG_DATABASE"),
        std::env::var("RTDB_PG_USER").expect("RTDB_PG_USER"),
        std::env::var("RTDB_PG_PASSWORD").unwrap_or_default(),
    );
    remote.port = std::env::var("RTDB_PG_PORT").unwrap_or_else(|_| "5432".into()).parse().unwrap();
    remote.ssl = SslMode::Disable;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table = format!("rtdb_vector_{suffix}");
    let fixed_domain = format!("rtdb_fixed_{suffix}");
    let nested_domain = format!("rtdb_nested_{suffix}");
    let variable_domain = format!("rtdb_variable_{suffix}");
    let reals_domain = format!("rtdb_reals_{suffix}");
    let doubles_domain = format!("rtdb_doubles_{suffix}");
    let publication = format!("rtdb_pub_{suffix}");
    let slot = format!("rtdb_slot_{suffix}");
    let memory = MemoryBudget::new(256 << 20);
    let mut control = Connection::open(&remote, false, Arc::clone(&memory)).await.unwrap();
    for (domain, base) in [
        (&fixed_domain, "vector(2)"), (&nested_domain, fixed_domain.as_str()),
        (&variable_domain, "halfvec"), (&reals_domain, "real[]"), (&doubles_domain, "double precision[]"),
    ] {
        control.query(&format!("CREATE DOMAIN {domain} AS {base}")).await.unwrap();
    }
    control.query(&format!("CREATE TABLE {table} (_id integer PRIMARY KEY, embedding vector(2), half halfvec, reals real[], doubles double precision[], domain_fixed {nested_domain}, domain_variable {variable_domain}, domain_reals {reals_domain}, domain_doubles {doubles_domain})")).await.unwrap();
    control.query(&format!("INSERT INTO {table} VALUES (1, '[-0,1.5]', '[1,2,3]', '{{-0,NULL}}', '{{5.25,NULL}}', '[-0,1.5]', '[1,2,3]', '{{-0,NULL}}', '{{5.25,NULL}}')")).await.unwrap();
    control.query(&format!("CREATE PUBLICATION {publication} FOR TABLE {table}")).await.unwrap();
    let config = SourceConfig { remote: "test".into(), slot_name: slot.clone(), publication_name: publication.clone(), indexer: None, ignore_tables: BTreeSet::new() };
    let mut snapshot = SnapshotReader::open(&remote, &config, Arc::clone(&memory)).await.unwrap();
    let start = snapshot.lsn;
    let snapshot_tx = snapshot.next_batch().await.unwrap().unwrap();
    assert!(snapshot.next_batch().await.unwrap().is_none());
    snapshot.finish().await.unwrap();
    wait_for_slot_release(&mut control, &slot).await;
    let mut stream = ChangeStream::open(&remote, &config, start, Arc::clone(&memory)).await.unwrap();
    control.query(&format!("UPDATE {table} SET _id = 2")).await.unwrap();
    let wal_tx = tokio::time::timeout(Duration::from_secs(20), async {
        loop { if let Some(transaction) = stream.read().await.unwrap().transaction { break transaction; } }
    }).await.unwrap();
    drop(stream);
    wait_for_slot_release(&mut control, &slot).await;
    control.query(&format!("SELECT pg_drop_replication_slot('{slot}')")).await.unwrap();
    control.query(&format!("DROP PUBLICATION {publication}")).await.unwrap();
    control.query(&format!("DROP TABLE {table}")).await.unwrap();

    for domain in [nested_domain, fixed_domain, variable_domain, reals_domain, doubles_domain] {
        control.query(&format!("DROP DOMAIN {domain}")).await.unwrap();
    }

    let snapshot = snapshot_tx.batches[0].rows.batch();
    let wal = wal_tx.batches[0].rows.batch();
    assert_eq!(snapshot.schema(), wal.schema());
    for column in 1..9 {
        assert_eq!(snapshot.column(column).to_data(), wal.column(column).to_data());
    }
    for column in 1..5 {
        assert_eq!(wal.column(column).to_data(), wal.column(column + 4).to_data());
    }
    let vector = wal.column(1).as_any().downcast_ref::<FixedSizeListArray>().unwrap();
    assert_eq!(vector.value_length(), 2);
    assert_eq!(vector.values().as_any().downcast_ref::<Float32Array>().unwrap().value(0).to_bits(), (-0.0_f32).to_bits());
    let half = wal.column(2).as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(half.value_length(0), 3);
    assert_eq!(half.values().data_type(), &DataType::Float32);
    let reals = wal.column(3).as_any().downcast_ref::<ListArray>().unwrap();
    assert!(reals.values().as_any().is::<Float32Array>());
    assert!(reals.values().is_null(1));
    let doubles = wal.column(4).as_any().downcast_ref::<ListArray>().unwrap();
    assert!(doubles.values().as_any().is::<Float64Array>());
    assert!(doubles.values().is_null(1));
}
