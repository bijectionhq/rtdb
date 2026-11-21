
#[tokio::test]
async fn eligible_trie_collection_strands_an_admitted_native_reader() {
    use rtdb_format::keys::{TableSlug, TrieKey};
    use rtdb_storage::ObjectKey;

    let directory = tempfile::tempdir().unwrap();
    let clock = Arc::new(ClockFixture(AtomicI64::new(1_000_000)));
    let table = TableRef::new("public", "docs");
    let slug = TableSlug::for_unrecorded_table(&table).unwrap();
    let old_trie: TrieKey = "l01-rc-b00".parse().unwrap();
    let data_key = ObjectKey::new(slug.data_key(&old_trie)).unwrap();
    let metadata_key = ObjectKey::new(slug.metadata_key(&old_trie)).unwrap();
    struct ServingCatalog(Arc<rtdb_index::PartitionValue>);
    impl rtdb_maintenance::SnapshotSource for ServingCatalog {
        fn snapshot(&self) -> Arc<rtdb_index::PartitionValue> { Arc::clone(&self.0) }
    }
    #[derive(Default)]
    struct Published(std::sync::Mutex<std::collections::BTreeSet<TrieKey>>);
    #[async_trait]
    impl rtdb_maintenance::TrieDeletionPublisher for Published {
        async fn publish(&self, _: TableRef, keys: Vec<TrieKey>) -> Result<rtdb_types::ReplicaPosition> {
            self.0.lock().unwrap().extend(keys);
            Ok(rtdb_types::ReplicaPosition(0))
        }
    }

    let schedule = FaultSchedule::new(vec![], 50_000).unwrap();
    let mut config = options(directory.path());
    config.compaction_threads = 1;
    let trie_config = config.trie;
    let (node, driver) = Node::open(config, services(clock, schedule.clone())).await.unwrap();
    let owner = tokio::spawn(driver.run());
    let database = node.catalog().database("xtdb").unwrap();
    let mut session = node.session("xtdb", Arc::new(Identity)).unwrap();
    let limit = Duration::from_secs(10);
    let insert = |id| TxOp::Sql { sql: format!("INSERT INTO docs RECORDS {{_id: {id}}}"), args: None };

    assert!(session.state_mut().execute_tx(vec![insert(1)], AutonomousTxOptions::default()).await.unwrap().committed());
    node.flush_all(limit).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    assert_eq!(
        database.snapshot().trie_entries(&table).iter().map(|trie| &trie.key).collect::<Vec<_>>(),
        [&old_trie],
    );
    database.object_store().head(&data_key).await.unwrap();
    database.object_store().head(&metadata_key).await.unwrap();

    // Admit and open the cursor without consuming the first persisted page.
    let mut reader = session.query_sql("SELECT _id FROM docs", vec![]).await.unwrap();
    assert!(session.state_mut().execute_tx(vec![insert(2)], AutonomousTxOptions::default()).await.unwrap().committed());
    node.flush_all(limit).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    assert!(database.trie_catalog().garbage_tries(&table, Timestamp::MAX).iter().any(|trie| trie.key == old_trie));

    let replacement = database.snapshot();
    let catalog = rtdb_index::PartitionValue::loaded(
        replacement.trie_catalog().clone(), replacement.table_catalog().clone(), trie_config,
    ).unwrap();
    let publisher = Arc::new(Published::default());
    let collector = rtdb_maintenance::TrieCollector::new(
        Arc::clone(database.object_store()),
        Arc::new(ServingCatalog(Arc::new(catalog))),
        publisher.clone(), Arc::new(Observer), rtdb_maintenance::GarbageConfig::default(),
    ).unwrap();
    collector.collect_as_of(Timestamp::MAX, node.cancellation()).await.unwrap();
    assert_eq!(*publisher.0.lock().unwrap(), [old_trie].into());
    for key in [&data_key, &metadata_key] {
        let error = Anomaly::from(database.object_store().head(key).await.unwrap_err());
        assert_eq!(error.code, "rtdb.storage/missing-object");
    }
    let resumed = tokio::time::timeout(limit, reader.next_batch()).await.unwrap();
    let fresh = rows(session.query_sql("SELECT _id FROM docs ORDER BY _id", vec![]).await.unwrap()).await;

    drop(reader);
    drop(collector);
    drop(session);
    drop(database);
    node.shutdown();
    tokio::time::timeout(limit, owner).await.unwrap().unwrap().unwrap();
    schedule.assert_exhausted().unwrap();

    assert_eq!(fresh, [vec![Value::I64(1)], vec![Value::I64(2)]]);
    let Err(error) = resumed else { panic!("the admitted reader did not observe the deletion"); };
    assert_eq!(error.code, "rtdb.storage/missing-object", "{error}");
    let missing_key = error.context.get("key").and_then(serde_json::Value::as_str).unwrap();
    assert!([data_key.as_str(), metadata_key.as_str()].contains(&missing_key), "{error}");
    eprintln!("age-only collection stranded an admitted query at {missing_key}");
}
