use std::sync::{Arc, atomic::{AtomicI64, Ordering}};
use std::time::Duration;

use async_trait::async_trait;
use rtdb_columnar::read_value;
use rtdb_maintenance::CollectionObserver;
use rtdb_node::*;
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows, CatalogUser, CatalogUsers};
use rtdb_query::QueryCursor;
use rtdb_tx::{TransactionAuthorization, TxOp};
use rtdb_types::{Anomaly, Result, TableRef, Timestamp, Value};

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
fn sql(statement: &str) -> TxOp { TxOp::Sql { sql: statement.into(), args: None } }
fn ids(rows: Vec<Vec<Value>>) -> Vec<i64> {
    let mut ids: Vec<i64> = rows.into_iter().map(|row| match row[0] { Value::I64(id) => id, ref other => panic!("unexpected id {other:?}") }).collect();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn match_selects_the_version_visible_at_the_queried_valid_time() {
    let (node, driver) = Node::open(options(), services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    for statement in [
        "INSERT INTO docs RECORDS {_id: 1, body: 'The invoice is OVERDUE', _valid_from: TIMESTAMP '2024-01-01Z'}",
        "INSERT INTO docs RECORDS {_id: 2, body: 'An overdue library book', _valid_from: TIMESTAMP '2024-01-01Z'}",
        "INSERT INTO docs RECORDS {_id: 3, body: 42, _valid_from: TIMESTAMP '2024-01-01Z'}",
        "INSERT INTO docs RECORDS {_id: 1, body: 'The invoice is paid', _valid_from: TIMESTAMP '2024-06-01Z'}",
    ] {
        assert!(session.state_mut().execute_tx(vec![sql(statement)], AutonomousTxOptions::default()).await.unwrap().committed(), "{statement}");
    }
    let query = |at: &str| format!("SELECT _id FROM docs FOR VALID_TIME AS OF TIMESTAMP '{at}' WHERE MATCH(body, 'overdue INVOICE')");
    assert_eq!(ids(rows(session.query_sql(&query("2024-03-01Z"), vec![]).await.unwrap()).await), [1]);
    assert!(ids(rows(session.query_sql(&query("2024-07-01Z"), vec![]).await.unwrap()).await).is_empty(), "the later version no longer matches");
    let any = "SELECT _id FROM docs FOR VALID_TIME AS OF TIMESTAMP '2024-03-01Z' WHERE MATCH(body, 'overdue')";
    assert_eq!(ids(rows(session.query_sql(any, vec![]).await.unwrap()).await), [1, 2]);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn search_index_definitions_are_documents_as_of_their_transaction() {
    let (node, driver) = Node::open(options(), services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let created = session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_text ON docs (title, body)")], AutonomousTxOptions::default()).await.unwrap();
    assert!(created.committed());
    let definitions = "SELECT _id, schema_name, table_name, columns, analyzer FROM xt.search_indexes";
    assert_eq!(rows(session.query_sql(definitions, vec![]).await.unwrap()).await, [vec![
        Value::Utf8("docs_text".into()), Value::Utf8("public".into()), Value::Utf8("docs".into()),
        Value::List(vec![Value::Utf8("title".into()), Value::Utf8("body".into())]), Value::Utf8("simple".into()),
    ]]);
    let version = rows(session.query_sql("SELECT version FROM xt.search_indexes", vec![]).await.unwrap()).await;
    assert_eq!(version, [vec![Value::I64(created.tx_key().tx_id.0)]], "the version is the defining transaction");
    assert!(session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX IF NOT EXISTS docs_text ON docs (body) WITH (history = false)")], AutonomousTxOptions::default()).await.unwrap().committed());
    assert_eq!(rows(session.query_sql("SELECT version FROM xt.search_indexes", vec![]).await.unwrap()).await, version);
    assert!(session.state_mut().execute_tx(vec![sql("DROP SEARCH INDEX docs_text")], AutonomousTxOptions::default()).await.unwrap().committed());
    assert!(rows(session.query_sql(definitions, vec![]).await.unwrap()).await.is_empty());
    let created_at = Value::Timestamp { value: created.tx_key().system_time.0, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) };
    let before_drop = "SELECT _id FROM xt.search_indexes FOR SYSTEM_TIME AS OF ?";
    assert_eq!(rows(session.query_sql(before_drop, vec![created_at]).await.unwrap()).await, [vec![Value::Utf8("docs_text".into())]]);
    assert!(session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_now ON docs (body) WITH (history = false)")], AutonomousTxOptions::default()).await.unwrap().committed());
    let histories = rows(session.query_sql("SELECT _id, history FROM xt.search_indexes", vec![]).await.unwrap()).await;
    assert_eq!(histories, [vec![Value::Utf8("docs_now".into()), Value::Bool(false)]]);
    for invalid in ["CREATE SEARCH INDEX docs_text ON docs ()", "CREATE SEARCH INDEX docs_text ON docs (_id)", "CREATE SEARCH INDEX docs_text ON docs (body, body)", "DROP SEARCH INDEX",
        "CREATE SEARCH INDEX docs_text ON docs (body) WITH (depth = false)", "CREATE SEARCH INDEX docs_text ON docs (body) WITH (history = maybe)", "CREATE SEARCH INDEX docs_text ON docs (body) WITH (history = false) extra"] {
        assert!(session.state_mut().execute_tx(vec![sql(invalid)], AutonomousTxOptions::default()).await.map(|result| !result.committed()).unwrap_or(true), "{invalid}");
    }
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn the_lane_records_a_search_file_on_every_settled_trie_of_an_indexed_table() {
    let mut options = options();
    options.file_size_target = 1;
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    assert!(session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")], AutonomousTxOptions::default()).await.unwrap().committed());
    for id in 0..20 {
        let statement = format!("INSERT INTO docs RECORDS {{_id: {id}, body: 'document number {id} about {}'}}", if id % 2 == 0 { "invoices" } else { "books" });
        assert!(session.state_mut().execute_tx(vec![sql(&statement)], AutonomousTxOptions::default()).await.unwrap().committed());
    }
    let database = node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    database.build_artefacts().await.unwrap();
    let docs = TableRef::new("public", "docs");
    let catalog = database.trie_catalog();
    let settled: Vec<_> = catalog.current_tries(&docs).into_iter().filter(|trie| trie.key.level() > 0).collect();
    assert!(!settled.is_empty(), "compaction produced no settled trie");
    for trie in settled {
        let record = trie.artefacts.iter().find(|record| record.kind() == "search").unwrap_or_else(|| panic!("{} has no search file", trie.key));
        assert!(record.definition_id().starts_with("docs_body~a"));
    }
    database.build_artefacts().await.unwrap();
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn owned_search_counts_no_documents_for_null_or_absent_text() {
    use rtdb_types::{EntityId, OwnedRelation, OwnedSchema, StructFields};

    let mut options = options();
    options.owned_relations = true;
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("owned-sparse-search").unwrap();
    let path = OwnedRelation::new("owned-sparse-body").unwrap();
    let schema = OwnedSchema::from_json(br#"{"fields":[{"name":"body","type":{"kind":"utf8"},"optional":true,"nullable":true}],"validity":"acceptance"}"#).unwrap();
    let rows = rtdb_tx::Documents::new(vec![
        StructFields::from([("_id".into(), Value::I64(1))]),
        StructFields::from([("_id".into(), Value::I64(2)), ("body".into(), Value::Null)]),
    ], node.memory()).unwrap();
    let installed = session.state_mut().execute_tx(vec![
        TxOp::OwnedDeclareRelation { relation: relation.clone(), schema },
        TxOp::OwnedDeclareSearchPath { relation: relation.clone(), name: path.clone(), column: "body".into() },
        TxOp::OwnedInsertDocs { relation: relation.clone(), documents: rows, valid_from: None, valid_to: None },
    ], AutonomousTxOptions::default()).await.unwrap();
    assert!(installed.committed(), "{:?}", installed.error());

    let held = session.open_owned_read().await.unwrap();
    assert_eq!(held.search_corpus(&relation, &path, "invoice").await.unwrap().documents, 0);
    let partitions = [(relation, path)];
    let (empty, _) = held.ranked_search_page(&partitions, "invoice", 0, 4, false).await.unwrap();
    assert_eq!(empty.total, 0);
    let mut staged = HeldTextCorpus::new(Arc::clone(node.memory()), "invoice").unwrap();
    staged.shadow(0, EntityId::Integer(1), Some("invoice")).unwrap();
    let (populated, _) = held.ranked_search_with_overlay(&partitions, "invoice", 0, 4, false, None, Some(staged)).await.unwrap();
    assert_eq!(populated.total, 1);
    assert_eq!(populated.hits[0].id, EntityId::Integer(1));
    drop(held);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn owned_search_declaration_is_private_and_builds_real_search_artefacts() {
    use rtdb_types::{EntityId, OwnedRelation, StructFields};

    let mut options = options();
    options.owned_relations = true;
    options.file_size_target = 1;
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("owned-search-docs").unwrap();
    let path = OwnedRelation::new("owned-body").unwrap();
    let documents = rtdb_tx::Documents::new((0..20).map(|id| StructFields::from([
        ("_id".into(), Value::I64(id)),
        ("body".into(), Value::Utf8("an invoice".into())),
        ("secret".into(), Value::Binary(vec![7, 9])),
    ])).collect(), node.memory()).unwrap();
    let insert = TxOp::OwnedInsertDocs { relation: relation.clone(), documents, valid_from: Some(Timestamp(0)), valid_to: None };
    assert!(session.state_mut().execute_tx(vec![insert], AutonomousTxOptions::default()).await.unwrap().committed());

    let declare = |column: &str| TxOp::OwnedDeclareSearchPath { relation: relation.clone(), name: path.clone(), column: column.into() };
    let rejected = session.state_mut().execute_tx(vec![declare("secret")], AutonomousTxOptions::default()).await.unwrap();
    assert!(!rejected.committed());
    let accepted = session.state_mut().execute_tx(vec![declare("body")], AutonomousTxOptions::default()).await.unwrap();
    assert!(accepted.committed(), "{:?}", accepted.error());
    let changed = rtdb_tx::Documents::new(vec![StructFields::from([
        ("_id".into(), Value::I64(0)),
        ("body".into(), Value::Utf8("paid".into())),
    ])], node.memory()).unwrap();
    let replace = TxOp::OwnedReplaceDocs { relation: relation.clone(), documents: changed, valid_from: Some(Timestamp(0)), valid_to: None };
    assert!(session.state_mut().execute_tx(vec![replace], AutonomousTxOptions::default()).await.unwrap().committed());
    let uncovered = session.open_owned_read().await.unwrap();
    assert_eq!(uncovered.search_corpus(&relation, &path, "invoice paid").await.unwrap_err().code, "rtdb.owned/search-uncovered");
    let partitions = [(relation.clone(), path.clone())];
    let staged = || {
        let mut stage = rtdb_node::HeldTextCorpus::new(Arc::clone(node.memory()), "invoice").unwrap();
        stage.shadow(0, EntityId::Integer(1), None).unwrap();
        stage.shadow(0, EntityId::Integer(2), Some("paid")).unwrap();
        stage.shadow(0, EntityId::String("1".into()), Some("an invoice")).unwrap();
        stage
    };
    let (uncovered_rank, _) = uncovered.ranked_search_with_overlay(
        &partitions, "invoice", 0, 2, false, None, Some(staged()),
    ).await.unwrap();
    assert_eq!(uncovered_rank.total, 18);
    assert_eq!(uncovered_rank.hits[0].id, EntityId::String("1".into()));
    assert_eq!(uncovered_rank.hits[1].id, EntityId::Integer(3));
    drop(uncovered);
    let private_catalog = OwnedRelation::search_catalog("xtdb");
    let direct = format!("SELECT _id FROM {}.\"{}\"", private_catalog.schema, private_catalog.table);
    assert!(session.query_sql(&direct, vec![]).await.is_err());

    let database = node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    database.build_artefacts().await.unwrap();
    let catalog = database.trie_catalog();
    let table = relation.table("xtdb");
    let settled: Vec<_> = catalog.current_tries(&table).into_iter().filter(|trie| trie.key.level() > 0).collect();
    assert!(!settled.is_empty(), "compaction produced no settled owned trie");
    assert!(settled.iter().all(|trie| trie.artefacts.iter().any(|record| {
        record.kind() == "search" && record.definition_id().starts_with("owned-body~a")
            && record.definition_version() == accepted.tx_key().tx_id.0
    })));
    let held = session.open_owned_read().await.unwrap();
    let corpus = held.search_corpus(&relation, &path, "invoice paid").await.unwrap();
    assert_eq!(corpus.documents, 20);
    assert_eq!(corpus.tokens, 39);
    assert_eq!(corpus.frequencies["invoice"], 19);
    assert_eq!(corpus.frequencies["paid"], 1);
    let (covered_rank, _) = held.ranked_search_with_overlay(
        &partitions, "invoice", 0, 2, false, None, Some(staged()),
    ).await.unwrap();
    assert_eq!(covered_rank.total, uncovered_rank.total);
    assert_eq!(covered_rank.hits, uncovered_rank.hits);
    let (hits, _) = held.ranked_search(&partitions, "invoice", 2, false).await.unwrap();
    assert_eq!(hits.iter().map(|hit| hit.id.clone()).collect::<Vec<_>>(),
        [rtdb_types::EntityId::Integer(1), rtdb_types::EntityId::Integer(2)]);
    assert_eq!(hits[0].score.to_bits(), hits[1].score.to_bits());
    let (paid, _) = held.ranked_search(&partitions, "paid", 2, false).await.unwrap();
    assert_eq!(paid[0].id, rtdb_types::EntityId::Integer(0));
    drop(held);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

struct Counting { inner: rtdb_search::SearchAccelerator, narrowed: std::sync::atomic::AtomicUsize }
#[async_trait]
impl rtdb_query::ScanAccelerator for Counting {
    async fn candidate_iids(&self, request: &rtdb_query::ScanRequest<'_>) -> Result<Option<std::collections::BTreeSet<rtdb_types::Iid>>> {
        let candidates = self.inner.candidate_iids(request).await?;
        if candidates.is_some() { self.narrowed.fetch_add(1, Ordering::Relaxed); }
        Ok(candidates)
    }
}

#[tokio::test]
async fn accelerated_match_returns_what_the_unaccelerated_predicate_returns_at_every_basis() {
    let mut options = options();
    options.file_size_target = 1;
    options.automatic_artefacts = false;
    let counting = Arc::new(Counting { inner: rtdb_search::SearchAccelerator::default(), narrowed: Default::default() });
    let services = NodeServices { scan_accelerators: vec![counting.clone()], ..services() };
    let (node, driver) = Node::open(options, services).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    let execute = |statement: String| TxOp::Sql { sql: statement, args: None };
    assert!(session.state_mut().execute_tx(vec![execute("CREATE SEARCH INDEX docs_body ON docs (body)".into())], AutonomousTxOptions::default()).await.unwrap().committed());
    let words = ["invoice", "overdue", "paid", "book", "library", "red", "blue", "green", "late", "early", "lost", "found", "open", "closed", "spare", "sharp"];
    let mut systems = Vec::new();
    let mut seed = 7u64;
    let mut next = || { seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); (seed >> 33) as usize };
    for round in 0..4 {
        for id in 0..150 {
            let body: Vec<_> = (0..2).map(|_| words[next() % words.len()]).collect();
            let year = 2020 + next() % 4;
            let statement = match next() % 6 {
                0 => format!("DELETE FROM docs FOR PORTION OF VALID_TIME FROM TIMESTAMP '{year}-01-01Z' TO TIMESTAMP '{}-01-01Z' WHERE _id = {id}", year + 1),
                1 => format!("INSERT INTO docs RECORDS {{_id: {id}, body: {}, _valid_from: TIMESTAMP '{year}-01-01Z'}}", next() % 100),
                _ => format!("INSERT INTO docs RECORDS {{_id: {id}, body: '{}', _valid_from: TIMESTAMP '{year}-01-01Z', _valid_to: TIMESTAMP '{}-06-01Z'}}", body.join(" "), year + 1),
            };
            let result = session.state_mut().execute_tx(vec![execute(statement)], AutonomousTxOptions::default()).await.unwrap();
            if id == 0 { systems.push(result.tx_key().system_time); }
            // Two blocks a round make four full level-1 tries by the second, which compact into a level-2 partition group.
            if id == 74 && round < 3 {
                database.flush().await.unwrap();
                node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
            }
        }
        if round < 3 {
            database.flush().await.unwrap();
            node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
            database.compaction().unwrap().compact_all().await.unwrap();
            if round < 2 { database.build_artefacts().await.unwrap(); }
        }
    }
    let ids = |rows: Vec<Vec<Value>>| { let mut ids: Vec<_> = rows.into_iter().map(|row| row[0].clone()).map(|value| match value { Value::I64(id) => id, other => panic!("{other:?}") }).collect(); ids.sort_unstable(); ids };
    let shared: Vec<_> = database.trie_catalog().current_tries(&TableRef::new("public", "docs")).into_iter()
        .filter(|trie| trie.artefacts.iter().any(|record| record.kind() == "search" && record.scope() == rtdb_format::log::proto::ArtefactScope::PartGroup))
        .map(|trie| trie.key.clone()).collect();
    let groups: std::collections::BTreeSet<_> = shared.iter().filter_map(|key| key.part_group()).collect();
    assert_eq!(shared.len(), groups.len() * 4, "every member of a group shares its file: {shared:?}");
    assert!(shared.iter().any(|key| key.level() == 2 && key.recency().is_none()), "the level-2 partition group's members share one search file: {shared:?}");
    let docs = TableRef::new("public", "docs");
    let snapshot = database.snapshot();
    let slug = &snapshot.table_entry(&docs).unwrap().slug;
    let mut expected_files = std::collections::BTreeMap::new();
    for trie in snapshot.trie_catalog().current_tries(&docs) {
        for record in trie.artefacts.iter().filter(|record| record.kind() == "search") {
            let key = slug.recorded_artefact_key(&trie.key, record).unwrap();
            expected_files.insert(key, record.file_size.unwrap());
        }
    }
    let files = rows(session.query_sql(
        "SELECT object_key, file_size FROM xt.live_trie_artefacts WHERE schema_name = 'public' AND table_name = 'docs' AND kind = 'search'",
        vec![],
    ).await.unwrap()).await;
    assert_eq!(files.len(), expected_files.len(), "each live file is charged once");
    for file in files {
        let [Value::Utf8(key), Value::I64(size)] = file.as_slice() else { panic!("invalid file usage: {file:?}") };
        assert_eq!(expected_files.remove(key), Some(*size));
        let object = rtdb_storage::ObjectKey::new(key.clone()).unwrap();
        assert_eq!(database.object_store().head_fresh(&object).await.unwrap().size, u64::try_from(*size).unwrap());
    }
    assert!(expected_files.is_empty());
    let mut compared = 0;
    let mut answers = std::collections::BTreeMap::<&str, std::collections::BTreeSet<Vec<i64>>>::new();
    for query in ["overdue", "invoice overdue", "red blue", "paid book", "absent", "\"red blue\"", "overdue \"late book\""] {
        for valid in ["2020-03-01Z", "2021-07-01Z", "2023-02-01Z"] {
            for system in [None, Some(systems[1]), Some(systems[3])] {
                let system_clause = system.map_or(String::new(), |time| format!(" FOR SYSTEM_TIME AS OF TIMESTAMP '{}'", chrono::DateTime::from_timestamp_micros(time.0).unwrap().to_rfc3339()));
                let accelerated = format!("SELECT _id FROM docs FOR VALID_TIME AS OF TIMESTAMP '{valid}'{system_clause} WHERE MATCH(body, '{query}')");
                let oracle = format!("SELECT _id FROM docs FOR VALID_TIME AS OF TIMESTAMP '{valid}'{system_clause} WHERE MATCH(body, '{query}') OR _id < -1");
                let expected = ids(rows(session.query_sql(&oracle, vec![]).await.unwrap()).await);
                assert_eq!(ids(rows(session.query_sql(&accelerated, vec![]).await.unwrap()).await), expected, "{accelerated}");
                answers.entry(query).or_default().insert(expected);
                compared += 1;
            }
        }
    }
    assert_eq!(compared, 63);
    assert!(answers["overdue"].len() > 2 && answers["overdue"].iter().any(|ids| !ids.is_empty()), "the bases disagree, so the test can tell versions apart: {answers:?}");
    assert_eq!(answers["absent"], std::collections::BTreeSet::from([Vec::new()]));
    assert!(counting.narrowed.load(Ordering::Relaxed) >= compared, "every MATCH scan was narrowed");
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn search_scores_agree_across_plans_and_ignore_unselected_documents() {
    let mut options = options();
    options.file_size_target = 1;
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let writes = vec![
        sql("CREATE SEARCH INDEX docs_body ON docs (body)"),
        sql("INSERT INTO docs RECORDS {_id: 100, body: 'common rare words'}"),
        sql("INSERT INTO docs RECORDS {_id: 101, body: 'common common words'}"),
    ];
    assert!(session.state_mut().execute_tx(writes, AutonomousTxOptions::default()).await.unwrap().committed());
    let statement = |predicate: &str| format!("SELECT _id, SEARCH_SCORE(body, 'common rare') AS score FROM docs WHERE {predicate} ORDER BY score DESC, _id LIMIT 2");
    let indexed = statement("MATCH(body, 'common')");
    let expected = rows(session.query_sql(&indexed, vec![]).await.unwrap()).await;
    let scoring = rtdb_text::Query::parse("common rare");
    assert_eq!(expected, [(100, "common rare words"), (101, "common common words")].map(|(id, body)| {
        vec![Value::I64(id), Value::F64(rtdb_text::bm25(&scoring, &rtdb_text::tokens(body), None))]
    }));

    let unselected = (0..40).map(|id| sql(&format!("INSERT INTO docs RECORDS {{_id: {id}, body: 'rare words'}}"))).collect();
    assert!(session.state_mut().execute_tx(unselected, AutonomousTxOptions::default()).await.unwrap().committed());
    let database = node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    database.build_artefacts().await.unwrap();
    let residual = statement("MATCH(body, 'common') OR _id = -1");
    assert_eq!(rows(session.query_sql(&indexed, vec![]).await.unwrap()).await, expected);
    assert_eq!(rows(session.query_sql(&residual, vec![]).await.unwrap()).await, expected);
    assert!(session.state_mut().execute_tx(
        vec![sql("DROP SEARCH INDEX docs_body")], AutonomousTxOptions::default(),
    ).await.unwrap().committed());
    assert_eq!(rows(session.query_sql(&indexed, vec![]).await.unwrap()).await, expected);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn compaction_publishes_each_trie_it_writes_with_its_search_file() {
    let mut options = options();
    options.file_size_target = 1;
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    assert!(session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")], AutonomousTxOptions::default()).await.unwrap().committed());
    for id in 0..20 {
        assert!(session.state_mut().execute_tx(vec![sql(&format!("INSERT INTO docs RECORDS {{_id: {id}, body: 'note {}'}}", if id % 2 == 0 { "overdue" } else { "paid" }))], AutonomousTxOptions::default()).await.unwrap().committed());
    }
    let database = node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    let docs = TableRef::new("public", "docs");
    let catalog = database.trie_catalog();
    let settled: Vec<_> = catalog.current_tries(&docs).into_iter().filter(|trie| trie.key.level() > 0).collect();
    assert!(!settled.is_empty());
    let store = Arc::clone(database.object_store());
    for trie in settled {
        let record = trie.artefacts.iter().find(|record| record.kind() == "search").unwrap_or_else(|| panic!("{} was published without its search file", trie.key));
        let key = rtdb_storage::ObjectKey::new(format!("tables/public$docs/artefacts/{}/search/{}-v{}.bin", trie.key, record.definition_id(), record.definition_version())).unwrap();
        assert!(store.head(&key).await.is_ok(), "{key} was uploaded before the trie was published");
    }
    assert_eq!(ids(rows(session.query_sql("SELECT _id FROM docs WHERE MATCH(body, 'overdue')", vec![]).await.unwrap()).await), (0..20).step_by(2).collect::<Vec<_>>());
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn a_search_file_a_scan_cannot_read_is_rebuilt_to_its_original_bytes() {
    let mut options = options();
    options.file_size_target = 1;
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    assert!(session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")], AutonomousTxOptions::default()).await.unwrap().committed());
    for id in 0..10 {
        assert!(session.state_mut().execute_tx(vec![sql(&format!("INSERT INTO docs RECORDS {{_id: {id}, body: 'note {}'}}", if id % 2 == 0 { "overdue" } else { "paid" }))], AutonomousTxOptions::default()).await.unwrap().committed());
    }
    let database = node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    database.build_artefacts().await.unwrap();
    let store = Arc::clone(database.object_store());
    let artefacts: Vec<_> = store.list(Some(&rtdb_storage::ObjectKey::new("tables").unwrap())).await.unwrap().into_iter().filter(|object| object.key.as_str().contains("/artefacts/")).collect();
    assert!(!artefacts.is_empty());
    let key = artefacts[0].key.clone();
    let original = store.get(&key).await.unwrap();
    store.delete(&key).await.unwrap();
    store.put(&key, b"not a search file".to_vec().into()).await.unwrap();
    let query = "SELECT _id FROM docs WHERE MATCH(body, 'overdue')";
    assert_eq!(ids(rows(session.query_sql(query, vec![]).await.unwrap()).await), [0, 2, 4, 6, 8], "an unreadable search file costs time, not rows");
    database.build_artefacts().await.unwrap();
    assert_eq!(store.get(&key).await.unwrap(), original, "the lane rebuilt the file it was told about, to the same bytes");
    assert_eq!(ids(rows(session.query_sql(query, vec![]).await.unwrap()).await), [0, 2, 4, 6, 8]);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn a_dropped_index_leaves_no_search_file_and_no_record_behind() {
    let mut options = options();
    options.file_size_target = 1;
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    assert!(session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")], AutonomousTxOptions::default()).await.unwrap().committed());
    for id in 0..10 {
        assert!(session.state_mut().execute_tx(vec![sql(&format!("INSERT INTO docs RECORDS {{_id: {id}, body: 'note {id}'}}"))], AutonomousTxOptions::default()).await.unwrap().committed());
    }
    let database = node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    database.build_artefacts().await.unwrap();
    let store = Arc::clone(database.object_store());
    let artefacts = || async { store.list(Some(&rtdb_storage::ObjectKey::new("tables").unwrap())).await.unwrap().into_iter().map(|object| object.key).filter(|key| key.as_str().contains("/artefacts/")).collect::<Vec<_>>() };
    let recorded = artefacts().await;
    assert!(!recorded.is_empty());
    let format = format!("f{}-v", rtdb_search::FORMAT_VERSION);
    let stray = rtdb_storage::ObjectKey::new(recorded[0].as_str().replace(&format, "f0-v")).unwrap();
    store.put(&stray, b"uploaded by a builder that failed before publishing".to_vec().into()).await.unwrap();
    database.build_artefacts().await.unwrap();
    assert!(artefacts().await.contains(&stray), "the lane looks again only when the table's definitions change");
    assert!(session.state_mut().execute_tx(vec![sql("DROP SEARCH INDEX docs_body")], AutonomousTxOptions::default()).await.unwrap().committed());
    database.build_artefacts().await.unwrap();
    assert_eq!(artefacts().await, Vec::new(), "the recorded files and the one left unpublished are deleted");
    let docs = TableRef::new("public", "docs");
    assert!(database.trie_catalog().current_tries(&docs).iter().all(|trie| trie.artefacts.is_empty()), "and no trie records them");
    assert_eq!(ids(rows(session.query_sql("SELECT _id FROM docs WHERE MATCH(body, 'note')", vec![]).await.unwrap()).await), (0..10).collect::<Vec<_>>());
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn live_rows_written_between_queries_are_found_as_the_live_index_grows() {
    let (node, driver) = Node::open(options(), services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    assert!(session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")], AutonomousTxOptions::default()).await.unwrap().committed());
    let accelerated = "SELECT _id FROM docs WHERE MATCH(body, 'overdue')";
    let oracle = "SELECT _id FROM docs WHERE MATCH(body, 'overdue') OR _id < -1";
    let mut expected = Vec::new();
    for id in 0..6 {
        let body = if id % 2 == 0 { "an overdue note" } else { "a paid note" };
        assert!(session.state_mut().execute_tx(vec![sql(&format!("INSERT INTO docs RECORDS {{_id: {id}, body: '{body}'}}"))], AutonomousTxOptions::default()).await.unwrap().committed());
        if id % 2 == 0 { expected.push(id); }
        assert_eq!(ids(rows(session.query_sql(accelerated, vec![]).await.unwrap()).await), expected, "after row {id}");
        assert_eq!(ids(rows(session.query_sql(oracle, vec![]).await.unwrap()).await), expected);
    }
    assert!(session.state_mut().execute_tx(vec![sql("INSERT INTO docs RECORDS {_id: 0, body: 'now paid'}")], AutonomousTxOptions::default()).await.unwrap().committed());
    assert_eq!(ids(rows(session.query_sql(accelerated, vec![]).await.unwrap()).await), [2, 4], "a later version that no longer matches hides the earlier one");
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn plain_match_scans_when_the_index_build_cannot_fit() {
    let mut options = options();
    options.execution_memory_bytes = 16 << 20;
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let statements = [
        "CREATE SEARCH INDEX docs_body ON docs (body)",
        "INSERT INTO docs RECORDS {_id: 1, body: 'invoice overdue'}, {_id: 2, body: 'paid'}",
    ];
    for statement in statements {
        assert!(session.state_mut().execute_tx(vec![sql(statement)], AutonomousTxOptions::default()).await.unwrap().committed());
    }
    let database = node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    let actual = ids(rows(session.query_sql("SELECT _id FROM docs WHERE MATCH(body, 'invoice')", vec![]).await.unwrap()).await);
    assert_eq!(actual, [1]);
    let score = rtdb_text::bm25(&rtdb_text::Query::parse("invoice"), &rtdb_text::tokens("invoice overdue"), None);
    let scored = rows(session.query_sql("SELECT _id, SEARCH_SCORE(body, 'invoice') FROM docs WHERE MATCH(body, 'invoice')", vec![]).await.unwrap()).await;
    assert_eq!(scored, [vec![Value::I64(1), Value::F64(score)]]);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn plain_match_scans_historical_pages_with_independent_recency_order() {
    let mut options = options();
    options.automatic_artefacts = false;
    options.compaction.page_size = 1;
    options.compaction.within_file_recency_branching = true;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    assert!(session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")], AutonomousTxOptions::default()).await.unwrap().committed());

    let mut entities: Vec<i64> = (0..24).collect();
    entities.sort_by_key(|id| rtdb_types::EntityId::Integer(*id).iid());
    let operations = entities.iter().enumerate().map(|(offset, id)| {
        let end = if offset % 2 == 1 { "01:00:00" } else { "12:00:00" };
        sql(&format!("INSERT INTO docs RECORDS {{_id: {id}, body: 'invoice', _valid_from: TIMESTAMP '1970-01-01Z', _valid_to: TIMESTAMP '1970-01-05T{end}Z'}}"))
    }).collect();
    assert!(session.state_mut().execute_tx(operations, AutonomousTxOptions::default()).await.unwrap().committed());
    let database = node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();

    let snapshot = database.snapshot();
    let table = TableRef::new("public", "docs");
    let entry = snapshot.table_entry(&table).unwrap();
    let history = snapshot.trie_entries(&table).into_iter().find(|trie| trie.key.recency().is_some()).unwrap();
    let key = rtdb_storage::ObjectKey::new(entry.slug.data_key(&history.key)).unwrap();
    let lease = database.storage().get(&key).await.unwrap();
    let cancellation = tokio_util::sync::CancellationToken::new();
    let columns = ["body".to_string()];
    let physical_iids: Vec<_> = rtdb_search::search_pages(lease.as_slice(), &columns, &cancellation).unwrap()
        .flat_map(|page| page.unwrap().into_iter().map(|row| row.iid)).collect();
    assert_eq!(physical_iids.len(), entities.len());
    assert!(physical_iids.windows(2).any(|pair| pair[0] > pair[1]));
    drop(lease);

    let query = "SELECT _id FROM docs FOR VALID_TIME AS OF TIMESTAMP '1970-01-05T00:00:00Z' WHERE MATCH(body, 'invoice')";
    assert_eq!(ids(rows(session.query_sql(query, vec![]).await.unwrap()).await), (0..24).collect::<Vec<_>>());
    let scored = "SELECT _id, SEARCH_SCORE(body, 'invoice') AS score FROM docs FOR VALID_TIME AS OF TIMESTAMP '1970-01-05T00:00:00Z' WHERE MATCH(body, 'invoice') ORDER BY score DESC, _id LIMIT 1";
    let score = rtdb_text::bm25(&rtdb_text::Query::parse("invoice"), &rtdb_text::tokens("invoice"), None);
    assert_eq!(rows(session.query_sql(scored, vec![]).await.unwrap()).await, [vec![Value::I64(0), Value::F64(score)]]);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn a_ranked_cutoff_counts_only_rows_that_pass_every_match() {
    let mut options = options();
    options.file_size_target = 1;
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let operations = vec![
        sql("CREATE SEARCH INDEX docs_headline ON docs (headline)"),
        sql("INSERT INTO docs RECORDS {_id: 1, headline: 'invoice priority', status: 'unpaid'}"),
        sql("INSERT INTO docs RECORDS {_id: 2, headline: 'invoice standard', status: 'paid'}"),
    ];
    for operation in operations {
        assert!(session.state_mut().execute_tx(vec![operation], AutonomousTxOptions::default()).await.unwrap().committed());
    }
    let database = node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    database.build_artefacts().await.unwrap();
    let sql = "SELECT _id FROM docs WHERE MATCH(headline, 'invoice') AND MATCH(status, 'paid') ORDER BY SEARCH_SCORE(headline, 'priority') DESC, _id";
    let all = ids(rows(session.query_sql(sql, vec![]).await.unwrap()).await);
    let top = ids(rows(session.query_sql(&format!("{sql} LIMIT 1"), vec![]).await.unwrap()).await);
    assert_eq!(all, [2]);
    assert_eq!(top, all);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn explicit_search_statistics_rank_the_admitted_corpus() {
    let (node, driver) = Node::open(options(), services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let committed = session.state_mut().execute_tx(vec![
        sql("CREATE SEARCH INDEX docs_body ON docs (body)"),
        sql("INSERT INTO docs RECORDS {_id: 1, body: 'invoice paid invoice', admitted: TRUE}"),
        sql("INSERT INTO docs RECORDS {_id: 2, body: 'paid', admitted: TRUE}"),
        sql("INSERT INTO docs RECORDS {_id: 3, body: 'invoice paid secret', admitted: FALSE}"),
    ], AutonomousTxOptions::default()).await.unwrap();
    assert!(committed.committed());
    let ranked = "
        WITH MATERIALIZED admitted_docs AS (SELECT _id, body FROM docs WHERE admitted),
        counts AS (
            SELECT COUNT(SEARCH_LENGTH(body)) AS n, SUM(SEARCH_LENGTH(body)) AS tokens,
                   COUNT(CASE WHEN MATCH(body, 'invoice') THEN 1 END) AS invoices,
                   COUNT(CASE WHEN MATCH(body, 'paid') THEN 1 END) AS paid
            FROM admitted_docs
        )
        SELECT d._id, SEARCH_SCORE(d.body, 'invoice paid', c.n, c.tokens, ARRAY[c.invoices, c.paid]) AS score
        FROM admitted_docs AS d CROSS JOIN counts AS c
        WHERE MATCH(d.body, 'invoice paid')
        ORDER BY score DESC, d._id LIMIT 1";
    let first = rows(session.query_sql(ranked, vec![]).await.unwrap()).await;
    let corpus = rtdb_text::Corpus {
        documents: 2,
        tokens: 4,
        frequencies: std::collections::BTreeMap::from([("invoice".into(), 1), ("paid".into(), 2)]),
    };
    let scorer = rtdb_text::Scorer::new(rtdb_text::Query::parse("invoice paid"), Some(&corpus));
    let score = scorer.bm25(&rtdb_text::tokens("invoice paid invoice"));
    assert_eq!(first, [vec![Value::I64(1), Value::F64(score)]]);
    let terms = rows(session.query_sql(
        r#"SELECT SEARCH_TERMS('"Invoice paid" invoice')"#,
        vec![],
    ).await.unwrap()).await;
    assert_eq!(terms, [vec![Value::List(vec![Value::Utf8("invoice".into()), Value::Utf8("paid".into())])]]);
    let fingerprint = rows(session.query_sql(
        "SELECT SEARCH_LENGTH(42), SEARCH_LENGTH(''), \
         SEARCH_CORPUS_FINGERPRINT(ARRAY['1', '2'], ARRAY['a', 'b']) = \
         SEARCH_CORPUS_FINGERPRINT(ARRAY['1', '2'], ARRAY['a', 'b']), \
         SEARCH_CORPUS_FINGERPRINT(ARRAY['1', '2'], ARRAY['a', 'b']) = \
         SEARCH_CORPUS_FINGERPRINT(ARRAY['1', '2'], ARRAY['a', 'c'])",
        vec![],
    ).await.unwrap()).await;
    assert_eq!(fingerprint, [vec![Value::Null, Value::I64(0), Value::Bool(true), Value::Bool(false)]]);


    let changed = session.state_mut().execute_tx(vec![
        sql("INSERT INTO docs RECORDS {_id: 3, body: 'public list archived', admitted: FALSE}"),
        sql("INSERT INTO docs RECORDS {_id: 4, body: 'invoice paid another secret', admitted: FALSE}"),
    ], AutonomousTxOptions::default()).await.unwrap();
    assert!(changed.committed());
    assert_eq!(rows(session.query_sql(ranked, vec![]).await.unwrap()).await, first);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn a_ranked_limit_returns_the_same_rows_as_ranking_everything_and_keeping_the_first() {
    let mut options = options();
    options.file_size_target = 1;
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    assert!(session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")], AutonomousTxOptions::default()).await.unwrap().committed());
    let words = ["invoice", "overdue", "paid", "book", "late", "red"];
    let mut seed = 11u64;
    let mut next = || { seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); (seed >> 33) as usize };
    let mut systems = Vec::new();
    for round in 0..3 {
        let mut operations = Vec::new();
        for id in 0..120 {
            let length = 1 + next() % 8;
            let body: Vec<_> = (0..length).map(|_| words[next() % words.len()]).collect();
            let year = 2020 + next() % 3;
            operations.push(sql(&format!("INSERT INTO docs RECORDS {{_id: {id}, body: '{}', _valid_from: TIMESTAMP '{year}-01-01Z'}}", body.join(" "))));
        }
        systems.push(session.state_mut().execute_tx(operations, AutonomousTxOptions::default()).await.unwrap().tx_key().system_time);
        if round < 2 {
            database.flush().await.unwrap();
            node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
            database.compaction().unwrap().compact_all().await.unwrap();
            if round == 0 { database.build_artefacts().await.unwrap(); }
        }
    }
    let ranked = |rows: Vec<Vec<Value>>| rows.into_iter().map(|row| (row[0].clone(), row[1].clone())).collect::<Vec<_>>();
    for (filter, scoring) in [("MATCH(body, 'invoice')", "invoice overdue"), ("MATCH(body, 'overdue late')", "late"), ("MATCH(body, 'book')", "book red paid"),
        ("MATCH(body, 'invoice') AND MATCH(body, 'paid')", "invoice overdue"), ("MATCH(body, 'book') AND _id % 3 = 0", "book")] {
        for valid in ["2020-06-01Z", "2022-06-01Z"] {
            for system in [None, Some(systems[0]), Some(systems[1])] {
                let system_clause = system.map_or(String::new(), |time| format!(" FOR SYSTEM_TIME AS OF TIMESTAMP '{}'", chrono::DateTime::from_timestamp_micros(time.0).unwrap().to_rfc3339()));
                let query = format!("SELECT _id, SEARCH_SCORE(body, '{scoring}') AS score FROM docs FOR VALID_TIME AS OF TIMESTAMP '{valid}'{system_clause} WHERE {filter} ORDER BY score DESC, _id");
                let everything = ranked(rows(session.query_sql(&query, vec![]).await.unwrap()).await);
                for k in [1, 5, 20] {
                    let limited = ranked(rows(session.query_sql(&format!("{query} LIMIT {k}"), vec![]).await.unwrap()).await);
                    assert_eq!(limited, everything.iter().take(k).cloned().collect::<Vec<_>>(), "{query} LIMIT {k}");
                }
                for unbounded in [format!("{query} LIMIT 9223372036854775807"), format!("{query} LIMIT 9223372036854775807 OFFSET 1")] {
                    let skip = usize::from(unbounded.ends_with("OFFSET 1"));
                    assert_eq!(ranked(rows(session.query_sql(&unbounded, vec![]).await.unwrap()).await), everything.iter().skip(skip).cloned().collect::<Vec<_>>(), "{unbounded}");
                }
                let offset = ranked(rows(session.query_sql(&format!("{query} LIMIT 3 OFFSET 4"), vec![]).await.unwrap()).await);
                assert_eq!(offset, everything.iter().skip(4).take(3).cloned().collect::<Vec<_>>(), "{query} LIMIT 3 OFFSET 4");
            }
        }
    }
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

/// Records how many IIDs each narrowing named, to tell a ranked narrowing from an unranked one.
struct Recording { inner: Arc<rtdb_search::SearchAccelerator>, sizes: std::sync::Mutex<Vec<usize>> }
#[async_trait]
impl rtdb_query::ScanAccelerator for Recording {
    async fn candidate_iids(&self, request: &rtdb_query::ScanRequest<'_>) -> Result<Option<std::collections::BTreeSet<rtdb_types::Iid>>> {
        let candidates = self.inner.candidate_iids(request).await?;
        if let Some(candidates) = &candidates { self.sizes.lock().unwrap().push(candidates.len()); }
        Ok(candidates)
    }
}

#[tokio::test]
async fn a_ranked_limit_over_several_partitions_returns_the_same_rows_as_ranking_everything() {
    let mut options = options();
    options.file_size_target = 1;
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    assert!(session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")], AutonomousTxOptions::default()).await.unwrap().committed());
    let words = ["invoice", "overdue", "paid", "book", "late", "red"];
    let mut seed = 23u64;
    let mut next = || { seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); (seed >> 33) as usize };
    let mut partitions = Vec::new();
    for round in 0..2 {
        let operations = (0..150).map(|id| {
            let body: Vec<_> = (0..1 + next() % 8).map(|_| words[next() % words.len()]).collect();
            sql(&format!("INSERT INTO docs RECORDS {{_id: {}, body: '{}'}}", id + 1000 * round, body.join(" ")))
        }).collect();
        assert!(session.state_mut().execute_tx(operations, AutonomousTxOptions::default()).await.unwrap().committed());
        database.flush().await.unwrap();
        node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
        database.compaction().unwrap().compact_all().await.unwrap();
        database.build_artefacts().await.unwrap();
        partitions.push(database.snapshot());
    }
    let memory = rtdb_columnar::MemoryBudget::new(1 << 30);
    let recording = Arc::new(Recording { inner: rtdb_search::SearchRuntime::new(64 << 20).accelerator(), sizes: Default::default() });
    let run = |query: String| {
        let catalog = rtdb_query::CatalogSnapshot::new("xtdb", std::collections::BTreeMap::from([("xtdb".to_string(), rtdb_query::DatabaseSnapshot { partitions: partitions.clone(), storage: Arc::clone(database.storage()) })]), false).unwrap();
        let (memory, recording) = (Arc::clone(&memory), Arc::clone(&recording));
        async move {
            let context = rtdb_query::QueryContext::new(catalog, Arc::clone(&memory), Timestamp(i64::MAX / 2), "UTC", rtdb_query::QueryOptions::default(), &tokio_util::sync::CancellationToken::new()).unwrap()
                .with_accelerators(Arc::from([recording as Arc<dyn rtdb_query::ScanAccelerator>]));
            let prepared = rtdb_sql::prepare_in(&query, &context.catalog, &rtdb_sql::PrepareOptions::default(), &memory).unwrap();
            let cursor = rtdb_query::PreparedQuery::from_prepared(prepared, &memory).unwrap().execute(context, Vec::new()).await.unwrap();
            rows(cursor).await.into_iter().map(|row| (row[0].clone(), row[1].clone())).collect::<Vec<_>>()
        }
    };
    for (matching, scoring) in [("invoice", "invoice overdue"), ("overdue late", "late"), ("book", "book red paid")] {
        let query = format!("SELECT _id, SEARCH_SCORE(body, '{scoring}') AS score FROM docs WHERE MATCH(body, '{matching}') ORDER BY score DESC, _id");
        let everything = run(query.clone()).await;
        assert!(everything.len() > 40, "{query} matched only {}", everything.len());
        for k in [1, 5, 20] {
            recording.sizes.lock().unwrap().clear();
            assert_eq!(run(format!("{query} LIMIT {k}")).await, everything.iter().take(k).cloned().collect::<Vec<_>>(), "{query} LIMIT {k}");
            let sizes = recording.sizes.lock().unwrap().clone();
            assert_eq!(sizes.len(), 2, "each partition narrowed");
            assert!(sizes.iter().sum::<usize>() < everything.len(), "{query} LIMIT {k}: each partition was ranked, not merely narrowed: {sizes:?}");
        }
    }
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

/// Counts the rows a scan's MATCH took from search files rather than from the row's own value.
struct Answered { inner: Arc<dyn FileMatches>, count: Arc<std::sync::atomic::AtomicUsize> }
impl FileMatches for Answered {
    fn value(&self, page: usize, row: usize) -> Result<Option<bool>> { self.count.fetch_add(1, Ordering::Relaxed); self.inner.value(page, row) }
}
struct Answering { inner: Arc<rtdb_search::SearchAccelerator>, answered: Arc<std::sync::atomic::AtomicUsize> }
#[async_trait]
impl rtdb_query::ScanAccelerator for Answering {
    async fn candidate_iids(&self, request: &rtdb_query::ScanRequest<'_>) -> Result<Option<std::collections::BTreeSet<rtdb_types::Iid>>> { self.inner.candidate_iids(request).await }
    async fn match_answers(&self, request: &rtdb_query::ScanRequest<'_>, column: &str, query: &str) -> Result<Option<rtdb_query::MatchAnswers>> {
        let Some(answers) = self.inner.match_answers(request, column, query).await? else { return Ok(None) };
        let mut counted = rtdb_query::MatchAnswers::default();
        for (key, matches) in answers.iter() {
            counted.insert(&rtdb_storage::ObjectKey::new(key).unwrap(), Arc::new(Answered { inner: Arc::clone(matches), count: Arc::clone(&self.answered) }));
        }
        Ok(Some(counted))
    }
}
use rtdb_query::FileMatches;

#[tokio::test]
async fn not_match_count_and_null_matches_return_what_the_unpushed_predicate_returns() {
    let mut options = options();
    options.file_size_target = 1;
    options.automatic_artefacts = false;
    let answering = Arc::new(Answering { inner: Arc::new(rtdb_search::SearchAccelerator::default()), answered: Default::default() });
    let services = NodeServices { scan_accelerators: vec![answering.clone()], ..services() };
    let (node, driver) = Node::open(options, services).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    assert!(session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")], AutonomousTxOptions::default()).await.unwrap().committed());
    let words = ["invoice", "overdue", "paid", "book", "late", "red"];
    let mut seed = 5u64;
    let mut next = || { seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); (seed >> 33) as usize };
    let mut systems = Vec::new();
    for round in 0..3 {
        for id in 0..120 {
            let body: Vec<_> = (0..1 + next() % 4).map(|_| words[next() % words.len()]).collect();
            let year = 2020 + next() % 3;
            let statement = match next() % 7 {
                0 => format!("DELETE FROM docs FOR PORTION OF VALID_TIME FROM TIMESTAMP '{year}-01-01Z' TO TIMESTAMP '{}-01-01Z' WHERE _id = {id}", year + 1),
                1 => format!("INSERT INTO docs RECORDS {{_id: {id}, body: {}, _valid_from: TIMESTAMP '{year}-01-01Z'}}", next() % 100),
                2 => format!("INSERT INTO docs RECORDS {{_id: {id}, title: 'untitled', _valid_from: TIMESTAMP '{year}-01-01Z'}}"),
                _ => format!("INSERT INTO docs RECORDS {{_id: {id}, body: '{}', _valid_from: TIMESTAMP '{year}-01-01Z'}}", body.join(" ")),
            };
            let result = session.state_mut().execute_tx(vec![sql(&statement)], AutonomousTxOptions::default()).await.unwrap();
            if id == 0 { systems.push(result.tx_key().system_time); }
        }
        if round < 2 {
            database.flush().await.unwrap();
            node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
            database.compaction().unwrap().compact_all().await.unwrap();
            database.build_artefacts().await.unwrap();
        }
    }
    let all = |rows: Vec<Vec<Value>>| { let mut rows: Vec<String> = rows.into_iter().map(|row| format!("{row:?}")).collect(); rows.sort(); rows };
    let mut nonempty = 0;
    for query in ["overdue", "invoice paid", "\"late book\"", "absent"] {
        for valid in ["2020-06-01Z", "2022-06-01Z"] {
            for system in [None, Some(systems[1])] {
                let at = format!("FOR VALID_TIME AS OF TIMESTAMP '{valid}'{}", system.map_or(String::new(), |time| format!(" FOR SYSTEM_TIME AS OF TIMESTAMP '{}'", chrono::DateTime::from_timestamp_micros(time.0).unwrap().to_rfc3339())));
                for (pushed, unpushed) in [
                    (format!("SELECT _id FROM docs {at} WHERE NOT MATCH(body, '{query}')"), format!("SELECT _id FROM docs {at} WHERE NOT MATCH(body, '{query}') OR _id < -1")),
                    (format!("SELECT count(*) FROM docs {at} WHERE MATCH(body, '{query}')"), format!("SELECT count(*) FROM docs {at} WHERE MATCH(body, '{query}') OR _id < -1")),
                    (format!("SELECT count(*) FROM docs {at} WHERE NOT MATCH(body, '{query}')"), format!("SELECT count(*) FROM docs {at} WHERE NOT MATCH(body, '{query}') OR _id < -1")),
                    (format!("SELECT _id, title FROM docs {at} WHERE MATCH(body, '{query}') IS NULL"), format!("SELECT _id, title FROM docs {at} WHERE MATCH(body, '{query}') IS NULL OR _id < -1")),
                ] {
                    let expected = all(rows(session.query_sql(&unpushed, vec![]).await.unwrap()).await);
                    assert_eq!(all(rows(session.query_sql(&pushed, vec![]).await.unwrap()).await), expected, "{pushed}");
                    if expected.len() > 1 { nonempty += 1; }
                }
            }
        }
    }
    assert!(nonempty > 10, "the queries find rows to compare");
    assert!(answering.answered.load(Ordering::Relaxed) > 100, "indexed rows were answered from search files");
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn an_index_without_history_answers_past_queries_as_scanning_does_with_no_historical_file() {
    let mut options = options();
    options.file_size_target = 1;
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    assert!(session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_body ON docs (body) WITH (history = false)")], AutonomousTxOptions::default()).await.unwrap().committed());
    let words = ["invoice", "overdue", "paid", "book"];
    let mut systems = Vec::new();
    for round in 0..3 {
        let operations = (0..60).flat_map(|id| [("01", 0), ("07", 1)].map(|(month, half)| sql(&format!("INSERT INTO docs RECORDS {{_id: {id}, body: '{} {}', _valid_from: TIMESTAMP '202{round}-{month}-01Z'}}", words[(id + round + half) % 4], words[(id * 3 + round) % 4])))).collect();
        systems.push(session.state_mut().execute_tx(operations, AutonomousTxOptions::default()).await.unwrap().tx_key().system_time);
        database.flush().await.unwrap();
        node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
        database.compaction().unwrap().compact_all().await.unwrap();
        database.build_artefacts().await.unwrap();
    }
    let docs = TableRef::new("public", "docs");
    let catalog = database.trie_catalog();
    let tries = catalog.current_tries(&docs);
    let recorded = |trie: &&rtdb_index::CatalogTrie| trie.artefacts.iter().any(|record| record.kind() == "search");
    assert!(tries.iter().any(|trie| trie.key.recency().is_some()), "compaction wrote history to test against");
    assert!(tries.iter().filter(|trie| trie.key.recency().is_some()).all(|trie| !recorded(trie)), "no historical trie has a search file");
    assert!(tries.iter().any(|trie| trie.key.recency().is_none() && recorded(trie)), "the current side has search files");
    for valid in ["2020-03-01Z", "2020-09-01Z", "2021-06-01Z", "2022-09-01Z"] {
        for system in [Some(systems[0]), Some(systems[1]), None] {
            let at = format!("FOR VALID_TIME AS OF TIMESTAMP '{valid}'{}", system.map_or(String::new(), |time| format!(" FOR SYSTEM_TIME AS OF TIMESTAMP '{}'", chrono::DateTime::from_timestamp_micros(time.0).unwrap().to_rfc3339())));
            for query in ["invoice", "overdue paid"] {
                let pushed = format!("SELECT _id FROM docs {at} WHERE MATCH(body, '{query}')");
                let unpushed = format!("SELECT _id FROM docs {at} WHERE MATCH(body, '{query}') OR _id < -1");
                assert_eq!(ids(rows(session.query_sql(&pushed, vec![]).await.unwrap()).await), ids(rows(session.query_sql(&unpushed, vec![]).await.unwrap()).await), "{pushed}");
            }
        }
    }
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn resolved_document_scores_are_unmoved_by_compaction() {
    let mut options = options();
    options.file_size_target = 1;
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    assert!(session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")], AutonomousTxOptions::default()).await.unwrap().committed());
    let words = ["invoice", "overdue", "paid", "book", "late", "red"];
    let mut seed = 41u64;
    let mut next = || { seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); (seed >> 33) as usize };
    for round in 0..3 {
        for id in 0..60 {
            let body: Vec<_> = (0..1 + next() % 5).map(|_| words[next() % words.len()]).collect();
            let statement = match next() % 8 {
                0 => format!("DELETE FROM docs WHERE _id = {id}"),
                1 => format!("INSERT INTO docs RECORDS {{_id: {id}, body: {}}}", next() % 100),
                2 => format!("INSERT INTO docs RECORDS {{_id: {id}, title: 'untitled'}}"),
                _ => format!("INSERT INTO docs RECORDS {{_id: {id}, body: '{}'}}", body.join(" ")),
            };
            assert!(session.state_mut().execute_tx(vec![sql(&statement)], AutonomousTxOptions::default()).await.unwrap().committed());
        }
        if round < 2 {
            database.flush().await.unwrap();
            node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
            if round == 0 { database.compaction().unwrap().compact_all().await.unwrap(); }
        }
    }
    let scoring = rtdb_text::Query::parse("invoice overdue");
    let query = "SELECT _id, body, SEARCH_SCORE(body, 'invoice overdue') AS score FROM docs WHERE MATCH(body, 'invoice') ORDER BY _id";
    let scored = rows(session.query_sql(query, vec![]).await.unwrap()).await;
    assert!(scored.len() > 5, "{scored:?}");
    for row in &scored {
        let Value::Utf8(body) = &row[1] else { panic!("{row:?}") };
        assert_eq!(row[2], Value::F64(rtdb_text::bm25(&scoring, &rtdb_text::tokens(body), None)), "{row:?}");
    }
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    database.build_artefacts().await.unwrap();
    assert_eq!(rows(session.query_sql(query, vec![]).await.unwrap()).await, scored, "the same snapshot scores the same whatever search files exist");
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn same_transaction_writes_score_only_the_visible_document() {
    let mut options = options();
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();

    let operations = vec![
        sql("CREATE SEARCH INDEX docs_body ON docs (body)"),
        sql("INSERT INTO docs RECORDS {_id: 1, body: 'superseded alpha'}"),
        sql("INSERT INTO docs RECORDS {_id: 1, body: 'visible beta beta'}"),
        sql("INSERT INTO docs RECORDS {_id: 2, body: 'other alpha'}"),
    ];
    let result = session.state_mut().execute_tx(operations, AutonomousTxOptions::default()).await.unwrap();
    assert!(result.committed(), "{result:?}");

    let scoring = rtdb_text::Query::parse("beta");
    let statement = "SELECT _id, SEARCH_SCORE(body, 'beta') AS score FROM docs WHERE MATCH(body, 'visible') ORDER BY score DESC LIMIT 1";
    let expected = vec![vec![
        Value::I64(1),
        Value::F64(rtdb_text::bm25(&scoring, &rtdb_text::tokens("visible beta beta"), None)),
    ]];

    assert_eq!(rows(session.query_sql(statement, vec![]).await.unwrap()).await, expected);
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    database.build_artefacts().await.unwrap();
    assert_eq!(rows(session.query_sql(statement, vec![]).await.unwrap()).await, expected);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}
