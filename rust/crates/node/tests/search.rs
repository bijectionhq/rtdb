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
    assert!(session.state_mut().execute_tx(vec![sql("DROP SEARCH INDEX docs_text")], AutonomousTxOptions::default()).await.unwrap().committed());
    assert!(rows(session.query_sql(definitions, vec![]).await.unwrap()).await.is_empty());
    let created_at = Value::Timestamp { value: created.tx_key().system_time.0, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) };
    let before_drop = "SELECT _id FROM xt.search_indexes FOR SYSTEM_TIME AS OF ?";
    assert_eq!(rows(session.query_sql(before_drop, vec![created_at]).await.unwrap()).await, [vec![Value::Utf8("docs_text".into())]]);
    for invalid in ["CREATE SEARCH INDEX docs_text ON docs ()", "CREATE SEARCH INDEX docs_text ON docs (_id)", "CREATE SEARCH INDEX docs_text ON docs (body, body)", "DROP SEARCH INDEX"] {
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
        }
        if round < 3 {
            database.flush().await.unwrap();
            node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
            database.compaction().unwrap().compact_all().await.unwrap();
            if round < 2 { database.build_artefacts().await.unwrap(); }
        }
    }
    let ids = |rows: Vec<Vec<Value>>| { let mut ids: Vec<_> = rows.into_iter().map(|row| row[0].clone()).map(|value| match value { Value::I64(id) => id, other => panic!("{other:?}") }).collect(); ids.sort_unstable(); ids };
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
async fn search_score_ranks_rarer_terms_above_common_ones_once_the_corpus_is_indexed() {
    let mut options = options();
    options.file_size_target = 1;
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    assert!(session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")], AutonomousTxOptions::default()).await.unwrap().committed());
    let mut operations = Vec::new();
    for id in 0..40 { operations.push(sql(&format!("INSERT INTO docs RECORDS {{_id: {id}, body: 'common words here'}}"))); }
    operations.push(sql("INSERT INTO docs RECORDS {_id: 100, body: 'common rare words'}"));
    operations.push(sql("INSERT INTO docs RECORDS {_id: 101, body: 'common common words'}"));
    assert!(session.state_mut().execute_tx(operations, AutonomousTxOptions::default()).await.unwrap().committed());
    let database = node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    database.build_artefacts().await.unwrap();
    let ranked = rows(session.query_sql("SELECT _id, SEARCH_SCORE(body, 'common rare') AS score FROM docs WHERE MATCH(body, 'common') ORDER BY score DESC, _id LIMIT 3", vec![]).await.unwrap()).await;
    assert_eq!(ranked.iter().map(|row| row[0].clone()).collect::<Vec<_>>(), [Value::I64(100), Value::I64(101), Value::I64(0)], "{ranked:?}");
    let scores: Vec<f64> = ranked.iter().map(|row| match row[1] { Value::F64(score) => score, ref other => panic!("{other:?}") }).collect();
    assert!(scores[0] > 2.0 * scores[1], "the rare term carries more weight than a repeated common one: {scores:?}");
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
async fn a_search_file_left_unpublished_is_deleted_once_its_definition_leaves_force() {
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
    assert_eq!(artefacts().await, recorded, "the stray is deleted; the dropped definition's recorded files stay with their tries");
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
    for (matching, scoring) in [("invoice", "invoice overdue"), ("overdue late", "late"), ("book", "book red paid")] {
        for valid in ["2020-06-01Z", "2022-06-01Z"] {
            for system in [None, Some(systems[0]), Some(systems[1])] {
                let system_clause = system.map_or(String::new(), |time| format!(" FOR SYSTEM_TIME AS OF TIMESTAMP '{}'", chrono::DateTime::from_timestamp_micros(time.0).unwrap().to_rfc3339()));
                let query = format!("SELECT _id, SEARCH_SCORE(body, '{scoring}') AS score FROM docs FOR VALID_TIME AS OF TIMESTAMP '{valid}'{system_clause} WHERE MATCH(body, '{matching}') ORDER BY score DESC, _id");
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
