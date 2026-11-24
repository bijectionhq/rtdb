use std::sync::{Arc, atomic::{AtomicI64, AtomicUsize, Ordering}};
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
    NodeServices { resources: Arc::new(NativeResources::default().with_local_log_clock(clock.clone(), false)), clock, ..NodeServices::local_with_search_format(Arc::new(Authority), Arc::new(Observer), Arc::new(CatalogFixtures), Arc::new(CatalogFixtures), rtdb_node::SearchFileFormat::Paged) }
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
        Value::list(vec![Value::Utf8("title".into()), Value::Utf8("body".into())]), Value::Utf8("simple".into()),
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
    assert!(session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_stemmed ON docs (body) WITH (analyzer = 'english_stop', history = false)")], AutonomousTxOptions::default()).await.unwrap().committed());
    let analyzers = rows(session.query_sql("SELECT _id, analyzer, history FROM xt.search_indexes WHERE _id = 'docs_stemmed'", vec![]).await.unwrap()).await;
    assert_eq!(analyzers, [vec![Value::Utf8("docs_stemmed".into()), Value::Utf8("english_stop".into()), Value::Bool(false)]]);
    for invalid in ["CREATE SEARCH INDEX docs_text ON docs ()",
        "CREATE SEARCH INDEX docs_text ON docs (body) WITH (analyzer = 'klingon')", "CREATE SEARCH INDEX docs_text ON docs (body) WITH (analyzer = english)",
        "CREATE SEARCH INDEX docs_text ON docs (body) WITH (analyzer = 'english', analyzer = 'french')", "CREATE SEARCH INDEX docs_text ON docs (_id)", "CREATE SEARCH INDEX docs_text ON docs (body, body)", "DROP SEARCH INDEX",
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
    verify_owned_search_empty_corpus(false).await;
    verify_owned_search_empty_corpus(true).await;
}

fn owned_search_schema(wrapped: bool, acceptance: bool) -> rtdb_types::OwnedSchema {
    let kind = if wrapped {
        serde_json::json!({"kind":"struct","fields":[
            {"name":"v2_value","type":{"kind":"utf8"},"optional":false,"nullable":true}
        ]})
    } else {
        serde_json::json!({"kind":"utf8"})
    };
    let mut schema = serde_json::json!({
        "fields": [{"name":"body","type": kind,"optional": true,"nullable": !wrapped}],
        "validity": if acceptance { "acceptance" } else { "explicit" },
    });
    if wrapped { schema["version"] = 2.into(); }
    rtdb_types::OwnedSchema::from_json(&serde_json::to_vec(&schema).unwrap()).unwrap()
}

fn owned_search_value(wrapped: bool, value: Value) -> Value {
    if wrapped {
        Value::Struct(rtdb_types::StructFields::from([("v2_value".into(), value)]))
    } else {
        value
    }
}

async fn verify_owned_search_empty_corpus(wrapped: bool) {
    use rtdb_types::{EntityId, OwnedRelation, StructFields};

    let mut options = options();
    options.owned_relations = true;
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("owned-sparse-search").unwrap();
    let path = OwnedRelation::new("owned-sparse-body").unwrap();
    let schema = owned_search_schema(wrapped, true);
    let rows = rtdb_tx::Documents::new(vec![
        StructFields::from([("_id".into(), Value::I64(1))]),
        StructFields::from([("_id".into(), Value::I64(2)), ("body".into(), owned_search_value(wrapped, Value::Null))]),
    ], node.memory()).unwrap();
    let installed = session.state_mut().execute_tx(vec![
        TxOp::OwnedDeclareRelation { relation: relation.clone(), schema },
        TxOp::OwnedDeclareSearchPath { relation: relation.clone(), name: path.clone(), column: "body".into(), analyzer: rtdb_plan::Analyzer::Simple },
        TxOp::OwnedInsertDocs { relation: relation.clone(), documents: rows, valid_from: None, valid_to: None },
    ], AutonomousTxOptions::default()).await.unwrap();
    assert!(installed.committed(), "{:?}", installed.error());

    let held = session.open_owned_read().await.unwrap();
    assert_eq!(held.search_corpus(&relation, &path, "invoice").await.unwrap().documents, 0);
    let partitions = [(relation, path)];
    let (empty, _) = held.ranked_search_page(&partitions, "invoice", 0, 4, false).await.unwrap();
    assert_eq!(empty.total, 0);
    let mut staged = HeldTextCorpus::new(Arc::clone(node.memory()), rtdb_text::Analyzer::Simple, "invoice").unwrap();
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
    verify_owned_search_artefacts(13, false).await;
}

#[tokio::test]
async fn an_owned_corpus_reads_a_published_format_eleven_file() {
    verify_owned_search_artefacts(11, false).await;
}

#[tokio::test]
async fn an_owned_corpus_reads_a_published_format_fourteen_tree() {
    verify_owned_search_artefacts(14, false).await;
}

#[tokio::test]
async fn an_owned_corpus_reads_a_published_format_fifteen_tree() {
    verify_owned_search_artefacts(15, false).await;
}

#[tokio::test]
async fn owned_fact_text_ranks_the_same_before_and_after_sidecar_publication() {
    verify_owned_search_artefacts(13, true).await;
}

async fn verify_owned_search_artefacts(format: u32, wrapped: bool) {
    use rtdb_types::{EntityId, OwnedRelation, StructFields};

    let mut options = options();
    options.owned_relations = true;
    options.file_size_target = 1;
    options.automatic_artefacts = false;
    let runtime = match format {
        11 => rtdb_search::SearchRuntime::with_tantivy_files(16 << 20),
        14 => rtdb_search::SearchRuntime::with_tree_files(16 << 20),
        15 => rtdb_search::SearchRuntime::with_search_packs(16 << 20),
        _ => rtdb_search::SearchRuntime::new(16 << 20),
    };
    let services = NodeServices { artefact_kinds: vec![runtime.artefacts()], scan_accelerators: vec![runtime.accelerator()], ..services() };
    let (node, driver) = Node::open(options, services).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("owned-search-docs").unwrap();
    let path = OwnedRelation::new("owned-body").unwrap();
    let documents = rtdb_tx::Documents::new((0..20).map(|id| {
        let mut row = StructFields::from([
            ("_id".into(), Value::I64(id)),
            ("body".into(), owned_search_value(wrapped, Value::Utf8("an invoice".into()))),
        ]);
        if !wrapped { row.insert("secret".into(), Value::Binary(vec![7, 9])); }
        row
    }).collect(), node.memory()).unwrap();
    let mut setup = Vec::new();
    if wrapped {
        setup.push(TxOp::OwnedDeclareRelation { relation: relation.clone(), schema: owned_search_schema(true, false) });
    }
    setup.push(TxOp::OwnedInsertDocs { relation: relation.clone(), documents, valid_from: Some(Timestamp(0)), valid_to: None });
    let result = session.state_mut().execute_tx(setup, AutonomousTxOptions::default()).await.unwrap();
    assert!(result.committed(), "{:?}", result.error());

    let declare = |column: &str| TxOp::OwnedDeclareSearchPath { relation: relation.clone(), name: path.clone(), column: column.into(), analyzer: rtdb_plan::Analyzer::Simple };
    let rejected = session.state_mut().execute_tx(vec![declare("secret")], AutonomousTxOptions::default()).await.unwrap();
    assert!(!rejected.committed());
    let accepted = session.state_mut().execute_tx(vec![declare("body")], AutonomousTxOptions::default()).await.unwrap();
    assert!(accepted.committed(), "{:?}", accepted.error());
    let changed = rtdb_tx::Documents::new(vec![StructFields::from([
        ("_id".into(), Value::I64(0)),
        ("body".into(), owned_search_value(wrapped, Value::Utf8("paid".into()))),
    ])], node.memory()).unwrap();
    let replace = TxOp::OwnedReplaceDocs { relation: relation.clone(), documents: changed, valid_from: Some(Timestamp(0)), valid_to: None };
    assert!(session.state_mut().execute_tx(vec![replace], AutonomousTxOptions::default()).await.unwrap().committed());
    let uncovered = session.open_owned_read().await.unwrap();
    assert_eq!(uncovered.search_corpus(&relation, &path, "invoice paid").await.unwrap_err().code, "rtdb.owned/search-uncovered");
    let partitions = [(relation.clone(), path.clone())];
    let staged = || {
        let mut stage = rtdb_node::HeldTextCorpus::new(Arc::clone(node.memory()), rtdb_text::Analyzer::Simple, "invoice").unwrap();
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
    let (uncovered_phrase, _) = uncovered.ranked_search_page(&partitions, "\"an invoice\"", 1, 2, false).await.unwrap();
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
            && record.definition_id().ends_with(&format!("f{format}"))
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
    let (covered_phrase, _) = held.ranked_search_page(&partitions, "\"an invoice\"", 1, 2, false).await.unwrap();
    assert_eq!(covered_phrase.total, uncovered_phrase.total);
    assert_eq!(covered_phrase.hits, uncovered_phrase.hits);
    let (hits, _) = held.ranked_search(&partitions, "invoice", 2, false).await.unwrap();
    assert_eq!(hits.iter().map(|hit| hit.id.clone()).collect::<Vec<_>>(),
        [rtdb_types::EntityId::Integer(1), rtdb_types::EntityId::Integer(2)]);
    assert_eq!(hits[0].score.to_bits(), hits[1].score.to_bits());
    let (page, _) = held.ranked_search_page(&partitions, "invoice", 1, 2, false).await.unwrap();
    assert_eq!(page.total, 19);
    assert_eq!(page.hits.into_iter().map(|hit| hit.id).collect::<Vec<_>>(),
        [rtdb_types::EntityId::Integer(2), rtdb_types::EntityId::Integer(3)]);
    let (paid, _) = held.ranked_search(&partitions, "paid", 2, false).await.unwrap();
    assert_eq!(paid[0].id, rtdb_types::EntityId::Integer(0));
    drop(held);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

fn spatial_point(x: f64, y: f64) -> Vec<u8> {
    let mut bytes = vec![0, 1, 1, 0, 0, 0];
    bytes.extend_from_slice(&x.to_le_bytes());
    bytes.extend_from_slice(&y.to_le_bytes());
    bytes
}

fn spatial_ids(page: &rtdb_spatial::Page) -> Vec<rtdb_types::EntityId> {
    let Some(rows) = page.rows() else { return Vec::new(); };
    let rows = rows.batch();
    (0..rows.num_rows()).map(|row| {
        rtdb_columnar::read_entity_id(rows.schema_ref().field(0), rows.column(0).as_ref(), row)
            .unwrap().into_owned()
    }).collect()
}

#[tokio::test]
async fn held_spatial_reads_resolve_moved_rows_and_replaced_generations_across_compaction() {
    use rtdb_spatial::{PageOrder, Predicate, Request};
    use rtdb_types::{EntityId, OwnedRelation, OwnedSchema, StructFields};
    let mut options = options();
    options.owned_relations = true;
    options.file_size_target = 1;
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("places").unwrap();
    let path = OwnedRelation::new("by-tenant-location").unwrap();
    let schema = OwnedSchema::from_json(br#"{"fields":[{"name":"tenant","type":{"kind":"binary"},"optional":false,"nullable":false},{"name":"location","type":{"kind":"binary"},"optional":true,"nullable":false}]}"#).unwrap();
    let declare = || TxOp::OwnedDeclareSpatialPath {
        relation: relation.clone(), name: path.clone(), columns: vec!["location".into(), "tenant".into()],
    };
    let request = |after, limit| Request {
        partition: vec![vec![1].into()], region: spatial_point(179.999, 0.0).into(), predicate: Predicate::WithinDistance(500.0),
        valid_at: Timestamp(25), page: PageOrder::Identity { after }, limit,
    };
    let document = |id: EntityId, tenant, x| StructFields::from([
        ("_id".into(), id.as_value()), ("tenant".into(), Value::Binary(vec![tenant])),
        ("location".into(), Value::Binary(spatial_point(x, 0.0))),
    ]);
    let read_ids = |page: rtdb_spatial::Page| (spatial_ids(&page), page.more);
    let opts = AutonomousTxOptions::default;
    let created = session.state_mut().execute_tx(vec![
        TxOp::OwnedDeclareRelation { relation: relation.clone(), schema }, declare(),
    ], opts()).await.unwrap();
    assert!(created.committed(), "{:?}", created.error());
    let empty = session.open_owned_read().await.unwrap();
    assert!(spatial_ids(&empty.spatial(&relation, &path, request(None, 1), false).await.unwrap().0).is_empty());
    drop(empty);

    let moved = EntityId::Integer(1);
    let same_iid = EntityId::Uuid(moved.iid().0);
    let documents = rtdb_tx::Documents::new(vec![
        document(moved.clone(), 1, 179.999),
        document(EntityId::Integer(2), 1, -179.999),
        document(EntityId::Integer(3), 2, 179.999),
        document(same_iid.clone(), 2, 179.999),
    ], node.memory()).unwrap();
    assert!(session.state_mut().execute_tx(vec![TxOp::OwnedInsertDocs {
        relation: relation.clone(), documents, valid_from: Some(Timestamp(0)), valid_to: None,
    }], opts()).await.unwrap().committed());
    let old = session.open_owned_read().await.unwrap();
    let (first, _) = old.spatial(&relation, &path, request(None, 1), false).await.unwrap();
    assert_eq!(read_ids(first), (vec![EntityId::Integer(1)], true));
    let (second, _) = old.spatial(&relation, &path, request(Some(rtdb_columnar::EntityIdRef::Integer(1)), 1), false).await.unwrap();
    assert_eq!(read_ids(second), (vec![EntityId::Integer(2)], false));

    let documents = rtdb_tx::Documents::new(vec![document(moved, 1, 0.0)], node.memory()).unwrap();
    assert!(session.state_mut().execute_tx(vec![TxOp::OwnedReplaceDocs {
        relation: relation.clone(), documents, valid_from: Some(Timestamp(10)), valid_to: None,
    }], opts()).await.unwrap().committed());
    let db = node.catalog().database("xtdb").unwrap();
    db.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    db.compaction().unwrap().compact_all().await.unwrap();
    db.build_artefacts().await.unwrap();
    assert!(db.trie_catalog().current_tries(&relation.table("xtdb")).iter().any(|trie| {
        trie.artefacts.iter().any(|record| record.kind() == "spatial")
    }));
    let new = session.open_owned_read().await.unwrap();
    assert_eq!(read_ids(new.spatial(&relation, &path, request(None, 128), false).await.unwrap().0), (vec![EntityId::Integer(2)], false));
    let mut prior_valid = request(None, 128);
    prior_valid.valid_at = Timestamp(5);
    let (projected, _) = new.spatial_projected(&relation, &path, prior_valid, false, rtdb_spatial::Projection::Documents).await.unwrap();
    let documents = projected.documents().unwrap().batch();
    assert_eq!(documents.num_rows(), 2);
    assert!(documents.column_by_name("_system_from").is_some());
    for (row, longitude) in [179.999, -179.999].into_iter().enumerate() {
        let (index, field) = documents.schema_ref().column_with_name("location").unwrap();
        assert_eq!(rtdb_columnar::binary_cell(field, documents.column(index).as_ref(), row).unwrap(), Some(spatial_point(longitude, 0.0).as_slice()));
    }
    assert_eq!(read_ids(projected), (vec![EntityId::Integer(1), EntityId::Integer(2)], false));
    let mut other_partition = request(None, 128);
    other_partition.partition = vec![vec![2].into()];
    assert_eq!(read_ids(new.spatial(&relation, &path, other_partition, false).await.unwrap().0), (vec![EntityId::Integer(3), same_iid], false));
    assert_eq!(read_ids(old.spatial(&relation, &path, request(None, 128), false).await.unwrap().0), (vec![EntityId::Integer(1), EntityId::Integer(2)], false));
    drop(old);
    assert!(session.state_mut().execute_tx(vec![TxOp::OwnedDropSpatialPath {
        relation: relation.clone(), name: path.clone(),
    }], opts()).await.unwrap().committed());
    let removed = session.open_owned_read().await.unwrap();
    assert_eq!(removed.spatial(&relation, &path, request(None, 1), false).await.err().unwrap().code, "rtdb.owned/spatial-path");
    assert!(session.state_mut().execute_tx(vec![declare()], opts()).await.unwrap().committed());
    let recreated = session.open_owned_read().await.unwrap();
    assert_eq!(read_ids(recreated.spatial(&relation, &path, request(None, 128), false).await.unwrap().0), (vec![EntityId::Integer(2)], false));
    drop((new, removed, recreated));
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn unindexed_spatial_reads_decode_only_geometry_visible_at_the_held_cut() {
    use rtdb_spatial::{PageOrder, Predicate, Request};
    use rtdb_types::{EntityId, OwnedRelation, OwnedSchema, StructFields};

    let mut options = options();
    options.owned_relations = true;
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let database = node.catalog().database("xtdb").unwrap();
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("unindexed-places").unwrap();
    let path = OwnedRelation::new("by-place").unwrap();
    let schema = OwnedSchema::from_json(br#"{"fields":[{"name":"location","type":{"kind":"binary"},"optional":false,"nullable":false}]}"#).unwrap();
    let documents = |location| rtdb_tx::Documents::new(vec![StructFields::from([
        ("_id".into(), Value::I64(1)), ("location".into(), Value::Binary(location)),
    ])], node.memory()).unwrap();
    let opts = AutonomousTxOptions::default;
    let initial = session.state_mut().execute_tx(vec![
        TxOp::OwnedDeclareRelation { relation: relation.clone(), schema },
        TxOp::OwnedDeclareSpatialPath { relation: relation.clone(), name: path.clone(), columns: vec!["location".into()] },
        TxOp::OwnedInsertDocs { relation: relation.clone(), documents: documents(vec![0]), valid_from: Some(Timestamp(0)), valid_to: None },
    ], opts()).await.unwrap();
    assert!(initial.committed(), "{:?}", initial.error());
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    assert!(!database.trie_catalog().current_tries(&relation.table("xtdb")).is_empty());
    let latest = session.state_mut().execute_tx(vec![TxOp::OwnedReplaceDocs {
        relation: relation.clone(), documents: documents(spatial_point(1.0, 2.0)),
        valid_from: Some(Timestamp(10)), valid_to: None,
    }], opts()).await.unwrap();
    assert!(latest.committed(), "{:?}", latest.error());

    let held = session.open_owned_read().await.unwrap();
    let request = |valid_at| Request {
        partition: vec![], region: spatial_point(1.0, 2.0).into(), predicate: Predicate::WithinDistance(1.0),
        valid_at: Timestamp(valid_at), page: PageOrder::Identity { after: None }, limit: 2,
    };
    let (recent, _) = held.spatial(&relation, &path, request(25), false).await.unwrap();
    assert_eq!(spatial_ids(&recent), [EntityId::Integer(1)]);
    let error = held.spatial(&relation, &path, request(5), false).await.err().unwrap();
    assert_eq!(error.code, "rtdb.spatial/invalid");
    drop(held);

    let mut limited = session.query_context().await.unwrap();
    let filtered = request(25);
    let prepared = rtdb_spatial::PreparedRegion::new(&filtered.region, filtered.predicate, &limited.memory).unwrap();
    let memory = limited.memory.scope_with_limit(4 << 20);
    Arc::make_mut(&mut limited).memory = memory;
    let unrestricted = Request { region: Default::default(), predicate: Predicate::All, ..request(5) };
    let read = rtdb_spatial::read_held(Arc::clone(&limited), "xtdb", &relation, &path, unrestricted,
        rtdb_spatial::Projection::Identities);
    let page = limited.reading(read).await.unwrap();
    assert_eq!(spatial_ids(&page), [EntityId::Integer(1)]);
    drop(page);

    let read = rtdb_spatial::read_prepared(Arc::clone(&limited), "xtdb", &relation, &path, &prepared, filtered,
        rtdb_spatial::Projection::Identities);
    let error = limited.reading(read).await.err().unwrap();
    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    let skipped = Request {
        page: PageOrder::Identity { after: Some(rtdb_columnar::EntityIdRef::Integer(1)) },
        ..request(25)
    };
    let read = rtdb_spatial::read_prepared(Arc::clone(&limited), "xtdb", &relation, &path, &prepared, skipped,
        rtdb_spatial::Projection::Identities);
    let page = limited.reading(read).await.unwrap();
    assert!(spatial_ids(&page).is_empty());

    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn spatial_path_backfills_original_values_across_live_writes() {
    use rtdb_spatial::{PageOrder, Predicate, Request};
    use rtdb_types::{EntityId, OwnedRelation, OwnedSchema, SpatialGeometryEncoding, StructFields};

    fn canonical_point(x: f64, y: f64) -> Vec<u8> {
        let mut bytes = b"\x0dcoordinates\0\x01\x0c".to_vec();
        for value in [x, y] {
            let bits = value.to_bits();
            let ordered = if bits < (1 << 63) { bits ^ (1 << 63) } else { !bits };
            bytes.push(5);
            bytes.extend(ordered.to_be_bytes());
        }
        bytes.extend(b"\0\0type\0\x01\x09Point\0\x01\0\0");
        bytes
    }

    let mut options = options();
    options.owned_relations = true;
    options.file_size_target = 1;
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let relation = OwnedRelation::new("original-places").unwrap();
    let path = OwnedRelation::new("by-location").unwrap();
    let schema = OwnedSchema::from_json(br#"{"fields":[{"name":"location","type":{"kind":"binary"},"optional":false,"nullable":false}]}"#).unwrap();
    let document = |id, x| rtdb_tx::Documents::new(vec![StructFields::from([
        ("_id".into(), Value::I64(id)), ("location".into(), Value::Binary(canonical_point(x, 0.0))),
    ])], node.memory()).unwrap();
    let opts = AutonomousTxOptions::default;
    let original = session.state_mut().execute_tx(vec![
        TxOp::OwnedDeclareRelation { relation: relation.clone(), schema },
        TxOp::OwnedInsertDocs {
            relation: relation.clone(), documents: document(1, -1.0),
            valid_from: Some(Timestamp(0)), valid_to: None,
        },
    ], opts()).await.unwrap();
    assert!(original.committed(), "{:?}", original.error());
    let db = node.catalog().database("xtdb").unwrap();
    db.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    db.compaction().unwrap().compact_all().await.unwrap();
    let before = session.open_owned_read().await.unwrap();

    let declaration = TxOp::OwnedDeclareEncodedSpatialPath {
        relation: relation.clone(), name: path.clone(), columns: vec!["location".into()],
        encoding: SpatialGeometryEncoding::BijectionGeometry,
    };
    let result = session.state_mut().execute_tx(vec![declaration], opts()).await.unwrap();
    assert!(result.committed(), "{:?}", result.error());
    assert!(before.spatial_path_ready(&relation, &path).await.is_err());
    let pending = session.open_owned_read().await.unwrap();
    assert!(!pending.spatial_path_ready(&relation, &path).await.unwrap());
    let concurrent = session.state_mut().execute_tx(vec![TxOp::OwnedInsertDocs {
        relation: relation.clone(), documents: document(2, -1.0),
        valid_from: Some(Timestamp(0)), valid_to: None,
    }], opts()).await.unwrap();
    assert!(concurrent.committed(), "{:?}", concurrent.error());
    db.build_artefacts().await.unwrap();

    let latest = session.open_owned_read().await.unwrap();
    assert!(latest.spatial_path_ready(&relation, &path).await.unwrap());
    let request = Request {
        partition: vec![], region: spatial_point(-1.0, 0.0).into(), predicate: Predicate::WithinDistance(25.0),
        valid_at: Timestamp(25), page: PageOrder::Identity { after: None }, limit: 8,
    };
    let (page, _) = latest.spatial_projected(&relation, &path, request, false, rtdb_spatial::Projection::Documents).await.unwrap();
    assert_eq!(spatial_ids(&page), [EntityId::Integer(1), EntityId::Integer(2)]);
    let documents = page.documents().unwrap().batch();
    let revisions = documents.column_by_name("_system_from").unwrap().as_any()
        .downcast_ref::<arrow_array::TimestampMicrosecondArray>().unwrap();
    assert_eq!([revisions.value(0), revisions.value(1)], [original.tx_key().system_time.0, concurrent.tx_key().system_time.0]);
    assert!(!pending.spatial_path_ready(&relation, &path).await.unwrap());
    drop((before, pending, latest));
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
        vec![Value::I64(id), Value::F64(rtdb_text::bm25(&scoring, &rtdb_text::Analyzer::Simple.tokens(body), None))]
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
    let artefacts: Vec<_> = store.list(Some(&rtdb_storage::ObjectKey::new("tables").unwrap())).await.unwrap().into_iter().filter(|object| object.key.as_str().contains("/search/")).collect();
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
    let artefacts = || async { store.list(Some(&rtdb_storage::ObjectKey::new("tables").unwrap())).await.unwrap().into_iter().map(|object| object.key).filter(|key| key.as_str().contains("/search/")).collect::<Vec<_>>() };
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
    assert!(database.trie_catalog().current_tries(&docs).iter().all(|trie| trie.artefacts.iter().all(|record| record.kind() != "search")), "and no trie records them");
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
    let score = rtdb_text::bm25(&rtdb_text::Query::parse("invoice"), &rtdb_text::Analyzer::Simple.tokens("invoice overdue"), None);
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
    let physical_iids: Vec<_> = rtdb_search::search_pages(lease.as_slice(), &columns, rtdb_text::Analyzer::Simple, &cancellation).unwrap()
        .flat_map(|page| page.unwrap().into_iter().map(|row| row.iid)).collect();
    assert_eq!(physical_iids.len(), entities.len());
    assert!(physical_iids.windows(2).any(|pair| pair[0] > pair[1]));
    drop(lease);

    let query = "SELECT _id FROM docs FOR VALID_TIME AS OF TIMESTAMP '1970-01-05T00:00:00Z' WHERE MATCH(body, 'invoice')";
    assert_eq!(ids(rows(session.query_sql(query, vec![]).await.unwrap()).await), (0..24).collect::<Vec<_>>());
    let scored = "SELECT _id, SEARCH_SCORE(body, 'invoice') AS score FROM docs FOR VALID_TIME AS OF TIMESTAMP '1970-01-05T00:00:00Z' WHERE MATCH(body, 'invoice') ORDER BY score DESC, _id LIMIT 1";
    let score = rtdb_text::bm25(&rtdb_text::Query::parse("invoice"), &rtdb_text::Analyzer::Simple.tokens("invoice"), None);
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
    let score = scorer.bm25(&rtdb_text::Analyzer::Simple.tokens("invoice paid invoice"));
    assert_eq!(first, [vec![Value::I64(1), Value::F64(score)]]);
    let terms = rows(session.query_sql(
        r#"SELECT SEARCH_TERMS('"Invoice paid" invoice')"#,
        vec![],
    ).await.unwrap()).await;
    assert_eq!(terms, [vec![Value::list(vec![Value::Utf8("invoice".into()), Value::Utf8("paid".into())])]]);
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
        ("MATCH(body, '\"red paid\"')", "red paid"),
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
struct Recording { inner: Arc<rtdb_search::SearchAccelerator>, sizes: std::sync::Mutex<Vec<usize>>, retained: std::sync::Mutex<Vec<usize>> }
#[async_trait]
impl rtdb_query::ScanAccelerator for Recording {
    async fn candidate_iids(&self, request: &rtdb_query::ScanRequest<'_>) -> Result<Option<std::collections::BTreeSet<rtdb_types::Iid>>> {
        self.inner.candidate_iids(request).await
    }

    async fn select_candidates(&self, request: &rtdb_query::ScanRequest<'_>) -> Result<Option<rtdb_query::ScanSelection>> {
        let selection = self.inner.select_candidates(request).await?;
        if let Some(selection) = &selection {
            self.sizes.lock().unwrap().push(selection.iids.len());
            let retained = selection.scores.as_ref().map_or(0, |scores| selection.iids.iter().filter(|iid| scores.row(*iid).is_some()).count());
            self.retained.lock().unwrap().push(retained);
        }
        Ok(selection)
    }
}

#[tokio::test]
async fn an_integer_tie_break_prunes_persisted_hits_and_accounts_for_live_replacements() {
    verify_ranked_integer_ties(false).await;
}

#[tokio::test]
async fn the_arrow_tree_resolves_and_reuses_winning_rows_after_live_replacements() {
    verify_ranked_integer_ties(true).await;
}

async fn verify_ranked_integer_ties(tree: bool) {
    let mut settings = options();
    settings.file_size_target = 1;
    settings.automatic_artefacts = false;
    let runtime = if tree { rtdb_search::SearchRuntime::with_tree_files(64 << 20) }
        else { rtdb_search::SearchRuntime::new(64 << 20) };
    let mut accelerator = rtdb_search::SearchAccelerator::with_runtime(Arc::clone(&runtime));
    accelerator.max_candidates = 2400;
    let recording = Arc::new(Recording { inner: Arc::new(accelerator), sizes: Default::default(), retained: Default::default() });
    let services = NodeServices { scan_accelerators: vec![recording.clone()], artefact_kinds: vec![runtime.artefacts()], ..services() };
    let (node, driver) = Node::open(settings, services).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    let table = TableRef::new("public", "docs");
    let transactions = [-2048..0, 0..2048].into_iter().map(|ids| {
        let rows = ids.map(|id| rtdb_types::StructFields::from([
            ("_id".into(), Value::I64(id)),
            ("body".into(), Value::Utf8(if id % 2 == 0 { "invoice audit audit" } else { "invoice about a library book audit" }.into())),
        ])).collect();
        TxOp::PutDocs { table: table.clone(), documents: rtdb_tx::Documents::new(rows, node.memory()).unwrap(),
            valid_from: Some(Timestamp(0)), valid_to: None }
    });
    let statements = std::iter::once(sql("CREATE SEARCH INDEX docs_body ON docs (body)")).chain(transactions).collect();
    let result = session.state_mut().execute_tx(statements, AutonomousTxOptions::default()).await.unwrap();
    assert!(result.committed(), "{:?}", result.error());
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    database.build_artefacts().await.unwrap();

    let query = "SELECT _id, body, SEARCH_SCORE(body, 'invoice') AS score FROM docs WHERE MATCH(body, 'invoice') ORDER BY score DESC, _id LIMIT 3";
    let oracle = "SELECT _id, body, SEARCH_SCORE(body, 'invoice') AS score FROM docs WHERE CASE WHEN MATCH(body, 'invoice') THEN TRUE ELSE FALSE END ORDER BY score DESC, _id LIMIT 3";
    for expected in [[-2048, -2047, -2046], [-3000, -2047, -2046]] {
        recording.sizes.lock().unwrap().clear();
        recording.retained.lock().unwrap().clear();
        let got = rows(session.query_sql(query, vec![]).await.unwrap()).await;
        assert_eq!(got, rows(session.query_sql(oracle, vec![]).await.unwrap()).await);
        assert_eq!(ids(got), expected);
        assert_eq!(*recording.sizes.lock().unwrap(), [3]);
        if tree { assert_eq!(*recording.retained.lock().unwrap(), [3]); }
        recording.sizes.lock().unwrap().clear();
        recording.retained.lock().unwrap().clear();
        let dense_order = |sql: &str| sql.replace("SEARCH_SCORE(body, 'invoice')", "SEARCH_SCORE(body, 'audit')")
            .replace("ORDER BY score DESC, _id", "ORDER BY score DESC, _id, _system_from");
        let dense = dense_order(query);
        let dense_oracle = dense_order(oracle);
        let got = rows(session.query_sql(&dense, vec![]).await.unwrap()).await;
        assert_eq!(got, rows(session.query_sql(&dense_oracle, vec![]).await.unwrap()).await);
        let expected = if expected[0] == -2048 { [-2048, -2046, -2044] } else { [-2046, -2044, -2042] };
        assert_eq!(ids(got), expected);
        assert_eq!(*recording.sizes.lock().unwrap(), [3]);
        if tree { assert_eq!(*recording.retained.lock().unwrap(), [3]); }
        if expected[0] == -2048 {
            let changed = session.state_mut().execute_tx(vec![
                sql("INSERT INTO docs RECORDS {_id: -2048, body: 'paid'}"),
                sql("INSERT INTO docs RECORDS {_id: -3000, body: 'invoice'}"),
            ], AutonomousTxOptions::default()).await.unwrap();
            assert!(changed.committed());
        }
    }
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn corruption_in_a_scoring_only_page_falls_back_and_queues_repair() {
    let mut settings = options();
    settings.file_size_target = 1;
    settings.automatic_artefacts = false;
    let runtime = rtdb_search::SearchRuntime::new(16 << 20);
    let services = NodeServices { scan_accelerators: vec![runtime.accelerator()], artefact_kinds: vec![runtime.artefacts()], ..services() };
    let (node, driver) = Node::open(settings, services).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    let body = (0..96).map(|term| format!("token{term:03}")).collect::<Vec<_>>().join(" ");
    let created = session.state_mut().execute_tx(vec![
        sql("CREATE SEARCH INDEX docs_body ON docs (body)"),
        sql(&format!("INSERT INTO docs RECORDS {{_id: 1, body: '{body}'}}")),
    ], AutonomousTxOptions::default()).await.unwrap();
    assert!(created.committed(), "{:?}", created.error());
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    database.build_artefacts().await.unwrap();
    let snapshot = database.snapshot();
    let table = TableRef::new("public", "docs");
    let slug = &snapshot.table_entry(&table).unwrap().slug;
    let (trie, record) = snapshot.trie_catalog().current_tries(&table).into_iter().find_map(|trie| {
        trie.artefacts.iter().find(|record| record.definition_id().ends_with("f13")).map(|record| (trie, record))
    }).unwrap();
    let key = rtdb_storage::ObjectKey::new(slug.recorded_artefact_key(&trie.key, record).unwrap()).unwrap();
    let store = database.object_store();
    let original = store.get(&key).await.unwrap();
    let trailer = original.len() - 10;
    let footer_len = arrow_ipc::reader::read_footer_length(original[trailer..].try_into().unwrap()).unwrap();
    let footer = arrow_ipc::root_as_footer(&original[trailer - footer_len..trailer]).unwrap();
    let block = footer.recordBatches().unwrap().get(6);
    let mut corrupted = original.to_vec();
    corrupted[(block.offset() + i64::from(block.metaDataLength())) as usize] ^= 1;
    store.delete(&key).await.unwrap();
    store.put(&key, corrupted.into()).await.unwrap();

    let statement = "SELECT _id, SEARCH_SCORE(body, 'token095') AS score FROM docs WHERE MATCH(body, 'token000') ORDER BY score DESC, _id LIMIT 1";
    assert_eq!(ids(rows(session.query_sql(statement, vec![]).await.unwrap()).await), [1]);
    assert_eq!(runtime.suspects("xtdb").len(), 1);
    database.build_artefacts().await.unwrap();
    assert!(runtime.suspects("xtdb").is_empty());
    assert_eq!(store.get(&key).await.unwrap(), original);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn compatible_writers_keep_both_search_formats_through_restart() {
    use rtdb_format::database::proto::{DatabaseConfig, LocalLog, LocalStorage, database_config};

    let directory = tempfile::tempdir().unwrap();
    let path = |name: &str| Some(directory.path().join(name).to_string_lossy().into_owned());
    let mut settings = options();
    settings.file_size_target = 1;
    settings.automatic_artefacts = false;
    settings.membership = Membership::Durable {
        primary: DatabaseConfig {
            log: Some(database_config::Log::LocalLog(LocalLog { path: path("log"), epoch: Some(0) })),
            storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: path("store"), epoch: Some(0) })),
            ..Default::default()
        },
        skipped: Default::default(),
    };
    let template = services();
    let table = TableRef::new("public", "docs");
    let mut published = std::collections::BTreeSet::new();
    for (phase, format) in [11, 14, 12, 13, 15, 11].into_iter().enumerate() {
        let runtime = match format {
            11 => rtdb_search::SearchRuntime::with_tantivy_files(16 << 20),
            14 => rtdb_search::SearchRuntime::with_tree_files(16 << 20),
            15 => rtdb_search::SearchRuntime::with_search_packs(16 << 20),
            _ => rtdb_search::SearchRuntime::with_paged_files(16 << 20, format == 13),
        };
        let services = NodeServices {
            resources: Arc::new(NativeResources::default().with_local_log_clock(Arc::clone(&template.clock), false)),
            artefact_kinds: vec![runtime.artefacts()], scan_accelerators: vec![runtime.accelerator()],
            ..template.clone()
        };
        let (node, driver) = Node::open(settings.clone(), services).await.unwrap();
        let driver = tokio::spawn(driver.run());
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        let database = node.catalog().database("xtdb").unwrap();
        let cancellation = tokio_util::sync::CancellationToken::new();
        node.sync_all(Duration::from_secs(10), &cancellation).await.unwrap();
        if phase == 0 {
            let result = session.state_mut().execute_tx(vec![
                sql("CREATE SEARCH INDEX docs_body ON docs (body)"),
                sql("INSERT INTO docs RECORDS {_id: 1, body: 'invoice'}, {_id: -2, body: 'overdue invoice'}"),
            ], AutonomousTxOptions::default()).await.unwrap();
            assert!(result.committed(), "{:?}", result.error());
            database.flush().await.unwrap();
            node.sync_all(Duration::from_secs(10), &cancellation).await.unwrap();
            database.compaction().unwrap().compact_all().await.unwrap();
        }
        database.build_artefacts().await.unwrap();
        published.insert(match format { 11 => "f11", 12 => "f12", 13 => "f13", 14 => "f14", _ => "f15" });
        let catalog = database.trie_catalog();
        let seen: std::collections::BTreeSet<_> = catalog.current_tries(&table).into_iter()
            .flat_map(|trie| trie.artefacts.iter())
            .filter(|record| record.kind() == "search")
            .map(|record| &record.definition_id()[record.definition_id().len() - 3..]).collect();
        assert_eq!(seen, published, "phase {phase}");
        let legacy = if format == 14 {
            let trie = catalog.current_tries(&table).into_iter().find(|trie| {
                trie.artefacts.iter().any(|record| record.definition_id().ends_with("f11"))
                    && trie.artefacts.iter().any(|record| record.definition_id().ends_with("f14"))
            }).unwrap();
            let record = trie.artefacts.iter().find(|record| record.definition_id().ends_with("f11")).unwrap();
            let snapshot = database.snapshot();
            let slug = &snapshot.table_entry(&table).unwrap().slug;
            let key = rtdb_storage::ObjectKey::new(slug.recorded_artefact_key(&trie.key, record).unwrap()).unwrap();
            let store = database.object_store();
            let original = store.get(&key).await.unwrap();
            store.delete(&key).await.unwrap();
            Some((store, key, original))
        } else { None };
        let statement = "SELECT _id FROM docs WHERE MATCH(body, 'invoice') ORDER BY _id";
        assert_eq!(ids(rows(session.query_sql(statement, vec![]).await.unwrap()).await), [-2, 1]);
        assert!(runtime.suspects("xtdb").is_empty());
        if let Some((store, key, original)) = legacy { store.put(&key, original).await.unwrap(); }
        let sql = "SELECT _id, SEARCH_SCORE(body, 'invoice') AS score FROM docs WHERE MATCH(body, 'invoice') ORDER BY score DESC, _id LIMIT 1";
        assert_eq!(ids(rows(session.query_sql(sql, vec![]).await.unwrap()).await), [-2]);
        drop(session);
        node.shutdown();
        tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
    }
}

#[test]
fn a_ranked_snapshot_combines_a_tree_and_an_older_search_format() {
    std::thread::Builder::new()
        .name("mixed-format search fixture".into())
        .stack_size(rtdb_node::WORKER_STACK_BYTES)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(verify_ranked_snapshot_with_mixed_search_formats());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn verify_ranked_snapshot_with_mixed_search_formats() {
    use rtdb_index::{LiveIndex, Snapshot};
    use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext};

    let mut settings = options();
    settings.file_size_target = 1;
    settings.automatic_artefacts = false;
    let runtime = rtdb_search::SearchRuntime::with_tree_files(64 << 20);
    let (node, driver) = Node::open(settings, NodeServices {
        artefact_kinds: vec![runtime.artefacts()], scan_accelerators: vec![runtime.accelerator()], ..services()
    }).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    let table = TableRef::new("public", "docs");
    let cancellation = tokio_util::sync::CancellationToken::new();
    let mut older = std::collections::BTreeSet::new();
    for (phase, statement) in [
        "INSERT INTO docs RECORDS {_id: -2, body: 'invoice invoice invoice'}, {_id: 1, body: 'invoice'}",
        "INSERT INTO docs RECORDS {_id: -2, body: 'paid'}, {_id: -1, body: 'invoice invoice'}",
    ].into_iter().enumerate() {
        let mut operations = vec![sql(statement)];
        if phase == 0 { operations.insert(0, sql("CREATE SEARCH INDEX docs_body ON docs (body)")); }
        assert!(session.state_mut().execute_tx(operations, AutonomousTxOptions::default()).await.unwrap().committed());
        database.flush().await.unwrap();
        node.sync_all(Duration::from_secs(10), &cancellation).await.unwrap();
        database.compaction().unwrap().compact_all().await.unwrap();
        database.build_artefacts().await.unwrap();
        if phase == 0 {
            older.extend(database.trie_catalog().current_tries(&table).into_iter().map(|trie| trie.key.clone()));
        }
    }
    let pinned = database.snapshot();
    let catalog = pinned.trie_catalog();
    let earlier = catalog.current_tries(&table).into_iter().find(|trie| older.contains(&trie.key)).unwrap();
    let record = earlier.artefacts.iter().find(|record| record.definition_id().ends_with("f14")).unwrap();
    let catalog = catalog.without_artefacts(&table, [(earlier.key.clone(), record.clone())]);
    assert!(catalog.current_tries(&table).into_iter().any(|trie| trie.artefacts.iter().any(|record| record.definition_id().ends_with("f14"))));
    let mixed = Snapshot::new(Arc::new(catalog), Arc::new(pinned.table_catalog().clone()),
        Arc::new(LiveIndex::new(pinned.block_index(), pinned.tx_basis(), Default::default())));
    let input = CatalogSnapshot::new("xtdb", std::collections::BTreeMap::from([("xtdb".into(), DatabaseSnapshot {
        partitions: vec![mixed], storage: Arc::clone(database.storage()),
    })]), false).unwrap();
    let memory = Arc::clone(node.memory());
    let recording = Arc::new(Recording { inner: runtime.accelerator(), sizes: Default::default(), retained: Default::default() });
    let context = QueryContext::new(input, Arc::clone(&memory), Timestamp(i64::MAX / 2), "UTC", Default::default(), &cancellation).unwrap()
        .with_accelerators(Arc::from([recording.clone() as Arc<dyn rtdb_query::ScanAccelerator>]));
    let statement = "SELECT _id, body, SEARCH_SCORE(body, 'invoice') AS score FROM docs WHERE MATCH(body, 'invoice') ORDER BY score DESC, _id LIMIT 2";
    let selected = std::thread::Builder::new()
        .name("mixed-format ranked query".into())
        .stack_size(2 << 20)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async move {
                let prepared = rtdb_sql::prepare_in(statement, &context.catalog, &Default::default(), &memory).unwrap();
                let cursor = PreparedQuery::from_prepared(prepared, &memory).unwrap().execute(context, vec![]).await.unwrap();
                rows(cursor).await
            })
        })
        .unwrap()
        .join()
        .unwrap();
    assert_eq!(ids(selected.clone()), [-1, 1]);
    assert_eq!(selected, rows(session.query_sql(statement, vec![]).await.unwrap()).await);
    assert_eq!(*recording.retained.lock().unwrap(), [2]);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn a_uuid_tie_break_prunes_tied_posting_windows_and_preserves_the_sql_page() {
    let mut options = options();
    options.file_size_target = 1;
    options.automatic_artefacts = false;
    let runtime = rtdb_search::SearchRuntime::with_tree_files(64 << 20);
    let services = NodeServices { artefact_kinds: vec![runtime.artefacts()], ..services() };
    let (node, driver) = Node::open(options, services).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    assert!(session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_body ON uuid_docs (body)")], AutonomousTxOptions::default()).await.unwrap().committed());
    let operations = (0..900).rev().map(|id| {
        sql(&format!("INSERT INTO uuid_docs RECORDS {{_id: UUID '00000000-0000-0000-0000-{id:012x}', body: 'invoice'}}"))
    }).collect();
    assert!(session.state_mut().execute_tx(operations, AutonomousTxOptions::default()).await.unwrap().committed());
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    database.build_artefacts().await.unwrap();
    let query = "SELECT _id, SEARCH_SCORE(body, 'invoice') AS score FROM uuid_docs FOR VALID_TIME AS OF TIMESTAMP '1970-01-02Z' FOR SYSTEM_TIME AS OF TIMESTAMP '1970-01-02Z' WHERE MATCH(body, 'invoice') ORDER BY score DESC, _id";
    let everything = rows(session.query_sql(query, vec![]).await.unwrap()).await;
    assert_eq!(everything.len(), 900);

    let memory = rtdb_columnar::MemoryBudget::new(1 << 30);
    let mut accelerator = rtdb_search::SearchAccelerator::with_runtime(runtime);
    accelerator.max_candidates = 700;
    let recording = Arc::new(Recording { inner: Arc::new(accelerator), sizes: Default::default(), retained: Default::default() });
    let catalog = rtdb_query::CatalogSnapshot::new("xtdb", std::collections::BTreeMap::from([("xtdb".into(), rtdb_query::DatabaseSnapshot {
        partitions: vec![database.snapshot()], storage: Arc::clone(database.storage()),
    })]), false).unwrap();
    let context = rtdb_query::QueryContext::new(catalog, Arc::clone(&memory), Timestamp(i64::MAX / 2), "UTC",
        rtdb_query::QueryOptions::default(), &tokio_util::sync::CancellationToken::new()).unwrap()
        .with_accelerators(Arc::from([Arc::clone(&recording) as Arc<dyn rtdb_query::ScanAccelerator>]));
    for (bounds, parameters) in [
        ("LIMIT 3 OFFSET 4", vec![]),
        ("LIMIT $1 OFFSET $2", vec![Value::I64(3), Value::I64(4)]),
    ] {
        let page = format!("{query} {bounds}");
        let prepared = rtdb_sql::prepare_in(&page, &context.catalog, &rtdb_sql::PrepareOptions::default(), &memory).unwrap();
        let cursor = rtdb_query::PreparedQuery::from_prepared(prepared, &memory).unwrap().execute(Arc::clone(&context), parameters).await.unwrap();
        assert_eq!(rows(cursor).await, everything[4..7]);
    }
    assert_eq!(*recording.sizes.lock().unwrap(), [7, 7]);
    assert_eq!(*recording.retained.lock().unwrap(), [7, 7]);

    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

struct FrontierProbe {
    search: Arc<rtdb_search::SearchAccelerator>,
    opened: AtomicUsize,
    fallback: AtomicUsize,
}

#[async_trait]
impl rtdb_query::ScanAccelerator for FrontierProbe {
    async fn candidate_iids(&self, request: &rtdb_query::ScanRequest<'_>) -> Result<Option<std::collections::BTreeSet<rtdb_types::Iid>>> {
        if request.scan.top_k.is_some() { self.fallback.fetch_add(1, Ordering::Relaxed); }
        self.search.candidate_iids(request).await
    }

    async fn rank_source<'a>(&'a self, request: rtdb_query::ScanRequest<'a>) -> Result<Option<Box<dyn rtdb_query::RankSource + 'a>>> {
        let source = self.search.rank_source(request).await?;
        if source.is_some() { self.opened.fetch_add(1, Ordering::Relaxed); }
        Ok(source)
    }
}

async fn partition_search(
    partitions: &[rtdb_index::Snapshot], storage: &Arc<rtdb_storage::cache::BufferPool>,
    sql: &str, accelerator: Option<Arc<dyn rtdb_query::ScanAccelerator>>,
) -> Vec<Vec<Value>> {
    let memory = rtdb_columnar::MemoryBudget::new(1 << 30);
    let catalog = rtdb_query::CatalogSnapshot::new("xtdb", std::collections::BTreeMap::from([("xtdb".into(),
        rtdb_query::DatabaseSnapshot { partitions: partitions.to_vec(), storage: Arc::clone(storage) },
    )]), false).unwrap();
    let context = rtdb_query::QueryContext::new(catalog, Arc::clone(&memory), Timestamp(i64::MAX / 2), "UTC",
        rtdb_query::QueryOptions::default(), &tokio_util::sync::CancellationToken::new()).unwrap();
    let context = match accelerator {
        Some(accelerator) => context.with_accelerators(Arc::from([accelerator])),
        None => context,
    };
    let prepared = rtdb_sql::prepare_in(sql, &context.catalog, &rtdb_sql::PrepareOptions::default(), &memory).unwrap();
    let cursor = rtdb_query::PreparedQuery::from_prepared(prepared, &memory).unwrap().execute(context, Vec::new()).await.unwrap();
    rows(cursor).await
}

#[tokio::test]
async fn randomized_ranked_windows_over_partitions_match_exhaustive_sql() {
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
    for round in 0..4 {
        let size = 60 + next() % 120;
        let operations = (0..size).map(|id| {
            let body: Vec<_> = (0..1 + next() % 8).map(|_| words[next() % words.len()]).collect();
            sql(&format!("INSERT INTO docs RECORDS {{_id: {}, body: '{}', bucket: {}, payload: 'doc-{round}-{id}'}}",
                id + 1000 * round, body.join(" "), next() % 4))
        }).collect();
        assert!(session.state_mut().execute_tx(operations, AutonomousTxOptions::default()).await.unwrap().committed());
        database.flush().await.unwrap();
        node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
        database.compaction().unwrap().compact_all().await.unwrap();
        database.build_artefacts().await.unwrap();
        partitions.push(database.snapshot());
    }
    let frontier = Arc::new(FrontierProbe {
        search: rtdb_search::SearchRuntime::new(64 << 20).accelerator(), opened: Default::default(), fallback: Default::default(),
    });
    for (matching, scoring) in [("invoice", "invoice overdue"), ("overdue late", "late"), ("book", "book red paid")] {
        let query = format!("SELECT _id, payload, SEARCH_SCORE(body, '{scoring}') AS score FROM docs \
            WHERE MATCH(body, '{matching}') AND bucket <> 3 ORDER BY score DESC, _id");
        let everything = partition_search(&partitions, database.storage(), &query, None).await;
        assert!(everything.len() > 20, "{query} matched only {}", everything.len());
        for (limit, offset) in [(1, 0), (5, 3), (20, 17)] {
            let statement = format!("{query} LIMIT {limit} OFFSET {offset}");
            let ranked = partition_search(&partitions, database.storage(), &statement, Some(frontier.clone())).await;
            assert_eq!(ranked, everything.iter().skip(offset).take(limit).cloned().collect::<Vec<_>>(), "{statement}");
            assert_eq!(frontier.opened.swap(0, Ordering::Relaxed), partitions.len(), "{statement}");
            assert_eq!(frontier.fallback.load(Ordering::Relaxed), 0, "{statement}");
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
        assert_eq!(row[2], Value::F64(rtdb_text::bm25(&scoring, &rtdb_text::Analyzer::Simple.tokens(body), None)), "{row:?}");
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
        Value::F64(rtdb_text::bm25(&scoring, &rtdb_text::Analyzer::Simple.tokens("visible beta beta"), None)),
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

#[tokio::test]
async fn packed_search_preserves_rank_across_commit_flush_correction_and_restart() {
    use rtdb_format::database::proto::{DatabaseConfig, LocalLog, LocalStorage, database_config};
    use tokio_util::sync::CancellationToken;

    let directory = tempfile::tempdir().unwrap();
    let path = |name: &str| Some(directory.path().join(name).to_string_lossy().into_owned());
    let mut settings = options();
    settings.automatic_artefacts = false;
    settings.membership = Membership::Durable {
        primary: DatabaseConfig {
            log: Some(database_config::Log::LocalLog(LocalLog { path: path("log"), epoch: Some(0) })),
            storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: path("store"), epoch: Some(0) })),
            ..Default::default()
        },
        skipped: Default::default(),
    };
    let template = services();
    let statement = "SELECT _id, body, SEARCH_SCORE(body, 'report') AS score FROM docs WHERE MATCH(body, '\"audit report\"') ORDER BY score DESC, _id ASC LIMIT 1";
    let exhaustive = "SELECT _id, body, SEARCH_SCORE(body, 'report') AS score FROM docs WHERE MATCH(body, '\"audit report\"') OR _id = -99 ORDER BY score DESC, _id ASC LIMIT 1";
    let table = TableRef::new("public", "docs");
    let stop = CancellationToken::new();
    for restart in [false, true] {
        let runtime = rtdb_search::SearchRuntime::with_search_packs(16 << 20);
        let services = NodeServices {
            resources: Arc::new(NativeResources::default().with_local_log_clock(Arc::clone(&template.clock), false)),
            artefact_kinds: vec![runtime.artefacts()],
            scan_accelerators: vec![runtime.accelerator()],
            ..template.clone()
        };
        let (node, driver) = Node::open(settings.clone(), services).await.unwrap();
        let driver = tokio::spawn(driver.run());
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        let database = node.catalog().database("xtdb").unwrap();
        node.sync_all(Duration::from_secs(10), &stop).await.unwrap();
        if !restart {
            for statement in [
                "CREATE SEARCH INDEX docs_body ON docs (body)",
                "INSERT INTO docs RECORDS {_id: 1, body: 'audit report report report'}, {_id: 2, body: 'audit report'}",
            ] {
                assert!(session.state_mut().execute_tx(vec![sql(statement)], AutonomousTxOptions::default()).await.unwrap().committed());
            }
            let before = rows(session.query_sql(exhaustive, vec![]).await.unwrap()).await;
            assert_eq!(before[0][0], Value::I64(1));
            assert_eq!(rows(session.query_sql(statement, vec![]).await.unwrap()).await, before);
            let pinned = session.query_sql(statement, vec![]).await.unwrap();
            database.flush().await.unwrap();
            node.sync_all(Duration::from_secs(10), &stop).await.unwrap();
            assert_eq!(rows(session.query_sql(statement, vec![]).await.unwrap()).await, before);
            assert!(session.state_mut().execute_tx(
                vec![sql("INSERT INTO docs RECORDS {_id: 1, body: 'report no longer under audit'}")],
                AutonomousTxOptions::default(),
            ).await.unwrap().committed());
            assert_eq!(rows(pinned).await, before);
            let corrected = rows(session.query_sql(statement, vec![]).await.unwrap()).await;
            assert_eq!(corrected[0][0], Value::I64(2));
            database.flush().await.unwrap();
            node.sync_all(Duration::from_secs(10), &stop).await.unwrap();
        }
        let catalog = database.trie_catalog();
        assert_eq!(catalog.current_tries(&table).into_iter().filter(|trie| trie.key.level() == 0 &&
            trie.artefacts.iter().any(|record| record.definition_id().ends_with("f15"))).count(), 2);
        let expected = rows(session.query_sql(exhaustive, vec![]).await.unwrap()).await;
        assert_eq!(expected[0][0], Value::I64(2));
        assert_eq!(rows(session.query_sql(statement, vec![]).await.unwrap()).await, expected);
        node.shutdown();
        tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
    }
}

#[tokio::test]
async fn logical_postings_survive_compaction_while_pinned_history_keeps_its_payload() {
    use rtdb_format::database::proto::{DatabaseConfig, LocalLog, LocalStorage, database_config};
    use tokio_util::sync::CancellationToken;

    let directory = tempfile::tempdir().unwrap();
    let path = |name: &str| Some(directory.path().join(name).to_string_lossy().into_owned());
    let mut settings = options();
    settings.automatic_artefacts = false;
    settings.automatic_gc = false;
    settings.membership = Membership::Durable {
        primary: DatabaseConfig {
            log: Some(database_config::Log::LocalLog(LocalLog { path: path("log"), epoch: Some(0) })),
            storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: path("store"), epoch: Some(0) })),
            ..Default::default()
        },
        skipped: Default::default(),
    };
    let template = services();
    let ranked = "SELECT _id, body, SEARCH_SCORE(body, 'report') AS score FROM docs WHERE MATCH(body, '\"audit report\"') ORDER BY score DESC, _id ASC LIMIT 1";
    let oracle = "SELECT _id, body, SEARCH_SCORE(body, 'report') AS score FROM docs WHERE MATCH(body, '\"audit report\"') OR _id = -99 ORDER BY score DESC, _id ASC LIMIT 1";
    let point = "SELECT body FROM docs WHERE _id = 1";
    let table = TableRef::new("public", "docs");
    let stop = CancellationToken::new();
    for restart in [false, true] {
        let runtime = rtdb_search::SearchRuntime::with_logical_search(64 << 20);
        let services = NodeServices {
            resources: Arc::new(NativeResources::default().with_local_log_clock(Arc::clone(&template.clock), false)),
            artefact_kinds: vec![runtime.artefacts()],
            scan_accelerators: vec![runtime.accelerator()],
            ..template.clone()
        };
        let (node, driver) = Node::open(settings.clone(), services).await.unwrap();
        let driver = tokio::spawn(driver.run());
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        let database = node.catalog().database("xtdb").unwrap();
        node.sync_all(Duration::from_secs(10), &stop).await.unwrap();
        if !restart {
            for statement in [
                "CREATE SEARCH INDEX docs_body ON docs (body)",
                "INSERT INTO plain_docs RECORDS {_id: 7, body: 'ordinary entity point read'}",
                "INSERT INTO docs RECORDS {_id: 1, body: 'audit report report report', marks: 1}, {_id: 2, body: 'audit report'}, {_id: 3, body: 42}",
            ] {
                assert!(session.state_mut().execute_tx(vec![sql(statement)], AutonomousTxOptions::default()).await.unwrap().committed());
            }
            let before = rows(session.query_sql(oracle, vec![]).await.unwrap()).await;
            assert_eq!(before[0][0], Value::I64(1));
            assert_eq!(rows(session.query_sql(ranked, vec![]).await.unwrap()).await, before);
            database.flush().await.unwrap();
            node.sync_all(Duration::from_secs(10), &stop).await.unwrap();
            let pinned = session.query_sql(ranked, vec![]).await.unwrap();
            let old_catalog = database.trie_catalog();
            assert_eq!(old_catalog.search_runs(&table).count(), 1);
            let plain = TableRef::new("public", "plain_docs");
            let plain_tries = old_catalog.current_tries(&plain);
            assert!(!plain_tries.is_empty());
            assert!(plain_tries.iter().all(|trie| trie.artefact(rtdb_format::entity_history::KIND,
                rtdb_format::entity_history::ID, rtdb_format::entity_history::VERSION).is_some()));
            let plain_snapshot = database.snapshot();
            let plain_entry = plain_snapshot.table_entry(&plain).unwrap();
            let primary_meta = rtdb_storage::ObjectKey::new(plain_entry.slug.metadata_key(&plain_tries[0].key)).unwrap();
            let store = database.object_store();
            let metadata = store.get(&primary_meta).await.unwrap();
            database.storage().invalidate(&primary_meta).await.unwrap();
            store.delete(&primary_meta).await.unwrap();
            assert_eq!(rows(session.query_sql("SELECT body FROM plain_docs WHERE _id = 7", vec![]).await.unwrap()).await,
                [vec![Value::Utf8("ordinary entity point read".into())]]);
            store.put(&primary_meta, metadata).await.unwrap();
            for statement in [
                "INSERT INTO docs RECORDS {_id: 1, body: 'AUDIT—report, report REPORT', marks: 2}",
                "INSERT INTO docs RECORDS {_id: 1, body: 'corrected text', marks: 3}",
            ] {
                assert!(session.state_mut().execute_tx(vec![sql(statement)], AutonomousTxOptions::default()).await.unwrap().committed());
            }
            database.flush().await.unwrap();
            node.sync_all(Duration::from_secs(10), &stop).await.unwrap();
            assert_eq!(database.trie_catalog().search_runs(&table).count(), 2);
            database.build_artefacts().await.unwrap();
            let merged = database.trie_catalog();
            assert_eq!(merged.search_runs(&table).count(), 1);
            assert_eq!(merged.retired_search_runs(&table).count(), 2);
            assert_eq!(merged.search_runs(&table).next().unwrap().blocks.len(), 2);
            database.compaction().unwrap().compact_all().await.unwrap();
            node.sync_all(Duration::from_secs(10), &stop).await.unwrap();
            database.build_artefacts().await.unwrap();
            assert!(database.trie_catalog().search_runs(&table).eq(merged.search_runs(&table)));
            assert_eq!(rows(pinned).await, before);
            database.flush().await.unwrap();
            node.sync_all(Duration::from_secs(10), &stop).await.unwrap();
        }
        let catalog = database.trie_catalog();
        assert_eq!(catalog.search_runs(&table).count(), 1);
        let merged = catalog.search_runs(&table).next().unwrap();
        assert_eq!(catalog.term_directory(&table, merged.definition_id(), merged.definition_version()).unwrap().runs(), 1);
        let expected = rows(session.query_sql(oracle, vec![]).await.unwrap()).await;
        assert_eq!(expected[0][0], Value::I64(2));
        let reads = runtime.logical_run_reads();
        assert_eq!(rows(session.query_sql(ranked, vec![]).await.unwrap()).await, expected);
        assert!(runtime.logical_run_reads() > reads, "ranking must consume logical runs after primary compaction and recovery");
        assert_eq!(rows(session.query_sql(point, vec![]).await.unwrap()).await, vec![vec![Value::Utf8("corrected text".into())]]);
        assert_eq!(rows(session.query_sql("SELECT _id FROM docs WHERE MATCH(body, 'audit')", vec![]).await.unwrap()).await,
            vec![vec![Value::I64(2)]]);
        if restart {
            let before_erase = database.trie_catalog();
            assert!(session.state_mut().execute_tx(vec![sql("ERASE FROM docs WHERE _id = 2")],
                AutonomousTxOptions::default()).await.unwrap().committed());
            database.flush().await.unwrap();
            node.sync_all(Duration::from_secs(10), &stop).await.unwrap();
            database.build_artefacts().await.unwrap();
            let erased = database.trie_catalog();
            assert!(erased.search_runs(&table).any(|run| run.has_erases()));
            assert!(erased.retired_search_runs(&table).count() > before_erase.retired_search_runs(&table).count());
            assert!(rows(session.query_sql(ranked, vec![]).await.unwrap()).await.is_empty());
            assert!(session.state_mut().execute_tx(vec![sql("INSERT INTO docs RECORDS {_id: 2, body: 'audit report'}")],
                AutonomousTxOptions::default()).await.unwrap().committed());
            assert_eq!(rows(session.query_sql(ranked, vec![]).await.unwrap()).await, expected);
        }
        node.shutdown();
        tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
    }
}

#[tokio::test]
async fn native_search_rebuilds_from_the_retained_journal_after_loss_of_postings_and_source_tries() {
    use rtdb_format::database::proto::{DatabaseConfig, LocalLog, LocalStorage, database_config};
    use rtdb_storage::ObjectKey;
    use rtdb_types::BlockIndex;
    use tokio_util::sync::CancellationToken;

    let directory = tempfile::tempdir().unwrap();
    let path = |name: &str| Some(directory.path().join(name).to_string_lossy().into_owned());
    let mut settings = options();
    settings.automatic_artefacts = false;
    settings.membership = Membership::Durable {
        primary: DatabaseConfig {
            log: Some(database_config::Log::LocalLog(LocalLog { path: path("log"), epoch: Some(0) })),
            storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: path("store"), epoch: Some(0) })),
            ..Default::default()
        },
        skipped: Default::default(),
    };
    let runtime = rtdb_search::SearchRuntime::with_logical_search(64 << 20);
    let services = NodeServices { artefact_kinds: vec![runtime.artefacts()], scan_accelerators: vec![runtime.accelerator()], ..services() };
    let (node, driver) = Node::open(settings, services).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    let stop = CancellationToken::new();
    assert!(session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")],
        AutonomousTxOptions::default()).await.unwrap().committed());
    for statement in [
        "INSERT INTO docs RECORDS {_id: 1, body: 'audit report report'}",
        "INSERT INTO docs RECORDS {_id: 2, body: 'audit report'}",
    ] {
        assert!(session.state_mut().execute_tx(vec![sql(statement)], AutonomousTxOptions::default()).await.unwrap().committed());
        database.flush().await.unwrap();
        node.sync_all(Duration::from_secs(10), &stop).await.unwrap();
    }
    let table = TableRef::new("public", "docs");
    assert_eq!(database.trie_catalog().search_runs(&table).flat_map(|run| run.blocks()).count(), 2);
    database.build_artefacts().await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    node.sync_all(Duration::from_secs(10), &stop).await.unwrap();
    let snapshot = database.snapshot();
    let entry = snapshot.table_entry(&table).unwrap();
    let removed = snapshot.trie_catalog().l0_source(&table, BlockIndex(-1), BlockIndex(i64::MAX))
        .map(|trie| {
            assert!(matches!(trie.state, rtdb_index::TrieState::Garbage { .. }));
            trie.key.clone()
        }).collect::<Vec<_>>();
    assert!(!removed.is_empty());
    let runs = snapshot.trie_catalog().search_runs(&table).collect::<Vec<_>>();
    assert!(!runs.is_empty());
    for trie in &removed {
        assert!(runs.iter().any(|run| run.blocks().any(|block| block == trie.block_index())));
        for path in [entry.slug.data_key(trie), entry.slug.metadata_key(trie)] {
            let key = ObjectKey::new(path).unwrap();
            database.storage().invalidate(&key).await.unwrap();
            database.object_store().delete(&key).await.unwrap();
        }
    }
    for run in runs {
        let key = ObjectKey::new(run.object_key(&entry.slug).unwrap()).unwrap();
        database.object_store().head(&key).await.unwrap();
        database.storage().invalidate(&key).await.unwrap();
        database.object_store().delete(&key).await.unwrap();
    }
    drop(snapshot);
    let ranked = "SELECT _id FROM docs WHERE MATCH(body, '\"audit report\"') ORDER BY SEARCH_SCORE(body, 'report') DESC, _id ASC LIMIT 1";
    assert_eq!(ids(rows(session.query_sql(ranked, vec![]).await.unwrap()).await), [1]);
    database.build_artefacts().await.unwrap();
    let catalog = database.trie_catalog();
    for trie in &removed {
        assert!(catalog.search_runs(&table).any(|run| run.source_receipt.as_ref().is_some_and(|receipt|
            receipt.checkpoint_boundary() == -1 && receipt.blocks.contains(&trie.block_index().0))));
    }
    let reads = runtime.logical_run_reads();
    assert_eq!(ids(rows(session.query_sql(ranked, vec![]).await.unwrap()).await), [1]);
    assert!(runtime.logical_run_reads() > reads);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn a_same_commit_reinsert_keeps_only_the_post_erase_search_revision() {
    let mut settings = options();
    settings.automatic_artefacts = false;
    let runtime = rtdb_search::SearchRuntime::with_logical_search(64 << 20);
    let services = NodeServices { artefact_kinds: vec![runtime.artefacts()], scan_accelerators: vec![runtime.accelerator()], ..services() };
    let (node, driver) = Node::open(settings, services).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    assert!(session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")],
        AutonomousTxOptions::default()).await.unwrap().committed());
    assert!(session.state_mut().execute_tx(vec![
        sql("INSERT INTO docs RECORDS {_id: 1, body: 'oldrevision'}"),
        sql("ERASE FROM docs WHERE _id = 1"),
        sql("INSERT INTO docs RECORDS {_id: 1, body: 'newrevision'}"),
    ], AutonomousTxOptions::default()).await.unwrap().committed());

    let database = node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.build_artefacts().await.unwrap();
    let table = TableRef::new("public", "docs");
    let catalog = database.trie_catalog();
    let run = catalog.search_runs(&table).next().unwrap();
    assert!(run.has_erases());
    assert_eq!(run.source_rows(), 3);
    let root = catalog.term_directory(&table, run.definition_id(), run.definition_version()).unwrap()
        .page_root().unwrap().unwrap();
    let reads = runtime.logical_run_reads();
    assert!(rows(session.query_sql("SELECT _id FROM docs WHERE MATCH(body, 'oldrevision')", vec![]).await.unwrap()).await.is_empty());
    assert_eq!(runtime.logical_run_reads(), reads);
    assert_eq!(ids(rows(session.query_sql("SELECT _id FROM docs WHERE MATCH(body, 'newrevision')", vec![]).await.unwrap()).await), [1]);
    assert!(runtime.logical_run_reads() > reads);
    let reads = runtime.logical_run_reads();
    let point = "SELECT _id FROM docs WHERE _id = 1 AND MATCH(body, 'newrevision') ORDER BY SEARCH_SCORE(body, 'newrevision') DESC LIMIT 1";
    assert_eq!(ids(rows(session.query_sql(point, vec![]).await.unwrap()).await), [1]);
    assert_eq!(runtime.logical_run_reads(), reads);
    let slug = database.snapshot().table_entry(&table).unwrap().slug.clone();
    let page = rtdb_storage::ObjectKey::new(rtdb_format::cow_tree::page_key(
        &rtdb_format::term_directory::page_prefix(&slug), root.digest)).unwrap();
    database.storage().invalidate(&page).await.unwrap();
    database.object_store().delete(&page).await.unwrap();
    database.object_store().put(&page, bytes::Bytes::from_static(b"corrupt")).await.unwrap();
    let gaps = runtime.serving_gaps();
    assert_eq!(ids(rows(session.query_sql("SELECT _id FROM docs WHERE MATCH(body, 'newrevision')", vec![]).await.unwrap()).await), [1]);
    assert!(runtime.serving_gaps() > gaps);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[test]
fn a_missing_logical_run_becomes_gap_work_while_healthy_runs_keep_ranking() {
    std::thread::Builder::new().stack_size(rtdb_node::WORKER_STACK_BYTES).spawn(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(check_missing_logical_run_repair());
    }).unwrap().join().unwrap();
}

async fn check_missing_logical_run_repair() {
    use rtdb_format::database::proto::{DatabaseConfig, LocalLog, LocalStorage, database_config};
    use tokio_util::sync::CancellationToken;

    let directory = tempfile::tempdir().unwrap();
    let path = |name: &str| Some(directory.path().join(name).to_string_lossy().into_owned());
    let mut settings = options();
    settings.automatic_artefacts = false;
    settings.automatic_gc = false;
    settings.membership = Membership::Durable {
        primary: DatabaseConfig {
            log: Some(database_config::Log::LocalLog(LocalLog { path: path("log"), epoch: Some(0) })),
            storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: path("store"), epoch: Some(0) })),
            ..Default::default()
        },
        skipped: Default::default(),
    };
    let template = services();
    let runtime = rtdb_search::SearchRuntime::with_logical_search(64 << 20);
    let services = NodeServices {
        resources: Arc::new(NativeResources::default().with_local_log_clock(Arc::clone(&template.clock), false)),
        artefact_kinds: vec![runtime.artefacts()],
        scan_accelerators: vec![runtime.accelerator()],
        ..template.clone()
    };
    let (node, driver) = Node::open(settings, services).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    let stop = CancellationToken::new();
    let table = TableRef::new("public", "docs");
    node.sync_all(Duration::from_secs(10), &stop).await.unwrap();
    assert!(session.state_mut().execute_tx(vec![
        sql("CREATE SEARCH INDEX docs_body ON docs (body)"),
        sql("CREATE SEARCH INDEX uuid_docs_body ON uuid_docs (body)"),
    ], AutonomousTxOptions::default()).await.unwrap().committed());
    let mut partitions = Vec::new();
    for (batch, uuid_ids) in [
        ("INSERT INTO docs RECORDS {_id: 1, body: 'audit report report'}, {_id: 2, body: 'audit memo'}, {_id: 3, body: 'report'}", [1, 4]),
        ("INSERT INTO docs RECORDS {_id: 5, body: 'audit report'}, {_id: 2, body: 'audit report memo'}", [2, 3]),
        ("INSERT INTO docs RECORDS {_id: 4, body: 'audit report report report'}", [5, 6]),
    ] {
        let mut writes = vec![sql(batch)];
        writes.extend(uuid_ids.map(|id| sql(&format!(
            "INSERT INTO uuid_docs RECORDS {{_id: UUID '00000000-0000-0000-0000-{id:012x}', body: 'audit report'}}"
        ))));
        assert!(session.state_mut().execute_tx(writes, AutonomousTxOptions::default()).await.unwrap().committed());
        database.flush().await.unwrap();
        node.sync_all(Duration::from_secs(10), &stop).await.unwrap();
        partitions.push(database.snapshot());
    }
    database.build_artefacts().await.unwrap();
    let catalog = database.trie_catalog();
    let mut runs: Vec<_> = catalog.search_runs(&table).cloned().collect();
    runs.sort_unstable_by_key(|run| std::cmp::Reverse(run.blocks.len()));
    assert_eq!(runs.len(), 2);
    let slug = database.snapshot().table_entry(&table).unwrap().slug.clone();
    let missing = rtdb_storage::ObjectKey::new(runs[0].object_key(&slug).unwrap()).unwrap();
    database.storage().invalidate(&missing).await.unwrap();
    database.object_store().delete(&missing).await.unwrap();

    let uuid_table = TableRef::new("public", "uuid_docs");
    let uuid_slug = database.snapshot().table_entry(&uuid_table).unwrap().slug.clone();
    let missing_uuid = catalog.search_runs(&uuid_table).next().unwrap().object_key(&uuid_slug).unwrap();
    let missing_uuid = rtdb_storage::ObjectKey::new(missing_uuid).unwrap();
    database.storage().invalidate(&missing_uuid).await.unwrap();
    database.object_store().delete(&missing_uuid).await.unwrap();

    assert!(session.state_mut().execute_tx(vec![
        sql("INSERT INTO docs RECORDS {_id: -1, body: 'audit report', payload: 'live'}"),
        sql("INSERT INTO uuid_docs RECORDS {_id: UUID '00000000-0000-0000-0000-000000000000', body: 'audit report', payload: 'live'}"),
    ], AutonomousTxOptions::default()).await.unwrap().committed());
    partitions.push(database.snapshot());
    let frontier = Arc::new(FrontierProbe { search: runtime.accelerator(), opened: Default::default(), fallback: Default::default() });
    for table_name in ["docs", "uuid_docs"] {
        let query = format!("SELECT _id, body, payload, SEARCH_SCORE(body, 'absent') AS score FROM {table_name} \
            WHERE MATCH(body, 'audit report') ORDER BY score DESC, _id");
        let everything = partition_search(&partitions, database.storage(), &query, None).await;
        assert!(everything.len() > 4, "{query}");
        for (limit, offset) in [(2, 0), (3, 2)] {
            let statement = format!("{query} LIMIT {limit} OFFSET {offset}");
            let ranked = partition_search(&partitions, database.storage(), &statement, Some(frontier.clone())).await;
            assert_eq!(ranked, everything.iter().skip(offset).take(limit).cloned().collect::<Vec<_>>(), "{statement}");
            assert_eq!(frontier.opened.swap(0, Ordering::Relaxed), partitions.len(), "{statement}");
            assert_eq!(frontier.fallback.load(Ordering::Relaxed), 0, "{statement}");
        }
    }

    let explicit = "SELECT _id, body FROM docs WHERE _id = 1 AND MATCH(body, 'audit report') \
        ORDER BY SEARCH_SCORE(body, 'report') DESC LIMIT 2";
    let reads = runtime.logical_run_reads();
    let expected = partition_search(&partitions, database.storage(), explicit, None).await;
    assert_eq!(partition_search(&partitions, database.storage(), explicit, Some(frontier.clone())).await, expected);
    assert_eq!(frontier.opened.load(Ordering::Relaxed), partitions.len());
    assert_eq!(frontier.fallback.load(Ordering::Relaxed), 0);
    assert_eq!(runtime.logical_run_reads(), reads);

    let queries = [
        ("SELECT _id, SEARCH_SCORE(body, 'report') AS score FROM docs WHERE MATCH(body, 'audit report') ORDER BY score DESC, _id ASC LIMIT 2",
         "SELECT _id, SEARCH_SCORE(body, 'report') AS score FROM docs WHERE MATCH(body, 'audit report') OR _id = -99 ORDER BY score DESC, _id ASC LIMIT 2"),
        ("SELECT _id FROM docs WHERE MATCH(body, 'audit report') ORDER BY _id",
         "SELECT _id FROM docs WHERE MATCH(body, 'audit report') OR _id = -99 ORDER BY _id"),
    ];
    for (accelerated, oracle) in queries {
        let reads = runtime.logical_run_reads();
        let expected = rows(session.query_sql(oracle, vec![]).await.unwrap()).await;
        assert!(!expected.is_empty());
        assert_eq!(rows(session.query_sql(accelerated, vec![]).await.unwrap()).await, expected, "{accelerated}");
        assert!(runtime.logical_run_reads() > reads, "the healthy run still serves {accelerated}");
    }

    database.build_artefacts().await.unwrap();
    let repaired = database.trie_catalog();
    let active: Vec<_> = repaired.search_runs(&table).map(|run| (run.blocks.clone(), run.object_key(&slug).unwrap())).collect();
    let replacement = repaired.search_runs(&table).find(|run| runs[0].blocks.iter().all(|block| run.blocks.contains(block)))
        .unwrap_or_else(|| panic!("the damaged extent is covered again: {active:?}"));
    assert_ne!(replacement.object_key(&slug).unwrap(), runs[0].object_key(&slug).unwrap());
    let directory = repaired.term_directory(&table, replacement.definition_id(), replacement.definition_version()).unwrap();
    assert_eq!(directory.runs(), active.len() as i64);
    assert!(database.object_store().head(&rtdb_storage::ObjectKey::new(replacement.object_key(&slug).unwrap()).unwrap()).await.is_ok());
    for (accelerated, oracle) in queries {
        assert_eq!(rows(session.query_sql(accelerated, vec![]).await.unwrap()).await, rows(session.query_sql(oracle, vec![]).await.unwrap()).await);
    }
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

struct NarrowingProbe { search: Arc<rtdb_search::SearchAccelerator>, narrowed: AtomicUsize, declined: AtomicUsize }

#[async_trait]
impl rtdb_query::ScanAccelerator for NarrowingProbe {
    async fn candidate_iids(&self, request: &rtdb_query::ScanRequest<'_>) -> Result<Option<std::collections::BTreeSet<rtdb_types::Iid>>> {
        let candidates = self.search.candidate_iids(request).await?;
        let counter = if candidates.is_some() { &self.narrowed } else { &self.declined };
        counter.fetch_add(1, Ordering::Relaxed);
        Ok(candidates)
    }
}

#[tokio::test]
async fn gap_nomination_charges_only_uncovered_directory_rows() {
    use rtdb_format::database::proto::{DatabaseConfig, LocalLog, LocalStorage, database_config};
    use tokio_util::sync::CancellationToken;

    let directory = tempfile::tempdir().unwrap();
    let path = |name: &str| Some(directory.path().join(name).to_string_lossy().into_owned());
    let mut settings = options();
    settings.automatic_artefacts = false;
    settings.membership = Membership::Durable {
        primary: DatabaseConfig {
            log: Some(database_config::Log::LocalLog(LocalLog { path: path("log"), epoch: Some(0) })),
            storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: path("store"), epoch: Some(0) })),
            ..Default::default()
        },
        skipped: Default::default(),
    };
    let template = services();
    let runtime = rtdb_search::SearchRuntime::with_logical_search(64 << 20);
    let services = NodeServices {
        resources: Arc::new(NativeResources::default().with_local_log_clock(Arc::clone(&template.clock), false)),
        artefact_kinds: vec![runtime.artefacts()],
        scan_accelerators: vec![runtime.accelerator()],
        ..template.clone()
    };
    let (node, driver) = Node::open(settings, services).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    let stop = CancellationToken::new();
    node.sync_all(Duration::from_secs(10), &stop).await.unwrap();
    assert!(session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")], AutonomousTxOptions::default()).await.unwrap().committed());
    let filler = (0..300).map(|id| format!("{{_id: {id}, body: 'filler text'}}")).collect::<Vec<_>>().join(", ");
    assert!(session.state_mut().execute_tx(vec![sql(&format!("INSERT INTO docs RECORDS {filler}"))], AutonomousTxOptions::default()).await.unwrap().committed());
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &stop).await.unwrap();
    database.build_artefacts().await.unwrap();
    assert!(session.state_mut().execute_tx(vec![sql("INSERT INTO docs RECORDS {_id: 1000, body: 'audit'}")], AutonomousTxOptions::default()).await.unwrap().committed());
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &stop).await.unwrap();

    let mut search = rtdb_search::SearchAccelerator::with_runtime(Arc::clone(&runtime));
    search.max_candidates = 4;
    let probe = Arc::new(NarrowingProbe { search: Arc::new(search), narrowed: Default::default(), declined: Default::default() });
    let query = "SELECT _id FROM docs WHERE MATCH(body, 'audit')";
    let partitions = [database.snapshot()];
    assert_eq!(partition_search(&partitions, database.storage(), query, Some(probe.clone())).await, vec![vec![Value::I64(1000)]]);
    assert_eq!((probe.narrowed.load(Ordering::Relaxed), probe.declined.load(Ordering::Relaxed)), (1, 0),
        "three hundred covered rows stay outside a budget of four candidates");
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}
