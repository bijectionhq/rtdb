use std::sync::{Arc, atomic::{AtomicI64, Ordering}};
use std::time::Duration;
use async_trait::async_trait;
use rtdb_auth::{AuthConfig, Authenticator, HttpRequest, HttpResponse, HttpTransport, Secret};
use rtdb_columnar::MemoryBudget;
use rtdb_maintenance::CollectionObserver;
use rtdb_node::{Clock, Node, NodeOptions, NodeServices};
use rtdb_pgwire::{ServerConfig, ServerDriver, ServerHandle};
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows};
use rtdb_types::{Anomaly, Result, TableRef, Timestamp};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

struct NoHttp;
#[async_trait]
impl HttpTransport for NoHttp { async fn request(&self, _: HttpRequest) -> Result<HttpResponse> { Err(Anomaly::fault("test/http", "Unexpected HTTP request")) } }
struct Observer;
impl CollectionObserver for Observer { fn table_failed(&self, _: Option<&TableRef>, error: &Anomaly) { panic!("Unexpected collection failure: {error}"); } }
struct Metrics;
impl CatalogMetrics for Metrics { fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> { Ok(CatalogRows::new(Vec::new(), Arc::new(()))) } }
struct TickClock(AtomicI64);
impl Clock for TickClock { fn now(&self) -> Timestamp { Timestamp(self.0.fetch_add(1000, Ordering::Relaxed)) } }

struct Running { node: Node, node_task: JoinHandle<Result<()>>, server: ServerHandle, server_task: JoinHandle<Result<()>> }
impl Running {
    async fn start(config: ServerConfig, password: Option<&str>) -> Self { Self::start_with(config, AuthConfig::SingleRootUser { password: password.map(Secret::new) }).await }
    async fn start_with(config: ServerConfig, authentication: AuthConfig) -> Self {
        Self::start_with_tracer(config, authentication, None).await
    }
    async fn start_with_tracer(config: ServerConfig, authentication: AuthConfig, tracer: Option<Arc<dyn rtdb_plan::Tracer>>) -> Self {
        let clock: Arc<dyn Clock> = Arc::new(TickClock(AtomicI64::new(1_000_000)));
        let auth = Authenticator::open(authentication, MemoryBudget::new(64 << 20), Arc::clone(&clock), Arc::new(NoHttp)).await.unwrap();
        let services = NodeServices { clock, tracer, ..NodeServices::local(auth.clone(), Arc::new(Observer), auth.clone(), Arc::new(Metrics)) };
        let mut options = NodeOptions::default(); options.automatic_compaction = false; options.automatic_gc = false;
        options.replication.assertion_interval = Duration::from_millis(1); options.replication.election_seed = 1;
        let (node, driver) = Node::open(options, services).await.unwrap(); let node_task = tokio::spawn(driver.run());
        let (server, driver) = ServerDriver::bind(config, node.clone(), auth).await.unwrap(); let server_task = tokio::spawn(driver.run());
        Self { node, node_task, server, server_task }
    }
    async fn stop(self) {
        self.server.shutdown(); tokio::time::timeout(Duration::from_secs(10), self.server_task).await.unwrap().unwrap().unwrap();
        assert!(self.server.status().stopped); assert_eq!(self.server.status().active_connections, 0);
        self.node.shutdown(); tokio::time::timeout(Duration::from_secs(10), self.node_task).await.unwrap().unwrap().unwrap();
    }
}

struct Client { stream: TcpStream, process: i32 }
impl Client {
    async fn connect(server: &ServerHandle, password: Option<&str>) -> Self {
        Self::connect_named(server, password, None).await
    }
    async fn connect_named(server: &ServerHandle, password: Option<&str>, name: Option<&str>) -> Self {
        let mut stream = TcpStream::connect(server.address()).await.unwrap();
        let mut bytes = 196608i32.to_be_bytes().to_vec();
        bytes.extend_from_slice(b"user\0xtdb\0database\0xtdb\0");
        if let Some(name) = name {
            bytes.extend_from_slice(b"application_name\0");
            bytes.extend_from_slice(&cstring(name));
        }
        bytes.push(0);
        stream.write_i32((bytes.len() + 4) as i32).await.unwrap(); stream.write_all(&bytes).await.unwrap();
        let mut client = Self { stream, process: 0 };
        loop {
            let (tag, bytes) = client.receive().await;
            match tag {
                b'R' if bytes == 3i32.to_be_bytes() => client.send(b'p', &cstring(password.unwrap())).await,
                b'K' => { client.process = i32::from_be_bytes(bytes[..4].try_into().unwrap()); assert_eq!(&bytes[4..], &[0; 4]); },
                b'Z' => { assert_eq!(bytes, b"I"); break; },
                b'E' => panic!("Startup failed: {}", String::from_utf8_lossy(&bytes)),
                _ => (),
            }
        }
        assert!(client.process > 0); client
    }
    async fn send(&mut self, tag: u8, bytes: &[u8]) { self.stream.write_u8(tag).await.unwrap(); self.stream.write_i32((bytes.len() + 4) as i32).await.unwrap(); self.stream.write_all(bytes).await.unwrap(); self.stream.flush().await.unwrap(); }
    async fn receive(&mut self) -> (u8, Vec<u8>) {
        tokio::time::timeout(Duration::from_secs(10), async {
            let tag = self.stream.read_u8().await.unwrap(); let length = self.stream.read_i32().await.unwrap();
            assert!((4..1_000_000).contains(&length)); let mut bytes = vec![0; (length - 4) as usize]; self.stream.read_exact(&mut bytes).await.unwrap(); (tag, bytes)
        }).await.unwrap()
    }
    async fn through(&mut self, end: u8) -> Vec<(u8, Vec<u8>)> { let mut result = Vec::new(); loop { let frame = self.receive().await; let done = frame.0 == end; result.push(frame); if done { return result; } } }
    async fn query(&mut self, sql: &str) -> Vec<(u8, Vec<u8>)> { self.send(b'Q', &cstring(sql)).await; self.through(b'Z').await }
    async fn parse(&mut self, name: &str, sql: &str, oids: &[i32]) { let mut bytes = cstring(name); bytes.extend_from_slice(&cstring(sql)); bytes.extend_from_slice(&(oids.len() as u16).to_be_bytes()); for oid in oids { bytes.extend_from_slice(&oid.to_be_bytes()); } self.send(b'P', &bytes).await; }
    async fn bind(&mut self, portal: &str, statement: &str) {
        self.bind_formats(portal, statement, &[]).await;
    }
    async fn bind_formats(&mut self, portal: &str, statement: &str, formats: &[u16]) {
        let mut bytes = cstring(portal);
        bytes.extend_from_slice(&cstring(statement));
        bytes.extend_from_slice(&[0; 4]);
        bytes.extend_from_slice(&(formats.len() as u16).to_be_bytes());
        for format in formats { bytes.extend_from_slice(&format.to_be_bytes()); }
        self.send(b'B', &bytes).await;
    }
    async fn execute(&mut self, portal: &str, rows: i32) { let mut bytes = cstring(portal); bytes.extend_from_slice(&rows.to_be_bytes()); self.send(b'E', &bytes).await; }
}
fn cstring(text: &str) -> Vec<u8> { let mut bytes = text.as_bytes().to_vec(); bytes.push(0); bytes }
fn tags(frames: &[(u8, Vec<u8>)]) -> Vec<u8> { frames.iter().map(|frame| frame.0).collect() }

fn initial_statement_mismatch(line: usize, column: usize) -> String {
    format!(
        "Errors parsing SQL statement:\n  - line {line}:{column} mismatched input 'SELEC' expecting {{'(', 'ASSERT', 'ATTACH', 'BEGIN', 'COMMIT', 'COPY', 'CREATE', 'DELETE', 'DETACH', 'DISCARD', 'ERASE', 'EXECUTE', 'EXPLAIN', 'FROM', 'GRANT', 'INSERT', 'PATCH', 'PREPARE', 'RECORDS', 'REVOKE', 'ROLLBACK', 'SELECT', 'SET', 'SETTING', 'SHOW', 'START', 'UPDATE', 'VALUES', 'WITH', 'XTQL'}}"
    )
}

fn error_fields(body: &[u8]) -> (String, String, serde_json::Value) {
    let field = |tag: u8| body.split(|&byte| byte == 0).find_map(|field| field.strip_prefix(&[tag])).unwrap();
    let text = |tag: u8| String::from_utf8(field(tag).to_vec()).unwrap();
    (text(b'C'), text(b'M'), serde_json::from_slice(field(b'D')).unwrap())
}
fn rows(frames: &[(u8, Vec<u8>)]) -> Vec<Vec<Option<Vec<u8>>>> {
    frames.iter().filter(|frame| frame.0 == b'D').map(|(_, bytes)| {
        let count = u16::from_be_bytes(bytes[..2].try_into().unwrap()); let mut offset = 2;
        (0..count).map(|_| { let length = i32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()); offset += 4; if length < 0 { None } else { let value = bytes[offset..offset + length as usize].to_vec(); offset += length as usize; Some(value) } }).collect()
    }).collect()
}

#[tokio::test]
async fn date64_arrow_copy_can_be_read_as_binary_postgres_dates() {
    use arrow_array::{ArrayRef, Date64Array, Int64Array, RecordBatch};
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    let batch = RecordBatch::try_from_iter([
        ("_id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        ("d", Arc::new(Date64Array::from(vec![Some(-1), Some(86_400_000), None])) as ArrayRef),
    ]).unwrap();
    let mut ipc = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    client.send(b'Q', &cstring("COPY dates FROM STDIN WITH (FORMAT 'arrow-stream')")).await;
    assert_eq!(client.receive().await.0, b'G');
    client.send(b'd', &ipc).await;
    client.send(b'c', &[]).await;
    assert_eq!(tags(&client.through(b'Z').await), b"CZ");

    client.parse("dates", "SELECT d FROM dates ORDER BY _id", &[]).await;
    client.bind_formats("dates", "dates", &[1]).await;
    client.execute("dates", 0).await;
    client.send(b'S', &[]).await;
    let frames = client.through(b'Z').await;
    assert_eq!(tags(&frames), b"12DDDCZ");
    assert_eq!(rows(&frames), [vec![Some((-10_958i32).to_be_bytes().to_vec())], vec![Some((-10_956i32).to_be_bytes().to_vec())], vec![None]]);
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn query_spans_follow_pgwire_application_name_across_entry_points() {
    let (recorder, mut receiver) = rtdb_metrics::TraceRecorder::new(MemoryBudget::new(8 << 20), 128, true, false).unwrap();
    let running = Running::start_with_tracer(ServerConfig::default(), AuthConfig::SingleRootUser { password: None }, Some(recorder.clone())).await;
    let mut default_client = Client::connect(&running.server, None).await;
    assert_eq!(rows(&default_client.query("SELECT 101 AS app_probe").await), [vec![Some(b"101".to_vec())]]);
    let mut client = Client::connect_named(&running.server, None, Some("startup-app")).await;
    assert_eq!(rows(&client.query("SELECT 102 AS app_probe").await), [vec![Some(b"102".to_vec())]]);
    assert_eq!(tags(&client.query("SET application_name = 'live-app'").await), b"SCZ");
    client.parse("app", "SELECT 103 AS app_probe", &[]).await;
    client.bind("app", "app").await;
    client.execute("app", 0).await;
    client.send(b'S', &[]).await;
    assert_eq!(rows(&client.through(b'Z').await), [vec![Some(b"103".to_vec())]]);
    assert!(!tags(&client.query("DISCARD ALL").await).contains(&b'E'));
    assert_eq!(rows(&client.query("SELECT 104 AS app_probe").await), [vec![Some(b"104".to_vec())]]);
    drop((default_client, client));
    running.stop().await;
    recorder.close();

    let mut names = std::collections::BTreeMap::new();
    while let Some(span) = receiver.next().await {
        if span.name == "xtdb.query" {
            let attrs: std::collections::BTreeMap<_, _> = span.attributes().collect();
            if let (Some(text), Some(app)) = (attrs.get("query.text"), attrs.get("application.name")) {
                names.insert((*text).to_owned(), (*app).to_owned());
            }
        }
    }
    for (n, app) in [(101, "XTDB PGWire"), (102, "startup-app"), (103, "live-app"), (104, "live-app")] {
        assert_eq!(names.get(&format!("SELECT {n} AS app_probe")).map(String::as_str), Some(app));
    }
}

#[tokio::test]
async fn copy_to_arrow_stream_uses_the_original_read_transaction() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut writer = Client::connect(&running.server, None).await;
    let mut reader = Client::connect(&running.server, None).await;
    assert_eq!(tags(&writer.query("INSERT INTO payloads (_id, val) VALUES (1, X'6162')").await), b"CZ");
    assert_eq!(tags(&reader.query("BEGIN READ ONLY").await), b"CZ");
    let first = reader.query("SELECT COUNT(*) FROM payloads").await;
    assert_eq!(rows(&first), [vec![Some(b"1".to_vec())]]);
    assert_eq!(tags(&writer.query("INSERT INTO payloads (_id, val) VALUES (2, X'6364')").await), b"CZ");

    reader.parse("arrow", "COPY (SELECT val FROM (
        SELECT 0::BIGINT AS probe, _id, val FROM payloads WHERE _id <= 1
        UNION ALL
        SELECT 1::BIGINT AS probe, _id, val FROM payloads WHERE _id > 1
    ) AS probes ORDER BY probe, _id) TO STDOUT WITH (FORMAT 'arrow-stream')", &[]).await;
    reader.send(b'H', &[]).await;
    assert_eq!(reader.receive().await.0, b'1');
    reader.bind("transfer", "arrow").await;
    reader.send(b'H', &[]).await;
    assert_eq!(reader.receive().await.0, b'2');
    reader.execute("transfer", 0).await;
    reader.send(b'S', &[]).await;
    let frames = reader.through(b'Z').await;
    assert_eq!(frames[0].0, b'H');
    assert_eq!(&frames[frames.len() - 3..].iter().map(|frame| frame.0).collect::<Vec<_>>(), b"cCZ");
    assert_eq!(frames[frames.len() - 2].1, b"COPY 1\0");
    assert_eq!(frames.last().unwrap().1, b"T");
    let ipc = frames.into_iter().filter(|frame| frame.0 == b'd').flat_map(|(_, bytes)| bytes).collect::<Vec<_>>();
    let batches = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(ipc), None).unwrap()
        .collect::<std::result::Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches.len(), 1);
    let array = batches[0].column(0).as_any().downcast_ref::<arrow_array::BinaryArray>().unwrap();
    assert_eq!(array.value(0), b"ab");
    assert_eq!(tags(&reader.query("COMMIT").await), b"CZ");
    running.stop().await;
}

#[tokio::test]
async fn copy_to_history_label_dictionary_is_opt_in_at_the_original_cut() {
    use arrow_array::{DictionaryArray, FixedSizeBinaryArray, Int64Array, StringArray, StructArray, types::Int32Type};
    use arrow_ipc::reader::StreamReader;
    use arrow_schema::DataType;

    let running = Running::start(ServerConfig::default(), None).await;
    let mut writer = Client::connect(&running.server, None).await;
    let mut reader = Client::connect(&running.server, None).await;
    assert_eq!(tags(&writer.query(r#"INSERT INTO effects (_id, label) VALUES
        (1, '[]'), (2, '["org:tenant:é"]'), (3, 'top'), (4, '[]')"#).await), b"CZ");
    assert_eq!(tags(&reader.query("BEGIN READ ONLY").await), b"CZ");
    assert_eq!(rows(&reader.query("SELECT COUNT(*) FROM effects").await), [vec![Some(b"4".to_vec())]]);
    assert_eq!(tags(&writer.query("INSERT INTO effects (_id, label) VALUES (5, '[]')").await), b"CZ");

    let expected = [
        ("[]", 0x37dcbab2e0812c3533cdd7a7238521e2u128.to_be_bytes()),
        (r#"["org:tenant:é"]"#, 0x9708e3350c49ae4d46dcc78e72242168u128.to_be_bytes()),
        ("top", 0xc41022fe40b6bf36c736c22351bee076u128.to_be_bytes()),
    ];
    for (name, offer) in [("legacy", ""), ("dictionary", "/* rtdb-history-label-dictionary-v1 */ ")] {
        let sql = format!("COPY ({offer}SELECT _id, label FROM effects ORDER BY _id) TO STDOUT WITH (FORMAT 'arrow-stream')");
        reader.parse(name, &sql, &[]).await;
        reader.bind(name, name).await;
        reader.execute(name, 0).await;
        reader.send(b'S', &[]).await;
        let frames = reader.through(b'Z').await;
        assert_eq!(tags(&frames[..3]), b"12H");
        assert_eq!(frames[frames.len() - 2].1, b"COPY 4\0");
        assert_eq!(frames.last().unwrap().1, b"T");
        let ipc = frames.into_iter().filter(|frame| frame.0 == b'd').flat_map(|(_, bytes)| bytes).collect::<Vec<_>>();
        let batches = StreamReader::try_new(std::io::Cursor::new(ipc), None).unwrap();
        let mut count = 0;
        for batch in batches {
            let batch = batch.unwrap();
            let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let array = batch.column(1);
            for row in 0..batch.num_rows() {
                let (spelling, expected_id) = expected[count % 3];
                count += 1;
                assert_eq!(ids.value(row), count as i64);
                if offer.is_empty() {
                    assert_eq!(array.data_type(), &DataType::Utf8);
                    assert_eq!(array.as_any().downcast_ref::<StringArray>().unwrap().value(row), spelling);
                } else {
                    let dictionary = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
                    let symbols = dictionary.values().as_any().downcast_ref::<StructArray>().unwrap();
                    let index = dictionary.keys().value(row) as usize;
                    let ids = symbols.column_by_name("id").unwrap().as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
                    let spellings = symbols.column_by_name("canonical").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
                    assert_eq!(ids.value(index), expected_id.as_slice());
                    assert_eq!(spellings.value(index), spelling);
                }
            }
        }
        assert_eq!(count, 4);
    }
    assert_eq!(tags(&reader.query("COMMIT").await), b"CZ");
    running.stop().await;
}

#[tokio::test]
async fn duration_literal_overflow_occurs_only_when_query_output_demands_value() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;

    let frames = client.query("SELECT DURATION 'PT9223372037S'").await;
    assert_eq!(tags(&frames), b"TEZ");
    let error: serde_json::Value = serde_json::from_slice(
        frames[1].1.split(|byte| *byte == 0).find_map(|field| field.strip_prefix(b"D")).unwrap(),
    ).unwrap();
    assert_eq!(error["code"], "xtdb.expression/overflow-error");

    let frames = client.query("SELECT DURATION 'PT9223372037S' WHERE FALSE").await;
    assert_eq!(tags(&frames), b"TCZ");
    assert!(rows(&frames).is_empty());

    let frames = client.query("SET application_name = DURATION 'PT9223372037S'").await;
    assert_eq!(tags(&frames), b"SCZ");
    let frames = client.query("SHOW application_name").await;
    assert_eq!(tags(&frames), b"TDCZ");
    assert_eq!(rows(&frames), [vec![Some(b"PT2562047H47M17S".to_vec())]]);
    running.stop().await;
}

#[tokio::test]
async fn sql_parse_failures_return_anomaly_details_and_an_idle_connection() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    for sql in ["SELEC 1", "extra SELECT 1"] {
        let frames = client.query(sql).await;
        assert_eq!(tags(&frames), b"EZ");
        assert_eq!(frames[1].1, b"I");
        let body = &frames[0].1;
        assert!(body.ends_with(b"\0\0"));
        let mut fields = body.split(|&byte| byte == 0);
        assert_eq!(fields.next(), Some(b"SERROR".as_slice()));
        assert_eq!(fields.next(), Some(b"VERROR".as_slice()));
        assert_eq!(fields.next(), Some(b"C08P01".as_slice()));
        let message = fields.next().unwrap().strip_prefix(b"M").unwrap();
        let message = std::str::from_utf8(message).unwrap();
        assert!(!message.is_empty());
        let detail = fields.next().unwrap().strip_prefix(b"D").unwrap();
        let detail: serde_json::Value = serde_json::from_slice(detail).unwrap();
        assert_eq!(detail["category"], "incorrect");
        assert_eq!(detail["code"], "xtdb/sql-error");
        assert_eq!(detail["message"], message);
        assert!(detail.get("cause").is_none());
        assert!(fields.all(|field| field.is_empty()));
    }
    assert_eq!(rows(&client.query("SELECT 42 AS answer").await), vec![vec![Some(b"42".to_vec())]]);
    drop(client);
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cancel_request_stops_a_large_sorted_query() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    client.parse("", "FROM GENERATE_SERIES(1,10000000) AS system(_id) ORDER BY _id", &[]).await;
    client.bind("", "").await;
    let mut describe = vec![b'P']; describe.extend_from_slice(&cstring("")); client.send(b'D', &describe).await;
    client.execute("", 0).await; client.send(b'S', &[]).await;
    let (address, process) = (running.server.address(), client.process);
    let canceller = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(2000)).await;
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket.write_i32(16).await.unwrap(); socket.write_i32(80877102).await.unwrap(); socket.write_i32(process).await.unwrap(); socket.write_i32(0).await.unwrap(); socket.flush().await.unwrap();
    });
    let started = std::time::Instant::now();
    let frames = client.through(b'Z').await;
    canceller.await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(10), "cancellation took {:?}", started.elapsed());
    let error = frames.iter().find(|frame| frame.0 == b'E').expect("an error");
    assert!(error.1.split(|&byte| byte == 0).any(|field| field == b"C57014"), "{}", String::from_utf8_lossy(&error.1));
    drop(client); running.stop().await;
}

#[tokio::test]
async fn begin_syntax_errors_report_the_grammar_diagnostic() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    for (sql, expected) in [
        ("BEGIN not valid sql!", "line 1:6 mismatched input 'not'"),
        ("BEGIN SERIALIZABLE", "line 1:6 extraneous input 'SERIALIZABLE'"),
    ] {
        let frames = client.query(sql).await;
        assert_eq!(tags(&frames), b"EZ", "{sql}");
        let (_, message, _) = error_fields(&frames[0].1);
        assert!(message.contains(expected), "{sql}: {message}");
        client.parse("", sql, &[]).await; client.send(b'S', &[]).await;
        let frames = client.through(b'Z').await;
        assert_eq!(tags(&frames), b"EZ", "extended {sql}");
        let (_, message, _) = error_fields(&frames[0].1);
        assert!(message.contains(expected), "extended {sql}: {message}");
    }
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn first_simple_query_report_preserves_an_open_transaction_and_json_ld_detail() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    assert_eq!(tags(&client.query("SET fallback_output_format = 'json-ld'").await), b"SCZ");
    assert_eq!(client.query("BEGIN READ ONLY").await.last().unwrap().1, b"T");

    let sql = "  SELEC 1; SELECT 2; ";
    let frames = client.query(sql).await;
    assert_eq!(tags(&frames), b"EZ");
    assert_eq!(frames[1].1, b"T");
    let fields = frames[0].1.split(|&byte| byte == 0).filter_map(|field| field.split_first()).collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(fields[&b'C'], b"08P01");
    let message = std::str::from_utf8(fields[&b'M']).unwrap();
    assert!(message.starts_with("Errors parsing SQL statement:\n  - line 1:0 mismatched input 'SELEC' expecting "));
    let detail: serde_json::Value = serde_json::from_slice(fields[&b'D']).unwrap();
    assert_eq!(detail["@type"], "xt:incorrect");
    assert_eq!(detail["@value"]["xtdb.error/message"], message);
    let data = &detail["@value"]["xtdb.error/data"];
    assert_eq!(data["xtdb.error/code"], serde_json::json!({"@type":"xt:keyword","@value":"xtdb/sql-error"}));
    assert_eq!(data["sql"], sql);

    let resumed = client.query("SELECT 42 AS answer").await;
    assert_eq!(resumed.last().unwrap().1, b"T");
    assert_eq!(rows(&resumed), [vec![Some(b"42".to_vec())]]);
    assert_eq!(client.query("ROLLBACK").await.last().unwrap().1, b"I");
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn min_max_incomparable_default_json_pgwire() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    assert_eq!(tags(&client.query("INSERT INTO docs (_id, v) VALUES (1, 12)").await), b"CZ");
    assert_eq!(tags(&client.query("INSERT INTO docs (_id, v) VALUES (2, TIMESTAMP '2022-08-01T13:34:00+01:00[Europe/London]')").await), b"CZ");

    let frames = client.query("SELECT MAX(v) AS m FROM docs").await;
    assert_eq!(tags(&frames), b"EZ");
    assert_eq!(frames[1].1, b"I");
    let fields = frames[0].1.split(|&byte| byte == 0).filter_map(|field| field.split_first()).collect::<std::collections::BTreeMap<_, _>>();
    let message = std::str::from_utf8(fields[&b'M']).unwrap();
    assert!(message.contains("Incomparable types in min/max aggregate"));
    assert!(message.contains("#xt/type"));
    let detail: serde_json::Value = serde_json::from_slice(fields[&b'D']).unwrap();
    assert_eq!(detail["category"], "incorrect");
    assert_eq!(detail["code"], "xtdb.group-by/incomparable-min-max-types");
    assert_eq!(detail["message"], message);
    assert!(detail["data"]["from-type"].as_str().unwrap().contains("#xt/type"));
    assert_eq!(rows(&client.query("SELECT 42 AS answer").await), vec![vec![Some(b"42".to_vec())]]);

    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn min_max_incomparable_json_ld_pgwire() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    assert_eq!(tags(&client.query("INSERT INTO docs (_id, v) VALUES (1, 12)").await), b"CZ");
    assert_eq!(tags(&client.query("INSERT INTO docs (_id, v) VALUES (2, TIMESTAMP '2022-08-01T13:34:00+01:00[Europe/London]')").await), b"CZ");
    let setting = client.query("SET fallback_output_format = 'json-ld'").await;
    assert_eq!(tags(&setting), b"SCZ");
    assert_eq!(setting[0].1, b"fallback_output_format\0json-ld\0");

    let frames = client.query("SELECT MAX(v) AS m FROM docs").await;
    assert_eq!(tags(&frames), b"EZ");
    let fields = frames[0].1.split(|&byte| byte == 0).filter_map(|field| field.split_first()).collect::<std::collections::BTreeMap<_, _>>();
    let message = std::str::from_utf8(fields[&b'M']).unwrap();
    assert!(message.contains("Incomparable types in min/max aggregate"));
    let detail: serde_json::Value = serde_json::from_slice(fields[&b'D']).unwrap();
    assert_eq!(detail["@type"], "xt:incorrect");
    assert_eq!(detail["@value"]["xtdb.error/message"], message);
    let data = &detail["@value"]["xtdb.error/data"];
    assert_eq!(data["xtdb.error/code"], serde_json::json!({"@type":"xt:keyword","@value":"xtdb.group-by/incomparable-min-max-types"}));
    let from_type = &data["from-type"];
    assert_eq!(from_type["@type"], "xt:type");
    assert_eq!(from_type["@value"]["@type"], "xt:set");
    let legs = from_type["@value"]["@value"].as_array().unwrap();
    assert!(legs.contains(&serde_json::json!({"@type":"xt:keyword","@value":"i64"})));
    assert!(legs.iter().any(|leg| leg[0] == serde_json::json!({"@type":"xt:keyword","@value":"timestamp-tz"}) && leg[2] == "Europe/London"));

    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn percentile_planning_warning_precedes_simple_and_extended_results_without_repeating_at_bind() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    let sql = "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v NULLS FIRST) AS p FROM (VALUES (1), (3)) AS t(v)";
    let expected = b"SWARNING\0VWARNING\0C01000\0MNULL ordering is ignored for percentile_cont (nulls are excluded from ordered-set aggregates)\0\0";
    let simple = client.query(sql).await;
    assert_eq!(tags(&simple), b"NTDCZ");
    assert_eq!(simple[0].1, expected);
    client.parse("percentile", sql, &[]).await;
    client.bind("first", "percentile").await;
    client.execute("first", 0).await;
    client.send(b'S', &[]).await;
    let extended = client.through(b'Z').await;
    assert_eq!(tags(&extended), b"N12DCZ");
    assert_eq!(extended[0].1, expected);
    client.bind("second", "percentile").await;
    client.execute("second", 0).await;
    client.send(b'S', &[]).await;
    assert_eq!(tags(&client.through(b'Z').await), b"2DCZ");
    let prepared = client.query(&format!("PREPARE named AS {sql}")).await;
    assert_eq!(tags(&prepared), b"NCZ");
    assert_eq!(prepared[0].1, expected);
    assert_eq!(tags(&client.query("EXECUTE named").await), b"TDCZ");
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn virtual_endpoint_retains_timestamp_description_and_emits_null() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    client.parse("endpoint", "SELECT _valid_from FROM pg_catalog.pg_namespace LIMIT 1", &[]).await;
    client.bind("endpoint", "endpoint").await;
    let mut description = vec![b'P']; description.extend_from_slice(&cstring("endpoint")); client.send(b'D', &description).await;
    client.execute("endpoint", 0).await; client.send(b'S', &[]).await;
    let frames = client.through(b'Z').await;
    assert_eq!(tags(&frames), b"12TDCZ");
    assert_eq!(rows(&frames), vec![vec![None]]);
    let description = &frames.iter().find(|frame| frame.0 == b'T').unwrap().1;
    let name_end = 2 + description[2..].iter().position(|byte| *byte == 0).unwrap();
    assert_eq!(i32::from_be_bytes(description[name_end + 7..name_end + 11].try_into().unwrap()), 1184);
    drop(client); running.stop().await;
}

#[tokio::test]
async fn simple_queries_password_authentication_and_transaction_visibility() {
    let running = Running::start(ServerConfig::default(), Some("correct")).await;
    let mut client = Client::connect(&running.server, Some("correct")).await;
    let frames = client.query("BEGIN READ WRITE; INSERT INTO docs (_id, n) VALUES (1, 10); COMMIT;").await;
    assert_eq!(tags(&frames), b"CCCZ");
    let frames = client.query("SELECT n FROM docs WHERE _id = 1").await;
    assert_eq!(tags(&frames), b"TDCZ"); assert_eq!(rows(&frames), vec![vec![Some(b"10".to_vec())]]);
    let frames = client.query("BEGIN READ WRITE; INSERT INTO docs (_id, n) VALUES (2, 20); ROLLBACK;").await;
    assert_eq!(tags(&frames), b"CCCZ");
    let frames = client.query("SELECT n FROM docs ORDER BY _id").await;
    assert_eq!(rows(&frames), vec![vec![Some(b"10".to_vec())]]);
    drop(client); running.stop().await;
}

#[tokio::test]
async fn portal_limit_equal_to_remaining_rows_suspends_then_reports_cumulative_count() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    client.parse("s", "SELECT 1 AS n UNION ALL SELECT 2 AS n", &[]).await; client.bind("p", "s").await;
    let mut description = vec![b'P']; description.extend_from_slice(&cstring("p")); client.send(b'D', &description).await;
    client.execute("p", 1).await; client.send(b'H', &[]).await;
    let first = client.through(b's').await; assert_eq!(tags(&first), b"12TDs"); assert_eq!(rows(&first), vec![vec![Some(b"1".to_vec())]]);
    client.execute("p", 1).await; client.send(b'H', &[]).await;
    let second = client.through(b's').await; assert_eq!(tags(&second), b"Ds"); assert_eq!(rows(&second), vec![vec![Some(b"2".to_vec())]]);
    client.execute("p", 0).await; client.send(b'S', &[]).await;
    let complete = client.through(b'Z').await; assert_eq!(tags(&complete), b"CZ"); assert_eq!(complete[0].1, b"SELECT 2\0");
    drop(client); running.stop().await;
}

#[tokio::test]
async fn extended_error_discards_messages_through_sync_and_simple_query_recovers() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    client.parse("bad", "SELECT FROM", &[]).await; client.send(b'Q', &cstring("SELECT 1")).await; client.send(b'S', &[]).await;
    assert_eq!(tags(&client.through(b'Z').await), b"EZ");
    assert_eq!(tags(&client.query("SELECT 1").await), b"TDCZ");
    drop(client); running.stop().await;
}

#[tokio::test]
async fn transit_copy_fragments_normalize_keyword_ids_and_copy_fail_discards_the_file() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    client.send(b'Q', &cstring("COPY docs FROM STDIN WITH (FORMAT 'transit-json')")).await;
    assert_eq!(client.receive().await, (b'G', vec![0, 0, 1, 0, 0]));
    let bytes = br#"["^ ","~:xt/id",1,"~:n",10]["^ ","_id",2,"n",20]"#;
    for chunk in bytes.chunks(7) { client.send(b'd', chunk).await; }
    client.send(b'c', &[]).await; let complete = client.through(b'Z').await;
    assert_eq!(tags(&complete), b"CZ"); assert_eq!(complete[0].1, b"COPY 2\0");
    assert_eq!(rows(&client.query("SELECT n FROM docs ORDER BY _id").await), vec![vec![Some(b"10".to_vec())], vec![Some(b"20".to_vec())]]);
    client.send(b'Q', &cstring("COPY docs FROM STDIN WITH (FORMAT 'transit-json')")).await; assert_eq!(client.receive().await.0, b'G');
    client.send(b'd', br#"["^ ","_id",3,"n",30]"#).await; client.send(b'f', &cstring("cancelled by client")).await;
    assert_eq!(tags(&client.through(b'Z').await), b"Z");
    assert_eq!(rows(&client.query("SELECT n FROM docs ORDER BY _id").await).len(), 2);
    drop(client); running.stop().await;
}

#[tokio::test]
async fn transit_copy_msgpack_decodes_across_pages_and_rejects_a_truncated_tail_atomically() {
    use rtdb_format::transit::{TransitValue as T, encode_msgpack};

    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    let sql = "COPY docs FROM STDIN WITH (FORMAT 'transit-msgpack')";
    for start in [0, 130] {
        client.send(b'Q', &cstring(sql)).await;
        assert_eq!(client.receive().await, (b'G', vec![1, 0, 1, 0, 1]));
        let mut bytes = (start..start + 130).flat_map(|id| {
            encode_msgpack(&T::Map(vec![(T::String("_id".into()), T::Integer(id))])).unwrap()
        }).collect::<Vec<_>>();
        if start != 0 { bytes.extend_from_slice(&[0xdd, 0]); }
        for chunk in bytes.chunks(19) { client.send(b'd', chunk).await; }
        client.send(b'c', &[]).await;
        let result = client.through(b'Z').await;
        if start == 0 {
            assert_eq!(tags(&result), b"CZ");
            assert_eq!(result[0].1, b"COPY 130\0");
        } else {
            assert_eq!(tags(&result), b"EZ");
        }
        assert_eq!(rows(&client.query("SELECT COUNT(*) FROM docs").await), vec![vec![Some(b"130".to_vec())]]);
    }
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn a_read_only_pgwire_listener_refuses_copy_before_accepting_input() {
    let running = Running::start(ServerConfig { read_only: true, ..Default::default() }, None).await;
    let mut client = Client::connect(&running.server, None).await;
    let refused = client.query("COPY docs FROM STDIN WITH (FORMAT 'transit-json')").await;
    assert_eq!(tags(&refused), b"EZ");
    assert!(String::from_utf8_lossy(&refused[0].1).contains("COPY is not allowed on the READ ONLY server"));
    assert_eq!(tags(&client.query("SELECT 1").await), b"TDCZ");
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn ssl_decline_can_continue_to_startup_and_oversized_preauth_frame_is_rejected() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut stream = TcpStream::connect(running.server.address()).await.unwrap();
    stream.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).await.unwrap(); assert_eq!(stream.read_u8().await.unwrap(), b'N');
    stream.write_i32(10_005).await.unwrap();
    let mut client = Client { stream, process: 0 }; assert_eq!(client.receive().await.0, b'E');
    let mut byte = [0]; assert_eq!(tokio::time::timeout(Duration::from_secs(5), client.stream.read(&mut byte)).await.unwrap().unwrap(), 0);
    running.stop().await;
}

#[tokio::test]
async fn shutdown_joins_connections_waiting_for_authentication_and_extended_sync() {
    let config = ServerConfig { drain_timeout: Duration::from_millis(1), ..Default::default() };
    let running = Running::start(config, None).await;
    let _unauthenticated = TcpStream::connect(running.server.address()).await.unwrap();
    let mut client = Client::connect(&running.server, None).await;
    client.parse("s", "SELECT 1", &[]).await; client.send(b'H', &[]).await; assert_eq!(client.receive().await.0, b'1');
    running.stop().await;
    let mut byte = [0]; assert_eq!(client.stream.read(&mut byte).await.unwrap(), 0);
}

#[tokio::test]
async fn binary_bind_parameters_and_result_formats_use_declared_oids() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    client.parse("s", "SELECT $1 AS n", &[23]).await;
    let mut bind = cstring("p"); bind.extend_from_slice(&cstring("s"));
    bind.extend_from_slice(&1u16.to_be_bytes()); bind.extend_from_slice(&1u16.to_be_bytes());
    bind.extend_from_slice(&1u16.to_be_bytes()); bind.extend_from_slice(&4i32.to_be_bytes()); bind.extend_from_slice(&42i32.to_be_bytes());
    bind.extend_from_slice(&1u16.to_be_bytes()); bind.extend_from_slice(&1u16.to_be_bytes());
    client.send(b'B', &bind).await; client.execute("p", 0).await; client.send(b'S', &[]).await;
    let frames = client.through(b'Z').await; assert_eq!(tags(&frames), b"12DCZ");
    assert_eq!(rows(&frames), vec![vec![Some(42i32.to_be_bytes().to_vec())]]);
    drop(client); running.stop().await;
}

#[tokio::test]
async fn numeric_parameters_keep_numeric_result_types_through_integer_arithmetic() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    client.parse("numeric", "SELECT $1 AS d, $2 + 1 AS e, 1 AS i", &[1700, 1700]).await;
    let mut description = vec![b'S']; description.extend_from_slice(&cstring("numeric"));
    client.send(b'D', &description).await; client.send(b'S', &[]).await;
    let frames = client.through(b'Z').await;
    assert_eq!(tags(&frames), vec![b'1', b't', b'T', b'Z']);
    let fields = &frames[2].1;
    let mut offset = 2; let mut oids = Vec::new();
    for _ in 0..u16::from_be_bytes(fields[..2].try_into().unwrap()) {
        offset += fields[offset..].iter().position(|&byte| byte == 0).unwrap() + 1 + 6;
        oids.push(i32::from_be_bytes(fields[offset..offset + 4].try_into().unwrap())); offset += 12;
    }
    assert_eq!(oids, [1700, 1700, 20]);
    drop(client); running.stop().await;
}

#[tokio::test]
async fn parameter_descriptions_keep_dml_and_control_oids_and_resize_query_oids() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    for (name, sql, supplied, expected, result) in [
        ("dml_missing", "UPDATE docs SET v=$2", vec![], vec![], b'n'),
        ("dml_extra", "DELETE FROM docs", vec![23, 0], vec![23, 25], b'n'),
        ("control_missing", "SET TIME ZONE $1", vec![], vec![], b'n'),
        ("control_extra", "SET TIME ZONE 'UTC'", vec![25], vec![25], b'n'),
        ("query_extra", "SELECT $1 AS value", vec![23, 999_999], vec![23], b'T'),
        ("query_missing", "SELECT $2 AS value", vec![23], vec![23, 25], b'T'),
    ] {
        client.parse(name, sql, &supplied).await;
        let mut description = vec![b'S']; description.extend_from_slice(&cstring(name));
        client.send(b'D', &description).await; client.send(b'S', &[]).await;
        let frames = client.through(b'Z').await;
        assert_eq!(tags(&frames), vec![b'1', b't', result, b'Z'], "{name}");
        let mut bytes = (expected.len() as u16).to_be_bytes().to_vec();
        for oid in expected { bytes.extend_from_slice(&i32::to_be_bytes(oid)); }
        assert_eq!(frames[1].1, bytes, "{name}");
    }
    drop(client); running.stop().await;
}

#[tokio::test]
async fn dml_missing_sql_arguments_reach_commit_after_successful_bind_and_execute() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    assert_eq!(tags(&client.query("INSERT INTO docs (_id,v) VALUES (1,10)").await), b"CZ");
    assert_eq!(tags(&client.query("BEGIN READ WRITE").await), b"CZ");
    client.parse("missing", "UPDATE docs SET v=$1", &[]).await;
    client.bind("missing", "missing").await; client.execute("missing", 0).await; client.send(b'S', &[]).await;
    let buffered = client.through(b'Z').await;
    assert_eq!(tags(&buffered), b"12CZ");
    assert_eq!(buffered.last().unwrap().1, b"T");
    let commit = client.query("COMMIT").await;
    assert_eq!(tags(&commit), b"EZ");
    assert!(String::from_utf8_lossy(&commit[0].1).contains("Arguments list was expected but not provided"));
    assert_eq!(rows(&client.query("SELECT v FROM docs").await), vec![vec![Some(b"10".to_vec())]]);
    drop(client); running.stop().await;
}

#[tokio::test]
async fn sql_execute_binds_derived_dml_arguments_without_wire_oid_descriptors() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    assert_eq!(tags(&client.query("PREPARE insert_doc AS INSERT INTO docs (_id,v) VALUES ($1,$2)").await), b"CZ");
    assert_eq!(tags(&client.query("EXECUTE insert_doc(1,42)").await), b"CZ");
    assert_eq!(rows(&client.query("SELECT _id,v FROM docs").await), vec![vec![Some(b"1".to_vec()), Some(b"42".to_vec())]]);
    drop(client); running.stop().await;
}

#[tokio::test]
async fn unprepared_controls_consume_actual_arguments_independently_of_description_width() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    client.parse("zone", "SET TIME ZONE 'UTC'", &[25]).await;
    client.bind("zone", "zone").await; client.execute("zone", 0).await; client.send(b'S', &[]).await;
    let frames = client.through(b'Z').await;
    assert_eq!(tags(&frames), b"12SCZ");
    client.parse("show", "SHOW TIME ZONE", &[]).await;
    let mut bind = cstring("show"); bind.extend_from_slice(&cstring("show"));
    bind.extend_from_slice(&0u16.to_be_bytes()); bind.extend_from_slice(&1u16.to_be_bytes()); bind.extend_from_slice(&(-1i32).to_be_bytes()); bind.extend_from_slice(&0u16.to_be_bytes());
    client.send(b'B', &bind).await; client.execute("show", 0).await; client.send(b'S', &[]).await;
    let frames = client.through(b'Z').await;
    assert_eq!(tags(&frames), b"12DCZ");
    assert_eq!(rows(&frames), vec![vec![Some(b"UTC".to_vec())]]);
    drop(client); running.stop().await;
}

fn arrow_copy_bytes(file: bool, offset: i64) -> Vec<u8> {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    let batch = |id, value| RecordBatch::try_from_iter(vec![("_id", Arc::new(Int64Array::from(vec![id])) as ArrayRef), ("n", Arc::new(Int64Array::from(vec![value])) as ArrayRef)]).unwrap();
    let first = batch(1 + offset, 10); let second = batch(2 + offset, 20); let mut bytes = Vec::new();
    if file { let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut bytes, first.schema().as_ref()).unwrap(); writer.write(&first).unwrap(); writer.write(&second).unwrap(); writer.finish().unwrap(); }
    else { let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, first.schema().as_ref()).unwrap(); writer.write(&first).unwrap(); writer.write(&second).unwrap(); writer.finish().unwrap(); }
    bytes
}

#[tokio::test]
async fn arrow_copy_unsigned_ids_and_values_enter_signed_execution() {
    use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    let batch = RecordBatch::try_from_iter(vec![("_id", Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef), ("v", Arc::new(UInt64Array::from(vec![1 << 63])) as ArrayRef)]).unwrap();
    for (table, file) in [("unsigned_stream", false), ("unsigned_file", true)] {
        let bytes = if file { rtdb_format::ipc::write_file(batch.schema().as_ref(), std::slice::from_ref(&batch)).unwrap() } else { rtdb_format::ipc::write_stream(batch.schema().as_ref(), std::slice::from_ref(&batch)).unwrap() };
        client.send(b'Q', &cstring(&format!("COPY {table} FROM STDIN WITH (FORMAT '{}')", if file { "arrow-file" } else { "arrow-stream" }))).await;
        assert_eq!(client.receive().await.0, b'G');
        client.send(b'd', &bytes).await; client.send(b'c', &[]).await;
        assert_eq!(tags(&client.through(b'Z').await), b"CZ");
        let frames = client.query(&format!("SELECT _id,v FROM {table}")).await;
        assert_eq!(rows(&frames), vec![vec![Some(b"-1".to_vec()), Some(i64::MIN.to_string().into_bytes())]]);
    }
    drop(client); running.stop().await;
}

#[tokio::test]
async fn arrow_copy_batches_autocommit_separately_and_share_an_explicit_transaction() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    for (table, file) in [("stream_docs", false), ("file_docs", true)] {
        client.send(b'Q', &cstring(&format!("COPY {table} FROM STDIN WITH (FORMAT '{}')", if file { "arrow-file" } else { "arrow-stream" }))).await;
        assert_eq!(client.receive().await, (b'G', vec![1, 0, 1, 0, 1]));
        for chunk in arrow_copy_bytes(file, 0).chunks(11) { client.send(b'd', chunk).await; }
        client.send(b'c', &[]).await; let copied = client.through(b'Z').await;
        assert_eq!(tags(&copied), b"CZ"); assert_eq!(copied[0].1, b"COPY 2\0");
        let frames = client.query(&format!("SELECT n FROM {table} ORDER BY _id")).await;
        assert_eq!(rows(&frames), vec![vec![Some(b"10".to_vec())], vec![Some(b"20".to_vec())]]);
        let times = rows(&client.query(&format!("SELECT _valid_from FROM {table} ORDER BY _id")).await);
        assert_ne!(times[0], times[1]);
    }
    assert_eq!(tags(&client.query("BEGIN READ WRITE").await), b"CZ");
    client.send(b'Q', &cstring("COPY stream_docs FROM STDIN WITH (FORMAT 'arrow-stream')")).await; assert_eq!(client.receive().await.0, b'G');
    client.send(b'd', &arrow_copy_bytes(false, 2)).await; client.send(b'c', &[]).await;
    assert_eq!(tags(&client.through(b'Z').await), b"CZ"); assert_eq!(tags(&client.query("ROLLBACK").await), b"CZ");
    let frames = client.query("SELECT n FROM stream_docs ORDER BY _id").await; assert_eq!(tags(&frames), b"TDDCZ"); assert_eq!(rows(&frames).len(), 2);
    assert_eq!(tags(&client.query("BEGIN READ WRITE").await), b"CZ");
    client.send(b'Q', &cstring("COPY stream_docs FROM STDIN WITH (FORMAT 'arrow-stream')")).await;
    assert_eq!(client.receive().await.0, b'G');
    client.send(b'd', &arrow_copy_bytes(false, 2)).await;
    client.send(b'c', &[]).await;
    assert_eq!(tags(&client.through(b'Z').await), b"CZ");
    assert_eq!(tags(&client.query("COMMIT").await), b"CZ");
    let times = rows(&client.query("SELECT _valid_from FROM stream_docs ORDER BY _id").await);
    assert_eq!(times.len(), 4);
    assert_eq!(times[2], times[3]);
    drop(client); running.stop().await;
}

#[derive(Debug)]
struct PinnedCertificate(tokio_rustls::rustls::pki_types::CertificateDer<'static>);
impl tokio_rustls::rustls::client::danger::ServerCertVerifier for PinnedCertificate {
    fn verify_server_cert(&self, certificate: &tokio_rustls::rustls::pki_types::CertificateDer<'_>, _: &[tokio_rustls::rustls::pki_types::CertificateDer<'_>], _: &tokio_rustls::rustls::pki_types::ServerName<'_>, _: &[u8], _: tokio_rustls::rustls::pki_types::UnixTime) -> std::result::Result<tokio_rustls::rustls::client::danger::ServerCertVerified, tokio_rustls::rustls::Error> {
        if certificate.as_ref() != self.0.as_ref() { return Err(tokio_rustls::rustls::Error::General("Unexpected certificate".into())); }
        Ok(tokio_rustls::rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(&self, message: &[u8], certificate: &tokio_rustls::rustls::pki_types::CertificateDer<'_>, signature: &tokio_rustls::rustls::DigitallySignedStruct) -> std::result::Result<tokio_rustls::rustls::client::danger::HandshakeSignatureValid, tokio_rustls::rustls::Error> {
        tokio_rustls::rustls::crypto::verify_tls12_signature(message, certificate, signature, &tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms)
    }
    fn verify_tls13_signature(&self, message: &[u8], certificate: &tokio_rustls::rustls::pki_types::CertificateDer<'_>, signature: &tokio_rustls::rustls::DigitallySignedStruct) -> std::result::Result<tokio_rustls::rustls::client::danger::HandshakeSignatureValid, tokio_rustls::rustls::Error> {
        tokio_rustls::rustls::crypto::verify_tls13_signature(message, certificate, signature, &tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms)
    }
    fn supported_verify_schemes(&self) -> Vec<tokio_rustls::rustls::SignatureScheme> { tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms.supported_schemes() }
}

#[tokio::test]
async fn ssl_upgrade_authenticates_and_queries_over_the_owned_tls_transport() {
    use tokio_rustls::rustls::{self, pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject}};
    let certificate = CertificateDer::from_pem_slice(include_bytes!("../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.crt")).unwrap();
    let key = PrivateKeyDer::from_pem_slice(include_bytes!("../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.key")).unwrap();
    let tls = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider())).with_safe_default_protocol_versions().unwrap().with_no_client_auth().with_single_cert(vec![certificate.clone()], key).unwrap();
    let running = Running::start(ServerConfig { tls: Some(Arc::new(tls)), ..Default::default() }, None).await;
    let client = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider())).with_safe_default_protocol_versions().unwrap().dangerous().with_custom_certificate_verifier(Arc::new(PinnedCertificate(certificate))).with_no_client_auth();
    let mut socket = TcpStream::connect(running.server.address()).await.unwrap();
    socket.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).await.unwrap(); assert_eq!(socket.read_u8().await.unwrap(), b'S');
    let mut socket = tokio_rustls::TlsConnector::from(Arc::new(client)).connect(ServerName::try_from("xtdb.example.com").unwrap(), socket).await.unwrap();
    let mut startup = 196608i32.to_be_bytes().to_vec(); startup.extend_from_slice(b"user\0xtdb\0database\0xtdb\0\0");
    socket.write_i32(startup.len() as i32 + 4).await.unwrap(); socket.write_all(&startup).await.unwrap();
    async fn receive<S: tokio::io::AsyncRead + Unpin>(socket: &mut S) -> (u8, Vec<u8>) { let tag = socket.read_u8().await.unwrap(); let length = socket.read_i32().await.unwrap(); let mut bytes = vec![0; (length - 4) as usize]; socket.read_exact(&mut bytes).await.unwrap(); (tag, bytes) }
    tokio::time::timeout(Duration::from_secs(10), async {
        loop { let frame = receive(&mut socket).await; assert_ne!(frame.0, b'E'); if frame.0 == b'Z' { break; } }
        let query = cstring("SELECT 42 AS n"); socket.write_u8(b'Q').await.unwrap(); socket.write_i32(query.len() as i32 + 4).await.unwrap(); socket.write_all(&query).await.unwrap(); socket.flush().await.unwrap();
        let mut frames = Vec::new(); loop { let frame = receive(&mut socket).await; let ready = frame.0 == b'Z'; frames.push(frame); if ready { break; } }
        assert_eq!(tags(&frames), b"TDCZ"); assert_eq!(rows(&frames), vec![vec![Some(b"42".to_vec())]]);
    }).await.unwrap();
    drop(socket); running.stop().await;
}

#[tokio::test]
async fn simple_batch_lexical_failure_precedes_session_effects_and_recovers() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    assert_eq!(tags(&client.query("SET application_name = 'before'").await), b"SCZ");
    let frames = client.query("SET application_name = 'changed'; SELECT @").await;
    assert_eq!(tags(&frames), b"EZ");
    assert_eq!(frames[1].1, b"I");
    let fields = frames[0].1.split(|&byte| byte == 0).filter_map(|field| field.split_first()).collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(std::str::from_utf8(fields[&b'M']).unwrap(), "Invalid character in SQL source");
    let detail: serde_json::Value = serde_json::from_slice(fields[&b'D']).unwrap();
    assert_eq!(detail["code"], "xtdb/sql-error");
    let unchanged = client.query("SHOW application_name").await;
    assert_eq!(tags(&unchanged), b"TDCZ");
    assert_eq!(rows(&unchanged), vec![vec![Some(b"before".to_vec())]]);
    let recovered = client.query("SELECT 42 AS answer").await;
    assert_eq!(tags(&recovered), b"TDCZ");
    assert_eq!(rows(&recovered), vec![vec![Some(b"42".to_vec())]]);
    assert_eq!(recovered.last().unwrap().1, b"I");
    drop(client); running.stop().await;
}

#[tokio::test]
async fn simple_comments_separators_and_canned_keyword_results_keep_protocol_shapes() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    let empty = client.query(" -- idle\n -- still idle ; ;").await;
    assert_eq!(tags(&empty), b"IZ");
    assert!(empty[0].1.is_empty());
    assert_eq!(empty[1].1, b"I");
    let block = client.query(" -- idle\n /* ; */ ; ;").await;
    assert_eq!(tags(&block), b"EZ");
    assert_eq!(block[1].1, b"I");
    let semicolon = client.query("; SELECT string_agg(word, ',') FROM pg_catalog.pg_get_keywords(); ;").await;
    assert_eq!(semicolon, vec![(b'n', vec![]), (b'C', b" 0\0".to_vec()), (b'Z', b"I".to_vec())]);
    let canned = client.query("SELECT string_agg(word, ',') FROM pg_catalog.pg_get_keywords()").await;
    assert_eq!(tags(&canned), b"nDCZ");
    assert!(canned[0].1.is_empty());
    assert_eq!(rows(&canned), vec![vec![Some(b"xtdb".to_vec())]]);
    assert_eq!(canned[2].1, b"SELECT 1\0");
    assert_eq!(canned[3].1, b"I");
    drop(client); running.stop().await;
}

#[tokio::test]
async fn simple_batch_rejects_sql_grammar_before_session_effects() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    for sql in [
        "SET TIME ZONE 'America/New_York'; SELEC 7",
        "SET TIME ZONE 'America/New_York'; BEGIN READ ONLY, CLOCK_TIME = )",
        "SET TIME ZONE 'America/New_York'; PREPARE p AS PREPARE q AS SELEC 7",
        "SET TIME ZONE 'America/New_York'; PREPARE BEGIN AS SELECT 7",
        "SET TIME ZONE 'America/New_York'; PREPARE p AS PREPARE BEGIN AS SELECT 7",
        "SET TIME ZONE 'America/New_York';; SELECT 7",
        "SET TIME ZONE 'America/New_York'; XTQL $$($$; SELEC 7",
    ] {
        let frames = client.query(sql).await;
        assert_eq!(tags(&frames), b"EZ", "{sql}");
        assert_eq!(frames[1].1, b"I");
        assert_eq!(rows(&client.query("SHOW TIME ZONE").await), [vec![Some(b"UTC".to_vec())]]);
    }
    for sql in ["SELECT 1;;", "SELECT 1; /* tail */"] {
        let frames = client.query(sql).await;
        assert_eq!(tags(&frames), b"TDCZ", "{sql}");
        assert_eq!(rows(&frames), [vec![Some(b"1".to_vec())]]);
    }
    for sql in ["SELECT 1;;;", "SELECT 1;; /* tail */"] {
        assert_eq!(tags(&client.query(sql).await), b"EZ", "{sql}");
    }
    running.stop().await;
}

#[tokio::test]
async fn simple_batch_binds_each_statement_and_continues_after_reached_errors() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    for source in [
        "SET TIME ZONE 'America/New_York'; XTQL $$($$; SELECT 7",
        "SET TIME ZONE 'America/New_York'; PREPARE p AS SELECT unknown_combo(); SELECT 7",
    ] {
        let frames = client.query(source).await;
        assert_eq!(tags(&frames), b"SCETDCZ", "{source}");
        assert_eq!(rows(&frames), [vec![Some(b"7".to_vec())]]);
        assert_eq!(rows(&client.query("SHOW TIME ZONE").await), [vec![Some(b"UTC".to_vec())]]);
    }
    let frames = client.query("PREPARE first_query AS SELECT 7; EXECUTE first_query;").await;
    assert_eq!(tags(&frames), b"CTDCZ");
    assert_eq!(rows(&frames), [vec![Some(b"7".to_vec())]]);
    for name in ["\"BEGIN\"", "SELECT"] {
        let frames = client.query(&format!("PREPARE {name} AS SELECT 5; EXECUTE {name};")).await;
        assert_eq!(tags(&frames), b"CTDCZ", "{name}");
        assert_eq!(rows(&frames), [vec![Some(b"5".to_vec())]]);
    }
    let frames = client.query("PREPARE outer_q AS PREPARE inner_q AS SELECT 5; EXECUTE outer_q;").await;
    assert_eq!(tags(&frames), b"CEZ");
    let message = frames[1].1.split(|&byte| byte == 0).find_map(|field| field.strip_prefix(b"M"));
    assert_eq!(message, Some(b"EXECUTE only supports a prepared query or DML statement".as_slice()));
    running.stop().await;
}

#[tokio::test]
async fn whole_simple_markers_bypass_tokenization_and_preserve_transaction_status() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    assert_eq!(tags(&client.query("SET application_name = 'before'").await), b"SCZ");

    let empty = client.query("\u{1c}\u{2003}-- idle\r -- end").await;
    assert_eq!(empty, vec![(b'I', vec![]), (b'Z', b"I".to_vec())]);
    let semicolon = client.query("\u{1c} ; SET application_name = 'changed'; SELECT @").await;
    assert_eq!(semicolon, vec![(b'n', vec![]), (b'C', b" 0\0".to_vec()), (b'Z', b"I".to_vec())]);
    assert_eq!(rows(&client.query("SHOW application_name").await), vec![vec![Some(b"before".to_vec())]]);

    let begun = client.query("BEGIN").await;
    assert_eq!(tags(&begun), b"CZ");
    assert_eq!(begun[1].1, b"T");
    let semicolon = client.query(";;").await;
    assert_eq!(semicolon, vec![(b'n', vec![]), (b'C', b" 0\0".to_vec()), (b'Z', b"T".to_vec())]);
    let failed = client.query("BEGIN").await;
    assert_eq!(tags(&failed), b"EZ");
    assert_eq!(failed[1].1, b"E");
    let semicolon = client.query("; SELECT @").await;
    assert_eq!(semicolon, vec![(b'n', vec![]), (b'C', b" 0\0".to_vec()), (b'Z', b"E".to_vec())]);
    let empty = client.query(";\u{b}\u{2028}").await;
    assert_eq!(empty, vec![(b'I', vec![]), (b'Z', b"E".to_vec())]);
    let rolled_back = client.query("ROLLBACK").await;
    assert_eq!(tags(&rolled_back), b"CZ");
    assert_eq!(rolled_back[1].1, b"I");
    drop(client); running.stop().await;
}

#[tokio::test]
async fn dbeaver_namespace_projection_is_rewritten_only_for_the_whole_simple_message() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    let query = "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass ORDER BY nspname";
    let normalized = format!("\u{1c}\u{2003}{query};\t\r\n");
    for source in [query, normalized.as_str()] {
        let frames = client.query(source).await;
        assert_eq!(tags(&frames), b"TDDDDCZ");
        let (count, mut fields) = frames[0].1.split_at(2);
        assert_eq!(count, &5u16.to_be_bytes());
        for name in ["oid", "nspname", "nspowner", "nspacl", "description"] {
            let end = fields.iter().position(|&byte| byte == 0).unwrap();
            assert_eq!(&fields[..end], name.as_bytes());
            fields = &fields[end + 19..];
        }
        assert!(fields.is_empty());
        let rows = rows(&frames);
        for (row, name) in rows.iter().zip(["information_schema", "pg_catalog", "public", "xt"]) {
            assert_eq!(row.len(), 5);
            assert_eq!(row[1].as_deref(), Some(name.as_bytes()));
            assert!(row[3].is_none() && row[4].is_none());
        }
        assert_eq!(frames[5].1, b"SELECT 4\0");
        assert_eq!(frames[6].1, b"I");
    }

    let frames = client.query(&format!("SELECT 42 AS answer; {query}")).await;
    assert_eq!(tags(&frames), b"TDCEZ");
    assert_eq!(rows(&frames), vec![vec![Some(b"42".to_vec())]]);
    let message = frames[3].1.split(|&byte| byte == 0).find_map(|field| field.strip_prefix(b"M"));
    assert_eq!(message, Some(b"Duplicate column projection: oid".as_slice()));
    assert_eq!(frames[4].1, b"I");
    drop(client); running.stop().await;
}

#[tokio::test]
async fn extended_namespace_rewrite_preserves_the_registration_after_a_case_mismatch() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    let query = "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass ORDER BY nspname";
    let describe = b"Snamespace\0";
    client.parse("namespace", query, &[]).await;
    client.send(b'D', describe).await;
    client.send(b'S', &[]).await;
    let prepared = client.through(b'Z').await;
    assert_eq!(tags(&prepared), b"1tTZ");
    assert_eq!(prepared[1].1, b"\0\0");
    assert_eq!(prepared[3].1, b"I");
    let (count, mut fields) = prepared[2].1.split_at(2);
    assert_eq!(count, &5u16.to_be_bytes());
    for name in ["oid", "nspname", "nspowner", "nspacl", "description"] {
        let end = fields.iter().position(|&byte| byte == 0).unwrap();
        assert_eq!(&fields[..end], name.as_bytes());
        fields = &fields[end + 19..];
    }
    assert!(fields.is_empty());

    client.parse("namespace", &query.to_ascii_lowercase(), &[]).await;
    client.send(b'S', &[]).await;
    let rejected = client.through(b'Z').await;
    assert_eq!(tags(&rejected), b"EZ");
    let message = rejected[0].1.split(|&byte| byte == 0).find_map(|field| field.strip_prefix(b"M"));
    assert_eq!(message, Some(b"Duplicate column projection: oid".as_slice()));
    assert_eq!(rejected[1].1, b"I");

    client.send(b'D', describe).await;
    client.send(b'S', &[]).await;
    let retained = client.through(b'Z').await;
    assert_eq!(retained.as_slice(), &prepared[1..]);
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn whole_simple_keyword_marker_ignores_its_suffix_and_preserves_explicit_status() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    assert_eq!(tags(&client.query("SET application_name = 'before'").await), b"SCZ");
    let prefix = "\u{1c}SeLECt StriNG_AGG(word, ',') FROM pg_catalog.pg_get_\u{212a}EYWORDS()";
    for source in [
        format!("{prefix}; SET application_name = 'changed'; SELECT @"),
        format!("{prefix};\u{2028}"),
    ] {
        let marker = client.query(&source).await;
        assert_eq!(tags(&marker), b"nDCZ");
        assert!(marker[0].1.is_empty());
        assert_eq!(rows(&marker), vec![vec![Some(b"xtdb".to_vec())]]);
        assert_eq!(marker[2].1, b"SELECT 1\0");
        assert_eq!(marker[3].1, b"I");
    }
    assert_eq!(rows(&client.query("SHOW application_name").await), vec![vec![Some(b"before".to_vec())]]);

    let begun = client.query("BEGIN").await;
    assert_eq!(tags(&begun), b"CZ");
    assert_eq!(begun[1].1, b"T");
    let failed = client.query("BEGIN").await;
    assert_eq!(tags(&failed), b"EZ");
    assert_eq!(failed[1].1, b"E");
    let marker = client.query(prefix).await;
    assert_eq!(tags(&marker), b"nDCZ");
    assert!(marker[0].1.is_empty());
    assert_eq!(rows(&marker), vec![vec![Some(b"xtdb".to_vec())]]);
    assert_eq!(marker[2].1, b"SELECT 1\0");
    assert_eq!(marker[3].1, b"E");
    let rolled_back = client.query("ROLLBACK").await;
    assert_eq!(tags(&rolled_back), b"CZ");
    assert_eq!(rolled_back[1].1, b"I");
    drop(client); running.stop().await;
}

#[tokio::test]
async fn later_keyword_text_in_a_simple_batch_reaches_ordinary_table_binding() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    let frames = client.query("SELECT 42 AS answer; SELECT string_agg(word, ',') FROM pg_catalog.pg_get_keywords()").await;
    assert_eq!(tags(&frames), b"TDCEZ");
    assert_eq!(rows(&frames), vec![vec![Some(b"42".to_vec())]]);
    let fields = frames[3].1.split(|&byte| byte == 0).filter_map(|field| field.split_first()).collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(fields[&b'M'], b"Unknown function: pg_get_keywords");
    let detail: serde_json::Value = serde_json::from_slice(fields[&b'D']).unwrap();
    assert_eq!(detail["code"], "xtdb/sql-error");
    assert_eq!(frames[4].1, b"I");
    drop(client); running.stop().await;
}

#[tokio::test]
async fn locale_dependent_simple_and_extended_keyword_responses_keep_their_descriptions() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    let locale_dependent = client.query("SELECT STRING_AGG(word, ',') FROM pg_catalog.pg_get_keywords()").await;
    assert_eq!(tags(&locale_dependent), b"TDCZ");
    assert_eq!(rows(&locale_dependent), vec![vec![Some(b"xtdb".to_vec())]]);
    assert_eq!(locale_dependent[2].1, b"SELECT 1\0");
    assert_eq!(locale_dependent[3].1, b"I");

    client.parse("keywords", "SELECT string_agg(word, ',') FROM pg_catalog.pg_get_keywords()", &[]).await;
    let mut describe = vec![b'S'];
    describe.extend_from_slice(&cstring("keywords"));
    client.send(b'D', &describe).await;
    client.bind("", "keywords").await;
    client.execute("", 0).await;
    client.send(b'S', &[]).await;
    let extended = client.through(b'Z').await;
    assert_eq!(tags(&extended), b"1T2DCZ");
    let columns = &extended[1].1;
    assert_eq!(&columns[..7], b"\0\x01col1\0");
    assert_eq!(i32::from_be_bytes(columns[13..17].try_into().unwrap()), 1043);
    assert_eq!(rows(&extended), vec![vec![Some(b"xtdb".to_vec())]]);
    assert_eq!(extended[4].1, b"SELECT 1\0");
    assert_eq!(extended[5].1, b"I");
    drop(client); running.stop().await;
}

#[tokio::test]
async fn simple_batch_grammar_errors_preserve_transactions_and_reached_errors_fail_them() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    let source = "-- début\r\nSELEC 42 /* tail */ ;  ";

    let idle = client.query(source).await;
    assert_eq!(tags(&idle), b"EZ");
    assert_eq!(idle[1].1, b"I");
    let (state, message, detail) = error_fields(&idle[0].1);
    assert_eq!(state, "08P01");
    assert_eq!(message, initial_statement_mismatch(2, 0));
    assert_eq!(detail, serde_json::json!({"category": "incorrect", "code": "xtdb/sql-error", "message": message, "data": {"sql": source}}));

    let begun = client.query("BEGIN").await;
    assert_eq!(tags(&begun), b"CZ");
    assert_eq!(begun[1].1, b"T");
    let malformed = client.query(source).await;
    assert_eq!(tags(&malformed), b"EZ");
    assert_eq!(malformed[0], idle[0]);
    assert_eq!(malformed[1].1, b"T");
    let valid = client.query("SELECT 42").await;
    assert_eq!(tags(&valid), b"TDCZ");
    assert_eq!(rows(&valid), [vec![Some(b"42".to_vec())]]);
    assert_eq!(valid[3].1, b"T");
    assert_eq!(client.query("ROLLBACK").await.last().unwrap().1, b"I");

    let error_detail = |body: &[u8]| {
        let detail = body.split(|byte| *byte == 0).find_map(|field| field.strip_prefix(b"D")).unwrap();
        serde_json::from_slice::<serde_json::Value>(detail).unwrap()
    };
    for (sql, code) in [("XTQL $$[$$", "xtql/edn-reader"), ("SELECT $0", "xtdb/sql-error")] {
        assert_eq!(client.query("BEGIN").await.last().unwrap().1, b"T");
        let failed = client.query(sql).await;
        assert_eq!(tags(&failed), b"EZ");
        assert_eq!(failed[1].1, b"E");
        assert_eq!(error_detail(&failed[0].1)["code"], code);
        let context = client.query("SELECT 42").await;
        assert_eq!(tags(&context), b"EZ");
        assert_eq!(context[1].1, b"E");
        assert_eq!(error_detail(&context[0].1)["code"], "xtdb/tx-failed");
        assert_eq!(client.query("ROLLBACK").await.last().unwrap().1, b"I");
    }
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn single_range_grammar_error_keeps_an_extended_transaction_open_until_sync() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    client.parse("scalar", "SELECT 42", &[]).await;
    client.bind("scalar", "scalar").await;
    client.execute("scalar", 0).await;
    client.send(b'H', &[]).await;
    let extended = client.through(b'C').await;
    assert_eq!(tags(&extended), b"12DC");
    assert_eq!(rows(&extended), [vec![Some(b"42".to_vec())]]);

    let malformed = client.query("SELEC 42").await;
    assert_eq!(tags(&malformed), b"EZ");
    assert_eq!(malformed[1].1, b"T");

    client.send(b'S', &[]).await;
    assert_eq!(client.through(b'Z').await, vec![(b'Z', b"I".to_vec())]);
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn simple_batch_reports_the_first_statement_grammar_error_after_lexing() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    let limit_all = "Errors parsing SQL statement:\n  - line 1:15 mismatched input 'ALL' expecting {UNSIGNED_INTEGER, POSTGRES_PARAMETER_SPECIFICATION, '?'}".to_owned();
    for (source, expected) in [
        ("SELEC 7; SELECT 1 LIMIT ALL", initial_statement_mismatch(1, 0)),
        ("SELECT 1 LIMIT ALL; SELEC 7", limit_all),
        ("SELEC 7; SELECT @", initial_statement_mismatch(1, 0)),
    ] {
        let frames = client.query(source).await;
        assert_eq!(tags(&frames), b"EZ");
        assert_eq!(frames[1].1, b"I");
        let (state, message, detail) = error_fields(&frames[0].1);
        assert_eq!(state, "08P01");
        assert_eq!(message, expected);
        assert_eq!(detail, serde_json::json!({"category": "incorrect", "code": "xtdb/sql-error", "message": expected, "data": {"sql": source}}));
    }
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn locale_dependent_keyword_responses_bypass_only_matching_range_grammar() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    let prefix = "SELECT STRING_AGG(word, ',') FROM pg_catalog.pg_get_keywords()";
    let lowercase = prefix.to_ascii_lowercase();

    let canned = client.query(&format!("{prefix}) LIMIT ALL")).await;
    assert_eq!(tags(&canned), b"TDCZ");
    assert_eq!(rows(&canned), [vec![Some(b"xtdb".to_vec())]]);
    assert_eq!(canned[2].1, b"SELECT 1\0");
    assert_eq!(canned[3].1, b"I");

    let mixed = client.query(&format!("{prefix}); SELECT 42; {lowercase})")).await;
    assert_eq!(tags(&mixed), b"TDCTDCTDCZ");
    assert_eq!(rows(&mixed), [vec![Some(b"xtdb".to_vec())], vec![Some(b"42".to_vec())], vec![Some(b"xtdb".to_vec())]]);
    assert_eq!(mixed[9].1, b"I");

    let ordinary = client.query(&format!("SELECT 42; {prefix})")).await;
    assert_eq!(tags(&ordinary), b"EZ");
    assert_eq!(ordinary[1].1, b"I");

    assert_eq!(tags(&client.query("SET application_name = 'before'").await), b"SCZ");
    for (suffix, message) in [("SELEC 7", "SELEC"), ("SELECT @", "Invalid character in SQL source")] {
        let source = format!("{prefix}); SET application_name = 'changed'; {suffix}");
        let failed = client.query(&source).await;
        assert_eq!(tags(&failed), b"EZ");
        assert_eq!(failed[1].1, b"I");
        let detail = failed[0].1.split(|byte| *byte == 0).find_map(|field| field.strip_prefix(b"D")).unwrap();
        let anomaly: serde_json::Value = serde_json::from_slice(detail).unwrap();
        assert_eq!(anomaly["code"], "xtdb/sql-error");
        assert!(anomaly["message"].as_str().unwrap().contains(message));
        let offset = if suffix == "SELECT @" { source.find('@').unwrap() } else { source.len() - suffix.len() };
        assert_eq!(anomaly["data"]["position"], serde_json::json!(offset + 1));
        assert_eq!(rows(&client.query("SHOW application_name").await), [vec![Some(b"before".to_vec())]]);
    }
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn extended_client_probes_keep_emulation_and_sql_prepare_children_use_ordinary_sql() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    let keywords = "SELECT string_agg(word, ',') FROM pg_catalog.pg_get_keywords()";
    let namespace = "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass ORDER BY nspname";
    let cases = [
        (keywords, b"1TZ".as_slice(), ["col1"].as_slice(), "xtdb/sql-error", "Unknown function: pg_get_keywords"),
        (namespace, b"1tTZ".as_slice(), ["oid", "nspname", "nspowner", "nspacl", "description"].as_slice(), "xtdb/sql-error", "Duplicate column projection: oid"),
    ];
    let assert_prepare_error = |frames: &[(u8, Vec<u8>)], expected_tags: &[u8], code: &str, message: &str| {
        assert_eq!(tags(frames), expected_tags);
        assert_eq!(frames.last().unwrap().1, b"I");
        let body = &frames[frames.len() - 2].1;
        let detail = body.split(|&byte| byte == 0).find_map(|field| field.strip_prefix(b"D")).unwrap();
        let anomaly: serde_json::Value = serde_json::from_slice(detail).unwrap();
        assert_eq!(anomaly["code"], code);
        assert_eq!(anomaly["message"], message);
    };

    for (index, (probe, description_tags, columns, code, message)) in cases.into_iter().enumerate() {
        let direct = format!("probe_{index}");
        client.parse(&direct, probe, &[]).await;
        let mut describe = vec![b'S'];
        describe.extend_from_slice(&cstring(&direct));
        client.send(b'D', &describe).await;
        client.send(b'S', &[]).await;
        let description = client.through(b'Z').await;
        assert_eq!(tags(&description), description_tags);
        assert_eq!(description.last().unwrap().1, b"I");
        let (count, mut fields) = description[description.len() - 2].1.split_at(2);
        assert_eq!(count, &u16::try_from(columns.len()).unwrap().to_be_bytes());
        for name in columns {
            let end = fields.iter().position(|&byte| byte == 0).unwrap();
            assert_eq!(&fields[..end], name.as_bytes());
            fields = &fields[end + 19..];
        }
        assert!(fields.is_empty());

        let statement = format!("prepare_{index}");
        let portal = format!("direct_portal_{index}");
        client.parse(&statement, &format!("PREPARE child_{index} AS {probe}"), &[]).await;
        client.bind(&portal, &statement).await;
        client.execute(&portal, 0).await;
        client.send(b'S', &[]).await;
        assert_prepare_error(&client.through(b'Z').await, b"12EZ", code, message);

        let outer = format!("outer_{index}");
        let portal = format!("nested_portal_{index}");
        let prepared = client.query(&format!("PREPARE {outer} AS PREPARE inner_{index} AS {probe}")).await;
        assert_eq!(prepared, vec![(b'C', b"PREPARE\0".to_vec()), (b'Z', b"I".to_vec())]);
        client.bind(&portal, &outer).await;
        client.execute(&portal, 0).await;
        client.send(b'S', &[]).await;
        assert_prepare_error(&client.through(b'Z').await, b"2EZ", code, message);
    }
    drop(client);
    running.stop().await;
}

fn prepare_error_detail(frame: &(u8, Vec<u8>)) -> serde_json::Value {
    assert_eq!(frame.0, b'E');
    let detail = frame.1.split(|&byte| byte == 0).find_map(|field| field.strip_prefix(b"D")).unwrap();
    serde_json::from_slice(detail).unwrap()
}

#[tokio::test]
async fn simple_query_reports_initial_mismatch_and_unwanted_token_with_the_received_sql() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    for (sql, message) in [
        ("SELEC 1", "line 1:0 mismatched input 'SELEC'"),
        ("  -- comment\n extra SELECT 1 ;", "line 2:1 extraneous input 'extra'"),
    ] {
        let frames = client.query(sql).await;
        assert_eq!(tags(&frames), b"EZ");
        assert_eq!(frames[1].1, b"I");
        assert!(frames[0].1.split(|&byte| byte == 0).any(|field| field == b"C08P01"));
        let detail = prepare_error_detail(&frames[0]);
        assert_eq!(detail["code"], "xtdb/sql-error");
        assert_eq!(detail["data"], serde_json::json!({"sql": sql}));
        let prefix = format!("Errors parsing SQL statement:\n  - {message} expecting ");
        assert!(detail["message"].as_str().unwrap().starts_with(&prefix));
    }
    assert_eq!(rows(&client.query("SELECT 42::bigint AS answer").await), [vec![Some(b"42".to_vec())]]);
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn extended_control_errors_preserve_coordinates_and_the_registered_statement() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    client.parse("zone", "SET TIME ZONE 'Europe/Paris'", &[]).await;
    client.send(b'S', &[]).await;
    assert_eq!(tags(&client.through(b'Z').await), b"1Z");

    let bounded = "SET TIME ZONE /* café */ 'UTC' TRAILING";
    let control_error = format!("-- début\r\n; ; {bounded}; ;");
    let lexical_error = "-- début\r\n; SET TIME ZONE 'UTC'; SELECT @; ;";
    for (source, message, start, end) in [
        (control_error.as_str(), "Unexpected input after control statement", bounded.find("TRAILING").unwrap(), bounded.len()),
        (lexical_error, "Invalid character in SQL source", lexical_error.find('@').unwrap(), lexical_error.find('@').unwrap() + 1),
    ] {
        client.parse("zone", source, &[]).await;
        client.bind("", "zone").await;
        client.send(b'S', &[]).await;
        let rejected = client.through(b'Z').await;
        assert_eq!(tags(&rejected), b"EZ");
        assert_eq!(rejected[1].1, b"I");
        let detail = prepare_error_detail(&rejected[0]);
        assert_eq!(detail["code"], "xtdb/sql-error");
        assert_eq!(detail["message"], message);
        assert_eq!(detail["data"]["position"], serde_json::json!(start + 1));
        assert_eq!(detail["data"]["span"], serde_json::json!({"start": start, "end": end}));
    }

    client.bind("", "zone").await;
    client.execute("", 0).await;
    client.send(b'S', &[]).await;
    assert_eq!(tags(&client.through(b'Z').await), b"2SCZ");
    assert_eq!(rows(&client.query("SHOW TIME ZONE").await), [vec![Some(b"Europe/Paris".to_vec())]]);

    client.parse("zone", "SET TIME ZONE 'UTC'", &[]).await;
    client.bind("", "zone").await;
    client.execute("", 0).await;
    client.send(b'S', &[]).await;
    assert_eq!(tags(&client.through(b'Z').await), b"12SCZ");
    assert_eq!(rows(&client.query("SHOW TIME ZONE").await), [vec![Some(b"UTC".to_vec())]]);
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn extended_prepare_rejects_nested_grammar_before_acknowledgment_and_keeps_the_registration() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    client.parse("retained", "SELECT 42 AS n", &[]).await;
    client.send(b'S', &[]).await;
    assert_eq!(tags(&client.through(b'Z').await), b"1Z");

    let source = "-- début\r\nPREPARE parent AS PREPARE child AS SELEC 7";
    let start = source.find("SELEC").unwrap();
    for in_transaction in [false, true] {
        if in_transaction {
            assert_eq!(client.query("BEGIN READ WRITE").await.last().unwrap().1, b"T");
        }
        client.parse("retained", source, &[]).await;
        client.bind("skipped", "retained").await;
        client.send(b'S', &[]).await;
        let rejected = client.through(b'Z').await;
        assert_eq!(tags(&rejected), b"EZ");
        assert_eq!(rejected[1].1, if in_transaction { b"E" } else { b"I" });
        let error = prepare_error_detail(&rejected[0]);
        assert_eq!(error["code"], "xtdb/sql-error");
        assert!(error["message"].as_str().unwrap().contains("SELEC"));
        assert_eq!(error["data"]["position"], serde_json::json!(start + 1));
        assert_eq!(error["data"]["span"], serde_json::json!({"start": start, "end": start + 5}));
        if in_transaction {
            assert_eq!(client.query("ROLLBACK").await.last().unwrap().1, b"I");
        }
        client.bind("", "retained").await;
        client.execute("", 0).await;
        client.send(b'S', &[]).await;
        let retained = client.through(b'Z').await;
        assert_eq!(tags(&retained), b"2DCZ");
        assert_eq!(rows(&retained), [vec![Some(b"42".to_vec())]]);
    }
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn extended_prepare_reports_a_reached_leaf_error_at_its_original_input_span() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    let source = "-- début\r\nPREPARE parent AS PREPARE child AS\n  XTQL $$($$";
    client.parse("wire", source, &[23]).await;
    let mut describe = vec![b'S'];
    describe.extend_from_slice(&cstring("wire"));
    client.send(b'D', &describe).await;
    client.send(b'S', &[]).await;
    let parsed = client.through(b'Z').await;
    assert_eq!(tags(&parsed), b"1tnZ");
    assert_eq!(parsed[1].1, b"\0\x01\0\0\0\x17");
    assert_eq!(parsed[3].1, b"I");

    client.bind("", "wire").await;
    client.execute("", 0).await;
    client.send(b'S', &[]).await;
    let outer = client.through(b'Z').await;
    assert_eq!(tags(&outer), b"2CZ");
    assert_eq!(outer[1].1, b"PREPARE\0");
    assert_eq!(outer[2].1, b"I");

    client.bind("", "parent").await;
    client.execute("", 0).await;
    client.send(b'S', &[]).await;
    let reached = client.through(b'Z').await;
    assert_eq!(tags(&reached), b"2EZ");
    assert_eq!(reached[2].1, b"I");
    let start = source.find("$$").unwrap();
    let error = prepare_error_detail(&reached[1]);
    assert_eq!(error["code"], "xtql/edn-reader");
    assert_eq!(error["data"]["position"], serde_json::json!(start + 1));
    assert_eq!(error["data"]["span"], serde_json::json!({"start": start, "end": start + "$$($$".len()}));
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn extended_prepare_portals_keep_their_body_across_name_replacement_and_repeated_execution() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    for (portal, sql) in [
        ("first", "PREPARE first_child AS SELECT 7 AS n"),
        ("second", "PREPARE second_child AS SELECT 9 AS n"),
    ] {
        client.parse("wire", sql, &[]).await;
        client.bind(portal, "wire").await;
        client.send(b'S', &[]).await;
        assert_eq!(tags(&client.through(b'Z').await), b"12Z");
    }
    for portal in ["first", "second", "first"] {
        client.execute(portal, 0).await;
    }
    client.send(b'S', &[]).await;
    let installed = client.through(b'Z').await;
    assert_eq!(tags(&installed), b"CCCZ");
    assert!(installed[..3].iter().all(|(_, body)| body == b"PREPARE\0"));
    for (sql, value) in [("EXECUTE first_child", b"7"), ("EXECUTE second_child", b"9")] {
        let frames = client.query(sql).await;
        assert_eq!(tags(&frames), b"TDCZ");
        assert_eq!(rows(&frames), [vec![Some(value.to_vec())]]);
    }

    client.parse("zone", "PREPARE zone_child AS SET TIME ZONE 'America/New_York'", &[]).await;
    client.bind("", "zone").await;
    client.execute("", 0).await;
    client.send(b'S', &[]).await;
    assert_eq!(tags(&client.through(b'Z').await), b"12CZ");
    client.bind("", "zone_child").await;
    client.execute("", 0).await;
    client.send(b'S', &[]).await;
    assert_eq!(tags(&client.through(b'Z').await), b"2SCZ");
    assert_eq!(rows(&client.query("SHOW TIME ZONE").await), [vec![Some(b"America/New_York".to_vec())]]);
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn nested_simple_prepare_reports_a_later_execute_error_in_the_normalized_message() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    let normalized = "SELECT 'café';\r\n/* ☕ */ PREPARE err_root AS PREPARE err_leaf AS XTQL $$($$";
    let source = format!("\u{2003}{normalized}; ");
    let outer = client.query(&source).await;
    assert_eq!(tags(&outer), b"TDCCZ");
    assert_eq!(rows(&outer), [vec![Some("café".as_bytes().to_vec())]]);
    assert_eq!(outer[3].1, b"PREPARE\0");
    assert_eq!(outer[4].1, b"I");

    client.bind("", "err_root").await;
    client.execute("", 0).await;
    client.send(b'S', &[]).await;
    let reached = client.through(b'Z').await;
    assert_eq!(tags(&reached), b"2EZ");
    assert_eq!(reached[2].1, b"I");
    let error = prepare_error_detail(&reached[1]);
    let start = normalized.find("$$").unwrap();
    assert_eq!(error["code"], "xtql/edn-reader");
    assert_eq!(error["data"]["position"], serde_json::json!(start + 1));
    assert_eq!(error["data"]["span"], serde_json::json!({"start": start, "end": start + "$$($$".len()}));
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn two_nested_simple_prepare_roots_keep_their_bound_bodies_through_replacement() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    let begun = client.query("BEGIN").await;
    assert_eq!(tags(&begun), b"CZ");
    assert_eq!(begun[1].1, b"T");

    for (sql, alpha, beta) in [
        ("PREPARE alpha AS PREPARE alpha_q AS SELECT 7 AS n; PREPARE beta AS PREPARE beta_q AS SELECT 9 AS n", "old_alpha", "old_beta"),
        ("PREPARE alpha AS PREPARE new_alpha_q AS SELECT 17 AS n; PREPARE beta AS PREPARE new_beta_q AS SELECT 19 AS n", "new_alpha", "new_beta"),
    ] {
        let prepared = client.query(sql).await;
        assert_eq!(tags(&prepared), b"CCZ");
        assert_eq!(prepared[0].1, b"PREPARE\0");
        assert_eq!(prepared[1].1, b"PREPARE\0");
        assert_eq!(prepared[2].1, b"T");
        client.bind(alpha, "alpha").await;
        client.bind(beta, "beta").await;
        client.send(b'S', &[]).await;
        let bound = client.through(b'Z').await;
        assert_eq!(tags(&bound), b"22Z");
        assert_eq!(bound[2].1, b"T");
    }
    for portal in ["old_alpha", "old_beta", "new_alpha", "new_beta", "old_beta"] {
        client.execute(portal, 0).await;
    }
    client.send(b'S', &[]).await;
    let reached = client.through(b'Z').await;
    assert_eq!(tags(&reached), b"CCCCCZ");
    assert!(reached[..5].iter().all(|(_, body)| body == b"PREPARE\0"));
    assert_eq!(reached[5].1, b"T");

    let mut close = vec![b'P'];
    close.extend_from_slice(&cstring("old_alpha"));
    client.send(b'C', &close).await;
    client.bind("old_alpha", "beta").await;
    client.execute("old_alpha", 0).await;
    client.send(b'S', &[]).await;
    let rebound = client.through(b'Z').await;
    assert_eq!(tags(&rebound), b"32CZ");
    assert_eq!(rebound[2].1, b"PREPARE\0");
    assert_eq!(rebound[3].1, b"T");

    for (sql, value) in [
        ("EXECUTE alpha_q", "7"), ("EXECUTE beta_q", "9"),
        ("EXECUTE new_alpha_q", "17"), ("EXECUTE new_beta_q", "19"),
    ] {
        let frames = client.query(sql).await;
        assert_eq!(tags(&frames), b"TDCZ");
        assert_eq!(rows(&frames), [vec![Some(value.as_bytes().to_vec())]]);
        assert_eq!(frames[3].1, b"T");
    }
    client.execute("old_beta", 0).await;
    client.send(b'S', &[]).await;
    assert_eq!(client.through(b'Z').await, vec![(b'C', b"PREPARE\0".to_vec()), (b'Z', b"T".to_vec())]);
    let rolled_back = client.query("ROLLBACK").await;
    assert_eq!(tags(&rolled_back), b"CZ");
    assert_eq!(rolled_back[1].1, b"I");
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn nested_simple_prepare_emits_terminal_planning_warnings_when_the_child_is_prepared() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    let query = "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v NULLS FIRST) AS p FROM (VALUES (1), (3)) AS t(v)";
    let outer = client.query(&format!("PREPARE warn_root AS PREPARE warn_leaf AS {query}")).await;
    assert_eq!(tags(&outer), b"CZ");
    assert_eq!(outer[0].1, b"PREPARE\0");
    assert_eq!(outer[1].1, b"I");

    client.bind("", "warn_root").await;
    client.execute("", 0).await;
    client.send(b'S', &[]).await;
    let reached = client.through(b'Z').await;
    assert_eq!(tags(&reached), b"2NCZ");
    assert_eq!(reached[1].1, b"SWARNING\0VWARNING\0C01000\0MNULL ordering is ignored for percentile_cont (nulls are excluded from ordered-set aggregates)\0\0");
    assert_eq!(reached[2].1, b"PREPARE\0");
    assert_eq!(reached[3].1, b"I");
    assert_eq!(tags(&client.query("EXECUTE warn_leaf").await), b"TDCZ");
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn extended_parse_reports_the_reference_multi_statement_grammar_error() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    client.parse("broken", "SELEC 7", &[]).await;
    client.send(b'S', &[]).await;
    let frames = client.through(b'Z').await;
    assert_eq!(tags(&frames), b"EZ");
    let (_, message, detail) = error_fields(&frames[0].1);
    let expected = initial_statement_mismatch(1, 0);
    assert_eq!(message, expected);
    assert_eq!(detail["code"], "xtdb/sql-error");
    assert_eq!(detail["category"], "incorrect");
    assert_eq!(detail["data"]["sql"], "SELEC 7");
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn an_extended_execute_describes_the_rows_of_the_statement_it_names() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    assert_eq!(tags(&client.query("PREPARE foo AS SELECT $1::bigint forty_two").await), b"CZ");
    client.parse("", "EXECUTE foo ($1)", &[20]).await;
    client.send(b'D', b"S\0").await;
    client.send(b'S', &[]).await;
    let described = client.through(b'Z').await;
    assert_eq!(tags(&described), b"1tTZ");
    assert!(described[2].1.windows(9).any(|name| name == b"forty_two"));
    drop(client);
    running.stop().await;
}

/// A trusted startup as `user` on the primary database: the client, or the error the startup was refused with.
async fn start_as(server: &ServerHandle, user: &str) -> std::result::Result<Client, String> {
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let mut bytes = 196608i32.to_be_bytes().to_vec();
    for field in ["user", user, "database", "xtdb"] { bytes.extend_from_slice(&cstring(field)); }
    bytes.push(0);
    stream.write_i32((bytes.len() + 4) as i32).await.unwrap(); stream.write_all(&bytes).await.unwrap();
    let mut client = Client { stream, process: 0 };
    loop {
        match client.receive().await {
            (b'Z', _) => return Ok(client),
            (b'E', body) => return Err(String::from_utf8_lossy(&body).into_owned()),
            _ => (),
        }
    }
}

#[tokio::test]
async fn a_user_limited_by_grants_uses_only_the_databases_it_is_granted() {
    use std::collections::BTreeMap;
    use rtdb_auth::{GrantedAccess, Method, MethodRule};
    let grants = BTreeMap::from([
        ("reader".to_owned(), BTreeMap::from([("xtdb".to_owned(), GrantedAccess::Read)])),
        ("outsider".to_owned(), BTreeMap::from([
            ("*".to_owned(), GrantedAccess::Write),
            ("xtdb".to_owned(), GrantedAccess::Deny),
        ])),
        ("ordinary".to_owned(), BTreeMap::from([("*".to_owned(), GrantedAccess::Read)])),
    ]);
    let running = Running::start_with(ServerConfig::default(), AuthConfig::UserList { users: BTreeMap::new(), rules: vec![MethodRule::any(Method::Trust)], grants }).await;
    let mut writer = start_as(&running.server, "xtdb").await.unwrap();
    assert!(!tags(&writer.query("INSERT INTO docs (_id) VALUES (1)").await).contains(&b'E'), "a user the grants do not name uses every database");
    let mut reader = start_as(&running.server, "reader").await.unwrap();
    assert_eq!(rows(&reader.query("SELECT _id FROM docs").await).len(), 1, "a read grant reads");
    let refused = reader.query("INSERT INTO docs (_id) VALUES (2)").await;
    assert!(refused.iter().any(|(tag, body)| *tag == b'E' && String::from_utf8_lossy(body).contains("read-only-grant")), "and does not write: {refused:?}");
    let mut ordinary = start_as(&running.server, "ordinary").await.unwrap();
    assert_eq!(rows(&ordinary.query("SELECT _id FROM docs").await).len(), 1);
    let outsider = start_as(&running.server, "outsider").await.err().expect("explicit denial takes precedence over a default write grant");
    assert!(outsider.contains("database-not-granted"), "{outsider}");
    drop((writer, reader, ordinary));
    running.stop().await;
}

#[tokio::test]
async fn ordered_index_ddl_and_keyset_queries_use_the_pgwire_entry_point() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    let statements = [
        ("/* define access path */ CREATE INDEX orders_customer_priority ON orders (customer, priority, _id)", "CREATE INDEX"),
        ("INSERT INTO orders RECORDS {_id: 10, customer: 'a', priority: 1}, {_id: 11, customer: 'a', priority: 1}, {_id: 12, customer: 'a', priority: 3}", "INSERT 0 0"),
    ];
    for (statement, tag) in statements {
        let frames = client.query(statement).await;
        assert_eq!(tags(&frames), b"CZ");
        assert_eq!(frames[0].1, cstring(tag));
    }
    let database = running.node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    running.node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let installed = database.object_store().list(None).await.unwrap().iter().any(|object| {
                let key = object.key.as_str();
                key.starts_with("ordered-index/v2/") && key.contains("/manifests/")
            });
            if installed { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();

    let page = client.query(
        "SELECT _id FROM orders WHERE customer = 'a' AND (priority > 1 OR (priority = 1 AND _id > 10)) ORDER BY priority, _id LIMIT 2",
    ).await;
    assert_eq!(rows(&page), [vec![Some(b"11".to_vec())], vec![Some(b"12".to_vec())]]);

    client.parse("drop_orders_index", "DROP INDEX orders_customer_priority", &[]).await;
    client.bind("drop_orders_index", "drop_orders_index").await;
    client.execute("drop_orders_index", 0).await;
    client.send(b'S', &[]).await;
    let frames = client.through(b'Z').await;
    assert_eq!(tags(&frames), b"12CZ");
    assert_eq!(frames[2].1, cstring("DROP INDEX"));
    assert!(rows(&client.query("SELECT _id FROM xt.ordered_indexes").await).is_empty());
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn arrow_copy_vectors_rank_after_flush_for_both_ipc_formats() {
    use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field};

    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    let embedding = FixedSizeListArray::try_new(item, 2, Arc::new(Float32Array::from(vec![0.0, 1.0, 1.0, 0.0])), None).unwrap();
    let batch = RecordBatch::try_from_iter([
        ("_id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        ("embedding", Arc::new(embedding) as ArrayRef),
    ]).unwrap();
    for (table, file) in [("stream_vectors", false), ("file_vectors", true)] {
        let bytes = if file { rtdb_format::ipc::write_file(batch.schema().as_ref(), std::slice::from_ref(&batch)).unwrap() }
            else { rtdb_format::ipc::write_stream(batch.schema().as_ref(), std::slice::from_ref(&batch)).unwrap() };
        let format = if file { "arrow-file" } else { "arrow-stream" };
        client.send(b'Q', &cstring(&format!("COPY {table} FROM STDIN WITH (FORMAT '{format}')"))).await;
        assert_eq!(client.receive().await.0, b'G');
        client.send(b'd', &bytes).await;
        client.send(b'c', &[]).await;
        assert_eq!(tags(&client.through(b'Z').await), b"CZ");
        running.node.flush_all(std::time::Duration::from_secs(10)).await.unwrap();

        let results = client.query(&format!(
            "SELECT _id FROM {table} ORDER BY COSINE_DISTANCE(embedding, ARRAY[1,0]), _id LIMIT 2"
        )).await;
        assert_eq!(rows(&results), vec![vec![Some(b"2".to_vec())], vec![Some(b"1".to_vec())]]);
    }
    drop(client);
    running.stop().await;
}

#[tokio::test]
async fn float_array_bind_round_trips_through_queries_and_dml() {
    let running = Running::start(ServerConfig::default(), None).await;
    let mut client = Client::connect(&running.server, None).await;

    let binary = [1, 1, 700, 3, 1, 4, i32::MIN, -1, 4, 0x7fa0_0042]
        .into_iter().flat_map(i32::to_be_bytes).collect::<Vec<_>>();
    for (statement, sql, oid, input_format, output_format, payload, result) in [
        ("float4", "SELECT $1 AS value", 1021, 1u16, 1u16, Some(binary.as_slice()), Some(binary.clone())),
        ("float8", "SELECT $1 AS value", 1022, 0, 0, Some(b"{-0,NULL,Infinity}".as_slice()), Some(b"{-0.0,NULL,Infinity}".to_vec())),
        ("empty", "SELECT $1 AS value", 1022, 0, 0, Some(b"{}".as_slice()), Some(b"{}".to_vec())),
        ("null", "SELECT $1 AS value", 1021, 0, 0, None, None),
        ("insert", "INSERT INTO vectors (_id, value) VALUES (1, $1)", 1021, 1, 0, Some(binary.as_slice()), None),
    ] {
        client.parse(statement, sql, &[oid]).await;
        let mut bind = cstring(statement);
        bind.extend_from_slice(&cstring(statement));
        for size in [1u16, input_format, 1] { bind.extend_from_slice(&size.to_be_bytes()); }
        match payload {
            Some(bytes) => {
                bind.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                bind.extend_from_slice(bytes);
            }
            None => bind.extend_from_slice(&(-1i32).to_be_bytes()),
        }
        bind.extend_from_slice(&1u16.to_be_bytes());
        bind.extend_from_slice(&output_format.to_be_bytes());
        client.send(b'B', &bind).await;
        client.execute(statement, 0).await;
        client.send(b'S', &[]).await;
        let frames = client.through(b'Z').await;
        if statement == "insert" {
            assert_eq!(tags(&frames), b"12CZ", "{frames:?}");
        } else {
            assert_eq!(tags(&frames), b"12DCZ", "{frames:?}");
            assert_eq!(rows(&frames), vec![vec![result]]);
        }
    }

    assert_eq!(tags(&client.query("PREPARE bound AS INSERT INTO vectors (_id, value) VALUES (2, $1)").await), b"CZ");
    client.parse("rebind", "EXECUTE bound ($1)", &[1021]).await;
    let mut bind = cstring("rebind");
    bind.extend_from_slice(&cstring("rebind"));
    bind.extend_from_slice(&[0, 1, 0, 1, 0, 1]);
    bind.extend_from_slice(&(binary.len() as i32).to_be_bytes());
    bind.extend_from_slice(&binary);
    bind.extend_from_slice(&[0, 1, 0, 1]);
    client.send(b'B', &bind).await;
    client.execute("rebind", 0).await;
    client.send(b'S', &[]).await;
    let frames = client.through(b'Z').await;
    assert_eq!(tags(&frames), b"12CZ", "{frames:?}");

    client.parse("stored", "SELECT value FROM vectors ORDER BY _id", &[]).await;
    client.bind_formats("stored", "stored", &[1]).await;
    client.execute("stored", 0).await;
    client.send(b'S', &[]).await;
    let frames = client.through(b'Z').await;
    assert_eq!(tags(&frames), b"12DDCZ", "{frames:?}");
    assert_eq!(rows(&frames), vec![vec![Some(binary.clone())], vec![Some(binary)]]);
    drop(client);
    running.stop().await;
}
