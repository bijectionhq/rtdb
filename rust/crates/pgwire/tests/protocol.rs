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
    async fn start(config: ServerConfig, password: Option<&str>) -> Self {
        let clock: Arc<dyn Clock> = Arc::new(TickClock(AtomicI64::new(1_000_000)));
        let auth = Authenticator::open(AuthConfig::SingleRootUser { password: password.map(Secret::new) }, MemoryBudget::new(64 << 20), Arc::clone(&clock), Arc::new(NoHttp)).await.unwrap();
        let services = NodeServices { clock, ..NodeServices::local(auth.clone(), Arc::new(Observer), auth.clone(), Arc::new(Metrics)) };
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
        let mut stream = TcpStream::connect(server.address()).await.unwrap();
        let mut bytes = 196608i32.to_be_bytes().to_vec(); bytes.extend_from_slice(b"user\0xtdb\0database\0xtdb\0\0");
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
    async fn bind(&mut self, portal: &str, statement: &str) { let mut bytes = cstring(portal); bytes.extend_from_slice(&cstring(statement)); bytes.extend_from_slice(&[0; 6]); self.send(b'B', &bytes).await; }
    async fn execute(&mut self, portal: &str, rows: i32) { let mut bytes = cstring(portal); bytes.extend_from_slice(&rows.to_be_bytes()); self.send(b'E', &bytes).await; }
}
fn cstring(text: &str) -> Vec<u8> { let mut bytes = text.as_bytes().to_vec(); bytes.push(0); bytes }
fn tags(frames: &[(u8, Vec<u8>)]) -> Vec<u8> { frames.iter().map(|frame| frame.0).collect() }
fn rows(frames: &[(u8, Vec<u8>)]) -> Vec<Vec<Option<Vec<u8>>>> {
    frames.iter().filter(|frame| frame.0 == b'D').map(|(_, bytes)| {
        let count = u16::from_be_bytes(bytes[..2].try_into().unwrap()); let mut offset = 2;
        (0..count).map(|_| { let length = i32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()); offset += 4; if length < 0 { None } else { let value = bytes[offset..offset + length as usize].to_vec(); offset += length as usize; Some(value) } }).collect()
    }).collect()
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

fn arrow_copy_bytes(file: bool, offset: i64) -> Vec<u8> {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    let batch = |id, value| RecordBatch::try_from_iter(vec![("_id", Arc::new(Int64Array::from(vec![id])) as ArrayRef), ("n", Arc::new(Int64Array::from(vec![value])) as ArrayRef)]).unwrap();
    let first = batch(1 + offset, 10); let second = batch(2 + offset, 20); let mut bytes = Vec::new();
    if file { let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut bytes, first.schema().as_ref()).unwrap(); writer.write(&first).unwrap(); writer.write(&second).unwrap(); writer.finish().unwrap(); }
    else { let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, first.schema().as_ref()).unwrap(); writer.write(&first).unwrap(); writer.write(&second).unwrap(); writer.finish().unwrap(); }
    bytes
}

#[tokio::test]
async fn arrow_copy_batches_autocommit_separately_and_explicit_rollback_discards_all_batches() {
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
