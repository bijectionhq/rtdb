use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage, config::SslMode};
use tokio_rustls::{
    TlsConnector,
    rustls::{self, pki_types::ServerName},
};

const UUID: &str = "01234567-89ab-4def-8000-000000000001";
const NAME: &str = "team.dev";
const PASSWORD: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const TIMEOUT: Duration = Duration::from_secs(20);

fn command(operation: &str, state: &Path, id: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rtdb"));
    command
        .args([operation, "--state"])
        .arg(state)
        .args(["--name", NAME, "--id", id]);
    command
}

fn launch(state: &Path, session: &Path, id: &str) -> Command {
    let mut command = command("standalone", state, id);
    command
        .arg("--certificate")
        .arg(session.join("certificate.pem"))
        .arg("--private-key")
        .arg(session.join("key.pem"))
        .arg("--password-file")
        .arg(session.join("password"))
        .arg("--ready-file")
        .arg(session.join("ready.json"))
        .arg("--parent-stdin")
        .stdin(Stdio::piped());
    command
}

fn private_file(path: &Path, bytes: &[u8]) {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).unwrap().write_all(bytes).unwrap();
}

fn private_dir() -> tempfile::TempDir {
    let mut options = tempfile::Builder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        options.permissions(fs::Permissions::from_mode(0o700));
    }
    options.tempdir().unwrap()
}

struct Process(Child);

impl Process {
    fn ready(&mut self, path: &Path) -> Value {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            assert!(
                self.0.try_wait().unwrap().is_none(),
                "server stopped before ready"
            );
            match fs::read(path) {
                Ok(bytes) => return serde_json::from_slice(&bytes).unwrap(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(error) => panic!("read readiness: {error}"),
            }
            assert!(Instant::now() < deadline, "timed out awaiting ready");
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn exit(&mut self) -> ExitStatus {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "server did not drain after parent EOF"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

#[test]
fn standalone_reopens_the_exact_private_authority_and_drains_on_parent_eof() {
    let state = private_dir();
    assert!(
        command("initialize-standalone", state.path(), UUID)
            .status()
            .unwrap()
            .success()
    );
    let marker = state.path().join("native-authority.json");
    let installed = fs::read(&marker).unwrap();

    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../src/test/resources/xtdb/pgwire");
    for _ in 0..2 {
        let session = private_dir();
        private_file(
            &session.path().join("certificate.pem"),
            &fs::read(fixture.join("xtdb.example.com.crt")).unwrap(),
        );
        private_file(
            &session.path().join("key.pem"),
            &fs::read(fixture.join("xtdb.example.com.key")).unwrap(),
        );
        private_file(&session.path().join("password"), PASSWORD.as_bytes());

        let mut child = Process(launch(state.path(), session.path(), UUID).spawn().unwrap());
        let ready_file = session.path().join("ready.json");
        let ready = child.ready(&ready_file);
        assert_eq!(ready["format"], 3);
        assert_eq!(ready["database"], NAME);
        assert_eq!(ready["installation"], UUID);
        assert_eq!(ready["host"], "127.0.0.1");
        let pgwire = ready["port"].as_u64().unwrap();
        let flight = ready["native_flight_port"].as_u64().unwrap();
        assert!((1..=65535).contains(&pgwire) && (1..=65535).contains(&flight));
        assert_ne!(pgwire, flight);
        assert!(ready.get("owned_flight_port").is_none());

        drop(child.0.stdin.take().unwrap());
        assert!(child.exit().success());
        assert!(!ready_file.exists());
        assert_eq!(fs::read(&marker).unwrap(), installed);
    }

    let session = private_dir();
    let other = "01234567-89ab-4def-8000-000000000002";
    let rejected = launch(state.path(), session.path(), other)
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("belongs to another installation"));
    fs::remove_file(state.path().join("journal/REPLICA_LOG")).unwrap();
    assert!(
        !launch(state.path(), session.path(), UUID)
            .output()
            .unwrap()
            .status
            .success()
    );
    assert!(!state.path().join("journal/REPLICA_LOG").exists());
    assert!(!session.path().join("ready.json").exists());
}

#[test]
fn standalone_refuses_classic_state_during_initialization_and_open() {
    let state = private_dir();
    let session = private_dir();
    let authority = state.path().join("authority.json");
    private_file(&authority, br#"{"format":2}"#);

    assert!(
        !command("initialize-standalone", state.path(), UUID)
            .output()
            .unwrap()
            .status
            .success()
    );
    let rejected = launch(state.path(), session.path(), UUID).output().unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("explicit migration and cutover"));
    assert_eq!(fs::read(&authority).unwrap(), br#"{"format":2}"#);
    assert_eq!(fs::read_dir(state.path()).unwrap().count(), 1);
}

fn sql_identity(session: &Path) -> rustls::ClientConfig {
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    private_file(
        &session.join("certificate.pem"),
        identity.cert.pem().as_bytes(),
    );
    private_file(
        &session.join("key.pem"),
        identity.signing_key.serialize_pem().as_bytes(),
    );
    private_file(&session.join("password"), PASSWORD.as_bytes());
    let mut roots = rustls::RootCertStore::empty();
    roots.add(identity.cert.der().clone()).unwrap();
    rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth()
}

async fn sql_client(port: u16, tls: rustls::ClientConfig) -> Client {
    let mut tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    tcp.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).await.unwrap();
    assert_eq!(tcp.read_u8().await.unwrap(), b'S');
    let socket = TlsConnector::from(Arc::new(tls))
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
        .unwrap();
    let (client, connection) = tokio_postgres::Config::new()
        .user("xtdb")
        .password(PASSWORD)
        .dbname(NAME)
        .ssl_mode(SslMode::Disable)
        .connect_raw(socket, NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    client
}

async fn rows(client: &Client, sql: &str) -> Vec<Vec<String>> {
    client
        .simple_query(sql)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|column| row.get(column).unwrap().to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_single_journal_recovers_a_multi_table_commit_after_kill() {
    let state = private_dir();
    assert!(
        command("initialize-standalone", state.path(), UUID)
            .status()
            .unwrap()
            .success()
    );
    let authority: Value =
        serde_json::from_slice(&fs::read(state.path().join("native-authority.json")).unwrap())
            .unwrap();
    assert_eq!(authority["format"], 2);
    assert!(authority.get("source_lineage").is_none());
    assert!(state.path().join("journal/REPLICA_LOG").is_file());
    assert!(!state.path().join("journal/LOG").exists());

    let current = "SELECT o.total, i.quantity FROM orders o JOIN inventory i ON o._id = i._id";
    for reopening in [false, true] {
        let session = private_dir();
        let tls = sql_identity(session.path());
        let mut child = Process(launch(state.path(), session.path(), UUID).spawn().unwrap());
        let ready = child.ready(&session.path().join("ready.json"));
        let port = ready["port"].as_u64().unwrap().try_into().unwrap();
        tokio::time::timeout(TIMEOUT, async {
            let client = sql_client(port, tls).await;
            if reopening {
                assert_eq!(rows(&client, current).await, vec![vec!["42", "7"]]);
                client.batch_execute("UPDATE orders SET total = total + 1 WHERE _id = 1").await.unwrap();
                assert_eq!(rows(&client, current).await, vec![vec!["43", "7"]]);
            } else {
                client.batch_execute(
                    "BEGIN READ WRITE;                      INSERT INTO orders (_id, total) VALUES (1, 42);                      INSERT INTO inventory (_id, quantity) VALUES (1, 7); COMMIT",
                ).await.unwrap();
                assert_eq!(rows(&client, current).await, vec![vec!["42", "7"]]);
                let error = client.batch_execute(
                    "BEGIN READ WRITE; UPDATE orders SET total = 99 WHERE _id = 1; COMMIT ASYNC",
                ).await.unwrap_err();
                assert_eq!(error.code(), Some(&tokio_postgres::error::SqlState::FEATURE_NOT_SUPPORTED));
            }
        }).await.unwrap();
        assert!(!state.path().join("journal/LOG").exists());
        if reopening {
            drop(child.0.stdin.take().unwrap());
            assert!(child.exit().success());
        } else {
            child.0.kill().unwrap();
            assert!(!child.0.wait().unwrap().success());
            OpenOptions::new()
                .append(true)
                .open(state.path().join("journal/REPLICA_LOG"))
                .unwrap()
                .write_all(&[0x1e, 0, 0])
                .unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_classic_journals_keep_durable_async_submission() {
    let state = private_dir();
    assert!(
        command("initialize-standalone", state.path(), UUID)
            .arg("--classic-journals")
            .status()
            .unwrap()
            .success()
    );
    let authority: Value =
        serde_json::from_slice(&fs::read(state.path().join("native-authority.json")).unwrap())
            .unwrap();
    assert_eq!(authority["format"], 1);
    assert!(authority["source_lineage"].is_string());
    assert!(state.path().join("journal/LOG").is_file());

    let session = private_dir();
    let tls = sql_identity(session.path());
    let mut child = Process(launch(state.path(), session.path(), UUID).spawn().unwrap());
    let port = child.ready(&session.path().join("ready.json"))["port"]
        .as_u64()
        .unwrap()
        .try_into()
        .unwrap();
    tokio::time::timeout(TIMEOUT, async {
        let client = sql_client(port, tls).await;
        client
            .batch_execute(
                "BEGIN READ WRITE; INSERT INTO orders (_id, total) VALUES (1, 42); COMMIT ASYNC",
            )
            .await
            .unwrap();
        assert_eq!(
            rows(&client, "SELECT total FROM orders").await,
            vec![vec!["42"]]
        );
    })
    .await
    .unwrap();
    drop(child.0.stdin.take().unwrap());
    assert!(child.exit().success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_interactive_sql_opens_portals_at_execute_and_checks_the_commit_basis() {
    let state = private_dir();
    assert!(
        command("initialize-standalone", state.path(), UUID)
            .status()
            .unwrap()
            .success()
    );

    let session = private_dir();
    let tls = sql_identity(session.path());
    let mut child = Process(launch(state.path(), session.path(), UUID).spawn().unwrap());
    let port = child.ready(&session.path().join("ready.json"))["port"]
        .as_u64()
        .unwrap()
        .try_into()
        .unwrap();
    tokio::time::timeout(TIMEOUT, async {
        let mut client = sql_client(port, tls.clone()).await;
        let observer = sql_client(port, tls).await;
        client.batch_execute("SET TIME ZONE 'UTC'").await.unwrap();
        observer.batch_execute("SET TIME ZONE 'UTC'").await.unwrap();
        client
            .batch_execute("INSERT INTO orders (_id, total) VALUES (1, 10), (2, 20)")
            .await
            .unwrap();

        let tx = client
            .build_transaction()
            .read_only(false)
            .start()
            .await
            .unwrap();
        let begin_clock: String = tx.query_one("SELECT CAST(CURRENT_TIMESTAMP AS VARCHAR)", &[]).await.unwrap().get(0);
        let prepared = tx
            .prepare("SELECT CAST(total AS BIGINT) FROM orders ORDER BY _id")
            .await
            .unwrap();
        let portal = tx.bind(&prepared, &[]).await.unwrap();
        tx.batch_execute("UPDATE orders SET total = total + 1")
            .await
            .unwrap();
        let first = tx.query_portal(&portal, 1).await.unwrap();
        assert_eq!(first[0].get::<_, i64>(0), 11);
        tx.batch_execute("UPDATE orders SET total = 44 WHERE _id = 2")
            .await
            .unwrap();
        let remaining = tx.query_portal(&portal, 0).await.unwrap();
        assert_eq!(remaining[0].get::<_, i64>(0), 21);
        let staged_clock: String = tx
            .query_one("SELECT CAST(_system_from AS VARCHAR) FROM orders WHERE _id = 2", &[])
            .await.unwrap().get(0);
        assert_eq!(staged_clock, begin_clock);
        assert_eq!(
            rows(&observer, "SELECT total FROM orders ORDER BY _id").await,
            vec![vec!["10"], vec!["20"]]
        );
        tx.commit().await.unwrap();
        assert_eq!(
            rows(&observer, "SELECT total FROM orders ORDER BY _id").await,
            vec![vec!["11"], vec!["44"]]
        );
        let durable_clock: String = observer
            .query_one("SELECT CAST(_system_from AS VARCHAR) FROM orders WHERE _id = 2", &[])
            .await.unwrap().get(0);
        assert_eq!(durable_clock, begin_clock);

        let tx = client
            .build_transaction()
            .read_only(false)
            .start()
            .await
            .unwrap();
        tx.batch_execute("UPDATE orders SET total = 12 WHERE _id = 1")
            .await
            .unwrap();
        let read = tx
            .simple_query("SELECT total FROM orders WHERE _id = 1")
            .await
            .unwrap();
        let observed = read.iter().find_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0),
            _ => None,
        });
        assert_eq!(observed, Some("12"));
        observer
            .batch_execute("UPDATE orders SET total = 99 WHERE _id = 1")
            .await
            .unwrap();
        let conflict = tx.commit().await.unwrap_err();
        assert_eq!(
            conflict.code(),
            Some(&tokio_postgres::error::SqlState::T_R_SERIALIZATION_FAILURE)
        );
        assert_eq!(
            rows(&observer, "SELECT total FROM orders ORDER BY _id").await,
            vec![vec!["99"], vec!["44"]]
        );

        let tx = client.build_transaction().read_only(false).start().await.unwrap();
        let count: i64 = tx.query_one("SELECT COUNT(*) FROM orders WHERE total < 50", &[])
            .await.unwrap().get(0);
        assert_eq!(count, 1);
        tx.batch_execute("UPDATE orders SET total = 45 WHERE _id = 2").await.unwrap();
        observer.batch_execute("INSERT INTO orders (_id, total) VALUES (3, 10)").await.unwrap();
        assert_eq!(tx.commit().await.unwrap_err().code(), Some(&tokio_postgres::error::SqlState::T_R_SERIALIZATION_FAILURE));

        let tx = client.build_transaction().read_only(false).start().await.unwrap();
        tx.batch_execute("DELETE FROM orders WHERE _id = 3").await.unwrap();
        assert!(tx.query_opt("SELECT total FROM orders WHERE _id = 3", &[]).await.unwrap().is_none());
        tx.rollback().await.unwrap();

        let tx = client.build_transaction().read_only(false).start().await.unwrap();
        assert_eq!(
            tx.batch_execute("SET rtdb_idempotency_key = 'late-key'").await.unwrap_err().code(),
            Some(&tokio_postgres::error::SqlState::FEATURE_NOT_SUPPORTED)
        );
        tx.rollback().await.unwrap();
        assert_eq!(
            rows(&observer, "SELECT total FROM orders ORDER BY _id").await,
            vec![vec!["99"], vec!["44"], vec!["10"]]
        );
    })
    .await
    .unwrap();
    child.0.kill().unwrap();
    assert!(!child.exit().success());

    let reopened = private_dir();
    let tls = sql_identity(reopened.path());
    let mut child = Process(launch(state.path(), reopened.path(), UUID).spawn().unwrap());
    let port = child.ready(&reopened.path().join("ready.json"))["port"]
        .as_u64().unwrap().try_into().unwrap();
    tokio::time::timeout(TIMEOUT, async {
        let observer = sql_client(port, tls).await;
        assert_eq!(
            rows(&observer, "SELECT total FROM orders ORDER BY _id").await,
            vec![vec!["99"], vec!["44"], vec!["10"]]
        );
    }).await.unwrap();
    drop(child.0.stdin.take().unwrap());
    assert!(child.exit().success());
}
