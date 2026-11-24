use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use serde_json::Value;

const UUID: &str = "01234567-89ab-4def-8000-000000000001";
const NAME: &str = "team.dev";
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
        private_file(&session.path().join("password"), "a".repeat(64).as_bytes());

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
