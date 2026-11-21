//! The private installation endpoint of a hosted gateway.

use std::{
    path::Path,
    sync::Arc,
    time::Duration,
};

use ring::hmac;
use rtdb_directory::Directory;
use rtdb_types::{
    Anomaly,
    AnomalyCategory,
    Result,
};
use serde::{
    Deserialize,
    Serialize,
};
use tokio::{
    io::{
        AsyncBufReadExt,
        AsyncReadExt,
        AsyncWriteExt,
        BufReader,
    },
    net::{
        TcpListener,
        TcpStream,
    },
    sync::Semaphore,
};
use tokio_rustls::{
    rustls::ServerConfig,
    TlsAcceptor,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::admin;

const FRAME_BYTES: u64 = 2048;
const DEADLINE: Duration = Duration::from_secs(15);
const TOKEN_LENGTH: usize = 64;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    token: String,
    name: String,
    authority: Uuid,
}

#[derive(Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
enum Reply {
    Installed { name: String, authority: Uuid },
    Refused,
    Deferred,
}

pub struct Installer {
    expected: hmac::Tag,
    verifier: hmac::Key,
    org: String,
    replicas: usize,
}

impl Installer {
    pub fn from_file(path: &Path, org: String, replicas: usize) -> Result<Self> {
        let token = std::fs::read_to_string(path)
            .map_err(|_| invalid("Cannot read the installation token"))?;
        let token = token.trim_end_matches('\n');
        if token.len() != TOKEN_LENGTH
            || !token.bytes().all(|byte| byte.is_ascii_hexdigit())
            || replicas == 0
        {
            return Err(invalid(
                "Installation requires a 32-byte hexadecimal token and a positive replica count",
            ));
        }
        let verifier = hmac::Key::generate(hmac::HMAC_SHA256, &ring::rand::SystemRandom::new())
            .map_err(|_| invalid("Cannot initialize the installation token verifier"))?;
        let expected = hmac::sign(&verifier, token.as_bytes());
        Ok(Self {
            expected,
            verifier,
            org,
            replicas,
        })
    }

    async fn install(&self, directory: &dyn Directory, line: &str) -> Reply {
        let Ok(request) = serde_json::from_str::<Request>(line) else {
            return Reply::Refused;
        };
        if hmac::verify(
            &self.verifier,
            request.token.as_bytes(),
            self.expected.as_ref(),
        )
        .is_err()
        {
            return Reply::Refused;
        }
        match admin::install_deployment(
            directory,
            request.authority,
            &request.name,
            &self.org,
            self.replicas,
        )
        .await
        {
            Ok(record) => Reply::Installed {
                name: record.name,
                authority: request.authority,
            },
            Err(error)
                if matches!(
                    error.category,
                    AnomalyCategory::Incorrect | AnomalyCategory::Conflict
                ) =>
            {
                Reply::Refused
            },
            Err(_) => Reply::Deferred,
        }
    }
}

pub async fn serve(
    listener: TcpListener,
    tls: Arc<ServerConfig>,
    directory: Arc<dyn Directory>,
    installer: Arc<Installer>,
    cancellation: CancellationToken,
) -> Result<()> {
    let capacity = Arc::new(Semaphore::new(32));
    let acceptor = TlsAcceptor::from(tls);
    loop {
        let (socket, _) = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            accepted = listener.accept() => accepted.map_err(|_| invalid("The installation listener stopped accepting connections"))?,
        };
        let Ok(permit) = capacity.clone().try_acquire_owned() else {
            continue;
        };
        let (installer, directory, acceptor, cancellation) = (
            installer.clone(),
            directory.clone(),
            acceptor.clone(),
            cancellation.clone(),
        );
        tokio::spawn(async move {
            let _permit = permit;
            let request =
                tokio::time::timeout(DEADLINE, exchange(socket, acceptor, directory, installer));
            let _ = cancellation.run_until_cancelled(request).await;
        });
    }
}

async fn exchange(
    socket: TcpStream,
    acceptor: TlsAcceptor,
    directory: Arc<dyn Directory>,
    installer: Arc<Installer>,
) -> std::io::Result<()> {
    let mut socket = acceptor.accept(socket).await?;
    let mut line = String::new();
    let mut bounded = BufReader::new((&mut socket).take(FRAME_BYTES));
    bounded.read_line(&mut line).await?;
    if !line.ends_with('\n') {
        return Ok(());
    }
    let reply = installer.install(directory.as_ref(), &line).await;
    let mut bytes = serde_json::to_vec(&reply).expect("installation response serializes");
    bytes.push(b'\n');
    socket.write_all(&bytes).await?;
    socket.shutdown().await
}

fn invalid(message: &'static str) -> Anomaly {
    Anomaly::incorrect("rtdb.multi/installation-endpoint", message)
}
