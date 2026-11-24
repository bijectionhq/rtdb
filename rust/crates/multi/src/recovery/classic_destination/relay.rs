use std::{
    fs::File,
    io::Read,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use ring::signature::{Ed25519KeyPair, KeyPair};
use rtdb_node::{Clock, SystemClock};
use rtdb_server::multi::classic_ingress::{
    self, Authorization, MAX_CHECKPOINT_BYTES, MAX_CONTROL_BYTES, Operation, Request, Response,
};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::Deserialize;
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tokio_rustls::{
    TlsConnector,
    rustls::{
        self, ClientConfig, RootCertStore,
        pki_types::{CertificateDer, ServerName, pem::PemObject},
    },
};
use uuid::Uuid;

const DEADLINE: Duration = Duration::from_secs(90);
const MAX_REDIRECTS: usize = 2;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    entry: String,
    port: u16,
    server_name: String,
    ca_file: PathBuf,
    catalog: Uuid,
    approval_public_key: [u8; 32],
    source_signing_key_file: PathBuf,
}

pub(crate) struct Relay {
    entry: String,
    port: u16,
    server_name: ServerName<'static>,
    tls: Arc<ClientConfig>,
    catalog: Uuid,
    approval: [u8; 32],
    source: Ed25519KeyPair,
}

impl Relay {
    pub(crate) fn from_file(path: &Path) -> Result<Self> {
        let configuration: Configuration =
            serde_json::from_slice(&read_file(path, MAX_CONTROL_BYTES)?)
                .map_err(|_| invalid_configuration())?;
        if configuration.entry.is_empty()
            || configuration.port == 0
            || configuration.catalog.is_nil()
            || configuration.approval_public_key == [0; 32]
        {
            return Err(invalid_configuration());
        }
        let mut roots = RootCertStore::empty();
        let ca = read_file(&configuration.ca_file, 64 << 10)?;
        for certificate in CertificateDer::pem_slice_iter(&ca) {
            roots
                .add(certificate.map_err(|_| invalid_configuration())?)
                .map_err(|_| invalid_configuration())?;
        }
        if roots.is_empty() {
            return Err(invalid_configuration());
        }
        let tls = ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| invalid_configuration())?
        .with_root_certificates(roots)
        .with_no_client_auth();
        let key = zeroize::Zeroizing::new(read_file(&configuration.source_signing_key_file, 4096)?);
        let source = Ed25519KeyPair::from_pkcs8(&key).map_err(|_| invalid_configuration())?;
        if source.public_key().as_ref() == configuration.approval_public_key.as_slice() {
            return Err(invalid_configuration());
        }
        Ok(Self {
            entry: configuration.entry,
            port: configuration.port,
            server_name: ServerName::try_from(configuration.server_name)
                .map_err(|_| invalid_configuration())?,
            tls: Arc::new(tls),
            catalog: configuration.catalog,
            approval: configuration.approval_public_key,
            source,
        })
    }

    pub(super) fn catalog(&self) -> Uuid {
        self.catalog
    }

    pub(super) fn verify(&self, authorization: &Authorization, operation: Operation) -> Result<()> {
        if authorization.grant.binding.catalog != self.catalog
            || authorization.grant.operation != operation
        {
            return Err(denied());
        }
        authorization.verify(self.approval, SystemClock.now())
    }

    pub(super) async fn forward(
        &self,
        authorization: Authorization,
        bytes: &[u8],
    ) -> Result<Response> {
        let operation = authorization.grant.operation;
        self.verify(&authorization, operation)?;
        let expected = if operation == Operation::Install {
            authorization.grant.binding.checkpoint_bytes as usize
        } else {
            0
        };
        if bytes.len() != expected || bytes.len() > MAX_CHECKPOINT_BYTES {
            return Err(denied());
        }
        let binding = authorization.grant.binding.identity()?;
        let source_signature = self
            .source
            .sign(&authorization.source_digest()?)
            .as_ref()
            .to_vec();
        let request = Request {
            authorization,
            source_signature,
        };
        let mut directed = None;
        tokio::time::timeout(DEADLINE, async {
            for _ in 0..=MAX_REDIRECTS {
                let socket = match directed {
                    Some(writer) => TcpStream::connect(SocketAddr::new(writer, self.port)).await,
                    None => TcpStream::connect((self.entry.as_str(), self.port)).await,
                }
                .map_err(|_| unavailable())?;
                let mut tls = TlsConnector::from(self.tls.clone())
                    .connect(self.server_name.clone(), socket)
                    .await
                    .map_err(|_| unavailable())?;
                classic_ingress::write_control(&mut tls, &request).await?;
                tls.write_u32(bytes.len() as u32)
                    .await
                    .map_err(|_| unavailable())?;
                tls.write_all(bytes).await.map_err(|_| unavailable())?;
                tls.flush().await.map_err(|_| unavailable())?;
                match classic_ingress::read_control(&mut tls).await? {
                    Response::Redirect {
                        writer,
                        binding_sha256,
                    } if operation == Operation::Install
                        && binding_sha256 == binding
                        && directed != Some(writer) =>
                    {
                        directed = Some(writer)
                    }
                    response @ Response::Observed { binding_sha256, .. }
                        if binding_sha256 == binding =>
                    {
                        return Ok(response);
                    }
                    Response::Refused => return Err(denied()),
                    _ => return Err(unavailable()),
                }
            }
            Err(unavailable())
        })
        .await
        .map_err(|_| unavailable())?
    }
}

fn read_file(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)
        .and_then(|file| file.take(limit as u64 + 1).read_to_end(&mut bytes))
        .map_err(|_| invalid_configuration())?;
    if bytes.len() > limit {
        return Err(invalid_configuration());
    }
    Ok(bytes)
}

fn invalid_configuration() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.multi/classic-candidate-relay-config",
        "Invalid private Classic candidate relay configuration",
    )
}

fn denied() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Forbidden,
        "rtdb.multi/classic-candidate-relay",
        "Candidate approval does not match the Original selection",
    )
}

fn unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.multi/classic-candidate-relay",
        "The candidate ingress outcome is unavailable",
    )
}
