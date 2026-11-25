use std::{
    collections::HashSet,
    fs::File,
    io::Read,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::net::TcpStream;
use tokio_rustls::{
    TlsConnector,
    rustls::{
        self, CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore,
        SignatureScheme,
        client::{
            Resumption,
            danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
            verify_server_cert_signed_by_trust_anchor,
        },
        crypto::WebPkiSupportedAlgorithms,
        pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime, pem::PemObject},
        server::ParsedCertificate,
    },
};
use uuid::Uuid;
use zeroize::Zeroizing;

use super::{Reply, Witness, transport::Request};
use crate::{
    cli::invalid,
    multi::classic_ingress::{MAX_CONTROL_BYTES, read_control, write_control},
};

const DEADLINE: Duration = Duration::from_secs(90);
const MAX_REDIRECTS: usize = 2;
const MAX_PEM_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    Open,
    Exclude,
    Status,
}

impl Action {
    fn request(self, witness: &Witness) -> Request {
        let Witness {
            decision,
            held_ready,
        } = witness.clone();
        match self {
            Self::Open => Request::OpenClassicMigrationDestination {
                decision,
                held_ready,
            },
            Self::Exclude => Request::ExcludeClassicMigrationOpening {
                decision,
                held_ready,
            },
            Self::Status => Request::ClassicMigrationOpeningStatus {
                decision,
                held_ready,
            },
        }
    }

    fn correlate(self, witness: &Witness, reply: Reply) -> Result<Reply> {
        let (decision, held_ready, ordered_at) = match &reply {
            Reply::Opened {
                decision,
                held_ready,
                ordered_at,
                workload_fence,
            } if self != Self::Exclude && witness.valid_fence(workload_fence) => {
                (decision, held_ready, ordered_at)
            }
            Reply::Excluded {
                decision,
                held_ready,
                ordered_at,
            } => (decision, held_ready, ordered_at),
            Reply::Refused => return Err(super::denied()),
            Reply::Unresolved => return Err(super::unavailable()),
            _ => {
                return Err(unresolved(
                    "The private peer returned an unexpected opening reply",
                ));
            }
        };
        let recorded_at = witness.held_ready.recorded_at;
        if decision != &witness.decision
            || held_ready != &witness.held_ready
            || ordered_at.incarnation() != recorded_at.incarnation()
            || *ordered_at <= recorded_at
        {
            return Err(unresolved(
                "The opening terminal does not match the exact witness",
            ));
        }
        Ok(reply)
    }
}

#[derive(Clone, Debug)]
pub struct Options {
    action: Action,
    connection_file: PathBuf,
    witness_file: PathBuf,
}

impl Options {
    pub(crate) fn parse(mut args: impl Iterator<Item = String>) -> Result<Self> {
        let action = match args.next().as_deref() {
            Some("open") => Action::Open,
            Some("exclude") => Action::Exclude,
            Some("status") => Action::Status,
            _ => return Err(invalid("Specify classic-opening open, exclude or status")),
        };
        let mut connection_file = None;
        let mut witness_file = None;
        while let Some(option) = args.next() {
            let target = match option.as_str() {
                "--connection-file" if connection_file.is_none() => &mut connection_file,
                "--witness-file" if witness_file.is_none() => &mut witness_file,
                _ => return Err(invalid("Unknown or repeated classic-opening option")),
            };
            *target = Some(PathBuf::from(
                args.next()
                    .filter(|value| !value.is_empty() && !value.starts_with('-'))
                    .ok_or_else(|| invalid(format!("{option} requires a path")))?,
            ));
        }
        Ok(Self {
            action,
            connection_file: connection_file.ok_or_else(|| invalid("Specify --connection-file"))?,
            witness_file: witness_file.ok_or_else(|| invalid("Specify --witness-file"))?,
        })
    }

    pub async fn run(self) -> Result<impl Serialize> {
        let connection: Connection =
            serde_json::from_slice(&read_file(&self.connection_file, MAX_CONTROL_BYTES)?)
                .map_err(|_| invalid("Invalid Classic opening connection JSON"))?;
        let witness: Witness =
            serde_json::from_slice(&read_file(&self.witness_file, MAX_CONTROL_BYTES)?)
                .map_err(|_| invalid("Invalid Classic opening witness JSON"))?;
        if !witness.valid(connection.catalog) {
            return Err(invalid(
                "The Classic opening witness is invalid for the configured catalog",
            ));
        }
        let request = self.action.request(&witness);
        tokio::time::timeout(
            DEADLINE,
            connection.exchange(self.action, &request, &witness),
        )
        .await
        .map_err(|_| unresolved("Classic opening request deadline expired"))?
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Connection {
    entry: String,
    port: u16,
    catalog: Uuid,
    ca_file: PathBuf,
    client_certificate_file: PathBuf,
    client_private_key_file: PathBuf,
    server_identity: ServerIdentity,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ServerIdentity {
    ServerName(String),
    LeafSha256([u8; 32]),
}

impl Connection {
    fn tls(&self) -> Result<(TlsConnector, Option<ServerName<'static>>)> {
        if self.entry.is_empty() || self.port == 0 || self.catalog.is_nil() {
            return Err(invalid(
                "Configure a private Classic opening entry, port and catalog",
            ));
        }
        let tls_error = |_| invalid("Invalid Classic opening TLS profile");
        let ca = read_file(&self.ca_file, MAX_PEM_BYTES)?;
        let mut roots = RootCertStore::empty();
        for certificate in CertificateDer::pem_slice_iter(&ca) {
            roots
                .add(certificate.map_err(|_| invalid("Invalid Classic opening CA PEM"))?)
                .map_err(tls_error)?;
        }
        if roots.is_empty() {
            return Err(invalid("The Classic opening CA file has no certificates"));
        }
        let cert = read_file(&self.client_certificate_file, MAX_PEM_BYTES)?;
        let chain = CertificateDer::pem_slice_iter(&cert)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| invalid("Invalid Classic opening client certificate PEM"))?;
        let key = read_file(&self.client_private_key_file, MAX_PEM_BYTES)?;
        let key = PrivateKeyDer::from_pem_slice(&key)
            .map_err(|_| invalid("Invalid Classic opening client key PEM"))?;
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .map_err(tls_error)?;
        let (builder, server_name) = match &self.server_identity {
            ServerIdentity::ServerName(name) => {
                let name = ServerName::try_from(name.clone())
                    .map_err(|_| invalid("Invalid Classic opening TLS server name"))?;
                (builder.with_root_certificates(roots), Some(name))
            }
            ServerIdentity::LeafSha256(digest) if *digest != [0; 32] => (
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(LeafVerifier {
                        digest: *digest,
                        roots,
                        algorithms: provider.signature_verification_algorithms,
                    })),
                None,
            ),
            ServerIdentity::LeafSha256(_) => {
                return Err(invalid("Configure a nonzero server leaf digest"));
            }
        };
        let mut tls = builder
            .with_client_auth_cert(chain, key)
            .map_err(tls_error)?;
        tls.resumption = Resumption::disabled();
        Ok((TlsConnector::from(Arc::new(tls)), server_name))
    }

    async fn exchange(
        &self,
        action: Action,
        request: &Request,
        witness: &Witness,
    ) -> Result<Reply> {
        let (tls, server_name) = self.tls()?;
        let mut visited = HashSet::new();
        let mut directed = None;
        let mut redirects_left = MAX_REDIRECTS;
        loop {
            let socket = match directed {
                None => TcpStream::connect((self.entry.as_str(), self.port)).await,
                Some(writer) => TcpStream::connect(SocketAddr::new(writer, self.port)).await,
            }
            .map_err(|_| unresolved("Could not connect to the private Classic opening peer"))?;
            let ip = socket
                .peer_addr()
                .map_err(|_| unresolved("Could not identify the private Classic opening peer"))?
                .ip()
                .to_canonical();
            visited.insert(ip);
            let name = server_name
                .clone()
                .unwrap_or_else(|| ServerName::IpAddress(ip.into()));
            let mut stream = tls
                .connect(name, socket)
                .await
                .map_err(|_| unresolved("Private Classic opening mTLS authentication failed"))?;
            write_control(&mut stream, request)
                .await
                .map_err(|_| unresolved("Could not send the exact Classic opening request"))?;
            match read_control(&mut stream)
                .await
                .map_err(|_| unresolved("Could not read a valid Classic opening reply"))?
            {
                Reply::Redirect { writer } => {
                    let writer = writer.to_canonical();
                    if action != Action::Open
                        || redirects_left == 0
                        || writer.is_unspecified()
                        || writer.is_multicast()
                        || visited.contains(&writer)
                    {
                        return Err(unresolved(
                            "The private peer returned an invalid or exhausted writer redirect",
                        ));
                    }
                    redirects_left -= 1;
                    directed = Some(writer);
                }
                reply => return action.correlate(witness, reply),
            }
        }
    }
}

#[derive(Debug)]
struct LeafVerifier {
    digest: [u8; 32],
    roots: RootCertStore,
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for LeafVerifier {
    fn verify_server_cert(
        &self,
        leaf: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _name: &ServerName<'_>,
        _ocsp: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        if <[u8; 32]>::from(Sha256::digest(leaf.as_ref())) != self.digest {
            return Err(CertificateError::ApplicationVerificationFailure.into());
        }
        verify_server_cert_signed_by_trust_anchor(
            &ParsedCertificate::try_from(leaf)?,
            &self.roots,
            intermediates,
            now,
            self.algorithms.all,
        )?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        leaf: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, leaf, signature, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        leaf: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, leaf, signature, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

fn read_file(path: &Path, limit: usize) -> Result<Zeroizing<Vec<u8>>> {
    let mut bytes = Zeroizing::new(Vec::new());
    File::open(path)
        .and_then(|file| file.take(limit as u64 + 1).read_to_end(&mut bytes))
        .map_err(|_| {
            invalid(format!(
                "Could not read Classic opening file {}",
                path.display()
            ))
        })?;
    if bytes.len() > limit {
        return Err(invalid(format!(
            "Classic opening file exceeds {limit} bytes: {}",
            path.display()
        )));
    }
    Ok(bytes)
}

fn unresolved(message: &str) -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/classic-opening-client",
        message,
    )
}

#[cfg(test)]
mod tests;
