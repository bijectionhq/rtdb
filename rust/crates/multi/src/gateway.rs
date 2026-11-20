//! The gateway: terminates a client's pgwire startup, finds or activates the deployment's holder, and splices the connection through to it (`RouteRead`, `ActivateOnDemand`, `ReclaimOnDemand`).
//!
//! Routing never authorises: a node may refuse or redirect, and the gateway walks on.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rtdb_directory::{DeploymentRecord, Directory, Lifecycle, MemberId, MemberRecord, MemberRole, effective_holder, now_ms};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, rustls};
use tokio_util::sync::CancellationToken;

use crate::control::{self, Request, Response};

const SSL_REQUEST: i32 = 80877103;
const GSSENC_REQUEST: i32 = 80877104;
const CANCEL_REQUEST: i32 = 80877102;
const PROTOCOL_3: i32 = 196608;
const MAXIMUM_STARTUP_BYTES: usize = 10_000;
const SNAPSHOT_PARAMETER: &str = "rtdb.read";

#[derive(Clone)]
pub struct GatewayOptions {
    /// The cell map and this gateway's cell, so a connection for a deployment living elsewhere is forwarded there.
    pub cells: Option<(Arc<dyn crate::cells::CellMap>, String)>,
    pub control_timeout: Duration,
    /// When set, every frontend must negotiate TLS before startup.
    pub tls: Option<Arc<rustls::ServerConfig>>,
    /// How often an open connection tells its holder it is in use.
    pub touch_interval: Duration,
    /// The cell's secret, with which the gateway mints a capability for each reconnection of an authenticated session.
    pub capabilities: Option<Arc<crate::capability::CellSecret>>,
}

impl Default for GatewayOptions {
    fn default() -> Self { Self { cells: None, tls: None, control_timeout: Duration::from_secs(35), touch_interval: Duration::from_secs(10), capabilities: None } }
}

pub async fn serve(listener: TcpListener, directory: Arc<dyn Directory>, options: GatewayOptions, cancellation: CancellationToken) -> Result<()> {
    loop {
        let (client, _) = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            accepted = listener.accept() => accepted.map_err(io)?,
        };
        let directory = Arc::clone(&directory);
        let options = options.clone();
        let cancellation = cancellation.child_token();
        tokio::spawn(async move { let _ = connection(client, directory, options, cancellation).await; });
    }
}

struct Startup { bytes: Vec<u8>, parameters: BTreeMap<String, String> }

impl Startup {
    /// Whether the client asked to read the deployment as of its checkpoint (`rtdb.read=snapshot`, as a parameter or in `options`), which any node may serve from object storage.
    fn snapshot(&self) -> bool {
        self.parameters.get(SNAPSHOT_PARAMETER).is_some_and(|value| value == "snapshot")
            || self.parameters.get("options").is_some_and(|options| options.split_whitespace().any(|option| option.trim_start_matches("-c").split_once('=')
                .is_some_and(|(key, value)| matches!(key.trim(), SNAPSHOT_PARAMETER | "rtdb_read") && value == "snapshot")))
    }

    /// Whether the client asked for a read-only session, which a log-fed replica may serve.
    fn read_only(&self) -> bool {
        if self.snapshot() { return true; }
        let on = |value: &str| matches!(value.trim().to_ascii_lowercase().as_str(), "on" | "true" | "1" | "yes");
        self.parameters.get("default_transaction_read_only").is_some_and(|value| on(value))
            || self.parameters.get("target_session_attrs").is_some_and(|value| matches!(value.as_str(), "read-only" | "prefer-standby" | "standby"))
            || self.parameters.get("options").is_some_and(|options| options.split_whitespace().any(|option| option.trim_start_matches("-c").split_once('=')
                .is_some_and(|(key, value)| key.trim() == "default_transaction_read_only" && on(value))))
    }
}

trait Transport: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Transport for T {}
type Client = Box<dyn Transport>;

async fn connection(client: TcpStream, directory: Arc<dyn Directory>, options: GatewayOptions, cancellation: CancellationToken) -> Result<()> {
    let startup = tokio::time::timeout(options.control_timeout, read_startup(Box::new(client), options.tls.as_ref()));
    let Some(result) = cancellation.run_until_cancelled(startup).await else { return Ok(()); };
    let Some((mut client, startup)) = result.map_err(|_| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/startup-timeout", "PostgreSQL startup did not finish"))?? else { return Ok(()); };
    let Some(name) = startup.parameters.get("database").or_else(|| startup.parameters.get("user")).cloned() else {
        return refuse(&mut client, "3D000", "The startup message names no database").await;
    };
    if let Some(gateway) = elsewhere(&directory, &name, &options).await {
        let mut forward = match connect(&gateway).await { Ok(forward) => forward, Err(error) => return refuse(&mut client, "08006", error.message.as_deref().unwrap_or_default()).await };
        forward.write_all(&startup.bytes).await.map_err(io)?;
        return tokio::select! {
            _ = cancellation.cancelled() => Ok(()),
            copied = tokio::io::copy_bidirectional(&mut client, &mut forward) => copied.map(|_| ()).map_err(io),
        };
    }
    let router = Arc::new(GatewayRouter { directory: Arc::clone(&directory), name: name.clone(), read_only: startup.read_only(), snapshot: startup.snapshot(), options: options.clone() });
    let (server, control, epoch, deployment) = match router.route_with_record().await {
        Ok(route) => route,
        Err(error) => return refuse(&mut client, "08006", error.message.as_deref().unwrap_or(error.code.as_ref())).await,
    };
    let touching = control.map(|control| {
        let name = name.clone();
        let interval = options.touch_interval;
        let timeout = options.control_timeout;
        let key = control::ControlKey(options.capabilities.clone());
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let _ = control::call(&control, &Request::Touch { name: name.clone() }, timeout, &key).await;
            }
        })
    });
    let grants = options.capabilities.clone().map(|secret| crate::session::Grants { secret, parameters: startup.parameters.clone(), read_only: startup.read_only() });
    let keyed = !startup.read_only();
    let session = crate::session::Session::new(Bytes::from(startup.bytes), router, directory, deployment, grants, keyed);
    let outcome = tokio::select! {
        _ = cancellation.cancelled() => Ok(()),
        outcome = session.run(client, server, epoch) => outcome,
    };
    if let Some(touching) = touching { touching.abort(); }
    outcome
}

/// The gateway of the cell a deployment lives in, when that is not this gateway's cell.
async fn elsewhere(directory: &Arc<dyn Directory>, name: &str, options: &GatewayOptions) -> Option<String> {
    let (map, cell) = options.cells.as_ref()?;
    let here = directory.deployment_by_name(name).await.ok().flatten();
    if here.as_ref().is_some_and(|record| matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)) { return None; }
    let living = map.cell_of(name).await.ok().flatten()?;
    if &living == cell { return None; }
    map.gateway(&living).await.ok().flatten()
}

struct GatewayRouter { directory: Arc<dyn Directory>, name: String, read_only: bool, snapshot: bool, options: GatewayOptions }

impl GatewayRouter {
    async fn route_with_record(&self) -> Result<(TcpStream, Option<String>, u64, String)> {
        route(&self.directory, &self.name, self.read_only, self.snapshot, &self.options).await
    }
}

#[async_trait::async_trait]
impl crate::session::Router for GatewayRouter {
    async fn route(&self) -> Result<(TcpStream, Option<String>, u64)> {
        let (stream, control, epoch, _) = self.route_with_record().await?;
        Ok((stream, control, epoch))
    }
}

/// The open connection to the deployment's holder, and that holder's control address.
async fn route(directory: &Arc<dyn Directory>, name: &str, read_only: bool, snapshot: bool, options: &GatewayOptions) -> Result<(TcpStream, Option<String>, u64, String)> {
    let record = directory.deployment_by_name(name).await?
        .ok_or_else(|| Anomaly::incorrect("rtdb.multi/unknown-deployment", format!("No deployment is named {name}")))?;
    match record.lifecycle {
        Lifecycle::Live | Lifecycle::Quarantined => {}
        Lifecycle::Moving => return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/moving", format!("{name} is moving to another cell; retry shortly"))),
        Lifecycle::Deleting | Lifecycle::Deleted => return Err(Anomaly::incorrect("rtdb.multi/deleted", format!("{name} has been deleted"))),
    }
    let members: BTreeMap<MemberId, MemberRecord> = directory.members().await?.into_iter().map(|member| (member.id.clone(), member)).collect();
    if snapshot && record.checkpoint.is_some() && record.lifecycle == Lifecycle::Live {
        for reader in snapshot_readers(&record, &members) {
            let Some(control_address) = &reader.control_address else { continue; };
            if let Ok(Response::Subscribed { address }) = control::call(control_address, &Request::Snapshot { name: name.to_owned() }, options.control_timeout, &control::ControlKey(options.capabilities.clone())).await {
                if let Ok(stream) = connect(&address).await { return Ok((stream, Some(control_address.clone()), record.epoch, record.id.clone())); }
            }
        }
    }
    if read_only && record.read_width > 1 && record.lifecycle == Lifecycle::Live {
        for replica in replicas(&record, &members) {
            let Some(control_address) = &replica.control_address else { continue; };
            if let Ok(Response::Subscribed { address }) = control::call(control_address, &Request::Subscribe { name: name.to_owned() }, options.control_timeout, &control::ControlKey(options.capabilities.clone())).await {
                if let Ok(stream) = connect(&address).await { return Ok((stream, Some(control_address.clone()), record.epoch, record.id.clone())); }
            }
        }
    }
    let mut last = None;
    for candidate in candidates(&record, &members) {
        let Some(control_address) = &candidate.control_address else { continue; };
        match control::call(control_address, &Request::Activate { name: name.to_owned() }, options.control_timeout, &control::ControlKey(options.capabilities.clone())).await {
            Ok(Response::Held { address, epoch }) => return Ok((connect(&address).await?, Some(control_address.clone()), epoch, record.id.clone())),
            Ok(Response::Redirect { holder, address }) => {
                let control = members.get(&MemberId(holder)).and_then(|member| member.control_address.clone());
                return Ok((connect(&address).await?, control, record.epoch, record.id.clone()));
            }
            Ok(Response::Refused { code, message }) => last = Some(Anomaly::new(AnomalyCategory::Unavailable, code, message)),
            Ok(Response::Done | Response::Subscribed { .. } | Response::Published { .. } | Response::Fragment { .. }) => {}
            Err(error) => last = Some(error),
        }
    }
    Err(last.unwrap_or_else(|| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/no-node", "No node can serve this deployment")))
}

/// The nodes that may serve a snapshot read: every node that may take a placement, in the deployment's affinity order, the holder last, so snapshots spare the writer.
fn snapshot_readers<'a>(record: &DeploymentRecord, members: &'a BTreeMap<MemberId, MemberRecord>) -> Vec<&'a MemberRecord> {
    let now = now_ms();
    let holder = effective_holder(record, members, now).cloned();
    let nodes: Vec<&MemberRecord> = members.values().filter(|member| member.role == MemberRole::Node && member.pool == rtdb_directory::MemberPool::General && member.may_receive_placements(now)).collect();
    let (mut others, holders): (Vec<&MemberRecord>, Vec<&MemberRecord>) = rtdb_affinity::rank(&record.storage_identity(), &nodes, |member| member.id.as_str()).into_iter().copied()
        .partition(|member| holder.as_ref() != Some(&member.id));
    others.extend(holders);
    others
}

/// The nodes that may serve the deployment's reads beside its holder: the ranks after the holder's, within its read width.
fn replicas<'a>(record: &DeploymentRecord, members: &'a BTreeMap<MemberId, MemberRecord>) -> Vec<&'a MemberRecord> {
    let now = now_ms();
    let holder = effective_holder(record, members, now).cloned();
    let nodes: Vec<&MemberRecord> = members.values().filter(|member| member.role == MemberRole::Node && member.may_receive_placements(now)).collect();
    rtdb_affinity::rank(&record.storage_identity(), &nodes, |member| member.id.as_str()).into_iter().copied()
        .filter(|member| holder.as_ref() != Some(&member.id)).take(record.read_width.saturating_sub(1) as usize).collect()
}

/// The holder that counts, then every node that may take a placement, in the deployment's affinity order.
fn candidates<'a>(record: &DeploymentRecord, members: &'a BTreeMap<MemberId, MemberRecord>) -> Vec<&'a MemberRecord> {
    let now = now_ms();
    let holder = effective_holder(record, members, now).and_then(|holder| members.get(holder));
    let nodes: Vec<&MemberRecord> = members.values().filter(|member| member.role == MemberRole::Node && member.may_receive_placements(now)).collect();
    let ranked = rtdb_affinity::rank(&record.storage_identity(), &nodes, |member| member.id.as_str());
    holder.into_iter().chain(ranked.into_iter().copied().filter(|member| holder.is_none_or(|holder| holder.id != member.id))).collect()
}

async fn connect(address: &str) -> Result<TcpStream> {
    let stream = TcpStream::connect(address).await.map_err(io)?;
    stream.set_nodelay(true).map_err(io)?;
    Ok(stream)
}

/// Returns None for a cancel request, which this gateway does not forward.
async fn read_startup(mut client: Client, tls: Option<&Arc<rustls::ServerConfig>>) -> Result<Option<(Client, Startup)>> {
    let mut encrypted = false;
    loop {
        let length = client.read_i32().await.map_err(io)?;
        let length = usize::try_from(length).ok().filter(|length| (8..=MAXIMUM_STARTUP_BYTES).contains(length))
            .ok_or_else(|| Anomaly::incorrect("rtdb.multi/startup", "Startup message length is out of range"))?;
        let mut body = vec![0; length - 4];
        client.read_exact(&mut body).await.map_err(io)?;
        let code = i32::from_be_bytes(body[..4].try_into().expect("four bytes"));
        match code {
            SSL_REQUEST if length == 8 => {
                if let Some(tls) = tls.filter(|_| !encrypted) {
                    client.write_all(b"S").await.map_err(io)?;
                    client = Box::new(TlsAcceptor::from(Arc::clone(tls)).accept(client).await.map_err(io)?);
                    encrypted = true;
                } else {
                    client.write_all(b"N").await.map_err(io)?;
                    client.flush().await.map_err(io)?;
                }
            }
            GSSENC_REQUEST if length == 8 => {
                client.write_all(b"N").await.map_err(io)?;
                client.flush().await.map_err(io)?;
            }
            CANCEL_REQUEST if length == 16 => return Ok(None),
            PROTOCOL_3 => {
                if tls.is_some() && !encrypted {
                    refuse(&mut client, "08004", "This gateway requires TLS").await?;
                    return Ok(None);
                }
                let mut parameters = BTreeMap::new();
                let mut fields = body[4..].split(|byte| *byte == 0).map(|field| String::from_utf8_lossy(field).into_owned());
                while let (Some(key), Some(value)) = (fields.next(), fields.next()) {
                    if key.is_empty() { break; }
                    parameters.insert(key, value);
                }
                // Only a gateway grants capabilities: one a client sends is dropped.
                parameters.remove(rtdb_pgwire::CAPABILITY_PARAMETER);
                return Ok(Some((client, Startup { bytes: encode_startup(&parameters), parameters })));
            }
            _ => return Err(Anomaly::incorrect("rtdb.multi/startup", format!("Unsupported startup code {code}"))),
        }
    }
}

/// A protocol-3 startup message carrying `parameters`.
pub(crate) fn encode_startup(parameters: &BTreeMap<String, String>) -> Vec<u8> {
    let mut body = PROTOCOL_3.to_be_bytes().to_vec();
    for (key, value) in parameters { for field in [key, value] { body.extend_from_slice(field.as_bytes()); body.push(0); } }
    body.push(0);
    let mut bytes = ((body.len() + 4) as i32).to_be_bytes().to_vec();
    bytes.extend_from_slice(&body);
    bytes
}

async fn refuse(client: &mut (impl AsyncWrite + Unpin), sqlstate: &str, message: &str) -> Result<()> {
    let mut fields = Vec::new();
    for (tag, value) in [(b'S', "FATAL"), (b'V', "FATAL"), (b'C', sqlstate), (b'M', message)] {
        fields.push(tag);
        fields.extend_from_slice(value.as_bytes());
        fields.push(0);
    }
    fields.push(0);
    let mut frame = vec![b'E'];
    frame.extend_from_slice(&((fields.len() + 4) as i32).to_be_bytes());
    frame.extend_from_slice(&fields);
    client.write_all(&frame).await.map_err(io)?;
    client.flush().await.map_err(io)
}

fn io(error: std::io::Error) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/gateway-io", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_rustls::{TlsConnector, rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject}};

    // The repository's test certificate predates subject alternative names.
    // Pin it for this transport test and still verify the handshake signatures.
    #[derive(Debug)]
    struct PinnedCertificate(CertificateDer<'static>);

    impl rustls::client::danger::ServerCertVerifier for PinnedCertificate {
        fn verify_server_cert(&self, certificate: &CertificateDer<'_>, _: &[CertificateDer<'_>], _: &ServerName<'_>, _: &[u8], _: rustls::pki_types::UnixTime) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            if certificate.as_ref() != self.0.as_ref() {
                return Err(rustls::Error::General("Unexpected gateway test certificate".into()));
            }
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(&self, message: &[u8], certificate: &CertificateDer<'_>, signature: &rustls::DigitallySignedStruct) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls12_signature(message, certificate, signature, &rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms)
        }

        fn verify_tls13_signature(&self, message: &[u8], certificate: &CertificateDer<'_>, signature: &rustls::DigitallySignedStruct) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls13_signature(message, certificate, signature, &rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms)
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms.supported_schemes()
        }
    }

    #[tokio::test]
    async fn gateway_requires_tls_before_routing_startup() {
        let certificate = CertificateDer::from_pem_slice(include_bytes!("../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.crt")).unwrap();
        let key = PrivateKeyDer::from_pem_slice(include_bytes!("../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.key")).unwrap();
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let server = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions().unwrap().with_no_client_auth()
            .with_single_cert(vec![certificate.clone()], key).unwrap();
        let client = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions().unwrap().dangerous()
            .with_custom_certificate_verifier(Arc::new(PinnedCertificate(certificate)))
            .with_no_client_auth();
        let tls = Arc::new(server);
        let parameters = BTreeMap::from([("database".into(), "tenant".into())]);
        let message = encode_startup(&parameters);

        let (mut socket, server) = tokio::io::duplex(4096);
        socket.write_all(&message).await.unwrap();
        assert!(read_startup(Box::new(server), Some(&tls)).await.unwrap().is_none());
        assert_eq!(socket.read_u8().await.unwrap(), b'E');

        let (mut socket, server) = tokio::io::duplex(4096);
        let read = tokio::spawn(async move { read_startup(Box::new(server), Some(&tls)).await.unwrap().unwrap().1 });
        socket.write_i32(8).await.unwrap();
        socket.write_i32(SSL_REQUEST).await.unwrap();
        assert_eq!(socket.read_u8().await.unwrap(), b'S');
        let mut socket = TlsConnector::from(Arc::new(client))
            .connect(ServerName::try_from("xtdb.example.com").unwrap(), socket).await.unwrap();
        socket.write_all(&message).await.unwrap();
        socket.flush().await.unwrap();
        assert_eq!(read.await.unwrap().parameters, parameters);
    }
}
