use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use rtdb_auth::{Authenticator, Method};
use rtdb_columnar::MemoryReservation;
use rtdb_node::{DatabaseAccess, NativeSession, Node, SessionIdentity};
use rtdb_types::{Anomaly, AnomalyCategory, PgType, Result};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use crate::{CAPABILITY_PARAMETER, DELEGABLE_PARAMETER, GATEWAY_AWAIT_TOKEN_PARAMETER, GATEWAY_REPLICA_READ_PARAMETER, GATEWAY_SETTINGS_PARAMETER, REQUIRED_INSTALLATION_PARAMETER, ServerConfig, VERIFIED_INSTALLATION_PARAMETER, error::{network, protocol}, server::CancellationRegistry, wire::{Input, Reader, Writer, STARTUP_LIMIT}};

trait Socket: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Socket for T {}
type Stream = Box<dyn Socket>;

pub(crate) struct Startup { pub parameters: BTreeMap<String, String>, pub gateway_session_state: bool, pub gateway_settings: bool, pub replica_read: bool, pub _reservation: Arc<MemoryReservation> }
pub(crate) struct ConnectionDriver { process: i32, stream: Stream, address: SocketAddr, encrypted: bool, node: Node, auth: Arc<Authenticator>, config: Arc<ServerConfig>, registry: Arc<CancellationRegistry>, cancellation: CancellationToken, draining: CancellationToken }
impl ConnectionDriver {
    pub fn new(
        process: i32, stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
        address: SocketAddr, encrypted: bool, node: Node, auth: Arc<Authenticator>,
        config: Arc<ServerConfig>, registry: Arc<CancellationRegistry>,
        cancellation: CancellationToken, draining: CancellationToken,
    ) -> Self {
        Self { process, stream: Box::new(stream), address, encrypted, node, auth, config, registry, cancellation, draining }
    }
    pub async fn run(self) -> Result<()> {
        let memory = Arc::clone(self.node.memory());
        let _region = memory.reserve(32 * 1024)?;
        let mut stream = Some(self.stream);
        let startup = self.cancellation.run_until_cancelled(tokio::time::timeout(self.config.authentication_timeout,
            startup(self.process, &mut stream, self.address, self.encrypted, &self.node, &self.auth, &self.config, &self.registry, &self.cancellation))).await;
        let (session, settings) = match startup {
            None | Some(Err(_)) | Some(Ok(Ok(None))) => return Ok(()),
            Some(Ok(Err(error))) => { if let Some(stream) = &mut stream { let mut writer = Writer::new(stream, Arc::clone(&memory)); let send = writer.error(&error, PgType::Json, error.context.get("sql-state").and_then(serde_json::Value::as_str) == Some("3D000")); let _ = tokio::time::timeout(std::time::Duration::from_secs(1), self.cancellation.run_until_cancelled(send)).await; } return Err(error); },
            Some(Ok(Ok(Some(session)))) => session,
        };
        let stream = stream.take().ok_or_else(|| protocol("Connection transport is absent after authentication"))?;
        let _read_session = settings.replica_read.then(|| session.track_replica_read_session()).transpose()?;
        let registration = self.registry.register(self.process);
        if let Some(observer) = self.node.execution_observer() { observer.connection_event(rtdb_plan::ConnectionEvent::Opened); }
        let mut protocol = crate::protocol::ProtocolSession::new(session, self.node, self.config.clone(), settings, Arc::clone(&registration.slot));
        let (read, write) = tokio::io::split(stream);
        let mut reader = Reader::new(read, Arc::clone(&memory)); let mut writer = Writer::new(tokio::io::BufWriter::with_capacity(64 * 1024, write), memory);
        let reader_stop = self.cancellation.child_token();
        let (sender, mut receiver) = mpsc::channel(64);
        let reader_token = reader_stop.clone(); let slot = Arc::clone(&registration.slot); let limit = self.config.maximum_message_bytes;
        let reader_task = tokio::spawn(async move {
            let result = reader_token.run_until_cancelled(async {
                loop {
                    let Some(frame) = reader.read(limit).await? else { return Ok(()); };
                    let terminate = frame.tag == b'X'; if terminate { slot.peer_closed(); }
                    if sender.send(frame).await.is_err() || terminate { return Ok(()); }
                }
            }).await.unwrap_or(Ok(()));
            slot.peer_closed(); result
        });
        let outcome = loop {
            if self.draining.is_cancelled() && !protocol.extended() { break Ok(()); }
            let frame = tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => break Ok(()),
                _ = self.draining.cancelled(), if !protocol.extended() => break Ok(()),
                frame = receiver.recv() => match frame { Some(frame) => frame, None => break Ok(()) },
            };
            if frame.tag == b'X' { break Ok(()); }
            let work = protocol.handle(frame, &mut writer);
            let result = tokio::select! { biased; _ = self.cancellation.cancelled() => None, result = work => Some(result) };
            match result { None => break Ok(()), Some(Err(error)) => break Err(error), Some(Ok(())) => () }
        };
        reader_stop.cancel(); protocol.close(); drop(writer); drop(receiver);
        let reader_result = reader_task.await.map_err(|error| Anomaly::fault("xtdb.pgwire/reader-task", error.to_string()))?;
        drop(registration);
        outcome.and(reader_result)
    }
}

async fn startup(process: i32, transport: &mut Option<Stream>, address: SocketAddr, mut encrypted: bool, node: &Node, auth: &Arc<Authenticator>, config: &ServerConfig, registry: &CancellationRegistry, cancellation: &CancellationToken) -> Result<Option<(NativeSession, Startup)>> {
    loop {
        let stream = transport.as_mut().ok_or_else(|| protocol("Missing PostgreSQL transport"))?;
        let extra = if config.gateway_session_state {
            GATEWAY_AWAIT_TOKEN_PARAMETER.len() + GATEWAY_SETTINGS_PARAMETER.len() + GATEWAY_REPLICA_READ_PARAMETER.len() + 3 * ("on".len() + 2)
        } else { 0 };
        let extra = extra + if config.capabilities.is_some() { REQUIRED_INSTALLATION_PARAMETER.len() + 36 + 2 } else { 0 };
        let frame = Reader::new(&mut **stream, Arc::clone(node.memory())).startup(extra).await?;
        let mut input = Input::new(&frame.body); let version = input.integer()?;
        match version {
            80877103 => {
                input.end()?;
                let enabled = !encrypted && config.tls.is_some();
                Writer::new(&mut **stream, Arc::clone(node.memory())).raw(if enabled { b"S" } else { b"N" }).await?;
                if enabled {
                    let stream = transport.take().ok_or_else(|| protocol("Missing TLS transport"))?;
                    let tls = tokio_rustls::TlsAcceptor::from(Arc::clone(config.tls.as_ref().ok_or_else(|| protocol("Missing TLS configuration"))?)).accept(stream).await.map_err(network)?;
                    *transport = Some(Box::new(tls)); encrypted = true;
                }
            }
            80877104 => return Err(protocol("GSSAPI is not supported")),
            80877102 => { let process = input.integer()?; let _secret = input.integer()?; input.end()?; registry.cancel(process); return Ok(None); }
            196608 => {
                if config.require_tls && !encrypted {
                    return Err(protocol("This PostgreSQL listener requires TLS"));
                }
                let mut requested = input.startup()?; input.end()?;
                if requested.keys().any(|key| key.eq_ignore_ascii_case(crate::config::CHECKPOINT_STAGE_PARAMETER)) {
                    return Err(protocol("The checkpoint stage is assigned by the reader"));
                }
                if requested.keys().any(|key| key.eq_ignore_ascii_case(VERIFIED_INSTALLATION_PARAMETER)) {
                    return Err(protocol("The installation verification is assigned by the server"));
                }
                let required_installation = requested.remove(REQUIRED_INSTALLATION_PARAMETER);
                let capability = requested.remove(CAPABILITY_PARAMETER);
                let gateway_session_state = requested.remove(GATEWAY_AWAIT_TOKEN_PARAMETER).as_deref() == Some("on")
                    && config.gateway_session_state;
                let gateway_settings = requested.remove(GATEWAY_SETTINGS_PARAMETER).as_deref() == Some("on")
                    && gateway_session_state;
                let replica_read = requested.remove(GATEWAY_REPLICA_READ_PARAMETER).as_deref() == Some("on")
                    && gateway_session_state;
                let database = requested.get("database").map_or("xtdb", String::as_str);
                if config.playground { node.database_or_attach(database.to_owned(), Default::default()).await?; }
                let local = node.catalog().database(database).map_err(|_| Anomaly::incorrect("xtdb.pgwire/invalid-catalog", format!("database '{database}' does not exist")).with_context("sql-state", "3D000".into()))?;
                let verified_attachment = if let Some(required) = &required_installation {
                    let verifier = config.capabilities.as_ref().ok_or_else(installation_changed)?;
                    verifier.require_installation(required, &local).map_err(|_| installation_changed())?;
                    Some(local)
                } else { None };
                let user = requested.get("user").map(String::as_str); let remote = address.ip().to_string();
                let method = if capability.is_some() { Method::Trust } else { auth.method_for(user, Some(&remote)).ok_or_else(|| Anomaly::new(AnomalyCategory::Forbidden, "xtdb.authn/no-matching-rule", format!("no authentication record found for user: {}", user.unwrap_or_default())))? };
                let identity: Arc<dyn SessionIdentity> = if let Some(token) = &capability {
                    let verifier = config.capabilities.as_ref().ok_or_else(|| Anomaly::new(AnomalyCategory::Forbidden, "xtdb.authn/capability-not-accepted", "This server does not accept capabilities").with_context("sql-state", "28000".into()))?;
                    let granted = verifier.verify(token, database).map_err(|error| error.with_context("sql-state", "28000".into()))?;
                    if user.is_some_and(|user| user != granted.principal) { return Err(Anomaly::new(AnomalyCategory::Forbidden, "xtdb.authn/capability-principal", "The capability was granted to another user").with_context("sql-state", "28000".into())); }
                    Arc::new(CapabilityIdentity { principal: auth.authenticate_trusted(&granted.principal)?, read_only: granted.read_only })
                } else { match method {
                    Method::Trust => match user { Some(user) => auth.authenticate_trusted(user)?, None => auth.unauthenticated()? },
                    Method::Password | Method::ClientCredentials => {
                        Writer::new(&mut **stream, Arc::clone(node.memory())).auth(3).await?;
                        let password = Reader::new(&mut **stream, Arc::clone(node.memory())).read(STARTUP_LIMIT).await?.ok_or_else(|| protocol("Connection closed during authentication"))?;
                        if password.tag != b'p' { return Err(Anomaly::incorrect("xtdb.pgwire/invalid-auth-spec", format!("password authentication failed for user: {}", user.unwrap_or_default())).with_context("sql-state", "28000".into())); }
                        let mut input = Input::new(&password.body); let secret = input.string()?; input.end()?;
                        if method == Method::Password { auth.verify_password(user.unwrap_or_default(), secret).await? }
                        else { let (id, secret) = client_credentials(secret)?; auth.verify_client_credentials(id, secret).await? }
                    }
                    Method::DeviceAuth => {
                        let device = auth.start_device_auth(user.unwrap_or_default()).await?;
                        let mut writer = Writer::new(&mut **stream, Arc::clone(node.memory())); writer.auth(0).await?;
                        writer.notice(&format!("\nHello! Please head over here to authenticate:\n\n{}\n", device.url())).await?;
                        device.await_principal(cancellation).await?
                    }
                } };
                let delegable = identity.delegable();
                let mut session = node.session(database, identity)?;
                if let Some(verified) = &verified_attachment {
                    let current = node.catalog().database(database).map_err(|_| installation_changed())?;
                    if !Arc::ptr_eq(verified, &current) { return Err(installation_changed()); }
                }
                let size = config.parameters.iter().chain(requested.iter()).try_fold(8192usize, |size, (key, value)| size.checked_add(key.len().checked_mul(4)?).and_then(|size| size.checked_add(value.len())).and_then(|size| size.checked_add(256))).ok_or_else(|| protocol("Startup parameters exceed memory range"))?;
                let reservation = node.memory().reserve(size)?;
                let mut parameters: BTreeMap<_, _> = config.parameters.iter().map(|(key, value)| (key.to_ascii_lowercase(), value.clone())).collect(); for (key, value) in requested { parameters.insert(key.to_ascii_lowercase(), value); }
                if let Some(user) = session.state().user() { parameters.insert("user".into(), user.into()); }
                let mut writer = Writer::new(&mut **stream, Arc::clone(node.memory()));
                if method != Method::DeviceAuth { writer.auth(0).await?; }
                apply_startup(&mut session, &parameters, &mut writer).await?;
                // Tells a gateway it may vouch for this session's user when it reconnects it elsewhere.
                if delegable { writer.parameter(DELEGABLE_PARAMETER, "on").await?; }
                if let Some(required) = &required_installation { writer.parameter(VERIFIED_INSTALLATION_PARAMETER, required).await?; }
                writer.backend_key(process).await?;
                if gateway_session_state {
                    writer.gateway_await_token(None).await?;
                    writer.send(crate::GATEWAY_SESSION_STATE_TAG, &[crate::GATEWAY_KEY_KIND]).await?;
                    writer.send(crate::GATEWAY_SESSION_STATE_TAG, &[crate::GATEWAY_KEY_CONFIRM_KIND]).await?;
                    writer.send(crate::GATEWAY_SESSION_STATE_TAG, &[crate::GATEWAY_REPLAY_PERMIT_KIND]).await?;
                }
                if gateway_settings {
                    writer.gateway_settings(&[]).await?;
                    writer.send(crate::GATEWAY_SESSION_STATE_TAG, &[crate::GATEWAY_SQL_PREPARE_KIND]).await?;
                    writer.send(crate::GATEWAY_SESSION_STATE_TAG, &[crate::GATEWAY_NAMED_PARSE_KIND]).await?;
                }
                writer.ready(b'I').await?;
                return Ok(Some((session, Startup { parameters, gateway_session_state, gateway_settings, replica_read, _reservation: reservation })));
            }
            _ => return Err(protocol("Unknown protocol version")),
        }
    }
}

fn installation_changed() -> Anomaly {
    Anomaly::incorrect("rtdb.pgwire/installation", "The node does not hold the requested installation")
        .with_context("sql-state", "3D000".into())
}

/// A principal admitted by a capability: the access the capability grants bounds what the principal may do.
struct CapabilityIdentity { principal: Arc<dyn SessionIdentity>, read_only: bool }

#[async_trait::async_trait]
impl SessionIdentity for CapabilityIdentity {
    fn user(&self) -> Option<&str> { self.principal.user() }
    fn delegable(&self) -> bool { self.principal.delegable() }
    async fn revalidate(&self, now: rtdb_types::Timestamp) -> Result<()> { self.principal.revalidate(now).await }
    fn authorize_database(&self, database: &str, access: DatabaseAccess) -> Result<()> {
        if self.read_only && access == DatabaseAccess::Write { return Err(Anomaly::new(AnomalyCategory::Forbidden, "xtdb.authz/read-only-capability", "This session's capability grants read access only")); }
        self.principal.authorize_database(database, access)
    }
}

pub(crate) async fn apply_startup<W: AsyncWrite + Unpin>(session: &mut NativeSession, parameters: &BTreeMap<String, String>, writer: &mut Writer<W>) -> Result<()> {
    let preserve_application_name = session.state().parameter("application_name").is_some();
    for (key, value) in parameters {
        match key.as_str() {
            "timezone" => session.state_mut().set_timezone(value)?,
            "options" => for (key, value) in options(value) {
                if !preserve_application_name || !key.eq_ignore_ascii_case("application_name") {
                    session.state_mut().set_parameter(key, Some(value))?;
                }
            },
            "application_name" => {
                if !preserve_application_name {
                    session.state_mut().set_parameter(key, Some(value.clone()))?;
                }
                continue;
            }
            _ => session.state_mut().set_parameter(key, Some(value.clone()))?,
        }
        writer.parameter(display_parameter(key), if key == "standard_conforming_strings" { "on" } else { value }).await?;
    }
    if session.state().parameter("application_name").is_none() {
        session.state_mut().set_parameter("application_name", Some("XTDB PGWire".into()))?;
    }
    if let Some(name) = session.state().parameter("application_name") {
        writer.parameter("application_name", name).await?;
    }
    Ok(())
}
pub(crate) fn display_parameter(key: &str) -> &str { match key { "timezone" => "TimeZone", "datestyle" => "DateStyle", "intervalstyle" => "IntervalStyle", key => key } }
fn options(value: &str) -> Vec<(String, String)> { value.split("-c ").skip(1).filter_map(|part| { let (key, value) = part.split_once('=')?; let valid = |ch: char| ch.is_alphanumeric() || ch == '_'; if !key.chars().all(valid) { return None; } Some((key.into(), value.chars().take_while(|ch| valid(*ch)).collect())) }).collect() }
fn client_credentials(value: &str) -> Result<(&str, &str)> { if value.trim().is_empty() { return Err(Anomaly::incorrect("xtdb.authn/malformed-client-credentials", "Client credentials were empty")); } let (id, secret) = value.split_once(':').filter(|(_, secret)| !secret.is_empty() && !secret.contains(':')).ok_or_else(|| Anomaly::incorrect("xtdb.authn/malformed-client-credentials", "Client credentials must be provided in the format 'client-id:client-secret'"))?; Ok((id, secret)) }
