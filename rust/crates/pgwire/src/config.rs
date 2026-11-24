use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio_rustls::rustls;

#[derive(Clone)]
pub struct ServerConfig {
    pub address: SocketAddr,
    pub maximum_connections: usize,
    pub pending_connections: usize,
    pub authentication_timeout: Duration,
    pub drain_timeout: Duration,
    /// Maximum authenticated message body length, excluding the tag and four-byte length prefix.
    pub maximum_message_bytes: usize,
    pub maximum_copy_bytes: u64,
    pub read_only: bool,
    pub playground: bool,
    pub tls: Option<Arc<rustls::ServerConfig>>,
    pub require_tls: bool,
    pub parameters: BTreeMap<String, String>,
    /// Accepts a capability in the startup's `rtdb.capability` parameter in place of the configured authentication.
    pub capabilities: Option<Arc<dyn CapabilityVerifier>>,
    pub gateway_session_state: bool,
}

/// The authenticated identity of an isolated checkpoint reader.
pub const CHECKPOINT_STAGE_PARAMETER: &str = "rtdb.checkpoint_stage";

/// A placed session can exchange its connection state with the gateway.
pub const GATEWAY_AWAIT_TOKEN_PARAMETER: &str = "rtdb.gateway_await_token";
pub const GATEWAY_SESSION_STATE_TAG: u8 = b'Y';
pub const GATEWAY_SETTINGS_KIND: u8 = 2;
pub const GATEWAY_SQL_PREPARE_KIND: u8 = 3;
pub const GATEWAY_NAMED_PARSE_KIND: u8 = 4;
pub const GATEWAY_KEY_KIND: u8 = 5;
pub const GATEWAY_KEY_CONFIRM_KIND: u8 = 6;
pub const GATEWAY_ANSWER_DEADLINE_KIND: u8 = 9;
pub const GATEWAY_SETTINGS_PARAMETER: &str = "rtdb.gateway_settings";
pub const GATEWAY_REPLICA_READ_PARAMETER: &str = "rtdb.gateway_replica_read";
pub const REQUIRED_INSTALLATION_PARAMETER: &str = "rtdb.require_installation";
pub const VERIFIED_INSTALLATION_PARAMETER: &str = "rtdb.verified_installation";

/// The startup parameter a capability travels in.
pub const CAPABILITY_PARAMETER: &str = "rtdb.capability";

/// The parameter status a server reports at startup when the session's principal may be re-established from its name, by a capability.
pub const DELEGABLE_PARAMETER: &str = "rtdb.delegable";

/// What a verified capability grants: one principal's session on the database it names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedCapability { pub principal: String, pub read_only: bool }

/// Verifies capabilities minted by a party the server trusts to have authenticated their principal.
pub trait CapabilityVerifier: Send + Sync {
    /// The grant `token` carries for `database`, refused unless it is genuine, unexpired and names `database`.
    fn verify(&self, token: &str, database: &str) -> rtdb_types::Result<VerifiedCapability>;
    fn require_installation(&self, _installation: &str, _database: &rtdb_node::Database) -> rtdb_types::Result<()> {
        Err(rtdb_types::Anomaly::incorrect("rtdb.pgwire/installation", "This listener cannot verify deployment installations"))
    }
}
impl Default for ServerConfig {
    fn default() -> Self {
        Self { address: (Ipv4Addr::LOCALHOST, 0).into(), maximum_connections: 42, pending_connections: 42,
            authentication_timeout: Duration::from_secs(60), drain_timeout: Duration::from_secs(5), maximum_message_bytes: 0x3fff_ffff, maximum_copy_bytes: 1024 * 1024 * 1024,
            read_only: false, playground: false, tls: None, require_tls: false, capabilities: None, gateway_session_state: false,
            parameters: BTreeMap::from([("server_version".into(), "16".into()), ("server_encoding".into(), "UTF8".into()), ("client_encoding".into(), "UTF8".into()),
                ("search_path".into(), "public".into()), ("datestyle".into(), "ISO".into()), ("intervalstyle".into(), "ISO_8601".into()),
                ("timezone".into(), "UTC".into()), ("integer_datetimes".into(), "on".into()), ("standard_conforming_strings".into(), "on".into())]) }
    }
}
