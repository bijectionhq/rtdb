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
    pub parameters: BTreeMap<String, String>,
}
impl Default for ServerConfig {
    fn default() -> Self {
        Self { address: (Ipv4Addr::LOCALHOST, 0).into(), maximum_connections: 42, pending_connections: 42,
            authentication_timeout: Duration::from_secs(60), drain_timeout: Duration::from_secs(5), maximum_message_bytes: 0x3fff_ffff, maximum_copy_bytes: 1024 * 1024 * 1024,
            read_only: false, playground: false, tls: None,
            parameters: BTreeMap::from([("server_version".into(), "16".into()), ("server_encoding".into(), "UTF8".into()), ("client_encoding".into(), "UTF8".into()),
                ("search_path".into(), "public".into()), ("datestyle".into(), "ISO".into()), ("intervalstyle".into(), "ISO_8601".into()),
                ("timezone".into(), "UTC".into()), ("integer_datetimes".into(), "on".into()), ("standard_conforming_strings".into(), "on".into())]) }
    }
}
