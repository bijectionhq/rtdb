use std::{net::{IpAddr, Ipv4Addr, SocketAddr}, sync::Arc, time::Duration};
use rtdb_node::Database;
use rtdb_types::{Anomaly, Result};

pub const FORWARDED_PEER_HEADER: &str = "x-rtdb-flight-peer";

pub trait FlightPeerVerifier: std::fmt::Debug + Send + Sync {
    fn verify(&self, proof: &str, attachment: &Database) -> Result<IpAddr>;
}

#[derive(Clone, Debug)]
pub struct FlightConfig {
    pub address: SocketAddr,
    pub transaction_idle_timeout: Duration,
    pub prepared_statement_idle_timeout: Duration,
    pub session_idle_timeout: Duration,
    pub session_unclaimed_timeout: Duration,
    pub sweep_interval: Duration,
    pub maximum_message_bytes: usize,
    pub maximum_concurrent_calls: usize,
    pub playground: bool,
    pub forwarded_peer: Option<Arc<dyn FlightPeerVerifier>>,
}
impl Default for FlightConfig {
    fn default() -> Self {
        Self { address: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9839), transaction_idle_timeout: Duration::from_secs(1800), prepared_statement_idle_timeout: Duration::from_secs(1800), session_idle_timeout: Duration::from_secs(1800), session_unclaimed_timeout: Duration::from_secs(60), sweep_interval: Duration::from_secs(10), maximum_message_bytes: 64 * 1024 * 1024, maximum_concurrent_calls: 256, playground: false, forwarded_peer: None }
    }
}
impl FlightConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if [self.transaction_idle_timeout, self.prepared_statement_idle_timeout, self.session_idle_timeout, self.session_unclaimed_timeout, self.sweep_interval].iter().any(Duration::is_zero) || self.maximum_message_bytes == 0 || self.maximum_concurrent_calls == 0 {
            return Err(Anomaly::incorrect("rtdb.flight-sql/configuration", "Flight timeouts and transport limits must be positive"));
        }
        Ok(())
    }
}
